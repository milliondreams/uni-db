// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 2 Day 6: per-fork WAL replay.
//!
//! Verifies that mutations persisted to a fork's WAL are restored
//! on `Uni::open` (in tests, simulated by reconstructing the fork's
//! Writer via the factory and calling `replay_wal`). Primary's WAL
//! is byte-for-byte unaffected — its directory listing does not
//! see fork segments because the prefixes are disjoint
//! (`wal/` vs `wal_forks/`).

// Rust guideline compliant

#![cfg(feature = "lance-backend")]

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use uni_common::Vid;
use uni_common::config::UniConfig;
use uni_common::core::fork::ForkId;
use uni_common::core::schema::SchemaManager;
use uni_store::fork::wal as fork_wal;
use uni_store::fork::writer_factory;
use uni_store::runtime::wal::Mutation;
use uni_store::storage::manager::StorageManager;

async fn fixture() -> (
    TempDir,
    Arc<dyn ObjectStore>,
    Arc<StorageManager>,
    Arc<SchemaManager>,
) {
    let dir = TempDir::new().unwrap();
    // Schema lives under the temp root; storage lives under
    // `<temp>/storage`. The fork WAL prefix is relative to the
    // *storage's* ObjectStore, so we re-fetch the store from the
    // StorageManager and hand it back to callers — they must use the
    // same store the writer factory will use, not a separately-built
    // one rooted at `dir.path()`.
    let schema_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let schema = Arc::new(
        SchemaManager::load_from_store(
            schema_store,
            &object_store::path::Path::from("schema.json"),
        )
        .await
        .unwrap(),
    );

    let storage_path = dir.path().join("storage");
    std::fs::create_dir_all(&storage_path).unwrap();
    let storage = Arc::new(
        StorageManager::new_with_config(
            storage_path.to_str().unwrap(),
            schema.clone(),
            UniConfig::default(),
        )
        .await
        .unwrap(),
    );
    let storage_store = storage.store();

    (dir, storage_store, storage, schema)
}

#[tokio::test]
async fn fork_wal_replay_restores_persisted_mutations() {
    let (_dir, store, storage, schema) = fixture().await;
    let fork_id = ForkId::new();

    // 1. Persist some mutations directly through the fork WAL.
    //    This mirrors what a Writer would do on commit_transaction_l0.
    let wal = fork_wal::new_for_fork(store.clone(), &fork_id);
    wal.initialize().await.unwrap();
    wal.append(&Mutation::DeleteVertex {
        vid: Vid::new(11),
        labels: vec!["L".into()],
    })
    .unwrap();
    wal.append(&Mutation::DeleteVertex {
        vid: Vid::new(12),
        labels: vec!["L".into()],
    })
    .unwrap();
    let lsn = wal.flush().await.unwrap();
    assert!(lsn >= 1);

    // 2. Build a new Writer via the factory — the same flow that
    //    `UniInner::at_fork` uses. Then call `replay_wal(0)` to load
    //    persisted mutations into the freshly-built L0 buffer.
    let writer = writer_factory::new_for_fork(storage, schema, &fork_id, UniConfig::default())
        .await
        .unwrap();
    let replayed = writer.replay_wal(0).await.unwrap();
    assert_eq!(
        replayed, 2,
        "fork WAL replay should restore exactly the 2 persisted mutations"
    );
}

#[tokio::test]
async fn primary_wal_unaffected_by_fork_wal_segments() {
    // Sibling test for Day 5: confirm that even AFTER a fork has
    // flushed WAL segments to disk, the primary `wal/` listing is
    // empty. This is the spec §10 isolation invariant for the WAL
    // layer.
    let (_dir, store, _storage, _schema) = fixture().await;
    let fork_id = ForkId::new();

    let fork = fork_wal::new_for_fork(store.clone(), &fork_id);
    fork.initialize().await.unwrap();
    fork.append(&Mutation::DeleteVertex {
        vid: Vid::new(99),
        labels: vec![],
    })
    .unwrap();
    fork.flush().await.unwrap();

    // Primary WAL prefix is just "wal".
    let primary =
        uni_store::runtime::wal::WriteAheadLog::new(store, object_store::path::Path::from("wal"));
    let max = primary.initialize().await.unwrap();
    assert_eq!(max, 0, "primary WAL must not see fork-tagged segments");
}

#[tokio::test]
async fn replay_with_no_persisted_mutations_is_noop() {
    let (_dir, _store, storage, schema) = fixture().await;
    let fork_id = ForkId::new();

    let writer = writer_factory::new_for_fork(storage, schema, &fork_id, UniConfig::default())
        .await
        .unwrap();
    let replayed = writer.replay_wal(0).await.unwrap();
    assert_eq!(replayed, 0);
}
