// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Constructor for a fork-scoped [`Writer`].
//!
//! Each fork session owns its own `Writer` so that:
//! - Commits on different forks proceed in parallel (spec §10).
//! - Writes against `storage.backend()` route to the fork's branches
//!   automatically (the storage's backend is a `BranchedBackend`).
//! - The fork has its own `IdAllocator` (Day 3) so VID/EID streams
//!   don't collide with primary or with sibling forks.
//!
//! Phase 2 Day 4 scope: no per-fork WAL yet (Day 5), no auto-flush
//! task (Day 4 MVP — manual flush via commit). Each piece is layered
//! in across the next two days.

// Rust guideline compliant

use std::sync::Arc;

use anyhow::Result;
use uni_common::config::UniConfig;
use uni_common::core::fork::ForkId;
use uni_common::core::schema::SchemaManager;

use crate::runtime::writer::Writer;
use crate::storage::manager::StorageManager;

use super::{id_alloc, wal as fork_wal};

/// Construct a fresh [`Writer`] for a forked session.
///
/// The supplied `storage` must already be fork-scoped — typically
/// `primary.at_fork(scope)` from Day 5 wiring. The returned Writer
/// uses a per-fork `IdAllocator` (Day 3) persisted under
/// `catalog/forks/{fork_id}/id_allocator.json`, a per-fork
/// `WriteAheadLog` (Day 5) rooted at `wal/forks/{fork_id}/`, and
/// an L0 buffer whose version counter starts at `start_version` — the
/// parent's fork-point version HWM, so a fork transaction's
/// `_version <= pin` read still sees inherited (base_paths) rows while the
/// fork's own writes get versions above it.
///
/// # Errors
///
/// Returns the underlying [`anyhow::Error`] if the allocator cannot
/// be built (object-store IO failure on persisted state).
pub async fn new_for_fork(
    storage: Arc<StorageManager>,
    schema_manager: Arc<SchemaManager>,
    fork_id: &ForkId,
    start_version: u64,
    config: UniConfig,
) -> Result<Writer> {
    let store = storage.store();
    let allocator =
        id_alloc::new_for_fork_arc(store.clone(), fork_id, id_alloc::DEFAULT_FORK_BATCH_SIZE)
            .await?;
    // Local stores get fsync-on-flush for the fork WAL, same as the primary.
    let wal =
        Arc::new(fork_wal::new_for_fork(store, fork_id).with_local_root(storage.local_fs_root()));
    // Initialize the WAL so its LSN counter picks up any persisted
    // segments from prior sessions on the same fork.
    wal.initialize().await?;

    // The fork identity is a constructor argument, not a later assignment:
    // `new_with_config` captures the `SharedFlushCtx` the `FlushCoordinator`
    // uses for every async flush, so anything tagged on afterwards never
    // reaches that path. Tagging it here previously left the coordinator
    // finalizing this fork's flushes under primary's identity.
    Writer::new_with_config(
        storage,
        schema_manager,
        // Bootstrap the fork's version floor to the parent's fork-point
        // HWM so a fork tx read sees inherited rows (fork writes go above).
        start_version,
        config,
        Some(wal),
        Some(allocator),
        Some(*fork_id),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStore;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as ObjectStorePath;
    use tempfile::TempDir;

    /// Build a **fork-scoped** StorageManager for `fork_id`, alongside its
    /// schema manager.
    ///
    /// These tests used to hand `new_for_fork` a *primary* StorageManager.
    /// That pairing is invalid — a writer carrying a `fork_id` must publish
    /// through a fork-scoped snapshot namespace, which is asserted both at
    /// construction and in `flush_finalize_body`. It went unnoticed because
    /// these tests never flush, so only the construction-time assertion catches
    /// it. Building the real object graph costs a dozen lines and keeps the
    /// allocator contract being tested here anchored to a writer that could
    /// actually flush.
    async fn fork_storage(fork_id: ForkId) -> (TempDir, Arc<StorageManager>, Arc<SchemaManager>) {
        let (dir, primary, schema) = primary_storage().await;
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let registry = Arc::new(crate::fork::ForkRegistryHandle::load(store).await.unwrap());
        let info = uni_common::core::fork::ForkInfo::new_pending(
            fork_id,
            "writer_factory_test",
            "snap-1",
            1,
        );
        registry.begin_create(info.clone()).await.unwrap();
        let active = registry
            .finish_create("writer_factory_test", info.datasets.clone())
            .await
            .unwrap();
        let scope = Arc::new(crate::fork::ForkScope::new(
            Arc::new(active),
            uni_common::core::fork::SchemaDelta::empty(),
            registry,
        ));
        let forked = Arc::new(primary.at_fork(scope));
        debug_assert!(forked.fork_scope().is_some());
        (dir, forked, schema)
    }

    /// Build a primary StorageManager + SchemaManager from a temp dir.
    async fn primary_storage() -> (TempDir, Arc<StorageManager>, Arc<SchemaManager>) {
        let dir = TempDir::new().unwrap();
        let schema_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let schema =
            SchemaManager::load_from_store(schema_store, &ObjectStorePath::from("schema.json"))
                .await
                .unwrap();
        let schema = Arc::new(schema);

        let storage_path = dir.path().join("storage");
        std::fs::create_dir_all(&storage_path).unwrap();
        let storage = StorageManager::new_with_config(
            storage_path.to_str().unwrap(),
            schema.clone(),
            UniConfig::default(),
        )
        .await
        .unwrap();
        (dir, Arc::new(storage), schema)
    }

    #[tokio::test]
    async fn new_for_fork_builds_writer_with_fork_allocator() {
        let fork_id = ForkId::new();
        let (_dir, storage, schema) = fork_storage(fork_id).await;

        let writer = new_for_fork(
            storage.clone(),
            schema.clone(),
            &fork_id,
            0,
            UniConfig::default(),
        )
        .await
        .unwrap();

        // The fork's allocator starts at VID 0 (per Day 3 contract).
        let v = writer.allocator.allocate_vid().await.unwrap();
        assert_eq!(u64::from(v), 0);
    }

    #[tokio::test]
    async fn two_fork_writers_have_independent_allocators() {
        // One fork-scoped storage per fork: a storage manager is scoped to a
        // single fork, so sharing one across two fork writers would pair at
        // least one of them with the wrong namespace.
        let id_a = ForkId::new();
        let id_b = ForkId::new();
        let (_dir_a, storage_a, schema_a) = fork_storage(id_a).await;
        let (_dir_b, storage_b, schema_b) = fork_storage(id_b).await;

        let writer_a = new_for_fork(storage_a, schema_a, &id_a, 0, UniConfig::default())
            .await
            .unwrap();
        let writer_b = new_for_fork(storage_b, schema_b, &id_b, 0, UniConfig::default())
            .await
            .unwrap();

        // Each starts at VID 0, independently — promotion later
        // resolves any collisions via UniId dedup.
        assert_eq!(
            u64::from(writer_a.allocator.allocate_vid().await.unwrap()),
            0
        );
        assert_eq!(
            u64::from(writer_b.allocator.allocate_vid().await.unwrap()),
            0
        );
    }
}
