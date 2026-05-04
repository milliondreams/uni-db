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
/// uses a per-fork [`IdAllocator`] (Day 3) persisted under
/// `catalog/forks/{fork_id}/id_allocator.json`, a per-fork
/// [`WriteAheadLog`] (Day 5) rooted at `wal/forks/{fork_id}/`, and
/// a fresh L0 buffer at `start_version=0`.
///
/// # Errors
///
/// Returns the underlying [`anyhow::Error`] if the allocator cannot
/// be built (object-store IO failure on persisted state).
pub async fn new_for_fork(
    storage: Arc<StorageManager>,
    schema_manager: Arc<SchemaManager>,
    fork_id: &ForkId,
    config: UniConfig,
) -> Result<Writer> {
    let store = storage.store();
    let allocator = id_alloc::new_for_fork_arc(
        store.clone(),
        fork_id,
        id_alloc::DEFAULT_FORK_BATCH_SIZE,
    )
    .await?;
    let wal = fork_wal::new_for_fork_arc(store, fork_id);
    // Initialize the WAL so its LSN counter picks up any persisted
    // segments from prior sessions on the same fork.
    wal.initialize().await?;

    Writer::new_with_config(
        storage,
        schema_manager,
        // Fork's own version namespace; not interleaved with primary.
        0,
        config,
        Some(wal),
        Some(allocator),
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

    /// Build a primary StorageManager + SchemaManager from a temp dir.
    async fn primary_storage() -> (TempDir, Arc<StorageManager>, Arc<SchemaManager>) {
        let dir = TempDir::new().unwrap();
        let schema_path = dir.path().join("schema.json");
        let schema_store: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
        );
        let schema = SchemaManager::load_from_store(
            schema_store,
            &ObjectStorePath::from("schema.json"),
        )
        .await
        .unwrap();
        let _ = schema_path; // path read by load_from_store
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
        let (_dir, storage, schema) = primary_storage().await;
        let fork_id = ForkId::new();

        let writer = new_for_fork(
            storage.clone(),
            schema.clone(),
            &fork_id,
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
        let (_dir, storage, schema) = primary_storage().await;
        let id_a = ForkId::new();
        let id_b = ForkId::new();

        let writer_a = new_for_fork(
            storage.clone(),
            schema.clone(),
            &id_a,
            UniConfig::default(),
        )
        .await
        .unwrap();
        let writer_b = new_for_fork(
            storage.clone(),
            schema.clone(),
            &id_b,
            UniConfig::default(),
        )
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
