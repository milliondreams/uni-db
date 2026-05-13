// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Fork-scoped [`IdAllocator`] factory.
//!
//! Each fork gets its own VID/EID counter persisted at
//! `catalog/forks/{fork_id}/id_allocator.json`. This isolates fork
//! allocations from primary's *concurrent* allocator (avoiding §10
//! "fork operations don't block primary") while still ensuring fork
//! VIDs do not collide with primary's existing on-disk data via the
//! `base_paths` chain.
//!
//! # The collision problem
//!
//! Naively starting fork allocators at zero produces a read-time
//! collision: primary's Lance branch (visible to the fork through
//! `base_paths`) has rows at VIDs that the fork would re-allocate.
//! Lance read merges prefer branch data, so the fork's first writes
//! get shadowed by primary's pre-existing rows at the same VID.
//!
//! # Resolution: bootstrap from primary's HWM
//!
//! At fork-creation time we copy primary's `id_allocator.json` to
//! `catalog/forks/{fork_id}/id_allocator.json` and load the fork's
//! allocator from that copy. The fork starts allocating *above*
//! primary's snapshot HWM, so its first VID does not collide with
//! any primary row that was on disk at fork-point.
//!
//! Subsequent primary allocations may eventually reach the fork's
//! range; that's a Phase 6 (promotion) concern. UniId is content-
//! hashed, so user-visible identity stays consistent even when
//! VIDs collide on the disk layer.
//!
//! # Path convention
//!
//! - Primary: `id_allocator.json` (under the database root store)
//! - Fork: `catalog/forks/{fork_id}/id_allocator.json`
//!
//! These paths live on the same `ObjectStore` as the primary allocator.
//! No new store is created per fork.

// Rust guideline compliant

use std::sync::Arc;

use anyhow::Result;
use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;
use uni_common::core::fork::ForkId;

use crate::runtime::id_allocator::IdAllocator;

/// Default batch size — matches the primary allocator's default
/// (`UniBuilder::build` reserves 1000 IDs at a time).
pub const DEFAULT_FORK_BATCH_SIZE: u64 = 1000;

/// Build the canonical persistence path for a fork's IdAllocator.
///
/// Used by both the factory below and the recovery driver in Day 6,
/// which needs to know where to look for fork-local allocator state
/// when reconstructing a fork's `Writer` on `Uni::open`.
#[must_use]
pub fn id_allocator_path(fork_id: &ForkId) -> ObjectStorePath {
    ObjectStorePath::from(format!("catalog/forks/{fork_id}/id_allocator.json"))
}

/// Construct (or reload) a fork-scoped [`IdAllocator`].
///
/// Uses the same primary `ObjectStore`, just at a different path.
/// Reusing the store keeps a single fsync/flush story for the
/// database; the fork has its own counter file but isn't otherwise
/// independent.
///
/// # Errors
///
/// Returns the underlying [`anyhow::Error`] if the store cannot be
/// read or the persisted manifest is malformed.
pub async fn new_for_fork(
    store: Arc<dyn ObjectStore>,
    fork_id: &ForkId,
    batch_size: u64,
) -> Result<IdAllocator> {
    let path = id_allocator_path(fork_id);
    IdAllocator::new(store, path, batch_size).await
}

/// Convenience for callers who want an `Arc<IdAllocator>`.
///
/// Day 4's Writer factory will use this directly.
///
/// # Errors
///
/// Same as [`new_for_fork`].
pub async fn new_for_fork_arc(
    store: Arc<dyn ObjectStore>,
    fork_id: &ForkId,
    batch_size: u64,
) -> Result<Arc<IdAllocator>> {
    Ok(Arc::new(new_for_fork(store, fork_id, batch_size).await?))
}

/// Bootstrap a fork's allocator file from primary's in-memory HWM.
///
/// Writes a fresh `CounterManifest` to
/// `catalog/forks/{fork_id}/id_allocator.json` whose `next_vid_batch`
/// and `next_eid_batch` are at least `vid_hwm` and `eid_hwm`. After
/// this call, [`new_for_fork`] returns an allocator whose first
/// allocations land *above* primary's HWM at fork-creation time —
/// preventing the read-time VID collision documented in the module
/// rustdoc.
///
/// Caller obtains the HWMs via [`IdAllocator::current_hwm`] on
/// primary's allocator. Reading from primary's in-memory state
/// avoids fragile file-copy across potentially-different
/// `ObjectStore` roots (primary's allocator file may live on the
/// database root store while the fork's allocator file lives on the
/// storage-rooted store).
///
/// Idempotent: if the fork's file already exists, this is a no-op.
/// Called at fork creation (Phase 2 Day 7); never at fork open.
///
/// # Errors
///
/// - Object-store IO failure on the fork's path.
pub async fn bootstrap_fork_from_primary_hwm(
    store: Arc<dyn ObjectStore>,
    fork_id: &ForkId,
    vid_hwm: u64,
    eid_hwm: u64,
) -> Result<()> {
    use crate::store_utils::{DEFAULT_TIMEOUT, get_with_timeout, put_with_timeout};

    let target = id_allocator_path(fork_id);

    // Skip if the fork's allocator already exists — bootstrap is a
    // creation-time operation, not an open-time one.
    if get_with_timeout(&store, &target, DEFAULT_TIMEOUT)
        .await
        .is_ok()
    {
        return Ok(());
    }

    // Build a manifest that sets next_*_batch to primary's HWM. The
    // fork's IdAllocator constructor sets `current_vid =
    // manifest.next_vid_batch`, so loading this file produces a fork
    // allocator that allocates from `vid_hwm` upward.
    //
    // We mirror the IdAllocator's persisted format directly.
    #[derive(serde::Serialize)]
    struct CounterManifestSnapshot {
        next_vid_batch: u64,
        next_eid_batch: u64,
    }
    let manifest = CounterManifestSnapshot {
        next_vid_batch: vid_hwm,
        next_eid_batch: eid_hwm,
    };
    let bytes = serde_json::to_vec_pretty(&manifest)?;
    put_with_timeout(&store, &target, bytes::Bytes::from(bytes), DEFAULT_TIMEOUT).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    async fn fresh_store() -> (TempDir, Arc<dyn ObjectStore>) {
        let dir = TempDir::new().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        (dir, store)
    }

    #[tokio::test]
    async fn path_includes_fork_id_under_catalog_forks() {
        let id = ForkId::new();
        let p = id_allocator_path(&id);
        let s = p.to_string();
        assert!(s.starts_with("catalog/forks/"));
        assert!(s.ends_with("/id_allocator.json"));
        assert!(s.contains(&id.to_string()));
    }

    #[tokio::test]
    async fn fresh_allocator_starts_from_zero() {
        // Mirrors the primary `IdAllocator`'s `CounterManifest::default`
        // — VIDs and EIDs are 0-indexed.
        let (_dir, store) = fresh_store().await;
        let id = ForkId::new();
        let alloc = new_for_fork(store, &id, DEFAULT_FORK_BATCH_SIZE)
            .await
            .unwrap();
        let v = alloc.allocate_vid().await.unwrap();
        assert_eq!(u64::from(v), 0, "fresh fork allocator starts at VID 0");
    }

    #[tokio::test]
    async fn two_forks_have_independent_vid_streams() {
        let (_dir, store) = fresh_store().await;
        let id_a = ForkId::new();
        let id_b = ForkId::new();

        let alloc_a = new_for_fork(store.clone(), &id_a, DEFAULT_FORK_BATCH_SIZE)
            .await
            .unwrap();
        let alloc_b = new_for_fork(store.clone(), &id_b, DEFAULT_FORK_BATCH_SIZE)
            .await
            .unwrap();

        // Each fork allocates the same VID values — they're independent
        // namespaces. Promotion (Phase 6) translates fork-local VIDs
        // via UniId dedup, so collisions are not a hazard.
        let a_first = alloc_a.allocate_vid().await.unwrap();
        let b_first = alloc_b.allocate_vid().await.unwrap();
        assert_eq!(u64::from(a_first), 0);
        assert_eq!(u64::from(b_first), 0);

        // Pull a few more on each — they continue independently.
        let a_next = alloc_a.allocate_vid().await.unwrap();
        let b_next = alloc_b.allocate_vid().await.unwrap();
        assert_eq!(u64::from(a_next), 1);
        assert_eq!(u64::from(b_next), 1);
    }

    #[tokio::test]
    async fn allocator_persists_across_reopen() {
        let (_dir, store) = fresh_store().await;
        let id = ForkId::new();

        // First open: allocate three VIDs (0, 1, 2).
        {
            let alloc = new_for_fork(store.clone(), &id, DEFAULT_FORK_BATCH_SIZE)
                .await
                .unwrap();
            for _ in 0..3 {
                alloc.allocate_vid().await.unwrap();
            }
        }

        // Reopen on the same path. The IdAllocator persists batch
        // reservations, so the next VID jumps to the start of the
        // reserved batch (>= batch_size) — durability without
        // per-allocation fsync.
        let alloc2 = new_for_fork(store, &id, DEFAULT_FORK_BATCH_SIZE)
            .await
            .unwrap();
        let next = alloc2.allocate_vid().await.unwrap();
        assert!(
            u64::from(next) >= DEFAULT_FORK_BATCH_SIZE,
            "reopened allocator must skip past previous batch's HWM; got {}",
            u64::from(next)
        );
    }

    #[tokio::test]
    async fn primary_id_allocator_unaffected_by_fork_allocator() {
        // Path isolation: the fork allocator's writes go to
        // `catalog/forks/{id}/id_allocator.json`, not to the primary's
        // `id_allocator.json`. Primary's file doesn't exist after a
        // fork-only session, and a primary IdAllocator opened against
        // the same store starts from scratch.
        let (_dir, store) = fresh_store().await;
        let id = ForkId::new();
        let fork_alloc = new_for_fork(store.clone(), &id, DEFAULT_FORK_BATCH_SIZE)
            .await
            .unwrap();
        for _ in 0..5 {
            fork_alloc.allocate_vid().await.unwrap();
        }

        // Primary allocator opens against the canonical primary path.
        let primary = IdAllocator::new(
            store,
            ObjectStorePath::from("id_allocator.json"),
            DEFAULT_FORK_BATCH_SIZE,
        )
        .await
        .unwrap();
        let primary_first = primary.allocate_vid().await.unwrap();
        assert_eq!(
            u64::from(primary_first),
            0,
            "primary allocator must not see any fork-side allocations"
        );
    }
}
