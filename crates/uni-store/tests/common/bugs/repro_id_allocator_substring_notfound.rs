// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for `runtime/id_allocator.rs:60`.
//!
//! `IdAllocator::new` decided "the counter manifest does not exist yet" by
//! substring-matching the *rendered* error:
//!
//! ```ignore
//! Err(e) if e.to_string().contains("not found") => (CounterManifest::default(), None),
//! ```
//!
//! Any transient failure whose message merely *reads* like a missing object —
//! a proxy 404 body, a wrapped S3 `bucket not found`, a misconfigured endpoint —
//! therefore starts the allocator from a **defaulted** manifest
//! (`next_vid_batch = 0`) against a database that already has rows, and it
//! proceeds to hand out VIDs that are already live.
//!
//! Two sibling loaders had already been migrated to the typed
//! `store_utils::is_not_found` predicate, each with a comment explaining why
//! substring matching is unsafe (`fork/registry.rs:172`,
//! `snapshot/manager.rs:185`). The allocator was the one never converted.

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path;
use uni_store::runtime::id_allocator::IdAllocator;

use super::fault_store::FaultStore;

/// A transient error whose text contains "not found" must propagate, not be
/// mistaken for an absent manifest.
#[tokio::test]
async fn transient_error_reading_like_not_found_must_not_reset_allocator() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fault = Arc::new(FaultStore::new(inner.clone()));
    let path = Path::from("catalog/counters.json");

    // Seed a real manifest by running an allocator to completion against the
    // healthy inner store, so the on-disk high-water mark is non-zero.
    {
        let alloc = IdAllocator::new(inner.clone(), path.clone(), 100)
            .await
            .expect("fresh allocator");
        for _ in 0..150 {
            alloc.allocate_vid().await.expect("allocate");
        }
    }
    let seeded = IdAllocator::new(inner.clone(), path.clone(), 100)
        .await
        .expect("reload seeded allocator");
    let seeded_vid = seeded.allocate_vid().await.expect("allocate from seeded");
    assert!(
        seeded_vid.as_u64() > 100,
        "precondition: the persisted manifest must carry a non-zero high-water mark, got {seeded_vid:?}"
    );

    // Now fail the GET with a *transient* Generic error whose message happens to
    // read like a missing object. This is the shape substring matching cannot
    // distinguish from a genuine NotFound.
    fault.set_transient_message("the specified bucket was not found");
    fault.set_fail_get(true);
    let res = IdAllocator::new(fault.clone(), path.clone(), 100).await;
    fault.set_fail_get(false);

    // FIXED (id_allocator.rs): the guard now uses the typed
    // `store_utils::is_not_found`, so a Generic error propagates regardless of
    // how its message renders.
    assert!(
        res.is_err(),
        "a transient store error must propagate; instead the allocator silently \
         reset to a default manifest and would re-issue live VIDs"
    );
}

/// The genuine-NotFound path must stay reachable — a fresh store still starts
/// from a defaulted manifest rather than erroring.
#[tokio::test]
async fn genuine_missing_manifest_still_starts_from_default() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("catalog/counters.json");

    let alloc = IdAllocator::new(inner, path, 100)
        .await
        .expect("absent manifest is not an error");
    let first = alloc.allocate_vid().await.expect("allocate");
    assert_eq!(
        first.as_u64(),
        0,
        "a fresh allocator must start at the beginning, got {first:?}"
    );
}
