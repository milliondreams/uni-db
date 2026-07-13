// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for id_allocator.rs:93 (finding [10]).
//!
//! `allocate_vid` advances the in-memory batch reservation
//! (`manifest.next_vid_batch`) BEFORE `persist_manifest`, and a persist
//! failure propagates via `?` without rolling the advance back. The next
//! allocation then sees `current_vid < next_vid_batch`, skips both the
//! reservation AND the persist, and returns Ok — a silent success against a
//! batch that was never durably reserved. After a reload the on-disk manifest
//! still holds the un-advanced value, so the same VID is handed out twice.

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path;
use uni_common::core::id::Vid;
use uni_store::runtime::id_allocator::IdAllocator;

use super::fault_store::FaultStore;

#[tokio::test]
async fn repro_persist_failure_leaves_advance_and_reuses_vid() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fault = Arc::new(FaultStore::new(inner.clone()));
    let path = Path::from("counters.json");

    let alloc = IdAllocator::new(fault.clone(), path.clone(), 100)
        .await
        .expect("allocator opens");

    // Fail the FIRST persist (batch reservation put).
    fault.fail_next_puts(1);
    let first = alloc.allocate_vid().await;
    assert!(
        first.is_err(),
        "the batch-reservation persist must fail on the injected fault"
    );

    // FIXED (id_allocator.rs): the failed persist rolled next_vid_batch back to 0,
    // so the retry finds current_vid(0) >= next_vid_batch(0), re-reserves, and this
    // time persists durably (the fault only failed one put). It returns Vid(0),
    // now backed by a durable batch reservation.
    let reserved = alloc
        .allocate_vid()
        .await
        .expect("retry re-attempts the durable reservation and succeeds");
    assert_eq!(
        reserved,
        Vid::new(0),
        "retry hands out Vid(0), now durably reserved"
    );

    // Reload from the REAL store: the batch [0,100) was durably reserved, so a
    // fresh allocator restarts at next_vid_batch (100) — Vid(0) is never reused.
    let alloc2 = IdAllocator::new(inner.clone(), path.clone(), 100)
        .await
        .expect("reopen");
    let after_reload = alloc2.allocate_vid().await.expect("allocate after reload");
    assert_eq!(
        after_reload,
        Vid::new(100),
        "reload must NOT reuse Vid(0): it resumes past the durably-reserved batch"
    );
}
