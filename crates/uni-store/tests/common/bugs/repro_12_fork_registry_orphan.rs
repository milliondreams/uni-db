// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for fork/registry.rs:171 (finding [12]).
//!
//! `ForkRegistryHandle::load` maps EVERY `get` failure — not just NotFound —
//! into an empty registry (`Err(_) => ForkRegistryFile::default()`). A
//! transient-but-persistent object-store error at load is therefore
//! indistinguishable from a genuinely-absent registry, and the next mutation
//! persists the empty-plus-new cache over the real file, orphaning every
//! previously-registered fork.

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use uni_common::core::fork::{ForkId, ForkInfo};
use uni_store::fork::registry::ForkRegistryHandle;

use super::fault_store::FaultStore;

#[tokio::test]
async fn repro_transient_get_failure_orphans_existing_forks() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Seed two Pending forks through a clean handle.
    let clean = ForkRegistryHandle::load(inner.clone()).await.unwrap();
    clean
        .begin_create(ForkInfo::new_pending(ForkId::new(), "f1", "snap", 1))
        .await
        .unwrap();
    clean
        .begin_create(ForkInfo::new_pending(ForkId::new(), "f2", "snap", 1))
        .await
        .unwrap();

    // Confirm they are durable on disk.
    let reload = ForkRegistryHandle::load(inner.clone()).await.unwrap();
    assert_eq!(
        reload.snapshot().await.forks.len(),
        2,
        "two forks persisted on disk"
    );

    // Transient (non-NotFound) GET failure during load.
    let fault = Arc::new(FaultStore::new(inner.clone()));
    fault.set_fail_get(true);
    let bad = ForkRegistryHandle::load(fault.clone()).await;

    // Fixed (registry.rs:171): a transient failure surfaces as an error (per the
    // doc, a ForkLifecycle failure) instead of collapsing to an empty registry.
    assert!(
        bad.is_err(),
        "transient load failure must surface as Err, not an empty registry; got Ok"
    );

    // Because load failed, no empty handle exists to overwrite the real file.
    // Reloading from the underlying store shows f1 and f2 intact — not orphaned.
    fault.set_fail_get(false);
    let after = ForkRegistryHandle::load(inner.clone()).await.unwrap();
    let forks = after.snapshot().await.forks;
    assert!(
        forks.contains_key("f1") && forks.contains_key("f2"),
        "pre-existing forks must be preserved when load errors transiently"
    );
}
