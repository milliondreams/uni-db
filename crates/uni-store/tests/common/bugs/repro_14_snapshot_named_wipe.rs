// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for snapshot/manager.rs:178 (finding [14]).
//!
//! `load_named_snapshots` maps EVERY error into an empty map
//! (`Err(_) => Ok(HashMap::new())`). `save_named_snapshot` is a
//! read-modify-write over that map, so a transient store error during the
//! internal load silently wipes all previously named snapshots: the
//! subsequent `put` writes a map of just the single new entry, and no error
//! is surfaced.

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use uni_store::snapshot::manager::SnapshotManager;

use super::fault_store::FaultStore;

#[tokio::test]
async fn repro_transient_get_failure_wipes_named_snapshots() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let fault = Arc::new(FaultStore::new(inner.clone()));
    let mgr = SnapshotManager::new(fault.clone());

    // Seed two named snapshots.
    mgr.save_named_snapshot("prod", "snap-1").await.unwrap();
    mgr.save_named_snapshot("staging", "snap-2").await.unwrap();
    assert_eq!(
        mgr.load_named_snapshots().await.unwrap().len(),
        2,
        "two named snapshots persisted"
    );

    // Transient (non-NotFound) GET failure during the internal load of the
    // read-modify-write in save_named_snapshot.
    fault.set_fail_get(true);
    let res = mgr.save_named_snapshot("qa", "snap-3").await;
    fault.set_fail_get(false);

    // Fixed (manager.rs:178): the internal load now propagates the transient
    // error, so save fails instead of silently doing a read-modify-write over
    // an empty map.
    assert!(
        res.is_err(),
        "save must propagate the transient load failure, not swallow it; got Ok"
    );

    // The persisted map is untouched — prod & staging survive, qa was not added.
    let map = mgr.load_named_snapshots().await.unwrap();
    assert_eq!(
        map.len(),
        2,
        "existing named snapshots must be preserved when the load fails; got {map:?}"
    );
    assert!(
        map.contains_key("prod") && map.contains_key("staging"),
        "prod & staging must survive the failed save"
    );
    assert!(!map.contains_key("qa"), "the failed save must not persist qa");
}
