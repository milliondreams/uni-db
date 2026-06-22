// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Fault-injection recovery test: simulate a crash mid-`create_branch`
//! and verify recovery rolls back cleanly without leaving zombies.
//!
//! Uses the env-var-gated fault hook in
//! `uni_store::backend::lance_branch::create_branch`. This test runs
//! alone (single-test binary) so its env-var manipulation can't race
//! with other tests.

// Rust guideline compliant

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use uni_common::core::fork::{ForkId, ForkInfo, ForkStatus};
use uni_store::backend::lance_branch;
use uni_store::fork::recovery::{join_uri_with, recover_forks};
use uni_store::fork::registry::ForkRegistryHandle;

/// Seeds two Lance datasets so a partial fork creation can succeed
/// on the first and fail on the second, leaving a Pending registry
/// entry plus exactly one orphan branch on disk.
async fn seed(uri: &str) {
    use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator, UInt64Array};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![1u64, 2])),
            Arc::new(Int64Array::from(vec![10i64, 20])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
    lance::Dataset::write(reader, uri, None).await.unwrap();
}

#[tokio::test]
async fn partial_create_branch_rolls_back_on_recovery() {
    // We exercise the fault hook by directly setting the env var,
    // calling create_branch a couple of times, then running recovery.
    let dir = TempDir::new().unwrap();
    let uri_a = format!("{}/vertices_A.lance", dir.path().display());
    let uri_b = format!("{}/vertices_B.lance", dir.path().display());
    seed(&uri_a).await;
    seed(&uri_b).await;

    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let registry = ForkRegistryHandle::load(store.clone()).await.unwrap();

    // Begin a Pending fork that "wants" branches on both datasets. Use the
    // real branch-naming convention `fork_{fork_id}_{dataset}` so recovery's
    // candidate-name reconstruction can find the zombie.
    let fork_id = ForkId::new();
    let info = ForkInfo::new_pending(fork_id, "partial", "snap-1", 1);
    registry.begin_create(info.clone()).await.unwrap();
    let branch_a = format!("fork_{fork_id}_vertices_A");
    let branch_b = format!("fork_{fork_id}_vertices_B");

    // Reset the counter and arm the hook to fail after the first call.
    lance_branch::fault_injection::reset();
    // SAFETY: integration tests run with `--test-threads=1` per binary
    // by default in nextest; this binary has only one test.
    unsafe { std::env::set_var("UNI_FORK_INJECT_FAIL_AFTER", "1") };

    // Create the first branch successfully.
    let parent_v_a = lance_branch::current_version(&uri_a).await.unwrap();
    lance_branch::create_branch(&uri_a, &branch_a, parent_v_a)
        .await
        .expect("first call succeeds before threshold");

    // Second call hits the fault.
    let parent_v_b = lance_branch::current_version(&uri_b).await.unwrap();
    let err = lance_branch::create_branch(&uri_b, &branch_b, parent_v_b).await;
    assert!(
        err.is_err(),
        "second create_branch must fail under fault hook"
    );

    // Disarm the hook and reset the counter so recovery's calls don't trip.
    unsafe { std::env::remove_var("UNI_FORK_INJECT_FAIL_AFTER") };
    lance_branch::fault_injection::reset();

    // At this point: registry has Pending entry "partial" with no recorded
    // `datasets`, and dataset A has a zombie branch `fork_{id}_vertices_A`.
    // Reload the registry to simulate restart and run recovery, passing the
    // schema-derived candidate dataset names.
    let h2 = ForkRegistryHandle::load(store.clone()).await.unwrap();
    {
        let snap = h2.snapshot().await;
        assert_eq!(snap.forks["partial"].status, ForkStatus::Pending);
    }
    let base = format!("{}/", dir.path().display());
    let candidates = vec!["vertices_A".to_string(), "vertices_B".to_string()];
    let reconciled = recover_forks(&h2, &store, &candidates, join_uri_with(base))
        .await
        .unwrap();
    assert_eq!(reconciled, 1);

    // Registry: empty.
    assert!(h2.snapshot().await.forks.is_empty());

    // L3: the zombie branch on dataset A is reclaimed by recovery (no manual
    // cleanup needed) — recovery reconstructed `fork_{id}_vertices_A` from
    // the candidate names and force-deleted it.
    let live_a = lance_branch::list_branches(&uri_a).await.unwrap();
    assert!(
        !live_a.iter().any(|b| b == &branch_a),
        "recovery must reclaim the zombie branch; still present: {live_a:?}"
    );

    // Dataset B has no branch (the fault prevented creation).
    let live_b = lance_branch::list_branches(&uri_b).await.unwrap();
    assert!(!live_b.iter().any(|b| b == &branch_b));
}
