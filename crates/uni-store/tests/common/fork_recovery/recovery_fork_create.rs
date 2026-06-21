// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Recovery from a partial fork *create* — the registry has a
//! `Pending` entry written by step 2 of the create 2PC, but the
//! process died before step 4 promoted it to `Active`.
//!
//! Phase 1 policy: roll back. The recovery driver removes the
//! Pending entry from the registry and force-deletes any partial
//! Lance branches it can identify from the registry's `datasets`
//! map. Day 9 adds env-var-gated panic injection inside the
//! `lance_branch::create_branch` loop to drive end-to-end crash
//! tests; here we manufacture the synthetic on-disk state directly.

// Rust guideline compliant

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use uni_common::core::fork::{ForkId, ForkInfo};
use uni_store::backend::lance_branch;
use uni_store::fork::recovery::{join_uri_with, recover_forks};
use uni_store::fork::registry::ForkRegistryHandle;

#[tokio::test]
async fn pending_entry_with_no_branches_is_rolled_back() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let h = ForkRegistryHandle::load(store.clone()).await.unwrap();

    // Stage a Pending entry as if step 2 of create 2PC completed and
    // the process died before any branches were created.
    let info = ForkInfo::new_pending(ForkId::new(), "stuck", "snap-1", 1);
    h.begin_create(info).await.unwrap();

    // Reload from disk to simulate restart.
    let h2 = ForkRegistryHandle::load(store.clone()).await.unwrap();
    let base = format!("{}/", dir.path().display());
    let reconciled = recover_forks(&h2, &store, &[], join_uri_with(base))
        .await
        .unwrap();

    assert_eq!(reconciled, 1);
    let snap = h2.snapshot().await;
    assert!(snap.forks.is_empty(), "pending entry must be rolled back");
}

#[tokio::test]
async fn pending_entry_with_partial_branches_force_deletes_them() {
    // Seed a real Lance dataset, take its version, branch it, then
    // craft a Pending registry entry that records the branch. The
    // recovery driver should walk `datasets` and force-delete each
    // branch it lists.
    let dir = TempDir::new().unwrap();
    let dataset_uri = format!("{}/vertices_Person.lance", dir.path().display());
    seed_lance_dataset(&dataset_uri).await;

    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let h = ForkRegistryHandle::load(store.clone()).await.unwrap();

    let parent_v = lance_branch::current_version(&dataset_uri).await.unwrap();
    let id = ForkId::new();
    let branch_name = format!("fork_{id}_v_Person");
    lance_branch::create_branch(&dataset_uri, &branch_name, parent_v)
        .await
        .unwrap();

    // Record a Pending entry that captures the branch we just created.
    let mut info = ForkInfo::new_pending(id, "midflight", "snap-1", 1);
    info.datasets
        .insert("vertices_Person".into(), branch_name.clone());
    h.begin_create(info).await.unwrap();

    // Restart-equivalent recovery.
    let h2 = ForkRegistryHandle::load(store.clone()).await.unwrap();
    let base = format!("{}/", dir.path().display());
    recover_forks(&h2, &store, &[], join_uri_with(base))
        .await
        .unwrap();

    // Registry empty.
    assert!(h2.snapshot().await.forks.is_empty());

    // Branch removed from the dataset.
    let live_branches = lance_branch::list_branches(&dataset_uri).await.unwrap();
    assert!(
        !live_branches.iter().any(|b| b == &branch_name),
        "expected branch {branch_name} to be removed; saw {live_branches:?}"
    );
}

async fn seed_lance_dataset(uri: &str) {
    use arrow_array::{Int64Array, RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![1u64, 2, 3])),
            Arc::new(Int64Array::from(vec![10i64, 20, 30])),
        ],
    )
    .unwrap();
    let reader = arrow_array::RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
    lance::Dataset::write(reader, uri, None).await.unwrap();
}
