// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for branch-aware reads through `LanceDbBackend`.
//!
//! These tests exercise the full read path that a forked session uses
//! in Phase 1 — `ScanRequest.branch` set, `LanceDbBackend::scan`
//! dispatching through `lance_branch::open_branch`. They complement
//! the lower-level tests inline in `backend::lance_branch::tests`,
//! which cover the lance crate primitives directly.

// Rust guideline compliant

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use tempfile::TempDir;
use uni_store::backend::lance::LanceDbBackend;
use uni_store::backend::lance_branch;
use uni_store::backend::traits::StorageBackend;
use uni_store::backend::types::{ScanRequest, WriteMode};

fn arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn batch(ids: Vec<u64>, values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(
        arrow_schema(),
        vec![
            Arc::new(UInt64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap()
}

async fn setup_backend() -> (TempDir, LanceDbBackend) {
    let dir = TempDir::new().unwrap();
    let backend = LanceDbBackend::connect(dir.path().to_str().unwrap(), None)
        .await
        .unwrap();
    backend
        .create_table("rows", vec![batch(vec![1, 2, 3], vec![10, 20, 30])])
        .await
        .unwrap();
    (dir, backend)
}

#[tokio::test]
async fn primary_scan_unaffected_by_branch_field_default() {
    // ScanRequest::all leaves branch = None — primary path must behave
    // identically to before the field existed.
    let (_dir, backend) = setup_backend().await;
    let batches = backend.scan(ScanRequest::all("rows")).await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3);
}

#[tokio::test]
async fn branch_scan_reads_parent_rows_via_base_paths() {
    // Spec §6.1: branch reads chain to parent through Lance's base_paths.
    let (dir, backend) = setup_backend().await;
    let dataset_uri = format!("{}/rows.lance", dir.path().display());
    let parent_v = lance_branch::current_version(&dataset_uri).await.unwrap();
    lance_branch::create_branch(&dataset_uri, "fork-1", parent_v)
        .await
        .unwrap();

    let request = ScanRequest::all("rows").with_branch("fork-1");
    let batches = backend.scan(request).await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3, "branch scan must see parent rows");
}

#[tokio::test]
async fn branch_scan_isolated_from_post_fork_primary_writes() {
    // Spec §10: snapshot isolation at fork point — primary writes after
    // branch creation are invisible to the branch.
    let (dir, backend) = setup_backend().await;
    let dataset_uri = format!("{}/rows.lance", dir.path().display());
    let parent_v = lance_branch::current_version(&dataset_uri).await.unwrap();
    lance_branch::create_branch(&dataset_uri, "snap", parent_v)
        .await
        .unwrap();

    backend
        .write(
            "rows",
            vec![batch(vec![4, 5], vec![40, 50])],
            WriteMode::Append,
        )
        .await
        .unwrap();

    // Primary sees five rows.
    let primary_batches = backend.scan(ScanRequest::all("rows")).await.unwrap();
    let primary_total: usize = primary_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(primary_total, 5);

    // Branch still sees the three pre-fork rows.
    let branch_batches = backend
        .scan(ScanRequest::all("rows").with_branch("snap"))
        .await
        .unwrap();
    let branch_total: usize = branch_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        branch_total, 3,
        "post-fork primary writes must not leak into branch"
    );
}

#[tokio::test]
async fn branch_scan_respects_filter_and_projection() {
    let (dir, backend) = setup_backend().await;
    let dataset_uri = format!("{}/rows.lance", dir.path().display());
    let parent_v = lance_branch::current_version(&dataset_uri).await.unwrap();
    lance_branch::create_branch(&dataset_uri, "filter-fork", parent_v)
        .await
        .unwrap();

    let request = ScanRequest::all("rows")
        .with_branch("filter-fork")
        .with_filter("id > 1")
        .with_columns(vec!["id".to_string()]);
    let batches = backend.scan(request).await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2);
    // Projection narrowed to one column.
    if let Some(b) = batches.first() {
        assert_eq!(b.num_columns(), 1, "projection should drop value column");
    }
}
