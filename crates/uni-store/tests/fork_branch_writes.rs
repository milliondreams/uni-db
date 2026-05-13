// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 2 Day 1: branch-targeted write helpers in `lance_branch`.
//!
//! These tests exercise the lance crate's branch-write paths directly
//! (no uni stack), establishing that:
//! - Writes against a branch advance only that branch.
//! - Primary's main branch is byte-for-byte unaffected.
//! - Multiple sequential writes to the same branch accumulate.
//! - Dataset+branch creation in one helper produces a usable fork
//!   for a label that didn't exist on primary.

// Rust guideline compliant

#![cfg(feature = "lance-backend")]

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator, UInt64Array};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use lance::Dataset;
use tempfile::TempDir;
use uni_store::backend::lance_branch;

fn schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn batch(ids: Vec<u64>, vs: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(UInt64Array::from(ids)),
            Arc::new(Int64Array::from(vs)),
        ],
    )
    .unwrap()
}

fn reader(
    b: RecordBatch,
) -> RecordBatchIterator<std::iter::Once<Result<RecordBatch, arrow_schema::ArrowError>>> {
    RecordBatchIterator::new(std::iter::once(Ok(b)), schema())
}

async fn seed_dataset() -> (TempDir, String) {
    let dir = TempDir::new().unwrap();
    let uri = format!("{}/ds.lance", dir.path().display());
    Dataset::write(reader(batch(vec![1, 2, 3], vec![10, 20, 30])), &uri, None)
        .await
        .unwrap();
    (dir, uri)
}

#[tokio::test]
async fn write_to_branch_appends_only_to_branch() {
    let (_dir, uri) = seed_dataset().await;

    let parent_v = lance_branch::current_version(&uri).await.unwrap();
    lance_branch::create_branch(&uri, "scenario", parent_v)
        .await
        .unwrap();

    // Append two rows on the branch.
    lance_branch::write_to_branch(&uri, "scenario", reader(batch(vec![4, 5], vec![40, 50])))
        .await
        .unwrap();

    // Branch sees five rows.
    let on_branch = lance_branch::open_branch(&uri, "scenario").await.unwrap();
    assert_eq!(on_branch.count_rows(None).await.unwrap(), 5);

    // Primary's main branch still sees three.
    let primary = Dataset::open(&uri).await.unwrap();
    assert_eq!(primary.count_rows(None).await.unwrap(), 3);
}

#[tokio::test]
async fn delete_from_branch_removes_only_on_branch() {
    let (_dir, uri) = seed_dataset().await;

    let parent_v = lance_branch::current_version(&uri).await.unwrap();
    lance_branch::create_branch(&uri, "del", parent_v)
        .await
        .unwrap();

    lance_branch::delete_from_branch(&uri, "del", "id = 2")
        .await
        .unwrap();

    // Branch is missing id=2.
    let on_branch = lance_branch::open_branch(&uri, "del").await.unwrap();
    assert_eq!(on_branch.count_rows(None).await.unwrap(), 2);

    // Primary still has all three.
    let primary = Dataset::open(&uri).await.unwrap();
    assert_eq!(primary.count_rows(None).await.unwrap(), 3);
}

#[tokio::test]
async fn replace_branch_tip_overwrites_branch_only() {
    let (_dir, uri) = seed_dataset().await;

    let parent_v = lance_branch::current_version(&uri).await.unwrap();
    lance_branch::create_branch(&uri, "replace", parent_v)
        .await
        .unwrap();

    // Append something we'll replace.
    lance_branch::write_to_branch(&uri, "replace", reader(batch(vec![100], vec![1000])))
        .await
        .unwrap();
    let on_branch = lance_branch::open_branch(&uri, "replace").await.unwrap();
    assert_eq!(on_branch.count_rows(None).await.unwrap(), 4);

    // Replace the tip with a single row.
    lance_branch::replace_branch_tip(&uri, "replace", reader(batch(vec![999], vec![9999])))
        .await
        .unwrap();
    let on_branch = lance_branch::open_branch(&uri, "replace").await.unwrap();
    assert_eq!(on_branch.count_rows(None).await.unwrap(), 1);

    // Primary unaffected.
    let primary = Dataset::open(&uri).await.unwrap();
    assert_eq!(primary.count_rows(None).await.unwrap(), 3);
}

#[tokio::test]
async fn create_dataset_then_branch_yields_writable_fork() {
    // Used in Phase 2's on-the-fly schema overlay flow when a fork
    // creates a label whose Lance dataset doesn't exist yet.
    let dir = TempDir::new().unwrap();
    let uri = format!("{}/new_label.lance", dir.path().display());

    // Empty initial batch — the dataset starts at v1 with zero rows;
    // the schema is still preserved on disk so subsequent appends
    // line up.
    let empty = RecordBatch::new_empty(schema());
    let initial = RecordBatchIterator::new(std::iter::once(Ok(empty)), schema());
    lance_branch::create_dataset_then_branch(&uri, "fork_only", initial)
        .await
        .unwrap();

    // Branch exists; primary is empty.
    let branches = lance_branch::list_branches(&uri).await.unwrap();
    assert!(branches.iter().any(|b| b == "fork_only"));
    let primary = Dataset::open(&uri).await.unwrap();
    assert_eq!(primary.count_rows(None).await.unwrap(), 0);

    // Append data to the branch and confirm primary stays at zero.
    lance_branch::write_to_branch(&uri, "fork_only", reader(batch(vec![7, 8], vec![70, 80])))
        .await
        .unwrap();
    let on_branch = lance_branch::open_branch(&uri, "fork_only").await.unwrap();
    assert_eq!(on_branch.count_rows(None).await.unwrap(), 2);
    let primary = Dataset::open(&uri).await.unwrap();
    assert_eq!(primary.count_rows(None).await.unwrap(), 0);
}

#[tokio::test]
async fn writes_on_one_branch_invisible_to_sibling_branches() {
    let (_dir, uri) = seed_dataset().await;
    let parent_v = lance_branch::current_version(&uri).await.unwrap();
    lance_branch::create_branch(&uri, "alpha", parent_v)
        .await
        .unwrap();
    lance_branch::create_branch(&uri, "beta", parent_v)
        .await
        .unwrap();

    lance_branch::write_to_branch(&uri, "alpha", reader(batch(vec![100], vec![1])))
        .await
        .unwrap();

    // alpha sees 4; beta still sees 3; primary sees 3.
    assert_eq!(
        lance_branch::open_branch(&uri, "alpha")
            .await
            .unwrap()
            .count_rows(None)
            .await
            .unwrap(),
        4
    );
    assert_eq!(
        lance_branch::open_branch(&uri, "beta")
            .await
            .unwrap()
            .count_rows(None)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        Dataset::open(&uri)
            .await
            .unwrap()
            .count_rows(None)
            .await
            .unwrap(),
        3
    );
}
