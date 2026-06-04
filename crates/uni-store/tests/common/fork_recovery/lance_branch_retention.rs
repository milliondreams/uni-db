// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! E1 verification: Lance compaction honors branch references.
//!
//! Spec §10 invariant: "Primary compaction doesn't break forks
//! (retention honors branch references)." This is the single most
//! important guarantee in the storage substrate; if it fails,
//! Phase 1 stops.
//!
//! Method: write rows on primary, branch off, write more rows on
//! primary, force `cleanup_old_versions` with a zero-duration
//! threshold (i.e. delete every reclaimable version), then read
//! through the branch and assert the original rows are still there.

// Rust guideline compliant

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

#[tokio::test]
async fn cleanup_old_versions_does_not_delete_branch_referenced_fragments() {
    let dir = TempDir::new().unwrap();
    let uri = format!("{}/ds.lance", dir.path().display());

    // V1: 1000 rows, batched into a single row group.
    let mut ids = Vec::with_capacity(1000);
    let mut vs = Vec::with_capacity(1000);
    for i in 0..1000u64 {
        ids.push(i);
        vs.push(i as i64);
    }
    let reader = RecordBatchIterator::new(vec![Ok(batch(ids, vs))].into_iter(), schema());
    Dataset::write(reader, &uri, None).await.unwrap();

    // Branch off V1.
    let v1 = lance_branch::current_version(&uri).await.unwrap();
    lance_branch::create_branch(&uri, "snap_v1", v1)
        .await
        .unwrap();

    // V2: append 1000 more rows on primary (post-fork writes).
    let mut ids = Vec::with_capacity(1000);
    let mut vs = Vec::with_capacity(1000);
    for i in 1000..2000u64 {
        ids.push(i);
        vs.push(i as i64);
    }
    let reader = RecordBatchIterator::new(vec![Ok(batch(ids, vs))].into_iter(), schema());
    let mut primary = Dataset::open(&uri).await.unwrap();
    primary.append(reader, None).await.unwrap();

    // V3: append another 1000.
    let mut ids = Vec::with_capacity(1000);
    let mut vs = Vec::with_capacity(1000);
    for i in 2000..3000u64 {
        ids.push(i);
        vs.push(i as i64);
    }
    let reader = RecordBatchIterator::new(vec![Ok(batch(ids, vs))].into_iter(), schema());
    let mut primary = Dataset::open(&uri).await.unwrap();
    primary.append(reader, None).await.unwrap();

    // Aggressive cleanup: zero-duration threshold reclaims every
    // version that *isn't* protected by a branch or tag.
    let primary = Dataset::open(&uri).await.unwrap();
    primary
        .cleanup_old_versions(chrono::TimeDelta::zero(), None, None)
        .await
        .expect("cleanup_old_versions");

    // Branch reads must still see the original 1000 rows.
    let branched = lance_branch::open_branch(&uri, "snap_v1").await.unwrap();
    let branch_count = branched.count_rows(None).await.unwrap();
    assert_eq!(
        branch_count, 1000,
        "post-cleanup: branch must still see fork-point rows; \
         retention is not honoring branch references — Phase 1 STOP"
    );

    // Primary still sees all 3000 rows.
    let primary = Dataset::open(&uri).await.unwrap();
    let primary_count = primary.count_rows(None).await.unwrap();
    assert_eq!(primary_count, 3000);
}

#[tokio::test]
async fn cleanup_then_branch_drop_recovers_disk() {
    // Sanity: after dropping the branch and running cleanup again,
    // the dataset's footprint shrinks (or at least the branch's
    // exclusive fragments become reclaimable). Phase 1 doesn't
    // measure exact byte counts; this test just confirms drop +
    // cleanup don't crash.
    let dir = TempDir::new().unwrap();
    let uri = format!("{}/ds.lance", dir.path().display());

    let reader = RecordBatchIterator::new(
        vec![Ok(batch(vec![1, 2, 3], vec![10, 20, 30]))].into_iter(),
        schema(),
    );
    Dataset::write(reader, &uri, None).await.unwrap();

    let v1 = lance_branch::current_version(&uri).await.unwrap();
    lance_branch::create_branch(&uri, "ephemeral", v1)
        .await
        .unwrap();

    let reader = RecordBatchIterator::new(
        vec![Ok(batch(vec![4, 5], vec![40, 50]))].into_iter(),
        schema(),
    );
    let mut primary = Dataset::open(&uri).await.unwrap();
    primary.append(reader, None).await.unwrap();

    // Drop the branch.
    lance_branch::delete_branch(&uri, "ephemeral")
        .await
        .unwrap();

    // Cleanup; primary still readable.
    let primary = Dataset::open(&uri).await.unwrap();
    primary
        .cleanup_old_versions(chrono::TimeDelta::zero(), None, None)
        .await
        .expect("cleanup_old_versions");
    let primary = Dataset::open(&uri).await.unwrap();
    assert_eq!(primary.count_rows(None).await.unwrap(), 5);
}
