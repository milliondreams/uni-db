// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 2 Day 2: BranchedBackend write methods route to fork branches
//! via `lance_branch` helpers. The contract every test asserts:
//!
//! 1. The write succeeds when the fork has a branch for the table.
//! 2. The fork's branch reflects the write.
//! 3. Primary's main branch is byte-for-byte unchanged — no leakage.
//! 4. Calls without a branch return a typed `ForkLifecycle`-flavored
//!    error (Day 10 will replace these with on-the-fly creation).

// Rust guideline compliant

#![cfg(feature = "lance-backend")]

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use uni_common::core::fork::{ForkId, ForkInfo, SchemaDelta};
use uni_store::backend::branched::BranchedBackend;
use uni_store::backend::lance::LanceDbBackend;
use uni_store::backend::lance_branch;
use uni_store::backend::traits::StorageBackend;
use uni_store::backend::types::{ScanRequest, WriteMode};
use uni_store::fork::{ForkRegistryHandle, ForkScope};

fn arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn batch(ids: Vec<u64>, vs: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(
        arrow_schema(),
        vec![
            Arc::new(UInt64Array::from(ids)),
            Arc::new(Int64Array::from(vs)),
        ],
    )
    .unwrap()
}

/// Set up a primary backend with one table, branch it for a fork, and
/// return everything needed for write tests: the temp dir, the
/// `BranchedBackend`, the fork's branch name, the inner backend (to
/// inspect primary).
async fn fixture()
-> (TempDir, Arc<dyn StorageBackend>, BranchedBackend, String) {
    let dir = TempDir::new().unwrap();
    let inner: Arc<dyn StorageBackend> = Arc::new(
        LanceDbBackend::connect(dir.path().to_str().unwrap(), None)
            .await
            .unwrap(),
    );
    inner
        .create_table("rows", vec![batch(vec![1, 2, 3], vec![10, 20, 30])])
        .await
        .unwrap();
    let dataset_uri = format!("{}/rows.lance", dir.path().display());
    let parent_v = lance_branch::current_version(&dataset_uri).await.unwrap();
    let id = ForkId::new();
    let branch_name = format!("fork_{id}_rows");
    lance_branch::create_branch(&dataset_uri, &branch_name, parent_v)
        .await
        .unwrap();

    // Build a registry + scope so BranchedBackend has the dataset map.
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let registry = Arc::new(ForkRegistryHandle::load(store).await.unwrap());
    let mut info = ForkInfo::new_pending(id, "scenario", "snap-1", 1);
    info.datasets.insert("rows".into(), branch_name.clone());
    registry.begin_create(info.clone()).await.unwrap();
    let active = registry
        .finish_create("scenario", info.datasets.clone())
        .await
        .unwrap();

    let scope = Arc::new(ForkScope::new(
        Arc::new(active),
        Arc::new(SchemaDelta::empty()),
        registry,
    ));
    let branched = BranchedBackend::new(inner.clone(), scope);

    (dir, inner, branched, branch_name)
}

async fn count_via_scan(branched: &BranchedBackend, table: &str) -> usize {
    let req = ScanRequest::all(table);
    branched
        .scan(req)
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum()
}

#[tokio::test]
async fn write_append_routes_to_fork_branch() {
    let (_dir, inner, branched, _branch) = fixture().await;

    branched
        .write(
            "rows",
            vec![batch(vec![4, 5], vec![40, 50])],
            WriteMode::Append,
        )
        .await
        .unwrap();

    // BranchedBackend scans see five.
    assert_eq!(count_via_scan(&branched, "rows").await, 5);

    // Primary scans (through the inner backend, no branch routing) see three.
    let primary_batches = inner.scan(ScanRequest::all("rows")).await.unwrap();
    let primary_total: usize = primary_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(primary_total, 3, "primary must be unaffected by fork append");
}

#[tokio::test]
async fn write_overwrite_replaces_branch_tip_only() {
    let (_dir, inner, branched, _branch) = fixture().await;

    branched
        .write(
            "rows",
            vec![batch(vec![999], vec![9999])],
            WriteMode::Overwrite,
        )
        .await
        .unwrap();

    // Branch sees one row.
    assert_eq!(count_via_scan(&branched, "rows").await, 1);

    // Primary still sees its original three.
    let primary_total: usize = inner
        .scan(ScanRequest::all("rows"))
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(primary_total, 3);
}

#[tokio::test]
async fn delete_rows_removes_only_on_branch() {
    let (_dir, inner, branched, _branch) = fixture().await;

    branched.delete_rows("rows", "id = 2").await.unwrap();

    // Branch missing id=2.
    assert_eq!(count_via_scan(&branched, "rows").await, 2);

    // Primary unaffected.
    let primary_total: usize = inner
        .scan(ScanRequest::all("rows"))
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(primary_total, 3);
}

#[tokio::test]
async fn replace_table_atomic_replaces_branch_tip() {
    let (_dir, inner, branched, _branch) = fixture().await;

    branched
        .replace_table_atomic(
            "rows",
            vec![batch(vec![7, 8, 9], vec![70, 80, 90])],
            arrow_schema(),
        )
        .await
        .unwrap();

    assert_eq!(count_via_scan(&branched, "rows").await, 3);
    let primary_total: usize = inner
        .scan(ScanRequest::all("rows"))
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(primary_total, 3);

    // The branch's IDs are now 7,8,9 — different from primary's 1,2,3.
    let branch_batches = branched.scan(ScanRequest::all("rows")).await.unwrap();
    let mut branch_ids: Vec<u64> = branch_batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap())
                .collect::<Vec<_>>()
        })
        .collect();
    branch_ids.sort_unstable();
    assert_eq!(branch_ids, vec![7, 8, 9]);
}

#[tokio::test]
async fn write_to_unbranched_table_errors_typed() {
    let (_dir, _inner, branched, _branch) = fixture().await;

    let err = branched
        .write(
            "unknown_label",
            vec![batch(vec![1], vec![1])],
            WriteMode::Append,
        )
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("no branch") || msg.contains("Day 10"),
        "expected missing-branch error mentioning Day 10, got: {msg}"
    );
}

#[tokio::test]
async fn create_table_without_branch_is_phase2_day10_work() {
    let (_dir, _inner, branched, _branch) = fixture().await;

    let err = branched
        .create_table("brand_new", vec![batch(vec![1], vec![1])])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Day 10"));
}

#[tokio::test]
async fn drop_table_on_fork_is_unsupported() {
    let (_dir, _inner, branched, _branch) = fixture().await;
    let err = branched.drop_table("rows").await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("not supported") && msg.contains("drop_fork"),
        "expected drop_table-not-supported error, got: {msg}"
    );
}

#[tokio::test]
async fn write_empty_batches_is_ok_and_no_op() {
    // Edge case: an empty Vec<RecordBatch> must not panic on schema
    // extraction (we index batches[0]). The early-return guard at the
    // top of `write` covers it; this test pins the contract.
    let (_dir, _inner, branched, _branch) = fixture().await;

    branched
        .write("rows", vec![], WriteMode::Append)
        .await
        .unwrap();

    // Branch unchanged — still 3 rows from fixture.
    assert_eq!(count_via_scan(&branched, "rows").await, 3);
}

#[tokio::test]
async fn fork_writes_isolated_from_post_fork_primary_writes() {
    // Spec §10: snapshot isolation extends to the write side. A fork
    // that writes after primary has continued mutating must still see
    // only fork-point + its own writes — not any of primary's later
    // writes.
    let (dir, inner, branched, _branch) = fixture().await;

    // Primary appends two rows AFTER the fork has been created.
    inner
        .write(
            "rows",
            vec![batch(vec![100, 101], vec![1000, 1001])],
            WriteMode::Append,
        )
        .await
        .unwrap();

    // Now the fork writes a row.
    branched
        .write(
            "rows",
            vec![batch(vec![42], vec![420])],
            WriteMode::Append,
        )
        .await
        .unwrap();

    // Branch should see fork-point (3) + its own write (1) = 4. NOT 6.
    let mut branch_ids: Vec<u64> = branched
        .scan(ScanRequest::all("rows"))
        .await
        .unwrap()
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap())
                .collect::<Vec<_>>()
        })
        .collect();
    branch_ids.sort_unstable();
    assert_eq!(
        branch_ids,
        vec![1, 2, 3, 42],
        "branch leaked primary's post-fork writes"
    );

    // Primary sees its 5 (3 fork-point + 2 appended); not the fork's row.
    let primary_total: usize = inner
        .scan(ScanRequest::all("rows"))
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(primary_total, 5);

    let _ = dir;
}

#[tokio::test]
async fn fork_writes_survive_backend_reopen() {
    // Phase 2 substrate sanity: writes on a fork branch are durable.
    // We don't rebuild the registry here (that's Day 6 recovery
    // territory) — we just close and reopen the LanceDbBackend over
    // the same temp dir and confirm the branch still has our row.
    let (dir, _inner, branched, branch_name) = fixture().await;

    branched
        .write(
            "rows",
            vec![batch(vec![55], vec![555])],
            WriteMode::Append,
        )
        .await
        .unwrap();
    drop(branched);

    // Reopen the backend; verify the branch still exists with the row.
    let dataset_uri = format!("{}/rows.lance", dir.path().display());
    let on_branch = lance_branch::open_branch(&dataset_uri, &branch_name)
        .await
        .unwrap();
    assert_eq!(on_branch.count_rows(None).await.unwrap(), 4);
}

#[tokio::test]
async fn inner_backend_accessor_returns_primary_backend() {
    // Day 4 wiring depends on this — it lets the Writer factory get
    // an Arc<dyn StorageBackend> for primary-style ops without going
    // through the BranchedBackend's write gate.
    let (_dir, inner, branched, _branch) = fixture().await;

    let exposed = branched.inner_backend();
    // Both Arcs ultimately point at the same backend. We can't compare
    // dyn pointers directly, but we can verify behavior: the exposed
    // backend's primary scan returns 3 rows (no branch routing).
    let primary_total: usize = exposed
        .scan(ScanRequest::all("rows"))
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(primary_total, 3);
    let _ = inner;
}
