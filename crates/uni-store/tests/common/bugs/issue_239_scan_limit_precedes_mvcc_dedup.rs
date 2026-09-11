// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #239 — `ScanRequest::limit` truncates *before* MVCC dedup, so it
//! cannot be used to implement Cypher `LIMIT`.
//!
//! #239 observes that `with_limit` has zero callers and proposes threading a
//! Cypher `LIMIT` into `ScanRequest`. This test exists to show why that fix
//! direction is unsound, and to keep it unsound-by-demonstration rather than
//! by comment.
//!
//! # The pipeline
//!
//! A labelled vertex read is three stages, and the limit lands on the first:
//!
//! ```text
//! ScanRequest          raw Lance rows, filtered only by `_version <= hwm`
//!   -> mvcc_dedup_batch      sort (_vid ASC, _version DESC), keep first per vid
//!   -> merge_lance_and_l0    concat the L0 batch, dedup again
//! ```
//!
//! `ScanRequest::limit` is applied by the Lance scanner
//! (`backend/lance.rs`, `scanner.limit(...)` after `project` and `filter`), so
//! it cuts the row set while *every version of every vid is still present*.
//! Two distinct failures follow, and this test pins both:
//!
//! 1. **Short read.** N raw rows can hold fewer than N distinct vids, because
//!    an updated vertex contributes one row per version. `limit(2)` over a
//!    table holding two versions of the same vertex yields one vertex.
//! 2. **Stale read.** Lance returns rows in write order, so the truncation
//!    keeps the *oldest* surviving version and drops the newest. Dedup then has
//!    only the stale row to choose from and faithfully returns it. There is no
//!    error — the query answers with a value that was overwritten.
//!
//! (2) is the dangerous one: (1) returns too little and might be noticed, while
//! (2) is a well-formed wrong answer.
//!
//! **Measured**, on the fixture below: the three physical rows come back in
//! write order as `[(B,2), (A,1), (A,100)]`, so `limit(2)` yields
//! `[(B,2), (A,1)]` — two distinct vids, but `A` carrying the value it held
//! before it was overwritten. This lands on failure (2): the count is right and
//! the answer is stale.
//!
//! # The fixture needs two flushes, and the control is why we know
//!
//! The first version of this test wrote A, B and then A-again inside a single
//! flush. Its control failed: only **two** physical rows reached Lance, because
//! two writes to one vid coalesce in L0 and are flushed as one row. The
//! multi-version condition the hazard depends on was never set up, so the main
//! assertion would have been reporting on a fixture that could not show the
//! defect either way. Overwriting in a *separate* flush is what appends a
//! second row. Keep the control: it is the only thing standing between this
//! test and passing for the wrong reason.
//!
//! # This class already shipped once
//!
//! `with_limit`'s only historical caller was `rebuild_vid_labels_index`, which
//! carried `.with_limit(100_000)` and was removed for #211 — truncating that
//! scan left every vertex past the cap out of the index, and the traversal
//! label filter *keeps* unresolved rows, so a `Post` predicate admitted
//! `Comment`s at LDBC SF1. Same mechanism, different consumer. See
//! `test_issue_211_vid_labels_index_rebuild.rs`.
//!
//! # What this does not claim
//!
//! It does not claim a Cypher `LIMIT` cannot be optimised — only that it cannot
//! be optimised *here*, below dedup. A sound pushdown has to act above the
//! merge, where a vid appears exactly once.

// Rust guideline compliant

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use arrow_array::{Array, Int64Array, UInt64Array};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::tempdir;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_store::backend::table_names;
use uni_store::backend::types::ScanRequest;
use uni_store::runtime::Writer;
use uni_store::storage::manager::StorageManager;

/// Write `n = 1` for vertex A and `n = 2` for vertex B, then overwrite A with
/// `n = 100`. Flushed, so all three rows are in Lance and none is in L0.
///
/// Returns `(path, dir_guard, vid_a)`.
async fn store_with_an_overwritten_vertex() -> Result<(tempfile::TempDir, Arc<SchemaManager>, u64)>
{
    let dir = tempdir()?;
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_manager = Arc::new(
        SchemaManager::load_from_store(store, &ObjectStorePath::from("schema.json")).await?,
    );
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "n", DataType::Int64, false)?;
    schema_manager.save().await?;

    let path = dir.path().to_str().unwrap().to_string();
    let storage = Arc::new(StorageManager::new(&path, schema_manager.clone()).await?);
    let writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

    let vid_a = writer.next_vid().await?;
    let mut props = HashMap::new();
    props.insert("n".to_string(), 1i64.into());
    writer
        .insert_vertex_with_labels(vid_a, props, &["Person".to_string()], None)
        .await?;

    let vid_b = writer.next_vid().await?;
    let mut props = HashMap::new();
    props.insert("n".to_string(), 2i64.into());
    writer
        .insert_vertex_with_labels(vid_b, props, &["Person".to_string()], None)
        .await?;

    // Flush before overwriting. Two writes to one vid inside a *single* flush
    // coalesce in L0 and reach Lance as one row — the first version of this
    // fixture did that and its control caught it. A second flush is what
    // actually appends a second physical row for the same `_vid`.
    writer.flush_to_l1(None).await?;

    // Overwrite A. Now in its own flush, so it appends a second,
    // higher-`_version` row for the same `_vid` rather than replacing the
    // first — which is exactly what makes a pre-dedup limit unsafe.
    let mut props = HashMap::new();
    props.insert("n".to_string(), 100i64.into());
    writer
        .insert_vertex_with_labels(vid_a, props, &["Person".to_string()], None)
        .await?;

    writer.flush_to_l1(None).await?;
    drop(writer);
    drop(storage);

    Ok((dir, schema_manager, vid_a.as_u64()))
}

/// Read `(_vid, n)` pairs straight off the backend, optionally limited.
async fn raw_scan(storage: &StorageManager, limit: Option<usize>) -> Result<Vec<(u64, i64)>> {
    let table = table_names::vertex_table_name("Person");
    let mut request = ScanRequest::all(&table).with_columns(vec![
        "_vid".to_string(),
        "_version".to_string(),
        "n".to_string(),
    ]);
    if let Some(l) = limit {
        request = request.with_limit(l);
    }
    let batches = storage.backend().scan(request).await?;
    let mut out = Vec::new();
    for b in &batches {
        let vids = b
            .column_by_name("_vid")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>().cloned())
            .expect("_vid is UInt64");
        let ns = b
            .column_by_name("n")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>().cloned())
            .expect("n is Int64");
        for i in 0..b.num_rows() {
            out.push((vids.value(i), ns.value(i)));
        }
    }
    Ok(out)
}

/// The unlimited scan is the control: it must show three physical rows for two
/// logical vertices. If this ever shows two, the fixture stopped exercising
/// multi-version storage and the two assertions below prove nothing.
#[tokio::test]
async fn the_unlimited_scan_sees_every_version() -> Result<()> {
    let (dir, sm, vid_a) = store_with_an_overwritten_vertex().await?;
    let storage = StorageManager::new(dir.path().to_str().unwrap(), sm).await?;

    let rows = raw_scan(&storage, None).await?;
    assert_eq!(
        rows.len(),
        3,
        "control: two vertices, one of them written twice, must be three \
         physical rows before dedup. Got {rows:?}"
    );
    let a_versions: Vec<i64> = rows
        .iter()
        .filter(|(v, _)| *v == vid_a)
        .map(|(_, n)| *n)
        .collect();
    assert!(
        a_versions.contains(&1) && a_versions.contains(&100),
        "control: both versions of the overwritten vertex must be present, \
         got {a_versions:?}"
    );
    Ok(())
}

/// `limit(2)` over three physical rows covering two vertices yields fewer than
/// two distinct vertices, and drops the newest value of the one it keeps.
///
/// This is the assertion that makes #239's proposed fix direction unsafe.
#[tokio::test]
async fn a_scan_limit_truncates_before_dedup_and_can_return_a_stale_row() -> Result<()> {
    let (dir, sm, vid_a) = store_with_an_overwritten_vertex().await?;
    let storage = StorageManager::new(dir.path().to_str().unwrap(), sm).await?;

    let all = raw_scan(&storage, None).await?;
    let limited = raw_scan(&storage, Some(2)).await?;

    assert_eq!(
        limited.len(),
        2,
        "the limit reached Lance: two physical rows came back, not {}",
        all.len()
    );

    // Failure 1 — short read: the two rows do not cover two distinct vertices.
    let distinct: std::collections::HashSet<u64> = limited.iter().map(|(v, _)| *v).collect();
    // Failure 2 — stale read: if the newest version of A was cut, dedup over
    // this truncated set returns `n = 1` for a vertex whose value is 100.
    let a_after_limit: Vec<i64> = limited
        .iter()
        .filter(|(v, _)| *v == vid_a)
        .map(|(_, n)| *n)
        .collect();
    let stale = a_after_limit.contains(&1) && !a_after_limit.contains(&100);

    assert!(
        distinct.len() < 2 || stale,
        "a pre-dedup limit must be demonstrably lossy, otherwise this test is \
         not pinning the hazard #239 needs to avoid. Got rows {limited:?} \
         covering {} distinct vids (all rows: {all:?}). If storage started \
         returning rows newest-version-first, or stopped appending a new row \
         per write, re-derive the hazard before trusting `with_limit`.",
        distinct.len()
    );

    Ok(())
}
