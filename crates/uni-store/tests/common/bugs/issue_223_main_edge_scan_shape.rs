// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #223 — main-edge scans reach `scans_reported`, so the scan-count
//! *shape* of an edge-property read is finally assertable.
//!
//! #218 was a loop issuing one storage scan per edge, ~1.5 ms each. The batched
//! and per-item forms **return identical answers**, so no correctness test
//! distinguishes them; the regression guard has to be a count or a scaling
//! assertion. It could not be written, because `MainEdgeDataset::execute_query`
//! built every `ScanRequest` with `counters: None`.
//!
//! A `None` there is not merely an unrecorded count. `attach_scan_stats`
//! (`backend/lance.rs`) early-returns without registering the stats callback at
//! all, so the scan was *unobservable* rather than uncounted — nothing
//! downstream could have attributed it after the fact.
//!
//! `the_counter_distinguishes_per_edge_from_batched` is the assertion #223 was
//! filed to make possible, and it is deliberately written as a contrast rather
//! than a threshold: it runs both shapes over the same EIDs in the same test and
//! compares them. That way it cannot pass by measuring nothing — a counter that
//! stopped working takes the per-edge arm to zero and fails the test, and so
//! does one that counts both arms identically.

use anyhow::Result;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use uni_common::Properties;
use uni_common::Value;
use uni_common::core::id::Eid;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_store::QueryCounters;
use uni_store::runtime::writer::Writer;
use uni_store::storage::MainEdgeDataset;
use uni_store::storage::manager::StorageManager;

/// Enough edges that a per-edge loop is unmistakable against a batched read,
/// and small enough to stay fast unoptimized in CI.
const EDGES: usize = 60;

/// A hub with `EDGES` outgoing `KNOWS` edges, flushed so the properties are
/// dual-written into the main edge table.
async fn fixture() -> Result<(TempDir, Arc<StorageManager>, Vec<Eid>)> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path())?);

    let schema_manager = Arc::new(
        SchemaManager::load_from_store(store, &ObjectStorePath::from("schema.json")).await?,
    );
    schema_manager.add_label("Person")?;
    let edge_type_id = schema_manager.add_edge_type(
        "KNOWS",
        vec!["Person".to_string()],
        vec!["Person".to_string()],
    )?;
    schema_manager.add_property("KNOWS", "since", DataType::Int, true)?;
    schema_manager.save().await?;

    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

    let hub = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(hub, HashMap::new(), &["Person".to_string()], None)
        .await?;

    let mut eids = Vec::with_capacity(EDGES);
    for i in 0..EDGES {
        let dst = writer.next_vid().await?;
        writer
            .insert_vertex_with_labels(dst, HashMap::new(), &["Person".to_string()], None)
            .await?;
        let mut props = Properties::new();
        props.insert("since".to_string(), Value::Int(2000 + i as i64));
        let eid = writer.next_eid(edge_type_id).await?;
        writer
            .insert_edge(hub, dst, edge_type_id, eid, props, None, None)
            .await?;
        eids.push(eid);
    }

    // Dual-writes Delta L1 and main_edges; without it every read below is
    // served from L0 and touches no Lance scan at all.
    writer.flush_to_l1(None).await?;

    Ok((temp_dir, storage, eids))
}

/// The denominator: a batched main-edge read reports at least one scan.
///
/// Before #223 this was `0`, which is what made every scaling assertion about
/// this path vacuous — a comparison of zero against zero holds for any shape.
#[tokio::test]
async fn batched_main_edge_reads_are_reported() -> Result<()> {
    let (_tmp, storage, eids) = fixture().await?;

    let counters = Arc::new(QueryCounters::new());
    let props = MainEdgeDataset::find_props_by_eids_counted(
        storage.backend(),
        &eids,
        None,
        Some(&counters),
    )
    .await?;

    assert_eq!(props.len(), EDGES, "fixture did not resolve every edge");
    assert!(
        counters.scans_reported() > 0,
        "a batched main-edge read of {EDGES} edges reported zero Lance scans, \
         so main-edge scans are still invisible to `scans_reported` and no shape \
         assertion about them can mean anything"
    );
    Ok(())
}

/// The assertion #223 was filed to make possible: the counter tells the
/// per-edge loop apart from the batched read.
///
/// Both arms resolve the same EIDs and must agree on every value — that
/// equality is what makes the scan counts the *only* difference between them,
/// and therefore what makes the count a legitimate guard on a refactor that
/// cannot be caught by comparing results.
#[tokio::test]
async fn the_counter_distinguishes_per_edge_from_batched() -> Result<()> {
    let (_tmp, storage, eids) = fixture().await?;
    let backend = storage.backend();

    // Arm 1: the shape #218 removed — one `ScanRequest` per edge.
    let per_edge = Arc::new(QueryCounters::new());
    let mut per_edge_props = HashMap::new();
    for eid in &eids {
        let p = MainEdgeDataset::find_props_by_eid_counted(backend, *eid, None, Some(&per_edge))
            .await?
            .expect("every flushed edge has main-table properties");
        per_edge_props.insert(*eid, p);
    }

    // Arm 2: the shape it was replaced with.
    let batched = Arc::new(QueryCounters::new());
    let batched_props =
        MainEdgeDataset::find_props_by_eids_counted(backend, &eids, None, Some(&batched)).await?;

    // The premise: identical answers. If this ever fails, the scan-count
    // comparison below is comparing two different reads and means nothing.
    assert_eq!(
        per_edge_props, batched_props,
        "the two read shapes disagreed, so the scan counts below are not \
         measuring the same work"
    );

    let per_edge_scans = per_edge.scans_reported();
    let batched_scans = batched.scans_reported();
    eprintln!(
        "issue #223: {EDGES} edges — per-edge arm {per_edge_scans} scans, \
         batched arm {batched_scans} scans"
    );

    assert_eq!(
        per_edge_scans, EDGES as u64,
        "the per-edge arm should report exactly one scan per edge; \
         got {per_edge_scans} for {EDGES} edges"
    );
    assert!(
        batched_scans > 0,
        "the batched arm reported no scans at all, so this test measured nothing"
    );
    assert!(
        batched_scans * 4 < per_edge_scans,
        "the batched read cost {batched_scans} scans against the per-edge loop's \
         {per_edge_scans} for the same {EDGES} edges — not the order-of-magnitude \
         gap that distinguishes the two shapes"
    );
    Ok(())
}
