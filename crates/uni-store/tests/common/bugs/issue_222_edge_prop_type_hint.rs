// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #222 — a batched edge-property read scans every edge type in the
//! schema when L0 cannot resolve the EIDs' types.
//!
//! `PropertyManager::get_batch_edge_props` has to know which per-type delta
//! tables to read, because EIDs are pure auto-increment and carry no type. It
//! asked L0 for each EID's type, and **a single unresolved EID `break`s to
//! scanning every type in the schema**. L0 is empty on a reloaded or compacted
//! store, so that fallback is the normal path there, not the exceptional one:
//! measured on LDBC IC5's `HAS_MEMBER` clause at SF1, all 195 calls took it and
//! scanned all 15 edge types for 31.7 s — 20% of the clause. The cost is per
//! *call*, so it scales with call count rather than with edges.
//!
//! The fix is a caller-supplied `edge_types` hint. Three of the four production
//! callers already had the type in hand — a traversal knows the exact set from
//! its own expansions — and now pass it.
//!
//! # Why this is a contrast, not a threshold
//!
//! Following `issue_223_main_edge_scan_shape.rs`: both arms run over the same
//! EIDs in the same test and are compared to each other. A threshold ("fewer
//! than N scans") would pass if the counter silently stopped working, and would
//! need re-tuning whenever the fixture changed. A contrast fails in both
//! directions — a dead counter takes the unhinted arm to zero, and a hint that
//! is quietly ignored makes the two equal.
//!
//! The equal-results assertion is load-bearing for the same reason it is in
//! #223: it is what makes the scan count the *only* difference between the
//! arms, and therefore what licenses reading the count as a statement about
//! read strategy rather than about two different reads.

// Rust guideline compliant

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::TempDir;
use uni_common::Properties;
use uni_common::Value;
use uni_common::core::id::Eid;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_store::QueryCounters;
use uni_store::runtime::PropertyManager;
use uni_store::runtime::context::QueryContext;
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

/// Edge types declared in the schema. Only `T0` is ever written, so the
/// unhinted read's extra scans are all of empty tables — which is precisely the
/// LDBC shape, where a `HAS_MEMBER` read paid for fourteen unrelated types.
const TYPE_COUNT: usize = 6;
/// Edges written, all of type `T0`.
const EDGES: usize = 20;

/// A store with `TYPE_COUNT` declared edge types, `EDGES` edges of `T0`,
/// flushed so L0 is cold and cannot resolve any EID's type.
type Fixture = (
    TempDir,
    Arc<StorageManager>,
    Arc<SchemaManager>,
    Vec<Eid>,
    Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>,
);

async fn fixture() -> Result<Fixture> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path())?);

    let schema_manager = Arc::new(
        SchemaManager::load_from_store(store, &ObjectStorePath::from("schema.json")).await?,
    );
    schema_manager.add_label("N")?;
    let mut type_ids = Vec::new();
    for i in 0..TYPE_COUNT {
        let id = schema_manager.add_edge_type(
            &format!("T{i}"),
            vec!["N".to_string()],
            vec!["N".to_string()],
        )?;
        schema_manager.add_property(&format!("T{i}"), "w", DataType::Int, true)?;
        type_ids.push(id);
    }
    schema_manager.save().await?;

    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

    let src = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(src, HashMap::new(), &["N".to_string()], None)
        .await?;

    let mut eids = Vec::with_capacity(EDGES);
    for i in 0..EDGES {
        let dst = writer.next_vid().await?;
        writer
            .insert_vertex_with_labels(dst, HashMap::new(), &["N".to_string()], None)
            .await?;
        let mut props = Properties::new();
        props.insert("w".to_string(), Value::Int(i as i64));
        let eid = writer.next_eid(type_ids[0]).await?;
        writer
            .insert_edge(src, dst, type_ids[0], eid, props, None, None)
            .await?;
        eids.push(eid);
    }

    // One edge of every *other* type, so their delta tables actually exist.
    // Declaring a type is not enough: an unwritten type has no table, the
    // fallback skips it on `table_exists`, and the fan-out costs nothing — the
    // first version of this fixture measured 1 scan against 1 for exactly that
    // reason. LDBC's fifteen types are all populated, which is what makes the
    // fan-out expensive there.
    for (i, tid) in type_ids.iter().enumerate().skip(1) {
        let dst = writer.next_vid().await?;
        writer
            .insert_vertex_with_labels(dst, HashMap::new(), &["N".to_string()], None)
            .await?;
        let mut props = Properties::new();
        props.insert("w".to_string(), Value::Int(i as i64));
        let eid = writer.next_eid(*tid).await?;
        writer
            .insert_edge(src, dst, *tid, eid, props, None, None)
            .await?;
    }

    // Cold L0 is the whole point: without the flush every EID resolves from L0
    // and the all-types fallback is never reached, so both arms below would be
    // identical and the test would prove nothing.
    writer.flush_to_l1(None).await?;

    // Handed back so both arms share one (now-empty) L0: the fallback under
    // test is exactly what a cold L0 triggers.
    let l0 = writer.l0_manager.get_current();
    Ok((temp_dir, storage, schema_manager, eids, l0))
}

/// A `QueryContext` over the fixture's cold L0, carrying its own counters.
fn ctx_with_counters(
    l0: &Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>,
    counters: &Arc<QueryCounters>,
) -> QueryContext {
    let mut ctx = QueryContext::new(l0.clone());
    ctx.counters = Some(counters.clone());
    ctx
}

/// A hinted read scans strictly fewer tables than an unhinted one, and returns
/// the same properties.
#[tokio::test]
async fn a_type_hint_avoids_scanning_every_edge_type() -> Result<()> {
    let (_tmp, storage, schema_manager, eids, l0) = fixture().await?;
    let pm = PropertyManager::new(storage.clone(), schema_manager, 0);

    // Arm 1: no hint. L0 is cold, so this takes the all-types fallback.
    let unhinted_counters = Arc::new(QueryCounters::new());
    let unhinted_ctx = ctx_with_counters(&l0, &unhinted_counters);
    let unhinted = pm
        .get_batch_edge_props(&eids, &["w"], None, Some(&unhinted_ctx))
        .await?;

    // Arm 2: the type the caller knows.
    let hinted_counters = Arc::new(QueryCounters::new());
    let hinted_ctx = ctx_with_counters(&l0, &hinted_counters);
    let hinted_types = vec!["T0".to_string()];
    let hinted = pm
        .get_batch_edge_props(&eids, &["w"], Some(&hinted_types), Some(&hinted_ctx))
        .await?;

    // The premise. If the two arms disagree the scan comparison below is
    // comparing different reads and says nothing about strategy.
    assert_eq!(
        unhinted, hinted,
        "the hint must not change the answer — only how many tables are read"
    );
    assert_eq!(
        hinted.len(),
        EDGES,
        "fixture did not resolve every edge; both arms are reading nothing"
    );

    let unhinted_scans = unhinted_counters.scans_reported();
    let hinted_scans = hinted_counters.scans_reported();
    assert!(
        unhinted_scans > 0,
        "the unhinted arm reported zero scans, so the counter is not observing \
         this path and the comparison below cannot mean anything"
    );
    assert!(
        hinted_scans < unhinted_scans,
        "a hinted read of one edge type must scan fewer tables than an unhinted \
         one over {TYPE_COUNT} declared types: hinted {hinted_scans}, unhinted \
         {unhinted_scans}. Equal means the hint is being ignored and #222's \
         all-types fan-out is still being paid."
    );
    Ok(())
}

/// An empty hint means "no hint", not "no types".
///
/// A caller that computes its type set from an empty expansion list would
/// otherwise silently read no properties at all — a wrong answer rather than a
/// slow one, which is the worse failure of the two.
#[tokio::test]
async fn an_empty_hint_is_treated_as_no_hint() -> Result<()> {
    let (_tmp, storage, schema_manager, eids, _l0) = fixture().await?;
    let pm = PropertyManager::new(storage.clone(), schema_manager, 0);

    let empty: Vec<String> = Vec::new();
    let with_empty = pm
        .get_batch_edge_props(&eids, &["w"], Some(&empty), None)
        .await?;
    let with_none = pm.get_batch_edge_props(&eids, &["w"], None, None).await?;

    assert_eq!(
        with_empty, with_none,
        "an empty hint must fall back to resolving types, not scan nothing"
    );
    assert_eq!(
        with_empty.len(),
        EDGES,
        "an empty hint returned {} of {EDGES} edges — it was treated as \
         'scan no types' and silently lost every property",
        with_empty.len()
    );
    Ok(())
}
