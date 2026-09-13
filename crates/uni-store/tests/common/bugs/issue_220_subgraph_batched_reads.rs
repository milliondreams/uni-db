// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #220, site 1 — `load_subgraph`'s BFS issued one adjacency scan per
//! (vertex, label) and one delta scan per vertex, per hop.
//!
//! Both batched primitives already existed and had **zero callers anywhere in
//! the workspace**: `AdjacencyDataset::read_adjacency_backend_batch` and
//! `DeltaDataset::read_deltas_batch`. They were written and never wired up,
//! which is the sharpest form this defect class takes — the fix is not a new
//! primitive but a call.
//!
//! # Why the observable is a backend decorator
//!
//! `QueryCounters` cannot see this path: the adjacency and delta reads build
//! their `ScanRequest`s without counters, so the query-level counter reports
//! zero however many round-trips happen. Counting at the `StorageBackend`
//! instead observes what actually reaches storage, whoever built the request.
//!
//! # Why the assertion is invariance, not a threshold
//!
//! A frontier of N and one of 2N cover the same edge types and the same
//! labels, so a batched BFS issues *identical* round-trip counts for both. A
//! per-vertex one issues twice as many. Comparing the two arms fails in both
//! directions — a dead counter takes both to zero and trips the liveness
//! assertion, and a regression to per-vertex reads makes the second arm grow.
//! A threshold would instead need re-tuning whenever the fixture changed.

// Rust guideline compliant
#![cfg(feature = "lance-backend")]

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::TempDir;
use uni_common::Properties;
use uni_common::config::UniConfig;
use uni_common::core::id::Vid;
use uni_common::core::schema::SchemaManager;
use uni_common::graph::simple_graph::Direction as GraphDirection;
use uni_store::backend::lance::LanceDbBackend;
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

use super::fault_backend::FaultBackend;

/// Source vertices in the small arm. The large arm uses twice this many.
const SOURCES: usize = 8;

/// A store of `2 * SOURCES` sources, each with one outgoing `E` edge, flushed
/// so every read must reach storage rather than L0.
async fn fixture() -> Result<(
    TempDir,
    Arc<StorageManager>,
    Arc<FaultBackend>,
    Vec<Vid>,
    u32,
)> {
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap().to_string();

    let lance = LanceDbBackend::connect(&uri, None).await?;
    let fault = Arc::new(FaultBackend::new(Arc::new(lance)));

    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_manager = Arc::new(
        SchemaManager::load_from_store(store.clone(), &ObjectStorePath::from("schema.json"))
            .await?,
    );
    schema_manager.add_label("N")?;
    let etype = schema_manager.add_edge_type("E", vec!["N".to_string()], vec!["N".to_string()])?;
    schema_manager.save().await?;

    let storage = Arc::new(
        StorageManager::new_with_backend(
            &uri,
            store,
            fault.clone(),
            schema_manager.clone(),
            UniConfig::default(),
        )
        .await?,
    );
    let writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

    let mut sources = Vec::with_capacity(SOURCES * 2);
    for _ in 0..SOURCES * 2 {
        let src = writer.next_vid().await?;
        writer
            .insert_vertex_with_labels(src, HashMap::new(), &["N".to_string()], None)
            .await?;
        let dst = writer.next_vid().await?;
        writer
            .insert_vertex_with_labels(dst, HashMap::new(), &["N".to_string()], None)
            .await?;
        let eid = writer.next_eid(etype).await?;
        writer
            .insert_edge(src, dst, etype, eid, Properties::new(), None, None)
            .await?;
        sources.push(src);
    }

    // Cold L0: with the edges still buffered, `load_subgraph` answers from
    // memory and issues no storage reads at all, so both arms would be zero.
    writer.flush_to_l1(None).await?;

    Ok((dir, storage, fault, sources, etype))
}

/// Storage round-trips do not grow with the size of the BFS frontier.
#[tokio::test]
async fn a_subgraph_load_scans_per_edge_type_not_per_vertex() -> Result<()> {
    let (_dir, storage, fault, sources, etype) = fixture().await?;

    fault.reset_scans();
    let small = storage
        .load_subgraph(
            &sources[..SOURCES],
            &[etype],
            1,
            GraphDirection::Outgoing,
            None,
        )
        .await?;
    let small_scans = fault.scans();

    fault.reset_scans();
    let large = storage
        .load_subgraph(&sources, &[etype], 1, GraphDirection::Outgoing, None)
        .await?;
    let large_scans = fault.scans();

    // The premise: both arms actually loaded their edges. Without this the
    // scan comparison could be comparing two reads that found nothing.
    assert_eq!(
        small.edge_count(),
        SOURCES,
        "small arm loaded {} edges, expected {SOURCES} — it read nothing and \
         the scan counts below mean nothing",
        small.edge_count()
    );
    assert_eq!(
        large.edge_count(),
        SOURCES * 2,
        "large arm loaded {} edges, expected {}",
        large.edge_count(),
        SOURCES * 2
    );

    assert!(
        small_scans > 0,
        "the small arm issued zero storage scans, so the decorator is not \
         observing this path and the comparison below cannot mean anything"
    );
    assert_eq!(
        small_scans,
        large_scans,
        "a frontier of {} vertices issued {small_scans} storage scans and one \
         of {} issued {large_scans}. The reads are batched per (edge type, \
         label), so doubling the frontier must not change the count — growth \
         means the BFS is back to one round-trip per vertex (#220)",
        SOURCES,
        SOURCES * 2
    );
    Ok(())
}

/// A batch insert probes flushed `ext_id`s once for the batch, not once per
/// vertex (#220, site 5).
///
/// Unlike the other sites in this class, this one had no batched primitive to
/// call — `MainVertexDataset::find_by_ext_ids_counted` is new. The probe backs
/// a uniqueness constraint, so the test asserts on the *answer* as well as the
/// shape: an insert whose `ext_id` already exists on disk must still be
/// rejected, and rejected at the right index.
#[tokio::test]
async fn a_batch_insert_probes_ext_ids_once_for_the_batch() -> Result<()> {
    use uni_common::Value;

    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap().to_string();
    let lance = LanceDbBackend::connect(&uri, None).await?;
    let fault = Arc::new(FaultBackend::new(Arc::new(lance)));
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_manager = Arc::new(
        SchemaManager::load_from_store(store.clone(), &ObjectStorePath::from("schema.json")).await?,
    );
    schema_manager.add_label("N")?;
    schema_manager.save().await?;
    let storage = Arc::new(
        StorageManager::new_with_backend(
            &uri,
            store,
            fault.clone(),
            schema_manager.clone(),
            UniConfig::default(),
        )
        .await?,
    );
    let writer = Writer::new(storage, schema_manager, 1).await?;

    // One flushed vertex, so the main-table probe has something to find and is
    // not trivially skipped on an absent table.
    let seed = writer.next_vid().await?;
    let mut seed_props = Properties::new();
    seed_props.insert("ext_id".to_string(), Value::String("seed".to_string()));
    writer
        .insert_vertex_with_labels(seed, seed_props, &["N".to_string()], None)
        .await?;
    writer.flush_to_l1(None).await?;

    let batch = |n: usize, offset: usize| -> (Vec<Properties>, Vec<String>) {
        let props = (0..n)
            .map(|i| {
                let mut p = Properties::new();
                p.insert(
                    "ext_id".to_string(),
                    Value::String(format!("e{}", i + offset)),
                );
                p
            })
            .collect();
        (props, vec!["N".to_string()])
    };

    let (small_props, labels) = batch(SOURCES, 0);
    let mut small_vids = Vec::new();
    for _ in 0..SOURCES {
        small_vids.push(writer.next_vid().await?);
    }
    fault.reset_scans();
    writer
        .insert_vertices_batch(small_vids, small_props, labels.clone(), None)
        .await?;
    let small_scans = fault.scans();

    let (large_props, _) = batch(SOURCES * 2, 1000);
    let mut large_vids = Vec::new();
    for _ in 0..SOURCES * 2 {
        large_vids.push(writer.next_vid().await?);
    }
    fault.reset_scans();
    writer
        .insert_vertices_batch(large_vids, large_props, labels.clone(), None)
        .await?;
    let large_scans = fault.scans();

    assert!(
        small_scans > 0,
        "the small batch issued zero storage scans, so the decorator is not \
         observing the ext_id probe and the comparison below cannot mean anything"
    );
    assert_eq!(
        small_scans, large_scans,
        "a batch of {} issued {small_scans} storage scans and one of {} issued \
         {large_scans}. The ext_id probe is batched, so doubling the batch must \
         not change the count — growth means it is back to one probe per vertex",
        SOURCES,
        SOURCES * 2
    );

    // The probe must still answer correctly: "seed" is on disk, so an insert
    // carrying it is a constraint violation. A batched probe that quietly
    // returned nothing would pass the shape assertion above and admit this.
    let dup_vid = writer.next_vid().await?;
    let mut dup = Properties::new();
    dup.insert("ext_id".to_string(), Value::String("seed".to_string()));
    let err = writer
        .insert_vertices_batch(vec![dup_vid], vec![dup], labels, None)
        .await
        .expect_err("a flushed duplicate ext_id must be rejected");
    assert!(
        err.to_string().contains("already exists"),
        "expected a uniqueness violation, got: {err}"
    );
    Ok(())
}
