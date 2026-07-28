// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro: edge properties vanish from `get_batch_edge_props` after compaction.
//!
//! `compact_adjacency` folds topology into the L2 table — which carries only
//! `src_vid` / `neighbors` / `edge_ids` — and then physically deletes the delta
//! rows it incorporated, on the stated invariant that "edge properties survive
//! in main_edges (dual-written during flush)". Both sibling readers honour that:
//! the single-EID path and the per-type batch path each fall back to
//! `MainEdgeDataset`. `get_batch_edge_props` did not, so from the first
//! compaction onward it returned nothing for every EID and never recovered.
//!
//! It surfaced as a *silent* wrong answer several layers up. A GraphCompute
//! projection materializes edge properties through this function; a missing one
//! becomes `NaN`; `edge_mask_window` compares `v >= lo && v <= hi`, which NaN
//! fails under **any** window — so every masked traversal read zero after a few
//! hundred write transactions, with nothing raised. Weighted algorithms silently
//! fell back to unit weights at the same instant.

#![cfg(feature = "lance-backend")]

use std::collections::HashMap;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::tempdir;
use uni_common::Value;
use uni_common::core::schema::SchemaManager;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::compaction::Compactor;
use uni_store::storage::manager::StorageManager;

#[tokio::test]
async fn edge_properties_survive_adjacency_compaction() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

    let schema_manager = Arc::new(
        SchemaManager::load_from_store(store, &ObjectStorePath::from("schema.json"))
            .await
            .unwrap(),
    );
    schema_manager.add_label("Person").unwrap();
    let edge_type_id = schema_manager
        .add_edge_type(
            "KNOWS",
            vec!["Person".to_string()],
            vec!["Person".to_string()],
        )
        .unwrap();
    schema_manager
        .add_property("KNOWS", "sel", uni_common::DataType::Float, true)
        .unwrap();
    schema_manager.save().await.unwrap();

    let storage = Arc::new(
        StorageManager::new(path, schema_manager.clone())
            .await
            .unwrap(),
    );
    let writer = Writer::new(storage.clone(), schema_manager.clone(), 1)
        .await
        .unwrap();

    let vid_a = writer.next_vid().await.unwrap();
    let vid_b = writer.next_vid().await.unwrap();
    for vid in [vid_a, vid_b] {
        writer
            .insert_vertex_with_labels(vid, HashMap::new(), &["Person".to_string()], None)
            .await
            .unwrap();
    }
    let eid = writer.next_eid(edge_type_id).await.unwrap();
    let mut props = HashMap::new();
    props.insert("sel".to_string(), Value::Float(1.0));
    writer
        .insert_edge(vid_a, vid_b, edge_type_id, eid, props, None, None)
        .await
        .unwrap();
    writer.flush_to_l1(None).await.unwrap();

    let pm = PropertyManager::new(storage.clone(), schema_manager.clone(), 1000);
    let names = ["sel"];

    // Before compaction the delta rows still hold the property.
    let before = pm.get_batch_edge_props(&[eid], &names, None).await.unwrap();
    assert!(
        !before.is_empty(),
        "sanity: the property must resolve before compaction"
    );

    // Compaction moves topology to L2 and deletes the delta rows.
    Compactor::new(storage.clone())
        .compact_adjacency("KNOWS", "Person", "fwd")
        .await
        .unwrap();

    let after = pm.get_batch_edge_props(&[eid], &names, None).await.unwrap();
    let key = uni_common::core::id::Vid::from(eid.as_u64());
    let got = after
        .get(&key)
        .and_then(|p| p.get("sel"))
        .and_then(uni_common::Value::as_f64);
    assert_eq!(
        got,
        Some(1.0),
        "edge properties must survive compaction via the main-edges fallback; \
         returning nothing here reads as NaN upstream and silently empties every \
         edge mask"
    );
}
