// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for compaction.rs:511 (finding [11]).
//!
//! `compact_adjacency` used to skip the L2 table replace when the compacted
//! output was empty (`if !src_vid_builder.is_empty()`), yet still clear the
//! Delta L1 tombstones (`delete_up_to_version`). When a delete removed the last
//! edge of every vertex, the stale L2 adjacency rows survived while their delete
//! tombstones were erased — so subsequent reads (which union L2 + Delta L1)
//! resurrected the deleted edge.
//!
//! Fixed (compaction.rs:511): the L2 replace now always runs (writing an empty
//! batch when the compacted output is empty), so the stale rows are overwritten
//! before the tombstone-clear. This test drives the exact empty-output second
//! compaction and asserts the deleted edge stays gone.

#![cfg(feature = "lance-backend")]

use std::collections::HashMap;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::tempdir;
use uni_common::core::schema::SchemaManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::compaction::Compactor;
use uni_store::storage::direction::Direction;
use uni_store::storage::manager::StorageManager;

#[tokio::test]
async fn repro_empty_output_compaction_resurrects_deleted_edge() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let schema_path = ObjectStorePath::from("schema.json");

    let schema_manager = Arc::new(
        SchemaManager::load_from_store(store, &schema_path)
            .await
            .unwrap(),
    );
    let _label_id = schema_manager.add_label("Person").unwrap();
    let edge_type_id = schema_manager
        .add_edge_type(
            "KNOWS",
            vec!["Person".to_string()],
            vec!["Person".to_string()],
        )
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

    // 1. Insert A -[e1]-> B, flush, compact so e1 lands in the L2 adjacency table.
    let vid_a = writer.next_vid().await.unwrap();
    let vid_b = writer.next_vid().await.unwrap();
    writer
        .insert_vertex_with_labels(vid_a, HashMap::new(), &["Person".to_string()], None)
        .await
        .unwrap();
    writer
        .insert_vertex_with_labels(vid_b, HashMap::new(), &["Person".to_string()], None)
        .await
        .unwrap();
    let eid1 = writer.next_eid(edge_type_id).await.unwrap();
    writer
        .insert_edge(vid_a, vid_b, edge_type_id, eid1, HashMap::new(), None, None)
        .await
        .unwrap();
    writer.flush_to_l1(None).await.unwrap();

    let compactor = Compactor::new(storage.clone());
    compactor
        .compact_adjacency("KNOWS", "Person", "fwd")
        .await
        .unwrap();

    // 2. Delete e1, flush -> Delta L1 now holds Delete(e1); L2 still lists e1.
    writer
        .delete_edge(eid1, vid_a, vid_b, edge_type_id, None)
        .await
        .unwrap();
    writer.flush_to_l1(None).await.unwrap();

    // 3. Compact again. All edges deleted -> compacted output is empty, so the
    //    L2 replace is SKIPPED but the delete deltas are cleared.
    compactor
        .compact_adjacency("KNOWS", "Person", "fwd")
        .await
        .unwrap();

    // 4. Re-warm from storage (L2 + Delta L1) and read A's outgoing edges.
    let storage2 = Arc::new(
        StorageManager::new(path, schema_manager.clone())
            .await
            .unwrap(),
    );
    let am2 = storage2.adjacency_manager();
    am2.warm(&storage2, edge_type_id, Direction::Outgoing, None)
        .await
        .unwrap();
    let neighbors = am2.get_neighbors(vid_a, edge_type_id, Direction::Outgoing);

    // Fixed (compaction.rs:511): 0 neighbors — the empty-output compaction now
    // overwrites the stale L2 row before clearing the delete tombstone, so the
    // deleted edge stays deleted.
    assert_eq!(
        neighbors.len(),
        0,
        "deleted edge must stay deleted after empty-output compaction; got {neighbors:?}"
    );
}
