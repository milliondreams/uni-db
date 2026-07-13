// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Temporary repro: does compact_adjacency resurrect deleted edges when the
//! compacted output is empty (all edges deleted)?

use anyhow::Result;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_common::core::schema::SchemaManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::compaction::Compactor;
use uni_store::storage::direction::Direction;
use uni_store::storage::manager::StorageManager;

#[tokio::test]
async fn test_delete_all_edges_then_compact_does_not_resurrect() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");

    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    let _label_id = schema_manager.add_label("Person")?;
    let edge_type_id = schema_manager.add_edge_type(
        "KNOWS",
        vec!["Person".to_string()],
        vec!["Person".to_string()],
    )?;
    schema_manager.save().await?;

    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

    // 1. Insert A -[e1]-> B, flush, compact so e1 lands in the L2 adjacency table.
    let vid_a = writer.next_vid().await?;
    let vid_b = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(vid_a, HashMap::new(), &["Person".to_string()], None)
        .await?;
    writer
        .insert_vertex_with_labels(vid_b, HashMap::new(), &["Person".to_string()], None)
        .await?;
    let eid1 = writer.next_eid(edge_type_id).await?;
    writer
        .insert_edge(vid_a, vid_b, edge_type_id, eid1, HashMap::new(), None, None)
        .await?;
    writer.flush_to_l1(None).await?;

    let compactor = Compactor::new(storage.clone());
    compactor
        .compact_adjacency("KNOWS", "Person", "fwd")
        .await?;

    // Sanity: edge visible after first compaction.
    let am = storage.adjacency_manager();
    am.warm(&storage, edge_type_id, Direction::Outgoing, None)
        .await?;
    let n = am.get_neighbors(vid_a, edge_type_id, Direction::Outgoing);
    assert_eq!(n.len(), 1, "edge should exist after insert+compact");

    // 2. Delete e1, flush -> Delta L1 now holds Delete(e1); L2 still lists e1.
    writer
        .delete_edge(eid1, vid_a, vid_b, edge_type_id, None)
        .await?;
    writer.flush_to_l1(None).await?;

    // 3. Compact again. All edges deleted -> compacted output is empty.
    // NEGATIVE CONTROL: second compaction disabled; delta should mask the edge.
    // compactor
    //     .compact_adjacency("KNOWS", "Person", "fwd")
    //     .await?;

    // 4. Re-warm from storage (L2 + Delta L1) and read.
    let storage2 = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let am2 = storage2.adjacency_manager();
    am2.warm(&storage2, edge_type_id, Direction::Outgoing, None)
        .await?;
    let neighbors = am2.get_neighbors(vid_a, edge_type_id, Direction::Outgoing);
    assert_eq!(
        neighbors.len(),
        0,
        "deleted edge resurrected after empty-output compaction: {:?}",
        neighbors
    );

    Ok(())
}
