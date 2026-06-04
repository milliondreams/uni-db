// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration test for Issue #73: Shared CSR Breaks Snapshot Isolation
//!
//! Verifies that pinned snapshots have their own AdjacencyManager and don't
//! see edges from the live database that were created after the snapshot.

use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::id::{Eid, Vid};
use uni_db::core::schema::SchemaManager;
use uni_db::core::snapshot::SnapshotManifest;
use uni_db::storage::direction::Direction;
use uni_db::storage::manager::StorageManager;
use uuid::Uuid;

#[tokio::test]
async fn test_separate_adjacency_managers_for_snapshots() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // Setup Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _person_lbl = schema_manager.add_label("Person")?;
    let knows_edge = schema_manager.add_edge_type(
        "KNOWS",
        vec!["Person".to_string()],
        vec!["Person".to_string()],
    )?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );

    // Insert edge into live database's adjacency manager
    let vid_a = Vid::new(0);
    let vid_b = Vid::new(1);
    let eid_live = Eid::new(100);

    storage
        .adjacency_manager()
        .insert_edge(vid_a, vid_b, eid_live, knows_edge, 1);

    // Verify live database sees the edge
    let live_edges =
        storage
            .adjacency_manager()
            .get_neighbors(vid_a, knows_edge, Direction::Outgoing);
    assert_eq!(live_edges.len(), 1, "Live database should have 1 edge");

    // Create a mock snapshot manifest (minimal fields needed for pinned())
    let snapshot = SnapshotManifest::new(
        Uuid::new_v4().to_string(),
        schema_manager.schema().schema_version,
    );

    // Create pinned storage with separate AdjacencyManager (Issue #73 fix)
    let snapshot_storage = storage.pinned(snapshot);

    // Verify that snapshot's adjacency manager is empty (separate from live)
    let snapshot_edges =
        snapshot_storage
            .adjacency_manager()
            .get_neighbors(vid_a, knows_edge, Direction::Outgoing);

    assert_eq!(
        snapshot_edges.len(),
        0,
        "Snapshot should have its own empty AdjacencyManager, not share with live"
    );

    // Verify they have different pointers (separate instances)
    let live_ptr = Arc::as_ptr(&storage.adjacency_manager());
    let snapshot_ptr = Arc::as_ptr(&snapshot_storage.adjacency_manager());

    assert_ne!(
        live_ptr, snapshot_ptr,
        "Snapshot and live should have different AdjacencyManager instances"
    );

    Ok(())
}

#[tokio::test]
async fn test_snapshot_isolation_with_warm() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // Setup Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _node_lbl = schema_manager.add_label("Node")?;
    let link_edge =
        schema_manager.add_edge_type("LINK", vec!["Node".to_string()], vec!["Node".to_string()])?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );

    // Insert edge E1 into live adjacency manager
    let vid_a = Vid::new(0);
    let vid_b = Vid::new(1);
    let eid1 = Eid::new(100);
    storage
        .adjacency_manager()
        .insert_edge(vid_a, vid_b, eid1, link_edge, 1);

    // Create snapshot (with separate AdjacencyManager per Issue #73 fix)
    let snapshot = SnapshotManifest::new(
        Uuid::new_v4().to_string(),
        schema_manager.schema().schema_version,
    );
    let snapshot_storage = storage.pinned(snapshot);

    // Insert edge E2 into live adjacency manager AFTER snapshot
    let eid2 = Eid::new(200);
    storage
        .adjacency_manager()
        .insert_edge(vid_a, vid_b, eid2, link_edge, 2);

    // Live should see both E1 and E2
    let live_edges =
        storage
            .adjacency_manager()
            .get_neighbors(vid_a, link_edge, Direction::Outgoing);
    assert_eq!(live_edges.len(), 2, "Live should see both edges");

    // Snapshot should see neither (it has its own empty AdjacencyManager)
    let snapshot_edges =
        snapshot_storage
            .adjacency_manager()
            .get_neighbors(vid_a, link_edge, Direction::Outgoing);
    assert_eq!(
        snapshot_edges.len(),
        0,
        "Snapshot should not see any edges from live database"
    );

    Ok(())
}
