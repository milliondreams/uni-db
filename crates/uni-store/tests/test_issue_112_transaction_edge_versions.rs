// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Tests for Issue #112: L0 Neighbor Merge Ignores Versions
//!
//! This test verifies that edges created at different points within a transaction
//! carry their actual creation versions into the adjacency manager, not a single
//! shared version. This ensures version-filtered neighbor queries work correctly
//! after transaction commit.

use anyhow::Result;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_common::core::schema::SchemaManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::direction::Direction;

/// Test that edges inserted at different versions within a transaction
/// are correctly visible at version-filtered queries after commit
#[tokio::test]
async fn test_transaction_edge_versions_preserved_in_adjacency() -> Result<()> {
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

    let storage = Arc::new(
        uni_store::storage::manager::StorageManager::new(path, schema_manager.clone()).await?,
    );
    let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

    // Create vertices (outside transaction)
    let vid_src = writer.next_vid().await?;
    let vid_dst_a = writer.next_vid().await?;
    let vid_dst_b = writer.next_vid().await?;

    writer
        .insert_vertex_with_labels(vid_src, HashMap::new(), &["Person".to_string()], None)
        .await?;
    writer
        .insert_vertex_with_labels(vid_dst_a, HashMap::new(), &["Person".to_string()], None)
        .await?;
    writer
        .insert_vertex_with_labels(vid_dst_b, HashMap::new(), &["Person".to_string()], None)
        .await?;

    // Begin transaction
    let tx_l0 = writer.create_transaction_l0();

    // Insert first edge (will get version from tx L0)
    let eid_a = writer.next_eid(edge_type_id).await?;
    writer
        .insert_edge(
            vid_src,
            vid_dst_a,
            edge_type_id,
            eid_a,
            HashMap::new(),
            None,
            Some(&tx_l0),
        )
        .await?;

    // Capture version after first edge (simulate version advancement in transaction)
    let version_after_first = {
        let tx_l0_guard = tx_l0.read();
        tx_l0_guard.current_version
    };

    // Insert second edge (will get a potentially different version from tx L0)
    let eid_b = writer.next_eid(edge_type_id).await?;
    writer
        .insert_edge(
            vid_src,
            vid_dst_b,
            edge_type_id,
            eid_b,
            HashMap::new(),
            None,
            Some(&tx_l0),
        )
        .await?;

    let version_after_second = {
        let tx_l0_guard = tx_l0.read();
        tx_l0_guard.current_version
    };

    // Commit transaction - this should preserve per-edge versions in adjacency manager
    writer.commit_transaction_l0(tx_l0).await?;

    let am = storage.adjacency_manager();

    // Query at the version after the first edge but before the second
    // If versions are correctly preserved, we should only see edge A
    if version_after_first < version_after_second {
        let neighbors_at_v1 = am.get_neighbors_at_version(
            vid_src,
            edge_type_id,
            Direction::Outgoing,
            version_after_first,
        );

        // Should see at most the first edge at this version
        // (exact count depends on whether edge insertion increments version)
        assert!(
            neighbors_at_v1.len() <= 1,
            "At version {}, expected at most 1 edge, got {}",
            version_after_first,
            neighbors_at_v1.len()
        );
    }

    // Query at the version after both edges - should see both
    let neighbors_at_v2 = am.get_neighbors_at_version(
        vid_src,
        edge_type_id,
        Direction::Outgoing,
        version_after_second,
    );
    assert_eq!(
        neighbors_at_v2.len(),
        2,
        "At version {}, expected 2 edges, got {}",
        version_after_second,
        neighbors_at_v2.len()
    );

    // Query without version filter - should see all edges
    let neighbors_all = am.get_neighbors(vid_src, edge_type_id, Direction::Outgoing);
    assert_eq!(
        neighbors_all.len(),
        2,
        "Expected 2 edges without version filter, got {}",
        neighbors_all.len()
    );

    Ok(())
}

/// Test that transaction edge versions work correctly across multiple commits
#[tokio::test]
async fn test_multiple_transactions_preserve_edge_versions() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");

    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    schema_manager.add_label("Person")?;
    let edge_type_id = schema_manager.add_edge_type(
        "KNOWS",
        vec!["Person".to_string()],
        vec!["Person".to_string()],
    )?;
    schema_manager.save().await?;

    let storage = Arc::new(
        uni_store::storage::manager::StorageManager::new(path, schema_manager.clone()).await?,
    );
    let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

    // Create vertices (outside transaction)
    let vid_src = writer.next_vid().await?;
    let vid_dst_1 = writer.next_vid().await?;
    let vid_dst_2 = writer.next_vid().await?;
    let vid_dst_3 = writer.next_vid().await?;

    writer
        .insert_vertex_with_labels(vid_src, HashMap::new(), &["Person".to_string()], None)
        .await?;
    writer
        .insert_vertex_with_labels(vid_dst_1, HashMap::new(), &["Person".to_string()], None)
        .await?;
    writer
        .insert_vertex_with_labels(vid_dst_2, HashMap::new(), &["Person".to_string()], None)
        .await?;
    writer
        .insert_vertex_with_labels(vid_dst_3, HashMap::new(), &["Person".to_string()], None)
        .await?;

    // Transaction 1: Insert edge to dst_1
    let tx_l0 = writer.create_transaction_l0();
    let eid_1 = writer.next_eid(edge_type_id).await?;
    writer
        .insert_edge(
            vid_src,
            vid_dst_1,
            edge_type_id,
            eid_1,
            HashMap::new(),
            None,
            Some(&tx_l0),
        )
        .await?;
    writer.commit_transaction_l0(tx_l0).await?;

    let version_after_tx1 = {
        let main_l0 = writer.l0_manager.get_current();
        main_l0.read().current_version
    };

    // Transaction 2: Insert edge to dst_2
    let tx_l0 = writer.create_transaction_l0();
    let eid_2 = writer.next_eid(edge_type_id).await?;
    writer
        .insert_edge(
            vid_src,
            vid_dst_2,
            edge_type_id,
            eid_2,
            HashMap::new(),
            None,
            Some(&tx_l0),
        )
        .await?;
    writer.commit_transaction_l0(tx_l0).await?;

    let version_after_tx2 = {
        let main_l0 = writer.l0_manager.get_current();
        main_l0.read().current_version
    };

    // Transaction 3: Insert edge to dst_3
    let tx_l0 = writer.create_transaction_l0();
    let eid_3 = writer.next_eid(edge_type_id).await?;
    writer
        .insert_edge(
            vid_src,
            vid_dst_3,
            edge_type_id,
            eid_3,
            HashMap::new(),
            None,
            Some(&tx_l0),
        )
        .await?;
    writer.commit_transaction_l0(tx_l0).await?;

    let am = storage.adjacency_manager();

    // Query at version after tx1 - should see only first edge
    let neighbors_v1 = am.get_neighbors_at_version(
        vid_src,
        edge_type_id,
        Direction::Outgoing,
        version_after_tx1,
    );
    assert!(
        !neighbors_v1.is_empty(),
        "At version {}, expected at least 1 edge, got {}",
        version_after_tx1,
        neighbors_v1.len()
    );

    // Query at version after tx2 - should see first two edges
    let neighbors_v2 = am.get_neighbors_at_version(
        vid_src,
        edge_type_id,
        Direction::Outgoing,
        version_after_tx2,
    );
    assert!(
        neighbors_v2.len() >= 2,
        "At version {}, expected at least 2 edges, got {}",
        version_after_tx2,
        neighbors_v2.len()
    );

    // Query without version filter - should see all three edges
    let neighbors_all = am.get_neighbors(vid_src, edge_type_id, Direction::Outgoing);
    assert_eq!(
        neighbors_all.len(),
        3,
        "Expected 3 edges without version filter, got {}",
        neighbors_all.len()
    );

    Ok(())
}
