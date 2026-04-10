// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Test for issue #53: Edge properties must remain readable after adjacency compaction
//!
//! This test verifies that:
//! 1. Edge properties survive in main_edges after flush (dual-write)
//! 2. Adjacency compaction clears Delta L1 after incorporating into L2
//! 3. Property reads correctly fall back to main_edges after compaction
//! 4. Debug assertions verify all Delta L1 EIDs exist in main_edges before clearing

use anyhow::Result;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use uni_common::Properties;
use uni_common::Value;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_store::runtime::context::QueryContext;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::compaction::Compactor;
use uni_store::storage::manager::StorageManager;

async fn setup_test_db() -> Result<(TempDir, Arc<StorageManager>, Writer, PropertyManager, u32)> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");

    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);

    // Add label and edge type
    let _label_id = schema_manager.add_label("Person")?;
    let edge_type_id = schema_manager.add_edge_type(
        "KNOWS",
        vec!["Person".to_string()],
        vec!["Person".to_string()],
    )?;

    // Add property definitions for KNOWS edge type
    schema_manager.add_property("KNOWS", "since", DataType::Int, true)?;
    schema_manager.add_property("KNOWS", "weight", DataType::Float, true)?;
    schema_manager.save().await?;

    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;
    let property_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 0); // Disable cache

    Ok((temp_dir, storage, writer, property_manager, edge_type_id))
}

#[tokio::test]
async fn test_edge_properties_readable_after_compaction() -> Result<()> {
    let (_temp_dir, storage, mut writer, property_manager, edge_type_id) = setup_test_db().await?;

    // Create two vertices
    let v1 = writer.next_vid().await?;
    let v2 = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(v1, HashMap::new(), &["Person".to_string()], None)
        .await?;
    writer
        .insert_vertex_with_labels(v2, HashMap::new(), &["Person".to_string()], None)
        .await?;

    // Create edge with properties
    let mut edge_props = Properties::new();
    edge_props.insert("since".to_string(), Value::Int(2020));
    edge_props.insert("weight".to_string(), Value::Float(0.85));

    let eid = writer.next_eid(edge_type_id).await?;
    writer
        .insert_edge(v1, v2, edge_type_id, eid, edge_props.clone(), None, None)
        .await?;

    // Flush to storage (dual-writes to Delta L1 and main_edges)
    writer.flush_to_l1(None).await?;

    // Verify properties are readable from Delta L1
    let ctx = QueryContext::new(writer.l0_manager.get_current());
    let props_before = property_manager
        .get_all_edge_props_with_ctx(eid, Some(&ctx))
        .await?;
    assert!(
        props_before.is_some(),
        "Properties should exist before compaction"
    );
    let props_before = props_before.unwrap();
    assert_eq!(props_before.get("since"), Some(&Value::Int(2020)));
    assert_eq!(props_before.get("weight"), Some(&Value::Float(0.85)));

    // Compact adjacency (incorporates Delta L1 into L2, then clears Delta L1)
    let compactor = Compactor::new(storage.clone());
    let _ = compactor
        .compact_adjacency("KNOWS", "Person", "fwd")
        .await?;
    let _ = compactor
        .compact_adjacency("KNOWS", "Person", "bwd")
        .await?;

    // Verify Delta L1 is now empty (cleared after compaction)
    let delta_ds = storage.delta_dataset("KNOWS", "fwd")?;
    let schema_ref = storage.schema_manager().schema();
    let delta_entries = delta_ds
        .scan_all_backend(storage.backend(), &schema_ref)
        .await?;
    assert!(
        delta_entries.is_empty(),
        "Delta L1 should be empty after compaction (found {} entries)",
        delta_entries.len()
    );

    // CRITICAL TEST: Verify properties are STILL readable after compaction
    // This tests the fallback to main_edges
    let props_after = property_manager
        .get_all_edge_props_with_ctx(eid, Some(&ctx))
        .await?;
    assert!(
        props_after.is_some(),
        "Properties should still exist after compaction (fallback to main_edges)"
    );
    let props_after = props_after.unwrap();
    assert_eq!(
        props_after.get("since"),
        Some(&Value::Int(2020)),
        "Property 'since' should be preserved after compaction"
    );
    assert_eq!(
        props_after.get("weight"),
        Some(&Value::Float(0.85)),
        "Property 'weight' should be preserved after compaction"
    );

    Ok(())
}

#[tokio::test]
async fn test_main_edges_fallback_when_delta_cleared() -> Result<()> {
    let (_temp_dir, storage, mut writer, property_manager, edge_type_id) = setup_test_db().await?;

    // Create vertices and edge with properties
    let v1 = writer.next_vid().await?;
    let v2 = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(v1, HashMap::new(), &["Person".to_string()], None)
        .await?;
    writer
        .insert_vertex_with_labels(v2, HashMap::new(), &["Person".to_string()], None)
        .await?;

    let mut edge_props = Properties::new();
    edge_props.insert("since".to_string(), Value::Int(2015));
    edge_props.insert("weight".to_string(), Value::Float(0.95));

    let eid = writer.next_eid(edge_type_id).await?;
    writer
        .insert_edge(v1, v2, edge_type_id, eid, edge_props.clone(), None, None)
        .await?;

    // Flush to storage
    writer.flush_to_l1(None).await?;

    // Manually clear Delta L1 (simulates post-compaction state)
    let delta_ds = storage.delta_dataset("KNOWS", "fwd")?;
    let schema_ref = storage.schema_manager().schema();
    let delta_schema = delta_ds.get_arrow_schema(&schema_ref)?;
    let empty_batch = arrow_array::RecordBatch::new_empty(delta_schema);
    delta_ds.replace(storage.backend(), empty_batch).await?;

    // Verify Delta L1 is empty
    let delta_entries = delta_ds
        .scan_all_backend(storage.backend(), &schema_ref)
        .await?;
    assert!(
        delta_entries.is_empty(),
        "Delta L1 should be manually cleared"
    );

    // Verify properties are readable via main_edges fallback
    let ctx = QueryContext::new(writer.l0_manager.get_current());
    let props = property_manager
        .get_all_edge_props_with_ctx(eid, Some(&ctx))
        .await?;
    assert!(
        props.is_some(),
        "Properties should be readable from main_edges after Delta L1 is cleared"
    );
    let props = props.unwrap();
    assert_eq!(props.get("since"), Some(&Value::Int(2015)));
    assert_eq!(props.get("weight"), Some(&Value::Float(0.95)));

    Ok(())
}

#[tokio::test]
async fn test_multiple_edges_properties_after_compaction() -> Result<()> {
    let (_temp_dir, storage, mut writer, property_manager, edge_type_id) = setup_test_db().await?;

    // Create vertices
    let v1 = writer.next_vid().await?;
    let v2 = writer.next_vid().await?;
    let v3 = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(v1, HashMap::new(), &["Person".to_string()], None)
        .await?;
    writer
        .insert_vertex_with_labels(v2, HashMap::new(), &["Person".to_string()], None)
        .await?;
    writer
        .insert_vertex_with_labels(v3, HashMap::new(), &["Person".to_string()], None)
        .await?;

    // Create multiple edges with different properties
    let mut props1 = Properties::new();
    props1.insert("since".to_string(), Value::Int(2018));
    props1.insert("weight".to_string(), Value::Float(0.7));

    let mut props2 = Properties::new();
    props2.insert("since".to_string(), Value::Int(2021));
    props2.insert("weight".to_string(), Value::Float(0.9));

    let mut props3 = Properties::new();
    props3.insert("since".to_string(), Value::Int(2019));
    props3.insert("weight".to_string(), Value::Float(0.65));

    let eid1 = writer.next_eid(edge_type_id).await?;
    writer
        .insert_edge(v1, v2, edge_type_id, eid1, props1, None, None)
        .await?;
    let eid2 = writer.next_eid(edge_type_id).await?;
    writer
        .insert_edge(v1, v3, edge_type_id, eid2, props2, None, None)
        .await?;
    let eid3 = writer.next_eid(edge_type_id).await?;
    writer
        .insert_edge(v2, v3, edge_type_id, eid3, props3, None, None)
        .await?;

    // Flush to storage
    writer.flush_to_l1(None).await?;

    // Compact adjacency
    let compactor = Compactor::new(storage.clone());
    let _ = compactor
        .compact_adjacency("KNOWS", "Person", "fwd")
        .await?;
    let _ = compactor
        .compact_adjacency("KNOWS", "Person", "bwd")
        .await?;

    // Verify all edge properties are still readable
    let ctx = QueryContext::new(writer.l0_manager.get_current());

    let props1_after = property_manager
        .get_all_edge_props_with_ctx(eid1, Some(&ctx))
        .await?
        .unwrap();
    assert_eq!(props1_after.get("since"), Some(&Value::Int(2018)));
    assert_eq!(props1_after.get("weight"), Some(&Value::Float(0.7)));

    let props2_after = property_manager
        .get_all_edge_props_with_ctx(eid2, Some(&ctx))
        .await?
        .unwrap();
    assert_eq!(props2_after.get("since"), Some(&Value::Int(2021)));
    assert_eq!(props2_after.get("weight"), Some(&Value::Float(0.9)));

    let props3_after = property_manager
        .get_all_edge_props_with_ctx(eid3, Some(&ctx))
        .await?
        .unwrap();
    assert_eq!(props3_after.get("since"), Some(&Value::Int(2019)));
    assert_eq!(props3_after.get("weight"), Some(&Value::Float(0.65)));

    Ok(())
}

#[tokio::test]
async fn test_edge_with_no_properties_after_compaction() -> Result<()> {
    let (_temp_dir, storage, mut writer, property_manager, edge_type_id) = setup_test_db().await?;

    // Create vertices and edge WITHOUT properties
    let v1 = writer.next_vid().await?;
    let v2 = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(v1, HashMap::new(), &["Person".to_string()], None)
        .await?;
    writer
        .insert_vertex_with_labels(v2, HashMap::new(), &["Person".to_string()], None)
        .await?;

    let eid = writer.next_eid(edge_type_id).await?;
    writer
        .insert_edge(v1, v2, edge_type_id, eid, HashMap::new(), None, None)
        .await?;

    // Flush and compact
    writer.flush_to_l1(None).await?;
    let compactor = Compactor::new(storage);
    let _ = compactor
        .compact_adjacency("KNOWS", "Person", "fwd")
        .await?;

    // Verify edge is still readable (returns empty properties)
    let ctx = QueryContext::new(writer.l0_manager.get_current());
    let props = property_manager
        .get_all_edge_props_with_ctx(eid, Some(&ctx))
        .await?;
    assert!(
        props.is_some(),
        "Edge with no properties should still be readable after compaction"
    );
    assert!(
        props.unwrap().is_empty(),
        "Edge should have empty properties"
    );

    Ok(())
}

#[tokio::test]
async fn test_deleted_edge_compaction_does_not_assert() -> Result<()> {
    let (_temp_dir, storage, mut writer, _property_manager, edge_type_id) =
        setup_test_db().await?;

    // Create two vertices
    let v1 = writer.next_vid().await?;
    let v2 = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(v1, HashMap::new(), &["Person".to_string()], None)
        .await?;
    writer
        .insert_vertex_with_labels(v2, HashMap::new(), &["Person".to_string()], None)
        .await?;

    // Create edge with properties
    let mut edge_props = Properties::new();
    edge_props.insert("since".to_string(), Value::Int(2020));
    edge_props.insert("weight".to_string(), Value::Float(0.85));

    let eid = writer.next_eid(edge_type_id).await?;
    writer
        .insert_edge(v1, v2, edge_type_id, eid, edge_props, None, None)
        .await?;

    // Flush to storage (dual-writes insert to Delta L1 + main_edges)
    writer.flush_to_l1(None).await?;

    // Delete the edge
    writer
        .delete_edge(eid, v1, v2, edge_type_id, None)
        .await?;

    // Flush again (dual-writes the delete tombstone — main_edges gets _deleted=true)
    writer.flush_to_l1(None).await?;

    // Compact adjacency — previously triggered a false-positive debug_assert
    // because find_props_by_eid filtered _deleted=false, missing the deleted edge
    let compactor = Compactor::new(storage.clone());
    let _ = compactor
        .compact_adjacency("KNOWS", "Person", "fwd")
        .await?;
    let _ = compactor
        .compact_adjacency("KNOWS", "Person", "bwd")
        .await?;

    // If we reach here, the debug_assert passed — the fix works
    Ok(())
}
