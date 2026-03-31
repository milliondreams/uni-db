// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Tests for Issue #29: VidLabelsIndex integration
//!
//! Verifies that the VID-to-labels index provides O(1) lookups with
//! LanceDB fallback when the index is disabled or VIDs are not found.

use anyhow::Result;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_common::UniConfig;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_store::runtime::Writer;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::storage::manager::StorageManager;

#[tokio::test]
async fn test_vid_labels_index_basic_functionality() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");

    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;

    // Create storage with index enabled (default)
    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);

    // Create a writer and insert some vertices
    let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

    // Allocate VIDs and insert 3 vertices with labels
    let vid1 = writer.next_vid().await?;
    let mut props1 = HashMap::new();
    props1.insert("name".to_string(), "Alice".into());
    writer
        .insert_vertex_with_labels(vid1, props1, &["Person".to_string()], None)
        .await?;

    let vid2 = writer.next_vid().await?;
    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), "Bob".into());
    writer
        .insert_vertex_with_labels(vid2, props2, &["Person".to_string()], None)
        .await?;

    let vid3 = writer.next_vid().await?;
    let mut props3 = HashMap::new();
    props3.insert("name".to_string(), "Charlie".into());
    writer
        .insert_vertex_with_labels(vid3, props3, &["Person".to_string()], None)
        .await?;

    // Flush to storage (this should update the index)
    writer.flush_to_l1(None).await?;

    // Verify the index returns correct labels
    let labels1 = storage.get_labels_from_index(vid1);
    assert_eq!(labels1, Some(vec!["Person".to_string()]));

    let labels2 = storage.get_labels_from_index(vid2);
    assert_eq!(labels2, Some(vec!["Person".to_string()]));

    let labels3 = storage.get_labels_from_index(vid3);
    assert_eq!(labels3, Some(vec!["Person".to_string()]));

    Ok(())
}

#[tokio::test]
async fn test_vid_labels_index_delete_removes_from_index() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");

    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;

    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

    // Insert a vertex
    let vid = writer.next_vid().await?;
    let mut props = HashMap::new();
    props.insert("name".to_string(), "Alice".into());
    writer
        .insert_vertex_with_labels(vid, props, &["Person".to_string()], None)
        .await?;
    writer.flush_to_l1(None).await?;

    // Verify it's in the index
    assert!(storage.get_labels_from_index(vid).is_some());

    // Delete the vertex
    writer.delete_vertex(vid, None, None).await?;
    writer.flush_to_l1(None).await?;

    // Verify it's removed from the index
    assert_eq!(storage.get_labels_from_index(vid), None);

    Ok(())
}

#[tokio::test]
async fn test_vid_labels_index_disabled_fallback() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");

    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;

    // Create config with index disabled
    let config = UniConfig {
        enable_vid_labels_index: false,
        ..Default::default()
    };

    let storage =
        Arc::new(StorageManager::new_with_config(path, schema_manager.clone(), config).await?);

    let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

    // Insert a vertex
    let vid = writer.next_vid().await?;
    let mut props = HashMap::new();
    props.insert("name".to_string(), "Alice".into());
    writer
        .insert_vertex_with_labels(vid, props, &["Person".to_string()], None)
        .await?;
    writer.flush_to_l1(None).await?;

    // Index should return None (disabled)
    assert_eq!(storage.get_labels_from_index(vid), None);

    // But PropertyManager should still work via LanceDB fallback
    let property_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let labels = property_manager.get_batch_labels(&[vid], None).await?;
    assert_eq!(labels.get(&vid), Some(&vec!["Person".to_string()]));

    Ok(())
}

#[tokio::test]
async fn test_vid_labels_index_rebuild_on_open() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");

    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;

    let vid1;
    let vid2;

    // Create storage and insert vertices
    {
        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
        let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

        vid1 = writer.next_vid().await?;
        let mut props1 = HashMap::new();
        props1.insert("name".to_string(), "Alice".into());
        writer
            .insert_vertex_with_labels(vid1, props1, &["Person".to_string()], None)
            .await?;

        vid2 = writer.next_vid().await?;
        let mut props2 = HashMap::new();
        props2.insert("name".to_string(), "Bob".into());
        writer
            .insert_vertex_with_labels(vid2, props2, &["Person".to_string()], None)
            .await?;

        writer.flush_to_l1(None).await?;
    }

    // Re-open storage (this should rebuild the index)
    let storage2 = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);

    // Verify the index has the correct data
    assert_eq!(
        storage2.get_labels_from_index(vid1),
        Some(vec!["Person".to_string()])
    );
    assert_eq!(
        storage2.get_labels_from_index(vid2),
        Some(vec!["Person".to_string()])
    );

    Ok(())
}
