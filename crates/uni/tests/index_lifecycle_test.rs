// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for index lifecycle management (IndexStatus, metadata, auto-rebuild).

use anyhow::Result;
use std::collections::HashMap;
use uni_db::Uni;
use uni_db::UniConfig;
use uni_db::core::schema::{IndexDefinition, IndexStatus, ScalarIndexConfig, ScalarIndexType};
use uni_db::unival;

#[tokio::test]
async fn test_list_indexes_with_metadata() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // Add a label and index
    db.schema_manager().add_label("Person")?;
    db.schema_manager().add_property(
        "Person",
        "name",
        uni_db::core::schema::DataType::String,
        false,
    )?;

    let idx = IndexDefinition::Scalar(ScalarIndexConfig {
        name: "idx_person_name".to_string(),
        label: "Person".to_string(),
        properties: vec!["name".to_string()],
        index_type: ScalarIndexType::BTree,
        where_clause: None,
        metadata: Default::default(),
    });
    db.schema_manager().add_index(idx)?;

    // list_indexes returns indexes for a specific label
    let indexes = db.list_indexes("Person");
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].name(), "idx_person_name");
    assert_eq!(indexes[0].metadata().status, IndexStatus::Online);
    assert!(indexes[0].metadata().last_built_at.is_none());

    // list_all_indexes returns all
    let all = db.list_all_indexes();
    assert_eq!(all.len(), 1);

    // No indexes for a non-existent label
    let empty = db.list_indexes("NoSuchLabel");
    assert!(empty.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_update_index_metadata_persists() -> Result<()> {
    let db = Uni::temporary().build().await?;

    db.schema_manager().add_label("Product")?;
    let idx = IndexDefinition::Scalar(ScalarIndexConfig {
        name: "idx_product_sku".to_string(),
        label: "Product".to_string(),
        properties: vec!["sku".to_string()],
        index_type: ScalarIndexType::BTree,
        where_clause: None,
        metadata: Default::default(),
    });
    db.schema_manager().add_index(idx)?;

    // Update metadata
    let now = chrono::Utc::now();
    db.schema_manager()
        .update_index_metadata("idx_product_sku", |m| {
            m.status = IndexStatus::Stale;
            m.last_built_at = Some(now);
            m.row_count_at_build = Some(500);
        })?;

    // Verify through list_indexes
    let indexes = db.list_indexes("Product");
    assert_eq!(indexes[0].metadata().status, IndexStatus::Stale);
    assert_eq!(indexes[0].metadata().row_count_at_build, Some(500));

    // Save and reload
    db.schema_manager().save().await?;
    let indexes2 = db.list_indexes("Product");
    assert_eq!(indexes2[0].metadata().status, IndexStatus::Stale);

    Ok(())
}

#[tokio::test]
async fn test_bulk_sync_sets_metadata() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // Setup schema with a scalar index
    db.schema_manager().add_label("Item")?;
    db.schema_manager().add_property(
        "Item",
        "name",
        uni_db::core::schema::DataType::String,
        false,
    )?;
    db.schema_manager().save().await?;

    let idx = IndexDefinition::Scalar(ScalarIndexConfig {
        name: "idx_item_name".to_string(),
        label: "Item".to_string(),
        properties: vec!["name".to_string()],
        index_type: ScalarIndexType::BTree,
        where_clause: None,
        metadata: Default::default(),
    });
    db.schema_manager().add_index(idx)?;
    db.schema_manager().save().await?;

    // Bulk load some data with sync index rebuild
    let s = db.session();
    let tx = s.tx().await?;
    let mut bulk = tx
        .bulk_writer()
        .defer_scalar_indexes(true)
        .async_indexes(false)
        .build()?;

    let mut vertices = Vec::new();
    for i in 0..10 {
        let mut props = HashMap::new();
        props.insert("name".to_string(), unival!(format!("item_{}", i)));
        vertices.push(props);
    }
    bulk.insert_vertices("Item", vertices).await?;
    let stats = bulk.commit().await?;
    assert_eq!(stats.vertices_inserted, 10);
    assert_eq!(stats.indexes_rebuilt, 1);

    // Verify metadata was updated on our original index
    let indexes = db.list_indexes("Item");
    assert!(!indexes.is_empty());
    let our_idx = indexes
        .iter()
        .find(|i| i.name() == "idx_item_name")
        .expect("idx_item_name should exist");
    assert_eq!(our_idx.metadata().status, IndexStatus::Online);
    assert!(our_idx.metadata().last_built_at.is_some());
    // row_count_at_build should be set (may be 10 or more depending on table format)
    assert!(our_idx.metadata().row_count_at_build.is_some());

    Ok(())
}

#[tokio::test]
async fn test_auto_rebuild_config_default_disabled() {
    let config = UniConfig::default();
    assert!(!config.index_rebuild.auto_rebuild_enabled);
    assert_eq!(config.index_rebuild.growth_trigger_ratio, 0.5);
    assert!(config.index_rebuild.max_index_age.is_none());
}
