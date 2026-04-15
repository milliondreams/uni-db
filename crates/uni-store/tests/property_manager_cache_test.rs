// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Tests for PropertyManager cache behavior.

use std::sync::Arc;
use tempfile::tempdir;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::storage::manager::StorageManager;

async fn setup_property_manager(
    path: &std::path::Path,
    capacity: usize,
) -> (
    Arc<PropertyManager>,
    Arc<StorageManager>,
    Arc<SchemaManager>,
) {
    let schema_manager = SchemaManager::load(&path.join("schema.json"))
        .await
        .unwrap();
    schema_manager.add_label("Person").unwrap();
    schema_manager
        .add_property("Person", "name", DataType::String, true)
        .unwrap();
    schema_manager.save().await.unwrap();

    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await
        .unwrap(),
    );

    let prop_manager = Arc::new(PropertyManager::new(
        storage.clone(),
        schema_manager.clone(),
        capacity,
    ));

    (prop_manager, storage, schema_manager)
}

#[tokio::test]
async fn test_cache_disabled_when_capacity_zero() {
    let dir = tempdir().unwrap();
    let (pm, _, _) = setup_property_manager(dir.path(), 0).await;

    assert!(!pm.caching_enabled());
    assert_eq!(pm.cache_size(), 0);
}

#[tokio::test]
async fn test_cache_enabled_when_capacity_nonzero() {
    let dir = tempdir().unwrap();
    let (pm, _, _) = setup_property_manager(dir.path(), 100).await;

    assert!(pm.caching_enabled());
    assert_eq!(pm.cache_size(), 100);
}

#[tokio::test]
async fn test_clear_cache_succeeds() {
    let dir = tempdir().unwrap();
    let (pm, _, _) = setup_property_manager(dir.path(), 10).await;

    // Smoke test: clear_cache should not panic on an empty cache
    pm.clear_cache().await;
}

#[tokio::test]
async fn test_invalidate_vertex_does_not_panic() {
    let dir = tempdir().unwrap();
    let (pm, _, _) = setup_property_manager(dir.path(), 10).await;

    // Invalidating a non-existent vertex should not panic
    pm.invalidate_vertex(uni_common::core::id::Vid::new(999))
        .await;
}

#[tokio::test]
async fn test_invalidate_edge_does_not_panic() {
    let dir = tempdir().unwrap();
    let (pm, _, _) = setup_property_manager(dir.path(), 10).await;

    // Invalidating a non-existent edge should not panic
    pm.invalidate_edge(uni_common::core::id::Eid::new(999))
        .await;
}
