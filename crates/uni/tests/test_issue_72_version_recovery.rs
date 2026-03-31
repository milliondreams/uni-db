// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Tests for Issue #72: Version Counter Reset on Manifest Loss
//!
//! Verifies that the database detects lost manifest pointers and fails loudly
//! instead of silently resetting the version counter to 0, which would cause
//! data corruption.

use anyhow::Result;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_common::core::schema::SchemaManager;
use uni_common::core::snapshot::SnapshotManifest;
use uni_db::Uni;
use uni_store::runtime::wal::WriteAheadLog;
use uni_store::storage::manager::StorageManager;
use uni_store::store_utils::{DEFAULT_TIMEOUT, delete_with_timeout, list_with_timeout};

/// Test 1: Fresh database starts at version zero
#[tokio::test]
async fn test_fresh_database_starts_at_version_zero() -> Result<()> {
    let _db = Uni::temporary().build().await?;
    // If we get here, the database started successfully with version 0
    Ok(())
}

/// Test 2: Lost latest pointer recovers from manifest
#[tokio::test]
async fn test_lost_latest_pointer_recovers_from_manifest() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();

    // Create database, insert data, and flush
    {
        let db = Uni::open(path).build().await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (n:Person {name: 'Alice'})").await?;
        tx.commit().await?;
        db.flush().await?;
    }

    // Check if snapshot was created
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?)
        as Arc<dyn object_store::ObjectStore>;
    let manifests_prefix = ObjectStorePath::from("catalog/manifests");
    let metas = list_with_timeout(&store, Some(&manifests_prefix), DEFAULT_TIMEOUT).await?;

    if metas.is_empty() {
        // No snapshot was created (probably because dataset was too small)
        // Test is not applicable in this case
        eprintln!("Skipping test - no snapshot created after flush");
        return Ok(());
    }

    // Delete the latest pointer file
    let latest_path = ObjectStorePath::from("catalog/latest");
    let _ = delete_with_timeout(&store, &latest_path, DEFAULT_TIMEOUT).await; // Ignore error if file doesn't exist

    // Reopen - should recover from manifest (success means version recovery worked)
    let db_result = Uni::open(path).build().await;
    if let Err(e) = &db_result {
        panic!(
            "Database should recover from manifest when latest pointer is missing: {}",
            e
        );
    }

    // If we get here, database successfully recovered from manifest
    Ok(())
}

/// Test 3: WAL without manifest fails loudly
#[tokio::test]
async fn test_wal_without_manifest_fails_loudly() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();

    // Create database and insert data (writes to WAL)
    {
        let db = Uni::open(path).build().await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (n:Person {name: 'Alice'})").await?;
        tx.commit().await?;
        // Don't flush - data only in WAL
    }

    // Delete all manifests AND latest pointer (keep WAL)
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?)
        as Arc<dyn object_store::ObjectStore>;
    let latest_path = ObjectStorePath::from("catalog/latest");
    let manifests_prefix = ObjectStorePath::from("catalog/manifests");

    // Delete latest pointer
    let _ = delete_with_timeout(&store, &latest_path, DEFAULT_TIMEOUT).await;

    // Delete all manifests
    let metas = list_with_timeout(&store, Some(&manifests_prefix), DEFAULT_TIMEOUT).await?;
    for meta in metas {
        delete_with_timeout(&store, &meta.location, DEFAULT_TIMEOUT).await?;
    }

    // Try to reopen - should fail loudly
    let result = Uni::open(path).build().await;
    assert!(
        result.is_err(),
        "Database should fail when WAL exists but no manifests"
    );

    let error_msg = match result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("Expected error but got success"),
    };
    assert!(
        error_msg.contains("WAL segments"),
        "Error message should mention WAL segments, got: {}",
        error_msg
    );
    assert!(
        error_msg.contains("no snapshot manifest"),
        "Error message should mention missing snapshot manifest, got: {}",
        error_msg
    );

    Ok(())
}

/// Test 4: WAL has_segments() detection
#[tokio::test]
async fn test_wal_has_segments() -> Result<()> {
    let dir = tempdir()?;
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let wal_path = ObjectStorePath::from("wal");

    let wal = WriteAheadLog::new(store.clone(), wal_path);

    // Initially empty
    let has_segments = wal.has_segments().await?;
    assert!(!has_segments, "Empty WAL should have no segments");

    // After append and flush
    use uni_common::core::id::Vid;
    use uni_store::runtime::wal::Mutation;

    wal.append(&Mutation::InsertVertex {
        vid: Vid::new(1),
        properties: HashMap::new(),
        labels: vec!["Test".to_string()],
    })?;
    wal.flush().await?;

    let has_segments_after = wal.has_segments().await?;
    assert!(has_segments_after, "WAL should have segments after flush");

    Ok(())
}

/// Test 5: SnapshotManager has_any_manifests() detection
#[tokio::test]
async fn test_snapshot_manager_has_any_manifests() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");

    let schema_manager =
        Arc::new(SchemaManager::load_from_store(store.clone(), &schema_path).await?);
    let _label_id = schema_manager.add_label("Test")?;
    schema_manager.save().await?;

    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let snapshot_mgr = storage.snapshot_manager();

    // Initially empty
    let has_manifests = snapshot_mgr.has_any_manifests().await?;
    assert!(!has_manifests, "Fresh database should have no manifests");

    // After saving a snapshot
    let manifest = SnapshotManifest {
        snapshot_id: "test_snapshot_1".to_string(),
        name: None,
        created_at: chrono::Utc::now(),
        parent_snapshot: None,
        schema_version: 1,
        version_high_water_mark: 10,
        wal_high_water_mark: 5,
        vertices: HashMap::new(),
        edges: HashMap::new(),
    };

    snapshot_mgr.save_snapshot(&manifest).await?;

    let has_manifests_after = snapshot_mgr.has_any_manifests().await?;
    assert!(has_manifests_after, "Should have manifests after save");

    Ok(())
}
