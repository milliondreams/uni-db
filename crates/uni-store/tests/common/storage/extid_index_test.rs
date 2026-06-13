// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Global `ext_id` uniqueness via the L0 `extid_index`.
//!
//! `Writer::check_extid_globally_unique` used to scan every
//! `vertex_properties` map in every L0 buffer per insert — O(n²) constrained
//! ingest. It now consults the maintained `extid_index`. These tests pin the
//! semantics the index must preserve: duplicate rejection, overwrite
//! precision, deletion release, WAL-recovery consistency, and the
//! concurrent-commit race close.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::TempDir;
use uni_common::Value;
use uni_common::core::schema::SchemaManager;
use uni_store::runtime::wal::WriteAheadLog;
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

// Rust guideline compliant

fn ext_props(ext_id: &str) -> HashMap<String, Value> {
    let mut props = HashMap::new();
    props.insert("ext_id".to_string(), Value::String(ext_id.to_string()));
    props
}

async fn make_writer() -> Result<(Arc<Writer>, TempDir)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");
    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    schema_manager.add_label("Node")?;
    schema_manager.save().await?;
    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let writer = Arc::new(Writer::new(storage, schema_manager, 1).await?);
    Ok((writer, dir))
}

const LABEL: &str = "Node";

/// A duplicate ext_id insert must be rejected; a distinct one must pass.
#[tokio::test]
async fn duplicate_extid_rejected() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let v1 = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(v1, ext_props("x"), &[LABEL.to_string()], None)
        .await?;

    let v2 = writer.next_vid().await?;
    let dup = writer
        .insert_vertex_with_labels(v2, ext_props("x"), &[LABEL.to_string()], None)
        .await;
    assert!(dup.is_err(), "duplicate ext_id 'x' must be rejected");

    let v3 = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(v3, ext_props("y"), &[LABEL.to_string()], None)
        .await?;
    Ok(())
}

/// Overwrite precision: after A changes its ext_id x→y, another vertex may
/// claim x, but y stays taken (the index must drop the OLD value on change,
/// not keep both).
#[tokio::test]
async fn extid_overwrite_releases_old_value() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let a = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(a, ext_props("x"), &[LABEL.to_string()], None)
        .await?;
    // A re-inserts with a new ext_id (CRDT-merge overwrite of the property).
    writer
        .insert_vertex_with_labels(a, ext_props("y"), &[LABEL.to_string()], None)
        .await?;

    // x is free again.
    let b = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(b, ext_props("x"), &[LABEL.to_string()], None)
        .await?;

    // y is taken by A.
    let c = writer.next_vid().await?;
    let dup = writer
        .insert_vertex_with_labels(c, ext_props("y"), &[LABEL.to_string()], None)
        .await;
    assert!(
        dup.is_err(),
        "ext_id 'y' is owned by A and must be rejected"
    );
    Ok(())
}

/// Deleting a vertex releases its ext_id.
#[tokio::test]
async fn delete_releases_extid() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let a = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(a, ext_props("x"), &[LABEL.to_string()], None)
        .await?;
    writer.delete_vertex(a, None, None).await?;

    let b = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(b, ext_props("x"), &[LABEL.to_string()], None)
        .await?;
    Ok(())
}

/// WAL recovery rebuilds the index: a committed-but-unflushed ext_id must
/// still be unique after reopening from the WAL.
#[tokio::test]
async fn extid_uniqueness_survives_wal_recovery() -> Result<()> {
    use uni_common::config::UniConfig;

    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");
    let schema_manager =
        Arc::new(SchemaManager::load_from_store(store.clone(), &schema_path).await?);
    schema_manager.add_label(LABEL)?;
    schema_manager.save().await?;
    let no_autoflush = UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        ..Default::default()
    };

    {
        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
        let wal = Arc::new(WriteAheadLog::new(
            store.clone(),
            ObjectStorePath::from("wal"),
        ));
        let writer = Arc::new(
            Writer::new_with_config(
                storage,
                schema_manager.clone(),
                1,
                no_autoflush.clone(),
                Some(wal),
                None,
            )
            .await?,
        );
        let v1 = writer.next_vid().await?;
        let tx = writer.create_transaction_l0();
        writer
            .insert_vertex_with_labels(v1, ext_props("x"), &[LABEL.to_string()], Some(&tx))
            .await?;
        writer.commit_transaction_l0(tx).await?;
        // Drop the writer without flushing to Lance — the row lives only in
        // the WAL, exactly the recovery window.
    }

    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let wal = Arc::new(WriteAheadLog::new(store, ObjectStorePath::from("wal")));
    let writer = Arc::new(
        Writer::new_with_config(storage, schema_manager, 1, no_autoflush, Some(wal), None).await?,
    );
    let replayed = writer.replay_wal(0).await?;
    assert!(
        replayed >= 1,
        "WAL replay must restore the committed vertex"
    );

    let v2 = writer.next_vid().await?;
    let dup = writer
        .insert_vertex_with_labels(v2, ext_props("x"), &[LABEL.to_string()], None)
        .await;
    assert!(
        dup.is_err(),
        "a WAL-recovered ext_id must still be unique after reopen"
    );
    Ok(())
}

/// Concurrent-commit race: two transactions insert the same ext_id; the
/// per-insert check passes for both (neither sees the other), so the
/// commit-time re-probe must abort the second committer.
#[tokio::test]
async fn concurrent_commits_same_extid_conflict() -> Result<()> {
    let (writer, _dir) = make_writer().await?;

    let tx_a = writer.create_transaction_l0();
    let tx_b = writer.create_transaction_l0();
    let va = writer.next_vid().await?;
    let vb = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(va, ext_props("shared"), &[LABEL.to_string()], Some(&tx_a))
        .await?;
    writer
        .insert_vertex_with_labels(vb, ext_props("shared"), &[LABEL.to_string()], Some(&tx_b))
        .await?;

    writer.commit_transaction_l0(tx_a).await?;
    let second = writer.commit_transaction_l0(tx_b).await;
    assert!(
        second.is_err(),
        "second committer of the same ext_id must abort (commit-time re-probe)"
    );
    let err = format!("{:#}", second.unwrap_err());
    assert!(
        err.contains("ext_id"),
        "abort should name the ext_id conflict, got: {err}"
    );
    Ok(())
}
