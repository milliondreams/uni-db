// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase A acceptance tests for optimistic write-set conflict detection.
//!
//! These exercise the lost-update fix directly at the Writer commit boundary
//! (`commit_transaction_l0`): two transactions that begin with the same read
//! sequence and write the same vertex must not both commit. See
//! `docs/proposals/serializable_snapshot_isolation.md` (Component C4, Request 1).

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::TempDir;
use uni_common::UniError;
use uni_common::core::schema::{Constraint, ConstraintTarget, ConstraintType, SchemaManager};
use uni_store::runtime::QueryContext;
use uni_store::runtime::l0_visibility::lookup_vertex_prop;
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

async fn make_writer() -> Result<(Arc<Writer>, TempDir)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");
    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    schema_manager.add_label("Counter")?;
    schema_manager.save().await?;
    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let writer = Arc::new(Writer::new(storage, schema_manager, 1).await?);
    Ok((writer, dir))
}

fn counter_props(n: i64) -> HashMap<String, uni_common::Value> {
    let mut props = HashMap::new();
    props.insert("n".to_string(), uni_common::Value::Int(n));
    props
}

const LABEL: &str = "Counter";

/// Two concurrent read-modify-writes of the same vertex: the second committer
/// must abort with a serialization conflict (the lost update is prevented).
#[tokio::test]
async fn concurrent_writes_to_same_vertex_conflict() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let vid = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(vid, counter_props(0), &[LABEL.to_string()], None)
        .await?;

    // Both transactions begin before either commits → same read sequence.
    let tx_a = writer.create_transaction_l0();
    let tx_b = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(vid, counter_props(1), &[LABEL.to_string()], Some(&tx_a))
        .await?;
    writer
        .insert_vertex_with_labels(vid, counter_props(1), &[LABEL.to_string()], Some(&tx_b))
        .await?;

    // First commit wins.
    writer.commit_transaction_l0(tx_a).await?;

    // Second commit aborts — its read sequence predates A's committed write.
    let err = writer.commit_transaction_l0(tx_b).await.unwrap_err();
    match err.downcast::<UniError>() {
        Ok(UniError::SerializationConflict { .. }) => Ok(()),
        Ok(other) => panic!("expected SerializationConflict, got {other:?}"),
        Err(other) => panic!("expected typed UniError, got {other:?}"),
    }
}

/// Disjoint write-sets do not conflict — both transactions commit.
#[tokio::test]
async fn concurrent_writes_to_disjoint_vertices_both_commit() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let vid_a = writer.next_vid().await?;
    let vid_b = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(vid_a, counter_props(0), &[LABEL.to_string()], None)
        .await?;
    writer
        .insert_vertex_with_labels(vid_b, counter_props(0), &[LABEL.to_string()], None)
        .await?;

    let tx_a = writer.create_transaction_l0();
    let tx_b = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(vid_a, counter_props(1), &[LABEL.to_string()], Some(&tx_a))
        .await?;
    writer
        .insert_vertex_with_labels(vid_b, counter_props(1), &[LABEL.to_string()], Some(&tx_b))
        .await?;

    writer.commit_transaction_l0(tx_a).await?;
    writer.commit_transaction_l0(tx_b).await?;
    Ok(())
}

/// A transaction that begins after a conflicting commit (newer read sequence)
/// does not falsely conflict with it.
#[tokio::test]
async fn transaction_begun_after_commit_does_not_conflict() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let vid = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(vid, counter_props(0), &[LABEL.to_string()], None)
        .await?;

    let tx_a = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(vid, counter_props(1), &[LABEL.to_string()], Some(&tx_a))
        .await?;
    writer.commit_transaction_l0(tx_a).await?;

    // Begins now, observing A's commit → higher read sequence → no conflict.
    let tx_b = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(vid, counter_props(2), &[LABEL.to_string()], Some(&tx_b))
        .await?;
    writer.commit_transaction_l0(tx_b).await?;
    Ok(())
}

async fn make_writer_unique() -> Result<(Arc<Writer>, TempDir)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");
    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    schema_manager.add_label("E")?;
    schema_manager.add_constraint(Constraint {
        name: "E_eid_unique".to_string(),
        constraint_type: ConstraintType::Unique {
            properties: vec!["eid".to_string()],
        },
        target: ConstraintTarget::Label("E".to_string()),
        enabled: true,
    })?;
    schema_manager.save().await?;
    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let writer = Arc::new(Writer::new(storage, schema_manager, 1).await?);
    Ok((writer, dir))
}

fn eid_props(value: &str) -> HashMap<String, uni_common::Value> {
    let mut props = HashMap::new();
    props.insert("eid".to_string(), uni_common::Value::String(value.to_string()));
    props
}

/// Serializable MERGE: two transactions concurrently create distinct vertices
/// carrying the same unique key. Their write-sets are disjoint (different vids),
/// so write-set OCC does not catch it — the commit-time unique-key check must.
#[tokio::test]
async fn concurrent_unique_key_inserts_conflict() -> Result<()> {
    let (writer, _dir) = make_writer_unique().await?;
    let vid_a = writer.next_vid().await?;
    let vid_b = writer.next_vid().await?;

    let tx_a = writer.create_transaction_l0();
    let tx_b = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(vid_a, eid_props("shared"), &["E".to_string()], Some(&tx_a))
        .await?;
    writer
        .insert_vertex_with_labels(vid_b, eid_props("shared"), &["E".to_string()], Some(&tx_b))
        .await?;

    writer.commit_transaction_l0(tx_a).await?;

    let err = writer.commit_transaction_l0(tx_b).await.unwrap_err();
    match err.downcast::<UniError>() {
        Ok(UniError::ConstraintConflict { .. }) => Ok(()),
        Ok(other) => panic!("expected ConstraintConflict, got {other:?}"),
        Err(other) => panic!("expected typed UniError, got {other:?}"),
    }
}

/// SSI read-write antidependency: a transaction that read a vertex which a
/// concurrent transaction then wrote must abort, even though their write-sets
/// are disjoint (write-set OCC alone would miss it).
#[tokio::test]
async fn read_write_antidependency_aborts() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let x = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(x, counter_props(0), &[LABEL.to_string()], None)
        .await?;

    // tx_a begins and reads X, recording it in tx_a's read-set.
    let tx_a = writer.create_transaction_l0();
    {
        let ctx = QueryContext::new_with_tx(writer.l0_manager.get_current(), Some(tx_a.clone()));
        let _ = lookup_vertex_prop(x, "n", Some(&ctx));
    }

    // tx_b writes X and commits.
    let tx_b = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(x, counter_props(1), &[LABEL.to_string()], Some(&tx_b))
        .await?;
    writer.commit_transaction_l0(tx_b).await?;

    // tx_a writes an unrelated vertex Y, then commits — must abort because it
    // read X, which tx_b wrote after tx_a began.
    let y = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(y, counter_props(9), &[LABEL.to_string()], Some(&tx_a))
        .await?;
    let err = writer.commit_transaction_l0(tx_a).await.unwrap_err();
    match err.downcast::<UniError>() {
        Ok(UniError::SerializationConflict { .. }) => Ok(()),
        Ok(other) => panic!("expected SerializationConflict, got {other:?}"),
        Err(other) => panic!("expected typed UniError, got {other:?}"),
    }
}

/// Control: without the read, the same disjoint-write interleaving commits fine.
#[tokio::test]
async fn disjoint_writes_without_read_do_not_conflict() -> Result<()> {
    let (writer, _dir) = make_writer().await?;
    let x = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(x, counter_props(0), &[LABEL.to_string()], None)
        .await?;

    let tx_a = writer.create_transaction_l0(); // does NOT read X
    let tx_b = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(x, counter_props(1), &[LABEL.to_string()], Some(&tx_b))
        .await?;
    writer.commit_transaction_l0(tx_b).await?;

    let y = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(y, counter_props(9), &[LABEL.to_string()], Some(&tx_a))
        .await?;
    writer.commit_transaction_l0(tx_a).await?;
    Ok(())
}
