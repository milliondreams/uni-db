// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for writer.rs:3231 (finding [2]).
//!
//! `insert_vertices_batch` writes rows via the L0 `insert_vertex_with_labels`
//! helper, which never populates the L0 `constraint_index`. Only the
//! single-vertex `Writer::insert_vertex_with_labels` path does. So a unique
//! key of a batch-inserted, not-yet-flushed vertex is invisible to every
//! `has_constraint_key` check, and a subsequent single insert with the same
//! key passes validation and twins the unique key.

#![cfg(feature = "lance-backend")]

use std::collections::HashMap;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::tempdir;
use uni_common::Value;
use uni_common::core::schema::{Constraint, ConstraintTarget, ConstraintType, SchemaManager};
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

async fn setup() -> (tempfile::TempDir, Arc<SchemaManager>, Writer) {
    let dir = tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let schema_path = ObjectStorePath::from("schema.json");
    let schema_manager = Arc::new(
        SchemaManager::load_from_store(store, &schema_path)
            .await
            .unwrap(),
    );
    schema_manager.add_label("Person").unwrap();
    schema_manager
        .add_constraint(Constraint {
            name: "uniq_person_email".to_string(),
            constraint_type: ConstraintType::Unique {
                properties: vec!["email".to_string()],
            },
            target: ConstraintTarget::Label("Person".to_string()),
            enabled: true,
        })
        .unwrap();
    schema_manager.save().await.unwrap();
    let storage = Arc::new(
        StorageManager::new(&path, schema_manager.clone())
            .await
            .unwrap(),
    );
    let writer = Writer::new(storage.clone(), schema_manager.clone(), 1)
        .await
        .unwrap();
    (dir, schema_manager, writer)
}

fn email_props(email: &str) -> HashMap<String, Value> {
    let mut p = HashMap::new();
    p.insert("email".to_string(), Value::String(email.to_string()));
    p
}

/// Control: the single-vertex insert path DOES populate the constraint index,
/// so a duplicate is correctly rejected — isolating the batch-path gap.
#[tokio::test]
async fn control_single_insert_path_rejects_duplicate() {
    let (_dir, _sm, writer) = setup().await;
    let v1 = writer.next_vid().await.unwrap();
    writer
        .insert_vertex_with_labels(v1, email_props("a@x.com"), &["Person".to_string()], None)
        .await
        .unwrap();
    let v2 = writer.next_vid().await.unwrap();
    let dup = writer
        .insert_vertex_with_labels(v2, email_props("a@x.com"), &["Person".to_string()], None)
        .await;
    assert!(
        dup.is_err(),
        "control: single-path duplicate must be rejected"
    );
}

#[tokio::test]
async fn repro_batch_insert_hides_unique_key_from_single_insert() {
    let (_dir, _sm, writer) = setup().await;

    // Batch-insert vertex A with email=a@x.com. Do NOT flush.
    let v1 = writer.next_vid().await.unwrap();
    writer
        .insert_vertices_batch(
            vec![v1],
            vec![email_props("a@x.com")],
            vec!["Person".to_string()],
            None,
        )
        .await
        .unwrap();

    // Single-insert vertex B with the SAME email. The batch path never put A's
    // key in the L0 constraint_index, so has_constraint_key(current) is false,
    // pending is empty, tx is empty, and the Lance table is unflushed (count 0).
    let v2 = writer.next_vid().await.unwrap();
    let dup = writer
        .insert_vertex_with_labels(v2, email_props("a@x.com"), &["Person".to_string()], None)
        .await;

    // FIXED (writer.rs): insert_vertices_batch now populates the L0
    // constraint_index, so the single insert's has_constraint_key check sees A's
    // key and rejects the duplicate — matching the single-vertex control.
    assert!(
        dup.is_err(),
        "batch-inserted unique key must be visible to a later insert; got {dup:?}"
    );
}
