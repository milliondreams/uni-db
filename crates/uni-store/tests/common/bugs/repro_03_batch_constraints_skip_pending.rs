// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for writer.rs:1961 (finding [3]).
//!
//! `validate_vertex_batch_constraints` builds its existing-key/ext_id index
//! from only the current L0 buffer and the transaction L0 — it never scans
//! `get_pending_flush()`. During an in-flight flush a key's buffer sits on
//! `pending_flush` (rows not yet in Lance, current L0 fresh-empty), so a batch
//! insert with a duplicate key/ext_id whose only prior copy is on
//! `pending_flush` passes all checks. The single-vertex path
//! (`check_unique_constraint_multi`) DOES scan pending_flush and catches it —
//! the batch path is the outlier.

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

async fn setup() -> (tempfile::TempDir, Writer) {
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
    (dir, writer)
}

fn email_props(email: &str) -> HashMap<String, Value> {
    let mut p = HashMap::new();
    p.insert("email".to_string(), Value::String(email.to_string()));
    p
}

#[tokio::test]
async fn repro_batch_constraints_ignore_pending_flush_buffer() {
    let (_dir, writer) = setup().await;

    // Insert vertex A via the SINGLE path so its unique key lands in the
    // current L0's constraint_index AND vertex_properties.
    let v1 = writer.next_vid().await.unwrap();
    writer
        .insert_vertex_with_labels(v1, email_props("a@x.com"), &["Person".to_string()], None)
        .await
        .unwrap();

    // Simulate an in-flight flush: rotate the current L0 (holding A) onto the
    // pending_flush list WITHOUT completing the flush. Current L0 is now a
    // fresh, empty buffer; A's rows are NOT yet in Lance.
    let next_version = writer.l0_manager.get_current().read().current_version + 1;
    let _rotated = writer.l0_manager.begin_flush(next_version, None);
    assert_eq!(
        writer.l0_manager.get_pending_flush().len(),
        1,
        "A's buffer is now on pending_flush"
    );

    // Batch-insert vertex B with the SAME email. validate_vertex_batch_constraints
    // scans current L0 (empty) + tx (none) + storage (unflushed) but NOT
    // pending_flush, so it misses A.
    let v2 = writer.next_vid().await.unwrap();
    let batch_dup = writer
        .insert_vertices_batch(
            vec![v2],
            vec![email_props("a@x.com")],
            vec!["Person".to_string()],
            None,
        )
        .await;
    // FIXED (writer.rs): validate_vertex_batch_constraints now scans
    // pending_flush, so the batch path rejects the duplicate like the single
    // path does.
    assert!(
        batch_dup.is_err(),
        "batch path must scan pending_flush and reject the duplicate; got {batch_dup:?}"
    );

    // Control: the single-vertex path DOES scan pending_flush and rejects the
    // same duplicate key.
    let v3 = writer.next_vid().await.unwrap();
    let single_dup = writer
        .insert_vertex_with_labels(v3, email_props("a@x.com"), &["Person".to_string()], None)
        .await;
    assert!(
        single_dup.is_err(),
        "control: single path scans pending_flush and rejects the duplicate"
    );
}
