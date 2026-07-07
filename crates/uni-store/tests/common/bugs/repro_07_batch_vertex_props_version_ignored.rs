// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for property_manager.rs:506 (finding [7]).
//!
//! `PropertyManager::get_batch_vertex_props` projects `_version` (columns list
//! at :465) but never reads it: rows are applied in raw scan order with a
//! full-map `result.insert` overwrite (:516) and an unconditional
//! `result.remove` on `_deleted` (:507). There is NO `_version` ranking, so the
//! materialized properties depend on which storage row the scan happens to
//! return last. When two versions of the same vid coexist in the per-label
//! vertex table (e.g. two flushes before compaction) and the scan surfaces the
//! OLDER row last, the batch reader returns the STALE value — the exact MVCC
//! defect (review C2) that the single-vid `find_props_by_vid` was fixed to
//! close.
//!
//! We write both versions into one batch with the NEWER row first, so the
//! order-blind loop applies the older row last and the stale value wins.
//!
//! Ignored: the manifestation depends on scan row order, which the backend does
//! not contractually guarantee; the assertion pins the buggy (order-dependent)
//! outcome rather than a fixed invariant.

#![cfg(feature = "lance-backend")]

use std::collections::HashMap;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::tempdir;
use uni_common::Value;
use uni_common::core::id::Vid;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::storage::manager::StorageManager;

#[tokio::test]
async fn repro_batch_vertex_props_returns_stale_version() {
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
        .add_property("Person", "name", DataType::String, true)
        .unwrap();
    schema_manager.save().await.unwrap();

    let storage = Arc::new(
        StorageManager::new(&path, schema_manager.clone())
            .await
            .unwrap(),
    );

    // Write two LIVE versions of Vid(1) into a single batch, NEWER row first so
    // the order-blind loop applies the stale v1 row last.
    let vid = Vid::new(1);
    let mut newer = HashMap::new();
    newer.insert("name".to_string(), Value::String("Bob".to_string())); // v2
    let mut older = HashMap::new();
    older.insert("name".to_string(), Value::String("Alice".to_string())); // v1

    let ds = storage.vertex_dataset("Person").unwrap();
    let schema = schema_manager.schema();
    let batch = ds
        .build_record_batch(
            &[
                (vid, vec!["Person".to_string()], newer),
                (vid, vec!["Person".to_string()], older),
            ],
            &[false, false],
            &[2u64, 1u64],
            schema.as_ref(),
        )
        .unwrap();
    ds.write_batch(storage.backend(), batch, schema.as_ref())
        .await
        .unwrap();

    let pm = PropertyManager::new(storage.clone(), schema_manager.clone(), 0);
    let res = pm
        .get_batch_vertex_props(&[vid], &["name"], None)
        .await
        .unwrap();

    // FIXED (property_manager.rs): get_batch_vertex_props now version-ranks, so
    // the v2 value "Bob" wins regardless of physical scan order.
    assert_eq!(
        res.get(&vid).and_then(|p| p.get("name")),
        Some(&Value::String("Bob".to_string())),
        "version-ranked batch read must return the newest (v2) value; got {res:?}"
    );
}
