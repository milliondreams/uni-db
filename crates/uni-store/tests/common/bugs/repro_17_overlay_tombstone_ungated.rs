// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for property_manager.rs:572 (finding [17]).
//!
//! `overlay_l0_batch` removes a vid on ANY L0 tombstone unconditionally
//! (`result.remove(&vid); continue;`), while the property-overlay branch five
//! lines below IS gated on `version_high_water_mark` (`entry_version > hwm ->
//! skip`). So under a version-pinned read, an L0 tombstone at a version BEYOND
//! the pin wrongly deletes a vertex that should still be visible at the pinned
//! snapshot — a snapshot-isolation / time-travel violation. The property
//! branch's gate is the designers' own proof that L0 buffers here can
//! legitimately hold entries newer than the pin.

#![cfg(feature = "lance-backend")]

use std::collections::HashMap;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use parking_lot::RwLock;
use tempfile::tempdir;
use uni_common::Value;
use uni_common::core::schema::SchemaManager;
use uni_store::runtime::context::QueryContext;
use uni_store::runtime::l0::L0Buffer;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

#[tokio::test]
async fn repro_pinned_read_deleted_by_beyond_pin_tombstone() {
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
    schema_manager.save().await.unwrap();

    let storage = Arc::new(StorageManager::new(&path, schema_manager.clone()).await.unwrap());
    let writer = Writer::new(storage.clone(), schema_manager.clone(), 1)
        .await
        .unwrap();

    // Create V:Person {name:'Alice'} and flush to L1 (low version, e.g. 1-2).
    let v = writer.next_vid().await.unwrap();
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("Alice".to_string()));
    writer
        .insert_vertex_with_labels(v, props, &["Person".to_string()], None)
        .await
        .unwrap();
    writer.flush_to_l1(None).await.unwrap();

    // Pin a read at hwm = 100 (comfortably above V's flushed version).
    const HWM: u64 = 100;
    let pinned = Arc::new(storage.pinned_at_version(HWM));
    let pm = PropertyManager::new(pinned, schema_manager.clone(), 0);

    // A current L0 that tombstones V at a version BEYOND the pin (101 > 100),
    // as a post-pin DELETE would.
    let mut l0 = L0Buffer::new(0, None);
    l0.vertex_tombstones.insert(v);
    l0.vertex_versions.insert(v, HWM + 1);
    let ctx = QueryContext::new(Arc::new(RwLock::new(l0)));

    let res = pm
        .get_batch_vertex_props(&[v], &["name"], Some(&ctx))
        .await
        .unwrap();

    // Fixed (property_manager.rs:572): V is still visible at the pinned
    // snapshot. Its delete happened at version 101 > pin 100, so the now
    // version-gated tombstone branch skips it, exactly as the gated property
    // branch ignores a post-pin property update at v101.
    assert!(
        res.contains_key(&v),
        "beyond-pin tombstone must not remove V from the pinned read; got {res:?}"
    );
}
