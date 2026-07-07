// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for manager.rs:2737 (finding [18]).
//!
//! `merge_l0_into_fts_results` has the identical copy-paste defect as the
//! vector path: the append loop (:2737) pushes every text-match `l0_candidates`
//! entry into the FTS result set without checking `tombstoned`. A vertex live
//! (label + text) in an EARLIER L0 buffer but only tombstoned in a LATER buffer
//! is appended anyway — resurrecting a deleted vertex into full-text results.

#![cfg(feature = "lance-backend")]

use std::collections::HashMap;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use parking_lot::RwLock;
use tempfile::tempdir;
use uni_common::Value;
use uni_common::core::id::Vid;
use uni_common::core::schema::SchemaManager;
use uni_store::runtime::context::QueryContext;
use uni_store::runtime::l0::L0Buffer;
use uni_store::storage::manager::StorageManager;

#[tokio::test]
async fn repro_fts_search_resurrects_tombstoned_l0_vertex() {
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

    let storage = Arc::new(
        StorageManager::new(&path, schema_manager.clone())
            .await
            .unwrap(),
    );

    let vid = Vid::new(1);

    // Earlier buffer: V is live with a Person label + matching text.
    let mut earlier = L0Buffer::new(0, None);
    earlier
        .vertex_labels
        .insert(vid, vec!["Person".to_string()]);
    let mut props = HashMap::new();
    props.insert("bio".to_string(), Value::String("hello world".to_string()));
    earlier.vertex_properties.insert(vid, props);
    earlier.vertex_versions.insert(vid, 1);

    // Later buffer: V is tombstoned with no re-creation.
    let mut later = L0Buffer::new(0, None);
    later.vertex_tombstones.insert(vid);
    later.vertex_versions.insert(vid, 2);

    let ctx = QueryContext::new_with_pending(
        Arc::new(RwLock::new(later)),
        None,
        vec![Arc::new(RwLock::new(earlier))],
    );

    let results = storage
        .fts_search("Person", "bio", "hello", 10, None, Some(&ctx))
        .await
        .unwrap();

    // Fixed (manager.rs:2737): the newest buffer deleted V, so the append now
    // honours `tombstoned` and V must NOT appear in FTS results.
    assert!(
        !results.iter().any(|(v, _)| *v == vid),
        "tombstoned L0 vertex must not be resurrected into FTS results; got {results:?}"
    );
}
