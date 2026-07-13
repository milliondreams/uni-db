// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for manager.rs:2582 (finding [17]).
//!
//! `merge_l0_into_vector_results` accumulates `l0_candidates` and `tombstoned`
//! across the L0 chain, but the final append loop (:2582) pushes EVERY
//! `l0_candidates` entry into the result set without checking `tombstoned`. A
//! vertex that is live (has the label + embedding) in an EARLIER L0 buffer is
//! added to `l0_candidates`; if a LATER buffer only tombstones it, the later
//! buffer's label loop never revisits it (it has no props there), so it stays
//! in `l0_candidates` AND lands in `tombstoned`. The append then resurrects it
//! into vector-search results even though the newest buffer deleted it.

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
use uni_store::backend::types::VectorQueryOpts;
use uni_store::runtime::context::QueryContext;
use uni_store::runtime::l0::L0Buffer;
use uni_store::storage::manager::StorageManager;

#[tokio::test]
async fn repro_vector_search_resurrects_tombstoned_l0_vertex() {
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

    // Earlier (pending-flush) buffer: V is live with a Person label + embedding.
    let mut earlier = L0Buffer::new(0, None);
    earlier
        .vertex_labels
        .insert(vid, vec!["Person".to_string()]);
    let mut props = HashMap::new();
    props.insert("embedding".to_string(), Value::Vector(vec![0.1, 0.2, 0.3]));
    earlier.vertex_properties.insert(vid, props);
    earlier.vertex_versions.insert(vid, 1);

    // Later (current) buffer: V is tombstoned (deleted) with no re-creation.
    let mut later = L0Buffer::new(0, None);
    later.vertex_tombstones.insert(vid);
    later.vertex_versions.insert(vid, 2);

    let ctx = QueryContext::new_with_pending(
        Arc::new(RwLock::new(later)),
        None,
        vec![Arc::new(RwLock::new(earlier))],
    );

    let query = vec![0.1f32, 0.2, 0.3];
    let results = storage
        .vector_search(
            "Person",
            "embedding",
            &query,
            10,
            None,
            VectorQueryOpts::default(),
            Some(&ctx),
        )
        .await
        .unwrap();

    // Fixed (manager.rs:2582): the newest buffer deleted V, so the append now
    // honours `tombstoned` and V must NOT appear in vector-search results.
    assert!(
        !results.iter().any(|(v, _)| *v == vid),
        "tombstoned L0 vertex must not be resurrected into vector results; got {results:?}"
    );
}
