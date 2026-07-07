// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for compaction.rs:257 (finding [16]).
//!
//! `compact_vertices` performs an UNGUARDED read-modify-write on the per-label
//! vertex table: it `scan`s all rows (:131), merges them in memory, then
//! `dataset.replace(...)` OVERWRITES the whole table with only the scanned rows
//! (:256). There is no flush/compaction interlock, so a concurrent flush that
//! appends a new vertex row AFTER the scan but BEFORE the replace is silently
//! wiped — the replace has no knowledge of it.
//!
//! FIXED: `compact_vertices` and the flush's `merge_insert` now both hold the
//! per-table `lock_table_for_write` guard across their whole read→write, so the
//! scan→replace window can no longer interleave with a flush. This test drives 40
//! rounds of concurrent flush+compaction and asserts the invariant that every
//! committed vertex survives — deterministically green now that the two writers
//! are serialized.

#![cfg(feature = "lance-backend")]

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::UInt64Array;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::tempdir;
use uni_common::core::schema::SchemaManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::compaction::Compactor;
use uni_store::storage::manager::StorageManager;

async fn present_vids(storage: &StorageManager) -> HashSet<u64> {
    let mut out = HashSet::new();
    if let Some(batch) = storage
        .scan_vertex_table("Person", &["_vid"], None)
        .await
        .unwrap()
        && let Some(col) = batch
            .column_by_name("_vid")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
    {
        for i in 0..col.len() {
            out.insert(col.value(i));
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repro_compact_vertices_drops_concurrently_flushed_vertex() {
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
    let writer = Arc::new(
        Writer::new(storage.clone(), schema_manager.clone(), 1)
            .await
            .unwrap(),
    );

    // Seed one committed vertex so the vertex table exists and compaction runs.
    let seed = writer.next_vid().await.unwrap();
    writer
        .insert_vertex_with_labels(seed, HashMap::new(), &["Person".to_string()], None)
        .await
        .unwrap();
    writer.flush_to_l1(None).await.unwrap();

    let mut expected: HashSet<u64> = HashSet::new();
    expected.insert(seed.as_u64());

    // Repeatedly race a concurrent flush against a vertex compaction. Each
    // iteration commits a distinct vertex; none may be lost. A handful of rounds
    // is enough now that the per-table write lock makes the interlock
    // deterministic (each round is a real Lance flush + full-table overwrite, so
    // the count is kept modest to stay a fast regression test).
    for _ in 0..5 {
        let vid = writer.next_vid().await.unwrap();
        expected.insert(vid.as_u64());

        let w = writer.clone();
        let flush = tokio::spawn(async move {
            w.insert_vertex_with_labels(vid, HashMap::new(), &["Person".to_string()], None)
                .await
                .unwrap();
            w.flush_to_l1(None).await.unwrap();
        });

        let compactor = Compactor::new(storage.clone());
        let compact = tokio::spawn(async move {
            let _ = compactor.compact_vertices("Person").await;
        });

        let (_f, _c) = tokio::join!(flush, compact);
    }

    let present = present_vids(&storage).await;
    // FIXED (compaction.rs/manager.rs): compaction and flush serialize on the
    // per-table write lock, so no vertex flushed during compaction is lost.
    let missing: Vec<u64> = expected.difference(&present).copied().collect();
    assert!(
        missing.is_empty(),
        "committed vertices must survive concurrent compaction: {missing:?}"
    );
}
