// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for main_vertex.rs:636 / :793 (finding [18]).
//!
//! `find_batch_props_by_vids` and `find_batch_labels_by_vids` filter
//! `_deleted = false` and take last-row-wins with NO `_version` ranking —
//! the exact MVCC bug (review C2) that the single-VID `find_props_by_vid` /
//! `find_labels_by_vid` were fixed to close. A schemaless vertex whose
//! deletion tombstone lives only in the main table is therefore RESURRECTED
//! by the batch readers (the `_deleted = false` filter drops the winning
//! tombstone), while the single-VID readers correctly report it deleted.

#![cfg(feature = "lance-backend")]

use std::collections::HashMap;

use uni_common::Value;
use uni_common::core::id::Vid;
use uni_store::backend::lance::LanceDbBackend;
use uni_store::backend::traits::StorageBackend;
use uni_store::storage::main_vertex::MainVertexDataset;

#[tokio::test]
async fn repro_batch_readers_resurrect_deleted_vertex() {
    let dir = tempfile::TempDir::new().unwrap();
    let be = LanceDbBackend::connect(dir.path().to_str().unwrap(), None)
        .await
        .unwrap();
    let backend: &dyn StorageBackend = &be;

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("Alice".to_string()));

    // v1: live row for Vid(1).
    let live = MainVertexDataset::build_record_batch(
        &[(Vid::new(1), vec!["Person".to_string()], props.clone(), false, 1u64)],
        None,
        None,
    )
    .unwrap();
    MainVertexDataset::write_batch(backend, live).await.unwrap();

    // v2: deletion tombstone at a higher version (the winning row).
    let dead = MainVertexDataset::build_record_batch(
        &[(Vid::new(1), vec!["Person".to_string()], props, true, 2u64)],
        None,
        None,
    )
    .unwrap();
    MainVertexDataset::write_batch(backend, dead).await.unwrap();

    // Single-VID reader: version-ranked, winner is the tombstone -> None.
    let single = MainVertexDataset::find_props_by_vid(backend, Vid::new(1), None)
        .await
        .unwrap();
    assert!(
        single.is_none(),
        "control: single-VID find_props_by_vid respects the tombstone winner"
    );
    let single_labels = MainVertexDataset::find_labels_by_vid(backend, Vid::new(1), None)
        .await
        .unwrap();
    assert!(
        single_labels.is_none(),
        "control: single-VID find_labels_by_vid respects the tombstone winner"
    );

    // Batch readers now version-rank and include tombstones (like the single-VID
    // readers): the highest-version row is the tombstone, so the vertex is absent.
    let batch = MainVertexDataset::find_batch_props_by_vids(backend, &[Vid::new(1)], None)
        .await
        .unwrap();
    // FIXED (main_vertex.rs): Vid(1) is deleted -> must NOT be resurrected.
    assert!(
        !batch.contains_key(&Vid::new(1)),
        "batch props must respect the tombstone winner (no resurrection); got {batch:?}"
    );

    let batch_labels = MainVertexDataset::find_batch_labels_by_vids(backend, &[Vid::new(1)], None)
        .await
        .unwrap();
    // FIXED (main_vertex.rs): same tombstone respect for labels.
    assert!(
        !batch_labels.contains_key(&Vid::new(1)),
        "batch labels must respect the tombstone winner (no resurrection); got {batch_labels:?}"
    );
}
