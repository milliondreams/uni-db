// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Tests for JsonPathIndex write/read roundtrip.

use tempfile::tempdir;
use uni_common::core::id::Vid;
use uni_store::storage::json_index::JsonPathIndex;

#[test]
fn test_json_index_schema() {
    let schema = JsonPathIndex::get_arrow_schema();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "value");
    assert_eq!(schema.field(1).name(), "vids");
}

#[tokio::test]
async fn test_json_index_write_and_query() {
    let dir = tempdir().unwrap();
    let base_uri = dir.path().to_str().unwrap();
    let index = JsonPathIndex::new(base_uri, "Person", "$.name");

    // Write entries
    let entries = vec![
        ("Alice".to_string(), vec![Vid::from(1u64), Vid::from(2u64)]),
        ("Bob".to_string(), vec![Vid::from(3u64)]),
    ];
    index.write_entries(entries).await.unwrap();

    // Query for "Alice"
    let vids = index.get_vids("Alice").await.unwrap();
    assert_eq!(vids.len(), 2, "Alice should map to 2 VIDs");
    assert!(vids.contains(&Vid::from(1u64)));
    assert!(vids.contains(&Vid::from(2u64)));

    // Query for "Bob"
    let vids = index.get_vids("Bob").await.unwrap();
    assert_eq!(vids.len(), 1);
    assert!(vids.contains(&Vid::from(3u64)));
}

#[tokio::test]
async fn test_json_index_query_nonexistent() {
    let dir = tempdir().unwrap();
    let base_uri = dir.path().to_str().unwrap();
    let index = JsonPathIndex::new(base_uri, "Person", "$.name");

    // Write some data first
    index
        .write_entries(vec![("Alice".to_string(), vec![Vid::from(1u64)])])
        .await
        .unwrap();

    // Query for nonexistent value
    let vids = index.get_vids("Charlie").await.unwrap();
    assert!(vids.is_empty(), "Nonexistent value should return empty");
}

#[tokio::test]
async fn test_json_index_open_nonexistent() {
    let dir = tempdir().unwrap();
    let base_uri = dir.path().to_str().unwrap();
    let index = JsonPathIndex::new(base_uri, "X", "$.y");

    // get_vids on non-existent dataset should return empty (not error)
    let vids = index.get_vids("anything").await.unwrap();
    assert!(vids.is_empty());
}
