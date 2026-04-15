// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! WAL durability and edge case tests.
//!
//! Tests cover:
//! - LSN ordering guarantees
//! - Replay idempotency
//! - Partial segment recovery
//! - Concurrent WAL operations
//! - Empty WAL handling

use anyhow::Result;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path;
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::core::id::{Eid, Vid};
use uni_store::runtime::wal::{Mutation, WriteAheadLog};

fn create_memory_store() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

#[tokio::test]
async fn test_wal_lsn_ordering() -> Result<()> {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    // Flush multiple segments
    wal.append(&Mutation::InsertVertex {
        vid: Vid::new(100),
        properties: HashMap::new(),
        labels: vec![],
    })?;
    let lsn1 = wal.flush().await?;

    wal.append(&Mutation::InsertVertex {
        vid: Vid::new(101),
        properties: HashMap::new(),
        labels: vec![],
    })?;
    let lsn2 = wal.flush().await?;

    wal.append(&Mutation::InsertVertex {
        vid: Vid::new(102),
        properties: HashMap::new(),
        labels: vec![],
    })?;
    let lsn3 = wal.flush().await?;

    // Verify LSNs are monotonically increasing
    assert!(lsn1 < lsn2, "LSN2 should be greater than LSN1");
    assert!(lsn2 < lsn3, "LSN3 should be greater than LSN2");

    Ok(())
}

#[tokio::test]
async fn test_wal_replay_since_high_water_mark() -> Result<()> {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    // Create segments with known LSNs
    wal.append(&Mutation::InsertVertex {
        vid: Vid::new(100),
        properties: HashMap::new(),
        labels: vec![],
    })?;
    let lsn1 = wal.flush().await?;

    wal.append(&Mutation::InsertVertex {
        vid: Vid::new(101),
        properties: HashMap::new(),
        labels: vec![],
    })?;
    let lsn2 = wal.flush().await?;

    wal.append(&Mutation::InsertVertex {
        vid: Vid::new(102),
        properties: HashMap::new(),
        labels: vec![],
    })?;
    wal.flush().await?;

    // Replay all mutations
    let all_mutations = wal.replay_since(0).await?;
    assert_eq!(all_mutations.len(), 3, "Should have 3 mutations total");

    // Replay only mutations since LSN1 (should get 2 mutations)
    let since_lsn1 = wal.replay_since(lsn1).await?;
    assert_eq!(since_lsn1.len(), 2, "Should have 2 mutations since LSN1");

    // Replay only mutations since LSN2 (should get 1 mutation)
    let since_lsn2 = wal.replay_since(lsn2).await?;
    assert_eq!(since_lsn2.len(), 1, "Should have 1 mutation since LSN2");

    Ok(())
}

#[tokio::test]
async fn test_wal_empty_flush() -> Result<()> {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    // Flush empty buffer should return flushed_lsn (0 initially)
    let lsn = wal.flush().await?;
    assert_eq!(lsn, 0, "Empty flush should return current flushed_lsn");

    // Add a mutation and flush
    wal.append(&Mutation::InsertVertex {
        vid: Vid::new(100),
        properties: HashMap::new(),
        labels: vec![],
    })?;
    let lsn1 = wal.flush().await?;
    assert!(lsn1 > 0, "LSN should be positive after flush");

    // Another empty flush should return the same LSN
    let lsn2 = wal.flush().await?;
    assert_eq!(lsn1, lsn2, "Empty flush should return same LSN");

    Ok(())
}

#[tokio::test]
async fn test_wal_truncate_before_high_water_mark() -> Result<()> {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    // Create multiple segments
    for i in 0..5 {
        wal.append(&Mutation::InsertVertex {
            vid: Vid::new(100 + i),
            properties: HashMap::new(),
            labels: vec![],
        })?;
        wal.flush().await?;
    }

    // Replay all (should have 5 mutations)
    let all = wal.replay_since(0).await?;
    assert_eq!(all.len(), 5);

    // Get LSN after 3rd flush
    let lsn3 = 3;

    // Truncate segments with LSN <= 3
    wal.truncate_before(lsn3).await?;

    // Replay should now only have mutations from LSN > 3
    let remaining = wal.replay_since(0).await?;
    assert_eq!(
        remaining.len(),
        2,
        "Should have 2 mutations after truncating first 3"
    );

    Ok(())
}

#[tokio::test]
async fn test_wal_initialize_from_existing() -> Result<()> {
    let store = create_memory_store();
    let path = Path::from("wal");

    // Create WAL and flush some segments
    {
        let wal = WriteAheadLog::new(store.clone(), path.clone());
        for i in 0..5 {
            wal.append(&Mutation::InsertVertex {
                vid: Vid::new(100 + i),
                properties: HashMap::new(),
                labels: vec![],
            })?;
            wal.flush().await?;
        }
    }

    // Create new WAL instance and initialize
    let wal2 = WriteAheadLog::new(store.clone(), path);
    let max_lsn = wal2.initialize().await?;

    // Max LSN should be 5 (5 segments flushed)
    assert_eq!(
        max_lsn, 5,
        "Max LSN should match number of segments flushed"
    );

    // Next flush should get LSN 6
    wal2.append(&Mutation::InsertVertex {
        vid: Vid::new(105),
        properties: HashMap::new(),
        labels: vec![],
    })?;
    let next_lsn = wal2.flush().await?;
    assert_eq!(next_lsn, 6, "Next LSN should continue from max");

    Ok(())
}

#[tokio::test]
async fn test_wal_edge_mutations() -> Result<()> {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    let test_eid = Eid::new(1100);
    let src_vid = Vid::new(100);
    let dst_vid = Vid::new(200);

    // Insert edge
    wal.append(&Mutation::InsertEdge {
        src_vid,
        dst_vid,
        edge_type: 1,
        eid: test_eid,
        version: 1,
        properties: [("weight".to_string(), uni_common::Value::Float(1.5))]
            .into_iter()
            .collect(),
        edge_type_name: None,
    })?;

    // Delete edge
    wal.append(&Mutation::DeleteEdge {
        eid: test_eid,
        src_vid,
        dst_vid,
        edge_type: 1,
        version: 2,
    })?;

    wal.flush().await?;

    // Replay and verify
    let mutations = wal.replay().await?;
    assert_eq!(mutations.len(), 2);

    match &mutations[0] {
        Mutation::InsertEdge {
            eid,
            edge_type,
            properties,
            ..
        } => {
            assert_eq!(eid.as_u64(), 1100);
            assert_eq!(*edge_type, 1);
            assert!(properties.contains_key("weight"));
        }
        _ => panic!("Expected InsertEdge mutation"),
    }

    match &mutations[1] {
        Mutation::DeleteEdge { eid, version, .. } => {
            assert_eq!(eid.as_u64(), 1100);
            assert_eq!(*version, 2);
        }
        _ => panic!("Expected DeleteEdge mutation"),
    }

    Ok(())
}

#[tokio::test]
async fn test_wal_delete_vertex_mutation() -> Result<()> {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    let test_vid = Vid::new(100);

    // Insert then delete vertex
    wal.append(&Mutation::InsertVertex {
        vid: test_vid,
        properties: [(
            "name".to_string(),
            uni_common::Value::String("Test".to_string()),
        )]
        .into_iter()
        .collect(),
        labels: vec![],
    })?;

    wal.append(&Mutation::DeleteVertex {
        vid: test_vid,
        labels: vec![],
    })?;

    wal.flush().await?;

    let mutations = wal.replay().await?;
    assert_eq!(mutations.len(), 2);

    match &mutations[1] {
        Mutation::DeleteVertex { vid, .. } => {
            assert_eq!(vid.as_u64(), 100);
        }
        _ => panic!("Expected DeleteVertex mutation"),
    }

    Ok(())
}

#[tokio::test]
async fn test_wal_concurrent_flushes() -> Result<()> {
    let store = create_memory_store();
    let wal = Arc::new(WriteAheadLog::new(store.clone(), Path::from("wal")));

    // Pre-populate with some mutations
    for i in 0..100 {
        wal.append(&Mutation::InsertVertex {
            vid: Vid::new(100 + i),
            properties: HashMap::new(),
            labels: vec![],
        })?;
    }

    // Spawn concurrent flush tasks
    let mut handles = Vec::new();
    for _ in 0..10 {
        let wal_clone = wal.clone();
        handles.push(tokio::spawn(async move { wal_clone.flush().await }));
    }

    // Wait for all flushes
    let mut lsns = Vec::new();
    for handle in handles {
        let lsn = handle.await??;
        if lsn > 0 {
            lsns.push(lsn);
        }
    }

    // At least one flush should succeed (the first one with data)
    assert!(!lsns.is_empty() || wal.flushed_lsn().unwrap() > 0);

    // Replay should have all 100 mutations
    let mutations = wal.replay().await?;
    assert_eq!(mutations.len(), 100);

    Ok(())
}

#[tokio::test]
async fn test_wal_flushed_lsn_tracking() -> Result<()> {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    // Initially flushed_lsn should be 0
    assert_eq!(wal.flushed_lsn().unwrap(), 0);

    // Flush with data
    wal.append(&Mutation::InsertVertex {
        vid: Vid::new(100),
        properties: HashMap::new(),
        labels: vec![],
    })?;
    wal.flush().await?;

    assert_eq!(wal.flushed_lsn().unwrap(), 1);

    // Another flush
    wal.append(&Mutation::InsertVertex {
        vid: Vid::new(101),
        properties: HashMap::new(),
        labels: vec![],
    })?;
    wal.flush().await?;

    assert_eq!(wal.flushed_lsn().unwrap(), 2);

    Ok(())
}

#[tokio::test]
async fn test_wal_full_truncate() -> Result<()> {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    // Create segments
    for i in 0..5 {
        wal.append(&Mutation::InsertVertex {
            vid: Vid::new(100 + i),
            properties: HashMap::new(),
            labels: vec![],
        })?;
        wal.flush().await?;
    }

    // Full truncate
    wal.truncate().await?;

    // Replay should return empty
    let mutations = wal.replay().await?;
    assert!(mutations.is_empty());

    Ok(())
}

// ============================================================================
// Issue #76 Regression Tests: Tombstone Label Flushing
// ============================================================================

/// Test that replay restores vertex labels from InsertVertex mutations
#[tokio::test]
async fn test_replay_restores_vertex_labels() -> Result<()> {
    use uni_store::runtime::L0Buffer;

    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    // Create mutations with labels
    let mutations = vec![
        Mutation::InsertVertex {
            vid: Vid::new(100),
            properties: HashMap::new(),
            labels: vec!["Person".to_string(), "User".to_string()],
        },
        Mutation::InsertVertex {
            vid: Vid::new(101),
            properties: HashMap::new(),
            labels: vec!["Person".to_string()],
        },
    ];

    // Serialize to WAL
    for m in &mutations {
        wal.append(m)?;
    }
    wal.flush().await?;

    // Replay into L0 buffer
    let replayed = wal.replay().await?;
    let mut l0 = L0Buffer::new(0, None);
    l0.replay_mutations(replayed)?;

    // Verify labels were restored
    let labels_100 = l0.vertex_labels.get(&Vid::new(100)).unwrap();
    assert_eq!(labels_100.len(), 2);
    assert!(labels_100.contains(&"Person".to_string()));
    assert!(labels_100.contains(&"User".to_string()));

    let labels_101 = l0.vertex_labels.get(&Vid::new(101)).unwrap();
    assert_eq!(labels_101.len(), 1);
    assert!(labels_101.contains(&"Person".to_string()));

    Ok(())
}

/// Test that replay of DeleteVertex preserves labels for tombstoned vertices
#[tokio::test]
async fn test_replay_delete_preserves_labels_for_flush() -> Result<()> {
    use uni_store::runtime::L0Buffer;

    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    // Insert then delete with labels preserved
    let mutations = vec![
        Mutation::InsertVertex {
            vid: Vid::new(100),
            properties: HashMap::new(),
            labels: vec!["Person".to_string()],
        },
        Mutation::DeleteVertex {
            vid: Vid::new(100),
            labels: vec!["Person".to_string()],
        },
    ];

    for m in &mutations {
        wal.append(m)?;
    }
    wal.flush().await?;

    // Replay into L0
    let replayed = wal.replay().await?;
    let mut l0 = L0Buffer::new(0, None);
    l0.replay_mutations(replayed)?;

    // Verify vertex is tombstoned but labels are still present in vertex_labels map
    assert!(l0.vertex_tombstones.contains(&Vid::new(100)));
    let labels = l0.vertex_labels.get(&Vid::new(100)).unwrap();
    assert_eq!(labels.len(), 1);
    assert!(labels.contains(&"Person".to_string()));

    Ok(())
}

/// Test backward compatibility: deserialize old WAL without labels field
#[tokio::test]
async fn test_wal_serde_backward_compat_missing_labels() -> Result<()> {
    // Simulate old WAL format without labels field
    let old_insert = r#"{"InsertVertex":{"vid":100,"properties":{}}}"#;
    let old_delete = r#"{"DeleteVertex":{"vid":100}}"#;

    // Should deserialize with default empty labels
    let insert: Mutation = serde_json::from_str(old_insert)?;
    match insert {
        Mutation::InsertVertex { vid, labels, .. } => {
            assert_eq!(vid.as_u64(), 100);
            assert!(labels.is_empty());
        }
        _ => panic!("Expected InsertVertex"),
    }

    let delete: Mutation = serde_json::from_str(old_delete)?;
    match delete {
        Mutation::DeleteVertex { vid, labels } => {
            assert_eq!(vid.as_u64(), 100);
            assert!(labels.is_empty());
        }
        _ => panic!("Expected DeleteVertex"),
    }

    Ok(())
}

/// Test full WAL cycle: append DeleteVertex with labels, flush, replay
#[tokio::test]
async fn test_wal_delete_vertex_labels_round_trip() -> Result<()> {
    use uni_store::runtime::L0Buffer;

    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    // Create DeleteVertex with labels
    let delete_mutation = Mutation::DeleteVertex {
        vid: Vid::new(100),
        labels: vec!["Person".to_string(), "Admin".to_string()],
    };

    wal.append(&delete_mutation)?;
    wal.flush().await?;

    // Replay in new L0
    let replayed = wal.replay().await?;
    assert_eq!(replayed.len(), 1);

    let mut l0 = L0Buffer::new(0, None);
    l0.replay_mutations(replayed)?;

    // Verify labels were preserved through the cycle
    let labels = l0.vertex_labels.get(&Vid::new(100)).unwrap();
    assert_eq!(labels.len(), 2);
    assert!(labels.contains(&"Person".to_string()));
    assert!(labels.contains(&"Admin".to_string()));

    Ok(())
}

// ============================================================================
// Edge Type Name WAL Tests (Issue #28, #102)
// ============================================================================

/// Test that edge type names are preserved through WAL round-trip
#[tokio::test]
async fn test_edge_type_name_wal_roundtrip() -> Result<()> {
    use uni_store::runtime::L0Buffer;

    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    let test_eid = Eid::new(1100);
    let src_vid = Vid::new(100);
    let dst_vid = Vid::new(200);

    // Append InsertEdge with edge_type_name
    wal.append(&Mutation::InsertEdge {
        src_vid,
        dst_vid,
        edge_type: 1,
        eid: test_eid,
        version: 1,
        properties: HashMap::new(),
        edge_type_name: Some("KNOWS".to_string()),
    })?;
    wal.flush().await?;

    // Replay into L0 buffer
    let replayed = wal.replay().await?;
    let mut l0 = L0Buffer::new(0, None);
    l0.replay_mutations(replayed)?;

    // Verify edge type name was restored
    let edge_type_name = l0.get_edge_type(test_eid);
    assert_eq!(
        edge_type_name,
        Some("KNOWS"),
        "Edge type name should be restored from WAL"
    );

    Ok(())
}

/// Test backward compatibility: deserialize old WAL without edge_type_name field
#[tokio::test]
async fn test_wal_serde_backward_compat_missing_edge_type_name() -> Result<()> {
    // Simulate old WAL format without edge_type_name field
    let old_insert_edge = r#"{"InsertEdge":{"src_vid":100,"dst_vid":200,"edge_type":1,"eid":1100,"version":1,"properties":{}}}"#;

    // Should deserialize with default None for edge_type_name
    let mutation: Mutation = serde_json::from_str(old_insert_edge)?;
    match mutation {
        Mutation::InsertEdge {
            eid,
            edge_type,
            edge_type_name,
            ..
        } => {
            assert_eq!(eid.as_u64(), 1100);
            assert_eq!(edge_type, 1);
            assert_eq!(
                edge_type_name, None,
                "edge_type_name should default to None for old WAL format"
            );
        }
        _ => panic!("Expected InsertEdge mutation"),
    }

    Ok(())
}

// ── WAL Corruption Recovery Tests ────────────────────────────────────

#[tokio::test]
async fn test_wal_truncated_segment_recovery() {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));
    wal.initialize().await.unwrap();

    // Write a valid segment first
    wal.append(&Mutation::InsertVertex {
        vid: Vid::new(1),
        labels: vec!["Person".to_string()],
        properties: HashMap::new(),
    })
    .unwrap();
    wal.flush().await.unwrap();

    // Write a truncated (invalid JSON) segment directly
    let truncated_path = Path::from("wal/00000000000000000099_bad.wal");
    store
        .put(
            &truncated_path,
            bytes::Bytes::from(b"{\"lsn\":99,\"mutations\":[{\"Inser" as &[u8]).into(),
        )
        .await
        .unwrap();

    // Replaying should fail on the corrupt segment or skip it.
    // We verify it doesn't panic.
    let result = wal.replay_since(0).await;
    let _ = result;
}

#[tokio::test]
async fn test_wal_corrupted_segment_data() {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));
    wal.initialize().await.unwrap();

    // Write random bytes as a WAL segment
    let bad_path = Path::from("wal/00000000000000000001_corrupt.wal");
    store
        .put(
            &bad_path,
            bytes::Bytes::from(vec![0xDE, 0xAD, 0xBE, 0xEF]).into(),
        )
        .await
        .unwrap();

    // Replay should handle corrupt data gracefully (error or skip)
    let result = wal.replay_since(0).await;
    let _ = result; // Should not panic
}

#[tokio::test]
async fn test_wal_empty_segment_file() {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));
    wal.initialize().await.unwrap();

    // Write a zero-byte WAL segment
    let empty_path = Path::from("wal/00000000000000000001_empty.wal");
    store
        .put(&empty_path, bytes::Bytes::new().into())
        .await
        .unwrap();

    // Empty segments should be handled gracefully
    let result = wal.replay_since(0).await;
    let _ = result; // Should not panic
}

// ============================================================================
// Transaction Commit Atomicity Tests (Issue #137)
// ============================================================================
// NOTE: These tests are currently disabled because they rely on an old Writer API
// that used UniConfig::new() instead of Writer::new(storage, schema, writer_id).
// The transaction API tests should be re-implemented using the current API patterns
// once the transaction feature is fully integrated with the new storage architecture.
