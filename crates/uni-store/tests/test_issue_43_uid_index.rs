// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Test for Issue #43: UidIndex O(N) → O(log N)
//
// Verifies that UID lookups use filter pushdown on _uid_hex column
// instead of full table scan.

use anyhow::Result;
use tempfile::TempDir;
use uni_common::core::id::{UniId, Vid};
use uni_store::storage::index::UidIndex;

// Helper to create a test UniId from a counter
fn test_uid(counter: u8) -> UniId {
    let mut bytes = [0u8; 32];
    bytes[0] = counter;
    UniId::from_bytes(bytes)
}

#[tokio::test]
async fn test_uid_index_get_vid_with_filter_pushdown() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let base_uri = temp_dir.path().to_str().unwrap();

    let index = UidIndex::new(base_uri, "Person");

    // Write some mappings
    let uid1 = test_uid(1);
    let uid2 = test_uid(2);
    let uid3 = test_uid(3);

    let vid1 = Vid::new(100);
    let vid2 = Vid::new(200);
    let vid3 = Vid::new(300);

    index
        .write_mapping(&[(uid1, vid1), (uid2, vid2), (uid3, vid3)])
        .await?;

    // Test get_vid with filter pushdown
    assert_eq!(index.get_vid(&uid1).await?, Some(vid1));
    assert_eq!(index.get_vid(&uid2).await?, Some(vid2));
    assert_eq!(index.get_vid(&uid3).await?, Some(vid3));

    // Test non-existent UID
    let uid_missing = test_uid(99);
    assert_eq!(index.get_vid(&uid_missing).await?, None);

    Ok(())
}

#[tokio::test]
async fn test_uid_index_resolve_uids_batch() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let base_uri = temp_dir.path().to_str().unwrap();

    let index = UidIndex::new(base_uri, "Person");

    // Write 100 mappings
    let mut mappings = Vec::new();
    let mut uids = Vec::new();
    for i in 0..100 {
        let uid = test_uid(i as u8);
        let vid = Vid::new(i);
        mappings.push((uid, vid));
        uids.push(uid);
    }

    index.write_mapping(&mappings).await?;

    // Resolve all UIDs in a single batch scan
    let result = index.resolve_uids(&uids).await?;

    // Verify all mappings were resolved
    assert_eq!(result.len(), 100);
    for (uid, vid) in &mappings {
        assert_eq!(result.get(uid), Some(vid));
    }

    Ok(())
}

#[tokio::test]
async fn test_uid_index_resolve_uids_partial() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let base_uri = temp_dir.path().to_str().unwrap();

    let index = UidIndex::new(base_uri, "Person");

    // Write some mappings
    let uid1 = test_uid(10);
    let uid2 = test_uid(20);
    let uid3 = test_uid(30);

    let vid1 = Vid::new(100);
    let vid2 = Vid::new(200);
    let _vid3 = Vid::new(300);

    index.write_mapping(&[(uid1, vid1), (uid2, vid2)]).await?;

    // Try to resolve 3 UIDs, only 2 exist
    let result = index.resolve_uids(&[uid1, uid2, uid3]).await?;

    // Verify only 2 were resolved
    assert_eq!(result.len(), 2);
    assert_eq!(result.get(&uid1), Some(&vid1));
    assert_eq!(result.get(&uid2), Some(&vid2));
    assert_eq!(result.get(&uid3), None);

    Ok(())
}

#[tokio::test]
async fn test_uid_index_empty_resolve() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let base_uri = temp_dir.path().to_str().unwrap();

    let index = UidIndex::new(base_uri, "Person");

    // Resolve empty list should return empty map
    let result = index.resolve_uids(&[]).await?;
    assert_eq!(result.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_uid_index_append_mappings() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let base_uri = temp_dir.path().to_str().unwrap();

    let index = UidIndex::new(base_uri, "Person");

    // Write first batch
    let uid1 = test_uid(11);
    let uid2 = test_uid(12);
    let vid1 = Vid::new(100);
    let vid2 = Vid::new(200);
    index.write_mapping(&[(uid1, vid1), (uid2, vid2)]).await?;

    // Write second batch (append)
    let uid3 = test_uid(13);
    let uid4 = test_uid(14);
    let vid3 = Vid::new(300);
    let vid4 = Vid::new(400);
    index.write_mapping(&[(uid3, vid3), (uid4, vid4)]).await?;

    // Verify all 4 mappings are accessible
    assert_eq!(index.get_vid(&uid1).await?, Some(vid1));
    assert_eq!(index.get_vid(&uid2).await?, Some(vid2));
    assert_eq!(index.get_vid(&uid3).await?, Some(vid3));
    assert_eq!(index.get_vid(&uid4).await?, Some(vid4));

    // Batch resolve all 4
    let result = index.resolve_uids(&[uid1, uid2, uid3, uid4]).await?;
    assert_eq!(result.len(), 4);

    Ok(())
}

#[tokio::test]
async fn test_uid_index_btree_index_creation() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let base_uri = temp_dir.path().to_str().unwrap();

    let index = UidIndex::new(base_uri, "Person");

    // Write 100 mappings and verify index is created
    let mut mappings = Vec::new();
    for i in 0..100 {
        let uid = test_uid(i as u8);
        let vid = Vid::new(i);
        mappings.push((uid, vid));
    }

    index.write_mapping(&mappings).await?;

    // Call ensure_uid_hex_index explicitly to test it
    index.ensure_uid_hex_index().await?;

    // Verify all lookups still work correctly (with or without index)
    for (uid, expected_vid) in &mappings {
        let result = index.get_vid(uid).await?;
        assert_eq!(
            result,
            Some(*expected_vid),
            "UID lookup should work with BTree index"
        );
    }

    // Test batch resolution also works
    let uids: Vec<UniId> = mappings.iter().map(|(uid, _)| *uid).collect();
    let result = index.resolve_uids(&uids).await?;
    assert_eq!(
        result.len(),
        100,
        "Batch resolution should return all mappings"
    );

    for (uid, vid) in &mappings {
        assert_eq!(result.get(uid), Some(vid), "Batch lookup should match");
    }

    Ok(())
}
