// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

// Test for issue #80: Out-of-Order Compaction Corruption
// Verifies that delta operations are correctly ordered by version regardless of scan order

use arrow_array::{
    LargeBinaryArray, RecordBatch, TimestampNanosecondArray, UInt8Array, UInt64Array,
};
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::id::{Eid, Vid};
use uni_db::core::schema::SchemaManager;
use uni_db::storage::compaction::Compactor;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_out_of_order_delta_operations() -> anyhow::Result<()> {
    // This test verifies that when delta operations are scanned in the wrong order
    // (Delete before Insert), the compaction still produces the correct result
    // (Delete wins because it has a higher version).

    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");
    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&schema_path).await?;
    schema_manager.add_label("Person")?;
    schema_manager.add_edge_type("knows", vec!["Person".into()], vec!["Person".into()])?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(StorageManager::new(storage_str, schema_manager.clone()).await?);
    let compactor = Compactor::new(storage.clone());

    // 2. Directly write delta entries to LanceDB in WRONG ORDER
    // We'll write: Delete(eid=1, v=2), then Insert(eid=1, v=1)
    // Expected result after compaction: eid=1 should be ABSENT (Delete wins)

    let vid_a = Vid::new(1);
    let vid_b = Vid::new(2);
    let eid1 = Eid::new(1);

    let delta_ds = storage.delta_dataset("knows", "fwd")?;
    let arrow_schema = delta_ds.get_arrow_schema(&schema_manager.schema())?;
    let lancedb_store = storage.lancedb_store();

    // Batch 1: Delete(eid=1, version=2) - later operation written first
    use arrow_array::LargeBinaryArray;
    let batch1 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![vid_a.as_u64()])), // src_vid
            Arc::new(UInt64Array::from(vec![vid_b.as_u64()])), // dst_vid
            Arc::new(UInt64Array::from(vec![eid1.as_u64()])),  // eid
            Arc::new(UInt8Array::from(vec![1u8])),             // op: 1=DELETE
            Arc::new(UInt64Array::from(vec![2u64])),           // version=2
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>]).with_timezone("UTC")), // _created_at
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>]).with_timezone("UTC")), // _updated_at
            Arc::new(LargeBinaryArray::from(vec![Some(b"{}" as &[u8])])), // overflow_json (JSONB)
        ],
    )?;

    delta_ds.write_run_lancedb(lancedb_store, batch1).await?;

    // Batch 2: Insert(eid=1, version=1) - earlier operation written second
    let batch2 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![vid_a.as_u64()])), // src_vid
            Arc::new(UInt64Array::from(vec![vid_b.as_u64()])), // dst_vid
            Arc::new(UInt64Array::from(vec![eid1.as_u64()])),  // eid
            Arc::new(UInt8Array::from(vec![0u8])),             // op: 0=INSERT
            Arc::new(UInt64Array::from(vec![1u64])),           // version=1
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>]).with_timezone("UTC")), // _created_at
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>]).with_timezone("UTC")), // _updated_at
            Arc::new(LargeBinaryArray::from(vec![Some(b"{}" as &[u8])])), // overflow_json (JSONB)
        ],
    )?;

    delta_ds.write_run_lancedb(lancedb_store, batch2).await?;

    // 3. Run Compaction
    let _ = compactor
        .compact_adjacency("knows", "Person", "fwd")
        .await?;

    // 4. Verify: eid=1 should be ABSENT because Delete(v=2) wins over Insert(v=1)
    let adj_ds = storage.adjacency_dataset("knows", "Person", "fwd")?;
    let l2_data = adj_ds.read_adjacency_lancedb(lancedb_store, vid_a).await?;

    // The edge should not exist after compaction
    assert!(
        l2_data.is_none(),
        "Edge should be absent after Delete(v=2) beats Insert(v=1)"
    );

    Ok(())
}

#[tokio::test]
async fn test_multiple_out_of_order_operations() -> anyhow::Result<()> {
    // Test multiple edges with interleaved operations in wrong order
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");
    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&schema_path).await?;
    schema_manager.add_label("Person")?;
    schema_manager.add_edge_type("knows", vec!["Person".into()], vec!["Person".into()])?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(StorageManager::new(storage_str, schema_manager.clone()).await?);
    let compactor = Compactor::new(storage.clone());

    let vid_a = Vid::new(1);
    let vid_b = Vid::new(2);
    let vid_c = Vid::new(3);
    let eid1 = Eid::new(1);
    let eid2 = Eid::new(2);

    let delta_ds = storage.delta_dataset("knows", "fwd")?;
    let arrow_schema = delta_ds.get_arrow_schema(&schema_manager.schema())?;
    let lancedb_store = storage.lancedb_store();

    // Write operations in scrambled order:
    // 1. Delete(eid1, v=3) - should win
    // 2. Insert(eid2, v=2) - should survive
    // 3. Insert(eid1, v=1) - should be overridden by Delete
    // 4. Insert(eid1, v=2) - should be overridden by Delete

    let operations = vec![
        (vid_a, vid_b, eid1, 1u8, 3u64), // Delete eid1, v=3
        (vid_a, vid_c, eid2, 0u8, 2u64), // Insert eid2, v=2
        (vid_a, vid_b, eid1, 0u8, 1u64), // Insert eid1, v=1
        (vid_a, vid_b, eid1, 0u8, 2u64), // Insert eid1, v=2
    ];

    for (src, dst, eid, op, version) in operations {
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![src.as_u64()])),
                Arc::new(UInt64Array::from(vec![dst.as_u64()])),
                Arc::new(UInt64Array::from(vec![eid.as_u64()])),
                Arc::new(UInt8Array::from(vec![op])),
                Arc::new(UInt64Array::from(vec![version])),
                Arc::new(TimestampNanosecondArray::from(vec![None::<i64>]).with_timezone("UTC")),
                Arc::new(TimestampNanosecondArray::from(vec![None::<i64>]).with_timezone("UTC")),
                Arc::new(LargeBinaryArray::from(vec![Some(b"{}" as &[u8])])),
            ],
        )?;

        delta_ds.write_run_lancedb(lancedb_store, batch).await?;
    }

    // Run Compaction
    let _ = compactor
        .compact_adjacency("knows", "Person", "fwd")
        .await?;

    // Verify: Only eid2 should exist (eid1 was deleted)
    let adj_ds = storage.adjacency_dataset("knows", "Person", "fwd")?;
    let l2_data = adj_ds.read_adjacency_lancedb(lancedb_store, vid_a).await?;

    assert!(l2_data.is_some(), "Should have at least one edge");
    let (neighbors, eids) = l2_data.unwrap();

    assert_eq!(neighbors.len(), 1, "Should have exactly one neighbor");
    assert_eq!(neighbors[0], vid_c, "Should be connected to vid_c");
    assert_eq!(eids.len(), 1, "Should have exactly one edge");
    assert_eq!(eids[0], eid2, "Should be eid2 (eid1 was deleted)");

    Ok(())
}

#[tokio::test]
async fn test_insert_delete_insert_sequence() -> anyhow::Result<()> {
    // Test Insert(v=1), Delete(v=2), Insert(v=3) sequence
    // Expected: final Insert(v=3) should win
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");
    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    let schema_manager = SchemaManager::load(&schema_path).await?;
    schema_manager.add_label("Person")?;
    schema_manager.add_edge_type("knows", vec!["Person".into()], vec!["Person".into()])?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(StorageManager::new(storage_str, schema_manager.clone()).await?);
    let compactor = Compactor::new(storage.clone());

    let vid_a = Vid::new(1);
    let vid_b = Vid::new(2);
    let eid1 = Eid::new(1);

    let delta_ds = storage.delta_dataset("knows", "fwd")?;
    let arrow_schema = delta_ds.get_arrow_schema(&schema_manager.schema())?;
    let lancedb_store = storage.lancedb_store();

    // Write in reverse order to test sorting
    let operations = vec![
        (0u8, 3u64), // Insert v=3 (written first)
        (1u8, 2u64), // Delete v=2 (written second)
        (0u8, 1u64), // Insert v=1 (written third)
    ];

    for (op, version) in operations {
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![vid_a.as_u64()])),
                Arc::new(UInt64Array::from(vec![vid_b.as_u64()])),
                Arc::new(UInt64Array::from(vec![eid1.as_u64()])),
                Arc::new(UInt8Array::from(vec![op])),
                Arc::new(UInt64Array::from(vec![version])),
                Arc::new(TimestampNanosecondArray::from(vec![None::<i64>]).with_timezone("UTC")),
                Arc::new(TimestampNanosecondArray::from(vec![None::<i64>]).with_timezone("UTC")),
                Arc::new(LargeBinaryArray::from(vec![Some(b"{}" as &[u8])])),
            ],
        )?;

        delta_ds.write_run_lancedb(lancedb_store, batch).await?;
    }

    // Run Compaction
    let _ = compactor
        .compact_adjacency("knows", "Person", "fwd")
        .await?;

    // Verify: Edge should exist (Insert(v=3) wins over Delete(v=2))
    let adj_ds = storage.adjacency_dataset("knows", "Person", "fwd")?;
    let l2_data = adj_ds.read_adjacency_lancedb(lancedb_store, vid_a).await?;

    assert!(l2_data.is_some(), "Edge should exist after Insert(v=3)");
    let (neighbors, eids) = l2_data.unwrap();

    assert_eq!(neighbors.len(), 1, "Should have exactly one neighbor");
    assert_eq!(neighbors[0], vid_b, "Should be connected to vid_b");
    assert_eq!(eids.len(), 1, "Should have exactly one edge");
    assert_eq!(eids[0], eid1, "Should be eid1");

    Ok(())
}
