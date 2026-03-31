// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression test for rebuild_indexes path mismatch bug.
//!
//! `VertexDataset::new()` was constructing URIs as `vertices/Label` (slash-separated),
//! but LanceDB stores tables as `vertices_Label.lance` (underscore-separated).
//! This caused `rebuild_indexes` to silently fail — the underlying `open_raw()`
//! couldn't find the dataset, so no HNSW vector indexes were ever built.

use tempfile::tempdir;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_db::Uni;

#[tokio::test]
async fn test_rebuild_indexes_finds_correct_dataset() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema with a vector property
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Chunk")?;
    schema_manager.add_property("Chunk", "text", DataType::String, false)?;
    schema_manager.add_property(
        "Chunk",
        "embedding",
        DataType::Vector { dimensions: 3 },
        false,
    )?;
    schema_manager.save().await?;

    // 2. Create database and insert vertices with embeddings
    let db = Uni::open(path.to_str().unwrap()).build().await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Chunk {text: 'alpha', embedding: [1.0, 0.0, 0.0]})")
        .await?;
    tx.execute("CREATE (:Chunk {text: 'beta', embedding: [0.0, 1.0, 0.0]})")
        .await?;
    tx.execute("CREATE (:Chunk {text: 'gamma', embedding: [0.0, 0.0, 1.0]})")
        .await?;
    tx.commit().await?;

    db.flush().await?;

    // 3. rebuild_indexes should NOT error (previously it silently failed
    //    because VertexDataset.uri pointed to a non-existent path)
    db.rebuild_indexes("Chunk", false).await?;

    // 4. Verify vector search still works after rebuild
    let result = db
        .session()
        .query(
            "CALL uni.vector.query('Chunk', 'embedding', [1.0, 0.0, 0.0], 3)
             YIELD node, distance
             RETURN node.text AS text, distance",
        )
        .await?;

    assert_eq!(result.len(), 3, "expected 3 results from vector search");

    // The closest result to [1, 0, 0] should be 'alpha'
    let first_text: String = result.rows()[0].get("text")?;
    assert_eq!(first_text, "alpha", "closest vector should be 'alpha'");

    // The distance of the exact match should be 0 (or very close)
    let first_dist: f64 = result.rows()[0].get("distance")?;
    assert!(
        first_dist < 0.01,
        "exact match distance should be ~0, got {}",
        first_dist
    );

    Ok(())
}
