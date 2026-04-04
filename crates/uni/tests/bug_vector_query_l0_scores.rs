// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression tests for: `uni.vector.query` returns zero scores on L0 brute-force fallback.
//!
//! When no HNSW index exists the query falls back to brute-force KNN.  The
//! scores must still be correctly computed (identical vectors ≈ 1.0, orthogonal
//! vectors < identical).

use tempfile::tempdir;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_db::Uni;

/// Scores must be non-zero when querying without an HNSW index.
#[tokio::test]
async fn test_vector_query_scores_without_hnsw_index() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema with vector property
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Item")?;
    schema_manager.add_property("Item", "name", DataType::String, false)?;
    schema_manager.add_property(
        "Item",
        "embedding",
        DataType::Vector { dimensions: 3 },
        false,
    )?;
    schema_manager.save().await?;

    let db = Uni::open(path.to_str().unwrap()).build().await?;

    // 2. Insert data with known embeddings: A and B identical, C orthogonal
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Item {name: 'A', embedding: [1.0, 0.0, 0.0]})")
        .await?;
    tx.execute("CREATE (:Item {name: 'B', embedding: [1.0, 0.0, 0.0]})")
        .await?;
    tx.execute("CREATE (:Item {name: 'C', embedding: [0.0, 0.0, 1.0]})")
        .await?;
    tx.commit().await?;

    // 3. Flush but DO NOT rebuild indexes — force brute-force path
    db.flush().await?;

    // 4. Query for items similar to [1.0, 0.0, 0.0]
    let res = db
        .session()
        .query(
            "CALL uni.vector.query('Item', 'embedding', [1.0, 0.0, 0.0], 10)
             YIELD node, score
             RETURN node.name AS name, score
             ORDER BY score DESC",
        )
        .await?;

    assert!(
        res.len() >= 2,
        "Should find items even without HNSW index, got {}",
        res.len()
    );

    // 5. Identical vectors must have score ~1.0
    let top_score: f64 = res.rows()[0].get("score")?;
    assert!(
        top_score > 0.9,
        "Identical vector should have score near 1.0, got {} (0.0 indicates L0 score bug)",
        top_score
    );

    // 6. All scores must be non-zero
    for row in res.rows() {
        let name: String = row.get("name")?;
        let score: f64 = row.get("score")?;
        assert!(
            score > 0.0,
            "Score for '{}' should be > 0.0 even without HNSW index, got {}",
            name,
            score
        );
    }

    // 7. Orthogonal vector should score lower than identical ones
    let score_for = |target: &str| -> f64 {
        res.rows()
            .iter()
            .find_map(|r| {
                let name: String = r.get("name").unwrap();
                (name == target).then(|| r.get::<f64>("score").unwrap())
            })
            .unwrap_or_else(|| panic!("Expected to find item '{target}' in results"))
    };

    let a_score = score_for("A");
    let c_score = score_for("C");

    assert!(
        a_score > c_score,
        "Identical vector (A: {}) should score higher than orthogonal (C: {})",
        a_score,
        c_score
    );

    Ok(())
}

/// Scores should be consistent between L0 (no index) and indexed paths.
#[tokio::test]
async fn test_vector_query_score_consistency_with_and_without_index() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Item")?;
    schema_manager.add_property("Item", "name", DataType::String, false)?;
    schema_manager.add_property(
        "Item",
        "embedding",
        DataType::Vector { dimensions: 3 },
        false,
    )?;
    schema_manager.save().await?;

    let db = Uni::open(path.to_str().unwrap()).build().await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Item {name: 'X', embedding: [1.0, 0.0, 0.0]})")
        .await?;
    tx.execute("CREATE (:Item {name: 'Y', embedding: [0.7, 0.7, 0.0]})")
        .await?;
    tx.commit().await?;

    db.flush().await?;

    // 2. Query WITHOUT index
    let res_no_index = db
        .session()
        .query(
            "CALL uni.vector.query('Item', 'embedding', [1.0, 0.0, 0.0], 10)
             YIELD node, score
             RETURN node.name AS name, score
             ORDER BY score DESC",
        )
        .await?;

    // 3. Build index and query again
    db.indexes().rebuild("Item", false).await?;

    let res_with_index = db
        .session()
        .query(
            "CALL uni.vector.query('Item', 'embedding', [1.0, 0.0, 0.0], 10)
             YIELD node, score
             RETURN node.name AS name, score
             ORDER BY score DESC",
        )
        .await?;

    // 4. Both paths should return the same items
    assert_eq!(
        res_no_index.len(),
        res_with_index.len(),
        "Same number of results with and without index"
    );

    // 5. Scores should be consistent (within tolerance)
    for (row_no_idx, row_with_idx) in res_no_index.rows().iter().zip(res_with_index.rows()) {
        let name_no: String = row_no_idx.get("name")?;
        let name_with: String = row_with_idx.get("name")?;
        let score_no: f64 = row_no_idx.get("score")?;
        let score_with: f64 = row_with_idx.get("score")?;

        assert_eq!(name_no, name_with, "Same ordering with and without index");
        assert!(
            (score_no - score_with).abs() < 0.1,
            "Scores should be consistent: without={}, with={} for '{}'",
            score_no,
            score_with,
            name_no
        );
    }

    Ok(())
}

/// BUG-1 regression: vector search must see L0 data after tx.commit()
/// WITHOUT an explicit flush.
#[tokio::test]
async fn test_vector_query_sees_l0_data_without_flush() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Doc")?;
    schema_manager.add_property("Doc", "title", DataType::String, false)?;
    schema_manager.add_property(
        "Doc",
        "embedding",
        DataType::Vector { dimensions: 3 },
        false,
    )?;
    schema_manager.save().await?;

    let db = Uni::open(path.to_str().unwrap()).build().await?;

    // Insert data and commit — but do NOT flush
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {title: 'hello', embedding: [1.0, 0.0, 0.0]})")
        .await?;
    tx.execute("CREATE (:Doc {title: 'world', embedding: [0.0, 1.0, 0.0]})")
        .await?;
    tx.commit().await?;

    // No flush! Data is still in L0 only.
    // Vector search must still find it via merge_l0_into_vector_results.
    let res = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0, 0.0], 10)
             YIELD node, score
             RETURN node.title AS title, score
             ORDER BY score DESC",
        )
        .await?;

    assert!(
        res.len() >= 2,
        "Vector search should find L0 data without flush, got {} rows",
        res.len()
    );

    // Top result should be the identical vector
    let top_title: String = res.rows()[0].get("title")?;
    assert_eq!(
        top_title, "hello",
        "Identical vector should be top result"
    );

    let top_score: f64 = res.rows()[0].get("score")?;
    assert!(
        top_score > 0.9,
        "Identical vector should have score near 1.0, got {}",
        top_score
    );

    Ok(())
}
