#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! M4 cutover dispatch tests for host-coupled built-in procedures.
//!
//! Proves that `uni.schema.*` and `uni.algo.*` are served by
//! `uni-query::procedures_plugin` registrations through the framework
//! plugin path, not by the deleted hardcoded match arms in
//! `procedure_call.rs`.

use anyhow::Result;
use uni_common::core::schema::DataType;
use uni_db::Uni;

// Rust guideline compliant

async fn seed(db: &Uni, cypher: &str) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(cypher).await?;
    tx.commit().await?;
    Ok(())
}

async fn ensure_labels(db: &Uni, labels: &[&str]) -> Result<()> {
    for l in labels {
        db.schema().label(l).apply().await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Schema introspection — uni.schema.*
// ---------------------------------------------------------------------------

#[tokio::test]
async fn call_uni_schema_labels_returns_seeded_labels() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    ensure_labels(&db, &["Person", "Company"]).await?;
    seed(&db, "CREATE (:Person {name: 'a'}), (:Company {name: 'c'})").await?;
    let result = db
        .session()
        .query("CALL uni.schema.labels() YIELD label RETURN label")
        .await?;
    let labels: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get::<String>("label").unwrap())
        .collect();
    assert!(labels.iter().any(|l| l == "Person"));
    assert!(labels.iter().any(|l| l == "Company"));
    Ok(())
}

#[tokio::test]
async fn call_uni_schema_edge_types_returns_seeded_edge_types() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    ensure_labels(&db, &["Person"]).await?;
    db.schema()
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()
        .await?;
    seed(&db, "CREATE (a:Person)-[:KNOWS]->(b:Person)").await?;
    let result = db
        .session()
        .query("CALL uni.schema.edgeTypes() YIELD type RETURN type")
        .await?;
    let types: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get::<String>("type").unwrap())
        .collect();
    assert!(types.iter().any(|t| t == "KNOWS"));
    Ok(())
}

#[tokio::test]
async fn call_uni_schema_relationship_types_alias_works() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    ensure_labels(&db, &["Person"]).await?;
    db.schema()
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()
        .await?;
    seed(&db, "CREATE (a:Person)-[:KNOWS]->(b:Person)").await?;
    let result = db
        .session()
        .query("CALL uni.schema.relationshipTypes() YIELD type RETURN type")
        .await?;
    let types: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get::<String>("type").unwrap())
        .collect();
    assert!(types.iter().any(|t| t == "KNOWS"));
    Ok(())
}

#[tokio::test]
async fn call_uni_schema_indexes_returns_seeded_indexes() -> Result<()> {
    use uni_db::{IndexType, ScalarType};
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Doc")
        .property("body", DataType::String)
        .index("body", IndexType::Scalar(ScalarType::BTree))
        .apply()
        .await?;
    seed(&db, "CREATE (:Doc {body: 'hello'})").await?;
    let result = db
        .session()
        .query("CALL uni.schema.indexes() YIELD label, type RETURN label, type")
        .await?;
    let rows: Vec<(String, String)> = result
        .rows()
        .iter()
        .map(|r| {
            (
                r.get::<String>("label").unwrap(),
                r.get::<String>("type").unwrap(),
            )
        })
        .collect();
    assert!(rows.iter().any(|(l, _)| l == "Doc"));
    Ok(())
}

#[tokio::test]
async fn call_uni_schema_label_info_returns_property_metadata() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Widget")
        .property("sku", DataType::String)
        .property("price", DataType::Float)
        .apply()
        .await?;
    seed(&db, "CREATE (:Widget {sku: 'abc', price: 1.5})").await?;
    let result = db
        .session()
        .query("CALL uni.schema.labelInfo('Widget') YIELD property RETURN property")
        .await?;
    let props: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get::<String>("property").unwrap())
        .collect();
    assert!(props.iter().any(|p| p == "sku"));
    assert!(props.iter().any(|p| p == "price"));
    Ok(())
}

// ---------------------------------------------------------------------------
// Algorithm adapter — uni.algo.*
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Search procedures — uni.vector.query / uni.fts.query / uni.search
// ---------------------------------------------------------------------------

#[tokio::test]
async fn call_uni_vector_query_via_plugin_succeeds() -> Result<()> {
    use uni_db::{IndexType, VectorAlgo, VectorIndexCfg, VectorMetric};
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Doc")
        .property("text", DataType::String)
        .vector("embedding", 3)
        .index(
            "embedding",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::L2,
                embedding: None,
            }),
        )
        .apply()
        .await?;
    seed(
        &db,
        "CREATE (:Doc {text: 'a', embedding: [1.0, 0.0, 0.0]}), \
         (:Doc {text: 'b', embedding: [0.0, 1.0, 0.0]}), \
         (:Doc {text: 'c', embedding: [0.0, 0.0, 1.0]})",
    )
    .await?;

    let result = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0, 0.0], 2) \
             YIELD vid RETURN vid",
        )
        .await?;
    assert!(
        !result.rows().is_empty(),
        "vector.query should return at least one row"
    );
    Ok(())
}

#[tokio::test]
async fn call_uni_fts_query_via_plugin_succeeds() -> Result<()> {
    use uni_db::IndexType;
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Doc")
        .property("body", DataType::String)
        .index("body", IndexType::FullText)
        .apply()
        .await?;
    seed(
        &db,
        "CREATE (:Doc {body: 'the quick brown fox'}), \
         (:Doc {body: 'lazy dog'}), \
         (:Doc {body: 'quick rabbit'})",
    )
    .await?;

    let result = db
        .session()
        .query("CALL uni.fts.query('Doc', 'body', 'quick', 5) YIELD vid RETURN vid")
        .await?;
    assert!(
        !result.rows().is_empty(),
        "fts.query should return at least one row"
    );
    Ok(())
}

#[tokio::test]
async fn call_uni_search_hybrid_via_plugin_succeeds() -> Result<()> {
    use uni_db::{IndexType, VectorAlgo, VectorIndexCfg, VectorMetric};
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Doc")
        .property("body", DataType::String)
        .vector("embedding", 3)
        .index(
            "embedding",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::L2,
                embedding: None,
            }),
        )
        .index("body", IndexType::FullText)
        .apply()
        .await?;
    seed(
        &db,
        "CREATE (:Doc {body: 'quick fox', embedding: [1.0, 0.0, 0.0]}), \
         (:Doc {body: 'lazy dog', embedding: [0.0, 1.0, 0.0]}), \
         (:Doc {body: 'quick rabbit', embedding: [0.0, 0.0, 1.0]})",
    )
    .await?;

    let result = db
        .session()
        .query(
            "CALL uni.search('Doc', {vector: 'embedding', fts: 'body'}, \
             'quick', [1.0, 0.0, 0.0], 2) YIELD vid RETURN vid",
        )
        .await?;
    assert!(
        !result.rows().is_empty(),
        "hybrid search should return at least one row"
    );
    Ok(())
}

#[tokio::test]
async fn call_uni_algo_page_rank_via_plugin_adapter_succeeds() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    ensure_labels(&db, &["N"]).await?;
    db.schema().edge_type("E", &["N"], &["N"]).apply().await?;
    seed(
        &db,
        "CREATE (a:N {id: 1})-[:E]->(b:N {id: 2}), \
         (b)-[:E]->(c:N {id: 3}), \
         (c)-[:E]->(a)",
    )
    .await?;
    // The adapter resolves the algo plugin through the framework
    // PluginRegistry (the `uni.algo.*` legacy match arm is deleted).
    let result = db
        .session()
        .query("CALL uni.algo.pageRank({nodeLabels: ['N'], edgeTypes: ['E']}, {}) YIELD score RETURN score")
        .await?;
    assert!(
        !result.rows().is_empty(),
        "pageRank should yield at least one row"
    );
    for row in result.rows() {
        let s: f64 = row.get("score").unwrap_or(0.0);
        assert!(s.is_finite(), "score must be finite, got {s}");
    }
    Ok(())
}
