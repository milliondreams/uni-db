// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! M4 scoring/fusion/observability tests for sparse vectors (issue #95, set B).
//!
//! Covers the reachable-from-Cypher surface added in M4: the scalar
//! `sparse_similar_to(a, b)` dot product, the three-way (`dense + fts + sparse`)
//! `uni.search` hybrid under both RRF and weighted fusion, the `sparse_score`
//! output column, and the `.explain()` `SparseRrf` fusion-kind label. The
//! two-way hybrid is asserted unchanged when no `sparse` source is present.

use std::collections::HashMap;
use uni_db::{DataType, IndexType, Uni, Value};

const VOCAB: usize = 1000;

fn sv(indices: Vec<u32>, values: Vec<f32>) -> Value {
    Value::SparseVector { indices, values }
}

/// `true` if a row's column is absent or SQL-NULL.
fn is_null(row: &uni_db::Row, col: &str) -> bool {
    matches!(row.value(col), None | Some(Value::Null))
}

/// Brute-force dot product over the parallel arrays, in f64.
fn dot(qi: &[u32], qv: &[f32], di: &[u32], dv: &[f32]) -> f64 {
    let qm: HashMap<u32, f64> = qi.iter().zip(qv).map(|(&t, &w)| (t, w as f64)).collect();
    di.iter()
        .zip(dv)
        .filter_map(|(&t, &w)| qm.get(&t).map(|qw| qw * w as f64))
        .sum()
}

// ---------------------------------------------------------------------------
// Scalar `sparse_similar_to`
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sparse_similar_to_scalar_equals_dot() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;

    let a_i = vec![1u32, 5, 9, 42];
    let a_v = vec![1.0f32, 2.0, 3.0, 0.5];
    let b_i = vec![5u32, 9, 100];
    let b_v = vec![4.0f32, 1.0, 9.0];
    let want = dot(&a_i, &a_v, &b_i, &b_v); // overlap on terms 5, 9 → 2*4 + 3*1 = 11

    let rows = db
        .session()
        .query_with("RETURN sparse_similar_to($a, $b) AS d")
        .param("a", sv(a_i, a_v))
        .param("b", sv(b_i, b_v))
        .fetch_all()
        .await?;

    let got = rows.rows()[0].get::<f64>("d").unwrap();
    assert!(
        (got - want).abs() < 1e-6,
        "sparse_similar_to scalar = {got}, want {want}"
    );
    Ok(())
}

#[tokio::test]
async fn sparse_similar_to_zero_overlap_is_zero() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    let rows = db
        .session()
        .query_with("RETURN sparse_similar_to($a, $b) AS d")
        .param("a", sv(vec![1, 2, 3], vec![1.0, 1.0, 1.0]))
        .param("b", sv(vec![4, 5, 6], vec![1.0, 1.0, 1.0]))
        .fetch_all()
        .await?;
    assert_eq!(rows.rows()[0].get::<f64>("d").unwrap(), 0.0);
    Ok(())
}

#[tokio::test]
async fn sparse_similar_to_null_propagates() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    let rows = db
        .session()
        .query_with("RETURN sparse_similar_to($a, null) AS d")
        .param("a", sv(vec![1, 2], vec![1.0, 1.0]))
        .fetch_all()
        .await?;
    // NULL operand → NULL result (3VL).
    assert!(is_null(&rows.rows()[0], "d"));
    Ok(())
}

// ---------------------------------------------------------------------------
// Three-way hybrid (dense + fts + sparse)
// ---------------------------------------------------------------------------

/// Build a `Doc` label with dense-vector, FTS, and sparse indexes, plus a small
/// corpus. The doc titled `target` is the sparse-query self-match (the sparse
/// dot maximizer).
async fn setup_hybrid(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("content", DataType::String)
        .property("embedding", DataType::Vector { dimensions: 2 })
        .property("emb", DataType::SparseVector { dimensions: VOCAB })
        .index("emb", IndexType::Sparse { dimensions: VOCAB })
        .apply()
        .await?;

    let q = (vec![1u32, 5, 9], vec![1.0f32, 2.0, 3.0]);
    let tx = db.session().tx().await?;
    // target: sparse emb == query (max dot); also FTS-relevant text.
    tx.execute_with("CREATE (:Doc {title: $t, content: $c, embedding: [0.9, 0.1], emb: $e})")
        .param("t", Value::String("target".into()))
        .param("c", Value::String("zebra stripes pattern".into()))
        .param("e", sv(q.0.clone(), q.1.clone()))
        .run()
        .await?;
    tx.execute_with("CREATE (:Doc {title: $t, content: $c, embedding: [0.1, 0.9], emb: $e})")
        .param("t", Value::String("other".into()))
        .param("c", Value::String("a quiet meadow".into()))
        .param("e", sv(vec![100, 200], vec![1.0, 1.0]))
        .run()
        .await?;
    tx.commit().await?;

    let tx2 = db.session().tx().await?;
    tx2.execute("CREATE FULLTEXT INDEX doc_fts FOR (d:Doc) ON EACH [d.content]")
        .await?;
    tx2.commit().await?;

    db.flush().await?;
    db.indexes().rebuild("Doc", false).await?;
    Ok(())
}

/// Run a three-way `uni.search` with the given fusion options and return
/// `(title, score, sparse_score)` rows in engine order.
async fn run_hybrid(db: &Uni, options: &str) -> anyhow::Result<Vec<(String, f64, Option<f64>)>> {
    let q = (vec![1u32, 5, 9], vec![1.0f32, 2.0, 3.0]);
    let cypher = format!(
        "CALL uni.search('Doc', {{vector: 'embedding', fts: 'content', sparse: 'emb'}}, \
         'zebra', $qvec, 5, null, {options}) \
         YIELD node, score, sparse_score \
         RETURN node.title AS title, score, sparse_score"
    );
    let rows = db
        .session()
        .query_with(&cypher)
        .param(
            "qvec",
            Value::List(vec![Value::Float(0.9), Value::Float(0.1)]),
        )
        .param("sq", sv(q.0, q.1))
        .fetch_all()
        .await?;
    Ok(rows
        .iter()
        .map(|r| {
            (
                r.get::<String>("title").unwrap(),
                r.get::<f64>("score").unwrap(),
                r.get::<f64>("sparse_score").ok(),
            )
        })
        .collect())
}

#[tokio::test]
async fn hybrid_three_way_rrf_populates_sparse_score() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    setup_hybrid(&db).await?;

    let results = run_hybrid(&db, "{method: 'rrf', sparse_query: $sq}").await?;
    assert!(!results.is_empty(), "hybrid returned no rows");

    // The target's sparse_score is its exact self-dot (1+4+9 = 14).
    let target = results
        .iter()
        .find(|(t, _, _)| t == "target")
        .expect("target present");
    let ss = target.2.expect("sparse_score column populated for target");
    assert!(
        (ss - 14.0).abs() < 1e-3,
        "target sparse_score = {ss}, want 14.0"
    );
    // target is the unique sparse maximizer → it should be ranked first.
    assert_eq!(
        results[0].0, "target",
        "target should rank first: {results:?}"
    );
    Ok(())
}

#[tokio::test]
async fn hybrid_three_way_weighted_populates_sparse_score() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    setup_hybrid(&db).await?;

    // Weighted fusion with the sparse source dominant.
    let results = run_hybrid(
        &db,
        "{method: 'weighted', weights: [0.1, 0.1, 0.8], sparse_query: $sq}",
    )
    .await?;
    assert!(!results.is_empty());
    let target = results
        .iter()
        .find(|(t, _, _)| t == "target")
        .expect("target present");
    assert!(
        target.2.expect("sparse_score populated") > 0.0,
        "target sparse_score should be positive"
    );
    assert_eq!(
        results[0].0, "target",
        "sparse-dominant weighted → target first"
    );
    Ok(())
}

#[tokio::test]
async fn hybrid_two_way_unchanged_without_sparse() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    setup_hybrid(&db).await?;

    // No `sparse` property → pure dense+fts hybrid. Must still work and the
    // sparse_score column is null/absent for every row.
    let rows = db
        .session()
        .query_with(
            "CALL uni.search('Doc', {vector: 'embedding', fts: 'content'}, 'zebra', $qvec, 5) \
             YIELD node, score, sparse_score \
             RETURN node.title AS title, sparse_score",
        )
        .param(
            "qvec",
            Value::List(vec![Value::Float(0.9), Value::Float(0.1)]),
        )
        .fetch_all()
        .await?;
    assert!(!rows.is_empty(), "two-way hybrid returned no rows");
    for r in rows.iter() {
        assert!(
            is_null(r, "sparse_score"),
            "sparse_score must be null when no sparse source is queried"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Observability: EXPLAIN shows the SparseRrf fusion kind
// ---------------------------------------------------------------------------

#[tokio::test]
async fn explain_hybrid_with_sparse_shows_sparse_rrf() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    setup_hybrid(&db).await?;

    let plan = db
        .session()
        .query_with(
            "CALL uni.search('Doc', {vector: 'embedding', fts: 'content', sparse: 'emb'}, \
             'zebra', $qvec, 5, null, {sparse_query: $sq}) \
             YIELD node, score RETURN node.title AS title",
        )
        .param(
            "qvec",
            Value::List(vec![Value::Float(0.9), Value::Float(0.1)]),
        )
        .param("sq", sv(vec![1, 5, 9], vec![1.0, 2.0, 3.0]))
        .explain()
        .await?;
    assert!(
        plan.plan_text.contains("SparseRrf"),
        "expected SparseRrf fusion kind in plan; got {}",
        plan.plan_text
    );

    // A two-way hybrid (no sparse key) must NOT carry the SparseRrf label.
    let plan2 = db
        .session()
        .query_with(
            "CALL uni.search('Doc', {vector: 'embedding', fts: 'content'}, 'zebra', $qvec, 5) \
             YIELD node, score RETURN node.title AS title",
        )
        .param(
            "qvec",
            Value::List(vec![Value::Float(0.9), Value::Float(0.1)]),
        )
        .explain()
        .await?;
    assert!(
        !plan2.plan_text.contains("SparseRrf"),
        "two-way hybrid must not be labeled SparseRrf; got {}",
        plan2.plan_text
    );
    Ok(())
}
