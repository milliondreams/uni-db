// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end test for multi-vector (ColBERT / late-interaction) MaxSim
//! reranking (issue #96, Phase 1).
//!
//! A `Doc` label carries a dense `embedding` (for the first-stage ANN) plus a
//! `tokens` multi-vector property (`List<Vector>`). `uni.vector.query` fetches
//! candidates by dense distance, then the `reranker: 'maxsim'` mode rescores
//! them by MaxSim against a query multi-vector — no neural model, no index.

use uni_db::{DataType, QueryResult, Uni};

/// Extracts titles from a query result, in result order.
fn titles(result: &QueryResult) -> Vec<String> {
    result
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect()
}

/// DB with three docs whose dense order (distance to `[1,0]`) is DocA, DocB,
/// DocC, but whose MaxSim order against query tokens `{e0, e1}` is DocB first:
///
/// - DocA tokens `{e0}`      -> MaxSim = 1 (e0) + 0 (e1) = 1.0
/// - DocB tokens `{e0, e1}`  -> MaxSim = 1 (e0) + 1 (e1) = 2.0  (best)
/// - DocC tokens `{e1}`      -> MaxSim = 0 (e0) + 1 (e1) = 1.0
async fn setup_db() -> anyhow::Result<Uni> {
    let db = Uni::temporary().build().await?;

    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("embedding", DataType::Vector { dimensions: 2 })
        // Multi-vector (ColBERT) token embeddings: a variable-count set per row.
        .property(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: 2 })),
        )
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    // DocA: dense-closest to [1,0]; single token e0.
    tx.execute("CREATE (:Doc {title: 'DocA', embedding: [1.0, 0.0], tokens: [[1.0, 0.0]]})")
        .await?;
    // DocB: dense second; two tokens {e0, e1} -> wins MaxSim.
    tx.execute(
        "CREATE (:Doc {title: 'DocB', embedding: [0.9, 0.1], tokens: [[1.0, 0.0], [0.0, 1.0]]})",
    )
    .await?;
    // DocC: dense farthest; single token e1.
    tx.execute("CREATE (:Doc {title: 'DocC', embedding: [0.0, 1.0], tokens: [[0.0, 1.0]]})")
        .await?;
    tx.commit().await?;

    db.flush().await?;
    db.indexes().rebuild("Doc", false).await?;

    Ok(db)
}

#[tokio::test]
async fn test_maxsim_rerank_reorders_results() -> anyhow::Result<()> {
    let db = setup_db().await?;

    let result = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 3, null, null, \
             {reranker: 'maxsim', reranker_property: 'tokens', maxsim_query: [[1.0, 0.0], [0.0, 1.0]]}) \
             YIELD node, score, rerank_score \
             RETURN node.title AS title, rerank_score",
        )
        .await?;

    let order = titles(&result);
    assert_eq!(order.len(), 3, "all three docs should be returned");
    // DocB has the highest MaxSim (2.0) and must lead, even though it is NOT the
    // dense-closest (DocA is).
    assert_eq!(order[0], "DocB", "highest-MaxSim doc should rank first");
    assert_ne!(
        order[0], "DocA",
        "dense-closest doc should not lead after MaxSim rerank"
    );

    // The rerank_score column is populated, and DocB's equals its MaxSim (2.0,
    // cosine over unit vectors), strictly above the runners-up (1.0).
    let top_score = result.rows()[0].get::<f64>("rerank_score")?;
    let second_score = result.rows()[1].get::<f64>("rerank_score")?;
    assert!(
        (top_score - 2.0).abs() < 1e-5,
        "DocB rerank_score should be MaxSim 2.0, got {top_score}"
    );
    assert!(
        top_score > second_score,
        "DocB rerank_score ({top_score}) should exceed the next ({second_score})"
    );

    Ok(())
}

#[tokio::test]
async fn test_maxsim_via_hybrid_search() -> anyhow::Result<()> {
    // The maxsim branch lives in the shared rerank stage, so it must also work
    // through uni.search (hybrid), not just uni.vector.query.
    let db = setup_db().await?;

    let result = db
        .session()
        .query(
            "CALL uni.search('Doc', {vector: 'embedding'}, 'unused', [1.0, 0.0], 3, null, \
             {reranker: 'maxsim', reranker_property: 'tokens', maxsim_query: [[1.0, 0.0], [0.0, 1.0]]}) \
             YIELD node, rerank_score \
             RETURN node.title AS title, rerank_score",
        )
        .await?;

    assert_eq!(
        titles(&result)[0],
        "DocB",
        "maxsim should rerank in hybrid path too"
    );
    Ok(())
}

#[tokio::test]
async fn test_maxsim_metric_option_dot() -> anyhow::Result<()> {
    // Non-unit token so Dot (6.0) and the default Cosine (1.0) diverge; passing
    // maxsim_metric:'dot' must change the rerank_score, proving the option threads
    // through parse_reranker_options -> RerankerConfig -> maxsim.
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("embedding", DataType::Vector { dimensions: 2 })
        .property(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: 2 })),
        )
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {title: 'D', embedding: [1.0, 0.0], tokens: [[3.0, 0.0]]})")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    db.indexes().rebuild("Doc", false).await?;

    let score_for = |opts: &str| {
        let q = format!(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 1, null, null, {opts}) \
             YIELD node, rerank_score RETURN rerank_score"
        );
        q
    };

    let dot = db
        .session()
        .query(&score_for(
            "{reranker: 'maxsim', reranker_property: 'tokens', maxsim_query: [[2.0, 0.0]], maxsim_metric: 'dot'}",
        ))
        .await?;
    let cosine = db
        .session()
        .query(&score_for(
            "{reranker: 'maxsim', reranker_property: 'tokens', maxsim_query: [[2.0, 0.0]]}",
        ))
        .await?;

    let dot_score = dot.rows()[0].get::<f64>("rerank_score")?;
    let cos_score = cosine.rows()[0].get::<f64>("rerank_score")?;
    assert!((dot_score - 6.0).abs() < 1e-4, "dot score {dot_score}");
    assert!((cos_score - 1.0).abs() < 1e-4, "cosine score {cos_score}");
    Ok(())
}

#[tokio::test]
async fn test_maxsim_dimension_mismatch_errors() -> anyhow::Result<()> {
    // A query token whose dimension differs from the stored token dimension must
    // surface as a query error, not a silent wrong score.
    let db = setup_db().await?;
    let res = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 3, null, null, \
             {reranker: 'maxsim', reranker_property: 'tokens', maxsim_query: [[1.0, 0.0, 0.0]]}) \
             YIELD node RETURN node.title AS title",
        )
        .await;
    assert!(res.is_err(), "dimension mismatch should error");
    Ok(())
}

#[tokio::test]
async fn test_maxsim_malformed_query_errors() -> anyhow::Result<()> {
    // A flat list (not a list-of-vectors) is malformed and must error rather than
    // silently skipping the rerank.
    let db = setup_db().await?;
    let res = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 3, null, null, \
             {reranker: 'maxsim', reranker_property: 'tokens', maxsim_query: [1.0, 2.0]}) \
             YIELD node RETURN node.title AS title",
        )
        .await;
    assert!(res.is_err(), "flat (non-nested) maxsim_query should error");
    Ok(())
}

#[tokio::test]
async fn test_maxsim_empty_tokens_scores_zero() -> anyhow::Result<()> {
    // A declared property is non-nullable, so the realistic "no document tokens"
    // case is an EMPTY multi-vector (`tokens: []`). It must score 0 (no tokens to
    // match) and rank below a candidate with matching tokens — not crash.
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("embedding", DataType::Vector { dimensions: 2 })
        .property(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: 2 })),
        )
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    // HasTokens matches the query; EmptyTokens has a present-but-empty multivec.
    tx.execute("CREATE (:Doc {title: 'HasTokens', embedding: [1.0, 0.0], tokens: [[1.0, 0.0]]})")
        .await?;
    tx.execute("CREATE (:Doc {title: 'EmptyTokens', embedding: [0.95, 0.05], tokens: []})")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    db.indexes().rebuild("Doc", false).await?;

    let result = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 2, null, null, \
             {reranker: 'maxsim', reranker_property: 'tokens', maxsim_query: [[1.0, 0.0]]}) \
             YIELD node, rerank_score \
             RETURN node.title AS title, rerank_score",
        )
        .await?;

    let order = titles(&result);
    assert_eq!(
        order[0], "HasTokens",
        "doc with matching tokens should lead"
    );
    // The empty-token doc is still returned, scored 0.
    let last = result.rows().last().unwrap();
    assert_eq!(last.get::<String>("title")?, "EmptyTokens");
    assert!(
        last.get::<f64>("rerank_score")?.abs() < 1e-6,
        "empty-token candidate should score 0"
    );
    Ok(())
}

// NOTE: declaring a `List<Vector>` property via Cypher `CREATE LABEL` DDL is NOT
// possible today, but this is a pre-existing grammar limitation unrelated to
// multi-vector: `property_definition` (cypher.pest) accepts only a single
// bare-token type, so NO parameterized type — `VECTOR(N)`, `LIST<STRING>`,
// `LIST<VECTOR(N)>` — is expressible in `CREATE LABEL`. Multi-vector properties
// are declared through the schema builder API (see `setup_db` above); Cypher is
// used for writes (list-of-lists literals) and queries (maxsim rerank).

#[tokio::test]
async fn test_maxsim_rerank_missing_query_errors() -> anyhow::Result<()> {
    let db = setup_db().await?;

    // `reranker: 'maxsim'` without a `maxsim_query` must fail loudly rather than
    // silently skipping the rerank.
    let res = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 3, null, null, \
             {reranker: 'maxsim', reranker_property: 'tokens'}) \
             YIELD node RETURN node.title AS title",
        )
        .await;

    assert!(res.is_err(), "maxsim without maxsim_query should error");
    Ok(())
}
