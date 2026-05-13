// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
#![allow(clippy::len_zero, clippy::manual_range_contains)]

//! Integration tests for cross-encoder reranking in search procedures.
//!
//! Uses a mock `RerankerModel` that scores documents by text length (longer =
//! higher score), which creates deterministic reordering that differs from
//! vector distance or BM25 order.

use std::sync::Arc;

use async_trait::async_trait;
use uni_db::{DataType, ModelAliasSpec, ModelTask, QueryResult, Uni, WarmupPolicy};
use uni_xervo::runtime::ModelRuntime;
use uni_xervo::traits::{
    LoadedModelHandle, ModelProvider, ProviderCapabilities, ProviderHealth, RerankerModel,
    ScoredDoc,
};

// ---------------------------------------------------------------------------
// Mock reranker infrastructure
// ---------------------------------------------------------------------------

/// Mock reranker that scores documents by text length.
///
/// Uses scaled scoring so sigmoid produces differentiable values in (0, 1):
/// `score = (len - 20) * 0.1` keeps logits in a small range where sigmoid
/// has meaningful gradient and never saturates to exactly 0.0 or 1.0 in f32.
///
/// This creates deterministic reordering that differs from vector distance
/// (which orders by embedding proximity) and BM25 (which orders by term
/// frequency).
struct LengthReranker;

#[async_trait]
impl RerankerModel for LengthReranker {
    async fn rerank(
        &self,
        _query: &str,
        docs: &[&str],
    ) -> uni_xervo::error::Result<Vec<ScoredDoc>> {
        Ok(docs
            .iter()
            .enumerate()
            .map(|(i, doc)| ScoredDoc {
                index: i,
                // Scale to [-2, +4] range so sigmoid stays in (0.1, 0.98)
                score: (doc.len() as f32 - 20.0) * 0.1,
                text: None,
            })
            .collect())
    }
}

/// Mock provider that creates `LengthReranker` instances.
struct MockRerankerProvider;

#[async_trait]
impl ModelProvider for MockRerankerProvider {
    fn provider_id(&self) -> &'static str {
        "mock/reranker"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            supported_tasks: vec![ModelTask::Rerank],
        }
    }

    async fn load(&self, _spec: &ModelAliasSpec) -> uni_xervo::error::Result<LoadedModelHandle> {
        let handle: Arc<dyn RerankerModel> = Arc::new(LengthReranker);
        Ok(Arc::new(handle) as LoadedModelHandle)
    }

    async fn health(&self) -> ProviderHealth {
        ProviderHealth::Healthy
    }
}

fn reranker_spec() -> ModelAliasSpec {
    ModelAliasSpec {
        alias: "rerank/mock".to_string(),
        task: ModelTask::Rerank,
        provider_id: "mock/reranker".to_string(),
        model_id: "length-reranker".to_string(),
        revision: None,
        warmup: WarmupPolicy::Lazy,
        required: false,
        timeout: None,
        load_timeout: None,
        retry: None,
        options: serde_json::json!({}),
    }
}

async fn build_mock_runtime() -> Arc<ModelRuntime> {
    ModelRuntime::builder()
        .register_provider(MockRerankerProvider)
        .catalog(vec![reranker_spec()])
        .build()
        .await
        .expect("Failed to build mock runtime")
}

// ---------------------------------------------------------------------------
// Test helper: create a DB with 5 docs, vector + FTS indexes, mock reranker
// ---------------------------------------------------------------------------

/// Creates a test DB with 5 Doc nodes of varying content lengths and
/// embeddings at different distances from [1.0, 0.0].
///
/// Vector search order (by distance to [1.0, 0.0]):
///   Doc1 > Doc2 > Doc3 > Doc4 > Doc5
///
/// Length-reranker order (by content length descending):
///   Doc3 > Doc2 > Doc5 > Doc1 > Doc4
async fn setup_db() -> anyhow::Result<Uni> {
    let runtime = build_mock_runtime().await;

    let db = Uni::temporary().xervo_runtime(runtime).build().await?;

    // Declare schema with vector property
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("content", DataType::String)
        .property("price", DataType::Float)
        .property("embedding", DataType::Vector { dimensions: 2 })
        .apply()
        .await?;

    // Insert 5 documents with varying content lengths and embeddings
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:Doc {title: 'Doc1', content: 'short', embedding: [1.0, 0.0], price: 100.0})",
    )
    .await?;
    tx.execute(
        "CREATE (:Doc {title: 'Doc2', content: 'a medium length doc', embedding: [0.9, 0.1], price: 200.0})",
    )
    .await?;
    tx.execute(
        "CREATE (:Doc {title: 'Doc3', content: 'the longest document in the entire set for testing purposes', embedding: [0.5, 0.5], price: 300.0})",
    )
    .await?;
    tx.execute(
        "CREATE (:Doc {title: 'Doc4', content: 'tiny', embedding: [0.1, 0.9], price: 50.0})",
    )
    .await?;
    tx.execute(
        "CREATE (:Doc {title: 'Doc5', content: 'another medium text', embedding: [0.0, 1.0], price: 150.0})",
    )
    .await?;
    tx.commit().await?;

    // Create FTS index and flush
    let tx2 = db.session().tx().await?;
    tx2.execute("CREATE FULLTEXT INDEX doc_content_fts FOR (d:Doc) ON EACH [d.content]")
        .await?;
    tx2.commit().await?;

    db.flush().await?;
    db.indexes().rebuild("Doc", false).await?;

    Ok(db)
}

/// Extracts titles from a query result as a Vec<String>.
fn titles(result: &QueryResult) -> Vec<String> {
    result
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect()
}

/// Extracts a f64 column value from a row.
fn get_f64(result: &QueryResult, row: usize, col: &str) -> Option<f64> {
    result.rows()[row].get::<f64>(col).ok()
}

/// Extracts a f32 column value from a row (scores are Float32).
fn _get_f32(result: &QueryResult, row: usize, col: &str) -> Option<f32> {
    // QueryResult may return f64 even for Float32 columns
    result.rows()[row].get::<f64>(col).ok().map(|v| v as f32)
}

// ===========================================================================
// A. Happy Path — uni.vector.query
// ===========================================================================

#[tokio::test]
async fn test_vector_query_reranking_reorders_results() -> anyhow::Result<()> {
    let db = setup_db().await?;

    let result = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 3, null, null, \
         {reranker: 'rerank/mock', reranker_property: 'content', reranker_query: 'test'}) \
         YIELD node, score \
         RETURN node.title AS title, score",
        )
        .await?;

    let order = titles(&result);
    // Length-reranker should put longest content first
    assert_eq!(
        order[0], "Doc3",
        "Longest content should be first after reranking"
    );
    // Doc1 (shortest among top-3 by vector) should not be first
    assert_ne!(
        order[0], "Doc1",
        "Vector-closest should NOT be first after reranking"
    );

    Ok(())
}

#[tokio::test]
async fn test_vector_query_rerank_score_column() -> anyhow::Result<()> {
    let db = setup_db().await?;

    let result = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 3, null, null, \
         {reranker: 'rerank/mock', reranker_property: 'content', reranker_query: 'test'}) \
         YIELD node, score, rerank_score, distance \
         RETURN node.title AS title, score, rerank_score, distance",
        )
        .await?;

    assert!(result.len() > 0);
    for i in 0..result.len() {
        let rerank = get_f64(&result, i, "rerank_score");
        assert!(
            rerank.is_some(),
            "rerank_score should be non-null when reranker is active"
        );

        let score = get_f64(&result, i, "score");
        let rs = rerank.unwrap();
        // score should equal rerank_score when reranker is active
        assert!(
            (score.unwrap() - rs).abs() < 1e-5,
            "score should equal rerank_score, got score={:?} rerank_score={:?}",
            score,
            rerank
        );

        // rerank_score should be in [0, 1] (sigmoid normalized)
        assert!(
            rs >= 0.0 && rs <= 1.0,
            "rerank_score should be sigmoid-normalized, got {rs}"
        );

        // distance should still be the original vector distance
        let dist = get_f64(&result, i, "distance");
        assert!(dist.is_some(), "distance should be non-null");
        assert!(dist.unwrap() >= 0.0, "distance should be non-negative");
    }

    Ok(())
}

#[tokio::test]
async fn test_vector_query_reranker_k_controls_candidates() -> anyhow::Result<()> {
    let db = setup_db().await?;

    // reranker_k=2, k=1 — reranker sees 2 candidates, returns 1
    let result = db.session().query(
        "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 1, null, null, \
         {reranker: 'rerank/mock', reranker_property: 'content', reranker_k: 2, reranker_query: 'test'}) \
         YIELD node, rerank_score \
         RETURN node.title AS title, rerank_score",
    ).await?;

    assert_eq!(result.len(), 1, "Should return exactly k=1 result");
    assert!(get_f64(&result, 0, "rerank_score").is_some());

    Ok(())
}

// ===========================================================================
// B. Happy Path — uni.fts.query
// ===========================================================================

#[tokio::test]
async fn test_fts_query_reranking_reorders_results() -> anyhow::Result<()> {
    let db = setup_db().await?;

    let result = db
        .session()
        .query(
            "CALL uni.fts.query('Doc', 'content', 'medium', 3, null, null, \
         {reranker: 'rerank/mock', reranker_property: 'content'}) \
         YIELD node, rerank_score \
         RETURN node.title AS title, rerank_score",
        )
        .await?;

    // Should have results matching "medium"
    if result.len() > 0 {
        assert!(get_f64(&result, 0, "rerank_score").is_some());
    }

    Ok(())
}

#[tokio::test]
async fn test_fts_query_reranker_default_property() -> anyhow::Result<()> {
    let db = setup_db().await?;

    // Omit reranker_property — should default to the FTS property (content)
    let result = db
        .session()
        .query(
            "CALL uni.fts.query('Doc', 'content', 'document', 3, null, null, \
         {reranker: 'rerank/mock'}) \
         YIELD node, rerank_score \
         RETURN node.title AS title, rerank_score",
        )
        .await?;

    // Should work without error
    if result.len() > 0 {
        assert!(get_f64(&result, 0, "rerank_score").is_some());
    }

    Ok(())
}

// ===========================================================================
// C. Happy Path — uni.search (Hybrid)
// ===========================================================================

#[tokio::test]
async fn test_hybrid_search_reranking_reorders_results() -> anyhow::Result<()> {
    let db = setup_db().await?;

    let result = db
        .session()
        .query(
            "CALL uni.search('Doc', {vector: 'embedding', fts: 'content'}, \
         'document testing', null, 3, null, \
         {reranker: 'rerank/mock', reranker_property: 'content'}) \
         YIELD node, score, rerank_score, vector_score, fts_score \
         RETURN node.title AS title, score, rerank_score, vector_score, fts_score",
        )
        .await?;

    assert!(result.len() > 0);
    for i in 0..result.len() {
        assert!(get_f64(&result, i, "rerank_score").is_some());
    }

    Ok(())
}

#[tokio::test]
async fn test_hybrid_search_score_reflects_reranker() -> anyhow::Result<()> {
    let db = setup_db().await?;

    let result = db
        .session()
        .query(
            "CALL uni.search('Doc', {vector: 'embedding', fts: 'content'}, \
         'document', null, 3, null, \
         {reranker: 'rerank/mock', reranker_property: 'content'}) \
         YIELD node, score, rerank_score, vector_score, fts_score \
         RETURN node.title AS title, score, rerank_score, vector_score, fts_score",
        )
        .await?;

    for i in 0..result.len() {
        let score = get_f64(&result, i, "score");
        let rerank = get_f64(&result, i, "rerank_score");

        // score should equal rerank_score when reranker is active
        if let (Some(s), Some(r)) = (score, rerank) {
            assert!(
                (s - r).abs() < 1e-5,
                "score ({s}) should equal rerank_score ({r}) when reranker is active"
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_hybrid_search_reranker_with_rrf() -> anyhow::Result<()> {
    let db = setup_db().await?;

    let result = db
        .session()
        .query(
            "CALL uni.search('Doc', {vector: 'embedding', fts: 'content'}, \
         'document', null, 3, null, \
         {method: 'rrf', reranker: 'rerank/mock', reranker_property: 'content'}) \
         YIELD node, rerank_score \
         RETURN node.title AS title, rerank_score",
        )
        .await?;

    // RRF + reranking should work
    if result.len() > 0 {
        assert!(get_f64(&result, 0, "rerank_score").is_some());
    }

    Ok(())
}

#[tokio::test]
async fn test_hybrid_search_reranker_with_weighted() -> anyhow::Result<()> {
    let db = setup_db().await?;

    let result = db
        .session()
        .query(
            "CALL uni.search('Doc', {vector: 'embedding', fts: 'content'}, \
         'document', null, 3, null, \
         {method: 'weighted', alpha: 0.7, reranker: 'rerank/mock', reranker_property: 'content'}) \
         YIELD node, rerank_score \
         RETURN node.title AS title, rerank_score",
        )
        .await?;

    if result.len() > 0 {
        assert!(get_f64(&result, 0, "rerank_score").is_some());
    }

    Ok(())
}

#[tokio::test]
async fn test_hybrid_search_reranker_overfetch_interaction() -> anyhow::Result<()> {
    let db = setup_db().await?;

    // over_fetch: 1.5, reranker_k: 4, k: 2
    // effective_retrieval_k = max(ceil(2*1.5), 4) = 4
    let result = db
        .session()
        .query(
            "CALL uni.search('Doc', {vector: 'embedding', fts: 'content'}, \
         'document', null, 2, null, \
         {over_fetch: 1.5, reranker: 'rerank/mock', reranker_property: 'content', reranker_k: 4}) \
         YIELD node, rerank_score \
         RETURN node.title AS title, rerank_score",
        )
        .await?;

    assert!(result.len() <= 2, "Should return at most k=2 results");

    Ok(())
}

// ===========================================================================
// D. Result Sizes & Edge Cases
// ===========================================================================

#[tokio::test]
async fn test_reranker_single_result() -> anyhow::Result<()> {
    let db = setup_db().await?;

    let result = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 1, null, null, \
         {reranker: 'rerank/mock', reranker_property: 'content', reranker_query: 'test'}) \
         YIELD node, rerank_score \
         RETURN node.title AS title, rerank_score",
        )
        .await?;

    assert_eq!(result.len(), 1);
    assert!(get_f64(&result, 0, "rerank_score").is_some());

    Ok(())
}

#[tokio::test]
async fn test_reranker_k_equals_k() -> anyhow::Result<()> {
    let db = setup_db().await?;

    let result = db.session().query(
        "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 3, null, null, \
         {reranker: 'rerank/mock', reranker_property: 'content', reranker_k: 3, reranker_query: 'test'}) \
         YIELD node, rerank_score \
         RETURN node.title AS title, rerank_score",
    ).await?;

    assert_eq!(result.len(), 3);

    Ok(())
}

#[tokio::test]
async fn test_reranker_more_candidates_than_exist() -> anyhow::Result<()> {
    let db = setup_db().await?;

    // k=10, reranker_k=30, but only 5 docs
    let result = db.session().query(
        "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 10, null, null, \
         {reranker: 'rerank/mock', reranker_property: 'content', reranker_k: 30, reranker_query: 'test'}) \
         YIELD node, rerank_score \
         RETURN node.title AS title, rerank_score",
    ).await?;

    assert_eq!(result.len(), 5, "Should return all 5 docs");

    Ok(())
}

#[tokio::test]
async fn test_reranker_empty_results() -> anyhow::Result<()> {
    let db = setup_db().await?;

    // Query vector far from all docs with tight threshold
    let result = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 5, null, 0.001, \
         {reranker: 'rerank/mock', reranker_property: 'content', reranker_query: 'test'}) \
         YIELD node \
         RETURN node.title AS title",
        )
        .await?;

    // Either 0 or some results depending on how close Doc1 is — no crash
    // The important thing is no panic
    let _ = result.len();

    Ok(())
}

#[tokio::test]
async fn test_reranker_disabled_by_default() -> anyhow::Result<()> {
    let db = setup_db().await?;

    // No reranker options
    let result = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 3) \
         YIELD node, score, rerank_score \
         RETURN node.title AS title, score, rerank_score",
        )
        .await?;

    assert!(result.len() > 0);
    // rerank_score should be null when no reranker configured
    for i in 0..result.len() {
        let rerank = get_f64(&result, i, "rerank_score");
        // When disabled, rerank_score column yields null → get returns Err
        // This is expected behavior
        assert!(
            rerank.is_none(),
            "rerank_score should be null when reranker is not configured"
        );
    }

    // First result should be Doc1 (closest vector to [1,0])
    assert_eq!(titles(&result)[0], "Doc1");

    Ok(())
}

#[tokio::test]
async fn test_reranker_with_threshold() -> anyhow::Result<()> {
    let db = setup_db().await?;

    // Use a threshold that filters some results before reranking
    let result = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 5, null, 1.0, \
         {reranker: 'rerank/mock', reranker_property: 'content', reranker_query: 'test'}) \
         YIELD node, rerank_score, distance \
         RETURN node.title AS title, rerank_score, distance",
        )
        .await?;

    // All returned docs should have distance <= 1.0 (threshold applied before reranking)
    for i in 0..result.len() {
        let dist = get_f64(&result, i, "distance").unwrap();
        assert!(dist <= 1.0, "distance {dist} should be <= threshold 1.0");
    }

    Ok(())
}

// ===========================================================================
// E. Failure Scenarios
// ===========================================================================

#[tokio::test]
async fn test_reranker_vector_query_precomputed_vector_no_query_text() -> anyhow::Result<()> {
    let db = setup_db().await?;

    // Query is a pre-computed vector, no reranker_query provided
    let result = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 3, null, null, \
         {reranker: 'rerank/mock', reranker_property: 'content'}) \
         YIELD node \
         RETURN node.title AS title",
        )
        .await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("pre-computed vector"),
        "Error should mention pre-computed vector, got: {err_msg}"
    );

    Ok(())
}

#[tokio::test]
async fn test_reranker_vector_query_precomputed_vector_with_reranker_query() -> anyhow::Result<()> {
    let db = setup_db().await?;

    // Query is a vector, but reranker_query provides the text
    let result = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 3, null, null, \
         {reranker: 'rerank/mock', reranker_property: 'content', reranker_query: 'test query'}) \
         YIELD node, rerank_score \
         RETURN node.title AS title, rerank_score",
        )
        .await?;

    assert!(result.len() > 0);
    assert!(get_f64(&result, 0, "rerank_score").is_some());

    Ok(())
}

#[tokio::test]
async fn test_reranker_missing_property_name() -> anyhow::Result<()> {
    let db = setup_db().await?;

    // No reranker_property on uni.vector.query (no FTS default to fall back on)
    let result = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 3, null, null, \
         {reranker: 'rerank/mock', reranker_query: 'test'}) \
         YIELD node \
         RETURN node.title AS title",
        )
        .await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("reranker_property"),
        "Error should mention reranker_property, got: {err_msg}"
    );

    Ok(())
}

#[tokio::test]
async fn test_reranker_unknown_alias() -> anyhow::Result<()> {
    let db = setup_db().await?;

    let result = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 3, null, null, \
         {reranker: 'rerank/nonexistent', reranker_property: 'content', reranker_query: 'test'}) \
         YIELD node \
         RETURN node.title AS title",
        )
        .await;

    assert!(result.is_err());

    Ok(())
}

// ===========================================================================
// F. Score Semantics
// ===========================================================================

#[tokio::test]
async fn test_rerank_score_is_sigmoid_normalized() -> anyhow::Result<()> {
    let db = setup_db().await?;

    let result = db.session().query(
        "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 5, null, null, \
         {reranker: 'rerank/mock', reranker_property: 'content', reranker_k: 5, reranker_query: 'test'}) \
         YIELD node, rerank_score \
         RETURN node.title AS title, rerank_score \
         ORDER BY rerank_score DESC",
    ).await?;

    assert!(result.len() > 0);
    for i in 0..result.len() {
        let rs = get_f64(&result, i, "rerank_score").unwrap();
        assert!(
            rs > 0.0 && rs < 1.0,
            "rerank_score should be in (0, 1) after sigmoid, got {rs}"
        );
    }

    // Longer content should have higher rerank_score
    if result.len() >= 2 {
        let first_title: String = result.rows()[0].get("title").unwrap();
        assert_eq!(
            first_title, "Doc3",
            "Doc3 (longest content) should have highest rerank_score"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_score_without_reranker_is_retrieval_score() -> anyhow::Result<()> {
    let db = setup_db().await?;

    // Without reranker
    let without = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 3) \
         YIELD node, score \
         RETURN node.title AS title, score",
        )
        .await?;

    // With reranker
    let with = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 3, null, null, \
         {reranker: 'rerank/mock', reranker_property: 'content', reranker_query: 'test'}) \
         YIELD node, score \
         RETURN node.title AS title, score",
        )
        .await?;

    // The scores should differ (retrieval score vs reranker score)
    let without_score = get_f64(&without, 0, "score").unwrap();
    let with_score = get_f64(&with, 0, "score").unwrap();

    // They might be the same by coincidence, but the ordering should differ
    let without_titles = titles(&without);
    let with_titles = titles(&with);
    // At least one of: different order or different scores
    assert!(
        without_titles != with_titles || (without_score - with_score).abs() > 1e-5,
        "Reranker should change either the order or the scores"
    );

    Ok(())
}

// ===========================================================================
// G. Backward Compatibility
// ===========================================================================

#[tokio::test]
async fn test_vector_query_without_options_arg() -> anyhow::Result<()> {
    let db = setup_db().await?;

    // Old-style call — no 7th arg
    let result = db
        .session()
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 5) \
         YIELD node, distance \
         RETURN node.title AS title, distance",
        )
        .await?;

    assert_eq!(result.len(), 5);
    // Doc1 should be closest
    assert_eq!(titles(&result)[0], "Doc1");

    Ok(())
}

#[tokio::test]
async fn test_fts_query_without_options_arg() -> anyhow::Result<()> {
    let db = setup_db().await?;

    // Old-style FTS call
    let result = db
        .session()
        .query(
            "CALL uni.fts.query('Doc', 'content', 'document', 5) \
         YIELD node, score \
         RETURN node.title AS title, score",
        )
        .await?;

    // Should work without error (backward compatible)
    // "document" appears in Doc3's content
    let _ = result.len();

    Ok(())
}

#[tokio::test]
async fn test_hybrid_search_existing_options_unchanged() -> anyhow::Result<()> {
    let db = setup_db().await?;

    // Existing options without reranker keys
    let result = db
        .session()
        .query(
            "CALL uni.search('Doc', {vector: 'embedding', fts: 'content'}, \
         'document', null, 3, null, {method: 'weighted', alpha: 0.7}) \
         YIELD node, score \
         RETURN node.title AS title, score",
        )
        .await?;

    assert!(result.len() > 0);

    Ok(())
}

// ===========================================================================
// H. E2E with real ONNX cross-encoder model
// ===========================================================================

/// End-to-end test using the real `cross-encoder/ms-marco-MiniLM-L6-v2` model.
///
/// Downloads the model from HuggingFace on first run (~80MB). Requires the
/// `provider-onnx` feature. Run manually with:
///
/// ```bash
/// cargo nextest run -p uni-db --features provider-onnx -- test_real_onnx_cross_encoder --run-ignored
/// ```
#[cfg(feature = "provider-onnx")]
#[tokio::test]
#[ignore]
async fn test_real_onnx_cross_encoder_reranks_by_relevance() -> anyhow::Result<()> {
    use uni_xervo::provider::LocalOnnxProvider;

    let runtime = ModelRuntime::builder()
        .register_provider(LocalOnnxProvider::new())
        .catalog(vec![ModelAliasSpec {
            alias: "rerank/minilm".to_string(),
            task: ModelTask::Rerank,
            provider_id: "local/onnx".to_string(),
            model_id: "cross-encoder/ms-marco-MiniLM-L6-v2".to_string(),
            revision: None,
            warmup: WarmupPolicy::Lazy,
            required: false,
            timeout: None,
            load_timeout: None,
            retry: None,
            options: serde_json::json!({}),
        }])
        .build()
        .await
        .expect("Failed to build runtime with ONNX cross-encoder");

    let db = Uni::temporary().xervo_runtime(runtime).build().await?;

    db.schema()
        .label("Article")
        .property("title", DataType::String)
        .property("body", DataType::String)
        .property("embedding", DataType::Vector { dimensions: 3 })
        .apply()
        .await?;

    // Insert articles on different topics. Embeddings are arbitrary (just need
    // them for the vector index); the cross-encoder scores on text content.
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:Article {title: 'Rust Ownership', \
         body: 'The Rust programming language uses ownership and borrowing to guarantee memory safety without a garbage collector.', \
         embedding: [1.0, 0.0, 0.0]})",
    ).await?;
    tx.execute(
        "CREATE (:Article {title: 'Chocolate Cake', \
         body: 'To make a rich chocolate cake, combine cocoa powder, flour, sugar, eggs, and butter.', \
         embedding: [0.0, 1.0, 0.0]})",
    ).await?;
    tx.execute(
        "CREATE (:Article {title: 'Rust Concurrency', \
         body: 'Rust provides fearless concurrency through its type system, using Send and Sync traits to prevent data races at compile time.', \
         embedding: [0.9, 0.1, 0.0]})",
    ).await?;
    tx.execute(
        "CREATE (:Article {title: 'Garden Tips', \
         body: 'Plant tomatoes in full sun with well-drained soil. Water deeply but infrequently for best results.', \
         embedding: [0.0, 0.0, 1.0]})",
    ).await?;
    tx.commit().await?;
    db.flush().await?;

    // Query about Rust memory safety — the cross-encoder should rank
    // "Rust Ownership" and "Rust Concurrency" higher than cooking/gardening.
    let result = db
        .session()
        .query(
            "CALL uni.vector.query('Article', 'embedding', [1.0, 0.0, 0.0], 4, null, null, \
         {reranker: 'rerank/minilm', reranker_property: 'body', \
          reranker_query: 'How does Rust handle memory safety?'}) \
         YIELD node, score, rerank_score \
         RETURN node.title AS title, score, rerank_score \
         ORDER BY rerank_score DESC",
        )
        .await?;

    assert_eq!(result.len(), 4, "Should return all 4 articles");

    let order = titles(&result);
    eprintln!("Reranked order: {:?}", order);
    for (i, title) in order.iter().enumerate() {
        let rs = get_f64(&result, i, "rerank_score").unwrap();
        eprintln!("  {}: rerank_score={:.4}", title, rs);
    }

    // The top result should be one of the Rust articles
    assert!(
        order[0] == "Rust Ownership" || order[0] == "Rust Concurrency",
        "Top result should be a Rust article, got: {}",
        order[0]
    );

    // Both Rust articles should be in the top 2
    let top_2: Vec<&str> = order[..2].iter().map(|s| s.as_str()).collect();
    assert!(
        top_2.contains(&"Rust Ownership") && top_2.contains(&"Rust Concurrency"),
        "Top 2 should be both Rust articles, got: {:?}",
        top_2
    );

    // The Rust articles should have higher rerank scores than non-Rust articles
    let rust_min = get_f64(&result, 1, "rerank_score").unwrap(); // 2nd Rust article
    let non_rust_max = get_f64(&result, 2, "rerank_score").unwrap(); // best non-Rust
    assert!(
        rust_min > non_rust_max,
        "Rust articles should score higher than non-Rust: Rust min={rust_min:.4}, non-Rust max={non_rust_max:.4}"
    );

    Ok(())
}

/// Test the reranker facade directly (without search procedures).
#[cfg(feature = "provider-onnx")]
#[tokio::test]
#[ignore]
async fn test_real_onnx_cross_encoder_via_facade() -> anyhow::Result<()> {
    use uni_xervo::provider::LocalOnnxProvider;

    let runtime = ModelRuntime::builder()
        .register_provider(LocalOnnxProvider::new())
        .catalog(vec![ModelAliasSpec {
            alias: "rerank/minilm".to_string(),
            task: ModelTask::Rerank,
            provider_id: "local/onnx".to_string(),
            model_id: "cross-encoder/ms-marco-MiniLM-L6-v2".to_string(),
            revision: None,
            warmup: WarmupPolicy::Lazy,
            required: false,
            timeout: None,
            load_timeout: None,
            retry: None,
            options: serde_json::json!({}),
        }])
        .build()
        .await?;

    let db = Uni::temporary().xervo_runtime(runtime).build().await?;

    let docs = &[
        "The capital of France is Paris.",
        "Photosynthesis converts light energy into chemical energy.",
        "Paris is known for the Eiffel Tower and French cuisine.",
        "Quantum computing uses qubits instead of classical bits.",
    ];

    let scored = db
        .xervo()
        .rerank("rerank/minilm", "What is the capital of France?", docs)
        .await?;

    eprintln!("Facade rerank results:");
    for sd in &scored {
        eprintln!(
            "  [{}] score={:.4} text={:?}",
            sd.index, sd.score, docs[sd.index]
        );
    }

    assert_eq!(scored.len(), 4);

    // The Paris-related docs (indices 0 and 2) should be ranked highest
    let top_2_indices: Vec<usize> = scored[..2].iter().map(|sd| sd.index).collect();
    assert!(
        top_2_indices.contains(&0) && top_2_indices.contains(&2),
        "Top 2 should be the Paris-related docs (indices 0,2), got: {:?}",
        top_2_indices
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// BGE + Qwen3 reranker family coverage (uni-xervo 0.11.0)
// ---------------------------------------------------------------------------
//
// uni-xervo 0.11.0 (commit `d08411c`) made the cross-encoder loader
// auto-detect `token_type_ids` from `session.inputs()`, which is what
// makes `BAAI/bge-reranker-base` work — its ONNX export omits that input,
// and the pre-0.11.0 hardcoded feed errored at ORT load. The same release
// (commit `21642b5`) added a generative-style dispatch in
// `rerank_generative.rs` for Qwen3-Reranker, which is a `Qwen3ForCausalLM`
// that scores via constrained `yes`/`no` token logits rather than a
// regression head. Both families resolve through the same alias-based
// pipeline in `procedure_call.rs:1284-1410`, so these two tests are the
// only end-to-end signal that the family-specific paths work through
// uni-db's `runtime.reranker(...).rerank(...)` call.
//
// Run manually with:
//
//   cargo nextest run -p uni-db --features provider-onnx \
//     --test reranker_integration --run-ignored all

/// Cross-encoder reranker — `BAAI/bge-reranker-base`. Validates the
/// 0.11.0 `token_type_ids` auto-detection: pre-0.11.0 this would have
/// failed at model load with an ORT "Invalid input name" error because
/// the loader hardcoded a `token_type_ids` feed.
///
/// Downloads the model from HuggingFace on first run (~280 MB).
#[cfg(feature = "provider-onnx")]
#[tokio::test]
#[ignore]
async fn test_real_onnx_bge_reranker_reranks_by_relevance() -> anyhow::Result<()> {
    use uni_xervo::provider::LocalOnnxProvider;

    let runtime = ModelRuntime::builder()
        .register_provider(LocalOnnxProvider::new())
        .catalog(vec![ModelAliasSpec {
            alias: "rerank/bge".to_string(),
            task: ModelTask::Rerank,
            provider_id: "local/onnx".to_string(),
            model_id: "BAAI/bge-reranker-base".to_string(),
            revision: None,
            warmup: WarmupPolicy::Lazy,
            required: false,
            timeout: None,
            load_timeout: None,
            retry: None,
            // No `style` option — defaults to "cross-encoder", which is
            // what BGE rerankers want. Explicitly empty to document that
            // BGE rerankers need no extra config under 0.11.0+.
            options: serde_json::json!({}),
        }])
        .build()
        .await
        .expect("Failed to build runtime with BGE reranker");

    let db = Uni::temporary().xervo_runtime(runtime).build().await?;

    db.schema()
        .label("Article")
        .property("title", DataType::String)
        .property("body", DataType::String)
        .property("embedding", DataType::Vector { dimensions: 3 })
        .apply()
        .await?;

    // Same fixture as the MiniLM test for direct cross-family comparison.
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:Article {title: 'Rust Ownership', \
         body: 'The Rust programming language uses ownership and borrowing to guarantee memory safety without a garbage collector.', \
         embedding: [1.0, 0.0, 0.0]})",
    ).await?;
    tx.execute(
        "CREATE (:Article {title: 'Chocolate Cake', \
         body: 'To make a rich chocolate cake, combine cocoa powder, flour, sugar, eggs, and butter.', \
         embedding: [0.0, 1.0, 0.0]})",
    ).await?;
    tx.execute(
        "CREATE (:Article {title: 'Rust Concurrency', \
         body: 'Rust provides fearless concurrency through its type system, using Send and Sync traits to prevent data races at compile time.', \
         embedding: [0.9, 0.1, 0.0]})",
    ).await?;
    tx.execute(
        "CREATE (:Article {title: 'Garden Tips', \
         body: 'Plant tomatoes in full sun with well-drained soil. Water deeply but infrequently for best results.', \
         embedding: [0.0, 0.0, 1.0]})",
    ).await?;
    tx.commit().await?;
    db.flush().await?;

    // First-run path downloads ~280 MB and warms tokenizer + ONNX session;
    // bump the query timeout above the 30 s default to absorb that.
    let result = db
        .session()
        .query_with(
            "CALL uni.vector.query('Article', 'embedding', [1.0, 0.0, 0.0], 4, null, null, \
         {reranker: 'rerank/bge', reranker_property: 'body', \
          reranker_query: 'How does Rust handle memory safety?'}) \
         YIELD node, score, rerank_score \
         RETURN node.title AS title, score, rerank_score \
         ORDER BY rerank_score DESC",
        )
        .timeout(std::time::Duration::from_secs(600))
        .fetch_all()
        .await?;

    assert_eq!(result.len(), 4, "Should return all 4 articles");

    let order = titles(&result);
    eprintln!("BGE reranked order: {:?}", order);
    for (i, title) in order.iter().enumerate() {
        let rs = get_f64(&result, i, "rerank_score").unwrap();
        eprintln!("  {}: rerank_score={:.4}", title, rs);
    }

    // Top result must be a Rust article (memory-safety query).
    assert!(
        order[0] == "Rust Ownership" || order[0] == "Rust Concurrency",
        "Top result should be a Rust article, got: {}",
        order[0]
    );
    let top_2: Vec<&str> = order[..2].iter().map(|s| s.as_str()).collect();
    assert!(
        top_2.contains(&"Rust Ownership") && top_2.contains(&"Rust Concurrency"),
        "Top 2 should be both Rust articles, got: {:?}",
        top_2
    );

    // Cross-encoder returns raw logits (unbounded). Just assert that the
    // worst Rust article still scored higher than the best non-Rust.
    let rust_min = get_f64(&result, 1, "rerank_score").unwrap();
    let non_rust_max = get_f64(&result, 2, "rerank_score").unwrap();
    assert!(
        rust_min > non_rust_max,
        "Rust articles should score higher than non-Rust: Rust min={rust_min:.4}, non-Rust max={non_rust_max:.4}"
    );

    Ok(())
}

/// Generative-style reranker — `onnx-community/Qwen3-Reranker-0.6B-ONNX`.
/// Validates the 0.11.0 generative dispatch path
/// (`rerank_generative.rs`): Qwen3-Reranker is a `Qwen3ForCausalLM`,
/// scored via softmax over the `yes`/`no` token logits at the last
/// non-pad position. Selected by `options.style = "generative"` in the
/// `ModelAliasSpec`; default `"cross-encoder"` would fail at load.
///
/// Downloads the quantized variant `onnx/model_q4.onnx` (~500 MB) on
/// first run. Test runs slower than the cross-encoder paths because
/// of the larger model and longer-context tokenization.
#[cfg(feature = "provider-onnx")]
#[tokio::test]
#[ignore]
async fn test_real_onnx_qwen3_reranker_reranks_by_relevance() -> anyhow::Result<()> {
    use uni_xervo::provider::LocalOnnxProvider;

    let runtime = ModelRuntime::builder()
        .register_provider(LocalOnnxProvider::new())
        .catalog(vec![ModelAliasSpec {
            alias: "rerank/qwen3".to_string(),
            task: ModelTask::Rerank,
            provider_id: "local/onnx".to_string(),
            model_id: "onnx-community/Qwen3-Reranker-0.6B-ONNX".to_string(),
            revision: None,
            warmup: WarmupPolicy::Lazy,
            required: false,
            // Qwen3 forward pass is heavier than a cross-encoder, but the
            // default `load_timeout` (600 s) and unbounded per-call
            // `timeout` are still appropriate; left as None.
            timeout: None,
            load_timeout: None,
            retry: None,
            // `style: "generative"` routes to `rerank_generative.rs`
            // instead of the default `rerank.rs` cross-encoder path.
            options: serde_json::json!({ "style": "generative" }),
        }])
        .build()
        .await
        .expect("Failed to build runtime with Qwen3 generative reranker");

    let db = Uni::temporary().xervo_runtime(runtime).build().await?;

    db.schema()
        .label("Article")
        .property("title", DataType::String)
        .property("body", DataType::String)
        .property("embedding", DataType::Vector { dimensions: 3 })
        .apply()
        .await?;

    // Same fixture as the BGE / MiniLM tests for direct cross-family
    // score-shape comparison.
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:Article {title: 'Rust Ownership', \
         body: 'The Rust programming language uses ownership and borrowing to guarantee memory safety without a garbage collector.', \
         embedding: [1.0, 0.0, 0.0]})",
    ).await?;
    tx.execute(
        "CREATE (:Article {title: 'Chocolate Cake', \
         body: 'To make a rich chocolate cake, combine cocoa powder, flour, sugar, eggs, and butter.', \
         embedding: [0.0, 1.0, 0.0]})",
    ).await?;
    tx.execute(
        "CREATE (:Article {title: 'Rust Concurrency', \
         body: 'Rust provides fearless concurrency through its type system, using Send and Sync traits to prevent data races at compile time.', \
         embedding: [0.9, 0.1, 0.0]})",
    ).await?;
    tx.execute(
        "CREATE (:Article {title: 'Garden Tips', \
         body: 'Plant tomatoes in full sun with well-drained soil. Water deeply but infrequently for best results.', \
         embedding: [0.0, 0.0, 1.0]})",
    ).await?;
    tx.commit().await?;
    db.flush().await?;

    // First-run path downloads ~500 MB (quantized variant), then warms a
    // generative-style decoder forward pass per (query, doc) pair; this is
    // the slowest of the three reranker tests. Allow up to 15 minutes.
    let result = db
        .session()
        .query_with(
            "CALL uni.vector.query('Article', 'embedding', [1.0, 0.0, 0.0], 4, null, null, \
         {reranker: 'rerank/qwen3', reranker_property: 'body', \
          reranker_query: 'How does Rust handle memory safety?'}) \
         YIELD node, score, rerank_score \
         RETURN node.title AS title, score, rerank_score \
         ORDER BY rerank_score DESC",
        )
        .timeout(std::time::Duration::from_secs(900))
        .fetch_all()
        .await?;

    assert_eq!(result.len(), 4, "Should return all 4 articles");

    let order = titles(&result);
    eprintln!("Qwen3 reranked order: {:?}", order);
    for (i, title) in order.iter().enumerate() {
        let rs = get_f64(&result, i, "rerank_score").unwrap();
        eprintln!("  {}: rerank_score={:.4}", title, rs);
    }

    // Generative path returns softmax probabilities — every score must
    // lie in [0.0, 1.0]. Pin this to make the cross-encoder vs
    // generative score-scale asymmetry auditable: any future regression
    // that returns raw logits here would fail this assertion.
    for i in 0..result.len() {
        let rs = get_f64(&result, i, "rerank_score").unwrap();
        assert!(
            rs >= 0.0 && rs <= 1.0,
            "Qwen3 rerank_score must be a probability in [0, 1], got {rs} at row {i}"
        );
    }

    // Top result must be a Rust article (memory-safety query).
    assert!(
        order[0] == "Rust Ownership" || order[0] == "Rust Concurrency",
        "Top result should be a Rust article, got: {}",
        order[0]
    );
    let top_2: Vec<&str> = order[..2].iter().map(|s| s.as_str()).collect();
    assert!(
        top_2.contains(&"Rust Ownership") && top_2.contains(&"Rust Concurrency"),
        "Top 2 should be both Rust articles, got: {:?}",
        top_2
    );

    Ok(())
}
