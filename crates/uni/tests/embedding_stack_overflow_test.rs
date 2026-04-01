// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Tests for embedding service implementations.
// Candle-based embeddings are the default (no stack overflow issues).
// FastEmbed tests require the 'fastembed' feature flag.

use anyhow::Result;
use uni_db::Uni;

/// Test that Candle embedding works for vector search.
/// Candle is the default embedding provider.
#[tokio::test]
#[ignore] // Requires model download from HuggingFace Hub
async fn test_candle_embedding_basic() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // 1. Create label with content property
    // 2. Create vector index with Candle auto-embedding
    // all-MiniLM-L6-v2 produces 384-dimensional embeddings
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Document (content STRING)").await?;
    tx.execute(
        r#"
        CREATE VECTOR INDEX doc_embed_idx
        FOR (d:Document) ON (d.embedding)
        OPTIONS {
            metric: 'cosine',
            embedding: {
                provider: 'Candle',
                model: 'all-MiniLM-L6-v2',
                source: ['content']
            }
        }
    "#,
    )
    .await?;
    // 3. Insert a document - this triggers auto-embedding
    tx.execute(r#"CREATE (:Document {content: "Test content for embedding generation."})"#)
        .await?;
    tx.commit().await?;

    // 4. Flush to persist the data
    db.flush().await?;

    // 5. Verify the embedding was generated
    let result = db
        .session()
        .query("MATCH (d:Document) RETURN count(d) AS cnt")
        .await?;
    let count: i64 = result.rows()[0].get("cnt")?;
    assert_eq!(count, 1, "Expected 1 document");

    // Verify embedding was stored
    let result = db
        .session()
        .query("MATCH (d:Document) WHERE d.embedding IS NOT NULL RETURN count(d) AS cnt")
        .await?;
    let emb_count: i64 = result.rows()[0].get("cnt")?;
    assert_eq!(emb_count, 1, "Document should have embedding");

    Ok(())
}

/// Test multiple Candle embeddings to ensure stability.
#[tokio::test]
#[ignore] // Requires model download from HuggingFace Hub
async fn test_candle_multiple_embeddings() -> Result<()> {
    let db = Uni::temporary().build().await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Article (title STRING, body STRING)")
        .await?;
    tx.execute(
        r#"
        CREATE VECTOR INDEX article_embed_idx
        FOR (a:Article) ON (a.embedding)
        OPTIONS {
            metric: 'cosine',
            embedding: {
                provider: 'Candle',
                model: 'all-MiniLM-L6-v2',
                source: ['title', 'body']
            }
        }
    "#,
    )
    .await?;
    tx.commit().await?;

    // Insert multiple documents
    for i in 1..=5 {
        let tx = db.session().tx().await?;
        tx.execute(&format!(
            r#"CREATE (:Article {{title: "Article {}", body: "This is the body of article number {}."}})"#,
            i, i
        ))
        .await?;
        tx.commit().await?;
    }

    db.flush().await?;

    // Verify all documents have embeddings
    let result = db
        .session()
        .query("MATCH (a:Article) RETURN count(a) AS cnt")
        .await?;
    let count: i64 = result.rows()[0].get("cnt")?;
    assert_eq!(count, 5, "Expected 5 articles");

    // Verify embeddings were generated for all
    let result = db
        .session()
        .query("MATCH (a:Article) WHERE a.embedding IS NOT NULL RETURN count(a) AS cnt")
        .await?;
    let emb_count: i64 = result.rows()[0].get("cnt")?;
    assert_eq!(emb_count, 5, "All 5 articles should have embeddings");

    Ok(())
}

// MistralRS tests (only compiled when mistralrs feature is enabled)
#[cfg(feature = "provider-mistralrs")]
mod mistralrs_tests {
    use super::*;
    use serde_json::json;
    use uni_common::core::schema::DataType;
    use uni_db::api::schema::{EmbeddingCfg, IndexType, VectorAlgo, VectorIndexCfg, VectorMetric};
    use uni_xervo::api::{ModelAliasSpec, ModelTask, WarmupPolicy};

    /// Build a [`ModelAliasSpec`] for `google/embeddinggemma-300m` via MistralRS.
    ///
    /// `google/embeddinggemma-300m` uses the `EmbeddingGemma` architecture supported
    /// by MistralRS 0.7 and produces 768-dimensional embeddings.
    fn gemma_embed_alias(alias: &str) -> ModelAliasSpec {
        ModelAliasSpec {
            alias: alias.to_string(),
            task: ModelTask::Embed,
            provider_id: "local/mistralrs".to_string(),
            // EmbeddingGemma architecture; 768-dimensional output.
            model_id: "google/embeddinggemma-300m".to_string(),
            revision: None,
            warmup: WarmupPolicy::Lazy,
            required: false,
            timeout: None,
            load_timeout: None,
            retry: None,
            options: json!({}),
        }
    }

    /// Verify that MistralRS EmbeddingGemma auto-embeds text on node creation.
    ///
    /// Inserts a node without an embedding property and asserts that the
    /// auto-embedding pipeline fills it in before the write completes.
    #[tokio::test]
    #[ignore] // Requires model download from HuggingFace Hub
    async fn test_mistralrs_embeddinggemma_auto_embed() -> Result<()> {
        let db = Uni::temporary()
            .xervo_catalog(vec![gemma_embed_alias("embed/default")])
            .build()
            .await?;

        // google/embeddinggemma-300m emits 768-dimensional vectors.
        // The schema dimension must match to avoid a flush-time mismatch.
        db.schema()
            .label("Document")
            .property("content", DataType::String)
            .property("embedding", DataType::Vector { dimensions: 768 })
            .index(
                "embedding",
                IndexType::Vector(VectorIndexCfg {
                    algorithm: VectorAlgo::Flat,
                    metric: VectorMetric::Cosine,
                    embedding: Some(EmbeddingCfg {
                        alias: "embed/default".to_string(),
                        source_properties: vec!["content".to_string()],
                        batch_size: 32,
                    }),
                }),
            )
            .apply()
            .await?;

        // Insert without providing an embedding — auto-embedding fills it in.
        let tx = db.session().tx().await?;
        tx.execute(
            r#"CREATE (:Document {content: "MistralRS EmbeddingGemma produces dense vectors."})"#,
        )
        .await?;
        tx.commit().await?;

        db.flush().await?;

        // Embedding should have been generated and persisted.
        let result = db
            .session()
            .query("MATCH (d:Document) WHERE d.embedding IS NOT NULL RETURN count(d) AS cnt")
            .await?;
        let emb_count: i64 = result.rows()[0].get("cnt")?;
        assert_eq!(emb_count, 1, "Document should have a generated embedding");

        Ok(())
    }

    /// Verify auto-embeddings from MistralRS EmbeddingGemma are searchable via `~=`.
    ///
    /// Inserts several articles (each auto-embedded on write) then issues a
    /// `~=` vector similarity query and checks that a nearest-neighbour is returned.
    #[tokio::test]
    #[ignore] // Requires model download from HuggingFace Hub
    async fn test_mistralrs_embeddinggemma_vector_search() -> Result<()> {
        let db = Uni::temporary()
            .xervo_catalog(vec![gemma_embed_alias("embed/default")])
            .build()
            .await?;

        db.schema()
            .label("Article")
            .property("title", DataType::String)
            .property("body", DataType::String)
            .property("embedding", DataType::Vector { dimensions: 768 })
            .index(
                "embedding",
                IndexType::Vector(VectorIndexCfg {
                    algorithm: VectorAlgo::Flat,
                    metric: VectorMetric::Cosine,
                    embedding: Some(EmbeddingCfg {
                        alias: "embed/default".to_string(),
                        source_properties: vec!["title".to_string(), "body".to_string()],
                        batch_size: 32,
                    }),
                }),
            )
            .apply()
            .await?;

        // Insert multiple articles — each triggers auto-embedding on write.
        for i in 1..=3_u32 {
            let tx = db.session().tx().await?;
            tx.execute(&format!(
                r#"CREATE (:Article {{title: "Article {i}", body: "Body text for article number {i}."}})"#,
            ))
            .await?;
            tx.commit().await?;
        }

        db.flush().await?;

        // All three articles should carry auto-generated embeddings.
        let result = db
            .session()
            .query("MATCH (a:Article) WHERE a.embedding IS NOT NULL RETURN count(a) AS cnt")
            .await?;
        let emb_count: i64 = result.rows()[0].get("cnt")?;
        assert_eq!(emb_count, 3, "All 3 articles should have embeddings");

        // A similarity query against a non-zero probe vector should return one result.
        // Using 0.01 rather than 0.0 to avoid division-by-zero in cosine distance.
        let probe: Vec<f32> = vec![0.01; 768];
        let results = db
            .session()
            .query_with("MATCH (a:Article) WHERE a.embedding ~= $q RETURN a.title LIMIT 1")
            .param("q", probe)
            .fetch_all()
            .await?;
        assert_eq!(results.len(), 1, "Vector search should return 1 result");

        Ok(())
    }
}

// FastEmbed tests (only compiled when fastembed feature is enabled)
#[cfg(feature = "provider-fastembed")]
mod fastembed_tests {
    use super::*;

    /// Test that fastembed embedding works without stack overflow.
    /// This test triggers auto-embedding via CREATE with a vector index
    /// that has embedding_config set. Without the fix (explicit 8MB stack),
    /// this would cause a stack overflow on the Tokio blocking thread pool.
    #[tokio::test]
    async fn test_fastembed_no_stack_overflow() -> Result<()> {
        let db = Uni::temporary().build().await?;

        // 1. Create label with content property
        // 2. Create vector index with fastembed auto-embedding
        // BGESmallENV15 produces 384-dimensional embeddings
        // 3. Insert a document - this triggers auto-embedding
        // Without the stack overflow fix, this would crash
        let tx = db.session().tx().await?;
        tx.execute("CREATE LABEL Document (content STRING)").await?;
        tx.execute(
            r#"
            CREATE VECTOR INDEX doc_embed_idx
            FOR (d:Document) ON (d.embedding)
            OPTIONS {
                metric: 'cosine',
                embedding: {
                    provider: 'FastEmbed',
                    model: 'BGESmallENV15',
                    source: ['content']
                }
            }
        "#,
        )
        .await?;
        tx.execute(r#"CREATE (:Document {content: "Test content for embedding generation."})"#)
            .await?;
        tx.commit().await?;

        // 4. Flush to persist the data
        db.flush().await?;

        // 5. Verify the embedding was generated (reaching this point means no stack overflow)
        let result = db
            .session()
            .query("MATCH (d:Document) RETURN count(d) AS cnt")
            .await?;
        let count: i64 = result.rows()[0].get("cnt")?;
        assert_eq!(count, 1, "Expected 1 document");

        // Verify embedding was stored
        let result = db
            .session()
            .query("MATCH (d:Document) WHERE d.embedding IS NOT NULL RETURN count(d) AS cnt")
            .await?;
        let emb_count: i64 = result.rows()[0].get("cnt")?;
        assert_eq!(emb_count, 1, "Document should have embedding");

        Ok(())
    }

    /// Test multiple embeddings to ensure thread spawning is stable.
    #[tokio::test]
    async fn test_fastembed_multiple_embeddings() -> Result<()> {
        let db = Uni::temporary().build().await?;

        let tx = db.session().tx().await?;
        tx.execute("CREATE LABEL Article (title STRING, body STRING)")
            .await?;
        tx.execute(
            r#"
            CREATE VECTOR INDEX article_embed_idx
            FOR (a:Article) ON (a.embedding)
            OPTIONS {
                metric: 'cosine',
                embedding: {
                    provider: 'FastEmbed',
                    model: 'AllMiniLML6V2',
                    source: ['title', 'body']
                }
            }
        "#,
        )
        .await?;
        tx.commit().await?;

        // Insert multiple documents
        for i in 1..=5 {
            let tx = db.session().tx().await?;
            tx.execute(&format!(
                r#"CREATE (:Article {{title: "Article {}", body: "This is the body of article number {}."}})"#,
                i, i
            ))
            .await?;
            tx.commit().await?;
        }

        db.flush().await?;

        // Verify all documents have embeddings
        let result = db
            .session()
            .query("MATCH (a:Article) RETURN count(a) AS cnt")
            .await?;
        let count: i64 = result.rows()[0].get("cnt")?;
        assert_eq!(count, 5, "Expected 5 articles");

        // Verify embeddings were generated for all
        let result = db
            .session()
            .query("MATCH (a:Article) WHERE a.embedding IS NOT NULL RETURN count(a) AS cnt")
            .await?;
        let emb_count: i64 = result.rows()[0].get("cnt")?;
        assert_eq!(emb_count, 5, "All 5 articles should have embeddings");

        Ok(())
    }
}
