// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Auto-embed for multi-vector (late-interaction / ColBERT) properties (issue #104).
//!
//! A `List<Vector>` property with an `EmbeddingCfg` is auto-embedded at write time via
//! uni-xervo's multi-vector model (`multi_vector_embedder`), producing per-token vectors —
//! mirroring single-vector auto-embed. Uses a deterministic mock model (one token vector per
//! whitespace word, the i-th token = `[i+1; DIM]`) so the output is exactly assertable,
//! exercising both the eager (default) and deferred (`defer_embeddings`) flush paths.

use std::sync::Arc;

use async_trait::async_trait;
use uni_common::UniConfig;
use uni_db::{
    DataType, EmbeddingCfg, IndexType, ModelAliasSpec, ModelTask, Uni, VectorAlgo, VectorIndexCfg,
    VectorMetric, WarmupPolicy,
};
use uni_xervo::runtime::ModelRuntime;
use uni_xervo::traits::multivector::{MultiVectorEmbedResult, MultiVectorEmbeddingModel};
use uni_xervo::traits::{
    LoadedModelHandle, ModelInfo, ModelProvider, ProviderCapabilities, ProviderHealth,
};

const DIM: usize = 4;

/// Deterministic multi-vector model: one token vector per whitespace word; token `i` is
/// `[i+1; DIM]`. So `"a b c"` -> `[[1,1,1,1],[2,2,2,2],[3,3,3,3]]`.
struct WordCountMultiVector;

impl ModelInfo for WordCountMultiVector {
    fn model_id(&self) -> &str {
        "mock-multivector"
    }
}

#[async_trait]
impl MultiVectorEmbeddingModel for WordCountMultiVector {
    async fn embed(&self, texts: &[&str]) -> uni_xervo::error::Result<MultiVectorEmbedResult> {
        let vectors = texts
            .iter()
            .map(|t| {
                let n = t.split_whitespace().count().max(1);
                (0..n)
                    .map(|i| vec![(i + 1) as f32; DIM])
                    .collect::<Vec<_>>()
            })
            .collect();
        Ok(MultiVectorEmbedResult {
            vectors,
            usage: None,
        })
    }

    fn dimensions(&self) -> u32 {
        DIM as u32
    }
}

struct MockMultiVectorProvider;

#[async_trait]
impl ModelProvider for MockMultiVectorProvider {
    fn provider_id(&self) -> &'static str {
        "mock/multivector"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            supported_tasks: vec![ModelTask::EmbedMultiVector],
        }
    }

    async fn load(&self, _spec: &ModelAliasSpec) -> uni_xervo::error::Result<LoadedModelHandle> {
        let handle: Arc<dyn MultiVectorEmbeddingModel> = Arc::new(WordCountMultiVector);
        Ok(Arc::new(handle) as LoadedModelHandle)
    }

    async fn health(&self) -> ProviderHealth {
        ProviderHealth::Healthy
    }
}

fn mv_spec() -> ModelAliasSpec {
    ModelAliasSpec {
        alias: "mv/mock".to_string(),
        task: ModelTask::EmbedMultiVector,
        provider_id: "mock/multivector".to_string(),
        model_id: "mock-multivector".to_string(),
        revision: None,
        warmup: WarmupPolicy::Lazy,
        required: false,
        timeout: None,
        load_timeout: None,
        retry: None,
        options: serde_json::json!({}),
    }
}

async fn mock_runtime() -> Arc<ModelRuntime> {
    ModelRuntime::builder()
        .register_provider(MockMultiVectorProvider)
        .catalog(vec![mv_spec()])
        .build()
        .await
        .expect("build mock multi-vector runtime")
}

/// Declare `Doc(content STRING, tokens List<Vector{DIM}>)` with a multi-vector index whose
/// `EmbeddingCfg` auto-embeds `content` -> `tokens` via the mock model.
async fn define_autoembed_schema(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("content", DataType::String)
        // Nullable: in deferred mode the row is written before the embedding is materialized
        // at flush, so the target must tolerate a transient null.
        .property_nullable(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
        )
        .index(
            "tokens",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::Cosine,
                embedding: Some(EmbeddingCfg {
                    alias: "mv/mock".to_string(),
                    source_properties: vec!["content".to_string()],
                    batch_size: 16,
                    document_prefix: None,
                    query_prefix: None,
                }),
            }),
        )
        .apply()
        .await?;
    Ok(())
}

async fn read_tokens(db: &Uni, title_filter: &str) -> anyhow::Result<Vec<Vec<f64>>> {
    let res = db
        .session()
        .query(&format!(
            "MATCH (d:Doc {{content: '{title_filter}'}}) RETURN d.tokens AS t"
        ))
        .await?;
    Ok(serde_json::from_value(
        res.rows()[0].value("t").unwrap().clone().into(),
    )?)
}

fn expect_word_tokens(n: usize) -> Vec<Vec<f64>> {
    (0..n).map(|i| vec![(i + 1) as f64; DIM]).collect()
}

/// Eager path (default config): the source text is auto-embedded into per-token vectors on
/// write, with no `tokens` provided by the user.
#[tokio::test]
async fn multivec_autoembed_eager() -> anyhow::Result<()> {
    let db = Uni::temporary()
        .xervo_runtime(mock_runtime().await)
        .build()
        .await?;
    define_autoembed_schema(&db).await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {content: 'hello world foo'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let toks = read_tokens(&db, "hello world foo").await?;
    assert_eq!(
        toks,
        expect_word_tokens(3),
        "3 words must auto-embed to 3 per-token vectors: {toks:?}"
    );
    Ok(())
}

/// Deferred path (`defer_embeddings`): embedding is batched at flush via the multi-vector
/// model; per-token vectors land in the column.
#[tokio::test]
async fn multivec_autoembed_deferred() -> anyhow::Result<()> {
    let cfg = UniConfig {
        defer_embeddings: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory()
        .config(cfg)
        .xervo_runtime(mock_runtime().await)
        .build()
        .await?;
    define_autoembed_schema(&db).await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {content: 'one two'})").await?;
    tx.execute("CREATE (:Doc {content: 'a b c d'})").await?;
    tx.commit().await?;
    db.flush().await?;

    assert_eq!(read_tokens(&db, "one two").await?, expect_word_tokens(2));
    assert_eq!(read_tokens(&db, "a b c d").await?, expect_word_tokens(4));
    Ok(())
}

/// A user-provided `tokens` value is NOT overwritten by auto-embed (explicit wins).
#[tokio::test]
async fn multivec_autoembed_respects_user_supplied_tokens() -> anyhow::Result<()> {
    let db = Uni::temporary()
        .xervo_runtime(mock_runtime().await)
        .build()
        .await?;
    define_autoembed_schema(&db).await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {content: 'hello world foo', tokens: [[9.0,9.0,9.0,9.0]]})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let toks = read_tokens(&db, "hello world foo").await?;
    assert_eq!(
        toks,
        vec![vec![9.0; DIM]],
        "user-supplied tokens must be preserved (no auto-embed overwrite): {toks:?}"
    );
    Ok(())
}
