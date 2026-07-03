// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Per-field model selection: two DIFFERENT embedding models on two DIFFERENT fields of one label.
//!
//! The parity suite (`autoembed_parity.rs`) covers one model per task-type. This closes the
//! complementary gap: a single label with two dense vector columns, each wired to its own
//! embedding alias + its own source property, must embed independently — each column gets the
//! output of its own model, and each model's provider is invoked. Distinguishable mock outputs
//! (`[word_count * mult; DIM]`) make the routing observable.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use uni_common::UniConfig;
use uni_db::{
    DataType, EmbeddingCfg, IndexType, ModelAliasSpec, ModelTask, Uni, VectorAlgo, VectorIndexCfg,
    VectorMetric, WarmupPolicy,
};
use uni_xervo::runtime::ModelRuntime;
use uni_xervo::traits::{
    EmbedResult, EmbeddingModel, LoadedModelHandle, ModelInfo, ModelProvider, ProviderCapabilities,
    ProviderHealth,
};

const DIM: usize = 4;

/// Dense mock that scales each text's whitespace-word count by `mult`, so two instances with
/// different `mult` produce distinguishable vectors and their call counters can be asserted.
struct DenseMock {
    calls: Arc<AtomicUsize>,
    mult: f32,
}
impl ModelInfo for DenseMock {
    fn model_id(&self) -> &str {
        "mock-dense"
    }
}
#[async_trait]
impl EmbeddingModel for DenseMock {
    async fn embed(&self, texts: &[&str]) -> uni_xervo::error::Result<EmbedResult> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(EmbedResult {
            vectors: texts
                .iter()
                .map(|t| vec![t.split_whitespace().count() as f32 * self.mult; DIM])
                .collect(),
            usage: None,
        })
    }
    fn dimensions(&self) -> u32 {
        DIM as u32
    }
}

/// Single-head dense provider parameterized by id + the per-model `mult`.
macro_rules! dense_provider {
    ($name:ident, $id:literal, $mult:expr) => {
        struct $name {
            calls: Arc<AtomicUsize>,
        }
        #[async_trait]
        impl ModelProvider for $name {
            fn provider_id(&self) -> &'static str {
                $id
            }
            fn capabilities(&self) -> ProviderCapabilities {
                ProviderCapabilities {
                    supported_tasks: vec![ModelTask::Embed],
                }
            }
            async fn load(
                &self,
                _spec: &ModelAliasSpec,
            ) -> uni_xervo::error::Result<LoadedModelHandle> {
                let h: Arc<dyn EmbeddingModel> = Arc::new(DenseMock {
                    calls: self.calls.clone(),
                    mult: $mult,
                });
                Ok(Arc::new(h) as LoadedModelHandle)
            }
            async fn health(&self) -> ProviderHealth {
                ProviderHealth::Healthy
            }
        }
    };
}

dense_provider!(DenseProviderA, "mock/dense-a", 1.0);
dense_provider!(DenseProviderB, "mock/dense-b", 10.0);

fn spec(alias: &str, provider: &str) -> ModelAliasSpec {
    ModelAliasSpec {
        alias: alias.to_string(),
        task: ModelTask::Embed,
        provider_id: provider.to_string(),
        model_id: "mock-dense".to_string(),
        revision: None,
        warmup: WarmupPolicy::Lazy,
        required: false,
        timeout: None,
        load_timeout: None,
        retry: None,
        options: serde_json::json!({}),
    }
}

fn emb_cfg(alias: &str, source: &str) -> EmbeddingCfg {
    EmbeddingCfg {
        alias: alias.to_string(),
        source_properties: vec![source.to_string()],
        batch_size: 16,
        document_prefix: None,
        query_prefix: None,
    }
}

fn dense_index(cfg: EmbeddingCfg) -> IndexType {
    IndexType::Vector(VectorIndexCfg {
        algorithm: VectorAlgo::Flat,
        metric: VectorMetric::Cosine,
        embedding: Some(cfg),
    })
}

/// Read a dense vector column back as `Vec<f64>` (empty if null/missing).
async fn read_vec(db: &Uni, title: &str, col: &str) -> anyhow::Result<Vec<f64>> {
    let res = db
        .session()
        .query(&format!(
            "MATCH (d:Doc {{title: '{title}'}}) RETURN d.{col} AS v"
        ))
        .await?;
    let v = res.rows()[0].value("v").unwrap().clone();
    Ok(serde_json::from_value(v.into()).unwrap_or_default())
}

/// Two dense fields on one `Doc`, each routed to a different alias + source property, embed
/// independently: `title_emb` from model A over `title`, `body_emb` from model B over `body`.
#[tokio::test]
async fn two_models_two_fields_embed_independently() -> anyhow::Result<()> {
    let calls_a = Arc::new(AtomicUsize::new(0));
    let calls_b = Arc::new(AtomicUsize::new(0));

    let runtime = ModelRuntime::builder()
        .register_provider(DenseProviderA {
            calls: calls_a.clone(),
        })
        .register_provider(DenseProviderB {
            calls: calls_b.clone(),
        })
        .catalog(vec![
            spec("title/model", "mock/dense-a"),
            spec("body/model", "mock/dense-b"),
        ])
        .build()
        .await
        .unwrap();

    let cfg = UniConfig {
        defer_embeddings: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory()
        .config(cfg)
        .xervo_runtime(runtime)
        .build()
        .await?;

    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("body", DataType::String)
        .property_nullable("title_emb", DataType::Vector { dimensions: DIM })
        .property_nullable("body_emb", DataType::Vector { dimensions: DIM })
        .index("title_emb", dense_index(emb_cfg("title/model", "title")))
        .index("body_emb", dense_index(emb_cfg("body/model", "body")))
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    // title = 2 words, body = 3 words.
    tx.execute("CREATE (:Doc {title: 'alpha beta', body: 'one two three'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // model A (mult 1.0) over the 2-word title → [2.0; DIM].
    let title_emb = read_vec(&db, "alpha beta", "title_emb").await?;
    assert_eq!(
        title_emb,
        vec![2.0; DIM],
        "title_emb should come from model A over `title`"
    );
    // model B (mult 10.0) over the 3-word body → [30.0; DIM].
    let body_emb = read_vec(&db, "alpha beta", "body_emb").await?;
    assert_eq!(
        body_emb,
        vec![30.0; DIM],
        "body_emb should come from model B over `body`"
    );

    // Both distinct models were actually invoked — neither field borrowed the other's model.
    assert!(
        calls_a.load(Ordering::SeqCst) > 0,
        "model A (title) was never invoked"
    );
    assert!(
        calls_b.load(Ordering::SeqCst) > 0,
        "model B (body) was never invoked"
    );
    Ok(())
}
