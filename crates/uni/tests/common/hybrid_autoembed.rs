// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Single-pass hybrid auto-embed: one multi-functional model (e.g. BGE-M3) fills BOTH a dense
//! `Vector` column and a multi-vector `List<Vector>` column from ONE forward pass.
//!
//! Opt-in is type-inferred: a dense `Vector` column and a multi-vector `List<Vector>` column
//! that share the SAME embedding `alias` + `source` are auto-detected as a hybrid group (a
//! single alias has a single task, so that mix can only be served by a hybrid model). The
//! mock `HybridEmbeddingModel` counts its `embed` calls so the tests can PROVE a single
//! inference feeds both columns.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use uni_common::UniConfig;
use uni_db::{
    DataType, EmbeddingCfg, IndexType, ModelAliasSpec, ModelTask, Uni, VectorAlgo, VectorIndexCfg,
    VectorMetric, WarmupPolicy,
};
use uni_xervo::runtime::ModelRuntime;
use uni_xervo::traits::hybrid::{HeadSet, HybridEmbedResult, HybridEmbeddingModel};
use uni_xervo::traits::{
    EmbedResult, EmbeddingModel, LoadedModelHandle, ModelInfo, ModelProvider, ProviderCapabilities,
    ProviderHealth,
};

const DIM: usize = 4;

// --- Mock hybrid model: dense = [word_count; DIM]; multi = one [i+1; DIM] token per word. ---
struct CountingHybrid {
    calls: Arc<AtomicUsize>,
}

impl ModelInfo for CountingHybrid {
    fn model_id(&self) -> &str {
        "mock-hybrid"
    }
}

#[async_trait]
impl HybridEmbeddingModel for CountingHybrid {
    async fn embed(
        &self,
        texts: &[&str],
        heads: HeadSet,
    ) -> uni_xervo::error::Result<HybridEmbedResult> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        // `HybridEmbedResult` is #[non_exhaustive]: build via Default + field set.
        let mut res = HybridEmbedResult::default();
        if heads.contains(HeadSet::DENSE) {
            res.dense = Some(
                texts
                    .iter()
                    .map(|t| vec![t.split_whitespace().count() as f32; DIM])
                    .collect(),
            );
        }
        if heads.contains(HeadSet::MULTI_VECTOR) {
            res.multi_vector = Some(
                texts
                    .iter()
                    .map(|t| {
                        let n = t.split_whitespace().count().max(1);
                        (0..n)
                            .map(|i| vec![(i + 1) as f32; DIM])
                            .collect::<Vec<_>>()
                    })
                    .collect(),
            );
        }
        Ok(res)
    }

    fn available_heads(&self) -> HeadSet {
        HeadSet::DENSE | HeadSet::MULTI_VECTOR
    }
}

struct HybridProvider {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl ModelProvider for HybridProvider {
    fn provider_id(&self) -> &'static str {
        "mock/hybrid"
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            supported_tasks: vec![ModelTask::EmbedHybrid],
        }
    }
    async fn load(&self, _spec: &ModelAliasSpec) -> uni_xervo::error::Result<LoadedModelHandle> {
        let handle: Arc<dyn HybridEmbeddingModel> = Arc::new(CountingHybrid {
            calls: self.calls.clone(),
        });
        Ok(Arc::new(handle) as LoadedModelHandle)
    }
    async fn health(&self) -> ProviderHealth {
        ProviderHealth::Healthy
    }
}

// --- Mock dense-only model + provider (task Embed) for the negative test. ---
struct DenseOnly;
impl ModelInfo for DenseOnly {
    fn model_id(&self) -> &str {
        "mock-dense"
    }
}
#[async_trait]
impl EmbeddingModel for DenseOnly {
    async fn embed(&self, texts: &[&str]) -> uni_xervo::error::Result<EmbedResult> {
        Ok(EmbedResult {
            vectors: texts.iter().map(|_| vec![0.0; DIM]).collect(),
            usage: None,
        })
    }
    fn dimensions(&self) -> u32 {
        DIM as u32
    }
}
struct DenseProvider;
#[async_trait]
impl ModelProvider for DenseProvider {
    fn provider_id(&self) -> &'static str {
        "mock/dense"
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            supported_tasks: vec![ModelTask::Embed],
        }
    }
    async fn load(&self, _spec: &ModelAliasSpec) -> uni_xervo::error::Result<LoadedModelHandle> {
        let handle: Arc<dyn EmbeddingModel> = Arc::new(DenseOnly);
        Ok(Arc::new(handle) as LoadedModelHandle)
    }
    async fn health(&self) -> ProviderHealth {
        ProviderHealth::Healthy
    }
}

fn spec(alias: &str, task: ModelTask, provider_id: &str, model_id: &str) -> ModelAliasSpec {
    ModelAliasSpec {
        alias: alias.to_string(),
        task,
        provider_id: provider_id.to_string(),
        model_id: model_id.to_string(),
        revision: None,
        warmup: WarmupPolicy::Lazy,
        required: false,
        timeout: None,
        load_timeout: None,
        retry: None,
        options: serde_json::json!({}),
    }
}

async fn hybrid_runtime(calls: Arc<AtomicUsize>) -> Arc<ModelRuntime> {
    ModelRuntime::builder()
        .register_provider(HybridProvider { calls })
        .catalog(vec![spec(
            "hybrid/mock",
            ModelTask::EmbedHybrid,
            "mock/hybrid",
            "mock-hybrid",
        )])
        .build()
        .await
        .expect("build hybrid runtime")
}

fn hybrid_index() -> IndexType {
    IndexType::Vector(VectorIndexCfg {
        algorithm: VectorAlgo::Flat,
        metric: VectorMetric::Cosine,
        embedding: Some(EmbeddingCfg {
            alias: "hybrid/mock".to_string(),
            source_properties: vec!["content".to_string()],
            batch_size: 16,
            document_prefix: None,
            query_prefix: None,
        }),
    })
}

/// `Doc(content, embedding Vector{DIM}, tokens List<Vector{DIM}>)` — both index configs point
/// at the SAME hybrid alias + source, so the engine treats them as one single-pass group.
async fn define_hybrid_schema(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("content", DataType::String)
        .property_nullable("embedding", DataType::Vector { dimensions: DIM })
        .property_nullable(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
        )
        .index("embedding", hybrid_index())
        .index("tokens", hybrid_index())
        .apply()
        .await?;
    Ok(())
}

async fn read_doc(db: &Uni, content: &str) -> anyhow::Result<(Vec<f64>, Vec<Vec<f64>>)> {
    let res = db
        .session()
        .query(&format!(
            "MATCH (d:Doc {{content: '{content}'}}) RETURN d.embedding AS e, d.tokens AS t"
        ))
        .await?;
    let row = &res.rows()[0];
    let dense: Vec<f64> = serde_json::from_value(row.value("e").unwrap().clone().into())?;
    let tokens: Vec<Vec<f64>> = serde_json::from_value(row.value("t").unwrap().clone().into())?;
    Ok((dense, tokens))
}

/// Eager path: writing `content` auto-embeds BOTH columns from ONE hybrid inference.
#[tokio::test]
async fn hybrid_single_pass_eager() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(hybrid_runtime(calls.clone()).await)
        .build()
        .await?;
    define_hybrid_schema(&db).await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {content: 'hello world foo'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let (dense, tokens) = read_doc(&db, "hello world foo").await?;
    assert_eq!(
        dense,
        vec![3.0; DIM],
        "dense = [word_count; DIM]: {dense:?}"
    );
    assert_eq!(
        tokens,
        vec![vec![1.0; DIM], vec![2.0; DIM], vec![3.0; DIM]],
        "3 per-token vectors: {tokens:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "ONE hybrid inference must fill both columns (single pass)"
    );
    Ok(())
}

/// Deferred path: a batch of docs auto-embeds both columns with ONE batched hybrid inference.
#[tokio::test]
async fn hybrid_single_pass_deferred() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let cfg = UniConfig {
        defer_embeddings: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory()
        .config(cfg)
        .xervo_runtime(hybrid_runtime(calls.clone()).await)
        .build()
        .await?;
    define_hybrid_schema(&db).await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {content: 'one two'})").await?;
    tx.execute("CREATE (:Doc {content: 'a b c'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let (d1, t1) = read_doc(&db, "one two").await?;
    assert_eq!(d1, vec![2.0; DIM]);
    assert_eq!(t1, vec![vec![1.0; DIM], vec![2.0; DIM]]);
    let (d2, t2) = read_doc(&db, "a b c").await?;
    assert_eq!(d2, vec![3.0; DIM]);
    assert_eq!(t2, vec![vec![1.0; DIM], vec![2.0; DIM], vec![3.0; DIM]]);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "ONE batched hybrid inference must fill both columns for the whole batch"
    );
    Ok(())
}

/// A user-supplied column is preserved; only the missing one is auto-embedded (still via the
/// single hybrid pass).
#[tokio::test]
async fn hybrid_preserves_user_supplied_column() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(hybrid_runtime(calls.clone()).await)
        .build()
        .await?;
    define_hybrid_schema(&db).await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {content: 'x y', embedding: [5.0,5.0,5.0,5.0]})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let (dense, tokens) = read_doc(&db, "x y").await?;
    assert_eq!(
        dense,
        vec![5.0; DIM],
        "user-supplied dense must be preserved"
    );
    assert_eq!(
        tokens,
        vec![vec![1.0; DIM], vec![2.0; DIM]],
        "missing multi-vector column is auto-embedded: {tokens:?}"
    );
    Ok(())
}

/// Negative: a mixed dense + multi-vector group pointed at a single-task (`Embed`) alias can't
/// be served by one model — it must fail with a clear error, not silently half-fill.
#[tokio::test]
async fn hybrid_mixed_group_on_single_task_alias_errors() -> anyhow::Result<()> {
    let runtime = ModelRuntime::builder()
        .register_provider(DenseProvider)
        .catalog(vec![spec(
            "dense/only",
            ModelTask::Embed,
            "mock/dense",
            "mock-dense",
        )])
        .build()
        .await?;
    let db = Uni::temporary().xervo_runtime(runtime).build().await?;
    let dense_only_index = || {
        IndexType::Vector(VectorIndexCfg {
            algorithm: VectorAlgo::Flat,
            metric: VectorMetric::Cosine,
            embedding: Some(EmbeddingCfg {
                alias: "dense/only".to_string(),
                source_properties: vec!["content".to_string()],
                batch_size: 16,
                document_prefix: None,
                query_prefix: None,
            }),
        })
    };
    db.schema()
        .label("Doc")
        .property("content", DataType::String)
        .property_nullable("embedding", DataType::Vector { dimensions: DIM })
        .property_nullable(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
        )
        .index("embedding", dense_only_index())
        .index("tokens", dense_only_index())
        .apply()
        .await?;

    // The mixed group needs the hybrid head from a dense-only alias -> capability mismatch.
    let attempt = async {
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {content: 'a b'})").await?;
        tx.commit().await?;
        db.flush().await?;
        anyhow::Ok(())
    }
    .await;
    assert!(
        attempt.is_err(),
        "a mixed dense+multivector group on a single-task alias must error, not half-fill"
    );
    Ok(())
}
