// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Auto-embed PARITY suite across embedding modalities (dense / multi-vector / sparse / hybrid).
//!
//! Fills the cross-modality coverage matrix with shared, deterministic, call-counting mocks so
//! every modality exercises the same scenarios: deferred write, query-time text embed, error paths
//! (no runtime / no embedding config), explicit-not-overwritten, multi-source, document_prefix,
//! persistence across reopen, SET re-embed, and batched inference. Per-modality happy-path basics
//! live in `{multivec,hybrid,sparse}_autoembed.rs`; this file closes the gaps and asserts parity.
//!
//! Mock convention: each whitespace word contributes a term. dense = `[word_count; DIM]`;
//! multi = one `[i+1; DIM]` per word; sparse = `(char_len, 1.0)` per word.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use tempfile::TempDir;
use uni_common::UniConfig;
use uni_db::{
    DataType, EmbeddingCfg, IndexType, ModelAliasSpec, ModelTask, Uni, Value, VectorAlgo,
    VectorIndexCfg, VectorMetric, WarmupPolicy,
};
use uni_xervo::runtime::ModelRuntime;
use uni_xervo::traits::multivector::{MultiVectorEmbedResult, MultiVectorEmbeddingModel};
use uni_xervo::traits::sparse::{SparseEmbedResult, SparseEmbeddingModel};
use uni_xervo::traits::{
    EmbedResult, EmbeddingModel, LoadedModelHandle, ModelInfo, ModelProvider, ProviderCapabilities,
    ProviderHealth,
};

const DIM: usize = 4;
const VOCAB: usize = 100;

// ─────────────────────────── mocks (call-counting) ───────────────────────────

/// Dense: `[word_count; DIM]`. `prefix_marker` lets a prefix test detect prefixing
/// (a `document_prefix`/`query_prefix` adds its own words to the count).
struct DenseMock {
    calls: Arc<AtomicUsize>,
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
                .map(|t| vec![t.split_whitespace().count() as f32; DIM])
                .collect(),
            usage: None,
        })
    }
    fn dimensions(&self) -> u32 {
        DIM as u32
    }
}

struct SparseMock {
    calls: Arc<AtomicUsize>,
}
impl ModelInfo for SparseMock {
    fn model_id(&self) -> &str {
        "mock-sparse"
    }
}
#[async_trait]
impl SparseEmbeddingModel for SparseMock {
    async fn embed(&self, texts: &[&str]) -> uni_xervo::error::Result<SparseEmbedResult> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(SparseEmbedResult {
            vectors: texts
                .iter()
                .map(|t| {
                    t.split_whitespace()
                        .map(|w| (w.chars().count() as u32, 1.0f32))
                        .collect()
                })
                .collect(),
            usage: None,
        })
    }
    fn vocab_size(&self) -> u32 {
        VOCAB as u32
    }
}

struct MultiMock {
    calls: Arc<AtomicUsize>,
}
impl ModelInfo for MultiMock {
    fn model_id(&self) -> &str {
        "mock-multi"
    }
}
#[async_trait]
impl MultiVectorEmbeddingModel for MultiMock {
    async fn embed(&self, texts: &[&str]) -> uni_xervo::error::Result<MultiVectorEmbedResult> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(MultiVectorEmbedResult {
            vectors: texts
                .iter()
                .map(|t| {
                    let n = t.split_whitespace().count().max(1);
                    (0..n).map(|i| vec![(i + 1) as f32; DIM]).collect()
                })
                .collect(),
            usage: None,
        })
    }
    fn dimensions(&self) -> u32 {
        DIM as u32
    }
}

/// Provider for a single dedicated head, parameterized by task + the handle factory.
macro_rules! single_head_provider {
    ($name:ident, $task:expr, $id:literal, $make:expr) => {
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
                    supported_tasks: vec![$task],
                }
            }
            async fn load(
                &self,
                _spec: &ModelAliasSpec,
            ) -> uni_xervo::error::Result<LoadedModelHandle> {
                let h = ($make)(self.calls.clone());
                Ok(Arc::new(h) as LoadedModelHandle)
            }
            async fn health(&self) -> ProviderHealth {
                ProviderHealth::Healthy
            }
        }
    };
}

single_head_provider!(DenseProvider, ModelTask::Embed, "mock/dense", |c| {
    let h: Arc<dyn EmbeddingModel> = Arc::new(DenseMock { calls: c });
    h
});
single_head_provider!(SparseProvider, ModelTask::EmbedSparse, "mock/sparse", |c| {
    let h: Arc<dyn SparseEmbeddingModel> = Arc::new(SparseMock { calls: c });
    h
});
single_head_provider!(
    MultiProvider,
    ModelTask::EmbedMultiVector,
    "mock/multi",
    |c| {
        let h: Arc<dyn MultiVectorEmbeddingModel> = Arc::new(MultiMock { calls: c });
        h
    }
);

fn spec(alias: &str, task: ModelTask, provider: &str, model: &str) -> ModelAliasSpec {
    ModelAliasSpec {
        alias: alias.to_string(),
        task,
        provider_id: provider.to_string(),
        model_id: model.to_string(),
        revision: None,
        warmup: WarmupPolicy::Lazy,
        required: false,
        timeout: None,
        load_timeout: None,
        retry: None,
        options: serde_json::json!({}),
    }
}

fn emb_cfg(alias: &str, sources: &[&str], doc_prefix: Option<&str>) -> EmbeddingCfg {
    EmbeddingCfg {
        alias: alias.to_string(),
        source_properties: sources.iter().map(|s| s.to_string()).collect(),
        batch_size: 16,
        document_prefix: doc_prefix.map(|s| s.to_string()),
        query_prefix: None,
    }
}

/// Read a dense `emb` column back as `Vec<f64>` (`[]`-decoded). Empty if null/missing.
async fn read_dense(db: &Uni, title: &str) -> anyhow::Result<Vec<f64>> {
    let res = db
        .session()
        .query(&format!(
            "MATCH (d:Doc {{title: '{title}'}}) RETURN d.emb AS e"
        ))
        .await?;
    let v = res.rows()[0].value("e").unwrap().clone();
    Ok(serde_json::from_value(v.into()).unwrap_or_default())
}

// ─────────────────────────── DENSE ───────────────────────────

async fn dense_runtime(calls: Arc<AtomicUsize>) -> Arc<ModelRuntime> {
    ModelRuntime::builder()
        .register_provider(DenseProvider { calls })
        .catalog(vec![spec(
            "d/mock",
            ModelTask::Embed,
            "mock/dense",
            "mock-dense",
        )])
        .build()
        .await
        .unwrap()
}

async fn dense_schema(db: &Uni, embedding: Option<EmbeddingCfg>) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("content", DataType::String)
        .property_nullable("emb", DataType::Vector { dimensions: DIM })
        .index(
            "emb",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::Cosine,
                embedding,
            }),
        )
        .apply()
        .await?;
    Ok(())
}

#[tokio::test]
async fn dense_autoembed_deferred() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let cfg = UniConfig {
        defer_embeddings: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory()
        .config(cfg)
        .xervo_runtime(dense_runtime(calls).await)
        .build()
        .await?;
    dense_schema(&db, Some(emb_cfg("d/mock", &["content"], None))).await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {title: 't', content: 'a b c'})")
        .await?; // 3 words
    tx.commit().await?;
    db.flush().await?;

    assert_eq!(
        read_dense(&db, "t").await?,
        vec![3.0; DIM],
        "deferred dense embed"
    );
    Ok(())
}

#[tokio::test]
async fn dense_autoembed_explicit_not_overwritten() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(dense_runtime(calls).await)
        .build()
        .await?;
    dense_schema(&db, Some(emb_cfg("d/mock", &["content"], None))).await?;

    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Doc {title: 't', content: 'a b c', emb: $e})")
        .param("e", Value::List(vec![Value::Float(9.0); DIM]))
        .run()
        .await?;
    tx.commit().await?;
    db.flush().await?;

    assert_eq!(
        read_dense(&db, "t").await?,
        vec![9.0; DIM],
        "explicit emb must win"
    );
    Ok(())
}

#[tokio::test]
async fn dense_autoembed_set_reembeds() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(dense_runtime(calls).await)
        .build()
        .await?;
    dense_schema(&db, Some(emb_cfg("d/mock", &["content"], None))).await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {title: 't', content: 'a b c'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    assert_eq!(read_dense(&db, "t").await?, vec![3.0; DIM]);

    let tx = db.session().tx().await?;
    tx.execute("MATCH (d:Doc {title: 't'}) SET d.content = 'a b'")
        .await?; // 2 words
    tx.commit().await?;
    db.flush().await?;
    assert_eq!(
        read_dense(&db, "t").await?,
        vec![2.0; DIM],
        "SET must re-embed dense"
    );
    Ok(())
}

#[tokio::test]
async fn dense_autoembed_persistence_across_reopen() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    {
        let calls = Arc::new(AtomicUsize::new(0));
        let db = Uni::open(&path)
            .xervo_runtime(dense_runtime(calls).await)
            .build()
            .await?;
        dense_schema(&db, Some(emb_cfg("d/mock", &["content"], None))).await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {title: 't', content: 'a b c'})")
            .await?;
        tx.commit().await?;
        db.flush().await?;
    }
    // Reopen with a fresh runtime (a schema with embedding aliases requires the catalog at
    // open). The embedding was materialized at write time, so reading must NOT re-embed — the
    // call counter stays 0 and the persisted vector comes back unchanged.
    let reopen_calls = Arc::new(AtomicUsize::new(0));
    let db = Uni::open(&path)
        .xervo_runtime(dense_runtime(reopen_calls.clone()).await)
        .build()
        .await?;
    assert_eq!(
        read_dense(&db, "t").await?,
        vec![3.0; DIM],
        "embed must survive reopen"
    );
    assert_eq!(
        reopen_calls.load(Ordering::SeqCst),
        0,
        "reopen must not re-embed"
    );
    Ok(())
}

#[tokio::test]
async fn dense_autoembed_document_prefix_applied() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(dense_runtime(calls).await)
        .build()
        .await?;
    // document_prefix "x y " adds 2 words → 'a b c' embeds as count 5, not 3.
    dense_schema(&db, Some(emb_cfg("d/mock", &["content"], Some("x y ")))).await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {title: 't', content: 'a b c'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    assert_eq!(
        read_dense(&db, "t").await?,
        vec![5.0; DIM],
        "document_prefix must be prepended before embedding"
    );
    Ok(())
}

#[tokio::test]
async fn dense_autoembed_batches_one_inference() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let db = Uni::in_memory()
        .config(UniConfig {
            defer_embeddings: true,
            ..UniConfig::default()
        })
        .xervo_runtime(dense_runtime(calls.clone()).await)
        .build()
        .await?;
    dense_schema(&db, Some(emb_cfg("d/mock", &["content"], None))).await?;

    let tx = db.session().tx().await?;
    for i in 0..5 {
        tx.execute(&format!("CREATE (:Doc {{title: 't{i}', content: 'a b'}})"))
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;
    // Deferred path batches a label's rows into ONE embed call.
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "5 docs must embed in one batched inference"
    );
    Ok(())
}

#[tokio::test]
async fn dense_autoembed_string_query_requires_runtime() -> anyhow::Result<()> {
    // Embedding config present but NO runtime → a text query must error clearly.
    let db = Uni::temporary().build().await?;
    dense_schema(&db, Some(emb_cfg("d/mock", &["content"], None))).await?;
    let err = db
        .session()
        .query("CALL uni.vector.query('Doc', 'emb', 'some text', 3) YIELD node RETURN node")
        .await
        .err();
    assert!(err.is_some(), "string query without runtime must error");
    Ok(())
}

#[tokio::test]
async fn dense_autoembed_string_query_requires_config() -> anyhow::Result<()> {
    // Index has NO embedding_config → a text query cannot auto-embed → error.
    let calls = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(dense_runtime(calls).await)
        .build()
        .await?;
    dense_schema(&db, None).await?;
    let err = db
        .session()
        .query("CALL uni.vector.query('Doc', 'emb', 'some text', 3) YIELD node RETURN node")
        .await
        .err();
    assert!(
        err.is_some(),
        "string query without embedding_config must error"
    );
    Ok(())
}

// ─────────────────────────── MULTI-VECTOR ───────────────────────────

async fn multi_runtime(calls: Arc<AtomicUsize>) -> Arc<ModelRuntime> {
    ModelRuntime::builder()
        .register_provider(MultiProvider { calls })
        .catalog(vec![spec(
            "m/mock",
            ModelTask::EmbedMultiVector,
            "mock/multi",
            "mock-multi",
        )])
        .build()
        .await
        .unwrap()
}

async fn multi_schema(db: &Uni, embedding: Option<EmbeddingCfg>) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("content", DataType::String)
        .property_nullable(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
        )
        .index(
            "tokens",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::Cosine,
                embedding,
            }),
        )
        .apply()
        .await?;
    Ok(())
}

async fn read_multi(db: &Uni, title: &str) -> anyhow::Result<Vec<Vec<f64>>> {
    let res = db
        .session()
        .query(&format!(
            "MATCH (d:Doc {{title: '{title}'}}) RETURN d.tokens AS t"
        ))
        .await?;
    let v = res.rows()[0].value("t").unwrap().clone();
    Ok(serde_json::from_value(v.into()).unwrap_or_default())
}

fn expect_tokens(n: usize) -> Vec<Vec<f64>> {
    (0..n).map(|i| vec![(i + 1) as f64; DIM]).collect()
}

#[tokio::test]
async fn multi_autoembed_set_reembeds() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(multi_runtime(calls).await)
        .build()
        .await?;
    multi_schema(&db, Some(emb_cfg("m/mock", &["content"], None))).await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {title: 't', content: 'a b c'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    assert_eq!(read_multi(&db, "t").await?, expect_tokens(3));

    let tx = db.session().tx().await?;
    tx.execute("MATCH (d:Doc {title: 't'}) SET d.content = 'a b'")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    assert_eq!(
        read_multi(&db, "t").await?,
        expect_tokens(2),
        "SET must re-embed multi"
    );
    Ok(())
}

#[tokio::test]
async fn multi_autoembed_persistence_across_reopen() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    {
        let calls = Arc::new(AtomicUsize::new(0));
        let db = Uni::open(&path)
            .xervo_runtime(multi_runtime(calls).await)
            .build()
            .await?;
        multi_schema(&db, Some(emb_cfg("m/mock", &["content"], None))).await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {title: 't', content: 'a b c'})")
            .await?;
        tx.commit().await?;
        db.flush().await?;
    }
    let reopen = Arc::new(AtomicUsize::new(0));
    let db = Uni::open(&path)
        .xervo_runtime(multi_runtime(reopen.clone()).await)
        .build()
        .await?;
    assert_eq!(
        read_multi(&db, "t").await?,
        expect_tokens(3),
        "multi embed survives reopen"
    );
    assert_eq!(reopen.load(Ordering::SeqCst), 0, "reopen must not re-embed");
    Ok(())
}

#[tokio::test]
async fn multi_autoembed_string_query_requires_runtime() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    multi_schema(&db, Some(emb_cfg("m/mock", &["content"], None))).await?;
    let err = db
        .session()
        .query("CALL uni.vector.query('Doc', 'tokens', 'some text', 3) YIELD node RETURN node")
        .await
        .err();
    assert!(
        err.is_some(),
        "multi string query without runtime must error"
    );
    Ok(())
}

// ─────────────────────────── SPARSE ───────────────────────────

async fn sparse_runtime(calls: Arc<AtomicUsize>) -> Arc<ModelRuntime> {
    ModelRuntime::builder()
        .register_provider(SparseProvider { calls })
        .catalog(vec![spec(
            "s/mock",
            ModelTask::EmbedSparse,
            "mock/sparse",
            "mock-sparse",
        )])
        .build()
        .await
        .unwrap()
}

async fn sparse_schema(db: &Uni, embedding: Option<EmbeddingCfg>) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("content", DataType::String)
        .property_nullable("emb", DataType::SparseVector { dimensions: VOCAB })
        .index(
            "emb",
            IndexType::Sparse {
                dimensions: VOCAB,
                quantize: true,
                embedding,
            },
        )
        .apply()
        .await?;
    Ok(())
}

/// Top sparse query result title for an explicit `{indices,values}` probe.
async fn sparse_top(
    db: &Uni,
    indices: Vec<u32>,
    values: Vec<f32>,
) -> anyhow::Result<Option<String>> {
    let res = db
        .session()
        .query_with(
            "CALL uni.sparse.query('Doc', 'emb', $q, 5, null, null, {}) \
             YIELD node, score RETURN node.title AS title",
        )
        .param("q", Value::SparseVector { indices, values })
        .param("k", Value::Int(5))
        .fetch_all()
        .await?;
    Ok(res
        .rows()
        .first()
        .map(|r| r.get::<String>("title").unwrap()))
}

#[tokio::test]
async fn sparse_autoembed_persistence_across_reopen() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    {
        let calls = Arc::new(AtomicUsize::new(0));
        let db = Uni::open(&path)
            .xervo_runtime(sparse_runtime(calls).await)
            .build()
            .await?;
        sparse_schema(&db, Some(emb_cfg("s/mock", &["content"], None))).await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {title: 't', content: 'a bb ccc'})")
            .await?; // {1,2,3}
        tx.commit().await?;
        db.flush().await?;
    }
    let reopen = Arc::new(AtomicUsize::new(0));
    let db = Uni::open(&path)
        .xervo_runtime(sparse_runtime(reopen.clone()).await)
        .build()
        .await?;
    assert_eq!(
        sparse_top(&db, vec![1, 2, 3], vec![1.0, 1.0, 1.0]).await?,
        Some("t".to_string()),
        "sparse embed survives reopen"
    );
    assert_eq!(reopen.load(Ordering::SeqCst), 0, "reopen must not re-embed");
    Ok(())
}

#[tokio::test]
async fn sparse_autoembed_multi_source() -> anyhow::Result<()> {
    // Two source columns are joined before embedding: 'a' + 'bb ccc' → 'a bb ccc' → {1,2,3}.
    let calls = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(sparse_runtime(calls).await)
        .build()
        .await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("a", DataType::String)
        .property("b", DataType::String)
        .property_nullable("emb", DataType::SparseVector { dimensions: VOCAB })
        .index(
            "emb",
            IndexType::Sparse {
                dimensions: VOCAB,
                quantize: true,
                embedding: Some(emb_cfg("s/mock", &["a", "b"], None)),
            },
        )
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {title: 't', a: 'a', b: 'bb ccc'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    assert_eq!(
        sparse_top(&db, vec![1, 2, 3], vec![1.0, 1.0, 1.0]).await?,
        Some("t".to_string()),
        "multi-source must join both columns before embedding"
    );
    Ok(())
}
