// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Auto-embed for SPARSE (SPLADE / learned-sparse) properties (issue #95).
//!
//! A `SparseVector` property with an `EmbeddingCfg` is auto-embedded at write time via uni-xervo's
//! sparse model (`sparse_embedder`), and a text query to `uni.sparse.query` is auto-embedded at
//! query time. Uses a deterministic mock model — each whitespace word contributes a term whose id
//! is the word's character length with weight 1.0, so `"a bb ccc"` → `{1:1, 2:1, 3:1}` — exercising
//! the eager + deferred write paths, the query-text path, and the single-pass hybrid path.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use uni_common::UniConfig;
use uni_db::{
    DataType, EmbeddingCfg, IndexType, ModelAliasSpec, ModelTask, Uni, Value, VectorAlgo,
    VectorIndexCfg, VectorMetric, WarmupPolicy,
};
use uni_xervo::runtime::ModelRuntime;
use uni_xervo::traits::hybrid::{HeadSet, HybridEmbedResult, HybridEmbeddingModel};
use uni_xervo::traits::sparse::{SparseEmbedResult, SparseEmbeddingModel};
use uni_xervo::traits::{
    LoadedModelHandle, ModelInfo, ModelProvider, ProviderCapabilities, ProviderHealth,
};

const VOCAB: usize = 100;
const DIM: usize = 4;

/// `(term_id, weight)` pairs for a text: one `(char_len, 1.0)` per whitespace word (deliberately
/// unsorted / with duplicate terms, so `from_pairs` sort+sum is exercised).
fn word_len_terms(text: &str) -> Vec<(u32, f32)> {
    text.split_whitespace()
        .map(|w| (w.chars().count() as u32, 1.0f32))
        .collect()
}

// --- Mock sparse-only model + provider (task EmbedSparse). ---
struct WordLenSparse;
impl ModelInfo for WordLenSparse {
    fn model_id(&self) -> &str {
        "mock-sparse"
    }
}
#[async_trait]
impl SparseEmbeddingModel for WordLenSparse {
    async fn embed(&self, texts: &[&str]) -> uni_xervo::error::Result<SparseEmbedResult> {
        Ok(SparseEmbedResult {
            vectors: texts.iter().map(|t| word_len_terms(t)).collect(),
            usage: None,
        })
    }
    fn vocab_size(&self) -> u32 {
        VOCAB as u32
    }
}

struct MockSparseProvider;
#[async_trait]
impl ModelProvider for MockSparseProvider {
    fn provider_id(&self) -> &'static str {
        "mock/sparse"
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            supported_tasks: vec![ModelTask::EmbedSparse],
        }
    }
    async fn load(&self, _spec: &ModelAliasSpec) -> uni_xervo::error::Result<LoadedModelHandle> {
        let handle: Arc<dyn SparseEmbeddingModel> = Arc::new(WordLenSparse);
        Ok(Arc::new(handle) as LoadedModelHandle)
    }
    async fn health(&self) -> ProviderHealth {
        ProviderHealth::Healthy
    }
}

fn sparse_spec() -> ModelAliasSpec {
    ModelAliasSpec {
        alias: "sparse/mock".to_string(),
        task: ModelTask::EmbedSparse,
        provider_id: "mock/sparse".to_string(),
        model_id: "mock-sparse".to_string(),
        revision: None,
        warmup: WarmupPolicy::Lazy,
        required: false,
        timeout: None,
        load_timeout: None,
        retry: None,
        options: serde_json::json!({}),
    }
}

async fn sparse_runtime() -> Arc<ModelRuntime> {
    ModelRuntime::builder()
        .register_provider(MockSparseProvider)
        .catalog(vec![sparse_spec()])
        .build()
        .await
        .expect("build mock sparse runtime")
}

/// `Doc(title, content STRING, emb SparseVector)` with a sparse index whose `EmbeddingCfg`
/// auto-embeds `content` → `emb`. `emb` is nullable so the deferred path can write the row before
/// the embedding is materialized at flush.
async fn define_schema(db: &Uni, alias: &str) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("content", DataType::String)
        .property_nullable("emb", DataType::SparseVector { dimensions: VOCAB })
        .index(
            "emb",
            IndexType::sparse_with_embedding(
                VOCAB,
                EmbeddingCfg {
                    alias: alias.to_string(),
                    source_properties: vec!["content".to_string()],
                    batch_size: 16,
                    document_prefix: None,
                    query_prefix: None,
                },
            ),
        )
        .apply()
        .await?;
    Ok(())
}

fn sv(indices: Vec<u32>, values: Vec<f32>) -> Value {
    Value::SparseVector { indices, values }
}

/// Run `uni.sparse.query` with `q` (an explicit `SparseVector` or a `String` to auto-embed).
async fn run_query(db: &Uni, q: Value, k: i64) -> anyhow::Result<Vec<(String, f64)>> {
    let rows = db
        .session()
        .query_with(
            "CALL uni.sparse.query('Doc', 'emb', $q, $k, null, null, {}) \
             YIELD node, score RETURN node.title AS title, score",
        )
        .param("q", q)
        .param("k", Value::Int(k))
        .fetch_all()
        .await?;
    Ok(rows
        .iter()
        .map(|r| {
            (
                r.get::<String>("title").unwrap(),
                r.get::<f64>("score").unwrap(),
            )
        })
        .collect())
}

fn score_of(results: &[(String, f64)], title: &str) -> Option<f64> {
    results.iter().find(|(t, _)| t == title).map(|(_, s)| *s)
}

#[tokio::test]
async fn sparse_autoembed_eager() -> anyhow::Result<()> {
    let db = Uni::temporary()
        .xervo_runtime(sparse_runtime().await)
        .build()
        .await?;
    define_schema(&db, "sparse/mock").await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {title: 'abc', content: 'a bb ccc'})")
        .await?;
    tx.execute("CREATE (:Doc {title: 'a_only', content: 'x'})") // 'x' → term 1
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Query the exact embedding of 'a bb ccc' → {1,2,3}. abc self-matches (3.0); a_only shares term 1 (1.0).
    let results = run_query(&db, sv(vec![1, 2, 3], vec![1.0, 1.0, 1.0]), 5).await?;
    assert_eq!(score_of(&results, "abc"), Some(3.0), "{results:?}");
    assert_eq!(score_of(&results, "a_only"), Some(1.0), "{results:?}");
    Ok(())
}

#[tokio::test]
async fn sparse_autoembed_deferred() -> anyhow::Result<()> {
    let cfg = UniConfig {
        defer_embeddings: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory()
        .config(cfg)
        .xervo_runtime(sparse_runtime().await)
        .build()
        .await?;
    define_schema(&db, "sparse/mock").await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {title: 'two', content: 'aa bbb'})") // {2,3}
        .await?;
    tx.execute("CREATE (:Doc {title: 'one', content: 'aa'})") // {2}
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let results = run_query(&db, sv(vec![2, 3], vec![1.0, 1.0]), 5).await?;
    assert_eq!(score_of(&results, "two"), Some(2.0), "{results:?}");
    assert_eq!(score_of(&results, "one"), Some(1.0), "{results:?}");
    Ok(())
}

#[tokio::test]
async fn sparse_autoembed_query_text() -> anyhow::Result<()> {
    let db = Uni::temporary()
        .xervo_runtime(sparse_runtime().await)
        .build()
        .await?;
    define_schema(&db, "sparse/mock").await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {title: 'abc', content: 'a bb ccc'})")
        .await?;
    tx.execute("CREATE (:Doc {title: 'a_only', content: 'q'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // A STRING query is auto-embedded ('a bb ccc' → {1,2,3}); abc must rank first.
    let results = run_query(&db, Value::String("a bb ccc".into()), 5).await?;
    assert_eq!(
        results.first().map(|(t, _)| t.as_str()),
        Some("abc"),
        "query-text auto-embed must rank the self-match first: {results:?}"
    );
    assert_eq!(score_of(&results, "abc"), Some(3.0), "{results:?}");
    Ok(())
}

#[tokio::test]
async fn sparse_autoembed_set_source_reembeds() -> anyhow::Result<()> {
    let db = Uni::temporary()
        .xervo_runtime(sparse_runtime().await)
        .build()
        .await?;
    define_schema(&db, "sparse/mock").await?;

    // CREATE 'a bb ccc' → emb {1,2,3}.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {title: 'd', content: 'a bb ccc'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Update the source text: emb MUST refresh to 'dddd eeeee' → {4,5}.
    let tx = db.session().tx().await?;
    tx.execute("MATCH (d:Doc {title: 'd'}) SET d.content = 'dddd eeeee'")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    assert_eq!(
        score_of(
            &run_query(&db, sv(vec![4, 5], vec![1.0, 1.0]), 5).await?,
            "d"
        ),
        Some(2.0),
        "SET on a source column must re-embed to the new text"
    );
    assert_eq!(
        score_of(
            &run_query(&db, sv(vec![1, 2, 3], vec![1.0, 1.0, 1.0]), 5).await?,
            "d"
        ),
        None,
        "the stale embedding must not persist after SET"
    );
    Ok(())
}

#[tokio::test]
async fn sparse_autoembed_explicit_not_overwritten() -> anyhow::Result<()> {
    let db = Uni::temporary()
        .xervo_runtime(sparse_runtime().await)
        .build()
        .await?;
    define_schema(&db, "sparse/mock").await?;

    // emb supplied explicitly ({7:5}); content is present but must NOT overwrite it.
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Doc {title: 'x', content: 'a bb ccc', emb: $e})")
        .param("e", sv(vec![7], vec![5.0]))
        .run()
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Matches the explicit term 7 (5.0), not the content embedding {1,2,3} (0.0).
    assert_eq!(
        score_of(&run_query(&db, sv(vec![7], vec![1.0]), 5).await?, "x"),
        Some(5.0)
    );
    assert_eq!(
        score_of(
            &run_query(&db, sv(vec![1, 2, 3], vec![1.0, 1.0, 1.0]), 5).await?,
            "x"
        ),
        None,
        "content must not have been embedded over the explicit emb"
    );
    Ok(())
}

#[tokio::test]
async fn sparse_autoembed_via_cypher_ddl_embedding() -> anyhow::Result<()> {
    // The `embedding` option threads through the Cypher `CREATE VECTOR INDEX … OPTIONS{type:'sparse'}`
    // planner path (create-before-ingest, so eager embed fires at write time).
    let db = Uni::temporary()
        .xervo_runtime(sparse_runtime().await)
        .build()
        .await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("content", DataType::String)
        .property_nullable("emb", DataType::SparseVector { dimensions: VOCAB })
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE VECTOR INDEX emb_idx FOR (d:Doc) ON (d.emb) \
         OPTIONS {type: 'sparse', embedding: {alias: 'sparse/mock', source: ['content']}}",
    )
    .await?;
    tx.execute("CREATE (:Doc {title: 'abc', content: 'a bb ccc'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    assert_eq!(
        score_of(
            &run_query(&db, sv(vec![1, 2, 3], vec![1.0, 1.0, 1.0]), 5).await?,
            "abc"
        ),
        Some(3.0),
        "DDL embedding option must auto-embed content into emb"
    );
    Ok(())
}

// --- Single-pass hybrid: one model fills a dense `vec` and a sparse `emb` from ONE inference. ---
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
        let mut res = HybridEmbedResult::default();
        if heads.contains(HeadSet::DENSE) {
            res.dense = Some(
                texts
                    .iter()
                    .map(|t| vec![t.split_whitespace().count() as f32; DIM])
                    .collect(),
            );
        }
        if heads.contains(HeadSet::SPARSE) {
            res.sparse = Some(texts.iter().map(|t| word_len_terms(t)).collect());
        }
        Ok(res)
    }
    fn available_heads(&self) -> HeadSet {
        HeadSet::DENSE | HeadSet::SPARSE
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

#[tokio::test]
async fn sparse_autoembed_hybrid_single_pass() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let runtime = ModelRuntime::builder()
        .register_provider(HybridProvider {
            calls: calls.clone(),
        })
        .catalog(vec![ModelAliasSpec {
            alias: "hybrid/mock".to_string(),
            task: ModelTask::EmbedHybrid,
            provider_id: "mock/hybrid".to_string(),
            model_id: "mock-hybrid".to_string(),
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

    // A dense `vec` and a sparse `emb` sharing the SAME alias+source → one hybrid group.
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("content", DataType::String)
        .property_nullable("vec", DataType::Vector { dimensions: DIM })
        .property_nullable("emb", DataType::SparseVector { dimensions: VOCAB })
        .index(
            "vec",
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
            }),
        )
        .index(
            "emb",
            IndexType::sparse_with_embedding(
                VOCAB,
                EmbeddingCfg {
                    alias: "hybrid/mock".to_string(),
                    source_properties: vec!["content".to_string()],
                    batch_size: 16,
                    document_prefix: None,
                    query_prefix: None,
                },
            ),
        )
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {title: 'abc', content: 'a bb ccc'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // ONE forward pass filled both heads.
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "dense + sparse must share one inference"
    );
    // The sparse head populated emb ({1,2,3}).
    assert_eq!(
        score_of(
            &run_query(&db, sv(vec![1, 2, 3], vec![1.0, 1.0, 1.0]), 5).await?,
            "abc"
        ),
        Some(3.0),
        "hybrid sparse head must have populated emb"
    );
    Ok(())
}
