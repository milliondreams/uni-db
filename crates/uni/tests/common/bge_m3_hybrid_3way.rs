// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! BGE-M3 style 3-way hybrid auto-embed coverage (mock model).
//!
//! Exercises a single `EmbedHybrid` alias filling three index columns from one forward pass —
//! dense (`Vector`), learned-sparse (`SparseVector`), and multi-vector / ColBERT
//! (`List<Vector>`) — exactly the shape BGE-M3 produces. The tests assert the multi-vector
//! column round-trips through every distinct read path: post-flush, pre-flush (L0 scan
//! projection), deferred-batch materialization at flush, and direct Cypher `CREATE` literals,
//! each of which has its own Value→Arrow conversion.

use std::sync::Arc;

use async_trait::async_trait;
use uni_db::{
    DataType, EmbeddingCfg, IndexType, ModelAliasSpec, ModelTask, Uni, VectorAlgo, VectorIndexCfg,
    VectorMetric, WarmupPolicy,
};
use uni_xervo::runtime::ModelRuntime;
use uni_xervo::traits::hybrid::{HeadSet, HybridEmbedResult, HybridEmbeddingModel};
use uni_xervo::traits::{
    LoadedModelHandle, ModelInfo, ModelProvider, ProviderCapabilities, ProviderHealth,
};

const DIM: usize = 4;
const VOCAB: usize = 100;

/// 3-way hybrid mock: dense = [word_count; DIM]; sparse = one (i, len) pair per word;
/// multi = one [i+1; DIM] token per word.
struct ThreeWayHybrid;

impl ModelInfo for ThreeWayHybrid {
    fn model_id(&self) -> &str {
        "mock-hybrid-3way"
    }
}

#[async_trait]
impl HybridEmbeddingModel for ThreeWayHybrid {
    async fn embed(
        &self,
        texts: &[&str],
        heads: HeadSet,
    ) -> uni_xervo::error::Result<HybridEmbedResult> {
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
            res.sparse = Some(
                texts
                    .iter()
                    .map(|t| {
                        t.split_whitespace()
                            .enumerate()
                            .map(|(i, w)| (i as u32, w.len() as f32))
                            .collect::<Vec<_>>()
                    })
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
        HeadSet::DENSE | HeadSet::SPARSE | HeadSet::MULTI_VECTOR
    }
}

struct ThreeWayProvider;

#[async_trait]
impl ModelProvider for ThreeWayProvider {
    fn provider_id(&self) -> &'static str {
        "mock/hybrid3"
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            supported_tasks: vec![ModelTask::EmbedHybrid],
        }
    }
    async fn load(&self, _spec: &ModelAliasSpec) -> uni_xervo::error::Result<LoadedModelHandle> {
        let handle: Arc<dyn HybridEmbeddingModel> = Arc::new(ThreeWayHybrid);
        Ok(Arc::new(handle) as LoadedModelHandle)
    }
    async fn health(&self) -> ProviderHealth {
        ProviderHealth::Healthy
    }
}

fn spec() -> ModelAliasSpec {
    ModelAliasSpec {
        alias: "hybrid3/mock".to_string(),
        task: ModelTask::EmbedHybrid,
        provider_id: "mock/hybrid3".to_string(),
        model_id: "mock-hybrid-3way".to_string(),
        revision: None,
        warmup: WarmupPolicy::Lazy,
        required: false,
        timeout: None,
        load_timeout: None,
        retry: None,
        options: serde_json::json!({}),
    }
}

async fn runtime() -> Arc<ModelRuntime> {
    ModelRuntime::builder()
        .register_provider(ThreeWayProvider)
        .catalog(vec![spec()])
        .build()
        .await
        .expect("build 3-way hybrid runtime")
}

fn hybrid_emb() -> EmbeddingCfg {
    EmbeddingCfg {
        alias: "hybrid3/mock".to_string(),
        source_properties: vec!["content".to_string()],
        batch_size: 16,
        document_prefix: None,
        query_prefix: None,
    }
}

fn dense_index() -> IndexType {
    IndexType::Vector(VectorIndexCfg {
        algorithm: VectorAlgo::Flat,
        metric: VectorMetric::Cosine,
        embedding: Some(hybrid_emb()),
    })
}

/// `Doc(content, embedding Vector, emb SparseVector, tokens List<Vector>)` — all three index
/// configs share the SAME hybrid alias + source, so the engine treats them as ONE single-pass
/// 3-way group.
async fn define_schema(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("content", DataType::String)
        .property_nullable("embedding", DataType::Vector { dimensions: DIM })
        .property_nullable("emb", DataType::SparseVector { dimensions: VOCAB })
        .property_nullable(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
        )
        .index("embedding", dense_index())
        .index("emb", IndexType::sparse_with_embedding(VOCAB, hybrid_emb()))
        .index("tokens", dense_index())
        .apply()
        .await?;
    Ok(())
}

/// Post-flush: the multi-vector column reads back as a non-Null `List<Vector>`.
#[tokio::test]
async fn hybrid_3way_multivector_roundtrips() -> anyhow::Result<()> {
    let db = Uni::temporary()
        .xervo_runtime(runtime().await)
        .build()
        .await?;
    define_schema(&db).await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {content: 'hello world foo'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let res = db
        .session()
        .query("MATCH (d:Doc {content: 'hello world foo'}) RETURN d.embedding AS e, d.emb AS s, d.tokens AS t")
        .await?;
    let row = &res.rows()[0];

    let dense = row.value("e").cloned().unwrap_or(uni_db::Value::Null);
    let sparse = row.value("s").cloned().unwrap_or(uni_db::Value::Null);
    let tokens = row.value("t").cloned().unwrap_or(uni_db::Value::Null);

    eprintln!("dense  = {dense:?}");
    eprintln!("sparse = {sparse:?}");
    eprintln!("tokens = {tokens:?}");

    assert_ne!(
        tokens,
        uni_db::Value::Null,
        "multi-vector column read back as Null"
    );
    // 3 tokens, each a DIM-length vector.
    let expected = uni_db::Value::List(vec![
        uni_db::Value::Vector(vec![1.0; DIM]),
        uni_db::Value::Vector(vec![2.0; DIM]),
        uni_db::Value::Vector(vec![3.0; DIM]),
    ]);
    assert_eq!(tokens, expected, "tokens column mismatch");
    Ok(())
}

/// Same 3-way hybrid, but read the multi-vector column BEFORE `flush()` — i.e. from the
/// unflushed L0 buffer / scan projection path, which has its own Value→Arrow conversion.
#[tokio::test]
async fn hybrid_3way_multivector_roundtrips_pre_flush() -> anyhow::Result<()> {
    let db = Uni::temporary()
        .xervo_runtime(runtime().await)
        .build()
        .await?;
    define_schema(&db).await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {content: 'hello world foo'})")
        .await?;
    tx.commit().await?;
    // NO flush — read straight from L0.

    let res = db
        .session()
        .query("MATCH (d:Doc {content: 'hello world foo'}) RETURN d.tokens AS t")
        .await?;
    let row = &res.rows()[0];
    let tokens = row.value("t").cloned().unwrap_or(uni_db::Value::Null);
    eprintln!("tokens (pre-flush) = {tokens:?}");

    assert_ne!(
        tokens,
        uni_db::Value::Null,
        "multi-vector column read back as Null (pre-flush)"
    );
    Ok(())
}

/// Variant: `tokens` is auto-embedded + indexed but is NOT declared as a `List(Vector)`
/// schema property — so at flush it falls into the `build_overflow_json_column` CV-encoded
/// LargeBinary blob path rather than `build_list_column` (and `is_multivector_property`
/// returns false). Documents the behavior of the undeclared-column path.
async fn define_schema_tokens_undeclared(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("content", DataType::String)
        .property_nullable("embedding", DataType::Vector { dimensions: DIM })
        .property_nullable("emb", DataType::SparseVector { dimensions: VOCAB })
        // tokens intentionally NOT declared as a property
        .index("embedding", dense_index())
        .index("emb", IndexType::sparse_with_embedding(VOCAB, hybrid_emb()))
        .index("tokens", dense_index())
        .apply()
        .await?;
    Ok(())
}

#[tokio::test]
async fn hybrid_3way_multivector_tokens_undeclared() -> anyhow::Result<()> {
    let db = Uni::temporary()
        .xervo_runtime(runtime().await)
        .build()
        .await?;
    // If schema apply rejects an index on an undeclared property, that itself is informative.
    if let Err(e) = define_schema_tokens_undeclared(&db).await {
        eprintln!("schema apply rejected undeclared-tokens index: {e}");
        return Ok(());
    }

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {content: 'hello world foo'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let res = db
        .session()
        .query("MATCH (d:Doc {content: 'hello world foo'}) RETURN d.tokens AS t")
        .await?;
    let row = &res.rows()[0];
    let tokens = row.value("t").cloned().unwrap_or(uni_db::Value::Null);
    eprintln!("tokens (undeclared) = {tokens:?}");
    Ok(())
}

/// Write a multi-vector `List<Vector>` via a Cypher `CREATE` LITERAL (no auto-embed), so the
/// value flows through the DataFusion mutation operator's `rows_to_batches` /
/// `normalize_mutation_schema` projection — a different write path than `bulk_insert_vertices`'s
/// `build_list_column`. Asserts the literal round-trips exactly.
#[tokio::test]
async fn multivector_via_cypher_create_literal() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?; // no embedding runtime needed
    db.schema()
        .label("Doc")
        .property("content", DataType::String)
        .property_nullable(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
        )
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {content: 'x', tokens: [[1.0,2.0,3.0,4.0],[5.0,6.0,7.0,8.0]]})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let res = db
        .session()
        .query("MATCH (d:Doc {content: 'x'}) RETURN d.tokens AS t")
        .await?;
    let tokens = res.rows()[0]
        .value("t")
        .cloned()
        .unwrap_or(uni_db::Value::Null);
    eprintln!("tokens (cypher CREATE literal) = {tokens:?}");

    let expected = uni_db::Value::List(vec![
        uni_db::Value::Vector(vec![1.0, 2.0, 3.0, 4.0]),
        uni_db::Value::Vector(vec![5.0, 6.0, 7.0, 8.0]),
    ]);
    assert_eq!(
        tokens, expected,
        "multi-vector via Cypher CREATE corrupted (read back: {tokens:?})"
    );
    Ok(())
}

/// Same, but read BEFORE flush (L0) to see the pre-flush projection.
#[tokio::test]
async fn multivector_via_cypher_create_literal_pre_flush() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Doc")
        .property("content", DataType::String)
        .property_nullable(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
        )
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {content: 'x', tokens: [[1.0,2.0,3.0,4.0],[5.0,6.0,7.0,8.0]]})")
        .await?;
    tx.commit().await?;

    let res = db
        .session()
        .query("MATCH (d:Doc {content: 'x'}) RETURN d.tokens AS t")
        .await?;
    let tokens = res.rows()[0]
        .value("t")
        .cloned()
        .unwrap_or(uni_db::Value::Null);
    eprintln!("tokens (cypher CREATE literal, pre-flush) = {tokens:?}");
    let expected = uni_db::Value::List(vec![
        uni_db::Value::Vector(vec![1.0, 2.0, 3.0, 4.0]),
        uni_db::Value::Vector(vec![5.0, 6.0, 7.0, 8.0]),
    ]);
    assert_eq!(tokens, expected, "pre-flush multivector via Cypher CREATE");
    Ok(())
}

/// DEFERRED 3-way: many docs, embeddings materialized at flush via
/// `process_embeddings_for_batch` (a different code path from the eager single-row path).
/// This is what real at-scale bge-m3 ingest uses.
#[tokio::test]
async fn hybrid_3way_multivector_deferred_batch() -> anyhow::Result<()> {
    let cfg = uni_common::UniConfig {
        defer_embeddings: true,
        ..uni_common::UniConfig::default()
    };
    let db = Uni::temporary()
        .config(cfg)
        .xervo_runtime(runtime().await)
        .build()
        .await?;
    define_schema(&db).await?;

    let tx = db.session().tx().await?;
    for i in 0..50 {
        tx.execute(&format!("CREATE (:Doc {{content: 'doc number {i} here'}})"))
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;

    let res = db
        .session()
        .query("MATCH (d:Doc {content: 'doc number 0 here'}) RETURN d.tokens AS t")
        .await?;
    let tokens = res.rows()[0]
        .value("t")
        .cloned()
        .unwrap_or(uni_db::Value::Null);
    eprintln!("tokens (deferred batch) = {tokens:?}");
    assert_ne!(
        tokens,
        uni_db::Value::Null,
        "deferred-batch multi-vector read back as Null"
    );
    Ok(())
}
