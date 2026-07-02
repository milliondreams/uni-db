// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Capability-based embedding-alias routing (issues #129 + #130).
//!
//! A hybrid model (e.g. BGE-M3) may serve a *single* vector column on its own alias, not just a
//! mixed multi-head group. Write-time routing therefore tries the narrow single-head facade first
//! and, when the alias resolves to a hybrid model that doesn't implement that narrow trait, falls
//! back to the hybrid embedder for the one requested head (#129). The fallback reuses the
//! already-loaded model (no second load), and a head the model does not expose is a hard error,
//! never a silently-empty column.
//!
//! The mock hybrid counts both provider loads and `embed` calls and has a configurable
//! `available_heads()`, so these tests can PROVE the load/forward-pass accounting and the
//! partial-hybrid failure mode.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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

/// Mock hybrid model with a configurable head set; counts its `embed` calls. Each head is filled
/// deterministically from the whitespace-word structure of the input, but only for heads in
/// `requested ∩ available` (mirroring real hybrid semantics: an absent head yields `None`).
struct ConfigurableHybrid {
    calls: Arc<AtomicUsize>,
    available: HeadSet,
}

impl ModelInfo for ConfigurableHybrid {
    fn model_id(&self) -> &str {
        "mock-hybrid"
    }
}

#[async_trait]
impl HybridEmbeddingModel for ConfigurableHybrid {
    async fn embed(
        &self,
        texts: &[&str],
        heads: HeadSet,
    ) -> uni_xervo::error::Result<HybridEmbedResult> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let effective = heads & self.available;
        let mut res = HybridEmbedResult::default();
        if effective.contains(HeadSet::DENSE) {
            res.dense = Some(
                texts
                    .iter()
                    .map(|t| vec![t.split_whitespace().count() as f32; DIM])
                    .collect(),
            );
        }
        if effective.contains(HeadSet::SPARSE) {
            res.sparse = Some(
                texts
                    .iter()
                    .map(|t| {
                        t.split_whitespace()
                            .map(|w| (w.chars().count() as u32, 1.0f32))
                            .collect()
                    })
                    .collect(),
            );
        }
        if effective.contains(HeadSet::MULTI_VECTOR) {
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
        self.available
    }
}

/// Provider for [`ConfigurableHybrid`], counting how many times the model is *loaded* (distinct
/// from how many times it is *invoked*), so the #129 "no double work" claim is observable.
struct CountingHybridProvider {
    calls: Arc<AtomicUsize>,
    loads: Arc<AtomicUsize>,
    available: HeadSet,
}

#[async_trait]
impl ModelProvider for CountingHybridProvider {
    fn provider_id(&self) -> &'static str {
        "mock/hybrid"
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            supported_tasks: vec![ModelTask::EmbedHybrid],
        }
    }
    async fn load(&self, _spec: &ModelAliasSpec) -> uni_xervo::error::Result<LoadedModelHandle> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        let handle: Arc<dyn HybridEmbeddingModel> = Arc::new(ConfigurableHybrid {
            calls: self.calls.clone(),
            available: self.available,
        });
        Ok(Arc::new(handle) as LoadedModelHandle)
    }
    async fn health(&self) -> ProviderHealth {
        ProviderHealth::Healthy
    }
}

fn spec(alias: &str, task: ModelTask) -> ModelAliasSpec {
    ModelAliasSpec {
        alias: alias.to_string(),
        task,
        provider_id: "mock/hybrid".to_string(),
        model_id: "mock-hybrid".to_string(),
        revision: None,
        warmup: WarmupPolicy::Lazy,
        required: false,
        timeout: None,
        load_timeout: None,
        retry: None,
        options: serde_json::json!({}),
    }
}

/// A runtime exposing one `EmbedHybrid` alias `hybrid/mock`, with shared load/call counters and a
/// configurable head set.
async fn hybrid_runtime(
    calls: Arc<AtomicUsize>,
    loads: Arc<AtomicUsize>,
    available: HeadSet,
) -> Arc<ModelRuntime> {
    ModelRuntime::builder()
        .register_provider(CountingHybridProvider {
            calls,
            loads,
            available,
        })
        .catalog(vec![spec("hybrid/mock", ModelTask::EmbedHybrid)])
        .build()
        .await
        .expect("build hybrid runtime")
}

fn dense_index(alias: &str, source: &str) -> IndexType {
    IndexType::Vector(VectorIndexCfg {
        algorithm: VectorAlgo::Flat,
        metric: VectorMetric::Cosine,
        embedding: Some(EmbeddingCfg {
            alias: alias.to_string(),
            source_properties: vec![source.to_string()],
            batch_size: 16,
            document_prefix: None,
            query_prefix: None,
        }),
    })
}

fn embedding_cfg(alias: &str, source: &str) -> EmbeddingCfg {
    EmbeddingCfg {
        alias: alias.to_string(),
        source_properties: vec![source.to_string()],
        batch_size: 16,
        document_prefix: None,
        query_prefix: None,
    }
}

/// #129: a *lone* dense `Vector` column on a hybrid alias auto-embeds via the hybrid fallback —
/// loading the model exactly once and running exactly one forward pass.
#[tokio::test]
async fn lone_dense_on_hybrid_alias_routes_via_fallback() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let loads = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(hybrid_runtime(calls.clone(), loads.clone(), HeadSet::ALL).await)
        .build()
        .await?;
    db.schema()
        .label("Doc")
        .property("content", DataType::String)
        .property_nullable("embedding", DataType::Vector { dimensions: DIM })
        .index("embedding", dense_index("hybrid/mock", "content"))
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {content: 'hello world foo'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let res = db
        .session()
        .query("MATCH (d:Doc {content: 'hello world foo'}) RETURN d.embedding AS e")
        .await?;
    let dense: Vec<f64> = serde_json::from_value(res.rows()[0].value("e").unwrap().clone().into())?;
    assert_eq!(
        dense,
        vec![3.0; DIM],
        "dense = [word_count; DIM]: {dense:?}"
    );
    assert_eq!(
        loads.load(Ordering::SeqCst),
        1,
        "the hybrid model must load exactly once (no narrow/hybrid double-load)"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "exactly one forward pass fills the lone dense column"
    );
    Ok(())
}

/// #129: a lone sparse `SparseVector` column on a hybrid alias auto-embeds via the hybrid fallback.
#[tokio::test]
async fn lone_sparse_on_hybrid_alias_routes_via_fallback() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let loads = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(hybrid_runtime(calls.clone(), loads.clone(), HeadSet::ALL).await)
        .build()
        .await?;
    db.schema()
        .label("Doc")
        .property("content", DataType::String)
        .property_nullable("emb", DataType::SparseVector { dimensions: VOCAB })
        .index(
            "emb",
            IndexType::sparse_with_embedding(VOCAB, embedding_cfg("hybrid/mock", "content")),
        )
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {content: 'a bb ccc'})").await?;
    tx.commit().await?;
    db.flush().await?;

    // The row exists and the lone-sparse fallback ran exactly once (one load, one pass).
    let res = db
        .session()
        .query("MATCH (d:Doc {content: 'a bb ccc'}) RETURN d.emb IS NOT NULL AS has")
        .await?;
    let has: bool = serde_json::from_value(res.rows()[0].value("has").unwrap().clone().into())?;
    assert!(
        has,
        "sparse column must be auto-embedded via hybrid fallback"
    );
    assert_eq!(loads.load(Ordering::SeqCst), 1, "model loads once");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "one forward pass");
    Ok(())
}

/// #129: a lone multi-vector `List<Vector>` column on a hybrid alias auto-embeds via the fallback.
#[tokio::test]
async fn lone_multi_on_hybrid_alias_routes_via_fallback() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let loads = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(hybrid_runtime(calls.clone(), loads.clone(), HeadSet::ALL).await)
        .build()
        .await?;
    db.schema()
        .label("Doc")
        .property("content", DataType::String)
        .property_nullable(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
        )
        .index("tokens", dense_index("hybrid/mock", "content"))
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {content: 'one two'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let res = db
        .session()
        .query("MATCH (d:Doc {content: 'one two'}) RETURN d.tokens AS t")
        .await?;
    let tokens: Vec<Vec<f64>> =
        serde_json::from_value(res.rows()[0].value("t").unwrap().clone().into())?;
    assert_eq!(
        tokens,
        vec![vec![1.0; DIM], vec![2.0; DIM]],
        "two per-token vectors via hybrid multi-vector head: {tokens:?}"
    );
    assert_eq!(loads.load(Ordering::SeqCst), 1, "model loads once");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "one forward pass");
    Ok(())
}

/// A hybrid model that does NOT expose the required head must produce a hard error, never a
/// silently-empty column (proposal §4.3): a `DENSE`-only model asked for the sparse head.
#[tokio::test]
async fn partial_hybrid_missing_head_errors() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let loads = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(hybrid_runtime(calls.clone(), loads.clone(), HeadSet::DENSE).await)
        .build()
        .await?;
    db.schema()
        .label("Doc")
        .property("content", DataType::String)
        .property_nullable("emb", DataType::SparseVector { dimensions: VOCAB })
        .index(
            "emb",
            IndexType::sparse_with_embedding(VOCAB, embedding_cfg("hybrid/mock", "content")),
        )
        .apply()
        .await?;

    let attempt = async {
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {content: 'a bb'})").await?;
        tx.commit().await?;
        db.flush().await?;
        anyhow::Ok(())
    }
    .await;
    let err = attempt.expect_err("a hybrid lacking the sparse head must error, not half-fill");
    assert!(
        err.to_string().contains("sparse"),
        "error should name the missing sparse head: {err}"
    );
    Ok(())
}

/// The auto-embedded column survives close + reopen, and reading it back triggers no new
/// embedding work on the fresh runtime.
#[tokio::test]
async fn lone_dense_reopen_roundtrip() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().expect("utf-8 temp path");

    {
        let calls = Arc::new(AtomicUsize::new(0));
        let loads = Arc::new(AtomicUsize::new(0));
        let db = Uni::open(path)
            .xervo_runtime(hybrid_runtime(calls.clone(), loads, HeadSet::ALL).await)
            .build()
            .await?;
        db.schema()
            .label("Doc")
            .property("content", DataType::String)
            .property_nullable("embedding", DataType::Vector { dimensions: DIM })
            .index("embedding", dense_index("hybrid/mock", "content"))
            .apply()
            .await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {content: 'persist me now'})")
            .await?;
        tx.commit().await?;
        db.flush().await?;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    // Reopen with a fresh runtime: the value is already persisted, so no re-embed happens.
    let calls = Arc::new(AtomicUsize::new(0));
    let loads = Arc::new(AtomicUsize::new(0));
    let db = Uni::open(path)
        .xervo_runtime(hybrid_runtime(calls.clone(), loads.clone(), HeadSet::ALL).await)
        .build()
        .await?;
    let res = db
        .session()
        .query("MATCH (d:Doc {content: 'persist me now'}) RETURN d.embedding AS e")
        .await?;
    let dense: Vec<f64> = serde_json::from_value(res.rows()[0].value("e").unwrap().clone().into())?;
    assert_eq!(dense, vec![3.0; DIM], "persisted dense survives reopen");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "reading a persisted embedding must not re-invoke the model"
    );
    assert_eq!(
        loads.load(Ordering::SeqCst),
        0,
        "reopen + read must not load the model"
    );
    Ok(())
}

/// A dense `Vector` index on `alias` sourced from `content`.
fn dense_col(alias: &str) -> (&'static str, DataType, IndexType) {
    (
        "embedding",
        DataType::Vector { dimensions: DIM },
        dense_index(alias, "content"),
    )
}

/// A sparse `SparseVector` index on `alias` sourced from `content`.
fn sparse_col(alias: &str) -> (&'static str, DataType, IndexType) {
    (
        "emb",
        DataType::SparseVector { dimensions: VOCAB },
        IndexType::sparse_with_embedding(VOCAB, embedding_cfg(alias, "content")),
    )
}

/// A multi-vector `List<Vector>` index on `alias` sourced from `content`.
fn multi_col(alias: &str) -> (&'static str, DataType, IndexType) {
    (
        "tokens",
        DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
        dense_index(alias, "content"),
    )
}

/// Persist a `Doc` schema (no rows) with the given `(property, type, index)` columns, so a later
/// reopen runs open-time embedding-alias validation against the persisted schema (the reopen path
/// where #130 manifests). DDL applies under a permissive prebuilt runtime (validation is skipped on
/// that path) and no row is written.
async fn persist_doc(path: &str, columns: Vec<(&str, DataType, IndexType)>) -> anyhow::Result<()> {
    let rt = hybrid_runtime(
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        HeadSet::ALL,
    )
    .await;
    let db = Uni::open(path).xervo_runtime(rt).build().await?;
    let mut schema = db
        .schema()
        .label("Doc")
        .property("content", DataType::String);
    for (prop, dt, _) in &columns {
        schema = schema.property_nullable(prop, dt.clone());
    }
    for (prop, _, index) in columns {
        schema = schema.index(prop, index);
    }
    schema.apply().await?;
    db.flush().await?;
    Ok(())
}

/// Reopen the DB at `path` with `catalog`, exercising the open-time validation path.
fn reopen(path: &str, catalog: Vec<ModelAliasSpec>) -> uni_db::UniBuilder {
    Uni::open(path).xervo_catalog(catalog)
}

/// Assert a reopen got PAST open-time validation. It may still fail afterward, but only because the
/// mock provider can't be registered on the `.xervo_catalog()` path — never the capability
/// rejection (the old "must be an embed task" or the new "cannot produce ... head(s)").
fn assert_passed_validation<T, E: std::fmt::Display>(result: Result<T, E>) {
    if let Err(e) = result {
        let msg = e.to_string();
        assert!(
            !msg.contains("must be an embed task") && !msg.contains("cannot produce"),
            "expected to pass open-time validation, got a capability rejection: {msg}"
        );
    }
}

/// #130: a dense `Vector` index on an `EmbedHybrid` alias must PASS open-time validation on reopen
/// (it was wrongly rejected with "must be an embed task" before the fix).
#[tokio::test]
async fn reopen_accepts_hybrid_alias_on_vector_index() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().expect("utf-8 temp path");
    persist_doc(path, vec![dense_col("x")]).await?;
    assert_passed_validation(
        reopen(path, vec![spec("x", ModelTask::EmbedHybrid)])
            .build()
            .await,
    );
    Ok(())
}

/// #130 multi-vector face: a `List<Vector>` index on an `EmbedMultiVector` alias must be accepted.
#[tokio::test]
async fn reopen_accepts_multivector_on_multivector_task() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().expect("utf-8 temp path");
    persist_doc(path, vec![multi_col("x")]).await?;
    assert_passed_validation(
        reopen(path, vec![spec("x", ModelTask::EmbedMultiVector)])
            .build()
            .await,
    );
    Ok(())
}

/// Backward-compat: the pre-fix two-alias workaround (one `Embed` + one `EmbedSparse` alias) must
/// still pass validation — the fix must not invalidate KBs built to dodge #129.
#[tokio::test]
async fn reopen_accepts_two_alias_workaround() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().expect("utf-8 temp path");
    persist_doc(path, vec![dense_col("dense/a"), sparse_col("sparse/b")]).await?;
    assert_passed_validation(
        reopen(
            path,
            vec![
                spec("dense/a", ModelTask::Embed),
                spec("sparse/b", ModelTask::EmbedSparse),
            ],
        )
        .build()
        .await,
    );
    Ok(())
}

/// #130 / sparse gap: a `Sparse` index on an `Embed` (dense-only) alias must be REJECTED at reopen
/// with a clear capability error (sparse aliases were previously unvalidated).
#[tokio::test]
async fn reopen_rejects_sparse_on_embed_task() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().expect("utf-8 temp path");
    persist_doc(path, vec![sparse_col("x")]).await?;
    let err = reopen(path, vec![spec("x", ModelTask::Embed)])
        .build()
        .await
        .err()
        .expect("an Embed alias cannot produce a sparse head");
    let msg = err.to_string();
    assert!(
        msg.contains("cannot produce") && msg.contains("emb"),
        "rejection should name the offending sparse column: {msg}"
    );
    Ok(())
}

/// A dense column on an `EmbedSparse` alias must be rejected (head mismatch in the other direction).
#[tokio::test]
async fn reopen_rejects_dense_on_sparse_task() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().expect("utf-8 temp path");
    persist_doc(path, vec![dense_col("x")]).await?;
    let err = reopen(path, vec![spec("x", ModelTask::EmbedSparse)])
        .build()
        .await
        .err()
        .expect("an EmbedSparse alias cannot produce a dense head");
    assert!(
        err.to_string().contains("cannot produce"),
        "expected a capability rejection: {err}"
    );
    Ok(())
}

/// Text-modality rule (§4.1): an image-embedding alias produces no *text* head, so a dense text
/// column bound to it must be rejected — even though an image model emits a dense vector.
#[tokio::test]
async fn reopen_rejects_dense_on_image_task() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().expect("utf-8 temp path");
    persist_doc(path, vec![dense_col("x")]).await?;
    let err = reopen(path, vec![spec("x", ModelTask::EmbedImage)])
        .build()
        .await
        .err()
        .expect("an image-embedding alias cannot auto-embed a text column");
    assert!(
        err.to_string().contains("cannot produce"),
        "expected a capability rejection for the text-modality mismatch: {err}"
    );
    Ok(())
}

/// Mixed-on-narrow guard: dense + sparse columns sharing one `Embed` alias union to
/// `{DENSE,SPARSE} ⊄ {DENSE}` and must be rejected, naming the sparse column.
#[tokio::test]
async fn reopen_rejects_mixed_heads_on_single_task_alias() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().expect("utf-8 temp path");
    persist_doc(path, vec![dense_col("x"), sparse_col("x")]).await?;
    let err = reopen(path, vec![spec("x", ModelTask::Embed)])
        .build()
        .await
        .err()
        .expect("a dense+sparse union on an Embed alias must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("cannot produce") && msg.contains("emb"),
        "rejection should name the sparse column the Embed alias can't serve: {msg}"
    );
    Ok(())
}

/// The preserved "missing alias" error still fires when a referenced alias is absent from the
/// catalog at reopen.
#[tokio::test]
async fn reopen_reports_missing_alias() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().expect("utf-8 temp path");
    persist_doc(path, vec![dense_col("x")]).await?;
    let err = reopen(path, vec![spec("other", ModelTask::Embed)])
        .build()
        .await
        .err()
        .expect("a schema alias absent from the catalog must error");
    let msg = err.to_string();
    assert!(
        msg.contains("Missing Uni-Xervo alias") && msg.contains("x"),
        "expected the preserved missing-alias error naming 'x': {msg}"
    );
    Ok(())
}

/// All three heads sharing one hybrid alias + source resolve through a SINGLE load and a SINGLE
/// forward pass, with each head written to its own column (head→column wiring).
#[tokio::test]
async fn hybrid_three_heads_single_load_single_pass() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let loads = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(hybrid_runtime(calls.clone(), loads.clone(), HeadSet::ALL).await)
        .build()
        .await?;
    db.schema()
        .label("Doc")
        .property("content", DataType::String)
        .property_nullable("embedding", DataType::Vector { dimensions: DIM })
        .property_nullable("emb", DataType::SparseVector { dimensions: VOCAB })
        .property_nullable(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
        )
        .index("embedding", dense_index("hybrid/mock", "content"))
        .index(
            "emb",
            IndexType::sparse_with_embedding(VOCAB, embedding_cfg("hybrid/mock", "content")),
        )
        .index("tokens", dense_index("hybrid/mock", "content"))
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {content: 'a bb ccc'})").await?;
    tx.commit().await?;
    db.flush().await?;

    assert_eq!(
        loads.load(Ordering::SeqCst),
        1,
        "one alias+source → one load"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "one forward pass fills all three heads"
    );
    let res = db
        .session()
        .query(
            "MATCH (d:Doc {content: 'a bb ccc'}) RETURN d.embedding AS e, d.tokens AS t, \
             d.emb IS NOT NULL AS s",
        )
        .await?;
    let row = &res.rows()[0];
    let dense: Vec<f64> = serde_json::from_value(row.value("e").unwrap().clone().into())?;
    let tokens: Vec<Vec<f64>> = serde_json::from_value(row.value("t").unwrap().clone().into())?;
    let has_sparse: bool = serde_json::from_value(row.value("s").unwrap().clone().into())?;
    assert_eq!(dense, vec![3.0; DIM], "dense head → embedding column");
    assert_eq!(
        tokens.len(),
        3,
        "multi-vector head → tokens column (3 words)"
    );
    assert!(has_sparse, "sparse head → emb column");
    Ok(())
}

/// Two separate writes on the same hybrid alias load the model once and run one pass each — the
/// loaded model (and its facade wrapper) is reused, not reloaded.
#[tokio::test]
async fn second_write_reuses_loaded_model() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let loads = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(hybrid_runtime(calls.clone(), loads.clone(), HeadSet::ALL).await)
        .build()
        .await?;
    db.schema()
        .label("Doc")
        .property("content", DataType::String)
        .property_nullable("embedding", DataType::Vector { dimensions: DIM })
        .index("embedding", dense_index("hybrid/mock", "content"))
        .apply()
        .await?;

    for content in ["one", "two three"] {
        let tx = db.session().tx().await?;
        tx.execute(&format!("CREATE (:Doc {{content: '{content}'}})"))
            .await?;
        tx.commit().await?;
        db.flush().await?;
    }
    assert_eq!(
        loads.load(Ordering::SeqCst),
        1,
        "the model loads once across multiple writes"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "one forward pass per write"
    );
    Ok(())
}

/// A hybrid that exposes exactly the requested subset (`DENSE|SPARSE`, no multi) serves a
/// dense+sparse group from one pass without error.
#[tokio::test]
async fn hybrid_serves_its_available_subset() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let loads = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(
            hybrid_runtime(
                calls.clone(),
                loads.clone(),
                HeadSet::DENSE | HeadSet::SPARSE,
            )
            .await,
        )
        .build()
        .await?;
    db.schema()
        .label("Doc")
        .property("content", DataType::String)
        .property_nullable("embedding", DataType::Vector { dimensions: DIM })
        .property_nullable("emb", DataType::SparseVector { dimensions: VOCAB })
        .index("embedding", dense_index("hybrid/mock", "content"))
        .index(
            "emb",
            IndexType::sparse_with_embedding(VOCAB, embedding_cfg("hybrid/mock", "content")),
        )
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {content: 'a bb'})").await?;
    tx.commit().await?;
    db.flush().await?;

    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "one pass for the dense+sparse subset"
    );
    let res = db
        .session()
        .query("MATCH (d:Doc {content: 'a bb'}) RETURN d.embedding AS e, d.emb IS NOT NULL AS s")
        .await?;
    let row = &res.rows()[0];
    let dense: Vec<f64> = serde_json::from_value(row.value("e").unwrap().clone().into())?;
    let has_sparse: bool = serde_json::from_value(row.value("s").unwrap().clone().into())?;
    assert_eq!(dense, vec![2.0; DIM], "dense filled (2 words)");
    assert!(has_sparse, "sparse filled");
    Ok(())
}

/// The ≥2-head path's existing guard: a hybrid lacking a *requested* head in a mixed group is a
/// hard error, not a silently-empty column (proposal §4.3).
#[tokio::test]
async fn mixed_group_missing_head_on_partial_hybrid_errors() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let loads = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(hybrid_runtime(calls.clone(), loads.clone(), HeadSet::DENSE).await)
        .build()
        .await?;
    db.schema()
        .label("Doc")
        .property("content", DataType::String)
        .property_nullable("embedding", DataType::Vector { dimensions: DIM })
        .property_nullable("emb", DataType::SparseVector { dimensions: VOCAB })
        .index("embedding", dense_index("hybrid/mock", "content"))
        .index(
            "emb",
            IndexType::sparse_with_embedding(VOCAB, embedding_cfg("hybrid/mock", "content")),
        )
        .apply()
        .await?;

    let attempt = async {
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {content: 'a bb'})").await?;
        tx.commit().await?;
        db.flush().await?;
        anyhow::Ok(())
    }
    .await;
    let err = attempt.expect_err("a hybrid lacking the sparse head must error in a mixed group");
    assert!(
        err.to_string().contains("sparse"),
        "error should name the missing sparse head: {err}"
    );
    Ok(())
}

/// A user-supplied column value is preserved and the model is never invoked — even on the hybrid
/// fallback path (the `!contains_key` guard still holds).
#[tokio::test]
async fn explicit_value_preserved_on_hybrid_lone_dense() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let loads = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(hybrid_runtime(calls.clone(), loads.clone(), HeadSet::ALL).await)
        .build()
        .await?;
    db.schema()
        .label("Doc")
        .property("content", DataType::String)
        .property_nullable("embedding", DataType::Vector { dimensions: DIM })
        .index("embedding", dense_index("hybrid/mock", "content"))
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {content: 'x y', embedding: [5.0, 5.0, 5.0, 5.0]})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let res = db
        .session()
        .query("MATCH (d:Doc {content: 'x y'}) RETURN d.embedding AS e")
        .await?;
    let dense: Vec<f64> = serde_json::from_value(res.rows()[0].value("e").unwrap().clone().into())?;
    assert_eq!(dense, vec![5.0; DIM], "user-supplied dense preserved");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "a fully user-supplied row must not invoke the model"
    );
    Ok(())
}

/// #137: a schema/model width mismatch (column declared wider than the model's
/// output) fails the write with an error naming the alias — instead of the
/// pre-fix behavior where the wrong-width embedding was inserted, silently
/// nulled by the Arrow converters at flush, and detonated at shutdown.
#[tokio::test]
async fn autoembed_model_output_dim_mismatch_is_actionable_error() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let loads = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(hybrid_runtime(calls.clone(), loads.clone(), HeadSet::ALL).await)
        .build()
        .await?;
    db.schema()
        .label("Doc")
        .property("content", DataType::String)
        // Declared wider than the mock model's DIM-wide output.
        .property_nullable(
            "embedding",
            DataType::Vector {
                dimensions: DIM + 4,
            },
        )
        .index("embedding", dense_index("hybrid/mock", "content"))
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    let err = tx
        .query_with("CREATE (:Doc {content: 'hello world'})")
        .fetch_all()
        .await
        .expect_err("a model emitting DIM-wide vectors into VECTOR(DIM+4) must fail the write");
    let msg = err.to_string();
    assert!(
        msg.contains("hybrid/mock"),
        "the error must name the embedding alias so the misconfiguration is actionable: {msg}"
    );
    assert!(
        msg.contains(&DIM.to_string()) && msg.contains(&(DIM + 4).to_string()),
        "the error must carry both widths: {msg}"
    );
    Ok(())
}
