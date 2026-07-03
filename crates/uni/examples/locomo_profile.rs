//! LoCoMo runtime-performance profile of BGE-M3's three retrieval heads.
//!
//! A single `aapot/bge-m3-onnx` `EmbedHybrid` pass produces the dense, learned-
//! sparse (SPLADE-style) and multi-vector (ColBERT late-interaction) heads for
//! every LoCoMo dialogue turn, so the *embedding* cost is shared across heads.
//! What differs per head — and what this harness measures — is what happens
//! afterward in uni's real ANN indexes: insert time, index build time, on-disk
//! footprint, and KNN query latency through the HNSW / sparse-inverted / MUVERA
//! structures.
//!
//! Embeddings are computed once up front and the three heads are then loaded as
//! *precomputed* vectors into three single-index databases (`embedding: None`),
//! so the per-head timings isolate index/query cost from the shared embed pass.
//!
//! # Examples
//! ```text
//! cargo run --release --example locomo_profile --features provider-onnx
//! ```
//!
//! Environment overrides:
//! - `LOCOMO_JSON` — path to `locomo10.json`
//!   (default `~/.cache/uniko-bench/datasets/locomo/locomo10.json`).
//! - `LOCOMO_NUM_CONVS` — conversations to pool into one corpus (default `3`).
//! - `PROFILE_K` — top-k for KNN queries (default `10`).
//! - `LOCOMO_PROFILE_DIR` — scratch dir for the per-head databases.
//! - `BGE_M3_DIR` / `HF_HUB_CACHE` / `HF_HOME` — model directory resolution
//!   (falls back to the standard Hugging Face hub cache).
//
// Rust guideline compliant: application-style example (M-APP-ERROR uses anyhow;
// all items are private to the example binary, so no public-API docs apply).

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, anyhow};
use serde_json::Value as Json;

use uni_db::{
    DataType, IndexType, ModelAliasSpec, ModelTask, Uni, Value, VectorAlgo, VectorIndexCfg,
    VectorMetric, WarmupPolicy,
};
use uni_xervo::provider::LocalOnnxProvider;
use uni_xervo::runtime::ModelRuntime;
use uni_xervo::traits::hybrid::{HeadSet, HybridEmbeddingModel};

/// BGE-M3 dense / ColBERT token dimension.
const DIM: usize = 1024;
/// XLM-RoBERTa vocabulary size = learned-sparse dimensionality.
const VOCAB: usize = 250_002;
/// Catalog alias bound to the hybrid `EmbedHybrid` model.
const ALIAS: &str = "hybrid/bge-m3";
/// Batch size for embedding passes (turns and questions).
const EMBED_BATCH: usize = 32;
/// Docs per write transaction; bounds commit size so large multi-vector
/// payloads do not exceed uni's commit-timeout window.
const INSERT_BATCH: usize = 200;

/// Resolves the BGE-M3 model directory (env override, then HF hub cache).
fn model_dir() -> String {
    if let Ok(dir) = std::env::var("BGE_M3_DIR") {
        return dir;
    }
    if let Ok(dir) = std::env::var("HF_HUB_CACHE") {
        return dir;
    }
    if let Ok(hf_home) = std::env::var("HF_HOME") {
        return format!("{hf_home}/hub");
    }
    let home = std::env::var("HOME").expect("HOME must be set to locate the HF hub cache");
    format!("{home}/.cache/huggingface/hub")
}

/// Builds the `EmbedHybrid` catalog spec for the local ONNX BGE-M3 model.
fn spec() -> ModelAliasSpec {
    ModelAliasSpec {
        alias: ALIAS.to_string(),
        task: ModelTask::EmbedHybrid,
        provider_id: "local/onnx".to_string(),
        model_id: "aapot/bge-m3-onnx".to_string(),
        revision: None,
        warmup: WarmupPolicy::Lazy,
        required: false,
        timeout: None,
        load_timeout: None,
        retry: None,
        options: serde_json::json!({ "cache_dir": model_dir() }),
    }
}

/// Latency summary in milliseconds over a set of queries.
#[derive(Debug, Clone, Copy)]
struct Latency {
    mean: f64,
    p50: f64,
    p95: f64,
    p99: f64,
}

impl Latency {
    /// Computes mean / p50 / p95 / p99 from per-query millisecond samples.
    fn from_samples(mut samples: Vec<f64>) -> Self {
        samples.sort_by(f64::total_cmp);
        let n = samples.len().max(1);
        let mean = samples.iter().sum::<f64>() / n as f64;
        Latency {
            mean,
            p50: percentile(&samples, 0.50),
            p95: percentile(&samples, 0.95),
            p99: percentile(&samples, 0.99),
        }
    }
}

/// Nearest-rank percentile of an ascending-sorted slice (empty → 0.0).
fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p * sorted.len() as f64).ceil() as usize).saturating_sub(1);
    sorted[idx.min(sorted.len() - 1)]
}

/// Per-head profiling result rolled up for the final report.
#[derive(Debug)]
struct HeadReport {
    name: &'static str,
    index_kind: &'static str,
    n_docs: usize,
    /// Logical stored floats (dense: n·DIM; sparse: Σnnz; multivec: Σtokens·DIM).
    floats: u64,
    insert_ms: f64,
    build_ms: f64,
    disk_bytes: u64,
    rss_after_build_mb: f64,
    latency: Latency,
    qps: f64,
}

/// Pooled LoCoMo corpus: dialogue turns to index and questions to query.
struct Corpus {
    turns: Vec<String>,
    questions: Vec<String>,
}

/// Loads and pools the first `n` LoCoMo conversations into one corpus.
///
/// # Errors
/// Returns an error if the file is missing or not the expected LoCoMo shape.
fn load_locomo(path: &Path, n: usize) -> Result<Corpus> {
    let data: Json = serde_json::from_reader(BufReader::new(
        File::open(path).with_context(|| format!("open {}", path.display()))?,
    ))
    .context("parse locomo json")?;
    let arr = data
        .as_array()
        .ok_or_else(|| anyhow!("locomo root is not an array"))?;

    let mut turns = Vec::new();
    let mut questions = Vec::new();
    for sample in arr.iter().take(n) {
        let conv = sample["conversation"]
            .as_object()
            .ok_or_else(|| anyhow!("sample missing conversation object"))?;
        // Session turn lists have array values keyed "session_<k>"; the sibling
        // "session_<k>_date_time" keys are strings and are skipped by is_array().
        for (key, val) in conv {
            if !key.starts_with("session") {
                continue;
            }
            let Some(list) = val.as_array() else { continue };
            for turn in list {
                if let Some(text) = turn["text"].as_str() {
                    let speaker = turn["speaker"].as_str().unwrap_or("");
                    turns.push(format!("{speaker}: {text}"));
                }
            }
        }
        if let Some(qa) = sample["qa"].as_array() {
            for item in qa {
                if let Some(q) = item["question"].as_str() {
                    questions.push(q.to_string());
                }
            }
        }
    }
    Ok(Corpus { turns, questions })
}

/// All three heads for a batch of texts, plus the wall-clock embed time.
struct Embeddings {
    seconds: f64,
    dense: Vec<Vec<f32>>,
    sparse: Vec<Vec<(u32, f32)>>,
    multi: Vec<Vec<Vec<f32>>>,
}

/// Embeds `texts` producing dense + sparse + multi-vector heads in one pass each.
///
/// # Errors
/// Returns an error if the model omits any requested head or inference fails.
async fn embed_heads(embedder: &dyn HybridEmbeddingModel, texts: &[String]) -> Result<Embeddings> {
    let heads = HeadSet::DENSE | HeadSet::SPARSE | HeadSet::MULTI_VECTOR;
    let mut dense = Vec::with_capacity(texts.len());
    let mut sparse = Vec::with_capacity(texts.len());
    let mut multi = Vec::with_capacity(texts.len());

    let start = Instant::now();
    for chunk in texts.chunks(EMBED_BATCH) {
        let refs: Vec<&str> = chunk.iter().map(String::as_str).collect();
        let res = embedder
            .embed(&refs, heads)
            .await
            .map_err(|e| anyhow!("hybrid embed failed: {e}"))?;
        dense.extend(
            res.dense
                .ok_or_else(|| anyhow!("model returned no dense head"))?,
        );
        sparse.extend(
            res.sparse
                .ok_or_else(|| anyhow!("model returned no sparse head"))?,
        );
        multi.extend(
            res.multi_vector
                .ok_or_else(|| anyhow!("model returned no multi-vector head"))?,
        );
    }
    Ok(Embeddings {
        seconds: start.elapsed().as_secs_f64(),
        dense,
        sparse,
        multi,
    })
}

/// Opens a fresh (wiped) database rooted at `path`.
///
/// # Errors
/// Returns an error if the directory cannot be reset or the DB cannot open.
async fn fresh_db(path: &Path) -> Result<Uni> {
    if path.exists() {
        std::fs::remove_dir_all(path).ok();
    }
    std::fs::create_dir_all(path)?;
    // Default commit_timeout is 5s; multi-vector List<Vector> batches write far
    // more to Lance per doc than dense/sparse and need a wider window.
    let cfg = uni_common::UniConfig {
        commit_timeout: std::time::Duration::from_secs(300),
        ..uni_common::UniConfig::default()
    };
    Uni::open(path.to_string_lossy().to_string())
        .config(cfg)
        .build()
        .await
        .map_err(Into::into)
}

/// Recursively sums file sizes under `path` (bytes on disk).
fn dir_size(path: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            match entry.metadata() {
                Ok(m) if m.is_dir() => total += dir_size(&entry.path()),
                Ok(m) => total += m.len(),
                Err(_) => {}
            }
        }
    }
    total
}

/// Current process resident-set size in MiB (Linux `/proc/self/status`).
fn rss_mb() -> f64 {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return 0.0;
    };
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: f64 = rest
                .split_whitespace()
                .next()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            return kb / 1024.0;
        }
    }
    0.0
}

/// Encodes multi-vector tokens as `List<List<Float>>` (uni param encoding).
fn mv_value(tokens: &[Vec<f32>]) -> Value {
    Value::List(
        tokens
            .iter()
            .map(|t| Value::List(t.iter().map(|&x| Value::Float(x as f64)).collect()))
            .collect(),
    )
}

/// Splits sparse `(index, weight)` pairs into sorted-unique parallel arrays.
fn norm_sparse(pairs: &[(u32, f32)]) -> (Vec<u32>, Vec<f32>) {
    let mut pairs = pairs.to_vec();
    pairs.sort_by_key(|&(i, _)| i);
    pairs.dedup_by_key(|&mut (i, _)| i);
    let indices = pairs.iter().map(|&(i, _)| i).collect();
    let values = pairs.iter().map(|&(_, v)| v).collect();
    (indices, values)
}

/// Profiles the dense head through an HNSW index.
async fn profile_dense(
    path: &Path,
    docs: &[Vec<f32>],
    queries: &[Vec<f32>],
    k: usize,
) -> Result<HeadReport> {
    let db = fresh_db(path).await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("emb", DataType::Vector { dimensions: DIM })
        .index(
            "emb",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Hnsw {
                    m: 16,
                    ef_construction: 100,
                    partitions: None,
                },
                metric: VectorMetric::Cosine,
                embedding: None,
            }),
        )
        .apply()
        .await?;

    let insert_start = Instant::now();
    let mut tx = db.session().tx().await?;
    for (i, v) in docs.iter().enumerate() {
        tx.execute_with("CREATE (:Doc {title: $title, emb: $emb})")
            .param("title", Value::String(format!("d{i}")))
            .param("emb", Value::Vector(v.clone()))
            .run()
            .await?;
        if (i + 1) % INSERT_BATCH == 0 {
            tx.commit().await?;
            tx = db.session().tx().await?;
        }
    }
    tx.commit().await?;
    let insert_ms = insert_start.elapsed().as_secs_f64() * 1000.0;

    let build_start = Instant::now();
    db.flush().await?;
    db.indexes().rebuild("Doc", false).await?;
    let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;
    let rss_after_build_mb = rss_mb();

    let cypher = "CALL uni.vector.query('Doc', 'emb', $q, $k, null, null, {ef_search: 100}) \
                  YIELD node, score RETURN node.title AS title";
    let mut samples = Vec::with_capacity(queries.len());
    let qstart = Instant::now();
    for q in queries {
        let t = Instant::now();
        db.session()
            .query_with(cypher)
            .param("q", Value::Vector(q.clone()))
            .param("k", Value::Int(k as i64))
            .fetch_all()
            .await?;
        samples.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    let qps = queries.len() as f64 / qstart.elapsed().as_secs_f64();
    let floats = (docs.len() * DIM) as u64;

    Ok(HeadReport {
        name: "dense",
        index_kind: "HNSW (m=16, efc=100, ef_search=100)",
        n_docs: docs.len(),
        floats,
        insert_ms,
        build_ms,
        disk_bytes: dir_size(path),
        rss_after_build_mb,
        latency: Latency::from_samples(samples),
        qps,
    })
}

/// Profiles the learned-sparse head through the inverted sparse index.
async fn profile_sparse(
    path: &Path,
    docs: &[Vec<(u32, f32)>],
    queries: &[Vec<(u32, f32)>],
    k: usize,
) -> Result<HeadReport> {
    let db = fresh_db(path).await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("emb", DataType::SparseVector { dimensions: VOCAB })
        .index(
            "emb",
            IndexType::Sparse {
                dimensions: VOCAB,
                quantize: false,
                embedding: None,
            },
        )
        .apply()
        .await?;

    let insert_start = Instant::now();
    let mut tx = db.session().tx().await?;
    let mut nnz: u64 = 0;
    for (i, doc) in docs.iter().enumerate() {
        let (indices, values) = norm_sparse(doc);
        nnz += indices.len() as u64;
        tx.execute_with("CREATE (:Doc {title: $title, emb: $emb})")
            .param("title", Value::String(format!("d{i}")))
            .param("emb", Value::SparseVector { indices, values })
            .run()
            .await?;
        if (i + 1) % INSERT_BATCH == 0 {
            tx.commit().await?;
            tx = db.session().tx().await?;
        }
    }
    tx.commit().await?;
    let insert_ms = insert_start.elapsed().as_secs_f64() * 1000.0;

    // Sparse indexing materializes at flush; no ANN rebuild step.
    let build_start = Instant::now();
    db.flush().await?;
    let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;
    let rss_after_build_mb = rss_mb();

    let cypher = "CALL uni.sparse.query('Doc', 'emb', $q, $k, null, null, {}) \
                  YIELD node, score RETURN node.title AS title";
    let mut samples = Vec::with_capacity(queries.len());
    let qstart = Instant::now();
    for q in queries {
        let (indices, values) = norm_sparse(q);
        let t = Instant::now();
        db.session()
            .query_with(cypher)
            .param("q", Value::SparseVector { indices, values })
            .param("k", Value::Int(k as i64))
            .fetch_all()
            .await?;
        samples.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    let qps = queries.len() as f64 / qstart.elapsed().as_secs_f64();

    Ok(HeadReport {
        name: "sparse",
        index_kind: "inverted (f32, VOCAB=250002)",
        n_docs: docs.len(),
        floats: nnz,
        insert_ms,
        build_ms,
        disk_bytes: dir_size(path),
        rss_after_build_mb,
        latency: Latency::from_samples(samples),
        qps,
    })
}

/// Profiles the multi-vector head through a MUVERA (FDE) index.
async fn profile_multi(
    path: &Path,
    docs: &[Vec<Vec<f32>>],
    queries: &[Vec<Vec<f32>>],
    k: usize,
) -> Result<HeadReport> {
    let db = fresh_db(path).await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
        )
        .index(
            "tokens",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Muvera {
                    k_sim: 4,
                    reps: 8,
                    d_proj: 8,
                    seed: uni_db::api::schema::DEFAULT_FDE_SEED,
                    inner: Box::new(VectorAlgo::Flat),
                },
                metric: VectorMetric::Cosine,
                embedding: None,
            }),
        )
        .apply()
        .await?;

    let insert_start = Instant::now();
    let mut tx = db.session().tx().await?;
    let mut tokens_total: u64 = 0;
    for (i, doc) in docs.iter().enumerate() {
        tokens_total += doc.len() as u64;
        tx.execute_with("CREATE (:Doc {title: $title, tokens: $toks})")
            .param("title", Value::String(format!("d{i}")))
            .param("toks", mv_value(doc))
            .run()
            .await?;
        if (i + 1) % INSERT_BATCH == 0 {
            tx.commit().await?;
            tx = db.session().tx().await?;
        }
    }
    tx.commit().await?;
    let insert_ms = insert_start.elapsed().as_secs_f64() * 1000.0;

    let build_start = Instant::now();
    db.flush().await?;
    db.indexes().rebuild("Doc", false).await?;
    let build_ms = build_start.elapsed().as_secs_f64() * 1000.0;
    let rss_after_build_mb = rss_mb();

    let cypher = "CALL uni.vector.query('Doc', 'tokens', $q, $k, null, null, {}) \
                  YIELD node, score RETURN node.title AS title";
    let mut samples = Vec::with_capacity(queries.len());
    let qstart = Instant::now();
    for q in queries {
        let t = Instant::now();
        db.session()
            .query_with(cypher)
            .param("q", mv_value(q))
            .param("k", Value::Int(k as i64))
            .fetch_all()
            .await?;
        samples.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    let qps = queries.len() as f64 / qstart.elapsed().as_secs_f64();
    let floats = tokens_total * DIM as u64;

    Ok(HeadReport {
        name: "multivector",
        index_kind: "MUVERA FDE (k_sim=4, reps=8, d_proj=8)",
        n_docs: docs.len(),
        floats,
        insert_ms,
        build_ms,
        disk_bytes: dir_size(path),
        rss_after_build_mb,
        latency: Latency::from_samples(samples),
        qps,
    })
}

/// Prints the per-head performance comparison table.
fn print_report(
    convs: usize,
    n_turns: usize,
    n_queries: usize,
    turn_embed_s: f64,
    query_embed_s: f64,
    reports: &[HeadReport],
) {
    let mb = |b: u64| b as f64 / (1024.0 * 1024.0);
    println!("\n══════════════════════════════════════════════════════════════════════");
    println!(" LoCoMo BGE-M3 3-head runtime profile — {convs} conversations");
    println!("══════════════════════════════════════════════════════════════════════");
    println!(" corpus: {n_turns} turns indexed, {n_queries} questions queried (k per head)");
    println!(" shared embed (all 3 heads, one hybrid pass):");
    println!(
        "   turns : {:6.1}s  = {:7.1} turns/s",
        turn_embed_s,
        n_turns as f64 / turn_embed_s
    );
    println!(
        "   queries: {:5.1}s  = {:7.1} queries/s",
        query_embed_s,
        n_queries as f64 / query_embed_s
    );
    println!("──────────────────────────────────────────────────────────────────────");
    println!(
        " {:<12} {:>10} {:>10} {:>9} {:>9} {:>8} {:>8} {:>8} {:>8}",
        "head", "insert(ms)", "build(ms)", "disk(MB)", "vec(MB)", "mean", "p95", "p99", "QPS"
    );
    println!(
        " {:<12} {:>10} {:>10} {:>9} {:>9} {:>8} {:>8} {:>8} {:>8}",
        "", "", "", "", "", "(ms)", "(ms)", "(ms)", ""
    );
    for r in reports {
        println!(
            " {:<12} {:>10.0} {:>10.0} {:>9.1} {:>9.1} {:>8.2} {:>8.2} {:>8.2} {:>8.0}",
            r.name,
            r.insert_ms,
            r.build_ms,
            mb(r.disk_bytes),
            r.floats as f64 * 4.0 / (1024.0 * 1024.0),
            r.latency.mean,
            r.latency.p95,
            r.latency.p99,
            r.qps,
        );
    }
    println!("──────────────────────────────────────────────────────────────────────");
    for r in reports {
        println!(
            "   {:<12} index: {}  ({} docs, p50={:.2}ms, RSS≈{:.0}MB)",
            r.name, r.index_kind, r.n_docs, r.latency.p50, r.rss_after_build_mb
        );
    }
    println!("══════════════════════════════════════════════════════════════════════\n");
}

/// Entry point: load LoCoMo, embed once, profile the three heads.
///
/// # Errors
/// Returns an error if the dataset, model, or any DB operation fails.
#[tokio::main]
async fn main() -> Result<()> {
    let json_path = PathBuf::from(std::env::var("LOCOMO_JSON").unwrap_or_else(|_| {
        let home = std::env::var("HOME").unwrap_or_default();
        format!("{home}/.cache/uniko-bench/datasets/locomo/locomo10.json")
    }));
    let num_convs: usize = std::env::var("LOCOMO_NUM_CONVS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    let k: usize = std::env::var("PROFILE_K")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let scratch = PathBuf::from(std::env::var("LOCOMO_PROFILE_DIR").unwrap_or_else(|_| {
        std::env::temp_dir()
            .join("locomo_profile")
            .display()
            .to_string()
    }));

    let corpus = load_locomo(&json_path, num_convs)?;
    println!(
        "Loaded {} turns + {} questions from {} LoCoMo conversations ({})",
        corpus.turns.len(),
        corpus.questions.len(),
        num_convs,
        json_path.display()
    );

    println!(
        "Building BGE-M3 hybrid runtime (model: {}) ...",
        model_dir()
    );
    let runtime: std::sync::Arc<ModelRuntime> = ModelRuntime::builder()
        .register_provider(LocalOnnxProvider::new())
        .catalog(vec![spec()])
        .build()
        .await
        .map_err(|e| anyhow!("build bge-m3 runtime: {e}"))?;
    let embedder = runtime
        .hybrid_embedder(ALIAS)
        .await
        .map_err(|e| anyhow!("resolve hybrid embedder: {e}"))?;

    println!(
        "Embedding {} turns (dense+sparse+multivector) ...",
        corpus.turns.len()
    );
    let doc_emb = embed_heads(embedder.as_ref(), &corpus.turns).await?;
    println!(
        "  done in {:.1}s ({:.1} turns/s)",
        doc_emb.seconds,
        corpus.turns.len() as f64 / doc_emb.seconds
    );
    println!("Embedding {} questions ...", corpus.questions.len());
    let q_emb = embed_heads(embedder.as_ref(), &corpus.questions).await?;
    println!("  done in {:.1}s", q_emb.seconds);

    println!("\nProfiling dense head (HNSW) ...");
    let dense = profile_dense(&scratch.join("dense"), &doc_emb.dense, &q_emb.dense, k).await?;
    println!("Profiling sparse head (inverted) ...");
    let sparse = profile_sparse(&scratch.join("sparse"), &doc_emb.sparse, &q_emb.sparse, k).await?;
    println!("Profiling multivector head (MUVERA) ...");
    let multi = profile_multi(&scratch.join("multi"), &doc_emb.multi, &q_emb.multi, k).await?;

    print_report(
        num_convs,
        corpus.turns.len(),
        corpus.questions.len(),
        doc_emb.seconds,
        q_emb.seconds,
        &[dense, sparse, multi],
    );
    Ok(())
}
