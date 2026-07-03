//! LoCoMo hybrid-retrieval demo: `similar_to()` and `uni.search` over BGE-M3.
//!
//! Unlike `locomo_profile.rs` — which profiles each BGE-M3 head in its *own*
//! single-index database — this example loads all heads onto **one** `Doc`
//! node so the two hybrid idioms uni exposes can be exercised side by side:
//!
//! - `similar_to(source, query)` / `similar_to([sources], [queries], opts)` —
//!   a per-row scalar usable in `RETURN` / `WHERE` / `ORDER BY`. It scores each
//!   already-bound node (a full label scan here), so it is the right tool once a
//!   `MATCH` (or graph traversal) has produced the candidate set.
//! - `CALL uni.search(label, {vector, fts, sparse}, text, vec, k, filter, opts)`
//!   — a top-K procedure that runs each arm's index, then fuses the ranked lists
//!   with Reciprocal Rank Fusion (RRF) or weighted blending.
//!
//! Each LoCoMo dialogue turn becomes a `Doc` carrying four retrieval signals:
//! a dense BGE-M3 vector (HNSW index), the raw turn text (BM25 FULLTEXT index),
//! the learned-sparse SPLADE weights (sparse index), and the ColBERT token
//! vectors (MUVERA/FDE index). The demo runs five modes per question — dense-only
//! `similar_to`, two-way dense+FTS `uni.search`, three-way dense+FTS+sparse
//! `uni.search`, standalone multivector `uni.vector.query` (MUVERA + MaxSim), and
//! dense+FTS `uni.search` with a ColBERT MaxSim rerank — printing the top hits
//! and then a per-mode latency/QPS table. Note `similar_to` has no multivector
//! mode: late interaction is only reachable as the `reranker: 'maxsim'` stage.
//!
//! # Examples
//! ```text
//! cargo run --release --example locomo_hybrid --features provider-onnx
//! ```
//!
//! Environment overrides:
//! - `LOCOMO_JSON` — path to `locomo10.json`
//!   (default `~/.cache/uniko-bench/datasets/locomo/locomo10.json`).
//! - `LOCOMO_NUM_CONVS` — conversations pooled into one corpus (default `3`).
//! - `HYBRID_K` — top-k returned per query (default `5`).
//! - `HYBRID_NUM_QUERIES` — LoCoMo questions to display (default `5`).
//! - `LOCOMO_HYBRID_DIR` — scratch dir for the database.
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

/// BGE-M3 dense embedding dimension.
const DIM: usize = 1024;
/// XLM-RoBERTa vocabulary size = learned-sparse dimensionality.
const VOCAB: usize = 250_002;
/// Catalog alias bound to the hybrid `EmbedHybrid` model.
const ALIAS: &str = "hybrid/bge-m3";
/// Batch size for embedding passes (turns and questions).
const EMBED_BATCH: usize = 32;
/// Docs per write transaction; bounds commit size for the sparse/vector writes.
const INSERT_BATCH: usize = 200;
/// Max characters of a turn shown per result line (keeps output readable).
const SNIPPET: usize = 90;

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

/// Dense, learned-sparse and multivector heads for a batch of texts.
struct Embeddings {
    dense: Vec<Vec<f32>>,
    sparse: Vec<Vec<(u32, f32)>>,
    multi: Vec<Vec<Vec<f32>>>,
}

/// Embeds `texts` producing the dense, sparse and multivector heads per pass.
///
/// # Errors
/// Returns an error if the model omits any head or inference fails.
async fn embed(embedder: &dyn HybridEmbeddingModel, texts: &[String]) -> Result<Embeddings> {
    let heads = HeadSet::DENSE | HeadSet::SPARSE | HeadSet::MULTI_VECTOR;
    let mut dense = Vec::with_capacity(texts.len());
    let mut sparse = Vec::with_capacity(texts.len());
    let mut multi = Vec::with_capacity(texts.len());
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
        dense,
        sparse,
        multi,
    })
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

/// Truncates a turn to a single readable line for result printing.
fn snippet(text: &str) -> String {
    let one_line = text.replace('\n', " ");
    if one_line.chars().count() <= SNIPPET {
        return one_line;
    }
    let cut: String = one_line.chars().take(SNIPPET).collect();
    format!("{cut}…")
}

/// Opens a fresh (wiped) database with a widened commit window.
///
/// # Errors
/// Returns an error if the directory cannot be reset or the DB cannot open.
async fn fresh_db(path: &Path) -> Result<Uni> {
    if path.exists() {
        std::fs::remove_dir_all(path).ok();
    }
    std::fs::create_dir_all(path)?;
    // Sparse + dense writes per doc stay well under 5s, but a wide window keeps
    // batched commits robust across machines.
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

/// Declares the unified `Doc` schema: dense HNSW + FULLTEXT + sparse + MUVERA.
///
/// # Errors
/// Returns an error if the schema cannot be applied.
async fn create_schema(db: &Uni) -> Result<()> {
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("text", DataType::String)
        .property("emb", DataType::Vector { dimensions: DIM })
        .property("sparse", DataType::SparseVector { dimensions: VOCAB })
        .property(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
        )
        // Dense arm: HNSW over the BGE-M3 dense head.
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
        // Lexical arm: BM25 full-text over the raw turn text.
        .index("text", IndexType::FullText)
        // Learned-sparse arm: inverted index over the SPLADE weights.
        .index(
            "sparse",
            IndexType::Sparse {
                dimensions: VOCAB,
                quantize: false,
                embedding: None,
            },
        )
        // Multivector arm: MUVERA (FDE) index over ColBERT token vectors.
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
    Ok(())
}

/// Inserts each turn as a `Doc` carrying dense, text, sparse and token signals.
///
/// # Errors
/// Returns an error if any write or commit fails.
async fn ingest(db: &Uni, corpus: &Corpus, emb: &Embeddings) -> Result<()> {
    let mut tx = db.session().tx().await?;
    for (i, turn) in corpus.turns.iter().enumerate() {
        let (indices, values) = norm_sparse(&emb.sparse[i]);
        tx.execute_with(
            "CREATE (:Doc {title: $title, text: $text, emb: $emb, \
                           sparse: $sparse, tokens: $tokens})",
        )
        .param("title", Value::String(format!("t{i}")))
        .param("text", Value::String(turn.clone()))
        .param("emb", Value::Vector(emb.dense[i].clone()))
        .param("sparse", Value::SparseVector { indices, values })
        .param("tokens", mv_value(&emb.multi[i]))
        .run()
        .await?;
        if (i + 1) % INSERT_BATCH == 0 {
            tx.commit().await?;
            tx = db.session().tx().await?;
        }
    }
    tx.commit().await?;
    db.flush().await?;
    db.indexes().rebuild("Doc", false).await?;
    Ok(())
}

/// One retrieved hit: node title, snippet, fused score and per-arm components.
struct Hit {
    title: String,
    text: String,
    score: f64,
    vector_score: Option<f64>,
    fts_score: Option<f64>,
    sparse_score: Option<f64>,
}

/// Dense-only ranking via the per-row `similar_to` scalar (full label scan).
///
/// # Errors
/// Returns an error if the query fails.
async fn dense_similar_to(db: &Uni, qvec: &[f32], k: usize) -> Result<Vec<Hit>> {
    let cypher = "MATCH (d:Doc) \
                  RETURN d.title AS title, d.text AS text, \
                         similar_to(d.emb, $q) AS score \
                  ORDER BY score DESC LIMIT $k";
    let rows = db
        .session()
        .query_with(cypher)
        .param("q", Value::Vector(qvec.to_vec()))
        .param("k", Value::Int(k as i64))
        .fetch_all()
        .await?;
    Ok(rows
        .iter()
        .map(|r| Hit {
            title: r.get::<String>("title").unwrap_or_default(),
            text: r.get::<String>("text").unwrap_or_default(),
            score: r.get::<f64>("score").unwrap_or(0.0),
            vector_score: None,
            fts_score: None,
            sparse_score: None,
        })
        .collect())
}

/// Multivector (ColBERT) top-K via the MUVERA FDE index + MaxSim scoring.
///
/// # Errors
/// Returns an error if the query fails.
async fn multivec_query(db: &Uni, tokens: &[Vec<f32>], k: usize) -> Result<Vec<Hit>> {
    let cypher = "CALL uni.vector.query('Doc', 'tokens', $q, $k) \
                  YIELD node, score \
                  RETURN node.title AS title, node.text AS text, score \
                  ORDER BY score DESC";
    let rows = db
        .session()
        .query_with(cypher)
        .param("q", mv_value(tokens))
        .param("k", Value::Int(k as i64))
        .fetch_all()
        .await?;
    Ok(rows
        .iter()
        .map(|r| Hit {
            title: r.get::<String>("title").unwrap_or_default(),
            text: r.get::<String>("text").unwrap_or_default(),
            score: r.get::<f64>("score").unwrap_or(0.0),
            vector_score: None,
            fts_score: None,
            sparse_score: None,
        })
        .collect())
}

/// Hybrid retrieval (dense + FTS) followed by ColBERT MaxSim reranking.
///
/// `similar_to` has no multivector mode, so late interaction enters only as a
/// rerank stage: dense+FTS cheaply retrieve `k*3` candidates, then
/// `reranker: 'maxsim'` re-scores each by MaxSim over the `tokens` property.
/// With a reranker active, `uni.search`'s `score` equals the MaxSim
/// `rerank_score`.
///
/// # Errors
/// Returns an error if the query fails.
async fn hybrid_maxsim_rerank(
    db: &Uni,
    qtext: &str,
    qvec: &[f32],
    qtokens: &[Vec<f32>],
    k: usize,
) -> Result<Vec<Hit>> {
    let qvec_list = Value::List(qvec.iter().map(|&x| Value::Float(x as f64)).collect());
    let cypher = "CALL uni.search('Doc', {vector: 'emb', fts: 'text'}, $qtext, $qvec, $k, null, \
                  {reranker: 'maxsim', reranker_property: 'tokens', maxsim_query: $mvq}) \
                  YIELD node, score, rerank_score \
                  RETURN node.title AS title, node.text AS text, score \
                  ORDER BY score DESC";
    let rows = db
        .session()
        .query_with(cypher)
        .param("qtext", Value::String(qtext.to_string()))
        .param("qvec", qvec_list)
        .param("mvq", mv_value(qtokens))
        .param("k", Value::Int(k as i64))
        .fetch_all()
        .await?;
    Ok(rows
        .iter()
        .map(|r| Hit {
            title: r.get::<String>("title").unwrap_or_default(),
            text: r.get::<String>("text").unwrap_or_default(),
            score: r.get::<f64>("score").unwrap_or(0.0),
            vector_score: None,
            fts_score: None,
            sparse_score: None,
        })
        .collect())
}

/// Hybrid ranking via `uni.search`; three-way when `sparse_q` is provided.
///
/// Two-way fuses the dense (HNSW) and lexical (BM25 FULLTEXT) arms with RRF;
/// passing `sparse_q` additionally enables the learned-sparse arm.
///
/// # Errors
/// Returns an error if the query fails.
async fn hybrid_search(
    db: &Uni,
    qtext: &str,
    qvec: &[f32],
    sparse_q: Option<(Vec<u32>, Vec<f32>)>,
    k: usize,
) -> Result<Vec<Hit>> {
    // Dense query vector as a List<Float> literal (uni.search's vector arg).
    let qvec_list = Value::List(qvec.iter().map(|&x| Value::Float(x as f64)).collect());

    let (props, options, sparse_param) = match sparse_q {
        Some((indices, values)) => (
            "{vector: 'emb', fts: 'text', sparse: 'sparse'}",
            "{method: 'rrf', sparse_query: $sq}",
            Some(Value::SparseVector { indices, values }),
        ),
        None => ("{vector: 'emb', fts: 'text'}", "{method: 'rrf'}", None),
    };

    let cypher = format!(
        "CALL uni.search('Doc', {props}, $qtext, $qvec, $k, null, {options}) \
         YIELD node, score, vector_score, fts_score, sparse_score \
         RETURN node.title AS title, node.text AS text, \
                score, vector_score, fts_score, sparse_score \
         ORDER BY score DESC"
    );

    // Bind the session so the query builder's borrow outlives the `if` below
    // (a bare `db.session()` temporary would be dropped at end of statement).
    let session = db.session();
    let mut builder = session
        .query_with(&cypher)
        .param("qtext", Value::String(qtext.to_string()))
        .param("qvec", qvec_list)
        .param("k", Value::Int(k as i64));
    if let Some(sq) = sparse_param {
        builder = builder.param("sq", sq);
    }
    let rows = builder.fetch_all().await?;

    Ok(rows
        .iter()
        .map(|r| Hit {
            title: r.get::<String>("title").unwrap_or_default(),
            text: r.get::<String>("text").unwrap_or_default(),
            score: r.get::<f64>("score").unwrap_or(0.0),
            vector_score: r.get::<f64>("vector_score").ok(),
            fts_score: r.get::<f64>("fts_score").ok(),
            sparse_score: r.get::<f64>("sparse_score").ok(),
        })
        .collect())
}

/// Prints a labelled block of hits with per-arm score transparency.
fn print_hits(label: &str, hits: &[Hit]) {
    println!("  ── {label} ──");
    if hits.is_empty() {
        println!("     (no results)");
        return;
    }
    for (rank, h) in hits.iter().enumerate() {
        let mut parts = format!("fused={:.4}", h.score);
        if let Some(v) = h.vector_score {
            parts.push_str(&format!(" vec={v:.4}"));
        }
        if let Some(f) = h.fts_score {
            parts.push_str(&format!(" fts={f:.4}"));
        }
        if let Some(s) = h.sparse_score {
            parts.push_str(&format!(" sparse={s:.4}"));
        }
        println!(
            "   {:>2}. [{}] {}\n       {}",
            rank + 1,
            h.title,
            parts,
            snippet(&h.text),
        );
    }
}

/// Entry point: load LoCoMo, embed, build the hybrid index, run demo queries.
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
    let k: usize = std::env::var("HYBRID_K")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let num_queries: usize = std::env::var("HYBRID_NUM_QUERIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    // How many questions to time each mode over (default: all LoCoMo questions).
    let num_timing: usize = std::env::var("HYBRID_TIMING_QUERIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(usize::MAX);
    let scratch = PathBuf::from(std::env::var("LOCOMO_HYBRID_DIR").unwrap_or_else(|_| {
        std::env::temp_dir()
            .join("locomo_hybrid")
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
        "Embedding {} turns (dense + sparse) ...",
        corpus.turns.len()
    );
    let doc_emb = embed(embedder.as_ref(), &corpus.turns).await?;

    // Embed the questions we will time over; the first `num_queries` of these
    // are also the ones shown in the qualitative top-k display below.
    let n_time = num_timing.min(corpus.questions.len());
    let ask: Vec<String> = corpus.questions.iter().take(n_time).cloned().collect();
    let num_display = num_queries.min(ask.len());
    println!(
        "Embedding {} questions ({num_display} displayed, {n_time} timed) ...",
        ask.len()
    );
    let q_emb = embed(embedder.as_ref(), &ask).await?;

    println!("Building unified Doc index (dense HNSW + FULLTEXT + sparse + MUVERA) ...");
    let db = fresh_db(&scratch).await?;
    create_schema(&db).await?;
    ingest(&db, &corpus, &doc_emb).await?;

    println!("\n══════════════════════════════════════════════════════════════════════");
    println!(
        " LoCoMo hybrid retrieval — {} turns, top-{k} per query",
        corpus.turns.len()
    );
    println!("══════════════════════════════════════════════════════════════════════");

    for (i, question) in ask.iter().take(num_display).enumerate() {
        let qvec = &q_emb.dense[i];
        let (sq_idx, sq_val) = norm_sparse(&q_emb.sparse[i]);

        println!("\nQ{}: {question}", i + 1);

        let dense = dense_similar_to(&db, qvec, k).await?;
        print_hits("dense-only  similar_to(d.emb, $q)", &dense);

        let two_way = hybrid_search(&db, question, qvec, None, k).await?;
        print_hits("hybrid 2-way  uni.search {vector, fts} (RRF)", &two_way);

        let three_way = hybrid_search(&db, question, qvec, Some((sq_idx, sq_val)), k).await?;
        print_hits(
            "hybrid 3-way  uni.search {vector, fts, sparse} (RRF)",
            &three_way,
        );

        let multivec = multivec_query(&db, &q_emb.multi[i], k).await?;
        print_hits(
            "multivec  uni.vector.query tokens (MUVERA + MaxSim)",
            &multivec,
        );

        let maxsim = hybrid_maxsim_rerank(&db, question, qvec, &q_emb.multi[i], k).await?;
        print_hits(
            "hybrid+MaxSim  uni.search {vector, fts} + maxsim rerank",
            &maxsim,
        );
    }

    // ── Timing phase ──────────────────────────────────────────────────────
    // Each mode is timed sequentially over the same `n_time` questions on CPU;
    // these are single-threaded per-query latencies, not concurrent throughput.
    let timings = time_modes(&db, &ask, &q_emb, k, n_time).await?;
    print_timings(n_time, corpus.turns.len(), k, &timings);

    db.shutdown().await?;
    Ok(())
}

/// Per-mode latency + throughput measured over the same question set.
struct ModeTiming {
    name: &'static str,
    detail: &'static str,
    latency: Latency,
    qps: f64,
}

// ── Lean timing queries ───────────────────────────────────────────────────
// The display helpers above `YIELD node → RETURN node.title, node.text`, which
// materializes the full node — including the heavy `tokens` List(Vector) column
// (projection leak). For timing that isolates *retrieval/index* cost these lean
// variants return only `vid`/`score`, so no property loading is timed.

/// Times a dense-only `similar_to` scan returning only the per-row score.
///
/// # Errors
/// Returns an error if the query fails.
async fn t_dense(db: &Uni, qvec: &[f32], k: usize) -> Result<()> {
    db.session()
        .query_with("MATCH (d:Doc) RETURN similar_to(d.emb, $q) AS s ORDER BY s DESC LIMIT $k")
        .param("q", Value::Vector(qvec.to_vec()))
        .param("k", Value::Int(k as i64))
        .fetch_all()
        .await?;
    Ok(())
}

/// Times a hybrid `uni.search` returning only `vid`/`score` (no node load).
///
/// # Errors
/// Returns an error if the query fails.
async fn t_hybrid(
    db: &Uni,
    qtext: &str,
    qvec: &[f32],
    sparse_q: Option<(Vec<u32>, Vec<f32>)>,
    k: usize,
) -> Result<()> {
    let qvec_list = Value::List(qvec.iter().map(|&x| Value::Float(x as f64)).collect());
    let (props, options, sparse_param) = match sparse_q {
        Some((indices, values)) => (
            "{vector: 'emb', fts: 'text', sparse: 'sparse'}",
            "{method: 'rrf', sparse_query: $sq}",
            Some(Value::SparseVector { indices, values }),
        ),
        None => ("{vector: 'emb', fts: 'text'}", "{method: 'rrf'}", None),
    };
    let cypher = format!(
        "CALL uni.search('Doc', {props}, $qtext, $qvec, $k, null, {options}) \
         YIELD vid, score RETURN vid, score ORDER BY score DESC"
    );
    let session = db.session();
    let mut b = session
        .query_with(&cypher)
        .param("qtext", Value::String(qtext.to_string()))
        .param("qvec", qvec_list)
        .param("k", Value::Int(k as i64));
    if let Some(sq) = sparse_param {
        b = b.param("sq", sq);
    }
    b.fetch_all().await?;
    Ok(())
}

/// Times a MUVERA multivector `uni.vector.query` returning only `vid`/`score`.
///
/// # Errors
/// Returns an error if the query fails.
async fn t_multivec(db: &Uni, tokens: &[Vec<f32>], k: usize) -> Result<()> {
    db.session()
        .query_with(
            "CALL uni.vector.query('Doc', 'tokens', $q, $k) \
             YIELD vid, score RETURN vid, score ORDER BY score DESC",
        )
        .param("q", mv_value(tokens))
        .param("k", Value::Int(k as i64))
        .fetch_all()
        .await?;
    Ok(())
}

/// Times a dense+FTS `uni.search` with MaxSim rerank returning only `vid`/`score`.
///
/// # Errors
/// Returns an error if the query fails.
async fn t_maxsim(
    db: &Uni,
    qtext: &str,
    qvec: &[f32],
    qtokens: &[Vec<f32>],
    k: usize,
) -> Result<()> {
    let qvec_list = Value::List(qvec.iter().map(|&x| Value::Float(x as f64)).collect());
    db.session()
        .query_with(
            "CALL uni.search('Doc', {vector: 'emb', fts: 'text'}, $qtext, $qvec, $k, null, \
             {reranker: 'maxsim', reranker_property: 'tokens', maxsim_query: $mvq}) \
             YIELD vid, score RETURN vid, score ORDER BY score DESC",
        )
        .param("qtext", Value::String(qtext.to_string()))
        .param("qvec", qvec_list)
        .param("mvq", mv_value(qtokens))
        .param("k", Value::Int(k as i64))
        .fetch_all()
        .await?;
    Ok(())
}

/// Times all five retrieval modes over the first `n` questions, in order.
///
/// Uses the lean `YIELD vid` query variants so the timings isolate retrieval and
/// index cost from node/property materialization.
///
/// # Errors
/// Returns an error if any query fails.
async fn time_modes(
    db: &Uni,
    ask: &[String],
    q_emb: &Embeddings,
    k: usize,
    n: usize,
) -> Result<Vec<ModeTiming>> {
    // Dense-only similar_to (full label scan, per-row cosine).
    let mut dense_ms = Vec::with_capacity(n);
    let dense_start = Instant::now();
    for qd in q_emb.dense.iter().take(n) {
        let t = Instant::now();
        t_dense(db, qd, k).await?;
        dense_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    let dense_qps = n as f64 / dense_start.elapsed().as_secs_f64();

    // Hybrid 2-way uni.search (dense HNSW + BM25 FULLTEXT, RRF).
    let mut two_ms = Vec::with_capacity(n);
    let two_start = Instant::now();
    for (qtext, qd) in ask.iter().zip(&q_emb.dense).take(n) {
        let t = Instant::now();
        t_hybrid(db, qtext, qd, None, k).await?;
        two_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    let two_qps = n as f64 / two_start.elapsed().as_secs_f64();

    // Hybrid 3-way uni.search (+ learned-sparse arm, RRF).
    let mut three_ms = Vec::with_capacity(n);
    let three_start = Instant::now();
    for ((qtext, qd), qs) in ask.iter().zip(&q_emb.dense).zip(&q_emb.sparse).take(n) {
        let sq = norm_sparse(qs);
        let t = Instant::now();
        t_hybrid(db, qtext, qd, Some(sq), k).await?;
        three_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    let three_qps = n as f64 / three_start.elapsed().as_secs_f64();

    // Multivector (ColBERT) top-K via the MUVERA FDE index + MaxSim.
    let mut mv_ms = Vec::with_capacity(n);
    let mv_start = Instant::now();
    for qt in q_emb.multi.iter().take(n) {
        let t = Instant::now();
        t_multivec(db, qt, k).await?;
        mv_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    let mv_qps = n as f64 / mv_start.elapsed().as_secs_f64();

    // Hybrid retrieve (dense + FTS) then ColBERT MaxSim rerank of candidates.
    let mut rr_ms = Vec::with_capacity(n);
    let rr_start = Instant::now();
    for ((qtext, qd), qt) in ask.iter().zip(&q_emb.dense).zip(&q_emb.multi).take(n) {
        let t = Instant::now();
        t_maxsim(db, qtext, qd, qt, k).await?;
        rr_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    let rr_qps = n as f64 / rr_start.elapsed().as_secs_f64();

    Ok(vec![
        ModeTiming {
            name: "dense-only",
            detail: "similar_to(d.emb, $q) — full label scan, per-row cosine",
            latency: Latency::from_samples(dense_ms),
            qps: dense_qps,
        },
        ModeTiming {
            name: "hybrid 2-way",
            detail: "uni.search {vector, fts} RRF — HNSW + BM25",
            latency: Latency::from_samples(two_ms),
            qps: two_qps,
        },
        ModeTiming {
            name: "hybrid 3-way",
            detail: "uni.search {vector, fts, sparse} RRF — + learned-sparse",
            latency: Latency::from_samples(three_ms),
            qps: three_qps,
        },
        ModeTiming {
            name: "multivec",
            detail: "uni.vector.query tokens — MUVERA FDE + MaxSim, top-K",
            latency: Latency::from_samples(mv_ms),
            qps: mv_qps,
        },
        ModeTiming {
            name: "hybrid+maxsim",
            detail: "uni.search {vector, fts} + ColBERT MaxSim rerank of k*3",
            latency: Latency::from_samples(rr_ms),
            qps: rr_qps,
        },
    ])
}

/// Prints the per-mode query-latency comparison table.
fn print_timings(n_queries: usize, n_turns: usize, k: usize, modes: &[ModeTiming]) {
    println!("\n══════════════════════════════════════════════════════════════════════");
    println!(" Query timings — {n_queries} questions × top-{k} over {n_turns} turns");
    println!(" (single-threaded, sequential, CPU; YIELD vid — retrieval only,");
    println!("  query-embed + node/property materialization excluded)");
    println!("──────────────────────────────────────────────────────────────────────");
    println!(
        " {:<14} {:>9} {:>9} {:>9} {:>9} {:>8}",
        "mode", "mean(ms)", "p50(ms)", "p95(ms)", "p99(ms)", "QPS"
    );
    for m in modes {
        println!(
            " {:<14} {:>9.2} {:>9.2} {:>9.2} {:>9.2} {:>8.0}",
            m.name, m.latency.mean, m.latency.p50, m.latency.p95, m.latency.p99, m.qps,
        );
    }
    println!("──────────────────────────────────────────────────────────────────────");
    for m in modes {
        println!("   {:<14} {}", m.name, m.detail);
    }
    println!("══════════════════════════════════════════════════════════════════════\n");
}
