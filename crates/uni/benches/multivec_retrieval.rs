// Rust guideline compliant
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Multi-vector (ColBERT / MaxSim) retrieval benchmark — parity with the sparse
//! (`sparse_retrieval.rs`) and dense (`dense_retrieval.rs`) benches.
//!
//! Promotes the recall measurement that previously lived only in the
//! `uni-store/examples/multivec_recall_*` example binaries (not run by
//! `cargo test`/`bench`, and one needing an external corpus) into a first-class,
//! CI-runnable criterion bench on the real engine path. It measures end-to-end
//! `uni.vector.query` latency AND `recall@k` against a brute-force cosine-MaxSim
//! oracle at an env-tunable corpus size.
//!
//! Two variants are swept per scale: `flat` (no index → exact brute-force MaxSim
//! rerank; recall ≈1.0, a fidelity/latency floor) and `ivf_pq` (native
//! multi-vector IVF_PQ first stage + exact rescore; recall < 1.0 is the
//! approximate number that gates building a native index).
//!
//! Corpus size is in *documents*; each doc carries 2..=6 token vectors, so the
//! number of indexed token vectors is ~4× the doc count.
//!
//! # Running
//!
//! ```bash
//! # Defaults: 1k and 5k docs, flat + ivf_pq.
//! cargo bench -p uni-db --bench multivec_retrieval
//!
//! # Custom scales (comma-separated doc counts).
//! MULTIVEC_BENCH_DOCS=1000,5000,20000 cargo bench -p uni-db --bench multivec_retrieval
//!
//! # Smoke test.
//! MULTIVEC_BENCH_DOCS=300 cargo bench -p uni-db --bench multivec_retrieval
//! ```

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::collections::HashSet;
use std::env;
use tokio::runtime::Runtime;
use uni_db::{DataType, Uni, Value};

/// Token-vector dimensionality (IVF_PQ `sub_vectors` must divide this).
const DIM: usize = 64;
/// Product-quantization sub-vectors (64 / 8 = 8 dims per sub-vector).
const SUB_VECTORS: u32 = 8;
/// Query tokens per probe.
const N_QUERY_TOKENS: usize = 4;
/// Top-k retrieved per query.
const K: usize = 10;

/// Deterministic xorshift PRNG so corpora are reproducible across runs.
struct Rng(u64);
impl Rng {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    /// A unit-norm token vector of dimension `DIM` (cosine cares only about
    /// direction, but normalizing keeps scores in a tidy range).
    fn unit_vector(&mut self) -> Vec<f32> {
        let mut v: Vec<f32> = (0..DIM)
            .map(|_| ((self.next_u64() >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0)
            .collect();
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
        for x in &mut v {
            *x /= norm;
        }
        v
    }
}

/// A document's token vectors.
type Tokens = Vec<Vec<f32>>;

/// Encodes a multi-vector as a `Value::List(Vec<Value::List<Float>>)`.
fn to_value(tokens: &Tokens) -> Value {
    Value::List(
        tokens
            .iter()
            .map(|t| Value::List(t.iter().map(|&x| Value::Float(x as f64)).collect()))
            .collect(),
    )
}

/// Brute-force cosine-MaxSim ground truth in f64: `Σ_q max_d cos(q, d)`.
fn maxsim_oracle(query: &Tokens, doc: &Tokens) -> f64 {
    let cos = |a: &[f32], b: &[f32]| -> f64 {
        let dot: f64 = a.iter().zip(b).map(|(&x, &y)| x as f64 * y as f64).sum();
        let na = a.iter().map(|&x| (x as f64).powi(2)).sum::<f64>().sqrt();
        let nb = b.iter().map(|&x| (x as f64).powi(2)).sum::<f64>().sqrt();
        if na == 0.0 || nb == 0.0 {
            0.0
        } else {
            dot / (na * nb)
        }
    };
    query
        .iter()
        .map(|q| {
            doc.iter()
                .map(|d| cos(q, d))
                .fold(f64::NEG_INFINITY, f64::max)
        })
        .sum()
}

/// Doc counts to benchmark (env `MULTIVEC_BENCH_DOCS`, comma-separated).
fn scales() -> Vec<usize> {
    match env::var("MULTIVEC_BENCH_DOCS") {
        Ok(s) => s
            .split(',')
            .filter_map(|t| t.trim().parse::<usize>().ok())
            .filter(|n| *n > 0)
            .collect(),
        Err(_) => vec![1_000, 5_000],
    }
}

/// Builds a flushed corpus of `n` docs (2..=6 token vectors each) and returns it
/// with the open database. The `ivf_pq` variant additionally creates and rebuilds
/// the native multi-vector index; `flat` leaves the column unindexed (brute force).
///
/// # Errors
/// Returns an error if schema application, ingest, flush, or index build fails.
async fn setup_db(n: usize, kind: &str) -> anyhow::Result<(Uni, Vec<Tokens>)> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
        )
        .apply()
        .await?;

    let mut rng = Rng(0x0BAD_5EED ^ n as u64);
    let mut corpus = Vec::with_capacity(n);
    let tx = db.session().tx().await?;
    for i in 0..n {
        let n_tokens = 2 + (rng.next_u64() % 5) as usize; // 2..=6
        let toks: Tokens = (0..n_tokens).map(|_| rng.unit_vector()).collect();
        tx.execute_with("CREATE (:Doc {title: $title, tokens: $toks})")
            .param("title", Value::String(format!("d{i}")))
            .param("toks", to_value(&toks))
            .run()
            .await?;
        corpus.push(toks);
    }
    tx.commit().await?;
    db.flush().await?;

    if kind == "ivf_pq" {
        // IVF partition count scaled to corpus size (heuristic ≈ √rows), kept in a
        // range Lance can train (needs ≥256 training vectors per the PQ codebook).
        let partitions = ((n as f64).sqrt() as u32).clamp(4, 256);
        let tx = db.session().tx().await?;
        tx.execute(&format!(
            "CREATE VECTOR INDEX tok_idx FOR (d:Doc) ON (d.tokens) \
             OPTIONS {{type: 'ivf_pq', partitions: {partitions}, \
             sub_vectors: {SUB_VECTORS}, num_bits: 8, metric: 'cosine'}}"
        ))
        .await?;
        tx.commit().await?;
        db.indexes().rebuild("Doc", false).await?;
    }
    Ok((db, corpus))
}

/// Query options literal for a sweep cell: empty for brute force; a partition
/// probe + refine for the approximate IVF_PQ first stage.
fn query_options(kind: &str, n: usize) -> String {
    match kind {
        "flat" => "{}".to_string(),
        "ivf_pq" => {
            let partitions = ((n as f64).sqrt() as u32).clamp(4, 256);
            // Probe a quarter of the partitions (a realistic ANN operating point),
            // then re-rank a wider candidate set exactly via `refine_factor`.
            let nprobes = (partitions / 4).max(1);
            format!("{{nprobes: {nprobes}, refine_factor: 16}}")
        }
        other => unreachable!("unknown multi-vector index kind: {other}"),
    }
}

/// Runs one top-`K` `uni.vector.query` over the `tokens` column, returning titles.
///
/// # Errors
/// Returns an error if the query fails to execute or decode.
async fn run_query(db: &Uni, query: &Tokens, options: &str) -> anyhow::Result<Vec<String>> {
    let cypher = format!(
        "CALL uni.vector.query('Doc', 'tokens', $q, $k, null, null, {options}) \
         YIELD node, score RETURN node.title AS title"
    );
    let rows = db
        .session()
        .query_with(&cypher)
        .param("q", to_value(query))
        .param("k", Value::Int(K as i64))
        .fetch_all()
        .await?;
    Ok(rows
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect())
}

/// Recall@K of the engine's titles against the brute-force MaxSim oracle top-K.
fn recall_at_k(engine: &[String], query: &Tokens, corpus: &[Tokens]) -> f64 {
    let mut scored: Vec<(usize, f64)> = corpus
        .iter()
        .enumerate()
        .map(|(i, d)| (i, maxsim_oracle(query, d)))
        .collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    let oracle_top: HashSet<String> = scored
        .iter()
        .take(K)
        .map(|(i, _)| format!("d{i}"))
        .collect();
    if oracle_top.is_empty() {
        return 1.0;
    }
    let hit = engine.iter().filter(|t| oracle_top.contains(*t)).count();
    hit as f64 / oracle_top.len() as f64
}

/// Criterion entry point: sweeps `{flat, ivf_pq} × scales()`, printing recall@K
/// (outside the timed loop) and timing end-to-end query latency.
fn bench_multivec_retrieval(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // A fixed, deterministic query multi-vector (distinct seed from the corpus).
    let query: Tokens = {
        let mut rng = Rng(0x5EED_0DE5);
        (0..N_QUERY_TOKENS).map(|_| rng.unit_vector()).collect()
    };

    let mut group = c.benchmark_group("multivec_retrieval");
    for &n in &scales() {
        for kind in ["flat", "ivf_pq"] {
            let (db, corpus) = rt.block_on(setup_db(n, kind)).unwrap();
            let opts = query_options(kind, n);

            // Recall is fidelity context, reported outside the timed loop.
            let titles = rt.block_on(run_query(&db, &query, &opts)).unwrap();
            let recall = recall_at_k(&titles, &query, &corpus);
            println!("[multivec_retrieval] docs={n} index={kind} recall@{K}={recall:.3}");

            group.bench_with_input(BenchmarkId::new(kind, n), &n, |b, _| {
                b.iter(|| {
                    let titles = rt.block_on(run_query(&db, &query, &opts)).unwrap();
                    assert!(!titles.is_empty());
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_multivec_retrieval);
criterion_main!(benches);
