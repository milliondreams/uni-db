// Rust guideline compliant
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Dense-vector (KNN / ANN) retrieval benchmark — parity with the sparse
//! (`sparse_retrieval.rs`) and multi-vector (`multivec_retrieval.rs`) benches.
//!
//! This closes the dense-side scale gap: before this, the largest dense corpus
//! anywhere in the suite was ~60 vectors (the metamorphic oracle), and the only
//! dense bench (`comprehensive.rs::bench_vector_index`) timed HNSW *latency* with
//! no recall measurement. Here we measure end-to-end `uni.vector.query` latency
//! AND `recall@k` against a brute-force cosine oracle, at an env-tunable corpus
//! size, on the real engine path (Lance scan + L0 union + exact rescore).
//!
//! Per scale we measure an exact `flat` cell (recall ≈1.0, a fidelity/latency
//! floor) and an `hnsw` index swept across `ef_search` beam widths (15 ≈ Lance's
//! default, 100, 200). Recall climbs toward the floor as the beam widens, which
//! both surfaces the previously unmeasured ANN recall AND demonstrates the
//! query-time `ef_search` knob newly exposed on `uni.vector.query`.
//!
//! # Running
//!
//! ```bash
//! # Defaults: 2k and 10k docs, flat + hnsw.
//! cargo bench -p uni-db --bench dense_retrieval
//!
//! # Custom scales (comma-separated doc counts), e.g. add a 100k run:
//! DENSE_BENCH_DOCS=2000,10000,100000 cargo bench -p uni-db --bench dense_retrieval
//!
//! # Smoke test.
//! DENSE_BENCH_DOCS=500 cargo bench -p uni-db --bench dense_retrieval
//! ```

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::collections::HashSet;
use std::env;
use tokio::runtime::Runtime;
use uni_db::{DataType, IndexType, Uni, Value, VectorAlgo, VectorIndexCfg, VectorMetric};

/// Embedding dimensionality (typical sentence-embedding size band).
const DIM: usize = 128;
/// Top-k retrieved per query.
const K: usize = 10;

/// Deterministic xorshift PRNG so corpora are reproducible across runs (random
/// nondeterminism would make recall numbers unstable between invocations).
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
    /// A pseudo-random `f32` component in `[-1, 1)`.
    fn component(&mut self) -> f32 {
        (self.next_u64() >> 40) as f32 / (1u64 << 23) as f32 - 1.0
    }
}

/// A dense embedding vector.
type Dense = Vec<f32>;

/// Draw a `DIM`-dimensional vector with components in `[-1, 1)`.
fn random_dense(rng: &mut Rng) -> Dense {
    (0..DIM).map(|_| rng.component()).collect()
}

/// Brute-force cosine score in f64: `(1 + cos(q, d)) / 2`, matching the engine.
fn cosine_score_oracle(q: &Dense, d: &Dense) -> f64 {
    let dot: f64 = q.iter().zip(d).map(|(&x, &y)| x as f64 * y as f64).sum();
    let nq = q.iter().map(|&x| (x as f64).powi(2)).sum::<f64>().sqrt();
    let nd = d.iter().map(|&x| (x as f64).powi(2)).sum::<f64>().sqrt();
    let cos = if nq == 0.0 || nd == 0.0 {
        0.0
    } else {
        dot / (nq * nd)
    };
    (1.0 + cos) / 2.0
}

/// Doc counts to benchmark (env `DENSE_BENCH_DOCS`, comma-separated).
fn scales() -> Vec<usize> {
    match env::var("DENSE_BENCH_DOCS") {
        Ok(s) => s
            .split(',')
            .filter_map(|t| t.trim().parse::<usize>().ok())
            .filter(|n| *n > 0)
            .collect(),
        Err(_) => vec![2_000, 10_000],
    }
}

/// The index algorithm under test for a given sweep cell.
fn algo(kind: &str) -> VectorAlgo {
    match kind {
        "flat" => VectorAlgo::Flat,
        "hnsw" => VectorAlgo::Hnsw {
            m: 16,
            ef_construction: 100,
            partitions: None,
        },
        other => unreachable!("unknown dense index kind: {other}"),
    }
}

/// Builds a flushed, indexed corpus of `n` random dense vectors.
///
/// # Errors
/// Returns an error if schema application, ingest, flush, or index rebuild fails.
async fn setup_db(n: usize, kind: &str) -> anyhow::Result<(Uni, Vec<Dense>)> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("emb", DataType::Vector { dimensions: DIM })
        .index(
            "emb",
            IndexType::Vector(VectorIndexCfg {
                algorithm: algo(kind),
                metric: VectorMetric::Cosine,
                embedding: None,
            }),
        )
        .apply()
        .await?;

    let mut rng = Rng(0x0BAD_5EED ^ n as u64);
    let mut corpus = Vec::with_capacity(n);
    let tx = db.session().tx().await?;
    for i in 0..n {
        let v = random_dense(&mut rng);
        tx.execute_with("CREATE (:Doc {title: $title, emb: $emb})")
            .param("title", Value::String(format!("d{i}")))
            .param("emb", Value::Vector(v.clone()))
            .run()
            .await?;
        corpus.push(v);
    }
    tx.commit().await?;
    db.flush().await?;
    // Force the ANN structure to be (re)built over the full flushed corpus so the
    // measured query exercises the index, not a residual brute-force fallback.
    db.indexes().rebuild("Doc", false).await?;
    Ok((db, corpus))
}

/// Runs one top-`K` `uni.vector.query` with the given options literal (e.g.
/// `{ef_search: 200}`) and returns the result titles.
///
/// # Errors
/// Returns an error if the query fails to execute or decode.
async fn run_query(db: &Uni, query: &Dense, options: &str) -> anyhow::Result<Vec<String>> {
    let cypher = format!(
        "CALL uni.vector.query('Doc', 'emb', $q, $k, null, null, {options}) \
         YIELD node, score RETURN node.title AS title"
    );
    let rows = db
        .session()
        .query_with(&cypher)
        .param("q", Value::Vector(query.clone()))
        .param("k", Value::Int(K as i64))
        .fetch_all()
        .await?;
    Ok(rows
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect())
}

/// Recall@K of the engine's titles against the brute-force oracle top-K.
fn recall_at_k(engine: &[String], query: &Dense, corpus: &[Dense]) -> f64 {
    let mut scored: Vec<(usize, f64)> = corpus
        .iter()
        .enumerate()
        .map(|(i, d)| (i, cosine_score_oracle(query, d)))
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

/// HNSW search-time beam widths (`ef_search`) to sweep. `15` ≈ Lance's `1.5×k`
/// default for `k=10`; larger beams trade latency for recall.
const EF_SWEEP: [usize; 3] = [15, 100, 200];

/// Criterion entry point. For each scale: an exact `flat` cell (recall floor
/// ≈1.0) plus an `hnsw` index queried across [`EF_SWEEP`] beam widths, so the
/// output is a recall/latency curve showing the `ef_search` knob at work. Recall@K
/// is printed outside the timed loop; the timed loop measures query latency.
fn bench_dense_retrieval(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // A fixed, deterministic query vector (distinct seed from the corpus).
    let query: Dense = {
        let mut rng = Rng(0x5EED_0DE5);
        random_dense(&mut rng)
    };

    let mut group = c.benchmark_group("dense_retrieval");
    for &n in &scales() {
        // Exact baseline: recall ≈1.0, the fidelity/latency floor.
        let (flat_db, corpus) = rt.block_on(setup_db(n, "flat")).unwrap();
        let titles = rt.block_on(run_query(&flat_db, &query, "{}")).unwrap();
        let recall = recall_at_k(&titles, &query, &corpus);
        println!("[dense_retrieval] docs={n} index=flat recall@{K}={recall:.3}");
        group.bench_with_input(BenchmarkId::new("flat", n), &n, |b, _| {
            b.iter(|| {
                let titles = rt.block_on(run_query(&flat_db, &query, "{}")).unwrap();
                assert!(!titles.is_empty());
            });
        });

        // Approximate HNSW: build once, sweep ef_search at query time.
        let (hnsw_db, _) = rt.block_on(setup_db(n, "hnsw")).unwrap();
        for ef in EF_SWEEP {
            let opts = format!("{{ef_search: {ef}}}");
            let titles = rt.block_on(run_query(&hnsw_db, &query, &opts)).unwrap();
            let recall = recall_at_k(&titles, &query, &corpus);
            println!("[dense_retrieval] docs={n} index=hnsw ef_search={ef} recall@{K}={recall:.3}");
            let id = format!("hnsw_ef{ef}");
            group.bench_with_input(BenchmarkId::new(id, n), &n, |b, _| {
                b.iter(|| {
                    let titles = rt.block_on(run_query(&hnsw_db, &query, &opts)).unwrap();
                    assert!(!titles.is_empty());
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_dense_retrieval);
criterion_main!(benches);
