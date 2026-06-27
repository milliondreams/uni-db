// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Sparse-vector (SPLADE / learned-sparse) retrieval benchmark — issue #95 M5.
//!
//! This is the **P2 gate**: it measures end-to-end `uni.sparse.query` latency
//! (the P1 brute-force DAAT path) on a SPLADE-shaped synthetic corpus, so the
//! decision to build block-max pruning (P2) is driven by data rather than
//! speculation. The proposal gates P2 on P1 missing a latency target; this bench
//! produces that number.
//!
//! Why end-to-end and not candidate-gen-only: the live path uses the index purely
//! as a candidate generator and then re-scores every candidate exactly from the
//! lossless stored vector. The reported latency therefore already includes the
//! candidate scan, the L0 union, the property fetch, and the exact rescore — the
//! full thing a user pays for. Isolating the index scan would need internal hooks
//! not on the public API; the rescore + fetch typically dominate, which is itself
//! the finding that makes block-max pruning low-value here.
//!
//! The term distribution is deliberately skewed (mass concentrated at low term
//! ids) so a handful of terms get very long posting lists — the high-DF worst
//! case the proposal flags as what defeats classical skipping.
//!
//! # Running
//!
//! ```bash
//! # Defaults: 2k and 10k docs, quantized + lossless.
//! cargo bench -p uni-db --bench sparse_retrieval
//!
//! # Custom scales (comma-separated doc counts), e.g. add a 100k run:
//! SPARSE_BENCH_DOCS=2000,10000,100000 cargo bench -p uni-db --bench sparse_retrieval
//!
//! # Smoke test.
//! SPARSE_BENCH_DOCS=500 cargo bench -p uni-db --bench sparse_retrieval
//! ```

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::collections::BTreeMap;
use std::env;
use tokio::runtime::Runtime;
use uni_db::{DataType, IndexType, Uni, Value};

/// Term-space cardinality (SPLADE/BGE-M3 vocabularies are ~30k WordPiece tokens).
const VOCAB: usize = 30_000;
/// Non-zero terms per document (SPLADE doc expansion is typically ~100–200).
const DOC_NNZ: usize = 150;
/// Non-zero terms per query (SPLADE query side is sparser).
const QUERY_NNZ: usize = 30;
/// Top-k retrieved per query.
const K: usize = 10;

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
    /// Uniform `f32` in `[0, 1)`.
    fn unit(&mut self) -> f32 {
        (self.next_u64() >> 40) as f32 / (1u64 << 24) as f32
    }
    /// Positive weight in `[0.1, ~1.1)`, matching learned-sparse (ReLU) output.
    fn weight(&mut self) -> f32 {
        self.unit() + 0.1
    }
    /// Skewed term id: squaring concentrates mass at low ids, so those terms get
    /// long posting lists (the high-DF worst case for retrieval).
    fn skewed_term(&mut self) -> u32 {
        let u = self.unit();
        ((u * u * VOCAB as f32) as usize).min(VOCAB - 1) as u32
    }
}

/// A sparse vector as parallel sorted-unique `(indices, values)`.
type Sparse = (Vec<u32>, Vec<f32>);

/// Draw a sorted-unique sparse vector of `nnz` skewed terms.
fn random_sparse(rng: &mut Rng, nnz: usize) -> Sparse {
    let mut m: BTreeMap<u32, f32> = BTreeMap::new();
    while m.len() < nnz {
        let t = rng.skewed_term();
        let w = rng.weight();
        m.insert(t, w);
    }
    (m.keys().copied().collect(), m.values().copied().collect())
}

fn sv_value((indices, values): &Sparse) -> Value {
    Value::SparseVector {
        indices: indices.clone(),
        values: values.clone(),
    }
}

fn sparse_dot_oracle(q: &Sparse, d: &Sparse) -> f64 {
    let qm: std::collections::HashMap<u32, f64> =
        q.0.iter().zip(&q.1).map(|(&t, &w)| (t, w as f64)).collect();
    d.0.iter()
        .zip(&d.1)
        .filter_map(|(&t, &w)| qm.get(&t).map(|qw| qw * w as f64))
        .sum()
}

/// Doc counts to benchmark (env `SPARSE_BENCH_DOCS`, comma-separated).
fn scales() -> Vec<usize> {
    match env::var("SPARSE_BENCH_DOCS") {
        Ok(s) => s
            .split(',')
            .filter_map(|t| t.trim().parse::<usize>().ok())
            .filter(|n| *n > 0)
            .collect(),
        Err(_) => vec![2_000, 10_000],
    }
}

async fn setup_db(n: usize, quantize: bool) -> anyhow::Result<(Uni, Vec<Sparse>)> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("emb", DataType::SparseVector { dimensions: VOCAB })
        .index(
            "emb",
            IndexType::Sparse {
                dimensions: VOCAB,
                quantize,
                embedding: None,
            },
        )
        .apply()
        .await?;

    let mut rng = Rng(0x0BAD_5EED ^ n as u64);
    let mut corpus = Vec::with_capacity(n);
    let tx = db.session().tx().await?;
    for i in 0..n {
        let doc = random_sparse(&mut rng, DOC_NNZ);
        tx.execute_with("CREATE (:Doc {title: $title, emb: $emb})")
            .param("title", Value::String(format!("d{i}")))
            .param("emb", sv_value(&doc))
            .run()
            .await?;
        corpus.push(doc);
    }
    tx.commit().await?;
    db.flush().await?;
    Ok((db, corpus))
}

async fn run_query(db: &Uni, query: &Sparse) -> anyhow::Result<Vec<String>> {
    let rows = db
        .session()
        .query_with(
            "CALL uni.sparse.query('Doc', 'emb', $q, $k, null, null, {}) \
             YIELD node, score RETURN node.title AS title",
        )
        .param("q", sv_value(query))
        .param("k", Value::Int(K as i64))
        .fetch_all()
        .await?;
    Ok(rows
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect())
}

/// Recall@K of the engine's titles against the brute-force oracle top-K.
fn recall_at_k(engine: &[String], query: &Sparse, corpus: &[Sparse]) -> f64 {
    let mut scored: Vec<(usize, f64)> = corpus
        .iter()
        .enumerate()
        .map(|(i, d)| (i, sparse_dot_oracle(query, d)))
        .collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    let oracle_top: std::collections::HashSet<String> = scored
        .iter()
        .take(K)
        .filter(|(_, s)| *s > 0.0)
        .map(|(i, _)| format!("d{i}"))
        .collect();
    if oracle_top.is_empty() {
        return 1.0;
    }
    let hit = engine.iter().filter(|t| oracle_top.contains(*t)).count();
    hit as f64 / oracle_top.len() as f64
}

fn bench_sparse_retrieval(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // A fixed query mixing hot (low-id, long-posting) terms with random ones.
    let query: Sparse = {
        let mut rng = Rng(0x5EED_0095);
        let mut m: BTreeMap<u32, f32> = BTreeMap::new();
        while m.len() < QUERY_NNZ {
            m.insert(rng.skewed_term(), rng.weight());
        }
        (m.keys().copied().collect(), m.values().copied().collect())
    };

    let mut group = c.benchmark_group("sparse_retrieval");
    for &n in &scales() {
        for quantize in [true, false] {
            let (db, corpus) = rt.block_on(setup_db(n, quantize)).unwrap();
            let kind = if quantize { "int8" } else { "f32" };

            // Recall is fidelity context, reported outside the timed loop.
            let titles = rt.block_on(run_query(&db, &query)).unwrap();
            let recall = recall_at_k(&titles, &query, &corpus);
            println!("[sparse_retrieval] docs={n} weights={kind} recall@{K}={recall:.3}");

            group.bench_with_input(BenchmarkId::new(kind, n), &n, |b, _| {
                b.iter(|| {
                    let titles = rt.block_on(run_query(&db, &query)).unwrap();
                    assert!(!titles.is_empty());
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_sparse_retrieval);
criterion_main!(benches);
