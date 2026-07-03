// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Recall@k correctness tests for every quantized vector index mode.
//!
//! The `vector_index_options_test.rs` matrix asserts each algorithm *builds and returns rows*;
//! it does NOT check that the returned neighbors are actually the nearest ones. These tests close
//! that gap: they retrieve top-k from the real engine path and compare against a brute-force
//! oracle, asserting `recall@k` clears a conservative floor — enough to catch gross regressions
//! (a broken quantizer, a mis-wired distance metric) without being flaky.
//!
//! All tests are `#[ignore]` by default: they build real ANN indexes over multi-thousand-vector
//! corpora and are slower / more param-sensitive than the rest of the suite. Run explicitly with
//! `cargo nextest run -p uni-db --run-ignored all vector_recall` (or `cargo test -- --ignored`).
//!
//! Helpers (`Rng`, oracles, `recall_at_k`) mirror the `dense_retrieval` / `multivec_retrieval`
//! benches so the test path and bench path measure recall the same way.

use std::collections::HashSet;

use uni_db::{DataType, IndexType, Uni, Value, VectorAlgo, VectorIndexCfg, VectorMetric};

/// Embedding dimensionality. Divisible by every PQ `sub_vectors` count used below.
const DIM: usize = 64;
/// Top-k retrieved per query.
const K: usize = 10;
/// Corpus size — large enough to train IVF/PQ codebooks, small enough to stay fast.
const N: usize = 3_000;

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
    /// A pseudo-random `f32` component in `[-1, 1)`.
    fn component(&mut self) -> f32 {
        (self.next_u64() >> 40) as f32 / (1u64 << 23) as f32 - 1.0
    }
}

type Dense = Vec<f32>;

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

/// Recall@K of the engine's titles against the brute-force cosine oracle top-K.
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

/// Build a flushed, indexed corpus of `N` random dense vectors under `algo`.
async fn setup(algo: VectorAlgo) -> anyhow::Result<(Uni, Vec<Dense>)> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("emb", DataType::Vector { dimensions: DIM })
        .index(
            "emb",
            IndexType::Vector(VectorIndexCfg {
                algorithm: algo,
                metric: VectorMetric::Cosine,
                embedding: None,
            }),
        )
        .apply()
        .await?;

    let mut rng = Rng(0x0BAD_5EED ^ N as u64);
    let mut corpus = Vec::with_capacity(N);
    let tx = db.session().tx().await?;
    for i in 0..N {
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
    // Build the ANN structure over the full flushed corpus.
    db.indexes().rebuild("Doc", false).await?;
    Ok((db, corpus))
}

/// Top-K `uni.vector.query` with an options literal (e.g. `{nprobes: 32, refine_factor: 32}`).
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

/// Build under `algo`, query with `options`, assert `recall@K >= floor`.
async fn assert_recall(algo: VectorAlgo, options: &str, floor: f64) -> anyhow::Result<()> {
    let (db, corpus) = setup(algo).await?;
    let query: Dense = {
        let mut rng = Rng(0x5EED_0DE5);
        random_dense(&mut rng)
    };
    let titles = run_query(&db, &query, options).await?;
    let recall = recall_at_k(&titles, &query, &corpus);
    eprintln!("recall@{K} = {recall:.3} (floor {floor:.2}) options={options}");
    assert!(
        recall >= floor,
        "recall@{K} = {recall:.3} below floor {floor:.2} (options={options})"
    );
    Ok(())
}

// IVF probes all partitions + a wide exact-rescore window so the floor reflects quantizer
// fidelity, not first-stage candidate selection.
const IVF_OPTS: &str = "{nprobes: 64, refine_factor: 32}";
// HNSW widens the search beam well past the k≈1.5× default.
const HNSW_OPTS: &str = "{ef_search: 200}";

#[tokio::test]
#[ignore = "recall@k over a multi-thousand-vector corpus; run explicitly"]
async fn recall_flat_is_exact() -> anyhow::Result<()> {
    // Flat is brute-force exact: recall must be ≈1.0.
    assert_recall(VectorAlgo::Flat, "{}", 0.99).await
}

#[tokio::test]
#[ignore = "recall@k over a multi-thousand-vector corpus; run explicitly"]
async fn recall_ivf_pq() -> anyhow::Result<()> {
    assert_recall(
        VectorAlgo::IvfPq {
            partitions: 32,
            sub_vectors: 8,
        },
        IVF_OPTS,
        0.8,
    )
    .await
}

#[tokio::test]
#[ignore = "recall@k over a multi-thousand-vector corpus; run explicitly"]
async fn recall_ivf_sq() -> anyhow::Result<()> {
    assert_recall(VectorAlgo::IvfSq { partitions: 32 }, IVF_OPTS, 0.8).await
}

#[tokio::test]
#[ignore = "recall@k over a multi-thousand-vector corpus; run explicitly"]
async fn recall_ivf_rq() -> anyhow::Result<()> {
    // RaBitQ is the most aggressive quantizer: its first-stage candidate ranking is coarser than
    // PQ/SQ, so the true top-k can fall outside a narrow rescore window. 8 bits/dim plus a wider
    // exact-rescore window (refine_factor 96) lands recall@10 around 0.7. The floor sits one
    // recall-bucket (1/k = 0.1) below that to absorb training nondeterminism while still flagging
    // a collapse — it is well under the scalar/PQ modes by design (RaBitQ trades recall for size).
    assert_recall(
        VectorAlgo::IvfRq {
            partitions: 32,
            num_bits: Some(8),
        },
        "{nprobes: 64, refine_factor: 96}",
        0.6,
    )
    .await
}

#[tokio::test]
#[ignore = "recall@k over a multi-thousand-vector corpus; run explicitly"]
async fn recall_hnsw_sq() -> anyhow::Result<()> {
    assert_recall(
        VectorAlgo::HnswSq {
            m: 16,
            ef_construction: 200,
            partitions: None,
        },
        HNSW_OPTS,
        0.8,
    )
    .await
}

#[tokio::test]
#[ignore = "recall@k over a multi-thousand-vector corpus; run explicitly"]
async fn recall_hnsw_pq() -> anyhow::Result<()> {
    // HNSW has no IVF-style exact-rescore (`refine_factor`) knob, so the product-quantized codes
    // drive graph traversal directly. Fine-grained sub-vectors (DIM/sub_vectors = 2 dims each)
    // keep the codes accurate enough for a high-recall traversal; the floor is a touch lower than
    // the scalar/IVF modes because there is no exact-rescore stage to fall back on.
    assert_recall(
        VectorAlgo::HnswPq {
            m: 16,
            ef_construction: 200,
            sub_vectors: 32,
            partitions: None,
        },
        HNSW_OPTS,
        0.7,
    )
    .await
}

// ---------------------------------------------------------------------------
// Multi-vector (ColBERT / MaxSim) recall — native IVF_PQ first stage + exact rescore.
// ---------------------------------------------------------------------------

/// Token-vector multi-set for one document.
type Tokens = Vec<Vec<f32>>;

impl Rng {
    /// A unit-norm token vector of dimension `DIM`.
    fn unit_vector(&mut self) -> Vec<f32> {
        let mut v: Vec<f32> = (0..DIM).map(|_| self.component()).collect();
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
        for x in &mut v {
            *x /= norm;
        }
        v
    }
}

fn tokens_value(tokens: &Tokens) -> Value {
    Value::List(
        tokens
            .iter()
            .map(|t| Value::List(t.iter().map(|&x| Value::Float(x as f64)).collect()))
            .collect(),
    )
}

/// Brute-force cosine-MaxSim ground truth: `Σ_q max_d cos(q, d)`.
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

fn maxsim_recall_at_k(engine: &[String], query: &Tokens, corpus: &[Tokens]) -> f64 {
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

#[tokio::test]
#[ignore = "recall@k over a multi-thousand-doc multi-vector corpus; run explicitly"]
async fn recall_multivector_maxsim_ivf_pq() -> anyhow::Result<()> {
    const DOCS: usize = 1_500;
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

    let mut rng = Rng(0x0BAD_5EED ^ DOCS as u64);
    let mut corpus: Vec<Tokens> = Vec::with_capacity(DOCS);
    let tx = db.session().tx().await?;
    for i in 0..DOCS {
        let n_tokens = 2 + (rng.next_u64() % 5) as usize; // 2..=6 tokens
        let toks: Tokens = (0..n_tokens).map(|_| rng.unit_vector()).collect();
        tx.execute_with("CREATE (:Doc {title: $title, tokens: $toks})")
            .param("title", Value::String(format!("d{i}")))
            .param("toks", tokens_value(&toks))
            .run()
            .await?;
        corpus.push(toks);
    }
    tx.commit().await?;
    db.flush().await?;

    // Native multi-vector IVF_PQ index over the token vectors.
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE VECTOR INDEX tok_idx FOR (d:Doc) ON (d.tokens) \
         OPTIONS {type: 'ivf_pq', partitions: 32, sub_vectors: 8, num_bits: 8, metric: 'cosine'}",
    )
    .await?;
    tx.commit().await?;
    db.indexes().rebuild("Doc", false).await?;

    let query: Tokens = {
        let mut qr = Rng(0x5EED_0DE5);
        (0..4).map(|_| qr.unit_vector()).collect()
    };
    let cypher = format!(
        "CALL uni.vector.query('Doc', 'tokens', $q, {K}, null, null, {{nprobes: 32, refine_factor: 32}}) \
         YIELD node, score RETURN node.title AS title"
    );
    let rows = db
        .session()
        .query_with(&cypher)
        .param("q", tokens_value(&query))
        .fetch_all()
        .await?;
    let titles: Vec<String> = rows
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect();

    let recall = maxsim_recall_at_k(&titles, &query, &corpus);
    eprintln!("multivector maxsim recall@{K} = {recall:.3} (floor 0.70)");
    assert!(
        recall >= 0.70,
        "multivector maxsim recall@{K} = {recall:.3} below floor 0.70"
    );
    Ok(())
}
