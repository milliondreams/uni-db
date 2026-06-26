//! Metamorphic oracle for the dense-vector (KNN) index — parity with the sparse
//! metamorphic oracle (`metamorphic/sparse.rs`).
//!
//! Relation: `uni.vector.query` over an exact `Flat`/Cosine index returns the same
//! ranked top-k as an independent brute-force cosine oracle over the same corpus.
//! The index path (Lance flat scan + L0 union + `calculate_score`) and the oracle
//! share no code, so agreement on randomized query vectors is a strong
//! silent-wrong-answer check.
//!
//! The corpus is deterministic and flushed; only the query vector and `k` vary per
//! case. Comparison is by **score** (`(1 + cos) / 2`), rank-by-rank, within `EPS`:
//! a returned doc must carry its exact cosine score, and the descending score
//! vector must equal the oracle's top-k score vector.

use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use uni_db::{DataType, IndexType, Uni, Value, VectorAlgo, VectorIndexCfg, VectorMetric};

use super::{smoke_cases, soak_cases};

/// Vector dimensionality.
const DIM: usize = 16;
/// Number of seeded documents.
const CORPUS: usize = 60;
/// Absolute tolerance for the f32-engine vs f64-oracle comparison.
const EPS: f64 = 1e-3;

type Dense = Vec<f32>;

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
    fn component(&mut self) -> f32 {
        (self.next_u64() >> 40) as f32 / (1u64 << 23) as f32 - 1.0
    }
}

fn random_dense(rng: &mut Rng) -> Dense {
    (0..DIM).map(|_| rng.component()).collect()
}

fn deterministic_corpus(n: usize) -> Vec<(String, Dense)> {
    let mut rng = Rng(0x5EED_5EED_5EED_5EED);
    (0..n)
        .map(|i| (format!("doc{i}"), random_dense(&mut rng)))
        .collect()
}

/// Brute-force cosine score ground truth in f64: `(1 + cos(q, d)) / 2`.
fn dense_score_oracle(q: &Dense, d: &Dense) -> f64 {
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

/// The seeded dense corpus plus its open database.
struct DenseSeed {
    db: Uni,
    corpus: Vec<(String, Dense)>,
}

async fn build_dense_seed() -> anyhow::Result<DenseSeed> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("emb", DataType::Vector { dimensions: DIM })
        .index(
            "emb",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::Cosine,
                embedding: None,
            }),
        )
        .apply()
        .await?;
    let corpus = deterministic_corpus(CORPUS);
    let tx = db.session().tx().await?;
    for (title, d) in &corpus {
        tx.execute_with("CREATE (:Doc {title: $t, emb: $e})")
            .param("t", Value::String(title.clone()))
            .param("e", Value::Vector(d.clone()))
            .run()
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;
    Ok(DenseSeed { db, corpus })
}

/// A randomized query: a dense vector (non-degenerate norm) and a top-`k`.
fn arb_query() -> impl Strategy<Value = (Dense, usize)> {
    (
        proptest::collection::vec(-1.0f32..1.0f32, DIM),
        1usize..=10usize,
    )
        .prop_filter("non-zero norm", |(v, _)| v.iter().any(|&x| x.abs() > 1e-3))
        .prop_map(|(v, k)| (v, k))
}

async fn check_oracle(
    db: &Uni,
    corpus: &[(String, Dense)],
    q: &(Dense, usize),
) -> anyhow::Result<()> {
    let (query, k) = q;

    let rows = db
        .session()
        .query_with(
            "CALL uni.vector.query('Doc', 'emb', $q, $k, null, null, {}) \
             YIELD node, score RETURN node.title AS title, score",
        )
        .param("q", Value::Vector(query.clone()))
        .param("k", Value::Int(*k as i64))
        .fetch_all()
        .await?;
    let engine: Vec<(String, f64)> = rows
        .rows()
        .iter()
        .map(|r| -> anyhow::Result<(String, f64)> {
            Ok((r.get::<String>("title")?, r.get::<f64>("score")?))
        })
        .collect::<anyhow::Result<_>>()?;

    let oracle_scores: std::collections::HashMap<&str, f64> = corpus
        .iter()
        .map(|(t, d)| (t.as_str(), dense_score_oracle(query, d)))
        .collect();
    let mut oracle: Vec<(&str, f64)> = oracle_scores.iter().map(|(t, s)| (*t, *s)).collect();
    oracle.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(b.0))
    });
    oracle.truncate(*k);

    // (a) Each returned doc carries its exact cosine score.
    for (title, score) in &engine {
        let want = oracle_scores.get(title.as_str()).copied().ok_or_else(|| {
            anyhow::anyhow!("engine returned a title absent from the corpus: {title:?}")
        })?;
        anyhow::ensure!(
            (score - want).abs() < EPS,
            "score fidelity: engine {title:?}={score} but oracle cos={want}"
        );
    }
    // (b) Same count, descending score vectors agree rank-by-rank (stable under
    // k-boundary ties, which are essentially impossible for continuous cosine).
    anyhow::ensure!(
        engine.len() == oracle.len(),
        "top-k size mismatch: engine {} vs oracle {} (k={k})\n  engine: {engine:?}\n  oracle: {oracle:?}",
        engine.len(),
        oracle.len(),
    );
    for (rank, ((_, e), (_, o))) in engine.iter().zip(&oracle).enumerate() {
        anyhow::ensure!(
            (e - o).abs() < EPS,
            "rank {rank} score mismatch: engine={e} oracle={o}\n  engine: {engine:?}\n  oracle: {oracle:?}",
        );
    }
    // (c) Engine results are in non-increasing score order.
    for w in engine.windows(2) {
        anyhow::ensure!(
            w[0].1 >= w[1].1 - EPS,
            "engine results not descending by score: {engine:?}"
        );
    }
    Ok(())
}

/// Run the dense metamorphic relation over `cases` randomized queries against one
/// shared, flushed corpus.
///
/// # Panics
///
/// Panics (failing the test) if the seed cannot be built or any case violates the
/// index-vs-oracle relation; proptest shrinks to a minimal failing query.
fn run(cases: u32) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let seed = rt
        .block_on(build_dense_seed())
        .expect("build dense seed db");
    let mut runner = TestRunner::new(Config {
        cases,
        ..Config::default()
    });
    runner
        .run(&arb_query(), |q| {
            rt.block_on(check_oracle(&seed.db, &seed.corpus, &q))
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
            Ok(())
        })
        .expect("dense metamorphic relation violated");
}

/// PR smoke gate for the dense index-vs-oracle relation.
#[test]
fn dense_oracle_smoke() {
    run(smoke_cases());
}

/// Nightly soak for the dense index-vs-oracle relation; volume from
/// `METAMORPHIC_CASES`.
#[test]
#[ignore = "soak: high-volume nightly run; volume set by METAMORPHIC_CASES"]
fn dense_oracle_soak() {
    run(soak_cases());
}
