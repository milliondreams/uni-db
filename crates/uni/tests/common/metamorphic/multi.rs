//! Metamorphic oracle for multi-vector (ColBERT / MaxSim) search — parity with the
//! sparse + dense metamorphic oracles.
//!
//! Relation: `uni.vector.query` over a `List<Vector>` token column returns the same
//! ranked top-k as an independent brute-force cosine-MaxSim oracle over the same
//! corpus. No index is created, so the exact MaxSim rerank path
//! (`multivector_rerank` over the full corpus) is exercised against a disjoint
//! oracle — agreement on randomized query multi-vectors is a strong
//! silent-wrong-answer check on the scoring itself (the MUVERA FDE first stage is
//! a separate, approximate recall concern, unsuited to exact-equality checking).
//!
//! Comparison is by **score** (`Σ_q max_d cos(q, d)`), rank-by-rank, within `EPS`.

use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use uni_db::{DataType, Uni, Value};

use super::{smoke_cases, soak_cases};

/// Token dimensionality.
const DIM: usize = 8;
/// Number of seeded documents.
const CORPUS: usize = 40;
/// Tokens per seeded document.
const DOC_TOKENS: usize = 3;
/// Absolute tolerance for the f32-engine vs f64-oracle comparison.
const EPS: f64 = 1e-3;

type Tokens = Vec<Vec<f32>>;

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
    fn unit(&mut self) -> Vec<f32> {
        let mut v: Vec<f32> = (0..DIM)
            .map(|_| (self.next_u64() >> 40) as f32 / (1u64 << 23) as f32 - 1.0)
            .collect();
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
        for x in &mut v {
            *x /= norm;
        }
        v
    }
}

fn deterministic_corpus(n: usize) -> Vec<(String, Tokens)> {
    let mut rng = Rng(0x5EED_5EED_5EED_5EED);
    (0..n)
        .map(|i| {
            (
                format!("doc{i}"),
                (0..DOC_TOKENS).map(|_| rng.unit()).collect(),
            )
        })
        .collect()
}

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
    // A query token contributes its single best (max) cosine over the doc tokens —
    // which may be NEGATIVE; only a doc with zero tokens contributes 0. This mirrors
    // `uni_query_functions::similar_to::maxsim` (no clamping), the exact score the
    // engine returns.
    query
        .iter()
        .map(|q| {
            doc.iter()
                .map(|d| cos(q, d))
                .fold(None, |acc: Option<f64>, s| {
                    Some(acc.map_or(s, |b: f64| b.max(s)))
                })
                .unwrap_or(0.0)
        })
        .sum()
}

/// The seeded multi-vector corpus plus its open database.
struct MultiSeed {
    db: Uni,
    corpus: Vec<(String, Tokens)>,
}

async fn build_multi_seed() -> anyhow::Result<MultiSeed> {
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
    let corpus = deterministic_corpus(CORPUS);
    let tx = db.session().tx().await?;
    for (title, toks) in &corpus {
        tx.execute_with("CREATE (:Doc {title: $t, tokens: $e})")
            .param("t", Value::String(title.clone()))
            .param("e", to_value(toks))
            .run()
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;
    Ok(MultiSeed { db, corpus })
}

/// A randomized query: 1–3 non-degenerate token vectors and a top-`k`. Tokens
/// need not be unit-normalized — cosine normalizes both sides, so only direction
/// matters to the score.
fn arb_query() -> impl Strategy<Value = (Tokens, usize)> {
    (
        proptest::collection::vec(proptest::collection::vec(-1.0f32..1.0f32, DIM), 1..=3),
        1usize..=10usize,
    )
        .prop_filter("non-degenerate tokens", |(toks, _)| {
            toks.iter().all(|t| t.iter().any(|&x| x.abs() > 1e-3))
        })
}

async fn check_oracle(
    db: &Uni,
    corpus: &[(String, Tokens)],
    q: &(Tokens, usize),
) -> anyhow::Result<()> {
    let (query, k) = q;

    // Pass the query multi-vector as a param (not a Cypher literal): randomized
    // negative components render as `-0.73…`, which the procedure-arg parser would
    // reject as a unary-negation expression.
    //
    // `over_fetch` is forced high so the first-stage candidate generator returns
    // the WHOLE corpus before the exact MaxSim re-rank — this oracle pins the
    // re-rank *scoring*, not the approximate first-stage recall (a separate
    // property). With `retrieval_k = k * over_fetch >= CORPUS` the rerank is
    // exhaustive, so its top-k must equal the brute-force oracle's exactly.
    let rows = db
        .session()
        .query_with(
            "CALL uni.vector.query('Doc', 'tokens', $q, $k, null, null, {over_fetch: 100.0}) \
             YIELD node, score RETURN node.title AS title, score",
        )
        .param("q", to_value(query))
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
        .map(|(t, toks)| (t.as_str(), maxsim_oracle(query, toks)))
        .collect();
    let mut oracle: Vec<(&str, f64)> = oracle_scores.iter().map(|(t, s)| (*t, *s)).collect();
    oracle.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(b.0))
    });
    oracle.truncate(*k);

    for (title, score) in &engine {
        let want = oracle_scores.get(title.as_str()).copied().ok_or_else(|| {
            anyhow::anyhow!("engine returned a title absent from the corpus: {title:?}")
        })?;
        anyhow::ensure!(
            (score - want).abs() < EPS,
            "score fidelity: engine {title:?}={score} but oracle maxsim={want}"
        );
    }
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
    for w in engine.windows(2) {
        anyhow::ensure!(
            w[0].1 >= w[1].1 - EPS,
            "engine results not descending by score: {engine:?}"
        );
    }
    Ok(())
}

/// Run the multi-vector metamorphic relation over `cases` randomized queries
/// against one shared, flushed corpus.
///
/// # Panics
///
/// Panics (failing the test) if the seed cannot be built or any case violates the
/// rerank-vs-oracle relation; proptest shrinks to a minimal failing query.
fn run(cases: u32) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let seed = rt
        .block_on(build_multi_seed())
        .expect("build multi seed db");
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
        .expect("multi-vector metamorphic relation violated");
}

/// PR smoke gate for the multi-vector rerank-vs-oracle relation.
#[test]
fn multi_oracle_smoke() {
    run(smoke_cases());
}

/// Nightly soak for the multi-vector rerank-vs-oracle relation; volume from
/// `METAMORPHIC_CASES`.
#[test]
#[ignore = "soak: high-volume nightly run; volume set by METAMORPHIC_CASES"]
fn multi_oracle_soak() {
    run(soak_cases());
}
