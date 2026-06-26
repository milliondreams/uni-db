//! Metamorphic oracle for the scored sparse-vector index (issue #95, test set I).
//!
//! Relation: `uni.sparse.query` over the sparse index returns the same ranked
//! top-k as an independent brute-force `sparse_dot` oracle over the same corpus.
//! The index path (postings `term_id IN (...)` filter + L0 union + exact
//! rescore) and the oracle share no code, so agreement on randomized query
//! vectors is a strong silent-wrong-answer check — the first vector-modality
//! case in the metamorphic harness.
//!
//! The corpus is deterministic and flushed (so the postings index, not just the
//! L0 brute-force path, is exercised); only the query vector and `k` vary per
//! case. Comparison is by **score**, not title: a returned doc must carry its
//! exact dot score, and the descending score vector must equal the oracle's
//! top-k score vector within `EPS`. This is stable under score ties at the
//! k-boundary (which swap *which title* is returned, never *which score*) while
//! still catching a genuinely mis-ranked doc.

use std::collections::{BTreeMap, HashMap};

use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use uni_db::{DataType, IndexType, Uni, Value};

use super::{smoke_cases, soak_cases};

/// Term-space cardinality (sparse-index `dimensions`).
const VOCAB: usize = 256;
/// Number of seeded documents.
const CORPUS: usize = 60;
/// Non-zeros per seeded document.
const DOC_NNZ: usize = 8;
/// Absolute tolerance for the f32-engine vs f64-oracle dot comparison.
const EPS: f64 = 1e-3;

/// A sparse vector as parallel sorted-unique `(indices, values)`.
type Sparse = (Vec<u32>, Vec<f32>);

/// A xorshift PRNG — a deterministic corpus, no external rand dependency.
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
    fn weight(&mut self) -> f32 {
        // Positive weights in [0.1, ~1.1), matching learned-sparse (ReLU) output.
        ((self.next_u64() >> 40) as f32 / (1u64 << 24) as f32) + 0.1
    }
    fn term(&mut self) -> u32 {
        (self.next_u64() % VOCAB as u64) as u32
    }
}

/// A random sparse vector with `nnz` sorted-unique terms and positive weights.
fn random_sparse(rng: &mut Rng, nnz: usize) -> Sparse {
    let mut m: BTreeMap<u32, f32> = BTreeMap::new();
    while m.len() < nnz {
        let t = rng.term();
        let w = rng.weight();
        m.insert(t, w);
    }
    (m.keys().copied().collect(), m.values().copied().collect())
}

/// Deterministic corpus of `(title, sparse_vector)`.
fn deterministic_corpus(n: usize) -> Vec<(String, Sparse)> {
    let mut rng = Rng(0x5EED_5EED_5EED_5EED);
    (0..n)
        .map(|i| (format!("doc{i}"), random_sparse(&mut rng, DOC_NNZ)))
        .collect()
}

fn sv_value((indices, values): &Sparse) -> Value {
    Value::SparseVector {
        indices: indices.clone(),
        values: values.clone(),
    }
}

/// Brute-force dot-product ground truth in f64.
fn sparse_dot_oracle(q: &Sparse, d: &Sparse) -> f64 {
    let qm: HashMap<u32, f64> = q.0.iter().zip(&q.1).map(|(&t, &w)| (t, w as f64)).collect();
    d.0.iter()
        .zip(&d.1)
        .filter_map(|(&t, &w)| qm.get(&t).map(|qw| qw * w as f64))
        .sum()
}

/// The seeded sparse corpus plus its open database.
struct SparseSeed {
    db: Uni,
    corpus: Vec<(String, Sparse)>,
}

/// Build the `Doc(title, emb)` schema + sparse index, insert the deterministic
/// corpus, and flush so queries hit the postings index.
async fn build_sparse_seed() -> anyhow::Result<SparseSeed> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("emb", DataType::SparseVector { dimensions: VOCAB })
        .index("emb", IndexType::Sparse { dimensions: VOCAB })
        .apply()
        .await?;
    let corpus = deterministic_corpus(CORPUS);
    let tx = db.session().tx().await?;
    for (title, sp) in &corpus {
        tx.execute_with("CREATE (:Doc {title: $t, emb: $e})")
            .param("t", Value::String(title.clone()))
            .param("e", sv_value(sp))
            .run()
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;
    Ok(SparseSeed { db, corpus })
}

/// A randomized query: a sparse vector and a top-`k`.
fn arb_query() -> impl Strategy<Value = (Sparse, usize)> {
    (1usize..=8)
        .prop_flat_map(|nnz| {
            (
                proptest::collection::vec((0u32..VOCAB as u32, 0.01f32..2.0), nnz),
                1usize..=10usize,
            )
        })
        .prop_map(|(pairs, k)| {
            let mut m: BTreeMap<u32, f32> = BTreeMap::new();
            for (t, w) in pairs {
                m.insert(t, w);
            }
            (
                (m.keys().copied().collect(), m.values().copied().collect()),
                k,
            )
        })
}

/// Assert the index result for `(query, k)` matches the brute-force oracle.
async fn check_oracle(
    db: &Uni,
    corpus: &[(String, Sparse)],
    q: &(Sparse, usize),
) -> anyhow::Result<()> {
    let (query, k) = q;

    // Index path: ranked top-k with the engine's exact dot score.
    let rows = db
        .session()
        .query_with(
            "CALL uni.sparse.query('Doc', 'emb', $q, $k, null, null, {}) \
             YIELD node, score RETURN node.title AS title, score",
        )
        .param("q", sv_value(query))
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

    // Oracle: brute-force dot over the corpus, drop zero-overlap, sort by score
    // (descending, title-tie-broken for determinism), truncate to k.
    let oracle_scores: HashMap<&str, f64> = corpus
        .iter()
        .map(|(t, sp)| (t.as_str(), sparse_dot_oracle(query, sp)))
        .collect();
    let mut oracle: Vec<(&str, f64)> = oracle_scores
        .iter()
        .filter(|(_, s)| **s > 0.0)
        .map(|(t, s)| (*t, *s))
        .collect();
    oracle.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(b.0))
    });
    oracle.truncate(*k);

    // (a) Each returned doc carries its exact dot score.
    for (title, score) in &engine {
        let want = oracle_scores.get(title.as_str()).copied().ok_or_else(|| {
            anyhow::anyhow!("engine returned a title absent from the corpus: {title:?}")
        })?;
        anyhow::ensure!(
            (score - want).abs() < EPS,
            "score fidelity: engine {title:?}={score} but oracle dot={want}"
        );
    }
    // (b) Same count, and the descending score vectors agree rank-by-rank
    // (stable under k-boundary ties, which permute titles but not scores).
    anyhow::ensure!(
        engine.len() == oracle.len(),
        "top-k size mismatch: engine returned {} docs, oracle {} (k={k})\n  engine: {engine:?}\n  oracle: {oracle:?}",
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

/// Run the sparse metamorphic relation over `cases` randomized queries against a
/// single shared, flushed corpus.
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
        .block_on(build_sparse_seed())
        .expect("build sparse seed db");
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
        .expect("sparse metamorphic relation violated");
}

/// PR smoke gate for the sparse index-vs-oracle relation.
#[test]
fn sparse_oracle_smoke() {
    run(smoke_cases());
}

/// Nightly soak for the sparse index-vs-oracle relation; volume from
/// `METAMORPHIC_CASES`.
#[test]
#[ignore = "soak: high-volume nightly run; volume set by METAMORPHIC_CASES"]
fn sparse_oracle_soak() {
    run(soak_cases());
}
