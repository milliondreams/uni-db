// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase-3 tests for MUVERA (Fixed-Dimensional-Encoding) multi-vector indexes
//! (issue #96). A `CREATE VECTOR INDEX ... OPTIONS {type:'muvera', ...}` over a
//! `List<Vector>` column derives a single-vector FDE column (`__fde_*`) and builds a
//! normal single-vector ANN over it; `uni.vector.query` runs the FDE ANN as a fast
//! first stage and re-ranks candidates by EXACT MaxSim.
//!
//! Token convention (DIM=8): query `[e0, e1]`. The doc titled `"target"` has tokens ==
//! the query tokens, so it is the unique MaxSim maximizer (score 2.0); other docs are
//! random (well below 2.0). Self-retrieval of an exact match is robust under MUVERA even
//! on cluster-free synthetic data, so `target` must rank first.

use uni_db::core::schema::{IndexDefinition, VectorIndexType};
use uni_db::{DataType, Uni, Value};

const DIM: usize = 8;
/// FDE params kept small so corpora train quickly; `inner: 'flat'` = exact ANN over the
/// FDE column (no PQ training needed), which makes the correctness assertions stable.
const MUVERA_OPTS: &str = "{type: 'muvera', k_sim: 4, reps: 8, d_proj: 8, inner: 'flat'}";

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
            .map(|_| ((self.next_u64() >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0)
            .collect();
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
        for x in &mut v {
            *x /= norm;
        }
        v
    }
}

fn basis(i: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; DIM];
    v[i] = 1.0;
    v
}

fn to_value(tokens: &[Vec<f32>]) -> Value {
    Value::List(
        tokens
            .iter()
            .map(|t| Value::List(t.iter().map(|&x| Value::Float(x as f64)).collect()))
            .collect(),
    )
}

fn cypher_lit(tokens: &[Vec<f32>]) -> String {
    let toks: Vec<String> = tokens
        .iter()
        .map(|t| {
            let nums: Vec<String> = t.iter().map(|x| format!("{x:?}")).collect();
            format!("[{}]", nums.join(","))
        })
        .collect();
    format!("[{}]", toks.join(","))
}

fn query_tokens() -> Vec<Vec<f32>> {
    vec![basis(0), basis(1)]
}

async fn define_schema(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
        )
        .apply()
        .await?;
    Ok(())
}

/// Deterministic corpus: doc `n/2` titled `target` has tokens == the query (the unique
/// MaxSim maximizer, score 2.0); the rest are random unit-vector docs. Returned (not just
/// inserted) so a test can compute an independent brute-force MaxSim ground truth over the
/// exact same data.
fn build_corpus(n: usize, seed: u64) -> Vec<(String, Vec<Vec<f32>>)> {
    let q = query_tokens();
    let mut rng = Rng(seed);
    (0..n)
        .map(|i| {
            if i == n / 2 {
                ("target".to_string(), q.clone())
            } else {
                (format!("doc{i}"), (0..3).map(|_| rng.unit()).collect())
            }
        })
        .collect()
}

/// Insert an explicit corpus in one tx.
async fn insert_docs(db: &Uni, docs: &[(String, Vec<Vec<f32>>)]) -> anyhow::Result<()> {
    let tx = db.session().tx().await?;
    for (title, tokens) in docs {
        tx.execute_with("CREATE (:Doc {title: $title, tokens: $toks})")
            .param("title", Value::String(title.clone()))
            .param("toks", to_value(tokens))
            .run()
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Insert `n` docs (the middle one titled `target` with tokens == query) in one tx.
async fn insert_corpus(db: &Uni, n: usize, seed: u64) -> anyhow::Result<()> {
    insert_docs(db, &build_corpus(n, seed)).await
}

async fn create_muvera_index(db: &Uni, opts: &str) -> anyhow::Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "CREATE VECTOR INDEX tok_idx FOR (d:Doc) ON (d.tokens) OPTIONS {opts}"
    ))
    .await?;
    tx.commit().await?;
    Ok(())
}

async fn query_titles(db: &Uni, k: usize) -> anyhow::Result<Vec<String>> {
    let lit = cypher_lit(&query_tokens());
    let cypher = format!(
        "CALL uni.vector.query('Doc', 'tokens', {lit}, {k}, null, null, {{}}) \
         YIELD node, score RETURN node.title AS title"
    );
    let res = db.session().query(&cypher).await?;
    Ok(res
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect())
}

/// The `uni.vector.query` Cypher for the standard query tokens, yielding title + score.
fn vector_query_cypher(k: usize) -> String {
    let lit = cypher_lit(&query_tokens());
    format!(
        "CALL uni.vector.query('Doc', 'tokens', {lit}, {k}, null, null, {{}}) \
         YIELD node, score RETURN node.title AS title, score"
    )
}

/// Run the query and return `(title, score)` in engine rank order.
async fn query_results(db: &Uni, k: usize) -> anyhow::Result<Vec<(String, f64)>> {
    let res = db.session().query(&vector_query_cypher(k)).await?;
    Ok(res
        .rows()
        .iter()
        .map(|r| {
            (
                r.get::<String>("title").unwrap(),
                r.get::<f64>("score").unwrap(),
            )
        })
        .collect())
}

/// Brute-force ground truth: cosine MaxSim `Σ_q max_d cos(q,d)` (a query token with no doc
/// tokens contributes 0), computed in f64. This mirrors
/// `uni_query_functions::similar_to::maxsim` under the `Cosine` metric — raw cosine summed
/// over query tokens — which is the EXACT score the engine returns for a MUVERA / native
/// multi-vector query (the FDE first stage only selects candidates; the reported score is
/// the exact re-rank). The MUVERA indexes in this file use the default Cosine metric.
fn cosine_maxsim(query: &[Vec<f32>], doc: &[Vec<f32>]) -> f64 {
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
                .fold(None, |acc: Option<f64>, s| {
                    Some(acc.map_or(s, |b| b.max(s)))
                })
                .unwrap_or(0.0)
        })
        .sum()
}

/// Assert engine results match the brute-force oracle over `corpus`:
/// (a) every returned doc carries its EXACT MaxSim score, (b) results are ordered by
/// descending score, (c) the top score equals the oracle maximum, and — when
/// `expect_full_set` (retrieval covered the whole corpus, e.g. an exact `flat` inner with
/// `k == corpus.len()`) — (d) the returned title set equals the full corpus. `EPS` absorbs
/// f32-vs-f64 rounding.
fn assert_matches_oracle(
    engine: &[(String, f64)],
    corpus: &[(String, Vec<Vec<f32>>)],
    query: &[Vec<f32>],
    expect_full_set: bool,
) {
    const EPS: f64 = 1e-4;
    let oracle: std::collections::HashMap<&str, f64> = corpus
        .iter()
        .map(|(t, toks)| (t.as_str(), cosine_maxsim(query, toks)))
        .collect();

    for (title, score) in engine {
        let want = oracle
            .get(title.as_str())
            .unwrap_or_else(|| panic!("engine returned a title not in the corpus: {title:?}"));
        assert!(
            (score - want).abs() < EPS,
            "exact-MaxSim score mismatch for {title:?}: engine={score} oracle={want}"
        );
    }
    for w in engine.windows(2) {
        assert!(
            w[0].1 >= w[1].1 - EPS,
            "results are not ordered by descending score: {engine:?}"
        );
    }
    let oracle_max = oracle.values().copied().fold(f64::NEG_INFINITY, f64::max);
    if let Some((_, top)) = engine.first() {
        assert!(
            (top - oracle_max).abs() < EPS,
            "top score {top} != oracle max {oracle_max}: {engine:?}"
        );
    }
    if expect_full_set {
        let got: std::collections::HashSet<&str> = engine.iter().map(|(t, _)| t.as_str()).collect();
        let want: std::collections::HashSet<&str> = oracle.keys().copied().collect();
        assert_eq!(got, want, "returned set != full corpus (recall gap)");
    }
}

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

/// Backfill path: write + flush a corpus, THEN create the MUVERA index (which scans the
/// already-flushed rows, derives the FDE column, and builds the ANN). Query ranks the
/// exact MaxSim maximizer first.
#[tokio::test]
async fn muvera_backfill_then_query_ranks_target_first() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    insert_corpus(&db, 60, 0xD1CE_5EED).await?;
    db.flush().await?;

    create_muvera_index(&db, MUVERA_OPTS).await?;

    let titles = query_titles(&db, 5).await?;
    assert_eq!(
        titles.first().map(String::as_str),
        Some("target"),
        "MUVERA first-stage + exact re-rank should rank the maximizer first: {titles:?}"
    );
    Ok(())
}

/// Create-before-ingest: create the MUVERA index on an empty label, THEN write + flush.
/// The flush materialises the FDE column for the new rows (no backfill needed); the query
/// still finds the target (flat-scan over the FDE column when no ANN was built on empty).
#[tokio::test]
async fn muvera_create_before_ingest() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    create_muvera_index(&db, MUVERA_OPTS).await?;

    insert_corpus(&db, 60, 0xBEEF_F00D).await?;
    db.flush().await?;

    let titles = query_titles(&db, 5).await?;
    assert_eq!(
        titles.first().map(String::as_str),
        Some("target"),
        "flush-materialised FDE should retrieve the target: {titles:?}"
    );
    Ok(())
}

/// IVF_PQ inner index over the FDE column exercises the PQ build path (fde_dim must be
/// divisible by sub_vectors). The target still surfaces after exact re-rank.
#[tokio::test]
async fn muvera_ivf_pq_inner() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    let corpus = build_corpus(120, 0x1234_5678);
    insert_docs(&db, &corpus).await?;
    db.flush().await?;

    // fde_dim = reps*2^k_sim*d_proj = 8*16*8 = 1024; sub_vectors=8 divides it.
    create_muvera_index(
        &db,
        "{type: 'muvera', k_sim: 4, reps: 8, d_proj: 8, inner: 'ivf_pq', partitions: 4, sub_vectors: 8}",
    )
    .await?;

    // IVF_PQ is a LOSSY first stage (it may skip partitions), so we do NOT require
    // full-corpus recall. But the contract that must hold regardless of the lossy ANN:
    // (1) every retrieved doc carries its exact MaxSim score (re-rank is exact),
    // (2) results are ordered by descending score, and
    // (3) the exact-match `target` — reliably surfaced by the FDE self-retrieval property —
    //     is the global maximizer, so it must rank first after re-rank.
    let results = query_results(&db, 10).await?;
    assert_matches_oracle(&results, &corpus, &query_tokens(), false);
    assert_eq!(
        results.first().map(|(t, _)| t.as_str()),
        Some("target"),
        "exact-match target must rank first after re-rank: {results:?}"
    );
    Ok(())
}

/// L0 visibility: with a MUVERA index + flushed corpus, an UNFLUSHED insert is still
/// found — `multivector_rerank` unions live L0 candidates with the FDE first stage and
/// re-scores everything by exact MaxSim.
#[tokio::test]
async fn muvera_l0_and_flushed_mix() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    let mut corpus = build_corpus(40, 0xAAAA_5555);
    insert_docs(&db, &corpus).await?;
    db.flush().await?;
    create_muvera_index(&db, MUVERA_OPTS).await?;

    // A second exact-match doc inserted WITHOUT flushing (lives in L0 only).
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Doc {title: $t, tokens: $toks})")
        .param("t", Value::String("l0-target".to_string()))
        .param("toks", to_value(&query_tokens()))
        .run()
        .await?;
    tx.commit().await?;
    corpus.push(("l0-target".to_string(), query_tokens()));

    // Query the full corpus (flat inner ⇒ retrieval covers everything), so the unioned
    // Lance+L0 result must equal the brute-force ranking — not merely contain the L0 doc.
    let results = query_results(&db, corpus.len()).await?;
    assert_matches_oracle(&results, &corpus, &query_tokens(), true);
    // Both exact matches (flushed `target` + unflushed `l0-target`) occupy the top-2 at
    // the maximal score 2.0 — the L0 row is not just visible, it is ranked correctly.
    let top2: std::collections::HashSet<&str> =
        results.iter().take(2).map(|(t, _)| t.as_str()).collect();
    let want_top2: std::collections::HashSet<&str> = ["target", "l0-target"].into_iter().collect();
    assert_eq!(
        top2, want_top2,
        "both exact matches must occupy the top-2: {results:?}"
    );
    assert!(
        results.iter().take(2).all(|(_, s)| (s - 2.0).abs() < 1e-4),
        "top-2 exact matches must score 2.0: {results:?}"
    );
    Ok(())
}

/// Persistence: the MUVERA params (seed/k_sim/reps/d_proj) and the FDE column survive a
/// close + reopen, so query-time encoding still matches doc-time encoding.
#[tokio::test]
async fn muvera_persists_across_reopen() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap();
    {
        let db = Uni::open(path).build().await?;
        define_schema(&db).await?;
        insert_corpus(&db, 60, 0xFEED_BEEF).await?;
        db.flush().await?;
        create_muvera_index(&db, MUVERA_OPTS).await?;
        let titles = query_titles(&db, 5).await?;
        assert_eq!(titles.first().map(String::as_str), Some("target"));
        // Data is durable after flush; drop to release the path before reopening.
        drop(db);
    }
    // Reopen the same path.
    let db = Uni::open(path).build().await?;
    let titles = query_titles(&db, 5).await?;
    assert_eq!(
        titles.first().map(String::as_str),
        Some("target"),
        "MUVERA results must survive reopen: {titles:?}"
    );
    Ok(())
}

/// Fork fallback: a MUVERA-indexed parent is forked; the fork query inherits parent docs
/// and finds a fork-local exact match via the brute-force branch path (the FDE ANN is not
/// branched — forks always exact-rerank). The parent stays isolated from the fork write.
#[tokio::test]
async fn muvera_fork_fallback() -> anyhow::Result<()> {
    use uni_db::core::schema::DataType as DT;
    let cfg = uni_db::UniConfig {
        disable_fork_sweeper: true,
        disable_fork_index_builder: true,
        ..uni_db::UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;
    db.schema()
        .label("Doc")
        .property("title", DT::String)
        .property("tokens", DT::List(Box::new(DT::Vector { dimensions: DIM })))
        .apply()
        .await?;

    let parent_corpus = build_corpus(40, 0x0F0F_0F0F);
    insert_docs(&db, &parent_corpus).await?;
    db.flush().await?;
    create_muvera_index(&db, MUVERA_OPTS).await?;

    let forked = db.session().fork("muvera_fork").await?;
    let tx = forked.tx().await?;
    tx.execute_with("CREATE (:Doc {title: $t, tokens: $toks})")
        .param("t", Value::String("fork-target".to_string()))
        .param("toks", to_value(&query_tokens()))
        .run()
        .await?;
    tx.commit().await?;

    // Query the full fork corpus on the brute-force fork path (it scores ALL candidates,
    // including inherited rows), so the fork ranking must equal the brute-force oracle.
    let mut fork_corpus = parent_corpus.clone();
    fork_corpus.push(("fork-target".to_string(), query_tokens()));
    let fork_results: Vec<(String, f64)> = forked
        .query(&vector_query_cypher(fork_corpus.len()))
        .await?
        .rows()
        .iter()
        .map(|r| {
            (
                r.get::<String>("title").unwrap(),
                r.get::<f64>("score").unwrap(),
            )
        })
        .collect();
    assert_matches_oracle(&fork_results, &fork_corpus, &query_tokens(), true);
    // The fork inherits the parent's planted `target` (also tokens == query), so there are
    // two exact maximizers: the fork must surface its OWN `fork-target` alongside the
    // inherited `target` in the top-2, both at the maximal score 2.0.
    let fork_top2: std::collections::HashSet<&str> = fork_results
        .iter()
        .take(2)
        .map(|(t, _)| t.as_str())
        .collect();
    let want_fork_top2: std::collections::HashSet<&str> =
        ["target", "fork-target"].into_iter().collect();
    assert_eq!(
        fork_top2, want_fork_top2,
        "fork must surface its own exact match (brute-force fallback) in the top-2: {fork_results:?}"
    );
    assert!(
        fork_results
            .iter()
            .take(2)
            .all(|(_, s)| (s - 2.0).abs() < 1e-4),
        "both fork maximizers must score 2.0: {fork_results:?}"
    );

    // Parent is isolated from the fork's write and still matches its own ground truth.
    let parent_results = query_results(&db, parent_corpus.len()).await?;
    assert!(
        !parent_results.iter().any(|(t, _)| t == "fork-target"),
        "parent must not see fork-local docs: {parent_results:?}"
    );
    assert_matches_oracle(&parent_results, &parent_corpus, &query_tokens(), true);
    Ok(())
}

// ---------------------------------------------------------------------------
// Updates / tombstones / isolation
// ---------------------------------------------------------------------------

/// Updating the source tokens recomputes the FDE on the next flush (so the new tokens
/// drive ranking); deleting a doc removes it from results.
#[tokio::test]
async fn muvera_update_and_tombstone() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    let mut corpus = build_corpus(40, 0xC0FF_EE00);
    insert_docs(&db, &corpus).await?;
    db.flush().await?;
    create_muvera_index(&db, MUVERA_OPTS).await?;

    // Demote the target: overwrite its tokens with orthogonal vectors (MaxSim 0).
    let orthogonal = vec![basis(4), basis(5)];
    let tx = db.session().tx().await?;
    tx.execute_with("MATCH (d:Doc {title:'target'}) SET d.tokens = $toks")
        .param("toks", to_value(&orthogonal))
        .run()
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Mirror the update in the oracle and re-verify the WHOLE ranking, not just rank-1.
    for (title, toks) in corpus.iter_mut() {
        if title == "target" {
            *toks = orthogonal.clone();
        }
    }
    let results = query_results(&db, corpus.len()).await?;
    assert_matches_oracle(&results, &corpus, &query_tokens(), true);
    let target_score = results
        .iter()
        .find(|(t, _)| t == "target")
        .map(|(_, s)| *s)
        .expect("demoted target is still present, just re-ranked");
    assert!(
        target_score.abs() < 1e-4,
        "demoted target (orthogonal tokens) must score 0: {target_score}"
    );
    assert_ne!(
        results.first().map(|(t, _)| t.as_str()),
        Some("target"),
        "after demotion the target must NOT rank first: {results:?}"
    );

    // Now delete it; it must vanish, and the remaining ranking must still match the oracle.
    let tx = db.session().tx().await?;
    tx.execute("MATCH (d:Doc {title:'target'}) DETACH DELETE d")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    corpus.retain(|(t, _)| t != "target");
    let results = query_results(&db, corpus.len()).await?;
    assert!(
        !results.iter().any(|(t, _)| t == "target"),
        "deleted doc must not appear: {results:?}"
    );
    assert_matches_oracle(&results, &corpus, &query_tokens(), true);
    Ok(())
}

// ---------------------------------------------------------------------------
// Internal-column hiding + metadata
// ---------------------------------------------------------------------------

/// The derived `__fde_*` column must not leak into user output: not in `keys(d)`, not in
/// `uni.schema.labelInfo`. The MUVERA index itself IS listed (the user created it).
#[tokio::test]
async fn muvera_internal_column_hidden() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    insert_corpus(&db, 20, 0x5151_5151).await?;
    db.flush().await?;
    create_muvera_index(&db, MUVERA_OPTS).await?;

    // keys(d) must not contain an internal column.
    let res = db
        .session()
        .query("MATCH (d:Doc {title:'target'}) RETURN keys(d) AS k")
        .await?;
    let keys: Vec<String> = res.rows()[0].get::<Vec<String>>("k").unwrap_or_default();
    assert!(
        !keys.iter().any(|k| k.starts_with("__")),
        "internal FDE column leaked into keys(d): {keys:?}"
    );
    assert!(
        keys.iter().any(|k| k == "tokens"),
        "tokens should be present"
    );

    // labelInfo must not list the internal column.
    let info = db
        .session()
        .query("CALL uni.schema.labelInfo('Doc') YIELD property RETURN property")
        .await?;
    let props: Vec<String> = info
        .rows()
        .iter()
        .map(|r| r.get::<String>("property").unwrap())
        .collect();
    assert!(
        !props.iter().any(|p| p.starts_with("__")),
        "internal FDE column leaked into labelInfo: {props:?}"
    );

    // The MUVERA index IS persisted and listed.
    let has_muvera = db.indexes().list(Some("Doc")).into_iter().any(|idx| {
        matches!(idx, IndexDefinition::Vector(c) if c.property == "tokens" && c.name == "tok_idx")
    });
    assert!(
        has_muvera,
        "the MUVERA index should be listed for the label"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Cross-surface parity: the `uni.schema.createIndex` procedure
// ---------------------------------------------------------------------------

/// MUVERA works via the `uni.schema.createIndex` procedure exactly like the DDL path
/// (same shared option parser), including backfill over already-flushed rows.
#[tokio::test]
async fn muvera_via_procedure_createindex() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    insert_corpus(&db, 60, 0x9999_1111).await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "CALL uni.schema.createIndex('Doc', 'tokens', \
         {type: 'VECTOR', algorithm: 'muvera', k_sim: 4, reps: 8, d_proj: 8, inner: 'flat'})",
    )
    .await?;
    tx.commit().await?;

    let titles = query_titles(&db, 5).await?;
    assert_eq!(
        titles.first().map(String::as_str),
        Some("target"),
        "procedure-path MUVERA should rank the maximizer first: {titles:?}"
    );
    // The persisted index is a MUVERA index, and the FDE column stays hidden.
    let is_muvera = db.indexes().list(Some("Doc")).into_iter().any(|idx| {
        matches!(idx, IndexDefinition::Vector(c)
            if c.property == "tokens" && matches!(c.index_type, VectorIndexType::Muvera { .. }))
    });
    assert!(is_muvera, "procedure should persist a MUVERA index");
    Ok(())
}

/// Default-ANN parity: a dense vector index created WITHOUT specifying the algorithm
/// resolves to IVF_PQ via BOTH the DDL and the procedure path (previously the procedure
/// defaulted to HNSW — the divergence this alignment fixes).
#[tokio::test]
async fn dense_default_ann_is_ivf_pq_on_both_paths() -> anyhow::Result<()> {
    async fn dense_index_type(create_via_procedure: bool) -> anyhow::Result<VectorIndexType> {
        let db = Uni::temporary().build().await?;
        db.schema()
            .label("Vec")
            .property("emb", DataType::Vector { dimensions: DIM })
            .apply()
            .await?;
        let tx = db.session().tx().await?;
        tx.execute_with("CREATE (:Vec {emb: $e})")
            .param("e", Value::List(vec![Value::Float(0.0); DIM]))
            .run()
            .await?;
        tx.commit().await?;
        db.flush().await?;

        let tx = db.session().tx().await?;
        if create_via_procedure {
            // No `algorithm` → canonical default.
            tx.execute("CALL uni.schema.createIndex('Vec', 'emb', {type: 'VECTOR'})")
                .await?;
        } else {
            // No `type` in OPTIONS → canonical default.
            tx.execute("CREATE VECTOR INDEX v_idx FOR (n:Vec) ON (n.emb) OPTIONS {}")
                .await?;
        }
        tx.commit().await?;

        Ok(db
            .indexes()
            .list(Some("Vec"))
            .into_iter()
            .find_map(|idx| match idx {
                IndexDefinition::Vector(c) if c.property == "emb" => Some(c.index_type),
                _ => None,
            })
            .expect("vector index on emb"))
    }

    assert!(
        matches!(
            dense_index_type(false).await?,
            VectorIndexType::IvfPq { .. }
        ),
        "DDL default ANN must be IVF_PQ"
    );
    assert!(
        matches!(dense_index_type(true).await?, VectorIndexType::IvfPq { .. }),
        "procedure default ANN must be IVF_PQ (aligned with DDL)"
    );
    Ok(())
}

/// Reproduces the schema-BUILDER creation path (the surface the Python `.index()` API
/// uses → `rebuild_indexes_for_label` → `create_vector_index`), which the DDL/procedure
/// e2e tests above don't exercise.
#[tokio::test]
async fn muvera_via_schema_builder_path() -> anyhow::Result<()> {
    use uni_db::{VectorAlgo, VectorIndexCfg, VectorMetric};
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    insert_corpus(&db, 40, 0x7777_3333).await?;
    db.flush().await?;

    db.schema()
        .label("Doc")
        .index(
            "tokens",
            uni_db::IndexType::Vector(VectorIndexCfg {
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

    // A plain read must not error (this is what the Python test tripped on).
    let keys_rows = db
        .session()
        .query("MATCH (d:Doc {title:'target'}) RETURN keys(d) AS k")
        .await?;
    assert_eq!(keys_rows.rows().len(), 1, "target row must be readable");

    let titles = query_titles(&db, 5).await?;
    assert_eq!(
        titles.first().map(String::as_str),
        Some("target"),
        "builder-path MUVERA should rank the maximizer first: {titles:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// SHOW INDEXES must not leak the internal FDE artifacts
// ---------------------------------------------------------------------------

/// `SHOW INDEXES` is a user-facing surface, so the derived `__fde_*` column and any index
/// built over it must stay hidden: no listed row may be named for or reference `__fde`.
/// The user's own MUVERA index IS listed (they created it).
#[tokio::test]
async fn muvera_show_indexes_hides_internal() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    insert_corpus(&db, 20, 0xABCD_1234).await?;
    db.flush().await?;
    create_muvera_index(&db, MUVERA_OPTS).await?;

    let res = db.session().query("SHOW INDEXES").await?;
    let mut names = Vec::new();
    let mut leaked = Vec::new();
    for row in res.rows() {
        let name = row.get::<String>("name").unwrap_or_default();
        let details = row.get::<String>("details").unwrap_or_default();
        if name.starts_with("__") || name.contains("__fde") || details.contains("__fde") {
            leaked.push(format!("{name} :: {details}"));
        }
        names.push(name);
    }
    assert!(
        leaked.is_empty(),
        "internal FDE index/column leaked into SHOW INDEXES: {leaked:?}"
    );
    assert!(
        names.iter().any(|n| n == "tok_idx"),
        "the user MUVERA index should be listed by SHOW INDEXES: {names:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Cross-surface parity: dense (non-MUVERA) native multi-vector index via the
// Rust schema-builder path (the Python `.index()` surface)
// ---------------------------------------------------------------------------

/// The schema-builder path (`db.schema().label().index().apply()` →
/// `rebuild_indexes_for_label` → `create_vector_index`) must also build a working DENSE
/// native multi-vector IVF_PQ index — not just MUVERA. This complements
/// `muvera_via_schema_builder_path` and guards the same rebuild path for the non-MUVERA
/// algorithm. IVF_PQ requires `sub_vectors | DIM` and enough training vectors (>=256), so
/// the corpus is sized accordingly (~3 tokens/doc).
#[tokio::test]
async fn dense_ivfpq_via_schema_builder_path() -> anyhow::Result<()> {
    use uni_db::{VectorAlgo, VectorIndexCfg, VectorMetric};
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    let corpus = build_corpus(120, 0x2468_ACE0);
    insert_docs(&db, &corpus).await?;
    db.flush().await?;

    db.schema()
        .label("Doc")
        .index(
            "tokens",
            uni_db::IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::IvfPq {
                    partitions: 4,
                    sub_vectors: 4,
                },
                metric: VectorMetric::Cosine,
                embedding: None,
            }),
        )
        .apply()
        .await?;

    // Native IVF_PQ is a lossy first stage (don't require full-set recall), but the exact
    // MaxSim re-rank must score every retrieved doc correctly and surface the exact-match
    // target (the global maximizer) at rank 1.
    let results = query_results(&db, 10).await?;
    assert_matches_oracle(&results, &corpus, &query_tokens(), false);
    assert_eq!(
        results.first().map(|(t, _)| t.as_str()),
        Some("target"),
        "builder-path native IVF_PQ should rank the exact match first: {results:?}"
    );
    Ok(())
}

#[tokio::test]
async fn muvera_wrong_dim_token_does_not_wedge_flush() -> anyhow::Result<()> {
    // Regression for issue #96: a source multi-vector token whose dimension != the index
    // `input_dim` made the FDE encoder hard-error inside `materialize_fde_columns`, which
    // aborted the WHOLE flush and wedged L0 (the rotated buffer stuck on the pending list,
    // so every subsequent flush re-hit the bad row and also failed). The flush must now
    // skip the malformed row (leaving its FDE NULL) and succeed, with well-formed docs
    // still retrievable.
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    create_muvera_index(&db, MUVERA_OPTS).await?;

    // Well-formed corpus (including the exact-match `target`), all in L0.
    let corpus = build_corpus(8, 0xBAD_D1ED);
    insert_docs(&db, &corpus).await?;

    // Plus one doc whose single token has the WRONG dimension (DIM + 1).
    let bad_tokens = Value::List(vec![Value::List(
        (0..DIM + 1).map(|i| Value::Float(i as f64)).collect(),
    )]);
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Doc {title: $title, tokens: $toks})")
        .param("title", Value::String("malformed".to_string()))
        .param("toks", bad_tokens)
        .run()
        .await?;
    tx.commit().await?;

    // The critical step: the flush must NOT wedge on the malformed row.
    db.flush().await?;
    // A second flush still succeeds (the bad row never got stuck on the pending list).
    db.flush().await?;

    // The well-formed exact match is still retrievable and ranks first; the malformed doc
    // (NULL FDE under the Dot first stage) must not crowd out real results.
    let titles = query_titles(&db, 8).await?;
    assert_eq!(
        titles.first().map(String::as_str),
        Some("target"),
        "exact match must still rank first after a malformed row was skipped: {titles:?}"
    );
    Ok(())
}
