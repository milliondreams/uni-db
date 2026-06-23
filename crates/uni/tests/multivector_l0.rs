// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! L0-merge tests for native multi-vector (ColBERT / MaxSim) retrieval (issue #96).
//!
//! Native multi-vector queries used to see **flushed/indexed data only** —
//! unflushed L0 rows were invisible until an explicit `flush()`. These tests
//! prove the fix: `uni.vector.query` (and the inline `vector_similarity`
//! predicate) now treat Lance as a candidate generator over flushed data, union
//! its hits with live L0 vids, and re-score every candidate by exact MaxSim — so
//! recent writes are visible without a flush, with correct ordering, updates,
//! tombstones, and a similarity-based `threshold`.
//!
//! Token convention: the query is `[e0, e1]`. A doc with tokens `[e0, e1]` is the
//! unique MaxSim maximizer (score 2.0); `[e0, e0]` scores 1.0; orthogonal tokens
//! score 0.0.

use uni_db::{DataType, Uni, Value};

const DIM: usize = 8;

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

/// `i`-th standard basis vector (a clean, unit-norm token).
fn basis(i: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; DIM];
    v[i] = 1.0;
    v
}

/// A multi-vector as a `Value::List(Vec<Value::List<Float>>)`.
fn to_value(tokens: &[Vec<f32>]) -> Value {
    Value::List(
        tokens
            .iter()
            .map(|t| Value::List(t.iter().map(|&x| Value::Float(x as f64)).collect()))
            .collect(),
    )
}

/// A multi-vector as a Cypher list-of-lists literal (so reads need no params).
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

/// Query tokens: basis vectors e0 and e1. The doc whose tokens equal these is the
/// unique MaxSim maximizer (score 2.0).
fn query_tokens() -> Vec<Vec<f32>> {
    vec![basis(0), basis(1)]
}

/// An empty DB with the `Doc { title, tokens: List<Vector<DIM>> }` schema.
async fn doc_db() -> anyhow::Result<Uni> {
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
    Ok(db)
}

/// Write a batch of `(title, tokens)` docs in one transaction; flush iff `flush`.
async fn write(db: &Uni, docs: &[(&str, Vec<Vec<f32>>)], flush: bool) -> anyhow::Result<()> {
    let tx = db.session().tx().await?;
    for (title, tokens) in docs {
        tx.execute_with("CREATE (:Doc {title: $title, tokens: $toks})")
            .param("title", Value::String((*title).to_string()))
            .param("toks", to_value(tokens))
            .run()
            .await?;
    }
    tx.commit().await?;
    if flush {
        db.flush().await?;
    }
    Ok(())
}

/// Result titles of a `uni.vector.query` over the `tokens` column.
async fn titles(db: &Uni, k: usize, options: &str) -> anyhow::Result<Vec<String>> {
    let lit = cypher_lit(&query_tokens());
    let cypher = format!(
        "CALL uni.vector.query('Doc', 'tokens', {lit}, {k}, null, null, {options}) \
         YIELD node, score RETURN node.title AS title"
    );
    let res = db.session().query(&cypher).await?;
    Ok(res
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect())
}

/// Result `(title, score)` pairs of a `uni.vector.query` over the `tokens` column.
async fn titles_scores(db: &Uni, k: usize, options: &str) -> anyhow::Result<Vec<(String, f64)>> {
    let lit = cypher_lit(&query_tokens());
    let cypher = format!(
        "CALL uni.vector.query('Doc', 'tokens', {lit}, {k}, null, null, {options}) \
         YIELD node, score RETURN node.title AS title, score AS score"
    );
    let res = db.session().query(&cypher).await?;
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

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

/// Core fix: a query immediately after a write (NO flush, no index) sees the L0
/// rows and ranks the exact MaxSim maximizer first.
#[tokio::test]
async fn test_l0_only_no_flush_no_index() -> anyhow::Result<()> {
    let db = doc_db().await?;
    write(
        &db,
        &[
            ("noise0", vec![basis(4), basis(5)]),
            ("target", query_tokens()),
            ("noise1", vec![basis(2), basis(3)]),
            ("noise2", vec![basis(6), basis(7)]),
        ],
        false, // NO FLUSH — all data is in L0 only
    )
    .await?;

    let order = titles(&db, 5, "{}").await?;
    assert_eq!(
        order.len(),
        4,
        "all L0 docs should be visible without flush: {order:?}"
    );
    assert_eq!(
        order[0], "target",
        "MaxSim maximizer must rank first: {order:?}"
    );
    Ok(())
}

/// A flushed corpus plus a fresh L0-only doc: the L0 doc is unioned with the
/// flushed candidates and ranked correctly.
#[tokio::test]
async fn test_mixed_l0_and_flushed() -> anyhow::Result<()> {
    let db = doc_db().await?;
    // Flushed noise corpus, no target.
    write(
        &db,
        &[
            ("flushed0", vec![basis(4), basis(5)]),
            ("flushed1", vec![basis(2), basis(3)]),
            ("flushed2", vec![basis(6), basis(7)]),
        ],
        true,
    )
    .await?;
    // Target arrives in L0 only.
    write(&db, &[("target", query_tokens())], false).await?;

    let order = titles(&db, 5, "{}").await?;
    assert_eq!(
        order[0], "target",
        "L0 target must outrank flushed noise: {order:?}"
    );
    assert!(
        order.iter().any(|t| t.starts_with("flushed")),
        "flushed docs must still appear: {order:?}"
    );
    Ok(())
}

/// An in-place L0 update (last-writer-wins) is reflected: lowering a flushed
/// doc's score via `SET` in L0 demotes it below an unmodified flushed rival.
#[tokio::test]
async fn test_l0_update_last_writer_wins() -> anyhow::Result<()> {
    let db = doc_db().await?;
    write(
        &db,
        &[
            ("target", query_tokens()),          // flushed score 2.0
            ("rival", vec![basis(0), basis(0)]), // flushed score 1.0
        ],
        true,
    )
    .await?;

    // Demote target to orthogonal tokens in L0 (score 0.0), no flush.
    let tx = db.session().tx().await?;
    tx.execute_with("MATCH (d:Doc {title: 'target'}) SET d.tokens = $toks")
        .param("toks", to_value(&[basis(4), basis(5)]))
        .run()
        .await?;
    tx.commit().await?;

    let scored = titles_scores(&db, 5, "{}").await?;
    assert_eq!(
        scored[0].0, "rival",
        "rival (1.0) must now outrank the L0-demoted target (0.0): {scored:?}"
    );
    let target = scored
        .iter()
        .find(|(t, _)| t == "target")
        .expect("target present");
    assert!(
        target.1.abs() < 1e-5,
        "L0-updated target must score ~0, got {}",
        target.1
    );
    Ok(())
}

/// A doc deleted in L0 (no flush) disappears from results even though it is still
/// flushed in Lance.
#[tokio::test]
async fn test_l0_tombstone_hides_flushed_doc() -> anyhow::Result<()> {
    let db = doc_db().await?;
    write(
        &db,
        &[
            ("target", query_tokens()),
            ("rival", vec![basis(0), basis(0)]),
        ],
        true,
    )
    .await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (d:Doc {title: 'target'}) DETACH DELETE d")
        .await?;
    tx.commit().await?;

    let order = titles(&db, 5, "{}").await?;
    assert!(
        !order.iter().any(|t| t == "target"),
        "L0-tombstoned target must not appear: {order:?}"
    );
    assert_eq!(
        order,
        vec!["rival".to_string()],
        "only rival remains: {order:?}"
    );
    Ok(())
}

/// Delete one doc and create another in the same L0 generation (no flush): the
/// new doc is scored and the deleted one is gone — union and tombstone applied
/// together.
#[tokio::test]
async fn test_l0_delete_and_create_together() -> anyhow::Result<()> {
    let db = doc_db().await?;
    write(
        &db,
        &[
            ("target", query_tokens()),
            ("rival", vec![basis(0), basis(0)]),
        ],
        true,
    )
    .await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (d:Doc {title: 'target'}) DETACH DELETE d")
        .await?;
    tx.execute_with("CREATE (:Doc {title: $title, tokens: $toks})")
        .param("title", Value::String("fresh".to_string()))
        .param("toks", to_value(&query_tokens()))
        .run()
        .await?;
    tx.commit().await?;

    let order = titles(&db, 5, "{}").await?;
    assert_eq!(order[0], "fresh", "new L0 doc must rank first: {order:?}");
    assert!(
        !order.iter().any(|t| t == "target"),
        "deleted doc must be gone: {order:?}"
    );
    Ok(())
}

/// The ranking is identical whether queried before or after `flush()` — proving
/// the exact-MaxSim scale is consistent across the L0 and flushed paths.
#[tokio::test]
async fn test_flush_equivalence() -> anyhow::Result<()> {
    let db = doc_db().await?;
    let docs: Vec<(&str, Vec<Vec<f32>>)> = vec![
        ("target", query_tokens()),
        ("rival", vec![basis(0), basis(0)]),
        ("noise", vec![basis(4), basis(5)]),
    ];
    write(&db, &docs, false).await?;

    let before = titles(&db, 5, "{}").await?;
    db.flush().await?;
    let after = titles(&db, 5, "{}").await?;

    assert_eq!(before, after, "ordering must match pre/post flush");
    assert_eq!(before[0], "target", "target first either way: {before:?}");
    Ok(())
}

/// With an IVF_PQ index built over flushed data, an L0-only doc still surfaces:
/// Lance generates candidates from the index, the L0 doc is unioned in, and the
/// exact re-rank picks it.
#[tokio::test]
async fn test_index_plus_l0_merge() -> anyhow::Result<()> {
    let db = doc_db().await?;

    // Flushed noise corpus large enough to build/train PQ (>=256 tokens).
    let mut rng = Rng(0xD1CE_5EED);
    let noise: Vec<(String, Vec<Vec<f32>>)> = (0..120)
        .map(|i| (format!("doc{i}"), (0..3).map(|_| rng.unit()).collect()))
        .collect();
    let noise_refs: Vec<(&str, Vec<Vec<f32>>)> =
        noise.iter().map(|(t, v)| (t.as_str(), v.clone())).collect();
    write(&db, &noise_refs, true).await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE VECTOR INDEX tok_idx FOR (d:Doc) ON (d.tokens) \
         OPTIONS {type: 'ivf_pq', partitions: 4, sub_vectors: 4, num_bits: 8, metric: 'cosine'}",
    )
    .await?;
    tx.commit().await?;
    db.indexes().rebuild("Doc", false).await?;

    // Target arrives in L0 only (not in the index).
    write(&db, &[("target", query_tokens())], false).await?;

    let order = titles(&db, 5, "{nprobes: 4, refine_factor: 16}").await?;
    assert_eq!(
        order[0], "target",
        "L0 target must surface above indexed flushed candidates: {order:?}"
    );
    Ok(())
}

/// The inline `vector_similarity` predicate path also sees L0 data without flush.
#[tokio::test]
async fn test_inline_predicate_sees_l0() -> anyhow::Result<()> {
    let db = doc_db().await?;
    write(
        &db,
        &[
            ("target", query_tokens()),          // MaxSim 2.0
            ("rival", vec![basis(0), basis(0)]), // MaxSim 1.0
            ("noise", vec![basis(4), basis(5)]), // MaxSim 0.0
        ],
        false, // NO FLUSH
    )
    .await?;

    let lit = cypher_lit(&query_tokens());
    let cypher = format!(
        "MATCH (d:Doc) WHERE vector_similarity(d.tokens, {lit}) > 1.5 RETURN d.title AS title"
    );
    let res = db.session().query(&cypher).await?;
    let titles: Vec<String> = res
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect();
    assert_eq!(
        titles,
        vec!["target".to_string()],
        "only the MaxSim maximizer (in L0) should exceed the threshold: {titles:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Failure scenarios
// ---------------------------------------------------------------------------

/// A query whose token dimension differs from the stored L0 tokens is a hard
/// error (propagated from the in-process MaxSim re-score), not a silent miss.
#[tokio::test]
async fn test_l0_dimension_mismatch_errors() -> anyhow::Result<()> {
    let db = doc_db().await?;
    // L0-only corpus (no table exists → Lance is skipped, so the error must come
    // from the in-process MaxSim path).
    write(&db, &[("target", query_tokens())], false).await?;

    // Query with 3-dim tokens against the 8-dim column.
    let bad = cypher_lit(&[vec![1.0, 0.0, 0.0], vec![0.0, 1.0, 0.0]]);
    let cypher = format!(
        "CALL uni.vector.query('Doc', 'tokens', {bad}, 5) YIELD node RETURN node.title AS title"
    );
    let res = db.session().query(&cypher).await;
    assert!(res.is_err(), "dimension mismatch in L0 must error");
    Ok(())
}

/// A doc with an empty token list in L0 scores 0 and ranks last, without
/// crashing.
#[tokio::test]
async fn test_l0_empty_tokens_score_zero() -> anyhow::Result<()> {
    let db = doc_db().await?;
    write(&db, &[("target", query_tokens()), ("empty", vec![])], false).await?;

    let scored = titles_scores(&db, 5, "{}").await?;
    assert_eq!(scored[0].0, "target", "target leads: {scored:?}");
    let empty = scored
        .iter()
        .find(|(t, _)| t == "empty")
        .expect("empty present");
    assert!(
        empty.1.abs() < 1e-6,
        "empty-token doc must score 0, got {}",
        empty.1
    );
    Ok(())
}

/// A malformed (flat, non-nested) query against an L0 corpus errors.
#[tokio::test]
async fn test_l0_malformed_flat_query_errors() -> anyhow::Result<()> {
    let db = doc_db().await?;
    write(&db, &[("target", query_tokens())], false).await?;

    // A flat list [1.0, 0.0] is not a list-of-vectors.
    let cypher = "CALL uni.vector.query('Doc', 'tokens', [1.0, 0.0], 5) YIELD node RETURN node.title AS title";
    let res = db.session().query(cypher).await;
    assert!(
        res.is_err(),
        "flat (non-nested) multi-vector query must error"
    );
    Ok(())
}

/// The `threshold` argument is a *minimum similarity* (higher is better) for
/// multi-vector queries: it filters L0 docs below the floor.
#[tokio::test]
async fn test_l0_threshold_is_min_similarity() -> anyhow::Result<()> {
    let db = doc_db().await?;
    write(
        &db,
        &[
            ("target", query_tokens()),          // 2.0
            ("rival", vec![basis(0), basis(0)]), // 1.0
            ("noise", vec![basis(4), basis(5)]), // 0.0
        ],
        false,
    )
    .await?;

    // threshold = 1.5 → only target (2.0) qualifies.
    let lit = cypher_lit(&query_tokens());
    let cypher = format!(
        "CALL uni.vector.query('Doc', 'tokens', {lit}, 5, null, 1.5) \
         YIELD node, score RETURN node.title AS title"
    );
    let res = db.session().query(&cypher).await?;
    let order: Vec<String> = res
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect();
    assert_eq!(
        order,
        vec!["target".to_string()],
        "only docs at/above the similarity floor remain: {order:?}"
    );
    Ok(())
}
