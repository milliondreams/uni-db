// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! E2E correctness tests for the scored sparse-vector index (issue #95, set B/C).
//!
//! A `SparseVector` column + a sparse index scored by dot product, queried via
//! `uni.sparse.query`. The doc titled `"target"` has `emb == query`, so it is
//! the unique dot-product maximizer; the engine's reported `score` is the EXACT
//! `sparse_dot`, validated against an independent brute-force oracle over the
//! same corpus. Covers the backfill and create-before-ingest build paths, the
//! L0 (no-flush) union path, and L0 update / tombstone (MVCC) visibility.

use std::collections::{BTreeMap, HashMap};
use uni_db::{DataType, IndexType, Uni, Value};

const VOCAB: usize = 1000;

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

/// A sparse vector as parallel sorted-unique `(indices, values)`.
type Sparse = (Vec<u32>, Vec<f32>);

fn random_sparse(rng: &mut Rng, nnz: usize) -> Sparse {
    let mut m: BTreeMap<u32, f32> = BTreeMap::new();
    while m.len() < nnz {
        let t = rng.term();
        let w = rng.weight();
        m.insert(t, w);
    }
    (m.keys().copied().collect(), m.values().copied().collect())
}

/// The fixed query sparse vector (distinct terms, positive weights).
fn query_vec() -> Sparse {
    (vec![1, 5, 9, 42, 77], vec![1.0, 2.0, 3.0, 0.5, 1.5])
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

/// Deterministic corpus: doc `n/2` titled `target` has `emb == query` (the
/// unique dot maximizer); the rest are random sparse vectors. Returned so a test
/// can compute the oracle over the exact same data.
fn build_corpus(n: usize, seed: u64) -> Vec<(String, Sparse)> {
    let q = query_vec();
    let mut rng = Rng(seed);
    (0..n)
        .map(|i| {
            if i == n / 2 {
                ("target".to_string(), q.clone())
            } else {
                (format!("doc{i}"), random_sparse(&mut rng, 8))
            }
        })
        .collect()
}

async fn define_schema(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("emb", DataType::SparseVector { dimensions: VOCAB })
        .index("emb", IndexType::Sparse { dimensions: VOCAB })
        .apply()
        .await?;
    Ok(())
}

/// Schema with the sparse column but NO index (for create-before-ingest, where
/// the index is added separately).
async fn define_schema_no_index(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("emb", DataType::SparseVector { dimensions: VOCAB })
        .apply()
        .await?;
    Ok(())
}

async fn add_index(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .index("emb", IndexType::Sparse { dimensions: VOCAB })
        .apply()
        .await?;
    Ok(())
}

async fn insert_docs(db: &Uni, docs: &[(String, Sparse)], flush: bool) -> anyhow::Result<()> {
    let tx = db.session().tx().await?;
    for (title, sparse) in docs {
        tx.execute_with("CREATE (:Doc {title: $title, emb: $emb})")
            .param("title", Value::String(title.clone()))
            .param("emb", sv_value(sparse))
            .run()
            .await?;
    }
    tx.commit().await?;
    if flush {
        db.flush().await?;
    }
    Ok(())
}

/// Run `uni.sparse.query` for the standard query and return `(title, score)` in
/// engine rank order.
async fn query_results(db: &Uni, k: usize) -> anyhow::Result<Vec<(String, f64)>> {
    let q = query_vec();
    let rows = db
        .session()
        .query_with(
            "CALL uni.sparse.query('Doc', 'emb', $q, $k, null, null, {}) \
             YIELD node, score RETURN node.title AS title, score",
        )
        .param("q", sv_value(&q))
        .param("k", Value::Int(k as i64))
        .fetch_all()
        .await?;
    Ok(rows
        .iter()
        .map(|r| {
            (
                r.get::<String>("title").unwrap(),
                r.get::<f64>("score").unwrap(),
            )
        })
        .collect())
}

/// Assert engine results match the brute-force oracle: every returned doc
/// carries its EXACT dot score, results are descending, and the top score
/// equals the oracle maximum (the `target`).
fn assert_matches_oracle(engine: &[(String, f64)], corpus: &[(String, Sparse)]) {
    const EPS: f64 = 1e-3;
    let q = query_vec();
    let oracle: HashMap<&str, f64> = corpus
        .iter()
        .map(|(t, s)| (t.as_str(), sparse_dot_oracle(&q, s)))
        .collect();

    for (title, score) in engine {
        let want = oracle
            .get(title.as_str())
            .unwrap_or_else(|| panic!("engine returned a title not in the corpus: {title:?}"));
        assert!(
            (score - want).abs() < EPS,
            "exact dot-score mismatch for {title:?}: engine={score} oracle={want}"
        );
    }
    // Descending order.
    for w in engine.windows(2) {
        assert!(
            w[0].1 >= w[1].1 - EPS,
            "results not in descending score order: {engine:?}"
        );
    }
    // Top == oracle maximum (the target self-match).
    let oracle_max = oracle.values().cloned().fold(f64::MIN, f64::max);
    if let Some((_, top)) = engine.first() {
        assert!(
            (top - oracle_max).abs() < EPS,
            "top score {top} != oracle max {oracle_max}"
        );
    }
}

#[tokio::test]
async fn sparse_backfill_then_query_matches_oracle() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    define_schema_no_index(&db).await?;
    let corpus = build_corpus(60, 0xD1CE_5EED);
    insert_docs(&db, &corpus, true).await?; // flush so the backfill scan sees rows
    add_index(&db).await?; // backfill build path

    let results = query_results(&db, 10).await?;
    assert_eq!(
        results.first().map(|(t, _)| t.as_str()),
        Some("target"),
        "target (emb == query) must rank first: {results:?}"
    );
    assert_matches_oracle(&results, &corpus);
    Ok(())
}

#[tokio::test]
async fn sparse_create_before_ingest_matches_oracle() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?; // index created on empty label
    let corpus = build_corpus(60, 0xBEEF_F00D);
    insert_docs(&db, &corpus, true).await?; // flush populates the index incrementally

    let results = query_results(&db, 10).await?;
    assert_eq!(
        results.first().map(|(t, _)| t.as_str()),
        Some("target"),
        "flush-maintained index should retrieve the target: {results:?}"
    );
    assert_matches_oracle(&results, &corpus);
    Ok(())
}

#[tokio::test]
async fn sparse_l0_only_no_flush_matches_oracle() -> anyhow::Result<()> {
    // No flush: all rows live in L0. The query path must union L0 candidates and
    // re-score them, so results are correct without an index dataset on disk.
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    let corpus = build_corpus(40, 0x0A0A_0A0A);
    insert_docs(&db, &corpus, false).await?;

    let results = query_results(&db, 10).await?;
    assert_eq!(
        results.first().map(|(t, _)| t.as_str()),
        Some("target"),
        "L0-only query must find the target: {results:?}"
    );
    assert_matches_oracle(&results, &corpus);
    Ok(())
}

#[tokio::test]
async fn sparse_flush_equivalence() -> anyhow::Result<()> {
    // The ranking must be identical before and after a flush (scale consistency
    // across the L0 and flushed-index paths).
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    let corpus = build_corpus(50, 0xF1F1_F1F1);
    insert_docs(&db, &corpus, false).await?;
    let before = query_results(&db, 10).await?;
    db.flush().await?;
    let after = query_results(&db, 10).await?;

    let names_before: Vec<&str> = before.iter().map(|(t, _)| t.as_str()).collect();
    let names_after: Vec<&str> = after.iter().map(|(t, _)| t.as_str()).collect();
    assert_eq!(
        names_before, names_after,
        "ranking changed across flush: {names_before:?} vs {names_after:?}"
    );
    assert_matches_oracle(&after, &corpus);
    Ok(())
}

#[tokio::test]
async fn sparse_l0_update_last_writer_wins() -> anyhow::Result<()> {
    // Flush a corpus, then in L0 overwrite `target`'s emb with a vector that no
    // longer matches the query. The stale flushed posting still makes it a
    // candidate, but the re-score uses the fresh L0 value → it drops out of top.
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    let corpus = build_corpus(30, 0xABCD_1234);
    insert_docs(&db, &corpus, true).await?;

    // Overwrite target with a disjoint sparse vector (no query terms).
    let tx = db.session().tx().await?;
    tx.execute_with("MATCH (d:Doc {title: 'target'}) SET d.emb = $emb")
        .param(
            "emb",
            Value::SparseVector {
                indices: vec![500, 600, 700],
                values: vec![1.0, 1.0, 1.0],
            },
        )
        .run()
        .await?;
    tx.commit().await?;

    let results = query_results(&db, 10).await?;
    let target_score = results.iter().find(|(t, _)| t == "target").map(|(_, s)| *s);
    // target now shares no terms with the query → score 0 (or absent from top-k).
    assert!(
        target_score.map(|s| s.abs() < 1e-6).unwrap_or(true),
        "after L0 update target should no longer match the query: {target_score:?}"
    );
    Ok(())
}

#[tokio::test]
async fn sparse_l0_tombstone_hides_flushed_doc() -> anyhow::Result<()> {
    // Flush a corpus, then delete `target` in L0. A tombstone must hide it from
    // results even though its flushed posting still matches the query.
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    let corpus = build_corpus(30, 0x5555_AAAA);
    insert_docs(&db, &corpus, true).await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (d:Doc {title: 'target'}) DELETE d")
        .await?;
    tx.commit().await?;

    let results = query_results(&db, 10).await?;
    assert!(
        !results.iter().any(|(t, _)| t == "target"),
        "tombstoned target must not appear in results: {results:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Set G — restart / reopen durability
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sparse_persists_across_reopen() -> anyhow::Result<()> {
    // Build + flush the index, drop the db, reopen the same on-disk path, and
    // assert the query returns an identical, oracle-exact ranking — the flushed
    // postings dataset and schema-registered index survive a restart.
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap();
    let corpus = build_corpus(60, 0xD00D_FEED);

    let before = {
        let db = Uni::open(path).build().await?;
        define_schema(&db).await?;
        insert_docs(&db, &corpus, true).await?;
        let r = query_results(&db, 10).await?;
        assert_eq!(r.first().map(|(t, _)| t.as_str()), Some("target"));
        assert_matches_oracle(&r, &corpus);
        drop(db);
        r
    };

    // Reopen: WAL/manifest recovery restores the flushed dataset + index config.
    let db = Uni::open(path).build().await?;
    let after = query_results(&db, 10).await?;
    let names_before: Vec<&str> = before.iter().map(|(t, _)| t.as_str()).collect();
    let names_after: Vec<&str> = after.iter().map(|(t, _)| t.as_str()).collect();
    assert_eq!(
        names_before, names_after,
        "ranking changed across reopen: {names_before:?} vs {names_after:?}"
    );
    assert_matches_oracle(&after, &corpus);
    Ok(())
}

#[tokio::test]
async fn sparse_wal_replay_after_reopen_unflushed_delta() -> anyhow::Result<()> {
    // Durability of UNFLUSHED sparse writes through the WAL (tagged-msgpack CV
    // codec, TAG_SPARSE_VECTOR). The engine requires a snapshot manifest to
    // reopen, so we flush a base batch first (creates the manifest), then commit
    // a second batch — including `target` — WITHOUT flushing, then drop + reopen.
    //
    // Two guarantees are asserted:
    //   1. The sparse VALUE survives WAL recovery — a plain MATCH finds `target`
    //      with its sparse vector intact (this is the sparse-specific concern:
    //      the CV codec round-trips through the WAL).
    //   2. Secondary indexes are NOT maintained during WAL recovery (a universal
    //      engine property shared by dense/inverted/FTS indexes — recovered-but-
    //      unflushed rows are flushed to L1 without index maintenance). An
    //      explicit index rebuild restores sparse-query consistency.
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap();

    // Base batch: random docs only (no target), flushed → creates the manifest.
    let mut rng = Rng(0x1234_5678_9ABC_DEF0);
    let base: Vec<(String, Sparse)> = (0..20)
        .map(|i| (format!("base{i}"), random_sparse(&mut rng, 8)))
        .collect();
    // Delta batch: the target (emb == query) + a few randoms, committed unflushed.
    let mut delta: Vec<(String, Sparse)> = vec![("target".to_string(), query_vec())];
    for i in 0..5 {
        delta.push((format!("delta{i}"), random_sparse(&mut rng, 8)));
    }

    {
        let db = Uni::open(path).build().await?;
        define_schema(&db).await?;
        insert_docs(&db, &base, true).await?; // flush → manifest
        insert_docs(&db, &delta, false).await?; // commit only, in WAL
        let r = query_results(&db, 10).await?;
        assert_eq!(r.first().map(|(t, _)| t.as_str()), Some("target"));
        drop(db);
    }

    let db = Uni::open(path).build().await?;

    // The WAL now serializes mutation values through the explicit CV codec, so
    // the unflushed delta — including `target`'s sparse vector — recovers as a
    // genuine `Value::SparseVector` (not a degraded untagged-serde `Map`).
    // Secondary indexes are not maintained during WAL recovery (a universal
    // engine property), so rebuild the index, then verify the sparse query
    // returns `target` first with EXACT oracle scores — which is only possible
    // if the recovered weights survived intact through the WAL.
    db.indexes().rebuild("Doc", false).await?;
    let after = query_results(&db, 10).await?;
    assert_eq!(
        after.first().map(|(t, _)| t.as_str()),
        Some("target"),
        "after WAL recovery + rebuild, recovered sparse data must be queryable: {after:?}"
    );
    let full: Vec<(String, Sparse)> = base.into_iter().chain(delta).collect();
    assert_matches_oracle(&after, &full);
    Ok(())
}

// ---------------------------------------------------------------------------
// Set D — MVCC snapshot isolation
// ---------------------------------------------------------------------------

/// Run `uni.sparse.query` inside a transaction's pinned snapshot.
async fn query_results_tx(tx: &uni_db::Transaction, k: usize) -> anyhow::Result<Vec<String>> {
    let q = query_vec();
    let rows = tx
        .query_with(
            "CALL uni.sparse.query('Doc', 'emb', $q, $k, null, null, {}) \
             YIELD node, score RETURN node.title AS title",
        )
        .param("q", sv_value(&q))
        .param("k", Value::Int(k as i64))
        .fetch_all()
        .await?;
    Ok(rows
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sparse_snapshot_isolates_reader_from_concurrent_insert() -> anyhow::Result<()> {
    // A reader transaction pins its snapshot at begin. A concurrent writer
    // inserts a new doc that matches the query and commits. The reader, querying
    // within its pinned snapshot, must NOT see the new doc.
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    let corpus = build_corpus(20, 0x5A4B_3C2D_1E0F_9876);
    insert_docs(&db, &corpus, true).await?;

    let s_r = db.session();
    let tx_r = s_r.tx().await?;
    let before = query_results_tx(&tx_r, 50).await?;
    assert!(
        before.contains(&"target".to_string()),
        "snapshot sees the seed corpus"
    );

    // Concurrent writer inserts a brand-new matching doc and commits.
    {
        let s_w = db.session();
        let tx_w = s_w.tx().await?;
        tx_w.execute_with("CREATE (:Doc {title: $title, emb: $emb})")
            .param("title", Value::String("late_arrival".to_string()))
            .param("emb", sv_value(&query_vec()))
            .run()
            .await?;
        tx_w.commit().await?;
    }

    // The reader's pinned snapshot must not surface the post-begin insert.
    let after = query_results_tx(&tx_r, 50).await?;
    assert!(
        !after.contains(&"late_arrival".to_string()),
        "reader snapshot must be isolated from the concurrent insert: {after:?}"
    );

    // A fresh session (live view) DOES see it.
    let live = query_results(&db, 50).await?;
    assert!(
        live.iter().any(|(t, _)| t == "late_arrival"),
        "live view should see the committed insert: {live:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Result-surface — projecting a sparse property in RETURN
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sparse_property_projection_in_return() -> anyhow::Result<()> {
    // `RETURN d.emb` must materialise the sparse `Struct{indices,values}` result
    // column (not fall back to a Utf8 string column). Covers both the L0 and the
    // flushed read paths.
    let db = Uni::temporary().build().await?;
    define_schema(&db).await?;
    let (qi, qv) = query_vec();
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Doc {title: 'p', emb: $emb})")
        .param("emb", sv_value(&(qi.clone(), qv.clone())))
        .run()
        .await?;
    tx.commit().await?;

    // Read once from L0, then again after a flush — both must round-trip.
    for flush in [false, true] {
        if flush {
            db.flush().await?;
        }
        let rows = db
            .session()
            .query("MATCH (d:Doc {title: 'p'}) RETURN d.emb AS emb")
            .await?;
        assert_eq!(rows.rows().len(), 1);
        match rows.rows()[0].value("emb") {
            Some(Value::SparseVector { indices, values }) => {
                assert_eq!(indices, &qi, "projected sparse indices (flush={flush})");
                assert_eq!(values, &qv, "projected sparse values (flush={flush})");
            }
            other => panic!("expected projected SparseVector (flush={flush}), got {other:?}"),
        }
    }
    Ok(())
}
