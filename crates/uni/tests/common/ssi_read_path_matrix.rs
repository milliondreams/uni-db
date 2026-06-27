// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The read-path conflict matrix: systematic proof that *every* read operator
//! feeds the SSI read-set.
//!
//! The G1 (vector-KNN) hole existed because read-set recording was wired
//! operator-by-operator, ad hoc. This file closes that class: for each read
//! operator we assert the antidependency invariant directly —
//!
//! > a read-write transaction that reads item X via the operator, then writes
//! > (anything) after a concurrent transaction commits a write to X, MUST abort.
//!
//! and, where the read is targeted, the precision invariant —
//!
//! > the same transaction must NOT abort when a *disjoint* item is concurrently
//! > written (no false conflict from over-recording).
//!
//! A failing cell is not a flaky test — it is an unrecorded read path, i.e. a
//! silent serializability hole exactly like G1. Vector-KNN (the G1 fix) and the
//! deliberately-excluded virtual/catalog scans (G2) live in dedicated tests at
//! the bottom.

use anyhow::Result;
use uni_db::{DataType, IndexType, Uni, Value, VectorAlgo, VectorIndexCfg, VectorMetric};

use crate::ssi_support::schedule::{assert_committed, assert_serialization_conflict};

/// Schema `T(id, val)` + `S(id, val)` (a disjoint label) + edge `R:T->T`,
/// seeded: T{x,y,z}=0, S{s}=0, and `(x)-[:R]->(y)`.
async fn matrix_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("T")
        .property("id", DataType::String)
        .property("val", DataType::Int)
        .done()
        .label("S")
        .property("id", DataType::String)
        .property("val", DataType::Int)
        .done()
        .edge_type("R", &["T"], &["T"])
        .done()
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (x:T {id: 'x', val: 0})-[:R]->(y:T {id: 'y', val: 0})")
        .await?;
    tx.execute("CREATE (:T {id: 'z', val: 0})").await?;
    tx.execute("CREATE (:S {id: 's', val: 0})").await?;
    tx.commit().await?;
    Ok(db)
}

/// `read` is evaluated in tx_a; then a separate transaction commits
/// `conflict_write`; then tx_a writes a fresh sentinel and commits. Because tx_a
/// read an item the concurrent transaction wrote, the commit MUST abort.
async fn assert_read_conflicts(read: &str, conflict_write: &str) -> Result<()> {
    let db = matrix_db().await?;
    let sa = db.session();
    let ta = sa.tx().await?;
    ta.query(read).await?;

    {
        let tb = db.session().tx().await?;
        tb.execute(conflict_write).await?;
        assert_committed(tb.commit().await);
    }

    ta.execute("CREATE (:T {id: 'sentinel_c', val: 0})").await?;
    assert_serialization_conflict(ta.commit().await);
    Ok(())
}

/// As above, but the concurrent transaction writes a *disjoint* item the read
/// did not touch, so tx_a must commit cleanly (no false conflict).
async fn assert_read_no_false_abort(read: &str, disjoint_write: &str) -> Result<()> {
    let db = matrix_db().await?;
    let sa = db.session();
    let ta = sa.tx().await?;
    ta.query(read).await?;

    {
        let tb = db.session().tx().await?;
        tb.execute(disjoint_write).await?;
        assert_committed(tb.commit().await);
    }

    ta.execute("CREATE (:T {id: 'sentinel_ok', val: 0})")
        .await?;
    assert_committed(ta.commit().await);
    Ok(())
}

// ── Vertex read operators ────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyed_point_read() -> Result<()> {
    assert_read_conflicts(
        "MATCH (n:T {id: 'x'}) RETURN n.val",
        "MATCH (n:T {id: 'x'}) SET n.val = 1",
    )
    .await?;
    assert_read_no_false_abort(
        "MATCH (n:T {id: 'x'}) RETURN n.val",
        "MATCH (n:T {id: 'y'}) SET n.val = 1",
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filtered_non_key_scan() -> Result<()> {
    // A residual (non-key) predicate; read-set recorded post-filter, so only the
    // surviving row x is recorded.
    assert_read_conflicts(
        "MATCH (n:T) WHERE n.val = 0 AND n.id = 'x' RETURN n.id",
        "MATCH (n:T {id: 'x'}) SET n.val = 1",
    )
    .await?;
    assert_read_no_false_abort(
        "MATCH (n:T) WHERE n.val = 0 AND n.id = 'x' RETURN n.id",
        "MATCH (n:T {id: 'y'}) SET n.val = 1",
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_label_scan() -> Result<()> {
    // Reads every T (x, y, z): any T write conflicts; a disjoint-label S write
    // does not (S was never scanned).
    assert_read_conflicts(
        "MATCH (n:T) RETURN n.id",
        "MATCH (n:T {id: 'z'}) SET n.val = 1",
    )
    .await?;
    assert_read_no_false_abort(
        "MATCH (n:T) RETURN n.id",
        "MATCH (n:S {id: 's'}) SET n.val = 1",
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aggregation_input() -> Result<()> {
    // The aggregate consumes every T row; those reads must still be recorded.
    assert_read_conflicts(
        "MATCH (n:T) RETURN sum(n.val) AS s",
        "MATCH (n:T {id: 'x'}) SET n.val = 1",
    )
    .await?;
    assert_read_no_false_abort(
        "MATCH (n:T) RETURN sum(n.val) AS s",
        "MATCH (n:S {id: 's'}) SET n.val = 1",
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn with_pipeline() -> Result<()> {
    assert_read_conflicts(
        "MATCH (n:T {id: 'x'}) WITH n WHERE n.val = 0 RETURN n.id",
        "MATCH (n:T {id: 'x'}) SET n.val = 1",
    )
    .await?;
    assert_read_no_false_abort(
        "MATCH (n:T {id: 'x'}) WITH n WHERE n.val = 0 RETURN n.id",
        "MATCH (n:T {id: 'y'}) SET n.val = 1",
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn union_branches() -> Result<()> {
    // Both arms read; a write to either branch's item conflicts.
    assert_read_conflicts(
        "MATCH (n:T {id: 'x'}) RETURN n.id AS id UNION MATCH (n:T {id: 'y'}) RETURN n.id AS id",
        "MATCH (n:T {id: 'y'}) SET n.val = 1",
    )
    .await?;
    assert_read_no_false_abort(
        "MATCH (n:T {id: 'x'}) RETURN n.id AS id UNION MATCH (n:T {id: 'y'}) RETURN n.id AS id",
        "MATCH (n:T {id: 'z'}) SET n.val = 1",
    )
    .await
}

// ── Traversal / edge read operators ──────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_one_hop_traversal() -> Result<()> {
    // Traversal records the neighbour b (=y) even though only a.id is returned.
    assert_read_conflicts(
        "MATCH (a:T {id: 'x'})-[:R]->(b) RETURN a.id",
        "MATCH (n:T {id: 'y'}) SET n.val = 1",
    )
    .await?;
    assert_read_no_false_abort(
        "MATCH (a:T {id: 'x'})-[:R]->(b) RETURN a.id",
        "MATCH (n:T {id: 'z'}) SET n.val = 1",
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn variable_length_traversal() -> Result<()> {
    assert_read_conflicts(
        "MATCH (a:T {id: 'x'})-[:R*1..2]->(b) RETURN b.id",
        "MATCH (n:T {id: 'y'}) SET n.val = 1",
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn optional_match() -> Result<()> {
    // The optional side reaches y; writing y must conflict.
    assert_read_conflicts(
        "MATCH (n:T {id: 'x'}) OPTIONAL MATCH (n)-[:R]->(m) RETURN n.id, m.id",
        "MATCH (n:T {id: 'y'}) SET n.val = 1",
    )
    .await
}

// ── G1: vector-KNN read records its matches ──────────────────────────────────

/// G1 regression: a read through a vector-KNN search records the matched vids,
/// so a concurrent write to a matched vertex makes the reader abort. Before the
/// fix the KNN exec was not wrapped in `ReadSetRecordingExec`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vector_knn_records_matches() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Doc")
        .property("id", DataType::String)
        .property("val", DataType::Int)
        .vector("embedding", 2)
        .index(
            "embedding",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::Cosine,
                embedding: None,
            }),
        )
        .done()
        .apply()
        .await?;
    {
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {id: 'd1', val: 0, embedding: [1.0, 0.0]})")
            .await?;
        tx.execute("CREATE (:Doc {id: 'd2', val: 0, embedding: [0.0, 1.0]})")
            .await?;
        tx.commit().await?;
    }

    let sa = db.session();
    let ta = sa.tx().await?;
    // KNN read: the nearest Doc to [1,0] is d1 — its vid must enter tx_a's
    // read-set via the `GraphVectorKnnExec` wrap added by the G1 fix.
    ta.query(
        "MATCH (d:Doc) RETURN d.id AS id \
         ORDER BY similar_to(d.embedding, [1.0, 0.0]) DESC LIMIT 1",
    )
    .await?;

    {
        let tb = db.session().tx().await?;
        tb.execute("MATCH (d:Doc {id: 'd1'}) SET d.val = 1").await?;
        assert_committed(tb.commit().await);
    }

    ta.execute("CREATE (:Doc {id: 'sentinel', val: 0, embedding: [0.5, 0.5]})")
        .await?;
    assert_serialization_conflict(ta.commit().await);
    Ok(())
}

// ── G1-class: the sparse-query procedure records its matches ──────────────────

const SPARSE_VOCAB: usize = 1000;

/// The query sparse vector. `d1` is seeded with this exact vector (the unique
/// dot-product maximizer); `d2` shares no terms, so it never enters a top-k.
fn sparse_query() -> Value {
    Value::SparseVector {
        indices: vec![1, 5, 9],
        values: vec![1.0, 2.0, 3.0],
    }
}

/// Schema `Doc(id, val, emb: SparseVector)` + sparse index and a disjoint label
/// `Other(id, val)` that the sparse query never scans. Seeded with a strong
/// match `d1` (`emb == query`), a term-disjoint `d2`, and an `Other` node `o1`.
async fn sparse_matrix_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Doc")
        .property("id", DataType::String)
        .property("val", DataType::Int)
        .property(
            "emb",
            DataType::SparseVector {
                dimensions: SPARSE_VOCAB,
            },
        )
        .index("emb", IndexType::sparse(SPARSE_VOCAB))
        .done()
        .label("Other")
        .property("id", DataType::String)
        .property("val", DataType::Int)
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Doc {id: 'd1', val: 0, emb: $emb})")
        .param("emb", sparse_query())
        .run()
        .await?;
    tx.execute_with("CREATE (:Doc {id: 'd2', val: 0, emb: $emb})")
        .param(
            "emb",
            Value::SparseVector {
                indices: vec![100, 200],
                values: vec![1.0, 1.0],
            },
        )
        .run()
        .await?;
    tx.execute("CREATE (:Other {id: 'o1', val: 0})").await?;
    tx.commit().await?;
    Ok(db)
}

/// A `uni.sparse.query` read records the matched vids, so a concurrent write to
/// a matched vertex makes the reader abort. The procedure reaches matches
/// through `sparse_rerank`, whose MVCC-aware property fetch records every
/// candidate it reads via `record_vertex_read` (`l0_visibility.rs`) — the
/// storage-layer read-set path, not a DataFusion `ReadSetRecordingExec` wrap.
/// This locks that antidependency invariant in for the procedure read path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sparse_query_records_matches() -> Result<()> {
    let db = sparse_matrix_db().await?;
    let sa = db.session();
    let ta = sa.tx().await?;
    // Top-1 sparse match to the query is `d1` (emb == query) — its vid must
    // enter tx_a's read-set via the procedure-call read-set wrap.
    ta.query_with(
        "CALL uni.sparse.query('Doc', 'emb', $q, 1, null, null, {}) \
         YIELD node, score RETURN node.id AS id",
    )
    .param("q", sparse_query())
    .fetch_all()
    .await?;

    {
        let tb = db.session().tx().await?;
        tb.execute("MATCH (n:Doc {id: 'd1'}) SET n.val = 1").await?;
        assert_committed(tb.commit().await);
    }

    ta.execute_with("CREATE (:Doc {id: 'sentinel', val: 0, emb: $emb})")
        .param("emb", sparse_query())
        .run()
        .await?;
    assert_serialization_conflict(ta.commit().await);
    Ok(())
}

/// Precision (label-level): a concurrent write to a vertex of a *disjoint
/// label* the sparse query never scanned (`Other`) must NOT abort the reader.
/// The query's candidate scan is scoped to the `Doc` label, so an `Other` write
/// is no antidependency — mirrors `full_label_scan`'s disjoint-`S` case.
///
/// (A write to the term-disjoint same-label `d2` *would* conflict on this
/// brute-force L0 path: a top-k scan genuinely ranks every `Doc` it reads, so
/// the result depends on each — a path-specific over-approximation, hence not
/// asserted here.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sparse_query_disjoint_label_no_false_abort() -> Result<()> {
    let db = sparse_matrix_db().await?;
    let sa = db.session();
    let ta = sa.tx().await?;
    ta.query_with(
        "CALL uni.sparse.query('Doc', 'emb', $q, 1, null, null, {}) \
         YIELD node, score RETURN node.id AS id",
    )
    .param("q", sparse_query())
    .fetch_all()
    .await?;

    {
        let tb = db.session().tx().await?;
        tb.execute("MATCH (n:Other {id: 'o1'}) SET n.val = 1")
            .await?;
        assert_committed(tb.commit().await);
    }

    ta.execute_with("CREATE (:Doc {id: 'sentinel_ok', val: 0, emb: $emb})")
        .param("emb", sparse_query())
        .run()
        .await?;
    assert_committed(ta.commit().await);
    Ok(())
}
