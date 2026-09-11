// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Emission proofs for operators that had none (#177).
//!
//! [`super::registry`] ratchets down the number of physical operators nothing
//! asserts. These are the retrofits.
//!
//! # Why these are batched here rather than beside each feature
//!
//! The retrofit recipe in [`super`] says to write a proof next to the feature it
//! belongs to, and that remains right for an operator whose *guard conditions*
//! are the interesting part — `VidLookupJoinExec` earns its place beside the
//! join tests because the fixture shape is what makes it fire or fall back.
//!
//! The operators here are the opposite case: a label scan emits
//! `GraphScanExec`, `UNWIND` emits `GraphUnwindExec`. There is no guard to get
//! wrong and no fallback to be confused with, so the proof carries no
//! feature-specific knowledge and one fixture serves all of them. Splitting
//! them across fifteen files would spread the same three lines fifteen ways
//! without making any of them easier to read. #177 says as much: "Batching is
//! fine and probably preferable — many operators can be proven by the same
//! fixture with different query shapes."
//!
//! # These prove emission, not correctness
//!
//! An `assert_plan_uses` says the operator ran. It says nothing about whether
//! it ran *correctly* — that is what the feature suites' result assertions are
//! for, and neither substitutes for the other. What this file buys is the thing
//! a result assertion structurally cannot buy: if one of these operators stops
//! being emitted, something goes red.
//!
//! # Negative twins
//!
//! The recipe makes a twin mandatory for an operator with a silent fallback.
//! Most operators here have none, but a twin is still worth writing wherever
//! two operators are near-neighbours that a planner change could confuse, and
//! that is where the ones below are spent:
//!
//! * `GraphTraverseExec` vs `GraphTraverseMainExec` — chosen by whether the
//!   edge type is *declared*, which is invisible in the query text.
//! * The same split for the two variable-length traversals.
//! * `MutationSetExec` vs the other four mutation names, which share one `ty`
//!   and differ only in `display_name`.
//!
//! Those twins are the load-bearing ones: the names are prefixes of each other
//! (`GraphTraverseExec` of `GraphTraverseMainExec`), so a matcher that ever
//! regressed to `contains` would vouch for the wrong operator. `plan_shape`
//! matches on exact equality and `matching_is_exact_not_substring` pins it —
//! these twins are the end-to-end counterpart of that unit test.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, IndexType, Uni, Value, VectorAlgo, VectorIndexCfg, VectorMetric};

use uni_query::plan_shape::{assert_avoids, assert_uses};

use super::{assert_plan_avoids, assert_plan_uses, tx_plan_ops};

/// Rows per label. Small: these assert plan shape, not throughput.
const N: i64 = 8;

/// A graph with one declared edge type and one deliberately undeclared, an
/// external id, and a `linked` property holding a real vid.
///
/// The declared/undeclared pair is what lets a single fixture prove both
/// `GraphTraverseExec` and `GraphTraverseMainExec`: the planner picks between
/// them on whether the edge type is in the schema, so the *same* traversal
/// text over a different edge type routes to the other operator.
async fn fixture() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Src")
        .property_nullable("k", DataType::Int)
        .property_nullable("linked", DataType::Int)
        .done()
        .label("Dst")
        .property_nullable("k", DataType::Int)
        .done()
        .edge_type("R", &["Src"], &["Dst"])
        .done()
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    // `ext_id` is a built-in system column on every vertex table, not a
    // declared property — writing it is what makes the ext-id anchor reachable.
    tx.query_with("UNWIND range(0, $n - 1) AS i CREATE (:Src {k: i, ext_id: 'e' + toString(i)})")
        .param("n", Value::Int(N))
        .fetch_all()
        .await?;
    tx.query_with("UNWIND range(0, $n - 1) AS i CREATE (:Dst {k: i})")
        .param("n", Value::Int(N))
        .fetch_all()
        .await?;
    tx.query("MATCH (a:Src), (b:Dst) WHERE a.k = b.k CREATE (a)-[:R]->(b)")
        .await?;
    // An edge of a type the schema does not declare, so the schemaless
    // traversal has something to walk.
    tx.query("MATCH (a:Src {k: 0}), (b:Dst {k: 1}) CREATE (a)-[:UNDECLARED]->(b)")
        .await?;
    // An unconnected vertex, so the DELETE proof has something it is allowed to
    // remove: a plain `DELETE` on a node that still has relationships is
    // refused outright by the DeleteConnectedNode constraint, and never reaches
    // the mutation operator at all.
    tx.query("CREATE (:Dst {k: 999})").await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

// ── Scans ──────────────────────────────────────────────────────────────────

/// A labelled scan emits the scan operator.
#[tokio::test]
async fn a_labelled_match_runs_the_graph_scan() -> Result<()> {
    let db = fixture().await?;
    assert_plan_uses(&db.session(), "MATCH (n:Src) RETURN n.k", "GraphScanExec").await;
    Ok(())
}

// ── Traversals ─────────────────────────────────────────────────────────────

/// A single hop over a **declared** edge type runs the schema'd traversal.
#[tokio::test]
async fn a_declared_single_hop_runs_the_graph_traverse() -> Result<()> {
    let db = fixture().await?;
    assert_plan_uses(
        &db.session(),
        "MATCH (a:Src)-[:R]->(b:Dst) RETURN b.k",
        "GraphTraverseExec",
    )
    .await;
    Ok(())
}

/// A single hop over an **undeclared** edge type runs the schemaless traversal.
///
/// The twin of the test above, and the reason both exist: the query text is the
/// same shape and only the schema differs, so this is the pair that would catch
/// the planner routing every traversal to one operator.
#[tokio::test]
async fn an_undeclared_single_hop_runs_the_main_traverse_instead() -> Result<()> {
    let db = fixture().await?;
    let session = db.session();
    const Q: &str = "MATCH (a:Src)-[:UNDECLARED]->(b) RETURN b";
    assert_plan_uses(&session, Q, "GraphTraverseMainExec").await;
    assert_plan_avoids(&session, Q, "GraphTraverseExec").await;
    Ok(())
}

/// The declared hop must *not* take the schemaless operator.
///
/// Stated separately from the positive assertion because these two are exact
/// prefixes of one another — this is the assertion that goes red if matching
/// ever loosens to a substring test.
#[tokio::test]
async fn a_declared_single_hop_avoids_the_main_traverse() -> Result<()> {
    let db = fixture().await?;
    assert_plan_avoids(
        &db.session(),
        "MATCH (a:Src)-[:R]->(b:Dst) RETURN b.k",
        "GraphTraverseMainExec",
    )
    .await;
    Ok(())
}

/// A declared variable-length hop runs the schema'd variable-length traversal.
#[tokio::test]
async fn a_declared_variable_length_hop_runs_the_vlp_traverse() -> Result<()> {
    let db = fixture().await?;
    assert_plan_uses(
        &db.session(),
        "MATCH (a:Src)-[:R*1..2]->(b:Dst) RETURN b.k",
        "GraphVariableLengthTraverseExec",
    )
    .await;
    Ok(())
}

/// An undeclared variable-length hop runs the schemaless one — the same
/// declared/undeclared split as the single-hop pair, on the other operator.
#[tokio::test]
async fn an_undeclared_variable_length_hop_runs_the_main_vlp_traverse() -> Result<()> {
    let db = fixture().await?;
    let session = db.session();
    const Q: &str = "MATCH (a:Src)-[:UNDECLARED*1..2]->(b) RETURN b";
    assert_plan_uses(&session, Q, "GraphVariableLengthTraverseMainExec").await;
    assert_plan_avoids(&session, Q, "GraphVariableLengthTraverseExec").await;
    Ok(())
}

// ── Row-shaping ────────────────────────────────────────────────────────────

/// `UNWIND` emits the unwind operator.
#[tokio::test]
async fn an_unwind_runs_the_graph_unwind() -> Result<()> {
    let db = fixture().await?;
    assert_plan_uses(
        &db.session(),
        "UNWIND [1, 2, 3] AS i RETURN i",
        "GraphUnwindExec",
    )
    .await;
    Ok(())
}

/// `OPTIONAL MATCH` with a predicate on the optional side runs the
/// null-preserving filter rather than a plain filter.
///
/// This one *is* a correctness-shaped choice: a plain filter over an
/// OPTIONAL MATCH would drop the null rows the clause exists to preserve, and
/// the result difference is visible only when some row fails to match.
#[tokio::test]
async fn an_optional_match_predicate_runs_the_optional_filter() -> Result<()> {
    let db = fixture().await?;
    assert_plan_uses(
        &db.session(),
        "MATCH (a:Src) OPTIONAL MATCH (a)-[:R]->(b:Dst) WHERE b.k > 1 RETURN a.k, b.k",
        "OptionalFilterExec",
    )
    .await;
    Ok(())
}

// ── Mutations ──────────────────────────────────────────────────────────────
//
// `MutationExec` reports one of five `display_name`s, and the registry gives
// each its own row: proving SET fires says nothing about DETACH DELETE. Each
// test below therefore asserts its own name, and the SET test additionally
// asserts the other four are absent — the one place where a display-name
// regression would otherwise let one proof vouch for all five.
//
// These profile through a **transaction**, because `Session::query` refuses a
// mutation clause outright ("Session.query() is read-only"). A mutation
// operator is therefore unreachable from the read-side helper, which is why
// `tx_plan_ops` exists.

/// `CREATE` runs the create mutation.
#[tokio::test]
async fn a_create_runs_the_create_mutation() -> Result<()> {
    let db = fixture().await?;
    let session = db.session();
    let tx = session.tx().await?;
    const Q: &str = "CREATE (n:Src {k: 99})";
    let ops = tx_plan_ops(&tx, Q).await;
    assert_uses(&ops, "MutationCreateExec", Q);
    tx.commit().await?;
    Ok(())
}

/// `SET` runs the set mutation, and none of its four siblings.
#[tokio::test]
async fn a_set_runs_the_set_mutation_and_not_its_siblings() -> Result<()> {
    let db = fixture().await?;
    let session = db.session();
    let tx = session.tx().await?;
    const Q: &str = "MATCH (n:Src {k: 0}) SET n.k = 1000";
    let ops = tx_plan_ops(&tx, Q).await;
    assert_uses(&ops, "MutationSetExec", Q);
    for sibling in [
        "MutationCreateExec",
        "MutationDeleteExec",
        "MutationRemoveExec",
        "MutationMergeExec",
    ] {
        assert_avoids(&ops, sibling, Q);
    }
    tx.commit().await?;
    Ok(())
}

/// `DELETE` runs the delete mutation.
#[tokio::test]
async fn a_delete_runs_the_delete_mutation() -> Result<()> {
    let db = fixture().await?;
    let session = db.session();
    let tx = session.tx().await?;
    const Q: &str = "MATCH (n:Dst {k: 999}) DELETE n";
    let ops = tx_plan_ops(&tx, Q).await;
    assert_uses(&ops, "MutationDeleteExec", Q);
    tx.commit().await?;
    Ok(())
}

/// `REMOVE` runs the remove mutation.
#[tokio::test]
async fn a_remove_runs_the_remove_mutation() -> Result<()> {
    let db = fixture().await?;
    let session = db.session();
    let tx = session.tx().await?;
    const Q: &str = "MATCH (n:Src {k: 0}) REMOVE n.k";
    let ops = tx_plan_ops(&tx, Q).await;
    assert_uses(&ops, "MutationRemoveExec", Q);
    tx.commit().await?;
    Ok(())
}

/// `MERGE` runs the merge mutation.
#[tokio::test]
async fn a_merge_runs_the_merge_mutation() -> Result<()> {
    let db = fixture().await?;
    let session = db.session();
    let tx = session.tx().await?;
    const Q: &str = "MERGE (n:Src {k: 12345})";
    let ops = tx_plan_ops(&tx, Q).await;
    assert_uses(&ops, "MutationMergeExec", Q);
    tx.commit().await?;
    Ok(())
}

// ── Paths ──────────────────────────────────────────────────────────────────

/// A named fixed-length path binds through the fixed-path operator.
#[tokio::test]
async fn a_named_fixed_path_runs_the_bind_fixed_path() -> Result<()> {
    let db = fixture().await?;
    assert_plan_uses(
        &db.session(),
        "MATCH p = (a:Src)-[:R]->(b:Dst) RETURN p",
        "BindFixedPathExec",
    )
    .await;
    Ok(())
}

/// A path bound to a **single node pattern** runs the zero-length binder.
///
/// The operator's name suggests `*0..`, and that is the wrong lead: a `*0..2`
/// pattern plans a `BindFixedPathExec` over a variable-length traverse, with or
/// without a declared edge type — both measured. `plan_bind_zero_length_path`
/// is reached from `p = (a)`, a path with one node and no relationship, which
/// is the only shape that has no edge to bind.
#[tokio::test]
async fn a_single_node_path_runs_the_bind_zero_length_path() -> Result<()> {
    let db = fixture().await?;
    let session = db.session();
    const Q: &str = "MATCH p = (a:Src) RETURN p";
    assert_plan_uses(&session, Q, "BindZeroLengthPathExec").await;
    // The twin: once there is a relationship to bind, the fixed-path binder
    // takes over and this operator must not appear.
    assert_plan_avoids(
        &session,
        "MATCH p = (a:Src)-[:R]->(b:Dst) RETURN p",
        "BindZeroLengthPathExec",
    )
    .await;
    Ok(())
}

/// `shortestPath` runs the shortest-path operator.
#[tokio::test]
async fn a_shortest_path_runs_the_shortest_path_operator() -> Result<()> {
    let db = fixture().await?;
    assert_plan_uses(
        &db.session(),
        "MATCH p = shortestPath((a:Src {k: 0})-[:R*]-(b:Dst {k: 0})) RETURN p",
        "GraphShortestPathExec",
    )
    .await;
    Ok(())
}

// ── Procedures ─────────────────────────────────────────────────────────────

/// `CALL ... YIELD` runs the procedure-call operator.
#[tokio::test]
async fn a_procedure_call_runs_the_procedure_call_operator() -> Result<()> {
    let db = fixture().await?;
    assert_plan_uses(
        &db.session(),
        "CALL uni.schema.labels() YIELD label RETURN label",
        "GraphProcedureCallExec",
    )
    .await;
    Ok(())
}

// ── Subqueries ─────────────────────────────────────────────────────────────

/// A `CALL { ... }` subquery runs the apply operator.
///
/// `EXISTS { }` and pattern comprehensions do *not*: neither builds an `Apply`
/// or `SubqueryCall` logical node, so neither reaches `plan_apply`. Measured —
/// and worth recording, because all three read like the same feature.
#[tokio::test]
async fn a_call_subquery_runs_the_graph_apply() -> Result<()> {
    let db = fixture().await?;
    let session = db.session();
    assert_plan_uses(
        &session,
        "MATCH (n:Src) CALL { WITH n RETURN n.k AS kk } RETURN n.k, kk",
        "GraphApplyExec",
    )
    .await;
    // The twin: an EXISTS subquery reads like the same construct and routes
    // elsewhere entirely.
    assert_plan_avoids(
        &session,
        "MATCH (a:Src) WHERE EXISTS { MATCH (a)-[:R]->(:Dst) } RETURN a.k",
        "GraphApplyExec",
    )
    .await;
    Ok(())
}

// ── Anchors ────────────────────────────────────────────────────────────────

/// An unlabelled `ext_id` equality anchors through the external-id lookup.
///
/// The guard is easy to defeat by accident and invisible in the result: the
/// pattern must carry **no label**, and the value must be a string *literal*.
/// A labelled pattern over the same property plans an ordinary scan and returns
/// the identical row — which is exactly the silent-downgrade shape this gate
/// exists for, so the twin below is the load-bearing half of this test.
///
/// Both arms project `ext_id` rather than `k`: without a label there is no
/// property schema to resolve against, and the unlabelled plan carries only
/// `_vid`, `ext_id` and `_label`.
#[tokio::test]
async fn an_unlabelled_ext_id_match_runs_the_ext_id_lookup() -> Result<()> {
    let db = fixture().await?;
    let session = db.session();
    assert_plan_uses(
        &session,
        "MATCH (n {ext_id: 'e1'}) RETURN n.ext_id",
        "GraphExtIdLookupExec",
    )
    .await;
    assert_plan_avoids(
        &session,
        "MATCH (n:Src {ext_id: 'e1'}) RETURN n.ext_id",
        "GraphExtIdLookupExec",
    )
    .await;
    Ok(())
}

// ── Transactions ───────────────────────────────────────────────────────────

/// A read **inside a transaction** records its read set; the same read outside
/// one does not.
///
/// SSI is on by default (`UniConfig::ssi_enabled`), and every explicit
/// transaction gets an `occ_read_set`, so the wrapper is present for
/// `session.tx()` and absent for `session.query()`. The pair is the proof: the
/// operator is pure overhead on the plan and would be easy to drop with no test
/// noticing, since it changes no row.
#[tokio::test]
async fn a_transactional_read_runs_the_read_set_recording() -> Result<()> {
    let db = fixture().await?;
    let session = db.session();
    const Q: &str = "MATCH (n:Src) RETURN n.k";

    let tx = session.tx().await?;
    let ops = tx_plan_ops(&tx, Q).await;
    assert_uses(&ops, "ReadSetRecordingExec", Q);
    tx.commit().await?;

    // Outside a transaction there is no read set to record.
    assert_plan_avoids(&session, Q, "ReadSetRecordingExec").await;
    Ok(())
}

// ── Recursion ──────────────────────────────────────────────────────────────

/// `WITH RECURSIVE` runs the recursive-CTE operator.
///
/// The body must be a `UNION` of an anchor and a recursive part; the planner
/// rejects any other shape outright, so there is no silent fallback here — but
/// the operator is otherwise unwitnessed by the result, which is a chain of
/// ids any non-recursive plan could also produce on this fixture.
#[tokio::test]
async fn a_with_recursive_runs_the_recursive_cte() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("id", DataType::Int32)
        .edge_type("CHILD", &["Item"], &["Item"])
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (n0:Item {id: 0})").await?;
    tx.execute("CREATE (n1:Item {id: 1})").await?;
    tx.execute("MATCH (a:Item {id: 0}), (b:Item {id: 1}) CREATE (a)-[:CHILD]->(b)")
        .await?;
    tx.commit().await?;

    assert_plan_uses(
        &session,
        "WITH RECURSIVE hierarchy AS (
             MATCH (root:Item {id: 0}) RETURN root
             UNION
             MATCH (parent:Item)-[:CHILD]->(child:Item)
             WHERE parent IN hierarchy
             RETURN child
         )
         MATCH (n:Item) WHERE n IN hierarchy
         RETURN n.id AS id ORDER BY id",
        "RecursiveCTEExec",
    )
    .await;
    Ok(())
}

// ── Vector search ──────────────────────────────────────────────────────────

/// A `WHERE vector_similarity(...) > t` predicate runs the KNN operator.
///
/// # The shape that looks right and is not
///
/// `ORDER BY similar_to(d.embedding, ...) DESC LIMIT 1` reads like the KNN
/// query and **does not reach this operator**: measured, it plans
/// `GraphScanExec` + `SortExec(TopK)` and answers correctly by scanning
/// everything and sorting. The rewrite is driven from a *predicate*
/// (`planner.rs:6895`), matching a `vector_similarity(var.prop, q)` call under
/// a comparison or `~=` — not from an ordering.
///
/// That is the exact hazard #177 exists for, and it is live in this repo:
/// `ssi_read_path_matrix.rs::vector_knn_records_matches` uses the `ORDER BY`
/// form and passes, because a full scan records the same read set the KNN exec
/// would have. Its assertion cannot tell the two apart. This test can.
#[tokio::test]
async fn a_vector_similarity_predicate_runs_the_vector_knn() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Doc")
        .property("id", DataType::String)
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
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Doc {id: 'd1', embedding: [1.0, 0.0]})")
        .await?;
    tx.execute("CREATE (:Doc {id: 'd2', embedding: [0.0, 1.0]})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    assert_plan_uses(
        &session,
        "MATCH (d:Doc) WHERE vector_similarity(d.embedding, [1.0, 0.0]) > 0.5 \
         RETURN d.id AS id",
        "GraphVectorKnnExec",
    )
    .await;

    // The twin, and the finding: the ordering form silently takes a scan.
    assert_plan_avoids(
        &session,
        "MATCH (d:Doc) RETURN d.id AS id \
         ORDER BY similar_to(d.embedding, [1.0, 0.0]) DESC LIMIT 1",
        "GraphVectorKnnExec",
    )
    .await;
    Ok(())
}
