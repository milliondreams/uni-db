// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression (#141): variable-length-path label *filters* must not fail open
//! after a flush.
//!
//! The two VLP label predicates on `GraphVariableLengthTraverseExecData` —
//! `check_target_label` (terminal `(m:Label)` filter) and
//! `check_state_constraint` (QPP intermediate `(y:Label)` filter) — resolved a
//! vertex's labels from the in-memory L0 chain only, and fell *open* (`None =>
//! admit`) when the vertex was absent from L0. Once a vertex is flushed to Lance
//! its labels live only in the persisted `VidLabelsIndex`, so the L0-only read
//! returned `None` and the filter admitted the vertex **without checking the
//! label** — over-admitting endpoints that do not carry the required label.
//!
//! The fix routes both predicates through `GraphExecutionContext::
//! resolve_vertex_labels` (L0 chain then persisted index), so a flushed vertex
//! is judged against its real labels. Each test forces the wrongly-labeled
//! vertex out of L0 with an explicit `db.flush()` before the traversal reads it.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni};

/// T1 — terminal `(m:Label)` VLP stays correct after a flush.
///
/// `a` fans out via `R` to a `:B` endpoint and a `:C` endpoint; a `*1..2`
/// traversal scoped to `(m:B)` must return only the `:B` endpoint. This is
/// complementary coverage rather than an isolating repro: instrumentation
/// confirms `check_target_label` itself fails open here (post-flush it reads
/// `None` and admits the `:C` endpoint), but a downstream `_labels`-based
/// filter — which resolves via the persisted index and is therefore correct
/// after a flush — masks the over-admission at the query level. The
/// `check_target_label` hardening is defense-in-depth for query shapes without
/// that downstream filter; the observable over-admission is exercised by T2
/// (`check_state_constraint`, which has no downstream backstop).
#[tokio::test]
async fn t1_terminal_label_filter_after_flush() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("A")
        .property("name", DataType::String)
        .label("B")
        .property("name", DataType::String)
        .label("C")
        .property("name", DataType::String)
        .edge_type("R", &["A"], &["B", "C"])
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    // One `a` linked to both a :B and a :C endpoint (comma-joined so `a` is reused).
    tx.execute("CREATE (a:A {name: 'a'})-[:R]->(b:B {name: 'b'}), (a)-[:R]->(c:C {name: 'c'})")
        .await?;
    tx.commit().await?;
    db.flush().await?; // b and c now live only in Lance.

    let result = db
        .session()
        .query("MATCH (a:A)-[:R*1..2]->(m:B) RETURN m.name AS name")
        .await?;

    let mut names: Vec<String> = result
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("name").ok())
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec!["b".to_string()],
        "VLP `(m:B)` must reject the flushed :C endpoint; got {names:?}"
    );

    db.shutdown().await?;
    Ok(())
}

/// T2 — QPP intermediate `(y:Label)` constraint rejects a flushed non-matching
/// intermediate node.
///
/// A two-hop QPP body `((x)-[:R]->(w:Mid)-[:R]->(y)){1,1}` requires the
/// intermediate `w` to be `:Mid`. The path through a `:Other` intermediate must
/// be pruned by `check_state_constraint`. After a flush the intermediate's
/// labels leave L0; before the fix the constraint read L0-only, saw `None`, and
/// let the wrong-label path through.
#[tokio::test]
async fn t2_qpp_intermediate_constraint_after_flush() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("A")
        .property("name", DataType::String)
        .label("Mid")
        .property("name", DataType::String)
        .label("Other")
        .property("name", DataType::String)
        .label("End")
        .property("name", DataType::String)
        .edge_type("R", &["A", "Mid", "Other"], &["Mid", "Other", "End"])
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    // a -> (:Mid) -> end1   [valid: intermediate is :Mid]
    // a -> (:Other) -> end2 [invalid: intermediate is :Other, must be pruned]
    tx.execute(
        "CREATE (a:A {name: 'a'})-[:R]->(w1:Mid {name: 'w1'})-[:R]->(e1:End {name: 'end1'}), \
         (a)-[:R]->(w2:Other {name: 'w2'})-[:R]->(e2:End {name: 'end2'})",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?; // w1 and w2 now live only in Lance.

    let result = db
        .session()
        .query(
            "MATCH (a:A {name: 'a'}) \
             MATCH (a)((x)-[:R]->(w:Mid)-[:R]->(y)){1,1}(m) \
             RETURN m.name AS name",
        )
        .await?;

    let mut names: Vec<String> = result
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("name").ok())
        .collect();
    names.sort();
    names.dedup();
    assert_eq!(
        names,
        vec!["end1".to_string()],
        "QPP `(w:Mid)` must prune the path through the flushed :Other intermediate; got {names:?}"
    );

    db.shutdown().await?;
    Ok(())
}
