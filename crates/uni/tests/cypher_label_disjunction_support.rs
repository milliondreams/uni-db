// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
// Rust guideline compliant

//! Regression tests for label and edge-type disjunction in MATCH.
//!
//! "Label disjunction" is three surface forms with different histories:
//!
//! 1. `MATCH ()-[r:A|B]->()` — relationship type disjunction. openCypher
//!    standard since pre-1.0; in GQL.
//! 2. `MATCH (n:A|B)` — node label disjunction. Neo4j 5+ "label
//!    expression" syntax; GQL-aligned.
//! 3. `MATCH (n) WHERE n:A OR n:B` — predicate form. Standard since
//!    openCypher 1.x. The portable cross-engine fallback.
//!
//! These tests cover all three plus the symmetric edge-side predicate
//! form `WHERE type(r) = 'A' OR type(r) = 'B'`. Each asserts both
//! correct row counts AND the perf-critical plan shape: the planner
//! must lower disjunction into a `Union` of label-scoped `Scan`
//! operators (or merge `Traverse.edge_type_ids`) — never `ScanAll +
//! Filter(LabelCheck)`.
//!
//! Background: customer issue rustic-ai/uni-db#56 reported (2)
//! parse-failing. The parser fix alone wouldn't have closed the gap —
//! the planner also wasn't rewriting (3) into label-scoped scans, so
//! the customer's LoCoMo `ABOUT` benchmark would still have hit the
//! 1.8× ceiling. Both layers are fixed here.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test cypher_label_disjunction_support \
//!       --no-capture

use uni_db::{DataType, Uni};

/// Build a graph with two node labels and two edge types, then return the
/// db plus the vids of the inserted Person, Organization, and the two
/// edges.
async fn setup() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .done()
        .label("Organization")
        .property("name", DataType::String)
        .done()
        .edge_type("KNOWS", &["Person"], &["Person"])
        .done()
        .edge_type("FOLLOWS", &["Person"], &["Organization"])
        .done()
        .apply()
        .await
        .unwrap();

    let session = db.session();
    let tx = session.tx().await.unwrap();
    tx.execute(
        r#"
        CREATE (alice:Person {name: 'Alice'})
        CREATE (bob:Person {name: 'Bob'})
        CREATE (acme:Organization {name: 'Acme'})
        CREATE (alice)-[:KNOWS]->(bob)
        CREATE (alice)-[:FOLLOWS]->(acme)
        "#,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    db
}

/// Q1: relationship-type disjunction `[r:KNOWS|FOLLOWS]`.
///
/// Should match both edges (one KNOWS, one FOLLOWS) → 2 rows.
#[tokio::test]
async fn q1_rel_type_disjunction_executes() {
    let db = setup().await;
    let cypher = "MATCH (a)-[r:KNOWS|FOLLOWS]->(b) RETURN id(r) AS eid";

    let explain = db.session().query_with(cypher).explain().await;
    match &explain {
        Ok(e) => eprintln!("Q1 EXPLAIN ok\nplan_text:\n{}", e.plan_text),
        Err(err) => eprintln!("Q1 EXPLAIN err: {err:?}"),
    }

    let result = db.session().query(cypher).await;
    match &result {
        Ok(r) => eprintln!("Q1 row count = {}", r.rows().len()),
        Err(err) => eprintln!("Q1 query err: {err:?}"),
    }

    let rows = result.expect("Q1 should parse and execute").rows().len();
    assert_eq!(
        rows, 2,
        "Q1 [r:KNOWS|FOLLOWS]: expected 2 edges (1 KNOWS + 1 FOLLOWS)"
    );
}

/// Q2: node-label disjunction `(n:Person|Organization)`.
///
/// Should match all three nodes (2 Person + 1 Organization) → 3 rows.
///
/// Was previously blocked by rustic-ai/uni-db#56 (parser rejected `:A|B`
/// syntax). Fixed by extending `node_labels` in `cypher.pest` and lowering
/// `LabelExpr::Disjunction` to a `Union` of label-scoped `Scan` operators.
#[tokio::test]
async fn q2_node_label_disjunction_executes() {
    let db = setup().await;
    let cypher = "MATCH (n:Person|Organization) RETURN id(n) AS vid";

    let explain = db.session().query_with(cypher).explain().await;
    match &explain {
        Ok(e) => eprintln!("Q2 EXPLAIN ok\nplan_text:\n{}", e.plan_text),
        Err(err) => eprintln!("Q2 EXPLAIN err: {err:?}"),
    }

    let result = db.session().query(cypher).await;
    match &result {
        Ok(r) => eprintln!("Q2 row count = {}", r.rows().len()),
        Err(err) => eprintln!("Q2 query err: {err:?}"),
    }

    let rows = result.expect("Q2 should parse and execute").rows().len();
    assert_eq!(
        rows, 3,
        "Q2 (n:Person|Organization): expected 3 nodes (2 Person + 1 Organization)"
    );
}

/// Pipe-after-colon edge case: `(n:A|:B)` is the form Neo4j accepts as
/// equivalent to `(n:A|B)`. The relationship-type rule has accepted the
/// optional colon since before this issue; the node-label rule needs to
/// match for syntactic uniformity.
#[tokio::test]
async fn q2_node_label_disjunction_with_colon_after_pipe() {
    let db = setup().await;
    let cypher = "MATCH (n:Person|:Organization) RETURN id(n) AS vid";
    let result = db
        .session()
        .query(cypher)
        .await
        .expect("(:A|:B) form should parse");
    assert_eq!(result.rows().len(), 3);
}

/// Baseline: single-label `MATCH (n:Person)` for plan-shape comparison
/// against Q3. If single-label compiles to a label-scoped scan and Q3
/// compiles to `ScanAll + Filter`, the planner is missing a label-
/// disjunction → union-of-label-scans rewrite.
#[tokio::test]
async fn baseline_single_label_plan() {
    let db = setup().await;
    let cypher = "MATCH (n:Person) RETURN id(n) AS vid";
    let explain = db.session().query_with(cypher).explain().await.unwrap();
    eprintln!("BASELINE single-label plan:\n{}", explain.plan_text);
}

/// Q2 plan-shape: `(n:Person|Organization)` must lower to a `Union` of
/// label-scoped `Scan` operators, **not** `ScanAll + Filter`. This is the
/// perf-critical assertion — without the narrow scan, the customer's
/// LoCoMo `ABOUT` benchmark would still cap at 1.8× even though the syntax
/// now parses.
#[tokio::test]
async fn q2_node_label_disjunction_uses_label_scoped_scans() {
    let db = setup().await;
    let cypher = "MATCH (n:Person|Organization) RETURN id(n) AS vid";
    let explain = db.session().query_with(cypher).explain().await.unwrap();
    eprintln!("Q2 plan-shape:\n{}", explain.plan_text);

    assert!(
        explain.plan_text.contains("Union"),
        "Q2 plan must use Union of label-scoped scans, got:\n{}",
        explain.plan_text
    );
    assert!(
        explain.plan_text.contains("Scan {"),
        "Q2 plan must contain at least one label-scoped Scan, got:\n{}",
        explain.plan_text
    );
    assert!(
        !explain.plan_text.contains("ScanAll"),
        "Q2 plan must NOT contain ScanAll (would defeat the label hint), got:\n{}",
        explain.plan_text
    );
}

/// Q3 plan-shape: `WHERE n:A OR n:B` must rewrite the upstream `ScanAll`
/// into a `Union` of label-scoped scans, identical to the inline form's
/// plan shape. This was previously the bigger gap behind issue #56 — the
/// parser fix alone wouldn't have closed the perf gap because Q3 (the
/// portable workaround) plans to `ScanAll + Filter` without this rewrite.
#[tokio::test]
async fn q3_predicate_form_uses_label_scoped_scans() {
    let db = setup().await;
    let cypher =
        "MATCH (n) WHERE n:Person OR n:Organization RETURN id(n) AS vid";
    let explain = db.session().query_with(cypher).explain().await.unwrap();
    eprintln!("Q3 plan-shape:\n{}", explain.plan_text);

    assert!(
        explain.plan_text.contains("Union"),
        "Q3 plan must rewrite to Union of label-scoped scans, got:\n{}",
        explain.plan_text
    );
    assert!(
        !explain.plan_text.contains("ScanAll"),
        "Q3 plan must NOT contain ScanAll after the predicate rewrite, got:\n{}",
        explain.plan_text
    );
    assert!(
        !explain.plan_text.contains("LabelCheck"),
        "Q3 plan must NOT retain residual LabelCheck filters, got:\n{}",
        explain.plan_text
    );
}

/// Q4: relationship-type disjunction expressed as a WHERE predicate
/// (`type(r) = 'A' OR type(r) = 'B'`). Should be rewritten to merge the
/// type names into the upstream `Traverse.edge_type_ids`, matching the
/// plan shape of the inline `[r:A|B]` form.
#[tokio::test]
async fn q4_edge_type_disjunction_via_where() {
    let db = setup().await;
    let cypher = "MATCH (a)-[r]->(b) \
                  WHERE type(r) = 'KNOWS' OR type(r) = 'FOLLOWS' \
                  RETURN id(r) AS eid";

    let explain = db.session().query_with(cypher).explain().await.unwrap();
    eprintln!("Q4 plan-shape:\n{}", explain.plan_text);

    let result = db.session().query(cypher).await.unwrap();
    assert_eq!(
        result.rows().len(),
        2,
        "Q4 expected 2 edges (1 KNOWS + 1 FOLLOWS)"
    );

    // After the rewrite, `type(...)` should be entirely consumed from
    // the predicate (merged into `Traverse.edge_type_ids`). If `type(`
    // appears in the plan text, it means the rewrite didn't fire and
    // the residual filter is still doing the work.
    assert!(
        !explain.plan_text.contains("\"type\""),
        "Q4 plan must not retain `type(r)` predicates as residual filter, got:\n{}",
        explain.plan_text
    );
}

/// Q3: predicate form `WHERE n:Person OR n:Organization`.
///
/// Should match all three nodes → 3 rows. This is the universal portable
/// fallback; if even this fails, every customer is forced to invent
/// ad-hoc workarounds.
#[tokio::test]
async fn q3_predicate_form_executes() {
    let db = setup().await;
    let cypher =
        "MATCH (n) WHERE n:Person OR n:Organization RETURN id(n) AS vid";

    let explain = db.session().query_with(cypher).explain().await;
    match &explain {
        Ok(e) => eprintln!("Q3 EXPLAIN ok\nplan_text:\n{}", e.plan_text),
        Err(err) => eprintln!("Q3 EXPLAIN err: {err:?}"),
    }

    let result = db.session().query(cypher).await;
    match &result {
        Ok(r) => eprintln!("Q3 row count = {}", r.rows().len()),
        Err(err) => eprintln!("Q3 query err: {err:?}"),
    }

    let rows = result.expect("Q3 should parse and execute").rows().len();
    assert_eq!(
        rows, 3,
        "Q3 WHERE n:Person OR n:Organization: expected 3 nodes (2 Person + 1 Organization)"
    );
}
