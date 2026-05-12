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
    let cypher = "MATCH (n) WHERE n:Person OR n:Organization RETURN id(n) AS vid";
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
    let cypher = "MATCH (n) WHERE n:Person OR n:Organization RETURN id(n) AS vid";

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

// ---------------------------------------------------------------------------
// Heterogeneous-schema label disjunction (issue rustic-ai/uni-db#62)
// ---------------------------------------------------------------------------
//
// The Q1–Q3 fixtures above all use labels with a single uniform `name:
// String` property, so per-label `Scan` branches under a `Union` happen to
// produce identical Arrow schemas. That hid a structural hazard: when the
// labels in a disjunction carry *different* property sets, the per-label
// branch schemas have different field counts, and DataFusion's
// `UnionExec::try_new` panics inside `union_schema` (`index out of bounds:
// the len is N but the index is N`). The panic escapes the await as a
// process abort, since the underlying arrow-schema helper has no `Result`.
//
// The fix keeps the narrow-scan Union path for homogeneous label sets, and
// for heterogeneous sets routes every branch through a *single-label*
// `ScanMainByLabels` (which resolves columns schemaless-style, not
// per-label) so the Union sees a uniform schema across branches.

/// Build a graph with three labels of intentionally different property
/// counts (matching the scenario described in issue #62: an `ABOUT` edge
/// pointing at either a low-property `Entity` or a high-property
/// `Participant`).
async fn setup_heterogeneous() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Source")
        .property("name", DataType::String)
        .done()
        // 2 properties.
        .label("DestA")
        .property("name", DataType::String)
        .property("category", DataType::String)
        .done()
        // 4 properties.
        .label("DestB")
        .property("name", DataType::String)
        .property("kind", DataType::String)
        .property("first_seen", DataType::String)
        .property("last_seen", DataType::String)
        .done()
        .edge_type("LINK", &["Source"], &["DestA", "DestB"])
        .done()
        .apply()
        .await
        .unwrap();

    let session = db.session();
    let tx = session.tx().await.unwrap();
    tx.execute(
        r#"
        CREATE (:Source {name: 'src'})
        CREATE (:DestA {name: 'a1', category: 'cat'})
        CREATE (:DestB {name: 'b1', kind: 'k', first_seen: 'fs', last_seen: 'ls'})
        "#,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    db
}

/// Q6: heterogeneous-schema inline disjunction, no property projection.
///
/// Direct repro of issue #62: `MATCH (b:DestA|DestB)` over labels with
/// 2-vs-4 property sets used to panic in DataFusion before the fix.
/// Asserts both correct row count *and* the new fallback plan shape
/// (single `ScanMainByLabels`, no per-label `Union { Scan, Scan }`).
#[tokio::test]
async fn q6_node_label_disjunction_heterogeneous_no_panic() {
    let db = setup_heterogeneous().await;
    let cypher = "MATCH (b:DestA|DestB) RETURN id(b) AS vid";

    let explain = db
        .session()
        .query_with(cypher)
        .explain()
        .await
        .expect("Q6 EXPLAIN should succeed");
    eprintln!("Q6 plan-shape:\n{}", explain.plan_text);

    let result = db
        .session()
        .query(cypher)
        .await
        .expect("Q6 must not panic on heterogeneous label disjunction");
    assert_eq!(result.rows().len(), 2, "Q6: expected 1 DestA + 1 DestB");

    assert!(
        explain.plan_text.contains("ScanMainByLabels"),
        "Q6 heterogeneous disjunction must route branches through ScanMainByLabels, got:\n{}",
        explain.plan_text
    );
    assert!(
        explain.plan_text.contains("Union"),
        "Q6 heterogeneous disjunction must keep the per-label Union shape (with main-table branches), got:\n{}",
        explain.plan_text
    );
    // Each branch must be a *single-label* ScanMainByLabels — multi-label
    // ScanMainByLabels has AND/intersection semantics, wrong for a
    // disjunction. The plan-text format prints the labels list, so
    // assert no `"DestA",\n                "DestB"` co-occurrence inside
    // a single ScanMainByLabels (proxy: ensure each branch list has one
    // element via per-label substring presence + branch count).
    let main_count = explain.plan_text.matches("ScanMainByLabels").count();
    assert_eq!(
        main_count, 2,
        "Q6 must produce exactly one ScanMainByLabels branch per label, got {} occurrences in:\n{}",
        main_count, explain.plan_text
    );
}

/// Q7: heterogeneous-schema disjunction with a *common* property
/// projection. `name` exists on both DestA and DestB, but the labels'
/// full schemas still differ — the planner must take the heterogeneous
/// fallback rather than try to narrow-scan a property that happens to be
/// shared. Validates that property access flows correctly through the
/// `ScanMainByLabels` path (which extracts properties from `props_json`
/// rather than per-label columns).
#[tokio::test]
async fn q7_node_label_disjunction_heterogeneous_with_common_property() {
    let db = setup_heterogeneous().await;
    let cypher = "MATCH (b:DestA|DestB) RETURN b.name AS name ORDER BY name";

    let result = db.session().query(cypher).await.expect("Q7 must execute");
    let rows = result.rows();
    assert_eq!(rows.len(), 2, "Q7: expected 1 DestA + 1 DestB");
    let names: Vec<String> = rows
        .iter()
        .map(|r| r.get::<String>("name").unwrap())
        .collect();
    assert_eq!(names, vec!["a1".to_string(), "b1".to_string()]);
}

/// Q8: heterogeneous-schema predicate form. `WHERE n:DestA OR n:DestB`
/// goes through the *separate* `replace_scan_all_with_label_union`
/// rewrite, so the heterogeneous fallback has to fire there too.
#[tokio::test]
async fn q8_where_label_disjunction_heterogeneous_no_panic() {
    let db = setup_heterogeneous().await;
    let cypher = "MATCH (b) WHERE b:DestA OR b:DestB RETURN id(b) AS vid";

    let explain = db
        .session()
        .query_with(cypher)
        .explain()
        .await
        .expect("Q8 EXPLAIN should succeed");
    eprintln!("Q8 plan-shape:\n{}", explain.plan_text);

    let result = db
        .session()
        .query(cypher)
        .await
        .expect("Q8 must not panic on heterogeneous WHERE-form disjunction");
    assert_eq!(result.rows().len(), 2);

    assert!(
        explain.plan_text.contains("ScanMainByLabels"),
        "Q8 must fall back to ScanMainByLabels, got:\n{}",
        explain.plan_text
    );
}
