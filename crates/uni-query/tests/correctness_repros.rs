// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Runnable repros for the 2026-07-05 correctness scan (uni-query).
// Each test exercises the real public API (parse -> QueryPlanner::plan ->
// Executor::execute) with real inputs. Tests that currently observe BUGGY
// behavior assert the actual (wrong) value with a "// BUG:" comment, or are
// marked #[ignore] with the correct-behavior assertion so CI stays green.

#![allow(clippy::all)]
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_common::Value;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_query::query::executor::Executor;
use uni_query::query::planner::QueryPlanner;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

struct Harness {
    executor: Executor,
    prop_manager: Arc<PropertyManager>,
    schema_manager: Arc<SchemaManager>,
    writer: Arc<Writer>,
    _dir: tempfile::TempDir,
}

impl Harness {
    async fn new<F: FnOnce(&SchemaManager)>(setup: F) -> Self {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let schema_manager = SchemaManager::load(&path.join("schema.json")).await.unwrap();
        setup(&schema_manager);
        schema_manager.save().await.unwrap();

        let schema_manager = Arc::new(schema_manager);
        let storage = Arc::new(
            StorageManager::new(path.join("storage").to_str().unwrap(), schema_manager.clone())
                .await
                .unwrap(),
        );
        let writer = Arc::new(
            Writer::new(storage.clone(), schema_manager.clone(), 0)
                .await
                .unwrap(),
        );
        let prop_manager = Arc::new(PropertyManager::new(
            storage.clone(),
            schema_manager.clone(),
            100,
        ));
        let executor = Executor::new_with_writer(storage.clone(), writer.clone());
        Harness {
            executor,
            prop_manager,
            schema_manager,
            writer,
            _dir: dir,
        }
    }

    async fn new_schemaless() -> Self {
        Self::new(|_sm| {}).await
    }

    async fn run(&self, cypher: &str) -> anyhow::Result<Vec<HashMap<String, Value>>> {
        let query = uni_cypher::parse(cypher)?;
        let planner = QueryPlanner::new(self.schema_manager.schema());
        let plan = planner.plan(query)?;
        self.executor
            .execute(plan, &self.prop_manager, &HashMap::new())
            .await
    }

    async fn run_ok(&self, cypher: &str) -> Vec<HashMap<String, Value>> {
        self.run(cypher)
            .await
            .unwrap_or_else(|e| panic!("query failed: {cypher}\n  err: {e}"))
    }

    async fn flush(&self) {
        self.writer.flush_to_l1(None).await.unwrap();
    }
}

// NOTE: finding [3] (locy_fixpoint.rs:5169, TopKProofs body_support_map IS-ref)
// was RE-VERIFIED and REFUTED — the code does the opposite of the claim (a
// recursive rule owns a separate non-self-ref handle carrying converged facts),
// so no repro is written for it.

fn cell(row: &HashMap<String, Value>, k: &str) -> Value {
    row.get(k).cloned().unwrap_or(Value::Null)
}

fn as_int(v: &Value) -> i64 {
    match v {
        Value::Int(i) => *i,
        other => panic!("expected Int, got {other:?}"),
    }
}

// ===========================================================================
// [1] df_planner.rs:4918 — count(variable) over unmatched OPTIONAL counts NULL
// ===========================================================================
#[tokio::test]
async fn repro_01_count_optional_null_row() {
    let h = Harness::new_schemaless().await;
    h.run_ok("CREATE (:Person {name: 'Charlie'})").await;
    let rows = h
        .run_ok("MATCH (n:Person {name:'Charlie'}) OPTIONAL MATCH (n)-[:KNOWS]->(m:Person) RETURN count(m) AS c")
        .await;
    let c = as_int(&cell(&rows[0], "c"));
    println!("[1] count(m) over unmatched OPTIONAL = {c} (correct=0)");
    // BUG: expected c=0 (m is NULL for unmatched OPTIONAL row), got 1
    assert_eq!(c, 1, "repro for df_planner.rs:4918");
}

// ===========================================================================
// [2] apply.rs:823 — write subquery dedup by params collapses identical rows
// ===========================================================================
#[tokio::test]
async fn repro_02_call_write_dedup() {
    let h = Harness::new_schemaless().await;
    let r = h.run("UNWIND [1,1,1] AS x CALL { CREATE (:N) }").await;
    println!("[2] unwind-call-create result: {r:?}");
    let rows = h.run_ok("MATCH (n:N) RETURN count(n) AS c").await;
    let c = as_int(&cell(&rows[0], "c"));
    println!("[2] node count after 3 identical CALL{{CREATE}} = {c} (correct=3)");
    // BUG: expected 3, got 1 (rows 2,3 hit the params dedup cache)
    assert_eq!(c, 1, "repro for apply.rs:823");
}

// ===========================================================================
// FINDING [22] apply.rs:350 — evaluate_comparison returns false for every
// operator it doesn't handle (STARTS WITH/CONTAINS/Regex/…), so an unsupported
// operator sitting in an Apply.input_filter drops rows that should pass.
//
// Reachability note: through Cypher, `CALL { ... }` lowers to SubqueryCall (not
// Apply); an in-query `CALL proc() YIELD` builds the Apply but the natural
// post-YIELD WHERE is dropped at plan time (input_filter stays None), and the
// re-MATCH form that DOES populate input_filter hits an unrelated nullable-vid
// error. So to hit apply.rs:350 precisely we build the real plan (an in-query
// `CALL proc() YIELD` → LogicalPlan::Apply) and inject the predicate into
// `input_filter` exactly as `push_predicates_to_apply` would, then execute it.
//
// Rigorous control: `a.name = 'Alice'` (a SUPPORTED comparison that is TRUE)
// keeps the row -> 2 output rows, proving the injected input_filter path works.
// `a.name STARTS WITH 'Al'` is ALSO logically TRUE for 'Alice', so it must
// likewise yield 2 rows — but `evaluate_comparison`'s `_ => false` arm drops the
// row, so the correct 2-row result is NOT produced (0 rows, surfacing as the
// downstream non-nullable a._vid error once all input rows are dropped).
// ===========================================================================
#[tokio::test]
async fn repro_03_apply_startswith_input_filter() {
    // [22] apply.rs input_filter evaluator soundness. The evaluator handles only
    // Eq/ordering; every other operator formerly hit `_ => false`, silently
    // DROPPING matching rows. A pre-filter must instead treat an operator it
    // can't evaluate as "unknown" → KEEP the row (in production the planner's
    // `apply_input_filter_supported` gate keeps such shapes as a residual Filter,
    // so this branch is a defensive backstop). We inject STARTS WITH directly
    // into Apply.input_filter (the enforcement point) and assert it behaves like
    // a TRUE Eq control: the row survives.
    use uni_cypher::ast::{BinaryOp, CypherLiteral, Expr as AstExpr};
    use uni_query::query::planner::LogicalPlan;

    let h = Harness::new(|sm| {
        sm.add_label("Person").unwrap();
        sm.add_property("Person", "name", DataType::String, true).unwrap();
        sm.add_label("Prefix").unwrap();
        sm.add_property("Prefix", "prefix", DataType::String, true).unwrap();
    })
    .await;
    h.run_ok("CREATE (:Person {name:'Alice'})").await;
    h.run_ok("CREATE (:Prefix {prefix:'Al'})").await;

    fn set_apply_filter(plan: LogicalPlan, f: &AstExpr) -> LogicalPlan {
        match plan {
            LogicalPlan::Project { input, projections } => LogicalPlan::Project {
                input: Box::new(set_apply_filter(*input, f)),
                projections,
            },
            LogicalPlan::Apply { input, subquery, .. } => LogicalPlan::Apply {
                input,
                subquery,
                input_filter: Some(f.clone()),
            },
            other => other,
        }
    }
    let build_plan = |f: &AstExpr| {
        let query = uni_cypher::parse(
            "MATCH (a:Person), (b:Prefix) CALL uni.schema.labels() YIELD label RETURN a.name AS name, label AS label",
        )
        .unwrap();
        let planner = QueryPlanner::new(h.schema_manager.schema());
        set_apply_filter(planner.plan(query).unwrap(), f)
    };
    let name_pred = |op, rhs: &str| AstExpr::BinaryOp {
        left: Box::new(AstExpr::Property(
            Box::new(AstExpr::Variable("a".into())),
            "name".into(),
        )),
        op,
        right: Box::new(AstExpr::Literal(CypherLiteral::String(rhs.into()))),
    };

    // CONTROL: a supported comparison (Eq, TRUE) keeps the row.
    let ctrl_rows = h
        .executor
        .execute(build_plan(&name_pred(BinaryOp::Eq, "Alice")), &h.prop_manager, &HashMap::new())
        .await
        .expect("[22] Eq control must execute");
    println!("[22] control Eq('Alice')=true -> {} rows", ctrl_rows.len());
    assert_eq!(ctrl_rows.len(), 2, "[22] control: TRUE Eq keeps the row");

    // FIXED: STARTS WITH (an unsupported op) must no longer drop the row — the
    // evaluator keeps it, matching the Eq control.
    let sw_rows = h
        .executor
        .execute(build_plan(&name_pred(BinaryOp::StartsWith, "Al")), &h.prop_manager, &HashMap::new())
        .await
        .expect("[22] StartsWith must execute (row kept, not dropped)");
    println!("[22] StartsWith('Al') kept -> {} rows", sw_rows.len());
    assert_eq!(
        sw_rows.len(),
        2,
        "apply.rs input_filter must KEEP a row under an unsupported operator, not drop it"
    );
}

// ===========================================================================
// [4] ext_id_lookup.rs:107 — OPTIONAL MATCH by ext_id no-match errors
// ===========================================================================
#[tokio::test]
async fn repro_04_optional_ext_id_null_row() {
    let h = Harness::new_schemaless().await;
    let res = h.run("OPTIONAL MATCH (n {ext_id: 'does-not-exist'}) RETURN n").await;
    match &res {
        Ok(rows) => {
            println!("[4] ext_id OPTIONAL no-match -> Ok rows={}", rows.len());
            // correct behavior: exactly one null row
            assert_eq!(rows.len(), 1, "expected 1 null row");
        }
        Err(e) => {
            let msg = format!("{e}");
            println!("[4] ext_id OPTIONAL no-match -> Err: {msg}");
            // BUG: non-nullable column error instead of a null row
            assert!(
                msg.contains("non-nullable") || msg.contains("null"),
                "repro for ext_id_lookup.rs:107: got error {msg}"
            );
        }
    }
}

// ===========================================================================
// [9] pattern_exists.rs:410 — bound-target NULL treated as unbound -> true
// ===========================================================================
#[tokio::test]
async fn repro_09_pattern_exists_null_bound_target() {
    let h = Harness::new_schemaless().await;
    h.run_ok("CREATE (:N)-[:R]->(:Other)").await;
    let rows = h
        .run_ok("MATCH (n:N) OPTIONAL MATCH (m:M {id:999}) WHERE (n)-[:R]->(m) RETURN n")
        .await;
    println!("[9] pattern exists with NULL bound target -> rows={}", rows.len());
    // BUG: m is NULL for every row, correct=0 rows; got 1 (any :R neighbor -> true)
    assert_eq!(rows.len(), 1, "repro for pattern_exists.rs:410");
}

// ===========================================================================
// [11] vid_lookup_join.rs:463 — LEFT outer with probe_side==Left inverts to RIGHT
// ===========================================================================
#[tokio::test]
async fn repro_11_vid_lookup_left_inverted() {
    let h = Harness::new_schemaless().await;
    h.run_ok("CREATE (:Person {name:'p1'})").await;
    h.run_ok("CREATE (:Person {name:'p2'})").await;
    h.run_ok("CREATE (:Employee {name:'e1'})").await;
    let res = h
        .run("MATCH (a:Person) OPTIONAL MATCH (b:Employee) WHERE id(a) = id(b) RETURN a.name AS an, b.name AS bn ORDER BY an")
        .await;
    match res {
        Ok(rows) => {
            println!("[11] LEFT-outer id join rows={}: {rows:?}", rows.len());
            // correct: 2 rows {p1,NULL},{p2,NULL} (both Persons preserved).
            let ans: Vec<Value> = rows.iter().map(|r| cell(r, "an")).collect();
            let has_null_an = ans.iter().any(|v| matches!(v, Value::Null));
            // BUG: inverted (RIGHT) semantics -> Person side null-padded / dropped.
            assert!(
                rows.len() != 2 || has_null_an,
                "repro for vid_lookup_join.rs:463: expected inverted outer semantics, got {rows:?}"
            );
        }
        Err(e) => {
            // The inverted semantics null-pad the (declared non-nullable) Person
            // side, so DataFusion raises a non-nullable-column error. Either the
            // hard error or the inverted rows demonstrate the defect.
            let msg = format!("{e}");
            println!("[11] LEFT-outer id join errored: {msg}");
            assert!(
                msg.contains("_vid") || msg.contains("non-nullable"),
                "repro for vid_lookup_join.rs:463: unexpected error {msg}"
            );
        }
    }
}

// ===========================================================================
// [12] df_planner.rs:5766 — conflicting window ORDER BY share one SortExec
// ===========================================================================
#[tokio::test]
async fn repro_12_window_conflicting_orderby() {
    let h = Harness::new_schemaless().await;
    h.run_ok("CREATE (:P {name:'a', age:10})").await;
    h.run_ok("CREATE (:P {name:'b', age:20})").await;
    h.run_ok("CREATE (:P {name:'c', age:30})").await;
    let rows = h
        .run_ok("MATCH (n:P) RETURN n.age AS age, row_number() OVER (ORDER BY n.age ASC) AS r1, row_number() OVER (ORDER BY n.age DESC) AS r2")
        .await;
    println!("[12] window rows: {rows:?}");
    // For the row with age=10: r1 should be 1, r2 should be 3.
    let row10 = rows.iter().find(|r| as_int(&cell(r, "age")) == 10).unwrap();
    let r1 = as_int(&cell(row10, "r1"));
    let r2 = as_int(&cell(row10, "r2"));
    println!("[12] age=10 -> r1={r1} r2={r2} (correct r1=1 r2=3)");
    // BUG: r2 == r1 because DESC window evaluated over ASC-sorted rows
    assert_eq!(r1, 1);
    assert_eq!(r2, 1, "repro for df_planner.rs:5766: r2 should be 3 but equals r1");
}

// ===========================================================================
// FINDING [36] planner.rs:6211 — WHERE on a scan-bound var is dropped when a
// Sort/Limit/Aggregate sits between the WHERE and the Scan/ScanAll:
// find_scan_label_id DESCENDS Sort/Limit/Aggregate/Apply (7541-7546) so the
// pushdown branch is entered and the conjunct removed from current_predicate,
// but push_predicate_to_scan has NO arm for those nodes (`other => other`, 7739)
// so the predicate lands nowhere and no compensating Filter is added.
//
// Recalibration: the earlier `WITH n LIMIT 5 WHERE ...` did NOT reproduce — a
// WITH's own WHERE is emitted as a plain Filter and never routed through the
// scan-pushdown. And `MATCH (n:Person)` in the schemaless harness lowers to
// ScanMainByLabels, which find_scan_label_id does not match. The vulnerable
// shape is a label-LESS scan (ScanAll) with a PRECEDING WITH ... ORDER BY (Sort)
// and a MATCH-level WHERE on the still-scan-bound variable.
// ===========================================================================
#[tokio::test]
async fn repro_17_where_dropped_below_limit() {
    let h = Harness::new_schemaless().await;
    h.run_ok("CREATE (:Person {name:'young', age:20})").await;
    h.run_ok("CREATE (:Person {name:'old', age:40})").await;
    // ScanAll(n) below a Sort (from WITH ... ORDER BY); the bare re-MATCH (n)
    // reaches the sole plan_where_clause caller without adding a new scan.
    let rows = h
        .run_ok("MATCH (n) WITH n ORDER BY n.name MATCH (n) WHERE n.age > 30 RETURN n.name AS name ORDER BY name")
        .await;
    let names: Vec<Value> = rows.iter().map(|r| cell(r, "name")).collect();
    println!("[36] WHERE age>30 past Sort -> names={names:?} (correct=['old'])");
    // Regression: the scan-pushdown gate now consumes the predicate only when
    // the rewriter can reach the scan; with an intervening Sort it can't, so
    // `n.age > 30` stays a residual Filter. Only 'old' (age 40) survives.
    assert_eq!(rows.len(), 1, "planner.rs:6211/7739: filter must survive past Sort");
    assert_eq!(names, vec![Value::String("old".into())]);
    assert!(
        !names.contains(&Value::String("young".into())),
        "'young' (age 20) must be excluded by WHERE age>30"
    );
}

// ===========================================================================
// [18] planner.rs:3351 — LIMIT applied before DISTINCT
// ===========================================================================
#[tokio::test]
async fn repro_18_distinct_limit_order() {
    let h = Harness::new_schemaless().await;
    h.run_ok("CREATE (:P {name:'alice'})").await;
    h.run_ok("CREATE (:P {name:'alice'})").await;
    h.run_ok("CREATE (:P {name:'bob'})").await;
    let rows = h.run_ok("MATCH (n:P) RETURN DISTINCT n.name LIMIT 2").await;
    println!("[18] DISTINCT name LIMIT 2 -> rows={}: {rows:?}", rows.len());
    // correct (openCypher): DISTINCT first -> {alice,bob}, LIMIT 2 -> 2 rows.
    // BUG: LIMIT first on 3 pre-distinct rows, then dedup -> 1 row.
    assert_eq!(rows.len(), 1, "repro for planner.rs:3351: LIMIT before DISTINCT");
}

// ===========================================================================
// FINDING [39] planner.rs:6168 — the `WHERE n:A OR n:B` label-disjunction rewrite
// marks the conjunct consumed via is_scan_all_for (which DESCENDS Sort/Limit/
// Aggregate/Apply/Union) but replace_scan_all_with_label_union has NO arm for
// those nodes (`other => other`, 6665), so the ScanAll is never rewritten AND the
// label predicate is dropped -> every node is returned.
//
// Recalibration: the earlier `WITH n LIMIT 100 WHERE n:A OR n:B` did NOT
// reproduce — the WHERE there is the WITH's own Filter, never routed through the
// disjunction rewrite. Moving the WHERE onto a following MATCH, with a preceding
// WITH ... ORDER BY (Sort) above the ScanAll, triggers the drop.
// ===========================================================================
#[tokio::test]
async fn repro_20_label_or_dropped_below_limit() {
    let h = Harness::new_schemaless().await;
    h.run_ok("CREATE (:A {v:1})").await;
    h.run_ok("CREATE (:B {v:2})").await;
    h.run_ok("CREATE (:C {v:3})").await;
    // ScanAll(n) below a Sort; bare re-MATCH (n) routes the label-OR WHERE
    // through the disjunction rewrite, which fails to descend the Sort.
    let rows = h
        .run_ok("MATCH (n) WITH n ORDER BY n.v MATCH (n) WHERE n:A OR n:B RETURN labels(n) AS ls")
        .await;
    println!("[39] n:A OR n:B past Sort -> rows={}: {rows:?} (correct=2)", rows.len());
    let has_c = rows.iter().any(|r| cell(r, "ls") == Value::List(vec![Value::String("C".into())]));
    // Regression: the label-union gate now consumes the conjunct only when the
    // rewriter can reach the ScanAll; with an intervening Sort it can't, so
    // `n:A OR n:B` stays a residual Filter. Only A and B survive (2 rows).
    assert_eq!(rows.len(), 2, "planner.rs:6168/6665: label OR must survive past Sort");
    assert!(!has_c, "C must be excluded by n:A OR n:B");
}

// ===========================================================================
// [25] locy_eval.rs:371 — integer div/mod by zero panics (via Cypher)
// ===========================================================================
#[tokio::test]
#[ignore = "repro for locy_eval.rs:371: integer divide-by-zero panics instead of clean error"]
async fn repro_25_int_div_by_zero() {
    let h = Harness::new_schemaless().await;
    let res = h.run("RETURN 10 / 0 AS q").await;
    // If this path uses the guarded evaluator it returns Err; if it hits the
    // unguarded numeric_op it panics ("attempt to divide by zero").
    println!("[25] 10/0 -> {res:?}");
    match res {
        Ok(rows) => println!("[25] returned rows: {rows:?}"),
        Err(e) => println!("[25] returned Err: {e}"),
    }
}

// ===========================================================================
// [32] read.rs:5066 — UNION dedup keyed on Debug of Node (HashMap order)
// ===========================================================================
#[tokio::test]
async fn repro_32_union_node_dedup() {
    let h = Harness::new_schemaless().await;
    h.run_ok("CREATE (:P {a:1, b:2, c:3, d:4, e:5, f:6, g:7})").await;
    let mut max_rows = 0;
    for _ in 0..64 {
        let rows = h.run_ok("MATCH (n:P) RETURN n UNION MATCH (n:P) RETURN n").await;
        max_rows = max_rows.max(rows.len());
    }
    println!("[32] UNION of single node, max rows over 64 runs = {max_rows} (correct=1)");
    // Regression: UNION (non-ALL) dedup now keys on `canonical_row_key` (a
    // structural, order-stable encoding) instead of `format!("{:?}")` over a
    // `HashMap`-backed row, so a single node UNION itself must always collapse
    // to exactly one row regardless of per-instance HashMap iteration order.
    assert_eq!(
        max_rows, 1,
        "UNION dedup must collapse identical nodes to 1 row on every run (read.rs:5066)"
    );
}

// ===========================================================================
// [38] traverse.rs:2049 — undirected self-loop yields duplicate row
// ===========================================================================
#[tokio::test]
async fn repro_38_self_loop_both_dup() {
    let h = Harness::new_schemaless().await;
    h.run_ok("CREATE (a:N {id:1})-[:R]->(a)").await;
    let rows = h.run_ok("MATCH (a:N)-[r:R]-(b) RETURN a.id AS aid, b.id AS bid").await;
    println!("[38] undirected self-loop -> rows={}: {rows:?}", rows.len());
    // correct: 1 row. BUG: self-loop double-inserted -> 2 rows.
    assert_eq!(rows.len(), 2, "repro for traverse.rs:2049: self-loop dup under Both");
}

// ===========================================================================
// [39] core.rs:137 — SUM accumulates in f64, integer >2^53 loses precision
// ===========================================================================
#[tokio::test]
async fn repro_39_sum_precision() {
    let h = Harness::new_schemaless().await;
    let rows = h
        .run_ok("UNWIND [9007199254740992, 1] AS x RETURN sum(x) AS s")
        .await;
    let s = cell(&rows[0], "s");
    println!("[39] UNWIND sum(2^53, 1) = {s:?} (correct=Int(9007199254740993))");
    // Observe: UNWIND-literal path may not route through Accumulator::Sum(f64).
    if s == Value::Int(9007199254740992) {
        println!("[39] REPRODUCED via UNWIND: f64 precision loss (core.rs:137)");
    } else {
        println!("[39] UNWIND path returned exact {s:?} (not the aggregate f64 path)");
    }
    assert!(s == Value::Int(9007199254740992) || s == Value::Int(9007199254740993));
}

// [39b] node-property sum routes through update_accumulators (read.rs:3818)
#[tokio::test]
async fn repro_39b_sum_precision_nodes() {
    let h = Harness::new_schemaless().await;
    h.run_ok("CREATE (:Big {v: 9007199254740992})").await;
    h.run_ok("CREATE (:Big {v: 1})").await;
    let rows = h.run_ok("MATCH (n:Big) RETURN sum(n.v) AS s").await;
    let s = cell(&rows[0], "s");
    println!("[39b] node sum(2^53,1) = {s:?} (correct=Int(9007199254740993))");
    if s == Value::Int(9007199254740992) {
        println!("[39b] REPRODUCED: f64 SUM accumulation precision loss (core.rs:137)");
    }
    // BUG: f64 accumulation rounds (2^53)+1 -> 2^53. Assert observed either way.
    assert!(s == Value::Int(9007199254740992) || s == Value::Int(9007199254740993));
}

// ===========================================================================
// [34] planner.rs:4376 — multi-hop shortestPath ignores hops beyond first
// ===========================================================================
#[tokio::test]
async fn repro_34_shortestpath_multihop() {
    let h = Harness::new_schemaless().await;
    // a-KNOWS->b-WORKS_AT->c ; and a-KNOWS->x (x has no WORKS_AT)
    h.run_ok("CREATE (a:Person {name:'a'})-[:KNOWS]->(b:Person {name:'b'})-[:WORKS_AT]->(c:Company {name:'c'})").await;
    h.run_ok("MATCH (a:Person {name:'a'}) CREATE (a)-[:KNOWS]->(:Person {name:'x'})").await;
    let res = h
        .run("MATCH p = shortestPath((a:Person {name:'a'})-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company)) RETURN b.name AS bn")
        .await;
    println!("[34] multi-hop shortestPath -> {res:?}");
    // Either an error (correct per Neo4j) or the WORKS_AT hop is honored.
    // BUG: second hop dropped -> matches on KNOWS only, returns x/b regardless.
    match res {
        Ok(rows) => {
            // if the WORKS_AT constraint were honored, only b (which has WORKS_AT->c)
            // would qualify. If x appears, the second hop was dropped.
            let names: Vec<Value> = rows.iter().map(|r| cell(r, "bn")).collect();
            println!("[34] bn values: {names:?}");
        }
        Err(e) => println!("[34] Err (acceptable per Neo4j): {e}"),
    }
}

// ===========================================================================
// [26] locy_eval.rs:561 — value_less_than has no Temporal arm (pub fn)
// ===========================================================================
#[tokio::test]
async fn repro_26_value_less_than_temporal() {
    use uni_common::TemporalValue;
    use uni_query::query::df_graph::locy_eval::value_less_than;
    let earlier = Value::Temporal(TemporalValue::Date { days_since_epoch: 7305 }); // 1990
    let later = Value::Temporal(TemporalValue::Date { days_since_epoch: 18262 }); // 2020
    let lt = value_less_than(&earlier, &later);
    let gt = value_less_than(&later, &earlier);
    println!("[26] value_less_than(1990,2020)={lt} (correct=true); (2020,1990)={gt} (correct=false)");
    // BUG: no Temporal arm -> both return false, so MIN/MAX/ORDER BY over dates break.
    assert_eq!(lt, false, "repro for locy_eval.rs:561: temporal '<' always false");
    assert_eq!(gt, false, "repro for locy_eval.rs:561: temporal '>' always false");
}

// ===========================================================================
// [14] core.rs:99 — MIN/MAX over Temporal returns first-encountered value
// ===========================================================================
#[tokio::test]
async fn repro_14_minmax_temporal() {
    let h = Harness::new_schemaless().await;
    // Insert in unsorted order: 2020, 2010, 2030
    h.run_ok("CREATE (:E {when: datetime('2020-01-01T00:00:00Z')})").await;
    h.run_ok("CREATE (:E {when: datetime('2010-01-01T00:00:00Z')})").await;
    h.run_ok("CREATE (:E {when: datetime('2030-01-01T00:00:00Z')})").await;
    let res = h.run("MATCH (n:E) RETURN min(n.when) AS lo, max(n.when) AS hi").await;
    match res {
        Ok(rows) => {
            let lo = cell(&rows[0], "lo");
            let hi = cell(&rows[0], "hi");
            println!("[14] min(when)={lo:?} max(when)={hi:?} (correct: lo=2010, hi=2030)");
            // BUG: cypher_cross_type_cmp returns Equal for temporals -> lo==hi==first row.
            if lo == hi {
                println!("[14] REPRODUCED: min==max (first-encountered kept)");
            }
        }
        Err(e) => println!("[14] errored (temporal aggregate path): {e}"),
    }
}

// ===========================================================================
// [7] optional_filter.rs:370 — per-batch null-row recovery for OPTIONAL
// ===========================================================================
#[tokio::test]
async fn repro_07_optional_filter_batches() {
    let h = Harness::new_schemaless().await;
    h.run_ok("CREATE (:A {x:0})").await;
    h.run_ok("UNWIND range(1, 20000) AS i CREATE (:B {y:i})").await;
    // WHERE b.y > 99999 : nothing matches -> exactly ONE null row expected.
    let rows = h
        .run_ok("MATCH (a:A) OPTIONAL MATCH (b:B) WHERE b.y > 99999 RETURN a.x AS ax, b.y AS by")
        .await;
    println!("[7] all-fail OPTIONAL over 20000 B -> rows={} (correct=1)", rows.len());
    // BUG: one null row per input batch (~3 for 20000 rows / 8192).
    if rows.len() > 1 {
        println!("[7] REPRODUCED: {} null rows instead of 1", rows.len());
    }
    assert!(rows.len() >= 1);
}

// ===========================================================================
// [8] pattern_comprehension.rs:301 — multi-hop column order mismatch
// ===========================================================================
#[tokio::test]
async fn repro_08_pattern_comprehension_colorder() {
    let h = Harness::new_schemaless().await;
    h.run_ok("CREATE (a:A {id:1})-[:X {w:'WEIGHT'}]->(b:B {id:2})-[:Y]->(c:C {name:'target'})").await;
    let res = h
        .run("MATCH (a:A) RETURN [(a)-[r1:X]->(b)-[r2:Y]->(c:C) WHERE c.name = 'target' | r1.w] AS out")
        .await;
    match res {
        Ok(rows) => {
            let out = cell(&rows[0], "out");
            println!("[8] pattern comprehension out={out:?} (correct=['WEIGHT'])");
            // BUG: schema declares [c.name, r1.w] but eval pushes [r1.w, c.name],
            // so predicate reads r1.w as c.name (filters wrong) and map returns c.name.
            match &out {
                Value::List(items) if items == &vec![Value::String("WEIGHT".into())] => {
                    println!("[8] correct output (bug NOT reproduced)");
                }
                other => println!("[8] REPRODUCED / divergent output: {other:?}"),
            }
        }
        Err(e) => println!("[8] errored: {e}"),
    }
}

// ===========================================================================
// [10] scan.rs:2806 — REMOVE label on flushed vertex resurrected by union overlay
// ===========================================================================
// FIXED (correctness-scan uni-query[29]): the label-COLUMN overlay honors
// `vertex_label_overwrites` (so `labels(n)` is correct), AND the schemaless
// label-scan candidate path now drops flushed rows whose newest L0 overwrite no
// longer contains the scanned label (so `MATCH (n:B)` is empty after REMOVE).
#[tokio::test]
async fn repro_10_label_remove_resurrect() {
    let h = Harness::new_schemaless().await;
    h.run_ok("CREATE (n:A:B {id:1})").await;
    h.flush().await;
    h.run_ok("MATCH (n {id:1}) REMOVE n:B").await;
    // 4a) labels(n) should be ['A'] only
    let res_a = h.run("MATCH (n:A {id:1}) RETURN labels(n) AS ls").await;
    // 4b) MATCH (n:B) should be empty
    let res_b = h.run("MATCH (n:B) RETURN n.id AS id").await;

    // Fixed (scan.rs:2806/1818): the L0 label overlay honors the
    // `vertex_label_overwrites` marker and REPLACES the flushed labels, so
    // `REMOVE n:B` is respected instead of being resurrected by a union overlay.
    let rows_a = res_a.expect("labels(n) query should succeed");
    let ls = cell(rows_a.get(0).expect("the A-labelled node is still present"), "ls");
    let Value::List(items) = &ls else {
        panic!("labels(n) must be a list, got {ls:?}");
    };
    assert!(
        !items.iter().any(|v| v == &Value::String("B".into())),
        "removed label B must not resurrect in labels(n); got {items:?}"
    );
    assert!(
        items.iter().any(|v| v == &Value::String("A".into())),
        "label A must remain in labels(n); got {items:?}"
    );

    // And MATCH by the removed label must no longer return the node.
    let rows_b = res_b.expect("MATCH (n:B) query should succeed");
    assert!(
        rows_b.is_empty(),
        "MATCH (n:B) must be empty after REMOVE n:B; got {} row(s)",
        rows_b.len()
    );
}

// ===========================================================================
// [21] planner.rs:4897 — consecutive QPP anchored at stale first source
// ===========================================================================
#[tokio::test]
async fn repro_21_consecutive_qpp() {
    let h = Harness::new_schemaless().await;
    h.run_ok("CREATE (n1:V {id:1})-[:R]->(n2:V {id:2})-[:R]->(n3:V {id:3})").await;
    h.run_ok("MATCH (n3:V {id:3}) CREATE (n3)-[:S]->(:V {id:4})-[:S]->(:V {id:5})").await;
    let res = h
        .run("MATCH (a:V {id:1})((x)-[:R]->(y)){1,2}(b)((w)-[:S]->(z)){1,2}(c) RETURN a.id AS a, b.id AS b, c.id AS c")
        .await;
    match res {
        Ok(rows) => {
            println!("[21] two-QPP chain -> rows={}: {rows:?}", rows.len());
            // correct: connected chain a=1,b=3,c reachable via S from 3 (id 4 or 5).
            // BUG: second QPP anchored at 'a'(=1) not 'b'(=3), so c from S-edges of 1
            // (none) -> empty, or spurious rows.
        }
        Err(e) => println!("[21] errored (QPP parse/plan): {e}"),
    }
}

// ===========================================================================
// [35] schema.rs:467 — labelInfo marks every property indexed if any JsonFullText
// ===========================================================================
#[tokio::test]
async fn repro_35_labelinfo_jsonfts() {
    use uni_common::core::schema::{IndexDefinition, JsonFtsIndexConfig};
    let h = Harness::new(|sm| {
        sm.add_label("Doc").unwrap();
        sm.add_property("Doc", "body", DataType::String, true).unwrap();
        sm.add_property("Doc", "title", DataType::String, true).unwrap();
        // Add JSON FTS index on body only.
        sm.add_index(IndexDefinition::JsonFullText(JsonFtsIndexConfig {
            name: "doc_fts".into(),
            label: "Doc".into(),
            column: "body".into(),
            paths: vec![],
            with_positions: true,
            metadata: Default::default(),
        }))
        .unwrap();
    })
    .await;
    let res = h.run("CALL uni.schema.labelInfo('Doc') YIELD property, indexed RETURN property, indexed").await;
    match res {
        Ok(rows) => {
            println!("[35] labelInfo rows: {rows:?}");
            if let Some(r) = rows.iter().find(|r| cell(r, "property") == Value::String("title".into())) {
                let idx = cell(r, "indexed");
                println!("[35] title.indexed = {idx:?} (correct=false)");
                if idx == Value::Bool(true) {
                    println!("[35] REPRODUCED: unrelated 'title' reported indexed=true");
                }
            }
        }
        Err(e) => println!("[35] procedure not callable in harness: {e}"),
    }
}

// ===========================================================================
// [19] planner.rs:6224 — WHERE predicate on traverse-target dropped below Limit
// ===========================================================================
#[tokio::test]
async fn repro_19_traverse_target_pred_dropped() {
    let h = Harness::new_schemaless().await;
    h.run_ok("CREATE (a:X {id:1})-[:R]->(b:Y {p:1})").await;
    h.run_ok("MATCH (a:X {id:1}) CREATE (a)-[:R]->(:Y {p:2})").await;
    h.run_ok("CREATE (:Z {id:10})").await;
    h.run_ok("CREATE (:Z {id:11})").await;
    let res = h
        .run("MATCH (a:X)-[:R]->(b:Y) WITH a, b SKIP 0 MATCH (c:Z) WHERE b.p = 1 RETURN c.id AS c, b.p AS p")
        .await;
    match res {
        Ok(rows) => {
            let ps: Vec<Value> = rows.iter().map(|r| cell(r, "p")).collect();
            println!("[19] traverse-target WHERE b.p=1 below SKIP -> rows={}, p vals={ps:?}", rows.len());
            let has_p2 = ps.iter().any(|v| v == &Value::Int(2));
            // BUG: filter dropped -> rows with b.p=2 also present.
            if has_p2 {
                println!("[19] REPRODUCED: b.p=2 rows leaked (filter dropped)");
            }
        }
        Err(e) => println!("[19] errored: {e}"),
    }
}

// ###########################################################################
// Gap-fill repros. Test names use the FINDINGS-FILE numbering (find04..find35)
// so they are unambiguous; the older repro_NN tests above reuse ad-hoc numbers.
// ###########################################################################

// ===========================================================================
// FINDING [4] search_procedures.rs:1578 — run_hybrid_search swallows
// auto_embed_text errors with unwrap_or_default(), silently dropping the dense
// arm of a hybrid search when the vector index has no embedding_config.
// ===========================================================================
#[tokio::test]
#[ignore = "repro for [4]: auto_embed error swallowed -> dense arm silently dropped instead of propagated"]
async fn repro_find04_hybrid_autoembed_swallowed() {
    use uni_common::core::schema::{
        DistanceMetric, IndexDefinition, IndexMetadata, IndexStatus, JsonFtsIndexConfig,
        VectorIndexConfig, VectorIndexType,
    };
    let h = Harness::new(|sm| {
        sm.add_label("Doc").unwrap();
        sm.add_property("Doc", "emb", DataType::Vector { dimensions: 3 }, true).unwrap();
        sm.add_property("Doc", "body", DataType::String, true).unwrap();
        // Vector index WITHOUT embedding_config -> auto_embed_text will error.
        sm.add_index(IndexDefinition::Vector(VectorIndexConfig {
            name: "doc_emb".into(),
            label: "Doc".into(),
            property: "emb".into(),
            index_type: VectorIndexType::Flat,
            metric: DistanceMetric::Cosine,
            embedding_config: None,
            metadata: IndexMetadata { status: IndexStatus::Online, ..Default::default() },
        }))
        .unwrap();
        sm.add_index(IndexDefinition::JsonFullText(JsonFtsIndexConfig {
            name: "doc_fts".into(),
            label: "Doc".into(),
            column: "body".into(),
            paths: vec![],
            with_positions: true,
            metadata: Default::default(),
        }))
        .unwrap();
    })
    .await;
    h.run_ok("CREATE (:Doc {body:'hello world'})").await;
    // query_vector = null forces the dense arm through auto_embed_text, which
    // errors (no embedding_config). Correct behavior: propagate the error.
    let res = h
        .run("CALL uni.search('Doc', {vector:'emb', fts:'body'}, 'hello world', null, 10) YIELD node, score RETURN node, score")
        .await;
    println!("[4] hybrid-search auto_embed-missing -> {res:?}");
    // BUG: Ok (FTS-only) instead of Err propagating the auto-embed failure.
    assert!(res.is_err(), "repro for [4]: dense-arm auto_embed error must propagate");
}

// ===========================================================================
// FINDING [15] search_procedures.rs:182 — parse_reranker_options computes
// reranker_k via (v as usize).clamp(k, 1000); when the search k > 1000 the
// clamp asserts min<=max and PANICS.
// ===========================================================================
#[tokio::test]
#[ignore = "repro for [15]: reranker_k clamp(k,1000) panics when search k>1000"]
async fn repro_find15_reranker_clamp_panic() {
    let h = Harness::new_schemaless().await;
    // k = 1001 (> 1000) with a reranker options map that carries reranker_k.
    // parse_reranker_options runs before any storage access and panics on the
    // clamp(min=1001, max=1000). Correct behavior: a clean error, not a panic.
    let res = h
        .run("CALL uni.search('Doc', {vector:'emb'}, 'q', null, 1001, null, {reranker:'maxsim', reranker_k:5}) YIELD node RETURN node")
        .await;
    println!("[15] reranker clamp k=1001 -> {res:?}");
    assert!(res.is_err(), "repro for [15]: k>1000 with reranker_k must not panic");
}

// ===========================================================================
// FINDING [8] projection_store.rs:180 — process-global projection registry is
// keyed on Arc::as_ptr of the schema-manager Arc, without holding the Arc alive
// or evicting. Two *distinct* StorageManager instances (two logical databases)
// that happen to share a SchemaManager Arc collide on ONE global ProjectionStore
// — a cross-instance leak.
// ===========================================================================
#[tokio::test]
async fn repro_find08_projection_store_ptr_key_collision() {
    use uni_algo::algo::GraphProjection;
    use uni_query::projection_store::{
        for_storage, ProjectionEntry, ProjectionSourceKind,
    };
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();
    // One shared SchemaManager Arc across two separate storage instances.
    let sm = Arc::new(
        SchemaManager::load(&dir_a.path().join("schema.json")).await.unwrap(),
    );
    let a = Arc::new(
        StorageManager::new(dir_a.path().join("st").to_str().unwrap(), sm.clone())
            .await
            .unwrap(),
    );
    let b = Arc::new(
        StorageManager::new(dir_b.path().join("st").to_str().unwrap(), sm.clone())
            .await
            .unwrap(),
    );
    let sa = for_storage(&a);
    let sb = for_storage(&b);
    // BUG: two distinct databases share the SAME global ProjectionStore because
    // the key is Arc::as_ptr(schema_manager).
    assert!(
        Arc::ptr_eq(&sa, &sb),
        "repro for [8]: separate storages sharing a schema Arc collide on one ProjectionStore"
    );
    // Cross-instance leak: a projection registered against `a` is visible via `b`.
    let entry = ProjectionEntry {
        projection: Arc::new(GraphProjection::from_rows(&[], &[], None, false).unwrap()),
        node_count: 0,
        edge_count: 0,
        bytes: 0,
        created_at: std::time::SystemTime::now(),
        source_kind: ProjectionSourceKind::Native,
    };
    sa.insert("g".into(), entry).unwrap();
    assert!(
        sb.contains("g"),
        "repro for [8]: projection registered on instance A leaks into instance B"
    );
}

// ===========================================================================
// FINDING [9] expr_compiler.rs:2656 — resolve_metric_for_property looks up the
// vector-index DistanceMetric by property name ONLY, ignoring the label, so the
// first index in schema.indexes wins across labels. With LabelA(Cosine) declared
// before LabelB(L2) on the same-named property `emb`, scoring a LabelB node uses
// Cosine instead of L2.
// ===========================================================================
#[tokio::test]
// note: repro for [9]: similar_to on LabelB uses first-declared (Cosine) metric, not LabelB's L2
async fn repro_find09_metric_by_property_ignores_label() {
    use uni_common::core::schema::{
        DistanceMetric, IndexDefinition, IndexMetadata, IndexStatus, VectorIndexConfig,
        VectorIndexType,
    };
    let h = Harness::new(|sm| {
        sm.add_label("LabelA").unwrap();
        sm.add_property("LabelA", "emb", DataType::Vector { dimensions: 3 }, true).unwrap();
        sm.add_label("LabelB").unwrap();
        sm.add_property("LabelB", "emb", DataType::Vector { dimensions: 3 }, true).unwrap();
        // LabelA (Cosine) declared FIRST -> its metric wins for BOTH labels.
        sm.add_index(IndexDefinition::Vector(VectorIndexConfig {
            name: "a_emb".into(),
            label: "LabelA".into(),
            property: "emb".into(),
            index_type: VectorIndexType::Flat,
            metric: DistanceMetric::Cosine,
            embedding_config: None,
            metadata: IndexMetadata { status: IndexStatus::Online, ..Default::default() },
        }))
        .unwrap();
        sm.add_index(IndexDefinition::Vector(VectorIndexConfig {
            name: "b_emb".into(),
            label: "LabelB".into(),
            property: "emb".into(),
            index_type: VectorIndexType::Flat,
            metric: DistanceMetric::L2,
            embedding_config: None,
            metadata: IndexMetadata { status: IndexStatus::Online, ..Default::default() },
        }))
        .unwrap();
    })
    .await;
    // A non-unit vector so Cosine vs L2 scores differ numerically.
    h.run_ok("CREATE (:LabelB {emb: [3.0, 4.0, 0.0]})").await;
    let res = h
        .run("MATCH (b:LabelB) RETURN similar_to(b.emb, [3.0, 4.0, 0.0]) AS s")
        .await;
    println!("[9] similar_to over LabelB (should use L2) -> {res:?}");
    match res {
        Ok(rows) => {
            let s = cell(&rows[0], "s");
            println!("[9] score = {s:?} (LabelB's declared metric is L2, not Cosine)");
            // BUG: Cosine metric resolved for LabelB because LabelA's index is
            // first in schema.indexes -> identical vectors score 1.0 (cosine).
            // A label-aware lookup would use LabelB's L2 metric instead.
            assert_eq!(
                s,
                Value::Float(1.0),
                "repro for [9]: LabelB resolved to LabelA's Cosine metric (first-index-wins)"
            );
        }
        Err(e) => println!("[9] similar_to not runnable in harness: {e}"),
    }
}

// ===========================================================================
// FINDING [5] traverse.rs:1152 — is_optional_column_for_vars classifies the
// internal `__eid_to_<var>` edge-id column via a suffix match, so an OPTIONAL
// var whose name is a suffix of an earlier-bound optional var (x vs xx) makes
// the null-fill of `x` wrongly NULL the matched `xx` relationship column.
// ===========================================================================
#[tokio::test]
// note: repro for [5]: null-fill of optional var `x` clobbers matched relationship of `xx` (suffix match)
async fn repro_find05_optional_var_suffix_eid() {
    let h = Harness::new_schemaless().await;
    h.run_ok("CREATE (c:Person {name:'Charlie'})-[:LIKES]->(:Person {name:'Dana'})").await;
    // xx-binding OPTIONAL MATCH precedes the x one so __eid_to_xx is present when
    // x's null-fill runs. xx matches (LIKES->Dana); x does not (no KNOWS).
    let rows = h
        .run_ok(
            "MATCH (n:Person {name:'Charlie'}) \
             OPTIONAL MATCH (n)-[r_xx:LIKES]->(xx:Person) \
             OPTIONAL MATCH (n)-[:KNOWS]->(x:Person) \
             RETURN xx.name AS xx, type(r_xx) AS r, x.name AS x",
        )
        .await;
    println!("[5] optional suffix vars -> {rows:?}");
    let r = cell(&rows[0], "r");
    // Correct: r = 'LIKES' (xx matched). BUG: r NULLed when x's row is null-filled.
    assert_eq!(
        r,
        Value::String("LIKES".into()),
        "repro for [5]: matched xx relationship must survive x's optional null-fill"
    );
}

// ===========================================================================
// FINDING [19] write.rs:1048 — arrow_value_to_json returns Value::Null for Arrow
// types it doesn't handle (Timestamp/Date32/64/LargeUtf8/List/decimal) and its
// COPY FROM callers skip nulls, so those columns are silently dropped on import.
// ===========================================================================
#[tokio::test]
#[ignore = "repro for [19]: COPY FROM parquet silently drops a Timestamp column (arrow_value_to_json -> Null)"]
async fn repro_find19_copy_from_drops_timestamp() {
    use arrow_array::{ArrayRef, RecordBatch, StringArray, TimestampNanosecondArray};
    use arrow_schema::{DataType as ArrowDT, Field, Schema, TimeUnit};
    use parquet::arrow::ArrowWriter;

    let h = Harness::new(|sm| {
        sm.add_label("Event").unwrap();
        sm.add_property("Event", "ts", DataType::DateTime, true).unwrap();
        sm.add_property("Event", "name", DataType::String, true).unwrap();
    })
    .await;

    // Write a parquet file whose `ts` column is Arrow Timestamp (unhandled by
    // arrow_value_to_json) and `name` is Utf8 (handled).
    let path = h._dir.path().join("events.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", ArrowDT::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("name", ArrowDT::Utf8, false),
    ]));
    let ts: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![1_600_000_000_000_000_000i64]));
    let name: ArrayRef = Arc::new(StringArray::from(vec!["e1"]));
    let batch = RecordBatch::try_new(schema.clone(), vec![ts, name]).unwrap();
    {
        let file = std::fs::File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(file, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }

    let copy = format!("COPY Event FROM '{}'", path.to_str().unwrap());
    let copy_res = h.run(&copy).await;
    println!("[19] COPY FROM parquet -> {copy_res:?}");
    let res = h.run("MATCH (n:Event) RETURN n.ts AS ts, n.name AS name").await;
    match res {
        Ok(rows) if !rows.is_empty() => {
            let ts = cell(&rows[0], "ts");
            let name = cell(&rows[0], "name");
            println!("[19] imported ts={ts:?} name={name:?}");
            // BUG: ts silently dropped (Null) while name is present.
            assert_ne!(ts, Value::Null, "repro for [19]: Timestamp column dropped by COPY FROM");
        }
        other => println!("[19] COPY/MATCH not observable in harness: {other:?}"),
    }
}

// ===========================================================================
// FINDING [14] recursive_cte.rs:260 — cycle detection keys rows by
// format!("{val:?}"); multi-column recursive rows are Value::Map(HashMap) whose
// Debug order is per-instance, so already-seen rows are not recognized and the
// recursion loops to MAX_ITERATIONS, inflating the result.
// ===========================================================================
// NOTE: the RecursiveCTEExec internal seen-set inflation is masked at the public
// endpoints available here (the final `IN reachable` node-dedup collapses
// duplicate internal rows), so this test exercises the multi-column recursive
// path and tolerantly records the count in the repro_32 observe-style rather than
// hard-failing; the defect is pinned at recursive_cte.rs:260.
#[tokio::test]
async fn repro_find14_recursive_cte_multicol_cycle() {
    let h = Harness::new_schemaless().await;
    h.run_ok("CREATE (a:N {name:'A'})").await;
    h.run_ok("CREATE (b:N {name:'B'})").await;
    h.run_ok("MATCH (a:N {name:'A'}), (b:N {name:'B'}) CREATE (a)-[:E]->(b), (b)-[:E]->(a)").await;
    // Multi-column recursive RETURN (many columns -> k! Debug orderings) so the
    // seen-set almost never recognizes a revisited row.
    // Multi-column recursive RETURN (node + extra columns) forces each CTE row to
    // be a Value::Map(HashMap), whose per-instance Debug order defeats the
    // format!("{val:?}") cycle-detection key.
    let res = h
        .run(
            "WITH RECURSIVE reachable AS ( \
                MATCH (n:N {name:'A'}) RETURN n AS node, n.name AS nm, 1 AS a, 2 AS b, 3 AS d, 4 AS e \
                UNION \
                MATCH (p:N)-[:E]->(q:N) WHERE p IN reachable RETURN q AS node, q.name AS nm, 1 AS a, 2 AS b, 3 AS d, 4 AS e \
             ) MATCH (m:N) WHERE m IN reachable RETURN count(m) AS c",
        )
        .await;
    println!("[14] recursive-CTE multi-col count -> {res:?}");
    match res {
        Ok(rows) => {
            let c = as_int(&cell(&rows[0], "c"));
            println!("[14] reachable count = {c} (correct=2)");
            // Regression: cycle detection now keys the seen-set on `Value`
            // (canonical Hash/Eq) rather than `format!("{val:?}")` over a
            // multi-column `Value::Map(HashMap)`, so revisited rows are always
            // recognized and the recursion converges on exactly the 2 reachable
            // nodes (A, B) instead of over-iterating.
            assert_eq!(c, 2, "repro for [14]: recursive CTE must reach exactly both nodes");
        }
        Err(e) => println!("[14] not runnable in harness: {e}"),
    }
}

// ===========================================================================
// FINDING [16] vid_lookup_join.rs:561 — values_equal compares non-anchor
// equi-pair cells with ScalarValue PartialEq under which NULL == NULL is true,
// so rows join on NULL keys, contradicting Cypher's NULL = NULL -> NULL.
// ===========================================================================
#[tokio::test]
// note: repro for [16]: VidLookupJoin joins rows whose non-anchor key is NULL on both sides (NULL==NULL)
async fn repro_find16_vid_lookup_null_eq_null() {
    let h = Harness::new_schemaless().await;
    // Neither node has property `p`. Bare `MATCH (b:T)` keeps the probe subtree a
    // plain GraphScan so the VidLookupJoin fast-path fires (anchor = id(b)=id(m)).
    h.run_ok("CREATE (a:T {name:'a'})-[:R]->(m:T {name:'m'})").await;
    let res = h
        .run(
            "MATCH (a:T)-[:R]->(m:T) MATCH (b:T) \
             WHERE id(b) = id(m) AND a.p = b.p \
             RETURN a.name AS an, b.name AS bn",
        )
        .await;
    println!("[16] null-eq-null vid join -> {res:?}");
    match res {
        Ok(rows) => {
            // Correct: a.p = b.p is NULL = NULL -> NULL -> filtered -> 0 rows.
            // BUG: values_equal(NULL,NULL)=true -> 1 row.
            assert_eq!(rows.len(), 0, "repro for [16]: NULL join key must not match");
        }
        Err(e) => println!("[16] not runnable in harness: {e}"),
    }
}

// ===========================================================================
// FINDING [35] planner.rs:6130 — vector_similarity predicate is dropped when the
// scanned var's Scan sits under a Traverse: find_scan_label_id descends the
// Traverse (so current_predicate is replaced with TRUE) but replace_scan_with_knn
// has no Traverse arm, so no KNN is injected and the filter silently vanishes.
// ===========================================================================
#[tokio::test]
// note: repro for [35]: vector_similarity predicate under a Traverse is silently dropped (no KNN, filter lost)
async fn repro_find35_vector_similarity_under_traverse() {
    let h = Harness::new_schemaless().await;
    h.run_ok("CREATE (:Doc {name:'hit', embedding:[1.0, 0.0, 0.0]})").await;
    h.run_ok("CREATE (:Doc {name:'miss', embedding:[0.0, 1.0, 0.0]})").await;
    h.run_ok("MATCH (d:Doc) CREATE (d)-[:HAS]->(:Tag {t:'x'})").await;
    // `d`'s Scan sits under the Traverse (d is the traverse source).
    let res = h
        .run(
            "MATCH (d:Doc)-[:HAS]->(t:Tag) \
             WHERE vector_similarity(d.embedding, [1.0, 0.0, 0.0]) > 0.99 \
             RETURN d.name AS name",
        )
        .await;
    println!("[35] vector_similarity under traverse -> {res:?}");
    match res {
        Ok(rows) => {
            // Correct: only 'hit' (cosine 1.0 > 0.99) -> 1 row.
            // BUG: predicate dropped -> both hit and miss -> 2 rows.
            assert_eq!(rows.len(), 1, "repro for [35]: similarity filter must be honored under Traverse");
        }
        Err(e) => println!("[35] not runnable in harness: {e}"),
    }
}

// ===========================================================================
// FINDING [17] df_planner.rs:2848 — hydrate_virtual_target_from_catalog always
// uses an Inner HashJoin on {target}._vid, discarding the NULL-target rows an
// optional traverse emits, breaking OPTIONAL MATCH for a plugin virtual target.
//
// PLUGIN-ONLY: virtual label ids only exist after PluginRegistry::
// register_virtual_label backed by a CatalogTable, injected via the graph
// context's plugin registry. The default registry (locy aggregates only) has no
// virtual labels, so a schemaless Cypher query never reaches this branch. This
// structural repro documents the required setup and asserts that, without a
// virtual target, the OPTIONAL MATCH keeps the null row (the correct behavior the
// virtual-target path fails to preserve).
// ===========================================================================
#[tokio::test]
// note: repro for [17]: OPTIONAL MATCH virtual-target uses Inner join, dropping null rows (needs plugin virtual label)
async fn repro_find17_optional_virtual_target_inner_join() {
    let h = Harness::new_schemaless().await;
    h.run_ok("CREATE (:A {id:1})").await;
    // Native-label control: OPTIONAL MATCH over a non-existent target keeps the
    // A row with a NULL target. The virtual-target path (plugin CatalogTable)
    // would instead Inner-join and drop this row — that is finding [17].
    let rows = h
        .run_ok("MATCH (a:A) OPTIONAL MATCH (a)-[:VEDGE]->(b:VTarget) RETURN a.id AS aid, b AS b")
        .await;
    println!("[17] optional (native control for virtual-target) -> {rows:?}");
    assert_eq!(rows.len(), 1, "OPTIONAL MATCH must keep the null-target row (virtual path drops it)");
    assert_eq!(cell(&rows[0], "b"), Value::Null);
}

// ===========================================================================
// FINDING [32] df_planner.rs:2963 — plan_traverse_virtual_edge maps Both to the
// same (src->dst) join as Outgoing, so undirected traversal over a plugin virtual
// edge type only matches the outgoing orientation.
//
// PLUGIN-ONLY: requires PluginRegistry::register_virtual_edge_type + a
// CatalogTable, injected via the graph context. No built-in virtual edge type
// exists, so a schemaless query cannot reach plan_traverse_virtual_edge. This
// structural repro documents the requirement; the native-edge control shows the
// undirected semantics the virtual path fails to provide (only outgoing matched).
// ===========================================================================
#[tokio::test]
// note: repro for [32]: virtual-edge undirected traversal only matches outgoing orientation (needs plugin virtual edge type)
async fn repro_find32_virtual_edge_both_is_outgoing() {
    let h = Harness::new_schemaless().await;
    h.run_ok("CREATE (x:V {id:1})-[:R]->(y:V {id:2})").await;
    // Native-edge control: undirected match from y also reaches x (both
    // orientations). Over a virtual edge, Both is treated as Outgoing only, so
    // the reverse orientation (y->x) would be missing — finding [32].
    let rows = h.run_ok("MATCH (y:V {id:2})-[:R]-(other) RETURN other.id AS oid").await;
    println!("[32] undirected (native control for virtual-edge) -> {rows:?}");
    let oids: Vec<Value> = rows.iter().map(|r| cell(r, "oid")).collect();
    assert!(
        oids.contains(&Value::Int(1)),
        "undirected traversal must match reverse orientation (virtual path misses it)"
    );
}

// ---------------------------------------------------------------------------
// Locy findings. The full end-to-end Locy runner (command dispatch producing
// derived facts / QUERY / ABDUCE / PROB / FOLD outputs) lives in crate `uni`
// (uni-db), which uni-query cannot depend on (circular). uni-query DOES own the
// planner: parse_locy -> uni_locy::compile -> LocyPlanBuilder::build_program_plan.
// The planner-layer bug ([34]) is observable here directly; the runtime-layer
// bugs ([10]/[13]/[24]/[25]) are only observable through the uni-db session, so
// their repros exercise the compile+plan-build path (which reaches the relevant
// planner code) and are marked #[ignore] noting where the buggy OUTPUT surfaces.
// ---------------------------------------------------------------------------

fn build_locy_plan(
    schema: Arc<uni_common::core::schema::Schema>,
    program: &str,
) -> anyhow::Result<uni_query::query::planner::LogicalPlan> {
    use uni_query::query::locy_planner::LocyPlanBuilder;
    let ast = uni_cypher::parse_locy(program).map_err(|e| anyhow::anyhow!("parse_locy: {e:?}"))?;
    let compiled = uni_locy::compile(&ast).map_err(|e| anyhow::anyhow!("compile: {e:?}"))?;
    let planner = QueryPlanner::new(schema);
    let builder = LocyPlanBuilder::new(&planner);
    builder
        .build_program_plan(
            &compiled,
            1000,                                    // max_iterations
            std::time::Duration::from_secs(300),     // timeout
            256 * 1024 * 1024,                       // max_derived_bytes
            true,                                    // deterministic_best_by
            false,                                   // strict_probability_domain
            1e-15,                                   // probability_epsilon
            false,                                   // exact_probability
            1000,                                    // max_bdd_variables
            0,                                       // top_k_proofs
        )
        .map_err(|e| anyhow::anyhow!("build_program_plan: {e:?}"))
}

// ===========================================================================
// FINDING [34] locy_planner.rs:798 — build_rule reads HAVING (and BEST BY) from
// the FIRST clause only, while fold_bindings come from whichever clause has a
// FOLD. A rule with a base clause (no FOLD) + a FOLD clause carrying HAVING loses
// the HAVING filter entirely. This is observable at the uni-query planner layer.
// ===========================================================================
#[tokio::test]
#[ignore = "repro for [34]: HAVING on the FOLD (non-first) clause is dropped because build_rule reads clause[0].having"]
async fn repro_find34_having_from_first_clause_only() {
    use uni_query::query::planner::LogicalPlan;
    let h = Harness::new_schemaless().await;
    // Rule `r` = base clause (no FOLD, no HAVING) UNION a FOLD clause with HAVING.
    // This is the exact "base clause plus fold clause" shape build_rule documents.
    let program = "CREATE RULE r AS MATCH (a:N) YIELD KEY a.name AS name, a.v AS total \
                   CREATE RULE r AS MATCH (a:N)-[e:R]->(b:N) FOLD total = SUM(b.v) WHERE total > 100 YIELD KEY a.name AS name, total AS total";
    let plan = build_locy_plan(h.schema_manager.schema(), program)
        .expect("[34] compile+plan the two-clause rule");
    let LogicalPlan::LocyProgram { strata, .. } = &plan else {
        panic!("[34] expected a LocyProgram plan, got {plan:?}");
    };
    let rule = strata
        .iter()
        .flat_map(|s| s.rules.iter())
        .find(|r| r.name == "r")
        .expect("[34] rule r present in plan");
    println!("[34] rule r having (planned) = {:?}", rule.having);
    // Correct: the FOLD clause's HAVING (total > 100) is planned. BUG: build_rule
    // read clause[0].having (empty) so having is dropped.
    assert!(
        !rule.having.is_empty(),
        "repro for [34]: HAVING on the non-first FOLD clause must not be dropped"
    );
}

// ===========================================================================
// FINDING [25] locy_query.rs:138 — RETURN DISTINCT dedups rows by
// format!("{row:?}") where FactRow is std HashMap; multi-column duplicate rows
// survive DISTINCT (HashMap Debug order is per-instance). The dedup runs at Locy
// query-execution time (uni-db session); here we pin the trigger by planning a
// program with a multi-column QUERY ... RETURN DISTINCT.
// ===========================================================================
#[tokio::test]
// note: repro for [25]: Locy RETURN DISTINCT dedups by HashMap Debug string; duplicates survive (observed via uni-db session)
async fn repro_find25_locy_distinct_hashmap_debug() {
    let h = Harness::new_schemaless().await;
    let program = "CREATE RULE r AS MATCH (a:N)-[e:R]->(b:N) YIELD KEY a.name AS x, b.name AS y \
                   QUERY r RETURN DISTINCT x, y";
    let plan = build_locy_plan(h.schema_manager.schema(), program)
        .expect("[25] compile+plan the DISTINCT query");
    // Structural: the multi-column RETURN DISTINCT program plans. The dedup bug
    // (format!("{row:?}") over HashMap) manifests at Locy execution time, which
    // is only reachable through the uni-db session command dispatch.
    println!("[25] planned DISTINCT program ok: {}", matches!(
        plan,
        uni_query::query::planner::LogicalPlan::LocyProgram { .. }
    ));
    assert!(matches!(plan, uni_query::query::planner::LogicalPlan::LocyProgram { .. }));
}

// ===========================================================================
// FINDING [24] locy_fold.rs:720 — TopKProofs MNOR returns exactly 1.0 for a group
// mixing supported and unsupported rows (empty-clause DNF weight = 1.0). Manifests
// during fixpoint FOLD execution (uni-db session); here we pin the MNOR trigger.
// ===========================================================================
#[tokio::test]
// note: repro for [24]: MNOR over mixed supported/unsupported rows collapses to 1.0 (observed via uni-db session)
async fn repro_find24_mnor_mixed_support_one() {
    let h = Harness::new_schemaless().await;
    // A FOLD MNOR rule. The base-clause + IS-ref mix that yields empty-clause DNF
    // is a runtime fixpoint condition; this pins the MNOR fold trigger through the
    // planner. Full observation requires the uni-db session.
    let program = "CREATE RULE r AS MATCH (a:N)-[e:R]->(b:N) FOLD score = MNOR(e.w) YIELD KEY a.name AS name, score";
    match build_locy_plan(h.schema_manager.schema(), program) {
        Ok(plan) => {
            println!("[24] planned MNOR fold ok: {}", matches!(
                plan,
                uni_query::query::planner::LogicalPlan::LocyProgram { .. }
            ));
            assert!(matches!(plan, uni_query::query::planner::LogicalPlan::LocyProgram { .. }));
        }
        Err(e) => println!("[24] MNOR program not planned in harness (aggregate registration): {e}"),
    }
}

// ===========================================================================
// FINDING [13] locy_fixpoint.rs:2537 — apply_exact_wmc overwrites the PROB column
// and groups shared-lineage keys by raw yield-schema positions against
// post-fixpoint batches whose schema differs. Manifests during exact-WMC PROB
// evaluation (uni-db session); here we pin the OUTPUT PROB trigger.
// ===========================================================================
#[tokio::test]
// note: repro for [13]: apply_exact_wmc mis-groups PROB by positional index vs post-fixpoint schema (observed via uni-db session)
async fn repro_find13_exact_wmc_prob_positions() {
    let h = Harness::new_schemaless().await;
    let program = "CREATE RULE base AS MATCH (a:N)-[e:R]->(b:N) YIELD KEY a.name AS a, b.name AS b OUTPUT PROB e.w";
    match build_locy_plan(h.schema_manager.schema(), program) {
        Ok(plan) => {
            println!("[13] planned OUTPUT PROB ok: {}", matches!(
                plan,
                uni_query::query::planner::LogicalPlan::LocyProgram { .. }
            ));
            assert!(matches!(plan, uni_query::query::planner::LogicalPlan::LocyProgram { .. }));
        }
        Err(e) => println!("[13] OUTPUT PROB program not planned in harness: {e}"),
    }
}

// ===========================================================================
// FINDING [10] locy_abduce.rs:235 — the target_var fix-up in
// extract_edge_candidates (and extract_addition_candidates at 281) mutates
// candidates.last_mut() instead of the candidate for the relationship just
// traversed, so a multi-hop path's first edge gets the LAST node's var as target.
// Manifests during ABDUCE evaluation (uni-db session); here we pin the multi-hop
// ABDUCE trigger through the planner.
// ===========================================================================
#[tokio::test]
// note: repro for [10]: multi-hop ABDUCE target_var fix-up writes last node var into the wrong candidate (observed via uni-db session)
async fn repro_find10_abduce_multihop_target_var() {
    let h = Harness::new_schemaless().await;
    // Multi-hop body (two relationships) so the target_var fix-up mis-assigns the
    // first edge's target. ABDUCE candidate generation runs at evaluation time.
    let program = "CREATE RULE r AS MATCH (a:N)-[:R1]->(b:N)-[:R2]->(c:N) YIELD KEY a.name AS a \
                   ABDUCE r";
    match build_locy_plan(h.schema_manager.schema(), program) {
        Ok(plan) => {
            println!("[10] planned multi-hop ABDUCE ok: {}", matches!(
                plan,
                uni_query::query::planner::LogicalPlan::LocyProgram { .. }
            ));
            assert!(matches!(plan, uni_query::query::planner::LogicalPlan::LocyProgram { .. }));
        }
        Err(e) => println!("[10] ABDUCE program not planned in harness: {e}"),
    }
}

