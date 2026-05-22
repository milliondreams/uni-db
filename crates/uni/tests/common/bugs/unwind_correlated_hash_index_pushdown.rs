// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro/regression: `UNWIND $list AS u MATCH (n:Label) WHERE n.prop = u`
//! (and the map-projecting variant `n.prop = u.field`) must use the
//! Hash index on `Label.prop` for an O(1)-per-key point lookup, not a
//! full label scan + post-scan FilterExec.
//!
//! Pre-fix: the planner's `try_plan_cross_join_as_hash_join` builds an
//! `Expr::In { expr: Property(Variable("n"), "prop"), list: List([...]) }`
//! and merges it into `Scan.filter` via `wrap_with_filter`. But at
//! physical-plan time `plan_scan` (df_planner.rs:1941) only calls
//! `build_indexed_property_pushdown` to render a Lance pushdown when the
//! analyzer detects a hash-indexed column — and somewhere in that chain
//! the IN-list filter isn't being routed into the Hash-index pushdown
//! path for entity_id-shaped joins. Result: GraphScanExec returns the
//! entire label, FilterExec drops everything except the IN-list matches.
//!
//! This is the **property-side** analogue of the `_vid IN (...)` case
//! that already works (Lance handles row-id pruning natively).
//!
//! Sister test pinning the *single-row* form: `issue_57_match_label_hash_index.rs`.
//! The single-row form works today; only the UNWIND-correlated form is broken.

use std::collections::HashMap;
use uni_db::{DataType, IndexType, ScalarType, Uni, Value};

const TOTAL_ROWS: usize = 200;

async fn setup_db() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .property("color", DataType::String)
        .index("name", IndexType::Scalar(ScalarType::Hash))
        .done()
        .apply()
        .await
        .unwrap();

    let session = db.session();
    let tx = session.tx().await.unwrap();
    let rows: Vec<uni_common::Properties> = (0..TOTAL_ROWS)
        .map(|i| {
            let mut h: HashMap<String, Value> = HashMap::new();
            h.insert("name".into(), Value::String(format!("name-{i:04}")));
            h.insert(
                "color".into(),
                Value::String(if i % 2 == 0 { "red".into() } else { "blue".into() }),
            );
            h
        })
        .collect();
    tx.bulk_insert_vertices("Item", rows).await.unwrap();
    tx.commit().await.unwrap();
    db
}

fn print_profile(label: &str, profile: &uni_query::ProfileOutput) {
    eprintln!("\n=== PROFILE: {label} ===");
    eprintln!("total_time_ms = {}", profile.total_time_ms);
    eprintln!("{:<40} {:>10} {:>10}", "operator", "rows", "time(ms)");
    eprintln!("{}", "-".repeat(64));
    for s in &profile.runtime_stats {
        eprintln!(
            "{:<40} {:>10} {:>10.3}",
            s.operator, s.actual_rows, s.time_ms
        );
    }
}

fn scan_rows(profile: &uni_query::ProfileOutput) -> usize {
    profile
        .runtime_stats
        .iter()
        .find(|s| s.operator.contains("GraphScanExec"))
        .map(|s| s.actual_rows)
        .expect("expected a GraphScanExec operator in the plan")
}

/// UNWIND of a list of primitive strings, MATCH against an indexed
/// property. The simplest UNWIND-correlated property-eq case.
///
/// `UNWIND $names AS u MATCH (n:Item) WHERE n.name = u`
#[tokio::test]
async fn unwind_primitive_list_property_eq_uses_hash_index() {
    let db = setup_db().await;

    let names: Vec<Value> = vec![
        Value::String("name-0010".into()),
        Value::String("name-0100".into()),
        Value::String("name-0150".into()),
    ];

    let (results, profile) = db
        .session()
        .query_with(
            "UNWIND $names AS u \
             MATCH (n:Item) WHERE n.name = u \
             RETURN n.name AS name ORDER BY name",
        )
        .param("names", Value::List(names))
        .profile()
        .await
        .unwrap();
    print_profile("UNWIND $names AS u WHERE n.name = u", &profile);

    assert_eq!(results.len(), 3, "should match the 3 names in the UNWIND");
    let rows = scan_rows(&profile);
    assert!(
        rows <= 10,
        "GraphScanExec returned {rows} rows for a 3-element UNWIND; \
         Hash index on Item.name NOT used in UNWIND-correlated path \
         (have {TOTAL_ROWS} total). build_indexed_property_pushdown is \
         failing to route the IN-list filter into the Hash pushdown path."
    );
}

/// UNWIND of map literals projecting a field, MATCH against an indexed
/// property. The shape the downstream session's prep_read uses.
///
/// `UNWIND $rows AS u MATCH (n:Item) WHERE n.name = u.name_key`
#[tokio::test]
async fn unwind_map_param_field_property_eq_uses_hash_index() {
    let db = setup_db().await;

    let mut row1 = HashMap::new();
    row1.insert("name_key".to_string(), Value::String("name-0010".into()));
    let mut row2 = HashMap::new();
    row2.insert("name_key".to_string(), Value::String("name-0100".into()));
    let mut row3 = HashMap::new();
    row3.insert("name_key".to_string(), Value::String("name-0150".into()));

    let updates: Vec<Value> = vec![Value::Map(row1), Value::Map(row2), Value::Map(row3)];

    let (results, profile) = db
        .session()
        .query_with(
            "UNWIND $rows AS u \
             MATCH (n:Item) WHERE n.name = u.name_key \
             RETURN n.name AS name ORDER BY name",
        )
        .param("rows", Value::List(updates))
        .profile()
        .await
        .unwrap();
    print_profile("UNWIND $rows AS u WHERE n.name = u.name_key", &profile);

    assert_eq!(results.len(), 3, "should match the 3 name_keys in the UNWIND");
    let rows = scan_rows(&profile);
    assert!(
        rows <= 10,
        "GraphScanExec returned {rows} rows for a 3-element map-projecting UNWIND; \
         Hash index on Item.name NOT used in UNWIND-correlated map-field path \
         (have {TOTAL_ROWS} total). Same root cause as the primitive-list variant \
         above; both flow through `build_indexed_property_pushdown`."
    );
}

/// Sanity baseline: the single-row form already works (issue_57). If
/// THIS test fails, the regression is elsewhere — not in the
/// UNWIND-correlation path.
#[tokio::test]
async fn baseline_single_row_property_eq_uses_hash_index() {
    let db = setup_db().await;

    let (results, profile) = db
        .session()
        .query_with("MATCH (n:Item) WHERE n.name = $t RETURN n.name AS name")
        .param("t", Value::String("name-0042".into()))
        .profile()
        .await
        .unwrap();
    print_profile("baseline: single-row WHERE", &profile);

    assert_eq!(results.len(), 1);
    let rows = scan_rows(&profile);
    assert!(
        rows <= 1,
        "Single-row Hash index pushdown is broken (baseline failure); \
         the UNWIND test failures above point at a deeper issue."
    );
}
