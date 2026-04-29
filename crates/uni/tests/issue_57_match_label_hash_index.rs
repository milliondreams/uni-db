// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro/regression for issue #57.
//!
//! `MATCH (n:Label {prop: $val})` and `MATCH (n:Label) WHERE n.prop = $val`
//! must use the hash index on `prop` for an O(1) point lookup, not a full
//! label scan + post-scan FilterExec.
//!
//! Pre-fix: GraphScanExec returns the entire label (`actual_rows == TOTAL_ROWS`).
//! Post-fix: GraphScanExec returns 1 (or 0 for missing).
//!
//! Run with:
//!   cargo nextest run -p uni --test issue_57_match_label_hash_index --no-capture

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
    for i in 0..TOTAL_ROWS {
        tx.query_with("CREATE (:Item {name: $n, color: $c})")
            .param("n", Value::String(format!("name-{i:04}")))
            .param(
                "c",
                Value::String(if i % 2 == 0 {
                    "red".into()
                } else {
                    "blue".into()
                }),
            )
            .fetch_all()
            .await
            .unwrap();
    }
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

#[tokio::test]
async fn inline_property_match_uses_hash_index() {
    let db = setup_db().await;
    let target = "name-0042";

    let (results, profile) = db
        .session()
        .query_with("MATCH (n:Item {name: $t}) RETURN n.name AS name")
        .param("t", Value::String(target.into()))
        .profile()
        .await
        .unwrap();
    print_profile("inline {name: $t}", &profile);

    assert_eq!(results.len(), 1, "should match exactly 1 row");
    let rows = scan_rows(&profile);
    assert!(
        rows <= 1,
        "GraphScanExec returned {rows} rows; hash index on Item.name not used (have {TOTAL_ROWS} total)"
    );
}

#[tokio::test]
async fn where_clause_property_eq_uses_hash_index() {
    let db = setup_db().await;
    let target = "name-0099";

    let (results, profile) = db
        .session()
        .query_with("MATCH (n:Item) WHERE n.name = $t RETURN n.name AS name")
        .param("t", Value::String(target.into()))
        .profile()
        .await
        .unwrap();
    print_profile("WHERE n.name = $t", &profile);

    assert_eq!(results.len(), 1);
    let rows = scan_rows(&profile);
    assert!(
        rows <= 1,
        "GraphScanExec returned {rows} rows; hash index on Item.name not used"
    );
}

#[tokio::test]
async fn missing_key_uses_hash_index() {
    // Lookup of a non-existent value should still use the index — pre-fix this
    // also full-scans the label.
    let db = setup_db().await;

    let (results, profile) = db
        .session()
        .query_with("MATCH (n:Item {name: $t}) RETURN n.name AS name")
        .param("t", Value::String("does-not-exist".into()))
        .profile()
        .await
        .unwrap();
    print_profile("missing {name: $t}", &profile);

    assert_eq!(results.len(), 0);
    let rows = scan_rows(&profile);
    assert!(
        rows == 0,
        "GraphScanExec returned {rows} rows for missing key; hash index not used"
    );
}

#[tokio::test]
async fn property_eq_literal_uses_hash_index() {
    // String literal (not parameter) form.
    let db = setup_db().await;

    let (results, profile) = db
        .session()
        .query_with("MATCH (n:Item {name: 'name-0123'}) RETURN n.name AS name")
        .profile()
        .await
        .unwrap();
    print_profile("inline literal", &profile);

    assert_eq!(results.len(), 1);
    let rows = scan_rows(&profile);
    assert!(
        rows <= 1,
        "GraphScanExec returned {rows} rows; hash index on Item.name not used (literal)"
    );
}

#[tokio::test]
async fn multi_property_pushes_indexed_conjunct() {
    // `name` is indexed, `color` is not. The indexed conjunct must still drive
    // an index lookup; the non-indexed conjunct stays as a residual filter.
    let db = setup_db().await;

    let (results, profile) = db
        .session()
        .query_with("MATCH (n:Item {name: $t, color: $c}) RETURN n.name AS name")
        .param("t", Value::String("name-0042".into()))
        .param("c", Value::String("red".into())) // 0042 is even -> red, so it matches
        .profile()
        .await
        .unwrap();
    print_profile("multi-prop inline", &profile);

    assert_eq!(results.len(), 1);
    let rows = scan_rows(&profile);
    assert!(
        rows <= 1,
        "GraphScanExec returned {rows} rows; indexed conjunct not pushed into scan"
    );
}

#[tokio::test]
async fn explain_reports_hash_index_usage() {
    let db = setup_db().await;

    let explain = db
        .session()
        .query_with("MATCH (n:Item {name: $t}) RETURN n")
        .param("t", Value::String("name-0042".into()))
        .explain()
        .await
        .unwrap();

    eprintln!("plan_text:\n{}", explain.plan_text);
    eprintln!("index_usage = {:#?}", explain.index_usage);

    let entry = explain
        .index_usage
        .iter()
        .find(|u| u.label_or_type == "Item" && u.property == "name")
        .expect("EXPLAIN should report the Item.name index");
    assert!(
        entry.used,
        "hash index on Item.name should be reported as USED, got {entry:?}"
    );
}
