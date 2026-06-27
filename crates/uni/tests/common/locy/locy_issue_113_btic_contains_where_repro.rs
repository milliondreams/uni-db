// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression test for <https://github.com/rustic-ai/uni-db/issues/113>
//!
//! A BTIC-containment predicate in a Locy rule `WHERE` evaluated to `Null`
//! instead of `Boolean`, so evaluation failed with
//! `Filter predicate must return BOOLEAN values, got Null`. The BTIC column
//! arrived at the predicate as `Null` because the Locy→DataFusion coercion
//! layer had no `FixedSizeBinary(24)`/Btic arm and collapsed the operand to a
//! Null-typed value.
//!
//! The repro uses `btic_contains_point(BTIC, instant)` (the point-containment
//! function the issue intended; `btic.contains` in the report is its colloquial
//! spelling) and asserts the predicate actually FILTERS — an empty result set
//! would mean the predicate returned `Null` rather than `Boolean`.

// Rust guideline compliant

use std::collections::HashSet;

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

async fn setup_btic_periods() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Period")
        .property("name", DataType::String)
        .property("valid_at", DataType::Btic)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    // `a` covers 1985 (so it contains 1985-06-01); `b` covers 2024 (it does not).
    tx.execute("CREATE (:Period {name: 'a', valid_at: btic('1985')})")
        .await?;
    tx.execute("CREATE (:Period {name: 'b', valid_at: btic('2024')})")
        .await?;
    tx.commit().await?;
    Ok(db)
}

#[tokio::test]
async fn btic_contains_point_predicate_filters_rows() -> Result<()> {
    let db = setup_btic_periods().await?;
    let program = r#"
        CREATE RULE active AS
            MATCH (p:Period)
            WHERE btic_contains_point(p.valid_at, datetime('1985-06-01T00:00:00Z'))
            YIELD KEY p.name AS name
        QUERY active RETURN name
    "#;
    // Before the fix this returns Err("Filter predicate must return BOOLEAN
    // values, got Null") because the BTIC column reads as Null in the plan.
    let result = db.session().locy(program).await?;
    let empty = vec![];
    let rows = result.rows().unwrap_or(&empty);
    let names: HashSet<String> = rows
        .iter()
        .filter_map(|r| match r.get("name") {
            Some(Value::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    // Only `a` covers 1985-06-01. An EMPTY set is the bug signature (predicate
    // collapsed to Null → row filtered out / query errored).
    assert_eq!(
        names,
        HashSet::from(["a".to_string()]),
        "btic_contains_point must filter to {{a}}; got {names:?}"
    );
    Ok(())
}

#[tokio::test]
async fn btic_dot_contains_predicate_filters_rows() -> Result<()> {
    // The faithful issue repro: the dot-namespaced `btic.contains(...)` form
    // (as written in #113) is NOT pushed into the SLG/Cypher condition path and
    // instead evaluates in the DataFusion FILTER, where the `FixedSizeBinary(24)`
    // Btic column was read as Null — yielding "Filter predicate must return
    // BOOLEAN values, got Null". Uses correct (Btic, Btic) arg types so it is
    // GREEN once the column reads correctly.
    let db = setup_btic_periods().await?;
    let program = r#"
        CREATE RULE covering AS
            MATCH (p:Period)
            WHERE btic.contains(p.valid_at, btic('1985-06/1985-07'))
            YIELD KEY p.name AS name
        QUERY covering RETURN name
    "#;
    let result = db.session().locy(program).await?;
    let empty = vec![];
    let rows = result.rows().unwrap_or(&empty);
    let names: HashSet<String> = rows
        .iter()
        .filter_map(|r| match r.get("name") {
            Some(Value::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        names,
        HashSet::from(["a".to_string()]),
        "btic_contains must filter to {{a}}; got {names:?}"
    );
    Ok(())
}
