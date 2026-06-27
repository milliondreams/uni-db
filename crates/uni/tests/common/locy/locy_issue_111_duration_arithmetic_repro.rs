// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression test for <https://github.com/rustic-ai/uni-db/issues/111>
//!
//! A Locy rule doing arithmetic over `duration.inDays(...)` registered but failed
//! at evaluation with `DataFusion planning failed: Unsupported CAST from
//! LargeBinary to Float64`. Because uni-db plans all registered rules together,
//! one such rule POISONED every other rule's `QUERY`.
//!
//! Resolution (openCypher-conformant, issue #111 decision C): `duration.inDays`
//! keeps returning a truncated `Duration` (Neo4j returns a Duration, not a
//! number). The crash is removed by routing unary math through value-aware
//! coercion, and the numeric value is obtained via the `.days` component
//! accessor — `duration.inDays(a, b).days` — which is usable in arithmetic.
//!
//! Two guarantees are asserted:
//! 1. the previously-crashing direct form (`... * exp(... duration.inDays ...)`)
//!    plans and evaluates WITHOUT error and does NOT poison a co-registered rule;
//! 2. the `.days` accessor form yields the correct numeric and filters rows.

// Rust guideline compliant

use std::collections::HashSet;

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

use crate::locy::value_assert::assert_column_eq;

async fn setup() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Span")
        .property("name", DataType::String)
        .property("imp", DataType::Float64)
        .property("t0", DataType::DateTime)
        .property("t1", DataType::DateTime)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    // 2020-01-01 .. 2020-01-11 = exactly 10 days apart.
    tx.execute(
        "CREATE (:Span {name: 'a', imp: 0.5, \
         t0: datetime('2020-01-01T00:00:00Z'), \
         t1: datetime('2020-01-11T00:00:00Z')})",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

#[tokio::test]
async fn duration_arithmetic_does_not_crash_or_poison() -> Result<()> {
    // The exact failure mode from the issue: arithmetic over a duration inside a
    // WHERE, registered alongside an unrelated rule. Before the fix the whole
    // QUERY failed to PLAN ("Unsupported CAST from LargeBinary to Float64"),
    // poisoning the unrelated `healthy` rule too.
    let db = setup().await?;
    // Direct arithmetic over the duration (no `.days`) is the exact issue repro:
    // it previously failed to PLAN with "Unsupported CAST from LargeBinary to
    // Float64". Under decision C this no longer crashes — `exp` of a non-numeric
    // duration cleanly yields NULL — so `decay` simply matches nothing while the
    // co-registered `healthy` rule is unaffected.
    let program = r#"
        CREATE RULE decay AS
            MATCH (s:Span)
            WHERE s.imp * exp(-0.1 * duration.inDays(s.t0, s.t1)) <= 1.0
            YIELD KEY s.name AS name
        CREATE RULE healthy AS
            MATCH (s:Span) YIELD KEY s.name AS name
        QUERY healthy RETURN name
    "#;
    let result = db.session().locy(program).await;
    assert!(
        result.is_ok(),
        "issue #111: duration arithmetic must not crash the planner; got {:?}",
        result.err()
    );
    let result = result?;
    // The unrelated rule must still return its row (no cross-rule poisoning).
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
        "co-registered rule was poisoned by the duration rule; got {names:?}"
    );
    Ok(())
}

#[tokio::test]
async fn duration_indays_days_accessor_is_numeric() -> Result<()> {
    // The `.days` accessor must yield an integer usable in arithmetic.
    // inDays(2020-01-01, 2020-01-11) = P10D → .days = 10 → * 2 = 20 (> 15).
    let db = setup().await?;
    let program = r#"
        CREATE RULE long_spans AS
            MATCH (s:Span)
            WHERE duration.inDays(s.t0, s.t1).days * 2 > 15
            YIELD KEY s.name AS name, duration.inDays(s.t0, s.t1).days * 2 AS double_days
        QUERY long_spans RETURN name, double_days
    "#;
    let result = db.session().locy(program).await?;
    let empty = vec![];
    let rows = result.rows().unwrap_or(&empty);
    assert_column_eq(rows, "name", &Value::String("a".to_string()));
    assert_column_eq(rows, "double_days", &Value::Int(20));
    Ok(())
}
