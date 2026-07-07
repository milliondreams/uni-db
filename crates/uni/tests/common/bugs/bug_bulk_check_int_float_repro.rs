// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Regression test for crates/uni-bulk/src/bulk.rs `evaluate_check_expression`
// (finding [5] / D10, Low) — now FIXED.
//
// Previously, the '='/'!=' operators used Value's type-strict PartialEq, which
// has NO Int/Float cross arm — so Value::Float(5.0) != Value::Int(5). The literal
// `5` in a CHECK expression parses to Int (i64 tried before f64). Meanwhile
// '<'/'>'/'>='/'<=' route through `compare_values`, which DOES coerce Int<->Float.
// Result: a numeric CHECK equality against a Float property spuriously failed,
// while the equivalent bounding form passed.
//
// The fix routes '='/'!=' through `compare_values` when both operands are numbers,
// so Float(5.0) satisfies `= 5`.

use anyhow::Result;
use std::collections::HashMap;
use uni_common::core::schema::{Constraint, ConstraintTarget, ConstraintType};
use uni_db::{DataType, Uni, Value};

async fn setup(check_expr: &str) -> Result<(Uni, tempfile::TempDir)> {
    let temp_dir = tempfile::tempdir()?;
    let db = Uni::open(temp_dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("Metric")
        .property("score", DataType::Float64)
        .done()
        .apply()
        .await?;
    db.schema_manager().add_constraint(Constraint {
        name: "Metric_score_check".to_string(),
        constraint_type: ConstraintType::Check {
            expression: check_expr.to_string(),
        },
        target: ConstraintTarget::Label("Metric".to_string()),
        enabled: true,
    })?;
    Ok((db, temp_dir))
}

/// Regression for FIXED finding uni-bulk[5] / D10: `(n.score = 5)` against a
/// stored Float(5.0) now SATISFIES the CHECK because `=` coerces Int<->Float via
/// `compare_values`.
#[tokio::test]
async fn bulk_check_equality_float_vs_int_literal_passes() -> Result<()> {
    let (db, _temp) = setup("(n.score = 5)").await?;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let mut props: HashMap<String, Value> = HashMap::new();
    props.insert("score".to_string(), Value::Float(5.0));

    let res = bulk.insert_vertices("Metric", vec![props]).await;
    bulk.commit().await?;
    drop(tx);

    // 5.0 equals 5 now that '=' coerces numerically, so the CHECK passes.
    assert!(
        res.is_ok(),
        "expected Float(5.0) to satisfy '= 5' after Int/Float coercion, got: {res:?}"
    );
    Ok(())
}

/// Control: the SAME value satisfies the bounding form `(n.score >= 5)` because
/// '>=' routes through compare_values which coerces Int<->Float. This isolates
/// the defect to the '=' operator, not the data.
#[tokio::test]
async fn bulk_check_ge_float_vs_int_literal_passes_control() -> Result<()> {
    let (db, _temp) = setup("(n.score >= 5)").await?;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let mut props: HashMap<String, Value> = HashMap::new();
    props.insert("score".to_string(), Value::Float(5.0));

    let res = bulk.insert_vertices("Metric", vec![props]).await;
    bulk.commit().await?;
    drop(tx);

    // CONTROL: '>=' coerces, so Float(5.0) >= Int(5) passes as expected.
    assert!(
        res.is_ok(),
        "control failed — '>=' bounding form should coerce and pass: {res:?}"
    );
    Ok(())
}
