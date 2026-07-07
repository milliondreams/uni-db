// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Repro for crates/uni-bulk/src/bulk.rs:686 (finding [5], Low).
//
// In `evaluate_check_expression`, the '='/'!=' operators use Value's
// type-strict PartialEq, which has NO Int/Float cross arm — so
// Value::Float(5.0) != Value::Int(5). The literal `5` in a CHECK expression
// parses to Int (i64 tried before f64). Meanwhile '<'/'>'/'>='/'<=' route
// through `compare_values`, which DOES coerce Int<->Float. Result: a numeric
// CHECK equality against a Float property spuriously fails, while the
// equivalent bounding form would pass.

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

/// `(n.score = 5)` against a stored Float(5.0) falsely fails the CHECK because
/// Float(5.0) == Int(5) is false under Value's type-strict PartialEq.
// Pins OPEN finding uni-bulk[5] (bulk.rs:782): CHECK `=`/`!=` use type-strict
// PartialEq (no Int/Float arm) so Float(5.0) != Int(5), while the ordering ops
// coerce via compare_values. Tracked in docs/correctness-deferred.md as D10.
// When fixed, remove `#[ignore]` and flip to assert the insert SUCCEEDS.
#[tokio::test]
#[ignore = "pins OPEN finding uni-bulk[5] (bulk.rs:782); tracked as D10 in docs/correctness-deferred.md"]
async fn bulk_check_equality_float_vs_int_literal_false_reject() -> Result<()> {
    let (db, _temp) = setup("(n.score = 5)").await?;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let mut props: HashMap<String, Value> = HashMap::new();
    props.insert("score".to_string(), Value::Float(5.0));

    let res = bulk.insert_vertices("Metric", vec![props]).await;

    // BUG: expected Ok (5.0 equals 5), got Err "CHECK constraint ... violated"
    // because Float(5.0) == Int(5) is false. (repro for bulk.rs:686)
    let err = res.expect_err(
        "OBSERVED-BUG PREMISE FAILED: Float(5.0) satisfied '= 5' CHECK (bug may be fixed)",
    );
    let msg = format!("{err}");
    assert!(
        msg.contains("CHECK constraint") && msg.contains("violated"),
        "repro for bulk.rs:686 — expected spurious CHECK violation, got: {msg}"
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
