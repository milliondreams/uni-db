// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for the CHECK-evaluator divergence between the bulk and transactional
//! write paths (Tier 1.4).
//!
//! `uni-bulk`'s `BulkWriter` and `uni-store`'s `Writer` each carried their own
//! copy of the same ~100-line CHECK evaluator, token for token identical except
//! for the equality operators. The bulk copy routed numeric `=` / `!=` operands
//! through `compare_values` — with a comment explaining that `Value`'s
//! `PartialEq` is type-strict and has no Int/Float arm — while the writer copy
//! used a bare `==`.
//!
//! So `CHECK (score = 5)` against a stored `Float(5.0)` **passed** via
//! `BulkWriter::insert_vertices` and **failed** via `tx.execute("CREATE ...")`:
//! the same row and the same constraint, two verdicts, decided by which API the
//! caller happened to use.
//!
//! This was a fix applied to one copy and never propagated, not an intentional
//! split — `bug_bulk_check_int_float_repro.rs` is the landed regression test
//! that pins the bulk side.
//!
//! The assertion below is deliberately *differential*: it runs both paths and
//! requires them to agree. Each side is also asserted individually first, so a
//! failure names which one moved rather than only reporting a mismatch.

use anyhow::Result;
use std::collections::HashMap;
use uni_common::core::schema::{Constraint, ConstraintTarget, ConstraintType};
use uni_db::{DataType, Uni, Value};

async fn db_with_check(check_expr: &str) -> Result<(Uni, tempfile::TempDir)> {
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

/// Does `Float(5.0)` satisfy `= 5` through the bulk loader?
async fn bulk_accepts(check_expr: &str) -> Result<bool> {
    let (db, _temp) = db_with_check(check_expr).await?;
    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let mut props: HashMap<String, Value> = HashMap::new();
    props.insert("score".to_string(), Value::Float(5.0));
    let res = bulk.insert_vertices("Metric", vec![props]).await;
    bulk.commit().await?;
    drop(tx);
    Ok(res.is_ok())
}

/// The same question through the transactional path.
async fn tx_accepts(check_expr: &str) -> Result<bool> {
    let (db, _temp) = db_with_check(check_expr).await?;
    let tx = db.session().tx().await?;
    let res = tx.execute("CREATE (:Metric {score: 5.0})").await;
    let ok = res.is_ok();
    if ok {
        tx.commit().await?;
    }
    Ok(ok)
}

/// The two write paths must reach the same verdict on the same row.
#[tokio::test]
async fn bulk_and_tx_agree_on_numeric_check_equality() -> Result<()> {
    let expr = "(n.score = 5)";

    let bulk = bulk_accepts(expr).await?;
    let tx = tx_accepts(expr).await?;

    // Individually first, so a failure names the side that moved.
    assert!(
        bulk,
        "bulk path should accept Float(5.0) against '= 5' (coerces numerically)"
    );
    assert!(
        tx,
        "transactional path rejected Float(5.0) against '= 5'; before the shared \
         evaluator it used a type-strict `==` with no Int/Float arm"
    );

    assert_eq!(
        bulk, tx,
        "the same row and constraint must not depend on which write API is used"
    );
    Ok(())
}

/// The negative direction must agree too: `!=` is the same operator family, and
/// a fix that only touched `=` would leave this split.
#[tokio::test]
async fn bulk_and_tx_agree_on_numeric_check_inequality() -> Result<()> {
    let expr = "(n.score != 5)";

    let bulk = bulk_accepts(expr).await?;
    let tx = tx_accepts(expr).await?;

    assert!(!bulk, "5.0 != 5 is false, so the CHECK must reject");
    assert!(!tx, "5.0 != 5 is false, so the CHECK must reject");
    assert_eq!(bulk, tx);
    Ok(())
}

/// Control: the bounding form already agreed, because both copies routed
/// ordering operators through `compare_values`. This isolates the divergence to
/// the equality operators rather than to the data or the harness.
#[tokio::test]
async fn bulk_and_tx_already_agreed_on_ordering() -> Result<()> {
    let expr = "(n.score >= 5)";

    let bulk = bulk_accepts(expr).await?;
    let tx = tx_accepts(expr).await?;

    assert!(bulk);
    assert!(tx);
    assert_eq!(bulk, tx);
    Ok(())
}
