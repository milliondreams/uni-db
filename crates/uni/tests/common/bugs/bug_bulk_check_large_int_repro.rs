// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Repro for crates/uni-bulk/src/bulk.rs `compare_values` (D5 mirror) — the
// cross-type Int/Float arm casts `i64 as f64`, losing precision above 2^53.
//
// A bulk CHECK like `(n.big > 9007199254740992.0)` compares a stored Int
// property against a Float literal. For big = 2^53+1 (9007199254740993) the true
// value is strictly greater than the 2^53 bound, so the CHECK must pass. But the
// comparator first does `big as f64`, which rounds 2^53+1 down to 2^53, so the
// comparison sees Equal, `is_gt()` is false, and the valid row is wrongly
// rejected. Pins the bug; flipped to `is_ok()` once the exact `cmp_i64_f64`
// lands. Sibling of `bug_bulk_check_int_float_repro.rs` (D10, equality arm).

use anyhow::Result;
use std::collections::HashMap;
use uni_common::core::schema::{Constraint, ConstraintTarget, ConstraintType};
use uni_db::{DataType, Uni, Value};

async fn setup(check_expr: &str) -> Result<(Uni, tempfile::TempDir)> {
    let temp_dir = tempfile::tempdir()?;
    let db = Uni::open(temp_dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("BigMetric")
        .property("big", DataType::Int64)
        .done()
        .apply()
        .await?;
    db.schema_manager().add_constraint(Constraint {
        name: "BigMetric_big_check".to_string(),
        constraint_type: ConstraintType::Check {
            expression: check_expr.to_string(),
        },
        target: ConstraintTarget::Label("BigMetric".to_string()),
        enabled: true,
    })?;
    Ok((db, temp_dir))
}

/// Regression — FIXED: `big = 2^53+1` truly satisfies `> 2^53.0`; the bulk CHECK
/// now compares exactly (no `i64 as f64` collapse), so the row is accepted.
#[tokio::test]
async fn repro_bulk_check_large_int_gt_float_precision_collapse() -> Result<()> {
    let (db, _temp) = setup("(n.big > 9007199254740992.0)").await?;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let mut props: HashMap<String, Value> = HashMap::new();
    props.insert("big".to_string(), Value::Int(9_007_199_254_740_993));

    let res = bulk.insert_vertices("BigMetric", vec![props]).await;
    let commit = bulk.commit().await;
    drop(tx);

    // 2^53+1 > 2^53.0 is true; exact comparison accepts it.
    assert!(
        res.is_ok() && commit.is_ok(),
        "2^53+1 > 2^53.0 must be accepted after the exact fix; res={res:?} commit={commit:?}"
    );
    Ok(())
}

/// Control: a clearly-smaller int (below 2^53, exactly representable) correctly
/// FAILS the same `> 2^53.0` bound, proving the CHECK itself works and isolating
/// the defect to the precision cast above 2^53.
#[tokio::test]
async fn bulk_check_small_int_gt_float_control() -> Result<()> {
    let (db, _temp) = setup("(n.big > 9007199254740992.0)").await?;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let mut props: HashMap<String, Value> = HashMap::new();
    props.insert("big".to_string(), Value::Int(1_000));

    let res = bulk.insert_vertices("BigMetric", vec![props]).await;
    let _ = bulk.commit().await;
    drop(tx);

    // CONTROL: 1000 is not > 2^53, so the CHECK correctly rejects it.
    assert!(
        res.is_err(),
        "control: a small int below the bound must be rejected; got {res:?}"
    );
    Ok(())
}
