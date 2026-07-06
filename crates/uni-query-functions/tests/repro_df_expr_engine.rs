// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Runnable repros for verified findings in the translation / type-coercion /
//! aggregate layers. Finding [1] inspects the real `apply_type_coercion`
//! output; findings [6] and [8] drive the translated expression / aggregate
//! through a real DataFusion `SessionContext` and observe the wrong value.

use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, LargeBinaryArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchema;
use datafusion::logical_expr::expr::Case;
use datafusion::logical_expr::{Expr as DfExpr, col, lit};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

use uni_common::Value;
use uni_common::cypher_value_codec::encode_int;
use uni_cypher::ast::{BinaryOp, CypherLiteral, Expr as CyExpr};

use uni_query_functions::df_expr::{apply_type_coercion, cypher_expr_to_df};
use uni_query_functions::df_udfs::create_cypher_sum_udaf;

/// Finding [1] df_expr.rs:3368 — `coerce_case_expr` wraps the WHEN of a
/// simple/operand CASE in `_cv_to_bool` (whose type is Null); the subsequent
/// `rewrite_simple_case_to_generic` then compares the operand against a
/// Null-typed WHEN, hitting NullInvolved and emitting a literal-NULL condition,
/// so the branch is unreachable and the CASE always yields the ELSE value.
#[test]
fn repro_finding_01_simple_case_when_becomes_literal_null() {
    // Both operand and WHEN are LargeBinary (schemaless CypherValue) columns.
    let arrow_schema = Schema::new(vec![
        Field::new("op", DataType::LargeBinary, true),
        Field::new("w", DataType::LargeBinary, true),
    ]);
    let df_schema = DFSchema::try_from(arrow_schema).unwrap();

    // CASE op WHEN w THEN 1 ELSE 0 END  (simple/operand form: expr = Some(op)).
    let case = DfExpr::Case(Case {
        expr: Some(Box::new(col("op"))),
        when_then_expr: vec![(Box::new(col("w")), Box::new(lit(1_i64)))],
        else_expr: Some(Box::new(lit(0_i64))),
    });

    let coerced = apply_type_coercion(&case, &df_schema).unwrap();

    let DfExpr::Case(generic) = coerced else {
        panic!("expected a CASE, got {coerced:?}");
    };
    // The simple CASE was rewritten to a generic CASE (operand folded away).
    assert!(generic.expr.is_none(), "operand should be folded into WHEN");

    let when_condition = &*generic.when_then_expr[0].0;
    // BUG: the WHEN condition is a literal NULL, so `THEN 1` is unreachable and
    // the CASE always returns the ELSE (0), even when op == w
    // (repro for df_expr.rs:3368). A correct coercion keeps a real comparison.
    assert!(
        matches!(
            when_condition,
            DfExpr::Literal(ScalarValue::Boolean(None), _)
        ),
        "WHEN condition should be a real comparison but is a literal NULL: {when_condition:?}"
    );
}

/// Finding [6] df_expr.rs:1374 — the regex operator `=~` is translated as
/// `regexp_match(left, right).is_not_null()`, collapsing NULL inputs to `false`
/// instead of propagating NULL per Cypher three-valued semantics.
#[tokio::test]
async fn repro_finding_06_regex_null_collapses_to_false() {
    // Cypher `x =~ 'foo'` where x is a column reference.
    let cy = CyExpr::BinaryOp {
        left: Box::new(CyExpr::Variable("x".to_string())),
        op: BinaryOp::Regex,
        right: Box::new(CyExpr::Literal(CypherLiteral::String("foo".to_string()))),
    };
    let df_expr = cypher_expr_to_df(&cy, None).unwrap();

    // A single row where x IS NULL.
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)]));
    let x = StringArray::from(vec![None as Option<&str>]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(x)]).unwrap();

    let ctx = SessionContext::new();
    let out = ctx
        .read_batch(batch)
        .unwrap()
        .select(vec![df_expr.alias("r")])
        .unwrap()
        .collect()
        .await
        .unwrap();

    let column = out[0].column(0);
    let result = column.as_any().downcast_ref::<BooleanArray>().unwrap();

    // Correct Cypher 3VL: `null =~ 'foo'` must be NULL.
    // BUG: expected NULL, got false (repro for df_expr.rs:1374).
    assert!(
        !result.is_null(0),
        "null =~ pattern should yield NULL but yields a concrete boolean"
    );
    assert!(
        !result.value(0),
        "null =~ 'foo' wrongly evaluates to false instead of NULL"
    );
}

/// Finding [8] df_udfs.rs:6959 — `CypherSumAccumulator` accumulates integers
/// with `wrapping_add`, so SUM over integers silently wraps on i64 overflow and
/// returns a garbage integer instead of the overflow error this codebase raises
/// everywhere else for integer arithmetic.
#[tokio::test]
async fn repro_finding_08_sum_integer_wraps_on_overflow() {
    let ctx = SessionContext::new();
    ctx.register_udaf(create_cypher_sum_udaf());

    // Two integers whose sum overflows i64: i64::MAX + 1.
    let schema = Arc::new(Schema::new(vec![Field::new(
        "v",
        DataType::LargeBinary,
        false,
    )]));
    let values = LargeBinaryArray::from_iter_values([encode_int(i64::MAX), encode_int(1)]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap();
    ctx.register_batch("t", batch).unwrap();

    let out = ctx
        .sql("SELECT _cypher_sum(v) AS s FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let column = out[0].column(0);
    let lb = column.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    let decoded = uni_common::cypher_value_codec::decode(lb.value(0)).unwrap();

    // FIXED (df_udfs.rs): on i64 overflow the accumulator drops the exact-int
    // path (checked_add -> all_ints=false) and returns the f64 sum, rather than
    // silently WRAPPING to i64::MIN. The result is a large positive Float, never
    // the wrapped negative integer.
    match decoded {
        Value::Float(f) => assert!(
            f > 9.0e18,
            "overflowing SUM must be a large positive float (~i64::MAX+1), got {f}"
        ),
        other => panic!("expected a Float on overflow, got {other:?} (must not wrap to i64::MIN)"),
    }
}
