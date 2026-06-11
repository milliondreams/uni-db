// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression coverage for silent integer-overflow wrapping in the native
//! DataFusion read-projection arithmetic path.
//!
//! Root cause: `uni-query-functions/src/df_expr.rs::translate_binary_op`
//! (around df_expr.rs:1334-1342) lowers Cypher `+`/`-`/`*` on typed `Int64`
//! columns to DataFusion's native operators (`left + right`, `left - right`,
//! `left * right`), which wrap silently on `i64` overflow instead of raising
//! an error. This is a third arithmetic path, distinct from the interpreted
//! `expr_eval.rs` path and the `apply_int_arithmetic` UDF path, both of which
//! already error on overflow. The correct contract — matching the rest of the
//! engine and Neo4j — is that integer overflow raises an error.

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// A read projection that overflows `i64` must error, not silently wrap.
///
/// Builds an in-memory database with a typed `Int64` property so the
/// arithmetic routes through the native DataFusion path (a `CypherValue` /
/// schemaless operand would instead hit the checked UDF path). With
/// `v = i64::MAX`, the query `RETURN n.v + 1` must fail rather than return the
/// wrapped value `i64::MIN`.
///
/// RED today: the native path returns `Ok` with `r == i64::MIN`, so the
/// `is_err()` assertion fails. It passes once df_expr.rs:1334 uses checked
/// arithmetic.
#[tokio::test]
async fn read_int_arithmetic_overflow_errors() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Typed Int64 property `v` so arithmetic routes through the native
    // DataFusion operator path rather than the checked CypherValue UDF path.
    db.schema()
        .label("T")
        .property("v", DataType::Int)
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:T {v: $v})")
        .param("v", Value::Int(i64::MAX))
        .run()
        .await?;
    tx.commit().await?;

    // `i64::MAX + 1` wraps to `i64::MIN` under native arithmetic.
    let res = db.session().query("MATCH (n:T) RETURN n.v + 1 AS r").await;
    assert!(
        res.is_err(),
        "integer overflow in a read-path projection must error, not silently \
         wrap (df_expr.rs:1334 native Int64 Add); got {res:?}",
    );

    // Same contract for multiplication: `i64::MAX * 2` overflows.
    let res_mul = db.session().query("MATCH (n:T) RETURN n.v * 2 AS r").await;
    assert!(
        res_mul.is_err(),
        "integer overflow in a read-path projection must error, not silently \
         wrap (df_expr.rs:1342 native Int64 Mul); got {res_mul:?}",
    );

    Ok(())
}
