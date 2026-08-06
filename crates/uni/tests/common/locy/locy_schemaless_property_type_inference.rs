// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro: in **schemaless** mode a property expression in `YIELD` is
//! type-inferred as `Float64`, so `derived` reports the wrong value.
//!
//! Not one of #158/#159/#160 — found by the generic QUERY/derived parity guard
//! the first time it compared key *values* rather than merely checking the
//! result was non-empty. The existing TCK scenario over this shape
//! (`evaluate/YieldValueColumns.feature`, "KEY property column without FOLD
//! projects its value in QUERY") asserts only on the QUERY surface, so it could
//! not see that `derived` disagreed — the same blind spot that let
//! `IsNotFoldQueryMatrix` 2d-3 stay vacuously green.
//!
//! ## Measured behaviour
//!
//! | property | schema declared | `derived`  | `QUERY` |
//! |----------|-----------------|------------|---------|
//! | string   | yes             | `'a'`      | `'a'`   |
//! | string   | **no**          | **`NULL`** | `'a'`   |
//! | int      | yes             | `7`        | `7`     |
//! | int      | **no**          | **`7.0`**  | `7`     |
//!
//! KEY-ness is irrelevant — a non-KEY property column behaves identically. With
//! a `FOLD` present *both* surfaces return `NULL`.
//!
//! ## Suspected root cause (unverified)
//!
//! With no schema to consult, `infer_expr_type` falls back to `Float64` for a
//! property expression. A string coerced to `Float64` becomes `NULL`; an
//! integer becomes a float. `reconcile_schema`
//! (`uni-query/src/query/df_graph/locy_fixpoint.rs`) carries a comment naming
//! exactly this failure — *"`infer_expr_type` may guess wrong (e.g. `Property →
//! Float64` for a string column)"* — and repairs the schema from the first
//! non-empty batch, which suggests the repair does not reach this path.
//!
//! Treat that as a hypothesis: it has not been confirmed by instrumenting the
//! code, only inferred from the value/type pattern above.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration \
//!     -E 'test(schemaless_property)' --run-ignored all

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// Builds a graph with one `Tag {name: 'a', n: 7}`, with or without a declared
/// schema.
async fn setup(declare_schema: bool) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    if declare_schema {
        db.schema()
            .label("Tag")
            .property("name", DataType::String)
            .property("n", DataType::Int64)
            .done()
            .apply()
            .await?;
    }
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Tag {name: 'a', n: 7})").await?;
    tx.commit().await?;
    Ok(db)
}

/// Returns the single `derived` value of `column` for `rule`.
async fn derived_value(db: &Uni, program: &str, rule: &str, column: &str) -> Result<Option<Value>> {
    let result = db.session().locy(program).await?;
    let empty = vec![];
    Ok(result
        .derived_facts(rule)
        .unwrap_or(&empty)
        .first()
        .and_then(|row| row.get(column).cloned()))
}

const STRING_RULE: &str = "CREATE RULE tags AS MATCH (t:Tag) YIELD KEY t.name AS tag";
const INT_RULE: &str = "CREATE RULE nums AS MATCH (t:Tag) YIELD KEY t.n AS num";

// ---------------------------------------------------------------------------
// Green: with a declared schema the values are correct. These pin the boundary
// — the defect is specifically the schemaless path, not property projection.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn schemaless_property_control_string_with_schema_is_correct() -> Result<()> {
    let db = setup(true).await?;
    assert_eq!(
        derived_value(&db, STRING_RULE, "tags", "tag").await?,
        Some(Value::String("a".to_string()))
    );
    Ok(())
}

#[tokio::test]
async fn schemaless_property_control_int_with_schema_is_correct() -> Result<()> {
    let db = setup(true).await?;
    assert_eq!(
        derived_value(&db, INT_RULE, "nums", "num").await?,
        Some(Value::Int(7))
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Red: the open defect.
// ---------------------------------------------------------------------------

/// A string property projected in `YIELD` comes back as `NULL` from `derived`
/// when no schema is declared. `QUERY` returns `'a'` for the same rule in the
/// same evaluation.
#[tokio::test]
#[ignore = "open defect: schemaless YIELD property inferred as Float64 — string becomes NULL"]
async fn schemaless_string_property_must_not_become_null() -> Result<()> {
    let db = setup(false).await?;
    assert_eq!(
        derived_value(&db, STRING_RULE, "tags", "tag").await?,
        Some(Value::String("a".to_string())),
        "schemaless string property must survive projection into `derived`"
    );
    Ok(())
}

/// An integer property comes back as a float. Quieter than the string case but
/// the same cause, and it changes the type users see.
#[tokio::test]
#[ignore = "open defect: schemaless YIELD property inferred as Float64 — int becomes float"]
async fn schemaless_int_property_must_keep_its_type() -> Result<()> {
    let db = setup(false).await?;
    assert_eq!(
        derived_value(&db, INT_RULE, "nums", "num").await?,
        Some(Value::Int(7)),
        "schemaless integer property must not be widened to Float"
    );
    Ok(())
}

/// The two surfaces must agree, whichever value is correct. This is the
/// assertion the existing TCK scenario was missing.
#[tokio::test]
#[ignore = "open defect: schemaless YIELD property — derived and QUERY disagree"]
async fn schemaless_property_derived_and_query_must_agree() -> Result<()> {
    let db = setup(false).await?;
    let program = format!("{STRING_RULE}\nQUERY tags RETURN tag");
    let result = db.session().locy(&program).await?;
    let empty = vec![];
    let derived = result
        .derived_facts("tags")
        .unwrap_or(&empty)
        .first()
        .and_then(|r| r.get("tag").cloned());
    let queried = result
        .rows()
        .unwrap_or(&empty)
        .first()
        .and_then(|r| r.get("tag").cloned());
    assert_eq!(
        derived, queried,
        "the same rule in the same evaluation must not disagree across surfaces"
    );
    Ok(())
}
