// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression: any **`LargeUtf8`-typed** `YIELD` column came back as `NULL`
//! from `derived`, while `QUERY` returned it correctly.
//!
//! Named for the string *literal* case because that is how it was found — while
//! adding `derived` assertions to the TCK during the schemaless
//! property-inference work — but the trigger is the Arrow type, not literals,
//! and the real blast radius is **every string-returning function**. See
//! `string_literal_yield_string_functions_reach_derived` below.
//!
//! ## Measured behaviour before the fix
//!
//! | YIELD expression      | `derived`  | `QUERY` |
//! |-----------------------|------------|---------|
//! | `'patch' AS a`        | **`NULL`** | `patch` |
//! | `toUpper(n.name) AS a`| **`NULL`** | `A`     |
//! | `0.5 AS a`            | `0.5`      | `0.5`   |
//! | `3 AS a`              | `3`        | `3`     |
//! | `true AS a`           | `true`     | `true`  |
//!
//! Declaring a schema makes no difference — this is not the schemaless defect.
//!
//! ## Root cause (fixed)
//!
//! `arrow_to_value` (`uni-store/src/storage/arrow_convert.rs`) had a
//! `StringArray` (Arrow `Utf8`) arm but **no `LargeStringArray` (`LargeUtf8`)
//! arm** — the identifier appeared nowhere in the file. A `LargeUtf8` column
//! fell through every arm to the trailing `Value::Null` fallback.
//!
//! Locy hits that constantly. `infer_expr_type` types string literals *and*
//! `toString`/`toLower`/`toUpper`/`trim`/`replace`/`substring`/`left`/`right`/
//! `reverse`/`type` — plus its catch-all — as `LargeUtf8`. That type lands in
//! `LocyProject`'s `target_types` and in the derived registry schema, and
//! `plan_locy_project` inserts a lossless cast to it. The cast is not the
//! destroyer; it just moves the value into the type the decoder could not read.
//!
//! The other literal types were unaffected because `Int64Array`/`Float64Array`/
//! `BooleanArray` arms exist. A *declared* `DataType::String` property maps to
//! Arrow `Utf8` and hit the `StringArray` arm; a schemaless one is `LargeBinary`
//! and hit the cv-codec arm. Only `LargeUtf8` had no home — hence "schema
//! presence is irrelevant".
//!
//! `QUERY` was correct throughout because it never touches Arrow: the SLG
//! resolver evaluates the YIELD expression natively into a `Value`.
//!
//! A `FOLD` rule is a separate story, measured rather than assumed: a plain
//! value column alongside `FOLD` is **dropped by `FoldExec` entirely**, not
//! nulled, so it never reached this decoder at all. Pinned below by
//! `string_literal_yield_under_fold_drops_the_column_entirely` to keep
//! "absent column" and "column holding NULL" from being confused for each other.
//!
//! Two hand-rolled copies of the decoder (`locy_fixpoint.rs`'s
//! `extract_common_value` and `extract_feature_value`) already carried a
//! `LargeStringArray` arm: the same gap was patched privately twice instead of
//! in the shared decoder.
//!
//! This is the defect behind the "string literals in non-KEY YIELD columns
//! return None" limitation recorded in the Locy runtime notes. It was never a
//! designed restriction, and the numeric-encoding workaround it recommends is
//! no longer needed.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration \
//!     -E 'test(string_literal_yield)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

async fn setup(declare_schema: bool) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    if declare_schema {
        db.schema()
            .label("N")
            .property("name", DataType::String)
            .done()
            .apply()
            .await?;
    }
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:N {name: 'A'})").await?;
    tx.commit().await?;
    Ok(db)
}

/// Returns `(derived_value, query_value)` for the single yielded column `a`.
async fn both_surfaces(db: &Uni, yield_expr: &str) -> Result<(Option<Value>, Option<Value>)> {
    let program =
        format!("CREATE RULE t AS MATCH (n:N) YIELD KEY n, {yield_expr} AS a\nQUERY t RETURN a");
    let result = db.session().locy(&program).await?;
    let empty = vec![];
    let derived = result
        .derived_facts("t")
        .unwrap_or(&empty)
        .first()
        .and_then(|r| r.get("a").cloned());
    let queried = result
        .rows()
        .unwrap_or(&empty)
        .first()
        .and_then(|r| r.get("a").cloned());
    Ok((derived, queried))
}

// ---------------------------------------------------------------------------
// Green: non-string literals are fine on both surfaces. These pin the trigger
// to the string case specifically.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn string_literal_yield_control_numeric_and_bool_literals_agree() -> Result<()> {
    let db = setup(false).await?;
    for (expr, expected) in [
        ("0.5", Value::Float(0.5)),
        ("3", Value::Int(3)),
        ("true", Value::Bool(true)),
    ] {
        let (derived, queried) = both_surfaces(&db, expr).await?;
        assert_eq!(
            derived,
            Some(expected.clone()),
            "{expr}: derived must carry the literal"
        );
        assert_eq!(derived, queried, "{expr}: surfaces must agree");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The defect itself.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn string_literal_yield_must_reach_derived() -> Result<()> {
    let db = setup(false).await?;
    let (derived, _) = both_surfaces(&db, "'patch'").await?;
    assert_eq!(
        derived,
        Some(Value::String("patch".to_string())),
        "a string literal must materialize in the derived relation"
    );
    Ok(())
}

#[tokio::test]
async fn string_literal_yield_surfaces_must_agree() -> Result<()> {
    let db = setup(false).await?;
    let (derived, queried) = both_surfaces(&db, "'patch'").await?;
    assert_eq!(
        derived, queried,
        "the same rule in the same evaluation must not disagree across surfaces"
    );
    Ok(())
}

/// Declaring a schema does not help — this is not the schemaless defect.
#[tokio::test]
async fn string_literal_yield_is_not_schema_dependent() -> Result<()> {
    let db = setup(true).await?;
    let (derived, _) = both_surfaces(&db, "'patch'").await?;
    assert_eq!(derived, Some(Value::String("patch".to_string())));
    Ok(())
}

/// **The actual blast radius.** `infer_expr_type` types every string-returning
/// Cypher function as `LargeUtf8`, so all of them shared the literal's fate:
/// NULL in `derived`, correct in `QUERY`.
///
/// Testing only the literal would re-create the narrow framing that let this
/// survive for so long as a documented "string literal" constraint — the
/// limitation was never about literals.
#[tokio::test]
async fn string_literal_yield_string_functions_reach_derived() -> Result<()> {
    let db = setup(true).await?;
    for (expr, expected) in [
        ("toUpper(n.name)", "A"),
        ("toLower('PATCH')", "patch"),
        ("toString(42)", "42"),
        ("substring('patch', 0, 3)", "pat"),
        ("trim('  pad  ')", "pad"),
        ("replace('patch', 'p', 'b')", "batch"),
    ] {
        let (derived, queried) = both_surfaces(&db, expr).await?;
        assert_eq!(
            derived,
            Some(Value::String(expected.to_string())),
            "{expr}: a string-returning function must materialize in `derived`"
        );
        assert_eq!(derived, queried, "{expr}: surfaces must agree");
    }
    Ok(())
}

/// Boundary: a plain value column alongside `FOLD` is **dropped entirely**, not
/// returned as NULL.
///
/// Measured, not assumed. `FoldExec` emits only the KEY and fold columns, so
/// `g` never reaches the derived relation at all — the row is
/// `{"p": Float(0.5), "a": Node(..)}` with no `g` key.
///
/// Pinned here to keep the two behaviours distinct. The `LargeUtf8` bug this
/// file is about produced a *present column holding NULL*; this produces an
/// *absent column*. If a future change turned this into `g = NULL`, it would
/// look like a regression of the decode bug while actually being a different
/// fault, and vice versa.
///
/// Whether silently dropping a yielded column is itself right is a separate
/// question, deliberately not litigated here.
#[tokio::test]
async fn string_literal_yield_under_fold_drops_the_column_entirely() -> Result<()> {
    let db = setup(true).await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:N {name: 'B'})").await?;
    tx.execute("MATCH (a:N {name: 'A'}), (b:N {name: 'B'}) CREATE (a)-[:R {prob: 0.5}]->(b)")
        .await?;
    tx.commit().await?;

    let program = "CREATE RULE t AS\n\
           MATCH (a:N)-[e:R]->(b:N)\n\
           FOLD p = MNOR(e.prob)\n\
           YIELD KEY a, 'grp' AS g, p";
    let result = db.session().locy(program).await?;
    let empty = vec![];
    let row = result
        .derived_facts("t")
        .unwrap_or(&empty)
        .first()
        .expect("precondition: the FOLD rule must derive a fact")
        .clone();

    assert!(
        !row.contains_key("g"),
        "a non-KEY, non-fold column is dropped by FoldExec, not nulled; got {row:?}"
    );
    assert_eq!(
        row.get("p"),
        Some(&Value::Float(0.5)),
        "the fold column itself must survive"
    );
    Ok(())
}
