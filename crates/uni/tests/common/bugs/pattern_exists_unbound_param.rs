// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression: an **unbound parameter** in a pattern-property position inside a
//! pattern predicate silently dropped the predicate instead of erroring.
//!
//! ## Behaviour before the fix
//!
//! `WHERE (n)-[:R]->(:B {name: $missing})` with `$missing` never
//! supplied did not fail. `resolve_predicate_value` returned `None`, the
//! `filter_map` collecting `resolved_preds` dropped the entry, and the property
//! check was simply never applied — so the pattern degenerated to "any
//! neighbour with the right label" and matched. Under `NOT` the error is
//! inverted: rows that should have been kept were filtered out.
//!
//! Both directions are silent wrong answers, and the permissive direction is
//! the dangerous one for a guard clause.
//!
//! ## Why this site was the outlier
//!
//! Everywhere else an unbound parameter is a hard error — `common.rs`
//! (`"Parameter '{}' not found"`) and `unwind.rs` both raise. Plan-time
//! construction here is already fail-closed: `build_property_predicates` bails
//! on an unsupported *value expression*. Only the parameter arm could fail, and
//! only at runtime, which is why it went unnoticed.
//!
//! A parameter bound to an explicit `null` is **not** affected and must keep
//! working: that resolves to `Some(Value::Null)` and compares normally. The
//! distinction between "no value supplied" and "the value is null" is the whole
//! point — pinned by `null_bound_param_is_not_an_error`.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(pattern_exists_unbound)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// `a1` has an edge to `b1`; `a2` has no edges.
async fn setup() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("A")
        .property("n", DataType::String)
        .done()
        .label("B")
        .property("name", DataType::String)
        .done()
        .edge_type("R", &["A"], &["B"])
        .done()
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:A {n: 'a1'})").await?;
    tx.execute("CREATE (:A {n: 'a2'})").await?;
    tx.execute("CREATE (:B {name: 'b1'})").await?;
    tx.execute("MATCH (a:A {n: 'a1'}), (b:B {name: 'b1'}) CREATE (a)-[:R]->(b)")
        .await?;
    tx.commit().await?;
    Ok(db)
}

// ---------------------------------------------------------------------------
// Control: the same query with the parameter actually bound must keep working.
// These pin that the fix targets *unbound* specifically.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pattern_exists_unbound_control_bound_param_filters() -> Result<()> {
    let db = setup().await?;
    let session = db.session();

    // Bound to a value that matches: only a1 has such a neighbour.
    let rows = session
        .query_with("MATCH (a:A) WHERE (a)-[:R]->(:B {name: $p}) RETURN a.n AS n")
        .param("p", Value::String("b1".to_string()))
        .fetch_all()
        .await?;
    let mut names: Vec<String> = rows
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("n").ok())
        .collect();
    names.sort();
    assert_eq!(names, vec!["a1".to_string()]);

    // Bound to a value that matches nothing: the predicate must actually apply.
    let rows = session
        .query_with("MATCH (a:A) WHERE (a)-[:R]->(:B {name: $p}) RETURN a.n AS n")
        .param("p", Value::String("nope".to_string()))
        .fetch_all()
        .await?;
    assert!(
        rows.rows().is_empty(),
        "a bound-but-non-matching param must exclude every row; got {rows:?}"
    );
    Ok(())
}

/// A parameter explicitly bound to `null` is a *supplied* value and must not be
/// confused with an unbound one. It resolves to `Some(Value::Null)`, compares
/// normally against a non-null stored property, and matches nothing.
#[tokio::test]
async fn null_bound_param_is_not_an_error() -> Result<()> {
    let db = setup().await?;
    let rows = db
        .session()
        .query_with("MATCH (a:A) WHERE (a)-[:R]->(:B {name: $p}) RETURN a.n AS n")
        .param("p", Value::Null)
        .fetch_all()
        .await?;
    assert!(
        rows.rows().is_empty(),
        "a null-bound param must compare, not be treated as absent; got {rows:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// The defect: an unbound parameter must error, not silently drop the predicate.
// ---------------------------------------------------------------------------

/// Under a positive pattern predicate, dropping the predicate made the pattern match too much — a
/// guard clause that silently passes everything.
#[tokio::test]
async fn pattern_exists_unbound_param_must_error() -> Result<()> {
    let db = setup().await?;
    let err = db
        .session()
        .query("MATCH (a:A) WHERE (a)-[:R]->(:B {name: $missing}) RETURN a.n AS n")
        .await
        .err()
        .expect("an unbound parameter must not silently drop the property predicate");
    let msg = err.to_string();
    assert!(
        msg.contains("missing"),
        "the error must name the unbound parameter; got: {msg}"
    );
    Ok(())
}

/// Under `NOT` the same drop errs in the opposite direction, filtering
/// out rows that should have been kept. Same cause, so the same error.
#[tokio::test]
async fn pattern_not_exists_unbound_param_must_error() -> Result<()> {
    let db = setup().await?;
    let err = db
        .session()
        .query("MATCH (a:A) WHERE NOT (a)-[:R]->(:B {name: $missing}) RETURN a.n AS n")
        .await
        .err()
        .expect("an unbound parameter must error under NOT too");
    assert!(err.to_string().contains("missing"));
    Ok(())
}
