// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro: a `WHERE` clause directly following `CALL ... YIELD ...` was silently
//! dropped, returning unfiltered rows.
//!
//! The Cypher grammar nests `WHERE <expr>` inside `yield_clause`
//! (`uni-cypher/src/grammar/cypher.pest`), so `CALL ... YIELD ... WHERE ...`
//! stores the predicate on the `CallClause` AST node (`ast.rs` `where_clause`)
//! rather than as a standalone clause. The planner's `Clause::Call` arm
//! (`uni-query/src/query/planner.rs`) built the `ProcedureCall`/`Apply` plan but
//! never consumed `call_clause.where_clause`, so no `Filter` was emitted and the
//! predicate was ignored — a silent wrong-results bug.
//!
//! The asymmetry that made this easy to miss: a `WHERE` attached to a *following
//! MATCH* (`... YIELD x MATCH (c) WHERE <expr> ...`) is stored on the MATCH node
//! and was always applied correctly; only the standalone post-YIELD `WHERE` was
//! lost. Both supported (`=`) and unsupported (`STARTS WITH`) two-variable
//! predicates were dropped.
//!
//! Fixed by applying `plan_where_clause` on `call_clause.where_clause` at the end
//! of the CALL-planning arm, mirroring the MATCH-WHERE / WITH-WHERE handling.
//! Now a regression guard.
//!
//! Run with:
//!   cargo nextest run -p uni --test integration bug_call_yield_where_dropped

use anyhow::Result;
use uni_db::{DataType, Uni};

/// Build a DB where a cartesian `(a:A) × (b:B)` has exactly one row satisfying
/// `a.name = b.full` / `a.name STARTS WITH b.prefix` (Alice), and one that does
/// not (Zed) — so a dropped WHERE is observable as an extra "Zed" row.
async fn setup() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("A")
        .property("name", DataType::String)
        .done()
        .label("B")
        .property("prefix", DataType::String)
        .property("full", DataType::String)
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:A {name: 'Alice'})").await?;
    tx.execute("CREATE (:A {name: 'Zed'})").await?;
    tx.execute("CREATE (:B {prefix: 'Al', full: 'Alice'})")
        .await?;
    tx.commit().await?;
    Ok(db)
}

fn names_of(result: &uni_db::QueryResult) -> Vec<String> {
    let mut v: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get::<String>("a.name").unwrap())
        .collect();
    v.sort();
    v
}

/// A supported (`=`) two-variable predicate as a post-YIELD `WHERE` must filter.
#[tokio::test]
async fn post_yield_where_supported_eq_filters() -> Result<()> {
    let db = setup().await?;
    let result = db
        .session()
        .query(
            "MATCH (a:A), (b:B) \
             CALL uni.schema.labelInfo('A') YIELD property \
             WHERE a.name = b.full \
             RETURN DISTINCT a.name",
        )
        .await?;
    assert_eq!(names_of(&result), vec!["Alice"]);
    Ok(())
}

/// An unsupported-for-pushdown (`STARTS WITH`) two-variable predicate as a
/// post-YIELD `WHERE` must also filter (evaluated in the Filter, not dropped).
#[tokio::test]
async fn post_yield_where_unsupported_starts_with_filters() -> Result<()> {
    let db = setup().await?;
    let result = db
        .session()
        .query(
            "MATCH (a:A), (b:B) \
             CALL uni.schema.labelInfo('A') YIELD property \
             WHERE a.name STARTS WITH b.prefix \
             RETURN DISTINCT a.name",
        )
        .await?;
    assert_eq!(names_of(&result), vec!["Alice"]);
    Ok(())
}

/// Negation over a post-YIELD `WHERE` must produce the complement (Zed only),
/// proving the predicate is genuinely evaluated rather than dropped or inverted.
#[tokio::test]
async fn post_yield_where_negation_filters() -> Result<()> {
    let db = setup().await?;
    let result = db
        .session()
        .query(
            "MATCH (a:A), (b:B) \
             CALL uni.schema.labelInfo('A') YIELD property \
             WHERE NOT (a.name STARTS WITH b.prefix) \
             RETURN DISTINCT a.name",
        )
        .await?;
    assert_eq!(names_of(&result), vec!["Zed"]);
    Ok(())
}

/// Guard against over-filtering: the same shape with NO post-YIELD WHERE must
/// return every row (Alice and Zed).
#[tokio::test]
async fn no_post_yield_where_returns_all_rows() -> Result<()> {
    let db = setup().await?;
    let result = db
        .session()
        .query(
            "MATCH (a:A), (b:B) \
             CALL uni.schema.labelInfo('A') YIELD property \
             RETURN DISTINCT a.name",
        )
        .await?;
    assert_eq!(names_of(&result), vec!["Alice", "Zed"]);
    Ok(())
}
