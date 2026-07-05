// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repros for the broader defect class surfaced by the #145 second review.
//! Two independent, confirmed bug families — neither depends on the #145 YIELD
//! rename (they reproduce with same-name columns):
//!
//! Family C — compound-expression substitution drop:
//!   `substitute_along_vars` / `substitute_fold_aliases`
//!   (`crates/uni-query/src/query/locy_planner.rs`) recurse only into
//!   BinaryOp/UnaryOp/FunctionCall; their `other => other` arm drops CASE / IN /
//!   IS [NOT] NULL / List / Map. So an ALONG binding or FOLD output nested in any
//!   of those is never rewritten → planning fails with `No field named <var>`.
//!   Contrast: the same reference in an *arithmetic* expr (BinaryOp) works.
//!
//! Family B — QUERY-level WHERE silently bypassed for FOLD rules:
//!   `crates/uni-query/src/query/df_graph/locy_query.rs:92-101` filters on the
//!   SLG path, but FOLD/fixpoint rules take the post-fixpoint chain and never
//!   reach it, so `QUERY r WHERE <pred>` returns rows that fail the predicate.
//!   Regular (non-fold) rules filter correctly.

use anyhow::Result;
use uni_db::{DataType, Uni};

/// Graph: a-[R,w=1]->b, a-[R,w=2]->c. Node a has COUNT=2, SUM(weight)=3.0.
async fn graph() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("name", DataType::String)
        .property("w", DataType::Float64)
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute(
        "CREATE (a:N {name:'a', w:1.0}), (b:N {name:'b', w:2.0}), (c:N {name:'c', w:3.0}), \
         (a)-[:R {weight: 1.0}]->(b), (a)-[:R {weight: 2.0}]->(c)",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

/// Return `Err` message (or None on success) for a Locy program — used to assert
/// the planner does NOT drop a nested variable (the substitute-drop symptom is a
/// hard `No field named <var>` schema error at planning time).
async fn run_err(db: &Uni, program: &str) -> Option<String> {
    db.session()
        .locy_with(program)
        .run()
        .await
        .err()
        .map(|e| e.to_string())
}

async fn row_count(db: &Uni, program: &str) -> usize {
    let r = db
        .session()
        .locy_with(program)
        .run()
        .await
        .expect("locy run");
    r.rows().map(|v| v.len()).unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Family C — compound-expression substitution drop
// ---------------------------------------------------------------------------

/// C-control: FOLD output nested in ARITHMETIC (BinaryOp) in HAVING works —
/// substitute_fold_aliases recurses into BinaryOp. This is the contrast case.
#[tokio::test]
async fn fold_alias_in_having_arithmetic_ok() -> Result<()> {
    let db = graph().await?;
    let n = row_count(
        &db,
        "CREATE RULE r AS MATCH (a:N)-[e:R]->(b:N) \
         FOLD n = COUNT(*) \
         WHERE n * 1 > 1 \
         YIELD KEY a.name AS name, n AS support \
         QUERY r RETURN name, support",
    )
    .await;
    assert_eq!(n, 1, "arithmetic HAVING on fold output should keep node a");
    Ok(())
}

/// C1: ALONG binding nested inside CASE in a FOLD input. substitute_along_vars
/// drops the CASE subtree → `No field named ew`.
#[tokio::test]
async fn along_var_nested_in_case_dropped() -> Result<()> {
    let db = graph().await?;
    let err = run_err(
        &db,
        "CREATE RULE r AS MATCH (a:N)-[e:R]->(b:N) \
         ALONG ew = e.weight \
         FOLD s = SUM(CASE WHEN ew > 0.0 THEN ew ELSE 0.0 END) \
         YIELD KEY a.name AS name, s \
         QUERY r RETURN name, s",
    )
    .await;
    assert!(
        err.as_deref()
            .map(|e| !e.contains("No field named ew"))
            .unwrap_or(true),
        "ALONG var nested in CASE was dropped by substitute_along_vars: {err:?}"
    );
    Ok(())
}

/// C2: FOLD output nested inside CASE in HAVING. substitute_fold_aliases drops
/// the CASE → the fold var is not rewritten to its alias → `No field named n`.
#[tokio::test]
async fn fold_alias_nested_in_case_dropped() -> Result<()> {
    let db = graph().await?;
    let err = run_err(
        &db,
        "CREATE RULE r AS MATCH (a:N)-[e:R]->(b:N) \
         FOLD n = COUNT(*) \
         WHERE (CASE WHEN n > 1 THEN 1 ELSE 0 END) = 1 \
         YIELD KEY a.name AS name, n AS support \
         QUERY r RETURN name, support",
    )
    .await;
    assert!(
        err.as_deref()
            .map(|e| !e.contains("No field named n"))
            .unwrap_or(true),
        "FOLD alias nested in CASE was dropped by substitute_fold_aliases: {err:?}"
    );
    Ok(())
}

/// C3: FOLD output nested inside IN. Same drop → `No field named n`.
#[tokio::test]
async fn fold_alias_nested_in_in_dropped() -> Result<()> {
    let db = graph().await?;
    let err = run_err(
        &db,
        "CREATE RULE r AS MATCH (a:N)-[e:R]->(b:N) \
         FOLD n = COUNT(*) \
         WHERE n IN [2, 3] \
         YIELD KEY a.name AS name, n AS support \
         QUERY r RETURN name, support",
    )
    .await;
    assert!(
        err.as_deref()
            .map(|e| !e.contains("No field named n"))
            .unwrap_or(true),
        "FOLD alias nested in IN was dropped: {err:?}"
    );
    Ok(())
}

/// C4: FOLD output nested inside IS NOT NULL. Same drop → `No field named n`.
#[tokio::test]
async fn fold_alias_nested_in_isnotnull_dropped() -> Result<()> {
    let db = graph().await?;
    let err = run_err(
        &db,
        "CREATE RULE r AS MATCH (a:N)-[e:R]->(b:N) \
         FOLD n = COUNT(*) \
         WHERE n IS NOT NULL \
         YIELD KEY a.name AS name, n AS support \
         QUERY r RETURN name, support",
    )
    .await;
    assert!(
        err.as_deref()
            .map(|e| !e.contains("No field named n"))
            .unwrap_or(true),
        "FOLD alias nested in IS NOT NULL was dropped: {err:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Family B — QUERY-level WHERE silently bypassed for FOLD rules
// ---------------------------------------------------------------------------

/// B-control: QUERY WHERE on a NON-fold rule filters correctly.
#[tokio::test]
async fn query_where_nonfold_filters() -> Result<()> {
    let db = graph().await?;
    let dropped = row_count(
        &db,
        "CREATE RULE r AS MATCH (a:N) \
         YIELD KEY a.name AS name, a.w AS wv \
         QUERY r WHERE wv > 100.0 RETURN name, wv",
    )
    .await;
    assert_eq!(
        dropped, 0,
        "non-fold QUERY WHERE (false predicate) must drop all rows"
    );

    let kept = row_count(
        &db,
        "CREATE RULE r AS MATCH (a:N) \
         YIELD KEY a.name AS name, a.w AS wv \
         QUERY r WHERE wv > 1.5 RETURN name, wv",
    )
    .await;
    assert_eq!(
        kept, 2,
        "non-fold QUERY WHERE (partial predicate) must keep b and c"
    );
    Ok(())
}

/// B1: QUERY WHERE on a FOLD-aggregate column is silently ignored — the row
/// fails the predicate (total=3.0, `> 100.0`) yet is returned.
#[tokio::test]
async fn query_where_on_fold_aggregate_ignored() -> Result<()> {
    let db = graph().await?;
    let n = row_count(
        &db,
        "CREATE RULE r AS MATCH (a:N)-[e:R]->(b:N) \
         FOLD total = SUM(e.weight) \
         YIELD KEY a.name AS name, total \
         QUERY r WHERE total > 100.0 RETURN name, total",
    )
    .await;
    assert_eq!(
        n, 0,
        "QUERY WHERE on FOLD aggregate column was silently bypassed"
    );
    Ok(())
}

/// B2: QUERY WHERE on a KEY column of a FOLD rule is also ignored — proving the
/// bypass is whole-rule, not aggregate-column-specific.
#[tokio::test]
async fn query_where_on_fold_rule_key_ignored() -> Result<()> {
    let db = graph().await?;
    let n = row_count(
        &db,
        "CREATE RULE r AS MATCH (a:N)-[e:R]->(b:N) \
         FOLD total = SUM(e.weight) \
         YIELD KEY a.name AS name, total \
         QUERY r WHERE name = 'zzz_nonexistent' RETURN name, total",
    )
    .await;
    assert_eq!(
        n, 0,
        "QUERY WHERE on FOLD-rule KEY column was silently bypassed"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// CASE / IN in the in-memory (SLG) evaluator — previously returned Null
// because `eval_expr` had no arm for these variants and the error was
// swallowed into Null by the YIELD projection.
// ---------------------------------------------------------------------------

/// A searched CASE in a YIELD column evaluates to its branch value, not Null.
/// Nodes: a(w=1.0), b(w=2.0), c(w=3.0). `w > 1.5` → flag 1 for b,c and 0 for a.
#[tokio::test]
async fn case_in_yield_evaluates() -> Result<()> {
    let db = graph().await?;
    let ones = row_count(
        &db,
        "CREATE RULE r AS MATCH (a:N) \
         YIELD KEY a.name AS name, CASE WHEN a.w > 1.5 THEN 1 ELSE 0 END AS flag \
         QUERY r WHERE flag = 1 RETURN name, flag",
    )
    .await;
    assert_eq!(
        ones, 2,
        "CASE should yield 1 for b and c (was Null before fix)"
    );

    let zeros = row_count(
        &db,
        "CREATE RULE r AS MATCH (a:N) \
         YIELD KEY a.name AS name, CASE WHEN a.w > 1.5 THEN 1 ELSE 0 END AS flag \
         QUERY r WHERE flag = 0 RETURN name, flag",
    )
    .await;
    assert_eq!(zeros, 1, "CASE should yield 0 for a");
    Ok(())
}

/// An IN predicate in a QUERY WHERE evaluates via the in-memory evaluator.
/// `wv IN [2.0, 3.0]` keeps b and c.
#[tokio::test]
async fn in_predicate_evaluates() -> Result<()> {
    let db = graph().await?;
    let n = row_count(
        &db,
        "CREATE RULE r AS MATCH (a:N) \
         YIELD KEY a.name AS name, a.w AS wv \
         QUERY r WHERE wv IN [2.0, 3.0] RETURN name, wv",
    )
    .await;
    assert_eq!(
        n, 2,
        "IN predicate should keep b and c (errored to false before fix)"
    );
    Ok(())
}
