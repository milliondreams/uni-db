// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for <https://github.com/rustic-ai/uni-db/issues/158>
//!
//! `WHERE x IS NOT some_rule` silently does not filter — every row is returned,
//! with no error and no warning.
//!
//! ## The issue's stated trigger is wrong
//!
//! The report attributes the failure to `x` "not being a KEY column". Measured
//! behaviour (the matrix below) shows KEY-ness is irrelevant:
//!
//! | consuming rule's YIELD    | negated subject | filters? |
//! |---------------------------|-----------------|----------|
//! | `YIELD KEY b, a`          | `a`             | yes      |
//! | `YIELD KEY b, KEY a AS s` | `s`             | yes      |
//! | `YIELD KEY b, a AS s`     | `s`             | yes      |
//! | `YIELD KEY b, a AS s`     | `a`             | **no**   |
//! | `YIELD KEY b`             | `a`             | **no**   |
//!
//! A *non*-KEY column filters correctly (row 1) and a *KEY* column fails once
//! aliased (row 4). The real predicate is purely name-based: does a column with
//! that exact name exist in the rule's **projected output**?
//!
//! ## Root cause
//!
//! Two independent faults compose.
//!
//! 1. **Phase ordering.** The anti-join runs *after* `LocyProject`
//!    (`Body(yield + positive IS) -> IS NOT(anti-join) -> PROB -> post-fixpoint`).
//!    `LocyProject` emits exactly the YIELD names
//!    (`uni-query/src/query/locy_planner.rs:1364`), so a variable that is bound
//!    in MATCH but not yielded — or yielded under an alias — no longer exists as
//!    a column by the time the negation is evaluated. The user writes `WHERE` as
//!    scoping over MATCH variables; it is implemented as scoping over projection
//!    outputs. `is_not_positive_is_ref_is_unaffected` pins the asymmetry: the
//!    *positive* `IS` form resolves the same subject correctly, because it is
//!    evaluated inside the body plan, before projection.
//!
//! 2. **Fail-open degradation.** Given an unresolvable column,
//!    `uni-query/src/query/df_graph/locy_complement.rs:205-212` executes
//!    `result.push(batch); continue` — it emits precisely the rows it was asked
//!    to exclude. A second pass-through at `:213-222` does the same when the
//!    column is not `UInt64`.
//!
//! Fault 2 is what makes this silent, and it is the security-relevant one: the
//! originating report describes a tax-exemption guard
//! (`WHERE c IS NOT exempt_customer`) that had no effect at all.
//!
//! The condition is decidable at compile time. `uni-locy/src/compiler/
//! typecheck.rs:252-284` already walks every `IsReference` to check arity and
//! holds the consuming rule's `yield_schema` at that point.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration \
//!     -E 'test(issue_158)' --run-ignored all

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// `a1` is flagged (it has an outgoing `E`); `a2` is not.
async fn setup() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("A")
        .property("n", DataType::String)
        .done()
        .label("B")
        .property("n", DataType::String)
        .done()
        .edge_type("E", &["A"], &["B"])
        .done()
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:A {n: 'a1'})").await?;
    tx.execute("CREATE (:A {n: 'a2'})").await?;
    tx.execute("CREATE (:B {n: 'b1'})").await?;
    tx.execute("MATCH (a:A {n: 'a1'}), (b:B {n: 'b1'}) CREATE (a)-[:E]->(b)")
        .await?;
    tx.commit().await?;
    Ok(db)
}

const FLAGGED: &str = "CREATE RULE flagged AS MATCH (a:A)-[:E]->(b:B) YIELD KEY a, KEY b";

/// Runs `probe` with the given YIELD clause and negated subject, returning the
/// sorted distinct `A.n` values that survived the filter.
///
/// `col` names the output column holding the `A` node.
async fn probe(db: &Uni, yield_clause: &str, subject: &str, col: &str) -> Result<Vec<String>> {
    let program = format!(
        "{FLAGGED}\n\
         CREATE RULE probe AS\n\
           MATCH (a:A), (b:B)\n\
           WHERE {subject} IS NOT flagged\n\
           {yield_clause}\n"
    );
    let result = db.session().locy(&program).await?;
    let empty = vec![];
    let facts = result.derived_facts("probe").unwrap_or(&empty);

    let mut names: Vec<String> = facts
        .iter()
        .filter_map(|row| match row.get(col) {
            Some(Value::Node(node)) => match node.properties.get("n") {
                Some(Value::String(s)) => Some(s.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect();
    names.sort();
    names.dedup();
    Ok(names)
}

// ---------------------------------------------------------------------------
// Green: the paths that DO work. These pin the boundary of the bug — each one
// falsifies the "must be a KEY column" hypothesis from the issue report.
// ---------------------------------------------------------------------------

/// A **non-KEY** yielded column filters correctly, so KEY-ness is not the
/// discriminator. This is the single most important assertion in the file.
#[tokio::test]
async fn issue_158_non_key_column_with_own_name_does_filter() -> Result<()> {
    let db = setup().await?;
    let got = probe(&db, "YIELD KEY b, a", "a", "a").await?;
    assert_eq!(
        got,
        vec!["a2".to_string()],
        "a non-KEY column that keeps its own name must still filter; \
         KEY-ness is not what makes IS NOT work"
    );
    Ok(())
}

/// The mirror: a KEY column reached through its **alias** also filters, so the
/// resolution is by projected name in both directions.
#[tokio::test]
async fn issue_158_key_column_via_alias_does_filter() -> Result<()> {
    let db = setup().await?;
    let got = probe(&db, "YIELD KEY b, KEY a AS subj", "subj", "subj").await?;
    assert_eq!(got, vec!["a2".to_string()]);
    Ok(())
}

/// A non-KEY aliased column filters when the negation names the alias — the
/// documented workaround for the bug until the scope fix lands.
#[tokio::test]
async fn issue_158_aliased_column_filters_under_the_alias_name() -> Result<()> {
    let db = setup().await?;
    let got = probe(&db, "YIELD KEY b, a AS subj", "subj", "subj").await?;
    assert_eq!(got, vec!["a2".to_string()]);
    Ok(())
}

/// The **positive** `IS` form resolves a subject the negated form cannot,
/// because it is evaluated in the body plan before `LocyProject`. This
/// asymmetry is the direct evidence for the phase-ordering root cause.
#[tokio::test]
async fn issue_158_positive_is_ref_is_unaffected() -> Result<()> {
    let db = setup().await?;
    let program = format!(
        "{FLAGGED}\n\
         CREATE RULE probe AS\n\
           MATCH (a:A), (b:B)\n\
           WHERE a IS flagged\n\
           YIELD KEY b, a AS subj\n"
    );
    let result = db.session().locy(&program).await?;
    let empty = vec![];
    let facts = result.derived_facts("probe").unwrap_or(&empty);
    let names: Vec<String> = facts
        .iter()
        .filter_map(|row| match row.get("subj") {
            Some(Value::Node(node)) => match node.properties.get("n") {
                Some(Value::String(s)) => Some(s.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert_eq!(
        names,
        vec!["a1".to_string()],
        "positive IS resolves the same subject that negated IS NOT cannot"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Wave 0: the negation now FAILS LOUDLY instead of returning unfiltered rows.
//
// These two programs are semantically valid — `a` is bound in MATCH — so
// erroring is not the final answer, it is the safe intermediate one. Wave 1
// carries a hidden `a._vid` column through `LocyProject` so the anti-join can
// resolve the subject regardless of what YIELD named it; at that point both
// tests flip again, to assert filtering (`["a2"]`). Until then, a loud error
// beats a confident wrong answer.
// ---------------------------------------------------------------------------

/// Asserts the error surfaced by an unresolvable `IS NOT` subject names the
/// column and explains the projection requirement.
fn assert_unresolvable_subject_error(err: &anyhow::Error, subject: &str) {
    let msg = err.to_string();
    assert!(
        msg.contains("IS NOT") && msg.contains(subject),
        "error must name the unresolvable subject `{subject}`; got: {msg}"
    );
    assert!(
        msg.contains("projected"),
        "error must explain the projection requirement; got: {msg}"
    );
}

/// The subject is bound in MATCH and yielded, but under an alias — so the bare
/// name no longer names a column. Previously the anti-join was skipped and
/// every row was returned.
#[tokio::test]
async fn issue_158_aliased_subject_errors_rather_than_failing_open() -> Result<()> {
    let db = setup().await?;
    let err = probe(&db, "YIELD KEY b, a AS subj", "a", "subj")
        .await
        .expect_err("an unresolvable IS NOT subject must not silently succeed");
    assert_unresolvable_subject_error(&err, "a");
    Ok(())
}

/// The worst shape: the subject is bound in MATCH but never yielded as a node
/// column, so no name could reach it. This is the exemption-guard case from the
/// originating report — the one that billed tax to exempt customers.
#[tokio::test]
async fn issue_158_unprojected_subject_errors_rather_than_failing_open() -> Result<()> {
    let db = setup().await?;
    let program = format!(
        "{FLAGGED}\n\
         CREATE RULE probe AS\n\
           MATCH (a:A), (b:B)\n\
           WHERE a IS NOT flagged\n\
           YIELD KEY b, a.n AS name\n"
    );
    let err = db
        .session()
        .locy(&program)
        .await
        .err()
        .map(anyhow::Error::from)
        .expect("an unresolvable IS NOT subject must not silently succeed");
    assert_unresolvable_subject_error(&err, "a");
    Ok(())
}
