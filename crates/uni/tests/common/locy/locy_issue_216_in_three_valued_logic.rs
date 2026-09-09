// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression test for <https://github.com/rustic-ai/uni-db/issues/216>
//!
//! `IN` on the Locy in-memory evaluation path had no three-valued logic. The
//! `Expr::In` arm returned `Value::Bool(items.iter().any(...))`, which can only
//! be `true` or `false`, so a list containing `null` produced `false` where
//! Cypher requires `null`.
//!
//! # Why `NOT` is what makes this observable
//!
//! `filter_where` (`locy_query.rs`) keeps a row on `as_bool().unwrap_or(false)`,
//! so `null` and `false` both drop a row. A bare `WHERE x IN [...]` therefore
//! cannot tell the two apart — it is a **non-discriminating** probe and would
//! have passed against the defect.
//!
//! `NOT` is the discriminator, because `eval_unary_op` propagates `Value::Null`
//! correctly (`Null => Ok(Null)`) while inverting a `Bool`:
//!
//! | `name IN ['a', null]` | `NOT (...)` | row kept? |
//! |---|---|---|
//! | before: `Bool(false)` | `Bool(true)`  | **yes — wrong** |
//! | after:  `Null`        | `Null`        | no — correct    |
//!
//! # The control
//!
//! An empty result set is a weak assertion: a broken query, a parse failure or
//! a rule that yielded nothing all produce one. `not_in_without_a_null_still_
//! keeps_the_non_matching_row` is the paired control — same rule, same `QUERY`
//! shape, same `NOT (... IN ...)`, with only the `null` removed from the list.
//! It must return `{"b"}`. Without it, the discriminating test could pass for
//! the wrong reason.

// Rust guideline compliant

use std::collections::HashSet;

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// Two `Item`s named `a` and `b`, and a rule that yields both names.
async fn setup_items() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Item {name: 'a'})").await?;
    tx.execute("CREATE (:Item {name: 'b'})").await?;
    tx.commit().await?;
    Ok(db)
}

/// Run `QUERY named WHERE {where_expr} RETURN name` and collect the names.
///
/// The `WHERE` sits on the `QUERY` clause, not on the rule body: that is the
/// clause `filter_where` evaluates with the in-memory evaluator. A rule-body
/// `WHERE` is lowered to DataFusion instead and would not reach `Expr::In`
/// here at all.
async fn names_matching(db: &Uni, where_expr: &str) -> Result<HashSet<String>> {
    let program = format!(
        r#"
        CREATE RULE named AS
            MATCH (i:Item)
            YIELD KEY i.name AS name
        QUERY named WHERE {where_expr} RETURN name
    "#
    );
    let result = db.session().locy(&program).await?;
    let empty = vec![];
    Ok(result
        .rows()
        .unwrap_or(&empty)
        .iter()
        .filter_map(|r| match r.get("name") {
            Some(Value::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect())
}

/// The defect: `NOT (name IN ['a', null])` must be `null` for `name = 'b'`,
/// so no row survives. Before the fix `IN` returned `false`, `NOT` inverted it
/// to `true`, and `b` was returned.
#[tokio::test]
async fn not_in_a_list_containing_null_is_null_and_keeps_no_row() -> Result<()> {
    let db = setup_items().await?;
    let names = names_matching(&db, "NOT (name IN ['a', null])").await?;
    assert!(
        names.is_empty(),
        "`NOT (name IN ['a', null])` must be null for every row — null for 'b' \
         because the list holds a null and nothing matched, false for 'a' \
         because it matched. Got {names:?}; {:?} means IN collapsed null to \
         false and NOT inverted it (#216).",
        names
    );
    Ok(())
}

/// The control for the test above. Identical shape with the `null` removed:
/// `b` must come back. If this ever goes empty, the assertion above is passing
/// vacuously and proves nothing.
#[tokio::test]
async fn not_in_without_a_null_still_keeps_the_non_matching_row() -> Result<()> {
    let db = setup_items().await?;
    let names = names_matching(&db, "NOT (name IN ['a'])").await?;
    assert_eq!(
        names,
        HashSet::from(["b".to_string()]),
        "control: with no null in the list `IN` is two-valued, so `b` must \
         survive `NOT`. An empty set here means the rule, the QUERY WHERE or \
         the projection is broken, not that #216 is fixed."
    );
    Ok(())
}

/// A null needle is null regardless of the list, so `NOT` keeps no row.
#[tokio::test]
async fn a_null_needle_makes_in_null() -> Result<()> {
    let db = setup_items().await?;
    let names = names_matching(&db, "NOT (null IN ['a', 'b'])").await?;
    assert!(
        names.is_empty(),
        "`null IN [...]` is null, so `NOT` of it is null and drops every row. \
         Got {names:?}"
    );
    Ok(())
}

/// Positive membership is unchanged: `IN` still returns `true` when an element
/// matches, even though a null sits in the list. Cypher resolves a true match
/// before the unknown. Guards the fix against over-returning null.
#[tokio::test]
async fn a_match_beats_a_null_in_the_list() -> Result<()> {
    let db = setup_items().await?;
    let names = names_matching(&db, "name IN ['a', null]").await?;
    assert_eq!(
        names,
        HashSet::from(["a".to_string()]),
        "'a' matches an element, so IN is true despite the null; 'b' matches \
         nothing and is null, so it drops. Over-returning null would empty this."
    );
    Ok(())
}
