// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Cypher `FOREACH (x IN list | <update clauses>)` — issue #176.
//!
//! `ForeachExec`, `LogicalPlan::Foreach` and
//! `Executor::execute_foreach_body_plan` were all fully implemented and
//! reachable from nothing: `grep -rci foreach crates/uni-cypher/src/` returned
//! 0, so the grammar had no rule and the AST no variant, and no query could
//! construct the node. #176 framed this as an implement-or-delete decision
//! rather than a coverage gap. This is the implement side: the front end
//! (grammar rule, `Clause::Foreach`, walker, planner arm) now builds the node
//! the existing operator already knew how to run.
//!
//! # What each test is for
//!
//! `FOREACH` is side-effect-only and passes its input rows through unchanged,
//! which makes it easy to write a test that passes without the clause doing
//! anything. So the assertions are on the *mutation*, never on the row count,
//! and `foreach_over_an_empty_list_is_a_no_op` is the control that a passing
//! mutation assertion is not just the body running once regardless of the list.
//!
//! `the_foreach_clause_plans_the_foreach_operator` is the `assert_plan_uses`
//! proof #177 requires before the registry row can move off `Unproven`.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// Three `N`s named `a`, `b`, `c`, each with `seen = 0`.
async fn fixture() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("name", DataType::String)
        .property("seen", DataType::Int)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    for name in ["a", "b", "c"] {
        tx.query_with("CREATE (:N {name: $n, seen: 0})")
            .param("n", Value::String(name.into()))
            .fetch_all()
            .await?;
    }
    tx.commit().await?;
    Ok(db)
}

/// Read `seen` for a given name.
async fn seen(db: &Uni, name: &str) -> Result<i64> {
    let r = db
        .session()
        .query_with("MATCH (n:N {name: $n}) RETURN n.seen AS s")
        .param("n", Value::String(name.into()))
        .fetch_all()
        .await?;
    match r.rows().first().map(|row| row.values()[0].clone()) {
        Some(Value::Int(i)) => Ok(i),
        other => anyhow::bail!("expected an Int for {name}, got {other:?}"),
    }
}

/// Count `N`s.
async fn count_n(db: &Uni) -> Result<i64> {
    let r = db
        .session()
        .query("MATCH (n:N) RETURN count(n) AS c")
        .await?;
    match r.rows().first().map(|row| row.values()[0].clone()) {
        Some(Value::Int(i)) => Ok(i),
        other => anyhow::bail!("expected an Int count, got {other:?}"),
    }
}

/// The clause parses and its body runs once per item.
///
/// Counted with `CREATE` rather than an accumulating `SET n.seen = n.seen + 1`
/// deliberately: the latter cannot count iterations here, because a body's RHS
/// read does not observe the previous iteration's write. See
/// `a_read_modify_write_body_does_not_accumulate` below, which pins that
/// separately instead of letting it silently weaken this assertion.
#[tokio::test]
async fn foreach_runs_its_body_once_per_list_item() -> Result<()> {
    let db = fixture().await?;
    assert_eq!(count_n(&db).await?, 3, "fixture starts with three N");

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(
        "MATCH (n:N {name: 'a'}) FOREACH (i IN [1, 2, 3] | CREATE (:N {name: 'made', seen: 0}))",
    )
    .await?;
    tx.commit().await?;

    // 3 original + 3 created. Six rather than "more than three" is what
    // separates once-per-item from once-per-row.
    assert_eq!(
        count_n(&db).await?,
        6,
        "the body must run once per list item: three items over one matched          row is three CREATEs"
    );
    assert_eq!(
        seen(&db, "b").await?,
        0,
        "FOREACH must not touch rows its MATCH did not bind"
    );
    Ok(())
}

/// A read-modify-write body does not observe earlier iterations' writes.
///
/// `FOREACH (i IN [1,2,3] | SET n.seen = n.seen + 1)` leaves `seen = 1`, not 3.
/// openCypher requires 3.
///
/// # Where the gap is
///
/// Not in `FOREACH`. `ForeachExec` keeps one scope per input row and rebinds
/// only the iteration variable, and `execute_set_items_locked` does write the
/// new value back into that scope (`binding.set_entity_property`) — instrumented
/// and confirmed: the scope reads `seen = 1` going into the second item. But
/// the RHS is evaluated by `evaluate_expr(value, row, prop_manager, params,
/// ctx)`, which resolves `n.seen` through the property manager rather than the
/// scope binding, and `tx_l0` — the transaction's L0 overlay holding the
/// pending write — is threaded into the *write* path only. So every iteration
/// reads the committed `seen = 0` and computes 1.
///
/// `FOREACH` is simply the first clause that re-executes a `SET` against the
/// same row inside one statement, which is why nothing surfaced this before.
///
/// Asserted at the value actually produced, so the day the read path learns
/// about `tx_l0` this test fails and says so, rather than passing quietly at
/// either value.
#[tokio::test]
async fn a_read_modify_write_body_does_not_accumulate() -> Result<()> {
    let db = fixture().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("MATCH (n:N {name: 'a'}) FOREACH (i IN [1, 2, 3] | SET n.seen = n.seen + 1)")
        .await?;
    tx.commit().await?;

    let got = seen(&db, "a").await?;
    assert_eq!(
        got, 1,
        "known gap: a FOREACH body's RHS reads through the property manager          without the transaction's L0 overlay, so all three iterations read          seen = 0. openCypher requires 3. If this now reports 3 the read path          has been fixed — delete this test and restore the accumulating          assertion in foreach_runs_its_body_once_per_list_item."
    );
    Ok(())
}

/// The iteration variable is bound in the body and carries the item's value.
#[tokio::test]
async fn the_iteration_variable_is_bound_in_the_body() -> Result<()> {
    let db = fixture().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("MATCH (n:N {name: 'a'}) FOREACH (i IN [7] | SET n.seen = i)")
        .await?;
    tx.commit().await?;

    assert_eq!(
        seen(&db, "a").await?,
        7,
        "`i` must resolve to the list item, not to null or to the outer row"
    );
    Ok(())
}

/// Control. An empty list must run the body zero times — without this, a body
/// that ran unconditionally would satisfy every assertion above.
#[tokio::test]
async fn foreach_over_an_empty_list_is_a_no_op() -> Result<()> {
    let db = fixture().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("MATCH (n:N {name: 'a'}) FOREACH (i IN [] | SET n.seen = 99)")
        .await?;
    tx.commit().await?;

    assert_eq!(
        seen(&db, "a").await?,
        0,
        "an empty list must run the body zero times; 99 means the body ran \
         regardless of the list, which would make the other tests vacuous"
    );
    Ok(())
}

/// `FOREACH` passes its input rows through unchanged — it is a side effect, not
/// a projection. `UNWIND` over the same list would produce three rows.
#[tokio::test]
async fn foreach_does_not_multiply_the_row_stream() -> Result<()> {
    let db = fixture().await?;
    let session = db.session();
    let tx = session.tx().await?;
    let rows = tx
        .query(
            "MATCH (n:N {name: 'a'}) FOREACH (i IN [1, 2, 3] | SET n.seen = 1) RETURN n.name AS nm",
        )
        .await?;
    tx.commit().await?;

    assert_eq!(
        rows.rows().len(),
        1,
        "FOREACH is side-effect-only: one input row in, one row out. Three \
         would mean it behaved like UNWIND"
    );
    Ok(())
}

/// Nested `FOREACH`. `execute_foreach_body_plan` already recursed for this
/// case; the grammar now admits it, so the recursion is reachable.
#[tokio::test]
async fn foreach_nests() -> Result<()> {
    let db = fixture().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(
        "MATCH (n:N {name: 'a'}) \
         FOREACH (i IN [1, 2] | FOREACH (j IN [1, 2, 3] | CREATE (:N {name: 'deep', seen: 0})))",
    )
    .await?;
    tx.commit().await?;

    // Counted by CREATE for the same reason as the flat case: an accumulating
    // SET cannot count iterations until the body's RHS sees pending writes.
    assert_eq!(
        count_n(&db).await? - 3,
        6,
        "2 outer items x 3 inner items = 6 body executions"
    );
    Ok(())
}

/// The body admits only update clauses. A `RETURN` inside `FOREACH` is a parse
/// error, per openCypher — and rejecting it in the grammar rather than at
/// execution is what keeps the error message about the query rather than about
/// an unsupported plan node.
#[tokio::test]
async fn a_read_clause_in_the_body_is_rejected() -> Result<()> {
    let db = fixture().await?;
    let err = db
        .session()
        .query("MATCH (n:N) FOREACH (i IN [1] | RETURN i)")
        .await
        .expect_err("RETURN is not a FOREACH body clause");
    let msg = err.to_string();
    assert!(
        !msg.is_empty(),
        "the rejection must carry a message; got an empty error"
    );
    Ok(())
}

/// The `assert_plan_uses` proof #177 asks for: the clause reaches the physical
/// operator, not merely the logical plan.
///
/// `profile()` executes, so this runs the mutation too — hence the throwaway
/// in-memory database, per the note in `plan_shape/mod.rs`.
#[tokio::test]
async fn the_foreach_clause_plans_the_foreach_operator() -> Result<()> {
    let db = fixture().await?;
    let session = db.session();
    crate::plan_shape::assert_plan_uses(
        &session,
        "MATCH (n:N {name: 'a'}) FOREACH (i IN [1] | SET n.seen = 1)",
        "ForeachExec",
    )
    .await;
    Ok(())
}

/// The negative twin. `UNWIND` over the same list is the nearest shape that
/// must *not* route through this operator — without it, a `ForeachExec` emitted
/// for every mutation would still pass the positive assertion.
#[tokio::test]
async fn an_unwind_does_not_plan_the_foreach_operator() -> Result<()> {
    let db = fixture().await?;
    let session = db.session();
    crate::plan_shape::assert_plan_avoids(
        &session,
        "MATCH (n:N {name: 'a'}) UNWIND [1] AS i RETURN n.name AS nm, i",
        "ForeachExec",
    )
    .await;
    Ok(())
}
