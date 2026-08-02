// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for `planner.rs` — `unreachable!("TimeTravel should be resolved at API
//! layer before planning")`.
//!
//! Five entry points in `impl_query.rs` destructure `Query::TimeTravel` before
//! planning: two resolve the snapshot and re-dispatch against a pinned
//! instance, three reject it cleanly because transactions cannot time-travel.
//! Three more parsed and planned straight through — `explain_internal`,
//! `profile_internal` and `execute_cursor_internal_with_config` — so the AST
//! reached the planner still wrapped and hit the `unreachable!`.
//!
//! That is a **panic**, not an error: it aborts the caller, and through the
//! PyO3 boundary it surfaces as `pyo3_runtime.PanicException` from a
//! `block_on`. Three of `QueryBuilder`'s four terminals were affected; only
//! `fetch_all` worked.
//!
//! Note what these assert. Not merely "does not panic" — that would also pass
//! if the fix simply stripped the spec and ran against the live version, which
//! is *worse* than panicking: the caller asked for history and would silently
//! get the present. `profile` and `cursor` therefore assert the historical row
//! count, and `explain` asserts a plan comes back for a query the live schema
//! can still describe.

use anyhow::Result;
use uni_db::Uni;

/// Two committed versions, returning the snapshot id of the first.
///
/// State 1: one Person. State 2: two. Any time-travelling read pinned to the
/// returned id must see exactly one.
async fn seed_two_versions(db: &Uni) -> Result<String> {
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.commit().await?;
    db.flush().await?;
    let snap1 = db.create_snapshot("alice-only").await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Bob'})").await?;
    tx.commit().await?;
    db.flush().await?;

    // Precondition: the live version really does have both.
    let live = db
        .session()
        .query("MATCH (n:Person) RETURN n.name AS name")
        .await?;
    assert_eq!(live.len(), 2, "precondition: live version has two people");

    Ok(snap1)
}

#[tokio::test]
async fn explain_accepts_a_time_travel_query() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let snap1 = seed_two_versions(&db).await?;

    let cypher = format!("MATCH (n:Person) RETURN n.name AS name VERSION AS OF '{snap1}'");

    // FIXED: the spec is resolved and the plan is produced against the pinned
    // instance instead of the wrapped AST reaching the planner's `unreachable!`.
    let plan = db.session().query_with(&cypher).explain().await?;
    assert!(
        !format!("{plan:?}").is_empty(),
        "explain must return a plan for a time-travel query"
    );

    Ok(())
}

#[tokio::test]
async fn profile_runs_a_time_travel_query_against_the_pinned_version() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let snap1 = seed_two_versions(&db).await?;

    let cypher = format!("MATCH (n:Person) RETURN n.name AS name VERSION AS OF '{snap1}'");

    let (result, _profile) = db.session().query_with(&cypher).profile().await?;

    // `profile` executes, so pinning is mandatory: one row, not the live two.
    assert_eq!(
        result.len(),
        1,
        "profile must execute against the pinned version, not the live one"
    );
    assert_eq!(result.rows()[0].get::<String>("name")?, "Alice");

    Ok(())
}

#[tokio::test]
async fn cursor_streams_a_time_travel_query_from_the_pinned_version() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let snap1 = seed_two_versions(&db).await?;

    let cypher = format!("MATCH (n:Person) RETURN n.name AS name VERSION AS OF '{snap1}'");

    let mut cursor = db.session().query_with(&cypher).cursor().await?;
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await {
        rows.extend(batch?);
    }

    assert_eq!(
        rows.len(),
        1,
        "cursor must stream the pinned version, not the live one"
    );
    assert_eq!(rows[0].get::<String>("name")?, "Alice");

    Ok(())
}

/// The already-working terminal, as a control on the seed data.
#[tokio::test]
async fn fetch_all_still_time_travels() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let snap1 = seed_two_versions(&db).await?;

    let cypher = format!("MATCH (n:Person) RETURN n.name AS name VERSION AS OF '{snap1}'");
    let result = db.session().query_with(&cypher).fetch_all().await?;

    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<String>("name")?, "Alice");

    Ok(())
}

/// Inverse guard: ordinary queries on all three terminals are unaffected.
#[tokio::test]
async fn non_time_travel_terminals_are_unaffected() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed_two_versions(&db).await?;

    let cypher = "MATCH (n:Person) RETURN n.name AS name";

    let plan = db.session().query_with(cypher).explain().await?;
    assert!(!format!("{plan:?}").is_empty());

    let (result, _profile) = db.session().query_with(cypher).profile().await?;
    assert_eq!(result.len(), 2, "live profile sees both people");

    let mut cursor = db.session().query_with(cypher).cursor().await?;
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await {
        rows.extend(batch?);
    }
    assert_eq!(rows.len(), 2, "live cursor sees both people");

    Ok(())
}
