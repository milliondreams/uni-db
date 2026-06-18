// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! REQ-1b regression: a Locy program's result must be a pure function of
//! (program, data, params) — independent of execution context.
//!
//! # Bug
//!
//! The Locy command-dispatch path (`QUERY`/SLG, EXPLAIN, ASSUME/ABDUCE, and the
//! delta engine's base-fact `MATCH`) rebuilt its execution context from *live*
//! storage and *live* L0 generations, ignoring the transaction's pinned MVCC
//! read snapshot. So inside `tx.locy(...)` a concurrent commit (or an L0→L1
//! flush completing mid-transaction) leaked into pattern matching — making the
//! same program return different per-iteration results via `session.locy()`
//! (live) vs `tx.locy()` (should be snapshot-isolated).
//!
//! # Fix
//!
//! `NativeExecutionAdapter` now carries the pinned `SnapshotView` and rebuilds
//! its context from the frozen L0 generations (`snap.main`/`snap.extra`) and the
//! version-pinned L1 storage, exactly like the fixpoint planner. These tests pin
//! a snapshot, commit a matching edge from a *concurrent* session, and assert
//! the transaction's repeated read is unchanged (isolation) while a fresh
//! `session.locy()` sees the new edge (liveness — we did not over-pin).

use anyhow::Result;
use uni_db::Uni;
use uni_db::locy::{CommandResult, LocyConfig, LocyResult};

/// A program ending in a `QUERY`, which re-evaluates the rule's base `MATCH`
/// through the SLG/`execute_pattern` command-dispatch path — the path that used
/// to bypass the read snapshot. A single non-recursive `edge` rule makes the row
/// count exactly the number of `:E` edges visible to the read, isolating the
/// snapshot concern from fixpoint semantics.
const EDGE_QUERY: &str = "CREATE RULE edge AS \
       MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b \n\
     QUERY edge RETURN a.name AS src, b.name AS dst";

fn config() -> LocyConfig {
    LocyConfig {
        max_iterations: 1000,
        ..Default::default()
    }
}

/// Rows returned by a trailing `QUERY` command.
fn query_row_count(result: &LocyResult) -> usize {
    for cr in &result.command_results {
        if let CommandResult::Query(rows) = cr {
            return rows.len();
        }
    }
    panic!(
        "no QUERY command result found in {:?}",
        result.command_results
    );
}

/// Seed a single edge A→B.
async fn seed_one_edge(db: &Uni) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:N {name: 'A'})-[:E]->(:N {name: 'B'})")
        .await?;
    tx.commit().await?;
    Ok(())
}

/// Core REQ-1b: `tx.locy()` reads its pinned snapshot; a concurrent commit after
/// the snapshot is pinned must NOT change the transaction's result, but a fresh
/// `session.locy()` must see it. Before the fix, the SLG `execute_pattern` read
/// live storage, so the transaction leaked the concurrent edge (2 rows) instead
/// of staying isolated (1 row) — i.e. tx and session results wrongly agreed
/// instead of the tx being snapshot-isolated.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tx_locy_query_is_snapshot_isolated_session_is_live() -> Result<()> {
    let db = seed_db().await?;

    let s_a = db.session();
    let tx_a = s_a.tx().await?;

    // First read pins the transaction's snapshot. Only A→B is reachable.
    let r1 = tx_a.locy(EDGE_QUERY).await?;
    assert_eq!(
        query_row_count(&r1),
        1,
        "tx should initially see only the A->B edge"
    );

    // A concurrent session commits a B→C edge AFTER tx_a pinned its snapshot.
    {
        let s_b = db.session();
        let tx_b = s_b.tx().await?;
        tx_b.execute("MATCH (b:N {name: 'B'}) CREATE (b)-[:E]->(:N {name: 'C'})")
            .await?;
        tx_b.commit().await?;
    }

    // Isolation: re-running the SAME program in tx_a must still see only A→B.
    // (Live reads would now also yield the B→C edge ⇒ 2 rows.)
    let r2 = tx_a.locy(EDGE_QUERY).await?;
    assert_eq!(
        query_row_count(&r2),
        1,
        "tx.locy() leaked a concurrently-committed edge — snapshot was bypassed"
    );

    // Liveness: a fresh session reads live committed state ⇒ A→B and B→C.
    let r3 = db.session().locy(EDGE_QUERY).await?;
    assert_eq!(
        query_row_count(&r3),
        2,
        "session.locy() must see the concurrently-committed B->C edge"
    );

    Ok(())
}

async fn seed_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    seed_one_edge(&db).await?;
    Ok(db)
}

/// Context-independence in the quiescent case: with NO concurrent writes, the
/// same program must yield identical results via `session.locy()` and
/// `tx.locy()`. Guards against the snapshot path and the live path disagreeing
/// on a stable database.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_and_tx_locy_agree_when_quiescent() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:N {name: 'A'})-[:E]->(:N {name: 'B'})-[:E]->(:N {name: 'C'})")
        .await?;
    tx.commit().await?;

    let via_session = db.session().locy(EDGE_QUERY).await?;

    let s = db.session();
    let tx_r = s.tx().await?;
    let via_tx = tx_r.locy(EDGE_QUERY).await?;
    tx_r.commit().await?;

    // A→B→C has two direct :E edges ⇒ 2 rows, in both contexts.
    assert_eq!(query_row_count(&via_session), 2);
    assert_eq!(
        query_row_count(&via_session),
        query_row_count(&via_tx),
        "session.locy() and tx.locy() must agree on a quiescent database"
    );
    Ok(())
}

/// Same isolation guarantee for an in-transaction read whose program uses the
/// fixpoint config explicitly (covers the `locy_with(...).run()` builder path,
/// which threads the snapshot the same way).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tx_locy_with_builder_is_snapshot_isolated() -> Result<()> {
    let db = seed_db().await?;
    let s_a = db.session();
    let tx_a = s_a.tx().await?;

    let r1 = tx_a
        .locy_with(EDGE_QUERY)
        .with_config(config())
        .run()
        .await?;
    assert_eq!(query_row_count(&r1), 1);

    {
        let s_b = db.session();
        let tx_b = s_b.tx().await?;
        tx_b.execute("MATCH (b:N {name: 'B'}) CREATE (b)-[:E]->(:N {name: 'C'})")
            .await?;
        tx_b.commit().await?;
    }

    let r2 = tx_a
        .locy_with(EDGE_QUERY)
        .with_config(config())
        .run()
        .await?;
    assert_eq!(
        query_row_count(&r2),
        1,
        "builder-path tx.locy() leaked a concurrently-committed edge"
    );
    Ok(())
}
