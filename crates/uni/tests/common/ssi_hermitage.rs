// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The Hermitage isolation-anomaly suite, adapted to uni-db's SSI/OCC.
//!
//! Martin Kleppmann's "Hermitage" (github.com/ept/hermitage) is the standard
//! battery for probing exactly which isolation anomalies an engine prevents.
//! Each test scripts a precise two-transaction interleaving and asserts the
//! outcome our design promises:
//!
//! - **Prevented** (must abort the right transaction or hide the write):
//!   P4 lost update, G1a/G1b/G1c dirty/intermediate/circular reads, OTV atomic
//!   visibility, G-single read-skew-with-write, and the headline **G2-item write
//!   skew** — the test that separates true SSI from plain snapshot isolation.
//! - **Out of scope, asserted as documented**: the predicate phantom (PMP / G2).
//!   Item-level SSI tracks the *items* a transaction read, not the *gaps* in a
//!   predicate, so a concurrently-inserted row that matches the predicate is not
//!   a conflict. We assert the phantom DOES occur, fencing the guarantee so a
//!   future change that silently alters it is caught.
//!
//! No barriers: each interleaving is a single task awaiting two transactions'
//! operations in a chosen order; the only contention point (`flush_lock` at
//! commit) is reached one transaction at a time. See `ssi_support::schedule`.

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

use crate::ssi_support::schedule::{assert_committed, assert_serialization_conflict};

/// A `T(id, val)` table seeded with the given rows.
async fn db_with(rows: &[(&str, i64)]) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("T")
        .property("id", DataType::String)
        .property("val", DataType::Int)
        .done()
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    for (id, v) in rows {
        tx.execute(&format!("CREATE (:T {{id: '{id}', val: {v}}})"))
            .await?;
    }
    tx.commit().await?;
    Ok(db)
}

/// Committed value of `T{id}` read via a fresh read-only query.
async fn val(db: &Uni, id: &str) -> Result<i64> {
    let r = db
        .session()
        .query(&format!("MATCH (n:T {{id: '{id}'}}) RETURN n.val AS v"))
        .await?;
    match r.rows()[0].value("v") {
        Some(Value::Int(n)) => Ok(*n),
        other => panic!("expected Int, got {other:?}"),
    }
}

// ── P4: Lost update (PREVENTED) ──────────────────────────────────────────────

/// Two transactions read x from the same snapshot, each increment, and commit.
/// The second commit must abort — otherwise one increment is lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p4_lost_update_is_prevented() -> Result<()> {
    let db = db_with(&[("x", 0)]).await?;
    let (sa, sb) = (db.session(), db.session());
    let ta = sa.tx().await?;
    let tb = sb.tx().await?;

    // Both read-modify-write x from their begin snapshots.
    ta.execute("MATCH (n:T {id: 'x'}) SET n.val = n.val + 1")
        .await?;
    tb.execute("MATCH (n:T {id: 'x'}) SET n.val = n.val + 1")
        .await?;

    assert_committed(ta.commit().await); // first writer wins
    assert_serialization_conflict(tb.commit().await); // second must abort

    // After the loser aborts, x reflects exactly the one committed increment.
    assert_eq!(val(&db, "x").await?, 1, "no lost update");
    Ok(())
}

// ── G2-item: Write skew (PREVENTED — the decisive SSI test) ───────────────────

/// Invariant: x + y >= 1, upheld by "you may zero one only while the other is 1".
/// T1 reads both, zeroes x; T2 reads both, zeroes y. Under plain snapshot
/// isolation both commit and the invariant breaks (x=y=0). Item-level SSI must
/// abort the second: its read-set {x,y} intersects the first's write-set {x}.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g2_item_write_skew_is_prevented() -> Result<()> {
    let db = db_with(&[("x", 1), ("y", 1)]).await?;
    let (sa, sb) = (db.session(), db.session());
    let ta = sa.tx().await?;
    let tb = sb.tx().await?;

    // Each transaction reads BOTH items (the predicate of the invariant) — this
    // is what populates the read-set that makes the skew detectable.
    ta.query("MATCH (n:T {id: 'x'}) RETURN n.val").await?;
    ta.query("MATCH (n:T {id: 'y'}) RETURN n.val").await?;
    tb.query("MATCH (n:T {id: 'x'}) RETURN n.val").await?;
    tb.query("MATCH (n:T {id: 'y'}) RETURN n.val").await?;

    // Disjoint writes: T1 zeroes x, T2 zeroes y.
    ta.execute("MATCH (n:T {id: 'x'}) SET n.val = 0").await?;
    tb.execute("MATCH (n:T {id: 'y'}) SET n.val = 0").await?;

    assert_committed(ta.commit().await);
    // T2 read x, which T1 just wrote — a read-write antidependency. Abort.
    assert_serialization_conflict(tb.commit().await);

    // Invariant x + y >= 1 holds: only x was zeroed.
    assert!(val(&db, "x").await? + val(&db, "y").await? >= 1, "write skew leaked");
    Ok(())
}

// ── G1a: Dirty read / aborted read (PREVENTED) ───────────────────────────────

/// A transaction must never observe another's uncommitted (and ultimately
/// rolled-back) write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g1a_dirty_read_is_prevented() -> Result<()> {
    let db = db_with(&[("x", 0)]).await?;
    let (sa, sb) = (db.session(), db.session());
    let ta = sa.tx().await?;
    ta.execute("MATCH (n:T {id: 'x'}) SET n.val = 99").await?;

    // T2 begins while T1's write is uncommitted; it must see the committed 0.
    let tb = sb.tx().await?;
    let r = tb
        .query("MATCH (n:T {id: 'x'}) RETURN n.val AS v")
        .await?;
    assert_eq!(
        r.rows()[0].value("v"),
        Some(&Value::Int(0)),
        "dirty read: observed T1's uncommitted write"
    );

    ta.rollback();
    drop(tb);
    assert_eq!(val(&db, "x").await?, 0);
    Ok(())
}

// ── G1b: Intermediate read (PREVENTED) ───────────────────────────────────────

/// A transaction must not observe another transaction's intermediate value
/// (a write later overwritten within the same transaction).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g1b_intermediate_read_is_prevented() -> Result<()> {
    let db = db_with(&[("x", 0)]).await?;
    let (sa, sb) = (db.session(), db.session());
    let ta = sa.tx().await?;
    let tb = sb.tx().await?; // snapshot before any T1 write

    ta.execute("MATCH (n:T {id: 'x'}) SET n.val = 1").await?; // intermediate
    ta.execute("MATCH (n:T {id: 'x'}) SET n.val = 2").await?; // final

    // T2's snapshot predates T1: it sees neither 1 nor 2.
    let r = tb.query("MATCH (n:T {id: 'x'}) RETURN n.val AS v").await?;
    assert_eq!(r.rows()[0].value("v"), Some(&Value::Int(0)), "saw intermediate/final");

    assert_committed(ta.commit().await);
    drop(tb);
    assert_eq!(val(&db, "x").await?, 2);
    Ok(())
}

// ── G1c: Circular information flow (PREVENTED) ────────────────────────────────

/// T1 reads y then writes x; T2 reads x then writes y. The read-write
/// dependencies form a cycle; SSI must break it by aborting one transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn g1c_circular_information_flow_is_prevented() -> Result<()> {
    let db = db_with(&[("x", 10), ("y", 20)]).await?;
    let (sa, sb) = (db.session(), db.session());
    let ta = sa.tx().await?;
    let tb = sb.tx().await?;

    ta.query("MATCH (n:T {id: 'y'}) RETURN n.val").await?; // T1 reads y
    tb.query("MATCH (n:T {id: 'x'}) RETURN n.val").await?; // T2 reads x

    ta.execute("MATCH (n:T {id: 'x'}) SET n.val = 30").await?; // T1 writes x
    assert_committed(ta.commit().await);

    tb.execute("MATCH (n:T {id: 'y'}) SET n.val = 40").await?; // T2 writes y
    // T2 read x, which T1 wrote — the cycle is cut here.
    assert_serialization_conflict(tb.commit().await);
    Ok(())
}

// ── OTV: Observed transaction vanishes (PREVENTED — atomic visibility) ─────────

/// A transaction's two writes become visible atomically: a concurrent snapshot
/// sees both or neither, never one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn otv_atomic_visibility() -> Result<()> {
    let db = db_with(&[("x", 0), ("y", 0)]).await?;
    let (sa, sb) = (db.session(), db.session());

    let tb = sb.tx().await?; // snapshot at 0,0 BEFORE T1 commits

    let ta = sa.tx().await?;
    ta.execute("MATCH (n:T {id: 'x'}) SET n.val = 1").await?;
    ta.execute("MATCH (n:T {id: 'y'}) SET n.val = 1").await?;
    assert_committed(ta.commit().await);

    // T2's snapshot predates the commit: it must see BOTH old values, not a torn
    // mix where x=1 but y=0 (or vice versa).
    let rx = tb.query("MATCH (n:T {id: 'x'}) RETURN n.val AS v").await?;
    let ry = tb.query("MATCH (n:T {id: 'y'}) RETURN n.val AS v").await?;
    assert_eq!(rx.rows()[0].value("v"), Some(&Value::Int(0)), "x torn");
    assert_eq!(ry.rows()[0].value("v"), Some(&Value::Int(0)), "y torn");
    drop(tb);

    // A fresh reader after the commit sees both new values.
    assert_eq!(val(&db, "x").await?, 1);
    assert_eq!(val(&db, "y").await?, 1);
    Ok(())
}

// ── PMP / G2: Predicate phantom (OUT OF SCOPE — asserted to occur) ────────────

/// Item-level SSI tracks read *items*, not predicate *gaps*. A row inserted by a
/// concurrent transaction that matches a predicate this transaction evaluated is
/// NOT a conflict, because the new vid was never in the read-set. This is a known
/// limitation (use `FOR UPDATE` or coarser locking for phantom protection); we
/// assert it so the boundary is explicit and a silent change is caught.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pmp_predicate_phantom_is_not_prevented() -> Result<()> {
    let db = db_with(&[("x", 0)]).await?;
    let (sa, sb) = (db.session(), db.session());
    let ta = sa.tx().await?;

    // T1 evaluates a predicate over the T set (records the existing items).
    let before = ta.query("MATCH (n:T) RETURN count(n) AS c").await?;
    assert_eq!(before.rows()[0].value("c"), Some(&Value::Int(1)));

    // T2 inserts a NEW row matching the predicate and commits.
    {
        let tb = sb.tx().await?;
        tb.execute("CREATE (:T {id: 'phantom', val: 0})").await?;
        assert_committed(tb.commit().await);
    }

    // T1 writes an existing row and commits. The phantom row was never in T1's
    // read-set (it didn't exist at read time), so there is no item conflict —
    // T1 commits despite the predicate population having changed underneath it.
    ta.execute("MATCH (n:T {id: 'x'}) SET n.val = 1").await?;
    assert_committed(ta.commit().await);
    Ok(())
}
