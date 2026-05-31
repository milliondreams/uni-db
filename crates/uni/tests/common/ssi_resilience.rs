// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Resilience: crash / recovery / abort-cleanup for SSI.
//!
//! These validate the durability boundary the design stakes its correctness on —
//! *validation happens before the WAL is touched, and the WAL flush is the commit
//! point* — end-to-end through a real close-and-reopen (the WAL replays from
//! disk), plus the abort-cleanup invariants (no leaked locks, pins, or registry
//! entries).
//!
//! The crash-injection tests (gated behind the `failpoints` feature) drive a
//! commit to panic at a precise seam (`commit::after-validate` /
//! `after-wal-flush` / `after-merge`), then reopen and assert **atomicity**: the
//! recovered value is all-or-nothing (`0` or the written value), never a partial
//! or corrupt state, and the database stays usable. A crash before the WAL touch
//! must recover NOTHING (no resurrection). Because the commit panicked before
//! `commit()` returned, the transaction was never acknowledged to the caller, so
//! whether a mid-commit crash recovers the value is unspecified — only that it is
//! atomic. Run with `--features ssi,failpoints`. Each test owns its failpoint and
//! runs in its own process under nextest, so the global registry does not bleed.

#[cfg(feature = "failpoints")]
use std::sync::Arc;

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

use crate::ssi_support::reopen::DiskHarness;
use crate::ssi_support::schedule::{assert_committed, assert_serialization_conflict};

/// Sets up the `C(id, n)` schema and seeds `x = 0` on a freshly-opened db.
async fn init_schema_and_seed(db: &Uni) -> Result<()> {
    db.schema()
        .label("C")
        .property("id", DataType::String)
        .property("n", DataType::Int)
        .done()
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (:C {id: 'x', n: 0})").await?;
    tx.commit().await?;
    Ok(())
}

async fn read_n(db: &Uni) -> Result<i64> {
    let r = db
        .session()
        .query("MATCH (c:C {id: 'x'}) RETURN c.n AS n")
        .await?;
    match r.rows()[0].value("n") {
        Some(Value::Int(n)) => Ok(*n),
        other => panic!("expected Int, got {other:?}"),
    }
}

// ── Reopen / recovery (no fault injection) ───────────────────────────────────

/// Baseline: a committed write survives close-and-reopen (WAL replay).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_write_survives_reopen() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let db = h.open().await?;
        init_schema_and_seed(&db).await?;
        let s = db.session();
        let tx = s.tx().await?;
        tx.execute("MATCH (c:C {id: 'x'}) SET c.n = 5").await?;
        tx.commit().await?;
        db.flush().await?;
    }
    let db = h.open().await?;
    assert_eq!(read_n(&db).await?, 5, "committed write lost across reopen");
    Ok(())
}

/// The central correctness claim, end-to-end: a transaction aborted by SSI
/// validation leaves NO trace after a real reopen — its mutations never reached
/// the WAL (validation runs before the WAL append). The winner's write persists.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validation_aborted_tx_leaves_no_trace_through_reopen() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let db = h.open().await?;
        init_schema_and_seed(&db).await?;

        let (sa, sb) = (db.session(), db.session());
        let ta = sa.tx().await?;
        let tb = sb.tx().await?; // snapshot before ta commits

        ta.execute("MATCH (c:C {id: 'x'}) SET c.n = 1").await?;
        tb.execute("MATCH (c:C {id: 'x'}) SET c.n = 2").await?;

        assert_committed(ta.commit().await); // winner
        assert_serialization_conflict(tb.commit().await); // loser aborts

        db.flush().await?;
    }
    // Reopen: only the winner (n = 1) is durable; the aborted writer's n = 2
    // never touched the WAL, so it cannot resurrect on replay.
    let db = h.open().await?;
    assert_eq!(
        read_n(&db).await?,
        1,
        "aborted transaction resurrected after reopen"
    );
    Ok(())
}

/// After a reopen the in-memory commit registry is empty and conflict detection
/// resumes correctly: a fresh pair of concurrent transactions still conflicts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflict_detection_resumes_after_reopen() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let db = h.open().await?;
        init_schema_and_seed(&db).await?;
        db.flush().await?;
    }
    let db = h.open().await?;
    let (sa, sb) = (db.session(), db.session());
    let ta = sa.tx().await?;
    let tb = sb.tx().await?;
    ta.execute("MATCH (c:C {id: 'x'}) SET c.n = 1").await?;
    tb.execute("MATCH (c:C {id: 'x'}) SET c.n = 2").await?;
    assert_committed(ta.commit().await);
    assert_serialization_conflict(tb.commit().await);
    Ok(())
}

// ── Abort cleanup ────────────────────────────────────────────────────────────

/// An aborted commit leaves no residue: it does not freeze a generation, the
/// FOR UPDATE lock map stays empty, and the database keeps accepting commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_leaves_no_residue() -> Result<()> {
    let h = DiskHarness::new()?;
    let db = h.open().await?;
    init_schema_and_seed(&db).await?;
    let writer = db.writer().expect("disk db has a writer");

    let (sa, sb) = (db.session(), db.session());
    let ta = sa.tx().await?;
    let tb = sb.tx().await?;
    ta.execute("MATCH (c:C {id: 'x'}) SET c.n = 1").await?;
    tb.execute("MATCH (c:C {id: 'x'}) SET c.n = 2").await?;
    assert_committed(ta.commit().await);
    assert_serialization_conflict(tb.commit().await);

    // The aborted transaction leaves no FOR UPDATE lock entries behind...
    assert_eq!(
        writer.for_update_lock_count(),
        0,
        "an abort must not leak FOR UPDATE lock entries"
    );
    // ...and the database is unharmed: a subsequent commit succeeds and the
    // value reflects only the winner plus the new write.
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("MATCH (c:C {id: 'x'}) SET c.n = 9").await?;
    assert_committed(tx.commit().await);
    assert_eq!(read_n(&db).await?, 9);
    Ok(())
}

/// A transaction older than the retained commit history aborts conservatively
/// with a (retriable) serialization conflict rather than silently missing a
/// possible conflict. Ignored by default: it commits 4097+ transactions to push
/// the long-running reader past the 4096-entry registry capacity.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "slow: commits >4096 transactions to exceed the OCC registry capacity"]
async fn long_transaction_past_registry_capacity_aborts() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    init_schema_and_seed(&db).await?;
    {
        let s = db.session();
        let tx = s.tx().await?;
        tx.execute("CREATE (:C {id: 'long', n: 0})").await?;
        tx.commit().await?;
    }

    // A long-running reader pins an old read sequence.
    let s_long = db.session();
    let long = s_long.tx().await?;
    long.query("MATCH (c:C {id: 'long'}) RETURN c.n").await?;

    // Churn past the registry capacity with disjoint committed writes.
    for i in 0..4097 {
        let s = db.session();
        let tx = s.tx().await?;
        tx.execute(&format!("CREATE (:C {{id: 'churn{i}', n: 0}})"))
            .await?;
        tx.commit().await?;
    }

    // The long reader now writes and commits: its read sequence predates the
    // retained history, so it must abort conservatively.
    long.execute("MATCH (c:C {id: 'long'}) SET c.n = 1").await?;
    assert_serialization_conflict(long.commit().await);
    Ok(())
}

// ── Crash-mid-commit atomicity (requires `failpoints`) ───────────────────────

/// Helper: run a `SET c.n = <val>` commit that is expected to panic at the
/// configured failpoint. Returns once the panicking task has been joined.
#[cfg(feature = "failpoints")]
async fn commit_that_crashes(db: Arc<Uni>, val: i64) {
    let res = tokio::spawn(async move {
        let s = db.session();
        let tx = s.tx().await.unwrap();
        tx.execute(&format!("MATCH (c:C {{id: 'x'}}) SET c.n = {val}"))
            .await
            .unwrap();
        tx.commit().await
    })
    .await;
    assert!(res.is_err(), "commit task should have panicked at the failpoint");
}

/// After a mid-commit crash + reopen, the value is atomic (`0` or `val`, never a
/// partial state) and the database still accepts new writes. Usability is probed
/// with a *fresh* node (not the recovered one) to avoid any WAL-replay node
/// identity ambiguity for `x`.
#[cfg(feature = "failpoints")]
async fn assert_atomic_and_usable(db: &Uni, val: i64) -> Result<()> {
    let recovered = read_n(db).await?;
    assert!(
        recovered == 0 || recovered == val,
        "non-atomic recovery: n = {recovered} (expected 0 or {val})"
    );
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (:C {id: 'probe', n: 1})").await?;
    assert_committed(tx.commit().await);
    let r = db
        .session()
        .query("MATCH (c:C {id: 'probe'}) RETURN c.n AS n")
        .await?;
    assert_eq!(
        r.rows()[0].value("n"),
        Some(&Value::Int(1)),
        "database unusable after crash recovery"
    );
    Ok(())
}

/// A crash AFTER validation but BEFORE the WAL append recovers nothing: the
/// transaction never became durable.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_after_validate_recovers_nothing() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let db = h.open().await?;
        init_schema_and_seed(&db).await?;
        db.flush().await?;
    }
    {
        let db = Arc::new(h.open().await?);
        fail::cfg("commit::after-validate", "panic").unwrap();
        commit_that_crashes(db.clone(), 42).await;
        fail::remove("commit::after-validate");
        drop(db);
    }
    let db = h.open().await?;
    assert_eq!(
        read_n(&db).await?,
        0,
        "a crash before the WAL flush must leave no trace"
    );
    Ok(())
}

/// A crash AFTER the WAL flush but before the L0 merge is atomic on reopen — the
/// transaction was not acknowledged, so it recovers wholly or not at all, never
/// partially.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_after_wal_flush_is_atomic() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let db = h.open().await?;
        init_schema_and_seed(&db).await?;
        db.flush().await?;
    }
    {
        let db = Arc::new(h.open().await?);
        fail::cfg("commit::after-wal-flush", "panic").unwrap();
        commit_that_crashes(db.clone(), 7).await;
        fail::remove("commit::after-wal-flush");
        drop(db);
    }
    let db = h.open().await?;
    assert_atomic_and_usable(&db, 7).await
}

/// A crash AFTER the L0 merge but before the in-memory registry record is also
/// atomic on reopen (the registry is rebuilt empty, so it plays no part in
/// durability).
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_after_merge_is_atomic() -> Result<()> {
    let h = DiskHarness::new()?;
    {
        let db = h.open().await?;
        init_schema_and_seed(&db).await?;
        db.flush().await?;
    }
    {
        let db = Arc::new(h.open().await?);
        fail::cfg("commit::after-merge", "panic").unwrap();
        commit_that_crashes(db.clone(), 3).await;
        fail::remove("commit::after-merge");
        drop(db);
    }
    let db = h.open().await?;
    assert_atomic_and_usable(&db, 3).await
}
