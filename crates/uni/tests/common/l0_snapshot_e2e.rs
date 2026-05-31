// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end tests for L0 snapshot-isolation reads (Component C1, item "F").
//!
//! A read-write transaction pins an L0 snapshot at begin; its reads see that
//! frozen view for its lifetime, isolated from concurrent commits (lazy
//! clone-on-freeze in `commit_transaction_l0`). Read-your-writes is preserved
//! because the transaction's private `tx_l0` stays live over the snapshot. See
//! `docs/proposals/serializable_snapshot_isolation.md` (Component C1 / item F).
//!
//! Gated on `l0-snapshot` (enabled transitively by `ssi`), so these run under
//! both the snapshot-only and the full SSI builds.

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// A `Counter` schema seeded with `{id: 'x', n: 0}`.
async fn counter_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Counter")
        .property("id", DataType::String)
        .property("n", DataType::Int)
        .done()
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Counter {id: 'x', n: 0})").await?;
    tx.commit().await?;
    Ok(db)
}

/// Reads `n` for the `x` counter within transaction `tx` (uses the tx's snapshot).
async fn read_n(tx: &uni_db::Transaction) -> Result<i64> {
    let r = tx
        .query("MATCH (c:Counter {id: 'x'}) RETURN c.n AS n")
        .await?;
    match r.rows()[0].value("n") {
        Some(Value::Int(n)) => Ok(*n),
        other => panic!("expected Int, got {other:?}"),
    }
}

/// Reads the committed `n` via a fresh read-only session query (live view).
async fn committed_n(db: &Uni) -> Result<i64> {
    let r = db
        .session()
        .query("MATCH (c:Counter {id: 'x'}) RETURN c.n AS n")
        .await?;
    match r.rows()[0].value("n") {
        Some(Value::Int(n)) => Ok(*n),
        other => panic!("expected Int, got {other:?}"),
    }
}

/// Core (T1): a transaction's reads are isolated from a concurrent commit. The
/// reader pins a snapshot at begin; a concurrent writer commits `n = 1` (which
/// freezes the pinned generation aside); the reader still sees `n = 0`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_isolates_tx_reads_from_concurrent_commit() -> Result<()> {
    let db = counter_db().await?;

    let s_r = db.session();
    let tx_r = s_r.tx().await?;
    assert_eq!(read_n(&tx_r).await?, 0, "snapshot sees the seed value");

    // A concurrent transaction sets n = 1 and commits — pinned ⇒ clone-on-freeze.
    {
        let s_w = db.session();
        let tx_w = s_w.tx().await?;
        tx_w.execute("MATCH (c:Counter {id: 'x'}) SET c.n = 1").await?;
        tx_w.commit().await?;
    }

    // The reader still observes the begin-time value via its pinned snapshot.
    assert_eq!(
        read_n(&tx_r).await?,
        0,
        "reads are isolated from the later commit"
    );
    tx_r.rollback();

    // A fresh read observes the committed value.
    assert_eq!(committed_n(&db).await?, 1);
    Ok(())
}

/// T3: read-your-writes is preserved — a transaction sees its own uncommitted
/// write (via the live `tx_l0`) even though reads otherwise resolve against the
/// pinned snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_preserves_read_your_writes() -> Result<()> {
    let db = counter_db().await?;
    let s = db.session();
    let tx = s.tx().await?;

    tx.execute("MATCH (c:Counter {id: 'x'}) SET c.n = 42").await?;
    assert_eq!(
        read_n(&tx).await?,
        42,
        "the tx sees its own uncommitted write over the snapshot"
    );

    tx.commit().await?;
    assert_eq!(committed_n(&db).await?, 42);
    Ok(())
}

/// The reader's snapshot stays stable across MANY concurrent commits — the
/// generation is frozen once (first commit after the pin); later commits land in
/// the post-freeze generation, invisible to the reader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_stable_across_many_commits() -> Result<()> {
    let db = counter_db().await?;
    let s_r = db.session();
    let tx_r = s_r.tx().await?;
    assert_eq!(read_n(&tx_r).await?, 0);

    for i in 1..=8 {
        let s_w = db.session();
        let tx_w = s_w.tx().await?;
        tx_w.execute(&format!("MATCH (c:Counter {{id: 'x'}}) SET c.n = {i}"))
            .await?;
        tx_w.commit().await?;
        assert_eq!(read_n(&tx_r).await?, 0, "reader stays at the pinned value");
    }
    tx_r.rollback();
    assert_eq!(committed_n(&db).await?, 8);
    Ok(())
}

/// Two readers pinned at different points each see their own consistent view: a
/// reader pinned before a commit sees the old value; one pinned after sees the new.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshots_pinned_at_different_points_are_independent() -> Result<()> {
    let db = counter_db().await?;

    let s_early = db.session();
    let tx_early = s_early.tx().await?; // pins at n = 0

    {
        let s_w = db.session();
        let tx_w = s_w.tx().await?;
        tx_w.execute("MATCH (c:Counter {id: 'x'}) SET c.n = 7").await?;
        tx_w.commit().await?;
    }

    let s_late = db.session();
    let tx_late = s_late.tx().await?; // pins at n = 7

    assert_eq!(read_n(&tx_early).await?, 0, "early snapshot sees old value");
    assert_eq!(read_n(&tx_late).await?, 7, "late snapshot sees new value");

    tx_early.rollback();
    tx_late.rollback();
    Ok(())
}

// ── Self-pin regression guard ────────────────────────────────────────────────
// The commit-time freeze increments `uni_l0_snapshot_freezes_total`. A minimal
// global recorder captures it: an *uncontended* RW commit must record ZERO
// freezes (the committing transaction releases its own pin first), while a commit
// under a *concurrent reader* must still freeze. This pins the fix that a
// transaction's own pin no longer forces a deep clone of the main L0 per commit.

static FREEZES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[derive(Debug)]
struct FreezeCounter;
impl metrics::CounterFn for FreezeCounter {
    fn increment(&self, value: u64) {
        FREEZES.fetch_add(value, std::sync::atomic::Ordering::Relaxed);
    }
    fn absolute(&self, value: u64) {
        FREEZES.store(value, std::sync::atomic::Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct FreezeRecorder;
impl metrics::Recorder for FreezeRecorder {
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        if key.name() == "uni_l0_snapshot_freezes_total" {
            metrics::Counter::from_arc(std::sync::Arc::new(FreezeCounter))
        } else {
            metrics::Counter::noop()
        }
    }
    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }
    fn register_histogram(
        &self,
        _: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

/// Uncontended commit ⇒ no freeze; commit under a concurrent reader ⇒ freeze.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn freeze_fires_only_under_contention() -> Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    assert!(
        metrics::set_global_recorder(FreezeRecorder).is_ok(),
        "a metrics recorder was already installed in this process"
    );

    let db = counter_db().await?;

    // (1) Uncontended: a single RW transaction, nothing else open → no freeze.
    let base = FREEZES.load(Relaxed);
    {
        let s = db.session();
        let tx = s.tx().await?;
        tx.execute("MATCH (c:Counter {id: 'x'}) SET c.n = 1").await?;
        tx.commit().await?;
    }
    assert_eq!(
        FREEZES.load(Relaxed) - base,
        0,
        "an uncontended commit must not freeze (the tx releases its own pin)"
    );

    // (2) Contended: a concurrent reader pins a snapshot while a writer commits.
    let base = FREEZES.load(Relaxed);
    {
        let s_r = db.session();
        let tx_r = s_r.tx().await?;
        tx_r.query("MATCH (c:Counter {id: 'x'}) RETURN c.n").await?;
        {
            let s_w = db.session();
            let tx_w = s_w.tx().await?;
            tx_w.execute("MATCH (c:Counter {id: 'x'}) SET c.n = 2").await?;
            tx_w.commit().await?;
        }
        tx_r.rollback();
    }
    assert!(
        FREEZES.load(Relaxed) - base >= 1,
        "a commit under a concurrent reader must freeze to isolate it"
    );
    Ok(())
}
