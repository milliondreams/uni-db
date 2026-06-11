// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase E acceptance: `FOR UPDATE` pessimistic row locking (SSI escape hatch).

use std::sync::Arc;
use std::time::Duration;

use uni_db::{DataType, Uni, Value};

/// Reads the committed `n` for the `x` counter.
async fn counter_x(db: &Uni) -> anyhow::Result<i64> {
    let r = db
        .session()
        .query("MATCH (c:Counter {id: 'x'}) RETURN c.n AS n")
        .await?;
    match r.rows()[0].value("n") {
        Some(Value::Int(n)) => Ok(*n),
        other => panic!("expected Int, got {other:?}"),
    }
}

async fn seeded_db() -> anyhow::Result<Uni> {
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

/// A second transaction's `FOR UPDATE` on the same key blocks until the first
/// transaction holding the lock commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn for_update_serializes_concurrent_transactions() -> anyhow::Result<()> {
    let db = Arc::new(seeded_db().await?);

    // tx1 acquires the FOR UPDATE lock on id='x'.
    let s1 = db.session();
    let tx1 = s1.tx().await?;
    tx1.query("MATCH (c:Counter {id: 'x'}) FOR UPDATE RETURN c.n")
        .await?;

    // tx2 (separate session) tries to acquire the same lock — it must block.
    let db2 = db.clone();
    let handle = tokio::spawn(async move {
        let s2 = db2.session();
        let tx2 = s2.tx().await.unwrap();
        tx2.query("MATCH (c:Counter {id: 'x'}) FOR UPDATE RETURN c.n")
            .await
            .unwrap();
        tx2.commit().await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !handle.is_finished(),
        "tx2 should be blocked on tx1's FOR UPDATE lock"
    );

    // Releasing tx1's lock (commit drops the guard) lets tx2 proceed.
    tx1.commit().await?;
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("tx2 should acquire the lock after tx1 commits")
        .expect("tx2 task panicked");
    Ok(())
}

/// Regression for review (Tier 2): a `FOR UPDATE` issued through the
/// *parameterized builder* must take its row locks too. Previously the builder
/// path never called `acquire_for_update_locks`, so `tx.query(cypher, params)`
/// with `FOR UPDATE` silently lost its pessimistic-lock guarantee — exactly the
/// path the Python bindings use when params are supplied.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn for_update_via_parameterized_builder_acquires_lock() -> anyhow::Result<()> {
    let db = Arc::new(seeded_db().await?);

    // tx1 acquires the lock via the parameterized builder (key is `$id`).
    let s1 = db.session();
    let tx1 = s1.tx().await?;
    tx1.query_with("MATCH (c:Counter {id: $id}) FOR UPDATE RETURN c.n")
        .param("id", "x")
        .fetch_all()
        .await?;

    // tx2 attempts the same lock through the builder — it must block.
    let db2 = db.clone();
    let handle = tokio::spawn(async move {
        let s2 = db2.session();
        let tx2 = s2.tx().await.unwrap();
        tx2.query_with("MATCH (c:Counter {id: $id}) FOR UPDATE RETURN c.n")
            .param("id", "x")
            .fetch_all()
            .await
            .unwrap();
        tx2.commit().await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !handle.is_finished(),
        "parameterized FOR UPDATE must acquire the lock and block tx2"
    );

    tx1.commit().await?;
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("tx2 should acquire the lock after tx1 commits")
        .expect("tx2 task panicked");
    Ok(())
}

/// Sequential `FOR UPDATE` acquisitions on the same key all succeed — the lock
/// is released when each transaction commits.
#[tokio::test]
async fn for_update_lock_released_after_commit() -> anyhow::Result<()> {
    let db = seeded_db().await?;
    for _ in 0..3 {
        let session = db.session();
        let tx = session.tx().await?;
        tx.query("MATCH (c:Counter {id: 'x'}) FOR UPDATE RETURN c.n")
            .await?;
        tx.commit().await?;
    }
    Ok(())
}

/// The `FOR UPDATE` lock is released on rollback, not only on commit: a blocked
/// transaction proceeds once the holder rolls back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn for_update_lock_released_after_rollback() -> anyhow::Result<()> {
    let db = Arc::new(seeded_db().await?);
    let s1 = db.session();
    let tx1 = s1.tx().await?;
    tx1.query("MATCH (c:Counter {id: 'x'}) FOR UPDATE RETURN c.n")
        .await?;

    let db2 = db.clone();
    let handle = tokio::spawn(async move {
        let s2 = db2.session();
        let tx2 = s2.tx().await.unwrap();
        tx2.query("MATCH (c:Counter {id: 'x'}) FOR UPDATE RETURN c.n")
            .await
            .unwrap();
        tx2.commit().await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!handle.is_finished(), "tx2 should block on tx1's lock");

    // Roll back (not commit) — the lock must still be released.
    tx1.rollback();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("tx2 should proceed after tx1 rolls back")
        .expect("tx2 task panicked");
    Ok(())
}

async fn seeded_db_xy() -> anyhow::Result<Uni> {
    let db = seeded_db().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Counter {id: 'y', n: 0})").await?;
    tx.commit().await?;
    Ok(db)
}

/// Two transactions locking the same two keys in opposite request order must not
/// deadlock — keys are acquired in a canonical sorted order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn for_update_multi_key_opposite_order_no_deadlock() -> anyhow::Result<()> {
    let db = Arc::new(seeded_db_xy().await?);

    let run = |db: Arc<Uni>, q: &'static str| {
        tokio::spawn(async move {
            let s = db.session();
            let tx = s.tx().await.unwrap();
            tx.query(q).await.unwrap();
            // Hold both locks briefly to force the orders to interleave.
            tokio::time::sleep(Duration::from_millis(50)).await;
            tx.commit().await.unwrap();
        })
    };
    let h1 = run(
        db.clone(),
        "MATCH (a:Counter {id: 'x'}), (b:Counter {id: 'y'}) FOR UPDATE RETURN a.n",
    );
    let h2 = run(
        db.clone(),
        "MATCH (a:Counter {id: 'y'}), (b:Counter {id: 'x'}) FOR UPDATE RETURN a.n",
    );

    // With sorted acquisition there is no deadlock; both finish promptly.
    tokio::time::timeout(Duration::from_secs(10), async move {
        h1.await.expect("h1 panicked");
        h2.await.expect("h2 panicked");
    })
    .await
    .expect("multi-key FOR UPDATE deadlocked");
    Ok(())
}

/// An unsupported `FOR UPDATE` pattern (unlabeled node) acquires no lock, so it
/// does not block a concurrent transaction — the hint is a logged no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn for_update_unsupported_pattern_does_not_block() -> anyhow::Result<()> {
    let db = Arc::new(seeded_db().await?);
    let s1 = db.session();
    let tx1 = s1.tx().await?;
    // Unlabeled node → unsupported → no lock acquired (warns).
    tx1.query("MATCH (c {id: 'x'}) FOR UPDATE RETURN c.n")
        .await?;

    let db2 = db.clone();
    let handle = tokio::spawn(async move {
        let s2 = db2.session();
        let tx2 = s2.tx().await.unwrap();
        tx2.query("MATCH (c {id: 'x'}) FOR UPDATE RETURN c.n")
            .await
            .unwrap();
        tx2.commit().await.unwrap();
    });

    // No lock was taken, so tx2 must not block even while tx1 is open.
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("unsupported FOR UPDATE must not block")
        .expect("tx2 task panicked");
    tx1.rollback();
    Ok(())
}

// ── G5: lock-map does not leak ───────────────────────────────────────────────

/// The `FOR UPDATE` lock map holds an entry only while a transaction holds the
/// lock; it is pruned when the transaction ends. Before the G5 fix, every
/// distinct locked key accumulated a permanent entry.
#[tokio::test]
async fn for_update_lock_map_does_not_leak() -> anyhow::Result<()> {
    let db = seeded_db().await?;
    let writer = db.writer().expect("persistent db has a writer");
    assert_eq!(writer.for_update_lock_count(), 0, "map starts empty");

    {
        let s = db.session();
        let tx = s.tx().await?;
        tx.query("MATCH (c:Counter {id: 'x'}) FOR UPDATE RETURN c.n")
            .await?;
        assert_eq!(
            writer.for_update_lock_count(),
            1,
            "entry present while the lock is held"
        );
        tx.commit().await?;
    }
    assert_eq!(
        writer.for_update_lock_count(),
        0,
        "entry pruned when the holder commits (G5)"
    );

    // Many sequential transactions on the same key never accumulate entries.
    for _ in 0..10 {
        let s = db.session();
        let tx = s.tx().await?;
        tx.query("MATCH (c:Counter {id: 'x'}) FOR UPDATE RETURN c.n")
            .await?;
        tx.commit().await?;
    }
    assert_eq!(
        writer.for_update_lock_count(),
        0,
        "lock map leaked across completed transactions (G5)"
    );
    Ok(())
}

// ── G6: a contended lock surfaces a retriable LockTimeout ─────────────────────

/// A `FOR UPDATE` acquisition that cannot complete within the bound surfaces
/// `UniError::LockTimeout`, which `is_retriable()` — so `transact_with_retry`
/// re-runs and wins once the holder releases. Ignored by default because it must
/// wait out the 10s acquisition bound to trigger the timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "G6: takes >10s — must wait out the FOR UPDATE acquisition bound"]
async fn for_update_lock_timeout_is_retriable_and_recovers() -> anyhow::Result<()> {
    use uni_db::{RetryOptions, UniError};

    let db = Arc::new(seeded_db().await?);

    // Holder grabs the lock and keeps it for ~12s (past the 10s acquire bound),
    // then releases — so the contender's first acquire times out (LockTimeout)
    // and its retry succeeds.
    let holder_db = db.clone();
    let holder = tokio::spawn(async move {
        let s = holder_db.session();
        let tx = s.tx().await.unwrap();
        tx.query("MATCH (c:Counter {id: 'x'}) FOR UPDATE RETURN c.n")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(12_000)).await;
        tx.commit().await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Sanity: a plain (non-retrying) acquire returns the retriable LockTimeout.
    let probe_db = db.clone();
    let err = {
        let s = probe_db.session();
        let tx = s.tx().await?;
        tx.query("MATCH (c:Counter {id: 'x'}) FOR UPDATE RETURN c.n")
            .await
            .expect_err("should time out while the holder keeps the lock")
    };
    assert!(matches!(err, UniError::LockTimeout { .. }), "got {err:?}");
    assert!(err.is_retriable(), "LockTimeout must be retriable");

    // With retry, the contender eventually wins after the holder releases.
    db.session()
        .transact_with_retry(
            RetryOptions {
                max_attempts: 5,
                ..Default::default()
            },
            |tx| {
                Box::pin(async move {
                    tx.query("MATCH (c:Counter {id: 'x'}) FOR UPDATE RETURN c.n")
                        .await?;
                    Ok(())
                })
            },
        )
        .await?;

    holder.await.expect("holder task panicked");
    Ok(())
}

// ── Read-latest semantics (FOR UPDATE on a fresh transaction) ────────────────

/// The headline new guarantee: concurrent `FOR UPDATE` read-modify-writes with
/// NO retry all commit and converge exactly. Acquiring the lock on a fresh
/// transaction re-pins its snapshot to lock-acquisition time, so each writer
/// reads the latest committed value under the lock and never conflicts.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn for_update_rmw_no_retry_converges() -> anyhow::Result<()> {
    const WRITERS: i64 = 8;
    let db = Arc::new(seeded_db().await?);
    let mut handles = Vec::new();
    for _ in 0..WRITERS {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            // No retry: a conflict here would surface as an Err and fail the test.
            let s = db.session();
            let tx = s.tx().await?;
            tx.query("MATCH (c:Counter {id: 'x'}) FOR UPDATE RETURN c.n")
                .await?;
            tx.execute("MATCH (c:Counter {id: 'x'}) SET c.n = c.n + 1")
                .await?;
            tx.commit().await.map(|_| ())
        }));
    }
    for h in handles {
        // `?` propagates any SerializationConflict — there must be none.
        h.await.expect("task panicked")?;
    }
    assert_eq!(
        counter_x(&db).await?,
        WRITERS,
        "FOR UPDATE RMW must converge without retry"
    );
    Ok(())
}

/// A `FOR UPDATE` read on a fresh transaction sees the LATEST committed value,
/// not the transaction's begin snapshot — even when another transaction
/// committed after this one began (but before it acquired the lock).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn for_update_read_sees_latest_committed() -> anyhow::Result<()> {
    let db = seeded_db().await?; // x.n = 0

    // T2 begins (pins a snapshot at n=0) but does NOT read yet — stays fresh.
    let s2 = db.session();
    let t2 = s2.tx().await?;

    // A separate transaction commits n=100 while T2 is open.
    {
        let s1 = db.session();
        let t1 = s1.tx().await?;
        t1.query("MATCH (c:Counter {id: 'x'}) FOR UPDATE RETURN c.n")
            .await?;
        t1.execute("MATCH (c:Counter {id: 'x'}) SET c.n = 100")
            .await?;
        t1.commit().await?;
    }

    // T2's first op is a FOR UPDATE read: re-pin makes it observe 100, not 0.
    let r = t2
        .query("MATCH (c:Counter {id: 'x'}) FOR UPDATE RETURN c.n AS n")
        .await?;
    assert_eq!(
        r.rows()[0].value("n"),
        Some(&Value::Int(100)),
        "FOR UPDATE on a fresh tx must read the latest committed value"
    );
    t2.rollback();
    Ok(())
}

/// Documented limitation / fallback: if a transaction READS before `FOR UPDATE`,
/// the re-pin is suppressed (a single per-tx read sequence cannot keep the
/// earlier read at its begin basis), so the FOR UPDATE read keeps the begin
/// snapshot and the RMW still needs retry — today's behaviour, preserved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_before_for_update_keeps_begin_snapshot() -> anyhow::Result<()> {
    let db = seeded_db().await?; // x.n = 0

    let s2 = db.session();
    let t2 = s2.tx().await?;
    // A prior read makes T2 non-fresh — no re-pin on the later FOR UPDATE.
    t2.query("MATCH (c:Counter {id: 'x'}) RETURN c.n").await?;

    {
        let s1 = db.session();
        let t1 = s1.tx().await?;
        t1.execute("MATCH (c:Counter {id: 'x'}) SET c.n = 100")
            .await?;
        t1.commit().await?;
    }

    // FOR UPDATE after the prior read still sees the begin snapshot (0).
    let r = t2
        .query("MATCH (c:Counter {id: 'x'}) FOR UPDATE RETURN c.n AS n")
        .await?;
    assert_eq!(
        r.rows()[0].value("n"),
        Some(&Value::Int(0)),
        "after a prior read, FOR UPDATE keeps the begin snapshot (fallback)"
    );
    t2.rollback();
    Ok(())
}
