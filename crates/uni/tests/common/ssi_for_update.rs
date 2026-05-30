// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase E acceptance: `FOR UPDATE` pessimistic row locking (SSI escape hatch).
//!
//! See `docs/proposals/serializable_snapshot_isolation.md` (Component C5).

use std::sync::Arc;
use std::time::Duration;

use uni_db::{DataType, Uni};

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
