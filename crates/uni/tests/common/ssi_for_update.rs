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
