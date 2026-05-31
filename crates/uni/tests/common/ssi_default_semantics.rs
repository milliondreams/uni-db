// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Backward-compatibility guard for the `ssi_enabled = false` configuration.
//!
//! SSI/OCC now defaults on; this module opens every database with
//! `UniConfig::ssi_enabled = false` (see `db_xy`) and pins the legacy
//! last-writer-wins contract so a regression in the off path is caught:
//!
//! - concurrent writes use last-writer-wins and never raise a serialization
//!   conflict (no OCC when disabled),
//! - the workload that SSI would abort (write skew) instead commits both sides,
//!   proving the protection is exactly what `ssi_enabled` adds,
//! - `FOR UPDATE` parses and is an inert no-op (it acquires no lock; a
//!   `tracing::warn!` is emitted).

use std::sync::Arc;

use anyhow::Result;
use uni_db::{DataType, Uni, UniConfig, Value};

async fn db_xy() -> Result<Uni> {
    // This suite pins the last-writer-wins contract, so it must opt OUT of the
    // now-default SSI/OCC behavior explicitly.
    let config = UniConfig {
        ssi_enabled: false,
        ..Default::default()
    };
    let db = Uni::in_memory().config(config).build().await?;
    db.schema()
        .label("T")
        .property("id", DataType::String)
        .property("val", DataType::Int)
        .done()
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (:T {id: 'x', val: 1})").await?;
    tx.execute("CREATE (:T {id: 'y', val: 1})").await?;
    tx.commit().await?;
    Ok(db)
}

/// Without `ssi`, the write-skew interleaving that SSI aborts commits on both
/// sides — there is no commit-time conflict detection in this build.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_skew_not_prevented_without_ssi() -> Result<()> {
    let db = db_xy().await?;
    let (sa, sb) = (db.session(), db.session());
    let ta = sa.tx().await?;
    let tb = sb.tx().await?;

    ta.query("MATCH (n:T {id: 'x'}) RETURN n.val").await?;
    tb.query("MATCH (n:T {id: 'y'}) RETURN n.val").await?;
    ta.execute("MATCH (n:T {id: 'x'}) SET n.val = 0").await?;
    tb.execute("MATCH (n:T {id: 'y'}) SET n.val = 0").await?;

    // Both commit cleanly — the legacy (LWW) contract.
    ta.commit().await?;
    tb.commit().await?;
    Ok(())
}

/// Concurrent increments complete without any serialization conflict surfacing
/// (the retry helper still works, it just never needs to retry here).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_do_not_conflict_without_ssi() -> Result<()> {
    let db = Arc::new(db_xy().await?);
    let mut handles = Vec::new();
    for _ in 0..4 {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            db.session()
                .execute_with_retry("MATCH (n:T {id: 'x'}) SET n.val = n.val + 1")
                .await
        }));
    }
    for h in handles {
        h.await.expect("task panicked").expect("no conflict in the off build");
    }
    Ok(())
}

/// `FOR UPDATE` is accepted by the parser and is an inert no-op without `ssi`:
/// the query runs and a concurrent transaction is never blocked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn for_update_is_noop_without_ssi() -> Result<()> {
    let db = Arc::new(db_xy().await?);
    let s1 = db.session();
    let tx1 = s1.tx().await?;
    let r = tx1
        .query("MATCH (c:T {id: 'x'}) FOR UPDATE RETURN c.val AS v")
        .await?;
    assert_eq!(r.rows()[0].value("v"), Some(&Value::Int(1)));

    // No lock is taken in this build, so a second FOR UPDATE never blocks even
    // while tx1 is open.
    let db2 = db.clone();
    let handle = tokio::spawn(async move {
        let s2 = db2.session();
        let tx2 = s2.tx().await.unwrap();
        tx2.query("MATCH (c:T {id: 'x'}) FOR UPDATE RETURN c.val")
            .await
            .unwrap();
        tx2.commit().await.unwrap();
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), handle)
        .await
        .expect("FOR UPDATE must not block in the ssi-off build")
        .expect("tx2 task panicked");
    tx1.rollback();
    Ok(())
}
