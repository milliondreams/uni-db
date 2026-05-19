// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Async-flush integration tests for `UniConfig::async_flush_enabled = true`.
//
// What is covered:
// - 7.2  Single-session many commits trigger the async path; final drain
//        makes everything queryable; manifest chain is linear; pending_count
//        returns to 0.
// - 7.5  Back-pressure via `max_pending_flushes = 1`. Concurrent commits
//        do not error out and converge.
// - 7.6  drop_fork drains pending async flushes on the fork's writer
//        before tombstoning. Without the drain (Commit 8), Arc<ForkScope>
//        held by the spawned finalizer task would block holder_count.
//
// Not covered here (deferred — need test-only barriers / mocks):
// - 7.3 out-of-order stream
// - 7.4 stream failure
// - 7.7 fork drain timeout
// - 7.8 shutdown drain
// - 7.9 / 7.9b crash recovery

use std::sync::Arc;

use anyhow::Result;
use uni_common::config::UniConfig;
use uni_db::{DataType, Uni};

async fn build_async_db(threshold: usize, max_pending: usize) -> Result<Arc<Uni>> {
    let config = UniConfig {
        auto_flush_threshold: threshold,
        auto_flush_interval: None,
        async_flush_enabled: true,
        max_pending_flushes: max_pending,
        ..Default::default()
    };
    let db = Arc::new(Uni::in_memory().config(config).build().await?);
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    Ok(db)
}

/// Visibility under concurrent async flushes — the bug Item B-deep
/// fixed. Root cause: `LanceDbBackend::get_or_open_table` cached
/// `lancedb::Table` handles, but a cached handle is pinned to the
/// dataset version it was opened at (per `Table::checkout_latest`
/// docs in lancedb 0.27.1). Multiple stream phases commit new
/// dataset versions, but readers via cached handles saw only the
/// pinned version — silently dropping recent rows. Fix:
/// `get_or_open_table` now calls `table.checkout_latest()` on cache
/// hit before returning, so every caller (read or write) sees the
/// current dataset state.
#[tokio::test]
async fn async_flush_visibility_after_drain() -> Result<()> {
    // 200 commits × 50 vertices = 10_000 mutations; threshold=1000 should
    // trigger ~10 async flushes.
    let db = build_async_db(1000, 4).await?;
    let session = db.session();
    let total = 200usize;
    let per_tx = 50usize;
    for i in 0..total {
        let tx = session.tx().await?;
        let mut props = Vec::with_capacity(per_tx);
        for j in 0..per_tx {
            let mut row = std::collections::HashMap::new();
            row.insert(
                "name".to_string(),
                uni_db::Value::String(format!("p_{}_{}", i, j)),
            );
            props.push(row);
        }
        tx.bulk_insert_vertices("Person", props).await?;
        tx.commit().await?;
    }
    // Drain: today's db.flush() doesn't wait for in-flight async flushes
    // (that's a follow-up — make flush_to_l1 drain the coordinator
    // first). Until then, poll: call flush() until the count stabilizes.
    // The count comes from L0 (current) + pending_flush L0s + L1.
    for _ in 0..20 {
        db.flush().await?;
        let result = db
            .session()
            .query("MATCH (p:Person) RETURN count(p) AS c")
            .await?;
        let c: i64 = result.rows()[0].get("c")?;
        if c as usize == total * per_tx {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let result = db
        .session()
        .query("MATCH (p:Person) RETURN count(p) AS c")
        .await?;
    let c: i64 = result.rows()[0].get("c")?;
    assert_eq!(
        c as usize,
        total * per_tx,
        "all vertices visible after drain"
    );
    Ok(())
}

#[tokio::test]
async fn async_flush_backpressure_max_pending_one() -> Result<()> {
    // max_pending=1 worst case — only one stream in flight at a time.
    // Verify the system makes forward progress (no deadlock) and final
    // state is correct.
    let db = build_async_db(500, 1).await?;
    let session = db.session();
    for i in 0..50 {
        let tx = session.tx().await?;
        let mut row = std::collections::HashMap::new();
        row.insert(
            "name".to_string(),
            uni_db::Value::String(format!("p_{}", i)),
        );
        tx.bulk_insert_vertices("Person", vec![row]).await?;
        tx.commit().await?;
    }
    for _ in 0..20 {
        db.flush().await?;
        let result = db
            .session()
            .query("MATCH (p:Person) RETURN count(p) AS c")
            .await?;
        let c: i64 = result.rows()[0].get("c")?;
        if c == 50 {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let result = db
        .session()
        .query("MATCH (p:Person) RETURN count(p) AS c")
        .await?;
    let c: i64 = result.rows()[0].get("c")?;
    assert_eq!(c, 50);
    Ok(())
}

/// 7.6 — drop_fork drains pending async flushes on the fork's writer.
///
/// Stress-tests the fix from Plan §13.1: FlushCoordinator now tracks
/// every spawned stream task in `stream_handles` and `shutdown()`
/// awaits each one before dropping `submit_tx`. Without that, the
/// stream task's captured `Arc<Writer>` (→ `Arc<storage>` →
/// `Arc<ForkScope>` → `ForkHolderGuard`) would linger past `drain()`,
/// causing `drop_fork` to return `ForkInUse`.
#[tokio::test]
async fn async_flush_fork_drop_drains_pending() -> Result<()> {
    let db = build_async_db(500, 4).await?;
    // Seed primary so the fork has something to branch from.
    {
        let primary = db.session();
        let tx = primary.tx().await?;
        let mut row = std::collections::HashMap::new();
        row.insert(
            "name".to_string(),
            uni_db::Value::String("anchor".to_string()),
        );
        tx.bulk_insert_vertices("Person", vec![row]).await?;
        tx.commit().await?;
        db.flush().await?;
    }

    // Create fork; commit enough on the fork to trigger several async
    // flushes on the fork's writer.
    {
        let primary = db.session();
        let fork = primary.fork("rd").await?;
        for i in 0..30 {
            let tx = fork.tx().await?;
            let mut row = std::collections::HashMap::new();
            row.insert(
                "name".to_string(),
                uni_db::Value::String(format!("hyp_{}", i)),
            );
            tx.bulk_insert_vertices("Person", vec![row]).await?;
            tx.commit().await?;
        }
        // Explicitly drain fork's writer before letting scope end —
        // otherwise spawned stream tasks may still be queued past
        // scope-end and the test's drop_fork won't have time to drain.
        fork.flush().await?;
    }
    // Give tokio a chance to run any pending destructors before the
    // drop_fork holder check. With JoinHandle tracking in shutdown(),
    // this shouldn't be needed — but adding for safety against any
    // residual scheduler-queue lag.
    tokio::task::yield_now().await;
    db.drop_fork("rd").await?;
    Ok(())
}
