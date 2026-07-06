#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for crates/uni/src/api/transaction.rs:1044 (finding [3]).
//!
//! `commit()` wraps the ENTIRE `commit_transaction_l0` future in
//! `tokio::time::timeout(commit_timeout, ...)`. The durable commit point is
//! `flush_wal().await` (writer.rs), after which the main-L0 merge and SSI
//! registration run over synchronous locks. But the best-effort post-commit
//! auto-flush (`flush_inline_under_lock().await` when `async_flush_enabled` is
//! false) does real Lance L0→L1 I/O AFTER the durable point — and it is inside
//! the same timeout scope. If `commit_timeout` elapses during that flush, the
//! future is cancelled and `commit()` returns a retriable `CommitTimeout` with
//! the hint "your transaction is still active — retry", even though the
//! transaction is already durable and visible.
//!
//! FIXED: the `commit_timeout` now bounds ONLY the `flush_lock` acquisition
//! (inside `commit_transaction_l0_with_lock_timeout`); once the lock is held the
//! durable WAL flush and the inline post-commit flush run UNCANCELLED. So a
//! single-writer commit whose inline flush out-runs the tiny `commit_timeout`
//! still returns `Ok` (the lock is uncontended) instead of a false
//! `CommitTimeout` for an already-durable transaction. Deterministic now.

use std::time::Duration;

use tempfile::tempdir;
use uni_common::config::UniConfig;
use uni_db::{DataType, Uni, UniError};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_timeout_after_durable_point() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let cfg = UniConfig {
        // Force the inline (non-async) post-commit flush path, which does the
        // full L0→L1 Lance write inside the commit future.
        async_flush_enabled: false,
        // Any commit crosses the flush threshold → inline flush every commit.
        auto_flush_threshold: 1,
        auto_flush_min_mutations: 1,
        // Tiny budget: the inline Lance write out-runs it.
        commit_timeout: Duration::from_millis(3),
        ..UniConfig::default()
    };
    let db = Uni::open(dir.path().to_str().unwrap())
        .config(cfg)
        .build()
        .await?;
    db.schema()
        .label("Row")
        .property("i", DataType::Int64)
        .property("payload", DataType::String)
        .apply()
        .await?;

    let session = db.session();

    // Each commit's inline post-commit flush clearly exceeds the 3ms budget, but
    // the (uncontended) flush_lock is acquired instantly, so the commit runs to
    // completion and returns Ok — never a false CommitTimeout for the durable tx.
    for attempt in 0..8 {
        let base = attempt * 100_000i64;
        let tx = session.tx().await?;
        tx.execute(&format!(
            "UNWIND range({base}, {base} + 2999) AS n \
             CREATE (:Row {{i: n, payload: 'bulk-payload-string-to-make-the-flush-do-real-work'}})"
        ))
        .await?;

        let commit = tx.commit().await;
        assert!(
            !matches!(commit, Err(UniError::CommitTimeout { .. })),
            "attempt {attempt}: a single-writer commit must not report CommitTimeout \
             for its own slow post-durable flush; got {commit:?}"
        );
        commit.map_err(|e| anyhow::anyhow!("attempt {attempt}: commit failed: {e:?}"))?;

        // And the write is durable/visible.
        let res = session
            .query(&format!("MATCH (r:Row {{i: {base}}}) RETURN count(r) AS c"))
            .await?;
        let c: i64 = res.rows()[0].get("c")?;
        assert_eq!(c, 1, "attempt {attempt}: committed marker row must be visible");
    }

    Ok(())
}
