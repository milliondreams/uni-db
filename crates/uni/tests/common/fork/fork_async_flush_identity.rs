// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A fork's **async** flush must finalize under the fork's own identity.
//!
//! `Writer::new_with_config` captures the `SharedFlushCtx` that the
//! `FlushCoordinator` moves into its finalizer loop. That capture used to
//! hard-code `fork_id: None`, and the fork identity was patched onto the writer
//! only *after* the constructor returned — so the coordinator finalized every
//! async flush believing it was primary's writer, while holding fork-scoped
//! storage. `flush_finalize_body` asserts the two agree
//! (`writer.rs`, "fork writer must publish to a fork-scoped snapshot
//! namespace"), and `fsync_snapshot_pointer` takes the same `fork_id` — so past
//! the debug assertion, in a release build, a fork's async flush fsynced
//! *primary's* snapshot pointer instead of `catalog/forks/{id}/latest`.
//!
//! The sync path was never affected: it rebuilds its context per call from
//! `Writer::shared_ctx()`, which reads the writer's real `fork_id`.
//!
//! `fork_create_concurrent_writes.rs` catches this, but only when the async
//! finalize happens to win a race — it went three months without firing, then
//! failed CI twice in one PR with two different symptoms (a 30s timeout and the
//! assertion). This test removes the race: `auto_flush_threshold: 1` with
//! `async_flush_enabled` makes the fork's own writer take the coordinator path
//! on ordinary commits, with no competing writer at all.

// Rust guideline compliant

use std::time::Duration;

use uni_db::{DataType, Uni, UniConfig};

/// Commits several batches on a fork with async flush armed, then reads them
/// back. Pre-fix this panicked inside the finalizer loop on the fork/primary
/// identity mismatch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fork_async_flush_finalizes_under_fork_identity() {
    let dir = tempfile::tempdir().unwrap();

    // Threshold 1: every commit crosses it, so the fork writer routes through
    // the FlushCoordinator rather than the sync path. No racing writer is
    // needed — the defect is in construction order, not in concurrency.
    let config = UniConfig {
        auto_flush_threshold: 1,
        async_flush_enabled: true,
        commit_timeout: Duration::from_secs(120),
        ..Default::default()
    };

    let db = Uni::open(dir.path().to_str().unwrap())
        .config(config)
        .build()
        .await
        .unwrap();
    db.schema()
        .label("Item")
        .property("kind", DataType::String)
        .apply()
        .await
        .unwrap();

    // A base row so `vertices_Item` exists on disk at fork-point, and a
    // primary flush so the fork branches from a published snapshot.
    let session = db.session();
    let tx = session.tx().await.unwrap();
    tx.execute("CREATE (:Item {kind: 'base'})").await.unwrap();
    tx.commit().await.unwrap();
    db.flush().await.unwrap();

    let forked = session.fork("async_identity").await.unwrap();

    // Each of these crosses the threshold on the FORK's writer, so each one
    // finalizes through the coordinator under the captured context.
    for i in 0..8 {
        let tx = forked.tx().await.unwrap();
        tx.execute(&format!("CREATE (:Item {{kind: 'fork_{i}'}})"))
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    forked.flush().await.unwrap();

    // Every fork write must be readable, and the primary must not have seen
    // them — which is what a manifest published under the wrong identity would
    // put at risk.
    let rows = forked
        .query("MATCH (n:Item) WHERE n.kind STARTS WITH 'fork_' RETURN n.kind AS kind")
        .await
        .unwrap();
    assert_eq!(
        rows.rows().len(),
        8,
        "fork lost writes across its async flushes"
    );

    let primary = session
        .query("MATCH (n:Item) WHERE n.kind STARTS WITH 'fork_' RETURN n.kind AS kind")
        .await
        .unwrap();
    assert_eq!(
        primary.rows().len(),
        0,
        "fork writes leaked into primary — the fork's flush published outside \
         its own namespace"
    );

    db.shutdown().await.unwrap();
}
