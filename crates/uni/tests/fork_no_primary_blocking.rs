// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Spec §10: "Fork creation doesn't block primary."
//!
//! Verifies that a long-running fork creation does not stall primary
//! reads/writes. Run a primary read in a tight loop while a fork is
//! being created on a separate task; assert no read takes pathologically
//! long during the fork creation window.

// Rust guideline compliant

use std::time::{Duration, Instant};

use uni_db::{DataType, Uni};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_fork_creation_does_not_block_primary_reads() {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Item")
        .property("kind", DataType::String)
        .apply()
        .await
        .unwrap();

    let session = db.session();
    // Seed enough rows so each label has an on-disk dataset to branch.
    let tx = session.tx().await.unwrap();
    for i in 0..50 {
        tx.execute(&format!("CREATE (:Item {{kind: 'k{i}'}})"))
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    db.flush().await.unwrap();

    // Spawn fork creation. Build the session here so the spawned task
    // doesn't need a Clone-able Uni handle.
    let fork_session = db.session();
    let fork_handle = tokio::spawn(async move {
        fork_session.fork("non_blocking_fork").await.unwrap();
        // Hold for a brief moment so the primary read loop runs while
        // the fork session is live.
        tokio::time::sleep(Duration::from_millis(50)).await;
    });

    // Tight read loop on primary while fork creation runs.
    let read_session = db.session();
    let mut max_read_latency = Duration::from_secs(0);
    let read_loop_start = Instant::now();
    let mut iterations = 0usize;
    while !fork_handle.is_finished() && read_loop_start.elapsed() < Duration::from_secs(5) {
        let t = Instant::now();
        let _rows = read_session
            .query("MATCH (i:Item) RETURN i LIMIT 5")
            .await
            .unwrap();
        let dt = t.elapsed();
        if dt > max_read_latency {
            max_read_latency = dt;
        }
        iterations += 1;
    }
    fork_handle.await.unwrap();

    eprintln!(
        "iterations={iterations} max_read_latency={max_read_latency:?}"
    );

    // Threshold: a single primary read must not take more than 250 ms
    // during the fork creation window. CI variance can be wide; keep
    // this generous and let it catch only true blocking.
    assert!(
        max_read_latency < Duration::from_millis(250),
        "max primary read latency during fork creation was {max_read_latency:?}; \
         fork creation appears to be blocking primary"
    );

    db.shutdown().await.unwrap();
}
