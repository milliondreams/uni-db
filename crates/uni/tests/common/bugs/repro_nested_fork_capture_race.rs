#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for crates/uni/src/api/fork.rs:452 (finding [6]).
//!
//! For a NESTED fork, `build_datasets_for_fork` reads the parent fork branch's
//! LIVE tip via `lance_branch::current_version_on_branch(&uri, &parent_branch)`
//! (line 452) — AFTER `flush_and_capture_fork_point` released the `flush_lock`.
//! The primary-parent path instead branches at the version captured atomically
//! under the lock (`captured_versions.get(&dataset_name)`), which is what the
//! M1 fix installed to close the capture-vs-branch race. The nested path never
//! captures the parent branch tip under the lock, so a concurrent commit+flush
//! on the parent fork that lands between `flush_and_capture_fork_point`
//! returning and line 452 executing advances the parent branch's Lance tip and
//! the child branches off that post-fork-point version — a snapshot-isolation
//! violation.
//!
//! This is a genuine race whose deterministic reproduction requires an injected
//! suspension point inside `build_datasets_for_fork` (between capture and line
//! 452) — production source we must NOT modify. This harness runs a
//! best-effort concurrency stress loop: a writer task tightly commits+flushes
//! new rows to fork `a` while the main task repeatedly creates nested forks
//! `b_k` and checks that each child's visible row set does not exceed the
//! parent's committed count observed just before the fork call. A strict leak
//! (child sees strictly more than the pre-fork snapshot plus the small window)
//! flags the race.
//!
//! `#[ignore]`d — timing-dependent and may not trip without the injected hook.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use uni_db::{DataType, Uni};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "repro for fork.rs:452: nested-fork capture race; needs injected suspension to be deterministic — run with --run-ignored=all"]
async fn nested_fork_reads_parent_live_tip_under_concurrency() -> anyhow::Result<()> {
    let db = Arc::new(Uni::in_memory().build().await?);
    db.schema()
        .label("Person")
        .property("k", DataType::Int64)
        .apply()
        .await?;

    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {k: 0})").await?;
    tx.commit().await?;
    db.flush().await?;

    let a = primary.fork("a").await?;
    let tx = a.tx().await?;
    tx.execute("CREATE (:Person {k: 1})").await?;
    tx.commit().await?;
    a.flush().await?;

    // Highest `k` durably committed+flushed to fork `a`.
    let highest = Arc::new(AtomicI64::new(1));

    // Writer task: keep committing+flushing new rows onto `a`.
    let a_writer = a.clone();
    let highest_w = Arc::clone(&highest);
    let writer = tokio::spawn(async move {
        for k in 2..2000i64 {
            let tx = match a_writer.tx().await {
                Ok(t) => t,
                Err(_) => break,
            };
            if tx
                .execute(&format!("CREATE (:Person {{k: {k}}})"))
                .await
                .is_err()
            {
                break;
            }
            if tx.commit().await.is_err() {
                break;
            }
            let _ = a_writer.flush().await;
            highest_w.store(k, Ordering::SeqCst);
        }
    });

    let mut leaked: Option<(usize, i64, i64)> = None;
    for round in 0..300usize {
        // Snapshot the parent's durable tip just before creating the child.
        let _pre = highest.load(Ordering::SeqCst);

        let name = format!("b_{round}");
        let b = match a.fork(&name).await {
            Ok(b) => b,
            Err(_) => break,
        };
        // Immediately after fork creation, re-read the parent tip.
        let post = highest.load(Ordering::SeqCst);

        let rows = b.query("MATCH (p:Person) RETURN max(p.k) AS mk").await?;
        let child_max: i64 = rows.rows()[0].get("mk").unwrap_or(0);

        // Correct behavior: the child is snapshot-isolated at the fork point,
        // so it must not see a `k` beyond what the parent could have flushed
        // during the fork call (`post`). Seeing more than `post` is impossible;
        // seeing beyond `pre` is the suspicious window the bug widens.
        if child_max > post {
            leaked = Some((round, child_max, post));
            break;
        }

        let _ = db.drop_fork_cascade(&name).await;
    }

    writer.abort();
    let _ = writer.await;

    match leaked {
        Some((round, child_max, post)) => {
            // BUG: nested child observed a parent write past the fork point.
            // (repro for crates/uni/src/api/fork.rs:452)
            println!(
                "round {round}: child saw k={child_max} > parent tip-at-fork {post} — leak"
            );
            assert!(child_max > post);
        }
        None => {
            // Not observed without an injected suspension point.
            panic!(
                "nested-fork capture race not observed in the stress window; \
                 deterministic reproduction requires an injected suspension inside \
                 build_datasets_for_fork (production code, not modified here)"
            );
        }
    }

    Ok(())
}
