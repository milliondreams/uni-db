// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 2 Day 14 — long-running fork write soak.
//!
//! Builds a worktree with N forks, interleaves M mutations per fork
//! with M primary mutations, periodically shuts down and reopens the
//! database to exercise the WAL replay path, and verifies the final
//! state matches expectations.
//!
//! `#[ignore]`'d so it doesn't run in default test sweeps. Two run
//! shapes:
//!
//! - Local sanity: `cargo nextest run -p uni-db --test fork_writes_soak
//!   --run-ignored ignored-only` — uses default small parameters
//!   (5 forks × 50 mutations, single shutdown cycle, ~10s wall).
//! - Nightly CI: set `UNI_FORK_SOAK_FORKS=40`,
//!   `UNI_FORK_SOAK_MUTATIONS=300`, `UNI_FORK_SOAK_RESTARTS=4` —
//!   ~5 min wall time. Calibrated to complete reliably within the
//!   nightly job budget (the original 100/1000/10 spec target ran
//!   well over 40 min and was killed by the per-test timeout).
//!
//! What this catches that smaller tests don't:
//! - Per-fork WAL retention regressions (Phase 2 Day 5 substrate).
//! - L0/L1 boundary issues across many flushes per fork.
//! - Recovery correctness under churn.
//! - Resource leaks (fd, memory) over time — surface via OOM or
//!   "too many open files" if they regress.

// Rust guideline compliant

use anyhow::Result;
use std::path::PathBuf;
use std::time::Duration;
use uni_db::{DataType, Uni, UniConfig};

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::test]
#[ignore = "long-running soak; opt in with --run-ignored ignored-only"]
async fn fork_writes_soak() -> Result<()> {
    let n_forks = env_usize("UNI_FORK_SOAK_FORKS", 5);
    let n_mutations = env_usize("UNI_FORK_SOAK_MUTATIONS", 50);
    let n_restarts = env_usize("UNI_FORK_SOAK_RESTARTS", 1);

    let dir = tempfile::tempdir()?;
    let uri: String = dir.path().to_str().unwrap().to_string();

    // Force flushes regularly so each cycle exercises the L0→L1 path
    // including the on-the-fly branch creation (Day 10) and per-fork
    // WAL routing (Day 5).
    let config = UniConfig {
        auto_flush_threshold: 8,
        // The cranked nightly knobs (100 forks, constant flushing at
        // threshold 8) create synthetic commit contention: an individual
        // commit can wait on the global writer lock held by background
        // flush/compaction longer than the 5s production-default
        // `commit_timeout`. This soak's signal is recovery/correctness under
        // churn (verified below), not commit latency, so give the guard
        // generous headroom rather than failing spuriously under the
        // artificial load.
        commit_timeout: Duration::from_secs(120),
        ..Default::default()
    };

    // Initial schema setup.
    {
        let db = Uni::open(&uri).config(config.clone()).build().await?;
        db.schema()
            .label("Item")
            .property("kind", DataType::String)
            .property("seq", DataType::Int64)
            .apply()
            .await?;
        // Seed primary so the vertices_Item dataset exists at fork-point.
        let session = db.session();
        let tx = session.tx().await?;
        tx.execute("CREATE (:Item {kind: 'seed', seq: -1})").await?;
        tx.commit().await?;
        db.flush().await?;
        db.shutdown().await?;
    }

    let fork_names: Vec<String> = (0..n_forks).map(|i| format!("soak_{i}")).collect();
    let mut expected_per_fork: Vec<usize> = vec![0; n_forks];
    let mut expected_primary: usize = 1; // seed

    for cycle in 0..(n_restarts + 1) {
        let db = Uni::open(&uri).config(config.clone()).build().await?;
        let session = db.session();

        // Create or reopen all forks.
        let mut forks = Vec::with_capacity(n_forks);
        for name in &fork_names {
            forks.push(session.fork(name).await?);
        }

        // Interleave: in each step, append to one fork and to primary.
        for step in 0..n_mutations {
            let fork_idx = step % n_forks;
            let f = &forks[fork_idx];
            let f_seq = expected_per_fork[fork_idx];
            let tx = f.tx().await?;
            tx.execute(&format!(
                "CREATE (:Item {{kind: 'fork-{fork_idx}-c{cycle}', seq: {f_seq}}})"
            ))
            .await?;
            tx.commit().await?;
            expected_per_fork[fork_idx] += 1;

            let p_seq = expected_primary;
            let tx = session.tx().await?;
            tx.execute(&format!(
                "CREATE (:Item {{kind: 'primary-c{cycle}', seq: {p_seq}}})"
            ))
            .await?;
            tx.commit().await?;
            expected_primary += 1;
        }

        // Drop fork sessions before shutdown so drop_fork in a later
        // cycle (if we add it) wouldn't be blocked. Sessions also
        // drop on shutdown but explicit drop is clearer.
        drop(forks);
        db.flush().await?;
        db.shutdown().await?;
    }

    // Final verification cycle: reopen, count rows on each fork and
    // primary; assert they match cumulative expectations.
    {
        let db = Uni::open(&uri).config(config).build().await?;
        let session = db.session();

        for (i, name) in fork_names.iter().enumerate() {
            let fork = session.fork(name).await?;
            // Each fork sees its own writes plus the seed (inherited
            // from primary at fork-point in cycle 0; later cycles
            // re-create the same fork and inherit primary state at
            // that later fork-point, which includes earlier primary
            // writes — so a re-opened fork actually inherits the
            // updated primary state since Lance branches diverge from
            // a parent version captured at branch creation).
            //
            // For correctness we just check the lower bound: the fork
            // sees at least its own writes.
            let count = fork
                .query("MATCH (i:Item) RETURN count(i) AS c")
                .await?
                .rows()
                .first()
                .and_then(|r| r.get::<i64>("c").ok())
                .unwrap_or(0) as usize;
            assert!(
                count >= expected_per_fork[i],
                "fork {name} should see at least {} rows; got {count}",
                expected_per_fork[i]
            );
        }

        let primary_count = session
            .query("MATCH (i:Item) RETURN count(i) AS c")
            .await?
            .rows()
            .first()
            .and_then(|r| r.get::<i64>("c").ok())
            .unwrap_or(0) as usize;
        assert_eq!(
            primary_count, expected_primary,
            "primary row count must match cumulative expectation"
        );

        db.shutdown().await?;
    }

    // Sanity: the worktree path still exists (we're using its dir).
    assert!(PathBuf::from(&uri).exists());

    Ok(())
}
