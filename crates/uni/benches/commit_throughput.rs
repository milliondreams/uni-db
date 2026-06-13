// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Commit-throughput benchmark — the group-commit headroom measurement.
//!
//! Spawns `N` tokio tasks, each committing `COMMITS_PER_TASK` SMALL
//! transactions on DISJOINT keys (no SSI conflicts), so the measured cost
//! is pure commit-path serialization: every commit holds `flush_lock`
//! across OCC validation + one WAL segment PUT (+ fsync on local stores)
//! + merge. Two series:
//!
//! - `wal_on`  — the production path (one durable WAL write per commit).
//! - `wal_off` — `UniConfig.wal_enabled = false`: the same protocol minus
//!   the WAL write. This is the theoretical ceiling a perfect group commit
//!   could approach; the gap between the series IS the group-commit
//!   headroom (see `docs/proposals/group_commit.md`).
//!
//! ```bash
//! cargo bench --bench commit_throughput
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mimalloc::MiMalloc;
use tokio::runtime::Runtime;
use uni_db::{Uni, UniConfig};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const COMMITS_PER_TASK: usize = 25;
const SESSIONS_SWEEP: &[usize] = &[1, 4, 12, 24];

/// Disk-backed DB: the WAL write must hit the real filesystem (PUT + file
/// and directory fsync) or the `wal_on`/`wal_off` gap — the quantity this
/// bench exists to measure — collapses to a hashmap insert. (An earlier
/// in-memory variant measured wal_on ≈ wal_off at every N for exactly
/// that reason.) The TempDir is returned so it outlives the measurement.
async fn build_db(wal_enabled: bool) -> (Arc<Uni>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let config = UniConfig {
        wal_enabled,
        ..Default::default()
    };
    let db = Uni::open(dir.path().join("db").to_string_lossy().as_ref())
        .config(config)
        .build()
        .await
        .unwrap();
    (Arc::new(db), dir)
}

/// One sample: `n_sessions` tasks, each running `COMMITS_PER_TASK`
/// single-CREATE transactions (commit per statement — the commit path IS
/// the workload).
async fn run_concurrent_commits(db: Arc<Uni>, n_sessions: usize) {
    let mut handles = Vec::with_capacity(n_sessions);
    for s in 0..n_sessions {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            let session = db.session();
            for i in 0..COMMITS_PER_TASK {
                let tx = session.tx().await.unwrap();
                tx.execute(&format!("CREATE (:C {{sess: {s}, idx: {i}}})"))
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

fn bench_commit_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    for wal_enabled in [true, false] {
        let series = if wal_enabled { "wal_on" } else { "wal_off" };
        let mut group = c.benchmark_group(format!("commit_throughput/{series}"));
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(15));
        for &n in SESSIONS_SWEEP {
            // Commits per sample so throughput (commits/s) is readable.
            group.throughput(criterion::Throughput::Elements(
                (n * COMMITS_PER_TASK) as u64,
            ));
            group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        // Fresh DB per iteration: keeps L0 size (and any
                        // auto-flush) from skewing later iterations.
                        let (db, _dir) = rt.block_on(build_db(wal_enabled));
                        let start = Instant::now();
                        rt.block_on(run_concurrent_commits(db, n));
                        total += start.elapsed();
                    }
                    total
                });
            });
        }
        group.finish();
    }
}

criterion_group!(benches, bench_commit_throughput);
criterion_main!(benches);
