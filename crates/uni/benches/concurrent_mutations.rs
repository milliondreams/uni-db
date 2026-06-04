// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Concurrent-mutation benchmark for the writer-RwLock removal.
//!
//! Spawns `N` tokio tasks, each running 100 trivial `CREATE` statements
//! through its own `Session`/`Transaction`. The wall time at `N=24` versus
//! `N=1` is the headline number: per proposal acceptance, the ratio should
//! be **≤ 2×**. Before Phase 4 the ratio would have been ~24× because the
//! database-wide writer-RwLock serialized every mutation entry.
//!
//! ```bash
//! cargo bench --bench concurrent_mutations
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mimalloc::MiMalloc;
use tokio::runtime::Runtime;
use uni_db::Uni;

// Profile (perf-recorded, 131k samples at sess=24) showed ~50% of CPU time
// in glibc malloc / kernel page-fault zeroing under heavy concurrent
// allocation. mimalloc's per-thread arenas + thread-local free lists avoid
// most of that traffic.
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const STATEMENTS_PER_TASK: usize = 100;
const SESSIONS_SWEEP: &[usize] = &[1, 4, 12, 24];

/// One Criterion sample: `n_sessions` tasks each running
/// `STATEMENTS_PER_TASK` `CREATE` statements through their own `tx()`.
async fn run_concurrent_creates(db: Arc<Uni>, n_sessions: usize) {
    let mut handles = Vec::with_capacity(n_sessions);
    for s in 0..n_sessions {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            let session = db.session();
            let tx = session.tx().await.unwrap();
            for i in 0..STATEMENTS_PER_TASK {
                tx.execute(&format!("CREATE (n:BenchNode {{idx: {i}, sess: {s}}})"))
                    .await
                    .unwrap();
            }
            tx.commit().await.unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

fn bench_concurrent_creates(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("concurrent_creates");
    group.sample_size(10);
    // Each task does 100 CREATEs; with N=24 that's 2400 mutations per sample.
    // Bound measurement time so the whole sweep stays under a few minutes.
    group.measurement_time(Duration::from_secs(20));

    for &n in SESSIONS_SWEEP {
        group.bench_with_input(BenchmarkId::new("sessions", n), &n, |b, &n| {
            b.iter_custom(|iters| {
                rt.block_on(async move {
                    // Per-iteration setup: fresh in-memory DB + label.
                    // Doing this outside the timed window keeps the
                    // measurement focused on the mutation path.
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let db = Arc::new(Uni::in_memory().build().await.unwrap());
                        let start = Instant::now();
                        run_concurrent_creates(db.clone(), n).await;
                        total += start.elapsed();
                    }
                    total
                })
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_concurrent_creates);
criterion_main!(benches);
