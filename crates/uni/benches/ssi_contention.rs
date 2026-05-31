// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Writer throughput under contention: N retried writers on a hot key.
//!
//! The OCC worst case is many writers contending on one row: every conflict is a
//! retry. This bench measures wall time to apply a fixed amount of work as the
//! writer count grows, so throughput scaling and the retry cost under `ssi` can
//! be characterized.
//!
//! ```bash
//! cargo bench --bench ssi_contention                  # baseline (LWW, no aborts)
//! cargo bench --bench ssi_contention --features ssi   # ssi on: retries under contention
//! ```
//!
//! Correctness (final == total increments) is covered by the stress tests; here
//! we only measure time. With `ssi` off this is the no-conflict baseline (LWW —
//! note it silently loses updates); with `ssi` on the gap is the
//! conflict-and-retry overhead that buys correctness.

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mimalloc::MiMalloc;
use tokio::runtime::Runtime;
use uni_db::Uni;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const INCREMENTS_PER_WRITER: usize = 20;
const WRITERS_SWEEP: &[usize] = &[1, 4, 12, 24];

async fn fresh_counter() -> Arc<Uni> {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Counter")
        .property("id", uni_db::DataType::String)
        .property("n", uni_db::DataType::Int)
        .done()
        .apply()
        .await
        .unwrap();
    let s = db.session();
    let tx = s.tx().await.unwrap();
    tx.execute("CREATE (:Counter {id: 'x', n: 0})")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    Arc::new(db)
}

async fn run_contended(db: Arc<Uni>, writers: usize) {
    let mut handles = Vec::with_capacity(writers);
    for _ in 0..writers {
        let db = db.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..INCREMENTS_PER_WRITER {
                // `execute_with_retry` retries serialization conflicts under ssi;
                // under the off build it never conflicts (LWW).
                db.session()
                    .execute_with_retry("MATCH (c:Counter {id: 'x'}) SET c.n = c.n + 1")
                    .await
                    .unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
}

fn bench_contention(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("ssi_contention");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    for &w in WRITERS_SWEEP {
        group.bench_with_input(BenchmarkId::new("writers", w), &w, |b, &w| {
            b.iter_custom(|iters| {
                rt.block_on(async move {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let db = fresh_counter().await;
                        let start = Instant::now();
                        run_contended(db, w).await;
                        total += start.elapsed();
                    }
                    total
                })
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_contention);
criterion_main!(benches);
