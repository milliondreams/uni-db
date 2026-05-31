// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Clone-on-freeze latency: the tail-risk of the shipped strategy-C snapshot
//! design.
//!
//! When a transaction commits while another transaction holds a pinned snapshot
//! generation, the committer deep-clones the whole L0 buffer aside so the pinned
//! reader stays isolated. That clone is O(L0 size). This bench measures commit
//! latency WITH a concurrent pin (freeze fires) vs WITHOUT (in-place merge)
//! across L0 sizes, so the p99 commit tail under snapshot contention is known.
//!
//! ```bash
//! cargo bench --bench ssi_freeze
//! ```
//!
//! Freeze fires under the default (SSI-on) config when a commit races a pinned
//! snapshot; with `ssi_enabled = false` the two arms are identical (no freeze),
//! so the gap is the freeze cost.

use std::sync::Arc;
use std::time::Instant;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;
use uni_db::{DataType, Uni};

const L0_SIZES: &[usize] = &[1_000, 10_000];

async fn seed(n: usize) -> Arc<Uni> {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("N")
        .property("id", DataType::String)
        .property("v", DataType::Int)
        .done()
        .apply()
        .await
        .unwrap();
    let s = db.session();
    let tx = s.tx().await.unwrap();
    for i in 0..n {
        tx.execute(&format!("CREATE (:N {{id: 'n{i}', v: {i}}})"))
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    Arc::new(db)
}

/// One small committed write. When `pin` is true a reader transaction holds a
/// snapshot for the duration, so the committer must freeze the generation aside.
async fn one_commit(db: &Arc<Uni>, pin: bool) {
    let reader = if pin {
        let s = db.session();
        let tx = s.tx().await.unwrap();
        tx.query("MATCH (n:N {id: 'n0'}) RETURN n.v").await.unwrap();
        Some(tx)
    } else {
        None
    };

    let s = db.session();
    let tx = s.tx().await.unwrap();
    tx.execute("MATCH (n:N {id: 'n0'}) SET n.v = n.v + 1")
        .await
        .unwrap();
    tx.commit().await.unwrap();

    drop(reader);
}

fn bench_freeze(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("ssi_freeze");
    group.sample_size(20);

    for &n in L0_SIZES {
        let db = rt.block_on(seed(n));
        group.bench_with_input(BenchmarkId::new("commit_no_pin", n), &n, |b, _| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let start = Instant::now();
                    for _ in 0..iters {
                        one_commit(&db, false).await;
                    }
                    start.elapsed()
                })
            });
        });
        group.bench_with_input(BenchmarkId::new("commit_with_pin", n), &n, |b, _| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    let start = Instant::now();
                    for _ in 0..iters {
                        one_commit(&db, true).await;
                    }
                    start.elapsed()
                })
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_freeze);
criterion_main!(benches);
