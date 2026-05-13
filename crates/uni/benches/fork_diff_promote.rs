// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 7d — Criterion benches for fork diff & promote.
//!
//! Run with:
//! ```bash
//! cargo bench --bench fork_diff_promote
//! ```
//!
//! Or for a fast smoke run:
//! ```bash
//! cargo bench --bench fork_diff_promote -- --quick
//! ```
//!
//! N is dialed to 1k vertices to keep each case under ~10s on a
//! developer laptop. A 1M-vertex soak would be a separate
//! `#[ignore]`'d integration test — the goal of this harness is to
//! surface regressions in the typical write-audit-publish case.
//!
//! **Phase 7d findings (HEAD ~`7182ef9a`):**
//!
//! | Bench | N | Time |
//! |---|---:|---:|
//! | `diff_primary_vs_fork` | seed=1k + fork-add=1k | ~24 ms |
//! | `diff_unrelated_forks` | 1k+1k vs 1k+1k | ~34 ms |
//! | `promote_vertices` | seed=1k + fork-add=1k | ~9 s |
//!
//! Diff is fast (Arrow scan + HashMap bucket). Promote is the slow
//! path because the vertex branch runs *two* primary queries per
//! fork row — `UidIndex::get_vid` and a `MATCH … WHERE id(n) = $vid`
//! verify (the `resolve_primary_vid` helper in `fork_diff.rs`). At
//! ~5 ms per query that's ~10 s for 1k rows. Batching the
//! UID→primary-VID resolution into a single fork-side query would
//! be the obvious optimization; tracked separately from Phase 7d.

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;
use uni_db::api::fork_diff::PromotePattern;
use uni_db::{DataType, Uni};

const SEED_N: usize = 1_000;
const FORK_N: usize = 1_000;

async fn make_db() -> Uni {
    Uni::in_memory().build().await.unwrap()
}

async fn seed_persons(db: &Uni, count: usize) {
    let s = db.session();
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await
        .unwrap();
    let tx = s.tx().await.unwrap();
    let props: Vec<uni_common::Properties> = (0..count)
        .map(|i| {
            let mut p = uni_common::Properties::new();
            p.insert(
                "name".into(),
                uni_common::Value::String(format!("seed-{i}")),
            );
            p
        })
        .collect();
    tx.bulk_insert_vertices("Person", props).await.unwrap();
    tx.commit().await.unwrap();
    db.flush().await.unwrap();
}

async fn fork_with_adds(db: &Uni, name: &str, count: usize) {
    let s = db.session();
    let fork = s.fork(name).await.unwrap();
    let tx = fork.tx().await.unwrap();
    let props: Vec<uni_common::Properties> = (0..count)
        .map(|i| {
            let mut p = uni_common::Properties::new();
            p.insert(
                "name".into(),
                uni_common::Value::String(format!("fork-{name}-{i}")),
            );
            p
        })
        .collect();
    tx.bulk_insert_vertices("Person", props).await.unwrap();
    tx.commit().await.unwrap();
    fork.flush().await.unwrap();
}

/// Diff a fork with FORK_N fork-only vertex adds against a primary
/// seeded with SEED_N rows. Measures the dominant cost: scan +
/// bucket + UID compute on both sides.
fn bench_diff_primary_vs_fork(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("fork_diff_primary_vs_fork");
    group.sample_size(10);

    group.bench_function(format!("seed_{SEED_N}_add_{FORK_N}"), |b| {
        b.iter_batched(
            || {
                rt.block_on(async {
                    let db = make_db().await;
                    seed_persons(&db, SEED_N).await;
                    fork_with_adds(&db, "bench_fork", FORK_N).await;
                    db
                })
            },
            |db| {
                rt.block_on(async {
                    let diff = db.diff_fork_primary("bench_fork").await.unwrap();
                    // Defensive — drift detector if the harness ever
                    // creates a degenerate fork that the diff path
                    // can't see. Without it the optimizer might drop
                    // the call entirely.
                    assert!(diff.vertices.added.len() >= FORK_N);
                })
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

/// Promote a vertex pattern off a fork with FORK_N fresh Person rows
/// onto a primary seeded with SEED_N existing rows. Measures the full
/// scan + UID dedup + bulk_insert_vertices path.
fn bench_promote_vertices(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("fork_promote_vertices");
    group.sample_size(10);

    group.bench_function(format!("seed_{SEED_N}_promote_{FORK_N}"), |b| {
        b.iter_batched(
            || {
                rt.block_on(async {
                    let db = make_db().await;
                    seed_persons(&db, SEED_N).await;
                    fork_with_adds(&db, "promote_bench", FORK_N).await;
                    db
                })
            },
            |db| {
                rt.block_on(async {
                    let report = db
                        .promote_from_fork("promote_bench", &[PromotePattern::label("Person")])
                        .await
                        .unwrap();
                    assert!(report.vertices_inserted >= FORK_N);
                })
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

/// Diff two unrelated fork siblings. Each side has 1k fork-only
/// vertices; the bucket join is across 2 × (SEED_N + FORK_N) rows.
fn bench_diff_unrelated_forks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("fork_diff_unrelated");
    group.sample_size(10);

    group.bench_function(format!("each_{FORK_N}_adds"), |b| {
        b.iter_batched(
            || {
                rt.block_on(async {
                    let db = make_db().await;
                    seed_persons(&db, SEED_N).await;
                    fork_with_adds(&db, "left", FORK_N).await;
                    fork_with_adds(&db, "right", FORK_N).await;
                    db
                })
            },
            |db| {
                rt.block_on(async {
                    let diff = db.diff_forks("left", "right").await.unwrap();
                    assert!(diff.vertices.added.len() >= FORK_N);
                    assert!(diff.vertices.deleted.len() >= FORK_N);
                })
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_diff_primary_vs_fork,
    bench_promote_vertices,
    bench_diff_unrelated_forks,
);
criterion_main!(benches);
