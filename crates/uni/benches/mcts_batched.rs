// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Batched-descent MCTS — does coarsening the granularity close the Mode B-seq
//! perf gap? (issue #152 / plugin-compute proposal §14 Q1 follow-up.)
//!
//! `mode_b_seq_wasm` measured the per-op Mode B-seq ABI at **56× (JSON, native)**
//! and **362× (JIT'd WASM)** over the host-resident baseline, against a pinned
//! ≤ 10× gate — a miss so large that no guest technology can close it while every
//! tree edge costs a host crossing. This bench tests the alternative: keep the
//! guest as *conductor* and make each crossing carry a whole batch of rollouts.
//!
//! Three arms run the identical search — a complete binary tree of depth `DEPTH`,
//! `ROLLOUTS` descents, each visiting every node on its path and choosing the
//! least-visited child (deterministic, ties by lower slot):
//!
//! | arm | granularity | crossings |
//! |---|---|---|
//! | `native` | direct Rust calls | 0 (the baseline) |
//! | `per_op_json` | one op per tree edge (today's B-seq ABI) | `ROLLOUTS × DEPTH × 3` |
//! | `batched_json` | one op per *level*, `BATCH` rollouts wide | `(ROLLOUTS / BATCH) × DEPTH × 2` |
//!
//! **Semantic caveat, stated up front:** batching is not free. Rollouts inside a
//! batch descend against the *same* stale visit counts, whereas the sequential
//! search sees every prior rollout's updates. Real batched MCTS compensates with
//! virtual loss. This bench measures the **cost ceiling** of the batched shape;
//! whether the staleness is acceptable is a separate search-quality question.
//!
//! Run: `cargo bench -p uni-db --bench mcts_batched`

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::time::{Duration, Instant};
use uni_plugin_builtin::algorithms::graph_compute::scratch::ScratchGraph;
use uni_plugin_builtin::algorithms::graph_compute::{Arena, WorkBudget};

/// Depth of the complete binary game tree (root is level 0).
const DEPTH: usize = 10;
/// Rollouts per search.
const ROLLOUTS: usize = 4_096;
/// Rollouts advanced per crossing in the batched arm.
const BATCH: usize = 256;
/// The pinned Mode B-seq bound (proposal §14 Q1 / Q-5).
const GATE_RATIO: f64 = 10.0;
/// Repetitions behind the printed verdict.
const REPS: usize = 5;

/// Builds a complete binary tree of `DEPTH` levels with zeroed visit fields.
fn build() -> ScratchGraph {
    let nodes = (1usize << (DEPTH + 1)) - 1;
    let mut g = ScratchGraph::new(
        WorkBudget::new(u64::MAX / 4),
        Arena::new(1 << 30, 1 << 24),
        0x4D43_5453, // "MCTS"
    );
    for _ in 0..nodes {
        g.add_node(0.0).unwrap();
    }
    for p in 0..nodes {
        let (l, r) = (2 * p + 1, 2 * p + 2);
        if r < nodes {
            g.add_edge(p as u32, l as u32).unwrap();
            g.add_edge(p as u32, r as u32).unwrap();
        }
    }
    g
}

/// The host-resident baseline: the same search via native accessors.
fn mcts_native(g: &mut ScratchGraph) -> f64 {
    for _ in 0..ROLLOUTS {
        let mut node = 0u32;
        for _ in 0..DEPTH {
            let v = g.get_field(node).unwrap();
            g.set_field(node, v + 1.0).unwrap();
            let kids = g.neighbors(node).unwrap();
            if kids.is_empty() {
                break;
            }
            node = *kids
                .iter()
                .min_by(|&&x, &&y| {
                    g.get_field(x)
                        .unwrap()
                        .total_cmp(&g.get_field(y).unwrap())
                        .then(x.cmp(&y))
                })
                .unwrap();
        }
    }
    g.get_field(0).unwrap()
}

/// One JSON round-trip, returning the parsed `v` payload.
fn op(g: &mut ScratchGraph, req: &str) -> serde_json::Value {
    let resp = g.call_json(req).unwrap();
    serde_json::from_str::<serde_json::Value>(&resp)
        .ok()
        .and_then(|v| v.get("v").cloned())
        .unwrap_or(serde_json::Value::Null)
}

/// Today's per-op B-seq ABI: one crossing per `get_field` / `set_field` /
/// `neighbors`, i.e. three per tree edge descended.
fn mcts_per_op(g: &mut ScratchGraph) -> f64 {
    for _ in 0..ROLLOUTS {
        let mut node = 0u32;
        for _ in 0..DEPTH {
            let v = op(g, &format!(r#"{{"op":"get_field","a":{node}}}"#))
                .as_f64()
                .unwrap_or(0.0);
            op(
                g,
                &format!(r#"{{"op":"set_field","a":{node},"f":{}}}"#, v + 1.0),
            );
            let kids: Vec<u32> = op(g, &format!(r#"{{"op":"neighbors","a":{node}}}"#))
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_u64().map(|u| u as u32))
                        .collect()
                })
                .unwrap_or_default();
            if kids.is_empty() {
                break;
            }
            // The guest must also read each child's stat to choose — the cost the
            // batched kernel folds host-side.
            let mut best = kids[0];
            let mut best_v = f64::MAX;
            for &c in &kids {
                let cv = op(g, &format!(r#"{{"op":"get_field","a":{c}}}"#))
                    .as_f64()
                    .unwrap_or(0.0);
                if cv < best_v {
                    best_v = cv;
                    best = c;
                }
            }
            node = best;
        }
    }
    op(g, r#"{"op":"get_field","a":0}"#).as_f64().unwrap_or(0.0)
}

/// The batched shape: `BATCH` rollouts advance in lockstep, two crossings per
/// level (`visit_batch` + `descend_batch`) regardless of batch width.
fn mcts_batched(g: &mut ScratchGraph) -> f64 {
    let batches = ROLLOUTS / BATCH;
    for _ in 0..batches {
        let mut active = vec![0u32; BATCH];
        for _ in 0..DEPTH {
            let list = serde_json::to_string(&active).unwrap();
            op(g, &format!(r#"{{"op":"visit_batch","v":{list},"f":1.0}}"#));
            let next: Vec<u32> = op(g, &format!(r#"{{"op":"descend_batch","v":{list}}}"#))
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_u64().map(|u| u as u32))
                        .collect()
                })
                .unwrap_or_default();
            if next.is_empty() {
                break;
            }
            active = next;
        }
    }
    op(g, r#"{"op":"get_field","a":0}"#).as_f64().unwrap_or(0.0)
}

fn time(reps: usize, mut f: impl FnMut(&mut ScratchGraph) -> f64) -> Duration {
    let mut total = Duration::ZERO;
    for _ in 0..reps {
        let mut g = build();
        let t = Instant::now();
        black_box(f(&mut g));
        total += t.elapsed();
    }
    total / reps as u32
}

fn report() {
    let nat = time(REPS, mcts_native);
    let per = time(REPS, mcts_per_op);
    let bat = time(REPS, mcts_batched);

    let (n, p, b) = (nat.as_secs_f64(), per.as_secs_f64(), bat.as_secs_f64());
    // Crossing counts: per-op pays get+set+neighbors + one get per child (2).
    let per_cross = ROLLOUTS * DEPTH * 5;
    let bat_cross = (ROLLOUTS / BATCH) * DEPTH * 2;

    println!("\n=== Batched-descent MCTS (issue #152 / Q-5 follow-up) ===");
    println!(
        "tree: complete binary, depth {DEPTH} ({} nodes)",
        (1 << (DEPTH + 1)) - 1
    );
    println!("rollouts: {ROLLOUTS}   batch width: {BATCH}\n");
    println!(
        "native           : {:>10.3} ms   crossings         0        1.00× native",
        n * 1e3
    );
    println!(
        "per_op_json      : {:>10.3} ms   crossings {per_cross:>8}   {:>7.1}× native",
        p * 1e3,
        p / n
    );
    println!(
        "batched_json     : {:>10.3} ms   crossings {bat_cross:>8}   {:>7.1}× native",
        b * 1e3,
        b / n
    );
    println!(
        "\ncrossing reduction : {:.0}×   (per_op / batched)",
        per_cross as f64 / bat_cross as f64
    );
    println!("speedup vs per-op  : {:.1}×", p / b);
    println!(
        "gate (≤ {GATE_RATIO:.0}× native): per_op = {:.1}× → {}, batched = {:.1}× → {}",
        p / n,
        if p / n <= GATE_RATIO { "PASS" } else { "FAIL" },
        b / n,
        if b / n <= GATE_RATIO { "PASS" } else { "FAIL" },
    );
    println!("=========================================================\n");
}

fn bench(c: &mut Criterion) {
    report();
    let mut group = c.benchmark_group("mcts_batched");
    group.sample_size(10);
    group.bench_function("native", |bch| {
        bch.iter_batched(
            build,
            |mut g| black_box(mcts_native(&mut g)),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("per_op_json", |bch| {
        bch.iter_batched(
            build,
            |mut g| black_box(mcts_per_op(&mut g)),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("batched_json", |bch| {
        bch.iter_batched(
            build,
            |mut g| black_box(mcts_batched(&mut g)),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
