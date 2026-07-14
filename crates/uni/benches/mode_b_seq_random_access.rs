// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Mode B-seq random-access perf gate (plugin-compute proposal §14 Q1 / test Q-5).
//!
//! The proposal gates Mode B-seq on a perf prototype: a JIT'd-WASM guest's
//! random-access step rate must stay within a pinned ratio (≤ 10×) of a
//! host-resident Rust baseline on a pointer-chasing microbench; otherwise a
//! host-resident fast path is needed before the phase is committed.
//!
//! This bench establishes the **host-resident baseline** (`direct` — the
//! [`ScratchGraph`] accessed natively) and the **JSON-ABI crossing cost**
//! (`json_abi` — the same walk driven through `ScratchGraph::call_json`, the
//! boundary a *compiled* guest crosses per op). Since a JIT'd-WASM body's per-op
//! cost is dominated by that host boundary crossing (the JIT itself is fast, and
//! §7a/§7b's whole objection to per-element callbacks is the *crossing*, not the
//! compute), the `json_abi / direct` ratio is the quantity Q-5 cares about. The
//! JIT'd-WASM arm proper is added once the WASM Mode B-seq fixture lands; run:
//!
//! ```text
//! cargo bench --bench mode_b_seq_random_access
//! ```

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use uni_plugin_builtin::algorithms::graph_compute::scratch::ScratchGraph;
use uni_plugin_builtin::algorithms::graph_compute::{Arena, WorkBudget};

/// Vertices in the pointer-chasing scratch graph.
const NODES: u32 = 4_096;
/// Out-edges per node (a ring edge + a pseudo-random chord).
const STEPS: usize = 2_048;

/// Builds a fresh scratch graph with a ring + chord adjacency and a generous
/// budget/arena so the walk itself never trips the meter.
fn build() -> ScratchGraph {
    let mut g = ScratchGraph::new(
        WorkBudget::new(1_000_000_000),
        Arena::new(1 << 30, 1 << 24),
        0xB5E9,
    );
    for i in 0..NODES {
        g.add_node(f64::from(i)).unwrap();
    }
    for i in 0..NODES {
        g.add_edge(i, (i + 1) % NODES).unwrap();
        g.add_edge(i, (i.wrapping_mul(2_654_435_761)) % NODES)
            .unwrap();
    }
    g
}

/// Chases pointers `STEPS` times via the native accessors (the host baseline).
fn walk_direct(g: &mut ScratchGraph) -> f64 {
    let mut node = 0u32;
    let mut acc = 0.0f64;
    for k in 0..STEPS {
        let nb = g.neighbors(node).unwrap();
        node = nb[k % nb.len()];
        acc += g.get_field(node).unwrap();
    }
    acc
}

/// Chases the same pointers through the JSON `host-graph` ABI a compiled guest
/// crosses per op (parse request → dispatch → serialize response).
fn walk_json(g: &mut ScratchGraph) -> f64 {
    let mut node = 0u32;
    let mut acc = 0.0f64;
    for k in 0..STEPS {
        let resp = g
            .call_json(&format!(r#"{{"op":"neighbors","a":{node}}}"#))
            .unwrap();
        // Parse the neighbor list back out of the JSON response.
        let nb: Vec<u32> = serde_json::from_str::<serde_json::Value>(&resp)
            .ok()
            .and_then(|v| {
                v.get("v").and_then(|l| l.as_array()).map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_u64().map(|u| u as u32))
                        .collect()
                })
            })
            .unwrap_or_default();
        node = nb[k % nb.len()];
        let fr = g
            .call_json(&format!(r#"{{"op":"get_field","a":{node}}}"#))
            .unwrap();
        acc += serde_json::from_str::<serde_json::Value>(&fr)
            .ok()
            .and_then(|v| v.get("v").and_then(serde_json::Value::as_f64))
            .unwrap_or(0.0);
    }
    acc
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("mode_b_seq_random_access");
    group.bench_function("direct_host_baseline", |b| {
        b.iter_batched(
            build,
            |mut g| black_box(walk_direct(&mut g)),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("json_abi_crossing", |b| {
        b.iter_batched(
            build,
            |mut g| black_box(walk_json(&mut g)),
            BatchSize::SmallInput,
        );
    });
    group.finish();
    // The `json_abi_crossing / direct_host_baseline` time ratio is the per-op
    // host-boundary overhead a compiled guest pays; Q-5's ≤ 10× bound is applied
    // to the JIT'd-WASM-vs-host ratio once the WASM fixture arm is wired in.
}

criterion_group!(benches, bench);
criterion_main!(benches);
