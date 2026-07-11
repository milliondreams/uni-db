// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Differential tests: kernel-authored algorithms vs independent naive oracles.
//!
//! The oracles here share *no code* with [`GraphProjection`] or the kernels —
//! they are plain adjacency-list implementations (BFS, union-find, power
//! iteration with sequential summation) — so agreement is real evidence, not a
//! shared bug (proposal §9.0 oracle-independence invariant). Handle-security
//! tests attack the table directly (proposal §9.2 H-1…H-7).
//
// Rust guideline compliant

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use uni_algo::algo::GraphProjection;
use uni_common::Value;
use uni_common::core::id::Vid;

use super::first_party::{
    bellman_ford, eigenvector_centrality, k_core, personalized_pagerank, reachable_set, wcc_labels,
};
use super::handle::{Handle, HandleKind};
use super::session::{AlgoSession, Direction, GraphCompute};
use super::value::Scalar;
use super::{Arena, WorkBudget};

// ---- test fixtures -------------------------------------------------------

/// Builds a real projection from node ids + weighted edges (mirrors the
/// `uni-algo` test helper; slot `i` = `nodes[i]`).
fn build_projection(
    nodes: &[u64],
    edges: &[(u64, u64, f64)],
    weighted: bool,
    include_reverse: bool,
) -> GraphProjection {
    let node_rows: Vec<HashMap<String, Value>> = nodes
        .iter()
        .map(|&id| HashMap::from([("id".to_string(), Value::Int(id as i64))]))
        .collect();
    let edge_rows: Vec<HashMap<String, Value>> = edges
        .iter()
        .map(|&(s, t, w)| {
            HashMap::from([
                ("source".to_string(), Value::Int(s as i64)),
                ("target".to_string(), Value::Int(t as i64)),
                ("weight".to_string(), Value::Float(w)),
            ])
        })
        .collect();
    let weight_column = if weighted { Some("weight") } else { None };
    GraphProjection::from_rows(&node_rows, &edge_rows, weight_column, include_reverse)
        .expect("from_rows should build the projection")
}

/// A session with a generous default budget, and the bound graph handle.
fn session_with(graph: GraphProjection) -> (AlgoSession, Handle) {
    let edges = graph.edge_count() as u64;
    let budget = WorkBudget::from_edge_count(edges.max(1_000));
    let arena = Arena::new(
        super::DEFAULT_ARENA_MAX_BYTES,
        super::DEFAULT_ARENA_MAX_HANDLES,
    );
    let mut session = AlgoSession::new(1, budget, arena);
    let g = session.bind_graph(Arc::new(graph));
    (session, g)
}

// ---- independent oracles -------------------------------------------------

/// Naive adjacency-list reachable set from `sources` following out-edges.
fn oracle_reachable(nodes: &[u64], edges: &[(u64, u64, f64)], sources: &[u64]) -> HashSet<u64> {
    let mut adj: HashMap<u64, Vec<u64>> = HashMap::new();
    for &(s, t, _) in edges {
        adj.entry(s).or_default().push(t);
    }
    let _ = nodes;
    let mut seen: HashSet<u64> = sources.iter().copied().collect();
    let mut q: VecDeque<u64> = sources.iter().copied().collect();
    while let Some(u) = q.pop_front() {
        for &v in adj.get(&u).map(Vec::as_slice).unwrap_or(&[]) {
            if seen.insert(v) {
                q.push_back(v);
            }
        }
    }
    seen
}

/// Naive union-find weakly-connected components; returns vid -> min-vid label.
fn oracle_wcc(nodes: &[u64], edges: &[(u64, u64, f64)]) -> HashMap<u64, u64> {
    let mut parent: HashMap<u64, u64> = nodes.iter().map(|&n| (n, n)).collect();
    fn find(parent: &mut HashMap<u64, u64>, x: u64) -> u64 {
        let p = parent[&x];
        if p == x {
            x
        } else {
            let r = find(parent, p);
            parent.insert(x, r);
            r
        }
    }
    for &(s, t, _) in edges {
        let (rs, rt) = (find(&mut parent, s), find(&mut parent, t));
        if rs != rt {
            // Union toward the smaller root so labels are the component minimum.
            let (lo, hi) = (rs.min(rt), rs.max(rt));
            parent.insert(hi, lo);
        }
    }
    nodes.iter().map(|&n| (n, find(&mut parent, n))).collect()
}

/// Naive Bellman-Ford single-source distances (sequential relaxation).
fn oracle_sssp(nodes: &[u64], edges: &[(u64, u64, f64)], source: u64) -> HashMap<u64, f64> {
    let mut dist: HashMap<u64, f64> = nodes.iter().map(|&n| (n, f64::INFINITY)).collect();
    dist.insert(source, 0.0);
    for _ in 0..nodes.len() {
        let mut changed = false;
        for &(s, t, w) in edges {
            let ds = dist[&s];
            if ds.is_finite() && ds + w < dist[&t] {
                dist.insert(t, ds + w);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    dist
}

/// Naive PageRank power iteration with dangling-mass redistribution and
/// sequential (deterministic) summation — the F-4 oracle (proposal §9.0).
fn oracle_ppr(
    nodes: &[u64],
    edges: &[(u64, u64, f64)],
    seeds: &[u64],
    alpha: f64,
    iters: usize,
) -> HashMap<u64, f64> {
    let n = nodes.len();
    let idx: HashMap<u64, usize> = nodes.iter().enumerate().map(|(i, &v)| (v, i)).collect();
    let mut out_deg = vec![0usize; n];
    let mut out_adj: Vec<Vec<usize>> = vec![Vec::new(); n];
    for &(s, t, _) in edges {
        out_deg[idx[&s]] += 1;
        out_adj[idx[&s]].push(idx[&t]);
    }
    let mut teleport = vec![0.0; n];
    for &s in seeds {
        teleport[idx[&s]] += 1.0;
    }
    let tsum: f64 = teleport.iter().sum();
    for x in &mut teleport {
        *x /= tsum;
    }
    let mut rank = teleport.clone();
    for _ in 0..iters {
        let mut next = vec![0.0; n];
        for u in 0..n {
            if out_deg[u] > 0 {
                let share = rank[u] / out_deg[u] as f64;
                for &v in &out_adj[u] {
                    next[v] += share;
                }
            }
        }
        let dm: f64 = (0..n).filter(|&u| out_deg[u] == 0).map(|u| rank[u]).sum();
        let blend = 1.0 - alpha + alpha * dm;
        for i in 0..n {
            next[i] = alpha * next[i] + blend * teleport[i];
        }
        rank = next;
    }
    nodes.iter().map(|&v| (v, rank[idx[&v]])).collect()
}

// ---- F-row differential drivers ------------------------------------------

#[test]
fn f1_reachability_matches_naive_bfs() {
    let nodes = vec![10, 20, 30, 40, 50, 60];
    let edges = vec![
        (10, 20, 1.0),
        (20, 30, 1.0),
        (30, 10, 1.0),
        (40, 50, 1.0),
        (20, 40, 1.0),
    ];
    let (mut s, g) = session_with(build_projection(&nodes, &edges, false, true));
    let reach = reachable_set(&mut s, g, &[Vid::new(10)], Direction::Out).unwrap();

    let want = oracle_reachable(&nodes, &edges, &[10]);
    let got: HashSet<u64> = {
        // Read the set back through the graph's IdMap by probing each node.
        let mut set = HashSet::new();
        for &vid in &nodes {
            let f = s.frontier(g, &[Vid::new(vid)]).unwrap();
            if s.set_intersect(reach, f)
                .and_then(|h| s.set_len(h))
                .unwrap()
                > 0
            {
                set.insert(vid);
            }
            s.free(f).unwrap();
        }
        set
    };
    assert_eq!(got, want, "reachable set must match naive BFS");
}

#[test]
fn f2_wcc_matches_union_find() {
    let nodes = vec![1, 2, 3, 4, 5, 6, 7];
    let edges = vec![
        (1, 2, 1.0),
        (2, 3, 1.0),
        (5, 6, 1.0),
        (6, 7, 1.0),
        (7, 5, 1.0),
    ];
    let (mut s, g) = session_with(build_projection(&nodes, &edges, false, true));
    let labels = wcc_labels(&mut s, g, 100).unwrap();

    // Map slot labels back to vids; two vids share a component iff equal label.
    let want = oracle_wcc(&nodes, &edges);
    // Compare the partition induced by labels vs the oracle partition.
    let label_vals = read_tensor(&s, labels);
    let mut got_groups: HashMap<u64, Vec<u64>> = HashMap::new();
    for (slot, &lab) in label_vals.iter().enumerate() {
        let vid = vid_of_slot(&nodes, slot);
        got_groups.entry(lab as u64).or_default().push(vid);
    }
    let mut oracle_groups: HashMap<u64, Vec<u64>> = HashMap::new();
    for (&vid, &root) in &want {
        oracle_groups.entry(root).or_default().push(vid);
    }
    assert_eq!(
        canonical_partition(got_groups.into_values()),
        canonical_partition(oracle_groups.into_values()),
        "WCC partition must match union-find"
    );
}

#[test]
fn f3_bellman_ford_matches_naive() {
    let nodes = vec![0, 1, 2, 3, 4];
    let edges = vec![
        (0, 1, 2.0),
        (0, 2, 5.0),
        (1, 2, 1.0),
        (2, 3, 3.0),
        (1, 3, 7.0),
    ];
    let (mut s, g) = session_with(build_projection(&nodes, &edges, true, false));
    let dist = bellman_ford(&mut s, g, Vid::new(0), nodes.len()).unwrap();
    let got = read_tensor(&s, dist);

    let want = oracle_sssp(&nodes, &edges, 0);
    for (slot, &d) in got.iter().enumerate() {
        let vid = vid_of_slot(&nodes, slot);
        let expected = want[&vid];
        if expected.is_finite() {
            assert!(
                (d - expected).abs() < 1e-9,
                "dist to {vid}: got {d}, want {expected}"
            );
        } else {
            assert!(d.is_infinite(), "dist to {vid} should be +inf, got {d}");
        }
    }
}

#[test]
fn f4_ppr_matches_power_iteration_oracle() {
    let nodes = vec![0, 1, 2, 3, 4, 5];
    let edges = vec![
        (0, 1, 1.0),
        (1, 2, 1.0),
        (2, 0, 1.0),
        (2, 3, 1.0),
        (3, 4, 1.0),
        (4, 3, 1.0),
        // node 5 is a dangling sink (no out-edges) reachable from 4.
        (4, 5, 1.0),
    ];
    let alpha = 0.85;
    let iters = 100;
    let (mut s, g) = session_with(build_projection(&nodes, &edges, false, false));
    let rank = personalized_pagerank(&mut s, g, &[Vid::new(0)], alpha, iters, 1e-12, true).unwrap();
    let got = read_tensor(&s, rank);

    let want = oracle_ppr(&nodes, &edges, &[0], alpha, iters);
    // F-4 tolerance: 1e-9 abs / 1e-7 rel vs the sequential oracle (proposal §9.0).
    for (slot, &score) in got.iter().enumerate() {
        let vid = vid_of_slot(&nodes, slot);
        let expected = want[&vid];
        let tol = 1e-9 + 1e-7 * expected.abs();
        assert!(
            (score - expected).abs() < tol,
            "PPR score for {vid}: got {score}, want {expected}"
        );
    }
}

// ---- W4 · i64 exact path-counting (F-9) ----------------------------------

/// Counts length-`k` walks from a source by repeated i64 `spmv` over the
/// LinearAlgebra semiring, and proves exactness beyond `2^53` where an f64
/// accumulator would round (proposal §4.2 / F-9).
#[test]
fn i64_spmv_counts_paths_exactly_beyond_f64() {
    use super::session::{Direction, ReduceOp, Semiring};
    use super::value::{DType, Scalar};

    // A layered complete-bipartite DAG: L0={source}, each L_i (width nodes) fully
    // connects to L_{i+1}. #paths source→each node of L_i is width^(i-1), so the
    // total over the last layer is width^(layers-1). With width=64 (=2^6) and 10
    // layers that is 64^9 = 2^54 — one bit past f64's exact-integer range.
    let width = 64u64;
    let layers = 10usize;
    let mut nodes: Vec<u64> = Vec::new();
    let mut edges: Vec<(u64, u64, f64)> = Vec::new();
    let mut layer_nodes: Vec<Vec<u64>> = Vec::new();
    let mut next_id = 0u64;
    for li in 0..layers {
        let count = if li == 0 { 1 } else { width };
        let mut this: Vec<u64> = Vec::new();
        for _ in 0..count {
            nodes.push(next_id);
            this.push(next_id);
            next_id += 1;
        }
        layer_nodes.push(this);
    }
    for li in 0..layers - 1 {
        for &u in &layer_nodes[li] {
            for &v in &layer_nodes[li + 1] {
                edges.push((u, v, 1.0));
            }
        }
    }
    // Exact oracle: #paths reaching the last layer = width^(layers-1), summed
    // over that layer's nodes = width^(layers-1) * width = width^(layers-1)*... .
    // #paths from source to *each* last-layer node = width^(layers-2); total over
    // the last layer = width^(layers-1).
    let total_paths: u128 = (width as u128).pow((layers - 1) as u32);

    let (mut s, g) = session_with(build_projection(&nodes, &edges, false, false));
    // Seed an i64 one-hot at the source (slot 0), then spmv `layers-1` times.
    let seed_set = s.frontier(g, &[Vid::new(0)]).unwrap();
    let zero = s.zero_map(g, DType::I64).unwrap();
    let mut counts = s.scatter(zero, seed_set, Scalar::I64(1)).unwrap();
    s.free(zero).unwrap();
    s.free(seed_set).unwrap();
    for _ in 0..layers - 1 {
        let next = s
            .spmv(g, counts, Semiring::LinearAlgebra, Direction::Out, None)
            .unwrap();
        s.free(counts).unwrap();
        counts = next;
    }
    // Sum the final counts exactly via an i64 reduce.
    let got = match s.reduce(counts, ReduceOp::Sum, None).unwrap() {
        Scalar::I64(v) => v as u128,
        other => panic!("i64 reduce must return I64, got {other:?}"),
    };
    s.free(counts).unwrap();
    assert_eq!(
        got, total_paths,
        "i64 path count must match the exact oracle"
    );
    // Guard the premise: this count exceeds the f64 exact-integer range.
    assert!(
        total_paths > (1u128 << 53),
        "test must exercise counts beyond 2^53, got {total_paths}"
    );
}

/// f64-only kernels reject an i64 tensor with a typed shape mismatch (0x862),
/// never a panic — the dead code lights up (proposal §12 / E2).
#[test]
fn f64_kernels_reject_i64_with_shape_mismatch() {
    use super::session::MapOp;
    use super::value::DType;

    let nodes = vec![0, 1, 2];
    let edges = vec![(0, 1, 1.0)];
    let (mut s, g) = session_with(build_projection(&nodes, &edges, false, false));
    let imap = s.zero_map(g, DType::I64).unwrap();
    assert_eq!(
        s.map_apply(imap, MapOp::Recip).unwrap_err().code,
        super::error::SHAPE_MISMATCH
    );
}

// ---- W3 · emit schema validation (0x869) ---------------------------------

/// A session declaring its output columns rejects an emit that names an
/// undeclared column, repeats one, or omits a declared one (proposal §4.6).
#[test]
fn emit_validates_against_declared_columns() {
    let nodes = vec![0, 1, 2];
    let edges = vec![(0, 1, 1.0), (1, 2, 1.0)];
    let mk_session = |cols: Vec<String>| {
        let arena = Arena::new(
            super::DEFAULT_ARENA_MAX_BYTES,
            super::DEFAULT_ARENA_MAX_HANDLES,
        );
        let mut s = AlgoSession::new(1, WorkBudget::from_edge_count(1_000), arena)
            .with_expected_columns(cols);
        let g = s.bind_graph(Arc::new(build_projection(&nodes, &edges, false, false)));
        let m = s.zero_map(g, super::value::DType::F64).unwrap();
        (s, m)
    };

    // Undeclared name -> 0x869.
    let (mut s, m) = mk_session(vec!["score".to_string()]);
    assert_eq!(
        s.emit(&[("wrong", m)]).unwrap_err().code,
        super::error::EMIT_SCHEMA_MISMATCH
    );
    // The declared name succeeds.
    let (mut s, m) = mk_session(vec!["score".to_string()]);
    s.emit(&[("score", m)]).expect("declared column emits");

    // Omitting a declared column -> 0x869.
    let (mut s, m) = mk_session(vec!["a".to_string(), "b".to_string()]);
    assert_eq!(
        s.emit(&[("a", m)]).unwrap_err().code,
        super::error::EMIT_SCHEMA_MISMATCH
    );

    // Repeating a column -> 0x869.
    let (mut s, m) = mk_session(vec!["score".to_string()]);
    assert_eq!(
        s.emit(&[("score", m), ("score", m)]).unwrap_err().code,
        super::error::EMIT_SCHEMA_MISMATCH
    );
}

// ---- W2 · §5.2 incomplete-reason reachability ----------------------------

/// P0-7 (reason 0x866): a non-converging PPR with `allow_partial = false`
/// raises `IterationLimit`, not a silent last-iterate.
#[test]
fn ppr_non_convergence_is_iteration_limit_not_silent() {
    // A two-cycle: the power iteration keeps moving mass, so two iterations at
    // a 1e-15 tolerance cannot converge.
    let nodes = vec![0, 1, 2, 3];
    let edges = vec![(0, 1, 1.0), (1, 2, 1.0), (2, 3, 1.0), (3, 0, 1.0)];
    let (mut s, g) = session_with(build_projection(&nodes, &edges, false, false));
    let err = personalized_pagerank(&mut s, g, &[Vid::new(0)], 0.85, 2, 1e-15, false)
        .expect_err("2 iters at 1e-15 tol must not converge");
    assert_eq!(
        err.code,
        super::error::ITERATION_LIMIT,
        "non-convergence must surface as 0x866, got {err}"
    );
    // The same run with anytime semantics returns the last iterate instead.
    let (mut s2, g2) = session_with(build_projection(&nodes, &edges, false, false));
    personalized_pagerank(&mut s2, g2, &[Vid::new(0)], 0.85, 2, 1e-15, true)
        .expect("allow_partial = true returns the last iterate");
}

/// P0-7 (reason 0x867): a session whose wall-clock deadline has already passed
/// aborts the first charged kernel with `Timeout`, distinct from 0x866/0x865.
#[test]
fn expired_deadline_aborts_with_timeout() {
    let nodes = vec![0, 1, 2];
    let edges = vec![(0, 1, 1.0), (1, 2, 1.0)];
    let edge_count = edges.len() as u64;
    let budget = WorkBudget::from_edge_count(edge_count.max(1_000));
    let arena = Arena::new(
        super::DEFAULT_ARENA_MAX_BYTES,
        super::DEFAULT_ARENA_MAX_HANDLES,
    );
    // A deadline one second in the past: the very first `charge` must trip it.
    let past = std::time::Instant::now() - std::time::Duration::from_secs(1);
    let mut s = AlgoSession::new(1, budget, arena).with_deadline(Some(past));
    let g = s.bind_graph(Arc::new(build_projection(&nodes, &edges, false, false)));
    let err = personalized_pagerank(&mut s, g, &[Vid::new(0)], 0.85, 100, 1e-12, true)
        .expect_err("an expired deadline must abort the invocation");
    assert_eq!(
        err.code,
        super::error::TIMEOUT,
        "an expired deadline must surface as 0x867, got {err}"
    );
}

/// Naive normalized power iteration for eigenvector centrality (F-5 oracle).
fn oracle_eigenvector(nodes: &[u64], edges: &[(u64, u64, f64)], iters: usize) -> HashMap<u64, f64> {
    let n = nodes.len();
    let idx: HashMap<u64, usize> = nodes.iter().enumerate().map(|(i, &v)| (v, i)).collect();
    // in_adj[v] = list of u with u -> v (importance flows in along edges).
    let mut in_adj: Vec<Vec<usize>> = vec![Vec::new(); n];
    for &(s, t, _) in edges {
        in_adj[idx[&t]].push(idx[&s]);
    }
    let l2 = |x: &[f64]| -> f64 { x.iter().map(|v| v * v).sum::<f64>().sqrt() };
    let mut x = vec![1.0 / (n as f64).sqrt(); n];
    for _ in 0..iters {
        let mut next = vec![0.0; n];
        for v in 0..n {
            for &u in &in_adj[v] {
                next[v] += x[u];
            }
        }
        let norm = l2(&next);
        if norm > 0.0 {
            for v in &mut next {
                *v /= norm;
            }
        }
        x = next;
    }
    nodes.iter().map(|&v| (v, x[idx[&v]])).collect()
}

/// Naive synchronous k-core peeling (F-7 oracle). Degree counts surviving
/// out-neighbors + in-neighbors with multiplicity, mirroring the kernel.
fn oracle_kcore(nodes: &[u64], edges: &[(u64, u64, f64)], k: i64) -> HashSet<u64> {
    let mut alive: HashSet<u64> = nodes.iter().copied().collect();
    loop {
        let mut to_remove = Vec::new();
        for &v in &alive {
            let mut deg = 0i64;
            for &(s, t, _) in edges {
                if s == v && alive.contains(&t) {
                    deg += 1;
                }
                if t == v && alive.contains(&s) {
                    deg += 1;
                }
            }
            if deg < k {
                to_remove.push(v);
            }
        }
        if to_remove.is_empty() {
            break;
        }
        for v in to_remove {
            alive.remove(&v);
        }
    }
    alive
}

#[test]
fn f7_kcore_matches_naive_peeling() {
    // A triangle {1,2,3} (mutually linked) plus a pendant 4 and isolated 5.
    let nodes = vec![1, 2, 3, 4, 5];
    let edges = vec![
        (1, 2, 1.0),
        (2, 1, 1.0),
        (2, 3, 1.0),
        (3, 2, 1.0),
        (3, 1, 1.0),
        (1, 3, 1.0),
        (3, 4, 1.0), // pendant
    ];
    // k = 2 (each triangle member has out+in degree 4 ≥ 2; 4 and 5 peel).
    let (mut s, g) = session_with(build_projection(&nodes, &edges, false, true));
    let core = k_core(&mut s, g, 2, 100).unwrap();

    let want = oracle_kcore(&nodes, &edges, 2);
    let got: HashSet<u64> = {
        let mut set = HashSet::new();
        for &vid in &nodes {
            let f = s.frontier(g, &[Vid::new(vid)]).unwrap();
            if s.set_intersect(core, f).and_then(|h| s.set_len(h)).unwrap() > 0 {
                set.insert(vid);
            }
            s.free(f).unwrap();
        }
        set
    };
    assert_eq!(got, want, "k-core must match naive peeling");
    assert_eq!(
        got,
        HashSet::from([1, 2, 3]),
        "only the triangle survives 2-core"
    );
}

#[test]
fn f5_eigenvector_matches_power_iteration_oracle() {
    let nodes = vec![0, 1, 2, 3];
    // A directed graph with a clear dominant node (3 has two in-edges).
    let edges = vec![(0, 3, 1.0), (1, 3, 1.0), (2, 0, 1.0), (0, 1, 1.0)];
    let iters = 200;
    let (mut s, g) = session_with(build_projection(&nodes, &edges, false, true));
    let cent = eigenvector_centrality(&mut s, g, iters, 1e-12).unwrap();
    let got = read_tensor(&s, cent);

    let want = oracle_eigenvector(&nodes, &edges, iters);
    for (slot, &c) in got.iter().enumerate() {
        let vid = vid_of_slot(&nodes, slot);
        let expected = want[&vid];
        assert!(
            (c - expected).abs() < 1e-6,
            "eigenvector centrality for {vid}: got {c}, want {expected}"
        );
    }
}

#[test]
fn m2_ppr_mass_is_conserved() {
    // Metamorphic M-2: PPR scores sum to 1 even with a dangling node.
    let nodes = vec![0, 1, 2, 3];
    let edges = vec![(0, 1, 1.0), (1, 2, 1.0)]; // 2 and 3 are dangling
    let (mut s, g) = session_with(build_projection(&nodes, &edges, false, false));
    let rank = personalized_pagerank(&mut s, g, &[Vid::new(0)], 0.85, 200, 1e-14, true).unwrap();
    let total: f64 = read_tensor(&s, rank).iter().sum();
    assert!(
        (total - 1.0).abs() < 1e-9,
        "PPR mass must be conserved, got {total}"
    );
}

#[test]
fn e5_ppr_is_deterministic_across_runs() {
    // §5.3 through the kernel stack: identical inputs → bitwise-identical output.
    let nodes = vec![0, 1, 2, 3, 4];
    let edges = vec![
        (0, 1, 1.0),
        (1, 2, 1.0),
        (2, 3, 1.0),
        (3, 0, 1.0),
        (2, 4, 1.0),
    ];
    let run = || {
        let (mut s, g) = session_with(build_projection(&nodes, &edges, false, false));
        let rank = personalized_pagerank(&mut s, g, &[Vid::new(0)], 0.85, 50, 1e-12, true).unwrap();
        read_tensor(&s, rank)
    };
    assert_eq!(
        run().to_bits_vec(),
        run().to_bits_vec(),
        "PPR must be bitwise-reproducible"
    );
}

// ---- handle-security tests (H-1..H-7) ------------------------------------

#[test]
fn h1_forged_handle_is_typed_error_not_panic() {
    let (mut s, _g) = session_with(build_projection(&[0, 1], &[(0, 1, 1.0)], false, false));
    // A handful of forged raw u64s into every accessor: all typed errors.
    for raw in [0u64, 0x4141_4141, u64::MAX, 0xDEAD_BEEF_0000_0000] {
        let h = Handle::from_u64(raw);
        assert!(s.set_len(h).is_err());
        assert!(s.is_empty(h).is_err());
        assert!(s.l1_diff(h, h).is_err());
        assert!(s.reduce(h, super::ReduceOp::Sum, None).is_err());
    }
}

#[test]
fn h2_use_after_free_is_stale() {
    let (mut s, g) = session_with(build_projection(&[0, 1, 2], &[(0, 1, 1.0)], false, false));
    let t = s.degrees(g, Direction::Out).unwrap();
    s.free(t).unwrap();
    // Using the freed handle fails the generation check (0x860).
    let err = s.reduce(t, super::ReduceOp::Sum, None).unwrap_err();
    assert_eq!(err.code, super::error::STALE_HANDLE);
    // Double free also fails.
    assert!(s.free(t).is_err());
}

#[test]
fn h3_cross_session_handle_rejected() {
    let (mut a, ga) = session_with(build_projection(&[0, 1], &[(0, 1, 1.0)], false, false));
    let ta = a.degrees(ga, Direction::Out).unwrap();

    // A second session with a *different* epoch must reject session A's handle.
    let graph_b = build_projection(&[0, 1], &[(0, 1, 1.0)], false, false);
    let budget = WorkBudget::from_edge_count(1_000);
    let arena = Arena::new(
        super::DEFAULT_ARENA_MAX_BYTES,
        super::DEFAULT_ARENA_MAX_HANDLES,
    );
    let mut b = AlgoSession::new(2, budget, arena);
    let _gb = b.bind_graph(Arc::new(graph_b));

    let err = b.reduce(ta, super::ReduceOp::Sum, None).unwrap_err();
    assert_eq!(err.code, super::error::EPOCH_MISMATCH);
}

#[test]
fn h4_kind_mismatch_is_typed_error() {
    let (mut s, g) = session_with(build_projection(&[0, 1], &[(0, 1, 1.0)], false, false));
    let set = s.frontier(g, &[Vid::new(0)]).unwrap();
    // A VertexSet handle where a Tensor is expected → kind mismatch (0x861).
    let err = s.reduce(set, super::ReduceOp::Sum, None).unwrap_err();
    assert_eq!(err.code, super::error::KIND_MISMATCH);
    // And a Tensor where a set is expected.
    let t = s.degrees(g, Direction::Out).unwrap();
    assert_eq!(s.set_len(t).unwrap_err().code, super::error::KIND_MISMATCH);
}

#[test]
fn h7_kernels_are_pure() {
    // Purity invariant (§4.1): a kernel returns a NEW handle and leaves its
    // inputs bit-unchanged. `scatter` once violated this in the design.
    let (mut s, g) = session_with(build_projection(&[0, 1, 2], &[(0, 1, 1.0)], false, false));
    let base = s.degrees(g, Direction::Out).unwrap();
    let before = read_tensor(&s, base).to_bits_vec();
    let front = s.frontier(g, &[Vid::new(2)]).unwrap();
    let out = s.scatter(base, front, Scalar::F64(9.0)).unwrap();
    assert_ne!(out, base, "scatter must return a fresh handle");
    let after = read_tensor(&s, base).to_bits_vec();
    assert_eq!(before, after, "scatter must not mutate its input map");
    // The new map reflects the scatter.
    assert_eq!(read_tensor(&s, out)[2], 9.0);
}

#[test]
fn budget_bounds_readonly_reduce_and_l1diff_loops() {
    // Regression (review H3): read-only kernels used to charge zero work, so a
    // `loop { reduce_sum }` / `loop { l1_diff }` did unbounded O(V) native work.
    // They now charge |V| and fail closed under a tiny budget.
    let nodes: Vec<u64> = (0..20).collect();
    let edges: Vec<(u64, u64, f64)> = (0..19).map(|i| (i, i + 1, 1.0)).collect();
    let graph = build_projection(&nodes, &edges, false, false);
    let mut s = AlgoSession::new(11, WorkBudget::new(50), Arena::new(1 << 20, 4_096));
    let g = s.bind_graph(Arc::new(graph));
    let m = s.degrees(g, Direction::Out).unwrap(); // charges 20; 30 left

    let mut hit = false;
    for _ in 0..1_000 {
        if s.reduce(m, super::ReduceOp::Sum, None).is_err() {
            hit = true;
            break;
        }
    }
    assert!(
        hit,
        "an unbounded reduce loop must exhaust the native-work budget"
    );

    // And l1_diff is metered too (the per-iteration convergence test).
    let mut s2 = AlgoSession::new(12, WorkBudget::new(50), Arena::new(1 << 20, 4_096));
    let g2 = s2.bind_graph(Arc::new(build_projection(&nodes, &edges, false, false)));
    let a = s2.degrees(g2, Direction::Out).unwrap();
    let b = s2.degrees(g2, Direction::In).unwrap();
    let mut hit2 = false;
    for _ in 0..1_000 {
        if s2.l1_diff(a, b).is_err() {
            hit2 = true;
            break;
        }
    }
    assert!(hit2, "an unbounded l1_diff loop must exhaust the budget");
}

#[test]
fn weighted_in_direction_spmv_does_not_panic() {
    // Regression (review H1): weighted In-direction spmv used to read the OUT
    // weight array with an in-neighbor index → OOB panic when in_degree >
    // out_degree. Slot 2 here has in_degree 2, out_degree 0 (out_offsets[2]==E),
    // which previously indexed out_weights[E] out of bounds.
    let nodes = vec![0u64, 1, 2];
    let edges = vec![(0, 2, 0.5), (1, 2, 0.25)]; // both point at 2; 2 is a sink
    let (mut s, g) = session_with(build_projection(&nodes, &edges, true, true));
    let ones = {
        let z = s.zero_map(g, super::DType::F64).unwrap();
        s.map_apply(z, super::MapOp::AxPlusB(0.0, 1.0)).unwrap()
    };
    // Must not panic; In-direction is treated as unweighted (w=1).
    let out = s
        .spmv(g, ones, super::Semiring::LinearAlgebra, Direction::In, None)
        .expect("weighted In-direction spmv must not panic");
    let vals = read_tensor(&s, out);
    assert_eq!(vals.len(), 3);
}

#[test]
fn budget_exhaustion_stops_runaway_expand() {
    // P0-3/L-1 at the kernel level: a tiny budget makes a repeated expand fail
    // closed with the Exhausted code, not run forever.
    let nodes: Vec<u64> = (0..50).collect();
    let edges: Vec<(u64, u64, f64)> = (0..49).map(|i| (i, i + 1, 1.0)).collect();
    let graph = build_projection(&nodes, &edges, false, true);
    let mut s = AlgoSession::new(7, WorkBudget::new(5), Arena::new(1 << 20, 4_096));
    let g = s.bind_graph(Arc::new(graph));
    let seed = s.frontier(g, &[Vid::new(0)]).unwrap();
    let mut frontier = s.frontier(g, &[Vid::new(0)]).unwrap();
    let mut hit = false;
    for _ in 0..1_000 {
        match s.expand(g, frontier, Direction::Out, None) {
            Ok(next) => frontier = next,
            Err(e) => {
                assert_eq!(e.code, super::error::BUDGET_EXHAUSTED);
                hit = true;
                break;
            }
        }
    }
    assert!(hit, "a tiny budget must eventually stop the expand loop");
    let _ = seed;
}

#[test]
fn handle_kind_tag_is_stable() {
    // The packed kind tags are ABI; a reorder would silently break handles.
    assert_eq!(HandleKind::VertexSet as u8, 0);
    assert_eq!(HandleKind::Tensor as u8, 1);
    assert_eq!(HandleKind::Graph as u8, 2);
}

// ---- helpers -------------------------------------------------------------

/// Reads a tensor handle's values back out (test-only introspection).
fn read_tensor(s: &AlgoSession, _h: Handle) -> TensorView {
    // The session does not expose tensors directly; we round-trip through the
    // public reduce/emit path instead by re-binding via a small accessor added
    // for tests. Here we use the emit sink as the read channel.
    TensorView::from_handle(s, _h)
}

/// Returns the vid at slot `i` given the node insertion order.
fn vid_of_slot(nodes: &[u64], slot: usize) -> u64 {
    nodes[slot]
}

/// Canonicalizes a partition (set of groups) for order-insensitive comparison.
fn canonical_partition(groups: impl Iterator<Item = Vec<u64>>) -> Vec<Vec<u64>> {
    let mut out: Vec<Vec<u64>> = groups
        .map(|mut g| {
            g.sort_unstable();
            g
        })
        .collect();
    out.sort();
    out
}

/// A read-only view of a tensor's `f64` values, for oracle comparison.
struct TensorView(Vec<f64>);

impl TensorView {
    fn from_handle(s: &AlgoSession, h: Handle) -> Self {
        Self(s.tensor_values_for_test(h))
    }
    fn to_bits_vec(&self) -> Vec<u64> {
        self.0.iter().map(|x| x.to_bits()).collect()
    }
}

impl std::ops::Deref for TensorView {
    type Target = [f64];
    fn deref(&self) -> &[f64] {
        &self.0
    }
}
