// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Runnable repros for 8 verified correctness findings in `uni-algo`.
//!
//! Each test drives the REAL public algorithm surface
//! ([`uni_algo::algo::algorithms`]) over a real [`GraphProjection`]
//! built via the public [`GraphProjection::from_rows`] constructor (the
//! same path the Cypher adapters use), and asserts on the OBSERVED
//! (currently-buggy) behavior. Where the correct-behavior assertion
//! would fail today, the buggy value is asserted and annotated with a
//! `BUG:` comment naming the offending `file:line`.

use std::collections::HashMap;

use uni_algo::algo::GraphProjection;
use uni_algo::algo::algorithms::{
    AStar, AStarConfig, Algorithm, AllPairsShortestPath, AllPairsShortestPathConfig, Betweenness,
    BetweennessConfig, BidirectionalDijkstra, BidirectionalDijkstraConfig, Dijkstra,
    DijkstraConfig, ElementaryCircuits, ElementaryCircuitsConfig, KCore, KCoreConfig,
    MinimumSpanningTree, MstConfig,
};
use uni_common::Value;
use uni_common::core::id::Vid;

/// Build a real [`GraphProjection`] from node ids and weighted edges.
///
/// Nodes are inserted in the given order, so slot indices follow the id
/// order (`nodes[i]` -> slot `i`). Edges preserve insertion order within
/// each source (so parallel edges keep their CSR order). When `weighted`
/// is false the weights are ignored and the projection carries no
/// weights; when `include_reverse` is true the in-CSR (transpose) is
/// built alongside the out-CSR.
fn build_projection(
    nodes: &[u64],
    edges: &[(u64, u64, f64)],
    weighted: bool,
    include_reverse: bool,
) -> GraphProjection {
    let node_rows: Vec<HashMap<String, Value>> = nodes
        .iter()
        .map(|&id| {
            let mut m = HashMap::new();
            m.insert("id".to_string(), Value::Int(id as i64));
            m
        })
        .collect();

    let edge_rows: Vec<HashMap<String, Value>> = edges
        .iter()
        .map(|&(s, t, w)| {
            let mut m = HashMap::new();
            m.insert("source".to_string(), Value::Int(s as i64));
            m.insert("target".to_string(), Value::Int(t as i64));
            m.insert("weight".to_string(), Value::Float(w));
            m
        })
        .collect();

    let weight_column = if weighted { Some("weight") } else { None };
    GraphProjection::from_rows(&node_rows, &edge_rows, weight_column, include_reverse)
        .expect("from_rows should build the projection")
}

/// [1] bidirectional_dijkstra.rs:145 — backward step takes the FIRST
/// parallel edge weight, overestimating dist_bwd and stopping early with
/// a non-minimal distance.
#[test]
fn repro_bidirectional_dijkstra_parallel_edges_overestimate() {
    // s=0, m=1, t=2, d1=3, d2=4, d3=5.
    // s->m(1); two parallel m->t edges with weight 10 FIRST then 1;
    // three dummy s->d* edges make heap_fwd larger so the 2nd iteration
    // takes the backward branch (heap_fwd.len() > heap_bwd.len()).
    let nodes = [0u64, 1, 2, 3, 4, 5];
    let edges = [
        (0u64, 1u64, 1.0),
        (1, 2, 10.0), // parallel edge, larger weight FIRST in CSR order
        (1, 2, 1.0),  // parallel edge, true minimum weight
        (0, 3, 5.0),
        (0, 4, 5.0),
        (0, 5, 5.0),
    ];
    let graph = build_projection(&nodes, &edges, true, true);

    let cfg = BidirectionalDijkstraConfig {
        source: Vid::from(0),
        target: Vid::from(2),
    };
    let result = BidirectionalDijkstra::run(&graph, cfg);

    // True shortest path s->m->t = 1 + 1 = 2.0.
    // FIXED: the backward search now resolves m->t via the MINIMUM parallel
    // edge weight (1), so dist_bwd is exact and the true distance 2.0 is
    // returned (fix for
    // crates/uni-algo/src/algo/algorithms/bidirectional_dijkstra.rs).
    assert_eq!(
        result.distance,
        Some(2.0),
        "backward scan must take the min parallel-edge weight (true distance 2.0)"
    );
}

/// [2] kcore.rs:110 — initial degree counts reciprocal edges twice but
/// the peeling loop dedups neighbors, inflating core numbers.
#[test]
fn repro_kcore_reciprocal_edges_inflated_core() {
    // Star: center c=0 reciprocally linked to leaves a=1, b=2, d=3.
    let nodes = [0u64, 1, 2, 3];
    let edges = [
        (0u64, 1u64, 1.0),
        (1, 0, 1.0),
        (0, 2, 1.0),
        (2, 0, 1.0),
        (0, 3, 1.0),
        (3, 0, 1.0),
    ];
    let graph = build_projection(&nodes, &edges, false, true);

    let result = KCore::run(&graph, KCoreConfig { k: None });
    let core: HashMap<Vid, u32> = result.core_numbers.into_iter().collect();
    let center = *core.get(&Vid::from(0)).expect("center core number");

    // A consistent simple-undirected star has core number 1 everywhere.
    // FIXED: the initial degree now counts UNIQUE neighbors (matching the
    // peeling loop), so reciprocal edges are no longer double-counted and the
    // center's core number is 1 (fix for
    // crates/uni-algo/src/algo/algorithms/kcore.rs).
    assert_eq!(center, 1, "star center must have core number 1");
}

/// [3] mst.rs:55 — Kruskal keeps an edge only when `u < v` (slot order)
/// and never normalizes, silently dropping higher->lower directed edges.
#[test]
fn repro_mst_drops_high_to_low_directed_edge() {
    // Two vertices; single directed edge from the higher slot to the
    // lower slot. Cypher MST adapter uses include_reverse=false.
    let nodes = [1u64, 2]; // Vid 1 -> slot 0, Vid 2 -> slot 1
    let edges = [(2u64, 1u64, 5.0)]; // slot 1 -> slot 0
    let graph = build_projection(&nodes, &edges, true, false);

    let result = MinimumSpanningTree::run(&graph, MstConfig {});

    // Connected 2-node graph: the MST is the single edge, weight 5.0.
    // FIXED: edges are now normalized to (min, max) instead of dropping the
    // higher->lower direction, so the only edge is retained (fix for
    // crates/uni-algo/src/algo/algorithms/mst.rs).
    assert_eq!(
        result.edges.len(),
        1,
        "the single connecting edge must be kept"
    );
    assert_eq!(result.total_weight, 5.0, "MST total weight must be 5.0");
}

/// [4] astar.rs:154 — heap key is raw `f64::to_bits()`, which mis-orders
/// negative f-scores; a negative heuristic buries the optimal node and
/// A* returns a suboptimal distance.
#[test]
fn repro_astar_negative_heuristic_suboptimal() {
    // 0->1(1), 1->3(1): optimal path cost 2 via node 1.
    // 0->2(1), 2->3(5): worse path cost 6 via node 2.
    let nodes = [0u64, 1, 2, 3];
    let edges = [(0u64, 1u64, 1.0), (1, 3, 1.0), (0, 2, 1.0), (2, 3, 5.0)];
    let graph = build_projection(&nodes, &edges, true, false);

    let mut heuristic = HashMap::new();
    heuristic.insert(Vid::from(1), -10.0); // negative f-score for node 1
    heuristic.insert(Vid::from(2), 0.0);
    heuristic.insert(Vid::from(3), 0.0);
    let cfg = AStarConfig {
        source: Vid::from(0),
        target: Vid::from(3),
        heuristic,
    };
    let result = AStar::run(&graph, cfg);

    // True shortest distance is 2.0 (0->1->3).
    // FIXED: the heap is now ordered by `f64::total_cmp` instead of raw
    // `to_bits()`, so the negative f-score at node 1 is popped in correct
    // numeric priority and the optimal 0->1->3 path is found (fix for
    // crates/uni-algo/src/algo/algorithms/astar.rs).
    assert_eq!(
        result.distance,
        Some(2.0),
        "A* must return the optimal distance 2.0 with a negative heuristic"
    );
}

/// [5] dijkstra.rs:128 — the max_distance cutoff only skips expansion but
/// still returns the over-budget node's already-relaxed distance.
#[test]
fn repro_dijkstra_max_distance_returns_over_budget_node() {
    // source 0, single edge 0->1 weight 3.0, budget 2.0.
    let nodes = [0u64, 1];
    let edges = [(0u64, 1u64, 3.0)];
    let graph = build_projection(&nodes, &edges, true, false);

    let cfg = DijkstraConfig {
        source: Vid::from(0),
        target: None,
        max_distance: Some(2.0),
    };
    let result = Dijkstra::run(&graph, cfg).expect("dijkstra with non-negative weights");
    let dist: HashMap<Vid, f64> = result.distances.into_iter().collect();

    // Node 1 is at distance 3.0 > max_distance 2.0 and must NOT appear.
    // FIXED: over-budget distances are now filtered out of the SSSP rows, so
    // node 1 is absent (fix for
    // crates/uni-algo/src/algo/algorithms/dijkstra.rs).
    assert_eq!(
        dist.get(&Vid::from(1)),
        None,
        "over-budget node 1 (dist 3.0 > max 2.0) must be excluded"
    );
    // The source itself (distance 0.0 <= 2.0) is still reported.
    assert_eq!(
        dist.get(&Vid::from(0)),
        Some(&0.0),
        "source must remain in output"
    );
}

/// [6] elementary_circuits.rs:153 — depth truncation leaves a node
/// blocked without unblocking, so a shorter budget-fitting cycle through
/// it is silently missed.
#[test]
fn repro_elementary_circuits_depth_truncation_blocks_shorter_cycle() {
    // One SCC: 0->1->2->3->0 (len 4) and 0->2->3->0 (len 3).
    // Edge order matters: 0's out-neighbors are [1, 2] (1 explored first).
    let nodes = [0u64, 1, 2, 3];
    let edges = [
        (0u64, 1u64, 1.0),
        (1, 2, 1.0),
        (0, 2, 1.0),
        (2, 3, 1.0),
        (3, 0, 1.0),
    ];
    let graph = build_projection(&nodes, &edges, false, false);

    let cfg = ElementaryCircuitsConfig {
        min_length: 2,
        max_length: 3,
        limit: 1000,
    };
    let result = ElementaryCircuits::run(&graph, cfg);

    // Exactly one cycle fits max_length 3: [0, 2, 3]. The len-4 cycle is
    // correctly excluded.
    // FIXED: a node left unexplored purely due to the depth bound is now
    // unblocked, so the direct edge 0->2 is re-explored and cycle 0->2->3->0 is
    // found (fix for
    // crates/uni-algo/src/algo/algorithms/elementary_circuits.rs).
    assert_eq!(
        result.cycles.len(),
        1,
        "the in-budget cycle 0->2->3->0 must be found"
    );
    let cycle_slots: Vec<u64> = result.cycles[0].iter().map(|v| v.as_u64()).collect();
    assert_eq!(
        cycle_slots,
        vec![0, 2, 3],
        "the found cycle must be [0, 2, 3]"
    );
}

/// [7] apsp.rs:51 — the weighted branch filters `dist > 0.0`, dropping a
/// genuinely-reachable target whose shortest-path weight is exactly 0.0.
#[test]
fn repro_apsp_zero_weight_target_dropped() {
    // Two vertices; single zero-weight edge 0->1.
    let nodes = [0u64, 1];
    let edges = [(0u64, 1u64, 0.0)];
    let graph = build_projection(&nodes, &edges, true, false);

    let result = AllPairsShortestPath::run(&graph, AllPairsShortestPathConfig);
    let has_pair = result
        .distances
        .iter()
        .any(|&(s, t, _)| s == Vid::from(0) && t == Vid::from(1));

    // Vertex 1 is reachable from 0 at cumulative distance 0.0 and is
    // distinct from the source, so (0, 1, 0.0) should be present.
    // FIXED: source exclusion is now by slot index (not `dist > 0.0`), so the
    // legitimately zero-cost target is retained (fix for
    // crates/uni-algo/src/algo/algorithms/apsp.rs).
    assert!(
        has_pair,
        "reachable zero-weight pair (0, 1) must be present"
    );
    // The source must NOT report a self-pair (0, 0).
    let has_self = result
        .distances
        .iter()
        .any(|&(s, t, _)| s == Vid::from(0) && t == Vid::from(0));
    assert!(!has_self, "source self-pair (0, 0) must be excluded");
}

/// [8] betweenness.rs:130 — sampled Brandes sums only k sources without
/// the n/k rescaling, biasing the estimate low by ~k/n.
#[test]
fn repro_betweenness_sampling_missing_rescale() {
    // Directed 4-cycle 0->1->2->3->0. By rotational symmetry every
    // single-source contribution to the total is identical, so summing
    // any k sources yields exactly (k/n) of the exact total.
    let nodes = [0u64, 1, 2, 3];
    let edges = [(0u64, 1u64, 1.0), (1, 2, 1.0), (2, 3, 1.0), (3, 0, 1.0)];
    let graph = build_projection(&nodes, &edges, false, false);

    let exact = Betweenness::run(
        &graph,
        BetweennessConfig {
            normalize: false,
            sampling_size: None,
        },
    );
    let exact_total: f64 = exact.scores.iter().map(|&(_, s)| s).sum();

    // k = 2 of n = 4 sources; symmetry makes the sampled total exactly
    // half the exact total regardless of which sources are drawn.
    let sampled = Betweenness::run(
        &graph,
        BetweennessConfig {
            normalize: false,
            sampling_size: Some(2),
        },
    );
    let sampled_total: f64 = sampled.scores.iter().map(|&(_, s)| s).sum();

    assert!(exact_total > 0.0, "sanity: exact betweenness is non-zero");
    // FIXED: the sampled estimator now multiplies by n/k (Brandes-Pich), so it
    // is unbiased. By the 4-cycle's rotational symmetry every single-source
    // contribution is identical, so the rescaled 2-source sample equals the
    // exact total (fix for
    // crates/uni-algo/src/algo/algorithms/betweenness.rs).
    assert!(
        (sampled_total - exact_total).abs() < 1e-9,
        "rescaled sampled_total must equal exact_total (got sampled={sampled_total}, exact={exact_total})"
    );
}
