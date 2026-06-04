// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Core algorithm trait and common utilities.

use crate::algo::GraphProjection;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};

/// Unweighted single-source BFS over the outbound CSR.
///
/// Returns a `dist` vector of length `graph.vertex_count()` where
/// `dist[v]` is the number of hops from `source` to slot `v`, or `-1`
/// if `v` is unreachable. The source itself has distance `0`.
///
/// This is the shared building block for the unweighted centrality and
/// all-pairs algorithms (closeness, harmonic, betweenness samples,
/// all-pairs shortest path) which previously each hand-rolled the same
/// queue/visited bookkeeping with subtly different conventions.
pub(crate) fn bfs_levels(graph: &GraphProjection, source: u32) -> Vec<i32> {
    let n = graph.vertex_count();
    let mut dist = vec![-1; n];
    let mut q = VecDeque::with_capacity(n);

    dist[source as usize] = 0;
    q.push_back(source);

    while let Some(u) = q.pop_front() {
        let dist_u = dist[u as usize];
        for &v in graph.out_neighbors(u) {
            if dist[v as usize] == -1 {
                dist[v as usize] = dist_u + 1;
                q.push_back(v);
            }
        }
    }

    dist
}

/// Weighted single-source shortest paths (Dijkstra) over the outbound CSR.
///
/// Returns a `dist` vector of length `graph.vertex_count()` where
/// `dist[v]` is the shortest weighted distance from `source` to slot `v`,
/// or `f64::INFINITY` if `v` is unreachable. Edge weights default to `1.0`
/// when the projection carries no weights.
///
/// Shared by [`Dijkstra`] (single-source distances) and harmonic
/// centrality, which previously duplicated the relaxation loop.
pub(crate) fn dijkstra_distances(graph: &GraphProjection, source: u32) -> Vec<f64> {
    let n = graph.vertex_count();
    let mut dist = vec![f64::INFINITY; n];
    let mut heap = BinaryHeap::new();

    dist[source as usize] = 0.0;
    heap.push(Reverse((0.0f64.to_bits(), source)));

    while let Some(Reverse((d_bits, u))) = heap.pop() {
        let d = f64::from_bits(d_bits);
        if d > dist[u as usize] {
            continue;
        }
        for (i, &v) in graph.out_neighbors(u).iter().enumerate() {
            let weight = if graph.has_weights() {
                graph.out_weight(u, i)
            } else {
                1.0
            };
            let new_dist = d + weight;
            if new_dist < dist[v as usize] {
                dist[v as usize] = new_dist;
                heap.push(Reverse((new_dist.to_bits(), v)));
            }
        }
    }

    dist
}

/// Core trait for all graph algorithms.
pub trait Algorithm: Send + Sync {
    /// Algorithm parameters.
    type Config: Default + Clone + Send + 'static;
    /// Result type.
    type Result: Send + 'static;

    /// Algorithm identifier.
    fn name() -> &'static str;

    /// Execute algorithm on a projection.
    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result;

    /// Whether this algorithm requires reverse edges.
    fn needs_reverse() -> bool {
        false
    }

    /// Whether this algorithm requires edge weights.
    fn needs_weights() -> bool {
        false
    }
}

mod pagerank;
pub use pagerank::{PageRank, PageRankConfig, PageRankResult};

mod wcc;
pub use wcc::{Wcc, WccConfig, WccResult};

mod dijkstra;
pub use dijkstra::{Dijkstra, DijkstraConfig, DijkstraResult};

mod louvain;
pub use louvain::{Louvain, LouvainConfig, LouvainResult};

mod label_propagation;
pub use label_propagation::{LabelPropagation, LabelPropagationConfig, LabelPropagationResult};

mod betweenness;
pub use betweenness::{Betweenness, BetweennessConfig, BetweennessResult};

mod node_similarity;
pub use node_similarity::{
    NodeSimilarity, NodeSimilarityConfig, NodeSimilarityResult, SimilarityMetric,
};

mod closeness;
pub use closeness::{Closeness, ClosenessConfig, ClosenessResult};

mod triangle_count;
pub use triangle_count::{TriangleCount, TriangleCountConfig, TriangleCountResult};

mod kcore;
pub use kcore::{KCore, KCoreConfig, KCoreResult};

mod random_walk;
pub use random_walk::{RandomWalk, RandomWalkConfig, RandomWalkResult};

mod apsp;
pub use apsp::{AllPairsShortestPath, AllPairsShortestPathConfig, AllPairsShortestPathResult};

mod scc;
pub use scc::{Scc, SccConfig, SccResult};

mod topological_sort;
pub use topological_sort::{TopologicalSort, TopologicalSortConfig, TopologicalSortResult};

mod cycle_detection;
pub use cycle_detection::{CycleDetection, CycleDetectionConfig, CycleDetectionResult};

mod bipartite_check;
pub use bipartite_check::{BipartiteCheck, BipartiteCheckConfig, BipartiteCheckResult};

mod bridges;
pub use bridges::{Bridges, BridgesConfig, BridgesResult};

mod articulation_points;
pub use articulation_points::{
    ArticulationPoints, ArticulationPointsConfig, ArticulationPointsResult,
};

mod astar;
pub use astar::{AStar, AStarConfig, AStarResult};

mod bidirectional_dijkstra;
pub use bidirectional_dijkstra::{
    BidirectionalDijkstra, BidirectionalDijkstraConfig, BidirectionalDijkstraResult,
};

mod bellman_ford;
pub use bellman_ford::{BellmanFord, BellmanFordConfig, BellmanFordResult};

mod k_shortest_paths;
pub use k_shortest_paths::{KShortestPaths, KShortestPathsConfig, KShortestPathsResult};

mod mst;
pub use mst::{MinimumSpanningTree, MstConfig, MstResult};

mod max_matching;
pub use max_matching::{MaximumMatching, MaximumMatchingConfig, MaximumMatchingResult};

mod dinic;
pub use dinic::{Dinic, DinicConfig, DinicResult};

mod ford_fulkerson;
pub use ford_fulkerson::{FordFulkerson, FordFulkersonConfig, FordFulkersonResult};

mod graph_metrics;
pub use graph_metrics::{GraphMetrics, GraphMetricsConfig, GraphMetricsResult};

mod degree_centrality;
pub use degree_centrality::{
    DegreeCentrality, DegreeCentralityConfig, DegreeCentralityResult, DegreeDirection,
};

mod harmonic_centrality;
pub use harmonic_centrality::{
    HarmonicCentrality, HarmonicCentralityConfig, HarmonicCentralityResult,
};

mod eigenvector_centrality;
pub use eigenvector_centrality::{
    EigenvectorCentrality, EigenvectorCentralityConfig, EigenvectorCentralityResult,
};

mod katz_centrality;
pub use katz_centrality::{KatzCentrality, KatzCentralityConfig, KatzCentralityResult};

mod maximal_cliques;
pub use maximal_cliques::{MaximalCliques, MaximalCliquesConfig, MaximalCliquesResult};

mod all_simple_paths;
pub use all_simple_paths::{AllSimplePaths, AllSimplePathsConfig, AllSimplePathsResult};

mod elementary_circuits;
pub use elementary_circuits::{
    ElementaryCircuits, ElementaryCircuitsConfig, ElementaryCircuitsResult,
};

mod graph_coloring;
pub use graph_coloring::{GraphColoring, GraphColoringConfig, GraphColoringResult};
