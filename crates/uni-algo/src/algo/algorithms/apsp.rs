// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! All Pairs Shortest Path Algorithm.

use crate::algo::GraphProjection;
use crate::algo::algorithms::{Algorithm, bfs_levels, dijkstra_distances};
use rayon::prelude::*;
use uni_common::core::id::Vid;

pub struct AllPairsShortestPath;

#[derive(Debug, Clone, Default)]
pub struct AllPairsShortestPathConfig;

pub struct AllPairsShortestPathResult {
    /// `(source, target, distance)`. Distance is a weighted shortest-path cost
    /// (`f64`) so that weighted graphs are not reported as mere hop counts.
    pub distances: Vec<(Vid, Vid, f64)>,
}

impl Algorithm for AllPairsShortestPath {
    type Config = AllPairsShortestPathConfig;
    type Result = AllPairsShortestPathResult;

    fn name() -> &'static str {
        "allPairsShortestPath"
    }

    fn run(graph: &GraphProjection, _config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return AllPairsShortestPathResult {
                distances: Vec::new(),
            };
        }

        // One shortest-path tree per source, in parallel. On a WEIGHTED graph,
        // BFS would return hop counts rather than weighted distances — branch on
        // `has_weights()` and run Dijkstra-per-source there. (review H16a)
        let weighted = graph.has_weights();
        let distances = (0..n as u32)
            .into_par_iter()
            .flat_map_iter(|s| {
                let src_vid = graph.to_vid(s);
                let row: Vec<(Vid, Vid, f64)> = if weighted {
                    dijkstra_distances(graph, s)
                        .into_iter()
                        .enumerate()
                        // Skip the source itself (0.0) and unreachable (INF).
                        .filter(|&(_, dist)| dist.is_finite() && dist > 0.0)
                        .map(|(tgt, dist)| (src_vid, graph.to_vid(tgt as u32), dist))
                        .collect()
                } else {
                    bfs_levels(graph, s)
                        .into_iter()
                        .enumerate()
                        .filter(|&(_, dist)| dist > 0)
                        .map(|(tgt, dist)| (src_vid, graph.to_vid(tgt as u32), dist as f64))
                        .collect()
                };
                row.into_iter()
            })
            .collect();

        AllPairsShortestPathResult { distances }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;
    use std::collections::HashMap;

    /// H16a: on a WEIGHTED graph, APSP must return weighted shortest-path
    /// distances, not BFS hop counts. Here the 2-hop path 0→2→1 (cost 2) beats
    /// the direct edge 0→1 (cost 10), so dist(0,1) must be 2.0 — a hop-count BFS
    /// would wrongly report 1.
    #[test]
    fn weighted_apsp_uses_distance_not_hops() {
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2)];
        let edges = vec![
            (Vid::from(0), Vid::from(1)),
            (Vid::from(0), Vid::from(2)),
            (Vid::from(2), Vid::from(1)),
        ];
        let mut graph = build_test_graph(vids, edges);
        // Flattened CSR weights (out-adjacency order): 0->1=10, 0->2=1, 2->1=1.
        graph.out_weights = Some(vec![10.0, 1.0, 1.0]);

        let result = AllPairsShortestPath::run(&graph, AllPairsShortestPathConfig);
        let dist: HashMap<(u64, u64), f64> = result
            .distances
            .iter()
            .map(|(s, t, d)| ((s.as_u64(), t.as_u64()), *d))
            .collect();

        assert_eq!(
            dist.get(&(0, 1)),
            Some(&2.0),
            "weighted dist(0,1) should be 2.0 (0->2->1), not the hop count 1"
        );
        assert_eq!(dist.get(&(0, 2)), Some(&1.0));
        assert_eq!(dist.get(&(2, 1)), Some(&1.0));
    }

    /// Without weights, APSP falls back to hop counts (BFS).
    #[test]
    fn unweighted_apsp_returns_hop_counts() {
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2)];
        let edges = vec![
            (Vid::from(0), Vid::from(1)),
            (Vid::from(0), Vid::from(2)),
            (Vid::from(2), Vid::from(1)),
        ];
        let graph = build_test_graph(vids, edges);

        let result = AllPairsShortestPath::run(&graph, AllPairsShortestPathConfig);
        let dist: HashMap<(u64, u64), f64> = result
            .distances
            .iter()
            .map(|(s, t, d)| ((s.as_u64(), t.as_u64()), *d))
            .collect();

        // Direct edge 0->1 is one hop.
        assert_eq!(dist.get(&(0, 1)), Some(&1.0));
        assert_eq!(dist.get(&(0, 2)), Some(&1.0));
    }
}
