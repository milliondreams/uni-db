// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Dijkstra's Shortest Path Algorithm.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use uni_common::core::id::Vid;

pub struct Dijkstra;

#[derive(Debug, Clone)]
pub struct DijkstraConfig {
    pub source: Vid,
    pub target: Option<Vid>,
    pub max_distance: Option<f64>,
}

impl Default for DijkstraConfig {
    fn default() -> Self {
        Self {
            source: Vid::from(0),
            target: None,
            max_distance: None,
        }
    }
}

pub struct DijkstraResult {
    pub distances: Vec<(Vid, f64)>,
    pub path: Option<Vec<Vid>>,
}

/// Error returned when Dijkstra cannot produce a correct result.
///
/// Dijkstra's settled-on-first-pop invariant only holds for non-negative
/// edge weights, so a negative weight is rejected rather than silently
/// yielding a wrong shortest distance.
#[derive(Debug, Clone)]
pub enum DijkstraError {
    /// An edge from `source` to `target` carries a negative `weight`.
    NegativeEdge {
        source: Vid,
        target: Vid,
        weight: f64,
    },
}

impl std::fmt::Display for DijkstraError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DijkstraError::NegativeEdge {
                source,
                target,
                weight,
            } => write!(
                f,
                "Dijkstra rejects negative edge {source:?} -> {target:?} (weight {weight})"
            ),
        }
    }
}

impl std::error::Error for DijkstraError {}

impl Algorithm for Dijkstra {
    type Config = DijkstraConfig;
    type Result = Result<DijkstraResult, DijkstraError>;

    fn name() -> &'static str {
        "shortestPath"
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        // Reject negative weights up front: Dijkstra's correctness relies on
        // non-negative edges, so any negative weight is a hard error rather
        // than a silently wrong answer.
        if graph.has_weights() {
            let n = graph.vertex_count();
            for u in 0..n as u32 {
                for (i, &v) in graph.out_neighbors(u).iter().enumerate() {
                    let weight = graph.out_weight(u, i);
                    if weight < 0.0 {
                        return Err(DijkstraError::NegativeEdge {
                            source: graph.to_vid(u),
                            target: graph.to_vid(v),
                            weight,
                        });
                    }
                }
            }
        }

        let source_slot = match graph.to_slot(config.source) {
            Some(slot) => slot,
            None => {
                return Ok(DijkstraResult {
                    distances: Vec::new(),
                    path: None,
                });
            }
        };

        let n = graph.vertex_count();
        let mut dist = vec![f64::INFINITY; n];
        let mut prev: Vec<Option<u32>> = vec![None; n];
        let mut heap = BinaryHeap::new();

        dist[source_slot as usize] = 0.0;
        heap.push(Reverse((0.0f64.to_bits(), source_slot)));

        let target_slot = config.target.and_then(|t| graph.to_slot(t));

        while let Some(Reverse((d_bits, u))) = heap.pop() {
            let d = f64::from_bits(d_bits);
            if d > dist[u as usize] {
                continue;
            }

            // Early exit for point-to-point
            if target_slot == Some(u) {
                break;
            }

            // Max distance cutoff
            if let Some(max_d) = config.max_distance
                && d > max_d
            {
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
                    prev[v as usize] = Some(u);
                    heap.push(Reverse((new_dist.to_bits(), v)));
                }
            }
        }

        // Reconstruct path if target specified
        let mut path = None;
        if let Some(t_slot) = target_slot
            && dist[t_slot as usize] < f64::INFINITY
        {
            let mut p = Vec::new();
            let mut curr = Some(t_slot);
            while let Some(slot) = curr {
                p.push(graph.to_vid(slot));
                if slot == source_slot {
                    break;
                }
                curr = prev[slot as usize];
            }
            p.reverse();
            path = Some(p);
        }

        // Exclude over-budget nodes from the output. The `max_distance` cutoff
        // above only stops EXPANSION of over-budget nodes; their already-relaxed
        // distances still sit in `dist`, so without this filter a node beyond
        // `max_distance` would be reported in the SSSP rows.
        let results = dist
            .into_iter()
            .enumerate()
            .filter(|(_, d)| *d < f64::INFINITY)
            .filter(|(_, d)| config.max_distance.is_none_or(|max_d| *d <= max_d))
            .map(|(slot, d)| (graph.to_vid(slot as u32), d))
            .collect();

        Ok(DijkstraResult {
            distances: results,
            path,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    /// Regression: Dijkstra must reject negative edge weights with an error.
    ///
    /// Dijkstra's settled-on-first-pop guard (`if d > dist[u] { continue }`)
    /// and `to_bits()` heap key are only valid for non-negative weights, so a
    /// negative weight would silently yield a wrong shortest distance instead
    /// of the true minimum. The chosen contract is to reject such input with a
    /// clean [`DijkstraError::NegativeEdge`].
    // Rust guideline compliant
    #[test]
    fn test_dijkstra_rejects_or_handles_negative_weight() {
        // Edges: 0->1 = 2, 0->2 = 5, 2->1 = -4.
        // True shortest 0 -> 2 -> 1 = 5 + (-4) = 1, beating the direct 0 -> 1 = 2.
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2)];
        let edges = vec![
            (Vid::from(0), Vid::from(1)),
            (Vid::from(0), Vid::from(2)),
            (Vid::from(2), Vid::from(1)),
        ];

        let mut graph = build_test_graph(vids, edges);
        // Flattened CSR weights, matching out-adjacency layout:
        // Node 0: [0->1 = 2.0, 0->2 = 5.0]
        // Node 1: []
        // Node 2: [2->1 = -4.0]
        graph.out_weights = Some(vec![2.0, 5.0, -4.0]);

        let config = DijkstraConfig {
            source: Vid::from(0),
            target: Some(Vid::from(1)),
            max_distance: None,
        };

        // The negative edge 2 -> 1 must be rejected, not silently mis-routed.
        assert!(matches!(
            Dijkstra::run(&graph, config),
            Err(DijkstraError::NegativeEdge { .. })
        ));
    }
}
