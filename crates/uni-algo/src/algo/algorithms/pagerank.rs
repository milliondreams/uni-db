// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! PageRank Centrality Algorithm.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use rayon::prelude::*;
use uni_common::core::id::Vid;

pub struct PageRank;

#[derive(Debug, Clone)]
pub struct PageRankConfig {
    pub damping_factor: f64,
    pub max_iterations: usize,
    pub tolerance: f64,
}

impl Default for PageRankConfig {
    fn default() -> Self {
        Self {
            damping_factor: 0.85,
            max_iterations: 20,
            tolerance: 1e-6,
        }
    }
}

pub struct PageRankResult {
    pub scores: Vec<(Vid, f64)>,
    pub iterations: usize,
    pub converged: bool,
}

impl Algorithm for PageRank {
    type Config = PageRankConfig;
    type Result = PageRankResult;

    fn name() -> &'static str {
        "pageRank"
    }

    fn needs_reverse() -> bool {
        true
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return PageRankResult {
                scores: Vec::new(),
                iterations: 0,
                converged: true,
            };
        }

        let d = config.damping_factor;
        let base = (1.0 - d) / n as f64;

        let mut scores = vec![1.0 / n as f64; n];
        let mut next = vec![0.0; n];

        let mut iterations = 0;
        let mut converged = false;

        for iter in 0..config.max_iterations {
            iterations = iter + 1;

            // Total mass held by dangling nodes (out-degree 0) in the current
            // score vector. Sinks absorb but never re-emit rank, so without an
            // explicit correction their mass leaks out every iteration and the
            // score vector decays below 1.0. Standard PageRank redistributes
            // this mass uniformly across all `n` nodes, scaled by the damping
            // factor, so total probability mass is conserved.
            let dangling_mass: f64 = (0..n as u32)
                .into_par_iter()
                .filter(|&u| graph.out_degree(u) == 0)
                .map(|u| scores[u as usize])
                .sum();
            let dangling_share = d * dangling_mass / n as f64;

            // Parallel iteration over vertices
            next.par_iter_mut().enumerate().for_each(|(v, score)| {
                let sum: f64 = graph
                    .in_neighbors(v as u32)
                    .iter()
                    .map(|&u| {
                        let out_deg = graph.out_degree(u);
                        if out_deg > 0 {
                            scores[u as usize] / out_deg as f64
                        } else {
                            // Dangling node: its rank is redistributed via
                            // `dangling_share` below, not through this in-edge sum.
                            0.0
                        }
                    })
                    .sum();
                *score = base + dangling_share + d * sum;
            });

            // Convergence check
            let diff: f64 = scores
                .par_iter()
                .zip(next.par_iter())
                .map(|(a, b)| (a - b).abs())
                .sum();

            std::mem::swap(&mut scores, &mut next);

            if diff < config.tolerance {
                converged = true;
                break;
            }
        }

        // Map results back to VIDs
        let results = scores
            .into_iter()
            .enumerate()
            .map(|(slot, score)| (graph.to_vid(slot as u32), score))
            .collect();

        PageRankResult {
            scores: results,
            iterations,
            converged,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    /// Regression: PageRank must conserve total mass with a dangling node.
    ///
    /// A dangling node (out-degree 0) contributes `0.0` to its neighbors'
    /// sums (`run`, dangling branch) and its mass is *not* folded into
    /// `base = (1 - d) / n`. So the rank of any walk that reaches a sink
    /// leaks out every iteration and the score vector decays well below
    /// `1.0`. A correct implementation redistributes dangling mass so the
    /// scores sum to `1.0`.
    // Rust guideline compliant
    #[test]
    fn test_pagerank_conserves_mass_with_dangling_node() {
        // Path graph 0 -> 1 -> 2, with node 2 dangling (out-degree 0).
        let vids = vec![Vid::from(0), Vid::from(1), Vid::from(2)];
        let edges = vec![(Vid::from(0), Vid::from(1)), (Vid::from(1), Vid::from(2))];

        let mut graph = build_test_graph(vids, edges);

        // `build_test_graph` leaves the in-CSR empty, but PageRank iterates
        // `in_neighbors`. Populate the reverse CSR by hand to match the
        // out-edges: in(0) = {}, in(1) = {0}, in(2) = {1}.
        // CSR offsets [V+1]: vertex slot -> start index in in_neighbors.
        graph.in_offsets = vec![0, 0, 1, 2];
        graph.in_neighbors = vec![0, 1];

        let config = PageRankConfig {
            damping_factor: 0.85,
            max_iterations: 100,
            tolerance: 1e-12,
        };
        let result = PageRank::run(&graph, config);

        let total: f64 = result.scores.iter().map(|(_, s)| *s).sum();

        // Correct PageRank conserves probability mass: the scores sum to 1.0.
        // RED today: the dangling node's mass leaks out, leaving total ~= 0.27.
        assert!(
            (total - 1.0).abs() < 1e-3,
            "PageRank scores should sum to ~1.0, got {total}"
        );
        for (vid, score) in &result.scores {
            assert!(
                *score > 0.0,
                "score for {vid:?} should be positive, got {score}"
            );
        }
    }
}
