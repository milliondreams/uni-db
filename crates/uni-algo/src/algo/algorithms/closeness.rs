// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Closeness Centrality Algorithm.

use crate::algo::GraphProjection;
use crate::algo::algorithms::{Algorithm, bfs_levels};
use rayon::prelude::*;
use uni_common::core::id::Vid;

pub struct Closeness;

#[derive(Debug, Clone, Default)]
pub struct ClosenessConfig {
    pub wasserman_faust: bool, // Improved formula for disconnected graphs
}

pub struct ClosenessResult {
    pub scores: Vec<(Vid, f64)>,
}

impl Algorithm for Closeness {
    type Config = ClosenessConfig;
    type Result = ClosenessResult;

    fn name() -> &'static str {
        "closeness"
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return ClosenessResult { scores: Vec::new() };
        }

        let mut scores = vec![0.0; n];

        // Parallel BFS from every node
        scores.par_iter_mut().enumerate().for_each(|(s, score)| {
            let d = bfs_levels(graph, s as u32);

            // Accumulate distances to all reached nodes (excluding self,
            // whose distance is 0 and is skipped by the `> 0` filter).
            let mut sum_dist = 0i64;
            let mut reached = 0u64;
            for &dist_v in &d {
                if dist_v > 0 {
                    sum_dist += dist_v as i64;
                    reached += 1;
                }
            }

            if sum_dist > 0 {
                if config.wasserman_faust {
                    // WF = (reached / (n-1)) * (reached / sum_dist)
                    //    = reached^2 / ((n-1) * sum_dist)
                    if n > 1 {
                        *score = (reached as f64).powi(2) / ((n - 1) as f64 * sum_dist as f64);
                    }
                } else {
                    // Standard = reached / sum_dist (normalized by n-1 usually implies 1/(avg_dist))
                    // Standard def: 1 / avg_dist = 1 / (sum_dist / (n-1)) = (n-1) / sum_dist
                    // But if not connected, standard is 0 or only component.
                    // Neo4j uses: (reached / (n-1)) / (sum_dist / reached) = reached^2 / ((n-1)sum)
                    // Which is effectively Wasserman-Faust for component?

                    // Let's stick to standard harmonic closeness or normalized closeness.
                    // Normalized: (n-1) / sum_dist
                    if n > 1 {
                        *score = (n - 1) as f64 / sum_dist as f64;
                    }
                }
            }
        });

        let results = scores
            .into_iter()
            .enumerate()
            .map(|(i, s)| (graph.to_vid(i as u32), s))
            .collect();

        ClosenessResult { scores: results }
    }
}
