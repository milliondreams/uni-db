// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! All Pairs Shortest Path Algorithm.

use crate::algo::GraphProjection;
use crate::algo::algorithms::{Algorithm, bfs_levels};
use rayon::prelude::*;
use uni_common::core::id::Vid;

pub struct AllPairsShortestPath;

#[derive(Debug, Clone, Default)]
pub struct AllPairsShortestPathConfig;

pub struct AllPairsShortestPathResult {
    pub distances: Vec<(Vid, Vid, u32)>, // (source, target, distance)
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

        // O(V(V+E)): one BFS per source, collected in parallel.
        let distances = (0..n as u32)
            .into_par_iter()
            .flat_map_iter(|s| {
                let d = bfs_levels(graph, s);
                let src_vid = graph.to_vid(s);
                d.into_iter()
                    .enumerate()
                    .filter(|&(_, dist)| dist > 0)
                    .map(move |(tgt, dist)| (src_vid, graph.to_vid(tgt as u32), dist as u32))
            })
            .collect();

        AllPairsShortestPathResult { distances }
    }
}
