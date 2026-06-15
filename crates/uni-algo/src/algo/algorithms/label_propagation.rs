// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Label Propagation Community Detection Algorithm.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use rand::prelude::*;
use rand::rngs::StdRng;
use std::collections::HashMap;
use uni_common::core::id::Vid;

/// Fixed default RNG seed so community assignments are reproducible run-to-run
/// (deterministic by default). Callers set `LabelPropagationConfig::seed` for a
/// different but still reproducible stream.
const DEFAULT_SEED: u64 = 0x51F3_9C7B_2E4D_8A16;

pub struct LabelPropagation;

/// Configuration for Label Propagation.
#[derive(Debug, Clone)]
pub struct LabelPropagationConfig {
    pub max_iterations: usize,
    pub seed_property: Option<String>,
    pub write: bool,
    pub write_property: String,
    /// RNG seed for the processing-order shuffle and tie-breaks. `None` =>
    /// `DEFAULT_SEED` (deterministic by default).
    pub seed: Option<u64>,
}

impl Default for LabelPropagationConfig {
    fn default() -> Self {
        Self {
            max_iterations: 10,
            seed_property: None,
            write: false,
            write_property: "community".to_string(),
            seed: None,
        }
    }
}

/// Result of Label Propagation.
#[derive(Debug)]
pub struct LabelPropagationResult {
    /// Community ID for each node (VID, CommunityID).
    pub communities: Vec<(Vid, u64)>,
    /// Number of iterations executed.
    pub iterations: usize,
    /// Whether the algorithm converged.
    pub converged: bool,
}

impl Algorithm for LabelPropagation {
    type Config = LabelPropagationConfig;
    type Result = LabelPropagationResult;

    fn name() -> &'static str {
        "labelPropagation"
    }

    fn needs_reverse() -> bool {
        // We typically treat graph as undirected for communities
        true
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let num_nodes = graph.vertex_count();
        if num_nodes == 0 {
            return LabelPropagationResult {
                communities: Vec::new(),
                iterations: 0,
                converged: true,
            };
        }

        let mut labels = vec![0u64; num_nodes];

        // 1. Initialize labels with VID
        for (i, label) in labels.iter_mut().enumerate().take(num_nodes) {
            *label = graph.to_vid(i as u32).as_u64();
        }

        let mut converged = false;
        let mut iterations = 0;
        let mut node_indices: Vec<u32> = (0..num_nodes as u32).collect();
        let mut rng = StdRng::seed_from_u64(config.seed.unwrap_or(DEFAULT_SEED));

        while iterations < config.max_iterations {
            let mut changes = 0;

            // Shuffle processing order to prevent oscillation
            node_indices.shuffle(&mut rng);

            for &node_idx in &node_indices {
                // Collect all neighbors (undirected view if reverse edges present)
                let out_neighbors = graph.out_neighbors(node_idx);

                let mut label_counts: HashMap<u64, usize> = HashMap::new();

                for &neighbor_idx in out_neighbors {
                    let neighbor_label = labels[neighbor_idx as usize];
                    *label_counts.entry(neighbor_label).or_insert(0) += 1;
                }

                if graph.has_reverse() {
                    let in_neighbors = graph.in_neighbors(node_idx);
                    for &neighbor_idx in in_neighbors {
                        let neighbor_label = labels[neighbor_idx as usize];
                        *label_counts.entry(neighbor_label).or_insert(0) += 1;
                    }
                }

                if label_counts.is_empty() {
                    continue;
                }

                // Find max frequency
                let mut max_count = 0;
                for &count in label_counts.values() {
                    if count > max_count {
                        max_count = count;
                    }
                }

                // Collect best labels (ties). Sort so the candidate order is
                // canonical — `HashMap` iteration order is randomized per
                // instance, which would otherwise make the seeded tie-break
                // pick a different label run-to-run.
                let mut best_labels: Vec<u64> = label_counts
                    .iter()
                    .filter(|(_, count)| **count == max_count)
                    .map(|(label, _)| *label)
                    .collect();
                best_labels.sort_unstable();

                // Pick one randomly (deterministic given the seeded RNG and the
                // now-canonical candidate order).
                let new_label = if best_labels.len() == 1 {
                    best_labels[0]
                } else {
                    *best_labels.choose(&mut rng).unwrap()
                };

                if labels[node_idx as usize] != new_label {
                    labels[node_idx as usize] = new_label;
                    changes += 1;
                }
            }

            iterations += 1;
            if changes == 0 {
                converged = true;
                break;
            }
        }

        // Map results back to VIDs
        let communities = labels
            .into_iter()
            .enumerate()
            .map(|(slot, label)| (graph.to_vid(slot as u32), label))
            .collect();

        LabelPropagationResult {
            communities,
            iterations,
            converged,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    fn two_cliques() -> GraphProjection {
        // Two triangles {0,1,2} and {3,4,5} bridged by a single 2-3 edge,
        // edges added both ways so the undirected community view is symmetric.
        let undirected = [(0, 1), (1, 2), (2, 0), (3, 4), (4, 5), (5, 3), (2, 3)];
        let mut edges = Vec::new();
        for (a, b) in undirected {
            edges.push((Vid::from(a as u64), Vid::from(b as u64)));
            edges.push((Vid::from(b as u64), Vid::from(a as u64)));
        }
        build_test_graph((0..6).map(Vid::from).collect(), edges)
    }

    #[test]
    fn label_propagation_is_deterministic_with_seed() {
        let graph = two_cliques();
        let config = LabelPropagationConfig {
            max_iterations: 20,
            seed: Some(123),
            ..Default::default()
        };
        let a = LabelPropagation::run(&graph, config.clone());
        let b = LabelPropagation::run(&graph, config);
        assert_eq!(
            a.communities, b.communities,
            "identical seed must yield identical communities"
        );
    }

    #[test]
    fn label_propagation_default_seed_is_deterministic() {
        let graph = two_cliques();
        let cfg = LabelPropagationConfig::default();
        let a = LabelPropagation::run(&graph, cfg.clone());
        let b = LabelPropagation::run(&graph, cfg);
        assert_eq!(a.communities, b.communities);
    }
}
