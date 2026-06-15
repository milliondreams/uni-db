// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Louvain Community Detection Algorithm.

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use std::collections::HashMap;
use uni_common::core::id::Vid;

pub struct Louvain;

#[derive(Debug, Clone)]
pub struct LouvainConfig {
    pub resolution: f64,
    pub max_iterations: usize,
    pub min_modularity_gain: f64,
}

impl Default for LouvainConfig {
    fn default() -> Self {
        Self {
            resolution: 1.0,
            max_iterations: 10,
            min_modularity_gain: 1e-4,
        }
    }
}

pub struct LouvainResult {
    pub communities: Vec<(Vid, u64)>,
    pub modularity: f64,
    pub community_count: usize,
}

impl Algorithm for Louvain {
    type Config = LouvainConfig;
    type Result = LouvainResult;

    fn name() -> &'static str {
        "louvain"
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return LouvainResult {
                communities: Vec::new(),
                modularity: 0.0,
                community_count: 0,
            };
        }

        // Initialize: each node in its own community
        let mut community: Vec<u32> = (0..n as u32).collect();

        // Total edge weight (m)
        // For unweighted graph, m = edge_count / 2 (since each edge is counted twice if bidirectional)
        // But Uni GraphProjection might be directed.
        // Louvain usually works on undirected graphs.
        // We treat it as undirected by summing all out_degrees.
        let mut m: f64 = 0.0;
        let mut node_weights = vec![0.0; n];
        for v in 0..n as u32 {
            let mut deg = graph.out_degree(v) as f64;
            if graph.has_reverse() {
                deg += graph.in_degree(v) as f64;
            }
            m += deg;
            node_weights[v as usize] = deg;
        }
        m /= 2.0;

        if m == 0.0 {
            return LouvainResult {
                communities: community
                    .into_iter()
                    .enumerate()
                    .map(|(i, c)| (graph.to_vid(i as u32), c as u64))
                    .collect(),
                modularity: 0.0,
                community_count: n,
            };
        }

        // Track community total weights (Sigma_tot)
        let mut community_weights = node_weights.clone();

        for _ in 0..config.max_iterations {
            let mut improved = false;

            // Phase 1: Local moves
            for v in 0..n as u32 {
                let v_idx = v as usize;
                let current_comm = community[v_idx];
                let v_weight = node_weights[v_idx];

                // Find neighbor communities and weights to them (k_i,in)
                let mut neighbor_comm_weights: HashMap<u32, f64> = HashMap::new();
                for &u in graph.out_neighbors(v) {
                    let u_comm = community[u as usize];
                    *neighbor_comm_weights.entry(u_comm).or_insert(0.0) += 1.0;
                }
                if graph.has_reverse() {
                    for &u in graph.in_neighbors(v) {
                        let u_comm = community[u as usize];
                        *neighbor_comm_weights.entry(u_comm).or_insert(0.0) += 1.0;
                    }
                }

                let mut best_comm = current_comm;

                // Remove v from current community
                community_weights[current_comm as usize] -= v_weight;

                // Baseline: the gain of returning v to its OWN community. A move
                // is only worthwhile if it beats staying put, so we score moves
                // by the delta relative to this baseline. Seeding the best score
                // at a bare `0.0` (the old code) ignored the current community
                // entirely: it could relocate v to a community that scored
                // positive in absolute terms yet WORSE than where v already was,
                // and could strand v in a negative-gain community when a strictly
                // better (still-negative-absolute) move existed. (review H16c)
                let current_k_i_in = neighbor_comm_weights
                    .get(&current_comm)
                    .copied()
                    .unwrap_or(0.0);
                let current_gain = current_k_i_in
                    - (community_weights[current_comm as usize] * v_weight * config.resolution)
                        / (2.0 * m);

                // Iterate candidate communities in a deterministic (community-id)
                // order so HashMap iteration-seed differences cannot flip an
                // otherwise-tied best-community choice. This keeps the detected
                // partition reproducible across runs.
                let mut candidates: Vec<(u32, f64)> = neighbor_comm_weights
                    .iter()
                    .map(|(&c, &w)| (c, w))
                    .collect();
                candidates.sort_unstable_by_key(|&(c, _)| c);

                // Best improvement over staying put. A move must strictly improve
                // modularity (delta > 0) to be considered at all.
                let mut best_delta = 0.0;

                for &(target_comm, k_i_in) in &candidates {
                    if target_comm == current_comm {
                        continue;
                    }
                    let target_comm_weight = community_weights[target_comm as usize];

                    // Modularity gain of inserting v into `target_comm`:
                    // delta_Q = (1/2m) * (k_i,in - (Sigma_tot * k_i) / m)
                    let gain =
                        k_i_in - (target_comm_weight * v_weight * config.resolution) / (2.0 * m);
                    let delta = gain - current_gain;

                    if delta > best_delta {
                        best_delta = delta;
                        best_comm = target_comm;
                    }
                }

                if best_delta > config.min_modularity_gain && best_comm != current_comm {
                    community[v_idx] = best_comm;
                    improved = true;
                }

                // Add v to best community
                community_weights[community[v_idx] as usize] += v_weight;
            }

            if !improved {
                break;
            }
        }

        // Final modularity calculation
        let q = compute_modularity(graph, &community, m, config.resolution);

        // Map back to VIDs and renumber communities
        let mut comm_map: HashMap<u32, u64> = HashMap::new();
        let mut next_id = 0u64;
        let mut results = Vec::with_capacity(n);
        for (i, &comm) in community.iter().enumerate() {
            let id = *comm_map.entry(comm).or_insert_with(|| {
                let val = next_id;
                next_id += 1;
                val
            });
            results.push((graph.to_vid(i as u32), id));
        }

        LouvainResult {
            communities: results,
            modularity: q,
            community_count: comm_map.len(),
        }
    }
}

fn compute_modularity(graph: &GraphProjection, community: &[u32], m: f64, resolution: f64) -> f64 {
    let n = graph.vertex_count();
    let mut q = 0.0;

    // Sum over communities.
    //
    // `comm_total_weights` accumulates each community's *full undirected
    // degree* `D_c`: every edge `v -> u` adds 1 to both endpoints' community
    // totals. Summing out-degree alone (as before) under-counts the degree of
    // every node that only appears as an edge target on a single-direction
    // projection, mis-scaling `Q` relative to the undirected `m` and internal
    // counts. `comm_internal_weights` counts each undirected internal edge
    // once (`L_c`), matching the single-direction edge layout.
    let mut comm_internal_weights: HashMap<u32, f64> = HashMap::new();
    let mut comm_total_weights: HashMap<u32, f64> = HashMap::new();

    for v in 0..n as u32 {
        let v_comm = community[v as usize];
        for &u in graph.out_neighbors(v) {
            let u_comm = community[u as usize];
            // Undirected degree: the edge contributes to both endpoints.
            *comm_total_weights.entry(v_comm).or_insert(0.0) += 1.0;
            *comm_total_weights.entry(u_comm).or_insert(0.0) += 1.0;
            if u_comm == v_comm {
                *comm_internal_weights.entry(v_comm).or_insert(0.0) += 1.0;
            }
        }
    }

    // The undirected modularity is `Q = Σ_c [ L_c / M − (D_c / 2M)^2 ]`, where
    // `M = 2m` is the total number of undirected edges. Here `m` is the value
    // `Louvain::run` derives (sum of out-degrees / 2), so `M = 2m` and the
    // degree term denominator is `2M = 4m`.
    let two_m = 2.0 * m;
    for (&comm, &internal) in &comm_internal_weights {
        let total = comm_total_weights[&comm];
        q += (internal / two_m) - resolution * (total / (2.0 * two_m)).powi(2);
    }

    q
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;

    /// Regression: `compute_modularity` mis-scales directed degree vs internal.
    ///
    /// `comm_total_weights` sums out-degree only while `comm_internal_weights`
    /// counts each undirected internal edge once and `m = sum(out_degree) / 2`.
    /// On a directed (single-direction) projection these conventions disagree,
    /// so the reported modularity `Q` is wrong for the natural partition.
    ///
    /// Two triangles {0,1,2} and {3,4,5} joined by the single bridge edge 2-3,
    /// represented as 7 single-direction edges, give `m = 3.5`. The natural
    /// 2-community partition has correct undirected modularity `Q = 0.3571`
    /// (M = 7, internal = 3 per community, D_c = 7 per community).
    // Rust guideline compliant
    #[test]
    fn test_louvain_modularity_scaling() {
        // Two triangles plus a bridge, each undirected edge written once.
        // Triangle A: 0-1, 1-2, 0-2 ; Triangle B: 3-4, 4-5, 3-5 ; bridge 2-3.
        let vids = (0..6).map(Vid::from).collect::<Vec<_>>();
        let edges = vec![
            (Vid::from(0), Vid::from(1)),
            (Vid::from(1), Vid::from(2)),
            (Vid::from(0), Vid::from(2)),
            (Vid::from(3), Vid::from(4)),
            (Vid::from(4), Vid::from(5)),
            (Vid::from(3), Vid::from(5)),
            (Vid::from(2), Vid::from(3)),
        ];

        let graph = build_test_graph(vids, edges);

        // Natural 2-community partition: {0,1,2} -> 0, {3,4,5} -> 1.
        let community: Vec<u32> = vec![0, 0, 0, 1, 1, 1];

        // m as `Louvain::run` derives it for a no-reverse projection:
        // sum of out-degrees / 2 = 7 / 2 = 3.5.
        let m: f64 = (0..graph.vertex_count() as u32)
            .map(|v| graph.out_degree(v) as f64)
            .sum::<f64>()
            / 2.0;

        let q = compute_modularity(&graph, &community, m, 1.0);

        // Correct undirected modularity for this partition is 0.3571.
        // RED today: the directed degree/internal mis-scaling yields ~0.3469.
        assert!(
            (q - 0.3571).abs() < 1e-3,
            "modularity should be ~0.3571, got {q}"
        );
    }

    /// H16c: the baseline-aware local move must recover the optimal two-community
    /// partition of the two-triangle/bridge graph (each triangle its own
    /// community), reaching the natural-partition modularity (~0.357 > 0). The
    /// old seed-at-0.0 move scored candidates without ever scoring the current
    /// community as a baseline, so it could strand a node or make a move worse
    /// than staying — landing on a lower-modularity partition.
    #[test]
    fn local_move_recovers_optimal_communities() {
        let vids = (0..6).map(Vid::from).collect::<Vec<_>>();
        // Undirected two-triangle/bridge graph: each edge written in BOTH
        // directions so the no-reverse projection sees a symmetric structure
        // (out-degree captures every neighbor). Single-direction edges would
        // skew the gain (the projection has no reverse adjacency).
        let undirected = [(0u64, 1u64), (1, 2), (0, 2), (3, 4), (4, 5), (3, 5), (2, 3)];
        let mut edges = Vec::new();
        for (a, b) in undirected {
            edges.push((Vid::from(a), Vid::from(b)));
            edges.push((Vid::from(b), Vid::from(a)));
        }
        let graph = build_test_graph(vids, edges);

        let result = Louvain::run(&graph, LouvainConfig::default());
        let comm: HashMap<u64, u64> = result
            .communities
            .iter()
            .map(|(v, c)| (v.as_u64(), *c))
            .collect();

        // Each triangle ends in one community; the two triangles are distinct.
        assert_eq!(comm[&0], comm[&1]);
        assert_eq!(comm[&1], comm[&2]);
        assert_eq!(comm[&3], comm[&4]);
        assert_eq!(comm[&4], comm[&5]);
        assert_ne!(comm[&0], comm[&3], "the two triangles must not be merged");
        assert_eq!(result.community_count, 2);
        assert!(
            result.modularity > 0.3,
            "should reach the natural-partition modularity, got {}",
            result.modularity
        );
    }
}
