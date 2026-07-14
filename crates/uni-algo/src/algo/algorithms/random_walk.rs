// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Random Walk Algorithm (uniform + node2vec second-order biased).

use crate::algo::GraphProjection;
use crate::algo::algorithms::Algorithm;
use fxhash::FxHashSet;
use rand::distr::Distribution;
use rand::distr::weighted::WeightedIndex;
use rand::prelude::*;
use rand::rngs::StdRng;
use rayon::prelude::*;
use uni_common::core::id::Vid;

/// Fixed default seed used when no explicit `seed` is supplied, so results are
/// reproducible run-to-run (deterministic by default — appropriate for a
/// database). Callers pass `RandomWalkConfig::seed` for a different but still
/// reproducible stream.
const DEFAULT_SEED: u64 = 0x9E37_79B9_7F4A_7C15;

/// Tolerance for treating a p/q parameter as "1.0" (the unbiased case).
const PARAM_EPS: f64 = 1e-12;

pub struct RandomWalk;

#[derive(Debug, Clone)]
pub struct RandomWalkConfig {
    pub walk_length: usize,
    pub walks_per_node: usize,
    pub start_nodes: Vec<Vid>, // If empty, all nodes
    pub return_param: f64,     // node2vec p — controls likelihood of returning to `prev`
    pub in_out_param: f64,     // node2vec q — controls inward (BFS) vs outward (DFS) exploration
    pub seed: Option<u64>,     // None => DEFAULT_SEED (deterministic by default)
}

impl Default for RandomWalkConfig {
    fn default() -> Self {
        Self {
            walk_length: 0,
            walks_per_node: 0,
            start_nodes: Vec::new(),
            // p = q = 1.0 is an unbiased (first-order) random walk.
            return_param: 1.0,
            in_out_param: 1.0,
            seed: None,
        }
    }
}

pub struct RandomWalkResult {
    pub walks: Vec<Vec<Vid>>,
}

/// Derive a deterministic per-walk seed from the base seed, start slot and walk
/// index. This makes the parallel (`par_iter`) walk generation independent of
/// thread scheduling: each walk owns a fully determined RNG stream.
///
/// The input mixing and the shared [`splitmix64_finalize`](crate::algo::rng::splitmix64_finalize)
/// avalanche are byte-identical to the pre-promotion inline version, so shipped
/// walk streams are unchanged by hoisting the finalizer (proposal §8, test S-6).
#[inline]
fn walk_seed(base: u64, start_slot: u32, walk_idx: usize) -> u64 {
    // SplitMix64-style mixing of the three inputs (same constants as before the
    // finalizer was hoisted into `algo::rng`).
    let s = base
        .wrapping_add((start_slot as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
        .wrapping_add((walk_idx as u64).wrapping_mul(0xD1B5_4A32_D192_ED03));
    crate::algo::rng::splitmix64_finalize(s)
}

/// Unnormalized weight of the outbound edge at `edge_idx` from `curr`.
#[inline]
fn edge_weight(graph: &GraphProjection, curr: u32, edge_idx: usize) -> f64 {
    if graph.has_weights() {
        graph.out_weight(curr, edge_idx).max(0.0)
    } else {
        1.0
    }
}

/// Pick the next slot for an unbiased step. Weighted by edge weight when the
/// projection carries weights, else uniform — preserving the historical
/// uniform behaviour for unweighted graphs.
#[inline]
fn choose_uniform_step(
    graph: &GraphProjection,
    curr: u32,
    neighbors: &[u32],
    rng: &mut StdRng,
) -> u32 {
    if graph.has_weights() {
        let weights: Vec<f64> = (0..neighbors.len())
            .map(|i| edge_weight(graph, curr, i))
            .collect();
        if let Ok(dist) = WeightedIndex::new(&weights) {
            return neighbors[dist.sample(rng)];
        }
    }
    *neighbors.choose(rng).expect("neighbors is non-empty")
}

/// Pick the next slot using the node2vec second-order bias relative to `prev`.
///
/// For each candidate `x` reachable from `curr`, the unnormalized transition
/// probability is `alpha_pq(prev, x) * weight(curr -> x)` where:
///   * `x == prev`                       => `1/p` (return)
///   * `x` is a neighbour of `prev`       => `1`   (stay "local", BFS-like)
///   * otherwise (distance 2 from `prev`) => `1/q` (explore outward, DFS-like)
#[inline]
fn choose_node2vec_step(
    graph: &GraphProjection,
    prev: u32,
    curr: u32,
    neighbors: &[u32],
    inv_pq: (f64, f64),
    rng: &mut StdRng,
) -> u32 {
    let (inv_p, inv_q) = inv_pq;
    let prev_neighbors = undirected_neighbors(graph, prev);
    let weights: Vec<f64> = neighbors
        .iter()
        .enumerate()
        .map(|(i, &x)| {
            let alpha = if x == prev {
                inv_p
            } else if prev_neighbors.contains(&x) {
                1.0
            } else {
                inv_q
            };
            alpha * edge_weight(graph, curr, i)
        })
        .collect();

    match WeightedIndex::new(&weights) {
        Ok(dist) => neighbors[dist.sample(rng)],
        // All weights zero (e.g. zero edge weights) — fall back to uniform.
        Err(_) => *neighbors.choose(rng).expect("neighbors is non-empty"),
    }
}

/// Build the undirected neighbourhood of `node`: outbound neighbours plus
/// inbound neighbours when the projection carries reverse edges. Used as the
/// "is candidate adjacent to prev?" test for the node2vec bias.
fn undirected_neighbors(graph: &GraphProjection, node: u32) -> FxHashSet<u32> {
    let mut set: FxHashSet<u32> = graph.out_neighbors(node).iter().copied().collect();
    if graph.has_reverse() {
        set.extend(graph.in_neighbors(node).iter().copied());
    }
    set
}

impl Algorithm for RandomWalk {
    type Config = RandomWalkConfig;
    type Result = RandomWalkResult;

    fn name() -> &'static str {
        "randomWalk"
    }

    fn needs_reverse() -> bool {
        // node2vec's bias needs prev's neighbourhood to classify candidates.
        // The projection is built with reverse edges by the adapter
        // (`include_reverse` defaults to true); when reverse edges are absent
        // we fall back to the directed out-neighbourhood.
        true
    }

    fn run(graph: &GraphProjection, config: Self::Config) -> Self::Result {
        let n = graph.vertex_count();
        if n == 0 {
            return RandomWalkResult { walks: Vec::new() };
        }

        let start_slots: Vec<u32> = if config.start_nodes.is_empty() {
            (0..n as u32).collect()
        } else {
            config
                .start_nodes
                .iter()
                .filter_map(|&vid| graph.to_slot(vid))
                .collect()
        };

        // Sanitize p/q (must be > 0) and decide whether to run the biased walk.
        let p = if config.return_param > 0.0 {
            config.return_param
        } else {
            1.0
        };
        let q = if config.in_out_param > 0.0 {
            config.in_out_param
        } else {
            1.0
        };
        let biased = (p - 1.0).abs() > PARAM_EPS || (q - 1.0).abs() > PARAM_EPS;
        let inv_p = 1.0 / p;
        let inv_q = 1.0 / q;
        let base_seed = config.seed.unwrap_or(DEFAULT_SEED);

        // `flat_map_iter(..).collect()` preserves the input (start-slot) order
        // regardless of how rayon schedules the work, so the result is fully
        // deterministic for a given seed.
        let walks: Vec<Vec<Vid>> = start_slots
            .par_iter()
            .flat_map_iter(|&start_node| {
                let mut local_walks = Vec::with_capacity(config.walks_per_node);

                for w in 0..config.walks_per_node {
                    // Deterministic per-walk RNG: result is independent of the
                    // order in which rayon schedules start nodes.
                    let mut rng = StdRng::seed_from_u64(walk_seed(base_seed, start_node, w));

                    let mut walk = Vec::with_capacity(config.walk_length + 1);
                    let mut curr = start_node;
                    walk.push(graph.to_vid(curr));
                    let mut prev: Option<u32> = None;

                    for _ in 0..config.walk_length {
                        let neighbors = graph.out_neighbors(curr);
                        if neighbors.is_empty() {
                            break;
                        }

                        let next = match prev {
                            Some(prev_slot) if biased => choose_node2vec_step(
                                graph,
                                prev_slot,
                                curr,
                                neighbors,
                                (inv_p, inv_q),
                                &mut rng,
                            ),
                            // First step, or unbiased walk.
                            _ => choose_uniform_step(graph, curr, neighbors, &mut rng),
                        };

                        prev = Some(curr);
                        curr = next;
                        walk.push(graph.to_vid(curr));
                    }
                    local_walks.push(walk);
                }
                local_walks.into_iter()
            })
            .collect();

        RandomWalkResult { walks }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::test_utils::build_test_graph;
    use uni_common::core::id::Vid;

    fn vids(n: u64) -> Vec<Vid> {
        (0..n).map(Vid::from).collect()
    }

    /// S-6 (proposal §8, non-regression): hoisting the SplitMix64 finalizer into
    /// [`crate::algo::rng`] must not shift shipped walk streams. Recompute the
    /// exact pre-hoist inline formula and require byte-identical seeds — this
    /// pins the guard so a future edit to the shared finalizer that changed the
    /// mixing would fail here before it changed any user's walks.
    #[test]
    fn walk_seed_is_byte_identical_to_the_prehoist_formula() {
        let prehoist = |base: u64, start_slot: u32, walk_idx: usize| -> u64 {
            let mut s = base
                .wrapping_add((start_slot as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
                .wrapping_add((walk_idx as u64).wrapping_mul(0xD1B5_4A32_D192_ED03));
            s = (s ^ (s >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            s = (s ^ (s >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            s ^ (s >> 31)
        };
        for (base, slot, idx) in [
            (0u64, 0u32, 0usize),
            (1, 2, 3),
            (u64::MAX, 7, 11),
            (0x9E37_79B9_7F4A_7C15, 100, 1000),
            (DEFAULT_SEED, 42, 5),
        ] {
            assert_eq!(
                walk_seed(base, slot, idx),
                prehoist(base, slot, idx),
                "walk_seed drifted for ({base}, {slot}, {idx})"
            );
        }
    }

    /// A line + a triangle, undirected (edges added both ways) so node2vec's
    /// "is candidate adjacent to prev" test sees a symmetric neighbourhood.
    fn small_graph() -> GraphProjection {
        // 0 - 1 - 2 - 0 (triangle) and 2 - 3 - 4 (tail)
        let undirected = [(0, 1), (1, 2), (2, 0), (2, 3), (3, 4)];
        let mut edges = Vec::new();
        for (a, b) in undirected {
            edges.push((Vid::from(a), Vid::from(b)));
            edges.push((Vid::from(b), Vid::from(a)));
        }
        build_test_graph(vids(5), edges)
    }

    #[test]
    fn random_walk_is_deterministic_with_seed() {
        let graph = small_graph();
        let config = RandomWalkConfig {
            walk_length: 20,
            walks_per_node: 3,
            start_nodes: Vec::new(),
            return_param: 1.0,
            in_out_param: 1.0,
            seed: Some(42),
        };

        let a = RandomWalk::run(&graph, config.clone());
        let b = RandomWalk::run(&graph, config);

        assert_eq!(
            a.walks, b.walks,
            "identical seed must produce identical walks"
        );
    }

    #[test]
    fn random_walk_default_seed_is_deterministic() {
        let graph = small_graph();
        let config = RandomWalkConfig {
            walk_length: 15,
            walks_per_node: 2,
            seed: None, // => DEFAULT_SEED
            ..Default::default()
        };
        let a = RandomWalk::run(&graph, config.clone());
        let b = RandomWalk::run(&graph, config);
        assert_eq!(a.walks, b.walks, "default seed must be deterministic");
    }

    #[test]
    fn node2vec_return_param_biases_walk() {
        // With a tiny return param p (=> 1/p large), the walk should strongly
        // prefer returning to the previous node; with a large p it should
        // avoid it. Measure how often a step returns to where we just were.
        let graph = small_graph();

        let backtrack_rate = |p: f64| -> f64 {
            let cfg = RandomWalkConfig {
                walk_length: 40,
                walks_per_node: 40,
                start_nodes: Vec::new(),
                return_param: p,
                in_out_param: 1.0,
                seed: Some(7),
            };
            let res = RandomWalk::run(&graph, cfg);
            let mut returns = 0usize;
            let mut steps = 0usize;
            for walk in &res.walks {
                for i in 2..walk.len() {
                    steps += 1;
                    if walk[i] == walk[i - 2] {
                        returns += 1;
                    }
                }
            }
            if steps == 0 {
                0.0
            } else {
                returns as f64 / steps as f64
            }
        };

        let low_p = backtrack_rate(0.05); // 1/p = 20  => strongly prefer returning
        let high_p = backtrack_rate(20.0); // 1/p = 0.05 => avoid returning

        assert!(
            low_p > high_p + 0.1,
            "small return_param must increase backtracking (low_p={low_p:.3}, high_p={high_p:.3})"
        );
    }

    #[test]
    fn unbiased_walk_stays_on_graph() {
        let graph = small_graph();
        let config = RandomWalkConfig {
            walk_length: 10,
            walks_per_node: 5,
            seed: Some(1),
            ..Default::default()
        };
        let res = RandomWalk::run(&graph, config);
        // Every consecutive pair in a walk must be a real edge.
        for walk in &res.walks {
            for pair in walk.windows(2) {
                let u = graph.to_slot(pair[0]).unwrap();
                let v = graph.to_slot(pair[1]).unwrap();
                assert!(
                    graph.out_neighbors(u).contains(&v),
                    "walk traversed a non-edge {:?}->{:?}",
                    pair[0],
                    pair[1]
                );
            }
        }
    }
}
