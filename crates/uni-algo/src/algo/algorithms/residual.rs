// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Shared residual-graph construction for the max-flow algorithms.

use crate::algo::GraphProjection;

/// One arc of a residual graph: the forward arc carries the edge's
/// capacity, its paired back-edge starts at capacity `0`.
#[derive(Clone, Copy)]
pub(super) struct ResidualEdge {
    pub(super) to: usize,
    pub(super) rev: usize, // index of reverse edge in adj[to]
    pub(super) cap: f64,
    pub(super) flow: f64,
}

/// Build the residual adjacency for `graph`.
///
/// For every edge `u -> v` with capacity `C` this pushes `u -> v`
/// (cap `C`, flow 0) and the paired back-edge `v -> u` (cap 0, flow 0).
/// Capacities come from the projection's weights when present, else `1.0`.
pub(super) fn build_residual(graph: &GraphProjection) -> Vec<Vec<ResidualEdge>> {
    let n = graph.vertex_count();
    let mut adj: Vec<Vec<ResidualEdge>> = (0..n).map(|_| Vec::new()).collect();

    for u in 0..n {
        for (i, &v_u32) in graph.out_neighbors(u as u32).iter().enumerate() {
            let v = v_u32 as usize;
            let cap = if graph.has_weights() {
                graph.out_weight(u as u32, i)
            } else {
                1.0
            };

            let a_len = adj[u].len();
            let b_len = adj[v].len();

            adj[u].push(ResidualEdge {
                to: v,
                rev: b_len,
                cap,
                flow: 0.0,
            });
            adj[v].push(ResidualEdge {
                to: u,
                rev: a_len,
                cap: 0.0, // Back edge capacity 0
                flow: 0.0,
            });
        }
    }

    adj
}
