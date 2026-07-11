// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! First-party graph algorithms authored against the [`GraphCompute`] catalog.
//!
//! These are the Phase-1 dogfood: each is written *only* through the public
//! [`GraphCompute`] trait — no downcast, no direct CSR access — exactly the way a
//! third-party guest plugin must author one (proposal §4.4, §8). They double as
//! the executable form of the "does the catalog express every F-row algorithm"
//! gate: if a signature could not express one, its function here would not
//! compile. They are differentially tested against independent naive oracles.
//!
//! Every intermediate handle is freed inside the loop so a long convergence run
//! does not pile dead maps against the arena cap (proposal §4.2 handle lifetime).
//
// Rust guideline compliant

use uni_common::core::id::Vid;
use uni_plugin::errors::FnError;

use super::error;
use super::handle::Handle;
use super::session::{
    Direction, EwiseOp, GraphCompute, MapOp, Norm, Predicate, ReduceOp, Semiring,
};
use super::value::{DType, Scalar};

/// Computes Personalized PageRank seeded at `seeds` (proposal §4.4).
///
/// Runs the damping/teleport power iteration with out-degree normalization and
/// dangling-mass redistribution — the details without which a "PPR" converges to
/// the wrong vector while the convergence test still reports success. Returns a
/// tensor handle holding the score map; the caller owns and must `free` it (or
/// `emit` it). The interpreter runs `O(iterations)` trivial steps while every
/// `O(E)` operation is a native kernel.
///
/// When `allow_partial` is false and the power iteration exhausts `max_iters`
/// without the L1 delta falling below `tol`, the result is *not* returned
/// silently — an `IterationLimit` error (`0x866`) is raised instead, mirroring
/// Locy's non-convergence contract (proposal §5.2). Pass `allow_partial = true`
/// for anytime semantics (return the last iterate).
///
/// # Errors
/// Returns a typed [`FnError`] on a bad handle, an unmapped seed (`0x868`), an
/// exhausted native-work budget / arena cap, or `0x866` on non-convergence when
/// `allow_partial` is false.
pub fn personalized_pagerank(
    gc: &mut dyn GraphCompute,
    g: Handle,
    seeds: &[Vid],
    alpha: f64,
    max_iters: usize,
    tol: f64,
    allow_partial: bool,
) -> Result<Handle, FnError> {
    // Uniform teleport over the seed set (each seed = 1/|seeds|).
    let seed_set = gc.frontier(g, seeds)?;
    let seed_map = gc.set_to_map(seed_set, Scalar::F64(1.0))?;
    let teleport = gc.map_apply(seed_map, MapOp::Normalize(Norm::L1))?;
    gc.free(seed_map)?;
    gc.free(seed_set)?;

    // inv_deg[u] = 1/outdeg(u), with recip(0) = 0 so dangling rows drop out.
    let deg = gc.degrees(g, Direction::Out)?;
    let inv_deg = gc.map_apply(deg, MapOp::Recip)?;
    let dangling = gc.map_to_set(deg, Predicate::IsZero)?;
    gc.free(deg)?;

    // rank := teleport (a fresh copy, so freeing rank never frees teleport).
    let mut rank = gc.map_apply(teleport, MapOp::Scale(1.0))?;
    let mut converged = false;
    for _ in 0..max_iters {
        let contrib = gc.ewise(rank, inv_deg, EwiseOp::Mul)?;
        let spread = gc.spmv(g, contrib, Semiring::LinearAlgebra, Direction::Out, None)?;
        gc.free(contrib)?;
        let dm = gc.reduce(rank, ReduceOp::Sum, Some(dangling))?.as_f64();
        let scaled = gc.map_apply(spread, MapOp::Scale(alpha))?;
        gc.free(spread)?;
        // next = alpha*spread + (1 - alpha + alpha*dm) * teleport.
        let blend = 1.0 - alpha + alpha * dm;
        let next = gc.ewise(scaled, teleport, EwiseOp::Axpy(blend))?;
        gc.free(scaled)?;
        let diff = gc.l1_diff(rank, next)?;
        gc.free(rank)?;
        rank = next;
        if diff < tol {
            converged = true;
            break;
        }
    }

    gc.free(teleport)?;
    gc.free(inv_deg)?;
    gc.free(dangling)?;
    if !converged && !allow_partial {
        gc.free(rank)?;
        return Err(error::iteration_limit(max_iters));
    }
    Ok(rank)
}

/// Computes the set of vertices reachable from `seeds` via BFS expansion.
///
/// Returns a vertex-set handle of the reachable slots. Each hop is one native
/// `expand` charged Σ frontier degree; the guest loop runs `O(diameter)` steps.
///
/// # Errors
/// Returns a typed [`FnError`] on a bad handle, an unmapped seed, or exhausted
/// resources.
pub fn reachable_set(
    gc: &mut dyn GraphCompute,
    g: Handle,
    seeds: &[Vid],
    dir: Direction,
) -> Result<Handle, FnError> {
    let mut visited = gc.frontier(g, seeds)?;
    let mut frontier = gc.frontier(g, seeds)?;
    loop {
        let next = gc.expand(g, frontier, dir, Some(visited))?;
        gc.free(frontier)?;
        if gc.is_empty(next)? {
            gc.free(next)?;
            break;
        }
        let grown = gc.set_union(visited, next)?;
        gc.free(visited)?;
        visited = grown;
        frontier = next;
    }
    Ok(visited)
}

/// Computes weakly-connected-component labels by min-label propagation.
///
/// Each vertex is initialized to its own slot id and repeatedly takes the
/// minimum label among itself and its in/out neighbors until a fixpoint. Returns
/// a tensor handle whose value at each slot is that component's minimum slot id.
///
/// # Errors
/// Returns a typed [`FnError`] on a bad handle or exhausted resources.
pub fn wcc_labels(
    gc: &mut dyn GraphCompute,
    g: Handle,
    max_iters: usize,
) -> Result<Handle, FnError> {
    let mut labels = gc.vertex_ids(g)?;
    for _ in 0..max_iters {
        let out_prop = gc.spmv(g, labels, Semiring::Propagate, Direction::Out, None)?;
        let in_prop = gc.spmv(g, labels, Semiring::Propagate, Direction::In, None)?;
        let both = gc.ewise(out_prop, in_prop, EwiseOp::Min)?;
        gc.free(out_prop)?;
        gc.free(in_prop)?;
        let next = gc.ewise(labels, both, EwiseOp::Min)?;
        gc.free(both)?;
        let diff = gc.l1_diff(labels, next)?;
        gc.free(labels)?;
        labels = next;
        if diff == 0.0 {
            break;
        }
    }
    Ok(labels)
}

/// Computes single-source shortest paths via Bellman-Ford relaxation.
///
/// Initializes the source to 0 and all others to +∞, then relaxes every edge
/// through the tropical `ShortestPath` semiring until a fixpoint. Returns a
/// tensor handle of distances (+∞ for unreachable vertices).
///
/// # Errors
/// Returns a typed [`FnError`] on a bad handle, an unmapped source, or exhausted
/// resources.
pub fn bellman_ford(
    gc: &mut dyn GraphCompute,
    g: Handle,
    source: Vid,
    max_iters: usize,
) -> Result<Handle, FnError> {
    // dist := +∞ everywhere, then 0 at the source.
    let base = gc.zero_map(g, DType::F64)?;
    let inf = gc.map_apply(base, MapOp::AxPlusB(0.0, f64::INFINITY))?;
    gc.free(base)?;
    let src_set = gc.frontier(g, &[source])?;
    let mut dist = gc.scatter(inf, src_set, Scalar::F64(0.0))?;
    gc.free(inf)?;
    gc.free(src_set)?;

    for _ in 0..max_iters {
        let relaxed = gc.spmv(g, dist, Semiring::ShortestPath, Direction::Out, None)?;
        let next = gc.ewise(dist, relaxed, EwiseOp::Min)?;
        gc.free(relaxed)?;
        let diff = gc.l1_diff(dist, next)?;
        gc.free(dist)?;
        dist = next;
        if diff == 0.0 {
            break;
        }
    }
    Ok(dist)
}

/// Computes the k-core: the maximal vertex set where every member keeps at
/// least `k` surviving neighbors, by iterated synchronous peeling.
///
/// Each round recomputes every vertex's surviving-neighbor count as
/// `spmv(In) + spmv(Out)` over the membership indicator and peels those below
/// `k`; multiplying by the prior membership makes peeling monotone. Returns the
/// surviving-vertex set. Degree counts out-neighbors and in-neighbors with
/// multiplicity (the directed analogue of undirected degree).
///
/// # Errors
/// Returns a typed [`FnError`] on a bad handle or exhausted resources.
pub fn k_core(
    gc: &mut dyn GraphCompute,
    g: Handle,
    k: u32,
    max_iters: usize,
) -> Result<Handle, FnError> {
    // member[v] = 1.0 while v is in the core, 0.0 once peeled.
    let zero = gc.zero_map(g, DType::F64)?;
    let mut member = gc.map_apply(zero, MapOp::AxPlusB(0.0, 1.0))?;
    gc.free(zero)?;
    let threshold = f64::from(k) - 0.5; // eff_deg (integer) >= k  ⟺  > k - 0.5

    for _ in 0..max_iters {
        // eff_out[v] = Σ member over out-neighbors of v  = spmv(In, member);
        // eff_in[v]  = Σ member over in-neighbors of v   = spmv(Out, member).
        let eff_out = gc.spmv(g, member, Semiring::LinearAlgebra, Direction::In, None)?;
        let eff_in = gc.spmv(g, member, Semiring::LinearAlgebra, Direction::Out, None)?;
        let eff_deg = gc.ewise(eff_out, eff_in, EwiseOp::Add)?;
        gc.free(eff_out)?;
        gc.free(eff_in)?;

        let keep_set = gc.map_to_set(eff_deg, Predicate::Gt(threshold))?;
        gc.free(eff_deg)?;
        let keep_map = gc.set_to_map(keep_set, Scalar::F64(1.0))?;
        gc.free(keep_set)?;
        // Monotone: a peeled vertex (member 0) stays peeled regardless of keep.
        let next = gc.ewise(member, keep_map, EwiseOp::Mul)?;
        gc.free(keep_map)?;
        let diff = gc.l1_diff(member, next)?;
        gc.free(member)?;
        member = next;
        if diff == 0.0 {
            break;
        }
    }

    let surviving = gc.map_to_set(member, Predicate::Gt(0.5))?;
    gc.free(member)?;
    Ok(surviving)
}

/// Computes eigenvector centrality by normalized power iteration.
///
/// Iterates `x_{k+1} = normalize_L2(A^T x_k)` — a vertex is central when central
/// vertices point to it — converging to the principal eigenvector. Exercises the
/// `LinearAlgebra` semiring and L2 normalization; the same shape yields Katz and
/// HITS. Returns the normalized centrality map.
///
/// # Errors
/// Returns a typed [`FnError`] on a bad handle or exhausted resources.
pub fn eigenvector_centrality(
    gc: &mut dyn GraphCompute,
    g: Handle,
    max_iters: usize,
    tol: f64,
) -> Result<Handle, FnError> {
    // Start from a uniform, L2-normalized vector (ones then normalize).
    let zero = gc.zero_map(g, DType::F64)?;
    let ones = gc.map_apply(zero, MapOp::AxPlusB(0.0, 1.0))?;
    gc.free(zero)?;
    let mut x = gc.map_apply(ones, MapOp::Normalize(Norm::L2))?;
    gc.free(ones)?;

    for _ in 0..max_iters {
        // spmv(Out) accumulates each source's value at its targets, so
        // next[v] = Σ_{u -> v} x[u] — the in-neighbor sum eigenvector uses.
        let ax = gc.spmv(g, x, Semiring::LinearAlgebra, Direction::Out, None)?;
        let next = gc.map_apply(ax, MapOp::Normalize(Norm::L2))?;
        gc.free(ax)?;
        let diff = gc.l1_diff(x, next)?;
        gc.free(x)?;
        x = next;
        if diff < tol {
            break;
        }
    }
    Ok(x)
}
