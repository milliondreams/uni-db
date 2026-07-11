// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! GraphCompute kernel surface for Rhai guest algorithms.
//!
//! Exposes the coarse GraphCompute kernels (proposal §4.3, `graph-compute@1`) to
//! a Rhai script as methods on an opaque [`GcSession`] handle. The guest holds
//! only integer handles and the session object — no vertex data ever crosses
//! into the interpreter ("conductor, not worker", proposal §4.5). Each method
//! locks the per-CALL [`AlgoSession`], drives one native kernel, and returns a
//! packed handle (as an `i64`) or a scalar. The native-work budget and arena cap
//! carried by the session make a runaway guest loop fail closed exactly as they
//! do for the first-party provider (proposal §5.1).
//!
//! Handles cross the boundary as `i64` (the packed `u64` reinterpreted); the
//! handle table validates every one, so a forged or stale integer becomes a
//! typed Rhai runtime error, never an out-of-bounds access (proposal §4.2).
//
// Rust guideline compliant

#![cfg(feature = "rhai-runtime")]

use std::sync::Arc;

use parking_lot::Mutex;
use rhai::{Array, Engine, EvalAltResult, ImmutableString, Position};
use uni_common::core::id::Vid;
use uni_plugin::errors::FnError;
use uni_plugin_builtin::algorithms::graph_compute::handle::Handle;
use uni_plugin_builtin::algorithms::graph_compute::session::{
    AlgoSession, Direction, EwiseOp, GraphCompute, MapOp, Norm, Predicate, ReduceOp, Semiring,
};
use uni_plugin_builtin::algorithms::graph_compute::value::Scalar;

/// A Rhai-visible handle to a per-CALL GraphCompute session.
///
/// Cheap to clone (shares the inner `Arc<Mutex<AlgoSession>>`), as required by
/// Rhai's `sync` feature. The `graph` field is the handle of the projected graph
/// the guest algorithm runs over, exposed via [`GcSession::graph`].
#[derive(Clone)]
pub struct GcSession {
    session: Arc<Mutex<AlgoSession>>,
    graph: i64,
}

impl std::fmt::Debug for GcSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcSession").finish_non_exhaustive()
    }
}

/// Wraps a shared session and its bound graph handle for the Rhai entrypoint.
#[must_use]
pub fn new_session(session: Arc<Mutex<AlgoSession>>, graph: Handle) -> GcSession {
    GcSession {
        session,
        graph: to_i64(graph),
    }
}

/// Packs a handle into the `i64` the guest holds.
fn to_i64(h: Handle) -> i64 {
    // Reinterpret the packed u64 as i64; the round-trip is bit-exact.
    #[expect(
        clippy::cast_possible_wrap,
        reason = "opaque handle round-trips bit-exact"
    )]
    let v = h.as_u64() as i64;
    v
}

/// Reconstructs a handle from the guest's `i64`.
fn from_i64(v: i64) -> Handle {
    #[expect(clippy::cast_sign_loss, reason = "opaque handle round-trips bit-exact")]
    let bits = v as u64;
    Handle::from_u64(bits)
}

/// Converts a kernel [`FnError`] into a Rhai runtime error.
fn rt(e: FnError) -> Box<EvalAltResult> {
    Box::new(EvalAltResult::ErrorRuntime(
        rhai::Dynamic::from(format!("GraphCompute: {e}")),
        Position::NONE,
    ))
}

/// Parses a direction string (`"out"` / `"in"`).
fn dir(s: &str) -> Result<Direction, Box<EvalAltResult>> {
    match s {
        "out" => Ok(Direction::Out),
        "in" => Ok(Direction::In),
        other => Err(rt(FnError::new(0x861, format!("bad direction `{other}`")))),
    }
}

/// Parses a semiring string.
fn semiring(s: &str) -> Result<Semiring, Box<EvalAltResult>> {
    match s {
        "reachability" => Ok(Semiring::Reachability),
        "shortest_path" => Ok(Semiring::ShortestPath),
        "propagate" => Ok(Semiring::Propagate),
        "linear_algebra" => Ok(Semiring::LinearAlgebra),
        "min_max" => Ok(Semiring::MinMax),
        other => Err(rt(FnError::new(0x861, format!("bad semiring `{other}`")))),
    }
}

impl GcSession {
    /// Returns the bound graph handle.
    fn graph_handle(&mut self) -> i64 {
        self.graph
    }

    /// Vertex count of a graph handle.
    fn vertex_count(&mut self, g: i64) -> Result<i64, Box<EvalAltResult>> {
        let s = self.session.lock();
        s.vertex_count(from_i64(g))
            .map(|v| i64::try_from(v).unwrap_or(i64::MAX))
            .map_err(rt)
    }

    /// Builds a frontier from an array of external vertex ids.
    fn frontier(&mut self, g: i64, seeds: Array) -> Result<i64, Box<EvalAltResult>> {
        let vids: Vec<Vid> = seeds
            .into_iter()
            .map(|d| {
                d.as_int()
                    .map(|i| {
                        #[expect(clippy::cast_sign_loss, reason = "vertex ids are non-negative")]
                        let u = i as u64;
                        Vid::new(u)
                    })
                    .map_err(|_| rt(FnError::new(0x802, "frontier: seed must be an integer")))
            })
            .collect::<Result<_, _>>()?;
        let mut s = self.session.lock();
        s.frontier(from_i64(g), &vids).map(to_i64).map_err(rt)
    }

    /// Per-vertex degree map in `dir`.
    fn degrees(&mut self, g: i64, d: ImmutableString) -> Result<i64, Box<EvalAltResult>> {
        let direction = dir(d.as_str())?;
        let mut s = self.session.lock();
        s.degrees(from_i64(g), direction).map(to_i64).map_err(rt)
    }

    /// Per-vertex own-slot-id map (WCC init).
    fn vertex_ids(&mut self, g: i64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.vertex_ids(from_i64(g)).map(to_i64).map_err(rt)
    }

    /// Lifts a set into a map assigning `value` to members.
    fn set_to_map(&mut self, set: i64, value: f64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.set_to_map(from_i64(set), Scalar::F64(value))
            .map(to_i64)
            .map_err(rt)
    }

    /// Lowers a map into the set matching a predicate (`is_zero`/`gt`/`lt`/`eq`).
    fn map_to_set(
        &mut self,
        m: i64,
        pred: ImmutableString,
        threshold: f64,
    ) -> Result<i64, Box<EvalAltResult>> {
        let p = match pred.as_str() {
            "is_zero" => Predicate::IsZero,
            "gt" => Predicate::Gt(threshold),
            "lt" => Predicate::Lt(threshold),
            "eq" => Predicate::Eq(threshold),
            other => return Err(rt(FnError::new(0x861, format!("bad predicate `{other}`")))),
        };
        let mut s = self.session.lock();
        s.map_to_set(from_i64(m), p).map(to_i64).map_err(rt)
    }

    /// Reciprocal map, with `recip(0) = 0` (dangling rows drop out).
    fn recip(&mut self, m: i64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.map_apply(from_i64(m), MapOp::Recip)
            .map(to_i64)
            .map_err(rt)
    }

    /// Scales a map by a constant.
    fn scale(&mut self, m: i64, a: f64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.map_apply(from_i64(m), MapOp::Scale(a))
            .map(to_i64)
            .map_err(rt)
    }

    /// Normalizes a map to unit L1 or L2 norm.
    fn normalize(&mut self, m: i64, norm: ImmutableString) -> Result<i64, Box<EvalAltResult>> {
        let n = match norm.as_str() {
            "l1" => Norm::L1,
            "l2" => Norm::L2,
            other => return Err(rt(FnError::new(0x861, format!("bad norm `{other}`")))),
        };
        let mut s = self.session.lock();
        s.map_apply(from_i64(m), MapOp::Normalize(n))
            .map(to_i64)
            .map_err(rt)
    }

    /// Element-wise combine (`mul`/`add`/`min`/`max`/`axpy`); `coef` is used by axpy.
    fn ewise(
        &mut self,
        a: i64,
        b: i64,
        op: ImmutableString,
        coef: f64,
    ) -> Result<i64, Box<EvalAltResult>> {
        let o = match op.as_str() {
            "mul" => EwiseOp::Mul,
            "add" => EwiseOp::Add,
            "min" => EwiseOp::Min,
            "max" => EwiseOp::Max,
            "axpy" => EwiseOp::Axpy(coef),
            other => return Err(rt(FnError::new(0x861, format!("bad ewise op `{other}`")))),
        };
        let mut s = self.session.lock();
        s.ewise(from_i64(a), from_i64(b), o).map(to_i64).map_err(rt)
    }

    /// Sparse mat-vec under a named semiring and direction.
    fn spmv(
        &mut self,
        g: i64,
        vec: i64,
        sr: ImmutableString,
        d: ImmutableString,
    ) -> Result<i64, Box<EvalAltResult>> {
        let semi = semiring(sr.as_str())?;
        let direction = dir(d.as_str())?;
        let mut s = self.session.lock();
        s.spmv(from_i64(g), from_i64(vec), semi, direction, None)
            .map(to_i64)
            .map_err(rt)
    }

    /// Sum reduction over a map.
    fn reduce_sum(&mut self, m: i64) -> Result<f64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.reduce(from_i64(m), ReduceOp::Sum, None)
            .map(Scalar::as_f64)
            .map_err(rt)
    }

    /// Sum reduction over a map, restricted to a mask set.
    fn reduce_sum_masked(&mut self, m: i64, mask: i64) -> Result<f64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.reduce(from_i64(m), ReduceOp::Sum, Some(from_i64(mask)))
            .map(Scalar::as_f64)
            .map_err(rt)
    }

    /// L1 distance between two maps (a convergence test).
    fn l1_diff(&mut self, a: i64, b: i64) -> Result<f64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.l1_diff(from_i64(a), from_i64(b)).map_err(rt)
    }

    /// One-hop expansion of a frontier, excluding a visited mask.
    fn expand(
        &mut self,
        g: i64,
        frontier: i64,
        d: ImmutableString,
        exclude: i64,
    ) -> Result<i64, Box<EvalAltResult>> {
        let direction = dir(d.as_str())?;
        let mut s = self.session.lock();
        s.expand(
            from_i64(g),
            from_i64(frontier),
            direction,
            Some(from_i64(exclude)),
        )
        .map(to_i64)
        .map_err(rt)
    }

    /// Set union.
    fn set_union(&mut self, a: i64, b: i64) -> Result<i64, Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.set_union(from_i64(a), from_i64(b))
            .map(to_i64)
            .map_err(rt)
    }

    /// Set cardinality.
    fn set_len(&mut self, set: i64) -> Result<i64, Box<EvalAltResult>> {
        let s = self.session.lock();
        s.set_len(from_i64(set))
            .map(|v| i64::try_from(v).unwrap_or(i64::MAX))
            .map_err(rt)
    }

    /// Whether a set is empty.
    fn is_empty(&mut self, set: i64) -> Result<bool, Box<EvalAltResult>> {
        let s = self.session.lock();
        s.is_empty(from_i64(set)).map_err(rt)
    }

    /// Frees a handle.
    fn free(&mut self, h: i64) -> Result<(), Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.free(from_i64(h)).map_err(rt)
    }

    /// Emits a single named per-vertex column into the result sink.
    fn emit(&mut self, name: ImmutableString, h: i64) -> Result<(), Box<EvalAltResult>> {
        let mut s = self.session.lock();
        s.emit(&[(name.as_str(), from_i64(h))]).map_err(rt)
    }
}

/// Registers the [`GcSession`] type and its kernel methods on `engine`.
///
/// Always registered when the GraphCompute surface is available; the capability
/// gate is enforced at projection time on the host side (proposal §4.6), and a
/// guest that never receives a [`GcSession`] cannot call any method.
pub fn register_graph_compute(engine: &mut Engine) {
    engine
        .register_type_with_name::<GcSession>("GcSession")
        .register_fn("graph", GcSession::graph_handle)
        .register_fn("vertex_count", GcSession::vertex_count)
        .register_fn("frontier", GcSession::frontier)
        .register_fn("degrees", GcSession::degrees)
        .register_fn("vertex_ids", GcSession::vertex_ids)
        .register_fn("set_to_map", GcSession::set_to_map)
        .register_fn("map_to_set", GcSession::map_to_set)
        .register_fn("recip", GcSession::recip)
        .register_fn("scale", GcSession::scale)
        .register_fn("normalize", GcSession::normalize)
        .register_fn("ewise", GcSession::ewise)
        .register_fn("spmv", GcSession::spmv)
        .register_fn("reduce_sum", GcSession::reduce_sum)
        .register_fn("reduce_sum_masked", GcSession::reduce_sum_masked)
        .register_fn("l1_diff", GcSession::l1_diff)
        .register_fn("expand", GcSession::expand)
        .register_fn("set_union", GcSession::set_union)
        .register_fn("set_len", GcSession::set_len)
        .register_fn("is_empty", GcSession::is_empty)
        .register_fn("free", GcSession::free)
        .register_fn("emit", GcSession::emit);
}
