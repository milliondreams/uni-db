// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! GraphCompute kernel surface for Python guest algorithms.
//!
//! Exposes the coarse GraphCompute kernels (proposal §4.3) to a Python guest as
//! methods on a `#[pyclass] GcSession`. The guest holds only integer handles and
//! the session object — no vertex data crosses into the interpreter ("conductor,
//! not worker", proposal §4.5). Passing the session as the guest's first argument
//! is what closes the PyO3 loader's "no query-time host callback" gap (§4.5).
//!
//! Loop bounding has two layers. The **cooperative** layer is the per-kernel
//! [`deadline`] check here: it fires the moment a guest calls a kernel past the
//! deadline (typed `Timeout`, `0x867`) — cheap, but it alone cannot stop a guest
//! spinning in pure Python (`while True: pass`) since that never calls back. The
//! **forced** layer is the wall-clock watchdog in [`crate::watchdog`], armed by
//! the adapter, which injects `KeyboardInterrupt` into the guest thread at the
//! deadline and does bound a pure-Python spin loop. The native-work budget on the
//! session is the third, work-proportional bound (proposal §5.1). Caveat: a guest
//! blocked in a C extension that never yields the GIL is not interruptible — the
//! same limitation as CPython's own `KeyboardInterrupt`.
//!
//! [`deadline`]: GcSession
//
// Rust guideline compliant

#![cfg(feature = "pyo3")]

use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use uni_common::core::id::Vid;
use uni_plugin::errors::FnError;
use uni_plugin_builtin::algorithms::graph_compute::handle::Handle;
use uni_plugin_builtin::algorithms::graph_compute::session::{
    AlgoSession, Direction, EwiseOp, GraphCompute, MapOp, Norm, Predicate, ReduceOp, Semiring,
};
use uni_plugin_builtin::algorithms::graph_compute::value::{DType, Scalar};

/// A Python-visible handle to a per-CALL GraphCompute session.
#[pyclass]
pub struct GcSession {
    session: Arc<Mutex<AlgoSession>>,
    graph: i64,
    deadline: Option<Instant>,
}

impl std::fmt::Debug for GcSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcSession")
            .field("graph", &self.graph)
            .finish_non_exhaustive()
    }
}

/// Builds a session object for the guest, with an optional wall-clock deadline.
#[must_use]
pub fn new_session(
    session: Arc<Mutex<AlgoSession>>,
    graph: Handle,
    deadline: Option<Instant>,
) -> GcSession {
    GcSession {
        session,
        graph: to_i64(graph),
        deadline,
    }
}

fn to_i64(h: Handle) -> i64 {
    #[expect(
        clippy::cast_possible_wrap,
        reason = "opaque handle round-trips bit-exact"
    )]
    let v = h.as_u64() as i64;
    v
}

fn from_i64(v: i64) -> Handle {
    #[expect(clippy::cast_sign_loss, reason = "opaque handle round-trips bit-exact")]
    let bits = v as u64;
    Handle::from_u64(bits)
}

fn py_err(e: FnError) -> PyErr {
    PyRuntimeError::new_err(format!("GraphCompute (0x{:x}): {}", e.code, e.message))
}

fn dir(s: &str) -> PyResult<Direction> {
    match s {
        "out" => Ok(Direction::Out),
        "in" => Ok(Direction::In),
        other => Err(PyRuntimeError::new_err(format!("bad direction `{other}`"))),
    }
}

fn semiring(s: &str) -> PyResult<Semiring> {
    match s {
        "reachability" => Ok(Semiring::Reachability),
        "shortest_path" => Ok(Semiring::ShortestPath),
        "propagate" => Ok(Semiring::Propagate),
        "linear_algebra" => Ok(Semiring::LinearAlgebra),
        "min_max" => Ok(Semiring::MinMax),
        other => Err(PyRuntimeError::new_err(format!("bad semiring `{other}`"))),
    }
}

/// Packs an external vertex id into the `i64` a guest holds.
fn vid_to_i64(vid: Vid) -> i64 {
    #[expect(clippy::cast_possible_wrap, reason = "vids fit i64 in practice")]
    let v = vid.as_u64() as i64;
    v
}

/// Parses a generic `map_apply` op string with its scalar operands.
fn map_op(s: &str, a: f64, b: f64) -> PyResult<MapOp> {
    match s {
        "recip" => Ok(MapOp::Recip),
        "scale" => Ok(MapOp::Scale(a)),
        "log" => Ok(MapOp::Log),
        "affine" => Ok(MapOp::AxPlusB(a, b)),
        "normalize_l1" => Ok(MapOp::Normalize(Norm::L1)),
        "normalize_l2" => Ok(MapOp::Normalize(Norm::L2)),
        other => Err(PyRuntimeError::new_err(format!("bad map op `{other}`"))),
    }
}

#[pymethods]
impl GcSession {
    /// The bound graph handle.
    fn graph(&self) -> PyResult<i64> {
        self.check_deadline()?;
        Ok(self.graph)
    }

    /// Vertex count of a graph handle.
    fn vertex_count(&self, g: i64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .vertex_count(from_i64(g))
            .map(|v| i64::try_from(v).unwrap_or(i64::MAX))
            .map_err(py_err)
    }

    /// Builds a frontier from a list of external vertex ids.
    fn frontier(&self, g: i64, seeds: Vec<i64>) -> PyResult<i64> {
        self.check_deadline()?;
        #[expect(clippy::cast_sign_loss, reason = "vertex ids are non-negative")]
        let vids: Vec<Vid> = seeds.into_iter().map(|i| Vid::new(i as u64)).collect();
        self.session
            .lock()
            .frontier(from_i64(g), &vids)
            .map(to_i64)
            .map_err(py_err)
    }

    /// Per-vertex degree map in `direction` (`"out"`/`"in"`).
    fn degrees(&self, g: i64, direction: &str) -> PyResult<i64> {
        self.check_deadline()?;
        let d = dir(direction)?;
        self.session
            .lock()
            .degrees(from_i64(g), d)
            .map(to_i64)
            .map_err(py_err)
    }

    /// Per-vertex own-slot-id map (WCC init).
    fn vertex_ids(&self, g: i64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .vertex_ids(from_i64(g))
            .map(to_i64)
            .map_err(py_err)
    }

    /// Lifts a set into a map assigning `value` to members.
    fn set_to_map(&self, s: i64, value: f64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .set_to_map(from_i64(s), Scalar::F64(value))
            .map(to_i64)
            .map_err(py_err)
    }

    /// Lowers a map into the set matching `pred` (`is_zero`/`gt`/`lt`/`eq`).
    fn map_to_set(&self, m: i64, pred: &str, threshold: f64) -> PyResult<i64> {
        self.check_deadline()?;
        let p = match pred {
            "is_zero" => Predicate::IsZero,
            "gt" => Predicate::Gt(threshold),
            "lt" => Predicate::Lt(threshold),
            "eq" => Predicate::Eq(threshold),
            other => return Err(PyRuntimeError::new_err(format!("bad predicate `{other}`"))),
        };
        self.session
            .lock()
            .map_to_set(from_i64(m), p)
            .map(to_i64)
            .map_err(py_err)
    }

    /// Reciprocal map, with `recip(0) = 0`.
    fn recip(&self, m: i64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .map_apply(from_i64(m), MapOp::Recip)
            .map(to_i64)
            .map_err(py_err)
    }

    /// Scales a map by a constant.
    fn scale(&self, m: i64, a: f64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .map_apply(from_i64(m), MapOp::Scale(a))
            .map(to_i64)
            .map_err(py_err)
    }

    /// Normalizes a map to unit L1 or L2 norm.
    fn normalize(&self, m: i64, norm: &str) -> PyResult<i64> {
        self.check_deadline()?;
        let n = match norm {
            "l1" => Norm::L1,
            "l2" => Norm::L2,
            other => return Err(PyRuntimeError::new_err(format!("bad norm `{other}`"))),
        };
        self.session
            .lock()
            .map_apply(from_i64(m), MapOp::Normalize(n))
            .map(to_i64)
            .map_err(py_err)
    }

    /// Element-wise combine (`mul`/`add`/`min`/`max`/`axpy`); `coef` is for axpy.
    #[pyo3(signature = (a, b, op, coef=0.0))]
    fn ewise(&self, a: i64, b: i64, op: &str, coef: f64) -> PyResult<i64> {
        self.check_deadline()?;
        let o = match op {
            "mul" => EwiseOp::Mul,
            "add" => EwiseOp::Add,
            "min" => EwiseOp::Min,
            "max" => EwiseOp::Max,
            "axpy" => EwiseOp::Axpy(coef),
            other => return Err(PyRuntimeError::new_err(format!("bad ewise op `{other}`"))),
        };
        self.session
            .lock()
            .ewise(from_i64(a), from_i64(b), o)
            .map(to_i64)
            .map_err(py_err)
    }

    /// Sparse mat-vec under a named semiring and direction.
    fn spmv(&self, g: i64, vec: i64, sr: &str, direction: &str) -> PyResult<i64> {
        self.check_deadline()?;
        let semi = semiring(sr)?;
        let d = dir(direction)?;
        self.session
            .lock()
            .spmv(from_i64(g), from_i64(vec), semi, d, None)
            .map(to_i64)
            .map_err(py_err)
    }

    /// Sum reduction over a map.
    fn reduce_sum(&self, m: i64) -> PyResult<f64> {
        self.check_deadline()?;
        self.session
            .lock()
            .reduce(from_i64(m), ReduceOp::Sum, None)
            .map(Scalar::as_f64)
            .map_err(py_err)
    }

    /// Sum reduction over a map, restricted to a mask set.
    fn reduce_sum_masked(&self, m: i64, mask: i64) -> PyResult<f64> {
        self.check_deadline()?;
        self.session
            .lock()
            .reduce(from_i64(m), ReduceOp::Sum, Some(from_i64(mask)))
            .map(Scalar::as_f64)
            .map_err(py_err)
    }

    /// L1 distance between two maps (a convergence test).
    fn l1_diff(&self, a: i64, b: i64) -> PyResult<f64> {
        self.check_deadline()?;
        self.session
            .lock()
            .l1_diff(from_i64(a), from_i64(b))
            .map_err(py_err)
    }

    /// One-hop expansion of a frontier, excluding a visited mask.
    fn expand(&self, g: i64, frontier: i64, direction: &str, exclude: i64) -> PyResult<i64> {
        self.check_deadline()?;
        let d = dir(direction)?;
        self.session
            .lock()
            .expand(from_i64(g), from_i64(frontier), d, Some(from_i64(exclude)))
            .map(to_i64)
            .map_err(py_err)
    }

    /// Set union.
    fn set_union(&self, a: i64, b: i64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .set_union(from_i64(a), from_i64(b))
            .map(to_i64)
            .map_err(py_err)
    }

    /// Set cardinality.
    fn set_len(&self, s: i64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .set_len(from_i64(s))
            .map(|v| i64::try_from(v).unwrap_or(i64::MAX))
            .map_err(py_err)
    }

    /// Whether a set is empty.
    fn is_empty(&self, s: i64) -> PyResult<bool> {
        self.check_deadline()?;
        self.session.lock().is_empty(from_i64(s)).map_err(py_err)
    }

    /// Frees a handle.
    fn free(&self, h: i64) -> PyResult<()> {
        self.check_deadline()?;
        self.session.lock().free(from_i64(h)).map_err(py_err)
    }

    /// Emits a single named per-vertex column into the result sink.
    fn emit(&self, name: &str, h: i64) -> PyResult<()> {
        self.check_deadline()?;
        self.session
            .lock()
            .emit(&[(name, from_i64(h))])
            .map_err(py_err)
    }

    /// Generic map transform (`recip`/`scale`/`log`/`affine`/`normalize_l1|l2`);
    /// `a`,`b` are the scalar operands (`scale a`, `affine a*x+b`).
    #[pyo3(signature = (m, op, a = 0.0, b = 0.0))]
    fn map_apply(&self, m: i64, op: &str, a: f64, b: f64) -> PyResult<i64> {
        self.check_deadline()?;
        let o = map_op(op, a, b)?;
        self.session
            .lock()
            .map_apply(from_i64(m), o)
            .map(to_i64)
            .map_err(py_err)
    }

    /// A zeroed map over the graph's vertices (`dtype` = `"f64"` or `"i64"`).
    ///
    /// An `"i64"` map seeds an exact integer path-counting run (F-9).
    #[pyo3(signature = (g, dtype = "f64"))]
    fn zero_map(&self, g: i64, dtype: &str) -> PyResult<i64> {
        self.check_deadline()?;
        let ty = if dtype == "i64" {
            DType::I64
        } else {
            DType::F64
        };
        self.session
            .lock()
            .zero_map(from_i64(g), ty)
            .map(to_i64)
            .map_err(py_err)
    }

    /// Overwrites `map` at each `frontier` member with `value`.
    fn scatter(&self, map: i64, frontier: i64, value: f64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .scatter(from_i64(map), from_i64(frontier), Scalar::F64(value))
            .map(to_i64)
            .map_err(py_err)
    }

    /// Set difference `a \ b`.
    fn set_diff(&self, a: i64, b: i64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .set_diff(from_i64(a), from_i64(b))
            .map(to_i64)
            .map_err(py_err)
    }

    /// Set intersection `a & b`.
    fn set_intersect(&self, a: i64, b: i64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .set_intersect(from_i64(a), from_i64(b))
            .map(to_i64)
            .map_err(py_err)
    }

    /// The `(vertexId, value)` extremum of a map (`want_max` selects max vs min).
    fn arg_extreme(&self, m: i64, want_max: bool) -> PyResult<(i64, f64)> {
        self.check_deadline()?;
        let (vid, val) = self
            .session
            .lock()
            .arg_extreme(from_i64(m), want_max)
            .map_err(py_err)?;
        Ok((vid_to_i64(vid), val.as_f64()))
    }

    /// The top-`k` `(vertexId, value)` pairs by descending value.
    fn topk(&self, m: i64, k: u32) -> PyResult<Vec<(i64, f64)>> {
        self.check_deadline()?;
        let ranked = self.session.lock().topk(from_i64(m), k).map_err(py_err)?;
        Ok(ranked
            .into_iter()
            .map(|(vid, val)| (vid_to_i64(vid), val.as_f64()))
            .collect())
    }
}

impl GcSession {
    /// Cooperative deadline check (loader gap b): returns a `Timeout` (`0x867`)
    /// if the invocation's wall-clock budget is exhausted.
    fn check_deadline(&self) -> PyResult<()> {
        if self.deadline.is_some_and(|d| Instant::now() >= d) {
            return Err(PyRuntimeError::new_err(
                "GraphCompute (0x867): invocation deadline exceeded",
            ));
        }
        Ok(())
    }
}
