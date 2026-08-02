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
use pyo3::exceptions::{PyAttributeError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use uni_common::core::id::Vid;
use uni_plugin::errors::FnError;
use uni_plugin_builtin::algorithms::graph_compute::handle::Handle;
use uni_plugin_builtin::algorithms::graph_compute::session::{
    AlgoSession, CmpOp, Direction, EndpointOp, EwiseOp, GraphArenaCompute, GraphCompute, MapOp,
    Norm, OverlapMetric, PairSpec, Predicate, ReduceOp, Semiring,
};
use uni_plugin_builtin::algorithms::graph_compute::value::{DType, Scalar};

/// A Python-visible handle to a per-CALL GraphCompute session.
#[pyclass]
pub struct GcSession {
    session: Arc<Mutex<AlgoSession>>,
    graph: i64,
    deadline: Option<Instant>,
    /// Pre-declared named scopes, built by the host before the guest ran.
    scopes: Vec<(String, i64)>,
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
    scopes: Vec<(String, i64)>,
) -> GcSession {
    GcSession {
        session,
        graph: to_i64(graph),
        deadline,
        scopes,
    }
}

pub(crate) fn to_i64(h: Handle) -> i64 {
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
/// Packs an external vertex id into the `i64` a guest holds.
fn vid_to_i64(vid: Vid) -> i64 {
    #[expect(clippy::cast_possible_wrap, reason = "vids fit i64 in practice")]
    let v = vid.as_u64() as i64;
    v
}

#[pymethods]
impl GcSession {
    /// Explains an attribute that does not exist, when we can say something useful.
    ///
    /// Python's default `AttributeError` says the name is unknown; it cannot say
    /// the operation is *expressible by another spelling*. Three consecutive
    /// field reports asked for kernels that were already composable, so this
    /// attaches the published recipe — and, for `graph-arena@1` names, the typed
    /// slice error the Rhai loader has raised since issue #152. PyO3 had no
    /// equivalent of Rhai's arena stubs at all, so this closes that asymmetry too.
    ///
    /// Deliberately an `AttributeError`, not the `PyRuntimeError` the kernels
    /// use: `hasattr()` only swallows `AttributeError`, so anything else would
    /// make a routine feature probe explode. Names starting with `_` get the
    /// plain message untouched — CPython and IPython probe `__iter__`,
    /// `__deepcopy__`, `_ipython_canary_method_*` and friends constantly, and
    /// answering those with kernel advice would be noise at best.
    fn __getattr__(&self, name: &str) -> PyResult<()> {
        let base = format!("'GcSession' object has no attribute '{name}'");
        if name.starts_with('_') {
            return Err(PyAttributeError::new_err(base));
        }
        let hint = uni_plugin_builtin::algorithms::graph_compute::unknown_method_message(name)
            .or_else(|| {
                uni_plugin_builtin::algorithms::graph_compute::GRAPH_ARENA_OPS
                    .contains(&name)
                    .then(|| {
                        uni_plugin_builtin::algorithms::graph_compute::unresolved_op_error(name)
                            .message
                    })
            });
        Err(PyAttributeError::new_err(match hint {
            Some(msg) => format!("{base}. {msg}"),
            None => base,
        }))
    }

    /// The bound graph handle.
    fn graph(&self) -> PyResult<i64> {
        self.check_deadline()?;
        Ok(self.graph)
    }

    /// The handle of a pre-declared named scope.
    ///
    /// Scopes are built by the host before the guest runs, so this is a lookup
    /// rather than a projection: a guest cannot reach storage in a loop.
    fn graph_named(&self, name: &str) -> PyResult<i64> {
        self.check_deadline()?;
        self.scopes
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, h)| *h)
            .ok_or_else(|| {
                let declared: Vec<&str> = self.scopes.iter().map(|(n, _)| n.as_str()).collect();
                py_err(FnError::new(
                    0x86E,
                    if declared.is_empty() {
                        format!(
                            "no graph scope `{name}`: this CALL declared no `scopes` map, so \
                             only the primary projection (`graph()`) exists"
                        )
                    } else {
                        format!(
                            "no graph scope `{name}`: declared scopes are {}",
                            declared.join(", ")
                        )
                    },
                ))
            })
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

    /// Edge count of a graph handle.
    fn edge_count(&self, g: i64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .edge_count(from_i64(g))
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

    /// BFS-to-fixpoint: the set of vertices reachable from `seeds` along `direction`.
    fn reach_fixpoint(&self, g: i64, seeds: Vec<i64>, direction: &str) -> PyResult<i64> {
        self.check_deadline()?;
        let d = Direction::parse(direction).map_err(py_err)?;
        #[expect(clippy::cast_sign_loss, reason = "vertex ids are non-negative")]
        let vids: Vec<Vid> = seeds.into_iter().map(|i| Vid::new(i as u64)).collect();
        self.session
            .lock()
            .reach_fixpoint(from_i64(g), &vids, d)
            .map(to_i64)
            .map_err(py_err)
    }

    /// Per-vertex degree map in `direction` (`"out"`/`"in"`).
    fn degrees(&self, g: i64, direction: &str) -> PyResult<i64> {
        self.check_deadline()?;
        let d = Direction::parse(direction).map_err(py_err)?;
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
        let p = Predicate::parse(pred, threshold).map_err(py_err)?;
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
        let n = Norm::parse(norm).map_err(py_err)?;
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
        let o = EwiseOp::parse(op, coef).map_err(py_err)?;
        self.session
            .lock()
            .ewise(from_i64(a), from_i64(b), o)
            .map(to_i64)
            .map_err(py_err)
    }

    /// Sparse mat-vec under a named semiring and direction.
    fn spmv(&self, g: i64, vec: i64, sr: &str, direction: &str) -> PyResult<i64> {
        self.check_deadline()?;
        let semi = Semiring::parse(sr).map_err(py_err)?;
        let d = Direction::parse(direction).map_err(py_err)?;
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
        let d = Direction::parse(direction).map_err(py_err)?;
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

    /// Emits named per-vertex columns into the result sink.
    ///
    /// Two forms: `gc.emit("score", h)` for one column, and
    /// `gc.emit({"a": h1, "b": h2})` for several in one call. Python has no
    /// overloading, so the batch form is the same function with the handle
    /// omitted. Both are equivalent — the session accumulates across calls —
    /// but the batch form is the shape the host trait models and keeps a
    /// multi-column egress to a single boundary crossing.
    #[pyo3(signature = (name, h = None))]
    fn emit(&self, name: &Bound<'_, PyAny>, h: Option<i64>) -> PyResult<()> {
        self.check_deadline()?;
        if let Some(handle) = h {
            let col: String = name.extract()?;
            return self
                .session
                .lock()
                .emit(&[(col.as_str(), from_i64(handle))])
                .map_err(py_err);
        }
        // Batch form: a dict of column name -> handle. Iterating the dict (rather
        // than collecting into a HashMap) keeps Python's insertion order.
        let dict = name.cast::<PyDict>().map_err(|_| {
            PyRuntimeError::new_err(
                "emit: pass a column name and a handle, or a dict of name -> handle",
            )
        })?;
        let mut pairs: Vec<(String, i64)> = Vec::with_capacity(dict.len());
        for (k, v) in dict {
            pairs.push((k.extract()?, v.extract()?));
        }
        let cols: Vec<(&str, Handle)> = pairs
            .iter()
            .map(|(n, handle)| (n.as_str(), from_i64(*handle)))
            .collect();
        self.session.lock().emit(&cols).map_err(py_err)
    }

    /// The native-work budget this invocation started with.
    fn work_budget(&self) -> PyResult<f64> {
        self.session.lock().work_budget().map_err(py_err)
    }

    /// Native-work units charged so far.
    fn work_spent(&self) -> PyResult<f64> {
        self.session.lock().work_spent().map_err(py_err)
    }

    /// Native-work units still available. Reading it is free.
    fn work_remaining(&self) -> PyResult<f64> {
        self.session.lock().work_remaining().map_err(py_err)
    }

    /// Elementwise comparison, yielding a 1.0/0.0 mask.
    fn compare(&self, a: i64, b: i64, op: &str) -> PyResult<i64> {
        self.check_deadline()?;
        let o = CmpOp::parse(op).map_err(py_err)?;
        self.session
            .lock()
            .compare(from_i64(a), from_i64(b), o)
            .map(to_i64)
            .map_err(py_err)
    }

    /// Generic map transform (`recip`/`scale`/`log`/`affine`/`normalize_l1|l2`);
    /// `a`,`b` are the scalar operands (`scale a`, `affine a*x+b`).
    #[pyo3(signature = (m, op, a = 0.0, b = 0.0))]
    fn map_apply(&self, m: i64, op: &str, a: f64, b: f64) -> PyResult<i64> {
        self.check_deadline()?;
        let o = MapOp::parse(op, a, b).map_err(py_err)?;
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

    /// Samples node2vec/DeepWalk random walks; empty `seeds` walks every vertex.
    ///
    /// `p`/`q` are the return/in-out bias (`1.0` = unbiased); `seed` makes the
    /// sampling deterministic. Returns a walks handle for `emit_walks` /
    /// `walk_visit_counts`.
    #[pyo3(signature = (g, seeds, walk_length, walks_per_node = 1, p = 1.0, q = 1.0, seed = 0))]
    #[expect(
        clippy::too_many_arguments,
        reason = "each arg is a distinct Python keyword in the exposed node2vec signature"
    )]
    fn random_walks(
        &self,
        g: i64,
        seeds: Vec<i64>,
        walk_length: usize,
        walks_per_node: usize,
        p: f64,
        q: f64,
        seed: u64,
    ) -> PyResult<i64> {
        self.check_deadline()?;
        #[expect(clippy::cast_sign_loss, reason = "vertex ids are non-negative")]
        let vids: Vec<Vid> = seeds.into_iter().map(|i| Vid::new(i as u64)).collect();
        self.session
            .lock()
            .random_walks(from_i64(g), walk_length, walks_per_node, &vids, p, q, seed)
            .map(to_i64)
            .map_err(py_err)
    }

    /// Draws a `Bernoulli(prob[v])` mask over a `[V]` probability tensor.
    ///
    /// `seed`/`iter` select the reproducible counter-hash stream; advancing
    /// `iter` yields a fresh, decorrelated per-iteration mask (proposal §8).
    /// Returns a vertex-set (mask) handle.
    #[pyo3(signature = (prob, seed = 0, iter = 0))]
    fn sample(&self, prob: i64, seed: u64, iter: u64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .sample(from_i64(prob), seed, iter)
            .map(to_i64)
            .map_err(py_err)
    }

    /// Builds a `[E]` per-edge tensor of out-edge weights (proposal §5).
    fn edge_weights(&self, g: i64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .edge_weights(from_i64(g))
            .map(to_i64)
            .map_err(py_err)
    }

    /// Builds a `[E]` per-edge tensor of the named projected edge property (#151).
    fn edge_property(&self, g: i64, name: &str) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .edge_property(from_i64(g), name)
            .map(to_i64)
            .map_err(py_err)
    }

    /// Builds a `[V]` per-vertex tensor of the named projected vertex property (#151).
    fn node_property(&self, g: i64, name: &str) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .node_property(from_i64(g), name)
            .map(to_i64)
            .map_err(py_err)
    }

    /// The full edge mask — every edge of `g` active.
    fn edges_all(&self, g: i64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .edges_all(from_i64(g))
            .map(to_i64)
            .map_err(py_err)
    }

    /// Draws a `Bernoulli(prob[e])` edge mask from a `[E]` probability tensor.
    #[pyo3(signature = (prob, seed = 0, iter = 0))]
    fn sample_edges(&self, prob: i64, seed: u64, iter: u64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .sample_edges(from_i64(prob), seed, iter)
            .map(to_i64)
            .map_err(py_err)
    }

    /// Undirected edge sampler: both half-edges of a pair share one draw.
    fn sample_edges_undirected(&self, g: i64, prob: i64, seed: u64, iter: u64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .sample_edges_undirected(from_i64(g), from_i64(prob), seed, iter)
            .map(to_i64)
            .map_err(py_err)
    }

    /// Cardinality of an edge mask.
    fn edge_set_len(&self, m: i64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .edge_set_len(from_i64(m))
            .map(|v| i64::try_from(v).unwrap_or(i64::MAX))
            .map_err(py_err)
    }

    /// Edges whose `[E]` value lies in the window `[lo, hi]` (F-11 time windows).
    fn edge_mask_window(&self, edge_vals: i64, lo: f64, hi: f64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .edge_mask_window(from_i64(edge_vals), lo, hi)
            .map(to_i64)
            .map_err(py_err)
    }

    /// Deterministic segmented reduce: per-group totals broadcast to members.
    fn segmented_reduce(&self, values: i64, groups: i64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .segmented_reduce(from_i64(values), from_i64(groups))
            .map(to_i64)
            .map_err(py_err)
    }

    /// Intersection of two edge masks.
    fn edge_intersect(&self, a: i64, b: i64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .edge_intersect(from_i64(a), from_i64(b))
            .map(to_i64)
            .map_err(py_err)
    }

    /// Union of two edge masks.
    fn edge_union(&self, a: i64, b: i64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .edge_union(from_i64(a), from_i64(b))
            .map(to_i64)
            .map_err(py_err)
    }

    /// One-hop expansion over the masked out-edges (exclude `0` = none).
    #[pyo3(signature = (g, frontier, direction, edge_mask, exclude = 0))]
    fn expand_masked(
        &self,
        g: i64,
        frontier: i64,
        direction: &str,
        edge_mask: i64,
        exclude: i64,
    ) -> PyResult<i64> {
        self.check_deadline()?;
        let d = Direction::parse(direction).map_err(py_err)?;
        self.session
            .lock()
            .expand_masked(
                from_i64(g),
                from_i64(frontier),
                d,
                if exclude == 0 {
                    None
                } else {
                    Some(from_i64(exclude))
                },
                from_i64(edge_mask),
            )
            .map(to_i64)
            .map_err(py_err)
    }

    /// Fused frontier-scoped sampled expansion: draw + expand, out-edges only.
    #[expect(
        clippy::too_many_arguments,
        reason = "kernel arity mirrors the wire op"
    )]
    fn expand_sampled(
        &self,
        g: i64,
        frontier: i64,
        direction: &str,
        exclude: i64,
        prob: i64,
        seed: u64,
        iter: u64,
    ) -> PyResult<i64> {
        self.check_deadline()?;
        let d = Direction::parse(direction).map_err(py_err)?;
        self.session
            .lock()
            .expand_sampled(
                from_i64(g),
                from_i64(frontier),
                d,
                if exclude == 0 {
                    None
                } else {
                    Some(from_i64(exclude))
                },
                from_i64(prob),
                seed,
                iter,
            )
            .map(to_i64)
            .map_err(py_err)
    }

    /// `spmv` restricted to the masked out-edges (out-direction only).
    fn spmv_masked(&self, g: i64, vec: i64, semiring_name: &str, edge_mask: i64) -> PyResult<i64> {
        self.check_deadline()?;
        let sr = Semiring::parse(semiring_name).map_err(py_err)?;
        self.session
            .lock()
            .spmv_masked(from_i64(g), from_i64(vec), sr, from_i64(edge_mask))
            .map(to_i64)
            .map_err(py_err)
    }

    /// Folds a walks handle into a per-vertex visit-count map.
    /// Copies a projection's topology into an empty arena.
    fn arena_seed(&self, arena: i64, g: i64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .arena_seed(from_i64(arena), from_i64(g))
            .map(to_i64)
            .map_err(py_err)
    }

    /// Gathers a `[V]` node value onto edges, yielding `[E]`.
    fn edge_from_nodes(&self, g: i64, x: i64, op: &str) -> PyResult<i64> {
        self.check_deadline()?;
        let op = EndpointOp::parse(op).map_err(py_err)?;
        self.session
            .lock()
            .edge_from_nodes(from_i64(g), from_i64(x), op)
            .map(to_i64)
            .map_err(py_err)
    }

    /// Piecewise-linear table lookup over `(xs, ys)` breakpoints.
    fn interp(&self, x: i64, xs: Vec<f64>, ys: Vec<f64>) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .interp(from_i64(x), &xs, &ys)
            .map(to_i64)
            .map_err(py_err)
    }

    /// Re-keys a `[V]` value into another projection's index space, verified.
    fn rekey(&self, value: i64, g: i64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .rekey(from_i64(value), from_i64(g))
            .map(to_i64)
            .map_err(py_err)
    }

    fn walk_visit_counts(&self, walks: i64, g: i64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .walk_visit_counts(from_i64(walks), from_i64(g))
            .map(to_i64)
            .map_err(py_err)
    }

    /// Emits the walk *sequences* as `(walk_id, step, nodeId)` result rows.
    fn emit_walks(&self, walks: i64) -> PyResult<()> {
        self.check_deadline()?;
        self.session
            .lock()
            .emit_walks(from_i64(walks))
            .map_err(py_err)
    }

    /// Per-vertex neighbourhood-overlap similarity to `source`.
    ///
    /// `metric` is `"jaccard"`, `"overlap"`, `"cosine"`, or `"adamic_adar"`.
    fn neighborhood_overlap(&self, g: i64, source: i64, metric: &str) -> PyResult<i64> {
        self.check_deadline()?;
        let m = OverlapMetric::parse(metric).map_err(py_err)?;
        #[expect(clippy::cast_sign_loss, reason = "vertex ids are non-negative")]
        let src = Vid::new(source as u64);
        self.session
            .lock()
            .neighborhood_overlap(from_i64(g), src, m)
            .map(to_i64)
            .map_err(py_err)
    }

    /// The Δ-stepping frontier of vertices whose distance lies in the bucket band.
    fn next_bucket(&self, dist: i64, delta: f64, bucket: u32) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .next_bucket(from_i64(dist), delta, bucket)
            .map(to_i64)
            .map_err(py_err)
    }

    /// All-pairs neighbourhood overlap over adjacent vertex pairs.
    ///
    /// `metric` is `"count"` (triangle support), `"jaccard"`, `"overlap"`,
    /// `"cosine"`, or `"adamic_adar"`; `pair_mode` is `"adjacent"` or `"topk"`.
    #[pyo3(signature = (g, metric = "count", pair_mode = "adjacent", k = 0))]
    fn all_pairs_overlap(&self, g: i64, metric: &str, pair_mode: &str, k: u32) -> PyResult<i64> {
        self.check_deadline()?;
        let m = OverlapMetric::parse(metric).map_err(py_err)?;
        let spec = if pair_mode == "topk" {
            PairSpec::TopKCandidates(k)
        } else {
            PairSpec::AdjacentPairs
        };
        self.session
            .lock()
            .all_pairs_overlap(from_i64(g), spec, m)
            .map(to_i64)
            .map_err(py_err)
    }

    /// Emits a pair list as `(srcId, dstId, value)` result rows.
    fn emit_pairs(&self, pairs: i64) -> PyResult<()> {
        self.check_deadline()?;
        self.session
            .lock()
            .emit_pairs(from_i64(pairs))
            .map_err(py_err)
    }

    // --- `graph-arena@1`: mutable session-local structure (proposal §5.1) ---

    /// Creates an arena of `capacity` slots with `branching` child slack.
    fn arena_new(&self, capacity: i64, branching: i64) -> PyResult<i64> {
        self.check_deadline()?;
        let (c, b) = (
            u32_arg(capacity, "capacity")?,
            u32_arg(branching, "branching")?,
        );
        self.session
            .lock()
            .arena_new(c, b)
            .map(to_i64)
            .map_err(py_err)
    }

    /// Bump-allocates `count` slots, returning an `i64` tensor of their ids.
    fn arena_alloc(&self, arena: i64, count: i64) -> PyResult<i64> {
        self.check_deadline()?;
        let n = u32_arg(count, "count")?;
        self.session
            .lock()
            .arena_alloc(from_i64(arena), n)
            .map(to_i64)
            .map_err(py_err)
    }

    /// Links each `kids[i]` as a child of `parents[i]`.
    fn arena_link(&self, arena: i64, parents: i64, kids: i64) -> PyResult<()> {
        self.check_deadline()?;
        self.session
            .lock()
            .arena_link(from_i64(arena), from_i64(parents), from_i64(kids))
            .map_err(py_err)
    }

    /// Adds a zero-filled per-slot state column, returning its index.
    fn arena_column(&self, arena: i64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .arena_column(from_i64(arena))
            .map_err(py_err)
    }

    /// The children of every slot in `roots`, concatenated.
    fn arena_candidates(&self, arena: i64, roots: i64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .arena_candidates(from_i64(arena), from_i64(roots))
            .map(to_i64)
            .map_err(py_err)
    }

    /// Gathers `column` at `slots` into a compact tensor.
    fn arena_gather(&self, arena: i64, column: i64, slots: i64) -> PyResult<i64> {
        self.check_deadline()?;
        let c = u32_arg(column, "column")?;
        self.session
            .lock()
            .arena_gather(from_i64(arena), c, from_i64(slots))
            .map(to_i64)
            .map_err(py_err)
    }

    /// Scatters `values` into `column` at `slots`.
    fn arena_scatter(&self, arena: i64, column: i64, slots: i64, values: i64) -> PyResult<()> {
        self.check_deadline()?;
        let c = u32_arg(column, "column")?;
        self.session
            .lock()
            .arena_scatter(from_i64(arena), c, from_i64(slots), from_i64(values))
            .map_err(py_err)
    }

    /// Adds `deltas[i]` to `value_col` along the root path of every `leaves[i]`.
    fn arena_backup(&self, arena: i64, value_col: i64, leaves: i64, deltas: i64) -> PyResult<()> {
        self.check_deadline()?;
        let c = u32_arg(value_col, "value_col")?;
        self.session
            .lock()
            .arena_backup(from_i64(arena), c, from_i64(leaves), from_i64(deltas))
            .map_err(py_err)
    }

    /// Descends from each root to a leaf by the guest's `score` column.
    fn arena_descend(
        &self,
        arena: i64,
        roots: i64,
        score: i64,
        visit: i64,
        maximize: bool,
        vloss: f64,
    ) -> PyResult<i64> {
        self.check_deadline()?;
        let (sc, vi) = (u32_arg(score, "score")?, u32_arg(visit, "visit")?);
        self.session
            .lock()
            .arena_descend(from_i64(arena), from_i64(roots), sc, vi, maximize, vloss)
            .map(to_i64)
            .map_err(py_err)
    }

    /// Allocates `fanout` children for every slot in `parents` and links them.
    fn arena_expand(&self, arena: i64, parents: i64, fanout: i64) -> PyResult<i64> {
        self.check_deadline()?;
        let f = u32_arg(fanout, "fanout")?;
        self.session
            .lock()
            .arena_expand(from_i64(arena), from_i64(parents), f)
            .map(to_i64)
            .map_err(py_err)
    }

    /// Compacts the arena into an immutable graph handle.
    fn arena_freeze(&self, arena: i64) -> PyResult<i64> {
        self.check_deadline()?;
        self.session
            .lock()
            .arena_freeze(from_i64(arena))
            .map(to_i64)
            .map_err(py_err)
    }
}

/// Narrows a guest `int` to the `u32` a kernel expects, or raises typed.
fn u32_arg(v: i64, what: &str) -> PyResult<u32> {
    u32::try_from(v).map_err(|_| {
        PyRuntimeError::new_err(format!(
            "GraphCompute (0x86E): {what} must be a non-negative 32-bit value, got {v}"
        ))
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use uni_plugin_builtin::algorithms::graph_compute::kernel_id::{
        KernelId, KernelReach, OPTIONAL_ARITIES,
    };
    use uni_plugin_builtin::algorithms::graph_compute::{Arena, WorkBudget};

    /// Every arity declared in `OPTIONAL_ARITIES` is callable **from Python**.
    ///
    /// The sibling of the Rhai test of the same shape. This surface already
    /// satisfied the contract — its `#[pyo3(signature = ..)]` defaults supply
    /// the short forms — so this is a drift guard, not a fix: dropping a default
    /// here would silently break the published `map_apply(a, "scale", -1.0)`
    /// spelling exactly the way Rhai's missing overloads did.
    ///
    /// It goes through the interpreter rather than calling the `#[pymethods]`
    /// function directly, because a direct Rust call cannot observe a signature
    /// default at all — which is why the neighbouring tests here never caught
    /// the loader divergence.
    #[test]
    fn every_declared_optional_arity_is_callable_from_python() {
        use std::collections::HashMap;
        use std::ffi::CString;
        use uni_algo::algo::GraphProjection;
        use uni_common::Value;
        use uni_plugin_builtin::algorithms::graph_compute::session::GraphCompute;
        use uni_plugin_builtin::algorithms::graph_compute::value::DType;

        // One probe per declared (kernel, arity). Locked both ways below.
        let probes: &[(KernelId, usize, &str)] = &[
            (KernelId::MapApply, 2, "gc.map_apply(m, 'log')"),
            (KernelId::MapApply, 3, "gc.map_apply(m, 'scale', 2.0)"),
            (KernelId::MapApply, 4, "gc.map_apply(m, 'affine', 1.0, 0.5)"),
            (KernelId::Ewise, 3, "gc.ewise(m, m, 'add')"),
            (KernelId::Ewise, 4, "gc.ewise(m, m, 'axpy', -1.0)"),
            (KernelId::ZeroMap, 1, "gc.zero_map(g)"),
            (KernelId::ZeroMap, 2, "gc.zero_map(g, 'i64')"),
            (KernelId::Emit, 1, "gc.emit({'score': m})"),
            (KernelId::Emit, 2, "gc.emit('score', m)"),
            (KernelId::Sample, 1, "gc.sample(m)"),
            (KernelId::Sample, 2, "gc.sample(m, 1)"),
            (KernelId::Sample, 3, "gc.sample(m, 1, 0)"),
            (KernelId::SampleEdges, 1, "gc.sample_edges(e)"),
            (KernelId::SampleEdges, 2, "gc.sample_edges(e, 1)"),
            (KernelId::SampleEdges, 3, "gc.sample_edges(e, 1, 0)"),
            (KernelId::AllPairsOverlap, 1, "gc.all_pairs_overlap(g)"),
            (
                KernelId::AllPairsOverlap,
                2,
                "gc.all_pairs_overlap(g, 'count')",
            ),
            (
                KernelId::AllPairsOverlap,
                3,
                "gc.all_pairs_overlap(g, 'count', 'adjacent')",
            ),
            (
                KernelId::AllPairsOverlap,
                4,
                "gc.all_pairs_overlap(g, 'count', 'adjacent', 0)",
            ),
            (KernelId::RandomWalks, 3, "gc.random_walks(g, [], 2)"),
            (KernelId::RandomWalks, 4, "gc.random_walks(g, [], 2, 1)"),
            (
                KernelId::RandomWalks,
                5,
                "gc.random_walks(g, [], 2, 1, 1.0)",
            ),
            (
                KernelId::RandomWalks,
                6,
                "gc.random_walks(g, [], 2, 1, 1.0, 1.0)",
            ),
            (
                KernelId::RandomWalks,
                7,
                "gc.random_walks(g, [], 2, 1, 1.0, 1.0, 0)",
            ),
        ];

        Python::initialize();
        let node_rows: Vec<HashMap<String, Value>> = (0..3u64)
            .map(|id| HashMap::from([("id".to_string(), Value::Int(id as i64))]))
            .collect();
        let graph =
            GraphProjection::from_rows(&node_rows, &[], None, false).expect("projection builds");
        let mut session =
            AlgoSession::new(23, WorkBudget::new(1_000_000), Arena::new(1 << 20, 256));
        let g = session.bind_graph(Arc::new(graph));
        let shared = Arc::new(Mutex::new(session));
        let m = to_i64(shared.lock().zero_map(g, DType::F64).expect("zero_map"));
        // An `[E]` tensor for `sample_edges`; empty here, which is enough to
        // resolve the call — this test is about arity, not about the data.
        let e = to_i64(shared.lock().edge_weights(g).expect("edge_weights"));
        let g_i = to_i64(g);

        Python::attach(|py| {
            let gc = Py::new(py, new_session(Arc::clone(&shared), g, None, Vec::new()))
                .expect("GcSession into Python");
            for (kernel, arity, expr) in probes {
                let code =
                    CString::new(format!("def probe(gc, m, e, g):\n    return {expr}\n")).unwrap();
                let module = pyo3::types::PyModule::from_code(
                    py,
                    code.as_c_str(),
                    CString::new("arity_probe.py").unwrap().as_c_str(),
                    CString::new("arity_probe").unwrap().as_c_str(),
                )
                .expect("probe module compiles");
                let f = module.getattr("probe").expect("probe defined");
                if let Err(err) = f.call1((gc.clone_ref(py), m, e, g_i)) {
                    assert!(
                        !err.is_instance_of::<pyo3::exceptions::PyTypeError>(py),
                        "`{}` is declared callable at arity {arity} but Python rejects the \
                         call: {expr}\n  {err}",
                        kernel.op_name()
                    );
                }
            }
        });

        // Negative control: the check above is only meaningful if a genuinely
        // short call *is* rejected. `compare` declares no defaults, so two
        // arguments must raise `TypeError` — if this ever stops holding, the
        // assertions above have quietly stopped testing anything.
        Python::attach(|py| {
            let gc = Py::new(py, new_session(Arc::clone(&shared), g, None, Vec::new()))
                .expect("GcSession into Python");
            let code = CString::new("def probe(gc, m):\n    return gc.compare(m, m)\n").unwrap();
            let module = pyo3::types::PyModule::from_code(
                py,
                code.as_c_str(),
                CString::new("arity_control.py").unwrap().as_c_str(),
                CString::new("arity_control").unwrap().as_c_str(),
            )
            .expect("control module compiles");
            let err = module
                .getattr("probe")
                .expect("probe defined")
                .call1((gc.clone_ref(py), m))
                .expect_err("`compare` takes three arguments, so two must be rejected");
            assert!(
                err.is_instance_of::<pyo3::exceptions::PyTypeError>(py),
                "a too-short call must surface as TypeError for this test to have teeth, got {err}"
            );
        });

        // Two-way lock against the shared declaration, identical to the Rhai side.
        for (kernel, arities) in OPTIONAL_ARITIES {
            for arity in *arities {
                assert!(
                    probes.iter().any(|(k, a, _)| k == kernel && a == arity),
                    "`{}` declares arity {arity} with no probe here",
                    kernel.op_name()
                );
            }
        }
        for (kernel, arity, _) in probes {
            let declared = OPTIONAL_ARITIES
                .iter()
                .find(|(k, _)| k == kernel)
                .is_some_and(|(_, arities)| arities.contains(arity));
            assert!(
                declared,
                "`{}` is probed at arity {arity} but the shared table does not declare it",
                kernel.op_name()
            );
        }
    }

    /// The reachability contract for the PyO3 surface (proposal §5.4 / §13.2).
    ///
    /// Every kernel the catalog declares `AllLoaders` must be an attribute of
    /// the `GcSession` type. This is asserted against the *live* type object
    /// rather than a maintained list, so it cannot be satisfied by updating a
    /// declaration — the `#[pymethods]` block itself has to have the method.
    ///
    /// This is the assertion that was missing when `edge_count` shipped
    /// dispatchable over JSON but invisible to Rhai and Python guests.
    /// An absent method earns its recipe, without breaking `hasattr` probing.
    #[test]
    fn an_unknown_attribute_earns_its_composition_recipe() {
        Python::initialize();
        Python::attach(|py| {
            let sess = Py::new(
                py,
                new_session(
                    Arc::new(Mutex::new(AlgoSession::new(
                        5,
                        WorkBudget::from_edge_count(1000),
                        Arena::new(1 << 20, 64),
                    ))),
                    from_i64(0),
                    None,
                    Vec::new(),
                ),
            )
            .expect("GcSession");
            let obj = sess.bind(py);

            for (name, needle) in [
                ("select", "ewise"),
                ("arena_spmv", "arena_freeze"),
                ("sub", "axpy"),
            ] {
                let err = obj.getattr(name).expect_err("absent attribute must raise");
                assert!(
                    err.is_instance_of::<PyAttributeError>(py),
                    "`{name}` must raise AttributeError so hasattr() still works"
                );
                let msg = err.to_string();
                assert!(
                    msg.contains(needle),
                    "`{name}` must carry the recipe (expected `{needle}`), got: {msg}"
                );
            }

            // A `graph-arena@1` name earns the typed slice message, matching the
            // Rhai loader's stubs — PyO3 previously had no equivalent at all.
            let err = obj
                .getattr("add_node")
                .expect_err("arena op is unavailable");
            assert!(
                err.to_string().contains("graph-arena"),
                "an arena op must name the slice it needs: {err}"
            );

            // Dunder and private probes must stay plain, or every `hasattr`
            // sweep by CPython/IPython would collect kernel advice.
            let err = obj.getattr("__deepcopy__").expect_err("dunder is absent");
            assert!(
                !err.to_string().contains("compose"),
                "a dunder probe must not be answered with kernel advice: {err}"
            );

            // And an ordinary typo earns no invented advice.
            let err = obj.getattr("wibble").expect_err("typo is absent");
            assert!(
                !err.to_string().contains("compose"),
                "an unkeyed name must not be given a made-up recipe: {err}"
            );
        });
    }

    #[test]
    fn every_in_process_kernel_is_reachable_from_python() {
        Python::initialize();
        Python::attach(|py| {
            let ty = py.get_type::<GcSession>();
            // `in_process`, not `all_loaders`: a Python guest holds the session
            // object and calls `graph()` / `graph_named(..)` on it, so the
            // host-supplied bucket must exist here too. Filtering on
            // `all_loaders` let `graph_named` ship unasserted.
            let missing: Vec<&str> = KernelId::in_process()
                .filter(|k| !ty.hasattr(k.op_name()).unwrap_or(false))
                .map(KernelId::op_name)
                .collect();
            assert!(
                missing.is_empty(),
                "kernels in the catalog but absent from the Python surface: {missing:?}"
            );

            // `LoaderLocal` is reserved and currently empty, so this loop is
            // vacuous today. It is kept as the guard that a future carve-out
            // must actually exist rather than merely being declared — writing
            // this check is what revealed that `check_deadline`, initially
            // catalogued as LoaderLocal, is a private Rust helper in a plain
            // `impl` block and never was guest-callable.
            for k in KernelId::ALL
                .iter()
                .filter(|k| k.reach() == KernelReach::LoaderLocal)
            {
                assert!(
                    ty.hasattr(k.op_name()).unwrap_or(false),
                    "`{}` is classified LoaderLocal but is absent from GcSession",
                    k.op_name()
                );
            }
        });
    }

    /// T5 — op strings must be parsed by `graph_compute::op_parse`, never here.
    ///
    /// Companion to the kernel-reachability test above: that one proves every
    /// catalogued kernel is exposed, this one proves no string vocabulary has
    /// been re-triplicated into this loader.
    #[test]
    fn op_strings_are_not_parsed_in_this_loader() {
        let body = include_str!("graph_compute.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("the file has a non-test prefix");
        for needle in [
            "=> Direction::",
            "=> EwiseOp::",
            "=> MapOp::",
            "=> Predicate::",
            "=> Norm::",
            "=> Semiring::",
            "=> OverlapMetric::",
        ] {
            assert!(
                !body.contains(needle),
                "`{needle}` appears in this loader — op strings belong in \
                 graph_compute::op_parse, so every surface rejects them identically"
            );
        }
    }

    /// The budget accessors *return values* from Python, not merely exist.
    ///
    /// `every_all_loaders_kernel_is_reachable_from_python` proves the attribute
    /// is present; a registered-but-broken kernel would satisfy it unchanged.
    #[test]
    fn budget_accessors_return_values_from_python() {
        use std::collections::HashMap;
        use uni_algo::algo::GraphProjection;
        use uni_common::Value;

        Python::initialize();
        let node_rows: Vec<HashMap<String, Value>> = (0..4u64)
            .map(|id| HashMap::from([("id".to_string(), Value::Int(id as i64))]))
            .collect();
        let graph =
            GraphProjection::from_rows(&node_rows, &[], None, false).expect("projection builds");

        let mut session = AlgoSession::new(
            21,
            uni_plugin_builtin::algorithms::graph_compute::WorkBudget::new(250),
            uni_plugin_builtin::algorithms::graph_compute::Arena::new(1 << 20, 256),
        );
        let g = session.bind_graph(Arc::new(graph));
        let gc = new_session(Arc::new(Mutex::new(session)), g, None, Vec::new());

        assert_eq!(gc.work_budget().expect("budget"), 250.0);
        assert_eq!(gc.work_spent().expect("spent"), 0.0);
        // Reading is free.
        let _ = gc.work_remaining().expect("remaining");
        let _ = gc.work_budget().expect("budget");
        assert_eq!(
            gc.work_spent().expect("spent"),
            0.0,
            "the accessors must not charge the meter they report"
        );

        gc.degrees(to_i64(g), "out").expect("degrees");
        let spent = gc.work_spent().expect("spent");
        assert_eq!(spent, 4.0, "degrees charges one unit per vertex");
        assert_eq!(gc.work_remaining().expect("remaining"), 250.0 - spent);
    }
}
