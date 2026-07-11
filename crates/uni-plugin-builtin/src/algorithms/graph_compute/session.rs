// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The per-invocation session and the coarse kernel catalog.
//!
//! An [`AlgoSession`] is created once per CALL. It owns the generational handle
//! table, the native-work budget, the arena cap, and the emit sink, and it holds
//! the projected graph(s) behind handles. It implements [`GraphCompute`], the
//! coarse kernel catalog a guest algorithm drives: every method takes and returns
//! handles plus scalars, so *no vertex data ever crosses the guest boundary* —
//! the guest is a conductor running an `O(iterations)` control loop while native
//! code does all `O(V + E)` work (proposal §4).
//!
//! Every kernel is metered against the [`WorkBudget`] in proportion to the native
//! work it does (proposal §5.1), allocates through the [`Arena`] cap
//! (proposal §5.1), and returns a typed [`FnError`] rather than panicking
//! (proposal §5.4). Pure-functional kernels return a *new* handle and never
//! mutate an input (proposal §4.1 / decision D6).
//!
//! # Scope
//! This ships kernel groups 0–6 and 8 (plumbing, sets, traversal, `spmv`, value
//! maps, reductions, iteration control, and `emit`) — enough to author the F-row
//! algorithms (PageRank/PPR, reachability, WCC, Bellman-Ford, HITS) — plus the
//! stochastic `random_walks` (F-8, with `walk_visit_counts`) and the starred
//! `neighborhood_overlap` (C-3) and `next_bucket` (C-1) kernels. The remaining
//! starred Brandes primitives (`bfs_levels`, `reverse_accumulate`) are deferred;
//! exact betweenness is available today via the native `uni.algo.betweenness`
//! provider (proposal §8).
//
// Rust guideline compliant

use std::sync::Arc;

use uni_algo::algo::GraphProjection;
use uni_algo::algo::algorithms::{Algorithm, RandomWalk, RandomWalkConfig};
use uni_common::core::id::Vid;
use uni_plugin::errors::FnError;

use super::error;
use super::handle::{Handle, HandleKind};
use super::table::HandleTable;
use super::value::{DType, Scalar, Tensor, VertexSet, WalkMatrix};
use super::{Arena, BUDGET_CHECK_CHUNK, WorkBudget};

/// Which adjacency direction a traversal or `spmv` follows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    /// Follow out-edges (`u -> v`).
    Out,
    /// Follow in-edges (`v -> u`).
    In,
}

/// A closed, host-evaluated per-vertex value transform (proposal §4.3).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MapOp {
    /// Normalize the map to unit L1 or L2 norm.
    Normalize(Norm),
    /// Multiply every element by a constant.
    Scale(f64),
    /// Affine transform `a * x + b`.
    AxPlusB(f64, f64),
    /// Reciprocal with the convention `recip(0) = 0` (dangling rows drop out).
    Recip,
    /// Natural logarithm.
    Log,
}

/// A vector norm used by [`MapOp::Normalize`] and [`ReduceOp`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Norm {
    /// L1 (sum of absolute values).
    L1,
    /// L2 (Euclidean).
    L2,
}

/// A closed, element-wise combiner over two maps (proposal §4.3).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum EwiseOp {
    /// Element-wise sum.
    Add,
    /// Element-wise product.
    Mul,
    /// Element-wise minimum.
    Min,
    /// Element-wise maximum.
    Max,
    /// `a + coef * b` (the PageRank teleport blend).
    Axpy(f64),
}

/// A closed reduction over a map, optionally masked (proposal §4.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReduceOp {
    /// Sum of elements.
    Sum,
    /// Minimum element.
    Min,
    /// Maximum element.
    Max,
    /// Count of elements (respecting the mask).
    Count,
    /// L1 norm (sum of absolute values).
    NormL1,
    /// L2 norm (Euclidean).
    NormL2,
}

/// A closed predicate lifting a map into a set (proposal §4.3).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Predicate {
    /// Elements equal to zero.
    IsZero,
    /// Elements strictly greater than a threshold.
    Gt(f64),
    /// Elements strictly less than a threshold.
    Lt(f64),
    /// Elements equal to a value.
    Eq(f64),
}

/// A named semiring for [`GraphCompute::spmv`] (proposal §4.3, decision D4).
///
/// The set is closed and curated: an open, guest-supplied semiring would
/// reintroduce the per-element boundary crossing this design eliminates and
/// defeat determinism (proposal §4.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Semiring {
    /// `(lor, land)` — boolean BFS / reachability.
    Reachability,
    /// `(min, plus)` — tropical / shortest path.
    ShortestPath,
    /// `(min, first)` — label / parent propagation.
    Propagate,
    /// `(plus, times)` — PageRank / HITS / eigenvector (and i64 path counting).
    LinearAlgebra,
    /// `(max, min)` — bottleneck / widest path.
    MinMax,
}

/// A neighbourhood-overlap similarity metric (proposal §8, C-3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OverlapMetric {
    /// `|N(u) ∩ N(v)| / |N(u) ∪ N(v)|`.
    Jaccard,
    /// `|N(u) ∩ N(v)| / min(|N(u)|, |N(v)|)`.
    Overlap,
    /// `|N(u) ∩ N(v)| / sqrt(|N(u)| · |N(v)|)`.
    Cosine,
}

/// A per-invocation GraphCompute session: handle table, budget, arena, sink.
///
/// Created once per CALL and dropped when the invocation returns, which frees
/// every handle (proposal §4.1). See the [module docs](self) for the metering
/// and purity contracts.
#[derive(Debug)]
pub struct AlgoSession {
    table: HandleTable,
    budget: WorkBudget,
    arena: Arena,
    /// The first graph bound, used by `emit` for slot→Vid `nodeId` translation.
    primary_graph: Option<Arc<GraphProjection>>,
    /// Captured `emit` output: `(column_name, values)` per emitted column.
    emitted: Vec<(String, Vec<f64>)>,
    /// Optional wall-clock deadline; every metered kernel checks it (§5.2).
    deadline: Option<std::time::Instant>,
    /// Optional declared guest-emitted column names (the host `nodeId` column
    /// excluded). When set, `emit` requires the emitted set to match it exactly
    /// — no missing, extra, or duplicate columns — failing `0x869` otherwise.
    expected_columns: Option<Vec<String>>,
}

impl AlgoSession {
    /// Creates a session with the given epoch, work budget, and arena caps.
    #[must_use]
    pub fn new(epoch: u16, budget: WorkBudget, arena: Arena) -> Self {
        Self {
            table: HandleTable::new(epoch),
            budget,
            arena,
            primary_graph: None,
            emitted: Vec::new(),
            deadline: None,
            expected_columns: None,
        }
    }

    /// Sets a wall-clock deadline after which any metered kernel fails with
    /// `Timeout` (`0x867`), distinguishing "too slow" from budget `Exhausted`
    /// and non-convergence `IterationLimit` (proposal §5.2).
    #[must_use]
    pub fn with_deadline(mut self, deadline: Option<std::time::Instant>) -> Self {
        self.deadline = deadline;
        self
    }

    /// Declares the columns the guest is required to `emit` (the host-generated
    /// `nodeId` column excluded), enabling exact-match schema validation.
    ///
    /// Set by the provider/loader adapters from the declared `output_fields`
    /// before the guest runs: `emit` then rejects a guest that omits a declared
    /// column, invents an undeclared one, or repeats one (`0x869`), instead of
    /// silently dropping extras and only catching omissions downstream (§4.6).
    #[must_use]
    pub fn with_expected_columns(mut self, columns: Vec<String>) -> Self {
        self.expected_columns = Some(columns);
        self
    }

    /// Binds a pre-built projection into the table, returning its graph handle.
    ///
    /// This is the native/first-party entry point; the capability-gated
    /// `project(spec)` path (which additionally requires `HostQuery`) is wired at
    /// the loader bridge in a later phase (proposal §4.3, §4.6).
    pub fn bind_graph(&mut self, graph: Arc<GraphProjection>) -> Handle {
        if self.primary_graph.is_none() {
            self.primary_graph = Some(Arc::clone(&graph));
        }
        self.table.insert_graph(graph)
    }

    /// Consumes the session's captured `emit` output.
    #[must_use]
    pub fn take_emitted(&mut self) -> Vec<(String, Vec<f64>)> {
        std::mem::take(&mut self.emitted)
    }

    /// Returns the work units charged so far (for accounting tests).
    #[must_use]
    pub fn work_spent(&self) -> u64 {
        self.budget.spent()
    }

    /// Returns the session's total native-work budget (for incomplete diagnostics).
    #[must_use]
    pub fn work_budget(&self) -> u64 {
        self.budget.total()
    }

    /// Returns the count of live handles (for reclaim tests).
    #[must_use]
    pub fn live_handles(&self) -> usize {
        self.table.live_handles()
    }

    /// Reads a tensor handle's values back out, for differential tests only.
    ///
    /// # Panics
    /// Panics if the handle does not resolve to a tensor.
    #[cfg(test)]
    pub(crate) fn tensor_values_for_test(&self, h: Handle) -> Vec<f64> {
        self.table
            .get_tensor(h)
            .expect("test tensor handle must resolve")
            .values()
            .to_vec()
    }

    /// Translates a slot to its external Vid via the primary graph's `IdMap`.
    fn slot_to_vid(&self, slot: u32) -> Vid {
        self.primary_graph
            .as_ref()
            .expect("emit/arg kernels require a bound graph")
            .to_vid(slot)
    }

    /// Charges `units` of native work, mapping exhaustion to error `0x865`.
    ///
    /// Also enforces the wall-clock deadline (§5.2): since every metered kernel
    /// funnels through here, a per-kernel deadline check bounds a slow guest even
    /// when it stays within its native-work budget, surfacing `Timeout` (0x867).
    fn charge(&mut self, units: u64) -> Result<(), FnError> {
        if self
            .deadline
            .is_some_and(|d| std::time::Instant::now() >= d)
        {
            return Err(error::timeout());
        }
        self.budget
            .try_charge(units)
            .map_err(|e| error::budget_exhausted(e.to_string()))
    }

    /// Reserves `bytes` in the arena, mapping a breach to error `0x864`.
    fn reserve(&mut self, bytes: usize) -> Result<(), FnError> {
        self.arena
            .try_alloc(bytes)
            .map_err(|e| error::arena_cap_exceeded(e.to_string()))
    }

    /// Charges the arena and inserts a tensor, returning its handle.
    fn alloc_tensor(&mut self, tensor: Tensor) -> Result<Handle, FnError> {
        self.reserve(tensor.heap_bytes())?;
        Ok(self.table.insert_tensor(tensor))
    }

    /// Charges the arena and inserts a vertex set, returning its handle.
    fn alloc_set(&mut self, set: VertexSet) -> Result<Handle, FnError> {
        self.reserve(set.heap_bytes())?;
        Ok(self.table.insert_set(set))
    }

    /// Charges the arena and inserts a walk batch, returning its handle.
    fn alloc_walks(&mut self, walks: WalkMatrix) -> Result<Handle, FnError> {
        self.reserve(walks.heap_bytes())?;
        Ok(self.table.insert_walks(walks))
    }
}

/// The coarse graph kernel catalog driven by a guest algorithm.
///
/// See the [module docs](self) for the metering, purity, and error contracts
/// every implementation must honor. This is the `graph-compute@1` slice, kernel
/// groups 0–6 and 8 (proposal §4.3).
pub trait GraphCompute {
    /// Returns the vertex count of a graph handle.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] if the handle does not resolve to a graph.
    fn vertex_count(&self, g: Handle) -> Result<u64, FnError>;

    /// Returns the edge count of a graph handle.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] if the handle does not resolve to a graph.
    fn edge_count(&self, g: Handle) -> Result<u64, FnError>;

    /// Group 0: builds a `[V]` map of each vertex's degree in `dir`.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] on a bad handle or an exhausted budget/arena.
    fn degrees(&mut self, g: Handle, dir: Direction) -> Result<Handle, FnError>;

    /// Group 0: builds a `[V]` map where each vertex holds its own slot id.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] on a bad handle or an exhausted budget/arena.
    fn vertex_ids(&mut self, g: Handle) -> Result<Handle, FnError>;

    /// Group 0: element-wise combine of two maps under `op`.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] on a bad handle, a length mismatch, or an
    /// exhausted budget/arena.
    fn ewise(&mut self, a: Handle, b: Handle, op: EwiseOp) -> Result<Handle, FnError>;

    /// Group 0: lifts a set into a map, assigning `value` to set members.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] on a bad handle or an exhausted budget/arena.
    fn set_to_map(&mut self, s: Handle, value: Scalar) -> Result<Handle, FnError>;

    /// Group 0: lowers a map into the set of vertices satisfying `pred`.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] on a bad handle or an exhausted budget/arena.
    fn map_to_set(&mut self, m: Handle, pred: Predicate) -> Result<Handle, FnError>;

    /// Group 0: frees a handle and reclaims its arena bytes.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] on a stale, forged, or cross-session handle.
    fn free(&mut self, h: Handle) -> Result<(), FnError>;

    /// Group 1: builds a frontier set from external `seeds` (Vids).
    ///
    /// # Errors
    /// Returns `0x868` if a seed is not present in the projection (fail closed,
    /// not skip), or a typed [`FnError`] on a bad handle or exhausted resources.
    fn frontier(&mut self, g: Handle, seeds: &[Vid]) -> Result<Handle, FnError>;

    /// Group 1: set union.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] on a bad handle or an exhausted budget/arena.
    fn set_union(&mut self, a: Handle, b: Handle) -> Result<Handle, FnError>;

    /// Group 1: set difference `a \ b`.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] on a bad handle or an exhausted budget/arena.
    fn set_diff(&mut self, a: Handle, b: Handle) -> Result<Handle, FnError>;

    /// Group 1: set intersection.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] on a bad handle or an exhausted budget/arena.
    fn set_intersect(&mut self, a: Handle, b: Handle) -> Result<Handle, FnError>;

    /// Group 1: cardinality of a set.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] if the handle does not resolve to a set.
    fn set_len(&self, s: Handle) -> Result<u64, FnError>;

    /// Group 1: whether a set is empty.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] if the handle does not resolve to a set.
    fn is_empty(&self, s: Handle) -> Result<bool, FnError>;

    /// Group 2: expands a frontier one hop, optionally excluding a visited mask.
    ///
    /// Direction-agnostic to the guest and charged Σ frontier degree, checked in
    /// chunks so a celebrity super-node cannot blow past the budget (§5.1).
    ///
    /// # Errors
    /// Returns a typed [`FnError`] on a bad handle or an exhausted budget/arena.
    fn expand(
        &mut self,
        g: Handle,
        frontier: Handle,
        dir: Direction,
        exclude: Option<Handle>,
    ) -> Result<Handle, FnError>;

    /// Group 3: sparse mat-vec of a map under a named semiring.
    ///
    /// Charges nnz (the edge count). `mask`, when present, restricts the output
    /// to masked positions (fused, not a separate materialize-then-filter).
    ///
    /// # Errors
    /// Returns a typed [`FnError`] on a bad handle, a non-tensor `vec`, or an
    /// exhausted budget/arena.
    fn spmv(
        &mut self,
        g: Handle,
        vec: Handle,
        sr: Semiring,
        dir: Direction,
        mask: Option<Handle>,
    ) -> Result<Handle, FnError>;

    /// Group 4: builds a `[V]` map of zeros.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] on a bad handle or an exhausted budget/arena.
    fn zero_map(&mut self, g: Handle, ty: DType) -> Result<Handle, FnError>;

    /// Group 4: returns a *new* map with `value` scattered onto `frontier`.
    ///
    /// Pure: the input map is unchanged (proposal §4.1).
    ///
    /// # Errors
    /// Returns a typed [`FnError`] on a bad handle or an exhausted budget/arena.
    fn scatter(&mut self, map: Handle, frontier: Handle, value: Scalar) -> Result<Handle, FnError>;

    /// Group 4: applies a closed per-vertex transform, returning a new map.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] on a bad handle or an exhausted budget/arena.
    fn map_apply(&mut self, map: Handle, op: MapOp) -> Result<Handle, FnError>;

    /// Group 5: reduces a map to a scalar, optionally over a mask.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] on a bad handle.
    fn reduce(
        &mut self,
        map: Handle,
        op: ReduceOp,
        mask: Option<Handle>,
    ) -> Result<Scalar, FnError>;

    /// Group 5: returns the `(Vid, value)` of the extreme element (lowest-slot tie).
    ///
    /// # Errors
    /// Returns a typed [`FnError`] on a bad handle or an empty map.
    fn arg_extreme(&mut self, map: Handle, want_max: bool) -> Result<(Vid, Scalar), FnError>;

    /// Group 5: returns the top-`k` `(Vid, value)` pairs, highest first.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] on a bad handle.
    fn topk(&mut self, map: Handle, k: u32) -> Result<Vec<(Vid, Scalar)>, FnError>;

    /// Group 6: the L1 distance between two maps (a convergence test).
    ///
    /// # Errors
    /// Returns a typed [`FnError`] on a bad handle or a length mismatch.
    fn l1_diff(&mut self, a: Handle, b: Handle) -> Result<f64, FnError>;

    /// Group 8: emits per-vertex columns into the session result sink.
    ///
    /// Host-terminal: the guest never receives a `RecordBatch` back. The host
    /// later prepends a `nodeId` column via slot→Vid translation (§4.3).
    ///
    /// # Errors
    /// Returns `0x869` if a column is not a `[V]` map, or a typed [`FnError`] on
    /// a bad handle.
    fn emit(&mut self, cols: &[(&str, Handle)]) -> Result<(), FnError>;

    /// Group 7 (F-8): samples `walks_per_node` random walks of length
    /// `walk_length` from each seed (all vertices when `seeds` is empty).
    ///
    /// `p`/`q` are the node2vec return/in-out bias (both `1.0` = uniform); `seed`
    /// makes the sampling deterministic. Returns a [`HandleKind::Walks`] handle.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] on a bad handle, an unmapped seed, or exhausted
    /// resources.
    #[allow(clippy::too_many_arguments, reason = "mirrors RandomWalkConfig fields")]
    fn random_walks(
        &mut self,
        g: Handle,
        walk_length: usize,
        walks_per_node: usize,
        seeds: &[Vid],
        p: f64,
        q: f64,
        seed: u64,
    ) -> Result<Handle, FnError>;

    /// Group 7 (F-8): folds a walks handle into a per-vertex visit-count map.
    ///
    /// `counts[v]` is the number of times slot `v` appears across all walks — the
    /// co-occurrence basis for DeepWalk/node2vec and an emittable `[V]` map.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] on a bad handle or exhausted resources.
    fn walk_visit_counts(&mut self, walks: Handle, g: Handle) -> Result<Handle, FnError>;

    /// Starred (C-3): the per-vertex neighbourhood-overlap similarity to `source`.
    ///
    /// `overlap[v]` is the chosen [`OverlapMetric`] between the undirected
    /// neighbourhoods of `source` and `v` (0 for `v = source`). One bulk kernel
    /// over sorted adjacency; returns an emittable `[V]` map.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] on a bad handle, an unmapped source, or
    /// exhausted resources.
    fn neighborhood_overlap(
        &mut self,
        g: Handle,
        source: Vid,
        metric: OverlapMetric,
    ) -> Result<Handle, FnError>;

    /// Starred (C-1): the Δ-stepping frontier — vertices whose distance lies in
    /// `[bucket · delta, (bucket + 1) · delta)`.
    ///
    /// A bucket primitive over a distance `[V]` map (e.g. from Bellman-Ford):
    /// returns the vertex set for the next bucket to relax. Infinite distances
    /// never fall in a finite bucket.
    ///
    /// # Errors
    /// Returns a typed [`FnError`] on a bad handle or exhausted resources.
    fn next_bucket(&mut self, dist: Handle, delta: f64, bucket: u32) -> Result<Handle, FnError>;
}

impl GraphCompute for AlgoSession {
    fn vertex_count(&self, g: Handle) -> Result<u64, FnError> {
        Ok(self.table.get_graph(g)?.vertex_count() as u64)
    }

    fn edge_count(&self, g: Handle) -> Result<u64, FnError> {
        Ok(self.table.get_graph(g)?.edge_count() as u64)
    }

    fn degrees(&mut self, g: Handle, dir: Direction) -> Result<Handle, FnError> {
        let graph = Arc::clone(self.table.get_graph(g)?);
        let n = graph.vertex_count();
        self.charge(n as u64)?;
        let values: Vec<f64> = (0..n as u32)
            .map(|s| match dir {
                Direction::Out => f64::from(graph.out_degree(s)),
                Direction::In => f64::from(graph.in_degree(s)),
            })
            .collect();
        self.alloc_tensor(Tensor::from_f64(values))
    }

    fn vertex_ids(&mut self, g: Handle) -> Result<Handle, FnError> {
        let graph = Arc::clone(self.table.get_graph(g)?);
        let n = graph.vertex_count();
        self.charge(n as u64)?;
        // Each vertex holds its own slot id (WCC min-label init). Slot ids fit
        // exactly in f64 below 2^53 (see value.rs dtype note).
        let values: Vec<f64> = (0..n).map(|s| s as f64).collect();
        self.alloc_tensor(Tensor::from_f64_typed(values, DType::U32))
    }

    fn ewise(&mut self, a: Handle, b: Handle, op: EwiseOp) -> Result<Handle, FnError> {
        let ta = self.table.get_tensor(a)?;
        let tb = self.table.get_tensor(b)?;
        if ta.len() != tb.len() {
            return Err(error::shape_mismatch(
                "ewise requires two maps of equal length",
            ));
        }
        if ta.is_i64() || tb.is_i64() {
            return Err(error::shape_mismatch("ewise requires f64 maps"));
        }
        let n = ta.len();
        let (xa, xb) = (ta.values(), tb.values());
        let out: Vec<f64> = (0..n)
            .map(|i| {
                let (x, y) = (xa[i], xb[i]);
                match op {
                    EwiseOp::Add => x + y,
                    EwiseOp::Mul => x * y,
                    EwiseOp::Min => x.min(y),
                    EwiseOp::Max => x.max(y),
                    EwiseOp::Axpy(coef) => x + coef * y,
                }
            })
            .collect();
        self.charge(n as u64)?;
        self.alloc_tensor(Tensor::from_f64(out))
    }

    fn set_to_map(&mut self, s: Handle, value: Scalar) -> Result<Handle, FnError> {
        let set = self.table.get_set(s)?;
        let n = set.capacity();
        let v = value.as_f64();
        let mut out = vec![0.0; n];
        for slot in set.iter() {
            out[slot as usize] = v;
        }
        self.charge(n as u64)?;
        self.alloc_tensor(Tensor::from_f64(out))
    }

    fn map_to_set(&mut self, m: Handle, pred: Predicate) -> Result<Handle, FnError> {
        let t = self.table.get_tensor(m)?;
        if t.is_i64() {
            return Err(error::shape_mismatch("map_to_set requires an f64 map"));
        }
        let n = t.len();
        let mut set = VertexSet::with_capacity(n);
        for (i, &x) in t.values().iter().enumerate() {
            let hit = match pred {
                Predicate::IsZero => x == 0.0,
                Predicate::Gt(k) => x > k,
                Predicate::Lt(k) => x < k,
                Predicate::Eq(k) => x == k,
            };
            if hit {
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "index bounded by tensor length which fits u32"
                )]
                set.insert(i as u32);
            }
        }
        self.charge(n as u64)?;
        self.alloc_set(set)
    }

    fn free(&mut self, h: Handle) -> Result<(), FnError> {
        // Graph handles are never counted against the arena (they share an `Arc`
        // and are not `try_alloc`-ed on bind), so freeing one must NOT decrement
        // the arena's live-handle counter — doing so would let a guest that
        // binds+frees graphs drive the count below zero and breach the cap.
        let is_graph = h.kind() == Some(HandleKind::Graph);
        let bytes = self.table.free(h)?;
        if !is_graph {
            self.arena.free(bytes);
        }
        Ok(())
    }

    fn frontier(&mut self, g: Handle, seeds: &[Vid]) -> Result<Handle, FnError> {
        let graph = Arc::clone(self.table.get_graph(g)?);
        let mut set = VertexSet::with_capacity(graph.vertex_count());
        for &vid in seeds {
            // Fail closed: a seed absent from the projection is an error, not a
            // silent skip (proposal §4.3).
            let slot = graph
                .to_slot(vid)
                .ok_or_else(|| error::seed_not_in_projection(vid.as_u64()))?;
            set.insert(slot);
        }
        self.charge(seeds.len() as u64)?;
        self.alloc_set(set)
    }

    fn set_union(&mut self, a: Handle, b: Handle) -> Result<Handle, FnError> {
        let out = self.table.get_set(a)?.union(self.table.get_set(b)?);
        self.charge(out.capacity() as u64 / 64 + 1)?;
        self.alloc_set(out)
    }

    fn set_diff(&mut self, a: Handle, b: Handle) -> Result<Handle, FnError> {
        let out = self.table.get_set(a)?.difference(self.table.get_set(b)?);
        self.charge(out.capacity() as u64 / 64 + 1)?;
        self.alloc_set(out)
    }

    fn set_intersect(&mut self, a: Handle, b: Handle) -> Result<Handle, FnError> {
        let out = self.table.get_set(a)?.intersect(self.table.get_set(b)?);
        self.charge(out.capacity() as u64 / 64 + 1)?;
        self.alloc_set(out)
    }

    fn set_len(&self, s: Handle) -> Result<u64, FnError> {
        Ok(self.table.get_set(s)?.len() as u64)
    }

    fn is_empty(&self, s: Handle) -> Result<bool, FnError> {
        Ok(self.table.get_set(s)?.is_empty())
    }

    fn expand(
        &mut self,
        g: Handle,
        frontier: Handle,
        dir: Direction,
        exclude: Option<Handle>,
    ) -> Result<Handle, FnError> {
        let graph = Arc::clone(self.table.get_graph(g)?);
        let front = self.table.get_set(frontier)?.clone();
        let excl = match exclude {
            Some(h) => Some(self.table.get_set(h)?.clone()),
            None => None,
        };
        let mut out = VertexSet::with_capacity(graph.vertex_count());
        // Charge Σ frontier degree, checked every BUDGET_CHECK_CHUNK edges so a
        // single super-node expansion cannot overshoot by more than one chunk.
        let mut since_check: u64 = 0;
        for u in front.iter() {
            let neighbors = match dir {
                Direction::Out => graph.out_neighbors(u),
                Direction::In => graph.in_neighbors(u),
            };
            for &v in neighbors {
                if excl.as_ref().is_none_or(|e| !e.contains(v)) {
                    out.insert(v);
                }
                since_check += 1;
                if since_check >= BUDGET_CHECK_CHUNK {
                    self.charge(since_check)?;
                    since_check = 0;
                }
            }
        }
        self.charge(since_check)?;
        self.alloc_set(out)
    }

    fn spmv(
        &mut self,
        g: Handle,
        vec: Handle,
        sr: Semiring,
        dir: Direction,
        mask: Option<Handle>,
    ) -> Result<Handle, FnError> {
        let graph = Arc::clone(self.table.get_graph(g)?);
        let n = graph.vertex_count();
        let input = self.table.get_tensor(vec)?;
        if input.len() != n {
            return Err(error::shape_mismatch(
                "spmv input must be a [V] map matching the graph",
            ));
        }
        let is_i64 = input.is_i64();
        // The exact-integer path counts walks; only the LinearAlgebra semiring
        // (plus-times over the counting monoid) has an integer meaning. The
        // tropical/boolean semirings are f64-only (proposal §4.2 / F-9).
        if is_i64 && !matches!(sr, Semiring::LinearAlgebra) {
            return Err(error::shape_mismatch(
                "i64 spmv supports only the LinearAlgebra semiring (path counting)",
            ));
        }
        // Capture the source values now, releasing the table's immutable borrow
        // before `charge` takes `&mut self`.
        let src_i64: Option<Vec<i64>> = input.values_i64().map(<[i64]>::to_vec);
        let src_f64: Option<Vec<f64>> = if is_i64 {
            None
        } else {
            Some(input.values().to_vec())
        };
        let mask_set = match mask {
            Some(h) => Some(self.table.get_set(h)?.clone()),
            None => None,
        };
        // Admission control: charge nnz (edge count) BEFORE doing the O(E)
        // scatter, so an exhausted budget stops the work rather than accounting
        // for it after the fact (proposal §5.1 — the meter must fail closed).
        self.charge(graph.edge_count() as u64)?;

        if let Some(src) = src_i64 {
            // Exact integer path-counting: out[v] += Σ_{u→v} src[u]. Unweighted
            // (a path count has no edge-weight meaning), accumulated in i64 so a
            // count beyond 2⁵³ stays exact where f64 would round (F-9).
            let mut out = vec![0i64; n];
            for u in 0..n as u32 {
                let contrib = src[u as usize];
                if contrib == 0 {
                    continue;
                }
                let neighbors = match dir {
                    Direction::Out => graph.out_neighbors(u),
                    Direction::In => graph.in_neighbors(u),
                };
                for &v in neighbors {
                    out[v as usize] = out[v as usize].saturating_add(contrib);
                }
            }
            if let Some(m) = &mask_set {
                for (v, slot) in out.iter_mut().enumerate() {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "vertex index bounded by n which fits u32"
                    )]
                    if !m.contains(v as u32) {
                        *slot = 0;
                    }
                }
            }
            self.charge(graph.edge_count() as u64)?;
            return self.alloc_tensor(Tensor::from_i64(out));
        }

        let src = src_f64.expect("non-i64 tensor captured an f64 source");
        let has_w = graph.has_weights();

        // Identity element of the additive monoid.
        let identity = match sr {
            Semiring::Reachability => 0.0,
            Semiring::ShortestPath | Semiring::Propagate => f64::INFINITY,
            Semiring::LinearAlgebra => 0.0,
            Semiring::MinMax => f64::NEG_INFINITY,
        };
        let mut out = vec![identity; n];

        // Scatter each source's contribution across its edges (dir = which
        // adjacency to walk). Accumulate at the target under the semiring's
        // additive monoid; the multiplicative op combines the source value with
        // the edge weight.
        for u in 0..n as u32 {
            let contrib = src[u as usize];
            if matches!(sr, Semiring::LinearAlgebra) && contrib == 0.0 {
                continue; // sparse fast path
            }
            let neighbors = match dir {
                Direction::Out => graph.out_neighbors(u),
                Direction::In => graph.in_neighbors(u),
            };
            for (idx, &v) in neighbors.iter().enumerate() {
                // Edge weights are only stored for the OUT adjacency; the reverse
                // CSR carries none. Fetching `out_weight(u, idx)` for an In-edge
                // index reads the wrong (or an out-of-bounds) slot, so weighted
                // In-direction is treated as unweighted (w = 1). Out-direction is
                // exact.
                let w = if has_w && matches!(dir, Direction::Out) {
                    graph.out_weight(u, idx)
                } else {
                    1.0
                };
                let acc = &mut out[v as usize];
                match sr {
                    Semiring::Reachability => {
                        if contrib != 0.0 {
                            *acc = 1.0;
                        }
                    }
                    Semiring::ShortestPath => *acc = acc.min(contrib + w),
                    Semiring::Propagate => *acc = acc.min(contrib),
                    Semiring::LinearAlgebra => *acc += contrib * w,
                    Semiring::MinMax => *acc = acc.max(contrib.min(w)),
                }
            }
        }

        // Fused mask: positions outside the mask are reset to identity, so
        // `spmv(mask=m)` equals `spmv` then filter-by-m without materializing the
        // intermediate (proposal metamorphic property M-4).
        if let Some(m) = mask_set {
            for (v, slot_out) in out.iter_mut().enumerate() {
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "vertex index bounded by n which fits u32"
                )]
                if !m.contains(v as u32) {
                    *slot_out = identity;
                }
            }
        }

        self.charge(graph.edge_count() as u64)?;
        self.alloc_tensor(Tensor::from_f64(out))
    }

    fn zero_map(&mut self, g: Handle, ty: DType) -> Result<Handle, FnError> {
        let n = self.table.get_graph(g)?.vertex_count();
        self.charge(n as u64)?;
        // An I64 zero map seeds an exact path-counting run (F-9); every other
        // dtype uses the f64 buffer (the v1 compute default).
        let tensor = if matches!(ty, DType::I64) {
            Tensor::from_i64(vec![0; n])
        } else {
            Tensor::from_f64(vec![0.0; n])
        };
        self.alloc_tensor(tensor)
    }

    fn scatter(&mut self, map: Handle, frontier: Handle, value: Scalar) -> Result<Handle, FnError> {
        let t = self.table.get_tensor(map)?;
        if let Some(ivals) = t.values_i64() {
            let mut out = ivals.to_vec();
            let v = value.as_i64();
            let set = self.table.get_set(frontier)?;
            for slot in set.iter() {
                out[slot as usize] = v;
            }
            self.charge(out.len() as u64)?;
            return self.alloc_tensor(Tensor::from_i64(out));
        }
        let mut out = t.values().to_vec();
        let set = self.table.get_set(frontier)?;
        let v = value.as_f64();
        for slot in set.iter() {
            out[slot as usize] = v;
        }
        self.charge(out.len() as u64)?;
        self.alloc_tensor(Tensor::from_f64(out))
    }

    fn map_apply(&mut self, map: Handle, op: MapOp) -> Result<Handle, FnError> {
        let t = self.table.get_tensor(map)?;
        if t.is_i64() {
            return Err(error::shape_mismatch("map_apply requires an f64 map"));
        }
        let x = t.values();
        let n = x.len();
        let out: Vec<f64> = match op {
            MapOp::Scale(a) => x.iter().map(|v| v * a).collect(),
            MapOp::AxPlusB(a, b) => x.iter().map(|v| a * v + b).collect(),
            // recip(0) = 0 so dangling (zero out-degree) rows drop out (§4.4).
            MapOp::Recip => x
                .iter()
                .map(|&v| if v == 0.0 { 0.0 } else { 1.0 / v })
                .collect(),
            MapOp::Log => x.iter().map(|v| v.ln()).collect(),
            MapOp::Normalize(norm) => {
                let denom = match norm {
                    Norm::L1 => x.iter().map(|v| v.abs()).sum::<f64>(),
                    Norm::L2 => x.iter().map(|v| v * v).sum::<f64>().sqrt(),
                };
                if denom == 0.0 {
                    x.to_vec()
                } else {
                    x.iter().map(|v| v / denom).collect()
                }
            }
        };
        self.charge(n as u64)?;
        self.alloc_tensor(Tensor::from_f64(out))
    }

    fn reduce(
        &mut self,
        map: Handle,
        op: ReduceOp,
        mask: Option<Handle>,
    ) -> Result<Scalar, FnError> {
        // Charge |V| before scanning (§5.1): read-only reductions run every
        // convergence iteration, so an unmetered reduce is an amplification hole.
        let n = self.table.get_tensor(map)?.len();
        self.charge(n as u64)?;
        let t = self.table.get_tensor(map)?;
        let mask_set = match mask {
            Some(h) => Some(self.table.get_set(h)?),
            None => None,
        };
        // Fixed ascending-slot order for deterministic float reduction (§5.3).
        let included = |i: usize| mask_set.is_none_or(|m| m.contains(i as u32));
        // Exact integer reductions read the i64 buffer directly, so a summed
        // path count stays exact (F-9). Only Sum/Count have an integer meaning;
        // the norm/min/max forms are f64-only.
        if t.is_i64() {
            let ivals = t.values_i64().expect("i64 tensor exposes an i64 slice");
            return match op {
                ReduceOp::Sum => Ok(Scalar::I64(
                    ivals
                        .iter()
                        .enumerate()
                        .filter(|&(i, _)| included(i))
                        .map(|(_, v)| *v)
                        .sum(),
                )),
                ReduceOp::Count => Ok(Scalar::I64(
                    ivals
                        .iter()
                        .enumerate()
                        .filter(|&(i, _)| included(i))
                        .count() as i64,
                )),
                _ => Err(error::shape_mismatch(
                    "i64 reduce supports only Sum and Count",
                )),
            };
        }
        let vals = t.values();
        let result = match op {
            ReduceOp::Sum => vals
                .iter()
                .enumerate()
                .filter(|&(i, _)| included(i))
                .map(|(_, v)| *v)
                .sum(),
            ReduceOp::Count => vals
                .iter()
                .enumerate()
                .filter(|&(i, _)| included(i))
                .count() as f64,
            ReduceOp::NormL1 => vals
                .iter()
                .enumerate()
                .filter(|&(i, _)| included(i))
                .map(|(_, v)| v.abs())
                .sum(),
            ReduceOp::NormL2 => vals
                .iter()
                .enumerate()
                .filter(|&(i, _)| included(i))
                .map(|(_, v)| v * v)
                .sum::<f64>()
                .sqrt(),
            ReduceOp::Min => vals
                .iter()
                .enumerate()
                .filter(|&(i, _)| included(i))
                .map(|(_, v)| *v)
                .fold(f64::INFINITY, f64::min),
            ReduceOp::Max => vals
                .iter()
                .enumerate()
                .filter(|&(i, _)| included(i))
                .map(|(_, v)| *v)
                .fold(f64::NEG_INFINITY, f64::max),
        };
        Ok(Scalar::F64(result))
    }

    fn arg_extreme(&mut self, map: Handle, want_max: bool) -> Result<(Vid, Scalar), FnError> {
        let n = self.table.get_tensor(map)?.len();
        self.charge(n as u64)?;
        let t = self.table.get_tensor(map)?;
        if let Some(ivals) = t.values_i64() {
            if ivals.is_empty() {
                return Err(error::shape_mismatch(
                    "arg_extreme requires a non-empty map",
                ));
            }
            let mut best_slot = 0usize;
            let mut best = ivals[0];
            for (i, &v) in ivals.iter().enumerate().skip(1) {
                if (want_max && v > best) || (!want_max && v < best) {
                    best = v;
                    best_slot = i;
                }
            }
            #[expect(
                clippy::cast_possible_truncation,
                reason = "slot index bounded by tensor length which fits u32"
            )]
            return Ok((self.slot_to_vid(best_slot as u32), Scalar::I64(best)));
        }
        let vals = t.values();
        if vals.is_empty() {
            return Err(error::shape_mismatch(
                "arg_extreme requires a non-empty map",
            ));
        }
        // Lowest-slot-id tie-break (proposal §4.3): a strict `>` / `<` keeps the
        // first (lowest) slot on ties since we scan in ascending order.
        let mut best_slot = 0usize;
        let mut best = vals[0];
        for (i, &v) in vals.iter().enumerate().skip(1) {
            if (want_max && v > best) || (!want_max && v < best) {
                best = v;
                best_slot = i;
            }
        }
        #[expect(
            clippy::cast_possible_truncation,
            reason = "slot index bounded by tensor length which fits u32"
        )]
        Ok((self.slot_to_vid(best_slot as u32), Scalar::F64(best)))
    }

    fn topk(&mut self, map: Handle, k: u32) -> Result<Vec<(Vid, Scalar)>, FnError> {
        let n = self.table.get_tensor(map)?.len();
        self.charge(n as u64)?;
        let t = self.table.get_tensor(map)?;
        if let Some(ivals) = t.values_i64() {
            // Sort by value desc, lowest-slot-id tie-break, then take k.
            let mut indexed: Vec<(usize, i64)> = ivals.iter().copied().enumerate().collect();
            indexed.sort_by(|&(ia, a), &(ib, b)| b.cmp(&a).then(ia.cmp(&ib)));
            return Ok(indexed
                .into_iter()
                .take(k as usize)
                .map(|(slot, v)| {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "slot index bounded by tensor length which fits u32"
                    )]
                    (self.slot_to_vid(slot as u32), Scalar::I64(v))
                })
                .collect());
        }
        // Sort by value desc, lowest-slot-id tie-break, then take k.
        let mut indexed: Vec<(usize, f64)> = t.values().iter().copied().enumerate().collect();
        indexed.sort_by(|&(ia, a), &(ib, b)| {
            b.partial_cmp(&a)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(ia.cmp(&ib))
        });
        Ok(indexed
            .into_iter()
            .take(k as usize)
            .map(|(slot, v)| {
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "slot index bounded by tensor length which fits u32"
                )]
                (self.slot_to_vid(slot as u32), Scalar::F64(v))
            })
            .collect())
    }

    fn l1_diff(&mut self, a: Handle, b: Handle) -> Result<f64, FnError> {
        let n = {
            let ta = self.table.get_tensor(a)?;
            let tb = self.table.get_tensor(b)?;
            if ta.len() != tb.len() {
                return Err(error::shape_mismatch(
                    "l1_diff requires two maps of equal length",
                ));
            }
            if ta.is_i64() || tb.is_i64() {
                return Err(error::shape_mismatch("l1_diff requires f64 maps"));
            }
            ta.len()
        };
        self.charge(n as u64)?;
        let ta = self.table.get_tensor(a)?;
        let tb = self.table.get_tensor(b)?;
        Ok(ta
            .values()
            .iter()
            .zip(tb.values())
            .map(|(x, y)| (x - y).abs())
            .sum())
    }

    fn emit(&mut self, cols: &[(&str, Handle)]) -> Result<(), FnError> {
        // Validate the emitted set against the declared columns (when known)
        // before any handle work: exactly the declared names, no repeats, no
        // extras, none missing (proposal §4.6, error 0x869).
        if let Some(expected) = &self.expected_columns {
            let mut seen: Vec<&str> = Vec::with_capacity(cols.len());
            for &(name, _) in cols {
                if seen.contains(&name) {
                    return Err(error::emit_schema_mismatch(format!(
                        "emit column `{name}` declared more than once"
                    )));
                }
                if !expected.iter().any(|e| e == name) {
                    return Err(error::emit_schema_mismatch(format!(
                        "emit column `{name}` is not a declared output field"
                    )));
                }
                seen.push(name);
            }
            for want in expected {
                if !seen.contains(&want.as_str()) {
                    return Err(error::emit_schema_mismatch(format!(
                        "declared output field `{want}` was not emitted"
                    )));
                }
            }
        }

        // Validate every column is a [V] map of equal length before capturing.
        let mut captured = Vec::with_capacity(cols.len());
        let mut expected_len: Option<usize> = None;
        for &(name, h) in cols {
            let t = self.table.get_tensor(h)?;
            match expected_len {
                Some(len) if len != t.len() => {
                    return Err(error::emit_schema_mismatch(format!(
                        "emit column `{name}` length {} != {len}",
                        t.len()
                    )));
                }
                _ => expected_len = Some(t.len()),
            }
            // Widen an i64 (path-count) column to the f64 result sink.
            captured.push((name.to_owned(), t.as_f64_vec()));
        }
        self.charge(expected_len.unwrap_or(0) as u64 * cols.len() as u64)?;
        self.emitted = captured;
        Ok(())
    }

    fn random_walks(
        &mut self,
        g: Handle,
        walk_length: usize,
        walks_per_node: usize,
        seeds: &[Vid],
        p: f64,
        q: f64,
        seed: u64,
    ) -> Result<Handle, FnError> {
        let graph = Arc::clone(self.table.get_graph(g)?);
        // Validate every seed maps into the projection before doing any work.
        for &vid in seeds {
            if graph.to_slot(vid).is_none() {
                return Err(error::seed_not_in_projection(vid.as_u64()));
            }
        }
        // The native walker touches Σ walks · length edges; charge that up front.
        let start_count = if seeds.is_empty() {
            graph.vertex_count()
        } else {
            seeds.len()
        };
        self.charge((start_count * walks_per_node * walk_length.max(1)) as u64)?;

        let config = RandomWalkConfig {
            walk_length,
            walks_per_node,
            start_nodes: seeds.to_vec(),
            return_param: p,
            in_out_param: q,
            seed: Some(seed),
        };
        let result = RandomWalk::run(&graph, config);
        // Store walks as slot sequences (translate each Vid back to its slot).
        let mut walks: Vec<Vec<u32>> = Vec::with_capacity(result.walks.len());
        for walk in &result.walks {
            let mut slots = Vec::with_capacity(walk.len());
            for &vid in walk {
                let slot = graph
                    .to_slot(vid)
                    .ok_or_else(|| error::seed_not_in_projection(vid.as_u64()))?;
                slots.push(slot);
            }
            walks.push(slots);
        }
        self.alloc_walks(WalkMatrix::new(walks))
    }

    fn walk_visit_counts(&mut self, walks: Handle, g: Handle) -> Result<Handle, FnError> {
        let n = self.table.get_graph(g)?.vertex_count();
        let wm = self.table.get_walks(walks)?;
        let total_steps: usize = wm.walks().iter().map(Vec::len).sum();
        let mut counts = vec![0.0f64; n];
        for walk in wm.walks() {
            for &slot in walk {
                counts[slot as usize] += 1.0;
            }
        }
        self.charge(total_steps as u64)?;
        self.alloc_tensor(Tensor::from_f64(counts))
    }

    fn neighborhood_overlap(
        &mut self,
        g: Handle,
        source: Vid,
        metric: OverlapMetric,
    ) -> Result<Handle, FnError> {
        let graph = Arc::clone(self.table.get_graph(g)?);
        let n = graph.vertex_count();
        let src = graph
            .to_slot(source)
            .ok_or_else(|| error::seed_not_in_projection(source.as_u64()))?;
        // Undirected neighbourhood of a slot: sorted, deduped out ∪ in neighbours.
        let undirected = |u: u32| -> Vec<u32> {
            let mut ns: Vec<u32> = graph.out_neighbors(u).to_vec();
            if graph.has_reverse() {
                ns.extend_from_slice(graph.in_neighbors(u));
            }
            ns.sort_unstable();
            ns.dedup();
            ns
        };
        let src_nbrs = undirected(src);
        let mut out = vec![0.0f64; n];
        // O(Σ_v (deg(src) + deg(v))) — charge each vertex's neighbourhood scan.
        let mut charged = 0u64;
        for v in 0..n as u32 {
            if v == src {
                continue;
            }
            let v_nbrs = undirected(v);
            charged += (src_nbrs.len() + v_nbrs.len()) as u64;
            let overlap = intersect_sorted_len(&src_nbrs, &v_nbrs) as f64;
            let (du, dv) = (src_nbrs.len() as f64, v_nbrs.len() as f64);
            out[v as usize] = match metric {
                OverlapMetric::Jaccard => {
                    let union = du + dv - overlap;
                    if union == 0.0 { 0.0 } else { overlap / union }
                }
                OverlapMetric::Overlap => {
                    let m = du.min(dv);
                    if m == 0.0 { 0.0 } else { overlap / m }
                }
                OverlapMetric::Cosine => {
                    let d = (du * dv).sqrt();
                    if d == 0.0 { 0.0 } else { overlap / d }
                }
            };
        }
        self.charge(charged.max(n as u64))?;
        self.alloc_tensor(Tensor::from_f64(out))
    }

    fn next_bucket(&mut self, dist: Handle, delta: f64, bucket: u32) -> Result<Handle, FnError> {
        let t = self.table.get_tensor(dist)?;
        if t.is_i64() {
            return Err(error::shape_mismatch(
                "next_bucket requires an f64 distance map",
            ));
        }
        if delta.is_nan() || delta <= 0.0 {
            return Err(error::shape_mismatch("next_bucket delta must be positive"));
        }
        let lo = f64::from(bucket) * delta;
        let hi = lo + delta;
        let n = t.len();
        let mut set = VertexSet::with_capacity(n);
        for (i, &d) in t.values().iter().enumerate() {
            if d.is_finite() && d >= lo && d < hi {
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "index bounded by tensor length which fits u32"
                )]
                set.insert(i as u32);
            }
        }
        self.charge(n as u64)?;
        self.alloc_set(set)
    }
}

/// Branchless sorted-slice intersection size (mirrors `triangle_count`).
fn intersect_sorted_len(a: &[u32], b: &[u32]) -> usize {
    let (mut i, mut j, mut count) = (0usize, 0usize, 0usize);
    while i < a.len() && j < b.len() {
        let (va, vb) = (a[i], b[j]);
        let le = va <= vb;
        let ge = va >= vb;
        count += usize::from(le && ge);
        i += usize::from(le);
        j += usize::from(ge);
    }
    count
}
