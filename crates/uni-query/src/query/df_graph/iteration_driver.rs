// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Fixpoint iteration driver (plugin-compute proposal §6, DF-5).
//!
//! DataFusion's recursive CTE is linear-only, off-by-default, and buggy, so it
//! cannot express a graph fixpoint (PageRank/BFS). The loop therefore lives
//! *beside* DataFusion, in a driver that **re-invokes a cached physical sub-plan
//! once per round** rather than re-planning: the per-round body is an
//! [`ExecutionPlan`] planned exactly once, and the previous round's output is
//! fed back as the next round's input through a shared state handle the plan
//! re-reads on each `execute()`. This is the lift the Mode B-vec message-passing
//! iteration (§7a) builds on — its round body is a `edges JOIN state → GROUP BY
//! dst → guest-UDAF` sub-plan; here the reference round body is a single
//! PageRank power-iteration step, so the driver can be validated against the
//! native `personalized_pagerank` to `1e-9`.
//!
//! The key contract (test DF-5): planning happens **once** — the driver's
//! [`IterationDriver::plan_count`] stays `1` no matter how many rounds run.
//
// Rust guideline compliant

use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::{Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use parking_lot::RwLock;
use uni_algo::algo::GraphProjection;

use crate::query::df_graph::common::{collect_all_partitions, compute_plan_properties};

/// The per-round state shared between the driver and its cached round plan.
///
/// The round plan reads the current `[V]` rank from here on each `execute()`;
/// the driver writes the round's output back before the next `execute()`. This
/// shared handle is what lets the physical plan be cached (planned once) yet see
/// fresh state each round — the same discipline the Locy fixpoint loop uses for
/// its derived-scan leaves.
type SharedRank = Arc<RwLock<Vec<f64>>>;

/// One PageRank power-iteration step as a cached, re-executable `ExecutionPlan`.
///
/// A leaf/source node: it holds the immutable projection and the precomputed
/// per-round-invariant vectors (teleport, `inv_deg`, dangling mask), reads the
/// current rank from the shared `SharedRank` handle, and emits the next rank
/// as a `(nodeId, score)` batch in slot order. Constructed **once** and executed
/// once per round by [`IterationDriver`]; the numeric spec mirrors
/// `personalized_pagerank` exactly (fixed slot-order accumulation) so the driven
/// fixpoint matches the native kernel to `1e-9`.
pub struct PowerStepExec {
    graph: Arc<GraphProjection>,
    alpha: f64,
    teleport: Vec<f64>,
    inv_deg: Vec<f64>,
    dangling: Vec<bool>,
    state: SharedRank,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for PowerStepExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PowerStepExec")
            .field("vertices", &self.graph.vertex_count())
            .field("alpha", &self.alpha)
            .finish()
    }
}

impl PowerStepExec {
    /// Builds the cached power-step plan and the shared state seeded to teleport.
    ///
    /// `seeds` are the personalization slots; an empty seed set teleports
    /// uniformly. Returns the plan plus the shared `SharedRank` handle the
    /// driver writes each round's output into.
    #[must_use]
    pub fn new(
        graph: Arc<GraphProjection>,
        seed_slots: &[u32],
        alpha: f64,
    ) -> (Arc<Self>, SharedRank) {
        let n = graph.vertex_count();
        // Teleport = L1-normalized seed indicator (uniform when no seeds).
        let mut teleport = vec![0.0f64; n];
        if seed_slots.is_empty() {
            let u = 1.0 / n as f64;
            teleport.iter_mut().for_each(|t| *t = u);
        } else {
            let w = 1.0 / seed_slots.len() as f64;
            for &s in seed_slots {
                teleport[s as usize] = w;
            }
        }
        // inv_deg[u] = 1/outdeg(u), 0 for dangling (recip(0) = 0).
        let mut inv_deg = vec![0.0f64; n];
        let mut dangling = vec![false; n];
        for u in 0..n as u32 {
            let d = graph.out_degree(u);
            if d == 0 {
                dangling[u as usize] = true;
            } else {
                inv_deg[u as usize] = 1.0 / d as f64;
            }
        }
        let state: SharedRank = Arc::new(RwLock::new(teleport.clone()));
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("nodeId", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
        ]));
        let properties = compute_plan_properties(schema.clone());
        let plan = Arc::new(Self {
            graph,
            alpha,
            teleport,
            inv_deg,
            dangling,
            state: Arc::clone(&state),
            schema,
            properties,
        });
        (plan, state)
    }

    /// Computes one power-iteration step from `rank`, in fixed slot order.
    ///
    /// `next = alpha * spread + (1 - alpha + alpha*dm) * teleport`, with
    /// `spread[v] = Σ_{u→v} rank[u]*inv_deg[u]*w(u,v)` and `dm` the dangling
    /// mass — the exact `personalized_pagerank` recurrence.
    fn step(&self, rank: &[f64]) -> Vec<f64> {
        let n = self.graph.vertex_count();
        let has_w = self.graph.has_weights();
        let mut spread = vec![0.0f64; n];
        let mut dm = 0.0f64;
        for u in 0..n as u32 {
            let ui = u as usize;
            if self.dangling[ui] {
                dm += rank[ui];
                continue;
            }
            let contrib = rank[ui] * self.inv_deg[ui];
            if contrib == 0.0 {
                continue;
            }
            for (k, &v) in self.graph.out_neighbors(u).iter().enumerate() {
                let w = if has_w {
                    self.graph.out_weight(u, k)
                } else {
                    1.0
                };
                spread[v as usize] += contrib * w;
            }
        }
        let blend = 1.0 - self.alpha + self.alpha * dm;
        (0..n)
            .map(|v| self.alpha * spread[v] + blend * self.teleport[v])
            .collect()
    }
}

impl DisplayAs for PowerStepExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PowerStepExec: vertices={}", self.graph.vertex_count())
    }
}

impl ExecutionPlan for PowerStepExec {
    fn name(&self) -> &str {
        "PowerStepExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // Read the current rank from the shared handle and emit the next rank as
        // a slot-ordered (nodeId, score) batch. Reading fresh state on every
        // execute() is what makes caching this plan across rounds sound.
        let rank = self.state.read().clone();
        let next = self.step(&rank);
        let node_ids: Vec<i64> = (0..self.graph.vertex_count())
            .map(|slot| {
                #[expect(clippy::cast_possible_wrap, reason = "vertex ids fit i64 in practice")]
                let id = self.graph.to_vid(slot as u32).as_u64() as i64;
                id
            })
            .collect();
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(Int64Array::from(node_ids)),
                Arc::new(Float64Array::from(next)),
            ],
        )
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
        let schema = self.schema.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move { Ok(batch) }),
        )))
    }
}

/// A guest-authorable message-passing aggregate body (proposal §7a, Mode B-vec).
///
/// This is the pluggable slot that a guest UDAF fills: the per-destination
/// aggregation of the messages arriving along in-edges (`edges JOIN state →
/// GROUP BY dst → agg`). PageRank uses summation; belief propagation would use a
/// max/product monoid; an ABM tick a custom combine. Because the aggregate is a
/// monoid (`identity` + associative `combine` + `finalize`), it rides the
/// existing UDAF sandbox (partial-state serialization) unchanged — the only new
/// wiring is the graph-gather that feeds it, which [`GraphGatherStepExec`]
/// provides.
pub trait MessageAggregate: Send + Sync {
    /// The aggregate's identity element (a vertex with no incoming messages).
    fn identity(&self) -> f64;
    /// Folds one incoming message into the running per-destination accumulator.
    fn combine(&self, acc: f64, msg: f64) -> f64;
    /// Produces the gathered value for a destination from its accumulator.
    fn finalize(&self, acc: f64) -> f64;
}

/// The summation aggregate — PageRank's message-passing body.
#[derive(Debug, Default, Clone, Copy)]
pub struct SumAggregate;

impl MessageAggregate for SumAggregate {
    fn identity(&self) -> f64 {
        0.0
    }
    fn combine(&self, acc: f64, msg: f64) -> f64 {
        acc + msg
    }
    fn finalize(&self, acc: f64) -> f64 {
        acc
    }
}

/// One message-passing PageRank round as a cached, re-executable `ExecutionPlan`
/// (proposal §7a, Mode B-vec graph-gather).
///
/// Unlike [`PowerStepExec`] (a vertex-centric CSR walk), this expresses the round
/// as the message-passing workhorse the proposal names — **`edges JOIN state →
/// GROUP BY dst → aggregate`**: each directed edge carries the message
/// `state[src] / outdeg(src) * w`, and the pluggable [`MessageAggregate`] reduces
/// the messages arriving at each destination. Driven by the same
/// [`IterationDriver`], so the driver is proven generic over round bodies; with
/// [`SumAggregate`] it reproduces PageRank and matches the native kernel to
/// `1e-9`. Swapping the aggregate for a guest UDAF is the Mode B-vec extension.
pub struct GraphGatherStepExec {
    edges: Vec<(u32, u32, f64)>, // (src slot, dst slot, weight) — the "edges table"
    node_ids: Vec<i64>,
    inv_deg: Vec<f64>,
    dangling: Vec<bool>,
    teleport: Vec<f64>,
    alpha: f64,
    agg: Arc<dyn MessageAggregate>,
    state: SharedRank,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for GraphGatherStepExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphGatherStepExec")
            .field("edges", &self.edges.len())
            .field("vertices", &self.node_ids.len())
            .finish()
    }
}

impl GraphGatherStepExec {
    /// Builds the cached message-passing round and its shared state (teleport-seeded).
    #[must_use]
    pub fn new(
        graph: &GraphProjection,
        seed_slots: &[u32],
        alpha: f64,
        agg: Arc<dyn MessageAggregate>,
    ) -> (Arc<Self>, SharedRank) {
        let n = graph.vertex_count();
        let mut edges = Vec::with_capacity(graph.edge_count());
        let mut inv_deg = vec![0.0f64; n];
        let mut dangling = vec![false; n];
        let has_w = graph.has_weights();
        for u in 0..n as u32 {
            let d = graph.out_degree(u);
            if d == 0 {
                dangling[u as usize] = true;
            } else {
                inv_deg[u as usize] = 1.0 / d as f64;
            }
            for (k, &v) in graph.out_neighbors(u).iter().enumerate() {
                let w = if has_w { graph.out_weight(u, k) } else { 1.0 };
                edges.push((u, v, w));
            }
        }
        let mut teleport = vec![0.0f64; n];
        if seed_slots.is_empty() {
            teleport.iter_mut().for_each(|t| *t = 1.0 / n as f64);
        } else {
            let w = 1.0 / seed_slots.len() as f64;
            for &s in seed_slots {
                teleport[s as usize] = w;
            }
        }
        let node_ids: Vec<i64> = (0..n as u32)
            .map(|slot| {
                #[expect(clippy::cast_possible_wrap, reason = "vertex ids fit i64")]
                let id = graph.to_vid(slot).as_u64() as i64;
                id
            })
            .collect();
        let state: SharedRank = Arc::new(RwLock::new(teleport.clone()));
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("nodeId", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
        ]));
        let properties = compute_plan_properties(schema.clone());
        let plan = Arc::new(Self {
            edges,
            node_ids,
            inv_deg,
            dangling,
            teleport,
            alpha,
            agg,
            state: Arc::clone(&state),
            schema,
            properties,
        });
        (plan, state)
    }

    /// One message-passing step: gather messages by destination via the pluggable
    /// aggregate, then apply the PageRank teleport blend.
    fn step(&self, rank: &[f64]) -> Vec<f64> {
        let n = self.node_ids.len();
        // GROUP BY dst: fold each edge's message into its destination accumulator.
        let mut acc: Vec<f64> = vec![self.agg.identity(); n];
        let dm: f64 = rank
            .iter()
            .zip(self.dangling.iter())
            .filter_map(|(r, &is_dangling)| if is_dangling { Some(*r) } else { None })
            .sum();
        for &(src, dst, w) in &self.edges {
            let msg = rank[src as usize] * self.inv_deg[src as usize] * w;
            let d = dst as usize;
            acc[d] = self.agg.combine(acc[d], msg);
        }
        let blend = 1.0 - self.alpha + self.alpha * dm;
        (0..n)
            .map(|v| self.alpha * self.agg.finalize(acc[v]) + blend * self.teleport[v])
            .collect()
    }
}

impl DisplayAs for GraphGatherStepExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GraphGatherStepExec: edges={}", self.edges.len())
    }
}

impl ExecutionPlan for GraphGatherStepExec {
    fn name(&self) -> &str {
        "GraphGatherStepExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let rank = self.state.read().clone();
        let next = self.step(&rank);
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(Int64Array::from(self.node_ids.clone())),
                Arc::new(Float64Array::from(next)),
            ],
        )
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
        let schema = self.schema.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move { Ok(batch) }),
        )))
    }
}

/// Drives a graph fixpoint by re-invoking a **cached** round plan per round.
///
/// Constructed once around a pre-planned round body (here [`PowerStepExec`]) and
/// the shared state it reads; each round the driver executes the cached plan,
/// reads back the next `[V]` state, checks L1 convergence, and writes the new
/// state for the next round — never re-planning. [`plan_count`](Self::plan_count)
/// therefore stays `1` for the driver's whole life (test DF-5).
pub struct IterationDriver {
    round: Arc<dyn ExecutionPlan>,
    state: SharedRank,
    max_iters: usize,
    tol: f64,
    plan_count: Arc<AtomicUsize>,
    rounds_run: AtomicUsize,
}

impl IterationDriver {
    /// Builds a driver over a cached round plan and its shared state handle.
    ///
    /// `round` is planned by the caller exactly once; the driver never re-plans
    /// it. `state` must be the same handle the round plan reads.
    #[must_use]
    pub fn new(
        round: Arc<dyn ExecutionPlan>,
        state: SharedRank,
        max_iters: usize,
        tol: f64,
    ) -> Self {
        Self {
            round,
            state,
            max_iters,
            tol,
            // The single planning event: the round body was compiled once.
            plan_count: Arc::new(AtomicUsize::new(1)),
            rounds_run: AtomicUsize::new(0),
        }
    }

    /// Number of times the round body was *planned* — always `1` (DF-5).
    #[must_use]
    pub fn plan_count(&self) -> usize {
        self.plan_count.load(Ordering::Relaxed)
    }

    /// Number of rounds executed by the last [`run`](Self::run).
    #[must_use]
    pub fn rounds_run(&self) -> usize {
        self.rounds_run.load(Ordering::Relaxed)
    }

    /// Runs the fixpoint to L1 convergence, returning the final `[V]` state.
    ///
    /// Each round: execute the cached plan (reading current state) → read back
    /// the `score` column as the next state → L1 delta vs the current state →
    /// write the next state for the following round. Stops at convergence or the
    /// iteration cap.
    ///
    /// # Errors
    /// Returns a `DataFusionError` if the cached round plan fails to execute or
    /// produces a batch without the expected `score` column.
    pub async fn run(&self, task_ctx: Arc<TaskContext>) -> DFResult<Vec<f64>> {
        self.rounds_run.store(0, Ordering::Relaxed);
        for _ in 0..self.max_iters {
            let batches = collect_all_partitions(&self.round, task_ctx.clone()).await?;
            let next = extract_score_column(&batches)?;
            let cur = self.state.read().clone();
            let l1: f64 = cur
                .iter()
                .zip(next.iter())
                .map(|(a, b)| (a - b).abs())
                .sum();
            *self.state.write() = next.clone();
            self.rounds_run.fetch_add(1, Ordering::Relaxed);
            if l1 < self.tol {
                return Ok(next);
            }
        }
        Ok(self.state.read().clone())
    }
}

/// Reads the `score` (Float64) column out of the round's output batches, in row
/// order (slot order), concatenating across batches.
fn extract_score_column(batches: &[RecordBatch]) -> DFResult<Vec<f64>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of("score").map_err(|_| {
            datafusion::error::DataFusionError::Execution(
                "iteration round output missing `score` column".to_string(),
            )
        })?;
        let col = b
            .column(idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "iteration round `score` column is not Float64".to_string(),
                )
            })?;
        out.extend_from_slice(col.values());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use std::collections::HashMap;
    use uni_common::Value;

    /// Builds a small directed projection from `(src, dst)` edges over `n` nodes.
    fn projection(n: usize, edges: &[(u64, u64)]) -> Arc<GraphProjection> {
        let nodes: Vec<HashMap<String, Value>> = (0..n as u64)
            .map(|id| HashMap::from([("id".to_string(), Value::Int(id as i64))]))
            .collect();
        let edge_rows: Vec<HashMap<String, Value>> = edges
            .iter()
            .map(|&(s, d)| {
                HashMap::from([
                    ("source".to_string(), Value::Int(s as i64)),
                    ("target".to_string(), Value::Int(d as i64)),
                ])
            })
            .collect();
        Arc::new(
            GraphProjection::from_rows(&nodes, &edge_rows, None, false).expect("projection builds"),
        )
    }

    /// An *independent* native reference: a plain adjacency-list personalized
    /// PageRank power iteration that shares no code with `PowerStepExec` (builds
    /// its own adjacency, degree, and dangling structures from `(src,dst)` edges),
    /// so agreement with the plan-driven fixpoint is real evidence, not a shared
    /// implementation. Mirrors the `personalized_pagerank` recurrence exactly.
    fn native_pagerank(
        n: usize,
        edges: &[(u64, u64)],
        seed_slots: &[u32],
        alpha: f64,
        tol: f64,
    ) -> Vec<f64> {
        let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut outdeg = vec![0usize; n];
        for &(s, d) in edges {
            adj[s as usize].push(d as usize);
            outdeg[s as usize] += 1;
        }
        let mut teleport = vec![0.0f64; n];
        if seed_slots.is_empty() {
            teleport.iter_mut().for_each(|t| *t = 1.0 / n as f64);
        } else {
            let w = 1.0 / seed_slots.len() as f64;
            for &s in seed_slots {
                teleport[s as usize] = w;
            }
        }
        let mut rank = teleport.clone();
        for _ in 0..1000 {
            let mut spread = vec![0.0f64; n];
            let mut dm = 0.0f64;
            for u in 0..n {
                if outdeg[u] == 0 {
                    dm += rank[u];
                    continue;
                }
                let contrib = rank[u] / outdeg[u] as f64;
                for &v in &adj[u] {
                    spread[v] += contrib;
                }
            }
            let blend = 1.0 - alpha + alpha * dm;
            let next: Vec<f64> = (0..n)
                .map(|v| alpha * spread[v] + blend * teleport[v])
                .collect();
            let l1: f64 = rank.iter().zip(&next).map(|(a, b)| (a - b).abs()).sum();
            rank = next;
            if l1 < tol {
                break;
            }
        }
        rank
    }

    #[tokio::test]
    async fn df5_driver_reaches_pagerank_fixpoint_with_one_planning() {
        // DF-5: the driver re-invokes the cached PowerStepExec each round to a
        // fixpoint; the plan is compiled once (plan_count == 1) yet executed many
        // rounds, and the result matches the native recurrence to 1e-9.
        let edges = [(0u64, 1u64), (1, 2), (2, 0), (0, 3), (3, 4)];
        let graph = projection(5, &edges);
        let seeds = [0u32];
        let alpha = 0.85;
        let tol = 1e-12;

        let (plan, state) = PowerStepExec::new(Arc::clone(&graph), &seeds, alpha);
        let driver = IterationDriver::new(plan, state, 1000, tol);
        let task_ctx = SessionContext::new().task_ctx();
        let got = driver.run(task_ctx).await.expect("driver runs");

        // The cached plan was planned exactly once, executed many rounds.
        assert_eq!(
            driver.plan_count(),
            1,
            "the sub-plan must be cached, not re-planned"
        );
        assert!(
            driver.rounds_run() > 1,
            "the fixpoint must take several rounds"
        );

        let want = native_pagerank(5, &edges, &seeds, alpha, tol);
        assert_eq!(got.len(), want.len());
        for (i, (a, b)) in got.iter().zip(&want).enumerate() {
            assert!(
                (a - b).abs() <= 1e-9,
                "slot {i}: driver {a} vs native {b} exceeds 1e-9"
            );
        }
        // PageRank conserves mass (Σ ≈ 1).
        let mass: f64 = got.iter().sum();
        assert!(
            (mass - 1.0).abs() < 1e-6,
            "PPR mass must sum to 1, got {mass}"
        );
    }

    #[tokio::test]
    async fn v1_message_passing_pagerank_matches_native() {
        // V-1 (core): PageRank expressed as `edges → GROUP BY dst → SumAggregate`
        // (message-passing), driven by the same IterationDriver, matches the
        // independent adjacency-list reference to 1e-9 — proving the graph-gather
        // round body + driver, with a pluggable aggregate. A guest UDAF swapped
        // for SumAggregate is the Mode B-vec extension (rides the UDAF sandbox).
        let edges = [(0u64, 1u64), (1, 2), (2, 0), (0, 3), (3, 4), (4, 1)];
        let graph = projection(5, &edges);
        let seeds = [0u32];
        let alpha = 0.85;
        let tol = 1e-12;

        let (plan, state) = GraphGatherStepExec::new(&graph, &seeds, alpha, Arc::new(SumAggregate));
        let driver = IterationDriver::new(plan, state, 1000, tol);
        let task_ctx = SessionContext::new().task_ctx();
        let got = driver.run(task_ctx).await.expect("driver runs");

        assert_eq!(
            driver.plan_count(),
            1,
            "the gather sub-plan is cached, not re-planned"
        );
        let want = native_pagerank(5, &edges, &seeds, alpha, tol);
        for (i, (a, b)) in got.iter().zip(&want).enumerate() {
            assert!(
                (a - b).abs() <= 1e-9,
                "slot {i}: message-passing {a} vs native {b} exceeds 1e-9"
            );
        }
        let mass: f64 = got.iter().sum();
        assert!(
            (mass - 1.0).abs() < 1e-6,
            "PPR mass must sum to 1, got {mass}"
        );
    }

    #[tokio::test]
    async fn v1_gather_agrees_with_vertex_centric_step() {
        // The message-passing gather and the vertex-centric CSR walk are two
        // independent formulations of the same round — they must agree bitwise on
        // the driven fixpoint (both are PageRank, same seeds/alpha).
        let edges = [(0u64, 1u64), (1, 2), (2, 0), (0, 3), (3, 4)];
        let graph = projection(5, &edges);
        let seeds = [0u32];
        let (alpha, tol) = (0.85, 1e-12);
        let task_ctx = SessionContext::new().task_ctx();

        let (p1, s1) = PowerStepExec::new(Arc::clone(&graph), &seeds, alpha);
        let a = IterationDriver::new(p1, s1, 1000, tol)
            .run(task_ctx.clone())
            .await
            .unwrap();
        let (p2, s2) = GraphGatherStepExec::new(&graph, &seeds, alpha, Arc::new(SumAggregate));
        let b = IterationDriver::new(p2, s2, 1000, tol)
            .run(task_ctx)
            .await
            .unwrap();
        for (i, (x, y)) in a.iter().zip(&b).enumerate() {
            assert!((x - y).abs() <= 1e-12, "slot {i}: {x} vs {y}");
        }
    }

    #[tokio::test]
    async fn v1_datafusion_group_by_gather_equals_the_hand_coded_gather() {
        // V-1 (DataFusion path): the message-passing gather runs as a *real*
        // DataFusion relational aggregate — `edges JOIN state → GROUP BY dst →
        // SUM(score·contrib)` — and its per-destination spread equals the
        // hand-coded `GraphGatherStepExec` gather. This is the "rides the existing
        // UDF/UDAF sandbox" path (§7a): a guest UDAF registers and slots into this
        // GROUP BY exactly the way the built-in `sum` does here.
        use arrow_array::{Float64Array, Int64Array};
        use datafusion::catalog::TableProvider;
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        let edges_list = [(0u64, 1u64), (1, 2), (2, 0), (0, 3), (3, 4), (4, 1)];
        let graph = projection(5, &edges_list);
        let n = graph.vertex_count();

        // Current [V] state (arbitrary but fixed) and per-source contribution
        // factor inv_deg(src) (unweighted → weight 1).
        let score: Vec<f64> = (0..n).map(|i| 1.0 + i as f64).collect();
        let mut inv_deg = vec![0.0f64; n];
        for u in 0..n as u32 {
            let d = graph.out_degree(u);
            if d > 0 {
                inv_deg[u as usize] = 1.0 / d as f64;
            }
        }

        // edges table: (src, dst, contrib = inv_deg(src)).
        let (mut src_col, mut dst_col, mut contrib_col) = (vec![], vec![], vec![]);
        for u in 0..n as u32 {
            for &v in graph.out_neighbors(u) {
                src_col.push(i64::from(u));
                dst_col.push(i64::from(v));
                contrib_col.push(inv_deg[u as usize]);
            }
        }
        let edges_schema = Arc::new(Schema::new(vec![
            Field::new("src", DataType::Int64, false),
            Field::new("dst", DataType::Int64, false),
            Field::new("contrib", DataType::Float64, false),
        ]));
        let edges_batch = RecordBatch::try_new(
            edges_schema.clone(),
            vec![
                Arc::new(Int64Array::from(src_col)),
                Arc::new(Int64Array::from(dst_col)),
                Arc::new(Float64Array::from(contrib_col)),
            ],
        )
        .unwrap();

        // state table: (node, score).
        let state_schema = Arc::new(Schema::new(vec![
            Field::new("node", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
        ]));
        let state_batch = RecordBatch::try_new(
            state_schema.clone(),
            vec![
                Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())),
                Arc::new(Float64Array::from(score.clone())),
            ],
        )
        .unwrap();

        let ctx = SessionContext::new();
        let edges_provider: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(edges_schema, vec![vec![edges_batch]]).unwrap());
        let state_provider: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(state_schema, vec![vec![state_batch]]).unwrap());
        ctx.register_table("edges", edges_provider).unwrap();
        ctx.register_table("state", state_provider).unwrap();

        // The message-passing gather as a DataFusion GROUP BY aggregate.
        let df = ctx
            .sql(
                "SELECT e.dst AS node, SUM(s.score * e.contrib) AS spread \
                 FROM edges e JOIN state s ON e.src = s.node \
                 GROUP BY e.dst",
            )
            .await
            .expect("gather query plans");
        let batches = df.collect().await.expect("gather query runs");

        // Read the DataFusion spread into a [V] vector.
        let mut df_spread = vec![0.0f64; n];
        for b in &batches {
            let node = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let spread = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
            for r in 0..b.num_rows() {
                df_spread[node.value(r) as usize] = spread.value(r);
            }
        }

        // Independent hand-coded gather: spread[v] = Σ_{u→v} score[u]*inv_deg[u].
        let mut want = vec![0.0f64; n];
        for u in 0..n as u32 {
            for &v in graph.out_neighbors(u) {
                want[v as usize] += score[u as usize] * inv_deg[u as usize];
            }
        }
        for (i, (a, b)) in df_spread.iter().zip(&want).enumerate() {
            assert!(
                (a - b).abs() <= 1e-9,
                "slot {i}: DataFusion GROUP BY gather {a} vs hand-coded {b}"
            );
        }
    }

    /// A guest-authored summation aggregate (a plugin `AggregatePluginFn`), the
    /// Mode B-vec message-passing body registered through the existing UDAF
    /// sandbox rather than the built-in `sum`.
    struct GuestSum {
        sig: uni_plugin::traits::aggregate::AggSignature,
    }
    struct GuestSumAcc {
        sum: f64,
    }
    impl uni_plugin::traits::aggregate::AggregatePluginFn for GuestSum {
        fn signature(&self) -> &uni_plugin::traits::aggregate::AggSignature {
            &self.sig
        }
        fn create_accumulator(&self) -> Box<dyn uni_plugin::traits::aggregate::PluginAccumulator> {
            Box::new(GuestSumAcc { sum: 0.0 })
        }
    }
    impl uni_plugin::traits::aggregate::PluginAccumulator for GuestSumAcc {
        fn update_batch(
            &mut self,
            values: &[arrow_array::ArrayRef],
        ) -> Result<(), uni_plugin::errors::FnError> {
            let col = values[0]
                .as_any()
                .downcast_ref::<arrow_array::Float64Array>()
                .unwrap();
            self.sum += col.values().iter().sum::<f64>();
            Ok(())
        }
        fn merge_batch(
            &mut self,
            states: &[arrow_array::ArrayRef],
        ) -> Result<(), uni_plugin::errors::FnError> {
            let col = states[0]
                .as_any()
                .downcast_ref::<arrow_array::Float64Array>()
                .unwrap();
            self.sum += col.values().iter().sum::<f64>();
            Ok(())
        }
        fn state(
            &self,
        ) -> Result<Vec<datafusion::scalar::ScalarValue>, uni_plugin::errors::FnError> {
            Ok(vec![datafusion::scalar::ScalarValue::Float64(Some(
                self.sum,
            ))])
        }
        fn evaluate(&self) -> Result<datafusion::scalar::ScalarValue, uni_plugin::errors::FnError> {
            Ok(datafusion::scalar::ScalarValue::Float64(Some(self.sum)))
        }
        fn size(&self) -> usize {
            std::mem::size_of::<Self>()
        }
    }

    #[tokio::test]
    async fn v1_guest_udaf_drives_the_graph_gather_group_by() {
        // V-1 (guest UDAF): the message-passing gather's aggregate is an actual
        // guest-authored plugin `AggregatePluginFn` (`myco.gsum`) bridged into
        // DataFusion via `PluginAggregateUdaf` and run in the `edges JOIN state →
        // GROUP BY dst` plan — proving a guest UDAF slots into the graph gather
        // exactly like the built-in `sum`. Its result matches the hand-coded
        // gather to 1e-9 (§7a Mode B-vec, riding the existing UDAF sandbox).
        use arrow_array::{Float64Array, Int64Array};
        use arrow_schema::Field as ArrowField;
        use datafusion::catalog::TableProvider;
        use datafusion::datasource::MemTable;
        use datafusion::logical_expr::{AggregateUDF, Volatility};
        use datafusion::prelude::SessionContext;
        use uni_plugin::traits::aggregate::AggSignature;
        use uni_plugin::traits::scalar::ArgType;
        use uni_plugin::{
            Capability, CapabilitySet, PluginId, PluginRegistrar, PluginRegistry, QName,
        };

        let edges_list = [(0u64, 1u64), (1, 2), (2, 0), (0, 3), (3, 4), (4, 1)];
        let graph = projection(5, &edges_list);
        let n = graph.vertex_count();
        let score: Vec<f64> = (0..n).map(|i| 1.0 + i as f64).collect();
        let mut inv_deg = vec![0.0f64; n];
        for u in 0..n as u32 {
            let d = graph.out_degree(u);
            if d > 0 {
                inv_deg[u as usize] = 1.0 / d as f64;
            }
        }

        // Register the guest aggregate `myco.gsum` through the plugin registrar.
        let sig = AggSignature::new(
            vec![ArgType::Primitive(DataType::Float64)],
            ArgType::Primitive(DataType::Float64),
            vec![ArrowField::new("sum", DataType::Float64, true)],
            Volatility::Immutable,
        );
        let pr = PluginRegistry::new();
        let caps = CapabilitySet::from_iter_of([Capability::AggregateFn]);
        let mut reg = PluginRegistrar::new(PluginId::new("myco"), &caps, &pr);
        reg.aggregate_fn(
            QName::new("myco", "gsum"),
            sig.clone(),
            Arc::new(GuestSum { sig: sig.clone() }),
        )
        .unwrap();
        reg.commit_to_registry().unwrap();
        let registry = Arc::new(pr);

        // Bridge the guest aggregate into a DataFusion UDAF.
        let qname = QName::new("myco", "gsum");
        let entry = registry
            .aggregate(&qname)
            .expect("guest aggregate registered");
        let udaf = AggregateUDF::from(crate::query::df_udaf_plugin::PluginAggregateUdaf::new(
            qname,
            Arc::clone(&registry),
            entry.signature.clone(),
        ));

        // edges + state tables (as in the built-in-sum gather test).
        let (mut src_col, mut dst_col, mut contrib_col) = (vec![], vec![], vec![]);
        for u in 0..n as u32 {
            for &v in graph.out_neighbors(u) {
                src_col.push(i64::from(u));
                dst_col.push(i64::from(v));
                contrib_col.push(inv_deg[u as usize]);
            }
        }
        let edges_schema = Arc::new(Schema::new(vec![
            Field::new("src", DataType::Int64, false),
            Field::new("dst", DataType::Int64, false),
            Field::new("contrib", DataType::Float64, false),
        ]));
        let edges_batch = RecordBatch::try_new(
            edges_schema.clone(),
            vec![
                Arc::new(Int64Array::from(src_col)),
                Arc::new(Int64Array::from(dst_col)),
                Arc::new(Float64Array::from(contrib_col)),
            ],
        )
        .unwrap();
        let state_schema = Arc::new(Schema::new(vec![
            Field::new("node", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
        ]));
        let state_batch = RecordBatch::try_new(
            state_schema.clone(),
            vec![
                Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())),
                Arc::new(Float64Array::from(score.clone())),
            ],
        )
        .unwrap();

        let ctx = SessionContext::new();
        ctx.register_udaf(udaf);
        let edges_provider: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(edges_schema, vec![vec![edges_batch]]).unwrap());
        let state_provider: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(state_schema, vec![vec![state_batch]]).unwrap());
        ctx.register_table("edges", edges_provider).unwrap();
        ctx.register_table("state", state_provider).unwrap();

        // The gather GROUP BY, aggregated by the GUEST UDAF `myco.gsum`.
        let df = ctx
            .sql(
                "SELECT e.dst AS node, \"myco.gsum\"(s.score * e.contrib) AS spread \
                 FROM edges e JOIN state s ON e.src = s.node \
                 GROUP BY e.dst",
            )
            .await
            .expect("guest-UDAF gather query plans");
        let batches = df.collect().await.expect("guest-UDAF gather runs");

        let mut got = vec![0.0f64; n];
        for b in &batches {
            let node = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let spread = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
            for r in 0..b.num_rows() {
                got[node.value(r) as usize] = spread.value(r);
            }
        }
        let mut want = vec![0.0f64; n];
        for u in 0..n as u32 {
            for &v in graph.out_neighbors(u) {
                want[v as usize] += score[u as usize] * inv_deg[u as usize];
            }
        }
        for (i, (a, b)) in got.iter().zip(&want).enumerate() {
            assert!(
                (a - b).abs() <= 1e-9,
                "slot {i}: guest-UDAF gather {a} vs hand-coded {b}"
            );
        }
    }

    #[tokio::test]
    async fn df5_cached_plan_is_reused_across_runs() {
        // Re-running the driver does not re-plan: plan_count stays 1 across two
        // full fixpoint runs over the same cached plan.
        let graph = projection(4, &[(0, 1), (1, 2), (2, 3), (3, 0)]);
        let (plan, state) = PowerStepExec::new(graph, &[0u32], 0.85);
        let driver = IterationDriver::new(plan, state, 500, 1e-12);
        let task_ctx = SessionContext::new().task_ctx();
        let _ = driver.run(task_ctx.clone()).await.expect("first run");
        let first_rounds = driver.rounds_run();
        let _ = driver.run(task_ctx).await.expect("second run");
        assert_eq!(driver.plan_count(), 1, "no re-planning across runs");
        assert!(first_rounds >= 1);
    }
}
