//! A vertex-centric (Pregel) executor built on the public [`GraphView`].
//!
//! Pregel is shipped as a *library atop [`AlgorithmProvider`]*, not a revived
//! plugin surface kind: a [`VertexProgram`] defines per-vertex `init`/`compute`
//! plus an optional message `combine`, and [`run_pregel`] drives supersteps over
//! a frozen `GraphView`. Concrete first-party programs ([`PageRankProvider`],
//! [`SsspProvider`]) wrap a program as an `AlgorithmProvider`, inheriting the
//! already-wired CALL dispatch and `HostQuery` capability gate.
//!
//! # Model
//!
//! Each vertex owns a `State` and a message inbox. In a superstep every active
//! vertex (or one that received messages) runs [`VertexProgram::compute`], which
//! returns its next state, messages addressed to neighbor slots, and whether it
//! votes to halt. A halted vertex reactivates when a message arrives. Execution
//! stops when no vertex is active and no messages are in flight, or after
//! `max_supersteps`. An optional [`VertexProgram::combine`] folds messages bound
//! for the same target to bound memory.
//
// Rust guideline compliant

use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use uni_common::core::id::Vid;
use uni_plugin::FnError;
use uni_plugin::traits::algorithm::{
    AlgorithmContext, AlgorithmHost, AlgorithmProvider, AlgorithmSignature, GraphProjectionSpec,
    GraphView,
};

/// The result of one vertex's superstep: its next state, the messages it emits
/// (`(target_slot, message)`), and whether it votes to halt.
#[derive(Debug, Clone)]
pub struct VertexStep<S, M> {
    /// The vertex's state after this superstep.
    pub state: S,
    /// Messages to deliver to neighbor slots next superstep.
    pub messages: Vec<(u32, M)>,
    /// `true` to go inactive until a message reactivates this vertex.
    pub halt: bool,
}

/// A vertex-centric program driven by [`run_pregel`].
pub trait VertexProgram: Send + Sync {
    /// Per-vertex mutable state carried across supersteps.
    type State: Clone + Send;
    /// The message type exchanged between vertices.
    type Message: Clone + Send;

    /// Initial state for `slot`.
    fn init(&self, slot: u32, view: &dyn GraphView) -> Self::State;

    /// Advance one active vertex by one superstep.
    fn compute(
        &self,
        slot: u32,
        state: &Self::State,
        inbox: &[Self::Message],
        superstep: usize,
        view: &dyn GraphView,
    ) -> VertexStep<Self::State, Self::Message>;

    /// Optional associative+commutative combiner folding two messages bound for
    /// the same target into one. `None` (default) keeps messages un-combined.
    fn combine(&self, _a: &Self::Message, _b: &Self::Message) -> Option<Self::Message> {
        None
    }

    /// The scalar output value for a vertex's final state (result column).
    fn output(&self, state: &Self::State) -> f64;
}

/// Deliver `msg` into `inbox`, folding via the program's combiner when present.
fn deliver<P: VertexProgram>(program: &P, inbox: &mut Vec<P::Message>, msg: P::Message) {
    // With a combiner an inbox holds at most one (folded) message.
    if let Some(existing) = inbox.pop() {
        match program.combine(&existing, &msg) {
            Some(folded) => inbox.push(folded),
            None => {
                inbox.push(existing);
                inbox.push(msg);
            }
        }
    } else {
        inbox.push(msg);
    }
}

/// Run `program` over `view` for up to `max_supersteps`, returning each vertex's
/// [`VertexProgram::output`] value indexed by slot.
#[must_use]
pub fn run_pregel<P: VertexProgram>(
    program: &P,
    view: &dyn GraphView,
    max_supersteps: usize,
) -> Vec<f64> {
    let n = view.vertex_count();
    let mut states: Vec<P::State> = (0..n as u32).map(|s| program.init(s, view)).collect();
    let mut active = vec![true; n];
    let mut inboxes: Vec<Vec<P::Message>> = (0..n).map(|_| Vec::new()).collect();

    for superstep in 0..max_supersteps {
        let mut next_inboxes: Vec<Vec<P::Message>> = (0..n).map(|_| Vec::new()).collect();
        let mut any_work = false;

        for slot in 0..n {
            if !active[slot] && inboxes[slot].is_empty() {
                continue;
            }
            let step = program.compute(slot as u32, &states[slot], &inboxes[slot], superstep, view);
            states[slot] = step.state;
            for (target, msg) in step.messages {
                deliver(program, &mut next_inboxes[target as usize], msg);
            }
            active[slot] = !step.halt;
        }

        // A vertex with pending messages runs next superstep even if it halted.
        for slot in 0..n {
            if !next_inboxes[slot].is_empty() {
                active[slot] = true;
            }
            if active[slot] {
                any_work = true;
            }
        }

        inboxes = next_inboxes;
        if !any_work {
            break;
        }
    }

    (0..n).map(|s| program.output(&states[s])).collect()
}

/// Two-column `(nodeId INT, value FLOAT)` output schema shared by Pregel
/// providers.
fn pregel_output_schema(value_name: &str) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("nodeId", DataType::Int64, false),
        Field::new(value_name, DataType::Float64, false),
    ]))
}

/// Project a `GraphView` via the host, run `program`, and stream one
/// `(nodeId, value)` row per vertex. Shared by the concrete providers.
fn run_program_to_stream<P: VertexProgram + 'static>(
    host: &dyn AlgorithmHost,
    program: P,
    spec: GraphProjectionSpec,
    max_supersteps: usize,
    out_schema: Arc<Schema>,
) -> Result<SendableRecordBatchStream, FnError> {
    // Obtain the `'static` projection future before the stream so the borrow of
    // `host` does not escape into the returned stream (mirrors reachability).
    let projection = host.project(&spec);
    let schema_for_batch = Arc::clone(&out_schema);

    let stream = futures::stream::once(async move {
        let view = projection
            .await
            .map_err(|e| DataFusionError::Execution(format!("pregel: {e}")))?;
        let values = run_pregel(&program, view.as_ref(), max_supersteps);

        let mut node_ids: Vec<i64> = Vec::with_capacity(values.len());
        #[allow(
            clippy::cast_possible_wrap,
            reason = "vids fit i64 in practice; Cypher integers are i64"
        )]
        for (slot, _) in values.iter().enumerate() {
            node_ids.push(view.to_vid(slot as u32).as_u64() as i64);
        }
        RecordBatch::try_new(
            schema_for_batch,
            vec![
                Arc::new(Int64Array::from(node_ids)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    });

    Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
}

/// Parse the leading projection config shared by Pregel providers from a
/// positional-JSON object at `args[obj_index]`.
fn parse_spec(obj: Option<&serde_json::Map<String, serde_json::Value>>) -> GraphProjectionSpec {
    let mut spec = GraphProjectionSpec::default();
    let Some(cfg) = obj else {
        return spec;
    };
    if let Some(labels) = cfg.get("nodeLabels").and_then(serde_json::Value::as_array) {
        spec.node_labels = labels
            .iter()
            .filter_map(|v| v.as_str().map(str::to_owned))
            .collect();
    }
    if let Some(types) = cfg.get("edgeTypes").and_then(serde_json::Value::as_array) {
        spec.edge_types = types
            .iter()
            .filter_map(|v| v.as_str().map(str::to_owned))
            .collect();
    }
    if let Some(w) = cfg
        .get("weightProperty")
        .and_then(serde_json::Value::as_str)
    {
        spec.weight_property = Some(w.to_owned());
    }
    spec
}

fn parse_args(config_json: &str) -> Result<Vec<serde_json::Value>, FnError> {
    if config_json.is_empty() {
        Ok(Vec::new())
    } else {
        serde_json::from_str(config_json)
            .map_err(|e| FnError::new(0x821, format!("pregel: config_json parse: {e}")))
    }
}

// ── PageRank ────────────────────────────────────────────────────────────────

/// Default damping factor for PageRank (Brin & Page, 1998).
const PAGERANK_DAMPING: f64 = 0.85;
/// Default iteration count when the caller does not specify one.
const PAGERANK_ITERS: usize = 20;

/// Vertex program for damped PageRank with a summing combiner.
struct PageRankProgram {
    damping: f64,
    vertex_count: usize,
}

impl VertexProgram for PageRankProgram {
    type State = f64;
    type Message = f64;

    fn init(&self, _slot: u32, _view: &dyn GraphView) -> f64 {
        1.0 / self.vertex_count as f64
    }

    fn compute(
        &self,
        slot: u32,
        state: &f64,
        inbox: &[f64],
        superstep: usize,
        view: &dyn GraphView,
    ) -> VertexStep<f64, f64> {
        let rank = if superstep == 0 {
            *state
        } else {
            let sum: f64 = inbox.iter().sum();
            (1.0 - self.damping) / self.vertex_count as f64 + self.damping * sum
        };

        let out_degree = view.out_degree(slot);
        let mut messages = Vec::new();
        if out_degree > 0 {
            let contribution = rank / f64::from(out_degree);
            for &nb in view.out_neighbors(slot) {
                messages.push((nb, contribution));
            }
        }
        // Never self-halts: runs the full fixed iteration budget.
        VertexStep {
            state: rank,
            messages,
            halt: false,
        }
    }

    fn combine(&self, a: &f64, b: &f64) -> Option<f64> {
        Some(a + b)
    }

    fn output(&self, state: &f64) -> f64 {
        *state
    }
}

/// First-party `uni.algo.pagerank` provider (Pregel PageRank).
///
/// `CALL uni.algo.pagerank([{nodeLabels, edgeTypes, damping, maxIterations}])`
/// yields `(nodeId INT, rank FLOAT)`.
pub struct PageRankProvider {
    signature: AlgorithmSignature,
}

impl Default for PageRankProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl PageRankProvider {
    /// Construct the provider with its fixed output signature.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: AlgorithmSignature {
                output_fields: vec![
                    Field::new("nodeId", DataType::Int64, false),
                    Field::new("rank", DataType::Float64, false),
                ],
                docs: "uni.algo.pagerank([config]) — damped PageRank over a projection".to_owned(),
            },
        }
    }
}

impl std::fmt::Debug for PageRankProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageRankProvider").finish_non_exhaustive()
    }
}

impl AlgorithmProvider for PageRankProvider {
    fn signature(&self) -> &AlgorithmSignature {
        &self.signature
    }

    fn run(&self, ctx: AlgorithmContext<'_>) -> Result<SendableRecordBatchStream, FnError> {
        let host = ctx
            .host
            .ok_or_else(|| FnError::new(0x820, "pagerank: host unbound"))?;

        let args = parse_args(ctx.config_json)?;
        let cfg = args.first().and_then(serde_json::Value::as_object);
        let spec = parse_spec(cfg);
        let damping = cfg
            .and_then(|c| c.get("damping"))
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(PAGERANK_DAMPING);
        let iters = cfg
            .and_then(|c| c.get("maxIterations"))
            .and_then(serde_json::Value::as_u64)
            .map_or(PAGERANK_ITERS, |v| v as usize);

        // vertex_count is not known until projection; defer program build into
        // the stream by projecting first. We build the program with the view's
        // count inside `run_program_to_stream`? — instead, wrap in a tiny shim:
        // project here to learn n, then run. To keep the borrow rules simple we
        // pass the count via a closure-built program.
        let out_schema = pregel_output_schema("rank");
        let projection = host.project(&spec);
        let schema_for_batch = Arc::clone(&out_schema);
        let stream = futures::stream::once(async move {
            let view = projection
                .await
                .map_err(|e| DataFusionError::Execution(format!("pagerank: {e}")))?;
            let program = PageRankProgram {
                damping,
                vertex_count: view.vertex_count().max(1),
            };
            let values = run_pregel(&program, view.as_ref(), iters);
            let mut node_ids: Vec<i64> = Vec::with_capacity(values.len());
            #[allow(
                clippy::cast_possible_wrap,
                reason = "vids fit i64 in practice; Cypher integers are i64"
            )]
            for (slot, _) in values.iter().enumerate() {
                node_ids.push(view.to_vid(slot as u32).as_u64() as i64);
            }
            RecordBatch::try_new(
                schema_for_batch,
                vec![
                    Arc::new(Int64Array::from(node_ids)),
                    Arc::new(Float64Array::from(values)),
                ],
            )
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }
}

// ── SSSP (single-source shortest path) ───────────────────────────────────────

/// Default superstep budget for SSSP; a shortest path visits at most `V` hops,
/// so a generous cap avoids infinite loops on pathological weights.
const SSSP_MAX_SUPERSTEPS: usize = 10_000;

/// Vertex program for single-source shortest path with a `min` combiner.
///
/// Uses edge weights when the projection surfaces them (`weightProperty`),
/// otherwise treats every edge as weight `1` (BFS distance).
struct SsspProgram {
    source_vid: u64,
}

impl VertexProgram for SsspProgram {
    type State = f64;
    type Message = f64;

    fn init(&self, slot: u32, view: &dyn GraphView) -> f64 {
        if view.to_slot(Vid::new(self.source_vid)) == Some(slot) {
            0.0
        } else {
            f64::INFINITY
        }
    }

    fn compute(
        &self,
        slot: u32,
        state: &f64,
        inbox: &[f64],
        superstep: usize,
        view: &dyn GraphView,
    ) -> VertexStep<f64, f64> {
        let mut dist = *state;
        for &m in inbox {
            if m < dist {
                dist = m;
            }
        }
        // The source relaxes its edges in superstep 0; others only when improved.
        let improved = dist < *state || (superstep == 0 && dist.is_finite());

        let mut messages = Vec::new();
        if improved {
            let has_weights = view.has_weights();
            for (idx, &nb) in view.out_neighbors(slot).iter().enumerate() {
                let w = if has_weights {
                    view.out_weight(slot, idx)
                } else {
                    1.0
                };
                messages.push((nb, dist + w));
            }
        }
        VertexStep {
            state: dist,
            messages,
            halt: !improved,
        }
    }

    fn combine(&self, a: &f64, b: &f64) -> Option<f64> {
        Some(a.min(*b))
    }

    fn output(&self, state: &f64) -> f64 {
        *state
    }
}

/// First-party `uni.algo.sssp` provider (Pregel single-source shortest path).
///
/// `CALL uni.algo.sssp(sourceVid[, {nodeLabels, edgeTypes, weightProperty}])`
/// yields `(nodeId INT, distance FLOAT)`; unreachable vertices report `+inf`.
pub struct SsspProvider {
    signature: AlgorithmSignature,
}

impl Default for SsspProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl SsspProvider {
    /// Construct the provider with its fixed output signature.
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: AlgorithmSignature {
                output_fields: vec![
                    Field::new("nodeId", DataType::Int64, false),
                    Field::new("distance", DataType::Float64, false),
                ],
                docs: "uni.algo.sssp(sourceVid[, config]) — single-source shortest path".to_owned(),
            },
        }
    }
}

impl std::fmt::Debug for SsspProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SsspProvider").finish_non_exhaustive()
    }
}

impl AlgorithmProvider for SsspProvider {
    fn signature(&self) -> &AlgorithmSignature {
        &self.signature
    }

    fn run(&self, ctx: AlgorithmContext<'_>) -> Result<SendableRecordBatchStream, FnError> {
        let host = ctx
            .host
            .ok_or_else(|| FnError::new(0x822, "sssp: host unbound"))?;

        let args = parse_args(ctx.config_json)?;
        let source = args
            .first()
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| {
                FnError::new(0x823, "sssp: first argument must be an integer source vid")
            })?;
        let spec = parse_spec(args.get(1).and_then(serde_json::Value::as_object));

        #[allow(
            clippy::cast_sign_loss,
            reason = "vids are non-negative; a negative arg fails to resolve to a slot"
        )]
        let program = SsspProgram {
            source_vid: source as u64,
        };
        run_program_to_stream(
            host,
            program,
            spec,
            SSSP_MAX_SUPERSTEPS,
            pregel_output_schema("distance"),
        )
    }
}
