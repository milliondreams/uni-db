// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! First-party `uni.algo.gcwalks` — node2vec/DeepWalk walks via GraphCompute.
//!
//! This provider dogfoods the stochastic `random_walks` kernel and the
//! `emit_walks` egress path (proposal §4.6). Unlike `uni.algo.gcpagerank`, whose
//! result is a per-vertex `[V]` map, a walk corpus is a ragged, multi-row table,
//! so the result path here emits `(walk_id, step, nodeId)` rows directly rather
//! than the host-prepended `nodeId` + `[V]` scores shape. This is the surface a
//! guest DeepWalk/node2vec plugin drives to obtain the actual walk *sequences*
//! (the skip-gram basis), which the lossy `walk_visit_counts` fold cannot express.
//!
//! # CALL shape
//! `CALL uni.algo.gcwalks(<sourceVids>[, <walkLength>[, <walksPerNode>[, <p>[,
//! <q>[, <seed>[, {nodeLabels, edgeTypes}]]]]]])` yielding
//! `(walk_id INT, step INT, nodeId INT)`. An empty `sourceVids` array walks from
//! every vertex.
//
// Rust guideline compliant

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::scalar::ScalarValue;
use uni_common::core::id::Vid;
use uni_plugin::FnError;
use uni_plugin::traits::algorithm::{
    AlgorithmContext, AlgorithmProvider, AlgorithmSignature, GraphProjectionSpec,
};
use uni_plugin::traits::procedure::NamedArgType;
use uni_plugin::traits::scalar::ArgType;

use super::graph_compute_slice_req;
use super::session::{AlgoSession, GraphCompute};
use super::{Arena, WorkBudget};
use crate::algorithms::bridge::AlgorithmHostBridge;

/// Default walk length when the CALL omits it.
const DEFAULT_WALK_LENGTH: usize = 10;
/// Default number of walks sampled per start vertex.
const DEFAULT_WALKS_PER_NODE: usize = 1;
/// Default node2vec return/in-out bias (unbiased random walk).
const DEFAULT_BIAS: f64 = 1.0;

/// The declared positional arguments of `uni.algo.gcwalks` (proposal §4.6).
///
/// `sourceVids` is an opaque [`ArgType::CypherValue`] because it accepts either a
/// single vid or an array (empty = walk from every vertex); the remaining
/// numeric arguments are typed and defaulted so the host fills omissions.
fn gcwalks_args() -> Vec<NamedArgType> {
    #[expect(
        clippy::cast_possible_wrap,
        reason = "small literal defaults fit i64 exactly"
    )]
    let defaults = [DEFAULT_WALK_LENGTH as i64, DEFAULT_WALKS_PER_NODE as i64];
    vec![
        NamedArgType {
            name: "sourceVids".into(),
            ty: ArgType::CypherValue,
            default: None,
            doc: "Start vertex, array of start vertices, or [] for every vertex.".to_owned(),
        },
        NamedArgType {
            name: "walkLength".into(),
            ty: ArgType::Primitive(arrow_schema::DataType::Int64),
            default: Some(ScalarValue::Int64(Some(defaults[0]))),
            doc: "Steps per walk; defaults to 10.".to_owned(),
        },
        NamedArgType {
            name: "walksPerNode".into(),
            ty: ArgType::Primitive(arrow_schema::DataType::Int64),
            default: Some(ScalarValue::Int64(Some(defaults[1]))),
            doc: "Walks sampled per start vertex; defaults to 1.".to_owned(),
        },
        NamedArgType {
            name: "p".into(),
            ty: ArgType::Primitive(arrow_schema::DataType::Float64),
            default: Some(ScalarValue::Float64(Some(DEFAULT_BIAS))),
            doc: "node2vec return bias; defaults to 1.0 (unbiased).".to_owned(),
        },
        NamedArgType {
            name: "q".into(),
            ty: ArgType::Primitive(arrow_schema::DataType::Float64),
            default: Some(ScalarValue::Float64(Some(DEFAULT_BIAS))),
            doc: "node2vec in-out bias; defaults to 1.0 (unbiased).".to_owned(),
        },
        NamedArgType {
            name: "seed".into(),
            ty: ArgType::Primitive(arrow_schema::DataType::Int64),
            default: Some(ScalarValue::Int64(Some(0))),
            doc: "Deterministic RNG seed; defaults to 0.".to_owned(),
        },
        NamedArgType {
            name: "config".into(),
            ty: ArgType::CypherValue,
            default: Some(ScalarValue::Null),
            doc: "Optional {nodeLabels, edgeTypes} projection filter.".to_owned(),
        },
    ]
}

/// Random-walk generation provider authored on the GraphCompute catalog.
///
/// See the [module docs](self) for the CALL shape and the egress rationale.
pub struct GraphComputeWalksProvider {
    signature: AlgorithmSignature,
}

impl std::fmt::Debug for GraphComputeWalksProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphComputeWalksProvider")
            .finish_non_exhaustive()
    }
}

impl Default for GraphComputeWalksProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphComputeWalksProvider {
    /// Constructs the provider with its fixed `(walk_id, step, nodeId)` signature.
    #[must_use]
    pub fn new() -> Self {
        let output_fields = vec![
            Field::new("walk_id", DataType::Int64, false),
            Field::new("step", DataType::Int64, false),
            Field::new("nodeId", DataType::Int64, false),
        ];
        Self {
            signature: AlgorithmSignature {
                output_fields,
                docs: "uni.algo.gcwalks(sourceVids[, walkLength[, walksPerNode[, p[, q[, \
                       seed[, config]]]]]]) — node2vec/DeepWalk random walks driven through \
                       the GraphCompute kernel catalog, yielding walk sequences"
                    .to_owned(),
                args: gcwalks_args(),
                slices: vec![graph_compute_slice_req()],
            },
        }
    }
}

/// The parsed positional arguments of a `gcwalks` CALL.
struct WalksArgs {
    seeds: Vec<Vid>,
    walk_length: usize,
    walks_per_node: usize,
    p: f64,
    q: f64,
    seed: u64,
    spec: GraphProjectionSpec,
}

/// Parses the positional `config_json` array into [`WalksArgs`].
fn parse_config(config_json: &str) -> Result<WalksArgs, FnError> {
    let args: Vec<serde_json::Value> = serde_json::from_str(config_json)
        .map_err(|e| FnError::new(0x802, format!("gcwalks: bad config json: {e}")))?;

    // arg0: a single source vid, an array of source vids, or an empty array
    // (walk from every vertex).
    let seeds = match args.first() {
        Some(serde_json::Value::Number(n)) => {
            let v = n
                .as_u64()
                .ok_or_else(|| FnError::new(0x802, "gcwalks: sourceVid must be a u64"))?;
            vec![Vid::new(v)]
        }
        Some(serde_json::Value::Array(arr)) => arr
            .iter()
            .map(|v| {
                v.as_u64()
                    .map(Vid::new)
                    .ok_or_else(|| FnError::new(0x802, "gcwalks: seed must be a u64"))
            })
            .collect::<Result<Vec<_>, _>>()?,
        _ => return Err(FnError::new(0x802, "gcwalks: missing sourceVids arg")),
    };

    let usize_arg = |i: usize, default: usize| -> usize {
        match args.get(i) {
            Some(serde_json::Value::Number(n)) => n.as_u64().map_or(default, |v| v as usize),
            _ => default,
        }
    };
    let f64_arg = |i: usize, default: f64| -> f64 {
        match args.get(i) {
            Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(default),
            _ => default,
        }
    };

    let walk_length = usize_arg(1, DEFAULT_WALK_LENGTH).max(1);
    let walks_per_node = usize_arg(2, DEFAULT_WALKS_PER_NODE).max(1);
    let p = f64_arg(3, DEFAULT_BIAS);
    let q = f64_arg(4, DEFAULT_BIAS);
    let seed = match args.get(5) {
        Some(serde_json::Value::Number(n)) => n.as_u64().unwrap_or(0),
        _ => 0,
    };

    let mut spec = GraphProjectionSpec::default();
    if let Some(serde_json::Value::Object(cfg)) = args.get(6) {
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
    }
    Ok(WalksArgs {
        seeds,
        walk_length,
        walks_per_node,
        p,
        q,
        seed,
        spec,
    })
}

impl AlgorithmProvider for GraphComputeWalksProvider {
    fn signature(&self) -> &AlgorithmSignature {
        &self.signature
    }

    fn run(&self, ctx: AlgorithmContext<'_>) -> Result<SendableRecordBatchStream, FnError> {
        let host = ctx
            .host
            .ok_or_else(|| FnError::new(0x800, "gcwalks: host unbound"))?;
        let bridge = host
            .as_any()
            .downcast_ref::<AlgorithmHostBridge>()
            .ok_or_else(|| FnError::new(0x801, "gcwalks: host is not the algorithm bridge"))?;

        let args = parse_config(ctx.config_json)?;
        // Build the 'static projection future and read caps BEFORE the stream so
        // no borrow of `ctx.host` escapes this synchronous `run`.
        let projection = bridge.project_for_graph_compute(&args.spec);
        let (work_cap, arena_bytes) = bridge.graph_compute_caps();
        let deadline_ms = bridge.graph_compute_deadline_ms();

        let out_schema = Arc::new(Schema::new(self.signature.output_fields.clone()));
        let schema_for_batch = Arc::clone(&out_schema);
        let stream = futures::stream::once(async move {
            let graph = projection
                .await
                .map_err(|e| DataFusionError::Execution(format!("gcwalks: {e}")))?;

            // Install the native-work budget: min(declared cap, size-multiple).
            let size_budget =
                WorkBudget::from_graph_size(graph.vertex_count() as u64, graph.edge_count() as u64)
                    .total();
            let total = work_cap.map_or(size_budget, |w| w.min(size_budget));
            let budget = WorkBudget::new(total.max(1));
            let arena = Arena::new(arena_bytes, super::DEFAULT_ARENA_MAX_HANDLES);
            let deadline_at = deadline_ms
                .map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));
            // Walks emit their own `nodeId` data column, so — unlike gcpagerank —
            // the session installs NO expected-columns contract (the host does
            // not prepend a `nodeId` here).
            let mut session = AlgoSession::new(super::next_session_epoch(), budget, arena)
                .with_deadline(deadline_at);
            let g = session.bind_graph(Arc::clone(&graph));

            let started = std::time::Instant::now();
            let map_incomplete = |e: FnError, session: &AlgoSession| {
                super::error::incomplete_tag_for(
                    &e,
                    "uni.algo.gcwalks",
                    started.elapsed().as_millis() as u64,
                    0,
                    session.work_spent(),
                    session.work_budget(),
                )
                .map_or_else(
                    || DataFusionError::Execution(format!("gcwalks: {e}")),
                    DataFusionError::Execution,
                )
            };
            let walks = session
                .random_walks(
                    g,
                    args.walk_length,
                    args.walks_per_node,
                    &args.seeds,
                    args.p,
                    args.q,
                    args.seed,
                )
                .map_err(|e| map_incomplete(e, &session))?;
            session
                .emit_walks(walks)
                .map_err(|e| map_incomplete(e, &session))?;
            let rows = session.take_emitted_walks();

            let mut walk_id = Vec::with_capacity(rows.len());
            let mut step = Vec::with_capacity(rows.len());
            let mut node_id = Vec::with_capacity(rows.len());
            for (w, s, n) in rows {
                walk_id.push(w);
                step.push(s);
                node_id.push(n);
            }
            RecordBatch::try_new(
                schema_for_batch,
                vec![
                    Arc::new(Int64Array::from(walk_id)),
                    Arc::new(Int64Array::from(step)),
                    Arc::new(Int64Array::from(node_id)),
                ],
            )
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }
}
