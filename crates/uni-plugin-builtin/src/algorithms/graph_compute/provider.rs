// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! First-party `uni.algo.gcpagerank` — Personalized PageRank via GraphCompute.
//!
//! This provider dogfoods the GraphCompute kernel catalog end to end through the
//! `CALL` dispatch path (proposal §8 Phase 1 exit / §9.4 E-1). Unlike the pregel
//! `uni.algo.pagerank`, it never touches the CSR directly: it projects a graph
//! into an [`AlgoSession`], then drives [`personalized_pagerank`] purely through
//! the [`GraphCompute`] trait — exactly the surface
//! a guest plugin will drive once the loader shims land. The native-work budget
//! and arena cap are installed from the invocation's capabilities on the CALL
//! path (proposal §5.1 / §12), so a runaway kernel loop fails closed here just as
//! it will for a guest.
//!
//! # CALL shape
//! `CALL uni.algo.gcpagerank(<sourceVid>[, <alpha>[, {nodeLabels, edgeTypes}]])`
//! yielding `(nodeId INT, score FLOAT)`.
//
// Rust guideline compliant

use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch};
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

use super::first_party::personalized_pagerank;
use super::graph_compute_slice_req;
use super::session::{AlgoSession, GraphCompute};
use super::{Arena, WorkBudget};
use crate::algorithms::bridge::AlgorithmHostBridge;

/// Default damping factor when the CALL omits `alpha`.
const DEFAULT_ALPHA: f64 = 0.85;

/// The declared positional arguments of `uni.algo.gcpagerank` (proposal §4.6).
///
/// Enables host-side arity/type validation before the provider runs: a bad
/// `sourceVid` or `alpha` type is rejected with a typed error up front, and an
/// omitted `alpha` is filled from its default. `config` is an opaque
/// [`ArgType::CypherValue`] map, per the existing untyped-config convention.
fn gcpagerank_args() -> Vec<NamedArgType> {
    vec![
        NamedArgType {
            // Opaque because it accepts a single vid *or* an array of vids.
            name: "sourceVid".into(),
            ty: ArgType::CypherValue,
            default: None,
            doc: "The personalization seed vertex (or an array of seed vertices).".to_owned(),
        },
        NamedArgType {
            name: "alpha".into(),
            ty: ArgType::Primitive(arrow_schema::DataType::Float64),
            default: Some(ScalarValue::Float64(Some(DEFAULT_ALPHA))),
            doc: "Damping factor in [0, 1); defaults to 0.85.".to_owned(),
        },
        NamedArgType {
            name: "config".into(),
            ty: ArgType::CypherValue,
            default: Some(ScalarValue::Null),
            doc: "Optional {nodeLabels, edgeTypes} projection filter.".to_owned(),
        },
    ]
}
/// Default power-iteration cap.
const DEFAULT_ITERS: usize = 100;
/// Default convergence tolerance (L1).
const DEFAULT_TOL: f64 = 1e-9;

/// Personalized PageRank provider authored on the GraphCompute catalog.
///
/// See the [module docs](self) for the CALL shape and the dogfood rationale.
pub struct GraphComputePageRankProvider {
    signature: AlgorithmSignature,
}

impl std::fmt::Debug for GraphComputePageRankProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphComputePageRankProvider")
            .finish_non_exhaustive()
    }
}

impl Default for GraphComputePageRankProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphComputePageRankProvider {
    /// Constructs the provider with its fixed `(nodeId, score)` signature.
    #[must_use]
    pub fn new() -> Self {
        let output_fields = vec![
            Field::new("nodeId", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
        ];
        Self {
            signature: AlgorithmSignature {
                output_fields,
                docs: "uni.algo.gcpagerank(sourceVid[, alpha[, config]]) — Personalized \
                       PageRank driven through the GraphCompute kernel catalog"
                    .to_owned(),
                args: gcpagerank_args(),
                slices: vec![graph_compute_slice_req()],
                // First-party GraphCompute providers compose as DF plan nodes.
                df_composable: true,
            },
        }
    }
}

/// Parses the positional `config_json` array into `(seeds, alpha, spec)`.
fn parse_config(config_json: &str) -> Result<(Vec<Vid>, f64, GraphProjectionSpec), FnError> {
    let args: Vec<serde_json::Value> = serde_json::from_str(config_json)
        .map_err(|e| FnError::new(0x802, format!("gcpagerank: bad config json: {e}")))?;

    // arg0: a single source vid or an array of source vids.
    let seeds = match args.first() {
        Some(serde_json::Value::Number(n)) => {
            let v = n
                .as_u64()
                .ok_or_else(|| FnError::new(0x802, "gcpagerank: sourceVid must be a u64"))?;
            vec![Vid::new(v)]
        }
        Some(serde_json::Value::Array(arr)) => arr
            .iter()
            .map(|v| {
                v.as_u64()
                    .map(Vid::new)
                    .ok_or_else(|| FnError::new(0x802, "gcpagerank: seed must be a u64"))
            })
            .collect::<Result<Vec<_>, _>>()?,
        _ => return Err(FnError::new(0x802, "gcpagerank: missing sourceVid arg")),
    };

    let alpha = match args.get(1) {
        Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(DEFAULT_ALPHA),
        _ => DEFAULT_ALPHA,
    };

    let mut spec = GraphProjectionSpec::default();
    if let Some(serde_json::Value::Object(cfg)) = args.get(2) {
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
    Ok((seeds, alpha, spec))
}

impl AlgorithmProvider for GraphComputePageRankProvider {
    fn signature(&self) -> &AlgorithmSignature {
        &self.signature
    }

    fn run(&self, ctx: AlgorithmContext<'_>) -> Result<SendableRecordBatchStream, FnError> {
        let host = ctx
            .host
            .ok_or_else(|| FnError::new(0x800, "gcpagerank: host unbound"))?;
        // Downcast to the concrete bridge to reach the GraphCompute projection +
        // capability-derived budget (the AlgoSession is constructed on the bridge,
        // proposal §4.1).
        let bridge = host
            .as_any()
            .downcast_ref::<AlgorithmHostBridge>()
            .ok_or_else(|| FnError::new(0x801, "gcpagerank: host is not the algorithm bridge"))?;

        let (seeds, alpha, spec) = parse_config(ctx.config_json)?;
        // Build the 'static projection future and read the caps BEFORE the
        // stream, so no borrow of `ctx.host` escapes this synchronous `run`.
        let projection = bridge.project_for_graph_compute(&spec);
        let (work_cap, arena_bytes) = bridge.graph_compute_caps();
        let deadline_ms = bridge.graph_compute_deadline_ms();

        let out_schema = Arc::new(Schema::new(self.signature.output_fields.clone()));
        let schema_for_batch = Arc::clone(&out_schema);
        let expected_cols = super::guest_emit_columns(&self.signature.output_fields);
        let stream = futures::stream::once(async move {
            let graph = projection
                .await
                .map_err(|e| DataFusionError::Execution(format!("gcpagerank: {e}")))?;

            // Install the native-work budget: an explicit grant is authoritative
            // and may raise the ceiling; otherwise the size-derived default (§9).
            let budget = WorkBudget::resolve(
                work_cap,
                graph.vertex_count() as u64,
                graph.edge_count() as u64,
            );
            let arena = Arena::new(arena_bytes, super::DEFAULT_ARENA_MAX_HANDLES);
            // Wall-clock deadline (proposal §5.2): the guest/native loop aborts
            // with Timeout (0x867) once this instant passes, checked in `charge`.
            let deadline_at = deadline_ms
                .map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));
            let mut session = AlgoSession::new(super::next_session_epoch(), budget, arena)
                .with_deadline(deadline_at)
                .with_expected_columns(expected_cols);
            let g = session.bind_graph(Arc::clone(&graph));

            // Flagship returns the last iterate (allow_partial = true), matching
            // the fixed-loop guest scripts; the IterationLimit error path is
            // exercised by a first-party unit test with allow_partial = false.
            // A mid-run budget/deadline abort (0x865/0x867) still surfaces as a
            // typed incomplete outcome (§5.2), not a plain execution error.
            let started = std::time::Instant::now();
            let rank = personalized_pagerank(
                &mut session,
                g,
                &seeds,
                alpha,
                DEFAULT_ITERS,
                DEFAULT_TOL,
                true,
            )
            .map_err(|e| {
                super::error::incomplete_tag_for(
                    &e,
                    "uni.algo.gcpagerank",
                    started.elapsed().as_millis() as u64,
                    0,
                    session.work_spent(),
                    session.work_budget(),
                )
                .map_or_else(
                    || DataFusionError::Execution(format!("gcpagerank: {e}")),
                    DataFusionError::Execution,
                )
            })?;
            session
                .emit(&[("score", rank)])
                .map_err(|e| DataFusionError::Execution(format!("gcpagerank emit: {e}")))?;
            let emitted = session.take_emitted();
            let scores = emitted
                .into_iter()
                .next()
                .map(|(_, v)| v)
                .unwrap_or_default();

            // Host prepends nodeId via slot→Vid reverse translation (§4.3 grp 8).
            #[expect(
                clippy::cast_possible_wrap,
                reason = "vids fit i64 in practice; Cypher integers are i64"
            )]
            let node_ids: Vec<i64> = (0..scores.len() as u32)
                .map(|slot| graph.to_vid(slot).as_u64() as i64)
                .collect();
            RecordBatch::try_new(
                schema_for_batch,
                vec![
                    Arc::new(Int64Array::from(node_ids)),
                    Arc::new(Float64Array::from(scores)),
                ],
            )
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }
}
