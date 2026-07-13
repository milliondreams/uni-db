// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Algorithm adapter — a Rhai guest algorithm driving the GraphCompute kernels.
//!
//! A Rhai algorithm exports `fn ${name}(gc, ...args)` where `gc` is an injected
//! [`GcSession`](crate::graph_compute::GcSession). The guest drives the coarse
//! kernels through `gc` and calls `gc.emit(name, handle)` to publish its
//! per-vertex result; the adapter then prepends a `nodeId` column (slot→Vid) and
//! assembles the declared `(nodeId, ...)` output batch. This is the guest
//! authoring path of the GraphCompute proposal (§4.6): no vertex data crosses
//! into the interpreter, only opaque handles.
//!
//! The projection is capability-gated on both `GraphCompute` and `HostQuery`
//! (proposal §4.6), and the per-invocation budget / arena caps are installed
//! from the invocation's capabilities, so a runaway guest loop fails closed.
//!
//! v1 projects the whole graph (all labels / all edge types); a per-algorithm
//! projection spec is a follow-up.
//
// Rust guideline compliant

#![cfg(feature = "rhai-runtime")]

use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Schema, SchemaRef};
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use rhai::{Dynamic, Scope};
use smol_str::SmolStr;

use uni_plugin::errors::FnError;
use uni_plugin::traits::algorithm::{
    AlgorithmContext, AlgorithmProvider, AlgorithmSignature, GraphProjectionSpec,
};
use uni_plugin_builtin::algorithms::bridge::AlgorithmHostBridge;
use uni_plugin_builtin::algorithms::graph_compute::{
    AlgoSession, Arena, DEFAULT_ARENA_MAX_HANDLES, WorkBudget, next_session_epoch,
};

use crate::graph_compute::new_session;
use crate::runtime::RhaiPluginRuntime;

/// Per-algorithm Rhai callable adapter driving the GraphCompute kernels.
#[derive(Debug)]
pub struct RhaiAlgorithm {
    runtime: Arc<RhaiPluginRuntime>,
    name: SmolStr,
    signature: AlgorithmSignature,
}

impl RhaiAlgorithm {
    /// Constructs an algorithm adapter binding `name` against the shared runtime.
    #[must_use]
    pub fn new(
        runtime: Arc<RhaiPluginRuntime>,
        name: impl Into<SmolStr>,
        signature: AlgorithmSignature,
    ) -> Self {
        Self {
            runtime,
            name: name.into(),
            signature,
        }
    }
}

impl AlgorithmProvider for RhaiAlgorithm {
    fn signature(&self) -> &AlgorithmSignature {
        &self.signature
    }

    fn run(&self, ctx: AlgorithmContext<'_>) -> Result<SendableRecordBatchStream, FnError> {
        let host = ctx
            .host
            .ok_or_else(|| FnError::new(0x800, "rhai algorithm: host unbound"))?;
        let bridge = host
            .as_any()
            .downcast_ref::<AlgorithmHostBridge>()
            .ok_or_else(|| {
                FnError::new(0x801, "rhai algorithm: host is not the algorithm bridge")
            })?;

        // Convert the positional-JSON CALL args to Rhai Dynamic values; they are
        // passed to the guest after the injected `GcSession`.
        let json_args: Vec<serde_json::Value> = serde_json::from_str(ctx.config_json)
            .map_err(|e| FnError::new(0x802, format!("rhai algorithm: bad config json: {e}")))?;
        let guest_args: Vec<Dynamic> = json_args
            .into_iter()
            .map(|v| {
                rhai::serde::to_dynamic(v)
                    .map_err(|e| FnError::new(0x802, format!("rhai algorithm arg: {e}")))
            })
            .collect::<Result<_, _>>()?;

        // v1: project the whole graph. Build the 'static projection future and
        // read the caps BEFORE the stream so no borrow of `ctx.host` escapes.
        let spec = GraphProjectionSpec {
            include_reverse: true, // enable In-direction kernels (WCC/k-core/HITS)
            ..GraphProjectionSpec::default()
        };
        let projection = bridge.project_for_graph_compute(&spec);
        let (work_cap, arena_bytes) = bridge.graph_compute_caps();

        let out_schema: SchemaRef = Arc::new(Schema::new(self.signature.output_fields.clone()));
        let runtime = Arc::clone(&self.runtime);
        let name = self.name.clone();
        let schema_for_batch = Arc::clone(&out_schema);
        let expected_cols = uni_plugin_builtin::algorithms::graph_compute::guest_emit_columns(
            &self.signature.output_fields,
        );

        let stream = futures::stream::once(async move {
            let graph = projection
                .await
                .map_err(|e| DataFusionError::Execution(format!("rhai algorithm: {e}")))?;

            // An explicit grant is authoritative and may raise the ceiling;
            // otherwise the size-derived default (proposal §9).
            let budget = WorkBudget::resolve(
                work_cap,
                graph.vertex_count() as u64,
                graph.edge_count() as u64,
            );
            let session = Arc::new(parking_lot::Mutex::new(
                AlgoSession::new(
                    next_session_epoch(),
                    budget,
                    Arena::new(arena_bytes, DEFAULT_ARENA_MAX_HANDLES),
                )
                .with_expected_columns(expected_cols),
            ));
            let g = session.lock().bind_graph(Arc::clone(&graph));
            let gc = new_session(Arc::clone(&session), g);

            // Drive the guest: `fn name(gc, ...args)`. All O(V+E) work happens in
            // the native kernels the guest calls; the interpreter only loops.
            // Wrap the whole invocation in catch_unwind (proposal §5.4): Rhai
            // does not isolate a panic in a registered host fn, so a defensive
            // kernel panic would otherwise unwind past the engine and crash the
            // query worker. Isolate it into an aborted invocation instead.
            let mut scope = Scope::new();
            let mut call_args: Vec<Dynamic> = Vec::with_capacity(guest_args.len() + 1);
            call_args.push(Dynamic::from(gc));
            call_args.extend(guest_args);
            let engine = Arc::clone(&runtime.engine);
            let ast = Arc::clone(&runtime.ast);
            let fn_name = name.clone();
            let started = std::time::Instant::now();
            let call = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                engine.call_fn::<Dynamic>(&mut scope, &ast, fn_name.as_str(), call_args)
            }));
            match call {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    // A drained native-work budget is a typed Exhausted outcome
                    // (§5.2); a Rhai guest has no host wall-clock deadline, so a
                    // Timeout is never inferred here. Other faults report verbatim.
                    let (spent, budget) = {
                        let s = session.lock();
                        (s.work_spent(), s.work_budget())
                    };
                    return Err(
                        uni_plugin_builtin::algorithms::graph_compute::error::incomplete_tag_after_guest(
                            name.as_str(),
                            false,
                            spent,
                            budget,
                            started.elapsed().as_millis() as u64,
                        )
                        .map_or_else(
                            || DataFusionError::Execution(format!("rhai algorithm `{name}`: {e}")),
                            DataFusionError::Execution,
                        ),
                    );
                }
                Err(_) => {
                    return Err(DataFusionError::Execution(format!(
                        "rhai algorithm `{name}`: kernel panicked (isolated)"
                    )));
                }
            }

            // Read the guest's emitted columns and assemble the output batch.
            let emitted = session.lock().take_emitted();
            build_batch(&schema_for_batch, &graph, &emitted)
                .map_err(|e| DataFusionError::Execution(format!("rhai algorithm emit: {e}")))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }
}

/// Assembles the declared output batch from emitted columns + a `nodeId` column.
///
/// The `nodeId` field (if declared) is filled from the projection's slot→Vid
/// map; every other declared field is matched by name against an emitted column.
fn build_batch(
    schema: &SchemaRef,
    graph: &uni_algo::algo::GraphProjection,
    emitted: &[(String, Vec<f64>)],
) -> Result<RecordBatch, FnError> {
    let n = graph.vertex_count();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        if field.name() == "nodeId" {
            #[expect(
                clippy::cast_possible_wrap,
                reason = "vids fit i64 in practice; Cypher integers are i64"
            )]
            let ids: Vec<i64> = (0..n as u32)
                .map(|slot| graph.to_vid(slot).as_u64() as i64)
                .collect();
            columns.push(Arc::new(Int64Array::from(ids)));
            continue;
        }
        let (_, values) = emitted
            .iter()
            .find(|(name, _)| name == field.name())
            .ok_or_else(|| {
                FnError::new(
                    0x869,
                    format!("guest did not emit declared column `{}`", field.name()),
                )
            })?;
        match field.data_type() {
            DataType::Float64 => columns.push(Arc::new(Float64Array::from(values.clone()))),
            #[expect(
                clippy::cast_possible_truncation,
                reason = "int columns hold whole f64 values"
            )]
            DataType::Int64 => {
                let ints: Vec<i64> = values.iter().map(|&v| v as i64).collect();
                columns.push(Arc::new(Int64Array::from(ints)));
            }
            other => {
                return Err(FnError::new(
                    0x862,
                    format!(
                        "unsupported emit column type {other:?} for `{}`",
                        field.name()
                    ),
                ));
            }
        }
    }
    RecordBatch::try_new(Arc::clone(schema), columns)
        .map_err(|e| FnError::new(0x15, format!("rhai algorithm batch: {e}")))
}
