// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Algorithm adapter — a Component Model guest driving GraphCompute kernels.
//!
//! Mirrors the Extism algorithm adapter: the host projects the graph (gated on
//! `GraphCompute` + `HostQuery`), opens a session in the shared registry, invokes
//! the guest's `invoke-algorithm` export with a JSON `{session, graph, args}`
//! blob, then closes the session and assembles the declared `(nodeId, …)` batch.
//! The guest drives the coarse kernels through the imported `host-graph`
//! interface — only handles + scalars cross (proposal §4.5/§4.6).
//
// Rust guideline compliant

use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Schema, SchemaRef};
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use uni_plugin::QName;
use uni_plugin::errors::FnError;
use uni_plugin::traits::algorithm::{
    AlgorithmContext, AlgorithmProvider, AlgorithmSignature, GraphProjectionSpec,
};
use uni_plugin_builtin::algorithms::bridge::AlgorithmHostBridge;
use uni_plugin_builtin::algorithms::graph_compute::handle::Handle;
use uni_plugin_builtin::algorithms::graph_compute::{
    AlgoSession, Arena, DEFAULT_ARENA_MAX_HANDLES, SharedRegistry, WorkBudget, next_session_epoch,
};

use crate::adapter_common::acquire;
use crate::loader::AlgorithmPluginInstance;
use crate::pool::WasmInstancePool;

/// `AlgorithmProvider` adapter wrapping a Component Model pool + session registry.
pub struct ComponentAlgorithm {
    pool: Arc<WasmInstancePool<AlgorithmPluginInstance>>,
    registry: SharedRegistry,
    qname: QName,
    signature: AlgorithmSignature,
}

impl std::fmt::Debug for ComponentAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComponentAlgorithm")
            .field("qname", &self.qname)
            .finish_non_exhaustive()
    }
}

impl ComponentAlgorithm {
    /// Constructs an algorithm adapter against `pool` and the shared `registry`.
    #[must_use]
    pub fn new(
        pool: Arc<WasmInstancePool<AlgorithmPluginInstance>>,
        registry: SharedRegistry,
        qname: QName,
        signature: AlgorithmSignature,
    ) -> Self {
        Self {
            pool,
            registry,
            qname,
            signature,
        }
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

impl AlgorithmProvider for ComponentAlgorithm {
    fn signature(&self) -> &AlgorithmSignature {
        &self.signature
    }

    fn run(&self, ctx: AlgorithmContext<'_>) -> Result<SendableRecordBatchStream, FnError> {
        let host = ctx
            .host
            .ok_or_else(|| FnError::new(0x800, "wasm algorithm: host unbound"))?;
        let bridge = host
            .as_any()
            .downcast_ref::<AlgorithmHostBridge>()
            .ok_or_else(|| FnError::new(0x801, "wasm algorithm: host is not the bridge"))?;

        let json_args: Vec<serde_json::Value> = serde_json::from_str(ctx.config_json)
            .map_err(|e| FnError::new(0x802, format!("wasm algorithm: bad config json: {e}")))?;

        let spec = GraphProjectionSpec {
            include_reverse: true, // enable In-direction kernels (WCC/k-core/HITS)
            ..GraphProjectionSpec::default()
        };
        let projection = bridge.project_for_graph_compute(&spec);
        let (work_cap, arena_bytes) = bridge.graph_compute_caps();

        let out_schema: SchemaRef = Arc::new(Schema::new(self.signature.output_fields.clone()));
        let schema_for_batch = Arc::clone(&out_schema);
        let registry = Arc::clone(&self.registry);
        let pool = Arc::clone(&self.pool);
        let qname_str = self.qname.to_string();

        let stream = futures::stream::once(async move {
            let graph = projection
                .await
                .map_err(|e| DataFusionError::Execution(format!("wasm algorithm: {e}")))?;

            let size_budget =
                WorkBudget::from_graph_size(graph.vertex_count() as u64, graph.edge_count() as u64)
                    .total();
            let total = work_cap.map_or(size_budget, |w| w.min(size_budget));
            let mut session = AlgoSession::new(
                next_session_epoch(),
                WorkBudget::new(total.max(1)),
                Arena::new(arena_bytes, DEFAULT_ARENA_MAX_HANDLES),
            );
            let g = to_i64(session.bind_graph(Arc::clone(&graph)));
            let sid = registry.open(session);

            // Wrap the fallible steps so the session is ALWAYS closed after open
            // (no leak of the projected graph), even if input-build/acquire fails.
            let started = std::time::Instant::now();
            let call_result: Result<(), DataFusionError> = (|| {
                let input = serde_json::to_vec(&serde_json::json!({
                    "session": sid, "graph": g, "args": json_args,
                }))
                .map_err(|e| DataFusionError::Execution(format!("wasm algorithm input: {e}")))?;
                let mut leased = acquire(&pool, "algorithm")
                    .map_err(|e| DataFusionError::Execution(format!("wasm acquire: {e}")))?;
                leased
                    .get_mut()
                    .invoke_algorithm(&qname_str, &input)
                    .map(|_| ())
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "wasm invoke_algorithm `{qname_str}`: {e}"
                        ))
                    })
            })();

            let closed = registry.close(sid);
            if let Err(orig) = call_result {
                // Classify from the closed session: a drained budget is a typed
                // Exhausted outcome (§5.2). This adapter sets no host wall-clock
                // deadline, so a Timeout is never inferred. Other faults verbatim.
                let (spent, budget) = closed
                    .as_ref()
                    .map_or((0, 0), |s| (s.work_spent(), s.work_budget()));
                return Err(
                    uni_plugin_builtin::algorithms::graph_compute::error::incomplete_tag_after_guest(
                        &qname_str,
                        false,
                        spent,
                        budget,
                        started.elapsed().as_millis() as u64,
                    )
                    .map_or(orig, DataFusionError::Execution),
                );
            }
            let mut closed =
                closed.ok_or_else(|| DataFusionError::Execution("session vanished".into()))?;
            let emitted = closed.take_emitted();

            build_batch(&schema_for_batch, &graph, &emitted)
                .map_err(|e| DataFusionError::Execution(format!("wasm algorithm emit: {e}")))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }
}

/// Assembles the output batch from emitted columns + a `nodeId` column.
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
                    format!("unsupported emit column type {other:?}"),
                ));
            }
        }
    }
    RecordBatch::try_new(Arc::clone(schema), columns)
        .map_err(|e| FnError::new(0x15, format!("wasm algorithm batch: {e}")))
}
