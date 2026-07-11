// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Algorithm adapter — an Extism guest algorithm driving GraphCompute kernels.
//!
//! ## Wire contract (per qname `q`)
//! - `algo_<q>_invoke` — input is a JSON object `{session, graph, args}`. The
//!   guest drives the coarse kernels via the imported `uni_graph_call` host fn
//!   (referencing `session` on every call) and publishes its per-vertex result
//!   with `emit`. The output bytes are ignored; the host reads the emitted
//!   columns from the session registry (proposal §4.6).
//!
//! The host projects the graph (capability-gated on `GraphCompute` + `HostQuery`
//! via the bridge), opens a session in the shared registry, invokes the guest,
//! then closes the session and assembles the declared `(nodeId, …)` batch.
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

use crate::adapter_common::{acquire, sanitize_qname};
use crate::pool::ExtismInstancePool;

/// Plugin-side algorithm-invoke export name from a qname.
#[must_use]
pub(crate) fn algo_invoke_export_name(qname: &QName) -> String {
    format!("algo_{}_invoke", sanitize_qname(qname))
}

/// `AlgorithmProvider` adapter wrapping an Extism plugin pool + session registry.
pub struct ExtismAlgorithm {
    pool: Arc<ExtismInstancePool<extism::Plugin>>,
    registry: SharedRegistry,
    qname: QName,
    invoke_export: String,
    signature: AlgorithmSignature,
}

impl std::fmt::Debug for ExtismAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExtismAlgorithm")
            .field("qname", &self.qname)
            .finish_non_exhaustive()
    }
}

impl ExtismAlgorithm {
    /// Constructs an algorithm adapter against `pool` and the shared `registry`.
    #[must_use]
    pub fn new(
        pool: Arc<ExtismInstancePool<extism::Plugin>>,
        registry: SharedRegistry,
        qname: QName,
        signature: AlgorithmSignature,
    ) -> Self {
        let invoke_export = algo_invoke_export_name(&qname);
        Self {
            pool,
            registry,
            qname,
            invoke_export,
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

impl AlgorithmProvider for ExtismAlgorithm {
    fn signature(&self) -> &AlgorithmSignature {
        &self.signature
    }

    fn run(&self, ctx: AlgorithmContext<'_>) -> Result<SendableRecordBatchStream, FnError> {
        let host = ctx
            .host
            .ok_or_else(|| FnError::new(0x800, "extism algorithm: host unbound"))?;
        let bridge = host
            .as_any()
            .downcast_ref::<AlgorithmHostBridge>()
            .ok_or_else(|| FnError::new(0x801, "extism algorithm: host is not the bridge"))?;

        let json_args: Vec<serde_json::Value> = serde_json::from_str(ctx.config_json)
            .map_err(|e| FnError::new(0x802, format!("extism algorithm: bad config json: {e}")))?;

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
        let invoke_export = self.invoke_export.clone();

        let stream = futures::stream::once(async move {
            let graph = projection
                .await
                .map_err(|e| DataFusionError::Execution(format!("extism algorithm: {e}")))?;

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

            // Everything that can fail between open and close is wrapped so the
            // session is ALWAYS closed (no leak of the projected graph into the
            // process-global registry), even if input-build or `acquire` fails.
            let call_result: Result<(), DataFusionError> = (|| {
                let input = serde_json::to_vec(&serde_json::json!({
                    "session": sid, "graph": g, "args": json_args,
                }))
                .map_err(|e| DataFusionError::Execution(format!("extism algorithm input: {e}")))?;
                let mut leased = acquire(&pool)
                    .map_err(|e| DataFusionError::Execution(format!("extism acquire: {e}")))?;
                leased
                    .get_mut()
                    .call::<&[u8], &[u8]>(&invoke_export, &input)
                    .map(|_| ())
                    .map_err(|e| {
                        DataFusionError::Execution(format!("extism call `{invoke_export}`: {e}"))
                    })
            })();

            // Always close the session (freeing handles) even on guest error.
            let closed = registry.close(sid);
            call_result?;
            let mut closed =
                closed.ok_or_else(|| DataFusionError::Execution("session vanished".into()))?;
            let emitted = closed.take_emitted();

            build_batch(&schema_for_batch, &graph, &emitted)
                .map_err(|e| DataFusionError::Execution(format!("extism algorithm emit: {e}")))
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
        .map_err(|e| FnError::new(0x15, format!("extism algorithm batch: {e}")))
}
