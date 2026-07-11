//! Bridge wiring `uni_algo::AlgoProcedure` into
//! `uni_plugin::AlgorithmProvider`.
//!
//! The bridge implements `AlgorithmProvider::run` by:
//! 1. Parsing `config_json` into `Vec<serde_json::Value>` args (the
//!    shape the algo expects from `CALL`).
//! 2. Downcasting `AlgorithmContext::host` to [`AlgorithmHostBridge`] to
//!    recover the concrete `AlgoContext` (StorageManager + L0Manager).
//! 3. Driving the algorithm's `AlgoResultRow` stream to completion and
//!    collecting it into a single Arrow `RecordBatch` matching the
//!    declared `AlgorithmSignature::output_fields`.
//!
//! When no host is bound, the bridge returns an
//! `Unbound` error code so the caller can supply the host on retry.
//
// Rust guideline compliant

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use futures::future::BoxFuture;
use uni_algo::algo::procedures::{AlgoContext, AlgoProcedure, AlgoResultRow, ValueType};
use uni_algo::algo::projection::{GraphProjection, ProjectionBuilder};
use uni_common::core::id::Vid;
use uni_plugin::traits::algorithm::{
    AlgorithmContext, AlgorithmHost, AlgorithmProvider, AlgorithmSignature, GraphProjectionSpec,
    GraphView,
};
use uni_plugin::{Capability, CapabilitySet, FnError};

/// Read-only [`GraphView`] backed by a materialized [`GraphProjection`].
///
/// Slot-indexed accessors delegate directly to the projection's dense CSR
/// arrays; see [`GraphView`] for the panic contract on weights / reverse.
pub struct GraphViewImpl(Arc<GraphProjection>);

impl std::fmt::Debug for GraphViewImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphViewImpl")
            .field("vertex_count", &self.0.vertex_count())
            .field("edge_count", &self.0.edge_count())
            .finish_non_exhaustive()
    }
}

impl GraphView for GraphViewImpl {
    fn vertex_count(&self) -> usize {
        self.0.vertex_count()
    }
    fn edge_count(&self) -> usize {
        self.0.edge_count()
    }
    fn out_neighbors(&self, slot: u32) -> &[u32] {
        self.0.out_neighbors(slot)
    }
    fn out_degree(&self, slot: u32) -> u32 {
        self.0.out_degree(slot)
    }
    fn in_neighbors(&self, slot: u32) -> &[u32] {
        self.0.in_neighbors(slot)
    }
    fn in_degree(&self, slot: u32) -> u32 {
        self.0.in_degree(slot)
    }
    fn has_reverse(&self) -> bool {
        self.0.has_reverse()
    }
    fn out_weight(&self, slot: u32, edge_idx: usize) -> f64 {
        self.0.out_weight(slot, edge_idx)
    }
    fn has_weights(&self) -> bool {
        self.0.has_weights()
    }
    fn to_vid(&self, slot: u32) -> Vid {
        self.0.to_vid(slot)
    }
    fn to_slot(&self, vid: Vid) -> Option<u32> {
        self.0.to_slot(vid)
    }
    fn vertices(&self) -> Box<dyn Iterator<Item = (u32, Vid)> + '_> {
        Box::new(self.0.vertices())
    }
}

/// Bridge host that surfaces `StorageManager` + optional `L0Manager`
/// to plugin algorithms through [`AlgorithmHost`].
///
/// Provides [`AlgorithmHost::project`] (gated on [`Capability::HostQuery`])
/// as the stable topology-access path, and retains
/// [`AlgorithmHost::as_any`] for the legacy downcast used by
/// [`AlgoProviderBridge`].
pub struct AlgorithmHostBridge {
    /// The concrete algo context the wrapped procedures need.
    pub algo_ctx: AlgoContext,
    /// Effective capabilities of the plugin owning the running algorithm.
    pub effective_caps: CapabilitySet,
}

impl std::fmt::Debug for AlgorithmHostBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlgorithmHostBridge")
            .field("effective_caps", &self.effective_caps)
            .finish_non_exhaustive()
    }
}

impl AlgorithmHostBridge {
    /// Construct a host bridge from an [`AlgoContext`] and effective caps.
    #[must_use]
    pub fn new(algo_ctx: AlgoContext, effective_caps: CapabilitySet) -> Self {
        Self {
            algo_ctx,
            effective_caps,
        }
    }

    /// Builds a concrete projection for GraphCompute kernels, gated on caps.
    ///
    /// Unlike [`AlgorithmHost::project`] (which yields an opaque
    /// `Arc<dyn GraphView>`), this returns the concrete `Arc<GraphProjection>`
    /// an [`AlgoSession`](crate::algorithms::graph_compute::AlgoSession) binds.
    /// It enforces the two orthogonal gates of the proposal (§4.6):
    /// [`Capability::GraphCompute`] for the kernel surface and
    /// [`Capability::HostQuery`] for the data read.
    ///
    /// # Errors
    /// Returns `0x86C` if `GraphCompute` is not granted, `0x804` if `HostQuery`
    /// is not granted, or `0x803` if the projection build fails.
    pub fn project_for_graph_compute(
        &self,
        spec: &GraphProjectionSpec,
    ) -> BoxFuture<'static, Result<Arc<GraphProjection>, FnError>> {
        if !self
            .effective_caps
            .contains_variant(&Capability::GraphCompute)
        {
            return Box::pin(async {
                Err(FnError::new(
                    crate::algorithms::graph_compute::error::CAPABILITY_DENIED,
                    "GraphCompute: capability `graph-compute` not granted",
                ))
            });
        }
        if !self
            .effective_caps
            .contains_variant(&Capability::HostQuery {
                read_only: false,
                scopes: Vec::new(),
            })
        {
            return Box::pin(async {
                Err(FnError::new(
                    0x804,
                    "GraphCompute: `project` additionally requires `HostQuery`",
                ))
            });
        }
        // Enforce the HostQuery scope restriction (E5): when the grant names
        // scopes (label / edge-type prefixes), every projected label and edge
        // type must match one — a plugin scoped to `Person` cannot project the
        // whole graph. An empty scope list is unrestricted (the default).
        let scope_prefixes: Vec<String> = self
            .effective_caps
            .iter()
            .find_map(|c| match c {
                Capability::HostQuery { scopes, .. } => {
                    Some(scopes.iter().map(ToString::to_string).collect())
                }
                _ => None,
            })
            .unwrap_or_default();
        if !scope_prefixes.is_empty() {
            let in_scope = |name: &str| scope_prefixes.iter().any(|p| name.starts_with(p.as_str()));
            let denied = spec
                .node_labels
                .iter()
                .chain(spec.edge_types.iter())
                .find(|name| !in_scope(name));
            if let Some(name) = denied {
                let name = name.clone();
                let scopes = scope_prefixes.join(", ");
                return Box::pin(async move {
                    Err(FnError::new(
                        0x804,
                        format!(
                            "GraphCompute: `{name}` is outside the granted HostQuery scopes [{scopes}]"
                        ),
                    ))
                });
            }
        }
        let storage = Arc::clone(&self.algo_ctx.storage);
        let l0 = self.algo_ctx.l0_manager.as_ref().map(Arc::clone);
        let spec = spec.clone();
        Box::pin(async move {
            let node_labels: Vec<&str> = spec.node_labels.iter().map(String::as_str).collect();
            let edge_types: Vec<&str> = spec.edge_types.iter().map(String::as_str).collect();
            let mut builder = ProjectionBuilder::new(storage)
                .l0_manager(l0)
                .node_labels(&node_labels)
                .edge_types(&edge_types)
                .include_reverse(spec.include_reverse);
            if let Some(prop) = spec.weight_property.as_deref() {
                builder = builder.weight_property(prop);
            }
            let projection = builder.build().await.map_err(|e| {
                FnError::new(0x803, format!("GraphCompute project build failed: {e}"))
            })?;
            Ok(Arc::new(projection))
        })
    }

    /// Reads the per-invocation GraphCompute work and arena-byte caps.
    ///
    /// Uses a plugin's declared [`Capability::GraphComputeWork`] /
    /// [`Capability::GraphComputeArenaBytes`] quota when present, otherwise the
    /// pinned defaults (proposal §12). The work cap is combined with the
    /// edge-count multiple by the caller.
    #[must_use]
    pub fn graph_compute_caps(&self) -> (Option<u64>, usize) {
        let mut work = None;
        let mut arena = crate::algorithms::graph_compute::DEFAULT_ARENA_MAX_BYTES;
        for cap in self.effective_caps.iter() {
            match cap {
                Capability::GraphComputeWork(w) => work = Some(*w),
                Capability::GraphComputeArenaBytes(b) => {
                    arena = usize::try_from(*b).unwrap_or(usize::MAX);
                }
                _ => {}
            }
        }
        (work, arena)
    }

    /// Reads the per-invocation wall-clock deadline for a GraphCompute guest.
    ///
    /// Returns the plugin's declared [`Capability::WallClockMillisPerCall`] grant
    /// in milliseconds, if present. A loader uses it to arm its watchdog /
    /// deadline; absent, the loader applies its own default.
    #[must_use]
    pub fn graph_compute_deadline_ms(&self) -> Option<u64> {
        self.effective_caps.iter().find_map(|cap| match cap {
            Capability::WallClockMillisPerCall(ms) => Some(*ms),
            _ => None,
        })
    }
}

impl AlgorithmHost for AlgorithmHostBridge {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn project(
        &self,
        spec: &GraphProjectionSpec,
    ) -> BoxFuture<'static, Result<Arc<dyn GraphView>, FnError>> {
        // Gate topology access on `HostQuery` (variant match; payload
        // attenuation is applied at registration-time intersection).
        if !self
            .effective_caps
            .contains_variant(&Capability::HostQuery {
                read_only: false,
                scopes: Vec::new(),
            })
        {
            return Box::pin(async {
                Err(FnError::new(
                    0x804,
                    "AlgorithmHost::project: capability `HostQuery` not granted",
                ))
            });
        }

        // Clone owned inputs into the `'static` future so it can be moved
        // into the stream a provider returns from the synchronous `run`.
        let storage = Arc::clone(&self.algo_ctx.storage);
        let l0 = self.algo_ctx.l0_manager.as_ref().map(Arc::clone);
        let spec = spec.clone();

        Box::pin(async move {
            let node_labels: Vec<&str> = spec.node_labels.iter().map(String::as_str).collect();
            let edge_types: Vec<&str> = spec.edge_types.iter().map(String::as_str).collect();
            let mut builder = ProjectionBuilder::new(storage)
                .l0_manager(l0)
                .node_labels(&node_labels)
                .edge_types(&edge_types)
                .include_reverse(spec.include_reverse);
            if let Some(prop) = spec.weight_property.as_deref() {
                builder = builder.weight_property(prop);
            }
            let projection = builder.build().await.map_err(|e| {
                FnError::new(0x803, format!("AlgorithmHost::project build failed: {e}"))
            })?;
            Ok(Arc::new(GraphViewImpl(Arc::new(projection))) as Arc<dyn GraphView>)
        })
    }
}

/// Provider wrapping a single [`AlgoProcedure`].
pub struct AlgoProviderBridge {
    proc: Arc<dyn AlgoProcedure>,
    signature: AlgorithmSignature,
    yields: Vec<(&'static str, ValueType)>,
}

impl std::fmt::Debug for AlgoProviderBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlgoProviderBridge")
            .field("name", &self.proc.name())
            .finish_non_exhaustive()
    }
}

impl AlgoProviderBridge {
    /// Wrap an `AlgoProcedure` as an `AlgorithmProvider`.
    #[must_use]
    pub fn new(proc: Arc<dyn AlgoProcedure>) -> Self {
        let sig = proc.signature();
        let output_fields: Vec<Field> = sig
            .yields
            .iter()
            .map(|(n, vt)| Field::new((*n).to_owned(), value_type_to_arrow(vt), true))
            .collect();
        let signature = AlgorithmSignature {
            output_fields,
            docs: format!("uni.{} (algorithm)", proc.name()),
        };
        Self {
            proc,
            signature,
            yields: sig.yields,
        }
    }
}

impl AlgorithmProvider for AlgoProviderBridge {
    fn signature(&self) -> &AlgorithmSignature {
        &self.signature
    }

    fn run(&self, ctx: AlgorithmContext<'_>) -> Result<SendableRecordBatchStream, FnError> {
        let host = ctx
            .host
            .ok_or_else(|| FnError::new(0x800, "AlgoProviderBridge: host unbound"))?;
        let bridge = host
            .as_any()
            .downcast_ref::<AlgorithmHostBridge>()
            .ok_or_else(|| {
                FnError::new(0x801, "AlgoProviderBridge: host is not AlgorithmHostBridge")
            })?;

        let args: Vec<serde_json::Value> = if ctx.config_json.is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(ctx.config_json)
                .map_err(|e| FnError::new(0x802, format!("config_json parse: {e}")))?
        };

        // Clone what we need into the async stream; the wrapped
        // `AlgoContext` is `!Clone`, but `StorageManager` / `L0Manager`
        // inside are `Arc`, so we rebuild a fresh `AlgoContext` from
        // their clones.
        let algo_ctx = AlgoContext::new(
            Arc::clone(&bridge.algo_ctx.storage),
            bridge.algo_ctx.l0_manager.as_ref().map(Arc::clone),
        );
        let proc = Arc::clone(&self.proc);
        let yields = self.yields.clone();
        let fields = self.signature.output_fields.clone();
        let out_schema = Arc::new(Schema::new(fields.clone()));

        let stream = futures::stream::once(async move {
            // Same dispatch logic as `uni-query`'s V2Plan::Direct
            // branch: route cypher-path algos through
            // `execute_with_native_terminals`; everything else builds
            // a projection from `(nodeLabels, edgeTypes, …)` args and
            // takes the projection-aware entry point.
            let mut algo_stream = if proc.wants_native_terminals() {
                proc.execute_with_native_terminals(algo_ctx, args)
            } else {
                let projection =
                    uni_algo::algo::procedure_template::build_projection_from_direct_args(
                        proc.as_ref(),
                        &algo_ctx,
                        &args,
                    )
                    .await
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "AlgoProviderBridge projection build failed: {e}"
                        ))
                    })?;
                proc.execute_with_projection(algo_ctx, args, projection)
            };
            let mut rows: Vec<AlgoResultRow> = Vec::new();
            while let Some(row_res) = algo_stream.next().await {
                let row = row_res
                    .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
                rows.push(row);
            }
            build_record_batch(&rows, &yields, &fields)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }
}

fn value_type_to_arrow(vt: &ValueType) -> DataType {
    match vt {
        ValueType::Int => DataType::Int64,
        ValueType::Float => DataType::Float64,
        ValueType::String => DataType::Utf8,
        ValueType::Bool => DataType::Boolean,
        ValueType::List | ValueType::Map | ValueType::Path => DataType::LargeBinary,
        ValueType::Node => DataType::Int64,
        ValueType::Relationship => DataType::Int64,
        ValueType::Any => DataType::Utf8,
    }
}

fn build_record_batch(
    rows: &[AlgoResultRow],
    yields: &[(&'static str, ValueType)],
    fields: &[Field],
) -> Result<RecordBatch, datafusion::error::DataFusionError> {
    use arrow_array::{BooleanArray, Float64Array, Int64Array, LargeBinaryArray, StringArray};
    let schema = Arc::new(Schema::new(fields.to_vec()));
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(fields.len());
    for (idx, (_name, vt)) in yields.iter().enumerate() {
        let col: ArrayRef = match vt {
            ValueType::Int | ValueType::Node | ValueType::Relationship => {
                let v: Vec<Option<i64>> = rows
                    .iter()
                    .map(|r| {
                        r.values
                            .get(idx)
                            .and_then(|x| x.as_i64().or_else(|| x.as_u64().map(|u| u as i64)))
                    })
                    .collect();
                Arc::new(Int64Array::from(v))
            }
            ValueType::Float => {
                let v: Vec<Option<f64>> = rows
                    .iter()
                    .map(|r| r.values.get(idx).and_then(|x| x.as_f64()))
                    .collect();
                Arc::new(Float64Array::from(v))
            }
            ValueType::Bool => {
                let v: Vec<Option<bool>> = rows
                    .iter()
                    .map(|r| r.values.get(idx).and_then(|x| x.as_bool()))
                    .collect();
                Arc::new(BooleanArray::from(v))
            }
            ValueType::String | ValueType::Any => {
                let v: Vec<Option<String>> = rows
                    .iter()
                    .map(|r| {
                        r.values.get(idx).map(|x| {
                            x.as_str()
                                .map(str::to_owned)
                                .unwrap_or_else(|| x.to_string())
                        })
                    })
                    .collect();
                Arc::new(StringArray::from(v))
            }
            ValueType::List | ValueType::Map | ValueType::Path => {
                let v: Vec<Option<Vec<u8>>> = rows
                    .iter()
                    .map(|r| {
                        r.values
                            .get(idx)
                            .map(|x| serde_json::to_vec(x).unwrap_or_default())
                    })
                    .collect();
                Arc::new(LargeBinaryArray::from_iter(v.iter().map(|o| o.as_deref())))
            }
        };
        cols.push(col);
    }
    RecordBatch::try_new(schema, cols)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Helper: build an `AlgorithmHostBridge` from `StorageManager` + L0.
///
/// Hosts use this when constructing an `AlgorithmContext`. `effective_caps`
/// carries the owning plugin's grants so [`AlgorithmHost::project`] can gate
/// topology access on [`Capability::HostQuery`].
#[must_use]
pub fn host_bridge_from_storage(
    storage: Arc<uni_store::storage::manager::StorageManager>,
    l0: Option<Arc<uni_store::runtime::L0Manager>>,
    effective_caps: CapabilitySet,
) -> AlgorithmHostBridge {
    AlgorithmHostBridge::new(AlgoContext::new(storage, l0), effective_caps)
}
