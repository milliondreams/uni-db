// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `uni.algo.*` adapter wrapping the static
//! [`uni_algo::algo::AlgorithmRegistry`].
//!
//! One `AlgorithmProcedureAdapter` is registered per algorithm at startup
//! time. The adapter stores the algorithm's [`AlgoProcedure`] handle
//! directly, plus a cached [`ProcedureSignature`] derived from the
//! algorithm's signature. At invocation time the adapter downcasts the
//! [`ProcedureContext::host`] to [`QueryProcedureHost`], rebuilds an
//! [`AlgoContext`] from the host snapshot (mirroring the legacy
//! `execute_algo_procedure` in `procedure_call.rs`), drives the algo
//! stream to completion, and converts rows into a single Arrow
//! [`RecordBatch`].
//!
//! This is the M4 transitional adapter. M5c.1 will replace each wrapped
//! algorithm with a proper `AlgorithmProvider` implementation, and the
//! static `AlgorithmRegistry` will retire.
//!
//! **M5c.2 (2026-05-24):** the adapter now discriminates between two
//! call shapes by inspecting the JSON shape of `args[0]`:
//!
//! - **Legacy** — `args[0]` is a JSON `Array` (the `nodeLabels` list).
//!   The full positional arg vector is forwarded to
//!   `AlgoProcedure::execute` unchanged. The first call per `QName`
//!   per process emits a `tracing::warn!` flagged "legacy-arity";
//!   subsequent calls are silent.
//! - **V2 `(graphRef, config)`** — `args[0]` is a JSON `Object`. The
//!   adapter parses `graphRef` via [`uni_algo::parse_graph_ref`] into a
//!   [`ProjectionInput`], synthesises the algorithm's internal positional
//!   arg vector from `Native` variants (label/edge lists + per-optional-
//!   arg lookups into `config`), and forwards to the same
//!   `AlgoProcedure::execute`. `Cypher` runs the inner queries through
//!   `QueryProcedureHost::execute_inner_query`; `Named` resolves the
//!   stored projection via the per-`StorageManager` `ProjectionStore`.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::scalar::ScalarValue;
use futures::StreamExt;
use uni_algo::algo::AlgorithmRegistry;
use uni_algo::algo::procedures::{AlgoContext, AlgoProcedure, AlgoResultRow, ValueType};
use uni_algo::{ProjectionInput, parse_graph_ref};
use uni_common::Value;
use uni_plugin::traits::procedure::{
    NamedArgType, ProcedureContext, ProcedureMode, ProcedurePlugin, ProcedureSignature,
};
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::{FnError, PluginError, PluginRegistrar, QName, SideEffects};

use crate::query::df_graph::procedure_call::{
    build_typed_column, is_complex_value_type, json_to_value, value_type_to_arrow,
};
use crate::query::executor::procedure_host::QueryProcedureHost;

// Rust guideline compliant

/// `ProcedurePlugin` wrapping a single [`AlgoProcedure`] from the
/// static `AlgorithmRegistry`.
struct AlgorithmProcedureAdapter {
    proc: Arc<dyn AlgoProcedure>,
    /// Cached `uni-plugin` signature derived once at register time.
    signature: ProcedureSignature,
    /// Cached `uni-algo` yields (for `AlgoResultRow` column index
    /// lookup at row-projection time).
    algo_yields: Vec<(&'static str, ValueType)>,
}

impl std::fmt::Debug for AlgorithmProcedureAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlgorithmProcedureAdapter")
            .field("name", &self.proc.name())
            .finish()
    }
}

impl ProcedurePlugin for AlgorithmProcedureAdapter {
    fn signature(&self) -> &ProcedureSignature {
        &self.signature
    }

    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let host = ctx
            .host
            .and_then(|h| h.as_any().downcast_ref::<QueryProcedureHost>())
            .ok_or_else(|| {
                FnError::new(
                    0x701,
                    "uni.algo.*: requires QueryProcedureHost (host not bound on ProcedureContext)",
                )
            })?;

        // ColumnarValue → serde_json::Value, matching the legacy
        // `execute_algo_procedure` conversion. Scalars convert
        // directly; complex types (List/Map/etc) are encoded as
        // LargeBinary JSON by `procedure_call::value_to_columnar` and
        // are deserialized here back to JSON values so algos that take
        // `Vec<String>` (label lists, etc.) see the proper shape.
        let raw_args: Vec<serde_json::Value> = args
            .iter()
            .map(|cv| match cv {
                ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(b)))
                | ColumnarValue::Scalar(ScalarValue::Binary(Some(b))) => {
                    serde_json::from_slice::<serde_json::Value>(b)
                        .unwrap_or(serde_json::Value::Null)
                }
                ColumnarValue::Scalar(s) => {
                    serde_json::Value::from(scalar_value_to_uni_value(s.clone()))
                }
                ColumnarValue::Array(_) => serde_json::Value::Null,
            })
            .collect();

        // M5c.5: only the V2 `(graphRef, config)` shape is accepted at
        // the public Cypher entry. A JSON-Object first arg is parsed as
        // `graphRef`; anything else (positional vid args for procedures
        // like `shortestPath`, or empty arg lists) falls through to
        // `V2Plan::Direct` and is validated by the procedure's own
        // signature. The legacy `(['L'], ['E'], ...)` array form is
        // gone — callers must use the map shape.
        let plan = match raw_args.first() {
            Some(v) if v.is_object() => {
                let projection = parse_graph_ref(v)
                    .map_err(|e| FnError::new(0x820, format!("graphRef parse: {e}")))?;
                let config = raw_args
                    .get(1)
                    .cloned()
                    .unwrap_or_else(|| serde_json::Value::Object(Default::default()));
                let store = crate::projection_store::for_storage(host.storage());
                V2Plan::from_projection(
                    self.proc.name(),
                    self.proc.as_ref(),
                    projection,
                    config,
                    &store,
                )?
            }
            _ => V2Plan::Direct(raw_args),
        };

        // Build AlgoContext with L0 visibility, mirroring
        // `procedure_call.rs::execute_algo_procedure` lines 1244-1258.
        let l0_mgr = {
            let l0_ctx = host.l0_context();
            l0_ctx.current_l0.as_ref().map(|current| {
                let mut pending = l0_ctx.pending_flush_l0s.clone();
                if let Some(tx_l0) = &l0_ctx.transaction_l0 {
                    pending.push(tx_l0.clone());
                }
                Arc::new(uni_store::runtime::l0_manager::L0Manager::from_snapshot(
                    current.clone(),
                    pending,
                ))
            })
        };
        let algo_ctx = AlgoContext::new(Arc::clone(host.storage()), l0_mgr);

        let proc = Arc::clone(&self.proc);
        let algo_yields = self.algo_yields.clone();
        let plugin_yields = self.signature.yields.clone();
        let host = host.clone();
        let algo_name = self.proc.name().to_owned();

        // Drive the algo stream off-thread via a tokio task and pipe
        // RecordBatches into a one-shot channel; wrap as a
        // RecordBatchStreamAdapter for DataFusion.
        let out_schema = Arc::new(Schema::new(plugin_yields.clone()));

        let stream = futures::stream::once(async move {
            let mut algo_stream = match plan {
                V2Plan::Direct(args) => {
                    if proc.wants_native_terminals() {
                        // Cypher-path family: algorithm builds its own
                        // projection from the edge-type schema or
                        // per-call terminals. No projection arg.
                        proc.execute_with_native_terminals(algo_ctx, args)
                    } else {
                        // Standard `(nodeLabels, edgeTypes, …)` shape:
                        // host materialises the projection here and
                        // routes through the V2 entry point.
                        let projection =
                            uni_algo::algo::procedure_template::build_projection_from_direct_args(
                                proc.as_ref(),
                                &algo_ctx,
                                &args,
                            )
                            .await
                            .map_err(|e| {
                                datafusion::error::DataFusionError::Execution(format!(
                                    "{algo_name}: Direct projection build failed: {e}"
                                ))
                            })?;
                        proc.execute_with_projection(algo_ctx, args, projection)
                    }
                }
                V2Plan::Cypher {
                    node_query,
                    edge_query,
                    weight_column,
                    include_reverse,
                    args,
                } => {
                    // Materialise the projection by running the two
                    // inner queries against the outer L0 snapshot.
                    let inner_params = std::collections::HashMap::new();
                    let node_rows = host
                        .execute_inner_query(
                            &node_query,
                            &inner_params,
                            uni_plugin::traits::procedure::ProcedureMode::Read,
                        )
                        .await
                        .map_err(|e| {
                            datafusion::error::DataFusionError::Execution(format!(
                                "{algo_name}: Cypher projection node query failed: {e}"
                            ))
                        })?;
                    let edge_rows = host
                        .execute_inner_query(
                            &edge_query,
                            &inner_params,
                            uni_plugin::traits::procedure::ProcedureMode::Read,
                        )
                        .await
                        .map_err(|e| {
                            datafusion::error::DataFusionError::Execution(format!(
                                "{algo_name}: Cypher projection edge query failed: {e}"
                            ))
                        })?;
                    let projection = uni_algo::algo::projection::GraphProjection::from_rows(
                        &node_rows,
                        &edge_rows,
                        weight_column.as_deref(),
                        include_reverse,
                    )
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "{algo_name}: Cypher projection schema: {e}"
                        ))
                    })?;
                    proc.execute_with_projection(algo_ctx, args, projection)
                }
                V2Plan::Named { projection, args } => {
                    // ProjectionStore returned an Arc<GraphProjection>;
                    // try_unwrap so we move the projection into the
                    // algorithm when no other reader holds it, else
                    // clone the inner contents.
                    let owned = Arc::try_unwrap(projection).unwrap_or_else(|arc| (*arc).clone());
                    proc.execute_with_projection(algo_ctx, args, owned)
                }
            };
            let mut rows: Vec<AlgoResultRow> = Vec::new();
            while let Some(row_res) = algo_stream.next().await {
                if rows.len().is_multiple_of(1000) {
                    host.check_timeout().map_err(|e| {
                        datafusion::error::DataFusionError::Execution(e.to_string())
                    })?;
                }
                let row = row_res
                    .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
                rows.push(row);
            }
            build_algo_record_batch(&rows, &algo_yields, &plugin_yields)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }
}

/// Dispatch shape for the V2 `(graphRef, config)` adapter path.
///
/// Native and legacy resolve to [`V2Plan::Direct`] (just forward the
/// positional vector to `AlgoProcedure::execute`); Cypher carries the
/// inner queries so the async stream can materialise the projection
/// before calling [`AlgoProcedure::execute_with_projection`]; Named
/// carries the stored projection looked up from `ProjectionStore`.
enum V2Plan {
    /// Positional arg vector ready for `AlgoProcedure::execute`.
    Direct(Vec<serde_json::Value>),
    /// Cypher projection — inner queries deferred to the async path.
    Cypher {
        node_query: String,
        edge_query: String,
        weight_column: Option<String>,
        include_reverse: bool,
        /// Argument vector for `execute_with_projection`: positions 0
        /// and 1 are empty arrays (placeholders for `nodeLabels` /
        /// `edgeTypes`); positions 2.. are the algorithm-specific
        /// args drawn from `config`.
        args: Vec<serde_json::Value>,
    },
    /// Pre-materialised projection from `ProjectionStore`.
    Named {
        /// Projection looked up from the store.
        projection: Arc<uni_algo::algo::GraphProjection>,
        /// Argument vector for `execute_with_projection`.
        args: Vec<serde_json::Value>,
    },
}

impl V2Plan {
    /// Decide which `V2Plan` variant a parsed [`ProjectionInput`] maps
    /// to and prepare the positional argument vector the algorithm
    /// will see.
    fn from_projection(
        algo_name: &str,
        proc: &dyn AlgoProcedure,
        projection: ProjectionInput,
        config: serde_json::Value,
        store: &crate::projection_store::ProjectionStore,
    ) -> Result<Self, FnError> {
        use serde_json::Value as J;

        let mut config_obj = match config {
            J::Object(m) => m,
            J::Null => serde_json::Map::new(),
            other => {
                return Err(FnError::new(
                    0x820,
                    format!("config must be a Map, got {other}"),
                ));
            }
        };

        match projection {
            ProjectionInput::Native {
                node_labels,
                edge_types,
                weight_property,
                include_reverse: _,
            } => {
                if let Some(weight) = weight_property
                    && !config_obj.contains_key("weightProperty")
                {
                    config_obj.insert("weightProperty".to_owned(), J::String(weight));
                }
                Ok(V2Plan::Direct(build_legacy_arg_vec(
                    proc,
                    node_labels,
                    edge_types,
                    &config_obj,
                )))
            }
            ProjectionInput::Cypher {
                node_query,
                edge_query,
                weight_column,
                include_reverse,
            } => {
                // `graphRef.weightColumn` mirrors into the algorithm's
                // `weightProperty` config key for algos that read it.
                if let Some(col) = &weight_column
                    && !config_obj.contains_key("weightProperty")
                {
                    config_obj.insert("weightProperty".to_owned(), J::String(col.clone()));
                }
                // Build the arg vector with empty label/edge placeholders;
                // `execute_with_projection` ignores positions 0/1 entirely.
                let args = build_legacy_arg_vec(proc, Vec::new(), Vec::new(), &config_obj);
                Ok(V2Plan::Cypher {
                    node_query,
                    edge_query,
                    weight_column,
                    include_reverse,
                    args,
                })
            }
            ProjectionInput::Named { name } => {
                let entry = store.get(&name).ok_or_else(|| {
                    FnError::new(
                        0x822,
                        format!(
                            "{algo_name}: no projection named `{name}` \
                             in the ProjectionStore; call `uni.graph.project` first"
                        ),
                    )
                })?;
                let args = build_legacy_arg_vec(proc, Vec::new(), Vec::new(), &config_obj);
                Ok(V2Plan::Named {
                    projection: entry.projection,
                    args,
                })
            }
        }
    }
}

/// Assemble the positional `Vec<serde_json::Value>` for an algorithm
/// from the canonical 2-arg `(graphRef Native, config)` shape.
///
/// Walks the algorithm's *native* `uni-algo::ProcedureSignature`
/// (`proc.signature()`) so we can read each optional arg's declared
/// default. We compute the highest optional slot that `config` actually
/// supplies; the result is truncated to that length so any unsupplied
/// optionals beyond it stay missing and `validate_args` substitutes
/// them from `signature.optional_args` defaults.
fn build_legacy_arg_vec(
    proc: &dyn AlgoProcedure,
    node_labels: Vec<String>,
    edge_types: Vec<String>,
    config: &serde_json::Map<String, serde_json::Value>,
) -> Vec<serde_json::Value> {
    use serde_json::Value as J;
    let sig = proc.signature();
    let req = &sig.args; // required (always [nodeLabels, relationshipTypes])
    let opt = &sig.optional_args; // optional, in declared order

    let last_supplied = opt
        .iter()
        .enumerate()
        .rev()
        .find_map(|(i, (n, _, _))| config.contains_key(*n).then_some(i));

    let mut out: Vec<J> = Vec::with_capacity(req.len() + opt.len());
    out.push(J::Array(node_labels.into_iter().map(J::String).collect()));
    out.push(J::Array(edge_types.into_iter().map(J::String).collect()));
    let Some(end) = last_supplied else {
        return out; // bare 2-slot internal vector — no optional args supplied.
    };
    for (i, (name, _ty, default)) in opt.iter().enumerate().take(end + 1) {
        let v = config
            .get(*name)
            .cloned()
            .unwrap_or_else(|| default.clone());
        let _ = i;
        out.push(v);
    }
    out
}

/// Convert a DataFusion `ScalarValue` into a `uni_common::Value` for the
/// algo crate. Covers the primitives the algo CALL signatures use; falls
/// back to `Null` for other shapes (algos don't currently take complex
/// arg types).
fn scalar_value_to_uni_value(sv: ScalarValue) -> Value {
    match sv {
        ScalarValue::Null => Value::Null,
        ScalarValue::Boolean(Some(b)) => Value::Bool(b),
        ScalarValue::Int64(Some(i)) => Value::Int(i),
        ScalarValue::Int32(Some(i)) => Value::Int(i64::from(i)),
        ScalarValue::UInt64(Some(u)) => i64::try_from(u).map(Value::Int).unwrap_or(Value::Null),
        ScalarValue::Float64(Some(f)) => Value::Float(f),
        ScalarValue::Float32(Some(f)) => Value::Float(f64::from(f)),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Value::String(s),
        ScalarValue::Binary(Some(b)) | ScalarValue::LargeBinary(Some(b)) => Value::Bytes(b),
        _ => Value::Null,
    }
}

/// Project the algorithm's `AlgoResultRow`s onto the plugin's declared
/// Arrow schema. Mirrors `procedure_call.rs::build_algo_batch` but
/// operates on the plugin's `signature.yields` (Arrow `Field`s) directly
/// rather than the legacy yield-name-and-alias pairs.
fn build_algo_record_batch(
    rows: &[AlgoResultRow],
    algo_yields: &[(&'static str, ValueType)],
    plugin_yields: &[Field],
) -> Result<RecordBatch, datafusion::error::DataFusionError> {
    let out_schema = Arc::new(Schema::new(plugin_yields.to_vec()));
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(out_schema));
    }

    let num_rows = rows.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(plugin_yields.len());

    for (idx, field) in plugin_yields.iter().enumerate() {
        // The plugin schema is built 1:1 from the algo schema, so column
        // `idx` corresponds to `algo_yields[idx]`. Defensive: fall back
        // to a name lookup if the orders ever drift.
        let algo_idx = if idx < algo_yields.len() && algo_yields[idx].0 == field.name() {
            idx
        } else {
            algo_yields
                .iter()
                .position(|(n, _)| *n == field.name())
                .unwrap_or(usize::MAX)
        };

        let uni_values: Vec<Value> = rows
            .iter()
            .map(|row| {
                if algo_idx == usize::MAX || algo_idx >= row.values.len() {
                    Value::Null
                } else {
                    json_to_value(&row.values[algo_idx])
                }
            })
            .collect();

        let values_iter = uni_values.iter().map(Some);
        columns.push(build_typed_column(values_iter, num_rows, field.data_type()));
    }

    RecordBatch::try_new(out_schema, columns).map_err(crate::query::df_graph::common::arrow_err)
}

/// Build a `uni-plugin` `ProcedureSignature` from an algorithm's
/// `uni-algo` signature. The args list combines required + optional.
fn build_plugin_signature(
    proc: &dyn AlgoProcedure,
) -> (ProcedureSignature, Vec<(&'static str, ValueType)>) {
    let algo_sig = proc.signature();

    let mut args: Vec<NamedArgType> =
        Vec::with_capacity(algo_sig.args.len() + algo_sig.optional_args.len());
    for (name, vt) in &algo_sig.args {
        args.push(NamedArgType {
            name: smol_str::SmolStr::new(*name),
            ty: ArgType::Primitive(value_type_to_arrow(vt)),
            default: None,
            doc: String::new(),
        });
    }
    for (name, vt, default) in &algo_sig.optional_args {
        args.push(NamedArgType {
            name: smol_str::SmolStr::new(*name),
            ty: ArgType::Primitive(value_type_to_arrow(vt)),
            default: serde_json_to_scalar(default, vt),
            doc: String::new(),
        });
    }

    let yields: Vec<Field> = algo_sig
        .yields
        .iter()
        .map(|(name, vt)| {
            let mut field = Field::new((*name).to_owned(), value_type_to_arrow(vt), true);
            if is_complex_value_type(vt) {
                let mut metadata = std::collections::HashMap::new();
                metadata.insert("cv_encoded".to_owned(), "true".to_owned());
                field = field.with_metadata(metadata);
            }
            field
        })
        .collect();

    let plugin_sig = ProcedureSignature {
        args,
        yields,
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: format!("uni.{} (algorithm adapter)", proc.name()),
    };
    (plugin_sig, algo_sig.yields)
}

/// Best-effort conversion of an algo optional-arg default to a
/// DataFusion `ScalarValue`. Returns `None` if the shape doesn't map
/// cleanly; callers requesting the optional arg without supplying it
/// will then see `Null` and the algo's own `validate_args` re-applies
/// the default from `signature().optional_args`.
fn serde_json_to_scalar(v: &serde_json::Value, vt: &ValueType) -> Option<ScalarValue> {
    match (v, vt) {
        (serde_json::Value::Null, _) => Some(ScalarValue::Null),
        (serde_json::Value::Bool(b), _) => Some(ScalarValue::Boolean(Some(*b))),
        (serde_json::Value::Number(n), ValueType::Int) => {
            n.as_i64().map(|i| ScalarValue::Int64(Some(i)))
        }
        (serde_json::Value::Number(n), _) => n.as_f64().map(|f| ScalarValue::Float64(Some(f))),
        (serde_json::Value::String(s), _) => Some(ScalarValue::Utf8(Some(s.clone()))),
        _ => None,
    }
}

/// Register every `uni.algo.*` adapter from `algo_registry` into `r`.
///
/// One `ProcedurePlugin` registration per entry; the adapter retains an
/// `Arc` to the algorithm so the registry itself can be dropped after
/// registration without invalidating the plugins.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a qname is already
/// taken in the underlying plugin registry.
pub fn register_into(
    r: &mut PluginRegistrar<'_>,
    algo_registry: &Arc<AlgorithmRegistry>,
) -> Result<(), PluginError> {
    let _ = DataType::Utf8; // silence unused-import lint if it ever appears
    for name in algo_registry.list() {
        let Some(proc) = algo_registry.get(name) else {
            continue;
        };
        let (signature, algo_yields) = build_plugin_signature(&*proc);
        let suffix = name.strip_prefix("uni.").unwrap_or(name).to_owned();
        let adapter = Arc::new(AlgorithmProcedureAdapter {
            proc,
            signature: signature.clone(),
            algo_yields,
        });
        r.procedure(QName::new("uni", suffix), signature, adapter)?;
    }
    Ok(())
}
