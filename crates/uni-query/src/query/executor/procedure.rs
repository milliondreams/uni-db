// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use super::core::*;
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::Value;
use uni_cypher::ast::Expr;
use uni_store::QueryContext;
use uni_store::runtime::property_manager::PropertyManager;

fn success_result(success: bool) -> Result<Vec<HashMap<String, Value>>> {
    Ok(vec![HashMap::from([(
        "success".to_string(),
        Value::Bool(success),
    )])])
}

/// Value type for procedure parameters and outputs.
#[derive(Debug, Clone, PartialEq)]
pub enum ProcedureValueType {
    /// Cypher STRING type.
    String,
    /// Cypher INTEGER type.
    Integer,
    /// Cypher FLOAT type.
    Float,
    /// Cypher NUMBER type (accepts both INTEGER and FLOAT).
    Number,
    /// Cypher BOOLEAN type.
    Boolean,
    /// Accepts any value type.
    Any,
}

/// Single parameter declaration for a registered procedure.
#[derive(Debug, Clone)]
pub struct ProcedureParam {
    /// Parameter name as declared in the procedure signature.
    pub name: String,
    /// Expected type for this parameter.
    pub param_type: ProcedureValueType,
}

/// Single output column declaration for a registered procedure.
#[derive(Debug, Clone)]
pub struct ProcedureOutput {
    /// Output column name as declared in the procedure signature.
    pub name: String,
    /// Type of the output column.
    pub output_type: ProcedureValueType,
}

/// A procedure registered at runtime with static mock data.
///
/// Used by the TCK harness to define test procedures that the query
/// engine can call via `CALL proc.name(args) YIELD columns`.
#[derive(Debug, Clone)]
pub struct RegisteredProcedure {
    /// Fully qualified procedure name (e.g. `test.my.proc`).
    pub name: String,
    /// Declared input parameters.
    pub params: Vec<ProcedureParam>,
    /// Declared output columns.
    pub outputs: Vec<ProcedureOutput>,
    /// Mock data rows keyed by column name.
    pub data: Vec<HashMap<String, Value>>,
}

/// Thread-safe registry of procedures.
///
/// **M4 bridge:** The legacy `procedures` hashmap holds test-only
/// `RegisteredProcedure` mock rows used by TCK step definitions. The new
/// `plugin_registry` field holds an `Arc<uni_plugin::PluginRegistry>`
/// that the M4 cutover commits route real procedure dispatch through.
/// Both coexist during the M4 coexistence window; once every consumer
/// switches to plugin-path dispatch, the legacy hashmap is removed.
#[derive(Debug, Default)]
pub struct ProcedureRegistry {
    procedures: std::sync::RwLock<HashMap<String, RegisteredProcedure>>,
    plugin_registry: std::sync::RwLock<Option<Arc<uni_plugin::PluginRegistry>>>,
}

impl ProcedureRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a procedure, replacing any existing one with the same name.
    pub fn register(&self, proc_def: RegisteredProcedure) {
        self.procedures
            .write()
            .expect("ProcedureRegistry lock poisoned")
            .insert(proc_def.name.clone(), proc_def);
    }

    /// Looks up a procedure by fully qualified name (legacy path).
    pub fn get(&self, name: &str) -> Option<RegisteredProcedure> {
        self.procedures
            .read()
            .expect("ProcedureRegistry lock poisoned")
            .get(name)
            .cloned()
    }

    /// Removes all registered procedures.
    pub fn clear(&self) {
        self.procedures
            .write()
            .expect("ProcedureRegistry lock poisoned")
            .clear();
    }

    /// Attach an [`uni_plugin::PluginRegistry`] for plugin-path dispatch.
    ///
    /// M4 bridge: callers configure this once at executor construction,
    /// and the procedure dispatch site consults the plugin registry for
    /// any qname not present in the legacy `procedures` hashmap.
    pub fn set_plugin_registry(&self, pr: Arc<uni_plugin::PluginRegistry>) {
        *self
            .plugin_registry
            .write()
            .expect("ProcedureRegistry plugin-registry lock poisoned") = Some(pr);
    }

    /// Snapshot of the currently attached plugin registry, if any.
    ///
    /// Used by the read executor to thread the host's `PluginRegistry`
    /// into the physical planner so consultation sites like
    /// `plan_vector_knn` can look up registered `IndexHandle`s. Returns
    /// `None` if `set_plugin_registry` was never called (e.g., low-level
    /// test setups that bypass `Uni::build`).
    pub fn plugin_registry(&self) -> Option<Arc<uni_plugin::PluginRegistry>> {
        self.plugin_registry
            .read()
            .expect("ProcedureRegistry plugin-registry lock poisoned")
            .clone()
    }

    /// Look up a procedure through the attached `PluginRegistry`, if any.
    ///
    /// Dual-consult (M8.6): checks the per-task session-local plugin
    /// registry first (set by host crates via
    /// [`crate::scoped_with_session_plugin_registry`]) and falls back
    /// to the executor's instance-attached plugin registry. Returns
    /// `None` if neither has `qname`.
    pub fn get_plugin(
        &self,
        qname: &uni_plugin::QName,
    ) -> Option<std::sync::Arc<uni_plugin::registry::ProcedureEntry>> {
        // Session-local first.
        if let Some(session_pr) = crate::current_session_plugin_registry()
            && let Some(entry) = session_pr.procedure(qname)
        {
            return Some(entry);
        }
        self.plugin_registry
            .read()
            .expect("ProcedureRegistry plugin-registry lock poisoned")
            .as_ref()
            .and_then(|pr| pr.procedure(qname))
    }

    /// Resolve a user-facing procedure name (as written in `CALL X.Y.Z(...)`)
    /// to a registered plugin entry, applying the framework's namespace
    /// resolution rules:
    ///
    /// 1. If `user_qname` parses as `<namespace>.<local>`, try that exact
    ///    qname against the plugin registry.
    /// 2. Strip a leading `uni.` prefix if present, then try each known
    ///    built-in plugin namespace (`builtin`, `apoc-core`) with the
    ///    stripped local name. This lets user-facing names like
    ///    `uni.bitwise.and` route to plugins that registered under the
    ///    `apoc-core` namespace as `apoc-core.bitwise.and`.
    ///
    /// Future user plugins that want their qnames reachable as `uni.*`
    /// can declare their own namespace; the resolver will try the
    /// declared namespace before falling through.
    pub fn resolve_user_procedure(
        &self,
        user_qname: &str,
    ) -> Option<std::sync::Arc<uni_plugin::registry::ProcedureEntry>> {
        // Exact namespace.local match first.
        if let Some((ns, local)) = user_qname.split_once('.')
            && let Some(p) = self.get_plugin(&uni_plugin::QName::new(ns, local))
        {
            return Some(p);
        }
        // Strip `uni.` prefix and try each known built-in plugin namespace.
        // The `uni` namespace itself is reserved for host-coupled procedures
        // registered from `uni-query::procedures_plugin` (M4).
        let stripped = user_qname.strip_prefix("uni.").unwrap_or(user_qname);
        for plugin_id in ["uni", "builtin", "apoc-core", "custom"] {
            if let Some(p) = self.get_plugin(&uni_plugin::QName::new(plugin_id, stripped)) {
                return Some(p);
            }
        }
        None
    }
}

/// Convert a [`uni_common::Value`] into a DataFusion
/// [`datafusion::logical_expr::ColumnarValue`] scalar, suitable for
/// passing to a plugin procedure's `invoke()`.
fn value_to_columnar(
    v: &Value,
) -> std::result::Result<datafusion::logical_expr::ColumnarValue, String> {
    use datafusion::logical_expr::ColumnarValue;
    use datafusion::scalar::ScalarValue;

    let scalar = match v {
        Value::Null => ScalarValue::Null,
        Value::Bool(b) => ScalarValue::Boolean(Some(*b)),
        Value::Int(i) => ScalarValue::Int64(Some(*i)),
        Value::Float(f) => ScalarValue::Float64(Some(*f)),
        Value::String(s) => ScalarValue::Utf8(Some(s.clone())),
        Value::Bytes(b) => ScalarValue::Binary(Some(b.clone())),
        // Complex types (Map / List / etc.) are JSON-encoded into
        // `LargeBinary` to match the convention the DataFusion
        // procedure dispatcher (`procedure_call::value_to_columnar`)
        // uses. The adapter on the receiving end decodes via
        // `serde_json::from_slice`.
        Value::List(_) | Value::Map(_) => {
            let json = serde_json::Value::from(v.clone());
            let bytes = serde_json::to_vec(&json)
                .map_err(|e| format!("plugin procedure arg JSON encode failure: {e}"))?;
            ScalarValue::LargeBinary(Some(bytes))
        }
        other => {
            return Err(format!(
                "unsupported Value variant for plugin procedure argument: {other:?}"
            ));
        }
    };
    Ok(ColumnarValue::Scalar(scalar))
}

/// Convert one row of an Arrow array column into a [`uni_common::Value`].
/// Used when draining a plugin's output `RecordBatch` back to the legacy
/// row-shaped `Vec<HashMap<String, Value>>` the Executor returns.
fn arrow_scalar_to_value(
    arr: &dyn arrow_array::Array,
    row_idx: usize,
) -> std::result::Result<Value, String> {
    use arrow_array::cast::AsArray;
    use arrow_schema::DataType as Dt;

    if arr.is_null(row_idx) {
        return Ok(Value::Null);
    }
    match arr.data_type() {
        Dt::Boolean => Ok(Value::Bool(arr.as_boolean().value(row_idx))),
        Dt::Int64 => Ok(Value::Int(
            arr.as_primitive::<arrow_array::types::Int64Type>()
                .value(row_idx),
        )),
        Dt::Int32 => Ok(Value::Int(
            arr.as_primitive::<arrow_array::types::Int32Type>()
                .value(row_idx) as i64,
        )),
        Dt::UInt64 => Ok(Value::Int(
            arr.as_primitive::<arrow_array::types::UInt64Type>()
                .value(row_idx) as i64,
        )),
        Dt::Float64 => Ok(Value::Float(
            arr.as_primitive::<arrow_array::types::Float64Type>()
                .value(row_idx),
        )),
        Dt::Float32 => Ok(Value::Float(
            arr.as_primitive::<arrow_array::types::Float32Type>()
                .value(row_idx) as f64,
        )),
        Dt::Utf8 => Ok(Value::String(
            arr.as_string::<i32>().value(row_idx).to_string(),
        )),
        Dt::LargeUtf8 => Ok(Value::String(
            arr.as_string::<i64>().value(row_idx).to_string(),
        )),
        Dt::Binary => Ok(Value::Bytes(arr.as_binary::<i32>().value(row_idx).to_vec())),
        Dt::LargeBinary => Ok(Value::Bytes(arr.as_binary::<i64>().value(row_idx).to_vec())),
        other => Err(format!(
            "unsupported Arrow type in plugin procedure output: {other:?}"
        )),
    }
}

/// Filters a full result map to only the requested yield items.
/// If `yield_items` is empty, returns the full result unchanged.
fn filter_yield_items(
    full_result: HashMap<String, Value>,
    yield_items: &[String],
) -> HashMap<String, Value> {
    if yield_items.is_empty() {
        return full_result;
    }
    yield_items
        .iter()
        .filter_map(|name| full_result.get(name).map(|val| (name.clone(), val.clone())))
        .collect()
}

impl Executor {
    /// Evaluate a procedure argument as a string, returning an error with the given description.
    async fn eval_string_arg<'a>(
        &'a self,
        arg: &Expr,
        description: &str,
        prop_manager: &'a PropertyManager,
        params: &'a HashMap<String, Value>,
        ctx: Option<&'a QueryContext>,
    ) -> Result<String> {
        let empty_row = HashMap::new();
        self.evaluate_expr(arg, &empty_row, prop_manager, params, ctx)
            .await?
            .as_str()
            .ok_or_else(|| anyhow!("{} must be string", description))
            .map(|s| s.to_string())
    }

    pub(crate) async fn execute_procedure<'a>(
        &'a self,
        name: &str,
        args: &[Expr],
        yield_items: &[String],
        prop_manager: &'a PropertyManager,
        params: &'a HashMap<String, Value>,
        ctx: Option<&'a QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        match name {
            "uni.admin.compact" => {
                let stats = self.storage.compact().await?;
                let full_result = HashMap::from([
                    (
                        "files_compacted".to_string(),
                        Value::Int(stats.files_compacted as i64),
                    ),
                    (
                        "bytes_before".to_string(),
                        Value::Int(stats.bytes_before as i64),
                    ),
                    (
                        "bytes_after".to_string(),
                        Value::Int(stats.bytes_after as i64),
                    ),
                    (
                        "duration_ms".to_string(),
                        Value::Int(stats.duration.as_millis() as i64),
                    ),
                ]);

                Ok(vec![filter_yield_items(full_result, yield_items)])
            }
            "uni.admin.compactionStatus" => {
                let status = self
                    .storage
                    .compaction_status()
                    .map_err(|e| anyhow::anyhow!("Failed to get compaction status: {}", e))?;
                let full_result = HashMap::from([
                    ("l1_runs".to_string(), Value::Int(status.l1_runs as i64)),
                    (
                        "l1_size_bytes".to_string(),
                        Value::Int(status.l1_size_bytes as i64),
                    ),
                    (
                        "in_progress".to_string(),
                        Value::Bool(status.compaction_in_progress),
                    ),
                    (
                        "pending".to_string(),
                        Value::Int(status.compaction_pending as i64),
                    ),
                    (
                        "total_compactions".to_string(),
                        Value::Int(status.total_compactions as i64),
                    ),
                    (
                        "total_bytes_compacted".to_string(),
                        Value::Int(status.total_bytes_compacted as i64),
                    ),
                ]);

                Ok(vec![filter_yield_items(full_result, yield_items)])
            }
            "uni.admin.snapshot.create" => {
                let name = if !args.is_empty() {
                    Some(
                        self.eval_string_arg(&args[0], "Snapshot name", prop_manager, params, ctx)
                            .await?,
                    )
                } else {
                    None
                };

                let writer_arc = self
                    .writer
                    .as_ref()
                    .ok_or_else(|| anyhow!("Database is in read-only mode"))?;
                let writer: &uni_store::Writer = writer_arc.as_ref();
                let snapshot_id = writer.flush_to_l1(name).await?;

                Ok(vec![HashMap::from([(
                    "snapshot_id".to_string(),
                    Value::String(snapshot_id),
                )])])
            }
            "uni.admin.snapshot.list" => {
                let sm = self.storage.snapshot_manager();
                let ids = sm.list_snapshots().await?;
                let mut results = Vec::new();
                for id in ids {
                    if let Ok(m) = sm.load_snapshot(&id).await {
                        results.push(HashMap::from([
                            ("snapshot_id".to_string(), Value::String(m.snapshot_id)),
                            (
                                "name".to_string(),
                                m.name.map(Value::String).unwrap_or(Value::Null),
                            ),
                            (
                                "created_at".to_string(),
                                Value::String(m.created_at.to_rfc3339()),
                            ),
                            (
                                "version_hwm".to_string(),
                                Value::Int(m.version_high_water_mark as i64),
                            ),
                        ]));
                    }
                }
                Ok(results)
            }
            "uni.admin.snapshot.restore" => {
                let id = self
                    .eval_string_arg(&args[0], "Snapshot ID", prop_manager, params, ctx)
                    .await?;

                self.storage
                    .snapshot_manager()
                    .set_latest_snapshot(&id)
                    .await?;
                Ok(vec![HashMap::from([(
                    "status".to_string(),
                    Value::String("Restored".to_string()),
                )])])
            }
            // DDL Procedures
            "uni.schema.createLabel" => {
                let empty_row = HashMap::new();
                let name = self
                    .eval_string_arg(&args[0], "Label name", prop_manager, params, ctx)
                    .await?;
                let config = self
                    .evaluate_expr(&args[1], &empty_row, prop_manager, params, ctx)
                    .await?;

                let success =
                    super::ddl_procedures::create_label(&self.storage, &name, &config).await?;
                success_result(success)
            }
            "uni.schema.createEdgeType" => {
                let empty_row = HashMap::new();
                let name = self
                    .eval_string_arg(&args[0], "Edge type name", prop_manager, params, ctx)
                    .await?;
                let src_val = self
                    .evaluate_expr(&args[1], &empty_row, prop_manager, params, ctx)
                    .await?;
                let dst_val = self
                    .evaluate_expr(&args[2], &empty_row, prop_manager, params, ctx)
                    .await?;
                let config = self
                    .evaluate_expr(&args[3], &empty_row, prop_manager, params, ctx)
                    .await?;

                // Convert src/dst to Vec<String>
                let src_labels = src_val
                    .as_array()
                    .ok_or(anyhow!("Source labels must be a list"))?
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .ok_or(anyhow!("Label must be string"))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let dst_labels = dst_val
                    .as_array()
                    .ok_or(anyhow!("Target labels must be a list"))?
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .ok_or(anyhow!("Label must be string"))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let success = super::ddl_procedures::create_edge_type(
                    &self.storage,
                    &name,
                    src_labels,
                    dst_labels,
                    &config,
                )
                .await?;
                success_result(success)
            }
            "uni.schema.createIndex" => {
                let empty_row = HashMap::new();
                let label = self
                    .eval_string_arg(&args[0], "Label", prop_manager, params, ctx)
                    .await?;
                let property = self
                    .eval_string_arg(&args[1], "Property", prop_manager, params, ctx)
                    .await?;
                let config = self
                    .evaluate_expr(&args[2], &empty_row, prop_manager, params, ctx)
                    .await?;

                let success =
                    super::ddl_procedures::create_index(&self.storage, &label, &property, &config)
                        .await?;
                success_result(success)
            }
            "uni.schema.createConstraint" => {
                let label = self
                    .eval_string_arg(&args[0], "Label", prop_manager, params, ctx)
                    .await?;
                let c_type = self
                    .eval_string_arg(&args[1], "Constraint type", prop_manager, params, ctx)
                    .await?;
                let empty_row = HashMap::new();
                let props_val = self
                    .evaluate_expr(&args[2], &empty_row, prop_manager, params, ctx)
                    .await?;

                let properties = props_val
                    .as_array()
                    .ok_or(anyhow!("Properties must be a list"))?
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .ok_or(anyhow!("Property must be string"))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let success = super::ddl_procedures::create_constraint(
                    &self.storage,
                    &label,
                    &c_type,
                    properties,
                )
                .await?;
                success_result(success)
            }
            "uni.schema.dropLabel" => {
                let name = self
                    .eval_string_arg(&args[0], "Label name", prop_manager, params, ctx)
                    .await?;
                let success = super::ddl_procedures::drop_label(&self.storage, &name).await?;
                success_result(success)
            }
            "uni.schema.dropEdgeType" => {
                let name = self
                    .eval_string_arg(&args[0], "Edge type name", prop_manager, params, ctx)
                    .await?;
                let success = super::ddl_procedures::drop_edge_type(&self.storage, &name).await?;
                success_result(success)
            }
            "uni.schema.dropIndex" => {
                let name = self
                    .eval_string_arg(&args[0], "Index name", prop_manager, params, ctx)
                    .await?;
                let success = super::ddl_procedures::drop_index(&self.storage, &name).await?;
                success_result(success)
            }
            "uni.schema.dropConstraint" => {
                let name = self
                    .eval_string_arg(&args[0], "Constraint name", prop_manager, params, ctx)
                    .await?;
                let success = super::ddl_procedures::drop_constraint(&self.storage, &name).await?;
                success_result(success)
            }
            _ => {
                // M4: Plugin path — consult the framework PluginRegistry
                // before falling back to the legacy TCK mock registry.
                if let Some(registry) = &self.procedure_registry
                    && let Some(entry) = registry.resolve_user_procedure(name)
                {
                    return self
                        .execute_plugin_procedure(
                            name,
                            &entry,
                            args,
                            yield_items,
                            prop_manager,
                            params,
                            ctx,
                        )
                        .await;
                }

                // Legacy TCK mock-procedure registry.
                if let Some(registry) = &self.procedure_registry
                    && let Some(proc_def) = registry.get(name)
                {
                    return self
                        .execute_registered_procedure(
                            &proc_def,
                            args,
                            yield_items,
                            prop_manager,
                            params,
                            ctx,
                        )
                        .await;
                }
                Err(anyhow!("ProcedureNotFound: Unknown procedure '{}'", name))
            }
        }
    }

    /// Executes a procedure registered through the plugin framework.
    ///
    /// Evaluates argument `Expr`s to Values, converts them to
    /// `ColumnarValue` scalars, calls the plugin's `invoke()` to obtain
    /// a `SendableRecordBatchStream`, drains the stream, and converts the
    /// resulting Arrow batches to the legacy `Vec<HashMap<String, Value>>`
    /// shape the Executor expects.
    #[allow(clippy::too_many_arguments)] // mirrors the legacy execute_procedure signature
    async fn execute_plugin_procedure<'a>(
        &'a self,
        name: &str,
        entry: &uni_plugin::registry::ProcedureEntry,
        args: &[Expr],
        yield_items: &[String],
        prop_manager: &'a PropertyManager,
        params: &'a HashMap<String, Value>,
        ctx: Option<&'a QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        use datafusion::logical_expr::ColumnarValue;
        use futures::StreamExt;

        // Evaluate each arg expression to a Value, then map to a
        // ColumnarValue scalar for the plugin's invoke signature.
        let empty_row: HashMap<String, Value> = HashMap::new();
        let mut columnar_args: Vec<ColumnarValue> = Vec::with_capacity(args.len());
        for arg in args {
            let v = self
                .evaluate_expr(arg, &empty_row, prop_manager, params, ctx)
                .await?;
            columnar_args.push(
                value_to_columnar(&v)
                    .map_err(|e| anyhow!("Procedure '{name}': argument conversion failed: {e}"))?,
            );
        }

        let mut host = crate::query::executor::procedure_host::QueryProcedureHost::from_components(
            Arc::clone(&self.storage),
            Some(Arc::clone(&self.algo_registry)),
            self.procedure_registry.clone(),
        );
        // FU-1 / M11 #6: attach the outer executor's writer handle so
        // declared `WRITE`-mode procedures synthesized by
        // `CypherProcedureSynthesizer` can mutate via the write-enabled
        // inner-query host. The simple-Executor path
        // (`from_components`) is what the procedure_call -> stream
        // pipeline lands on for top-level `CALL <declared.qname>()`
        // invocations.
        if let Some(writer) = &self.writer {
            host = host.with_writer(Arc::clone(writer));
        }
        // FU-1: propagate the in-flight principal so capability gates
        // (e.g., `Capability::ProcedureWrites` on
        // `uni.plugin.declareProcedure WRITE`) see the session's
        // authenticated user, not an anonymous default. The
        // host + principal -> ProcedureContext construction is
        // consolidated in `uni_plugin::host::build_procedure_context`.
        let principal = crate::current_principal();
        let pctx = uni_plugin::host::build_procedure_context(&host, principal.as_deref());
        let mut stream = entry
            .procedure
            .invoke(pctx, &columnar_args)
            .map_err(|e| anyhow!("Procedure '{name}': {e}"))?;

        // Collect every batch the plugin yields and convert to row-shaped
        // Value maps. Schema comes from the plugin signature's yields.
        let mut rows: Vec<HashMap<String, Value>> = Vec::new();
        while let Some(item) = stream.next().await {
            let batch = item.map_err(|e| anyhow!("Procedure '{name}' stream error: {e}"))?;
            for row_idx in 0..batch.num_rows() {
                let mut row: HashMap<String, Value> = HashMap::new();
                let schema = batch.schema();
                for col_idx in 0..batch.num_columns() {
                    let field = schema.field(col_idx);
                    let arr = batch.column(col_idx);
                    let v = arrow_scalar_to_value(arr.as_ref(), row_idx)
                        .map_err(|e| anyhow!("Procedure '{name}': output decode: {e}"))?;
                    row.insert(field.name().clone(), v);
                }
                if !yield_items.is_empty() {
                    row.retain(|k, _| yield_items.iter().any(|y| y == k));
                }
                rows.push(row);
            }
        }
        Ok(rows)
    }

    /// Executes a procedure from the external registry.
    ///
    /// Evaluates arguments, validates count and types against the procedure
    /// declaration, filters data rows by matching input columns, and projects
    /// the requested output columns.
    ///
    /// # Errors
    ///
    /// Returns `InvalidNumberOfArguments` if the argument count is wrong,
    /// or `InvalidArgumentType` if an argument has an incompatible type.
    async fn execute_registered_procedure<'a>(
        &'a self,
        proc_def: &RegisteredProcedure,
        args: &[Expr],
        yield_items: &[String],
        prop_manager: &'a PropertyManager,
        params: &'a HashMap<String, Value>,
        ctx: Option<&'a QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let empty_row = HashMap::new();

        // Evaluate arguments
        let mut evaluated_args = Vec::with_capacity(args.len());
        for arg in args {
            evaluated_args.push(
                self.evaluate_expr(arg, &empty_row, prop_manager, params, ctx)
                    .await?,
            );
        }

        // Validate argument count
        if evaluated_args.len() != proc_def.params.len() {
            if evaluated_args.is_empty() && !proc_def.params.is_empty() {
                if yield_items.is_empty() {
                    // Standalone CALL — resolve implicit arguments from query parameters
                    let mut resolved = Vec::with_capacity(proc_def.params.len());
                    for param in &proc_def.params {
                        if let Some(val) = params.get(&param.name) {
                            resolved.push(val.clone());
                        } else {
                            return Err(anyhow!(
                                "MissingParameter: Procedure '{}' requires implicit argument '{}' \
                                 but it was not provided as a query parameter",
                                proc_def.name,
                                param.name
                            ));
                        }
                    }
                    evaluated_args = resolved;
                } else {
                    // In-query CALL with YIELD cannot use implicit arguments
                    return Err(anyhow!(
                        "InvalidArgumentPassingMode: Procedure '{}' requires explicit argument passing in in-query CALL",
                        proc_def.name
                    ));
                }
            } else {
                return Err(anyhow!(
                    "InvalidNumberOfArguments: Procedure '{}' expects {} argument(s), got {}",
                    proc_def.name,
                    proc_def.params.len(),
                    evaluated_args.len()
                ));
            }
        }

        // Validate argument types
        for (i, (arg_val, param)) in evaluated_args.iter().zip(&proc_def.params).enumerate() {
            if !arg_val.is_null() && !check_type_compatible(arg_val, &param.param_type) {
                return Err(anyhow!(
                    "InvalidArgumentType: Argument {} ('{}') of procedure '{}' has incompatible type",
                    i,
                    param.name,
                    proc_def.name
                ));
            }
        }

        // Filter data rows: keep rows where input columns match the provided args
        let filtered: Vec<&HashMap<String, Value>> = proc_def
            .data
            .iter()
            .filter(|row| {
                for (param, arg_val) in proc_def.params.iter().zip(&evaluated_args) {
                    if let Some(row_val) = row.get(&param.name)
                        && !values_match(row_val, arg_val)
                    {
                        return false;
                    }
                }
                true
            })
            .collect();

        // Collect output column names
        let output_names: Vec<&str> = proc_def.outputs.iter().map(|o| o.name.as_str()).collect();

        // Project output columns, applying yield_items filtering
        let results = filtered
            .into_iter()
            .map(|row| {
                let mut result = HashMap::new();
                if yield_items.is_empty() {
                    // Return all output columns
                    for name in &output_names {
                        if let Some(val) = row.get(*name) {
                            result.insert((*name).to_string(), val.clone());
                        }
                    }
                } else {
                    for yield_name in yield_items {
                        if let Some(val) = row.get(yield_name.as_str()) {
                            result.insert(yield_name.clone(), val.clone());
                        }
                    }
                }
                result
            })
            .collect();

        Ok(results)
    }
}

/// Checks whether a value is compatible with a procedure type.
fn check_type_compatible(val: &Value, expected: &ProcedureValueType) -> bool {
    match expected {
        ProcedureValueType::Any => true,
        ProcedureValueType::String => val.is_string(),
        ProcedureValueType::Boolean => val.is_bool(),
        ProcedureValueType::Integer => val.is_i64(),
        ProcedureValueType::Float => val.is_f64() || val.is_i64(),
        ProcedureValueType::Number => val.is_number(),
    }
}

/// Checks whether two values match for input-column filtering.
fn values_match(row_val: &Value, arg_val: &Value) -> bool {
    if arg_val.is_null() || row_val.is_null() {
        return arg_val.is_null() && row_val.is_null();
    }
    // Compare numbers by f64 to handle int/float cross-comparison
    if let (Some(a), Some(b)) = (row_val.as_f64(), arg_val.as_f64()) {
        return (a - b).abs() < f64::EPSILON;
    }
    row_val == arg_val
}
