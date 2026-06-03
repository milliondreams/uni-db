// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Procedure call execution plan for DataFusion.
//!
//! This module provides [`GraphProcedureCallExec`], a DataFusion [`ExecutionPlan`] that
//! executes Cypher `CALL` procedures natively within the DataFusion engine.
//!
//! Used for composite queries where a `CALL` is followed by `MATCH`, e.g.:
//! ```text
//! CALL uni.schema.labels() YIELD label
//! MATCH (n:Person) WHERE label = 'Person'
//! RETURN n.name, label
//! ```

use arrow_array::builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::Stream;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use uni_common::Value;
use uni_cypher::ast::Expr;

use crate::query::df_graph::GraphExecutionContext;
use crate::query::df_graph::common::{
    arrow_err, compute_plan_properties, evaluate_simple_expr, labels_data_type,
};
use crate::query::df_graph::scan::resolve_property_type;

/// Maps a user-provided yield name to a canonical name.
///
/// - `vid`, `_vid` → `vid`
/// - `distance`, `dist`, `_distance` → `distance`
/// - `score`, `_score` → `score`
/// - `vector_score`, `fts_score`, `raw_score`, `rerank_score`,
///   `_rerank_score` → matching canonical
/// - anything else → `node` (treated as a node-variable yield, which
///   the planner expands into `<name>._vid + <name> + <name>._labels +
///   <name>.<prop>` columns when the target procedure has opted into
///   node-shaped yields via the `_yield_kind = node_vid_source` field
///   metadata tag on its signature).
pub(crate) fn map_yield_to_canonical(yield_name: &str) -> &'static str {
    match yield_name.to_lowercase().as_str() {
        "vid" | "_vid" => "vid",
        "distance" | "dist" | "_distance" => "distance",
        "score" | "_score" => "score",
        "vector_score" => "vector_score",
        "fts_score" => "fts_score",
        "raw_score" => "raw_score",
        "rerank_score" | "_rerank_score" => "rerank_score",
        _ => "node",
    }
}

/// Built-in procedure names that produce node-shaped yields. Mirrors
/// the runtime convention encoded via signature metadata
/// (`_yield_kind = node_vid_source`) — kept synchronized so planning
/// paths without a `PluginRegistry` in scope (variable-kind collection,
/// the simplified schema inferrer in `common.rs`) can still detect
/// node-yield procedures.
///
/// The authoritative source for new plugins is the field-metadata tag;
/// this list is only consulted by paths that can't reach the registry.
pub(crate) const NODE_YIELD_PROCEDURE_NAMES: &[&str] = &[
    "uni.vector.query",
    "uni.fts.query",
    "uni.search",
    // M5g — `uni.create.vNode` yields a typed Node column via the
    // same `_yield_kind = node_vid_source` mechanism, expanded by
    // `expand_node_yield_fields` into the canonical
    // `<n>._vid + <n> + <n>._labels + <n>.<prop>` tuple.
    "uni.create.vNode",
];

/// Returns `true` if `name` identifies a procedure whose plugin
/// signature declares a node-shaped yield (canonically the `vid` field
/// tagged with `_yield_kind = node_vid_source`).
pub(crate) fn is_node_yield_procedure_static(name: &str) -> bool {
    NODE_YIELD_PROCEDURE_NAMES.contains(&name)
}

/// Arrow type to assign a search-canonical yield name when the
/// procedure's signature doesn't declare it explicitly (e.g.
/// `YIELD distance` against `uni.fts.query`, which has no distance
/// metric — runtime emits null, planner still needs a type).
pub(crate) fn canonical_search_type(canonical: &str) -> DataType {
    match canonical {
        "distance" => DataType::Float64,
        "score" | "vector_score" | "fts_score" | "raw_score" | "rerank_score" => DataType::Float32,
        "vid" => DataType::Int64,
        _ => DataType::Utf8,
    }
}

/// Expand a node-shaped yield into the canonical column tuple:
/// `<name>._vid + <name> + <name>._labels + <name>.<prop>...`. The
/// property columns come from the planner-supplied `target_properties`
/// map (the set of properties accessed downstream of the procedure
/// call); property types are resolved from any matching label in the
/// schema since the procedure may emit vertices of any label.
fn expand_node_yield_fields(
    output_name: &str,
    target_properties: &HashMap<String, Vec<String>>,
    graph_ctx: &GraphExecutionContext,
    fields: &mut Vec<Field>,
) {
    fields.push(Field::new(
        format!("{}._vid", output_name),
        DataType::UInt64,
        false,
    ));
    fields.push(Field::new(output_name, DataType::Utf8, false));
    fields.push(Field::new(
        format!("{}._labels", output_name),
        labels_data_type(),
        true,
    ));

    if let Some(props) = target_properties.get(output_name) {
        let uni_schema = graph_ctx.storage().schema_manager().schema();
        for prop_name in props {
            let col_name = format!("{}.{}", output_name, prop_name);
            let arrow_type = resolve_property_type(prop_name, None);
            let resolved_type = uni_schema
                .properties
                .values()
                .find_map(|label_props| {
                    label_props
                        .get(prop_name.as_str())
                        .map(|_| resolve_property_type(prop_name, Some(label_props)))
                })
                .unwrap_or(arrow_type);
            fields.push(Field::new(&col_name, resolved_type, true));
        }
    }
}

/// Build an output `Field` for a yield based on a signature field, using
/// the user's output name (alias or yield name) and preserving the
/// signature field's data type, nullability, and metadata.
fn field_from_signature(col_name: &str, sig_field: &Field) -> Field {
    let mut new_field = Field::new(
        col_name,
        sig_field.data_type().clone(),
        sig_field.is_nullable(),
    );
    if !sig_field.metadata().is_empty() {
        new_field = new_field.with_metadata(sig_field.metadata().clone());
    }
    new_field
}

/// Procedure call execution plan for DataFusion.
///
/// Executes Cypher CALL procedures (schema introspection, vector search, FTS, etc.)
/// and emits results as Arrow RecordBatches.
pub struct GraphProcedureCallExec {
    /// Graph execution context for storage access.
    graph_ctx: Arc<GraphExecutionContext>,

    /// Fully qualified procedure name (e.g. "uni.schema.labels").
    procedure_name: String,

    /// Argument expressions from the CALL clause.
    arguments: Vec<Expr>,

    /// Yield items: (original_name, optional_alias).
    yield_items: Vec<(String, Option<String>)>,

    /// Query parameters for expression evaluation.
    params: HashMap<String, Value>,

    /// Outer values from correlated context (e.g. MATCH variables).
    outer_values: HashMap<String, Value>,

    /// Target properties per variable (for node-like yields).
    target_properties: HashMap<String, Vec<String>>,

    /// Output schema.
    schema: SchemaRef,

    /// Plan properties.
    properties: Arc<PlanProperties>,

    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for GraphProcedureCallExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphProcedureCallExec")
            .field("procedure_name", &self.procedure_name)
            .field("yield_items", &self.yield_items)
            .finish()
    }
}

impl GraphProcedureCallExec {
    /// Create a new procedure call execution plan.
    pub fn new(
        graph_ctx: Arc<GraphExecutionContext>,
        procedure_name: String,
        arguments: Vec<Expr>,
        yield_items: Vec<(String, Option<String>)>,
        params: HashMap<String, Value>,
        outer_values: HashMap<String, Value>,
        target_properties: HashMap<String, Vec<String>>,
    ) -> Self {
        let schema = Self::build_schema(
            &procedure_name,
            &yield_items,
            &target_properties,
            &graph_ctx,
        );
        let properties = compute_plan_properties(schema.clone());

        Self {
            graph_ctx,
            procedure_name,
            arguments,
            yield_items,
            params,
            outer_values,
            target_properties,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Build the output schema based on the procedure's plugin signature
    /// and the user's YIELD clause.
    ///
    /// Lookup order:
    /// 1. Plugin path — `ProcedureRegistry::resolve_user_procedure`. When
    ///    the plugin opts into node-shaped yields by tagging a signature
    ///    field with `_yield_kind = node_vid_source` (canonically the
    ///    `vid` field on search procs), unknown yield names are canonical-
    ///    aliased via [`map_yield_to_canonical`] and the canonical `node`
    ///    case expands into the property-bearing column tuple.
    /// 2. Legacy TCK mock-registry path — for procedure shells whose
    ///    output types still come from the `proc_def.outputs` table.
    /// 3. Unknown / void — empty schema or Utf8 fallback columns.
    fn build_schema(
        procedure_name: &str,
        yield_items: &[(String, Option<String>)],
        target_properties: &HashMap<String, Vec<String>>,
        graph_ctx: &GraphExecutionContext,
    ) -> SchemaRef {
        let mut fields = Vec::new();

        if let Some(registry) = graph_ctx.procedure_registry()
            && let Some(entry) = registry.resolve_user_procedure(procedure_name)
        {
            let supports_node_yield = entry.signature.yields.iter().any(|f| {
                f.metadata()
                    .get("_yield_kind")
                    .is_some_and(|v| v == "node_vid_source")
            });

            for (yield_name, alias) in yield_items {
                let col_name = alias.as_ref().unwrap_or(yield_name);

                if supports_node_yield {
                    let canonical = map_yield_to_canonical(yield_name);
                    if canonical == "node" {
                        expand_node_yield_fields(
                            col_name,
                            target_properties,
                            graph_ctx,
                            &mut fields,
                        );
                        continue;
                    }
                    // Canonical aliasing (e.g. `_vid` → `vid`): look up the
                    // canonical name in the signature first, then fall back
                    // to the standard search-canonical type table for
                    // yields the proc doesn't declare (e.g. `distance`
                    // against `uni.fts.query`).
                    if let Some(sig_field) = entry
                        .signature
                        .yields
                        .iter()
                        .find(|f| f.name() == canonical)
                    {
                        fields.push(field_from_signature(col_name, sig_field));
                    } else {
                        fields.push(Field::new(col_name, canonical_search_type(canonical), true));
                    }
                    continue;
                }

                // Non-node-yield procedures: exact-name match against the
                // signature; Utf8 fallback if the user requested a yield
                // not declared by the plugin.
                let field = entry
                    .signature
                    .yields
                    .iter()
                    .find(|f| f.name() == yield_name.as_str())
                    .map(|f| field_from_signature(col_name, f))
                    .unwrap_or_else(|| Field::new(col_name, DataType::Utf8, true));
                fields.push(field);
            }
        } else if let Some(registry) = graph_ctx.procedure_registry()
            && let Some(proc_def) = registry.get(procedure_name)
        {
            for (name, alias) in yield_items {
                let col_name = alias.as_ref().unwrap_or(name);
                let data_type = proc_def
                    .outputs
                    .iter()
                    .find(|o| o.name == *name)
                    .map(|o| procedure_value_type_to_arrow(&o.output_type))
                    .unwrap_or(DataType::Utf8);
                fields.push(Field::new(col_name, data_type, true));
            }
        } else if yield_items.is_empty() {
            // Void procedure (no YIELD) — no output columns.
        } else {
            for (name, alias) in yield_items {
                let col_name = alias.as_ref().unwrap_or(name);
                fields.push(Field::new(col_name, DataType::Utf8, true));
            }
        }

        Arc::new(Schema::new(fields))
    }
}

/// Convert an algorithm `ValueType` to an Arrow `DataType`.
pub(crate) fn value_type_to_arrow(vt: &uni_algo::algo::procedures::ValueType) -> DataType {
    use uni_algo::algo::procedures::ValueType;
    match vt {
        ValueType::Int => DataType::Int64,
        ValueType::Float => DataType::Float64,
        ValueType::String => DataType::Utf8,
        ValueType::Bool => DataType::Boolean,
        ValueType::List
        | ValueType::Map
        | ValueType::Node
        | ValueType::Relationship
        | ValueType::Path
        | ValueType::Any => DataType::Utf8,
    }
}

/// Returns true if the ValueType is a complex type that should be JSON-encoded as Utf8
/// and tagged with `cv_encoded=true` metadata for downstream parsing.
pub(crate) fn is_complex_value_type(vt: &uni_algo::algo::procedures::ValueType) -> bool {
    use uni_algo::algo::procedures::ValueType;
    matches!(
        vt,
        ValueType::List
            | ValueType::Map
            | ValueType::Node
            | ValueType::Relationship
            | ValueType::Path
    )
}

/// Convert a `ProcedureValueType` to an Arrow `DataType`.
fn procedure_value_type_to_arrow(
    vt: &crate::query::executor::procedure::ProcedureValueType,
) -> DataType {
    use crate::query::executor::procedure::ProcedureValueType;
    match vt {
        ProcedureValueType::Integer => DataType::Int64,
        ProcedureValueType::Float | ProcedureValueType::Number => DataType::Float64,
        ProcedureValueType::Boolean => DataType::Boolean,
        ProcedureValueType::String | ProcedureValueType::Any => DataType::Utf8,
    }
}

impl DisplayAs for GraphProcedureCallExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GraphProcedureCallExec: procedure={}",
            self.procedure_name
        )
    }
}

impl ExecutionPlan for GraphProcedureCallExec {
    fn name(&self) -> &str {
        "GraphProcedureCallExec"
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Internal(
                "GraphProcedureCallExec has no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let metrics = BaselineMetrics::new(&self.metrics, partition);

        // Evaluate arguments upfront (outer_values provides MATCH-bound variables)
        let mut evaluated_args = Vec::with_capacity(self.arguments.len());
        for arg in &self.arguments {
            evaluated_args.push(evaluate_simple_expr(arg, &self.params, &self.outer_values)?);
        }

        Ok(Box::pin(ProcedureCallStream::new(
            self.graph_ctx.clone(),
            self.procedure_name.clone(),
            evaluated_args,
            self.yield_items.clone(),
            self.target_properties.clone(),
            self.schema.clone(),
            metrics,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

// ---------------------------------------------------------------------------
// Stream implementation
// ---------------------------------------------------------------------------

/// State machine for procedure call stream.
enum ProcedureCallState {
    /// Initial state, ready to start execution.
    Init,
    /// Executing the async procedure.
    Executing(Pin<Box<dyn std::future::Future<Output = DFResult<Option<RecordBatch>>> + Send>>),
    /// Stream is done.
    Done,
}

/// Stream that executes a procedure call.
struct ProcedureCallStream {
    graph_ctx: Arc<GraphExecutionContext>,
    procedure_name: String,
    evaluated_args: Vec<Value>,
    yield_items: Vec<(String, Option<String>)>,
    target_properties: HashMap<String, Vec<String>>,
    schema: SchemaRef,
    state: ProcedureCallState,
    metrics: BaselineMetrics,
}

impl ProcedureCallStream {
    fn new(
        graph_ctx: Arc<GraphExecutionContext>,
        procedure_name: String,
        evaluated_args: Vec<Value>,
        yield_items: Vec<(String, Option<String>)>,
        target_properties: HashMap<String, Vec<String>>,
        schema: SchemaRef,
        metrics: BaselineMetrics,
    ) -> Self {
        Self {
            graph_ctx,
            procedure_name,
            evaluated_args,
            yield_items,
            target_properties,
            schema,
            state: ProcedureCallState::Init,
            metrics,
        }
    }
}

impl Stream for ProcedureCallStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let metrics = self.metrics.clone();
        let _timer = metrics.elapsed_compute().timer();
        loop {
            let state = std::mem::replace(&mut self.state, ProcedureCallState::Done);

            match state {
                ProcedureCallState::Init => {
                    let graph_ctx = self.graph_ctx.clone();
                    let procedure_name = self.procedure_name.clone();
                    let evaluated_args = self.evaluated_args.clone();
                    let yield_items = self.yield_items.clone();
                    let target_properties = self.target_properties.clone();
                    let schema = self.schema.clone();

                    let fut = async move {
                        graph_ctx.check_timeout().map_err(|e| {
                            datafusion::error::DataFusionError::Execution(e.to_string())
                        })?;

                        execute_procedure(
                            &graph_ctx,
                            &procedure_name,
                            &evaluated_args,
                            &yield_items,
                            &target_properties,
                            &schema,
                        )
                        .await
                    };

                    self.state = ProcedureCallState::Executing(Box::pin(fut));
                }
                ProcedureCallState::Executing(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(batch)) => {
                        self.state = ProcedureCallState::Done;
                        self.metrics
                            .record_output(batch.as_ref().map(|b| b.num_rows()).unwrap_or(0));
                        return Poll::Ready(batch.map(Ok));
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = ProcedureCallState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.state = ProcedureCallState::Executing(fut);
                        return Poll::Pending;
                    }
                },
                ProcedureCallState::Done => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for ProcedureCallStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// ---------------------------------------------------------------------------
// Procedure execution dispatch
// ---------------------------------------------------------------------------

/// Execute a procedure and build a RecordBatch result.
///
/// **M4 dispatch order:**
/// 1. **Plugin path first** — consult the framework `PluginRegistry`
///    (via `ProcedureRegistry::resolve_user_procedure`). Procedures
///    registered through `BuiltinPlugin` / `ApocCorePlugin` / user
///    plugins are reachable here.
/// 2. **Legacy hardcoded dispatch** — for procedures not yet ported to
///    the plugin framework (`uni.schema.*`, `uni.vector.query`,
///    `uni.fts.query`, `uni.search`, `uni.algo.*`).
/// 3. **Legacy registered fallback** — the TCK's mock procedure
///    registry, kept until every test moves to the plugin path.
///
/// As procedures port to the plugin framework, the hardcoded dispatch
/// arms are deleted one namespace at a time. The legacy fallback
/// retires when the TCK migrates.
async fn execute_procedure(
    graph_ctx: &GraphExecutionContext,
    procedure_name: &str,
    args: &[Value],
    yield_items: &[(String, Option<String>)],
    target_properties: &HashMap<String, Vec<String>>,
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    // Plugin path — every built-in (`uni.schema.*`, `uni.algo.*`,
    // `uni.vector.query`, `uni.fts.query`, `uni.search`, APOC, …) is
    // registered through `PluginRegistry`. The only fallthrough is the
    // legacy TCK mock-procedure registry, kept until every test moves
    // to the plugin path.
    if let Some(registry) = graph_ctx.procedure_registry()
        && let Some(entry) = registry.resolve_user_procedure(procedure_name)
    {
        return execute_plugin_procedure(
            graph_ctx,
            procedure_name,
            &entry,
            args,
            yield_items,
            target_properties,
            schema,
        )
        .await;
    }

    execute_registered_procedure(graph_ctx, procedure_name, args, yield_items, schema).await
}

/// Execute a procedure via the plugin framework.
///
/// Drives the plugin's `invoke()` stream to completion and concatenates
/// the result into a single RecordBatch. Most procedures return a
/// single batch; multi-batch streams are concatenated. The schema of the
/// returned batch is whatever the plugin declared in its
/// [`uni_plugin::traits::procedure::ProcedureSignature::yields`]; the
/// caller-supplied `schema` is informational here since the plugin's
/// output schema is authoritative.
async fn execute_plugin_procedure(
    graph_ctx: &GraphExecutionContext,
    procedure_name: &str,
    entry: &uni_plugin::registry::ProcedureEntry,
    args: &[Value],
    yield_items: &[(String, Option<String>)],
    target_properties: &HashMap<String, Vec<String>>,
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    use datafusion::logical_expr::ColumnarValue;
    use futures::StreamExt;

    // Convert Cypher values into ColumnarValue scalars per the plugin's
    // declared signature. Currently a straightforward 1:1 mapping over
    // primitive types; richer Cypher types (Node/Edge/Path/Vector) flow
    // through `ArgType::CypherValue` once those plugin authoring forms
    // land.
    let mut columnar_args: Vec<ColumnarValue> = Vec::with_capacity(args.len());
    for v in args {
        columnar_args.push(value_to_columnar(v).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Procedure '{procedure_name}': argument conversion failed: {e}"
            ))
        })?);
    }

    let mut host =
        crate::query::executor::procedure_host::QueryProcedureHost::from_graph_ctx_with_request(
            graph_ctx,
            target_properties.clone(),
            yield_items.to_vec(),
            Some(schema.clone()),
        );
    // FU-1 / M11 #6: attach the outer transaction's writer handle so
    // declared `WRITE`-mode procedures (synthesized by
    // `CypherProcedureSynthesizer`) can mutate via the write-enabled
    // inner-query host.
    if let Some(writer) = graph_ctx.writer() {
        host = host.with_writer(std::sync::Arc::clone(writer));
    }
    // FU-1: propagate the in-flight principal from the
    // `CURRENT_PRINCIPAL` task-local so capability gates can see the
    // authenticated user. Set by `Session` / `Transaction` execute
    // boundaries; `None` outside a scoped scope (low-level tests).
    // The host + principal -> ProcedureContext construction is
    // consolidated in `uni_plugin::host::build_procedure_context`.
    let principal = crate::current_principal();
    let ctx = uni_plugin::host::build_procedure_context(&host, principal.as_deref());
    let mut stream = entry.procedure.invoke(ctx, &columnar_args).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!("Procedure '{procedure_name}': {e}"))
    })?;

    // Collect every batch the plugin yields. For most procedures the
    // stream produces a single batch; this works for multi-batch streams
    // by concatenating.
    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(item) = stream.next().await {
        let batch = item.map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Procedure '{procedure_name}' stream error: {e}"
            ))
        })?;
        batches.push(batch);
    }

    if batches.is_empty() {
        // Procedure yielded no rows — return an empty batch with the
        // expected schema so downstream operators stay schema-coherent.
        return Ok(Some(create_empty_batch(schema.clone())?));
    }

    // If the plugin yielded multiple batches, concatenate them under the
    // first batch's schema. For now, single-batch is the common case.
    let plugin_schema = batches[0].schema();
    let combined = if batches.len() == 1 {
        batches.pop().unwrap()
    } else {
        arrow::compute::concat_batches(&plugin_schema, &batches).map_err(arrow_err)?
    };

    // Pass-through when the plugin already produced columns matching
    // the planner-expected schema (search procedures do this — they
    // expand `node` yields into `{alias}._vid` / `{alias}` etc.).
    if combined.schema().fields() == schema.fields() {
        return Ok(Some(combined));
    }

    // Project the requested yield columns. If the caller asked for a
    // subset (or different order), reproject; otherwise pass through.
    if yield_items.is_empty()
        || (yield_items.len() == combined.num_columns()
            && yield_items
                .iter()
                .zip(combined.schema().fields().iter())
                .all(|((name, _alias), field)| name == field.name()))
    {
        return Ok(Some(combined));
    }

    let mut projected_cols: Vec<ArrayRef> = Vec::with_capacity(yield_items.len());
    let mut projected_fields: Vec<Field> = Vec::with_capacity(yield_items.len());
    for (name, _alias) in yield_items {
        let idx = combined.schema().index_of(name).map_err(|_| {
            datafusion::error::DataFusionError::Execution(format!(
                "Procedure '{procedure_name}': YIELD column `{name}` not in plugin output schema"
            ))
        })?;
        projected_cols.push(combined.column(idx).clone());
        projected_fields.push(combined.schema().field(idx).clone());
    }
    let projected_schema = Arc::new(Schema::new(projected_fields));
    let projected = RecordBatch::try_new(projected_schema, projected_cols).map_err(arrow_err)?;
    Ok(Some(projected))
}

/// Convert a [`uni_common::Value`] into a DataFusion
/// [`datafusion::logical_expr::ColumnarValue`] scalar, suitable for
/// passing to a plugin procedure's `invoke()`.
pub(crate) fn value_to_columnar(
    v: &Value,
) -> Result<datafusion::logical_expr::ColumnarValue, String> {
    use datafusion::logical_expr::ColumnarValue;
    use datafusion::scalar::ScalarValue;

    let scalar = match v {
        Value::Null => ScalarValue::Null,
        Value::Bool(b) => ScalarValue::Boolean(Some(*b)),
        Value::Int(i) => ScalarValue::Int64(Some(*i)),
        Value::Float(f) => ScalarValue::Float64(Some(*f)),
        Value::String(s) => ScalarValue::Utf8(Some(s.clone())),
        Value::Bytes(b) => ScalarValue::Binary(Some(b.clone())),
        other => {
            // Encode complex Cypher values (List, Map, Vector, Node,
            // Edge, …) as LargeBinary JSON bytes so the plugin path can
            // forward them losslessly. Plugins that need to consume
            // these (e.g., the `uni.algo.*` adapter) deserialize back
            // to `serde_json::Value`. Scalars take the direct path
            // above so primitive-typed plugins stay zero-copy.
            let json = serde_json::to_vec(other)
                .map_err(|e| format!("plugin arg encoding failed for {other:?}: {e}"))?;
            ScalarValue::LargeBinary(Some(json))
        }
    };
    Ok(ColumnarValue::Scalar(scalar))
}

/// Build a typed Arrow column from an iterator of optional `Value`s.
///
/// Dispatches on `data_type` to build the appropriate Arrow array. For types
/// not explicitly handled (Utf8 fallback), values are stringified.
///
/// **M5g — Node/Edge logical types.** A `Value::Node` input encodes
/// into the planner's canonical Node-column tuple by way of the
/// procedure-call dispatcher's `expand_node_yield_fields` (plugins
/// drive that path directly — the dispatcher never funnels
/// `Value::Node` through this builder). Top-level `Struct(...)` outputs
/// (the canonical Edge struct) are emitted as `StructArray` rows by
/// decoding `Value::Edge` against the requested field set; foreign
/// struct shapes fall through to the Utf8 stringification.
pub(crate) fn build_typed_column<'a>(
    values: impl Iterator<Item = Option<&'a Value>>,
    num_rows: usize,
    data_type: &DataType,
) -> ArrayRef {
    match data_type {
        DataType::UInt64 => {
            let mut builder = arrow_array::builder::UInt64Builder::with_capacity(num_rows);
            for val in values {
                match val.and_then(uni_common::Value::as_u64) {
                    Some(u) => builder.append_value(u),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Struct(fields) if is_edge_struct_shape(fields) => {
            build_edge_struct_column(values, num_rows, fields)
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(num_rows);
            for val in values {
                match val.and_then(|v| v.as_i64()) {
                    Some(i) => builder.append_value(i),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(num_rows);
            for val in values {
                match val.and_then(|v| v.as_f64()) {
                    Some(f) => builder.append_value(f),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(num_rows);
            for val in values {
                match val.and_then(|v| v.as_bool()) {
                    Some(b) => builder.append_value(b),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        _ => {
            // Utf8 fallback: stringify values
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
            for val in values {
                match val {
                    Some(Value::String(s)) => builder.append_value(s),
                    Some(v) => builder.append_value(format!("{v}")),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
    }
}

/// Returns `true` if `fields` matches the canonical edge-struct shape
/// declared by `df_graph::common::edge_struct_fields()` — i.e. the
/// `(_eid, _type_name, _src, _dst, properties)` tuple emitted by
/// `uni.create.vEdge`. Recognised by field-name set so older callers
/// that pass the fields in a different order still match.
fn is_edge_struct_shape(fields: &arrow_schema::Fields) -> bool {
    let names: std::collections::HashSet<&str> = fields.iter().map(|f| f.name().as_str()).collect();
    names.contains("_eid")
        && names.contains("_type_name")
        && names.contains("_src")
        && names.contains("_dst")
        && names.contains("properties")
}

/// Build a `StructArray` column matching the canonical edge struct
/// shape from an iterator of `Option<&Value>`. Each input `Value::Edge`
/// supplies one row; non-Edge or null inputs become null rows in every
/// child field.
fn build_edge_struct_column<'a>(
    values: impl Iterator<Item = Option<&'a Value>>,
    _num_rows: usize,
    fields: &arrow_schema::Fields,
) -> ArrayRef {
    use arrow_array::builder::{LargeBinaryBuilder, StringBuilder, UInt64Builder};
    use uni_common::Value as V;

    let mut eid_b = UInt64Builder::new();
    let mut type_b = StringBuilder::new();
    let mut src_b = UInt64Builder::new();
    let mut dst_b = UInt64Builder::new();
    let mut props_b = LargeBinaryBuilder::new();
    let mut validity: Vec<bool> = Vec::new();

    for val in values {
        match val {
            Some(V::Edge(e)) => {
                eid_b.append_value(e.eid.as_u64());
                type_b.append_value(&e.edge_type);
                src_b.append_value(e.src.as_u64());
                dst_b.append_value(e.dst.as_u64());
                let props_value = V::Map(e.properties.clone());
                let bytes = uni_common::cypher_value_codec::encode(&props_value);
                props_b.append_value(&bytes);
                validity.push(true);
            }
            _ => {
                eid_b.append_null();
                type_b.append_null();
                src_b.append_null();
                dst_b.append_null();
                props_b.append_null();
                validity.push(false);
            }
        }
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(eid_b.finish()),
        Arc::new(type_b.finish()),
        Arc::new(src_b.finish()),
        Arc::new(dst_b.finish()),
        Arc::new(props_b.finish()),
    ];
    // Reorder arrays to match the field order declared by the caller.
    // The canonical order is (_eid, _type_name, _src, _dst, properties);
    // any caller that declared a different field order needs the
    // corresponding column re-aligned.
    let canonical: [&str; 5] = ["_eid", "_type_name", "_src", "_dst", "properties"];
    let mut ordered: Vec<ArrayRef> = Vec::with_capacity(fields.len());
    for f in fields.iter() {
        let idx = canonical
            .iter()
            .position(|n| *n == f.name().as_str())
            .expect("is_edge_struct_shape vetted these field names");
        ordered.push(arrays[idx].clone());
    }
    let nulls = arrow::buffer::NullBuffer::from(validity);
    Arc::new(
        arrow_array::StructArray::try_new(fields.clone(), ordered, Some(nulls))
            .expect("StructArray construction with vetted shape"),
    )
}

/// Create an empty RecordBatch for the given schema.
///
/// When a schema has zero fields, `RecordBatch::new_empty()` panics because it
/// cannot determine the row count from an empty array. This helper handles that
/// edge case by using `RecordBatchOptions::with_row_count(0)`.
pub(crate) fn create_empty_batch(schema: SchemaRef) -> DFResult<RecordBatch> {
    if schema.fields().is_empty() {
        let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(0));
        RecordBatch::try_new_with_options(schema, vec![], &options).map_err(arrow_err)
    } else {
        Ok(RecordBatch::new_empty(schema))
    }
}

// ---------------------------------------------------------------------------
// External/registered procedures
// ---------------------------------------------------------------------------

/// Execute an externally registered procedure (e.g., TCK test procedures).
///
/// Looks up the procedure in the `ProcedureRegistry`, evaluates arguments,
/// filters data rows by matching input columns, and projects output columns.
async fn execute_registered_procedure(
    graph_ctx: &GraphExecutionContext,
    procedure_name: &str,
    args: &[Value],
    yield_items: &[(String, Option<String>)],
    schema: &SchemaRef,
) -> DFResult<Option<RecordBatch>> {
    let registry = graph_ctx.procedure_registry().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(format!(
            "Procedure '{}' not supported in DataFusion engine (no procedure registry)",
            procedure_name
        ))
    })?;

    let proc_def = registry.get(procedure_name).ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(format!(
            "ProcedureNotFound: Unknown procedure '{}'",
            procedure_name
        ))
    })?;

    // Validate argument count
    if args.len() != proc_def.params.len() {
        return Err(datafusion::error::DataFusionError::Execution(format!(
            "InvalidNumberOfArguments: Procedure '{}' expects {} argument(s), got {}",
            proc_def.name,
            proc_def.params.len(),
            args.len()
        )));
    }

    // Validate argument types
    for (i, (arg_val, param)) in args.iter().zip(&proc_def.params).enumerate() {
        if !arg_val.is_null() && !check_proc_type_compatible(arg_val, &param.param_type) {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "InvalidArgumentType: Argument {} ('{}') of procedure '{}' has incompatible type",
                i, param.name, proc_def.name
            )));
        }
    }

    // Filter data rows: keep rows where input columns match the provided args
    let filtered: Vec<&HashMap<String, Value>> = proc_def
        .data
        .iter()
        .filter(|row| {
            for (param, arg_val) in proc_def.params.iter().zip(args) {
                if let Some(row_val) = row.get(&param.name)
                    && !proc_values_match(row_val, arg_val)
                {
                    return false;
                }
            }
            true
        })
        .collect();

    // If the procedure has no yield items (void procedure), return empty batch
    if yield_items.is_empty() {
        return Ok(Some(create_empty_batch(schema.clone())?));
    }

    if filtered.is_empty() {
        return Ok(Some(create_empty_batch(schema.clone())?));
    }

    // Project output columns based on yield items
    // We need to map yield names back to output column names in the procedure definition
    let num_rows = filtered.len();
    let mut columns: Vec<ArrayRef> = Vec::new();

    for (idx, (name, _alias)) in yield_items.iter().enumerate() {
        let field = schema.field(idx);
        let values = filtered.iter().map(|row| row.get(name.as_str()));
        columns.push(build_typed_column(values, num_rows, field.data_type()));
    }

    let batch = RecordBatch::try_new(schema.clone(), columns).map_err(arrow_err)?;
    Ok(Some(batch))
}

/// Checks whether a value is compatible with a procedure type (DF engine version).
fn check_proc_type_compatible(
    val: &Value,
    expected: &crate::query::executor::procedure::ProcedureValueType,
) -> bool {
    use crate::query::executor::procedure::ProcedureValueType;
    match expected {
        ProcedureValueType::Any => true,
        ProcedureValueType::String => val.is_string(),
        ProcedureValueType::Boolean => val.is_bool(),
        ProcedureValueType::Integer => val.is_i64(),
        ProcedureValueType::Float => val.is_f64() || val.is_i64(),
        ProcedureValueType::Number => val.is_number(),
    }
}

/// Checks whether two values match for input-column filtering (DF engine version).
fn proc_values_match(row_val: &Value, arg_val: &Value) -> bool {
    if arg_val.is_null() || row_val.is_null() {
        return arg_val.is_null() && row_val.is_null();
    }
    // Compare numbers by f64 to handle int/float cross-comparison
    if let (Some(a), Some(b)) = (row_val.as_f64(), arg_val.as_f64()) {
        return (a - b).abs() < f64::EPSILON;
    }
    row_val == arg_val
}

/// Convert a `serde_json::Value` to a `uni_common::Value` for column building.
pub(crate) fn json_to_value(jv: &serde_json::Value) -> Value {
    match jv {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        other => Value::String(other.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Shared search argument helpers
// ---------------------------------------------------------------------------

/// Extract a required string argument from the argument list at a given position.
pub(crate) fn require_string_arg(
    args: &[Value],
    index: usize,
    description: &str,
) -> DFResult<String> {
    args.get(index)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!("{description} must be a string"))
        })
}

/// Extract an optional filter string from the argument list.
/// Returns `None` if the argument is missing, null, or not a string.
pub(crate) fn extract_optional_filter(args: &[Value], index: usize) -> Option<String> {
    args.get(index).and_then(|v| {
        if v.is_null() {
            None
        } else {
            v.as_str().map(|s| s.to_string())
        }
    })
}
