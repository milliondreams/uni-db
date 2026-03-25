// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Apply (correlated subquery) execution plan for DataFusion.
//!
//! Implements `CALL { ... }` subqueries by executing the subquery once per
//! input row, injecting the input row's columns as parameters, and cross-joining
//! the results.
//!
//! # Semantics
//!
//! For each row from the input plan:
//! 1. Optionally filter via `input_filter`
//! 2. Inject the input row's columns as parameters
//! 3. Re-plan and execute the subquery with those parameters
//! 4. Cross-join: merge each subquery result row with the input row
//!
//! If input produces zero rows (after filtering), execute the subquery once
//! with the base parameters (standalone CALL support).

use crate::query::df_graph::GraphExecutionContext;
use crate::query::df_graph::common::{
    arrow_err, collect_all_partitions, compute_plan_properties, execute_subplan, extract_row_params,
};
use crate::query::planner::LogicalPlan;
use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder, UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::SessionContext;
use futures::Stream;
use parking_lot::RwLock;
use std::any::Any;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use uni_common::Value;
use uni_common::core::schema::Schema as UniSchema;
use uni_cypher::ast::{Expr, UnaryOp};
use uni_store::storage::manager::StorageManager;

/// Apply (correlated subquery) execution plan.
///
/// The input is pre-planned as a physical plan (executed directly).
/// The subquery is stored as a **logical** plan and re-planned per row at runtime
/// with correlated parameters injected.
/// Handles both `SubqueryCall` (no input_filter) and `Apply` (with input_filter).
pub struct GraphApplyExec {
    /// Physical plan for the driving input (e.g., MATCH scan).
    /// Pre-planned at construction time to preserve property context.
    input_exec: Arc<dyn ExecutionPlan>,

    /// Logical plan for the correlated subquery (re-planned per row).
    subquery_plan: LogicalPlan,

    /// Optional pre-filter applied to input rows before subquery execution.
    input_filter: Option<Expr>,

    /// Graph execution context shared with sub-planners.
    graph_ctx: Arc<GraphExecutionContext>,

    /// DataFusion session context.
    session_ctx: Arc<RwLock<SessionContext>>,

    /// Storage manager for creating sub-planners.
    storage: Arc<StorageManager>,

    /// Schema for label/edge type lookups.
    schema_info: Arc<UniSchema>,

    /// Query parameters.
    params: HashMap<String, Value>,

    /// Output schema (merged: input columns + subquery columns).
    output_schema: SchemaRef,

    /// Cached plan properties.
    properties: PlanProperties,

    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for GraphApplyExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphApplyExec")
            .field("has_input_filter", &self.input_filter.is_some())
            .finish()
    }
}

impl GraphApplyExec {
    /// Create a new Apply execution plan.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        input_exec: Arc<dyn ExecutionPlan>,
        subquery_plan: LogicalPlan,
        input_filter: Option<Expr>,
        graph_ctx: Arc<GraphExecutionContext>,
        session_ctx: Arc<RwLock<SessionContext>>,
        storage: Arc<StorageManager>,
        schema_info: Arc<UniSchema>,
        params: HashMap<String, Value>,
        output_schema: SchemaRef,
    ) -> Self {
        let properties = compute_plan_properties(output_schema.clone());

        Self {
            input_exec,
            subquery_plan,
            input_filter,
            graph_ctx,
            session_ctx,
            storage,
            schema_info,
            params,
            output_schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for GraphApplyExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GraphApplyExec: filter={}",
            if self.input_filter.is_some() {
                "yes"
            } else {
                "none"
            }
        )
    }
}

impl ExecutionPlan for GraphApplyExec {
    fn name(&self) -> &str {
        "GraphApplyExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // No physical children — sub-plans are re-planned at execution time
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Plan(
                "GraphApplyExec has no children".to_string(),
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

        let input_exec = self.input_exec.clone();
        let subquery_plan = self.subquery_plan.clone();
        let input_filter = self.input_filter.clone();
        let graph_ctx = self.graph_ctx.clone();
        let session_ctx = self.session_ctx.clone();
        let storage = self.storage.clone();
        let schema_info = self.schema_info.clone();
        let params = self.params.clone();
        let output_schema = self.output_schema.clone();

        let fut = async move {
            run_apply(
                input_exec,
                &subquery_plan,
                input_filter.as_ref(),
                &graph_ctx,
                &session_ctx,
                &storage,
                &schema_info,
                &params,
                &output_schema,
            )
            .await
        };

        Ok(Box::pin(ApplyStream {
            state: ApplyStreamState::Running(Box::pin(fut)),
            schema: self.output_schema.clone(),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

// ---------------------------------------------------------------------------
// Core apply logic
// ---------------------------------------------------------------------------

/// Convert record batches into row-oriented `HashMap<String, Value>` representation.
fn batches_to_row_maps(batches: &[RecordBatch]) -> Vec<HashMap<String, Value>> {
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(move |row_idx| extract_row_params(batch, row_idx))
        })
        .collect()
}

/// Evaluate a Cypher filter expression against a row.
///
/// Supports simple binary comparisons and boolean operations needed for
/// input_filter pushdown (e.g., `p.age > 30`, `p.status = 'active'`).
fn evaluate_filter(filter: &Expr, row: &HashMap<String, Value>) -> bool {
    match filter {
        Expr::BinaryOp { left, op, right } => {
            use uni_cypher::ast::BinaryOp;
            match op {
                BinaryOp::And => evaluate_filter(left, row) && evaluate_filter(right, row),
                BinaryOp::Or => evaluate_filter(left, row) || evaluate_filter(right, row),
                _ => {
                    let left_val = resolve_expr_value(left, row);
                    let right_val = resolve_expr_value(right, row);
                    evaluate_comparison(op, &left_val, &right_val)
                }
            }
        }
        Expr::UnaryOp {
            op: UnaryOp::Not,
            expr,
        } => !evaluate_filter(expr, row),
        _ => {
            // Treat any other expression as a truth test on its resolved value
            let val = resolve_expr_value(filter, row);
            val.as_bool().unwrap_or(false)
        }
    }
}

/// Resolve a simple expression to a Value using the row context.
fn resolve_expr_value(expr: &Expr, row: &HashMap<String, Value>) -> Value {
    match expr {
        Expr::Literal(lit) => lit.to_value(),
        Expr::Variable(name) => row.get(name).cloned().unwrap_or(Value::Null),
        Expr::Property(base_expr, key) => {
            if let Expr::Variable(var) = base_expr.as_ref() {
                // Look up "var.key" in the row map
                let col_name = format!("{}.{}", var, key);
                row.get(&col_name).cloned().unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        }
        _ => Value::Null,
    }
}

/// Compare two Values for ordering.
fn compare_values(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
        (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
        (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)),
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

/// Evaluate a binary comparison operator on two Values.
///
/// Handles equality (`Eq`, `NotEq`) directly and delegates ordering
/// comparisons (`Lt`, `LtEq`, `Gt`, `GtEq`) to [`compare_values`].
fn evaluate_comparison(op: &uni_cypher::ast::BinaryOp, left: &Value, right: &Value) -> bool {
    use std::cmp::Ordering;
    use uni_cypher::ast::BinaryOp;

    match op {
        BinaryOp::Eq => left == right,
        BinaryOp::NotEq => left != right,
        BinaryOp::Lt => compare_values(left, right) == Some(Ordering::Less),
        BinaryOp::LtEq => matches!(
            compare_values(left, right),
            Some(Ordering::Less | Ordering::Equal)
        ),
        BinaryOp::Gt => compare_values(left, right) == Some(Ordering::Greater),
        BinaryOp::GtEq => matches!(
            compare_values(left, right),
            Some(Ordering::Greater | Ordering::Equal)
        ),
        _ => false,
    }
}

/// Build a typed column from row maps using a builder and value extractor.
///
/// For each row, looks up `col_name`, applies `extract` to get an `Option<T>`,
/// and appends the value or null to the builder.
fn build_column<B, T>(
    rows: &[HashMap<String, Value>],
    col_name: &str,
    mut builder: B,
    extract: impl Fn(&Value) -> Option<T>,
) -> ArrayRef
where
    B: arrow_array::builder::ArrayBuilder,
    B: PrimitiveAppend<T>,
{
    for row in rows {
        match row.get(col_name).and_then(&extract) {
            Some(v) => builder.append_typed_value(v),
            None => builder.append_typed_null(),
        }
    }
    Arc::new(builder.finish_to_array())
}

/// Trait to abstract over typed append for primitive Arrow builders.
///
/// This avoids repeating the same get-value/convert/append-or-null pattern
/// for each numeric/boolean type in `rows_to_batch`.
trait PrimitiveAppend<T> {
    fn append_typed_value(&mut self, val: T);
    fn append_typed_null(&mut self);
    fn finish_to_array(self) -> ArrayRef;
}

macro_rules! impl_primitive_append {
    ($builder:ty, $native:ty, $array:ty) => {
        impl PrimitiveAppend<$native> for $builder {
            fn append_typed_value(&mut self, val: $native) {
                self.append_value(val);
            }
            fn append_typed_null(&mut self) {
                self.append_null();
            }
            fn finish_to_array(mut self) -> ArrayRef {
                Arc::new(self.finish()) as ArrayRef
            }
        }
    };
}

impl_primitive_append!(UInt64Builder, u64, arrow_array::UInt64Array);
impl_primitive_append!(Int64Builder, i64, arrow_array::Int64Array);
impl_primitive_append!(Int32Builder, i32, arrow_array::Int32Array);
impl_primitive_append!(Float64Builder, f64, arrow_array::Float64Array);
impl_primitive_append!(BooleanBuilder, bool, arrow_array::BooleanArray);

/// Build a RecordBatch from merged row maps using the output schema.
fn rows_to_batch(rows: &[HashMap<String, Value>], schema: &SchemaRef) -> DFResult<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    let num_rows = rows.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let col_name = field.name();
        let col = match field.data_type() {
            DataType::UInt64 => build_column(
                rows,
                col_name,
                UInt64Builder::with_capacity(num_rows),
                |v| v.as_u64().or_else(|| v.as_i64().map(|i| i as u64)),
            ),
            DataType::Int64 => build_column(
                rows,
                col_name,
                Int64Builder::with_capacity(num_rows),
                Value::as_i64,
            ),
            DataType::Int32 => {
                build_column(rows, col_name, Int32Builder::with_capacity(num_rows), |v| {
                    v.as_i64().map(|i| i as i32)
                })
            }
            DataType::Float64 => build_column(
                rows,
                col_name,
                Float64Builder::with_capacity(num_rows),
                Value::as_f64,
            ),
            DataType::Boolean => build_column(
                rows,
                col_name,
                BooleanBuilder::with_capacity(num_rows),
                Value::as_bool,
            ),
            DataType::LargeBinary => {
                let mut builder = arrow_array::builder::LargeBinaryBuilder::with_capacity(
                    num_rows,
                    num_rows * 64,
                );
                for row in rows {
                    match row.get(col_name) {
                        Some(val) if !val.is_null() => {
                            let cv_bytes = uni_common::cypher_value_codec::encode(val);
                            builder.append_value(&cv_bytes);
                        }
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            DataType::List(inner_field) if inner_field.data_type() == &DataType::Utf8 => {
                let mut builder = arrow_array::builder::ListBuilder::new(StringBuilder::new());
                for row in rows {
                    match row.get(col_name) {
                        Some(Value::List(items)) => {
                            for item in items {
                                match item {
                                    Value::String(s) => builder.values().append_value(s),
                                    Value::Null => builder.values().append_null(),
                                    other => builder.values().append_value(format!("{other}")),
                                }
                            }
                            builder.append(true);
                        }
                        _ => builder.append_null(),
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            DataType::Null => Arc::new(arrow_array::NullArray::new(num_rows)) as ArrayRef,
            // Default: Utf8 for everything else
            _ => {
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
                for row in rows {
                    match row.get(col_name) {
                        Some(Value::Null) | None => builder.append_null(),
                        Some(Value::String(s)) => builder.append_value(s),
                        Some(other) => builder.append_value(format!("{other}")),
                    }
                }
                Arc::new(builder.finish()) as ArrayRef
            }
        };
        columns.push(col);
    }

    RecordBatch::try_new(schema.clone(), columns).map_err(arrow_err)
}

/// Slice a single row from a RecordBatch, preserving Arrow types.
fn slice_row(batch: &RecordBatch, row_idx: usize) -> Vec<ArrayRef> {
    batch
        .columns()
        .iter()
        .map(|col| col.slice(row_idx, 1))
        .collect()
}

/// Check if a logical plan is or contains a ProcedureCall node.
/// This helps distinguish procedure calls (CALL...YIELD) from regular subqueries (CALL { ... }).
fn is_procedure_call(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::ProcedureCall { .. } => true,
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input } => is_procedure_call(input),
        _ => false,
    }
}

/// Compute a hash for row parameters to enable deduplication.
///
/// Sorts entries by key for deterministic hashing regardless of iteration order.
fn hash_row_params(params: &HashMap<String, Value>) -> u64 {
    let mut hasher = DefaultHasher::new();
    let mut entries: Vec<_> = params.iter().collect();
    entries.sort_unstable_by_key(|(k, _)| *k);
    for (key, val) in entries {
        key.hash(&mut hasher);
        format!("{val:?}").hash(&mut hasher);
    }
    hasher.finish()
}

/// Check if batching is eligible for this apply operation.
/// Returns true if:
/// - There are 2+ filtered entries (single row → existing path)
/// - At least one `._vid` correlation key exists
fn is_batch_eligible(filtered_entries: &[(&RecordBatch, usize, HashMap<String, Value>)]) -> bool {
    if filtered_entries.len() < 2 {
        return false;
    }

    // Check if at least one correlation key (._vid) exists
    filtered_entries
        .iter()
        .any(|(_, _, row_params)| row_params.keys().any(|k| k.ends_with("._vid")))
}

/// Run the apply operation: execute input, filter, correlate subquery, merge results.
///
/// Uses Arrow-native row slicing for input columns to preserve complex types
/// (Struct, List, etc.), and only converts to Value for parameter injection.
#[expect(clippy::too_many_arguments)]
async fn run_apply(
    input_exec: Arc<dyn ExecutionPlan>,
    subquery_plan: &LogicalPlan,
    input_filter: Option<&Expr>,
    graph_ctx: &Arc<GraphExecutionContext>,
    session_ctx: &Arc<RwLock<SessionContext>>,
    storage: &Arc<StorageManager>,
    schema_info: &Arc<UniSchema>,
    params: &HashMap<String, Value>,
    output_schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    let apply_start = std::time::Instant::now();
    let is_proc_call = is_procedure_call(subquery_plan);
    tracing::debug!("run_apply: is_procedure_call={}", is_proc_call);

    // 1. Execute pre-planned input physical plan directly
    let task_ctx = session_ctx.read().task_ctx();
    let input_batches = collect_all_partitions(&input_exec, task_ctx).await?;

    // 2. Collect (batch_ref, row_idx) for rows that pass the input filter,
    //    along with their Value-based params for subquery injection.
    let mut filtered_entries: Vec<(&RecordBatch, usize, HashMap<String, Value>)> = Vec::new();
    for batch in &input_batches {
        for row_idx in 0..batch.num_rows() {
            let row_params = extract_row_params(batch, row_idx);
            if let Some(filter) = input_filter
                && !evaluate_filter(filter, &row_params)
            {
                continue;
            }
            filtered_entries.push((batch, row_idx, row_params));
        }
    }

    tracing::debug!(
        "run_apply: filtered_entries count = {}",
        filtered_entries.len()
    );

    // 3. Handle empty input: execute subquery once with base params
    if filtered_entries.is_empty() {
        let sub_batches = execute_subplan(
            subquery_plan,
            params,
            &HashMap::new(), // No outer values for empty input case
            graph_ctx,
            session_ctx,
            storage,
            schema_info,
        )
        .await?;
        let sub_rows = batches_to_row_maps(&sub_batches);
        return rows_to_batch(&sub_rows, output_schema);
    }

    // 4. Check if we can batch the subplan execution
    // IMPORTANT: Only batch when NOT a procedure call AND has input_filter.
    // - Procedure calls use outer_values (not params), incompatible with batching
    // - No input_filter indicates CALL subquery (e.g., MATCH (p) CALL { MATCH (p) })
    //   which requires per-row correlation, not batching
    // - Target pattern: procedure call → Apply with filter → MATCH traversal
    let has_filter = input_filter.is_some();

    if is_batch_eligible(&filtered_entries) && !is_proc_call && has_filter {
        tracing::debug!("run_apply: batching eligible, attempting batch execution");

        // Collect unique VID values and build batched params
        let mut vid_values: HashMap<String, Vec<Value>> = HashMap::new();
        for (_, _, row_params) in &filtered_entries {
            for (key, value) in row_params {
                if key.ends_with("._vid") {
                    vid_values
                        .entry(key.clone())
                        .or_default()
                        .push(value.clone());
                }
            }
        }

        // Build batched params: VID keys become Value::List
        let mut batched_params = params.clone();
        for (key, values) in &vid_values {
            batched_params.insert(key.clone(), Value::List(values.clone()));
        }

        // Add carry-through parameters from first row (for literals in projections)
        // These won't affect the WHERE filter but ensure planning succeeds
        if let Some((_, _, first_row_params)) = filtered_entries.first() {
            for (key, value) in first_row_params {
                if !key.ends_with("._vid") {
                    batched_params
                        .entry(key.clone())
                        .or_insert_with(|| value.clone());
                }
            }
        }

        // Execute subquery ONCE with batched VID params
        let subplan_start = std::time::Instant::now();
        let sub_batches = execute_subplan(
            subquery_plan,
            &batched_params,
            &HashMap::new(),
            graph_ctx,
            session_ctx,
            storage,
            schema_info,
        )
        .await?;
        let subplan_elapsed = subplan_start.elapsed();
        tracing::debug!(
            "run_apply: batch execute_subplan took {:?}",
            subplan_elapsed
        );

        // Build hash index: VID → Vec<subquery result rows>
        let sub_rows = batches_to_row_maps(&sub_batches);
        let mut sub_index: HashMap<i64, Vec<&HashMap<String, Value>>> = HashMap::new();

        // Find the VID key (should be the same for all rows)
        let vid_key = vid_values.keys().next().expect("at least one VID key");

        for sub_row in &sub_rows {
            if let Some(Value::Int(vid)) = sub_row.get(vid_key) {
                sub_index.entry(*vid).or_default().push(sub_row);
            }
        }

        // Hash-join: for each input row, look up by VID, emit input+subquery columns
        let input_schema = input_batches[0].schema();
        let num_input_cols = input_schema.fields().len();
        let num_output_cols = output_schema.fields().len();
        let mut column_arrays: Vec<Vec<ArrayRef>> = vec![Vec::new(); num_output_cols];

        for (batch, row_idx, row_params) in &filtered_entries {
            // Extract VID from row params
            let input_vid = if let Some(Value::Int(vid)) = row_params.get(vid_key) {
                *vid
            } else {
                continue; // Skip if VID is not present
            };

            // Look up matching subquery rows by VID
            if let Some(matching_sub_rows) = sub_index.get(&input_vid) {
                let input_row_arrays = slice_row(batch, *row_idx);

                for sub_row in matching_sub_rows {
                    append_cross_join_row(
                        &mut column_arrays,
                        &input_row_arrays,
                        sub_row,
                        output_schema,
                        num_input_cols,
                    )?;
                }
            }
            // else: inner join — skip input row (no subquery matches)
        }

        let result = concat_column_arrays(&column_arrays, output_schema);

        let apply_elapsed = apply_start.elapsed();
        tracing::debug!(
            "run_apply: completed (batched) in {:?}, 1 subplan execution",
            apply_elapsed
        );

        return result;
    }

    // 5. Fallback: For each input row, execute subquery and collect output column arrays.
    //    Used when batching is not eligible (single row, no VID keys, or procedure call).
    //    Each output row is: input columns (sliced) + subquery columns (sliced).
    let input_schema = input_batches[0].schema();
    let num_input_cols = input_schema.fields().len();
    let num_output_cols = output_schema.fields().len();
    // Accumulate per-column arrays for all output rows
    let mut column_arrays: Vec<Vec<ArrayRef>> = vec![Vec::new(); num_output_cols];

    let mut total_subplan_time = std::time::Duration::ZERO;
    let mut subplan_executions = 0;

    // Cache to deduplicate subplan executions for identical row parameters
    let mut subplan_cache: HashMap<u64, Vec<HashMap<String, Value>>> = HashMap::new();
    let mut cache_hits = 0;

    for (batch, row_idx, row_params) in &filtered_entries {
        // For procedure calls (CALL...YIELD), pass row_params as outer_values to avoid
        // shadowing user parameters. For regular subqueries (CALL { ... }), merge them
        // into parameters for backward compatibility with correlated variables.
        let (sub_params, sub_outer_values) = if is_procedure_call(subquery_plan) {
            // Procedure call: keep params separate from outer values
            (params.clone(), row_params.clone())
        } else {
            // Regular subquery: merge outer values into params (old behavior)
            let mut merged = params.clone();
            merged.extend(row_params.clone());
            (merged, HashMap::new())
        };

        // Check cache for identical row params
        let params_hash = hash_row_params(row_params);
        let sub_rows = if let Some(cached_rows) = subplan_cache.get(&params_hash) {
            // Cache hit: reuse previous results
            cache_hits += 1;
            tracing::debug!(
                "run_apply: cache hit for params hash {}, skipping execute_subplan",
                params_hash
            );
            cached_rows.clone()
        } else {
            // Cache miss: execute subplan
            let subplan_start = std::time::Instant::now();
            let sub_batches = execute_subplan(
                subquery_plan,
                &sub_params,
                &sub_outer_values,
                graph_ctx,
                session_ctx,
                storage,
                schema_info,
            )
            .await?;
            let subplan_elapsed = subplan_start.elapsed();
            total_subplan_time += subplan_elapsed;
            subplan_executions += 1;

            tracing::debug!(
                "run_apply: execute_subplan #{} took {:?}",
                subplan_executions,
                subplan_elapsed
            );

            let rows = batches_to_row_maps(&sub_batches);
            subplan_cache.insert(params_hash, rows.clone());
            rows
        };

        let input_row_arrays = slice_row(batch, *row_idx);

        if sub_rows.is_empty() {
            // No subquery results — skip this input row (inner join semantics)
            continue;
        }

        for sub_row in &sub_rows {
            append_cross_join_row(
                &mut column_arrays,
                &input_row_arrays,
                sub_row,
                output_schema,
                num_input_cols,
            )?;
        }
    }

    // 5. Concatenate all accumulated arrays per column
    let result = concat_column_arrays(&column_arrays, output_schema);

    let apply_elapsed = apply_start.elapsed();
    tracing::debug!(
        "run_apply: completed in {:?}, {} subplan executions, {} cache hits, {:?} total subplan time",
        apply_elapsed,
        subplan_executions,
        cache_hits,
        total_subplan_time
    );

    result
}

/// Build a single-row Arrow array from a builder and optional value.
fn single_row_array<B, T>(mut builder: B, val: Option<T>) -> ArrayRef
where
    B: PrimitiveAppend<T>,
{
    match val {
        Some(v) => builder.append_typed_value(v),
        None => builder.append_typed_null(),
    }
    builder.finish_to_array()
}

/// Convert a single Value to a single-row Arrow array of the given type.
fn value_to_single_row_array(val: &Value, data_type: &DataType) -> DFResult<ArrayRef> {
    Ok(match data_type {
        DataType::UInt64 => single_row_array(
            UInt64Builder::with_capacity(1),
            val.as_u64().or_else(|| val.as_i64().map(|v| v as u64)),
        ),
        DataType::Int64 => single_row_array(Int64Builder::with_capacity(1), val.as_i64()),
        DataType::Int32 => single_row_array(
            Int32Builder::with_capacity(1),
            val.as_i64().map(|v| v as i32),
        ),
        DataType::Float64 => single_row_array(Float64Builder::with_capacity(1), val.as_f64()),
        DataType::Boolean => single_row_array(BooleanBuilder::with_capacity(1), val.as_bool()),
        DataType::Null => Arc::new(arrow_array::NullArray::new(1)) as ArrayRef,
        _ => {
            let mut b = StringBuilder::with_capacity(1, 64);
            match val {
                Value::Null => b.append_null(),
                Value::String(s) => b.append_value(s),
                other => b.append_value(format!("{other}")),
            }
            Arc::new(b.finish()) as ArrayRef
        }
    })
}

/// Append one cross-joined row (input + subquery) to the per-column accumulator.
///
/// For input columns, uses the Arrow-native sliced arrays to preserve complex types.
/// For subquery columns, converts `Value` to single-row Arrow arrays.
fn append_cross_join_row(
    column_arrays: &mut [Vec<ArrayRef>],
    input_row_arrays: &[ArrayRef],
    sub_row: &HashMap<String, Value>,
    output_schema: &SchemaRef,
    num_input_cols: usize,
) -> DFResult<()> {
    // Add input columns (Arrow-native, preserves types)
    for (col_idx, arr) in input_row_arrays.iter().enumerate() {
        column_arrays[col_idx].push(arr.clone());
    }

    // Add subquery columns using Value -> Arrow conversion
    let num_output_cols = output_schema.fields().len();
    for (col_arr, field) in column_arrays[num_input_cols..num_output_cols]
        .iter_mut()
        .zip(output_schema.fields()[num_input_cols..num_output_cols].iter())
    {
        let col_name = field.name();
        let val = sub_row.get(col_name).cloned().unwrap_or(Value::Null);
        let arr = value_to_single_row_array(&val, field.data_type())?;
        col_arr.push(arr);
    }
    Ok(())
}

/// Concatenate per-column array accumulators into a single `RecordBatch`.
///
/// Returns an empty batch if no rows were accumulated.
fn concat_column_arrays(
    column_arrays: &[Vec<ArrayRef>],
    output_schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    if column_arrays[0].is_empty() {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    let mut final_columns: Vec<ArrayRef> = Vec::with_capacity(column_arrays.len());
    for arrays in column_arrays {
        let refs: Vec<&dyn arrow_array::Array> = arrays.iter().map(|a| a.as_ref()).collect();
        let concatenated = arrow::compute::concat(&refs).map_err(arrow_err)?;
        final_columns.push(concatenated);
    }

    RecordBatch::try_new(output_schema.clone(), final_columns).map_err(arrow_err)
}

// ---------------------------------------------------------------------------
// Stream implementation
// ---------------------------------------------------------------------------

/// Stream state for the apply operation.
enum ApplyStreamState {
    /// The apply computation is running.
    Running(Pin<Box<dyn std::future::Future<Output = DFResult<RecordBatch>> + Send>>),
    /// Computation completed.
    Done,
}

/// Stream that runs the apply operation and emits the result.
struct ApplyStream {
    state: ApplyStreamState,
    schema: SchemaRef,
    metrics: BaselineMetrics,
}

impl Stream for ApplyStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.state {
            ApplyStreamState::Running(fut) => match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(batch)) => {
                    self.metrics.record_output(batch.num_rows());
                    self.state = ApplyStreamState::Done;
                    Poll::Ready(Some(Ok(batch)))
                }
                Poll::Ready(Err(e)) => {
                    self.state = ApplyStreamState::Done;
                    Poll::Ready(Some(Err(e)))
                }
                Poll::Pending => Poll::Pending,
            },
            ApplyStreamState::Done => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for ApplyStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
