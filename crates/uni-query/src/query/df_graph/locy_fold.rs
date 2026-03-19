// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! FOLD operator for Locy.
//!
//! `FoldExec` applies fold (lattice-join) semantics: for each group of rows sharing
//! the same KEY columns, it reduces non-key columns via their declared fold functions.

use crate::query::df_graph::common::{ScalarKey, compute_plan_properties, extract_scalar_key};
use arrow_array::builder::{Float64Builder, Int64Builder, LargeBinaryBuilder};
use arrow_array::{Array, Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, TryStreamExt};
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Direction of monotonicity for a fold aggregate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MonotonicDirection {
    /// Value can only stay the same or increase across iterations.
    NonDecreasing,
    /// Value can only stay the same or decrease across iterations.
    NonIncreasing,
}

/// Aggregate function kind for FOLD bindings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FoldAggKind {
    Sum,
    Max,
    Min,
    Count,
    Avg,
    Collect,
    Nor,  // Noisy-OR: 1 − ∏(1 − pᵢ)
    Prod, // Product:  ∏ pᵢ
}

impl FoldAggKind {
    /// Returns `true` if this aggregate is monotonic (safe for fixpoint iteration).
    pub fn is_monotonic(&self) -> bool {
        matches!(
            self,
            Self::Sum | Self::Max | Self::Min | Self::Count | Self::Nor | Self::Prod
        )
    }

    /// Returns the monotonicity direction, or `None` for non-monotonic aggregates.
    pub fn monotonicity_direction(&self) -> Option<MonotonicDirection> {
        match self {
            Self::Sum | Self::Max | Self::Count | Self::Nor => {
                Some(MonotonicDirection::NonDecreasing)
            }
            Self::Min | Self::Prod => Some(MonotonicDirection::NonIncreasing),
            Self::Avg | Self::Collect => None,
        }
    }

    /// Returns the identity element for this aggregate, or `None` for non-monotonic aggregates.
    pub fn identity(&self) -> Option<f64> {
        match self {
            Self::Sum | Self::Count | Self::Nor => Some(0.0),
            Self::Max => Some(f64::NEG_INFINITY),
            Self::Min => Some(f64::INFINITY),
            Self::Prod => Some(1.0),
            Self::Avg | Self::Collect => None,
        }
    }
}

/// A single FOLD binding: aggregate an input column into an output column.
#[derive(Debug, Clone)]
pub struct FoldBinding {
    pub output_name: String,
    pub kind: FoldAggKind,
    pub input_col_index: usize,
}

/// DataFusion `ExecutionPlan` that applies FOLD semantics.
///
/// Groups rows by KEY columns and computes aggregates (SUM, MAX, MIN, COUNT, AVG, COLLECT)
/// for each fold binding. Output schema is KEY columns + fold output columns.
#[derive(Debug)]
pub struct FoldExec {
    input: Arc<dyn ExecutionPlan>,
    key_indices: Vec<usize>,
    fold_bindings: Vec<FoldBinding>,
    strict_probability_domain: bool,
    probability_epsilon: f64,
    schema: SchemaRef,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl FoldExec {
    /// Create a new `FoldExec`.
    ///
    /// # Arguments
    /// * `input` - Child execution plan
    /// * `key_indices` - Indices of KEY columns for grouping
    /// * `fold_bindings` - Aggregate bindings (output name, kind, input col index)
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        key_indices: Vec<usize>,
        fold_bindings: Vec<FoldBinding>,
        strict_probability_domain: bool,
        probability_epsilon: f64,
    ) -> Self {
        let input_schema = input.schema();
        let schema = Self::build_output_schema(&input_schema, &key_indices, &fold_bindings);
        let properties = compute_plan_properties(Arc::clone(&schema));

        Self {
            input,
            key_indices,
            fold_bindings,
            strict_probability_domain,
            probability_epsilon,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    fn build_output_schema(
        input_schema: &SchemaRef,
        key_indices: &[usize],
        fold_bindings: &[FoldBinding],
    ) -> SchemaRef {
        let mut fields = Vec::new();

        // Key columns preserve original types
        for &ki in key_indices {
            fields.push(Arc::new(input_schema.field(ki).clone()));
        }

        // Fold output columns
        for binding in fold_bindings {
            let input_type = input_schema.field(binding.input_col_index).data_type();
            let output_type = match binding.kind {
                FoldAggKind::Sum | FoldAggKind::Avg | FoldAggKind::Nor | FoldAggKind::Prod => {
                    DataType::Float64
                }
                FoldAggKind::Count => DataType::Int64,
                FoldAggKind::Max | FoldAggKind::Min => input_type.clone(),
                FoldAggKind::Collect => DataType::LargeBinary,
            };
            fields.push(Arc::new(Field::new(
                &binding.output_name,
                output_type,
                true,
            )));
        }

        Arc::new(Schema::new(fields))
    }
}

impl DisplayAs for FoldExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FoldExec: key_indices={:?}, bindings={:?}",
            self.key_indices, self.fold_bindings
        )
    }
}

impl ExecutionPlan for FoldExec {
    fn name(&self) -> &str {
        "FoldExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Plan(
                "FoldExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.key_indices.clone(),
            self.fold_bindings.clone(),
            self.strict_probability_domain,
            self.probability_epsilon,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, Arc::clone(&context))?;
        let metrics = BaselineMetrics::new(&self.metrics, partition);
        let key_indices = self.key_indices.clone();
        let fold_bindings = self.fold_bindings.clone();
        let strict = self.strict_probability_domain;
        let epsilon = self.probability_epsilon;
        let output_schema = Arc::clone(&self.schema);
        let input_schema = self.input.schema();

        let fut = async move {
            let batches: Vec<RecordBatch> = input_stream.try_collect().await?;

            if batches.is_empty() {
                return Ok(RecordBatch::new_empty(output_schema));
            }

            let batch = arrow::compute::concat_batches(&input_schema, &batches)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

            if batch.num_rows() == 0 {
                return Ok(RecordBatch::new_empty(output_schema));
            }

            // Group by key columns → row indices, preserving insertion order
            let mut groups: HashMap<Vec<ScalarKey>, Vec<usize>> = HashMap::new();
            let mut ordered_keys: Vec<Vec<ScalarKey>> = Vec::new();
            for row_idx in 0..batch.num_rows() {
                let key = extract_scalar_key(&batch, &key_indices, row_idx);
                let entry = groups.entry(key.clone());
                if matches!(entry, std::collections::hash_map::Entry::Vacant(_)) {
                    ordered_keys.push(key);
                }
                entry.or_default().push(row_idx);
            }

            let num_groups = ordered_keys.len();

            // Build output columns
            let mut output_columns: Vec<arrow_array::ArrayRef> = Vec::new();

            // Key columns: take from first row of each group
            for &ki in &key_indices {
                let col = batch.column(ki);
                let first_indices: Vec<u32> =
                    ordered_keys.iter().map(|k| groups[k][0] as u32).collect();
                let idx_array = arrow_array::UInt32Array::from(first_indices);
                let taken = arrow::compute::take(col.as_ref(), &idx_array, None).map_err(|e| {
                    datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                })?;
                output_columns.push(taken);
            }

            // Fold binding columns: compute aggregates per group
            for binding in &fold_bindings {
                let col = batch.column(binding.input_col_index);
                let agg_col = compute_fold_aggregate(
                    col.as_ref(),
                    &binding.kind,
                    &ordered_keys,
                    &groups,
                    num_groups,
                    strict,
                    epsilon,
                )?;
                output_columns.push(agg_col);
            }

            RecordBatch::try_new(output_schema, output_columns)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
        };

        Ok(Box::pin(FoldStream {
            state: FoldStreamState::Running(Box::pin(fut)),
            schema: Arc::clone(&self.schema),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

// ---------------------------------------------------------------------------
// Aggregate computation
// ---------------------------------------------------------------------------

fn compute_fold_aggregate(
    col: &dyn Array,
    kind: &FoldAggKind,
    ordered_keys: &[Vec<ScalarKey>],
    groups: &HashMap<Vec<ScalarKey>, Vec<usize>>,
    num_groups: usize,
    strict: bool,
    probability_epsilon: f64,
) -> DFResult<arrow_array::ArrayRef> {
    match kind {
        FoldAggKind::Sum => {
            let mut builder = Float64Builder::with_capacity(num_groups);
            for key in ordered_keys {
                builder.append_option(sum_f64(col, &groups[key]));
            }
            Ok(Arc::new(builder.finish()))
        }
        FoldAggKind::Count => {
            let mut builder = Int64Builder::with_capacity(num_groups);
            for key in ordered_keys {
                let indices = &groups[key];
                let count = indices.iter().filter(|&&i| !col.is_null(i)).count();
                builder.append_value(count as i64);
            }
            Ok(Arc::new(builder.finish()))
        }
        FoldAggKind::Max => compute_minmax(col, ordered_keys, groups, num_groups, false),
        FoldAggKind::Min => compute_minmax(col, ordered_keys, groups, num_groups, true),
        FoldAggKind::Avg => {
            let mut builder = Float64Builder::with_capacity(num_groups);
            for key in ordered_keys {
                let indices = &groups[key];
                let count = indices.iter().filter(|&&i| !col.is_null(i)).count();
                let avg = sum_f64(col, indices)
                    .filter(|_| count > 0)
                    .map(|s| s / count as f64);
                builder.append_option(avg);
            }
            Ok(Arc::new(builder.finish()))
        }
        FoldAggKind::Collect => {
            let mut builder = LargeBinaryBuilder::with_capacity(num_groups, num_groups * 32);
            for key in ordered_keys {
                let values: Vec<uni_common::Value> = groups[key]
                    .iter()
                    .filter(|&&i| !col.is_null(i))
                    .map(|&i| scalar_to_value(col, i))
                    .collect();
                let encoded =
                    uni_common::cypher_value_codec::encode(&uni_common::Value::List(values));
                builder.append_value(&encoded);
            }
            Ok(Arc::new(builder.finish()))
        }
        FoldAggKind::Nor => {
            let mut builder = Float64Builder::with_capacity(num_groups);
            for key in ordered_keys {
                let indices = &groups[key];
                builder.append_option(noisy_or_f64(col, indices, strict)?);
            }
            Ok(Arc::new(builder.finish()))
        }
        FoldAggKind::Prod => {
            let mut builder = Float64Builder::with_capacity(num_groups);
            for key in ordered_keys {
                builder.append_option(product_f64(col, &groups[key], strict, probability_epsilon)?);
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

fn sum_f64(col: &dyn Array, indices: &[usize]) -> Option<f64> {
    let mut sum = 0.0;
    let mut has_value = false;
    for &i in indices {
        if col.is_null(i) {
            continue;
        }
        has_value = true;
        if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
            sum += arr.value(i);
        } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            sum += arr.value(i) as f64;
        }
    }
    if has_value { Some(sum) } else { None }
}

/// Noisy-OR: P = 1 − ∏(1 − pᵢ). Inputs clamped to [0, 1] unless strict.
fn noisy_or_f64(col: &dyn Array, indices: &[usize], strict: bool) -> DFResult<Option<f64>> {
    let mut complement_product = 1.0;
    let mut has_value = false;
    for &i in indices {
        if col.is_null(i) {
            continue;
        }
        has_value = true;
        let raw = if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
            arr.value(i)
        } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            arr.value(i) as f64
        } else {
            continue;
        };
        if strict && !(0.0..=1.0).contains(&raw) {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "strict_probability_domain: MNOR input {raw} is outside [0, 1]"
            )));
        }
        if !strict && !(0.0..=1.0).contains(&raw) {
            tracing::warn!(
                "MNOR input {raw} outside [0,1], clamped to {}",
                raw.clamp(0.0, 1.0)
            );
        }
        let p = raw.clamp(0.0, 1.0);
        complement_product *= 1.0 - p;
    }
    if has_value {
        Ok(Some(1.0 - complement_product))
    } else {
        Ok(None)
    }
}

/// Product: P = ∏ pᵢ. Inputs clamped to [0, 1] unless strict.
///
/// Switches to log-space when the running product drops below
/// `probability_epsilon` to prevent floating-point underflow.
fn product_f64(
    col: &dyn Array,
    indices: &[usize],
    strict: bool,
    probability_epsilon: f64,
) -> DFResult<Option<f64>> {
    let mut product = 1.0;
    let mut log_sum = 0.0;
    let mut use_log = false;
    let mut has_value = false;
    for &i in indices {
        if col.is_null(i) {
            continue;
        }
        has_value = true;
        let raw = if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
            arr.value(i)
        } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            arr.value(i) as f64
        } else {
            continue;
        };
        if strict && !(0.0..=1.0).contains(&raw) {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "strict_probability_domain: MPROD input {raw} is outside [0, 1]"
            )));
        }
        if !strict && !(0.0..=1.0).contains(&raw) {
            tracing::warn!(
                "MPROD input {raw} outside [0,1], clamped to {}",
                raw.clamp(0.0, 1.0)
            );
        }
        let p = raw.clamp(0.0, 1.0);
        if p == 0.0 {
            return Ok(Some(0.0));
        }
        if use_log {
            log_sum += p.ln();
        } else {
            product *= p;
            if product < probability_epsilon {
                // Switch to log-space to prevent underflow
                log_sum = product.ln();
                use_log = true;
            }
        }
    }
    if !has_value {
        return Ok(None);
    }
    if use_log {
        Ok(Some(log_sum.exp()))
    } else {
        Ok(Some(product))
    }
}

fn compute_minmax(
    col: &dyn Array,
    ordered_keys: &[Vec<ScalarKey>],
    groups: &HashMap<Vec<ScalarKey>, Vec<usize>>,
    num_groups: usize,
    is_min: bool,
) -> DFResult<arrow_array::ArrayRef> {
    match col.data_type() {
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            let mut builder = Int64Builder::with_capacity(num_groups);
            for key in ordered_keys {
                let mut result: Option<i64> = None;
                for &i in &groups[key] {
                    if !arr.is_null(i) {
                        let v = arr.value(i);
                        result = Some(match result {
                            None => v,
                            Some(cur) if is_min => cur.min(v),
                            Some(cur) => cur.max(v),
                        });
                    }
                }
                builder.append_option(result);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            let mut builder = Float64Builder::with_capacity(num_groups);
            for key in ordered_keys {
                let mut result: Option<f64> = None;
                for &i in &groups[key] {
                    if !arr.is_null(i) {
                        let v = arr.value(i);
                        result = Some(match result {
                            None => v,
                            Some(cur) if is_min => cur.min(v),
                            Some(cur) => cur.max(v),
                        });
                    }
                }
                builder.append_option(result);
            }
            Ok(Arc::new(builder.finish()))
        }
        dt => {
            // Fallback: treat as string comparison.
            // Use LargeStringBuilder for LargeUtf8 input to match the output schema
            // (build_output_schema preserves the input type for MAX/MIN).
            let use_large = matches!(dt, DataType::LargeUtf8);
            let mut values: Vec<Option<String>> = Vec::with_capacity(num_groups);
            for key in ordered_keys {
                let indices = &groups[key];
                let mut result: Option<String> = None;
                for &i in indices {
                    if col.is_null(i) {
                        continue;
                    }
                    let v = format!("{:?}", scalar_to_value(col, i));
                    result = Some(match result {
                        None => v,
                        Some(cur) if is_min && v < cur => v,
                        Some(cur) if !is_min && v > cur => v,
                        Some(cur) => cur,
                    });
                }
                values.push(result);
            }
            Ok(build_optional_string_array(&values, use_large))
        }
    }
}

fn build_optional_string_array(
    values: &[Option<String>],
    use_large: bool,
) -> arrow_array::ArrayRef {
    if use_large {
        let mut builder = arrow_array::builder::LargeStringBuilder::new();
        for v in values {
            match v {
                Some(s) => builder.append_value(s),
                None => builder.append_null(),
            }
        }
        Arc::new(builder.finish())
    } else {
        let mut builder = arrow_array::builder::StringBuilder::new();
        for v in values {
            match v {
                Some(s) => builder.append_value(s),
                None => builder.append_null(),
            }
        }
        Arc::new(builder.finish())
    }
}

fn scalar_to_value(col: &dyn Array, row_idx: usize) -> uni_common::Value {
    if col.is_null(row_idx) {
        return uni_common::Value::Null;
    }
    match col.data_type() {
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            uni_common::Value::Int(arr.value(row_idx))
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            uni_common::Value::Float(arr.value(row_idx))
        }
        DataType::Utf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            uni_common::Value::String(arr.value(row_idx).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow_array::LargeStringArray>()
                .unwrap();
            uni_common::Value::String(arr.value(row_idx).to_string())
        }
        DataType::Boolean => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .unwrap();
            uni_common::Value::Bool(arr.value(row_idx))
        }
        DataType::LargeBinary => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow_array::LargeBinaryArray>()
                .unwrap();
            let bytes = arr.value(row_idx);
            uni_common::cypher_value_codec::decode(bytes).unwrap_or(uni_common::Value::Null)
        }
        _ => uni_common::Value::Null,
    }
}

// ---------------------------------------------------------------------------
// Stream implementation
// ---------------------------------------------------------------------------

enum FoldStreamState {
    Running(Pin<Box<dyn std::future::Future<Output = DFResult<RecordBatch>> + Send>>),
    Done,
}

struct FoldStream {
    state: FoldStreamState,
    schema: SchemaRef,
    metrics: BaselineMetrics,
}

impl Stream for FoldStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.state {
            FoldStreamState::Running(fut) => match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(batch)) => {
                    self.metrics.record_output(batch.num_rows());
                    self.state = FoldStreamState::Done;
                    Poll::Ready(Some(Ok(batch)))
                }
                Poll::Ready(Err(e)) => {
                    self.state = FoldStreamState::Done;
                    Poll::Ready(Some(Err(e)))
                }
                Poll::Pending => Poll::Pending,
            },
            FoldStreamState::Done => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for FoldStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::memory::MemoryStream;
    use datafusion::prelude::SessionContext;

    fn make_test_batch(names: Vec<&str>, values: Vec<f64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    names.into_iter().map(Some).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap()
    }

    fn make_memory_exec(batch: RecordBatch) -> Arc<dyn ExecutionPlan> {
        let schema = batch.schema();
        Arc::new(TestMemoryExec {
            batches: vec![batch],
            schema: schema.clone(),
            properties: compute_plan_properties(schema),
        })
    }

    #[derive(Debug)]
    struct TestMemoryExec {
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
        properties: PlanProperties,
    }

    impl DisplayAs for TestMemoryExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "TestMemoryExec")
        }
    }

    impl ExecutionPlan for TestMemoryExec {
        fn name(&self) -> &str {
            "TestMemoryExec"
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
        fn properties(&self) -> &PlanProperties {
            &self.properties
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> DFResult<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> DFResult<SendableRecordBatchStream> {
            Ok(Box::pin(MemoryStream::try_new(
                self.batches.clone(),
                Arc::clone(&self.schema),
                None,
            )?))
        }
    }

    async fn execute_fold(
        input: Arc<dyn ExecutionPlan>,
        key_indices: Vec<usize>,
        fold_bindings: Vec<FoldBinding>,
    ) -> RecordBatch {
        let exec = FoldExec::new(input, key_indices, fold_bindings, false, 1e-15);
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let stream = exec.execute(0, task_ctx).unwrap();
        let batches: Vec<RecordBatch> = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();
        if batches.is_empty() {
            RecordBatch::new_empty(exec.schema())
        } else {
            arrow::compute::concat_batches(&exec.schema(), &batches).unwrap()
        }
    }

    #[tokio::test]
    async fn test_sum_single_group() {
        let batch = make_test_batch(vec!["a", "a", "a"], vec![1.0, 2.0, 3.0]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "total".to_string(),
                kind: FoldAggKind::Sum,
                input_col_index: 1,
            }],
        )
        .await;

        assert_eq!(result.num_rows(), 1);
        let totals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((totals.value(0) - 6.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_count_non_null() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("a"), Some("a"), Some("a")])),
                Arc::new(Float64Array::from(vec![Some(1.0), None, Some(3.0)])),
            ],
        )
        .unwrap();
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "cnt".to_string(),
                kind: FoldAggKind::Count,
                input_col_index: 1,
            }],
        )
        .await;

        assert_eq!(result.num_rows(), 1);
        let counts = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.value(0), 2); // null not counted
    }

    #[tokio::test]
    async fn test_max_min() {
        let batch = make_test_batch(vec!["a", "a", "a"], vec![3.0, 1.0, 5.0]);
        let input_max = make_memory_exec(batch.clone());
        let input_min = make_memory_exec(batch);

        let result_max = execute_fold(
            input_max,
            vec![0],
            vec![FoldBinding {
                output_name: "mx".to_string(),
                kind: FoldAggKind::Max,
                input_col_index: 1,
            }],
        )
        .await;
        let result_min = execute_fold(
            input_min,
            vec![0],
            vec![FoldBinding {
                output_name: "mn".to_string(),
                kind: FoldAggKind::Min,
                input_col_index: 1,
            }],
        )
        .await;

        let max_vals = result_max
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(max_vals.value(0), 5.0);

        let min_vals = result_min
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(min_vals.value(0), 1.0);
    }

    #[tokio::test]
    async fn test_avg() {
        let batch = make_test_batch(vec!["a", "a", "a", "a"], vec![2.0, 4.0, 6.0, 8.0]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "average".to_string(),
                kind: FoldAggKind::Avg,
                input_col_index: 1,
            }],
        )
        .await;

        assert_eq!(result.num_rows(), 1);
        let avgs = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((avgs.value(0) - 5.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_multiple_groups() {
        let batch = make_test_batch(
            vec!["a", "a", "b", "b", "b"],
            vec![1.0, 2.0, 10.0, 20.0, 30.0],
        );
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "total".to_string(),
                kind: FoldAggKind::Sum,
                input_col_index: 1,
            }],
        )
        .await;

        assert_eq!(result.num_rows(), 2);
        let names = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let totals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        for i in 0..2 {
            match names.value(i) {
                "a" => assert!((totals.value(i) - 3.0).abs() < f64::EPSILON),
                "b" => assert!((totals.value(i) - 60.0).abs() < f64::EPSILON),
                _ => panic!("unexpected name"),
            }
        }
    }

    #[tokio::test]
    async fn test_empty_input() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        let batch = RecordBatch::new_empty(schema);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "total".to_string(),
                kind: FoldAggKind::Sum,
                input_col_index: 1,
            }],
        )
        .await;

        assert_eq!(result.num_rows(), 0);
    }

    #[tokio::test]
    async fn test_multiple_bindings() {
        let batch = make_test_batch(vec!["a", "a", "a"], vec![1.0, 2.0, 3.0]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![
                FoldBinding {
                    output_name: "total".to_string(),
                    kind: FoldAggKind::Sum,
                    input_col_index: 1,
                },
                FoldBinding {
                    output_name: "cnt".to_string(),
                    kind: FoldAggKind::Count,
                    input_col_index: 1,
                },
                FoldBinding {
                    output_name: "mx".to_string(),
                    kind: FoldAggKind::Max,
                    input_col_index: 1,
                },
            ],
        )
        .await;

        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.num_columns(), 4); // name + total + cnt + mx

        let totals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((totals.value(0) - 6.0).abs() < f64::EPSILON);

        let counts = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.value(0), 3);

        let maxes = result
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(maxes.value(0), 3.0);
    }

    // ── MNOR tests ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_nor_single_group() {
        // MNOR({0.3, 0.5}) = 1 - (1-0.3)*(1-0.5) = 1 - 0.7*0.5 = 1 - 0.35 = 0.65
        let batch = make_test_batch(vec!["a", "a"], vec![0.3, 0.5]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Nor,
                input_col_index: 1,
            }],
        )
        .await;

        assert_eq!(result.num_rows(), 1);
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.65).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_nor_identity() {
        // MNOR({0.0, 0.0}) = 1 - (1-0)*(1-0) = 1 - 1 = 0.0
        let batch = make_test_batch(vec!["a", "a"], vec![0.0, 0.0]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Nor,
                input_col_index: 1,
            }],
        )
        .await;

        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.0).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_nor_clamping() {
        // Out-of-range values should be clamped to [0, 1]
        let batch = make_test_batch(vec!["a", "a"], vec![-0.5, 1.5]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Nor,
                input_col_index: 1,
            }],
        )
        .await;

        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // Clamped to (0.0, 1.0): MNOR = 1 - (1-0)*(1-1) = 1 - 1*0 = 1.0
        assert!((vals.value(0) - 1.0).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_nor_multiple_groups() {
        let batch = make_test_batch(vec!["a", "a", "b", "b"], vec![0.3, 0.5, 0.1, 0.2]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Nor,
                input_col_index: 1,
            }],
        )
        .await;

        assert_eq!(result.num_rows(), 2);
        let names = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        for i in 0..2 {
            match names.value(i) {
                // MNOR({0.3, 0.5}) = 0.65
                "a" => assert!((vals.value(i) - 0.65).abs() < 1e-10),
                // MNOR({0.1, 0.2}) = 1 - 0.9*0.8 = 1 - 0.72 = 0.28
                "b" => assert!((vals.value(i) - 0.28).abs() < 1e-10),
                _ => panic!("unexpected name"),
            }
        }
    }

    // ── MPROD tests ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_prod_single_group() {
        // MPROD({0.6, 0.8}) = 0.48
        let batch = make_test_batch(vec!["a", "a"], vec![0.6, 0.8]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Prod,
                input_col_index: 1,
            }],
        )
        .await;

        assert_eq!(result.num_rows(), 1);
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.48).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_prod_identity() {
        // MPROD({1.0, 1.0}) = 1.0
        let batch = make_test_batch(vec!["a", "a"], vec![1.0, 1.0]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Prod,
                input_col_index: 1,
            }],
        )
        .await;

        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 1.0).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_prod_zero_absorbing() {
        // MPROD with 0.0 = 0.0 (zero is absorbing element)
        let batch = make_test_batch(vec!["a", "a", "a"], vec![0.5, 0.0, 0.8]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Prod,
                input_col_index: 1,
            }],
        )
        .await;

        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.0).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_prod_underflow_protection() {
        // 50 × 0.5 ≈ 8.88e-16, should not be exactly 0 thanks to log-space
        let names: Vec<&str> = vec!["a"; 50];
        let values: Vec<f64> = vec![0.5; 50];
        let batch = make_test_batch(names, values);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Prod,
                input_col_index: 1,
            }],
        )
        .await;

        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let expected = 0.5_f64.powi(50); // ≈ 8.88e-16
        assert!(vals.value(0) > 0.0, "should not underflow to zero");
        assert!(
            (vals.value(0) - expected).abs() / expected < 1e-6,
            "result {} should be close to expected {}",
            vals.value(0),
            expected
        );
    }

    // ── MNOR/MPROD mathematical correctness tests ───────────────────────

    fn make_nullable_test_batch(names: Vec<&str>, values: Vec<Option<f64>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    names.into_iter().map(Some).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_nor_single_element() {
        // MNOR({0.7}) = 0.7 (n=1 identity)
        let batch = make_test_batch(vec!["a"], vec![0.7]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Nor,
                input_col_index: 1,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.7).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_prod_single_element() {
        // MPROD({0.7}) = 0.7 (n=1 identity)
        let batch = make_test_batch(vec!["a"], vec![0.7]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Prod,
                input_col_index: 1,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.7).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_nor_three_elements() {
        // MNOR({0.3, 0.4, 0.5}) = 1 - (0.7)(0.6)(0.5) = 0.79
        let batch = make_test_batch(vec!["a", "a", "a"], vec![0.3, 0.4, 0.5]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Nor,
                input_col_index: 1,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.79).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_nor_four_elements_spec_example() {
        // Spec §4.5: MNOR({0.72, 0.54, 0.56, 0.42}) = 1 - (0.28)(0.46)(0.44)(0.58) = 0.96713024
        let batch = make_test_batch(vec!["a", "a", "a", "a"], vec![0.72, 0.54, 0.56, 0.42]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Nor,
                input_col_index: 1,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            (vals.value(0) - 0.96713024).abs() < 1e-10,
            "expected 0.96713024, got {}",
            vals.value(0)
        );
    }

    #[tokio::test]
    async fn test_prod_three_elements() {
        // MPROD({0.5, 0.5, 0.5}) = 0.125
        let batch = make_test_batch(vec!["a", "a", "a"], vec![0.5, 0.5, 0.5]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Prod,
                input_col_index: 1,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.125).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_nor_absorbing_element() {
        // p=1.0 absorbs: MNOR({0.3, 1.0}) = 1.0
        let batch = make_test_batch(vec!["a", "a"], vec![0.3, 1.0]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Nor,
                input_col_index: 1,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 1.0).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_prod_clamping() {
        // Out-of-range 2.0 clamped to 1.0: MPROD({2.0, 0.5}) = 1.0 * 0.5 = 0.5
        let batch = make_test_batch(vec!["a", "a"], vec![2.0, 0.5]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Prod,
                input_col_index: 1,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.5).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_prod_multiple_groups() {
        // a: MPROD({0.6, 0.8}) = 0.48, b: MPROD({0.5, 0.5}) = 0.25
        let batch = make_test_batch(vec!["a", "a", "b", "b"], vec![0.6, 0.8, 0.5, 0.5]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Prod,
                input_col_index: 1,
            }],
        )
        .await;

        assert_eq!(result.num_rows(), 2);
        let names = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..2 {
            match names.value(i) {
                "a" => assert!((vals.value(i) - 0.48).abs() < 1e-10),
                "b" => assert!((vals.value(i) - 0.25).abs() < 1e-10),
                _ => panic!("unexpected group name"),
            }
        }
    }

    #[tokio::test]
    async fn test_nor_commutativity() {
        // Order independence: MNOR({0.2, 0.5, 0.8}) = MNOR({0.8, 0.5, 0.2}) = 0.92
        let fwd = make_test_batch(vec!["a", "a", "a"], vec![0.2, 0.5, 0.8]);
        let rev = make_test_batch(vec!["a", "a", "a"], vec![0.8, 0.5, 0.2]);
        let binding = vec![FoldBinding {
            output_name: "prob".to_string(),
            kind: FoldAggKind::Nor,
            input_col_index: 1,
        }];
        let r1 = execute_fold(make_memory_exec(fwd), vec![0], binding.clone()).await;
        let r2 = execute_fold(make_memory_exec(rev), vec![0], binding).await;
        let v1 = r1
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        let v2 = r2
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((v1 - 0.92).abs() < 1e-10);
        assert!((v2 - 0.92).abs() < 1e-10);
        assert!((v1 - v2).abs() < 1e-15, "commutativity violated");
    }

    #[tokio::test]
    async fn test_prod_commutativity() {
        // Order independence: MPROD({0.5, 0.25}) = MPROD({0.25, 0.5}) = 0.125
        let fwd = make_test_batch(vec!["a", "a"], vec![0.5, 0.25]);
        let rev = make_test_batch(vec!["a", "a"], vec![0.25, 0.5]);
        let binding = vec![FoldBinding {
            output_name: "prob".to_string(),
            kind: FoldAggKind::Prod,
            input_col_index: 1,
        }];
        let r1 = execute_fold(make_memory_exec(fwd), vec![0], binding.clone()).await;
        let r2 = execute_fold(make_memory_exec(rev), vec![0], binding).await;
        let v1 = r1
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        let v2 = r2
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((v1 - 0.125).abs() < 1e-10);
        assert!((v2 - 0.125).abs() < 1e-10);
        assert!((v1 - v2).abs() < 1e-15, "commutativity violated");
    }

    #[tokio::test]
    async fn test_nor_boundary_near_zero() {
        // Precision near 0: MNOR({0.001, 0.002}) = 1 - (0.999)(0.998) = 0.002998
        let batch = make_test_batch(vec!["a", "a"], vec![0.001, 0.002]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Nor,
                input_col_index: 1,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let expected = 1.0 - 0.999 * 0.998;
        assert!(
            (vals.value(0) - expected).abs() < 1e-10,
            "expected {}, got {}",
            expected,
            vals.value(0)
        );
    }

    #[tokio::test]
    async fn test_nor_boundary_near_one() {
        // Precision near 1: MNOR({0.999, 0.998}) = 1 - (0.001)(0.002) = 0.999998
        let batch = make_test_batch(vec!["a", "a"], vec![0.999, 0.998]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Nor,
                input_col_index: 1,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let expected = 1.0 - 0.001 * 0.002;
        assert!(
            (vals.value(0) - expected).abs() < 1e-10,
            "expected {}, got {}",
            expected,
            vals.value(0)
        );
    }

    #[tokio::test]
    async fn test_prod_boundary_near_zero() {
        // Precision near 0: MPROD({0.001, 0.002}) = 2e-6
        let batch = make_test_batch(vec!["a", "a"], vec![0.001, 0.002]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Prod,
                input_col_index: 1,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            (vals.value(0) - 2e-6).abs() < 1e-15,
            "expected 2e-6, got {}",
            vals.value(0)
        );
    }

    #[tokio::test]
    async fn test_nor_empty_input() {
        // Empty input → 0 rows output
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        let batch = RecordBatch::new_empty(schema);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Nor,
                input_col_index: 1,
            }],
        )
        .await;
        assert_eq!(result.num_rows(), 0);
    }

    #[tokio::test]
    async fn test_nor_nan_handling() {
        // NaN propagates through noisy-OR
        let batch = make_test_batch(vec!["a", "a"], vec![0.3, f64::NAN]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Nor,
                input_col_index: 1,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(vals.value(0).is_nan(), "NaN should propagate through MNOR");
    }

    #[tokio::test]
    async fn test_prod_nan_handling() {
        // NaN propagates through product
        let batch = make_test_batch(vec!["a", "a"], vec![0.5, f64::NAN]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Prod,
                input_col_index: 1,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(vals.value(0).is_nan(), "NaN should propagate through MPROD");
    }

    #[tokio::test]
    async fn test_prod_infinity_handling() {
        // +∞ clamped to 1.0: MPROD({0.5, ∞}) = 0.5 * 1.0 = 0.5
        let batch = make_test_batch(vec!["a", "a"], vec![0.5, f64::INFINITY]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Prod,
                input_col_index: 1,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.5).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_nor_infinity_handling() {
        // +∞ clamped to 1.0, which absorbs: MNOR({0.3, ∞}) = 1.0
        let batch = make_test_batch(vec!["a", "a"], vec![0.3, f64::INFINITY]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Nor,
                input_col_index: 1,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 1.0).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_nor_all_null_values() {
        // All-null input → null output
        let batch = make_nullable_test_batch(vec!["a", "a"], vec![None, None]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Nor,
                input_col_index: 1,
            }],
        )
        .await;
        assert_eq!(result.num_rows(), 1);
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(vals.is_null(0), "all-null MNOR should produce null");
    }

    #[tokio::test]
    async fn test_prod_all_null_values() {
        // All-null input → null output
        let batch = make_nullable_test_batch(vec!["a", "a"], vec![None, None]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Prod,
                input_col_index: 1,
            }],
        )
        .await;
        assert_eq!(result.num_rows(), 1);
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(vals.is_null(0), "all-null MPROD should produce null");
    }

    #[tokio::test]
    async fn test_nor_mixed_null_values() {
        // Nulls skipped: MNOR({0.3, null, 0.5}) = 1 - (0.7)(0.5) = 0.65
        let batch = make_nullable_test_batch(vec!["a", "a", "a"], vec![Some(0.3), None, Some(0.5)]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Nor,
                input_col_index: 1,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.65).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_prod_mixed_null_values() {
        // Nulls skipped: MPROD({0.6, null, 0.8}) = 0.6 * 0.8 = 0.48
        let batch = make_nullable_test_batch(vec!["a", "a", "a"], vec![Some(0.6), None, Some(0.8)]);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Prod,
                input_col_index: 1,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((vals.value(0) - 0.48).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_nor_many_small_values() {
        // Large accumulation: 20 × 0.1 → 1 - 0.9^20 ≈ 0.8784
        let names: Vec<&str> = vec!["a"; 20];
        let values: Vec<f64> = vec![0.1; 20];
        let batch = make_test_batch(names, values);
        let input = make_memory_exec(batch);
        let result = execute_fold(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "prob".to_string(),
                kind: FoldAggKind::Nor,
                input_col_index: 1,
            }],
        )
        .await;
        let vals = result
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let expected = 1.0 - 0.9_f64.powi(20);
        assert!(
            (vals.value(0) - expected).abs() < 1e-10,
            "expected {}, got {}",
            expected,
            vals.value(0)
        );
    }

    // ── FoldAggKind classification tests (Phase 1) ────────────────────────

    #[test]
    fn test_is_monotonic() {
        assert!(FoldAggKind::Sum.is_monotonic());
        assert!(FoldAggKind::Max.is_monotonic());
        assert!(FoldAggKind::Min.is_monotonic());
        assert!(FoldAggKind::Count.is_monotonic());
        assert!(FoldAggKind::Nor.is_monotonic());
        assert!(FoldAggKind::Prod.is_monotonic());
        assert!(!FoldAggKind::Avg.is_monotonic());
        assert!(!FoldAggKind::Collect.is_monotonic());
    }

    #[test]
    fn test_monotonicity_direction() {
        use super::MonotonicDirection;
        assert_eq!(
            FoldAggKind::Sum.monotonicity_direction(),
            Some(MonotonicDirection::NonDecreasing)
        );
        assert_eq!(
            FoldAggKind::Max.monotonicity_direction(),
            Some(MonotonicDirection::NonDecreasing)
        );
        assert_eq!(
            FoldAggKind::Count.monotonicity_direction(),
            Some(MonotonicDirection::NonDecreasing)
        );
        assert_eq!(
            FoldAggKind::Nor.monotonicity_direction(),
            Some(MonotonicDirection::NonDecreasing)
        );
        assert_eq!(
            FoldAggKind::Min.monotonicity_direction(),
            Some(MonotonicDirection::NonIncreasing)
        );
        assert_eq!(
            FoldAggKind::Prod.monotonicity_direction(),
            Some(MonotonicDirection::NonIncreasing)
        );
        assert_eq!(FoldAggKind::Avg.monotonicity_direction(), None);
        assert_eq!(FoldAggKind::Collect.monotonicity_direction(), None);
    }

    #[test]
    fn test_identity_values() {
        assert_eq!(FoldAggKind::Sum.identity(), Some(0.0));
        assert_eq!(FoldAggKind::Count.identity(), Some(0.0));
        assert_eq!(FoldAggKind::Nor.identity(), Some(0.0));
        assert_eq!(FoldAggKind::Max.identity(), Some(f64::NEG_INFINITY));
        assert_eq!(FoldAggKind::Min.identity(), Some(f64::INFINITY));
        assert_eq!(FoldAggKind::Prod.identity(), Some(1.0));
        assert_eq!(FoldAggKind::Avg.identity(), None);
        assert_eq!(FoldAggKind::Collect.identity(), None);
    }

    // ── Strict mode tests (Phase 5) ──────────────────────────────────────

    async fn execute_fold_strict(
        input: Arc<dyn ExecutionPlan>,
        key_indices: Vec<usize>,
        fold_bindings: Vec<FoldBinding>,
        strict: bool,
    ) -> DFResult<RecordBatch> {
        let exec = FoldExec::new(input, key_indices, fold_bindings, strict, 1e-15);
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let stream = exec.execute(0, task_ctx).unwrap();
        let batches: Vec<RecordBatch> = datafusion::physical_plan::common::collect(stream).await?;
        if batches.is_empty() {
            Ok(RecordBatch::new_empty(exec.schema()))
        } else {
            arrow::compute::concat_batches(&exec.schema(), &batches)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
        }
    }

    #[tokio::test]
    async fn test_nor_strict_rejects_above_one() {
        let batch = make_test_batch(vec!["a"], vec![1.5]);
        let input = make_memory_exec(batch);
        let result = execute_fold_strict(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "p".into(),
                kind: FoldAggKind::Nor,
                input_col_index: 1,
            }],
            true,
        )
        .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("strict_probability_domain"),
            "Expected strict error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_nor_strict_rejects_negative() {
        let batch = make_test_batch(vec!["a"], vec![-0.1]);
        let input = make_memory_exec(batch);
        let result = execute_fold_strict(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "p".into(),
                kind: FoldAggKind::Nor,
                input_col_index: 1,
            }],
            true,
        )
        .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("strict_probability_domain"),
            "Expected strict error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_prod_strict_rejects_above_one() {
        let batch = make_test_batch(vec!["a"], vec![2.0]);
        let input = make_memory_exec(batch);
        let result = execute_fold_strict(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "p".into(),
                kind: FoldAggKind::Prod,
                input_col_index: 1,
            }],
            true,
        )
        .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("strict_probability_domain"),
            "Expected strict error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_prod_strict_rejects_negative() {
        let batch = make_test_batch(vec!["a"], vec![-0.5]);
        let input = make_memory_exec(batch);
        let result = execute_fold_strict(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "p".into(),
                kind: FoldAggKind::Prod,
                input_col_index: 1,
            }],
            true,
        )
        .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("strict_probability_domain"),
            "Expected strict error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_nor_strict_accepts_valid() {
        let batch = make_test_batch(vec!["a", "a"], vec![0.3, 0.5]);
        let input = make_memory_exec(batch);
        let result = execute_fold_strict(
            input,
            vec![0],
            vec![FoldBinding {
                output_name: "p".into(),
                kind: FoldAggKind::Nor,
                input_col_index: 1,
            }],
            true,
        )
        .await;
        assert!(result.is_ok(), "Expected Ok, got: {:?}", result.err());
        let batch = result.unwrap();
        let vals = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let expected = 0.65; // 1 - (1-0.3)(1-0.5)
        assert!(
            (vals.value(0) - expected).abs() < 1e-10,
            "expected {}, got {}",
            expected,
            vals.value(0)
        );
    }
}
