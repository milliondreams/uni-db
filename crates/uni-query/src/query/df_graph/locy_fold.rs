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

/// Aggregate function kind for FOLD bindings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FoldAggKind {
    Sum,
    Max,
    Min,
    Count,
    Avg,
    Collect,
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
    ) -> Self {
        let input_schema = input.schema();
        let schema = Self::build_output_schema(&input_schema, &key_indices, &fold_bindings);
        let properties = compute_plan_properties(Arc::clone(&schema));

        Self {
            input,
            key_indices,
            fold_bindings,
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
                FoldAggKind::Sum | FoldAggKind::Avg => DataType::Float64,
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

            // Group by key columns → row indices
            let mut groups: HashMap<Vec<ScalarKey>, Vec<usize>> = HashMap::new();
            for row_idx in 0..batch.num_rows() {
                let key = extract_scalar_key(&batch, &key_indices, row_idx);
                groups.entry(key).or_default().push(row_idx);
            }

            // Preserve insertion order by collecting keys in order of first appearance
            let mut ordered_keys: Vec<Vec<ScalarKey>> = Vec::new();
            {
                let mut seen: std::collections::HashSet<Vec<ScalarKey>> =
                    std::collections::HashSet::new();
                for row_idx in 0..batch.num_rows() {
                    let key = extract_scalar_key(&batch, &key_indices, row_idx);
                    if seen.insert(key.clone()) {
                        ordered_keys.push(key);
                    }
                }
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
) -> DFResult<arrow_array::ArrayRef> {
    match kind {
        FoldAggKind::Sum => {
            let mut builder = Float64Builder::with_capacity(num_groups);
            for key in ordered_keys {
                let indices = &groups[key];
                let sum = sum_f64(col, indices);
                match sum {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
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
                let sum = sum_f64(col, indices);
                let count = indices.iter().filter(|&&i| !col.is_null(i)).count();
                match (sum, count) {
                    (Some(s), c) if c > 0 => builder.append_value(s / c as f64),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        FoldAggKind::Collect => {
            let mut builder = LargeBinaryBuilder::with_capacity(num_groups, num_groups * 32);
            for key in ordered_keys {
                let indices = &groups[key];
                let mut values = Vec::new();
                for &i in indices {
                    if !col.is_null(i) {
                        let val = scalar_to_value(col, i);
                        values.push(val);
                    }
                }
                let list = uni_common::Value::List(values);
                let encoded = uni_common::cypher_value_codec::encode(&list);
                builder.append_value(&encoded);
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
                let indices = &groups[key];
                let mut result: Option<i64> = None;
                for &i in indices {
                    if arr.is_null(i) {
                        continue;
                    }
                    let v = arr.value(i);
                    result = Some(match result {
                        None => v,
                        Some(cur) if is_min => cur.min(v),
                        Some(cur) => cur.max(v),
                    });
                }
                match result {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            let mut builder = Float64Builder::with_capacity(num_groups);
            for key in ordered_keys {
                let indices = &groups[key];
                let mut result: Option<f64> = None;
                for &i in indices {
                    if arr.is_null(i) {
                        continue;
                    }
                    let v = arr.value(i);
                    result = Some(match result {
                        None => v,
                        Some(cur) if is_min => cur.min(v),
                        Some(cur) => cur.max(v),
                    });
                }
                match result {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => {
            // Fallback: treat as string comparison
            let mut builder = arrow_array::builder::StringBuilder::new();
            for key in ordered_keys {
                let indices = &groups[key];
                let mut result: Option<String> = None;
                for &i in indices {
                    if col.is_null(i) {
                        continue;
                    }
                    let v = format!("{:?}", scalar_to_value(col, i));
                    result = Some(match result {
                        None => v.clone(),
                        Some(cur) => {
                            if is_min {
                                if v < cur { v } else { cur }
                            } else if v > cur {
                                v
                            } else {
                                cur
                            }
                        }
                    });
                }
                match result {
                    Some(v) => builder.append_value(&v),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
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
        let exec = FoldExec::new(input, key_indices, fold_bindings);
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
}
