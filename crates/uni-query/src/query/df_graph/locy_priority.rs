// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! PRIORITY operator for Locy.
//!
//! `PriorityExec` applies priority semantics: among rows sharing KEY columns,
//! only those from the highest-priority clause are kept. The `__priority` column
//! is consumed and removed from the output schema.

use crate::query::df_graph::common::{ScalarKey, compute_plan_properties, extract_scalar_key};
use arrow::compute::filter as arrow_filter;
use arrow_array::{BooleanArray, Int64Array, RecordBatch};
use arrow_schema::{Field, Schema, SchemaRef};
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

/// DataFusion `ExecutionPlan` that applies PRIORITY filtering.
///
/// For each group of rows sharing the same KEY columns, keeps only those rows
/// whose `__priority` value equals the maximum priority in that group. The
/// `__priority` column is stripped from the output.
#[derive(Debug)]
pub struct PriorityExec {
    input: Arc<dyn ExecutionPlan>,
    key_indices: Vec<usize>,
    priority_col_index: usize,
    schema: SchemaRef,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl PriorityExec {
    /// Create a new `PriorityExec`.
    ///
    /// # Arguments
    /// * `input` - Child execution plan (must contain `__priority` column)
    /// * `key_indices` - Indices of KEY columns for grouping
    /// * `priority_col_index` - Index of the `__priority` Int64 column
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        key_indices: Vec<usize>,
        priority_col_index: usize,
    ) -> Self {
        let input_schema = input.schema();
        // Build output schema: all columns except __priority
        let output_fields: Vec<Arc<Field>> = input_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != priority_col_index)
            .map(|(_, f)| Arc::clone(f))
            .collect();
        let schema = Arc::new(Schema::new(output_fields));
        let properties = compute_plan_properties(Arc::clone(&schema));

        Self {
            input,
            key_indices,
            priority_col_index,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for PriorityExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PriorityExec: key_indices={:?}, priority_col={}",
            self.key_indices, self.priority_col_index
        )
    }
}

impl ExecutionPlan for PriorityExec {
    fn name(&self) -> &str {
        "PriorityExec"
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
                "PriorityExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.key_indices.clone(),
            self.priority_col_index,
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
        let priority_col_index = self.priority_col_index;
        let output_schema = Arc::clone(&self.schema);
        let input_schema = self.input.schema();

        let fut = async move {
            // Collect all input batches
            let batches: Vec<RecordBatch> = input_stream.try_collect().await?;

            if batches.is_empty() {
                return Ok(RecordBatch::new_empty(output_schema));
            }

            let batch = arrow::compute::concat_batches(&input_schema, &batches)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

            if batch.num_rows() == 0 {
                return Ok(RecordBatch::new_empty(output_schema));
            }

            // Extract priority column
            let priority_col = batch
                .column(priority_col_index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "__priority column must be Int64".to_string(),
                    )
                })?;

            // Group by key columns → find max priority per group
            let mut group_max: HashMap<Vec<ScalarKey>, i64> = HashMap::new();
            for row_idx in 0..batch.num_rows() {
                let key = extract_scalar_key(&batch, &key_indices, row_idx);
                let prio = priority_col.value(row_idx);
                let entry = group_max.entry(key).or_insert(i64::MIN);
                if prio > *entry {
                    *entry = prio;
                }
            }

            // Build filter mask: keep rows where priority == max for their group
            let keep: Vec<bool> = (0..batch.num_rows())
                .map(|row_idx| {
                    let key = extract_scalar_key(&batch, &key_indices, row_idx);
                    let prio = priority_col.value(row_idx);
                    group_max
                        .get(&key)
                        .is_some_and(|&max_prio| prio == max_prio)
                })
                .collect();

            let filter_mask = BooleanArray::from(keep);

            // Filter columns, skipping __priority
            let mut output_columns = Vec::with_capacity(output_schema.fields().len());
            for (i, col) in batch.columns().iter().enumerate() {
                if i == priority_col_index {
                    continue;
                }
                let filtered = arrow_filter(col.as_ref(), &filter_mask).map_err(|e| {
                    datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                })?;
                output_columns.push(filtered);
            }

            RecordBatch::try_new(output_schema, output_columns)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
        };

        Ok(Box::pin(PriorityStream {
            state: PriorityStreamState::Running(Box::pin(fut)),
            schema: Arc::clone(&self.schema),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

// ---------------------------------------------------------------------------
// Stream implementation
// ---------------------------------------------------------------------------

enum PriorityStreamState {
    Running(Pin<Box<dyn std::future::Future<Output = DFResult<RecordBatch>> + Send>>),
    Done,
}

struct PriorityStream {
    state: PriorityStreamState,
    schema: SchemaRef,
    metrics: BaselineMetrics,
}

impl Stream for PriorityStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.state {
            PriorityStreamState::Running(fut) => match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(batch)) => {
                    self.metrics.record_output(batch.num_rows());
                    self.state = PriorityStreamState::Done;
                    Poll::Ready(Some(Ok(batch)))
                }
                Poll::Ready(Err(e)) => {
                    self.state = PriorityStreamState::Done;
                    Poll::Ready(Some(Err(e)))
                }
                Poll::Pending => Poll::Pending,
            },
            PriorityStreamState::Done => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for PriorityStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::memory::MemoryStream;
    use datafusion::prelude::SessionContext;

    fn make_test_batch(names: Vec<&str>, values: Vec<i64>, priorities: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
            Field::new("__priority", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    names.into_iter().map(Some).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(values)),
                Arc::new(Int64Array::from(priorities)),
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

    async fn execute_priority(
        input: Arc<dyn ExecutionPlan>,
        key_indices: Vec<usize>,
        priority_col_index: usize,
    ) -> RecordBatch {
        let exec = PriorityExec::new(input, key_indices, priority_col_index);
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
    async fn test_single_group_keeps_highest_priority() {
        let batch = make_test_batch(vec!["a", "a", "a"], vec![10, 20, 30], vec![1, 3, 2]);
        let input = make_memory_exec(batch);
        // key_indices=[0] (name), priority_col=2
        let result = execute_priority(input, vec![0], 2).await;

        assert_eq!(result.num_rows(), 1);
        let values = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 20); // priority 3 was highest
    }

    #[tokio::test]
    async fn test_multiple_groups_independent_priority() {
        let batch = make_test_batch(
            vec!["a", "a", "b", "b"],
            vec![10, 20, 30, 40],
            vec![1, 2, 3, 1],
        );
        let input = make_memory_exec(batch);
        let result = execute_priority(input, vec![0], 2).await;

        assert_eq!(result.num_rows(), 2);
        // Group "a": priority 2 wins → value 20
        // Group "b": priority 3 wins → value 30
        let names = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Find each group's result
        for i in 0..2 {
            match names.value(i) {
                "a" => assert_eq!(values.value(i), 20),
                "b" => assert_eq!(values.value(i), 30),
                _ => panic!("unexpected name"),
            }
        }
    }

    #[tokio::test]
    async fn test_all_same_priority_keeps_all() {
        let batch = make_test_batch(vec!["a", "a", "a"], vec![10, 20, 30], vec![5, 5, 5]);
        let input = make_memory_exec(batch);
        let result = execute_priority(input, vec![0], 2).await;

        assert_eq!(result.num_rows(), 3);
    }

    #[tokio::test]
    async fn test_empty_input() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__priority", DataType::Int64, false),
        ]));
        let batch = RecordBatch::new_empty(schema.clone());
        let input = make_memory_exec(batch);
        let result = execute_priority(input, vec![0], 1).await;

        assert_eq!(result.num_rows(), 0);
    }

    #[tokio::test]
    async fn test_single_row_passthrough() {
        let batch = make_test_batch(vec!["x"], vec![42], vec![1]);
        let input = make_memory_exec(batch);
        let result = execute_priority(input, vec![0], 2).await;

        assert_eq!(result.num_rows(), 1);
        let values = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 42);
    }

    #[tokio::test]
    async fn test_output_schema_lacks_priority() {
        let batch = make_test_batch(vec!["a"], vec![1], vec![1]);
        let input = make_memory_exec(batch);
        let exec = PriorityExec::new(input, vec![0], 2);

        let schema = exec.schema();
        assert_eq!(schema.fields().len(), 2); // name + value, no __priority
        assert!(schema.column_with_name("__priority").is_none());
        assert!(schema.column_with_name("name").is_some());
        assert!(schema.column_with_name("value").is_some());
    }
}
