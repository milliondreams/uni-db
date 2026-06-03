// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Read-set recording wrapper for SSI/OCC conflict detection.
//!
//! [`ReadSetRecordingExec`] is a transparent pass-through `ExecutionPlan` that
//! records the identity (`_vid` / `_eid`) of every row surviving its child's
//! filters into the transaction's optimistic read-set. The planner inserts it
//! immediately above a scan and its residual `FilterExec`, so the read set
//! reflects exactly the rows the query logically depended on rather than the
//! wider set the scan physically touched. Compiled only with the `ssi` feature.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, StreamExt};

use crate::query::df_graph::GraphExecutionContext;

/// Transparent `ExecutionPlan` that records surviving row identities into the
/// SSI read-set.
///
/// Wraps a leaf scan (plus its residual filter) and, for each output batch,
/// records the `{variable}._vid` / `{variable}._eid` column values into the
/// transaction read-set. Batches are passed through unchanged.
#[derive(Debug)]
pub struct ReadSetRecordingExec {
    input: Arc<dyn ExecutionPlan>,
    graph_ctx: Arc<GraphExecutionContext>,
    /// Input-schema column indices holding vertex ids (`{var}._vid`).
    vertex_cols: Vec<usize>,
    /// Input-schema column indices holding edge ids (`{var}._eid`).
    edge_cols: Vec<usize>,
}

impl ReadSetRecordingExec {
    /// Creates a recording wrapper over `input` for the given pattern variable.
    ///
    /// Resolves the `{variable}._vid` / `{variable}._eid` columns from the input
    /// schema once; if neither is present the node is a pure pass-through.
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        graph_ctx: Arc<GraphExecutionContext>,
        variable: &str,
    ) -> Self {
        let vid_name = format!("{variable}._vid");
        let eid_name = format!("{variable}._eid");
        let mut vertex_cols = Vec::new();
        let mut edge_cols = Vec::new();
        for (i, field) in input.schema().fields().iter().enumerate() {
            if field.name() == &vid_name {
                vertex_cols.push(i);
            } else if field.name() == &eid_name {
                edge_cols.push(i);
            }
        }
        Self {
            input,
            graph_ctx,
            vertex_cols,
            edge_cols,
        }
    }
}

impl DisplayAs for ReadSetRecordingExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ReadSetRecordingExec")
    }
}

impl ExecutionPlan for ReadSetRecordingExec {
    fn name(&self) -> &str {
        "ReadSetRecordingExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let input = children.into_iter().next().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                "ReadSetRecordingExec requires exactly one child".to_string(),
            )
        })?;
        Ok(Arc::new(ReadSetRecordingExec {
            input,
            graph_ctx: self.graph_ctx.clone(),
            vertex_cols: self.vertex_cols.clone(),
            edge_cols: self.edge_cols.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let inner = self.input.execute(partition, context)?;
        Ok(Box::pin(ReadSetRecordingStream {
            schema: self.input.schema(),
            inner,
            graph_ctx: self.graph_ctx.clone(),
            vertex_cols: self.vertex_cols.clone(),
            edge_cols: self.edge_cols.clone(),
        }))
    }
}

/// Stream adapter that records surviving identities, then yields the batch.
struct ReadSetRecordingStream {
    schema: SchemaRef,
    inner: SendableRecordBatchStream,
    graph_ctx: Arc<GraphExecutionContext>,
    vertex_cols: Vec<usize>,
    edge_cols: Vec<usize>,
}

impl Stream for ReadSetRecordingStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                self.graph_ctx
                    .record_batch_ids(&batch, &self.vertex_cols, &self.edge_cols);
                Poll::Ready(Some(Ok(batch)))
            }
            other => other,
        }
    }
}

impl RecordBatchStream for ReadSetRecordingStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
