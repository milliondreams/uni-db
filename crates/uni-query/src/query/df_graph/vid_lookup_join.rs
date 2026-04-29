// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Cross-MATCH dynamic VID-filter pushdown (issue #55 PR #5).
//!
//! Specializes the inner-equi-join `Filter[id(probe) = build_expr] ←
//! CrossJoin{build, probe_scan}` pattern when `probe_scan` is a vertex
//! `GraphScanExec` and the build side is materialisable at runtime. Replaces
//! the would-be `HashJoinExec{build, full_table_scan}` (which fully scans the
//! probe label and discards mismatches) with a runtime IN-list pushdown:
//!
//! 1. Run the build child to completion. Collect its rows.
//! 2. Extract the distinct VIDs from the build side's join-key column.
//! 3. Hand them to the probe `GraphScanExec` as an `_vid IN (v1, v2, ...)`
//!    Lance filter, executing the scan ONCE.
//! 4. In-memory hash-join the build batches with the probe batch on
//!    `(build_key_col, probe._vid)`, emitting `(build_cols | probe_cols)`.
//!
//! ## When this operator fires
//!
//! Triggered by the planner in `try_plan_cross_join_as_hash_join` when:
//! - The equi-pair predicate has exactly one pair where one side is
//!   `Property(Variable(scan_var), "_vid")` (or `id(scan_var)` lowered the
//!   same way),
//! - The subtree containing `scan_var` is a single `LogicalPlan::Scan` (or
//!   `ScanMainByLabels`) — i.e. the probe is a fresh label scan with no
//!   conflicting filters,
//! - The join is INNER (LEFT/RIGHT outer joins fall back to HashJoinExec —
//!   they would need NULL-padding when the probe scan returns nothing for a
//!   build VID),
//! - There is exactly one equi-pair (multi-key joins fall back to HashJoin).
//!
//! The plan-time IN-list pushdown for `UNWIND $list` (PR #4) takes precedence
//! when both apply — it's cheaper (no runtime materialization).
//!
//! ## Build-size cap
//!
//! If the build side returns more distinct VIDs than [`MAX_VIDS`] (10 000),
//! the operator falls back to a full probe scan + post-join filter via the
//! same in-memory hash-join. This bounds the IN-list size we send to Lance
//! (which has practical SQL parser limits) and prevents OOM on pathological
//! build sides.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, TryStreamExt};

use super::common::compute_plan_properties;
use super::scan::GraphScanExec;

/// Cap on the number of distinct VIDs we'll push as an `_vid IN (...)`
/// filter. Mirrors the equivalent constant in `df_planner.rs` so the
/// behaviour is consistent across the static-UNWIND and runtime paths.
pub(crate) const MAX_VIDS: usize = 10_000;

/// Cross-MATCH dynamic VID-filter pushdown operator.
///
/// See module-level docs for the pattern this targets and the conditions
/// under which the planner emits it instead of `HashJoinExec`.
pub struct VidLookupJoinExec {
    /// Build-side child (any ExecutionPlan). Materialized fully at execute
    /// time to extract VIDs.
    build: Arc<dyn ExecutionPlan>,
    /// Probe-side scan, stored as `Arc<dyn ExecutionPlan>` so it slots into
    /// DataFusion plumbing; must downcast to `GraphScanExec`. We don't
    /// invoke its standard `execute()` — instead we call
    /// `execute_with_vid_filter` after materializing the build side.
    probe_scan: Arc<dyn ExecutionPlan>,
    /// Index into the build's output schema for the join-key column. The
    /// column must be UInt64 (a VID).
    build_key_col_idx: usize,
    /// Combined output schema = build_schema ++ probe_schema.
    output_schema: SchemaRef,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for VidLookupJoinExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VidLookupJoinExec")
            .field("build_key_col_idx", &self.build_key_col_idx)
            .finish()
    }
}

impl VidLookupJoinExec {
    /// Construct a new VID-lookup-join.
    ///
    /// The output schema is `build.schema()` concatenated with
    /// `probe_scan.schema()`. The caller must guarantee that
    /// `build.schema().field(build_key_col_idx)` is a UInt64 column whose
    /// values are valid VIDs for the probe label.
    pub fn try_new(
        build: Arc<dyn ExecutionPlan>,
        probe_scan: Arc<dyn ExecutionPlan>,
        build_key_col_idx: usize,
    ) -> DFResult<Self> {
        // Probe must be a GraphScanExec. Downcast eagerly to fail fast on
        // construction rather than at execute() time.
        if probe_scan.as_any().downcast_ref::<GraphScanExec>().is_none() {
            return Err(datafusion::error::DataFusionError::Plan(
                "VidLookupJoinExec: probe_scan must be a GraphScanExec".into(),
            ));
        }
        let build_schema = build.schema();
        if build_key_col_idx >= build_schema.fields().len() {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "VidLookupJoinExec: build_key_col_idx={} out of bounds for build schema (fields={})",
                build_key_col_idx,
                build_schema.fields().len()
            )));
        }
        let probe_schema = probe_scan.schema();
        let output_schema = concat_schemas(&build_schema, &probe_schema);
        let properties = compute_plan_properties(output_schema.clone());
        Ok(Self {
            build,
            probe_scan,
            build_key_col_idx,
            output_schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for VidLookupJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "VidLookupJoinExec: build_key_col_idx={}",
            self.build_key_col_idx
        )
    }
}

impl ExecutionPlan for VidLookupJoinExec {
    fn name(&self) -> &str {
        "VidLookupJoinExec"
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
        // We expose the build child only. The probe scan is conceptually a
        // child but we drive it with a runtime-supplied filter that
        // DataFusion's plan-tree walking can't represent — so it's kept
        // private to the operator. with_new_children below preserves the
        // probe scan as-is.
        vec![&self.build]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "VidLookupJoinExec expects exactly one child (the build side); got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self::try_new(
            children.into_iter().next().unwrap(),
            self.probe_scan.clone(),
            self.build_key_col_idx,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let metrics = BaselineMetrics::new(&self.metrics, partition);
        let build = self.build.clone();
        let probe_scan = self.probe_scan.clone();
        let build_key_col_idx = self.build_key_col_idx;
        let output_schema = self.output_schema.clone();

        let fut = async move {
            run_vid_lookup_join(
                build,
                probe_scan,
                build_key_col_idx,
                output_schema.clone(),
                partition,
                context,
            )
            .await
        };

        Ok(Box::pin(VidLookupJoinStream {
            state: VidLookupJoinStreamState::Running(Box::pin(fut)),
            schema: self.output_schema.clone(),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

// ---------------------------------------------------------------------------
// Stream state machine
// ---------------------------------------------------------------------------

enum VidLookupJoinStreamState {
    Running(Pin<Box<dyn std::future::Future<Output = DFResult<RecordBatch>> + Send>>),
    Done,
}

struct VidLookupJoinStream {
    state: VidLookupJoinStreamState,
    schema: SchemaRef,
    metrics: BaselineMetrics,
}

impl Stream for VidLookupJoinStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.state {
            VidLookupJoinStreamState::Running(fut) => match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(batch)) => {
                    self.metrics.record_output(batch.num_rows());
                    self.state = VidLookupJoinStreamState::Done;
                    Poll::Ready(Some(Ok(batch)))
                }
                Poll::Ready(Err(e)) => {
                    self.state = VidLookupJoinStreamState::Done;
                    Poll::Ready(Some(Err(e)))
                }
                Poll::Pending => Poll::Pending,
            },
            VidLookupJoinStreamState::Done => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for VidLookupJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// ---------------------------------------------------------------------------
// Core join logic
// ---------------------------------------------------------------------------

async fn run_vid_lookup_join(
    build: Arc<dyn ExecutionPlan>,
    probe_scan: Arc<dyn ExecutionPlan>,
    build_key_col_idx: usize,
    output_schema: SchemaRef,
    partition: usize,
    context: Arc<TaskContext>,
) -> DFResult<RecordBatch> {
    // Downcast verified at construction; safe to unwrap here.
    let probe_scan = probe_scan
        .as_any()
        .downcast_ref::<GraphScanExec>()
        .expect("VidLookupJoinExec::try_new ensured probe_scan is GraphScanExec");
    // 1. Materialize the build side fully.
    let build_stream = build.execute(partition, context)?;
    let build_batches: Vec<RecordBatch> = build_stream.try_collect().await?;

    if build_batches.is_empty() {
        // No build rows → empty join.
        return Ok(RecordBatch::new_empty(output_schema));
    }

    // 2. Extract distinct VIDs from build.
    let mut vid_set: HashSet<u64> = HashSet::new();
    for batch in &build_batches {
        let arr = batch.column(build_key_col_idx);
        let u64_arr = arr
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(format!(
                    "VidLookupJoinExec: build_key_col_idx={} is not UInt64 (got {:?})",
                    build_key_col_idx,
                    arr.data_type()
                ))
            })?;
        for i in 0..u64_arr.len() {
            if !u64_arr.is_null(i) {
                vid_set.insert(u64_arr.value(i));
            }
        }
    }

    if vid_set.is_empty() {
        // All build rows had NULL VIDs → empty join.
        return Ok(RecordBatch::new_empty(output_schema));
    }

    // 3. Defensive cap. Planner-side check should have steered above-cap
    // workloads to plain HashJoinExec; if we ever reach this, fail loud
    // rather than silently OOM the Lance scanner.
    if vid_set.len() > MAX_VIDS {
        return Err(datafusion::error::DataFusionError::Execution(format!(
            "VidLookupJoinExec: build side produced {} distinct VIDs, exceeds cap {}. \
             Planner should have selected HashJoinExec instead. This is a planner bug.",
            vid_set.len(),
            MAX_VIDS
        )));
    }

    let vids: Vec<u64> = vid_set.into_iter().collect();

    // 4. Run the probe scan once with the IN-list filter.
    let probe_batch = probe_scan.execute_with_vid_filter(&vids).await?;

    // 5. In-memory hash-join build × probe on the VID equi-pair. Build a
    // map from probe `_vid` to the row indices that hold it; then walk
    // build rows and emit (build | probe) for each match.
    let probe_vid_idx = locate_vid_column(&probe_batch.schema())?;
    let probe_vid_arr = probe_batch
        .column(probe_vid_idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(
                "VidLookupJoinExec: probe _vid column is not UInt64".into(),
            )
        })?;
    let mut probe_index: HashMap<u64, Vec<usize>> = HashMap::with_capacity(probe_vid_arr.len());
    for i in 0..probe_vid_arr.len() {
        if !probe_vid_arr.is_null(i) {
            probe_index.entry(probe_vid_arr.value(i)).or_default().push(i);
        }
    }

    let joined = join_batches(
        &build_batches,
        build_key_col_idx,
        &probe_batch,
        &probe_index,
        &output_schema,
    )?;

    Ok(joined)
}

/// Find the `_vid` column in a probe-side `RecordBatch` schema. The probe
/// scan emits a column literally named `_vid` (the bare underscore form,
/// not `<var>._vid`), per `build_vertex_schema` in scan.rs.
fn locate_vid_column(schema: &SchemaRef) -> DFResult<usize> {
    schema
        .fields()
        .iter()
        .enumerate()
        .find_map(|(i, f)| {
            if f.name() == "_vid" || f.name().ends_with("._vid") {
                Some(i)
            } else {
                None
            }
        })
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Plan(
                "VidLookupJoinExec: probe schema has no _vid column".into(),
            )
        })
}

/// Concatenate build_schema and probe_schema into a single output schema.
/// Field names are kept as-is — uniqueness across the two sides is the
/// caller's responsibility (it's enforced upstream by Cypher's
/// variable-naming rules).
fn concat_schemas(build_schema: &SchemaRef, probe_schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Field> = Vec::with_capacity(
        build_schema.fields().len() + probe_schema.fields().len(),
    );
    for f in build_schema.fields() {
        fields.push(f.as_ref().clone());
    }
    for f in probe_schema.fields() {
        fields.push(f.as_ref().clone());
    }
    Arc::new(Schema::new(fields))
}

/// Inner equi-join build batches with a single probe batch on `(build[key_col], probe._vid)`.
/// Returns ONE concatenated `RecordBatch` matching `output_schema`.
fn join_batches(
    build_batches: &[RecordBatch],
    build_key_col_idx: usize,
    probe_batch: &RecordBatch,
    probe_index: &HashMap<u64, Vec<usize>>,
    output_schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    use arrow_array::builder::UInt32Builder;

    // For each (build row, probe row) match, record the corresponding
    // (build_batch_idx, build_row_idx, probe_row_idx) so we can do a
    // single take(...) on each side at the end.
    let mut build_take_indices_per_batch: Vec<Vec<u32>> =
        (0..build_batches.len()).map(|_| Vec::new()).collect();
    let mut probe_take: Vec<u32> = Vec::new();

    for (build_batch_idx, build_batch) in build_batches.iter().enumerate() {
        let key_arr = build_batch
            .column(build_key_col_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(
                    "VidLookupJoinExec: build key column is not UInt64".into(),
                )
            })?;
        for build_row_idx in 0..key_arr.len() {
            if key_arr.is_null(build_row_idx) {
                continue;
            }
            let key = key_arr.value(build_row_idx);
            if let Some(probe_rows) = probe_index.get(&key) {
                for &probe_row in probe_rows {
                    build_take_indices_per_batch[build_batch_idx].push(build_row_idx as u32);
                    probe_take.push(probe_row as u32);
                }
            }
        }
    }

    let total_rows = probe_take.len();
    if total_rows == 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    // Build side: take per batch, then concatenate columns column-by-column.
    let n_build_cols = build_batches[0].num_columns();
    let mut build_col_arrays: Vec<Vec<ArrayRef>> = (0..n_build_cols).map(|_| Vec::new()).collect();
    for (batch_idx, take_idx) in build_take_indices_per_batch.iter().enumerate() {
        if take_idx.is_empty() {
            continue;
        }
        let mut builder = UInt32Builder::with_capacity(take_idx.len());
        for &i in take_idx {
            builder.append_value(i);
        }
        let take_array = builder.finish();
        let batch = &build_batches[batch_idx];
        for (col_idx, col_arrs) in build_col_arrays.iter_mut().enumerate() {
            let taken = arrow::compute::take(batch.column(col_idx), &take_array, None)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
            col_arrs.push(taken);
        }
    }
    let build_columns: Vec<ArrayRef> = build_col_arrays
        .into_iter()
        .map(|arrs| {
            if arrs.len() == 1 {
                Ok(arrs.into_iter().next().unwrap())
            } else {
                let refs: Vec<&dyn Array> = arrs.iter().map(|a| a.as_ref()).collect();
                arrow::compute::concat(&refs)
                    .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
            }
        })
        .collect::<DFResult<_>>()?;

    // Probe side: single take.
    let mut probe_take_builder = UInt32Builder::with_capacity(probe_take.len());
    for v in &probe_take {
        probe_take_builder.append_value(*v);
    }
    let probe_take_array = probe_take_builder.finish();
    let probe_columns: Vec<ArrayRef> = (0..probe_batch.num_columns())
        .map(|i| {
            arrow::compute::take(probe_batch.column(i), &probe_take_array, None)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
        })
        .collect::<DFResult<_>>()?;

    let mut all_columns: Vec<ArrayRef> = Vec::with_capacity(n_build_cols + probe_batch.num_columns());
    all_columns.extend(build_columns);
    all_columns.extend(probe_columns);

    RecordBatch::try_new(output_schema.clone(), all_columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}
