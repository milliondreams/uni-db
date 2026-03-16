// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Fixpoint iteration operator for recursive Locy strata.
//!
//! `FixpointExec` drives semi-naive evaluation: it repeatedly evaluates the rules
//! in a recursive stratum, feeding back deltas until no new facts are produced.

use crate::query::df_graph::GraphExecutionContext;
use crate::query::df_graph::common::{
    ScalarKey, collect_all_partitions, compute_plan_properties, execute_subplan, extract_scalar_key,
};
use crate::query::df_graph::locy_best_by::{BestByExec, SortCriterion};
use crate::query::df_graph::locy_errors::LocyRuntimeError;
use crate::query::df_graph::locy_explain::{DerivationEntry, DerivationTracker};
use crate::query::df_graph::locy_fold::{FoldBinding, FoldExec};
use crate::query::df_graph::locy_priority::PriorityExec;
use crate::query::planner::LogicalPlan;
use arrow_array::RecordBatch;
use arrow_row::{RowConverter, SortField};
use arrow_schema::SchemaRef;
use datafusion::common::JoinType;
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::Stream;
use parking_lot::RwLock;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, RwLock as StdRwLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use uni_common::Value;
use uni_common::core::schema::Schema as UniSchema;
use uni_store::storage::manager::StorageManager;

// ---------------------------------------------------------------------------
// DerivedScanRegistry — injection point for IS-ref data into subplans
// ---------------------------------------------------------------------------

/// A single entry in the derived scan registry.
///
/// Each entry corresponds to one `LocyDerivedScan` node in the logical plan tree.
/// The `data` handle is shared with the logical plan node so that writing data here
/// makes it visible when the subplan is re-planned and executed.
#[derive(Debug)]
pub struct DerivedScanEntry {
    /// Index matching the `scan_index` in `LocyDerivedScan`.
    pub scan_index: usize,
    /// Name of the rule this scan reads from.
    pub rule_name: String,
    /// Whether this is a self-referential scan (rule references itself).
    pub is_self_ref: bool,
    /// Shared data handle — write batches here to inject into subplans.
    pub data: Arc<RwLock<Vec<RecordBatch>>>,
    /// Schema of the derived relation.
    pub schema: SchemaRef,
}

/// Registry of derived scan handles for fixpoint iteration.
///
/// During fixpoint, each clause body may reference derived relations via
/// `LocyDerivedScan` nodes. The registry maps scan indices to shared data
/// handles so the fixpoint loop can inject delta/full facts before each
/// iteration.
#[derive(Debug, Default)]
pub struct DerivedScanRegistry {
    entries: Vec<DerivedScanEntry>,
}

impl DerivedScanRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an entry to the registry.
    pub fn add(&mut self, entry: DerivedScanEntry) {
        self.entries.push(entry);
    }

    /// Get an entry by scan index.
    pub fn get(&self, scan_index: usize) -> Option<&DerivedScanEntry> {
        self.entries.iter().find(|e| e.scan_index == scan_index)
    }

    /// Write data into a scan entry's shared handle.
    pub fn write_data(&self, scan_index: usize, batches: Vec<RecordBatch>) {
        if let Some(entry) = self.get(scan_index) {
            let mut guard = entry.data.write();
            *guard = batches;
        }
    }

    /// Get all entries for a given rule name.
    pub fn entries_for_rule(&self, rule_name: &str) -> Vec<&DerivedScanEntry> {
        self.entries
            .iter()
            .filter(|e| e.rule_name == rule_name)
            .collect()
    }
}

// ---------------------------------------------------------------------------
// MonotonicAggState — tracking monotonic aggregates across iterations
// ---------------------------------------------------------------------------

/// Monotonic aggregate binding: maps a fold name to its aggregate kind and column.
#[derive(Debug, Clone)]
pub struct MonotonicFoldBinding {
    pub fold_name: String,
    pub kind: crate::query::df_graph::locy_fold::FoldAggKind,
    pub input_col_index: usize,
}

/// Tracks monotonic aggregate accumulators across fixpoint iterations.
///
/// After each iteration, accumulators are updated and compared to their previous
/// snapshot. The fixpoint has converged (w.r.t. aggregates) when all accumulators
/// are stable (no change between iterations).
#[derive(Debug)]
pub struct MonotonicAggState {
    /// Current accumulator values keyed by (group_key, fold_name).
    accumulators: HashMap<(Vec<ScalarKey>, String), f64>,
    /// Snapshot from the previous iteration for stability check.
    prev_snapshot: HashMap<(Vec<ScalarKey>, String), f64>,
    /// Bindings describing which aggregates to track.
    bindings: Vec<MonotonicFoldBinding>,
}

impl MonotonicAggState {
    /// Create a new monotonic aggregate state.
    pub fn new(bindings: Vec<MonotonicFoldBinding>) -> Self {
        Self {
            accumulators: HashMap::new(),
            prev_snapshot: HashMap::new(),
            bindings,
        }
    }

    /// Update accumulators with new delta batches. Returns true if any value changed.
    pub fn update(&mut self, key_indices: &[usize], delta_batches: &[RecordBatch]) -> bool {
        use crate::query::df_graph::locy_fold::FoldAggKind;

        let mut changed = false;
        for batch in delta_batches {
            for row_idx in 0..batch.num_rows() {
                let group_key = extract_scalar_key(batch, key_indices, row_idx);
                for binding in &self.bindings {
                    let col = batch.column(binding.input_col_index);
                    let val = extract_f64(col.as_ref(), row_idx);
                    if let Some(val) = val {
                        let map_key = (group_key.clone(), binding.fold_name.clone());
                        let entry =
                            self.accumulators
                                .entry(map_key)
                                .or_insert(match binding.kind {
                                    FoldAggKind::Sum
                                    | FoldAggKind::Count
                                    | FoldAggKind::Avg
                                    | FoldAggKind::Nor => 0.0,
                                    FoldAggKind::Max => f64::NEG_INFINITY,
                                    FoldAggKind::Min => f64::INFINITY,
                                    FoldAggKind::Collect => 0.0,
                                    FoldAggKind::Prod => 1.0,
                                });
                        let old = *entry;
                        match binding.kind {
                            FoldAggKind::Sum | FoldAggKind::Count => *entry += val,
                            FoldAggKind::Max => {
                                if val > *entry {
                                    *entry = val;
                                }
                            }
                            FoldAggKind::Min => {
                                if val < *entry {
                                    *entry = val;
                                }
                            }
                            FoldAggKind::Nor => {
                                let p = val.clamp(0.0, 1.0);
                                *entry = 1.0 - (1.0 - *entry) * (1.0 - p);
                            }
                            FoldAggKind::Prod => {
                                let p = val.clamp(0.0, 1.0);
                                *entry *= p;
                            }
                            _ => {}
                        }
                        if (*entry - old).abs() > f64::EPSILON {
                            changed = true;
                        }
                    }
                }
            }
        }
        changed
    }

    /// Take a snapshot of current accumulators for stability comparison.
    pub fn snapshot(&mut self) {
        self.prev_snapshot = self.accumulators.clone();
    }

    /// Check if accumulators are stable (no change since last snapshot).
    pub fn is_stable(&self) -> bool {
        if self.accumulators.len() != self.prev_snapshot.len() {
            return false;
        }
        for (key, val) in &self.accumulators {
            match self.prev_snapshot.get(key) {
                Some(prev) if (*val - *prev).abs() <= f64::EPSILON => {}
                _ => return false,
            }
        }
        true
    }
}

/// Extract f64 value from an Arrow column at a given row index.
fn extract_f64(col: &dyn arrow_array::Array, row_idx: usize) -> Option<f64> {
    if col.is_null(row_idx) {
        return None;
    }
    if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Float64Array>() {
        Some(arr.value(row_idx))
    } else {
        col.as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .map(|arr| arr.value(row_idx) as f64)
    }
}

// ---------------------------------------------------------------------------
// RowDedupState — Arrow RowConverter-based persistent dedup set
// ---------------------------------------------------------------------------

/// Arrow-native row deduplication using [`RowConverter`].
///
/// Unlike the legacy `HashSet<Vec<ScalarKey>>` approach, this struct maintains a
/// persistent `seen` set across iterations so per-iteration cost is O(M) where M
/// is the number of candidate rows — the full facts table is never re-scanned.
struct RowDedupState {
    converter: RowConverter,
    seen: HashSet<Box<[u8]>>,
}

impl RowDedupState {
    /// Try to build a `RowDedupState` for the given schema.
    ///
    /// Returns `None` if any column type is not supported by `RowConverter`
    /// (triggers legacy fallback).
    fn try_new(schema: &SchemaRef) -> Option<Self> {
        let fields: Vec<SortField> = schema
            .fields()
            .iter()
            .map(|f| SortField::new(f.data_type().clone()))
            .collect();
        match RowConverter::new(fields) {
            Ok(converter) => Some(Self {
                converter,
                seen: HashSet::new(),
            }),
            Err(e) => {
                tracing::warn!(
                    "RowDedupState: RowConverter unsupported for schema, falling back to legacy dedup: {}",
                    e
                );
                None
            }
        }
    }

    /// Filter `candidates` to only rows not yet seen, updating the persistent set.
    ///
    /// Both cross-iteration dedup (rows already accepted in prior iterations) and
    /// within-batch dedup (duplicate rows in a single candidate batch) are handled
    /// in a single pass.
    fn compute_delta(
        &mut self,
        candidates: &[RecordBatch],
        schema: &SchemaRef,
    ) -> DFResult<Vec<RecordBatch>> {
        let mut delta_batches = Vec::new();
        for batch in candidates {
            if batch.num_rows() == 0 {
                continue;
            }

            // Vectorized encoding of all rows in this batch.
            let arrays: Vec<_> = batch.columns().to_vec();
            let rows = self
                .converter
                .convert_columns(&arrays)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

            // One pass: check+insert into persistent seen set.
            let mut keep = Vec::with_capacity(batch.num_rows());
            for row_idx in 0..batch.num_rows() {
                let row_bytes: Box<[u8]> = rows.row(row_idx).data().into();
                keep.push(self.seen.insert(row_bytes));
            }

            let keep_mask = arrow_array::BooleanArray::from(keep);
            let new_cols = batch
                .columns()
                .iter()
                .map(|col| {
                    arrow::compute::filter(col.as_ref(), &keep_mask).map_err(|e| {
                        datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                    })
                })
                .collect::<DFResult<Vec<_>>>()?;

            if new_cols.first().is_some_and(|c| !c.is_empty()) {
                let filtered = RecordBatch::try_new(Arc::clone(schema), new_cols).map_err(|e| {
                    datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                })?;
                delta_batches.push(filtered);
            }
        }
        Ok(delta_batches)
    }
}

// ---------------------------------------------------------------------------
// FixpointState — per-rule delta tracking during fixpoint iteration
// ---------------------------------------------------------------------------

/// Per-rule state for fixpoint iteration.
///
/// Tracks accumulated facts and the delta (new facts from the latest iteration).
/// Deduplication uses Arrow [`RowConverter`] with a persistent seen set (O(M) per
/// iteration) when supported, with a legacy `HashSet<Vec<ScalarKey>>` fallback.
pub struct FixpointState {
    rule_name: String,
    facts: Vec<RecordBatch>,
    delta: Vec<RecordBatch>,
    schema: SchemaRef,
    key_column_indices: Vec<usize>,
    /// All column indices for full-row dedup (legacy path only).
    all_column_indices: Vec<usize>,
    /// Running total of facts bytes for memory limit tracking.
    facts_bytes: usize,
    /// Maximum bytes allowed for this derived relation.
    max_derived_bytes: usize,
    /// Optional monotonic aggregate tracking.
    monotonic_agg: Option<MonotonicAggState>,
    /// Arrow RowConverter-based dedup state; `None` triggers legacy fallback.
    row_dedup: Option<RowDedupState>,
}

impl FixpointState {
    /// Create a new fixpoint state for a rule.
    pub fn new(
        rule_name: String,
        schema: SchemaRef,
        key_column_indices: Vec<usize>,
        max_derived_bytes: usize,
        monotonic_agg: Option<MonotonicAggState>,
    ) -> Self {
        let num_cols = schema.fields().len();
        let row_dedup = RowDedupState::try_new(&schema);
        Self {
            rule_name,
            facts: Vec::new(),
            delta: Vec::new(),
            schema,
            key_column_indices,
            all_column_indices: (0..num_cols).collect(),
            facts_bytes: 0,
            max_derived_bytes,
            monotonic_agg,
            row_dedup,
        }
    }

    /// Merge candidate rows into facts, computing delta (truly new rows).
    ///
    /// Returns `true` if any new facts were added.
    pub async fn merge_delta(
        &mut self,
        candidates: Vec<RecordBatch>,
        task_ctx: Option<Arc<TaskContext>>,
    ) -> DFResult<bool> {
        if candidates.is_empty() || candidates.iter().all(|b| b.num_rows() == 0) {
            self.delta.clear();
            return Ok(false);
        }

        // Round floats for stable dedup
        let candidates = round_float_columns(&candidates);

        // Compute delta: rows in candidates not already in facts
        let delta = self.compute_delta(&candidates, task_ctx.as_ref()).await?;

        if delta.is_empty() || delta.iter().all(|b| b.num_rows() == 0) {
            self.delta.clear();
            // Update monotonic aggs even with empty delta (for stability check)
            if let Some(ref mut agg) = self.monotonic_agg {
                agg.snapshot();
            }
            return Ok(false);
        }

        // Check memory limit
        let delta_bytes: usize = delta.iter().map(batch_byte_size).sum();
        if self.facts_bytes + delta_bytes > self.max_derived_bytes {
            return Err(datafusion::error::DataFusionError::Execution(
                LocyRuntimeError::MemoryLimitExceeded {
                    rule: self.rule_name.clone(),
                    bytes: self.facts_bytes + delta_bytes,
                    limit: self.max_derived_bytes,
                }
                .to_string(),
            ));
        }

        // Update monotonic aggs
        if let Some(ref mut agg) = self.monotonic_agg {
            agg.snapshot();
            agg.update(&self.key_column_indices, &delta);
        }

        // Append delta to facts
        self.facts_bytes += delta_bytes;
        self.facts.extend(delta.iter().cloned());
        self.delta = delta;

        Ok(true)
    }

    /// Dispatch to vectorized LeftAntiJoin, Arrow RowConverter dedup, or legacy ScalarKey dedup.
    ///
    /// Priority order:
    /// 1. `arrow_left_anti_dedup` when `total_existing >= DEDUP_ANTI_JOIN_THRESHOLD` and task_ctx available.
    /// 2. `RowDedupState` (persistent HashSet, O(M) per iteration) when schema is supported.
    /// 3. `compute_delta_legacy` (rebuilds from facts, fallback for unsupported column types).
    async fn compute_delta(
        &mut self,
        candidates: &[RecordBatch],
        task_ctx: Option<&Arc<TaskContext>>,
    ) -> DFResult<Vec<RecordBatch>> {
        let total_existing: usize = self.facts.iter().map(|b| b.num_rows()).sum();
        if total_existing >= DEDUP_ANTI_JOIN_THRESHOLD
            && let Some(ctx) = task_ctx
        {
            return arrow_left_anti_dedup(candidates.to_vec(), &self.facts, &self.schema, ctx)
                .await;
        }
        if let Some(ref mut rd) = self.row_dedup {
            rd.compute_delta(candidates, &self.schema)
        } else {
            self.compute_delta_legacy(candidates)
        }
    }

    /// Legacy dedup: rebuild a `HashSet<Vec<ScalarKey>>` from all facts each call.
    ///
    /// Used as fallback when `RowConverter` does not support the schema's column types.
    fn compute_delta_legacy(&self, candidates: &[RecordBatch]) -> DFResult<Vec<RecordBatch>> {
        // Build set of existing fact row keys (ALL columns)
        let mut existing: HashSet<Vec<ScalarKey>> = HashSet::new();
        for batch in &self.facts {
            for row_idx in 0..batch.num_rows() {
                let key = extract_scalar_key(batch, &self.all_column_indices, row_idx);
                existing.insert(key);
            }
        }

        let mut delta_batches = Vec::new();
        for batch in candidates {
            if batch.num_rows() == 0 {
                continue;
            }
            // Filter to only new rows
            let mut keep = Vec::with_capacity(batch.num_rows());
            for row_idx in 0..batch.num_rows() {
                let key = extract_scalar_key(batch, &self.all_column_indices, row_idx);
                keep.push(!existing.contains(&key));
            }

            // Also dedup within the candidate batch itself
            for (row_idx, kept) in keep.iter_mut().enumerate() {
                if *kept {
                    let key = extract_scalar_key(batch, &self.all_column_indices, row_idx);
                    if !existing.insert(key) {
                        *kept = false;
                    }
                }
            }

            let keep_mask = arrow_array::BooleanArray::from(keep);
            let new_rows = batch
                .columns()
                .iter()
                .map(|col| {
                    arrow::compute::filter(col.as_ref(), &keep_mask).map_err(|e| {
                        datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                    })
                })
                .collect::<DFResult<Vec<_>>>()?;

            if new_rows.first().is_some_and(|c| !c.is_empty()) {
                let filtered =
                    RecordBatch::try_new(Arc::clone(&self.schema), new_rows).map_err(|e| {
                        datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                    })?;
                delta_batches.push(filtered);
            }
        }

        Ok(delta_batches)
    }

    /// Check if this rule has converged (no new facts and aggs stable).
    pub fn is_converged(&self) -> bool {
        let delta_empty = self.delta.is_empty() || self.delta.iter().all(|b| b.num_rows() == 0);
        let agg_stable = self.monotonic_agg.as_ref().is_none_or(|a| a.is_stable());
        delta_empty && agg_stable
    }

    /// Get all accumulated facts.
    pub fn all_facts(&self) -> &[RecordBatch] {
        &self.facts
    }

    /// Get the delta from the latest iteration.
    pub fn all_delta(&self) -> &[RecordBatch] {
        &self.delta
    }

    /// Consume self and return facts.
    pub fn into_facts(self) -> Vec<RecordBatch> {
        self.facts
    }
}

/// Estimate byte size of a RecordBatch.
fn batch_byte_size(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|col| col.get_buffer_memory_size())
        .sum()
}

// ---------------------------------------------------------------------------
// Float rounding for stable dedup
// ---------------------------------------------------------------------------

/// Round all Float64 columns to 12 decimal places for stable dedup.
fn round_float_columns(batches: &[RecordBatch]) -> Vec<RecordBatch> {
    batches
        .iter()
        .map(|batch| {
            let schema = batch.schema();
            let has_float = schema
                .fields()
                .iter()
                .any(|f| *f.data_type() == arrow_schema::DataType::Float64);
            if !has_float {
                return batch.clone();
            }

            let columns: Vec<arrow_array::ArrayRef> = batch
                .columns()
                .iter()
                .enumerate()
                .map(|(i, col)| {
                    if *schema.field(i).data_type() == arrow_schema::DataType::Float64 {
                        let arr = col
                            .as_any()
                            .downcast_ref::<arrow_array::Float64Array>()
                            .unwrap();
                        let rounded: arrow_array::Float64Array = arr
                            .iter()
                            .map(|v| v.map(|f| (f * 1e12).round() / 1e12))
                            .collect();
                        Arc::new(rounded) as arrow_array::ArrayRef
                    } else {
                        Arc::clone(col)
                    }
                })
                .collect();

            RecordBatch::try_new(schema, columns).unwrap_or_else(|_| batch.clone())
        })
        .collect()
}

// ---------------------------------------------------------------------------
// LeftAntiJoin delta deduplication
// ---------------------------------------------------------------------------

/// Row threshold above which the vectorized Arrow LeftAntiJoin dedup path is used.
///
/// Below this threshold the persistent `RowDedupState` HashSet is O(M) and
/// avoids rebuilding the existing-row set; above it DataFusion's vectorized
/// HashJoinExec is more cache-efficient.
const DEDUP_ANTI_JOIN_THRESHOLD: usize = 300;

/// Deduplicate `candidates` against `existing` using DataFusion's HashJoinExec.
///
/// Returns rows in `candidates` that do not appear in `existing` (LeftAnti semantics).
/// `null_equals_null = true` so NULLs are treated as equal for dedup purposes.
async fn arrow_left_anti_dedup(
    candidates: Vec<RecordBatch>,
    existing: &[RecordBatch],
    schema: &SchemaRef,
    task_ctx: &Arc<TaskContext>,
) -> DFResult<Vec<RecordBatch>> {
    if existing.is_empty() || existing.iter().all(|b| b.num_rows() == 0) {
        return Ok(candidates);
    }

    let left: Arc<dyn ExecutionPlan> = Arc::new(InMemoryExec::new(candidates, Arc::clone(schema)));
    let right: Arc<dyn ExecutionPlan> =
        Arc::new(InMemoryExec::new(existing.to_vec(), Arc::clone(schema)));

    let on: Vec<(
        Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        Arc<dyn datafusion::physical_plan::PhysicalExpr>,
    )> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let l: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(
                datafusion::physical_plan::expressions::Column::new(field.name(), i),
            );
            let r: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(
                datafusion::physical_plan::expressions::Column::new(field.name(), i),
            );
            (l, r)
        })
        .collect();

    if on.is_empty() {
        return Ok(vec![]);
    }

    let join = HashJoinExec::try_new(
        left,
        right,
        on,
        None,
        &JoinType::LeftAnti,
        None,
        PartitionMode::CollectLeft,
        datafusion::common::NullEquality::NullEqualsNull,
    )?;

    let join_arc: Arc<dyn ExecutionPlan> = Arc::new(join);
    collect_all_partitions(&join_arc, task_ctx.clone()).await
}

// ---------------------------------------------------------------------------
// Plan types for fixpoint rules
// ---------------------------------------------------------------------------

/// IS-ref binding: a reference from a clause body to a derived relation.
#[derive(Debug, Clone)]
pub struct IsRefBinding {
    /// Index into the DerivedScanRegistry.
    pub derived_scan_index: usize,
    /// Name of the rule being referenced.
    pub rule_name: String,
    /// Whether this is a self-reference (rule references itself).
    pub is_self_ref: bool,
    /// Whether this is a negated reference (NOT IS).
    pub negated: bool,
    /// For negated IS-refs: `(left_body_col, right_derived_col)` pairs for anti-join filtering.
    ///
    /// `left_body_col` is the VID column in the clause body (e.g., `"n._vid"`);
    /// `right_derived_col` is the corresponding KEY column in the negated rule's facts (e.g., `"n"`).
    /// Empty for non-negated IS-refs.
    pub anti_join_cols: Vec<(String, String)>,
}

/// A single clause (body) within a fixpoint rule.
#[derive(Debug)]
pub struct FixpointClausePlan {
    /// The logical plan for the clause body.
    pub body_logical: LogicalPlan,
    /// IS-ref bindings used by this clause.
    pub is_ref_bindings: Vec<IsRefBinding>,
    /// Priority value for this clause (if PRIORITY semantics apply).
    pub priority: Option<i64>,
}

/// Physical plan for a single rule in a fixpoint stratum.
#[derive(Debug)]
pub struct FixpointRulePlan {
    /// Rule name.
    pub name: String,
    /// Clause bodies (each evaluates to candidate rows).
    pub clauses: Vec<FixpointClausePlan>,
    /// Output schema for this rule's derived relation.
    pub yield_schema: SchemaRef,
    /// Indices of KEY columns within yield_schema.
    pub key_column_indices: Vec<usize>,
    /// Priority value (if PRIORITY semantics apply).
    pub priority: Option<i64>,
    /// Whether this rule has FOLD semantics.
    pub has_fold: bool,
    /// FOLD bindings for post-fixpoint aggregation.
    pub fold_bindings: Vec<FoldBinding>,
    /// Whether this rule has BEST BY semantics.
    pub has_best_by: bool,
    /// BEST BY sort criteria for post-fixpoint selection.
    pub best_by_criteria: Vec<SortCriterion>,
    /// Whether this rule has PRIORITY semantics.
    pub has_priority: bool,
    /// Whether BEST BY should apply a deterministic secondary sort for
    /// tie-breaking. When false, tied rows are selected non-deterministically
    /// (faster but not repeatable across runs).
    pub deterministic: bool,
}

// ---------------------------------------------------------------------------
// run_fixpoint_loop — the core semi-naive iteration algorithm
// ---------------------------------------------------------------------------

/// Run the semi-naive fixpoint iteration loop.
///
/// Evaluates all rules in a stratum repeatedly, feeding deltas back through
/// derived scan handles until convergence or limits are reached.
#[allow(clippy::too_many_arguments)]
async fn run_fixpoint_loop(
    rules: Vec<FixpointRulePlan>,
    max_iterations: usize,
    timeout: Duration,
    graph_ctx: Arc<GraphExecutionContext>,
    session_ctx: Arc<RwLock<datafusion::prelude::SessionContext>>,
    storage: Arc<StorageManager>,
    schema_info: Arc<UniSchema>,
    params: HashMap<String, Value>,
    registry: Arc<DerivedScanRegistry>,
    output_schema: SchemaRef,
    max_derived_bytes: usize,
    derivation_tracker: Option<Arc<DerivationTracker>>,
    iteration_counts: Arc<StdRwLock<HashMap<String, usize>>>,
) -> DFResult<Vec<RecordBatch>> {
    let start = Instant::now();
    let task_ctx = session_ctx.read().task_ctx();

    // Initialize per-rule state
    let mut states: Vec<FixpointState> = rules
        .iter()
        .map(|rule| {
            let monotonic_agg = if !rule.fold_bindings.is_empty() {
                let bindings: Vec<MonotonicFoldBinding> = rule
                    .fold_bindings
                    .iter()
                    .map(|fb| MonotonicFoldBinding {
                        fold_name: fb.output_name.clone(),
                        kind: fb.kind.clone(),
                        input_col_index: fb.input_col_index,
                    })
                    .collect();
                Some(MonotonicAggState::new(bindings))
            } else {
                None
            };
            FixpointState::new(
                rule.name.clone(),
                Arc::clone(&rule.yield_schema),
                rule.key_column_indices.clone(),
                max_derived_bytes,
                monotonic_agg,
            )
        })
        .collect();

    // Main iteration loop
    let mut converged = false;
    let mut total_iters = 0usize;
    for iteration in 0..max_iterations {
        total_iters = iteration + 1;
        tracing::debug!("fixpoint iteration {}", iteration);
        let mut any_changed = false;

        for rule_idx in 0..rules.len() {
            let rule = &rules[rule_idx];

            // Update derived scan handles for this rule's clauses
            update_derived_scan_handles(&registry, &states, rule_idx, &rules);

            // Evaluate clause bodies, tracking per-clause candidates for provenance.
            let mut all_candidates = Vec::new();
            let mut clause_candidates: Vec<Vec<RecordBatch>> = Vec::new();
            for clause in &rule.clauses {
                let mut batches = execute_subplan(
                    &clause.body_logical,
                    &params,
                    &HashMap::new(),
                    &graph_ctx,
                    &session_ctx,
                    &storage,
                    &schema_info,
                )
                .await?;
                // Apply anti-joins for negated IS-refs (IS NOT semantics).
                for binding in &clause.is_ref_bindings {
                    if binding.negated
                        && !binding.anti_join_cols.is_empty()
                        && let Some(entry) = registry.get(binding.derived_scan_index)
                    {
                        let neg_facts = entry.data.read().clone();
                        if !neg_facts.is_empty() {
                            for (left_col, right_col) in &binding.anti_join_cols {
                                batches =
                                    apply_anti_join(batches, &neg_facts, left_col, right_col)?;
                            }
                        }
                    }
                }
                clause_candidates.push(batches.clone());
                all_candidates.extend(batches);
            }

            // Merge delta
            let changed = states[rule_idx]
                .merge_delta(all_candidates, Some(Arc::clone(&task_ctx)))
                .await?;
            if changed {
                any_changed = true;
                // Record provenance for newly derived facts when tracker is present.
                if let Some(ref tracker) = derivation_tracker {
                    record_provenance(
                        tracker,
                        rule,
                        &states[rule_idx],
                        &clause_candidates,
                        iteration,
                    );
                }
            }
        }

        // Check convergence
        if !any_changed && states.iter().all(|s| s.is_converged()) {
            tracing::debug!("fixpoint converged after {} iterations", iteration + 1);
            converged = true;
            break;
        }

        // Check timeout
        if start.elapsed() > timeout {
            return Err(datafusion::error::DataFusionError::Execution(
                LocyRuntimeError::NonConvergence {
                    iterations: iteration + 1,
                }
                .to_string(),
            ));
        }
    }

    // Write per-rule iteration counts to the shared slot.
    if let Ok(mut counts) = iteration_counts.write() {
        for rule in &rules {
            counts.insert(rule.name.clone(), total_iters);
        }
    }

    // If we exhausted all iterations without converging, return a non-convergence error.
    if !converged {
        return Err(datafusion::error::DataFusionError::Execution(
            LocyRuntimeError::NonConvergence {
                iterations: max_iterations,
            }
            .to_string(),
        ));
    }

    // Post-fixpoint processing per rule and collect output
    let task_ctx = session_ctx.read().task_ctx();
    let mut all_output = Vec::new();

    for (rule_idx, state) in states.into_iter().enumerate() {
        let rule = &rules[rule_idx];
        let facts = state.into_facts();
        if facts.is_empty() {
            continue;
        }

        let processed = apply_post_fixpoint_chain(facts, rule, &task_ctx).await?;
        all_output.extend(processed);
    }

    // If no output, return empty batch with output schema
    if all_output.is_empty() {
        all_output.push(RecordBatch::new_empty(output_schema));
    }

    Ok(all_output)
}

// ---------------------------------------------------------------------------
// Provenance recording helpers
// ---------------------------------------------------------------------------

/// Record provenance for all newly derived facts (rows in the current delta).
///
/// Called after `merge_delta` returns `true`. Attributes each new fact to the
/// clause most likely to have produced it, using first-derivation-wins semantics.
fn record_provenance(
    tracker: &Arc<DerivationTracker>,
    rule: &FixpointRulePlan,
    state: &FixpointState,
    clause_candidates: &[Vec<RecordBatch>],
    iteration: usize,
) {
    let all_indices: Vec<usize> = (0..rule.yield_schema.fields().len()).collect();

    for delta_batch in state.all_delta() {
        for row_idx in 0..delta_batch.num_rows() {
            let row_hash = format!(
                "{:?}",
                extract_scalar_key(delta_batch, &all_indices, row_idx)
            )
            .into_bytes();
            let fact_row = batch_row_to_value_map(delta_batch, row_idx);
            let clause_index =
                find_clause_for_row(delta_batch, row_idx, &all_indices, clause_candidates);

            let entry = DerivationEntry {
                rule_name: rule.name.clone(),
                clause_index,
                inputs: vec![],
                along_values: std::collections::HashMap::new(),
                iteration,
                fact_row,
            };
            tracker.record(row_hash, entry);
        }
    }
}

/// Determine which clause produced a given row by checking each clause's candidates.
///
/// Returns the index of the first clause whose candidates contain a matching row.
/// Falls back to 0 if no match is found.
fn find_clause_for_row(
    delta_batch: &RecordBatch,
    row_idx: usize,
    all_indices: &[usize],
    clause_candidates: &[Vec<RecordBatch>],
) -> usize {
    let target_key = extract_scalar_key(delta_batch, all_indices, row_idx);
    for (clause_idx, batches) in clause_candidates.iter().enumerate() {
        for batch in batches {
            if batch.num_columns() != all_indices.len() {
                continue;
            }
            for r in 0..batch.num_rows() {
                if extract_scalar_key(batch, all_indices, r) == target_key {
                    return clause_idx;
                }
            }
        }
    }
    0
}

/// Convert a single row from a `RecordBatch` at `row_idx` into a `HashMap<String, Value>`.
fn batch_row_to_value_map(
    batch: &RecordBatch,
    row_idx: usize,
) -> std::collections::HashMap<String, Value> {
    use uni_store::storage::arrow_convert::arrow_to_value;

    let schema = batch.schema();
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(col_idx, field)| {
            let col = batch.column(col_idx);
            let val = arrow_to_value(col.as_ref(), row_idx, None);
            (field.name().clone(), val)
        })
        .collect()
}

/// Filter `batches` to exclude rows where `left_col` VID appears in `neg_facts[right_col]`.
///
/// Implements anti-join semantics for negated IS-refs (`n IS NOT rule`): keeps only
/// rows whose subject VID is NOT present in the negated rule's fully-converged facts.
pub fn apply_anti_join(
    batches: Vec<RecordBatch>,
    neg_facts: &[RecordBatch],
    left_col: &str,
    right_col: &str,
) -> datafusion::error::Result<Vec<RecordBatch>> {
    use arrow::compute::filter_record_batch;
    use arrow_array::{Array as _, BooleanArray, UInt64Array};

    // Collect right-side VIDs from the negated rule's derived facts.
    let mut banned: std::collections::HashSet<u64> = std::collections::HashSet::new();
    for batch in neg_facts {
        let Ok(idx) = batch.schema().index_of(right_col) else {
            continue;
        };
        let arr = batch.column(idx);
        let Some(vids) = arr.as_any().downcast_ref::<UInt64Array>() else {
            continue;
        };
        for i in 0..vids.len() {
            if !vids.is_null(i) {
                banned.insert(vids.value(i));
            }
        }
    }

    if banned.is_empty() {
        return Ok(batches);
    }

    // Filter body batches: keep rows where left_col NOT IN banned.
    let mut result = Vec::new();
    for batch in batches {
        let Ok(idx) = batch.schema().index_of(left_col) else {
            result.push(batch);
            continue;
        };
        let arr = batch.column(idx);
        let Some(vids) = arr.as_any().downcast_ref::<UInt64Array>() else {
            result.push(batch);
            continue;
        };
        let keep: Vec<bool> = (0..vids.len())
            .map(|i| vids.is_null(i) || !banned.contains(&vids.value(i)))
            .collect();
        let keep_arr = BooleanArray::from(keep);
        let filtered = filter_record_batch(&batch, &keep_arr)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
        if filtered.num_rows() > 0 {
            result.push(filtered);
        }
    }
    Ok(result)
}

/// Update derived scan handles before evaluating a rule's clause bodies.
///
/// For self-references: inject delta (semi-naive optimization).
/// For cross-references: inject full facts.
fn update_derived_scan_handles(
    registry: &DerivedScanRegistry,
    states: &[FixpointState],
    current_rule_idx: usize,
    rules: &[FixpointRulePlan],
) {
    let current_rule_name = &rules[current_rule_idx].name;

    for entry in &registry.entries {
        // Find the state for this entry's rule
        let source_state_idx = rules.iter().position(|r| r.name == entry.rule_name);
        let Some(source_idx) = source_state_idx else {
            continue;
        };

        let is_self = entry.rule_name == *current_rule_name;
        let data = if is_self {
            // Self-ref: inject delta for semi-naive
            states[source_idx].all_delta().to_vec()
        } else {
            // Cross-ref: inject full facts
            states[source_idx].all_facts().to_vec()
        };

        // If empty, write an empty batch so the scan returns zero rows
        let data = if data.is_empty() || data.iter().all(|b| b.num_rows() == 0) {
            vec![RecordBatch::new_empty(Arc::clone(&entry.schema))]
        } else {
            data
        };

        let mut guard = entry.data.write();
        *guard = data;
    }
}

// ---------------------------------------------------------------------------
// DerivedScanExec — physical plan that reads from shared data at execution time
// ---------------------------------------------------------------------------

/// Physical plan for `LocyDerivedScan` that reads from a shared `Arc<RwLock>` at
/// execution time (not at plan creation time).
///
/// This is critical for fixpoint iteration: the data handle is updated between
/// iterations, and each re-execution of the subplan must read the latest data.
pub struct DerivedScanExec {
    data: Arc<RwLock<Vec<RecordBatch>>>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl DerivedScanExec {
    pub fn new(data: Arc<RwLock<Vec<RecordBatch>>>, schema: SchemaRef) -> Self {
        let properties = compute_plan_properties(Arc::clone(&schema));
        Self {
            data,
            schema,
            properties,
        }
    }
}

impl fmt::Debug for DerivedScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DerivedScanExec")
            .field("schema", &self.schema)
            .finish()
    }
}

impl DisplayAs for DerivedScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DerivedScanExec")
    }
}

impl ExecutionPlan for DerivedScanExec {
    fn name(&self) -> &str {
        "DerivedScanExec"
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
        let batches = {
            let guard = self.data.read();
            if guard.is_empty() {
                vec![RecordBatch::new_empty(Arc::clone(&self.schema))]
            } else {
                guard.clone()
            }
        };
        Ok(Box::pin(MemoryStream::try_new(
            batches,
            Arc::clone(&self.schema),
            None,
        )?))
    }
}

// ---------------------------------------------------------------------------
// InMemoryExec — wrapper to feed Vec<RecordBatch> into operator chains
// ---------------------------------------------------------------------------

/// Simple in-memory execution plan that serves pre-computed batches.
///
/// Used internally to feed fixpoint results into post-fixpoint operator chains
/// (FOLD, BEST BY). Not exported — only used within this module.
struct InMemoryExec {
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl InMemoryExec {
    fn new(batches: Vec<RecordBatch>, schema: SchemaRef) -> Self {
        let properties = compute_plan_properties(Arc::clone(&schema));
        Self {
            batches,
            schema,
            properties,
        }
    }
}

impl fmt::Debug for InMemoryExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryExec")
            .field("num_batches", &self.batches.len())
            .field("schema", &self.schema)
            .finish()
    }
}

impl DisplayAs for InMemoryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InMemoryExec: batches={}", self.batches.len())
    }
}

impl ExecutionPlan for InMemoryExec {
    fn name(&self) -> &str {
        "InMemoryExec"
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

// ---------------------------------------------------------------------------
// Post-fixpoint chain — FOLD and BEST BY on converged facts
// ---------------------------------------------------------------------------

/// Apply post-fixpoint operators (FOLD, BEST BY, PRIORITY) to converged facts.
pub(crate) async fn apply_post_fixpoint_chain(
    facts: Vec<RecordBatch>,
    rule: &FixpointRulePlan,
    task_ctx: &Arc<TaskContext>,
) -> DFResult<Vec<RecordBatch>> {
    if !rule.has_fold && !rule.has_best_by && !rule.has_priority {
        return Ok(facts);
    }

    // Wrap facts in InMemoryExec
    let schema = Arc::clone(&rule.yield_schema);
    let input: Arc<dyn ExecutionPlan> = Arc::new(InMemoryExec::new(facts, schema));

    // Apply PRIORITY first — keeps only rows with max __priority per KEY group,
    // then strips the __priority column from output.
    // Must run before FOLD so that the __priority column is still present.
    let current: Arc<dyn ExecutionPlan> = if rule.has_priority {
        let priority_schema = input.schema();
        let priority_idx = priority_schema.index_of("__priority").map_err(|_| {
            datafusion::common::DataFusionError::Internal(
                "PRIORITY rule missing __priority column".to_string(),
            )
        })?;
        Arc::new(PriorityExec::new(
            input,
            rule.key_column_indices.clone(),
            priority_idx,
        ))
    } else {
        input
    };

    // Apply FOLD
    let current: Arc<dyn ExecutionPlan> = if rule.has_fold && !rule.fold_bindings.is_empty() {
        Arc::new(FoldExec::new(
            current,
            rule.key_column_indices.clone(),
            rule.fold_bindings.clone(),
        ))
    } else {
        current
    };

    // Apply BEST BY
    let current: Arc<dyn ExecutionPlan> = if rule.has_best_by && !rule.best_by_criteria.is_empty() {
        Arc::new(BestByExec::new(
            current,
            rule.key_column_indices.clone(),
            rule.best_by_criteria.clone(),
            rule.deterministic,
        ))
    } else {
        current
    };

    collect_all_partitions(&current, Arc::clone(task_ctx)).await
}

// ---------------------------------------------------------------------------
// FixpointExec — DataFusion ExecutionPlan
// ---------------------------------------------------------------------------

/// DataFusion `ExecutionPlan` that drives semi-naive fixpoint iteration.
///
/// Has no physical children: clause bodies are re-planned from logical plans
/// on each iteration (same pattern as `RecursiveCTEExec` and `GraphApplyExec`).
pub struct FixpointExec {
    rules: Vec<FixpointRulePlan>,
    max_iterations: usize,
    timeout: Duration,
    graph_ctx: Arc<GraphExecutionContext>,
    session_ctx: Arc<RwLock<datafusion::prelude::SessionContext>>,
    storage: Arc<StorageManager>,
    schema_info: Arc<UniSchema>,
    params: HashMap<String, Value>,
    derived_scan_registry: Arc<DerivedScanRegistry>,
    output_schema: SchemaRef,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
    max_derived_bytes: usize,
    /// Optional provenance tracker populated during fixpoint iteration.
    derivation_tracker: Option<Arc<DerivationTracker>>,
    /// Shared slot written with per-rule iteration counts after convergence.
    iteration_counts: Arc<StdRwLock<HashMap<String, usize>>>,
}

impl fmt::Debug for FixpointExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FixpointExec")
            .field("rules_count", &self.rules.len())
            .field("max_iterations", &self.max_iterations)
            .field("timeout", &self.timeout)
            .field("output_schema", &self.output_schema)
            .field("max_derived_bytes", &self.max_derived_bytes)
            .finish_non_exhaustive()
    }
}

impl FixpointExec {
    /// Create a new `FixpointExec`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rules: Vec<FixpointRulePlan>,
        max_iterations: usize,
        timeout: Duration,
        graph_ctx: Arc<GraphExecutionContext>,
        session_ctx: Arc<RwLock<datafusion::prelude::SessionContext>>,
        storage: Arc<StorageManager>,
        schema_info: Arc<UniSchema>,
        params: HashMap<String, Value>,
        derived_scan_registry: Arc<DerivedScanRegistry>,
        output_schema: SchemaRef,
        max_derived_bytes: usize,
        derivation_tracker: Option<Arc<DerivationTracker>>,
        iteration_counts: Arc<StdRwLock<HashMap<String, usize>>>,
    ) -> Self {
        let properties = compute_plan_properties(Arc::clone(&output_schema));
        Self {
            rules,
            max_iterations,
            timeout,
            graph_ctx,
            session_ctx,
            storage,
            schema_info,
            params,
            derived_scan_registry,
            output_schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            max_derived_bytes,
            derivation_tracker,
            iteration_counts,
        }
    }

    /// Returns the shared iteration counts slot for post-execution inspection.
    pub fn iteration_counts(&self) -> Arc<StdRwLock<HashMap<String, usize>>> {
        Arc::clone(&self.iteration_counts)
    }
}

impl DisplayAs for FixpointExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FixpointExec: rules=[{}], max_iter={}, timeout={:?}",
            self.rules
                .iter()
                .map(|r| r.name.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            self.max_iterations,
            self.timeout,
        )
    }
}

impl ExecutionPlan for FixpointExec {
    fn name(&self) -> &str {
        "FixpointExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // No physical children — clause bodies are re-planned each iteration
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Plan(
                "FixpointExec has no children".to_string(),
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

        // Clone all fields for the async closure
        let rules = self
            .rules
            .iter()
            .map(|r| {
                // We need to clone the FixpointRulePlan, but it contains LogicalPlan
                // which doesn't implement Clone traditionally. However, our LogicalPlan
                // does implement Clone since it's an enum.
                FixpointRulePlan {
                    name: r.name.clone(),
                    clauses: r
                        .clauses
                        .iter()
                        .map(|c| FixpointClausePlan {
                            body_logical: c.body_logical.clone(),
                            is_ref_bindings: c.is_ref_bindings.clone(),
                            priority: c.priority,
                        })
                        .collect(),
                    yield_schema: Arc::clone(&r.yield_schema),
                    key_column_indices: r.key_column_indices.clone(),
                    priority: r.priority,
                    has_fold: r.has_fold,
                    fold_bindings: r.fold_bindings.clone(),
                    has_best_by: r.has_best_by,
                    best_by_criteria: r.best_by_criteria.clone(),
                    has_priority: r.has_priority,
                    deterministic: r.deterministic,
                }
            })
            .collect();

        let max_iterations = self.max_iterations;
        let timeout = self.timeout;
        let graph_ctx = Arc::clone(&self.graph_ctx);
        let session_ctx = Arc::clone(&self.session_ctx);
        let storage = Arc::clone(&self.storage);
        let schema_info = Arc::clone(&self.schema_info);
        let params = self.params.clone();
        let registry = Arc::clone(&self.derived_scan_registry);
        let output_schema = Arc::clone(&self.output_schema);
        let max_derived_bytes = self.max_derived_bytes;
        let derivation_tracker = self.derivation_tracker.clone();
        let iteration_counts = Arc::clone(&self.iteration_counts);

        let fut = async move {
            run_fixpoint_loop(
                rules,
                max_iterations,
                timeout,
                graph_ctx,
                session_ctx,
                storage,
                schema_info,
                params,
                registry,
                output_schema,
                max_derived_bytes,
                derivation_tracker,
                iteration_counts,
            )
            .await
        };

        Ok(Box::pin(FixpointStream {
            state: FixpointStreamState::Running(Box::pin(fut)),
            schema: Arc::clone(&self.output_schema),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

// ---------------------------------------------------------------------------
// FixpointStream — async state machine for streaming results
// ---------------------------------------------------------------------------

enum FixpointStreamState {
    /// Fixpoint loop is running.
    Running(Pin<Box<dyn std::future::Future<Output = DFResult<Vec<RecordBatch>>> + Send>>),
    /// Emitting accumulated result batches one at a time.
    Emitting(Vec<RecordBatch>, usize),
    /// All batches emitted.
    Done,
}

struct FixpointStream {
    state: FixpointStreamState,
    schema: SchemaRef,
    metrics: BaselineMetrics,
}

impl Stream for FixpointStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                FixpointStreamState::Running(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(batches)) => {
                        if batches.is_empty() {
                            this.state = FixpointStreamState::Done;
                            return Poll::Ready(None);
                        }
                        this.state = FixpointStreamState::Emitting(batches, 0);
                        // Loop to emit first batch
                    }
                    Poll::Ready(Err(e)) => {
                        this.state = FixpointStreamState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                FixpointStreamState::Emitting(batches, idx) => {
                    if *idx >= batches.len() {
                        this.state = FixpointStreamState::Done;
                        return Poll::Ready(None);
                    }
                    let batch = batches[*idx].clone();
                    *idx += 1;
                    this.metrics.record_output(batch.num_rows());
                    return Poll::Ready(Some(Ok(batch)));
                }
                FixpointStreamState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for FixpointStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    fn make_batch(names: &[&str], values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(StringArray::from(
                    names.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    // --- FixpointState dedup tests ---

    #[tokio::test]
    async fn test_fixpoint_state_empty_facts_adds_all() {
        let schema = test_schema();
        let mut state = FixpointState::new("test".into(), schema, vec![0], 1_000_000, None);

        let batch = make_batch(&["a", "b", "c"], &[1, 2, 3]);
        let changed = state.merge_delta(vec![batch], None).await.unwrap();

        assert!(changed);
        assert_eq!(state.all_facts().len(), 1);
        assert_eq!(state.all_facts()[0].num_rows(), 3);
        assert_eq!(state.all_delta().len(), 1);
        assert_eq!(state.all_delta()[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_fixpoint_state_exact_duplicates_excluded() {
        let schema = test_schema();
        let mut state = FixpointState::new("test".into(), schema, vec![0], 1_000_000, None);

        let batch1 = make_batch(&["a", "b"], &[1, 2]);
        state.merge_delta(vec![batch1], None).await.unwrap();

        // Same rows again
        let batch2 = make_batch(&["a", "b"], &[1, 2]);
        let changed = state.merge_delta(vec![batch2], None).await.unwrap();
        assert!(!changed);
        assert!(
            state.all_delta().is_empty() || state.all_delta().iter().all(|b| b.num_rows() == 0)
        );
    }

    #[tokio::test]
    async fn test_fixpoint_state_partial_overlap() {
        let schema = test_schema();
        let mut state = FixpointState::new("test".into(), schema, vec![0], 1_000_000, None);

        let batch1 = make_batch(&["a", "b"], &[1, 2]);
        state.merge_delta(vec![batch1], None).await.unwrap();

        // "a":1 is duplicate, "c":3 is new
        let batch2 = make_batch(&["a", "c"], &[1, 3]);
        let changed = state.merge_delta(vec![batch2], None).await.unwrap();
        assert!(changed);

        // Delta should have only "c":3
        let delta_rows: usize = state.all_delta().iter().map(|b| b.num_rows()).sum();
        assert_eq!(delta_rows, 1);

        // Total facts: a:1, b:2, c:3
        let total_rows: usize = state.all_facts().iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_fixpoint_state_convergence() {
        let schema = test_schema();
        let mut state = FixpointState::new("test".into(), schema, vec![0], 1_000_000, None);

        let batch = make_batch(&["a"], &[1]);
        state.merge_delta(vec![batch], None).await.unwrap();

        // Empty candidates → converged
        let changed = state.merge_delta(vec![], None).await.unwrap();
        assert!(!changed);
        assert!(state.is_converged());
    }

    // --- RowDedupState tests ---

    #[test]
    fn test_row_dedup_persistent_across_calls() {
        // RowDedupState should remember rows from the first call so the second
        // call does not re-accept them (O(M) per iteration, no facts re-scan).
        let schema = test_schema();
        let mut rd = RowDedupState::try_new(&schema).expect("schema should be supported");

        let batch1 = make_batch(&["a", "b"], &[1, 2]);
        let delta1 = rd.compute_delta(&[batch1], &schema).unwrap();
        // First call: both rows are new.
        let rows1: usize = delta1.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows1, 2);

        // Second call with same rows: seen set already has them → empty delta.
        let batch2 = make_batch(&["a", "b"], &[1, 2]);
        let delta2 = rd.compute_delta(&[batch2], &schema).unwrap();
        let rows2: usize = delta2.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows2, 0);

        // Third call with one old + one new: only the new row is returned.
        let batch3 = make_batch(&["a", "c"], &[1, 3]);
        let delta3 = rd.compute_delta(&[batch3], &schema).unwrap();
        let rows3: usize = delta3.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows3, 1);
    }

    #[test]
    fn test_row_dedup_null_handling() {
        use arrow_array::StringArray;
        use arrow_schema::{DataType, Field, Schema};

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let mut rd = RowDedupState::try_new(&schema).expect("schema should be supported");

        // Two rows: (NULL, 1) and (NULL, 1) — same NULLs → duplicate.
        let batch_nulls = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![None::<&str>, None::<&str>])),
                Arc::new(arrow_array::Int64Array::from(vec![1i64, 1i64])),
            ],
        )
        .unwrap();
        let delta = rd.compute_delta(&[batch_nulls], &schema).unwrap();
        let rows: usize = delta.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "two identical NULL rows should be deduped to one");

        // (NULL, 2) — NULL in same col but different non-null col → distinct.
        let batch_diff = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(arrow_array::Int64Array::from(vec![2i64])),
            ],
        )
        .unwrap();
        let delta2 = rd.compute_delta(&[batch_diff], &schema).unwrap();
        let rows2: usize = delta2.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows2, 1, "(NULL, 2) is distinct from (NULL, 1)");
    }

    #[test]
    fn test_row_dedup_within_candidate_dedup() {
        // Duplicate rows within a single candidate batch should be collapsed to one.
        let schema = test_schema();
        let mut rd = RowDedupState::try_new(&schema).expect("schema should be supported");

        // Batch with three rows: a:1, a:1, b:2 — "a:1" appears twice.
        let batch = make_batch(&["a", "a", "b"], &[1, 1, 2]);
        let delta = rd.compute_delta(&[batch], &schema).unwrap();
        let rows: usize = delta.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "within-batch dup should be collapsed: a:1, b:2");
    }

    // --- Float rounding tests ---

    #[test]
    fn test_round_float_columns_near_duplicates() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("dist", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("a"), Some("a")])),
                Arc::new(Float64Array::from(vec![1.0000000000001, 1.0000000000002])),
            ],
        )
        .unwrap();

        let rounded = round_float_columns(&[batch]);
        assert_eq!(rounded.len(), 1);
        let col = rounded[0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // Both should round to same value
        assert_eq!(col.value(0), col.value(1));
    }

    // --- DerivedScanRegistry tests ---

    #[test]
    fn test_registry_write_read_round_trip() {
        let schema = test_schema();
        let data = Arc::new(RwLock::new(Vec::new()));
        let mut reg = DerivedScanRegistry::new();
        reg.add(DerivedScanEntry {
            scan_index: 0,
            rule_name: "reachable".into(),
            is_self_ref: true,
            data: Arc::clone(&data),
            schema: Arc::clone(&schema),
        });

        let batch = make_batch(&["x"], &[42]);
        reg.write_data(0, vec![batch.clone()]);

        let entry = reg.get(0).unwrap();
        let guard = entry.data.read();
        assert_eq!(guard.len(), 1);
        assert_eq!(guard[0].num_rows(), 1);
    }

    #[test]
    fn test_registry_entries_for_rule() {
        let schema = test_schema();
        let mut reg = DerivedScanRegistry::new();
        reg.add(DerivedScanEntry {
            scan_index: 0,
            rule_name: "r1".into(),
            is_self_ref: true,
            data: Arc::new(RwLock::new(Vec::new())),
            schema: Arc::clone(&schema),
        });
        reg.add(DerivedScanEntry {
            scan_index: 1,
            rule_name: "r2".into(),
            is_self_ref: false,
            data: Arc::new(RwLock::new(Vec::new())),
            schema: Arc::clone(&schema),
        });
        reg.add(DerivedScanEntry {
            scan_index: 2,
            rule_name: "r1".into(),
            is_self_ref: false,
            data: Arc::new(RwLock::new(Vec::new())),
            schema: Arc::clone(&schema),
        });

        assert_eq!(reg.entries_for_rule("r1").len(), 2);
        assert_eq!(reg.entries_for_rule("r2").len(), 1);
        assert_eq!(reg.entries_for_rule("r3").len(), 0);
    }

    // --- MonotonicAggState tests ---

    #[test]
    fn test_monotonic_agg_update_and_stability() {
        use crate::query::df_graph::locy_fold::FoldAggKind;

        let bindings = vec![MonotonicFoldBinding {
            fold_name: "total".into(),
            kind: FoldAggKind::Sum,
            input_col_index: 1,
        }];
        let mut agg = MonotonicAggState::new(bindings);

        // First update
        let batch = make_batch(&["a"], &[10]);
        agg.snapshot();
        let changed = agg.update(&[0], &[batch]);
        assert!(changed);
        assert!(!agg.is_stable()); // changed since snapshot

        // Snapshot and check stability with no new data
        agg.snapshot();
        let changed = agg.update(&[0], &[]);
        assert!(!changed);
        assert!(agg.is_stable());
    }

    // --- Memory limit test ---

    #[tokio::test]
    async fn test_memory_limit_exceeded() {
        let schema = test_schema();
        // Set a tiny limit
        let mut state = FixpointState::new("test".into(), schema, vec![0], 1, None);

        let batch = make_batch(&["a", "b", "c"], &[1, 2, 3]);
        let result = state.merge_delta(vec![batch], None).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("memory limit"), "Error was: {}", err);
    }

    // --- FixpointStream lifecycle test ---

    #[tokio::test]
    async fn test_fixpoint_stream_emitting() {
        use futures::StreamExt;

        let schema = test_schema();
        let batch1 = make_batch(&["a"], &[1]);
        let batch2 = make_batch(&["b"], &[2]);

        let metrics = ExecutionPlanMetricsSet::new();
        let baseline = BaselineMetrics::new(&metrics, 0);

        let mut stream = FixpointStream {
            state: FixpointStreamState::Emitting(vec![batch1, batch2], 0),
            schema,
            metrics: baseline,
        };

        let stream = Pin::new(&mut stream);
        let batches: Vec<RecordBatch> = stream.filter_map(|r| async { r.ok() }).collect().await;

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[1].num_rows(), 1);
    }
}
