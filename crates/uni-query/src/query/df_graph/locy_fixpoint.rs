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
use crate::query::df_graph::locy_explain::{DerivationEntry, DerivationInput, DerivationTracker};
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
use uni_locy::RuntimeWarning;
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

    /// Update accumulators with new delta batches.
    ///
    /// Returns `true` if any accumulator value changed. When `strict` is
    /// `true`, Nor/Prod inputs outside `[0, 1]` produce an error instead
    /// of being clamped.
    pub fn update(
        &mut self,
        key_indices: &[usize],
        delta_batches: &[RecordBatch],
        strict: bool,
    ) -> DFResult<bool> {
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
                        let entry = self
                            .accumulators
                            .entry(map_key)
                            .or_insert(binding.kind.identity().unwrap_or(0.0));
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
                                if strict && !(0.0..=1.0).contains(&val) {
                                    return Err(datafusion::error::DataFusionError::Execution(
                                        format!(
                                            "strict_probability_domain: MNOR input {val} is outside [0, 1]"
                                        ),
                                    ));
                                }
                                if !strict && !(0.0..=1.0).contains(&val) {
                                    tracing::warn!(
                                        "MNOR input {val} outside [0,1], clamped to {}",
                                        val.clamp(0.0, 1.0)
                                    );
                                }
                                let p = val.clamp(0.0, 1.0);
                                *entry = 1.0 - (1.0 - *entry) * (1.0 - p);
                            }
                            FoldAggKind::Prod => {
                                if strict && !(0.0..=1.0).contains(&val) {
                                    return Err(datafusion::error::DataFusionError::Execution(
                                        format!(
                                            "strict_probability_domain: MPROD input {val} is outside [0, 1]"
                                        ),
                                    ));
                                }
                                if !strict && !(0.0..=1.0).contains(&val) {
                                    tracing::warn!(
                                        "MPROD input {val} outside [0,1], clamped to {}",
                                        val.clamp(0.0, 1.0)
                                    );
                                }
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
        Ok(changed)
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

    /// Test-only accessor for accumulator values.
    #[cfg(test)]
    pub(crate) fn get_accumulator(&self, key: &(Vec<ScalarKey>, String)) -> Option<f64> {
        self.accumulators.get(key).copied()
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
    /// Whether strict probability domain checks are enabled.
    strict_probability_domain: bool,
}

impl FixpointState {
    /// Create a new fixpoint state for a rule.
    pub fn new(
        rule_name: String,
        schema: SchemaRef,
        key_column_indices: Vec<usize>,
        max_derived_bytes: usize,
        monotonic_agg: Option<MonotonicAggState>,
        strict_probability_domain: bool,
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
            strict_probability_domain,
        }
    }

    /// Reconcile the pre-computed schema with the actual physical plan output.
    ///
    /// `infer_expr_type` may guess wrong (e.g. `Property → Float64` for a
    /// string column).  When the first real batch arrives with a different
    /// schema, update ours so that `RowDedupState` / `RecordBatch::try_new`
    /// use the correct types.
    fn reconcile_schema(&mut self, actual_schema: &SchemaRef) {
        if self.schema.fields() != actual_schema.fields() {
            tracing::debug!(
                rule = %self.rule_name,
                "Reconciling fixpoint schema from physical plan output",
            );
            self.schema = Arc::clone(actual_schema);
            self.row_dedup = RowDedupState::try_new(&self.schema);
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

        // Reconcile schema from the first non-empty candidate batch.
        // The physical plan's output types are authoritative over the
        // planner's inferred types.
        if let Some(first) = candidates.iter().find(|b| b.num_rows() > 0) {
            self.reconcile_schema(&first.schema());
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
            agg.update(
                &self.key_column_indices,
                &delta,
                self.strict_probability_domain,
            )?;
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
    /// Whether the target rule has a PROB column.
    pub target_has_prob: bool,
    /// Name of the PROB column in the target rule, if any.
    pub target_prob_col: Option<String>,
    /// `(body_col, derived_col)` pairs for provenance tracking.
    ///
    /// Used by shared-proof detection to find which source facts a derived row
    /// consumed. Populated for all IS-refs (not just negated ones).
    pub provenance_join_cols: Vec<(String, String)>,
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
    /// ALONG binding variable names propagated from the planner.
    pub along_bindings: Vec<String>,
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
    /// Name of the PROB column in this rule's yield schema, if any.
    pub prob_column_name: Option<String>,
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
    strict_probability_domain: bool,
    probability_epsilon: f64,
    warnings_slot: Arc<StdRwLock<Vec<RuntimeWarning>>>,
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
                strict_probability_domain,
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
                // Apply negated IS-ref semantics: probabilistic complement or anti-join.
                for binding in &clause.is_ref_bindings {
                    if binding.negated
                        && !binding.anti_join_cols.is_empty()
                        && let Some(entry) = registry.get(binding.derived_scan_index)
                    {
                        let neg_facts = entry.data.read().clone();
                        if !neg_facts.is_empty() {
                            if binding.target_has_prob && rule.prob_column_name.is_some() {
                                // Probabilistic complement: add 1-p column instead of filtering.
                                let complement_col =
                                    format!("__prob_complement_{}", binding.rule_name);
                                if let Some(prob_col) = &binding.target_prob_col {
                                    batches = apply_prob_complement_composite(
                                        batches,
                                        &neg_facts,
                                        &binding.anti_join_cols,
                                        prob_col,
                                        &complement_col,
                                    )?;
                                } else {
                                    // target_has_prob but no prob_col: fall back to anti-join.
                                    batches = apply_anti_join_composite(
                                        batches,
                                        &neg_facts,
                                        &binding.anti_join_cols,
                                    )?;
                                }
                            } else {
                                // Boolean exclusion: anti-join (existing behavior)
                                batches = apply_anti_join_composite(
                                    batches,
                                    &neg_facts,
                                    &binding.anti_join_cols,
                                )?;
                            }
                        }
                    }
                }
                // Multiply complement columns into the PROB column (if any) and clean up
                let complement_cols: Vec<String> = if !batches.is_empty() {
                    batches[0]
                        .schema()
                        .fields()
                        .iter()
                        .filter(|f| f.name().starts_with("__prob_complement_"))
                        .map(|f| f.name().clone())
                        .collect()
                } else {
                    vec![]
                };
                if !complement_cols.is_empty() {
                    batches = multiply_prob_factors(
                        batches,
                        rule.prob_column_name.as_deref(),
                        &complement_cols,
                    )?;
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
                        &registry,
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

        // Detect shared proofs before FOLD collapses groups.
        if let Some(ref tracker) = derivation_tracker {
            detect_shared_proofs(rule, &facts, tracker, &warnings_slot);
        }

        let processed = apply_post_fixpoint_chain(
            facts,
            rule,
            &task_ctx,
            strict_probability_domain,
            probability_epsilon,
        )
        .await?;
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
    registry: &Arc<DerivedScanRegistry>,
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

            let inputs = collect_is_ref_inputs(rule, clause_index, delta_batch, row_idx, registry);

            let entry = DerivationEntry {
                rule_name: rule.name.clone(),
                clause_index,
                inputs,
                along_values: {
                    let along_names: Vec<String> = rule
                        .clauses
                        .get(clause_index)
                        .map(|c| c.along_bindings.clone())
                        .unwrap_or_default();
                    along_names
                        .iter()
                        .filter_map(|name| fact_row.get(name).map(|v| (name.clone(), v.clone())))
                        .collect()
                },
                iteration,
                fact_row,
            };
            tracker.record(row_hash, entry);
        }
    }
}

/// Collect IS-ref input facts for a derived row using provenance join columns.
///
/// For each non-negated IS-ref binding in the clause, extracts body-side key
/// values from the delta row and finds matching source rows in the registry.
/// Returns a `DerivationInput` for each match (with the source fact hash).
fn collect_is_ref_inputs(
    rule: &FixpointRulePlan,
    clause_index: usize,
    delta_batch: &RecordBatch,
    row_idx: usize,
    registry: &Arc<DerivedScanRegistry>,
) -> Vec<DerivationInput> {
    let clause = match rule.clauses.get(clause_index) {
        Some(c) => c,
        None => return vec![],
    };

    let mut inputs = Vec::new();
    let delta_schema = delta_batch.schema();

    for binding in &clause.is_ref_bindings {
        if binding.negated {
            continue;
        }
        if binding.provenance_join_cols.is_empty() {
            continue;
        }

        // Extract body-side values from the delta row for each provenance join col.
        let body_values: Vec<(String, ScalarKey)> = binding
            .provenance_join_cols
            .iter()
            .filter_map(|(body_col, _derived_col)| {
                let col_idx = delta_schema
                    .fields()
                    .iter()
                    .position(|f| f.name() == body_col)?;
                let key = extract_scalar_key(delta_batch, &[col_idx], row_idx);
                Some((body_col.clone(), key.into_iter().next()?))
            })
            .collect();

        if body_values.len() != binding.provenance_join_cols.len() {
            continue;
        }

        // Read current data from the registry entry for this IS-ref's rule.
        let entry = match registry.get(binding.derived_scan_index) {
            Some(e) => e,
            None => continue,
        };
        let source_batches = entry.data.read();
        let source_schema = &entry.schema;

        // Find matching source rows and hash them.
        for src_batch in source_batches.iter() {
            let all_src_indices: Vec<usize> = (0..src_batch.num_columns()).collect();
            for src_row in 0..src_batch.num_rows() {
                let matches = binding.provenance_join_cols.iter().enumerate().all(
                    |(i, (_body_col, derived_col))| {
                        let src_col_idx = source_schema
                            .fields()
                            .iter()
                            .position(|f| f.name() == derived_col);
                        match src_col_idx {
                            Some(idx) => {
                                let src_key = extract_scalar_key(src_batch, &[idx], src_row);
                                src_key.first() == Some(&body_values[i].1)
                            }
                            None => false,
                        }
                    },
                );
                if matches {
                    let fact_hash = format!(
                        "{:?}",
                        extract_scalar_key(src_batch, &all_src_indices, src_row)
                    )
                    .into_bytes();
                    inputs.push(DerivationInput {
                        is_ref_rule: binding.rule_name.clone(),
                        fact_hash,
                    });
                }
            }
        }
    }

    inputs
}

// ---------------------------------------------------------------------------
// Shared-proof detection
// ---------------------------------------------------------------------------

/// Detect KEY groups in a rule's pre-fold facts where recursive derivation
/// may violate the independence assumption of MNOR/MPROD.
///
/// Uses a two-tier strategy:
/// 1. **Precise**: If the `DerivationTracker` has populated `inputs` for facts
///    in the group, we recursively collect base-level fact hashes and check for
///    pairwise overlap. A shared base fact proves a dependency.
/// 2. **Structural fallback**: When input tracking is unavailable (e.g., the
///    IS-ref subject variables were projected away), we check whether any fact
///    in a multi-row group was derived by a clause that has IS-ref bindings.
///    Recursive derivation through shared relations is a strong signal that
///    proof paths may share intermediate nodes.
///
/// Emits at most one `SharedProbabilisticDependency` warning per rule.
fn detect_shared_proofs(
    rule: &FixpointRulePlan,
    pre_fold_facts: &[RecordBatch],
    tracker: &Arc<DerivationTracker>,
    warnings_slot: &Arc<StdRwLock<Vec<RuntimeWarning>>>,
) {
    use crate::query::df_graph::locy_fold::FoldAggKind;
    use uni_locy::{RuntimeWarning, RuntimeWarningCode};

    // Only check rules with MNOR/MPROD fold bindings.
    let has_prob_fold = rule
        .fold_bindings
        .iter()
        .any(|fb| matches!(fb.kind, FoldAggKind::Nor | FoldAggKind::Prod));
    if !has_prob_fold {
        return;
    }

    // Group facts by KEY columns.
    let key_indices = &rule.key_column_indices;
    let all_indices: Vec<usize> = (0..rule.yield_schema.fields().len()).collect();

    let mut groups: HashMap<Vec<ScalarKey>, Vec<Vec<u8>>> = HashMap::new();
    for batch in pre_fold_facts {
        for row_idx in 0..batch.num_rows() {
            let key = extract_scalar_key(batch, key_indices, row_idx);
            let fact_hash =
                format!("{:?}", extract_scalar_key(batch, &all_indices, row_idx)).into_bytes();
            groups.entry(key).or_default().push(fact_hash);
        }
    }

    // Check each group with ≥2 rows.
    for fact_hashes in groups.values() {
        if fact_hashes.len() < 2 {
            continue;
        }

        // Tier 1: precise base-fact overlap detection via tracker inputs.
        let mut has_inputs = false;
        let mut per_row_bases: Vec<HashSet<Vec<u8>>> = Vec::new();
        for fh in fact_hashes {
            let bases = collect_base_facts_recursive(fh, tracker, &mut HashSet::new());
            if let Some(entry) = tracker.lookup(fh)
                && !entry.inputs.is_empty()
            {
                has_inputs = true;
            }
            per_row_bases.push(bases);
        }

        let shared_found = if has_inputs {
            // At least some facts have tracked inputs — do precise comparison.
            let mut found = false;
            'outer: for i in 0..per_row_bases.len() {
                for j in (i + 1)..per_row_bases.len() {
                    if !per_row_bases[i].is_disjoint(&per_row_bases[j]) {
                        found = true;
                        break 'outer;
                    }
                }
            }
            found
        } else {
            // Tier 2: structural fallback — check if any fact in the group was
            // derived by a clause with IS-ref bindings (recursive derivation).
            fact_hashes.iter().any(|fh| {
                tracker.lookup(fh).is_some_and(|entry| {
                    rule.clauses
                        .get(entry.clause_index)
                        .is_some_and(|clause| clause.is_ref_bindings.iter().any(|b| !b.negated))
                })
            })
        };

        if shared_found {
            if let Ok(mut warnings) = warnings_slot.write() {
                let already_warned = warnings.iter().any(|w| {
                    w.code == RuntimeWarningCode::SharedProbabilisticDependency
                        && w.rule_name == rule.name
                });
                if !already_warned {
                    warnings.push(RuntimeWarning {
                        code: RuntimeWarningCode::SharedProbabilisticDependency,
                        message: format!(
                            "Rule '{}' aggregates with MNOR/MPROD but some proof paths \
                             share intermediate facts, violating the independence assumption. \
                             Results may overestimate probability.",
                            rule.name
                        ),
                        rule_name: rule.name.clone(),
                    });
                }
            }
            return; // One warning per rule is enough.
        }
    }
}

/// Record provenance and detect shared proofs for non-recursive strata.
///
/// Non-recursive rules are evaluated in a single pass (no fixpoint loop), so
/// the regular `record_provenance` + `detect_shared_proofs` path is never hit.
/// This function bridges that gap by recording a `DerivationEntry` for every
/// fact produced by each clause and then running the same two-tier detection
/// logic used by the recursive path.
pub(crate) fn record_and_detect_shared_proofs_nonrecursive(
    rule: &FixpointRulePlan,
    tagged_clause_facts: &[(usize, Vec<RecordBatch>)],
    tracker: &Arc<DerivationTracker>,
    warnings_slot: &Arc<StdRwLock<Vec<RuntimeWarning>>>,
    registry: &Arc<DerivedScanRegistry>,
) {
    let all_indices: Vec<usize> = (0..rule.yield_schema.fields().len()).collect();

    // Record provenance for each clause's facts.
    for (clause_index, batches) in tagged_clause_facts {
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let row_hash =
                    format!("{:?}", extract_scalar_key(batch, &all_indices, row_idx)).into_bytes();
                let fact_row = batch_row_to_value_map(batch, row_idx);

                let inputs = collect_is_ref_inputs(rule, *clause_index, batch, row_idx, registry);

                let entry = DerivationEntry {
                    rule_name: rule.name.clone(),
                    clause_index: *clause_index,
                    inputs,
                    along_values: {
                        let along_names: Vec<String> = rule
                            .clauses
                            .get(*clause_index)
                            .map(|c| c.along_bindings.clone())
                            .unwrap_or_default();
                        along_names
                            .iter()
                            .filter_map(|name| {
                                fact_row.get(name).map(|v| (name.clone(), v.clone()))
                            })
                            .collect()
                    },
                    iteration: 0,
                    fact_row,
                };
                tracker.record(row_hash, entry);
            }
        }
    }

    // Flatten all clause facts and run detection.
    let all_facts: Vec<RecordBatch> = tagged_clause_facts
        .iter()
        .flat_map(|(_, batches)| batches.iter().cloned())
        .collect();
    detect_shared_proofs(rule, &all_facts, tracker, warnings_slot);
}

/// Recursively collect base fact hashes from a derivation entry.
///
/// A base fact is one with no IS-ref inputs (a graph-level fact). Intermediate
/// facts are expanded transitively through the tracker.
fn collect_base_facts_recursive(
    fact_hash: &[u8],
    tracker: &Arc<DerivationTracker>,
    visited: &mut HashSet<Vec<u8>>,
) -> HashSet<Vec<u8>> {
    if !visited.insert(fact_hash.to_vec()) {
        return HashSet::new(); // Cycle guard.
    }

    match tracker.lookup(fact_hash) {
        Some(entry) if !entry.inputs.is_empty() => {
            let mut bases = HashSet::new();
            for input in &entry.inputs {
                let child_bases = collect_base_facts_recursive(&input.fact_hash, tracker, visited);
                bases.extend(child_bases);
            }
            bases
        }
        _ => {
            // Base fact (no tracker entry or no inputs).
            let mut set = HashSet::new();
            set.insert(fact_hash.to_vec());
            set
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

/// Probabilistic complement for negated IS-refs targeting PROB rules.
///
/// Instead of filtering out matching VIDs (anti-join), this adds a complement column
/// `__prob_complement_{rule_name}` with value `1 - p` for each matching VID, and `1.0`
/// for VIDs not present in the negated rule's facts.
///
/// This implements the probabilistic complement semantics: `IS NOT risk` on a PROB rule
/// yields the probability that the entity is NOT risky.
pub fn apply_prob_complement(
    batches: Vec<RecordBatch>,
    neg_facts: &[RecordBatch],
    left_col: &str,
    right_col: &str,
    prob_col: &str,
    complement_col_name: &str,
) -> datafusion::error::Result<Vec<RecordBatch>> {
    use arrow_array::{Array as _, Float64Array, UInt64Array};

    // Build VID → probability lookup from negative facts
    let mut prob_map: std::collections::HashMap<u64, f64> = std::collections::HashMap::new();
    for batch in neg_facts {
        let Ok(vid_idx) = batch.schema().index_of(right_col) else {
            continue;
        };
        let Ok(prob_idx) = batch.schema().index_of(prob_col) else {
            continue;
        };
        let Some(vids) = batch.column(vid_idx).as_any().downcast_ref::<UInt64Array>() else {
            continue;
        };
        let prob_arr = batch.column(prob_idx);
        let probs = prob_arr.as_any().downcast_ref::<Float64Array>();
        for i in 0..vids.len() {
            if !vids.is_null(i) {
                let p = probs
                    .and_then(|arr| {
                        if arr.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        }
                    })
                    .unwrap_or(0.0);
                // If multiple facts for same VID, use noisy-OR combination:
                // combined = 1 - (1 - existing) * (1 - new)
                prob_map
                    .entry(vids.value(i))
                    .and_modify(|existing| {
                        *existing = 1.0 - (1.0 - *existing) * (1.0 - p);
                    })
                    .or_insert(p);
            }
        }
    }

    // Add complement column to each batch
    let mut result = Vec::new();
    for batch in batches {
        let Ok(idx) = batch.schema().index_of(left_col) else {
            result.push(batch);
            continue;
        };
        let Some(vids) = batch.column(idx).as_any().downcast_ref::<UInt64Array>() else {
            result.push(batch);
            continue;
        };

        // Compute complement values: 1 - p for matched VIDs, 1.0 for absent
        let complements: Vec<f64> = (0..vids.len())
            .map(|i| {
                if vids.is_null(i) {
                    1.0
                } else {
                    let p = prob_map.get(&vids.value(i)).copied().unwrap_or(0.0);
                    1.0 - p
                }
            })
            .collect();

        let complement_arr = Float64Array::from(complements);

        // Add the complement column to the batch
        let mut columns: Vec<arrow_array::ArrayRef> = batch.columns().to_vec();
        columns.push(std::sync::Arc::new(complement_arr));

        let mut fields: Vec<std::sync::Arc<arrow_schema::Field>> =
            batch.schema().fields().iter().cloned().collect();
        fields.push(std::sync::Arc::new(arrow_schema::Field::new(
            complement_col_name,
            arrow_schema::DataType::Float64,
            true,
        )));

        let new_schema = std::sync::Arc::new(arrow_schema::Schema::new(fields));
        let new_batch = RecordBatch::try_new(new_schema, columns)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
        result.push(new_batch);
    }
    Ok(result)
}

/// Probabilistic complement for composite (multi-column) join keys.
///
/// Builds a composite key from all `join_cols` right-side columns in
/// `neg_facts`, maps each composite key to a probability via noisy-OR
/// combination, then adds a single `complement_col_name` column with
/// `1 - p` for matched keys and `1.0` for absent keys.
pub fn apply_prob_complement_composite(
    batches: Vec<RecordBatch>,
    neg_facts: &[RecordBatch],
    join_cols: &[(String, String)],
    prob_col: &str,
    complement_col_name: &str,
) -> datafusion::error::Result<Vec<RecordBatch>> {
    use arrow_array::{Array as _, Float64Array, UInt64Array};

    // Build composite-key → probability lookup from negative facts.
    let mut prob_map: HashMap<Vec<u64>, f64> = HashMap::new();
    for batch in neg_facts {
        let right_indices: Vec<usize> = join_cols
            .iter()
            .filter_map(|(_, rc)| batch.schema().index_of(rc).ok())
            .collect();
        if right_indices.len() != join_cols.len() {
            continue;
        }
        let Ok(prob_idx) = batch.schema().index_of(prob_col) else {
            continue;
        };
        let prob_arr = batch.column(prob_idx);
        let probs = prob_arr.as_any().downcast_ref::<Float64Array>();
        for row in 0..batch.num_rows() {
            let mut key = Vec::with_capacity(right_indices.len());
            let mut valid = true;
            for &ci in &right_indices {
                let col = batch.column(ci);
                if let Some(vids) = col.as_any().downcast_ref::<UInt64Array>() {
                    if vids.is_null(row) {
                        valid = false;
                        break;
                    }
                    key.push(vids.value(row));
                } else {
                    valid = false;
                    break;
                }
            }
            if !valid {
                continue;
            }
            let p = probs
                .and_then(|arr| {
                    if arr.is_null(row) {
                        None
                    } else {
                        Some(arr.value(row))
                    }
                })
                .unwrap_or(0.0);
            // Noisy-OR combination for duplicate composite keys.
            prob_map
                .entry(key)
                .and_modify(|existing| {
                    *existing = 1.0 - (1.0 - *existing) * (1.0 - p);
                })
                .or_insert(p);
        }
    }

    // Add complement column to each batch.
    let mut result = Vec::new();
    for batch in batches {
        let left_indices: Vec<usize> = join_cols
            .iter()
            .filter_map(|(lc, _)| batch.schema().index_of(lc).ok())
            .collect();
        if left_indices.len() != join_cols.len() {
            result.push(batch);
            continue;
        }
        let all_u64 = left_indices.iter().all(|&ci| {
            batch
                .column(ci)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .is_some()
        });
        if !all_u64 {
            result.push(batch);
            continue;
        }

        let complements: Vec<f64> = (0..batch.num_rows())
            .map(|row| {
                let mut key = Vec::with_capacity(left_indices.len());
                for &ci in &left_indices {
                    let vids = batch
                        .column(ci)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();
                    if vids.is_null(row) {
                        return 1.0;
                    }
                    key.push(vids.value(row));
                }
                let p = prob_map.get(&key).copied().unwrap_or(0.0);
                1.0 - p
            })
            .collect();

        let complement_arr = Float64Array::from(complements);
        let mut columns: Vec<arrow_array::ArrayRef> = batch.columns().to_vec();
        columns.push(Arc::new(complement_arr));

        let mut fields: Vec<Arc<arrow_schema::Field>> =
            batch.schema().fields().iter().cloned().collect();
        fields.push(Arc::new(arrow_schema::Field::new(
            complement_col_name,
            arrow_schema::DataType::Float64,
            true,
        )));

        let new_schema = Arc::new(arrow_schema::Schema::new(fields));
        let new_batch = RecordBatch::try_new(new_schema, columns)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
        result.push(new_batch);
    }
    Ok(result)
}

/// Boolean anti-join for composite (multi-column) join keys.
///
/// Builds a `HashSet<Vec<u64>>` from `neg_facts` using all right-side
/// columns in `join_cols`, then filters `batches` to keep only rows
/// whose composite left-side key is NOT in the set.
pub fn apply_anti_join_composite(
    batches: Vec<RecordBatch>,
    neg_facts: &[RecordBatch],
    join_cols: &[(String, String)],
) -> datafusion::error::Result<Vec<RecordBatch>> {
    use arrow::compute::filter_record_batch;
    use arrow_array::{Array as _, BooleanArray, UInt64Array};

    // Collect composite keys from the negated rule's derived facts.
    let mut banned: HashSet<Vec<u64>> = HashSet::new();
    for batch in neg_facts {
        let right_indices: Vec<usize> = join_cols
            .iter()
            .filter_map(|(_, rc)| batch.schema().index_of(rc).ok())
            .collect();
        if right_indices.len() != join_cols.len() {
            continue;
        }
        for row in 0..batch.num_rows() {
            let mut key = Vec::with_capacity(right_indices.len());
            let mut valid = true;
            for &ci in &right_indices {
                let col = batch.column(ci);
                if let Some(vids) = col.as_any().downcast_ref::<UInt64Array>() {
                    if vids.is_null(row) {
                        valid = false;
                        break;
                    }
                    key.push(vids.value(row));
                } else {
                    valid = false;
                    break;
                }
            }
            if valid {
                banned.insert(key);
            }
        }
    }

    if banned.is_empty() {
        return Ok(batches);
    }

    // Filter body batches: keep rows where composite left key NOT IN banned.
    let mut result = Vec::new();
    for batch in batches {
        let left_indices: Vec<usize> = join_cols
            .iter()
            .filter_map(|(lc, _)| batch.schema().index_of(lc).ok())
            .collect();
        if left_indices.len() != join_cols.len() {
            result.push(batch);
            continue;
        }
        let all_u64 = left_indices.iter().all(|&ci| {
            batch
                .column(ci)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .is_some()
        });
        if !all_u64 {
            result.push(batch);
            continue;
        }

        let keep: Vec<bool> = (0..batch.num_rows())
            .map(|row| {
                let mut key = Vec::with_capacity(left_indices.len());
                for &ci in &left_indices {
                    let vids = batch
                        .column(ci)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();
                    if vids.is_null(row) {
                        return true; // null keys are never banned
                    }
                    key.push(vids.value(row));
                }
                !banned.contains(&key)
            })
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

/// Multiply `__prob_complement_*` columns into the rule's PROB column and clean up.
///
/// After IS NOT probabilistic complement semantics have added `__prob_complement_*`
/// columns to clause results, this function:
/// 1. Computes the product of all complement factor columns
/// 2. Multiplies the product into the existing PROB column (if any)
/// 3. Removes the internal `__prob_complement_*` columns from the output
///
/// If the rule has no PROB column, complement columns are simply removed
/// (the complement information is discarded and IS NOT acts as a keep-all).
pub fn multiply_prob_factors(
    batches: Vec<RecordBatch>,
    prob_col: Option<&str>,
    complement_cols: &[String],
) -> datafusion::error::Result<Vec<RecordBatch>> {
    use arrow_array::{Array as _, Float64Array};

    let mut result = Vec::with_capacity(batches.len());

    for batch in batches {
        if batch.num_rows() == 0 {
            // Remove complement columns from empty batches
            let keep: Vec<usize> = batch
                .schema()
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, f)| !complement_cols.contains(f.name()))
                .map(|(i, _)| i)
                .collect();
            let fields: Vec<_> = keep
                .iter()
                .map(|&i| batch.schema().field(i).clone())
                .collect();
            let cols: Vec<_> = keep.iter().map(|&i| batch.column(i).clone()).collect();
            let schema = std::sync::Arc::new(arrow_schema::Schema::new(fields));
            result.push(
                RecordBatch::try_new(schema, cols).map_err(|e| {
                    datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                })?,
            );
            continue;
        }

        let num_rows = batch.num_rows();

        // 1. Compute product of all complement factors
        let mut combined = vec![1.0f64; num_rows];
        for col_name in complement_cols {
            if let Ok(idx) = batch.schema().index_of(col_name) {
                let arr = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Internal(format!(
                            "Expected Float64 for complement column {col_name}"
                        ))
                    })?;
                for (i, val) in combined.iter_mut().enumerate().take(num_rows) {
                    if !arr.is_null(i) {
                        *val *= arr.value(i);
                    }
                }
            }
        }

        // 2. If there's a PROB column, multiply combined into it
        let final_prob: Vec<f64> = if let Some(prob_name) = prob_col {
            if let Ok(idx) = batch.schema().index_of(prob_name) {
                let arr = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Internal(format!(
                            "Expected Float64 for PROB column {prob_name}"
                        ))
                    })?;
                (0..num_rows)
                    .map(|i| {
                        if arr.is_null(i) {
                            combined[i]
                        } else {
                            arr.value(i) * combined[i]
                        }
                    })
                    .collect()
            } else {
                combined
            }
        } else {
            combined
        };

        let new_prob_array: arrow_array::ArrayRef =
            std::sync::Arc::new(Float64Array::from(final_prob));

        // 3. Build output: replace PROB column, remove complement columns
        let mut fields = Vec::new();
        let mut columns = Vec::new();

        for (idx, field) in batch.schema().fields().iter().enumerate() {
            if complement_cols.contains(field.name()) {
                continue;
            }
            if prob_col.is_some_and(|p| field.name() == p) {
                fields.push(field.clone());
                columns.push(new_prob_array.clone());
            } else {
                fields.push(field.clone());
                columns.push(batch.column(idx).clone());
            }
        }

        let schema = std::sync::Arc::new(arrow_schema::Schema::new(fields));
        result.push(
            RecordBatch::try_new(schema, columns)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?,
        );
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
    strict_probability_domain: bool,
    probability_epsilon: f64,
) -> DFResult<Vec<RecordBatch>> {
    if !rule.has_fold && !rule.has_best_by && !rule.has_priority {
        return Ok(facts);
    }

    // Wrap facts in InMemoryExec.
    // Prefer the actual batch schema (from physical execution) over the
    // pre-computed yield_schema, which may have wrong inferred types
    // (e.g. Float64 for a string property).
    let schema = facts
        .iter()
        .find(|b| b.num_rows() > 0)
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::clone(&rule.yield_schema));
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
            strict_probability_domain,
            probability_epsilon,
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
    strict_probability_domain: bool,
    probability_epsilon: f64,
    /// Shared slot for runtime warnings collected during fixpoint iteration.
    warnings_slot: Arc<StdRwLock<Vec<RuntimeWarning>>>,
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
        strict_probability_domain: bool,
        probability_epsilon: f64,
        warnings_slot: Arc<StdRwLock<Vec<RuntimeWarning>>>,
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
            strict_probability_domain,
            probability_epsilon,
            warnings_slot,
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
                            along_bindings: c.along_bindings.clone(),
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
                    prob_column_name: r.prob_column_name.clone(),
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
        let strict_probability_domain = self.strict_probability_domain;
        let probability_epsilon = self.probability_epsilon;
        let warnings_slot = Arc::clone(&self.warnings_slot);

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
                strict_probability_domain,
                probability_epsilon,
                warnings_slot,
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
        let mut state = FixpointState::new("test".into(), schema, vec![0], 1_000_000, None, false);

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
        let mut state = FixpointState::new("test".into(), schema, vec![0], 1_000_000, None, false);

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
        let mut state = FixpointState::new("test".into(), schema, vec![0], 1_000_000, None, false);

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
        let mut state = FixpointState::new("test".into(), schema, vec![0], 1_000_000, None, false);

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
        let changed = agg.update(&[0], &[batch], false).unwrap();
        assert!(changed);
        assert!(!agg.is_stable()); // changed since snapshot

        // Snapshot and check stability with no new data
        agg.snapshot();
        let changed = agg.update(&[0], &[], false).unwrap();
        assert!(!changed);
        assert!(agg.is_stable());
    }

    // --- Memory limit test ---

    #[tokio::test]
    async fn test_memory_limit_exceeded() {
        let schema = test_schema();
        // Set a tiny limit
        let mut state = FixpointState::new("test".into(), schema, vec![0], 1, None, false);

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

    // ── MonotonicAggState MNOR/MPROD tests ──────────────────────────────

    fn make_f64_batch(names: &[&str], values: &[f64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    names.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    fn make_nor_binding() -> Vec<MonotonicFoldBinding> {
        use crate::query::df_graph::locy_fold::FoldAggKind;
        vec![MonotonicFoldBinding {
            fold_name: "prob".into(),
            kind: FoldAggKind::Nor,
            input_col_index: 1,
        }]
    }

    fn make_prod_binding() -> Vec<MonotonicFoldBinding> {
        use crate::query::df_graph::locy_fold::FoldAggKind;
        vec![MonotonicFoldBinding {
            fold_name: "prob".into(),
            kind: FoldAggKind::Prod,
            input_col_index: 1,
        }]
    }

    fn acc_key(name: &str) -> (Vec<ScalarKey>, String) {
        (vec![ScalarKey::Utf8(name.to_string())], "prob".to_string())
    }

    #[test]
    fn test_monotonic_nor_first_update() {
        let mut agg = MonotonicAggState::new(make_nor_binding());
        let batch = make_f64_batch(&["a"], &[0.3]);
        let changed = agg.update(&[0], &[batch], false).unwrap();
        assert!(changed);
        let val = agg.get_accumulator(&acc_key("a")).unwrap();
        assert!((val - 0.3).abs() < 1e-10, "expected 0.3, got {}", val);
    }

    #[test]
    fn test_monotonic_nor_two_updates() {
        // Incremental NOR: acc = 1-(1-0.3)(1-0.5) = 0.65
        let mut agg = MonotonicAggState::new(make_nor_binding());
        let batch1 = make_f64_batch(&["a"], &[0.3]);
        agg.update(&[0], &[batch1], false).unwrap();
        let batch2 = make_f64_batch(&["a"], &[0.5]);
        agg.update(&[0], &[batch2], false).unwrap();
        let val = agg.get_accumulator(&acc_key("a")).unwrap();
        assert!((val - 0.65).abs() < 1e-10, "expected 0.65, got {}", val);
    }

    #[test]
    fn test_monotonic_prod_first_update() {
        let mut agg = MonotonicAggState::new(make_prod_binding());
        let batch = make_f64_batch(&["a"], &[0.6]);
        let changed = agg.update(&[0], &[batch], false).unwrap();
        assert!(changed);
        let val = agg.get_accumulator(&acc_key("a")).unwrap();
        assert!((val - 0.6).abs() < 1e-10, "expected 0.6, got {}", val);
    }

    #[test]
    fn test_monotonic_prod_two_updates() {
        // Incremental PROD: acc = 0.6 * 0.8 = 0.48
        let mut agg = MonotonicAggState::new(make_prod_binding());
        let batch1 = make_f64_batch(&["a"], &[0.6]);
        agg.update(&[0], &[batch1], false).unwrap();
        let batch2 = make_f64_batch(&["a"], &[0.8]);
        agg.update(&[0], &[batch2], false).unwrap();
        let val = agg.get_accumulator(&acc_key("a")).unwrap();
        assert!((val - 0.48).abs() < 1e-10, "expected 0.48, got {}", val);
    }

    #[test]
    fn test_monotonic_nor_stability() {
        let mut agg = MonotonicAggState::new(make_nor_binding());
        let batch = make_f64_batch(&["a"], &[0.3]);
        agg.update(&[0], &[batch], false).unwrap();
        agg.snapshot();
        let changed = agg.update(&[0], &[], false).unwrap();
        assert!(!changed);
        assert!(agg.is_stable());
    }

    #[test]
    fn test_monotonic_prod_stability() {
        let mut agg = MonotonicAggState::new(make_prod_binding());
        let batch = make_f64_batch(&["a"], &[0.6]);
        agg.update(&[0], &[batch], false).unwrap();
        agg.snapshot();
        let changed = agg.update(&[0], &[], false).unwrap();
        assert!(!changed);
        assert!(agg.is_stable());
    }

    #[test]
    fn test_monotonic_nor_multi_group() {
        // (a,0.3),(b,0.5) then (a,0.5),(b,0.2) → a=0.65, b=0.6
        let mut agg = MonotonicAggState::new(make_nor_binding());
        let batch1 = make_f64_batch(&["a", "b"], &[0.3, 0.5]);
        agg.update(&[0], &[batch1], false).unwrap();
        let batch2 = make_f64_batch(&["a", "b"], &[0.5, 0.2]);
        agg.update(&[0], &[batch2], false).unwrap();

        let val_a = agg.get_accumulator(&acc_key("a")).unwrap();
        let val_b = agg.get_accumulator(&acc_key("b")).unwrap();
        assert!(
            (val_a - 0.65).abs() < 1e-10,
            "expected a=0.65, got {}",
            val_a
        );
        assert!((val_b - 0.6).abs() < 1e-10, "expected b=0.6, got {}", val_b);
    }

    #[test]
    fn test_monotonic_prod_zero_absorbing() {
        // Zero absorbs: once 0.0, all further updates stay 0.0
        let mut agg = MonotonicAggState::new(make_prod_binding());
        let batch1 = make_f64_batch(&["a"], &[0.5]);
        agg.update(&[0], &[batch1], false).unwrap();
        let batch2 = make_f64_batch(&["a"], &[0.0]);
        agg.update(&[0], &[batch2], false).unwrap();

        let val = agg.get_accumulator(&acc_key("a")).unwrap();
        assert!((val - 0.0).abs() < 1e-10, "expected 0.0, got {}", val);

        // Further updates don't change the absorbing zero
        agg.snapshot();
        let batch3 = make_f64_batch(&["a"], &[0.5]);
        let changed = agg.update(&[0], &[batch3], false).unwrap();
        assert!(!changed);
        assert!(agg.is_stable());
    }

    #[test]
    fn test_monotonic_nor_clamping() {
        // 1.5 clamped to 1.0: acc = 1-(1-0)(1-1) = 1.0
        let mut agg = MonotonicAggState::new(make_nor_binding());
        let batch = make_f64_batch(&["a"], &[1.5]);
        agg.update(&[0], &[batch], false).unwrap();
        let val = agg.get_accumulator(&acc_key("a")).unwrap();
        assert!((val - 1.0).abs() < 1e-10, "expected 1.0, got {}", val);
    }

    #[test]
    fn test_monotonic_nor_absorbing() {
        // p=1.0 absorbs: 0.3 then 1.0 → 1.0
        let mut agg = MonotonicAggState::new(make_nor_binding());
        let batch1 = make_f64_batch(&["a"], &[0.3]);
        agg.update(&[0], &[batch1], false).unwrap();
        let batch2 = make_f64_batch(&["a"], &[1.0]);
        agg.update(&[0], &[batch2], false).unwrap();
        let val = agg.get_accumulator(&acc_key("a")).unwrap();
        assert!((val - 1.0).abs() < 1e-10, "expected 1.0, got {}", val);
    }

    // ── MonotonicAggState strict mode tests (Phase 5) ─────────────────────

    #[test]
    fn test_monotonic_agg_strict_nor_rejects() {
        let mut agg = MonotonicAggState::new(make_nor_binding());
        let batch = make_f64_batch(&["a"], &[1.5]);
        let result = agg.update(&[0], &[batch], true);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("strict_probability_domain"),
            "Expected strict error, got: {}",
            err
        );
    }

    #[test]
    fn test_monotonic_agg_strict_prod_rejects() {
        let mut agg = MonotonicAggState::new(make_prod_binding());
        let batch = make_f64_batch(&["a"], &[2.0]);
        let result = agg.update(&[0], &[batch], true);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("strict_probability_domain"),
            "Expected strict error, got: {}",
            err
        );
    }

    #[test]
    fn test_monotonic_agg_strict_accepts_valid() {
        let mut agg = MonotonicAggState::new(make_nor_binding());
        let batch = make_f64_batch(&["a"], &[0.5]);
        let result = agg.update(&[0], &[batch], true);
        assert!(result.is_ok());
        let val = agg.get_accumulator(&acc_key("a")).unwrap();
        assert!((val - 0.5).abs() < 1e-10, "expected 0.5, got {}", val);
    }

    // ── Complement function unit tests (Phase 4) ──────────────────────────

    fn make_vid_prob_batch(vids: &[u64], probs: &[f64]) -> RecordBatch {
        use arrow_array::UInt64Array;
        let schema = Arc::new(Schema::new(vec![
            Field::new("vid", DataType::UInt64, true),
            Field::new("prob", DataType::Float64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vids.to_vec())),
                Arc::new(Float64Array::from(probs.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_prob_complement_basic() {
        // neg has VID=1 with prob=0.7 → complement=0.3; VID=2 absent → complement=1.0
        let body = make_vid_prob_batch(&[1, 2], &[0.9, 0.8]);
        let neg = make_vid_prob_batch(&[1], &[0.7]);
        let join_cols = vec![("vid".to_string(), "vid".to_string())];
        let result = apply_prob_complement_composite(
            vec![body],
            &[neg],
            &join_cols,
            "prob",
            "__complement_0",
        )
        .unwrap();
        assert_eq!(result.len(), 1);
        let batch = &result[0];
        let complement = batch
            .column_by_name("__complement_0")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // VID=1: complement = 1 - 0.7 = 0.3
        assert!(
            (complement.value(0) - 0.3).abs() < 1e-10,
            "expected 0.3, got {}",
            complement.value(0)
        );
        // VID=2: absent from neg → complement = 1.0
        assert!(
            (complement.value(1) - 1.0).abs() < 1e-10,
            "expected 1.0, got {}",
            complement.value(1)
        );
    }

    #[test]
    fn test_prob_complement_noisy_or_duplicates() {
        // neg has VID=1 twice with prob=0.3 and prob=0.5
        // Combined via noisy-OR: 1-(1-0.3)(1-0.5) = 0.65
        // Complement = 1 - 0.65 = 0.35
        let body = make_vid_prob_batch(&[1], &[0.9]);
        let neg = make_vid_prob_batch(&[1, 1], &[0.3, 0.5]);
        let join_cols = vec![("vid".to_string(), "vid".to_string())];
        let result = apply_prob_complement_composite(
            vec![body],
            &[neg],
            &join_cols,
            "prob",
            "__complement_0",
        )
        .unwrap();
        let batch = &result[0];
        let complement = batch
            .column_by_name("__complement_0")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            (complement.value(0) - 0.35).abs() < 1e-10,
            "expected 0.35, got {}",
            complement.value(0)
        );
    }

    #[test]
    fn test_prob_complement_empty_neg() {
        // Empty neg_facts → body passes through with complement=1.0
        let body = make_vid_prob_batch(&[1, 2], &[0.5, 0.6]);
        let join_cols = vec![("vid".to_string(), "vid".to_string())];
        let result =
            apply_prob_complement_composite(vec![body], &[], &join_cols, "prob", "__complement_0")
                .unwrap();
        let batch = &result[0];
        let complement = batch
            .column_by_name("__complement_0")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..2 {
            assert!(
                (complement.value(i) - 1.0).abs() < 1e-10,
                "row {}: expected 1.0, got {}",
                i,
                complement.value(i)
            );
        }
    }

    #[test]
    fn test_anti_join_basic() {
        // body [1,2,3], neg [2] → result [1,3]
        use arrow_array::UInt64Array;
        let body = make_vid_prob_batch(&[1, 2, 3], &[0.5, 0.6, 0.7]);
        let neg = make_vid_prob_batch(&[2], &[0.0]);
        let join_cols = vec![("vid".to_string(), "vid".to_string())];
        let result = apply_anti_join_composite(vec![body], &[neg], &join_cols).unwrap();
        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);
        let vids = batch
            .column_by_name("vid")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(vids.value(0), 1);
        assert_eq!(vids.value(1), 3);
    }

    #[test]
    fn test_anti_join_empty_neg() {
        // Empty neg → all rows kept
        let body = make_vid_prob_batch(&[1, 2, 3], &[0.5, 0.6, 0.7]);
        let join_cols = vec![("vid".to_string(), "vid".to_string())];
        let result = apply_anti_join_composite(vec![body], &[], &join_cols).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 3);
    }

    #[test]
    fn test_anti_join_all_excluded() {
        // neg covers all body rows → empty result
        let body = make_vid_prob_batch(&[1, 2], &[0.5, 0.6]);
        let neg = make_vid_prob_batch(&[1, 2], &[0.0, 0.0]);
        let join_cols = vec![("vid".to_string(), "vid".to_string())];
        let result = apply_anti_join_composite(vec![body], &[neg], &join_cols).unwrap();
        let total: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    #[test]
    fn test_multiply_prob_single_complement() {
        // prob=0.8, complement=0.5 → output prob=0.4; complement col removed
        let body = make_vid_prob_batch(&[1], &[0.8]);
        // Add a complement column
        let complement_arr = Float64Array::from(vec![0.5]);
        let mut cols: Vec<arrow_array::ArrayRef> = body.columns().to_vec();
        cols.push(Arc::new(complement_arr));
        let mut fields: Vec<Arc<Field>> = body.schema().fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(
            "__complement_0",
            DataType::Float64,
            true,
        )));
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, cols).unwrap();

        let result =
            multiply_prob_factors(vec![batch], Some("prob"), &["__complement_0".to_string()])
                .unwrap();
        assert_eq!(result.len(), 1);
        let out = &result[0];
        // Complement column should be removed
        assert!(out.column_by_name("__complement_0").is_none());
        let prob = out
            .column_by_name("prob")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            (prob.value(0) - 0.4).abs() < 1e-10,
            "expected 0.4, got {}",
            prob.value(0)
        );
    }

    #[test]
    fn test_multiply_prob_multiple_complements() {
        // prob=0.8, c1=0.5, c2=0.6 → 0.8×0.5×0.6=0.24
        let body = make_vid_prob_batch(&[1], &[0.8]);
        let c1 = Float64Array::from(vec![0.5]);
        let c2 = Float64Array::from(vec![0.6]);
        let mut cols: Vec<arrow_array::ArrayRef> = body.columns().to_vec();
        cols.push(Arc::new(c1));
        cols.push(Arc::new(c2));
        let mut fields: Vec<Arc<Field>> = body.schema().fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new("__c1", DataType::Float64, true)));
        fields.push(Arc::new(Field::new("__c2", DataType::Float64, true)));
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, cols).unwrap();

        let result = multiply_prob_factors(
            vec![batch],
            Some("prob"),
            &["__c1".to_string(), "__c2".to_string()],
        )
        .unwrap();
        let out = &result[0];
        assert!(out.column_by_name("__c1").is_none());
        assert!(out.column_by_name("__c2").is_none());
        let prob = out
            .column_by_name("prob")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(
            (prob.value(0) - 0.24).abs() < 1e-10,
            "expected 0.24, got {}",
            prob.value(0)
        );
    }

    #[test]
    fn test_multiply_prob_no_prob_column() {
        // No prob column → combined complements become the output
        use arrow_array::UInt64Array;
        let schema = Arc::new(Schema::new(vec![
            Field::new("vid", DataType::UInt64, true),
            Field::new("__c1", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![1u64])),
                Arc::new(Float64Array::from(vec![0.7])),
            ],
        )
        .unwrap();

        let result = multiply_prob_factors(vec![batch], None, &["__c1".to_string()]).unwrap();
        let out = &result[0];
        // __c1 should be removed since it's a complement column
        assert!(out.column_by_name("__c1").is_none());
        // Only vid column remains
        assert_eq!(out.num_columns(), 1);
    }
}
