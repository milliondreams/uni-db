// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Top-level Locy program executor.
//!
//! `LocyProgramExec` orchestrates the full evaluation of a Locy program:
//! it evaluates strata in dependency order, runs fixpoint for recursive strata,
//! applies post-fixpoint operators (FOLD, PRIORITY, BEST BY), and then
//! executes the program's commands (goal queries, DERIVE, ASSUME, etc.).

use crate::query::df_graph::GraphExecutionContext;
use crate::query::df_graph::common::{
    collect_all_partitions, compute_plan_properties, execute_subplan,
};
use crate::query::df_graph::locy_best_by::SortCriterion;
use crate::query::df_graph::locy_explain::ProvenanceStore;
use crate::query::df_graph::locy_fixpoint::{
    DerivedScanRegistry, FixpointClausePlan, FixpointExec, FixpointRulePlan, IsRefBinding,
};
use crate::query::df_graph::locy_fold::{FoldAggKind, FoldBinding};
use crate::query::planner_locy_types::{
    LocyCommand, LocyIsRef, LocyRulePlan, LocyStratum, LocyYieldColumn,
};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::Stream;
use parking_lot::RwLock;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock as StdRwLock;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use uni_common::Value;
use uni_common::core::schema::Schema as UniSchema;
use uni_cypher::ast::Expr;
use uni_cypher::locy_ast::GoalQuery;
use uni_locy::{CommandResult, FactRow, RuntimeWarning};
use uni_store::storage::manager::StorageManager;

// ---------------------------------------------------------------------------
// DerivedStore — cross-stratum fact sharing
// ---------------------------------------------------------------------------

/// Simple store for derived relation facts across strata.
///
/// Each rule's converged facts are stored here after its stratum completes,
/// making them available for later strata that depend on them.
pub struct DerivedStore {
    relations: HashMap<String, Vec<RecordBatch>>,
}

impl Default for DerivedStore {
    fn default() -> Self {
        Self::new()
    }
}

impl DerivedStore {
    pub fn new() -> Self {
        Self {
            relations: HashMap::new(),
        }
    }

    pub fn insert(&mut self, rule_name: String, facts: Vec<RecordBatch>) {
        self.relations.insert(rule_name, facts);
    }

    pub fn get(&self, rule_name: &str) -> Option<&Vec<RecordBatch>> {
        self.relations.get(rule_name)
    }

    pub fn fact_count(&self, rule_name: &str) -> usize {
        self.relations
            .get(rule_name)
            .map(|batches| batches.iter().map(|b| b.num_rows()).sum())
            .unwrap_or(0)
    }

    pub fn rule_names(&self) -> impl Iterator<Item = &str> {
        self.relations.keys().map(|s| s.as_str())
    }
}

// ---------------------------------------------------------------------------
// LocyProgramExec — DataFusion ExecutionPlan
// ---------------------------------------------------------------------------

/// DataFusion `ExecutionPlan` that runs an entire Locy program.
///
/// Evaluates strata in dependency order, using `FixpointExec` for recursive
/// strata and direct subplan execution for non-recursive ones. After all
/// strata converge, dispatches commands.
pub struct LocyProgramExec {
    strata: Vec<LocyStratum>,
    commands: Vec<LocyCommand>,
    derived_scan_registry: Arc<DerivedScanRegistry>,
    graph_ctx: Arc<GraphExecutionContext>,
    session_ctx: Arc<RwLock<datafusion::prelude::SessionContext>>,
    storage: Arc<StorageManager>,
    schema_info: Arc<UniSchema>,
    params: HashMap<String, Value>,
    output_schema: SchemaRef,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
    max_iterations: usize,
    timeout: Duration,
    max_derived_bytes: usize,
    deterministic_best_by: bool,
    strict_probability_domain: bool,
    probability_epsilon: f64,
    exact_probability: bool,
    max_bdd_variables: usize,
    /// Shared slot for extracting the DerivedStore after execution completes.
    derived_store_slot: Arc<StdRwLock<Option<DerivedStore>>>,
    /// Shared slot for groups where BDD fell back to independence mode.
    approximate_slot: Arc<StdRwLock<HashMap<String, Vec<String>>>>,
    /// Optional provenance tracker injected after construction (via `set_derivation_tracker`).
    derivation_tracker: Arc<StdRwLock<Option<Arc<ProvenanceStore>>>>,
    /// Shared slot written with per-rule iteration counts after fixpoint convergence.
    iteration_counts_slot: Arc<StdRwLock<HashMap<String, usize>>>,
    /// Shared slot written with peak memory bytes after fixpoint completes.
    peak_memory_slot: Arc<StdRwLock<usize>>,
    /// Shared slot for runtime warnings collected during evaluation.
    warnings_slot: Arc<StdRwLock<Vec<RuntimeWarning>>>,
    /// Shared slot for inline command results (QUERY, Cypher) executed inside `run_program()`.
    command_results_slot: Arc<StdRwLock<Vec<(usize, CommandResult)>>>,
    /// Top-k proof filtering: 0 = unlimited (default), >0 = retain at most k proofs per fact.
    top_k_proofs: usize,
}

impl fmt::Debug for LocyProgramExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocyProgramExec")
            .field("strata_count", &self.strata.len())
            .field("commands_count", &self.commands.len())
            .field("max_iterations", &self.max_iterations)
            .field("timeout", &self.timeout)
            .field("output_schema", &self.output_schema)
            .field("max_derived_bytes", &self.max_derived_bytes)
            .finish_non_exhaustive()
    }
}

impl LocyProgramExec {
    #[expect(
        clippy::too_many_arguments,
        reason = "execution plan node requires full graph and session context"
    )]
    pub fn new(
        strata: Vec<LocyStratum>,
        commands: Vec<LocyCommand>,
        derived_scan_registry: Arc<DerivedScanRegistry>,
        graph_ctx: Arc<GraphExecutionContext>,
        session_ctx: Arc<RwLock<datafusion::prelude::SessionContext>>,
        storage: Arc<StorageManager>,
        schema_info: Arc<UniSchema>,
        params: HashMap<String, Value>,
        output_schema: SchemaRef,
        max_iterations: usize,
        timeout: Duration,
        max_derived_bytes: usize,
        deterministic_best_by: bool,
        strict_probability_domain: bool,
        probability_epsilon: f64,
        exact_probability: bool,
        max_bdd_variables: usize,
        top_k_proofs: usize,
    ) -> Self {
        let properties = compute_plan_properties(Arc::clone(&output_schema));
        Self {
            strata,
            commands,
            derived_scan_registry,
            graph_ctx,
            session_ctx,
            storage,
            schema_info,
            params,
            output_schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            max_iterations,
            timeout,
            max_derived_bytes,
            deterministic_best_by,
            strict_probability_domain,
            probability_epsilon,
            exact_probability,
            max_bdd_variables,
            derived_store_slot: Arc::new(StdRwLock::new(None)),
            approximate_slot: Arc::new(StdRwLock::new(HashMap::new())),
            derivation_tracker: Arc::new(StdRwLock::new(None)),
            iteration_counts_slot: Arc::new(StdRwLock::new(HashMap::new())),
            peak_memory_slot: Arc::new(StdRwLock::new(0)),
            warnings_slot: Arc::new(StdRwLock::new(Vec::new())),
            command_results_slot: Arc::new(StdRwLock::new(Vec::new())),
            top_k_proofs,
        }
    }

    /// Returns a shared handle to the derived store slot.
    ///
    /// After execution completes, the slot contains the `DerivedStore` with all
    /// converged facts. Read it with `slot.read().unwrap()`.
    pub fn derived_store_slot(&self) -> Arc<StdRwLock<Option<DerivedStore>>> {
        Arc::clone(&self.derived_store_slot)
    }

    /// Inject a `ProvenanceStore` to record provenance during fixpoint iteration.
    ///
    /// Must be called before `execute()` is invoked (i.e., before DataFusion runs
    /// the physical plan). Uses interior mutability so it works through `&self`.
    pub fn set_derivation_tracker(&self, tracker: Arc<ProvenanceStore>) {
        if let Ok(mut guard) = self.derivation_tracker.write() {
            *guard = Some(tracker);
        }
    }

    /// Returns the shared iteration counts slot.
    ///
    /// After execution, the slot contains per-rule iteration counts from the
    /// most recent fixpoint convergence. Sum the values for `total_iterations`.
    pub fn iteration_counts_slot(&self) -> Arc<StdRwLock<HashMap<String, usize>>> {
        Arc::clone(&self.iteration_counts_slot)
    }

    /// Returns the shared peak memory slot.
    ///
    /// After execution, the slot contains the peak byte count of derived facts
    /// across all strata. Read it with `slot.read().unwrap()`.
    pub fn peak_memory_slot(&self) -> Arc<StdRwLock<usize>> {
        Arc::clone(&self.peak_memory_slot)
    }

    /// Returns the shared runtime warnings slot.
    ///
    /// After execution, the slot contains warnings collected during fixpoint
    /// iteration (e.g. shared probabilistic dependencies).
    pub fn warnings_slot(&self) -> Arc<StdRwLock<Vec<RuntimeWarning>>> {
        Arc::clone(&self.warnings_slot)
    }

    /// Returns the shared approximate groups slot.
    ///
    /// After execution, the slot contains rule→key group descriptions for
    /// groups where BDD computation fell back to independence mode.
    pub fn approximate_slot(&self) -> Arc<StdRwLock<HashMap<String, Vec<String>>>> {
        Arc::clone(&self.approximate_slot)
    }

    /// Returns the shared command results slot.
    ///
    /// After execution, the slot contains `(command_index, CommandResult)` pairs
    /// for commands that were executed inline by `run_program()` (QUERY, Cypher).
    pub fn command_results_slot(&self) -> Arc<StdRwLock<Vec<(usize, CommandResult)>>> {
        Arc::clone(&self.command_results_slot)
    }
}

impl DisplayAs for LocyProgramExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LocyProgramExec: strata={}, commands={}, max_iter={}, timeout={:?}",
            self.strata.len(),
            self.commands.len(),
            self.max_iterations,
            self.timeout,
        )
    }
}

impl ExecutionPlan for LocyProgramExec {
    fn name(&self) -> &str {
        "LocyProgramExec"
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
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Plan(
                "LocyProgramExec has no children".to_string(),
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

        let strata = self.strata.clone();
        let registry = Arc::clone(&self.derived_scan_registry);
        let graph_ctx = Arc::clone(&self.graph_ctx);
        let session_ctx = Arc::clone(&self.session_ctx);
        let storage = Arc::clone(&self.storage);
        let schema_info = Arc::clone(&self.schema_info);
        let params = self.params.clone();
        let output_schema = Arc::clone(&self.output_schema);
        let max_iterations = self.max_iterations;
        let timeout = self.timeout;
        let max_derived_bytes = self.max_derived_bytes;
        let deterministic_best_by = self.deterministic_best_by;
        let strict_probability_domain = self.strict_probability_domain;
        let probability_epsilon = self.probability_epsilon;
        let exact_probability = self.exact_probability;
        let max_bdd_variables = self.max_bdd_variables;
        let derived_store_slot = Arc::clone(&self.derived_store_slot);
        let approximate_slot = Arc::clone(&self.approximate_slot);
        let iteration_counts_slot = Arc::clone(&self.iteration_counts_slot);
        let peak_memory_slot = Arc::clone(&self.peak_memory_slot);
        let derivation_tracker = self.derivation_tracker.read().ok().and_then(|g| g.clone());
        let warnings_slot = Arc::clone(&self.warnings_slot);
        let commands = self.commands.clone();
        let command_results_slot = Arc::clone(&self.command_results_slot);
        let top_k_proofs = self.top_k_proofs;

        let fut = async move {
            run_program(
                strata,
                commands,
                registry,
                graph_ctx,
                session_ctx,
                storage,
                schema_info,
                params,
                output_schema,
                max_iterations,
                timeout,
                max_derived_bytes,
                deterministic_best_by,
                strict_probability_domain,
                probability_epsilon,
                exact_probability,
                max_bdd_variables,
                derived_store_slot,
                approximate_slot,
                iteration_counts_slot,
                peak_memory_slot,
                derivation_tracker,
                warnings_slot,
                command_results_slot,
                top_k_proofs,
            )
            .await
        };

        Ok(Box::pin(ProgramStream {
            state: ProgramStreamState::Running(Box::pin(fut)),
            schema: Arc::clone(&self.output_schema),
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

// ---------------------------------------------------------------------------
// ProgramStream — async state machine for streaming results
// ---------------------------------------------------------------------------

enum ProgramStreamState {
    Running(Pin<Box<dyn std::future::Future<Output = DFResult<Vec<RecordBatch>>> + Send>>),
    Emitting(Vec<RecordBatch>, usize),
    Done,
}

struct ProgramStream {
    state: ProgramStreamState,
    schema: SchemaRef,
    metrics: BaselineMetrics,
}

impl Stream for ProgramStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                ProgramStreamState::Running(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(batches)) => {
                        if batches.is_empty() {
                            this.state = ProgramStreamState::Done;
                            return Poll::Ready(None);
                        }
                        this.state = ProgramStreamState::Emitting(batches, 0);
                    }
                    Poll::Ready(Err(e)) => {
                        this.state = ProgramStreamState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                ProgramStreamState::Emitting(batches, idx) => {
                    if *idx >= batches.len() {
                        this.state = ProgramStreamState::Done;
                        return Poll::Ready(None);
                    }
                    let batch = batches[*idx].clone();
                    *idx += 1;
                    this.metrics.record_output(batch.num_rows());
                    return Poll::Ready(Some(Ok(batch)));
                }
                ProgramStreamState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for ProgramStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

// ---------------------------------------------------------------------------
// Inline command execution helpers
// ---------------------------------------------------------------------------

/// Execute QUERY by scanning converged DerivedStore directly (no SLG).
///
/// Strata have already converged all facts, so we can scan the DerivedStore
/// without re-derivation. WHERE filtering and RETURN projection are applied
/// in-memory.
///
/// NOTE: Currently unused because the DerivedStore uses inferred Arrow types
/// (Float64 for all property-derived columns), so string property values are
/// not preserved. Once `infer_expr_type` is improved to use actual schema types,
/// this function can be re-enabled for QUERYs whose WHERE/RETURN only reference
/// reliably-typed columns.
#[allow(dead_code)]
fn execute_query_inline(
    query: &GoalQuery,
    derived_store: &DerivedStore,
    params: &HashMap<String, Value>,
) -> DFResult<Vec<FactRow>> {
    let rule_name = query.rule_name.to_string();
    let batches = derived_store.get(&rule_name).cloned().unwrap_or_default();
    let rows = super::locy_eval::record_batches_to_locy_rows(&batches);

    // Apply WHERE filter
    let filtered = if let Some(ref where_expr) = query.where_expr {
        rows.into_iter()
            .filter(|row| {
                let merged = super::locy_query::merge_params(row, params);
                super::locy_eval::eval_expr(where_expr, &merged)
                    .map(|v| v.as_bool().unwrap_or(false))
                    .unwrap_or(false)
            })
            .collect()
    } else {
        rows
    };

    // Apply RETURN (project, distinct, order, skip, limit)
    super::locy_query::apply_return_clause(filtered, &query.return_clause, params)
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
}

/// Execute Cypher passthrough via execute_subplan.
async fn execute_cypher_inline(
    query: &uni_cypher::ast::Query,
    schema_info: &Arc<UniSchema>,
    params: &HashMap<String, Value>,
    graph_ctx: &Arc<GraphExecutionContext>,
    session_ctx: &Arc<RwLock<datafusion::prelude::SessionContext>>,
    storage: &Arc<StorageManager>,
) -> DFResult<Vec<FactRow>> {
    let planner = crate::query::planner::QueryPlanner::new(Arc::clone(schema_info));
    let logical_plan = planner.plan(query.clone()).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!("Cypher plan error: {e}"))
    })?;
    let batches = execute_subplan(
        &logical_plan,
        params,
        &HashMap::new(),
        graph_ctx,
        session_ctx,
        storage,
        schema_info,
    )
    .await?;
    Ok(super::locy_eval::record_batches_to_locy_rows(&batches))
}

/// Returns true if the WHERE or RETURN expressions contain property access
/// (e.g. `a.name`) that requires full node objects not available in DerivedStore.
#[allow(dead_code)]
fn needs_node_enrichment(query: &GoalQuery) -> bool {
    let has_property_in_where = query
        .where_expr
        .as_ref()
        .is_some_and(expr_has_property_access);
    let has_property_in_return = query
        .return_clause
        .as_ref()
        .is_some_and(|rc| {
            rc.items.iter().any(|item| match item {
                uni_cypher::ast::ReturnItem::Expr { expr, .. } => {
                    expr_has_property_access(expr)
                }
                uni_cypher::ast::ReturnItem::All => false,
            })
        });
    has_property_in_where || has_property_in_return
}

/// Recursively check whether an expression contains property access (`a.name`).
#[allow(dead_code)]
fn expr_has_property_access(expr: &Expr) -> bool {
    match expr {
        Expr::Property(..) => true,
        Expr::BinaryOp { left, right, .. } => {
            expr_has_property_access(left) || expr_has_property_access(right)
        }
        Expr::UnaryOp { expr, .. } => expr_has_property_access(expr),
        Expr::FunctionCall { args, .. } => args.iter().any(expr_has_property_access),
        Expr::List(items) => items.iter().any(expr_has_property_access),
        Expr::Map(entries) => entries.iter().any(|(_, e)| expr_has_property_access(e)),
        Expr::Case {
            expr: case_expr,
            when_then,
            else_expr,
        } => {
            case_expr
                .as_ref()
                .is_some_and(|e| expr_has_property_access(e))
                || when_then
                    .iter()
                    .any(|(w, t)| expr_has_property_access(w) || expr_has_property_access(t))
                || else_expr
                    .as_ref()
                    .is_some_and(|e| expr_has_property_access(e))
        }
        Expr::IsNull(e) | Expr::IsNotNull(e) | Expr::IsUnique(e) => expr_has_property_access(e),
        Expr::In { expr, list } => {
            expr_has_property_access(expr) || expr_has_property_access(list)
        }
        Expr::ArrayIndex { array, index } => {
            expr_has_property_access(array) || expr_has_property_access(index)
        }
        Expr::ArraySlice { array, start, end } => {
            expr_has_property_access(array)
                || start
                    .as_ref()
                    .is_some_and(|e| expr_has_property_access(e))
                || end
                    .as_ref()
                    .is_some_and(|e| expr_has_property_access(e))
        }
        Expr::Quantifier {
            list, predicate, ..
        } => expr_has_property_access(list) || expr_has_property_access(predicate),
        Expr::Reduce {
            init, list, expr, ..
        } => {
            expr_has_property_access(init)
                || expr_has_property_access(list)
                || expr_has_property_access(expr)
        }
        Expr::ListComprehension {
            list,
            where_clause,
            map_expr,
            ..
        } => {
            expr_has_property_access(list)
                || where_clause
                    .as_ref()
                    .is_some_and(|e| expr_has_property_access(e))
                || expr_has_property_access(map_expr)
        }
        Expr::PatternComprehension {
            where_clause,
            map_expr,
            ..
        } => {
            where_clause
                .as_ref()
                .is_some_and(|e| expr_has_property_access(e))
                || expr_has_property_access(map_expr)
        }
        Expr::ValidAt {
            entity, timestamp, ..
        } => expr_has_property_access(entity) || expr_has_property_access(timestamp),
        Expr::MapProjection { base, .. } => expr_has_property_access(base),
        Expr::LabelCheck { expr, .. } => expr_has_property_access(expr),
        // Leaf nodes (Literal, Parameter, Variable, Wildcard) and subqueries
        // (Exists, CountSubquery, CollectSubquery) — no property access.
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// run_program — core evaluation algorithm
// ---------------------------------------------------------------------------

#[expect(
    clippy::too_many_arguments,
    reason = "program evaluation requires full graph and session context"
)]
async fn run_program(
    strata: Vec<LocyStratum>,
    commands: Vec<LocyCommand>,
    registry: Arc<DerivedScanRegistry>,
    graph_ctx: Arc<GraphExecutionContext>,
    session_ctx: Arc<RwLock<datafusion::prelude::SessionContext>>,
    storage: Arc<StorageManager>,
    schema_info: Arc<UniSchema>,
    params: HashMap<String, Value>,
    output_schema: SchemaRef,
    max_iterations: usize,
    timeout: Duration,
    max_derived_bytes: usize,
    deterministic_best_by: bool,
    strict_probability_domain: bool,
    probability_epsilon: f64,
    exact_probability: bool,
    max_bdd_variables: usize,
    derived_store_slot: Arc<StdRwLock<Option<DerivedStore>>>,
    approximate_slot: Arc<StdRwLock<HashMap<String, Vec<String>>>>,
    iteration_counts_slot: Arc<StdRwLock<HashMap<String, usize>>>,
    peak_memory_slot: Arc<StdRwLock<usize>>,
    derivation_tracker: Option<Arc<ProvenanceStore>>,
    warnings_slot: Arc<StdRwLock<Vec<RuntimeWarning>>>,
    command_results_slot: Arc<StdRwLock<Vec<(usize, CommandResult)>>>,
    top_k_proofs: usize,
) -> DFResult<Vec<RecordBatch>> {
    let start = Instant::now();
    let mut derived_store = DerivedStore::new();

    // Evaluate each stratum in topological order
    for stratum in &strata {
        // Write cross-stratum facts into registry handles for strata we depend on
        write_cross_stratum_facts(&registry, &derived_store, stratum);

        let remaining_timeout = timeout.saturating_sub(start.elapsed());
        if remaining_timeout.is_zero() {
            return Err(datafusion::error::DataFusionError::Execution(
                "Locy program timeout exceeded during stratum evaluation".to_string(),
            ));
        }

        if stratum.is_recursive {
            // Convert LocyRulePlan → FixpointRulePlan and run fixpoint
            let fixpoint_rules =
                convert_to_fixpoint_plans(&stratum.rules, &registry, deterministic_best_by)?;
            let fixpoint_schema = build_fixpoint_output_schema(&stratum.rules);

            let exec = FixpointExec::new(
                fixpoint_rules,
                max_iterations,
                remaining_timeout,
                Arc::clone(&graph_ctx),
                Arc::clone(&session_ctx),
                Arc::clone(&storage),
                Arc::clone(&schema_info),
                params.clone(),
                Arc::clone(&registry),
                fixpoint_schema,
                max_derived_bytes,
                derivation_tracker.clone(),
                Arc::clone(&iteration_counts_slot),
                strict_probability_domain,
                probability_epsilon,
                exact_probability,
                max_bdd_variables,
                Arc::clone(&warnings_slot),
                Arc::clone(&approximate_slot),
                top_k_proofs,
            );

            let task_ctx = session_ctx.read().task_ctx();
            let exec_arc: Arc<dyn ExecutionPlan> = Arc::new(exec);
            let batches = collect_all_partitions(&exec_arc, task_ctx).await?;

            // FixpointExec concatenates all rules' output; store per-rule.
            // For now, store all output under each rule name (since FixpointExec
            // handles per-rule state internally, the output is already correct).
            // NOTE(deferred): Per-rule fact demultiplexing is not yet implemented.
            // FixpointExec concatenates all rules' output into a single batch stream.
            // Proper demux requires FixpointExec to tag output batches with rule identity
            // (e.g. an extra column or side-channel), which is a non-trivial change to
            // run_fixpoint_loop. The current schema-field-count heuristic (filter below)
            // works because recursive stratum rules share compatible schemas.
            // Revisit when cross-stratum consumption of individual recursive rules is needed.
            for rule in &stratum.rules {
                // Write converged facts into registry handles for cross-stratum consumers
                let rule_entries = registry.entries_for_rule(&rule.name);
                for entry in rule_entries {
                    if !entry.is_self_ref {
                        // Cross-stratum handles get the full fixpoint output
                        // In practice, FixpointExec already wrote self-ref handles;
                        // we need to write non-self-ref handles for later strata.
                        let all_facts: Vec<RecordBatch> = batches
                            .iter()
                            .filter(|b| {
                                // If schemas match, this batch belongs to this rule
                                let rule_schema = yield_columns_to_arrow_schema(&rule.yield_schema);
                                b.schema().fields().len() == rule_schema.fields().len()
                            })
                            .cloned()
                            .collect();
                        let mut guard = entry.data.write();
                        *guard = if all_facts.is_empty() {
                            vec![RecordBatch::new_empty(Arc::clone(&entry.schema))]
                        } else {
                            all_facts
                        };
                    }
                }
                derived_store.insert(rule.name.clone(), batches.clone());
            }
        } else {
            // Non-recursive: single-pass evaluation
            let fixpoint_rules =
                convert_to_fixpoint_plans(&stratum.rules, &registry, deterministic_best_by)?;
            let task_ctx = session_ctx.read().task_ctx();

            for (rule, fp_rule) in stratum.rules.iter().zip(fixpoint_rules.iter()) {
                // Process each clause independently (per-clause IS NOT).
                let mut tagged_clause_facts: Vec<(usize, Vec<RecordBatch>)> = Vec::new();
                for (clause_idx, (clause, fp_clause)) in
                    rule.clauses.iter().zip(fp_rule.clauses.iter()).enumerate()
                {
                    let mut batches = execute_subplan(
                        &clause.body,
                        &params,
                        &HashMap::new(),
                        &graph_ctx,
                        &session_ctx,
                        &storage,
                        &schema_info,
                    )
                    .await?;

                    // Apply negated IS-ref semantics per-clause.
                    for binding in &fp_clause.is_ref_bindings {
                        if binding.negated
                            && !binding.anti_join_cols.is_empty()
                            && let Some(entry) = registry.get(binding.derived_scan_index)
                        {
                            let neg_facts = entry.data.read().clone();
                            if !neg_facts.is_empty() {
                                if binding.target_has_prob && fp_rule.prob_column_name.is_some() {
                                    let complement_col =
                                        format!("__prob_complement_{}", binding.rule_name);
                                    if let Some(prob_col) = &binding.target_prob_col {
                                        batches =
                                            super::locy_fixpoint::apply_prob_complement_composite(
                                                batches,
                                                &neg_facts,
                                                &binding.anti_join_cols,
                                                prob_col,
                                                &complement_col,
                                            )?;
                                    } else {
                                        // target_has_prob but no prob_col: fall back to anti-join.
                                        batches = super::locy_fixpoint::apply_anti_join_composite(
                                            batches,
                                            &neg_facts,
                                            &binding.anti_join_cols,
                                        )?;
                                    }
                                } else {
                                    batches = super::locy_fixpoint::apply_anti_join_composite(
                                        batches,
                                        &neg_facts,
                                        &binding.anti_join_cols,
                                    )?;
                                }
                            }
                        }
                    }

                    // Multiply complement columns into PROB per-clause.
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
                        batches = super::locy_fixpoint::multiply_prob_factors(
                            batches,
                            fp_rule.prob_column_name.as_deref(),
                            &complement_cols,
                        )?;
                    }

                    tagged_clause_facts.push((clause_idx, batches));
                }

                // Record provenance and detect shared proofs for non-recursive rules.
                let shared_info = if let Some(ref tracker) = derivation_tracker {
                    super::locy_fixpoint::record_and_detect_lineage_nonrecursive(
                        fp_rule,
                        &tagged_clause_facts,
                        tracker,
                        &warnings_slot,
                        &registry,
                        top_k_proofs,
                    )
                } else {
                    None
                };

                // Flatten tagged facts for post-fixpoint chain.
                let mut all_clause_facts: Vec<RecordBatch> = tagged_clause_facts
                    .into_iter()
                    .flat_map(|(_, batches)| batches)
                    .collect();

                // Apply BDD for shared groups if exact_probability is enabled.
                if exact_probability
                    && let Some(ref info) = shared_info
                    && let Some(ref tracker) = derivation_tracker
                {
                    all_clause_facts = super::locy_fixpoint::apply_exact_wmc(
                        all_clause_facts,
                        fp_rule,
                        info,
                        tracker,
                        max_bdd_variables,
                        &warnings_slot,
                        &approximate_slot,
                    )?;
                }

                // Apply post-fixpoint operators (PRIORITY, FOLD, BEST BY) on union.
                let facts = super::locy_fixpoint::apply_post_fixpoint_chain(
                    all_clause_facts,
                    fp_rule,
                    &task_ctx,
                    strict_probability_domain,
                    probability_epsilon,
                )
                .await?;

                // Write facts into registry handles for later strata
                write_facts_to_registry(&registry, &rule.name, &facts);
                derived_store.insert(rule.name.clone(), facts);
            }
        }
    }

    // Compute peak memory from derived store byte sizes
    let peak_bytes: usize = derived_store
        .relations
        .values()
        .flat_map(|batches| batches.iter())
        .map(|b| {
            b.columns()
                .iter()
                .map(|col| col.get_buffer_memory_size())
                .sum::<usize>()
        })
        .sum();
    *peak_memory_slot.write().unwrap() = peak_bytes;

    // Execute inline Cypher commands via execute_subplan.
    // QUERY is deferred to the orchestrator: the DerivedStore uses inferred types
    // (e.g. Float64 for property-derived columns) which don't preserve the actual
    // property values. The orchestrator's SLG path re-derives with correct types.
    // DERIVE/ASSUME/EXPLAIN/ABDUCE are also deferred (need savepoints, tree output, etc.).
    let mut inline_results: Vec<(usize, CommandResult)> = Vec::new();
    for (cmd_idx, cmd) in commands.iter().enumerate() {
        if let LocyCommand::Cypher { query } = cmd {
            let result = execute_cypher_inline(
                query,
                &schema_info,
                &params,
                &graph_ctx,
                &session_ctx,
                &storage,
            )
            .await?;
            inline_results.push((cmd_idx, CommandResult::Cypher(result)));
        }
        // QUERY, DERIVE, ASSUME, EXPLAIN, ABDUCE: deferred to orchestrator
    }
    *command_results_slot.write().unwrap() = inline_results;

    let stats = vec![build_stats_batch(&derived_store, &strata, output_schema)];
    *derived_store_slot.write().unwrap() = Some(derived_store);
    Ok(stats)
}

// ---------------------------------------------------------------------------
// Cross-stratum fact injection
// ---------------------------------------------------------------------------

/// Write already-evaluated facts into registry handles for cross-stratum IS-refs.
fn write_cross_stratum_facts(
    registry: &DerivedScanRegistry,
    derived_store: &DerivedStore,
    stratum: &LocyStratum,
) {
    // For each rule in this stratum, find IS-refs to rules in other strata
    for rule in &stratum.rules {
        for clause in &rule.clauses {
            for is_ref in &clause.is_refs {
                // If this IS-ref points to a rule already in the derived store
                // (i.e., from a previous stratum), write its facts into the registry
                if let Some(facts) = derived_store.get(&is_ref.rule_name) {
                    write_facts_to_registry(registry, &is_ref.rule_name, facts);
                }
            }
        }
    }
}

/// Write facts into non-self-ref registry handles for a given rule.
fn write_facts_to_registry(registry: &DerivedScanRegistry, rule_name: &str, facts: &[RecordBatch]) {
    let entries = registry.entries_for_rule(rule_name);
    for entry in entries {
        if !entry.is_self_ref {
            let mut guard = entry.data.write();
            *guard = if facts.is_empty() || facts.iter().all(|b| b.num_rows() == 0) {
                vec![RecordBatch::new_empty(Arc::clone(&entry.schema))]
            } else {
                // Try to re-wrap batches with the entry's schema for column name
                // alignment. If the types don't match (e.g. inferred Float64 vs
                // actual Utf8 from schema mode), fall back to the batch's own
                // schema to avoid silent data loss.
                facts
                    .iter()
                    .filter(|b| b.num_rows() > 0)
                    .map(|b| {
                        RecordBatch::try_new(Arc::clone(&entry.schema), b.columns().to_vec())
                            .unwrap_or_else(|_| b.clone())
                    })
                    .collect()
            };
        }
    }
}

// ---------------------------------------------------------------------------
// LocyRulePlan → FixpointRulePlan conversion
// ---------------------------------------------------------------------------

/// Convert logical `LocyRulePlan` types to physical `FixpointRulePlan` types.
fn convert_to_fixpoint_plans(
    rules: &[LocyRulePlan],
    registry: &DerivedScanRegistry,
    deterministic_best_by: bool,
) -> DFResult<Vec<FixpointRulePlan>> {
    rules
        .iter()
        .map(|rule| {
            let yield_schema = yield_columns_to_arrow_schema(&rule.yield_schema);
            let key_column_indices: Vec<usize> = rule
                .yield_schema
                .iter()
                .enumerate()
                .filter(|(_, yc)| yc.is_key)
                .map(|(i, _)| i)
                .collect();

            let clauses: Vec<FixpointClausePlan> = rule
                .clauses
                .iter()
                .map(|clause| {
                    let is_ref_bindings = convert_is_refs(&clause.is_refs, registry)?;
                    Ok(FixpointClausePlan {
                        body_logical: clause.body.clone(),
                        is_ref_bindings,
                        priority: clause.priority,
                        along_bindings: clause.along_bindings.clone(),
                    })
                })
                .collect::<DFResult<Vec<_>>>()?;

            let fold_bindings = convert_fold_bindings(&rule.fold_bindings, &rule.yield_schema)?;
            let best_by_criteria =
                convert_best_by_criteria(&rule.best_by_criteria, &rule.yield_schema)?;

            let has_priority = rule.priority.is_some();

            // Add __priority column to yield schema if PRIORITY is used
            let yield_schema = if has_priority {
                let mut fields: Vec<Arc<Field>> = yield_schema.fields().iter().cloned().collect();
                fields.push(Arc::new(Field::new("__priority", DataType::Int64, true)));
                ArrowSchema::new(fields)
            } else {
                yield_schema
            };

            let prob_column_name = rule
                .yield_schema
                .iter()
                .find(|yc| yc.is_prob)
                .map(|yc| yc.name.clone());

            Ok(FixpointRulePlan {
                name: rule.name.clone(),
                clauses,
                yield_schema: Arc::new(yield_schema),
                key_column_indices,
                priority: rule.priority,
                has_fold: !rule.fold_bindings.is_empty(),
                fold_bindings,
                has_best_by: !rule.best_by_criteria.is_empty(),
                best_by_criteria,
                has_priority,
                deterministic: deterministic_best_by,
                prob_column_name,
            })
        })
        .collect()
}

/// Convert `LocyIsRef` to `IsRefBinding` by looking up scan indices in the registry.
fn convert_is_refs(
    is_refs: &[LocyIsRef],
    registry: &DerivedScanRegistry,
) -> DFResult<Vec<IsRefBinding>> {
    is_refs
        .iter()
        .map(|is_ref| {
            let entries = registry.entries_for_rule(&is_ref.rule_name);
            // Find the matching entry (prefer self-ref for same-stratum rules)
            let entry = entries
                .iter()
                .find(|e| e.is_self_ref)
                .or_else(|| entries.first())
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "No derived scan entry found for IS-ref to '{}'",
                        is_ref.rule_name
                    ))
                })?;

            // For negated IS-refs, compute (left_body_col, right_derived_col) pairs for
            // anti-join filtering. Subject vars are assumed to be node variables, so
            // the body column is `{var}._vid` (UInt64). The derived column name is taken
            // positionally from the registry entry's schema (KEY columns come first).
            let anti_join_cols = if is_ref.negated {
                is_ref
                    .subjects
                    .iter()
                    .enumerate()
                    .filter_map(|(i, s)| {
                        if let uni_cypher::ast::Expr::Variable(var) = s {
                            let right_col = entry
                                .schema
                                .fields()
                                .get(i)
                                .map(|f| f.name().clone())
                                .unwrap_or_else(|| var.clone());
                            // After LocyProject the subject column is renamed to the yield
                            // column name (just `var`, not `var._vid`). Use bare var as left.
                            Some((var.clone(), right_col))
                        } else {
                            None
                        }
                    })
                    .collect()
            } else {
                Vec::new()
            };

            // Provenance join cols: for ALL IS-refs (not just negated), compute
            // (body_col, derived_col) pairs so shared-proof detection can trace
            // which source facts contributed to each derived row.
            let provenance_join_cols: Vec<(String, String)> = is_ref
                .subjects
                .iter()
                .enumerate()
                .filter_map(|(i, s)| {
                    if let uni_cypher::ast::Expr::Variable(var) = s {
                        let right_col = entry
                            .schema
                            .fields()
                            .get(i)
                            .map(|f| f.name().clone())
                            .unwrap_or_else(|| var.clone());
                        Some((var.clone(), right_col))
                    } else {
                        None
                    }
                })
                .collect();

            Ok(IsRefBinding {
                derived_scan_index: entry.scan_index,
                rule_name: is_ref.rule_name.clone(),
                is_self_ref: entry.is_self_ref,
                negated: is_ref.negated,
                anti_join_cols,
                target_has_prob: is_ref.target_has_prob,
                target_prob_col: is_ref.target_prob_col.clone(),
                provenance_join_cols,
            })
        })
        .collect()
}

/// Convert fold binding expressions to physical `FoldBinding`.
///
/// The input column is looked up by the fold binding's output name (e.g., "total")
/// in the yield schema, since the LocyProject aliases the aggregate input expression
/// to the fold output name.
fn convert_fold_bindings(
    fold_bindings: &[(String, Expr)],
    yield_schema: &[LocyYieldColumn],
) -> DFResult<Vec<FoldBinding>> {
    fold_bindings
        .iter()
        .map(|(name, expr)| {
            let (kind, _input_col_name) = parse_fold_aggregate(expr)?;

            // CountAll has no input column — LocyProject skips the output column
            // entirely, so there is nothing to look up.
            if kind == FoldAggKind::CountAll {
                return Ok(FoldBinding {
                    output_name: name.clone(),
                    kind,
                    input_col_index: 0, // unused for CountAll
                });
            }

            // The LocyProject projects the aggregate input expression AS the fold
            // output name, so the input column index matches the yield schema position.
            let input_col_index = yield_schema
                .iter()
                .position(|yc| yc.name == *name)
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "FOLD column '{}' not found in yield schema",
                        name
                    ))
                })?;
            Ok(FoldBinding {
                output_name: name.clone(),
                kind,
                input_col_index,
            })
        })
        .collect()
}

/// Parse a fold aggregate expression into (kind, input_column_name).
fn parse_fold_aggregate(expr: &Expr) -> DFResult<(FoldAggKind, String)> {
    match expr {
        Expr::FunctionCall { name, args, .. } => {
            let upper = name.to_uppercase();
            let is_count = matches!(upper.as_str(), "COUNT" | "MCOUNT");

            // COUNT/MCOUNT with zero args → CountAll (like SQL COUNT(*))
            if is_count && args.is_empty() {
                return Ok((FoldAggKind::CountAll, String::new()));
            }

            let kind = match upper.as_str() {
                "SUM" | "MSUM" => FoldAggKind::Sum,
                "MAX" | "MMAX" => FoldAggKind::Max,
                "MIN" | "MMIN" => FoldAggKind::Min,
                "COUNT" | "MCOUNT" => FoldAggKind::Count,
                "AVG" => FoldAggKind::Avg,
                "COLLECT" => FoldAggKind::Collect,
                "MNOR" => FoldAggKind::Nor,
                "MPROD" => FoldAggKind::Prod,
                _ => {
                    return Err(datafusion::error::DataFusionError::Plan(format!(
                        "Unknown FOLD aggregate function: {}",
                        name
                    )));
                }
            };
            let col_name = match args.first() {
                Some(Expr::Variable(v)) => v.clone(),
                Some(Expr::Property(_, prop)) => prop.clone(),
                Some(other) => other.to_string_repr(),
                None => {
                    return Err(datafusion::error::DataFusionError::Plan(
                        "FOLD aggregate function requires at least one argument".to_string(),
                    ));
                }
            };
            Ok((kind, col_name))
        }
        _ => Err(datafusion::error::DataFusionError::Plan(
            "FOLD binding must be a function call (e.g., SUM(x))".to_string(),
        )),
    }
}

/// Convert best-by criteria expressions to physical `SortCriterion`.
///
/// Resolves the criteria column by trying:
/// 1. Property name (e.g., `e.cost` → "cost")
/// 2. Variable name (e.g., `cost`)
/// 3. Full expression string (e.g., "e.cost" as a variable name)
fn convert_best_by_criteria(
    criteria: &[(Expr, bool)],
    yield_schema: &[LocyYieldColumn],
) -> DFResult<Vec<SortCriterion>> {
    criteria
        .iter()
        .map(|(expr, ascending)| {
            let col_name = match expr {
                Expr::Property(_, prop) => prop.clone(),
                Expr::Variable(v) => v.clone(),
                _ => {
                    return Err(datafusion::error::DataFusionError::Plan(
                        "BEST BY criterion must be a variable or property reference".to_string(),
                    ));
                }
            };
            // Try exact match first, then try just the last component after '.'
            let col_index = yield_schema
                .iter()
                .position(|yc| yc.name == col_name)
                .or_else(|| {
                    let short_name = col_name.rsplit('.').next().unwrap_or(&col_name);
                    yield_schema.iter().position(|yc| yc.name == short_name)
                })
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "BEST BY column '{}' not found in yield schema",
                        col_name
                    ))
                })?;
            Ok(SortCriterion {
                col_index,
                ascending: *ascending,
                nulls_first: false,
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Schema helpers
// ---------------------------------------------------------------------------

/// Convert `LocyYieldColumn` slice to Arrow schema using inferred types.
fn yield_columns_to_arrow_schema(columns: &[LocyYieldColumn]) -> ArrowSchema {
    let fields: Vec<Arc<Field>> = columns
        .iter()
        .map(|yc| Arc::new(Field::new(&yc.name, yc.data_type.clone(), true)))
        .collect();
    ArrowSchema::new(fields)
}

/// Build a combined output schema for fixpoint (union of all rules' schemas).
fn build_fixpoint_output_schema(rules: &[LocyRulePlan]) -> SchemaRef {
    // FixpointExec concatenates all rules' output, using the first rule's schema
    // as the output schema (all rules in a recursive stratum share compatible schemas).
    if let Some(rule) = rules.first() {
        Arc::new(yield_columns_to_arrow_schema(&rule.yield_schema))
    } else {
        Arc::new(ArrowSchema::empty())
    }
}

/// Build a stats RecordBatch summarizing derived relation counts.
fn build_stats_batch(
    derived_store: &DerivedStore,
    _strata: &[LocyStratum],
    output_schema: SchemaRef,
) -> RecordBatch {
    // Build a simple stats batch with rule_name and fact_count columns
    let mut rule_names: Vec<String> = derived_store.rule_names().map(String::from).collect();
    rule_names.sort();

    let name_col: arrow_array::StringArray = rule_names.iter().map(|s| Some(s.as_str())).collect();
    let count_col: arrow_array::Int64Array = rule_names
        .iter()
        .map(|name| Some(derived_store.fact_count(name) as i64))
        .collect();

    let stats_schema = stats_schema();
    RecordBatch::try_new(stats_schema, vec![Arc::new(name_col), Arc::new(count_col)])
        .unwrap_or_else(|_| RecordBatch::new_empty(output_schema))
}

/// Schema for the stats batch returned when no commands are present.
pub fn stats_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Arc::new(Field::new("rule_name", DataType::Utf8, false)),
        Arc::new(Field::new("fact_count", DataType::Int64, false)),
    ]))
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, LargeBinaryArray, StringArray};

    #[test]
    fn test_derived_store_insert_and_get() {
        let mut store = DerivedStore::new();
        assert!(store.get("test").is_none());

        let schema = Arc::new(ArrowSchema::new(vec![Arc::new(Field::new(
            "x",
            DataType::LargeBinary,
            true,
        ))]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(LargeBinaryArray::from(vec![
                Some(b"a" as &[u8]),
                Some(b"b"),
            ]))],
        )
        .unwrap();

        store.insert("test".to_string(), vec![batch.clone()]);

        let facts = store.get("test").unwrap();
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].num_rows(), 2);
    }

    #[test]
    fn test_derived_store_fact_count() {
        let mut store = DerivedStore::new();
        assert_eq!(store.fact_count("empty"), 0);

        let schema = Arc::new(ArrowSchema::new(vec![Arc::new(Field::new(
            "x",
            DataType::LargeBinary,
            true,
        ))]));
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(LargeBinaryArray::from(vec![Some(b"a" as &[u8])]))],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(LargeBinaryArray::from(vec![
                Some(b"b" as &[u8]),
                Some(b"c"),
            ]))],
        )
        .unwrap();

        store.insert("test".to_string(), vec![batch1, batch2]);
        assert_eq!(store.fact_count("test"), 3);
    }

    #[test]
    fn test_stats_batch_schema() {
        let schema = stats_schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "rule_name");
        assert_eq!(schema.field(1).name(), "fact_count");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
    }

    #[test]
    fn test_stats_batch_content() {
        let mut store = DerivedStore::new();
        let schema = Arc::new(ArrowSchema::new(vec![Arc::new(Field::new(
            "x",
            DataType::LargeBinary,
            true,
        ))]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(LargeBinaryArray::from(vec![
                Some(b"a" as &[u8]),
                Some(b"b"),
            ]))],
        )
        .unwrap();
        store.insert("reach".to_string(), vec![batch]);

        let output_schema = stats_schema();
        let stats = build_stats_batch(&store, &[], Arc::clone(&output_schema));
        assert_eq!(stats.num_rows(), 1);

        let names = stats
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "reach");

        let counts = stats
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.value(0), 2);
    }

    #[test]
    fn test_yield_columns_to_arrow_schema() {
        let columns = vec![
            LocyYieldColumn {
                name: "a".to_string(),
                is_key: true,
                is_prob: false,
                data_type: DataType::UInt64,
            },
            LocyYieldColumn {
                name: "b".to_string(),
                is_key: false,
                is_prob: false,
                data_type: DataType::LargeUtf8,
            },
            LocyYieldColumn {
                name: "c".to_string(),
                is_key: true,
                is_prob: false,
                data_type: DataType::Float64,
            },
        ];

        let schema = yield_columns_to_arrow_schema(&columns);
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "a");
        assert_eq!(schema.field(1).name(), "b");
        assert_eq!(schema.field(2).name(), "c");
        // Fields use inferred types
        assert_eq!(schema.field(0).data_type(), &DataType::UInt64);
        assert_eq!(schema.field(1).data_type(), &DataType::LargeUtf8);
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
        for field in schema.fields() {
            assert!(field.is_nullable());
        }
    }

    #[test]
    fn test_key_column_indices() {
        let columns = [
            LocyYieldColumn {
                name: "a".to_string(),
                is_key: true,
                is_prob: false,
                data_type: DataType::LargeBinary,
            },
            LocyYieldColumn {
                name: "b".to_string(),
                is_key: false,
                is_prob: false,
                data_type: DataType::LargeBinary,
            },
            LocyYieldColumn {
                name: "c".to_string(),
                is_key: true,
                is_prob: false,
                data_type: DataType::LargeBinary,
            },
        ];

        let key_indices: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_, yc)| yc.is_key)
            .map(|(i, _)| i)
            .collect();
        assert_eq!(key_indices, vec![0, 2]);
    }

    #[test]
    fn test_parse_fold_aggregate_sum() {
        let expr = Expr::FunctionCall {
            name: "SUM".to_string(),
            args: vec![Expr::Variable("cost".to_string())],
            distinct: false,
            window_spec: None,
        };
        let (kind, col) = parse_fold_aggregate(&expr).unwrap();
        assert!(matches!(kind, FoldAggKind::Sum));
        assert_eq!(col, "cost");
    }

    #[test]
    fn test_parse_fold_aggregate_monotonic() {
        let expr = Expr::FunctionCall {
            name: "MMAX".to_string(),
            args: vec![Expr::Variable("score".to_string())],
            distinct: false,
            window_spec: None,
        };
        let (kind, col) = parse_fold_aggregate(&expr).unwrap();
        assert!(matches!(kind, FoldAggKind::Max));
        assert_eq!(col, "score");
    }

    #[test]
    fn test_parse_fold_aggregate_unknown() {
        let expr = Expr::FunctionCall {
            name: "UNKNOWN_AGG".to_string(),
            args: vec![Expr::Variable("x".to_string())],
            distinct: false,
            window_spec: None,
        };
        assert!(parse_fold_aggregate(&expr).is_err());
    }

    #[test]
    fn test_no_commands_returns_stats() {
        let store = DerivedStore::new();
        let output_schema = stats_schema();
        let stats = build_stats_batch(&store, &[], Arc::clone(&output_schema));
        // Empty store → 0 rows
        assert_eq!(stats.num_rows(), 0);
    }
}
