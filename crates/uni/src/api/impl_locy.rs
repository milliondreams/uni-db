// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Locy engine integration: wires the Locy compiler and native execution engine to the real database.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use async_trait::async_trait;
use uni_common::{Result, UniError, Value};
use uni_cypher::ast::{Expr, Pattern, Query};
use uni_cypher::locy_ast::RuleOutput;
use uni_locy::types::CompiledCommand;
use uni_locy::{
    CommandResult, CompiledProgram, DerivedFactSet, FactRow, LocyCompileError, LocyConfig,
    LocyError, LocyStats, RuntimeWarning, SavepointId, compile,
};
use uni_query::{QueryMetrics, QueryPlanner};

use crate::api::locy_result::LocyResult;
use uni_query::query::df_graph::locy_ast_builder::build_match_return_query;
use uni_query::query::df_graph::locy_delta::{RowRelation, RowStore, extract_cypher_conditions};
use uni_query::query::df_graph::locy_derive::CollectedDeriveOutput;
use uni_query::query::df_graph::locy_eval::record_batches_to_locy_rows;
use uni_query::query::df_graph::locy_explain::ProvenanceStore;
use uni_query::query::df_graph::{DerivedFactSource, LocyExecutionContext};

/// Session-level registry for pre-compiled Locy rules.
///
/// Rules registered here are automatically merged into subsequent `evaluate()`
/// calls, eliminating the need to redeclare rules across multiple evaluations
/// (e.g., baseline, EXPLAIN, ASSUME, ABDUCE in notebooks).
#[derive(Debug, Default, Clone)]
pub struct LocyRuleRegistry {
    /// Compiled rules indexed by rule name.
    pub rules: HashMap<String, uni_locy::types::CompiledRule>,
    /// Strata from registered programs, for execution ordering.
    pub strata: Vec<uni_locy::types::Stratum>,
}

/// Compile and register rules into an existing rule registry.
///
/// Shared logic used by `Uni::register_rules()` and `Session::register_rules()`.
pub(crate) fn register_rules_on_registry(
    registry_lock: &std::sync::RwLock<LocyRuleRegistry>,
    program: &str,
) -> Result<()> {
    let ast = uni_cypher::parse_locy(program).map_err(map_parse_error)?;
    let registry = registry_lock.read().unwrap();
    let compiled = if registry.rules.is_empty() {
        drop(registry);
        compile(&ast).map_err(map_compile_error)?
    } else {
        let external_names: Vec<String> = registry.rules.keys().cloned().collect();
        drop(registry);
        uni_locy::compile_with_external_rules(&ast, &external_names).map_err(map_compile_error)?
    };
    let mut registry = registry_lock.write().unwrap();
    for (name, rule) in compiled.rule_catalog {
        registry.rules.insert(name, rule);
    }
    let base_id = registry.strata.len();
    for mut stratum in compiled.strata {
        let old_id = stratum.id;
        stratum.id = base_id + old_id;
        stratum.depends_on = stratum.depends_on.iter().map(|d| base_id + d).collect();
        registry.strata.push(stratum);
    }
    Ok(())
}

/// Evaluate a Locy program against the database with a specific rule registry.
///
/// This is the core evaluation path used by Session and Transaction.
pub(crate) async fn evaluate_with_db_and_config(
    db: &crate::api::UniInner,
    program: &str,
    config: &LocyConfig,
    rule_registry: &std::sync::RwLock<LocyRuleRegistry>,
) -> Result<LocyResult> {
    // Compile with the given registry
    let ast = uni_cypher::parse_locy(program).map_err(map_parse_error)?;
    let external_names: Option<Vec<String>> = {
        let registry = rule_registry.read().unwrap();
        if registry.rules.is_empty() {
            None
        } else {
            Some(registry.rules.keys().cloned().collect())
        }
    };
    let mut compiled = if let Some(names) = external_names {
        uni_locy::compile_with_external_rules(&ast, &names).map_err(map_compile_error)?
    } else {
        compile(&ast).map_err(map_compile_error)?
    };

    // Merge registered rules
    {
        let registry = rule_registry.read().unwrap();
        if !registry.rules.is_empty() {
            for (name, rule) in &registry.rules {
                compiled
                    .rule_catalog
                    .entry(name.clone())
                    .or_insert_with(|| rule.clone());
            }
            let base_id = registry.strata.len();
            for stratum in &mut compiled.strata {
                stratum.id += base_id;
                stratum.depends_on = stratum.depends_on.iter().map(|d| d + base_id).collect();
            }
            let mut merged_strata = registry.strata.clone();
            merged_strata.append(&mut compiled.strata);
            compiled.strata = merged_strata;
        }
    }

    // Create a LocyEngine directly from &UniInner.
    // Session-level: collect DERIVE output for deferred materialization.
    let engine = LocyEngine {
        db,
        tx_l0_override: None,
        collect_derive: true,
    };
    engine.evaluate_compiled_with_config(compiled, config).await
}

/// Engine for evaluating Locy programs against a real database.
pub struct LocyEngine<'a> {
    pub(crate) db: &'a crate::api::UniInner,
    /// When set, the engine routes reads/writes through this private L0 buffer
    /// (commit-time serialization for transactions).
    pub(crate) tx_l0_override: Option<Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
    /// When true, DERIVE commands collect ASTs + data instead of executing.
    /// Session-level evaluation sets this to true; transaction-level sets false.
    pub(crate) collect_derive: bool,
}

impl crate::api::Uni {
    /// Create a Locy evaluation engine bound to this database (internal).
    ///
    /// All external access goes through `Session::locy()` / `Session::locy_with()`.
    #[allow(dead_code)]
    pub(crate) fn locy(&self) -> LocyEngine<'_> {
        LocyEngine {
            db: &self.inner,
            tx_l0_override: None,
            collect_derive: true,
        }
    }
}

impl<'a> LocyEngine<'a> {
    /// Parse and compile a Locy program without executing it.
    ///
    /// If the session's rule registry contains pre-compiled rules, their names
    /// are passed to the compiler so that IS-ref and QUERY references to
    /// registered rules are accepted during validation.
    pub fn compile_only(&self, program: &str) -> Result<CompiledProgram> {
        let ast = uni_cypher::parse_locy(program).map_err(map_parse_error)?;
        let registry = self.db.locy_rule_registry.read().unwrap();
        if registry.rules.is_empty() {
            drop(registry);
            compile(&ast).map_err(map_compile_error)
        } else {
            let external_names: Vec<String> = registry.rules.keys().cloned().collect();
            drop(registry);
            uni_locy::compile_with_external_rules(&ast, &external_names).map_err(map_compile_error)
        }
    }

    /// Compile and register a Locy program's rules for reuse.
    ///
    /// Rules registered here persist within the database session and are
    /// automatically merged into subsequent `evaluate()` calls, so notebooks
    /// can define rules once and run QUERY, EXPLAIN, ASSUME, ABDUCE without
    /// redeclaring the full rule set each time.
    pub fn register(&self, program: &str) -> Result<()> {
        let compiled = self.compile_only(program)?;
        let mut registry = self.db.locy_rule_registry.write().unwrap();
        for (name, rule) in compiled.rule_catalog {
            registry.rules.insert(name, rule);
        }
        // Merge strata, assigning new IDs to avoid collisions.
        let base_id = registry.strata.len();
        for mut stratum in compiled.strata {
            let old_id = stratum.id;
            stratum.id = base_id + old_id;
            stratum.depends_on = stratum.depends_on.iter().map(|d| base_id + d).collect();
            registry.strata.push(stratum);
        }
        Ok(())
    }

    /// Clear all registered Locy rules from the session.
    pub fn clear_registry(&self) {
        let mut registry = self.db.locy_rule_registry.write().unwrap();
        registry.rules.clear();
        registry.strata.clear();
    }

    /// Parse, compile, and evaluate a Locy program with default config.
    pub async fn evaluate(&self, program: &str) -> Result<LocyResult> {
        self.evaluate_with_config(program, &LocyConfig::default())
            .await
    }

    /// Start building a Locy evaluation with fluent parameter binding.
    ///
    /// Mirrors `db.query_with(cypher).param(…).fetch_all()` for Cypher.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use uni_db::Uni;
    /// # async fn example(db: &Uni) -> uni_db::Result<()> {
    /// let result = db.session()
    ///     .locy_with("CREATE RULE ep AS MATCH (e:Episode) WHERE e.agent_id = $aid YIELD KEY e")
    ///     .param("aid", "agent-123")
    ///     .run()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn evaluate_with(&self, program: &str) -> crate::api::locy_builder::InnerLocyBuilder<'_> {
        crate::api::locy_builder::InnerLocyBuilder::new(self.db, program)
    }

    /// Convenience wrapper for EXPLAIN RULE commands.
    pub async fn explain(&self, program: &str) -> Result<LocyResult> {
        self.evaluate(program).await
    }

    /// Parse, compile, and evaluate a Locy program with custom config.
    ///
    /// If rules were previously registered via `register()`, they are
    /// automatically merged into the compiled program before execution.
    pub async fn evaluate_with_config(
        &self,
        program: &str,
        config: &LocyConfig,
    ) -> Result<LocyResult> {
        let mut compiled = self.compile_only(program)?;

        // Merge registered rules into the compiled program.
        {
            let registry = self.db.locy_rule_registry.read().unwrap();
            if !registry.rules.is_empty() {
                for (name, rule) in &registry.rules {
                    compiled
                        .rule_catalog
                        .entry(name.clone())
                        .or_insert_with(|| rule.clone());
                }
                let base_id = registry.strata.len();
                for stratum in &mut compiled.strata {
                    stratum.id += base_id;
                    stratum.depends_on = stratum.depends_on.iter().map(|d| d + base_id).collect();
                }
                let mut merged_strata = registry.strata.clone();
                merged_strata.append(&mut compiled.strata);
                compiled.strata = merged_strata;
            }
        }

        self.evaluate_compiled_with_config(compiled, config).await
    }

    /// Evaluate an already-compiled Locy program with custom config.
    ///
    /// This is the core execution path: it takes a `CompiledProgram` (with any
    /// registry merges already applied) and runs it through planning, execution,
    /// and command dispatch.
    pub async fn evaluate_compiled_with_config(
        &self,
        compiled: CompiledProgram,
        config: &LocyConfig,
    ) -> Result<LocyResult> {
        let start = Instant::now();

        // Capture current version for staleness detection in DerivedFactSet
        let evaluated_at_version = if self.collect_derive {
            if let Some(ref w) = self.db.writer {
                w.read()
                    .await
                    .l0_manager
                    .get_current()
                    .read()
                    .current_version
            } else {
                0
            }
        } else {
            0
        };

        // 1. Build logical plan
        let schema = self.db.schema.schema();
        let query_planner = uni_query::QueryPlanner::new(schema);
        let plan_builder = uni_query::query::locy_planner::LocyPlanBuilder::new(&query_planner);
        let logical = plan_builder
            .build_program_plan(
                &compiled,
                config.max_iterations,
                config.timeout,
                config.max_derived_bytes,
                config.deterministic_best_by,
                config.strict_probability_domain,
                config.probability_epsilon,
                config.exact_probability,
                config.max_bdd_variables,
                config.top_k_proofs,
            )
            .map_err(|e| UniError::Query {
                message: format!("LocyPlanBuildError: {e}"),
                query: None,
            })?;

        // 2. Create executor + physical planner
        let mut df_executor = uni_query::Executor::new(self.db.storage.clone());
        df_executor.set_config(self.db.config.clone());
        if let Some(ref w) = self.db.writer {
            df_executor.set_writer(w.clone());
        }
        df_executor.set_xervo_runtime(self.db.xervo_runtime.clone());
        df_executor.set_procedure_registry(self.db.procedure_registry.clone());
        if let Ok(reg) = self.db.custom_functions.read()
            && !reg.is_empty()
        {
            df_executor.set_custom_functions(std::sync::Arc::new(reg.clone()));
        }

        let (session_ctx, planner, _prop_mgr) = df_executor
            .create_datafusion_planner(&self.db.properties, &config.params)
            .await
            .map_err(map_native_df_error)?;

        // 3. Physical plan
        let exec_plan = planner.plan(&logical).map_err(map_native_df_error)?;

        // 4. Create tracker for EXPLAIN commands or shared-proof detection
        let has_explain = compiled
            .commands
            .iter()
            .any(|c| matches!(c, CompiledCommand::ExplainRule(_)));
        let has_prob_fold = compiled.strata.iter().any(|s| {
            s.rules.iter().any(|r| {
                r.clauses.iter().any(|c| {
                    c.fold.iter().any(|f| {
                        if let uni_cypher::ast::Expr::FunctionCall { name, .. } = &f.aggregate {
                            matches!(name.to_uppercase().as_str(), "MNOR" | "MPROD")
                        } else {
                            false
                        }
                    })
                })
            })
        });
        let needs_tracker = has_explain || has_prob_fold;
        let tracker: Option<Arc<uni_query::query::df_graph::ProvenanceStore>> = if needs_tracker {
            Some(Arc::new(uni_query::query::df_graph::ProvenanceStore::new()))
        } else {
            None
        };

        let (
            derived_store_slot,
            iteration_counts_slot,
            peak_memory_slot,
            warnings_slot,
            approximate_slot,
            command_results_slot,
        ) = if let Some(program_exec) = exec_plan
            .as_any()
            .downcast_ref::<uni_query::query::df_graph::LocyProgramExec>(
        ) {
            if let Some(ref t) = tracker {
                program_exec.set_derivation_tracker(Arc::clone(t));
            }
            (
                program_exec.derived_store_slot(),
                program_exec.iteration_counts_slot(),
                program_exec.peak_memory_slot(),
                program_exec.warnings_slot(),
                program_exec.approximate_slot(),
                program_exec.command_results_slot(),
            )
        } else {
            (
                Arc::new(std::sync::RwLock::new(None)),
                Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
                Arc::new(std::sync::RwLock::new(0usize)),
                Arc::new(std::sync::RwLock::new(Vec::new())),
                Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
                Arc::new(std::sync::RwLock::new(Vec::new())),
            )
        };

        // 5. Execute strata
        let _stats_batches = uni_query::Executor::collect_batches(&session_ctx, exec_plan)
            .await
            .map_err(map_native_df_error)?;

        // 6. Extract native DerivedStore
        let native_store = derived_store_slot
            .write()
            .unwrap()
            .take()
            .unwrap_or_default();

        // 7. Convert native DerivedStore → row-based RowStore for SLG/EXPLAIN
        let mut orch_store = native_store_to_row_store(&native_store, &compiled);

        // 8. Dispatch commands via native trait interfaces
        let native_ctx = NativeExecutionAdapter::new(
            self.db,
            &native_store,
            &compiled,
            planner.graph_ctx().clone(),
            planner.session_ctx().clone(),
            config.params.clone(),
            self.tx_l0_override.clone(),
        );
        let mut locy_stats = LocyStats {
            total_iterations: iteration_counts_slot
                .read()
                .map(|c| c.values().sum::<usize>())
                .unwrap_or(0),
            peak_memory_bytes: peak_memory_slot.read().map(|v| *v).unwrap_or(0),
            ..LocyStats::default()
        };
        let approx_for_explain = approximate_slot
            .read()
            .map(|a| a.clone())
            .unwrap_or_default();
        // Collect inline results (QUERY, Cypher) already executed by run_program()
        let inline_map: HashMap<usize, CommandResult> =
            command_results_slot.write().unwrap().drain(..).collect();

        let mut command_results = Vec::new();
        let mut collected_derives: Vec<CollectedDeriveOutput> = Vec::new();
        for (cmd_idx, cmd) in compiled.commands.iter().enumerate() {
            if let Some(result) = inline_map.get(&cmd_idx) {
                // Already executed inline by run_program
                command_results.push(result.clone());
                continue;
            }
            let result = dispatch_native_command(
                cmd,
                &compiled,
                &native_ctx,
                config,
                &mut orch_store,
                &mut locy_stats,
                tracker.clone(),
                start,
                &approx_for_explain,
                self.collect_derive,
                &mut collected_derives,
            )
            .await
            .map_err(map_runtime_error)?;
            command_results.push(result);
        }

        let evaluation_time = start.elapsed();

        // 9. Build derived map, enrich VID columns with full nodes
        let mut base_derived: HashMap<String, Vec<FactRow>> = native_store
            .rule_names()
            .filter_map(|name| {
                native_store
                    .get(name)
                    .map(|batches| (name.to_string(), record_batches_to_locy_rows(batches)))
            })
            .collect();

        // Stamp _approximate on facts in rules that had BDD fallback groups.
        let approximate_groups = approximate_slot
            .read()
            .map(|a| a.clone())
            .unwrap_or_default();
        for (rule_name, groups) in &approximate_groups {
            if !groups.is_empty()
                && let Some(rows) = base_derived.get_mut(rule_name)
            {
                for row in rows.iter_mut() {
                    row.insert("_approximate".to_string(), Value::Bool(true));
                }
            }
        }

        let enriched_derived = enrich_vids_with_nodes(
            self.db,
            &native_store,
            base_derived,
            planner.graph_ctx(),
            planner.session_ctx(),
        )
        .await;

        // 10. Build DerivedFactSet from collected derives (session path only)
        let derived_fact_set = if !collected_derives.is_empty() {
            let mut all_vertices = HashMap::new();
            let mut all_edges = Vec::new();
            let mut all_queries = Vec::new();
            for output in collected_derives {
                for (label, verts) in output.vertices {
                    all_vertices
                        .entry(label)
                        .or_insert_with(Vec::new)
                        .extend(verts);
                }
                all_edges.extend(output.edges);
                all_queries.extend(output.queries);
            }
            Some(DerivedFactSet {
                vertices: all_vertices,
                edges: all_edges,
                stats: locy_stats.clone(),
                evaluated_at_version,
                mutation_queries: all_queries,
            })
        } else {
            None
        };

        // 11. Build final LocyResult
        let warnings = warnings_slot.read().map(|w| w.clone()).unwrap_or_default();
        Ok(build_locy_result(
            enriched_derived,
            command_results,
            &compiled,
            evaluation_time,
            locy_stats,
            warnings,
            approximate_groups,
            derived_fact_set,
        ))
    }

    /// Run only the fixpoint strata (no commands) via the native DataFusion path.
    ///
    /// Used by `re_evaluate_strata()` so that savepoint-scoped mutations from
    /// ASSUME/ABDUCE hypothetical states are visible — the `Executor` is configured
    /// with `self.db.writer`, which holds the active transaction handle.
    async fn run_strata_native(
        &self,
        compiled: &CompiledProgram,
        config: &LocyConfig,
    ) -> Result<uni_query::query::df_graph::DerivedStore> {
        let schema = self.db.schema.schema();
        let query_planner = uni_query::QueryPlanner::new(schema);
        let plan_builder = uni_query::query::locy_planner::LocyPlanBuilder::new(&query_planner);
        let logical = plan_builder
            .build_program_plan(
                compiled,
                config.max_iterations,
                config.timeout,
                config.max_derived_bytes,
                config.deterministic_best_by,
                config.strict_probability_domain,
                config.probability_epsilon,
                config.exact_probability,
                config.max_bdd_variables,
                config.top_k_proofs,
            )
            .map_err(|e| UniError::Query {
                message: format!("LocyPlanBuildError: {e}"),
                query: None,
            })?;

        let mut df_executor = uni_query::Executor::new(self.db.storage.clone());
        df_executor.set_config(self.db.config.clone());
        if let Some(ref w) = self.db.writer {
            df_executor.set_writer(w.clone());
        }
        df_executor.set_xervo_runtime(self.db.xervo_runtime.clone());
        df_executor.set_procedure_registry(self.db.procedure_registry.clone());

        let (session_ctx, planner, _) = df_executor
            .create_datafusion_planner(&self.db.properties, &HashMap::new())
            .await
            .map_err(map_native_df_error)?;
        let exec_plan = planner.plan(&logical).map_err(map_native_df_error)?;

        let derived_store_slot = if let Some(program_exec) =
            exec_plan
                .as_any()
                .downcast_ref::<uni_query::query::df_graph::LocyProgramExec>()
        {
            program_exec.derived_store_slot()
        } else {
            Arc::new(std::sync::RwLock::new(None))
        };

        let _ = uni_query::Executor::collect_batches(&session_ctx, exec_plan)
            .await
            .map_err(map_native_df_error)?;
        Ok(derived_store_slot
            .write()
            .unwrap()
            .take()
            .unwrap_or_default())
    }
}

// ── NativeExecutionAdapter — implements DerivedFactSource + LocyExecutionContext ─

struct NativeExecutionAdapter<'a> {
    db: &'a crate::api::UniInner,
    native_store: &'a uni_query::query::df_graph::DerivedStore,
    compiled: &'a CompiledProgram,
    /// Execution contexts from the fixpoint planner for columnar query execution.
    graph_ctx: Arc<uni_query::query::df_graph::GraphExecutionContext>,
    session_ctx: Arc<parking_lot::RwLock<datafusion::prelude::SessionContext>>,
    /// Query parameters threaded from LocyConfig; passed to execute_subplan so
    /// that $param references in rule MATCH WHERE clauses are resolved.
    params: HashMap<String, Value>,
    /// Private transaction L0 override for commit-time serialization.
    tx_l0_override: Option<Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
    /// Set to true during savepoint execution to bypass tx_l0_override.
    /// Savepoints use writer.transaction_l0 directly for hypothetical mutations.
    savepoint_active: std::sync::atomic::AtomicBool,
}

impl<'a> NativeExecutionAdapter<'a> {
    fn new(
        db: &'a crate::api::UniInner,
        native_store: &'a uni_query::query::df_graph::DerivedStore,
        compiled: &'a CompiledProgram,
        graph_ctx: Arc<uni_query::query::df_graph::GraphExecutionContext>,
        session_ctx: Arc<parking_lot::RwLock<datafusion::prelude::SessionContext>>,
        params: HashMap<String, Value>,
        tx_l0_override: Option<Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
    ) -> Self {
        Self {
            db,
            native_store,
            compiled,
            graph_ctx,
            session_ctx,
            params,
            tx_l0_override,
            savepoint_active: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Execute a Query AST via execute_subplan, reusing the fixpoint contexts.
    async fn execute_query_ast(
        &self,
        ast: Query,
    ) -> std::result::Result<Vec<RecordBatch>, LocyError> {
        let schema = self.db.schema.schema();
        let logical_plan =
            QueryPlanner::new(schema)
                .plan(ast)
                .map_err(|e| LocyError::ExecutorError {
                    message: e.to_string(),
                })?;
        uni_query::query::df_graph::common::execute_subplan(
            &logical_plan,
            &self.params,
            &HashMap::new(),
            &self.graph_ctx,
            &self.session_ctx,
            &self.db.storage,
            &self.db.schema.schema(),
        )
        .await
        .map_err(|e| LocyError::ExecutorError {
            message: e.to_string(),
        })
    }
}

#[async_trait(?Send)]
impl DerivedFactSource for NativeExecutionAdapter<'_> {
    fn lookup_derived(&self, rule_name: &str) -> std::result::Result<Vec<FactRow>, LocyError> {
        let batches = self
            .native_store
            .get(rule_name)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        Ok(record_batches_to_locy_rows(batches))
    }

    fn lookup_derived_batches(
        &self,
        rule_name: &str,
    ) -> std::result::Result<Vec<RecordBatch>, LocyError> {
        Ok(self
            .native_store
            .get(rule_name)
            .map(|v| v.to_vec())
            .unwrap_or_default())
    }

    async fn execute_pattern(
        &self,
        pattern: &Pattern,
        where_conditions: &[Expr],
    ) -> std::result::Result<Vec<RecordBatch>, LocyError> {
        let query = build_match_return_query(pattern, where_conditions);
        let schema = self.db.schema.schema();
        let logical_plan =
            QueryPlanner::new(schema)
                .plan(query)
                .map_err(|e| LocyError::ExecutorError {
                    message: e.to_string(),
                })?;

        // When a transaction L0 override or savepoint transaction is active, the stored
        // graph_ctx may not include the transaction-local L0 buffer. Rebuild a temporary
        // context that includes it so pattern queries see the uncommitted state.
        let tx_l0_for_ctx = self.tx_l0_override.clone().or_else(|| {
            self.db.writer.as_ref().and_then(|w| {
                w.try_read()
                    .ok()
                    .and_then(|writer| writer.transaction_l0.clone())
            })
        });
        let transaction_ctx: Option<Arc<uni_query::query::df_graph::GraphExecutionContext>> =
            if let Some(tx_l0) = tx_l0_for_ctx {
                if let Some(writer_arc) = &self.db.writer {
                    if let Ok(writer) = writer_arc.try_read() {
                        let l0_ctx = uni_query::query::df_graph::L0Context {
                            current_l0: Some(writer.l0_manager.get_current()),
                            transaction_l0: Some(tx_l0),
                            pending_flush_l0s: writer.l0_manager.get_pending_flush(),
                        };
                        Some(Arc::new(
                            uni_query::query::df_graph::GraphExecutionContext::with_l0_context(
                                self.db.storage.clone(),
                                l0_ctx,
                                self.graph_ctx.property_manager().clone(),
                            ),
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };

        let effective_ctx = transaction_ctx.as_ref().unwrap_or(&self.graph_ctx);

        // Use the fixpoint planner's execution contexts directly via execute_subplan.
        uni_query::query::df_graph::common::execute_subplan(
            &logical_plan,
            &self.params,
            &HashMap::new(),
            effective_ctx,
            &self.session_ctx,
            &self.db.storage,
            &self.db.schema.schema(),
        )
        .await
        .map_err(|e| LocyError::ExecutorError {
            message: e.to_string(),
        })
    }
}

#[async_trait(?Send)]
impl LocyExecutionContext for NativeExecutionAdapter<'_> {
    async fn lookup_derived_enriched(
        &self,
        rule_name: &str,
    ) -> std::result::Result<Vec<FactRow>, LocyError> {
        use arrow_schema::DataType;

        if let Some(rule) = self.compiled.rule_catalog.get(rule_name) {
            let is_derive_rule = rule
                .clauses
                .iter()
                .all(|c| matches!(c.output, RuleOutput::Derive(_)));
            if is_derive_rule {
                let mut all_rows = Vec::new();
                for clause in &rule.clauses {
                    let cypher_conds = extract_cypher_conditions(&clause.where_conditions);
                    let raw_batches = self
                        .execute_pattern(&clause.match_pattern, &cypher_conds)
                        .await?;
                    all_rows.extend(record_batches_to_locy_rows(&raw_batches));
                }
                return Ok(all_rows);
            }
        }

        let batches = self
            .native_store
            .get(rule_name)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let rows = record_batches_to_locy_rows(batches);

        let vid_columns: HashSet<String> = batches
            .first()
            .map(|batch| {
                batch
                    .schema()
                    .fields()
                    .iter()
                    .filter(|f| *f.data_type() == DataType::UInt64)
                    .map(|f| f.name().clone())
                    .collect()
            })
            .unwrap_or_default();

        if vid_columns.is_empty() {
            return Ok(rows);
        }

        let unique_vids: HashSet<i64> = rows
            .iter()
            .flat_map(|row| {
                vid_columns.iter().filter_map(|col| {
                    if let Some(Value::Int(vid)) = row.get(col) {
                        Some(*vid)
                    } else {
                        None
                    }
                })
            })
            .collect();

        if unique_vids.is_empty() {
            return Ok(rows);
        }

        let vids_literal = unique_vids
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let query_str =
            format!("MATCH (n) WHERE id(n) IN [{vids_literal}] RETURN id(n) AS _vid, n");
        let mut vid_to_node: HashMap<i64, Value> = HashMap::new();
        if let Ok(ast) = uni_cypher::parse(&query_str)
            && let Ok(batches) = self.execute_query_ast(ast).await
        {
            for row in record_batches_to_locy_rows(&batches) {
                if let (Some(Value::Int(vid)), Some(node)) = (row.get("_vid"), row.get("n")) {
                    vid_to_node.insert(*vid, node.clone());
                }
            }
        }

        Ok(rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|(k, v)| {
                        if vid_columns.contains(&k)
                            && let Value::Int(vid) = &v
                        {
                            let new_v = vid_to_node.get(vid).cloned().unwrap_or(v);
                            return (k, new_v);
                        }
                        (k, v)
                    })
                    .collect()
            })
            .collect())
    }

    async fn execute_cypher_read(
        &self,
        ast: Query,
    ) -> std::result::Result<Vec<FactRow>, LocyError> {
        // Must use execute_ast_internal (fresh SessionContext) so that savepoint
        // mutations applied during ASSUME/ABDUCE body dispatch are visible.
        let result = self
            .db
            .execute_ast_internal(ast, "<locy>", HashMap::new(), self.db.config.clone())
            .await
            .map_err(|e| LocyError::ExecutorError {
                message: e.to_string(),
            })?;
        Ok(result
            .into_rows()
            .into_iter()
            .map(|row| {
                let cols: Vec<String> = row.columns().to_vec();
                cols.into_iter().zip(row.into_values()).collect()
            })
            .collect())
    }

    async fn execute_mutation(
        &self,
        ast: Query,
        params: HashMap<String, Value>,
    ) -> std::result::Result<usize, LocyError> {
        // When a savepoint is active, bypass tx_l0_override so mutations go to
        // the writer's savepoint L0 (hypothetical buffer that can be rolled back).
        let use_override = !self
            .savepoint_active
            .load(std::sync::atomic::Ordering::Relaxed);
        if use_override && let Some(ref tx_l0) = self.tx_l0_override {
            let before = tx_l0.read().mutation_count;
            self.db
                .execute_ast_internal_with_tx_l0(
                    ast,
                    "<locy>",
                    params,
                    self.db.config.clone(),
                    tx_l0.clone(),
                )
                .await
                .map_err(|e| LocyError::ExecutorError {
                    message: e.to_string(),
                })?;
            let after = tx_l0.read().mutation_count;
            return Ok(after.saturating_sub(before));
        }
        // Standard path: mutations go through writer.active_l0()
        let before = self.db.get_mutation_count().await;
        self.db
            .execute_ast_internal(ast, "<locy>", params, self.db.config.clone())
            .await
            .map_err(|e| LocyError::ExecutorError {
                message: e.to_string(),
            })?;
        let after = self.db.get_mutation_count().await;
        Ok(after.saturating_sub(before))
    }

    async fn begin_savepoint(&self) -> std::result::Result<SavepointId, LocyError> {
        let writer = self
            .db
            .writer
            .as_ref()
            .ok_or_else(|| LocyError::SavepointFailed {
                message: "database is read-only".to_string(),
            })?;
        let mut w = writer.write().await;
        w.begin_transaction()
            .map_err(|e| LocyError::SavepointFailed {
                message: e.to_string(),
            })?;
        // While savepoint is active, bypass tx_l0_override so mutations go to
        // the writer's savepoint L0 rather than the outer transaction's L0.
        self.savepoint_active
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(SavepointId(0))
    }

    async fn rollback_savepoint(&self, _id: SavepointId) -> std::result::Result<(), LocyError> {
        let writer = self
            .db
            .writer
            .as_ref()
            .ok_or_else(|| LocyError::SavepointFailed {
                message: "database is read-only".to_string(),
            })?;
        let mut w = writer.write().await;
        w.rollback_transaction()
            .map_err(|e| LocyError::SavepointFailed {
                message: e.to_string(),
            })?;
        self.savepoint_active
            .store(false, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn re_evaluate_strata(
        &self,
        program: &CompiledProgram,
        config: &LocyConfig,
    ) -> std::result::Result<RowStore, LocyError> {
        let strata_only = CompiledProgram {
            strata: program.strata.clone(),
            rule_catalog: program.rule_catalog.clone(),
            warnings: vec![],
            commands: vec![],
        };
        let engine = LocyEngine {
            db: self.db,
            tx_l0_override: None,
            collect_derive: false,
        };
        let native_store = engine
            .run_strata_native(&strata_only, config)
            .await
            .map_err(|e| LocyError::ExecutorError {
                message: e.to_string(),
            })?;
        Ok(native_store_to_row_store(&native_store, program))
    }
}

// ── Native command dispatch ───────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn dispatch_native_command<'a>(
    cmd: &'a CompiledCommand,
    program: &'a CompiledProgram,
    ctx: &'a NativeExecutionAdapter<'a>,
    config: &'a LocyConfig,
    orch_store: &'a mut RowStore,
    stats: &'a mut LocyStats,
    tracker: Option<Arc<ProvenanceStore>>,
    start: Instant,
    approximate_groups: &'a HashMap<String, Vec<String>>,
    collect_derive: bool,
    collected_derives: &'a mut Vec<CollectedDeriveOutput>,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = std::result::Result<CommandResult, LocyError>> + 'a>,
> {
    Box::pin(async move {
        match cmd {
            CompiledCommand::GoalQuery(gq) => {
                let rows = uni_query::query::df_graph::locy_query::evaluate_query(
                    gq, program, ctx, config, orch_store, stats, start,
                )
                .await?;
                Ok(CommandResult::Query(rows))
            }
            CompiledCommand::ExplainRule(eq) => {
                let node = uni_query::query::df_graph::locy_explain::explain_rule(
                    eq,
                    program,
                    ctx,
                    config,
                    orch_store,
                    stats,
                    tracker.as_deref(),
                    Some(approximate_groups),
                )
                .await?;
                Ok(CommandResult::Explain(node))
            }
            CompiledCommand::Assume(ca) => {
                let rows = uni_query::query::df_graph::locy_assume::evaluate_assume(
                    ca, program, ctx, config, stats,
                )
                .await?;
                Ok(CommandResult::Assume(rows))
            }
            CompiledCommand::Abduce(aq) => {
                let result = uni_query::query::df_graph::locy_abduce::evaluate_abduce(
                    aq,
                    program,
                    ctx,
                    config,
                    orch_store,
                    stats,
                    tracker.as_deref(),
                )
                .await?;
                Ok(CommandResult::Abduce(result))
            }
            CompiledCommand::DeriveCommand(dc) => {
                if collect_derive {
                    // Session path: collect ASTs + data for deferred materialization
                    let output = uni_query::query::df_graph::locy_derive::collect_derive_facts(
                        dc, program, ctx,
                    )
                    .await?;
                    let affected = output.affected;
                    collected_derives.push(output);
                    Ok(CommandResult::Derive { affected })
                } else {
                    // Transaction path: auto-apply mutations
                    let affected = uni_query::query::df_graph::locy_derive::derive_command(
                        dc, program, ctx, stats,
                    )
                    .await?;
                    Ok(CommandResult::Derive { affected })
                }
            }
            CompiledCommand::Cypher(q) => {
                let rows = ctx.execute_cypher_read(q.clone()).await?;
                stats.queries_executed += 1;
                Ok(CommandResult::Cypher(rows))
            }
        }
    })
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async fn enrich_vids_with_nodes(
    db: &crate::api::UniInner,
    native_store: &uni_query::query::df_graph::DerivedStore,
    derived: HashMap<String, Vec<FactRow>>,
    graph_ctx: &Arc<uni_query::query::df_graph::GraphExecutionContext>,
    session_ctx: &Arc<parking_lot::RwLock<datafusion::prelude::SessionContext>>,
) -> HashMap<String, Vec<FactRow>> {
    use arrow_schema::DataType;
    let mut enriched = HashMap::new();

    for (name, rows) in derived {
        let vid_columns: HashSet<String> = native_store
            .get(&name)
            .and_then(|batches| batches.first())
            .map(|batch| {
                batch
                    .schema()
                    .fields()
                    .iter()
                    .filter(|f| *f.data_type() == DataType::UInt64)
                    .map(|f| f.name().clone())
                    .collect()
            })
            .unwrap_or_default();

        if vid_columns.is_empty() {
            enriched.insert(name, rows);
            continue;
        }

        let unique_vids: HashSet<i64> = rows
            .iter()
            .flat_map(|row| {
                vid_columns.iter().filter_map(|col| {
                    if let Some(Value::Int(vid)) = row.get(col) {
                        Some(*vid)
                    } else {
                        None
                    }
                })
            })
            .collect();

        if unique_vids.is_empty() {
            enriched.insert(name, rows);
            continue;
        }

        let vids_literal = unique_vids
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let query_str = format!(
            "MATCH (n) WHERE id(n) IN [{}] RETURN id(n) AS _vid, n",
            vids_literal
        );
        let mut vid_to_node: HashMap<i64, Value> = HashMap::new();
        if let Ok(ast) = uni_cypher::parse(&query_str) {
            let schema = db.schema.schema();
            if let Ok(logical_plan) = uni_query::QueryPlanner::new(schema).plan(ast)
                && let Ok(batches) = uni_query::query::df_graph::common::execute_subplan(
                    &logical_plan,
                    &HashMap::new(),
                    &HashMap::new(),
                    graph_ctx,
                    session_ctx,
                    &db.storage,
                    &db.schema.schema(),
                )
                .await
            {
                for row in record_batches_to_locy_rows(&batches) {
                    if let (Some(Value::Int(vid)), Some(node)) = (row.get("_vid"), row.get("n")) {
                        vid_to_node.insert(*vid, node.clone());
                    }
                }
            }
        }

        let enriched_rows: Vec<FactRow> = rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|(k, v)| {
                        if vid_columns.contains(&k)
                            && let Value::Int(vid) = &v
                        {
                            let new_v = vid_to_node.get(vid).cloned().unwrap_or(v);
                            return (k, new_v);
                        }
                        (k, v)
                    })
                    .collect()
            })
            .collect();
        enriched.insert(name, enriched_rows);
    }

    enriched
}

#[allow(clippy::too_many_arguments)]
fn build_locy_result(
    derived: HashMap<String, Vec<FactRow>>,
    command_results: Vec<CommandResult>,
    compiled: &CompiledProgram,
    evaluation_time: Duration,
    mut orchestrator_stats: LocyStats,
    warnings: Vec<RuntimeWarning>,
    approximate_groups: HashMap<String, Vec<String>>,
    derived_fact_set: Option<DerivedFactSet>,
) -> LocyResult {
    let total_facts: usize = derived.values().map(|v| v.len()).sum();
    orchestrator_stats.strata_evaluated = compiled.strata.len();
    orchestrator_stats.derived_nodes = total_facts;
    orchestrator_stats.evaluation_time = evaluation_time;

    let inner = uni_locy::LocyResult {
        derived,
        stats: orchestrator_stats,
        command_results,
        warnings,
        approximate_groups,
        derived_fact_set,
    };
    let metrics = QueryMetrics {
        total_time: evaluation_time,
        exec_time: evaluation_time,
        rows_returned: total_facts,
        ..Default::default()
    };
    LocyResult::new(inner, metrics)
}

fn native_store_to_row_store(
    native: &uni_query::query::df_graph::DerivedStore,
    compiled: &CompiledProgram,
) -> RowStore {
    let mut result = RowStore::new();
    for name in native.rule_names() {
        if let Some(batches) = native.get(name) {
            let rows = record_batches_to_locy_rows(batches);
            let rule = compiled.rule_catalog.get(name);
            let columns: Vec<String> = rule
                .map(|r| r.yield_schema.iter().map(|yc| yc.name.clone()).collect())
                .unwrap_or_else(|| {
                    rows.first()
                        .map(|r| r.keys().cloned().collect())
                        .unwrap_or_default()
                });
            result.insert(name.to_string(), RowRelation::new(columns, rows));
        }
    }
    result
}

// ── Error mapping ──────────────────────────────────────────────────────────

fn map_parse_error(e: uni_cypher::ParseError) -> UniError {
    UniError::Parse {
        message: format!("LocyParseError: {e}"),
        position: None,
        line: None,
        column: None,
        context: None,
    }
}

fn map_compile_error(e: LocyCompileError) -> UniError {
    UniError::Query {
        message: format!("LocyCompileError: {e}"),
        query: None,
    }
}

fn map_runtime_error(e: LocyError) -> UniError {
    match e {
        LocyError::SavepointFailed { ref message } => UniError::Transaction {
            message: format!("LocyRuntimeError: {message}"),
        },
        other => UniError::Query {
            message: format!("LocyRuntimeError: {other}"),
            query: None,
        },
    }
}

fn map_native_df_error(e: impl std::fmt::Display) -> UniError {
    UniError::Query {
        message: format!("LocyRuntimeError: {e}"),
        query: None,
    }
}
