// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use uni_common::{Result, UniConfig, UniError};
use uni_query::{
    ExplainOutput, LogicalPlan, ProfileOutput, QueryCursor, QueryMetrics, QueryResult,
    ResultNormalizer, Row, Value as ApiValue,
};

/// Normalize backend/planner error text into canonical Cypher/TCK codes.
///
/// This keeps behavioral semantics unchanged while making error classification
/// stable across planner backends.
fn normalize_error_message(raw: &str, cypher: &str) -> String {
    let mut normalized = raw.to_string();
    let cypher_upper = cypher.to_uppercase();
    let cypher_lower = cypher.to_lowercase();

    if raw.contains("Error during planning: UDF") && raw.contains("is not registered") {
        normalized = format!("SyntaxError: UnknownFunction - {}", raw);
    } else if raw.contains("_cypher_in(): second argument must be a list") {
        normalized = format!("TypeError: InvalidArgumentType - {}", raw);
    } else if raw.contains("InvalidNumberOfArguments: Procedure") && raw.contains("got 0") {
        if cypher_upper.contains("YIELD") {
            normalized = format!("SyntaxError: InvalidArgumentPassingMode - {}", raw);
        } else {
            normalized = format!("ParameterMissing: MissingParameter - {}", raw);
        }
    } else if raw.contains("Function count not implemented or is aggregate")
        || raw.contains("Physical plan does not support logical expression AggregateFunction")
        || raw.contains("Expected aggregate function, got: ListComprehension")
    {
        normalized = format!("SyntaxError: InvalidAggregation - {}", raw);
    } else if raw.contains("Expected aggregate function, got: BinaryOp") {
        normalized = format!("SyntaxError: AmbiguousAggregationExpression - {}", raw);
    } else if raw.contains("Schema error: No field named \"me.age\". Valid fields are \"count(you.age)\".")
    {
        normalized = format!("SyntaxError: UndefinedVariable - {}", raw);
    } else if raw.contains(
        "Schema error: No field named \"me.age\". Valid fields are \"me.age + you.age\", \"count(*)\".",
    ) {
        normalized = format!("SyntaxError: AmbiguousAggregationExpression - {}", raw);
    } else if raw.contains("MERGE edge must have a type")
        || raw.contains("MERGE does not support multiple edge types")
    {
        normalized = format!("SyntaxError: NoSingleRelationshipType - {}", raw);
    } else if raw.contains("MERGE node must have a label") {
        if cypher.contains("$param") {
            normalized = format!("SyntaxError: InvalidParameterUse - {}", raw);
        } else if cypher.contains('*') && cypher.contains("-[:") {
            normalized = format!("SyntaxError: CreatingVarLength - {}", raw);
        } else if cypher_lower.contains("on create set x.")
            || cypher_lower.contains("on match set x.")
        {
            normalized = format!("SyntaxError: UndefinedVariable - {}", raw);
        }
    }

    normalized
}

/// Convert a parse error into `UniError::Parse`.
pub(crate) fn into_parse_error(e: impl std::fmt::Display) -> UniError {
    UniError::Parse {
        message: e.to_string(),
        position: None,
        line: None,
        column: None,
        context: None,
    }
}

/// Convert a planner/compile-time error into the appropriate `UniError` type.
///
/// Errors starting with "SyntaxError:" are treated as parse/syntax errors.
/// All other errors are query/semantic errors (CompileTime).
pub(crate) fn into_query_error(e: impl std::fmt::Display, cypher: &str) -> UniError {
    let msg = normalize_error_message(&e.to_string(), cypher);
    // Errors containing "SyntaxError:" prefix should be treated as syntax errors
    // This covers validation errors like VariableTypeConflict, UndefinedVariable, etc.
    if msg.starts_with("SyntaxError:") {
        UniError::Parse {
            message: msg,
            position: None,
            line: None,
            column: None,
            context: Some(cypher.to_string()),
        }
    } else {
        UniError::Query {
            message: msg,
            query: Some(cypher.to_string()),
        }
    }
}

/// Convert an executor/runtime error into the appropriate `UniError` type.
/// TypeError messages from UDF execution become `UniError::Type` (Runtime phase).
/// ConstraintVerificationFailed messages become `UniError::Constraint` (Runtime phase).
/// All other executor errors remain `UniError::Query`.
fn into_execution_error(e: impl std::fmt::Display, cypher: &str) -> UniError {
    let msg = normalize_error_message(&e.to_string(), cypher);
    if msg.contains("Query cancelled") {
        UniError::Cancelled
    } else if msg.contains("TypeError:") {
        UniError::Type {
            expected: msg,
            actual: String::new(),
        }
    } else if msg.starts_with("ConstraintVerificationFailed:") {
        UniError::Constraint { message: msg }
    } else {
        UniError::Query {
            message: msg,
            query: Some(cypher.to_string()),
        }
    }
}

/// Extract projection column names from a LogicalPlan, preserving query order.
/// Returns None if the plan doesn't have projections at the top level.
fn extract_projection_order(plan: &LogicalPlan) -> Option<Vec<String>> {
    match plan {
        LogicalPlan::Project { projections, .. } => Some(
            projections
                .iter()
                .map(|(expr, alias)| alias.clone().unwrap_or_else(|| expr.to_string_repr()))
                .collect(),
        ),
        LogicalPlan::Aggregate {
            group_by,
            aggregates,
            ..
        } => {
            let mut names: Vec<String> = group_by.iter().map(|e| e.to_string_repr()).collect();
            names.extend(aggregates.iter().map(|e| e.to_string_repr()));
            Some(names)
        }
        LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Filter { input, .. } => extract_projection_order(input),
        _ => None,
    }
}

impl crate::api::UniInner {
    /// Get the current L0Buffer mutation count (cumulative mutations since last flush).
    /// Used to compute affected_rows for mutation queries that return no result rows.
    pub(crate) async fn get_mutation_count(&self) -> usize {
        match self.writer.as_ref() {
            Some(w) => {
                let writer = w.read().await;
                writer.l0_manager.get_current().read().mutation_count
            }
            None => 0,
        }
    }

    /// Get the current L0Buffer mutation stats snapshot.
    /// Used together with `get_mutation_count` to compute per-type affected counters.
    pub(crate) async fn get_mutation_stats(&self) -> uni_store::runtime::l0::MutationStats {
        match self.writer.as_ref() {
            Some(w) => {
                let writer = w.read().await;
                writer
                    .l0_manager
                    .get_current()
                    .read()
                    .mutation_stats
                    .clone()
            }
            None => uni_store::runtime::l0::MutationStats::default(),
        }
    }

    /// Explain a Cypher query plan without executing it.
    pub(crate) async fn explain_internal(&self, cypher: &str) -> Result<ExplainOutput> {
        let ast = uni_cypher::parse(cypher).map_err(into_parse_error)?;

        let planner = uni_query::QueryPlanner::new(self.schema.schema().clone());
        planner
            .explain_plan(ast)
            .map_err(|e| into_query_error(e, cypher))
    }

    /// Profile a Cypher query execution.
    pub(crate) async fn profile_internal(
        &self,
        cypher: &str,
    ) -> Result<(QueryResult, ProfileOutput)> {
        let ast = uni_cypher::parse(cypher).map_err(into_parse_error)?;

        let planner = uni_query::QueryPlanner::new(self.schema.schema().clone());
        let logical_plan = planner.plan(ast).map_err(|e| into_query_error(e, cypher))?;

        let mut executor = uni_query::Executor::new(self.storage.clone());
        executor.set_config(self.config.clone());
        executor.set_xervo_runtime(self.xervo_runtime.clone());
        executor.set_procedure_registry(self.procedure_registry.clone());
        if let Ok(reg) = self.custom_functions.read()
            && !reg.is_empty()
        {
            executor.set_custom_functions(Arc::new(reg.clone()));
        }
        if let Some(w) = &self.writer {
            executor.set_writer(w.clone());
        }

        let params: HashMap<String, uni_common::Value> = HashMap::new(); // TODO: Support params in profile

        // Extract projection order
        let projection_order = extract_projection_order(&logical_plan);

        let (results, profile_output) = executor
            .profile(logical_plan, &params)
            .await
            .map_err(|e| into_execution_error(e, cypher))?;

        // Convert results to QueryResult
        let columns = if results.is_empty() {
            Arc::new(vec![])
        } else if let Some(order) = projection_order {
            Arc::new(order)
        } else {
            let mut cols: Vec<String> = results[0].keys().cloned().collect();
            cols.sort();
            Arc::new(cols)
        };

        let rows = results
            .into_iter()
            .map(|map| {
                let mut values = Vec::with_capacity(columns.len());
                for col in columns.iter() {
                    let value = map.get(col).cloned().unwrap_or(ApiValue::Null);
                    // Normalize to ensure proper Node/Edge/Path types
                    let normalized =
                        ResultNormalizer::normalize_value(value).unwrap_or(ApiValue::Null);
                    values.push(normalized);
                }
                Row::new(columns.clone(), values)
            })
            .collect();

        Ok((
            QueryResult::new(columns, rows, Vec::new(), Default::default()),
            profile_output,
        ))
    }

    pub(crate) async fn execute_cursor_internal(
        &self,
        cypher: &str,
        params: HashMap<String, ApiValue>,
    ) -> Result<QueryCursor> {
        self.execute_cursor_internal_with_config(cypher, params, self.config.clone())
            .await
    }

    pub(crate) async fn execute_cursor_internal_with_config(
        &self,
        cypher: &str,
        params: HashMap<String, ApiValue>,
        config: UniConfig,
    ) -> Result<QueryCursor> {
        let ast = uni_cypher::parse(cypher).map_err(into_parse_error)?;

        let planner =
            uni_query::QueryPlanner::new(self.schema.schema().clone()).with_params(params.clone());
        let logical_plan = planner.plan(ast).map_err(|e| into_query_error(e, cypher))?;

        let mut executor = uni_query::Executor::new(self.storage.clone());
        executor.set_config(config.clone());
        executor.set_xervo_runtime(self.xervo_runtime.clone());
        executor.set_procedure_registry(self.procedure_registry.clone());
        if let Ok(reg) = self.custom_functions.read()
            && !reg.is_empty()
        {
            executor.set_custom_functions(Arc::new(reg.clone()));
        }
        if let Some(w) = &self.writer {
            executor.set_writer(w.clone());
        }

        let projection_order = extract_projection_order(&logical_plan);
        let projection_order_for_rows = projection_order.clone();
        let cypher_for_error = cypher.to_string();

        let stream = executor.execute_stream(logical_plan, self.properties.clone(), params);

        let row_stream = stream.map(move |batch_res| {
            let results = batch_res.map_err(|e| {
                let msg = normalize_error_message(&e.to_string(), &cypher_for_error);
                if msg.contains("TypeError:") {
                    UniError::Type {
                        expected: msg,
                        actual: String::new(),
                    }
                } else if msg.starts_with("ConstraintVerificationFailed:") {
                    UniError::Constraint { message: msg }
                } else {
                    UniError::Query {
                        message: msg,
                        query: Some(cypher_for_error.clone()),
                    }
                }
            })?;

            if results.is_empty() {
                return Ok(vec![]);
            }

            // Determine columns for this batch (should be stable for the whole query)
            let columns = if let Some(order) = &projection_order_for_rows {
                Arc::new(order.clone())
            } else {
                let mut cols: Vec<String> = results[0].keys().cloned().collect();
                cols.sort();
                Arc::new(cols)
            };

            let rows = results
                .into_iter()
                .map(|map| {
                    let mut values = Vec::with_capacity(columns.len());
                    for col in columns.iter() {
                        let value = map.get(col).cloned().unwrap_or(ApiValue::Null);
                        values.push(value);
                    }
                    Row::new(columns.clone(), values)
                })
                .collect();

            Ok(rows)
        });

        // We need columns ahead of time for QueryCursor if possible.
        let columns = if let Some(order) = projection_order {
            Arc::new(order)
        } else {
            Arc::new(vec![])
        };

        Ok(QueryCursor::new(columns, Box::pin(row_stream)))
    }

    pub(crate) async fn execute_internal(
        &self,
        cypher: &str,
        params: HashMap<String, ApiValue>,
    ) -> Result<QueryResult> {
        self.execute_internal_with_config(cypher, params, self.config.clone())
            .await
    }

    /// Execute a Cypher query with a private transaction L0 buffer.
    /// The tx_l0 is installed on the executor so both reads and mutations
    /// are routed through the caller's private L0 (commit-time serialization).
    pub(crate) async fn execute_internal_with_tx_l0(
        &self,
        cypher: &str,
        params: HashMap<String, ApiValue>,
        tx_l0: std::sync::Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>,
    ) -> Result<QueryResult> {
        let total_start = Instant::now();

        let parse_start = Instant::now();
        let ast = uni_cypher::parse(cypher).map_err(into_parse_error)?;
        let parse_time = parse_start.elapsed();

        let (ast, tt_spec) = match ast {
            uni_cypher::ast::Query::TimeTravel { query, spec } => (*query, Some(spec)),
            other => (other, None),
        };

        if tt_spec.is_some() {
            return Err(UniError::Query {
                message: "Time-travel queries are not supported within transactions".to_string(),
                query: Some(cypher.to_string()),
            });
        }

        let plan_start = Instant::now();
        let planner =
            uni_query::QueryPlanner::new(self.schema.schema().clone()).with_params(params.clone());
        let logical_plan = planner.plan(ast).map_err(|e| into_query_error(e, cypher))?;
        let plan_time = plan_start.elapsed();

        let mut executor = uni_query::Executor::new(self.storage.clone());
        executor.set_config(self.config.clone());
        executor.set_xervo_runtime(self.xervo_runtime.clone());
        executor.set_procedure_registry(self.procedure_registry.clone());
        if let Ok(reg) = self.custom_functions.read()
            && !reg.is_empty()
        {
            executor.set_custom_functions(Arc::new(reg.clone()));
        }
        if let Some(w) = &self.writer {
            executor.set_writer(w.clone());
        }
        executor.set_transaction_l0(tx_l0);

        let projection_order = extract_projection_order(&logical_plan);

        let exec_start = Instant::now();
        let results = executor
            .execute(logical_plan, &self.properties, &params)
            .await
            .map_err(|e| into_execution_error(e, cypher))?;
        let exec_time = exec_start.elapsed();

        let columns = if results.is_empty() {
            Arc::new(vec![])
        } else if let Some(order) = projection_order {
            Arc::new(order)
        } else {
            let mut cols: Vec<String> = results[0].keys().cloned().collect();
            cols.sort();
            Arc::new(cols)
        };

        let rows: Vec<Row> = results
            .into_iter()
            .map(|map| {
                let mut values = Vec::with_capacity(columns.len());
                for col in columns.iter() {
                    let value = map.get(col).cloned().unwrap_or(ApiValue::Null);
                    let normalized =
                        ResultNormalizer::normalize_value(value).unwrap_or(ApiValue::Null);
                    values.push(normalized);
                }
                Row::new(columns.clone(), values)
            })
            .collect();

        let metrics = QueryMetrics {
            parse_time,
            plan_time,
            exec_time,
            total_time: total_start.elapsed(),
            rows_returned: rows.len(),
            ..Default::default()
        };

        Ok(QueryResult::new(
            columns,
            rows,
            executor.take_warnings(),
            metrics,
        ))
    }

    pub(crate) async fn execute_internal_with_config(
        &self,
        cypher: &str,
        params: HashMap<String, ApiValue>,
        config: UniConfig,
    ) -> Result<QueryResult> {
        let total_start = Instant::now();

        // Single parse: extract time-travel clause if present
        let parse_start = Instant::now();
        let ast = uni_cypher::parse(cypher).map_err(into_parse_error)?;
        let parse_time = parse_start.elapsed();

        let (ast, tt_spec) = match ast {
            uni_cypher::ast::Query::TimeTravel { query, spec } => (*query, Some(spec)),
            other => (other, None),
        };

        if let Some(spec) = tt_spec {
            uni_query::validate_read_only(&ast).map_err(|msg| into_query_error(msg, cypher))?;
            // Resolve to snapshot and execute on pinned instance
            let snapshot_id = self.resolve_time_travel(&spec).await?;
            let pinned = self.at_snapshot(&snapshot_id).await?;
            return pinned
                .execute_ast_internal(ast, cypher, params, config)
                .await;
        }

        let mut result = self
            .execute_ast_internal(ast, cypher, params, config)
            .await?;
        result.update_parse_timing(parse_time, total_start.elapsed());
        Ok(result)
    }

    /// Like `execute_internal_with_config` but also accepts a cancellation token.
    pub(crate) async fn execute_internal_with_config_and_token(
        &self,
        cypher: &str,
        params: HashMap<String, ApiValue>,
        config: UniConfig,
        cancellation_token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<QueryResult> {
        let total_start = Instant::now();

        let parse_start = Instant::now();
        let ast = uni_cypher::parse(cypher).map_err(into_parse_error)?;
        let parse_time = parse_start.elapsed();

        let (ast, tt_spec) = match ast {
            uni_cypher::ast::Query::TimeTravel { query, spec } => (*query, Some(spec)),
            other => (other, None),
        };

        if let Some(spec) = tt_spec {
            uni_query::validate_read_only(&ast).map_err(|msg| into_query_error(msg, cypher))?;
            let snapshot_id = self.resolve_time_travel(&spec).await?;
            let pinned = self.at_snapshot(&snapshot_id).await?;
            return pinned
                .execute_ast_internal(ast, cypher, params, config)
                .await;
        }

        let planner =
            uni_query::QueryPlanner::new(self.schema.schema().clone()).with_params(params.clone());
        let logical_plan = planner.plan(ast).map_err(|e| into_query_error(e, cypher))?;

        let mut result = self
            .execute_plan_internal(logical_plan, cypher, params, config, cancellation_token)
            .await?;
        result.update_parse_timing(parse_time, total_start.elapsed());
        Ok(result)
    }

    /// Execute a pre-parsed Cypher AST with a private transaction L0 override.
    pub(crate) async fn execute_ast_internal_with_tx_l0(
        &self,
        ast: uni_query::CypherQuery,
        cypher: &str,
        params: HashMap<String, ApiValue>,
        config: UniConfig,
        tx_l0: std::sync::Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>,
    ) -> Result<QueryResult> {
        let total_start = Instant::now();

        let plan_start = Instant::now();
        let planner =
            uni_query::QueryPlanner::new(self.schema.schema().clone()).with_params(params.clone());
        let logical_plan = planner.plan(ast).map_err(|e| into_query_error(e, cypher))?;
        let plan_time = plan_start.elapsed();

        let mut executor = uni_query::Executor::new(self.storage.clone());
        executor.set_config(config.clone());
        executor.set_xervo_runtime(self.xervo_runtime.clone());
        executor.set_procedure_registry(self.procedure_registry.clone());
        if let Ok(reg) = self.custom_functions.read()
            && !reg.is_empty()
        {
            executor.set_custom_functions(Arc::new(reg.clone()));
        }
        if let Some(w) = &self.writer {
            executor.set_writer(w.clone());
        }
        executor.set_transaction_l0(tx_l0);

        let projection_order = extract_projection_order(&logical_plan);

        let exec_start = Instant::now();
        let results = executor
            .execute(logical_plan, &self.properties, &params)
            .await
            .map_err(|e| into_execution_error(e, cypher))?;
        let exec_time = exec_start.elapsed();

        let columns = if results.is_empty() {
            Arc::new(vec![])
        } else if let Some(order) = projection_order {
            Arc::new(order)
        } else {
            let mut cols: Vec<String> = results[0].keys().cloned().collect();
            cols.sort();
            Arc::new(cols)
        };

        let rows = results
            .into_iter()
            .map(|map| {
                let mut values = Vec::with_capacity(columns.len());
                for col in columns.iter() {
                    let value = map.get(col).cloned().unwrap_or(ApiValue::Null);
                    let normalized =
                        ResultNormalizer::normalize_value(value).unwrap_or(ApiValue::Null);
                    values.push(normalized);
                }
                Row::new(columns.clone(), values)
            })
            .collect::<Vec<Row>>();

        let metrics = QueryMetrics {
            parse_time: std::time::Duration::ZERO,
            plan_time,
            exec_time,
            total_time: total_start.elapsed(),
            rows_returned: rows.len(),
            ..Default::default()
        };

        Ok(QueryResult::new(
            columns,
            rows,
            executor.take_warnings(),
            metrics,
        ))
    }

    /// Execute a pre-parsed Cypher AST through the planner and executor.
    ///
    /// The `cypher` parameter is the original query string, used only for
    /// error messages.
    pub(crate) async fn execute_ast_internal(
        &self,
        ast: uni_query::CypherQuery,
        cypher: &str,
        params: HashMap<String, ApiValue>,
        config: UniConfig,
    ) -> Result<QueryResult> {
        let total_start = Instant::now();

        let plan_start = Instant::now();
        let planner =
            uni_query::QueryPlanner::new(self.schema.schema().clone()).with_params(params.clone());
        let logical_plan = planner.plan(ast).map_err(|e| into_query_error(e, cypher))?;
        let plan_time = plan_start.elapsed();

        let mut executor = uni_query::Executor::new(self.storage.clone());
        executor.set_config(config.clone());
        executor.set_xervo_runtime(self.xervo_runtime.clone());
        executor.set_procedure_registry(self.procedure_registry.clone());
        if let Ok(reg) = self.custom_functions.read()
            && !reg.is_empty()
        {
            executor.set_custom_functions(Arc::new(reg.clone()));
        }
        if let Some(w) = &self.writer {
            executor.set_writer(w.clone());
        }

        let projection_order = extract_projection_order(&logical_plan);

        let exec_start = Instant::now();
        let results = executor
            .execute(logical_plan, &self.properties, &params)
            .await
            .map_err(|e| into_execution_error(e, cypher))?;
        let exec_time = exec_start.elapsed();

        let columns = if results.is_empty() {
            Arc::new(vec![])
        } else if let Some(order) = projection_order {
            Arc::new(order)
        } else {
            let mut cols: Vec<String> = results[0].keys().cloned().collect();
            cols.sort();
            Arc::new(cols)
        };

        let rows = results
            .into_iter()
            .map(|map| {
                let mut values = Vec::with_capacity(columns.len());
                for col in columns.iter() {
                    let value = map.get(col).cloned().unwrap_or(ApiValue::Null);
                    let normalized =
                        ResultNormalizer::normalize_value(value).unwrap_or(ApiValue::Null);
                    values.push(normalized);
                }
                Row::new(columns.clone(), values)
            })
            .collect::<Vec<Row>>();

        let metrics = QueryMetrics {
            parse_time: std::time::Duration::ZERO,
            plan_time,
            exec_time,
            total_time: total_start.elapsed(),
            rows_returned: rows.len(),
            ..Default::default()
        };

        Ok(QueryResult::new(
            columns,
            rows,
            executor.take_warnings(),
            metrics,
        ))
    }

    /// Resolve a time-travel spec to a snapshot ID.
    async fn resolve_time_travel(&self, spec: &uni_query::TimeTravelSpec) -> Result<String> {
        match spec {
            uni_query::TimeTravelSpec::Version(id) => Ok(id.clone()),
            uni_query::TimeTravelSpec::Timestamp(ts_str) => {
                let ts = chrono::DateTime::parse_from_rfc3339(ts_str)
                    .map_err(|e| {
                        into_parse_error(format!("Invalid timestamp '{}': {}", ts_str, e))
                    })?
                    .with_timezone(&chrono::Utc);
                self.resolve_time_travel_timestamp(ts).await
            }
        }
    }

    /// Resolve a `chrono::DateTime<Utc>` to the snapshot ID of the closest
    /// snapshot at or before that timestamp.
    pub(crate) async fn resolve_time_travel_timestamp(
        &self,
        ts: chrono::DateTime<chrono::Utc>,
    ) -> Result<String> {
        let manifest = self
            .storage
            .snapshot_manager()
            .find_snapshot_at_time(ts)
            .await
            .map_err(UniError::Internal)?
            .ok_or_else(|| UniError::Query {
                message: format!("No snapshot found at or before {}", ts),
                query: None,
            })?;
        Ok(manifest.snapshot_id)
    }

    /// Execute a pre-built logical plan, skipping parse and plan phases.
    ///
    /// Used by the plan cache and prepared statements to re-execute
    /// previously planned queries.
    pub(crate) async fn execute_plan_internal(
        &self,
        plan: uni_query::LogicalPlan,
        cypher: &str,
        params: HashMap<String, ApiValue>,
        config: UniConfig,
        cancellation_token: Option<tokio_util::sync::CancellationToken>,
    ) -> Result<QueryResult> {
        let total_start = Instant::now();

        let mut executor = uni_query::Executor::new(self.storage.clone());
        executor.set_config(config.clone());
        executor.set_xervo_runtime(self.xervo_runtime.clone());
        executor.set_procedure_registry(self.procedure_registry.clone());
        if let Ok(reg) = self.custom_functions.read()
            && !reg.is_empty()
        {
            executor.set_custom_functions(Arc::new(reg.clone()));
        }
        if let Some(w) = &self.writer {
            executor.set_writer(w.clone());
        }
        if let Some(token) = cancellation_token {
            executor.set_cancellation_token(token);
        }

        let projection_order = extract_projection_order(&plan);

        let exec_start = Instant::now();
        let results = executor
            .execute(plan, &self.properties, &params)
            .await
            .map_err(|e| into_execution_error(e, cypher))?;
        let exec_time = exec_start.elapsed();

        let columns = if results.is_empty() {
            Arc::new(vec![])
        } else if let Some(order) = projection_order {
            Arc::new(order)
        } else {
            let mut cols: Vec<String> = results[0].keys().cloned().collect();
            cols.sort();
            Arc::new(cols)
        };

        let rows: Vec<Row> = results
            .into_iter()
            .map(|map| {
                let mut values = Vec::with_capacity(columns.len());
                for col in columns.iter() {
                    let value = map.get(col).cloned().unwrap_or(ApiValue::Null);
                    let normalized =
                        ResultNormalizer::normalize_value(value).unwrap_or(ApiValue::Null);
                    values.push(normalized);
                }
                Row::new(columns.clone(), values)
            })
            .collect();

        let metrics = QueryMetrics {
            parse_time: std::time::Duration::ZERO,
            plan_time: std::time::Duration::ZERO,
            exec_time,
            total_time: total_start.elapsed(),
            rows_returned: rows.len(),
            ..Default::default()
        };

        Ok(QueryResult::new(
            columns,
            rows,
            executor.take_warnings(),
            metrics,
        ))
    }
}
