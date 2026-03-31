// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Prepared statements for repeated query and Locy program execution.
//!
//! `PreparedQuery` caches the parsed AST and logical plan so that repeated
//! executions skip the parse and plan phases. `PreparedLocy` caches the
//! compiled program. Both transparently refresh if the schema changes.

use std::collections::HashMap;
use std::sync::Arc;

use crate::api::UniInner;
use crate::api::impl_locy::LocyRuleRegistry;
use crate::api::locy_result::LocyResult;
use uni_common::{Result, UniError, Value};
use uni_locy::LocyConfig;
use uni_query::QueryResult;

// ── PreparedQuery ─────────────────────────────────────────────────

/// Interior state for schema-staleness detection.
struct PreparedQueryInner {
    ast: uni_query::CypherQuery,
    plan: uni_query::LogicalPlan,
    schema_version: u32,
}

/// A prepared Cypher query with a cached logical plan.
///
/// Created via [`Session::prepare()`](crate::api::session::Session::prepare).
/// The plan is cached and reused across executions. If the database schema
/// changes, the plan is automatically regenerated.
///
/// All methods take `&self`, so a `PreparedQuery` can be shared across
/// threads via `Arc<PreparedQuery>`.
pub struct PreparedQuery {
    db: Arc<UniInner>,
    query_text: String,
    inner: std::sync::RwLock<PreparedQueryInner>,
}

impl PreparedQuery {
    /// Create a new prepared query by parsing and planning the given Cypher.
    pub(crate) async fn new(db: Arc<UniInner>, cypher: &str) -> Result<Self> {
        let ast = uni_cypher::parse(cypher).map_err(|e| UniError::Parse {
            message: e.to_string(),
            position: None,
            line: None,
            column: None,
            context: Some(cypher.to_string()),
        })?;

        let schema_version = db.schema.schema().schema_version;
        let planner = uni_query::QueryPlanner::new(db.schema.schema().clone());
        let plan = planner.plan(ast.clone()).map_err(|e| UniError::Query {
            message: e.to_string(),
            query: Some(cypher.to_string()),
        })?;

        Ok(Self {
            db,
            query_text: cypher.to_string(),
            inner: std::sync::RwLock::new(PreparedQueryInner {
                ast,
                plan,
                schema_version,
            }),
        })
    }

    /// Execute the prepared query with the given parameters.
    ///
    /// If the schema has changed since preparation, the query is
    /// transparently re-planned before execution.
    pub async fn execute(&self, params: &[(&str, Value)]) -> Result<QueryResult> {
        self.ensure_plan_fresh()?;

        let param_map: HashMap<String, Value> = params
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();

        let plan = {
            let inner = self.inner.read().unwrap();
            inner.plan.clone()
        };

        self.db
            .execute_plan_internal(
                plan,
                &self.query_text,
                param_map,
                self.db.config.clone(),
                None,
            )
            .await
    }

    /// Fluent parameter builder for executing a prepared query.
    pub fn bind(&self) -> PreparedQueryBinder<'_> {
        PreparedQueryBinder {
            prepared: self,
            params: HashMap::new(),
        }
    }

    /// The original query text.
    pub fn query_text(&self) -> &str {
        &self.query_text
    }

    /// Re-plan the query if the schema has changed.
    fn ensure_plan_fresh(&self) -> Result<()> {
        let current_version = self.db.schema.schema().schema_version;

        // Fast path: read lock only
        {
            let inner = self.inner.read().unwrap();
            if inner.schema_version == current_version {
                return Ok(());
            }
        }

        // Slow path: write lock with double-check
        let mut inner = self.inner.write().unwrap();
        if inner.schema_version == current_version {
            return Ok(());
        }

        let planner = uni_query::QueryPlanner::new(self.db.schema.schema().clone());
        inner.plan = planner
            .plan(inner.ast.clone())
            .map_err(|e| UniError::Query {
                message: e.to_string(),
                query: Some(self.query_text.clone()),
            })?;
        inner.schema_version = current_version;
        Ok(())
    }
}

/// Fluent parameter builder for executing a [`PreparedQuery`].
pub struct PreparedQueryBinder<'a> {
    prepared: &'a PreparedQuery,
    params: HashMap<String, Value>,
}

impl<'a> PreparedQueryBinder<'a> {
    /// Bind a named parameter.
    pub fn param<K: Into<String>, V: Into<Value>>(mut self, key: K, value: V) -> Self {
        self.params.insert(key.into(), value.into());
        self
    }

    /// Execute with the bound parameters.
    pub async fn execute(self) -> Result<QueryResult> {
        self.prepared.ensure_plan_fresh()?;

        let plan = {
            let inner = self.prepared.inner.read().unwrap();
            inner.plan.clone()
        };

        self.prepared
            .db
            .execute_plan_internal(
                plan,
                &self.prepared.query_text,
                self.params,
                self.prepared.db.config.clone(),
                None,
            )
            .await
    }
}

// ── PreparedLocy ──────────────────────────────────────────────────

/// Interior state for schema-staleness detection.
struct PreparedLocyInner {
    compiled: uni_locy::CompiledProgram,
    schema_version: u32,
}

/// A prepared Locy program with a cached compiled program.
///
/// Created via [`Session::prepare_locy()`](crate::api::session::Session::prepare_locy).
/// The compiled program is cached and reused across executions. If the database
/// schema changes, the program is automatically recompiled.
///
/// All methods take `&self`, so a `PreparedLocy` can be shared across
/// threads via `Arc<PreparedLocy>`.
pub struct PreparedLocy {
    db: Arc<UniInner>,
    rule_registry: Arc<std::sync::RwLock<LocyRuleRegistry>>,
    program_text: String,
    inner: std::sync::RwLock<PreparedLocyInner>,
}

impl PreparedLocy {
    /// Create a new prepared Locy program.
    pub(crate) fn new(
        db: Arc<UniInner>,
        rule_registry: Arc<std::sync::RwLock<LocyRuleRegistry>>,
        program: &str,
    ) -> Result<Self> {
        let compiled = compile_locy_with_registry(program, &rule_registry)?;
        let schema_version = db.schema.schema().schema_version;

        Ok(Self {
            db,
            rule_registry,
            program_text: program.to_string(),
            inner: std::sync::RwLock::new(PreparedLocyInner {
                compiled,
                schema_version,
            }),
        })
    }

    /// Execute the prepared Locy program with the given parameters.
    ///
    /// Uses the cached compiled program. If the schema has changed since
    /// preparation, the program is automatically recompiled before execution.
    pub async fn execute(&self, params: &[(&str, Value)]) -> Result<LocyResult> {
        let param_map: HashMap<String, Value> = params
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        self.execute_internal(param_map).await
    }

    /// Fluent parameter builder for executing a prepared Locy program.
    pub fn bind(&self) -> PreparedLocyBinder<'_> {
        PreparedLocyBinder {
            prepared: self,
            params: HashMap::new(),
        }
    }

    /// The original program text.
    pub fn program_text(&self) -> &str {
        &self.program_text
    }

    /// Internal execution with a parameter map.
    async fn execute_internal(&self, params: HashMap<String, Value>) -> Result<LocyResult> {
        self.ensure_compiled_fresh()?;

        // Clone the compiled program and merge rules
        let mut compiled = {
            let inner = self.inner.read().unwrap();
            inner.compiled.clone()
        };

        // Merge registered rules (same logic as evaluate_with_config)
        {
            let registry = self.rule_registry.read().unwrap();
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

        // Build config with params and session-level semantics
        let config = LocyConfig {
            params,
            ..LocyConfig::default()
        };

        let engine = crate::api::impl_locy::LocyEngine {
            db: &self.db,
            tx_l0_override: None,
            locy_l0: None,
            collect_derive: true,
        };
        engine
            .evaluate_compiled_with_config(compiled, &config)
            .await
    }

    /// Re-compile if the schema has changed.
    fn ensure_compiled_fresh(&self) -> Result<()> {
        let current_version = self.db.schema.schema().schema_version;

        // Fast path: read lock only
        {
            let inner = self.inner.read().unwrap();
            if inner.schema_version == current_version {
                return Ok(());
            }
        }

        // Slow path: write lock with double-check
        let mut inner = self.inner.write().unwrap();
        if inner.schema_version == current_version {
            return Ok(());
        }

        inner.compiled = compile_locy_with_registry(&self.program_text, &self.rule_registry)?;
        inner.schema_version = current_version;
        Ok(())
    }
}

/// Parse and compile a Locy program using the given rule registry.
///
/// Shared between `PreparedLocy::new()` and `PreparedLocy::ensure_compiled_fresh()`.
fn compile_locy_with_registry(
    program: &str,
    rule_registry: &std::sync::RwLock<LocyRuleRegistry>,
) -> Result<uni_locy::CompiledProgram> {
    let ast = uni_cypher::parse_locy(program).map_err(|e| UniError::Parse {
        message: format!("LocyParseError: {e}"),
        position: None,
        line: None,
        column: None,
        context: None,
    })?;

    let registry = rule_registry.read().unwrap();
    if registry.rules.is_empty() {
        drop(registry);
        uni_locy::compile(&ast).map_err(|e| UniError::Query {
            message: format!("LocyCompileError: {e}"),
            query: None,
        })
    } else {
        let external_names: Vec<String> = registry.rules.keys().cloned().collect();
        drop(registry);
        uni_locy::compile_with_external_rules(&ast, &external_names).map_err(|e| {
            UniError::Query {
                message: format!("LocyCompileError: {e}"),
                query: None,
            }
        })
    }
}

/// Fluent parameter builder for executing a [`PreparedLocy`].
pub struct PreparedLocyBinder<'a> {
    prepared: &'a PreparedLocy,
    params: HashMap<String, Value>,
}

impl<'a> PreparedLocyBinder<'a> {
    /// Bind a named parameter.
    pub fn param<K: Into<String>, V: Into<Value>>(mut self, key: K, value: V) -> Self {
        self.params.insert(key.into(), value.into());
        self
    }

    /// Execute with the bound parameters.
    pub async fn execute(self) -> Result<LocyResult> {
        self.prepared.execute_internal(self.params).await
    }
}
