// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Prepared statements for repeated query execution.
//!
//! `PreparedQuery` caches the parsed AST and logical plan so that repeated
//! executions skip the parse and plan phases. If the schema changes between
//! executions, the query is transparently re-planned.

use std::collections::HashMap;
use std::sync::Arc;

use crate::api::UniInner;
use crate::api::impl_locy::LocyRuleRegistry;
use crate::api::locy_result::LocyResult;
use uni_common::{Result, UniError, Value};
use uni_query::QueryResult;

/// A prepared Cypher query with a cached logical plan.
///
/// Created via [`Session::prepare()`](crate::api::session::Session::prepare).
/// The plan is cached and reused across executions. If the database schema
/// changes, the plan is automatically regenerated.
pub struct PreparedQuery {
    db: Arc<UniInner>,
    query_text: String,
    ast: uni_query::CypherQuery,
    plan: uni_query::LogicalPlan,
    schema_version: u32,
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
            ast,
            plan,
            schema_version,
        })
    }

    /// Execute the prepared query with the given parameters.
    ///
    /// If the schema has changed since preparation, the query is
    /// transparently re-planned before execution.
    pub async fn execute(&mut self, params: &[(&str, Value)]) -> Result<QueryResult> {
        self.ensure_plan_fresh()?;

        let param_map: HashMap<String, Value> = params
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();

        self.db
            .execute_plan_internal(
                self.plan.clone(),
                &self.query_text,
                param_map,
                self.db.config.clone(),
                None,
            )
            .await
    }

    /// Fluent parameter builder for executing a prepared query.
    pub fn bind(&mut self) -> PreparedQueryBinder<'_> {
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
    fn ensure_plan_fresh(&mut self) -> Result<()> {
        let current_version = self.db.schema.schema().schema_version;
        if current_version != self.schema_version {
            let planner = uni_query::QueryPlanner::new(self.db.schema.schema().clone());
            self.plan = planner
                .plan(self.ast.clone())
                .map_err(|e| UniError::Query {
                    message: e.to_string(),
                    query: Some(self.query_text.clone()),
                })?;
            self.schema_version = current_version;
        }
        Ok(())
    }
}

/// Fluent parameter builder for executing a [`PreparedQuery`].
pub struct PreparedQueryBinder<'a> {
    prepared: &'a mut PreparedQuery,
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
        self.prepared
            .db
            .execute_plan_internal(
                self.prepared.plan.clone(),
                &self.prepared.query_text,
                self.params,
                self.prepared.db.config.clone(),
                None,
            )
            .await
    }
}

/// A prepared Locy program with a cached compiled program.
///
/// Created via [`Session::prepare_locy()`](crate::api::session::Session::prepare_locy).
pub struct PreparedLocy {
    db: Arc<UniInner>,
    #[allow(dead_code)]
    rule_registry: Arc<std::sync::RwLock<LocyRuleRegistry>>,
    program_text: String,
    #[allow(dead_code)]
    compiled: uni_locy::CompiledProgram,
    #[allow(dead_code)]
    schema_version: u32,
}

impl PreparedLocy {
    /// Create a new prepared Locy program.
    pub(crate) fn new(
        db: Arc<UniInner>,
        rule_registry: Arc<std::sync::RwLock<LocyRuleRegistry>>,
        program: &str,
    ) -> Result<Self> {
        let ast = uni_cypher::parse_locy(program).map_err(|e| UniError::Parse {
            message: format!("LocyParseError: {e}"),
            position: None,
            line: None,
            column: None,
            context: None,
        })?;

        let registry = rule_registry.read().unwrap();
        let compiled = if registry.rules.is_empty() {
            drop(registry);
            uni_locy::compile(&ast).map_err(|e| UniError::Query {
                message: format!("LocyCompileError: {e}"),
                query: None,
            })?
        } else {
            let external_names: Vec<String> = registry.rules.keys().cloned().collect();
            drop(registry);
            uni_locy::compile_with_external_rules(&ast, &external_names).map_err(|e| {
                UniError::Query {
                    message: format!("LocyCompileError: {e}"),
                    query: None,
                }
            })?
        };

        let schema_version = db.schema.schema().schema_version;

        Ok(Self {
            db,
            rule_registry,
            program_text: program.to_string(),
            compiled,
            schema_version,
        })
    }

    /// Execute the prepared Locy program.
    pub async fn execute(&self) -> Result<LocyResult> {
        let engine = crate::api::impl_locy::LocyEngine {
            db: &self.db,
            tx_l0_override: None,
            collect_derive: false,
        };
        engine.evaluate(&self.program_text).await
    }

    /// The original program text.
    pub fn program_text(&self) -> &str {
        &self.program_text
    }
}
