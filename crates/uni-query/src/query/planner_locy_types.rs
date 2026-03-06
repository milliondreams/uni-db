// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Supporting types for Locy logical plan variants.
//!
//! These types describe the structure of a Locy program after planning:
//! strata, rules, clauses, IS-references, yield schemas, and top-level commands.

use arrow_schema::DataType;

use super::planner::LogicalPlan;
use uni_cypher::ast::{Expr, Query};
use uni_cypher::locy_ast::{AbduceQuery, DeriveCommand, ExplainRule, GoalQuery};
use uni_locy::types::CompiledAssume;

/// A stratum in the stratified evaluation order.
///
/// Each stratum contains rules that can be evaluated together (possibly recursively).
/// Strata are ordered by dependency: stratum N depends only on strata < N.
#[derive(Debug, Clone)]
pub struct LocyStratum {
    /// Stratum index (0-based).
    pub id: usize,
    /// Rules in this stratum.
    pub rules: Vec<LocyRulePlan>,
    /// Whether this stratum requires fixpoint iteration.
    pub is_recursive: bool,
    /// Indices of strata this one depends on.
    pub depends_on: Vec<usize>,
}

/// A planned Locy rule (one named derived relation).
#[derive(Debug, Clone)]
pub struct LocyRulePlan {
    /// Rule name (e.g., `reachable`).
    pub name: String,
    /// Clauses (one per `<-` body). Multiple clauses form a union.
    pub clauses: Vec<LocyClausePlan>,
    /// Output schema columns.
    pub yield_schema: Vec<LocyYieldColumn>,
    /// Optional priority weight for PRIORITY semantics.
    pub priority: Option<i64>,
    /// FOLD bindings for post-fixpoint aggregation (name, aggregate expr).
    pub fold_bindings: Vec<(String, Expr)>,
    /// BEST BY criteria for post-fixpoint selection (expr, ascending).
    pub best_by_criteria: Vec<(Expr, bool)>,
}

/// A single clause (body) of a Locy rule.
#[derive(Debug, Clone)]
pub struct LocyClausePlan {
    /// The planned query body (Scan → Traverse → Filter → Project chain).
    pub body: LogicalPlan,
    /// IS-references to other derived relations in this clause.
    pub is_refs: Vec<LocyIsRef>,
    /// ALONG binding variable names.
    pub along_bindings: Vec<String>,
    /// Optional priority value for this clause.
    pub priority: Option<i64>,
}

/// An IS-reference from a clause body to another derived relation.
#[derive(Debug, Clone)]
pub struct LocyIsRef {
    /// The target rule name.
    pub rule_name: String,
    /// Subject variable bindings (FROM arguments).
    pub subjects: Vec<Expr>,
    /// Target variable binding (TO argument), if any.
    pub target: Option<Expr>,
    /// Whether this is a negated IS-reference (`NOT IS`).
    pub negated: bool,
}

/// A column in a rule's yield schema.
#[derive(Debug, Clone)]
pub struct LocyYieldColumn {
    /// Column name.
    pub name: String,
    /// Whether this column is a KEY column.
    pub is_key: bool,
    /// Arrow data type for this column (inferred from yield expressions).
    pub data_type: DataType,
}

/// A top-level Locy command to execute after fixpoint evaluation.
///
/// Commands carry compiled AST data and are dispatched by the caller
/// (e.g., `evaluate_native`) via the orchestrator after strata evaluation.
#[derive(Debug, Clone)]
pub enum LocyCommand {
    /// Query a derived relation: `QUERY rulename WHERE expr`
    GoalQuery { goal_query: GoalQuery },
    /// Derive facts into the database: `DERIVE rulename`
    Derive { derive_command: DeriveCommand },
    /// Assume facts and evaluate a body: `ASSUME { ... } THEN { ... }`
    Assume { compiled_assume: CompiledAssume },
    /// Explain a rule's derivation: `EXPLAIN RULE rulename WHERE expr`
    ExplainRule { explain_rule: ExplainRule },
    /// Abduce missing facts: `ABDUCE rulename WHERE expr`
    Abduce { abduce_query: AbduceQuery },
    /// Pass-through Cypher statement.
    Cypher { query: Query },
}
