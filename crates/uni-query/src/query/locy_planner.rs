// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Locy plan builder — translates a [`CompiledProgram`] into a `LogicalPlan::LocyProgram`.
//!
//! Stratifies rules, plans clause bodies via [`QueryPlanner::plan_pattern()`],
//! and assembles [`LocyStratum`] / [`LocyRulePlan`] / [`LocyClausePlan`] trees with
//! `LocyDerivedScan` nodes for IS-reference data injection.
//!
//! **Scope:** Strata (rules + clauses) only. Commands (`DERIVE`, `QUERY`, `ASSUME`,
//! `ABDUCE`, `EXPLAIN`) are dispatched by the orchestrator after strata evaluation;
//! dedicated command plan building is a future enhancement.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use parking_lot::RwLock;

use uni_cypher::ast::{BinaryOp, CypherLiteral, Expr, PatternElement};
use uni_cypher::locy_ast::{LocyBinaryOp, LocyExpr, RuleCondition, RuleOutput};
use uni_locy::types::{
    CompiledClause, CompiledCommand, CompiledProgram, CompiledRule, Stratum, YieldColumn,
};

/// Collect all node-typed variable names from a rule's clauses.
///
/// This includes MATCH pattern node variables AND IS-ref subject/target variables,
/// all of which hold node VIDs (UInt64). Used for yield column type inference.
///
/// NOTE: For IS-ref predicate building (where `{var}._vid` column references are
/// needed), use `collect_match_node_vars` instead — only MATCH pattern variables
/// have expanded `._vid` columns in the graph scan output.
fn collect_node_vars(clauses: &[CompiledClause]) -> HashSet<String> {
    let mut node_vars = HashSet::new();
    for clause in clauses {
        collect_match_node_vars(clause, &mut node_vars);
        // IS-ref subjects and targets are also node VIDs (UInt64)
        for condition in &clause.where_conditions {
            if let RuleCondition::IsReference(is_ref) = condition {
                for subject in &is_ref.subjects {
                    node_vars.insert(subject.clone());
                }
                if let Some(target_var) = &is_ref.target {
                    node_vars.insert(target_var.clone());
                }
            }
        }
    }
    node_vars
}

/// Collect node variable names from a single clause's MATCH pattern only.
///
/// Only these variables have expanded `{var}._vid`, `{var}._labels`, `{var}._all_props`
/// columns in the graph scan output. IS-ref target/subject variables exist as plain
/// columns in the derived scan and should NOT be rewritten to `{var}._vid`.
fn collect_match_node_vars(clause: &CompiledClause, node_vars: &mut HashSet<String>) {
    for path in &clause.match_pattern.paths {
        for elem in &path.elements {
            if let PatternElement::Node(np) = elem
                && let Some(var) = &np.variable
            {
                node_vars.insert(var.clone());
            }
        }
    }
}

/// Infer the Arrow DataType for a yield column based on its expression in the first clause.
fn infer_yield_type(
    name: &str,
    first_clause: &CompiledClause,
    node_vars: &HashSet<String>,
    fold_output_names: &HashSet<&str>,
    along_names: &HashSet<&str>,
) -> DataType {
    // Node variables → UInt64 (stores VID)
    if node_vars.contains(name) {
        return DataType::UInt64;
    }
    // FOLD outputs — type depends on the aggregate function.
    // COUNT/MCOUNT produce Int64; SUM/MSUM/AVG produce Float64.
    if fold_output_names.contains(name) {
        if let Some(fold) = first_clause.fold.iter().find(|fb| fb.name == name)
            && let Expr::FunctionCall { name: fn_name, .. } = &fold.aggregate
        {
            match fn_name.to_uppercase().as_str() {
                "COUNT" | "MCOUNT" => return DataType::Int64,
                _ => {}
            }
        }
        return DataType::Float64;
    }
    // ALONG bindings → Float64 (typically numeric accumulations)
    if along_names.contains(name) {
        return DataType::Float64;
    }
    // Look at the yield expression from the first clause
    if let RuleOutput::Yield(yc) = &first_clause.output {
        for item in &yc.items {
            let item_name = item.alias.clone().unwrap_or_else(|| match &item.expr {
                Expr::Variable(n) => n.clone(),
                Expr::Property(_, prop) => prop.clone(),
                _ => String::new(),
            });
            if item_name == name {
                // If the expression is a bare Variable referencing an ALONG name,
                // infer as Float64 (ALONG bindings are numeric). Without this,
                // `ew AS link_weight` would infer Variable("ew") as LargeUtf8.
                if let Expr::Variable(v) = &item.expr
                    && along_names.contains(v.as_str())
                {
                    return DataType::Float64;
                }
                return infer_expr_type(&item.expr, node_vars);
            }
        }
    }
    DataType::LargeUtf8
}

/// Infer Arrow DataType from a Cypher expression.
fn infer_expr_type(expr: &Expr, node_vars: &HashSet<String>) -> DataType {
    match expr {
        Expr::Variable(v) if node_vars.contains(v) => DataType::UInt64,
        Expr::Literal(CypherLiteral::Integer(_)) => DataType::Int64,
        Expr::Literal(CypherLiteral::Float(_)) => DataType::Float64,
        Expr::Literal(CypherLiteral::String(_)) => DataType::LargeUtf8,
        Expr::Literal(CypherLiteral::Bool(_)) => DataType::Boolean,
        Expr::Literal(CypherLiteral::Null) => DataType::LargeUtf8,
        Expr::Property(_, _) => DataType::Float64,
        // Binary operations: infer from operator and operand types.
        Expr::BinaryOp { left, op, right } => {
            use uni_cypher::ast::BinaryOp::*;
            match op {
                // Comparison and logical operators always return Boolean.
                Eq | NotEq | Lt | LtEq | Gt | GtEq | And | Or | Xor | Regex | Contains
                | StartsWith | EndsWith => DataType::Boolean,
                // Arithmetic operators: infer from operands.
                Add | Sub | Mul | Div | Mod | Pow | ApproxEq => {
                    let lt = infer_expr_type(left, node_vars);
                    let rt = infer_expr_type(right, node_vars);
                    // If either operand is Float64, result is Float64.
                    if lt == DataType::Float64 || rt == DataType::Float64 {
                        DataType::Float64
                    } else if lt == DataType::Int64 && rt == DataType::Int64 {
                        DataType::Int64
                    } else {
                        DataType::Float64
                    }
                }
            }
        }
        // Unary operations: infer from inner expression.
        Expr::UnaryOp { op, expr: inner } => {
            use uni_cypher::ast::UnaryOp;
            match op {
                UnaryOp::Not => DataType::Boolean,
                UnaryOp::Neg => infer_expr_type(inner, node_vars),
            }
        }
        Expr::IsNull(_) | Expr::IsNotNull(_) => DataType::Boolean,
        // Function calls: infer return type from function name.
        Expr::FunctionCall { name, args, .. } => {
            match name.to_uppercase().as_str() {
                // Numeric functions → Float64
                "SIMILAR_TO" | "ABS" | "SQRT" | "LOG" | "LOG10" | "EXP" | "CEIL" | "FLOOR"
                | "ROUND" | "SIGN" | "RAND" | "TOFLOAT" | "TODOUBLE" | "COS" | "SIN" | "TAN"
                | "ACOS" | "ASIN" | "ATAN" | "ATAN2" | "DEGREES" | "RADIANS" | "PI" | "E"
                | "DISTANCE" => DataType::Float64,
                // Integer-returning functions
                "TOINTEGER" | "LENGTH" | "SIZE" | "ID" => DataType::Int64,
                // String-returning functions
                "TOSTRING" | "TOLOWER" | "TOUPPER" | "TRIM" | "LTRIM" | "RTRIM" | "REPLACE"
                | "SUBSTRING" | "LEFT" | "RIGHT" | "REVERSE" | "TYPE" => DataType::LargeUtf8,
                // Boolean-returning functions
                "EXISTS" | "STARTSWITH" | "ENDSWITH" | "CONTAINS" => DataType::Boolean,
                // Aggregates that return Float64
                "SUM" | "AVG" | "MAX" | "MIN" => DataType::Float64,
                "COUNT" => DataType::Int64,
                // Unknown function: try to infer from first argument
                _ => {
                    if let Some(first_arg) = args.first() {
                        infer_expr_type(first_arg, node_vars)
                    } else {
                        DataType::LargeUtf8
                    }
                }
            }
        }
        _ => DataType::LargeUtf8,
    }
}

use super::df_graph::locy_fixpoint::{DerivedScanEntry, DerivedScanRegistry};
use super::planner::{LogicalPlan, QueryPlanner};
use super::planner_locy_types::{
    LocyClausePlan, LocyCommand, LocyIsRef, LocyRulePlan, LocyStratum, LocyYieldColumn,
};

// ---------------------------------------------------------------------------
// DerivedScanHandle
// ---------------------------------------------------------------------------

/// Internal handle tracking a single derived-scan injection point.
///
/// Each handle corresponds to a `LocyDerivedScan` node in the plan tree and
/// a `DerivedScanEntry` in the resulting registry. The `Arc<RwLock>` data handle
/// is shared between the plan node and registry entry so that `FixpointExec`
/// can inject data that flows into the subplan.
#[derive(Clone)]
struct DerivedScanHandle {
    rule_name: String,
    scan_index: usize,
    is_self_ref: bool,
    data: Arc<RwLock<Vec<RecordBatch>>>,
    schema: SchemaRef,
}

// ---------------------------------------------------------------------------
// LocyPlanBuilder
// ---------------------------------------------------------------------------

/// Builds a `LogicalPlan::LocyProgram` from a [`CompiledProgram`].
///
/// The builder translates each stratum, rule, and clause into corresponding
/// plan-level types, wiring `LocyDerivedScan` nodes for IS-reference data
/// injection. The returned `DerivedScanRegistry` shares `Arc<RwLock>` handles
/// with the plan nodes so that `FixpointExec` can populate derived data at
/// execution time.
pub struct LocyPlanBuilder<'a> {
    planner: &'a QueryPlanner,
    derived_scan_handles: RefCell<Vec<DerivedScanHandle>>,
}

impl<'a> LocyPlanBuilder<'a> {
    /// Create a new plan builder backed by the given `QueryPlanner`.
    pub fn new(planner: &'a QueryPlanner) -> Self {
        Self {
            planner,
            derived_scan_handles: RefCell::new(Vec::new()),
        }
    }

    /// Build a full `LogicalPlan::LocyProgram` with embedded `DerivedScanRegistry`.
    #[expect(clippy::too_many_arguments, reason = "mirrors LocyConfig fields")]
    pub fn build_program_plan(
        &self,
        compiled: &CompiledProgram,
        max_iterations: usize,
        timeout: std::time::Duration,
        max_derived_bytes: usize,
        deterministic_best_by: bool,
        strict_probability_domain: bool,
        probability_epsilon: f64,
        exact_probability: bool,
        max_bdd_variables: usize,
        top_k_proofs: usize,
    ) -> Result<LogicalPlan> {
        let mut strata = Vec::with_capacity(compiled.strata.len());

        for stratum in &compiled.strata {
            let rule_names: HashSet<String> =
                stratum.rules.iter().map(|r| r.name.clone()).collect();
            let locy_stratum = self.build_stratum(
                stratum,
                &compiled.rule_catalog,
                &rule_names,
                strict_probability_domain,
                probability_epsilon,
            )?;
            strata.push(locy_stratum);
        }

        let registry = self.build_registry();
        let plan = LogicalPlan::LocyProgram {
            strata,
            commands: self.build_commands(&compiled.commands),
            derived_scan_registry: Arc::new(registry),
            max_iterations,
            timeout,
            max_derived_bytes,
            deterministic_best_by,
            strict_probability_domain,
            probability_epsilon,
            exact_probability,
            max_bdd_variables,
            top_k_proofs,
        };

        Ok(plan)
    }

    /// Build `LocyCommand` list from compiled commands.
    ///
    /// Commands carry AST data for dispatch by the caller (e.g., `evaluate_native`)
    /// via the orchestrator after strata evaluation completes.
    fn build_commands(&self, commands: &[CompiledCommand]) -> Vec<LocyCommand> {
        commands
            .iter()
            .map(|cmd| match cmd {
                CompiledCommand::GoalQuery(gq) => LocyCommand::GoalQuery {
                    goal_query: gq.clone(),
                },
                CompiledCommand::ExplainRule(er) => LocyCommand::ExplainRule {
                    explain_rule: er.clone(),
                },
                CompiledCommand::Abduce(aq) => LocyCommand::Abduce {
                    abduce_query: aq.clone(),
                },
                CompiledCommand::Assume(ca) => LocyCommand::Assume {
                    compiled_assume: ca.clone(),
                },
                CompiledCommand::DeriveCommand(dc) => LocyCommand::Derive {
                    derive_command: dc.clone(),
                },
                CompiledCommand::Cypher(q) => LocyCommand::Cypher { query: q.clone() },
            })
            .collect()
    }

    // -- Stratum --------------------------------------------------------

    fn build_stratum(
        &self,
        stratum: &Stratum,
        rule_catalog: &HashMap<String, CompiledRule>,
        stratum_rule_names: &HashSet<String>,
        strict_probability_domain: bool,
        probability_epsilon: f64,
    ) -> Result<LocyStratum> {
        let mut rules = Vec::with_capacity(stratum.rules.len());
        for rule in &stratum.rules {
            rules.push(self.build_rule(
                rule,
                stratum.is_recursive,
                stratum_rule_names,
                rule_catalog,
                strict_probability_domain,
                probability_epsilon,
            )?);
        }

        Ok(LocyStratum {
            id: stratum.id,
            rules,
            is_recursive: stratum.is_recursive,
            depends_on: stratum.depends_on.clone(),
        })
    }

    // -- Rule -----------------------------------------------------------

    fn build_rule(
        &self,
        rule: &CompiledRule,
        is_recursive: bool,
        stratum_rule_names: &HashSet<String>,
        rule_catalog: &HashMap<String, CompiledRule>,
        strict_probability_domain: bool,
        probability_epsilon: f64,
    ) -> Result<LocyRulePlan> {
        // Collect node variable names from match patterns for VID-based joins
        let node_vars = collect_node_vars(&rule.clauses);

        let mut clauses = Vec::with_capacity(rule.clauses.len());
        for clause in &rule.clauses {
            clauses.push(self.build_clause(
                clause,
                &rule.yield_schema,
                is_recursive,
                stratum_rule_names,
                rule_catalog,
                &node_vars,
                strict_probability_domain,
                probability_epsilon,
            )?);
        }

        // All clauses share the same schema; derive metadata from first clause
        let first_clause = rule.clauses.first();

        // Collect fold bindings from the first clause that has them.
        // A rule may have a base clause (no FOLD) plus recursive clauses (with FOLD);
        // we need the FOLD metadata from whichever clause provides it.
        let fold_bindings: Vec<(String, Expr)> = rule
            .clauses
            .iter()
            .find(|c| !c.fold.is_empty())
            .map(|c| {
                c.fold
                    .iter()
                    .map(|fb| (fb.name.clone(), fb.aggregate.clone()))
                    .collect()
            })
            .unwrap_or_default();
        let best_by_criteria = first_clause
            .and_then(|c| c.best_by.as_ref())
            .map(|bb| {
                bb.items
                    .iter()
                    .map(|item| (item.expr.clone(), item.ascending))
                    .collect()
            })
            .unwrap_or_default();

        let fold_output_names: HashSet<&str> = first_clause
            .map(|c| c.fold.iter().map(|fb| fb.name.as_str()).collect())
            .unwrap_or_default();
        let along_names: HashSet<&str> = first_clause
            .map(|c| c.along.iter().map(|a| a.name.as_str()).collect())
            .unwrap_or_default();

        let yield_schema: Vec<LocyYieldColumn> = rule
            .yield_schema
            .iter()
            .map(|yc| {
                let data_type = match first_clause {
                    Some(fc) => {
                        infer_yield_type(&yc.name, fc, &node_vars, &fold_output_names, &along_names)
                    }
                    None => DataType::LargeUtf8,
                };
                LocyYieldColumn {
                    name: yc.name.clone(),
                    is_key: yc.is_key,
                    is_prob: yc.is_prob,
                    data_type,
                }
            })
            .collect();

        Ok(LocyRulePlan {
            name: rule.name.clone(),
            clauses,
            yield_schema,
            priority: rule.priority,
            fold_bindings,
            best_by_criteria,
        })
    }

    // -- Clause ---------------------------------------------------------

    #[expect(
        clippy::too_many_arguments,
        reason = "clause builder requires full planner context"
    )]
    fn build_clause(
        &self,
        clause: &CompiledClause,
        yield_cols: &[YieldColumn],
        _is_recursive: bool,
        stratum_rule_names: &HashSet<String>,
        rule_catalog: &HashMap<String, CompiledRule>,
        node_vars: &HashSet<String>,
        _strict_probability_domain: bool,
        _probability_epsilon: f64,
    ) -> Result<LocyClausePlan> {
        // Collect node variables from THIS clause's MATCH pattern only.
        // Used for IS-ref predicates: only variables in the current MATCH
        // have expanded {var}._vid columns in the graph scan output.
        let mut clause_node_vars = HashSet::new();
        collect_match_node_vars(clause, &mut clause_node_vars);

        // Step 1: MATCH pattern → Scan→Traverse chain
        let mut plan = self
            .planner
            .plan_pattern(&clause.match_pattern, &[])
            .context("planning MATCH pattern")?;

        // Step 2: WHERE filter (non-IS conditions only).
        // Conditions referencing IS-ref target variables must be deferred until
        // after the IS-ref joins that introduce those variables.
        let is_ref_target_vars: HashSet<String> = clause
            .where_conditions
            .iter()
            .filter_map(|c| match c {
                RuleCondition::IsReference(ir) if !ir.negated => ir.target.clone(),
                _ => None,
            })
            .collect();

        let all_filter_exprs: Vec<&Expr> = clause
            .where_conditions
            .iter()
            .filter_map(|c| match c {
                RuleCondition::Expression(e) => Some(e),
                _ => None,
            })
            .collect();

        let (deferred_filter_exprs, immediate_filter_exprs): (Vec<&Expr>, Vec<&Expr>) =
            if is_ref_target_vars.is_empty() {
                (Vec::new(), all_filter_exprs)
            } else {
                all_filter_exprs
                    .into_iter()
                    .partition(|e| expr_references_any(e, &is_ref_target_vars))
            };

        if !immediate_filter_exprs.is_empty() {
            let predicate = combine_with_and(&immediate_filter_exprs);
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate,
                optional_variables: HashSet::new(),
            };
        }

        // Step 3: IS-reference joins
        let mut is_refs = Vec::new();
        let mut along_bindings = Vec::new();

        for condition in &clause.where_conditions {
            if let RuleCondition::IsReference(is_ref) = condition {
                let target_rule_name = is_ref.rule_name.to_string();

                // Validate: rule exists in catalog
                let target_rule = rule_catalog.get(&target_rule_name).with_context(|| {
                    format!("IS-reference to unknown rule '{}'", target_rule_name)
                })?;

                // Validate: arity — subjects bind to KEY columns positionally.
                // The optional target binds to the next KEY column (if any remain)
                // or the first non-KEY column. So subjects must not exceed KEY count.
                let key_count = target_rule
                    .yield_schema
                    .iter()
                    .filter(|yc| yc.is_key)
                    .count();
                if is_ref.subjects.len() > key_count {
                    bail!(
                        "IS-reference to '{}': arity mismatch — {} subjects \
                         provided but rule has only {} KEY columns",
                        target_rule_name,
                        is_ref.subjects.len(),
                        key_count,
                    );
                }

                let is_self_ref = stratum_rule_names.contains(&target_rule_name);
                let handle = self.get_or_create_derived_scan_handle(
                    &target_rule_name,
                    target_rule,
                    is_self_ref,
                );

                // Look up target rule's PROB column (if any)
                let (target_has_prob, target_prob_col) = rule_catalog
                    .get(&target_rule_name)
                    .and_then(|r| {
                        r.yield_schema
                            .iter()
                            .find(|c| c.is_prob)
                            .map(|c| (true, Some(c.name.clone())))
                    })
                    .unwrap_or((false, None));

                // Build LocyIsRef metadata
                let locy_is_ref = LocyIsRef {
                    rule_name: target_rule_name.clone(),
                    subjects: is_ref
                        .subjects
                        .iter()
                        .map(|s| Expr::Variable(s.clone()))
                        .collect(),
                    target: is_ref.target.as_ref().map(|t| Expr::Variable(t.clone())),
                    negated: is_ref.negated,
                    target_has_prob,
                    target_prob_col,
                };
                is_refs.push(locy_is_ref);

                // Non-negated: CrossJoin + Filter (inner join semantics)
                // Negated: no plan nodes — FixpointExec handles anti-join
                if !is_ref.negated {
                    let derived_scan = LogicalPlan::LocyDerivedScan {
                        scan_index: handle.scan_index,
                        data: handle.data.clone(),
                        schema: handle.schema.clone(),
                    };
                    plan = LogicalPlan::CrossJoin {
                        left: Box::new(plan),
                        right: Box::new(derived_scan),
                    };

                    // Subject-only predicate: n._vid = a (target handled separately).
                    let predicate = build_is_ref_predicate(
                        &is_ref.subjects,
                        &None,
                        &target_rule.yield_schema,
                        &clause_node_vars,
                    )?;
                    plan = LogicalPlan::Filter {
                        input: Box::new(plan),
                        predicate,
                        optional_variables: HashSet::new(),
                    };

                    // If the IS-ref has a target variable, bind it to the derived
                    // scan's target column. When the target var name differs from
                    // the derived column name (e.g., `m` vs `b`), add an implicit
                    // ScanAll for the target so it becomes a proper node column.
                    if let Some(target_var) = &is_ref.target {
                        let key_cols: Vec<&YieldColumn> = target_rule
                            .yield_schema
                            .iter()
                            .filter(|yc| yc.is_key)
                            .collect();
                        let non_key_cols: Vec<&YieldColumn> = target_rule
                            .yield_schema
                            .iter()
                            .filter(|yc| !yc.is_key)
                            .collect();
                        let target_col_name = if is_ref.subjects.len() < key_cols.len() {
                            key_cols.get(is_ref.subjects.len()).map(|c| c.name.clone())
                        } else {
                            non_key_cols.first().map(|c| c.name.clone())
                        };

                        if let Some(col_name) = target_col_name {
                            // Always add a node scan so `target_var` becomes a
                            // proper node with `._vid`, `._labels`, and property
                            // columns.  Without this, the derived scan only
                            // provides the VID and property access (e.g.
                            // `b.embedding`) would fail at runtime.
                            let target_node_scan = LogicalPlan::ScanAll {
                                variable: target_var.clone(),
                                filter: None,
                                optional: false,
                            };
                            plan = LogicalPlan::CrossJoin {
                                left: Box::new(plan),
                                right: Box::new(target_node_scan),
                            };
                            // Bind: target_var._vid = derived_col (UInt64 equality)
                            let target_binding = Expr::BinaryOp {
                                left: Box::new(Expr::Variable(format!("{}._vid", target_var))),
                                op: BinaryOp::Eq,
                                right: Box::new(Expr::Variable(col_name)),
                            };
                            plan = LogicalPlan::Filter {
                                input: Box::new(plan),
                                predicate: target_binding,
                                optional_variables: HashSet::new(),
                            };
                        }
                    }
                }
            }
        }

        // Step 3.5: Apply deferred WHERE conditions (those referencing IS-ref target vars).
        if !deferred_filter_exprs.is_empty() {
            let predicate = combine_with_and(&deferred_filter_exprs);
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate,
                optional_variables: HashSet::new(),
            };
        }

        // Collect ALONG binding names
        for along in &clause.along {
            along_bindings.push(along.name.clone());
        }

        // Validate ALONG prev-references against available yield schemas
        if !clause.along.is_empty() {
            let available_prev_fields: HashSet<&str> = is_refs
                .iter()
                .flat_map(|ir| {
                    rule_catalog
                        .get(&ir.rule_name)
                        .map(|r| {
                            r.yield_schema
                                .iter()
                                .map(|yc| yc.name.as_str())
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default()
                })
                .collect();

            for along in &clause.along {
                validate_prev_refs(&along.expr, &available_prev_fields)?;
            }
        }

        // Steps 4+5+6: ALONG + YIELD + PRIORITY as a single projection
        let along_map: HashMap<&str, &LocyExpr> = clause
            .along
            .iter()
            .map(|a| (a.name.as_str(), &a.expr))
            .collect();

        // Build FOLD output name → aggregate input expression mapping.
        // FOLD outputs (e.g., `total` from `FOLD total = SUM(r.amount)`) are not
        // columns in the MATCH output; the clause projection must include the
        // aggregate's input expression (e.g., `r.amount`) so the FOLD operator
        // can find it later.
        let fold_output_names: HashSet<&str> =
            clause.fold.iter().map(|fb| fb.name.as_str()).collect();
        let fold_input_map: HashMap<&str, &Expr> = clause
            .fold
            .iter()
            .filter_map(|fb| {
                // Extract the first argument from the aggregate function call
                if let Expr::FunctionCall { args, .. } = &fb.aggregate {
                    args.first().map(|arg| (fb.name.as_str(), arg))
                } else {
                    None
                }
            })
            .collect();

        // Build yield name → original expression mapping from clause output.
        // This preserves literals (`'low' AS label`) and property accesses
        // (`e.cost AS cost`) that would otherwise be lost as bare Variable lookups.
        let yield_expr_map: HashMap<String, &Expr> = match &clause.output {
            RuleOutput::Yield(yc) => yc
                .items
                .iter()
                .map(|item| {
                    let name = item.alias.clone().unwrap_or_else(|| match &item.expr {
                        Expr::Variable(n) => n.clone(),
                        Expr::Property(_, prop) => prop.clone(),
                        _ => "?".to_string(),
                    });
                    (name, &item.expr)
                })
                .collect(),
            _ => HashMap::new(),
        };

        let along_names_set: HashSet<&str> = clause.along.iter().map(|a| a.name.as_str()).collect();

        // Pre-compute rewritten ALONG expressions for variable substitution.
        // When a YIELD expression references an ALONG name (e.g., `ew * 2.0 AS score`
        // where `ALONG ew = e.weight`), the Variable("ew") must be replaced with the
        // underlying expression Property("e", "weight") because "ew" is not a column
        // in the input plan schema.
        let rewritten_along: HashMap<&str, Expr> = along_map
            .iter()
            .filter_map(|(&name, locy_expr)| rewrite_locy_expr(locy_expr).ok().map(|e| (name, e)))
            .collect();

        let mut projections = Vec::new();
        let mut target_types = Vec::new();
        for yc in yield_cols {
            let expr = if let Some(locy_expr) = along_map.get(yc.name.as_str()) {
                rewrite_locy_expr(locy_expr)?
            } else if let Some(fold_input) = fold_input_map.get(yc.name.as_str()) {
                (*fold_input).clone()
            } else if fold_output_names.contains(yc.name.as_str()) {
                continue;
            } else if let Some(orig_expr) = yield_expr_map.get(&yc.name) {
                let e = (*orig_expr).clone();
                substitute_along_vars(e, &rewritten_along)
            } else {
                let e = Expr::Variable(yc.name.clone());
                substitute_along_vars(e, &rewritten_along)
            };
            projections.push((expr, Some(yc.name.clone())));
            target_types.push(infer_yield_type(
                &yc.name,
                clause,
                node_vars,
                &fold_output_names,
                &along_names_set,
            ));
        }

        // Add __priority literal column if present
        if let Some(priority) = clause.priority {
            projections.push((
                Expr::Literal(CypherLiteral::Integer(priority)),
                Some("__priority".to_string()),
            ));
            target_types.push(DataType::Int64);
        }

        plan = LogicalPlan::LocyProject {
            input: Box::new(plan),
            projections,
            target_types,
        };

        // Step 7: Non-recursive FOLD — handled by apply_post_fixpoint_chain.
        //
        // Fold is always applied post-fixpoint (both recursive and non-recursive).
        // Wrapping the body with LocyFold here would double-apply the aggregate,
        // producing wrong results for COUNT/AVG where f(f(x)) ≠ f(x).

        // Step 8: BEST BY wrapping
        if let Some(best_by) = &clause.best_by {
            let key_columns: Vec<String> = yield_cols
                .iter()
                .filter(|yc| yc.is_key)
                .map(|yc| yc.name.clone())
                .collect();

            let criteria: Vec<(Expr, bool)> = best_by
                .items
                .iter()
                .map(|item| (item.expr.clone(), item.ascending))
                .collect();

            plan = LogicalPlan::LocyBestBy {
                input: Box::new(plan),
                key_columns,
                criteria,
            };
        }

        Ok(LocyClausePlan {
            body: plan,
            is_refs,
            along_bindings,
            priority: clause.priority,
        })
    }

    // -- DerivedScanHandle management -----------------------------------

    /// Get or create a derived scan handle for a rule.
    ///
    /// Handles are keyed by `(rule_name, is_self_ref)` — a self-referential
    /// scan (receives delta data) and a cross-stratum scan (receives full facts)
    /// for the same rule need separate handles.
    fn get_or_create_derived_scan_handle(
        &self,
        rule_name: &str,
        target_rule: &CompiledRule,
        is_self_ref: bool,
    ) -> DerivedScanHandle {
        let mut handles = self.derived_scan_handles.borrow_mut();

        // Reuse existing handle with same (rule_name, is_self_ref)
        if let Some(handle) = handles
            .iter()
            .find(|h| h.rule_name == rule_name && h.is_self_ref == is_self_ref)
        {
            return handle.clone();
        }

        let scan_index = handles.len();
        let schema = yield_schema_to_arrow_from_rule(target_rule);
        let data = Arc::new(RwLock::new(Vec::new()));
        let handle = DerivedScanHandle {
            rule_name: rule_name.to_string(),
            scan_index,
            is_self_ref,
            data,
            schema,
        };
        handles.push(handle.clone());
        handle
    }

    /// Convert internal handles into a [`DerivedScanRegistry`].
    fn build_registry(&self) -> DerivedScanRegistry {
        let handles = self.derived_scan_handles.borrow();
        let mut registry = DerivedScanRegistry::new();
        for handle in handles.iter() {
            registry.add(DerivedScanEntry {
                scan_index: handle.scan_index,
                rule_name: handle.rule_name.clone(),
                is_self_ref: handle.is_self_ref,
                data: handle.data.clone(),
                schema: handle.schema.clone(),
            });
        }
        registry
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Combine multiple expressions with AND.
fn combine_with_and(exprs: &[&Expr]) -> Expr {
    assert!(!exprs.is_empty());
    let mut result = exprs[0].clone();
    for expr in &exprs[1..] {
        result = Expr::BinaryOp {
            left: Box::new(result),
            op: BinaryOp::And,
            right: Box::new((*expr).clone()),
        };
    }
    result
}

/// Build the join predicate for an IS-reference.
///
/// Returns true if `e` or any sub-expression references a variable in `vars`.
///
/// Used to partition WHERE conditions into those that can be applied before
/// IS-ref joins (immediate) vs those that must wait until after (deferred),
/// when conditions reference IS-ref target variables not yet in the plan.
fn expr_references_any(e: &Expr, vars: &HashSet<String>) -> bool {
    match e {
        Expr::Variable(v) => vars.contains(v.as_str()),
        Expr::Property(inner, _) => expr_references_any(inner, vars),
        Expr::BinaryOp { left, right, .. } => {
            expr_references_any(left, vars) || expr_references_any(right, vars)
        }
        Expr::UnaryOp { expr, .. } => expr_references_any(expr, vars),
        Expr::FunctionCall { args, .. } => args.iter().any(|a| expr_references_any(a, vars)),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) | Expr::IsUnique(inner) => {
            expr_references_any(inner, vars)
        }
        Expr::In { expr, list } => {
            expr_references_any(expr, vars) || expr_references_any(list, vars)
        }
        Expr::List(exprs) => exprs.iter().any(|a| expr_references_any(a, vars)),
        Expr::Map(entries) => entries.iter().any(|(_, a)| expr_references_any(a, vars)),
        Expr::Case {
            expr,
            when_then,
            else_expr,
        } => {
            expr.as_deref()
                .is_some_and(|a| expr_references_any(a, vars))
                || when_then
                    .iter()
                    .any(|(w, t)| expr_references_any(w, vars) || expr_references_any(t, vars))
                || else_expr
                    .as_deref()
                    .is_some_and(|a| expr_references_any(a, vars))
        }
        Expr::ArrayIndex { array, index } => {
            expr_references_any(array, vars) || expr_references_any(index, vars)
        }
        Expr::ArraySlice { array, start, end } => {
            expr_references_any(array, vars)
                || start
                    .as_deref()
                    .is_some_and(|a| expr_references_any(a, vars))
                || end.as_deref().is_some_and(|a| expr_references_any(a, vars))
        }
        _ => false,
    }
}

/// Maps subjects → KEY yield columns by position, and target → remaining KEY
/// or first non-KEY yield column. For node variables, compares `._vid` property
/// (UInt64) instead of bare variable (which doesn't exist as a column).
fn build_is_ref_predicate(
    subjects: &[String],
    target: &Option<String>,
    yield_schema: &[YieldColumn],
    node_vars: &HashSet<String>,
) -> Result<Expr> {
    let key_cols: Vec<&YieldColumn> = yield_schema.iter().filter(|yc| yc.is_key).collect();
    let non_key_cols: Vec<&YieldColumn> = yield_schema.iter().filter(|yc| !yc.is_key).collect();

    let mut predicates = Vec::new();

    /// Build the expression for a variable in an IS-ref predicate.
    /// Node variables use `var._vid` as a raw column reference (UInt64);
    /// scalars use bare `var`.
    fn make_var_expr(var_name: &str, node_vars: &HashSet<String>) -> Expr {
        if node_vars.contains(var_name) {
            // Use "var._vid" as a variable name — Column::from_name won't split
            // on the dot, so DataFusion resolves this to the physical `var._vid`
            // column from the graph scan (UInt64).
            Expr::Variable(format!("{}._vid", var_name))
        } else {
            Expr::Variable(var_name.to_string())
        }
    }

    // subjects[i] = key_cols[i]
    for (i, subject) in subjects.iter().enumerate() {
        let key_col = key_cols.get(i).with_context(|| {
            format!(
                "IS-ref subject index {} exceeds KEY column count {}",
                i,
                key_cols.len()
            )
        })?;
        predicates.push(Expr::BinaryOp {
            left: Box::new(make_var_expr(subject, node_vars)),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Variable(key_col.name.clone())),
        });
    }

    // target: bind to remaining KEY column (after subjects) or first non-KEY
    if let Some(target_var) = target {
        let target_col = if subjects.len() < key_cols.len() {
            Some(key_cols[subjects.len()])
        } else {
            non_key_cols.first().copied()
        };
        if let Some(col) = target_col {
            predicates.push(Expr::BinaryOp {
                left: Box::new(make_var_expr(target_var, node_vars)),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Variable(col.name.clone())),
            });
        }
    }

    if predicates.is_empty() {
        bail!("IS-ref predicate requires at least one subject binding");
    }

    Ok(combine_with_and(&predicates.iter().collect::<Vec<_>>()))
}

/// Rewrite a [`LocyExpr`] into a Cypher [`Expr`].
///
/// `PrevRef(field)` becomes `Expr::Variable(field)` — the column is available
/// directly from the derived scan output after the CrossJoin.
pub(crate) fn rewrite_locy_expr(expr: &LocyExpr) -> Result<Expr> {
    match expr {
        LocyExpr::PrevRef(field) => Ok(Expr::Variable(field.clone())),
        LocyExpr::Cypher(e) => Ok(e.clone()),
        LocyExpr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(rewrite_locy_expr(left)?),
            op: locy_op_to_cypher_op(op),
            right: Box::new(rewrite_locy_expr(right)?),
        }),
        LocyExpr::UnaryOp(op, inner) => Ok(Expr::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_locy_expr(inner)?),
        }),
    }
}

/// Recursively substitute `Variable(name)` nodes matching ALONG binding names
/// with their rewritten expressions. This allows YIELD expressions like
/// `ew * 2.0 AS score` to reference ALONG bindings (`ALONG ew = e.weight`)
/// by inlining the underlying expression (`e.weight * 2.0`).
fn substitute_along_vars(expr: Expr, along: &HashMap<&str, Expr>) -> Expr {
    if along.is_empty() {
        return expr;
    }
    match expr {
        Expr::Variable(ref name) if along.contains_key(name.as_str()) => {
            along[name.as_str()].clone()
        }
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(substitute_along_vars(*left, along)),
            op,
            right: Box::new(substitute_along_vars(*right, along)),
        },
        Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
            op,
            expr: Box::new(substitute_along_vars(*inner, along)),
        },
        Expr::FunctionCall {
            name,
            args,
            distinct,
            window_spec,
        } => Expr::FunctionCall {
            name,
            args: args
                .into_iter()
                .map(|a| substitute_along_vars(a, along))
                .collect(),
            distinct,
            window_spec,
        },
        other => other,
    }
}

/// Map [`LocyBinaryOp`] to Cypher [`BinaryOp`].
pub(crate) fn locy_op_to_cypher_op(op: &LocyBinaryOp) -> BinaryOp {
    match op {
        LocyBinaryOp::Add => BinaryOp::Add,
        LocyBinaryOp::Sub => BinaryOp::Sub,
        LocyBinaryOp::Mul => BinaryOp::Mul,
        LocyBinaryOp::Div => BinaryOp::Div,
        LocyBinaryOp::Mod => BinaryOp::Mod,
        LocyBinaryOp::Pow => BinaryOp::Pow,
        LocyBinaryOp::And => BinaryOp::And,
        LocyBinaryOp::Or => BinaryOp::Or,
        LocyBinaryOp::Xor => BinaryOp::Xor,
    }
}

/// Build an Arrow schema from a compiled rule's yield columns using inferred types.
///
/// Computes target-rule node vars and infers types using the same logic as `build_rule`,
/// ensuring the derived scan schema matches `yield_columns_to_arrow_schema()` in
/// `locy_program.rs`.
fn yield_schema_to_arrow_from_rule(target_rule: &CompiledRule) -> SchemaRef {
    let target_node_vars = collect_node_vars(&target_rule.clauses);
    let first_clause = target_rule.clauses.first();
    let fold_names: HashSet<&str> = first_clause
        .map(|c| c.fold.iter().map(|fb| fb.name.as_str()).collect())
        .unwrap_or_default();
    let along_names: HashSet<&str> = first_clause
        .map(|c| c.along.iter().map(|a| a.name.as_str()).collect())
        .unwrap_or_default();

    let fields: Vec<Field> = target_rule
        .yield_schema
        .iter()
        .map(|yc| {
            let dt = match first_clause {
                Some(fc) => {
                    infer_yield_type(&yc.name, fc, &target_node_vars, &fold_names, &along_names)
                }
                None => DataType::LargeUtf8,
            };
            Field::new(&yc.name, dt, true)
        })
        .collect();
    Arc::new(ArrowSchema::new(fields))
}

/// Validate that all `PrevRef` fields in a [`LocyExpr`] reference columns
/// available in the IS-reference yield schemas.
fn validate_prev_refs(expr: &LocyExpr, available: &HashSet<&str>) -> Result<()> {
    match expr {
        LocyExpr::PrevRef(field) => {
            if !available.contains(field.as_str()) {
                bail!(
                    "prev.{} references field '{}' not found in any \
                     IS-reference yield schema",
                    field,
                    field,
                );
            }
            Ok(())
        }
        LocyExpr::Cypher(_) => Ok(()),
        LocyExpr::BinaryOp { left, right, .. } => {
            validate_prev_refs(left, available)?;
            validate_prev_refs(right, available)
        }
        LocyExpr::UnaryOp(_, inner) => validate_prev_refs(inner, available),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use uni_cypher::ast::{NodePattern, PathPattern, Pattern, PatternElement, UnaryOp};
    use uni_cypher::locy_ast::{
        AlongBinding, BestByClause, BestByItem, FoldBinding, IsReference, LocyBinaryOp, LocyExpr,
        LocyYieldItem, QualifiedName, RuleCondition, RuleOutput, YieldClause,
    };
    use uni_locy::types::{CompiledClause, CompiledProgram, CompiledRule, Stratum, YieldColumn};

    use crate::query::planner::LogicalPlan;

    // -- Test helpers ---------------------------------------------------

    fn test_schema() -> Arc<uni_common::core::schema::Schema> {
        Arc::new(uni_common::core::schema::Schema {
            schema_version: 1,
            labels: HashMap::new(),
            edge_types: HashMap::new(),
            properties: HashMap::new(),
            indexes: vec![],
            constraints: vec![],
            schemaless_registry: Default::default(),
        })
    }

    fn test_planner() -> QueryPlanner {
        QueryPlanner::new(test_schema())
    }

    fn yield_col(name: &str, is_key: bool) -> YieldColumn {
        YieldColumn {
            name: name.to_string(),
            is_key,
            is_prob: false,
        }
    }

    fn qname(name: &str) -> QualifiedName {
        QualifiedName {
            parts: vec![name.to_string()],
        }
    }

    /// A minimal pattern: `(n)` — single node.
    fn node_pattern(var: &str) -> Pattern {
        Pattern {
            paths: vec![PathPattern {
                variable: None,
                elements: vec![PatternElement::Node(NodePattern {
                    variable: Some(var.to_string()),
                    labels: vec![],
                    properties: None,
                    where_clause: None,
                })],
                shortest_path_mode: None,
            }],
        }
    }

    /// A two-node pattern: `(a)-[e]->(b)`
    fn edge_pattern(a: &str, e: &str, b: &str) -> Pattern {
        use uni_cypher::ast::{Direction, RelationshipPattern};
        Pattern {
            paths: vec![PathPattern {
                variable: None,
                elements: vec![
                    PatternElement::Node(NodePattern {
                        variable: Some(a.to_string()),
                        labels: vec![],
                        properties: None,
                        where_clause: None,
                    }),
                    PatternElement::Relationship(RelationshipPattern {
                        variable: Some(e.to_string()),
                        types: vec![],
                        direction: Direction::Outgoing,
                        range: None,
                        properties: None,
                        where_clause: None,
                    }),
                    PatternElement::Node(NodePattern {
                        variable: Some(b.to_string()),
                        labels: vec![],
                        properties: None,
                        where_clause: None,
                    }),
                ],
                shortest_path_mode: None,
            }],
        }
    }

    fn simple_yield_output(names: &[&str]) -> RuleOutput {
        RuleOutput::Yield(YieldClause {
            items: names
                .iter()
                .map(|n| LocyYieldItem {
                    is_key: false,
                    is_prob: false,
                    expr: Expr::Variable(n.to_string()),
                    alias: None,
                })
                .collect(),
        })
    }

    fn simple_clause(pattern: Pattern, yield_names: &[&str]) -> CompiledClause {
        CompiledClause {
            match_pattern: pattern,
            where_conditions: vec![],
            along: vec![],
            fold: vec![],
            best_by: None,
            output: simple_yield_output(yield_names),
            priority: None,
        }
    }

    /// Create a minimal CompiledRule for use in derived scan handle tests.
    fn test_compiled_rule(yield_cols: &[YieldColumn]) -> CompiledRule {
        CompiledRule {
            name: "test".to_string(),
            clauses: vec![simple_clause(node_pattern("n"), &[])],
            yield_schema: yield_cols.to_vec(),
            priority: None,
        }
    }

    fn make_program(
        strata: Vec<Stratum>,
        rule_catalog: HashMap<String, CompiledRule>,
    ) -> CompiledProgram {
        CompiledProgram {
            strata,
            rule_catalog,
            warnings: vec![],
            commands: vec![],
        }
    }

    fn make_rule(
        name: &str,
        clauses: Vec<CompiledClause>,
        yield_schema: Vec<YieldColumn>,
    ) -> CompiledRule {
        CompiledRule {
            name: name.to_string(),
            clauses,
            yield_schema,
            priority: None,
        }
    }

    fn plan_is_project(plan: &LogicalPlan) -> bool {
        matches!(
            plan,
            LogicalPlan::Project { .. } | LogicalPlan::LocyProject { .. }
        )
    }

    fn plan_is_filter(plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::Filter { .. })
    }

    fn plan_is_cross_join(plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::CrossJoin { .. })
    }

    fn plan_is_derived_scan(plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::LocyDerivedScan { .. })
    }

    fn plan_is_fold(plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::LocyFold { .. })
    }

    fn plan_is_best_by(plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::LocyBestBy { .. })
    }

    // ===================================================================
    // Unit Tests — LocyExpr Rewriting
    // ===================================================================

    #[test]
    fn test_rewrite_prev_ref() {
        let expr = LocyExpr::PrevRef("cost".to_string());
        let result = rewrite_locy_expr(&expr).unwrap();
        assert_eq!(result, Expr::Variable("cost".to_string()));
    }

    #[test]
    fn test_rewrite_cypher_passthrough() {
        let inner = Expr::Literal(CypherLiteral::Integer(42));
        let expr = LocyExpr::Cypher(inner.clone());
        let result = rewrite_locy_expr(&expr).unwrap();
        assert_eq!(result, inner);
    }

    #[test]
    fn test_rewrite_binary_add() {
        let expr = LocyExpr::BinaryOp {
            left: Box::new(LocyExpr::PrevRef("x".to_string())),
            op: LocyBinaryOp::Add,
            right: Box::new(LocyExpr::Cypher(Expr::Literal(CypherLiteral::Integer(1)))),
        };
        let result = rewrite_locy_expr(&expr).unwrap();
        assert_eq!(
            result,
            Expr::BinaryOp {
                left: Box::new(Expr::Variable("x".to_string())),
                op: BinaryOp::Add,
                right: Box::new(Expr::Literal(CypherLiteral::Integer(1))),
            }
        );
    }

    #[test]
    fn test_rewrite_nested_binary() {
        // (prev.a + prev.b) * prev.c
        let expr = LocyExpr::BinaryOp {
            left: Box::new(LocyExpr::BinaryOp {
                left: Box::new(LocyExpr::PrevRef("a".to_string())),
                op: LocyBinaryOp::Add,
                right: Box::new(LocyExpr::PrevRef("b".to_string())),
            }),
            op: LocyBinaryOp::Mul,
            right: Box::new(LocyExpr::PrevRef("c".to_string())),
        };
        let result = rewrite_locy_expr(&expr).unwrap();
        let expected = Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Variable("a".to_string())),
                op: BinaryOp::Add,
                right: Box::new(Expr::Variable("b".to_string())),
            }),
            op: BinaryOp::Mul,
            right: Box::new(Expr::Variable("c".to_string())),
        };
        assert_eq!(result, expected);
    }

    #[test]
    fn test_rewrite_unary_not() {
        let expr = LocyExpr::UnaryOp(
            UnaryOp::Not,
            Box::new(LocyExpr::Cypher(Expr::Variable("x".to_string()))),
        );
        let result = rewrite_locy_expr(&expr).unwrap();
        assert_eq!(
            result,
            Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: Box::new(Expr::Variable("x".to_string())),
            }
        );
    }

    #[test]
    fn test_locy_op_to_cypher_op_all() {
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Add), BinaryOp::Add);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Sub), BinaryOp::Sub);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Mul), BinaryOp::Mul);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Div), BinaryOp::Div);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Mod), BinaryOp::Mod);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Pow), BinaryOp::Pow);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::And), BinaryOp::And);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Or), BinaryOp::Or);
        assert_eq!(locy_op_to_cypher_op(&LocyBinaryOp::Xor), BinaryOp::Xor);
    }

    // ===================================================================
    // Unit Tests — DerivedScanHandle Registry
    // ===================================================================

    #[test]
    fn test_handle_allocation_new() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);
        let cols = [yield_col("n", true), yield_col("m", false)];
        let rule = test_compiled_rule(&cols);
        let handle = builder.get_or_create_derived_scan_handle("reachable", &rule, false);
        assert_eq!(handle.scan_index, 0);
        assert_eq!(handle.rule_name, "reachable");
        assert!(handle.data.read().is_empty());
    }

    #[test]
    fn test_handle_reuse_same_rule() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);
        let cols = [yield_col("n", true), yield_col("m", false)];
        let rule = test_compiled_rule(&cols);
        let h1 = builder.get_or_create_derived_scan_handle("reachable", &rule, false);
        let h2 = builder.get_or_create_derived_scan_handle("reachable", &rule, false);
        assert!(Arc::ptr_eq(&h1.data, &h2.data));
        assert_eq!(h1.scan_index, h2.scan_index);
    }

    #[test]
    fn test_handle_different_rules() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);
        let cols = [yield_col("n", true)];
        let rule = test_compiled_rule(&cols);
        let h1 = builder.get_or_create_derived_scan_handle("reachable", &rule, false);
        let h2 = builder.get_or_create_derived_scan_handle("connected", &rule, false);
        assert_eq!(h1.scan_index, 0);
        assert_eq!(h2.scan_index, 1);
        assert!(!Arc::ptr_eq(&h1.data, &h2.data));
    }

    #[test]
    fn test_registry_conversion() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);
        let cols = [yield_col("n", true)];
        let rule = test_compiled_rule(&cols);
        builder.get_or_create_derived_scan_handle("rule_a", &rule, false);
        builder.get_or_create_derived_scan_handle("rule_b", &rule, true);

        let registry = builder.build_registry();
        assert!(registry.get(0).is_some());
        assert!(registry.get(1).is_some());
        assert_eq!(registry.get(0).unwrap().rule_name, "rule_a");
        assert_eq!(registry.get(1).unwrap().rule_name, "rule_b");
    }

    #[test]
    fn test_registry_self_ref_flag() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);
        let cols = [yield_col("n", true)];
        let rule = test_compiled_rule(&cols);
        builder.get_or_create_derived_scan_handle("self_rule", &rule, true);
        builder.get_or_create_derived_scan_handle("cross_rule", &rule, false);

        let registry = builder.build_registry();
        assert!(registry.get(0).unwrap().is_self_ref);
        assert!(!registry.get(1).unwrap().is_self_ref);
    }

    // ===================================================================
    // Unit Tests — build_stratum
    // ===================================================================

    #[test]
    fn test_non_recursive_stratum() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "base",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let mut catalog = HashMap::new();
        catalog.insert("base".to_string(), rule.clone());

        let stratum = Stratum {
            id: 0,
            rules: vec![rule],
            is_recursive: false,
            depends_on: vec![],
        };
        let names: HashSet<String> = ["base".to_string()].into();
        let result = builder
            .build_stratum(&stratum, &catalog, &names, false, 1e-15)
            .unwrap();

        assert_eq!(result.id, 0);
        assert!(!result.is_recursive);
        assert_eq!(result.rules.len(), 1);
        assert_eq!(result.rules[0].name, "base");
    }

    #[test]
    fn test_recursive_stratum() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "reach",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let mut catalog = HashMap::new();
        catalog.insert("reach".to_string(), rule.clone());

        let stratum = Stratum {
            id: 1,
            rules: vec![rule],
            is_recursive: true,
            depends_on: vec![0],
        };
        let names: HashSet<String> = ["reach".to_string()].into();
        let result = builder
            .build_stratum(&stratum, &catalog, &names, false, 1e-15)
            .unwrap();

        assert!(result.is_recursive);
        assert_eq!(result.depends_on, vec![0]);
    }

    #[test]
    fn test_stratum_depends_on() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "derived",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let mut catalog = HashMap::new();
        catalog.insert("derived".to_string(), rule.clone());

        let stratum = Stratum {
            id: 2,
            rules: vec![rule],
            is_recursive: false,
            depends_on: vec![0, 1],
        };
        let names: HashSet<String> = ["derived".to_string()].into();
        let result = builder
            .build_stratum(&stratum, &catalog, &names, false, 1e-15)
            .unwrap();

        assert_eq!(result.depends_on, vec![0, 1]);
    }

    #[test]
    fn test_stratum_multiple_rules() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rules: Vec<CompiledRule> = ["a", "b", "c"]
            .iter()
            .map(|name| {
                make_rule(
                    name,
                    vec![simple_clause(node_pattern("n"), &["n"])],
                    vec![yield_col("n", true)],
                )
            })
            .collect();
        let mut catalog = HashMap::new();
        for r in &rules {
            catalog.insert(r.name.clone(), r.clone());
        }

        let stratum = Stratum {
            id: 0,
            rules: rules.clone(),
            is_recursive: true,
            depends_on: vec![],
        };
        let names: HashSet<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let result = builder
            .build_stratum(&stratum, &catalog, &names, false, 1e-15)
            .unwrap();

        assert_eq!(result.rules.len(), 3);
        let rule_names: Vec<&str> = result.rules.iter().map(|r| r.name.as_str()).collect();
        assert!(rule_names.contains(&"a"));
        assert!(rule_names.contains(&"b"));
        assert!(rule_names.contains(&"c"));
    }

    // ===================================================================
    // Unit Tests — build_rule
    // ===================================================================

    #[test]
    fn test_rule_name_and_schema() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "reachable",
            vec![simple_clause(node_pattern("n"), &["n", "m"])],
            vec![yield_col("n", true), yield_col("m", false)],
        );
        let catalog = HashMap::from([("reachable".to_string(), rule.clone())]);
        let names: HashSet<String> = ["reachable".to_string()].into();

        let result = builder
            .build_rule(&rule, false, &names, &catalog, false, 1e-15)
            .unwrap();
        assert_eq!(result.name, "reachable");
        assert_eq!(result.yield_schema.len(), 2);
        assert_eq!(result.yield_schema[0].name, "n");
        assert!(result.yield_schema[0].is_key);
        assert_eq!(result.yield_schema[1].name, "m");
        assert!(!result.yield_schema[1].is_key);
    }

    #[test]
    fn test_rule_priority() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let mut rule = make_rule(
            "prio",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        rule.priority = Some(5);
        let catalog = HashMap::from([("prio".to_string(), rule.clone())]);
        let names: HashSet<String> = ["prio".to_string()].into();

        let result = builder
            .build_rule(&rule, false, &names, &catalog, false, 1e-15)
            .unwrap();
        assert_eq!(result.priority, Some(5));
    }

    #[test]
    fn test_rule_no_priority() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "noprio",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([("noprio".to_string(), rule.clone())]);
        let names: HashSet<String> = ["noprio".to_string()].into();

        let result = builder
            .build_rule(&rule, false, &names, &catalog, false, 1e-15)
            .unwrap();
        assert_eq!(result.priority, None);
    }

    #[test]
    fn test_rule_multiple_clauses() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "multi",
            vec![
                simple_clause(node_pattern("n"), &["n"]),
                simple_clause(node_pattern("n"), &["n"]),
                simple_clause(node_pattern("n"), &["n"]),
            ],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([("multi".to_string(), rule.clone())]);
        let names: HashSet<String> = ["multi".to_string()].into();

        let result = builder
            .build_rule(&rule, false, &names, &catalog, false, 1e-15)
            .unwrap();
        assert_eq!(result.clauses.len(), 3);
    }

    #[test]
    fn test_rule_yield_schema_key_flags() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "keyed",
            vec![simple_clause(node_pattern("n"), &["a", "b", "c"])],
            vec![
                yield_col("a", true),
                yield_col("b", false),
                yield_col("c", true),
            ],
        );
        let catalog = HashMap::from([("keyed".to_string(), rule.clone())]);
        let names: HashSet<String> = ["keyed".to_string()].into();

        let result = builder
            .build_rule(&rule, false, &names, &catalog, false, 1e-15)
            .unwrap();
        assert!(result.yield_schema[0].is_key);
        assert!(!result.yield_schema[1].is_key);
        assert!(result.yield_schema[2].is_key);
    }

    // ===================================================================
    // Integration Tests — build_clause
    // ===================================================================

    #[test]
    fn test_clause_simple_match_yield() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let clause = simple_clause(node_pattern("n"), &["n"]);
        let yield_cols = [yield_col("n", true)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                false,
                &names,
                &catalog,
                &HashSet::new(),
                false,
                1e-15,
            )
            .unwrap();

        assert!(plan_is_project(&result.body));
        assert!(result.is_refs.is_empty());
        assert!(result.along_bindings.is_empty());
    }

    #[test]
    fn test_clause_with_where_filter() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let clause = CompiledClause {
            match_pattern: node_pattern("n"),
            where_conditions: vec![RuleCondition::Expression(Expr::BinaryOp {
                left: Box::new(Expr::Variable("n".to_string())),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Literal(CypherLiteral::Integer(21))),
            })],
            along: vec![],
            fold: vec![],
            best_by: None,
            output: simple_yield_output(&["n"]),
            priority: None,
        };
        let yield_cols = [yield_col("n", true)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                false,
                &names,
                &catalog,
                &HashSet::new(),
                false,
                1e-15,
            )
            .unwrap();

        // Project wraps Filter wraps Scan
        assert!(plan_is_project(&result.body));
        if let LogicalPlan::LocyProject { input, .. } | LogicalPlan::Project { input, .. } =
            &result.body
        {
            assert!(plan_is_filter(input));
        }
    }

    #[test]
    fn test_clause_with_single_is_ref() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "reachable",
            vec![simple_clause(node_pattern("n"), &["n", "m"])],
            vec![yield_col("n", true), yield_col("m", false)],
        );
        let catalog = HashMap::from([("reachable".to_string(), target_rule)]);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["x".to_string()],
                rule_name: qname("reachable"),
                target: None,
                negated: false,
            })],
            along: vec![],
            fold: vec![],
            best_by: None,
            output: simple_yield_output(&["x"]),
            priority: None,
        };
        let yield_cols = [yield_col("x", true)];
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                false,
                &names,
                &catalog,
                &HashSet::new(),
                false,
                1e-15,
            )
            .unwrap();

        assert_eq!(result.is_refs.len(), 1);
        assert_eq!(result.is_refs[0].rule_name, "reachable");
        assert!(!result.is_refs[0].negated);

        // Structure: Project { Filter { CrossJoin { Scan, DerivedScan } } }
        assert!(plan_is_project(&result.body));
        if let LogicalPlan::LocyProject { input, .. } | LogicalPlan::Project { input, .. } =
            &result.body
        {
            assert!(plan_is_filter(input));
            if let LogicalPlan::Filter { input, .. } = input.as_ref() {
                assert!(plan_is_cross_join(input));
                if let LogicalPlan::CrossJoin { right, .. } = input.as_ref() {
                    assert!(plan_is_derived_scan(right));
                }
            }
        }
    }

    #[test]
    fn test_clause_with_is_ref_to_target() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "reachable",
            vec![simple_clause(node_pattern("n"), &["n", "m"])],
            vec![yield_col("n", true), yield_col("m", false)],
        );
        let catalog = HashMap::from([("reachable".to_string(), target_rule)]);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["x".to_string()],
                rule_name: qname("reachable"),
                target: Some("y".to_string()),
                negated: false,
            })],
            along: vec![],
            fold: vec![],
            best_by: None,
            output: simple_yield_output(&["x", "y"]),
            priority: None,
        };
        let yield_cols = [yield_col("x", true), yield_col("y", false)];
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                false,
                &names,
                &catalog,
                &HashSet::new(),
                false,
                1e-15,
            )
            .unwrap();

        assert_eq!(result.is_refs.len(), 1);
        assert_eq!(
            result.is_refs[0].target,
            Some(Expr::Variable("y".to_string()))
        );
    }

    #[test]
    fn test_clause_with_negated_is_ref() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "reachable",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([("reachable".to_string(), target_rule)]);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["x".to_string()],
                rule_name: qname("reachable"),
                target: None,
                negated: true,
            })],
            along: vec![],
            fold: vec![],
            best_by: None,
            output: simple_yield_output(&["x"]),
            priority: None,
        };
        let yield_cols = [yield_col("x", true)];
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                false,
                &names,
                &catalog,
                &HashSet::new(),
                false,
                1e-15,
            )
            .unwrap();

        assert_eq!(result.is_refs.len(), 1);
        assert!(result.is_refs[0].negated);

        // No CrossJoin for negated — anti-join deferred to fixpoint
        assert!(plan_is_project(&result.body));
        if let LogicalPlan::LocyProject { input, .. } | LogicalPlan::Project { input, .. } =
            &result.body
        {
            assert!(!plan_is_cross_join(input));
            assert!(!plan_is_filter(input));
        }
    }

    #[test]
    fn test_clause_with_multiple_is_refs() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule_a = make_rule(
            "reachable",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let rule_b = make_rule(
            "connected",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([
            ("reachable".to_string(), rule_a),
            ("connected".to_string(), rule_b),
        ]);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![
                RuleCondition::IsReference(IsReference {
                    subjects: vec!["x".to_string()],
                    rule_name: qname("reachable"),
                    target: None,
                    negated: false,
                }),
                RuleCondition::IsReference(IsReference {
                    subjects: vec!["x".to_string()],
                    rule_name: qname("connected"),
                    target: None,
                    negated: false,
                }),
            ],
            along: vec![],
            fold: vec![],
            best_by: None,
            output: simple_yield_output(&["x"]),
            priority: None,
        };
        let yield_cols = [yield_col("x", true)];
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                false,
                &names,
                &catalog,
                &HashSet::new(),
                false,
                1e-15,
            )
            .unwrap();

        assert_eq!(result.is_refs.len(), 2);
        assert_eq!(result.is_refs[0].rule_name, "reachable");
        assert_eq!(result.is_refs[1].rule_name, "connected");
    }

    #[test]
    fn test_clause_with_along() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "reachable",
            vec![simple_clause(node_pattern("n"), &["n", "cost"])],
            vec![yield_col("n", true), yield_col("cost", false)],
        );
        let catalog = HashMap::from([("reachable".to_string(), target_rule)]);

        let clause = CompiledClause {
            match_pattern: edge_pattern("a", "e", "b"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["a".to_string()],
                rule_name: qname("reachable"),
                target: None,
                negated: false,
            })],
            along: vec![AlongBinding {
                name: "cost".to_string(),
                expr: LocyExpr::BinaryOp {
                    left: Box::new(LocyExpr::PrevRef("cost".to_string())),
                    op: LocyBinaryOp::Add,
                    right: Box::new(LocyExpr::Cypher(Expr::Literal(CypherLiteral::Integer(1)))),
                },
            }],
            fold: vec![],
            best_by: None,
            output: simple_yield_output(&["a", "b", "cost"]),
            priority: None,
        };
        let yield_cols = [
            yield_col("a", true),
            yield_col("b", false),
            yield_col("cost", false),
        ];
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                false,
                &names,
                &catalog,
                &HashSet::new(),
                false,
                1e-15,
            )
            .unwrap();

        assert_eq!(result.along_bindings, vec!["cost".to_string()]);

        // The YIELD projection should contain the rewritten ALONG expression
        if let LogicalPlan::Project { projections, .. } = &result.body {
            let cost_proj = projections
                .iter()
                .find(|(_, alias)| alias.as_deref() == Some("cost"))
                .unwrap();
            assert!(matches!(cost_proj.0, Expr::BinaryOp { .. }));
        }
    }

    #[test]
    fn test_clause_with_fold_non_recursive() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let clause = CompiledClause {
            match_pattern: node_pattern("n"),
            where_conditions: vec![],
            along: vec![],
            fold: vec![FoldBinding {
                name: "total".to_string(),
                aggregate: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Variable("cost".to_string())],
                    distinct: false,
                    window_spec: None,
                },
            }],
            best_by: None,
            output: simple_yield_output(&["n", "total"]),
            priority: None,
        };
        let yield_cols = [yield_col("n", true), yield_col("total", false)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                false,
                &names,
                &catalog,
                &HashSet::new(),
                false,
                1e-15,
            )
            .unwrap();

        // Fold is deferred to apply_post_fixpoint_chain (not in body)
        assert!(!plan_is_fold(&result.body));
        assert!(plan_is_project(&result.body));
    }

    #[test]
    fn test_clause_fold_skipped_recursive() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let clause = CompiledClause {
            match_pattern: node_pattern("n"),
            where_conditions: vec![],
            along: vec![],
            fold: vec![FoldBinding {
                name: "total".to_string(),
                aggregate: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Variable("cost".to_string())],
                    distinct: false,
                    window_spec: None,
                },
            }],
            best_by: None,
            output: simple_yield_output(&["n", "total"]),
            priority: None,
        };
        let yield_cols = [yield_col("n", true), yield_col("total", false)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                true,
                &names,
                &catalog,
                &HashSet::new(),
                false,
                1e-15,
            )
            .unwrap();

        // Recursive: no LocyFold (deferred to FixpointExec)
        assert!(!plan_is_fold(&result.body));
        assert!(plan_is_project(&result.body));
    }

    #[test]
    fn test_clause_with_best_by() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let clause = CompiledClause {
            match_pattern: node_pattern("n"),
            where_conditions: vec![],
            along: vec![],
            fold: vec![],
            best_by: Some(BestByClause {
                items: vec![BestByItem {
                    expr: Expr::Variable("cost".to_string()),
                    ascending: true,
                }],
            }),
            output: simple_yield_output(&["n", "cost"]),
            priority: None,
        };
        let yield_cols = [yield_col("n", true), yield_col("cost", false)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                false,
                &names,
                &catalog,
                &HashSet::new(),
                false,
                1e-15,
            )
            .unwrap();

        assert!(plan_is_best_by(&result.body));
        if let LogicalPlan::LocyBestBy {
            key_columns,
            criteria,
            ..
        } = &result.body
        {
            assert_eq!(key_columns, &["n".to_string()]);
            assert_eq!(criteria.len(), 1);
            assert!(criteria[0].1); // ascending
        }
    }

    #[test]
    fn test_clause_with_priority() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let mut clause = simple_clause(node_pattern("n"), &["n"]);
        clause.priority = Some(3);
        let yield_cols = [yield_col("n", true)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                false,
                &names,
                &catalog,
                &HashSet::new(),
                false,
                1e-15,
            )
            .unwrap();

        assert_eq!(result.priority, Some(3));
        if let LogicalPlan::Project { projections, .. } = &result.body {
            let prio = projections
                .iter()
                .find(|(_, alias)| alias.as_deref() == Some("__priority"));
            assert!(prio.is_some());
            if let Some((Expr::Literal(CypherLiteral::Integer(v)), _)) = prio {
                assert_eq!(*v, 3);
            }
        }
    }

    #[test]
    fn test_clause_mixed_features() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "reachable",
            vec![simple_clause(node_pattern("n"), &["n", "cost"])],
            vec![yield_col("n", true), yield_col("cost", false)],
        );
        let catalog = HashMap::from([("reachable".to_string(), target_rule)]);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["x".to_string()],
                rule_name: qname("reachable"),
                target: None,
                negated: false,
            })],
            along: vec![AlongBinding {
                name: "cost".to_string(),
                expr: LocyExpr::PrevRef("cost".to_string()),
            }],
            fold: vec![FoldBinding {
                name: "total".to_string(),
                aggregate: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Variable("cost".to_string())],
                    distinct: false,
                    window_spec: None,
                },
            }],
            best_by: Some(BestByClause {
                items: vec![BestByItem {
                    expr: Expr::Variable("total".to_string()),
                    ascending: true,
                }],
            }),
            output: simple_yield_output(&["x", "cost", "total"]),
            priority: None,
        };
        let yield_cols = [
            yield_col("x", true),
            yield_col("cost", false),
            yield_col("total", false),
        ];
        let names = HashSet::new();

        let result = builder
            .build_clause(
                &clause,
                &yield_cols,
                false,
                &names,
                &catalog,
                &HashSet::new(),
                false,
                1e-15,
            )
            .unwrap();

        // Layered: BestBy { Project { Filter { CrossJoin { .. } } } }
        // (Fold is deferred to apply_post_fixpoint_chain)
        assert!(plan_is_best_by(&result.body));
        if let LogicalPlan::LocyBestBy { input, .. } = &result.body {
            assert!(plan_is_project(input));
        }
        assert_eq!(result.is_refs.len(), 1);
        assert_eq!(result.along_bindings, vec!["cost".to_string()]);
    }

    // ===================================================================
    // Integration Tests — build_program_plan
    // ===================================================================

    #[test]
    fn test_program_single_non_recursive_stratum() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "base",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([("base".to_string(), rule.clone())]);
        let program = make_program(
            vec![Stratum {
                id: 0,
                rules: vec![rule],
                is_recursive: false,
                depends_on: vec![],
            }],
            catalog,
        );

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();

        if let LogicalPlan::LocyProgram {
            strata, commands, ..
        } = &plan
        {
            assert_eq!(strata.len(), 1);
            assert!(!strata[0].is_recursive);
            assert!(commands.is_empty());
        } else {
            panic!("Expected LocyProgram");
        }
    }

    #[test]
    fn test_program_single_recursive_stratum() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule = make_rule(
            "reach",
            vec![CompiledClause {
                match_pattern: edge_pattern("a", "e", "b"),
                where_conditions: vec![RuleCondition::IsReference(IsReference {
                    subjects: vec!["a".to_string()],
                    rule_name: qname("reach"),
                    target: None,
                    negated: false,
                })],
                along: vec![],
                fold: vec![],
                best_by: None,
                output: simple_yield_output(&["a", "b"]),
                priority: None,
            }],
            vec![yield_col("a", true), yield_col("b", false)],
        );
        let catalog = HashMap::from([("reach".to_string(), rule.clone())]);
        let program = make_program(
            vec![Stratum {
                id: 0,
                rules: vec![rule],
                is_recursive: true,
                depends_on: vec![],
            }],
            catalog,
        );

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();
        let registry = if let LogicalPlan::LocyProgram {
            derived_scan_registry,
            ..
        } = &plan
        {
            derived_scan_registry.clone()
        } else {
            panic!("Expected LocyProgram")
        };

        let entry = registry.get(0).expect("should have scan entry");
        assert!(entry.is_self_ref);
        assert_eq!(entry.rule_name, "reach");
    }

    #[test]
    fn test_program_two_strata_topological() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let base_rule = make_rule(
            "base",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let derived_rule = make_rule(
            "derived",
            vec![CompiledClause {
                match_pattern: node_pattern("x"),
                where_conditions: vec![RuleCondition::IsReference(IsReference {
                    subjects: vec!["x".to_string()],
                    rule_name: qname("base"),
                    target: None,
                    negated: false,
                })],
                along: vec![],
                fold: vec![],
                best_by: None,
                output: simple_yield_output(&["x"]),
                priority: None,
            }],
            vec![yield_col("x", true)],
        );
        let catalog = HashMap::from([
            ("base".to_string(), base_rule.clone()),
            ("derived".to_string(), derived_rule.clone()),
        ]);
        let program = make_program(
            vec![
                Stratum {
                    id: 0,
                    rules: vec![base_rule],
                    is_recursive: false,
                    depends_on: vec![],
                },
                Stratum {
                    id: 1,
                    rules: vec![derived_rule],
                    is_recursive: false,
                    depends_on: vec![0],
                },
            ],
            catalog,
        );

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();

        if let LogicalPlan::LocyProgram { strata, .. } = &plan {
            assert_eq!(strata.len(), 2);
            assert_eq!(strata[0].id, 0);
            assert_eq!(strata[1].id, 1);
            assert_eq!(strata[1].depends_on, vec![0]);
        } else {
            panic!("Expected LocyProgram");
        }
    }

    #[test]
    fn test_program_cross_stratum_is_ref() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let base_rule = make_rule(
            "base",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let user_rule = make_rule(
            "user",
            vec![CompiledClause {
                match_pattern: node_pattern("x"),
                where_conditions: vec![RuleCondition::IsReference(IsReference {
                    subjects: vec!["x".to_string()],
                    rule_name: qname("base"),
                    target: None,
                    negated: false,
                })],
                along: vec![],
                fold: vec![],
                best_by: None,
                output: simple_yield_output(&["x"]),
                priority: None,
            }],
            vec![yield_col("x", true)],
        );
        let catalog = HashMap::from([
            ("base".to_string(), base_rule.clone()),
            ("user".to_string(), user_rule.clone()),
        ]);
        let program = make_program(
            vec![
                Stratum {
                    id: 0,
                    rules: vec![base_rule],
                    is_recursive: false,
                    depends_on: vec![],
                },
                Stratum {
                    id: 1,
                    rules: vec![user_rule],
                    is_recursive: false,
                    depends_on: vec![0],
                },
            ],
            catalog,
        );

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();
        let registry = if let LogicalPlan::LocyProgram {
            derived_scan_registry,
            ..
        } = &plan
        {
            derived_scan_registry.clone()
        } else {
            panic!("Expected LocyProgram")
        };

        let entry = registry.get(0).expect("should have scan entry");
        assert!(!entry.is_self_ref);
    }

    #[test]
    fn test_program_multi_rule_stratum() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let rule_a = make_rule(
            "rule_a",
            vec![CompiledClause {
                match_pattern: node_pattern("n"),
                where_conditions: vec![RuleCondition::IsReference(IsReference {
                    subjects: vec!["n".to_string()],
                    rule_name: qname("rule_b"),
                    target: None,
                    negated: false,
                })],
                along: vec![],
                fold: vec![],
                best_by: None,
                output: simple_yield_output(&["n"]),
                priority: None,
            }],
            vec![yield_col("n", true)],
        );
        let rule_b = make_rule(
            "rule_b",
            vec![CompiledClause {
                match_pattern: node_pattern("n"),
                where_conditions: vec![RuleCondition::IsReference(IsReference {
                    subjects: vec!["n".to_string()],
                    rule_name: qname("rule_a"),
                    target: None,
                    negated: false,
                })],
                along: vec![],
                fold: vec![],
                best_by: None,
                output: simple_yield_output(&["n"]),
                priority: None,
            }],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([
            ("rule_a".to_string(), rule_a.clone()),
            ("rule_b".to_string(), rule_b.clone()),
        ]);
        let program = make_program(
            vec![Stratum {
                id: 0,
                rules: vec![rule_a, rule_b],
                is_recursive: true,
                depends_on: vec![],
            }],
            catalog,
        );

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();
        let registry = if let LogicalPlan::LocyProgram {
            derived_scan_registry,
            ..
        } = &plan
        {
            derived_scan_registry.clone()
        } else {
            panic!("Expected LocyProgram")
        };

        if let LogicalPlan::LocyProgram { strata, .. } = &plan {
            assert_eq!(strata[0].rules.len(), 2);
        }

        let entries_a = registry.entries_for_rule("rule_a");
        let entries_b = registry.entries_for_rule("rule_b");
        assert_eq!(entries_a.len(), 1);
        assert_eq!(entries_b.len(), 1);
    }

    #[test]
    fn test_program_registry_arc_sharing() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "shared",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let user_rule = make_rule(
            "user",
            vec![
                CompiledClause {
                    match_pattern: node_pattern("x"),
                    where_conditions: vec![RuleCondition::IsReference(IsReference {
                        subjects: vec!["x".to_string()],
                        rule_name: qname("shared"),
                        target: None,
                        negated: false,
                    })],
                    along: vec![],
                    fold: vec![],
                    best_by: None,
                    output: simple_yield_output(&["x"]),
                    priority: None,
                },
                CompiledClause {
                    match_pattern: node_pattern("y"),
                    where_conditions: vec![RuleCondition::IsReference(IsReference {
                        subjects: vec!["y".to_string()],
                        rule_name: qname("shared"),
                        target: None,
                        negated: false,
                    })],
                    along: vec![],
                    fold: vec![],
                    best_by: None,
                    output: simple_yield_output(&["y"]),
                    priority: None,
                },
            ],
            vec![yield_col("x", true)],
        );
        let catalog = HashMap::from([
            ("shared".to_string(), target_rule.clone()),
            ("user".to_string(), user_rule.clone()),
        ]);
        let program = make_program(
            vec![
                Stratum {
                    id: 0,
                    rules: vec![target_rule],
                    is_recursive: false,
                    depends_on: vec![],
                },
                Stratum {
                    id: 1,
                    rules: vec![user_rule],
                    is_recursive: false,
                    depends_on: vec![0],
                },
            ],
            catalog,
        );

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();
        let registry = if let LogicalPlan::LocyProgram {
            derived_scan_registry,
            ..
        } = &plan
        {
            derived_scan_registry.clone()
        } else {
            panic!("Expected LocyProgram")
        };

        // Both clauses should share the same Arc<RwLock> via the registry
        let entries = registry.entries_for_rule("shared");
        assert_eq!(entries.len(), 1);

        // Verify both DerivedScan nodes in the plan share the same data handle
        if let LogicalPlan::LocyProgram { strata, .. } = &plan {
            let user_strat = &strata[1];
            let clauses = &user_strat.rules[0].clauses;
            assert_eq!(clauses.len(), 2);

            fn extract_derived_scan_data(
                plan: &LogicalPlan,
            ) -> Option<Arc<RwLock<Vec<RecordBatch>>>> {
                match plan {
                    LogicalPlan::LocyDerivedScan { data, .. } => Some(data.clone()),
                    LogicalPlan::CrossJoin { left, right } => {
                        extract_derived_scan_data(left).or_else(|| extract_derived_scan_data(right))
                    }
                    LogicalPlan::Filter { input, .. }
                    | LogicalPlan::Project { input, .. }
                    | LogicalPlan::LocyProject { input, .. } => extract_derived_scan_data(input),
                    _ => None,
                }
            }

            let data1 = extract_derived_scan_data(&clauses[0].body)
                .expect("clause 0 should have DerivedScan");
            let data2 = extract_derived_scan_data(&clauses[1].body)
                .expect("clause 1 should have DerivedScan");
            assert!(Arc::ptr_eq(&data1, &data2));
        }
    }

    #[test]
    fn test_program_empty_strata() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let program = make_program(vec![], HashMap::new());

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();
        let registry = if let LogicalPlan::LocyProgram {
            derived_scan_registry,
            ..
        } = &plan
        {
            derived_scan_registry.clone()
        } else {
            panic!("Expected LocyProgram")
        };

        if let LogicalPlan::LocyProgram {
            strata, commands, ..
        } = &plan
        {
            assert!(strata.is_empty());
            assert!(commands.is_empty());
        } else {
            panic!("Expected LocyProgram");
        }

        assert!(registry.get(0).is_none());
    }

    #[test]
    fn test_program_complex_3_strata() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let base = make_rule(
            "base",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let intermediate = make_rule(
            "intermediate",
            vec![CompiledClause {
                match_pattern: node_pattern("x"),
                where_conditions: vec![RuleCondition::IsReference(IsReference {
                    subjects: vec!["x".to_string()],
                    rule_name: qname("base"),
                    target: None,
                    negated: false,
                })],
                along: vec![],
                fold: vec![],
                best_by: None,
                output: simple_yield_output(&["x"]),
                priority: None,
            }],
            vec![yield_col("x", true)],
        );
        let recursive = make_rule(
            "recursive",
            vec![CompiledClause {
                match_pattern: node_pattern("y"),
                where_conditions: vec![
                    RuleCondition::IsReference(IsReference {
                        subjects: vec!["y".to_string()],
                        rule_name: qname("intermediate"),
                        target: None,
                        negated: false,
                    }),
                    RuleCondition::IsReference(IsReference {
                        subjects: vec!["y".to_string()],
                        rule_name: qname("recursive"),
                        target: None,
                        negated: false,
                    }),
                ],
                along: vec![],
                fold: vec![],
                best_by: None,
                output: simple_yield_output(&["y"]),
                priority: None,
            }],
            vec![yield_col("y", true)],
        );

        let catalog = HashMap::from([
            ("base".to_string(), base.clone()),
            ("intermediate".to_string(), intermediate.clone()),
            ("recursive".to_string(), recursive.clone()),
        ]);

        let program = make_program(
            vec![
                Stratum {
                    id: 0,
                    rules: vec![base],
                    is_recursive: false,
                    depends_on: vec![],
                },
                Stratum {
                    id: 1,
                    rules: vec![intermediate],
                    is_recursive: false,
                    depends_on: vec![0],
                },
                Stratum {
                    id: 2,
                    rules: vec![recursive],
                    is_recursive: true,
                    depends_on: vec![1],
                },
            ],
            catalog,
        );

        let plan = builder
            .build_program_plan(
                &program,
                1000,
                std::time::Duration::from_secs(30),
                256 * 1024 * 1024,
                true,
                false,
                1e-15,
                false,
                1000,
                0,
            )
            .unwrap();
        let registry = if let LogicalPlan::LocyProgram {
            derived_scan_registry,
            ..
        } = &plan
        {
            derived_scan_registry.clone()
        } else {
            panic!("Expected LocyProgram")
        };

        if let LogicalPlan::LocyProgram { strata, .. } = &plan {
            assert_eq!(strata.len(), 3);
            assert!(!strata[0].is_recursive);
            assert!(!strata[1].is_recursive);
            assert!(strata[2].is_recursive);
            assert_eq!(strata[2].depends_on, vec![1]);
        }

        // "intermediate" is cross-stratum ref (from stratum 2, target in stratum 1)
        let inter_entries = registry.entries_for_rule("intermediate");
        assert_eq!(inter_entries.len(), 1);
        assert!(!inter_entries[0].is_self_ref);

        // "recursive" is self-ref (within stratum 2)
        let rec_entries = registry.entries_for_rule("recursive");
        assert_eq!(rec_entries.len(), 1);
        assert!(rec_entries[0].is_self_ref);
    }

    // ===================================================================
    // Integration Tests — Validation Errors
    // ===================================================================

    #[test]
    fn test_validation_is_ref_unknown_rule() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["x".to_string()],
                rule_name: qname("nonexistent"),
                target: None,
                negated: false,
            })],
            along: vec![],
            fold: vec![],
            best_by: None,
            output: simple_yield_output(&["x"]),
            priority: None,
        };
        let yield_cols = [yield_col("x", true)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        let result = builder.build_clause(
            &clause,
            &yield_cols,
            false,
            &names,
            &catalog,
            &HashSet::new(),
            false,
            1e-15,
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("nonexistent"), "Error: {}", err);
    }

    #[test]
    fn test_validation_is_ref_arity_mismatch() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "target",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([("target".to_string(), target_rule)]);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["x".to_string(), "y".to_string()],
                rule_name: qname("target"),
                target: None,
                negated: false,
            })],
            along: vec![],
            fold: vec![],
            best_by: None,
            output: simple_yield_output(&["x"]),
            priority: None,
        };
        let yield_cols = [yield_col("x", true)];
        let names = HashSet::new();

        let result = builder.build_clause(
            &clause,
            &yield_cols,
            false,
            &names,
            &catalog,
            &HashSet::new(),
            false,
            1e-15,
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("arity mismatch"), "Error: {}", err);
    }

    #[test]
    fn test_validation_along_prev_field_not_in_schema() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let target_rule = make_rule(
            "reachable",
            vec![simple_clause(node_pattern("n"), &["n"])],
            vec![yield_col("n", true)],
        );
        let catalog = HashMap::from([("reachable".to_string(), target_rule)]);

        let clause = CompiledClause {
            match_pattern: node_pattern("x"),
            where_conditions: vec![RuleCondition::IsReference(IsReference {
                subjects: vec!["x".to_string()],
                rule_name: qname("reachable"),
                target: None,
                negated: false,
            })],
            along: vec![AlongBinding {
                name: "cost".to_string(),
                expr: LocyExpr::PrevRef("nonexistent".to_string()),
            }],
            fold: vec![],
            best_by: None,
            output: simple_yield_output(&["x", "cost"]),
            priority: None,
        };
        let yield_cols = [yield_col("x", true), yield_col("cost", false)];
        let names = HashSet::new();

        let result = builder.build_clause(
            &clause,
            &yield_cols,
            false,
            &names,
            &catalog,
            &HashSet::new(),
            false,
            1e-15,
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("nonexistent"), "Error: {}", err);
    }

    #[test]
    fn test_validation_fold_on_recursive_stratum_skipped() {
        let planner = test_planner();
        let builder = LocyPlanBuilder::new(&planner);

        let clause = CompiledClause {
            match_pattern: node_pattern("n"),
            where_conditions: vec![],
            along: vec![],
            fold: vec![FoldBinding {
                name: "total".to_string(),
                aggregate: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Variable("cost".to_string())],
                    distinct: false,
                    window_spec: None,
                },
            }],
            best_by: None,
            output: simple_yield_output(&["n", "total"]),
            priority: None,
        };
        let yield_cols = [yield_col("n", true), yield_col("total", false)];
        let catalog = HashMap::new();
        let names = HashSet::new();

        // No error — FOLD in recursive stratum is silently skipped
        let result = builder.build_clause(
            &clause,
            &yield_cols,
            true,
            &names,
            &catalog,
            &HashSet::new(),
            false,
            1e-15,
        );
        assert!(result.is_ok());
    }
}
