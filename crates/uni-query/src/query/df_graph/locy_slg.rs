// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! SLG (Selective Linear Definite clause) resolution for goal-directed evaluation.
//!
//! Ported from `uni-locy/src/orchestrator/slg.rs`. Uses `DerivedFactSource` instead
//! of `CypherExecutor` for query execution.
//!
//! Since strata are pre-computed bottom-up in the native path, `resolve_goal` hits
//! the "check derived_store" early return path for pre-populated rules. The full
//! tabling logic is preserved for correctness in case of partial population.

use std::collections::HashMap;
use std::time::Instant;

use uni_common::Value;
use uni_cypher::ast::{BinaryOp, Expr};
use uni_cypher::locy_ast::{LocyBinaryOp, LocyExpr, RuleCondition, RuleOutput};
use uni_locy::types::{CompiledClause, CompiledRule};
use uni_locy::{CompiledProgram, FactRow, LocyConfig, LocyError, LocyStats};

use super::locy_ast_builder::value_to_expr;
use super::locy_delta::{
    RowRelation, RowStore, extract_cypher_conditions, extract_key, resolve_clause_with_is_refs,
};
use super::locy_eval::{eval_expr, literal_to_value, record_batches_to_locy_rows};
use super::locy_traits::DerivedFactSource;

/// Status of a tabling cache entry.
#[derive(Debug, Clone, PartialEq)]
enum GoalStatus {
    InProgress,
    Complete,
}

/// A cache entry for a resolved goal.
#[derive(Debug, Clone)]
struct TableEntry {
    answers: Vec<FactRow>,
    status: GoalStatus,
}

/// Cache key: (rule_name, known key bindings sorted).
type CacheKey = (String, Vec<(String, Value)>);

/// SLG resolution engine for goal-directed evaluation.
///
/// Instead of computing the full fixpoint bottom-up, SLG starts from the query goal
/// and only computes facts relevant to that goal. Tabling prevents infinite loops.
pub struct SLGResolver<'a> {
    program: &'a CompiledProgram,
    fact_source: &'a dyn DerivedFactSource,
    cache: HashMap<CacheKey, TableEntry>,
    config: &'a LocyConfig,
    pub stats: LocyStats,
    derived_store: &'a mut RowStore,
    depth: usize,
    start: Instant,
}

impl<'a> SLGResolver<'a> {
    pub fn new(
        program: &'a CompiledProgram,
        fact_source: &'a dyn DerivedFactSource,
        config: &'a LocyConfig,
        derived_store: &'a mut RowStore,
        start: Instant,
    ) -> Self {
        Self {
            program,
            fact_source,
            cache: HashMap::new(),
            config,
            stats: LocyStats::default(),
            derived_store,
            depth: 0,
            start,
        }
    }

    /// Resolve a goal: find all facts for `rule_name` matching `goal_bindings`.
    ///
    /// Uses Box::pin for recursive async (subgoals call resolve_goal).
    pub fn resolve_goal<'s>(
        &'s mut self,
        rule_name: &'s str,
        goal_bindings: &'s HashMap<String, Value>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<FactRow>, LocyError>> + 's>>
    {
        Box::pin(async move {
            let elapsed = self.start.elapsed();
            if elapsed > self.config.timeout {
                return Err(LocyError::Timeout {
                    elapsed,
                    limit: self.config.timeout,
                });
            }
            if self.depth > self.config.max_slg_depth {
                return Err(LocyError::QueryResolutionError {
                    message: format!(
                        "SLG resolution depth exceeded {} for rule '{}'",
                        self.config.max_slg_depth, rule_name
                    ),
                });
            }

            let rule = self
                .program
                .rule_catalog
                .get(rule_name)
                .ok_or_else(|| LocyError::QueryResolutionError {
                    message: format!("rule '{}' not found", rule_name),
                })?
                .clone();

            let cache_key = make_cache_key(rule_name, goal_bindings);

            // Cache check
            if let Some(entry) = self.cache.get(&cache_key) {
                match entry.status {
                    GoalStatus::Complete => return Ok(entry.answers.clone()),
                    GoalStatus::InProgress => return Ok(entry.answers.clone()),
                }
            }

            // If derived_store already has facts (from fixpoint), use them directly.
            // This avoids re-executing queries for rules that were already computed.
            if let Some(relation) = self.derived_store.get(rule_name) {
                let all_facts = relation.rows.clone();
                if !all_facts.is_empty() {
                    let filtered: Vec<FactRow> = all_facts
                        .into_iter()
                        .filter(|row| matches_goal(row, goal_bindings))
                        .collect();
                    self.cache.insert(
                        cache_key,
                        TableEntry {
                            answers: filtered.clone(),
                            status: GoalStatus::Complete,
                        },
                    );
                    return Ok(filtered);
                }
            }

            // Mark InProgress
            self.cache.insert(
                cache_key.clone(),
                TableEntry {
                    answers: Vec::new(),
                    status: GoalStatus::InProgress,
                },
            );

            self.depth += 1;

            // Initial resolution
            let answers = self.resolve_rule_clauses(&rule, goal_bindings).await?;

            // Iterative completion for recursive rules
            let final_answers = self
                .iterative_complete(&rule, goal_bindings, answers)
                .await?;

            self.depth -= 1;

            // Mark Complete
            self.cache.insert(
                cache_key,
                TableEntry {
                    answers: final_answers.clone(),
                    status: GoalStatus::Complete,
                },
            );

            // Populate derived_store as side-effect
            store_derived_facts(self.derived_store, rule_name, &rule, &final_answers);

            Ok(final_answers)
        })
    }

    /// Resolve all clauses of a rule against goal bindings.
    async fn resolve_rule_clauses(
        &mut self,
        rule: &CompiledRule,
        goal_bindings: &HashMap<String, Value>,
    ) -> Result<Vec<FactRow>, LocyError> {
        let mut all_answers = Vec::new();

        for clause in &rule.clauses {
            let has_is_refs = clause
                .where_conditions
                .iter()
                .any(|c| matches!(c, RuleCondition::IsReference(_)));
            let has_along = !clause.along.is_empty();

            if has_is_refs || has_along {
                // Resolve IS ref subgoals first (populates derived_store).
                for cond in &clause.where_conditions {
                    if let RuleCondition::IsReference(is_ref) = cond {
                        let ref_rule_name = is_ref.rule_name.to_string();
                        self.resolve_goal(&ref_rule_name, &HashMap::new()).await?;
                    }
                }

                // In-memory join (no UNWIND serialization).
                let rows =
                    resolve_clause_with_is_refs(clause, self.fact_source, self.derived_store)
                        .await?;
                self.stats.queries_executed += 1;

                // Apply YIELD projections to compute non-key columns.
                let projected = apply_yield_projections(rows, clause);

                // Filter by goal bindings.
                let filtered: Vec<FactRow> = projected
                    .into_iter()
                    .filter(|row| matches_goal(row, goal_bindings))
                    .collect();
                all_answers.extend(filtered);
            } else {
                // Simple clause: inject goal constraints into WHERE
                let cypher_conditions = extract_cypher_conditions(&clause.where_conditions);
                let mut all_conditions = cypher_conditions;
                inject_goal_where(&mut all_conditions, goal_bindings);

                let raw_batches = self
                    .fact_source
                    .execute_pattern(&clause.match_pattern, &all_conditions)
                    .await?;
                self.stats.queries_executed += 1;
                let raw_rows = record_batches_to_locy_rows(&raw_batches);

                // Apply YIELD projections to compute non-key columns.
                let projected = apply_yield_projections(raw_rows, clause);
                all_answers.extend(projected);
            }
        }

        Ok(all_answers)
    }

    /// Iterative completion: re-resolve if new answers are discovered.
    async fn iterative_complete(
        &mut self,
        rule: &CompiledRule,
        goal_bindings: &HashMap<String, Value>,
        initial_answers: Vec<FactRow>,
    ) -> Result<Vec<FactRow>, LocyError> {
        let key_columns: Vec<String> = rule
            .yield_schema
            .iter()
            .filter(|c| c.is_key)
            .map(|c| c.name.clone())
            .collect();

        let mut answers = initial_answers;
        let mut iteration = 0;

        loop {
            iteration += 1;
            if iteration > self.config.max_iterations {
                break;
            }

            let prev_count = answers.len();

            // Store current answers so recursive subgoals can see them
            store_derived_facts(self.derived_store, &rule.name, rule, &answers);

            // Update cache
            let cache_key = make_cache_key(&rule.name, goal_bindings);
            if let Some(entry) = self.cache.get_mut(&cache_key) {
                entry.answers = answers.clone();
            }

            let new_answers = self.resolve_rule_clauses(rule, goal_bindings).await?;

            // Merge new answers (dedup by key)
            for new_row in new_answers {
                let new_key = extract_key(&new_row, &key_columns);
                let already_exists = answers
                    .iter()
                    .any(|existing| extract_key(existing, &key_columns) == new_key);
                if !already_exists {
                    answers.push(new_row);
                }
            }

            if answers.len() == prev_count {
                break;
            }
        }

        Ok(answers)
    }
}

/// Convert a LocyExpr to a standard Cypher Expr for in-memory evaluation.
/// Returns None for PrevRef (only meaningful in recursive fixpoint).
fn locy_expr_to_cypher(locy: &LocyExpr) -> Option<Expr> {
    match locy {
        LocyExpr::Cypher(e) => Some(e.clone()),
        LocyExpr::PrevRef(_) => None,
        LocyExpr::BinaryOp { left, op, right } => {
            let l = locy_expr_to_cypher(left)?;
            let r = locy_expr_to_cypher(right)?;
            let cypher_op = match op {
                LocyBinaryOp::Add => BinaryOp::Add,
                LocyBinaryOp::Sub => BinaryOp::Sub,
                LocyBinaryOp::Mul => BinaryOp::Mul,
                LocyBinaryOp::Div => BinaryOp::Div,
                LocyBinaryOp::Mod => BinaryOp::Mod,
                LocyBinaryOp::Pow => BinaryOp::Pow,
                LocyBinaryOp::And => BinaryOp::And,
                LocyBinaryOp::Or => BinaryOp::Or,
                LocyBinaryOp::Xor => BinaryOp::Xor,
            };
            Some(Expr::BinaryOp {
                left: Box::new(l),
                op: cypher_op,
                right: Box::new(r),
            })
        }
        LocyExpr::UnaryOp(op, inner) => {
            let e = locy_expr_to_cypher(inner)?;
            Some(Expr::UnaryOp {
                op: *op,
                expr: Box::new(e),
            })
        }
    }
}

/// Apply YIELD projections to raw rows from pattern execution.
///
/// The SLG resolver executes raw `MATCH ... RETURN *` queries, which return full
/// graph entities (nodes, edges) but do NOT include non-key YIELD columns like
/// property accesses (`n.val AS v`), computed expressions (`1.0 - n.val AS sev`),
/// or literal constants (`0.5 AS lit`).  This function evaluates each clause's
/// YIELD items against the raw rows to produce the projected columns.
fn apply_yield_projections(raw_rows: Vec<FactRow>, clause: &CompiledClause) -> Vec<FactRow> {
    let yield_items = match &clause.output {
        RuleOutput::Yield(yc) => &yc.items,
        _ => return raw_rows,
    };

    // If yield has no non-key items with expressions, skip projection
    let has_non_key_exprs = yield_items.iter().any(|item| !item.is_key);
    if !has_non_key_exprs {
        return raw_rows;
    }

    raw_rows
        .into_iter()
        .map(|raw_row| {
            let mut projected = FactRow::new();
            for item in yield_items {
                let name = item
                    .alias
                    .clone()
                    .unwrap_or_else(|| expr_name_for_yield(&item.expr));

                if item.is_key {
                    // KEY columns: copy from raw row (node/edge variables)
                    if let Some(val) = raw_row.get(&name) {
                        projected.insert(name, val.clone());
                    } else if let Expr::Variable(var_name) = &item.expr
                        && let Some(val) = raw_row.get(var_name)
                    {
                        // KEY variable might be the graph entity itself
                        projected.insert(name, val.clone());
                    }
                } else {
                    // Non-key columns: evaluate the YIELD expression against the raw row
                    match eval_expr(&item.expr, &raw_row) {
                        Ok(val) => {
                            projected.insert(name, val);
                        }
                        Err(_) => {
                            projected.insert(name, Value::Null);
                        }
                    }
                }
            }

            // Also carry through ALONG bindings from the raw row.
            // ALONG expressions use LocyExpr; extract the inner Cypher Expr
            // (PrevRef only applies in recursive fixpoint, not SLG resolution).
            for along in &clause.along {
                if !projected.contains_key(&along.name)
                    && let Some(cypher_expr) = locy_expr_to_cypher(&along.expr)
                {
                    match eval_expr(&cypher_expr, &raw_row) {
                        Ok(val) => {
                            projected.insert(along.name.clone(), val);
                        }
                        Err(_) => {
                            projected.insert(along.name.clone(), Value::Null);
                        }
                    }
                }
            }

            projected
        })
        .collect()
}

/// Derive a column name from a YIELD expression (mirrors typecheck.rs `expr_name`).
fn expr_name_for_yield(expr: &Expr) -> String {
    match expr {
        Expr::Variable(name) => name.clone(),
        Expr::Property(_, prop) => prop.clone(),
        _ => "?".to_string(),
    }
}

/// Store resolved facts into derived_store (free function to avoid borrow conflicts).
fn store_derived_facts(
    derived_store: &mut RowStore,
    rule_name: &str,
    rule: &CompiledRule,
    facts: &[FactRow],
) {
    let columns: Vec<String> = rule.yield_schema.iter().map(|c| c.name.clone()).collect();

    let mut all_columns = columns;
    for clause in &rule.clauses {
        for along in &clause.along {
            if !all_columns.contains(&along.name) {
                all_columns.push(along.name.clone());
            }
        }
    }

    let relation = RowRelation::new(all_columns, facts.to_vec());
    derived_store.insert(rule_name.to_string(), relation);
}

/// Build a cache key from rule name and goal bindings.
fn make_cache_key(rule_name: &str, goal_bindings: &HashMap<String, Value>) -> CacheKey {
    let mut bindings: Vec<(String, Value)> = goal_bindings
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    bindings.sort_by(|a, b| a.0.cmp(&b.0));
    (rule_name.to_string(), bindings)
}

/// Check if a row matches goal bindings.
fn matches_goal(row: &FactRow, goal_bindings: &HashMap<String, Value>) -> bool {
    goal_bindings
        .iter()
        .all(|(k, v)| row.get(k).map(|rv| rv == v).unwrap_or(false))
}

/// Inject goal bindings as equality WHERE conditions.
fn inject_goal_where(conditions: &mut Vec<Expr>, goal_bindings: &HashMap<String, Value>) {
    for (var, val) in goal_bindings {
        conditions.push(Expr::BinaryOp {
            left: Box::new(Expr::Variable(var.clone())),
            op: BinaryOp::Eq,
            right: Box::new(value_to_expr(val)),
        });
    }
}

/// Extract goal bindings from a WHERE expression.
///
/// Pattern-matches on `var = literal` and `literal = var` to extract
/// key constraints for the SLG resolver.
pub fn extract_goal_bindings(where_expr: &Expr, key_columns: &[String]) -> HashMap<String, Value> {
    let mut bindings = HashMap::new();
    collect_equality_bindings(where_expr, key_columns, &mut bindings);
    bindings
}

fn collect_equality_bindings(
    expr: &Expr,
    key_columns: &[String],
    bindings: &mut HashMap<String, Value>,
) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
        } => {
            if let (Expr::Variable(var), Expr::Literal(lit)) = (left.as_ref(), right.as_ref())
                && key_columns.contains(var)
            {
                bindings.insert(var.clone(), literal_to_value(lit));
            }
            if let (Expr::Literal(lit), Expr::Variable(var)) = (left.as_ref(), right.as_ref())
                && key_columns.contains(var)
            {
                bindings.insert(var.clone(), literal_to_value(lit));
            }
        }
        Expr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
        } => {
            collect_equality_bindings(left, key_columns, bindings);
            collect_equality_bindings(right, key_columns, bindings);
        }
        _ => {}
    }
}
