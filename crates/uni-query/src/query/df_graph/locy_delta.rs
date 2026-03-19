// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Delta tracking and IS-reference parameter building for Locy native execution.
//!
//! Relocated from `uni-locy/src/orchestrator/delta.rs` and parts of
//! `uni-locy/src/orchestrator/fixpoint.rs` during Phase 7.

use std::collections::HashMap;

use uni_common::Value;
use uni_cypher::ast::Expr;
use uni_cypher::locy_ast::{IsReference, RuleCondition};
use uni_locy::types::CompiledClause;
use uni_locy::{LocyError, Row};

use super::locy_ast_builder::expr_references_var;
use super::locy_eval::{
    eval_expr, eval_locy_expr, record_batches_to_locy_rows, values_equal_for_join,
};
use super::locy_traits::DerivedFactSource;

/// A tuple of key column values used for indexing derived facts.
pub type KeyTuple = Vec<Value>;

/// A minimal row-based relation for SLG/EXPLAIN command dispatch.
#[derive(Debug, Clone)]
pub struct RowRelation {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
}

impl RowRelation {
    pub fn new(columns: Vec<String>, rows: Vec<Row>) -> Self {
        Self { columns, rows }
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }
}

/// A map from rule name to its RowRelation (row-based store for SLG/EXPLAIN).
pub type RowStore = HashMap<String, RowRelation>;

/// Extracts the key tuple from a row given key column names.
pub fn extract_key(row: &Row, key_cols: &[String]) -> KeyTuple {
    key_cols
        .iter()
        .map(|k| row.get(k).cloned().unwrap_or(Value::Null))
        .collect()
}

/// Extract Cypher expression conditions from rule conditions (skip IS references).
pub fn extract_cypher_conditions(conditions: &[RuleCondition]) -> Vec<Expr> {
    conditions
        .iter()
        .filter_map(|c| match c {
            RuleCondition::Expression(expr) => Some(expr.clone()),
            _ => None,
        })
        .collect()
}

// ── In-memory IS-ref join helpers ─────────────────────────────────────────────

/// Check whether a derived fact matches the subject bindings from a base row.
///
/// Positional: `subjects[i]` maps to `schema[i]` in the derived relation.
fn is_ref_matches(
    base_row: &Row,
    derived_fact: &Row,
    is_ref: &IsReference,
    schema: &[String],
) -> bool {
    for (i, subject) in is_ref.subjects.iter().enumerate() {
        if i >= schema.len() {
            break;
        }
        let base_val = base_row.get(subject).cloned().unwrap_or(Value::Null);
        let fact_val = derived_fact.get(&schema[i]).cloned().unwrap_or(Value::Null);
        if !values_equal_for_join(&base_val, &fact_val) {
            return false;
        }
    }
    true
}

/// Positive IS semi-join: for each base row, produce one output row per matching
/// derived fact, binding the optional target variable and stashing derived fact
/// columns under `__prev_{col}` for ALONG PrevRef resolution.
fn semi_join_is_ref(
    base_rows: &[Row],
    derived_facts: &[Row],
    is_ref: &IsReference,
    schema: &[String],
) -> Vec<Row> {
    let mut result = Vec::new();
    for base_row in base_rows {
        for derived_fact in derived_facts {
            if is_ref_matches(base_row, derived_fact, is_ref, schema) {
                let mut row = base_row.clone();

                // Bind target variable (column after subjects in the derived schema).
                if let Some(target) = &is_ref.target {
                    let target_col_idx = is_ref.subjects.len();
                    if target_col_idx < schema.len() {
                        let val = derived_fact
                            .get(&schema[target_col_idx])
                            .cloned()
                            .unwrap_or(Value::Null);
                        row.insert(target.clone(), val);
                    }
                }

                // Stash derived fact columns for ALONG PrevRef lookups.
                for (col, val) in derived_fact {
                    row.insert(format!("__prev_{}", col), val.clone());
                }

                result.push(row);
            }
        }
    }
    result
}

/// Negative IS anti-join: retain base rows where no derived fact matches.
fn anti_join_is_ref(
    base_rows: &[Row],
    derived_facts: &[Row],
    is_ref: &IsReference,
    schema: &[String],
) -> Vec<Row> {
    base_rows
        .iter()
        .filter(|base_row| {
            !derived_facts
                .iter()
                .any(|df| is_ref_matches(base_row, df, is_ref, schema))
        })
        .cloned()
        .collect()
}

/// Resolve a clause by executing its MATCH pattern and applying IS-ref joins in memory.
///
/// Replaces the serialize-UNWIND-execute round-trip used by SLG and EXPLAIN Mode B.
/// IS-refs are resolved as in-memory semi-joins (positive) or anti-joins (negated).
/// ALONG expressions are evaluated last using `__prev_*` stash from semi-join results.
pub async fn resolve_clause_with_is_refs(
    clause: &CompiledClause,
    fact_source: &dyn DerivedFactSource,
    derived_store: &RowStore,
) -> Result<Vec<Row>, LocyError> {
    // Collect target variable names from positive IS-refs.
    let target_vars: Vec<String> = clause
        .where_conditions
        .iter()
        .filter_map(|c| {
            if let RuleCondition::IsReference(is_ref) = c {
                if !is_ref.negated {
                    is_ref.target.clone()
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    // Partition Cypher conditions: match-safe vs. target-dependent.
    let all_cypher = extract_cypher_conditions(&clause.where_conditions);
    let mut match_safe = Vec::new();
    let mut target_dependent = Vec::new();
    for expr in all_cypher {
        if target_vars.iter().any(|v| expr_references_var(&expr, v)) {
            target_dependent.push(expr);
        } else {
            match_safe.push(expr);
        }
    }

    // Execute base MATCH query.
    let raw_batches = fact_source
        .execute_pattern(&clause.match_pattern, &match_safe)
        .await?;
    let mut rows = record_batches_to_locy_rows(&raw_batches);

    // Apply IS-ref joins/anti-joins sequentially.
    for cond in &clause.where_conditions {
        if let RuleCondition::IsReference(is_ref) = cond {
            let rule_name = is_ref.rule_name.to_string();
            let (schema, derived_facts) = if let Some(relation) = derived_store.get(&rule_name) {
                (relation.columns.clone(), relation.rows.clone())
            } else {
                (Vec::new(), Vec::new())
            };

            if is_ref.negated {
                rows = anti_join_is_ref(&rows, &derived_facts, is_ref, &schema);
            } else {
                rows = semi_join_is_ref(&rows, &derived_facts, is_ref, &schema);
            }
        }
    }

    // Apply target-dependent Cypher conditions.
    if !target_dependent.is_empty() {
        rows.retain(|row| {
            target_dependent.iter().all(|expr| {
                eval_expr(expr, row)
                    .map(|v| v.as_bool().unwrap_or(false))
                    .unwrap_or(false)
            })
        });
    }

    // Evaluate ALONG expressions (using __prev_* stash for PrevRef lookups).
    if !clause.along.is_empty() {
        let mut new_rows = Vec::with_capacity(rows.len());
        for mut row in rows {
            let prev_values: Row = row
                .iter()
                .filter_map(|(k, v)| {
                    k.strip_prefix("__prev_")
                        .map(|col| (col.to_string(), v.clone()))
                })
                .collect();

            for along in &clause.along {
                let result = eval_locy_expr(&along.expr, &row, Some(&prev_values))?;
                row.insert(along.name.clone(), result);
            }
            new_rows.push(row);
        }
        rows = new_rows;
    }

    // Remove __prev_* staging keys from output rows.
    for row in &mut rows {
        row.retain(|k, _| !k.starts_with("__prev_"));
    }

    Ok(rows)
}
