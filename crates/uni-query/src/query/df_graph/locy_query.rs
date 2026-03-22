// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Goal-directed QUERY evaluation via SLG resolution.
//!
//! Ported from `uni-locy/src/orchestrator/query.rs`. Uses `DerivedFactSource`
//! instead of `CypherExecutor`.

use std::time::Instant;

use uni_cypher::ast::{CypherLiteral, Expr, ReturnItem};
use uni_cypher::locy_ast::GoalQuery;
use uni_locy::{CompiledProgram, LocyConfig, LocyError, LocyStats, Row};

use super::locy_delta::RowStore;

use super::locy_eval::{eval_expr, value_cmp};
use super::locy_slg::{SLGResolver, extract_goal_bindings};
use super::locy_traits::DerivedFactSource;

/// Entry point for goal-directed QUERY evaluation.
///
/// Uses SLG resolution for all rules (recursive and non-recursive).
/// SLG is goal-directed: it only computes facts relevant to the WHERE constraints.
pub async fn evaluate_query(
    query: &GoalQuery,
    program: &CompiledProgram,
    fact_source: &dyn DerivedFactSource,
    config: &LocyConfig,
    _derived_store: &mut RowStore,
    stats: &mut LocyStats,
    start: Instant,
) -> Result<Vec<Row>, LocyError> {
    let rule_name = query.rule_name.to_string();
    let rule =
        program
            .rule_catalog
            .get(&rule_name)
            .ok_or_else(|| LocyError::QueryResolutionError {
                message: format!("rule '{}' not found", rule_name),
            })?;

    let key_columns: Vec<String> = rule
        .yield_schema
        .iter()
        .filter(|c| c.is_key)
        .map(|c| c.name.clone())
        .collect();

    // Extract goal bindings from WHERE for goal-directed resolution
    let goal_bindings = extract_goal_bindings(&query.where_expr, &key_columns);

    // Use a fresh store rather than the pre-computed orch_store.
    // The native fixpoint stores node columns as VIDs (UInt64), not full node objects,
    // so orch_store rows would fail property-based WHERE/RETURN evaluation (a.name etc.).
    // SLG re-evaluation executes actual Cypher queries which return full node objects.
    let mut fresh_store = RowStore::new();
    let mut resolver = SLGResolver::new(program, fact_source, config, &mut fresh_store, start);
    let results = resolver.resolve_goal(&rule_name, &goal_bindings).await?;

    // Merge SLG stats
    stats.queries_executed += resolver.stats.queries_executed;
    stats.mutations_executed += resolver.stats.mutations_executed;

    // Apply WHERE filter (SLG may return superset if goal bindings are partial)
    let filtered: Vec<Row> = results
        .into_iter()
        .filter(|row| {
            eval_expr(&query.where_expr, row)
                .map(|v| v.as_bool().unwrap_or(false))
                .unwrap_or(false)
        })
        .collect();

    // Apply RETURN clause if present
    apply_return_clause(filtered, &query.return_clause)
}

/// Apply a RETURN clause (projection, ordering, skip, limit) to results.
fn apply_return_clause(
    rows: Vec<Row>,
    return_clause: &Option<uni_cypher::ast::ReturnClause>,
) -> Result<Vec<Row>, LocyError> {
    let rc = match return_clause {
        Some(rc) => rc,
        None => return Ok(rows),
    };

    // Project columns
    let mut projected: Vec<Row> = rows
        .into_iter()
        .map(|row| {
            let mut new_row = Row::new();
            for item in &rc.items {
                match item {
                    ReturnItem::All => return Ok(row.clone()),
                    ReturnItem::Expr { expr, alias, .. } => {
                        let value = eval_expr(expr, &row)?;
                        let name = alias.clone().unwrap_or_else(|| format!("{expr:?}"));
                        new_row.insert(name, value);
                    }
                }
            }
            Ok(new_row)
        })
        .collect::<Result<Vec<_>, LocyError>>()?;

    // Distinct
    if rc.distinct {
        let mut seen = std::collections::HashSet::new();
        projected.retain(|row| {
            let key = format!("{row:?}");
            seen.insert(key)
        });
    }

    // Order by
    if let Some(sort_items) = &rc.order_by {
        projected.sort_by(|a, b| {
            for item in sort_items {
                let va = eval_expr(&item.expr, a).unwrap_or(uni_common::Value::Null);
                let vb = eval_expr(&item.expr, b).unwrap_or(uni_common::Value::Null);
                let cmp = if item.ascending {
                    value_cmp(&va, &vb)
                } else {
                    value_cmp(&vb, &va)
                };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    // Skip
    if let Some(Expr::Literal(CypherLiteral::Integer(n))) = &rc.skip {
        let n = *n as usize;
        if n < projected.len() {
            projected = projected[n..].to_vec();
        } else {
            projected.clear();
        }
    }

    // Limit
    if let Some(Expr::Literal(CypherLiteral::Integer(n))) = &rc.limit {
        projected.truncate(*n as usize);
    }

    Ok(projected)
}
