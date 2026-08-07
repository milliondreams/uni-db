// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! QUERY evaluation.
//!
//! QUERY is answered from `derived_store` — the same semi-naive fixpoint result
//! that `.derived` reads — so the two agree by construction. SLG resolution
//! survives only as a fallback for rules containing a generator, which the
//! columnar fixpoint has no row-explosion operator for.
//!
//! Ported from `uni-locy/src/orchestrator/query.rs`. Uses `DerivedFactSource`
//! instead of `CypherExecutor`.

use std::collections::HashMap;
use std::time::Instant;

use uni_common::Value;
use uni_cypher::ast::{CypherLiteral, Expr, ReturnItem};
use uni_cypher::locy_ast::GoalQuery;
use uni_locy::{CompiledProgram, FactRow, LocyConfig, LocyError, LocyStats};

use super::locy_delta::RowStore;

use super::locy_eval::{eval_expr, value_cmp};
use super::locy_slg::{SLGResolver, extract_goal_bindings};
use super::locy_traits::DerivedFactSource;

/// Entry point for goal-directed QUERY evaluation.
///
/// Reads the fixpoint's `derived_store` for every rule it produced, then
/// applies the WHERE filter and RETURN clause. Falls back to SLG resolution
/// only for generator rules, which the fixpoint cannot evaluate.
pub async fn evaluate_query(
    query: &GoalQuery,
    program: &CompiledProgram,
    fact_source: &dyn DerivedFactSource,
    config: &LocyConfig,
    derived_store: &mut RowStore,
    stats: &mut LocyStats,
    start: Instant,
) -> Result<Vec<FactRow>, LocyError> {
    // `QUERY adult` inside `MODULE m` names the rule bare, but the catalog keys
    // it as `m.adult`. The compiler validates the reference module-aware, so a
    // plain lookup here accepts the program at compile time and then fails at
    // run time. Resolve with the same policy the derived store uses.
    let rule_name = query.rule_name.to_string();
    let catalog_key = uni_locy::names::resolve_unique(
        program.rule_catalog.keys().map(String::as_str),
        &rule_name,
    )
    .ok_or_else(|| LocyError::QueryResolutionError {
        message: format!("rule '{}' not found", rule_name),
    })?
    .to_string();
    let rule =
        program
            .rule_catalog
            .get(&catalog_key)
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
    let goal_bindings = match &query.where_expr {
        Some(expr) => extract_goal_bindings(expr, &key_columns),
        None => std::collections::HashMap::new(),
    };

    // Answer from the facts the fixpoint already derived.
    //
    // This used to re-derive the rule from scratch through the SLG resolver,
    // discarding `derived_store`, on the grounds that "the native fixpoint
    // stores node columns as VIDs (UInt64), not full node objects, so
    // orch_store rows would fail property-based WHERE/RETURN evaluation".
    // That reason is stale twice over: `FixpointState::reconcile_schema`
    // replaces the planner's inferred types with the physical plan's real
    // output schema, and `enrich_vids_with_nodes` hydrates VID columns into
    // node objects before commands are dispatched.
    //
    // The discard was the direct cause of issue #160: `.derived` read the
    // fixpoint's answer while `QUERY` read an independent SLG re-derivation
    // with weaker recursion and stratification, and the two disagreed. Reading
    // the same store makes them the same bytes by construction, which is a
    // stronger guarantee than any parity check could give.
    //
    // FOLD rules always took this path — the SLG resolver has no post-fixpoint
    // aggregation and would return raw pre-FOLD match rows — so this is that
    // branch generalized to every rule, not a new mechanism.
    // Module-aware: inside `MODULE m`, `QUERY adult` names the rule bare while
    // the store keys it `m.adult`. A plain lookup misses and falls through to
    // the SLG path, which then reports the rule as not found even though the
    // fixpoint derived it. `RowStore` is a bare map, so resolve the key with
    // the shared policy rather than a method.
    let store_key =
        uni_locy::names::resolve_unique(derived_store.keys().map(String::as_str), &rule_name)
            .map(str::to_string);
    if let Some(relation) = store_key.and_then(|k| derived_store.get(&k)) {
        let rows = relation.rows.clone();
        let filtered = filter_where(rows, query.where_expr.as_ref(), &config.params);
        return apply_return_clause(filtered, &query.return_clause, &config.params);
    }

    // Fallback: the fixpoint produced nothing for this rule.
    //
    // In practice that means a rule containing a generator, which the planner
    // skips when building strata because the columnar engine has no
    // row-explosion operator (`locy_planner.rs`, `locy_slg::apply_generators`).
    // Generators are the one capability the fixpoint genuinely lacks, so the
    // SLG path survives for exactly that case. Seed the store with FOLD
    // relations so an IS NOT across a FOLD boundary can still resolve.
    let mut fresh_store = RowStore::new();
    for (name, relation) in derived_store.iter() {
        if let Some(r) = program.rule_catalog.get(name)
            && r.clauses.iter().any(|c| !c.fold.is_empty())
        {
            fresh_store.insert(name.clone(), relation.clone());
        }
    }
    let mut resolver = SLGResolver::new(program, fact_source, config, &mut fresh_store, start);
    let results = resolver.resolve_goal(&rule_name, &goal_bindings).await?;

    // Merge SLG stats
    stats.queries_executed += resolver.stats.queries_executed;
    stats.mutations_executed += resolver.stats.mutations_executed;

    // Apply WHERE filter (SLG may return superset if goal bindings are partial).
    let filtered = filter_where(results, query.where_expr.as_ref(), &config.params);

    // Apply RETURN clause if present
    apply_return_clause(filtered, &query.return_clause, &config.params)
}

/// Apply a RETURN clause (projection, ordering, skip, limit) to results.
pub(super) fn apply_return_clause(
    rows: Vec<FactRow>,
    return_clause: &Option<uni_cypher::ast::ReturnClause>,
    params: &HashMap<String, Value>,
) -> Result<Vec<FactRow>, LocyError> {
    let rc = match return_clause {
        Some(rc) => rc,
        None => return Ok(rows),
    };

    // Project columns. Params are merged into each row so $name references
    // in RETURN expressions (e.g. RETURN $agent_id AS id) resolve correctly.
    let mut projected: Vec<FactRow> = rows
        .into_iter()
        .map(|row| {
            let merged = merge_params(&row, params);
            let mut new_row = FactRow::new();
            for item in &rc.items {
                match item {
                    ReturnItem::All => return Ok(row.clone()),
                    ReturnItem::Expr { expr, alias, .. } => {
                        let value = eval_expr(expr, &merged)?;
                        let name = alias.clone().unwrap_or_else(|| return_item_name(expr));
                        new_row.insert(name, value);
                    }
                }
            }
            Ok(new_row)
        })
        .collect::<Result<Vec<_>, LocyError>>()?;

    // Distinct
    if rc.distinct {
        // Key on a sorted `BTreeMap` rather than `format!("{row:?}")`: a
        // `FactRow` is a `HashMap`, whose `Debug` order is instance-dependent,
        // so byte-identical rows could render to different strings and survive
        // DISTINCT. `Value` has a canonical `Hash`/`Eq`, so this dedups by
        // content deterministically.
        let mut seen = std::collections::HashSet::new();
        projected.retain(|row| {
            let key: std::collections::BTreeMap<String, uni_common::Value> =
                row.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
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

/// Merge query parameters into a row so that `Expr::Parameter(name)` can
/// resolve `$name` references during in-memory expression evaluation.
///
/// Row values take precedence — parameters only fill in keys that are absent.
pub(super) fn merge_params(row: &FactRow, params: &HashMap<String, Value>) -> FactRow {
    let mut merged: FactRow = params.clone();
    merged.extend(row.iter().map(|(k, v)| (k.clone(), v.clone())));
    merged
}

/// Apply a QUERY `WHERE` predicate to result rows.
///
/// Params are injected per row so `$name` references resolve. A row is kept only
/// when the predicate evaluates to a truthy value; an evaluation error or a
/// non-boolean result drops the row. Used uniformly by both the FOLD-rule and
/// non-FOLD result paths so the filter is never bypassed.
pub(super) fn filter_where(
    rows: Vec<FactRow>,
    where_expr: Option<&Expr>,
    params: &HashMap<String, Value>,
) -> Vec<FactRow> {
    let Some(expr) = where_expr else {
        return rows;
    };
    rows.into_iter()
        .filter(|row| {
            let merged = merge_params(row, params);
            eval_expr(expr, &merged)
                .map(|v| v.as_bool().unwrap_or(false))
                .unwrap_or(false)
        })
        .collect()
}

/// Derive a column name from a RETURN expression when no alias is given.
///
/// Follows OpenCypher convention: `RETURN p` yields `"p"`,
/// `RETURN a.name` yields `"a.name"`.  Falls back to `Debug` for
/// complex expressions.
fn return_item_name(expr: &Expr) -> String {
    match expr {
        Expr::Variable(v) => v.clone(),
        Expr::Property(base, prop) => format!("{}.{}", return_item_name(base), prop),
        _ => format!("{expr:?}"),
    }
}
