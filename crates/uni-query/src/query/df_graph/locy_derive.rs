// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! DERIVE command execution via `LocyExecutionContext`.
//!
//! Extracted from `uni-locy/src/orchestrator/mod.rs::derive_command`.
//! Uses `LocyExecutionContext` for fact lookup and mutation execution.
//!
//! Supports two modes:
//! - **execute mode** (`derive_command`): immediately applies mutations via `ctx.execute_mutation()`
//! - **collect mode** (`collect_derive_facts`): collects Cypher ASTs + vertex/edge data for deferred application

use std::collections::HashMap;

use uni_common::Properties;
use uni_cypher::ast::Query;
use uni_cypher::locy_ast::{DeriveClause, DeriveCommand, DerivePattern, RuleOutput};
use uni_locy::result::DerivedEdge;
use uni_locy::{CompiledProgram, FactRow, LocyError, LocyStats};

use super::locy_ast_builder::build_derive_create;
use super::locy_eval::eval_expr;
use super::locy_traits::LocyExecutionContext;

/// Output of `collect_derive_facts()` — collected but not yet executed.
pub struct CollectedDeriveOutput {
    pub queries: Vec<Query>,
    pub vertices: HashMap<String, Vec<Properties>>,
    pub edges: Vec<DerivedEdge>,
    pub affected: usize,
}

/// Execute a top-level DERIVE command (auto-apply mode).
///
/// Looks up facts from the native store via `ctx.lookup_derived()`, applies optional
/// WHERE filtering, and for each matching fact executes the DERIVE mutation via
/// `ctx.execute_mutation()`.
pub async fn derive_command(
    dc: &DeriveCommand,
    program: &CompiledProgram,
    ctx: &dyn LocyExecutionContext,
    stats: &mut LocyStats,
) -> Result<usize, LocyError> {
    let collected = collect_derive_facts_inner(dc, program, ctx).await?;
    for query in collected.queries {
        ctx.execute_mutation(query, HashMap::new()).await?;
        stats.mutations_executed += 1;
    }
    Ok(collected.affected)
}

/// Collect derived facts without executing mutations (collect mode).
///
/// Returns the Cypher ASTs, vertex data, and edge data for deferred
/// application via `tx.apply()`.
pub async fn collect_derive_facts(
    dc: &DeriveCommand,
    program: &CompiledProgram,
    ctx: &dyn LocyExecutionContext,
) -> Result<CollectedDeriveOutput, LocyError> {
    collect_derive_facts_inner(dc, program, ctx).await
}

/// Shared implementation for both execute and collect modes.
async fn collect_derive_facts_inner(
    dc: &DeriveCommand,
    program: &CompiledProgram,
    ctx: &dyn LocyExecutionContext,
) -> Result<CollectedDeriveOutput, LocyError> {
    let rule_name = dc.rule_name.to_string();
    let rule = program
        .rule_catalog
        .get(&rule_name)
        .ok_or_else(|| LocyError::EvaluationError {
            message: format!("rule '{}' not found for DERIVE command", rule_name),
        })?;

    let facts = ctx.lookup_derived_enriched(&rule_name).await?;

    // Apply optional WHERE filter
    let filtered: Vec<_> = if let Some(where_expr) = &dc.where_expr {
        facts
            .into_iter()
            .filter(|row| {
                eval_expr(where_expr, row)
                    .map(|v| v.as_bool().unwrap_or(false))
                    .unwrap_or(false)
            })
            .collect()
    } else {
        facts
    };

    let mut all_queries = Vec::new();
    let mut all_vertices: HashMap<String, Vec<Properties>> = HashMap::new();
    let mut all_edges = Vec::new();
    let mut affected = 0;

    for clause in &rule.clauses {
        if let RuleOutput::Derive(derive_clause) = &clause.output {
            for row in &filtered {
                let queries = build_derive_create(derive_clause, row)?;
                affected += queries.len();

                // Extract vertex/edge data for inspection
                extract_vertex_edge_data(derive_clause, row, &mut all_vertices, &mut all_edges);

                all_queries.extend(queries);
            }
        }
    }

    Ok(CollectedDeriveOutput {
        queries: all_queries,
        vertices: all_vertices,
        edges: all_edges,
        affected,
    })
}

/// Extract vertex and edge inspection data from a DeriveClause + bindings row.
fn extract_vertex_edge_data(
    derive_clause: &DeriveClause,
    row: &FactRow,
    vertices: &mut HashMap<String, Vec<Properties>>,
    edges: &mut Vec<DerivedEdge>,
) {
    match derive_clause {
        DeriveClause::Patterns(patterns) => {
            for pattern in patterns {
                extract_from_pattern(pattern, row, vertices, edges);
            }
        }
        DeriveClause::Merge(a, b) => {
            // MERGE produces an edge between two existing nodes, no new vertices
            let source_props = node_properties_from_binding(a, row);
            let target_props = node_properties_from_binding(b, row);
            edges.push(DerivedEdge {
                edge_type: "MERGED_WITH".to_string(),
                source_label: node_label_from_binding(a, row),
                source_properties: source_props,
                target_label: node_label_from_binding(b, row),
                target_properties: target_props,
                edge_properties: Properties::new(),
            });
        }
    }
}

/// Extract vertex/edge data from a single DerivePattern.
fn extract_from_pattern(
    pattern: &DerivePattern,
    row: &FactRow,
    vertices: &mut HashMap<String, Vec<Properties>>,
    edges: &mut Vec<DerivedEdge>,
) {
    let source = &pattern.source;
    let target = &pattern.target;
    let edge = &pattern.edge;

    let source_label = source
        .labels
        .first()
        .cloned()
        .unwrap_or_else(|| node_label_from_binding(&source.variable, row));
    let target_label = target
        .labels
        .first()
        .cloned()
        .unwrap_or_else(|| node_label_from_binding(&target.variable, row));

    let source_props = node_properties_from_binding(&source.variable, row);
    let target_props = node_properties_from_binding(&target.variable, row);

    if source.is_new {
        vertices
            .entry(source_label.clone())
            .or_default()
            .push(source_props.clone());
    }
    if target.is_new {
        vertices
            .entry(target_label.clone())
            .or_default()
            .push(target_props.clone());
    }

    let edge_props = edge
        .properties
        .as_ref()
        .and_then(|expr| eval_map_expr(expr, row))
        .unwrap_or_default();

    edges.push(DerivedEdge {
        edge_type: edge.edge_type.clone(),
        source_label,
        source_properties: source_props,
        target_label,
        target_properties: target_props,
        edge_properties: edge_props,
    });
}

/// Extract properties from a binding row for a node variable.
fn node_properties_from_binding(var: &str, row: &FactRow) -> Properties {
    use uni_common::Value;
    match row.get(var) {
        Some(Value::Node(node)) => node.properties.clone(),
        Some(Value::Map(map)) => map.clone(),
        _ => Properties::new(),
    }
}

/// Extract the label from a binding row for a node variable.
fn node_label_from_binding(var: &str, row: &FactRow) -> String {
    use uni_common::Value;
    match row.get(var) {
        Some(Value::Node(node)) => node.labels.first().cloned().unwrap_or_default(),
        _ => String::new(),
    }
}

/// Try to evaluate a map expression to Properties.
fn eval_map_expr(expr: &uni_cypher::ast::Expr, row: &FactRow) -> Option<Properties> {
    use uni_common::Value;
    match eval_expr(expr, row) {
        Ok(Value::Map(m)) => Some(m),
        _ => None,
    }
}
