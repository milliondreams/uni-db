// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! DERIVE command execution via `LocyExecutionContext`.
//!
//! Extracted from `uni-locy/src/orchestrator/mod.rs::derive_command`.
//! Uses `LocyExecutionContext` for fact lookup and mutation execution.

use std::collections::HashMap;

use uni_cypher::locy_ast::{DeriveCommand, RuleOutput};
use uni_locy::{CompiledProgram, LocyError, LocyStats};

use super::locy_ast_builder::build_derive_create;

use super::locy_eval::eval_expr;
use super::locy_traits::LocyExecutionContext;

/// Execute a top-level DERIVE command.
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

    let mut affected = 0;
    for clause in &rule.clauses {
        if let RuleOutput::Derive(derive_clause) = &clause.output {
            for row in &filtered {
                let queries = build_derive_create(derive_clause, row)?;
                for query in queries {
                    ctx.execute_mutation(query, HashMap::new()).await?;
                    stats.mutations_executed += 1;
                    affected += 1;
                }
            }
        }
    }

    Ok(affected)
}
