// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! ASSUME block evaluation via `LocyExecutionContext`.
//!
//! Ported from `uni-locy/src/orchestrator/assume.rs`. Uses `LocyExecutionContext`
//! for L0 fork/restore, mutations, and strata re-evaluation.

use std::collections::HashMap;
use std::time::Instant;

use uni_cypher::ast::Query;
use uni_locy::result::CommandResult;
use uni_locy::types::{CompiledAssume, CompiledCommand};
use uni_locy::{CompiledProgram, FactRow, LocyConfig, LocyError, LocyStats};

use super::locy_delta::RowStore;

use super::locy_explain::ProvenanceStore;
use super::locy_traits::LocyExecutionContext;

/// Evaluate an ASSUME block: fork L0, apply mutations, re-evaluate rules,
/// execute body commands, collect results, then restore.
pub async fn evaluate_assume(
    assume: &CompiledAssume,
    parent_program: &CompiledProgram,
    ctx: &dyn LocyExecutionContext,
    config: &LocyConfig,
    stats: &mut LocyStats,
) -> Result<Vec<FactRow>, LocyError> {
    // 1. Fork L0 for hypothetical reasoning
    ctx.fork_l0()
        .await
        .map_err(|e| LocyError::SavepointFailed {
            message: format!("failed to fork L0: {}", e),
        })?;

    // 2. Execute mutations
    if !assume.mutations.is_empty() {
        let query = Query::Single(uni_cypher::ast::Statement {
            clauses: assume.mutations.clone(),
        });
        ctx.execute_mutation(query, HashMap::new()).await?;
        stats.mutations_executed += 1;
    }

    // 3. Re-evaluate the parent program's strata in the mutated state
    let mut assume_derived_store: RowStore = ctx.re_evaluate_strata(parent_program, config).await?;
    stats.queries_executed += 1; // rough accounting for the re-evaluation

    // 4. Also evaluate body program's strata if any (merge into assume_derived_store)
    if !assume.body_program.strata.is_empty() {
        let body_store = ctx.re_evaluate_strata(&assume.body_program, config).await?;
        for (name, rel) in body_store {
            assume_derived_store.insert(name, rel);
        }
    }

    // 5. Execute body commands and collect results
    let mut result_rows = Vec::new();
    let assume_start = Instant::now();
    for cmd in &assume.body_commands {
        let cmd_result = dispatch_body_command(
            cmd,
            parent_program,
            ctx,
            config,
            &mut assume_derived_store,
            stats,
            assume_start,
        )
        .await?;
        match cmd_result {
            CommandResult::Query(rows) => result_rows.extend(rows),
            CommandResult::Cypher(rows) => result_rows.extend(rows),
            _ => {}
        }
    }

    // If no commands produced rows, collect all derived facts
    if result_rows.is_empty() && assume.body_commands.is_empty() {
        for relation in assume_derived_store.values() {
            result_rows.extend(relation.rows.iter().cloned());
        }
    }

    // 6. Restore L0 (discard hypothetical mutations)
    ctx.restore_l0()
        .await
        .map_err(|e| LocyError::SavepointFailed {
            message: format!("failed to restore L0: {}", e),
        })?;

    Ok(result_rows)
}

/// Dispatch a body command inside an ASSUME block.
///
/// Uses Box::pin for recursive async (nested ASSUME may dispatch sub-commands).
fn dispatch_body_command<'a>(
    cmd: &'a CompiledCommand,
    program: &'a CompiledProgram,
    ctx: &'a dyn LocyExecutionContext,
    config: &'a LocyConfig,
    derived_store: &'a mut RowStore,
    stats: &'a mut LocyStats,
    start: Instant,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<CommandResult, LocyError>> + 'a>> {
    Box::pin(async move {
        match cmd {
            CompiledCommand::GoalQuery(gq) => {
                // For FOLD rules (MNOR/MPROD), the SLG resolver does not apply
                // post-fixpoint aggregation and would return raw pre-FOLD match rows.
                // Use the pre-computed `derived_store` (which ran the full native fixpoint
                // including FOLD aggregation and VID→Node enrichment).
                let rule_name_str = gq.rule_name.to_string();
                let is_fold_rule = program
                    .rule_catalog
                    .get(&rule_name_str)
                    .map(|r| r.clauses.iter().any(|c| !c.fold.is_empty()))
                    .unwrap_or(false);

                let fold_relation = if is_fold_rule {
                    derived_store.get(&rule_name_str)
                } else {
                    None
                };
                if let Some(relation) = fold_relation {
                    let rows = relation.rows.clone();
                    let projected = super::locy_query::apply_return_clause(
                        rows,
                        &gq.return_clause,
                        &config.params,
                    )
                    .map_err(|e| LocyError::QueryResolutionError {
                        message: format!("ASSUME FOLD query projection: {e}"),
                    })?;
                    return Ok(CommandResult::Query(projected));
                }

                let rows = super::locy_query::evaluate_query(
                    gq,
                    program,
                    ctx,
                    config,
                    derived_store,
                    stats,
                    start,
                )
                .await?;
                Ok(CommandResult::Query(rows))
            }
            CompiledCommand::DeriveCommand(dc) => {
                let affected = super::locy_derive::derive_command(dc, program, ctx, stats).await?;
                Ok(CommandResult::Derive { affected })
            }
            CompiledCommand::ExplainRule(eq) => {
                let node = super::locy_explain::explain_rule(
                    eq,
                    program,
                    ctx,
                    config,
                    derived_store,
                    stats,
                    None::<&ProvenanceStore>,
                    None,
                )
                .await?;
                Ok(CommandResult::Explain(node))
            }
            CompiledCommand::Abduce(aq) => {
                let result = super::locy_abduce::evaluate_abduce(
                    aq,
                    program,
                    ctx,
                    config,
                    derived_store,
                    stats,
                    None,
                )
                .await?;
                Ok(CommandResult::Abduce(result))
            }
            CompiledCommand::Assume(ca) => {
                let rows = evaluate_assume(ca, program, ctx, config, stats).await?;
                Ok(CommandResult::Assume(rows))
            }
            CompiledCommand::Cypher(q) => {
                let rows = ctx.execute_cypher_read(q.clone()).await?;
                stats.queries_executed += 1;
                Ok(CommandResult::Cypher(rows))
            }
        }
    })
}
