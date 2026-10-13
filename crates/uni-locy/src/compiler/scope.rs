// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Compile-time scope checking of Locy expressions.
//!
//! Every expression slot of a rule except the inside of ALONG is parsed as
//! plain Cypher, so neither a Locy construct written there (`prev.x`) nor a
//! misspelt variable was caught by the compiler: one failed at run time with
//! "No field named …", and in the slots the in-memory evaluator runs (a
//! command's WHERE) an unbound variable was NULL and the command silently
//! matched nothing. This pass checks each slot against the variables visible
//! there at run time.
//!
//! What each slot sees:
//!
//! * the rule body (WHERE, IS-reference arguments, DERIVE properties): the
//!   MATCH pattern's variables, positive IS references' subjects and targets,
//!   generator outputs, and every value column of a positive reference's
//!   rule by its bare name;
//! * ALONG, FOLD arguments and YIELD items: the body plus ALONG names;
//! * BEST BY: also the FOLD outputs and the rule's YIELD columns;
//! * a YIELD item over a FOLD output, the post-FOLD WHERE and REQUIRE: only
//!   what the folded batch holds, the KEY columns under their YIELD names and
//!   the FOLD outputs under their names and YIELD aliases.
//!
//! Variables bound inside a comprehension, quantifier, `reduce` or subquery
//! are local to it and not checked; `$params` are not variables.
//!
//! A clause that references a rule whose schema the compiler does not have
//! (an external or registered rule, or an outer rule seen from an ASSUME body)
//! cannot list that rule's value columns, so its body is checked only for
//! `prev`.

// Rust guideline compliant

use std::collections::{HashMap, HashSet};

use uni_cypher::ast::Expr;
use uni_cypher::locy_ast::{
    DeriveClause, LocyExpr, RuleCondition, RuleDefinition, RuleOutput, resolve_yield_column_names,
};

use super::errors::LocyCompileError;
use super::modules::{self, ModuleContext};
use super::warded::extract_pattern_variables;
use crate::types::CompiledRule;

/// The variables an expression reads from its enclosing scope.
///
/// Variables bound inside a comprehension, quantifier, `reduce`, pattern
/// comprehension or subquery are local to it and are not returned.
pub(super) fn free_variables(expr: &Expr) -> Vec<String> {
    fn walk(expr: &Expr, out: &mut Vec<String>) {
        if let Expr::Variable(v) = expr {
            out.push(v.clone());
        }
        expr.for_each_child_in_scope(&mut |child| walk(child, out));
    }
    let mut out = Vec::new();
    walk(expr, &mut out);
    out
}

/// Checks every rule's expressions; see the module documentation.
///
/// Runs after the type checker's other passes, so a program they reject keeps
/// its error.
///
/// # Errors
///
/// [`LocyCompileError::PrevOutsideAlong`], [`LocyCompileError::UnboundVariable`]
/// or [`LocyCompileError::FoldNotAnAggregateCall`] for the first offending
/// expression, rules in name order.
pub(super) fn check_rule_scopes(
    rule_groups: &HashMap<String, Vec<&RuleDefinition>>,
    compiled_rules: &HashMap<String, CompiledRule>,
    module_ctx: &ModuleContext,
) -> Result<(), LocyCompileError> {
    let mut rule_names: Vec<&String> = rule_groups.keys().collect();
    rule_names.sort();
    for rule_name in rule_names {
        for def in &rule_groups[rule_name] {
            check_clause(rule_name, def, compiled_rules, module_ctx)?;
        }
    }
    Ok(())
}

/// One slot's expressions and the names visible to them.
struct Slot<'a> {
    name: &'static str,
    exprs: Vec<&'a Expr>,
    visible: HashSet<String>,
}

fn check_clause(
    rule: &str,
    def: &RuleDefinition,
    compiled_rules: &HashMap<String, CompiledRule>,
    module_ctx: &ModuleContext,
) -> Result<(), LocyCompileError> {
    for fold in &def.fold {
        if !matches!(fold.aggregate, Expr::FunctionCall { .. }) {
            return Err(LocyCompileError::FoldNotAnAggregateCall {
                rule: rule.to_string(),
                name: fold.name.clone(),
            });
        }
    }

    let pattern_vars = extract_pattern_variables(&def.match_pattern);

    let mut body: HashSet<String> = pattern_vars.clone();
    // Whether every referenced rule's value columns are known.
    let mut closed = true;
    for cond in &def.where_conditions {
        match cond {
            RuleCondition::IsReference(is_ref) if !is_ref.negated => {
                body.extend(is_ref.subjects.iter().cloned());
                body.extend(is_ref.target.iter().cloned());
                let target = modules::resolve_rule_name(module_ctx, &is_ref.rule_name.to_string());
                match compiled_rules.get(&target) {
                    Some(referenced) => body.extend(
                        referenced
                            .yield_schema
                            .iter()
                            .filter(|c| !c.is_key)
                            .map(|c| c.name.clone()),
                    ),
                    None => closed = false,
                }
            }
            RuleCondition::Generator(generator) => body.extend(generator.outputs.iter().cloned()),
            _ => {}
        }
    }

    let along: HashSet<String> = def.along.iter().map(|a| a.name.clone()).collect();
    let folds: HashSet<String> = def.fold.iter().map(|f| f.name.clone()).collect();
    let fold_names: HashSet<&str> = folds.iter().map(String::as_str).collect();
    let yield_items = match &def.output {
        RuleOutput::Yield(yc) => yc.items.as_slice(),
        RuleOutput::Derive(_) => &[],
    };
    let yield_names: HashSet<String> = resolve_yield_column_names(yield_items)
        .into_iter()
        .collect();
    // What the folded batch holds: the KEY columns under their YIELD names,
    // and the FOLD outputs under their own names and their YIELD aliases. A
    // computed YIELD column does not exist yet when the post-FOLD stages run.
    let folded: HashSet<String> = resolve_yield_column_names(yield_items)
        .into_iter()
        .zip(yield_items)
        .filter(|(_, item)| {
            item.is_key || matches!(&item.expr, Expr::Variable(v) if folds.contains(v))
        })
        .map(|(name, _)| name)
        .chain(folds.iter().cloned())
        .collect();
    let union = |sets: &[&HashSet<String>]| -> HashSet<String> {
        sets.iter().flat_map(|s| s.iter().cloned()).collect()
    };

    let mut along_exprs = Vec::new();
    for binding in &def.along {
        collect_cypher_parts(&binding.expr, &mut along_exprs);
    }
    let (post_fold_yield, pre_fold_yield): (Vec<&Expr>, Vec<&Expr>) = yield_items
        .iter()
        .map(|item| &item.expr)
        .partition(|expr| mentions_any(expr, &fold_names));
    let derive_exprs: Vec<&Expr> = match &def.output {
        RuleOutput::Derive(DeriveClause::Patterns(patterns)) => patterns
            .iter()
            .flat_map(|p| {
                [
                    &p.source.properties,
                    &p.edge.properties,
                    &p.target.properties,
                ]
            })
            .filter_map(Option::as_ref)
            .collect(),
        _ => Vec::new(),
    };

    let slots = [
        Slot {
            name: "WHERE",
            exprs: def
                .where_conditions
                .iter()
                .filter_map(|c| match c {
                    RuleCondition::Expression(e) => Some(e),
                    _ => None,
                })
                .collect(),
            visible: body.clone(),
        },
        Slot {
            name: "ALONG",
            exprs: along_exprs,
            visible: union(&[&body, &along]),
        },
        Slot {
            name: "FOLD",
            exprs: def.fold.iter().map(|f| &f.aggregate).collect(),
            visible: union(&[&body, &along]),
        },
        Slot {
            name: "YIELD",
            exprs: pre_fold_yield,
            visible: union(&[&body, &along]),
        },
        Slot {
            name: "YIELD",
            exprs: post_fold_yield,
            visible: folded.clone(),
        },
        Slot {
            name: "DERIVE",
            exprs: derive_exprs,
            visible: union(&[&body, &along]),
        },
        Slot {
            name: "BEST BY",
            exprs: def
                .best_by
                .iter()
                .flat_map(|b| b.items.iter().map(|i| &i.expr))
                .collect(),
            visible: union(&[&body, &along, &folds, &yield_names]),
        },
        Slot {
            name: "post-FOLD WHERE",
            exprs: def.having.iter().collect(),
            visible: folded.clone(),
        },
        Slot {
            name: "REQUIRE",
            exprs: def.require.iter().collect(),
            visible: folded,
        },
    ];

    for slot in &slots {
        for expr in &slot.exprs {
            for variable in free_variables(expr) {
                if slot.visible.contains(&variable) {
                    continue;
                }
                if variable == "prev" && !pattern_vars.contains("prev") {
                    return Err(LocyCompileError::PrevOutsideAlong {
                        rule: rule.to_string(),
                        slot: slot.name.to_string(),
                    });
                }
                if closed {
                    let mut available: Vec<String> = slot.visible.iter().cloned().collect();
                    available.sort();
                    return Err(LocyCompileError::UnboundVariable {
                        rule: rule.to_string(),
                        slot: slot.name.to_string(),
                        variable,
                        available,
                    });
                }
            }
        }
    }
    Ok(())
}

/// The Cypher expressions inside an ALONG expression: the parts that are not
/// `prev.<column>` or Locy arithmetic over it.
fn collect_cypher_parts<'a>(expr: &'a LocyExpr, out: &mut Vec<&'a Expr>) {
    match expr {
        LocyExpr::PrevRef(_) => {}
        LocyExpr::Cypher(e) => out.push(e),
        LocyExpr::BinaryOp { left, right, .. } => {
            collect_cypher_parts(left, out);
            collect_cypher_parts(right, out);
        }
        LocyExpr::UnaryOp(_, inner) => collect_cypher_parts(inner, out),
    }
}

fn mentions_any(expr: &Expr, names: &HashSet<&str>) -> bool {
    free_variables(expr)
        .iter()
        .any(|v| names.contains(v.as_str()))
}
