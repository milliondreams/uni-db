use std::collections::{HashMap, HashSet};

use uni_cypher::ast::Expr;
use uni_cypher::locy_ast::{LocyExpr, LocyYieldItem, RuleCondition, RuleDefinition, RuleOutput};

use super::errors::LocyCompileError;
use super::stratify::StratificationResult;
use crate::types::{CompiledClause, CompiledRule, CompilerWarning, WarningCode, YieldColumn};

/// Validate all rules and produce `CompiledRule` entries plus warnings.
pub fn check(
    rule_groups: &HashMap<String, Vec<&RuleDefinition>>,
    strat: &StratificationResult,
) -> Result<(HashMap<String, CompiledRule>, Vec<CompilerWarning>), LocyCompileError> {
    let mut compiled_rules = HashMap::new();
    let mut warnings = Vec::new();

    // Process rules in deterministic order
    let mut rule_names: Vec<&String> = rule_groups.keys().collect();
    rule_names.sort();

    for rule_name in rule_names {
        let definitions = &rule_groups[rule_name];
        let scc_idx = strat.scc_map[rule_name.as_str()];
        let is_recursive = strat.is_recursive[scc_idx];

        check_mixed_priority(rule_name, definitions)?;

        let yield_schema = infer_yield_schema(rule_name, definitions)?;

        let scc_rules = &strat.sccs[scc_idx];

        let mut clauses = Vec::new();
        for def in definitions {
            // Check prev in any clause that lacks a self-IS-reference within the same SCC
            let has_self_is = def.where_conditions.iter().any(|cond| {
                if let RuleCondition::IsReference(is_ref) = cond {
                    scc_rules.contains(&is_ref.rule_name.to_string())
                } else {
                    false
                }
            });
            if !has_self_is {
                check_prev_in_base_case(rule_name, def)?;
            }

            if is_recursive {
                check_non_monotonic_in_recursion(rule_name, def)?;
                check_msum_warning(rule_name, def, &mut warnings);
            }

            check_best_by_monotonic_fold(rule_name, def)?;

            clauses.push(CompiledClause {
                match_pattern: def.match_pattern.clone(),
                where_conditions: def.where_conditions.clone(),
                along: def.along.clone(),
                fold: def.fold.clone(),
                best_by: def.best_by.clone(),
                output: def.output.clone(),
                priority: def.priority,
            });
        }

        let priority = definitions.first().and_then(|d| d.priority);

        compiled_rules.insert(
            rule_name.clone(),
            CompiledRule {
                name: rule_name.clone(),
                clauses,
                yield_schema,
                priority,
            },
        );
    }

    // Second pass: validate IS reference arity and prev field names (all yield schemas are inferred by now)
    for (rule_name, rule) in &compiled_rules {
        for clause in &rule.clauses {
            // Collect IS references that are within the same SCC (self-IS-refs)
            let scc_idx = strat.scc_map[rule_name.as_str()];
            let scc_rules = &strat.sccs[scc_idx];

            let mut has_self_is = false;
            let mut is_ref_targets = Vec::new();

            for cond in &clause.where_conditions {
                if let RuleCondition::IsReference(is_ref) = cond {
                    let target_name = is_ref.rule_name.to_string();
                    if let Some(target_rule) = compiled_rules.get(&target_name) {
                        let binding_count =
                            is_ref.subjects.len() + is_ref.target.is_some() as usize;
                        if binding_count > target_rule.yield_schema.len() {
                            return Err(LocyCompileError::IsArityMismatch {
                                rule: rule_name.clone(),
                                target: target_name,
                                expected: target_rule.yield_schema.len(),
                                actual: binding_count,
                            });
                        }
                    }

                    if scc_rules.contains(&target_name) {
                        has_self_is = true;
                        is_ref_targets.push(target_name);
                    }
                }
            }

            // For clauses with self-IS-refs, validate that prev fields exist in referenced schemas
            if has_self_is {
                // Collect available columns: yield columns + along names from all IS-referenced rules
                let mut available_cols: HashSet<String> = HashSet::new();
                for target_name in &is_ref_targets {
                    if let Some(target_rule) = compiled_rules.get(target_name) {
                        for col in &target_rule.yield_schema {
                            available_cols.insert(col.name.clone());
                        }
                        for target_clause in &target_rule.clauses {
                            for along in &target_clause.along {
                                available_cols.insert(along.name.clone());
                            }
                        }
                    }
                }

                for along in &clause.along {
                    for prev_field in collect_prev_refs(&along.expr) {
                        if !available_cols.contains(&prev_field) {
                            let mut sorted: Vec<&str> =
                                available_cols.iter().map(|s| s.as_str()).collect();
                            sorted.sort();
                            return Err(LocyCompileError::PrevFieldNotInSchema {
                                rule: rule_name.clone(),
                                field: prev_field,
                                available: sorted.join(", "),
                            });
                        }
                    }
                }
            }
        }
    }

    Ok((compiled_rules, warnings))
}

// ─── Mixed priority ──────────────────────────────────────────────────────────

fn check_mixed_priority(
    rule_name: &str,
    definitions: &[&RuleDefinition],
) -> Result<(), LocyCompileError> {
    if definitions.len() < 2 {
        return Ok(());
    }
    let some_have = definitions.iter().any(|d| d.priority.is_some());
    let some_lack = definitions.iter().any(|d| d.priority.is_none());
    if some_have && some_lack {
        return Err(LocyCompileError::MixedPriority {
            rule: rule_name.to_string(),
        });
    }
    Ok(())
}

// ─── YIELD schema ────────────────────────────────────────────────────────────

fn infer_yield_schema(
    rule_name: &str,
    definitions: &[&RuleDefinition],
) -> Result<Vec<YieldColumn>, LocyCompileError> {
    let mut schema: Option<Vec<YieldColumn>> = None;

    for def in definitions {
        if let RuleOutput::Yield(yc) = &def.output {
            let columns = yield_columns_from_items(&yc.items);
            if let Some(ref existing) = schema {
                if existing.len() != columns.len() {
                    return Err(LocyCompileError::YieldSchemaMismatch {
                        rule: rule_name.to_string(),
                        detail: format!(
                            "clause has {} columns, expected {}",
                            columns.len(),
                            existing.len()
                        ),
                    });
                }
            } else {
                schema = Some(columns);
            }
        }
    }

    Ok(schema.unwrap_or_default())
}

fn yield_columns_from_items(items: &[LocyYieldItem]) -> Vec<YieldColumn> {
    items
        .iter()
        .map(|item| {
            let name = item.alias.clone().unwrap_or_else(|| expr_name(&item.expr));
            YieldColumn {
                name,
                is_key: item.is_key,
            }
        })
        .collect()
}

fn expr_name(expr: &Expr) -> String {
    match expr {
        Expr::Variable(name) => name.clone(),
        Expr::Property(_, prop) => prop.clone(),
        _ => "?".to_string(),
    }
}

// ─── prev in base case ──────────────────────────────────────────────────────

fn check_prev_in_base_case(rule_name: &str, def: &RuleDefinition) -> Result<(), LocyCompileError> {
    for along in &def.along {
        if let Some(field) = find_prev_ref(&along.expr) {
            return Err(LocyCompileError::PrevInBaseCase {
                rule: rule_name.to_string(),
                field,
            });
        }
    }
    Ok(())
}

fn find_prev_ref(expr: &LocyExpr) -> Option<String> {
    match expr {
        LocyExpr::PrevRef(field) => Some(field.clone()),
        LocyExpr::BinaryOp { left, right, .. } => {
            find_prev_ref(left).or_else(|| find_prev_ref(right))
        }
        LocyExpr::UnaryOp(_, inner) => find_prev_ref(inner),
        LocyExpr::Cypher(_) => None,
    }
}

fn collect_prev_refs(expr: &LocyExpr) -> Vec<String> {
    match expr {
        LocyExpr::PrevRef(field) => vec![field.clone()],
        LocyExpr::BinaryOp { left, right, .. } => {
            let mut refs = collect_prev_refs(left);
            refs.extend(collect_prev_refs(right));
            refs
        }
        LocyExpr::UnaryOp(_, inner) => collect_prev_refs(inner),
        LocyExpr::Cypher(_) => vec![],
    }
}

// ─── Non-monotonic in recursion ──────────────────────────────────────────────

fn check_non_monotonic_in_recursion(
    rule_name: &str,
    def: &RuleDefinition,
) -> Result<(), LocyCompileError> {
    for fold in &def.fold {
        if let Some(func_name) = extract_function_name(&fold.aggregate) {
            let upper = func_name.to_uppercase();
            if matches!(upper.as_str(), "SUM" | "COUNT" | "AVG" | "MIN" | "MAX") {
                return Err(LocyCompileError::NonMonotonicInRecursion {
                    rule: rule_name.to_string(),
                    aggregate: func_name,
                });
            }
        }
    }
    Ok(())
}

// ─── MSUM warning ────────────────────────────────────────────────────────────

fn check_msum_warning(rule_name: &str, def: &RuleDefinition, warnings: &mut Vec<CompilerWarning>) {
    for fold in &def.fold {
        if let Some(func_name) = extract_function_name(&fold.aggregate)
            && func_name.to_uppercase() == "MSUM"
            && let Expr::FunctionCall { args, .. } = &fold.aggregate
        {
            let is_literal = args
                .first()
                .is_some_and(|arg| matches!(arg, Expr::Literal(_)));
            if !is_literal {
                warnings.push(CompilerWarning {
                    code: WarningCode::MsumNonNegativity,
                    message: format!(
                        "MSUM argument in fold '{}' may be negative; \
                         ensure non-negativity for convergence",
                        fold.name
                    ),
                    rule_name: rule_name.to_string(),
                });
            }
        }
    }
}

// ─── BEST BY + monotonic fold ────────────────────────────────────────────────

fn check_best_by_monotonic_fold(
    rule_name: &str,
    def: &RuleDefinition,
) -> Result<(), LocyCompileError> {
    if def.best_by.is_none() {
        return Ok(());
    }
    for fold in &def.fold {
        if let Some(func_name) = extract_function_name(&fold.aggregate) {
            let upper = func_name.to_uppercase();
            if matches!(upper.as_str(), "MSUM" | "MMAX" | "MMIN" | "MCOUNT") {
                return Err(LocyCompileError::BestByWithMonotonicFold {
                    rule: rule_name.to_string(),
                    fold: func_name,
                });
            }
        }
    }
    Ok(())
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn extract_function_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::FunctionCall { name, .. } => Some(name.clone()),
        _ => None,
    }
}
