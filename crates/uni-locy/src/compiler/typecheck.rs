use std::collections::{HashMap, HashSet};

use uni_cypher::ast::Expr;
use uni_cypher::locy_ast::{
    AlongBinding, FoldBinding, LocyExpr, LocyYieldItem, RuleCondition, RuleDefinition, RuleOutput,
};

use super::errors::LocyCompileError;
use super::stratify::StratificationResult;
use crate::types::{
    CompiledClause, CompiledModel, CompiledRule, CompilerWarning, ModelInvocation, WarningCode,
    YieldColumn,
};
use uni_cypher::locy_ast::OutputType;

/// Validate all rules and produce `CompiledRule` entries plus warnings.
///
/// `model_catalog` carries the Phase B `CREATE MODEL` declarations; rule
/// bodies that reference a model name via function-call syntax are
/// validated for arity here. An empty catalog (the legacy path) is
/// equivalent to "no models registered".
pub fn check(
    rule_groups: &HashMap<String, Vec<&RuleDefinition>>,
    strat: &StratificationResult,
    model_catalog: &HashMap<String, CompiledModel>,
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

        let mut yield_schema = infer_yield_schema(rule_name, definitions)?;

        // Implicit PROB: if a fold uses MNOR/MPROD, mark the matching yield column as PROB
        for def in definitions.iter() {
            for fold in &def.fold {
                if let Some(func_name) = extract_function_name(&fold.aggregate)
                    && matches!(func_name.to_uppercase().as_str(), "MNOR" | "MPROD")
                    && let Some(col) = yield_schema.iter_mut().find(|c| c.name == fold.name)
                {
                    col.is_prob = true;
                }
            }
        }

        // Phase B A5: auto-flag PROB for YIELD items whose expression is
        // a neural-model invocation declaring `OUTPUT PROB`. The yield
        // column's name is the explicit alias or the model's output
        // identifier when used bare. We resolve to the column name in
        // `yield_schema` produced by `infer_yield_schema`.
        for def in definitions.iter() {
            if let RuleOutput::Yield(yc) = &def.output {
                for item in &yc.items {
                    let Expr::FunctionCall { name, .. } = &item.expr else {
                        continue;
                    };
                    let Some(model) = model_catalog.get(name) else {
                        continue;
                    };
                    if model.output_type != OutputType::Prob {
                        continue;
                    }
                    // Column name: alias if present, else fall back to the
                    // function-call name (mirroring infer_yield_schema's
                    // alias-then-default policy).
                    let col_name = item.alias.clone().unwrap_or_else(|| name.clone());
                    if let Some(col) = yield_schema.iter_mut().find(|c| c.name == col_name) {
                        col.is_prob = true;
                    }
                }
            }
        }

        // Validate: at most 1 PROB column per rule
        let prob_count = yield_schema.iter().filter(|c| c.is_prob).count();
        if prob_count > 1 {
            return Err(LocyCompileError::MultipleProbColumns {
                rule: rule_name.clone(),
                count: prob_count,
            });
        }

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
                check_probability_domain_warning(rule_name, def, &mut warnings);
                // F1: clause has FOLD + recursive IS-ref (same SCC) + no ALONG
                // → almost certainly a semantic mistake (Stress Corpus B3).
                check_fold_in_recursive_path(rule_name, def, scc_rules, &mut warnings);
            }

            check_best_by_monotonic_fold(rule_name, def)?;

            // Validate model invocations in this clause's body. Each call
            // `model_name(arg1, ..., argN)` must (1) refer to a declared
            // model, and (2) supply N arguments matching `INPUT` arity.
            // Phase C C4: emits `UncalibratedNeuralPredicate` when an
            // invoked PROB model has no CALIBRATION declared.
            check_model_invocations(rule_name, def, model_catalog, &mut warnings)?;
            // Phase C F2: detect cross-model input sharing that
            // composes under independence-by-default — fires F2a /
            // F2b unless all involved models carry `@independent`.
            check_shared_neural_inputs(rule_name, def, model_catalog, &mut warnings);
            // Phase D F3 case 3: rule body has both `IS p` and `IS NOT q`
            // on the same subject — emits PositiveComplementCorrelation.
            check_positive_complement_pair(rule_name, def, &mut warnings);

            // HAVING (post-FOLD WHERE) requires a FOLD clause.
            if !def.having.is_empty() && def.fold.is_empty() {
                return Err(LocyCompileError::HavingWithoutFold {
                    rule: rule_name.clone(),
                });
            }

            // Phase B Slice 3 + A4 follow-up: extract model invocations
            // from YIELD items, ALONG bindings, and FOLD aggregate
            // expressions. All three positions are lifted into hidden
            // `__model_<n>_<idx>` columns produced by the runtime's
            // `LocyModelInvokeExec` (inserted by the planner between
            // the clause body and `LocyProject`). Property-access
            // feature exprs (e.g. `scorer(s.tier)`) accumulate hidden
            // YIELD items so the standard property-materialization
            // pipeline feeds the invocation pass.
            let extracted = extract_model_invocations(rule_name, def, model_catalog)?;

            clauses.push(CompiledClause {
                match_pattern: def.match_pattern.clone(),
                where_conditions: def.where_conditions.clone(),
                along: extracted.along,
                fold: extracted.fold,
                having: def.having.clone(),
                best_by: def.best_by.clone(),
                output: extracted.output,
                priority: def.priority,
                model_invocations: extracted.invocations,
                hidden_yield_cols: extracted.hidden_yield_cols,
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
    // Also runs Phase D F3 case 2 detection — needs all rules'
    // yield_schemas in place to identify PROB-bearing targets.
    for (rule_name, rule) in &compiled_rules {
        for clause in &rule.clauses {
            check_cross_predicate_correlation(rule_name, clause, &compiled_rules, &mut warnings);
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
                // Check is_prob consistency across clauses
                for (i, (e, c)) in existing.iter().zip(columns.iter()).enumerate() {
                    if e.is_prob != c.is_prob {
                        return Err(LocyCompileError::YieldSchemaMismatch {
                            rule: rule_name.to_string(),
                            detail: format!(
                                "column {} '{}' has inconsistent PROB annotation across clauses",
                                i, e.name
                            ),
                        });
                    }
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
                is_prob: item.is_prob,
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

/// Returns `true` if the fold function name is a monotonic variant (MSUM, MMAX, etc.).
fn is_monotonic_fold_name(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "MSUM" | "MMAX" | "MMIN" | "MCOUNT" | "MNOR" | "MPROD"
    )
}

fn check_non_monotonic_in_recursion(
    rule_name: &str,
    def: &RuleDefinition,
) -> Result<(), LocyCompileError> {
    for fold in &def.fold {
        if let Some(func_name) = extract_function_name(&fold.aggregate)
            && !is_monotonic_fold_name(&func_name)
        {
            return Err(LocyCompileError::NonMonotonicInRecursion {
                rule: rule_name.to_string(),
                aggregate: func_name,
            });
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

// ─── MNOR/MPROD probability domain warning ───────────────────────────────────

fn check_probability_domain_warning(
    rule_name: &str,
    def: &RuleDefinition,
    warnings: &mut Vec<CompilerWarning>,
) {
    for fold in &def.fold {
        if let Some(func_name) = extract_function_name(&fold.aggregate)
            && matches!(func_name.to_uppercase().as_str(), "MNOR" | "MPROD")
            && let Expr::FunctionCall { args, .. } = &fold.aggregate
        {
            let is_literal = args
                .first()
                .is_some_and(|arg| matches!(arg, Expr::Literal(_)));
            if !is_literal {
                warnings.push(CompilerWarning {
                    code: WarningCode::ProbabilityDomainViolation,
                    message: format!(
                        "{} argument in fold '{}' may be outside [0,1]; \
                         ensure values are valid probabilities for convergence",
                        func_name.to_uppercase(),
                        fold.name
                    ),
                    rule_name: rule_name.to_string(),
                });
            }
        }
    }
}

// ─── Phase D F3 case 3: positive + complement on same subject ──────────────

fn check_positive_complement_pair(
    rule_name: &str,
    def: &RuleDefinition,
    warnings: &mut Vec<CompilerWarning>,
) {
    // Index IS-refs by their first subject variable. The first subject
    // is the canonical "head" — e.g. `s IS p` or `(s, t) IS p`.
    let mut by_subject: HashMap<String, (Vec<String>, Vec<String>)> = HashMap::new();
    for cond in &def.where_conditions {
        if let RuleCondition::IsReference(is_ref) = cond
            && let Some(subj) = is_ref.subjects.first()
        {
            let entry = by_subject.entry(subj.clone()).or_default();
            let target = is_ref.rule_name.to_string();
            if is_ref.negated {
                entry.1.push(target);
            } else {
                entry.0.push(target);
            }
        }
    }
    for (subj, (positives, negateds)) in by_subject {
        for p in &positives {
            for q in &negateds {
                if p == q {
                    continue;
                }
                warnings.push(CompilerWarning {
                    code: WarningCode::PositiveComplementCorrelation,
                    message: format!(
                        "rule '{rule_name}': WHERE {subj} IS {p}, {subj} IS NOT {q} — \
                         positive and complement on the same subject correlate when \
                         their support sets overlap (CrossGroupCorrelationNotExact). \
                         Use BDD/TopKProofs for exact composition, or accept the \
                         independence approximation.",
                    ),
                    rule_name: rule_name.to_string(),
                });
            }
        }
    }
}

// ─── Phase D F3 case 2: cross-predicate correlation ────────────────────────

fn check_cross_predicate_correlation(
    rule_name: &str,
    clause: &CompiledClause,
    compiled_rules: &HashMap<String, CompiledRule>,
    warnings: &mut Vec<CompilerWarning>,
) {
    // Index positive IS-refs by their first subject variable, recording
    // target rule names. Negated refs are case 3's concern, not this one.
    let mut by_subject: HashMap<String, Vec<String>> = HashMap::new();
    for cond in &clause.where_conditions {
        if let RuleCondition::IsReference(is_ref) = cond
            && !is_ref.negated
            && let Some(subj) = is_ref.subjects.first()
        {
            by_subject
                .entry(subj.clone())
                .or_default()
                .push(is_ref.rule_name.to_string());
        }
    }
    for (subj, mut targets) in by_subject {
        // Dedupe and keep only distinct targets.
        targets.sort();
        targets.dedup();
        if targets.len() < 2 {
            continue;
        }
        // Count PROB-bearing targets.
        let prob_targets: Vec<&str> = targets
            .iter()
            .filter(|t| {
                compiled_rules
                    .get(t.as_str())
                    .is_some_and(|r| r.yield_schema.iter().any(|c| c.is_prob))
            })
            .map(String::as_str)
            .collect();
        if prob_targets.len() < 2 {
            continue;
        }
        warnings.push(CompilerWarning {
            code: WarningCode::CrossPredicateCorrelation,
            message: format!(
                "rule '{rule_name}': WHERE {subj} IS {} (multiple PROB-bearing IS-refs \
                 on the same subject) — the implicit conjunction assumes \
                 independence between predicates, which is wrong when their \
                 support sets overlap. Use BDD/TopKProofs for exact composition, \
                 or accept the independence approximation.",
                prob_targets.join(", ")
            ),
            rule_name: rule_name.to_string(),
        });
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
        if let Some(func_name) = extract_function_name(&fold.aggregate)
            && is_monotonic_fold_name(&func_name)
        {
            return Err(LocyCompileError::BestByWithMonotonicFold {
                rule: rule_name.to_string(),
                fold: func_name,
            });
        }
    }
    Ok(())
}

// ─── F1: FOLD in recursive path without ALONG ───────────────────────────────

/// Phase B F1 (Stress Corpus B3): a clause has a FOLD aggregate AND
/// references a rule in its own SCC (recursive IS-ref) AND lacks an
/// `ALONG` clause. This pattern is almost always a semantic mistake —
/// FOLD groups by KEY columns, but in recursive contexts the user
/// usually means per-path aggregation, which requires `ALONG`.
///
/// Conservative scope: only fires for self-SCC IS-refs (the common
/// recursive case). Cross-SCC recursion via non-recursive stratification
/// won't trigger it.
fn check_fold_in_recursive_path(
    rule_name: &str,
    def: &RuleDefinition,
    scc_rules: &std::collections::HashSet<String>,
    warnings: &mut Vec<CompilerWarning>,
) {
    if def.fold.is_empty() || !def.along.is_empty() {
        return;
    }
    let has_recursive_is_ref = def.where_conditions.iter().any(|cond| {
        if let RuleCondition::IsReference(is_ref) = cond {
            scc_rules.contains(&is_ref.rule_name.to_string())
        } else {
            false
        }
    });
    if has_recursive_is_ref {
        warnings.push(CompilerWarning {
            code: WarningCode::FoldInRecursivePath,
            message: format!(
                "rule '{}' has both FOLD and a recursive IS-reference but no ALONG \
                 clause; FOLD groups by KEY columns, not by path — did you mean to \
                 add ALONG for per-path aggregation? (Stress Corpus B3)",
                rule_name
            ),
            rule_name: rule_name.to_string(),
        });
    }
}

// ─── Model invocation validation ─────────────────────────────────────────────

/// Walk a clause body's expressions and validate any `model_name(args...)`
/// function-call that resolves to a Phase-B model in `model_catalog`. We
/// only flag KNOWN model names — bare function calls that don't match a
/// declared model are treated as built-ins (handled elsewhere) so that an
/// undeclared model name doesn't poison every function-call site.
///
/// Validations performed here:
///   * Arity: number of args must equal `model.inputs.len()`.
///
/// Output-type mismatch is intentionally deferred until the rule-output
/// surface is known (Phase B Slice 3 wires PROB column inference to model
/// output type). Unknown-model errors fire only when the caller writes
/// the call as `model_name(...)` AND the name shadows a declared model
/// that doesn't exist yet — currently surfaced via the rule's normal
/// `UndefinedRule` path.
fn check_model_invocations(
    rule_name: &str,
    def: &RuleDefinition,
    model_catalog: &HashMap<String, crate::types::CompiledModel>,
    warnings: &mut Vec<CompilerWarning>,
) -> Result<(), LocyCompileError> {
    if model_catalog.is_empty() {
        return Ok(());
    }
    // Track (rule, model) pairs we've already warned about so a model
    // invoked from multiple sites in the same rule warns just once.
    let mut warned: HashSet<String> = HashSet::new();
    // Helper closure: validates arity AND emits C4 warning.
    let mut visit = |expr: &Expr| -> Result<(), LocyCompileError> {
        walk_function_calls(expr, &mut |name, arg_count| {
            let Some(model) = model_catalog.get(name) else {
                return Ok(());
            };
            // Arity check.
            let expected = model.inputs.len();
            if arg_count != expected {
                return Err(LocyCompileError::ModelArityMismatch {
                    name: name.to_string(),
                    rule: rule_name.to_string(),
                    expected,
                    actual: arg_count,
                });
            }
            // C4: emit `UncalibratedNeuralPredicate` for PROB models
            // without an active calibration declaration.
            if model.output_type == uni_cypher::locy_ast::OutputType::Prob
                && matches!(
                    model.calibration,
                    None | Some(uni_cypher::locy_ast::CalibrationMethod::None)
                )
                && !warned.contains(&model.name)
            {
                warnings.push(CompilerWarning {
                    code: WarningCode::UncalibratedNeuralPredicate,
                    message: format!(
                        "rule '{}' invokes neural model '{}' (PROB output) with no \
                         CALIBRATION; downstream MNOR/MPROD/complement compound any \
                         miscalibration. Run `CALIBRATE {} ON MATCH ... TARGET ... \
                         METHOD platt_scaling` to fit a transform, or acknowledge the \
                         risk with `CALIBRATION none` in the model declaration",
                        rule_name, model.name, model.name
                    ),
                    rule_name: rule_name.to_string(),
                });
                warned.insert(model.name.clone());
            }
            Ok(())
        })
    };
    for cond in &def.where_conditions {
        if let RuleCondition::Expression(e) = cond {
            visit(e)?;
        }
    }
    for fold in &def.fold {
        visit(&fold.aggregate)?;
    }
    if let RuleOutput::Yield(yc) = &def.output {
        for item in &yc.items {
            visit(&item.expr)?;
        }
    }
    // Phase B follow-up (Slice 7): with arity validation done above,
    // reject any *valid-arity* WHERE-position model invocations with
    // a clear error. Runtime support requires splitting `body_logical`
    // at the planner level into pre-filter (MATCH+projection+invocation)
    // and post-filter (WHERE+YIELD) halves so the classifier can run
    // between them. Until that lands, direct users to lift the call
    // into YIELD where the current invocation machinery handles it.
    for cond in &def.where_conditions {
        if let RuleCondition::Expression(e) = cond {
            let mut found_model: Option<String> = None;
            walk_function_calls(e, &mut |name, _arg_count| {
                if found_model.is_none() && model_catalog.contains_key(name) {
                    found_model = Some(name.to_string());
                }
                Ok(())
            })?;
            if let Some(model) = found_model {
                return Err(LocyCompileError::WhereModelInvocationNotYetSupported {
                    rule: rule_name.to_string(),
                    model,
                });
            }
        }
    }
    Ok(())
}

/// Visit every `Expr::FunctionCall` sub-node, calling `f(name, arg_count)`.
/// Phase C F2: emit `SharedNeuralInputArgument` (F2a) and
/// `SharedNeuralFeatureValue` (F2b) warnings when multiple
/// neural-model invocations in the same rule share an input
/// variable or an equivalent feature expression. Suppressed when
/// every invocation involved carries the `@independent` annotation
/// on its `CREATE MODEL` declaration.
///
/// Pattern modelled on `check_fold_in_recursive_path` — pushes
/// directly to the per-rule warnings vec.
fn check_shared_neural_inputs(
    rule_name: &str,
    def: &RuleDefinition,
    model_catalog: &HashMap<String, CompiledModel>,
    warnings: &mut Vec<CompilerWarning>,
) {
    if model_catalog.is_empty() {
        return;
    }
    // Collect (model_name, feature_expr) pairs from every model
    // invocation in this rule. F3-AlongFoldExtract: walk YIELD, ALONG,
    // and FOLD positions. ALONG carries a `LocyExpr` (Cypher + prev
    // refs); FOLD carries an `Expr` (the aggregate, e.g. `MNOR(scorer(s))`).
    let mut invocations: Vec<(&str, &Vec<Expr>)> = Vec::new();
    fn collect_from_cypher_expr<'a>(
        expr: &'a Expr,
        model_catalog: &HashMap<String, CompiledModel>,
        out: &mut Vec<(&'a str, &'a Vec<Expr>)>,
    ) {
        if let Expr::FunctionCall { name, args, .. } = expr {
            if let Some(model) = model_catalog.get(name)
                && args.len() == model.inputs.len()
            {
                out.push((name.as_str(), args));
            }
            // Recurse into args (composed model calls inside
            // aggregates like `MNOR(scorer(s))`).
            for a in args {
                collect_from_cypher_expr(a, model_catalog, out);
            }
        }
    }
    fn collect_from_locy_expr<'a>(
        lexpr: &'a uni_cypher::locy_ast::LocyExpr,
        model_catalog: &HashMap<String, CompiledModel>,
        out: &mut Vec<(&'a str, &'a Vec<Expr>)>,
    ) {
        use uni_cypher::locy_ast::LocyExpr;
        match lexpr {
            LocyExpr::Cypher(e) => collect_from_cypher_expr(e, model_catalog, out),
            LocyExpr::BinaryOp { left, right, .. } => {
                collect_from_locy_expr(left, model_catalog, out);
                collect_from_locy_expr(right, model_catalog, out);
            }
            LocyExpr::UnaryOp(_, inner) => collect_from_locy_expr(inner, model_catalog, out),
            LocyExpr::PrevRef(_) => {}
        }
    }
    if let RuleOutput::Yield(yc) = &def.output {
        for item in &yc.items {
            collect_from_cypher_expr(&item.expr, model_catalog, &mut invocations);
        }
    }
    for along in &def.along {
        collect_from_locy_expr(&along.expr, model_catalog, &mut invocations);
    }
    for fold in &def.fold {
        collect_from_cypher_expr(&fold.aggregate, model_catalog, &mut invocations);
    }
    if invocations.len() < 2 {
        return;
    }
    let all_independent = |models: &[&str]| -> bool {
        models.iter().all(|m| {
            model_catalog
                .get(*m)
                .is_some_and(|cm| cm.annotations.independent)
        })
    };
    // Returns Some(sorted-deduped models) when ≥ 2 distinct non-independent
    // models share a group; None otherwise (no warning to emit).
    fn dedup_sorted<'a>(models: &[&'a str]) -> Vec<&'a str> {
        let mut unique: Vec<&'a str> = models.to_vec();
        unique.sort();
        unique.dedup();
        unique
    }
    // ── F2a: group by shared input-variable name ────────────────
    let mut by_var: HashMap<String, Vec<&str>> = HashMap::new();
    for (model, args) in &invocations {
        for a in args.iter() {
            if let Expr::Variable(v) = a {
                by_var.entry(v.clone()).or_default().push(model);
            }
        }
    }
    let mut warned_a: HashSet<String> = HashSet::new();
    for (var, models) in &by_var {
        let unique = dedup_sorted(models);
        if unique.len() >= 2 && !all_independent(&unique) && warned_a.insert(var.clone()) {
            warnings.push(CompilerWarning {
                code: WarningCode::SharedNeuralInputArgument,
                message: format!(
                    "rule '{}' invokes multiple neural models \
                     ({}) on the same input variable '{}'; under \
                     independence-by-default the composed \
                     probability assumes independence which is \
                     likely wrong (rollout D-8). Either annotate \
                     the models with `@independent` (if you have \
                     evidence they're conditionally independent \
                     given upstream context), or use \
                     `CALIBRATE` / TopKProofs for honest \
                     composition.",
                    rule_name,
                    unique.join(", "),
                    var
                ),
                rule_name: rule_name.to_string(),
            });
        }
    }
    // ── F2b: group by structural equality of NON-Variable feature exprs ──
    let mut by_expr: HashMap<String, Vec<&str>> = HashMap::new();
    for (model, args) in &invocations {
        for a in args.iter() {
            // Skip plain variables — F2a covers those.
            if matches!(a, Expr::Variable(_)) {
                continue;
            }
            let key = format!("{:?}", a);
            by_expr.entry(key).or_default().push(model);
        }
    }
    for models in by_expr.values() {
        let unique = dedup_sorted(models);
        if unique.len() >= 2 && !all_independent(&unique) {
            warnings.push(CompilerWarning {
                code: WarningCode::SharedNeuralFeatureValue,
                message: format!(
                    "rule '{}' invokes multiple neural models \
                     ({}) on an equivalent feature expression; \
                     even with distinct binding variables the \
                     probabilities share a common input value and \
                     cannot be composed under independence \
                     (rollout D-8). Annotate `@independent` or \
                     use TopKProofs for honest composition.",
                    rule_name,
                    unique.join(", ")
                ),
                rule_name: rule_name.to_string(),
            });
        }
    }
    // ── F2c (F3 case 4): group by retrieval-feature property path ──
    // Catches `similar_to(prop, _)` / `semantic_match(prop, _)` calls
    // that share the same `Property(Variable(v), prop)` operand across
    // ≥ 2 distinct model invocations. Suppressed by `@independent`.
    let mut by_retrieval_prop: HashMap<(String, String), Vec<&str>> = HashMap::new();
    for (model, args) in &invocations {
        for a in args.iter() {
            if let Expr::FunctionCall {
                name: fname,
                args: inner,
                ..
            } = a
                && matches!(fname.as_str(), "similar_to" | "semantic_match")
                && let Some(first) = inner.first()
                && let Expr::Property(boxed, prop) = first
                && let Expr::Variable(v) = boxed.as_ref()
            {
                by_retrieval_prop
                    .entry((v.clone(), prop.clone()))
                    .or_default()
                    .push(model);
            }
        }
    }
    for ((v, prop), models) in &by_retrieval_prop {
        let unique = dedup_sorted(models);
        if unique.len() >= 2 && !all_independent(&unique) {
            warnings.push(CompilerWarning {
                code: WarningCode::SharedRetrievalContext,
                message: format!(
                    "rule '{rule_name}' invokes multiple neural models ({}) \
                     whose features retrieve from the same property '{v}.{prop}'; \
                     independence between models is unlikely when both condition \
                     on the same retrieval evidence. Annotate `@independent` or \
                     use TopKProofs for honest composition.",
                    unique.join(", ")
                ),
                rule_name: rule_name.to_string(),
            });
        }
    }
}

fn walk_function_calls<F>(expr: &Expr, f: &mut F) -> Result<(), LocyCompileError>
where
    F: FnMut(&str, usize) -> Result<(), LocyCompileError>,
{
    match expr {
        Expr::FunctionCall { name, args, .. } => {
            f(name, args.len())?;
            for a in args {
                walk_function_calls(a, f)?;
            }
            Ok(())
        }
        Expr::BinaryOp { left, right, .. } => {
            walk_function_calls(left, f)?;
            walk_function_calls(right, f)
        }
        Expr::UnaryOp { expr: inner, .. } => walk_function_calls(inner, f),
        Expr::List(items) => {
            for i in items {
                walk_function_calls(i, f)?;
            }
            Ok(())
        }
        Expr::Map(entries) => {
            for (_, v) in entries {
                walk_function_calls(v, f)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

// ─── Slice 3: model invocation extraction + YIELD rewriting ─────────────────

/// Phase B Slice 3: lift neural-model calls out of YIELD items.
///
/// For each top-level `YIELD ... model_name(args) [AS alias]` where
/// `model_name` is declared in `model_catalog`, this emits a
/// [`ModelInvocation`] entry whose `output_column` matches the YIELD
/// item's resolved column name, and rewrites the YIELD item's expression
/// to a placeholder literal `0.0`. At runtime, the body projection still
/// materializes a column with that name (initially zero); the
/// invocation pass then **overwrites** that column with the classifier's
/// per-row output before any downstream operator (FOLD, IS-ref) reads
/// from it.
///
/// Why a literal placeholder rather than a synthetic column? The body's
/// projection is built by the planner from the (rewritten) YIELD items
/// alone — there's no plan-node insertion point between projection and
/// downstream operators where a brand-new column could be threaded in
/// without restructuring the planner. Overwriting an existing column
/// keeps the diff to the runtime alone.
///
/// Slice 3 limits the extraction to direct top-level model calls in
/// YIELD items (the common case from DEEP_LOCY.md §9.4). Nested calls
/// (`f(model_x(s))`, `model_x(s) + 1`), invocations in WHERE, FOLD, or
/// ALONG are not lifted in this slice — they parse and validate (arity)
/// but won't execute at runtime. Follow-up slices extend the lift.
pub(crate) struct ExtractedInvocations {
    pub output: RuleOutput,
    pub along: Vec<AlongBinding>,
    pub fold: Vec<FoldBinding>,
    pub invocations: Vec<ModelInvocation>,
    pub hidden_yield_cols: Vec<String>,
}

/// Accumulator state for `extract_model_invocations`. Walks YIELD,
/// ALONG, and FOLD positions of a clause body; whenever a known
/// model FunctionCall is encountered, lifts it into a fresh
/// `ModelInvocation` and replaces the call site with a
/// `Variable("__model_<name>_<idx>")` reference. The runtime
/// `LocyModelInvokeExec` (planner-inserted between the body and
/// `LocyProject`) materializes the column.
struct InvocationLifter<'a> {
    rule_name: &'a str,
    model_catalog: &'a HashMap<String, CompiledModel>,
    invocations: Vec<ModelInvocation>,
    /// Pairs of `(__feat_<var>_<prop>, Property(Variable(var), prop))`
    /// expressions to emit as hidden YIELD items. Deduplicated by
    /// column name via `seen_hidden`.
    hidden_items: Vec<(String, Expr)>,
    seen_hidden: std::collections::HashSet<String>,
    counter: usize,
}

/// Return type of `InvocationLifter::validate_features`: rewritten feature
/// argument list plus the `(var, property)` pairs accumulated for hidden-YIELD
/// materialization.
type ValidatedFeatures = Result<(Vec<Expr>, Vec<(String, String)>), LocyCompileError>;

impl<'a> InvocationLifter<'a> {
    fn new(rule_name: &'a str, model_catalog: &'a HashMap<String, CompiledModel>) -> Self {
        Self {
            rule_name,
            model_catalog,
            invocations: Vec::new(),
            hidden_items: Vec::new(),
            seen_hidden: std::collections::HashSet::new(),
            counter: 0,
        }
    }

    /// Validate feature expressions and emit hidden YIELD items for
    /// shapes that require pre-materialization (`Property(Variable,
    /// prop)` for graph properties; `similar_to(...)` /
    /// `semantic_match(...)` for retrieval-backed features). Returns
    /// the possibly-rewritten feature_exprs and the per-invocation
    /// `feature_property_refs` for record-keeping.
    fn validate_features(&mut self, model_name: &str, args: &[Expr]) -> ValidatedFeatures {
        let mut feature_property_refs = Vec::new();
        let mut rewritten = Vec::with_capacity(args.len());
        for fexpr in args {
            match fexpr {
                Expr::Variable(_) => {
                    rewritten.push(fexpr.clone());
                }
                Expr::Property(boxed_inner, prop)
                    if matches!(boxed_inner.as_ref(), Expr::Variable(_)) =>
                {
                    if let Expr::Variable(v) = boxed_inner.as_ref() {
                        feature_property_refs.push((v.clone(), prop.clone()));
                        let col_name = format!("__feat_{}_{}", v, prop);
                        if !self.seen_hidden.contains(&col_name) {
                            self.seen_hidden.insert(col_name.clone());
                            let hidden_expr =
                                Expr::Property(Box::new(Expr::Variable(v.clone())), prop.clone());
                            self.hidden_items.push((col_name, hidden_expr));
                        }
                    }
                    rewritten.push(fexpr.clone());
                }
                Expr::FunctionCall {
                    name, args: fargs, ..
                } if matches!(
                    name.as_str(),
                    "avg_neighbor" | "max_neighbor" | "sum_neighbor"
                ) =>
                {
                    // Phase D D1 graph-structural: one-hop neighborhood
                    // aggregators. Args: subject Variable, rel-type
                    // string literal, property string literal, and an
                    // optional 4th direction string literal
                    // ('OUTGOING' | 'INCOMING' | 'BOTH'; default
                    // 'OUTGOING').
                    if fargs.len() != 3 && fargs.len() != 4 {
                        return Err(LocyCompileError::UnsupportedFeatureExpression {
                            rule: self.rule_name.to_string(),
                            model: model_name.to_string(),
                            expr: format!(
                                "{}(...) requires 3 or 4 arguments (subject, 'REL_TYPE', 'property' [, 'OUTGOING' | 'INCOMING' | 'BOTH']), got {}",
                                name,
                                fargs.len()
                            ),
                        });
                    }
                    match &fargs[0] {
                        Expr::Variable(_) => {}
                        other => {
                            return Err(LocyCompileError::UnsupportedFeatureExpression {
                                rule: self.rule_name.to_string(),
                                model: model_name.to_string(),
                                expr: format!(
                                    "{}(...) first argument must be a node variable, got {other:?}",
                                    name
                                ),
                            });
                        }
                    }
                    for (i, inner) in fargs.iter().enumerate().skip(1) {
                        let is_string_literal = matches!(
                            inner,
                            Expr::Literal(uni_cypher::ast::CypherLiteral::String(_))
                        );
                        if !is_string_literal {
                            return Err(LocyCompileError::UnsupportedFeatureExpression {
                                rule: self.rule_name.to_string(),
                                model: model_name.to_string(),
                                expr: format!(
                                    "{}(...) argument {} must be a string literal, got {inner:?}",
                                    name,
                                    i + 1
                                ),
                            });
                        }
                    }
                    // If a direction was supplied, validate its value.
                    if let Some(Expr::Literal(uni_cypher::ast::CypherLiteral::String(dir))) =
                        fargs.get(3)
                    {
                        let upper = dir.to_uppercase();
                        if !matches!(upper.as_str(), "OUTGOING" | "INCOMING" | "BOTH") {
                            return Err(LocyCompileError::UnsupportedFeatureExpression {
                                rule: self.rule_name.to_string(),
                                model: model_name.to_string(),
                                expr: format!(
                                    "{}(...) 4th argument (direction) must be 'OUTGOING', 'INCOMING', or 'BOTH'; got '{dir}'",
                                    name
                                ),
                            });
                        }
                    }
                    rewritten.push(fexpr.clone());
                }
                Expr::FunctionCall {
                    name, args: fargs, ..
                } if matches!(
                    name.as_str(),
                    "degree_centrality"
                        | "pagerank_score"
                        | "closeness_centrality"
                        | "betweenness_centrality"
                        | "eigenvector_centrality"
                        | "harmonic_centrality"
                        | "katz_centrality"
                ) =>
                {
                    // Phase D D1 graph-structural: single-arg topology
                    // features. Arg must be `Expr::Variable(_)` — the
                    // subject binding whose `_vid` is materialized into
                    // the per-row fact_row via the hidden YIELD pipeline.
                    if fargs.len() != 1 {
                        return Err(LocyCompileError::UnsupportedFeatureExpression {
                            rule: self.rule_name.to_string(),
                            model: model_name.to_string(),
                            expr: format!(
                                "{}(...) requires exactly 1 argument, got {}",
                                name,
                                fargs.len()
                            ),
                        });
                    }
                    match &fargs[0] {
                        Expr::Variable(_) => {}
                        other => {
                            return Err(LocyCompileError::UnsupportedFeatureExpression {
                                rule: self.rule_name.to_string(),
                                model: model_name.to_string(),
                                expr: format!(
                                    "{}(...) argument must be a node variable, got {other:?}",
                                    name
                                ),
                            });
                        }
                    }
                    rewritten.push(fexpr.clone());
                }
                Expr::FunctionCall {
                    name, args: fargs, ..
                } if matches!(name.as_str(), "similar_to" | "semantic_match") => {
                    // Phase D D1/D2: retrieval-backed feature expressions.
                    // Both functions take exactly 2 args. Property-access
                    // args register the (var, prop) pair so the standard
                    // hidden-YIELD pipeline materializes them into the
                    // per-row fact_row; the FunctionCall itself flows
                    // through `original_feature_exprs` and is evaluated
                    // per-row in `apply_model_invocations` /
                    // `eval_feature_expr_against_fact_row`.
                    if fargs.len() != 2 {
                        return Err(LocyCompileError::UnsupportedFeatureExpression {
                            rule: self.rule_name.to_string(),
                            model: model_name.to_string(),
                            expr: format!(
                                "{}(...) requires exactly 2 arguments, got {}",
                                name,
                                fargs.len()
                            ),
                        });
                    }
                    for inner in fargs {
                        match inner {
                            Expr::Variable(_) | Expr::Literal(_) | Expr::List(_) => {}
                            Expr::Property(boxed_inner, prop)
                                if matches!(boxed_inner.as_ref(), Expr::Variable(_)) =>
                            {
                                if let Expr::Variable(v) = boxed_inner.as_ref() {
                                    feature_property_refs.push((v.clone(), prop.clone()));
                                    let col_name = format!("__feat_{}_{}", v, prop);
                                    if !self.seen_hidden.contains(&col_name) {
                                        self.seen_hidden.insert(col_name.clone());
                                        let hidden_expr = Expr::Property(
                                            Box::new(Expr::Variable(v.clone())),
                                            prop.clone(),
                                        );
                                        self.hidden_items.push((col_name, hidden_expr));
                                    }
                                }
                            }
                            other => {
                                return Err(LocyCompileError::UnsupportedFeatureExpression {
                                    rule: self.rule_name.to_string(),
                                    model: model_name.to_string(),
                                    expr: format!(
                                        "{}(...) argument must be Variable, Property, or Literal — got {other:?}",
                                        name
                                    ),
                                });
                            }
                        }
                    }
                    rewritten.push(fexpr.clone());
                }
                other => {
                    return Err(LocyCompileError::UnsupportedFeatureExpression {
                        rule: self.rule_name.to_string(),
                        model: model_name.to_string(),
                        expr: format!("{other:?}"),
                    });
                }
            }
        }
        Ok((rewritten, feature_property_refs))
    }

    /// Lift any model FunctionCall in `expr` (recursively) into the
    /// invocations accumulator, returning an Expr with each call site
    /// replaced by `Variable("__model_<name>_<idx>")`.
    fn lift_expr(&mut self, expr: &Expr) -> Result<Expr, LocyCompileError> {
        match expr {
            Expr::FunctionCall {
                name,
                args,
                distinct,
                window_spec,
            } if self.model_catalog.contains_key(name) => {
                let model = &self.model_catalog[name];
                if args.len() != model.inputs.len() {
                    // Arity already validated by check_model_invocations;
                    // pass through unchanged so the existing error path
                    // surfaces it.
                    return Ok(expr.clone());
                }
                let synthetic = format!("__model_{}_{}", name, self.counter);
                self.counter += 1;
                // Phase C B1-B3 follow-up: capture the pre-rewrite
                // feature args BEFORE validate_features mutates
                // them. EXPLAIN uses these to rebuild ClassifyInput
                // per fact (the rewritten copy carries synthetic
                // column references that don't evaluate against a
                // post-projection fact_row).
                let original_feature_exprs = args.clone();
                let (rewritten_feature_exprs, feature_property_refs) =
                    self.validate_features(name, args)?;
                let feature_names: Vec<String> =
                    model.inputs.iter().map(|b| b.variable.clone()).collect();
                self.invocations.push(ModelInvocation {
                    model_name: name.clone(),
                    output_column: synthetic.clone(),
                    feature_exprs: rewritten_feature_exprs,
                    feature_names,
                    feature_property_refs,
                    // Filled in by the YIELD-item walk in
                    // extract_model_invocations after lift_expr
                    // returns — the caller knows whether the
                    // invocation came from a YIELD position
                    // (alias-bearing) or ALONG/FOLD (no alias).
                    yield_alias: None,
                    original_feature_exprs,
                    path_context: model.path_context.clone(),
                    embedder_alias: model.embedder_alias.clone(),
                });
                let _ = (distinct, window_spec);
                Ok(Expr::Variable(synthetic))
            }
            Expr::FunctionCall {
                name,
                args,
                distinct,
                window_spec,
            } => {
                let new_args = args
                    .iter()
                    .map(|a| self.lift_expr(a))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Expr::FunctionCall {
                    name: name.clone(),
                    args: new_args,
                    distinct: *distinct,
                    window_spec: window_spec.clone(),
                })
            }
            Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
                left: Box::new(self.lift_expr(left)?),
                op: *op,
                right: Box::new(self.lift_expr(right)?),
            }),
            Expr::UnaryOp { op, expr: inner } => Ok(Expr::UnaryOp {
                op: *op,
                expr: Box::new(self.lift_expr(inner)?),
            }),
            Expr::List(items) => Ok(Expr::List(
                items
                    .iter()
                    .map(|e| self.lift_expr(e))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            Expr::Map(entries) => Ok(Expr::Map(
                entries
                    .iter()
                    .map(|(k, v)| self.lift_expr(v).map(|nv| (k.clone(), nv)))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            // Leaf or non-recursive shapes — no model call can hide.
            _ => Ok(expr.clone()),
        }
    }

    fn lift_locy_expr(&mut self, expr: &LocyExpr) -> Result<LocyExpr, LocyCompileError> {
        match expr {
            LocyExpr::Cypher(e) => Ok(LocyExpr::Cypher(self.lift_expr(e)?)),
            LocyExpr::BinaryOp { left, op, right } => Ok(LocyExpr::BinaryOp {
                left: Box::new(self.lift_locy_expr(left)?),
                op: *op,
                right: Box::new(self.lift_locy_expr(right)?),
            }),
            LocyExpr::UnaryOp(op, inner) => Ok(LocyExpr::UnaryOp(
                *op,
                Box::new(self.lift_locy_expr(inner)?),
            )),
            LocyExpr::PrevRef(_) => Ok(expr.clone()),
        }
    }
}

fn extract_model_invocations(
    rule_name: &str,
    def: &RuleDefinition,
    model_catalog: &HashMap<String, CompiledModel>,
) -> Result<ExtractedInvocations, LocyCompileError> {
    let mut lifter = InvocationLifter::new(rule_name, model_catalog);

    // ── YIELD position ──────────────────────────────────────────
    let new_output = match &def.output {
        RuleOutput::Yield(yc) => {
            let mut new_items = Vec::with_capacity(yc.items.len());
            for item in &yc.items {
                let before = lifter.invocations.len();
                let new_expr = lifter.lift_expr(&item.expr)?;
                // Tag every invocation lifted from THIS YIELD item
                // with the item's user-visible alias so EXPLAIN can
                // look up the model output by the column name that
                // survives `LocyProject`'s projection.
                let yield_alias = item.alias.clone().or_else(|| match &new_expr {
                    Expr::Variable(n) => Some(n.clone()),
                    _ => None,
                });
                for inv in lifter.invocations[before..].iter_mut() {
                    inv.yield_alias = yield_alias.clone();
                }
                new_items.push(LocyYieldItem {
                    is_key: item.is_key,
                    is_prob: item.is_prob,
                    expr: new_expr,
                    alias: item.alias.clone(),
                });
            }
            RuleOutput::Yield(uni_cypher::locy_ast::YieldClause { items: new_items })
        }
        other => other.clone(),
    };

    // ── ALONG position ──────────────────────────────────────────
    let mut new_along = Vec::with_capacity(def.along.len());
    for binding in &def.along {
        new_along.push(AlongBinding {
            name: binding.name.clone(),
            expr: lifter.lift_locy_expr(&binding.expr)?,
        });
    }

    // ── FOLD position ───────────────────────────────────────────
    let mut new_fold = Vec::with_capacity(def.fold.len());
    for binding in &def.fold {
        new_fold.push(FoldBinding {
            name: binding.name.clone(),
            aggregate: lifter.lift_expr(&binding.aggregate)?,
        });
    }

    // ── Hidden YIELD items for property-feature refs ─────────────
    let mut hidden_yield_cols: Vec<String> = Vec::with_capacity(lifter.hidden_items.len());
    let new_output = match new_output {
        RuleOutput::Yield(mut yc) => {
            for (col_name, hidden_expr) in &lifter.hidden_items {
                yc.items.push(LocyYieldItem {
                    is_key: false,
                    is_prob: false,
                    expr: hidden_expr.clone(),
                    alias: Some(col_name.clone()),
                });
                hidden_yield_cols.push(col_name.clone());
            }
            RuleOutput::Yield(yc)
        }
        other => other,
    };

    Ok(ExtractedInvocations {
        output: new_output,
        along: new_along,
        fold: new_fold,
        invocations: lifter.invocations,
        hidden_yield_cols,
    })
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn extract_function_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::FunctionCall { name, .. } => Some(name.clone()),
        _ => None,
    }
}
