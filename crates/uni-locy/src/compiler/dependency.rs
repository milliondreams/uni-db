use std::collections::{HashMap, HashSet};

use uni_cypher::ast::Expr;
use uni_cypher::locy_ast::{LocyExpr, RuleCondition, RuleDefinition, RuleOutput};

use super::errors::LocyCompileError;
use super::modules::{self, ModuleContext};
use crate::types::CompiledModel;

/// Directed graph of inter-rule dependencies, split into positive and negative edges.
pub struct DependencyGraph {
    /// rule → set of rules it depends on positively (IS)
    pub positive_edges: HashMap<String, HashSet<String>>,
    /// rule → set of rules it depends on negatively (IS NOT)
    pub negative_edges: HashMap<String, HashSet<String>>,
    /// All known rule names
    pub all_rules: HashSet<String>,
}

/// Extract IS references from all rule definitions and build a dependency graph.
/// Returns `UndefinedRule` if any IS reference targets a rule not in `rule_groups`
/// or `external_rules`.
pub fn build_dependency_graph(
    rule_groups: &HashMap<String, Vec<&RuleDefinition>>,
    module_ctx: &ModuleContext,
) -> Result<DependencyGraph, LocyCompileError> {
    build_dependency_graph_with_external(rule_groups, module_ctx, &[])
}

/// Build a dependency graph that also recognizes external (registered) rule names.
pub fn build_dependency_graph_with_external(
    rule_groups: &HashMap<String, Vec<&RuleDefinition>>,
    module_ctx: &ModuleContext,
    external_rules: &[String],
) -> Result<DependencyGraph, LocyCompileError> {
    build_dependency_graph_with_models(rule_groups, module_ctx, external_rules, &HashMap::new())
}

/// Like `build_dependency_graph_with_external`, but also injects positive
/// edges for Phase D D3 path-context model invocations: any rule that
/// invokes a model whose `CompiledModel.path_context.source_rule` is `R`
/// gains a positive edge to `R`, ensuring the stratifier places the
/// invoking rule strictly after `R`.
pub fn build_dependency_graph_with_models(
    rule_groups: &HashMap<String, Vec<&RuleDefinition>>,
    module_ctx: &ModuleContext,
    external_rules: &[String],
    model_catalog: &HashMap<String, CompiledModel>,
) -> Result<DependencyGraph, LocyCompileError> {
    let mut all_rules: HashSet<String> = rule_groups.keys().cloned().collect();
    all_rules.extend(external_rules.iter().cloned());
    let mut positive_edges: HashMap<String, HashSet<String>> = HashMap::new();
    let mut negative_edges: HashMap<String, HashSet<String>> = HashMap::new();

    for (rule_name, definitions) in rule_groups {
        for def in definitions {
            for cond in &def.where_conditions {
                if let RuleCondition::IsReference(is_ref) = cond {
                    let raw_target = is_ref.rule_name.to_string();
                    let target = modules::resolve_rule_name(module_ctx, &raw_target);

                    if !all_rules.contains(&target) {
                        return Err(LocyCompileError::UndefinedRule { name: target });
                    }

                    if is_ref.negated {
                        negative_edges
                            .entry(rule_name.clone())
                            .or_default()
                            .insert(target);
                    } else {
                        positive_edges
                            .entry(rule_name.clone())
                            .or_default()
                            .insert(target);
                    }
                }
            }
        }
    }

    // Phase D D3: walk each rule's YIELD / ALONG / FOLD / HAVING /
    // WHERE expressions for model `FunctionCall`s. When the invoked
    // model carries a `path_context.source_rule`, add a positive
    // edge `rule → source_rule`.
    for (rule_name, definitions) in rule_groups {
        for def in definitions {
            collect_path_context_deps(
                def,
                model_catalog,
                module_ctx,
                &all_rules,
                rule_name,
                &mut positive_edges,
            )?;
        }
    }

    Ok(DependencyGraph {
        positive_edges,
        negative_edges,
        all_rules,
    })
}

fn collect_path_context_deps(
    def: &RuleDefinition,
    model_catalog: &HashMap<String, CompiledModel>,
    module_ctx: &ModuleContext,
    all_rules: &HashSet<String>,
    rule_name: &str,
    positive_edges: &mut HashMap<String, HashSet<String>>,
) -> Result<(), LocyCompileError> {
    if model_catalog.is_empty() {
        return Ok(());
    }
    if let RuleOutput::Yield(yc) = &def.output {
        for item in &yc.items {
            walk_for_path_context(
                &item.expr,
                model_catalog,
                module_ctx,
                all_rules,
                rule_name,
                positive_edges,
            )?;
        }
    }
    for al in &def.along {
        walk_locy_for_path_context(
            &al.expr,
            model_catalog,
            module_ctx,
            all_rules,
            rule_name,
            positive_edges,
        )?;
    }
    for fold in &def.fold {
        walk_for_path_context(
            &fold.aggregate,
            model_catalog,
            module_ctx,
            all_rules,
            rule_name,
            positive_edges,
        )?;
    }
    for h in &def.having {
        walk_for_path_context(
            h,
            model_catalog,
            module_ctx,
            all_rules,
            rule_name,
            positive_edges,
        )?;
    }
    Ok(())
}

fn walk_for_path_context(
    expr: &Expr,
    model_catalog: &HashMap<String, CompiledModel>,
    module_ctx: &ModuleContext,
    all_rules: &HashSet<String>,
    rule_name: &str,
    positive_edges: &mut HashMap<String, HashSet<String>>,
) -> Result<(), LocyCompileError> {
    match expr {
        Expr::FunctionCall { name, args, .. } => {
            if let Some(model) = model_catalog.get(name)
                && let Some(pc) = &model.path_context
            {
                let target = modules::resolve_rule_name(module_ctx, &pc.source_rule);
                if !all_rules.contains(&target) {
                    return Err(LocyCompileError::UndefinedRule { name: target });
                }
                positive_edges
                    .entry(rule_name.to_string())
                    .or_default()
                    .insert(target);
            }
            for a in args {
                walk_for_path_context(
                    a,
                    model_catalog,
                    module_ctx,
                    all_rules,
                    rule_name,
                    positive_edges,
                )?;
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            walk_for_path_context(
                left,
                model_catalog,
                module_ctx,
                all_rules,
                rule_name,
                positive_edges,
            )?;
            walk_for_path_context(
                right,
                model_catalog,
                module_ctx,
                all_rules,
                rule_name,
                positive_edges,
            )?;
        }
        Expr::UnaryOp { expr, .. } => walk_for_path_context(
            expr,
            model_catalog,
            module_ctx,
            all_rules,
            rule_name,
            positive_edges,
        )?,
        _ => {}
    }
    Ok(())
}

fn walk_locy_for_path_context(
    expr: &LocyExpr,
    model_catalog: &HashMap<String, CompiledModel>,
    module_ctx: &ModuleContext,
    all_rules: &HashSet<String>,
    rule_name: &str,
    positive_edges: &mut HashMap<String, HashSet<String>>,
) -> Result<(), LocyCompileError> {
    match expr {
        LocyExpr::Cypher(e) => walk_for_path_context(
            e,
            model_catalog,
            module_ctx,
            all_rules,
            rule_name,
            positive_edges,
        ),
        _ => Ok(()),
    }
}
