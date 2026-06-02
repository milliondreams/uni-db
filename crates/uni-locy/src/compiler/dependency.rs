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

/// Extract IS references from all rule definitions and build a dependency
/// graph, also recognizing external (registered) rule names. Returns
/// `UndefinedRule` if any IS reference targets a rule not in `rule_groups`
/// or `external_rules`.
///
/// Additionally injects positive edges for Phase D D3 path-context model
/// invocations: any rule that invokes a model whose
/// `CompiledModel.path_context.source_rule` is `R` gains a positive edge
/// to `R`, ensuring the stratifier places the invoking rule strictly
/// after `R`.
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
            let mut walker = PathContextWalker {
                model_catalog,
                module_ctx,
                all_rules: &all_rules,
                rule_name,
                positive_edges: &mut positive_edges,
            };
            walker.collect_deps(def)?;
        }
    }

    Ok(DependencyGraph {
        positive_edges,
        negative_edges,
        all_rules,
    })
}

/// Bundles the invariant context threaded through the path-context
/// expression walk so the recursive helpers don't repeat six arguments.
struct PathContextWalker<'a> {
    model_catalog: &'a HashMap<String, CompiledModel>,
    module_ctx: &'a ModuleContext,
    all_rules: &'a HashSet<String>,
    rule_name: &'a str,
    positive_edges: &'a mut HashMap<String, HashSet<String>>,
}

impl PathContextWalker<'_> {
    fn collect_deps(&mut self, def: &RuleDefinition) -> Result<(), LocyCompileError> {
        if self.model_catalog.is_empty() {
            return Ok(());
        }
        if let RuleOutput::Yield(yc) = &def.output {
            for item in &yc.items {
                self.walk_expr(&item.expr)?;
            }
        }
        for al in &def.along {
            self.walk_locy_expr(&al.expr)?;
        }
        for fold in &def.fold {
            self.walk_expr(&fold.aggregate)?;
        }
        for h in &def.having {
            self.walk_expr(h)?;
        }
        Ok(())
    }

    fn walk_expr(&mut self, expr: &Expr) -> Result<(), LocyCompileError> {
        match expr {
            Expr::FunctionCall { name, args, .. } => {
                if let Some(model) = self.model_catalog.get(name)
                    && let Some(pc) = &model.path_context
                {
                    let target = modules::resolve_rule_name(self.module_ctx, &pc.source_rule);
                    if !self.all_rules.contains(&target) {
                        return Err(LocyCompileError::UndefinedRule { name: target });
                    }
                    self.positive_edges
                        .entry(self.rule_name.to_string())
                        .or_default()
                        .insert(target);
                }
                for a in args {
                    self.walk_expr(a)?;
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                self.walk_expr(left)?;
                self.walk_expr(right)?;
            }
            Expr::UnaryOp { expr, .. } => self.walk_expr(expr)?,
            _ => {}
        }
        Ok(())
    }

    fn walk_locy_expr(&mut self, expr: &LocyExpr) -> Result<(), LocyCompileError> {
        match expr {
            LocyExpr::Cypher(e) => self.walk_expr(e),
            _ => Ok(()),
        }
    }
}
