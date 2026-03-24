use std::collections::{HashMap, HashSet};

use uni_cypher::locy_ast::{RuleCondition, RuleDefinition};

use super::errors::LocyCompileError;
use super::modules::{self, ModuleContext};

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

    Ok(DependencyGraph {
        positive_edges,
        negative_edges,
        all_rules,
    })
}
