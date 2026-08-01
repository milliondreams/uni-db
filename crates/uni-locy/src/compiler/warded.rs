use std::collections::{HashMap, HashSet};

use uni_cypher::ast::{PathPattern, PatternElement};
use uni_cypher::locy_ast::{DeriveClause, RuleDefinition, RuleOutput};

use super::errors::LocyCompileError;

/// Check wardedness: for every DERIVE NEW node, its companion node(s) in the
/// same derive pattern must be bound by MATCH (not by IS references).
pub fn check_wardedness(
    rule_groups: &HashMap<String, Vec<&RuleDefinition>>,
) -> Result<(), LocyCompileError> {
    for (rule_name, definitions) in rule_groups {
        for def in definitions {
            if let RuleOutput::Derive(derive_clause) = &def.output {
                let match_vars = extract_match_variables(def);
                check_derive_warded(rule_name, derive_clause, &match_vars)?;
            }
        }
    }
    Ok(())
}

/// Extract all variables bound directly by the MATCH pattern.
fn extract_match_variables(def: &RuleDefinition) -> HashSet<String> {
    let mut vars = HashSet::new();
    for path in &def.match_pattern.paths {
        collect_path_vars(path, &mut vars);
    }
    vars
}

/// Collect every variable a path binds, descending into parenthesized
/// sub-patterns.
///
/// `Parenthesized` is the only `PatternElement` that carries a nested
/// `PathPattern`, and its arm here used to be empty. So a variable bound inside
/// parentheses was invisible to wardedness, and a rule that is perfectly warded
/// became a `WardednessViolation` purely because of how its pattern was
/// written — which matters because a quantified sub-pattern *has* to be
/// parenthesised. Every other consumer of the variant in the planner recurses;
/// this checker was the exception.
///
/// The arm stays explicit rather than collapsing into `_ => {}`: a future
/// `PatternElement` that binds variables should fail to compile here rather
/// than silently reintroduce the same false positive.
fn collect_path_vars(path: &PathPattern, vars: &mut HashSet<String>) {
    if let Some(v) = &path.variable {
        vars.insert(v.clone());
    }
    for elem in &path.elements {
        match elem {
            PatternElement::Node(n) => {
                if let Some(v) = &n.variable {
                    vars.insert(v.clone());
                }
            }
            PatternElement::Relationship(r) => {
                if let Some(v) = &r.variable {
                    vars.insert(v.clone());
                }
            }
            PatternElement::Parenthesized { pattern, .. } => {
                collect_path_vars(pattern, vars);
            }
        }
    }
}

/// For each derive pattern with a NEW node, the other node must be match-bound.
fn check_derive_warded(
    rule_name: &str,
    derive: &DeriveClause,
    match_vars: &HashSet<String>,
) -> Result<(), LocyCompileError> {
    match derive {
        DeriveClause::Patterns(patterns) => {
            for pat in patterns {
                let (source, target) = (&pat.source, &pat.target);

                // If source is NEW, target must be match-bound
                if source.is_new && !match_vars.contains(&target.variable) {
                    return Err(LocyCompileError::WardednessViolation {
                        rule: rule_name.to_string(),
                        variable: target.variable.clone(),
                    });
                }

                // If target is NEW, source must be match-bound
                if target.is_new && !match_vars.contains(&source.variable) {
                    return Err(LocyCompileError::WardednessViolation {
                        rule: rule_name.to_string(),
                        variable: source.variable.clone(),
                    });
                }
            }
        }
        DeriveClause::Merge(_, _) => {}
    }
    Ok(())
}
