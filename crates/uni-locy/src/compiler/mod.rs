pub mod dependency;
pub mod errors;
pub mod stratify;
pub mod typecheck;
pub mod warded;

pub mod modules;

use std::collections::HashMap;

use uni_cypher::locy_ast::{LocyProgram, LocyStatement, RuleDefinition};

use crate::types::{CompiledAssume, CompiledCommand, CompiledProgram, Stratum};
use errors::LocyCompileError;

/// Validate and stratify a parsed Locy program into a `CompiledProgram`.
///
/// Pipeline: group_rules → dependency graph → stratify → wardedness → typecheck → assemble.
pub fn compile(program: &LocyProgram) -> Result<CompiledProgram, LocyCompileError> {
    compile_with_modules(program, &HashMap::new())
}

/// Compile a Locy program with module resolution context.
///
/// `available_modules` maps module names to their exported rule names,
/// enabling MODULE/USE declarations to resolve rule references across modules.
/// When empty (e.g., from `compile()`), module resolution is a no-op.
pub fn compile_with_modules(
    program: &LocyProgram,
    available_modules: &HashMap<String, Vec<String>>,
) -> Result<CompiledProgram, LocyCompileError> {
    let module_ctx = modules::resolve_modules(program, available_modules)?;
    let rule_groups = group_rules_with_context(program, &module_ctx);
    let rule_names: Vec<String> = rule_groups.keys().cloned().collect();

    if rule_groups.is_empty() {
        let commands = extract_commands(program, &[], &module_ctx)?;
        return Ok(CompiledProgram {
            strata: Vec::new(),
            rule_catalog: HashMap::new(),
            warnings: Vec::new(),
            commands,
        });
    }

    let dep_graph = dependency::build_dependency_graph(&rule_groups, &module_ctx)?;
    let strat = stratify::stratify(&dep_graph)?;
    warded::check_wardedness(&rule_groups)?;
    let (compiled_rules, warnings) = typecheck::check(&rule_groups, &strat)?;

    // Assemble strata in topological order
    let mut strata = Vec::new();
    for &scc_idx in &strat.scc_order {
        let scc_rules: Vec<_> = strat.sccs[scc_idx]
            .iter()
            .filter_map(|name| compiled_rules.get(name).cloned())
            .collect();

        if scc_rules.is_empty() {
            continue;
        }

        let depends_on: Vec<usize> = strat.scc_depends_on[scc_idx].iter().copied().collect();

        strata.push(Stratum {
            id: scc_idx,
            rules: scc_rules,
            is_recursive: strat.is_recursive[scc_idx],
            depends_on,
        });
    }

    let commands = extract_commands(program, &rule_names, &module_ctx)?;

    Ok(CompiledProgram {
        strata,
        rule_catalog: compiled_rules,
        warnings,
        commands,
    })
}

/// Extract non-rule statements as compiled commands, validating rule references.
fn extract_commands(
    program: &LocyProgram,
    defined_rules: &[String],
    module_ctx: &modules::ModuleContext,
) -> Result<Vec<CompiledCommand>, LocyCompileError> {
    let validate_rule = |raw_name: &str| -> Result<(), LocyCompileError> {
        let resolved = modules::resolve_rule_name(module_ctx, raw_name);
        if !defined_rules.contains(&resolved) {
            return Err(LocyCompileError::UndefinedRule { name: resolved });
        }
        Ok(())
    };

    let mut commands = Vec::new();
    for stmt in &program.statements {
        match stmt {
            LocyStatement::Rule(_) => {} // handled by group_rules
            LocyStatement::GoalQuery(gq) => {
                validate_rule(&gq.rule_name.to_string())?;
                commands.push(CompiledCommand::GoalQuery(gq.clone()));
            }
            LocyStatement::ExplainRule(eq) => {
                validate_rule(&eq.rule_name.to_string())?;
                commands.push(CompiledCommand::ExplainRule(eq.clone()));
            }
            LocyStatement::AbduceQuery(aq) => {
                validate_rule(&aq.rule_name.to_string())?;
                commands.push(CompiledCommand::Abduce(aq.clone()));
            }
            LocyStatement::DeriveCommand(dc) => {
                validate_rule(&dc.rule_name.to_string())?;
                commands.push(CompiledCommand::DeriveCommand(dc.clone()));
            }
            LocyStatement::AssumeBlock(ab) => {
                // Compile the ASSUME body as a sub-program
                let body_program_ast = uni_cypher::locy_ast::LocyProgram {
                    module: None,
                    uses: vec![],
                    statements: ab.body.clone(),
                };
                let body_module_ctx = modules::ModuleContext::default();
                let body_rule_groups = group_rules(&body_program_ast);
                let all_rule_names: Vec<String> = defined_rules
                    .iter()
                    .chain(body_rule_groups.keys())
                    .cloned()
                    .collect();
                let body_commands =
                    extract_commands(&body_program_ast, &all_rule_names, &body_module_ctx)?;

                // Compile body rules if any exist
                let body_compiled = if !body_rule_groups.is_empty() {
                    compile(&body_program_ast)?
                } else {
                    CompiledProgram {
                        strata: Vec::new(),
                        rule_catalog: HashMap::new(),
                        warnings: Vec::new(),
                        commands: Vec::new(),
                    }
                };

                commands.push(CompiledCommand::Assume(CompiledAssume {
                    mutations: ab.mutations.clone(),
                    body_program: body_compiled,
                    body_commands,
                }));
            }
            LocyStatement::Cypher(query) => {
                commands.push(CompiledCommand::Cypher(query.clone()));
            }
        }
    }
    Ok(commands)
}

/// Group `CREATE RULE` statements by rule name (qualified-name string form).
fn group_rules(program: &LocyProgram) -> HashMap<String, Vec<&RuleDefinition>> {
    let mut groups: HashMap<String, Vec<&RuleDefinition>> = HashMap::new();
    for stmt in &program.statements {
        if let LocyStatement::Rule(rule_def) = stmt {
            let name = rule_def.name.to_string();
            groups.entry(name).or_default().push(rule_def);
        }
    }
    groups
}

/// Group `CREATE RULE` statements with module-qualified names.
fn group_rules_with_context<'a>(
    program: &'a LocyProgram,
    module_ctx: &modules::ModuleContext,
) -> HashMap<String, Vec<&'a RuleDefinition>> {
    let mut groups: HashMap<String, Vec<&RuleDefinition>> = HashMap::new();
    for stmt in &program.statements {
        if let LocyStatement::Rule(rule_def) = stmt {
            let raw_name = rule_def.name.to_string();
            let name = modules::resolve_rule_name(module_ctx, &raw_name);
            groups.entry(name).or_default().push(rule_def);
        }
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{WarningCode, YieldColumn};
    use uni_cypher::parse_locy;

    // ── Step 1: Single non-recursive rule → 1 stratum ───────────────────

    #[test]
    fn step1_single_non_recursive_rule() {
        let prog =
            parse_locy("CREATE RULE reachable AS MATCH (a)-[:KNOWS]->(b) YIELD a, b").unwrap();
        let compiled = compile(&prog).unwrap();

        assert_eq!(compiled.strata.len(), 1);
        assert!(!compiled.strata[0].is_recursive);
        assert_eq!(compiled.strata[0].rules.len(), 1);
        assert_eq!(compiled.strata[0].rules[0].name, "reachable");
        assert_eq!(compiled.strata[0].rules[0].clauses.len(), 1);
        assert_eq!(
            compiled.strata[0].rules[0].yield_schema,
            vec![
                YieldColumn {
                    name: "a".into(),
                    is_key: false
                },
                YieldColumn {
                    name: "b".into(),
                    is_key: false
                },
            ]
        );
    }

    // ── Step 2: A depends on B via IS → 2 strata, correct order ─────────

    #[test]
    fn step2_two_strata_dependency_order() {
        let prog = parse_locy(
            "CREATE RULE base AS MATCH (a)-[:KNOWS]->(b) YIELD a, b \
             CREATE RULE derived AS MATCH (x)-[:FOLLOWS]->(y) WHERE x IS base TO y YIELD x, y",
        )
        .unwrap();
        let compiled = compile(&prog).unwrap();

        assert_eq!(compiled.strata.len(), 2);

        let base_pos = compiled
            .strata
            .iter()
            .position(|s| s.rules.iter().any(|r| r.name == "base"))
            .expect("base stratum not found");
        let derived_pos = compiled
            .strata
            .iter()
            .position(|s| s.rules.iter().any(|r| r.name == "derived"))
            .expect("derived stratum not found");

        assert!(
            base_pos < derived_pos,
            "base stratum must precede derived in evaluation order"
        );

        let base_id = compiled.strata[base_pos].id;
        let derived_stratum = &compiled.strata[derived_pos];
        assert!(derived_stratum.depends_on.contains(&base_id));
    }

    // ── Step 3: Recursive rule (A IS A) → 1 recursive stratum ───────────

    #[test]
    fn step3_recursive_rule_single_stratum() {
        let prog = parse_locy(
            "CREATE RULE reachable AS MATCH (a)-[:KNOWS]->(b) YIELD a, b \
             CREATE RULE reachable AS MATCH (a)-[:KNOWS]->(mid) \
             WHERE mid IS reachable TO b YIELD a, b",
        )
        .unwrap();
        let compiled = compile(&prog).unwrap();

        assert_eq!(compiled.strata.len(), 1);
        assert!(compiled.strata[0].is_recursive);
        assert_eq!(compiled.strata[0].rules.len(), 1);
        assert_eq!(compiled.strata[0].rules[0].name, "reachable");
        assert_eq!(compiled.strata[0].rules[0].clauses.len(), 2);
    }

    // ── Step 4: Cyclic negation → CYCLIC_NEGATION error ─────────────────

    #[test]
    fn step4_cyclic_negation() {
        let prog = parse_locy(
            "CREATE RULE a AS MATCH (x)-[:R]->(y) WHERE x IS NOT b YIELD x, y \
             CREATE RULE b AS MATCH (x)-[:R]->(y) WHERE x IS NOT a YIELD x, y",
        )
        .unwrap();

        let result = compile(&prog);
        assert!(result.is_err());
        match result.unwrap_err() {
            LocyCompileError::CyclicNegation { rules } => {
                assert!(rules.contains(&"a".to_string()));
                assert!(rules.contains(&"b".to_string()));
            }
            e => panic!("expected CyclicNegation, got {e:?}"),
        }
    }

    // ── Step 5: prev in non-recursive → PREV_IN_BASE_CASE error ─────────

    #[test]
    fn step5_prev_in_non_recursive() {
        let prog = parse_locy(
            "CREATE RULE r AS MATCH (a)-[:E]->(b) ALONG cost = prev.cost + 1 YIELD a, b",
        )
        .unwrap();

        let result = compile(&prog);
        assert!(result.is_err());
        match result.unwrap_err() {
            LocyCompileError::PrevInBaseCase { rule, field } => {
                assert_eq!(rule, "r");
                assert_eq!(field, "cost");
            }
            e => panic!("expected PrevInBaseCase, got {e:?}"),
        }
    }

    // ── Step 6: SUM in recursive stratum → NON_MONOTONIC error ──────────

    #[test]
    fn step6_non_monotonic_in_recursion() {
        let prog = parse_locy(
            "CREATE RULE r AS MATCH (a)-[:E]->(b) YIELD a, b, 0 AS total \
             CREATE RULE r AS MATCH (a)-[:E]->(mid) WHERE mid IS r TO b \
             FOLD total = SUM(a.cost) YIELD a, b, total",
        )
        .unwrap();

        let result = compile(&prog);
        assert!(result.is_err());
        match result.unwrap_err() {
            LocyCompileError::NonMonotonicInRecursion { rule, aggregate } => {
                assert_eq!(rule, "r");
                assert_eq!(aggregate.to_uppercase(), "SUM");
            }
            e => panic!("expected NonMonotonicInRecursion, got {e:?}"),
        }
    }

    // ── Step 7: MSUM warning → MSUM_NON_NEGATIVITY warning ──────────────

    #[test]
    fn step7_msum_warning() {
        let prog = parse_locy(
            "CREATE RULE r AS MATCH (a)-[:E]->(b) YIELD a, b, 0 AS total \
             CREATE RULE r AS MATCH (a)-[:E]->(mid) WHERE mid IS r TO b \
             FOLD total = MSUM(a.weight) YIELD a, b, total",
        )
        .unwrap();

        let compiled = compile(&prog).unwrap();
        assert_eq!(compiled.warnings.len(), 1);
        assert_eq!(compiled.warnings[0].code, WarningCode::MsumNonNegativity);
    }

    // ── Step 8: BEST BY + MSUM → BEST_BY_WITH_MONOTONIC_FOLD error ──────

    #[test]
    fn step8_best_by_with_monotonic_fold() {
        let prog = parse_locy(
            "CREATE RULE r AS MATCH (a)-[:E]->(b) YIELD a, b, 0 AS total \
             CREATE RULE r AS MATCH (a)-[:E]->(mid) WHERE mid IS r TO b \
             FOLD total = MSUM(a.cost) BEST BY total ASC YIELD a, b, total",
        )
        .unwrap();

        let result = compile(&prog);
        assert!(result.is_err());
        match result.unwrap_err() {
            LocyCompileError::BestByWithMonotonicFold { rule, fold } => {
                assert_eq!(rule, "r");
                assert_eq!(fold.to_uppercase(), "MSUM");
            }
            e => panic!("expected BestByWithMonotonicFold, got {e:?}"),
        }
    }

    // ── Step 9: Undefined rule reference → UNDEFINED_RULE error ─────────

    #[test]
    fn step9_undefined_rule() {
        let prog =
            parse_locy("CREATE RULE r AS MATCH (x)-[:R]->(y) WHERE x IS nonexistent YIELD x, y")
                .unwrap();

        let result = compile(&prog);
        assert!(result.is_err());
        match result.unwrap_err() {
            LocyCompileError::UndefinedRule { name } => {
                assert_eq!(name, "nonexistent");
            }
            e => panic!("expected UndefinedRule, got {e:?}"),
        }
    }

    // ── Step 10: NEW without warded position → WARDEDNESS_VIOLATION ──────

    #[test]
    fn step10_wardedness_violation() {
        let prog = parse_locy(
            "CREATE RULE base AS MATCH (a)-[:R]->(b) YIELD a, b \
             CREATE RULE r AS MATCH (x)-[:R]->(y) WHERE y IS base TO z \
             DERIVE (NEW n:T)-[:LINK]->(z)",
        )
        .unwrap();

        let result = compile(&prog);
        assert!(result.is_err());
        match result.unwrap_err() {
            LocyCompileError::WardednessViolation { rule, variable } => {
                assert_eq!(rule, "r");
                assert_eq!(variable, "z");
            }
            e => panic!("expected WardednessViolation, got {e:?}"),
        }
    }

    // ── Step 11: YIELD schema inference → correct columns ────────────────

    #[test]
    fn step11_yield_schema_inference() {
        let prog = parse_locy(
            "CREATE RULE r AS MATCH (a)-[:E]->(b) YIELD KEY a, KEY b, a.cost AS total_cost",
        )
        .unwrap();
        let compiled = compile(&prog).unwrap();

        let rule = &compiled.rule_catalog["r"];
        assert_eq!(
            rule.yield_schema,
            vec![
                YieldColumn {
                    name: "a".into(),
                    is_key: true
                },
                YieldColumn {
                    name: "b".into(),
                    is_key: true
                },
                YieldColumn {
                    name: "total_cost".into(),
                    is_key: false
                },
            ]
        );
    }

    #[test]
    fn step11_yield_schema_mismatch() {
        let prog = parse_locy(
            "CREATE RULE r AS MATCH (a)-[:E]->(b) YIELD KEY a, KEY b, a.cost \
             CREATE RULE r AS MATCH (a)-[:E]->(c) YIELD KEY a, c.cost",
        )
        .unwrap();

        let result = compile(&prog);
        assert!(result.is_err());
        match result.unwrap_err() {
            LocyCompileError::YieldSchemaMismatch { rule, .. } => {
                assert_eq!(rule, "r");
            }
            e => panic!("expected YieldSchemaMismatch, got {e:?}"),
        }
    }

    // ── Step 12: Mixed priority → MIXED_PRIORITY error ───────────────────

    #[test]
    fn step12_mixed_priority() {
        let prog = parse_locy(
            "CREATE RULE r PRIORITY 1 AS MATCH (a)-[:E]->(b) YIELD a, b \
             CREATE RULE r AS MATCH (a)-[:E]->(c) YIELD a, c",
        )
        .unwrap();

        let result = compile(&prog);
        assert!(result.is_err());
        match result.unwrap_err() {
            LocyCompileError::MixedPriority { rule } => {
                assert_eq!(rule, "r");
            }
            e => panic!("expected MixedPriority, got {e:?}"),
        }
    }

    // ── Phase 4 Step 1: Commands extraction ───────────────────────────

    #[test]
    fn phase4_step1_query_command_extracted() {
        let prog = parse_locy(
            "CREATE RULE reachable AS MATCH (a)-[:KNOWS]->(b) YIELD a, b \
             QUERY reachable WHERE a = 'Alice' RETURN a, b",
        )
        .unwrap();
        let compiled = compile(&prog).unwrap();

        assert_eq!(compiled.commands.len(), 1);
        assert!(matches!(
            &compiled.commands[0],
            CompiledCommand::GoalQuery(_)
        ));
    }

    #[test]
    fn phase4_step1_undefined_rule_in_command() {
        let prog = parse_locy(
            "CREATE RULE reachable AS MATCH (a)-[:KNOWS]->(b) YIELD a, b \
             QUERY nonexistent WHERE a = 'Alice'",
        )
        .unwrap();
        let result = compile(&prog);
        assert!(result.is_err());
        match result.unwrap_err() {
            LocyCompileError::UndefinedRule { name } => {
                assert_eq!(name, "nonexistent");
            }
            e => panic!("expected UndefinedRule, got {e:?}"),
        }
    }

    #[test]
    fn phase4_step1_multiple_commands() {
        let prog = parse_locy(
            "CREATE RULE reachable AS MATCH (a)-[:KNOWS]->(b) YIELD a, b \
             QUERY reachable WHERE a = 'Alice' \
             EXPLAIN RULE reachable WHERE a = 'Bob'",
        )
        .unwrap();
        let compiled = compile(&prog).unwrap();

        assert_eq!(compiled.commands.len(), 2);
        assert!(matches!(
            &compiled.commands[0],
            CompiledCommand::GoalQuery(_)
        ));
        assert!(matches!(
            &compiled.commands[1],
            CompiledCommand::ExplainRule(_)
        ));
    }
}
