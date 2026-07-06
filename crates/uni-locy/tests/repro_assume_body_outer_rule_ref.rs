//! Repro for compiler/mod.rs:286 — ASSUME body RULES are compiled via the bare
//! `compile(&body_program_ast)` with `external_rules = &[]`, dropping the outer
//! program's rule names. So a body rule that IS-references an outer-program rule
//! fails with UndefinedRule, even though body COMMANDS (QUERY) validate that
//! same outer rule fine via the threaded `all_rule_names`.

use uni_locy::compile;
use uni_cypher::parse_locy;

#[test]
fn assume_body_rule_referencing_outer_rule_wrongly_undefined() {
    // Outer rule `adult` (yields 1 column). ASSUME body defines `eligible`
    // which IS-references the outer `adult`, plus a QUERY over `eligible`.
    let src = "CREATE RULE adult AS MATCH (p:Person) WHERE p.age >= 18 YIELD p \
        ASSUME { CREATE (x:Person) } THEN { \
          CREATE RULE eligible AS MATCH (p:Person) WHERE p IS adult YIELD p \
          QUERY eligible RETURN p \
        }";
    let prog = parse_locy(src).unwrap();
    let result = compile(&prog);

    // FIXED (mod.rs): the ASSUME body is compiled with the outer program's rule
    // names threaded in as external rules, so a body rule that IS-references the
    // outer `adult` resolves instead of failing with UndefinedRule.
    assert!(
        result.is_ok(),
        "ASSUME body must be able to reference outer rules, got {result:?}"
    );
}
