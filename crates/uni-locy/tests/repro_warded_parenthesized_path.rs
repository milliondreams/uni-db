//! Repro for `compiler/warded.rs` — `PatternElement::Parenthesized { .. } => {}`.
//!
//! `extract_match_variables` walks a rule's MATCH pattern collecting every
//! variable the pattern binds, and `check_derive_warded` then requires the
//! companion of a `DERIVE (NEW …)` node to appear in that set. The walk handled
//! `Node` and `Relationship` and left the `Parenthesized` arm empty — no
//! recursion, no comment.
//!
//! `Parenthesized` is the only `PatternElement` that carries a nested path
//! (`ast.rs`: `pattern: Box<PathPattern>`), so every variable bound inside
//! parentheses was invisible to wardedness. Writing the *same* pattern with
//! parentheses — which is how a quantifier like `(…){1,3}` must be written —
//! turned a legal rule into `WardednessViolation`.
//!
//! Every other consumer of `Parenthesized` in the planner recurses into the
//! nested path; this checker was the one that did not.

use uni_cypher::parse_locy;
use uni_locy::compile;
use uni_locy::compiler::errors::LocyCompileError;

/// The baseline: without parentheses the rule is warded and compiles.
#[test]
fn unparenthesized_companion_is_warded() {
    let src = "CREATE RULE r AS MATCH (a:Person)-[:KNOWS]->(b:Person) \
               DERIVE (NEW n:Tag)-[:LINK]->(b)";
    let prog = parse_locy(src).unwrap();
    assert!(
        compile(&prog).is_ok(),
        "plain path binds `b`, so the rule is warded"
    );
}

/// The same binding, written inside parentheses, must also be warded.
#[test]
fn parenthesized_companion_is_still_warded() {
    let src = "CREATE RULE r AS MATCH ((a:Person)-[:KNOWS]->(b:Person)) \
               DERIVE (NEW n:Tag)-[:LINK]->(b)";
    let prog = parse_locy(src).unwrap();
    let result = compile(&prog);

    // FIXED (warded.rs): the `Parenthesized` arm now recurses into its nested
    // path, so `b` is seen exactly as it is without the parentheses.
    assert!(
        result.is_ok(),
        "`b` is bound inside the parentheses and the rule is warded; \
         parenthesising a pattern must not change whether it compiles — got {result:?}"
    );
}

/// A quantified sub-pattern binds its variables too.
#[test]
fn quantified_parenthesized_companion_is_warded() {
    let src = "CREATE RULE r AS MATCH ((a:Person)-[:KNOWS]->(b:Person)){1,3} \
               DERIVE (NEW n:Tag)-[:LINK]->(b)";
    let prog = parse_locy(src).unwrap();
    let result = compile(&prog);
    assert!(
        result.is_ok(),
        "a quantified sub-pattern still binds `b`; got {result:?}"
    );
}

/// The check must keep rejecting a genuinely unwarded companion.
///
/// Recursing into `Parenthesized` widens the set of variables considered
/// match-bound, so the inverse case has to be pinned: a companion bound only by
/// an `IS` reference is still not warded, parentheses or not.
#[test]
fn genuinely_unwarded_companion_is_still_rejected() {
    let src = "CREATE RULE base AS MATCH (a)-[:R]->(b) YIELD a, b \
               CREATE RULE r AS MATCH ((x)-[:R]->(y)) WHERE y IS base TO z \
               DERIVE (NEW n:T)-[:LINK]->(z)";
    let prog = parse_locy(src).unwrap();
    let result = compile(&prog);

    match result.unwrap_err() {
        LocyCompileError::WardednessViolation { rule, variable } => {
            assert_eq!(rule, "r");
            assert_eq!(variable, "z", "`z` is IS-bound, never match-bound");
        }
        e => panic!("expected WardednessViolation, got {e:?}"),
    }
}
