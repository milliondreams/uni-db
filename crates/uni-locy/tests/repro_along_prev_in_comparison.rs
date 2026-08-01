//! Repro for `grammar/locy_walker.rs` — `build_locy_comparison_expression`.
//!
//! A Locy `ALONG` expression may reference the previous hop's value as
//! `prev.<field>`, which the walker turns into `LocyExpr::PrevRef`. But when the
//! expression contains a `comparison_tail`, the walker abandons the structured
//! walk: it re-slices the source span and re-parses the whole thing with
//! `CypherParser`, wrapping the result in `LocyExpr::Cypher`.
//!
//! `PREV` is reserved in `locy.pest` but *not* in `cypher.pest`, so the
//! re-parse succeeds and silently produces an ordinary `Expr::Property` on a
//! variable called `prev`. The marker is gone. Both validators are blinded the
//! same way: `collect_prev_refs` (typecheck) and `validate_prev_refs` (planner)
//! see a `LocyExpr::Cypher` and return empty / `Ok`, so nothing downstream
//! notices.
//!
//! Measured behaviour before the fix — note that rejection happens at *compile*
//! time, not parse time:
//!
//! | form                       | base case                   | recursive rule |
//! |----------------------------|-----------------------------|----------------|
//! | `prev.h + 1` (arithmetic)  | `PrevInBaseCase` ✔          | compiles ✔     |
//! | `prev.h > 5` (comparison)  | **compiles** ✘              | **compiles, binding lost** ✘ |
//!
//! So the comparison form is broken in *both* directions: wrongly accepted
//! where it should be refused, and silently stripped where it should carry the
//! previous hop's value.
//!
//! **The fix refuses `prev` inside a comparison outright, including in a
//! recursive rule where the construct would otherwise be legitimate.** That is
//! a deliberate trade: the form cannot work today in either position, and
//! walking comparisons structurally instead would mean re-implementing what the
//! re-parse handles for free — `IN`, `STARTS WITH`, `IS NULL`, list literals —
//! none of which `LocyBinaryOp` models. Regressing those to rescue a construct
//! that has never worked is the worse trade. Refusing it is loud, and leaves
//! the door open to supporting it properly later.

use uni_cypher::parse_locy;
use uni_locy::compile;
use uni_locy::compiler::errors::LocyCompileError;

/// Parse then compile, flattening both failure kinds into one message.
///
/// The fix may land as either a parse error or a compile error depending on
/// where the `prev` reference is detected; these tests care that it is refused
/// and says why, not which layer says it.
fn compile_src(src: &str) -> Result<(), String> {
    let prog = parse_locy(src).map_err(|e| format!("parse error: {e}"))?;
    compile(&prog).map(|_| ()).map_err(|e| format!("{e:?}"))
}

const BASE_ARITH: &str = "CREATE RULE r AS MATCH (a:N)-[:E]->(b:N) \
                          ALONG h = prev.h + 1 YIELD a, b";
const BASE_CMP: &str = "CREATE RULE r AS MATCH (a:N)-[:E]->(b:N) \
                        ALONG ok = prev.h > 5 YIELD a, b";
const REC_ARITH: &str = "CREATE RULE r AS MATCH (a:N)-[:E]->(b:N) ALONG h = 1 YIELD a, b \
                         CREATE RULE r AS MATCH (a:N)-[:E]->(b:N) WHERE a IS r \
                         ALONG h = prev.h + 1 YIELD a, b";
const REC_CMP: &str = "CREATE RULE r AS MATCH (a:N)-[:E]->(b:N) ALONG h = 1 YIELD a, b \
                       CREATE RULE r AS MATCH (a:N)-[:E]->(b:N) WHERE a IS r \
                       ALONG ok = prev.h > 5 YIELD a, b";

/// Control: arithmetic keeps the `PrevRef` marker, so a base case is caught.
/// This has always worked and must keep working.
#[test]
fn prev_in_arithmetic_base_case_is_still_rejected() {
    let prog = parse_locy(BASE_ARITH).expect("arithmetic form parses");
    match compile(&prog) {
        Err(LocyCompileError::PrevInBaseCase { rule, field }) => {
            assert_eq!(rule, "r");
            assert_eq!(field, "h");
        }
        other => panic!("expected PrevInBaseCase, got {other:?}"),
    }
}

/// Inverse guard: `prev` in arithmetic inside a *recursive* rule is legitimate
/// and must keep compiling. Refusing the comparison form must not spill over
/// onto the form that works.
#[test]
fn prev_in_arithmetic_recursive_still_compiles() {
    assert!(
        compile_src(REC_ARITH).is_ok(),
        "`prev` in a recursive rule is the whole point of ALONG"
    );
}

/// The bug: in a base case this used to compile, because the re-parse threw the
/// marker away and `collect_prev_refs` then had nothing to find.
#[test]
fn prev_in_comparison_base_case_is_not_silently_accepted() {
    let err = compile_src(BASE_CMP)
        .expect_err("`prev` inside a comparison must not slip past validation");
    assert!(
        err.contains("prev"),
        "the error must name what it refused, got: {err}"
    );
}

/// And in a recursive rule it compiled while silently losing the binding —
/// which is worse, because there the construct looks like it should work.
#[test]
fn prev_in_comparison_recursive_is_refused_not_silently_stripped() {
    let err = compile_src(REC_CMP).expect_err(
        "a comparison on `prev` cannot carry the previous hop's value, so it \
         must be refused rather than compiled with the binding silently lost",
    );
    assert!(
        err.contains("prev"),
        "the error must name `prev`, got: {err}"
    );
}

/// Inverse guard: comparisons that do not mention `prev` must keep working.
///
/// The re-parse exists because it handles these for free. Refusing `prev` must
/// not cost any of them.
#[test]
fn comparisons_without_prev_still_parse() {
    for src in [
        "CREATE RULE r AS MATCH (a:N)-[:E]->(b:N) ALONG ok = a.h > 5 YIELD a, b",
        "CREATE RULE r AS MATCH (a:N)-[:E]->(b:N) ALONG ok = a.h IN [1, 2, 3] YIELD a, b",
        "CREATE RULE r AS MATCH (a:N)-[:E]->(b:N) ALONG ok = a.name STARTS WITH 'x' YIELD a, b",
        "CREATE RULE r AS MATCH (a:N)-[:E]->(b:N) ALONG ok = a.h IS NULL YIELD a, b",
    ] {
        assert!(
            parse_locy(src).is_ok(),
            "a comparison without `prev` must still re-parse: {src}"
        );
    }
}

/// A variable genuinely named `prev` stays reachable via backticks.
///
/// `locy.pest` documents this escape hatch; refusing bare `prev` must not close
/// it, or a user with such a variable would have no way to compare on it.
#[test]
fn backtick_escaped_prev_is_not_refused() {
    let src = "CREATE RULE r AS MATCH (a:N)-[:E]->(b:N) \
               ALONG ok = `prev`.h > 5 YIELD a, b";
    assert!(
        parse_locy(src).is_ok(),
        "a backtick-escaped variable named `prev` is an ordinary variable"
    );
}
