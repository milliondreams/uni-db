// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Repro for crates/uni-cypher/src/grammar/mod.rs:141
//
// parse_expression parses with Rule::expression, which in cypher.pest is
// `expression = { or_expression }` — NO SOI/EOI anchor (unlike top-level
// `query = { SOI ~ ... ~ EOI }`). pest succeeds on the longest valid prefix
// and silently discards trailing tokens. So malformed input like
// "n.age > 18 AND" or "a = 1 b = 2" parses as a truncated expression with no
// error. Untrusted callers (plugin trigger conditions, UDF bodies) rely on
// this to reject malformed source.

use uni_cypher::parse_expression;

// FIXED: parse_expression now anchors on SOI/EOI, so an incomplete boolean
// expression (dangling `AND`) is a parse error instead of a truncated Ok
// (repro for grammar/mod.rs:141).
#[test]
fn trailing_and_is_an_error() {
    let res = parse_expression("n.age > 18 AND");
    assert!(
        res.is_err(),
        "an incomplete boolean expression must be a parse error. got={res:?}"
    );
    eprintln!("parse_expression(\"n.age > 18 AND\") = {res:?}");
}

// FIXED: two statements glued together no longer parse as the first with the
// tail silently dropped (repro for grammar/mod.rs:141).
#[test]
fn second_assignment_is_an_error() {
    let res = parse_expression("a = 1 b = 2");
    assert!(
        res.is_err(),
        "trailing `b = 2` must not be silently dropped. got={res:?}"
    );
    eprintln!("parse_expression(\"a = 1 b = 2\") = {res:?}");
}

// FIXED: obvious garbage after a valid prefix now errors rather than being
// discarded (repro for grammar/mod.rs:141).
#[test]
fn garbage_suffix_is_an_error() {
    let res = parse_expression("1 + 2 THIS IS GARBAGE )))");
    assert!(
        res.is_err(),
        "trailing garbage after `1 + 2` must be a parse error. got={res:?}"
    );
    eprintln!("parse_expression(\"1 + 2 THIS IS GARBAGE )))\") = {res:?}");
}

// A well-formed expression (with surrounding whitespace) still parses cleanly —
// the anchoring must not reject legitimate input.
#[test]
fn well_formed_expression_still_parses() {
    let res = parse_expression("  n.age > 18 AND n.active  ");
    assert!(
        res.is_ok(),
        "a complete expression must still parse. got={res:?}"
    );
}
