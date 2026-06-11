// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Regression for the 2026-06-10 review bug #7: the pest walker had no
// recursion-depth limit, so a deeply-nested expression / pattern (e.g. ~500
// nested parens, or nested lists / maps / CASE) overflowed the stack with an
// uncatchable `fatal runtime error: stack overflow, aborting`. For an embedded
// library that is a process abort triggered by a query string.
//
// These tests must complete (returning `Err(ParseError)`) rather than abort.
// Each pathological input is parsed on a dedicated thread with a SMALL stack so
// that, absent the depth guard, it would overflow well before any sane limit —
// proving the guard rejects deep nesting *before* the stack is exhausted.
// Rust guideline compliant

use uni_cypher::{parse, parse_expression};

/// Parse `input` on a worker thread with a deliberately small (1 MiB) stack so
/// that unbounded recursion would overflow. Returns the parse outcome; panics
/// (test failure) only if the thread itself could not be joined. A real stack
/// overflow aborts the whole process — so if this returns at all, the parser
/// did not recurse unboundedly.
fn parse_on_small_stack(input: String) -> Result<(), String> {
    std::thread::Builder::new()
        .stack_size(1024 * 1024)
        .spawn(move || parse(&input).map(|_| ()).map_err(|e| e.to_string()))
        .expect("spawn parse thread")
        .join()
        .expect("parse thread must not abort the process (stack overflow)")
}

#[test]
fn deeply_nested_parens_error_not_overflow() {
    let depth = 5000;
    let input = format!("RETURN {}1{}", "(".repeat(depth), ")".repeat(depth));
    let res = parse_on_small_stack(input);
    assert!(
        res.is_err(),
        "deeply nested parens must be rejected, not parsed"
    );
}

#[test]
fn deeply_nested_lists_error_not_overflow() {
    let depth = 5000;
    let input = format!("RETURN {}1{}", "[".repeat(depth), "]".repeat(depth));
    let res = parse_on_small_stack(input);
    assert!(res.is_err(), "deeply nested lists must be rejected");
}

#[test]
fn deeply_nested_maps_error_not_overflow() {
    let depth = 3000;
    let input = format!("RETURN {}1{}", "{a:".repeat(depth), "}".repeat(depth));
    let res = parse_on_small_stack(input);
    assert!(res.is_err(), "deeply nested maps must be rejected");
}

#[test]
fn deeply_nested_case_error_not_overflow() {
    let depth = 2000;
    let input = format!(
        "RETURN {}1{}",
        "CASE WHEN true THEN ".repeat(depth),
        " ELSE 0 END".repeat(depth)
    );
    let res = parse_on_small_stack(input);
    assert!(res.is_err(), "deeply nested CASE must be rejected");
}

#[test]
fn deeply_nested_parenthesized_pattern_error_not_overflow() {
    let depth = 3000;
    let input = format!(
        "MATCH {}(n){} RETURN n",
        "(".repeat(depth),
        ")".repeat(depth)
    );
    let res = parse_on_small_stack(input);
    assert!(
        res.is_err(),
        "deeply nested parenthesized patterns must be rejected"
    );
}

#[test]
fn moderately_nested_expression_still_parses() {
    // Well under the limit: legitimate moderately-nested expressions must
    // continue to parse successfully.
    let input = format!("RETURN {}1{}", "(".repeat(16), ")".repeat(16));
    assert!(
        parse(&input).is_ok(),
        "moderate paren nesting must still parse"
    );

    let nested_list = format!("{}1{}", "[".repeat(8), "]".repeat(8));
    assert!(
        parse_expression(&nested_list).is_ok(),
        "moderate list nesting must still parse"
    );

    // A realistic nested expression (function calls, arithmetic, CASE) must be
    // unaffected by the guard.
    assert!(
        parse("RETURN CASE WHEN abs(a.x + (b.y * 2)) > 3 THEN [1, [2, 3]] ELSE {k: 1} END").is_ok(),
        "realistic nested expression must still parse"
    );
}
