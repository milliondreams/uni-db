// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Regression for the 2026-06-10 review bug #4: the parenthesized-path
// quantifier walker (`build_path_quantifier`) mis-parsed the empty-lower form
// `{,m}` as `{m,}` (treating `m` as the *min*), and panicked (process abort for
// an embedded library) on malformed bounds `{-2}` / `{0x2}` / `{4294967296}`
// because it used `u32::parse().unwrap()`.
//
// openCypher semantics: `*`/`{,}` = 1..∞, `{n}` = n..n, `{n,}` = n..∞,
// `{n,m}` = n..m, `{,m}` = 1..m (represented as `min: None`, which the executor
// maps to 1).
// Rust guideline compliant

use uni_cypher::ast::{Clause, PatternElement, Query, Range, Statement};
use uni_cypher::parse;

/// Extracts the [`Range`] of the first parenthesized path element in the first
/// `MATCH` clause of `query`.
///
/// # Panics
///
/// Panics (failing the test) if `query` does not parse or does not contain a
/// parenthesized path element with a quantifier — the shape this regression
/// asserts on.
fn first_parenthesized_range(query: &str) -> Range {
    let parsed = parse(query).expect("query must parse");
    let Query::Single(Statement { clauses }) = parsed else {
        panic!("expected a single statement, got {parsed:?}");
    };
    for clause in &clauses {
        if let Clause::Match(m) = clause {
            for path in &m.pattern.paths {
                for element in &path.elements {
                    if let PatternElement::Parenthesized { range, .. } = element {
                        return range
                            .clone()
                            .expect("parenthesized element must carry a quantifier range");
                    }
                }
            }
        }
    }
    panic!("no parenthesized path element found in: {query}");
}

/// Parses `input` on a worker thread so a panic in the walker surfaces as a
/// joinable error rather than aborting the test process.
///
/// Returns `Ok` with the parse outcome stringified; `Err` only if the parse
/// itself panicked (which this regression must rule out).
fn parse_catching_panic(input: &str) -> Result<Result<(), String>, String> {
    let owned = input.to_string();
    std::thread::Builder::new()
        .spawn(move || parse(&owned).map(|_| ()).map_err(|e| e.to_string()))
        .expect("spawn parse thread")
        .join()
        .map_err(|_| "parser panicked on malformed quantifier".to_string())
}

#[test]
fn empty_lower_bound_is_max_not_min() {
    // RED before the fix: `{,2}` was parsed as `min: Some(2)` (i.e. `{2,}`).
    let range = first_parenthesized_range("MATCH ((a)-[r]->(b)){,2} RETURN a");
    assert_eq!(range.max, Some(2), "`{{,2}}` upper bound must be 2");
    assert_eq!(
        range.min, None,
        "`{{,2}}` lower bound must be unbounded-low (None), not Some(2)"
    );
}

#[test]
fn bounded_range_min_and_max() {
    let range = first_parenthesized_range("MATCH ((a)-[r]->(b)){2,5} RETURN a");
    assert_eq!(range.min, Some(2));
    assert_eq!(range.max, Some(5));
}

#[test]
fn exact_count_is_min_eq_max() {
    let range = first_parenthesized_range("MATCH ((a)-[r]->(b)){3} RETURN a");
    assert_eq!(range.min, Some(3));
    assert_eq!(range.max, Some(3));
}

#[test]
fn open_upper_bound_min_only() {
    let range = first_parenthesized_range("MATCH ((a)-[r]->(b)){2,} RETURN a");
    assert_eq!(range.min, Some(2));
    assert_eq!(range.max, None);
}

#[test]
fn negative_bound_errors_not_panics() {
    let outcome = parse_catching_panic("MATCH ((a)-[r]->(b)){-2} RETURN a")
        .expect("parser must not panic on `{-2}`");
    assert!(outcome.is_err(), "`{{-2}}` must be a parse error");
}

#[test]
fn hex_bound_errors_not_panics() {
    let outcome = parse_catching_panic("MATCH ((a)-[r]->(b)){0x2} RETURN a")
        .expect("parser must not panic on `{0x2}`");
    assert!(outcome.is_err(), "`{{0x2}}` must be a parse error");
}

#[test]
fn overflow_bound_errors_not_panics() {
    let outcome = parse_catching_panic("MATCH ((a)-[r]->(b)){4294967296} RETURN a")
        .expect("parser must not panic on `{4294967296}` (u32 overflow)");
    assert!(
        outcome.is_err(),
        "`{{4294967296}}` (> u32::MAX) must be a parse error"
    );
}

#[test]
fn caret_bound_errors_not_panics() {
    // Control: a grammar-level malformed bound must produce a clean error, not a
    // panic.
    let outcome = parse_catching_panic("MATCH ((a)-[r]->(b)){2^32} RETURN a")
        .expect("parser must not panic on `{2^32}`");
    assert!(outcome.is_err(), "`{{2^32}}` must be a parse error");
}

#[test]
fn relationship_form_variable_length_unaffected() {
    // Non-regression: the relationship variable-length form `*..m` (handled by
    // `build_range`, a different code path) must keep parsing.
    assert!(
        parse("MATCH (a)-[r*..2]->(b) RETURN a").is_ok(),
        "relationship-form `*..2` must still parse"
    );
}
