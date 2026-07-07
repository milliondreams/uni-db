// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Repro for crates/uni-cypher/src/grammar/walker.rs:1410
//
// build_map_literal strips the surrounding quotes from a string-literal map key
// (`&s[1..s.len()-1]`) but NEVER calls unescape_string — unlike every other
// string-literal path (general literal path ~L1226, build_string_literal
// ~L2765). So escape sequences (\n, \t, ...) and SQL-style doubled quotes ('')
// survive verbatim in the key, diverging from how the same literal is decoded
// as a value.

use uni_cypher::ast::{CypherLiteral, Expr};
use uni_cypher::parse_expression;

fn map_entries(src: &str) -> Vec<(String, Expr)> {
    match parse_expression(src).expect("parse map literal") {
        Expr::Map(entries) => entries,
        other => panic!("expected Expr::Map, got {other:?}"),
    }
}

fn string_value(src: &str) -> String {
    match parse_expression(src).expect("parse string literal") {
        Expr::Literal(CypherLiteral::String(s)) => s,
        other => panic!("expected string literal, got {other:?}"),
    }
}

// FIXED: string-literal map keys are now decoded via build_string_literal, so
// the key is the 3-char string a<LF>b (newline decoded), identical to how the
// same literal decodes as a VALUE (repro for grammar/walker.rs:1410).
#[test]
fn map_key_backslash_n_is_decoded() {
    // Cypher source:  { "a\nb": 1 }   where \n is backslash + 'n'.
    let entries = map_entries("{ \"a\\nb\": 1 }");
    let key = &entries[0].0;

    // As a VALUE the identical literal is decoded to a real newline:
    let as_value = string_value("\"a\\nb\"");
    assert_eq!(as_value, "a\nb", "value path decodes \\n to newline");
    assert_eq!(as_value.len(), 3);

    eprintln!("map key bytes = {:?} (len {})", key, key.len());
    // Correct behavior: the key decodes to a real newline, matching the value.
    assert_eq!(key, "a\nb", "map key decodes \\n to a real newline");
    assert_eq!(key.len(), 3, "decoded key is 3 chars (a, LF, b)");
    assert_eq!(
        key, &as_value,
        "map key matches the value decoding of the SAME literal"
    );
}

// FIXED: doubled quotes in a string-literal map key are collapsed exactly as in
// value decoding (repro for grammar/walker.rs:1410).
#[test]
fn map_key_doubled_quote_is_collapsed() {
    // Cypher source:  { 'it''s': 1 }
    let entries = map_entries("{ 'it''s': 1 }");
    let key = &entries[0].0;

    let as_value = string_value("'it''s'");
    assert_eq!(as_value, "it's", "value path collapses doubled quote");

    eprintln!("map key = {key:?}");
    assert_eq!(key, "it's", "map key collapses the doubled quote");
    assert_eq!(key, &as_value, "map key matches value decoding");
}
