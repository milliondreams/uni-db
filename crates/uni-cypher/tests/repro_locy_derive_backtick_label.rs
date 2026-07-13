// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Repro for crates/uni-cypher/src/grammar/locy_walker.rs:1359 (and :1386)
//
// build_derive_node_spec pushes DERIVE node labels with raw `.as_str()`
// (locy_walker.rs:1359) and build_derive_edge_spec sets edge_type with raw
// `.as_str()` (locy_walker.rs:1386) — neither strips backticks via
// normalize_identifier. The MATCH-side label handling DOES normalize
// (walker.rs:387, 816-818), and the DERIVE *variable* is normalized too
// (locy_walker.rs:1343). So a backtick-quoted DERIVE label/edge-type is stored
// WITH backticks while the same name on the MATCH side is stored without them —
// the DERIVE-created data never matches.

use uni_cypher::locy_ast::{DeriveClause, LocyStatement, RuleOutput};
use uni_cypher::parse_locy;

// FIXED: DERIVE node labels and edge types now strip backticks via
// normalize_identifier, matching the MATCH side (repro for
// locy_walker.rs:1359,1386).
#[test]
fn derive_backtick_label_and_edge_strip_backticks() {
    let program =
        parse_locy("CREATE RULE r AS MATCH (a) DERIVE (NEW x:`My Label`)-[:`HAS ITEM`]->(NEW y:B)")
            .expect("parse_locy");

    let rule = match &program.statements[0] {
        LocyStatement::Rule(r) => r,
        other => panic!("expected Rule, got {other:?}"),
    };
    let pats = match &rule.output {
        RuleOutput::Derive(DeriveClause::Patterns(p)) => p,
        other => panic!("expected Derive Patterns, got {other:?}"),
    };
    let source = &pats[0].source;
    let edge_type = &pats[0].edge.edge_type;

    eprintln!("DERIVE source.labels = {:?}", source.labels);
    eprintln!("DERIVE edge_type     = {edge_type:?}");

    // Correct behavior: backticks stripped on both label and edge type.
    assert_eq!(
        source.labels,
        vec!["My Label".to_string()],
        "DERIVE label must be normalized (backticks stripped) to match the MATCH side"
    );
    assert_eq!(
        edge_type, "HAS ITEM",
        "DERIVE edge type must be normalized (backticks stripped) to match the MATCH side"
    );

    // Contrast: the DERIVE variable IS normalized (control that normalization
    // is applied elsewhere in the same builder).
    assert_eq!(source.variable, "x", "DERIVE variable is normalized");
}
