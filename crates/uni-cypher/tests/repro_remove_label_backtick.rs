// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Repro for crates/uni-cypher/src/grammar/walker.rs:445
//
// build_remove_item collects REMOVE label names with raw `.as_str()` instead of
// normalize_identifier, so backtick-quoted labels keep their backticks. The SET
// branch (walker.rs:387) and every node-pattern label site normalize. A
// backtick-quoted REMOVE label therefore stores "`Person`" while the label
// created/matched elsewhere is the normalized "Person" — so `REMOVE n:`Person``
// silently no-ops.

use uni_cypher::ast::{Clause, Query, RemoveItem, SetItem};
use uni_cypher::parse;

fn remove_labels(src: &str) -> Vec<String> {
    match parse(src).expect("parse REMOVE") {
        Query::Single(stmt) => stmt
            .clauses
            .iter()
            .find_map(|c| match c {
                Clause::Remove(r) => r.items.iter().find_map(|i| match i {
                    RemoveItem::Labels { labels, .. } => Some(labels.clone()),
                    _ => None,
                }),
                _ => None,
            })
            .expect("no REMOVE labels item"),
        _ => panic!("unexpected query shape"),
    }
}

fn set_labels(src: &str) -> Vec<String> {
    match parse(src).expect("parse SET") {
        Query::Single(stmt) => stmt
            .clauses
            .iter()
            .find_map(|c| match c {
                Clause::Set(s) => s.items.iter().find_map(|i| match i {
                    SetItem::Labels { labels, .. } => Some(labels.clone()),
                    _ => None,
                }),
                _ => None,
            })
            .expect("no SET labels item"),
        _ => panic!("unexpected query shape"),
    }
}

// FIXED: REMOVE labels now strip backticks via normalize_identifier, matching
// the SET branch, so `REMOVE n:`Person`` targets the normalized label `Person`
// (repro for grammar/walker.rs:445).
#[test]
fn remove_backtick_label_strips_backticks() {
    let rem = remove_labels("MATCH (n:Person) REMOVE n:`Person`");
    let set = set_labels("MATCH (n) SET n:`Person`");

    eprintln!("REMOVE labels = {rem:?}   SET labels = {set:?}");

    // SET correctly normalizes:
    assert_eq!(set, vec!["Person".to_string()], "SET strips backticks");

    // Correct REMOVE behavior: backticks stripped, matching SET.
    assert_eq!(
        rem,
        vec!["Person".to_string()],
        "REMOVE must strip backticks so it matches the normalized label"
    );
    assert_eq!(rem, set, "REMOVE and SET normalize labels consistently");
}
