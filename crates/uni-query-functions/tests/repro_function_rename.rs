// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Runnable repro for the verified finding in the function-rename AST walker.
//! The walker drives plugin `ReplacementScanProvider` function renames as an
//! AST pass, but its `rewrite_clause` catch-all (`other => other`) silently
//! skips `Clause::Merge` and `Clause::WithRecursive`, so any function call
//! living inside a MERGE pattern / ON MATCH / ON CREATE, or inside a
//! WITH RECURSIVE nested query, is never rewritten.

use uni_cypher::ast::{
    Clause, Expr, LabelExpr, MergeClause, NodePattern, PathPattern, Pattern, PatternElement, Query,
    ReturnClause, ReturnItem, SetItem, Statement, WithRecursiveClause,
};

use uni_query_functions::rewrite::function_rename::rewrite_function_calls_in_query;

/// A rename closure that maps every `oldfn` call to `newfn`.
fn rename_oldfn(name: &str) -> anyhow::Result<Option<String>> {
    if name == "oldfn" {
        Ok(Some("newfn".to_string()))
    } else {
        Ok(None)
    }
}

fn call_oldfn() -> Expr {
    Expr::FunctionCall {
        name: "oldfn".to_string(),
        args: vec![],
        distinct: false,
        window_spec: None,
    }
}

/// Walk into a returned query and pull the first function-call name found in
/// the MERGE node's property map. Panics on any unexpected shape.
fn merge_prop_fn_name(query: &Query) -> String {
    let Query::Single(stmt) = query else {
        panic!("expected Query::Single, got {query:?}");
    };
    let Clause::Merge(m) = &stmt.clauses[0] else {
        panic!("expected Clause::Merge, got {:?}", stmt.clauses[0]);
    };
    let PatternElement::Node(n) = &m.pattern.paths[0].elements[0] else {
        panic!("expected a node pattern");
    };
    let Some(Expr::Map(entries)) = &n.properties else {
        panic!("expected a property map");
    };
    let Expr::FunctionCall { name, .. } = &entries[0].1 else {
        panic!("expected a function call value");
    };
    name.clone()
}

/// Finding [12] rewrite/function_rename.rs:146 — the catch-all `other => other`
/// in `rewrite_clause` skips `Clause::Merge`, so a function call in a MERGE
/// pattern's property map is never handed to the rename closure.
#[test]
fn repro_finding_12_merge_pattern_function_not_renamed() {
    let merge = Clause::Merge(MergeClause {
        pattern: Pattern {
            paths: vec![PathPattern {
                variable: None,
                elements: vec![PatternElement::Node(NodePattern {
                    variable: Some("n".to_string()),
                    labels: LabelExpr::Conjunction(vec!["Person".to_string()]),
                    properties: Some(Expr::Map(vec![("id".to_string(), call_oldfn())])),
                    where_clause: None,
                })],
                shortest_path_mode: None,
            }],
        },
        on_match: vec![],
        on_create: vec![],
    });

    let query = Query::Single(Statement {
        clauses: vec![merge],
    });

    let rewritten = rewrite_function_calls_in_query(query, &mut rename_oldfn).unwrap();

    // FIXED (rewrite/function_rename.rs): the walker now descends into
    // Clause::Merge, so the MERGE property-map call is renamed to `newfn`
    // exactly as it is in MATCH/RETURN/WHERE positions.
    assert_eq!(
        merge_prop_fn_name(&rewritten),
        "newfn",
        "MERGE property-map function call must be renamed by the walker"
    );
}

/// Finding [12] rewrite/function_rename.rs:146 — the same catch-all skips
/// `Clause::Merge`'s `on_match`/`on_create` SetItems, so a function call in an
/// ON CREATE SET assignment is also never renamed.
#[test]
fn repro_finding_12_merge_on_create_function_not_renamed() {
    let merge = Clause::Merge(MergeClause {
        pattern: Pattern {
            paths: vec![PathPattern {
                variable: None,
                elements: vec![PatternElement::Node(NodePattern {
                    variable: Some("n".to_string()),
                    labels: LabelExpr::Conjunction(vec!["Person".to_string()]),
                    properties: None,
                    where_clause: None,
                })],
                shortest_path_mode: None,
            }],
        },
        on_match: vec![],
        on_create: vec![SetItem::Variable {
            variable: "n".to_string(),
            value: call_oldfn(),
        }],
    });

    let query = Query::Single(Statement {
        clauses: vec![merge],
    });

    let rewritten = rewrite_function_calls_in_query(query, &mut rename_oldfn).unwrap();

    let Query::Single(stmt) = &rewritten else {
        panic!("expected Query::Single");
    };
    let Clause::Merge(m) = &stmt.clauses[0] else {
        panic!("expected Clause::Merge");
    };
    let SetItem::Variable { value, .. } = &m.on_create[0] else {
        panic!("expected a SetItem::Variable");
    };
    let Expr::FunctionCall { name, .. } = value else {
        panic!("expected a function call");
    };

    // FIXED (rewrite/function_rename.rs): the ON CREATE SET assignment call is
    // now renamed to `newfn`.
    assert_eq!(
        name, "newfn",
        "MERGE ON CREATE function call must be renamed by the walker"
    );
}

/// Finding [12] rewrite/function_rename.rs:146 — `Clause::WithRecursive` (whose
/// nested `Box<Query>` is user-rewritable) also falls through the catch-all, so
/// a function call inside the recursive sub-query is never renamed.
#[test]
fn repro_finding_12_with_recursive_nested_query_not_renamed() {
    // Nested query: RETURN oldfn() AS x
    let nested = Query::Single(Statement {
        clauses: vec![Clause::Return(ReturnClause {
            distinct: false,
            items: vec![ReturnItem::Expr {
                expr: call_oldfn(),
                alias: Some("x".to_string()),
                source_text: None,
            }],
            order_by: None,
            skip: None,
            limit: None,
        })],
    });

    let with_rec = Clause::WithRecursive(WithRecursiveClause {
        name: "r".to_string(),
        query: Box::new(nested),
        items: vec![],
    });

    let query = Query::Single(Statement {
        clauses: vec![with_rec],
    });

    let rewritten = rewrite_function_calls_in_query(query, &mut rename_oldfn).unwrap();

    let Query::Single(stmt) = &rewritten else {
        panic!("expected Query::Single");
    };
    let Clause::WithRecursive(wr) = &stmt.clauses[0] else {
        panic!("expected Clause::WithRecursive");
    };
    let Query::Single(inner) = wr.query.as_ref() else {
        panic!("expected nested Query::Single");
    };
    let Clause::Return(ret) = &inner.clauses[0] else {
        panic!("expected nested Return");
    };
    let ReturnItem::Expr { expr, .. } = &ret.items[0] else {
        panic!("expected a Return expr item");
    };
    let Expr::FunctionCall { name, .. } = expr else {
        panic!("expected a function call");
    };

    // FIXED (rewrite/function_rename.rs): the walker descends into
    // Clause::WithRecursive's nested query, so the recursive sub-query's call is
    // renamed to `newfn`.
    assert_eq!(
        name, "newfn",
        "WITH RECURSIVE nested-query function call must be renamed by the walker"
    );
}
