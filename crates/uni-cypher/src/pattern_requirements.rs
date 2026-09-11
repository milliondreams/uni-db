// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! What a query's patterns need the fixture to contain (#205).
//!
//! # Why this exists
//!
//! `ldbc_ic14_plans_and_executes` passed for weeks without ever running the
//! code it was written to cover. Its query text was the real IC14, whose weight
//! term is a pattern comprehension over
//! `(:Person)<-[:HAS_CREATOR]-(:Comment)-[:REPLY_OF]->(:Post)-[:HAS_CREATOR]->(:Person)`.
//! The fixture created two `Person`s and one `KNOWS` edge — no `Comment`, no
//! `Post`, neither edge type. The comprehension matched nothing, the `reduce`
//! never evaluated its body, `startNode(r)` was never called, and every
//! assertion passed honestly.
//!
//! Two properties made it invisible, and both are general:
//!
//! * **The query text was real.** A reviewer asking "does this test the real
//!   query?" gets yes. The reduction was in the *data*, which is not visible
//!   beside the query.
//! * **Vacuous truth reads as success.** A comprehension over an empty match
//!   yields `0.0` rather than erroring, so assertions that avoid the computed
//!   column pass on their own terms.
//!
//! This module extracts, from the query text alone, the entity types the
//! patterns need before they can match anything. A test harness pairs that with
//! row counts from the live fixture and fails loudly when one is empty, which is
//! the cheap statement issue #205 asked for.
//!
//! # What this proves, and what it does not
//!
//! A satisfied requirement set says the **ingredients** exist. It does not say
//! they **compose** into a match: three labels and two edge types can all be
//! non-empty while no path threads them together. This is a necessary
//! condition, deliberately — it is mechanical, needs no engine observable, and
//! catches the signature that actually occurred.
//!
//! The sufficient condition is asserting the value the code under test
//! *computes*. Nothing here substitutes for that, and a test that leans on this
//! module instead of asserting its result is still the defect #205 describes.
//!
//! # What is deliberately not required
//!
//! Each exclusion is a case where an empty match is legitimate, so requiring
//! rows would produce false failures:
//!
//! * **`CREATE` and `MERGE` patterns** — they populate the fixture rather than
//!   depend on it.
//! * **`OPTIONAL MATCH`** — its whole contract is to produce a row with `NULL`s
//!   when nothing matches.
//! * **`EXISTS { }`, `COUNT { }`, `COLLECT { }` subqueries** — routinely used to
//!   assert that something is *absent*.
//! * **Variable-length relationships whose range starts at zero** (`[:T*0..]`)
//!   — a zero-length path matches with no edge of that type in existence.
//!
//! `MATCH` patterns and pattern comprehensions are what remain, and they are
//! where the defect class lives.

use crate::ast::{Clause, Expr, LabelExpr, Pattern, PatternElement, Query, ReturnItem, Statement};
use crate::grammar::{ParseError, parse};

/// One thing a fixture must contain before a query's patterns can match.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Requirement {
    /// A node label needing at least one vertex.
    ///
    /// Emitted for `(n:A)` and for **each** member of the conjunctive `(n:A:B)`
    /// form, where the node must carry every listed label.
    Label(String),
    /// At least one of these node labels needs a vertex (`(n:A|B)`).
    ///
    /// A disjunction is satisfied by any single member, so requiring all of them
    /// would fail tests that legitimately populate one arm.
    AnyLabel(Vec<String>),
    /// An edge type needing at least one edge.
    EdgeType(String),
    /// At least one of these edge types needs an edge (`-[r:A|B]->`).
    AnyEdgeType(Vec<String>),
}

impl Requirement {
    /// The entity names this requirement mentions, for diagnostics.
    #[must_use]
    pub fn names(&self) -> &[String] {
        match self {
            Self::Label(n) | Self::EdgeType(n) => std::slice::from_ref(n),
            Self::AnyLabel(v) | Self::AnyEdgeType(v) => v,
        }
    }

    /// Whether this names node labels rather than edge types.
    #[must_use]
    pub fn is_label(&self) -> bool {
        matches!(self, Self::Label(_) | Self::AnyLabel(_))
    }
}

/// The entity types `query`'s `MATCH` patterns and pattern comprehensions need.
///
/// Deduplicated and sorted, so the result is stable across parses and can be
/// compared in tests.
///
/// # Errors
///
/// Returns the parse error if `query` is not valid Cypher.
pub fn of(query: &str) -> Result<Vec<Requirement>, ParseError> {
    let ast = parse(query)?;
    let mut out = Vec::new();
    walk_query(&ast, &mut out);
    out.sort();
    out.dedup();
    Ok(out)
}

fn walk_query(q: &Query, out: &mut Vec<Requirement>) {
    match q {
        Query::Single(stmt) => walk_statement(stmt, out),
        Query::Union { left, right, .. } => {
            walk_query(left, out);
            walk_query(right, out);
        }
        Query::Explain(inner) | Query::TimeTravel { query: inner, .. } => walk_query(inner, out),
        // A schema command has no data-dependent pattern.
        Query::Schema(_) => {}
    }
}

fn walk_statement(stmt: &Statement, out: &mut Vec<Requirement>) {
    for clause in &stmt.clauses {
        match clause {
            // The only clause whose pattern is a *read* dependency. An
            // `OPTIONAL MATCH` is contractually allowed to match nothing.
            Clause::Match(m) => {
                if !m.optional {
                    walk_pattern(&m.pattern, out);
                }
                if let Some(w) = &m.where_clause {
                    walk_expr(w, out);
                }
            }
            // Writes: these produce the entities rather than requiring them.
            // Their expressions can still host a pattern comprehension.
            Clause::Create(c) => walk_pattern_exprs(&c.pattern, out),
            Clause::Merge(m) => {
                walk_pattern_exprs(&m.pattern, out);
                for item in m.on_match.iter().chain(&m.on_create) {
                    walk_set_item(item, out);
                }
            }
            Clause::With(w) => {
                walk_return_items(&w.items, out);
                for e in w.order_by.iter().flatten() {
                    walk_expr(&e.expr, out);
                }
                for e in [&w.skip, &w.limit, &w.where_clause].into_iter().flatten() {
                    walk_expr(e, out);
                }
            }
            Clause::Return(r) => {
                walk_return_items(&r.items, out);
                for e in r.order_by.iter().flatten() {
                    walk_expr(&e.expr, out);
                }
                for e in [&r.skip, &r.limit].into_iter().flatten() {
                    walk_expr(e, out);
                }
            }
            Clause::WithRecursive(wr) => {
                walk_query(&wr.query, out);
                walk_return_items(&wr.items, out);
            }
            Clause::Unwind(u) => walk_expr(&u.expr, out),
            // The list expression is evaluated per input row, so it carries
            // requirements exactly as `UNWIND`'s does; the body clauses are
            // writes and are walked for the same reason `CREATE`/`SET` are.
            Clause::Foreach(f) => {
                walk_expr(&f.expr, out);
                walk_statement(
                    &Statement {
                        clauses: f.body.clone(),
                    },
                    out,
                );
            }
            Clause::Delete(d) => {
                for e in &d.items {
                    walk_expr(e, out);
                }
            }
            Clause::Set(s) => {
                for item in &s.items {
                    walk_set_item(item, out);
                }
            }
            Clause::Remove(_) => {}
            Clause::Call(c) => {
                if let Some(w) = &c.where_clause {
                    walk_expr(w, out);
                }
            }
        }
    }
}

fn walk_return_items(items: &[ReturnItem], out: &mut Vec<Requirement>) {
    for item in items {
        if let ReturnItem::Expr { expr, .. } = item {
            walk_expr(expr, out);
        }
    }
}

fn walk_set_item(item: &crate::ast::SetItem, out: &mut Vec<Requirement>) {
    use crate::ast::SetItem;
    match item {
        SetItem::Property { expr, value } => {
            walk_expr(expr, out);
            walk_expr(value, out);
        }
        SetItem::Variable { value, .. } | SetItem::VariablePlus { value, .. } => {
            walk_expr(value, out);
        }
        SetItem::Labels { .. } => {}
    }
}

/// Collect requirements from every pattern comprehension inside `e`.
///
/// `Expr::for_each_child` already declines to descend into `EXISTS` /
/// `COUNT { }` / `COLLECT { }`, which is exactly the scoping wanted here — those
/// are routinely written to assert absence. It *does* reach a pattern
/// comprehension's `WHERE` and projection, so a comprehension nested inside
/// another one is still found; its own `pattern` is the one field it does not
/// hand out, which is why that is taken first.
fn walk_expr(e: &Expr, out: &mut Vec<Requirement>) {
    if let Expr::PatternComprehension { pattern, .. } = e {
        walk_pattern(pattern, out);
    }
    e.for_each_child(&mut |child| walk_expr(child, out));
}

/// Requirements from the pattern itself.
fn walk_pattern(p: &Pattern, out: &mut Vec<Requirement>) {
    for path in &p.paths {
        walk_elements(&path.elements, out);
    }
}

fn walk_elements(elements: &[PatternElement], out: &mut Vec<Requirement>) {
    for el in elements {
        match el {
            PatternElement::Node(n) => push(&n.labels, true, out),
            PatternElement::Relationship(r) => {
                // `[:T*0..]` is satisfied by a zero-length path, so no edge of
                // type T need exist for the pattern to match.
                let optional_hop = r.range.as_ref().is_some_and(|g| g.min == Some(0));
                if !optional_hop {
                    push(&r.types, false, out);
                }
            }
            PatternElement::Parenthesized { pattern, range } => {
                if range.as_ref().is_some_and(|g| g.min == Some(0)) {
                    continue;
                }
                walk_elements(&pattern.elements, out);
            }
        }
    }
}

/// Expressions hosted by a write pattern, without requiring the pattern itself.
fn walk_pattern_exprs(p: &Pattern, out: &mut Vec<Requirement>) {
    for path in &p.paths {
        for el in &path.elements {
            let props = match el {
                PatternElement::Node(n) => n.properties.as_ref(),
                PatternElement::Relationship(r) => r.properties.as_ref(),
                PatternElement::Parenthesized { pattern, .. } => {
                    walk_pattern_exprs(
                        &Pattern {
                            paths: vec![(**pattern).clone()],
                        },
                        out,
                    );
                    None
                }
            };
            if let Some(e) = props {
                walk_expr(e, out);
            }
        }
    }
}

fn push(labels: &LabelExpr, is_node: bool, out: &mut Vec<Requirement>) {
    match labels {
        LabelExpr::Empty => {}
        // `:A:B` — the entity carries every listed label, so each is required.
        LabelExpr::Conjunction(names) => {
            for n in names {
                out.push(if is_node {
                    Requirement::Label(n.clone())
                } else {
                    Requirement::EdgeType(n.clone())
                });
            }
        }
        LabelExpr::Disjunction(names) => {
            // A single-member disjunction is the same statement as `:A`; keep it
            // in the simple form so diagnostics read naturally.
            if let [only] = names.as_slice() {
                out.push(if is_node {
                    Requirement::Label(only.clone())
                } else {
                    Requirement::EdgeType(only.clone())
                });
            } else {
                let mut v = names.clone();
                v.sort();
                out.push(if is_node {
                    Requirement::AnyLabel(v)
                } else {
                    Requirement::AnyEdgeType(v)
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Requirements as readable strings, sorted so the expectations below do
    /// not silently depend on the order `Requirement`'s variants happen to be
    /// declared in.
    fn names(q: &str) -> Vec<String> {
        let mut v: Vec<String> = of(q)
            .expect("query must parse")
            .iter()
            .map(|r| match r {
                Requirement::Label(n) => format!("label:{n}"),
                Requirement::EdgeType(n) => format!("edge:{n}"),
                Requirement::AnyLabel(v) => format!("any-label:{}", v.join("|")),
                Requirement::AnyEdgeType(v) => format!("any-edge:{}", v.join("|")),
            })
            .collect();
        v.sort();
        v
    }

    #[test]
    fn a_match_pattern_requires_its_labels_and_edge_types() {
        assert_eq!(
            names("MATCH (a:Person)-[:KNOWS]->(b:Company) RETURN a"),
            ["edge:KNOWS", "label:Company", "label:Person"]
        );
    }

    /// The motivating case. IC14's weight term is a pattern comprehension, and
    /// its entity types are the ones the original fixture omitted — so an
    /// extractor that only walked `MATCH` clauses would report exactly the
    /// entities that fixture *did* have and miss the defect entirely.
    #[test]
    fn a_pattern_comprehension_inside_an_expression_is_reached() {
        const IC14_WEIGHT: &str = "
MATCH path = allShortestPaths((person1:Person { id: 1 })-[:KNOWS*0..]-(person2:Person { id: 2 }))
WITH path, relationships(path) as rels_in_path
WITH [r in rels_in_path |
        reduce(w=0.0, v in [
            (a:Person)<-[:HAS_CREATOR]-(:Comment)-[:REPLY_OF]->(:Post)-[:HAS_CREATOR]->(b:Person)
            WHERE a.id = startNode(r).id and b.id = endNode(r).id
            | 1.0] | w+v)
     ] as weight1
RETURN weight1";
        let got = names(IC14_WEIGHT);
        for want in [
            "label:Comment",
            "label:Post",
            "edge:HAS_CREATOR",
            "edge:REPLY_OF",
        ] {
            assert!(
                got.contains(&want.to_string()),
                "{want} missing from {got:?}"
            );
        }
    }

    /// `[:KNOWS*0..]` matches a zero-length path, so no `KNOWS` edge need exist.
    /// Requiring one would fail the very IC14 fixture this module exists to
    /// vet — its `allShortestPaths` hop is written exactly that way.
    #[test]
    fn a_zero_length_variable_hop_requires_no_edge() {
        assert_eq!(
            names("MATCH (a:Person)-[:KNOWS*0..]-(b:Person) RETURN a"),
            ["label:Person"]
        );
        // A hop that must traverse at least once does require the edge.
        assert_eq!(
            names("MATCH (a:Person)-[:KNOWS*1..3]-(b:Person) RETURN a"),
            ["edge:KNOWS", "label:Person"]
        );
    }

    /// A disjunction is satisfied by one arm; requiring both would fail a
    /// fixture that legitimately populates only one.
    #[test]
    fn a_disjunction_requires_only_one_of_its_members() {
        assert_eq!(
            names("MATCH (a:Person|Company)-[:KNOWS|LIKES]->(b) RETURN a"),
            ["any-edge:KNOWS|LIKES", "any-label:Company|Person"]
        );
    }

    /// `:A:B` is a conjunction — the node carries both, so both are required.
    #[test]
    fn a_conjunction_requires_every_member() {
        assert_eq!(
            names("MATCH (a:Person:Employee) RETURN a"),
            ["label:Employee", "label:Person"]
        );
    }

    /// Writes populate the fixture rather than depending on it.
    #[test]
    fn create_and_merge_patterns_require_nothing() {
        assert!(names("CREATE (a:Person)-[:KNOWS]->(b:Person)").is_empty());
        assert!(names("MERGE (a:Person)-[:KNOWS]->(b:Person)").is_empty());
    }

    /// `OPTIONAL MATCH` is contractually allowed to match nothing, so requiring
    /// its entities would fail tests written to exercise the NULL-row path.
    #[test]
    fn an_optional_match_requires_nothing() {
        assert_eq!(
            names("MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b:Company) RETURN a, b"),
            ["label:Person"]
        );
    }

    /// `EXISTS { }` is routinely written to assert absence.
    #[test]
    fn an_exists_subquery_requires_nothing() {
        assert_eq!(
            names("MATCH (a:Person) WHERE NOT EXISTS { MATCH (a)-[:KNOWS]->(:Company) } RETURN a"),
            ["label:Person"]
        );
    }

    #[test]
    fn results_are_deduplicated_and_stable() {
        let q = "MATCH (a:Person)-[:KNOWS]->(b:Person) MATCH (c:Person) RETURN a";
        assert_eq!(names(q), ["edge:KNOWS", "label:Person"]);
        assert_eq!(of(q).unwrap(), of(q).unwrap());
    }

    #[test]
    fn an_unparseable_query_is_an_error_not_an_empty_requirement_set() {
        assert!(of("MATCH (a:Person RETURN").is_err());
    }
}
