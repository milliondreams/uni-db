// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Answer-preserving rewrites of a generated query's text.
//!
//! Two rewrites a correct engine must not notice:
//!
//! - [`permute_conjuncts`] reorders the top-level `AND` conjuncts of every
//!   `WHERE`. Cypher's `AND` is commutative under three-valued logic, so the
//!   bag of rows cannot change.
//! - [`alpha_rename`] renames variables (pattern variables, `Property` bases,
//!   projection aliases, and every read of them) through an injective map. The
//!   rows cannot change; only the column labels move with the aliases.
//!
//! They guard the Cypher side of the binding class behind issue #300: a name
//! resolved by schema lookup instead of by its binder captures whatever column
//! happens to carry the name. The renamings in [`RENAMINGS`] therefore aim at
//! the collisions such a lookup would trip over: swapping `a` and `b`, naming a
//! variable after a property key (`MATCH (age:Person) WHERE age.age > 30`), and
//! naming a projection alias after a property key the `ORDER BY` also reads
//! (`RETURN a.age AS name ORDER BY a.name`).
//!
//! Both rewrites keep the renderer's round-trip contract: for every generated
//! query `q`, `parse(render(t(q))) == t(q)` after [`normalize`](super::render::normalize).

// Rust guideline compliant

use std::collections::{BTreeMap, BTreeSet};

use uni_cypher::ast::{BinaryOp, Clause, Expr, PatternElement, Query, ReturnItem, SortItem};

/// Renamings the binding guard draws from, each injective.
///
/// Every image is either a renamed-away name (the swaps) or a name no
/// generated query uses as a variable or alias, so no renaming captures.
pub const RENAMINGS: &[&[(&str, &str)]] = &[
    // Swap the two pattern variables.
    &[("a", "b"), ("b", "a")],
    // Fresh names everywhere.
    &[
        ("a", "x"),
        ("b", "y"),
        ("c0", "d0"),
        ("c1", "d1"),
        ("c2", "d2"),
        ("keep", "kept"),
    ],
    // Aliases named after property keys, including the `ORDER BY` key `name`.
    &[
        ("c0", "name"),
        ("c1", "age"),
        ("c2", "city"),
        ("keep", "score"),
    ],
    // Pattern variables named after property keys.
    &[("a", "age"), ("b", "name")],
    // The swap plus aliases named after the other variable's property keys.
    &[
        ("a", "b"),
        ("b", "a"),
        ("c0", "age"),
        ("c1", "name"),
        ("c2", "founded"),
        ("keep", "city"),
    ],
];

/// Reorders the top-level `AND` conjuncts of every `WHERE` by `perm`.
///
/// Each `WHERE` is flattened into its conjuncts `e₀ … eₙ₋₁` and rebuilt as a
/// left-deep `AND` chain in the order `perm` induces on `0..n`: the elements of
/// `perm` below `n`, in `perm`'s order, then any conjuncts past `perm`'s
/// length in their own order. One permutation of `0..k` (any `k`) therefore
/// reorders every `WHERE` of the query consistently. A `WHERE` with one
/// conjunct is left as it is.
///
/// # Panics
///
/// Panics if `perm` is not a permutation of `0..perm.len()` (a caller bug).
pub fn permute_conjuncts(q: &Query, perm: &[usize]) -> Query {
    let mut out = q.clone();
    for_each_where(&mut out, &mut |w| {
        let mut conjuncts = Vec::new();
        flatten_and(w, &mut conjuncts);
        if conjuncts.len() < 2 {
            return;
        }
        let order = induced_order(perm, conjuncts.len());
        let mut slots: Vec<Option<Expr>> = conjuncts.into_iter().map(Some).collect();
        let mut picked = order
            .into_iter()
            .map(|i| slots[i].take().expect("index once"));
        let first = picked.next().expect("at least two conjuncts");
        *w = picked.fold(first, |acc, e| Expr::BinaryOp {
            left: Box::new(acc),
            op: BinaryOp::And,
            right: Box::new(e),
        });
    });
    out
}

/// Whether [`permute_conjuncts`] with `perm` moves any conjunct of `q`.
///
/// Re-association alone (the rebuilt chain is left-deep) does not count.
pub fn permutes_any(q: &Query, perm: &[usize]) -> bool {
    let mut moved = false;
    let mut copy = q.clone();
    for_each_where(&mut copy, &mut |w| {
        let mut conjuncts = Vec::new();
        flatten_and(w, &mut conjuncts);
        let n = conjuncts.len();
        if n >= 2 && induced_order(perm, n) != (0..n).collect::<Vec<_>>() {
            moved = true;
        }
    });
    moved
}

/// Renames every variable and alias of `q` through `map`.
///
/// Covers pattern variables (node, relationship and path), the base of every
/// `Property` access, every `Variable` read, and the aliases of `WITH` and
/// `RETURN` items. Property keys, labels, relationship types, map keys and
/// function names are not variables and are left alone.
///
/// # Panics
///
/// Panics if `map` is not injective on the names `q` uses, or if it renames a
/// name onto one `q` already uses and `map` does not rename away: either would
/// capture, which is a generator bug, not an engine behavior.
pub fn alpha_rename(q: &Query, map: &BTreeMap<String, String>) -> Query {
    let mut used = BTreeSet::new();
    let mut probe = q.clone();
    for_each_name(&mut probe, &mut |name| {
        used.insert(name.clone());
    });
    let mut images = BTreeSet::new();
    for from in used.iter().filter(|n| map.contains_key(*n)) {
        let to = &map[from];
        assert!(
            images.insert(to.clone()),
            "alpha_rename: two names map onto `{to}`"
        );
        assert!(
            !used.contains(to) || map.contains_key(to),
            "alpha_rename: `{from}` -> `{to}` captures an existing name"
        );
    }
    let mut out = q.clone();
    for_each_name(&mut out, &mut |name| {
        if let Some(to) = map.get(name.as_str()) {
            name.clone_from(to);
        }
    });
    out
}

/// The renaming [`RENAMINGS`]`[index]` as an owned map.
///
/// # Panics
///
/// Panics if `index` is out of range.
pub fn renaming(index: usize) -> BTreeMap<String, String> {
    RENAMINGS[index]
        .iter()
        .map(|(from, to)| ((*from).to_owned(), (*to).to_owned()))
        .collect()
}

/// The elements of `perm` below `n`, then `perm.len()..n` in order.
fn induced_order(perm: &[usize], n: usize) -> Vec<usize> {
    let mut order: Vec<usize> = perm.iter().copied().filter(|&i| i < n).collect();
    order.extend(perm.len()..n);
    let mut sorted = order.clone();
    sorted.sort_unstable();
    assert_eq!(
        sorted,
        (0..n).collect::<Vec<_>>(),
        "permutation {perm:?} does not cover 0..{n}"
    );
    order
}

fn flatten_and(e: &Expr, out: &mut Vec<Expr>) {
    match e {
        Expr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
        } => {
            flatten_and(left, out);
            flatten_and(right, out);
        }
        other => out.push(other.clone()),
    }
}

/// Calls `f` on the `WHERE` of every `MATCH` and `WITH`.
fn for_each_where(q: &mut Query, f: &mut dyn FnMut(&mut Expr)) {
    match q {
        Query::Single(stmt) => {
            for clause in &mut stmt.clauses {
                let w = match clause {
                    Clause::Match(m) => m.where_clause.as_mut(),
                    Clause::With(w) => w.where_clause.as_mut(),
                    _ => None,
                };
                if let Some(w) = w {
                    f(w);
                }
            }
        }
        Query::Explain(inner) => for_each_where(inner, f),
        other => panic!("permute_conjuncts: outside the generated subgrammar: {other:?}"),
    }
}

/// Calls `f` on every variable name and alias of `q`, mutably.
fn for_each_name(q: &mut Query, f: &mut dyn FnMut(&mut String)) {
    match q {
        Query::Single(stmt) => {
            for clause in &mut stmt.clauses {
                match clause {
                    Clause::Match(m) => {
                        for path in &mut m.pattern.paths {
                            if let Some(v) = &mut path.variable {
                                f(v);
                            }
                            for element in &mut path.elements {
                                let (variable, properties, inline) = match element {
                                    PatternElement::Node(n) => {
                                        (&mut n.variable, &mut n.properties, &mut n.where_clause)
                                    }
                                    PatternElement::Relationship(r) => {
                                        (&mut r.variable, &mut r.properties, &mut r.where_clause)
                                    }
                                    other => panic!(
                                        "alpha_rename: outside the generated subgrammar: {other:?}"
                                    ),
                                };
                                if let Some(v) = variable {
                                    f(v);
                                }
                                for e in properties.iter_mut().chain(inline.iter_mut()) {
                                    expr_names(e, f);
                                }
                            }
                        }
                        if let Some(w) = &mut m.where_clause {
                            expr_names(w, f);
                        }
                    }
                    Clause::With(w) => {
                        items_names(&mut w.items, f);
                        tail_names(w.order_by.as_mut(), w.skip.as_mut(), w.limit.as_mut(), f);
                        if let Some(p) = &mut w.where_clause {
                            expr_names(p, f);
                        }
                    }
                    Clause::Return(r) => {
                        items_names(&mut r.items, f);
                        tail_names(r.order_by.as_mut(), r.skip.as_mut(), r.limit.as_mut(), f);
                    }
                    other => panic!("alpha_rename: outside the generated subgrammar: {other:?}"),
                }
            }
        }
        Query::Explain(inner) => for_each_name(inner, f),
        other => panic!("alpha_rename: outside the generated subgrammar: {other:?}"),
    }
}

fn items_names(items: &mut [ReturnItem], f: &mut dyn FnMut(&mut String)) {
    for item in items {
        if let ReturnItem::Expr { expr, alias, .. } = item {
            expr_names(expr, f);
            if let Some(a) = alias {
                f(a);
            }
        }
    }
}

fn tail_names(
    order_by: Option<&mut Vec<SortItem>>,
    skip: Option<&mut Expr>,
    limit: Option<&mut Expr>,
    f: &mut dyn FnMut(&mut String),
) {
    for item in order_by.into_iter().flatten() {
        expr_names(&mut item.expr, f);
    }
    for e in skip.into_iter().chain(limit) {
        expr_names(e, f);
    }
}

fn expr_names(e: &mut Expr, f: &mut dyn FnMut(&mut String)) {
    match e {
        Expr::Literal(_) | Expr::Parameter(_) | Expr::Wildcard => {}
        Expr::Variable(v) => f(v),
        Expr::Property(base, _) => expr_names(base, f),
        Expr::List(items) => items.iter_mut().for_each(|i| expr_names(i, f)),
        Expr::Map(entries) => entries.iter_mut().for_each(|(_, v)| expr_names(v, f)),
        Expr::FunctionCall { args, .. } => args.iter_mut().for_each(|a| expr_names(a, f)),
        Expr::BinaryOp { left, right, .. }
        | Expr::In {
            expr: left,
            list: right,
        } => {
            expr_names(left, f);
            expr_names(right, f);
        }
        Expr::UnaryOp { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            expr_names(expr, f);
        }
        Expr::Case {
            expr,
            when_then,
            else_expr,
        } => {
            for e in expr.iter_mut().chain(else_expr.iter_mut()) {
                expr_names(e, f);
            }
            for (when, then) in when_then {
                expr_names(when, f);
                expr_names(then, f);
            }
        }
        other => panic!("alpha_rename: outside the generated subgrammar: {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::querygen::render::{normalize, render};
    use crate::querygen::{Partition, arb_case, arb_case_pushdown};
    use proptest::prelude::*;
    use uni_cypher::parse;

    fn roundtrips(q: &Query) -> Result<(), TestCaseError> {
        let rendered = render(q);
        let mut got = parse(&rendered)
            .map_err(|e| TestCaseError::fail(format!("parse failed for `{rendered}`: {e:?}")))?;
        let mut want = q.clone();
        normalize(&mut want);
        normalize(&mut got);
        prop_assert_eq!(want, got, "round-trip mismatch: `{}`", rendered);
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

        /// Both rewrites, alone and composed, keep `parse(render(t(q))) == t(q)`.
        #[test]
        fn transforms_roundtrip(
            case in prop_oneof![arb_case(), arb_case_pushdown()],
            perm in Just((0..6).collect::<Vec<usize>>()).prop_shuffle(),
            which in 0..RENAMINGS.len(),
        ) {
            let map = renaming(which);
            let mut variants = vec![
                case.base_query(),
                case.partition_query(Partition::True),
                case.partition_query(Partition::Null),
                case.norec_optimized(),
                case.norec_unoptimized(),
                case.ordered_limited_query(3),
                case.conjunctive_query(),
            ];
            variants.extend(case.alias_ordered_limited_query(3));
            for q in variants {
                roundtrips(&permute_conjuncts(&q, &perm))?;
                roundtrips(&alpha_rename(&q, &map))?;
                roundtrips(&alpha_rename(&permute_conjuncts(&q, &perm), &map))?;
            }
        }
    }

    fn parsed(src: &str) -> Query {
        parse(src).expect("test query parses")
    }

    /// The flattened conjuncts come back in the permuted order, left-deep.
    #[test]
    fn permute_reorders_flattened_conjuncts() {
        let q = parsed(
            "MATCH (a:Person) WHERE (a.age > 1) AND ((a.age < 9) AND (a.name = 'x')) RETURN a",
        );
        assert_eq!(
            render(&permute_conjuncts(&q, &[2, 0, 1])),
            "MATCH (a:Person) WHERE (((a.name = \"x\") AND (a.age > 1)) AND (a.age < 9)) RETURN a"
        );
        assert!(permutes_any(&q, &[2, 0, 1]));
        assert!(!permutes_any(&q, &[0, 1, 2, 3]));
    }

    /// A swap renames reads, pattern variables and property bases together.
    #[test]
    fn rename_swaps_variables_everywhere() {
        let q = parsed(
            "MATCH (a:Person)-[:WORKS_AT]->(b:Company) WHERE a.age > 1 RETURN b.name AS c0, a AS c1",
        );
        assert_eq!(
            render(&alpha_rename(&q, &renaming(0))),
            "MATCH (b:Person)-[:WORKS_AT]->(a:Company) WHERE (b.age > 1) RETURN a.name AS c0, b AS c1"
        );
    }

    /// An alias named after the `ORDER BY` property key leaves the key alone.
    #[test]
    fn rename_alias_onto_a_property_key() {
        let q = parsed("MATCH (a:Person) RETURN a.age AS c0 ORDER BY c0 ASC, a.name ASC LIMIT 3");
        assert_eq!(
            render(&alpha_rename(&q, &renaming(2))),
            "MATCH (a:Person) RETURN a.age AS name ORDER BY name ASC, a.name ASC LIMIT 3"
        );
    }

    /// A renaming onto a name the query keeps is a capture, and refused.
    #[test]
    #[should_panic(expected = "captures")]
    fn rename_refuses_capture() {
        let q = parsed("MATCH (a:Person)-[:WORKS_AT]->(b:Company) RETURN a AS c0");
        let map = BTreeMap::from([("a".to_owned(), "b".to_owned())]);
        let _ = alpha_rename(&q, &map);
    }
}
