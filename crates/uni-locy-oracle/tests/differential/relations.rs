// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Metamorphic relations that change only a program's text (issue #300 T4).
//!
//! Each transform is applied to the program's [`SurfaceProgram`], so it is
//! exact rather than a regex over text:
//!
//! * [`permute_conjuncts`]: a random permutation of every clause's `WHERE`
//!   conjuncts — a clause is a conjunction, so its answer cannot depend on
//!   the order;
//! * [`alpha_rename`]: a consistent renaming of every body variable, rule
//!   and rule output column — the oracle answer is mapped through the
//!   renaming;
//! * [`rename_into_collision`]: a variable renamed to a value column that
//!   exactly one *other* referenced rule of the clause exposes; a variable
//!   shadows an exposed column, so the answer cannot change. This is the
//!   relation that hunts name capture; a uniformly fresh renaming would hide
//!   it;
//! * [`interleave_yield`]: value columns moved among the KEY columns of each
//!   `YIELD`.
//!
//! All four run together in one second evaluation, whose `QUERY`s must
//! equal the oracle. On a divergence each transform is re-run alone to
//! attribute it. [`explain_agrees`] checks that `EXPLAIN RULE` derives
//! exactly the oracle's facts for every rule with two or more positive
//! references.

// Rust guideline compliant

use std::collections::{BTreeSet, HashMap};

use uni_db::Uni;
use uni_db::locy::CommandResult;
use uni_locy_oracle::generator::exposed_columns;
use uni_locy_oracle::ir::{Feature, Generated, OracleRule, Tuple};
use uni_locy_oracle::surface::{Orders, Renaming};

use super::random::{CaseReport, Check, Divergence, decode, judge, splitmix};

/// A small deterministic generator for the transforms' choices.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (splitmix(&mut self.0) % n as u64) as usize
        }
    }
}

/// A random permutation of every clause's conjuncts (some may come out the
/// identity; [`Orders::changes_any`] tells).
pub(crate) fn permute_conjuncts(g: &Generated, seed: u64) -> Orders {
    let mut rng = Rng(seed);
    let mut orders = Orders::canonical();
    for (i, c) in g.surface.clauses() {
        if c.conjuncts.len() < 2 {
            continue;
        }
        let mut p: Vec<usize> = (0..c.conjuncts.len()).collect();
        for k in (1..p.len()).rev() {
            let j = rng.below(k + 1);
            p.swap(k, j);
        }
        orders.0.insert(i, p);
    }
    orders
}

/// A consistent renaming of every name: each body variable and output
/// column name `x` becomes `zz_x` everywhere, each rule `r` becomes `zr_r`.
///
/// The map is one bijection on names, not a fresh name per occurrence, so a
/// variable that shares its name with a referenced rule's column keeps
/// sharing it: the renaming preserves every accidental collision a capture
/// bug needs, and changes only spellings.
pub(crate) fn alpha_rename(g: &Generated) -> Renaming {
    let mut renaming = Renaming::identity();
    let name = |x: &str| format!("zz_{x}");
    for (i, c) in g.surface.clauses() {
        let vars = c.vars().into_iter().map(|v| {
            let to = name(&v);
            (v, to)
        });
        renaming.vars.insert(i, vars.collect());
    }
    for rel in g.key_schema.keys() {
        renaming.rules.insert(rel.clone(), format!("zr_{rel}"));
        for col in &g.key_schema[rel] {
            renaming.cols.insert((rel.clone(), col.clone()), name(col));
        }
    }
    renaming
}

/// One collision rename: in statement `stmt`, variable `var` takes the name
/// of `rule`'s exposed value column `column`.
#[derive(Debug, Clone)]
pub(crate) struct Collision {
    stmt: usize,
    var: String,
    rule: String,
    column: String,
}

/// For each clause that has one, a variable renamed to a value column that
/// exactly one other positive reference of the clause exposes, where the new
/// name occurs nowhere else in the clause.
pub(crate) fn rename_into_collision(g: &Generated, seed: u64) -> Vec<Collision> {
    let mut rng = Rng(seed);
    let rules: HashMap<&str, &OracleRule> = g
        .oracle_rules
        .strata
        .iter()
        .flatten()
        .map(|r| (r.name.as_str(), r))
        .collect();
    let mut out = Vec::new();
    for (i, c) in g.surface.clauses() {
        let vars = c.vars();
        let spelled: BTreeSet<String> = c.cols().into_iter().map(|(_, col)| col).collect();
        // Value column -> the positive references exposing it.
        let mut exposers: HashMap<&str, Vec<usize>> = HashMap::new();
        let refs: Vec<_> = c.refs().filter(|(_, r)| !r.negated).collect();
        for (k, (_, r)) in refs.iter().enumerate() {
            let Some(rule) = rules.get(r.rule.as_str()) else {
                continue;
            };
            for col in exposed_columns(r, rule) {
                exposers.entry(col).or_default().push(k);
            }
        }
        let mut candidates = Vec::new();
        for (col, by) in &exposers {
            let [k] = by.as_slice() else { continue };
            if vars.contains(*col) || spelled.contains(*col) {
                continue;
            }
            let exposer = refs[*k].1;
            for v in &vars {
                let in_exposer = exposer
                    .subjects
                    .iter()
                    .chain(&exposer.target)
                    .any(|s| s == v);
                if !in_exposer {
                    candidates.push(Collision {
                        stmt: i,
                        var: v.clone(),
                        rule: exposer.rule.clone(),
                        column: (*col).to_string(),
                    });
                }
            }
        }
        candidates.sort_by(|a, b| (&a.var, &a.column).cmp(&(&b.var, &b.column)));
        if !candidates.is_empty() {
            let pick = rng.below(candidates.len());
            out.push(candidates.swap_remove(pick));
        }
    }
    out
}

/// Moves a rule line's value columns among its KEY columns, keeping the KEYs
/// in their order and the values in theirs: after the first KEY when there
/// are two or more, otherwise before the KEY. Every clause of a rule has the
/// same YIELD shape, so every clause is reordered alike.
///
/// A reference binds a rule's KEY columns first, in YIELD order, then its
/// value columns (`uni_locy::is_binding`), and values are read by name, so
/// moving value columns among the KEYs changes no answer, and the oracle,
/// which knows only the KEY-first form, still applies.
pub(crate) fn interleave_yield(line: &str) -> String {
    let Some(at) = line.find(" YIELD ") else {
        return line.to_string();
    };
    let (head, items) = line.split_at(at + " YIELD ".len());
    let items: Vec<&str> = items.split(", ").collect();
    let (keys, values): (Vec<&str>, Vec<&str>) = items.iter().partition(|i| i.starts_with("KEY "));
    if values.is_empty() {
        return line.to_string();
    }
    let reordered: Vec<&str> = if keys.len() >= 2 {
        keys[..1]
            .iter()
            .chain(&values)
            .chain(&keys[1..])
            .copied()
            .collect()
    } else {
        values.iter().chain(&keys).copied().collect()
    };
    format!("{head}{}", reordered.join(", "))
}

#[test]
fn interleave_yield_keeps_key_and_value_order() {
    assert_eq!(
        interleave_yield("CREATE RULE r AS MATCH (a) YIELD KEY a, KEY b, q"),
        "CREATE RULE r AS MATCH (a) YIELD KEY a, q, KEY b"
    );
    assert_eq!(
        interleave_yield("CREATE RULE f AS MATCH (a) FOLD n = COUNT(*) YIELD KEY a, n, s"),
        "CREATE RULE f AS MATCH (a) FOLD n = COUNT(*) YIELD n, s, KEY a"
    );
    assert_eq!(
        interleave_yield("CREATE RULE u AS MATCH (a) YIELD KEY a, KEY b"),
        "CREATE RULE u AS MATCH (a) YIELD KEY a, KEY b"
    );
}

/// One combination of transforms.
#[derive(Debug, Clone, Default)]
struct Transform {
    renaming: Renaming,
    orders: Orders,
    interleave: bool,
}

/// `alpha` with the collisions applied on top: a collided variable takes the
/// (renamed) name of the column it collides with, so it stays a collision.
fn with_collisions(alpha: &Renaming, collisions: &[Collision]) -> Renaming {
    let mut out = alpha.clone();
    for c in collisions {
        let name = alpha.col(&c.rule, &c.column).to_string();
        out.vars
            .entry(c.stmt)
            .or_default()
            .insert(c.var.clone(), name);
    }
    out
}

/// Evaluates the transformed program restricted to `keep`, with an
/// `EXPLAIN RULE` per relation before a `QUERY` per relation, and returns
/// each relation whose `QUERY` differs from the oracle — or, when the
/// program fails, the error.
///
/// Each `EXPLAIN` matches no fact, so it re-derives its rule through the
/// in-memory evaluator; that evaluator's rows once landed in the store every
/// later `QUERY` reads. Holding the `QUERY`s to the oracle, rather than
/// `result.derived`, also checks that a `QUERY` answers what the fixpoint
/// derived.
async fn run_transformed(
    db: &Uni,
    g: &Generated,
    oracle: &HashMap<String, Vec<Tuple>>,
    t: &Transform,
    keep: &BTreeSet<String>,
) -> Result<Vec<Divergence>, String> {
    let rels: Vec<&String> = keep
        .iter()
        .filter(|r| g.key_schema.contains_key(*r))
        .collect();
    let text = g
        .surface
        .render_where(&t.renaming, &t.orders, &|r| keep.contains(r));
    let mut program: Vec<String> = if t.interleave {
        text.lines().map(interleave_yield).collect()
    } else {
        text.lines().map(str::to_string).collect()
    };
    let engine_cols = |rel: &str| -> Vec<String> {
        g.key_schema[rel]
            .iter()
            .map(|c| t.renaming.col(rel, c).to_string())
            .collect()
    };
    for rel in &rels {
        program.push(format!(
            "EXPLAIN RULE {} WHERE {} IS NULL",
            t.renaming.rule(rel),
            engine_cols(rel)[0]
        ));
    }
    for rel in &rels {
        program.push(format!("QUERY {}", t.renaming.rule(rel)));
    }
    let result = db
        .session()
        .locy(&program.join("\n"))
        .await
        .map_err(|e| format!("ERR {e:#}"))?;
    let queries: Vec<_> = result
        .command_results
        .iter()
        .filter_map(|c| match c {
            CommandResult::Query(rows) => Some(rows),
            _ => None,
        })
        .collect();
    assert_eq!(queries.len(), rels.len(), "one QUERY result per relation");
    Ok(rels
        .iter()
        .zip(queries)
        .filter_map(|(rel, rows)| {
            judge(g, oracle, rel, &engine_cols(rel), rows, Check::Transformed)
        })
        .collect())
}

/// The second evaluation: every transform at once, `QUERY` after `EXPLAIN`,
/// against the oracle mapped through the renaming. Divergences are
/// attributed by re-running the untransformed program, then each transform
/// alone. `failing` are the relations the untransformed program fails on.
pub(crate) async fn transformed_divergences(
    db: &Uni,
    g: &Generated,
    oracle: &HashMap<String, Vec<Tuple>>,
    failing: &BTreeSet<String>,
    seed: u64,
    report: &mut CaseReport,
) {
    let identity_diverged: BTreeSet<String> =
        report.divergences.iter().map(|d| d.rel.clone()).collect();
    let mut state = seed;
    let orders = permute_conjuncts(g, splitmix(&mut state));
    let alpha = alpha_rename(g);
    let collisions = rename_into_collision(g, splitmix(&mut state));
    report.permuted = orders.changes_any();
    report.collided = !collisions.is_empty();
    let all = Transform {
        renaming: with_collisions(&alpha, &collisions),
        orders: orders.clone(),
        interleave: true,
    };
    let singles = [
        (
            "permute_conjuncts",
            Transform {
                orders,
                ..Transform::default()
            },
        ),
        (
            "alpha_rename",
            Transform {
                renaming: alpha,
                ..Transform::default()
            },
        ),
        (
            "rename_into_collision",
            Transform {
                renaming: with_collisions(&Renaming::identity(), &collisions),
                ..Transform::default()
            },
        ),
        (
            "interleave_yield",
            Transform {
                interleave: true,
                ..Transform::default()
            },
        ),
    ];
    // The relations the untransformed program evaluates are judged together;
    // each one it fails on (`failing`, with its dependents) alone, so a
    // failure cannot hide the others.
    let healthy: BTreeSet<String> = g
        .key_schema
        .keys()
        .filter(|rel| g.dependency_closure(rel).is_disjoint(failing))
        .cloned()
        .collect();
    let mut found: Vec<(Divergence, BTreeSet<String>)> = Vec::new();
    let mut whole_failed = None;
    let mut alone: Vec<&String> = g
        .key_schema
        .keys()
        .filter(|r| !healthy.contains(*r))
        .collect();
    // A collision in a relation judged without an engine error.
    let collided_in = |rels: &BTreeSet<String>| {
        collisions
            .iter()
            .any(|c| rels.contains(g.surface.stmts[c.stmt].rule()))
    };
    match run_transformed(db, g, oracle, &all, &healthy).await {
        Ok(ds) => {
            report.collided_judged = collided_in(&healthy);
            found.extend(ds.into_iter().map(|d| (d, healthy.clone())));
        }
        Err(whole) => {
            whole_failed = Some(whole);
            alone.extend(healthy.iter());
            alone.sort();
        }
    }
    let mut healthy_found = false;
    for rel in alone {
        let keep = g.dependency_closure(rel);
        let mut ds = match run_transformed(db, g, oracle, &all, &keep).await {
            Ok(ds) => {
                report.collided_judged |= collided_in(&BTreeSet::from([rel.clone()]));
                ds
            }
            Err(e) => vec![Divergence::new(Check::Transformed, rel, e)],
        };
        ds.retain(|d| &d.rel == rel);
        healthy_found |= healthy.contains(rel) && !ds.is_empty();
        found.extend(ds.into_iter().map(|d| (d, keep.clone())));
    }
    if let Some(whole) = whole_failed
        && !healthy_found
    {
        report.divergences.push(Divergence::new(
            Check::Transformed,
            "*",
            format!("the transformed program fails, no relation alone does: {whole}"),
        ));
    }
    for (mut d, keep) in found {
        // A relation the untransformed program already gets wrong says
        // nothing about any transform.
        let baseline = match run_transformed(db, g, oracle, &Transform::default(), &keep).await {
            Ok(ds) => ds.iter().any(|e| e.rel == d.rel),
            Err(_) => true,
        };
        if baseline {
            // Already reported by the untransformed judgment, unless that
            // one passed (the `QUERY`-after-`EXPLAIN` path differs).
            if !identity_diverged.contains(&d.rel) {
                d.attributed.push("untransformed");
                report.divergences.push(d);
            }
            continue;
        }
        for (name, t) in &singles {
            let reproduces = match run_transformed(db, g, oracle, t, &keep).await {
                Ok(ds) => ds.iter().any(|e| e.rel == d.rel),
                Err(_) => true,
            };
            if reproduces {
                d.attributed.push(name);
            }
        }
        report.divergences.push(d);
    }
}

/// `EXPLAIN RULE r` for every rule with two or more positive references:
/// the facts with a root derivation (the root's children) must equal the
/// oracle's facts, by KEY.
pub(crate) async fn explain_agrees(
    db: &Uni,
    g: &Generated,
    oracle: &HashMap<String, Vec<Tuple>>,
    report: &mut CaseReport,
) {
    let mut rels: Vec<&String> = g
        .rel_features
        .iter()
        .filter(|(rel, f)| f.contains(&Feature::MultiRef) && g.key_schema.contains_key(*rel))
        .map(|(rel, _)| rel)
        .collect();
    rels.sort();
    let key_counts: HashMap<&str, usize> = g
        .oracle_rules
        .strata
        .iter()
        .flatten()
        .map(|r| (r.name.as_str(), r.key_count))
        .collect();
    for rel in rels {
        let program = format!("{}\nEXPLAIN RULE {rel}", g.program_text);
        let node = match db.session().locy(&program).await {
            Ok(result) => result.command_results.iter().find_map(|c| match c {
                CommandResult::Explain(node) => Some(node.clone()),
                _ => None,
            }),
            Err(e) => {
                report.divergences.push(Divergence::new(
                    Check::Explain,
                    rel,
                    format!("EXPLAIN ERR {e:#}"),
                ));
                continue;
            }
        };
        let Some(node) = node else {
            report.divergences.push(Divergence::new(
                Check::Explain,
                rel,
                "no EXPLAIN result".into(),
            ));
            continue;
        };
        let keys = key_counts[rel.as_str()];
        let cols = &g.key_schema[rel][..keys];
        let kinds = &g.col_kinds[rel][..keys];
        let mut problems = Vec::new();
        // Each child of the root is a fact with its derivation.
        let explained: BTreeSet<Tuple> = node
            .children
            .iter()
            .map(|fact| {
                cols.iter()
                    .zip(kinds)
                    .map(
                        |(c, k)| match fact.bindings.get(c).map(|v| decode(*k, v, &g.strings)) {
                            Some(Ok(v)) => v,
                            other => {
                                problems.push(format!("{c}: {other:?}"));
                                i64::MIN
                            }
                        },
                    )
                    .collect()
            })
            .collect();
        let want: BTreeSet<Tuple> = oracle
            .get(rel.as_str())
            .into_iter()
            .flatten()
            .map(|t| t[..keys].to_vec())
            .collect();
        if explained != want || !problems.is_empty() {
            report.divergences.push(Divergence::new(
                Check::Explain,
                rel,
                format!("EXPLAIN derives {explained:?}\n      oracle {want:?} {problems:?}"),
            ));
        }
    }
}
