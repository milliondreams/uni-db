//! The naive Datalog fixpoint evaluator — the reference the engine must match.
//!
//! Evaluates an [`OracleProgram`] stratum by stratum,
//! re-deriving every clause to a least fixpoint with structural set semantics.
//! Unoptimized and obviously correct by construction; that is the whole point.
//!
//! The structural set semantics are what make this a useful oracle: a clause's
//! output feeds a [`HashSet`], so duplicate tuples emitted via different
//! intermediates collapse automatically — at *any* scale, with no threshold.
//! That is precisely the property the engine's optimized anti-join must preserve.

// Rust guideline compliant

use std::collections::{HashMap, HashSet};

use crate::ir::{Agg, OracleClause, OracleFold, OracleProgram, OracleRule, Tuple};

/// A relation: the set of derived (or base) fact tuples for one rule.
pub type Relation = HashSet<Tuple>;

/// Whether `fact` begins with `subj` (matching on the leading subject columns).
fn matches_subject(fact: &[i64], subj: &[i64]) -> bool {
    fact.len() >= subj.len() && fact[..subj.len()] == *subj
}

/// Evaluates a program to its least fixpoint, returning each rule's relation.
///
/// Strata are processed in order; each is driven to a fixpoint (re-derive every
/// clause until no new tuple appears) before the next begins. Because negated
/// references only target strictly-earlier strata, those relations are complete
/// by the time a later stratum reads them.
///
/// # Panics
/// Panics if a clause references a variable it does not bind (a malformed
/// program — a generator bug, not a runtime input).
#[must_use]
pub fn evaluate(program: &OracleProgram) -> HashMap<String, Relation> {
    let mut rels: HashMap<String, Relation> = HashMap::new();
    for stratum in &program.strata {
        // A FOLD rule reads only earlier strata, so it is evaluated once.
        for rule in stratum.iter().filter(|r| r.fold.is_some()) {
            let facts = eval_fold_rule(rule, &rels);
            rels.insert(rule.name.clone(), facts);
        }
        let stratum: Vec<&OracleRule> = stratum.iter().filter(|r| r.fold.is_none()).collect();
        loop {
            let mut changed = false;
            for rule in &stratum {
                for clause in &rule.clauses {
                    // Compute before mutably borrowing `rels` for the insert.
                    let derived = eval_clause(clause, &rels);
                    let entry = rels.entry(rule.name.clone()).or_default();
                    for tuple in derived {
                        if entry.insert(tuple) {
                            changed = true;
                        }
                    }
                }
            }
            if !changed {
                break;
            }
        }
    }
    rels
}

/// Evaluates a FOLD rule: the bag of its clauses' rows, grouped by key.
///
/// # Panics
/// Panics if the rule refers to itself (a recursive FOLD is out of scope) or a
/// row is shorter than the fold's columns (a generator bug).
fn eval_fold_rule(rule: &OracleRule, rels: &HashMap<String, Relation>) -> Relation {
    let fold: &OracleFold = rule.fold.as_ref().expect("a FOLD rule");
    assert!(
        rule.clauses.iter().all(|c| c
            .pos_refs
            .iter()
            .chain(&c.neg_refs)
            .all(|r| r.rule != rule.name)),
        "recursive FOLD is outside the oracle's scope: {}",
        rule.name
    );
    // The bag: `eval_clause` returns every binding's row, duplicates kept.
    let mut groups: HashMap<Tuple, Vec<Tuple>> = HashMap::new();
    for clause in &rule.clauses {
        for row in eval_clause(clause, rels) {
            groups
                .entry(row[..fold.key_count].to_vec())
                .or_default()
                .push(row);
        }
    }
    groups
        .into_iter()
        .map(|(key, rows)| {
            let mut fact = key;
            for a in &fold.aggs {
                let inputs = || rows.iter().map(|r| r[a.input.expect("an input column")]);
                fact.push(match a.agg {
                    Agg::CountStar => rows.len() as i64,
                    Agg::Count => inputs().count() as i64,
                    Agg::Sum => inputs().sum(),
                    Agg::Min => inputs().min().expect("a group has a row"),
                    Agg::Max => inputs().max().expect("a group has a row"),
                });
            }
            fact
        })
        .collect()
}

/// Evaluates one clause against the current relations: join, anti-join, project.
///
/// The pipeline is: seed bindings from the clause's base tuples, extend them
/// through each positive `IS` reference (binding the `TO` target), drop bindings
/// matched by each negated reference, then project to the `YIELD` columns.
///
/// # Panics
/// Panics if the clause binds a variable to a missing base column or projects a
/// variable that was never bound (malformed program).
fn eval_clause(clause: &OracleClause, rels: &HashMap<String, Relation>) -> Vec<Tuple> {
    // 1. Seed bindings: one per base tuple, mapping each local variable to its value.
    let mut bindings: Vec<HashMap<String, i64>> = clause
        .base
        .iter()
        .map(|row| {
            clause
                .var_cols
                .iter()
                .map(|(var, &col)| (var.clone(), row[col]))
                .collect()
        })
        .collect();

    // 2. Positive references: join each binding against the referenced relation,
    //    binding the `TO` target (the column following the subject columns).
    for r in &clause.pos_refs {
        let empty = Relation::new();
        let target_rel = rels.get(&r.rule).unwrap_or(&empty);
        let mut next = Vec::new();
        for b in &bindings {
            let subj: Tuple = r.subjects.iter().map(|s| b[s]).collect();
            for fact in target_rel {
                if matches_subject(fact, &subj) {
                    let mut nb = b.clone();
                    if let Some(t) = &r.target {
                        nb.insert(t.clone(), fact[subj.len()]);
                    }
                    next.push(nb);
                }
            }
        }
        bindings = next;
    }

    // 3. Negated references: keep only bindings whose subject tuple is absent.
    for r in &clause.neg_refs {
        let empty = Relation::new();
        let banned = rels.get(&r.rule).unwrap_or(&empty);
        bindings.retain(|b| {
            let subj: Tuple = r.subjects.iter().map(|s| b[s]).collect();
            !banned.iter().any(|fact| matches_subject(fact, &subj))
        });
    }

    // 4. Project to the YIELD KEY columns. The caller's HashSet dedups.
    bindings
        .into_iter()
        .map(|b| clause.yield_vars.iter().map(|v| b[v]).collect())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generator::{
        build_complement, build_layered_dag, build_union, expected_closure_size,
    };
    use crate::ir::{IsRef, OracleClause, OracleProgram, OracleRule};

    /// Oracle closure must equal the closed-form size across a grid spanning 300.
    #[test]
    fn closure_matches_closed_form_grid() {
        for stages in 1..6 {
            for width in 1..11 {
                let g = build_layered_dag(stages, width);
                let rels = evaluate(&g.oracle_rules);
                let got = rels.get("reaches").map_or(0, Relation::len);
                assert_eq!(
                    got,
                    expected_closure_size(stages, width),
                    "stages={stages} width={width}"
                );
            }
        }
    }

    /// Specific cases that straddle the engine's 300-fact threshold.
    #[test]
    fn closure_matches_closed_form_across_threshold() {
        for &(s, w) in &[(3, 10), (2, 20), (6, 5), (4, 15)] {
            let g = build_layered_dag(s, w);
            let rels = evaluate(&g.oracle_rules);
            assert_eq!(
                rels["reaches"].len(),
                expected_closure_size(s, w),
                "stages={s} width={w}"
            );
        }
    }

    /// Evaluation is order-independent: two runs yield identical relations.
    #[test]
    fn evaluation_is_deterministic() {
        let g = build_layered_dag(3, 8);
        assert_eq!(evaluate(&g.oracle_rules), evaluate(&g.oracle_rules));
    }

    /// Generated complement: `unreached` = all-pairs minus the closure, exactly.
    #[test]
    fn build_complement_oracle_is_exact() {
        for &(s, w) in &[(3, 5), (4, 12)] {
            let g = build_complement(s, w);
            let rels = evaluate(&g.oracle_rules);
            let total_pairs = (s * w) * (s * w);
            assert_eq!(
                rels["unreached"].len(),
                total_pairs - expected_closure_size(s, w),
                "({s},{w})"
            );
        }
    }

    /// Generated union: `linked` = disjoint forward ∪ reverse = `2 · |edges|`.
    #[test]
    fn build_union_oracle_is_exact() {
        for &(s, w) in &[(2, 4), (3, 5)] {
            let g = build_union(s, w);
            let rels = evaluate(&g.oracle_rules);
            let forward = w * w * s.saturating_sub(1);
            assert_eq!(rels["linked"].len(), 2 * forward, "({s},{w})");
        }
    }

    /// A FOLD reads the bag: two identical parallel edges count twice.
    #[test]
    fn fold_counts_every_row() {
        use crate::ir::{Agg, FoldAgg, OracleFold};
        // Edges [src, dst, index, weight]: two identical 0->1 edges and 0->2.
        let edges: Vec<Tuple> = vec![vec![0, 1, 0, 2], vec![0, 1, 1, 2], vec![0, 2, 2, 5]];
        let deg = OracleRule {
            name: "deg".to_string(),
            clauses: vec![OracleClause {
                base: edges,
                var_cols: HashMap::from([("a".to_string(), 0), ("w".to_string(), 3)]),
                pos_refs: Vec::new(),
                neg_refs: Vec::new(),
                yield_vars: vec!["a".to_string(), "w".to_string()],
            }],
            fold: Some(OracleFold {
                key_count: 1,
                aggs: [
                    (Agg::CountStar, None),
                    (Agg::Sum, Some(1)),
                    (Agg::Min, Some(1)),
                    (Agg::Max, Some(1)),
                    (Agg::Count, Some(1)),
                ]
                .into_iter()
                .map(|(agg, input)| FoldAgg { agg, input })
                .collect(),
            }),
        };
        let rels = evaluate(&OracleProgram {
            strata: vec![vec![deg]],
        });
        let expected: Relation = [vec![0, 3, 9, 2, 5, 3]].into_iter().collect();
        assert_eq!(rels["deg"], expected);
    }

    /// Stratified `IS NOT`: `unreached` is the exact complement of `reaches`.
    ///
    /// Graph 0->1->2. reaches = {(0,1),(1,2),(0,2)}; all 9 ordered pairs minus
    /// those 3 = 6 unreached pairs. Exercises the anti-join + stratum ordering.
    #[test]
    fn stratified_complement_is_exact() {
        let edges: Vec<Tuple> = vec![vec![0, 1], vec![1, 2]];
        let all_pairs: Vec<Tuple> = (0..3)
            .flat_map(|a| (0..3).map(move |b| vec![a, b]))
            .collect();

        let reaches = OracleRule {
            fold: None,
            name: "reaches".to_string(),
            clauses: vec![
                OracleClause {
                    base: edges.clone(),
                    var_cols: HashMap::from([("a".to_string(), 0), ("b".to_string(), 1)]),
                    pos_refs: Vec::new(),
                    neg_refs: Vec::new(),
                    yield_vars: vec!["a".to_string(), "b".to_string()],
                },
                OracleClause {
                    base: edges,
                    var_cols: HashMap::from([("a".to_string(), 0), ("mid".to_string(), 1)]),
                    pos_refs: vec![IsRef {
                        rule: "reaches".to_string(),
                        subjects: vec!["mid".to_string()],
                        target: Some("b".to_string()),
                    }],
                    neg_refs: Vec::new(),
                    yield_vars: vec!["a".to_string(), "b".to_string()],
                },
            ],
        };
        let unreached = OracleRule {
            fold: None,
            name: "unreached".to_string(),
            clauses: vec![OracleClause {
                base: all_pairs,
                var_cols: HashMap::from([("a".to_string(), 0), ("b".to_string(), 1)]),
                pos_refs: Vec::new(),
                neg_refs: vec![IsRef {
                    rule: "reaches".to_string(),
                    subjects: vec!["a".to_string(), "b".to_string()],
                    target: None,
                }],
                yield_vars: vec!["a".to_string(), "b".to_string()],
            }],
        };

        let program = OracleProgram {
            strata: vec![vec![reaches], vec![unreached]],
        };
        let rels = evaluate(&program);

        let expected_reaches: Relation = [vec![0, 1], vec![1, 2], vec![0, 2]].into_iter().collect();
        assert_eq!(rels["reaches"], expected_reaches);

        let expected_unreached: Relation = [
            vec![0, 0],
            vec![1, 0],
            vec![1, 1],
            vec![2, 0],
            vec![2, 1],
            vec![2, 2],
        ]
        .into_iter()
        .collect();
        assert_eq!(rels["unreached"], expected_unreached);
    }
}
