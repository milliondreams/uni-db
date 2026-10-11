// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Every Locy command reads the facts the fixpoint published.
//!
//! Locy had a second, in-memory evaluator, which EXPLAIN used to re-derive a
//! rule (its "Mode B") and QUERY used for generator rules. It implements no
//! FOLD, BEST BY or post-FOLD WHERE, so wherever it answered, the answer could
//! differ from the fixpoint's. EXPLAIN needed it only because a rule's edge
//! columns were never hydrated: a rule yielding a relationship yielded NULL for
//! it. Now edges are carried by their EID and hydrated like nodes, generator
//! rules run in the fixpoint, and the second evaluator is gone.
//!
//! Also here, found while removing it:
//!
//! - an EID resolved as a VID bound an unrelated node (the ID spaces overlap);
//! - an IS reference, EXPLAIN, ABDUCE or DERIVE inside a `MODULE` failed with
//!   "unknown rule": the compiler resolved the name for its own checks only;
//! - `COLLECT` in a recursive FOLD failed at run time: its output was typed as
//!   a float;
//! - ABDUCE never proposed a node's or edge's property, and proposed `SET i.i`
//!   for a bare scalar binding, which cannot be applied;
//! - EXPLAIN turned every integer KEY into a node.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(single_engine)'

// Rust guideline compliant

use std::time::Duration;

use anyhow::{Result, bail};
use uni_common::Value;
use uni_db::Uni;
use uni_db::locy::{CommandResult, LocyConfig};

/// Nodes first, so node VIDs and edge EIDs both start at 0: an EID resolved as
/// a VID would bind one of the `:Other` nodes.
const GRAPH: &str = "CREATE (:Other {name: 'x'}), (:Other {name: 'y'}), (:Other {name: 'z'}), \
     (a:N {name: 'a', k: 2}), (b:N {name: 'b', k: 0}), (c:N {name: 'c', k: 1}), \
     (d:N {name: 'd', k: 0}), \
     (a)-[:E {w: 1}]->(b), (b)-[:E {w: 2}]->(c), (c)-[:E {w: 3}]->(d)";

async fn run(program: &str) -> Result<Vec<CommandResult>> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(GRAPH).await?;
    tx.commit().await?;
    let result = db
        .session()
        .locy_with(program)
        .with_config(LocyConfig {
            timeout: Some(Duration::from_secs(60)),
            ..Default::default()
        })
        .run()
        .await?;
    Ok(result.command_results.clone())
}

async fn rows(program: &str) -> Result<Vec<String>> {
    let results = run(program).await?;
    let Some(CommandResult::Query(rows)) = results.last() else {
        bail!("no QUERY result");
    };
    let mut out: Vec<String> = rows
        .iter()
        .map(|row| {
            let mut cells: Vec<String> = row.iter().map(|(k, v)| format!("{k}={v}")).collect();
            cells.sort();
            cells.join(" ")
        })
        .collect();
    out.sort();
    Ok(out)
}

/// The bindings of the derivations an EXPLAIN returns.
async fn explain_bindings(program: &str) -> Result<Vec<std::collections::HashMap<String, Value>>> {
    let results = run(program).await?;
    let Some(CommandResult::Explain(root)) = results.last() else {
        bail!("no EXPLAIN result: {results:?}");
    };
    Ok(root.children.iter().map(|c| c.bindings.clone()).collect())
}

const EDGE_RULE: &str = "CREATE RULE r AS MATCH (a:N)-[e:E]->(b:N) YIELD KEY a, KEY e\n";

#[tokio::test]
async fn single_engine_a_rule_yields_its_edges() -> Result<()> {
    let got = rows(&format!("{EDGE_RULE}QUERY r RETURN a.name AS a, e.w AS w")).await?;
    let want = ["a=a w=1", "a=b w=2", "a=c w=3"];
    if got != want {
        bail!("expected {want:?}, got {got:?}");
    }
    let got = rows(&format!(
        "{EDGE_RULE}CREATE RULE s AS MATCH (a:N)-[f:E]->(b:N) WHERE a IS r TO f \
         YIELD KEY a, KEY f\nQUERY s RETURN a.name AS a, f.w AS w"
    ))
    .await?;
    let want = ["a=a w=1", "a=b w=2", "a=c w=3"];
    if got != want {
        bail!("a rule reading another's edge: expected {want:?}, got {got:?}");
    }
    Ok(())
}

#[tokio::test]
async fn single_engine_explain_binds_edges_and_keeps_scalar_keys() -> Result<()> {
    for bindings in
        explain_bindings(&format!("{EDGE_RULE}EXPLAIN RULE r WHERE a.name = 'a'")).await?
    {
        match bindings.get("e") {
            Some(Value::Edge(edge)) if edge.properties.get("w") == Some(&Value::Int(1)) => {}
            other => bail!("EXPLAIN bound e to {other:?}, expected the edge with w = 1"),
        }
    }
    for bindings in explain_bindings(
        "CREATE RULE r AS MATCH (a:N)-[e:E]->(b:N) YIELD KEY a, KEY e.w AS w\n\
         EXPLAIN RULE r WHERE a.name = 'a'",
    )
    .await?
    {
        if bindings.get("w") != Some(&Value::Int(1)) {
            bail!("EXPLAIN bound the integer KEY w to {:?}", bindings.get("w"));
        }
    }
    Ok(())
}

#[tokio::test]
async fn single_engine_module_rules_reference_each_other() -> Result<()> {
    let module = "MODULE acme\n\
         CREATE RULE e AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, KEY b\n\
         CREATE RULE r AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, KEY b\n\
         CREATE RULE r AS MATCH (a:N)-[:E]->(m:N) WHERE m IS r TO b YIELD KEY a, KEY b\n";
    let cases: Vec<(&str, String, &[&str])> = vec![
        (
            "IS",
            format!(
                "{module}CREATE RULE src AS MATCH (x:N) WHERE x IS e YIELD KEY x\nQUERY src RETURN x.name AS x"
            ),
            &["x=a", "x=b", "x=c"],
        ),
        (
            "IS NOT",
            format!(
                "{module}CREATE RULE leaf AS MATCH (x:N) WHERE x IS NOT e YIELD KEY x\nQUERY leaf RETURN x.name AS x"
            ),
            &["x=d"],
        ),
        (
            "recursion",
            format!("{module}QUERY r RETURN count(*) AS n"),
            &["n=6"],
        ),
    ];
    for (label, program, want) in cases {
        let got = rows(&program).await?;
        if got != want {
            bail!("{label}: expected {want:?}, got {got:?}");
        }
    }
    let explained = explain_bindings(&format!("{module}EXPLAIN RULE r WHERE a.name = 'a'")).await?;
    if explained.len() != 3 {
        bail!("EXPLAIN in a module: expected 3 derivations, got {explained:?}");
    }
    Ok(())
}

#[tokio::test]
async fn single_engine_collect_folds_in_a_recursive_rule() -> Result<()> {
    let got = rows(
        "CREATE RULE r AS MATCH (a:N)-[:E]->(b:N) FOLD l = COLLECT(b.name) YIELD KEY a, l\n\
         CREATE RULE r AS MATCH (a:N)-[:E]->(m:N) WHERE m IS r FOLD l = COLLECT(m.name) \
         YIELD KEY a, l\nQUERY r RETURN a.name AS a, l",
    )
    .await?;
    let want = ["a=a l=[b, b]", "a=b l=[c, c]", "a=c l=[d]"];
    if got != want {
        bail!("expected {want:?}, got {got:?}");
    }
    Ok(())
}

#[tokio::test]
async fn single_engine_abduce_proposes_entity_properties() -> Result<()> {
    let results = run("CREATE RULE big AS MATCH (n:N) WHERE n.k > 1 YIELD KEY n\n\
         ABDUCE NOT big WHERE n.name = 'a'")
    .await?;
    let Some(CommandResult::Abduce(result)) = results.last() else {
        bail!("no ABDUCE result: {results:?}");
    };
    let valid: Vec<String> = result
        .modifications
        .iter()
        .filter(|m| m.validated)
        .map(|m| format!("{:?}", m.modification))
        .collect();
    let proposes_k = valid
        .iter()
        .any(|m| m.contains("element_var: \"n\"") && m.contains("property: \"k\""));
    if !proposes_k {
        bail!(
            "ABDUCE did not validate setting n.k: {:?}",
            result.modifications
        );
    }
    Ok(())
}

/// EXPLAIN of a rule keeps every fact when another rule derives the same
/// tuples.
///
/// The provenance store keyed a fact by its row alone and kept the first
/// annotation, so when rule `e` derived `(a, b)` before rule `r` did, EXPLAIN
/// of `r` silently lost that derivation.
#[tokio::test]
async fn single_engine_explain_keeps_facts_another_rule_also_derives() -> Result<()> {
    let program = "CREATE RULE e AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, KEY b\n\
         CREATE RULE r AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, KEY b\n\
         CREATE RULE r AS MATCH (a:N)-[:E]->(m:N) WHERE m IS r TO b YIELD KEY a, KEY b\n\
         EXPLAIN RULE r WHERE a.name = 'a'";
    let mut targets: Vec<String> = explain_bindings(program)
        .await?
        .iter()
        .filter_map(|b| match b.get("b") {
            Some(Value::Node(n)) => n.properties.get("name").map(|v| v.to_string()),
            _ => None,
        })
        .collect();
    targets.sort();
    if targets != ["b", "c", "d"] {
        bail!("EXPLAIN of r: expected targets b, c, d, got {targets:?}");
    }
    Ok(())
}

/// ABDUCE finds the edge behind a probabilistic FOLD.
///
/// A program with an MNOR or MPROD FOLD records provenance, and ABDUCE built
/// its tree from that record, which has no children: it saw only the folded
/// fact, never the referenced rule's edge behind it, and proposed nothing that
/// could change the answer. It now always builds the tree from the published
/// facts.
#[tokio::test]
async fn single_engine_abduce_sees_through_a_probabilistic_fold() -> Result<()> {
    let results = run(
        "CREATE RULE link AS MATCH (a:N)-[e:E]->(b:N) YIELD KEY a, KEY b, e.w / 10.0 AS w\n\
         CREATE RULE risk AS MATCH (a:N) WHERE a IS link TO b FOLD p = MNOR(w) YIELD KEY a, p\n\
         ABDUCE NOT risk WHERE a.name = 'a'",
    )
    .await?;
    let Some(CommandResult::Abduce(result)) = results.last() else {
        bail!("no ABDUCE result: {results:?}");
    };
    let removes_edge = result
        .modifications
        .iter()
        .any(|m| m.validated && format!("{:?}", m.modification).contains("RemoveEdge"));
    if !removes_edge {
        bail!(
            "ABDUCE did not validate removing a->b: {:?}",
            result.modifications
        );
    }
    Ok(())
}
