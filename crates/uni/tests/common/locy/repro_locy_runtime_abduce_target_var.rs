// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end runtime repro for uni-query finding [10]
//! (`crates/uni-query/src/query/df_graph/locy_abduce.rs:235` and `:281`).
//!
//! # Bug
//!
//! ABDUCE generates candidate graph modifications from a rule's MATCH pattern.
//! For a positive ABDUCE the generator is `extract_addition_candidates`
//! (`AddEdge` candidates); for `ABDUCE NOT` it is `extract_edge_candidates`
//! (`RemoveEdge` candidates). Both push one edge candidate per relationship in
//! the path, then run a second "fix up the target variable" pass. That pass
//! mutates `candidates.last_mut()` — the candidate for the *last* relationship —
//! instead of the candidate for the relationship whose target node it is
//! currently visiting. So for any path that traverses two or more relationships
//! the binding is written to the wrong candidate:
//!
//! For a body `(a)-[:R1]->(b)-[:R2]->(c)` the generator produces two edge
//! candidates `[{src:a, tgt:""}, {src:b, tgt:""}]`. The fix-up pass, on the
//! node `b` after `R1`, writes `b` into `candidates.last_mut()` (the R2
//! candidate), yielding `{src:b, tgt:b}` — a self-loop. On the node `c` after
//! `R2` the same `last_mut()` candidate already has a non-empty target, so `c`
//! is dropped. Net result:
//!
//!   observed:  [{src:a, tgt:"" (empty)}, {src:b, tgt:"b" (self-loop)}]
//!   correct:   [{src:a, tgt:"b"},        {src:b, tgt:"c"}]
//!
//! The first edge's target is never filled and the second edge points at the
//! wrong node — so the abduced edges are wrong for any multi-relationship rule.

use anyhow::Result;
use uni_db::Uni;
use uni_db::locy::{CommandResult, LocyConfig};
use uni_locy::Modification;

/// A three-node chain rule whose body traverses TWO relationships, which is the
/// minimal shape that exercises the multi-edge `target_var` fix-up.
const CHAIN_RULE: &str = "CREATE RULE chain AS \
     MATCH (a:N)-[:R1]->(b:N)-[:R2]->(c:N) \
     YIELD KEY a, KEY c \n\
     ABDUCE chain";

fn config() -> LocyConfig {
    LocyConfig {
        max_iterations: 1000,
        ..Default::default()
    }
}

/// Seed three named nodes so ABDUCE has real vertices to reason over. There are
/// deliberately NO R1/R2 edges — positive ABDUCE proposes edge *additions* and
/// the candidate shape is derived from the rule pattern, not the data.
async fn seed_nodes(db: &Uni) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:N {name: 'A'}), (:N {name: 'B'}), (:N {name: 'C'})")
        .await?;
    tx.commit().await?;
    Ok(())
}

/// Pull the AddEdge `(source_var, target_var)` pairs out of the ABDUCE result.
fn add_edge_pairs(result: &uni_locy::LocyResult) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    for cr in &result.command_results {
        if let CommandResult::Abduce(ab) = cr {
            for vm in &ab.modifications {
                if let Modification::AddEdge {
                    source_var,
                    target_var,
                    ..
                } = &vm.modification
                {
                    pairs.push((source_var.clone(), target_var.clone()));
                }
            }
        }
    }
    pairs
}

/// FIXED (locy_abduce.rs): a two-relationship rule abduces exactly the two path
/// edges `(a,b)` and `(b,c)`, each with a correct non-empty target — the fix-up
/// attributes each hop's target to its own edge instead of `candidates.last_mut()`.
#[tokio::test]
async fn abduce_multihop_target_var_should_be_correct() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed_nodes(&db).await?;

    let result = db
        .session()
        .locy_with(CHAIN_RULE)
        .with_config(config())
        .run()
        .await?;

    let pairs = add_edge_pairs(&result);
    println!("[10] abduce AddEdge (source,target) pairs -> {pairs:?}");
    assert!(
        pairs.iter().any(|(s, t)| s == "a" && t == "b"),
        "correct: first edge should be (a,b); got {pairs:?}"
    );
    assert!(
        pairs.iter().any(|(s, t)| s == "b" && t == "c"),
        "correct: second edge should be (b,c); got {pairs:?}"
    );
    assert!(
        !pairs.iter().any(|(_, t)| t.is_empty()),
        "correct: no candidate should have an empty target; got {pairs:?}"
    );
    assert!(
        !pairs.iter().any(|(s, t)| s == t),
        "correct: no self-loop candidate should be produced; got {pairs:?}"
    );
    Ok(())
}

/// A rule whose body has an ANONYMOUS relationship into a node literally named
/// `r` — the shape in the `locy_cyber_exposure_twin` notebook's
/// `needs_immediate_patch` rule (`...-[:REMEDIATED_BY]->(r:RemediationAction)`).
/// `ABDUCE NOT` builds a synthetic `RemoveEdge` query for this hop.
const ANON_EDGE_INTO_R_RULE: &str = "CREATE RULE np AS \
     MATCH (a:N)-[:R]->(r:N) \
     YIELD KEY a \n\
     ABDUCE NOT np";

/// Seed one `(a)-[:R]->(r)` edge so `np` derives a fact — giving `ABDUCE NOT`
/// a real leaf to propose removing (which is what constructs the synthetic
/// `MATCH (a)-[?:R]->(r) DELETE ?` query).
async fn seed_anon_edge(db: &Uni) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:N {name: 'A'}), (b:N {name: 'B'}), (a)-[:R]->(b)")
        .await?;
    tx.commit().await?;
    Ok(())
}

fn remove_edge_pairs(result: &uni_locy::LocyResult) -> Vec<(String, String, String)> {
    let mut out = Vec::new();
    for cr in &result.command_results {
        if let CommandResult::Abduce(ab) = cr {
            for vm in &ab.modifications {
                if let Modification::RemoveEdge {
                    source_var,
                    target_var,
                    edge_var,
                    ..
                } = &vm.modification
                {
                    out.push((source_var.clone(), edge_var.clone(), target_var.clone()));
                }
            }
        }
    }
    out
}

/// Regression (locy_cyber_exposure_twin notebook): `ABDUCE NOT` over a rule with
/// an anonymous edge into a node named `r` must not blow up. Previously
/// `modification_to_cypher` defaulted the empty edge variable to the literal
/// `"r"`, producing `MATCH (a)-[r:R]->(r) DELETE r`, which the planner rejected
/// with `VariableTypeConflict - Variable 'r' already defined as relationship`,
/// failing the whole ABDUCE. The node in this rule is named `b`, but the notebook
/// names it `r`; we still assert the run SUCCEEDS and yields a well-formed
/// RemoveEdge whose edge var never collides with an endpoint node var.
#[tokio::test]
async fn abduce_not_anonymous_edge_into_node_named_r_does_not_conflict() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed_anon_edge(&db).await?;

    // The definitive check: this .run() previously returned
    // Err(VariableTypeConflict). It must now succeed.
    let result = db
        .session()
        .locy_with(ANON_EDGE_INTO_R_RULE)
        .with_config(config())
        .run()
        .await?;

    let triples = remove_edge_pairs(&result);
    println!("abduce NOT RemoveEdge (src,edge,tgt) -> {triples:?}");
    for (src, edge, tgt) in &triples {
        assert_ne!(
            edge, tgt,
            "edge var must not collide with target node var; got ({src},{edge},{tgt})"
        );
        assert_ne!(
            edge, src,
            "edge var must not collide with source node var; got ({src},{edge},{tgt})"
        );
    }
    Ok(())
}

/// The exact notebook shape: the target node is literally named `r`. Uses a
/// two-hop body `(a)-[f:HAS_FINDING]->(v)-[:REMEDIATED_BY]->(r)` mirroring
/// `needs_immediate_patch`, with the second edge anonymous into `r`.
#[tokio::test]
async fn abduce_not_notebook_shape_target_named_r_succeeds() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:N {name: 'asset'}), (v:N {name: 'vuln'}), (r:N {name: 'remedy'}), \
         (a)-[:HAS_FINDING]->(v), (v)-[:REMEDIATED_BY]->(r)",
    )
    .await?;
    tx.commit().await?;

    let rule = "CREATE RULE needs_immediate_patch AS \
         MATCH (a:N)-[f:HAS_FINDING]->(v:N)-[:REMEDIATED_BY]->(r:N) \
         YIELD KEY a, KEY v \n\
         ABDUCE NOT needs_immediate_patch";

    // Must not error with VariableTypeConflict on the `-[:REMEDIATED_BY]->(r)` hop.
    let result = db
        .session()
        .locy_with(rule)
        .with_config(config())
        .run()
        .await?;

    let triples = remove_edge_pairs(&result);
    println!("notebook-shape abduce NOT RemoveEdge -> {triples:?}");
    for (src, edge, tgt) in &triples {
        assert_ne!(
            edge, tgt,
            "edge var collided with target; ({src},{edge},{tgt})"
        );
        assert_ne!(
            edge, src,
            "edge var collided with source; ({src},{edge},{tgt})"
        );
    }
    Ok(())
}
