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
