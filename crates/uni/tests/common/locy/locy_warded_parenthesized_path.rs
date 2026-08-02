// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end companion to `uni-locy`'s `repro_warded_parenthesized_path`.
//!
//! That repro pins the *compiler* fix: `check_wardedness` now recurses into
//! `PatternElement::Parenthesized`, so a variable bound inside parentheses is
//! seen as match-bound and a legal rule stops being rejected.
//!
//! Passing the wardedness check only proves the rule compiles, though. If the
//! planner cannot expose a variable bound inside a parenthesised sub-pattern,
//! lifting the false positive would just move the failure one stage
//! downstream — a worse outcome than the original error, because it would
//! surface as an opaque planner or runtime fault instead of a named compile
//! error. This runs the rule against a real database to show it actually
//! derives.

use anyhow::Result;
use uni_db::Uni;

/// A DERIVE whose companion is bound inside parentheses must compile *and* run.
#[tokio::test]
async fn parenthesized_match_derives_at_runtime() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();

    let tx = session.tx().await?;
    tx.execute("CREATE (:P {name: 'a'})-[:KNOWS]->(:P {name: 'b'})")
        .await?;
    tx.commit().await?;

    // `b` is bound inside the parentheses. Before the compiler fix this was
    // rejected as a WardednessViolation; the point here is that nothing
    // downstream trips over it either.
    let result = session
        .locy(
            "CREATE RULE tagged AS \
               MATCH ((a:P)-[:KNOWS]->(b:P)) \
               DERIVE (b)-[:LINKED]->(a) \n\
             DERIVE tagged \n\
             MATCH (x:P)-[:LINKED]->(y:P) \
             RETURN x.name AS src, y.name AS dst",
        )
        .await?;

    let rows = match result.command_results.last().expect("no command results") {
        uni_db::locy::CommandResult::Cypher(rows) => rows,
        other => panic!("expected trailing Cypher, got {other:?}"),
    };
    assert_eq!(
        rows.len(),
        1,
        "a rule whose companion is bound inside parentheses must derive the \
         same edge the unparenthesised form does"
    );

    Ok(())
}

/// The unparenthesised form, as a control.
///
/// If this ever fails the parenthesised test above proves nothing, so the two
/// are kept side by side.
#[tokio::test]
async fn unparenthesized_match_derives_at_runtime() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();

    let tx = session.tx().await?;
    tx.execute("CREATE (:P {name: 'a'})-[:KNOWS]->(:P {name: 'b'})")
        .await?;
    tx.commit().await?;

    let result = session
        .locy(
            "CREATE RULE tagged AS \
               MATCH (a:P)-[:KNOWS]->(b:P) \
               DERIVE (b)-[:LINKED]->(a) \n\
             DERIVE tagged \n\
             MATCH (x:P)-[:LINKED]->(y:P) \
             RETURN x.name AS src, y.name AS dst",
        )
        .await?;

    let rows = match result.command_results.last().expect("no command results") {
        uni_db::locy::CommandResult::Cypher(rows) => rows,
        other => panic!("expected trailing Cypher, got {other:?}"),
    };
    assert_eq!(rows.len(), 1);

    Ok(())
}
