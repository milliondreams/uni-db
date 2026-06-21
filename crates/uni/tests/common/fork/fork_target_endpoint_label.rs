// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression tests for GitHub #99 — on a fork, a relationship pattern with a
//! label predicate on the *target* endpoint must match.
//!
//! Follow-up to #97. After #97 a fork inherits the parent's data by flushing
//! the parent's L0 to Lance before branching, so on a fork every inherited
//! vertex lives in Lance storage, not L0.
//!
//! In the *schemaless* path (the issue's repro declares no schema) the planner
//! compiles `b:B` inside `MATCH (a)-[r:R]->(b:B)` into a `hasLabel(b, "B")`
//! filter over a labels column the traverse emits. That column was built from
//! the L0 chain only, falling back to an *empty* label set for Lance-only
//! vertices — so `hasLabel` evaluated false on the fork and the query returned
//! 0, even though the edge, the target node, the unlabelled form, and the
//! source-label form all matched.
//!
//! The fix resolves labels from the L0 chain and then the persisted
//! `VidLabelsIndex` (populated at flush, inherited by the fork). The sibling
//! forms are asserted first so a regression in label resolution cannot hide
//! behind a broken traversal.

// Rust guideline compliant

use anyhow::Result;
use uni_db::Uni;

async fn count(scope: &uni_db::Session, cypher: &str) -> Result<usize> {
    Ok(scope.query(cypher).await?.rows().len())
}

/// #99 core repro, schemaless (exactly the issue's shape): a fork must match a
/// target-endpoint label predicate, just like the base session does.
#[tokio::test]
async fn fork_matches_target_endpoint_label_schemaless() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();

    // No schema is declared: this routes through the schemaless main path,
    // where `b:B` becomes a `hasLabel` filter rather than a label-scoped scan.
    let tx = session.tx().await?;
    tx.execute("CREATE (:A {k: 1})-[:R]->(:B {k: 2})").await?;
    tx.commit().await?;

    // Base session resolves the target-endpoint label from L0.
    assert_eq!(
        count(&session, "MATCH (a)-[r:R]->(b:B) RETURN r").await?,
        1,
        "base must match a target-endpoint label"
    );

    let fork = session.fork("scn").await?;

    // Sibling forms that already worked — guard against a broken traversal.
    assert_eq!(
        count(&fork, "MATCH (a)-[r:R]->(b) RETURN r").await?,
        1,
        "fork: unlabelled target must match"
    );
    assert_eq!(
        count(&fork, "MATCH (a:A)-[r:R]->(b) RETURN r").await?,
        1,
        "fork: source-endpoint label must match"
    );
    assert_eq!(
        count(&fork, "MATCH (b:B) RETURN b").await?,
        1,
        "fork: the target node itself must be visible"
    );

    // The #99 bug: target-endpoint label returned 0 on the fork.
    assert_eq!(
        count(&fork, "MATCH (a)-[r:R]->(b:B) RETURN r").await?,
        1,
        "fork must match the target-endpoint label (GitHub #99)"
    );

    db.shutdown().await?;
    Ok(())
}

/// The flip side of the same label-resolution path: a target-endpoint label
/// that does *not* match must be rejected on a fork. Before the fix the
/// traversal trusted storage and kept the row (a false positive); the index
/// now resolves the real label so the mismatch is filtered out.
#[tokio::test]
async fn fork_rejects_wrong_target_endpoint_label_schemaless() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();

    // Edge into a `B`; we will ask for `:C`, which must not match.
    let tx = session.tx().await?;
    tx.execute("CREATE (:A {k: 1})-[:R]->(:B {k: 2})").await?;
    tx.commit().await?;

    let fork = session.fork("scn").await?;
    assert_eq!(
        count(&fork, "MATCH (a)-[r:R]->(b:C) RETURN r").await?,
        0,
        "fork must not match a target label the vertex does not have (GitHub #99)"
    );
    // Sanity: the correct label still matches on the same fork.
    assert_eq!(
        count(&fork, "MATCH (a)-[r:R]->(b:B) RETURN r").await?,
        1,
        "fork must still match the correct target label"
    );

    db.shutdown().await?;
    Ok(())
}
