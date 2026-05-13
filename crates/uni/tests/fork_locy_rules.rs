// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 2 Day 9 — Locy rule registry isolation across forks.
//!
//! Locks in the behavior already established by Phase 1's
//! `UniInner::at_fork`, which deep-clones `locy_rule_registry` from
//! primary into the forked inner. Per the pending-work doc, no
//! substrate change is needed; this test exists so a future regression
//! that breaks fork↔primary registry isolation surfaces immediately.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni};

/// Build a Uni with the schema the test rules reference.
async fn db_with_schema() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema().edge_type("E", &["N"], &["N"]).apply().await?;
    Ok(db)
}

#[tokio::test]
async fn fork_inherits_primary_db_rules_at_fork_point() -> Result<()> {
    let db = db_with_schema().await?;

    // Register on the primary db registry — this is the inheritable state.
    db.rules().register(
        "CREATE RULE rule_a AS \
         MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b",
    )?;

    let primary = db.session();
    let forked = primary.fork("inherit").await?;

    let primary_rules = primary.rules().list();
    let fork_rules = forked.rules().list();

    assert!(
        primary_rules.contains(&"rule_a".to_string()),
        "primary should see rule_a; got {primary_rules:?}"
    );
    assert!(
        fork_rules.contains(&"rule_a".to_string()),
        "fork should inherit rule_a from fork point; got {fork_rules:?}"
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn fork_local_rules_do_not_leak_to_primary() -> Result<()> {
    let db = db_with_schema().await?;
    let primary = db.session();
    let forked = primary.fork("isolated").await?;

    // Fork registers its own rule.
    forked.rules().register(
        "CREATE RULE fork_only AS \
         MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b",
    )?;

    let fork_rules = forked.rules().list();
    let primary_rules = primary.rules().list();
    let db_rules = db.rules().list();

    assert!(
        fork_rules.contains(&"fork_only".to_string()),
        "fork should see its own rule; got {fork_rules:?}"
    );
    assert!(
        !primary_rules.contains(&"fork_only".to_string()),
        "primary session must not see fork's rule; got {primary_rules:?}"
    );
    assert!(
        !db_rules.contains(&"fork_only".to_string()),
        "primary db registry must not see fork's rule; got {db_rules:?}"
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn primary_rules_added_after_fork_are_invisible_to_fork() -> Result<()> {
    let db = db_with_schema().await?;
    let primary = db.session();
    let forked = primary.fork("snapshot").await?;

    // Register on primary db AFTER fork is created — fork must not see it
    // (snapshot semantics at fork point).
    db.rules().register(
        "CREATE RULE post_fork AS \
         MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b",
    )?;

    let fork_rules = forked.rules().list();
    let db_rules = db.rules().list();

    assert!(
        db_rules.contains(&"post_fork".to_string()),
        "primary db should see its own newly-registered rule; got {db_rules:?}"
    );
    assert!(
        !fork_rules.contains(&"post_fork".to_string()),
        "fork must not see primary rules added after fork point; got {fork_rules:?}"
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn fork_and_primary_each_see_own_plus_inherited() -> Result<()> {
    let db = db_with_schema().await?;

    // Pre-fork shared rule.
    db.rules().register(
        "CREATE RULE shared AS \
         MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b",
    )?;

    let primary = db.session();
    let forked = primary.fork("each_own").await?;

    // Each side adds its own rule after fork.
    db.rules().register(
        "CREATE RULE primary_only AS \
         MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b",
    )?;
    forked.rules().register(
        "CREATE RULE fork_only AS \
         MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b",
    )?;

    let db_rules = db.rules().list();
    let fork_rules = forked.rules().list();

    // Primary db: shared + primary_only, not fork_only.
    assert!(db_rules.contains(&"shared".to_string()));
    assert!(db_rules.contains(&"primary_only".to_string()));
    assert!(
        !db_rules.contains(&"fork_only".to_string()),
        "primary db must not see fork's rule; got {db_rules:?}"
    );

    // Fork: shared (inherited) + fork_only, not primary_only (post-fork).
    assert!(
        fork_rules.contains(&"shared".to_string()),
        "fork should inherit shared rule; got {fork_rules:?}"
    );
    assert!(
        fork_rules.contains(&"fork_only".to_string()),
        "fork should see its own rule; got {fork_rules:?}"
    );
    assert!(
        !fork_rules.contains(&"primary_only".to_string()),
        "fork must not see primary's post-fork rule; got {fork_rules:?}"
    );

    db.shutdown().await?;
    Ok(())
}
