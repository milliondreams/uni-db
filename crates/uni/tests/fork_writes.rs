// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 2 Day 7 — first end-to-end fork write test.
//!
//! Verifies the full path:
//! `session.fork(name) -> tx() -> execute() -> commit()` lands
//! mutations on the fork's Lance branches, and a read on the same
//! forked session sees those mutations. Primary's view is unaffected.
//!
//! This is the Phase 2 demo target. If anything substantive in the
//! substrate (Days 1–6) is wrong, it surfaces here.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn forked_tx_commit_lands_on_fork_branch() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    // Seed primary so the Lance dataset for `vertices_Person` exists
    // on disk. Without this seed there is no dataset to branch from
    // when the fork is created — schema overlay growth lands on Day 10.
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Primary-Alice'})").await?;
    tx.commit().await?;
    db.flush().await?;

    // Open a forked session.
    let forked = session.fork("scenario_1").await?;
    assert!(forked.is_forked());

    // Write through the fork — this is the new Phase 2 capability.
    let tx = forked.tx().await?;
    tx.execute("CREATE (:Person {name: 'Fork-Bob'})").await?;
    tx.execute("CREATE (:Person {name: 'Fork-Carol'})").await?;
    tx.commit().await?;

    // Diagnostic: enumerate what the fork actually sees.
    let result = forked.query("MATCH (p:Person) RETURN p.name").await?;
    let names: Vec<String> = result
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("p.name").ok())
        .collect();
    eprintln!("fork sees names = {names:?}");
    assert_eq!(
        names.len(),
        3,
        "fork should see fork-point + its own writes; got {names:?}"
    );

    // Primary's view is unchanged: only the seed row.
    let primary_rows = session
        .query("MATCH (p:Person) RETURN p.name")
        .await?
        .rows()
        .len();
    assert_eq!(
        primary_rows, 1,
        "primary must not see fork's writes"
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn fork_and_primary_writes_remain_isolated_under_interleaving() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("kind", DataType::String)
        .apply()
        .await?;

    // Seed primary with one item.
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Item {kind: 'seed'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let forked = session.fork("interleave").await?;

    // Interleave: fork writes, then primary writes, then fork writes.
    let tx = forked.tx().await?;
    tx.execute("CREATE (:Item {kind: 'fork-1'})").await?;
    tx.commit().await?;

    let tx = session.tx().await?;
    tx.execute("CREATE (:Item {kind: 'primary-2'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = forked.tx().await?;
    tx.execute("CREATE (:Item {kind: 'fork-3'})").await?;
    tx.commit().await?;

    // Primary sees: seed + primary-2 = 2 rows.
    let primary_count = session
        .query("MATCH (i:Item) RETURN i")
        .await?
        .rows()
        .len();
    assert_eq!(primary_count, 2);

    // Fork sees: seed (fork-point) + fork-1 + fork-3 = 3 rows.
    // It does NOT see primary-2 (post-fork primary write).
    let fork_count = forked
        .query("MATCH (i:Item) RETURN i")
        .await?
        .rows()
        .len();
    assert_eq!(fork_count, 3);

    db.shutdown().await?;
    Ok(())
}
