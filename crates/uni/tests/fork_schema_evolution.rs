// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 7 — Schema evolution × forks.
//!
//! Behavior we're documenting and pinning:
//!
//! 1. **Fork at v1, evolve primary to v2, fork reopens cleanly.**
//!    Adding a new label on primary doesn't disturb a pre-existing
//!    fork's view — the fork keeps reading the v1 schema columns
//!    via its branch.
//! 2. **Fork keeps writing against v1.** A fork session opened
//!    after the v2 evolution still writes against the v1 columns
//!    of the label it inherited; the new v2 label simply doesn't
//!    exist on the fork's branch path until the fork is dropped
//!    and recreated.
//! 3. **Adding a brand-new label on primary leaves existing forks
//!    untouched.** Forks created before the schema change never
//!    see the new label via their own session, even after primary
//!    has the label registered.

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn primary_schema_add_label_does_not_break_v1_fork() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    // v1 schema: just Person.
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'V1-Alice'})").await?;
    tx.commit().await?;
    db.flush().await?;

    // Fork at v1.
    {
        let fork = primary.fork("v1_fork").await?;
        let tx = fork.tx().await?;
        tx.execute("CREATE (:Person {name: 'V1-Fork-Bob'})").await?;
        tx.commit().await?;
        fork.flush().await?;
    }

    // Evolve primary to v2: add a brand-new label.
    db.schema()
        .label("Document")
        .property("title", DataType::String)
        .apply()
        .await?;
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Document {title: 'V2-Spec'})").await?;
    tx.commit().await?;
    db.flush().await?;

    // The v1 fork must still read its Person rows correctly.
    let fork = primary.fork("v1_fork").await?;
    let names: Vec<String> = fork
        .query("MATCH (p:Person) RETURN p.name AS name")
        .await?
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("name").ok())
        .collect();
    assert_eq!(
        names.len(),
        2,
        "v1 fork must still see V1-Alice + V1-Fork-Bob after primary v2 evolution: {:?}",
        names
    );

    // Drop and re-shutdown cleanly.
    drop(fork);
    db.drop_fork("v1_fork").await?;
    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn fork_continues_writing_after_primary_schema_grows() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Item {name: 'seed'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let fork = primary.fork("growing").await?;
    let tx = fork.tx().await?;
    tx.execute("CREATE (:Item {name: 'fork-pre'})").await?;
    tx.commit().await?;

    // Primary's schema grows; the fork's session keeps its v1 view.
    db.schema()
        .label("Tag")
        .property("name", DataType::String)
        .apply()
        .await?;

    // Fork continues to write against v1 cleanly.
    let tx = fork.tx().await?;
    tx.execute("CREATE (:Item {name: 'fork-post-v2'})").await?;
    tx.commit().await?;

    let names: Vec<String> = fork
        .query("MATCH (i:Item) RETURN i.name AS name")
        .await?
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("name").ok())
        .collect();
    assert_eq!(names.len(), 3, "fork sees seed + fork-pre + fork-post-v2");

    drop(fork);
    db.drop_fork("growing").await?;
    db.shutdown().await?;
    Ok(())
}
