// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A fork must be able to mutate (SET) and delete (DETACH DELETE) an
//! INHERITED vertex — one created+flushed on the parent before the fork
//! was created (seen by the fork only via Lance `base_paths`).
//!
//! Root cause of the original bug: the fork's writer started its MVCC
//! version counter at 0, so a fork write-transaction pinned reads to
//! `_version <= 0` and filtered out every inherited row (which carries the
//! parent's higher version stamps). The MATCH feeding SET/DELETE returned
//! nothing, so the mutation silently no-opped. The fix bootstraps the
//! fork's version floor to the parent's fork-point HWM.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn fork_deletes_inherited_vertex() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'A'})").await?;
    tx.execute("CREATE (:Person {name: 'B'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let fork = session.fork("del").await?;
    let ftx = fork.tx().await?;
    ftx.execute("MATCH (n:Person {name: 'B'}) DETACH DELETE n")
        .await?;
    ftx.commit().await?;

    // Fork no longer sees B; parent is unaffected (isolation).
    let fork_names = names(&fork, "MATCH (n:Person) RETURN n.name AS name").await?;
    assert_eq!(
        fork_names,
        vec!["A"],
        "fork should have deleted inherited B"
    );
    let parent_names = names(&session, "MATCH (n:Person) RETURN n.name AS name").await?;
    assert_eq!(parent_names, vec!["A", "B"], "parent must be unaffected");

    drop(db);
    Ok(())
}

#[tokio::test]
async fn fork_sets_property_on_inherited_vertex() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int64)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'A', age: 30})").await?;
    tx.commit().await?;
    db.flush().await?;

    let fork = session.fork("edit").await?;
    let ftx = fork.tx().await?;
    ftx.execute("MATCH (n:Person {name: 'A'}) SET n.age = 31")
        .await?;
    ftx.commit().await?;

    // Fork reflects the edit; parent keeps the original (isolation).
    let fork_age: i64 = fork
        .query("MATCH (n:Person {name: 'A'}) RETURN n.age AS age")
        .await?
        .rows()[0]
        .get("age")?;
    assert_eq!(fork_age, 31, "fork should reflect the SET on inherited A");
    let parent_age: i64 = session
        .query("MATCH (n:Person {name: 'A'}) RETURN n.age AS age")
        .await?
        .rows()[0]
        .get("age")?;
    assert_eq!(parent_age, 30, "parent must keep the original value");

    drop(db);
    Ok(())
}

#[tokio::test]
async fn fork_delete_inherited_survives_flush_and_reopen() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let uri = dir.path().to_str().unwrap().to_string();
    {
        let db = Uni::open(&uri).build().await?;
        db.schema()
            .label("Person")
            .property("name", DataType::String)
            .apply()
            .await?;
        let session = db.session();
        let tx = session.tx().await?;
        tx.execute("CREATE (:Person {name: 'A'})").await?;
        tx.execute("CREATE (:Person {name: 'B'})").await?;
        tx.commit().await?;
        db.flush().await?;

        let fork = session.fork("del").await?;
        let ftx = fork.tx().await?;
        ftx.execute("MATCH (n:Person {name: 'B'}) DETACH DELETE n")
            .await?;
        ftx.commit().await?;
        fork.flush().await?;
        assert_eq!(
            names(&fork, "MATCH (n:Person) RETURN n.name AS name").await?,
            vec!["A"],
            "fork delete must hold after fork flush"
        );
        drop(db);
    }
    // Reopen and re-open the fork: the inherited-vertex delete must persist.
    {
        let db = Uni::open(&uri).build().await?;
        let session = db.session();
        let fork = session.fork("del").await?;
        assert_eq!(
            names(&fork, "MATCH (n:Person) RETURN n.name AS name").await?,
            vec!["A"],
            "fork delete of inherited B must survive flush + reopen (durability)"
        );
        drop(db);
    }
    Ok(())
}

async fn names(s: &uni_db::Session, cypher: &str) -> Result<Vec<String>> {
    let rows = s.query(cypher).await?;
    let mut out: Vec<String> = rows
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("name").ok())
        .collect();
    out.sort();
    Ok(out)
}
