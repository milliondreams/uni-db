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

// ---------------------------------------------------------------------------
// #249 — declaring a property on a populated label, with forks in play.
//
// A Lance branch is a shallow *clone*: it carries its own manifest and its own
// copy of the schema, so widening primary's dataset does not reach it. Both
// scan paths project an explicit column list, which hard-errors on a column
// the branch has never seen, and a fork flush carrying the new column is
// rejected exactly as primary's was.
//
// These are the cases an eager, registry-driven propagation pass could not
// cover: the branch already exists, and the fork session that learns about the
// property is opened afterwards.
// ---------------------------------------------------------------------------

/// The fork already has a branch for the label (it wrote before the DDL), and
/// a later fork session sees the new property through the merged schema.
///
/// `Session::fork` is open-or-create and caches the fork's `UniInner` as a
/// `Weak`, so dropping the first session and re-forking rebuilds the merged
/// schema — which is how the fork comes to carry a column its branch predates.
#[tokio::test]
async fn fork_with_an_existing_branch_accepts_a_property_added_on_primary() -> Result<()> {
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

    // Cut the branch for `vertices_Item` *before* the property exists.
    {
        let fork = primary.fork("f249").await?;
        let tx = fork.tx().await?;
        tx.execute("CREATE (:Item {name: 'fork-pre'})").await?;
        tx.commit().await?;
        fork.flush().await?;
    }

    // Primary gains the property. The fork's branch manifest predates it.
    db.schema()
        .label("Item")
        .property_nullable("extra", DataType::String)
        .done()
        .apply()
        .await?;

    // A fresh fork session picks up the merged schema, so its batches now
    // carry `extra` against a branch that has never had the column.
    let fork = primary.fork("f249").await?;
    let tx = fork.tx().await?;
    tx.execute("CREATE (:Item {name: 'fork-post', extra: 'e1'})")
        .await?;
    tx.commit().await?;
    fork.flush().await?;

    let got: Vec<String> = fork
        .query("MATCH (i:Item) WHERE i.name = 'fork-post' RETURN i.extra AS v")
        .await?
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("v").ok())
        .collect();
    assert_eq!(got, vec!["e1".to_string()], "fork must round-trip `extra`");

    // The fork still sees everything it inherited: widening a branch clones
    // fragment metadata, so rows reached through `base_paths` survive.
    let names: Vec<String> = fork
        .query("MATCH (i:Item) RETURN i.name AS name")
        .await?
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("name").ok())
        .collect();
    assert_eq!(
        names.len(),
        3,
        "fork must see seed + fork-pre + fork-post, got {names:?}"
    );

    // Primary must not have gained the fork's rows.
    let primary_names: Vec<String> = primary
        .query("MATCH (i:Item) RETURN i.name AS name")
        .await?
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("name").ok())
        .collect();
    assert_eq!(
        primary_names,
        vec!["seed".to_string()],
        "fork rows leaked to primary: {primary_names:?}"
    );

    drop(fork);
    db.drop_fork("f249").await?;
    db.shutdown().await?;
    Ok(())
}

/// The same, one level down: a nested fork's branch chains
/// `child -> parent -> trunk`, and each level carries its own schema copy.
#[tokio::test]
async fn nested_fork_accepts_a_property_added_on_primary() -> Result<()> {
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

    {
        let parent = primary.fork("p249").await?;
        let tx = parent.tx().await?;
        tx.execute("CREATE (:Item {name: 'parent-pre'})").await?;
        tx.commit().await?;
        parent.flush().await?;

        let child = parent.fork("c249").await?;
        let tx = child.tx().await?;
        tx.execute("CREATE (:Item {name: 'child-pre'})").await?;
        tx.commit().await?;
        child.flush().await?;
    }

    db.schema()
        .label("Item")
        .property_nullable("extra", DataType::String)
        .done()
        .apply()
        .await?;

    let parent = primary.fork("p249").await?;
    let child = parent.fork("c249").await?;
    let tx = child.tx().await?;
    tx.execute("CREATE (:Item {name: 'child-post', extra: 'c1'})")
        .await?;
    tx.commit().await?;
    child.flush().await?;

    let got: Vec<String> = child
        .query("MATCH (i:Item) WHERE i.name = 'child-post' RETURN i.extra AS v")
        .await?
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("v").ok())
        .collect();
    assert_eq!(got, vec!["c1".to_string()], "nested fork must round-trip");

    let names: Vec<String> = child
        .query("MATCH (i:Item) RETURN i.name AS name")
        .await?
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("name").ok())
        .collect();
    assert_eq!(
        names.len(),
        4,
        "child must see seed + parent-pre + child-pre + child-post, got {names:?}"
    );

    drop(child);
    drop(parent);
    db.drop_fork_cascade("p249").await?;
    db.shutdown().await?;
    Ok(())
}

/// A fork created *after* the declaration branches from the already-widened
/// primary, so it needs no widening of its own. Guards the no-op path.
#[tokio::test]
async fn fork_created_after_the_declaration_needs_no_widening() -> Result<()> {
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

    db.schema()
        .label("Item")
        .property_nullable("extra", DataType::String)
        .done()
        .apply()
        .await?;

    let fork = primary.fork("after249").await?;
    let tx = fork.tx().await?;
    tx.execute("CREATE (:Item {name: 'fork', extra: 'x'})")
        .await?;
    tx.commit().await?;
    fork.flush().await?;

    let got: Vec<String> = fork
        .query("MATCH (i:Item) WHERE i.name = 'fork' RETURN i.extra AS v")
        .await?
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("v").ok())
        .collect();
    assert_eq!(got, vec!["x".to_string()]);

    drop(fork);
    db.drop_fork("after249").await?;
    db.shutdown().await?;
    Ok(())
}

/// The Cypher DDL path (`ALTER LABEL … ADD PROPERTY`) must widen storage the
/// same way the Rust builder does. This is the canonical user-facing entry
/// point for #249, and it reaches `declare_property` through
/// `execute_alter_entity` rather than through `SchemaBuilder::apply`.
#[tokio::test]
async fn cypher_alter_add_property_widens_primary_and_forks() -> Result<()> {
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

    // A branch that predates the property.
    {
        let fork = primary.fork("alter249").await?;
        let tx = fork.tx().await?;
        tx.execute("CREATE (:Item {name: 'fork-pre'})").await?;
        tx.commit().await?;
        fork.flush().await?;
    }

    let tx = primary.tx().await?;
    tx.execute("ALTER LABEL Item ADD PROPERTY extra STRING")
        .await?;
    tx.commit().await?;

    // Primary must accept a write carrying the new column.
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Item {name: 'primary-post', extra: 'p1'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // …and a fork whose branch predates the property must still read.
    let fork = primary.fork("alter249").await?;
    let names: Vec<String> = fork
        .query("MATCH (i:Item) RETURN i.name AS name")
        .await?
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("name").ok())
        .collect();
    assert_eq!(
        names.len(),
        2,
        "fork must still read seed + fork-pre after the ALTER, got {names:?}"
    );

    // Projecting the new column through the fork must not fail either.
    let projected = fork
        .query("MATCH (i:Item) WHERE i.name = 'fork-pre' RETURN i.extra AS v")
        .await;
    assert!(
        projected.is_ok(),
        "fork read projecting the new column failed: {:?}",
        projected.err()
    );

    let v = primary
        .query("MATCH (i:Item) WHERE i.name = 'primary-post' RETURN i.extra AS v")
        .await?;
    assert_eq!(
        v.rows()
            .first()
            .and_then(|r| r.get::<String>("v").ok())
            .as_deref(),
        Some("p1"),
        "primary must round-trip the ALTER-added property"
    );

    drop(fork);
    db.drop_fork("alter249").await?;
    db.shutdown().await?;
    Ok(())
}
