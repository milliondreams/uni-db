// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression tests for GitHub #97 — a fork must inherit the parent
//! session's committed-but-unflushed (L0) writes.
//!
//! A fork branches off concrete Lance dataset versions and resolves
//! reads through `base_paths`; it never consults the parent's in-memory
//! L0 buffer. `create_fork_2pc` therefore flushes the parent's L0 to L1
//! before branching, materializing the fork point. Before the fix this
//! flush was gated on `parent.is_forked()`, so a primary-parent fork
//! (and especially an in-memory DB, which never auto-flushes) branched
//! off a stale/empty Lance tip and read zero rows.
//!
//! Every "fork sees inherited data" test below asserts the inherited
//! rows are visible *before* the fork performs any of its own writes.
//! That ordering is load-bearing: a "create-fork-delete-assert-zero"
//! shape passes trivially under the bug (the fork never saw the base
//! data, so the delete is a no-op), so it cannot be used to detect #97.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Row, Uni};

/// Count `(:Person)` rows visible to a session.
async fn person_count(scope: &uni_db::Session) -> Result<usize> {
    Ok(scope.query("MATCH (n:Person) RETURN n").await?.rows().len())
}

fn names(rows: &[Row]) -> Vec<String> {
    rows.iter()
        .filter_map(|r| r.get::<String>("n.name").ok())
        .collect()
}

/// Build an in-memory DB with a `Person(name)` schema. Note: in-memory
/// DBs never auto-flush, so all committed data lives in L0 — the exact
/// condition that surfaced #97.
async fn person_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    Ok(db)
}

/// R1: the core #97 repro — commit one node, do NOT flush, fork, and
/// the fork must see that node *before* it mutates anything.
#[tokio::test]
async fn fork_inherits_unflushed_single_node() -> Result<()> {
    let db = person_db().await?;
    let session = db.session();

    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.commit().await?;
    // Deliberately NO db.flush() here.

    assert_eq!(
        person_count(&session).await?,
        1,
        "parent must see its write"
    );

    let fork = session.fork("scn").await?;
    let rows = fork.query("MATCH (n:Person) RETURN n.name").await?;
    let fork_names = names(rows.rows());
    assert_eq!(
        fork_names.len(),
        1,
        "fork must inherit the parent's unflushed L0 write; got {fork_names:?}"
    );
    assert_eq!(fork_names[0], "Alice");

    db.shutdown().await?;
    Ok(())
}

/// R2: the fork-count == parent-count invariant for unflushed data.
#[tokio::test]
async fn fork_count_equals_parent_count_no_flush() -> Result<()> {
    let db = person_db().await?;
    let session = db.session();

    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.execute("CREATE (:Person {name: 'Bob'})").await?;
    tx.commit().await?;

    let fork = session.fork("scn").await?;
    let parent = person_count(&session).await?;
    let forked = person_count(&fork).await?;
    assert_eq!(parent, 2);
    assert_eq!(forked, parent, "fork count must equal parent count");

    db.shutdown().await?;
    Ok(())
}

/// R3: a multi-row L0 batch (UNWIND) must be fully materialized.
#[tokio::test]
async fn fork_inherits_unflushed_many_nodes_unwind() -> Result<()> {
    let db = person_db().await?;
    let session = db.session();

    let tx = session.tx().await?;
    tx.execute("UNWIND range(1, 50) AS i CREATE (:Person {name: toString(i)})")
        .await?;
    tx.commit().await?;

    let fork = session.fork("scn").await?;
    assert_eq!(
        person_count(&fork).await?,
        50,
        "fork must see all 50 unflushed nodes before mutating"
    );

    db.shutdown().await?;
    Ok(())
}

/// R4: relationships and both endpoints must be inherited — edges live
/// in their own Lance datasets (main `edges` + per-type deltas), which
/// the pre-branch flush must also materialize.
#[tokio::test]
async fn fork_inherits_unflushed_relationship_and_endpoints() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema()
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()
        .await?;
    let session = db.session();

    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'A'})-[:KNOWS]->(:Person {name: 'B'})")
        .await?;
    tx.commit().await?;

    let fork = session.fork("scn").await?;
    assert_eq!(
        person_count(&fork).await?,
        2,
        "fork must see both endpoints"
    );
    let edge_count = fork
        .query("MATCH (:Person)-[r:KNOWS]->(:Person) RETURN r")
        .await?
        .rows()
        .len();
    assert_eq!(
        edge_count, 1,
        "fork must inherit the unflushed relationship before mutating"
    );

    db.shutdown().await?;
    Ok(())
}

/// R5: the pure in-memory case — never call `flush()` anywhere. This is
/// the scenario from the issue where a fork read zero rows.
#[tokio::test]
async fn in_memory_db_never_flushes_fork_sees_l0() -> Result<()> {
    let db = person_db().await?;
    let session = db.session();

    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'OnlyInL0'})").await?;
    tx.commit().await?;

    // No flush is ever issued; the data lives only in L0.
    let fork = session.fork("scn").await?;
    assert_eq!(
        person_count(&fork).await?,
        1,
        "in-memory fork must see committed L0 data without any flush"
    );

    db.shutdown().await?;
    Ok(())
}

/// R6: forking the very session that performed the writes (issue
/// variant). The writing session's own unflushed L0 must be inherited.
#[tokio::test]
async fn fork_the_writing_session_sees_own_unflushed_writes() -> Result<()> {
    let db = person_db().await?;
    let session = db.session();

    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Writer'})").await?;
    tx.commit().await?;

    // Fork the same session that just wrote.
    let fork = session.fork("self").await?;
    let fork_names = names(fork.query("MATCH (n:Person) RETURN n.name").await?.rows());
    assert_eq!(fork_names, vec!["Writer".to_string()]);

    db.shutdown().await?;
    Ok(())
}

/// R7: an on-the-fly label whose Lance dataset does not exist at
/// fork-point (created only in L0, never flushed) must still be branched.
/// `Person` is seeded and flushed so its dataset exists; `Tag` is only
/// ever in L0 — the pre-branch flush must create `vertices_Tag` on disk
/// so `build_datasets_for_fork` can branch it.
#[tokio::test]
async fn fork_inherits_unflushed_on_the_fly_label() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema()
        .label("Tag")
        .property("name", DataType::String)
        .apply()
        .await?;
    let session = db.session();

    // Person is flushed: its dataset exists on disk before the fork.
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'P'})").await?;
    tx.commit().await?;
    db.flush().await?;

    // Tag is committed only into L0: no `vertices_Tag` dataset yet.
    let tx = session.tx().await?;
    tx.execute("CREATE (:Tag {name: 'T'})").await?;
    tx.commit().await?;

    let fork = session.fork("scn").await?;
    let tags = fork
        .query("MATCH (t:Tag) RETURN t.name")
        .await?
        .rows()
        .len();
    assert_eq!(
        tags, 1,
        "fork must see the L0-only on-the-fly label materialized by the flush"
    );

    db.shutdown().await?;
    Ok(())
}

/// R8: snapshot isolation still holds post-fix even without any flush —
/// parent writes committed *after* the fork are invisible to the fork.
#[tokio::test]
async fn parent_writes_after_fork_invisible_no_initial_flush() -> Result<()> {
    let db = person_db().await?;
    let session = db.session();

    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.commit().await?;

    let fork = session.fork("scn").await?;

    // Parent commits more AFTER the fork — must not leak into the fork.
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Bob'})").await?;
    tx.commit().await?;

    assert_eq!(
        person_count(&fork).await?,
        1,
        "fork sees only the fork-point row"
    );
    assert_eq!(person_count(&session).await?, 2, "parent sees both rows");

    db.shutdown().await?;
    Ok(())
}

/// R9: reopening an existing fork must not pull in parent writes
/// committed after the fork was created (it skips `create_fork_2pc`, so
/// no re-flush/re-materialize happens). Uses an on-disk DB so the fork
/// inner can be dropped and reconstructed.
#[tokio::test]
async fn reopen_existing_fork_does_not_pull_new_parent_l0() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();

    let db = Uni::open(&path).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let session = db.session();

    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.commit().await?;

    // Create the fork (materializes Alice into its branch point), then
    // drop the fork session so reopening goes through `at_fork` again.
    {
        let fork = session.fork("f").await?;
        assert_eq!(person_count(&fork).await?, 1, "fresh fork sees Alice");
    }

    // Parent commits Bob AFTER the fork was created — unflushed.
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Bob'})").await?;
    tx.commit().await?;

    // Reopen the existing fork: it must still see only Alice.
    let reopened = session.fork("f").await?;
    assert_eq!(
        person_count(&reopened).await?,
        1,
        "reopened fork must not pull in parent's post-creation writes"
    );

    db.shutdown().await?;
    Ok(())
}

/// R10: nested chain with unflushed L0 at every level (no `db.flush()`
/// anywhere). primary→A→B; B must see primary's + A's unflushed writes
/// plus its own; A must not see B's.
#[tokio::test]
async fn nested_unflushed_chain_primary_fork_a_fork_b() -> Result<()> {
    let db = person_db().await?;
    let primary = db.session();

    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'P'})").await?;
    tx.commit().await?;

    // Fork A off primary (fix flushes primary's L0).
    let a = primary.fork("a").await?;
    let tx = a.tx().await?;
    tx.execute("CREATE (:Person {name: 'a'})").await?;
    tx.commit().await?;

    // Fork B off A (existing nested flush materializes A's L0).
    let b = a.fork("b").await?;
    assert_eq!(
        person_count(&b).await?,
        2,
        "B must see P + a (both inherited via unflushed chain) before writing"
    );

    let tx = b.tx().await?;
    tx.execute("CREATE (:Person {name: 'b'})").await?;
    tx.commit().await?;

    assert_eq!(person_count(&b).await?, 3, "B sees P + a + own b");
    assert_eq!(person_count(&a).await?, 2, "A sees P + a, not b");

    db.shutdown().await?;
    Ok(())
}
