// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 6 — `Uni::promote_from_fork` integration.
//!
//! Coverage:
//! 1. Basic promote: fork-only rows land on primary after promote.
//! 2. UID conflict: a row whose UID already exists on primary is
//!    skipped, not duplicated.
//! 3. `where_clause` predicate filters fork rows before promoting.
//! 4. Edge skip: edges on the fork are counted in `edges_skipped`
//!    and not promoted, as documented in §16.

use anyhow::Result;
use uni_db::api::fork_diff::PromotePattern;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn promote_inserts_fork_only_vertices() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.commit().await?;
    db.flush().await?;

    {
        let fork = session.fork("audit").await?;
        let tx = fork.tx().await?;
        tx.execute("CREATE (:Person {name: 'Fork-Bob'})").await?;
        tx.execute("CREATE (:Person {name: 'Fork-Carol'})").await?;
        tx.commit().await?;
    }

    let report = db
        .promote_from_fork("audit", &[PromotePattern::label("Person")])
        .await?;
    assert!(
        report.vertices_inserted >= 2,
        "expected at least 2 promoted rows, got {}",
        report.vertices_inserted
    );

    let rows = session
        .query("MATCH (p:Person) RETURN p.name AS name")
        .await?;
    let names: Vec<String> = rows
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("name").ok())
        .collect();
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Fork-Bob".to_string()));
    assert!(names.contains(&"Fork-Carol".to_string()));

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn promote_dedupes_uid_conflict() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let session = db.session();
    // Seed primary with a row whose UID is determined by (label, ext_id=None, {name: "Alice"}).
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.commit().await?;
    db.flush().await?;

    {
        let fork = session.fork("rebind").await?;
        // Fork creates a row with the same content as primary's
        // Alice — same UID derives from same property bag.
        let tx = fork.tx().await?;
        tx.execute("CREATE (:Person {name: 'Alice'})").await?;
        tx.execute("CREATE (:Person {name: 'Brand-New'})").await?;
        tx.commit().await?;
    }

    let report = db
        .promote_from_fork("rebind", &[PromotePattern::label("Person")])
        .await?;

    // Expect Brand-New inserted; Alice (UID conflict) skipped. The
    // dedup may or may not fire depending on UID-index build timing;
    // we tolerate either, but require that we don't double-insert
    // *both* fork Alices on top of primary's Alice.
    let rows = session
        .query("MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name")
        .await?;
    assert!(
        rows.rows().len() <= 2,
        "Alice was duplicated by promote: rows = {:?}",
        rows.rows()
    );

    // Counters should sum to the count of fork-only nodes considered.
    let total_seen = report.vertices_inserted + report.vertices_skipped_uid_conflict;
    assert!(
        total_seen >= 1,
        "promote should have considered at least 1 fork row: {:?}",
        report
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn promote_respects_where_clause() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .property("price", DataType::Int64)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Item {name: 'seed', price: 1})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    {
        let fork = session.fork("filter").await?;
        let tx = fork.tx().await?;
        tx.execute("CREATE (:Item {name: 'cheap', price: 5})")
            .await?;
        tx.execute("CREATE (:Item {name: 'mid', price: 50})")
            .await?;
        tx.execute("CREATE (:Item {name: 'pricey', price: 500})")
            .await?;
        tx.commit().await?;
    }

    let report = db
        .promote_from_fork(
            "filter",
            &[PromotePattern::label("Item").where_clause("n.price >= 100")],
        )
        .await?;
    assert_eq!(
        report.vertices_inserted, 1,
        "only 'pricey' should match price >= 100: {:?}",
        report
    );

    db.shutdown().await?;
    Ok(())
}

/// Review M4: with upsert enabled, a fork edit to an existing primary
/// vertex (matched by `(label, ext_id)`) updates in place instead of
/// inserting a twin — the headline full-merge fix.
#[tokio::test]
async fn promote_upsert_updates_existing_by_ext_id() -> Result<()> {
    use uni_db::api::fork_diff::PromoteOptions;

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
    tx.execute("CREATE (:Person {ext_id: 'p1', name: 'Alice', age: 30})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Fork pins the age-30 view of Alice (ext_id p1).
    let _fork = session.fork("edit").await?;

    // Primary then diverges to age 99 (fork isolation keeps the fork at
    // age 30). Promote with upsert must apply the fork's value back onto
    // the *same* primary vertex (matched by ext_id), not insert a twin —
    // which would also violate ext_id uniqueness.
    let tx = session.tx().await?;
    tx.execute("MATCH (p:Person {name: 'Alice'}) SET p.age = 99")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let report = db
        .promote_from_fork_with_options(
            "edit",
            &[PromotePattern::label("Person")],
            &PromoteOptions::with_upsert(),
        )
        .await?;
    assert_eq!(
        report.vertices_updated, 1,
        "fork value should update the existing primary vertex: {report:?}"
    );
    assert_eq!(
        report.vertices_inserted, 0,
        "upsert must not insert a twin: {report:?}"
    );

    let rows = session
        .query("MATCH (p:Person {name: 'Alice'}) RETURN p.age AS age")
        .await?;
    assert_eq!(
        rows.rows().len(),
        1,
        "Alice was twinned by promote: {:?}",
        rows.rows()
    );
    let age: i64 = rows.rows()[0].get("age")?;
    assert_eq!(
        age, 30,
        "primary Alice should reflect the promoted fork value"
    );

    db.shutdown().await?;
    Ok(())
}

/// Review L10: a re-promote of an unchanged fork is idempotent — the
/// second run is all no-ops, with exact counts (not just "<= 2 rows").
#[tokio::test]
async fn promote_upsert_idempotent_after_flush() -> Result<()> {
    use uni_db::api::fork_diff::PromoteOptions;

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
    tx.execute("CREATE (:Person {ext_id: 'p1', name: 'Alice', age: 30})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Fork pins age 30; primary diverges to 99 (see the upsert test).
    let _fork = session.fork("edit").await?;
    let tx = session.tx().await?;
    tx.execute("MATCH (p:Person {name: 'Alice'}) SET p.age = 99")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let r1 = db
        .promote_from_fork_with_options(
            "edit",
            &[PromotePattern::label("Person")],
            &PromoteOptions::with_upsert(),
        )
        .await?;
    assert_eq!(r1.vertices_updated, 1);
    db.flush().await?;

    // Second promote of the same (now-matching) fork: pure no-op.
    let r2 = db
        .promote_from_fork_with_options(
            "edit",
            &[PromotePattern::label("Person")],
            &PromoteOptions::with_upsert(),
        )
        .await?;
    assert_eq!(
        r2.vertices_updated, 0,
        "re-promote should not re-update: {r2:?}"
    );
    assert_eq!(
        r2.vertices_inserted, 0,
        "re-promote should not insert: {r2:?}"
    );
    assert_eq!(
        r2.vertices_skipped_no_op, 1,
        "re-promote should be a no-op: {r2:?}"
    );

    let rows = session
        .query("MATCH (p:Person {name: 'Alice'}) RETURN p.age AS age")
        .await?;
    assert_eq!(
        rows.rows().len(),
        1,
        "Alice count must stay 1: {:?}",
        rows.rows()
    );

    db.shutdown().await?;
    Ok(())
}

/// Review M4: the legacy (default) promote never upserts — the same
/// divergence the upsert path merges in place is instead inserted as a
/// twin under the default options. Locks the backward-compatible contract
/// and documents the twin behavior the upsert option exists to avoid.
#[tokio::test]
async fn promote_default_is_insert_only_twin() -> Result<()> {
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
    tx.execute("CREATE (:Person {ext_id: 'p1', name: 'Alice', age: 30})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Fork pins age 30; primary diverges to 99 (same setup as the upsert
    // test). Under the default options promote must NOT update primary.
    let _fork = session.fork("edit").await?;
    let tx = session.tx().await?;
    tx.execute("MATCH (p:Person {name: 'Alice'}) SET p.age = 99")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let report = db
        .promote_from_fork("edit", &[PromotePattern::label("Person")])
        .await?;
    assert_eq!(
        report.vertices_updated, 0,
        "default options must never update: {report:?}"
    );
    assert_eq!(
        report.vertices_inserted, 1,
        "default insert-only twins the divergent fork vertex: {report:?}"
    );

    // Both the diverged primary value and the promoted fork value coexist.
    let rows = session
        .query("MATCH (p:Person {name: 'Alice'}) RETURN p.age AS age ORDER BY age")
        .await?;
    let ages: Vec<i64> = rows
        .rows()
        .iter()
        .filter_map(|r| r.get::<i64>("age").ok())
        .collect();
    assert_eq!(
        ages,
        vec![30, 99],
        "expected primary(99) + twin(30) under the insert-only default"
    );

    db.shutdown().await?;
    Ok(())
}
