// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 3 — nested forks.
//!
//! Validates that `forked.fork(name)` creates a child fork whose reads
//! chain through Lance `base_paths` to its parent's branch and whose
//! writes/drops/schema additions remain isolated at every level.

// Rust guideline compliant

use anyhow::Result;
use uni_common::api::error::UniError;
use uni_common::config::UniConfig;
use uni_common::core::schema::DataType;
use uni_db::{Row, Uni};

fn count_names(rows: &[Row]) -> Vec<String> {
    rows.iter()
        .filter_map(|r| r.get::<String>("p.name").ok())
        .collect()
}

#[tokio::test]
async fn nested_fork_chain_resolves_through_both_ancestors() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    // Primary seed.
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'Primary-Alice'})").await?;
    tx.commit().await?;
    db.flush().await?;

    // Level 1 fork: A.
    let a = primary.fork("scenario_a").await?;
    let tx = a.tx().await?;
    tx.execute("CREATE (:Person {name: 'A-Bob'})").await?;
    tx.commit().await?;

    // Level 2 fork: B is a child of A.
    let b = a.fork("scenario_b").await?;
    assert!(b.is_forked(), "B should be a forked session");

    let tx = b.tx().await?;
    tx.execute("CREATE (:Person {name: 'B-Carol'})").await?;
    tx.commit().await?;

    // B sees: Primary-Alice + A-Bob (at B-fork-point) + B-Carol.
    let names = count_names(b.query("MATCH (p:Person) RETURN p.name").await?.rows());
    assert_eq!(
        names.len(),
        3,
        "B should see all three rows (chain through A and primary); got {names:?}"
    );
    assert!(names.iter().any(|n| n == "Primary-Alice"));
    assert!(names.iter().any(|n| n == "A-Bob"));
    assert!(names.iter().any(|n| n == "B-Carol"));

    // A sees: Primary-Alice + A-Bob (NOT B-Carol).
    let names = count_names(a.query("MATCH (p:Person) RETURN p.name").await?.rows());
    assert_eq!(names.len(), 2, "A must not see B's writes; got {names:?}");
    assert!(!names.iter().any(|n| n == "B-Carol"));

    // Primary sees only its own row.
    let names = count_names(primary.query("MATCH (p:Person) RETURN p.name").await?.rows());
    assert_eq!(names.len(), 1, "primary must see only its own row; got {names:?}");

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn nested_fork_snapshot_isolation_at_each_level() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'P-1'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let a = primary.fork("a").await?;
    let tx = a.tx().await?;
    tx.execute("CREATE (:Person {name: 'A-1'})").await?;
    tx.commit().await?;

    let b = a.fork("b").await?;

    // After B is created, write on A — B must not see it.
    let tx = a.tx().await?;
    tx.execute("CREATE (:Person {name: 'A-after-B'})").await?;
    tx.commit().await?;

    // After B is created, write on primary — neither A nor B see it.
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'P-after-B'})").await?;
    tx.commit().await?;

    let b_names = count_names(b.query("MATCH (p:Person) RETURN p.name").await?.rows());
    assert!(
        !b_names.iter().any(|n| n == "A-after-B"),
        "B must not see A's post-creation writes; got {b_names:?}"
    );
    assert!(
        !b_names.iter().any(|n| n == "P-after-B"),
        "B must not see primary's post-creation writes; got {b_names:?}"
    );

    let a_names = count_names(a.query("MATCH (p:Person) RETURN p.name").await?.rows());
    assert!(
        !a_names.iter().any(|n| n == "P-after-B"),
        "A must not see primary's post-A-creation writes; got {a_names:?}"
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn nested_sibling_forks_are_isolated() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'root'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let a = primary.fork("a").await?;
    let tx = a.tx().await?;
    tx.execute("CREATE (:Person {name: 'a-only'})").await?;
    tx.commit().await?;

    let b1 = a.fork("b1").await?;
    let b2 = a.fork("b2").await?;

    let tx = b1.tx().await?;
    tx.execute("CREATE (:Person {name: 'b1-only'})").await?;
    tx.commit().await?;
    let tx = b2.tx().await?;
    tx.execute("CREATE (:Person {name: 'b2-only'})").await?;
    tx.commit().await?;

    let b1_names = count_names(b1.query("MATCH (p:Person) RETURN p.name").await?.rows());
    let b2_names = count_names(b2.query("MATCH (p:Person) RETURN p.name").await?.rows());

    assert!(b1_names.iter().any(|n| n == "b1-only"));
    assert!(!b1_names.iter().any(|n| n == "b2-only"), "b1 saw b2's row");
    assert!(b2_names.iter().any(|n| n == "b2-only"));
    assert!(!b2_names.iter().any(|n| n == "b1-only"), "b2 saw b1's row");

    // Both still see ancestor state.
    assert!(b1_names.iter().any(|n| n == "a-only"));
    assert!(b2_names.iter().any(|n| n == "a-only"));
    assert!(b1_names.iter().any(|n| n == "root"));
    assert!(b2_names.iter().any(|n| n == "root"));

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn drop_fork_refuses_when_children_exist() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'root'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let a = primary.fork("a").await?;
    let _b = a.fork("b").await?;

    // Drop A while B still exists → error with ForkHasChildren.
    let err = db.drop_fork("a").await.unwrap_err();
    match err {
        UniError::ForkHasChildren { name, children } => {
            assert_eq!(name, "a");
            assert!(
                children.iter().any(|c| c == "b"),
                "children list should include 'b'; got {children:?}"
            );
        }
        other => panic!("expected ForkHasChildren, got {other:?}"),
    }

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn drop_fork_cascade_removes_whole_subtree() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'root'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let a = primary.fork("a").await?;
    let b = a.fork("b").await?;
    let _c = b.fork("c").await?;
    drop(a);
    drop(b);
    drop(_c);

    db.drop_fork_cascade("a").await?;

    // Registry should now have no forks named a, b, or c.
    let active = db.list_forks().await;
    let names: Vec<String> = active.into_iter().map(|i| i.name).collect();
    assert!(
        !names.iter().any(|n| n == "a" || n == "b" || n == "c"),
        "cascade should remove the whole subtree; remaining={names:?}"
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn drop_fork_cascade_refuses_when_subtree_in_use() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'root'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let a = primary.fork("a").await?;
    let b = a.fork("b").await?; // keep b alive
    drop(a);

    // b is alive → cascade must refuse before any tombstoning.
    let err = db.drop_fork_cascade("a").await.unwrap_err();
    match err {
        UniError::ForkSubtreeInUse { blockers } => {
            assert!(
                blockers.iter().any(|s| s.contains("b")),
                "expected b in blockers; got {blockers:?}"
            );
        }
        other => panic!("expected ForkSubtreeInUse, got {other:?}"),
    }

    // After dropping b, cascade succeeds.
    drop(b);
    db.drop_fork_cascade("a").await?;

    db.shutdown().await?;
    Ok(())
}

fn strict_config() -> UniConfig {
    UniConfig { strict_schema: true, ..UniConfig::default() }
}

#[tokio::test]
async fn nested_fork_strict_schema_composition() -> Result<()> {
    // Primary has Item. A adds OnlyOnA. B (child of A) adds OnlyOnB.
    // B must see both fork-only labels via the chained overlay; A must
    // see only OnlyOnA; primary rejects both fork-only labels.
    let db = Uni::in_memory().config(strict_config()).build().await?;
    db.schema()
        .label("Item")
        .property("kind", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Item {kind: 'seed'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let a = primary.fork("a").await?;
    a.fork_schema()
        .label("OnlyOnA")
        .description("A-only")
        .apply()
        .await?;

    let b = a.fork("b").await?;
    b.fork_schema()
        .label("OnlyOnB")
        .description("B-only")
        .apply()
        .await?;

    // B writes to both fork-only labels.
    let tx = b.tx().await?;
    tx.execute("CREATE (:OnlyOnA {})").await?;
    tx.execute("CREATE (:OnlyOnB {})").await?;
    tx.commit().await?;

    // A writes to OnlyOnA succeed; OnlyOnB fails strict check on A.
    let tx = a.tx().await?;
    tx.execute("CREATE (:OnlyOnA {})").await?;
    tx.commit().await?;
    {
        let tx = a.tx().await?;
        let bad = tx.execute("CREATE (:OnlyOnB {})").await;
        assert!(
            bad.is_err(),
            "OnlyOnB must not pass strict check on A (B-only label)"
        );
        tx.rollback();
    }

    // Primary rejects both fork-only labels.
    {
        let tx = primary.tx().await?;
        assert!(tx.execute("CREATE (:OnlyOnA {})").await.is_err());
        tx.rollback();
    }
    {
        let tx = primary.tx().await?;
        assert!(tx.execute("CREATE (:OnlyOnB {})").await.is_err());
        tx.rollback();
    }

    db.shutdown().await?;
    Ok(())
}
