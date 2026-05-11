// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 3 — nested-fork crash recovery.
//!
//! The existing `recovery_fork_create_fault.rs` test in `uni-store`
//! exercises Pending-rollback for a single-level fork. Phase 3 added
//! `ForkInfo.parent_fork_id`, so we cover three new scenarios here:
//!
//! 1. A nested fork crashing during creation — recovery rolls back
//!    the partial child without affecting the still-Active parent.
//! 2. `drop_fork_cascade` completes despite swallowed `delete_branch`
//!    errors on individual branches (best-effort, by design — the
//!    registry transition is what matters).
//! 3. After cascade, the on-disk registry has no entries with stale
//!    `parent_fork_id` pointers.

// Rust guideline compliant

use anyhow::Result;
use uni_common::core::schema::DataType;
use uni_db::Uni;
use uni_store::backend::lance_branch::fault_injection;

fn set_fail_create_after(threshold: i64) {
    unsafe {
        std::env::set_var("UNI_FORK_INJECT_FAIL_AFTER", threshold.to_string());
    }
}

fn clear_fail_create_after() {
    unsafe {
        std::env::remove_var("UNI_FORK_INJECT_FAIL_AFTER");
    }
}

fn set_fail_delete_after(threshold: i64) {
    unsafe {
        std::env::set_var("UNI_FORK_INJECT_FAIL_DELETE_AFTER", threshold.to_string());
    }
}

fn clear_fail_delete_after() {
    unsafe {
        std::env::remove_var("UNI_FORK_INJECT_FAIL_DELETE_AFTER");
    }
}

/// Nested fork creation crashes mid-flight; recovery rolls back the
/// leaf and leaves the parent intact.
#[tokio::test]
#[ignore = "mutates the process-wide UNI_FORK_INJECT_FAIL_AFTER counter; run with --test-threads=1 or --run-ignored ignored-only"]
async fn nested_fork_create_crash_rolls_back_leaf_only() -> Result<()> {
    fault_injection::reset();
    clear_fail_create_after();

    let dir = tempfile::tempdir()?;
    let uri = dir.path().display().to_string();

    {
        let db = Uni::open(&uri).build().await?;
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

        // Build the parent fork normally so its registry entry is Active.
        let a = primary.fork("a").await?;
        let tx = a.tx().await?;
        tx.execute("CREATE (:Person {name: 'A-1'})").await?;
        tx.commit().await?;

        // Arm fault hook so the *next* nested create_branch call errors,
        // leaving "b" half-built and Pending in the registry.
        fault_injection::reset();
        set_fail_create_after(1);
        let nested = a.fork("b").await;
        clear_fail_create_after();
        assert!(
            nested.is_err(),
            "nested create should have failed under the fault hook (Session does not impl Debug; success surfaces as a missing panic message)"
        );

        drop(a);
        drop(db);
    }

    // Reopen — recover_forks should roll back "b" and leave "a"
    // untouched.
    fault_injection::reset();
    let db2 = Uni::open(&uri).build().await?;
    let active: Vec<String> = db2
        .list_forks()
        .await
        .into_iter()
        .map(|f| f.name)
        .collect();
    assert!(
        active.contains(&"a".to_string()),
        "parent fork 'a' must survive child-creation crash; got active={active:?}"
    );
    assert!(
        !active.contains(&"b".to_string()),
        "leaf 'b' must be rolled back; got active={active:?}"
    );

    // Re-opening "a" should still work and surface A-1.
    let primary = db2.session();
    let a = primary.fork("a").await?;
    let names: Vec<String> = a
        .query("MATCH (p:Person) RETURN p.name")
        .await?
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("p.name").ok())
        .collect();
    assert!(
        names.iter().any(|n| n == "A-1"),
        "parent fork should still contain its own writes; got {names:?}"
    );

    db2.shutdown().await?;
    Ok(())
}

/// `drop_fork_cascade` is best-effort on individual `delete_branch`
/// calls: failures are logged-and-swallowed, the registry transition
/// is what's load-bearing. This test confirms a cascade succeeds end-
/// to-end even when some branches error out during deletion.
#[tokio::test]
#[ignore = "mutates the process-wide UNI_FORK_INJECT_FAIL_DELETE_AFTER counter; run with --test-threads=1 or --run-ignored ignored-only"]
async fn cascade_completes_despite_swallowed_delete_errors() -> Result<()> {
    fault_injection::reset_delete();
    clear_fail_delete_after();

    let dir = tempfile::tempdir()?;
    let uri = dir.path().display().to_string();

    {
        let db = Uni::open(&uri).build().await?;
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
        drop(_b);
        drop(a);

        // Fail every delete_branch attempt; cascade should still finish
        // and clear the registry because delete errors are swallowed.
        fault_injection::reset_delete();
        set_fail_delete_after(0);
        let cascade = db.drop_fork_cascade("a").await;
        clear_fail_delete_after();
        assert!(
            cascade.is_ok(),
            "cascade should swallow delete errors; got {cascade:?}"
        );

        let active = db.list_forks().await;
        assert!(
            active.is_empty(),
            "registry should be cleared even when deletes failed; got {:?}",
            active.iter().map(|f| &f.name).collect::<Vec<_>>()
        );
        db.shutdown().await?;
    }

    // Reopen — recovery has nothing to do; no forks remain.
    fault_injection::reset_delete();
    let db2 = Uni::open(&uri).build().await?;
    assert!(db2.list_forks().await.is_empty());
    db2.shutdown().await?;

    Ok(())
}
