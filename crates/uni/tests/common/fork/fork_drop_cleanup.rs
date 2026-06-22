// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Drop-path hygiene + tombstone safety (production-readiness review H3, M3).
//!
//! H3: dropping a fork must remove its WAL directory (`wal_forks/{id}/`) and id
//! allocator (`catalog/forks/{id}/id_allocator.json`), not just its Lance
//! branches — otherwise dropped forks leak disk monotonically.
//!
//! M3: if a branch delete fails mid-drop, the drop must NOT delete the recovery
//! tombstone (the only anchor that lets boot recovery retry) — it must leave the
//! fork Tombstoned and surface a typed error, so reopen completes the drop.

// Rust guideline compliant

use anyhow::Result;
use uni_common::core::schema::DataType;
use uni_db::Uni;
use uni_store::backend::lance_branch::fault_injection;

/// True if any **file** (not directory) under `root` has `needle` in its path.
/// Files are what consume disk; object_store may leave empty parent dirs behind.
fn file_tree_contains(root: &std::path::Path, needle: &str) -> bool {
    let mut stack = vec![root.to_path_buf()];
    while let Some(p) = stack.pop() {
        if p.is_file() && p.to_string_lossy().contains(needle) {
            return true;
        }
        if let Ok(rd) = std::fs::read_dir(&p) {
            for e in rd.flatten() {
                stack.push(e.path());
            }
        }
    }
    false
}

/// H3: a dropped fork leaves no WAL directory or id-allocator file behind.
#[tokio::test]
async fn drop_fork_removes_wal_dir_and_id_allocator() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().to_path_buf();
    let uri = root.display().to_string();

    let db = Uni::open(&uri).build().await?;
    db.schema()
        .label("N")
        .property("i", DataType::Int)
        .apply()
        .await?;
    let s = db.session();
    let fork = s.fork("x").await?;
    let tx = fork.tx().await?;
    tx.execute("CREATE (:N {i: 1})").await?;
    tx.commit().await?;
    fork.flush().await?; // ensure the fork's id allocator is persisted

    let fork_id = db
        .list_forks()
        .await
        .into_iter()
        .find(|f| f.name == "x")
        .map(|f| f.id.to_string())
        .expect("fork x is registered");

    // After a fork write+flush the per-fork id allocator exists on disk.
    assert!(
        file_tree_contains(&root, "id_allocator.json"),
        "fork id allocator should exist after a fork write"
    );
    assert!(
        file_tree_contains(&root, &fork_id),
        "fork-id-namespaced files should exist before drop"
    );

    drop(fork);
    db.drop_fork("x").await?;

    // After drop, no file namespaced by this fork's id may remain — WAL
    // segments, id allocator, and fork-scoped snapshot manifests (review H3).
    assert!(
        !file_tree_contains(&root, &fork_id),
        "drop must remove all files namespaced by the fork id (WAL + allocator + snapshots)"
    );

    db.shutdown().await?;
    Ok(())
}

/// M3: a branch-delete failure during drop must leave the fork Tombstoned (not
/// finished), and a later reopen must complete the drop via recovery.
#[tokio::test]
#[ignore = "mutates the process-wide UNI_FORK_INJECT_FAIL_DELETE_AFTER counter; run with --test-threads=1 or --run-ignored ignored-only"]
async fn drop_fork_keeps_tombstone_on_branch_delete_failure_then_recovers() -> Result<()> {
    fault_injection::reset();
    let dir = tempfile::tempdir()?;
    let uri = dir.path().display().to_string();

    {
        let db = Uni::open(&uri).build().await?;
        db.schema()
            .label("N")
            .property("i", DataType::Int)
            .apply()
            .await?;
        let s = db.session();
        let fork = s.fork("x").await?;
        let tx = fork.tx().await?;
        tx.execute("CREATE (:N {i: 1})").await?;
        tx.commit().await?;
        fork.flush().await?;
        drop(fork);

        // Arm the fault so every branch delete fails.
        fault_injection::reset();
        unsafe {
            std::env::set_var("UNI_FORK_INJECT_FAIL_DELETE_AFTER", "0");
        }
        let res = db.drop_fork("x").await;
        unsafe {
            std::env::remove_var("UNI_FORK_INJECT_FAIL_DELETE_AFTER");
        }

        // M3 core: the drop must report failure rather than silently finishing
        // (the old code warned-and-continued, then deleted the tombstone).
        assert!(
            res.is_err(),
            "drop must fail when a branch delete fails, not silently finish (review M3)"
        );
        // The fork is left Tombstoned (recoverable): re-opening it errors.
        assert!(
            s.fork("x").await.is_err(),
            "fork must remain tombstoned (recoverable), not finished"
        );
        drop(db);
    }

    // Reopen with no fault: recovery completes the interrupted drop.
    fault_injection::reset();
    let db2 = Uni::open(&uri).build().await?;
    let active: Vec<String> = db2.list_forks().await.into_iter().map(|f| f.name).collect();
    assert!(
        !active.contains(&"x".to_string()),
        "recovery must complete the interrupted drop; still present: {active:?}"
    );
    db2.shutdown().await?;
    Ok(())
}
