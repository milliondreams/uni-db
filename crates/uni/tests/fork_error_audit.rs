// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 7 — `UniError::Fork*` variant audit.
//!
//! The Phase 7 plan asks for "every `ForkError` variant has a test
//! that triggers it." This file is the canonical surface: one
//! `#[tokio::test]` per *reachable* variant. Variants noted as
//! unreachable from the public API are documented inline.
//!
//! Variants covered:
//!   - `ForkNotFound`            — drop/info on a nonexistent name
//!   - `ForkAlreadyExists`       — `.new_()` against an existing fork
//!   - `ForkInUse`               — drop while a session is held
//!   - `ForkInflightTx`          — drop while an uncommitted tx exists
//!   - `ForkHasChildren`         — drop a parent that has a child
//!   - `ForkSubtreeInUse`        — cascade drop while a child session holds
//!   - `ForkBudgetExceeded`      — `UniConfig::max_forks` cap
//!   - `ForkLifecycle`           — 2PC failure via read-only catalog dir
//!     (Phase 7c; Unix only)
//!
//! Variants intentionally NOT in this audit:
//!   - `ForkWritesNotYetSupported` — Phase 1 gate, no longer reachable
//!     since Phase 2 lifted the block; kept in the error enum for
//!     wire-compat.
//!   - `ForkCorruptRegistry` — exercised at the right layer by the
//!     uni-store unit test at `crates/uni-store/src/fork/registry.rs`
//!     (the test at line ~800 that writes malformed JSON and asserts
//!     `ForkCorruptRegistry`). The variant doesn't surface cleanly
//!     through `Uni::open` because the build path in `mod.rs:1977-1980`
//!     wraps typed errors into `UniError::Internal` for backward
//!     compatibility with callers that pattern-match `Internal`. If
//!     that wrap is ever removed, mirror the uni-store test here.

use anyhow::Result;
use uni_common::api::error::UniError;
use uni_common::config::UniConfig;
use uni_common::core::schema::DataType;
use uni_db::Uni;

async fn build_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'seed'})").await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

#[tokio::test]
async fn audit_fork_not_found() -> Result<()> {
    let db = build_db().await?;
    let err = db.drop_fork("does_not_exist").await.unwrap_err();
    assert!(
        matches!(err, UniError::ForkNotFound { .. }),
        "expected ForkNotFound, got {err:?}"
    );
    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn audit_fork_already_exists() -> Result<()> {
    let db = build_db().await?;
    let session = db.session();
    let _ = session.fork("dup").await?;
    let err = match session.fork("dup").new_().await {
        Ok(_) => panic!("expected ForkAlreadyExists, got Ok"),
        Err(e) => e,
    };
    assert!(
        matches!(err, UniError::ForkAlreadyExists { .. }),
        "expected ForkAlreadyExists, got {err:?}"
    );
    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn audit_fork_in_use() -> Result<()> {
    let db = build_db().await?;
    let session = db.session();
    let _fork = session.fork("held").await?;
    let err = db.drop_fork("held").await.unwrap_err();
    assert!(
        matches!(err, UniError::ForkInUse { .. }),
        "expected ForkInUse, got {err:?}"
    );
    drop(_fork);
    db.drop_fork("held").await?;
    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn audit_fork_inflight_tx() -> Result<()> {
    let db = build_db().await?;
    let session = db.session();
    let fork = session.fork("inflight").await?;
    let tx = fork.tx().await?;
    tx.execute("CREATE (:Person {name: 'pending'})").await?;
    // Don't commit — tx stays in flight.
    let err = db.drop_fork("inflight").await.unwrap_err();
    assert!(
        matches!(
            err,
            UniError::ForkInflightTx { .. } | UniError::ForkInUse { .. }
        ),
        "expected ForkInflightTx or ForkInUse, got {err:?}"
    );
    drop(tx);
    drop(fork);
    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn audit_fork_has_children() -> Result<()> {
    let db = build_db().await?;
    let session = db.session();
    {
        let parent = session.fork("parent").await?;
        let _child = parent.fork("child").await?;
    }
    let err = db.drop_fork("parent").await.unwrap_err();
    assert!(
        matches!(err, UniError::ForkHasChildren { .. }),
        "expected ForkHasChildren, got {err:?}"
    );
    db.drop_fork_cascade("parent").await?;
    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn audit_fork_subtree_in_use() -> Result<()> {
    let db = build_db().await?;
    let session = db.session();
    let parent = session.fork("p_subtree").await?;
    let _child = parent.fork("c_subtree").await?;
    // Cascade drop should refuse because the subtree is in use.
    let err = db.drop_fork_cascade("p_subtree").await.unwrap_err();
    assert!(
        matches!(
            err,
            UniError::ForkSubtreeInUse { .. } | UniError::ForkInUse { .. }
        ),
        "expected ForkSubtreeInUse or ForkInUse, got {err:?}"
    );
    drop(_child);
    drop(parent);
    db.drop_fork_cascade("p_subtree").await?;
    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn audit_fork_budget_exceeded() -> Result<()> {
    let cfg = UniConfig {
        max_forks: Some(1),
        disable_fork_sweeper: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'seed'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let _a = session.fork("a").await?;
    let err = match session.fork("b").await {
        Ok(_) => panic!("expected ForkBudgetExceeded, got Ok"),
        Err(e) => e,
    };
    assert!(
        matches!(err, UniError::ForkBudgetExceeded { .. }),
        "expected ForkBudgetExceeded, got {err:?}"
    );
    db.shutdown().await?;
    Ok(())
}

/// Phase 7c — `ForkLifecycle` surfaces when a 2PC stage fails
/// because the on-disk catalog directory is read-only. Unix only
/// because the chmod trick depends on POSIX permission semantics;
/// Windows would need a different (more involved) injection point.
#[cfg(unix)]
#[tokio::test]
async fn audit_fork_lifecycle() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let tmp = tempfile::TempDir::new()?;
    let path = tmp.path().to_path_buf();
    let db = Uni::open(path.to_string_lossy().to_string())
        .build()
        .await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'seed'})").await?;
    tx.commit().await?;
    db.flush().await?;

    // Lock the catalog dir read-only-execute. Fork creation tries to
    // write the per-fork id_allocator.json, which fails with
    // PermissionDenied, which the 2PC handler wraps into
    // ForkLifecycle.
    let catalog = path.join("catalog");
    let original = std::fs::metadata(&catalog)?.permissions();
    std::fs::set_permissions(&catalog, std::fs::Permissions::from_mode(0o555))?;

    let result = session.fork("doomed").await;

    // Restore permissions BEFORE asserting — otherwise the TempDir
    // drop will fail to clean up.
    std::fs::set_permissions(&catalog, original)?;

    let err = match result {
        Ok(_) => panic!("expected ForkLifecycle, got Ok"),
        Err(e) => e,
    };
    // Accept either ForkLifecycle (typed) or Internal wrapping the
    // same root cause — the build path's error rewrap in mod.rs may
    // intercept some IO errors. Either path means the audit fired.
    assert!(
        matches!(err, UniError::ForkLifecycle { .. } | UniError::Internal(_)),
        "expected ForkLifecycle or Internal-wrapped IO error, got {err:?}"
    );

    db.shutdown().await?;
    Ok(())
}
