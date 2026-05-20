// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 2 Day 11 — `drop_fork` surfaces in-flight transactions as a
//! typed error rather than silently corrupting state.
//!
//! The check uses `mutation_count > 0` on the fork writer's current L0
//! as a proxy for "an open tx has uncommitted state." The proxy is
//! imprecise: a freshly opened tx that hasn't yet executed any
//! mutation slips through (Phase 4 lands a true drain protocol). The
//! tests pin the proxy contract — that is, the obvious "you forgot to
//! commit" case errors with `ForkInflightTx`, not `ForkInUse` and not
//! a corrupted registry.

// Rust guideline compliant

use anyhow::Result;
use uni_common::api::error::UniError;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn drop_fork_with_uncommitted_tx_errors_inflight() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("kind", DataType::String)
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Item {kind: 'seed'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let forked = session.fork("inflight").await?;
    let tx = forked.tx().await?;
    tx.execute("CREATE (:Item {kind: 'pending'})").await?;
    // Deliberately do NOT commit. The tx holds uncommitted state on
    // the fork writer's L0 (mutation_count > 0).

    let err = db.drop_fork("inflight").await.unwrap_err();
    assert!(
        matches!(err, UniError::ForkInflightTx { .. }),
        "expected ForkInflightTx, got {err:?}"
    );

    // Cleanup: commit the tx and confirm drop now succeeds.
    tx.commit().await?;
    drop(forked);
    db.drop_fork("inflight").await?;

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn drop_fork_after_commit_succeeds() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("kind", DataType::String)
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Item {kind: 'seed'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let forked = session.fork("post_commit").await?;
    let tx = forked.tx().await?;
    tx.execute("CREATE (:Item {kind: 'committed'})").await?;
    tx.commit().await?;
    drop(forked);

    db.drop_fork("post_commit").await?;

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn drop_fork_with_alive_session_no_tx_still_errors_inuse() -> Result<()> {
    // Phase 1's `ForkInUse` semantics must be preserved: a held
    // session with no in-flight tx still blocks drop. Day 11 only
    // *adds* a more specific signal for the in-flight case; it
    // doesn't replace `ForkInUse`.
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("kind", DataType::String)
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Item {kind: 'seed'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let forked = session.fork("held").await?;

    let err = db.drop_fork("held").await.unwrap_err();
    assert!(
        matches!(err, UniError::ForkInUse { .. }),
        "expected ForkInUse for held-session-no-tx, got {err:?}"
    );

    drop(forked);
    db.drop_fork("held").await?;

    db.shutdown().await?;
    Ok(())
}
