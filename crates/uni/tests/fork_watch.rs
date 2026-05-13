// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 4a — `Session::watch` is fork-isolated.
//!
//! Spec §4.3 contract: a watch on a forked session sees only that
//! fork's commits; primary's watch never sees fork commits; sibling
//! forks don't bleed into each other.
//!
//! Implementation note: each `UniInner` (primary or fork-scoped)
//! owns its own `commit_tx: broadcast::Sender`, so the isolation is
//! by construction. These tests assert the contract end-to-end.

// Rust guideline compliant

use anyhow::Result;
use std::time::Duration;
use uni_common::core::schema::DataType;
use uni_db::{CommitNotification, CommitStream, Uni};

async fn next_within(stream: &mut CommitStream, timeout: Duration) -> Option<CommitNotification> {
    tokio::time::timeout(timeout, stream.next())
        .await
        .ok()
        .flatten()
}

#[tokio::test]
async fn fork_watch_isolated_from_primary() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'seed'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let fork = primary.fork("scenario").await?;
    let mut primary_watch = primary.watch();
    let mut fork_watch = fork.watch();

    // Commit on the fork — only the fork's stream should fire.
    let tx = fork.tx().await?;
    tx.execute("CREATE (:Person {name: 'fork-only'})").await?;
    tx.commit().await?;

    let fork_notif = next_within(&mut fork_watch, Duration::from_millis(500)).await;
    assert!(
        fork_notif.is_some(),
        "fork watch should fire on fork commit"
    );

    let primary_notif = next_within(&mut primary_watch, Duration::from_millis(200)).await;
    assert!(
        primary_notif.is_none(),
        "primary watch must not see fork commits"
    );

    // Commit on primary — only primary's stream should fire.
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'primary-only'})")
        .await?;
    tx.commit().await?;

    let primary_notif = next_within(&mut primary_watch, Duration::from_millis(500)).await;
    assert!(
        primary_notif.is_some(),
        "primary watch should fire on primary commit"
    );

    let fork_notif = next_within(&mut fork_watch, Duration::from_millis(200)).await;
    assert!(
        fork_notif.is_none(),
        "fork watch must not see primary commits"
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn sibling_forks_have_isolated_watches() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'seed'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let a = primary.fork("a").await?;
    let b = primary.fork("b").await?;
    let mut a_watch = a.watch();
    let mut b_watch = b.watch();

    let tx = a.tx().await?;
    tx.execute("CREATE (:Person {name: 'on-a'})").await?;
    tx.commit().await?;

    let a_notif = next_within(&mut a_watch, Duration::from_millis(500)).await;
    assert!(a_notif.is_some());
    let b_notif = next_within(&mut b_watch, Duration::from_millis(200)).await;
    assert!(
        b_notif.is_none(),
        "sibling fork b must not see fork a commits"
    );

    db.shutdown().await?;
    Ok(())
}
