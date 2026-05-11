// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 4a — pin/refresh work on forked sessions.
//!
//! Spec §4.2 contract: `Session::pin_to_version`, `refresh`, and
//! `is_pinned` apply correctly to forked sessions. Implementation:
//! the session's `live_db()` already routes through the fork-scoped
//! `UniInner` when forked, so `at_snapshot(...)` resolves against the
//! correct database view.

// Rust guideline compliant

use anyhow::Result;
use uni_common::core::schema::DataType;
use uni_db::Uni;

#[tokio::test]
async fn forked_session_pin_and_refresh() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'baseline'})").await?;
    tx.commit().await?;
    db.flush().await?;

    // Create a fork and grab a snapshot of primary's current state via
    // `db.create_snapshot` (snapshots live at the database level; pin
    // resolves against the fork-scoped `live_db` regardless).
    let mut fork = primary.fork("scenario").await?;
    let snapshot_id = db.create_snapshot("checkpoint").await?;

    // Write more after snapshot.
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'after-snapshot'})").await?;
    tx.commit().await?;
    db.flush().await?;

    // Pin the forked session to the snapshot. is_pinned reports true.
    fork.pin_to_version(&snapshot_id).await?;
    assert!(fork.is_pinned());

    // Pinned session is read-only — writes should reject.
    let tx_result = fork.tx().await;
    assert!(
        tx_result.is_err(),
        "pinned forked session must reject writes"
    );

    // Refresh restores live state; is_pinned flips back.
    fork.refresh().await?;
    assert!(!fork.is_pinned());

    // After refresh, writes succeed again.
    let tx = fork.tx().await?;
    tx.execute("CREATE (:Person {name: 'after-refresh'})").await?;
    tx.commit().await?;

    db.shutdown().await?;
    Ok(())
}
