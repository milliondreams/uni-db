// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 2 follow-up — fork-local strict-schema overlay growth.
//!
//! Verifies that `Session::fork_schema()` lets a forked session
//! introduce labels and edge types that:
//! - Pass strict-schema validation on the fork's writes.
//! - Are invisible to primary's strict-schema view.
//! - Survive a process restart via the persisted overlay file.
//! - Are visible to sibling sessions on the same fork through the
//!   Day 8 `UniInner` cache and the `ArcSwap`'d overlay.

// Rust guideline compliant

use anyhow::Result;
use uni_common::api::error::UniError;
use uni_common::config::UniConfig;
use uni_common::core::schema::DataType;
use uni_db::Uni;

fn strict_config() -> UniConfig {
    UniConfig { strict_schema: true, ..UniConfig::default() }
}

#[tokio::test]
async fn fork_local_label_passes_strict_validation_on_fork_only() -> Result<()> {
    let db = Uni::in_memory().config(strict_config()).build().await?;
    db.schema()
        .label("Item")
        .property("kind", DataType::String)
        .apply()
        .await?;

    // Seed primary so the vertices_Item dataset exists at fork-point.
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Item {kind: 'seed'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let forked = primary.fork("scenario").await?;

    // Fork declares a brand-new label that primary doesn't know about.
    forked
        .fork_schema()
        .label("OnlyOnFork")
        .description("fork-local strict-schema label")
        .apply()
        .await?;

    // Fork can now write to OnlyOnFork even in strict mode.
    let tx = forked.tx().await?;
    tx.execute("CREATE (:OnlyOnFork)").await?;
    tx.commit().await?;

    let names = forked
        .query("MATCH (n:OnlyOnFork) RETURN count(n) AS c")
        .await?
        .rows()
        .first()
        .and_then(|r| r.get::<i64>("c").ok())
        .unwrap_or(0);
    assert_eq!(names, 1, "fork should see its own write");

    // Primary still rejects writes to OnlyOnFork — its schema view is
    // unchanged, and strict mode still applies.
    let tx = primary.tx().await?;
    let err = tx.execute("CREATE (:OnlyOnFork)").await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("OnlyOnFork") || msg.contains("strict_schema") || msg.contains("not in"),
        "primary write to fork-only label must be rejected; got: {msg}"
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn fork_local_edge_type_passes_strict_validation_on_fork_only() -> Result<()> {
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

    let forked = primary.fork("edge_scenario").await?;

    forked
        .fork_schema()
        .edge_type("ONLY_ON_FORK", &["Item"], &["Item"])
        .apply()
        .await?;

    let tx = forked.tx().await?;
    tx.execute(
        "MATCH (a:Item), (b:Item) WHERE a.kind = 'seed' AND b.kind = 'seed' \
         CREATE (a)-[:ONLY_ON_FORK]->(b)",
    )
    .await?;
    tx.commit().await?;

    // Primary doesn't know the edge type; strict rejects.
    let tx = primary.tx().await?;
    let err = tx
        .execute(
            "MATCH (a:Item), (b:Item) WHERE a.kind = 'seed' AND b.kind = 'seed' \
             CREATE (a)-[:ONLY_ON_FORK]->(b)",
        )
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("ONLY_ON_FORK") || msg.contains("strict_schema") || msg.contains("not in"),
        "primary write of fork-only edge type must be rejected; got: {msg}"
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn fork_local_label_survives_restart() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let uri = dir.path().to_str().unwrap().to_string();

    {
        let db = Uni::open(&uri).config(strict_config()).build().await?;
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

        let forked = session.fork("persist").await?;
        forked
            .fork_schema()
            .label("OnlyOnFork")
            .apply()
            .await?;
        let tx = forked.tx().await?;
        tx.execute("CREATE (:OnlyOnFork)").await?;
        tx.commit().await?;
        db.shutdown().await?;
    }

    {
        let db = Uni::open(&uri).config(strict_config()).build().await?;
        let session = db.session();
        let forked = session.fork("persist").await?;

        // The label survives the restart through the overlay file —
        // strict mode would reject this query if the label weren't
        // present in the fork's merged schema.
        let count = forked
            .query("MATCH (n:OnlyOnFork) RETURN count(n) AS c")
            .await?
            .rows()
            .first()
            .and_then(|r| r.get::<i64>("c").ok())
            .unwrap_or(0);
        assert_eq!(count, 1, "fork's pre-restart write must survive reopen");

        // Primary's view is still untouched.
        let primary_err = session.tx().await?.execute("CREATE (:OnlyOnFork)").await;
        assert!(
            primary_err.is_err(),
            "primary must still reject the fork-only label after restart"
        );

        db.shutdown().await?;
    }

    Ok(())
}

#[tokio::test]
async fn sibling_sessions_see_overlay_growth_immediately() -> Result<()> {
    let db = Uni::in_memory().config(strict_config()).build().await?;
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

    let session_a = session.fork("sibling").await?;
    let session_b = session.fork("sibling").await?;

    // A declares a new label.
    session_a
        .fork_schema()
        .label("OnlyOnFork")
        .apply()
        .await?;

    // B (which shares the same UniInner via the Day 8 cache) can
    // write to OnlyOnFork without re-declaring it: the in-memory
    // SchemaManager is shared and the ArcSwap'd overlay was updated
    // atomically.
    let tx = session_b.tx().await?;
    tx.execute("CREATE (:OnlyOnFork)").await?;
    tx.commit().await?;

    let count = session_b
        .query("MATCH (n:OnlyOnFork) RETURN count(n) AS c")
        .await?
        .rows()
        .first()
        .and_then(|r| r.get::<i64>("c").ok())
        .unwrap_or(0);
    assert_eq!(count, 1);

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn fork_schema_on_primary_session_errors() -> Result<()> {
    let db = Uni::in_memory().config(strict_config()).build().await?;
    db.schema()
        .label("Item")
        .property("kind", DataType::String)
        .apply()
        .await?;
    let session = db.session();

    let err = session
        .fork_schema()
        .label("ShouldNotExist")
        .apply()
        .await
        .unwrap_err();
    assert!(
        matches!(err, UniError::InvalidArgument { .. }),
        "fork_schema() on a primary session must error with InvalidArgument; got {err:?}"
    );

    db.shutdown().await?;
    Ok(())
}
