// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 2 Day 10 — fork sessions can write to labels that don't have
//! a branched dataset at fork-point.
//!
//! Two scenarios covered:
//!
//! 1. **Label declared on primary, no rows at fork-point.** Primary
//!    declares `OnlyOnFork` in schema but never writes a row before
//!    forking. The fork then writes one. `BranchedBackend` materializes
//!    a branch on the fork via `ensure_branch_for_new` (which falls
//!    through to `create_dataset_then_branch` because the dataset
//!    doesn't exist on disk anywhere). The fork sees its own write and
//!    primary remains empty.
//!
//! 2. **Restart preserves the fork's view.** After the dynamic-branch
//!    is registered with the registry, reopening the database recovers
//!    the same dataset → branch mapping so the fork's reads still
//!    resolve correctly.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn fork_writes_to_label_without_fork_point_dataset() -> Result<()> {
    let config = uni_db::UniConfig {
        auto_flush_threshold: 1, // force per-commit flush so the
                                 // branched-write path is exercised
        ..Default::default()
    };
    let db = Uni::in_memory().config(config).build().await?;
    db.schema()
        .label("OnlyOnFork")
        .property("kind", DataType::String)
        .apply()
        .await?;

    // No primary writes — vertices_OnlyOnFork doesn't exist on disk.

    let session = db.session();
    let forked = session.fork("new_label").await?;

    // Fork writes the first row for OnlyOnFork. Without Day 10 this
    // bails inside `BranchedBackend::write` because the fork has no
    // branch and the dataset doesn't exist on primary either.
    let tx = forked.tx().await?;
    tx.execute("CREATE (:OnlyOnFork {kind: 'fork-only-1'})")
        .await?;
    tx.commit().await?;

    let names: Vec<String> = forked
        .query("MATCH (n:OnlyOnFork) RETURN n.kind")
        .await?
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("n.kind").ok())
        .collect();
    assert_eq!(names, vec!["fork-only-1".to_string()]);

    // Primary sees zero rows for the same label.
    let primary_names: Vec<String> = session
        .query("MATCH (n:OnlyOnFork) RETURN n.kind")
        .await?
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("n.kind").ok())
        .collect();
    assert!(
        primary_names.is_empty(),
        "primary must not see fork-only writes; got {primary_names:?}"
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn fork_dynamic_branch_persists_across_reopen() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let uri = dir.path().to_str().unwrap().to_string();

    let config = uni_db::UniConfig {
        auto_flush_threshold: 1,
        ..Default::default()
    };

    // First boot: declare a label with no rows on primary, fork, write
    // on the fork, shut down.
    {
        let db = Uni::open(&uri).config(config.clone()).build().await?;
        db.schema()
            .label("OnlyOnFork")
            .property("kind", DataType::String)
            .apply()
            .await?;
        let session = db.session();
        let forked = session.fork("persisted").await?;
        let tx = forked.tx().await?;
        tx.execute("CREATE (:OnlyOnFork {kind: 'pre-restart'})")
            .await?;
        tx.commit().await?;
        db.shutdown().await?;
    }

    // Second boot: re-open the same path, re-attach to the fork. The
    // pre-restart row must still be visible — the fork's dynamic
    // branch entry was persisted via `register_dataset_branch` so the
    // ForkInfo on disk now lists the dataset.
    {
        let db = Uni::open(&uri).config(config).build().await?;
        let session = db.session();
        let forked = session.fork("persisted").await?;
        let names: Vec<String> = forked
            .query("MATCH (n:OnlyOnFork) RETURN n.kind")
            .await?
            .rows()
            .iter()
            .filter_map(|r| r.get::<String>("n.kind").ok())
            .collect();
        assert_eq!(
            names,
            vec!["pre-restart".to_string()],
            "fork's pre-restart write must survive reopen"
        );
        db.shutdown().await?;
    }

    Ok(())
}
