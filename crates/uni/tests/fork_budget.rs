// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 4a — `UniConfig::max_forks` budget enforcement.
//!
//! Counts include Active + Pending + Tombstoned (per design — tombstoned
//! forks still hold branch state on disk until recovery completes).

// Rust guideline compliant

use anyhow::Result;
use uni_common::api::error::UniError;
use uni_common::config::UniConfig;
use uni_common::core::schema::DataType;
use uni_db::Uni;

#[tokio::test]
async fn budget_blocks_creation_at_cap() -> Result<()> {
    let cfg = UniConfig {
        max_forks: Some(2),
        disable_fork_sweeper: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;
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

    let _a = primary.fork("a").await?;
    let _b = primary.fork("b").await?;

    let result = primary.fork("c").await;
    let err = match result {
        Ok(_) => panic!("expected ForkBudgetExceeded, got Ok"),
        Err(e) => e,
    };
    match err {
        UniError::ForkBudgetExceeded { current, max } => {
            assert_eq!(max, 2);
            assert_eq!(current, 2);
        }
        other => panic!("expected ForkBudgetExceeded, got {other:?}"),
    }

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn budget_releases_slot_after_drop() -> Result<()> {
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
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'seed'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let a = primary.fork("a").await?;
    drop(a);
    db.drop_fork("a").await?;

    // Slot should be reusable now.
    let _b = primary.fork("b").await?;

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn unbounded_budget_default() -> Result<()> {
    // Default UniConfig has max_forks = None; assert no budget enforced.
    let cfg = UniConfig {
        disable_fork_sweeper: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;
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

    for i in 0..10 {
        let _ = primary.fork(format!("fork_{i}")).await?;
    }

    db.shutdown().await?;
    Ok(())
}
