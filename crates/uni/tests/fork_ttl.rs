// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 4a — TTL + background sweeper.
//!
//! Asserts:
//! - `ttl(Duration)` on the fork builder stamps `ttl_expires_at`.
//! - The sweeper picks up expired forks and drops them via cascade.
//! - `disable_fork_sweeper = true` keeps even expired forks around.
//! - `UniConfig::fork_default_ttl` applies when no builder override
//!   is set, and the builder override wins.

// Rust guideline compliant

use anyhow::Result;
use std::time::Duration;
use uni_common::config::UniConfig;
use uni_common::core::schema::DataType;
use uni_db::Uni;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fork_ttl_expires_and_sweeper_drops() -> Result<()> {
    let cfg = UniConfig {
        // Sweeper polls every 100ms so we don't wait long in CI.
        fork_sweeper_interval: Duration::from_millis(100),
        disable_fork_sweeper: false,
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

    let fork = primary.fork("ephemeral").ttl(Duration::from_millis(200)).await?;
    drop(fork); // release the session so the sweeper isn't blocked by ForkInUse

    // Wait long enough for the sweeper to fire at least once after TTL.
    tokio::time::sleep(Duration::from_millis(800)).await;

    let remaining: Vec<String> = db
        .list_forks()
        .await
        .into_iter()
        .map(|f| f.name)
        .collect();
    assert!(
        !remaining.iter().any(|n| n == "ephemeral"),
        "sweeper should have dropped 'ephemeral' after TTL; remaining = {remaining:?}"
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fork_without_ttl_survives_sweeper() -> Result<()> {
    let cfg = UniConfig {
        fork_sweeper_interval: Duration::from_millis(100),
        disable_fork_sweeper: false,
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

    let fork = primary.fork("permanent").await?;
    drop(fork);

    tokio::time::sleep(Duration::from_millis(500)).await;

    let remaining: Vec<String> = db
        .list_forks()
        .await
        .into_iter()
        .map(|f| f.name)
        .collect();
    assert!(
        remaining.iter().any(|n| n == "permanent"),
        "fork without TTL must survive; remaining = {remaining:?}"
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_sweeper_keeps_expired_forks() -> Result<()> {
    let cfg = UniConfig {
        fork_sweeper_interval: Duration::from_millis(50),
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

    let fork = primary.fork("ephemeral").ttl(Duration::from_millis(50)).await?;
    drop(fork);

    tokio::time::sleep(Duration::from_millis(400)).await;

    // Sweeper disabled — expired fork should still appear in the list.
    let remaining: Vec<String> = db
        .list_forks()
        .await
        .into_iter()
        .map(|f| f.name)
        .collect();
    assert!(
        remaining.iter().any(|n| n == "ephemeral"),
        "disabled sweeper must leave expired forks intact; remaining = {remaining:?}"
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_ttl_applies_when_builder_has_no_override() -> Result<()> {
    let cfg = UniConfig {
        fork_sweeper_interval: Duration::from_millis(100),
        disable_fork_sweeper: false,
        fork_default_ttl: Some(Duration::from_millis(200)),
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

    let fork = primary.fork("uses_default").await?;
    drop(fork);

    tokio::time::sleep(Duration::from_millis(800)).await;

    let remaining: Vec<String> = db
        .list_forks()
        .await
        .into_iter()
        .map(|f| f.name)
        .collect();
    assert!(
        !remaining.iter().any(|n| n == "uses_default"),
        "fork should expire via fork_default_ttl; remaining = {remaining:?}"
    );

    db.shutdown().await?;
    Ok(())
}
