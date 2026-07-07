#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for crates/uni/src/api/fork_maintenance.rs:61 (finding [1]).
//!
//! `sweep_tick` wraps the LIVE `Arc<UniInner>` into a transient `Uni { inner }`
//! (line 61) purely to call `drop_fork_cascade`. `Uni`'s `Drop` impl calls
//! `shutdown_handle.shutdown_blocking()`, which unconditionally broadcasts `()`
//! on the ONE shared shutdown channel. So the moment the sweeper drops at least
//! one expired fork and the local `db` drops at the end of `sweep_tick`, a
//! shutdown is broadcast to every background task of the still-running
//! database — including the sweeper's own ticker loop, which self-terminates.
//!
//! Observable end-to-end: the FIRST expired fork is swept (triggering the
//! errant broadcast), after which the sweeper is dead and a SECOND expired fork
//! created afterwards is NEVER swept — even though `db.query(...)` keeps
//! answering, proving background processing stopped silently.

use std::time::Duration;

use uni_common::config::UniConfig;
use uni_db::{DataType, Uni};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sweeper_self_terminates_after_first_productive_tick() -> anyhow::Result<()> {
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

    // First short-TTL fork: the sweeper drops it on its first productive tick,
    // and the transient `Uni` drop broadcasts shutdown to all tasks.
    let f1 = primary
        .fork("ephemeral_1")
        .ttl(Duration::from_millis(150))
        .await?;
    drop(f1);

    tokio::time::sleep(Duration::from_millis(700)).await;

    let after_first: Vec<String> = db.list_forks().await.into_iter().map(|f| f.name).collect();
    assert!(
        !after_first.iter().any(|n| n == "ephemeral_1"),
        "sanity: the first expired fork should be swept; remaining = {after_first:?}"
    );

    // Second short-TTL fork, created AFTER the sweeper self-terminated. In
    // correct behavior the sweeper would drop this too; with the bug it is
    // never swept.
    let f2 = primary
        .fork("ephemeral_2")
        .ttl(Duration::from_millis(150))
        .await?;
    drop(f2);

    // Wait for several sweeper intervals — far more than enough for a live
    // sweeper to fire.
    tokio::time::sleep(Duration::from_millis(900)).await;

    // The database is still fully queryable (Arc alive), proving this is a
    // silent loss of background processing, not a real shutdown.
    let alive = db
        .session()
        .query("MATCH (p:Person) RETURN count(p) AS c")
        .await?;
    let c: i64 = alive.rows()[0].get("c")?;
    assert_eq!(
        c, 1,
        "db must keep answering queries after the errant broadcast"
    );

    let after_second: Vec<String> = db.list_forks().await.into_iter().map(|f| f.name).collect();

    // Fixed (fork_maintenance.rs:61): the sweeper no longer broadcasts shutdown
    // when dropping a fork, so it stays alive and sweeps 'ephemeral_2' too.
    assert!(
        !after_second.iter().any(|n| n == "ephemeral_2"),
        "second expired fork must also be swept (sweeper stays alive); remaining = {after_second:?}"
    );

    Ok(())
}
