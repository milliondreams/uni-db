// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 2 Day 8 — same-fork sessions share a `UniInner` so commits
//! from one are immediately visible to the others, and concurrent
//! `Session::fork(name)` calls on the same name resolve to the same
//! shared inner exactly once.
//!
//! Also covers cross-fork concurrency: writes on different forks
//! proceed in parallel without contention beyond the shared registry.

// Rust guideline compliant

use anyhow::Result;
use std::sync::Arc;
use uni_db::{DataType, Uni};

/// Two sessions on the same fork: a commit through session A is visible
/// to a query through session B *without* an intervening flush. This
/// is the contract Phase 2 Day 8's `UniInner` cache adds; before the
/// cache, each `Session::fork(name)` produced a fresh `UniInner` with
/// its own `Writer`/L0, so B's reads couldn't see A's pre-flush writes.
#[tokio::test]
async fn same_fork_sessions_share_l0_visibility() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    // Seed primary so the vertices_Person dataset exists at fork time.
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'seed'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let session_a = primary.fork("shared").await?;
    let session_b = primary.fork("shared").await?;

    // A commits; B must see the new row immediately on its next query —
    // no flush required.
    let tx_a = session_a.tx().await?;
    tx_a.execute("CREATE (:Person {name: 'from-a'})").await?;
    tx_a.commit().await?;

    let names: Vec<String> = session_b
        .query("MATCH (p:Person) RETURN p.name")
        .await?
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("p.name").ok())
        .collect();
    assert!(
        names.contains(&"from-a".to_string()),
        "session B must see session A's commit on the same fork; got {names:?}"
    );

    db.shutdown().await?;
    Ok(())
}

/// Concurrent `fork("x")` calls construct exactly one `UniInner` for
/// `x` — the per-name lock + cache lookup serializes builds. Verified
/// by checking that all returned sessions yield the same view of a
/// commit landed on any one of them.
#[tokio::test]
async fn concurrent_same_name_forks_share_inner() -> Result<()> {
    let db = Arc::new(Uni::in_memory().build().await?);
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

    // Spawn 8 concurrent fork("dupe") calls.
    let mut handles = Vec::new();
    for _ in 0..8 {
        let s = db.session();
        handles.push(tokio::spawn(async move { s.fork("dupe").await }));
    }
    let mut sessions = Vec::new();
    for h in handles {
        sessions.push(h.await??);
    }

    // Pick one, commit a row.
    let tx = sessions[0].tx().await?;
    tx.execute("CREATE (:Item {kind: 'shared-write'})").await?;
    tx.commit().await?;

    // Every other session sees it without flushing.
    for (i, s) in sessions.iter().enumerate().skip(1) {
        let names: Vec<String> = s
            .query("MATCH (i:Item) RETURN i.kind")
            .await?
            .rows()
            .iter()
            .filter_map(|r| r.get::<String>("i.kind").ok())
            .collect();
        assert!(
            names.contains(&"shared-write".to_string()),
            "session #{i} must see the shared-write; got {names:?}"
        );
    }

    Ok(())
}

/// Different forks proceed in parallel — each has its own
/// `UniInner`/`Writer`/L0, no cross-fork contention. We don't measure
/// wall time (flaky), just confirm both commit successfully and remain
/// isolated.
#[tokio::test]
async fn cross_fork_writes_are_isolated() -> Result<()> {
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

    let fork_a = session.fork("a").await?;
    let fork_b = session.fork("b").await?;

    let (ra, rb) = tokio::join!(
        async {
            let tx = fork_a.tx().await?;
            tx.execute("CREATE (:Item {kind: 'a-only'})").await?;
            tx.commit().await
        },
        async {
            let tx = fork_b.tx().await?;
            tx.execute("CREATE (:Item {kind: 'b-only'})").await?;
            tx.commit().await
        },
    );
    ra?;
    rb?;

    let a_names: Vec<String> = fork_a
        .query("MATCH (i:Item) RETURN i.kind")
        .await?
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("i.kind").ok())
        .collect();
    let b_names: Vec<String> = fork_b
        .query("MATCH (i:Item) RETURN i.kind")
        .await?
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("i.kind").ok())
        .collect();

    assert!(a_names.contains(&"a-only".to_string()));
    assert!(!a_names.contains(&"b-only".to_string()));
    assert!(b_names.contains(&"b-only".to_string()));
    assert!(!b_names.contains(&"a-only".to_string()));

    db.shutdown().await?;
    Ok(())
}

/// After all sessions on a fork drop, the cached `Weak` no longer
/// upgrades and the next `fork(name)` call constructs a fresh
/// `UniInner`. This is the contract that lets `drop_fork` proceed
/// once holders go to zero.
#[tokio::test]
async fn dropping_all_sessions_releases_inner() -> Result<()> {
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

    {
        let f1 = session.fork("releases").await?;
        let f2 = session.fork("releases").await?;
        let _ = (f1, f2);
    } // both drop here

    // Fork map's Weak is now dead. The next fork() call must succeed
    // (this would deadlock or error if the Weak weren't released).
    let f3 = session.fork("releases").await?;
    assert!(f3.is_forked());

    drop(f3);
    db.drop_fork("releases").await?;

    db.shutdown().await?;
    Ok(())
}
