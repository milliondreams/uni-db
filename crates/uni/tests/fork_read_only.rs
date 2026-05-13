// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end Phase 1 fork tests through the public `Session::fork` API.
//!
//! Covers the Phase 1 exit criteria from
//! `docs/proposals/graph_fork_plan.md`:
//! - Read-only forks survive restart
//! - Primary writes after fork creation are invisible to the fork
//! - `forked.tx()` rejects with the typed error variant
//! - `.new_()` must-create variant works and rejects on conflict
//! - `drop_fork` while session held returns `ForkInUse`
//! - `Uni::list_forks` / `fork_info` round-trip

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, UniError};

#[tokio::test]
async fn fork_sees_fork_point_state_after_primary_writes() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.execute("CREATE (:Person {name: 'Bob'})").await?;
    tx.commit().await?;

    db.flush().await?;

    // Fork at this point.
    let forked = session.fork("scenario_1").await?;
    assert!(forked.is_forked());

    // Mutate primary further.
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Carol'})").await?;
    tx.commit().await?;
    db.flush().await?;

    // Primary sees three.
    let primary_rows = session
        .query("MATCH (p:Person) RETURN p.name")
        .await?
        .rows()
        .len();
    assert_eq!(primary_rows, 3, "primary should see all three names");

    // Fork still sees the original two — snapshot isolation at fork point.
    let fork_rows = forked
        .query("MATCH (p:Person) RETURN p.name")
        .await?
        .rows()
        .len();
    assert_eq!(fork_rows, 2, "fork must not see post-fork primary writes");

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn forked_session_tx_now_succeeds_post_phase2() -> Result<()> {
    // Phase 2 Day 7: the previous `ForkWritesNotYetSupported` gate has
    // been removed. `forked.tx()` returns a working Transaction whose
    // commits land on the fork's branches (see `fork_writes.rs` for
    // the full write-then-read cycle).
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    let session = db.session();
    let forked = session.fork("write_attempt").await?;

    let tx = forked
        .tx()
        .await
        .expect("forked.tx() should succeed after Phase 2");
    drop(tx); // rollback

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn must_create_variant_rejects_on_existing_succeeds_on_fresh() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();

    // First create succeeds via the open-or-create path.
    let _f1 = session.fork("only_once").await?;

    // .new_() on the existing name errors.
    match session.fork("only_once").new_().await {
        Ok(_) => panic!("expected ForkAlreadyExists"),
        Err(UniError::ForkAlreadyExists { .. }) => {}
        Err(other) => panic!("expected ForkAlreadyExists, got {other:?}"),
    }

    // .new_() on a fresh name succeeds.
    let _f2 = session.fork("brand_new").new_().await?;

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn drop_fork_in_use_blocks_until_session_released() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();

    let forked = session.fork("scenario_in_use").await?;

    // While the forked session is alive, drop_fork errors with ForkInUse.
    let err = db.drop_fork("scenario_in_use").await.unwrap_err();
    match err {
        UniError::ForkInUse { holder_count, .. } => assert!(holder_count >= 1),
        other => panic!("expected ForkInUse, got {other:?}"),
    }

    // Release the holder.
    drop(forked);

    // Now drop succeeds.
    db.drop_fork("scenario_in_use").await?;

    // Subsequent lookup returns ForkNotFound.
    let err = db.fork_info("scenario_in_use").await.unwrap_err();
    assert!(matches!(err, UniError::ForkNotFound { .. }));

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn list_and_info_round_trip() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let _a = session.fork("alpha").await?;
    let _b = session.fork("beta").await?;
    let _c = session.fork("gamma").await?;

    let mut names: Vec<String> = db.list_forks().await.into_iter().map(|f| f.name).collect();
    names.sort();
    assert_eq!(names, vec!["alpha", "beta", "gamma"]);

    let beta = db.fork_info("beta").await?;
    assert_eq!(beta.name, "beta");

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn fork_persists_across_reopen() -> Result<()> {
    // Use a temp directory so we can close and reopen the same DB.
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();

    {
        let db = Uni::open(&path).build().await?;
        db.schema()
            .label("Person")
            .property("name", DataType::String)
            .apply()
            .await?;
        let session = db.session();
        let tx = session.tx().await?;
        tx.execute("CREATE (:Person {name: 'Alice'})").await?;
        tx.commit().await?;
        db.flush().await?;
        let _f = session.fork("persist_me").await?;
        // Drop the session and the db.
        drop(_f);
        db.shutdown().await?;
    }

    // Reopen and confirm the fork is still there.
    let db = Uni::open(&path).build().await?;
    let info = db.fork_info("persist_me").await?;
    assert_eq!(info.name, "persist_me");

    // And it can be opened again as a forked session.
    let session = db.session();
    let forked = session.fork("persist_me").await?;
    assert!(forked.is_forked());

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn forks_remain_independent_when_primary_label_grows() -> Result<()> {
    // Spec §10: "Fork writes invisible to parent" applies symmetrically:
    // primary writes after the fork-point are invisible to the fork.
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("kind", DataType::String)
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Item {kind: 'A'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let f1 = session.fork("snap_a").await?;
    let f2 = session.fork("snap_b").await?;

    let tx = session.tx().await?;
    tx.execute("CREATE (:Item {kind: 'B'})").await?;
    tx.commit().await?;
    db.flush().await?;

    // Both forks see the original 1 item; primary sees 2.
    assert_eq!(f1.query("MATCH (i:Item) RETURN i").await?.rows().len(), 1);
    assert_eq!(f2.query("MATCH (i:Item) RETURN i").await?.rows().len(), 1);
    assert_eq!(
        session.query("MATCH (i:Item) RETURN i").await?.rows().len(),
        2
    );

    db.shutdown().await?;
    Ok(())
}
