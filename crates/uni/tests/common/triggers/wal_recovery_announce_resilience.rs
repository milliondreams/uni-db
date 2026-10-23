// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A commit durable in the WAL is announced even when the process died before
//! announcing it, and a commit already announced is not announced again by a
//! clean reopen.
//!
//! Measured before the fix: a process that died between a commit's WAL flush
//! and its announcement left a commit the next open replayed into L0 silently
//! -- no `CommitNotification`, no trigger fired, so CDC never saw it. A flush
//! running in that window moved the commit to L1 and truncated its WAL
//! segment, so even a recovery that looked at the WAL could not find it.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration --features failpoints \
//!     -E 'test(wal_recovery_announce_resilience)'

// Rust guideline compliant

use std::time::{Duration, Instant};

use uni_db::{DataType, Uni};

/// Declares `CREATE ON :A` with an action that writes `:AuditLog {msg: "create"}`.
const DECLARE_CREATE: &str = r#"CALL uni.plugin.declareTrigger(
    'test.wal_create',
    'CREATE ON :A',
    'CREATE (:AuditLog {msg: "create"})'
)"#;

/// Declares `UPDATE ON :A` with an action that writes `:AuditLog {msg: "update"}`.
const DECLARE_UPDATE: &str = r#"CALL uni.plugin.declareTrigger(
    'test.wal_update',
    'UPDATE ON :A',
    'CREATE (:AuditLog {msg: "update"})'
)"#;

async fn open(path: &std::path::Path) -> Uni {
    Uni::open(path.to_string_lossy())
        .build()
        .await
        .expect("open store")
}

async fn setup(db: &Uni, declarations: &[&str]) {
    db.schema()
        .label("A")
        .property("x", DataType::Int64)
        .apply()
        .await
        .expect("schema");
    for declaration in declarations {
        let tx = db.session().tx().await.expect("tx");
        tx.execute(declaration).await.expect("declare trigger");
        tx.commit().await.expect("commit declaration");
    }
}

async fn commit(db: &Uni, cypher: &str) {
    let tx = db.session().tx().await.expect("tx");
    tx.execute(cypher).await.expect("write");
    tx.commit().await.expect("commit");
}

async fn audit_count(db: &Uni, msg: &str) -> i64 {
    db.session()
        .query(&format!(
            "MATCH (n:AuditLog {{msg: '{msg}'}}) RETURN count(n) AS n"
        ))
        .await
        .expect("count")
        .rows()[0]
        .get::<i64>("n")
        .expect("n")
}

/// Wait until `msg`'s audit count reaches `at_least`, then let any further
/// (wrong) firing land before returning the count.
async fn settled_audit_count(db: &Uni, msg: &str, at_least: i64) -> i64 {
    let deadline = Instant::now() + Duration::from_secs(20);
    while audit_count(db, msg).await < at_least && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    audit_count(db, msg).await
}

/// A database closed by dropping it does not announce its last commit again
/// at the next open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_database_does_not_announce_its_commits_again_resilience() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("store");
    {
        let db = open(&root).await;
        setup(&db, &[DECLARE_CREATE]).await;
        commit(&db, "CREATE (:A {x: 1})").await;
        assert_eq!(settled_audit_count(&db, "create", 1).await, 1, "premise");
    }
    let db = open(&root).await;
    assert_eq!(
        settled_audit_count(&db, "create", 1).await,
        1,
        "the reopen fired the trigger for an already-announced commit"
    );
}

/// The same for a database shut down explicitly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shut_down_database_does_not_announce_its_commits_again_resilience() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("store");
    {
        let db = open(&root).await;
        setup(&db, &[DECLARE_CREATE]).await;
        commit(&db, "CREATE (:A {x: 1})").await;
        assert_eq!(settled_audit_count(&db, "create", 1).await, 1, "premise");
        db.shutdown().await.expect("shutdown");
    }
    let db = open(&root).await;
    assert_eq!(settled_audit_count(&db, "create", 1).await, 1);
}

/// The process dies between a commit's WAL flush and its announcement; the
/// next open announces it.
#[cfg(all(unix, feature = "failpoints"))]
mod crash {
    use std::time::Duration;

    use super::{DECLARE_CREATE, DECLARE_UPDATE, commit, open, settled_audit_count, setup};
    use crate::crash_harness;

    const ENTRY: &str =
        "triggers::wal_recovery_announce_resilience::crash::wal_recovery_abort_child";

    async fn count_a(db: &uni_db::Uni) -> i64 {
        db.session()
            .query("MATCH (n:A) RETURN count(n) AS n")
            .await
            .expect("count")
            .rows()[0]
            .get::<i64>("n")
            .expect("n")
    }

    /// Child: see the scenarios below.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "child-process entry point; driven by run_child"]
    async fn wal_recovery_abort_child() {
        let Some((scenario, path)) = crash_harness::child_env() else {
            return;
        };
        match scenario.as_str() {
            // Die after the commit is durable and visible, before it is
            // announced.
            "commit_then_crash" => {
                let db = open(&path).await;
                setup(&db, &[DECLARE_CREATE]).await;
                crash_harness::abort_at("commit::before-notify");
                commit(&db, "CREATE (:A {x: 1})").await;
            }
            // As above, with a flush started while the commit waits to be
            // announced: the flush must not move the commit to L1 (and drop
            // its WAL segment) first.
            "flush_then_crash" => {
                let db = std::sync::Arc::new(open(&path).await);
                setup(&db, &[DECLARE_CREATE]).await;
                let (paused_tx, paused_rx) = std::sync::mpsc::channel::<()>();
                let paused_tx = parking_lot::Mutex::new(paused_tx);
                fail::cfg_callback("commit::before-notify", move || {
                    let _ = paused_tx.lock().send(());
                    // Long enough for an unguarded flush of this small store
                    // to finish and truncate the WAL.
                    std::thread::sleep(Duration::from_secs(3));
                    std::process::abort();
                })
                .expect("arm seam");
                let committer = std::sync::Arc::clone(&db);
                tokio::spawn(async move { commit(&committer, "CREATE (:A {x: 1})").await });
                tokio::task::spawn_blocking(move || {
                    paused_rx.recv_timeout(Duration::from_secs(30))
                })
                .await
                .expect("join")
                .expect("the commit never reached the seam");
                let flusher = std::sync::Arc::clone(&db);
                tokio::spawn(async move { flusher.flush().await });
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            // Create (announced), then die before announcing an update of it.
            "update_then_crash" => {
                let db = open(&path).await;
                setup(&db, &[DECLARE_CREATE, DECLARE_UPDATE]).await;
                commit(&db, "CREATE (:A {x: 1})").await;
                assert_eq!(settled_audit_count(&db, "create", 1).await, 1, "premise");
                crash_harness::abort_at("commit::before-notify");
                commit(&db, "MATCH (a:A) SET a.x = 2").await;
            }
            other => crash_harness::unknown_scenario(ENTRY, other),
        }
    }

    /// The commit survives the crash, and the open announces it: the declared
    /// trigger fires, once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_unannounced_commit_is_announced_at_open_resilience() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("store");
        crash_harness::run_child_async(ENTRY, "commit_then_crash", &root).await;

        let db = open(&root).await;
        assert_eq!(count_a(&db).await, 1, "the commit is durable");
        assert_eq!(
            settled_audit_count(&db, "create", 1).await,
            1,
            "the declared trigger fires once for the recovered commit"
        );
    }

    /// A flush started while a commit waits for its announcement does not
    /// hide the commit from recovery.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_flush_does_not_overtake_an_unannounced_commit_resilience() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("store");
        crash_harness::run_child_async(ENTRY, "flush_then_crash", &root).await;

        let db = open(&root).await;
        assert_eq!(count_a(&db).await, 1, "the commit is durable");
        assert_eq!(
            settled_audit_count(&db, "create", 1).await,
            1,
            "the declared trigger fires once for the recovered commit"
        );
    }

    /// A recovered commit is classified against the state it committed on:
    /// an update of an existing vertex fires the update trigger, not the
    /// create trigger, and the announced create is not announced again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_recovered_update_is_announced_as_an_update_resilience() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("store");
        crash_harness::run_child_async(ENTRY, "update_then_crash", &root).await;

        let db = open(&root).await;
        assert_eq!(count_a(&db).await, 1);
        assert_eq!(
            settled_audit_count(&db, "update", 1).await,
            1,
            "the update trigger fires once for the recovered update"
        );
        assert_eq!(
            settled_audit_count(&db, "create", 1).await,
            1,
            "the create trigger fired only for the create, before the crash"
        );
    }
}
