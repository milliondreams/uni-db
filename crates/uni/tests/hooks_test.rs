// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Tests for SessionHook lifecycle: before/after query and commit hooks.

use anyhow::Result;
use std::sync::{Arc, Mutex};
use uni_db::{CommitHookContext, DataType, HookContext, QueryMetrics, SessionHook, Uni};

/// A hook that records invocations for testing.
struct RecorderHook {
    before_queries: Arc<Mutex<Vec<String>>>,
    after_queries: Arc<Mutex<Vec<String>>>,
    before_commits: Arc<Mutex<Vec<String>>>,
    after_commits: Arc<Mutex<Vec<String>>>,
}

type EventLog = Arc<Mutex<Vec<String>>>;

impl RecorderHook {
    #[allow(clippy::type_complexity)]
    fn new() -> (Self, EventLog, EventLog, EventLog, EventLog) {
        let bq = Arc::new(Mutex::new(Vec::new()));
        let aq = Arc::new(Mutex::new(Vec::new()));
        let bc = Arc::new(Mutex::new(Vec::new()));
        let ac = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                before_queries: bq.clone(),
                after_queries: aq.clone(),
                before_commits: bc.clone(),
                after_commits: ac.clone(),
            },
            bq,
            aq,
            bc,
            ac,
        )
    }
}

impl SessionHook for RecorderHook {
    fn before_query(&self, ctx: &HookContext) -> uni_common::Result<()> {
        self.before_queries
            .lock()
            .unwrap()
            .push(ctx.query_text.clone());
        Ok(())
    }

    fn after_query(&self, ctx: &HookContext, _metrics: &QueryMetrics) {
        self.after_queries
            .lock()
            .unwrap()
            .push(ctx.query_text.clone());
    }

    fn before_commit(&self, ctx: &CommitHookContext) -> uni_common::Result<()> {
        self.before_commits
            .lock()
            .unwrap()
            .push(ctx.tx_id.clone());
        Ok(())
    }

    fn after_commit(
        &self,
        ctx: &CommitHookContext,
        _result: &uni_db::CommitResult,
    ) {
        self.after_commits
            .lock()
            .unwrap()
            .push(ctx.tx_id.clone());
    }
}

/// A hook that rejects all queries.
struct BlockerHook;

impl SessionHook for BlockerHook {
    fn before_query(&self, _ctx: &HookContext) -> uni_common::Result<()> {
        Err(anyhow::anyhow!("denied by hook").into())
    }
}

/// A hook that rejects all commits.
struct CommitBlockerHook;

impl SessionHook for CommitBlockerHook {
    fn before_commit(&self, _ctx: &CommitHookContext) -> uni_common::Result<()> {
        Err(anyhow::anyhow!("commit denied").into())
    }
}

async fn setup_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    Ok(db)
}

// ── before_query tests ───────────────────────────────────────────────

#[tokio::test]
async fn test_before_query_hook_fires() -> Result<()> {
    let db = setup_db().await?;
    let (hook, before_queries, _, _, _) = RecorderHook::new();

    let mut session = db.session();
    session.add_hook("recorder", hook);

    session.query("RETURN 1 AS x").await?;

    let recorded = before_queries.lock().unwrap();
    assert_eq!(recorded.len(), 1, "before_query should fire once");
    assert_eq!(recorded[0], "RETURN 1 AS x");

    Ok(())
}

#[tokio::test]
async fn test_before_query_hook_rejects() -> Result<()> {
    let db = setup_db().await?;

    let mut session = db.session();
    session.add_hook("blocker", BlockerHook);

    let result = session.query("RETURN 1 AS x").await;
    assert!(result.is_err(), "Rejecting hook should abort query");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("denied") || err_msg.contains("Hook"),
        "Error should mention hook rejection, got: {}",
        err_msg
    );

    Ok(())
}

// ── after_query test ─────────────────────────────────────────────────

#[tokio::test]
async fn test_after_query_hook_fires() -> Result<()> {
    let db = setup_db().await?;
    let (hook, _, after_queries, _, _) = RecorderHook::new();

    let mut session = db.session();
    session.add_hook("recorder", hook);

    session.query("RETURN 42 AS val").await?;

    let recorded = after_queries.lock().unwrap();
    assert_eq!(recorded.len(), 1, "after_query should fire once");
    assert_eq!(recorded[0], "RETURN 42 AS val");

    Ok(())
}

// ── before_commit test ───────────────────────────────────────────────

#[tokio::test]
async fn test_before_commit_hook_rejects() -> Result<()> {
    let db = setup_db().await?;

    let mut session = db.session();
    session.add_hook("blocker", CommitBlockerHook);

    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    let result = tx.commit().await;

    assert!(result.is_err(), "Rejecting commit hook should abort commit");

    // Data should NOT be visible
    let check = db.session().query("MATCH (n:Person) RETURN count(n) AS cnt").await?;
    assert_eq!(check.rows()[0].get::<i64>("cnt")?, 0, "Rejected commit should not persist data");

    Ok(())
}

// ── after_commit test ────────────────────────────────────────────────

#[tokio::test]
async fn test_after_commit_hook_fires() -> Result<()> {
    let db = setup_db().await?;
    let (hook, _, _, _, after_commits) = RecorderHook::new();

    let mut session = db.session();
    session.add_hook("recorder", hook);

    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.commit().await?;

    let recorded = after_commits.lock().unwrap();
    assert_eq!(recorded.len(), 1, "after_commit should fire once");
    assert!(!recorded[0].is_empty(), "tx_id should be non-empty");

    Ok(())
}

// ── Multiple hooks test ──────────────────────────────────────────────

#[tokio::test]
async fn test_multiple_hooks_both_fire() -> Result<()> {
    let db = setup_db().await?;
    let (hook_a, bq_a, _, _, _) = RecorderHook::new();
    let (hook_b, bq_b, _, _, _) = RecorderHook::new();

    let mut session = db.session();
    session.add_hook("hook_a", hook_a);
    session.add_hook("hook_b", hook_b);

    session.query("RETURN 1 AS x").await?;

    let a_count = bq_a.lock().unwrap().len();
    let b_count = bq_b.lock().unwrap().len();
    assert_eq!(a_count, 1, "Hook A should fire");
    assert_eq!(b_count, 1, "Hook B should fire");

    Ok(())
}
