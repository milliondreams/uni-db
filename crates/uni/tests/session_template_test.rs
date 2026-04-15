// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Tests for SessionTemplate — pre-configured session factories.

use anyhow::Result;
use std::sync::{Arc, Mutex};
use uni_db::{DataType, HookContext, QueryMetrics, SessionHook, Uni, Value};

/// Simple hook that records before_query invocations.
struct AuditHook(Arc<Mutex<Vec<String>>>);

impl SessionHook for AuditHook {
    fn before_query(&self, ctx: &HookContext) -> uni_common::Result<()> {
        self.0.lock().unwrap().push(ctx.query_text.clone());
        Ok(())
    }

    fn after_query(&self, _ctx: &HookContext, _metrics: &QueryMetrics) {}
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

#[tokio::test]
async fn test_template_pre_bound_params() -> Result<()> {
    let db = setup_db().await?;

    let template = db
        .session_template()
        .param("tenant_id", Value::Int(42))
        .build()?;

    let session = template.create();
    // Session params are namespaced under $session.key
    let result = session.query("RETURN $session.tenant_id AS tid").await?;

    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<i64>("tid")?, 42);

    Ok(())
}

#[tokio::test]
async fn test_template_with_hooks() -> Result<()> {
    let db = setup_db().await?;
    let log = Arc::new(Mutex::new(Vec::new()));

    let template = db
        .session_template()
        .hook("audit", AuditHook(log.clone()))
        .build()?;

    let session = template.create();
    session.query("RETURN 1 AS x").await?;

    let recorded = log.lock().unwrap();
    assert_eq!(recorded.len(), 1, "Hook from template should fire");
    assert_eq!(recorded[0], "RETURN 1 AS x");

    Ok(())
}

#[tokio::test]
async fn test_template_sessions_are_independent() -> Result<()> {
    let db = setup_db().await?;

    let template = db.session_template().build()?;

    let s1 = template.create();
    let s2 = template.create();

    // Mutate data in s1's transaction
    let tx = s1.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.commit().await?;

    // s2 should see the data (sessions share the same DB), but the sessions
    // themselves should be independent objects
    assert_ne!(
        s1.id(),
        s2.id(),
        "Template-created sessions should have distinct IDs"
    );

    Ok(())
}

#[tokio::test]
async fn test_template_create_is_cheap() -> Result<()> {
    let db = setup_db().await?;

    let template = db.session_template().build()?;

    // Creating many sessions should succeed without re-compilation
    let mut sessions = Vec::new();
    for _ in 0..100 {
        sessions.push(template.create());
    }

    assert_eq!(
        sessions.len(),
        100,
        "Should create 100 sessions without error"
    );

    // Each should have a unique ID
    let ids: std::collections::HashSet<_> = sessions.iter().map(|s| s.id().to_string()).collect();
    assert_eq!(ids.len(), 100, "All sessions should have unique IDs");

    Ok(())
}
