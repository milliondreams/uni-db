// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Tests for DatabaseMetrics and SessionMetrics.

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn test_database_metrics_after_operations() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    // Perform some operations
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.execute("CREATE (:Person {name: 'Bob'})").await?;
    tx.commit().await?;

    session
        .query("MATCH (n:Person) RETURN count(n) AS cnt")
        .await?;

    let metrics = db.metrics();
    // Schema version should be set (at least 1 after schema.apply())
    assert!(
        metrics.schema_version > 0,
        "Schema version should be > 0 after apply"
    );

    Ok(())
}

#[tokio::test]
async fn test_session_metrics_tracks_queries() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .apply()
        .await?;

    let session = db.session();

    // Execute 3 queries
    session.query("RETURN 1 AS x").await?;
    session.query("RETURN 2 AS x").await?;
    session.query("RETURN 3 AS x").await?;

    let metrics = session.metrics();
    assert_eq!(
        metrics.queries_executed, 3,
        "Should track 3 queries, got {}",
        metrics.queries_executed
    );

    Ok(())
}

#[tokio::test]
async fn test_session_metrics_tracks_commits() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Counter")
        .property("val", DataType::Int32)
        .apply()
        .await?;

    let session = db.session();

    // Commit a transaction
    let tx = session.tx().await?;
    tx.execute("CREATE (:Counter {val: 1})").await?;
    tx.commit().await?;

    let metrics = session.metrics();
    assert_eq!(
        metrics.transactions_committed, 1,
        "Should track 1 committed transaction, got {}",
        metrics.transactions_committed
    );

    Ok(())
}
