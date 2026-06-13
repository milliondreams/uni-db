// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Tests for PreparedQuery and PreparedLocy — plan caching and schema staleness.

use anyhow::Result;
use std::sync::Arc;
use uni_db::{DataType, Uni, Value};

async fn setup_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property_nullable("age", DataType::Int32)
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice', age: 30})")
        .await?;
    tx.execute("CREATE (:Person {name: 'Bob', age: 25})")
        .await?;
    tx.commit().await?;

    Ok(db)
}

#[tokio::test]
async fn test_prepared_query_basic() -> Result<()> {
    let db = setup_db().await?;

    let session = db.session();
    let pq = session
        .prepare("MATCH (p:Person) RETURN p.name AS name ORDER BY name")
        .await?;

    let result = pq.execute(&[]).await?;
    assert_eq!(result.len(), 2);
    assert_eq!(result.rows()[0].get::<String>("name")?, "Alice");
    assert_eq!(result.rows()[1].get::<String>("name")?, "Bob");

    // Execute again — should use cached plan
    let result2 = pq.execute(&[]).await?;
    assert_eq!(
        result2.len(),
        2,
        "Second execution should return same results"
    );

    // Verify query text is preserved
    assert_eq!(
        pq.query_text(),
        "MATCH (p:Person) RETURN p.name AS name ORDER BY name"
    );

    Ok(())
}

#[tokio::test]
async fn test_prepared_query_bind_params() -> Result<()> {
    let db = setup_db().await?;

    let session = db.session();
    let pq = session
        .prepare("MATCH (p:Person) WHERE p.name = $name RETURN p.age AS age")
        .await?;

    let result = pq
        .bind()
        .param("name", Value::String("Alice".into()))
        .execute()
        .await?;

    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<i32>("age")?, 30);

    Ok(())
}

#[tokio::test]
async fn test_prepared_query_execute_with_params_array() -> Result<()> {
    let db = setup_db().await?;

    let session = db.session();
    let pq = session
        .prepare("MATCH (p:Person) WHERE p.name = $name RETURN p.age AS age")
        .await?;

    let result = pq.execute(&[("name", Value::String("Bob".into()))]).await?;

    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<i32>("age")?, 25);

    Ok(())
}

#[tokio::test]
async fn test_prepared_query_schema_change_replans() -> Result<()> {
    let db = setup_db().await?;

    let session = db.session();
    let pq = session
        .prepare("MATCH (p:Person) RETURN p.name AS name ORDER BY name")
        .await?;

    // Execute before schema change
    let result1 = pq.execute(&[]).await?;
    assert_eq!(result1.len(), 2);

    // Change schema — add a new property
    db.schema()
        .label("Person")
        .property_nullable("email", DataType::String)
        .apply()
        .await?;

    // Execute again — should transparently re-plan
    let result2 = pq.execute(&[]).await?;
    assert_eq!(
        result2.len(),
        2,
        "Re-planned query should still return correct results"
    );

    Ok(())
}

#[tokio::test]
async fn test_prepared_query_concurrent_execution() -> Result<()> {
    let db = setup_db().await?;

    let session = db.session();
    let pq = Arc::new(
        session
            .prepare("MATCH (p:Person) RETURN p.name AS name ORDER BY name")
            .await?,
    );

    // Execute from 4 concurrent tasks
    let mut handles = Vec::new();
    for _ in 0..4 {
        let pq_clone = pq.clone();
        handles.push(tokio::spawn(async move { pq_clone.execute(&[]).await }));
    }

    for handle in handles {
        let result = handle.await??;
        assert_eq!(
            result.len(),
            2,
            "Each concurrent execution should return 2 rows"
        );
    }

    Ok(())
}

// ── Regression: review #3 — prepared-statement validation + tx binding ──

/// A session-prepared mutation must be rejected at prepare time — session
/// prepared queries are read-only, like `session.query`. Previously
/// `PreparedQuery::new` skipped validation, so this executed an unvalidated,
/// non-transactional write.
#[tokio::test]
async fn test_session_prepare_rejects_write() -> Result<()> {
    let db = setup_db().await?;
    let session = db.session();

    let err = session
        .prepare("CREATE (:Person {name: 'Mallory'})")
        .await
        .expect_err("session.prepare() of a write must be rejected");
    assert!(
        err.to_string().contains("read-only"),
        "error should mention read-only, got: {err}"
    );

    // And nothing was written.
    let n = session
        .query("MATCH (p:Person {name: 'Mallory'}) RETURN count(p) AS c")
        .await?
        .rows()[0]
        .get::<i64>("c")?;
    assert_eq!(n, 0, "rejected prepared write must not have created a node");
    Ok(())
}

/// A transaction-prepared write must land in the transaction's L0 and be undone
/// by `rollback()` — previously it leaked into main L0 and survived rollback.
#[tokio::test]
async fn test_tx_prepare_write_is_rolled_back() -> Result<()> {
    let db = setup_db().await?;

    let tx = db.session().tx().await?;
    let pq = tx
        .prepare("CREATE (:Person {name: 'Carol', age: 41})")
        .await?;
    pq.execute(&[]).await?;

    // The write is visible within the tx (reads see uncommitted writes).
    let in_tx = tx
        .query("MATCH (p:Person {name: 'Carol'}) RETURN count(p) AS c")
        .await?
        .rows()[0]
        .get::<i64>("c")?;
    assert_eq!(in_tx, 1, "tx-prepared write must be visible within the tx");

    tx.rollback();

    // After rollback the write must be gone (it lived in tx_l0, not main L0).
    let after = db
        .session()
        .query("MATCH (p:Person {name: 'Carol'}) RETURN count(p) AS c")
        .await?
        .rows()[0]
        .get::<i64>("c")?;
    assert_eq!(after, 0, "tx-prepared write must be undone by rollback()");
    Ok(())
}

/// A transaction-prepared write that the transaction commits must persist.
#[tokio::test]
async fn test_tx_prepare_write_commits() -> Result<()> {
    let db = setup_db().await?;

    let tx = db.session().tx().await?;
    let pq = tx
        .prepare("CREATE (:Person {name: 'Dave', age: 52})")
        .await?;
    pq.execute(&[]).await?;
    tx.commit().await?;

    let after = db
        .session()
        .query("MATCH (p:Person {name: 'Dave'}) RETURN p.age AS age")
        .await?
        .rows()[0]
        .get::<i32>("age")?;
    assert_eq!(after, 52, "committed tx-prepared write must persist");
    Ok(())
}
