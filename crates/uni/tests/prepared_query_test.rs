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
    assert_eq!(result2.len(), 2, "Second execution should return same results");

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

    let result = pq
        .execute(&[("name", Value::String("Bob".into()))])
        .await?;

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
        assert_eq!(result.len(), 2, "Each concurrent execution should return 2 rows");
    }

    Ok(())
}
