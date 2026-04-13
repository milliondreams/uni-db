// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Tests for vertex deletion and persistence.

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn test_delete_vertex_persistence() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    // Insert a vertex
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.commit().await?;

    // Verify it exists
    let result = db
        .session()
        .query("MATCH (n:Person) RETURN count(n) AS cnt")
        .await?;
    assert_eq!(result.rows()[0].get::<i64>("cnt")?, 1);

    // Delete the vertex
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("MATCH (n:Person {name: 'Alice'}) DELETE n")
        .await?;
    tx.commit().await?;

    // Verify deletion persists
    let result = db
        .session()
        .query("MATCH (n:Person) RETURN count(n) AS cnt")
        .await?;
    assert_eq!(
        result.rows()[0].get::<i64>("cnt")?,
        0,
        "Deleted vertex should not appear in MATCH"
    );

    Ok(())
}

#[tokio::test]
async fn test_delete_vertex_properties_not_accessible() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    // Create and then delete
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.commit().await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("MATCH (n:Person {name: 'Alice'}) DELETE n")
        .await?;
    tx.commit().await?;

    // Query for the deleted vertex's property — should return 0 rows
    let result = db
        .session()
        .query("MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.name")
        .await?;
    assert_eq!(result.len(), 0, "Deleted vertex should not be queryable");

    Ok(())
}
