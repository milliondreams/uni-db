// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Tests for data persistence through the write pipeline (create → flush → query).

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn test_writer_flush() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .done()
        .edge_type("knows", &["Person"], &["Person"])
        .apply()
        .await?;

    // Create an edge via transaction
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (a:Person {name: 'Alice'})-[:knows]->(b:Person {name: 'Bob'})")
        .await?;
    tx.commit().await?;

    // Flush to ensure data is persisted to storage
    db.flush().await?;

    // Verify edge is queryable
    let result = db
        .session()
        .query("MATCH ()-[r:knows]->() RETURN count(r) AS cnt")
        .await?;
    assert_eq!(result.rows()[0].get::<i64>("cnt")?, 1);

    Ok(())
}

#[tokio::test]
async fn test_writer_vertex_flush() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    // Create vertex via transaction
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.commit().await?;

    // Flush to storage
    db.flush().await?;

    // Verify vertex is queryable with correct property
    let result = db
        .session()
        .query("MATCH (n:Person) RETURN n.name AS name")
        .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<String>("name")?, "Alice");

    Ok(())
}
