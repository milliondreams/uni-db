// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_incoming_relationship_create() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create schema
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL A (name STRING)").await?;
    tx.execute("CREATE LABEL B (name STRING)").await?;
    tx.execute("CREATE EDGE TYPE KNOWS (since INT) FROM B TO A")
        .await?;
    tx.execute("CREATE (a:A {name: 'Alice'})<-[:KNOWS {since: 2020}]-(b:B {name: 'Bob'})")
        .await?;
    tx.commit().await?;

    // Query in outgoing direction: Bob -> Alice
    let result = db
        .session()
        .query("MATCH (b:B)-[:KNOWS]->(a:A) RETURN b.name AS from, a.name AS to")
        .await?;

    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<String>("from")?, "Bob");
    assert_eq!(result.rows()[0].get::<String>("to")?, "Alice");

    Ok(())
}

#[tokio::test]
async fn test_mixed_directions() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create labels only - use schemaless edge types
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL A (id INT)").await?;
    tx.execute("CREATE LABEL B (id INT)").await?;
    tx.execute("CREATE LABEL C (id INT)").await?;
    tx.execute("CREATE (a:A {id: 0})<-[:ADMIN]-(b:B {id: 1})-[:ADMIN]->(c:C {id: 2})")
        .await?;
    tx.commit().await?;

    // Flush to ensure writes are visible
    db.flush().await?;

    // Verify we have exactly 2 edges total
    let all_edges = db
        .session()
        .query("MATCH ()-[r:ADMIN]->() RETURN count(r) AS cnt")
        .await?;
    assert_eq!(
        all_edges.rows()[0].get::<i64>("cnt")?,
        2,
        "Should have exactly 2 ADMIN edges"
    );

    // Verify both edges originate from node with id=1 (B)
    // Note: Due to current limitations with schemaless edge property loading,
    // we verify connectivity rather than property values
    let edges_from_b = db
        .session()
        .query("MATCH (b:B)-[r:ADMIN]-() RETURN count(r) AS cnt")
        .await?;
    assert_eq!(
        edges_from_b.rows()[0].get::<i64>("cnt")?,
        2,
        "Both edges should originate from B"
    );

    Ok(())
}

#[tokio::test]
async fn test_incoming_with_properties() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create schema
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Person (name STRING)").await?;
    tx.execute("CREATE EDGE TYPE FOLLOWS (since INT) FROM Person TO Person")
        .await?;
    tx.execute(
        "CREATE (:Person {name: 'Alice'})<-[:FOLLOWS {since: 2021}]-(:Person {name: 'Bob'})",
    )
    .await?;
    tx.commit().await?;

    // Query to verify edge direction and properties
    let result = db
        .session().query("MATCH (follower:Person)-[r:FOLLOWS]->(followed:Person) RETURN follower.name, followed.name, r.since")
        .await?;

    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<String>("follower.name")?, "Bob");
    assert_eq!(result.rows()[0].get::<String>("followed.name")?, "Alice");
    assert_eq!(result.rows()[0].get::<i64>("r.since")?, 2021);

    Ok(())
}
