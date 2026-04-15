// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Tests for CREATE operations verified through the public API only.
// No internal L0/Writer/StorageManager access — tests user-observable behavior.

use anyhow::Result;
use uni_db::{DataType, Uni};

async fn setup_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property_nullable("age", DataType::Int32)
        .done()
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()
        .await?;
    Ok(db)
}

// ── Basic CREATE tests ───────────────────────────────────────────────

#[tokio::test]
async fn test_create_single_node() -> Result<()> {
    let db = setup_db().await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice', age: 30})")
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .query("MATCH (n:Person) RETURN n.name AS name, n.age AS age")
        .await?;
    assert_eq!(result.len(), 1, "Should have exactly 1 Person node");
    assert_eq!(result.rows()[0].get::<String>("name")?, "Alice");
    assert_eq!(result.rows()[0].get::<i32>("age")?, 30);

    Ok(())
}

#[tokio::test]
async fn test_create_node_with_edge() -> Result<()> {
    let db = setup_db().await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")
        .await?;
    tx.commit().await?;

    // Verify nodes
    let nodes = db
        .session()
        .query("MATCH (n:Person) RETURN n.name AS name ORDER BY name")
        .await?;
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes.rows()[0].get::<String>("name")?, "Alice");
    assert_eq!(nodes.rows()[1].get::<String>("name")?, "Bob");

    // Verify edge
    let edges = db
        .session()
        .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name AS src, b.name AS dst")
        .await?;
    assert_eq!(edges.len(), 1, "Should have exactly 1 KNOWS edge");
    assert_eq!(edges.rows()[0].get::<String>("src")?, "Alice");
    assert_eq!(edges.rows()[0].get::<String>("dst")?, "Bob");

    Ok(())
}

#[tokio::test]
async fn test_create_chain() -> Result<()> {
    let db = setup_db().await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (x:Person {name: 'X'})-[:KNOWS]->(y:Person {name: 'Y'})")
        .await?;
    tx.commit().await?;

    let node_count = db
        .session()
        .query("MATCH (n:Person) RETURN count(n) AS cnt")
        .await?;
    assert_eq!(node_count.rows()[0].get::<i64>("cnt")?, 2);

    let edge_count = db
        .session()
        .query("MATCH ()-[r:KNOWS]->() RETURN count(r) AS cnt")
        .await?;
    assert_eq!(edge_count.rows()[0].get::<i64>("cnt")?, 1);

    Ok(())
}

#[tokio::test]
async fn test_create_returns_created_data() -> Result<()> {
    let db = setup_db().await?;

    let session = db.session();
    let tx = session.tx().await?;
    let result = tx
        .query("CREATE (n:Person {name: 'Alice', age: 30}) RETURN n.name AS name, n.age AS age")
        .await?;
    tx.commit().await?;

    assert_eq!(
        result.len(),
        1,
        "CREATE...RETURN should return created node"
    );
    assert_eq!(result.rows()[0].get::<String>("name")?, "Alice");
    assert_eq!(result.rows()[0].get::<i32>("age")?, 30);

    Ok(())
}

#[tokio::test]
async fn test_create_multiple_labels() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .done()
        .label("Employee")
        .property("name", DataType::String)
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (n:Person:Employee {name: 'Alice'})")
        .await?;
    tx.commit().await?;

    // Should be findable by either label
    let by_person = db
        .session()
        .query("MATCH (n:Person) RETURN n.name AS name")
        .await?;
    assert_eq!(by_person.len(), 1);
    assert_eq!(by_person.rows()[0].get::<String>("name")?, "Alice");

    let by_employee = db
        .session()
        .query("MATCH (n:Employee) RETURN n.name AS name")
        .await?;
    assert_eq!(by_employee.len(), 1);
    assert_eq!(by_employee.rows()[0].get::<String>("name")?, "Alice");

    Ok(())
}

// ── Error path tests ─────────────────────────────────────────────────

#[tokio::test]
async fn test_match_nonexistent_label_returns_empty() -> Result<()> {
    let db = setup_db().await?;

    let result = db.session().query("MATCH (n:NonExistent) RETURN n").await?;
    assert_eq!(
        result.len(),
        0,
        "Unknown label should return empty, not error"
    );

    Ok(())
}

#[tokio::test]
async fn test_delete_connected_node_without_detach_fails() -> Result<()> {
    let db = setup_db().await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")
        .await?;
    tx.commit().await?;

    // DELETE without DETACH should fail for connected node
    let session = db.session();
    let tx = session.tx().await?;
    let result = tx
        .execute("MATCH (n:Person {name: 'Alice'}) DELETE n")
        .await;

    assert!(
        result.is_err(),
        "DELETE connected node without DETACH should fail"
    );

    Ok(())
}
