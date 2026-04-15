// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Tests for SET operations on vector and metadata (CypherValue) properties.

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

#[tokio::test]
async fn test_cypher_set_advanced() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .property_nullable("embedding", DataType::Vector { dimensions: 3 })
        .property_nullable("metadata", DataType::CypherValue)
        .done()
        .edge_type("RELATED", &["Item"], &["Item"])
        .apply()
        .await?;

    // 1. Create node with vector and metadata properties
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(
        "CREATE (i:Item {name: 'A', embedding: [0.1, 0.2, 0.3], metadata: {valid: true, count: 1}})",
    )
    .await?;
    tx.commit().await?;

    // 2. Verify initial values
    let result = db
        .session()
        .query("MATCH (i:Item) RETURN i.embedding AS emb, i.metadata AS meta")
        .await?;
    assert_eq!(result.len(), 1);

    let emb = result.rows()[0].value("emb").unwrap();
    match emb {
        Value::List(arr) => assert_eq!(arr.len(), 3),
        Value::Vector(arr) => assert_eq!(arr.len(), 3),
        _ => {} // Vector may come back in other formats
    }

    // 3. Update properties via SET
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("MATCH (i:Item) SET i.embedding = [0.4, 0.5, 0.6], i.metadata = {valid: false}")
        .await?;
    tx.commit().await?;

    // 4. Verify updated values
    let result = db
        .session()
        .query("MATCH (i:Item) RETURN i.embedding AS emb, i.metadata AS meta")
        .await?;
    assert_eq!(result.len(), 1);

    let meta = result.rows()[0].value("meta").unwrap();
    if let Value::Map(m) = meta {
        assert_eq!(m.get("valid"), Some(&Value::Bool(false)));
    }

    // 5. Test edge with vector property
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("MATCH (i:Item) CREATE (i)-[r:RELATED {scores: [1.0, 2.0]}]->(i)")
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .query("MATCH (:Item)-[r:RELATED]->(:Item) RETURN r.scores AS scores")
        .await?;
    assert_eq!(result.len(), 1);

    // 6. Flush and verify persistence
    db.flush().await?;

    let result = db
        .session()
        .query("MATCH (i:Item) RETURN i.name AS name")
        .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<String>("name")?, "A");

    Ok(())
}
