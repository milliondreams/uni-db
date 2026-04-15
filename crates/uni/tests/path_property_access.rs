// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Tests for path expression property access: nodes(p)[0].name, etc.

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn test_path_property_access() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .done()
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()
        .await?;

    // Create a chain: Alice -> Bob
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")
        .await?;
    tx.commit().await?;

    // Test property access on path nodes: nodes(p)[0].name, nodes(p)[1].name
    let result = db
        .session()
        .query(
            "MATCH p = (a:Person)-[:KNOWS*1..2]->(b:Person) \
             RETURN nodes(p)[0].name AS first, nodes(p)[1].name AS second",
        )
        .await?;

    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<String>("first")?, "Alice");
    assert_eq!(result.rows()[0].get::<String>("second")?, "Bob");

    Ok(())
}
