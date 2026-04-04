// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Reproduction for BUG-4: COALESCE + toString(id()) Arrow UTF-8 encoding
//! error when schema includes CypherValue + DateTime columns.

use uni_db::{DataType, Uni};

/// PASSES: COALESCE works with a simple String-only schema.
#[tokio::test]
async fn coalesce_ok_with_string_only_schema() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("NodeA")
        .property("name", DataType::String)
        .done()
        .label("NodeB")
        .property("name", DataType::String)
        .done()
        .edge_type("LINKS", &["NodeA"], &["NodeB"])
        .done()
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:NodeA {name: 'a'})-[:LINKS]->(:NodeB {name: 'b'})")
        .await?;
    tx.commit().await?;

    let r = session
        .query("MATCH (a:NodeA)-[:LINKS]->(b) RETURN COALESCE(a.name, toString(id(a)))")
        .await?;
    assert_eq!(r.rows().len(), 1);
    Ok(())
}

/// FAILS: Same COALESCE query errors when schema has CypherValue + DateTime.
#[tokio::test]
async fn coalesce_fails_with_cyphervalue_datetime_schema() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("File")
        .property("path", DataType::String)
        .property("content", DataType::String)
        .property("hash", DataType::String)
        .property("size", DataType::Int64)
        .property("created_at", DataType::DateTime)
        .property("updated_at", DataType::DateTime)
        .done()
        .apply()
        .await?;

    db.schema()
        .label("Agent")
        .property("agent_id", DataType::String)
        .property_nullable("name", DataType::String)
        .property_nullable("first_seen", DataType::DateTime)
        .property_nullable("last_seen", DataType::DateTime)
        .property_nullable("metadata", DataType::CypherValue)
        .done()
        .edge_type("CREATED_BY", &["File"], &["Agent"])
        .property("timestamp", DataType::DateTime)
        .property_nullable("reason", DataType::String)
        .done()
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(
        "CREATE (:Agent {agent_id: 'a1', name: 'a1', \
         first_seen: datetime(), last_seen: datetime(), metadata: {}})",
    )
    .await?;
    tx.execute(
        "CREATE (:File {path: 'f.txt', content: 'x', hash: 'h', \
         size: 1, created_at: datetime(), updated_at: datetime()})",
    )
    .await?;
    tx.execute(
        "MATCH (a:Agent {agent_id: 'a1'}), (f:File {path: 'f.txt'}) \
         CREATE (f)-[:CREATED_BY {timestamp: datetime(), reason: 'test'}]->(a)",
    )
    .await?;
    tx.commit().await?;

    // Individual parts work:
    session
        .query("MATCH (a:Agent {agent_id: 'a1'}) MATCH (n)-[:CREATED_BY]->(a) RETURN n.path")
        .await?;
    session
        .query(
            "MATCH (a:Agent {agent_id: 'a1'}) MATCH (n)-[:CREATED_BY]->(a) \
             RETURN toString(id(n))",
        )
        .await?;

    // COALESCE previously triggered Arrow UTF-8 error:
    let result = session
        .query(
            "MATCH (a:Agent {agent_id: 'a1'}) MATCH (n)-[:CREATED_BY]->(a) \
             RETURN COALESCE(n.path, toString(id(n))) as target",
        )
        .await;

    assert!(
        result.is_ok(),
        "COALESCE should work but got: {}",
        result.unwrap_err()
    );

    Ok(())
}
