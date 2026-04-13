// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::{DataType, UniSync};

#[test]
fn test_sync_api() -> Result<()> {
    // 1. Initialize
    let db = UniSync::in_memory()?;

    // 2. Schema (Sync)
    db.schema()
        .label("User")
        .property("name", DataType::String)
        .property("age", DataType::Int32)
        .apply()?;

    // 3. Execute via transaction
    let session = db.session();
    let tx = session.tx()?;
    tx.execute("CREATE (:User {name: 'Alice', age: 30})")?;
    tx.execute("CREATE (:User {name: 'Bob', age: 25})")?;
    tx.commit()?;

    // 4. Query
    let result = session.query("MATCH (u:User) RETURN u.name, u.age ORDER BY u.age")?;
    assert_eq!(result.len(), 2);

    let row0 = &result.rows()[0];
    assert_eq!(row0.get::<String>("u.name")?, "Bob");
    assert_eq!(row0.get::<i32>("u.age")?, 25);

    // 5. Transaction
    let tx = session.tx()?;
    tx.execute("CREATE (:User {name: 'Charlie', age: 40})")?;
    tx.commit()?;

    let result = session.query("MATCH (u:User) WHERE u.name = 'Charlie' RETURN u.age")?;
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<i32>("u.age")?, 40);

    Ok(())
}

#[test]
fn test_sync_api_rollback() -> Result<()> {
    let db = UniSync::in_memory()?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .apply()?;

    // Commit some baseline data
    let session = db.session();
    let tx = session.tx()?;
    tx.execute("CREATE (:Item {name: 'Baseline'})")?;
    tx.commit()?;

    // Start new transaction, add data, then drop without committing (rollback)
    {
        let tx = session.tx()?;
        tx.execute("CREATE (:Item {name: 'Uncommitted'})")?;
        // tx drops here without commit — implicit rollback
    }

    // Verify only baseline data exists
    let result = session.query("MATCH (i:Item) RETURN i.name AS name ORDER BY name")?;
    assert_eq!(result.len(), 1, "Rolled-back data should not be visible");
    assert_eq!(result.rows()[0].get::<String>("name")?, "Baseline");

    Ok(())
}

#[test]
fn test_sync_api_error_handling() -> Result<()> {
    let db = UniSync::in_memory()?;
    db.schema()
        .label("X")
        .property("v", DataType::Int32)
        .apply()?;

    let session = db.session();

    // Invalid Cypher syntax should return error, not panic
    let result = session.query("THIS IS NOT VALID CYPHER");
    assert!(
        result.is_err(),
        "Invalid Cypher should return error, not panic"
    );

    Ok(())
}

#[test]
fn test_sync_api_query_no_results() -> Result<()> {
    let db = UniSync::in_memory()?;
    db.schema()
        .label("Ghost")
        .property("name", DataType::String)
        .apply()?;

    // Query empty database
    let session = db.session();
    let result = session.query("MATCH (n:Ghost) RETURN n")?;
    assert_eq!(result.len(), 0, "Empty DB should return 0 rows, not error");

    Ok(())
}

#[test]
fn test_sync_api_edge_operations() -> Result<()> {
    let db = UniSync::in_memory()?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .done()
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()?;

    let session = db.session();
    let tx = session.tx()?;
    tx.execute("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")?;
    tx.commit()?;

    // Verify traversal
    let result = session.query(
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS src, b.name AS dst",
    )?;
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<String>("src")?, "Alice");
    assert_eq!(result.rows()[0].get::<String>("dst")?, "Bob");

    Ok(())
}
