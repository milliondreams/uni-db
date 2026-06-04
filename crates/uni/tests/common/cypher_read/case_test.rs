// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Tests for CASE expressions (generic CASE WHEN and simple CASE).

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn test_case_expression() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property_nullable("age", DataType::Int64)
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice', age: 10})")
        .await?;
    tx.execute("CREATE (:Person {name: 'Bob', age: 20})")
        .await?;
    tx.execute("CREATE (:Person {name: 'Charlie', age: 30})")
        .await?;
    tx.commit().await?;

    // 1. Generic CASE WHEN
    let results = db
        .session()
        .query(
            "MATCH (n:Person) RETURN n.name AS name, \
             CASE WHEN n.age < 15 THEN 'Child' WHEN n.age < 25 THEN 'Teen' ELSE 'Adult' END AS category \
             ORDER BY name",
        )
        .await?;

    assert_eq!(results.len(), 3);
    assert_eq!(results.rows()[0].get::<String>("name")?, "Alice");
    assert_eq!(results.rows()[0].get::<String>("category")?, "Child");
    assert_eq!(results.rows()[1].get::<String>("name")?, "Bob");
    assert_eq!(results.rows()[1].get::<String>("category")?, "Teen");
    assert_eq!(results.rows()[2].get::<String>("name")?, "Charlie");
    assert_eq!(results.rows()[2].get::<String>("category")?, "Adult");

    // 2. Simple CASE
    let results = db
        .session()
        .query(
            "MATCH (n:Person) RETURN n.name AS name, \
             CASE n.name WHEN 'Alice' THEN 1 WHEN 'Bob' THEN 2 ELSE 3 END AS rank \
             ORDER BY name",
        )
        .await?;

    assert_eq!(results.rows()[0].get::<i64>("rank")?, 1); // Alice
    assert_eq!(results.rows()[1].get::<i64>("rank")?, 2); // Bob
    assert_eq!(results.rows()[2].get::<i64>("rank")?, 3); // Charlie

    Ok(())
}
