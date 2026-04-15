// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Tests for NULL handling functions (COALESCE, nullIf).

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

#[tokio::test]
async fn test_null_handling_functions() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property_nullable("age", DataType::Int64)
        .property_nullable("nickname", DataType::String)
        .apply()
        .await?;

    // Insert data with varying null patterns
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice', age: 30})")
        .await?; // no nickname
    tx.execute("CREATE (:Person {name: 'Bob', nickname: 'Bobby'})")
        .await?; // no age
    tx.execute("CREATE (:Person {name: 'Charlie'})").await?; // no age, no nickname
    tx.commit().await?;

    // Test COALESCE: returns first non-null argument
    let results = db
        .session()
        .query("MATCH (n:Person) RETURN n.name, coalesce(n.nickname, n.age, 'Unknown') AS val ORDER BY n.name")
        .await?;

    // Alice: nickname=null, age=30 → 30
    assert_eq!(results.rows()[0].get::<String>("n.name")?, "Alice");
    let alice_val = results.rows()[0].value("val").unwrap();
    assert!(
        alice_val == &Value::Int(30) || alice_val == &Value::String("30".to_string()),
        "Alice coalesce should be 30, got: {:?}",
        alice_val
    );

    // Bob: nickname='Bobby', age=null → 'Bobby'
    assert_eq!(results.rows()[1].get::<String>("n.name")?, "Bob");
    assert_eq!(results.rows()[1].get::<String>("val")?, "Bobby");

    // Charlie: nickname=null, age=null → 'Unknown'
    assert_eq!(results.rows()[2].get::<String>("n.name")?, "Charlie");
    assert_eq!(results.rows()[2].get::<String>("val")?, "Unknown");

    // Test nullIf: returns null when value equals second argument
    let results = db
        .session()
        .query("MATCH (n:Person) RETURN n.name, nullIf(n.name, 'Bob') AS val ORDER BY n.name")
        .await?;

    // Alice: 'Alice' != 'Bob' → 'Alice'
    assert_eq!(results.rows()[0].get::<String>("val")?, "Alice");

    // Bob: 'Bob' == 'Bob' → null
    assert_eq!(results.rows()[1].value("val"), Some(&Value::Null));

    // Charlie: 'Charlie' != 'Bob' → 'Charlie'
    assert_eq!(results.rows()[2].get::<String>("val")?, "Charlie");

    Ok(())
}
