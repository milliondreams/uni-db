// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_unique_constraint_enforcement() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Define label with UNIQUE constraint
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL User (email STRING UNIQUE, name STRING)")
        .await?;
    tx.commit().await?;

    // Insert first user
    let tx = db.session().tx().await?;
    tx.execute("CREATE (u:User {email: 'alice@example.com', name: 'Alice'})")
        .await?;
    tx.commit().await?;

    // Insert second user with DIFFERENT email -> Should succeed
    let tx = db.session().tx().await?;
    tx.execute("CREATE (u:User {email: 'bob@example.com', name: 'Bob'})")
        .await?;
    tx.commit().await?;

    // Insert third user with DUPLICATE email -> Should fail
    let tx = db.session().tx().await?;
    let result = tx
        .execute("CREATE (u:User {email: 'alice@example.com', name: 'Alice2'})")
        .await;

    assert!(result.is_err(), "Duplicate email insert should have failed");
    let err = result.unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("constraint"),
        "Error should mention constraint violation: {}",
        err
    );

    Ok(())
}

#[tokio::test]
async fn test_not_null_constraint_enforcement() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Define label with NOT NULL constraint
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Product (id STRING, price FLOAT NOT NULL)")
        .await?;
    tx.commit().await?;

    // Insert valid product
    let tx = db.session().tx().await?;
    tx.execute("CREATE (p:Product {id: 'p1', price: 10.0})")
        .await?;
    tx.commit().await?;

    // Insert product with MISSING price -> Should fail
    let tx = db.session().tx().await?;
    let result = tx.execute("CREATE (p:Product {id: 'p2'})").await;

    assert!(
        result.is_err(),
        "Insert with missing NOT NULL property should have failed"
    );
    let err = result.unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("null"),
        "Error should mention null/missing property: {}",
        err
    );

    Ok(())
}

#[tokio::test]
async fn test_check_constraint_enforcement() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Define label
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Adult (age INT, name STRING)")
        .await?;
    // Add CHECK constraint: age > 18
    tx.execute("CREATE CONSTRAINT age_check ON (a:Adult) ASSERT a.age > 18")
        .await?;
    tx.commit().await?;

    // Insert valid adult (age 25 > 18)
    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:Adult {name: 'Alice', age: 25})")
        .await?;
    tx.commit().await?;

    // Insert invalid adult (age 10 < 18) -> Should fail
    let tx = db.session().tx().await?;
    let result = tx.execute("CREATE (a:Adult {name: 'Bob', age: 10})").await;

    assert!(
        result.is_err(),
        "Insert violating CHECK constraint should have failed"
    );
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("CHECK constraint"),
        "Error should mention CHECK constraint violation: {}",
        err
    );

    // Insert edge case (age 18 not > 18) -> Should fail
    let tx = db.session().tx().await?;
    let result_boundary = tx
        .execute("CREATE (a:Adult {name: 'Charlie', age: 18})")
        .await;
    assert!(
        result_boundary.is_err(),
        "Insert violating CHECK constraint (boundary) should have failed"
    );

    Ok(())
}
