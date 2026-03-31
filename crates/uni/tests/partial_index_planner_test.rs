// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for partial index query planner integration.
//!
//! These tests verify that the query planner correctly identifies and uses
//! partial indexes when the query's WHERE clause subsumes the index's WHERE clause.

use anyhow::Result;
use uni_db::Uni;

/// Test that a partial index is recognized when query predicates exactly match.
#[tokio::test]
async fn test_partial_index_exact_match() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Setup
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL User (email STRING, active BOOL)")
        .await?;
    tx.execute("CREATE INDEX idx_active FOR (u:User) ON (u.email) WHERE u.active = true")
        .await?;
    tx.execute("CREATE (:User {email: 'alice@example.com', active: true})")
        .await?;
    tx.execute("CREATE (:User {email: 'bob@example.com', active: false})")
        .await?;
    tx.commit().await?;

    // Query with matching predicate - should use partial index
    let result = db
        .session().query(
            "MATCH (u:User) WHERE u.active = true AND u.email = 'alice@example.com' RETURN u.email AS email",
        )
        .await?;

    assert_eq!(result.len(), 1);
    assert_eq!(
        result.rows()[0].get::<String>("email")?,
        "alice@example.com"
    );

    Ok(())
}

/// Test that queries without the index predicate still work (but don't use the partial index).
#[tokio::test]
async fn test_partial_index_predicate_not_matching() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Setup
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL User (email STRING, active BOOL)")
        .await?;
    tx.execute("CREATE INDEX idx_active FOR (u:User) ON (u.email) WHERE u.active = true")
        .await?;
    tx.execute("CREATE (:User {email: 'alice@example.com', active: true})")
        .await?;
    tx.execute("CREATE (:User {email: 'bob@example.com', active: false})")
        .await?;
    tx.commit().await?;

    // Query WITHOUT the matching predicate - should NOT use partial index
    // But should still return correct results via full scan
    let result = db
        .session()
        .query("MATCH (u:User) WHERE u.email = 'bob@example.com' RETURN u.email AS email")
        .await?;

    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<String>("email")?, "bob@example.com");

    Ok(())
}

/// Test that variable name substitution works correctly.
#[tokio::test]
async fn test_partial_index_with_different_variable_name() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Setup - index uses 'u', query will use 'person'
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL User (email STRING, active BOOL)")
        .await?;
    tx.execute("CREATE INDEX idx_active FOR (u:User) ON (u.email) WHERE u.active = true")
        .await?;
    tx.execute("CREATE (:User {email: 'alice@example.com', active: true})")
        .await?;
    tx.commit().await?;

    // Query with DIFFERENT variable name but same predicate semantics
    let result = db
        .session().query(
            "MATCH (person:User) WHERE person.active = true AND person.email = 'alice@example.com' RETURN person.email AS email",
        )
        .await?;

    assert_eq!(result.len(), 1);
    assert_eq!(
        result.rows()[0].get::<String>("email")?,
        "alice@example.com"
    );

    Ok(())
}

/// Test composite index with partial predicate.
#[tokio::test]
async fn test_partial_index_composite_with_partial() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Setup: composite partial index
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Order (status STRING, customer_id STRING, total FLOAT)")
        .await?;
    tx.execute(
        "CREATE INDEX idx_pending FOR (o:Order) ON (o.customer_id, o.total) WHERE o.status = 'pending'",
    )
    .await?;
    tx.execute("CREATE (:Order {status: 'pending', customer_id: 'C001', total: 100.0})")
        .await?;
    tx.execute("CREATE (:Order {status: 'completed', customer_id: 'C001', total: 200.0})")
        .await?;
    tx.commit().await?;

    // Query matching partial index
    let result = db
        .session().query(
            "MATCH (o:Order) WHERE o.status = 'pending' AND o.customer_id = 'C001' RETURN o.total AS total",
        )
        .await?;

    assert_eq!(result.len(), 1);

    Ok(())
}

/// Test partial index with multiple conjuncts in the WHERE clause.
#[tokio::test]
async fn test_partial_index_multiple_conjuncts() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Setup: partial index with multiple conditions
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Product (name STRING, in_stock BOOL, active BOOL)")
        .await?;
    tx.execute(
        "CREATE INDEX idx_available FOR (p:Product) ON (p.name) WHERE p.in_stock = true AND p.active = true",
    )
    .await?;
    tx.execute("CREATE (:Product {name: 'Widget', in_stock: true, active: true})")
        .await?;
    tx.execute("CREATE (:Product {name: 'Gadget', in_stock: false, active: true})")
        .await?;
    tx.commit().await?;

    // Query matching all index conditions
    let result = db
        .session().query(
            "MATCH (p:Product) WHERE p.in_stock = true AND p.active = true AND p.name = 'Widget' RETURN p.name AS name",
        )
        .await?;

    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<String>("name")?, "Widget");

    Ok(())
}

/// Test that partial index works when query has extra predicates beyond the index condition.
#[tokio::test]
async fn test_partial_index_with_additional_predicates() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Setup
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL User (email STRING, active BOOL, age INT)")
        .await?;
    tx.execute("CREATE INDEX idx_active FOR (u:User) ON (u.email) WHERE u.active = true")
        .await?;
    tx.execute("CREATE (:User {email: 'alice@example.com', active: true, age: 30})")
        .await?;
    tx.execute("CREATE (:User {email: 'bob@example.com', active: true, age: 25})")
        .await?;
    tx.commit().await?;

    // Query with index predicate PLUS additional predicates
    let result = db
        .session()
        .query("MATCH (u:User) WHERE u.active = true AND u.age > 28 RETURN u.email AS email")
        .await?;

    assert_eq!(result.len(), 1);
    assert_eq!(
        result.rows()[0].get::<String>("email")?,
        "alice@example.com"
    );

    Ok(())
}

/// Test that partial index only matches when the predicate value matches.
#[tokio::test]
async fn test_partial_index_different_value_no_match() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Setup: index is for active = true
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL User (email STRING, active BOOL)")
        .await?;
    tx.execute("CREATE INDEX idx_active FOR (u:User) ON (u.email) WHERE u.active = true")
        .await?;
    tx.execute("CREATE (:User {email: 'alice@example.com', active: true})")
        .await?;
    tx.execute("CREATE (:User {email: 'bob@example.com', active: false})")
        .await?;
    tx.commit().await?;

    // Query with active = false (different value than index)
    // Should NOT use partial index but should still work
    let result = db
        .session()
        .query("MATCH (u:User) WHERE u.active = false RETURN u.email AS email")
        .await?;

    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<String>("email")?, "bob@example.com");

    Ok(())
}
