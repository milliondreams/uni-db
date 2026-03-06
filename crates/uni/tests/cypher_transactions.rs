// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;
use uni_db::unival;

use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_explicit_transactions() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );

    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let planner = QueryPlanner::new(schema_manager.schema());

    // 2. BEGIN Transaction
    executor
        .execute(
            planner.plan(uni_cypher::parse("BEGIN")?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;

    // 3. CREATE in transaction
    executor
        .execute(
            planner.plan(uni_cypher::parse("CREATE (n:Person {name: 'Alice'})")?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;

    // 4. Query in transaction (Should see Alice)
    let res = executor
        .execute(
            planner.plan(uni_cypher::parse("MATCH (n:Person) RETURN n.name")?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get("n.name"), Some(&unival!("Alice")));

    // 5. ROLLBACK
    executor
        .execute(
            planner.plan(uni_cypher::parse("ROLLBACK")?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;

    // 6. Query after rollback (Should be empty)
    let res = executor
        .execute(
            planner.plan(uni_cypher::parse("MATCH (n:Person) RETURN n.name")?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;
    assert_eq!(res.len(), 0);

    // 7. COMMIT Transaction
    executor
        .execute(
            planner.plan(uni_cypher::parse("BEGIN")?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;
    executor
        .execute(
            planner.plan(uni_cypher::parse("CREATE (n:Person {name: 'Bob'})")?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;
    executor
        .execute(
            planner.plan(uni_cypher::parse("COMMIT")?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;

    // 8. Query after commit (Should see Bob)
    let res = executor
        .execute(
            planner.plan(uni_cypher::parse("MATCH (n:Person) RETURN n.name")?)?,
            &prop_manager,
            &HashMap::new(),
        )
        .await?;
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get("n.name"), Some(&unival!("Bob")));

    Ok(())
}

// ---------------------------------------------------------------------------
// Phase 4: Transaction Atomicity and Cleanup Tests
// ---------------------------------------------------------------------------

/// Test 1: Transaction commit succeeds and changes are applied atomically
///
/// Verifies that when a transaction commits:
/// 1. All mutations are applied atomically (both nodes appear together)
/// 2. Commit completes successfully even if there are multiple operations
/// 3. Timestamp preservation is handled by L0 merge (tested in Phase 1 unit tests)
///
/// Note: This test does NOT verify isolation (transaction-in-progress invisibility)
/// as that's a separate feature. We only verify atomic commit.
#[tokio::test]
async fn test_transaction_commit_atomic() -> anyhow::Result<()> {
    use uni_db::Uni;

    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property("age", DataType::Int)
        .apply()
        .await?;

    // Start transaction and create multiple nodes
    let tx = db.begin().await?;
    tx.execute("CREATE (:Person {name: 'Alice', age: 30})")
        .await?;
    tx.execute("CREATE (:Person {name: 'Bob', age: 25})")
        .await?;
    tx.execute("CREATE (:Person {name: 'Charlie', age: 35})")
        .await?;

    // Commit transaction
    tx.commit().await?;

    // Verify all nodes are visible after commit (atomicity)
    let result = db
        .query("MATCH (n:Person) RETURN n.name, n.age ORDER BY n.name")
        .await?;
    assert_eq!(
        result.len(),
        3,
        "All 3 nodes should be visible after atomic commit"
    );

    let names: Vec<String> = result
        .rows
        .iter()
        .map(|r| r.get("n.name").unwrap())
        .collect();
    assert_eq!(names, vec!["Alice", "Bob", "Charlie"]);

    Ok(())
}

/// Test 2: Dropped transaction auto-rolls back and doesn't block new transactions
///
/// Verifies that when a Transaction is dropped without explicit commit/rollback:
/// 1. The transaction is automatically rolled back (changes not visible)
/// 2. A new transaction can be started (writer is not permanently locked)
#[tokio::test]
async fn test_transaction_drop_auto_rollback() -> anyhow::Result<()> {
    use uni_db::Uni;

    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    // Create and drop a transaction without commit
    {
        let tx = db.begin().await?;
        tx.execute("CREATE (:Person {name: 'Alice'})").await?;
        // tx is dropped here without commit
    }

    // Verify the node was NOT persisted (auto-rollback)
    let result = db.query("MATCH (n:Person) RETURN n.name").await?;
    assert_eq!(
        result.len(),
        0,
        "Dropped transaction should auto-rollback, node should not exist"
    );

    // Verify we can start a new transaction (writer is not locked)
    let tx2 = db.begin().await?;
    tx2.execute("CREATE (:Person {name: 'Bob'})").await?;
    tx2.commit().await?;

    let result = db.query("MATCH (n:Person) RETURN n.name").await?;
    assert_eq!(result.len(), 1, "New transaction after drop should work");
    let name: String = result.rows[0].get("n.name")?;
    assert_eq!(name, "Bob");

    Ok(())
}

/// Test 3: New transaction can start immediately after dropped transaction
///
/// Specifically verifies the writer lock is properly released and can be reacquired.
#[tokio::test]
async fn test_transaction_drop_then_new_transaction() -> anyhow::Result<()> {
    use uni_db::Uni;

    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Counter")
        .property("value", DataType::Int)
        .apply()
        .await?;

    // Create and drop 3 transactions in sequence
    for i in 1..=3 {
        let tx = db.begin().await?;
        tx.execute(&format!("CREATE (:Counter {{value: {}}})", i))
            .await?;
        // Drop without commit
    }

    // Verify no counters exist (all auto-rolled back)
    let result = db.query("MATCH (c:Counter) RETURN c.value").await?;
    assert_eq!(
        result.len(),
        0,
        "All dropped transactions should auto-rollback"
    );

    // Verify we can still create a new successful transaction
    let tx = db.begin().await?;
    tx.execute("CREATE (:Counter {value: 999})").await?;
    tx.commit().await?;

    let result = db.query("MATCH (c:Counter) RETURN c.value").await?;
    assert_eq!(result.len(), 1);
    let value: i64 = result.rows[0].get("c.value")?;
    assert_eq!(value, 999);

    Ok(())
}
