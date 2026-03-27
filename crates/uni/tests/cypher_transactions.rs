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
    let tx = db.session().tx().await?;
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
        .session()
        .query("MATCH (n:Person) RETURN n.name, n.age ORDER BY n.name")
        .await?;
    assert_eq!(
        result.len(),
        3,
        "All 3 nodes should be visible after atomic commit"
    );

    let names: Vec<String> = result
        .rows()
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
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Person {name: 'Alice'})").await?;
        // tx is dropped here without commit
    }

    // Verify the node was NOT persisted (auto-rollback)
    let result = db.session().query("MATCH (n:Person) RETURN n.name").await?;
    assert_eq!(
        result.len(),
        0,
        "Dropped transaction should auto-rollback, node should not exist"
    );

    // Verify we can start a new transaction (writer is not locked)
    let tx2 = db.session().tx().await?;
    tx2.execute("CREATE (:Person {name: 'Bob'})").await?;
    tx2.commit().await?;

    let result = db.session().query("MATCH (n:Person) RETURN n.name").await?;
    assert_eq!(result.len(), 1, "New transaction after drop should work");
    let name: String = result.rows()[0].get("n.name")?;
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
        let tx = db.session().tx().await?;
        tx.execute(&format!("CREATE (:Counter {{value: {}}})", i))
            .await?;
        // Drop without commit
    }

    // Verify no counters exist (all auto-rolled back)
    let result = db
        .session()
        .query("MATCH (c:Counter) RETURN c.value")
        .await?;
    assert_eq!(
        result.len(),
        0,
        "All dropped transactions should auto-rollback"
    );

    // Verify we can still create a new successful transaction
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Counter {value: 999})").await?;
    tx.commit().await?;

    let result = db
        .session()
        .query("MATCH (c:Counter) RETURN c.value")
        .await?;
    assert_eq!(result.len(), 1);
    let value: i64 = result.rows()[0].get("c.value")?;
    assert_eq!(value, 999);

    Ok(())
}

// ---------------------------------------------------------------------------
// Phase 5: Commit-Time Serialization Tests (Private L0 per Transaction)
// ---------------------------------------------------------------------------

/// Two sessions create transactions concurrently — both succeed.
/// (Was impossible before: begin-time locking blocked the second tx.)
#[tokio::test]
async fn test_concurrent_transaction_creation() -> anyhow::Result<()> {
    use uni_db::Uni;

    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Node")
        .property("id", DataType::Int)
        .apply()
        .await?;

    let s1 = db.session();
    let s2 = db.session();

    // Both sessions can start transactions concurrently
    let tx1 = s1.tx().await?;
    let tx2 = s2.tx().await?;

    // Both can execute mutations
    tx1.execute("CREATE (:Node {id: 1})").await?;
    tx2.execute("CREATE (:Node {id: 2})").await?;

    // Commit sequentially — both succeed
    let r1 = tx1.commit().await?;
    let r2 = tx2.commit().await?;

    assert!(r1.mutations_committed > 0, "tx1 committed mutations");
    assert!(r2.mutations_committed > 0, "tx2 committed mutations");

    // Both nodes visible
    let result = db
        .session()
        .query("MATCH (n:Node) RETURN n.id ORDER BY n.id")
        .await?;
    assert_eq!(result.len(), 2);
    let ids: Vec<i64> = result
        .rows()
        .iter()
        .map(|r| r.get("n.id").unwrap())
        .collect();
    assert_eq!(ids, vec![1, 2]);

    Ok(())
}

/// Transaction reads its own uncommitted writes (read-your-writes).
#[tokio::test]
async fn test_transaction_read_your_writes() -> anyhow::Result<()> {
    use uni_db::Uni;

    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Item {name: 'Alpha'})").await?;

    // Transaction should see its own write
    let result = tx.query("MATCH (i:Item) RETURN i.name").await?;
    assert_eq!(result.len(), 1);
    let name: String = result.rows()[0].get("i.name")?;
    assert_eq!(name, "Alpha");

    // Add another item and read both
    tx.execute("CREATE (:Item {name: 'Beta'})").await?;
    let result = tx
        .query("MATCH (i:Item) RETURN i.name ORDER BY i.name")
        .await?;
    assert_eq!(result.len(), 2);

    tx.rollback();
    Ok(())
}

/// Transaction A's writes are invisible to Session B.
#[tokio::test]
async fn test_transaction_isolation() -> anyhow::Result<()> {
    use uni_db::Uni;

    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Secret")
        .property("val", DataType::Int)
        .apply()
        .await?;

    // Create a baseline node visible to all
    db.session().execute("CREATE (:Secret {val: 0})").await?;

    let s1 = db.session();
    let s2 = db.session();

    let tx = s1.tx().await?;
    tx.execute("CREATE (:Secret {val: 42})").await?;

    // Transaction sees both nodes (baseline + uncommitted)
    let tx_result = tx
        .query("MATCH (s:Secret) RETURN s.val ORDER BY s.val")
        .await?;
    assert_eq!(tx_result.len(), 2, "tx sees baseline + own write");

    // Session B should only see the baseline node
    let s2_result = s2.query("MATCH (s:Secret) RETURN s.val").await?;
    assert_eq!(
        s2_result.len(),
        1,
        "session B should not see tx's uncommitted writes"
    );
    let val: i64 = s2_result.rows()[0].get("s.val")?;
    assert_eq!(val, 0, "session B sees only the committed baseline node");

    tx.commit().await?;

    // After commit, Session B sees both
    let s2_result = s2
        .query("MATCH (s:Secret) RETURN s.val ORDER BY s.val")
        .await?;
    assert_eq!(
        s2_result.len(),
        2,
        "after commit, session B sees both nodes"
    );

    Ok(())
}

/// Two transactions commit sequentially — both succeed, version increments.
#[tokio::test]
async fn test_sequential_transaction_commits() -> anyhow::Result<()> {
    use uni_db::Uni;

    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Seq")
        .property("n", DataType::Int)
        .apply()
        .await?;

    let s1 = db.session();
    let s2 = db.session();

    let tx1 = s1.tx().await?;
    let tx2 = s2.tx().await?;

    tx1.execute("CREATE (:Seq {n: 1})").await?;
    tx2.execute("CREATE (:Seq {n: 2})").await?;

    let r1 = tx1.commit().await?;
    let r2 = tx2.commit().await?;

    // Version should increment: tx2's version > tx1's version
    assert!(
        r2.version > r1.version,
        "second commit should have higher version: {} > {}",
        r2.version,
        r1.version
    );

    // Both nodes exist
    let result = db
        .session()
        .query("MATCH (s:Seq) RETURN s.n ORDER BY s.n")
        .await?;
    assert_eq!(result.len(), 2);

    Ok(())
}
