// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Tests for reader isolation: data visibility through create, flush, delete lifecycle.

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn test_reader_isolation_lifecycle() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    // 1. Insert via transaction (data in L0, no flush)
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.commit().await?;

    // 2. Query should see committed data
    let result = db
        .session()
        .query("MATCH (n:Person {name: 'Alice'}) RETURN n.name AS name")
        .await?;
    assert_eq!(result.len(), 1, "Should find Alice after commit");
    assert_eq!(result.rows()[0].get::<String>("name")?, "Alice");

    // 3. Flush to storage
    db.flush().await?;

    // 4. Query should still see data after flush
    let result = db
        .session()
        .query("MATCH (n:Person {name: 'Alice'}) RETURN n.name AS name")
        .await?;
    assert_eq!(result.len(), 1, "Should find Alice after flush");

    // 5. Delete via transaction
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("MATCH (n:Person {name: 'Alice'}) DELETE n")
        .await?;
    tx.commit().await?;

    // 6. Query should NOT see deleted data (tombstone in L0)
    let result = db
        .session()
        .query("MATCH (n:Person {name: 'Alice'}) RETURN n.name")
        .await?;
    assert_eq!(result.len(), 0, "Should NOT find Alice (L0 tombstone)");

    // 7. Flush deletion to storage
    db.flush().await?;

    // 8. Still should not see deleted data
    let result = db
        .session()
        .query("MATCH (n:Person {name: 'Alice'}) RETURN n.name")
        .await?;
    assert_eq!(
        result.len(),
        0,
        "Should NOT find Alice (deleted in storage)"
    );

    Ok(())
}
