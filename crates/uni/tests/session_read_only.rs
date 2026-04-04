// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression test for BUG-2: `session.query()` must reject mutation clauses
//! with a clear error instead of silently executing or discarding them.

use anyhow::Result;
use uni_db::Uni;

/// CREATE via session.query() must return an error.
#[tokio::test]
async fn test_session_query_rejects_create() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();

    let err = session
        .query("CREATE (:Foo {name: 'bar'})")
        .await
        .expect_err("session.query() should reject CREATE");

    let msg = err.to_string();
    assert!(
        msg.contains("read-only"),
        "error should mention read-only, got: {msg}"
    );
    assert!(
        msg.contains("session.tx()") || msg.contains("transaction"),
        "error should suggest using a transaction, got: {msg}"
    );

    Ok(())
}

/// SET via session.query() must return an error.
#[tokio::test]
async fn test_session_query_rejects_set() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Seed data
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Node {x: 1})").await?;
    tx.commit().await?;

    let session = db.session();
    let err = session
        .query("MATCH (n:Node) SET n.x = 99")
        .await
        .expect_err("session.query() should reject SET");

    assert!(
        err.to_string().contains("read-only"),
        "error should mention read-only, got: {}",
        err
    );

    Ok(())
}

/// MERGE via session.query() must return an error.
#[tokio::test]
async fn test_session_query_rejects_merge() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();

    let err = session
        .query("MERGE (:Foo {name: 'bar'})")
        .await
        .expect_err("session.query() should reject MERGE");

    assert!(
        err.to_string().contains("read-only"),
        "error should mention read-only, got: {}",
        err
    );

    Ok(())
}

/// DELETE via session.query() must return an error.
#[tokio::test]
async fn test_session_query_rejects_delete() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();

    let err = session
        .query("MATCH (n) DELETE n")
        .await
        .expect_err("session.query() should reject DELETE");

    assert!(
        err.to_string().contains("read-only"),
        "error should mention read-only, got: {}",
        err
    );

    Ok(())
}

/// Pure read queries via session.query() must still work.
#[tokio::test]
async fn test_session_query_allows_reads() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Seed data via transaction
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'}), (:Person {name: 'Bob'})")
        .await?;
    tx.commit().await?;

    // Read via session.query() should work fine
    let session = db.session();
    let result = session
        .query("MATCH (p:Person) RETURN p.name AS name ORDER BY name")
        .await?;

    assert_eq!(result.len(), 2);
    let name: String = result.rows()[0].get("name")?;
    assert_eq!(name, "Alice");

    Ok(())
}

/// Session.query_with() should also reject mutations.
#[tokio::test]
async fn test_session_query_with_rejects_mutations() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();

    let err = session
        .query_with("CREATE (:Foo {name: $name})")
        .param("name", "bar")
        .fetch_all()
        .await
        .expect_err("session.query_with() should reject CREATE");

    assert!(
        err.to_string().contains("read-only"),
        "error should mention read-only, got: {}",
        err
    );

    Ok(())
}

/// Mutations via transaction must still work (the correct pattern).
#[tokio::test]
async fn test_mutations_via_transaction_work() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();

    // Correct pattern: use tx for mutations
    let tx = session.tx().await?;
    tx.execute("CREATE (:Item {val: 42})").await?;
    tx.commit().await?;

    // Read back via session.query()
    let result = session
        .query("MATCH (i:Item) RETURN i.val AS val")
        .await?;
    assert_eq!(result.len(), 1);
    let val: i64 = result.rows()[0].get("val")?;
    assert_eq!(val, 42);

    Ok(())
}
