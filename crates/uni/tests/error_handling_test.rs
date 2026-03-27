// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::time::Duration;
use uni_db::{DataType, Uni, UniError};

/// Create a transaction with a 1ms timeout, sleep past the deadline,
/// then verify that `execute` returns `TransactionExpired`.
#[tokio::test]
async fn test_transaction_expired() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Node")
        .property("name", DataType::String)
        .apply()
        .await?;

    let session = db.session();
    let tx = session
        .tx_with()
        .timeout(Duration::from_millis(1))
        .start()
        .await?;

    // Wait well past the deadline
    tokio::time::sleep(Duration::from_millis(10)).await;

    let err = tx
        .execute("CREATE (:Node {name: 'late'})")
        .await
        .expect_err("should fail with TransactionExpired");

    assert!(
        matches!(err, UniError::TransactionExpired { .. }),
        "expected TransactionExpired, got: {err:?}"
    );

    Ok(())
}

/// Same as above but try `commit` after the deadline expires.
#[tokio::test]
async fn test_transaction_expired_on_commit() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;

    let session = db.session();
    let tx = session
        .tx_with()
        .timeout(Duration::from_millis(1))
        .start()
        .await?;

    tokio::time::sleep(Duration::from_millis(10)).await;

    let err = tx
        .commit()
        .await
        .expect_err("should fail with TransactionExpired");

    assert!(
        matches!(err, UniError::TransactionExpired { .. }),
        "expected TransactionExpired, got: {err:?}"
    );

    Ok(())
}

/// Verify that `Session::cancel()` does not panic and the session
/// remains usable for subsequent queries.
#[tokio::test]
async fn test_session_cancel() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;

    let mut session = db.session();

    // Cancel with no in-flight queries — must not panic
    session.cancel();

    // Session should still be usable after cancel
    let result = session.query("RETURN 1 AS n").await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<i64>("n")?, 1);

    Ok(())
}

/// Drop a transaction without commit/rollback and verify the write guard
/// is released so a new transaction can be created on the same session.
#[tokio::test]
async fn test_write_guard_released_on_tx_drop() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Item")
        .property("val", DataType::Int64)
        .apply()
        .await?;

    let session = db.session();

    // Create and drop a transaction (implicit rollback via Drop)
    {
        let tx = session.tx().await?;
        tx.execute("CREATE (:Item {val: 1})").await?;
        // tx dropped here without commit or rollback
    }

    // The write guard should be released — creating another tx must succeed
    let tx2 = session.tx().await?;
    tx2.execute("CREATE (:Item {val: 2})").await?;
    tx2.commit().await?;

    // Only the second transaction's data should be visible
    let result = db
        .session()
        .query("MATCH (i:Item) RETURN i.val AS val ORDER BY val")
        .await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<i64>("val")?, 2);

    Ok(())
}

/// Create a streaming appender, abort it, then verify the write guard
/// is released so a new transaction can be created on the same session.
#[tokio::test]
async fn test_write_guard_released_on_appender_abort() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Row")
        .property("x", DataType::Int64)
        .apply()
        .await?;

    let session = db.session();

    // Create an appender and abort it
    {
        let mut appender = session.appender("Row").build()?;
        appender.abort();
    }

    // The write guard should be released — creating a tx must succeed
    let tx = session.tx().await?;
    tx.execute("CREATE (:Row {x: 42})").await?;
    tx.commit().await?;

    let result = db.session().query("MATCH (r:Row) RETURN r.x AS x").await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<i64>("x")?, 42);

    Ok(())
}

/// Open the database in read-only mode and verify that write operations
/// are rejected with `UniError::ReadOnly`.
#[tokio::test]
async fn test_read_only_rejects_writes() -> anyhow::Result<()> {
    let db = Uni::in_memory().read_only().build().await?;
    let session = db.session();

    // Starting a transaction should fail on a read-only database
    let err = match session.tx().await {
        Err(e) => e,
        Ok(_) => panic!("expected ReadOnly error from tx() on read-only database"),
    };

    assert!(
        matches!(err, UniError::ReadOnly { .. }),
        "expected ReadOnly, got: {err:?}"
    );

    // Auto-committed execute should also fail (the error surfaces as a Query
    // error from the execution layer since there is no Writer, rather than
    // ReadOnly which is only returned for pinned sessions).
    let err = session
        .execute("CREATE (:Anything {x: 1})")
        .await
        .expect_err("should fail on read-only database");

    // Accept either ReadOnly or a Query error indicating no writer
    assert!(
        matches!(err, UniError::ReadOnly { .. } | UniError::Query { .. }),
        "expected ReadOnly or Query error on execute, got: {err:?}"
    );

    // Reads should still work
    let result = session.query("RETURN 42 AS val").await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<i64>("val")?, 42);

    Ok(())
}
