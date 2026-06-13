// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::Uni;
use uni_db::UniError;

/// Regression for bug #15: a failed statement must not leave half-applied rows
/// in `tx_l0` that a later `commit()` can persist (Neo4j-style rollback-only
/// atomicity).
///
/// A single multi-row `UNWIND ... CREATE` is fed three values where the last
/// duplicates the first on a UNIQUE-constrained property. Rows 1..N-1 are
/// written into the transaction's shared `tx_l0` before the duplicate row
/// fails constraint validation, leaving the buffer half-applied.
///
/// The contract under test is rollback-only: once any statement returns an
/// error the transaction is poisoned, so `commit()` must FAIL (with
/// [`UniError::TransactionRollbackOnly`]) instead of persisting the partial
/// rows. Only `rollback()`/drop succeeds. A fresh session must then observe
/// zero rows — nothing from the failed statement persisted.
#[tokio::test]
async fn failed_statement_does_not_partially_commit() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Declare a UNIQUE constraint on :P(id).
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL P (id STRING UNIQUE)").await?;
    tx.commit().await?;

    // One multi-row statement whose last row duplicates the first row's id.
    let tx = db.session().tx().await?;
    let res = tx
        .execute("UNWIND ['a', 'b', 'a'] AS x CREATE (:P {id: x})")
        .await;
    assert!(res.is_err(), "statement must fail on the duplicate row");

    // Rollback-only: the poisoned transaction must refuse to commit rather than
    // persist its half-applied rows.
    let commit = tx.commit().await;
    assert!(
        matches!(commit, Err(UniError::TransactionRollbackOnly)),
        "a tx with a failed statement must be rollback-only, not committable; got {commit:?}"
    );

    // Fresh session: nothing from the failed statement persisted.
    let count = db
        .session()
        .query("MATCH (p:P) RETURN count(p) AS c")
        .await?;
    assert_eq!(
        count.rows()[0].get::<i64>("c").unwrap(),
        0,
        "failed statement must persist nothing, but partial rows were committed"
    );

    Ok(())
}
