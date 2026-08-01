// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use std::time::Duration;
use uni_db::Uni;

#[tokio::test]
async fn test_query_timeout() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema().label("Node").apply().await?;

    // Create some data
    let tx = db.session().tx().await?;
    for _ in 0..100 {
        tx.execute("CREATE (:Node)").await?;
    }
    tx.commit().await?;

    // This query should be very fast, but let's set an extremely short timeout
    let res = db
        .session()
        .query_with("MATCH (n:Node) RETURN n")
        .timeout(Duration::from_nanos(1))
        .fetch_all()
        .await;

    // The typed variant, not a stringly-typed `Query`: an elapsed deadline is
    // exactly the case a dedicated error class exists for, and Python maps it
    // to `UniTimeoutError`.
    let err = res.err().expect("a 1ns timeout must reject");
    assert!(
        matches!(err, uni_db::UniError::Timeout { .. }),
        "expected UniError::Timeout, got: {err:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_query_memory_limit() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema().label("Node").apply().await?;

    // Create some data
    let tx = db.session().tx().await?;
    for _ in 0..100 {
        tx.execute("CREATE (:Node)").await?;
    }
    tx.commit().await?;

    // Set an extremely small memory limit
    let res = db
        .session()
        .query_with("MATCH (n:Node) RETURN n")
        .max_memory(100) // 100 bytes
        .fetch_all()
        .await;

    assert!(res.is_err());
    let err_msg = res.err().unwrap().to_string();
    assert!(err_msg.contains("Query exceeded memory limit"));

    Ok(())
}

// ---------------------------------------------------------------------------
// Cursor parity — the streaming path must enforce the same limits
// ---------------------------------------------------------------------------
//
// `QueryBuilder::cursor` advertises `.timeout()`, `.max_memory()` and
// `.cancellation_token()`, but enforcement lived entirely in the materializing
// path: `execute_plan_internal` wraps execution in `tokio::time::timeout` and
// calls `enforce_memory_limit`, while `execute_cursor_internal_with_config`
// did neither and never received the token at all. Every limit the builder
// accepted was silently inert once `.cursor()` was the terminal.
//
// The cooperative `GraphContext::check_timeout` is not a substitute: it only
// fires where an operator happens to call it, and no scan/join/traverse plan
// exercised here reaches one. `test_concurrent_query_cancellation_isolation`
// documents the same weakness from the other side — it accepts a cancelled
// query "racing to completion" as a valid outcome.

/// Drain a cursor to exhaustion, returning the first streamed error.
async fn drain_cursor(mut cursor: uni_query::QueryCursor) -> Option<uni_db::UniError> {
    while let Some(batch) = cursor.next_batch().await {
        if let Err(e) = batch {
            return Some(e);
        }
    }
    None
}

async fn seeded_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema().label("Node").apply().await?;
    let tx = db.session().tx().await?;
    for _ in 0..100 {
        tx.execute("CREATE (:Node)").await?;
    }
    tx.commit().await?;
    Ok(db)
}

#[tokio::test]
async fn test_query_memory_limit_applies_to_cursor() -> Result<()> {
    let db = seeded_db().await?;

    // Identical query and limit to `test_query_memory_limit`, which rejects.
    let cursor = db
        .session()
        .query_with("MATCH (n:Node) RETURN n")
        .max_memory(100)
        .cursor()
        .await?;

    let err = drain_cursor(cursor).await.expect(
        "cursor streamed every row under a 100-byte ceiling; `fetch_all` \
         rejects the same query with the same limit",
    );
    assert!(
        err.to_string().contains("Query exceeded memory limit"),
        "expected the memory-limit error `fetch_all` produces, got: {err}"
    );

    Ok(())
}

#[tokio::test]
async fn test_query_timeout_applies_to_cursor() -> Result<()> {
    let db = seeded_db().await?;

    // Identical query and timeout to `test_query_timeout`, which rejects.
    let cursor = db
        .session()
        .query_with("MATCH (n:Node) RETURN n")
        .timeout(Duration::from_nanos(1))
        .cursor()
        .await?;

    let err = drain_cursor(cursor).await.expect(
        "cursor ran to completion under a 1ns timeout; `fetch_all` rejects \
         the same query with the same timeout",
    );
    assert!(
        matches!(err, uni_db::UniError::Timeout { .. }),
        "expected the timeout error `fetch_all` produces, got: {err:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_cancellation_token_aborts_a_cursor() -> Result<()> {
    let db = seeded_db().await?;

    // Pre-cancelled: the outcome must be deterministic, not a race.
    let token = tokio_util::sync::CancellationToken::new();
    token.cancel();

    let cursor = db
        .session()
        .query_with("MATCH (n:Node) RETURN n")
        .cancellation_token(token)
        .cursor()
        .await?;

    let err = drain_cursor(cursor).await.expect(
        "cursor streamed every row despite an already-cancelled token; \
         `QueryBuilder::cursor` never read `self.cancellation_token`",
    );
    assert!(
        matches!(err, uni_db::UniError::Cancelled),
        "expected UniError::Cancelled, got: {err:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_cursor_tolerates_polling_past_exhaustion() -> Result<()> {
    // The limit guard wraps the row stream in `stream::unfold`, which panics
    // outright if polled after it has yielded `None`. Two supported call
    // patterns do exactly that, and neither is exotic:
    //
    //   * an empty result set — the very first poll is also the last;
    //   * `fetch_one()` on a drained cursor, which polls again to confirm
    //     exhaustion and is how Python's cursor reports "no more rows".
    //
    // The pre-guard `map`/`flat_map` chain tolerated both, so the guard has to
    // be `.fuse()`d. Without it the panic crosses the pyo3 boundary as a hard
    // abort rather than a Python exception.
    let db = seeded_db().await?;

    // Empty result: exhausted immediately, then polled once more.
    let mut empty = db
        .session()
        .query_with("MATCH (n:Node) WHERE n.missing = 'nope' RETURN n")
        .cursor()
        .await?;
    while let Some(batch) = empty.next_batch().await {
        batch?;
    }
    assert!(
        empty.next_batch().await.is_none(),
        "re-polling an exhausted empty cursor must stay None"
    );

    // Non-empty result, drained and then over-polled twice.
    let mut full = db
        .session()
        .query_with("MATCH (n:Node) RETURN n")
        .cursor()
        .await?;
    let mut seen = 0usize;
    while let Some(batch) = full.next_batch().await {
        seen += batch?.len();
    }
    assert_eq!(seen, 100, "cursor must stream every seeded row");
    assert!(full.next_batch().await.is_none());
    assert!(full.next_batch().await.is_none());

    Ok(())
}

// ---------------------------------------------------------------------------
// Transaction cursor — the same limits, on the other surface
// ---------------------------------------------------------------------------
//
// `TxQueryBuilder` accepts `.timeout()` and `.cancellation_token()`, and its
// `execute`/`fetch_all` terminals wrap the future in `tokio::time::timeout`.
// `cursor_inner` passed neither down, so a transaction cursor ran unbounded —
// the same defect the session cursor had, in the copy of the cursor-building
// code that lives next to it.
//
// Both surfaces now render an elapsed deadline as `UniError::Timeout`. They
// used to disagree — the session produced `Query { "Query timed out" }` — so
// the same condition surfaced as two different classes depending on which
// terminal the caller reached for.

async fn seeded_db_with_config(config: uni_db::UniConfig) -> Result<Uni> {
    let db = Uni::in_memory().config(config).build().await?;
    db.schema().label("Node").apply().await?;
    let tx = db.session().tx().await?;
    for _ in 0..100 {
        tx.execute("CREATE (:Node)").await?;
    }
    tx.commit().await?;
    Ok(db)
}

#[tokio::test]
async fn test_tx_cursor_honours_builder_timeout() -> Result<()> {
    let db = seeded_db().await?;
    let session = db.session();
    let tx = session.tx().await?;

    let cursor = tx
        .query_with("MATCH (n:Node) RETURN n")
        .timeout(Duration::from_nanos(1))
        .cursor()
        .await?;

    let err = drain_cursor(cursor).await.expect(
        "transaction cursor ran to completion under a 1ns timeout; the same \
         builder's `fetch_all` honours it",
    );
    assert!(
        matches!(err, uni_db::UniError::Timeout { .. }),
        "expected UniError::Timeout, got: {err:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_tx_cursor_honours_cancellation_token() -> Result<()> {
    let db = seeded_db().await?;
    let session = db.session();
    let tx = session.tx().await?;

    let token = tokio_util::sync::CancellationToken::new();
    token.cancel();

    let cursor = tx
        .query_with("MATCH (n:Node) RETURN n")
        .cancellation_token(token)
        .cursor()
        .await?;

    let err = drain_cursor(cursor).await.expect(
        "transaction cursor streamed every row despite an already-cancelled \
         token; `cursor_inner` never read it",
    );
    assert!(
        matches!(err, uni_db::UniError::Cancelled),
        "expected UniError::Cancelled, got: {err:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_tx_cursor_enforces_configured_memory_limit() -> Result<()> {
    // `TxQueryBuilder` has no `.max_memory()`, so the ceiling comes from
    // `UniConfig`. It was inert on both tx terminals.
    let mut config = uni_db::UniConfig::default();
    config.max_query_memory = 100;
    let db = seeded_db_with_config(config).await?;
    let session = db.session();
    let tx = session.tx().await?;

    let cursor = tx.query_with("MATCH (n:Node) RETURN n").cursor().await?;

    let err = drain_cursor(cursor)
        .await
        .expect("transaction cursor ignored the configured memory ceiling");
    assert!(
        err.to_string().contains("Query exceeded memory limit"),
        "expected the memory-limit error, got: {err}"
    );

    Ok(())
}

#[tokio::test]
async fn test_tx_fetch_all_enforces_configured_memory_limit() -> Result<()> {
    // Guards against fixing only the cursor: if the ceiling applied to the
    // streaming terminal but not the materializing one, the tx surface would
    // gain exactly the asymmetry this work exists to remove.
    let mut config = uni_db::UniConfig::default();
    config.max_query_memory = 100;
    let db = seeded_db_with_config(config).await?;
    let session = db.session();
    let tx = session.tx().await?;

    let res = tx.query_with("MATCH (n:Node) RETURN n").fetch_all().await;

    let err = res
        .err()
        .expect("transaction fetch_all ignored the configured memory ceiling")
        .to_string();
    assert!(
        err.to_string().contains("Query exceeded memory limit"),
        "expected the memory-limit error, got: {err}"
    );

    Ok(())
}

#[tokio::test]
async fn test_tx_cursor_streams_normally_without_limits() -> Result<()> {
    let db = seeded_db().await?;
    let session = db.session();
    let tx = session.tx().await?;

    let mut cursor = tx.query_with("MATCH (n:Node) RETURN n").cursor().await?;
    let mut seen = 0usize;
    while let Some(batch) = cursor.next_batch().await {
        seen += batch?.len();
    }
    assert_eq!(
        seen, 100,
        "an unconstrained tx cursor must stream every row"
    );
    assert!(cursor.next_batch().await.is_none());

    Ok(())
}

// ---------------------------------------------------------------------------
// Cancellation must reach the materializing terminals too
// ---------------------------------------------------------------------------
//
// Both cursors now abort on a cancelled token, but `fetch_all` on either
// surface does not: the token is handed to the executor, and the executor's
// only cooperative checkpoint (`GraphContext::check_timeout`) is never reached
// by a scan/join/traverse plan. So the surfaces were inconsistent in one
// direction before this work and the other direction after it.
//
// These pin the intended contract: a cancelled scope aborts the statement on
// every terminal, regardless of plan shape.

#[tokio::test]
async fn test_cancellation_token_aborts_fetch_all() -> Result<()> {
    let db = seeded_db().await?;

    let token = tokio_util::sync::CancellationToken::new();
    token.cancel();

    let res = db
        .session()
        .query_with("MATCH (n:Node) RETURN n")
        .cancellation_token(token)
        .fetch_all()
        .await;

    let err = res
        .err()
        .expect("session fetch_all ran to completion under an already-cancelled token");
    assert!(
        matches!(err, uni_db::UniError::Cancelled),
        "expected UniError::Cancelled, got: {err:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_tx_cancellation_token_aborts_fetch_all() -> Result<()> {
    let db = seeded_db().await?;
    let session = db.session();
    let tx = session.tx().await?;

    let token = tokio_util::sync::CancellationToken::new();
    token.cancel();

    let res = tx
        .query_with("MATCH (n:Node) RETURN n")
        .cancellation_token(token)
        .fetch_all()
        .await;

    let err = res
        .err()
        .expect("transaction fetch_all ran to completion under an already-cancelled token");
    assert!(
        matches!(err, uni_db::UniError::Cancelled),
        "expected UniError::Cancelled, got: {err:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_transaction_cancel_aborts_its_own_queries() -> Result<()> {
    // `Transaction::cancel()` cancels `Transaction.cancellation_token`, a child
    // of the session's token. That token was never handed to an executor, so
    // cancelling a transaction affected nothing in flight -- the whole point of
    // the API. No builder token here: the transaction's own scope must be
    // enough.
    let db = seeded_db().await?;
    let session = db.session();
    let tx = session.tx().await?;

    tx.cancel();

    let res = tx.query_with("MATCH (n:Node) RETURN n").fetch_all().await;
    let err = res
        .err()
        .expect("query ran to completion after `Transaction::cancel()`");
    assert!(
        matches!(err, uni_db::UniError::Cancelled),
        "expected UniError::Cancelled, got: {err:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_transaction_cancel_aborts_its_own_cursor() -> Result<()> {
    let db = seeded_db().await?;
    let session = db.session();
    let tx = session.tx().await?;

    tx.cancel();

    let cursor = tx.query_with("MATCH (n:Node) RETURN n").cursor().await?;
    let err = drain_cursor(cursor)
        .await
        .expect("cursor streamed every row after `Transaction::cancel()`");
    assert!(
        matches!(err, uni_db::UniError::Cancelled),
        "expected UniError::Cancelled, got: {err:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_uncancelled_transaction_is_unaffected() -> Result<()> {
    // Guards the inverse: wiring the transaction's scope into execution must
    // not make ordinary transactional queries fail.
    let db = seeded_db().await?;
    let session = db.session();
    let tx = session.tx().await?;

    let rows = tx
        .query_with("MATCH (n:Node) RETURN n")
        .fetch_all()
        .await?
        .into_rows();
    assert_eq!(rows.len(), 100);

    let mut cursor = tx.query_with("MATCH (n:Node) RETURN n").cursor().await?;
    let mut seen = 0usize;
    while let Some(batch) = cursor.next_batch().await {
        seen += batch?.len();
    }
    assert_eq!(seen, 100);

    Ok(())
}
