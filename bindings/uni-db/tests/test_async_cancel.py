# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Regression repro for Bug #8: AsyncTransaction.cancel() blocks behind an
in-flight operation instead of preempting it.

`bindings/uni-db/src/async_api.rs:1335` implements `cancel()` as

    let guard = inner.lock().await;   // <-- same Mutex every op holds
    let tx = active_tx(&guard)?;
    tx.cancel();

That `inner.lock().await` contends on the SAME `tokio::Mutex<Option<Transaction>>`
that `query`/`execute`/`locy` (async_api.rs:1159/1190/1227) hold across their
entire `.await`. So while a long-running op holds the guard, `cancel()` cannot
acquire the lock and therefore cannot fire the cancellation until the op it is
meant to cancel has already finished — defeating the whole point.

The correct, lock-free path is `cancellation_token()` ->
`PyCancellationToken.cancel()` (types.rs:1856): it fires the underlying
`tokio_util::sync::CancellationToken` without touching the transaction mutex.

These tests assert:
  1. `token.cancel()` fires immediately even while an op holds the guard
     (control: proves the lock-free path exists and works).
  2. `tx.cancel()` returns within 0.3s while an op holds the guard
     (RED today: it blocks behind the op's guard and times out; after the
     fix — cancel() made lock-free — it returns promptly and this passes).
"""

import asyncio
import time

import pytest

import uni_db

# Number of nodes whose self-join makes the "heavy" query take ~2s — long
# enough that a 0.3s wait_for(tx.cancel()) deterministically times out while
# the query holds the transaction mutex. (Measured ~2.0s at 5000; the margin
# over 0.3s is large, so this is not a flaky sleep-timing race.)
_HEAVY_NODES = 5000

# A self-join with a filter: O(n^2) row work, no early return, holds the
# transaction guard for the whole duration.
_HEAVY_QUERY = (
    "MATCH (a:N), (b:N) "
    "WHERE a.id < b.id AND (a.id + b.id) % 7 = 0 "
    "RETURN count(*) AS c"
)


async def _make_loaded_db():
    db = await uni_db.AsyncUni.temporary()
    await db.schema().label("N").property("id", "int").apply()
    session = db.session()
    tx0 = await session.tx()
    await tx0.execute(f"UNWIND range(0, {_HEAVY_NODES}) AS i CREATE (:N {{id: i}})")
    await tx0.commit()
    return db, session


def _spawn_heavy(tx):
    """Run the heavy self-join as a real asyncio Task.

    `tx.query(...)` returns a pyo3 future (not a native coroutine), so it
    must be wrapped in an `async def` before `create_task` will accept it.
    The wrapper acquires and holds the transaction mutex for the whole
    query duration — exactly the in-flight op that `cancel()` should be
    able to preempt.
    """

    async def _run():
        return await tx.query(_HEAVY_QUERY)

    return asyncio.create_task(_run())


@pytest.mark.asyncio
async def test_cancellation_token_fires_while_op_holds_guard():
    """CONTROL: the lock-free `cancellation_token().cancel()` path fires
    immediately even while a long op holds the transaction mutex.

    This is the path callers SHOULD use; it works today and must keep
    working after Bug #8 is fixed.
    """
    db, session = await _make_loaded_db()
    tx = await session.tx()

    # Grab the token BEFORE launching the heavy op (cancellation_token also
    # takes the mutex, so it must be acquired while the guard is free).
    token = await tx.cancellation_token()
    assert not token.is_cancelled()

    heavy = _spawn_heavy(tx)
    # Let the heavy op acquire the transaction mutex.
    await asyncio.sleep(0.05)
    assert not heavy.done(), "heavy op should still be running (holding the guard)"

    # Lock-free cancel: must be effectively instant even though the guard
    # is held by the in-flight query.
    t0 = time.monotonic()
    token.cancel()
    elapsed = time.monotonic() - t0
    assert token.is_cancelled()
    assert elapsed < 0.1, f"token.cancel() should be lock-free; took {elapsed:.3f}s"

    # Drain the heavy task so it does not leak into the next test.
    heavy.cancel()
    try:
        await heavy
    except BaseException:
        pass


@pytest.mark.asyncio
async def test_tx_cancel_does_not_block_behind_inflight_op():
    """RED (Bug #8): AsyncTransaction.cancel() must not block behind an
    in-flight operation.

    While a long-running query holds the transaction mutex, `tx.cancel()`
    queues behind it on the SAME mutex (async_api.rs:1335) and cannot fire
    until the op completes. We assert `tx.cancel()` returns within 0.3s —
    today it does NOT (it blocks ~2s behind the heavy query), so
    `asyncio.wait_for` raises `asyncio.TimeoutError` and this test FAILS
    (RED = bug present).

    After the fix (route `cancel()` through the lock-free cancellation
    token, like `PyCancellationToken.cancel()`), `tx.cancel()` returns
    promptly and this assertion passes (GREEN). There is intentionally no
    `pytest.raises(TimeoutError)` here: the timeout IS the failure, so this
    is a clean red->green regression test, not an assertion of the bug.
    """
    db, session = await _make_loaded_db()
    tx = await session.tx()

    heavy = _spawn_heavy(tx)
    # Let the heavy op acquire and hold the transaction mutex.
    await asyncio.sleep(0.05)
    assert not heavy.done(), "heavy op should still be running (holding the guard)"

    try:
        # Today: blocks behind the heavy query's guard -> TimeoutError (RED).
        # After fix: cancel() is lock-free -> returns well under 0.3s (GREEN).
        await asyncio.wait_for(tx.cancel(), timeout=0.3)
    finally:
        # Always drain the heavy task to avoid leaking it across tests,
        # regardless of whether the cancel timed out.
        heavy.cancel()
        try:
            await heavy
        except BaseException:
            pass


# ---------------------------------------------------------------------------
# Cancellation must be reachable, and effective, on the transaction surface
# ---------------------------------------------------------------------------
#
# `SessionQueryBuilder` exposed `.cancellation_token()` from the start;
# `TxQueryBuilder` never did, even though the Rust builder it wraps has the
# field. So the transaction surface had no way to attach a token from Python at
# all, and `Transaction.cancel()` cancelled a token that no executor was
# listening to.


def _seeded_sync_db():
    db = uni_db.UniBuilder.temporary().build()
    db.schema().label("Node").property("i", "int").apply()
    session = db.session()
    tx = session.tx()
    tx.execute("UNWIND range(1, 200) AS i CREATE (:Node {i: i})")
    tx.commit()
    session.flush()
    return db


def test_tx_query_builder_exposes_cancellation_token():
    db = _seeded_sync_db()
    tx = db.session().tx()
    builder = tx.query_with("MATCH (n:Node) RETURN n.i AS i")
    assert hasattr(builder, "cancellation_token"), (
        "TxQueryBuilder has no cancellation_token; the Rust builder it wraps "
        "has the field, so the transaction surface was unreachable from Python"
    )


def test_tx_builder_token_aborts_fetch_all():
    db = _seeded_sync_db()
    session = db.session()
    tx = session.tx()

    token = session.cancellation_token()
    token.cancel()

    with pytest.raises(uni_db.UniCancelledError):
        tx.query_with("MATCH (n:Node) RETURN n.i AS i").cancellation_token(
            token
        ).fetch_all()


def test_tx_builder_token_aborts_cursor():
    db = _seeded_sync_db()
    session = db.session()
    tx = session.tx()

    token = session.cancellation_token()
    token.cancel()

    with pytest.raises(uni_db.UniCancelledError):
        cursor = (
            tx.query_with("MATCH (n:Node) RETURN n.i AS i")
            .cancellation_token(token)
            .cursor()
        )
        cursor.fetch_all()


def test_transaction_cancel_aborts_its_own_queries():
    """`Transaction.cancel()` must reach work issued on that transaction."""
    db = _seeded_sync_db()
    tx = db.session().tx()
    tx.cancel()

    with pytest.raises(uni_db.UniCancelledError):
        tx.query_with("MATCH (n:Node) RETURN n.i AS i").fetch_all()


def test_session_cancel_aborts_plain_query():
    """`Session.cancel()` must reach the plain `query()` path, not just builders.

    `execute_cached` passed no token at any of its three call sites, so the most
    common way to run a query was the one `cancel()` could not touch.
    """
    db = _seeded_sync_db()
    session = db.session()
    token = session.cancellation_token()
    token.cancel()

    with pytest.raises(uni_db.UniCancelledError):
        session.query("MATCH (n:Node) RETURN n.i AS i")


def test_uncancelled_transaction_still_works():
    """Inverse guard: wiring the scope must not break ordinary transactions."""
    db = _seeded_sync_db()
    tx = db.session().tx()
    rows = tx.query_with("MATCH (n:Node) RETURN n.i AS i").fetch_all()
    assert len(rows) == 200
    cursor = tx.query_with("MATCH (n:Node) RETURN n.i AS i").cursor()
    assert len(cursor.fetch_all()) == 200


async def test_async_tx_query_builder_exposes_cancellation_token():
    db = await uni_db.AsyncUniBuilder.temporary().build()
    await db.schema().label("Node").property("i", "int").apply()
    session = db.session()
    tx = await session.tx()
    builder = tx.query_with("MATCH (n:Node) RETURN n.i AS i")
    assert hasattr(builder, "cancellation_token"), (
        "AsyncTxQueryBuilder must mirror the sync surface"
    )


def test_query_timeout_raises_uni_timeout_error():
    """An elapsed deadline is `UniTimeoutError` on every surface.

    The session path used to raise `UniQueryError` carrying the text "Query
    timed out" while the transaction path raised `UniTimeoutError`, so the same
    condition needed two different `except` clauses depending on which terminal
    produced it.
    """
    db = _seeded_sync_db()
    session = db.session()

    with pytest.raises(uni_db.UniTimeoutError):
        session.query_with("MATCH (n:Node) RETURN n.i AS i").timeout(1e-9).fetch_all()

    with pytest.raises(uni_db.UniTimeoutError):
        session.query_with("MATCH (n:Node) RETURN n.i AS i").timeout(
            1e-9
        ).cursor().fetch_all()

    tx = session.tx()
    with pytest.raises(uni_db.UniTimeoutError):
        tx.query_with("MATCH (n:Node) RETURN n.i AS i").timeout(1e-9).fetch_all()
