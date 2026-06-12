# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for the conflict-retry helpers (transact_with_retry & friends).

The multi-threaded tests exercise real concurrency: they only work because
the sync API releases the GIL across blocking database calls (the
`py.detach` sweep) — with the GIL held, a second Python thread could never
run a conflicting transaction.
"""

import threading

import pytest

import uni_db
from uni_db import (
    RETRIABLE_EXCEPTIONS,
    UniTransactionConflictError,
    async_transact_with_retry,
    execute_with_retry,
    transact_with_retry,
)


@pytest.fixture
def counter_db():
    """DB with a single Counter row at n = 0."""
    db = uni_db.UniBuilder.temporary().build()
    (
        db.schema()
        .label("Counter")
        .property("id", "string")
        .property("n", "int")
        .done()
        .apply()
    )
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (:Counter {id: 'x', n: 0})")
    tx.commit()
    return db


def _read_n(db):
    rows = db.session().query("MATCH (c:Counter {id: 'x'}) RETURN c.n AS n")
    return rows[0]["n"]


def test_retriable_exceptions_exported():
    assert UniTransactionConflictError in RETRIABLE_EXCEPTIONS
    assert uni_db.UniCommitTimeoutError in RETRIABLE_EXCEPTIONS
    assert uni_db.UniConstraintConflictError in RETRIABLE_EXCEPTIONS
    assert uni_db.UniLockTimeoutError in RETRIABLE_EXCEPTIONS


def test_bare_conflict_raises_transaction_conflict(counter_db):
    """Without retry, an SSI read-write conflict surfaces as
    UniTransactionConflictError (previously: the generic UniError)."""
    session_a = counter_db.session()
    session_b = counter_db.session()

    tx_a = session_a.tx()
    rows = tx_a.query("MATCH (c:Counter {id: 'x'}) RETURN c.n AS n")
    n = rows[0]["n"]

    # A concurrent transaction commits a write to the row tx_a read.
    tx_b = session_b.tx()
    tx_b.execute("MATCH (c:Counter {id: 'x'}) SET c.n = 100")
    tx_b.commit()

    tx_a.execute("MATCH (c:Counter {id: 'x'}) SET c.n = $v", {"v": n + 1})
    with pytest.raises(UniTransactionConflictError):
        tx_a.commit()


def test_transact_with_retry_single_thread(counter_db):
    """Uncontended path: body runs once, commit succeeds, result returned."""
    session = counter_db.session()

    def body(tx):
        rows = tx.query("MATCH (c:Counter {id: 'x'}) RETURN c.n AS n")
        n = rows[0]["n"]
        tx.execute("MATCH (c:Counter {id: 'x'}) SET c.n = $v", {"v": n + 1})
        return n + 1

    result = transact_with_retry(session, body)
    assert result == 1
    assert _read_n(counter_db) == 1


def test_transact_with_retry_no_lost_updates_under_contention(counter_db):
    """Two real threads increment the same counter through the retry helper;
    every increment must land (no lost updates, conflicts retried)."""
    increments_per_thread = 10
    errors = []

    def worker():
        try:
            session = counter_db.session()
            for _ in range(increments_per_thread):

                def body(tx):
                    rows = tx.query("MATCH (c:Counter {id: 'x'}) RETURN c.n AS n")
                    n = rows[0]["n"]
                    tx.execute(
                        "MATCH (c:Counter {id: 'x'}) SET c.n = $v",
                        {"v": n + 1},
                    )

                transact_with_retry(session, body, max_attempts=50)
        except Exception as e:  # pragma: no cover - failure reporting
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"worker errors: {errors}"
    assert _read_n(counter_db) == 2 * increments_per_thread, "lost update"


def test_retry_exhaustion_reraises(counter_db):
    """A body that always conflicts re-raises after max_attempts."""
    session = counter_db.session()
    attempts = []

    def always_conflicting(tx):
        attempts.append(1)
        # Read the row, then sabotage: a side transaction commits a write
        # to it before we do, guaranteeing an SSI conflict at commit.
        tx.query("MATCH (c:Counter {id: 'x'}) RETURN c.n AS n")
        side = counter_db.session()
        side_tx = side.tx()
        side_tx.execute("MATCH (c:Counter {id: 'x'}) SET c.n = c.n + 10")
        side_tx.commit()
        tx.execute("MATCH (c:Counter {id: 'x'}) SET c.n = 0")

    with pytest.raises(UniTransactionConflictError):
        transact_with_retry(session, always_conflicting, max_attempts=3)
    assert len(attempts) == 3, "body must run exactly max_attempts times"


def test_non_retriable_error_propagates_immediately(counter_db):
    """A non-retriable error (parse error) must not be retried."""
    session = counter_db.session()
    attempts = []

    def bad_body(tx):
        attempts.append(1)
        tx.execute("THIS IS NOT CYPHER")

    with pytest.raises(uni_db.UniParseError):
        transact_with_retry(session, bad_body, max_attempts=5)
    assert len(attempts) == 1


def test_execute_with_retry(counter_db):
    session = counter_db.session()
    execute_with_retry(session, "MATCH (c:Counter {id: 'x'}) SET c.n = c.n + 1")
    assert _read_n(counter_db) == 1


@pytest.mark.asyncio
async def test_async_transact_with_retry():
    db = await uni_db.AsyncUniBuilder.temporary().build()
    schema = (
        db.schema()
        .label("Counter")
        .property("id", "string")
        .property("n", "int")
        .done()
    )
    await schema.apply()
    session = db.session()
    tx = await session.tx()
    await tx.execute("CREATE (:Counter {id: 'x', n: 0})")
    await tx.commit()

    async def body(tx):
        rows = await tx.query("MATCH (c:Counter {id: 'x'}) RETURN c.n AS n")
        n = rows[0]["n"]
        await tx.execute("MATCH (c:Counter {id: 'x'}) SET c.n = $v", {"v": n + 1})
        return n + 1

    result = await async_transact_with_retry(session, body)
    assert result == 1


@pytest.fixture
def unique_email_db():
    """DB with a UNIQUE constraint on User.email and one row 'a@b.com'.

    A duplicate insert against this constraint fires *at execute time*
    (inline), not at commit — which is exactly the code path that
    distinguishes the prepared from the non-prepared error mapping.
    """
    db = uni_db.UniBuilder.temporary().build()
    db.schema().label("User").property("email", "string").done().apply()
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE CONSTRAINT ON (u:User) ASSERT u.email IS UNIQUE")
    tx.commit()

    seed = db.session().tx()
    seed.execute("CREATE (:User {email: 'a@b.com'})")
    seed.commit()
    return db


def test_nonprepared_execute_maps_to_typed_uni_error(unique_email_db):
    """Baseline / control: the NON-prepared execute path maps the inline
    constraint-violation UniError through `uni_error_to_pyerr`, so it
    surfaces as a typed `UniError` subclass — NOT a bare RuntimeError.

    (`sync_api.rs:194` maps correctly; this test documents the *correct*
    behavior the prepared path is supposed to mirror.)
    """
    tx = unique_email_db.session().tx()
    with pytest.raises(uni_db.UniError) as excinfo:
        tx.execute("CREATE (:User {email: 'a@b.com'})")
    # The raised class is a typed UniError, not a plain RuntimeError.
    assert not isinstance(excinfo.value, RuntimeError)
    assert "Constraint violation" in str(excinfo.value)


def test_prepared_execute_preserves_typed_uni_error(unique_email_db):
    """REGRESSION (Bug #7): a PREPARED statement's execute() must surface
    the underlying UniError through the typed exception hierarchy, exactly
    like the non-prepared path.

    `bindings/uni-db/src/types.rs:1513` wraps the Rust error in a bare
    `pyo3::exceptions::PyRuntimeError` instead of
    `crate::exceptions::uni_error_to_pyerr`. So an inline UniError returned
    by a prepared `execute()` (here: a UNIQUE-constraint duplicate insert,
    which fires at execute time) surfaces as a plain `RuntimeError`, NOT a
    typed `UniError` subclass. That makes retriable conflicts
    (Serialization/Constraint/LockTimeout) invisible to
    `transact_with_retry`, whose RETRIABLE_EXCEPTIONS are the typed classes.

    Today this is RED: the prepared execute raises bare `RuntimeError`,
    which is NOT a `uni_db.UniError`, so `pytest.raises(uni_db.UniError)`
    fails. After the one-line fix (use `uni_error_to_pyerr`) it raises a
    typed `UniError` subclass (the same `UniQueryError` the non-prepared
    path raises) and this test passes.
    """
    tx = unique_email_db.session().tx()
    pq = tx.prepare("CREATE (:User {email: 'a@b.com'})")

    # The prepared execute() must raise a typed UniError, just like the
    # non-prepared path. Today it raises a bare RuntimeError -> RED.
    with pytest.raises(uni_db.UniError) as excinfo:
        pq.execute()

    # And it must NOT be a bare RuntimeError — that is the precise bug:
    # the typed exception hierarchy is lost on the prepared path.
    assert not isinstance(excinfo.value, RuntimeError), (
        "prepared execute() lost the typed UniError hierarchy and surfaced "
        "a bare RuntimeError (Bug #7, types.rs:1513)"
    )
