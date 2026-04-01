# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for async transactions: begin, commit, rollback, context manager."""

import pytest

import uni_db


@pytest.mark.asyncio
async def test_async_transaction_commit():
    """Test begin/commit transaction."""
    db = await uni_db.AsyncUni.temporary()
    await db.schema().label("Person").property("name", "string").apply()

    session = db.session()

    tx = await session.tx()
    await tx.query("CREATE (n:Person {name: 'Alice'})")
    await tx.commit()

    results = await session.query("MATCH (n:Person) RETURN n.name AS name")
    assert len(results) == 1
    assert results[0]["name"] == "Alice"


@pytest.mark.asyncio
async def test_async_transaction_rollback():
    """Test begin/rollback transaction."""
    db = await uni_db.AsyncUni.temporary()
    await db.schema().label("Person").property("name", "string").apply()

    session = db.session()

    # Insert a baseline
    tx_setup = await session.tx()
    await tx_setup.execute("CREATE (n:Person {name: 'Existing'})")
    await tx_setup.commit()

    tx = await session.tx()
    await tx.query("CREATE (n:Person {name: 'WillBeRolledBack'})")
    await tx.rollback()

    results = await session.query("MATCH (n:Person) RETURN n.name AS name")
    # Only the baseline should exist
    assert len(results) == 1
    assert results[0]["name"] == "Existing"


@pytest.mark.asyncio
async def test_async_transaction_context_manager_commit():
    """Test explicit commit inside async context manager persists data.

    The async context manager auto-rollbacks uncommitted transactions on exit.
    To persist data, commit explicitly inside the block.
    """
    db = await uni_db.AsyncUni.temporary()
    await db.schema().label("Item").property("value", "int").apply()

    session = db.session()

    tx = await session.tx()
    async with tx:
        await tx.execute("CREATE (n:Item {value: 42})")
        await tx.commit()

    results = await session.query("MATCH (n:Item) RETURN n.value AS value")
    assert len(results) == 1
    assert results[0]["value"] == 42


@pytest.mark.asyncio
async def test_async_transaction_context_manager_rollback_on_error():
    """Test async context manager auto-rollback on exception."""
    db = await uni_db.AsyncUni.temporary()
    await db.schema().label("Item").property("value", "int").apply()

    session = db.session()
    tx_setup = await session.tx()
    await tx_setup.execute("CREATE (n:Item {value: 0})")
    await tx_setup.commit()

    tx = await session.tx()
    with pytest.raises(ValueError):
        async with tx:
            await tx.query("CREATE (n:Item {value: 99})")
            raise ValueError("Something went wrong")

    results = await session.query("MATCH (n:Item) RETURN n.value AS value")
    assert len(results) == 1
    assert results[0]["value"] == 0


@pytest.mark.asyncio
async def test_async_transaction_query_with_params():
    """Test parameterized queries within a transaction."""
    db = await uni_db.AsyncUni.temporary()
    await db.schema().label("Person").property("name", "string").apply()

    session = db.session()

    tx = await session.tx()
    await tx.query("CREATE (n:Person {name: $name})", {"name": "Bob"})
    await tx.commit()

    results = await session.query(
        "MATCH (n:Person {name: 'Bob'}) RETURN n.name AS name",
    )
    assert len(results) == 1
    assert results[0]["name"] == "Bob"


@pytest.mark.asyncio
async def test_async_transaction_double_commit_raises():
    """Test that committing twice raises an error."""
    db = await uni_db.AsyncUni.temporary()
    await db.schema().label("X").apply()

    session = db.session()

    tx = await session.tx()
    await tx.query("CREATE (n:X {v: 1})")
    await tx.commit()

    with pytest.raises(uni_db.UniTransactionAlreadyCompletedError):
        await tx.commit()


@pytest.mark.asyncio
async def test_async_transaction_double_rollback_raises():
    """Test that rolling back twice raises an error."""
    db = await uni_db.AsyncUni.temporary()
    await db.schema().label("X").apply()

    session = db.session()

    tx = await session.tx()
    await tx.rollback()

    with pytest.raises(uni_db.UniTransactionAlreadyCompletedError):
        await tx.rollback()
