# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for async Session API."""

import pytest

import uni_db


@pytest.fixture
async def db():
    """Create an async database with test data."""
    db = await uni_db.AsyncUni.temporary()
    await (
        db.schema()
        .label("Person")
        .property("name", "string")
        .property("age", "int")
        .apply()
    )
    session = db.session()
    tx = await session.tx()
    await tx.execute("CREATE (n:Person {name: 'Alice', age: 30})")
    await tx.execute("CREATE (n:Person {name: 'Bob', age: 25})")
    await tx.commit()
    await db.flush()
    return db


@pytest.mark.asyncio
async def test_async_session_set_and_get_variable(db):
    """Test setting and getting an async session variable."""
    session = db.session()
    session.params().set("user_name", "Alice")

    name = session.params().get("user_name")
    assert name == "Alice"


@pytest.mark.asyncio
async def test_async_session_query(db):
    """Test executing a query through an async session."""
    session = db.session()
    results = await session.query("MATCH (n:Person) RETURN n.name")
    assert len(results) == 2


@pytest.mark.asyncio
async def test_async_session_execute(db):
    """Test executing a mutation through an async session."""
    session = db.session()
    tx = await session.tx()
    result = await tx.execute("CREATE (n:Person {name: 'Charlie', age: 35})")
    assert result.affected_rows >= 0
    await tx.commit()

    results = await session.query(
        "MATCH (n:Person {name: 'Charlie'}) RETURN n.age AS age"
    )
    assert len(results) == 1
    assert results[0]["age"] == 35


@pytest.mark.asyncio
async def test_async_session_set_multiple_variables(db):
    """Test async session with multiple variables."""
    session = db.session()
    session.params().set("var1", "value1")
    session.params().set("var2", 42)
    session.params().set("var3", True)

    assert session.params().get("var1") == "value1"
    assert session.params().get("var2") == 42
    assert session.params().get("var3") is True


@pytest.mark.asyncio
async def test_async_session_get_nonexistent(db):
    """Test getting a nonexistent async session variable."""
    session = db.session()
    result = session.params().get("nonexistent")
    assert result is None
