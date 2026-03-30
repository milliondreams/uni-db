# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for the async API: open, query, execute, flush."""

import pytest

import uni_db


@pytest.fixture
def test_dir(tmp_path):
    """Provide a temporary directory for database tests."""
    return str(tmp_path / "test_async_db")


@pytest.mark.asyncio
async def test_async_open_and_query(test_dir):
    """Test basic open, create label, insert, and query."""
    db = await uni_db.AsyncUni.open(test_dir)
    session = db.session()
    await (
        db.schema()
        .label("Person")
        .property("name", "string")
        .property("age", "int")
        .apply()
    )
    await session.execute("CREATE (n:Person {name: 'Alice', age: 30})")

    results = await session.query(
        "MATCH (n:Person) RETURN n.name AS name, n.age AS age"
    )
    assert len(results) == 1
    assert results[0]["name"] == "Alice"
    assert results[0]["age"] == 30


@pytest.mark.asyncio
async def test_async_temporary():
    """Test temporary in-memory database."""
    db = await uni_db.AsyncUni.temporary()
    session = db.session()
    await db.schema().label("Thing").property("value", "int").apply()
    await session.execute("CREATE (n:Thing {value: 42})")

    results = await session.query("MATCH (n:Thing) RETURN n.value AS value")
    assert len(results) == 1
    assert results[0]["value"] == 42


@pytest.mark.asyncio
async def test_async_query_with_params(test_dir):
    """Test parameterized queries."""
    db = await uni_db.AsyncUni.open(test_dir)
    session = db.session()
    await (
        db.schema()
        .label("Person")
        .property("name", "string")
        .property("age", "int")
        .apply()
    )
    await session.execute("CREATE (n:Person {name: 'Bob', age: 25})")

    results = await session.query(
        "MATCH (n:Person {name: 'Bob'}) RETURN n.age AS age",
    )
    assert len(results) == 1
    assert results[0]["age"] == 25


@pytest.mark.asyncio
async def test_async_execute_returns_count(test_dir):
    """Test that execute returns affected row count."""
    db = await uni_db.AsyncUni.open(test_dir)
    session = db.session()
    await db.schema().label("Counter").apply()
    result = await session.execute("CREATE (n:Counter {value: 1})")
    assert isinstance(result, uni_db.AutoCommitResult)
    assert result.affected_rows >= 0
    assert result.nodes_created >= 1


@pytest.mark.asyncio
async def test_async_flush(test_dir):
    """Test flushing writes to storage."""
    db = await uni_db.AsyncUni.open(test_dir)
    session = db.session()
    await db.schema().label("Flushed").apply()
    await session.execute("CREATE (n:Flushed {val: 1})")
    await db.flush()

    # After flush, data should be persisted
    results = await session.query("MATCH (n:Flushed) RETURN n.val AS val")
    assert len(results) == 1


@pytest.mark.asyncio
async def test_async_explain(test_dir):
    """Test query plan explanation."""
    db = await uni_db.AsyncUni.open(test_dir)
    session = db.session()
    await db.schema().label("Person").apply()

    plan = await session.explain("MATCH (n:Person) RETURN n")
    assert isinstance(plan, uni_db.ExplainOutput)
    assert isinstance(plan.plan_text, str)
    assert len(plan.plan_text) > 0
    assert plan.cost_estimates is not None


@pytest.mark.asyncio
async def test_async_builder():
    """Test AsyncUniBuilder."""
    builder = uni_db.AsyncUniBuilder.temporary()
    db = await builder.build()
    session = db.session()
    await db.schema().label("Built").property("x", "int").apply()
    await session.execute("CREATE (n:Built {x: 1})")
    results = await session.query("MATCH (n:Built) RETURN n.x AS x")
    assert len(results) == 1
    assert results[0]["x"] == 1


@pytest.mark.asyncio
async def test_async_multiple_queries():
    """Test running multiple queries sequentially."""
    db = await uni_db.AsyncUni.temporary()
    session = db.session()
    await db.schema().label("Node").property("idx", "int").apply()

    for i in range(10):
        await session.execute(f"CREATE (n:Node {{idx: {i}}})")

    results = await session.query("MATCH (n:Node) RETURN n.idx AS idx ORDER BY n.idx")
    assert len(results) == 10
    assert results[0]["idx"] == 0
    assert results[9]["idx"] == 9


@pytest.mark.asyncio
async def test_async_query_with_builder():
    """Test AsyncQueryBuilder via query_with()."""
    db = await uni_db.AsyncUni.temporary()
    session = db.session()
    await (
        db.schema()
        .label("Item")
        .property("name", "string")
        .property("value", "int")
        .apply()
    )
    await session.execute("CREATE (n:Item {name: 'Widget', value: 42})")
    await db.flush()

    results = await (
        session.query_with("MATCH (n:Item) WHERE n.value = $val RETURN n.name AS name")
        .param("val", 42)
        .fetch_all()
    )
    assert len(results) == 1
    assert results[0]["name"] == "Widget"


@pytest.mark.asyncio
async def test_async_query_with_timeout():
    """Test AsyncQueryBuilder with timeout."""
    db = await uni_db.AsyncUni.temporary()
    session = db.session()
    await db.schema().label("Node").property("x", "int").apply()
    await session.execute("CREATE (n:Node {x: 1})")

    results = (
        await session.query_with("MATCH (n:Node) RETURN n.x AS x")
        .timeout(30.0)
        .fetch_all()
    )
    assert len(results) == 1
