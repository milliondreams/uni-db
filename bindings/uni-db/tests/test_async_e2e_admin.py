# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async E2E tests for AsyncUniBuilder and admin operations."""

import pytest

import uni_db


@pytest.mark.asyncio
async def test_async_database_builder_temporary():
    """Test creating a temporary database using AsyncUniBuilder."""
    db = await uni_db.AsyncUniBuilder.temporary().build()

    await db.schema().label("Person").property("name", "string").apply()

    session = db.session()
    await session.execute("CREATE (:Person {name: 'Alice'})")

    result = await session.query("MATCH (n:Person) RETURN n.name")
    assert len(result) == 1
    assert result[0]["n.name"] == "Alice"


@pytest.mark.asyncio
async def test_async_database_builder_in_memory():
    """Test creating an in-memory database using AsyncUniBuilder."""
    db = await uni_db.AsyncUniBuilder.in_memory().build()

    await (
        db.schema()
        .label("Product")
        .property("sku", "string")
        .property("price", "float")
        .done()
        .apply()
    )

    session = db.session()
    await session.execute("CREATE (:Product {sku: 'SKU001', price: 9.99})")

    result = await session.query("MATCH (p:Product) RETURN p.sku, p.price")
    assert len(result) == 1
    assert result[0]["p.sku"] == "SKU001"
    assert result[0]["p.price"] == 9.99


@pytest.mark.asyncio
async def test_async_database_builder_open_new_path(tmp_path):
    """Test opening/creating a database at a new path using AsyncUniBuilder.open()."""
    db_path = tmp_path / "test_open_db"

    db = await uni_db.AsyncUniBuilder.open(str(db_path)).build()

    await db.schema().label("User").property("username", "string").apply()

    session = db.session()
    await session.execute("CREATE (:User {username: 'testuser'})")

    result = await session.query("MATCH (u:User) RETURN u.username")
    assert len(result) == 1
    assert result[0]["u.username"] == "testuser"


@pytest.mark.asyncio
async def test_async_database_builder_create_new_path(tmp_path):
    """Test creating a new database using AsyncUniBuilder.create()."""
    db_path = tmp_path / "test_create_db"

    db = await uni_db.AsyncUniBuilder.create(str(db_path)).build()

    await db.schema().label("Item").property("id", "int").apply()

    session = db.session()
    await session.execute("CREATE (:Item {id: 42})")

    result = await session.query("MATCH (i:Item) RETURN i.id")
    assert len(result) == 1
    assert result[0]["i.id"] == 42


@pytest.mark.asyncio
async def test_async_database_builder_create_fails_on_existing(tmp_path):
    """Test that AsyncUniBuilder.create() fails on existing database."""
    db_path = tmp_path / "test_existing_db"

    db1 = await uni_db.AsyncUniBuilder.create(str(db_path)).build()
    await db1.schema().label("Test").apply()
    await db1.flush()
    del db1

    with pytest.raises(Exception):
        await uni_db.AsyncUniBuilder.create(str(db_path)).build()


@pytest.mark.asyncio
async def test_async_database_builder_open_existing_fails_on_missing(tmp_path):
    """Test that AsyncUniBuilder.open_existing() fails on non-existent database."""
    db_path = tmp_path / "test_nonexistent_db"

    with pytest.raises(Exception):
        await uni_db.AsyncUniBuilder.open_existing(str(db_path)).build()


@pytest.mark.asyncio
async def test_async_database_builder_open_existing_succeeds(tmp_path):
    """Test that AsyncUniBuilder.open_existing() succeeds on existing database."""
    db_path = tmp_path / "test_open_existing_db"

    db1 = await uni_db.AsyncUniBuilder.create(str(db_path)).build()
    await db1.schema().label("Person").property("name", "string").apply()
    session1 = db1.session()
    await session1.execute("CREATE (:Person {name: 'Alice'})")
    await db1.flush()
    del db1

    db2 = await uni_db.AsyncUniBuilder.open_existing(str(db_path)).build()

    session2 = db2.session()
    result = await session2.query("MATCH (p:Person) RETURN p.name")
    assert len(result) == 1
    assert result[0]["p.name"] == "Alice"


@pytest.mark.asyncio
async def test_async_database_builder_with_cache_size(tmp_path):
    """Test AsyncUniBuilder with custom cache size."""
    db_path = tmp_path / "test_cache_size_db"

    cache_size = 10 * 1024 * 1024
    db = (
        await uni_db.AsyncUniBuilder.open(str(db_path))
        .cache_size(cache_size)
        .build()
    )

    await db.schema().label("Data").property("value", "int").apply()

    session = db.session()
    await session.execute("CREATE (:Data {value: 123})")

    result = await session.query("MATCH (d:Data) RETURN d.value")
    assert len(result) == 1
    assert result[0]["d.value"] == 123


@pytest.mark.asyncio
async def test_async_database_builder_with_parallelism(tmp_path):
    """Test AsyncUniBuilder with custom parallelism setting."""
    db_path = tmp_path / "test_parallelism_db"

    db = await uni_db.AsyncUniBuilder.open(str(db_path)).parallelism(4).build()

    await db.schema().label("Task").property("name", "string").apply()

    session = db.session()
    await session.execute("CREATE (:Task {name: 'task1'})")

    result = await session.query("MATCH (t:Task) RETURN t.name")
    assert len(result) == 1
    assert result[0]["t.name"] == "task1"


@pytest.mark.asyncio
async def test_async_database_builder_chained_options(tmp_path):
    """Test AsyncUniBuilder with multiple chained options."""
    db_path = tmp_path / "test_chained_db"

    db = (
        await uni_db.AsyncUniBuilder.open(str(db_path))
        .cache_size(5 * 1024 * 1024)
        .parallelism(2)
        .build()
    )

    await (
        db.schema()
        .label("Config")
        .property("key", "string")
        .property("value", "string")
        .done()
        .apply()
    )

    session = db.session()
    await session.execute("CREATE (:Config {key: 'setting1', value: 'value1'})")

    result = await session.query("MATCH (c:Config) RETURN c.key, c.value")
    assert len(result) == 1
    assert result[0]["c.key"] == "setting1"
    assert result[0]["c.value"] == "value1"


@pytest.mark.asyncio
async def test_async_database_convenience_open(tmp_path):
    """Test AsyncUni.open() convenience method."""
    db_path = tmp_path / "test_convenience_open_db"

    db = await uni_db.AsyncUni.open(str(db_path))

    await db.schema().label("Note").property("content", "string").apply()

    session = db.session()
    await session.execute("CREATE (:Note {content: 'test note'})")

    result = await session.query("MATCH (n:Note) RETURN n.content")
    assert len(result) == 1
    assert result[0]["n.content"] == "test note"


@pytest.mark.asyncio
async def test_async_database_convenience_temporary():
    """Test AsyncUni.temporary() convenience method."""
    db = await uni_db.AsyncUni.temporary()

    await db.schema().label("TempData").property("id", "int").apply()

    session = db.session()
    await session.execute("CREATE (:TempData {id: 999})")

    result = await session.query("MATCH (t:TempData) RETURN t.id")
    assert len(result) == 1
    assert result[0]["t.id"] == 999


@pytest.mark.asyncio
async def test_explain():
    """Test the explain method for query plans."""
    db = await uni_db.AsyncUni.temporary()

    await (
        db.schema()
        .label("Person")
        .property("name", "string")
        .property("age", "int")
        .done()
        .apply()
    )

    session = db.session()
    await session.execute("CREATE (:Person {name: 'Alice', age: 30})")
    await session.execute("CREATE (:Person {name: 'Bob', age: 25})")

    plan = await session.explain(
        "MATCH (n:Person) WHERE n.age > 20 RETURN n.name, n.age"
    )

    assert isinstance(plan, uni_db.ExplainOutput)
    assert isinstance(plan.plan_text, str)
    assert len(plan.plan_text) > 0


@pytest.mark.asyncio
async def test_profile():
    """Test the profile method for query execution profiling."""
    db = await uni_db.AsyncUni.temporary()

    await (
        db.schema()
        .label("Product")
        .property("name", "string")
        .property("price", "float")
        .done()
        .apply()
    )

    session = db.session()
    await session.execute("CREATE (:Product {name: 'Widget A', price: 9.99})")
    await session.execute("CREATE (:Product {name: 'Widget B', price: 19.99})")
    await session.execute("CREATE (:Product {name: 'Widget C', price: 29.99})")

    results, stats = await session.profile(
        "MATCH (p:Product) WHERE p.price < 25.0 RETURN p.name, p.price"
    )

    assert isinstance(results, uni_db.QueryResult)
    assert isinstance(stats, uni_db.ProfileOutput)

    assert len(results) == 2  # Products with price < 25.0


@pytest.mark.asyncio
async def test_explain_with_complex_query():
    """Test explain with a more complex query involving joins."""
    db = await uni_db.AsyncUni.temporary()

    await (
        db.schema()
        .label("User")
        .property("username", "string")
        .done()
        .label("Post")
        .property("title", "string")
        .done()
        .edge_type("AUTHORED", ["User"], ["Post"])
        .done()
        .apply()
    )

    session = db.session()
    await session.execute("CREATE (:User {username: 'alice'})")
    await session.execute("CREATE (:User {username: 'bob'})")
    await session.execute("CREATE (:Post {title: 'Post 1'})")
    await session.execute("CREATE (:Post {title: 'Post 2'})")

    await session.execute("""
        MATCH (u:User {username: 'alice'}), (p:Post {title: 'Post 1'})
        CREATE (u)-[:AUTHORED]->(p)
    """)

    plan = await session.explain("""
        MATCH (u:User)-[:AUTHORED]->(p:Post)
        RETURN u.username, p.title
    """)

    assert isinstance(plan, uni_db.ExplainOutput)
    assert isinstance(plan.plan_text, str)
    assert len(plan.plan_text) > 0


@pytest.mark.asyncio
async def test_profile_with_aggregation():
    """Test profile with an aggregation query."""
    db = await uni_db.AsyncUni.temporary()

    await db.schema().label("Order").property("amount", "float").apply()

    session = db.session()
    for i in range(10):
        await session.execute(f"CREATE (:Order {{amount: {(i + 1) * 10.0}}})")

    results, stats = await session.profile("""
        MATCH (o:Order)
        RETURN count(o) AS total_orders, sum(o.amount) AS total_amount
    """)

    assert isinstance(results, uni_db.QueryResult)
    assert isinstance(stats, uni_db.ProfileOutput)
    assert len(results) == 1
    assert results[0]["total_orders"] == 10
    assert results[0]["total_amount"] == 550.0  # 10+20+30+...+100
