# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async E2E tests for session functionality."""

import pytest


@pytest.mark.asyncio
async def test_session_with_single_variable(async_social_db):
    """Test creating a session with a single variable."""
    setup = async_social_db.session()
    tx = await setup.tx()
    await tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
    await tx.execute("CREATE (p:Person {name: 'Bob', age: 25})")
    await tx.commit()
    await async_social_db.flush()

    # Create session and set single variable
    session = async_social_db.session()
    await session.set("min_age", 25)

    result = await session.query(
        "MATCH (p:Person) WHERE p.age >= $session.min_age RETURN p.name as name ORDER BY name"
    )
    assert len(result) == 2
    assert result[0]["name"] == "Alice"
    assert result[1]["name"] == "Bob"


@pytest.mark.asyncio
async def test_session_with_multiple_variables(async_social_db):
    """Test creating a session with multiple variables."""
    setup = async_social_db.session()
    tx = await setup.tx()
    await tx.execute(
        "CREATE (p:Person {name: 'Alice', age: 30, email: 'alice@nyc.com'})"
    )
    await tx.execute("CREATE (p:Person {name: 'Bob', age: 25, email: 'bob@la.com'})")
    await tx.execute(
        "CREATE (p:Person {name: 'Charlie', age: 35, email: 'charlie@nyc.com'})"
    )
    await tx.commit()
    await async_social_db.flush()

    session = async_social_db.session()
    await session.set("min_age", 28)
    await session.set("max_age", 40)

    result = await session.query(
        "MATCH (p:Person) WHERE p.age >= $session.min_age AND p.age <= $session.max_age RETURN p.name as name ORDER BY name"
    )
    assert len(result) == 2
    assert result[0]["name"] == "Alice"
    assert result[1]["name"] == "Charlie"


@pytest.mark.asyncio
async def test_session_get_nonexistent_returns_none(async_social_db):
    """Test that getting a nonexistent variable returns None."""
    session = async_social_db.session()
    await session.set("existing_key", "value")

    value = await session.get("existing_key")
    assert value == "value"

    value = await session.get("nonexistent_key")
    assert value is None


@pytest.mark.asyncio
async def test_session_query(async_social_db):
    """Test querying through a session."""
    setup = async_social_db.session()
    tx = await setup.tx()
    await tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
    await tx.execute("CREATE (p:Person {name: 'Bob', age: 25})")
    await tx.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)"
    )
    await tx.commit()
    await async_social_db.flush()

    session = async_social_db.session()
    await session.set("person_name", "Alice")

    result = await session.query(
        "MATCH (p:Person {name: $session.person_name})-[:KNOWS]->(friend) RETURN friend.name as friend_name"
    )
    assert len(result) == 1
    assert result[0]["friend_name"] == "Bob"

    # Query with additional params passed to query()
    result = await session.query(
        "MATCH (p:Person {name: $session.person_name})-[:KNOWS]->(friend) WHERE friend.age >= $min_age RETURN friend.name as friend_name",
        params={"min_age": 20},
    )
    assert len(result) == 1
    assert result[0]["friend_name"] == "Bob"


@pytest.mark.asyncio
async def test_session_execute(async_social_db):
    """Test executing mutations through a session."""
    session = async_social_db.session()
    await session.set("person_name", "SessionPerson")
    await session.set("person_age", 40)

    tx = await session.tx()
    result = await tx.execute(
        "CREATE (p:Person {name: $session.person_name, age: $session.person_age})"
    )
    assert result.nodes_created >= 1
    await tx.commit()

    verify = async_social_db.session()
    result = await verify.query(
        "MATCH (p:Person {name: 'SessionPerson'}) RETURN p.age as age"
    )
    assert len(result) == 1
    assert result[0]["age"] == 40

    session2 = async_social_db.session()
    await session2.set("name_to_update", "SessionPerson")
    await session2.set("new_age", 41)

    tx2 = await session2.tx()
    result = await tx2.execute(
        "MATCH (p:Person {name: $session.name_to_update}) SET p.age = $session.new_age"
    )
    assert result.properties_set >= 1
    await tx2.commit()

    result = await verify.query(
        "MATCH (p:Person {name: 'SessionPerson'}) RETURN p.age as age"
    )
    assert len(result) == 1
    assert result[0]["age"] == 41


@pytest.mark.asyncio
async def test_session_variables_persist_across_queries(async_social_db):
    """Test that session variables persist across multiple queries."""
    setup = async_social_db.session()
    tx = await setup.tx()
    await tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
    await tx.execute("CREATE (p:Person {name: 'Bob', age: 25})")
    await tx.execute("CREATE (p:Person {name: 'Charlie', age: 35})")
    await tx.commit()
    await async_social_db.flush()

    session = async_social_db.session()
    await session.set("min_age", 30)
    await session.set("max_age", 40)

    # First query using min_age
    result = await session.query(
        "MATCH (p:Person) WHERE p.age >= $session.min_age RETURN p.name as name ORDER BY name"
    )
    assert len(result) == 2
    assert result[0]["name"] == "Alice"
    assert result[1]["name"] == "Charlie"

    # Second query using max_age
    result = await session.query(
        "MATCH (p:Person) WHERE p.age <= $session.max_age RETURN p.name as name ORDER BY name"
    )
    assert len(result) == 3

    # Third query using both variables
    result = await session.query(
        "MATCH (p:Person) WHERE p.age >= $session.min_age AND p.age <= $session.max_age RETURN p.name as name ORDER BY name"
    )
    assert len(result) == 2
    assert result[0]["name"] == "Alice"
    assert result[1]["name"] == "Charlie"

    assert await session.get("min_age") == 30
    assert await session.get("max_age") == 40

    # Execute mutation using session variables
    tx_mut = await session.tx()
    result = await tx_mut.execute(
        "MATCH (p:Person) WHERE p.age >= $session.min_age SET p.email = 'senior@example.com'"
    )
    assert result.properties_set >= 1
    await tx_mut.commit()

    verify = async_social_db.session()
    result = await verify.query(
        "MATCH (p:Person {email: 'senior@example.com'}) RETURN p.name as name ORDER BY name"
    )
    assert len(result) == 2
    assert result[0]["name"] == "Alice"
    assert result[1]["name"] == "Charlie"
