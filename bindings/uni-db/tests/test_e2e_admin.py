# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""E2E tests for UniBuilder and admin operations in sync API.

Tests database creation modes, builder configuration, and admin utilities:
- UniBuilder.temporary(), in_memory(), open(), create(), open_existing()
- Builder config: cache_size(), parallelism()
- Admin operations: explain(), profile()
"""

import pytest

import uni_db


def test_database_builder_temporary():
    """UniBuilder.temporary() creates in-memory temporary database."""
    db = uni_db.UniBuilder.temporary().build()

    # Should be able to create schema and use it
    (db.schema().label("Test").property("value", "int").done().apply())

    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (t:Test {value: 42})")
    tx.commit()
    results = session.query("MATCH (t:Test) RETURN t.value as value")

    assert len(results) == 1
    assert results[0]["value"] == 42


def test_database_builder_in_memory():
    """UniBuilder.in_memory() is alias for temporary()."""
    db = uni_db.UniBuilder.in_memory().build()

    # Should work identically to temporary()
    (db.schema().label("Test").property("value", "int").done().apply())

    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (t:Test {value: 99})")
    tx.commit()
    results = session.query("MATCH (t:Test) RETURN t.value as value")

    assert len(results) == 1
    assert results[0]["value"] == 99


def test_database_builder_open_new_path(tmp_path):
    """UniBuilder.open() on new path creates database."""
    db_path = tmp_path / "new_db"

    db = uni_db.UniBuilder.open(str(db_path)).build()

    # Should be able to use the database
    (db.schema().label("Node").property("value", "int").done().apply())

    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (n:Node {value: 123})")
    tx.commit()
    results = session.query("MATCH (n:Node) RETURN n.value as value")

    assert len(results) == 1
    assert results[0]["value"] == 123


def test_database_builder_create_new_path(tmp_path):
    """UniBuilder.create() on new path creates database."""
    db_path = tmp_path / "create_db"

    db = uni_db.UniBuilder.create(str(db_path)).build()

    # Should be able to use the database
    (db.schema().label("Item").property("name", "string").done().apply())

    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (i:Item {name: 'TestItem'})")
    tx.commit()
    results = session.query("MATCH (i:Item) RETURN i.name as name")

    assert len(results) == 1
    assert results[0]["name"] == "TestItem"


def test_database_builder_create_fails_on_existing(tmp_path):
    """UniBuilder.create() fails if path already exists."""
    db_path = tmp_path / "existing_db"

    # Create database first
    db = uni_db.UniBuilder.create(str(db_path)).build()
    db.flush()
    del db

    # Try to create again - should fail
    with pytest.raises(Exception):
        uni_db.UniBuilder.create(str(db_path)).build()


def test_database_builder_open_existing_fails_on_missing(tmp_path):
    """UniBuilder.open_existing() fails if path doesn't exist."""
    db_path = tmp_path / "nonexistent_db"

    # Path doesn't exist - should fail
    with pytest.raises(Exception):
        uni_db.UniBuilder.open_existing(str(db_path)).build()


def test_database_builder_open_existing_succeeds(tmp_path):
    """UniBuilder.open_existing() succeeds on existing path."""
    db_path = tmp_path / "existing_db"

    # Create database first
    db = uni_db.UniBuilder.create(str(db_path)).build()
    (db.schema().label("Data").property("value", "int").done().apply())
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (d:Data {value: 456})")
    tx.commit()
    db.flush()
    del session
    del db

    # Open existing - should succeed
    db2 = uni_db.UniBuilder.open_existing(str(db_path)).build()

    # Data should be there
    session2 = db2.session()
    results = session2.query("MATCH (d:Data) RETURN d.value as value")
    assert len(results) == 1
    assert results[0]["value"] == 456


def test_builder_with_cache_size():
    """UniBuilder with cache_size configuration."""
    # Set cache size to 100MB
    db = uni_db.UniBuilder.temporary().cache_size(100 * 1024 * 1024).build()

    # Should work normally
    (db.schema().label("Cached").property("value", "int").done().apply())

    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (c:Cached {value: 777})")
    tx.commit()
    results = session.query("MATCH (c:Cached) RETURN c.value as value")

    assert len(results) == 1
    assert results[0]["value"] == 777


def test_builder_with_parallelism():
    """UniBuilder with parallelism configuration."""
    # Set parallelism to 4 threads
    db = uni_db.UniBuilder.temporary().parallelism(4).build()

    # Should work normally
    (db.schema().label("Parallel").property("value", "int").done().apply())

    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (p:Parallel {value: 888})")
    tx.commit()
    results = session.query("MATCH (p:Parallel) RETURN p.value as value")

    assert len(results) == 1
    assert results[0]["value"] == 888


def test_builder_with_multiple_configs():
    """UniBuilder with multiple configuration options."""
    db = (
        uni_db.UniBuilder.temporary()
        .cache_size(50 * 1024 * 1024)
        .parallelism(2)
        .build()
    )

    # Should work with all configs applied
    (db.schema().label("Multi").property("value", "int").done().apply())

    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (m:Multi {value: 999})")
    tx.commit()
    results = session.query("MATCH (m:Multi) RETURN m.value as value")

    assert len(results) == 1
    assert results[0]["value"] == 999


def test_explain_returns_plan_info():
    """session.explain(cypher) returns ExplainOutput with plan information."""
    db = uni_db.UniBuilder.temporary().build()

    (
        db.schema()
        .label("Person")
        .property("name", "string")
        .property("age", "int")
        .done()
        .apply()
    )

    session = db.session()

    # Explain a query
    plan = session.explain("MATCH (p:Person) WHERE p.age > 25 RETURN p.name")

    # Should return an ExplainOutput object (not a dict)
    assert (
        hasattr(plan, "plan_text")
        or hasattr(plan, "cost_estimates")
        or hasattr(plan, "warnings")
        or hasattr(plan, "index_usage")
        or hasattr(plan, "suggestions")
    )

    # plan_text should be a string if present
    if hasattr(plan, "plan_text"):
        assert isinstance(plan.plan_text, str)


def test_profile_returns_results_and_stats():
    """session.profile(cypher) returns (QueryResult, ProfileOutput) with execution stats."""
    db = uni_db.UniBuilder.temporary().build()

    (
        db.schema()
        .label("Person")
        .property("name", "string")
        .property("age", "int")
        .done()
        .apply()
    )

    session = db.session()

    # Insert some data
    tx = session.tx()
    tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
    tx.execute("CREATE (p:Person {name: 'Bob', age: 25})")
    tx.commit()

    # Profile a query
    results, profile = session.profile(
        "MATCH (p:Person) RETURN p.name as name, p.age as age"
    )

    # Results should be a QueryResult (supports len and indexing)
    assert len(results) == 2

    # Profile should be a ProfileOutput object
    # Should have execution stats (at least one of these)
    assert (
        hasattr(profile, "total_time_ms")
        or hasattr(profile, "peak_memory_bytes")
        or hasattr(profile, "operators")
    )

    # If total_time_ms present, should be a number
    if hasattr(profile, "total_time_ms"):
        assert isinstance(profile.total_time_ms, (int, float))
        assert profile.total_time_ms >= 0


def test_explain_complex_query():
    """Explain works on complex queries."""
    db = uni_db.UniBuilder.temporary().build()

    (
        db.schema()
        .label("Person")
        .property("name", "string")
        .done()
        .label("Company")
        .property("name", "string")
        .done()
        .edge_type("WORKS_AT", ["Person"], ["Company"])
        .done()
        .apply()
    )

    session = db.session()

    # Complex query with joins
    plan = session.explain(
        "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
        "WHERE c.name = 'TechCorp' "
        "RETURN p.name, c.name"
    )

    # Should return an ExplainOutput object
    assert plan is not None


def test_profile_with_filters():
    """Profile works on queries with filters and aggregations."""
    db = uni_db.UniBuilder.temporary().build()

    (
        db.schema()
        .label("Product")
        .property("name", "string")
        .property("price", "float")
        .done()
        .apply()
    )

    session = db.session()

    # Insert data
    tx = session.tx()
    tx.execute("CREATE (p:Product {name: 'Widget', price: 19.99})")
    tx.execute("CREATE (p:Product {name: 'Gadget', price: 29.99})")
    tx.execute("CREATE (p:Product {name: 'Doohickey', price: 9.99})")
    tx.commit()

    # Profile with filter
    results, profile = session.profile(
        "MATCH (p:Product) WHERE p.price > 15.0 RETURN p.name as name ORDER BY p.price"
    )

    assert len(results) == 2  # Only Widget and Gadget
    assert profile is not None


def test_explain_on_empty_database():
    """Explain works even on empty database."""
    db = uni_db.UniBuilder.temporary().build()

    (db.schema().label("Node").property("value", "int").done().apply())

    session = db.session()

    # Explain without any data
    plan = session.explain("MATCH (n:Node) RETURN n.value")

    assert plan is not None


def test_profile_on_empty_results():
    """Profile works when query returns no results."""
    db = uni_db.UniBuilder.temporary().build()

    (db.schema().label("Item").property("name", "string").done().apply())

    session = db.session()

    # No data inserted, query returns empty
    results, profile = session.profile("MATCH (i:Item) RETURN i.name")

    assert len(results) == 0
    assert profile is not None


def test_database_builder_chaining():
    """Builder methods can be chained in any order."""
    db = (
        uni_db.UniBuilder.temporary()
        .parallelism(4)
        .cache_size(100 * 1024 * 1024)
        .build()
    )

    (db.schema().label("Chain").property("value", "int").done().apply())

    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (c:Chain {value: 111})")
    tx.commit()
    results = session.query("MATCH (c:Chain) RETURN c.value as value")

    assert len(results) == 1
    assert results[0]["value"] == 111


def test_open_vs_create_vs_open_existing(tmp_path):
    """Comparison of open(), create(), and open_existing() behaviors."""
    db_path = tmp_path / "comparison_db"

    # open() on new path - should create
    db = uni_db.UniBuilder.open(str(db_path)).build()
    (db.schema().label("Test").property("value", "int").done().apply())
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (t:Test {value: 1})")
    tx.commit()
    db.flush()
    del session
    del db

    # open_existing() on existing path - should succeed
    db = uni_db.UniBuilder.open_existing(str(db_path)).build()
    session = db.session()
    results = session.query("MATCH (t:Test) RETURN t.value as value")
    assert len(results) == 1
    del session
    del db

    # create() on existing path - should fail
    with pytest.raises(Exception):
        uni_db.UniBuilder.create(str(db_path)).build()

    # open() on existing path - should succeed
    db = uni_db.UniBuilder.open(str(db_path)).build()
    session = db.session()
    results = session.query("MATCH (t:Test) RETURN t.value as value")
    assert len(results) == 1
