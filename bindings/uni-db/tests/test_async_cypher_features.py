# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for Cypher features accessible through the async Python API."""

import pytest

import uni_db


@pytest.fixture
async def db_with_data():
    """Create an async database with test data."""
    db = await uni_db.AsyncUni.temporary()
    session = db.session()
    await (
        db.schema()
        .label("Person")
        .property("name", "string")
        .property("age", "int")
        .done()
        .edge_type("KNOWS", ["Person"], ["Person"])
        .done()
        .apply()
    )

    tx = await session.tx()
    await tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
    await tx.execute("CREATE (p:Person {name: 'Bob', age: 25})")
    await tx.execute("CREATE (p:Person {name: 'Charlie', age: 35})")
    await tx.execute("""
        MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
        CREATE (a)-[:KNOWS]->(b)
    """)
    await tx.execute("""
        MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'})
        CREATE (b)-[:KNOWS]->(c)
    """)
    await tx.commit()
    await db.flush()
    return db, session


class TestAsyncExplainProfile:
    """Tests for async EXPLAIN and PROFILE functionality."""

    @pytest.mark.asyncio
    async def test_explain_returns_plan(self, db_with_data):
        """Test that explain returns a query plan."""
        db, session = db_with_data
        result = await session.explain("MATCH (n:Person) RETURN n.name")

        assert isinstance(result, uni_db.ExplainOutput)
        assert isinstance(result.plan_text, str)
        assert len(result.plan_text) > 0

    @pytest.mark.asyncio
    async def test_explain_includes_cost_estimates(self, db_with_data):
        """Test that explain includes cost estimates."""
        db, session = db_with_data
        result = await session.explain("MATCH (n:Person) RETURN n.name")

        assert isinstance(result, uni_db.ExplainOutput)
        assert result.cost_estimates is not None
        # cost_estimates is a dict with estimated_rows and estimated_cost
        assert "estimated_rows" in result.cost_estimates
        assert "estimated_cost" in result.cost_estimates

    @pytest.mark.asyncio
    async def test_explain_includes_index_usage(self, db_with_data):
        """Test that explain shows index usage information."""
        db, session = db_with_data
        result = await session.explain(
            "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n"
        )

        assert isinstance(result, uni_db.ExplainOutput)
        assert result.index_usage is not None

    @pytest.mark.asyncio
    async def test_profile_returns_results_and_stats(self, db_with_data):
        """Test that profile returns both results and execution statistics."""
        db, session = db_with_data
        results, profile = await session.profile("MATCH (n:Person) RETURN n.name")

        assert isinstance(results, uni_db.QueryResult)
        assert len(results) == 3

        assert isinstance(profile, uni_db.ProfileOutput)
        assert profile.total_time_ms >= 0
        assert profile.peak_memory_bytes >= 0
        assert profile.operators is not None

    @pytest.mark.asyncio
    async def test_profile_operator_stats(self, db_with_data):
        """Test that profile includes detailed operator statistics."""
        db, session = db_with_data
        _, profile = await session.profile("MATCH (n:Person) RETURN n.name")

        assert isinstance(profile, uni_db.ProfileOutput)
        assert profile.operators is not None
        # operators is a list of dicts with operator-level stats
        operators = list(profile.operators)
        assert isinstance(operators, list)

        for op in operators:
            assert "operator" in op
            assert "actual_rows" in op
            assert "time_ms" in op


class TestAsyncQueryWithParameters:
    """Tests for async parameterized queries."""

    @pytest.fixture
    async def db(self):
        """Create a test database."""
        db = await uni_db.AsyncUni.temporary()
        session = db.session()
        await (
            db.schema()
            .label("Person")
            .property("name", "string")
            .property("age", "int")
            .apply()
        )
        tx = await session.tx()
        await tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        await tx.execute("CREATE (p:Person {name: 'Bob', age: 25})")
        await tx.commit()
        await db.flush()
        return db, session

    @pytest.mark.asyncio
    async def test_query_with_string_param(self, db):
        """Test query with string parameter."""
        _, session = db
        results = await session.query(
            "MATCH (n:Person) WHERE n.name = $name RETURN n.age AS age",
            {"name": "Alice"},
        )

        assert len(results) == 1
        assert results[0]["age"] == 30

    @pytest.mark.asyncio
    async def test_query_with_int_param(self, db):
        """Test query with integer parameter."""
        _, session = db
        results = await session.query(
            "MATCH (n:Person) WHERE n.age > $min_age RETURN n.name AS name",
            {"min_age": 27},
        )

        assert len(results) == 1
        assert results[0]["name"] == "Alice"

    @pytest.mark.asyncio
    async def test_query_with_multiple_params(self, db):
        """Test query with multiple parameters."""
        _, session = db
        results = await session.query(
            "MATCH (n:Person) WHERE n.name = $name AND n.age = $age RETURN n",
            {"name": "Alice", "age": 30},
        )

        assert len(results) == 1


class TestAsyncAggregations:
    """Tests for async Cypher aggregation functions."""

    @pytest.fixture
    async def db(self):
        """Create a database with test data for aggregations."""
        db = await uni_db.AsyncUni.temporary()
        session = db.session()
        await (
            db.schema()
            .label("Product")
            .property("category", "string")
            .property("price", "float")
            .property("quantity", "int")
            .apply()
        )

        tx = await session.tx()
        await tx.execute(
            "CREATE (p:Product {category: 'Electronics', price: 100.0, quantity: 5})"
        )
        await tx.execute(
            "CREATE (p:Product {category: 'Electronics', price: 200.0, quantity: 3})"
        )
        await tx.execute(
            "CREATE (p:Product {category: 'Books', price: 20.0, quantity: 10})"
        )
        await tx.execute(
            "CREATE (p:Product {category: 'Books', price: 30.0, quantity: 8})"
        )
        await tx.commit()
        await db.flush()
        return db, session

    @pytest.mark.asyncio
    async def test_count_aggregation(self, db):
        """Test COUNT aggregation."""
        _, session = db
        results = await session.query("MATCH (p:Product) RETURN count(p) AS total")
        assert len(results) == 1
        assert results[0]["total"] == 4

    @pytest.mark.asyncio
    async def test_sum_aggregation(self, db):
        """Test SUM aggregation."""
        _, session = db
        results = await session.query(
            "MATCH (p:Product) RETURN sum(p.quantity) AS total_qty"
        )
        assert len(results) == 1
        assert results[0]["total_qty"] == 26

    @pytest.mark.asyncio
    async def test_avg_aggregation(self, db):
        """Test AVG aggregation."""
        _, session = db
        results = await session.query(
            "MATCH (p:Product) RETURN avg(p.price) AS avg_price"
        )
        assert len(results) == 1
        assert abs(results[0]["avg_price"] - 87.5) < 0.01

    @pytest.mark.asyncio
    async def test_min_max_aggregation(self, db):
        """Test MIN and MAX aggregations."""
        _, session = db
        results = await session.query(
            "MATCH (p:Product) RETURN min(p.price) AS min_price, max(p.price) AS max_price"
        )
        assert len(results) == 1
        assert results[0]["min_price"] == 20.0
        assert results[0]["max_price"] == 200.0

    @pytest.mark.asyncio
    async def test_group_by_aggregation(self, db):
        """Test aggregation with GROUP BY."""
        _, session = db
        results = await session.query("""
            MATCH (p:Product)
            RETURN p.category AS category, sum(p.quantity) AS total_qty
            ORDER BY category
        """)
        assert len(results) == 2
        categories = {r["category"]: r["total_qty"] for r in results}
        assert categories["Books"] == 18
        assert categories["Electronics"] == 8


class TestAsyncOrderingAndLimits:
    """Tests for async ORDER BY, LIMIT, and SKIP."""

    @pytest.fixture
    async def db(self):
        """Create a database with numbered test data."""
        db = await uni_db.AsyncUni.temporary()
        session = db.session()
        await (
            db.schema()
            .label("Item")
            .property("num", "int")
            .property("name", "string")
            .apply()
        )

        tx = await session.tx()
        for i in range(10):
            await tx.execute(f"CREATE (n:Item {{num: {i}, name: 'Item{i}'}})")
        await tx.commit()
        await db.flush()
        return db, session

    @pytest.mark.asyncio
    async def test_order_by_asc(self, db):
        """Test ORDER BY ascending."""
        _, session = db
        results = await session.query(
            "MATCH (n:Item) RETURN n.num AS num ORDER BY n.num ASC LIMIT 3"
        )
        assert [r["num"] for r in results] == [0, 1, 2]

    @pytest.mark.asyncio
    async def test_order_by_desc(self, db):
        """Test ORDER BY descending."""
        _, session = db
        results = await session.query(
            "MATCH (n:Item) RETURN n.num AS num ORDER BY n.num DESC LIMIT 3"
        )
        assert [r["num"] for r in results] == [9, 8, 7]

    @pytest.mark.asyncio
    async def test_limit(self, db):
        """Test LIMIT clause."""
        _, session = db
        results = await session.query("MATCH (n:Item) RETURN n.num AS num LIMIT 5")
        assert len(results) == 5

    @pytest.mark.asyncio
    async def test_skip(self, db):
        """Test SKIP clause."""
        _, session = db
        results = await session.query(
            "MATCH (n:Item) RETURN n.num AS num ORDER BY n.num SKIP 5 LIMIT 5"
        )
        assert [r["num"] for r in results] == [5, 6, 7, 8, 9]


class TestAsyncPatternMatching:
    """Tests for async Cypher pattern matching."""

    @pytest.fixture
    async def db(self):
        """Create a database with a simple social graph."""
        db = await uni_db.AsyncUni.temporary()
        session = db.session()
        await (
            db.schema()
            .label("Person")
            .property("name", "string")
            .done()
            .edge_type("KNOWS", ["Person"], ["Person"])
            .done()
            .edge_type("WORKS_WITH", ["Person"], ["Person"])
            .done()
            .apply()
        )

        tx = await session.tx()
        await tx.execute("CREATE (p:Person {name: 'Alice'})")
        await tx.execute("CREATE (p:Person {name: 'Bob'})")
        await tx.execute("CREATE (p:Person {name: 'Charlie'})")
        await tx.execute("CREATE (p:Person {name: 'David'})")
        await tx.execute("""
            MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
            CREATE (a)-[:KNOWS]->(b)
        """)
        await tx.execute("""
            MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'})
            CREATE (b)-[:KNOWS]->(c)
        """)
        await tx.execute("""
            MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Charlie'})
            CREATE (a)-[:WORKS_WITH]->(c)
        """)
        await tx.commit()
        await db.flush()
        return db, session

    @pytest.mark.asyncio
    async def test_simple_relationship_match(self, db):
        """Test matching a simple relationship."""
        _, session = db
        results = await session.query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "RETURN a.name AS a_name, b.name AS b_name"
        )
        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_relationship_type_filter(self, db):
        """Test filtering by relationship type."""
        _, session = db
        results = await session.query(
            "MATCH (a:Person)-[:WORKS_WITH]->(b:Person) "
            "RETURN a.name AS a_name, b.name AS b_name"
        )
        assert len(results) == 1
        assert results[0]["a_name"] == "Alice"
        assert results[0]["b_name"] == "Charlie"

    @pytest.mark.asyncio
    async def test_variable_length_path(self, db):
        """Test variable length path pattern."""
        _, session = db
        results = await session.query(
            "MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b:Person) "
            "RETURN b.name AS name"
        )
        names = [r["name"] for r in results]
        assert "Bob" in names
        assert "Charlie" in names

    @pytest.mark.asyncio
    async def test_bidirectional_relationship(self, db):
        """Test matching relationships in any direction."""
        _, session = db
        results = await session.query(
            "MATCH (a:Person {name: 'Bob'})-[:KNOWS]-(b:Person) RETURN b.name AS name"
        )
        # Bob is connected to Alice (incoming) and Charlie (outgoing)
        names = [r["name"] for r in results]
        assert "Alice" in names
        assert "Charlie" in names
