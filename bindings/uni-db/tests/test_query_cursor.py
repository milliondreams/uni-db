# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for cursor-based query streaming (sync and async)."""

import pytest

# =============================================================================
# Sync Tests
# =============================================================================


class TestSyncQueryCursor:
    """Synchronous QueryCursor tests."""

    def test_cursor_fetch_all(self, social_db_populated):
        """fetch_all() returns same results as session.query()."""
        db = social_db_populated
        session = db.session()
        cypher = "MATCH (p:Person) RETURN p.name AS name ORDER BY name"
        expected = session.query(cypher)
        cursor = session.query_with(cypher).cursor()
        result = cursor.fetch_all()
        assert len(result) == len(expected)
        assert [r["name"] for r in result] == [r["name"] for r in expected]

    def test_cursor_fetch_one(self, social_db_populated):
        """Iterate with fetch_one() until None."""
        db = social_db_populated
        session = db.session()
        cursor = session.query_with(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY name"
        ).cursor()
        names = []
        while True:
            row = cursor.fetch_one()
            if row is None:
                break
            names.append(row["name"])
        assert len(names) == 5
        assert names == ["Alice", "Bob", "Charlie", "Diana", "Eve"]

    def test_cursor_fetch_many(self, social_db_populated):
        """fetch_many(2) returns at most 2 rows per call."""
        db = social_db_populated
        session = db.session()
        cursor = session.query_with(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY name"
        ).cursor()
        batch1 = cursor.fetch_many(2)
        assert len(batch1) <= 2
        assert len(batch1) > 0
        # Collect the rest
        remaining = cursor.fetch_all()
        assert len(batch1) + len(remaining) == 5

    def test_cursor_iterator(self, social_db_populated):
        """for row in cursor: iterates all rows."""
        db = social_db_populated
        session = db.session()
        cursor = session.query_with(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY name"
        ).cursor()
        names = [row["name"] for row in cursor]
        assert names == ["Alice", "Bob", "Charlie", "Diana", "Eve"]

    def test_cursor_columns(self, social_db_populated):
        """columns property returns expected column names."""
        db = social_db_populated
        session = db.session()
        cursor = session.query_with(
            "MATCH (p:Person) RETURN p.name AS name, p.age AS age ORDER BY name"
        ).cursor()
        assert cursor.columns == ["name", "age"]
        cursor.close()

    def test_cursor_close(self, social_db_populated):
        """close() makes subsequent calls return empty/None."""
        db = social_db_populated
        session = db.session()
        cursor = session.query_with(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY name"
        ).cursor()
        cursor.close()
        assert cursor.fetch_one() is None
        assert cursor.fetch_all() == []

    def test_cursor_context_manager(self, social_db_populated):
        """with query_with().cursor() as cursor: auto-closes."""
        db = social_db_populated
        session = db.session()
        with session.query_with(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY name"
        ).cursor() as cursor:
            first = cursor.fetch_one()
            assert first is not None
        # After exiting context, cursor should be closed
        assert cursor.fetch_one() is None

    def test_cursor_empty_result(self, social_db_populated):
        """Cursor on query returning 0 rows works correctly."""
        db = social_db_populated
        session = db.session()
        cursor = session.query_with(
            "MATCH (p:Person {name: 'Nobody'}) RETURN p.name AS name"
        ).cursor()
        assert cursor.fetch_one() is None
        assert cursor.fetch_all() == []

    def test_cursor_with_params(self, social_db_populated):
        """query_with with params passes them correctly."""
        db = social_db_populated
        session = db.session()
        cursor = (
            session.query_with(
                "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name AS name ORDER BY name"
            )
            .params({"min_age": 30})
            .cursor()
        )
        names = [row["name"] for row in cursor]
        assert "Charlie" in names  # age 35
        assert "Eve" in names  # age 32
        assert "Bob" not in names  # age 25

    def test_query_builder_cursor(self, social_db_populated):
        """session.query_with(cypher).timeout(10).cursor() works."""
        db = social_db_populated
        session = db.session()
        cursor = (
            session.query_with("MATCH (p:Person) RETURN p.name AS name ORDER BY name")
            .timeout(10.0)
            .cursor()
        )
        names = [row["name"] for row in cursor]
        assert len(names) == 5


# =============================================================================
# Async Tests
# =============================================================================


class TestAsyncQueryCursor:
    """Asynchronous AsyncQueryCursor tests."""

    @pytest.mark.asyncio
    async def test_async_cursor_fetch_all(self, async_social_db_populated):
        """await cursor.fetch_all() returns all rows."""
        session = async_social_db_populated.session()
        cursor = await session.query_with(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY name"
        ).cursor()
        result = await cursor.fetch_all()
        assert len(result) == 5
        assert [r["name"] for r in result] == [
            "Alice",
            "Bob",
            "Charlie",
            "Diana",
            "Eve",
        ]

    @pytest.mark.asyncio
    async def test_async_cursor_fetch_one(self, async_social_db_populated):
        """await cursor.fetch_one() iterates row by row."""
        session = async_social_db_populated.session()
        cursor = await session.query_with(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY name"
        ).cursor()
        names = []
        while True:
            row = await cursor.fetch_one()
            if row is None:
                break
            names.append(row["name"])
        assert names == ["Alice", "Bob", "Charlie", "Diana", "Eve"]

    @pytest.mark.asyncio
    async def test_async_cursor_fetch_many(self, async_social_db_populated):
        """await cursor.fetch_many(2) returns at most 2 rows."""
        session = async_social_db_populated.session()
        cursor = await session.query_with(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY name"
        ).cursor()
        batch1 = await cursor.fetch_many(2)
        assert len(batch1) <= 2
        assert len(batch1) > 0
        remaining = await cursor.fetch_all()
        assert len(batch1) + len(remaining) == 5

    @pytest.mark.asyncio
    async def test_async_cursor_async_for(self, async_social_db_populated):
        """async for row in cursor: iterates all rows."""
        session = async_social_db_populated.session()
        cursor = await session.query_with(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY name"
        ).cursor()
        names = []
        async for row in cursor:
            names.append(row["name"])
        assert names == ["Alice", "Bob", "Charlie", "Diana", "Eve"]

    @pytest.mark.asyncio
    async def test_async_cursor_context_manager(self, async_social_db_populated):
        """async with await query_with().cursor() as cursor: auto-closes."""
        session = async_social_db_populated.session()
        async with await session.query_with(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY name"
        ).cursor() as cursor:
            first = await cursor.fetch_one()
            assert first is not None
        # After exiting context, cursor should be closed
        row = await cursor.fetch_one()
        assert row is None

    @pytest.mark.asyncio
    async def test_async_query_builder_cursor(self, async_social_db_populated):
        """await session.query_with(cypher).cursor() works."""
        session = async_social_db_populated.session()
        cursor = await (
            session.query_with("MATCH (p:Person) RETURN p.name AS name ORDER BY name")
            .timeout(10.0)
            .cursor()
        )
        result = await cursor.fetch_all()
        assert len(result) == 5
