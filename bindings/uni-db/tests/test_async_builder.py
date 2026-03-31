# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for AsyncUniBuilder API."""

import os
import tempfile

import pytest

import uni_db


class TestAsyncUniBuilderOpenModes:
    """Tests for different async database open modes."""

    @pytest.mark.asyncio
    async def test_create_new_database(self):
        """Test creating a new database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            db = await uni_db.AsyncUniBuilder.create(path).build()
            assert db is not None
            session = db.session()
            await db.schema().label("Test").apply()
            results = await session.query("MATCH (n:Test) RETURN n")
            assert len(results) == 0

    @pytest.mark.asyncio
    async def test_create_fails_if_exists(self):
        """Test that create() fails if database exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            db1 = await uni_db.AsyncUniBuilder.create(path).build()
            await db1.schema().label("Test").apply()
            await db1.flush()
            del db1

            with pytest.raises(uni_db.UniError):
                await uni_db.AsyncUniBuilder.create(path).build()

    @pytest.mark.asyncio
    async def test_open_existing_database(self):
        """Test opening an existing database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            db1 = await uni_db.AsyncUniBuilder.create(path).build()
            session1 = db1.session()
            await db1.schema().label("Person").property("name", "string").apply()
            tx1 = await session1.tx()
            await tx1.execute("CREATE (n:Person {name: 'Alice'})")
            await tx1.commit()
            await db1.flush()
            del db1

            db2 = await uni_db.AsyncUniBuilder.open_existing(path).build()
            session2 = db2.session()
            results = await session2.query("MATCH (n:Person) RETURN n.name AS name")
            assert len(results) == 1
            assert results[0]["name"] == "Alice"

    @pytest.mark.asyncio
    async def test_open_existing_fails_if_not_exists(self):
        """Test that open_existing() fails if database doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "nonexistent")
            with pytest.raises(uni_db.UniNotFoundError):
                await uni_db.AsyncUniBuilder.open_existing(path).build()

    @pytest.mark.asyncio
    async def test_open_creates_if_needed(self):
        """Test that open() creates database if it doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            db = await uni_db.AsyncUniBuilder.open(path).build()
            assert db is not None
            session = db.session()
            await db.schema().label("Test").apply()
            tx = await session.tx()
            await tx.execute("CREATE (n:Test)")
            await tx.commit()
            await db.flush()

    @pytest.mark.asyncio
    async def test_open_reuses_existing(self):
        """Test that open() reuses existing database."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            db1 = await uni_db.AsyncUniBuilder.create(path).build()
            session1 = db1.session()
            await db1.schema().label("Person").property("name", "string").apply()
            tx1 = await session1.tx()
            await tx1.execute("CREATE (n:Person {name: 'Bob'})")
            await tx1.commit()
            await db1.flush()
            del db1

            db2 = await uni_db.AsyncUniBuilder.open(path).build()
            session2 = db2.session()
            results = await session2.query("MATCH (n:Person) RETURN n.name AS name")
            assert len(results) == 1
            assert results[0]["name"] == "Bob"

    @pytest.mark.asyncio
    async def test_temporary_database(self):
        """Test creating a temporary database."""
        db = await uni_db.AsyncUniBuilder.temporary().build()
        assert db is not None
        session = db.session()
        await db.schema().label("Temp").property("value", "int").apply()
        tx = await session.tx()
        await tx.execute("CREATE (n:Temp {value: 42})")
        await tx.commit()
        await db.flush()
        results = await session.query("MATCH (n:Temp) RETURN n.value AS value")
        assert len(results) == 1
        assert results[0]["value"] == 42


class TestAsyncUniBuilderConfiguration:
    """Tests for async database builder configuration options."""

    @pytest.mark.asyncio
    async def test_cache_size(self):
        """Test setting cache size."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            db = await (
                uni_db.AsyncUniBuilder.create(path)
                .cache_size(1024 * 1024 * 100)
                .build()
            )
            assert db is not None

    @pytest.mark.asyncio
    async def test_parallelism(self):
        """Test setting parallelism level."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            db = await uni_db.AsyncUniBuilder.create(path).parallelism(4).build()
            assert db is not None

    @pytest.mark.asyncio
    async def test_chained_configuration(self):
        """Test chaining multiple configuration options."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "testdb")
            db = await (
                uni_db.AsyncUniBuilder.create(path)
                .cache_size(1024 * 1024 * 50)
                .parallelism(2)
                .build()
            )
            assert db is not None
            session = db.session()
            await db.schema().label("Test").apply()
            tx = await session.tx()
            await tx.execute("CREATE (n:Test)")
            await tx.commit()
            await db.flush()
            results = await session.query("MATCH (n:Test) RETURN n")
            assert len(results) == 1


class TestAsyncInMemory:
    """Tests for in_memory builder method."""

    @pytest.mark.asyncio
    async def test_in_memory_database(self):
        """Test creating an in-memory database via builder."""
        db = await uni_db.AsyncUniBuilder.in_memory().build()
        assert db is not None
        session = db.session()
        await db.schema().label("Mem").property("x", "int").apply()
        tx = await session.tx()
        await tx.execute("CREATE (n:Mem {x: 42})")
        await tx.commit()
        await db.flush()
        results = await session.query("MATCH (n:Mem) RETURN n.x AS x")
        assert len(results) == 1
        assert results[0]["x"] == 42


class TestAsyncBackwardCompatibility:
    """Tests for backward compatibility with AsyncUni constructors."""

    @pytest.mark.asyncio
    async def test_async_database_open(self):
        """Test that AsyncUni.open() still works."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db = await uni_db.AsyncUni.open(tmpdir)
            assert db is not None
            session = db.session()
            await db.schema().label("Legacy").apply()
            tx = await session.tx()
            await tx.execute("CREATE (n:Legacy)")
            await tx.commit()
            await db.flush()
            results = await session.query("MATCH (n:Legacy) RETURN n")
            assert len(results) == 1
