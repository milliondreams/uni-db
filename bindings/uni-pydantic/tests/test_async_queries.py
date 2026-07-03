# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async query builder tests for uni-pydantic."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from uni_pydantic import (
    AsyncQueryBuilder,
    Field,
    Relationship,
    UniNode,
)

# Skip all tests if uni_db is not available
pytestmark = [
    pytest.mark.skipif(
        not pytest.importorskip("uni_db", reason="uni_db not available"),
        reason="uni_db not available",
    ),
    pytest.mark.asyncio,
]


class Person(UniNode):
    """Test person model."""

    __label__ = "Person"

    name: str
    age: int | None = None
    email: str = Field(unique=True, index="btree")

    friends: list["Person"] = Relationship("FRIEND_OF", direction="both")


class TestAsyncQueryBuilder:
    """Tests for async query builder with database."""

    async def test_query_all(self, async_session):
        """Test querying all entities."""
        async_session.register(Person)
        await async_session.sync_schema()

        people = [
            Person(name="Alice", email="alice@test.com"),
            Person(name="Bob", email="bob@test.com"),
        ]
        async_session.add_all(people)
        await async_session.commit()

        results = await async_session.query(Person).all()
        assert len(results) >= 2

    async def test_query_filter(self, async_session):
        """Test querying with filter."""
        async_session.register(Person)
        await async_session.sync_schema()

        alice = Person(name="Alice", age=30, email="alice@test.com")
        bob = Person(name="Bob", age=25, email="bob@test.com")
        async_session.add_all([alice, bob])
        await async_session.commit()

        results = await async_session.query(Person).filter_by(name="Alice").all()
        assert len(results) == 1
        assert results[0].name == "Alice"

    async def test_query_first(self, async_session):
        """Test query first."""
        async_session.register(Person)
        await async_session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        async_session.add(alice)
        await async_session.commit()

        result = await async_session.query(Person).first()
        assert result is not None
        assert result.name == "Alice"

    async def test_query_count(self, async_session):
        """Test query count."""
        async_session.register(Person)
        await async_session.sync_schema()

        people = [
            Person(name=f"Person{i}", email=f"person{i}@test.com") for i in range(5)
        ]
        async_session.add_all(people)
        await async_session.commit()

        count = await async_session.query(Person).count()
        assert count >= 5

    async def test_query_exists(self, async_session):
        """Test query exists."""
        async_session.register(Person)
        await async_session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        async_session.add(alice)
        await async_session.commit()

        assert await async_session.query(Person).filter_by(name="Alice").exists()
        assert (
            not await async_session.query(Person).filter_by(name="NonExistent").exists()
        )

    async def test_query_limit(self, async_session):
        """Test query with limit."""
        async_session.register(Person)
        await async_session.sync_schema()

        people = [
            Person(name=f"Person{i}", email=f"person{i}@test.com") for i in range(10)
        ]
        async_session.add_all(people)
        await async_session.commit()

        results = await async_session.query(Person).limit(3).all()
        assert len(results) == 3

    async def test_query_delete(self, async_session):
        """Test query delete."""
        async_session.register(Person)
        await async_session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        async_session.add(alice)
        await async_session.commit()

        count = await async_session.query(Person).filter_by(name="Alice").delete()
        assert count >= 1

    async def test_query_update(self, async_session):
        """Test query update."""
        async_session.register(Person)
        await async_session.sync_schema()

        alice = Person(name="Alice", age=30, email="alice@test.com")
        async_session.add(alice)
        await async_session.commit()

        count = await async_session.query(Person).filter_by(name="Alice").update(age=31)
        assert count >= 1


class TestAsyncQueryBuilderImmutability:
    """Tests for async query builder immutability."""

    async def test_filter_returns_new_builder(self, async_session):
        """Test that filter returns a new builder."""
        async_session.register(Person)
        q1 = async_session.query(Person)
        q2 = q1.filter_by(name="Alice")
        assert q1 is not q2
        assert len(q1._filters) == 0
        assert len(q2._filters) == 1

    async def test_limit_returns_new_builder(self, async_session):
        """Test that limit returns a new builder."""
        async_session.register(Person)
        q1 = async_session.query(Person)
        q2 = q1.limit(10)
        assert q1 is not q2
        assert q1._limit is None
        assert q2._limit == 10

    async def test_chaining(self, async_session):
        """Test chaining multiple builder methods."""
        async_session.register(Person)
        q = (
            async_session.query(Person)
            .filter_by(name="Alice")
            .order_by("name")
            .limit(10)
            .skip(5)
            .distinct()
        )
        assert isinstance(q, AsyncQueryBuilder)
        assert len(q._filters) == 1
        assert len(q._order_by) == 1
        assert q._limit == 10
        assert q._skip == 5
        assert q._distinct is True


class ScoredNode(UniNode):
    """Minimal model for async search-score surfacing tests (no required email)."""

    name: str


def _async_search_session(rows: list[dict]) -> MagicMock:
    """Mock async session returning ``rows``; ``query`` is awaitable (AsyncMock)."""
    session = MagicMock()
    result = []
    for r in rows:
        m = MagicMock()
        m.to_dict.return_value = r
        result.append(m)
    # Async path awaits _db_session.query(...), unlike the sync builder.
    session._db_session.query = AsyncMock(return_value=result)

    def _to_model(node_data, model):
        data = dict(node_data)
        vid = data.pop("_vid", None)
        data.pop("_label", None)
        return model.from_properties(data, vid=vid)

    session._result_to_model.side_effect = _to_model
    return session


class TestAsyncSearchScoreSurfacing:
    """Async execution: search builders hydrate instances carrying .search_scores.

    Covers the async ``all()`` reroute (``async_query.py``) via a mock session,
    so no real DB is needed.
    """

    async def test_async_hybrid_scores(self):
        rows = [
            {
                "_props": {"name": "Alice"},
                "_vid": 1,
                "_labels": ["ScoredNode"],
                "score": 0.9,
                "vector_score": 0.8,
                "fts_score": 0.5,
                "sparse_score": 0.3,
            }
        ]
        results = await (
            AsyncQueryBuilder(_async_search_session(rows), ScoredNode)
            .hybrid_search(
                vector=("embedding", [1.0]),
                fts=("name", "a"),
                sparse=("emb", {1: 1.0}),
            )
            .all()
        )
        assert len(results) == 1
        s = results[0].search_scores
        assert s is not None
        assert s.score == 0.9
        assert s.vector == 0.8
        assert s.fts == 0.5
        assert s.sparse == 0.3

    async def test_async_vector_scores(self):
        # Regression guard: the old `node AS n` RETURN would yield zero rows.
        rows = [
            {
                "_props": {"name": "Bob"},
                "_vid": 2,
                "_labels": ["ScoredNode"],
                "distance": 0.1,
                "score": 0.95,
            }
        ]
        results = await (
            AsyncQueryBuilder(_async_search_session(rows), ScoredNode)
            .vector_search("embedding", [1.0], k=5)
            .all()
        )
        assert len(results) == 1
        assert results[0].search_scores.score == 0.95
        assert results[0].search_scores.distance == 0.1

    async def test_async_sparse_scores(self):
        rows = [
            {
                "_props": {"name": "Cara"},
                "_vid": 3,
                "_labels": ["ScoredNode"],
                "score": 0.7,
            }
        ]
        results = await (
            AsyncQueryBuilder(_async_search_session(rows), ScoredNode)
            .sparse_search("emb", {1: 1.0}, k=5)
            .all()
        )
        assert len(results) == 1
        assert results[0].search_scores.score == 0.7
