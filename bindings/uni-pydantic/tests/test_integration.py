# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Integration tests for uni-pydantic with actual database."""

from datetime import date, datetime

import pytest
from pydantic import field_validator

from uni_pydantic import (
    Field,
    Relationship,
    UniEdge,
    UniNode,
    UniSession,
    Vector,
    before_create,
)

# Skip all tests if uni_db is not available
pytestmark = pytest.mark.skipif(
    not pytest.importorskip("uni_db", reason="uni_db not available"),
    reason="uni_db not available",
)


class Person(UniNode):
    """Test person model."""

    __label__ = "Person"

    name: str
    age: int | None = None
    email: str = Field(unique=True, index="btree")
    created_at: datetime | None = None

    friends: list["Person"] = Relationship("FRIEND_OF", direction="both")

    @before_create
    def set_created_at(self):
        self.created_at = datetime.now()


class Company(UniNode):
    """Test company model."""

    __label__ = "Company"

    name: str = Field(index="btree", unique=True)
    founded: date | None = None


class FriendshipEdge(UniEdge):
    """Test friendship edge."""

    __edge_type__ = "FRIEND_OF"
    __from__ = Person
    __to__ = Person

    since: date


class TestSessionCRUD:
    """Tests for session CRUD operations."""

    def test_add_and_commit(self, session):
        """Test adding and committing a node."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        session.add(alice)
        session.commit()

        assert alice.is_persisted
        assert alice.vid is not None

    def test_add_multiple(self, session):
        """Test adding multiple nodes."""
        session.register(Person)
        session.sync_schema()

        people = [
            Person(name="Alice", email="alice@test.com"),
            Person(name="Bob", email="bob@test.com"),
            Person(name="Charlie", email="charlie@test.com"),
        ]
        session.add_all(people)
        session.commit()

        for person in people:
            assert person.is_persisted

    def test_get_by_vid(self, session):
        """Test getting entity by vid."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        session.add(alice)
        session.commit()

        found = session.get(Person, vid=alice.vid)
        assert found is not None
        assert found.name == "Alice"

    def test_get_by_property(self, session):
        """Test getting entity by property."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        session.add(alice)
        session.commit()

        found = session.get(Person, email="alice@test.com")
        assert found is not None
        assert found.name == "Alice"

    def test_update_entity(self, session):
        """Test updating an entity."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", age=30, email="alice@test.com")
        session.add(alice)
        session.commit()

        alice.age = 31
        session.commit()

        # Refresh and verify
        session.refresh(alice)
        assert alice.age == 31

    def test_delete_entity(self, session):
        """Test deleting an entity."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        session.add(alice)
        session.commit()

        vid = alice.vid
        session.delete(alice)
        session.commit()

        found = session.get(Person, vid=vid)
        assert found is None


class TestContextManager:
    """Tests for session context manager."""

    def test_session_context_manager(self, db):
        """Test session works as context manager."""
        with UniSession(db) as session:
            session.register(Person)
            session.sync_schema()

            alice = Person(name="Alice", email="alice@test.com")
            session.add(alice)
            session.commit()

            assert alice.is_persisted

    def test_session_close(self, db):
        """Test session close clears pending state."""
        session = UniSession(db)
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        session.add(alice)
        assert len(session._pending_new) == 1

        session.close()
        assert len(session._pending_new) == 0


class TestEdgeCRUD:
    """Tests for edge CRUD operations."""

    def test_create_and_get_edge(self, session):
        """Test creating and retrieving an edge."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        bob = Person(name="Bob", email="bob@test.com")
        session.add_all([alice, bob])
        session.commit()

        session.create_edge(alice, "FRIEND_OF", bob, {"since": date.today()})
        session._db.flush()

        edges = session.get_edge(alice, "FRIEND_OF", bob)
        assert len(edges) >= 1

    def test_delete_edge(self, session):
        """Test deleting an edge."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        bob = Person(name="Bob", email="bob@test.com")
        session.add_all([alice, bob])
        session.commit()

        session.create_edge(alice, "FRIEND_OF", bob)
        session._db.flush()

        count = session.delete_edge(alice, "FRIEND_OF", bob)
        assert count >= 1

    def test_update_edge(self, session):
        """Test updating edge properties."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        bob = Person(name="Bob", email="bob@test.com")
        session.add_all([alice, bob])
        session.commit()

        session.create_edge(alice, "FRIEND_OF", bob, {"strength": 0.5})
        session._db.flush()

        count = session.update_edge(alice, "FRIEND_OF", bob, {"strength": 0.9})
        assert count >= 1


class TestBulkAdd:
    """Tests for bulk add operations."""

    def test_bulk_add(self, session):
        """Test bulk adding entities."""
        session.register(Person)
        session.sync_schema()

        people = [
            Person(name=f"Person{i}", email=f"person{i}@test.com") for i in range(10)
        ]

        vids = session.bulk_add(people)
        assert len(vids) == 10

        for person in people:
            assert person.is_persisted
            assert person.vid is not None

    def test_bulk_add_empty(self, session):
        """Test bulk add with empty list."""
        vids = session.bulk_add([])
        assert vids == []


class TestQueryBuilder:
    """Tests for query builder with database."""

    def test_query_all(self, session):
        """Test querying all entities."""
        session.register(Person)
        session.sync_schema()

        people = [
            Person(name="Alice", email="alice@test.com"),
            Person(name="Bob", email="bob@test.com"),
        ]
        session.add_all(people)
        session.commit()

        results = session.query(Person).all()
        assert len(results) >= 2

    def test_query_filter(self, session):
        """Test querying with filter."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", age=30, email="alice@test.com")
        bob = Person(name="Bob", age=25, email="bob@test.com")
        session.add_all([alice, bob])
        session.commit()

        results = session.query(Person).filter_by(name="Alice").all()
        assert len(results) == 1
        assert results[0].name == "Alice"

    def test_query_first(self, session):
        """Test query first."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        session.add(alice)
        session.commit()

        result = session.query(Person).first()
        assert result is not None
        assert result.name == "Alice"

    def test_query_count(self, session):
        """Test query count."""
        session.register(Person)
        session.sync_schema()

        people = [
            Person(name=f"Person{i}", email=f"person{i}@test.com") for i in range(5)
        ]
        session.add_all(people)
        session.commit()

        count = session.query(Person).count()
        assert count >= 5

    def test_query_exists(self, session):
        """Test query exists."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        session.add(alice)
        session.commit()

        assert session.query(Person).filter_by(name="Alice").exists()
        assert not session.query(Person).filter_by(name="NonExistent").exists()

    def test_query_limit(self, session):
        """Test query with limit."""
        session.register(Person)
        session.sync_schema()

        people = [
            Person(name=f"Person{i}", email=f"person{i}@test.com") for i in range(10)
        ]
        session.add_all(people)
        session.commit()

        results = session.query(Person).limit(3).all()
        assert len(results) == 3


class TestSchemaSync:
    """Tests for schema synchronization."""

    def test_sync_creates_label(self, session):
        """Test sync creates label."""
        session.register(Person)
        session.sync_schema()

        assert session._db.label_exists("Person")

    def test_sync_creates_edge_type(self, session):
        """Test sync creates edge type."""
        session.register(Person, FriendshipEdge)
        session.sync_schema()

        assert session._db.edge_type_exists("FRIEND_OF")

    def test_sync_is_idempotent(self, session):
        """Test sync can be called multiple times."""
        session.register(Person)
        session.sync_schema()
        session.sync_schema()  # Should not error

        assert session._db.label_exists("Person")


class TestRelationships:
    """Tests for relationship operations."""

    def test_create_edge(self, session):
        """Test creating an edge between nodes."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        bob = Person(name="Bob", email="bob@test.com")
        session.add_all([alice, bob])
        session.commit()

        session.create_edge(alice, "FRIEND_OF", bob, {"since": date.today()})
        session._db.flush()

        # Verify via Cypher
        results = session.cypher(
            "MATCH (a:Person)-[:FRIEND_OF]->(b:Person) "
            "WHERE a.name = 'Alice' RETURN b.name as name"
        )
        assert len(results) == 1
        assert results[0]["name"] == "Bob"


class TestTransaction:
    """Tests for transaction handling."""

    def test_transaction_commit(self, session):
        """Test transaction commit."""
        session.register(Person)
        session.sync_schema()

        with session.transaction() as tx:
            alice = Person(name="Alice", email="alice@test.com")
            tx.add(alice)

        assert alice.is_persisted

    def test_transaction_rollback_on_error(self, session):
        """Test transaction rollback on error."""
        session.register(Person)
        session.sync_schema()

        try:
            with session.transaction() as tx:
                alice = Person(name="Alice", email="alice@test.com")
                tx.add(alice)
                raise ValueError("Test error")
        except ValueError:
            pass

        # Alice should not be persisted due to rollback
        found = session.get(Person, email="alice@test.com")
        assert found is None


class TestLifecycleHooks:
    """Tests for lifecycle hooks."""

    def test_before_create_hook(self, session):
        """Test before_create hook is called."""
        session.register(Person)
        session.sync_schema()

        alice = Person(name="Alice", email="alice@test.com")
        assert alice.created_at is None

        session.add(alice)
        session.commit()

        # before_create should have set created_at
        assert alice.created_at is not None


class TestMultiVectorField:
    """Tests for list[Vector[N]] (multi-vector / ColBERT) OGM fields."""

    def test_list_vector_roundtrip(self, session):
        """A list[Vector[N]] field persists and retrieves as list[Vector[N]]."""

        class Article(UniNode):
            __label__ = "Article"
            title: str
            tokens: list[Vector[2]]

        session.register(Article)
        session.sync_schema()

        article = Article(
            title="A",
            tokens=[Vector[2]([1.0, 0.0]), Vector[2]([0.0, 1.0])],
        )
        session.add(article)
        session.commit()

        found = session.get(Article, vid=article.vid)
        assert found is not None
        assert len(found.tokens) == 2
        assert all(isinstance(v, Vector) for v in found.tokens)
        assert found.tokens[0].values == [1.0, 0.0]
        assert found.tokens[1].values == [0.0, 1.0]


class MapNode(UniNode):
    """Model exercising typed MAP<STRING, V> properties (issue #105)."""

    __label__ = "MapNode"

    name: str = Field(unique=True, index="btree")
    scores: dict[str, float] = {}
    nested: dict[str, list[int]] = {}


class TestTypedMapProperty:
    """OGM round-trip for `dict[str, V]` -> typed MAP<STRING, V>."""

    def test_typed_map_roundtrip(self, session):
        session.register(MapNode)
        session.sync_schema()

        n = MapNode(
            name="a",
            scores={"x": 1.5, "y": 2.25},
            nested={"k": [1, 2, 3], "m": [4]},
        )
        session.add(n)
        session.commit()

        found = session.get(MapNode, vid=n.vid)
        assert found is not None
        assert found.scores == {"x": 1.5, "y": 2.25}
        assert found.nested == {"k": [1, 2, 3], "m": [4]}


# ---------------------------------------------------------------------------
# Hydration must not silently drop rows
# ---------------------------------------------------------------------------


class FalsyThing(UniNode):
    """A model that is legitimately falsy when empty.

    Defining ``__bool__`` is ordinary Python, and nothing documents that a
    model may not. Every hydration path gated on ``if instance:`` drops such a
    row on the floor, so ``.all()`` silently returns fewer rows than matched.
    """

    __label__ = "FalsyThing"

    name: str
    count: int = 0

    def __bool__(self) -> bool:
        return self.count > 0


class PickyThing(UniNode):
    """A model whose validator legitimately rejects some stored data."""

    __label__ = "PickyThing"

    name: str

    @field_validator("name")
    @classmethod
    def _reject_bad(cls, v: str) -> str:
        if v == "bad":
            raise ValueError("name must not be 'bad'")
        return v


class BuggyThing(UniNode):
    """A model whose validator raises a *non*-validation error.

    Stands in for any defect inside hydration -- a broken type coercion, a
    typo'd attribute in ``_convert_db_values``, a raising ``@field_validator``.
    ``except Exception: return None`` cannot tell this apart from data that
    genuinely failed validation, so the bug is invisible and the row vanishes.
    """

    __label__ = "BuggyThing"

    name: str

    @field_validator("name")
    @classmethod
    def _boom(cls, v: str) -> str:
        if v == "boom":
            raise RuntimeError("hydration bug, not a validation failure")
        return v


def _raw_create(session, cypher: str) -> None:
    """Insert nodes bypassing the ORM, so model validation runs only on load."""
    tx = session._db_session.tx()
    tx.execute(cypher)
    tx.commit()


class TestHydrationDoesNotDropRows:
    def test_falsy_instance_is_not_dropped(self, session):
        session.register(FalsyThing)
        session.sync_schema()

        _raw_create(
            session,
            "CREATE (:FalsyThing {name: 'empty', count: 0}) "
            "CREATE (:FalsyThing {name: 'full', count: 3})",
        )

        found = session.query(FalsyThing).all()
        names = sorted(f.name for f in found)
        assert names == ["empty", "full"], (
            f"a validly hydrated but falsy model was dropped; got {names}"
        )

    def test_hydration_bug_is_not_swallowed(self, session):
        session.register(BuggyThing)
        session.sync_schema()

        _raw_create(
            session,
            "CREATE (:BuggyThing {name: 'ok'}) CREATE (:BuggyThing {name: 'boom'})",
        )

        # A RuntimeError from inside hydration is a defect, not a data problem.
        # Silently returning one row instead of two hides it completely.
        with pytest.raises(RuntimeError, match="hydration bug"):
            session.query(BuggyThing).all()

    def test_validation_failure_warns_instead_of_vanishing(self, session):
        session.register(PickyThing)
        session.sync_schema()

        _raw_create(
            session,
            "CREATE (:PickyThing {name: 'good'}) CREATE (:PickyThing {name: 'bad'})",
        )

        # Genuinely invalid stored data is still skipped -- but it must say so,
        # naming the label and vid, rather than shrinking the result set in
        # silence.
        with pytest.warns(UserWarning, match="PickyThing"):
            found = session.query(PickyThing).all()

        assert [p.name for p in found] == ["good"]

    def test_valid_rows_are_unaffected(self, session):
        """Inverse guard: narrowing the except must not break ordinary loads."""
        session.register(Person)
        session.sync_schema()

        session.add_all(
            [
                Person(name="Alice", email="alice@drop.test"),
                Person(name="Bob", email="bob@drop.test"),
            ]
        )
        session.commit()

        found = session.query(Person).all()
        assert sorted(p.name for p in found) == ["Alice", "Bob"]
