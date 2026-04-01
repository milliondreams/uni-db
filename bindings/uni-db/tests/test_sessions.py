# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for Session API."""

import tempfile

import pytest

import uni_db


class TestSession:
    """Tests for Session functionality."""

    @pytest.fixture
    def db(self):
        """Create a database with test data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db = uni_db.UniBuilder.open(tmpdir).build()
            db.schema().label("Person").property("name", "string").property(
                "age", "int"
            ).apply()
            session = db.session()
            tx = session.tx()
            tx.execute("CREATE (n:Person {name: 'Alice', age: 30})")
            tx.execute("CREATE (n:Person {name: 'Bob', age: 25})")
            tx.commit()
            db.flush()
            yield db

    def test_session_set_and_get_variable(self, db):
        """Test setting and getting a session variable."""
        session = db.session()
        session.params().set("user_name", "Alice")

        # The session variable should be accessible via session.params().get()
        name = session.params().get("user_name")
        assert name == "Alice"

    def test_session_query(self, db):
        """Test executing a query through a session."""
        session = db.session()
        results = session.query("MATCH (n:Person) RETURN n.name")
        assert len(results) == 2

    def test_session_execute(self, db):
        """Test executing a mutation through a session."""
        session = db.session()
        tx = session.tx()
        result = tx.execute("CREATE (n:Person {name: 'Charlie', age: 35})")
        # execute returns ExecuteResult
        assert hasattr(result, "affected_rows") or hasattr(result, "nodes_created")
        tx.commit()

        # Verify the node was created
        results = session.query(
            "MATCH (n:Person {name: 'Charlie'}) RETURN n.age AS age"
        )
        assert len(results) == 1
        assert results[0]["age"] == 35

    def test_session_set_multiple_variables(self, db):
        """Test session with multiple variables."""
        session = db.session()
        session.params().set("var1", "value1")
        session.params().set("var2", 42)
        session.params().set("var3", True)

        assert session.params().get("var1") == "value1"
        assert session.params().get("var2") == 42
        assert session.params().get("var3") is True

    def test_session_get_nonexistent(self, db):
        """Test getting a nonexistent session variable."""
        session = db.session()
        result = session.params().get("nonexistent")
        assert result is None
