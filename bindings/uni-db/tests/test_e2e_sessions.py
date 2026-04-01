"""End-to-end tests for session functionality (sync API)."""

import pytest

import uni_db


@pytest.fixture
def social_db(tmp_path):
    """Create a database with social network schema."""
    db_path = tmp_path / "social_db"
    db = uni_db.UniBuilder.open(str(db_path)).build()

    # Create schema
    (
        db.schema()
        .label("User")
        .property("username", "string")
        .property("email", "string")
        .property("age", "int")
        .done()
        .label("Post")
        .property("title", "string")
        .property("content", "string")
        .property("likes", "int")
        .done()
        .edge_type("FOLLOWS", ["User"], ["User"])
        .done()
        .edge_type("POSTED", ["User"], ["Post"])
        .property_nullable("timestamp", "int")
        .done()
        .edge_type("LIKES", ["User"], ["Post"])
        .property_nullable("timestamp", "int")
        .done()
        .apply()
    )

    # Add test data
    session = db.session()
    tx = session.tx()
    tx.execute("""
        CREATE (:User {username: 'alice', email: 'alice@example.com', age: 30})
    """)
    tx.execute("""
        CREATE (:User {username: 'bob', email: 'bob@example.com', age: 25})
    """)
    tx.execute("""
        CREATE (:User {username: 'charlie', email: 'charlie@example.com', age: 35})
    """)
    tx.execute("""
        CREATE (:Post {title: 'Hello World', content: 'My first post!', likes: 5})
    """)
    tx.execute("""
        CREATE (:Post {title: 'Graph Databases', content: 'They are awesome!', likes: 10})
    """)

    # Create relationships
    tx.execute("""
        MATCH (a:User {username: 'alice'}), (b:User {username: 'bob'})
        CREATE (a)-[:FOLLOWS]->(b)
    """)
    tx.execute("""
        MATCH (a:User {username: 'alice'}), (p:Post {title: 'Hello World'})
        CREATE (a)-[:POSTED {timestamp: 1234567890}]->(p)
    """)
    tx.execute("""
        MATCH (b:User {username: 'bob'}), (p:Post {title: 'Hello World'})
        CREATE (b)-[:LIKES {timestamp: 1234567900}]->(p)
    """)
    tx.commit()

    db.flush()
    yield db


def test_session_with_single_variable(social_db):
    """Test creating a session with a single variable."""
    db = social_db

    # Create session and set a variable
    session = db.session()
    session.params().set("username", "alice")

    # Verify we can get the variable back
    value = session.params().get("username")
    assert value == "alice"


def test_session_with_multiple_variables(social_db):
    """Test creating a session with multiple variables."""
    db = social_db

    # Create session and set multiple variables
    session = db.session()
    session.params().set("username", "bob")
    session.params().set("min_age", 20)
    session.params().set("max_age", 30)
    session.params().set("is_active", True)

    # Verify all variables
    assert session.params().get("username") == "bob"
    assert session.params().get("min_age") == 20
    assert session.params().get("max_age") == 30
    assert session.params().get("is_active") is True


def test_session_get_nonexistent_returns_none(social_db):
    """Test that getting a non-existent variable returns None."""
    db = social_db

    session = db.session()
    session.params().set("existing_key", "value")

    # Get existing key
    assert session.params().get("existing_key") == "value"

    # Get non-existent key
    assert session.params().get("nonexistent_key") is None


def test_session_query(social_db):
    """Test executing queries within a session."""
    db = social_db

    # Create session and set variable
    session = db.session()
    session.params().set("target_username", "alice")

    # Execute query using session variable
    results = session.query("""
        MATCH (u:User {username: $session.target_username})
        RETURN u.username AS username, u.email AS email, u.age AS age
    """)

    assert len(results) == 1
    assert results[0]["username"] == "alice"
    assert results[0]["email"] == "alice@example.com"
    assert results[0]["age"] == 30


def test_session_query_with_params(social_db):
    """Test session query with additional parameters."""
    db = social_db

    # Create session with one variable
    session = db.session()
    session.params().set("min_likes", 5)

    # Query with additional params
    results = session.query(
        """
        MATCH (p:Post)
        WHERE p.likes >= $session.min_likes AND p.likes <= $max_likes
        RETURN p.title AS title, p.likes AS likes
        ORDER BY likes
        """,
        params={"max_likes": 10},
    )

    assert len(results) == 2
    assert results[0]["title"] == "Hello World"
    assert results[0]["likes"] == 5
    assert results[1]["title"] == "Graph Databases"
    assert results[1]["likes"] == 10


def test_session_execute(social_db):
    """Test executing write operations within a session."""
    db = social_db

    # Create session and set variables
    session = db.session()
    params = session.params()
    params.set("new_username", "diana")
    params.set("new_email", "diana@example.com")
    params.set("new_age", 28)

    # Execute create operation using transaction with explicit params
    tx = session.tx()
    tx.execute(
        "CREATE (:User {username: $new_username, email: $new_email, age: $new_age})",
        params=params.get_all(),
    )
    tx.commit()

    # Verify the user was created using a plain session
    verify_session = db.session()
    results = verify_session.query(
        "MATCH (u:User {username: 'diana'}) RETURN u.username AS username"
    )
    assert len(results) == 1
    assert results[0]["username"] == "diana"


def test_session_variables_persist_across_queries(social_db):
    """Test that session variables persist across multiple queries."""
    db = social_db

    # Create session and set variables
    session = db.session()
    session.params().set("user1", "alice")
    session.params().set("user2", "bob")

    # First query
    results1 = session.query("""
        MATCH (u:User {username: $session.user1})
        RETURN u.username AS username
    """)
    assert results1[0]["username"] == "alice"

    # Second query using different variable
    results2 = session.query("""
        MATCH (u:User {username: $session.user2})
        RETURN u.username AS username
    """)
    assert results2[0]["username"] == "bob"

    # Third query using both variables
    results3 = session.query("""
        MATCH (u1:User {username: $session.user1})-[:FOLLOWS]->(u2:User {username: $session.user2})
        RETURN u1.username AS follower, u2.username AS followed
    """)
    assert len(results3) == 1
    assert results3[0]["follower"] == "alice"
    assert results3[0]["followed"] == "bob"


def test_session_with_complex_types(social_db):
    """Test session variables with complex types."""
    db = social_db

    # Create session and set various types
    session = db.session()
    session.params().set("string_val", "test")
    session.params().set("int_val", 42)
    session.params().set("float_val", 3.14)
    session.params().set("bool_val", True)
    session.params().set("list_val", [1, 2, 3])
    session.params().set("dict_val", {"key": "value", "nested": {"deep": "data"}})

    # Verify all types
    assert session.params().get("string_val") == "test"
    assert session.params().get("int_val") == 42
    assert session.params().get("float_val") == 3.14
    assert session.params().get("bool_val") is True
    assert session.params().get("list_val") == [1, 2, 3]
    assert session.params().get("dict_val") == {
        "key": "value",
        "nested": {"deep": "data"},
    }


def test_session_execute_update(social_db):
    """Test executing update operations within a session."""
    db = social_db

    # Create session and set variables
    session = db.session()
    params = session.params()
    params.set("target_user", "charlie")
    params.set("new_age", 36)

    # Execute update using transaction with explicit params
    tx = session.tx()
    tx.execute(
        "MATCH (u:User {username: $target_user}) SET u.age = $new_age",
        params=params.get_all(),
    )
    tx.commit()

    # Verify update using a plain session
    verify_session = db.session()
    results = verify_session.query(
        "MATCH (u:User {username: 'charlie'}) RETURN u.age AS age"
    )
    assert results[0]["age"] == 36


def test_session_execute_delete(social_db):
    """Test executing delete operations within a session."""
    db = social_db

    # Verify initial state using a plain session
    verify_session = db.session()
    initial = verify_session.query(
        "MATCH (p:Post {title: 'Graph Databases'}) RETURN count(p) AS count"
    )
    assert initial[0]["count"] == 1

    # Create session and set variable
    session = db.session()
    params = session.params()
    params.set("post_title", "Graph Databases")

    # Execute delete using transaction with explicit params
    tx = session.tx()
    tx.execute(
        "MATCH (p:Post {title: $post_title}) DELETE p",
        params=params.get_all(),
    )
    tx.commit()

    # Verify deletion
    results = verify_session.query(
        "MATCH (p:Post {title: 'Graph Databases'}) RETURN count(p) AS count"
    )
    assert results[0]["count"] == 0


def test_multiple_independent_sessions(social_db):
    """Test that multiple sessions are independent."""
    db = social_db

    # Create first session and set variable
    session1 = db.session()
    session1.params().set("username", "alice")

    # Create second session with different value
    session2 = db.session()
    session2.params().set("username", "bob")

    # Verify sessions are independent
    assert session1.params().get("username") == "alice"
    assert session2.params().get("username") == "bob"

    # Query using both sessions
    results1 = session1.query(
        "MATCH (u:User {username: $session.username}) RETURN u.username AS username"
    )
    results2 = session2.query(
        "MATCH (u:User {username: $session.username}) RETURN u.username AS username"
    )

    assert results1[0]["username"] == "alice"
    assert results2[0]["username"] == "bob"


def test_session_with_aggregations(social_db):
    """Test session queries with aggregation functions."""
    db = social_db

    # Create session and set variable
    session = db.session()
    session.params().set("min_age", 25)

    # Query with aggregation
    results = session.query("""
        MATCH (u:User)
        WHERE u.age >= $session.min_age
        RETURN count(u) AS user_count, avg(u.age) AS avg_age
    """)

    assert len(results) == 1
    assert results[0]["user_count"] == 3
    assert isinstance(results[0]["avg_age"], (int, float))


def test_session_set_chaining(social_db):
    """Test that session set calls work correctly."""
    db = social_db

    # Set variables on session
    session = db.session()
    session.params().set("var1", "value1")
    session.params().set("var2", "value2")
    session.params().set("var3", "value3")

    # Verify all variables were set
    assert session.params().get("var1") == "value1"
    assert session.params().get("var2") == "value2"
    assert session.params().get("var3") == "value3"


def test_session_with_relationship_queries(social_db):
    """Test session queries involving relationships."""
    db = social_db

    # Create session and set variable
    session = db.session()
    session.params().set("follower", "alice")

    # Query relationships
    results = session.query("""
        MATCH (u:User {username: $session.follower})-[:FOLLOWS]->(followed:User)
        RETURN followed.username AS followed_username
    """)

    assert len(results) == 1
    assert results[0]["followed_username"] == "bob"


def test_session_query_returns_empty_list(social_db):
    """Test that session query returns empty result when no matches."""
    db = social_db

    session = db.session()
    session.params().set("username", "nonexistent_user")

    results = session.query("""
        MATCH (u:User {username: $session.username})
        RETURN u.username AS username
    """)

    assert len(results) == 0
