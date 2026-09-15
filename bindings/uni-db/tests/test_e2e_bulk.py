"""End-to-end tests for synchronous bulk operations."""

import pytest


def test_create_bulk_writer_with_builder(social_db):
    """Test creating a bulk writer using the builder pattern."""
    session = social_db.session()
    tx = session.tx()
    writer = tx.bulk_writer().build()
    assert writer is not None

    # Abort to clean up
    writer.abort()
    tx.rollback()


def test_bulk_insert_vertices(social_db):
    """Test bulk inserting vertices and receiving vertex IDs."""
    session = social_db.session()
    tx = session.tx()
    writer = tx.bulk_writer().build()

    people_data = [
        {"name": "Alice", "age": 30, "email": "alice@example.com"},
        {"name": "Bob", "age": 25, "email": "bob@example.com"},
        {"name": "Charlie", "age": 35},
    ]

    vids = writer.insert_vertices("Person", people_data)

    assert isinstance(vids, list)
    assert len(vids) == 3
    assert all(isinstance(vid, int) for vid in vids)
    assert len(set(vids)) == 3  # All IDs should be unique

    stats = writer.commit()
    tx.commit()
    assert stats.vertices_inserted == 3


def test_bulk_insert_edges(social_db):
    """Test bulk inserting edges after inserting vertices."""
    session = social_db.session()
    tx = session.tx()
    writer = tx.bulk_writer().build()

    # Insert vertices first
    people_data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
        {"name": "Charlie", "age": 35},
    ]
    vids = writer.insert_vertices("Person", people_data)

    # Insert edges between them
    edges_data = [
        (vids[0], vids[1], {"since": 2020}),
        (vids[1], vids[2], {"since": 2021}),
        (vids[0], vids[2], {}),
    ]

    result = writer.insert_edges("KNOWS", edges_data)
    assert result is None  # insert_edges returns None

    stats = writer.commit()
    tx.commit()
    assert stats.vertices_inserted == 3
    assert stats.edges_inserted == 3


def test_bulk_writer_with_defer_vector_indexes(social_db):
    """Test bulk writer with defer_vector_indexes option."""
    session = social_db.session()
    tx = session.tx()
    writer = tx.bulk_writer().defer_vector_indexes(True).build()

    people_data = [{"name": "Alice", "age": 30}]
    writer.insert_vertices("Person", people_data)

    stats = writer.commit()
    tx.commit()
    assert stats.vertices_inserted == 1


def test_bulk_writer_with_defer_scalar_indexes(social_db):
    """Test bulk writer with defer_scalar_indexes option."""
    session = social_db.session()
    tx = session.tx()
    writer = tx.bulk_writer().defer_scalar_indexes(True).build()

    people_data = [{"name": "Bob", "age": 25}]
    writer.insert_vertices("Person", people_data)

    stats = writer.commit()
    tx.commit()
    assert stats.vertices_inserted == 1


def test_bulk_writer_with_batch_size(social_db):
    """Test bulk writer with custom batch_size option."""
    session = social_db.session()
    tx = session.tx()
    writer = tx.bulk_writer().batch_size(100).build()

    people_data = [{"name": f"Person{i}", "age": 20 + i} for i in range(10)]
    writer.insert_vertices("Person", people_data)

    stats = writer.commit()
    tx.commit()
    assert stats.vertices_inserted == 10


def test_bulk_writer_with_async_indexes(social_db):
    """Test bulk writer with async_indexes option."""
    session = social_db.session()
    tx = session.tx()
    writer = tx.bulk_writer().async_indexes(True).build()

    people_data = [{"name": "Charlie", "age": 35}]
    writer.insert_vertices("Person", people_data)

    stats = writer.commit()
    tx.commit()
    assert stats.vertices_inserted == 1


def test_bulk_writer_with_all_config_options(social_db):
    """Test bulk writer with all configuration options combined."""
    session = social_db.session()
    tx = session.tx()
    writer = (
        tx.bulk_writer()
        .defer_vector_indexes(True)
        .defer_scalar_indexes(False)
        .batch_size(50)
        .async_indexes(True)
        .build()
    )

    people_data = [{"name": f"Person{i}", "age": 20 + i} for i in range(5)]
    writer.insert_vertices("Person", people_data)

    stats = writer.commit()
    tx.commit()
    assert stats.vertices_inserted == 5


def test_bulk_stats_attributes(social_db):
    """Test that BulkStats has all expected attributes."""
    session = social_db.session()
    tx = session.tx()
    writer = tx.bulk_writer().build()

    people_data = [{"name": "Alice", "age": 30}]
    vids = writer.insert_vertices("Person", people_data)

    companies_data = [{"name": "TechCorp", "founded": 2010}]
    company_vids = writer.insert_vertices("Company", companies_data)

    edges_data = [(vids[0], company_vids[0], {"role": "Engineer"})]
    writer.insert_edges("WORKS_AT", edges_data)

    stats = writer.commit()
    tx.commit()

    # Check all expected attributes
    assert hasattr(stats, "vertices_inserted")
    assert hasattr(stats, "edges_inserted")
    assert hasattr(stats, "indexes_rebuilt")
    assert hasattr(stats, "duration_secs")
    assert hasattr(stats, "index_build_duration_secs")
    assert hasattr(stats, "indexes_pending")

    # Verify values
    assert stats.vertices_inserted == 2
    assert stats.edges_inserted == 1
    assert isinstance(stats.indexes_rebuilt, int)
    assert isinstance(stats.duration_secs, (int, float))
    assert isinstance(stats.index_build_duration_secs, (int, float))
    assert isinstance(stats.indexes_pending, int)
    assert stats.duration_secs >= 0
    assert stats.index_build_duration_secs >= 0


def test_bulk_writer_abort(social_db):
    """abort() is what suppresses the rows -- asserted differentially.

    Was `xfail`ed on "abort() only sets a flag ... data is already committed
    before abort". That no longer describes the code: `BulkWriter::abort`
    (crates/uni-bulk/src/bulk.rs:1557) clears the pending buffers and calls
    `rollback_table` for every table the load touched. The marker was not
    strict, so the XPASS was silent and the stale reason outlived the fix.

    The control arm is load-bearing. Bulk rows land only on `writer.commit()`,
    so the original single-arm shape ("abort, then count == 0") passed whether
    or not abort did anything -- deleting the `abort()` call left it green. The
    two arms together are what make abort the variable: same inserts, and the
    only difference is the abort.
    """
    session = social_db.session()

    # Control: no abort, writer commits -> the row lands.
    tx = session.tx()
    writer = tx.bulk_writer().build()
    writer.insert_vertices("Person", [{"name": "Control", "age": 30}])
    writer.commit()
    tx.commit()
    social_db.flush()
    assert (
        session.query("MATCH (p:Person {name: 'Control'}) RETURN count(p) AS cnt")[0][
            "cnt"
        ]
        == 1
    ), "control arm did not land; the test below would pass for free"

    # Abort: the same sequence, aborted -> commit is refused and nothing lands.
    tx = session.tx()
    writer = tx.bulk_writer().build()
    writer.insert_vertices("Person", [{"name": "Aborted", "age": 30}])
    writer.abort()
    with pytest.raises(RuntimeError):
        writer.commit()
    tx.rollback()
    social_db.flush()
    assert (
        session.query("MATCH (p:Person {name: 'Aborted'}) RETURN count(p) AS cnt")[0][
            "cnt"
        ]
        == 0
    ), "abort() must discard the insert"


def test_operations_after_abort_raise_error(social_db):
    """Test that operations after abort raise RuntimeError."""
    session = social_db.session()
    tx = session.tx()
    writer = tx.bulk_writer().build()

    # Abort the writer
    writer.abort()
    tx.rollback()

    # Attempting to insert vertices should raise RuntimeError (Python-side check)
    with pytest.raises(RuntimeError):
        writer.insert_vertices("Person", [{"name": "Alice", "age": 30}])

    # Attempting to insert edges should raise RuntimeError (Python-side check)
    with pytest.raises(RuntimeError):
        writer.insert_edges("KNOWS", [(1, 2, {})])

    # Attempting to commit should raise RuntimeError (Python-side check)
    with pytest.raises(RuntimeError):
        writer.commit()


def test_convenience_bulk_insert_vertices(social_db):
    """Test the convenience bulk_insert_vertices method via bulk_writer."""
    session = social_db.session()
    tx = session.tx()
    people_data = [
        {"name": "Alice", "age": 30, "email": "alice@example.com"},
        {"name": "Bob", "age": 25, "email": "bob@example.com"},
        {"name": "Charlie", "age": 35},
    ]

    bw = tx.bulk_writer().build()
    vids = bw.insert_vertices("Person", people_data)
    bw.commit()
    tx.commit()

    assert isinstance(vids, list)
    assert len(vids) == 3
    assert all(isinstance(vid, int) for vid in vids)

    # Verify data was inserted
    social_db.flush()
    result = session.query("MATCH (p:Person) RETURN p.name ORDER BY p.name")
    assert len(result) == 3
    names = [row["p.name"] for row in result]
    assert names == ["Alice", "Bob", "Charlie"]


def test_convenience_bulk_insert_edges(social_db):
    """Test the convenience bulk_insert_edges method via bulk_writer."""
    session = social_db.session()
    tx = session.tx()
    # First insert vertices, then edges, in one writer
    people_data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
    ]
    bw = tx.bulk_writer().build()
    vids = bw.insert_vertices("Person", people_data)

    # Then insert edges
    edges_data = [
        (vids[0], vids[1], {"since": 2020}),
    ]

    bw.insert_edges("KNOWS", edges_data)
    bw.commit()
    tx.commit()

    # Verify edge was inserted
    social_db.flush()
    result = session.query(
        "MATCH (a:Person)-[k:KNOWS]->(b:Person) "
        "WHERE a.name = 'Alice' AND b.name = 'Bob' "
        "RETURN k.since"
    )
    assert len(result) == 1
    assert result[0]["k.since"] == 2020


def test_large_batch_insert(social_db):
    """Test inserting a large batch of vertices (1000+)."""
    session = social_db.session()
    tx = session.tx()
    writer = tx.bulk_writer().batch_size(200).build()

    # Generate 1500 vertices
    large_batch = [{"name": f"Person{i}", "age": 20 + (i % 50)} for i in range(1500)]

    vids = writer.insert_vertices("Person", large_batch)

    assert len(vids) == 1500
    assert len(set(vids)) == 1500  # All IDs should be unique

    stats = writer.commit()
    tx.commit()
    assert stats.vertices_inserted == 1500

    # Verify data was inserted
    social_db.flush()
    result = session.query("MATCH (p:Person) RETURN count(p) as cnt")
    assert result[0]["cnt"] == 1500


def test_verify_data_correctness_after_bulk_insert(social_db):
    """Test that data inserted via bulk operations is queryable and correct."""
    session = social_db.session()
    tx = session.tx()
    writer = tx.bulk_writer().build()

    # Insert people
    people_data = [
        {"name": "Alice", "age": 30, "email": "alice@example.com"},
        {"name": "Bob", "age": 25, "email": "bob@example.com"},
        {"name": "Charlie", "age": 35},
    ]
    person_vids = writer.insert_vertices("Person", people_data)

    # Insert companies
    companies_data = [
        {"name": "TechCorp", "founded": 2010},
        {"name": "StartupInc", "founded": 2020},
    ]
    company_vids = writer.insert_vertices("Company", companies_data)

    # Insert KNOWS edges
    knows_edges = [
        (person_vids[0], person_vids[1], {"since": 2020}),
        (person_vids[1], person_vids[2], {"since": 2021}),
    ]
    writer.insert_edges("KNOWS", knows_edges)

    # Insert WORKS_AT edges
    works_at_edges = [
        (person_vids[0], company_vids[0], {"role": "Engineer"}),
        (person_vids[1], company_vids[1], {"role": "Designer"}),
    ]
    writer.insert_edges("WORKS_AT", works_at_edges)

    stats = writer.commit()
    tx.commit()
    assert stats.vertices_inserted == 5
    assert stats.edges_inserted == 4

    # Flush to ensure data is queryable
    social_db.flush()

    # Verify Person vertices
    result = session.query(
        "MATCH (p:Person) RETURN p.name, p.age, p.email ORDER BY p.name"
    )
    assert len(result) == 3
    assert result[0]["p.name"] == "Alice"
    assert result[0]["p.age"] == 30
    assert result[0]["p.email"] == "alice@example.com"
    assert result[1]["p.name"] == "Bob"
    assert result[1]["p.age"] == 25
    assert result[2]["p.name"] == "Charlie"
    assert result[2]["p.age"] == 35
    assert result[2]["p.email"] is None

    # Verify Company vertices
    result = session.query("MATCH (c:Company) RETURN c.name, c.founded ORDER BY c.name")
    assert len(result) == 2
    assert result[0]["c.name"] == "StartupInc"
    assert result[0]["c.founded"] == 2020
    assert result[1]["c.name"] == "TechCorp"
    assert result[1]["c.founded"] == 2010

    # Verify KNOWS edges
    result = session.query(
        "MATCH (a:Person)-[k:KNOWS]->(b:Person) "
        "RETURN a.name, b.name, k.since "
        "ORDER BY a.name, b.name"
    )
    assert len(result) == 2
    assert result[0]["a.name"] == "Alice"
    assert result[0]["b.name"] == "Bob"
    assert result[0]["k.since"] == 2020
    assert result[1]["a.name"] == "Bob"
    assert result[1]["b.name"] == "Charlie"
    assert result[1]["k.since"] == 2021

    # Verify WORKS_AT edges
    result = session.query(
        "MATCH (p:Person)-[w:WORKS_AT]->(c:Company) "
        "RETURN p.name, c.name, w.role "
        "ORDER BY p.name"
    )
    assert len(result) == 2
    assert result[0]["p.name"] == "Alice"
    assert result[0]["c.name"] == "TechCorp"
    assert result[0]["w.role"] == "Engineer"
    assert result[1]["p.name"] == "Bob"
    assert result[1]["c.name"] == "StartupInc"
    assert result[1]["w.role"] == "Designer"


def test_multiple_vertex_labels_in_single_writer(social_db):
    """Test inserting multiple different vertex labels with a single writer."""
    session = social_db.session()
    tx = session.tx()
    writer = tx.bulk_writer().build()

    # Insert different types of vertices
    writer.insert_vertices("Person", [{"name": "Alice", "age": 30}])
    writer.insert_vertices("Company", [{"name": "TechCorp"}])

    stats = writer.commit()
    tx.commit()
    assert stats.vertices_inserted == 2

    social_db.flush()

    # Verify both types exist
    result = session.query("MATCH (p:Person) RETURN count(p) as cnt")
    assert result[0]["cnt"] == 1

    result = session.query("MATCH (c:Company) RETURN count(c) as cnt")
    assert result[0]["cnt"] == 1


def test_bulk_insert_with_empty_data(social_db):
    """An all-empty bulk load commits as a no-op.

    Was `xfail`ed: `insert_vertices` marked the label touched even for an empty
    vec, and `commit` then counted rows in a `vertices_<label>` table that was
    never created, failing the whole commit. Empty inserts now return early
    without marking the label (crates/uni-bulk/src/bulk.rs::insert_vertices).
    """
    session = social_db.session()
    tx = session.tx()
    writer = tx.bulk_writer().build()

    # Insert empty vertex list
    vids = writer.insert_vertices("Person", [])
    assert vids == []

    # Insert empty edge list
    result = writer.insert_edges("KNOWS", [])
    assert result is None

    stats = writer.commit()
    tx.commit()
    assert stats.vertices_inserted == 0
    assert stats.edges_inserted == 0


def test_bulk_insert_edges_without_properties(social_db):
    """Test bulk inserting edges without any properties."""
    session = social_db.session()
    tx = session.tx()
    writer = tx.bulk_writer().build()

    people_data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    vids = writer.insert_vertices("Person", people_data)

    # Insert edge without properties
    edges_data = [(vids[0], vids[1], {})]
    writer.insert_edges("KNOWS", edges_data)

    stats = writer.commit()
    tx.commit()
    assert stats.edges_inserted == 1

    social_db.flush()
    result = session.query(
        "MATCH (a:Person)-[k:KNOWS]->(b:Person) "
        "WHERE a.name = 'Alice' AND b.name = 'Bob' "
        "RETURN k"
    )
    assert len(result) == 1
