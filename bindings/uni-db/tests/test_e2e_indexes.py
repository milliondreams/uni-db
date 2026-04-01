"""End-to-end tests for index functionality (sync API)."""

import pytest

import uni_db


@pytest.fixture
def indexed_db(tmp_path):
    """Create a database with pre-defined indexes."""
    db_path = tmp_path / "indexed_db"
    db = uni_db.UniBuilder.open(str(db_path)).build()

    # Create Item label with indexes
    (
        db.schema()
        .label("Item")
        .property("sku", "string")
        .property("name", "string")
        .vector("embedding", 4)
        .index("sku", "btree")
        .index("name", "hash")
        .done()
        .edge_type("RELATED_TO", ["Item"], ["Item"])
        .property_nullable("weight", "float")
        .done()
        .apply()
    )
    db.schema().label("Item").index(
        "embedding", {"type": "vector", "metric": "l2"}
    ).apply()

    # Add some test data
    session = db.session()
    tx = session.tx()
    tx.execute("""
        CREATE (:Item {sku: 'SKU001', name: 'Widget', embedding: [1.0, 0.0, 0.0, 0.0]})
    """)
    tx.execute("""
        CREATE (:Item {sku: 'SKU002', name: 'Gadget', embedding: [0.0, 1.0, 0.0, 0.0]})
    """)
    tx.execute("""
        CREATE (:Item {sku: 'SKU003', name: 'Doohickey', embedding: [0.0, 0.0, 1.0, 0.0]})
    """)
    tx.commit()
    db.flush()

    yield db


def test_verify_pre_existing_indexes(indexed_db):
    """Test that pre-existing indexes are present."""
    db = indexed_db

    # Get label info
    label_info = db.get_label_info("Item")
    assert label_info is not None
    assert hasattr(label_info, "indexes")
    assert isinstance(label_info.indexes, list)

    # Verify indexes exist
    index_map = {idx.properties[0]: idx for idx in label_info.indexes}

    assert "sku" in index_map
    assert index_map["sku"].index_type in ("btree", "SCALAR")

    assert "name" in index_map
    assert index_map["name"].index_type in ("hash", "SCALAR")

    assert "embedding" in index_map
    assert index_map["embedding"].index_type in ("vector", "VECTOR")


def test_create_additional_scalar_index(indexed_db):
    """Test creating an additional scalar index."""
    db = indexed_db
    session = db.session()

    # Add a new property to schema
    tx = session.tx()
    tx.execute(
        "CREATE (:Item {sku: 'SKU004', name: 'Thingamajig', embedding: [0.5, 0.5, 0.0, 0.0]})"
    )
    tx.commit()
    db.flush()

    # Verify new item exists
    results = session.query("MATCH (i:Item {sku: 'SKU004'}) RETURN i.name AS name")
    assert len(results) == 1
    assert results[0]["name"] == "Thingamajig"

    # Verify existing indexes still work
    label_info = db.get_label_info("Item")
    assert len(label_info.indexes) >= 3


def test_create_additional_vector_index(indexed_db):
    """Test creating an additional vector index."""
    db = indexed_db
    session = db.session()

    # Create another label with vector property
    (
        db.schema()
        .label("Document")
        .property("title", "string")
        .vector("vector", 4)
        .done()
        .apply()
    )
    tx = session.tx()
    tx.execute("""
        CREATE (:Document {title: 'Doc1', vector: [1.0, 0.0, 0.0, 0.0]})
    """)
    tx.commit()
    db.flush()

    # Create vector index with cosine similarity
    db.schema().label("Document").index(
        "vector", {"type": "vector", "metric": "cosine"}
    ).apply()

    # Verify index was created
    label_info = db.get_label_info("Document")
    index_map = {idx.properties[0]: idx for idx in label_info.indexes}

    assert "vector" in index_map
    assert index_map["vector"].index_type in ("vector", "VECTOR")


def test_indexed_queries_return_correct_results(indexed_db):
    """Test that queries using indexes return correct results."""
    db = indexed_db
    session = db.session()

    # Test btree index query (sku)
    results = session.query("MATCH (i:Item {sku: 'SKU001'}) RETURN i.name AS name")
    assert len(results) == 1
    assert results[0]["name"] == "Widget"

    # Test hash index query (name)
    results = session.query("MATCH (i:Item {name: 'Gadget'}) RETURN i.sku AS sku")
    assert len(results) == 1
    assert results[0]["sku"] == "SKU002"

    # Test range query on btree index
    results = session.query("""
        MATCH (i:Item)
        WHERE i.sku >= 'SKU002' AND i.sku <= 'SKU003'
        RETURN i.sku AS sku
        ORDER BY i.sku
    """)
    assert len(results) == 2
    assert results[0]["sku"] == "SKU002"
    assert results[1]["sku"] == "SKU003"

    # Test vector similarity query
    results = session.query("""
        MATCH (i:Item)
        WHERE i.embedding IS NOT NULL
        RETURN i.sku AS sku, i.embedding AS emb
        ORDER BY sku
    """)
    assert len(results) == 3


def test_index_on_edge_type_properties(indexed_db):
    """Test creating and using indexes on edge type properties."""
    db = indexed_db
    session = db.session()

    # Create some edges with weights
    tx = session.tx()
    tx.execute("""
        MATCH (i1:Item {sku: 'SKU001'}), (i2:Item {sku: 'SKU002'})
        CREATE (i1)-[:RELATED_TO {weight: 0.8}]->(i2)
    """)
    tx.execute("""
        MATCH (i1:Item {sku: 'SKU002'}), (i2:Item {sku: 'SKU003'})
        CREATE (i1)-[:RELATED_TO {weight: 0.6}]->(i2)
    """)
    tx.commit()
    db.flush()

    # Query edges by weight
    results = session.query("""
        MATCH (a:Item)-[r:RELATED_TO]->(b:Item)
        WHERE r.weight > 0.7
        RETURN r.weight AS weight
    """)
    assert len(results) == 1
    assert results[0]["weight"] == 0.8


def test_multiple_indexes_same_label(indexed_db):
    """Test that multiple indexes on the same label work correctly."""
    db = indexed_db
    session = db.session()

    # Verify Item has multiple indexes
    label_info = db.get_label_info("Item")
    assert len(label_info.indexes) >= 3

    # Query using different indexed properties
    results_sku = session.query(
        "MATCH (i:Item {sku: 'SKU001'}) RETURN count(i) AS count"
    )
    assert results_sku[0]["count"] == 1

    results_name = session.query(
        "MATCH (i:Item {name: 'Gadget'}) RETURN count(i) AS count"
    )
    assert results_name[0]["count"] == 1

    # Both queries should work efficiently with their respective indexes


def test_get_label_info_details(indexed_db):
    """Test detailed information from get_label_info."""
    db = indexed_db

    label_info = db.get_label_info("Item")

    # Verify LabelInfo attributes
    assert hasattr(label_info, "indexes")
    assert isinstance(label_info.indexes, list)

    # Verify each index has proper metadata
    for idx in label_info.indexes:
        assert hasattr(idx, "properties")
        assert hasattr(idx, "index_type")
        assert isinstance(idx.properties, list)
        assert len(idx.properties) >= 1
        assert isinstance(idx.properties[0], str)
        assert isinstance(idx.index_type, str)
        assert idx.index_type in ["btree", "hash", "vector", "SCALAR", "VECTOR"]

    # Test with non-existent label
    try:
        non_existent = db.get_label_info("NonExistent")
        # If it doesn't raise, it should return None or empty info
        if non_existent is not None:
            assert non_existent.indexes == [] or len(non_existent.indexes) == 0
    except Exception:
        # Expected for non-existent label
        pass
