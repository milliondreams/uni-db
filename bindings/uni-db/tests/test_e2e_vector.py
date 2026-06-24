# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""E2E tests for synchronous vector search functionality."""


def test_basic_vector_search_knn(ecommerce_db_populated):
    """Test basic K-NN vector search returns top k results."""
    session = ecommerce_db_populated.session()

    results = session.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 3)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert len(results) == 3


def test_vector_search_ordered_by_distance(ecommerce_db_populated):
    """Test vector search results are ordered by increasing distance."""
    session = ecommerce_db_populated.session()

    results = session.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 4)
        YIELD vid, distance
        RETURN vid, distance
    """)

    distances = [r["distance"] for r in results]
    assert distances == sorted(distances), "Results should be ordered by distance"
    assert results[0]["distance"] < 0.01, (
        "Closest match should be the query vector itself"
    )


def test_vector_search_vid_and_distance(ecommerce_db_populated):
    """Test vector search returns vid and distance with correct types."""
    session = ecommerce_db_populated.session()

    results = session.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 1)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert len(results) == 1
    row = results[0]
    assert isinstance(row["vid"], int), "vid should be an integer"
    assert isinstance(row["distance"], float), "distance should be a float"
    assert row["vid"] >= 0, "vid should be non-negative"
    assert row["distance"] >= 0.0, "distance should be non-negative"


def test_vector_search_with_k(ecommerce_db_populated):
    """Test vector search with different k values."""
    session = ecommerce_db_populated.session()

    results = session.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 2)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert len(results) == 2


def test_vector_search_with_threshold(ecommerce_db_populated):
    """Test vector search with distance threshold filtering."""
    session = ecommerce_db_populated.session()

    results_tight = session.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 10, NULL, 0.5)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert all(r["distance"] <= 0.5 for r in results_tight), (
        "All matches should be within threshold"
    )

    results_wide = session.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 10, NULL, 2.0)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert len(results_wide) >= len(results_tight), (
        "Larger threshold should return more results"
    )


def test_vector_search_fetch_nodes(ecommerce_db_populated):
    """Test vector search with YIELD node to get full node properties."""
    session = ecommerce_db_populated.session()

    results = session.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 3)
        YIELD node, distance
        RETURN node.name AS name, node.price AS price, distance
    """)

    assert len(results) == 3

    for row in results:
        assert "name" in row, "Should have 'name' property"
        assert "price" in row, "Should have 'price' property"
        assert isinstance(row["distance"], float)
        assert row["distance"] >= 0.0


def test_fetch_nodes_returns_properties_and_distance(ecommerce_db_populated):
    """Test fetch_nodes returns node properties and ordered distances."""
    session = ecommerce_db_populated.session()

    results = session.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 2)
        YIELD node, distance
        RETURN node.name AS name, node.price AS price, distance
    """)

    distances = [r["distance"] for r in results]
    assert distances == sorted(distances), "Results should be ordered by distance"


def test_cosine_metric_index(empty_db):
    """Test creating and using a cosine metric vector index."""
    session = empty_db.session()

    (
        empty_db.schema()
        .label("CosineDoc")
        .property("title", "string")
        .vector("vec", 3)
        .done()
        .apply()
    )

    tx = session.tx()
    tx.execute("CREATE (d:CosineDoc {title: 'Doc1', vec: [1.0, 0.0, 0.0]})")
    tx.execute("CREATE (d:CosineDoc {title: 'Doc2', vec: [0.0, 1.0, 0.0]})")
    tx.execute("CREATE (d:CosineDoc {title: 'Doc3', vec: [0.707, 0.707, 0.0]})")
    tx.commit()
    empty_db.flush()

    empty_db.schema().label("CosineDoc").index(
        "vec", {"type": "vector", "metric": "cosine"}
    ).apply()

    results = session.query("""
        CALL uni.vector.query('CosineDoc', 'vec', [1.0, 0.0, 0.0], 3)
        YIELD node, distance
        RETURN node.title AS title, distance
    """)

    assert len(results) == 3

    assert results[0]["distance"] < 0.01, "Most similar should have distance ~0"
    assert results[2]["distance"] > results[1]["distance"], "Distances should increase"


def test_vector_search_with_graph_traversal(ecommerce_db_populated):
    """Test combining vector search with graph traversal."""
    session = ecommerce_db_populated.session()

    results = session.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 3)
        YIELD node, distance
        MATCH (node)-[:IN_CATEGORY]->(c:Category)
        RETURN node.name AS product, c.name AS category, distance
    """)

    assert len(results) > 0, "Should find categories for similar products"


def test_vector_search_with_filter_expression(ecommerce_db_populated):
    """Test vector search with pre-filter expression."""
    session = ecommerce_db_populated.session()

    results_expensive = session.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 10, 'price > 500')
        YIELD node, distance
        RETURN node.name AS name, node.price AS price, distance
    """)

    for row in results_expensive:
        assert row["price"] > 500, f"Product {row['name']} should have price > 500"

    results_cheap = session.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 10, 'price < 100')
        YIELD node, distance
        RETURN node.name AS name, node.price AS price, distance
    """)

    for row in results_cheap:
        assert row["price"] < 100, f"Product {row['name']} should have price < 100"

    names_expensive = {r["name"] for r in results_expensive}
    names_cheap = {r["name"] for r in results_cheap}
    assert names_expensive != names_cheap, (
        "Different filters should return different products"
    )


def test_vector_search_empty_results(empty_db):
    """Test vector search on label with no data returns empty results."""
    session = empty_db.session()

    (
        empty_db.schema()
        .label("EmptyLabel")
        .property("name", "string")
        .vector("vec", 4)
        .done()
        .apply()
    )

    empty_db.schema().label("EmptyLabel").index(
        "vec", {"type": "vector", "metric": "l2"}
    ).apply()

    results = session.query("""
        CALL uni.vector.query('EmptyLabel', 'vec', [1.0, 0.0, 0.0, 0.0], 5)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert len(results) == 0, "Should return empty result for label with no data"


def test_vector_search_k_larger_than_dataset(ecommerce_db_populated):
    """Test vector search with k larger than available nodes."""
    session = ecommerce_db_populated.session()

    results = session.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 100)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert len(results) <= 100, "Should return at most k results"
    assert len(results) >= 4, "Should return all available products"


def test_vector_search_threshold_excludes_distant_results(ecommerce_db_populated):
    """Test threshold properly excludes results beyond distance limit."""
    session = ecommerce_db_populated.session()

    results = session.query("""
        CALL uni.vector.query('Product', 'embedding', [0.0, 0.0, 1.0, 0.0], 10, NULL, 0.1)
        YIELD node, distance
        RETURN node.name AS name, distance
    """)

    assert len(results) >= 1, "Should find at least the exact match"
    for row in results:
        assert row["distance"] <= 0.1, "All results should be within threshold"


def test_vector_search_chained_constraints(ecommerce_db_populated):
    """Test vector search with both filter and threshold."""
    session = ecommerce_db_populated.session()

    results = session.query("""
        CALL uni.vector.query('Product', 'embedding', [1.0, 0.0, 0.0, 0.0], 5, 'price > 0', 1.5)
        YIELD node, distance
        RETURN node.name AS name, node.price AS price, distance
    """)

    assert isinstance(results, object)
    for row in results:
        assert row["distance"] <= 1.5, "Distance should be within threshold"
        assert row["price"] > 0, "Price should satisfy filter"


def test_vector_search_different_dimensions(empty_db):
    """Test vector search with different dimensionality vectors."""
    session = empty_db.session()

    (
        empty_db.schema()
        .label("Doc5D")
        .property("name", "string")
        .vector("vec5", 5)
        .done()
        .apply()
    )

    tx = session.tx()
    tx.execute("CREATE (d:Doc5D {name: 'A', vec5: [1.0, 0.0, 0.0, 0.0, 0.0]})")
    tx.execute("CREATE (d:Doc5D {name: 'B', vec5: [0.0, 1.0, 0.0, 0.0, 0.0]})")
    tx.execute("CREATE (d:Doc5D {name: 'C', vec5: [0.0, 0.0, 1.0, 0.0, 0.0]})")
    tx.commit()
    empty_db.flush()

    empty_db.schema().label("Doc5D").index(
        "vec5", {"type": "vector", "metric": "l2"}
    ).apply()

    results = session.query("""
        CALL uni.vector.query('Doc5D', 'vec5', [1.0, 0.0, 0.0, 0.0, 0.0], 2)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert len(results) == 2
    assert results[0]["distance"] < 0.01


# ---------------------------------------------------------------------------
# MUVERA (ColBERT / MaxSim) multi-vector index via the Python binding (issue #96)
# ---------------------------------------------------------------------------

_MV_DIM = 8


def _basis(i):
    v = [0.0] * _MV_DIM
    v[i] = 1.0
    return v


def _vec_lit(vals):
    return "[" + ",".join(str(float(x)) for x in vals) + "]"


def _mv_lit(tokens):
    return "[" + ",".join(_vec_lit(t) for t in tokens) + "]"


def test_muvera_index_create_and_query(empty_db):
    """MUVERA index via the Python config-map surface: declare a `list:vector` field,
    create a `{"type":"vector","algorithm":"muvera",...}` index, then `uni.vector.query`
    ranks the exact-MaxSim maximizer first (FDE first stage + exact MaxSim re-rank).

    Exercises the Python binding's MUVERA option-parsing path (shared `uni-common`
    parser), which the Rust e2e suite cannot reach. Token convention: query `[e0,e1]`;
    the `target` doc has identical tokens (MaxSim 2.0); noise docs are orthogonal."""
    session = empty_db.session()
    (
        empty_db.schema()
        .label("Doc")
        .property("title", "string")
        .property("tokens", "list:vector:8")
        .done()
        .apply()
    )

    query_tokens = [_basis(0), _basis(1)]
    tx = session.tx()
    tx.execute(f"CREATE (d:Doc {{title: 'target', tokens: {_mv_lit(query_tokens)}}})")
    for i in range(20):
        # Orthogonal noise (tokens from e2..e7) → MaxSim well below the target's 2.0.
        toks = [_basis(2 + (i % 6)), _basis(2 + ((i + 1) % 6))]
        tx.execute(f"CREATE (d:Doc {{title: 'doc{i}', tokens: {_mv_lit(toks)}}})")
    tx.commit()
    empty_db.flush()

    # Create the MUVERA index over the already-flushed rows (backfill path).
    empty_db.schema().label("Doc").index(
        "tokens",
        {
            "type": "vector",
            "algorithm": "muvera",
            "k_sim": 4,
            "reps": 8,
            "d_proj": 8,
            "inner": "flat",
        },
    ).apply()

    results = session.query(f"""
        CALL uni.vector.query('Doc', 'tokens', {_mv_lit(query_tokens)}, 5)
        YIELD node, score
        RETURN node.title AS title, score
    """)
    titles = [r["title"] for r in results]
    assert titles, "MUVERA query should return results"
    assert titles[0] == "target", (
        f"MUVERA (Python) should rank the MaxSim maximizer first: {titles}"
    )


def test_muvera_index_is_hidden_from_user_columns(empty_db):
    """The derived `__fde_*` column must not leak into a Python `RETURN n` node map."""
    session = empty_db.session()
    (
        empty_db.schema()
        .label("Doc")
        .property("title", "string")
        .property("tokens", "list:vector:8")
        .done()
        .apply()
    )
    tx = session.tx()
    tx.execute(
        f"CREATE (d:Doc {{title: 'a', tokens: {_mv_lit([_basis(0), _basis(1)])}}})"
    )
    tx.commit()
    empty_db.flush()
    empty_db.schema().label("Doc").index(
        "tokens",
        {
            "type": "vector",
            "algorithm": "muvera",
            "k_sim": 4,
            "reps": 8,
            "d_proj": 8,
            "inner": "flat",
        },
    ).apply()

    rows = session.query("MATCH (d:Doc {title:'a'}) RETURN keys(d) AS k")
    keys = rows[0]["k"]
    assert not any(str(x).startswith("__") for x in keys), (
        f"internal FDE column leaked into keys(d): {keys}"
    )
    assert "tokens" in keys
