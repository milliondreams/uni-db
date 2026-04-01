# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for async vector search via Cypher queries."""

import pytest

import uni_db


@pytest.fixture
async def vector_db():
    """Create an async database with vector-indexed documents."""
    db = await uni_db.AsyncUni.temporary()
    await (
        db.schema()
        .label("Document")
        .property("title", "string")
        .vector("embedding", 3)
        .index("embedding", {"type": "vector", "metric": "l2"})
        .done()
        .apply()
    )

    session = db.session()
    tx = await session.tx()
    await tx.execute("CREATE (d:Document {title: 'Doc1', embedding: [1.0, 0.0, 0.0]})")
    await tx.execute("CREATE (d:Document {title: 'Doc2', embedding: [0.0, 1.0, 0.0]})")
    await tx.execute("CREATE (d:Document {title: 'Doc3', embedding: [0.0, 0.0, 1.0]})")
    await tx.execute("CREATE (d:Document {title: 'Doc4', embedding: [0.5, 0.5, 0.0]})")
    await tx.commit()
    await db.flush()
    return db


@pytest.mark.asyncio
async def test_async_basic_vector_search(vector_db):
    """Test basic vector similarity search."""
    session = vector_db.session()
    results = await session.query("""
        CALL uni.vector.query('Document', 'embedding', [1.0, 0.0, 0.0], 2)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert len(results) == 2
    assert results[0]["distance"] < results[1]["distance"]


@pytest.mark.asyncio
async def test_async_vector_search_with_k(vector_db):
    """Test async vector search with k parameter."""
    session = vector_db.session()
    results = await session.query("""
        CALL uni.vector.query('Document', 'embedding', [0.5, 0.5, 0.0], 3)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert len(results) == 3


@pytest.mark.asyncio
async def test_async_vector_search_with_threshold(vector_db):
    """Test async vector search with distance threshold."""
    session = vector_db.session()
    results = await session.query("""
        CALL uni.vector.query('Document', 'embedding', [1.0, 0.0, 0.0], 10, NULL, 0.1)
        YIELD vid, distance
        RETURN vid, distance
    """)

    assert len(results) <= 1


@pytest.mark.asyncio
async def test_async_vector_search_fetch_nodes(vector_db):
    """Test fetching full nodes from async vector search."""
    session = vector_db.session()
    results = await session.query("""
        CALL uni.vector.query('Document', 'embedding', [1.0, 0.0, 0.0], 2)
        YIELD node, distance
        RETURN node.title AS title, distance
    """)

    assert len(results) == 2
    for row in results:
        assert "title" in row
        assert row["distance"] >= 0


@pytest.mark.asyncio
async def test_async_vector_match_attributes(vector_db):
    """Test vector search result attributes."""
    session = vector_db.session()
    results = await session.query("""
        CALL uni.vector.query('Document', 'embedding', [1.0, 0.0, 0.0], 1)
        YIELD vid, distance
        RETURN vid, distance
    """)
    assert len(results) == 1

    row = results[0]
    assert isinstance(row["vid"], int)
    assert isinstance(row["distance"], float)
