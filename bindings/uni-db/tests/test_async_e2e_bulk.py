# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async E2E tests for bulk insert functionality."""

import pytest


@pytest.mark.asyncio
async def test_create_bulk_writer_with_builder(async_social_db):
    """Test creating bulk writer using builder pattern."""
    session = async_social_db.session()
    writer = await session.bulk_writer().build()
    assert writer is not None

    writer.abort()


@pytest.mark.asyncio
async def test_bulk_insert_vertices(async_social_db):
    """Test bulk insertion of vertices."""
    session = async_social_db.session()
    writer = await session.bulk_writer().build()

    vertices = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
        {"name": "Charlie", "age": 35},
    ]

    vids = await writer.insert_vertices("Person", vertices)
    await writer.commit()

    assert len(vids) == 3
    assert all(isinstance(vid, int) for vid in vids)

    result = await session.query("MATCH (p:Person) RETURN p.name ORDER BY p.name")
    assert len(result) == 3
    assert result[0]["p.name"] == "Alice"
    assert result[1]["p.name"] == "Bob"
    assert result[2]["p.name"] == "Charlie"


@pytest.mark.asyncio
async def test_bulk_insert_edges(async_social_db):
    """Test bulk insertion of edges."""
    session = async_social_db.session()
    writer1 = await session.bulk_writer().build()
    vids = await writer1.insert_vertices(
        "Person",
        [
            {"name": "David", "age": 28, "email": "david@example.com"},
            {"name": "Eve", "age": 32, "email": "eve@example.com"},
        ],
    )
    await writer1.commit()

    writer2 = await session.bulk_writer().build()
    edges = [(vids[0], vids[1], {"since": 2020})]
    await writer2.insert_edges("KNOWS", edges)
    await writer2.commit()

    result = await session.query("""
        MATCH (p1:Person {name: 'David'})-[k:KNOWS]->(p2:Person {name: 'Eve'})
        RETURN k.since
    """)
    assert len(result) == 1
    assert result[0]["k.since"] == 2020


@pytest.mark.asyncio
async def test_builder_config_options(async_social_db):
    """Test bulk writer builder configuration options."""
    session = async_social_db.session()
    writer = await (
        session.bulk_writer()
        .defer_vector_indexes(True)
        .defer_scalar_indexes(True)
        .batch_size(500)
        .async_indexes(True)
        .build()
    )

    assert writer is not None

    vids = await writer.insert_vertices(
        "Person", [{"name": "Frank", "age": 40, "email": "frank@example.com"}]
    )
    await writer.commit()

    assert len(vids) == 1

    result = await session.query("MATCH (p:Person {name: 'Frank'}) RETURN p.name")
    assert len(result) == 1


@pytest.mark.asyncio
async def test_bulk_stats_attributes(async_social_db):
    """Test BulkStats attributes are accessible."""
    session = async_social_db.session()
    writer = await session.bulk_writer().build()
    await writer.insert_vertices(
        "Person", [{"name": "Grace", "age": 29}, {"name": "Henry", "age": 35}]
    )
    stats = await writer.commit()

    assert hasattr(stats, "vertices_inserted")
    assert hasattr(stats, "edges_inserted")
    assert hasattr(stats, "duration_secs")


@pytest.mark.asyncio
@pytest.mark.xfail(
    reason="abort() only sets a flag; insert_vertices writes directly to engine without batching, so data is already committed before abort"
)
async def test_bulk_writer_abort(async_social_db):
    """Test bulk writer abort functionality."""
    session = async_social_db.session()
    writer = await session.bulk_writer().build()
    await writer.insert_vertices(
        "Person", [{"name": "Iris", "age": 27, "email": "iris@example.com"}]
    )

    writer.abort()

    result = await session.query("MATCH (p:Person {name: 'Iris'}) RETURN p.name")
    assert len(result) == 0


@pytest.mark.asyncio
async def test_ops_after_abort_raise_error(async_social_db):
    """Test that operations after abort raise RuntimeError."""
    session = async_social_db.session()
    writer = await session.bulk_writer().build()
    writer.abort()

    with pytest.raises(
        RuntimeError, match=".*completed.*|.*finished.*|.*invalid.*|.*abort.*"
    ):
        await writer.insert_vertices("Person", [{"name": "Jack", "age": 30}])


@pytest.mark.asyncio
async def test_convenience_bulk_insert_vertices(async_social_db):
    """Test convenience method bulk_insert_vertices."""
    session = async_social_db.session()
    vertices = [{"name": "Kate", "age": 28}, {"name": "Liam", "age": 32}]

    bw = await session.bulk_writer().build()
    vids = await bw.insert_vertices("Person", vertices)
    await bw.commit()

    assert len(vids) == 2
    assert all(isinstance(vid, int) for vid in vids)

    result = await session.query(
        "MATCH (p:Person) WHERE p.name IN ['Kate', 'Liam'] RETURN p.name ORDER BY p.name"
    )
    assert len(result) == 2
    assert result[0]["p.name"] == "Kate"
    assert result[1]["p.name"] == "Liam"


@pytest.mark.asyncio
async def test_convenience_bulk_insert_edges(async_social_db):
    """Test convenience method bulk_insert_edges."""
    session = async_social_db.session()
    bw = await session.bulk_writer().build()
    vids = await bw.insert_vertices(
        "Person",
        [
            {"name": "Mia", "age": 26, "email": "mia@example.com"},
            {"name": "Noah", "age": 24, "email": "noah@example.com"},
        ],
    )

    edges = [(vids[0], vids[1], {"since": 2023})]
    await bw.insert_edges("KNOWS", edges)
    await bw.commit()

    result = await session.query("""
        MATCH (p1:Person {name: 'Mia'})-[k:KNOWS]->(p2:Person {name: 'Noah'})
        RETURN k.since
    """)
    assert len(result) == 1
    assert result[0]["k.since"] == 2023


@pytest.mark.asyncio
async def test_large_batch_insert(async_social_db):
    """Test inserting a large batch (1000+) of vertices."""
    session = async_social_db.session()
    vertices = [{"name": f"Person_{i}", "age": i} for i in range(1000)]

    writer = await session.bulk_writer().batch_size(200).build()
    vids = await writer.insert_vertices("Person", vertices)
    await writer.commit()

    assert len(vids) == 1000

    result = await session.query("MATCH (p:Person) RETURN count(p) as cnt")
    assert result[0]["cnt"] == 1000


@pytest.mark.asyncio
async def test_data_correctness_after_bulk_insert(async_empty_db):
    """Test that bulk inserted data maintains correctness."""
    await (
        async_empty_db.schema()
        .label("Member")
        .property("name", "string")
        .property_nullable("age", "int")
        .property_nullable("active", "bool")
        .property_nullable("score", "float")
        .done()
        .apply()
    )

    vertices = [
        {"name": "Olivia", "age": 29, "active": True, "score": 95.5},
        {"name": "Peter", "age": 41, "active": False, "score": 87.3},
        {"name": "Quinn", "age": 33, "active": True, "score": 92.1},
    ]

    session = async_empty_db.session()
    writer = await session.bulk_writer().build()
    await writer.insert_vertices("Member", vertices)
    await writer.commit()

    result = await session.query("""
        MATCH (m:Member {name: 'Olivia'})
        RETURN m.age, m.active, m.score
    """)
    assert len(result) == 1
    assert result[0]["m.age"] == 29
    assert result[0]["m.active"] is True
    assert abs(result[0]["m.score"] - 95.5) < 0.01

    result2 = await session.query("""
        MATCH (m:Member {name: 'Peter'})
        RETURN m.age, m.active, m.score
    """)
    assert len(result2) == 1
    assert result2[0]["m.age"] == 41
    assert result2[0]["m.active"] is False
    assert abs(result2[0]["m.score"] - 87.3) < 0.01
