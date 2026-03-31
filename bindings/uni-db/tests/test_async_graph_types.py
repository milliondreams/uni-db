# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async tests for typed Node, Edge, Path graph objects returned from queries."""

import warnings

import pytest

import uni_db


@pytest.mark.asyncio
class TestAsyncNode:
    """Async tests for the Node type."""

    async def test_node_from_query(self, async_social_db):
        session = async_social_db.session()
        tx = await session.tx()
        await tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        await tx.commit()
        result = await session.query("MATCH (p:Person) RETURN p")
        assert len(result) == 1
        node = result[0]["p"]
        assert isinstance(node, uni_db.Node)

    async def test_node_attributes(self, async_social_db):
        session = async_social_db.session()
        tx = await session.tx()
        await tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        await tx.commit()
        result = await session.query("MATCH (p:Person) RETURN p")
        node = result[0]["p"]

        assert isinstance(node.id, uni_db.Vid)
        assert node.labels == ["Person"]
        assert node.properties == {"name": "Alice", "age": 30}

    async def test_node_dict_access(self, async_social_db):
        session = async_social_db.session()
        tx = await session.tx()
        await tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        await tx.commit()
        result = await session.query("MATCH (p:Person) RETURN p")
        node = result[0]["p"]

        assert node["name"] == "Alice"
        assert node.get("missing", 42) == 42
        assert "name" in node
        assert len(node) == 2

    async def test_node_deprecated_keys(self, async_social_db):
        session = async_social_db.session()
        tx = await session.tx()
        await tx.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        await tx.commit()
        result = await session.query("MATCH (p:Person) RETURN p")
        node = result[0]["p"]

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            nid = node["_id"]
            assert isinstance(nid, int)
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)


@pytest.mark.asyncio
class TestAsyncEdge:
    """Async tests for the Edge type."""

    async def test_edge_from_query(self, async_social_db):
        session = async_social_db.session()
        tx = await session.tx()
        await tx.execute("CREATE (a:Person {name: 'Alice', age: 30})")
        await tx.execute("CREATE (b:Person {name: 'Bob', age: 25})")
        await tx.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS {since: 2020}]->(b)"
        )
        await tx.commit()
        result = await session.query("MATCH ()-[r:KNOWS]->() RETURN r")
        assert len(result) == 1
        edge = result[0]["r"]
        assert isinstance(edge, uni_db.Edge)

    async def test_edge_attributes(self, async_social_db):
        session = async_social_db.session()
        tx = await session.tx()
        await tx.execute("CREATE (a:Person {name: 'Alice', age: 30})")
        await tx.execute("CREATE (b:Person {name: 'Bob', age: 25})")
        await tx.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS {since: 2020}]->(b)"
        )
        await tx.commit()
        result = await session.query("MATCH ()-[r:KNOWS]->() RETURN r")
        edge = result[0]["r"]

        assert isinstance(edge.id, uni_db.Eid)
        assert edge.type == "KNOWS"
        assert isinstance(edge.start_id, uni_db.Vid)
        assert isinstance(edge.end_id, uni_db.Vid)
        assert edge.properties == {"since": 2020}


@pytest.mark.asyncio
class TestAsyncPath:
    """Async tests for the Path type."""

    async def test_path_from_query(self, async_social_db):
        session = async_social_db.session()
        tx = await session.tx()
        await tx.execute("CREATE (a:Person {name: 'Alice', age: 30})")
        await tx.execute("CREATE (b:Person {name: 'Bob', age: 25})")
        await tx.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS]->(b)"
        )
        await tx.commit()
        result = await session.query(
            "MATCH p=(a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) RETURN p"
        )
        assert len(result) == 1
        path = result[0]["p"]
        assert isinstance(path, uni_db.Path)
        assert len(path.nodes) == 2
        assert len(path.edges) == 1

    async def test_path_interleaved_indexing(self, async_social_db):
        session = async_social_db.session()
        tx = await session.tx()
        await tx.execute("CREATE (a:Person {name: 'Alice', age: 30})")
        await tx.execute("CREATE (b:Person {name: 'Bob', age: 25})")
        await tx.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS]->(b)"
        )
        await tx.commit()
        result = await session.query(
            "MATCH p=(a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) RETURN p"
        )
        path = result[0]["p"]
        assert isinstance(path[0], uni_db.Node)
        assert isinstance(path[1], uni_db.Edge)
        assert isinstance(path[2], uni_db.Node)


@pytest.mark.asyncio
class TestAsyncMixedReturn:
    """Async tests for mixed return types."""

    async def test_mixed_return(self, async_social_db):
        session = async_social_db.session()
        tx = await session.tx()
        await tx.execute("CREATE (a:Person {name: 'Alice', age: 30})")
        await tx.execute("CREATE (b:Person {name: 'Bob', age: 25})")
        await tx.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS {since: 2020}]->(b)"
        )
        await tx.commit()
        result = await session.query(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b"
        )
        row = result[0]
        assert isinstance(row["a"], uni_db.Node)
        assert isinstance(row["r"], uni_db.Edge)
        assert isinstance(row["b"], uni_db.Node)
        assert row["r"].start_id == row["a"].id
        assert row["r"].end_id == row["b"].id
