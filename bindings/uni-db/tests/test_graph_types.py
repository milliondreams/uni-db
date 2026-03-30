# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Tests for typed Node, Edge, Path graph objects returned from queries."""

import warnings

import pytest

import uni_db

# =============================================================================
# Node tests
# =============================================================================


class TestNode:
    """Tests for the Node type returned when a Cypher query returns a full node."""

    def test_node_from_query(self, social_db):
        session = social_db.session()
        session.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        result = session.query("MATCH (p:Person) RETURN p")
        assert len(result) == 1
        node = result[0]["p"]
        assert isinstance(node, uni_db.Node)

    def test_node_attributes(self, social_db):
        session = social_db.session()
        session.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        result = session.query("MATCH (p:Person) RETURN p")
        node = result[0]["p"]

        assert isinstance(node.id, uni_db.Vid)
        assert node.id == node.element_id
        assert node.labels == ["Person"]
        assert node.properties == {"name": "Alice", "age": 30}

    def test_node_dict_access(self, social_db):
        session = social_db.session()
        session.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        result = session.query("MATCH (p:Person) RETURN p")
        node = result[0]["p"]

        # __getitem__
        assert node["name"] == "Alice"
        assert node["age"] == 30

        # get() with default
        assert node.get("name") == "Alice"
        assert node.get("missing") is None
        assert node.get("missing", 42) == 42

        # __contains__
        assert "name" in node
        assert "missing" not in node

        # __len__
        assert len(node) == 2

        # keys/values/items
        assert sorted(node.keys()) == ["age", "name"]
        assert set(node.values()) == {"Alice", 30}
        items = dict(node.items())
        assert items == {"name": "Alice", "age": 30}

    def test_node_iteration(self, social_db):
        session = social_db.session()
        session.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        result = session.query("MATCH (p:Person) RETURN p")
        node = result[0]["p"]
        keys = list(node)
        assert sorted(keys) == ["age", "name"]

    def test_node_keyerror(self, social_db):
        session = social_db.session()
        session.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        result = session.query("MATCH (p:Person) RETURN p")
        node = result[0]["p"]
        with pytest.raises(KeyError, match="nonexistent"):
            node["nonexistent"]

    def test_node_deprecated_keys(self, social_db):
        session = social_db.session()
        session.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        result = session.query("MATCH (p:Person) RETURN p")
        node = result[0]["p"]

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            nid = node["_id"]
            assert isinstance(nid, int)
            assert nid == node.id
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "Node.id" in str(w[0].message)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            labels = node["_labels"]
            assert labels == ["Person"]
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)

    def test_node_equality_and_hash(self, social_db):
        session = social_db.session()
        session.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        r1 = session.query("MATCH (p:Person) RETURN p")
        r2 = session.query("MATCH (p:Person) RETURN p")
        n1 = r1[0]["p"]
        n2 = r2[0]["p"]

        assert n1 == n2
        assert hash(n1) == hash(n2)
        assert len({n1, n2}) == 1  # deduplicates in a set

    def test_node_repr(self, social_db):
        session = social_db.session()
        session.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        result = session.query("MATCH (p:Person) RETURN p")
        node = result[0]["p"]
        r = repr(node)
        assert "Node(" in r
        assert "Person" in r

    def test_node_bool_always_true(self, social_db):
        session = social_db.session()
        session.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        result = session.query("MATCH (p:Person) RETURN p")
        node = result[0]["p"]
        assert bool(node) is True

    def test_node_id_is_vid(self, social_db):
        """Node IDs must be Vid type (convertible to int)."""
        session = social_db.session()
        session.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        result = session.query("MATCH (p:Person) RETURN p")
        node = result[0]["p"]
        assert isinstance(node.id, uni_db.Vid)
        assert isinstance(int(node.id), int)


# =============================================================================
# Edge tests
# =============================================================================


class TestEdge:
    """Tests for the Edge type returned when a Cypher query returns a full edge."""

    def test_edge_from_query(self, social_db):
        session = social_db.session()
        session.execute("CREATE (a:Person {name: 'Alice', age: 30})")
        session.execute("CREATE (b:Person {name: 'Bob', age: 25})")
        session.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS {since: 2020}]->(b)"
        )
        result = session.query("MATCH ()-[r:KNOWS]->() RETURN r")
        assert len(result) == 1
        edge = result[0]["r"]
        assert isinstance(edge, uni_db.Edge)

    def test_edge_attributes(self, social_db):
        session = social_db.session()
        session.execute("CREATE (a:Person {name: 'Alice', age: 30})")
        session.execute("CREATE (b:Person {name: 'Bob', age: 25})")
        session.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS {since: 2020}]->(b)"
        )
        result = session.query("MATCH ()-[r:KNOWS]->() RETURN r")
        edge = result[0]["r"]

        assert isinstance(edge.id, uni_db.Eid)
        assert edge.id == edge.element_id
        assert edge.type == "KNOWS"
        assert isinstance(edge.start_id, uni_db.Vid)
        assert isinstance(edge.end_id, uni_db.Vid)
        assert edge.start_id != edge.end_id
        assert edge.properties == {"since": 2020}

    def test_edge_dict_access(self, social_db):
        session = social_db.session()
        session.execute("CREATE (a:Person {name: 'Alice', age: 30})")
        session.execute("CREATE (b:Person {name: 'Bob', age: 25})")
        session.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS {since: 2020}]->(b)"
        )
        result = session.query("MATCH ()-[r:KNOWS]->() RETURN r")
        edge = result[0]["r"]

        assert edge["since"] == 2020
        assert edge.get("since") == 2020
        assert edge.get("missing") is None
        assert "since" in edge
        assert "missing" not in edge
        assert len(edge) == 1
        assert edge.keys() == ["since"]

    def test_edge_deprecated_keys(self, social_db):
        session = social_db.session()
        session.execute("CREATE (a:Person {name: 'Alice', age: 30})")
        session.execute("CREATE (b:Person {name: 'Bob', age: 25})")
        session.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS {since: 2020}]->(b)"
        )
        result = session.query("MATCH ()-[r:KNOWS]->() RETURN r")
        edge = result[0]["r"]

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert edge["_type"] == "KNOWS"
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            src = edge["_src"]
            assert isinstance(src, str)  # backward compat: string
            assert len(w) == 1

    def test_edge_equality_and_hash(self, social_db):
        session = social_db.session()
        session.execute("CREATE (a:Person {name: 'Alice', age: 30})")
        session.execute("CREATE (b:Person {name: 'Bob', age: 25})")
        session.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS {since: 2020}]->(b)"
        )
        r1 = session.query("MATCH ()-[r:KNOWS]->() RETURN r")
        r2 = session.query("MATCH ()-[r:KNOWS]->() RETURN r")
        e1 = r1[0]["r"]
        e2 = r2[0]["r"]
        assert e1 == e2
        assert hash(e1) == hash(e2)

    def test_edge_repr(self, social_db):
        session = social_db.session()
        session.execute("CREATE (a:Person {name: 'Alice', age: 30})")
        session.execute("CREATE (b:Person {name: 'Bob', age: 25})")
        session.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS {since: 2020}]->(b)"
        )
        result = session.query("MATCH ()-[r:KNOWS]->() RETURN r")
        edge = result[0]["r"]
        r = repr(edge)
        assert "Edge(" in r
        assert "KNOWS" in r

    def test_edge_id_is_eid(self, social_db):
        """Edge IDs must be Eid type (convertible to int)."""
        session = social_db.session()
        session.execute("CREATE (a:Person {name: 'Alice', age: 30})")
        session.execute("CREATE (b:Person {name: 'Bob', age: 25})")
        session.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS]->(b)"
        )
        result = session.query("MATCH ()-[r:KNOWS]->() RETURN r")
        edge = result[0]["r"]
        assert isinstance(edge.id, uni_db.Eid)
        assert isinstance(int(edge.id), int)


# =============================================================================
# Path tests
# =============================================================================


class TestPath:
    """Tests for the Path type returned when a Cypher query returns a full path."""

    def test_path_from_query(self, social_db):
        session = social_db.session()
        session.execute("CREATE (a:Person {name: 'Alice', age: 30})")
        session.execute("CREATE (b:Person {name: 'Bob', age: 25})")
        session.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS {since: 2020}]->(b)"
        )
        result = session.query(
            "MATCH p=(a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) RETURN p"
        )
        assert len(result) == 1
        path = result[0]["p"]
        assert isinstance(path, uni_db.Path)

    def test_path_attributes(self, social_db):
        session = social_db.session()
        session.execute("CREATE (a:Person {name: 'Alice', age: 30})")
        session.execute("CREATE (b:Person {name: 'Bob', age: 25})")
        session.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS {since: 2020}]->(b)"
        )
        result = session.query(
            "MATCH p=(a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) RETURN p"
        )
        path = result[0]["p"]

        assert len(path.nodes) == 2
        assert len(path.edges) == 1
        assert all(isinstance(n, uni_db.Node) for n in path.nodes)
        assert all(isinstance(e, uni_db.Edge) for e in path.edges)
        assert isinstance(path.start, uni_db.Node)
        assert isinstance(path.end, uni_db.Node)
        assert path.start.id != path.end.id
        assert not path.is_empty()

    def test_path_len(self, social_db):
        session = social_db.session()
        session.execute("CREATE (a:Person {name: 'Alice', age: 30})")
        session.execute("CREATE (b:Person {name: 'Bob', age: 25})")
        session.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS]->(b)"
        )
        result = session.query(
            "MATCH p=(a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) RETURN p"
        )
        path = result[0]["p"]
        assert len(path) == 1  # 1 hop

    def test_path_interleaved_indexing(self, social_db):
        session = social_db.session()
        session.execute("CREATE (a:Person {name: 'Alice', age: 30})")
        session.execute("CREATE (b:Person {name: 'Bob', age: 25})")
        session.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS]->(b)"
        )
        result = session.query(
            "MATCH p=(a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) RETURN p"
        )
        path = result[0]["p"]

        # path[0] = first node, path[1] = first edge, path[2] = second node
        assert isinstance(path[0], uni_db.Node)
        assert isinstance(path[1], uni_db.Edge)
        assert isinstance(path[2], uni_db.Node)

        # Negative indexing
        assert isinstance(path[-1], uni_db.Node)
        assert isinstance(path[-2], uni_db.Edge)

    def test_path_iteration(self, social_db):
        session = social_db.session()
        session.execute("CREATE (a:Person {name: 'Alice', age: 30})")
        session.execute("CREATE (b:Person {name: 'Bob', age: 25})")
        session.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS]->(b)"
        )
        result = session.query(
            "MATCH p=(a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) RETURN p"
        )
        path = result[0]["p"]
        elements = list(path)
        assert len(elements) == 3  # node, edge, node
        assert isinstance(elements[0], uni_db.Node)
        assert isinstance(elements[1], uni_db.Edge)
        assert isinstance(elements[2], uni_db.Node)

    def test_path_index_out_of_range(self, social_db):
        session = social_db.session()
        session.execute("CREATE (a:Person {name: 'Alice', age: 30})")
        session.execute("CREATE (b:Person {name: 'Bob', age: 25})")
        session.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS]->(b)"
        )
        result = session.query(
            "MATCH p=(a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) RETURN p"
        )
        path = result[0]["p"]
        with pytest.raises(IndexError):
            path[99]

    def test_path_repr(self, social_db):
        session = social_db.session()
        session.execute("CREATE (a:Person {name: 'Alice', age: 30})")
        session.execute("CREATE (b:Person {name: 'Bob', age: 25})")
        session.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS]->(b)"
        )
        result = session.query(
            "MATCH p=(a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) RETURN p"
        )
        path = result[0]["p"]
        r = repr(path)
        assert "Path(" in r
        assert "nodes=2" in r
        assert "edges=1" in r


# =============================================================================
# Mixed return tests
# =============================================================================


class TestMixedReturn:
    """Tests for queries returning a mix of nodes, edges, and scalars."""

    def test_mixed_return_types(self, social_db):
        session = social_db.session()
        session.execute("CREATE (a:Person {name: 'Alice', age: 30})")
        session.execute("CREATE (b:Person {name: 'Bob', age: 25})")
        session.execute(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
            "CREATE (a)-[:KNOWS {since: 2020}]->(b)"
        )
        result = session.query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b")
        assert len(result) == 1
        row = result[0]
        assert isinstance(row["a"], uni_db.Node)
        assert isinstance(row["r"], uni_db.Edge)
        assert isinstance(row["b"], uni_db.Node)

        # Verify cross-references
        assert row["r"].start_id == row["a"].id
        assert row["r"].end_id == row["b"].id

    def test_node_and_scalar_mixed(self, social_db):
        session = social_db.session()
        session.execute("CREATE (p:Person {name: 'Alice', age: 30})")
        result = session.query(
            "MATCH (p:Person) RETURN p, p.name AS name, p.age AS age"
        )
        row = result[0]
        assert isinstance(row["p"], uni_db.Node)
        assert row["name"] == "Alice"
        assert row["age"] == 30
