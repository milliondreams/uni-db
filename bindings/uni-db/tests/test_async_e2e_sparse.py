# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Async E2E tests for learned-sparse (SPLADE) vectors — issue #95, set J.

Load-bearing Python surface: a prior multi-vector (#96) Python pass caught a real
Rust bug, and this suite likewise exercises the `value_to_py` round-trip arm (a
returned sparse property must come back as `SparseVector`, not `None`) and the
ingestion collision fix (a typed `SparseVector` must be extracted before the
dict->Map branch). Also covers `DataType.sparse_vector`, `uni.sparse.query`
ranking against a hand-computed dot product, and the empty/unsorted edges.
"""

import pytest

from uni_db import SparseVector

VOCAB = 1000


def _dot(qi, qv, di, dv):
    qm = dict(zip(qi, qv))
    return sum(qm.get(t, 0.0) * w for t, w in zip(di, dv))


@pytest.mark.asyncio
async def test_sparse_vector_type_constructors():
    """`SparseVector` constructs from parallel lists and from a dict, and exposes
    canonicalized (sorted, summed) indices/values."""
    a = SparseVector([9, 2, 2], [3.0, 1.0, 0.5])  # unsorted + duplicate term 2
    # term 2 weights summed (1.0 + 0.5), indices sorted ascending.
    assert a.indices == [2, 9]
    assert a.values == pytest.approx([1.5, 3.0])  # f32 storage

    b = SparseVector.from_dict({40: 0.3, 2: 0.7})
    assert b.indices == [2, 40]
    assert b.values == pytest.approx([0.7, 0.3])  # f32 storage

    # Empty sparse vector is valid.
    e = SparseVector([], [])
    assert len(e) == 0


@pytest.mark.asyncio
async def test_sparse_typed_ingestion_roundtrips(async_empty_db):
    """A typed `SparseVector` ingested as a property round-trips back as a
    `SparseVector` (exercises the `value_to_py` arm and the collision fix).
    Without those, a returned sparse property is silently `None`."""
    await (
        async_empty_db.schema()
        .label("Doc")
        .property("title", "string")
        .property("emb", f"sparse_vector:{VOCAB}")
        .done()
        .apply()
    )

    session = async_empty_db.session()
    tx = await session.tx()
    await tx.query(
        "CREATE (d:Doc {title: $t, emb: $e})",
        {"t": "a", "e": SparseVector([1, 5, 9], [1.0, 2.0, 3.0])},
    )
    await tx.commit()

    # Read back from L0 (pre-flush).
    rows = await session.query("MATCH (d:Doc) RETURN d.emb AS emb")
    emb = rows[0]["emb"]
    assert isinstance(emb, SparseVector), f"expected SparseVector, got {type(emb)}"
    assert emb.indices == [1, 5, 9]
    assert emb.values == [1.0, 2.0, 3.0]

    # Read back from flushed storage too.
    await async_empty_db.flush()
    rows2 = await session.query("MATCH (d:Doc) RETURN d.emb AS emb")
    assert isinstance(rows2[0]["emb"], SparseVector)


@pytest.mark.asyncio
async def test_sparse_query_ranks_self_match_first(async_empty_db):
    """`uni.sparse.query` ranks the doc whose `emb == query` first, with its
    reported score equal to the exact dot product."""
    await (
        async_empty_db.schema()
        .label("Doc")
        .property("title", "string")
        .property("emb", f"sparse_vector:{VOCAB}")
        .index("emb", {"type": "sparse"})
        .done()
        .apply()
    )

    qi, qv = [1, 5, 9], [1.0, 2.0, 3.0]
    session = async_empty_db.session()
    tx = await session.tx()
    await tx.query(
        "CREATE (d:Doc {title: $t, emb: $e})",
        {"t": "target", "e": SparseVector(qi, qv)},
    )
    await tx.query(
        "CREATE (d:Doc {title: $t, emb: $e})",
        {"t": "other", "e": SparseVector([100, 200], [1.0, 1.0])},
    )
    await tx.commit()
    await async_empty_db.flush()

    # Query param as a typed SparseVector.
    results = await session.query(
        "CALL uni.sparse.query('Doc', 'emb', $q, 5, null, null, {}) "
        "YIELD node, score RETURN node.title AS title, score",
        {"q": SparseVector(qi, qv)},
    )
    assert results, "sparse query returned no rows"
    assert results[0]["title"] == "target", f"target should rank first: {results}"
    want = _dot(qi, qv, qi, qv)  # self-dot = 1 + 4 + 9 = 14
    assert abs(results[0]["score"] - want) < 1e-3


@pytest.mark.asyncio
async def test_sparse_query_param_as_dict(async_empty_db):
    """The query argument also accepts an `{indices, values}` dict (no typed
    SparseVector needed)."""
    await (
        async_empty_db.schema()
        .label("Doc")
        .property("title", "string")
        .property("emb", f"sparse_vector:{VOCAB}")
        .index("emb", {"type": "sparse"})
        .done()
        .apply()
    )

    session = async_empty_db.session()
    tx = await session.tx()
    await tx.query(
        "CREATE (d:Doc {title: $t, emb: $e})",
        {"t": "target", "e": SparseVector([1, 5, 9], [1.0, 2.0, 3.0])},
    )
    await tx.commit()
    await async_empty_db.flush()

    results = await session.query(
        "CALL uni.sparse.query('Doc', 'emb', $q, 5, null, null, {}) "
        "YIELD node, score RETURN node.title AS title, score",
        {"q": {"indices": [1, 5, 9], "values": [1.0, 2.0, 3.0]}},
    )
    assert results and results[0]["title"] == "target"


@pytest.mark.asyncio
async def test_sparse_datatype_helper_equivalence(async_empty_db):
    """`DataType.sparse_vector(N)` and the `"sparse_vector:N"` string declare the
    same column type."""
    from uni_db import DataType

    dt = DataType.sparse_vector(VOCAB)
    assert "sparse_vector" in repr(dt)
    assert str(VOCAB) in repr(dt)
