# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Regression tests for issue #137: declared VECTOR(dim) must enforce its dimension.

Pre-fix, a wrong-length vector write was silently accepted (then nulled at
flush, detonating at shutdown), a wrong-length kNN query silently returned
0 rows, and re-declaring a column with a different dimension was silently
ignored. All three must now raise immediately.
"""

import pytest

import uni_db


@pytest.fixture
def dim_db():
    """A sync database with one VECTOR(4)-typed nullable column, no data."""
    db = uni_db.Uni.temporary()
    (
        db.schema()
        .label("Doc")
        .property_nullable("title", "string")
        .property_nullable("embedding", "vector:4")
        .index("embedding", {"type": "vector", "metric": "l2"})
        .done()
        .apply()
    )
    yield db
    db.shutdown()


def _write(db, cypher):
    session = db.session()
    tx = session.tx()
    try:
        tx.execute(cypher)
        tx.commit()
    except Exception:
        tx.rollback()
        raise


def test_wrong_dim_write_raises(dim_db):
    """Case (a): a 5-dim write into VECTOR(4) raises instead of nulling at flush."""
    with pytest.raises(uni_db.UniError) as exc_info:
        _write(
            dim_db, "CREATE (:Doc {title: 'bad', embedding: [1.0, 2.0, 3.0, 4.0, 5.0]})"
        )
    msg = str(exc_info.value)
    assert "declared" in msg
    assert "4" in msg and "5" in msg, msg


def test_correct_dim_write_and_shutdown_clean():
    """Negative control: correct-dim writes work and shutdown raises nothing."""
    db = uni_db.Uni.temporary()
    (
        db.schema()
        .label("Doc")
        .property_nullable("embedding", "vector:4")
        .index("embedding", {"type": "vector", "metric": "l2"})
        .done()
        .apply()
    )
    _write(db, "CREATE (:Doc {embedding: [1.0, 0.0, 0.0, 0.0]})")
    results = db.session().query(
        "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0, 0.0, 0.0], 5) "
        "YIELD vid, distance RETURN vid"
    )
    assert len(results) == 1
    db.flush()
    db.shutdown()  # the original repro raised UniInternalError here


def test_wrong_dim_query_raises(dim_db):
    """Case (b): an 8-dim query against VECTOR(4) raises instead of returning 0 rows."""
    _write(dim_db, "CREATE (:Doc {title: 'ok', embedding: [1.0, 0.0, 0.0, 0.0]})")
    with pytest.raises(uni_db.UniError) as exc_info:
        dim_db.session().query(
            "CALL uni.vector.query('Doc', 'embedding', "
            "[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 5) "
            "YIELD vid, distance RETURN vid"
        )
    assert "dimension mismatch" in str(exc_info.value)


def test_redeclare_identical_is_idempotent(dim_db):
    """Re-applying the identical schema (register-on-every-open) stays a no-op."""
    (
        dim_db.schema()
        .label("Doc")
        .property_nullable("title", "string")
        .property_nullable("embedding", "vector:4")
        .index("embedding", {"type": "vector", "metric": "l2"})
        .done()
        .apply()
    )


def test_redeclare_different_dim_raises(dim_db):
    """Case (c): re-declaring VECTOR(4) as VECTOR(8) is a schema conflict."""
    with pytest.raises(uni_db.UniSchemaError) as exc_info:
        (
            dim_db.schema()
            .label("Doc")
            .property_nullable("embedding", "vector:8")
            .done()
            .apply()
        )
    msg = str(exc_info.value)
    assert "4" in msg and "8" in msg, msg

    # The column still enforces the ORIGINAL dimension.
    _write(dim_db, "CREATE (:Doc {embedding: [1.0, 2.0, 3.0, 4.0]})")
    with pytest.raises(uni_db.UniError):
        _write(
            dim_db,
            "CREATE (:Doc {embedding: [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]})",
        )


def test_bulk_insert_wrong_dim_raises(dim_db):
    """The bulk writer (which bypasses the Cypher guard) rejects wrong-dim rows."""
    session = dim_db.session()
    tx = session.tx()
    writer = tx.bulk_writer().build()
    with pytest.raises(uni_db.UniError) as exc_info:
        writer.insert_vertices(
            "Doc",
            [
                {"embedding": [1.0, 2.0, 3.0, 4.0]},
                {"embedding": [1.0, 2.0]},  # wrong dim at row 1
            ],
        )
    msg = str(exc_info.value)
    assert "dimension mismatch" in msg
    tx.rollback()

    # A clean batch commits fine.
    tx = session.tx()
    writer = tx.bulk_writer().build()
    vids = writer.insert_vertices(
        "Doc",
        [
            {"embedding": [1.0, 2.0, 3.0, 4.0]},
            {"embedding": [4.0, 3.0, 2.0, 1.0]},
        ],
    )
    assert len(vids) == 2
    writer.commit()
    tx.commit()
