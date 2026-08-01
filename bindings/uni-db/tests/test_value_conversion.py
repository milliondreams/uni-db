"""Every stored ``Value`` variant must survive the Python boundary.

``convert.rs::value_to_py`` matches on ``uni_common::Value``, which is
``#[non_exhaustive]``. Because uni-db is a separate crate the match needs a
wildcard, so rustc cannot warn when a variant is added upstream. That wildcard
used to return ``None`` — and the ``SparseVector`` and ``BinaryVector`` arms
above it exist only because each was added *after* a release had already shipped
silently dropping that property.

The wildcard now raises. This file is the behavioural half: it pins that the
variants reachable from Python round-trip to real values rather than ``None``,
so a regression surfaces as a failing assert instead of a quietly missing field.
"""

import datetime

import uni_db


def _one_row(query: str):
    db = uni_db.UniBuilder.temporary().build()
    result = db.session().query(query)
    assert len(result) == 1
    return result[0]


def test_scalar_variants_do_not_become_none():
    row = _one_row(
        "RETURN 'hello' AS s, 42 AS i, 1.5 AS f, true AS b, null AS n"
    )
    assert row["s"] == "hello", f"String became {row['s']!r}"
    assert row["i"] == 42, f"Int became {row['i']!r}"
    assert row["f"] == 1.5, f"Float became {row['f']!r}"
    assert row["b"] is True, f"Bool became {row['b']!r}"
    # Null is the one variant that legitimately converts to None.
    assert row["n"] is None


def test_list_and_map_variants_do_not_become_none():
    row = _one_row("RETURN [1, 2, 3] AS lst, {a: 1, b: 'x'} AS m")
    assert row["lst"] == [1, 2, 3], f"List became {row['lst']!r}"
    assert row["m"] == {"a": 1, "b": "x"}, f"Map became {row['m']!r}"


def test_temporal_variant_does_not_become_none():
    row = _one_row("RETURN date('2024-01-15') AS d")
    assert row["d"] is not None, "Temporal became None at the Python boundary"
    assert row["d"] == datetime.date(2024, 1, 15), f"Temporal became {row['d']!r}"


def test_vector_variant_does_not_become_none():
    db = uni_db.UniBuilder.temporary().build()
    db.schema().label("V").property("v", uni_db.DataType.vector(3)).apply()

    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (:V {v: [1.0, 2.0, 3.0]})")
    tx.commit()

    result = session.query("MATCH (n:V) RETURN n.v AS v")
    got = result[0]["v"]
    assert got is not None, "Vector became None at the Python boundary"
    assert list(got) == [1.0, 2.0, 3.0], f"Vector became {got!r}"


def test_bytes_variant_does_not_become_none():
    db = uni_db.UniBuilder.temporary().build()
    db.schema().label("B").property("raw", uni_db.DataType.BYTES()).apply()

    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (:B {raw: $v})", {"v": b"\x00\x01\xff"})
    tx.commit()

    result = session.query("MATCH (n:B) RETURN n.raw AS raw")
    got = result[0]["raw"]
    assert got is not None, "Bytes became None at the Python boundary"
    assert bytes(got) == b"\x00\x01\xff", f"Bytes became {got!r}"
