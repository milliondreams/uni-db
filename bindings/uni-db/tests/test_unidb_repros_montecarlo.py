# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""Authoritative repros for the uniscape × uni-db gap report (UNIDB_REQUIREMENTS).

Mirrors the reporter's Python (wheel) environment for the six actionable items:

- REQ-1  : per-group recursive transitive closure via prefix-`TO`
           (`(it, a) IS reach TO m`).
- REQ-1b : a Locy program's result is context-independent.
- REQ-2  : Rhai aggregate UDFs callable in a Cypher aggregation.
- REQ-3  : native `stDev` / `variance` aggregates in Cypher.
- REQ-4  : a raw integer property passed to a plugin scalar without `toInteger`.
- REQ-5b : `AND` between two `IS` predicates in a rule `WHERE`.

The corresponding Rust regression tests (which also cover concurrent-writer
isolation and failure scenarios) live under `crates/uni/tests/common/`.
"""

import math

import pytest

import uni_db


def _fresh_db():
    return uni_db.UniBuilder.temporary().build()


# ---------------------------------------------------------------------------
# REQ-1 — per-group recursive transitive closure via prefix-`TO`
# ---------------------------------------------------------------------------

GROUPED_REACH = """
CREATE RULE active AS
  MATCH (it:Iter),(a:Bus)-[e:LINE]->(b:Bus)
  WHERE toInteger(e.it) = toInteger(it.idx)
  YIELD KEY it, KEY a, KEY b
CREATE RULE reach AS
  MATCH (it:Iter),(a:Bus),(b:Bus) WHERE (it,a,b) IS active
  YIELD KEY it, KEY a, KEY b
CREATE RULE reach AS
  MATCH (it:Iter),(a:Bus) WHERE (it,a) IS reach TO m, (it,m) IS active TO b
  YIELD KEY it, KEY a, KEY b
"""


def test_grouped_three_key_recursion_reaches_fixpoint():
    """it 0: A->B->C, it 1: A->B. Per-group closure = {AB,BC,AC}@0 + {AB}@1 = 4.

    3 would mean the recursion stalled at one hop; 5 a cross-group leak.
    """
    db = _fresh_db()
    tx = db.session().tx()
    tx.execute(
        """CREATE (i0:Iter {idx: 0}), (i1:Iter {idx: 1}),
                  (a:Bus {name: 'A'}), (b:Bus {name: 'B'}), (c:Bus {name: 'C'}),
                  (a)-[:LINE {it: 0}]->(b),
                  (b)-[:LINE {it: 0}]->(c),
                  (a)-[:LINE {it: 1}]->(b)"""
    )
    tx.commit()

    result = db.session().locy(GROUPED_REACH)
    reach = result.derived.get("reach", [])
    assert len(reach) == 4, f"expected per-group closure of 4 facts, got {len(reach)}"


# ---------------------------------------------------------------------------
# REQ-5b — AND between IS predicates
# ---------------------------------------------------------------------------


def _and_program(sep: str) -> str:
    return f"""
CREATE RULE active AS
  MATCH (it:Iter),(a:Bus)-[e:LINE]->(b:Bus)
  WHERE toInteger(e.it) = toInteger(it.idx)
  YIELD KEY it, KEY a, KEY b
CREATE RULE reach AS
  MATCH (it:Iter),(a:Bus),(b:Bus) WHERE (it,a,b) IS active
  YIELD KEY it, KEY a, KEY b
CREATE RULE reach AS
  MATCH (it:Iter),(a:Bus) WHERE (it,a) IS reach TO m, (it,m) IS active TO b
  YIELD KEY it, KEY a, KEY b
CREATE RULE both AS
  MATCH (it:Iter),(a:Bus),(b:Bus)
  WHERE (it,a,b) IS active {sep} (it,a,b) IS reach
  YIELD KEY it, KEY a, KEY b
"""


def _grid(db):
    tx = db.session().tx()
    tx.execute(
        """CREATE (i0:Iter {idx: 0}), (i1:Iter {idx: 1}),
                  (a:Bus {name: 'A'}), (b:Bus {name: 'B'}), (c:Bus {name: 'C'}),
                  (a)-[:LINE {it: 0}]->(b),
                  (b)-[:LINE {it: 0}]->(c),
                  (a)-[:LINE {it: 1}]->(b)"""
    )
    tx.commit()


def test_and_between_is_predicates_matches_comma():
    db_and = _fresh_db()
    _grid(db_and)
    n_and = len(db_and.session().locy(_and_program("AND")).derived.get("both", []))

    db_comma = _fresh_db()
    _grid(db_comma)
    n_comma = len(db_comma.session().locy(_and_program(",")).derived.get("both", []))

    assert n_and == 3, f"`both` (= active) should be 3 facts, got {n_and}"
    assert n_and == n_comma, "AND and comma separators must agree"


# ---------------------------------------------------------------------------
# REQ-1b — context-independence (session vs live, quiescent + liveness)
# ---------------------------------------------------------------------------

EDGE_RULE = """
CREATE RULE edge AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b
"""


def test_locy_result_is_context_independent():
    db = _fresh_db()
    tx = db.session().tx()
    tx.execute("CREATE (:N {name: 'A'})-[:E]->(:N {name: 'B'})")
    tx.commit()

    # Initial read.
    assert len(db.session().locy(EDGE_RULE).derived.get("edge", [])) == 1

    # Commit a second edge; a fresh session must see it (liveness).
    tx2 = db.session().tx()
    tx2.execute("MATCH (b:N {name: 'B'}) CREATE (b)-[:E]->(:N {name: 'C'})")
    tx2.commit()
    assert len(db.session().locy(EDGE_RULE).derived.get("edge", [])) == 2

    # Determinism: repeated identical reads agree.
    a = len(db.session().locy(EDGE_RULE).derived.get("edge", []))
    b = len(db.session().locy(EDGE_RULE).derived.get("edge", []))
    assert a == b == 2


# ---------------------------------------------------------------------------
# REQ-2 — Rhai aggregate callable in Cypher
# ---------------------------------------------------------------------------

SSTDDEV_PLUGIN = """
fn uni_manifest() {
    #{
        id: "mcagg",
        version: "0.1.0",
        determinism: "pure",
        aggregate_fns: [
            #{ name: "sstddev", args: ["float"], returns: "float", state: "map" },
        ],
    }
}
fn sstddev_init() { #{ n: 0, sum: 0.0, sum_sq: 0.0 } }
fn sstddev_accumulate(state, x) {
    state.n += 1; state.sum += x; state.sum_sq += x * x; state
}
fn sstddev_merge(a, b) {
    #{ n: a.n + b.n, sum: a.sum + b.sum, sum_sq: a.sum_sq + b.sum_sq }
}
fn sstddev_finalize(s) {
    if s.n < 2 { return (); }
    let mean = s.sum / s.n;
    let variance = (s.sum_sq - s.sum * mean) / (s.n - 1);
    variance.sqrt()
}
"""


def test_rhai_aggregate_callable_in_cypher():
    db = _fresh_db()
    outcome = db.load_rhai_plugin(SSTDDEV_PLUGIN, grants=["AggregateFn"])
    assert len(outcome["aggregates_registered"]) == 1

    tx = db.session().tx()
    tx.execute(
        "CREATE (:X {v: 2}), (:X {v: 4}), (:X {v: 4}), (:X {v: 4}), "
        "(:X {v: 5}), (:X {v: 5}), (:X {v: 7}), (:X {v: 9})"
    )
    tx.commit()

    rows = db.session().query("MATCH (n:X) RETURN mcagg.sstddev(toFloat(n.v)) AS sd")
    assert math.isclose(rows[0]["sd"], 2.138089935299395, rel_tol=1e-6)


# ---------------------------------------------------------------------------
# REQ-3 — native stDev / variance in Cypher
# ---------------------------------------------------------------------------


def test_native_stdev_and_variance_in_cypher():
    db = _fresh_db()
    tx = db.session().tx()
    tx.execute(
        "CREATE (:X {v: 2}), (:X {v: 4}), (:X {v: 4}), (:X {v: 4}), "
        "(:X {v: 5}), (:X {v: 5}), (:X {v: 7}), (:X {v: 9})"
    )
    tx.commit()
    s = db.session()

    assert math.isclose(
        s.query("MATCH (n:X) RETURN stDev(n.v) AS x")[0]["x"], 2.138089935, rel_tol=1e-6
    )
    assert math.isclose(
        s.query("MATCH (n:X) RETURN stDevP(n.v) AS x")[0]["x"], 2.0, rel_tol=1e-9
    )
    assert math.isclose(
        s.query("MATCH (n:X) RETURN variance(n.v) AS x")[0]["x"],
        32.0 / 7.0,
        rel_tol=1e-6,
    )
    assert math.isclose(
        s.query("MATCH (n:X) RETURN varianceP(n.v) AS x")[0]["x"], 4.0, rel_tol=1e-9
    )


# ---------------------------------------------------------------------------
# REQ-4 — plugin scalar accepts a raw integer property (no toInteger)
# ---------------------------------------------------------------------------

IDF_PLUGIN = """
fn uni_manifest() {
    #{
        id: "mcscale",
        version: "0.1.0",
        determinism: "pure",
        scalar_fns: [
            #{ name: "idf", args: ["int"], returns: "int" },
        ],
    }
}
fn idf(x) { x + 1 }
"""


def test_plugin_scalar_accepts_raw_int_property():
    db = _fresh_db()
    db.load_rhai_plugin(IDF_PLUGIN, grants=["ScalarFn"])

    tx = db.session().tx()
    tx.execute("CREATE (:Doc {freq: 5})")
    tx.commit()

    # No toInteger(...) wrapper — the property is auto-coerced to the declared int.
    rows = db.session().query("MATCH (d:Doc) RETURN mcscale.idf(d.freq) AS y")
    assert rows[0]["y"] == 6

    # The explicit-coercion form still works.
    rows = db.session().query(
        "MATCH (d:Doc) RETURN mcscale.idf(toInteger(d.freq)) AS y"
    )
    assert rows[0]["y"] == 6


def test_plugin_scalar_non_numeric_property_errors_clearly():
    db = _fresh_db()
    db.load_rhai_plugin(IDF_PLUGIN, grants=["ScalarFn"])

    tx = db.session().tx()
    tx.execute("CREATE (:Doc {tag: 'hello'})")
    tx.commit()

    with pytest.raises(Exception) as ei:
        db.session().query("MATCH (d:Doc) RETURN mcscale.idf(d.tag) AS y")
    assert "toInteger" in str(ei.value)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
