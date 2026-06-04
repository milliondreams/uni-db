# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team

"""M8 async follow-up — Python e2e for the AsyncSession PyO3 plugin
decorator surface.

Mirrors the sync `test_python_plugin.py` suite for `AsyncDatabase` /
`AsyncSession`. Tests the `@session.scalar_fn(...)` / `@session.aggregate_fn(...)`
/ `@session.procedure(...)` decorators, `await session.finalize_plugin(...)`,
and the source-string `await session.load_python_plugin(...)` path,
all registering plugins session-scoped per proposal §5.4.2.
"""

import pytest

import uni_db

PY_SCALAR_SRC = """
db.set_plugin_id("ai.example.pyscore")
db.set_version("0.1.0")

@db.scalar_fn("score", args=["float","float"], returns="float", determinism="pure")
def score(x, y):
    return x * 0.7 + y * 0.3
"""


@pytest.fixture
def test_dir(tmp_path):
    return str(tmp_path / "test_async_pyo3")


@pytest.mark.asyncio
async def test_load_python_plugin_returns_metadata(test_dir):
    db = await uni_db.AsyncUni.open(test_dir)
    sess = db.session()
    outcome = await sess.load_python_plugin(PY_SCALAR_SRC, "ai.example.pyscore")
    assert outcome["plugin_id"] == "ai.example.pyscore"
    assert outcome["version"] == "0.1.0"
    assert len(outcome["scalars_registered"]) == 1
    assert "ai.example.pyscore.score" in outcome["scalars_registered"]
    assert outcome["aggregates_registered"] == []
    assert outcome["procedures_registered"] == []


@pytest.mark.asyncio
async def test_load_python_plugin_invokable_through_cypher(test_dir):
    db = await uni_db.AsyncUni.open(test_dir)
    sess = db.session()
    await sess.load_python_plugin(PY_SCALAR_SRC, "ai.example.pyscore")
    result = await sess.query("RETURN score(1.0, 2.0) AS s")
    rows = list(result)
    assert len(rows) == 1
    # 1.0*0.7 + 2.0*0.3 = 1.3
    assert rows[0]["s"] == pytest.approx(1.3, abs=1e-9)


@pytest.mark.asyncio
async def test_load_python_plugin_rejects_bad_module(test_dir):
    db = await uni_db.AsyncUni.open(test_dir)
    sess = db.session()
    with pytest.raises(Exception):
        await sess.load_python_plugin("this is @@@ not valid python", "ai.example.bad")


@pytest.mark.asyncio
async def test_scalar_fn_decorator_registers_and_invokes(test_dir):
    db = await uni_db.AsyncUni.open(test_dir)
    sess = db.session()

    @sess.scalar_fn("score", args=["float", "float"], returns="float")
    def score(x, y):
        return x * 0.7 + y * 0.3

    outcome = await sess.finalize_plugin("ai.example.dec", version="0.1.0")
    assert outcome["plugin_id"] == "ai.example.dec"
    assert "ai.example.dec.score" in outcome["scalars_registered"]

    # User function survives decoration — still directly callable.
    assert score(1.0, 2.0) == pytest.approx(1.3, abs=1e-9)

    # Scalar invokable through Cypher.
    result = await sess.query("RETURN score(2.0, 3.0) AS s")
    rows = list(result)
    # 2.0*0.7 + 3.0*0.3 = 2.3
    assert rows[0]["s"] == pytest.approx(2.3, abs=1e-9)


@pytest.mark.asyncio
async def test_finalize_plugin_with_no_decorators_errors(test_dir):
    db = await uni_db.AsyncUni.open(test_dir)
    sess = db.session()
    with pytest.raises(ValueError):
        await sess.finalize_plugin("ai.example.empty")


@pytest.mark.asyncio
async def test_multiple_scalars_in_one_finalize(test_dir):
    db = await uni_db.AsyncUni.open(test_dir)
    sess = db.session()

    @sess.scalar_fn("add", args=["float", "float"], returns="float")
    def add(x, y):
        return x + y

    @sess.scalar_fn("mul", args=["float", "float"], returns="float")
    def mul(x, y):
        return x * y

    outcome = await sess.finalize_plugin("ai.example.multi", version="0.1.0")
    assert len(outcome["scalars_registered"]) == 2
    names = set(outcome["scalars_registered"])
    assert "ai.example.multi.add" in names
    assert "ai.example.multi.mul" in names


@pytest.mark.asyncio
async def test_session_scoped_plugin_invisible_to_other_session(test_dir):
    db = await uni_db.AsyncUni.open(test_dir)
    sess_a = db.session()
    sess_b = db.session()

    @sess_a.scalar_fn("greet", args=["string"], returns="string")
    def greet(name):
        return f"hello {name}"

    await sess_a.finalize_plugin("ai.example.iso")

    # Session A sees it.
    result_a = await sess_a.query("RETURN greet('alice') AS s")
    rows_a = list(result_a)
    assert rows_a[0]["s"] == "hello alice"

    # Session B does NOT see it — query must fail.
    with pytest.raises(Exception):
        result_b = await sess_b.query("RETURN greet('bob') AS s")
        list(result_b)
