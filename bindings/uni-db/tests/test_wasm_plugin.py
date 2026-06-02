"""Python e2e for the WASM plugin loaders (Gap A).

Mirrors `test_rhai_plugin.py` for `db.load_wasm_component` /
`db.load_wasm_extism`. Loads the prebuilt `example-wasm-geo` /
`example-extism-geo` artifacts; skips cleanly when they are absent (they
are produced by `scripts/build-wasm-fixtures.sh`, not on every checkout).
"""

import os
import shutil
import sys
import tempfile
import unittest

import pytest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import uni_db

_REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
_CM_WASM = os.path.join(
    _REPO_ROOT,
    "examples/example-wasm-geo/target/wasm32-wasip2/release/example_wasm_geo.wasm",
)
_EXTISM_WASM = os.path.join(
    _REPO_ROOT,
    "examples/example-extism-geo/target/wasm32-unknown-unknown/release/example_extism_geo.wasm",
)


class TestWasmPlugin(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="test_wasm_")
        self.db = uni_db.Uni.open(self.test_dir)

    def tearDown(self):
        del self.db
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_wasm_methods_exist(self):
        self.assertTrue(hasattr(self.db, "load_wasm_component"))
        self.assertTrue(hasattr(self.db, "load_wasm_extism"))

    @unittest.skipUnless(os.path.exists(_CM_WASM), f"missing fixture: {_CM_WASM}")
    def test_load_wasm_component(self):
        with open(_CM_WASM, "rb") as f:
            outcome = self.db.load_wasm_component(f.read(), grants=["ScalarFn"])
        self.assertTrue(outcome["plugin_id"])
        self.assertGreaterEqual(len(outcome["scalars_registered"]), 1)
        # The geo example registers a haversine scalar.
        self.assertTrue(
            any("haversine" in q for q in outcome["scalars_registered"]),
            outcome["scalars_registered"],
        )

    @unittest.skipUnless(
        os.path.exists(_EXTISM_WASM), f"missing fixture: {_EXTISM_WASM}"
    )
    def test_load_wasm_extism(self):
        with open(_EXTISM_WASM, "rb") as f:
            outcome = self.db.load_wasm_extism(f.read(), grants=["ScalarFn"])
        self.assertTrue(outcome["plugin_id"])
        self.assertGreaterEqual(len(outcome["scalars_registered"]), 1)


@pytest.mark.asyncio
async def test_async_load_wasm_component():
    """`await AsyncUni.load_wasm_component(...)` — does not block the loop."""
    if not os.path.exists(_CM_WASM):
        pytest.skip(f"missing fixture: {_CM_WASM}")
    db = await uni_db.AsyncUni.temporary()
    with open(_CM_WASM, "rb") as f:
        outcome = await db.load_wasm_component(f.read(), grants=["ScalarFn"])
    assert outcome["plugin_id"]
    assert len(outcome["scalars_registered"]) >= 1
    assert any("haversine" in q for q in outcome["scalars_registered"])


@pytest.mark.asyncio
async def test_async_load_wasm_extism():
    if not os.path.exists(_EXTISM_WASM):
        pytest.skip(f"missing fixture: {_EXTISM_WASM}")
    db = await uni_db.AsyncUni.temporary()
    with open(_EXTISM_WASM, "rb") as f:
        outcome = await db.load_wasm_extism(f.read(), grants=["ScalarFn"])
    assert outcome["plugin_id"]
    assert len(outcome["scalars_registered"]) >= 1


if __name__ == "__main__":
    unittest.main()
