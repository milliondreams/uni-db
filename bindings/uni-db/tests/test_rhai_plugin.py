"""M7 Phase 9 — Python e2e for db.load_rhai_plugin.

Loads a tiny Rhai plugin via the PyO3 binding and asserts the returned
metadata dict carries the expected shape. The actual scalar-invoke
path is exercised in Rust tests (m7_rhai_load_e2e); this test confirms
the Python surface is wired and round-trips through PyO3 cleanly.
"""

import os
import shutil
import sys
import tempfile
import unittest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import uni_db

SCRIPT = """
fn uni_manifest() {
    #{
        id: "ai.example.score",
        version: "0.1.0",
        determinism: "pure",
        scalar_fns: [
            #{ name: "score", args: ["float","float"], returns: "float" },
        ],
    }
}
fn score(x, y) { x * 0.7 + y * 0.3 }
"""


class TestRhaiPlugin(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="test_rhai_")
        self.db = uni_db.Uni.open(self.test_dir)

    def tearDown(self):
        del self.db
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_load_rhai_plugin_returns_metadata(self):
        outcome = self.db.load_rhai_plugin(SCRIPT)
        self.assertEqual(outcome["plugin_id"], "ai.example.score")
        self.assertEqual(outcome["version"], "0.1.0")
        self.assertEqual(len(outcome["scalars_registered"]), 1)
        self.assertIn("ai.example.score.score", outcome["scalars_registered"])
        self.assertEqual(outcome["aggregates_registered"], [])
        self.assertEqual(outcome["procedures_registered"], [])

    def test_load_rhai_plugin_rejects_bad_grant(self):
        with self.assertRaises(ValueError):
            self.db.load_rhai_plugin(SCRIPT, grants=["NotARealCapability"])

    def test_load_rhai_plugin_explicit_grants(self):
        outcome = self.db.load_rhai_plugin(
            SCRIPT,
            grants=["ScalarFn"],
        )
        self.assertEqual(outcome["plugin_id"], "ai.example.score")

    def test_load_rhai_plugin_rejects_bad_script(self):
        with self.assertRaises(Exception):
            self.db.load_rhai_plugin("@@@ this is not rhai @@@")


if __name__ == "__main__":
    unittest.main()
