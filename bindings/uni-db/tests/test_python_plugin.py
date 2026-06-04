"""M8 F2 — Python e2e for the PyO3 plugin decorator surface.

Tests the `@session.scalar_fn(...)` / `@session.aggregate_fn(...)` /
`@session.procedure(...)` decorators and the source-string
`session.load_python_plugin(...)` path, both registering plugins
session-scoped per proposal §5.4.2.
"""

import os
import shutil
import sys
import tempfile
import unittest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import uni_db

PY_SCALAR_SRC = """
db.set_plugin_id("ai.example.pyscore")
db.set_version("0.1.0")

@db.scalar_fn("score", args=["float","float"], returns="float", determinism="pure")
def score(x, y):
    return x * 0.7 + y * 0.3
"""


class TestPythonPluginSourceLoad(unittest.TestCase):
    """source-string `session.load_python_plugin(...)` path."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="test_pyo3_")
        self.db = uni_db.Uni.open(self.test_dir)

    def tearDown(self):
        del self.db
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_load_python_plugin_returns_metadata(self):
        sess = self.db.session()
        outcome = sess.load_python_plugin(PY_SCALAR_SRC, "ai.example.pyscore")
        self.assertEqual(outcome["plugin_id"], "ai.example.pyscore")
        self.assertEqual(outcome["version"], "0.1.0")
        self.assertEqual(len(outcome["scalars_registered"]), 1)
        self.assertIn(
            "ai.example.pyscore.score",
            outcome["scalars_registered"],
        )
        self.assertEqual(outcome["aggregates_registered"], [])
        self.assertEqual(outcome["procedures_registered"], [])

    def test_load_python_plugin_invokable_through_cypher(self):
        sess = self.db.session()
        sess.load_python_plugin(PY_SCALAR_SRC, "ai.example.pyscore")
        result = sess.query("RETURN score(1.0, 2.0) AS s")
        rows = list(result)
        self.assertEqual(len(rows), 1)
        # 1.0*0.7 + 2.0*0.3 = 1.3
        self.assertAlmostEqual(rows[0]["s"], 1.3, places=9)

    def test_load_python_plugin_rejects_bad_module(self):
        sess = self.db.session()
        with self.assertRaises(Exception):
            sess.load_python_plugin("this is @@@ not valid python", "ai.example.bad")


class TestPythonPluginDecorator(unittest.TestCase):
    """`@session.scalar_fn(...)` decorator + `finalize_plugin(...)` path."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="test_pyo3_dec_")
        self.db = uni_db.Uni.open(self.test_dir)

    def tearDown(self):
        del self.db
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_scalar_fn_decorator_registers_and_invokes(self):
        sess = self.db.session()

        @sess.scalar_fn("score", args=["float", "float"], returns="float")
        def score(x, y):
            return x * 0.7 + y * 0.3

        outcome = sess.finalize_plugin("ai.example.dec", version="0.1.0")
        self.assertEqual(outcome["plugin_id"], "ai.example.dec")
        self.assertIn("ai.example.dec.score", outcome["scalars_registered"])

        # User function survives decoration — still directly callable.
        self.assertAlmostEqual(score(1.0, 2.0), 1.3, places=9)

        # And the scalar is invokable through Cypher.
        result = sess.query("RETURN score(2.0, 3.0) AS s")
        rows = list(result)
        # 2.0*0.7 + 3.0*0.3 = 2.3
        self.assertAlmostEqual(rows[0]["s"], 2.3, places=9)

    def test_finalize_plugin_with_no_decorators_errors(self):
        sess = self.db.session()
        with self.assertRaises(ValueError):
            sess.finalize_plugin("ai.example.empty")

    def test_multiple_scalars_in_one_finalize(self):
        sess = self.db.session()

        @sess.scalar_fn("add", args=["float", "float"], returns="float")
        def add(x, y):
            return x + y

        @sess.scalar_fn("mul", args=["float", "float"], returns="float")
        def mul(x, y):
            return x * y

        outcome = sess.finalize_plugin("ai.example.multi", version="0.1.0")
        self.assertEqual(len(outcome["scalars_registered"]), 2)
        names = set(outcome["scalars_registered"])
        self.assertIn("ai.example.multi.add", names)
        self.assertIn("ai.example.multi.mul", names)


class TestPythonPluginSessionIsolation(unittest.TestCase):
    """Proposal §5.4.2: session-scoped plugins are invisible across sessions."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="test_pyo3_iso_")
        self.db = uni_db.Uni.open(self.test_dir)

    def tearDown(self):
        del self.db
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_session_scoped_plugin_invisible_to_other_session(self):
        sess_a = self.db.session()
        sess_b = self.db.session()

        @sess_a.scalar_fn("greet", args=["string"], returns="string")
        def greet(name):
            return f"hello {name}"

        sess_a.finalize_plugin("ai.example.iso")

        # Session A sees it.
        result_a = sess_a.query("RETURN greet('alice') AS s")
        rows_a = list(result_a)
        self.assertEqual(rows_a[0]["s"], "hello alice")

        # Session B does NOT see it — query must fail.
        with self.assertRaises(Exception):
            list(sess_b.query("RETURN greet('bob') AS s"))


if __name__ == "__main__":
    unittest.main()
