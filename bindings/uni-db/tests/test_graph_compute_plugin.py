"""Python e2e for a guest-authored GraphCompute algorithm plugin (issue #150).

Ports the Rust reference test
`crates/uni/tests/common/loaders/rhai_graph_compute.rs` to the Python binding:
a Rhai plugin whose manifest declares an `algorithms:` entry is loaded with the
GraphCompute capability triple, invoked through Cypher `CALL`, and asserted to
match the built-in `uni.algo.gcpagerank` row-for-row.

Regression guard for #150 (guest GraphCompute algorithm uninvokable from Python)
plus a capability-enforcement check: without the grant, the algorithm does not
register and the `CALL` fails.
"""

import math
import os
import shutil
import sys
import tempfile
import unittest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import uni_db

# Guest Personalized PageRank over the GraphCompute coarse kernels, verbatim from
# the Rust reference test's PPR_SCRIPT.
PPR_SCRIPT = """
fn uni_manifest() {
    #{
        id: "ai.example.gc",
        version: "0.1.0",
        determinism: "pure",
        algorithms: [
            #{ name: "ppr", args: ["int"], yields: ["nodeId:int", "score:float"] },
        ],
    }
}

fn ppr(gc, source) {
    let alpha = 0.85;
    let g = gc.graph();
    let seed_set = gc.frontier(g, [source]);
    let seed_map = gc.set_to_map(seed_set, 1.0);
    let teleport = gc.normalize(seed_map, "l1");
    gc.free(seed_map);
    gc.free(seed_set);

    let deg = gc.degrees(g, "out");
    let inv_deg = gc.recip(deg);
    let dangling = gc.map_to_set(deg, "is_zero", 0.0);
    gc.free(deg);

    let rank = gc.scale(teleport, 1.0);
    for i in 0..100 {
        let contrib = gc.ewise(rank, inv_deg, "mul", 0.0);
        let spread = gc.spmv(g, contrib, "linear_algebra", "out");
        gc.free(contrib);
        let dm = gc.reduce_sum_masked(rank, dangling);
        let scaled = gc.scale(spread, alpha);
        gc.free(spread);
        let blend = 1.0 - alpha + alpha * dm;
        let next = gc.ewise(scaled, teleport, "axpy", blend);
        gc.free(scaled);
        let diff = gc.l1_diff(rank, next);
        gc.free(rank);
        rank = next;
        if diff < 0.000000001 { break; }
    }

    gc.free(teleport);
    gc.free(inv_deg);
    gc.free(dangling);
    gc.emit("score", rank);
}
"""

# The three orthogonal capabilities a GraphCompute algorithm needs.
GC_GRANTS = ["Algorithm", "GraphCompute", "HostQuery"]


def _build_graph(db):
    """Create the reference graph and return `id(A)`.

    Four `:Node` A/B/C/D with `:LINKS` edges A->B, B->C, C->A, A->D. The schema
    (label + edge type) is applied first: the GraphCompute projection builds its
    adjacency from the registered edge type, so a schemaless graph projects empty.
    """
    db.schema().label("Node").property("name", "string").done().edge_type(
        "LINKS", ["Node"], ["Node"]
    ).done().apply()
    session = db.session()
    tx = session.tx()
    tx.execute(
        "CREATE (a:Node {name: 'A'}), (b:Node {name: 'B'}), "
        "(c:Node {name: 'C'}), (d:Node {name: 'D'})"
    )
    for src, dst in (("A", "B"), ("B", "C"), ("C", "A"), ("A", "D")):
        tx.execute(
            "MATCH (a:Node {name: $s}), (b:Node {name: $d}) CREATE (a)-[:LINKS]->(b)",
            {"s": src, "d": dst},
        )
    tx.commit()
    rows = session.query("MATCH (n:Node {name: 'A'}) RETURN id(n) AS vid")
    return int(rows[0]["vid"])


def _scores_by_node(session, call):
    """Run a `CALL ... YIELD nodeId, score` and return {nodeId: score}."""
    rows = session.query(f"{call} YIELD nodeId, score RETURN nodeId, score")
    return {int(r["nodeId"]): float(r["score"]) for r in rows}


class TestGraphComputePlugin(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="test_gc_plugin_")
        self.db = uni_db.Uni.open(self.test_dir)
        self.vid_a = _build_graph(self.db)

    def tearDown(self):
        del self.db
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_guest_ppr_registers_and_matches_native(self):
        outcome = self.db.load_rhai_plugin(PPR_SCRIPT, grants=GC_GRANTS)
        self.assertEqual(outcome["plugin_id"], "ai.example.gc")

        session = self.db.session()
        guest = _scores_by_node(
            session,
            f"CALL ai.example.gc.ppr({self.vid_a}, {{nodeLabels: ['Node'], edgeTypes: ['LINKS']}})",
        )
        native = _scores_by_node(
            session,
            f"CALL uni.algo.gcpagerank({self.vid_a}, 0.85, {{nodeLabels: ['Node'], edgeTypes: ['LINKS']}})",
        )

        # One score row per vertex; probability mass sums to 1.
        self.assertEqual(len(guest), 4)
        self.assertAlmostEqual(sum(guest.values()), 1.0, delta=1e-6)
        for score in guest.values():
            self.assertTrue(math.isfinite(score) and score >= 0.0)

        # Parity with the native GraphCompute PageRank, node-for-node.
        self.assertEqual(set(guest), set(native))
        for node_id, score in guest.items():
            self.assertAlmostEqual(score, native[node_id], delta=1e-9)

    def test_guest_algorithm_requires_capability(self):
        # Load the same plugin WITHOUT the Algorithm/GraphCompute grants: the
        # manifest still declares the algorithm, but the registration gate is not
        # satisfied, so `CALL` must not resolve. This locks in that the capability
        # actually gates behavior (the heart of #150).
        self.db.load_rhai_plugin(PPR_SCRIPT, grants=["ScalarFn"])
        session = self.db.session()
        with self.assertRaises(Exception):
            session.query(
                f"CALL ai.example.gc.ppr({self.vid_a}, {{nodeLabels: ['Node'], edgeTypes: ['LINKS']}}) YIELD nodeId, score "
                "RETURN nodeId"
            )


if __name__ == "__main__":
    unittest.main()
