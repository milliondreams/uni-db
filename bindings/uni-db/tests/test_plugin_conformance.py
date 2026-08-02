"""Cross-loader plugin conformance suite for the ``uni-db`` Python binding.

Issue #150 exposed that the guest-algorithm + host-fn + capability-enforcement
surface was Rust-tested but never exercised through the binding people ship on;
the pre-existing Python plugin tests were a ScalarFn-only smoke layer. This
suite closes that hole (proposal ``docs/proposals/python_plugin_abi_gaps_2026-07-16.md``
§5 WS-4): every loader (rhai / pyo3 / wasm-component / extism) is driven through
each extension kind it supports, and every case pairs the *granted* behavior with
a *denied* one so the tests prove the capability actually gates behavior — the
heart of #150.

What each row asserts:

- **Registration channel** — the loader's ``LoadOutcome`` dict reports the
  registered qnames in the right bucket (``scalars_registered`` /
  ``aggregates_registered`` / ``procedures_registered`` / ``algorithms_registered``,
  the P2 channel).
- **Invocation** — where the runtime supports it, the entity is callable through
  Cypher and (for graph algorithms) matches the native ``uni.algo.gcpagerank``
  provider row-for-row (the guest-authorable thesis of #150).
- **Enforcement** — loading without the gating grant changes behavior: the entity
  does not register and/or the call fails, or (for host-fn-importing WASM/Extism
  guests) the load itself fails at link time.

Fixture-dependent rows (WASM/Extism) skip cleanly when the build artifacts under
``examples/example-*/target`` are absent — build them with
``scripts/build-wasm-fixtures.sh``. This mirrors ``test_wasm_plugin.py``.
"""

import math
import os
import shutil
import sys
import tempfile

import pytest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import uni_db

# --------------------------------------------------------------------------- #
# Fixtures & helpers
# --------------------------------------------------------------------------- #

_REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)


def _fixture(rel):
    return os.path.join(_REPO_ROOT, rel)


# Built WASM/Extism artifacts (gitignored; built by scripts/build-wasm-fixtures.sh).
# The WASM/Extism loader methods are `#[cfg]`-gated on the `wasm-plugins` /
# `extism-plugins` features, which the published wheel drops for size (they
# remain available for source builds). A build without them is legitimate, so
# these rows skip rather than fail — the same contract as the missing-fixture
# skips they sit beside.
def _loader_absent(method: str) -> bool:
    """Whether `method` was compiled out of this build."""
    return not hasattr(uni_db._uni_db.Uni, method)


def _row_unavailable(method: str, fixture: str) -> bool:
    """Skip predicate: fixture not built, or loader not compiled in."""
    return not os.path.exists(fixture) or _loader_absent(method)


WASM_GEO = _fixture(
    "examples/example-wasm-geo/target/wasm32-wasip2/release/example_wasm_geo.wasm"
)
EXT_GEO = _fixture(
    "examples/example-extism-geo/target/wasm32-unknown-unknown/release/example_extism_geo.wasm"
)
WASM_GRAPH = _fixture(
    "examples/example-wasm-graph/target/wasm32-wasip2/release/example_wasm_graph.wasm"
)
EXT_GRAPH = _fixture(
    "examples/example-extism-graph/target/wasm32-unknown-unknown/release/example_extism_graph.wasm"
)
WASM_NET = _fixture(
    "examples/example-wasm-net/target/wasm32-wasip2/release/example_wasm_net.wasm"
)
EXT_NET = _fixture(
    "examples/example-extism-net/target/wasm32-unknown-unknown/release/example_extism_net.wasm"
)

# The three orthogonal capabilities a GraphCompute algorithm needs (see #150).
GC_GRANTS = ["Algorithm", "GraphCompute", "HostQuery"]


@pytest.fixture
def db():
    """A fresh on-disk database, torn down after the test."""
    path = tempfile.mkdtemp(prefix="test_conformance_")
    database = uni_db.Uni.open(path)
    try:
        yield database
    finally:
        del database
        shutil.rmtree(path, ignore_errors=True)


def _read(path):
    with open(path, "rb") as handle:
        return handle.read()


def _build_pagerank_graph(database):
    """Create the reference 4-node graph and return ``id(A)``.

    ``:Node`` A/B/C/D with ``:LINKS`` A->B, B->C, C->A, A->D. The schema is
    applied first: the GraphCompute projection builds adjacency from the
    registered edge type, so a schemaless graph would project empty.
    """
    database.schema().label("Node").property("name", "string").done().edge_type(
        "LINKS", ["Node"], ["Node"]
    ).done().apply()
    session = database.session()
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
    rows = session.query(f"{call} YIELD nodeId, score RETURN nodeId, score")
    return {int(r["nodeId"]): float(r["score"]) for r in rows}


def _build_weighted_graph(database):
    """Issue #151 fixture: 3 ``:A`` + 1 ``:B``, 3 ``:LINK`` edges each ``w = 5.0``.

    All three edges run among the ``:A`` nodes (a0->a1, a1->a2, a2->a0), so a
    projection scoped to ``:A`` keeps every edge. Returns ``id(a0)``.
    """
    database.schema().label("A").property("name", "string").done().label("B").property(
        "name", "string"
    ).done().edge_type("LINK", ["A", "B"], ["A", "B"]).property(
        "w", "float"
    ).done().apply()
    session = database.session()
    tx = session.tx()
    tx.execute(
        "CREATE (:A {name:'a0'}),(:A {name:'a1'}),(:A {name:'a2'}),(:B {name:'b0'})"
    )
    for src, dst in (("a0", "a1"), ("a1", "a2"), ("a2", "a0")):
        tx.execute(
            "MATCH (x {name:$s}),(y {name:$d}) CREATE (x)-[:LINK {w: 5.0}]->(y)",
            {"s": src, "d": dst},
        )
    tx.commit()
    # The graph projection reads edge/vertex *property values* from committed
    # storage (it is a storage-time analytics view, not a serializable read), so
    # a weightProperty projection sees L0-buffered weights only after a flush.
    # This matches the native uni.algo.* weighted path, which shares the same
    # ProjectionBuilder. (L0 property-value visibility is a separate, broader gap.)
    database.flush()
    rows = session.query("MATCH (n:A {name:'a0'}) RETURN id(n) AS vid")
    return int(rows[0]["vid"])


def _probe_rows(session, call):
    return session.query(
        f"{call} YIELD nodeId, total_edge_weight RETURN nodeId, total_edge_weight"
    )


def _loader(database, method):
    """Resolve a loader method (``load_wasm_component`` / ``load_wasm_extism``)."""
    return getattr(database, method)


# --------------------------------------------------------------------------- #
# Built-in GraphCompute algorithms are callable from Python
# --------------------------------------------------------------------------- #


def test_builtin_gcpagerank_callable_from_python(db):
    """#150's premise: the native GraphCompute PageRank is CALL-able from Python."""
    vid_a = _build_pagerank_graph(db)
    scores = _scores_by_node(
        db.session(),
        f"CALL uni.algo.gcpagerank({vid_a}, 0.85, {{nodeLabels: ['Node'], edgeTypes: ['LINKS']}})",
    )
    assert len(scores) == 4
    assert math.isclose(sum(scores.values()), 1.0, abs_tol=1e-6)
    assert all(math.isfinite(s) and s >= 0.0 for s in scores.values())


# --------------------------------------------------------------------------- #
# Rhai loader — scalar + algorithm, granted vs denied
# --------------------------------------------------------------------------- #

RHAI_SCALAR = """
fn uni_manifest() {
    #{
        id: "ai.example.rhaiscore", version: "0.1.0", determinism: "pure",
        scalar_fns: [ #{ name: "score", args: ["float","float"], returns: "float" } ],
    }
}
fn score(x, y) { x * 0.7 + y * 0.3 }
"""

# Guest Personalized PageRank over the GraphCompute coarse kernels (verbatim from
# the Rust reference test crates/uni/tests/common/loaders/rhai_graph_compute.rs).
RHAI_PPR = """
fn uni_manifest() {
    #{
        id: "ai.example.rhaigc", version: "0.1.0", determinism: "pure",
        algorithms: [ #{ name: "ppr", args: ["int"], yields: ["nodeId:int", "score:float"] } ],
    }
}
fn ppr(gc, source) {
    let alpha = 0.85;
    let g = gc.graph();
    let seed_set = gc.frontier(g, [source]);
    let seed_map = gc.set_to_map(seed_set, 1.0);
    let teleport = gc.normalize(seed_map, "l1");
    gc.free(seed_map); gc.free(seed_set);
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
    gc.free(teleport); gc.free(inv_deg); gc.free(dangling);
    gc.emit("score", rank);
}
"""


def test_rhai_scalar_granted_registers_and_invokes(db):
    outcome = db.load_rhai_plugin(RHAI_SCALAR, grants=["ScalarFn"])
    assert "ai.example.rhaiscore.score" in outcome["scalars_registered"]
    rows = db.session().query("RETURN score(1.0, 2.0) AS s")
    assert math.isclose(float(rows[0]["s"]), 1.3, abs_tol=1e-9)


def test_rhai_scalar_denied_does_not_register(db):
    # Withhold ScalarFn (grant only Procedure): the scalar must not register and
    # the call must fail.
    outcome = db.load_rhai_plugin(RHAI_SCALAR, grants=["Procedure"])
    assert outcome["scalars_registered"] == []
    with pytest.raises(Exception):
        db.session().query("RETURN score(1.0, 2.0) AS s")


def test_rhai_algorithm_granted_matches_native(db):
    vid_a = _build_pagerank_graph(db)
    outcome = db.load_rhai_plugin(RHAI_PPR, grants=GC_GRANTS)
    assert "ai.example.rhaigc.ppr" in outcome["algorithms_registered"]

    session = db.session()
    guest = _scores_by_node(
        session,
        f"CALL ai.example.rhaigc.ppr({vid_a}, {{nodeLabels: ['Node'], edgeTypes: ['LINKS']}})",
    )
    native = _scores_by_node(
        session,
        f"CALL uni.algo.gcpagerank({vid_a}, 0.85, {{nodeLabels: ['Node'], edgeTypes: ['LINKS']}})",
    )
    assert set(guest) == set(native)
    for node_id, score in guest.items():
        assert math.isclose(score, native[node_id], abs_tol=1e-9)


def test_rhai_algorithm_denied_does_not_register(db):
    vid_a = _build_pagerank_graph(db)
    outcome = db.load_rhai_plugin(RHAI_PPR, grants=["ScalarFn"])
    assert outcome["algorithms_registered"] == []
    with pytest.raises(Exception):
        db.session().query(
            f"CALL ai.example.rhaigc.ppr({vid_a}, {{nodeLabels: ['Node'], edgeTypes: ['LINKS']}}) YIELD nodeId, score RETURN nodeId"
        )


# --------------------------------------------------------------------------- #
# Projection shaping (issue #151): a guest scopes its projection to chosen
# labels/edge-types and binds an edge property as the traversal weight, via a
# trailing config object on the CALL — the same knobs native uni.algo.* accepts.
#
# The fail-closed scope invariant (an unscoped projection under a *narrow*
# HostQuery grant is rejected, 0x804) is exercised in Rust
# (crates/uni/tests/common/graph_algo/graph_compute_pagerank.rs) because the
# Python grant string only yields the unrestricted "**" scope — narrow scopes
# arise from host-side attenuation, which the string API can't express.
# --------------------------------------------------------------------------- #

# Sums gc.edge_weights over the projected graph and broadcasts the scalar to one
# row per projected vertex (verbatim from the issue #151 standalone repro).
RHAI_EDGE_PROBE = """
fn uni_manifest() {
    #{ id: "ai.example.gcprobe", version: "0.1.0", determinism: "pure",
       algorithms: [ #{ name: "probe", args: [], yields: ["nodeId:int", "total_edge_weight:float"] } ] }
}
fn probe(gc) {
    let g = gc.graph();
    let ew = gc.edge_weights(g);
    let total = gc.reduce_sum(ew);
    gc.free(ew);
    let deg = gc.degrees(g, "out");
    let all = gc.map_to_set(deg, "gt", -1.0);
    gc.free(deg);
    let out = gc.set_to_map(all, total);
    gc.emit("total_edge_weight", out);
}
"""


def test_rhai_projection_scopes_to_labels(db):
    """OBS1 (#151): a guest scopes its projection to chosen labels via nodeLabels."""
    _build_weighted_graph(db)
    db.load_rhai_plugin(RHAI_EDGE_PROBE, grants=GC_GRANTS)
    session = db.session()
    # Whole-graph now requires the explicit `projectAll` opt-in (G9): an
    # unscoped projection fails loud rather than silently taking everything.
    everything = _probe_rows(
        session, "CALL ai.example.gcprobe.probe({projectAll: true})"
    )
    assert len(everything) == 4
    # Scoped to :A projects only the 3 :A nodes.
    scoped = _probe_rows(session, 'CALL ai.example.gcprobe.probe({nodeLabels: ["A"]})')
    assert len(scoped) == 3


def test_rhai_projection_binds_edge_weight_property(db):
    """OBS2 (#151): weightProperty makes edge_weights read the numeric property."""
    _build_weighted_graph(db)
    db.load_rhai_plugin(RHAI_EDGE_PROBE, grants=GC_GRANTS)
    session = db.session()
    # Default: unit weights -> reduce_sum = edge count = 3.0.
    unweighted = _probe_rows(
        session, "CALL ai.example.gcprobe.probe({projectAll: true})"
    )
    assert math.isclose(float(unweighted[0]["total_edge_weight"]), 3.0, abs_tol=1e-9)
    # weightProperty="w": reduce_sum = 3 edges * 5.0 = 15.0.
    weighted = _probe_rows(
        session,
        'CALL ai.example.gcprobe.probe({projectAll: true, weightProperty: "w"})',
    )
    assert math.isclose(float(weighted[0]["total_edge_weight"]), 15.0, abs_tol=1e-9)


def test_native_gcpagerank_honors_projection_config(db):
    """The provider.rs shared-parser refactor: native gcpagerank honors nodeLabels."""
    vid_a = _build_weighted_graph(db)
    session = db.session()
    scoped = _scores_by_node(
        session, f'CALL uni.algo.gcpagerank({vid_a}, 0.85, {{nodeLabels: ["A"]}})'
    )
    # Scoped to :A -> only the 3 :A nodes appear in the result.
    assert len(scoped) == 3


# --------------------------------------------------------------------------- #
# Property tensors (issue #151 P2): a guest reads a named node/edge property
# into a per-vertex / per-edge tensor via gc.node_property / gc.edge_property,
# declared through nodeProperties / edgeProperties on the projection config.
# --------------------------------------------------------------------------- #

# Sums a node property and an edge property into one scalar broadcast per vertex.
RHAI_PROP_PROBE = """
fn uni_manifest() {
    #{ id: "ai.example.gcprop", version: "0.1.0", determinism: "pure",
       algorithms: [ #{ name: "sums", args: [], yields: ["nodeId:int", "total:float"] } ] }
}
fn sums(gc) {
    let g = gc.graph();
    let ep = gc.edge_property(g, "w");        // [E] tensor of edge property w
    let es = gc.reduce_sum(ep);
    gc.free(ep);
    let np = gc.node_property(g, "score");    // [V] tensor of node property score
    let ns = gc.reduce_sum(np);
    gc.free(np);
    let deg = gc.degrees(g, "out");
    let all = gc.map_to_set(deg, "gt", -1.0);
    gc.free(deg);
    let out = gc.set_to_map(all, es + ns);
    gc.emit("total", out);
}
"""


def _build_property_graph(database):
    """3 ``:A`` nodes with a numeric ``score`` + 3 ``:LINK`` edges with ``w = 5.0``.

    ``score`` = 1.0/2.0/3.0 (sum 6.0); ``w`` sum over 3 edges = 15.0. Flushed so
    the projection reads the property values from committed storage.
    """
    database.schema().label("A").property("name", "string").property(
        "score", "float"
    ).done().edge_type("LINK", ["A"], ["A"]).property("w", "float").done().apply()
    session = database.session()
    tx = session.tx()
    tx.execute(
        "CREATE (:A {name:'a0', score:1.0}),(:A {name:'a1', score:2.0}),"
        "(:A {name:'a2', score:3.0})"
    )
    for src, dst in (("a0", "a1"), ("a1", "a2"), ("a2", "a0")):
        tx.execute(
            "MATCH (x:A {name:$s}),(y:A {name:$d}) CREATE (x)-[:LINK {w:5.0}]->(y)",
            {"s": src, "d": dst},
        )
    tx.commit()
    database.flush()


def test_rhai_node_and_edge_property_tensors(db):
    """P2 (#151): guest reads node + edge properties into per-vertex/-edge tensors."""
    _build_property_graph(db)
    db.load_rhai_plugin(RHAI_PROP_PROBE, grants=GC_GRANTS)
    rows = db.session().query(
        'CALL ai.example.gcprop.sums({nodeLabels:["A"], edgeTypes:["LINK"], '
        'nodeProperties:["score"], edgeProperties:["w"]}) '
        "YIELD nodeId, total RETURN nodeId, total"
    )
    assert len(rows) == 3
    # edge_property(w) sum = 3*5 = 15; node_property(score) sum = 1+2+3 = 6; total 21.
    assert math.isclose(float(rows[0]["total"]), 21.0, abs_tol=1e-9)


def test_rhai_property_tensor_not_projected_errors(db):
    """A guest that reads a property it never declared (edgeProperties/nodeProperties) fails."""
    _build_property_graph(db)
    db.load_rhai_plugin(RHAI_PROP_PROBE, grants=GC_GRANTS)
    with pytest.raises(Exception):
        # Scoped, but with no nodeProperties/edgeProperties -> edge_property("w")
        # errors. The scope is supplied so the failure is the undeclared
        # property rather than G9 rejecting an unscoped projection.
        db.session().query(
            'CALL ai.example.gcprop.sums({nodeLabels:["A"], edgeTypes:["LINK"]}) '
            "YIELD nodeId, total RETURN nodeId"
        )


# --------------------------------------------------------------------------- #
# Cypher / Named projection (issue #151 P3): a guest projects a subgraph defined
# by inner Cypher queries, or a pre-registered named graph — resolved by the
# uni-query projection seam and injected into the bridge.
# --------------------------------------------------------------------------- #


def test_rhai_projection_cypher_graphref(db):
    """P3 (#151): a guest projects a Cypher-defined subgraph (nodeQuery/edgeQuery)."""
    _build_weighted_graph(db)  # :A x3 + :B, :LINK edges among the :A nodes
    db.load_rhai_plugin(RHAI_EDGE_PROBE, grants=GC_GRANTS)
    node_q = "MATCH (n:A) RETURN id(n) AS id"
    edge_q = "MATCH (a:A)-[:LINK]->(b:A) RETURN id(a) AS source, id(b) AS target"
    rows = _probe_rows(
        db.session(),
        f'CALL ai.example.gcprobe.probe({{nodeQuery: "{node_q}", edgeQuery: "{edge_q}"}})',
    )
    # The Cypher subgraph is exactly the 3 :A nodes (the :B node is excluded).
    assert len(rows) == 3


def test_rhai_projection_named_graphref(db):
    """P3 (#151): a guest projects a pre-registered Named graph."""
    _build_weighted_graph(db)
    # Register a named projection scoped to :A via the native graph procedure.
    db.session().query('CALL uni.graph.project("a_only", {nodeLabels: ["A"]})')
    db.load_rhai_plugin(RHAI_EDGE_PROBE, grants=GC_GRANTS)
    rows = _probe_rows(db.session(), 'CALL ai.example.gcprobe.probe({name: "a_only"})')
    assert len(rows) == 3


# --------------------------------------------------------------------------- #
# PyO3 loader — scalar (invokes), aggregate & procedure (register + gate)
# --------------------------------------------------------------------------- #

PY_SCALAR = """
db.set_plugin_id("ai.example.pyscore")
db.set_version("0.1.0")

@db.scalar_fn("score", args=["float","float"], returns="float", determinism="pure")
def score(x, y):
    return x * 0.7 + y * 0.3
"""


def test_pyo3_scalar_source_load_invokes(db):
    session = db.session()
    outcome = session.load_python_plugin(PY_SCALAR, "ai.example.pyscore")
    assert "ai.example.pyscore.score" in outcome["scalars_registered"]
    rows = session.query("RETURN score(1.0, 2.0) AS s")
    assert math.isclose(float(rows[0]["s"]), 1.3, abs_tol=1e-9)


def _register_pyo3_aggregate(session, grants=None):
    @session.aggregate_fn("wsum", args=["float"], returns="float")
    class WSum:  # noqa: N801 — decorator target; methods are the agg spec.
        @staticmethod
        def init():
            return {"sum": 0.0, "n": 0}

        @staticmethod
        def accumulate(state, x):
            if x is not None:
                state["sum"] += float(x)
                state["n"] += 1
            return state

        @staticmethod
        def merge(a, b):
            return {"sum": a["sum"] + b["sum"], "n": a["n"] + b["n"]}

        @staticmethod
        def finalize(state):
            return state["sum"]

    return session.finalize_plugin("ai.example.pyagg", version="0.1.0", grants=grants)


def test_pyo3_aggregate_granted_registers(db):
    session = db.session()
    outcome = _register_pyo3_aggregate(session)
    assert "ai.example.pyagg.wsum" in outcome["aggregates_registered"]


def test_pyo3_aggregate_denied_does_not_register(db):
    session = db.session()
    outcome = _register_pyo3_aggregate(session, grants=["ScalarFn"])
    assert outcome["aggregates_registered"] == []


def test_pyo3_aggregate_invokes_through_cypher(db):
    # Session-scoped aggregates resolve by fully-qualified name (like procedures);
    # the planner consults the session-local plugin registry for the UDAF.
    db.schema().label("Item").property("val", "float").done().apply()
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (:Item {val: 1.0}), (:Item {val: 2.0}), (:Item {val: 3.0})")
    tx.commit()
    _register_pyo3_aggregate(session)
    rows = session.query("MATCH (n:Item) RETURN ai.example.pyagg.wsum(n.val) AS t")
    assert math.isclose(float(rows[0]["t"]), 6.0, abs_tol=1e-9)


def _register_pyo3_procedure(session, grants=None):
    @session.procedure("evens", args=[], yields=["int"])
    def evens():
        return [{"col0": 0}, {"col0": 2}, {"col0": 4}]

    return session.finalize_plugin("ai.example.pyproc", version="0.1.0", grants=grants)


def test_pyo3_procedure_granted_registers_and_invokes(db):
    session = db.session()
    outcome = _register_pyo3_procedure(session)
    assert "ai.example.pyproc.evens" in outcome["procedures_registered"]
    # Procedures resolve by fully-qualified name.
    rows = session.query("CALL ai.example.pyproc.evens() YIELD col0 RETURN col0")
    assert [int(r["col0"]) for r in rows] == [0, 2, 4]


def test_pyo3_procedure_denied_does_not_register(db):
    session = db.session()
    outcome = _register_pyo3_procedure(session, grants=["ScalarFn"])
    assert outcome["procedures_registered"] == []


# --------------------------------------------------------------------------- #
# WASM Component Model + Extism — scalar (geo) and algorithm (graph)
# --------------------------------------------------------------------------- #

_GEO_CASES = [
    pytest.param(
        "load_wasm_component",
        WASM_GEO,
        marks=pytest.mark.skipif(
            _row_unavailable("load_wasm_component", WASM_GEO),
            reason=f"missing fixture {WASM_GEO} or loader load_wasm_component compiled out",
        ),
        id="wasm-geo",
    ),
    pytest.param(
        "load_wasm_extism",
        EXT_GEO,
        marks=pytest.mark.skipif(
            _row_unavailable("load_wasm_extism", EXT_GEO),
            reason=f"missing fixture {EXT_GEO} or loader load_wasm_extism compiled out",
        ),
        id="extism-geo",
    ),
]

_GRAPH_CASES = [
    pytest.param(
        "load_wasm_component",
        WASM_GRAPH,
        "ai.example.wasmgc",
        marks=pytest.mark.skipif(
            _row_unavailable("load_wasm_component", WASM_GRAPH),
            reason=f"missing fixture {WASM_GRAPH} or loader load_wasm_component compiled out",
        ),
        id="wasm-graph",
    ),
    pytest.param(
        "load_wasm_extism",
        EXT_GRAPH,
        "ai.example.extismgc",
        marks=pytest.mark.skipif(
            _row_unavailable("load_wasm_extism", EXT_GRAPH),
            reason=f"missing fixture {EXT_GRAPH} or loader load_wasm_extism compiled out",
        ),
        id="extism-graph",
    ),
]

_NET_CASES = [
    pytest.param(
        "load_wasm_component",
        WASM_NET,
        marks=pytest.mark.skipif(
            _row_unavailable("load_wasm_component", WASM_NET),
            reason=f"missing fixture {WASM_NET} or loader load_wasm_component compiled out",
        ),
        id="wasm-net",
    ),
    pytest.param(
        "load_wasm_extism",
        EXT_NET,
        marks=pytest.mark.skipif(
            _row_unavailable("load_wasm_extism", EXT_NET),
            reason=f"missing fixture {EXT_NET} or loader load_wasm_extism compiled out",
        ),
        id="extism-net",
    ),
]


@pytest.mark.parametrize("method, path", _GEO_CASES)
def test_wasm_scalar_granted_registers_and_invokes(db, method, path):
    outcome = _loader(db, method)(_read(path), grants=["ScalarFn"])
    assert outcome["plugin_id"] == "ai.example.geo"
    assert "ai.example.geo.haversine" in outcome["scalars_registered"]
    # Paris -> London great-circle distance (~343.6 km).
    rows = db.session().query(
        "RETURN ai.example.geo.haversine(48.8566, 2.3522, 51.5074, -0.1278) AS d"
    )
    assert math.isclose(float(rows[0]["d"]), 343.556, abs_tol=0.5)


@pytest.mark.parametrize("method, path", _GEO_CASES)
def test_wasm_scalar_denied_blocks(db, method, path):
    # Withhold ScalarFn: the scalar surface cannot register. WASM/Extism guests
    # surface this as a load-time registration failure (unlike rhai's silent skip).
    with pytest.raises(Exception):
        _loader(db, method)(_read(path), grants=["Procedure"])


@pytest.mark.parametrize("method, path, prefix", _GRAPH_CASES)
def test_wasm_algorithm_granted_matches_native(db, method, path, prefix):
    vid_a = _build_pagerank_graph(db)
    outcome = _loader(db, method)(_read(path), grants=GC_GRANTS)
    assert f"{prefix}.ppr" in outcome["algorithms_registered"]

    session = db.session()
    guest = _scores_by_node(
        session,
        f"CALL {prefix}.ppr({vid_a}, {{nodeLabels: ['Node'], edgeTypes: ['LINKS']}})",
    )
    native = _scores_by_node(
        session,
        f"CALL uni.algo.gcpagerank({vid_a}, 0.85, {{nodeLabels: ['Node'], edgeTypes: ['LINKS']}})",
    )
    assert len(guest) == 4
    assert math.isclose(sum(guest.values()), 1.0, abs_tol=1e-6)
    assert set(guest) == set(native)
    for node_id, score in guest.items():
        assert math.isclose(score, native[node_id], abs_tol=1e-9)


@pytest.mark.parametrize("method, path, prefix", _GRAPH_CASES)
def test_wasm_algorithm_denied_blocks(db, method, path, prefix):
    vid_a = _build_pagerank_graph(db)
    # Without GraphCompute/HostQuery the guest's host-graph import cannot resolve.
    # Depending on the loader this fails at load (link-time) or leaves the
    # algorithm unregistered and the CALL fails — both prove the grant gates it.
    try:
        outcome = _loader(db, method)(_read(path), grants=["ScalarFn"])
    except Exception:
        return  # load-time denial
    assert outcome["algorithms_registered"] == []
    with pytest.raises(Exception):
        db.session().query(
            f"CALL {prefix}.ppr({vid_a}) YIELD nodeId, score RETURN nodeId"
        )


@pytest.mark.parametrize("method, path", _NET_CASES)
def test_wasm_hostfn_network_gates_load(db, method, path):
    # The net guest imports a capability-gated host fn (uni_http_get). Without the
    # Network grant the import is omitted from the linker and the load fails at
    # link time; with it, the guest links and its scalar registers. Grant changes
    # behavior — the security-relevant property.
    with pytest.raises(Exception):
        _loader(db, method)(_read(path), grants=["ScalarFn"])

    outcome = _loader(db, method)(_read(path), grants=["ScalarFn", "Network"])
    assert outcome["plugin_id"] == "ai.example.net"
    assert "ai.example.net.fetch_status" in outcome["scalars_registered"]


# --------------------------------------------------------------------------- #
# Grant-string policy — the strict (rhai) path rejects non-guest-grantable names
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "grant",
    [
        "Auth",  # internal / first-party — not guest-grantable
        "MemoryBytes",  # resource quota — manifest-only, needs a value
        "NotARealCapability",  # unknown
    ],
)
def test_rhai_strict_grant_rejects_non_grantable(db, grant):
    with pytest.raises(ValueError):
        db.load_rhai_plugin(RHAI_SCALAR, grants=[grant])
