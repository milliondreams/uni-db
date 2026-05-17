# SPDX-License-Identifier: Apache-2.0
# Copyright 2024-2026 Dragonscale Team
#
# Smoke test for the Python binding exposing NeuralProvenance details
# on derivations returned by EXPLAIN RULE.

import uni_db


PROGRAM = """
CREATE MODEL risk_scorer AS
  INPUT (n)
  FEATURES n.score
  OUTPUT PROB risk
  USING xervo('classify/risk-v1')

CREATE RULE risky AS
  MATCH (n:Asset)
  YIELD KEY n, risk_scorer(n.score) AS risk
"""

EXPLAIN_PROGRAM = PROGRAM + "\nEXPLAIN RULE risky WHERE n.name = 'a3'\n"


def test_neural_provenance_exposed_on_derivations():
    db = uni_db.Uni.temporary()
    (
        db.schema()
        .label("Asset")
        .property("name", "string")
        .property("score", "float")
        .apply()
    )
    session = db.session()
    tx = session.tx()
    for name, score in [("a1", 0.1), ("a2", 0.4), ("a3", 0.7)]:
        tx.execute(f"CREATE (:Asset {{name: '{name}', score: {score}}})")
    tx.commit()

    def scorer(inputs):
        return [0.42 for _ in inputs]

    config = uni_db.LocyConfig()
    config.register_classifier("risk_scorer", scorer)

    result = session.locy_with(EXPLAIN_PROGRAM).with_config(config).run()

    # EXPLAIN populates result.explain (a list of derivation trees, one per
    # matched row). Walk every node and look for at least one neural_calls
    # entry naming our model.
    found = False
    saw_keys = set()

    def visit(node):
        nonlocal found
        if not isinstance(node, dict):
            return
        saw_keys.update(node.keys())
        for call in node.get("neural_calls", []):
            assert "model_name" in call
            assert "raw_probability" in call
            assert "calibrated_probability" in call
            assert "confidence_band" in call
            if call["model_name"] == "risk_scorer":
                assert call["raw_probability"] == 0.42
                found = True
        for child in node.get("children", []):
            visit(child)

    # EXPLAIN output lands inside result.command_results — each element is
    # a dict-like with a 'derivations' (or similar) key holding the trees.
    command_results = result.command_results
    assert command_results, (
        f"expected at least one command_result; got {command_results!r}"
    )
    trees_visited = 0
    for cmd in command_results:
        # ExplainCommandResult exposes `.tree` (single derivation tree)
        # or `.tree`-like nested structure. Walk whatever shape it has.
        tree = getattr(cmd, "tree", None)
        if tree is None:
            continue
        # The tree may be a dict at the root, or a list of dicts.
        if isinstance(tree, list):
            for t in tree:
                visit(t)
                trees_visited += 1
        else:
            visit(tree)
            trees_visited += 1
    assert trees_visited > 0, (
        f"no derivation trees walked; command_results structure: "
        f"{[type(c).__name__ for c in command_results]}, "
        f"first cmd attrs: "
        f"{dir(command_results[0]) if command_results else 'n/a'}"
    )

    assert "neural_calls" in saw_keys, (
        f"neural_calls key missing from every derivation dict; keys seen: "
        f"{sorted(saw_keys)}"
    )
    assert found, (
        "expected at least one neural_calls entry naming 'risk_scorer' "
        "with raw_probability == 0.42 — the side-channel store + the "
        "synthetic-column fallback in eval_feature_expr_against_fact_row "
        "must surface per-leaf NeuralProvenance for YIELD-position calls."
    )


def _walk_leaves(tree):
    """Depth-first walker yielding every leaf-like node (no children)."""
    stack = [tree]
    while stack:
        node = stack.pop()
        if not isinstance(node, dict):
            continue
        children = node.get("children") or []
        if not children:
            yield node
        else:
            stack.extend(children)


def test_scalar_int_yield_column_is_not_enriched_to_node():
    """Regression: when a rule YIELDs a non-KEY scalar Int column whose
    value happens to coincide with a real node vid, the enrichment must
    leave it as Int (not replace it with the Node). KEY-scoped enrichment
    is what protects us; this test pins that behavior."""
    db = uni_db.Uni.temporary()
    (
        db.schema()
        .label("Asset")
        .property("name", "string")
        .property("score", "float")
        .property("code", "int")
        .apply()
    )
    session = db.session()
    tx = session.tx()
    # `code` deliberately overlaps with vid range (0,1,2) so a naive
    # enrichment would mis-classify it.
    for name, score, code in [
        ("a1", 0.1, 0),
        ("a2", 0.4, 1),
        ("a3", 0.7, 2),
    ]:
        tx.execute(
            f"CREATE (:Asset {{name: '{name}', score: {score}, code: {code}}})"
        )
    tx.commit()

    PROG = """
CREATE RULE asset_with_code AS
  MATCH (n:Asset)
  YIELD KEY n, n.code AS code

EXPLAIN RULE asset_with_code WHERE n.name = 'a3'
"""
    result = session.locy_with(PROG).run()
    explain_records = [
        c for c in result.command_results if isinstance(c, uni_db.ExplainCommandResult)
    ]
    assert explain_records, "expected an ExplainCommandResult"
    tree = explain_records[0].tree
    assert tree is not None

    # Mode A WHERE filter should narrow to exactly one leaf for 'a3'.
    leaves = list(_walk_leaves(tree))
    assert len(leaves) == 1, (
        f"expected one matching leaf for n.name='a3'; got {len(leaves)} "
        f"(WHERE filter not effective — Mode A may have fallen back to Mode B)"
    )
    bindings = leaves[0].get("bindings", {})
    # `n` is a KEY → enriched to a Node.
    n_val = bindings.get("n")
    assert hasattr(n_val, "properties"), (
        f"KEY `n` should be a Node after enrichment; got {n_val!r}"
    )
    assert n_val.properties.get("name") == "a3"
    # `code` is a non-KEY scalar — its underlying Int (2) collides with
    # the vid 2 of node 'a3'. Must remain a numeric scalar, NOT a Node.
    code_val = bindings.get("code")
    assert not hasattr(code_val, "properties"), (
        f"non-KEY scalar `code` should remain a scalar (no enrichment to "
        f"Node); got {type(code_val).__name__} value {code_val!r}"
    )
    assert isinstance(code_val, (int, float)), (
        f"`code` should be numeric, got {type(code_val).__name__} {code_val!r}"
    )
    assert int(code_val) == 2


def test_edge_binding_where_filter_works_via_mode_a():
    """Edge bindings live as `Value::Map({_eid,...})` in tracker
    fact_rows. The enrichment must call normalize_graph_row so that
    Map → Value::Edge conversion happens before the WHERE filter
    runs — otherwise `WHERE e.since = 2020` falls through to Mode B
    with empty neural_calls."""
    db = uni_db.Uni.temporary()
    (
        db.schema()
        .label("Person")
        .property("name", "string")
        .done()
        .edge_type("KNOWS", ["Person"], ["Person"])
        .property("since", "int")
        .done()
        .apply()
    )
    session = db.session()
    tx = session.tx()
    tx.execute("CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2019}]->(b:Person {name: 'Bob'})")
    tx.execute("CREATE (a:Person {name: 'Carol'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Dan'})")
    tx.execute("CREATE (a:Person {name: 'Eve'})-[:KNOWS {since: 2021}]->(b:Person {name: 'Frank'})")
    tx.commit()

    # Use a neural-predicate rule so the tracker is populated (the
    # production tracker is auto-constructed when a MNOR/MPROD or
    # EXPLAIN is present; we add a CREATE MODEL + neural classifier
    # to ensure the tracker carries entries for this rule).
    PROG = """
CREATE MODEL trust AS
  INPUT (a)
  FEATURES a.name
  OUTPUT PROB t
  USING xervo('classify/trust-v1')

CREATE RULE friendship AS
  MATCH (a:Person)-[e:KNOWS]->(b:Person)
  YIELD KEY a, KEY e, KEY b, trust(a.name) AS confidence

EXPLAIN RULE friendship WHERE e.since = 2020
"""
    config = uni_db.LocyConfig()
    config.register_classifier("trust", lambda inputs: [0.7 for _ in inputs])

    result = session.locy_with(PROG).with_config(config).run()
    explain_records = [
        c for c in result.command_results if isinstance(c, uni_db.ExplainCommandResult)
    ]
    assert explain_records, "expected an ExplainCommandResult"
    tree = explain_records[0].tree
    assert tree is not None

    leaves = list(_walk_leaves(tree))
    assert len(leaves) == 1, (
        f"expected one matching leaf for e.since=2020; got {len(leaves)} "
        f"(WHERE on edge prop may have fallen back to Mode B)"
    )
    bindings = leaves[0].get("bindings", {})

    # `e` is a KEY edge → should be enriched to an Edge value with `since=2020`.
    e_val = bindings.get("e")
    assert hasattr(e_val, "properties") or (
        isinstance(e_val, dict) and "since" in e_val
    ), f"KEY `e` should be Edge after normalize_graph_row; got {e_val!r}"
    # Properties accessor differs between Edge object and Map dict.
    since = (
        e_val.properties.get("since")
        if hasattr(e_val, "properties")
        else e_val.get("since")
    )
    assert since == 2020, f"expected since=2020, got {since!r}"
