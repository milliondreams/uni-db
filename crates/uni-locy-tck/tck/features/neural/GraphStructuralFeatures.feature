Feature: Graph-structural features (Phase D D1)

  Two families of graph-structural FEATURE expressions land in
  Phase D Phase 12:

  • **Topology trio** — `degree_centrality(n)`, `pagerank_score(n)`,
    `closeness_centrality(n)` — each dispatches once per
    `apply_model_invocations` call to the corresponding
    `uni.algo.*` procedure and caches `(nodeId → score)`.

  • **Neighbor aggregators** — `avg_neighbor(n, 'REL', 'prop')`,
    `max_neighbor(n, 'REL', 'prop')`, `sum_neighbor(n, 'REL', 'prop')`
    — walk each subject's outgoing edges of `'REL'`, fetch
    `'prop'` from each neighbor, aggregate numerically.

  Both families require schema-registered labels / edge types. The
  TCK harness defaults to schema-less, so each scenario explicitly
  registers the labels and edge types it depends on.

  Background:
    Given an empty graph

  # ── degree_centrality(n) end-to-end ──────────────────────────────────

  Scenario: degree_centrality(n) feeds the classifier with the node's out-degree
    Given a registered node label "Supplier"
    And a registered edge type "SUPPLIES" from "Supplier" to "Supplier"
    And having executed:
      """
      CREATE (:Supplier {name: 'A'}), (:Supplier {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Supplier {name: 'A'}), (b:Supplier {name: 'B'})
      CREATE (a)-[:SUPPLIES]->(b)
      """
    And a registered mock classifier "echo" driven by Float feature "score"
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL echo AS
        INPUT (score)
        OUTPUT PROB risk
        USING xervo('classify/echo')

      CREATE RULE risky AS
        MATCH (s:Supplier)
        YIELD KEY s, echo(degree_centrality(s)) AS risk
      """
    Then evaluation should succeed
    # A has one outgoing :SUPPLIES edge → out-degree = 1.0
    # B has zero outgoing edges → out-degree = 0.0
    And the derived relation 'risky' should contain a fact where risk = 1.0
    And the derived relation 'risky' should contain a fact where risk = 0.0

  # ── pagerank_score(n) end-to-end ─────────────────────────────────────

  Scenario: pagerank_score(n) populates a Float feature in [0, 1]
    Given a registered node label "Node"
    And a registered edge type "LINKS" from "Node" to "Node"
    And having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:LINKS]->(b), (b)-[:LINKS]->(a)
      """
    And a registered mock classifier "echo" driven by Float feature "score"
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL echo AS
        INPUT (score)
        OUTPUT PROB risk
        USING xervo('classify/echo')

      CREATE RULE risky AS
        MATCH (n:Node)
        YIELD KEY n, echo(pagerank_score(n)) AS risk
      """
    Then evaluation should succeed
    # Symmetric 2-cycle → each node gets PageRank = 0.5 exactly.
    And the derived relation 'risky' should contain a fact where risk = 0.5

  # ── closeness_centrality(n) end-to-end ───────────────────────────────

  Scenario: closeness_centrality(n) populates a Float feature
    Given a registered node label "Node"
    And a registered edge type "LINKS" from "Node" to "Node"
    And having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:LINKS]->(b)
      """
    And a registered mock classifier "echo" driven by Float feature "score"
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL echo AS
        INPUT (score)
        OUTPUT PROB risk
        USING xervo('classify/echo')

      CREATE RULE risky AS
        MATCH (n:Node)
        YIELD KEY n, echo(closeness_centrality(n)) AS risk
      """
    Then evaluation should succeed
    # A reaches B via one outgoing edge → closeness = 1.0; B has no
    # outgoing edges → closeness = 0.0. (Wasserman-Faust off.)
    And the derived relation 'risky' should contain a fact where risk = 1.0
    And the derived relation 'risky' should contain a fact where risk = 0.0

  # ── avg_neighbor(n, 'REL', 'prop') end-to-end ────────────────────────

  Scenario: avg_neighbor averages a numeric neighbor property
    Given a registered node label "Supplier"
    And a registered edge type "TRUSTS" from "Supplier" to "Supplier"
    And having executed:
      """
      CREATE (:Supplier {name: 'A'}),
             (:Supplier {name: 'B', score: 0.6}),
             (:Supplier {name: 'C', score: 0.8})
      """
    And having executed:
      """
      MATCH (a:Supplier {name: 'A'}), (b:Supplier {name: 'B'})
      CREATE (a)-[:TRUSTS]->(b)
      """
    And having executed:
      """
      MATCH (a:Supplier {name: 'A'}), (c:Supplier {name: 'C'})
      CREATE (a)-[:TRUSTS]->(c)
      """
    And a registered mock classifier "echo" driven by Float feature "score"
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL echo AS
        INPUT (score)
        OUTPUT PROB risk
        USING xervo('classify/echo')

      CREATE RULE risky AS
        MATCH (s:Supplier {name: 'A'})
        YIELD KEY s, echo(avg_neighbor(s, 'TRUSTS', 'score')) AS risk
      """
    Then evaluation should succeed
    # A's TRUSTS-neighbors are B (score=0.6) and C (score=0.8). avg = 0.7
    And the derived relation 'risky' should contain a fact where risk = 0.7

  # ── max_neighbor ─────────────────────────────────────────────────────

  Scenario: max_neighbor returns the largest neighbor property
    Given a registered node label "Supplier"
    And a registered edge type "TRUSTS" from "Supplier" to "Supplier"
    And having executed:
      """
      CREATE (:Supplier {name: 'A'}),
             (:Supplier {name: 'B', score: 0.6}),
             (:Supplier {name: 'C', score: 0.8})
      """
    And having executed:
      """
      MATCH (a:Supplier {name: 'A'}), (b:Supplier {name: 'B'})
      CREATE (a)-[:TRUSTS]->(b)
      """
    And having executed:
      """
      MATCH (a:Supplier {name: 'A'}), (c:Supplier {name: 'C'})
      CREATE (a)-[:TRUSTS]->(c)
      """
    And a registered mock classifier "echo" driven by Float feature "score"
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL echo AS
        INPUT (score)
        OUTPUT PROB risk
        USING xervo('classify/echo')

      CREATE RULE risky AS
        MATCH (s:Supplier {name: 'A'})
        YIELD KEY s, echo(max_neighbor(s, 'TRUSTS', 'score')) AS risk
      """
    Then evaluation should succeed
    And the derived relation 'risky' should contain a fact where risk = 0.8

  # ── sum_neighbor (clamped at 1.0 by Float-driven mock) ───────────────

  Scenario: sum_neighbor totals the neighbor property
    Given a registered node label "Supplier"
    And a registered edge type "TRUSTS" from "Supplier" to "Supplier"
    And having executed:
      """
      CREATE (:Supplier {name: 'A'}),
             (:Supplier {name: 'B', score: 0.3}),
             (:Supplier {name: 'C', score: 0.4})
      """
    And having executed:
      """
      MATCH (a:Supplier {name: 'A'}), (b:Supplier {name: 'B'})
      CREATE (a)-[:TRUSTS]->(b)
      """
    And having executed:
      """
      MATCH (a:Supplier {name: 'A'}), (c:Supplier {name: 'C'})
      CREATE (a)-[:TRUSTS]->(c)
      """
    And a registered mock classifier "echo" driven by Float feature "score"
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL echo AS
        INPUT (score)
        OUTPUT PROB risk
        USING xervo('classify/echo')

      CREATE RULE risky AS
        MATCH (s:Supplier {name: 'A'})
        YIELD KEY s, echo(sum_neighbor(s, 'TRUSTS', 'score')) AS risk
      """
    Then evaluation should succeed
    # 0.3 + 0.4 = 0.7
    And the derived relation 'risky' should contain a fact where risk = 0.7

  # ── Subject with no neighbors → Null → mock returns 0.0 ──────────────

  Scenario: avg_neighbor with no neighbors surfaces Null (mock interprets as 0)
    Given a registered node label "Supplier"
    And a registered edge type "TRUSTS" from "Supplier" to "Supplier"
    And having executed:
      """
      CREATE (:Supplier {name: 'lonely'})
      """
    And a registered mock classifier "echo" driven by Float feature "score"
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL echo AS
        INPUT (score)
        OUTPUT PROB risk
        USING xervo('classify/echo')

      CREATE RULE risky AS
        MATCH (s:Supplier {name: 'lonely'})
        YIELD KEY s, echo(avg_neighbor(s, 'TRUSTS', 'score')) AS risk
      """
    Then evaluation should succeed
    And the derived relation 'risky' should contain a fact where risk = 0.0

  # ── Wrong arity rejected at compile time ─────────────────────────────

  Scenario: degree_centrality with too many arguments fails compilation
    Given a registered node label "Supplier"
    And having executed:
      """
      CREATE (:Supplier {name: 'A'})
      """
    And a registered mock classifier "echo" returning 0.5
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL echo AS
        INPUT (score)
        OUTPUT PROB risk
        USING xervo('classify/echo')

      CREATE RULE risky AS
        MATCH (s:Supplier)
        YIELD KEY s, echo(degree_centrality(s, 'extra')) AS risk
      """
    Then evaluation should fail
    And the evaluation error should mention "degree_centrality"

  Scenario: avg_neighbor with wrong number of args fails compilation
    Given a registered node label "Supplier"
    And having executed:
      """
      CREATE (:Supplier {name: 'A'})
      """
    And a registered mock classifier "echo" returning 0.5
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL echo AS
        INPUT (score)
        OUTPUT PROB risk
        USING xervo('classify/echo')

      CREATE RULE risky AS
        MATCH (s:Supplier)
        YIELD KEY s, echo(avg_neighbor(s, 'TRUSTS')) AS risk
      """
    Then evaluation should fail
    And the evaluation error should mention "avg_neighbor"
