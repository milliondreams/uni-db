Feature: FoldInRecursivePath compile warning (Semantic Stress Corpus B3)

  When a recursive rule (an IS-reference to a rule in the same SCC)
  carries a FOLD clause but no ALONG, the compiler emits
  `FoldInRecursivePath`. FOLD groups by KEY columns, not by path —
  authors who want per-path aggregation across recursive walks
  almost always meant to add ALONG.

  Background:
    Given an empty graph

  # ── Recursive rule with FOLD but no ALONG → warning ──────────────────

  Scenario: Recursive IS-ref + FOLD + no ALONG emits FoldInRecursivePath
    Given having executed:
      """
      CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(b:Node)
        FOLD risk = MNOR(0.9)
        YIELD KEY a, KEY b, risk

      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(mid:Node)
        WHERE mid IS reachable TO b
        FOLD risk = MNOR(0.9)
        YIELD KEY a, KEY b, risk
      """
    Then evaluation should succeed
    And the result should contain a FoldInRecursivePath warning

  # ── Recursive + ALONG (path-aware) → no warning ───────────────────────

  Scenario: Recursive IS-ref + FOLD + ALONG suppresses the warning
    Given having executed:
      """
      CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(b:Node)
        ALONG step = 0.9
        FOLD risk = MNOR(step)
        YIELD KEY a, KEY b, risk

      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(mid:Node)
        WHERE mid IS reachable TO b
        ALONG step = 0.9
        FOLD risk = MNOR(step)
        YIELD KEY a, KEY b, risk
      """
    Then evaluation should succeed

  # ── Non-recursive rule with FOLD but no ALONG → no warning ───────────

  Scenario: Non-recursive FOLD without ALONG does not warn
    Given having executed:
      """
      CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE edge_risk AS
        MATCH (a:Node)-[:EDGE]->(b:Node)
        FOLD risk = MNOR(0.9)
        YIELD KEY a, KEY b, risk
      """
    Then evaluation should succeed
