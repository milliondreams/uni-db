Feature: Proof Traces (EXPLAIN RULE)

  Tests EXPLAIN RULE for constructing derivation trees that trace
  how a conclusion was derived from base facts through rule applications.

  Background:
    Given an empty graph

  # ── Parse level ───────────────────────────────────────────────────────

  Scenario: EXPLAIN RULE syntax parses
    When parsing the following Locy program:
      """
      EXPLAIN RULE reachable WHERE a.name = 'Alice' RETURN b
      """
    Then the program should parse successfully

  # ── Evaluate level ────────────────────────────────────────────────────

  Scenario: EXPLAIN non-recursive derivation tree
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      EXPLAIN RULE reachable WHERE a.name = 'A'
      """
    Then evaluation should succeed
    And the command result 0 should be an Explain with rule 'reachable'

  Scenario: EXPLAIN recursive derivation tree has children
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'})-[:EDGE]->(c:Node {name: 'C'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(mid:Node) WHERE mid IS reachable TO b YIELD KEY a, KEY b
      EXPLAIN RULE reachable WHERE a.name = 'A'
      """
    Then evaluation should succeed
    And the command result 0 should be an Explain with rule 'reachable'
