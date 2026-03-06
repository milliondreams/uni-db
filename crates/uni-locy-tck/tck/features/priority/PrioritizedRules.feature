Feature: Prioritized Rules (PRIORITY)

  Tests PRIORITY clause for default/exception reasoning.
  Higher priority number wins per key group. Post-fixpoint filtering.

  Background:
    Given an empty graph

  # ── Parse level ───────────────────────────────────────────────────────

  Scenario: PRIORITY clause syntax parses
    When parsing the following Locy program:
      """
      CREATE RULE classify PRIORITY 2 AS MATCH (n:Node) YIELD KEY n, 'high' AS level
      """
    Then the program should parse successfully

  # ── Compile level ─────────────────────────────────────────────────────

  Scenario: Mixed priority across clauses rejected
    When compiling the following Locy program:
      """
      CREATE RULE classify PRIORITY 1 AS MATCH (n:Node) WHERE n.risk > 0.5 YIELD KEY n, 'high' AS level
      CREATE RULE classify AS MATCH (n:Node) YIELD KEY n, 'low' AS level
      """
    Then the program should fail to compile
    And the compile error should mention 'mixed priority'

  # ── Evaluate level ────────────────────────────────────────────────────

  Scenario: Higher priority overrides lower for same key
    Given having executed:
      """
      CREATE (:Node {name: 'A', risk: 0.8}),
             (:Node {name: 'B', risk: 0.2})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE classify PRIORITY 1 AS
        MATCH (n:Node)
        YIELD KEY n, 'low' AS level
      CREATE RULE classify PRIORITY 2 AS
        MATCH (n:Node) WHERE n.risk > 0.5
        YIELD KEY n, 'high' AS level
      """
    Then evaluation should succeed
    And the derived relation 'classify' should have 2 facts
    And the derived relation 'classify' should contain a fact where n.name = 'B'

  Scenario: Key matched only by lower priority retains lower
    Given having executed:
      """
      CREATE (:Node {name: 'A', risk: 0.2})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE classify PRIORITY 1 AS
        MATCH (n:Node)
        YIELD KEY n, 'low' AS level
      CREATE RULE classify PRIORITY 2 AS
        MATCH (n:Node) WHERE n.risk > 0.5
        YIELD KEY n, 'high' AS level
      """
    Then evaluation should succeed
    And the derived relation 'classify' should have 1 facts

  Scenario: Equal priority produces union
    Given having executed:
      """
      CREATE (:Node {name: 'A'})-[:FAST]->(:Node {name: 'B'}),
             (:Node {name: 'A'})-[:SLOW]->(:Node {name: 'B'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE routes PRIORITY 1 AS
        MATCH (a:Node)-[:FAST]->(b:Node)
        YIELD KEY a, KEY b, 'fast' AS mode
      CREATE RULE routes PRIORITY 1 AS
        MATCH (a:Node)-[:SLOW]->(b:Node)
        YIELD KEY a, KEY b, 'slow' AS mode
      """
    Then evaluation should succeed
    And the derived relation 'routes' should contain at least 2 facts
