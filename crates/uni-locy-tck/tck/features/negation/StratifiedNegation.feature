Feature: Stratified Negation (IS NOT)

  Tests IS NOT for excluding nodes/tuples present in another derived relation.
  Negation requires stratification: the negated rule must be in a lower stratum.

  Background:
    Given an empty graph

  # ── Parse level ───────────────────────────────────────────────────────

  Scenario: IS NOT basic syntax parses
    When parsing the following Locy program:
      """
      CREATE RULE clean AS MATCH (n) WHERE n IS NOT flagged YIELD KEY n
      """
    Then the program should parse successfully

  # ── Compile level ─────────────────────────────────────────────────────

  Scenario: IS NOT creates a higher stratum
    When compiling the following Locy program:
      """
      CREATE RULE flagged AS MATCH (n:Node) WHERE n.risk > 0.5 YIELD KEY n
      CREATE RULE clean AS MATCH (n:Node) WHERE n IS NOT flagged YIELD KEY n
      """
    Then the program should compile successfully
    And the program should have 2 strata

  # ── Evaluate level ────────────────────────────────────────────────────

  Scenario: IS NOT excludes matching nodes
    Given having executed:
      """
      CREATE (:Node {name: 'A', risk: 0.8}),
             (:Node {name: 'B', risk: 0.2}),
             (:Node {name: 'C', risk: 0.1})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE flagged AS MATCH (n:Node) WHERE n.risk > 0.5 YIELD KEY n
      CREATE RULE clean AS MATCH (n:Node) WHERE n IS NOT flagged YIELD KEY n
      """
    Then evaluation should succeed
    And the derived relation 'flagged' should have 1 facts
    And the derived relation 'clean' should have 2 facts
    And the derived relation 'clean' should contain a fact where n.name = 'B'
    And the derived relation 'clean' should contain a fact where n.name = 'C'
    And the derived relation 'clean' should not contain a fact where n.name = 'A'

  Scenario: IS NOT with empty negated relation passes all
    Given having executed:
      """
      CREATE (:Node {name: 'A', risk: 0.1}),
             (:Node {name: 'B', risk: 0.2})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE flagged AS MATCH (n:Node) WHERE n.risk > 0.9 YIELD KEY n
      CREATE RULE clean AS MATCH (n:Node) WHERE n IS NOT flagged YIELD KEY n
      """
    Then evaluation should succeed
    And the derived relation 'flagged' should have 0 facts
    And the derived relation 'clean' should have 2 facts

  Scenario: Multi-stratum negation chain evaluates in correct order
    Given having executed:
      """
      CREATE (:Node {name: 'A', risk: 0.8}),
             (:Node {name: 'B', risk: 0.2}),
             (:Node {name: 'C', risk: 0.1})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risky AS MATCH (n:Node) WHERE n.risk > 0.5 YIELD KEY n
      CREATE RULE safe AS MATCH (n:Node) WHERE n IS NOT risky YIELD KEY n
      CREATE RULE trusted AS MATCH (n:Node) WHERE n IS safe, n.risk < 0.15 YIELD KEY n
      """
    Then evaluation should succeed
    And the derived relation 'risky' should have 1 facts
    And the derived relation 'safe' should have 2 facts
    And the derived relation 'trusted' should have 1 facts
    And the derived relation 'trusted' should contain a fact where n.name = 'C'
