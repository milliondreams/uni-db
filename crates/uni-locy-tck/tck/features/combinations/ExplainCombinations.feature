Feature: Explain with Probabilistic and Similarity Features (COMB-EX)

  Tests EXPLAIN RULE operating on rules that use MNOR probabilistic
  aggregation, IS NOT complement, and similar_to() vector similarity.
  EXPLAIN must produce a derivation tree regardless of which rule
  features are involved.

  Background:
    Given an empty graph

  # ── Parse level ───────────────────────────────────────────────────────

  Scenario: EXPLAIN on MNOR rule parses
    When parsing the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      EXPLAIN RULE risk WHERE a.name = 'A'
      """
    Then the program should parse successfully

  Scenario: EXPLAIN on IS NOT complement rule parses
    When parsing the following Locy program:
      """
      CREATE RULE risky AS
        MATCH (n:Node)-[:HAS_RISK]->(r:Risk)
        YIELD KEY n, r.score AS risk_score PROB
      CREATE RULE safe AS
        MATCH (n:Node)
        WHERE n IS NOT risky
        YIELD KEY n, 1.0 AS safety PROB
      EXPLAIN RULE safe WHERE n.name = 'X'
      """
    Then the program should parse successfully

  Scenario: EXPLAIN on similar_to rule parses
    When parsing the following Locy program:
      """
      CREATE RULE scored AS
        MATCH (d:Doc)
        YIELD KEY d, similar_to(d.embedding, [1.0, 0.0, 0.0]) AS sim
      EXPLAIN RULE scored WHERE d.name = 'D1'
      """
    Then the program should parse successfully

  # ── Evaluate level ────────────────────────────────────────────────────

  Scenario: EXPLAIN on MNOR rule returns derivation tree
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:CAUSE {prob: 0.6}]->(b:Node {name: 'B'}),
             (a)-[:CAUSE {prob: 0.4}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      EXPLAIN RULE risk WHERE a.name = 'A'
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 1 facts
    And the derived relation 'risk' should contain a fact where p = 0.76
    And the command result 0 should be an Explain with rule 'risk'

  Scenario: EXPLAIN on IS NOT complement rule returns derivation tree
    Given having executed:
      """
      CREATE (:Node {name: 'X'})-[:HAS_RISK]->(:Risk {score: 0.7})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risky AS
        MATCH (n:Node)-[:HAS_RISK]->(r:Risk)
        YIELD KEY n, r.score AS risk_score PROB
      CREATE RULE safe AS
        MATCH (n:Node)
        WHERE n IS NOT risky
        YIELD KEY n, 1.0 AS safety PROB
      EXPLAIN RULE safe WHERE n.name = 'X'
      """
    Then evaluation should succeed
    And the derived relation 'risky' should have 1 facts
    And the derived relation 'safe' should have 1 facts
    And the derived relation 'safe' should contain a fact where n.name = 'X' and safety = 0.3
    And the command result 0 should be an Explain with rule 'safe'

  Scenario: EXPLAIN on similar_to-derived rule returns derivation tree
    Given having executed:
      """
      CREATE (:Doc {name: 'D1', embedding: [1.0, 0.0, 0.0]})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE scored AS
        MATCH (d:Doc)
        YIELD KEY d, similar_to(d.embedding, [1.0, 0.0, 0.0]) AS sim
      EXPLAIN RULE scored WHERE d.name = 'D1'
      """
    Then evaluation should succeed
    And the derived relation 'scored' should have 1 facts
    And the derived relation 'scored' should contain a fact where d.name = 'D1' and sim = 1.0
    And the command result 0 should be an Explain with rule 'scored'
