Feature: Similar To with Probabilistic Aggregation (COMB-SP)

  Tests the combination of similar_to() vector similarity scores with
  MNOR/MPROD monotonic aggregation and IS NOT probabilistic complement.
  Uses pre-computed float vectors only (no auto-embed).

  Background:
    Given an empty graph

  # ── Parse level ───────────────────────────────────────────────────────

  Scenario: similar_to score in YIELD with MNOR fold parses
    When parsing the following Locy program:
      """
      CREATE RULE scored AS
        MATCH (d:Doc)
        YIELD KEY d, similar_to(d.embedding, [1.0, 0.0, 0.0]) AS sim
      CREATE RULE combined AS
        MATCH (d:Doc)
        WHERE d IS scored TO x
        FOLD p = MNOR(sim)
        YIELD KEY d, p
      """
    Then the program should parse successfully

  Scenario: similar_to with MPROD fold parses
    When parsing the following Locy program:
      """
      CREATE RULE fit AS
        MATCH (p:Part)
        YIELD KEY p, similar_to(p.spec, [1.0, 0.0, 0.0]) AS fitness
      CREATE RULE joint AS
        MATCH (a:Assembly)
        WHERE a IS fit TO p
        FOLD quality = MPROD(fitness)
        YIELD KEY a, quality
      """
    Then the program should parse successfully

  # ── Evaluate level ────────────────────────────────────────────────────

  Scenario: similar_to score used as YIELD column
    Given having executed:
      """
      CREATE (:Doc {name: 'D1', embedding: [1.0, 0.0, 0.0]}),
             (:Doc {name: 'D2', embedding: [0.6, 0.8, 0.0]})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE scored AS
        MATCH (d:Doc)
        YIELD KEY d, similar_to(d.embedding, [1.0, 0.0, 0.0]) AS sim
      """
    Then evaluation should succeed
    And the derived relation 'scored' should have 2 facts
    And the derived relation 'scored' should contain a fact where d.name = 'D1' and sim = 1.0
    And the derived relation 'scored' should contain a fact where d.name = 'D2' and sim = 0.6

  Scenario: similar_to fed into MNOR via two-rule pattern
    Given having executed:
      """
      CREATE (a:Asset {name: 'Server', ref_vec: [1.0, 0.0, 0.0]}),
             (s1:Signal {name: 'S1', vec: [0.6, 0.8, 0.0]})-[:ALERT]->(a),
             (s2:Signal {name: 'S2', vec: [3.0, 4.0, 0.0]})-[:ALERT]->(a)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE scored_signal AS
        MATCH (s:Signal)-[:ALERT]->(a:Asset)
        YIELD KEY a, KEY s, similar_to(s.vec, a.ref_vec) AS sim
      CREATE RULE threat_level AS
        MATCH (a:Asset)
        WHERE a IS scored_signal TO s
        FOLD risk = MNOR(sim)
        YIELD KEY a, risk
      """
    Then evaluation should succeed
    And the derived relation 'scored_signal' should have 2 facts
    And the derived relation 'threat_level' should have 1 facts
    And the derived relation 'threat_level' should contain a fact where a.name = 'Server' and risk = 0.84

  Scenario: similar_to fed into MPROD via two-rule pattern
    Given having executed:
      """
      CREATE (a:Assembly {name: 'Asm1'}),
             (p1:Part {name: 'P1', spec: [1.0, 0.0, 0.0]})-[:REQUIRED_BY]->(a),
             (p2:Part {name: 'P2', spec: [0.8, 0.6, 0.0]})-[:REQUIRED_BY]->(a)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE part_fitness AS
        MATCH (p:Part)-[:REQUIRED_BY]->(a:Assembly)
        YIELD KEY a, KEY p, similar_to(p.spec, [1.0, 0.0, 0.0]) AS fit
      CREATE RULE assembly_fitness AS
        MATCH (a:Assembly)
        WHERE a IS part_fitness TO p
        FOLD joint = MPROD(fit)
        YIELD KEY a, joint
      """
    Then evaluation should succeed
    And the derived relation 'part_fitness' should have 2 facts
    And the derived relation 'assembly_fitness' should have 1 facts
    And the derived relation 'assembly_fitness' should contain a fact where a.name = 'Asm1' and joint = 0.8

  Scenario: similar_to score as PROB with IS NOT complement
    Given having executed:
      """
      CREATE (:Doc {name: 'D1', embedding: [0.6, 0.8, 0.0]})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE relevant AS
        MATCH (d:Doc)
        YIELD KEY d, similar_to(d.embedding, [1.0, 0.0, 0.0]) AS score PROB
      CREATE RULE irrelevant AS
        MATCH (d:Doc)
        WHERE d IS NOT relevant
        YIELD KEY d, 1.0 AS inv_score PROB
      """
    Then evaluation should succeed
    And the derived relation 'relevant' should have 1 facts
    And the derived relation 'relevant' should contain a fact where d.name = 'D1' and score = 0.6
    And the derived relation 'irrelevant' should have 1 facts
    And the derived relation 'irrelevant' should contain a fact where d.name = 'D1' and inv_score = 0.4
