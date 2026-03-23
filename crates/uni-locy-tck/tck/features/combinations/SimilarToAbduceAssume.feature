Feature: Similar To with Abduce and Assume (COMB-SAA)

  Tests ABDUCE and ASSUME operating on rules that use similar_to()
  vector similarity. ABDUCE walks the derivation tree of similarity-
  derived rules. ASSUME re-evaluates MNOR threat levels under
  hypothetical graph changes.

  Background:
    Given an empty graph

  # ── Parse level ───────────────────────────────────────────────────────

  Scenario: ABDUCE NOT on similar_to rule parses
    When parsing the following Locy program:
      """
      CREATE RULE relevant AS
        MATCH (d:Doc)-[:CANDIDATE]->(q:Query)
        YIELD KEY d, KEY q, similar_to(d.embedding, q.vec) AS score
      ABDUCE NOT relevant WHERE d.name = 'D1'
      """
    Then the program should parse successfully

  Scenario: ASSUME with similar_to MNOR rule parses
    When parsing the following Locy program:
      """
      CREATE RULE scored_signal AS
        MATCH (s:Signal)-[:ALERT]->(a:Asset)
        YIELD KEY a, KEY s, similar_to(s.vec, a.ref_vec) AS sim
      CREATE RULE threat_level AS
        MATCH (a:Asset)
        WHERE a IS scored_signal TO s
        FOLD risk = MNOR(sim)
        YIELD KEY a, risk
      ASSUME { MATCH (:Signal {name: 'S2'})-[e:ALERT]->() DELETE e }
      THEN { QUERY threat_level WHERE a.name = 'Server' RETURN risk }
      """
    Then the program should parse successfully

  # ── Evaluate level ────────────────────────────────────────────────────

  Scenario: ABDUCE NOT on similar_to-derived rule finds edge removal
    Given having executed:
      """
      CREATE (d:Doc {name: 'D1', embedding: [1.0, 0.0, 0.0]})-[:CANDIDATE]->(q:Query {name: 'Q1', vec: [1.0, 0.0, 0.0]})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE relevant AS
        MATCH (d:Doc)-[:CANDIDATE]->(q:Query)
        YIELD KEY d, KEY q, similar_to(d.embedding, q.vec) AS score
      ABDUCE NOT relevant WHERE d.name = 'D1'
      """
    Then evaluation should succeed
    And the derived relation 'relevant' should have 1 facts
    And the derived relation 'relevant' should contain a fact where d.name = 'D1' and score = 1.0
    And the command result 0 should be an Abduce with at least 1 modifications

  Scenario: ASSUME DELETE removes signal and reduces similar_to MNOR
    Given having executed:
      """
      CREATE (a:Asset {name: 'Server', ref_vec: [1.0, 0.0, 0.0]}),
             (s1:Signal {name: 'S1', vec: [0.6, 0.8, 0.0]})-[:ALERT]->(a),
             (s2:Signal {name: 'S2', vec: [0.8, 0.6, 0.0]})-[:ALERT]->(a)
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
      ASSUME { MATCH (:Signal {name: 'S2'})-[e:ALERT]->() DELETE e }
      THEN { QUERY threat_level WHERE a.name = 'Server' RETURN risk }
      """
    Then evaluation should succeed
    And the derived relation 'threat_level' should have 1 facts
    And the derived relation 'threat_level' should contain a fact where a.name = 'Server' and risk = 0.92
    And the command result 0 should be an Assume containing row where risk = 0.6

  Scenario: ASSUME CREATE adds signal and ABDUCE operates on same similar_to MNOR rule
    Given having executed:
      """
      CREATE (a:Asset {name: 'Server', ref_vec: [1.0, 0.0, 0.0]}),
             (s1:Signal {name: 'S1', vec: [0.6, 0.8, 0.0]})-[:ALERT]->(a)
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
      ASSUME { MATCH (a:Asset {name: 'Server'}) CREATE (:Signal {name: 'S2', vec: [0.8, 0.6, 0.0]})-[:ALERT]->(a) }
      THEN { QUERY threat_level WHERE a.name = 'Server' RETURN risk }
      ABDUCE NOT threat_level WHERE a.name = 'Server'
      """
    Then evaluation should succeed
    And the derived relation 'threat_level' should have 1 facts
    And the derived relation 'threat_level' should contain a fact where a.name = 'Server' and risk = 0.6
    And the command result 0 should be an Assume containing row where risk = 0.92
    And the command result 1 should be an Abduce with at least 1 modifications
