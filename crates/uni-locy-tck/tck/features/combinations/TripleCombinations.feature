Feature: Triple Feature Combinations (COMB-TRIPLE)

  Tests three-way feature interactions: similar_to with MNOR and IS NOT
  complement; similar_to with PROB and ASSUME re-evaluation; and
  similar_to with MNOR and ABDUCE. These scenarios exercise the full
  combination pipeline used in showcase notebooks.

  Background:
    Given an empty graph

  # ── Parse level ───────────────────────────────────────────────────────

  Scenario: similar_to + MNOR + IS NOT parses
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
      CREATE RULE safe_asset AS
        MATCH (a:Asset)
        WHERE a IS NOT threat_level
        YIELD KEY a
      """
    Then the program should parse successfully

  Scenario: similar_to + PROB + ASSUME parses
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

  Scenario: similar_to scores aggregated by MNOR then IS NOT complement
    Given having executed:
      """
      CREATE (a:Asset {name: 'Server', ref_vec: [1.0, 0.0, 0.0]}),
             (s1:Signal {name: 'S1', vec: [0.6, 0.8, 0.0]})-[:ALERT]->(a),
             (s2:Signal {name: 'S2', vec: [3.0, 4.0, 0.0]})-[:ALERT]->(a),
             (:Asset {name: 'Isolated'})
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
      CREATE RULE safe_asset AS
        MATCH (a:Asset)
        WHERE a IS NOT threat_level
        YIELD KEY a
      """
    Then evaluation should succeed
    And the derived relation 'scored_signal' should have 2 facts
    And the derived relation 'threat_level' should have 1 facts
    And the derived relation 'threat_level' should contain a fact where a.name = 'Server' and risk = 0.84
    And the derived relation 'safe_asset' should have 1 facts
    And the derived relation 'safe_asset' should contain a fact where a.name = 'Isolated'
    And the derived relation 'safe_asset' should not contain a fact where a.name = 'Server'

  Scenario: similar_to MNOR threat level reduced under ASSUME DELETE
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

  Scenario: similar_to MNOR aggregation then ABDUCE finds signal removal
    Given having executed:
      """
      CREATE (q:Query {name: 'Q1', vec: [1.0, 0.0, 0.0]}),
             (d1:Doc {name: 'D1', embedding: [0.8, 0.6, 0.0]})-[:CANDIDATE]->(q)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE scored AS
        MATCH (d:Doc)-[:CANDIDATE]->(q:Query)
        YIELD KEY q, KEY d, similar_to(d.embedding, q.vec) AS score
      CREATE RULE combined AS
        MATCH (q:Query)
        WHERE q IS scored TO d
        FOLD p = MNOR(score)
        YIELD KEY q, p
      ABDUCE NOT combined WHERE q.name = 'Q1'
      """
    Then evaluation should succeed
    And the derived relation 'scored' should have 1 facts
    And the derived relation 'combined' should have 1 facts
    And the derived relation 'combined' should contain a fact where q.name = 'Q1' and p = 0.8
    And the command result 0 should be an Abduce with at least 1 modifications
