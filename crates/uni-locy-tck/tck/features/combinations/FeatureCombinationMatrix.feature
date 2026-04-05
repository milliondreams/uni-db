Feature: Feature Combination Matrix

  Systematically tests untested feature interactions: FOLD with IS-ref
  value columns, FOLD chains with IS NOT PROB complement, composite-key
  patterns, and ASSUME/ABDUCE with FOLD rules.

  Background:
    Given an empty graph

  # ── FOLD + IS NOT PROB complement + QUERY ─────────────────────────────

  Scenario: FOLD MNOR penalty then IS NOT complement then QUERY
    Given having executed:
      """
      CREATE (:Drug {name: 'A'}), (:Drug {name: 'B'}),
             (:SE {name: 'nausea', sev: 0.9})
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'A'}), (s:SE {name: 'nausea'})
      CREATE (d)-[:ADR {freq: 0.7}]->(s)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE adr_risk AS
        MATCH (d:Drug)-[c:ADR]->(se:SE)
        YIELD KEY d, c.freq * se.sev AS hazard PROB

      CREATE RULE penalty AS
        MATCH (d:Drug)
        WHERE d IS adr_risk
        FOLD p = MNOR(hazard)
        YIELD KEY d, p

      CREATE RULE safe AS
        MATCH (d:Drug)
        WHERE d IS NOT penalty
        YIELD KEY d, 1.0 AS safety PROB

      QUERY safe WHERE d = d RETURN d.name AS drug, safety ORDER BY drug
      """
    Then evaluation should succeed
    And the derived relation 'safe' should have 2 facts
    And the command result 0 should be a Query with 2 rows

  # ── FOLD + composite-key IS NOT + QUERY ───────────────────────────────

  Scenario: FOLD MNOR signal then composite-key IS NOT novel then QUERY
    Given having executed:
      """
      CREATE (:Drug {name: 'A'}), (:Disease {name: 'Flu'}), (:Disease {name: 'Cold'})
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'A'}), (dis:Disease {name: 'Flu'}) CREATE (d)-[:IND]->(dis)
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'A'}), (dis:Disease {name: 'Flu'}) CREATE (d)-[:SIG {s: 0.8}]->(dis)
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'A'}), (dis:Disease {name: 'Cold'}) CREATE (d)-[:SIG {s: 0.6}]->(dis)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE signal AS
        MATCH (d:Drug)-[e:SIG]->(dis:Disease)
        FOLD ev = MNOR(e.s)
        YIELD KEY d, KEY dis, ev

      CREATE RULE known AS
        MATCH (d:Drug)-[:IND]->(dis:Disease)
        YIELD KEY d, KEY dis

      CREATE RULE novel AS
        MATCH (d:Drug)
        WHERE d IS signal TO dis, d IS NOT known TO dis
        YIELD KEY d, KEY dis, ev AS score

      QUERY novel WHERE d = d RETURN d.name AS drug, dis.name AS disease, score
      """
    Then evaluation should succeed
    And the derived relation 'novel' should have 1 facts
    And the derived relation 'novel' should contain a fact where dis.name = 'Cold'
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where disease = 'Cold'

  # ── ASSUME + FOLD + property access ───────────────────────────────────

  Scenario: ASSUME mutation with FOLD MNOR re-evaluation
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.4}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p

      ASSUME {
        MATCH (a:Node {name: 'A'})-[e:CAUSE]->(b:Node {name: 'B'})
        SET e.prob = 0.9
      } THEN {
        QUERY risk WHERE a.name = 'A' RETURN a.name AS src, b.name AS dst, p
      }
      """
    Then evaluation should succeed
    And the derived relation 'risk' should contain a fact where p = 0.4
    And the command result 0 should be an Assume with 1 rows

  # ── Three-stratum pipeline with QUERY ─────────────────────────────────

  Scenario: Facts to MNOR to IS NOT PROB complement with QUERY
    Given having executed:
      """
      CREATE (:Account {name: 'Alice'})-[:RISK {prob: 0.7}]->(:Category {type: 'fraud'}),
             (:Account {name: 'Bob'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risky AS
        MATCH (a:Account)-[r:RISK]->(c:Category)
        FOLD score = MNOR(r.prob)
        YIELD KEY a, score

      CREATE RULE safe AS
        MATCH (a:Account)
        WHERE a IS NOT risky
        YIELD KEY a, 1.0 AS confidence PROB

      QUERY safe WHERE a = a RETURN a.name AS name, confidence ORDER BY name
      """
    Then evaluation should succeed
    And the derived relation 'safe' should have 2 facts
    And the derived relation 'safe' should contain a fact where a.name = 'Alice' and confidence = 0.3
    And the derived relation 'safe' should contain a fact where a.name = 'Bob' and confidence = 1.0
    And the command result 0 should be a Query with 2 rows
    And the command result 0 should be a Query containing row where name = 'Bob'

  # ── WHERE filter on IS-ref value column + QUERY ───────────────────────

  Scenario: WHERE filter on similar_to score from IS-ref
    Given having executed:
      """
      CREATE (:Doc {name: 'A', emb: [0.9, 0.1, 0.0, 0.0]}),
             (:Doc {name: 'B', emb: [0.1, 0.9, 0.0, 0.0]}),
             (:Doc {name: 'C', emb: [0.85, 0.15, 0.0, 0.0]})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE sim AS
        MATCH (a:Doc), (b:Doc)
        WHERE a <> b
        YIELD KEY a, KEY b, similar_to(a.emb, b.emb) AS score

      CREATE RULE strong_match AS
        MATCH (a:Doc)
        WHERE a IS sim TO b, score >= 0.5
        YIELD KEY a

      QUERY strong_match WHERE a = a RETURN a.name AS name ORDER BY name
      """
    Then evaluation should succeed
    And the derived relation 'strong_match' should contain at least 1 facts
    And the command result 0 should be a Query with at least 1 rows

  # ── ABDUCE + FOLD MNOR ────────────────────────────────────────────────

  Scenario: ABDUCE NOT on FOLD MNOR rule finds modification
    Given having executed:
      """
      CREATE (:Node {name: 'A'})-[:CAUSE {prob: 0.6}]->(:Node {name: 'B'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p

      ABDUCE NOT risk WHERE a.name = 'A'
      """
    Then evaluation should succeed
    And the command result 0 should be an Abduce with at least 1 modifications
