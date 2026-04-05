Feature: Mathematical Reference Tests for MNOR and MPROD

  Validates MNOR (noisy-OR) and MPROD (product) against hand-computed reference
  values. MNOR: 1 - ∏(1 - pᵢ). MPROD: ∏pᵢ.

  Background:
    Given an empty graph

  # ── MNOR reference values ─────────────────────────────────────────────

  Scenario: MNOR single value 0.3
    Given having executed:
      """
      CREATE (:Node {name: 'A'})-[:CAUSE {prob: 0.3}]->(:Node {name: 'B'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should contain a fact where p = 0.3

  Scenario: MNOR two values 1-(0.7)(0.5) = 0.65
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.3}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.5}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 1 facts
    And the derived relation 'risk' should contain a fact where p = 0.65

  Scenario: MNOR three values 1-(0.7)(0.5)(0.3) = 0.895
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.3}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.5}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.7}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 1 facts
    And the derived relation 'risk' should contain a fact where p = 0.895

  Scenario: MNOR five values 1-(0.9)(0.8)(0.7)(0.6)(0.5) = 0.8488
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.1}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.2}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.3}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.4}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.5}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 1 facts
    And the derived relation 'risk' should contain a fact where p = 0.8488

  Scenario: MNOR edge case zero MNOR(0.0) = 0.0
    Given having executed:
      """
      CREATE (:Node {name: 'A'})-[:CAUSE {prob: 0.0}]->(:Node {name: 'B'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should contain a fact where p = 0.0

  Scenario: MNOR edge case one MNOR(1.0) = 1.0
    Given having executed:
      """
      CREATE (:Node {name: 'A'})-[:CAUSE {prob: 1.0}]->(:Node {name: 'B'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should contain a fact where p = 1.0

  # ── MPROD reference values ────────────────────────────────────────────

  Scenario: MPROD two values 0.8 * 0.9 = 0.72
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CHECK {conf: 0.8}]->(b),
             (a)-[:CHECK {conf: 0.9}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE coverage AS
        MATCH (a:Node)-[e:CHECK]->(b:Node)
        FOLD p = MPROD(e.conf)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'coverage' should have 1 facts
    And the derived relation 'coverage' should contain a fact where p = 0.72

  Scenario: MPROD three values 0.8 * 0.9 * 0.7 = 0.504
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CHECK {conf: 0.8}]->(b),
             (a)-[:CHECK {conf: 0.9}]->(b),
             (a)-[:CHECK {conf: 0.7}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE coverage AS
        MATCH (a:Node)-[e:CHECK]->(b:Node)
        FOLD p = MPROD(e.conf)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'coverage' should have 1 facts
    And the derived relation 'coverage' should contain a fact where p = 0.504

  Scenario: MPROD edge case with zero 0.0 * 0.5 = 0.0
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CHECK {conf: 0.0}]->(b),
             (a)-[:CHECK {conf: 0.5}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE coverage AS
        MATCH (a:Node)-[e:CHECK]->(b:Node)
        FOLD p = MPROD(e.conf)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'coverage' should contain a fact where p = 0.0

  Scenario: MPROD edge case with one 1.0 * 0.5 = 0.5
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CHECK {conf: 1.0}]->(b),
             (a)-[:CHECK {conf: 0.5}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE coverage AS
        MATCH (a:Node)-[e:CHECK]->(b:Node)
        FOLD p = MPROD(e.conf)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'coverage' should contain a fact where p = 0.5

  # ── IS NOT PROB complement reference ──────────────────────────────────

  Scenario: IS NOT complement of MNOR(0.3, 0.5) = 1 - 0.65 = 0.35
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.3}]->(b),
             (a)-[:CAUSE {prob: 0.5}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p

      CREATE RULE safe AS
        MATCH (a:Node), (b:Node)
        WHERE (a, b) IS NOT risk
        YIELD KEY a, KEY b, 1.0 AS safety PROB
      """
    Then evaluation should succeed
    And the derived relation 'risk' should contain a fact where p = 0.65
    And the derived relation 'safe' should contain a fact where a.name = 'A' and safety = 0.35

  Scenario: Complement chain evidence * safety with hand-computed values
    Given having executed:
      """
      CREATE (:Drug {name: 'D1'}),
             (:Disease {name: 'Flu'}),
             (:SE {name: 'headache', sev: 0.5})
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'D1'}), (dis:Disease {name: 'Flu'})
      CREATE (d)-[:SIGNAL {strength: 0.8}]->(dis)
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'D1'}), (se:SE {name: 'headache'})
      CREATE (d)-[:ADR {freq: 0.6}]->(se)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE evidence AS
        MATCH (d:Drug)-[s:SIGNAL]->(dis:Disease)
        FOLD ev = MNOR(s.strength)
        YIELD KEY d, KEY dis, ev

      CREATE RULE hazard AS
        MATCH (d:Drug)-[a:ADR]->(se:SE)
        YIELD KEY d, a.freq * se.sev AS hz PROB

      CREATE RULE penalty AS
        MATCH (d:Drug)
        WHERE d IS hazard
        FOLD pen = MNOR(hz)
        YIELD KEY d, pen

      CREATE RULE safe AS
        MATCH (d:Drug)
        WHERE d IS NOT penalty
        YIELD KEY d, 1.0 AS safety PROB
      """
    Then evaluation should succeed
    And the derived relation 'evidence' should contain a fact where ev = 0.8
    And the derived relation 'penalty' should contain a fact where pen = 0.3
    And the derived relation 'safe' should contain a fact where d.name = 'D1' and safety = 0.7
