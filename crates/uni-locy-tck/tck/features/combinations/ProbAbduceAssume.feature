Feature: Probabilistic Abduce and Assume Combinations (COMB-PA)

  Tests ABDUCE and ASSUME commands operating on rules that use
  MNOR probabilistic aggregation. Validates that ABDUCE walks MNOR
  derivation trees and that ASSUME correctly re-evaluates MNOR under
  hypothetical graph mutations.

  Background:
    Given an empty graph

  # ── Parse level ───────────────────────────────────────────────────────

  Scenario: ABDUCE NOT on MNOR rule parses
    When parsing the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      ABDUCE NOT risk WHERE a.name = 'A'
      """
    Then the program should parse successfully

  Scenario: ASSUME with MNOR rule re-evaluation parses
    When parsing the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      ASSUME { MATCH (:Node {name: 'A'})-[e:CAUSE {prob: 0.4}]->(:Node {name: 'B'}) DELETE e }
      THEN { QUERY risk WHERE a.name = 'A' RETURN p }
      """
    Then the program should parse successfully

  # ── Evaluate level ────────────────────────────────────────────────────

  Scenario: ABDUCE NOT on MNOR rule finds edge removal candidate
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:CAUSE {prob: 0.6}]->(b:Node {name: 'B'})
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
    And the derived relation 'risk' should have 1 facts
    And the derived relation 'risk' should contain a fact where p = 0.6
    And the command result 0 should be an Abduce with at least 1 modifications

  Scenario: ABDUCE NOT on multi-edge MNOR rule finds edge removal
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
      ABDUCE NOT risk WHERE a.name = 'A'
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 1 facts
    And the derived relation 'risk' should contain a fact where p = 0.76
    And the command result 0 should be an Abduce with at least 1 modifications

  Scenario: ASSUME DELETE removes one cause, reduces MNOR probability
    Given having executed:
      """
      CREATE (b:Node {name: 'B'}),
             (:Node {name: 'Strong'})-[:CAUSE {prob: 0.6}]->(b),
             (:Node {name: 'Weak'})-[:CAUSE {prob: 0.4}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY b, p
      ASSUME { MATCH (:Node {name: 'Weak'})-[e:CAUSE]->() DELETE e }
      THEN { QUERY risk WHERE b.name = 'B' RETURN p }
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 1 facts
    And the derived relation 'risk' should contain a fact where p = 0.76
    And the command result 0 should be an Assume containing row where p = 0.6

  Scenario: ASSUME and ABDUCE both operate on same MNOR rule
    Given having executed:
      """
      CREATE (b:Node {name: 'B'}),
             (:Node {name: 'Strong'})-[:CAUSE {prob: 0.6}]->(b),
             (:Node {name: 'Weak'})-[:CAUSE {prob: 0.4}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY b, p
      ASSUME { MATCH (:Node {name: 'Weak'})-[e:CAUSE]->() DELETE e }
      THEN { QUERY risk WHERE b.name = 'B' RETURN p }
      ABDUCE NOT risk WHERE b.name = 'B'
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 1 facts
    And the derived relation 'risk' should contain a fact where p = 0.76
    And the command result 0 should be an Assume containing row where p = 0.6
    And the command result 1 should be an Abduce with at least 1 modifications
