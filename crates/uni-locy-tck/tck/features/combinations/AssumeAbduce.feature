Feature: Assume and Abduce Sequential Combinations (COMB-AA)

  Tests ASSUME and ABDUCE used in the same Locy program. ASSUME
  evaluates rules under a hypothetical mutation; ABDUCE finds
  minimal modifications to falsify a rule. Both commands can appear
  in the same program and operate on the same underlying rules.

  Background:
    Given an empty graph

  # ── Parse level ───────────────────────────────────────────────────────

  Scenario: ASSUME then ABDUCE on same rule parses
    When parsing the following Locy program:
      """
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      ASSUME { CREATE (:Node {name: 'C'})-[:EDGE]->(:Node {name: 'D'}) }
      THEN { QUERY reachable WHERE a.name = 'C' RETURN b.name AS person }
      ABDUCE NOT reachable WHERE a.name = 'A'
      """
    Then the program should parse successfully

  # ── Evaluate level ────────────────────────────────────────────────────

  Scenario: ASSUME extends reachability then ABDUCE breaks it
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'})-[:EDGE]->(c:Node {name: 'C'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(mid:Node)
        WHERE mid IS reachable TO b
        YIELD KEY a, KEY b
      ASSUME { CREATE (:Node {name: 'C'})-[:EDGE]->(:Node {name: 'D'}) }
      THEN { QUERY reachable WHERE a.name = 'C' RETURN b.name AS person }
      ABDUCE NOT reachable WHERE a.name = 'A'
      """
    Then evaluation should succeed
    And the command result 0 should be an Assume containing row where person = 'D'
    And the command result 1 should be an Abduce with at least 1 modifications

  Scenario: ASSUME with PROB rules then ABDUCE on same PROB rules
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
