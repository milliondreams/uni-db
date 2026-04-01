Feature: ASSUME with Nested ABDUCE

  Tests that ABDUCE commands can be nested inside ASSUME THEN blocks.
  The grammar allows this (locy_clause includes abduce_query), and the
  ASSUME body dispatcher handles CompiledCommand::Abduce.

  Note: ASSUME body (locy_assume.rs) only surfaces Query and Cypher results.
  Abduce results are executed but not returned in the ASSUME command result.

  Background:
    Given an empty graph

  # ── Parse-level tests ─────────────────────────────────────────────────────

  Scenario: ABDUCE NOT inside ASSUME THEN parses
    When parsing the following Locy program:
      """
      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(b:Node)
        YIELD KEY a, KEY b
      ASSUME { CREATE (:Node {name: 'C'})-[:EDGE]->(:Node {name: 'D'}) }
      THEN { ABDUCE NOT reachable WHERE a.name = 'C' }
      """
    Then the program should parse successfully

  Scenario: ABDUCE NOT with MNOR inside ASSUME THEN parses
    When parsing the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE {prob: 0.5}]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY b, p
      ASSUME { MATCH (:Node {name: 'Weak'})-[e:CAUSE]->() DELETE e }
      THEN { ABDUCE NOT risk WHERE b.name = 'B' }
      """
    Then the program should parse successfully

  # ── Evaluation tests ──────────────────────────────────────────────────────

  Scenario: ABDUCE NOT inside ASSUME THEN evaluates without error
    Given having executed:
      """
      CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'}),
             (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'C'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(b:Node)
        YIELD KEY a, KEY b
      ASSUME { CREATE (:Node {name: 'X'})-[:EDGE]->(:Node {name: 'Y'}) }
      THEN { ABDUCE NOT reachable WHERE a.name = 'A' }
      """
    Then evaluation should succeed

  Scenario: QUERY and ABDUCE NOT on MNOR rule inside ASSUME THEN
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
      THEN {
        QUERY risk WHERE b.name = 'B' RETURN p
        ABDUCE NOT risk WHERE b.name = 'B'
      }
      """
    Then evaluation should succeed
    And the command result 0 should be an Assume containing row where p = 0.6
