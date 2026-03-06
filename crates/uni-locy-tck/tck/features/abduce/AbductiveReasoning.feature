Feature: Abductive Reasoning (ABDUCE)

  Tests ABDUCE for finding minimal graph modifications that would
  change a rule's truth value. Uses derivation trees + ASSUME validation.

  Background:
    Given an empty graph

  # ── Parse level ───────────────────────────────────────────────────────

  Scenario: ABDUCE positive syntax parses
    When parsing the following Locy program:
      """
      ABDUCE reachable WHERE a.name = 'Alice' RETURN b
      """
    Then the program should parse successfully

  Scenario: ABDUCE NOT syntax parses
    When parsing the following Locy program:
      """
      ABDUCE NOT reachable WHERE a.name = 'Alice'
      """
    Then the program should parse successfully

  # ── Evaluate level ────────────────────────────────────────────────────

  Scenario: ABDUCE NOT finds edge removal candidate
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'})-[:EDGE]->(c:Node {name: 'C'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(mid:Node) WHERE mid IS reachable TO b YIELD KEY a, KEY b
      ABDUCE NOT reachable WHERE a.name = 'A'
      """
    Then evaluation should succeed
    And the command result 0 should be an Abduce with at least 1 modifications

  Scenario: ABDUCE positive finds edge addition candidate
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'C'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      ABDUCE reachable WHERE a.name = 'A'
      """
    Then evaluation should succeed
