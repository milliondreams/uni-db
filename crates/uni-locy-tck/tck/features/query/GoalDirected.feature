Feature: QUERY Goal-Directed

  Tests parsing of QUERY statements for goal-directed rule evaluation.

  Background:
    Given an empty graph

  Scenario: QUERY with WHERE and RETURN
    When parsing the following Locy program:
      """
      QUERY reachable WHERE a.name = 'Alice' RETURN b
      """
    Then the program should parse successfully

  Scenario: QUERY with WHERE and multiple RETURN items
    When parsing the following Locy program:
      """
      QUERY reachable WHERE a.name = 'Alice' RETURN a, b, cost
      """
    Then the program should parse successfully

  Scenario: QUERY without RETURN
    When parsing the following Locy program:
      """
      QUERY reachable WHERE a.name = 'Alice'
      """
    Then the program should parse successfully

  Scenario: QUERY without WHERE clause
    When parsing the following Locy program:
      """
      QUERY reachable
      """
    Then the program should parse successfully

  Scenario: QUERY without WHERE but with RETURN
    When parsing the following Locy program:
      """
      QUERY reachable RETURN a, b
      """
    Then the program should parse successfully

  # ── Evaluate-level scenarios ──────────────────────────────────────────

  Scenario: QUERY without WHERE returns all facts
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'}),
             (b)-[:EDGE]->(c:Node {name: 'C'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      QUERY reachable
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 2 rows

  Scenario: QUERY filters to matching goal bindings
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'})-[:EDGE]->(c:Node {name: 'C'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(mid:Node) WHERE mid IS reachable TO b YIELD KEY a, KEY b
      QUERY reachable WHERE a.name = 'A' RETURN b.name AS person
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 2 rows

  Scenario: QUERY returns empty when no match
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      QUERY reachable WHERE a.name = 'Z' RETURN b.name AS person
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 0 rows

  Scenario: QUERY terminates on cyclic graph
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'}), (b)-[:EDGE]->(a)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(mid:Node) WHERE mid IS reachable TO b YIELD KEY a, KEY b
      QUERY reachable WHERE a.name = 'A' RETURN b.name AS person
      """
    Then evaluation should succeed
