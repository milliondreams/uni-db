Feature: Recursive Rule Definitions

  Tests parsing of recursive rules with self-reference, mutual recursion,
  and priority clauses.

  Background:
    Given an empty graph

  Scenario: Self-recursive rule with two clauses
    When parsing the following Locy program:
      """
      CREATE RULE reachable AS MATCH (a)-[:EDGE]->(b) YIELD KEY a, KEY b
      CREATE RULE reachable AS MATCH (a)-[:EDGE]->(mid) WHERE mid IS reachable TO b YIELD KEY a, KEY b
      """
    Then the program should parse successfully

  Scenario: Rule with PRIORITY clause
    When parsing the following Locy program:
      """
      CREATE RULE preferred PRIORITY 1 AS MATCH (a)-[:FAST]->(b) YIELD KEY a, KEY b
      CREATE RULE preferred PRIORITY 2 AS MATCH (a)-[:SLOW]->(b) YIELD KEY a, KEY b
      """
    Then the program should parse successfully

  Scenario: Rule with mixed WHERE conditions
    When parsing the following Locy program:
      """
      CREATE RULE filtered AS MATCH (n)-[:E]->(m) WHERE n IS reachable, n.weight > 0 YIELD KEY n, KEY m
      """
    Then the program should parse successfully
