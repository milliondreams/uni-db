Feature: Basic Rule Definitions

  Tests parsing of CREATE RULE statements with various clause combinations.

  Background:
    Given an empty graph

  Scenario: Simple YIELD KEY rule
    When parsing the following Locy program:
      """
      CREATE RULE reachable AS MATCH (a)-[:KNOWS]->(b) YIELD KEY a, KEY b
      """
    Then the program should parse successfully

  Scenario: Multi-clause rule with WHERE
    When parsing the following Locy program:
      """
      CREATE RULE adults AS MATCH (n:Person) WHERE n.age > 18 YIELD KEY n
      """
    Then the program should parse successfully

  Scenario: Unary IS reference in WHERE
    When parsing the following Locy program:
      """
      CREATE RULE flagged AS MATCH (n) WHERE n IS suspicious YIELD KEY n
      """
    Then the program should parse successfully

  Scenario: IS NOT reference in WHERE
    When parsing the following Locy program:
      """
      CREATE RULE clean_nodes AS MATCH (n) WHERE n IS NOT flagged YIELD KEY n
      """
    Then the program should parse successfully

  Scenario: Binary IS ... TO reference
    When parsing the following Locy program:
      """
      CREATE RULE connected AS MATCH (a)-[:E]->(b) WHERE a IS reachable TO b YIELD KEY a, KEY b
      """
    Then the program should parse successfully

  Scenario: Tuple IS reference
    When parsing the following Locy program:
      """
      CREATE RULE result AS MATCH (x)-[:E]->(y) WHERE (x, y, cost) IS control YIELD KEY x
      """
    Then the program should parse successfully
