Feature: Locy Reserved Keywords

  Locy reserved keywords (RULE, ALONG, prev, FOLD, BEST, DERIVE, ASSUME,
  ABDUCE, QUERY) are forbidden only in Locy-specific identifier positions
  such as rule names. They are allowed in Cypher property-access positions
  because the Cypher grammar uses `identifier_or_keyword` for dot-property access.

  Background:
    Given an empty graph

  # ── Negative tests: reserved keywords as rule names ──────────────────

  Scenario: Using RULE as a rule name should fail
    When parsing the following Locy program:
      """
      CREATE RULE RULE AS MATCH (n) YIELD KEY n
      """
    Then the program should fail to parse

  Scenario: Using ALONG as a rule name should fail
    When parsing the following Locy program:
      """
      CREATE RULE ALONG AS MATCH (n) YIELD KEY n
      """
    Then the program should fail to parse

  Scenario: Using prev as a rule name should fail
    When parsing the following Locy program:
      """
      CREATE RULE prev AS MATCH (n) YIELD KEY n
      """
    Then the program should fail to parse

  Scenario: Using FOLD as a rule name should fail
    When parsing the following Locy program:
      """
      CREATE RULE FOLD AS MATCH (n) YIELD KEY n
      """
    Then the program should fail to parse

  Scenario: Using BEST as a rule name should fail
    When parsing the following Locy program:
      """
      CREATE RULE BEST AS MATCH (n) YIELD KEY n
      """
    Then the program should fail to parse

  Scenario: Using DERIVE as a rule name should fail
    When parsing the following Locy program:
      """
      CREATE RULE DERIVE AS MATCH (n) YIELD KEY n
      """
    Then the program should fail to parse

  Scenario: Using ASSUME as a rule name should fail
    When parsing the following Locy program:
      """
      CREATE RULE ASSUME AS MATCH (n) YIELD KEY n
      """
    Then the program should fail to parse

  Scenario: Using ABDUCE as a rule name should fail
    When parsing the following Locy program:
      """
      CREATE RULE ABDUCE AS MATCH (n) YIELD KEY n
      """
    Then the program should fail to parse

  Scenario: Using QUERY as a rule name should fail
    When parsing the following Locy program:
      """
      CREATE RULE QUERY AS MATCH (n) YIELD KEY n
      """
    Then the program should fail to parse

  # ── Positive tests: reserved keywords in property access ─────────────

  Scenario: Normal property names should parse successfully
    When parsing the following Locy program:
      """
      CREATE RULE test AS MATCH (n) WHERE n.name = 'Alice' YIELD KEY n
      """
    Then the program should parse successfully

  Scenario: Reserved keywords as property names should parse successfully
    When parsing the following Locy program:
      """
      CREATE RULE test AS MATCH (n) WHERE n.RULE = 'x' YIELD KEY n
      """
    Then the program should parse successfully

  Scenario: Multiple reserved keywords as property names should parse
    When parsing the following Locy program:
      """
      CREATE RULE test AS MATCH (n) WHERE n.ALONG > 0, n.FOLD < 10 YIELD KEY n
      """
    Then the program should parse successfully

  Scenario: Contextual keywords as property names should parse
    When parsing the following Locy program:
      """
      CREATE RULE test AS MATCH (n) WHERE n.MODULE = 'x' YIELD KEY n
      """
    Then the program should parse successfully

  # ── Positive tests: valid Locy syntax ────────────────────────────────

  Scenario: Valid CREATE RULE with real syntax should parse
    When parsing the following Locy program:
      """
      CREATE RULE reachable AS MATCH (a)-[:KNOWS]->(b) YIELD KEY a, KEY b
      """
    Then the program should parse successfully

  Scenario: Valid DERIVE clause with real syntax should parse
    When parsing the following Locy program:
      """
      CREATE RULE link AS MATCH (a)-[:KNOWS]->(b) DERIVE (a)-[:FRIEND]->(b)
      """
    Then the program should parse successfully
