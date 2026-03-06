Feature: Module Composition (MODULE / USE)

  Tests MODULE declaration and USE import syntax.
  Note: module resolution is parsed but not yet wired into the compiler
  pipeline (see LOCY_IMPL_GAPS.md).

  Background:
    Given an empty graph

  # ── Parse level ───────────────────────────────────────────────────────

  Scenario: MODULE declaration parses
    When parsing the following Locy program:
      """
      MODULE acme.compliance
      CREATE RULE control AS MATCH (a)-[:OWNS]->(b) YIELD KEY a, KEY b
      """
    Then the program should parse successfully

  Scenario: USE import parses
    When parsing the following Locy program:
      """
      USE acme.compliance
      CREATE RULE flagged AS MATCH (n) WHERE n IS control YIELD KEY n
      """
    Then the program should parse successfully

  Scenario: MODULE with multiple USE imports parses
    When parsing the following Locy program:
      """
      MODULE acme.risk
      USE acme.compliance
      USE acme.sanctions
      CREATE RULE combined AS MATCH (n) WHERE n IS control, n IS NOT sanctioned YIELD KEY n
      """
    Then the program should parse successfully

  Scenario: Selective imports with multiple identifiers
    When parsing the following Locy program:
      """
      USE acme.compliance { control, reachable }
      CREATE RULE flagged AS MATCH (n) WHERE n IS control YIELD KEY n
      """
    Then the program should parse successfully

  Scenario: Single selective import
    When parsing the following Locy program:
      """
      USE acme.compliance { control }
      CREATE RULE flagged AS MATCH (n) WHERE n IS control YIELD KEY n
      """
    Then the program should parse successfully

  Scenario: Backtick-quoted reserved keyword in import list
    When parsing the following Locy program:
      """
      USE acme.compliance { `rule`, reachable }
      CREATE RULE flagged AS MATCH (n) WHERE n IS reachable YIELD KEY n
      """
    Then the program should parse successfully

  Scenario: Empty selective import list fails to parse
    When parsing the following Locy program:
      """
      USE acme.compliance { }
      CREATE RULE flagged AS MATCH (n) YIELD KEY n
      """
    Then the program should fail to parse

  Scenario: Glob import backward compatibility
    When parsing the following Locy program:
      """
      USE acme.compliance
      CREATE RULE flagged AS MATCH (n) WHERE n IS control YIELD KEY n
      """
    Then the program should parse successfully
