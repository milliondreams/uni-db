Feature: Compile-Level Stratification

  Tests compilation of Locy programs, verifying stratification,
  dependency analysis, and error detection.

  Background:
    Given an empty graph

  Scenario: Single-stratum non-recursive program
    When compiling the following Locy program:
      """
      CREATE RULE adults AS MATCH (n:Person) WHERE n.age > 18 YIELD KEY n
      """
    Then the program should compile successfully
    And the program should have 1 strata
    And the stratum 0 should not be recursive

  Scenario: Self-recursive rule in single stratum
    When compiling the following Locy program:
      """
      CREATE RULE reachable AS MATCH (a)-[:EDGE]->(b) YIELD KEY a, KEY b
      CREATE RULE reachable AS MATCH (a)-[:EDGE]->(mid) WHERE mid IS reachable TO b YIELD KEY a, KEY b
      """
    Then the program should compile successfully
    And the program should have 1 strata
    And the stratum 0 should be recursive

  Scenario: Two strata with negation dependency
    When compiling the following Locy program:
      """
      CREATE RULE base AS MATCH (n:Person) YIELD KEY n
      CREATE RULE excluded AS MATCH (n) WHERE n IS NOT base YIELD KEY n
      """
    Then the program should compile successfully
    And the program should have 2 strata

  Scenario: Cyclic negation should fail to compile
    When compiling the following Locy program:
      """
      CREATE RULE a AS MATCH (n) WHERE n IS NOT b YIELD KEY n
      CREATE RULE b AS MATCH (n) WHERE n IS NOT a YIELD KEY n
      """
    Then the program should fail to compile
    And the compile error should mention 'cyclic negation'

  Scenario: Undefined rule reference should fail to compile
    When compiling the following Locy program:
      """
      CREATE RULE test AS MATCH (n) WHERE n IS nonexistent YIELD KEY n
      """
    Then the program should fail to compile
    And the compile error should mention 'undefined rule'

  Scenario: Program with no rules compiles to zero strata
    When compiling the following Locy program:
      """
      MATCH (n) RETURN n
      """
    Then the program should compile successfully
    And the program should have 0 strata
