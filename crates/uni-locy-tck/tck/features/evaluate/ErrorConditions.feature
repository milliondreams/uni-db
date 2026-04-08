Feature: Evaluation Error Conditions

  Tests runtime and compile-time error detection.

  Background:
    Given an empty graph

  # ── Compile errors ────────────────────────────────────────────────────

  Scenario: Undefined rule in IS reference
    When compiling the following Locy program:
      """
      CREATE RULE test AS MATCH (n) WHERE n IS nonexistent YIELD KEY n
      """
    Then the program should fail to compile
    And the compile error should mention 'undefined rule'

  Scenario: prev in base case clause rejected
    When compiling the following Locy program:
      """
      CREATE RULE bad AS
        MATCH (a)-[e:EDGE]->(b)
        ALONG cost = prev.cost + e.weight
        YIELD KEY a, KEY b, cost
      """
    Then the program should fail to compile
    And the compile error should mention 'prev'

  Scenario: YIELD schema mismatch rejected
    When compiling the following Locy program:
      """
      CREATE RULE mismatched AS MATCH (a)-[:EDGE]->(b) YIELD KEY a, KEY b
      CREATE RULE mismatched AS MATCH (x)-[:EDGE]->(y) YIELD KEY x, KEY y, x.name AS extra
      """
    Then the program should fail to compile
    And the compile error should mention 'schema mismatch'

  Scenario: Cyclic negation rejected
    When compiling the following Locy program:
      """
      CREATE RULE a AS MATCH (n) WHERE n IS NOT b YIELD KEY n
      CREATE RULE b AS MATCH (n) WHERE n IS NOT a YIELD KEY n
      """
    Then the program should fail to compile
    And the compile error should mention 'cyclic negation'

  # ── Runtime errors ────────────────────────────────────────────────────

  Scenario: Max iterations exceeded returns partial results
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'}), (b)-[:EDGE]->(a)
      """
    When evaluating the following Locy program with max_iterations 1:
      """
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(mid:Node) WHERE mid IS reachable TO b YIELD KEY a, KEY b
      """
    Then evaluation should succeed with timed_out true
