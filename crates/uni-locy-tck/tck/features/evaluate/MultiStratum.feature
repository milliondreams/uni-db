Feature: Multi-Stratum Evaluation

  Tests evaluation of programs where rules span multiple strata,
  verifying that strata are evaluated in correct dependency order.

  Background:
    Given an empty graph

  Scenario: Two-stratum positive dependency
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'})-[:EDGE]->(c:Node {name: 'C'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(mid:Node) WHERE mid IS reachable TO b YIELD KEY a, KEY b
      CREATE RULE endpoints AS MATCH (n:Node) WHERE n IS reachable TO m, m.name = 'C' YIELD KEY n
      """
    Then evaluation should succeed
    And the derived relation 'reachable' should have 3 facts
    And the derived relation 'endpoints' should contain at least 1 facts

  Scenario: Three-stratum with negation
    Given having executed:
      """
      CREATE (:Node {name: 'A', risk: 0.8}),
             (:Node {name: 'B', risk: 0.2}),
             (:Node {name: 'C', risk: 0.1})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risky AS MATCH (n:Node) WHERE n.risk > 0.5 YIELD KEY n
      CREATE RULE safe AS MATCH (n:Node) WHERE n IS NOT risky YIELD KEY n
      CREATE RULE trusted AS MATCH (n:Node) WHERE n IS safe, n.risk < 0.15 YIELD KEY n
      """
    Then evaluation should succeed
    And the derived relation 'risky' should have 1 facts
    And the derived relation 'safe' should have 2 facts
    And the derived relation 'trusted' should have 1 facts

  Scenario: Cross-rule IS reference with value binding
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'}),
             (b)-[:EDGE]->(c:Node {name: 'C'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE knows AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      CREATE RULE indirect AS MATCH (a:Node) WHERE a IS knows TO b YIELD KEY a, KEY b
      """
    Then evaluation should succeed
    And the derived relation 'knows' should have 2 facts
    And the derived relation 'indirect' should have 2 facts
