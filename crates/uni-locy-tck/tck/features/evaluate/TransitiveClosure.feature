Feature: Transitive Closure Evaluation

  Tests recursive rule evaluation with fixpoint computation.

  Background:
    Given an empty graph

  Scenario: Three-node chain produces transitive closure
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'})-[:EDGE]->(c:Node {name: 'C'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(mid:Node) WHERE mid IS reachable TO b YIELD KEY a, KEY b
      """
    Then evaluation should succeed
    And the derived relation 'reachable' should have 3 facts
    And the derived relation 'reachable' should contain a fact where a.name = 'A' and b.name = 'C'

  Scenario: Cycle converges without infinite loop
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'}), (b)-[:EDGE]->(a)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(mid:Node) WHERE mid IS reachable TO b YIELD KEY a, KEY b
      """
    Then evaluation should succeed
    And the derived relation 'reachable' should have 4 facts
    And the derived relation 'reachable' should contain a fact where a.name = 'A' and b.name = 'A'
    And the derived relation 'reachable' should contain a fact where a.name = 'B' and b.name = 'B'
