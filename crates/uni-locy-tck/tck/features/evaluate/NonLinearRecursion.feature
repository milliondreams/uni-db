Feature: Non-Linear Recursion and Multiple IS-References

  Tests clauses with two positive IS-references: non-linear recursion
  (two self-references joined in one clause) and chained cross-rule
  references. Regression coverage for the derived-scan column-collision
  bug and the semi-naive delta-only injection gap (both silently
  under-derived before being fixed).

  Background:
    Given an empty graph

  Scenario: Non-linear transitive closure matches the linear formulation
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'})-[:EDGE]->(c:Node {name: 'C'})-[:EDGE]->(d:Node {name: 'D'})-[:EDGE]->(e:Node {name: 'E'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      CREATE RULE reachable AS MATCH (a:Node) WHERE a IS reachable TO mid, mid IS reachable TO b YIELD KEY a, KEY b
      """
    Then evaluation should succeed
    And the derived relation 'reachable' should have 10 facts
    And the derived relation 'reachable' should contain a fact where a.name = 'A' and b.name = 'D'
    And the derived relation 'reachable' should contain a fact where a.name = 'B' and b.name = 'E'
    And the derived relation 'reachable' should contain a fact where a.name = 'A' and b.name = 'E'

  Scenario: Two chained IS-references to another rule in one clause
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'})-[:EDGE]->(c:Node {name: 'C'})-[:EDGE]->(d:Node {name: 'D'})-[:EDGE]->(e:Node {name: 'E'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE hop AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      CREATE RULE two_hop AS MATCH (a:Node) WHERE a IS hop TO mid, mid IS hop TO b YIELD KEY a, KEY b
      """
    Then evaluation should succeed
    And the derived relation 'two_hop' should have 3 facts
    And the derived relation 'two_hop' should contain a fact where a.name = 'A' and b.name = 'C'
    And the derived relation 'two_hop' should contain a fact where a.name = 'C' and b.name = 'E'
