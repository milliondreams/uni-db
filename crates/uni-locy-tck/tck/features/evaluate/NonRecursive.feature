Feature: Non-Recursive Evaluation

  Tests end-to-end evaluation of non-recursive Locy programs
  against a real database.

  Background:
    Given an empty graph

  Scenario: Simple YIELD rule with graph data
    Given having executed:
      """
      CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE friends AS MATCH (a:Person)-[:KNOWS]->(b:Person) YIELD KEY a, KEY b
      """
    Then evaluation should succeed
    And the derived relation 'friends' should have 1 facts
    And the derived relation 'friends' should contain a fact where a.name = 'Alice' and b.name = 'Bob'

  Scenario: Empty result when no matching data
    When evaluating the following Locy program:
      """
      CREATE RULE friends AS MATCH (a:Person)-[:KNOWS]->(b:Person) YIELD KEY a, KEY b
      """
    Then evaluation should succeed
    And the derived relation 'friends' should have 0 facts

  Scenario: Multiple rules in single program
    Given having executed:
      """
      CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE friends AS MATCH (a:Person)-[:KNOWS]->(b:Person) YIELD KEY a, KEY b
      CREATE RULE people AS MATCH (n:Person) YIELD KEY n
      """
    Then evaluation should succeed
    And the derived relation 'friends' should have 1 facts
    And the derived relation 'people' should have 2 facts
    And the derived relation 'people' should contain a fact where n.name = 'Alice'
    And the derived relation 'people' should contain a fact where n.name = 'Bob'

  Scenario: Rule with WHERE condition filters results
    Given having executed:
      """
      CREATE (:Person {name: 'Alice', age: 30}), (:Person {name: 'Bob', age: 15})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE adults AS MATCH (n:Person) WHERE n.age >= 18 YIELD KEY n
      """
    Then evaluation should succeed
    And the derived relation 'adults' should have 1 facts
    And the derived relation 'adults' should contain a fact where n.name = 'Alice'
    And the derived relation 'adults' should not contain a fact where n.name = 'Bob'
