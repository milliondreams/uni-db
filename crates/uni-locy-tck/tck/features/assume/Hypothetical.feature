Feature: ASSUME Hypothetical Reasoning

  Tests parsing of ASSUME blocks for hypothetical what-if analysis.

  Background:
    Given an empty graph

  Scenario: ASSUME with CREATE mutation
    When parsing the following Locy program:
      """
      ASSUME { CREATE (x:Temp) } THEN { MATCH (n) RETURN n }
      """
    Then the program should parse successfully

  Scenario: ASSUME THEN with rule evaluation
    When parsing the following Locy program:
      """
      ASSUME { CREATE (a)-[:EDGE]->(b) } THEN { QUERY reachable WHERE a.name = 'Alice' RETURN b }
      """
    Then the program should parse successfully

  Scenario: ASSUME THEN with Cypher body
    When parsing the following Locy program:
      """
      ASSUME { CREATE (x:Test {name: 'temp'}) } THEN { MATCH (n:Test) RETURN n.name }
      """
    Then the program should parse successfully

  # ── Evaluate-level scenarios ──────────────────────────────────────────

  Scenario: ASSUME mutation is rolled back after evaluation
    Given having executed:
      """
      CREATE (:Person {name: 'Alice'}), (:Person {name: 'Bob'})
      """
    When evaluating the following Locy program:
      """
      ASSUME { CREATE (:Person {name: 'Carol'}) }
      THEN { MATCH (n:Person) RETURN n.name AS name }
      """
    Then evaluation should succeed
    And the command result 0 should be an Assume with 3 rows
    And the graph should contain 2 nodes with label 'Person'

  Scenario: ASSUME DELETE is rolled back
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'})
      """
    When evaluating the following Locy program:
      """
      ASSUME { MATCH ()-[r:EDGE]->() DELETE r }
      THEN { MATCH ()-[r:EDGE]->() RETURN r }
      """
    Then evaluation should succeed
    And the command result 0 should be an Assume with 0 rows
    And the graph should contain an edge from 'A' to 'B' with type 'EDGE'

  Scenario: ASSUME with rule re-evaluation under hypothesis
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(mid:Node) WHERE mid IS reachable TO b YIELD KEY a, KEY b
      ASSUME { CREATE (b:Node {name: 'B'})-[:EDGE]->(c:Node {name: 'C'}) }
      THEN { QUERY reachable WHERE a.name = 'A' RETURN b.name AS person }
      """
    Then evaluation should succeed
