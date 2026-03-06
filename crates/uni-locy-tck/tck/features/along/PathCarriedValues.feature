Feature: ALONG Path-Carried Values

  Tests parsing of ALONG clauses with prev references for
  accumulating values along recursive paths.

  Background:
    Given an empty graph

  Scenario: ALONG with prev reference
    When parsing the following Locy program:
      """
      CREATE RULE hops AS MATCH (a)-[:E]->(b) ALONG count = prev.count + 1 YIELD KEY a, KEY b, count
      """
    Then the program should parse successfully

  Scenario: ALONG with arithmetic expression
    When parsing the following Locy program:
      """
      CREATE RULE cost AS MATCH (a)-[e:E]->(b) ALONG total = prev.total + e.weight YIELD KEY a, KEY b, total
      """
    Then the program should parse successfully

  Scenario: Multiple ALONG variables
    When parsing the following Locy program:
      """
      CREATE RULE metrics AS MATCH (a)-[e:E]->(b) ALONG dist = prev.dist + e.len, hops = prev.hops + 1 YIELD KEY a, KEY b, dist, hops
      """
    Then the program should parse successfully

  Scenario: ALONG combined with FOLD
    When parsing the following Locy program:
      """
      CREATE RULE total AS MATCH (a)-[e:E]->(b) ALONG cost = prev.cost + e.weight FOLD total = SUM(cost) YIELD KEY a, total
      """
    Then the program should parse successfully

  # ── Evaluate-level scenarios ──────────────────────────────────────────

  # ── Evaluate-level scenarios ──────────────────────────────────────────

  Scenario: ALONG hop count accumulates across hops
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'})-[:EDGE]->(c:Node {name: 'C'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(b:Node)
        ALONG hops = 1
        YIELD KEY a, KEY b, hops
      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(mid:Node)
        WHERE mid IS reachable TO b
        ALONG hops = prev.hops + 1
        YIELD KEY a, KEY b, hops
      """
    Then evaluation should succeed
    And the derived relation 'reachable' should have 3 facts
    And the derived relation 'reachable' should contain a fact where a.name = 'A' and b.name = 'C'

  Scenario: ALONG weighted cost accumulates edge weights
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE {weight: 5.0}]->(b:Node {name: 'B'})-[:EDGE {weight: 3.0}]->(c:Node {name: 'C'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE cheapest AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        ALONG cost = e.weight
        YIELD KEY a, KEY b, cost
      CREATE RULE cheapest AS
        MATCH (a:Node)-[e:EDGE]->(mid:Node)
        WHERE mid IS cheapest TO b
        ALONG cost = prev.cost + e.weight
        YIELD KEY a, KEY b, cost
      """
    Then evaluation should succeed
    And the derived relation 'cheapest' should contain a fact where a.name = 'A' and b.name = 'C'

  Scenario: Multiple ALONG variables carried independently
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE {weight: 5.0}]->(b:Node {name: 'B'})-[:EDGE {weight: 3.0}]->(c:Node {name: 'C'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE path AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        ALONG dist = e.weight, hops = 1
        YIELD KEY a, KEY b, dist, hops
      CREATE RULE path AS
        MATCH (a:Node)-[e:EDGE]->(mid:Node)
        WHERE mid IS path TO b
        ALONG dist = prev.dist + e.weight, hops = prev.hops + 1
        YIELD KEY a, KEY b, dist, hops
      """
    Then evaluation should succeed
    And the derived relation 'path' should contain a fact where a.name = 'A' and b.name = 'C'

  Scenario: ALONG combined with FOLD SUM
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE {weight: 5.0}]->(b:Node {name: 'B'}),
             (a)-[:EDGE {weight: 3.0}]->(c:Node {name: 'C'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE outgoing AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        FOLD total = SUM(e.weight)
        YIELD KEY a, total
      """
    Then evaluation should succeed
    And the derived relation 'outgoing' should have 1 facts
