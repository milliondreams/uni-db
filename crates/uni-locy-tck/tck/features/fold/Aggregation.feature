Feature: FOLD Aggregation

  Tests parsing of FOLD clauses for post-fixpoint aggregation.

  Background:
    Given an empty graph

  Scenario: FOLD with SUM
    When parsing the following Locy program:
      """
      CREATE RULE totals AS MATCH (a)-[:E]->(b) FOLD total = SUM(b.value) YIELD KEY a, total
      """
    Then the program should parse successfully

  Scenario: FOLD with BEST BY
    When parsing the following Locy program:
      """
      CREATE RULE shortest AS MATCH (a)-[e:E]->(b) ALONG dist = prev.dist + e.weight BEST BY dist ASC YIELD KEY a, KEY b, dist
      """
    Then the program should parse successfully

  Scenario: FOLD with MSUM monotonic aggregate
    When parsing the following Locy program:
      """
      CREATE RULE running AS MATCH (a)-[:E]->(b) FOLD acc = MSUM(b.value) YIELD KEY a, acc
      """
    Then the program should parse successfully

  # ── Evaluate-level scenarios ──────────────────────────────────────────

  Scenario: FOLD SUM groups by key correctly
    Given having executed:
      """
      CREATE (a:Person {name: 'Alice'})-[:PAID {amount: 100}]->(:Invoice),
             (a)-[:PAID {amount: 200}]->(:Invoice),
             (b:Person {name: 'Bob'})-[:PAID {amount: 50}]->(:Invoice)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE spending AS
        MATCH (p:Person)-[r:PAID]->(i:Invoice)
        FOLD total = SUM(r.amount)
        YIELD KEY p, total
      """
    Then evaluation should succeed
    And the derived relation 'spending' should have 2 facts

  Scenario: FOLD COUNT counts matching rows
    Given having executed:
      """
      CREATE (:Person {name: 'Alice'}), (:Person {name: 'Bob'}), (:Person {name: 'Carol'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE census AS
        MATCH (n:Person)
        FOLD cnt = COUNT(n)
        YIELD cnt
      """
    Then evaluation should succeed
    And the derived relation 'census' should have 1 facts

  Scenario: BEST BY MIN selects cheapest per group
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE {cost: 5}]->(b:Node {name: 'B'}),
             (a)-[:EDGE {cost: 3}]->(c:Node {name: 'C'}),
             (a)-[:EDGE {cost: 7}]->(d:Node {name: 'D'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE cheapest_neighbor AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        BEST BY e.cost ASC
        YIELD KEY a, b, e.cost AS cost
      """
    Then evaluation should succeed
    And the derived relation 'cheapest_neighbor' should have 1 facts

  Scenario: BEST BY preserves full row including ALONG values
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE {weight: 5.0}]->(b:Node {name: 'B'})-[:EDGE {weight: 3.0}]->(c:Node {name: 'C'}),
             (a)-[:EDGE {weight: 20.0}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE shortest AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        ALONG cost = e.weight
        BEST BY cost ASC
        YIELD KEY a, KEY b, cost
      CREATE RULE shortest AS
        MATCH (a:Node)-[e:EDGE]->(mid:Node)
        WHERE mid IS shortest TO b
        ALONG cost = prev.cost + e.weight
        BEST BY cost ASC
        YIELD KEY a, KEY b, cost
      """
    Then evaluation should succeed
    And the derived relation 'shortest' should contain a fact where a.name = 'A' and b.name = 'C'
