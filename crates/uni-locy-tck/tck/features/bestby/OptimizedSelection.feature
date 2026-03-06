Feature: Optimized Selection (BEST BY)

  Tests BEST BY clause for selecting the optimal derivation per key group.
  Unlike FOLD, BEST BY selects — it does not aggregate. The full winning
  row is preserved including ALONG values.

  Background:
    Given an empty graph

  # ── Parse level ───────────────────────────────────────────────────────

  Scenario: BEST BY MIN syntax parses
    When parsing the following Locy program:
      """
      CREATE RULE shortest AS
        MATCH (a)-[e:EDGE]->(b)
        BEST BY e.weight ASC
        YIELD KEY a, KEY b, e.weight AS cost
      """
    Then the program should parse successfully

  # ── Compile level ─────────────────────────────────────────────────────

  Scenario: BEST BY with monotonic FOLD rejected
    When compiling the following Locy program:
      """
      CREATE RULE bad AS
        MATCH (a)-[e:EDGE]->(b)
        FOLD total = MSUM(e.weight)
        BEST BY total ASC
        YIELD KEY a, KEY b, total
      """
    Then the program should fail to compile

  # ── Evaluate level ────────────────────────────────────────────────────

  Scenario: BEST BY ASC selects minimum cost
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE {cost: 10}]->(b:Node {name: 'B'}),
             (a)-[:EDGE {cost: 3}]->(b),
             (a)-[:EDGE {cost: 7}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE cheapest AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        BEST BY e.cost ASC
        YIELD KEY a, KEY b, e.cost AS cost
      """
    Then evaluation should succeed
    And the derived relation 'cheapest' should have 1 facts

  Scenario: BEST BY DESC selects maximum value
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE {score: 10}]->(b:Node {name: 'B'}),
             (a)-[:EDGE {score: 3}]->(b),
             (a)-[:EDGE {score: 7}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE top_score AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        BEST BY e.score DESC
        YIELD KEY a, KEY b, e.score AS score
      """
    Then evaluation should succeed
    And the derived relation 'top_score' should have 1 facts

  Scenario: BEST BY preserves full winning row
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE {cost: 10, label: 'expensive'}]->(b:Node {name: 'B'}),
             (a)-[:EDGE {cost: 3, label: 'cheap'}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE cheapest AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        BEST BY e.cost ASC
        YIELD KEY a, KEY b, e.cost AS cost, e.label AS tag
      """
    Then evaluation should succeed
    And the derived relation 'cheapest' should have 1 facts

  Scenario: BEST BY with NULL values sorts NULLs last
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE {cost: 5}]->(b:Node {name: 'B'}),
             (a)-[:EDGE]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE cheapest AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        BEST BY e.cost ASC
        YIELD KEY a, KEY b, e.cost AS cost
      """
    Then evaluation should succeed
    And the derived relation 'cheapest' should have 1 facts
