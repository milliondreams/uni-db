Feature: DERIVE Visibility — Trailing Cypher Sees Derived Edges

  Verifies that trailing Cypher commands within a Locy program can see
  edges materialized by preceding DERIVE commands via an internal
  isolated L0 buffer.

  Background:
    Given an empty graph
    And having executed:
      """
      CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})
      """

  # ── Core visibility: trailing Cypher after DERIVE ─────────────────────

  Scenario: Trailing Cypher after DERIVE sees derived edges
    When evaluating the following Locy program:
      """
      CREATE RULE link AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        DERIVE (a)-[:FRIEND]->(b)
      DERIVE link
      MATCH (a:Person)-[:FRIEND]->(b:Person)
      RETURN a.name AS src, b.name AS dst
      """
    Then evaluation should succeed
    And the command result 0 should be a Derive with at least 1 affected
    And the command result 1 should be a Cypher with at least 1 rows
    And the command result 1 should be a Cypher containing row where src = 'Alice'

  Scenario: Trailing Cypher count reflects derived edge count
    Given having executed:
      """
      MATCH (b:Person {name: 'Bob'})
      CREATE (b)-[:KNOWS]->(:Person {name: 'Carol'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE link AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        DERIVE (a)-[:FRIEND]->(b)
      DERIVE link
      MATCH ()-[r:FRIEND]->() RETURN count(r) AS cnt
      """
    Then evaluation should succeed
    And the command result 1 should be a Cypher with at least 1 rows
    And the command result 1 should be a Cypher containing row where cnt = 2

  # ── Join derived edges with existing graph ────────────────────────────

  Scenario: Trailing Cypher joins derived edges with existing graph
    Given having executed:
      """
      MATCH (b:Person {name: 'Bob'})
      CREATE (b)-[:KNOWS]->(:Person {name: 'Carol'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE link AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        DERIVE (a)-[:FRIEND]->(b)
      DERIVE link
      MATCH (a:Person)-[:FRIEND]->(b:Person)-[:KNOWS]->(c:Person)
      WHERE a <> c
      RETURN a.name AS src, c.name AS via_friend
      """
    Then evaluation should succeed
    And the command result 1 should be a Cypher with at least 1 rows

  # ── Multiple DERIVE commands ──────────────────────────────────────────

  Scenario: Trailing Cypher sees edges from multiple DERIVE commands
    When evaluating the following Locy program:
      """
      CREATE RULE friend AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        DERIVE (a)-[:FRIEND]->(b)
      CREATE RULE colleague AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        DERIVE (a)-[:COLLEAGUE]->(b)
      DERIVE friend
      DERIVE colleague
      MATCH (a:Person)-[:FRIEND]->(b:Person), (a)-[:COLLEAGUE]->(b)
      RETURN a.name AS src, b.name AS dst
      """
    Then evaluation should succeed
    And the command result 2 should be a Cypher with at least 1 rows

  # ── Interleaved QUERY + DERIVE + Cypher ───────────────────────────────

  Scenario: QUERY then DERIVE then trailing Cypher
    When evaluating the following Locy program:
      """
      CREATE RULE knows_rule AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        YIELD KEY a, KEY b
      CREATE RULE derive_friend AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        DERIVE (a)-[:FRIEND]->(b)
      QUERY knows_rule WHERE a = a RETURN a.name AS n
      DERIVE derive_friend
      MATCH ()-[r:FRIEND]->() RETURN count(r) AS cnt
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with at least 1 rows
    And the command result 1 should be a Derive with at least 1 affected
    And the command result 2 should be a Cypher with at least 1 rows

  # ── Isolation: derived edges do not persist ────────────────────────────

  Scenario: DERIVE edges do not persist to graph without tx.apply
    When evaluating the following Locy program without applying derived facts:
      """
      CREATE RULE link AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        DERIVE (a)-[:FRIEND]->(b)
      DERIVE link
      """
    Then evaluation should succeed
    And the graph should NOT contain an edge with type 'FRIEND'

  # ── Empty DERIVE ──────────────────────────────────────────────────────

  Scenario: Trailing Cypher after empty DERIVE returns 0 correctly
    When evaluating the following Locy program:
      """
      CREATE RULE link AS
        MATCH (a:Person)-[:WORKS_AT]->(b:Person)
        DERIVE (a)-[:FRIEND]->(b)
      DERIVE link
      MATCH ()-[r:FRIEND]->() RETURN count(r) AS cnt
      """
    Then evaluation should succeed
    And the command result 1 should be a Cypher with at least 1 rows
    And the command result 1 should be a Cypher containing row where cnt = 0
