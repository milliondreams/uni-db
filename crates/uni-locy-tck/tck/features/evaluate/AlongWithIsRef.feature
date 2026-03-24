Feature: ALONG with IS-ref Coexistence

  Tests that ALONG declarations and IS-ref conditions can be used together
  in the same clause without schema errors (L-3 regression).

  Background:
    Given an empty graph

  # ── Simple ALONG + IS-ref ──────────────────────────────────────────────

  Scenario: ALONG binding directly as YIELD column with IS-ref
    Given having executed:
      """
      CREATE (:Node {name: 'A'})-[:EDGE {weight: 0.9}]->(b:Node {name: 'B'})-[:EDGE {weight: 0.7}]->(:Node {name: 'C'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reach AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        YIELD KEY a, KEY b

      CREATE RULE path AS
        MATCH (x:Node)-[e:EDGE]->(y:Node)
        WHERE y IS reach TO z
        ALONG ew = e.weight
        YIELD KEY x, KEY z, ew

      QUERY path WHERE x.name = 'A' RETURN x.name AS src, z.name AS dst, ew
      """
    Then evaluation should succeed
    And the derived relation 'path' should have 1 facts
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where src = 'A'
    And the command result 0 should be a Query containing row where ew = 0.9

  # ── ALONG in compound YIELD expression ─────────────────────────────────

  Scenario: ALONG binding used in compound YIELD expression with IS-ref
    Given having executed:
      """
      CREATE (:Node {name: 'A'})-[:EDGE {weight: 0.9}]->(b:Node {name: 'B'})-[:EDGE {weight: 0.7}]->(:Node {name: 'C'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reach AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        YIELD KEY a, KEY b

      CREATE RULE path AS
        MATCH (x:Node)-[e:EDGE]->(y:Node)
        WHERE y IS reach TO z
        ALONG ew = e.weight
        YIELD KEY x, KEY z, ew * 2.0 AS score

      QUERY path WHERE x.name = 'A' RETURN x.name AS src, z.name AS dst, score
      """
    Then evaluation should succeed
    And the derived relation 'path' should have 1 facts
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where src = 'A'

  # ── ALONG with aliased YIELD ───────────────────────────────────────────

  Scenario: ALONG binding with alias in YIELD with IS-ref
    Given having executed:
      """
      CREATE (:Node {name: 'A'})-[:EDGE {weight: 0.9}]->(b:Node {name: 'B'})-[:EDGE {weight: 0.7}]->(:Node {name: 'C'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reach AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        YIELD KEY a, KEY b

      CREATE RULE path AS
        MATCH (x:Node)-[e:EDGE]->(y:Node)
        WHERE y IS reach TO z
        ALONG ew = e.weight
        YIELD KEY x, KEY z, ew AS link_weight

      QUERY path WHERE x.name = 'A' RETURN x.name AS src, z.name AS dst, link_weight
      """
    Then evaluation should succeed
    And the derived relation 'path' should have 1 facts
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where link_weight = 0.9
