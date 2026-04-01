Feature: FOLD Query Projection

  Tests that FOLD aggregation columns (MNOR, MPROD, SUM) are correctly projected
  through a plain QUERY ... RETURN command, not just through derived relations or
  ASSUME THEN blocks.

  Regression test: FOLD values returned None through plain QUERY because SLG
  resolution skipped post-fixpoint aggregation (apply_post_fixpoint_chain).

  Background:
    Given an empty graph

  # ── MNOR via plain QUERY ──────────────────────────────────────────────────

  Scenario: MNOR fold value projects through plain QUERY RETURN
    Given having executed:
      """
      CREATE (b:Node {name: 'B'}),
             (:Node {name: 'X'})-[:CAUSE {prob: 0.3}]->(b),
             (:Node {name: 'Y'})-[:CAUSE {prob: 0.5}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY b, p
      QUERY risk RETURN p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 1 facts
    And the derived relation 'risk' should contain a fact where p = 0.65
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where p = 0.65

  # ── MPROD via plain QUERY ─────────────────────────────────────────────────

  Scenario: MPROD fold value projects through plain QUERY RETURN
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:REQ {prob: 0.6}]->(b:Node {name: 'B'}),
             (a)-[:REQ {prob: 0.8}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE joint AS
        MATCH (a:Node)-[e:REQ]->(b:Node)
        FOLD p = MPROD(e.prob)
        YIELD KEY a, KEY b, p
      QUERY joint RETURN p
      """
    Then evaluation should succeed
    And the derived relation 'joint' should have 1 facts
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where p = 0.48

  # ── SUM via plain QUERY ───────────────────────────────────────────────────

  Scenario: SUM fold value projects through plain QUERY RETURN
    Given having executed:
      """
      CREATE (p:Person {name: 'Alice'})-[:PAID {amount: 100}]->(:Invoice {id: 'I1'}),
             (p)-[:PAID {amount: 200}]->(:Invoice {id: 'I2'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE spending AS
        MATCH (p:Person)-[r:PAID]->(i:Invoice)
        FOLD total = SUM(r.amount)
        YIELD KEY p, total
      QUERY spending RETURN total
      """
    Then evaluation should succeed
    And the derived relation 'spending' should have 1 facts
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where total = 300.0

  # ── Multi-group MNOR via plain QUERY ──────────────────────────────────────

  Scenario: MNOR with multiple groups returns all groups via plain QUERY
    Given having executed:
      """
      CREATE (b1:Node {name: 'B1'}),
             (b2:Node {name: 'B2'}),
             (:Node {name: 'X'})-[:CAUSE {prob: 0.5}]->(b1),
             (:Node {name: 'Y'})-[:CAUSE {prob: 0.5}]->(b1),
             (:Node {name: 'Z'})-[:CAUSE {prob: 0.8}]->(b2)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY b, p
      QUERY risk RETURN p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 2 facts
    And the derived relation 'risk' should contain a fact where p = 0.75
    And the derived relation 'risk' should contain a fact where p = 0.8
    And the command result 0 should be a Query with 2 rows
    And the command result 0 should be a Query containing row where p = 0.75
    And the command result 0 should be a Query containing row where p = 0.8
