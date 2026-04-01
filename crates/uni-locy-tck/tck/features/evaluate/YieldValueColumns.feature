Feature: YIELD Value Columns

  Tests that non-key YIELD columns (property accesses, computed expressions,
  literal constants, edge properties) are correctly materialized in both
  derived relations and QUERY RETURN results.

  This is the regression test for L-ROOT: "All non-key YIELD columns return None."

  Background:
    Given an empty graph

  # ── Property access in YIELD ─────────────────────────────────────────────

  Scenario: Non-key property access in YIELD via derived relation
    Given having executed:
      """
      CREATE (:Sensor {name: 'S1', val: 0.8}), (:Sensor {name: 'S2', val: 0.3})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reading AS MATCH (n:Sensor) YIELD KEY n, n.val AS v
      """
    Then evaluation should succeed
    And the derived relation 'reading' should have 2 facts
    And the derived relation 'reading' should contain a fact where n.name = 'S1' and v = 0.8
    And the derived relation 'reading' should contain a fact where n.name = 'S2' and v = 0.3

  Scenario: Non-key property access in QUERY RETURN
    Given having executed:
      """
      CREATE (:Sensor {name: 'S1', val: 0.8}), (:Sensor {name: 'S2', val: 0.3})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reading AS MATCH (n:Sensor) YIELD KEY n, n.val AS v
      QUERY reading WHERE n.name = 'S1' RETURN n.name AS nid, v
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where nid = 'S1'
    And the command result 0 should be a Query containing row where v = 0.8

  # ── Literal constant in YIELD ────────────────────────────────────────────

  Scenario: Literal constant in YIELD
    Given having executed:
      """
      CREATE (:Node {name: 'A'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE tagged AS MATCH (n:Node) YIELD KEY n, 0.5 AS score
      QUERY tagged WHERE n.name = 'A' RETURN n.name AS nid, score
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where score = 0.5

  # ── String constant in YIELD ─────────────────────────────────────────────

  Scenario: String constant in YIELD via QUERY RETURN
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE tagged AS MATCH (n:Node) YIELD KEY n, 'patch-now' AS action
      QUERY tagged WHERE n.name = 'A' RETURN n.name AS nid, action
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where nid = 'A'
    And the command result 0 should be a Query containing row where action = 'patch-now'

  # ── Computed expression in YIELD ─────────────────────────────────────────

  Scenario: Computed expression in YIELD
    Given having executed:
      """
      CREATE (:Sensor {name: 'S1', val: 0.8})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE severity AS MATCH (n:Sensor) YIELD KEY n, 1.0 - n.val AS sev
      QUERY severity WHERE n.name = 'S1' RETURN n.name AS nid, sev
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where nid = 'S1'

  # ── Edge property in YIELD ───────────────────────────────────────────────

  Scenario: Edge property access in YIELD
    Given having executed:
      """
      CREATE (:Device {name: 'D1'})-[:LINK {weight: 0.9}]->(:Device {name: 'D2'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE connected AS
        MATCH (a:Device)-[e:LINK]->(b:Device)
        YIELD KEY a, KEY b, e.weight AS w
      QUERY connected WHERE a.name = 'D1' RETURN a.name AS src, b.name AS dst, w
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where src = 'D1'
    And the command result 0 should be a Query containing row where w = 0.9
