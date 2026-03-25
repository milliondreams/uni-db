Feature: Cypher Functions in Locy Expressions

  Locy is a superset of Cypher: every function valid in a Cypher expression
  must also be valid inside a Locy WHERE, YIELD (QUERY), or FOLD expression.

  The key architectural note: Locy has two evaluation paths.
  - Rule YIELD/WHERE → DataFusion physical expressions (via CypherPhysicalExprCompiler)
  - QUERY WHERE/RETURN → in-memory evaluator (locy_eval.rs)

  These tests guard the in-memory evaluator path (QUERY WHERE/RETURN) where
  functions are called on already-typed Value bindings from the derived relation.
  The pattern is: YIELD a numeric property as 'v', then call the function on 'v'
  inside QUERY WHERE or RETURN.

  Background:
    Given an empty graph

  # ── Math functions in QUERY WHERE ────────────────────────────────────────
  # sqrt(v), exp(v), log(v) are called inside the in-memory evaluator (QUERY).
  # 'v' is a Float already materialized in the derived relation.

  Scenario: sqrt() in QUERY WHERE filters correctly
    Given having executed:
      """
      CREATE (:Item {name: 'a', val: 4.0}), (:Item {name: 'b', val: 9.0})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE result AS MATCH (n:Item) YIELD KEY n, n.val AS v
      QUERY result WHERE sqrt(v) > 2.5 RETURN n.name AS id
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where id = 'b'

  Scenario: exp() in QUERY WHERE filters correctly
    Given having executed:
      """
      CREATE (:Item {name: 'zero', val: 0.0}), (:Item {name: 'one', val: 1.0})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE result AS MATCH (n:Item) YIELD KEY n, n.val AS v
      QUERY result WHERE exp(v) > 1.5 RETURN n.name AS id
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where id = 'one'

  Scenario: log() in QUERY WHERE filters correctly
    Given having executed:
      """
      CREATE (:Item {name: 'small', val: 1.0}), (:Item {name: 'big', val: 100.0})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE result AS MATCH (n:Item) YIELD KEY n, n.val AS v
      QUERY result WHERE log(v) > 3.0 RETURN n.name AS id
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where id = 'big'

  # ── Math functions in QUERY RETURN ───────────────────────────────────────
  # The function is called in the RETURN projection (still in-memory evaluator).

  Scenario: exp(0) = 1.0 in QUERY RETURN
    Given having executed:
      """
      CREATE (:Item {name: 'x', val: 0.0})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE result AS MATCH (n:Item) YIELD KEY n, n.val AS v
      QUERY result WHERE n.name = 'x' RETURN n.name AS id, exp(v) AS e
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where e = 1.0

  Scenario: sqrt(4) = 2.0 in QUERY RETURN
    Given having executed:
      """
      CREATE (:Item {name: 'y', val: 4.0})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE result AS MATCH (n:Item) YIELD KEY n, n.val AS v
      QUERY result WHERE n.name = 'y' RETURN n.name AS id, sqrt(v) AS s
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where s = 2.0

  # ── String functions in QUERY WHERE and RETURN ───────────────────────────

  Scenario: toUpper() in QUERY RETURN
    Given having executed:
      """
      CREATE (:Tag {name: 'hello'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE result AS MATCH (n:Tag) YIELD KEY n, n.name AS raw
      QUERY result RETURN toUpper(raw) AS upper
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where upper = 'HELLO'

  Scenario: toLower() in QUERY WHERE
    Given having executed:
      """
      CREATE (:Tag {name: 'MATCH_ME'}), (:Tag {name: 'SKIP_ME'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE result AS MATCH (n:Tag) YIELD KEY n, n.name AS raw
      QUERY result WHERE toLower(raw) = 'match_me' RETURN raw AS id
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where id = 'MATCH_ME'

  # ── Temporal functions ────────────────────────────────────────────────────

  Scenario: datetime() in QUERY RETURN does not error
    Given having executed:
      """
      CREATE (:Event {name: 'e1'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE result AS MATCH (n:Event) YIELD KEY n, n.name AS nm
      QUERY result RETURN toString(datetime()) AS ts
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows

  # ── Relevance decay pattern (exp + WHERE on QUERY) ───────────────────────
  # This is the motivating use case from the issue: apply exp-based decay
  # to a pre-materialized score value and filter by threshold.

  Scenario: exp() decay pattern in QUERY WHERE models relevance decay
    Given having executed:
      """
      CREATE (:Episode {eid: 'ep1', importance: 1.0, age_days: 0.0}),
             (:Episode {eid: 'ep2', importance: 0.5, age_days: 100.0})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE scored AS
        MATCH (e:Episode)
        YIELD KEY e, e.importance AS imp, e.age_days AS age_d
      QUERY scored WHERE imp * exp(-0.05 * age_d) > 0.1 RETURN e.eid AS id
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where id = 'ep1'
