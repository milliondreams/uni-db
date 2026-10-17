Feature: Cross-group correlation warnings (Phase D F3)

  Phase D F3 broadens `CrossGroupCorrelationNotExact` emission.
  Shipped:
  - `key_group` field on the existing runtime warning (case 1)
    — exercised implicitly by scenarios that fire the warning.
  - Case 3 structural detector: a clause with both `IS p` and
    `IS NOT q` on the same subject emits
    `PositiveComplementCorrelation` at compile time (over-detects;
    a future slice will refine to runtime support-set overlap).
  - Case 2 structural detector: a clause with two or more positive
    `IS` references to *different* PROB-bearing rules on the *same*
    subject emits `CrossPredicateCorrelation` at compile time.
    The same "louder is safer" over-detection trade-off as case 3.

  Background:
    Given an empty graph

  # ── Case 3 fires when positive + complement share subject ───────────────

  Scenario: WHERE s IS p, s IS NOT q emits PositiveComplementCorrelation
    Given having executed:
      """
      CREATE (:Supplier {name: 'A'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE p AS
        MATCH (s:Supplier)
        YIELD KEY s

      CREATE RULE q AS
        MATCH (s:Supplier {name: 'B'})
        YIELD KEY s

      CREATE RULE r AS
        MATCH (s:Supplier)
        WHERE s IS p, s IS NOT q
        YIELD KEY s
      """
    Then evaluation should succeed
    And the result should contain a PositiveComplementCorrelation warning

  # ── No warning when subjects differ ─────────────────────────────────────

  Scenario: WHERE on different subjects does not emit the warning
    Given having executed:
      """
      CREATE (:Supplier {name: 'A'}), (:Item {name: 'X'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE p AS
        MATCH (s:Supplier)
        YIELD KEY s

      CREATE RULE q AS
        MATCH (i:Item)
        YIELD KEY i

      CREATE RULE r AS
        MATCH (s:Supplier), (i:Item)
        WHERE s IS p, i IS NOT q
        YIELD KEY s, KEY i
      """
    Then evaluation should succeed
    And the result should not contain a PositiveComplementCorrelation warning

  # ── Case 2: two positive PROB IS-refs on same subject ──────────────────

  Scenario: WHERE s IS p, s IS q on PROB rules emits CrossPredicateCorrelation
    Given having executed:
      """
      CREATE (:Supplier {name: 'A'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE p AS
        MATCH (s:Supplier)
        FOLD prob = MNOR(0.7)
        YIELD KEY s, prob

      CREATE RULE q AS
        MATCH (s:Supplier)
        FOLD prob = MNOR(0.3)
        YIELD KEY s, prob

      CREATE RULE r AS
        MATCH (s:Supplier)
        WHERE s IS p, s IS q
        YIELD KEY s
      """
    Then evaluation should succeed
    And the result should contain a CrossPredicateCorrelation warning

  # ── Case 2: one PROB + one non-PROB → no warning ───────────────────────

  Scenario: Only one PROB IS-ref does not emit CrossPredicateCorrelation
    Given having executed:
      """
      CREATE (:Supplier {name: 'A'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE p AS
        MATCH (s:Supplier)
        FOLD prob = MNOR(0.7)
        YIELD KEY s, prob

      CREATE RULE q AS
        MATCH (s:Supplier)
        YIELD KEY s

      CREATE RULE r AS
        MATCH (s:Supplier)
        WHERE s IS p, s IS q
        YIELD KEY s
      """
    Then evaluation should succeed
    And the result should not contain a CrossPredicateCorrelation warning

  # ── Case 2: two PROB IS-refs on different subjects → no warning ────────

  Scenario: Different subjects do not emit CrossPredicateCorrelation
    Given having executed:
      """
      CREATE (:Supplier {name: 'A'}), (:Item {name: 'X'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE p AS
        MATCH (s:Supplier)
        FOLD prob = MNOR(0.7)
        YIELD KEY s, prob

      CREATE RULE q AS
        MATCH (i:Item)
        FOLD prob = MNOR(0.3)
        YIELD KEY i, prob

      CREATE RULE r AS
        MATCH (s:Supplier), (i:Item)
        WHERE s IS p, i IS q
        YIELD KEY s, KEY i
      """
    Then evaluation should succeed
    And the result should not contain a CrossPredicateCorrelation warning

  # ── Reading a column two references expose is ambiguous (issue #300) ───
  #
  # The scenarios above expose `prob` through both `p` and `q` without
  # reading it, which stays legal. Reading it names no single reference, so
  # it is a compile error in every conjunct order; it used to resolve to
  # whichever reference came first in the WHERE.

  Scenario: Reading a value column two IS references expose fails to compile
    When compiling the following Locy program:
      """
      CREATE RULE p AS
        MATCH (s:Supplier)
        FOLD prob = MNOR(0.7)
        YIELD KEY s, prob

      CREATE RULE q AS
        MATCH (s:Supplier)
        FOLD prob = MNOR(0.3)
        YIELD KEY s, prob

      CREATE RULE r AS
        MATCH (s:Supplier)
        WHERE prob > 0.5, s IS q, s IS p
        YIELD KEY s
      """
    Then the program should fail to compile
    And the compile error should mention 'ambiguous'
    And the compile error should mention 'prob'

  Scenario: Binding each exposed column positionally resolves the read
    Given having executed:
      """
      CREATE (:Supplier {name: 'A'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE p AS
        MATCH (s:Supplier)
        FOLD prob = MNOR(0.7)
        YIELD KEY s, prob

      CREATE RULE q AS
        MATCH (s:Supplier)
        FOLD prob = MNOR(0.3)
        YIELD KEY s, prob

      CREATE RULE r AS
        MATCH (s:Supplier)
        WHERE pp > 0.5, (s, pq) IS q, (s, pp) IS p
        YIELD KEY s, pp, pq
      """
    Then evaluation should succeed
    And the derived relation 'r' should contain a fact where pp is approximately 0.7 within 0.000001
