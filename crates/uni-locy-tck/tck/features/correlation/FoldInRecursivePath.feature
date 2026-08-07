Feature: FoldInRecursivePath compile warning (Semantic Stress Corpus B3)

  When a recursive rule (an IS-reference to a rule in the same SCC)
  carries a FOLD clause but no ALONG, the compiler emits
  `FoldInRecursivePath`. A recursive FOLD rolls up per KEY, composing one
  level at a time — a self-reference reads the target's folded value
  (issue #162). That is a legitimate and common shape, so the warning
  flags a choice rather than a defect: authors who wanted a value
  accumulated along each PATH want ALONG instead.

  Background:
    Given an empty graph

  # ── Recursive rule with FOLD but no ALONG → warning ──────────────────

  Scenario: Recursive IS-ref + FOLD + no ALONG emits FoldInRecursivePath
    Given having executed:
      """
      CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(b:Node)
        FOLD risk = MNOR(0.9)
        YIELD KEY a, KEY b, risk

      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(mid:Node)
        WHERE mid IS reachable TO b
        FOLD risk = MNOR(0.9)
        YIELD KEY a, KEY b, risk
      """
    Then evaluation should succeed
    And the result should contain a FoldInRecursivePath warning

  # ── Recursive + ALONG still warns ─────────────────────────────────────
  #
  # The scenario below used to be titled "... ALONG suppresses the warning",
  # which never matched the compiler: `phase_b_f1_fires_even_when_along_present`
  # asserts it fires regardless. The scenario asserted only that evaluation
  # succeeded, so the mismatch went unnoticed. It now asserts what actually
  # happens.

  Scenario: Recursive IS-ref + FOLD + ALONG still emits FoldInRecursivePath
    Given having executed:
      """
      CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(b:Node)
        ALONG step = 0.9
        FOLD risk = MNOR(step)
        YIELD KEY a, KEY b, risk

      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(mid:Node)
        WHERE mid IS reachable TO b
        ALONG step = 0.9
        FOLD risk = MNOR(step)
        YIELD KEY a, KEY b, risk
      """
    Then evaluation should succeed
    And the result should contain a FoldInRecursivePath warning

  # ── Non-recursive rule with FOLD but no ALONG → no warning ───────────

  Scenario: Non-recursive FOLD without ALONG does not warn
    Given having executed:
      """
      CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE edge_risk AS
        MATCH (a:Node)-[:EDGE]->(b:Node)
        FOLD risk = MNOR(0.9)
        YIELD KEY a, KEY b, risk
      """
    Then evaluation should succeed
