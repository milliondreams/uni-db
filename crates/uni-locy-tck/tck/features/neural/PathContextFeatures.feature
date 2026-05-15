Feature: Path-context features (Phase D D3 MVP)

  Phase D D3 lets a `CREATE MODEL` declaration pull a column from a
  prior-derived rule:

      FEATURES (subject, column) FROM source_rule

  This MVP ships the surface, compiler validation, and stratifier
  ordering: any rule that invokes a model with a path-context
  feature gains an implicit positive dependency on the source rule
  (so the invoking rule's stratum runs strictly after `source_rule`
  is materialized). The runtime join itself is pending — pulling
  the column at row time requires threading `DerivedScanRegistry`
  into `apply_model_invocations`, which is a separate plumbing
  step. The runtime surfaces a clear error pointing to the
  workaround.

  Background:
    Given an empty graph

  # ── Grammar + AST: the new syntax parses and compiles ───────────────────

  Scenario: FEATURES (subject, col) FROM rule parses successfully
    Given having executed:
      """
      CREATE (:Supplier {name: 'A'})
      """
    And a registered mock classifier "risk_model" returning 0.5
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE RULE supply_path AS
        MATCH (s:Supplier)
        YIELD KEY s, 0.42 AS path_risk

      CREATE MODEL risk_model AS
        INPUT (s)
        FEATURES (s, path_risk) FROM supply_path
        OUTPUT PROB risk
        USING xervo('classify/risk_model')

      CREATE RULE risky AS
        MATCH (s:Supplier)
        YIELD KEY s, risk_model(s) AS risk
      """
    # MVP: runtime errors clearly. The compile + stratification
    # work succeeds; only the per-row join is pending.
    Then evaluation should fail
    And the evaluation error should mention "FROM supply_path"

  # ── Undefined source rule → compile-time error ──────────────────────────

  Scenario: FEATURES FROM unknown_rule fails at compile time
    Given having executed:
      """
      CREATE (:Supplier {name: 'A'})
      """
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL risk_model AS
        INPUT (s)
        FEATURES (s, path_risk) FROM ghost_rule
        OUTPUT PROB risk
        USING xervo('classify/risk_model')

      CREATE RULE risky AS
        MATCH (s:Supplier)
        YIELD KEY s, risk_model(s) AS risk
      """
    Then evaluation should fail
    And the evaluation error should mention "ghost_rule"
