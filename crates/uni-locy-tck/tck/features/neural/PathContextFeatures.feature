Feature: Path-context features (Phase D D3)

  `FEATURES (subject, column) FROM source_rule` in a `CREATE MODEL`
  declaration pulls `column` from the prior derivation of
  `source_rule` (keyed by `subject`) at runtime. The compiler
  ensures the model's stratum follows `source_rule` (so the source
  facts are fully materialized before invocation); the runtime
  joins each candidate row against the source rule's derived facts
  via a pre-built `vid → FeatureValue` lookup and feeds the result
  into the classifier as a feature.

  Background:
    Given an empty graph

  # ── Happy path: path-context column flows to classifier ─────────────────

  Scenario: FEATURES (subject, col) FROM rule feeds classifier with column value
    Given having executed:
      """
      CREATE (:Supplier {name: 'A'})
      """
    And a registered mock classifier "risk_model" driven by Float feature "path_risk"
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
    Then evaluation should succeed
    # supply_path yields 0.42 for the Supplier; the path-context join pulls that
    # value into the classifier input, and the echo-style mock returns it unchanged.
    And the derived relation 'risky' should contain a fact where risk = 0.42

  # ── Subject not in source rule → Null feature ──────────────────────────

  Scenario: Subject absent from source rule surfaces FeatureValue::Null
    Given having executed:
      """
      CREATE (:Supplier {name: 'A'}), (:Item {name: 'X'})
      """
    # supply_path only derives for Suppliers; the model invocation against
    # an Item won't find a path_risk row.
    And a registered mock classifier "risk_model" driven by Float feature "path_risk"
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE RULE supply_path AS
        MATCH (s:Supplier)
        YIELD KEY s, 0.42 AS path_risk

      CREATE MODEL risk_model AS
        INPUT (i)
        FEATURES (i, path_risk) FROM supply_path
        OUTPUT PROB risk
        USING xervo('classify/risk_model')

      CREATE RULE risky AS
        MATCH (i:Item)
        YIELD KEY i, risk_model(i) AS risk
      """
    Then evaluation should succeed
    # Mock falls through to 0.0 for missing/non-Float features.
    And the derived relation 'risky' should contain a fact where risk = 0.0

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
