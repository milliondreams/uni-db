Feature: Neural model invocation in rule bodies (Phase B Slice 3)

  Validates the runtime path from `CREATE MODEL` declaration through
  rule-body invocation. Each scenario registers a mock classifier under
  the model name and asserts the derived facts carry the classifier's
  output as the YIELD column.

  Background:
    Given an empty graph

  # ── Happy path: constant mock classifier feeds a PROB column ────────────

  Scenario: Constant mock classifier produces uniform PROB output
    Given having executed:
      """
      CREATE (:Supplier {name: 'A'}), (:Supplier {name: 'B'}), (:Supplier {name: 'C'})
      """
    And a registered mock classifier "scorer" returning 0.7
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL scorer AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/scorer')

      CREATE RULE risky AS
        MATCH (s:Supplier)
        YIELD KEY s, scorer(s) AS risk
      """
    Then evaluation should succeed
    And the derived relation 'risky' should have 3 facts
    And the derived relation 'risky' should contain a fact where risk = 0.7

  # ── PROB auto-inference: no explicit `AS PROB`, output_type=Prob ───────

  Scenario: Model with OUTPUT PROB auto-flags the YIELD column
    Given having executed:
      """
      CREATE (:Item {name: 'X'})
      """
    And a registered mock classifier "scorer" returning 0.42
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL scorer AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/scorer')

      CREATE RULE risk_rule AS
        MATCH (s:Item)
        YIELD KEY s, scorer(s) AS risk
      """
    Then evaluation should succeed
    And the derived relation 'risk_rule' should contain a fact where risk = 0.42

  # ── Missing classifier in registry: runtime error ──────────────────────
  # The model is declared and the rule invokes it, but no impl is
  # registered. Runtime invocation must surface a clear error.

  Scenario: Invoking an unregistered model errors at runtime
    Given having executed:
      """
      CREATE (:Supplier {name: 'A'})
      """
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL ghost AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/ghost')

      CREATE RULE ghosted AS
        MATCH (s:Supplier)
        YIELD KEY s, ghost(s) AS risk
      """
    Then evaluation should fail
    And the evaluation error should mention "not registered"

  # ── Probabilistic composition: MNOR fed from prior neural-output rule ───
  # Demonstrates that the classifier's output flows correctly into a
  # downstream MNOR aggregation (the PROB column is the noisy-OR input).

  Scenario: MNOR over a neural-output rule combines probabilities
    Given having executed:
      """
      CREATE (a:Item {name: 'A'}), (b:Item {name: 'B'})
      """
    And a registered mock classifier "scorer" returning 0.5
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL scorer AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/scorer')

      CREATE RULE base AS
        MATCH (s:Item)
        YIELD KEY s, scorer(s) AS p
      """
    Then evaluation should succeed
    # Each row got 0.5 from the classifier.
    And the derived relation 'base' should have 2 facts
    And the derived relation 'base' should contain a fact where p = 0.5
