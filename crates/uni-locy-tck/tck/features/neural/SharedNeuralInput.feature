Feature: Phase C F2 — SharedNeuralInput graded warning

  Compile-time analysis detects when multiple neural-model
  invocations in the same rule share an input that makes the
  composed probability dependent. F2a fires for a shared input
  VARIABLE; F2b fires for a shared FEATURE VALUE (e.g.,
  `s.tier` reused across two models). Both are suppressed when
  ALL involved models carry the `@independent` annotation.

  Background:
    Given an empty graph

  Scenario: F2a fires on shared input variable across two models
    Given having executed:
      """
      CREATE (n:Item {name: 'N'})
      """
    And a registered mock classifier "model_a" returning 0.5
    And a registered mock classifier "model_b" returning 0.5
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL model_a AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/a')
        CALIBRATION platt_scaling

      CREATE MODEL model_b AS
        INPUT (s)
        OUTPUT SCORE risk
        USING xervo('classify/b')

      CREATE RULE composite AS
        MATCH (s:Item)
        YIELD KEY s, model_a(s) AS pa, model_b(s) AS pb
      """
    Then evaluation should succeed
    And the result should contain a SharedNeuralInputArgument warning for rule "composite"

  Scenario: F2a suppressed when both models carry @independent
    Given having executed:
      """
      CREATE (n:Item {name: 'N'})
      """
    And a registered mock classifier "model_a" returning 0.5
    And a registered mock classifier "model_b" returning 0.5
    When evaluating the following Locy program with neural_predicates_preview:
      """
      @independent
      CREATE MODEL model_a AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/a')
        CALIBRATION platt_scaling

      @independent
      CREATE MODEL model_b AS
        INPUT (s)
        OUTPUT SCORE risk
        USING xervo('classify/b')

      CREATE RULE composite AS
        MATCH (s:Item)
        YIELD KEY s, model_a(s) AS pa, model_b(s) AS pb
      """
    Then evaluation should succeed
    And the result should not contain a SharedNeuralInputArgument warning

  Scenario: F2b fires on shared property feature across two models
    # Property-access feature exprs share a feature VALUE even
    # though the binding variable is the same. F2b is the
    # property-shape sibling of F2a.
    Given having executed:
      """
      CREATE (n:Item {tier: 1})
      """
    And a registered mock classifier "model_a" returning 0.5
    And a registered mock classifier "model_b" returning 0.5
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL model_a AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/a')
        CALIBRATION platt_scaling

      CREATE MODEL model_b AS
        INPUT (s)
        OUTPUT SCORE risk
        USING xervo('classify/b')

      CREATE RULE composite AS
        MATCH (s:Item)
        YIELD KEY s, model_a(s.tier) AS pa, model_b(s.tier) AS pb
      """
    Then evaluation should succeed
    And the result should contain a SharedNeuralFeatureValue warning for rule "composite"

  Scenario: No F2 false positive for single-model MNOR over multiple paths
    # The noisy-OR pattern (one model, MNOR across rows) is NOT
    # cross-model sharing — F2 must stay silent.
    Given having executed:
      """
      CREATE (s:Item {name: 'S'}),
             (p:Part {name: 'P'}),
             (s)-[:ASSESSED]->(p),
             (s)-[:ASSESSED]->(p)
      """
    And a registered mock classifier "scorer" returning 0.5
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL scorer AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/scorer')

      CREATE RULE combined AS
        MATCH (s:Item)-[e:ASSESSED]->(p:Part)
        FOLD combined = MNOR(scorer(s))
        YIELD KEY s, KEY p, combined
      """
    Then evaluation should succeed
    And the result should not contain a SharedNeuralInputArgument warning
