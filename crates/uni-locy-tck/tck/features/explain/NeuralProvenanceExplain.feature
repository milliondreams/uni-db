Feature: Phase C B1–B3 — NeuralProvenance in EXPLAIN

  EXPLAIN's `DerivationNode` carries a per-invocation
  `NeuralProvenance` list (model_name, raw_probability,
  calibrated_probability, confidence_band) so users can see exactly
  which classifier outputs contributed to each derived fact.

  Background:
    Given an empty graph

  Scenario: EXPLAIN surfaces calibrated_probability and confidence_band
    # When the registered classifier wraps a Calibrator (here a
    # ConformalPredictor with quantile=0.05), EXPLAIN's NeuralProvenance
    # populates both calibrated_probability (signalling the value is
    # post-calibration) and confidence_band (from the Calibrator's
    # `confidence_band(p)`).
    Given having executed:
      """
      CREATE (a:Customer {name: 'A'})
      """
    And a registered Calibrated mock classifier "scorer" returning 0.5 with conformal quantile 0.05
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL scorer AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/scorer')

      CREATE RULE risky AS
        MATCH (s:Customer)
        YIELD KEY s, scorer(s) AS risk

      EXPLAIN RULE risky
      """
    Then evaluation should succeed
    And the command result 0 should be an Explain where child 0 has a neural call for "scorer" with raw_probability approximately 0.5
    And the command result 0 should be an Explain where child 0 has a neural call for "scorer" with calibrated_probability set
    And the command result 0 should be an Explain where child 0 has a neural call for "scorer" with confidence_band

  Scenario: EXPLAIN surfaces neural_calls on derivations
    Given having executed:
      """
      CREATE (a:Customer {name: 'A'}), (b:Customer {name: 'B'})
      """
    And a registered mock classifier "scorer" returning 0.5
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL scorer AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/scorer')
        CALIBRATION platt_scaling

      CREATE RULE risky AS
        MATCH (s:Customer)
        YIELD KEY s, scorer(s) AS risk

      EXPLAIN RULE risky
      """
    Then evaluation should succeed
    And the command result 0 should be an Explain where child 0 has a neural call for "scorer" with raw_probability approximately 0.5
    And the command result 0 should be an Explain where child 1 has a neural call for "scorer" with raw_probability approximately 0.5

  Scenario: EXPLAIN surfaces ALONG-position neural_calls
    # Phase C B1-B3 follow-up: invocations inside ALONG
    # expressions (e.g. `link = base * scorer(s)`) have no
    # `yield_alias` post-rewrite, but EXPLAIN re-evaluates the
    # classifier per fact using the ORIGINAL pre-rewrite feature
    # exprs stored on `ModelInvocation`. Closes the prior
    # documented limitation.
    Given having executed:
      """
      CREATE (s:Supplier {name: 'S1'}),
             (p:Part {name: 'P1'}),
             (s)-[e:ASSESSED {base: 0.8}]->(p)
      """
    And a registered mock classifier "scorer" returning 0.5
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL scorer AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/scorer')

      CREATE RULE link_reliability AS
        MATCH (s:Supplier)-[e:ASSESSED]->(p:Part)
        ALONG link = e.base * (1.0 - scorer(s))
        YIELD KEY s, KEY p, link

      EXPLAIN RULE link_reliability
      """
    Then evaluation should succeed
    And the command result 0 should be an Explain where child 0 has a neural call for "scorer" with raw_probability approximately 0.5

  Scenario: EXPLAIN surfaces FOLD-position neural_calls
    # FOLD-position invocations (`MNOR(scorer(s))`) — same
    # mechanism as ALONG. Confirms EXPLAIN doesn't depend on
    # post-projection column survival.
    Given having executed:
      """
      CREATE (s:Supplier {name: 'S1'}),
             (p:Part {name: 'P1'}),
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
        MATCH (s:Supplier)-[e:ASSESSED]->(p:Part)
        FOLD combined = MNOR(scorer(s))
        YIELD KEY s, KEY p, combined

      EXPLAIN RULE combined
      """
    Then evaluation should succeed
    And the command result 0 should be an Explain where child 0 has a neural call for "scorer" with raw_probability approximately 0.5
