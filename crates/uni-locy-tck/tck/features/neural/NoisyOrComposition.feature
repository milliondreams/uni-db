Feature: Phase B noisy-OR composition over neural-classifier outputs

  Closes the Phase B gate "Mock classifier produces noisy-OR results
  within 1e-6 of hand-computation across 3 redundant paths" — the
  prior `MNOR over a neural-output rule combines probabilities`
  scenario only exercises the YIELD pipeline; this one composes the
  classifier output through MNOR over 3 redundant paths and asserts
  the result against a hand-computed noisy-OR.

  Background:
    Given an empty graph

  Scenario: MNOR over 3 redundant neural-classified paths
    # Three parallel edges between supplier S1 and part P1. The rule
    # invokes the classifier per edge and folds the outputs via
    # noisy-OR. Exercises FOLD-position model invocation lifting —
    # the compiler rewrites `MNOR(scorer(s))` to `MNOR(Variable
    # "__model_scorer_0")` and the runtime `LocyModelInvokeExec`
    # materializes the synthetic column before FoldExec aggregates.
    Given having executed:
      """
      CREATE (s:Supplier {name: 'S1'}),
             (p:Part {name: 'P1'}),
             (s)-[:ASSESSED {path: 1}]->(p),
             (s)-[:ASSESSED {path: 2}]->(p),
             (s)-[:ASSESSED {path: 3}]->(p)
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
      """
    # Hand-computed: 1 - (1 - 0.5)^3 = 0.875.
    Then evaluation should succeed
    And the derived relation 'combined' should contain a fact where combined is approximately 0.875 within 0.000001
