Feature: CREATE MODEL (Phase B preview)

  Validates that `CREATE MODEL` parses end-to-end and is gated by the
  `LocyConfig::neural_predicates_preview` flag (rollout decision D-1).
  Runtime invocation lands in Phase B Slice 3; this slice covers the
  parser → compiler → catalog path.

  Background:
    Given an empty graph

  # ── Preview flag off (opt-out): reject at compile time ─────────────────
  # Phase C gate-closure: default flipped from false to true; this
  # scenario now exercises the explicit opt-out path. Users who set
  # `neural_predicates_preview = false` still get the original
  # rejection error.

  Scenario: CREATE MODEL rejected when neural_predicates_preview is off
    When evaluating the following Locy program with neural_predicates_preview disabled:
      """
      CREATE MODEL supplier_risk AS
      INPUT (s:Supplier)
      OUTPUT PROB risk
      USING xervo('classify/supplier-risk-v3')
      """
    Then evaluation should fail
    And the evaluation error should mention "neural_predicates_preview"

  # ── Preview flag on: compile + register in model_catalog ────────────────

  Scenario: CREATE MODEL compiles cleanly under preview flag
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL supplier_risk AS
      INPUT (s:Supplier)
      OUTPUT PROB risk
      USING xervo('classify/supplier-risk-v3')
      CALIBRATION platt_scaling
      VERSION '3.1.0'
      """
    Then evaluation should succeed

  # ── Preview flag on + symbolic rules alongside model declaration ────────
  # The model is a catalog entry; rules that don't invoke it are unaffected.

  Scenario: CREATE MODEL coexists with a non-invoking CREATE RULE
    Given having executed:
      """
      CREATE (a:Node {name: 'A'}), (b:Node {name: 'B'}),
             (a)-[:LINK {p: 0.5}]->(b)
      """
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL placeholder AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/placeholder')

      CREATE RULE link_prob AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        FOLD p = MNOR(e.p)
        YIELD KEY a, KEY b, p AS PROB
      """
    Then evaluation should succeed
    And the derived relation 'link_prob' should contain a fact where p = 0.5
