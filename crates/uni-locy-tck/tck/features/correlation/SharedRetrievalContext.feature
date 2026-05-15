Feature: Shared retrieval context (Phase D F3 case 4 / F2c)

  Phase D F3 case 4 detects when two or more neural-model
  invocations in the same rule receive retrieval-backed features
  (`similar_to(prop, …)` or `semantic_match(prop, …)`) over the
  *same* node property. The two models condition on the same
  retrieval evidence, so the implicit independence assumption that
  underlies composition via MNOR/MPROD is suspect. Structural
  detector — over-emits on the syntactic pattern; suppressed by
  `@independent` on all involved models.

  Background:
    Given an empty graph

  # ── Two similar_to invocations on same property → warning ───────────────

  Scenario: Two similar_to features on same property emits SharedRetrievalContext
    Given having executed:
      """
      CREATE (:Doc {name: 'D1', embedding: [1.0, 0.0, 0.0]})
      """
    And a registered mock classifier "fraud_scorer" driven by Float feature "sim"
    And a registered mock classifier "risk_scorer" driven by Float feature "sim"
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL fraud_scorer AS
        INPUT (sim)
        OUTPUT SCORE fraud_score
        USING xervo('classify/fraud_scorer')

      CREATE MODEL risk_scorer AS
        INPUT (sim)
        OUTPUT SCORE risk_score
        USING xervo('classify/risk_scorer')

      CREATE RULE risky AS
        MATCH (d:Doc)
        YIELD KEY d,
              fraud_scorer(similar_to(d.embedding, [1.0, 0.0, 0.0])) AS fraud,
              risk_scorer(similar_to(d.embedding, [0.0, 1.0, 0.0])) AS risk
      """
    Then evaluation should succeed
    And the result should contain a SharedRetrievalContext warning

  # ── Different properties → no warning ───────────────────────────────────

  Scenario: Retrieval features on different properties do not emit warning
    Given having executed:
      """
      CREATE (:Doc {name: 'D3', title_emb: [1.0, 0.0], body_emb: [0.0, 1.0]})
      """
    And a registered mock classifier "fraud_scorer" driven by Float feature "sim"
    And a registered mock classifier "risk_scorer" driven by Float feature "sim"
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL fraud_scorer AS
        INPUT (sim)
        OUTPUT SCORE fraud_score
        USING xervo('classify/fraud_scorer')

      CREATE MODEL risk_scorer AS
        INPUT (sim)
        OUTPUT SCORE risk_score
        USING xervo('classify/risk_scorer')

      CREATE RULE risky AS
        MATCH (d:Doc)
        YIELD KEY d,
              fraud_scorer(similar_to(d.title_emb, [1.0, 0.0])) AS fraud,
              risk_scorer(similar_to(d.body_emb, [0.0, 1.0])) AS risk
      """
    Then evaluation should succeed
    And the result should not contain a SharedRetrievalContext warning

  # ── @independent suppresses the warning ─────────────────────────────────

  Scenario: @independent annotation suppresses SharedRetrievalContext
    Given having executed:
      """
      CREATE (:Doc {name: 'D4', embedding: [1.0, 0.0, 0.0]})
      """
    And a registered mock classifier "fraud_scorer" driven by Float feature "sim"
    And a registered mock classifier "risk_scorer" driven by Float feature "sim"
    When evaluating the following Locy program with neural_predicates_preview:
      """
      @independent CREATE MODEL fraud_scorer AS
        INPUT (sim)
        OUTPUT SCORE fraud_score
        USING xervo('classify/fraud_scorer')

      @independent CREATE MODEL risk_scorer AS
        INPUT (sim)
        OUTPUT SCORE risk_score
        USING xervo('classify/risk_scorer')

      CREATE RULE risky AS
        MATCH (d:Doc)
        YIELD KEY d,
              fraud_scorer(similar_to(d.embedding, [1.0, 0.0, 0.0])) AS fraud,
              risk_scorer(similar_to(d.embedding, [0.0, 1.0, 0.0])) AS risk
      """
    Then evaluation should succeed
    And the result should not contain a SharedRetrievalContext warning
