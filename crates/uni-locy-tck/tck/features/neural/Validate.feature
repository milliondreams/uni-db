Feature: VALIDATE statement (Phase C C3)

  Measures the quality of a rule's PROB output against ground truth.
  No fitting — just metrics: Brier, log-loss, ECE, DEBIASED_ECE,
  accuracy, AUC. C4 emits `EceBinningBias` when bare `ECE` is
  requested.

  Background:
    Given an empty graph

  # ── Well-calibrated rule: Brier is low, accuracy is high ────────────────

  Scenario: VALIDATE on a perfectly-calibrated rule reports low Brier
    Given having executed:
      """
      CREATE
        (a:Item {idx: 1, label: true,  prob: 0.9}),
        (b:Item {idx: 2, label: false, prob: 0.1}),
        (c:Item {idx: 3, label: true,  prob: 0.8}),
        (d:Item {idx: 4, label: false, prob: 0.2}),
        (e:Item {idx: 5, label: true,  prob: 0.95}),
        (f:Item {idx: 6, label: false, prob: 0.05})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (s:Item)
        YIELD KEY s, s.prob AS risk PROB

      VALIDATE risk
        ON MATCH (s:Item)
        TARGET s.label
        METRICS brier_score, accuracy, auc
      """
    Then evaluation should succeed
    And the validation result for rule "risk" should report "brier_score" less than 0.05
    And the validation result for rule "risk" should report "accuracy" greater than 0.99
    And the validation result for rule "risk" should report "auc" greater than 0.99

  # ── Poorly-calibrated rule: AUC drops, Brier rises ──────────────────────

  Scenario: VALIDATE on a flipped-prediction rule reports low accuracy
    Given having executed:
      """
      CREATE
        (a:Item {idx: 1, label: true,  prob: 0.1}),
        (b:Item {idx: 2, label: false, prob: 0.9}),
        (c:Item {idx: 3, label: true,  prob: 0.2}),
        (d:Item {idx: 4, label: false, prob: 0.8})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (s:Item)
        YIELD KEY s, s.prob AS risk PROB

      VALIDATE risk
        ON MATCH (s:Item)
        TARGET s.label
        METRICS accuracy, auc
      """
    Then evaluation should succeed
    And the validation result for rule "risk" should report "accuracy" less than 0.05
    And the validation result for rule "risk" should report "auc" less than 0.05

  # ── C4: EceBinningBias fires for bare ECE; not for DEBIASED_ECE ────────

  Scenario: Bare ECE in VALIDATE emits EceBinningBias warning
    Given having executed:
      """
      CREATE (a:Item {idx: 1, label: true, prob: 0.9})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (s:Item)
        YIELD KEY s, s.prob AS risk PROB

      VALIDATE risk
        ON MATCH (s:Item)
        TARGET s.label
        METRICS ece
      """
    Then evaluation should succeed
    And the result should contain an EceBinningBias warning

  # ── VALIDATE on unknown rule errors at compile time ────────────────────

  Scenario: VALIDATE on unknown rule fails at compile
    When evaluating the following Locy program:
      """
      VALIDATE ghost
        ON MATCH (s:Item)
        TARGET s.label
        METRICS brier_score
      """
    Then evaluation should fail
    And the evaluation error should mention "ghost"
