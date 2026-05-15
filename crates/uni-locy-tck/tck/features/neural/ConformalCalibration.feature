Feature: Phase C C1a — Conformal predictor + ConfidenceBand

  Split-conformal calibration: at fit time, compute the
  (1 - alpha)-quantile of holdout nonconformity scores
  `s_i = 1 - p_i` if label=1 else `p_i`. At inference the
  calibrator's `apply` is identity (point prediction unchanged); the
  per-prediction confidence band is `[p - q, p + q]` clipped to
  `[0, 1]`.

  Background:
    Given an empty graph

  Scenario: Conformal CALIBRATE produces hand-computed quantile
    # 20 samples; mock classifier returns 0.5 for all. Labels
    # alternate true/false. With holdout = 0.5, the first 10 rows
    # are holdout and the rest are training. The training labels
    # are 10 alternating true/false → nonconformity scores
    # `s_i = 1 - 0.5 = 0.5` (label=true) or `s_i = 0.5`
    # (label=false). The quantile of constant-0.5 data is 0.5.
    Given having executed:
      """
      CREATE (n0:Sample {idx: 0, label: true}),  (n1:Sample {idx: 1, label: false}),
             (n2:Sample {idx: 2, label: true}),  (n3:Sample {idx: 3, label: false}),
             (n4:Sample {idx: 4, label: true}),  (n5:Sample {idx: 5, label: false}),
             (n6:Sample {idx: 6, label: true}),  (n7:Sample {idx: 7, label: false}),
             (n8:Sample {idx: 8, label: true}),  (n9:Sample {idx: 9, label: false}),
             (n10:Sample {idx: 10, label: true}),(n11:Sample {idx: 11, label: false}),
             (n12:Sample {idx: 12, label: true}),(n13:Sample {idx: 13, label: false}),
             (n14:Sample {idx: 14, label: true}),(n15:Sample {idx: 15, label: false}),
             (n16:Sample {idx: 16, label: true}),(n17:Sample {idx: 17, label: false}),
             (n18:Sample {idx: 18, label: true}),(n19:Sample {idx: 19, label: false})
      """
    And a registered mock classifier "scorer" returning 0.5
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL scorer AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/scorer')

      CALIBRATE scorer
        ON MATCH (s:Sample)
        TARGET s.label
        METHOD conformal(0.1)
        HOLDOUT 0.5
      """
    Then evaluation should succeed
    And the calibration result for "scorer" should report method "Conformal"
    And the calibration result for "scorer" should have confidence_band_quantile approximately 0.5 within 0.000001

  Scenario: Conformal CALIBRATE with default alpha keyword
    # Bare `conformal` keyword parses with default alpha = 0.1.
    Given having executed:
      """
      CREATE (n0:Sample {idx: 0, label: true}),  (n1:Sample {idx: 1, label: false}),
             (n2:Sample {idx: 2, label: true}),  (n3:Sample {idx: 3, label: false}),
             (n4:Sample {idx: 4, label: true}),  (n5:Sample {idx: 5, label: false}),
             (n6:Sample {idx: 6, label: true}),  (n7:Sample {idx: 7, label: false}),
             (n8:Sample {idx: 8, label: true}),  (n9:Sample {idx: 9, label: false})
      """
    And a registered mock classifier "scorer" returning 0.5
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL scorer AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/scorer')

      CALIBRATE scorer
        ON MATCH (s:Sample)
        TARGET s.label
        METHOD conformal
        HOLDOUT 0.5
      """
    Then evaluation should succeed
    And the calibration result for "scorer" should report method "Conformal"
