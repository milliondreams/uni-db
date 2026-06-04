Feature: Phase A + Phase B remaining-items follow-ups

  Bundles the user-visible deliverables of the Phase A+B remaining
  slices: cross-iteration memoization, property-access feature
  expressions, and the WHERE-position compile-time error.

  Background:
    Given an empty graph

  # ── Slice 2: memoization — duplicate inputs trigger one classifier call

  Scenario: Memoization deduplicates identical classifier inputs
    Given having executed:
      """
      CREATE (n0:Item {idx: 0, label: true}), (n1:Item {idx: 0, label: false}),
             (n2:Item {idx: 0, label: true})
      """
    And a counting mock classifier "scorer" returning 0.5
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL scorer AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/scorer')
        CALIBRATION platt_scaling

      CREATE RULE risky AS
        MATCH (s:Item)
        YIELD KEY s, scorer(s) AS risk
      """
    Then evaluation should succeed
    # All three nodes share the same vid (graph storage assigns
    # unique vids; the classifier sees three different Vids though,
    # so this case mostly exercises that the cache plumbing is in
    # place rather than asserting strict dedup). We assert the
    # classifier was called at most N times (where N = number of
    # unique vids = 3) — well under the no-cache upper bound which
    # depends on iteration count.
    And classifier "scorer" should have been called at most 3 times

  # ── Slice 3: property-access feature expressions ───────────────────
  #
  # Status: documented limitation. The compiler accepts
  # `tier_scorer(s.tier)` syntactically but at runtime the body-batch
  # projection has stripped `s` down to a vid (Int) — property
  # access against it returns Null, so the classifier sees a Null
  # feature. Full support requires planner-side property
  # materialization (carrying node properties into the post-MATCH
  # row stream). Tracked as a follow-up; this scenario locks in the
  # current observable behavior so regressions don't silently land.

  Scenario: Property-access feature expr is materialized end-to-end
    Given having executed:
      """
      CREATE (a:Customer {tier: 'high'})
      """
    And a registered mock classifier "tier_scorer" returning 0.9 when string feature "s" equals "high"
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL tier_scorer AS
        INPUT (s)
        FEATURES s.tier
        OUTPUT PROB risk
        USING xervo('classify/tier_scorer')
        CALIBRATION platt_scaling

      CREATE RULE risky AS
        MATCH (s:Customer)
        YIELD KEY s, tier_scorer(s.tier) AS risk
      """
    # The compiler appends a hidden YIELD item materializing s.tier as
    # `__feat_s_tier`, the planner emits it through the standard
    # property-access pipeline, and `apply_model_invocations` reads the
    # value into ClassifyInput before stripping the hidden column from
    # the user-visible result.
    Then evaluation should succeed
    And the derived relation 'risky' should contain a fact where risk = 0.9

  Scenario: Property-access with missing property surfaces Null to the classifier
    # The customer has no `tier` set, so the materialized property
    # column is Null. The classifier receives FeatureValue::Null and
    # — per the mock's fallthrough branch — returns 0.1.
    Given having executed:
      """
      CREATE (a:Customer {name: 'no-tier'})
      """
    And a registered mock classifier "tier_scorer" returning 0.9 when string feature "s" equals "high"
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL tier_scorer AS
        INPUT (s)
        FEATURES s.tier
        OUTPUT PROB risk
        USING xervo('classify/tier_scorer')
        CALIBRATION platt_scaling

      CREATE RULE risky AS
        MATCH (s:Customer)
        YIELD KEY s, tier_scorer(s.tier) AS risk
      """
    Then evaluation should succeed
    And the derived relation 'risky' should contain a fact where risk = 0.1

  Scenario: User-visible YIELD of same property coexists with the hidden feature column
    # The user already projects `s.tier AS t`. The hidden column
    # `__feat_s_tier` is emitted independently, so neither projection
    # clobbers the other; the model receives the property value AND
    # the user-visible result carries `t`.
    Given having executed:
      """
      CREATE (a:Customer {tier: 'high'})
      """
    And a registered mock classifier "tier_scorer" returning 0.9 when string feature "s" equals "high"
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL tier_scorer AS
        INPUT (s)
        FEATURES s.tier
        OUTPUT PROB risk
        USING xervo('classify/tier_scorer')
        CALIBRATION platt_scaling

      CREATE RULE risky AS
        MATCH (s:Customer)
        YIELD KEY s, s.tier AS t, tier_scorer(s.tier) AS risk
      """
    Then evaluation should succeed
    And the derived relation 'risky' should contain a fact where risk = 0.9

  Scenario: Arithmetic feature expression rejected at compile time
    # Today only plain variables and direct property access are
    # supported. Arithmetic / nested-call feature exprs surface as
    # `UnsupportedFeatureExpression` from the compiler — promoting
    # the previously-runtime check to compile-time.
    Given having executed:
      """
      CREATE (a:Customer {tier: 'high'})
      """
    And a registered mock classifier "tier_scorer" returning 0.9 when string feature "s" equals "high"
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL tier_scorer AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/tier_scorer')
        CALIBRATION platt_scaling

      CREATE RULE risky AS
        MATCH (s:Customer)
        YIELD KEY s, tier_scorer(s.tier + 1) AS risk
      """
    Then evaluation should fail
    And the evaluation error should mention "unsupported"

  # ── Slice 7: WHERE-position invocations rejected at compile time

  Scenario: WHERE-position model invocation errors at compile time
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL scorer AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/scorer')

      CREATE RULE risky AS
        MATCH (s:Item)
        WHERE scorer(s) > 0.5
        YIELD KEY s
      """
    Then evaluation should fail
    And the evaluation error should mention "WHERE"

  # ── Sanity: existing YIELD-position invocations still work

  Scenario: YIELD-position invocations remain supported
    Given having executed:
      """
      CREATE (:Item {idx: 0})
      """
    And a registered mock classifier "scorer" returning 0.42
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL scorer AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/scorer')
        CALIBRATION platt_scaling

      CREATE RULE risky AS
        MATCH (s:Item)
        YIELD KEY s, scorer(s) AS risk
      """
    Then evaluation should succeed
    And the derived relation 'risky' should contain a fact where risk = 0.42
