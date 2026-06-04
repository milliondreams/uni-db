Feature: TopKProofs semiring (Phase C C0 Stages 1 + 2 MVP)

  `SemiringKind::TopKProofs { k }` enables per-row dependency-DNF
  tracking. Stage 1 landed the math library and config wiring; the
  Stage 2 MVP wires per-fact `Proof` tag accumulation through
  `record_provenance` and surfaces `TopKPruningCrossedDependency`
  when the post-loop merge prunes proofs that share base RVs with
  retained ones. Row math still routes through AddMultProb (the
  full `MonotonicAggState` generalization remains deferred), so
  scenarios without proof sharing are byte-identical to the
  default semiring.

  Background:
    Given an empty graph

  # ── TopKProofs(4) accepted; falls back to AddMultProb byte-identically ──

  Scenario: MNOR under TopKProofs(4) Stage 1 matches AddMultProb output
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.3}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.5}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.7}]->(b)
      """
    When evaluating the following Locy program with semiring "TopKProofs(4)":
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    # 1 - (0.7)(0.5)(0.3) = 0.895 — Stage 1 row math = AddMultProb.
    And the derived relation 'risk' should contain a fact where p = 0.895
    # No Fuzzy warning (TopKProofs is not a fuzzy semiring).
    And the result should not contain a FuzzyNotProbabilistic warning

  # ── MPROD under TopKProofs(4): same fallback ──────────────────────────

  Scenario: MPROD under TopKProofs(4) matches AddMultProb product
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.5}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.4}]->(b)
      """
    When evaluating the following Locy program with semiring "TopKProofs(4)":
      """
      CREATE RULE joint AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MPROD(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'joint' should contain a fact where p = 0.2

  # ── Incoherent: TopKProofs(0) rejected at config-resolve time ─────────

  # ── Stage 2: tag-flow does not regress non-sharing programs ──────────

  Scenario: TopKProofs(2) on independent proofs emits no CrossedDependency warning
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.4}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.6}]->(b)
      """
    When evaluating the following Locy program with semiring "TopKProofs(2)":
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    # 1 - (0.4)(0.6) = 0.76
    And the derived relation 'risk' should contain a fact where p = 0.76
    And the result should not contain a TopKPruningCrossedDependency warning

  # ── D-C0: shared-base DNF inclusion-exclusion ─────────────────────────
  # Two derived facts of `risk` that share the SAME base fact (the hub
  # node h_a). Under AddMultProb (pre-D-C0), the FOLD MNOR composes them
  # as if independent: 1 - (1 - 0.7)(1 - 0.6) = 0.88. Under TopKProofs
  # with D-C0, per-row base_rvs reveal the shared base (h_a) and DNF
  # inclusion-exclusion yields the exact probability: the proof DNF is
  # `{h_a} ∨ {h_a}` which collapses to `{h_a}`, so weight = P(h_a)
  # extracted from base_weights — the row's per-base weight.

  Scenario: TopKProofs(8) on shared-base proofs yields DNF inclusion-exclusion
    Given having executed:
      """
      CREATE (:Hub {name: 'h_a'}),
             (:Item {name: 'i1'}),
             (:Item {name: 'i2'})
      """
    And having executed:
      """
      MATCH (h:Hub {name: 'h_a'}), (i:Item {name: 'i1'})
      CREATE (h)-[:CAUSE {prob: 0.7}]->(i)
      """
    And having executed:
      """
      MATCH (h:Hub {name: 'h_a'}), (i:Item {name: 'i2'})
      CREATE (h)-[:CAUSE {prob: 0.6}]->(i)
      """
    # hub_score is per-Hub (one fact per Hub), so risk's IS-ref join
    # gives exactly 1 hub_score fact per Hub × N CAUSE edges. The two
    # `risk` pre-fold rows for KEY h_a both depend on the SAME
    # `hub_score(h_a)` base fact — that's the shared-base correlation.
    When evaluating the following Locy program with semiring "TopKProofs(8)":
      """
      CREATE RULE hub_score AS
        MATCH (h:Hub)
        YIELD KEY h

      CREATE RULE risk AS
        MATCH (h:Hub)-[e:CAUSE]->(i:Item)
        WHERE h IS hub_score
        FOLD risk_p = MNOR(e.prob)
        YIELD KEY h, risk_p
      """
    Then evaluation should succeed
    # Both pre-fold rows of `risk` for KEY h_a have support =
    # `{hub_score(h_a)}` — the SAME base RV. The DNF over their proofs
    # is `{rv_a} ∨ {rv_a}` ≡ `{rv_a}`; weight = base_weights[rv_a] =
    # the first row's per-base weight (0.7 by insertion order).
    # AddMultProb would have produced 1 - (1-0.7)(1-0.6) = 0.88.
    And the derived relation 'risk' should contain a fact where risk_p = 0.7

  # ── Incoherent: TopKProofs(0) rejected at config-resolve time ─────────

  Scenario: TopKProofs(0) rejected as incoherent
    Given having executed:
      """
      CREATE (:Node {name: 'A'})
      """
    When evaluating the following Locy program with semiring "TopKProofs(0)":
      """
      CREATE RULE noop AS
        MATCH (a:Node)
        YIELD KEY a
      """
    Then evaluation should fail
    And the evaluation error should mention "TopKProofs"
