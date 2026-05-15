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
