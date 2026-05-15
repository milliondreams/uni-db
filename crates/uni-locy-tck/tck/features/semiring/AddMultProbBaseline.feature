Feature: AddMultProb semiring baseline (Phase A foundation)

  Asserts that an explicit `semiring: AddMultProb` selection produces results
  byte-identical to the existing default behavior. Phase A's primary gate
  is "zero behavioral change for current users" (rollout doc Phase A gate
  bullet 1).

  Background:
    Given an empty graph

  # ── MNOR under explicit AddMultProb matches default ──────────────────────

  Scenario: MNOR three values under AddMultProb matches default
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
    When evaluating the following Locy program with semiring "AddMultProb":
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    # 1 - (0.7)(0.5)(0.3) = 0.895 — same as MathematicalReference.feature.
    And the derived relation 'risk' should contain a fact where p = 0.895
    And the result should not contain a FuzzyNotProbabilistic warning

  # ── MPROD under explicit AddMultProb matches default ─────────────────────

  Scenario: MPROD two values under AddMultProb matches default
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
    When evaluating the following Locy program with semiring "AddMultProb":
      """
      CREATE RULE joint AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MPROD(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    # 0.5 * 0.4 = 0.2 — independence-mode product.
    And the derived relation 'joint' should contain a fact where p = 0.2
    And the result should not contain a FuzzyNotProbabilistic warning
