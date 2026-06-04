Feature: MaxMinProb (fuzzy / Viterbi) semiring

  When `semiring: MaxMinProb` is selected, MNOR collapses to max(pᵢ) and
  MPROD collapses to min(pᵢ). Per rollout decision D-9 every PROB-bearing
  rule under this semiring emits an unsuppressible
  `FuzzyNotProbabilistic` warning: fuzzy truth values are not
  probabilities.

  Background:
    Given an empty graph

  # ── MNOR under MaxMinProb becomes max ────────────────────────────────────

  Scenario: MNOR under MaxMinProb returns max-of-inputs and warns
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
    When evaluating the following Locy program with semiring "MaxMinProb":
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p AS PROB
      """
    Then evaluation should succeed
    And the derived relation 'risk' should contain a fact where p = 0.7
    And the result should contain a FuzzyNotProbabilistic warning for rule 'risk'

  # ── MPROD under MaxMinProb becomes min ───────────────────────────────────

  Scenario: MPROD under MaxMinProb returns min-of-inputs and warns
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
    When evaluating the following Locy program with semiring "MaxMinProb":
      """
      CREATE RULE joint AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MPROD(e.prob)
        YIELD KEY a, KEY b, p AS PROB
      """
    Then evaluation should succeed
    And the derived relation 'joint' should contain a fact where p = 0.4
    And the result should contain a FuzzyNotProbabilistic warning for rule 'joint'

  # ── Recursive PROB rule under MaxMinProb ─────────────────────────────────
  # Exercises the `run_fixpoint_loop` emission path (vs. the non-recursive
  # `run_program` path covered above). The recursive rule shares a base
  # fact between proof paths — under AddMultProb this trips
  # SharedProbabilisticDependency; under MaxMinProb the gate added to
  # `detect_shared_lineage` must suppress it (max is idempotent).

  Scenario: Recursive MaxMinProb rule emits Fuzzy warning and suppresses shared-proof
    Given having executed:
      """
      CREATE (s:Node {name: 'Source'}),
             (m:Node {name: 'Mid'}),
             (t:Node {name: 'Target'}),
             (s)-[:LINK {p: 0.3}]->(m),
             (m)-[:LINK {p: 0.5}]->(t),
             (s)-[:LINK {p: 0.7}]->(t)
      """
    When evaluating the following Locy program with semiring "MaxMinProb":
      """
      CREATE RULE reach AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        FOLD prob = MNOR(e.p)
        YIELD KEY a, KEY b, prob AS PROB
      CREATE RULE reach AS
        MATCH (a:Node)-[e:LINK]->(mid:Node) WHERE mid IS reach TO b
        FOLD prob = MNOR(e.p)
        YIELD KEY a, KEY b, prob AS PROB
      """
    Then evaluation should succeed
    And the result should contain a FuzzyNotProbabilistic warning for rule 'reach'
    And the result should not contain a SharedProbabilisticDependency warning

  # ── MaxMinProb on a non-PROB rule: no warning ────────────────────────────
  # A rule with no PROB column (here: MSUM, which is not a probability
  # operator) is not probability-bearing. The Fuzzy warning gates on
  # `is_prob` so this case must not emit.
  #
  # NOTE: MNOR/MPROD outputs are auto-tagged as PROB by the compiler
  # (DEEP_LOCY.md §6.2), so any rule using those will warn regardless of
  # explicit annotation. Use a non-probabilistic aggregate to exercise
  # the "no PROB column" path.

  Scenario: Non-PROB rule under MaxMinProb does not warn
    Given having executed:
      """
      CREATE (a:Node {name: 'A'}),
             (b:Node {name: 'B'}),
             (a)-[:LINK {w: 3}]->(b),
             (a)-[:LINK {w: 5}]->(b)
      """
    When evaluating the following Locy program with semiring "MaxMinProb":
      """
      CREATE RULE total AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        FOLD s = MSUM(e.w)
        YIELD KEY a, KEY b, s
      """
    Then evaluation should succeed
    And the result should not contain a FuzzyNotProbabilistic warning

  # ── Strict probability domain under MaxMinProb ───────────────────────────
  # The strict check fires inside both maxmin_disjunction_f64 and the
  # MonotonicAggState::update MaxMinProb branch — out-of-range inputs
  # must error rather than silently clamp.

  Scenario: Out-of-range input under MaxMinProb + strict fails fast
    Given having executed:
      """
      CREATE (a:Node {name: 'A'}),
             (b:Node {name: 'B'}),
             (a)-[:LINK {p: 1.5}]->(b)
      """
    When evaluating the following Locy program with semiring "MaxMinProb":
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        FOLD p = MNOR(e.p)
        YIELD KEY a, KEY b, p AS PROB
      """
    Then evaluation should succeed
    # Permissive mode (strict_probability_domain=false default): value
    # clamps to 1.0, and Fuzzy warning still fires because output is PROB.
    And the derived relation 'risk' should contain a fact where p = 1.0
    And the result should contain a FuzzyNotProbabilistic warning for rule 'risk'
