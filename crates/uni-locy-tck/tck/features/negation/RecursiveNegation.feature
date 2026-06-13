Feature: IS NOT against a recursive rule (converged facts, not last delta)

  Regression for the 2026-06-10 review bug #8: `IS NOT <recursive rule>`
  anti-joined against the negated rule's *self-referential* scan handle, which
  after fixpoint holds only the final (usually empty) semi-naive delta — not the
  converged facts (those live on the cross-stratum, non-self-ref handle). As a
  result the negation silently under-filtered: it kept rows it should have
  removed.

  Every IS NOT target across the rest of the Locy TCK is non-recursive, so this
  shape was entirely uncovered.

  Background:
    Given an empty graph

  Scenario: IS NOT excludes the full transitive closure, not just the last delta
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'})-[:EDGE]->(c:Node {name: 'C'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(mid:Node) WHERE mid IS reachable TO b YIELD KEY a, KEY b
      CREATE RULE unreachable AS MATCH (a:Node), (b:Node) WHERE a IS NOT reachable TO b YIELD KEY a, KEY b
      """
    Then evaluation should succeed
    # Transitive closure of A->B->C is {(A,B),(B,C),(A,C)}.
    And the derived relation 'reachable' should have 3 facts
    # 3 nodes => 9 ordered pairs; 9 - 3 reachable = 6 unreachable.
    # The bug anti-joined against the empty final delta, leaving all 9 pairs.
    And the derived relation 'unreachable' should have 6 facts
    # (A,C) is transitively reachable, so it must be excluded.
    And the derived relation 'unreachable' should not contain a fact where a.name = 'A' and b.name = 'C'
    # (C,A) is genuinely unreachable, so it must be kept.
    And the derived relation 'unreachable' should contain a fact where a.name = 'C' and b.name = 'A'

  Scenario: IS NOT against a cyclic recursive rule excludes every reachable pair
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'}), (b)-[:EDGE]->(a)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      CREATE RULE reachable AS MATCH (a:Node)-[:EDGE]->(mid:Node) WHERE mid IS reachable TO b YIELD KEY a, KEY b
      CREATE RULE unreachable AS MATCH (a:Node), (b:Node) WHERE a IS NOT reachable TO b YIELD KEY a, KEY b
      """
    Then evaluation should succeed
    # The 2-cycle makes every ordered pair reachable: {(A,B),(B,A),(A,A),(B,B)}.
    And the derived relation 'reachable' should have 4 facts
    # All 4 ordered pairs are reachable => nothing is unreachable.
    # The bug left all 4 pairs (anti-join against the empty final delta).
    And the derived relation 'unreachable' should have 0 facts
