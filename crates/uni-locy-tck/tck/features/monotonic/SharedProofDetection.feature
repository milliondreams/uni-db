Feature: Shared-Proof Detection
  Detect when MNOR/MPROD proof paths share intermediate facts, violating the
  independence assumption. Results are still computed with independence; only a
  RuntimeWarning is emitted.

  Background:
    Given an empty graph

  # ── Positive: diamond graph triggers warning ──────────────────────────────
  # Two proof paths to the same target share an intermediate node.
  #   Source --0.3--> Mid --0.5--> Target
  #   Source --0.7--> Target
  # The recursive rule derives (Source, Target) via two paths that both
  # originate from Source-level edges. MNOR treats these as independent but
  # they share the Source node, so the warning should fire.

  Scenario: Diamond graph triggers SharedProbabilisticDependency warning
    Given having executed:
      """
      CREATE (s:Node {name: 'Source'}),
             (m:Node {name: 'Mid'}),
             (t:Node {name: 'Target'}),
             (s)-[:LINK {p: 0.3}]->(m),
             (m)-[:LINK {p: 0.5}]->(t),
             (s)-[:LINK {p: 0.7}]->(t)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reach AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        FOLD prob = MNOR(e.p)
        YIELD KEY a, KEY b, prob
      CREATE RULE reach AS
        MATCH (a:Node)-[e:LINK]->(mid:Node) WHERE mid IS reach TO b
        FOLD prob = MNOR(e.p)
        YIELD KEY a, KEY b, prob
      """
    Then evaluation should succeed
    And the result should contain a SharedProbabilisticDependency warning for rule 'reach'

  # ── Negative: independent parallel paths — no warning ─────────────────────
  # Two direct edges with no shared intermediate.
  #   A --0.4--> B
  #   A --0.6--> B

  Scenario: Independent parallel paths produce no warning
    Given having executed:
      """
      CREATE (a:Node {name: 'A'}),
             (b:Node {name: 'B'}),
             (a)-[:LINK {p: 0.4}]->(b),
             (a)-[:LINK {p: 0.6}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        FOLD prob = MNOR(e.p)
        YIELD KEY a, KEY b, prob
      """
    Then evaluation should succeed
    And the result should not contain a SharedProbabilisticDependency warning

  # ── Negative: non-probabilistic FOLD — no warning ─────────────────────────
  # MSUM rule should not trigger shared-proof detection.

  Scenario: Non-probabilistic FOLD produces no warning
    Given having executed:
      """
      CREATE (a:Node {name: 'A'}),
             (b:Node {name: 'B'}),
             (a)-[:LINK {w: 3}]->(b),
             (a)-[:LINK {w: 5}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE total AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        FOLD s = MSUM(e.w)
        YIELD KEY a, KEY b, s
      """
    Then evaluation should succeed
    And the result should not contain a SharedProbabilisticDependency warning

  # ── Positive: non-recursive cross-stratum diamond triggers warning ──────
  # Three strata, all non-recursive:
  #   direct  (stratum 1): base edges
  #   two_hop (stratum 2): two-hop paths via IS direct
  #   combined(stratum 3): merges direct + two_hop with MNOR
  # The (Source, Target) KEY group gets rows from both clauses, and both
  # trace through Source-level edges → warning should fire.

  Scenario: Non-recursive cross-stratum shared proofs trigger warning
    Given having executed:
      """
      CREATE (s:Node {name: 'Source'}),
             (m:Node {name: 'Mid'}),
             (t:Node {name: 'Target'}),
             (s)-[:LINK {p: 0.3}]->(m),
             (m)-[:LINK {p: 0.5}]->(t),
             (s)-[:LINK {p: 0.7}]->(t)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE direct AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        FOLD prob = MNOR(e.p)
        YIELD KEY a, KEY b, prob

      CREATE RULE combined AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        FOLD p = MNOR(e.p)
        YIELD KEY a, KEY b, p
      CREATE RULE combined AS
        MATCH (a:Node)-[e:LINK]->(mid:Node) WHERE mid IS direct TO b
        FOLD p = MNOR(e.p)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the result should contain a SharedProbabilisticDependency warning for rule 'combined'
