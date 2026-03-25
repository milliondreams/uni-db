Feature: Path-Based Feature Combinations (COMB-PATH)

  Tests ALONG path accumulation combined with similar_to() similarity
  and MNOR probabilistic aggregation. ALONG carries computed scores
  along paths; MNOR aggregates multiple paths between the same endpoints.

  Background:
    Given an empty graph

  # ── Parse level ───────────────────────────────────────────────────────

  Scenario: similar_to in ALONG accumulator parses
    When parsing the following Locy program:
      """
      CREATE RULE relevance_path AS
        MATCH (a:Doc)-[:CITES]->(b:Doc)
        ALONG rel = similar_to(b.embedding, [1.0, 0.0, 0.0])
        YIELD KEY a, KEY b, rel
      CREATE RULE relevance_path AS
        MATCH (a:Doc)-[:CITES]->(mid:Doc)
        WHERE mid IS relevance_path TO b
        ALONG rel = prev.rel * similar_to(b.embedding, [1.0, 0.0, 0.0])
        YIELD KEY a, KEY b, rel
      """
    Then the program should parse successfully

  Scenario: ALONG with edge weight then MNOR fold parses
    When parsing the following Locy program:
      """
      CREATE RULE path AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        ALONG prob = e.rel
        YIELD KEY a, KEY b, prob
      CREATE RULE path AS
        MATCH (a:Node)-[e:LINK]->(mid:Node)
        WHERE mid IS path TO b
        ALONG prob = prev.prob * e.rel
        YIELD KEY a, KEY b, prob
      CREATE RULE reachability AS
        MATCH (a:Node)
        WHERE a IS path TO b
        FOLD p = MNOR(prob)
        YIELD KEY a, KEY b, p
      """
    Then the program should parse successfully

  # ── Evaluate level ────────────────────────────────────────────────────

  Scenario: similar_to carried along citation path
    Given having executed:
      """
      CREATE (a:Doc {name: 'A', embedding: [1.0, 0.0, 0.0]})-[:CITES]->(b:Doc {name: 'B', embedding: [0.6, 0.8, 0.0]})-[:CITES]->(c:Doc {name: 'C', embedding: [0.0, 0.0, 1.0]})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE relevance_path AS
        MATCH (a:Doc)-[:CITES]->(b:Doc)
        ALONG rel = similar_to(b.embedding, [1.0, 0.0, 0.0])
        YIELD KEY a, KEY b, rel
      CREATE RULE relevance_path AS
        MATCH (a:Doc)-[:CITES]->(mid:Doc)
        WHERE mid IS relevance_path TO b
        ALONG rel = prev.rel * similar_to(b.embedding, [1.0, 0.0, 0.0])
        YIELD KEY a, KEY b, rel
      """
    Then evaluation should succeed
    And the derived relation 'relevance_path' should contain a fact where a.name = 'A' and b.name = 'B' and rel = 0.6
    And the derived relation 'relevance_path' should contain a fact where a.name = 'B' and b.name = 'C' and rel = 0.0
    And the derived relation 'relevance_path' should contain a fact where a.name = 'A' and b.name = 'C' and rel = 0.0

  Scenario: ALONG edge reliability aggregated by MNOR across multiple paths
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:LINK {rel: 0.9}]->(m:Node {name: 'M'})-[:LINK {rel: 0.8}]->(b:Node {name: 'B'}),
             (a)-[:LINK {rel: 0.7}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE path AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        ALONG prob = e.rel
        YIELD KEY a, KEY b, prob
      CREATE RULE path AS
        MATCH (a:Node)-[e:LINK]->(mid:Node)
        WHERE mid IS path TO b
        ALONG prob = prev.prob * e.rel
        YIELD KEY a, KEY b, prob
      CREATE RULE reachability AS
        MATCH (a:Node)
        WHERE a IS path TO b
        FOLD p = MNOR(prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'reachability' should contain a fact where a.name = 'A' and b.name = 'B' and p = 0.916

  Scenario: ABDUCE NOT on ALONG-derived path rule finds edge removal
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:LINK {rel: 0.9}]->(b:Node {name: 'B'})-[:LINK {rel: 0.8}]->(c:Node {name: 'C'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE path AS
        MATCH (a:Node)-[e:LINK]->(b:Node)
        ALONG prob = e.rel
        YIELD KEY a, KEY b, prob
      CREATE RULE path AS
        MATCH (a:Node)-[e:LINK]->(mid:Node)
        WHERE mid IS path TO b
        ALONG prob = prev.prob * e.rel
        YIELD KEY a, KEY b, prob
      ABDUCE NOT path WHERE a.name = 'A'
      """
    Then evaluation should succeed
    And the command result 0 should be an Abduce with at least 1 modifications
