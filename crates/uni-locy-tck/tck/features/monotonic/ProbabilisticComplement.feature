Feature: Probabilistic Complement (PROB + IS NOT)

  PROB annotation marks yield columns as probability values.
  IS NOT on PROB rules computes complement (1-p) instead of Boolean exclusion.

  Background:
    Given an empty graph

  # ── Parse level ──────────────────────────────────────────────────────

  Scenario: PROB annotation parses
    When parsing the following Locy program:
      """
      CREATE RULE risky AS
        MATCH (n:Node)-[:HAS_RISK]->(r:Risk)
        YIELD KEY n, r.score AS risk_score PROB
      """
    Then the program should parse successfully

  Scenario: PROB with explicit alias syntax variants
    When parsing the following Locy program:
      """
      CREATE RULE r1 AS
        MATCH (n:Node)
        YIELD KEY n, n.value AS val PROB
      CREATE RULE r2 AS
        MATCH (n:Node)
        YIELD KEY n, n.value AS PROB
      CREATE RULE r3 AS
        MATCH (n:Node)
        YIELD KEY n, n.value PROB
      """
    Then the program should parse successfully

  # ── Compile level ────────────────────────────────────────────────────

  Scenario: Multiple PROB columns should fail to compile
    When compiling the following Locy program:
      """
      CREATE RULE bad AS
        MATCH (n:Node)
        YIELD KEY n, n.x AS p1 PROB, n.y AS p2 PROB
      """
    Then the program should fail to compile
    And the compile error should mention 'PROB'

  Scenario: MNOR output is implicitly PROB
    When compiling the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (n:Node)-[:HAS_RISK]->(r:Risk)
        FOLD score = MNOR(r.score)
        YIELD KEY n, score
      """
    Then the program should compile successfully

  # ── Evaluate level ──────────────────────────────────────────────────

  Scenario: IS NOT with PROB rule yields complement (1-p)
    Given having executed:
      """
      CREATE (:Node {name: 'Alice'})-[:HAS_RISK]->(:Risk {score: 0.7}),
             (:Node {name: 'Bob'})-[:HAS_RISK]->(:Risk {score: 0.3}),
             (:Node {name: 'Charlie'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risky AS
        MATCH (n:Node)-[:HAS_RISK]->(r:Risk)
        YIELD KEY n, r.score AS risk_score PROB
      CREATE RULE safe AS
        MATCH (n:Node)
        WHERE n IS NOT risky
        YIELD KEY n, 1.0 AS safety PROB
      """
    Then evaluation should succeed
    And the derived relation 'risky' should have 2 facts
    And the derived relation 'safe' should have 3 facts
    And the derived relation 'safe' should contain a fact where n.name = 'Alice' and safety = 0.3
    And the derived relation 'safe' should contain a fact where n.name = 'Bob' and safety = 0.7
    And the derived relation 'safe' should contain a fact where n.name = 'Charlie' and safety = 1.0

  Scenario: Absent key yields probability 1.0
    Given having executed:
      """
      CREATE (:Node {name: 'Alice'})-[:HAS_RISK]->(:Risk {score: 0.5}),
             (:Node {name: 'Bob'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risky AS
        MATCH (n:Node)-[:HAS_RISK]->(r:Risk)
        YIELD KEY n, r.score AS risk_score PROB
      CREATE RULE safe AS
        MATCH (n:Node)
        WHERE n IS NOT risky
        YIELD KEY n, 1.0 AS safety PROB
      """
    Then evaluation should succeed
    And the derived relation 'safe' should have 2 facts
    And the derived relation 'safe' should contain a fact where n.name = 'Alice' and safety = 0.5
    And the derived relation 'safe' should contain a fact where n.name = 'Bob' and safety = 1.0

  Scenario: IS NOT without PROB retains Boolean exclusion
    Given having executed:
      """
      CREATE (:Node {name: 'Alice', risk: 0.8}),
             (:Node {name: 'Bob', risk: 0.2}),
             (:Node {name: 'Charlie', risk: 0.1})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE flagged AS MATCH (n:Node) WHERE n.risk > 0.5 YIELD KEY n
      CREATE RULE clean AS MATCH (n:Node) WHERE n IS NOT flagged YIELD KEY n
      """
    Then evaluation should succeed
    And the derived relation 'flagged' should have 1 facts
    And the derived relation 'clean' should have 2 facts
    And the derived relation 'clean' should contain a fact where n.name = 'Bob'
    And the derived relation 'clean' should contain a fact where n.name = 'Charlie'
    And the derived relation 'clean' should not contain a fact where n.name = 'Alice'

  Scenario: Cross-predicate IS + IS NOT combines risk * (1-trust)
    Given having executed:
      """
      CREATE (alice:Node {name: 'Alice'})-[:HAS_RISK]->(:Risk {score: 0.8}),
             (alice)-[:HAS_TRUST]->(:Trust {score: 0.6}),
             (bob:Node {name: 'Bob'})-[:HAS_RISK]->(:Risk {score: 0.5}),
             (bob)-[:HAS_TRUST]->(:Trust {score: 0.2})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risky AS
        MATCH (n:Node)-[:HAS_RISK]->(r:Risk)
        YIELD KEY n, r.score AS risk_score PROB
      CREATE RULE trusted AS
        MATCH (n:Node)-[:HAS_TRUST]->(t:Trust)
        YIELD KEY n, t.score AS trust_score PROB
      CREATE RULE net_risk AS
        MATCH (n:Node)
        WHERE n IS risky, n IS NOT trusted
        YIELD KEY n, risk_score AS combined PROB
      """
    Then evaluation should succeed
    And the derived relation 'net_risk' should have 2 facts
    And the derived relation 'net_risk' should contain a fact where n.name = 'Alice' and combined = 0.32
    And the derived relation 'net_risk' should contain a fact where n.name = 'Bob' and combined = 0.4

  Scenario: Double complement (NOT NOT) recovers original probability
    Given having executed:
      """
      CREATE (:Node {name: 'Alice'})-[:HAS_RISK]->(:Risk {score: 0.7}),
             (:Node {name: 'Charlie'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risky AS
        MATCH (n:Node)-[:HAS_RISK]->(r:Risk)
        YIELD KEY n, r.score AS risk_score PROB
      CREATE RULE safe AS
        MATCH (n:Node)
        WHERE n IS NOT risky
        YIELD KEY n, 1.0 AS safety PROB
      CREATE RULE risky_again AS
        MATCH (n:Node)
        WHERE n IS NOT safe
        YIELD KEY n, 1.0 AS risk2 PROB
      """
    Then evaluation should succeed
    And the derived relation 'risky_again' should contain a fact where n.name = 'Alice' and risk2 = 0.7
    And the derived relation 'risky_again' should contain a fact where n.name = 'Charlie' and risk2 = 0.0

  Scenario: PROB column across strata via IS reference
    Given having executed:
      """
      CREATE (:Node {name: 'Alice'})-[:HAS_RISK]->(:Risk {score: 0.6}),
             (:Node {name: 'Bob'})-[:HAS_RISK]->(:Risk {score: 0.9})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risky AS
        MATCH (n:Node)-[:HAS_RISK]->(r:Risk)
        YIELD KEY n, r.score AS risk_score PROB
      CREATE RULE report AS
        MATCH (n:Node)
        WHERE n IS risky
        YIELD KEY n, risk_score
      """
    Then evaluation should succeed
    And the derived relation 'report' should have 2 facts
    And the derived relation 'report' should contain a fact where n.name = 'Alice' and risk_score = 0.6
    And the derived relation 'report' should contain a fact where n.name = 'Bob' and risk_score = 0.9

  Scenario: strict_probability_domain rejects out-of-range MNOR input
    Given having executed:
      """
      CREATE (:Node {name: 'A'})-[:HAS_RISK]->(:Risk {score: 1.5})
      """
    When evaluating the following Locy program with strict_probability_domain:
      """
      CREATE RULE risk AS
        MATCH (n:Node)-[:HAS_RISK]->(r:Risk)
        FOLD score = MNOR(r.score)
        YIELD KEY n, score
      """
    Then evaluation should fail
    And the evaluation error should mention 'strict_probability_domain'

  Scenario: MNOR with values outside range clamps in non-strict mode
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:HAS_RISK]->(:Risk {score: 1.5}),
             (a)-[:HAS_RISK]->(:Risk {score: 0.3})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (n:Node)-[:HAS_RISK]->(r:Risk)
        FOLD score = MNOR(r.score)
        YIELD KEY n, score
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 1 facts
