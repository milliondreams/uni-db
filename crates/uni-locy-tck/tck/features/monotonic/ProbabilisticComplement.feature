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

  Scenario: IS NOT complement after non-recursive PROB rule
    Given having executed:
      """
      CREATE (:Node {name: 'Alice'})-[:HAS_RISK]->(:Risk {score: 0.8}),
             (:Node {name: 'Bob'})-[:HAS_RISK]->(:Risk {score: 0.4}),
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
    And the derived relation 'safe' should have 3 facts
    And the derived relation 'safe' should contain a fact where n.name = 'Alice' and safety = 0.2
    And the derived relation 'safe' should contain a fact where n.name = 'Bob' and safety = 0.6
    And the derived relation 'safe' should contain a fact where n.name = 'Charlie' and safety = 1.0

  # ── QUERY path (SLG) complement tests ─────────────────────────────────

  Scenario: QUERY returns IS NOT PROB complement rows
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

      QUERY safe WHERE n = n RETURN n.name AS name, safety ORDER BY name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 3 rows
    And the command result 0 should be a Query containing row where name = 'Alice' and safety = 0.3
    And the command result 0 should be a Query containing row where name = 'Bob' and safety = 0.7
    And the command result 0 should be a Query containing row where name = 'Charlie' and safety = 1.0

  Scenario: QUERY with IS NOT PROB absent key returns 1.0
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

      QUERY safe WHERE n.name = 'Bob' RETURN n.name AS name, safety
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where name = 'Bob' and safety = 1.0

  Scenario: QUERY IS NOT PROB complement with FOLD MNOR target
    Given having executed:
      """
      CREATE (:Drug {name: 'DrugA'}),
             (:Drug {name: 'DrugB'}),
             (:SE {name: 'nausea', sev: 0.9}),
             (:SE {name: 'headache', sev: 0.2})
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'DrugA'}), (s:SE {name: 'nausea'})
      CREATE (d)-[:ADR {freq: 0.7}]->(s)
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'DrugA'}), (s:SE {name: 'headache'})
      CREATE (d)-[:ADR {freq: 0.3}]->(s)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE adr_risk AS
        MATCH (d:Drug)-[c:ADR]->(se:SE)
        YIELD KEY d, c.freq * se.sev AS hazard PROB

      CREATE RULE safety_penalty AS
        MATCH (d:Drug)
        WHERE d IS adr_risk
        FOLD penalty = MNOR(hazard)
        YIELD KEY d, penalty

      CREATE RULE safe_drug AS
        MATCH (d:Drug)
        WHERE d IS NOT safety_penalty
        YIELD KEY d, 1.0 AS safety PROB

      QUERY safe_drug WHERE d = d RETURN d.name AS drug, safety ORDER BY drug
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 2 rows

  Scenario: QUERY with cross-predicate IS + IS NOT PROB
    Given having executed:
      """
      CREATE (:Account {name: 'Alice'})-[:HAS_RISK]->(:Risk {score: 0.8}),
             (:Account {name: 'Alice'})-[:HAS_TRUST]->(:Trust {level: 0.6}),
             (:Account {name: 'Bob'})-[:HAS_RISK]->(:Risk {score: 0.5}),
             (:Account {name: 'Charlie'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risky AS
        MATCH (a:Account)-[:HAS_RISK]->(r:Risk)
        YIELD KEY a, r.score AS risk_score PROB

      CREATE RULE trusted AS
        MATCH (a:Account)-[:HAS_TRUST]->(t:Trust)
        YIELD KEY a, t.level AS trust_score PROB

      CREATE RULE net_risk AS
        MATCH (a:Account)
        WHERE a IS risky, a IS NOT trusted
        YIELD KEY a, risk_score AS combined PROB

      QUERY net_risk WHERE a = a RETURN a.name AS name, combined ORDER BY name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 2 rows

  Scenario: QUERY FOLD rule returns node properties after enrichment
    Given having executed:
      """
      CREATE (:Node {name: 'Alice'})-[:HAS_RISK]->(:Risk {score: 0.7}),
             (:Node {name: 'Bob'})-[:HAS_RISK]->(:Risk {score: 0.3})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk_agg AS
        MATCH (n:Node)-[:HAS_RISK]->(r:Risk)
        FOLD total_risk = MNOR(r.score)
        YIELD KEY n, total_risk

      QUERY risk_agg WHERE n = n RETURN n.name AS name, total_risk ORDER BY name
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 2 rows
    And the command result 0 should be a Query containing row where name = 'Alice'
    And the command result 0 should be a Query containing row where name = 'Bob'

  Scenario: Composite-key IS NOT with target variable
    Given having executed:
      """
      CREATE (:Drug {name: 'A'}), (:Disease {name: 'Flu'}), (:Disease {name: 'Cold'})
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'A'}), (dis:Disease {name: 'Flu'}) CREATE (d)-[:IND]->(dis)
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'A'}), (dis:Disease {name: 'Flu'}) CREATE (d)-[:SIG]->(dis)
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'A'}), (dis:Disease {name: 'Cold'}) CREATE (d)-[:SIG]->(dis)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE signal AS
        MATCH (d:Drug)-[:SIG]->(dis:Disease)
        YIELD KEY d, KEY dis

      CREATE RULE known AS
        MATCH (d:Drug)-[:IND]->(dis:Disease)
        YIELD KEY d, KEY dis

      CREATE RULE novel AS
        MATCH (d:Drug)
        WHERE d IS signal TO dis, d IS NOT known TO dis
        YIELD KEY d, KEY dis

      QUERY novel WHERE d = d RETURN d.name AS drug, dis.name AS disease
      """
    Then evaluation should succeed
    And the derived relation 'novel' should have 1 facts
    And the derived relation 'novel' should contain a fact where d.name = 'A' and dis.name = 'Cold'
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where disease = 'Cold'
