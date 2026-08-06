Feature: Semantic Parity — QUERY Results Must Match Derived Relations

  Every QUERY command result should return the same facts as the corresponding
  derived relation (modulo column aliasing and ordering). This feature validates
  parity across key patterns to catch SLG/fixpoint divergences.

  Background:
    Given an empty graph

  # ── Simple rules ──────────────────────────────────────────────────────

  Scenario: Simple non-recursive rule QUERY matches derived
    Given having executed:
      """
      CREATE (:Node {name: 'A', score: 0.8}),
             (:Node {name: 'B', score: 0.3}),
             (:Node {name: 'C', score: 0.1})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE high AS
        MATCH (n:Node)
        WHERE n.score > 0.5
        YIELD KEY n

      QUERY high WHERE n = n RETURN n.name AS name ORDER BY name
      """
    Then evaluation should succeed
    And the derived relation 'high' should have 1 facts
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where name = 'A'

  # ── FOLD rules ────────────────────────────────────────────────────────

  Scenario: FOLD MNOR QUERY matches derived
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.3}]->(b),
             (a)-[:CAUSE {prob: 0.5}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p

      QUERY risk WHERE a = a RETURN a.name AS src, b.name AS dst, p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 1 facts
    And the derived relation 'risk' should contain a fact where p = 0.65
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where p = 0.65

  Scenario: FOLD MPROD QUERY matches derived
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CHECK {conf: 0.8}]->(b),
             (a)-[:CHECK {conf: 0.9}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE coverage AS
        MATCH (a:Node)-[e:CHECK]->(b:Node)
        FOLD p = MPROD(e.conf)
        YIELD KEY a, KEY b, p

      QUERY coverage WHERE a = a RETURN a.name AS src, b.name AS dst, p
      """
    Then evaluation should succeed
    And the derived relation 'coverage' should have 1 facts
    And the derived relation 'coverage' should contain a fact where p = 0.72
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where p = 0.72

  # ── IS NOT rules ──────────────────────────────────────────────────────

  Scenario: Boolean IS NOT QUERY matches derived
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

      QUERY clean WHERE n = n RETURN n.name AS name ORDER BY name
      """
    Then evaluation should succeed
    And the derived relation 'clean' should have 2 facts
    And the command result 0 should be a Query with 2 rows
    And the command result 0 should be a Query containing row where name = 'Bob'
    And the command result 0 should be a Query containing row where name = 'Charlie'

  Scenario: IS NOT PROB complement derived relation correctness
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
    And the derived relation 'safe' should have 3 facts
    And the derived relation 'safe' should contain a fact where n.name = 'Alice' and safety = 0.3
    And the derived relation 'safe' should contain a fact where n.name = 'Bob' and safety = 0.7
    And the derived relation 'safe' should contain a fact where n.name = 'Charlie' and safety = 1.0

  # ── Composite-key IS NOT ──────────────────────────────────────────────

  Scenario: Composite-key IS NOT QUERY matches derived
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

  # ── Multi-stratum pipeline ────────────────────────────────────────────

  Scenario: Three-stratum FOLD chain QUERY matches derived
    Given having executed:
      """
      CREATE (:Drug {name: 'D1'}),
             (:Disease {name: 'Flu'}),
             (:SE {name: 'nausea', sev: 0.9})
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'D1'}), (se:SE {name: 'nausea'})
      CREATE (d)-[:ADR {freq: 0.7}]->(se)
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

      QUERY safe_drug WHERE d = d RETURN d.name AS drug, safety
      """
    Then evaluation should succeed
    And the derived relation 'safe_drug' should have 1 facts
    And the command result 0 should be a Query with 1 rows

  # ── IS-refs that introduce bindings ───────────────────────────────────
  #
  # The scenarios above all bind every IS-ref subject in the MATCH pattern.
  # That left a hole: an IS-ref may also *introduce* a variable the MATCH
  # pattern does not provide, and the SLG resolver behind QUERY can filter on
  # an already-bound subject but cannot bind a fresh one — so QUERY silently
  # returns nothing while the fixpoint derives facts. See issue #160.
  #
  # These two scenarios are the difference between the generic parity guard
  # protecting against future regressions and actually exercising the bug.

  Scenario: QUERY matches derived when an IS-ref introduces a binding
    Given having executed:
      """
      CREATE (:Person {name: 'alice'}),
             (:Role {name: 'senior'}),
             (:Perm {action: 'WRITE'})
      """
    And having executed:
      """
      MATCH (p:Person {name: 'alice'}), (r:Role {name: 'senior'})
      CREATE (p)-[:HAS_ROLE]->(r)
      """
    And having executed:
      """
      MATCH (r:Role {name: 'senior'}), (p:Perm {action: 'WRITE'})
      CREATE (r)-[:GRANTS]->(p)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE role_perm AS
        MATCH (r:Role)-[:GRANTS]->(p:Perm)
        YIELD KEY r, KEY p

      CREATE RULE holds AS
        MATCH (e:Person)-[:HAS_ROLE]->(r:Role)
        WHERE (r, p) IS role_perm
        YIELD KEY e, KEY p

      QUERY holds WHERE e = e RETURN e.name AS who
      """
    Then evaluation should succeed
    And the derived relation 'holds' should have 1 facts
    And the command result 0 should be a Query with 1 rows

  Scenario: QUERY matches derived for a recursive rule with a fresh binding
    Given having executed:
      """
      CREATE (:Role {name: 'senior'}),
             (:Role {name: 'junior'}),
             (:Perm {action: 'READ'}),
             (:Perm {action: 'WRITE'})
      """
    And having executed:
      """
      MATCH (s:Role {name: 'senior'}), (j:Role {name: 'junior'})
      CREATE (s)-[:INHERITS]->(j)
      """
    And having executed:
      """
      MATCH (r:Role {name: 'junior'}), (p:Perm {action: 'READ'})
      CREATE (r)-[:GRANTS]->(p)
      """
    And having executed:
      """
      MATCH (r:Role {name: 'senior'}), (p:Perm {action: 'WRITE'})
      CREATE (r)-[:GRANTS]->(p)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE role_perm AS
        MATCH (r:Role)-[:GRANTS]->(p:Perm)
        YIELD KEY r, KEY p

      CREATE RULE role_perm AS
        MATCH (r:Role)-[:INHERITS]->(parent:Role)
        WHERE (parent, p) IS role_perm
        YIELD KEY r, KEY p

      QUERY role_perm WHERE r = r RETURN r.name AS role
      """
    Then evaluation should succeed
    And the derived relation 'role_perm' should have 3 facts
    And the command result 0 should be a Query with 3 rows
