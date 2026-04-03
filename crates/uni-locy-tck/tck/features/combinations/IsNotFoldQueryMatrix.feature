Feature: IS NOT + FOLD + QUERY Combination Matrix (COMB-IFQ)

  Systematically tests the interaction of IS NOT (boolean exclusion and
  PROB complement), FOLD aggregation (MNOR, MPROD, MSUM), and QUERY
  projection. Covers single-key and composite-key patterns, non-recursive
  and recursive rules, and verifies that derived relations and QUERY
  results agree on computed values.

  Background:
    Given an empty graph

  # ── 2a: Boolean IS NOT + FOLD + QUERY ────────────────────────────────

  Scenario: 2a-1 Boolean IS NOT with FOLD MNOR and QUERY
    Given having executed:
      """
      CREATE (:Node {name: 'A', risk: 0.8}),
             (:Node {name: 'B', risk: 0.2}),
             (:Node {name: 'C', risk: 0.1})
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
    When evaluating the following Locy program:
      """
      CREATE RULE flagged AS MATCH (n:Node) WHERE n.risk > 0.5 YIELD KEY n
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        WHERE a IS NOT flagged
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      QUERY risk WHERE a = a RETURN a.name AS src, b.name AS dst, p
      """
    Then evaluation should succeed
    And the derived relation 'flagged' should have 1 facts
    And the derived relation 'risk' should have 0 facts
    And the command result 0 should be a Query with 0 rows

  Scenario: 2a-2 Boolean IS NOT passes unflagged nodes to FOLD MNOR with QUERY
    Given having executed:
      """
      CREATE (:Node {name: 'A', risk: 0.2}),
             (:Node {name: 'B', risk: 0.9})
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
    When evaluating the following Locy program:
      """
      CREATE RULE flagged AS MATCH (n:Node) WHERE n.risk > 0.5 YIELD KEY n
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        WHERE a IS NOT flagged
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      QUERY risk RETURN a.name AS src, p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 1 facts
    And the derived relation 'risk' should contain a fact where p = 0.65
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where p = 0.65

  Scenario: 2a-3 Boolean IS NOT with FOLD MPROD and QUERY
    Given having executed:
      """
      CREATE (:Node {name: 'A', risk: 0.1}),
             (:Node {name: 'B', risk: 0.9})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:REQ {prob: 0.8}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:REQ {prob: 0.9}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE flagged AS MATCH (n:Node) WHERE n.risk > 0.5 YIELD KEY n
      CREATE RULE joint AS
        MATCH (a:Node)-[e:REQ]->(b:Node)
        WHERE a IS NOT flagged
        FOLD p = MPROD(e.prob)
        YIELD KEY a, KEY b, p
      QUERY joint RETURN a.name AS src, p
      """
    Then evaluation should succeed
    And the derived relation 'joint' should have 1 facts
    And the derived relation 'joint' should contain a fact where p = 0.72
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where p = 0.72

  Scenario: 2a-4 Boolean IS NOT with FOLD MSUM and QUERY
    Given having executed:
      """
      CREATE (:Node {name: 'A', risk: 0.1}),
             (:Node {name: 'B', risk: 0.8})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:PAY {amount: 10}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:PAY {amount: 20}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE flagged AS MATCH (n:Node) WHERE n.risk > 0.5 YIELD KEY n
      CREATE RULE spending AS
        MATCH (a:Node)-[e:PAY]->(b:Node)
        WHERE a IS NOT flagged
        FOLD total = MSUM(e.amount)
        YIELD KEY a, KEY b, total
      QUERY spending RETURN a.name AS src, total
      """
    Then evaluation should succeed
    And the derived relation 'spending' should have 1 facts
    And the derived relation 'spending' should contain a fact where total = 30.0
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where total = 30.0

  Scenario: 2a-5 Boolean IS NOT with empty negated relation passes all to FOLD MNOR
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:CAUSE {prob: 0.3}]->(b:Node {name: 'B'}),
             (a)-[:CAUSE {prob: 0.5}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE flagged AS MATCH (n:Node) WHERE n.risk > 100 YIELD KEY n
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        WHERE a IS NOT flagged
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      QUERY risk RETURN a.name AS src, p
      """
    Then evaluation should succeed
    And the derived relation 'flagged' should have 0 facts
    And the derived relation 'risk' should have 1 facts
    And the derived relation 'risk' should contain a fact where p = 0.65
    And the command result 0 should be a Query containing row where p = 0.65

  Scenario: 2a-6 Boolean IS NOT multi-group FOLD MNOR with QUERY
    Given having executed:
      """
      CREATE (:Node {name: 'A', risk: 0.1}),
             (:Node {name: 'B', risk: 0.1}),
             (:Node {name: 'T1'}),
             (:Node {name: 'T2'}),
             (:Node {name: 'X', risk: 0.9})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (t:Node {name: 'T1'})
      CREATE (a)-[:CAUSE {prob: 0.4}]->(t)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (t:Node {name: 'T1'})
      CREATE (a)-[:CAUSE {prob: 0.6}]->(t)
      """
    And having executed:
      """
      MATCH (b:Node {name: 'B'}), (t:Node {name: 'T2'})
      CREATE (b)-[:CAUSE {prob: 0.5}]->(t)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE flagged AS MATCH (n:Node) WHERE n.risk > 0.5 YIELD KEY n
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        WHERE a IS NOT flagged
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      QUERY risk WHERE a = a RETURN a.name AS src, b.name AS dst, p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 2 facts
    And the derived relation 'risk' should contain a fact where p = 0.76
    And the derived relation 'risk' should contain a fact where p = 0.5
    And the command result 0 should be a Query with 2 rows

  # ── 2b: PROB IS NOT complement + FOLD + QUERY ───────────────────────

  Scenario: 2b-1 PROB complement after FOLD MNOR with QUERY
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
    When evaluating the following Locy program:
      """
      CREATE RULE risky AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY b, p
      CREATE RULE safe AS
        MATCH (n:Node)
        WHERE n IS NOT risky
        YIELD KEY n, 1.0 AS safety PROB
      QUERY safe WHERE n = n RETURN n.name AS name, safety ORDER BY name
      """
    Then evaluation should succeed
    And the derived relation 'risky' should have 1 facts
    And the derived relation 'risky' should contain a fact where p = 0.65
    And the derived relation 'safe' should have 2 facts
    And the derived relation 'safe' should contain a fact where n.name = 'A' and safety = 1.0
    And the derived relation 'safe' should contain a fact where n.name = 'B' and safety = 0.35
    And the command result 0 should be a Query with 2 rows
    And the command result 0 should be a Query containing row where name = 'A'

  Scenario: 2b-2 PROB complement after FOLD MPROD with QUERY
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:REQ {prob: 0.8}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:REQ {prob: 0.9}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reliable AS
        MATCH (a:Node)-[e:REQ]->(b:Node)
        FOLD p = MPROD(e.prob)
        YIELD KEY b, p
      CREATE RULE unreliable AS
        MATCH (n:Node)
        WHERE n IS NOT reliable
        YIELD KEY n, 1.0 AS fragility PROB
      QUERY unreliable WHERE n = n RETURN n.name AS name, fragility ORDER BY name
      """
    Then evaluation should succeed
    And the derived relation 'reliable' should have 1 facts
    And the derived relation 'reliable' should contain a fact where p = 0.72
    And the derived relation 'unreliable' should have 2 facts
    And the derived relation 'unreliable' should contain a fact where n.name = 'A' and fragility = 1.0
    And the derived relation 'unreliable' should contain a fact where n.name = 'B' and fragility = 0.28
    And the command result 0 should be a Query with 2 rows

  Scenario: 2b-3 PROB complement absent key yields 1.0 with FOLD MNOR and QUERY
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'}), (:Node {name: 'C'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.4}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risky AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY b, p
      CREATE RULE safe AS
        MATCH (n:Node)
        WHERE n IS NOT risky
        YIELD KEY n, 1.0 AS safety PROB
      QUERY safe WHERE n = n RETURN n.name AS name, safety ORDER BY name
      """
    Then evaluation should succeed
    And the derived relation 'safe' should have 3 facts
    And the derived relation 'safe' should contain a fact where n.name = 'A' and safety = 1.0
    And the derived relation 'safe' should contain a fact where n.name = 'B' and safety = 0.6
    And the derived relation 'safe' should contain a fact where n.name = 'C' and safety = 1.0
    And the command result 0 should be a Query with 3 rows

  Scenario: 2b-4 Cross-predicate IS + IS NOT PROB with FOLD MNOR and QUERY
    Given having executed:
      """
      CREATE (:Account {name: 'Alice'}), (:Account {name: 'Bob'})
      """
    And having executed:
      """
      MATCH (a:Account {name: 'Alice'})
      CREATE (a)-[:RISK {prob: 0.3}]->(:Factor {type: 'fraud'})
      """
    And having executed:
      """
      MATCH (a:Account {name: 'Alice'})
      CREATE (a)-[:RISK {prob: 0.5}]->(:Factor {type: 'geo'})
      """
    And having executed:
      """
      MATCH (a:Account {name: 'Alice'})
      CREATE (a)-[:TRUST {level: 0.4}]->(:Source {type: 'kyc'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risky AS
        MATCH (a:Account)-[r:RISK]->(f:Factor)
        FOLD score = MNOR(r.prob)
        YIELD KEY a, score
      CREATE RULE trusted AS
        MATCH (a:Account)-[t:TRUST]->(s:Source)
        YIELD KEY a, t.level AS trust_score PROB
      CREATE RULE net_risk AS
        MATCH (a:Account)
        WHERE a IS risky, a IS NOT trusted
        YIELD KEY a, score AS combined PROB
      QUERY net_risk WHERE a = a RETURN a.name AS name, combined ORDER BY name
      """
    Then evaluation should succeed
    And the derived relation 'risky' should have 1 facts
    And the derived relation 'risky' should contain a fact where score = 0.65
    And the derived relation 'net_risk' should have 1 facts
    And the derived relation 'net_risk' should contain a fact where combined = 0.39
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where combined = 0.39

  Scenario: 2b-5 Double complement recovers original FOLD MNOR probability
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
    When evaluating the following Locy program:
      """
      CREATE RULE risky AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY b, p
      CREATE RULE safe AS
        MATCH (n:Node)
        WHERE n IS NOT risky
        YIELD KEY n, 1.0 AS safety PROB
      CREATE RULE risky_again AS
        MATCH (n:Node)
        WHERE n IS NOT safe
        YIELD KEY n, 1.0 AS risk2 PROB
      QUERY risky_again WHERE n = n RETURN n.name AS name, risk2 ORDER BY name
      """
    Then evaluation should succeed
    And the derived relation 'risky_again' should contain a fact where n.name = 'A' and risk2 = 0.0
    And the derived relation 'risky_again' should contain a fact where n.name = 'B' and risk2 = 0.65
    And the command result 0 should be a Query with 2 rows

  Scenario: 2b-6 PROB complement with FOLD MNOR three causes and QUERY
    Given having executed:
      """
      CREATE (:Node {name: 'T'})
      """
    And having executed:
      """
      MATCH (t:Node {name: 'T'})
      CREATE (:Node {name: 'X'})-[:CAUSE {prob: 0.2}]->(t)
      """
    And having executed:
      """
      MATCH (t:Node {name: 'T'})
      CREATE (:Node {name: 'Y'})-[:CAUSE {prob: 0.3}]->(t)
      """
    And having executed:
      """
      MATCH (t:Node {name: 'T'})
      CREATE (:Node {name: 'Z'})-[:CAUSE {prob: 0.4}]->(t)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risky AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY b, p
      CREATE RULE safe AS
        MATCH (n:Node)
        WHERE n IS NOT risky
        YIELD KEY n, 1.0 AS safety PROB
      QUERY safe WHERE n.name = 'T' RETURN n.name AS name, safety
      """
    Then evaluation should succeed
    And the derived relation 'risky' should contain a fact where p = 0.664
    And the derived relation 'safe' should contain a fact where n.name = 'T' and safety = 0.336
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where safety = 0.336

  # ── 2c: Composite-key IS NOT + FOLD + QUERY ─────────────────────────

  Scenario: 2c-1 Composite-key boolean IS NOT with FOLD MNOR and QUERY
    Given having executed:
      """
      CREATE (:Drug {name: 'D1'}), (:Disease {name: 'Flu'}), (:Disease {name: 'Cold'})
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'D1'}), (dis:Disease {name: 'Flu'})
      CREATE (d)-[:SIG {s: 0.3}]->(dis)
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'D1'}), (dis:Disease {name: 'Flu'})
      CREATE (d)-[:SIG {s: 0.5}]->(dis)
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'D1'}), (dis:Disease {name: 'Cold'})
      CREATE (d)-[:SIG {s: 0.6}]->(dis)
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'D1'}), (dis:Disease {name: 'Flu'})
      CREATE (d)-[:IND]->(dis)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE signal AS
        MATCH (d:Drug)-[e:SIG]->(dis:Disease)
        FOLD ev = MNOR(e.s)
        YIELD KEY d, KEY dis, ev
      CREATE RULE known AS
        MATCH (d:Drug)-[:IND]->(dis:Disease)
        YIELD KEY d, KEY dis
      CREATE RULE novel AS
        MATCH (d:Drug)
        WHERE d IS signal TO dis, d IS NOT known TO dis
        YIELD KEY d, KEY dis, ev AS score
      QUERY novel WHERE d = d RETURN d.name AS drug, dis.name AS disease, score
      """
    Then evaluation should succeed
    And the derived relation 'signal' should have 2 facts
    And the derived relation 'novel' should have 1 facts
    And the derived relation 'novel' should contain a fact where dis.name = 'Cold' and score = 0.6
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where disease = 'Cold'

  Scenario: 2c-2 Composite-key PROB IS NOT complement with FOLD MNOR and QUERY
    Given having executed:
      """
      CREATE (:Drug {name: 'D1'}), (:SE {name: 'nausea'}), (:SE {name: 'rash'})
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'D1'}), (s:SE {name: 'nausea'})
      CREATE (d)-[:ADR {freq: 0.3}]->(s)
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'D1'}), (s:SE {name: 'nausea'})
      CREATE (d)-[:ADR {freq: 0.5}]->(s)
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'D1'}), (s:SE {name: 'rash'})
      CREATE (d)-[:ADR {freq: 0.4}]->(s)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE adr_risk AS
        MATCH (d:Drug)-[c:ADR]->(se:SE)
        FOLD p = MNOR(c.freq)
        YIELD KEY d, KEY se, p
      CREATE RULE adr_safe AS
        MATCH (d:Drug), (se:SE)
        WHERE d IS NOT adr_risk TO se
        YIELD KEY d, KEY se, 1.0 AS safety PROB
      QUERY adr_safe WHERE d = d RETURN d.name AS drug, se.name AS side_effect, safety
      """
    Then evaluation should succeed
    And the derived relation 'adr_risk' should have 2 facts
    And the derived relation 'adr_risk' should contain a fact where se.name = 'nausea' and p = 0.65
    And the derived relation 'adr_risk' should contain a fact where se.name = 'rash' and p = 0.4
    And the derived relation 'adr_safe' should contain a fact where se.name = 'nausea' and safety = 0.35
    And the derived relation 'adr_safe' should contain a fact where se.name = 'rash' and safety = 0.6
    And the command result 0 should be a Query with 2 rows

  Scenario: 2c-3 Composite-key IS NOT all pairs known yields empty novel set
    Given having executed:
      """
      CREATE (:Drug {name: 'D1'}), (:Disease {name: 'Flu'})
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'D1'}), (dis:Disease {name: 'Flu'})
      CREATE (d)-[:SIG {s: 0.7}]->(dis)
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'D1'}), (dis:Disease {name: 'Flu'})
      CREATE (d)-[:IND]->(dis)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE signal AS
        MATCH (d:Drug)-[e:SIG]->(dis:Disease)
        FOLD ev = MNOR(e.s)
        YIELD KEY d, KEY dis, ev
      CREATE RULE known AS
        MATCH (d:Drug)-[:IND]->(dis:Disease)
        YIELD KEY d, KEY dis
      CREATE RULE novel AS
        MATCH (d:Drug)
        WHERE d IS signal TO dis, d IS NOT known TO dis
        YIELD KEY d, KEY dis, ev AS score
      QUERY novel WHERE d = d RETURN d.name AS drug, dis.name AS disease, score
      """
    Then evaluation should succeed
    And the derived relation 'novel' should have 0 facts
    And the command result 0 should be a Query with 0 rows

  Scenario: 2c-4 Composite-key IS NOT with multiple drugs and FOLD MNOR QUERY
    Given having executed:
      """
      CREATE (:Drug {name: 'D1'}), (:Drug {name: 'D2'}),
             (:Disease {name: 'Flu'}), (:Disease {name: 'Cold'})
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'D1'}), (dis:Disease {name: 'Flu'})
      CREATE (d)-[:SIG {s: 0.6}]->(dis)
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'D1'}), (dis:Disease {name: 'Cold'})
      CREATE (d)-[:SIG {s: 0.7}]->(dis)
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'D2'}), (dis:Disease {name: 'Flu'})
      CREATE (d)-[:SIG {s: 0.8}]->(dis)
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'D1'}), (dis:Disease {name: 'Flu'})
      CREATE (d)-[:IND]->(dis)
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'D2'}), (dis:Disease {name: 'Flu'})
      CREATE (d)-[:IND]->(dis)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE signal AS
        MATCH (d:Drug)-[e:SIG]->(dis:Disease)
        FOLD ev = MNOR(e.s)
        YIELD KEY d, KEY dis, ev
      CREATE RULE known AS
        MATCH (d:Drug)-[:IND]->(dis:Disease)
        YIELD KEY d, KEY dis
      CREATE RULE novel AS
        MATCH (d:Drug)
        WHERE d IS signal TO dis, d IS NOT known TO dis
        YIELD KEY d, KEY dis, ev AS score
      QUERY novel WHERE d = d RETURN d.name AS drug, dis.name AS disease, score
      """
    Then evaluation should succeed
    And the derived relation 'novel' should have 1 facts
    And the derived relation 'novel' should contain a fact where d.name = 'D1' and dis.name = 'Cold'
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where drug = 'D1'

  # ── 2d: IS NOT + recursive FOLD + QUERY ─────────────────────────────

  Scenario: 2d-1 Boolean IS NOT feeding recursive FOLD MSUM with QUERY
    Given having executed:
      """
      CREATE (:Node {name: 'A', bad: false}),
             (:Node {name: 'B', bad: true}),
             (:Node {name: 'C', bad: false})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'})
      CREATE (a)-[:EDGE {weight: 10}]->(c)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:EDGE {weight: 5}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE banned AS MATCH (n:Node) WHERE n.bad = true YIELD KEY n
      CREATE RULE traffic AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        WHERE a IS NOT banned, b IS NOT banned
        FOLD total = MSUM(e.weight)
        YIELD KEY a, KEY b, total
      QUERY traffic WHERE a = a RETURN a.name AS src, b.name AS dst, total
      """
    Then evaluation should succeed
    And the derived relation 'banned' should have 1 facts
    And the derived relation 'traffic' should have 1 facts
    And the derived relation 'traffic' should contain a fact where total = 10.0
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where src = 'A'

  Scenario: 2d-2 PROB complement IS NOT with recursive MNOR transitive risk and QUERY
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'}), (:Node {name: 'C'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.3}]->(b)
      """
    And having executed:
      """
      MATCH (b:Node {name: 'B'}), (c:Node {name: 'C'})
      CREATE (b)-[:CAUSE {prob: 0.5}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risky AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY b, p
      CREATE RULE safe AS
        MATCH (n:Node)
        WHERE n IS NOT risky
        YIELD KEY n, 1.0 AS safety PROB
      QUERY safe WHERE n = n RETURN n.name AS name, safety ORDER BY name
      """
    Then evaluation should succeed
    And the derived relation 'risky' should have 2 facts
    And the derived relation 'risky' should contain a fact where b.name = 'B' and p = 0.3
    And the derived relation 'risky' should contain a fact where b.name = 'C' and p = 0.5
    And the derived relation 'safe' should have 3 facts
    And the derived relation 'safe' should contain a fact where n.name = 'A' and safety = 1.0
    And the derived relation 'safe' should contain a fact where n.name = 'B' and safety = 0.7
    And the derived relation 'safe' should contain a fact where n.name = 'C' and safety = 0.5
    And the command result 0 should be a Query with 3 rows

  Scenario: 2d-3 Boolean IS NOT chain with FOLD MSUM at each stage and QUERY
    Given having executed:
      """
      CREATE (:Node {name: 'A', tier: 1}),
             (:Node {name: 'B', tier: 2}),
             (:Node {name: 'C', tier: 1})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:FLOW {amount: 15}]->(b)
      """
    And having executed:
      """
      MATCH (c:Node {name: 'C'}), (b:Node {name: 'B'})
      CREATE (c)-[:FLOW {amount: 25}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE restricted AS MATCH (n:Node) WHERE n.tier > 1 YIELD KEY n
      CREATE RULE inflow AS
        MATCH (a:Node)-[e:FLOW]->(b:Node)
        WHERE a IS NOT restricted
        FOLD total = MSUM(e.amount)
        YIELD KEY b, total
      QUERY inflow WHERE b = b RETURN b.name AS target, total
      """
    Then evaluation should succeed
    And the derived relation 'inflow' should have 1 facts
    And the derived relation 'inflow' should contain a fact where total = 40.0
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where total = 40.0

  Scenario: 2d-4 IS NOT PROB complement with FOLD MPROD and chained QUERY
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:REQ {prob: 0.8}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:REQ {prob: 0.9}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reliable AS
        MATCH (a:Node)-[e:REQ]->(b:Node)
        FOLD p = MPROD(e.prob)
        YIELD KEY b, p
      CREATE RULE fragile AS
        MATCH (n:Node)
        WHERE n IS NOT reliable
        YIELD KEY n, 1.0 AS frag PROB
      QUERY fragile WHERE n = n RETURN n.name AS name, frag ORDER BY name
      QUERY reliable WHERE b = b RETURN b.name AS name, p ORDER BY name
      """
    Then evaluation should succeed
    And the derived relation 'reliable' should contain a fact where p = 0.72
    And the derived relation 'fragile' should contain a fact where n.name = 'A' and frag = 1.0
    And the derived relation 'fragile' should contain a fact where n.name = 'B' and frag = 0.28
    And the command result 0 should be a Query with 2 rows
    And the command result 1 should be a Query with 1 rows
    And the command result 1 should be a Query containing row where p = 0.72
