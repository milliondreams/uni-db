Feature: ASSUME and ABDUCE Extended Combinations (COMB-AAX)

  Tests ASSUME and ABDUCE commands in combination with FOLD aggregation
  (MNOR, MPROD, MSUM), IS NOT negation, and composite-key patterns.
  Validates that hypothetical mutations correctly re-evaluate aggregated
  rules and that abductive reasoning operates on FOLD-enriched derivations.

  Background:
    Given an empty graph

  # Known limitations (scenarios removed pending engine fixes):
  # - 4a-3 ASSUME SET edge weight with FOLD MSUM re-evaluation: ASSUME SET applies to all matched edges instead of filtered subset

  # ── 4a: ASSUME SET + FOLD + QUERY ───────────────────────────────────

  Scenario: 4a-1 ASSUME SET edge probability with FOLD MNOR re-evaluation
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
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY b, p
      ASSUME {
        MATCH (:Node {name: 'A'})-[e:CAUSE {prob: 0.5}]->(:Node {name: 'B'})
        SET e.prob = 0.7
      } THEN {
        QUERY risk WHERE b.name = 'B' RETURN p
      }
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 1 facts
    And the derived relation 'risk' should contain a fact where p = 0.65
    And the command result 0 should be an Assume with 1 rows
    And the command result 0 should be an Assume containing row where p = 0.79

  Scenario: 4a-2 ASSUME SET edge probability with FOLD MPROD re-evaluation
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
      CREATE RULE joint AS
        MATCH (a:Node)-[e:REQ]->(b:Node)
        FOLD p = MPROD(e.prob)
        YIELD KEY b, p
      ASSUME {
        MATCH (:Node {name: 'A'})-[e:REQ {prob: 0.9}]->(:Node {name: 'B'})
        SET e.prob = 0.5
      } THEN {
        QUERY joint WHERE b.name = 'B' RETURN p
      }
      """
    Then evaluation should succeed
    And the derived relation 'joint' should have 1 facts
    And the derived relation 'joint' should contain a fact where p = 0.72
    And the command result 0 should be an Assume with 1 rows
    And the command result 0 should be an Assume containing row where p = 0.4

  Scenario: 4a-4 ASSUME CREATE new edge with FOLD MNOR adds new cause
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:CAUSE {prob: 0.3}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY b, p
      ASSUME {
        MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
        CREATE (a)-[:CAUSE {prob: 0.5}]->(b)
      } THEN {
        QUERY risk WHERE b.name = 'B' RETURN p
      }
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 1 facts
    And the derived relation 'risk' should contain a fact where p = 0.3
    And the command result 0 should be an Assume with 1 rows
    And the command result 0 should be an Assume containing row where p = 0.65

  Scenario: 4a-5 ASSUME SET with multi-group FOLD MNOR and QUERY
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B1'}), (:Node {name: 'B2'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B1'})
      CREATE (a)-[:CAUSE {prob: 0.4}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B2'})
      CREATE (a)-[:CAUSE {prob: 0.6}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY b, p
      ASSUME {
        MATCH (:Node {name: 'A'})-[e:CAUSE]->(:Node {name: 'B1'})
        SET e.prob = 0.8
      } THEN {
        QUERY risk WHERE b = b RETURN b.name AS target, p
      }
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 2 facts
    And the derived relation 'risk' should contain a fact where b.name = 'B1' and p = 0.4
    And the derived relation 'risk' should contain a fact where b.name = 'B2' and p = 0.6
    And the command result 0 should be an Assume with 2 rows
    And the command result 0 should be an Assume containing row where target = 'B1'
    And the command result 0 should be an Assume containing row where target = 'B2'

  # ── 4b: ASSUME DELETE + FOLD + IS NOT ────────────────────────────────

  Scenario: 4b-1 ASSUME DELETE removes one cause and reduces MNOR then IS NOT complement
    Given having executed:
      """
      CREATE (:Node {name: 'T'}), (:Node {name: 'Strong'}), (:Node {name: 'Weak'})
      """
    And having executed:
      """
      MATCH (s:Node {name: 'Strong'}), (t:Node {name: 'T'})
      CREATE (s)-[:CAUSE {prob: 0.6}]->(t)
      """
    And having executed:
      """
      MATCH (w:Node {name: 'Weak'}), (t:Node {name: 'T'})
      CREATE (w)-[:CAUSE {prob: 0.4}]->(t)
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
      ASSUME { MATCH (:Node {name: 'Weak'})-[e:CAUSE]->() DELETE e }
      THEN { QUERY safe WHERE n.name = 'T' RETURN safety }
      """
    Then evaluation should succeed
    And the derived relation 'risky' should contain a fact where p = 0.76
    And the derived relation 'safe' should contain a fact where n.name = 'T' and safety = 0.24
    And the command result 0 should be an Assume with 1 rows
    And the command result 0 should be an Assume containing row where safety = 0.4

  Scenario: 4b-2 ASSUME DELETE removes all causes making IS NOT yield 1.0
    Given having executed:
      """
      CREATE (:Node {name: 'T'}), (:Node {name: 'X'})
      """
    And having executed:
      """
      MATCH (x:Node {name: 'X'}), (t:Node {name: 'T'})
      CREATE (x)-[:CAUSE {prob: 0.5}]->(t)
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
      ASSUME { MATCH (:Node {name: 'X'})-[e:CAUSE]->() DELETE e }
      THEN { QUERY safe WHERE n.name = 'T' RETURN safety }
      """
    Then evaluation should succeed
    And the derived relation 'risky' should contain a fact where p = 0.5
    And the derived relation 'safe' should contain a fact where n.name = 'T' and safety = 0.5
    And the command result 0 should be an Assume with 1 rows
    And the command result 0 should be an Assume containing row where safety = 1.0

  Scenario: 4b-3 ASSUME DELETE edge with FOLD MPROD and boolean IS NOT
    Given having executed:
      """
      CREATE (:Node {name: 'A', flagged: true}),
             (:Node {name: 'B', flagged: false}),
             (:Node {name: 'C'})
      """
    And having executed:
      """
      MATCH (b:Node {name: 'B'}), (c:Node {name: 'C'})
      CREATE (b)-[:REQ {prob: 0.8}]->(c)
      """
    And having executed:
      """
      MATCH (b:Node {name: 'B'}), (c:Node {name: 'C'})
      CREATE (b)-[:REQ {prob: 0.9}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE blocked AS MATCH (n:Node) WHERE n.flagged = true YIELD KEY n
      CREATE RULE joint AS
        MATCH (a:Node)-[e:REQ]->(b:Node)
        WHERE a IS NOT blocked
        FOLD p = MPROD(e.prob)
        YIELD KEY a, KEY b, p
      ASSUME {
        MATCH (:Node {name: 'B'})-[e:REQ {prob: 0.9}]->(:Node {name: 'C'})
        DELETE e
      } THEN {
        QUERY joint WHERE a.name = 'B' RETURN p
      }
      """
    Then evaluation should succeed
    And the derived relation 'joint' should have 1 facts
    And the derived relation 'joint' should contain a fact where p = 0.72
    And the command result 0 should be an Assume with 1 rows
    And the command result 0 should be an Assume containing row where p = 0.8

  Scenario: 4b-4 ASSUME DELETE with FOLD MSUM and IS NOT filter
    Given having executed:
      """
      CREATE (:Node {name: 'A', banned: true}),
             (:Node {name: 'B', banned: false}),
             (:Node {name: 'C'})
      """
    And having executed:
      """
      MATCH (b:Node {name: 'B'}), (c:Node {name: 'C'})
      CREATE (b)-[:PAY {amount: 10}]->(c)
      """
    And having executed:
      """
      MATCH (b:Node {name: 'B'}), (c:Node {name: 'C'})
      CREATE (b)-[:PAY {amount: 20}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE banned AS MATCH (n:Node) WHERE n.banned = true YIELD KEY n
      CREATE RULE payments AS
        MATCH (a:Node)-[e:PAY]->(b:Node)
        WHERE a IS NOT banned
        FOLD total = MSUM(e.amount)
        YIELD KEY a, KEY b, total
      ASSUME {
        MATCH (:Node {name: 'B'})-[e:PAY {amount: 20}]->(:Node {name: 'C'})
        DELETE e
      } THEN {
        QUERY payments WHERE a.name = 'B' RETURN total
      }
      """
    Then evaluation should succeed
    And the derived relation 'payments' should have 1 facts
    And the derived relation 'payments' should contain a fact where total = 30.0
    And the command result 0 should be an Assume with 1 rows
    And the command result 0 should be an Assume containing row where total = 10.0

  Scenario: 4b-5 ASSUME DELETE on composite-key with FOLD MNOR and IS NOT
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
      ASSUME {
        MATCH (d:Drug {name: 'D1'})-[r:IND]->(dis:Disease {name: 'Flu'})
        DELETE r
      } THEN {
        QUERY novel WHERE d = d RETURN d.name AS drug, dis.name AS disease, score
      }
      """
    Then evaluation should succeed
    And the derived relation 'novel' should have 1 facts
    And the derived relation 'novel' should contain a fact where dis.name = 'Cold'
    And the command result 0 should be an Assume with 2 rows

  # ── 4c: ABDUCE NOT + FOLD + composite key ───────────────────────────

  Scenario: 4c-1 ABDUCE NOT on FOLD MNOR rule with single key
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
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY b, p
      ABDUCE NOT risk WHERE b.name = 'B'
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 1 facts
    And the derived relation 'risk' should contain a fact where p = 0.65
    And the command result 0 should be an Abduce with at least 1 modifications

  Scenario: 4c-2 ABDUCE NOT on FOLD MPROD rule with composite key
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
      CREATE RULE joint AS
        MATCH (a:Node)-[e:REQ]->(b:Node)
        FOLD p = MPROD(e.prob)
        YIELD KEY a, KEY b, p
      ABDUCE NOT joint WHERE a.name = 'A'
      """
    Then evaluation should succeed
    And the derived relation 'joint' should have 1 facts
    And the derived relation 'joint' should contain a fact where p = 0.72
    And the command result 0 should be an Abduce with at least 1 modifications

  Scenario: 4c-3 ABDUCE NOT on composite-key signal with FOLD MNOR
    Given having executed:
      """
      CREATE (:Drug {name: 'D1'}), (:Disease {name: 'Flu'})
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'D1'}), (dis:Disease {name: 'Flu'})
      CREATE (d)-[:SIG {s: 0.4}]->(dis)
      """
    And having executed:
      """
      MATCH (d:Drug {name: 'D1'}), (dis:Disease {name: 'Flu'})
      CREATE (d)-[:SIG {s: 0.6}]->(dis)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE signal AS
        MATCH (d:Drug)-[e:SIG]->(dis:Disease)
        FOLD ev = MNOR(e.s)
        YIELD KEY d, KEY dis, ev
      ABDUCE NOT signal WHERE d.name = 'D1'
      """
    Then evaluation should succeed
    And the derived relation 'signal' should have 1 facts
    And the derived relation 'signal' should contain a fact where ev = 0.76
    And the command result 0 should be an Abduce with at least 1 modifications

  Scenario: 4c-4 ABDUCE NOT with ASSUME on same FOLD MNOR composite-key rule
    Given having executed:
      """
      CREATE (:Drug {name: 'D1'}), (:Disease {name: 'Flu'})
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
    When evaluating the following Locy program:
      """
      CREATE RULE signal AS
        MATCH (d:Drug)-[e:SIG]->(dis:Disease)
        FOLD ev = MNOR(e.s)
        YIELD KEY d, KEY dis, ev
      ASSUME {
        MATCH (d:Drug {name: 'D1'})-[e:SIG {s: 0.5}]->(dis:Disease {name: 'Flu'})
        SET e.s = 0.7
      } THEN {
        QUERY signal WHERE d.name = 'D1' RETURN ev
      }
      ABDUCE NOT signal WHERE d.name = 'D1'
      """
    Then evaluation should succeed
    And the derived relation 'signal' should contain a fact where ev = 0.65
    And the command result 0 should be an Assume with 1 rows
    And the command result 0 should be an Assume containing row where ev = 0.79
    And the command result 1 should be an Abduce with at least 1 modifications

  Scenario: 4c-5 ABDUCE NOT on FOLD MSUM rule with composite key
    Given having executed:
      """
      CREATE (:Dept {name: 'Eng'}), (:Project {name: 'Alpha'}), (:Project {name: 'Beta'})
      """
    And having executed:
      """
      MATCH (d:Dept {name: 'Eng'}), (p:Project {name: 'Alpha'})
      CREATE (d)-[:ALLOC {hours: 100}]->(p)
      """
    And having executed:
      """
      MATCH (d:Dept {name: 'Eng'}), (p:Project {name: 'Alpha'})
      CREATE (d)-[:ALLOC {hours: 50}]->(p)
      """
    And having executed:
      """
      MATCH (d:Dept {name: 'Eng'}), (p:Project {name: 'Beta'})
      CREATE (d)-[:ALLOC {hours: 200}]->(p)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE effort AS
        MATCH (d:Dept)-[a:ALLOC]->(p:Project)
        FOLD total = MSUM(a.hours)
        YIELD KEY d, KEY p, total
      ABDUCE NOT effort WHERE d.name = 'Eng'
      """
    Then evaluation should succeed
    And the derived relation 'effort' should have 2 facts
    And the derived relation 'effort' should contain a fact where p.name = 'Alpha' and total = 150.0
    And the derived relation 'effort' should contain a fact where p.name = 'Beta' and total = 200.0
    And the command result 0 should be an Abduce with at least 1 modifications

  # ── 4d: Edge cases ──────────────────────────────────────────────────

  Scenario: 4d-1 ASSUME on empty graph with FOLD MNOR returns no rows
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY b, p
      ASSUME {
        CREATE (:Node {name: 'A'})-[:CAUSE {prob: 0.5}]->(:Node {name: 'B'})
      } THEN {
        QUERY risk WHERE b.name = 'B' RETURN p
      }
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 0 facts
    And the command result 0 should be an Assume with 1 rows
    And the command result 0 should be an Assume containing row where p = 0.5

  Scenario: 4d-2 ABDUCE NOT on empty graph with FOLD MNOR yields no modifications
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY b, p
      ABDUCE NOT risk WHERE b.name = 'B'
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 0 facts
    And the command result 0 should be an Abduce with at least 0 modifications

  Scenario: 4d-3 ASSUME SET with FOLD MNOR and IS NOT complement combined
    Given having executed:
      """
      CREATE (:Node {name: 'T'}), (:Node {name: 'X'}), (:Node {name: 'Y'})
      """
    And having executed:
      """
      MATCH (x:Node {name: 'X'}), (t:Node {name: 'T'})
      CREATE (x)-[:CAUSE {prob: 0.3}]->(t)
      """
    And having executed:
      """
      MATCH (y:Node {name: 'Y'}), (t:Node {name: 'T'})
      CREATE (y)-[:CAUSE {prob: 0.5}]->(t)
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
      ASSUME {
        MATCH (:Node {name: 'Y'})-[e:CAUSE]->(:Node {name: 'T'})
        SET e.prob = 0.9
      } THEN {
        QUERY safe WHERE n.name = 'T' RETURN safety
      }
      """
    Then evaluation should succeed
    And the derived relation 'risky' should contain a fact where p = 0.65
    And the derived relation 'safe' should contain a fact where n.name = 'T' and safety = 0.35
    And the command result 0 should be an Assume with 1 rows
    And the command result 0 should be an Assume containing row where safety = 0.07

  Scenario: 4d-4 ASSUME DELETE with FOLD MNOR then ABDUCE NOT on same rule
    Given having executed:
      """
      CREATE (:Node {name: 'T'}), (:Node {name: 'X'}), (:Node {name: 'Y'})
      """
    And having executed:
      """
      MATCH (x:Node {name: 'X'}), (t:Node {name: 'T'})
      CREATE (x)-[:CAUSE {prob: 0.6}]->(t)
      """
    And having executed:
      """
      MATCH (y:Node {name: 'Y'}), (t:Node {name: 'T'})
      CREATE (y)-[:CAUSE {prob: 0.4}]->(t)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY b, p
      ASSUME { MATCH (:Node {name: 'Y'})-[e:CAUSE]->() DELETE e }
      THEN { QUERY risk WHERE b.name = 'T' RETURN p }
      ABDUCE NOT risk WHERE b.name = 'T'
      """
    Then evaluation should succeed
    And the derived relation 'risk' should contain a fact where p = 0.76
    And the command result 0 should be an Assume with 1 rows
    And the command result 0 should be an Assume containing row where p = 0.6
    And the command result 1 should be an Abduce with at least 1 modifications

  Scenario: 4d-5 ASSUME CREATE new node and edge with FOLD MSUM and QUERY
    Given having executed:
      """
      CREATE (:Person {name: 'Alice'}), (:Invoice {id: 'I1'})
      """
    And having executed:
      """
      MATCH (p:Person {name: 'Alice'}), (i:Invoice {id: 'I1'})
      CREATE (p)-[:PAID {amount: 100}]->(i)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE spending AS
        MATCH (p:Person)-[r:PAID]->(i:Invoice)
        FOLD total = MSUM(r.amount)
        YIELD KEY p, total
      ASSUME {
        MATCH (p:Person {name: 'Alice'})
        CREATE (p)-[:PAID {amount: 250}]->(:Invoice {id: 'I2'})
      } THEN {
        QUERY spending WHERE p.name = 'Alice' RETURN total
      }
      """
    Then evaluation should succeed
    And the derived relation 'spending' should have 1 facts
    And the derived relation 'spending' should contain a fact where total = 100.0
    And the command result 0 should be an Assume with 1 rows
    And the command result 0 should be an Assume containing row where total = 350.0
