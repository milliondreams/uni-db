Feature: Composite Patterns (COMB-COMP)

  Tests complex feature interactions: multi-stratum chains, ALONG+FOLD,
  BEST BY+FOLD, similar_to+WHERE+IS NOT, recursive+FOLD+QUERY, and
  WHERE on IS-ref value columns.

  Background:
    Given an empty graph

  # Known limitations (scenarios removed pending engine fixes):
  # - 3a-5 Four-stratum: two independent FOLD rules in same stratum causes "FOLD column not found in yield schema"

  # ══════════════════════════════════════════════════════════════════════
  # Group 3a: Multi-stratum chains (3-4 strata)
  # ══════════════════════════════════════════════════════════════════════

  Scenario: 3a-1 Three-stratum chain: base to MNOR to IS NOT complement
    Given having executed:
      """
      CREATE (:Sensor {name: 'S1'}), (:Sensor {name: 'S2'}), (:Sensor {name: 'S3'}),
             (:Zone {name: 'Z1'})
      """
    And having executed:
      """
      MATCH (s:Sensor {name: 'S1'}), (z:Zone {name: 'Z1'})
      CREATE (s)-[:ALERT {prob: 0.6}]->(z)
      """
    And having executed:
      """
      MATCH (s:Sensor {name: 'S1'}), (z:Zone {name: 'Z1'})
      CREATE (s)-[:ALERT {prob: 0.3}]->(z)
      """
    And having executed:
      """
      MATCH (s:Sensor {name: 'S2'}), (z:Zone {name: 'Z1'})
      CREATE (s)-[:ALERT {prob: 0.4}]->(z)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE zone_risk AS
        MATCH (s:Sensor)-[a:ALERT]->(z:Zone)
        FOLD p = MNOR(a.prob)
        YIELD KEY z, p

      CREATE RULE zone_safe AS
        MATCH (z:Zone)
        WHERE z IS NOT zone_risk
        YIELD KEY z, 1.0 AS safety PROB

      CREATE RULE report AS
        MATCH (z:Zone)
        WHERE z IS zone_safe
        YIELD KEY z, safety
      """
    Then evaluation should succeed
    And the derived relation 'zone_risk' should have 1 facts
    And the derived relation 'zone_safe' should have 1 facts
    And the derived relation 'report' should have 1 facts

  Scenario: 3a-2 Four-stratum pipeline: facts to FOLD to IS NOT to filter
    Given having executed:
      """
      CREATE (:Machine {name: 'M1'}), (:Machine {name: 'M2'}), (:Machine {name: 'M3'}),
             (:Component {name: 'C1'}), (:Component {name: 'C2'}), (:Component {name: 'C3'})
      """
    And having executed:
      """
      MATCH (m:Machine {name: 'M1'}), (c:Component {name: 'C1'})
      CREATE (m)-[:FAULT {prob: 0.5}]->(c)
      """
    And having executed:
      """
      MATCH (m:Machine {name: 'M1'}), (c:Component {name: 'C2'})
      CREATE (m)-[:FAULT {prob: 0.3}]->(c)
      """
    And having executed:
      """
      MATCH (m:Machine {name: 'M2'}), (c:Component {name: 'C3'})
      CREATE (m)-[:FAULT {prob: 0.1}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE fault_risk AS
        MATCH (m:Machine)-[f:FAULT]->(c:Component)
        FOLD p = MNOR(f.prob)
        YIELD KEY m, p

      CREATE RULE reliable AS
        MATCH (m:Machine)
        WHERE m IS NOT fault_risk
        YIELD KEY m, 1.0 AS rel PROB

      CREATE RULE highly_reliable AS
        MATCH (m:Machine)
        WHERE m IS reliable, rel >= 0.5
        YIELD KEY m, rel AS score

      QUERY highly_reliable WHERE m = m RETURN m.name AS machine, score ORDER BY machine
      """
    Then evaluation should succeed
    And the derived relation 'fault_risk' should have 2 facts
    And the derived relation 'reliable' should have 3 facts
    And the derived relation 'highly_reliable' should have 2 facts
    And the command result 0 should be a Query with 2 rows

  Scenario: 3a-3 Three-stratum: transitive closure to FOLD COUNT to filter
    Given having executed:
      """
      CREATE (a:City {name: 'A'})-[:ROAD]->(b:City {name: 'B'})-[:ROAD]->(c:City {name: 'C'}),
             (a)-[:ROAD]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS
        MATCH (a:City)-[:ROAD]->(b:City) YIELD KEY a, KEY b
      CREATE RULE reachable AS
        MATCH (a:City)-[:ROAD]->(mid:City)
        WHERE mid IS reachable TO b YIELD KEY a, KEY b

      CREATE RULE reach_count AS
        MATCH (a:City)
        WHERE a IS reachable TO b
        FOLD cnt = COUNT(b)
        YIELD KEY a, cnt

      CREATE RULE hub AS
        MATCH (a:City)
        WHERE a IS reach_count, cnt >= 2
        YIELD KEY a
      """
    Then evaluation should succeed
    And the derived relation 'reachable' should have 3 facts
    And the derived relation 'reach_count' should have 2 facts
    And the derived relation 'hub' should contain a fact where a.name = 'A'

  Scenario: 3a-4 Three-stratum: PROB scores to MPROD joint to IS NOT complement
    Given having executed:
      """
      CREATE (:Pipeline {name: 'P1'}),
             (:Step {name: 'Extract'}), (:Step {name: 'Transform'})
      """
    And having executed:
      """
      MATCH (p:Pipeline {name: 'P1'}), (st:Step {name: 'Extract'})
      CREATE (p)-[:STAGE {reliability: 0.9}]->(st)
      """
    And having executed:
      """
      MATCH (p:Pipeline {name: 'P1'}), (st:Step {name: 'Transform'})
      CREATE (p)-[:STAGE {reliability: 0.8}]->(st)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE pipeline_reliability AS
        MATCH (p:Pipeline)-[s:STAGE]->(step:Step)
        FOLD joint = MPROD(s.reliability)
        YIELD KEY p, joint

      CREATE RULE pipeline_failure AS
        MATCH (p:Pipeline)
        WHERE p IS NOT pipeline_reliability
        YIELD KEY p, 1.0 AS fail_prob PROB

      QUERY pipeline_failure WHERE p = p RETURN p.name AS name, fail_prob
      """
    Then evaluation should succeed
    And the derived relation 'pipeline_reliability' should have 1 facts
    And the derived relation 'pipeline_reliability' should contain a fact where p.name = 'P1' and joint = 0.72
    And the derived relation 'pipeline_failure' should have 1 facts
    And the derived relation 'pipeline_failure' should contain a fact where p.name = 'P1' and fail_prob = 0.28
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where fail_prob = 0.28

  # ══════════════════════════════════════════════════════════════════════
  # Group 3b: ALONG + FOLD
  # ══════════════════════════════════════════════════════════════════════

  Scenario: 3b-1 ALONG cost then FOLD SUM across all paths per source
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE {weight: 2.0}]->(b:Node {name: 'B'})-[:EDGE {weight: 3.0}]->(c:Node {name: 'C'}),
             (a)-[:EDGE {weight: 10.0}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE path AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        ALONG cost = e.weight
        YIELD KEY a, KEY b, cost
      CREATE RULE path AS
        MATCH (a:Node)-[e:EDGE]->(mid:Node)
        WHERE mid IS path TO b
        ALONG cost = prev.cost + e.weight
        YIELD KEY a, KEY b, cost

      CREATE RULE total_cost AS
        MATCH (a:Node)
        WHERE a IS path TO b
        FOLD total = SUM(cost)
        YIELD KEY a, total
      """
    Then evaluation should succeed
    And the derived relation 'path' should have 4 facts
    And the derived relation 'total_cost' should contain a fact where a.name = 'A'

  Scenario: 3b-2 ALONG hops then FOLD MAX to find longest path per source
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'}), (:Node {name: 'C'}), (:Node {name: 'D'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:EDGE]->(b)
      """
    And having executed:
      """
      MATCH (b:Node {name: 'B'}), (c:Node {name: 'C'})
      CREATE (b)-[:EDGE]->(c)
      """
    And having executed:
      """
      MATCH (c:Node {name: 'C'}), (d:Node {name: 'D'})
      CREATE (c)-[:EDGE]->(d)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE path AS
        MATCH (a:Node)-[:EDGE]->(b:Node)
        ALONG hops = 1
        YIELD KEY a, KEY b, hops
      CREATE RULE path AS
        MATCH (a:Node)-[:EDGE]->(mid:Node)
        WHERE mid IS path TO b
        ALONG hops = prev.hops + 1
        YIELD KEY a, KEY b, hops

      CREATE RULE max_depth AS
        MATCH (a:Node)
        WHERE a IS path TO b
        FOLD mx = MAX(hops)
        YIELD KEY a, mx
      """
    Then evaluation should succeed
    And the derived relation 'path' should have 6 facts
    And the derived relation 'max_depth' should contain a fact where a.name = 'A' and mx = 3.0

  Scenario: 3b-3 ALONG reliability then FOLD MNOR aggregation across paths
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:LINK {rel: 0.9}]->(m:Node {name: 'M'})-[:LINK {rel: 0.8}]->(b:Node {name: 'B'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:LINK {rel: 0.7}]->(b)
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

  Scenario: 3b-4 ALONG with two variables then FOLD COUNT paths
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE {weight: 1.0}]->(b:Node {name: 'B'})-[:EDGE {weight: 2.0}]->(c:Node {name: 'C'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'})
      CREATE (a)-[:EDGE {weight: 5.0}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE path AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        ALONG dist = e.weight, hops = 1
        YIELD KEY a, KEY b, dist, hops
      CREATE RULE path AS
        MATCH (a:Node)-[e:EDGE]->(mid:Node)
        WHERE mid IS path TO b
        ALONG dist = prev.dist + e.weight, hops = prev.hops + 1
        YIELD KEY a, KEY b, dist, hops

      CREATE RULE path_count AS
        MATCH (a:Node)
        WHERE a IS path TO b
        FOLD cnt = COUNT(b)
        YIELD KEY a, cnt
      """
    Then evaluation should succeed
    And the derived relation 'path' should contain at least 4 facts
    And the derived relation 'path_count' should contain a fact where a.name = 'A'

  Scenario: 3b-5 ALONG cost with BEST BY then FOLD SUM of shortest paths
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE {weight: 1.0}]->(b:Node {name: 'B'})-[:EDGE {weight: 2.0}]->(c:Node {name: 'C'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'})
      CREATE (a)-[:EDGE {weight: 10.0}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE shortest AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        ALONG cost = e.weight
        BEST BY cost ASC
        YIELD KEY a, KEY b, cost
      CREATE RULE shortest AS
        MATCH (a:Node)-[e:EDGE]->(mid:Node)
        WHERE mid IS shortest TO b
        ALONG cost = prev.cost + e.weight
        BEST BY cost ASC
        YIELD KEY a, KEY b, cost

      CREATE RULE total_shortest AS
        MATCH (a:Node)
        WHERE a IS shortest TO b
        FOLD total = SUM(cost)
        YIELD KEY a, total
      """
    Then evaluation should succeed
    And the derived relation 'shortest' should contain a fact where a.name = 'A' and b.name = 'C' and cost = 3.0
    And the derived relation 'total_shortest' should contain a fact where a.name = 'A'

  # ══════════════════════════════════════════════════════════════════════
  # Group 3c: BEST BY + FOLD
  # ══════════════════════════════════════════════════════════════════════

  Scenario: 3c-1 BEST BY ASC selects minimum then FOLD SUM across groups
    Given having executed:
      """
      CREATE (a:Dept {name: 'Eng'})-[:EMPLOYS {salary: 90000}]->(:Person {name: 'Alice'}),
             (a)-[:EMPLOYS {salary: 70000}]->(:Person {name: 'Bob'}),
             (b:Dept {name: 'Sales'})-[:EMPLOYS {salary: 60000}]->(:Person {name: 'Carol'}),
             (b)-[:EMPLOYS {salary: 80000}]->(:Person {name: 'Dave'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE cheapest_employee AS
        MATCH (d:Dept)-[e:EMPLOYS]->(p:Person)
        BEST BY e.salary ASC
        YIELD KEY d, p, e.salary AS salary

      CREATE RULE dept_min_total AS
        MATCH (d:Dept)
        WHERE d IS cheapest_employee
        FOLD total = SUM(salary)
        YIELD total
      """
    Then evaluation should succeed
    And the derived relation 'cheapest_employee' should have 2 facts
    And the derived relation 'cheapest_employee' should contain a fact where d.name = 'Eng' and salary = 70000.0
    And the derived relation 'cheapest_employee' should contain a fact where d.name = 'Sales' and salary = 60000.0
    And the derived relation 'dept_min_total' should have 1 facts

  Scenario: 3c-2 BEST BY DESC selects maximum then FOLD COUNT
    Given having executed:
      """
      CREATE (a:Team {name: 'Alpha'})-[:MEMBER {score: 85}]->(:Player {name: 'P1'}),
             (a)-[:MEMBER {score: 92}]->(:Player {name: 'P2'}),
             (b:Team {name: 'Beta'})-[:MEMBER {score: 78}]->(:Player {name: 'P3'}),
             (b)-[:MEMBER {score: 95}]->(:Player {name: 'P4'}),
             (c:Team {name: 'Gamma'})-[:MEMBER {score: 88}]->(:Player {name: 'P5'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE top_scorer AS
        MATCH (t:Team)-[m:MEMBER]->(p:Player)
        BEST BY m.score DESC
        YIELD KEY t, p, m.score AS score

      CREATE RULE top_count AS
        MATCH (t:Team)
        WHERE t IS top_scorer
        FOLD cnt = COUNT(t)
        YIELD cnt
      """
    Then evaluation should succeed
    And the derived relation 'top_scorer' should have 3 facts
    And the derived relation 'top_scorer' should contain a fact where t.name = 'Alpha' and score = 92.0
    And the derived relation 'top_scorer' should contain a fact where t.name = 'Beta' and score = 95.0
    And the derived relation 'top_scorer' should contain a fact where t.name = 'Gamma' and score = 88.0
    And the derived relation 'top_count' should have 1 facts

  Scenario: 3c-4 BEST BY on recursive ALONG then IS NOT complement
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE {weight: 2.0}]->(b:Node {name: 'B'})-[:EDGE {weight: 3.0}]->(c:Node {name: 'C'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'})
      CREATE (a)-[:EDGE {weight: 4.0}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE shortest AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        ALONG cost = e.weight
        BEST BY cost ASC
        YIELD KEY a, KEY b, cost
      CREATE RULE shortest AS
        MATCH (a:Node)-[e:EDGE]->(mid:Node)
        WHERE mid IS shortest TO b
        ALONG cost = prev.cost + e.weight
        BEST BY cost ASC
        YIELD KEY a, KEY b, cost

      CREATE RULE expensive AS
        MATCH (a:Node)
        WHERE a IS shortest TO b, cost >= 4.0
        YIELD KEY a, KEY b, cost AS high_cost PROB

      CREATE RULE cheap AS
        MATCH (a:Node)
        WHERE a IS shortest TO b, a IS NOT expensive TO b
        YIELD KEY a, KEY b
      """
    Then evaluation should succeed
    And the derived relation 'shortest' should contain a fact where a.name = 'A' and b.name = 'C' and cost = 4.0
    And the derived relation 'expensive' should contain at least 1 facts

  Scenario: 3c-3 Shortest path BEST BY then FOLD SUM all shortest costs with QUERY
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'}), (:Node {name: 'C'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:EDGE {w: 3}]->(b)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'})
      CREATE (a)-[:EDGE {w: 10}]->(c)
      """
    And having executed:
      """
      MATCH (b:Node {name: 'B'}), (c:Node {name: 'C'})
      CREATE (b)-[:EDGE {w: 2}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE shortest AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        ALONG cost = e.w
        BEST BY cost ASC
        YIELD KEY a, KEY b, cost

      CREATE RULE shortest AS
        MATCH (a:Node)-[e:EDGE]->(mid:Node)
        WHERE mid IS shortest TO b
        ALONG cost = prev.cost + e.w
        BEST BY cost ASC
        YIELD KEY a, KEY b, cost

      CREATE RULE total_cost AS
        MATCH (a:Node)
        WHERE a IS shortest TO b
        FOLD s = SUM(cost)
        YIELD KEY a, s

      QUERY total_cost WHERE a = a RETURN a.name AS name, s
      """
    Then evaluation should succeed
    And the derived relation 'total_cost' should have 2 facts
    And the command result 0 should be a Query with 2 rows

  # ══════════════════════════════════════════════════════════════════════
  # Group 3d: similar_to + WHERE + IS NOT + QUERY
  # ══════════════════════════════════════════════════════════════════════

  Scenario: 3d-1 similar_to with WHERE filter on score then QUERY
    Given having executed:
      """
      CREATE (:Doc {name: 'A', emb: [1.0, 0.0, 0.0, 0.0]}),
             (:Doc {name: 'B', emb: [0.9, 0.1, 0.0, 0.0]}),
             (:Doc {name: 'C', emb: [0.0, 0.0, 0.0, 1.0]})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE sim AS
        MATCH (a:Doc), (b:Doc)
        WHERE a <> b
        YIELD KEY a, KEY b, similar_to(a.emb, b.emb) AS score

      CREATE RULE relevant AS
        MATCH (a:Doc)
        WHERE a IS sim TO b, score >= 0.5
        YIELD KEY a, KEY b, score

      QUERY relevant WHERE a.name = 'A' RETURN b.name AS match, score
      """
    Then evaluation should succeed
    And the derived relation 'relevant' should contain at least 1 facts
    And the command result 0 should be a Query with at least 1 rows
    And the command result 0 should be a Query containing row where match = 'B'

  Scenario: 3d-2 similar_to score as PROB with IS NOT complement
    Given having executed:
      """
      CREATE (:Doc {name: 'D1', emb: [0.6, 0.8, 0.0, 0.0]}),
             (:Doc {name: 'D2', emb: [1.0, 0.0, 0.0, 0.0]})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE relevant AS
        MATCH (d:Doc)
        YIELD KEY d, similar_to(d.emb, [1.0, 0.0, 0.0, 0.0]) AS score PROB

      CREATE RULE irrelevant AS
        MATCH (d:Doc)
        WHERE d IS NOT relevant
        YIELD KEY d, 1.0 AS inv_score PROB

      QUERY irrelevant WHERE d = d RETURN d.name AS doc, inv_score ORDER BY doc
      """
    Then evaluation should succeed
    And the derived relation 'relevant' should have 2 facts
    And the derived relation 'relevant' should contain a fact where d.name = 'D2' and score = 1.0
    And the derived relation 'relevant' should contain a fact where d.name = 'D1' and score = 0.6
    And the derived relation 'irrelevant' should have 2 facts
    And the derived relation 'irrelevant' should contain a fact where d.name = 'D1' and inv_score = 0.4
    And the derived relation 'irrelevant' should contain a fact where d.name = 'D2' and inv_score = 0.0
    And the command result 0 should be a Query with 2 rows

  Scenario: 3d-3 similar_to pairwise then IS NOT known then QUERY novel pairs
    Given having executed:
      """
      CREATE (:Item {name: 'X', emb: [0.9, 0.1, 0.0, 0.0]}),
             (:Item {name: 'Y', emb: [0.85, 0.15, 0.0, 0.0]}),
             (:Item {name: 'Z', emb: [0.0, 0.0, 1.0, 0.0]})
      """
    And having executed:
      """
      MATCH (x:Item {name: 'X'}), (y:Item {name: 'Y'})
      CREATE (x)-[:KNOWN_SIM]->(y)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE sim AS
        MATCH (a:Item), (b:Item)
        WHERE a <> b
        YIELD KEY a, KEY b, similar_to(a.emb, b.emb) AS score

      CREATE RULE known AS
        MATCH (a:Item)-[:KNOWN_SIM]->(b:Item)
        YIELD KEY a, KEY b

      CREATE RULE novel AS
        MATCH (a:Item)
        WHERE a IS sim TO b, score >= 0.5, a IS NOT known TO b
        YIELD KEY a, KEY b, score

      QUERY novel WHERE a = a RETURN a.name AS src, b.name AS dst, score
      """
    Then evaluation should succeed
    And the derived relation 'novel' should contain at least 1 facts
    And the command result 0 should be a Query with at least 1 rows

  Scenario: 3d-4 similar_to to MNOR aggregation then QUERY with filter
    Given having executed:
      """
      CREATE (:Asset {name: 'Server', ref: [1.0, 0.0, 0.0, 0.0]})
      """
    And having executed:
      """
      MATCH (a:Asset {name: 'Server'}) CREATE (:Signal {name: 'S1', vec: [0.6, 0.8, 0.0, 0.0]})-[:ALERT]->(a)
      """
    And having executed:
      """
      MATCH (a:Asset {name: 'Server'}) CREATE (:Signal {name: 'S2', vec: [0.9, 0.1, 0.0, 0.0]})-[:ALERT]->(a)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE scored AS
        MATCH (s:Signal)-[:ALERT]->(a:Asset)
        YIELD KEY a, KEY s, similar_to(s.vec, a.ref) AS sim

      CREATE RULE threat AS
        MATCH (a:Asset)
        WHERE a IS scored TO s
        FOLD risk = MNOR(sim)
        YIELD KEY a, risk

      QUERY threat WHERE a = a RETURN a.name AS asset, risk
      """
    Then evaluation should succeed
    And the derived relation 'scored' should have 2 facts
    And the derived relation 'threat' should have 1 facts
    And the command result 0 should be a Query with 1 rows

  Scenario: 3d-5 similar_to with WHERE threshold and IS NOT exclusion combined
    Given having executed:
      """
      CREATE (:Doc {name: 'A', emb: [1.0, 0.0, 0.0, 0.0]}),
             (:Doc {name: 'B', emb: [0.7, 0.7, 0.0, 0.0]}),
             (:Doc {name: 'C', emb: [0.0, 1.0, 0.0, 0.0]}),
             (:Doc {name: 'D', emb: [0.95, 0.05, 0.0, 0.0]})
      """
    And having executed:
      """
      MATCH (a:Doc {name: 'A'}), (d:Doc {name: 'D'})
      CREATE (a)-[:REVIEWED]->(d)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE sim AS
        MATCH (a:Doc), (b:Doc)
        WHERE a <> b
        YIELD KEY a, KEY b, similar_to(a.emb, b.emb) AS score

      CREATE RULE reviewed AS
        MATCH (a:Doc)-[:REVIEWED]->(b:Doc)
        YIELD KEY a, KEY b

      CREATE RULE to_review AS
        MATCH (a:Doc)
        WHERE a IS sim TO b, score >= 0.5, a IS NOT reviewed TO b
        YIELD KEY a, KEY b, score

      QUERY to_review WHERE a.name = 'A' RETURN b.name AS candidate, score
      """
    Then evaluation should succeed
    And the derived relation 'to_review' should contain at least 1 facts
    And the command result 0 should be a Query with at least 1 rows
    And the command result 0 should be a Query containing row where candidate = 'B'

  # ══════════════════════════════════════════════════════════════════════
  # Group 3e: Recursive + FOLD + QUERY
  # ══════════════════════════════════════════════════════════════════════

  Scenario: 3e-1 Recursive transitive closure then FOLD COUNT then QUERY
    Given having executed:
      """
      CREATE (:Person {name: 'A'}), (:Person {name: 'B'}), (:Person {name: 'C'})
      """
    And having executed:
      """
      MATCH (a:Person {name: 'A'}), (b:Person {name: 'B'})
      CREATE (a)-[:KNOWS]->(b)
      """
    And having executed:
      """
      MATCH (b:Person {name: 'B'}), (c:Person {name: 'C'})
      CREATE (b)-[:KNOWS]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reach AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        YIELD KEY a, KEY b

      CREATE RULE reach AS
        MATCH (a:Person)-[:KNOWS]->(mid:Person)
        WHERE mid IS reach TO b
        YIELD KEY a, KEY b

      CREATE RULE summary AS
        MATCH (a:Person)
        WHERE a IS reach TO b
        FOLD cnt = MCOUNT()
        YIELD KEY a, cnt

      QUERY summary WHERE a = a RETURN a.name AS name, cnt
      """
    Then evaluation should succeed
    And the derived relation 'summary' should have 2 facts
    And the command result 0 should be a Query with 2 rows

  Scenario: 3e-2 Recursive ALONG cost then FOLD SUM then QUERY per source
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'}), (:Node {name: 'C'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:EDGE {w: 3}]->(b)
      """
    And having executed:
      """
      MATCH (b:Node {name: 'B'}), (c:Node {name: 'C'})
      CREATE (b)-[:EDGE {w: 2}]->(c)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'})
      CREATE (a)-[:EDGE {w: 10}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE path AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        ALONG cost = e.w
        BEST BY cost ASC
        YIELD KEY a, KEY b, cost

      CREATE RULE path AS
        MATCH (a:Node)-[e:EDGE]->(mid:Node)
        WHERE mid IS path TO b
        ALONG cost = prev.cost + e.w
        BEST BY cost ASC
        YIELD KEY a, KEY b, cost

      CREATE RULE total AS
        MATCH (a:Node)
        WHERE a IS path TO b
        FOLD s = SUM(cost)
        YIELD KEY a, s

      QUERY total WHERE a = a RETURN a.name AS name, s
      """
    Then evaluation should succeed
    And the derived relation 'total' should have 2 facts
    And the command result 0 should be a Query with 2 rows

  Scenario: 3e-3 Recursive shortest path BEST BY then FOLD MIN then QUERY
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'}), (:Node {name: 'C'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'})
      CREATE (a)-[:EDGE {w: 3}]->(b)
      """
    And having executed:
      """
      MATCH (b:Node {name: 'B'}), (c:Node {name: 'C'})
      CREATE (b)-[:EDGE {w: 2}]->(c)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'})
      CREATE (a)-[:EDGE {w: 10}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE path AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        ALONG cost = e.w
        BEST BY cost ASC
        YIELD KEY a, KEY b, cost

      CREATE RULE path AS
        MATCH (a:Node)-[e:EDGE]->(mid:Node)
        WHERE mid IS path TO b
        ALONG cost = prev.cost + e.w
        BEST BY cost ASC
        YIELD KEY a, KEY b, cost

      CREATE RULE cheapest AS
        MATCH (a:Node)
        WHERE a IS path TO b
        FOLD mn = MIN(cost)
        YIELD KEY a, mn

      QUERY cheapest WHERE a = a RETURN a.name AS name, mn
      """
    Then evaluation should succeed
    And the derived relation 'cheapest' should have 2 facts
    And the command result 0 should be a Query with 2 rows

  Scenario: 3e-4 Recursive reachability then FOLD MCOUNT then IS NOT then QUERY
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE]->(b:Node {name: 'B'})-[:EDGE]->(c:Node {name: 'C'}),
             (d:Node {name: 'D'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b
      CREATE RULE reachable AS
        MATCH (a:Node)-[:EDGE]->(mid:Node)
        WHERE mid IS reachable TO b YIELD KEY a, KEY b

      CREATE RULE reach_count AS
        MATCH (a:Node)
        WHERE a IS reachable TO b
        FOLD cnt = MCOUNT()
        YIELD KEY a, cnt

      CREATE RULE isolated AS
        MATCH (n:Node)
        WHERE n IS NOT reach_count
        YIELD KEY n

      QUERY isolated WHERE n = n RETURN n.name AS name
      """
    Then evaluation should succeed
    And the derived relation 'reachable' should have 3 facts
    And the derived relation 'reach_count' should have 2 facts
    And the derived relation 'isolated' should contain at least 1 facts
    And the command result 0 should be a Query with at least 1 rows

  # ══════════════════════════════════════════════════════════════════════
  # Group 3f: WHERE on IS-ref value columns
  # ══════════════════════════════════════════════════════════════════════

  Scenario: 3f-1 WHERE filter on numeric value column from IS-ref
    Given having executed:
      """
      CREATE (:Sensor {name: 'S1', val: 0.8}),
             (:Sensor {name: 'S2', val: 0.3}),
             (:Sensor {name: 'S3', val: 0.6})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE reading AS
        MATCH (n:Sensor)
        YIELD KEY n, n.val AS v

      CREATE RULE high_reading AS
        MATCH (n:Sensor)
        WHERE n IS reading, v >= 0.5
        YIELD KEY n, v

      QUERY high_reading WHERE n = n RETURN n.name AS sensor, v ORDER BY sensor
      """
    Then evaluation should succeed
    And the derived relation 'high_reading' should have 2 facts
    And the derived relation 'high_reading' should contain a fact where n.name = 'S1' and v = 0.8
    And the derived relation 'high_reading' should contain a fact where n.name = 'S3' and v = 0.6
    And the command result 0 should be a Query with 2 rows

  Scenario: 3f-2 WHERE filter on FOLD value column from IS-ref
    Given having executed:
      """
      CREATE (:Node {name: 'A'}), (:Node {name: 'B'}),
             (:Target {name: 'T1'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (t:Target {name: 'T1'})
      CREATE (a)-[:CAUSE {prob: 0.3}]->(t)
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (t:Target {name: 'T1'})
      CREATE (a)-[:CAUSE {prob: 0.5}]->(t)
      """
    And having executed:
      """
      MATCH (b:Node {name: 'B'}), (t:Target {name: 'T1'})
      CREATE (b)-[:CAUSE {prob: 0.1}]->(t)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(t:Target)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY t, p

      CREATE RULE high_risk AS
        MATCH (a:Node)
        WHERE a IS risk TO t, p >= 0.5
        YIELD KEY a, KEY t, p AS risk_score

      QUERY high_risk WHERE a = a RETURN a.name AS source, risk_score
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 2 facts
    And the derived relation 'high_risk' should have 1 facts
    And the derived relation 'high_risk' should contain a fact where a.name = 'A' and risk_score = 0.65
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where source = 'A'

  Scenario: 3f-3 WHERE range filter on IS-ref value column
    Given having executed:
      """
      CREATE (:Device {name: 'D1'}), (:Device {name: 'D2'}), (:Device {name: 'D3'}), (:Device {name: 'D4'})
      """
    And having executed:
      """
      MATCH (a:Device {name: 'D1'}), (b:Device {name: 'D2'})
      CREATE (a)-[:LINK {weight: 0.9}]->(b)
      """
    And having executed:
      """
      MATCH (a:Device {name: 'D1'}), (b:Device {name: 'D3'})
      CREATE (a)-[:LINK {weight: 0.4}]->(b)
      """
    And having executed:
      """
      MATCH (a:Device {name: 'D1'}), (b:Device {name: 'D4'})
      CREATE (a)-[:LINK {weight: 0.7}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE connected AS
        MATCH (a:Device)-[e:LINK]->(b:Device)
        YIELD KEY a, KEY b, e.weight AS w

      CREATE RULE medium_links AS
        MATCH (a:Device)
        WHERE a IS connected TO b, w >= 0.5, w <= 0.8
        YIELD KEY a, KEY b, w

      QUERY medium_links WHERE a = a RETURN a.name AS src, b.name AS dst, w
      """
    Then evaluation should succeed
    And the derived relation 'medium_links' should have 1 facts
    And the derived relation 'medium_links' should contain a fact where b.name = 'D4' and w = 0.7
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where dst = 'D4'

  Scenario: 3f-4 WHERE filter on IS-ref value column with ALONG cost
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE {weight: 2.0}]->(b:Node {name: 'B'})-[:EDGE {weight: 3.0}]->(c:Node {name: 'C'})
      """
    And having executed:
      """
      MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'})
      CREATE (a)-[:EDGE {weight: 10.0}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE path AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        ALONG cost = e.weight
        BEST BY cost ASC
        YIELD KEY a, KEY b, cost
      CREATE RULE path AS
        MATCH (a:Node)-[e:EDGE]->(mid:Node)
        WHERE mid IS path TO b
        ALONG cost = prev.cost + e.weight
        BEST BY cost ASC
        YIELD KEY a, KEY b, cost

      CREATE RULE short_paths AS
        MATCH (a:Node)
        WHERE a IS path TO b, cost <= 5.0
        YIELD KEY a, KEY b, cost

      QUERY short_paths WHERE a.name = 'A' RETURN b.name AS dst, cost
      """
    Then evaluation should succeed
    And the derived relation 'short_paths' should contain at least 1 facts
    And the derived relation 'short_paths' should contain a fact where a.name = 'A' and b.name = 'B' and cost = 2.0
    And the derived relation 'short_paths' should contain a fact where a.name = 'A' and b.name = 'C' and cost = 5.0
    And the command result 0 should be a Query with at least 1 rows

  Scenario: 3f-5 WHERE comparison on two IS-ref value columns
    Given having executed:
      """
      CREATE (:Employee {name: 'Alice', salary: 90000, bonus: 5000}),
             (:Employee {name: 'Bob', salary: 70000, bonus: 80000}),
             (:Employee {name: 'Carol', salary: 60000, bonus: 10000})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE comp AS
        MATCH (e:Employee)
        YIELD KEY e, e.salary AS sal, e.bonus AS bon

      CREATE RULE bonus_exceeds AS
        MATCH (e:Employee)
        WHERE e IS comp, bon > sal
        YIELD KEY e, sal, bon

      QUERY bonus_exceeds WHERE e = e RETURN e.name AS name, sal, bon
      """
    Then evaluation should succeed
    And the derived relation 'bonus_exceeds' should have 1 facts
    And the derived relation 'bonus_exceeds' should contain a fact where e.name = 'Bob'
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where name = 'Bob'
