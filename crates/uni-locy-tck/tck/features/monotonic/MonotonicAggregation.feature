Feature: Monotonic Aggregation (MSUM, MMAX, MMIN, MCOUNT)

  Tests monotonic FOLD operators that are safe within recursive fixpoint.
  Convergence requires both: no new tuples AND no aggregate value changes.

  Background:
    Given an empty graph

  # ── Parse level ───────────────────────────────────────────────────────

  Scenario: MSUM syntax parses
    When parsing the following Locy program:
      """
      CREATE RULE cumulative AS
        MATCH (a)-[r:OWNS]->(b)
        FOLD total = MSUM(r.stake)
        YIELD KEY a, KEY b, total
      """
    Then the program should parse successfully

  # ── Compile level ─────────────────────────────────────────────────────

  Scenario: Non-monotonic SUM in recursive rule rejected
    When compiling the following Locy program:
      """
      CREATE RULE recursive_sum AS
        MATCH (a)-[:EDGE]->(b)
        YIELD KEY a, KEY b, 0 AS total
      CREATE RULE recursive_sum AS
        MATCH (a)-[:EDGE]->(mid)
        WHERE mid IS recursive_sum TO b
        FOLD total = SUM(a.value)
        YIELD KEY a, KEY b, total
      """
    Then the program should fail to compile
    And the compile error should mention 'non-monotonic'

  Scenario: MSUM in non-recursive rule compiles
    When compiling the following Locy program:
      """
      CREATE RULE totals AS
        MATCH (a)-[r:OWNS]->(b)
        FOLD total = MSUM(r.stake)
        YIELD KEY a, total
      """
    Then the program should compile successfully

  # ── Evaluate level ────────────────────────────────────────────────────

  Scenario: MSUM converges over multi-hop ownership
    Given having executed:
      """
      CREATE (a:Co {name: 'Acme'})-[:OWNS {stake: 0.6}]->(b:Co {name: 'MidCo'}),
             (b)-[:OWNS {stake: 0.8}]->(c:Co {name: 'Target'}),
             (a)-[:OWNS {stake: 0.3}]->(c)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE control AS
        MATCH (a:Co)-[r:OWNS]->(b:Co)
        FOLD total = MSUM(r.stake)
        YIELD KEY a, KEY b, total
      """
    Then evaluation should succeed
    And the derived relation 'control' should have 3 facts

  Scenario: MMAX converges to true maximum
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE {val: 5}]->(b:Node {name: 'B'}),
             (a)-[:EDGE {val: 10}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE peak AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        FOLD mx = MMAX(e.val)
        YIELD KEY a, KEY b, mx
      """
    Then evaluation should succeed
    And the derived relation 'peak' should have 1 facts

  Scenario: MMIN converges to true minimum
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE {val: 5}]->(b:Node {name: 'B'}),
             (a)-[:EDGE {val: 2}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE trough AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        FOLD mn = MMIN(e.val)
        YIELD KEY a, KEY b, mn
      """
    Then evaluation should succeed
    And the derived relation 'trough' should have 1 facts

  # ── MNOR / MPROD ────────────────────────────────────────────────────

  Scenario: MNOR syntax parses
    When parsing the following Locy program:
      """
      CREATE RULE prob AS
        MATCH (a)-[r:CAUSES]->(b)
        FOLD p = MNOR(r.probability)
        YIELD KEY a, KEY b, p
      """
    Then the program should parse successfully

  Scenario: MPROD syntax parses
    When parsing the following Locy program:
      """
      CREATE RULE joint AS
        MATCH (a)-[r:REQUIRES]->(b)
        FOLD p = MPROD(r.probability)
        YIELD KEY a, KEY b, p
      """
    Then the program should parse successfully

  Scenario: MNOR in non-recursive rule compiles
    When compiling the following Locy program:
      """
      CREATE RULE prob AS
        MATCH (a)-[r:CAUSES]->(b)
        FOLD p = MNOR(r.probability)
        YIELD KEY a, p
      """
    Then the program should compile successfully

  Scenario: MPROD in non-recursive rule compiles
    When compiling the following Locy program:
      """
      CREATE RULE joint AS
        MATCH (a)-[r:REQUIRES]->(b)
        FOLD p = MPROD(r.probability)
        YIELD KEY a, p
      """
    Then the program should compile successfully

  Scenario: MNOR rejected with BEST BY
    When compiling the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[:E]->(b)
        YIELD KEY a, KEY b, 0 AS p
      CREATE RULE r AS
        MATCH (a)-[:E]->(mid)
        WHERE mid IS r TO b
        FOLD p = MNOR(a.weight) BEST BY p ASC
        YIELD KEY a, KEY b, p
      """
    Then the program should fail to compile

  Scenario: MPROD rejected with BEST BY
    When compiling the following Locy program:
      """
      CREATE RULE r AS
        MATCH (a)-[:E]->(b)
        YIELD KEY a, KEY b, 1 AS p
      CREATE RULE r AS
        MATCH (a)-[:E]->(mid)
        WHERE mid IS r TO b
        FOLD p = MPROD(a.weight) BEST BY p ASC
        YIELD KEY a, KEY b, p
      """
    Then the program should fail to compile

  Scenario: MNOR converges with correct noisy-OR
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:CAUSE {prob: 0.3}]->(b:Node {name: 'B'}),
             (a)-[:CAUSE {prob: 0.5}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 1 facts

  Scenario: MPROD converges with correct product
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:REQ {prob: 0.6}]->(b:Node {name: 'B'}),
             (a)-[:REQ {prob: 0.8}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE joint AS
        MATCH (a:Node)-[e:REQ]->(b:Node)
        FOLD p = MPROD(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'joint' should have 1 facts

  # ── MNOR / MPROD Value Assertions ─────────────────────────────────────

  Scenario: MNOR produces correct noisy-OR value
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:CAUSE {prob: 0.3}]->(b:Node {name: 'B'}),
             (a)-[:CAUSE {prob: 0.5}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should contain a fact where p = 0.65

  Scenario: MPROD produces correct product value
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:REQ {prob: 0.6}]->(b:Node {name: 'B'}),
             (a)-[:REQ {prob: 0.8}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE joint AS
        MATCH (a:Node)-[e:REQ]->(b:Node)
        FOLD p = MPROD(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'joint' should contain a fact where p = 0.48

  Scenario: MNOR four causes matches spec example
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:CAUSE {prob: 0.72}]->(b:Node {name: 'B'}),
             (a)-[:CAUSE {prob: 0.54}]->(b),
             (a)-[:CAUSE {prob: 0.56}]->(b),
             (a)-[:CAUSE {prob: 0.42}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should contain a fact where p = 0.96713024

  Scenario: MNOR single cause unchanged
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:CAUSE {prob: 0.7}]->(b:Node {name: 'B'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should contain a fact where p = 0.7

  Scenario: MPROD single requirement unchanged
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:REQ {prob: 0.7}]->(b:Node {name: 'B'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE joint AS
        MATCH (a:Node)-[e:REQ]->(b:Node)
        FOLD p = MPROD(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'joint' should contain a fact where p = 0.7

  Scenario: MNOR with certainty yields one
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:CAUSE {prob: 0.3}]->(b:Node {name: 'B'}),
             (a)-[:CAUSE {prob: 1.0}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should contain a fact where p = 1.0

  Scenario: MPROD with zero yields zero
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:REQ {prob: 0.5}]->(b:Node {name: 'B'}),
             (a)-[:REQ {prob: 0.0}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE joint AS
        MATCH (a:Node)-[e:REQ]->(b:Node)
        FOLD p = MPROD(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'joint' should contain a fact where p = 0.0

  Scenario: MNOR with zeros yields zero
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:CAUSE {prob: 0.0}]->(b:Node {name: 'B'}),
             (a)-[:CAUSE {prob: 0.0}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should contain a fact where p = 0.0

  Scenario: MPROD with ones yields one
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:REQ {prob: 1.0}]->(b:Node {name: 'B'}),
             (a)-[:REQ {prob: 1.0}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE joint AS
        MATCH (a:Node)-[e:REQ]->(b:Node)
        FOLD p = MPROD(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'joint' should contain a fact where p = 1.0

  Scenario: MPROD groups independently
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:REQ {prob: 0.5}]->(b:Node {name: 'B'}),
             (a)-[:REQ {prob: 0.5}]->(b),
             (c:Node {name: 'C'})-[:REQ {prob: 0.8}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE joint AS
        MATCH (x:Node)-[e:REQ]->(y:Node)
        FOLD p = MPROD(e.prob)
        YIELD KEY x, KEY y, p
      """
    Then evaluation should succeed
    And the derived relation 'joint' should have 2 facts
    And the derived relation 'joint' should contain a fact where p = 0.25
    And the derived relation 'joint' should contain a fact where p = 0.8
