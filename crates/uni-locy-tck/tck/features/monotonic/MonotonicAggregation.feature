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
