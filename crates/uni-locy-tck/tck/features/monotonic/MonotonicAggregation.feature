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

  # ── Recursive FOLD must aggregate across DERIVATIONS, not distinct values ──
  #
  # A derived relation is a set of facts identified by KEY, but a FOLD consumes
  # the BAG of derivations that produced them — "FOLD aggregates across paths".
  # Before issue #159 these three scenarios were impossible to write: all-column
  # dedup collapsed equal-valued sibling derivations before the fold ran, so a
  # parent with N children of equal cost rolled up to one child's worth.
  #
  # There was previously NO evaluate-level scenario for a recursive MSUM or
  # MCOUNT at all — only parse/compile ones — which is why the regression went
  # unnoticed.
  #
  # The base clause yields a literal `1.0` rather than a node property on
  # purpose: in schemaless mode (the TCK default) a property reads back as
  # cv-encoded LargeBinary while the FOLD output is Float64, so the two clauses
  # of `roll` would disagree on column type. That is a separate, pre-existing
  # limitation of recursive FOLD over an undeclared property — unrelated to the
  # multiplicity these scenarios pin.

  Scenario: Recursive MSUM counts each equal-valued child derivation
    Given having executed:
      """
      CREATE (t:Part {name: 'TOP'}),
             (t)-[:HAS {q: 1.0}]->(:Part {name: 'L1'}),
             (t)-[:HAS {q: 1.0}]->(:Part {name: 'L2'}),
             (t)-[:HAS {q: 1.0}]->(:Part {name: 'L3'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE assembly AS
        MATCH (p:Part)-[:HAS]->(:Part)
        YIELD KEY p
      CREATE RULE roll AS
        MATCH (p:Part)
        WHERE p IS NOT assembly
        YIELD KEY p, 1.0 AS cost
      CREATE RULE roll AS
        MATCH (p:Part)-[e:HAS]->(c:Part)
        WHERE c IS roll
        FOLD cost = MSUM(cost * e.q)
        YIELD KEY p, cost
      """
    Then evaluation should succeed
    # Three leaves each contributing exactly 1.0 must sum to 3.0, not 1.0.
    And the derived relation 'roll' should contain a fact where cost = 3.0

  Scenario: Recursive MSUM over a diamond counts both paths to a shared child
    Given having executed:
      """
      CREATE (t:Part {name: 'T'}),
             (m1:Part {name: 'M1'}),
             (m2:Part {name: 'M2'}),
             (x:Part {name: 'X'}),
             (t)-[:HAS {q: 1.0}]->(m1),
             (t)-[:HAS {q: 1.0}]->(m2),
             (m1)-[:HAS {q: 1.0}]->(x),
             (m2)-[:HAS {q: 1.0}]->(x)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE assembly AS
        MATCH (p:Part)-[:HAS]->(:Part)
        YIELD KEY p
      CREATE RULE roll AS
        MATCH (p:Part)
        WHERE p IS NOT assembly
        YIELD KEY p, 1.0 AS cost
      CREATE RULE roll AS
        MATCH (p:Part)-[e:HAS]->(c:Part)
        WHERE c IS roll
        FOLD cost = MSUM(cost * e.q)
        YIELD KEY p, cost
      """
    Then evaluation should succeed
    # X is the only leaf, so M1 and M2 both roll up to 1.0 — two intermediate
    # facts carrying the IDENTICAL value, which is the case that used to
    # collapse. T sums both, giving 2.0.
    And the derived relation 'roll' should contain a fact where cost = 2.0

  Scenario: Recursive MCOUNT counts derivations, not distinct values
    Given having executed:
      """
      CREATE (t:Item {name: 'TOP'}),
             (t)-[:LINK]->(:Item {name: 'L1'}),
             (t)-[:LINK]->(:Item {name: 'L2'}),
             (t)-[:LINK]->(:Item {name: 'L3'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE parent AS
        MATCH (p:Item)-[:LINK]->(:Item)
        YIELD KEY p
      CREATE RULE tally AS
        MATCH (p:Item)
        WHERE p IS NOT parent
        YIELD KEY p, 1.0 AS n
      CREATE RULE tally AS
        MATCH (p:Item)-[:LINK]->(c:Item)
        WHERE c IS tally
        FOLD n = MCOUNT(n)
        YIELD KEY p, n
      """
    Then evaluation should succeed
    # MCOUNT is `acc + 1` per contributing row. The three leaves all carry the
    # identical value 1.0, so under the old collapse TOP counted 1.
    And the derived relation 'tally' should contain a fact where n = 3

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

  # ── Spec-level assertions ─────────────────────────────────────────────

  Scenario: MNOR spec example (0.7, 0.5) yields 0.85
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:CAUSE {prob: 0.7}]->(b:Node {name: 'B'}),
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
    And the derived relation 'risk' should contain a fact where p = 0.85

  Scenario: MNOR zero does not affect result
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:CAUSE {prob: 0.0}]->(b:Node {name: 'B'}),
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
    And the derived relation 'risk' should contain a fact where p = 0.5

  Scenario: MNOR all-ones yields one
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:CAUSE {prob: 1.0}]->(b:Node {name: 'B'}),
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

  Scenario: MPROD three-value (0.9, 0.8, 0.7) yields 0.504
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:REQ {prob: 0.9}]->(b:Node {name: 'B'}),
             (a)-[:REQ {prob: 0.8}]->(b),
             (a)-[:REQ {prob: 0.7}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE joint AS
        MATCH (a:Node)-[e:REQ]->(b:Node)
        FOLD p = MPROD(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'joint' should contain a fact where p = 0.504

  Scenario: strict rejects MPROD input greater than one
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:REQ {prob: 1.5}]->(b:Node {name: 'B'})
      """
    When evaluating the following Locy program with strict_probability_domain:
      """
      CREATE RULE joint AS
        MATCH (a:Node)-[e:REQ]->(b:Node)
        FOLD p = MPROD(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should fail
    And the evaluation error should mention 'strict_probability_domain'

  Scenario: strict rejects negative MNOR input
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:CAUSE {prob: -0.5}]->(b:Node {name: 'B'})
      """
    When evaluating the following Locy program with strict_probability_domain:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should fail
    And the evaluation error should mention 'strict_probability_domain'
