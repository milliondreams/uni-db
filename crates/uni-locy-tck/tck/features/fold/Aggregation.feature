Feature: FOLD Aggregation

  Tests parsing of FOLD clauses for post-fixpoint aggregation.

  Background:
    Given an empty graph

  Scenario: FOLD with SUM
    When parsing the following Locy program:
      """
      CREATE RULE totals AS MATCH (a)-[:E]->(b) FOLD total = SUM(b.value) YIELD KEY a, total
      """
    Then the program should parse successfully

  Scenario: FOLD with BEST BY
    When parsing the following Locy program:
      """
      CREATE RULE shortest AS MATCH (a)-[e:E]->(b) ALONG dist = prev.dist + e.weight BEST BY dist ASC YIELD KEY a, KEY b, dist
      """
    Then the program should parse successfully

  Scenario: FOLD with MSUM monotonic aggregate
    When parsing the following Locy program:
      """
      CREATE RULE running AS MATCH (a)-[:E]->(b) FOLD acc = MSUM(b.value) YIELD KEY a, acc
      """
    Then the program should parse successfully

  # ── Evaluate-level scenarios ──────────────────────────────────────────

  Scenario: FOLD SUM groups by key correctly
    Given having executed:
      """
      CREATE (a:Person {name: 'Alice'})-[:PAID {amount: 100}]->(:Invoice),
             (a)-[:PAID {amount: 200}]->(:Invoice),
             (b:Person {name: 'Bob'})-[:PAID {amount: 50}]->(:Invoice)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE spending AS
        MATCH (p:Person)-[r:PAID]->(i:Invoice)
        FOLD total = SUM(r.amount)
        YIELD KEY p, total
      """
    Then evaluation should succeed
    And the derived relation 'spending' should have 2 facts

  Scenario: FOLD COUNT counts matching rows
    Given having executed:
      """
      CREATE (:Person {name: 'Alice'}), (:Person {name: 'Bob'}), (:Person {name: 'Carol'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE census AS
        MATCH (n:Person)
        FOLD cnt = COUNT(n)
        YIELD cnt
      """
    Then evaluation should succeed
    And the derived relation 'census' should have 1 facts

  Scenario: BEST BY MIN selects cheapest per group
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE {cost: 5}]->(b:Node {name: 'B'}),
             (a)-[:EDGE {cost: 3}]->(c:Node {name: 'C'}),
             (a)-[:EDGE {cost: 7}]->(d:Node {name: 'D'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE cheapest_neighbor AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        BEST BY e.cost ASC
        YIELD KEY a, b, e.cost AS cost
      """
    Then evaluation should succeed
    And the derived relation 'cheapest_neighbor' should have 1 facts

  Scenario: BEST BY preserves full row including ALONG values
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE {weight: 5.0}]->(b:Node {name: 'B'})-[:EDGE {weight: 3.0}]->(c:Node {name: 'C'}),
             (a)-[:EDGE {weight: 20.0}]->(c)
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
      """
    Then evaluation should succeed
    And the derived relation 'shortest' should contain a fact where a.name = 'A' and b.name = 'C'

  Scenario: MCOUNT zero-argument counts all facts in group
    Given having executed:
      """
      CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
             (a)-[:KNOWS]->(:Person {name: 'Carol'}),
             (b)-[:KNOWS]->(:Person {name: 'Dave'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE friend_count AS
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        FOLD cnt = MCOUNT()
        YIELD KEY a, cnt
      """
    Then evaluation should succeed
    And the derived relation 'friend_count' should have 2 facts

  Scenario: COUNT zero-argument counts all facts
    Given having executed:
      """
      CREATE (:Person {name: 'Alice'}), (:Person {name: 'Bob'}), (:Person {name: 'Carol'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE headcount AS
        MATCH (n:Person)
        FOLD cnt = COUNT()
        YIELD cnt
      """
    Then evaluation should succeed
    And the derived relation 'headcount' should have 1 facts

  Scenario: FOLD SUM with computed expression argument
    Given having executed:
      """
      CREATE (a:Person {name: 'Alice'})-[:PAID {amount: 100}]->(:Invoice),
             (a)-[:PAID {amount: 200}]->(:Invoice),
             (b:Person {name: 'Bob'})-[:PAID {amount: 50}]->(:Invoice)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE double_spending AS
        MATCH (p:Person)-[r:PAID]->(i:Invoice)
        FOLD total = SUM(r.amount * 2)
        YIELD KEY p, total
      """
    Then evaluation should succeed
    And the derived relation 'double_spending' should have 2 facts

  Scenario: FOLD SUM with arithmetic expression on edge properties
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:EDGE {weight: 5.0, cost: 2}]->(b:Node {name: 'B'}),
             (a)-[:EDGE {weight: 3.0, cost: 4}]->(c:Node {name: 'C'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE weighted_costs AS
        MATCH (a:Node)-[e:EDGE]->(b:Node)
        FOLD total = SUM(e.weight + 1.0)
        YIELD KEY a, total
      """
    Then evaluation should succeed
    And the derived relation 'weighted_costs' should have 1 facts

  # ── The two views, side by side (issue #162) ────────────────────────────
  #
  # "A FOLD aggregates the bag of derivations" still holds for a NON-recursive
  # fold: the rows a clause emits are the bag, and equal values contribute
  # once each. What changed is only what a SELF-reference inside a recursive
  # stratum observes — there it reads the target's folded value per KEY. A
  # consumer in a LATER stratum has always read published, folded facts, and
  # still does.

  Scenario: A later-stratum consumer folds the published folded value
    Given having executed:
      """
      CREATE (t:Asm {name: 'TOP'}),
             (t)-[:PART]->(:Asm {name: 'L1'}),
             (t)-[:PART]->(:Asm {name: 'L2'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE leafy AS
        MATCH (p:Asm)-[:PART]->(c:Asm)
        FOLD b = MPROD(0.5)
        YIELD KEY p, b
      CREATE RULE report AS
        MATCH (p:Asm)
        WHERE p IS leafy
        FOLD z = MPROD(b)
        YIELD KEY p, z
      """
    Then evaluation should succeed
    # `leafy` folds two contribution rows (one per PART edge), both 0.5, giving
    # 0.25 — the bag-of-derivations rule, unchanged. `report` is a separate
    # stratum, so it consumes the single published fact 0.25, not the two rows
    # behind it, giving 0.25 rather than 0.0625.
    And the derived relation 'leafy' should contain a fact where b = 0.25
    And the derived relation 'report' should contain a fact where z = 0.25
