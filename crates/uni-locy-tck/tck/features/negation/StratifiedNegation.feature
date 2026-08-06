Feature: Stratified Negation (IS NOT)

  Tests IS NOT for excluding nodes/tuples present in another derived relation.
  Negation requires stratification: the negated rule must be in a lower stratum.

  Background:
    Given an empty graph

  # ── Parse level ───────────────────────────────────────────────────────

  Scenario: IS NOT basic syntax parses
    When parsing the following Locy program:
      """
      CREATE RULE clean AS MATCH (n) WHERE n IS NOT flagged YIELD KEY n
      """
    Then the program should parse successfully

  # ── Compile level ─────────────────────────────────────────────────────

  Scenario: IS NOT creates a higher stratum
    When compiling the following Locy program:
      """
      CREATE RULE flagged AS MATCH (n:Node) WHERE n.risk > 0.5 YIELD KEY n
      CREATE RULE clean AS MATCH (n:Node) WHERE n IS NOT flagged YIELD KEY n
      """
    Then the program should compile successfully
    And the program should have 2 strata

  # ── Compile level: the negation subject must be a node ────────────────
  #
  # Negation joins on node identity, so an IS NOT subject has to have a vid.
  # These used to compile and fail only once evaluation reached the anti-join.

  Scenario: IS NOT subject bound only by a YIELD alias is rejected
    When compiling the following Locy program:
      """
      CREATE RULE flagged AS MATCH (a:A)-[:E]->(b:B) YIELD KEY a, KEY b
      CREATE RULE probe AS
        MATCH (a:A), (b:B)
        WHERE x IS NOT flagged
        YIELD KEY b, a.n AS x
      """
    Then the program should fail to compile
    And the compile error should mention 'IS NOT subject'

  Scenario: IS NOT subject that is a relationship variable is rejected
    When compiling the following Locy program:
      """
      CREATE RULE flagged AS MATCH (n:Node) WHERE n.risk > 0.5 YIELD KEY n
      CREATE RULE probe AS
        MATCH (a:Node)-[e:R]->(b:Node)
        WHERE e IS NOT flagged
        YIELD KEY a, KEY b
      """
    Then the program should fail to compile
    And the compile error should mention 'relationship variable'

  Scenario: IS NOT subject bound by an earlier positive IS TO target compiles
    When compiling the following Locy program:
      """
      CREATE RULE link AS MATCH (a:Node)-[:R]->(b:Node) YIELD KEY a, KEY b
      CREATE RULE blocked AS MATCH (n:Node) WHERE n.risk > 0.5 YIELD KEY n
      CREATE RULE ok AS
        MATCH (a:Node)
        WHERE a IS link TO mid, mid IS NOT blocked
        YIELD KEY a, KEY mid
      """
    Then the program should compile successfully

  # The TO-target of an IS NOT is bound by the preceding positive reference,
  # never by MATCH. Only the SUBJECT is constrained — this shape appears in
  # seven feature files, a Python binding test and a published notebook.
  Scenario: IS NOT TO-target bound by an earlier positive IS TO compiles
    When compiling the following Locy program:
      """
      CREATE RULE signal AS MATCH (d:Drug)-[:HINTS]->(x:Disease) YIELD KEY d, KEY x
      CREATE RULE known AS MATCH (d:Drug)-[:TREATS]->(x:Disease) YIELD KEY d, KEY x
      CREATE RULE novel AS
        MATCH (d:Drug)
        WHERE d IS signal TO dis, d IS NOT known TO dis
        YIELD KEY d, KEY dis
      """
    Then the program should compile successfully

  # ── Evaluate level ────────────────────────────────────────────────────

  Scenario: IS NOT excludes matching nodes
    Given having executed:
      """
      CREATE (:Node {name: 'A', risk: 0.8}),
             (:Node {name: 'B', risk: 0.2}),
             (:Node {name: 'C', risk: 0.1})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE flagged AS MATCH (n:Node) WHERE n.risk > 0.5 YIELD KEY n
      CREATE RULE clean AS MATCH (n:Node) WHERE n IS NOT flagged YIELD KEY n
      """
    Then evaluation should succeed
    And the derived relation 'flagged' should have 1 facts
    And the derived relation 'clean' should have 2 facts
    And the derived relation 'clean' should contain a fact where n.name = 'B'
    And the derived relation 'clean' should contain a fact where n.name = 'C'
    And the derived relation 'clean' should not contain a fact where n.name = 'A'

  Scenario: IS NOT with empty negated relation passes all
    Given having executed:
      """
      CREATE (:Node {name: 'A', risk: 0.1}),
             (:Node {name: 'B', risk: 0.2})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE flagged AS MATCH (n:Node) WHERE n.risk > 0.9 YIELD KEY n
      CREATE RULE clean AS MATCH (n:Node) WHERE n IS NOT flagged YIELD KEY n
      """
    Then evaluation should succeed
    And the derived relation 'flagged' should have 0 facts
    And the derived relation 'clean' should have 2 facts

  Scenario: Multi-stratum negation chain evaluates in correct order
    Given having executed:
      """
      CREATE (:Node {name: 'A', risk: 0.8}),
             (:Node {name: 'B', risk: 0.2}),
             (:Node {name: 'C', risk: 0.1})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risky AS MATCH (n:Node) WHERE n.risk > 0.5 YIELD KEY n
      CREATE RULE safe AS MATCH (n:Node) WHERE n IS NOT risky YIELD KEY n
      CREATE RULE trusted AS MATCH (n:Node) WHERE n IS safe, n.risk < 0.15 YIELD KEY n
      """
    Then evaluation should succeed
    And the derived relation 'risky' should have 1 facts
    And the derived relation 'safe' should have 2 facts
    And the derived relation 'trusted' should have 1 facts
    And the derived relation 'trusted' should contain a fact where n.name = 'C'
