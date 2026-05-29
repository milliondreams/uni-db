Feature: FOLD Aggregate Monotonicity in Recursive Strata

  Compile-time enforcement that FOLD aggregates used in recursive strata
  carry `Semilattice.monotone_join = true`. Built-in monotone aggregates
  (MMAX/MMIN/MCOUNT/MNOR/MPROD/MSUM) are accepted; non-monotone SUM/AVG/
  COLLECT are rejected. The check uses the plugin registry's Semilattice
  metadata so user-registered monotone aggregates participate.

  Background:
    Given an empty graph

  Scenario: Non-monotonic SUM in self-recursive rule is rejected
    When compiling the following Locy program:
      """
      CREATE RULE r AS MATCH (a)-[:E]->(b) YIELD KEY a, KEY b, 0 AS total
      CREATE RULE r AS MATCH (a)-[:E]->(mid) WHERE mid IS r TO b
        FOLD total = SUM(a.cost) YIELD KEY a, KEY b, total
      """
    Then the program should fail to compile
    And the compile error should mention 'non-monotonic'

  Scenario: Monotonic MMAX in self-recursive rule compiles
    When compiling the following Locy program:
      """
      CREATE RULE r AS MATCH (a)-[:E]->(b) YIELD KEY a, KEY b, 0 AS peak
      CREATE RULE r AS MATCH (a)-[:E]->(mid) WHERE mid IS r TO b
        FOLD peak = MMAX(a.cost) YIELD KEY a, KEY b, peak
      """
    Then the program should compile successfully
    And the program should have 1 strata
    And the stratum 0 should be recursive

  Scenario: Monotonic MSUM in self-recursive rule compiles
    When compiling the following Locy program:
      """
      CREATE RULE r AS MATCH (a)-[:E]->(b) YIELD KEY a, KEY b, 0 AS total
      CREATE RULE r AS MATCH (a)-[:E]->(mid) WHERE mid IS r TO b
        FOLD total = MSUM(a.weight) YIELD KEY a, KEY b, total
      """
    Then the program should compile successfully
    And the program should have 1 strata
    And the stratum 0 should be recursive

  Scenario: Monotonic MNOR in self-recursive rule compiles
    When compiling the following Locy program:
      """
      CREATE RULE r AS MATCH (a)-[:E]->(b) YIELD KEY a, KEY b, 0 AS score
      CREATE RULE r AS MATCH (a)-[:E]->(mid) WHERE mid IS r TO b
        FOLD score = MNOR(a.weight) YIELD KEY a, KEY b, score
      """
    Then the program should compile successfully
    And the program should have 1 strata
    And the stratum 0 should be recursive

  Scenario: Non-monotonic AVG in self-recursive rule is rejected
    When compiling the following Locy program:
      """
      CREATE RULE r AS MATCH (a)-[:E]->(b) YIELD KEY a, KEY b, 0 AS mean
      CREATE RULE r AS MATCH (a)-[:E]->(mid) WHERE mid IS r TO b
        FOLD mean = AVG(a.cost) YIELD KEY a, KEY b, mean
      """
    Then the program should fail to compile
    And the compile error should mention 'non-monotonic'

  Scenario: Non-monotonic SUM in non-recursive rule compiles
    When compiling the following Locy program:
      """
      CREATE RULE r AS MATCH (a)-[:E]->(b)
        FOLD total = SUM(b.cost) YIELD KEY a, total
      """
    Then the program should compile successfully
    And the program should have 1 strata
    And the stratum 0 should not be recursive
