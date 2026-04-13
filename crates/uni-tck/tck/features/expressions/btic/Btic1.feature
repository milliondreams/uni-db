#encoding: utf-8

@extension
Feature: Btic1 - BTIC Temporal Interval Functions
  # Implementation extension: Binary Temporal Interval Codec (BTIC)
  # These functions are uni-db extensions to Cypher, not part of openCypher.

  # =========================================================================
  # Construction via btic()
  # =========================================================================

  @extension
  Scenario Outline: [1] Should construct BTIC from string literal
    Given any graph
    When executing query:
      """
      RETURN btic(<literal>) AS b
      """
    Then the result should be, in any order:
      | b        |
      | <result> |
    And no side effects

    Examples:
      | literal              | result                                                                     |
      | '1985'               | '[1985-01-01T00:00:00.000Z, 1986-01-01T00:00:00.000Z) ~year'               |
      | '1985-03'            | '[1985-03-01T00:00:00.000Z, 1985-04-01T00:00:00.000Z) ~month'              |
      | '1985-03-15'         | '[1985-03-15T00:00:00.000Z, 1985-03-16T00:00:00.000Z) ~day'                |

  @extension
  Scenario: [2] Should construct BTIC with solidus range
    Given any graph
    When executing query:
      """
      RETURN btic('1985/1990') AS b
      """
    Then the result should be, in any order:
      | b                                                                          |
      | '[1985-01-01T00:00:00.000Z, 1991-01-01T00:00:00.000Z) ~year'              |
    And no side effects

  @extension
  Scenario: [3] Should construct unbounded BTIC
    Given any graph
    When executing query:
      """
      RETURN btic('2020-03/') AS b
      """
    Then the result should be, in any order:
      | b                                                     |
      | '[2020-03-01T00:00:00.000Z, +inf) month/'             |
    And no side effects

  @extension
  Scenario: [4] Should construct BTIC with certainty prefix
    Given any graph
    When executing query:
      """
      RETURN btic('~1985') AS b
      """
    Then the result should be, in any order:
      | b                                                                                          |
      | '[1985-01-01T00:00:00.000Z, 1986-01-01T00:00:00.000Z) ~year [approximate]'                 |
    And no side effects

  @extension
  Scenario: [5] Should propagate null through btic()
    Given any graph
    When executing query:
      """
      RETURN btic(null) AS b
      """
    Then the result should be, in any order:
      | b    |
      | null |
    And no side effects

  # =========================================================================
  # Accessor functions
  # =========================================================================

  @extension
  Scenario: [6] btic_lo should return lower bound as datetime
    Given any graph
    When executing query:
      """
      RETURN toString(btic_lo(btic('1985'))) AS lo
      """
    Then the result should be, in any order:
      | lo                               |
      | '1985-01-01T00:00Z[UTC]' |
    And no side effects

  @extension
  Scenario: [7] btic_hi should return upper bound as datetime
    Given any graph
    When executing query:
      """
      RETURN toString(btic_hi(btic('1985'))) AS hi
      """
    Then the result should be, in any order:
      | hi                            |
      | '1986-01-01T00:00Z[UTC]' |
    And no side effects

  @extension
  Scenario: [8] btic_duration should return milliseconds
    Given any graph
    When executing query:
      """
      RETURN btic_duration(btic('1985')) AS dur
      """
    Then the result should be, in any order:
      | dur         |
      | 31536000000 |
    And no side effects

  @extension
  Scenario: [9] btic_granularity should return granularity name
    Given any graph
    When executing query:
      """
      RETURN btic_granularity(btic('1985')) AS g
      """
    Then the result should be, in any order:
      | g      |
      | 'year' |
    And no side effects

  @extension
  Scenario: [10] btic_certainty should return certainty name
    Given any graph
    When executing query:
      """
      RETURN btic_certainty(btic('~1985')) AS c
      """
    Then the result should be, in any order:
      | c             |
      | 'approximate' |
    And no side effects

  # =========================================================================
  # Boolean predicates
  # =========================================================================

  @extension
  Scenario: [11] btic_is_finite should detect finite intervals
    Given any graph
    When executing query:
      """
      RETURN btic_is_finite(btic('1985')) AS finite,
             btic_is_finite(btic('/')) AS infinite
      """
    Then the result should be, in any order:
      | finite | infinite |
      | true   | false    |
    And no side effects

  @extension
  Scenario: [12] btic_is_unbounded should detect unbounded intervals
    Given any graph
    When executing query:
      """
      RETURN btic_is_unbounded(btic('2020-03/')) AS unbounded,
             btic_is_unbounded(btic('1985')) AS bounded
      """
    Then the result should be, in any order:
      | unbounded | bounded |
      | true      | false   |
    And no side effects

  # =========================================================================
  # Binary predicates
  # =========================================================================

  @extension
  Scenario: [13] btic_overlaps should detect overlapping intervals
    Given any graph
    When executing query:
      """
      RETURN btic_overlaps(btic('1985'), btic('1985-06/1986-06')) AS overlaps,
             btic_overlaps(btic('1985'), btic('1990')) AS disjoint
      """
    Then the result should be, in any order:
      | overlaps | disjoint |
      | true     | false    |
    And no side effects

  @extension
  Scenario: [14] btic_contains_point should test point-in-interval
    Given any graph
    When executing query:
      """
      RETURN btic_contains_point(btic('1985'), 486000000000) AS inside,
             btic_contains_point(btic('1985'), 0) AS outside
      """
    Then the result should be, in any order:
      | inside | outside |
      | true   | false   |
    And no side effects

  @extension
  Scenario: [15] btic_before and btic_after should test temporal ordering
    Given any graph
    When executing query:
      """
      RETURN btic_before(btic('1985'), btic('1990')) AS before_result,
             btic_after(btic('1990'), btic('1985')) AS after_result
      """
    Then the result should be, in any order:
      | before_result | after_result |
      | true          | true         |
    And no side effects

  @extension
  Scenario: [16] btic_equals should compare interval bounds
    Given any graph
    When executing query:
      """
      RETURN btic_equals(btic('1985'), btic('1985')) AS eq,
             btic_equals(btic('1985'), btic('1990')) AS neq
      """
    Then the result should be, in any order:
      | eq   | neq   |
      | true | false |
    And no side effects

  # =========================================================================
  # Set operations
  # =========================================================================

  @extension
  Scenario: [17] btic_span should compute bounding interval
    Given any graph
    When executing query:
      """
      RETURN btic_span(btic('1985'), btic('1990')) AS span
      """
    Then the result should be, in any order:
      | span                                                                       |
      | '[1985-01-01T00:00:00.000Z, 1991-01-01T00:00:00.000Z) ~year'               |
    And no side effects

  @extension
  Scenario: [18] btic_intersection should compute overlapping part
    Given any graph
    When executing query:
      """
      RETURN btic_intersection(btic('1985'), btic('1985-06/1986-06')) AS inter
      """
    Then the result should be, in any order:
      | inter                                                                      |
      | '[1985-06-01T00:00:00.000Z, 1986-01-01T00:00:00.000Z) month/year'          |
    And no side effects

  @extension
  Scenario: [19] btic_gap should compute gap between disjoint intervals
    Given any graph
    When executing query:
      """
      RETURN btic_gap(btic('1985'), btic('1990')) AS gap
      """
    Then the result should be, in any order:
      | gap                                                                        |
      | '[1986-01-01T00:00:00.000Z, 1990-01-01T00:00:00.000Z) ~year'               |
    And no side effects

  # =========================================================================
  # Storage round-trip
  # =========================================================================

  @extension
  Scenario: [20] Should store and retrieve BTIC value via CREATE
    Given an empty graph
    When executing query:
      """
      CREATE ({period: btic('1985')})
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 1 |
    When executing control query:
      """
      MATCH (n)
      RETURN n.period AS p
      """
    Then the result should be, in any order:
      | p                                                                          |
      | '[1985-01-01T00:00:00.000Z, 1986-01-01T00:00:00.000Z) ~year'               |

  @extension
  Scenario: [21] Should update BTIC value via SET
    Given an empty graph
    And having executed:
      """
      CREATE ({name: 'test', period: btic('1985')})
      """
    When executing query:
      """
      MATCH (n {name: 'test'})
      SET n.period = btic('1990')
      RETURN n.period AS p
      """
    Then the result should be, in any order:
      | p                                                                          |
      | '[1990-01-01T00:00:00.000Z, 1991-01-01T00:00:00.000Z) ~year'               |
