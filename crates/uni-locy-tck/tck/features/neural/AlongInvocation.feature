Feature: Phase B ALONG/FOLD-position neural model invocations

  Validates that model invocations are correctly lifted from ALONG
  expressions and FOLD aggregate inputs (not just YIELD items). The
  compiler rewrites every model call site to
  `Variable("__model_<name>_<idx>")` and the planner inserts
  `LocyModelInvokeExec` between the clause body and `LocyProject`
  so synthesized columns are in scope when the projection's ALONG /
  YIELD expressions and FoldExec evaluate.

  Background:
    Given an empty graph

  Scenario: ALONG-position model invocation inside arithmetic
    # `1.0 - scorer(s)` inside an ALONG expression — the model call
    # must be lifted out of the arithmetic, the synthesized column
    # injected, and the arithmetic evaluated against it.
    Given having executed:
      """
      CREATE (s:Supplier {name: 'S1'}),
             (p:Part {name: 'P1'}),
             (s)-[e:ASSESSED {base: 0.8}]->(p)
      """
    And a registered mock classifier "scorer" returning 0.25
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL scorer AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/scorer')

      CREATE RULE reliability AS
        MATCH (s:Supplier)-[e:ASSESSED]->(p:Part)
        ALONG link = e.base * (1.0 - scorer(s))
        YIELD KEY s, KEY p, link
      """
    # Hand-computed: 0.8 * (1.0 - 0.25) = 0.6.
    Then evaluation should succeed
    And the derived relation 'reliability' should contain a fact where link is approximately 0.6 within 0.000001

  Scenario: Same model invoked in YIELD and ALONG positions
    # Two lift sites in one clause. The compiler allocates distinct
    # synthetic columns; memoization keeps the actual classifier call
    # count down. Output values should match across both positions
    # for the same input.
    Given having executed:
      """
      CREATE (s:Supplier {name: 'S1'}),
             (p:Part {name: 'P1'}),
             (s)-[e:ASSESSED {base: 1.0}]->(p)
      """
    And a registered mock classifier "scorer" returning 0.4
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL scorer AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/scorer')

      CREATE RULE combined AS
        MATCH (s:Supplier)-[e:ASSESSED]->(p:Part)
        ALONG link = e.base * scorer(s)
        YIELD KEY s, KEY p, link, scorer(s) AS direct
      """
    Then evaluation should succeed
    And the derived relation 'combined' should contain a fact where link is approximately 0.4 within 0.000001
    And the derived relation 'combined' should contain a fact where direct is approximately 0.4 within 0.000001

  Scenario: FOLD aggregate consuming ALONG with model invocation
    # The §16 layered shape: ALONG carries a classifier-multiplied
    # reliability per edge, FOLD MNOR composes across edges. Two
    # parallel edges with same base reliability — composed result
    # uses the classifier output from both paths.
    Given having executed:
      """
      CREATE (s:Supplier {name: 'S1'}),
             (p:Part {name: 'P1'}),
             (s)-[e1:ASSESSED {base: 1.0}]->(p),
             (s)-[e2:ASSESSED {base: 1.0}]->(p)
      """
    And a registered mock classifier "scorer" returning 0.5
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL scorer AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/scorer')

      CREATE RULE combined AS
        MATCH (s:Supplier)-[e:ASSESSED]->(p:Part)
        ALONG link = e.base * scorer(s)
        FOLD avail = MNOR(link)
        YIELD KEY s, KEY p, avail
      """
    # Per edge: link = 1.0 * 0.5 = 0.5. MNOR over 2 = 1 - (1-0.5)^2 = 0.75.
    Then evaluation should succeed
    And the derived relation 'combined' should contain a fact where avail is approximately 0.75 within 0.000001
