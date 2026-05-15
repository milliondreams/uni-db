Feature: Retrieval-backed feature expressions (Phase D D1/D2)

  Phase D D1 enables `similar_to(...)` as a model-invocation
  argument in a rule body: the compiler validates the FunctionCall,
  the runtime resolves each sub-expression against the per-row
  fact_row, evaluates the UDF, and feeds the resulting Float to
  the classifier. Phase D D2 compile-accepts
  `semantic_match(prop, 'text')` and surfaces a clear runtime
  error pending Xervo embedder threading into
  `apply_model_invocations`.

  Background:
    Given an empty graph

  # ── D1 happy path: similar_to feeds a Float feature ─────────────────────

  Scenario: similar_to(prop, literal) at the call site feeds the classifier
    Given having executed:
      """
      CREATE (:Doc {name: 'D1', embedding: [1.0, 0.0, 0.0]})
      """
    And a registered mock classifier "echo" driven by Float feature "sim"
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL echo AS
        INPUT (sim)
        OUTPUT PROB risk
        USING xervo('classify/echo')

      CREATE RULE risky AS
        MATCH (d:Doc)
        YIELD KEY d, echo(similar_to(d.embedding, [1.0, 0.0, 0.0])) AS risk
      """
    Then evaluation should succeed
    # Cosine([1,0,0], [1,0,0]) = 1.0 → echo classifier returns the Float feature unchanged
    And the derived relation 'risky' should contain a fact where risk = 1.0

  # ── D1: orthogonal embedding → 0.0 similarity ───────────────────────────

  Scenario: similar_to with orthogonal vectors yields zero feature
    Given having executed:
      """
      CREATE (:Doc {name: 'D2', embedding: [0.0, 1.0, 0.0]})
      """
    And a registered mock classifier "echo" driven by Float feature "sim"
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL echo AS
        INPUT (sim)
        OUTPUT PROB risk
        USING xervo('classify/echo')

      CREATE RULE risky AS
        MATCH (d:Doc)
        YIELD KEY d, echo(similar_to(d.embedding, [1.0, 0.0, 0.0])) AS risk
      """
    Then evaluation should succeed
    And the derived relation 'risky' should contain a fact where risk = 0.0

  # ── D1: wrong arity rejected at compile time ────────────────────────────

  Scenario: similar_to with 1 arg fails at compile time
    Given having executed:
      """
      CREATE (:Doc {name: 'X'})
      """
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL bad AS
        INPUT (sim)
        OUTPUT PROB risk
        USING xervo('classify/bad')

      CREATE RULE r AS
        MATCH (d:Doc)
        YIELD KEY d, bad(similar_to(d.embedding)) AS risk
      """
    Then evaluation should fail
    And the evaluation error should mention "similar_to"

  # ── D2: semantic_match compile-accepts; runtime errors clearly ──────────

  Scenario: semantic_match accepted at compile, errors at runtime
    Given having executed:
      """
      CREATE (:Doc {name: 'D3', embedding: [0.5, 0.5, 0.7]})
      """
    And a registered mock classifier "echo" driven by Float feature "sim"
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL echo AS
        INPUT (sim)
        OUTPUT PROB risk
        USING xervo('classify/echo')

      CREATE RULE risky AS
        MATCH (d:Doc)
        YIELD KEY d, echo(semantic_match(d.embedding, 'sanctions violation')) AS risk
      """
    Then evaluation should fail
    And the evaluation error should mention "semantic_match"
