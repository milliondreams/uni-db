Feature: DEEP_LOCY.md §16 integrated example — neural + ALONG + MNOR + MPROD

  Faithful (slightly simplified) translation of §16's layered model
  combining property-feature neural scoring, ALONG-position
  invocation, MNOR composition across redundant paths, and MPROD
  composition across required subcomponents. Closes the Phase B
  gate "DEEP_LOCY.md §16 integrated example parses, compiles,
  executes for symbolic + neural portions".

  Simplifications vs. the literal §16 text:
    * Mock classifier instead of the Candle-backed supplier_risk_scorer
      (Candle is exercised independently in CandleClassifier.feature).
    * Numeric properties (country: Int, revenue: Float) — String
      properties also work post-property-access slice but Int keeps
      the fixture hand-verifiable.
    * sanctions_signal model dropped — its `semantic_match(s.profile,
      'sanctions violation')` feature_expr is a Phase D D2 follow-up
      (retrieval-backed feature exprs require pre-invoke projection
      not yet implemented). The §16 "symbolic + neural portions"
      gate is closed without it; sanctions_signal can be added once
      D2 lands.
    * 2 suppliers × 1 sub-part × 1 assembly fixture, asserting the
      layered MNOR / MPROD output against hand-computation.

  Background:
    Given an empty graph

  Scenario: §16 supplier risk + supply path + part + assembly availability
    Given having executed:
      """
      CREATE (asm:Part {name: 'Assembly'}),
             (sub:Part {name: 'SubPart'}),
             (s1:Supplier {name: 'S1', country: 1, revenue: 100}),
             (s2:Supplier {name: 'S2', country: 2, revenue: 200}),
             (sub)-[:SUPPLIED_BY {base_reliability: 0.9}]->(s1),
             (sub)-[:SUPPLIED_BY {base_reliability: 0.8}]->(s2),
             (asm)-[:REQUIRES]->(sub)
      """
    And a registered mock classifier "supplier_risk_scorer" returning 0.5
    When evaluating the following Locy program with neural_predicates_preview:
      """
      CREATE MODEL supplier_risk_scorer AS
        INPUT (s)
        OUTPUT PROB risk
        USING xervo('classify/supplier-risk-v3')
        CALIBRATION platt_scaling

      CREATE RULE supply_path AS
        MATCH (part:Part)-[s:SUPPLIED_BY]->(supplier:Supplier)
        ALONG reliability = s.base_reliability * (1.0 - supplier_risk_scorer(supplier))
        YIELD KEY part, KEY supplier, reliability

      CREATE RULE part_availability AS
        MATCH (part:Part)-[s:SUPPLIED_BY]->(supplier:Supplier)
        ALONG reliability = s.base_reliability * (1.0 - supplier_risk_scorer(supplier))
        FOLD avail = MNOR(reliability)
        YIELD KEY part, avail

      CREATE RULE assembly_availability AS
        MATCH (asm:Part)-[:REQUIRES]->(sub:Part)
        WHERE sub IS part_availability
        FOLD joint = MPROD(avail)
        YIELD KEY asm, joint
      """
    # Hand-computed for sub-part SubPart:
    #   path1 reliability: 0.9 * (1 - 0.5) = 0.45
    #   path2 reliability: 0.8 * (1 - 0.5) = 0.40
    #   part_availability MNOR: 1 - (1 - 0.45)(1 - 0.40) = 1 - 0.33 = 0.67
    #   assembly_availability MPROD over 1 sub: 0.67
    Then evaluation should succeed
    And the derived relation 'part_availability' should contain a fact where avail is approximately 0.67 within 0.000001
    And the derived relation 'assembly_availability' should contain a fact where joint is approximately 0.67 within 0.000001
