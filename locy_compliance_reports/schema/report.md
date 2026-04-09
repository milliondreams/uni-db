# TCK Compliance Report

**Generated:** 2026-04-09 02:25:42
**Results:** `results_20260409_022542.json`
**Compared to:** `results_20260409_015724.json`

## Summary

| Metric | Current | Previous | Delta |
|--------|---------|----------|-------|
| Scenarios | 402 | 402 |  |
| Passed | 402 | 270 | +132 |
| Failed | 0 | 132 | -132 |
| Pass Rate | 100.0% | 67.2% | 📈 +32.8pp |

**🟢 Fixed:** 132 scenarios now passing

## Feature Breakdown

| Feature | Scenarios | Passed | Failed | Rate | Delta |
|---------|-----------|--------|--------|------|-------|
| ✅ AbductiveReasoning | 4 | 4 | 0 | 100% |  |
| ✅ Aggregation | 11 | 11 | 0 | 100% |  |
| ✅ AlongWithIsRef | 3 | 3 | 0 | 100% |  |
| ✅ AssumeAbduce | 3 | 3 | 0 | 100% |  |
| ✅ AssumeAbduceExtended | 20 | 20 | 0 | 100% | +100pp |
| ✅ AssumeNestedAbduce | 4 | 4 | 0 | 100% | +100pp |
| ✅ BasicRules | 6 | 6 | 0 | 100% |  |
| ✅ CompositePatterns | 27 | 27 | 0 | 100% | +100pp |
| ✅ CypherFunctions | 9 | 9 | 0 | 100% |  |
| ✅ DeriveEdges | 9 | 9 | 0 | 100% |  |
| ✅ DeriveVisibility | 7 | 7 | 0 | 100% |  |
| ✅ ErrorConditions | 5 | 5 | 0 | 100% |  |
| ✅ ExactProbability | 15 | 15 | 0 | 100% |  |
| ✅ ExplainCombinations | 6 | 6 | 0 | 100% |  |
| ✅ FeatureCombinationMatrix | 6 | 6 | 0 | 100% | +100pp |
| ✅ FoldExecutionPaths | 32 | 32 | 0 | 100% | +100pp |
| ✅ FoldQueryProjection | 4 | 4 | 0 | 100% | +100pp |
| ✅ GoalDirected | 9 | 9 | 0 | 100% |  |
| ✅ Hypothetical | 6 | 6 | 0 | 100% |  |
| ✅ IsNotFoldQueryMatrix | 20 | 20 | 0 | 100% | +100pp |
| ✅ MathematicalReference | 12 | 12 | 0 | 100% | +100pp |
| ✅ ModuleComposition | 8 | 8 | 0 | 100% |  |
| ✅ MonotonicAggregation | 30 | 30 | 0 | 100% |  |
| ✅ MultiStratum | 3 | 3 | 0 | 100% |  |
| ✅ NonRecursive | 4 | 4 | 0 | 100% |  |
| ✅ OptimizedSelection | 6 | 6 | 0 | 100% |  |
| ✅ ParameterBinding | 5 | 5 | 0 | 100% |  |
| ✅ PathCarriedValues | 9 | 9 | 0 | 100% |  |
| ✅ PathCombinations | 5 | 5 | 0 | 100% |  |
| ✅ PrioritizedRules | 5 | 5 | 0 | 100% |  |
| ✅ ProbAbduceAssume | 6 | 6 | 0 | 100% |  |
| ✅ ProbabilisticComplement | 19 | 19 | 0 | 100% |  |
| ✅ ProbabilisticStress | 10 | 10 | 0 | 100% |  |
| ✅ ProofTraces | 3 | 3 | 0 | 100% |  |
| ✅ RecursiveRules | 3 | 3 | 0 | 100% |  |
| ✅ ReservedKeywords | 18 | 18 | 0 | 100% |  |
| ✅ SemanticParity | 7 | 7 | 0 | 100% | +100pp |
| ✅ SharedProofDetection | 4 | 4 | 0 | 100% |  |
| ✅ SimilarToAbduceAssume | 5 | 5 | 0 | 100% |  |
| ✅ SimilarToProbability | 6 | 6 | 0 | 100% |  |
| ✅ Stratification | 6 | 6 | 0 | 100% |  |
| ✅ StratifiedNegation | 5 | 5 | 0 | 100% |  |
| ✅ TopKProofs | 4 | 4 | 0 | 100% |  |
| ✅ TransitiveClosure | 2 | 2 | 0 | 100% |  |
| ✅ TripleCombinations | 5 | 5 | 0 | 100% |  |
| ✅ YieldValueColumns | 6 | 6 | 0 | 100% |  |

## 🟢 Newly Passing

Scenarios that were failing but are now passing:

- **AssumeAbduceExtended** — 4a-1 ASSUME SET edge probability with FOLD MNOR re-evaluation (line 15)
- **AssumeAbduceExtended** — 4a-2 ASSUME SET edge probability with FOLD MPROD re-evaluation (line 49)
- **AssumeAbduceExtended** — 4a-3 ASSUME SET edge weight with FOLD MSUM re-evaluation scoped to target (line 83)
- **AssumeAbduceExtended** — 4a-4 ASSUME CREATE new edge with FOLD MNOR adds new cause (line 118)
- **AssumeAbduceExtended** — 4a-5 ASSUME SET with multi-group FOLD MNOR and QUERY (line 147)
- **AssumeAbduceExtended** — 4b-1 ASSUME DELETE removes one cause and reduces MNOR then IS NOT complement (line 185)
- **AssumeAbduceExtended** — 4b-2 ASSUME DELETE removes all causes making IS NOT yield 1.0 (line 219)
- **AssumeAbduceExtended** — 4b-3 ASSUME DELETE edge with FOLD MPROD and boolean IS NOT (line 248)
- **AssumeAbduceExtended** — 4b-4 ASSUME DELETE with FOLD MSUM and IS NOT filter (line 286)
- **AssumeAbduceExtended** — 4b-5 ASSUME DELETE on composite-key with FOLD MNOR and IS NOT (line 324)
- **AssumeAbduceExtended** — 4c-1 ABDUCE NOT on FOLD MNOR rule with single key (line 376)
- **AssumeAbduceExtended** — 4c-2 ABDUCE NOT on FOLD MPROD rule with composite key (line 404)
- **AssumeAbduceExtended** — 4c-3 ABDUCE NOT on composite-key signal with FOLD MNOR (line 432)
- **AssumeAbduceExtended** — 4c-4 ABDUCE NOT with ASSUME on same FOLD MNOR composite-key rule (line 460)
- **AssumeAbduceExtended** — 4c-5 ABDUCE NOT on FOLD MSUM rule with composite key (line 495)
- **AssumeAbduceExtended** — 4d-1 ASSUME on empty graph with FOLD MNOR returns no rows (line 531)
- **AssumeAbduceExtended** — 4d-2 ABDUCE NOT on empty graph with FOLD MNOR yields no modifications (line 549)
- **AssumeAbduceExtended** — 4d-3 ASSUME SET with FOLD MNOR and IS NOT complement combined (line 562)
- **AssumeAbduceExtended** — 4d-4 ASSUME DELETE with FOLD MNOR then ABDUCE NOT on same rule (line 600)
- **AssumeAbduceExtended** — 4d-5 ASSUME CREATE new node and edge with FOLD MSUM and QUERY (line 631)
- **AssumeNestedAbduce** — ABDUCE NOT inside ASSUME THEN evaluates without error (line 40)
- **AssumeNestedAbduce** — ABDUCE NOT inside ASSUME THEN parses (line 15)
- **AssumeNestedAbduce** — ABDUCE NOT with MNOR inside ASSUME THEN parses (line 26)
- **AssumeNestedAbduce** — QUERY and ABDUCE NOT on MNOR rule inside ASSUME THEN (line 56)
- **CompositePatterns** — 3a-1 Three-stratum chain: base to MNOR to IS NOT complement (line 17)
- **CompositePatterns** — 3a-2 Four-stratum pipeline: facts to FOLD to IS NOT to filter (line 60)
- **CompositePatterns** — 3a-3 Three-stratum: transitive closure to FOLD COUNT to filter (line 106)
- **CompositePatterns** — 3a-4 Three-stratum: PROB scores to MPROD joint to IS NOT complement (line 136)
- **CompositePatterns** — 3b-1 ALONG cost then FOLD SUM across all paths per source (line 178)
- **CompositePatterns** — 3b-2 ALONG hops then FOLD MAX to find longest path per source (line 206)
- **CompositePatterns** — 3b-3 ALONG reliability then FOLD MNOR aggregation across paths (line 248)
- **CompositePatterns** — 3b-4 ALONG with two variables then FOLD COUNT paths (line 279)
- **CompositePatterns** — 3b-5 ALONG cost with BEST BY then FOLD SUM of shortest paths (line 311)
- **CompositePatterns** — 3c-1 BEST BY ASC selects minimum then FOLD SUM across groups (line 349)
- **CompositePatterns** — 3c-2 BEST BY DESC selects maximum then FOLD COUNT (line 376)
- **CompositePatterns** — 3c-3 Shortest path BEST BY then FOLD SUM all shortest costs with QUERY (line 443)
- **CompositePatterns** — 3c-4 BEST BY on recursive ALONG then IS NOT complement (line 405)
- **CompositePatterns** — 3d-1 similar_to with WHERE filter on score then QUERY (line 494)
- **CompositePatterns** — 3d-2 similar_to score as PROB with IS NOT complement (line 520)
- **CompositePatterns** — 3d-3 similar_to pairwise then IS NOT known then QUERY novel pairs (line 548)
- **CompositePatterns** — 3d-4 similar_to to MNOR aggregation then QUERY with filter (line 582)
- **CompositePatterns** — 3d-5 similar_to with WHERE threshold and IS NOT exclusion combined (line 614)
- **CompositePatterns** — 3e-1 Recursive transitive closure then FOLD COUNT then QUERY (line 654)
- **CompositePatterns** — 3e-2 Recursive ALONG cost then FOLD SUM then QUERY per source (line 692)
- **CompositePatterns** — 3e-3 Recursive shortest path BEST BY then FOLD MIN then QUERY (line 739)
- **CompositePatterns** — 3e-4 Recursive reachability then FOLD MCOUNT then IS NOT then QUERY (line 786)
- **CompositePatterns** — 3f-1 WHERE filter on numeric value column from IS-ref (line 823)
- **CompositePatterns** — 3f-2 WHERE filter on FOLD value column from IS-ref (line 849)
- **CompositePatterns** — 3f-3 WHERE range filter on IS-ref value column (line 891)
- **CompositePatterns** — 3f-4 WHERE filter on IS-ref value column with ALONG cost (line 930)
- **CompositePatterns** — 3f-5 WHERE comparison on two IS-ref value columns (line 967)
- **FeatureCombinationMatrix** — ABDUCE NOT on FOLD MNOR rule finds modification (line 177)
- **FeatureCombinationMatrix** — ASSUME mutation with FOLD MNOR re-evaluation (line 91)
- **FeatureCombinationMatrix** — FOLD MNOR penalty then IS NOT complement then QUERY (line 12)
- **FeatureCombinationMatrix** — FOLD MNOR signal then composite-key IS NOT novel then QUERY (line 48)
- **FeatureCombinationMatrix** — Facts to MNOR to IS NOT PROB complement with QUERY (line 121)
- **FeatureCombinationMatrix** — WHERE filter on similar_to score from IS-ref (line 150)
- **FoldExecutionPaths** — MCOUNT derived relation computes correct count (line 609)
- **FoldExecutionPaths** — MCOUNT groups independently across two teams (line 806)
- **FoldExecutionPaths** — MCOUNT via ABDUCE finds modification candidate (line 968)
- **FoldExecutionPaths** — MCOUNT via ASSUME reflects hypothetical member addition (line 672)
- **FoldExecutionPaths** — MCOUNT via QUERY returns correct value (line 640)
- **FoldExecutionPaths** — MMAX and MMIN on same graph yield complementary extremes (line 926)
- **FoldExecutionPaths** — MMAX derived relation computes correct maximum (line 365)
- **FoldExecutionPaths** — MMAX groups independently across two sensors (line 738)
- **FoldExecutionPaths** — MMAX via ABDUCE finds modification candidate (line 456)
- **FoldExecutionPaths** — MMAX via ASSUME reflects hypothetical new reading (line 428)
- **FoldExecutionPaths** — MMAX via QUERY returns correct value (line 396)
- **FoldExecutionPaths** — MMIN derived relation computes correct minimum (line 487)
- **FoldExecutionPaths** — MMIN groups independently across two servers (line 772)
- **FoldExecutionPaths** — MMIN via ABDUCE finds modification candidate (line 578)
- **FoldExecutionPaths** — MMIN via ASSUME reflects hypothetical faster measurement (line 550)
- **FoldExecutionPaths** — MMIN via QUERY returns correct value (line 518)
- **FoldExecutionPaths** — MNOR and MPROD coexist as separate rules (line 845)
- **FoldExecutionPaths** — MNOR derived relation computes correct noisy-OR (line 19)
- **FoldExecutionPaths** — MNOR via ABDUCE finds edge removal candidate (line 100)
- **FoldExecutionPaths** — MNOR via ASSUME reflects hypothetical edge removal (line 72)
- **FoldExecutionPaths** — MNOR via QUERY returns correct value (line 45)
- **FoldExecutionPaths** — MPROD derived relation computes correct product (line 131)
- **FoldExecutionPaths** — MPROD via ABDUCE finds edge removal candidate (line 212)
- **FoldExecutionPaths** — MPROD via ASSUME reflects hypothetical edge removal (line 184)
- **FoldExecutionPaths** — MPROD via QUERY returns correct value (line 157)
- **FoldExecutionPaths** — MSUM and MCOUNT on same graph with different rules (line 888)
- **FoldExecutionPaths** — MSUM derived relation computes correct sum (line 243)
- **FoldExecutionPaths** — MSUM groups independently across two departments (line 704)
- **FoldExecutionPaths** — MSUM via ABDUCE finds modification candidate (line 334)
- **FoldExecutionPaths** — MSUM via ASSUME reflects hypothetical edge addition (line 306)
- **FoldExecutionPaths** — MSUM via QUERY returns correct value (line 274)
- **FoldExecutionPaths** — Two independent FOLD rules produce correct values in same program (line 995)
- **FoldQueryProjection** — MNOR fold value projects through plain QUERY RETURN (line 15)
- **FoldQueryProjection** — MNOR with multiple groups returns all groups via plain QUERY (line 80)
- **FoldQueryProjection** — MPROD fold value projects through plain QUERY RETURN (line 38)
- **FoldQueryProjection** — SUM fold value projects through plain QUERY RETURN (line 59)
- **IsNotFoldQueryMatrix** — 2a-1 Boolean IS NOT with FOLD MNOR and QUERY (line 14)
- **IsNotFoldQueryMatrix** — 2a-2 Boolean IS NOT passes unflagged nodes to FOLD MNOR with QUERY (line 46)
- **IsNotFoldQueryMatrix** — 2a-3 Boolean IS NOT with FOLD MPROD and QUERY (line 78)
- **IsNotFoldQueryMatrix** — 2a-4 Boolean IS NOT with FOLD MSUM and QUERY (line 110)
- **IsNotFoldQueryMatrix** — 2a-5 Boolean IS NOT with empty negated relation passes all to FOLD MNOR (line 142)
- **IsNotFoldQueryMatrix** — 2a-6 Boolean IS NOT multi-group FOLD MNOR with QUERY (line 164)
- **IsNotFoldQueryMatrix** — 2b-1 PROB complement after FOLD MNOR with QUERY (line 206)
- **IsNotFoldQueryMatrix** — 2b-2 PROB complement after FOLD MPROD with QUERY (line 242)
- **IsNotFoldQueryMatrix** — 2b-3 PROB complement absent key yields 1.0 with FOLD MNOR and QUERY (line 277)
- **IsNotFoldQueryMatrix** — 2b-4 Cross-predicate IS + IS NOT PROB with FOLD MNOR and QUERY (line 306)
- **IsNotFoldQueryMatrix** — 2b-5 Double complement recovers original FOLD MNOR probability (line 349)
- **IsNotFoldQueryMatrix** — 2b-6 PROB complement with FOLD MNOR three causes and QUERY (line 385)
- **IsNotFoldQueryMatrix** — 2c-1 Composite-key boolean IS NOT with FOLD MNOR and QUERY (line 425)
- **IsNotFoldQueryMatrix** — 2c-2 Composite-key PROB IS NOT complement with FOLD MNOR and QUERY (line 472)
- **IsNotFoldQueryMatrix** — 2c-3 Composite-key IS NOT all pairs known yields empty novel set (line 512)
- **IsNotFoldQueryMatrix** — 2c-4 Composite-key IS NOT with multiple drugs and FOLD MNOR QUERY (line 546)
- **IsNotFoldQueryMatrix** — 2d-1 Boolean IS NOT feeding recursive FOLD MSUM with QUERY (line 600)
- **IsNotFoldQueryMatrix** — 2d-2 PROB complement IS NOT with recursive MNOR transitive risk and QUERY (line 634)
- **IsNotFoldQueryMatrix** — 2d-3 Boolean IS NOT chain with FOLD MSUM at each stage and QUERY (line 671)
- **IsNotFoldQueryMatrix** — 2d-4 IS NOT PROB complement with FOLD MPROD and chained QUERY (line 704)
- **MathematicalReference** — Complement chain evidence * safety with hand-computed values (line 272)
- **MathematicalReference** — IS NOT complement of MNOR(0.3, 0.5) = 1 - 0.65 = 0.35 (line 245)
- **MathematicalReference** — MNOR edge case one MNOR(1.0) = 1.0 (line 139)
- **MathematicalReference** — MNOR edge case zero MNOR(0.0) = 0.0 (line 124)
- **MathematicalReference** — MNOR five values 1-(0.9)(0.8)(0.7)(0.6)(0.5) = 0.8488 (line 83)
- **MathematicalReference** — MNOR single value 0.3 (line 11)
- **MathematicalReference** — MNOR three values 1-(0.7)(0.5)(0.3) = 0.895 (line 52)
- **MathematicalReference** — MNOR two values 1-(0.7)(0.5) = 0.65 (line 26)
- **MathematicalReference** — MPROD edge case with one 1.0 * 0.5 = 0.5 (line 222)
- **MathematicalReference** — MPROD edge case with zero 0.0 * 0.5 = 0.0 (line 201)
- **MathematicalReference** — MPROD three values 0.8 * 0.9 * 0.7 = 0.504 (line 178)
- **MathematicalReference** — MPROD two values 0.8 * 0.9 = 0.72 (line 156)
- **SemanticParity** — Boolean IS NOT QUERY matches derived (line 89)
- **SemanticParity** — Composite-key IS NOT QUERY matches derived (line 135)
- **SemanticParity** — FOLD MNOR QUERY matches derived (line 35)
- **SemanticParity** — FOLD MPROD QUERY matches derived (line 61)
- **SemanticParity** — IS NOT PROB complement derived relation correctness (line 109)
- **SemanticParity** — Simple non-recursive rule QUERY matches derived (line 12)
- **SemanticParity** — Three-stratum FOLD chain QUERY matches derived (line 175)

## Failed Scenarios

🎉 No failed scenarios!
