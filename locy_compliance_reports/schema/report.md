# TCK Compliance Report

**Generated:** 2026-03-30 21:29:44
**Results:** `results_20260330_212944.json`
**Compared to:** `results_20260330_210422.json`

## Summary

| Metric | Current | Previous | Delta |
|--------|---------|----------|-------|
| Scenarios | 263 | 263 |  |
| Passed | 263 | 256 | +7 |
| Failed | 0 | 7 | -7 |
| Pass Rate | 100.0% | 97.3% | 📈 +2.7pp |

**🟢 Fixed:** 7 scenarios now passing

## Feature Breakdown

| Feature | Scenarios | Passed | Failed | Rate | Delta |
|---------|-----------|--------|--------|------|-------|
| ✅ AbductiveReasoning | 4 | 4 | 0 | 100% |  |
| ✅ Aggregation | 11 | 11 | 0 | 100% |  |
| ✅ AlongWithIsRef | 3 | 3 | 0 | 100% |  |
| ✅ AssumeAbduce | 3 | 3 | 0 | 100% |  |
| ✅ BasicRules | 6 | 6 | 0 | 100% |  |
| ✅ CypherFunctions | 9 | 9 | 0 | 100% |  |
| ✅ DeriveEdges | 9 | 9 | 0 | 100% |  |
| ✅ DeriveVisibility | 7 | 7 | 0 | 100% | +100pp |
| ✅ ErrorConditions | 5 | 5 | 0 | 100% |  |
| ✅ ExactProbability | 15 | 15 | 0 | 100% |  |
| ✅ ExplainCombinations | 6 | 6 | 0 | 100% |  |
| ✅ GoalDirected | 9 | 9 | 0 | 100% |  |
| ✅ Hypothetical | 6 | 6 | 0 | 100% |  |
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
| ✅ ProbabilisticComplement | 13 | 13 | 0 | 100% |  |
| ✅ ProbabilisticStress | 10 | 10 | 0 | 100% |  |
| ✅ ProofTraces | 3 | 3 | 0 | 100% |  |
| ✅ RecursiveRules | 3 | 3 | 0 | 100% |  |
| ✅ ReservedKeywords | 18 | 18 | 0 | 100% |  |
| ✅ SharedProofDetection | 4 | 4 | 0 | 100% |  |
| ✅ SimilarToAbduceAssume | 5 | 5 | 0 | 100% |  |
| ✅ SimilarToProbability | 6 | 6 | 0 | 100% |  |
| ✅ Stratification | 6 | 6 | 0 | 100% |  |
| ✅ StratifiedNegation | 5 | 5 | 0 | 100% |  |
| ✅ TopKProofs | 4 | 4 | 0 | 100% |  |
| ✅ TransitiveClosure | 2 | 2 | 0 | 100% |  |
| ✅ TripleCombinations | 5 | 5 | 0 | 100% |  |
| ✅ YieldValueColumns | 5 | 5 | 0 | 100% |  |

## 🟢 Newly Passing

Scenarios that were failing but are now passing:

- **DeriveVisibility** — DERIVE edges do not persist to graph without tx.apply (line 111)
- **DeriveVisibility** — QUERY then DERIVE then trailing Cypher (line 91)
- **DeriveVisibility** — Trailing Cypher after DERIVE sees derived edges (line 16)
- **DeriveVisibility** — Trailing Cypher after empty DERIVE returns 0 correctly (line 124)
- **DeriveVisibility** — Trailing Cypher count reflects derived edge count (line 31)
- **DeriveVisibility** — Trailing Cypher joins derived edges with existing graph (line 51)
- **DeriveVisibility** — Trailing Cypher sees edges from multiple DERIVE commands (line 72)

## Failed Scenarios

🎉 No failed scenarios!
