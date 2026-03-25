# TCK Compliance Report

**Generated:** 2026-03-25 01:05:37
**Results:** `results_20260325_010537.json`
**Compared to:** `results_20260325_010203.json`

## Summary

| Metric | Current | Previous | Delta |
|--------|---------|----------|-------|
| Scenarios | 256 | 256 |  |
| Passed | 256 | 242 | +14 |
| Failed | 0 | 14 | -14 |
| Pass Rate | 100.0% | 94.5% | 📈 +5.5pp |

**🟢 Fixed:** 14 scenarios now passing

## Feature Breakdown

| Feature | Scenarios | Passed | Failed | Rate | Delta |
|---------|-----------|--------|--------|------|-------|
| ✅ AbductiveReasoning | 4 | 4 | 0 | 100% |  |
| ✅ Aggregation | 11 | 11 | 0 | 100% |  |
| ✅ AlongWithIsRef | 3 | 3 | 0 | 100% |  |
| ✅ AssumeAbduce | 3 | 3 | 0 | 100% |  |
| ✅ BasicRules | 6 | 6 | 0 | 100% |  |
| ✅ CypherFunctions | 9 | 9 | 0 | 100% | +100pp |
| ✅ DeriveEdges | 9 | 9 | 0 | 100% |  |
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
| ✅ ParameterBinding | 5 | 5 | 0 | 100% | +100pp |
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

- **CypherFunctions** — datetime() in QUERY RETURN does not error (line 127)
- **CypherFunctions** — exp() decay pattern in QUERY WHERE models relevance decay (line 144)
- **CypherFunctions** — exp() in QUERY WHERE filters correctly (line 36)
- **CypherFunctions** — exp(0) = 1.0 in QUERY RETURN (line 67)
- **CypherFunctions** — log() in QUERY WHERE filters correctly (line 50)
- **CypherFunctions** — sqrt() in QUERY WHERE filters correctly (line 22)
- **CypherFunctions** — sqrt(4) = 2.0 in QUERY RETURN (line 81)
- **CypherFunctions** — toLower() in QUERY WHERE (line 111)
- **CypherFunctions** — toUpper() in QUERY RETURN (line 97)
- **ParameterBinding** — $param in QUERY WHERE excludes non-matching rows (line 29)
- **ParameterBinding** — $param in QUERY WHERE filters to matching agent (line 13)
- **ParameterBinding** — $param in rule MATCH WHERE scopes derived relation (line 47)
- **ParameterBinding** — Integer $param used in WHERE comparison (line 68)
- **ParameterBinding** — Multiple $params filter on two dimensions (line 86)

## Failed Scenarios

🎉 No failed scenarios!
