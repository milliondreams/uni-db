# Probabilistic Logic (MNOR / MPROD)

## The Problem

Standard fold operators break down for probability combination:

- **MSUM** overestimates — summing probabilities can exceed 1.0, producing invalid results.
- **MMAX** underestimates — taking the maximum ignores the reinforcement from multiple independent paths.

When your domain is probabilities (values in [0, 1]), you need operators that respect probability semantics.

## MNOR — Noisy-OR

**Formula:** `P = 1 − ∏(1 − pᵢ)`

**Semantics:** "Any independent cause can trigger the effect." Each input is an independent probability that some cause produces the effect. The combined probability is: what is the chance that *at least one* cause fires?

| Property       | Value |
|----------------|-------|
| Identity       | `0.0` (no causes → no effect) |
| Direction      | Non-decreasing (more causes → higher probability) |
| Domain         | Inputs clamped to [0, 1] |

### Worked Example

Three quality signals flag a component as defective with probabilities 0.3, 0.2, and 0.1:

```
P(defective) = 1 − (1 − 0.3)(1 − 0.2)(1 − 0.1)
             = 1 − 0.7 × 0.8 × 0.9
             = 1 − 0.504
             = 0.496
```

In Locy:

```cypher
CREATE RULE component_failure_risk AS
MATCH (c:Component)-[:HAS_SIGNAL]->(s:QualitySignal)
FOLD risk = MNOR(1.0 - s.pass_rate)
YIELD KEY c, risk
```

With three signals having pass rates 0.7, 0.8, and 0.9 (failure probabilities 0.3, 0.2, 0.1), the fold computes `1 − (1−0.3)(1−0.2)(1−0.1) = 0.496`.

## MPROD — Product

**Formula:** `P = ∏ pᵢ`

**Semantics:** "All conditions must hold simultaneously." Each input is an independent probability of success. The combined probability is: what is the chance that *every* condition holds?

| Property       | Value |
|----------------|-------|
| Identity       | `1.0` (no conditions → certain) |
| Direction      | Non-increasing (more conditions → lower probability) |
| Domain         | Inputs clamped to [0, 1] |

### Underflow Protection

When many small probabilities are multiplied, the product can underflow to zero. MPROD automatically switches to log-space accumulation (`exp(∑ ln(pᵢ))`) when the running product drops below `1e-15`. If any input is exactly `0.0`, the result is immediately `0.0`.

### Worked Example

A vendor supplies three components with individual reliabilities 0.95, 0.90, and 0.85:

```
P(all reliable) = 0.95 × 0.90 × 0.85
                = 0.72675
```

In Locy:

```cypher
CREATE RULE vendor_reliability AS
MATCH (v:Vendor)-[:SUPPLIES]->(c:Component)
WHERE c IS component_failure_risk
FOLD reliability = MPROD(1.0 - component_failure_risk.risk)
YIELD KEY v, reliability
```

## When to Use Which

| Operator | Semantics | Direction | Use When |
|----------|-----------|-----------|----------|
| MSUM     | Sum       | ↑ | Non-negative counts or weights |
| MMAX     | Maximum   | ↑ | Worst-case / dominant signal |
| MMIN     | Minimum   | ↓ | Best-case / bottleneck |
| MNOR     | Noisy-OR  | ↑ | Independent OR-causes (probabilities) |
| MPROD    | Product   | ↓ | Independent AND-conditions (probabilities) |

**Rule of thumb:** If you're asking "could *any* of these cause X?" use MNOR. If you're asking "do *all* of these hold?" use MPROD.

## Compiler Guardrails

**ProbabilityDomainViolation warning.** When MNOR or MPROD is used with non-literal arguments, the compiler emits a warning reminding you that inputs should be valid probabilities in [0, 1]. Literal constants (e.g., `MNOR(0.3)`) are checked at compile time and do not trigger the warning.

**BEST BY rejection.** Monotonic folds (including MNOR and MPROD) are incompatible with `BEST BY` witness selection. The compiler rejects this combination with a `BestByWithMonotonicFold` error.

**Input clamping.** At runtime, values outside [0, 1] are clamped before computation. This prevents NaN or negative results from bad data, but you should fix the upstream source rather than relying on clamping.

## Combining with similar_to

`similar_to()` returns scores in [0, 1], making its output natural probability input for MNOR and MPROD:

```cypher
-- Combine semantic relevance signals with noisy-OR
CREATE RULE evidence_strength AS
MATCH (claim:Claim)-[:SUPPORTED_BY]->(doc:Document)
FOLD strength = MNOR(similar_to(doc.embedding, claim.text))
YIELD KEY claim, strength

-- Joint confidence across required criteria
CREATE RULE joint_match AS
MATCH (job:Job)-[:REQUIRES]->(skill:Skill)<-[:HAS]-(candidate:Candidate)
FOLD fit = MPROD(similar_to(skill.embedding, candidate.resume))
YIELD KEY job, KEY candidate, fit
```

## Related

- [ALONG / FOLD / BEST BY](along-fold-bestby.md) — full FOLD documentation
- [Rule Semantics](../rule-semantics.md) — monotonicity and stratification
- [Vector Search guide](../../guides/vector-search.md#similar_to-expression-function) — `similar_to()` documentation
