# Probabilistic Risk Scoring: Combining Independent Quality Signals

**Industry**: Supply Chain / Vendor Management | **Role**: Chief Risk Officer, Procurement Lead | **Time to value**: 1-2 hours

## The Problem

You evaluate vendors against multiple independent quality signals — financial stability, SOC 2 compliance, delivery performance, cybersecurity posture, ESG rating. Each signal has its own confidence level. You need two answers: what is the probability that at least one signal indicates a problem, and what is the probability that a vendor passes every check? Weighted averages give you neither.

## The Traditional Approach

Teams typically assign weights to each signal and compute a weighted average. This is mathematically wrong for the questions being asked — a vendor with five 80%-confidence signals is not "80% risky." Custom scoring models get built in Python or Excel, but they require manual calibration every time a new signal source is added. The difference between "any-failure" risk (noisy-OR) and "all-must-pass" reliability (joint probability) is either conflated or handled by maintaining two separate models that drift apart. When a vendor's score changes, explaining why requires digging through the model code.

## With Uni

The notebook models each quality signal as an independent probabilistic fact with its own confidence. Noisy-OR aggregation (MNOR) computes each component's failure risk — the probability that at least one of its signals indicates a problem, the "any-failure" score. Joint reliability (MPROD) then rolls those component risks up to a per-vendor reliability — the probability that all of a vendor's components hold simultaneously. Both are expressed as declarative `FOLD` rules, not custom aggregation code. Adding a new signal source means adding one fact and re-evaluating; the aggregation follows automatically.

## What You'll See

- Component risk scores using noisy-OR (MNOR) that correctly model independent failure modes
- Joint vendor reliability scores (MPROD) showing the probability all of a vendor's components hold simultaneously
- Clear semantic separation between "any-failure" risk and "all-must-pass" reliability
- Aggregation expressed as declarative `FOLD` rules rather than hand-tuned scoring code

## Why It Matters

The difference between noisy-OR and weighted-average scoring is not academic — it changes which vendors get flagged. Correct probabilistic combination means fewer false negatives on genuinely risky vendors and fewer false positives on safe ones.

---

[Run the notebook →](locy_probabilistic_risk_scoring.md)
