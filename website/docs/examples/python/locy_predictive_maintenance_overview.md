# Predictive Maintenance: Topology-Aware Failure Forecasting

**Industry**: Industrial Manufacturing | **Role**: VP Reliability, Plant Manager | **Time to value**: 2-3 hours

## The Problem

Your CMMS lists 500 assets due for maintenance. Your team has capacity to service ten this week. Pick the wrong ten and the eleventh fails in production, takes out three downstream assets through process-line dependencies, and turns a planned maintenance window into a multi-day unplanned outage. Sensor-based predictive maintenance tools score each asset in isolation; they do not know that asset 47's failure cascades into the entire downstream subsystem.

## The Traditional Approach

Reliability engineers run vibration analysis, temperature trending, and oil sampling per asset, then build a ranked list in a spreadsheet or specialist PdM tool. CMMS handles work-order scheduling but knows nothing about sensor data. Process-line topology lives in a P&ID drawing nobody has digitized. Calibration of the underlying ML scores against actual outcomes happens annually, if at all -- most teams ship raw classifier outputs and rely on engineer judgment to interpret them. Cascade-aware scheduling -- servicing the asset whose failure takes down the most downstream value -- is informal.

## With Uni

The notebook ingests AI4I 2020 sensor data plus a synthesized process-line topology directly into Uni's graph. A failure-likelihood classifier runs as a Locy neural predicate registered via `LocyConfig.register_classifier`. The classifier output is calibrated in-language using `CALIBRATE ... METHOD platt_scaling` against held-out historical failures, then validated with Brier and accuracy. Component-level risk composes through `FOLD MNOR(unhealth)` over a `HAS_PART` child set; line-level reliability composes through `FOLD MPROD(1 - asset_risk)` across `UPSTREAM_OF` chains using inline classifier invocation. The `EXPLAIN` trace surfaces every classifier call's `NeuralProvenance` for audit, with the rule chain that composed the per-asset score into the line-level rollup.

## What You'll See

- Calibrated per-asset failure probabilities from a registered Python classifier (in production, an ONNX-exported XGBoost or similar)
- Line-level joint reliability rollups computed by composing per-asset probabilities through `FOLD MPROD(1 - asset_risk)` across `UPSTREAM_OF` chains
- Component-level risk aggregation via `FOLD MNOR(unhealth)` over a `HAS_PART` child set
- Calibration delta: raw Brier vs Platt-calibrated Brier on held-out outcomes
- Validation report with Brier and accuracy
- Audit trail (`EXPLAIN` with `NeuralProvenance`) for every recommendation: sensor evidence, the classifier's calibrated probability, the rule chain that produced the score

## Why It Matters

Unplanned outages at a typical continuous-process plant cost $50k-200k per hour. Reducing false-positive maintenance alerts and false-negative missed failures directly protects throughput. Calibrated probabilities plus topology-aware composition (line-level reliability built from per-asset risks) change which ten assets get serviced this week.

**Data**: [AI4I 2020 Predictive Maintenance Dataset](https://archive.ics.uci.edu/dataset/601/) (UCI #601, CC BY 4.0) -- 10k machine instances, 14 sensor features, 5 failure modes. Process-line topology is synthesized for the notebook and clearly marked as such.

---

[Run the notebook →](locy_predictive_maintenance.md)
