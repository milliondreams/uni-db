# Fraud Ring Detection: Laundering Cycles and Shared-Device Anomalies

**Industry**: Banking / Fintech / Insurance | **Role**: Head of Financial Crime, VP Risk | **Time to value**: 3-5 hours

## The Problem

Money laundering rings move funds through chains of accounts designed to look like unrelated transactions. Shared-device patterns -- where multiple supposedly unrelated accounts log in from the same device or IP -- are a strong signal of coordinated fraud. Traditional systems catch individual suspicious transactions but miss the network structure that connects them.

## The Traditional Approach

Rule engines flag transactions above static thresholds (e.g., transfers over $10,000, rapid sequences). Network analysis, when it exists, runs in a separate tool -- often a batch job over exported transaction logs using custom graph analytics code (2,000-4,000 lines of Python or Scala). Shared-device detection lives in yet another system, typically the identity platform. Correlating a flagged transaction with its network context and device footprint requires an analyst to manually query three systems. Investigation time per case averages 45-90 minutes.

## With Uni

Cycle detection and shared-device pattern matching run in a single graph query. The system identifies 3-node transfer cycles (A pays B, B pays C, C pays A) directly from transaction data, then overlays device and IP sharing patterns to find accounts that are structurally connected. Risk signals from both analyses are combined per account, so investigators see the full picture -- network position, transaction flow, and device overlap -- in one result set.

## What You'll See

- Detection of 3-cycle laundering rings with the specific accounts and transaction amounts involved
- Shared-device clusters identifying accounts linked by common devices or IP addresses
- Combined risk signals that weight both network topology and device anomalies per account

## Why It Matters

Reducing investigation time from 60 minutes to 10 minutes per case matters when your team handles 500 cases a month. More importantly, network-level detection catches rings that transaction-level rules structurally cannot see.

---

[Run the notebook &rarr;](fraud_detection.md)
