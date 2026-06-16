# Fraud Risk Propagation Through Account Networks

**Industry**: Financial Services | **Role**: Head of Financial Crime, Chief Risk Officer | **Time to value**: 3 hours

## The Problem

When an account is confirmed fraudulent, every account that transacted with it becomes a potential risk. But fraud networks are not one hop deep — money moves through chains of intermediary accounts before reaching its destination. Identifying the full exposure from a single flagged account requires traversing a transaction graph that batch systems were not designed to explore.

## The Traditional Approach

Transaction monitoring systems score accounts independently. When fraud is confirmed, an analyst manually traces recent transfers, flags direct counterparties, and opens cases. Network analysis, if it exists, runs as a separate batch job on a different platform. A typical investigation covers 2-3 hops and takes 4-8 analyst-hours. Accounts beyond the analyst's traversal window remain unscored. Meanwhile, clean accounts that happen to share a demographic with flagged ones get caught in broad-brush freezes.

## With Uni

The notebook defines backward risk propagation through transfer edges: an account that transfers to a flagged or already-risky account becomes risky too, transitively along the chain. Accounts with no path to any flagged entity are explicitly isolated as clean using negation — not just unflagged, but provably uninvolved. The entire model is a handful of declarative rules covering seed flagging, transitive propagation, and clean-account isolation.

## What You'll See

- Risky accounts identified across the full transaction network, not just direct counterparties
- A clean account whitelist — accounts provably isolated from flagged entities, reducing false freezes
- Transitive propagation through chains of intermediary accounts, computed declaratively from the transfer graph

## Why It Matters

False positives cost financial institutions an estimated $50 per alert in analyst time. Replacing manual network tracing with automated propagation and provable clean-account isolation reduces both missed fraud and wasted investigation hours.

---

[Run the notebook →](locy_fraud_risk_propagation.md)
