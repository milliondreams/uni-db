# Locy Use Cases

## 1. Compliance and Control Inference

Model control chains and remediation paths using recursive rules and abductive suggestions.

Notebook:
- Python: [Compliance Remediation](../examples/python/locy_compliance_remediation.ipynb)
- Rust: [Compliance Remediation](../examples/rust/locy_compliance_remediation.ipynb)

## 2. RBAC / Policy Reasoning

Use prioritized rules to model allow/deny cascades with explainable outcomes.

Notebook:
- Python: [RBAC Priority](../examples/python/locy_rbac_priority.ipynb)
- Rust: [RBAC Priority](../examples/rust/locy_rbac_priority.ipynb)

## 3. Infrastructure Blast Radius

Combine recursive reachability with `ASSUME` to evaluate outage scenarios before changes.

Notebook:
- Python: [Infrastructure Blast Radius](../examples/python/locy_infrastructure_blast_radius.ipynb)
- Rust: [Infrastructure Blast Radius](../examples/rust/locy_infrastructure_blast_radius.ipynb)

## 4. Supply Chain Provenance

Track multi-hop lineage and derive trusted routes with `ALONG` + `BEST BY`.

Notebook:
- Python: [Supply Chain Provenance](../examples/python/locy_supply_chain_provenance.ipynb)
- Rust: [Supply Chain Provenance](../examples/rust/locy_supply_chain_provenance.ipynb)

## 5. Fraud and Risk Propagation

Express propagation and exception logic with stratified negation and monotonic aggregation.

Notebook:
- Python: [Fraud Risk Propagation](../examples/python/locy_fraud_risk_propagation.ipynb)
- Rust: [Fraud Risk Propagation](../examples/rust/locy_fraud_risk_propagation.ipynb)

## Pattern Template

For each use case, model:

1. Base graph entities and edges.
2. Inference relations (`CREATE RULE`).
3. Targeted questions (`QUERY`).
4. Explainability (`EXPLAIN RULE`).
5. Optional remediation (`ABDUCE`).
