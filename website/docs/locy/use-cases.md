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

## 6. Semiconductor Yield Excursion Triage (Planned)

Model excursion-driven failure triage in semiconductor manufacturing using real SECOM-derived data and full advanced Locy flow.

Notebook:
- Python: [Semiconductor Yield Excursion Triage](../examples/python/locy_semiconductor_yield_excursion.ipynb)

Plan + Data:
- Detailed notebook blueprint: [Semiconductor Yield Excursion Notebook Plan](semiconductor-yield-excursion-notebook-plan.md)
- Data bundle: [SECOM-derived notebook data](../examples/data/locy_semiconductor_yield_excursion/README.md)

## 7. Pharma Batch Genealogy Decisioning (Flagship #2)

Use recursive path reasoning and action selection to model batch-risk propagation and choose interventions by risk-first optimization.

Notebook:
- Python: [Pharma Batch Genealogy](../examples/python/locy_pharma_batch_genealogy.ipynb)

Data bundle:
- [Pharma notebook data](../examples/data/locy_pharma_batch_genealogy/README.md)

## 8. Cyber Exposure-to-Remediation Decision Twin (Flagship #3)

Integrate hybrid evidence retrieval, columnar risk analytics, and Locy remediation reasoning in one flow for high-impact cyber prioritization.

Notebook:
- Python: [Cyber Exposure-to-Remediation Twin](../examples/python/locy_cyber_exposure_twin.ipynb)

Data bundle:
- [Cyber flagship notebook data](../examples/data/locy_cyber_exposure_twin/README.md)

## 9. Knowledge-Augmented Incident Triage

Route support incidents to the best resolver by combining service-dependency graph traversal with semantic matching against runbooks and expertise profiles. Pure vector search finds similar runbooks but doesn't know which team owns them or which service is affected. Pure graph queries traverse service→team→runbook paths but can't score semantic relevance. Locy + `similar_to` does both: walk the dependency graph and score each candidate by how well their expertise matches the incident.

```cypher
-- Find the best-matching runbook reachable through the affected service's dependency graph
CREATE RULE best_resolver AS
MATCH (incident:Incident)-[:AFFECTS]->(svc:Service)-[:DEPENDS_ON*]->(dep:Service)
      <-[:OWNS]-(team:Team)-[:HAS_RUNBOOK]->(rb:Runbook)
WHERE similar_to(rb.embedding, incident.description) > 0.5
ALONG match_score = similar_to(rb.embedding, incident.description)
BEST BY match_score DESC
YIELD KEY incident, KEY team, KEY rb, match_score

QUERY best_resolver WHERE incident.id = $incident_id
RETURN team.name, rb.title, match_score
```

This pattern applies wherever routing decisions need both structural constraints (org hierarchy, dependency chains, escalation paths) and semantic relevance (matching descriptions, expertise, or domain knowledge).

See [Vector Search guide](../guides/vector-search.md#similar_to-expression-function) for `similar_to` details.

## Pattern Template

For each use case, model:

1. Base graph entities and edges.
2. Inference relations (`CREATE RULE`).
3. Targeted questions (`QUERY`).
4. Explainability (`EXPLAIN RULE`).
5. Optional remediation (`ABDUCE`).
