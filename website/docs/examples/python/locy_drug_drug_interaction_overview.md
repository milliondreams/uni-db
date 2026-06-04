# Polypharmacy Safety: GNN-Composed Drug-Drug Interaction Risk

**Industry**: Clinical Decision Support | **Role**: Clinical Pharmacist, EHR Tech Lead | **Time to value**: 3-4 hours

## The Problem

The average patient over 65 takes six prescription medications simultaneously. Pairwise drug-interaction databases exist, but they answer the wrong question. The clinical question is: given this patient's entire regimen, is the joint risk of any clinically significant interaction acceptable, and which pair contributes most to that joint risk? Manual review of one patient's polypharmacy risk takes a clinical pharmacist 20-30 minutes. Tens of millions of patients are on five or more daily medications.

## The Traditional Approach

EHR systems flag pairwise interactions from curated databases. The interface shows a list of pairwise warnings, often dozens for elderly patients, which clinicians learn to dismiss. Joint regimen risk is not computed; clinicians compose pairwise risks mentally. Calibration of the underlying interaction-likelihood scores against real-world adverse-event rates is not part of the workflow.

## With Uni

The notebook ingests a drug + interaction subgraph extracted from Hetionet directly into Uni. Drug embeddings are derived offline from the graph (a small `TruncatedSVD` on the drug-target adjacency matrix in the prep script, mirroring the production "offline graph learning → lightweight runtime head" pattern). The runtime model is a small MLP head that takes the concatenated embeddings of two drugs and predicts P(interact); it's exported to ONNX and wrapped as a registered Python classifier. Locy rules treat each patient's regimen as a clique and use inline classifier invocation in `FOLD MPROD(1 - p_interact)` across all drug-pairs to produce a joint regimen-safety probability. Calibration uses Vilar-style shared-target labels (drugs sharing ≥2 targeted genes flagged as dangerous) derived from the Hetionet `CbG` edges via `CALIBRATE ... METHOD platt_scaling`. `EXPLAIN` produces a trace combining the contributing pair predictions with the rule-derivation provenance.

## What You'll See

- Patient-level joint regimen risk computed from per-pair model predictions composed through Locy rules (`FOLD MPROD(1 - p_interact)` across all drug pairs in the regimen)
- Ranked pairwise interactions within the regimen — which two drugs contribute the highest calibrated interaction probability to joint risk
- Calibrated binary danger probabilities against held-out shared-target labels (raw vs Platt-calibrated Brier delta)
- Validation report with Brier and accuracy on held-out dangerous-interaction labels
- Audit-grade `EXPLAIN` trace combining per-pair predictions with rule provenance, surfacing the classifier's `NeuralProvenance` per derivation

## Why It Matters

Adverse drug events are among the leading preventable causes of death. Joint regimen risk with the audit trail that produced it changes a clinician's workflow from "dismiss the alert wall" to "this is the pair that drove the score, and why."

**Data**: [Hetionet v1.0](https://het.io/) (CC0 1.0 Universal) — 47k biomedical entities, 2.2M relationships across 11 node types and 24 edge types. The notebook ingests a small drug + interaction subgraph filtered from Hetionet. The patient regimen sample is synthesized for the notebook and clearly marked as such. Drug embeddings are derived offline by the prep script (`TruncatedSVD` over the drug-target adjacency matrix, 64-dim) — the production analogue is an R-GCN, but the SVD approach is dependency-light and demonstrates the same "offline graph learning → lightweight runtime head" deployment pattern. The runtime MLP head is exported to ONNX, loaded via `onnxruntime`, and wrapped as a registered Python classifier. Citation: Himmelstein DS et al., *eLife* 2017.

---

[Run the notebook →](locy_drug_drug_interaction.md)
