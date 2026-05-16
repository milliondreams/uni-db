# Polypharmacy Safety: GNN-Composed Drug-Drug Interaction Risk

**Industry**: Clinical Decision Support | **Role**: Clinical Pharmacist, EHR Tech Lead | **Time to value**: 3-4 hours

## The Problem

The average patient over 65 takes six prescription medications simultaneously. Pairwise drug-interaction databases exist, but they answer the wrong question. The clinical question is: given this patient's entire regimen, is the joint risk of any clinically significant interaction acceptable? And if not, what is the smallest substitution that brings it back into range? Manual review of one patient's polypharmacy risk takes a clinical pharmacist 20-30 minutes. Tens of millions of patients are on five or more daily medications.

## The Traditional Approach

EHR systems flag pairwise interactions from curated databases. The interface shows a list of pairwise warnings, often dozens for elderly patients, which clinicians learn to dismiss. Joint regimen risk is not computed; clinicians compose pairwise risks mentally. Substitution recommendations require manual lookup of therapeutic equivalents and re-running the pairwise check against the patient's remaining drugs. Calibration of the underlying interaction-likelihood scores against real-world adverse-event rates is not part of the workflow.

## With Uni

The notebook ingests Hetionet's heterogeneous biomedical graph -- drugs, protein targets, side effects, pathways, genes -- directly into Uni. A Relational Graph Convolutional Network (R-GCN) is trained offline on this graph to learn drug embeddings that capture each drug's full graph context: targets, pathways, side-effect patterns, and similarity to other drugs. The runtime model is a lightweight MLP head that takes the embeddings of two drugs and predicts P(interact). This is the standard production deployment pattern for graph-neural systems: heavyweight graph learning offline, lightweight composable head online. Locy rules treat each patient's regimen as a clique and use `FOLD MPROD(1 - p_interact)` across all drug-pairs to produce a joint regimen-safety probability. Calibration uses Hetionet-derived severity tiers as ground truth with Dirichlet calibration for the multi-severity outputs. `ASSUME` models drug substitution. `ABDUCE` finds the smallest substitution set that brings joint risk below threshold. `EXPLAIN` produces a trace that combines the contributing pair predictions with the rule-derivation provenance.

## What You'll See

- Patient-level joint regimen risk computed from per-pair model predictions composed through Locy rules
- Ranked pairwise interactions within the regimen -- which two drugs contribute most to joint risk
- Substitution simulation: pick a therapeutic-equivalent class, see how joint risk changes
- ABDUCE-generated minimum-change recommendation: smallest substitution set to bring risk below threshold
- Audit-grade EXPLAIN trace combining per-pair predictions with rule provenance
- Calibrated severity-tier probabilities against held-out real-world outcomes

## Why It Matters

Adverse drug events are among the leading preventable causes of death. Joint regimen risk plus actionable substitution recommendations is what changes a clinician's workflow from "dismiss the alert wall" to "this is the one substitution that matters."

**Data**: [Hetionet v1.0](https://het.io/) (CC BY 4.0) -- 47k biomedical entities, 2.2M relationships across 11 node types and 24 edge types, ample for heterogeneous-graph drug embedding learning. The patient regimen sample is synthesized for the notebook and clearly marked as such. The R-GCN is trained offline (one notebook cell on the drug-target-pathway subgraph) producing 64-dim drug embeddings; the runtime MLP head is exported to ONNX and registered with uni-xervo. Citation: Himmelstein DS et al., *eLife* 2017.

---

[Run the notebook →](locy_drug_drug_interaction.md)
