# Drug Repurposing: Finding New Uses for Approved Drugs

**Industry**: Pharmaceutical R&D | **Role**: VP Translational Science, Computational Biology Lead | **Time to value**: 2-3 hours

## The Problem

There are thousands of approved drugs with well-characterized safety profiles. There are thousands of diseases with unmet therapeutic need. Somewhere in the intersection are repurposing candidates — approved drugs that could treat diseases they were never designed for. The challenge is finding them systematically, ranking them by evidence strength, and explaining why each candidate was predicted.

## The Traditional Approach

Computational repurposing pipelines are typically assembled from separate components: literature mining to extract drug-target and target-disease associations, custom graph traversal code to walk protein-protein interaction networks, bespoke aggregation logic to combine evidence from multiple mechanistic pathways, a separate safety scoring module pulling adverse event data, and a structural similarity search for analogue discovery. A team of 3-4 computational biologists spends a sprint wiring these together — typically 500+ lines of Python — with no built-in explainability. When a reviewer asks "why was Drug X predicted for Disease Y?", someone traces the logic manually. Adding a new evidence source means modifying the traversal code.

## With Uni

The notebook encodes the entire pipeline in 15 declarative rules. Multi-hop protein-protein interaction traversal finds indirect mechanistic paths from drug targets to disease-associated proteins. Noisy-OR aggregation combines evidence from independent pathways — if a drug reaches a disease through three separate mechanisms, each adds independent support. Safety penalties down-weight drugs with severe adverse reactions. Structural analogue discovery identifies approved drugs with similar binding profiles. Novelty filtering removes known indications. Built-in EXPLAIN traces exactly why each drug-disease pair was predicted — the full mechanistic derivation tree. ASSUME simulates hypothetical new binding targets. ABDUCE suggests what minimal evidence would support a new indication.

## What You'll See

- 28 novel drug-disease candidates ranked by combined pathway evidence
- Safety-adjusted scores that penalize drugs with severe side effects
- Structural analogue matches linking candidates to known approved drugs
- Full mechanistic derivation trees for every prediction — reviewable by domain experts
- What-if simulation of new protein targets showing how hypothetical evidence changes rankings

## Why It Matters

The same analysis that traditionally requires a team, a sprint, and 500+ lines of custom code is expressed in 15 rules that a domain expert can read, audit, and extend. That changes repurposing from a software engineering project to a scientific reasoning exercise.

---

[Run the notebook →](locy_drug_repurposing.md)
