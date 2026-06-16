# Adverse Drug Reaction Signal Detection with Audit-Ready Provenance

**Industry**: Pharmaceutical Post-Market Safety | **Role**: Drug Safety Officer, Pharmacovigilance Lead | **Time to value**: 2-3 hours

## The Problem

Eighty thousand new adverse event reports landed in the safety system last month. Most are background noise; a handful are real signals that, if missed, become regulatory actions. The team has capacity to investigate roughly fifty signals deeply. Picking the wrong fifty means a real safety signal sits in the queue while investigators chase noise. The current scoring system is a disproportionality calculation that does not consider the underlying pharmacology, the narrative similarity to known historical events, or the confidence the team should place in any individual score.

## The Traditional Approach

Pharmacovigilance teams run disproportionality analysis (PRR, ROR) on FAERS-style report streams, manually map free-text narratives to MedDRA preferred terms, and triage signals by signal strength alone. Drug safety physicians review case narratives one at a time. Mechanism plausibility -- does this drug actually have a known target pathway that would produce this reaction? -- is assessed by recall from training. The audit trail submitted to the regulator is a Word document reconstructed from email threads and meeting notes. Calibration of the underlying disproportionality scores against confirmed outcomes is rarely done because the math is awkward.

## With Uni

The notebook ingests a drug-target-side-effect subgraph drawn from Hetionet directly into Uni. A registered Locy neural predicate (`signal_score`) scores each candidate report from its combined evidence (report count times precomputed narrative similarity). A separate `similar_to` rule scores each report's narrative embedding against a historical-confirmed-signal centroid. Graph-structural plausibility is composed separately: a `mechanistic_path` rule traverses real Hetionet `CbG`/`GpPW`/`CcSE` edges (the Vilar shared-pathway heuristic), and a `FOLD MNOR(...)` rollup turns the bridging paths into a per-pair mechanism-plausibility score. Calibration in-language (Platt scaling) fits the raw classifier outputs to the held-out `is_signal` labels and reports raw-vs-calibrated Brier + ECE; a separate `VALIDATE` step reports Brier and accuracy. The `EXPLAIN` trace produces the audit artifact regulators ask for: the derivation tree for a scored report, including the model name, the raw probability, the calibrated probability (when a calibrator is registered), and the exact feature dict the classifier saw.

## What You'll See

- Calibrated signal scores (Platt scaling) instead of raw disproportionality numbers
- Narrative-similarity scores (`similar_to`) ranking each report against a historical-confirmed-signal centroid
- Mechanistic path explanations: which bridging drug, which shared pathway, which adverse event -- composed with `FOLD MNOR`
- In-language `CALIBRATE` (raw vs calibrated Brier + ECE) and `VALIDATE` (Brier + accuracy) against held-out `is_signal` labels
- `BEST BY` selection: the single highest-evidence report per adverse event
- Audit-grade `EXPLAIN` derivation traces ready for regulatory submission -- every score is reproducible
- Ranked investigation queue: top (drug, AE) pairs by calibrated credibility times mechanism plausibility

## Why It Matters

A single missed safety signal that surfaces later as a regulatory action costs tens to hundreds of millions in remediation, recalls, and reputational damage. Calibrated scoring plus audit-ready provenance is the difference between defensible pharmacovigilance and pharmacovigilance theater.

**Data**: [Hetionet v1.0](https://het.io/) (CC0 1.0 Universal). The notebook uses a curated extract -- the 30 most-connected compounds plus their bound genes, participating pathways, and caused side effects, drawn from Hetionet's real `CbG`/`GpPW`/`CcSE` edges (no synthetic edges). The narrative report stream is synthesized from those real drug → side-effect pairs and clearly marked as such; the 16-dimensional narrative embeddings are synthetic vectors biased toward (or away from) a historical-signal centroid, used by the `similar_to` lookup. Citation: Himmelstein DS et al., *eLife* 2017 (DOI: 10.7554/eLife.26726).

---

[Run the notebook →](locy_adverse_drug_reaction.md)
