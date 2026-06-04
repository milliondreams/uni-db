# Adverse Drug Reaction Signal Detection with Audit-Ready Provenance

**Industry**: Pharmaceutical Post-Market Safety | **Role**: Drug Safety Officer, Pharmacovigilance Lead | **Time to value**: 2-3 hours

## The Problem

Eighty thousand new adverse event reports landed in the safety system last month. Most are background noise; a handful are real signals that, if missed, become regulatory actions. The team has capacity to investigate roughly fifty signals deeply. Picking the wrong fifty means a real safety signal sits in the queue while investigators chase noise. The current scoring system is a disproportionality calculation that does not consider the underlying pharmacology, the narrative similarity to known historical events, or the confidence the team should place in any individual score.

## The Traditional Approach

Pharmacovigilance teams run disproportionality analysis (PRR, ROR) on FAERS-style report streams, manually map free-text narratives to MedDRA preferred terms, and triage signals by signal strength alone. Drug safety physicians review case narratives one at a time. Mechanism plausibility -- does this drug actually have a known target pathway that would produce this reaction? -- is assessed by recall from training. The audit trail submitted to the regulator is a Word document reconstructed from email threads and meeting notes. Calibration of the underlying disproportionality scores against confirmed outcomes is rarely done because the math is awkward.

## With Uni

The notebook ingests a drug-target-side-effect subgraph drawn from Hetionet directly into Uni. A Locy neural predicate scores each candidate signal using property features (report counts, demographic enrichment), embedding similarity (`similar_to` between the reported narrative text and a corpus of historical confirmed-signal narratives), and graph-structural features (the drug's connectivity to the affected adverse-event class through known target pathways). Calibration in-language brings raw scores in line with historical confirmation rates; validation reports Brier and ECE. The EXPLAIN trace produces the audit artifact regulators ask for: which signal, which similar past confirmed report, which mechanistic path supports the alert, and the calibrated probability with confidence band.

## What You'll See

- Calibrated signal scores with confidence bands instead of raw disproportionality numbers
- Semantic-matched comparable historical reports for every candidate signal -- investigators see the closest past case
- Mechanistic path explanations: which drug target, which pathway, which adverse-event class
- Calibration improvement against held-out confirmed-vs-rejected signals
- Audit-grade derivation traces ready for regulatory submission -- every score is reproducible
- Ranked investigation queue: top signals for the week's case review

## Why It Matters

A single missed safety signal that surfaces later as a regulatory action costs tens to hundreds of millions in remediation, recalls, and reputational damage. Calibrated scoring plus audit-ready provenance is the difference between defensible pharmacovigilance and pharmacovigilance theater.

**Data**: [Hetionet v1.0](https://het.io/) (CC0 1.0 Universal) -- 47k biomedical entities, 2.2M relationships, 11 node types, 24 edge types. The narrative report stream is synthesized for the notebook and clearly marked as such; the drug → side-effect signal graph and mechanistic paths come from Hetionet's curated edges. Sentence embeddings use [`sentence-transformers/all-MiniLM-L6-v2`](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2) (Apache-2.0). Citation: Himmelstein DS et al., *eLife* 2017.

---

[Run the notebook →](locy_adverse_drug_reaction.md)
