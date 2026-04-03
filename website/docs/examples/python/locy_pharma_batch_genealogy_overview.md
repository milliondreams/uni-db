# Pharmaceutical Batch Genealogy and Intervention Planning

**Industry**: Pharmaceutical Manufacturing | **Role**: VP Quality, Head of Manufacturing Operations | **Time to value**: 4 hours

## The Problem

A deviation is found in a batch of active pharmaceutical ingredient. Every downstream batch that incorporated that material — and every batch made from those batches — is potentially affected. Identifying the full impact, assessing risk, and deciding which batches to quarantine, retest, or release is a regulatory obligation under 21 CFR Part 211 and EU GMP Annex 15. Getting it wrong means either a costly over-recall or an FDA warning letter.

## The Traditional Approach

Quality teams trace batch genealogy through MES batch records, often across multiple systems for different manufacturing stages. A single API batch may feed 8-12 formulation batches, each producing 3-5 packaging lots. Tracing this tree manually takes 2-4 days. Risk assessment is done in spreadsheets, with quality reviewers assigning impact scores by judgment. Intervention decisions — quarantine, retest, or release — are made batch by batch, balancing risk against cost and supply continuity. The entire process is documented in Word files for regulatory submission.

## With Uni

The notebook defines recursive campaign lineage traversal: starting from the deviated batch, Uni traces every downstream batch through formulation, filling, and packaging stages. Risk accumulates through the genealogy — batches closer to the deviation and with higher material incorporation rates receive higher risk scores. Intervention selection is cost-optimized with a dual priority: minimize risk first, then minimize cost among equally safe options. Every impacted batch, risk score, and intervention recommendation includes a derivation trace that constitutes ready-made evidence for regulatory submission. The model is 14 declarative rules covering lineage, risk propagation, intervention costing, and optimal selection.

## What You'll See

- Complete batch genealogy from deviated source through every downstream stage, with no manual tracing
- Risk-ranked impacted batches with scores derived from genealogy distance and material incorporation
- Optimal intervention plan: quarantine, retest, or release for each batch, selected risk-first then cost-second
- Derivation evidence for every conclusion, structured for inclusion in regulatory deviation reports

## Why It Matters

A pharmaceutical recall costs $10-50M on average and takes months to resolve. Reducing genealogy tracing from days to minutes and replacing judgment-based intervention with optimized, evidence-backed decisions protects both patients and the business.

---

[Run the notebook →](locy_pharma_batch_genealogy.md)
