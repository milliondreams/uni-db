# Automated Compliance Gap Detection and Prioritized Remediation

**Industry**: Financial Services / Healthcare / Technology | **Role**: CISO, VP Compliance, Director of Security Operations | **Time to value**: 3-6 hours

## The Problem

Compliance audits reveal gaps, but turning findings into prioritized remediation plans is manual and slow. Security teams maintain spreadsheets of exposed services, vulnerability scan results, and regulatory requirements in disconnected systems. When an auditor asks "show me every internet-facing service that handles PII and is missing encryption at rest," the answer requires joining data from three tools and a human who knows where to look.

## The Traditional Approach

Teams run vulnerability scanners (Qualys, Nessus, Tenable) on a schedule, export results to spreadsheets, and manually cross-reference against compliance frameworks (SOC 2, HIPAA, PCI-DSS). Prioritization is done by severity score, but severity alone does not account for business context -- a critical vulnerability on an internal dev server is not the same as a medium vulnerability on a PII-handling production endpoint. Remediation tracking lives in Jira tickets or a GRC tool, disconnected from the scan data. Audit preparation takes 2-4 weeks of manual evidence gathering.

## With Uni and Locy

Declarative rules define what "compliant" and "non-compliant" mean: an exposed service handling sensitive data without required controls is a gap. The system ingests service inventories, data classification tags, and control status, then materializes every gap as a concrete finding with a remediation action. Prioritization rules weight business context (data sensitivity, exposure level, regulatory scope) alongside technical severity. The output is a ranked remediation list with full provenance -- every finding traces back to the specific rule and data that produced it.

## What You'll See

- Prioritized remediation list ranked by combined business and technical risk, not just CVSS scores
- Automated gap detection that identifies services failing specific compliance controls with the evidence chain
- Audit-ready evidence: each finding includes the rule that flagged it, the data it matched, and the recommended remediation action

## Why It Matters

Audit preparation that takes 2-4 weeks compresses to hours when findings are materialized from declarative rules rather than assembled from spreadsheets. More critically, the remediation list reflects actual business risk, so the team fixes what matters first instead of chasing the highest CVSS score.

---

[Run the notebook &rarr;](locy_compliance_remediation.md)
