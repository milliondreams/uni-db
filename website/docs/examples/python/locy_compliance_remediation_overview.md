# Automated Compliance Gap Detection and Prioritized Remediation

**Industry**: Financial Services / Healthcare / Technology | **Role**: CISO, VP Compliance, Director of Security Operations | **Time to value**: 3-6 hours

## The Problem

Compliance audits reveal gaps, but turning findings into prioritized remediation plans is manual and slow. Security teams maintain spreadsheets of exposed services, vulnerability scan results, and regulatory requirements in disconnected systems. When an auditor asks "show me every internet-facing service that handles PII and is missing encryption at rest," the answer requires joining data from three tools and a human who knows where to look.

## The Traditional Approach

Teams run vulnerability scanners (Qualys, Nessus, Tenable) on a schedule, export results to spreadsheets, and manually cross-reference against compliance frameworks (SOC 2, HIPAA, PCI-DSS). Prioritization is done by severity score, but severity alone does not account for business context -- a critical vulnerability on an internal dev server is not the same as a medium vulnerability on a PII-handling production endpoint. Remediation tracking lives in Jira tickets or a GRC tool, disconnected from the scan data. Audit preparation takes 2-4 weeks of manual evidence gathering.

## With Uni and Locy

Declarative rules define what "compliant" and "non-compliant" mean: a service that is internet-reachable through its dependency chain and carries a high-severity vulnerability is a gap. The rules combine transitive exposure (a service reachable from the internet directly or via the services it depends on) with a severity threshold, then materialize every gap as a concrete finding with a remediation action. The output is a list of non-compliant services, each derived from the same rule set rather than assembled from disconnected spreadsheets.

## What You'll See

- A remediation list of non-compliant services, each with a concrete remediation action
- Automated gap detection combining transitive internet exposure with a vulnerability-severity threshold
- Services that are vulnerable but not internet-reachable correctly excluded, so the team fixes what is actually exposed

## Why It Matters

Audit preparation that takes 2-4 weeks compresses to hours when findings are materialized from declarative rules rather than assembled from spreadsheets. More critically, the rules combine severity with real internet exposure, so the team focuses on services that are genuinely reachable instead of every high-CVSS finding in isolation.

---

[Run the notebook &rarr;](locy_compliance_remediation.md)
