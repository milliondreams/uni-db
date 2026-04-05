# Cyber Exposure Twin: Vulnerability Prioritization at Scale

**Industry**: Cybersecurity | **Role**: CISO, Security Engineering Lead | **Time to value**: 2-3 hours

## The Problem

Your scanner just reported 10,000 vulnerabilities. CVSS scores tell you 3,200 are "critical" or "high." Your team has capacity to remediate 400 this quarter. Picking the wrong 400 means the breach comes through a medium-severity finding on a business-critical asset that was two hops from the internet.

## The Traditional Approach

Most teams start with CVSS and layer on manual context: asset owners tag criticality in spreadsheets, network topology lives in a separate CMDB, and SLA deadlines are tracked in ticketing systems. A senior analyst spends days cross-referencing these sources to build a prioritized list. Blast radius analysis — understanding which assets an attacker can reach from a compromised host — requires custom graph traversal code or expensive specialized tools. When leadership asks "why did we patch server X before server Y?", the answer is tribal knowledge. Remediation planning is done by gut feel, not by modeling which fixes reduce the most risk per dollar spent.

## With Uni

The notebook encodes exposure scoring as declarative rules that combine CVSS base scores, EPSS exploit probability, asset criticality tiers, and SLA urgency into a single prioritized ranking. Blast radius analysis uses graph traversal rules to trace reachability — which critical assets are downstream of each vulnerable host. Remediation candidates are ranked by risk-reduction-per-dollar, accounting for the fact that patching one upstream host may eliminate exposure for dozens of downstream assets. Every score comes with a full derivation: you can trace exactly why a vulnerability was ranked where it was.

## What You'll See

- Exposure-ranked vulnerability list that accounts for topology, criticality, and exploit likelihood — not just CVSS
- Blast radius per finding showing which business-critical assets are reachable from each vulnerable host
- Team-level rollup dashboards grouping exposure by remediation owner and SLA deadline
- Optimal remediation plan maximizing risk reduction within a fixed engineering budget
- Containment scenario simulation showing how network segmentation changes reduce blast radius

## Why It Matters

Security teams that prioritize by exposure context instead of raw CVSS scores typically reduce realized risk by 3-5x with the same remediation capacity. The audit trail means every prioritization decision is defensible.

---

[Run the notebook →](locy_cyber_exposure_twin.md)
