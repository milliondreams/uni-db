# Infrastructure Blast Radius Analysis

**Industry**: Cloud / Platform Engineering | **Role**: VP Engineering, SRE Lead | **Time to value**: 3 hours

## The Problem

When a service goes down at 2 AM, the first 20 minutes of every incident are spent answering the same question: what else is affected? Dependency maps are stale the day they are drawn. CMDBs reflect what was deployed six months ago, not what is running now. The blast radius is discovered empirically, one Slack message at a time.

## The Traditional Approach

Teams maintain dependency graphs in Confluence or a CMDB, updated quarterly if at all. During an incident, an engineer manually traces upstream and downstream services, often missing transitive dependencies two or three hops away. A typical microservices estate of 200 services has 600+ dependency edges; no human can traverse that reliably under pressure. Post-incident reviews routinely find impacted services that were missed during response.

## With Uni

The notebook loads service-to-service dependency edges and business criticality scores, then defines a recursive traversal rule. Given any failing service, Uni computes the complete set of transitively impacted downstream services in milliseconds. Results are ranked by business criticality so the incident commander knows which teams to page first. The entire analysis is 12 declarative rules with no imperative graph-walking code.

## What You'll See

- Complete transitive blast radius for any service, including indirect dependencies 4-5 hops deep
- Impact ranking by business criticality, so response effort goes to the highest-value systems first
- A derivation path for each impacted service, showing the exact chain of dependencies that propagates the failure

## Why It Matters

The average cost of a major incident is $5,600 per minute. Cutting 15 minutes of manual dependency tracing from every P1 saves real money and reduces the cascading failures that turn a single outage into a company-wide event.

---

[Run the notebook →](locy_infrastructure_blast_radius.md)
