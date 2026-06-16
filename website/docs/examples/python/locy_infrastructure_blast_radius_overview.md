# Infrastructure Blast Radius Analysis

**Industry**: Cloud / Platform Engineering | **Role**: VP Engineering, SRE Lead | **Time to value**: 3 hours

## The Problem

When a service goes down at 2 AM, the first 20 minutes of every incident are spent answering the same question: what else is affected? Dependency maps are stale the day they are drawn. CMDBs reflect what was deployed six months ago, not what is running now. The blast radius is discovered empirically, one Slack message at a time.

## The Traditional Approach

Teams maintain dependency graphs in Confluence or a CMDB, updated quarterly if at all. During an incident, an engineer manually traces upstream and downstream services, often missing transitive dependencies two or three hops away. A typical microservices estate of 200 services has 600+ dependency edges; no human can traverse that reliably under pressure. Post-incident reviews routinely find impacted services that were missed during response.

## With Uni

The notebook loads service-to-service `CALLS` dependency edges, then defines a recursive traversal rule. Given any failing service, Uni computes the complete set of transitively impacted downstream services. The entire analysis is two declarative rules plus a query, with no imperative graph-walking code.

## What You'll See

- Complete transitive blast radius for any service, including indirect dependencies several hops deep
- A single recursive rule that captures direct neighbors and all transitively reachable downstream services
- A flat list of every impacted service for a given failing service, derived from the dependency edges rather than hand-traced

## Why It Matters

The average cost of a major incident is $5,600 per minute. Cutting 15 minutes of manual dependency tracing from every P1 saves real money and reduces the cascading failures that turn a single outage into a company-wide event.

---

[Run the notebook →](locy_infrastructure_blast_radius.md)
