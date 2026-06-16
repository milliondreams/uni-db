# Supply Chain Provenance Tracing

**Industry**: Manufacturing / Procurement | **Role**: VP Supply Chain, Chief Procurement Officer | **Time to value**: 4 hours

## The Problem

Regulations like UFLPA, the EU Corporate Sustainability Due Diligence Directive, and customer audit requirements demand visibility into every upstream supplier for a finished product. Most organizations can name their tier-1 suppliers. Few can trace to tier-3 or beyond. When a compliance event hits — a sanctioned entity, a forced-labor finding, a conflict mineral — the scramble to map exposure takes weeks.

## The Traditional Approach

Procurement teams maintain supplier lists in spreadsheets or ERP modules that track direct purchase orders. Tracing beyond tier-1 requires manual outreach: emailing suppliers to ask who their suppliers are. A typical automotive OEM has 200 tier-1 suppliers and an estimated 5,000-10,000 upstream entities. Annual supplier audits cover a sample. The result is incomplete visibility and a compliance posture built on hope.

## With Uni

The notebook loads component-to-supplier edges across multiple tiers and defines a recursive upstream traversal rule. For any finished product or sub-assembly, Uni traces the complete supplier lineage — every entity that contributed material, no matter how many tiers removed (the seed data spans three tiers, from finished part through sub-assembly to raw material). Each part's kind (finished, sub-assembly, or raw) is visible at every tier of the trace. The logic is 2 declarative rules — a base case plus one recursive case — not a custom graph database deployment.

## What You'll See

- Complete provenance chain for any component, from finished product back to raw material suppliers
- Recursive multi-tier traversal that surfaces every upstream entity in a single declarative query
- Each upstream part's kind (finished, sub-assembly, raw) visible at every tier of the trace

## Why It Matters

A single UFLPA withhold-release order can block $10M+ in inventory at the port. Knowing your full supplier lineage before the audit letter arrives turns a 6-week fire drill into a same-day response.

---

[Run the notebook →](locy_supply_chain_provenance.md)
