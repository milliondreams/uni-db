# Supply Chain BOM Explosion, Cost Rollup, and Supplier Risk

**Industry**: Manufacturing / Aerospace / Automotive | **Role**: VP Supply Chain, Director of Procurement | **Time to value**: 3-5 hours

## The Problem

A finished product contains hundreds of subassemblies, each with its own bill of materials. Rolling up total landed cost across a 6-level BOM is error-prone. Identifying which suppliers represent single points of failure requires cross-referencing supplier records against every level of the hierarchy. When a supplier goes down, the question "what finished goods are affected?" takes days to answer.

## The Traditional Approach

Teams export BOM data from the ERP system into spreadsheets, then write custom scripts -- typically 1,500-3,000 lines of SQL and Python -- to recursively explode assemblies and aggregate costs. Supplier risk analysis lives in a separate tool or a second set of spreadsheets maintained by procurement. Reconciling the two requires manual joins and is usually done quarterly. A single BOM change means re-running the entire pipeline.

## With Uni

Recursive graph queries traverse the full assembly hierarchy in one pass, from finished goods down to raw materials. Cost rollup aggregates unit costs, quantities, and supplier pricing at every level automatically. Single-source risk detection identifies components where only one supplier exists, linked directly to the affected finished goods. All three analyses -- BOM explosion, cost rollup, and risk identification -- run from the same data and the same query layer.

## What You'll See

- Complete multi-level BOM tree with per-level and total rolled-up costs
- Total landed cost calculation that accounts for quantity multipliers across assembly levels
- Identification of risk-exposed suppliers: which components depend on a single source, and which finished goods they affect

## Why It Matters

Quarterly BOM reconciliation becomes on-demand. When a supplier disruption hits, the impact assessment that used to take a procurement team 2-3 days is available in seconds -- with the full chain of evidence from raw material to finished good.

---

[Run the notebook &rarr;](supply_chain.md)
