# Semiconductor Yield Excursion Root Cause Analysis

**Industry**: Semiconductor Manufacturing | **Role**: VP Yield Engineering, Fab Director | **Time to value**: 4 hours

## The Problem

A batch of wafers fails final test. Somewhere in a 400-step process, one or more tools introduced a defect. The fab has hundreds of tools, each processing thousands of lots per month. Identifying the responsible tool — and deciding what to quarantine without shutting down the line — is the most expensive diagnostic problem in semiconductor manufacturing.

## The Traditional Approach

Yield engineers manually correlate sensor readings from SECOM-style manufacturing data against fail/pass outcomes. They pull lot histories from MES, cross-reference tool assignments, and build pivot tables. A typical excursion investigation involves 50-100 process parameters across 1,500+ lots. Identifying the root cause takes 1-3 weeks. During that time, the suspect tool continues processing wafers, compounding the loss. Containment decisions — which tools to take offline — are made by experience and intuition, not quantified trade-offs.

## With Uni

The notebook loads real SECOM manufacturing data (1,567 lots, 590 sensor features) and defines 15 declarative rules that cover the full diagnostic workflow. Fail-lot to tool mapping identifies which tools processed failing wafers. Hotspot ranking scores tools by their disproportionate association with failures. Containment simulation answers "what if we quarantine Tool X?" by computing the projected yield impact and capacity cost. Minimal-change search finds the cheapest set of tools to quarantine that brings yield above target. Every conclusion — every hotspot ranking, every containment recommendation — includes a full derivation trace showing exactly which lots and sensor readings support it.

## What You'll See

- Ranked tool hotspots with statistical support, not just correlation but traceable evidence chains
- Containment scenario costs: projected yield improvement vs. capacity loss for each quarantine option
- Minimal quarantine recommendations — the smallest set of tool holds that achieves the yield target
- Full derivation evidence for each conclusion, from raw sensor data through to the recommendation

## Why It Matters

A single yield excursion at a modern fab can cost $2-5M in scrapped wafers per week. Reducing time-to-root-cause from weeks to hours and replacing intuition-based containment with cost-optimized quarantine decisions directly protects margin.

---

[Run the notebook →](locy_semiconductor_yield_excursion.md)
