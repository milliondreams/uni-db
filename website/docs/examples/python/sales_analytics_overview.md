# Multi-Dimensional Sales Analytics: Region, Category, and Order Intelligence

**Industry**: Retail / E-Commerce / CPG | **Role**: VP Sales, Director of Revenue Operations | **Time to value**: 1-3 hours

## The Problem

Sales leadership needs to slice revenue by region, rank product categories, and identify top orders -- often in the same meeting. The data exists in the warehouse, but getting a coherent cross-dimensional view requires multiple queries, manual pivot tables, and a BI analyst who knows where the joins are. Ad hoc questions ("Which region has the highest average order value for electronics?") take hours, not minutes.

## The Traditional Approach

A data warehouse stores transactional data across normalized tables. A BI tool (Tableau, Looker, Power BI) provides pre-built dashboards, but any question outside the existing views requires a new SQL query or dashboard modification -- typically a 1-2 day turnaround from the analytics team. Pivot tables in spreadsheets fill the gap for urgent requests, but they break on large datasets and lack reproducibility. The underlying SQL for multi-dimensional aggregation across regions and categories often runs to 200-400 lines with nested CTEs.

## With Uni

Graph traversal connects orders to customers to regions, while columnar aggregation computes revenue totals, category rankings, and order-level metrics in one query layer. New dimensions are added by extending the graph, not by rewriting SQL. A product manager can read the query logic -- it describes the business question, not the join mechanics. Results come back structured for direct use: sorted, ranked, and filtered.

## What You'll See

- Regional revenue breakdowns with totals, averages, and order counts per geography
- Category rankings showing top-performing product lines by revenue and volume
- Top order identification with full context: customer, region, product mix, and total value

## Why It Matters

The analytics team spends an estimated 30-40% of their time writing and maintaining ad hoc SQL. A declarative query layer lets business users self-serve the 80% of questions that are structural, freeing the analytics team for work that actually requires their expertise.

---

[Run the notebook &rarr;](sales_analytics.md)
