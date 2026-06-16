# Hybrid Book Recommendations: Purchase History Meets Semantic Understanding

**Industry**: Retail / Media | **Role**: Head of Personalization, VP Product | **Time to value**: 2-4 hours

## The Problem

Recommendation engines either know what customers bought or understand what products mean -- rarely both. Collaborative filtering surfaces popular items but misses niche discoveries. Embedding-based systems find semantically similar products but ignore actual purchase behavior. The result: recommendations that feel either obvious or irrelevant.

## The Traditional Approach

Most teams run a collaborative filtering service (typically 2,000-5,000 lines of Python) alongside a separate embedding pipeline for content similarity. A feature engineering layer stitches the two signals together, requiring manual weight tuning and A/B testing infrastructure. Updating the model means retraining both pipelines, redeploying the merger logic, and waiting 24-48 hours for new signals to propagate. Three teams maintain three systems.

## With Uni

A single query combines graph traversal over purchase history with vector search over book descriptions. Purchase patterns surface "customers who bought X also bought Y" connections, while semantic similarity finds thematically related titles that no customer has paired yet. The result set is ranked in one pass -- by co-purchase count for collaborative queries and by vector distance for semantic queries -- with no external stitching layer and no batch retraining. Queries are declarative: you state what a good recommendation looks like, not how to compute one.

## What You'll See

- Hybrid recommendations that blend behavioral co-purchase signals with semantic relevance
- Discovery of semantically related titles (e.g., other technical books surfaced by embedding proximity), then linked to the customers who actually bought them
- Real-time scoring that reflects the latest purchase data without pipeline delays

## Why It Matters

Teams typically spend 3-6 months building and integrating separate recommendation subsystems. This approach collapses that into a single declarative layer, cutting integration cost and making the recommendation logic auditable by a product manager, not just an ML engineer.

---

[Run the notebook &rarr;](recommendation.md)
