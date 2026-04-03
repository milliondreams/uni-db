# Knowledge-Grounded Retrieval: Beyond Vector-Only RAG

**Industry**: Legal / Healthcare / Financial Services | **Role**: Head of AI, Director of Knowledge Management | **Time to value**: 2-4 hours

## The Problem

Vector-only retrieval-augmented generation finds passages that sound relevant but misses the relationships between them. A question like "What are the side effects of Drug X when combined with Drug Y?" requires connecting information across multiple documents -- something embedding similarity alone cannot do. Keyword search catches exact terms but misses semantic equivalents. Neither approach bridges entities across documents.

## The Traditional Approach

Teams build a vector store over chunked documents (typically using an embedding model and a service like Pinecone or Weaviate), then layer keyword filters on top. When relationships matter, engineers manually construct knowledge graphs -- a 3-6 month effort involving entity extraction pipelines, schema design, and a separate graph database. The vector store and the graph rarely share an index, so queries hit two systems and results are merged in application code. Maintaining both is a standing cost of 1-2 engineers.

## With Uni

A single query layer combines vector similarity search with graph traversal over extracted entities. Documents are chunked and embedded as usual, but entities and their relationships are also indexed in the same system. At query time, vector search finds the most semantically relevant passages, while graph traversal follows entity links to pull in related context from other documents. The retrieval result includes both passage-level relevance and entity-level connections, giving the downstream LLM richer, more accurate context.

## What You'll See

- Contextually enriched retrieval that surfaces not just similar passages but related entities and their connections
- Entity-bridged answers where information from Document A is connected to Document B through shared entities
- Measurably higher relevance compared to vector-only baselines, particularly for multi-hop questions

## Why It Matters

Vector-only RAG is a solved problem; the unsolved problem is relationship-aware retrieval. This approach closes that gap without requiring a separate knowledge graph project, reducing what is typically a multi-quarter initiative to a single integration.

---

[Run the notebook &rarr;](rag.md)
