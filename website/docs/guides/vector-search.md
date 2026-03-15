# Vector Search Guide

Uni treats vector search as a first-class citizen, deeply integrated with the graph traversal engine. This guide covers schema design, index configuration, query patterns, and performance optimization for semantic similarity search.

## Overview

Vector search enables finding similar items based on high-dimensional embeddings:

```
Query: "papers about attention mechanisms"
         │
         ▼
    ┌───────────────────┐
    │  Embed Query      │
    │  → [0.12, -0.34,  │
    │     0.56, ...]    │
    └─────────┬─────────┘
              │
              ▼
    ┌───────────────────┐
    │  Vector Index     │
    │  (HNSW / IVF_PQ)  │
    └─────────┬─────────┘
              │
              ▼
    ┌───────────────────┐
    │  Top-K Results    │
    │  - Attention...   │
    │  - Transformer... │
    │  - BERT...        │
    └───────────────────┘
```

---

## Setting Up Vector Search

### Step 1: Define Vector Schema

Add a `Vector` type property to your schema:

```json
{
  "properties": {
    "Paper": {
      "title": { "type": "String", "nullable": false },
      "abstract": { "type": "String", "nullable": true },
      "embedding": {
        "type": "Vector",
        "dimensions": 768
      }
    },
    "Document": {
      "title": { "type": "String", "nullable": false },
      "content": { "type": "String", "nullable": true },
      "embedding": {
        "type": "Vector",
        "dimensions": 384
      }
    },
    "Product": {
      "name": { "type": "String", "nullable": false },
      "description": { "type": "String", "nullable": true },
      "desc_embedding": {
        "type": "Vector",
        "dimensions": 384
      },
      "image_embedding": {
        "type": "Vector",
        "dimensions": 512
      }
    }
  }
}
```

**Dimension Guidelines:**

| Model | Dimensions | Use Case |
|-------|------------|----------|
| all-MiniLM-L6-v2 | 384 | General text, fast |
| BGE-base-en-v1.5 | 768 | High quality text |
| OpenAI text-embedding-3-small | 1536 | Commercial, high quality |
| CLIP ViT-B/32 | 512 | Image + text |

### Step 2: Create Vector Index

Create an index for efficient similarity search:

**HNSW (Recommended for most cases):**

```cypher
CREATE VECTOR INDEX paper_embeddings
FOR (p:Paper)
ON p.embedding
OPTIONS {
  type: "hnsw"
}
```

**IVF_PQ (For memory-constrained environments):**

```cypher
CREATE VECTOR INDEX paper_embeddings
FOR (p:Paper)
ON p.embedding
OPTIONS {
  type: "ivf_pq"
}
```

### Step 3: Import Data with Embeddings

Your import data should include embedding vectors:

```json
{"id": "paper_001", "title": "Attention Is All You Need", "embedding": [0.12, -0.34, 0.56, ...]}
{"id": "paper_002", "title": "BERT: Pre-training of Deep Bidirectional Transformers", "embedding": [0.08, -0.21, 0.42, ...]}
```

---

## Preparing Uni-Xervo for Auto Embedding

Auto-embedding requires a Uni-Xervo catalog with an alias that matches the vector index configuration. Define that alias when you open the database (_e.g._, via `Uni::temporary().xervo_catalog(vec![ModelAliasSpec { alias: "embed/default", task: ModelTask::Embed, ... }])` in Rust or the equivalent JSON catalog in Cypher). Every vector index that sets `embedding.alias` must point to one of these catalog entries; when Uni writes nodes, the writer calls the alias, batches text inputs, and stores the returned embeddings in the indexed property.

### Using the Uni-Xervo Runtime Directly

`Uni::xervo()` exposes the underlying runtime so you can generate ad-hoc embeddings, run text generation, or pre-warm models from your application:

```rust
let xervo = db.xervo()?;

// Embed text directly
let vectors = xervo.embed("embed/default", &["some query text"]).await?;

// Text generation with structured messages (uni-xervo 0.2.0+)
use uni_db::xervo::{Message, GenerationOptions};
let result = xervo.generate("llm/default", &[
    Message::system("You are a helpful assistant."),
    Message::user("Summarize this document."),
], GenerationOptions::default()).await?;
println!("{}", result.text);

// Convenience: generate from plain strings (each treated as a user message)
let result = xervo.generate_text(
    "llm/default",
    &["Summarize this document."],
    GenerationOptions::default(),
).await?;
```

The `generate` method accepts structured `Message` objects with explicit roles (`System`, `User`, `Assistant`) and supports multimodal content blocks (text and images). The `generate_text` convenience method wraps plain strings as user messages for simpler use cases.

## Querying Vectors

### Basic KNN Search

Find the K nearest neighbors to a query vector:

```cypher
CALL uni.vector.query('Paper', 'embedding', $query_vector, 10)
YIELD node, score
RETURN node.title, score
ORDER BY score DESC
```

**Parameters:**
- `'Paper'`: Label to search
- `'embedding'`: Vector property name
- `$query_vector`: Query vector (list of floats) OR text string for auto-embedding
- `10`: Number of results (K)
- (optional) `filter`: Pre-filter clause
- (optional) `threshold`: Minimum score

**Yields:**
- `node`: Full node object with all properties
- `vid`: Vertex ID (for efficient joins)
- `score`: Normalized similarity score (higher is better, range 0-1)

### Auto-Embed Text Queries

When your vector index has an embedding configuration, you can pass text directly:

```cypher
-- The index auto-embeds the text query
CALL uni.vector.query('Paper', 'embedding', 'attention mechanisms in transformers', 10)
YIELD node, score
RETURN node.title, score
ORDER BY score DESC
```

This requires an embedding configuration on the index:

```cypher
CREATE VECTOR INDEX paper_embed FOR (p:Paper) ON (p.embedding)
OPTIONS {
    metric: 'cosine',
    embedding: {
        alias: 'embed/default',
        source: ['abstract'],
        batch_size: 32
    }
}
```

### Operator Form (`~=`) with Scores

You can also use the `~=` operator to run a vector search and get a similarity score:

```cypher
MATCH (p:Paper)
WHERE p.embedding ~= $query_vector
RETURN p.title, p._score AS score
ORDER BY score DESC
LIMIT 10
```

### With Distance Threshold

Filter results by maximum distance:

```cypher
CALL uni.vector.query('Paper', 'embedding', $query_vector, 100, NULL, 0.3)
YIELD node, distance
RETURN node.title, distance
ORDER BY distance
LIMIT 10
```

The threshold parameter (6th argument) filters results to only those with a similarity score ≥ 0.3 (higher = more similar).

### Hybrid Search: Pre-Filtering

Pre-filter at the vector index level for efficient hybrid search:

```cypher
// Filter BEFORE vector search (efficient!)
CALL uni.vector.query(
  'Paper',
  'embedding',
  $query_vector,
  10,
  'year >= 2020 AND venue IN (''NeurIPS'', ''ICML'')'  // Lance/DataFusion filter
)
YIELD node, distance, score
RETURN node.title, node.year, distance, score
ORDER BY distance
```

**Pre-filtering** searches only within the filtered subset, unlike post-filtering which searches all nodes then filters.

### Post-Filtering (Alternative)

Combine vector search with property filtering after search:

```cypher
CALL uni.vector.query('Paper', 'embedding', $query_vector, 50)
YIELD node AS paper, distance
WHERE paper.year >= 2020 AND paper.venue IN ['NeurIPS', 'ICML']
RETURN paper.title, paper.year, distance
ORDER BY distance
LIMIT 10
```

**Note:** Pre-filtering (above) is more efficient when the filter is selective.

### Filter + Threshold Together

Combine both for maximum control:

```cypher
CALL uni.vector.query(
  'Product',
  'embedding',
  $query_vector,
  100,
  'category = ''electronics'' AND price < 1000',  // Pre-filter
  0.5  // Similarity threshold
)
YIELD node, distance, score
RETURN node.name, node.price, distance, score
ORDER BY score DESC  // Use normalized score for ranking
LIMIT 10
```

---

## Hybrid Graph + Vector Queries

The real power comes from combining graph traversal with vector search.

### Pattern 1: Vector Search → Graph Expansion

Find similar papers, then explore their citations:

```cypher
// Find papers similar to query
CALL uni.vector.query('Paper', 'embedding', $query_vector, 10)
YIELD node AS seed, distance

// Expand to citations
MATCH (seed)-[:CITES]->(cited:Paper)
RETURN seed.title AS source, cited.title AS cited_paper, distance
ORDER BY distance, cited.year DESC
```

### Pattern 2: Graph Context → Vector Search

Start from a known node, find similar neighbors:

```cypher
// Start from a specific paper
MATCH (seed:Paper {title: 'Attention Is All You Need'})

// Get its embedding
WITH seed, seed.embedding AS seed_embedding

// Find papers cited by seed that are similar to seed
MATCH (seed)-[:CITES]->(cited:Paper)
WHERE similar_to(seed_embedding, cited.embedding) > 0.8
RETURN cited.title, cited.year
```

### Pattern 3: Multi-Hop with Similarity Filter

Find papers in citation chain with semantic similarity:

```cypher
MATCH (start:Paper {title: 'Attention Is All You Need'})
MATCH (start)-[:CITES]->(hop1:Paper)-[:CITES]->(hop2:Paper)
WHERE similar_to(start.embedding, hop2.embedding) > 0.7
RETURN DISTINCT hop2.title, hop2.year
ORDER BY hop2.year DESC
LIMIT 20
```

### Pattern 4: Author's Similar Papers

Find an author's papers similar to a query:

```cypher
// Vector search for similar papers
CALL uni.vector.query('Paper', 'embedding', $query_vector, 100)
YIELD node AS paper, distance

// Filter to specific author
MATCH (paper)-[:AUTHORED_BY]->(a:Author {name: 'Geoffrey Hinton'})
RETURN paper.title, paper.year, distance
ORDER BY distance
LIMIT 10
```

---

## `similar_to` Expression Function

`similar_to()` is a unified similarity scoring function that works as an expression — in `WHERE`, `RETURN`, `WITH`, `ORDER BY`, and Locy rule bodies. Unlike `CALL` procedures, it scores one already-bound node against a query (point computation, not top-K scan).

```
similar_to(sources, queries [, options]) → FLOAT [0, 1]
```

### Single Vector Source

Score a bound node's embedding against a pre-computed vector or text query:

```cypher
// Pre-computed vector query
MATCH (p:Paper)-[:CITES]->(cited:Paper)
WHERE similar_to(cited.embedding, $query_vector) > 0.8
RETURN cited.title, similar_to(cited.embedding, $query_vector) AS score

// Auto-embed text query (uses the index's embedding model)
MATCH (p:Paper)
WHERE similar_to(p.embedding, 'attention mechanisms in transformers') > 0.6
RETURN p.title
```

### Single FTS Source

Score a string property with a full-text index using BM25:

```cypher
MATCH (d:Document)
RETURN d.title, similar_to(d.content, 'graph database optimization') AS relevance
ORDER BY relevance DESC
```

BM25 scores are normalized to `[0, 1]` using a saturation function: `score / (score + fts_k)` where `fts_k` defaults to `1.0`.

### Multi-Source Hybrid

Combine vector and FTS scoring in a single expression:

```cypher
// Broadcast: same query applied to both sources
MATCH (d:Document)
RETURN d.title,
  similar_to([d.embedding, d.content], 'machine learning') AS relevance
ORDER BY relevance DESC

// Per-source queries: different query per source
MATCH (p:Product)
RETURN p.name, similar_to(
  [p.image_embedding, p.desc_embedding, p.description],
  [$photo_vec, 'red sneakers', 'affordable running shoes']
) AS relevance
```

### Options

The optional third argument controls fusion behavior:

| Key | Values | Description |
|-----|--------|-------------|
| `method` | `'rrf'` (default), `'weighted'` | Fusion algorithm for multi-source |
| `weights` | List of floats | Per-source weights for weighted fusion (must sum to 1.0) |
| `k` | Integer (default: 60) | RRF constant |
| `fts_k` | Float (default: 1.0) | BM25 saturation constant |

```cypher
// Weighted fusion: favor vector similarity 70/30
MATCH (d:Document)
RETURN d.title, similar_to([d.embedding, d.content], 'query',
  {method: 'weighted', weights: [0.7, 0.3]}) AS score
ORDER BY score DESC
```

### Procedures vs `similar_to`

| | `CALL uni.search(...)` | `similar_to()` |
|---|---|---|
| **Operation** | Scan index, return top-K | Score one bound node |
| **Use in WHERE** | No | Yes |
| **Use in Locy rules** | No | Yes |
| **Best for** | "Find top 10 from millions" | "Score this matched node" |

Both are needed. Use `CALL` procedures to find candidates from a full label, then `similar_to` to score or filter nodes already bound by `MATCH`.

### Execution Paths and Locy

`similar_to()` runs through different execution engines depending on context:

| Context | Engine | Vector | Auto-Embed | FTS | Multi-Source |
|---------|--------|--------|------------|-----|-------------|
| Cypher `MATCH ... WHERE/RETURN` | DataFusion | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| Locy rule `WHERE / YIELD / ALONG / FOLD` | DataFusion | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| Locy command `DERIVE / ABDUCE / ASSUME WHERE` | In-memory | :white_check_mark: | :x: | :x: | :x: |

In Cypher queries and Locy **rule bodies**, `similar_to()` runs inside DataFusion with full access to storage, schema, and embedding models. All scoring modes work.

In Locy **command** WHERE clauses (`DERIVE ... WHERE`, `ABDUCE ... WHERE`), `similar_to()` falls back to a pure vector cosine computation — no auto-embedding or FTS. This is because commands execute after strata converge on already-materialized row data. In practice this is rarely limiting: rule WHERE clauses (which have full capability) handle the semantic filtering, while command WHERE clauses typically apply simple scalar filters on already-derived columns.

---

## Generating Embeddings

### Auto-Embedding via Index Options

Uni can auto-generate embeddings on insert when you configure an embedding alias in the index options:

```cypher
CREATE VECTOR INDEX doc_embed_idx
FOR (d:Document) ON d.embedding
OPTIONS {
  type: "hnsw",
  embedding: {
    alias: "embed/default",
    source: ["content"]
  }
}
```

The `alias` field references a model alias from your Uni-Xervo catalog configuration.

**Supported Providers:**

| Provider | Feature flag | Type | Description |
|----------|-------------|------|-------------|
| `MistralRS` | `provider-mistralrs` | Local | CPU-friendly local inference via MistralRS loader. Opt-in; ideal for offline deployments without API keys. |
| `Candle` | `provider-candle` | Local | Native HuggingFace Candle models (optional, pulls in `tokenizers` + `candle` crates). |
| `FastEmbed` | `provider-fastembed` | Local | ONNX runtime provider for legacy models (optional). |
| `OpenAI` | `provider-openai` | Remote | OpenAI embedding and generation APIs (configure via `OPENAI_API_KEY`). |
| `Gemini` | `provider-gemini` | Remote | Google Gemini API (requires network access and credentials). |
| `Anthropic` | `provider-anthropic` | Remote | Anthropic Claude API for generation tasks. |
| `Vertex AI` | `provider-vertexai` | Remote | Google Cloud Vertex AI API. |
| `Mistral` | `provider-mistral` | Remote | Mistral AI hosted API. |
| `Cohere` | `provider-cohere` | Remote | Cohere embedding and generation APIs. |
| `Voyage AI` | `provider-voyageai` | Remote | Voyage AI embedding API. |
| `Azure OpenAI` | `provider-azure-openai` | Remote | Azure-hosted OpenAI API. |

Keep the feature list tight—only enable the providers your deployment actually needs. The workspace defaults include `provider-gemini` and `provider-openai`; all other providers (including `provider-mistralrs`) are opt-in.

**Embedding Model Recommendation:**
For local CPU auto-embedding, point your catalog alias at a lightweight embedding model such as `nomic-embed-text-v1.5`. It is already supported by `provider-mistralrs` via the MistralRS loader, runs well on an 8‑core laptop, and provides high-quality vectors for RAG tasks. That keeps dependencies small, avoids downloading large transformer checkpoints, and means your users can embed text entirely offline while still benefiting from Uni-Xervo’s batching.

### Using External APIs

For production, you might use external embedding APIs:

```python
import openai
import json

# Generate embeddings
def embed_text(text):
    response = openai.Embedding.create(
        input=text,
        model="text-embedding-3-small"
    )
    return response['data'][0]['embedding']

# Prepare JSONL with embeddings
papers = [
    {"id": "p1", "title": "Paper 1", "embedding": embed_text("Paper 1 abstract")},
    {"id": "p2", "title": "Paper 2", "embedding": embed_text("Paper 2 abstract")},
]

with open("papers.jsonl", "w") as f:
    for paper in papers:
        f.write(json.dumps(paper) + "\n")
```

### Understanding Yields

The `uni.vector.query` procedure returns multiple values:

```cypher
CALL uni.vector.query('Product', 'embedding', $vec, 10)
YIELD node, vid, distance, score
RETURN node.name, vid, distance, score
```

| Yield | Type | Description | Use When |
|-------|------|-------------|----------|
| `node` | Object | Full node with all properties | Need immediate property access |
| `vid` | Integer | Vertex ID for efficient joins | Joining with other queries |
| `distance` | Float | Raw distance (lower = better) | Need exact distance values |
| `score` | Float | Normalized similarity 0-1 (higher = better) | Ranking by similarity |

**Performance tip:** Use `YIELD vid` when you only need IDs - it's much faster than `YIELD node` for large result sets since it skips property loading.

```cypher
// Fast: Only loads IDs
CALL uni.vector.query('Product', 'embedding', $vec, 1000)
YIELD vid, distance
WHERE distance < 0.5
RETURN vid

// Slower: Loads all properties for 1000 nodes
CALL uni.vector.query('Product', 'embedding', $vec, 1000)
YIELD node, distance
WHERE distance < 0.5
RETURN node
```

---

## Distance Metrics

### Cosine Similarity

Best for normalized embeddings (most text models):

```
similarity = A · B / (||A|| × ||B||)
distance = 1 - similarity
```

- Range: 0 (identical) to 2 (opposite)
- Use when: Magnitude doesn't matter, only direction

### L2 (Euclidean) Distance

Best for embeddings where magnitude matters:

```
distance = √Σ(aᵢ - bᵢ)²
```

- Range: 0 (identical) to ∞
- Use when: Absolute position in space matters

### Dot Product

Best for unnormalized embeddings:

```
similarity = A · B
distance = -similarity (negated for ranking)
```

- Range: -∞ to +∞
- Use when: Embeddings have meaningful magnitudes

### Score Conversion

`uni.vector.query`, `uni.search`, and `similar_to()` all return normalized **similarity scores** (higher = more similar), not raw distances. The conversion is metric-aware:

| Metric | Raw Distance Range | Conversion Formula | Score Range |
|---|---|---|---|
| Cosine | [0, 2] | `(2 - d) / 2` | [0, 1] |
| L2 | [0, ∞) | `1 / (1 + d)` | (0, 1] |
| Dot | (-∞, +∞) | Pass-through | Unbounded |

This means you can compare scores across queries without worrying about which distance metric the index uses.

---

## Index Tuning

DDL currently supports selecting the index type (`hnsw`, `flat`, `ivf_pq`) but uses default parameters. To tune HNSW or IVF_PQ parameters, use the Rust schema builder:

```rust
use uni_db::{DataType, IndexType, VectorAlgo, VectorIndexCfg, VectorMetric};

db.schema()
    .label("Paper")
        .property("embedding", DataType::Vector { dimensions: 768 })
        .index("embedding", IndexType::Vector(VectorIndexCfg {
            algorithm: VectorAlgo::Hnsw { m: 32, ef_construction: 200 },
            metric: VectorMetric::Cosine,
        }))
    .apply()
    .await?;
```

---

## Performance Optimization

### Pre-filtering Strategy

For hybrid queries, choose the right filtering strategy:

```cypher
// ✅ BEST: Pre-filter at index level (most efficient)
CALL uni.vector.query(
  'Paper',
  'embedding',
  $query_vector,
  10,
  'year >= 2020 AND venue = "NeurIPS"'  // Filter pushed to LanceDB
)
YIELD node AS paper, distance
RETURN paper.title, distance
ORDER BY distance

// ✅ GOOD: Vector search first, then post-filter
CALL uni.vector.query('Paper', 'embedding', $query_vector, 100)
YIELD node AS paper, distance
WHERE paper.year >= 2020  // Filter after vector search
RETURN paper.title, distance
ORDER BY distance
LIMIT 10

// ⚠️ OK: Over-fetch for selective filters (less efficient)
CALL uni.vector.query('Paper', 'embedding', $query_vector, 500)
YIELD node AS paper, distance
WHERE paper.year >= 2020 AND paper.venue = 'NeurIPS'
RETURN paper.title, distance
ORDER BY distance
LIMIT 10
```

**When to use pre-filtering:**
- Filter is selective (reduces search space significantly)
- You need fewer results than the filtered set size
- The filter column is indexed in LanceDB

**When to use post-filtering:**
- Filter is not very selective
- You need many results
- Complex Cypher expressions not expressible in SQL

### Batch Queries

For multiple queries, batch them:

```rust
// Process multiple query vectors efficiently
let queries = vec![query1, query2, query3];
let results = storage.batch_vector_search(
    "Paper",
    "embedding",
    &queries,
    10  // k per query
).await?;
```

### Caching Query Vectors

Pre-compute and cache frequent query embeddings:

```cypher
// Store computed query embedding
CREATE (q:Query {
  text: 'transformer architectures',
  embedding: $precomputed_embedding,
  created_at: datetime()
})

// Reuse later
MATCH (q:Query {text: 'transformer architectures'})
CALL uni.vector.query('Paper', 'embedding', q.embedding, 10)
YIELD node, distance
RETURN node.title, distance
```

---

## Use Cases

### Semantic Document Search

```cypher
// Find documents similar to a natural language query
WITH $query_embedding AS query_vec
CALL uni.vector.query('Document', 'content_embedding', query_vec, 20)
YIELD node AS doc, distance
RETURN doc.title, doc.summary, distance
ORDER BY distance
LIMIT 10
```

### Recommendation System

```cypher
// Find products similar to what user viewed
MATCH (u:User {id: $user_id})-[:VIEWED]->(viewed:Product)
WITH COLLECT(viewed.embedding) AS viewed_embeddings

// Average the embeddings (simplified)
WITH reduce(sum = [0.0]*384, e IN viewed_embeddings |
  [i IN range(0, 383) | sum[i] + e[i]]) AS summed,
  size(viewed_embeddings) AS count
WITH [x IN summed | x / count] AS avg_embedding

CALL uni.vector.query('Product', 'embedding', avg_embedding, 20)
YIELD node AS product, distance
WHERE NOT EXISTS((u)-[:VIEWED]->(product))  // Exclude already viewed
RETURN product.name, product.price, distance
LIMIT 10
```

### Duplicate Detection

```cypher
// Find near-duplicate documents
MATCH (d:Document)
CALL uni.vector.query('Document', 'embedding', d.embedding, 5)
YIELD node AS similar, distance
WHERE similar.id <> d.id AND distance < 0.1  // Very similar
RETURN d.title, similar.title, distance
```

### Clustering via Vector Search

```cypher
// Find clusters of similar papers
MATCH (seed:Paper)
WHERE seed.citations > 100  // Start from influential papers
CALL uni.vector.query('Paper', 'embedding', seed.embedding, 20)
YIELD node AS similar, distance
WHERE distance < 0.3
RETURN seed.title AS cluster_center, COLLECT(similar.title) AS cluster_members
```

---

## Troubleshooting

### Low Recall

**Symptoms:** Missing expected results

**Solutions:**
1. Increase `k` and post-filter
2. Use HNSW (higher recall) instead of IVF_PQ
3. Check embedding model consistency (same model for indexing and querying)
4. Verify dimensions match the schema
5. (Rust) Increase HNSW `m` / `ef_construction` or IVF_PQ `partitions` / `sub_vectors`

### Slow Queries

**Symptoms:** High latency on vector search

**Solutions:**
1. Reduce `k` or add a distance `threshold`
2. Use IVF_PQ instead of HNSW for large datasets
3. Pre-filter with `uni.vector.query(..., filter)` when possible
4. Ensure a vector index exists (`SHOW INDEXES`)

### Memory Issues

**Symptoms:** OOM during indexing or queries

**Solutions:**
1. Switch to IVF_PQ (compressed vectors)
2. (Rust) Reduce HNSW `m` / `ef_construction`
3. (Rust) Reduce IVF_PQ `partitions` / `sub_vectors`
4. Consider smaller embeddings or fewer indexed labels

---

## Hybrid Search

For queries that benefit from both semantic similarity and keyword matching, use `uni.search`:

```cypher
CALL uni.search(
    'Paper',
    {vector: 'embedding', fts: 'abstract'},
    'transformer attention mechanisms',
    null,  -- auto-embed the text
    10
)
YIELD node, score, vector_score, fts_score
RETURN node.title, score, vector_score, fts_score
```

Hybrid search combines vector and full-text results using Reciprocal Rank Fusion (RRF) or weighted fusion. See the [Hybrid Search feature page](../features/hybrid-search.md) for details.

### Full-Text Search Procedure

For keyword-only search with BM25 scoring:

```cypher
CALL uni.fts.query('Paper', 'abstract', 'neural networks', 20)
YIELD node, score
RETURN node.title, score
ORDER BY score DESC
```

---

## Next Steps

- [Indexing](../concepts/indexing.md) — All index types and configuration
- [Hybrid Search](../features/hybrid-search.md) — Combined vector + FTS search
- [Performance Tuning](performance-tuning.md) — Optimization strategies
- [Data Ingestion](data-ingestion.md) — Import data with embeddings
