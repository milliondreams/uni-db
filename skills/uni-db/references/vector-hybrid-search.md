# Vector, Hybrid & Full-Text Search Reference

Quick reference for Uni DB's vector similarity, full-text, and hybrid search capabilities.

---

## 1. Vector Search -- `uni.vector.query`

```cypher
CALL uni.vector.query(label, property, query_vector, k [, filter] [, threshold])
YIELD node, score, distance, vector_score, vid
```

| Parameter | Type | Required | Description |
|---|---|---|---|
| `label` | String | Yes | Vertex label to search |
| `property` | String | Yes | Vector property name |
| `query_vector` | List\<Float\> or String | Yes | Embedding vector, or text string (auto-embedded when index has embedding config) |
| `k` | Integer | Yes | Number of results |
| `filter` | String | No | Lance/DataFusion WHERE predicate for **pre-filtering** |
| `threshold` | Float | No | Minimum similarity score (0-1); results below are excluded |

| YIELD Column | Type | Description |
|---|---|---|
| `node` | Object | Full node with all properties (slower for large k) |
| `vid` | Integer | Vertex ID for efficient joins (faster than `node`) |
| `score` | Float | Normalized similarity 0-1 (higher = more similar) |
| `distance` | Float | Raw distance (lower = closer; metric-dependent) |
| `vector_score` | Float | Same as `score` (for parity with `uni.search`) |

**Basic search:**
```cypher
CALL uni.vector.query('Document', 'embedding', $query_vector, 10)
YIELD node, score
RETURN node.title, score
ORDER BY score DESC
```

**With pre-filter and threshold:**
```cypher
CALL uni.vector.query('Document', 'embedding', $query_vector, 20,
    'category = "tech" AND year >= 2023', 0.5)
YIELD node, score
RETURN node.title, score
```

**Auto-embed text query (requires embedding config on index):**
```cypher
CALL uni.vector.query('Document', 'embedding', 'graph databases for beginners', 5)
YIELD node, score
RETURN node.title, score
```

---

## 2. `similar_to()` Expression Function

```cypher
similar_to(source, query [, options]) -> Float
```

A per-row expression for `WHERE`, `RETURN`, `ORDER BY`, and Locy rule bodies. Scores one already-bound node (not a top-K scan like `CALL` procedures).

### Scoring Modes (auto-detected)

| Source Type | Query Type | Mode | Behavior |
|---|---|---|---|
| Vector property | Vector literal | **Vector** | Metric-aware similarity per row |
| Vector property (with embedding config) | String literal | **AutoEmbed** | Embeds query once, then vector similarity per row |
| String property (with FTS index) | String literal | **FTS** | BM25 score normalized via `score / (score + fts_k)` |

Metric is resolved from the vector index at compile time. Defaults to cosine if no index found.

### Single-Source Examples

```cypher
-- Vector-to-vector
MATCH (d:Doc)
RETURN d.title, similar_to(d.embedding, $query_vector) AS score

-- Auto-embed text
MATCH (d:Doc)
WHERE similar_to(d.embedding, 'attention mechanisms') > 0.6
RETURN d.title

-- FTS (BM25)
MATCH (d:Doc)
RETURN d.title, similar_to(d.content, 'distributed systems') AS score
```

### Multi-Source Fusion

```cypher
-- Broadcast: same query applied to vector + FTS sources
MATCH (d:Doc)
RETURN d.title,
  similar_to([d.embedding, d.content], 'machine learning') AS score

-- Per-source queries with weighted fusion
MATCH (p:Product)
RETURN p.name, similar_to(
  [p.image_embedding, p.desc_embedding, p.description],
  [$photo_vec, 'red sneakers', 'affordable running shoes'],
  {method: 'weighted', weights: [0.4, 0.3, 0.3]}
) AS score
```

### Options Map

| Option | Type | Default | Description |
|---|---|---|---|
| `method` | String | `'rrf'` | `'rrf'` or `'weighted'` |
| `weights` | List\<Float\> | Equal | Per-source weights (must sum to 1.0); weighted mode only |
| `k` | Integer | `60` | RRF constant |
| `fts_k` | Float | `1.0` | BM25 saturation constant: `score / (score + fts_k)` |

**Gotcha -- RRF in point context:** `similar_to()` scores one node at a time (no ranked list), so RRF degenerates to equal-weight averaging. A `RrfPointContext` warning is emitted. Use `method: 'weighted'` for explicit control.

### Execution Path Capability

| Context | Vector | Auto-Embed | FTS | Multi-Source |
|---|---|---|---|---|
| Cypher `MATCH ... WHERE/RETURN` | Yes | Yes | Yes | Yes |
| Locy rule `WHERE / YIELD / ALONG / FOLD` | Yes | Yes | Yes | Yes |
| Locy command `DERIVE / ABDUCE / ASSUME WHERE` | Yes | No | No | No |

### Procedures vs `similar_to`

| | `CALL uni.vector.query / uni.search` | `similar_to()` |
|---|---|---|
| **Operation** | Scan index, return top-K | Score one bound node |
| **Use in WHERE** | No (standalone CALL) | Yes |
| **Use in Locy rules** | No | Yes |
| **Best for** | "Find top 10 from millions" | "Score this matched node" |

---

## 3. Full-Text Search -- `uni.fts.query`

```cypher
CALL uni.fts.query(label, property, search_term, k [, threshold])
YIELD node, score, fts_score, vid
```

| Parameter | Type | Required | Description |
|---|---|---|---|
| `label` | String | Yes | Node label |
| `property` | String | Yes | Text property with fulltext index |
| `search_term` | String | Yes | Search query |
| `k` | Integer | Yes | Number of results |
| `threshold` | Float | No | Minimum score (0-1) |

Scores are BM25, normalized to 0-1 relative to top match.

```cypher
CALL uni.fts.query('Article', 'content', 'distributed graph database', 10)
YIELD node, score
RETURN node.title, score
ORDER BY score DESC
```

**Immediate write visibility:** FTS queries see unflushed writes from the in-memory L0 buffer. Data is searchable immediately after write, no flush needed.

---

## 4. Hybrid Search -- `uni.search`

```cypher
CALL uni.search(label, properties, query_text [, query_vector] [, k]
    [, filter] [, options])
YIELD node, score, vector_score, fts_score, vid
```

| Parameter | Type | Required | Description |
|---|---|---|---|
| `label` | String | Yes | Node label |
| `properties` | Map | Yes | `{vector: 'prop1', fts: 'prop2'}` |
| `query_text` | String | Yes | Text for FTS and auto-embedding |
| `query_vector` | List or null | No | Pre-computed vector; null = auto-embed `query_text` |
| `k` | Integer | No | Number of results (default: 10) |
| `filter` | String | No | WHERE clause for pre-filtering |
| `options` | Map | No | Fusion options (see below) |

### Fusion Options

| Option | Values | Default | Description |
|---|---|---|---|
| `method` | `'rrf'`, `'weighted'` | `'rrf'` | Fusion algorithm |
| `alpha` | 0.0 - 1.0 | 0.5 | Vector weight for weighted fusion |
| `over_fetch` | Float | 2.0 | Over-fetch factor; each branch retrieves `k * over_fetch` |

### Fusion Formulas

**RRF (default):** `score = sum(1 / (60 + rank))` per result across branches. Robust, no score normalization needed.

**Weighted:** `score = alpha * vector_score + (1 - alpha) * fts_score`

| `alpha` | Behavior |
|---|---|
| `0.7` | Favor semantic similarity |
| `0.5` | Equal weight |
| `0.3` | Favor keyword matching |

```cypher
-- RRF (default)
CALL uni.search('Document', {vector: 'embedding', fts: 'content'},
    'graph databases', null, 10)
YIELD node, score
RETURN node.title, score

-- Weighted fusion favoring semantics, with pre-filter
CALL uni.search('Document', {vector: 'embedding', fts: 'content'},
    'deep learning', null, 10,
    'category = "technology" AND year >= 2023',
    {method: 'weighted', alpha: 0.7})
YIELD node, score, vector_score, fts_score
RETURN node.title, score, vector_score, fts_score
```

**Prerequisites:** Hybrid search requires both a vector index (with embedding config) and a fulltext index on the respective properties.

---

## 5. Vector Index Configuration

### Index Type Decision Tree

| Dataset Size | Recommended | Notes |
|---|---|---|
| < 10k vectors | **Flat** | Exact brute-force; no tuning needed |
| 10k - 1M vectors | **HNSW-SQ** (default) | Best recall-latency tradeoff with scalar quantization |
| > 1M vectors, high recall | **HNSW-PQ** | Graph-based with product quantization for memory savings |
| > 1M vectors, memory-constrained | **IVF-PQ** | Partition-based with product quantization, smallest footprint |
| > 1M vectors, quality priority | **IVF-SQ** | Partition-based with scalar quantization, better recall than PQ |

### Algorithm Variants

All 7 algorithms available, grouped by architecture:

**Flat (no index structure):**

| Type | Quantization | Parameters | Best For |
|---|---|---|---|
| **Flat** | None | — | < 10k vectors, exact results |

**IVF (Inverted File — partition-based):**

| Type | Quantization | Parameters | Best For |
|---|---|---|---|
| **IVF-Flat** | None | `partitions` | Medium datasets, exact within partitions |
| **IVF-SQ** | Scalar (int8) | `partitions` | Large datasets, good recall/memory tradeoff |
| **IVF-PQ** | Product | `partitions`, `sub_vectors` | Very large datasets, minimum memory |
| **IVF-RQ** | Residual | `partitions` | Experimental; better accuracy than PQ at same compression |

**HNSW (Hierarchical Navigable Small World — graph-based):**

| Type | Quantization | Parameters | Best For |
|---|---|---|---|
| **HNSW-SQ** | Scalar (int8) | `m`, `ef_construction` | Default choice. Best recall-latency tradeoff |
| **HNSW-PQ** | Product | `m`, `ef_construction`, `sub_vectors` | Large datasets needing graph-speed with memory savings |

### Parameter Reference

| Parameter | Applies To | Default | Description |
|---|---|---|---|
| `partitions` | All IVF variants | 256 | Number of Voronoi partitions. More = faster search, less recall |
| `sub_vectors` | IVF-PQ, HNSW-PQ | 16 | Number of PQ sub-quantizers. More = better recall, more memory |
| `m` | HNSW-SQ, HNSW-PQ | 16 | Edges per node in HNSW graph. Higher = better recall, more memory |
| `ef_construction` | HNSW-SQ, HNSW-PQ | 200 | Build-time search width. Higher = better graph quality, slower build |

### Distance Metrics

| Metric | Raw Distance Range | Score Conversion | Similarity Range | Best For |
|---|---|---|---|---|
| `Cosine` | [0, 2] (`1 - cos(a,b)`) | `(2 - d) / 2` | [0, 1] | Normalized embeddings (most text models) |
| `L2` | [0, inf) (squared Euclidean) | `1 / (1 + d)` | (0, 1] | Raw embeddings, spatial data |
| `Dot` | (-inf, +inf) (negative dot) | Pass-through | Unbounded | Maximum inner product search |

Score conversion is **metric-aware** and shared across `uni.vector.query`, `uni.search`, and `similar_to()`.

---

## 6. Creating Vector Indexes

### Cypher DDL

```cypher
-- HNSW-SQ (default, recommended)
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding)
OPTIONS { type: 'hnsw_sq', metric: 'cosine' }

-- HNSW-PQ for large datasets needing graph speed + compression
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding)
OPTIONS { type: 'hnsw_pq', metric: 'cosine', m: 16, ef_construction: 200, sub_vectors: 8 }

-- IVF-PQ for very large datasets, minimum memory
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding)
OPTIONS { type: 'ivf_pq', metric: 'l2', partitions: 256, sub_vectors: 16 }

-- IVF-SQ for large datasets, better recall than PQ
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding)
OPTIONS { type: 'ivf_sq', metric: 'cosine', partitions: 256 }

-- IVF-RQ (residual quantization)
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding)
OPTIONS { type: 'ivf_rq', metric: 'cosine', partitions: 256 }

-- IVF-Flat (no quantization, exact within partitions)
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding)
OPTIONS { type: 'ivf_flat', metric: 'cosine', partitions: 128 }

-- Flat (brute-force, exact)
CREATE VECTOR INDEX idx_embed FOR (d:Document) ON (d.embedding)
OPTIONS { type: 'flat', metric: 'cosine' }

-- With auto-embedding config (works with any algorithm)
CREATE VECTOR INDEX doc_embed FOR (d:Document) ON (d.embedding)
OPTIONS {
    metric: 'cosine',
    embedding: {
        alias: 'embed/default',
        source: ['content'],
        batch_size: 32
    }
}

-- Short form (defaults to HNSW-SQ)
CREATE VECTOR INDEX idx_embed ON Document (embedding) WITH { metric: 'cosine' }
```

### Rust API

```rust
use uni_db::{DataType, IndexType, VectorAlgo, VectorIndexCfg, VectorMetric};

db.schema()
    .label("Document")
        .property("title", DataType::String)
        .property("embedding", DataType::Vector { dimensions: 384 })
        .index("embedding", IndexType::Vector(VectorIndexCfg {
            algorithm: VectorAlgo::HnswSq { m: 16, ef_construction: 200 },
            metric: VectorMetric::Cosine,
            embedding: None,
        }))
    .apply()
    .await?;

// Other algorithm variants:
// VectorAlgo::Flat
// VectorAlgo::IvfFlat { partitions: 256 }
// VectorAlgo::IvfPq { partitions: 256, sub_vectors: 16 }
// VectorAlgo::IvfSq { partitions: 256 }
// VectorAlgo::IvfRq { partitions: 256 }
// VectorAlgo::Hnsw { m: 16, ef_construction: 200 }   // alias for HnswSq
// VectorAlgo::HnswSq { m: 16, ef_construction: 200 }
// VectorAlgo::HnswPq { m: 16, ef_construction: 200, sub_vectors: 16 }
```

### Python API

```python
db.schema() \
    .label("Document") \
        .property("title", "string") \
        .vector("embedding", 384) \
        .index("embedding", "vector") \
        .done() \
    .apply()
```

---

## 7. Full-Text Index Configuration

### BM25 Fulltext Index

```cypher
CREATE FULLTEXT INDEX idx_content FOR (a:Article) ON (a.content)

-- Multi-property
CREATE FULLTEXT INDEX doc_fts FOR (d:Doc) ON (d.title, d.body)
```

Index is built automatically on creation over existing data. No manual `rebuild_indexes()` needed.

### JSON FTS Index

```cypher
CREATE JSON_FULLTEXT INDEX idx_meta ON Data (metadata)
```

Enables full-text search on nested JSON/JSONB property values.

### CONTAINS Predicate

```cypher
MATCH (d:Doc) WHERE d.body CONTAINS 'vector' RETURN d.title
```

---

## 8. Auto-Embedding / Xervo

### Embedding Config on Vector Index

```cypher
CREATE VECTOR INDEX doc_embed FOR (d:Document) ON (d.embedding)
OPTIONS {
    metric: 'cosine',
    embedding: {
        alias: 'embed/default',
        source: ['title', 'content'],
        batch_size: 32
    }
}
```

- **On write:** The writer auto-embeds text from `source` properties using the `alias` model, stores result in the vector property.
- **On query:** `uni.vector.query` and `similar_to()` auto-embed string query arguments using the same alias.

### Supported Providers

| Provider | Type | Feature Flag |
|---|---|---|
| MistralRS | Local | `provider-mistralrs` |
| Candle | Local | `provider-candle` |
| FastEmbed | Local | `provider-fastembed` |
| OpenAI | Remote | `provider-openai` |
| Gemini | Remote | `provider-gemini` |
| Anthropic | Remote | `provider-anthropic` |
| Vertex AI | Remote | `provider-vertexai` |
| Mistral | Remote | `provider-mistral` |
| Cohere | Remote | `provider-cohere` |
| Voyage AI | Remote | `provider-voyageai` |
| Azure OpenAI | Remote | `provider-azure-openai` |

### Direct Xervo API

```rust
let xervo = db.xervo()?;

// Embed
let vectors = xervo.embed("embed/default", &["query text"]).await?;
// -> Vec<Vec<f32>>

// Generate (structured messages)
let result = xervo.generate("llm/default", &[
    Message::system("You are a helpful assistant."),
    Message::user("Summarize this document."),
], GenerationOptions::default()).await?;
```

```python
xervo = db.xervo()
vectors = xervo.embed("embed/default", ["graph databases", "neural search"])
# -> list[list[float]]
```

---

## 9. Best Practices

### Metric Matching
- Use **Cosine** for most text embedding models (they output normalized vectors).
- Use **L2** for raw/unnormalized embeddings or spatial data.
- Use **Dot** for maximum inner product search. Check your model's documentation.

### Index Type Selection
- **< 10k rows:** Flat (exact). Graph/partition-based indexes need minimum data to be effective.
- **10k - 1M rows:** HNSW-SQ (default, best recall-latency tradeoff with scalar quantization).
- **> 1M rows, quality priority:** HNSW-PQ or IVF-SQ (graph speed or good recall with moderate compression).
- **> 1M rows, memory-constrained:** IVF-PQ (most aggressive compression, smallest footprint).
- **Experimental:** IVF-RQ (residual quantization, potentially better accuracy than PQ at same compression).

### Hybrid Search Tuning
- Start with **RRF** (robust default, no tuning needed).
- Switch to **weighted** when you want explicit semantic vs keyword balance.
- `over_fetch: 2.0` is usually sufficient; increase for highly selective pre-filters.

### Pre-Filtering vs Post-Filtering
- **Pre-filter** (`filter` param in `uni.vector.query`): Pushed to LanceDB, searches only the filtered subset. Use when filter is selective.
- **Post-filter** (`WHERE` after `YIELD`): Searches all nodes, then filters. Use for complex Cypher expressions not expressible in SQL, or non-selective filters.
- Pre-filtering is more efficient when it significantly reduces the search space.

### `similar_to` vs CALL Procedures
- Use `CALL uni.vector.query` / `uni.search` to **find** top-K candidates from a full label.
- Use `similar_to()` to **score** nodes already bound by `MATCH` (e.g., after graph traversal).

### Performance Tips
- `YIELD vid` is much faster than `YIELD node` for large result sets (skips property loading).
- Ensure embedding dimensions match between model and schema.
- Ensure the same model is used for indexing and querying.

---

## 10. Examples

### RAG Pipeline End-to-End

```cypher
-- 1. Setup: schema + indexes
CREATE VECTOR INDEX doc_embed FOR (d:Document) ON (d.embedding)
OPTIONS {
    metric: 'cosine',
    embedding: { alias: 'embed/default', source: ['content'], batch_size: 32 }
}
CREATE FULLTEXT INDEX doc_fts FOR (d:Document) ON (d.content)

-- 2. Ingest (auto-embeds on write)
CREATE (d:Document {title: 'Graph DBs 101', content: 'Graph databases store...'})

-- 3. Retrieve (auto-embeds the query text)
CALL uni.vector.query('Document', 'embedding', 'how do graph databases work', 5)
YIELD node, score
RETURN node.title, node.content, score
ORDER BY score DESC
```

### Semantic Search with Graph Context

```cypher
-- Find similar papers, then expand citations
CALL uni.vector.query('Paper', 'embedding', $query_vector, 10)
YIELD node AS seed, score

MATCH (seed)-[:CITES]->(cited:Paper)
RETURN seed.title AS source, cited.title AS cited_paper, score
ORDER BY score DESC, cited.year DESC
```

### Multi-Hop with Similarity Filter

```cypher
MATCH (start:Paper {title: 'Attention Is All You Need'})
MATCH (start)-[:CITES]->(hop1:Paper)-[:CITES]->(hop2:Paper)
WHERE similar_to(start.embedding, hop2.embedding) > 0.7
RETURN DISTINCT hop2.title, hop2.year
ORDER BY hop2.year DESC
LIMIT 20
```

### Hybrid Search Setup

```cypher
-- Prerequisites: both index types
CREATE VECTOR INDEX doc_embed FOR (d:Document) ON (d.embedding)
OPTIONS {
    metric: 'cosine',
    embedding: { alias: 'embed/default', source: ['content'] }
}
CREATE FULLTEXT INDEX doc_fts FOR (d:Document) ON (d.content)

-- Hybrid query with score transparency
CALL uni.search('Document', {vector: 'embedding', fts: 'content'},
    'transformer attention mechanisms', null, 10)
YIELD node, score, vector_score, fts_score
RETURN node.title, score, vector_score, fts_score
ORDER BY score DESC
```

### Expression-Based Hybrid Scoring

```cypher
MATCH (d:Document)
RETURN d.title,
  similar_to([d.embedding, d.content], 'machine learning',
    {method: 'weighted', weights: [0.7, 0.3]}) AS relevance
ORDER BY relevance DESC
LIMIT 20
```
