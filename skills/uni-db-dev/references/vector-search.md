# Vector Search, Full-Text Search & Hybrid Search Reference

## Table of Contents
1. [Vector Search](#vector-search)
2. [Full-Text Search](#full-text-search)
3. [Hybrid Search](#hybrid-search)
4. [Schema Procedures](#schema-procedures)
5. [Index Management](#index-management)

---

## Vector Search

### Syntax

```cypher
CALL uni.vector.query(label, property, query, k, [filter], [threshold])
YIELD [node], [vid], [distance], [score]
```

### Parameters

| Position | Name | Type | Required | Description |
|----------|------|------|----------|-------------|
| 1 | label | STRING | Yes | Label to search (e.g., `'Paper'`) |
| 2 | property | STRING | Yes | Vector property name (e.g., `'embedding'`) |
| 3 | query | VECTOR or STRING | Yes | Query vector `[0.1, 0.2, ...]` or text for auto-embedding |
| 4 | k | INTEGER | Yes | Number of results |
| 5 | filter | STRING | No | Pre-filter WHERE clause (Lance SQL) |
| 6 | threshold | FLOAT | No | Max distance threshold (post-filter) |

### Yield Items

| Name | Type | Description |
|------|------|-------------|
| `node` | Node | Full node with all properties |
| `vid` | Integer | Vertex ID (efficient for joins) |
| `distance` | Float | Raw distance (lower = more similar) |
| `score` | Float | Normalized 0-1 similarity (higher = more similar) |

### Score Calculation by Metric

| Metric | Formula |
|--------|---------|
| Cosine | `(2 - distance) / 2` |
| L2 | `1 / (1 + distance)` |
| Dot | `distance` (raw dot product) |

### Examples

```cypher
-- Basic KNN
CALL uni.vector.query('Paper', 'embedding', $vec, 10)
YIELD node, distance
RETURN node.title, distance
ORDER BY distance

-- Auto-embedding (requires configured embedding model)
CALL uni.vector.query('Paper', 'embedding', 'transformer attention mechanisms', 10)
YIELD node, score
RETURN node.title, score

-- Pre-filtering
CALL uni.vector.query('Product', 'embedding', $vec, 100,
  'category = ''electronics'' AND price < 1000')
YIELD node, distance
RETURN node.name, node.price, distance

-- With distance threshold
CALL uni.vector.query('Paper', 'embedding', $vec, 1000, null, 0.5)
YIELD node, distance
RETURN node.title

-- VID-only for efficient graph joins
CALL uni.vector.query('Document', 'embedding', $query_vec, 5)
YIELD vid, distance
MATCH (d) WHERE d._vid = vid
MATCH (d)-[:MENTIONS]->(entity:Entity)
RETURN entity.name, distance
```

---

## Full-Text Search

### Syntax

```cypher
CALL uni.fts.query(label, property, search_term, k, [filter], [threshold])
YIELD [node], [vid], [score]
```

### Parameters

| Position | Name | Type | Required | Description |
|----------|------|------|----------|-------------|
| 1 | label | STRING | Yes | Label to search |
| 2 | property | STRING | Yes | Property with full-text index |
| 3 | search_term | STRING | Yes | Keyword search query |
| 4 | k | INTEGER | Yes | Number of results |
| 5 | filter | STRING | No | Pre-filter WHERE clause |
| 6 | threshold | FLOAT | No | Minimum BM25 score |

### Yield Items

| Name | Type | Description |
|------|------|-------------|
| `node` | Node | Full node |
| `vid` | Integer | Vertex ID |
| `score` | Float | BM25 relevance score |

### Example

```cypher
CALL uni.fts.query('Paper', 'abstract', 'neural networks', 20)
YIELD node, score
RETURN node.title, score
ORDER BY score DESC
```

---

## Hybrid Search

Combines vector similarity and full-text search using reciprocal rank fusion (RRF) or weighted scoring.

### Syntax

```cypher
CALL uni.search(label, properties, query_text, [query_vector], k, [filter], [options])
YIELD [node], [vid], [score], [vector_score], [fts_score], [distance]
```

### Parameters

| Position | Name | Type | Required | Description |
|----------|------|------|----------|-------------|
| 1 | label | STRING | Yes | Label to search |
| 2 | properties | OBJECT or STRING | Yes | `{vector: 'embedding', fts: 'abstract'}` or `'field'` |
| 3 | query_text | STRING | Yes | Search text for both vector and FTS |
| 4 | query_vector | VECTOR | No | Pre-computed vector (null for auto-embed) |
| 5 | k | INTEGER | Yes | Final result count |
| 6 | filter | STRING | No | Pre-filter clause |
| 7 | options | OBJECT | No | Fusion config (see below) |

### Options

| Key | Default | Description |
|-----|---------|-------------|
| `method` | `"rrf"` | Fusion method: `"rrf"` or `"weighted"` |
| `alpha` | `0.5` | Weight for vector vs FTS (0=FTS only, 1=vector only) |
| `over_fetch` | `2.0` | Intermediate result multiplier |
| `rrf_k` | `60` | RRF parameter |

### Yield Items

| Name | Type | Description |
|------|------|-------------|
| `node` | Node | Full node |
| `vid` | Integer | Vertex ID |
| `score` | Float | Combined fused score |
| `vector_score` | Float | Vector component (normalized 0-1) |
| `fts_score` | Float | FTS component (normalized) |
| `distance` | Float | Raw vector distance |

### Example

```cypher
CALL uni.search(
  'Paper',
  {vector: 'embedding', fts: 'abstract'},
  'transformer architectures',
  null,
  10,
  'year > 2020',
  {method: 'rrf', alpha: 0.7}
)
YIELD node, score, vector_score, fts_score
RETURN node.title, score
ORDER BY score DESC
```

---

## similar_to() Expression Function

Inline similarity scoring for already-bound nodes. Unlike `CALL` procedures that scan indexes for top-K results, `similar_to()` scores **one bound node** per row.

### Syntax

```cypher
similar_to(sources, queries [, options]) → FLOAT [0, 1]
```

### Scoring Modes (auto-detected)

| Source Property | Query Argument | Mode |
|---|---|---|
| Vector column | Vector literal/param | Vector-to-vector cosine |
| Vector column | String literal | Auto-embed → cosine |
| Text column (FTS-indexed) | String literal | BM25 full-text scoring |

### Examples

```cypher
-- Vector-to-vector
MATCH (d:Doc) RETURN d.title, similar_to(d.embedding, $vec) AS score

-- Auto-embed
MATCH (d:Doc) RETURN d.title, similar_to(d.embedding, 'graph databases') AS score

-- FTS scoring
MATCH (d:Doc) RETURN d.title, similar_to(d.content, 'distributed systems') AS score

-- In WHERE clause
MATCH (d:Doc) WHERE similar_to(d.embedding, $vec) > 0.8 RETURN d.title

-- Multi-source fusion (RRF)
MATCH (d:Doc)
RETURN d.title, similar_to([d.embedding, d.content], [$vec, 'search term']) AS score

-- Weighted fusion
MATCH (d:Doc)
RETURN d.title, similar_to(
  [d.embedding, d.content], [$vec, 'query'],
  {method: 'weighted', weights: [0.7, 0.3]}
) AS score
```

### Options Map

| Key | Default | Description |
|-----|---------|-------------|
| `method` | `"rrf"` | Fusion method: `"rrf"` or `"weighted"` |
| `weights` | equal | Per-source weights (for `"weighted"` method) |
| `k` | 60 | RRF ranking parameter |
| `fts_k` | 1000 | Max FTS candidates |

### Comparison with Procedures

| | `CALL uni.search(...)` | `similar_to()` |
|---|---|---|
| Operation | Scan index, return top-K | Score one bound node |
| Use in WHERE | No | Yes |
| Use in Locy rules | No | Yes |
| Best for | "Find top 10 from millions" | "Score this matched node" |

---

## Schema Procedures

### List Labels

```cypher
CALL uni.schema.labels()
YIELD label, propertyCount, nodeCount, indexCount
```

### List Edge Types

```cypher
CALL uni.schema.edgeTypes()
YIELD type, relationshipType, sourceLabels, targetLabels, propertyCount
```

### List Indexes

```cypher
CALL uni.schema.indexes()
YIELD name, type, label, state, properties
```

### List Constraints

```cypher
CALL uni.schema.constraints()
YIELD name, type, enabled, properties, label
```

### Label Details

```cypher
CALL uni.schema.labelInfo('Person')
YIELD property, dataType, nullable, indexed, unique
```

---

## Index Management

### Creating Indexes

```cypher
-- Scalar index
CREATE INDEX ON :Person(email)

-- Vector index
CREATE VECTOR INDEX ON :Paper(embedding) OPTIONS {
  type: "hnsw",
  metric: "cosine"
}

-- Vector index with auto-embedding
CREATE VECTOR INDEX ON :Paper(embedding) OPTIONS {
  type: "hnsw",
  metric: "cosine",
  embedding: {
    alias: "embed/default",
    source: ["abstract"],
    batch_size: 32
  }
}

-- Full-text index on JSON property
CREATE JSON FULLTEXT INDEX ON :Paper(metadata)
```

### Managing Indexes

```cypher
DROP INDEX index_name
SHOW INDEXES
```

### Vector Index Types

| Type | Best For |
|------|----------|
| `hnsw` | General purpose, good recall/speed tradeoff |
| `ivf_pq` | Very large datasets, lower memory |
| `flat` | Small datasets, exact results |

### Distance Metrics

| Metric | Use When |
|--------|----------|
| `cosine` | Normalized embeddings (most common) |
| `l2` | Euclidean distance |
| `dot` | Inner product (unnormalized embeddings) |
