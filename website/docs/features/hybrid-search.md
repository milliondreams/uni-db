# Hybrid Search

Uni provides hybrid search that combines vector similarity and full-text search using rank fusion algorithms. Get the best of both semantic understanding and keyword matching.

## What It Provides

- Combined vector + FTS search in a single procedure.
- Reciprocal Rank Fusion (RRF) for robust result merging.
- Weighted fusion for tunable semantic vs keyword balance.
- Pre-filtering applied to both search branches.
- Individual score transparency when needed.

## Example

=== "Cypher"
    ```cypher
    -- Hybrid search with auto-embedding
    CALL uni.search(
        'Document',
        {vector: 'embedding', fts: 'content'},
        'machine learning optimization',
        null,  -- auto-embed the query text
        10
    )
    YIELD node, score
    RETURN node.title, score
    ORDER BY score DESC
    ```

=== "Rust"
    ```rust
    use uni_db::Uni;

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my_db").build().await?;
    let session = db.session();

    let rows = session.query(r#"
        CALL uni.search(
            'Document',
            {vector: 'embedding', fts: 'content'},
            'neural network architectures',
            null,
            10
        )
        YIELD node, score, vector_score, fts_score
        RETURN node.title, score, vector_score, fts_score
    "#).await?;

    println!("{:?}", rows);
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Uni.open("./my_db")
    session = db.session()

    rows = session.query("""
        CALL uni.search(
            'Document',
            {vector: 'embedding', fts: 'content'},
            'neural network architectures',
            null,
            10
        )
        YIELD node, score
        RETURN node.title, score
    """)
    print(rows)
    ```

## Procedure Signature

```cypher
CALL uni.search(label, properties, query_text [, query_vector] [, k] [, filter] [, options])
YIELD vid, score, node [, vector_score] [, fts_score]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `label` | String | Node label to search |
| `properties` | Map | `{vector: 'prop1', fts: 'prop2'}` |
| `query_text` | String | Text for FTS and auto-embedding |
| `query_vector` | List or null | Pre-computed vector (optional) |
| `k` | Integer | Number of results (default: 10) |
| `filter` | String | WHERE clause for pre-filtering |
| `options` | Map | Fusion options |

**Fusion Options:**

| Option | Values | Description |
|--------|--------|-------------|
| `method` | `'rrf'` (default), `'weighted'` | Fusion algorithm |
| `alpha` | 0.0 - 1.0 | Vector weight for weighted fusion |
| `over_fetch` | Float | Over-fetch factor (default: 2.0) |
| `reranker` | String | Xervo alias for cross-encoder model (see [Reranking](#cross-encoder-reranking)) |
| `reranker_property` | String | Node text property for cross-encoder input |
| `reranker_k` | Integer | Candidates for reranking (default: k×3, max: 1000) |
| `reranker_query` | String | Override query text for cross-encoder |

## Fusion Methods

### RRF (Reciprocal Rank Fusion) - Default

Best for general use. Combines rankings without requiring score normalization:

```
score = Σ 1/(k + rank)  where k=60
```

### Weighted Fusion

Use when you want to tune the balance between semantic and keyword matching:

```
score = alpha × vector_score + (1 - alpha) × fts_score
```

- `alpha = 0.7` → Favor semantic similarity
- `alpha = 0.3` → Favor keyword matching
- `alpha = 0.5` → Equal weight (default)

## Examples

### With Pre-Filter

```cypher
CALL uni.search(
    'Document',
    {vector: 'embedding', fts: 'content'},
    'graph databases',
    null,
    10,
    'category = "technology" AND year >= 2023'
)
YIELD node, score
RETURN node.title, score
```

### Weighted Fusion (Favor Semantics)

```cypher
CALL uni.search(
    'Document',
    {vector: 'embedding', fts: 'content'},
    'deep learning',
    null,
    10,
    null,
    {method: 'weighted', alpha: 0.7}
)
YIELD node, score
RETURN node.title, score
```

### Score Transparency

```cypher
CALL uni.search(
    'Document',
    {vector: 'embedding', fts: 'content'},
    'transformer models',
    null,
    10
)
YIELD node, score, vector_score, fts_score
RETURN node.title, score, vector_score, fts_score
```

## Expression Form: `similar_to`

For scoring already-bound nodes (rather than top-K retrieval), use the `similar_to()` expression function. It works in `WHERE`, `RETURN`, `ORDER BY`, and Locy rule bodies:

```cypher
-- Hybrid scoring as an expression (correct way)
MATCH (d:Document)
RETURN d.title,
  similar_to([d.embedding, d.content], 'machine learning') AS relevance
ORDER BY relevance DESC
LIMIT 20

-- With weighted fusion
MATCH (d:Document)
WHERE similar_to([d.embedding, d.content], 'deep learning',
  {method: 'weighted', weights: [0.7, 0.3]}) > 0.5
RETURN d.title
```

`similar_to` uses the same fusion algorithms (RRF, weighted) as `uni.search`, but operates on one node at a time. Vector scoring is metric-aware — it automatically uses the index's configured distance metric (Cosine, L2, or Dot Product).

!!! note "RRF in point-computation context"
    Because `similar_to()` scores one node at a time (no ranked list), RRF fusion degenerates to equal-weight averaging. A `RrfPointContext` warning is emitted in this case. Use `method: 'weighted'` for explicit control over source weights.

### Correct vs Incorrect Hybrid

Always use a **single** `similar_to` call with multi-source arrays for hybrid search:

```cypher
-- ✅ CORRECT: single call with fusion and BM25 normalization
MATCH (d:Document)
RETURN d.title,
  similar_to([d.embedding, d.content], [$qvec, $qtxt]) AS score
ORDER BY score DESC

-- ❌ INCORRECT: naive addition mixes incompatible score scales
MATCH (d:Document)
RETURN d.title,
  (similar_to(d.embedding, $qvec) + similar_to(d.content, $qtxt)) AS score
ORDER BY score DESC
```

Adding two separate `similar_to` calls produces raw score addition without normalization — cosine similarity scores ([0, 1]) and BM25 scores (unbounded) live on different scales. The multi-source form normalizes BM25 via a saturation function (`score / (score + fts_k)`) before fusion.

See the [Vector Search guide](../guides/vector-search.md#similar_to-expression-function) for full documentation.

## Cross-Encoder Reranking

All three search procedures (`uni.search`, `uni.vector.query`, `uni.fts.query`) support an optional cross-encoder reranking stage. A cross-encoder jointly attends to a (query, document) pair to produce a more accurate relevance score, but is too expensive to run on the full corpus. By running it on a small over-fetched candidate set, you get fast retrieval with high-precision final ranking.

### How It Works

```
Retrieval (vector/FTS/hybrid) → Over-fetch reranker_k candidates → Cross-encoder re-scores → Top k returned
```

### Example

```cypher
CALL uni.search(
    'Document',
    {vector: 'embedding', fts: 'content'},
    'transformer attention mechanisms',
    null,
    10,
    null,
    {reranker: 'rerank/minilm', reranker_property: 'content'}
)
YIELD node, score, rerank_score, vector_score, fts_score
RETURN node.title, score
ORDER BY score DESC
```

When reranking is active, `score` reflects the reranker score. Original retrieval scores remain available via `vector_score` and `fts_score`. The `rerank_score` column is `null` when no reranker is configured.

### Available Providers

| Provider | Provider ID | Model | Type |
|----------|-------------|-------|------|
| ONNX (local) | `local/onnx` | `cross-encoder/ms-marco-MiniLM-L6-v2` | Local CPU, `provider-onnx` feature |
| Cohere | `remote/cohere` | `rerank-english-v3.0` | Remote API |
| Voyage AI | `remote/voyageai` | `rerank-2` | Remote API |

!!! note "Reranking does not apply to `similar_to()`"
    `similar_to()` is a per-row expression with no bounded candidate set. Cross-encoders are only effective on small candidate sets, so reranking is limited to the three search procedures.

## Use Cases

- **RAG applications**: Combine semantic retrieval with keyword boosting.
- **E-commerce search**: Match product descriptions semantically while respecting exact brand/model queries.
- **Document search**: Find relevant documents even when terminology varies.
- **Knowledge bases**: Surface answers that are both semantically relevant and contain key terms.

## When To Use

Use hybrid search when:

- Users might use different terminology than your content
- Some queries are keyword-specific (product names, codes) while others are conceptual
- You want robust search without tuning
- You need to combine the precision of keywords with the recall of semantics

## Prerequisites

Hybrid search requires both:

1. A **vector index** with embedding configuration
2. A **fulltext index** on the text property

```cypher
-- Vector index with auto-embed
CREATE VECTOR INDEX doc_embed FOR (d:Document) ON (d.embedding)
OPTIONS {
    metric: 'cosine',
    embedding: {provider: 'Candle', model: 'all-MiniLM-L6-v2', source: ['content']}
}

-- Fulltext index
CREATE FULLTEXT INDEX doc_fts FOR (d:Document) ON (d.content)
```

See also: [Vector Search](vector-search.md) | [Full-Text Search](full-text-json-search.md)
