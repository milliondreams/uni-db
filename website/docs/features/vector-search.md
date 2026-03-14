# Vector Search

Uni provides native vector search over embedding properties with ANN indexes (HNSW, IVF_PQ, Flat). Use it for semantic search, RAG, and similarity-based retrieval.

## What It Provides

- Vector properties stored alongside graph data.
- ANN indexes with cosine, L2, or dot distance — scores are automatically converted to [0, 1] similarity regardless of metric.
- `CALL uni.vector.query(...)` for KNN retrieval.
- `similar_to()` expression function for point-scoring bound nodes in `WHERE`, `RETURN`, and Locy rules.
- **Auto-embedding**: Pass text directly and let Uni embed it using the index's configured embedding model.

## Example

=== "Rust"
    ```rust
    use uni_db::{DataType, IndexType, Uni, VectorAlgo, VectorIndexCfg, VectorMetric};

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my_db").build().await?;

    db.schema()
        .label("Document")
            .property("title", DataType::String)
            .property("embedding", DataType::Vector { dimensions: 384 })
            .index("embedding", IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Hnsw { m: 16, ef_construction: 200 },
                metric: VectorMetric::Cosine,
                embedding: None,  // Or configure auto-embed
            }))
        .apply()
        .await?;

    let rows = db.query_with(
        "CALL uni.vector.query('Document', 'embedding', $q, 10) YIELD node, score RETURN node, score"
    )
    .param("q", vec![0.1_f32, 0.2, 0.3])
    .fetch_all()
    .await?;

    println!("{:?}", rows);
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Database("./my_db")

    db.schema() \
        .label("Document") \
            .property("title", "string") \
            .vector("embedding", 384) \
            .index("embedding", "vector") \
            .done() \
        .apply()

    rows = db.query(
        "CALL uni.vector.query('Document', 'embedding', $q, 10) YIELD node, score RETURN node, score",
        {"q": [0.1, 0.2, 0.3]}
    )
    print(rows)
    ```

## Auto-Embedding Queries

With an embedding configuration on your index, you can query with text directly:

```cypher
-- Create index with embedding config
CREATE VECTOR INDEX doc_embed FOR (d:Document) ON (d.embedding)
OPTIONS {
    metric: 'cosine',
    embedding: {
        alias: 'embed/default',
        source: ['content'],
        batch_size: 32
    }
}

-- Query with text - Uni auto-embeds it
CALL uni.vector.query('Document', 'embedding', 'machine learning tutorial', 10)
YIELD node, score
RETURN node.title, score
```

## Expression-Based Scoring: `similar_to`

For scoring already-bound nodes rather than top-K retrieval, use `similar_to()`:

```cypher
MATCH (a:Paper)-[:CITES]->(b:Paper)
WHERE similar_to(b.embedding, 'attention mechanisms') > 0.7
RETURN b.title, similar_to(b.embedding, 'attention mechanisms') AS score
```

`similar_to` supports vector similarity, FTS scoring, and multi-source hybrid fusion. It works in `WHERE`, `RETURN`, `ORDER BY`, and Locy rule bodies. See the [Vector Search guide](../guides/vector-search.md#similar_to-expression-function) for full details.

## Uni-Xervo Runtime

Beyond auto-embedding, the `Uni::xervo()` facade gives direct access to embedding and generation models:

=== "Embedding"
    ```rust
    let xervo = db.xervo()?;
    let vectors = xervo.embed("embed/default", &["query text"]).await?;
    ```

=== "Text Generation"
    ```rust
    use uni_db::xervo::{Message, GenerationOptions};
    let xervo = db.xervo()?;

    // Structured messages with roles
    let result = xervo.generate("llm/default", &[
        Message::system("You are a helpful assistant."),
        Message::user("Summarize this document."),
    ], GenerationOptions::default()).await?;

    // Or plain strings (convenience)
    let result = xervo.generate_text("llm/default",
        &["Summarize this."],
        GenerationOptions::default(),
    ).await?;
    ```

Uni-Xervo supports local providers (MistralRS, Candle, FastEmbed) and remote providers (OpenAI, Gemini, Anthropic, Cohere, Vertex AI, Mistral, Voyage AI, Azure OpenAI). See the [Vector Search Guide](../guides/vector-search.md) for the full provider table and configuration details.

## Use Cases

- Semantic search for documents or products.
- RAG retrieval over knowledge graphs.
- Similarity search over embeddings generated in-app.
- Scoring graph-traversed nodes with `similar_to()` in `WHERE` and Locy rules.
- LLM generation with context from graph queries.

## When To Use

Choose vector search when you need semantic similarity rather than exact matching. Pair it with graph traversal for contextual results.

- Use `CALL uni.vector.query(...)` to **find** top-K candidates from a full label.
- Use `similar_to()` to **score** nodes already bound by `MATCH`.

See also: [Full-Text Search](full-text-json-search.md) | [Hybrid Search](hybrid-search.md)
