"""
Hybrid Search with uni-db: Vector Similarity + BM25 Full-Text Search

This script demonstrates:
1. Creating an in-memory uni-db database
2. Defining a Document schema with vector and full-text indexes
3. Ingesting sample documents with pre-computed embeddings
4. Performing hybrid search combining vector similarity with BM25 full-text search
"""

import math
import random
from uni_db import Uni, DataType

# ---------------------------------------------------------------------------
# 1. Helpers: deterministic pseudo-embeddings for reproducibility
# ---------------------------------------------------------------------------

EMBEDDING_DIM = 384

# Seed vocabulary mapped to rough semantic clusters so that related documents
# produce embeddings with higher cosine similarity.
SEMANTIC_SEEDS: dict[str, list[float]] = {}


def _seed_vector(seed: int) -> list[float]:
    """Generate a deterministic unit vector from a seed."""
    rng = random.Random(seed)
    raw = [rng.gauss(0, 1) for _ in range(EMBEDDING_DIM)]
    norm = math.sqrt(sum(x * x for x in raw))
    return [x / norm for x in raw]


# Pre-build cluster centroids for a handful of topics.
_TOPIC_SEEDS = {
    "graph":       42,
    "database":    43,
    "vector":      44,
    "search":      45,
    "machine":     46,
    "learning":    47,
    "neural":      48,
    "network":     49,
    "distributed": 50,
    "index":       51,
    "embedding":   52,
    "knowledge":   53,
    "query":       54,
    "language":    55,
    "model":       56,
}

for word, seed in _TOPIC_SEEDS.items():
    SEMANTIC_SEEDS[word] = _seed_vector(seed)


def fake_embedding(text: str) -> list[float]:
    """
    Create a deterministic embedding by averaging topic centroid vectors
    found in the text, plus a small document-specific perturbation.
    This ensures documents about similar topics have higher cosine similarity
    while keeping the demo fully offline (no model required).
    """
    words = text.lower().split()
    matched_vecs: list[list[float]] = []
    for w in words:
        for topic, vec in SEMANTIC_SEEDS.items():
            if topic in w:
                matched_vecs.append(vec)
                break

    # Fallback: hash-based vector if no topic words matched.
    if not matched_vecs:
        return _seed_vector(hash(text) % (2**31))

    # Average the matched topic vectors.
    avg = [0.0] * EMBEDDING_DIM
    for v in matched_vecs:
        for i in range(EMBEDDING_DIM):
            avg[i] += v[i]
    n = len(matched_vecs)
    for i in range(EMBEDDING_DIM):
        avg[i] /= n

    # Add a small document-specific perturbation so each doc is unique.
    rng = random.Random(hash(text) % (2**31))
    for i in range(EMBEDDING_DIM):
        avg[i] += rng.gauss(0, 0.05)

    # L2-normalize to unit length (cosine metric expects this).
    norm = math.sqrt(sum(x * x for x in avg))
    return [x / norm for x in avg]


# ---------------------------------------------------------------------------
# 2. Sample documents
# ---------------------------------------------------------------------------

DOCUMENTS = [
    {
        "ext_id": "doc-001",
        "title": "Introduction to Graph Databases",
        "content": (
            "Graph databases store data as nodes and edges, enabling efficient "
            "traversal of complex relationships. They excel at queries involving "
            "connected data such as social networks, recommendation engines, and "
            "knowledge graphs."
        ),
        "category": "database",
        "year": 2024,
    },
    {
        "ext_id": "doc-002",
        "title": "Vector Search and Embeddings",
        "content": (
            "Vector search uses high-dimensional embeddings to find semantically "
            "similar items. Modern embedding models convert text, images, and other "
            "data into dense vectors that capture meaning, enabling similarity "
            "search at scale using indexes like HNSW and IVF-PQ."
        ),
        "category": "search",
        "year": 2024,
    },
    {
        "ext_id": "doc-003",
        "title": "BM25 Full-Text Search Explained",
        "content": (
            "BM25 is a probabilistic ranking function used in information retrieval. "
            "It scores documents based on term frequency and inverse document frequency, "
            "with saturation to prevent common terms from dominating. BM25 is the "
            "standard algorithm behind most full-text search engines."
        ),
        "category": "search",
        "year": 2023,
    },
    {
        "ext_id": "doc-004",
        "title": "Hybrid Search: Combining Vector and Keyword Retrieval",
        "content": (
            "Hybrid search combines vector similarity with keyword-based full-text "
            "search to get the best of both worlds. Reciprocal Rank Fusion (RRF) "
            "merges ranked lists from each search modality without requiring score "
            "normalization, producing robust results across diverse query types."
        ),
        "category": "search",
        "year": 2024,
    },
    {
        "ext_id": "doc-005",
        "title": "Neural Networks for Natural Language Processing",
        "content": (
            "Deep neural networks have transformed NLP through architectures like "
            "transformers. Pre-trained language models such as BERT and GPT learn "
            "contextual word embeddings that capture grammar, semantics, and world "
            "knowledge, enabling downstream tasks from classification to generation."
        ),
        "category": "machine_learning",
        "year": 2023,
    },
    {
        "ext_id": "doc-006",
        "title": "Knowledge Graphs and Reasoning",
        "content": (
            "Knowledge graphs represent structured information as entities and "
            "relations. Combined with graph databases, they enable logical reasoning, "
            "link prediction, and question answering over large-scale factual data. "
            "Graph neural networks can learn embeddings over knowledge graph structure."
        ),
        "category": "database",
        "year": 2024,
    },
    {
        "ext_id": "doc-007",
        "title": "Distributed Database Systems",
        "content": (
            "Distributed databases partition data across multiple nodes for "
            "scalability and fault tolerance. Consensus algorithms like Raft and "
            "Paxos ensure consistency. Modern distributed databases support both "
            "OLTP and OLAP workloads with columnar storage and vectorized execution."
        ),
        "category": "database",
        "year": 2023,
    },
    {
        "ext_id": "doc-008",
        "title": "Building RAG Pipelines with Graph and Vector Search",
        "content": (
            "Retrieval-augmented generation (RAG) pipelines combine vector search "
            "for semantic retrieval with graph traversal for structured context. "
            "By indexing document chunks as graph nodes with vector embeddings, "
            "systems can retrieve relevant passages and follow citation or "
            "co-reference edges to gather richer context for language model prompts."
        ),
        "category": "machine_learning",
        "year": 2024,
    },
]


# ---------------------------------------------------------------------------
# 3. Database setup: open, define schema, create indexes
# ---------------------------------------------------------------------------

def main() -> None:
    # Use an in-memory database (no files on disk, auto-cleaned).
    db = Uni.in_memory()

    # Define the Document label with typed properties and a vector column.
    # The schema-first approach ensures columnar storage for efficient scans.
    db.schema() \
        .label("Document") \
            .property("title", DataType.STRING()) \
            .property("content", DataType.STRING()) \
            .property("category", DataType.STRING()) \
            .property("year", DataType.INT64()) \
            .vector("embedding", EMBEDDING_DIM) \
            .index("title", "btree") \
            .index("category", "btree") \
        .apply()

    session = db.session()

    # Create vector index (HNSW with cosine metric -- matches normalized embeddings).
    with session.tx() as tx:
        tx.execute("""
            CREATE VECTOR INDEX doc_embed FOR (d:Document) ON (d.embedding)
            OPTIONS { type: 'hnsw', metric: 'cosine' }
        """)
        tx.commit()

    # Create full-text index on the content property for BM25 search.
    with session.tx() as tx:
        tx.execute("""
            CREATE FULLTEXT INDEX doc_fts FOR (d:Document) ON (d.content)
        """)
        tx.commit()

    print("Schema and indexes created.\n")

    # ------------------------------------------------------------------
    # 4. Ingest sample documents with embeddings
    # ------------------------------------------------------------------

    with session.tx() as tx:
        for doc in DOCUMENTS:
            embedding = fake_embedding(doc["content"])
            tx.execute(
                """
                CREATE (:Document {
                    ext_id: $ext_id,
                    title: $title,
                    content: $content,
                    category: $category,
                    year: $year,
                    embedding: $embedding
                })
                """,
                params={
                    "ext_id": doc["ext_id"],
                    "title": doc["title"],
                    "content": doc["content"],
                    "category": doc["category"],
                    "year": doc["year"],
                    "embedding": embedding,
                },
            )
        result = tx.commit()
        print(f"Ingested {len(DOCUMENTS)} documents "
              f"(version {result.version}, "
              f"{result.mutations_committed} mutations).\n")

    # ------------------------------------------------------------------
    # 5. Pure vector search
    # ------------------------------------------------------------------

    query_text = "semantic search using embeddings in databases"
    query_vector = fake_embedding(query_text)

    print("=" * 70)
    print(f"Query: \"{query_text}\"")
    print("=" * 70)

    print("\n--- Vector Search (top 5) ---")
    vector_results = session.query(
        """
        CALL uni.vector.query('Document', 'embedding', $query_vector, 5)
        YIELD node, score
        RETURN node.title AS title, node.category AS category, score
        ORDER BY score DESC
        """,
        params={"query_vector": query_vector},
    )
    for row in vector_results:
        print(f"  [{row['score']:.4f}] {row['title']} ({row['category']})")

    # ------------------------------------------------------------------
    # 6. Pure full-text (BM25) search
    # ------------------------------------------------------------------

    fts_query = "vector embeddings similarity search"
    print(f"\n--- Full-Text Search: \"{fts_query}\" (top 5) ---")
    fts_results = session.query(
        """
        CALL uni.fts.query('Document', 'content', $search_term, 5)
        YIELD node, score
        RETURN node.title AS title, node.category AS category, score
        ORDER BY score DESC
        """,
        params={"search_term": fts_query},
    )
    for row in fts_results:
        print(f"  [{row['score']:.4f}] {row['title']} ({row['category']})")

    # ------------------------------------------------------------------
    # 7. Hybrid search with RRF fusion (default)
    # ------------------------------------------------------------------

    print(f"\n--- Hybrid Search (RRF): \"{query_text}\" (top 5) ---")
    hybrid_rrf = session.query(
        """
        CALL uni.search('Document', {vector: 'embedding', fts: 'content'},
            $query_text, $query_vector, 5)
        YIELD node, score, vector_score, fts_score
        RETURN node.title AS title,
               node.category AS category,
               score,
               vector_score,
               fts_score
        ORDER BY score DESC
        """,
        params={"query_text": query_text, "query_vector": query_vector},
    )
    for row in hybrid_rrf:
        print(f"  [{row['score']:.4f}] {row['title']} "
              f"(vec={row['vector_score']:.4f}, fts={row['fts_score']:.4f})")

    # ------------------------------------------------------------------
    # 8. Hybrid search with weighted fusion (favor semantics)
    # ------------------------------------------------------------------

    print(f"\n--- Hybrid Search (Weighted, alpha=0.7): \"{query_text}\" (top 5) ---")
    hybrid_weighted = session.query(
        """
        CALL uni.search('Document', {vector: 'embedding', fts: 'content'},
            $query_text, $query_vector, 5,
            null,
            {method: 'weighted', alpha: 0.7})
        YIELD node, score, vector_score, fts_score
        RETURN node.title AS title,
               node.category AS category,
               score,
               vector_score,
               fts_score
        ORDER BY score DESC
        """,
        params={"query_text": query_text, "query_vector": query_vector},
    )
    for row in hybrid_weighted:
        print(f"  [{row['score']:.4f}] {row['title']} "
              f"(vec={row['vector_score']:.4f}, fts={row['fts_score']:.4f})")

    # ------------------------------------------------------------------
    # 9. Hybrid search with pre-filter (category + year)
    # ------------------------------------------------------------------

    print(f"\n--- Hybrid Search with Filter (category='search', year>=2024) ---")
    hybrid_filtered = session.query(
        """
        CALL uni.search('Document', {vector: 'embedding', fts: 'content'},
            $query_text, $query_vector, 5,
            'category = "search" AND year >= 2024')
        YIELD node, score, vector_score, fts_score
        RETURN node.title AS title,
               node.year AS year,
               score,
               vector_score,
               fts_score
        ORDER BY score DESC
        """,
        params={"query_text": query_text, "query_vector": query_vector},
    )
    for row in hybrid_filtered:
        print(f"  [{row['score']:.4f}] {row['title']} "
              f"(year={row['year']}, vec={row['vector_score']:.4f}, "
              f"fts={row['fts_score']:.4f})")

    # ------------------------------------------------------------------
    # 10. Expression-based hybrid scoring with similar_to()
    # ------------------------------------------------------------------

    print(f"\n--- Expression-Based Hybrid (similar_to, weighted) ---")
    expr_results = session.query(
        """
        MATCH (d:Document)
        RETURN d.title AS title,
               d.category AS category,
               similar_to(
                   [d.embedding, d.content],
                   [$query_vector, $query_text],
                   {method: 'weighted', weights: [0.6, 0.4]}
               ) AS relevance
        ORDER BY relevance DESC
        LIMIT 5
        """,
        params={"query_vector": query_vector, "query_text": query_text},
    )
    for row in expr_results:
        print(f"  [{row['relevance']:.4f}] {row['title']} ({row['category']})")

    # ------------------------------------------------------------------
    # 11. Vector search + graph traversal: find similar docs then expand
    # ------------------------------------------------------------------

    # First, create some edges between related documents.
    with session.tx() as tx:
        tx.execute("""
            MATCH (a:Document {ext_id: 'doc-001'}), (b:Document {ext_id: 'doc-006'})
            CREATE (a)-[:RELATED_TO {reason: 'both about graph databases'}]->(b)
        """)
        tx.execute("""
            MATCH (a:Document {ext_id: 'doc-002'}), (b:Document {ext_id: 'doc-004'})
            CREATE (a)-[:RELATED_TO {reason: 'both about vector/hybrid search'}]->(b)
        """)
        tx.execute("""
            MATCH (a:Document {ext_id: 'doc-004'}), (b:Document {ext_id: 'doc-008'})
            CREATE (a)-[:RELATED_TO {reason: 'hybrid search in RAG'}]->(b)
        """)
        tx.execute("""
            MATCH (a:Document {ext_id: 'doc-005'}), (b:Document {ext_id: 'doc-008'})
            CREATE (a)-[:RELATED_TO {reason: 'neural models in RAG'}]->(b)
        """)
        tx.commit()

    print(f"\n--- Vector Search + Graph Expansion ---")
    graph_results = session.query(
        """
        CALL uni.vector.query('Document', 'embedding', $query_vector, 3)
        YIELD node AS seed, score

        OPTIONAL MATCH (seed)-[:RELATED_TO]->(related:Document)
        RETURN seed.title AS seed_title,
               score AS seed_score,
               collect(related.title) AS related_titles
        ORDER BY seed_score DESC
        """,
        params={"query_vector": query_vector},
    )
    for row in graph_results:
        related = row["related_titles"]
        related_str = ", ".join(related) if related else "(none)"
        print(f"  [{row['seed_score']:.4f}] {row['seed_title']}")
        print(f"          -> Related: {related_str}")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    print("\nDone.")
    db.shutdown()


if __name__ == "__main__":
    main()
