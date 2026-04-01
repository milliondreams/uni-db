"""
Hybrid Search with uni-db: Vector Similarity + BM25 Full-Text Search

This script demonstrates how to:
1. Create a uni-db database with a document store schema
2. Define vector and full-text indexes on document content
3. Ingest sample documents with pre-computed embeddings
4. Perform hybrid search combining vector similarity with BM25 full-text search
5. Compare results from vector-only, FTS-only, and hybrid search
"""

import math
import tempfile

import uni_db


# ---------------------------------------------------------------------------
# 1. Synthetic embedding helper
# ---------------------------------------------------------------------------

# We use lightweight hand-crafted embeddings so the script is self-contained
# (no external model dependency). Each document gets a 64-dimensional vector
# whose components loosely encode its semantic topic.

TOPIC_VECTORS = {
    "machine_learning": [1.0, 0.0, 0.0, 0.0],
    "databases":        [0.0, 1.0, 0.0, 0.0],
    "web_dev":          [0.0, 0.0, 1.0, 0.0],
    "systems":          [0.0, 0.0, 0.0, 1.0],
}

def make_embedding(topics: list[str], weights: list[float] | None = None) -> list[float]:
    """Create a 64-dim embedding by mixing topic basis vectors."""
    dim = 64
    vec = [0.0] * dim
    if weights is None:
        weights = [1.0] * len(topics)
    for topic, w in zip(topics, weights):
        basis = TOPIC_VECTORS.get(topic, [0.0, 0.0, 0.0, 0.0])
        for i, b in enumerate(basis):
            vec[i] += b * w
    # Normalize to unit length
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [v / norm for v in vec]


# ---------------------------------------------------------------------------
# 2. Sample document corpus
# ---------------------------------------------------------------------------

DOCUMENTS = [
    {
        "title": "Introduction to Neural Networks",
        "content": (
            "Neural networks are computing systems inspired by biological neural "
            "networks. Deep learning uses multiple layers to progressively extract "
            "higher-level features from raw input. Backpropagation is the key "
            "algorithm for training neural networks."
        ),
        "category": "AI",
        "year": 2023,
        "topics": ["machine_learning"],
        "weights": [1.0],
    },
    {
        "title": "Graph Databases Explained",
        "content": (
            "Graph databases store data as nodes and edges, making them ideal for "
            "connected data. They excel at traversal queries, relationship-heavy "
            "workloads, and social network analysis. Popular engines include Neo4j "
            "and uni-db."
        ),
        "category": "Databases",
        "year": 2024,
        "topics": ["databases"],
        "weights": [1.0],
    },
    {
        "title": "Modern Web Frameworks",
        "content": (
            "React, Vue, and Svelte are popular JavaScript frameworks for building "
            "modern web applications. Server-side rendering improves SEO and initial "
            "load performance. Web components provide framework-agnostic reusability."
        ),
        "category": "Web",
        "year": 2024,
        "topics": ["web_dev"],
        "weights": [1.0],
    },
    {
        "title": "Rust for Systems Programming",
        "content": (
            "Rust provides memory safety without garbage collection through its "
            "ownership system. It is used for operating systems, game engines, and "
            "database internals. Rust eliminates data races at compile time."
        ),
        "category": "Systems",
        "year": 2023,
        "topics": ["systems"],
        "weights": [1.0],
    },
    {
        "title": "Vector Search in Databases",
        "content": (
            "Vector search enables similarity-based retrieval using embeddings. "
            "Approximate nearest neighbor algorithms like HNSW and IVF make "
            "large-scale vector search practical. Graph databases can combine "
            "vector search with structured traversal queries."
        ),
        "category": "Databases",
        "year": 2024,
        "topics": ["databases", "machine_learning"],
        "weights": [0.7, 0.3],
    },
    {
        "title": "Building ML Pipelines",
        "content": (
            "Machine learning pipelines orchestrate data ingestion, feature "
            "engineering, model training, and deployment. Tools like Kubeflow "
            "and MLflow help manage the lifecycle. Feature stores centralize "
            "feature computation and serving."
        ),
        "category": "AI",
        "year": 2023,
        "topics": ["machine_learning"],
        "weights": [1.0],
    },
    {
        "title": "Full-Text Search Internals",
        "content": (
            "Full-text search engines use inverted indexes and BM25 scoring to "
            "rank documents by relevance. Tokenization, stemming, and stop-word "
            "removal are key preprocessing steps. Tantivy is a fast Rust-based "
            "full-text search library used in modern databases."
        ),
        "category": "Databases",
        "year": 2024,
        "topics": ["databases"],
        "weights": [1.0],
    },
    {
        "title": "Hybrid Search Strategies",
        "content": (
            "Hybrid search combines vector similarity with keyword-based full-text "
            "search for improved retrieval quality. Reciprocal Rank Fusion (RRF) "
            "merges ranked lists from different search modalities. This approach "
            "is critical for RAG pipelines where both semantic and lexical matches "
            "matter."
        ),
        "category": "AI",
        "year": 2024,
        "topics": ["machine_learning", "databases"],
        "weights": [0.5, 0.5],
    },
    {
        "title": "WebAssembly and the Future of Web",
        "content": (
            "WebAssembly enables near-native performance in web browsers. "
            "Languages like Rust, C++, and Go can compile to Wasm. This opens "
            "the door to running database engines and machine learning models "
            "directly in the browser."
        ),
        "category": "Web",
        "year": 2024,
        "topics": ["web_dev", "systems"],
        "weights": [0.6, 0.4],
    },
    {
        "title": "Distributed Systems Fundamentals",
        "content": (
            "Distributed systems coordinate multiple machines to achieve fault "
            "tolerance and scalability. Consensus algorithms like Raft and Paxos "
            "ensure consistency. CRDTs enable conflict-free replicated data types "
            "for eventual consistency without coordination."
        ),
        "category": "Systems",
        "year": 2023,
        "topics": ["systems", "databases"],
        "weights": [0.6, 0.4],
    },
]


# ---------------------------------------------------------------------------
# 3. Database setup
# ---------------------------------------------------------------------------

def create_document_db(db_path: str) -> uni_db.Uni:
    """Create a uni-db instance with a document store schema."""
    db = uni_db.UniBuilder.open(db_path).build()

    # Define the schema: Document nodes with scalar, vector, and text properties
    (
        db.schema()
        .label("Document")
        .property("title", "string")
        .property("content", "string")
        .property("category", "string")
        .property("year", "int")
        .vector("embedding", 64)
        # Scalar index on category for filtered queries
        .index("category", "btree")
        .done()
        .label("Category")
        .property("name", "string")
        .done()
        .edge_type("IN_CATEGORY", ["Document"], ["Category"])
        .done()
        .apply()
    )

    return db


def ingest_documents(db: uni_db.Uni) -> None:
    """Insert sample documents with embeddings into the database."""
    session = db.session()
    tx = session.tx()

    # Create category nodes
    categories = set(doc["category"] for doc in DOCUMENTS)
    for cat in categories:
        tx.execute("CREATE (:Category {name: $name})", {"name": cat})

    # Insert documents with embeddings
    for doc in DOCUMENTS:
        embedding = make_embedding(doc["topics"], doc["weights"])
        tx.execute(
            """
            CREATE (d:Document {
                title: $title,
                content: $content,
                category: $category,
                year: $year,
                embedding: $embedding
            })
            """,
            {
                "title": doc["title"],
                "content": doc["content"],
                "category": doc["category"],
                "year": doc["year"],
                "embedding": embedding,
            },
        )

    # Link documents to categories
    for doc in DOCUMENTS:
        tx.execute(
            """
            MATCH (d:Document {title: $title}), (c:Category {name: $cat})
            CREATE (d)-[:IN_CATEGORY]->(c)
            """,
            {"title": doc["title"], "cat": doc["category"]},
        )

    tx.commit()

    # Flush data to storage so indexes can be built on persisted data
    db.flush()


def create_indexes(db: uni_db.Uni) -> None:
    """Create vector and full-text indexes on the Document label."""
    # Vector index with cosine similarity
    db.schema().label("Document").index(
        "embedding", {"type": "vector", "metric": "cosine"}
    ).apply()

    # Full-text index on the content property
    db.schema().label("Document").index(
        "content", "fulltext"
    ).apply()

    # Rebuild indexes to make sure they cover all flushed data
    db.indexes().rebuild("Document", False)

    print("Indexes created: vector (cosine) on embedding, fulltext on content")


# ---------------------------------------------------------------------------
# 4. Search functions
# ---------------------------------------------------------------------------

def vector_search(session, query_embedding: list[float], k: int = 5):
    """Perform pure vector similarity search."""
    results = session.query(
        """
        CALL uni.vector.query('Document', 'embedding', $vec, $k)
        YIELD node, distance
        RETURN node.title AS title,
               node.category AS category,
               distance
        """,
        {"vec": query_embedding, "k": k},
    )
    return results


def fulltext_search(session, query_text: str, k: int = 5):
    """Perform pure BM25 full-text search."""
    results = session.query(
        """
        CALL uni.fts.query('Document', 'content', $text, $k)
        YIELD node, score
        RETURN node.title AS title,
               node.category AS category,
               score
        """,
        {"text": query_text, "k": k},
    )
    return results


def hybrid_search(
    session,
    query_text: str,
    query_embedding: list[float],
    k: int = 5,
    method: str = "rrf",
):
    """
    Perform hybrid search combining vector similarity and BM25 full-text search.

    The uni.search procedure fuses results from both modalities using either:
    - 'rrf' (Reciprocal Rank Fusion) — default, parameter-free
    - 'weighted' — alpha * vector_score + (1-alpha) * fts_score

    Arguments to uni.search:
      1. label  (string)
      2. properties  ({vector: '...', fts: '...'})
      3. query_text  (string) — used for FTS; may also auto-embed for vector if
         no explicit vector is supplied
      4. query_vector (list[float] | NULL)
      5. k  (int)
      6. filter  (string | NULL)
      7. options  (map | NULL)  — {method, alpha, rrf_k, over_fetch}
    """
    results = session.query(
        """
        CALL uni.search(
            'Document',
            {vector: 'embedding', fts: 'content'},
            $text,
            $vec,
            $k,
            NULL,
            $options
        )
        YIELD node, score, vector_score, fts_score
        RETURN node.title AS title,
               node.category AS category,
               score,
               vector_score,
               fts_score
        """,
        {
            "text": query_text,
            "vec": query_embedding,
            "k": k,
            "options": {"method": method},
        },
    )
    return results


def hybrid_search_with_filter(
    session,
    query_text: str,
    query_embedding: list[float],
    category_filter: str,
    k: int = 5,
):
    """Hybrid search with a pre-filter on category."""
    filter_expr = f"category = '{category_filter}'"
    results = session.query(
        """
        CALL uni.search(
            'Document',
            {vector: 'embedding', fts: 'content'},
            $text,
            $vec,
            $k,
            $filter,
            NULL
        )
        YIELD node, score
        RETURN node.title AS title,
               node.category AS category,
               score
        """,
        {
            "text": query_text,
            "vec": query_embedding,
            "k": k,
            "filter": filter_expr,
        },
    )
    return results


def hybrid_with_graph_traversal(session, query_text: str, query_embedding: list[float], k: int = 5):
    """Hybrid search followed by graph traversal to fetch related categories."""
    results = session.query(
        """
        CALL uni.search(
            'Document',
            {vector: 'embedding', fts: 'content'},
            $text,
            $vec,
            $k,
            NULL,
            NULL
        )
        YIELD node, score
        MATCH (node)-[:IN_CATEGORY]->(c:Category)
        RETURN node.title AS title,
               c.name AS category,
               score
        ORDER BY score DESC
        """,
        {
            "text": query_text,
            "vec": query_embedding,
            "k": k,
        },
    )
    return results


# ---------------------------------------------------------------------------
# 5. Display helpers
# ---------------------------------------------------------------------------

def print_header(title: str) -> None:
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")


def print_results(results, score_col: str = "score", extra_cols: list[str] | None = None) -> None:
    if len(results) == 0:
        print("  (no results)")
        return
    for i, row in enumerate(results, 1):
        score_val = row[score_col]
        score_str = f"{score_val:.4f}" if isinstance(score_val, float) else str(score_val)
        line = f"  {i}. [{row['category']}] {row['title']}  ({score_col}={score_str})"
        if extra_cols:
            extras = ", ".join(
                f"{c}={row[c]:.4f}" if isinstance(row[c], float) else f"{c}={row[c]}"
                for c in extra_cols
                if c in row
            )
            line += f"  [{extras}]"
        print(line)


# ---------------------------------------------------------------------------
# 6. Main
# ---------------------------------------------------------------------------

def main():
    with tempfile.TemporaryDirectory(prefix="uni_hybrid_search_") as tmp_dir:
        # --- Setup ---
        print("Setting up uni-db document store...")
        db = create_document_db(tmp_dir)
        ingest_documents(db)
        create_indexes(db)
        print(f"Ingested {len(DOCUMENTS)} documents into {tmp_dir}")

        session = db.session()

        # --- Query 1: "vector search in graph databases" ---
        query_text = "vector search in graph databases"
        query_embedding = make_embedding(["databases", "machine_learning"], [0.6, 0.4])

        print_header(f'Query: "{query_text}"')

        # Pure vector search
        print("\n  --- Vector Search (cosine similarity) ---")
        vec_results = vector_search(session, query_embedding)
        print_results(vec_results, score_col="distance")

        # Pure full-text search
        print("\n  --- Full-Text Search (BM25) ---")
        fts_results = fulltext_search(session, query_text)
        print_results(fts_results)

        # Hybrid search (RRF)
        print("\n  --- Hybrid Search (RRF fusion) ---")
        hybrid_results = hybrid_search(session, query_text, query_embedding, method="rrf")
        print_results(hybrid_results, extra_cols=["vector_score", "fts_score"])

        # --- Query 2: "machine learning pipelines and deployment" ---
        query_text_2 = "machine learning pipelines and deployment"
        query_embedding_2 = make_embedding(["machine_learning"], [1.0])

        print_header(f'Query: "{query_text_2}"')

        print("\n  --- Vector Search ---")
        vec_results_2 = vector_search(session, query_embedding_2)
        print_results(vec_results_2, score_col="distance")

        print("\n  --- Hybrid Search (RRF) ---")
        hybrid_results_2 = hybrid_search(session, query_text_2, query_embedding_2, method="rrf")
        print_results(hybrid_results_2, extra_cols=["vector_score", "fts_score"])

        # --- Query 3: Hybrid with category filter ---
        query_text_3 = "search and retrieval"
        query_embedding_3 = make_embedding(["databases"], [1.0])

        print_header(f'Query: "{query_text_3}" (filtered to Databases category)')

        hybrid_filtered = hybrid_search_with_filter(
            session, query_text_3, query_embedding_3, category_filter="Databases"
        )
        print_results(hybrid_filtered)

        # --- Query 4: Hybrid + Graph Traversal ---
        query_text_4 = "Rust memory safety systems programming"
        query_embedding_4 = make_embedding(["systems"], [1.0])

        print_header(f'Query: "{query_text_4}" (with graph traversal)')

        traversal_results = hybrid_with_graph_traversal(
            session, query_text_4, query_embedding_4
        )
        print_results(traversal_results)

        # --- Summary ---
        print_header("Summary")
        print(f"  Documents indexed: {len(DOCUMENTS)}")
        print(f"  Embedding dimensions: 64")
        print(f"  Vector metric: cosine")
        print(f"  FTS tokenizer: standard (BM25)")
        print(f"  Fusion method: Reciprocal Rank Fusion (RRF)")
        print()
        print("  Hybrid search combines the strengths of both approaches:")
        print("  - Vector search captures SEMANTIC similarity (meaning)")
        print("  - Full-text search captures LEXICAL matches (exact terms)")
        print("  - RRF fusion merges ranked lists without needing score normalization")

        db.shutdown()
        print("\nDone.")


if __name__ == "__main__":
    main()
