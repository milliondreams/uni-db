# Uni

## One engine for graph queries, vector search, text retrieval, analytics, and reasoning.

Uni is an **embedded graph database** backed by object storage. It replaces the multi-system stack of a graph DB, a vector store, a search index, and an analytics engine with a single library you link into your application.

<div class="quick-links" markdown>
<a href="getting-started/installation/" class="quick-link">Install</a>
<a href="getting-started/quickstart/" class="quick-link">Quick Start</a>
<a href="guides/cypher-querying/" class="quick-link">Cypher Guide</a>
<a href="locy/index/" class="quick-link">Locy</a>
<a href="https://github.com/rustic-ai/uni-db" class="quick-link">GitHub</a>
</div>

---

## Why Uni?

Today, answering questions about connected data means stitching together four or five systems: a graph database for traversals, a vector store for semantic search, a text index for keyword retrieval, a columnar engine for analytics, and custom application code to glue them together. Each system has its own data model, consistency boundary, and operational overhead.

Uni collapses that stack into one embedded library. Your data lives in object storage (S3, GCS, or local disk), queries run in-process, and graph traversals, vector search, full-text retrieval, analytics, and logic-based reasoning all execute against the same data without ETL pipelines or cross-system joins.

---

## Full Cypher + 36 Graph Algorithms

Run day-to-day Cypher queries alongside capabilities that usually require separate systems:

- **Recursive CTEs** (`WITH RECURSIVE`) for arbitrary-depth traversals
- **Window functions** (`ROW_NUMBER`, `RANK`, `LAG`, `SUM OVER`) over graph results
- **Time travel** — query any historical snapshot with `VERSION AS OF` or `TIMESTAMP AS OF`
- **36 built-in algorithms** — PageRank, Louvain, Dijkstra, betweenness centrality, k-shortest paths, and more via `CALL algo.*`
- **Vectorized execution** — Arrow/DataFusion-backed scans, filters, and aggregations

```cypher
MATCH (u:User)-[:PURCHASED]->(p:Product)
WITH u, p, ROW_NUMBER() OVER (PARTITION BY u.id ORDER BY p.ts DESC) AS rn
WHERE rn <= 5
RETURN u.id, p.name, rn
ORDER BY u.id, rn
```

[Cypher Querying](guides/cypher-querying.md) | [Graph Algorithms](features/graph-algorithms.md) | [Window Functions](features/window-functions.md) | [Snapshots & Time Travel](features/snapshots-time-travel.md)

---

## Multi-Modal Search

Combine semantic, lexical, and structured search in a single query:

- **Vector indexes** — HNSW, IVF_PQ, and flat indexes with auto-embedding support
- **Full-text search** — BM25 ranking over text fields
- **JSON-path search** — query nested document properties
- **Hybrid search** — reciprocal-rank or weighted fusion across vector and text results via `uni.search`

```cypher
CALL uni.search(
  'Document',
  {vector: 'embedding', fts: 'content'},
  'graph anomaly detection',
  null,
  10
)
YIELD node, score, vector_score, fts_score
RETURN node.title, score, vector_score, fts_score
ORDER BY score DESC
```

[Vector Search](guides/vector-search.md) | [Hybrid Search](features/hybrid-search.md) | [Full-Text & JSON Search](features/full-text-json-search.md)

---

## Locy: Logic Programming for Graphs

Locy extends Cypher with recursive rules, hypothetical reasoning, and explainability — answering questions that pattern matching alone cannot:

*"What breaks if the auth service goes down?" "Why was this user denied access?" "What needs to change for compliance?"*

- **Recursive rules** — define transitive relationships once, query them at any depth
- **What-if simulation** — `ASSUME ... THEN` evaluates hypotheticals in a rollback boundary
- **Remediation search** — `ABDUCE` finds the minimal changes needed to achieve a goal
- **Explainability** — `EXPLAIN RULE` returns the full derivation tree proving a result
- **Goal-directed execution** — `QUERY` evaluates rules top-down with tabling (SLG resolution)

```cypher
-- Define transitive reachability
CREATE RULE reachable AS
MATCH (a:Service)-[:DEPENDS_ON]->(b:Service)
YIELD KEY a, KEY b

CREATE RULE reachable AS
MATCH (a:Service)-[:DEPENDS_ON]->(mid:Service)
WHERE mid IS reachable TO b
YIELD KEY a, KEY b

-- What breaks if the auth service goes down?
ASSUME {
  MATCH (s:Service {name: 'auth-service'})
  SET s.status = 'DOWN'
} THEN {
  QUERY reachable
  WHERE a.name = 'auth-service'
  RETURN b.name AS affected_service
}
```

**Use cases:** access control, compliance auditing, blast-radius analysis, supply-chain provenance, fraud-risk propagation.

[Locy Overview](locy/index.md) | [Language Guide](locy/language-guide.md) | [Use Cases](locy/use-cases.md)

---

## Embedded and Self-Managing

No server, no cluster — just a library. Uni runs in-process and manages its own storage:

- **In-process execution** — link as a Rust crate or Python package, no network round-trips
- **Object-store backed** — S3, GCS, Azure, or local disk with automatic local caching
- **Automatic compaction** — semantic compaction runs in the background, no manual tuning
- **Index lifecycle** — indexes are created, built, and maintained automatically
- **Single-writer, multi-reader** — snapshot isolation for concurrent reads, no lock contention

[Architecture](concepts/architecture.md) | [Storage Engine](internals/storage-engine.md) | [Performance Tuning](guides/performance-tuning.md)

---

## Performance

Indicative numbers from internal benchmarks. See the [Benchmarks](internals/benchmarks.md) doc for methodology.

| Operation | Latency |
|---|---|
| Point lookup (indexed) | 2–5 ms |
| 1-hop traversal (cached adjacency) | 4–8 ms |
| Vector KNN, k=10 | 1–3 ms |
| Aggregation over 1M rows | 50–200 ms |
| Batch insert (10K nodes) | 5–10 ms |

[Performance Tuning](guides/performance-tuning.md) | [Benchmarks](internals/benchmarks.md)

---

## Get Started

1. [Install Uni](getting-started/installation.md)
2. [Quick Start](getting-started/quickstart.md) — create a graph, run queries, and search in five minutes
3. [Programming Guide](getting-started/programming-guide.md) — Rust and Python APIs in depth

---

## Explore

### Guides

- [Cypher Querying](guides/cypher-querying.md)
- [Schema Design](guides/schema-design.md)
- [Data Ingestion](guides/data-ingestion.md)
- [Vector Search](guides/vector-search.md)
- [Pydantic OGM](guides/pydantic-ogm.md)

### Locy

- [Locy Overview](locy/index.md)
- [Language Guide](locy/language-guide.md)
- [Advanced Features](locy/advanced/along-fold-bestby.md)
- [Use Cases](locy/use-cases.md)

### Reference

- [Concepts](concepts/index.md)
- [Internals](internals/index.md)
- [API Reference](reference/index.md)
- [Examples (Rust / Python / Notebooks)](examples/index.md)
