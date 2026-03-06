# Uni

## The Embedded Graph Database for Cypher, Algorithms, and Logic

Uni combines **property graph queries**, **vector search**, **graph algorithms**, and **Locy reasoning** in one embedded engine.

<div class="quick-links" markdown>
<a href="getting-started/installation.html" class="quick-link">Install</a>
<a href="getting-started/quickstart.html" class="quick-link">Quick Start</a>
<a href="guides/cypher-querying.html" class="quick-link">Cypher Guide</a>
<a href="locy/index.html" class="quick-link">Locy</a>
<a href="https://github.com/rustic-ai/uni" class="quick-link">GitHub</a>
</div>

---

## What Makes Uni Shine

| Strength | Why it matters |
|---|---|
| **100% OpenCypher compatibility (open suite)** | Use familiar Cypher semantics and migrate existing query logic with confidence. |
| **Uni Cypher capabilities beyond baseline OpenCypher** | `WITH RECURSIVE`, window functions (`OVER`), temporal queries (`VERSION AS OF` / `TIMESTAMP AS OF`, `VALID_AT`), and admin procedures are built in. |
| **36 built-in graph algorithms** | Run centrality, community, pathfinding, flow, and structural analysis directly via `CALL algo.*`. |
| **Vectorized columnar execution** | Arrow/DataFusion-backed execution accelerates scans, filters, projections, and aggregations. |
| **Hybrid vector + FTS search** | Combine semantic vector retrieval and BM25 full-text ranking with rank-fusion in `uni.search`. |
| **Schema-first model (recommended)** | Typed schema + constraints + indexes gives the most predictable performance and correctness. |
| **Locy (Logic + Cypher)** | Add recursive rules, hypothetical reasoning (`ASSUME`), remediation search (`ABDUCE`), derivation (`DERIVE`), and explainability (`EXPLAIN RULE`). |
| **One engine for graph + vector + document + analytics** | Avoid stitching multiple systems for traversal, ANN search, text search, and analytical queries. |
| **Embedded deployment model** | No separate database service required; run in-process with predictable operational footprint. |

---

## Cypher, Extended

Uni supports full day-to-day Cypher workflows plus advanced capabilities that usually require separate systems:

- Recursive querying with `WITH RECURSIVE` CTEs.
- Window analytics (`ROW_NUMBER`, `RANK`, `LAG`, `SUM OVER`, and more).
- Time-travel and snapshot-aware reads.
- Built-in procedures for algorithms, schema introspection, storage ops, and search.

```cypher
MATCH (u:User)-[:PURCHASED]->(p:Product)
WITH u, p, ROW_NUMBER() OVER (PARTITION BY u.id ORDER BY p.ts DESC) AS rn
WHERE rn <= 5
RETURN u.id, p.name, rn
ORDER BY u.id, rn
```

See: [Cypher Querying](guides/cypher-querying.md) | [Window Functions](features/window-functions.md) | [Snapshots & Time Travel](features/snapshots-time-travel.md)

---

## Vectorized Analytics and Hybrid Retrieval

Uni is designed to combine graph traversals with fast analytical processing and modern retrieval patterns:

- Columnar, vectorized query execution for large scans and aggregations.
- Native vector indexing plus full-text/JSON indexing.
- Unified hybrid search (`uni.search`) with reciprocal-rank fusion or weighted fusion.

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

See: [Columnar Analytics](features/columnar-analytics.md) | [Vector Search](features/vector-search.md) | [Hybrid Search](features/hybrid-search.md)

---

## Algorithms at Query Time

Uni exposes 36 algorithms via Cypher so you can compute graph intelligence where your data already lives.

- Centrality: PageRank, betweenness, closeness, eigenvector, Katz
- Community: Louvain, label propagation, WCC, SCC, k-core
- Paths: Dijkstra, A*, k-shortest paths, APSP
- Structural + flow: articulation points, cycle detection, MST, max flow

```cypher
CALL algo.pageRank(['Paper'], ['CITES'])
YIELD nodeId, score
RETURN nodeId, score
ORDER BY score DESC
LIMIT 10
```

See: [Graph Algorithms](features/graph-algorithms.md)

---

## Locy: Logic Programming for Graphs

When plain pattern matching is not enough, Locy adds a logic layer on top of Cypher.

### Locy enables

- Rule definitions with recursion (`CREATE RULE`)
- Goal-directed execution (`QUERY`)
- What-if simulations with rollback boundaries (`ASSUME ... THEN`)
- Suggested interventions (`ABDUCE`)
- Explainable derived facts (`EXPLAIN RULE`)

### Example use cases

- Policy and access-control reasoning (deny/allow precedence)
- Compliance remediation planning
- Infrastructure blast radius analysis
- Supply chain provenance and risk propagation

Start here: [Locy Overview](locy/index.md) | [Locy Foundations](locy/foundations.md) | [Locy Use Cases](locy/use-cases.md)

---

## Search and AI Workloads

Uni natively combines graph traversal with semantic and lexical retrieval:

- Vector indexes (`HNSW`, `IVF_PQ`, `Flat`)
- Full-text and JSON-path search
- Hybrid search with rank fusion (`uni.search`)
- Graph-aware retrieval for RAG and recommender systems

See: [Vector Search](guides/vector-search.md) | [Hybrid Search](features/hybrid-search.md)

---

## Schema-First Is Recommended

Uni supports schemaless overflow properties, but production systems should define a typed schema for core fields and indexes.

- Better query planning and execution speed.
- Stronger data correctness with constraints.
- Clearer evolution path as workloads grow.

Recommended path: start with a minimal typed schema, then add indexes and constraints around real query patterns.

See: [Schema & Indexing](features/schema-indexing.md) | [Schema Design](guides/schema-design.md)

---

## Choose Your Starting Path

### New to Uni

1. [Installation](getting-started/installation.md)
2. [Quick Start](getting-started/quickstart.md)
3. [Programming Guide](getting-started/programming-guide.md)

### Building with Cypher

1. [Cypher Querying](guides/cypher-querying.md)
2. [Schema Design](guides/schema-design.md)
3. [Data Ingestion](guides/data-ingestion.md)

### Building with Locy

1. [Locy Overview](locy/index.md)
2. [Language Guide](locy/language-guide.md)
3. [Advanced Locy Features](locy/advanced/along-fold-bestby.md)
4. [Locy Internals & TCK](internals/locy/tck.md)

---

## Core Documentation

### Getting Started

- [Installation](getting-started/installation.md)
- [Quick Start](getting-started/quickstart.md)
- [CLI Reference](getting-started/cli-reference.md)

### Features

- [OpenCypher Querying](features/cypher-querying.md)
- [Graph Algorithms](features/graph-algorithms.md)
- [Window Functions](features/window-functions.md)
- [Vector Search](features/vector-search.md)
- [Hybrid Search](features/hybrid-search.md)
- [Snapshots & Time Travel](features/snapshots-time-travel.md)

### Advanced

- [Concepts](concepts/index.md)
- [Internals](internals/index.md)
- [Reference](reference/index.md)
- [Examples (Rust/Python/Notebooks)](examples/index.md)
