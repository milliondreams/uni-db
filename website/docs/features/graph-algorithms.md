# Graph Algorithms

Uni includes a built-in graph algorithm library exposed through Cypher procedures. Use it to compute centrality, clustering, paths, and connectivity without exporting data.

## What It Provides

- Centrality: PageRank, betweenness, closeness, eigenvector, Katz.
- Connectivity: WCC, SCC, bridges, articulation points.
- Paths: shortest paths, k-shortest, A*, BFS/DFS.
- Community detection: Louvain, label propagation.

## Example

=== "Rust"
    ```rust
    use uni_db::Uni;

    # async fn demo() -> Result<(), uni_db::UniError> {
    let db = Uni::open("./my_db").build().await?;
    let session = db.session();

    let rows = session.query(
        "CALL uni.algo.pageRank() YIELD nodeId, score RETURN nodeId, score ORDER BY score DESC LIMIT 10"
    ).await?;

    println!("{:?}", rows);
    # Ok(())
    # }
    ```

=== "Python"
    ```python
    import uni_db

    db = uni_db.Uni.open("./my_db")
    session = db.session()
    rows = session.query(
        "CALL uni.algo.pageRank() YIELD nodeId, score RETURN nodeId, score ORDER BY score DESC LIMIT 10"
    )
    print(rows)
    ```

## Use Cases

- Identify important nodes in a graph.
- Detect communities and clusters.
- Compute routing or similarity metrics.

## Use from Locy `FEATURES`

Ten of these algorithms are also callable as Locy feature expressions inside `CREATE MODEL`, so a classifier can consume topology directly without a separate feature pipeline:

| Function | Returns |
|---|---|
| `degree_centrality(n)` | Float64 |
| `pagerank_score(n)` | Float64 |
| `closeness_centrality(n)` | Float64 |
| `betweenness_centrality(n)` | Float64 |
| `eigenvector_centrality(n)` | Float64 |
| `harmonic_centrality(n)` | Float64 |
| `katz_centrality(n)` | Float64 |
| `avg_neighbor(n, 'prop')` | Float64 — mean of `prop` over neighbors |
| `max_neighbor(n, 'prop')` | Float64 |
| `sum_neighbor(n, 'prop')` | Float64 |

```cypher
CREATE MODEL risk_scorer AS
  INPUT (n)
  FEATURES degree_centrality(n), pagerank_score(n), avg_neighbor(n, 'risk')
  OUTPUT PROB risk
  USING xervo('classify/risk-v1')
```

See [Neural Predicates](../locy/advanced/neural-predicates.md) for the full reference.

## When To Use

Use built-in algorithms when you need graph analytics without exporting data or building your own pipeline.
