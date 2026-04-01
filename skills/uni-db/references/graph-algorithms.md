# Graph Algorithms Reference

Uni includes **35+ graph algorithms** accessible via `CALL uni.algo.<name>(...)` procedures.

---

## 1. Algorithm Catalog

### Path Finding (7 algorithms)

| Procedure | Description | Use Case | Execution Mode |
|---|---|---|---|
| `uni.algo.shortestPath` | Single-source shortest path (unweighted BFS) | Routing, navigation | DirectTraversal |
| `uni.algo.bidirectionalDijkstra` | Bidirectional shortest path (weighted) | Faster point-to-point | GraphProjection |
| `uni.algo.bellmanFord` | Shortest path with negative weights | Financial arbitrage | GraphProjection |
| `uni.algo.astar` | A* with heuristic guidance | Spatial routing | GraphProjection |
| `uni.algo.kShortestPaths` | K distinct shortest paths | Alternative routes | GraphProjection |
| `uni.algo.allSimplePaths` | All simple paths between two nodes | Dependency analysis | DirectTraversal |
| `uni.algo.allPairsShortestPath` | Floyd-Warshall all pairs | Network diameter | GraphProjection |

### Centrality (7 algorithms)

| Procedure | Description | Use Case |
|---|---|---|
| `uni.algo.degreeCentrality` | In/out/total degree | Hub identification |
| `uni.algo.betweenness` | Shortest-path betweenness | Bridge nodes, bottlenecks |
| `uni.algo.closeness` | Average distance to all others | Information spread |
| `uni.algo.harmonicCentrality` | Harmonic centrality | Disconnected graphs |
| `uni.algo.eigenvectorCentrality` | Eigenvector centrality (iterative) | Influence measurement |
| `uni.algo.katzCentrality` | Katz centrality | Status in social networks |
| `uni.algo.pageRank` | PageRank (iterative) | Web ranking, importance |

### Community Detection (4 algorithms)

| Procedure | Description | Use Case |
|---|---|---|
| `uni.algo.wcc` | Weakly Connected Components (union-find) | Cluster identification |
| `uni.algo.scc` | Strongly Connected Components (Tarjan) | Cycle groups |
| `uni.algo.louvain` | Louvain modularity optimization | Community structure |
| `uni.algo.labelPropagation` | Label propagation (semi-synchronous) | Fast community detection |

### Similarity (1 algorithm)

| Procedure | Description | Use Case |
|---|---|---|
| `uni.algo.nodeSimilarity` | Neighborhood overlap (Jaccard/Cosine/Overlap) | Similar users/items |

### Structural (4 algorithms)

| Procedure | Description | Use Case |
|---|---|---|
| `uni.algo.triangleCount` | Count triangles per node | Clustering coefficient |
| `uni.algo.topoSort` | DAG topological ordering | Build systems, dependencies |
| `uni.algo.hasCycle` | Detect cycles | Deadlock detection |
| `uni.algo.isBipartite` | Bipartite graph verification | Two-coloring |

### Connectivity (3 algorithms)

| Procedure | Description | Use Case |
|---|---|---|
| `uni.algo.bridges` | Bridge edge detection | Network reliability |
| `uni.algo.articulationPoints` | Cut vertex detection | Single points of failure |
| `uni.algo.kCore` | K-core decomposition | Dense subgraph discovery |

### Flow & Matching (3 algorithms)

| Procedure | Description | Use Case |
|---|---|---|
| `uni.algo.fordFulkerson` | Maximum flow (Ford-Fulkerson) | Network capacity |
| `uni.algo.maxFlow` | Maximum flow (Dinic's algorithm) | Large flow networks |
| `uni.algo.maxMatching` | Maximum cardinality matching | Assignment problems |

### Miscellaneous (7 algorithms)

| Procedure | Description | Use Case |
|---|---|---|
| `uni.algo.mst` | Minimum spanning tree (Kruskal) | Network design |
| `uni.algo.randomWalk` | Random walk sampling | Graph embedding, sampling |
| `uni.algo.elementaryCircuits` | All elementary cycles | Circuit analysis |
| `uni.algo.maximalCliques` | Maximal clique enumeration | Dense groups |
| `uni.algo.graphColoring` | Graph coloring (greedy) | Scheduling, register allocation |
| `uni.algo.metrics` | Global metrics (diameter, radius, center) | Graph summary |
| `uni.algo.diameter` | Graph diameter | Network diameter |

---

## 2. General Execution Pattern

### CALL Syntax

All projection-based algorithms share a common first two arguments:

```cypher
CALL uni.algo.<name>(
    ['NodeLabel1', 'NodeLabel2'],   -- nodeLabels: List (required)
    ['REL_TYPE1', 'REL_TYPE2'],     -- relationshipTypes: List (required)
    <algorithm-specific args...>     -- optional, with defaults
)
YIELD col1, col2, ...
RETURN col1, col2
```

DirectTraversal algorithms (shortestPath, allSimplePaths) use positional arguments instead of nodeLabels/relationshipTypes.

### Execution Modes

**DirectTraversal** -- Zero-copy BFS on AdjacencyManager + L0Buffer. Used for single-source path queries. Fast startup, streaming results, no materialization.

**GraphProjection** -- Materialized dense CSR graph in memory. Used for iterative algorithms (PageRank, WCC, Louvain, betweenness, eigenvector, etc.). Limits: `max_projection_memory` (1 GB default), `max_vertices` (100M default).

---

## 3. Path Algorithms

### uni.algo.shortestPath

```
Args:     sourceNode (Node), targetNode (Node), relationshipTypes (List)
Yields:   nodeIds (List), edgeIds (List), length (Int)
Mode:     DirectTraversal
```

```cypher
CALL uni.algo.shortestPath($startId, $endId, ['ROAD'])
YIELD nodeIds, length
RETURN nodeIds, length
```

### uni.algo.bellmanFord

```
Args:     nodeLabels (List), relationshipTypes (List), sourceNode (Node),
          weightProperty (String, default: null)
Yields:   nodeId (Int), distance (Float)
Note:     Errors if negative cycle detected.
```

```cypher
CALL uni.algo.bellmanFord(['City'], ['ROUTE'], $sourceId, 'cost')
YIELD nodeId, distance
RETURN nodeId, distance
ORDER BY distance ASC
```

### uni.algo.astar

```
Args:     startNode (Node), endNode (Node), edgeType (String), heuristicProperty (String)
Yields:   path (List), cost (Float)
```

### uni.algo.kShortestPaths

```
Args:     nodeLabels (List), relationshipTypes (List), startNode (Node), endNode (Node),
          k (Int), weightProperty (String, default: null)
Yields:   path (List), cost (Float), rank (Int)
```

### uni.algo.allSimplePaths

```
Args:     startNode (Node), endNode (Node), relationshipTypes (List), maxLength (Int)
Optional: nodeLabels (List, default: null)
Yields:   path (List)
Mode:     DirectTraversal
```

### uni.algo.allPairsShortestPath

```
Args:     nodeLabels (List), relationshipTypes (List)
Yields:   sourceNodeId (Int), targetNodeId (Int), distance (Int)
```

---

## 4. Centrality Algorithms

### uni.algo.pageRank

```
Args:     nodeLabels (List), relationshipTypes (List)
Optional: dampingFactor (Float, default: 0.85), maxIterations (Int, default: 20),
          tolerance (Float, default: 1e-6)
Yields:   nodeId (Int), score (Float)
```

```cypher
CALL uni.algo.pageRank(['Person'], ['KNOWS'], 0.85, 20, 0.0001)
YIELD nodeId, score
RETURN nodeId, score
ORDER BY score DESC
LIMIT 10
```

### uni.algo.betweenness

```
Args:     nodeLabels (List), relationshipTypes (List)
Optional: normalize (Bool, default: true), samplingSize (Int, default: null)
Yields:   nodeId (Int), score (Float)
```

```cypher
CALL uni.algo.betweenness(['Person'], ['KNOWS'], true, 100)
YIELD nodeId, score
RETURN nodeId, score
ORDER BY score DESC
```

### uni.algo.degreeCentrality

```
Args:     nodeLabels (List), relationshipTypes (List)
Optional: direction (String: 'OUTGOING'|'INCOMING'|'BOTH', default: 'OUTGOING')
Yields:   nodeId (Int), score (Float)
```

### uni.algo.closeness

```
Args:     nodeLabels (List), relationshipTypes (List)
Optional: wassermanFaust (Bool, default: false)
Yields:   nodeId (Int), score (Float)
```

### uni.algo.eigenvectorCentrality

```
Args:     nodeLabels (List), relationshipTypes (List)
Optional: maxIterations (Int, default: 100), tolerance (Float, default: 1e-6),
          weightProperty (String, default: null)
Yields:   nodeId (Int), score (Float)
```

### uni.algo.katzCentrality

```
Args:     nodeLabels (List), relationshipTypes (List)
Optional: alpha (Float, default: 0.1), beta (Float, default: 1.0),
          maxIterations (Int, default: 100), tolerance (Float, default: 1e-6),
          weightProperty (String, default: null)
Yields:   nodeId (Int), score (Float)
```

### uni.algo.harmonicCentrality

```
Args:     nodeLabels (List), relationshipTypes (List)
Yields:   nodeId (Int), centrality (Float)
```

---

## 5. Community Detection

### uni.algo.wcc

```
Args:     nodeLabels (List), relationshipTypes (List)
Optional: minComponentSize (Int, default: 1)
Yields:   nodeId (Int), componentId (Int)
```

```cypher
CALL uni.algo.wcc(['Device'], ['CONNECTED_TO'])
YIELD nodeId, componentId
RETURN componentId, collect(nodeId) AS members
```

### uni.algo.louvain

```
Args:     nodeLabels (List), relationshipTypes (List)
Optional: resolution (Float, default: 1.0), maxIterations (Int, default: 10),
          minModularityGain (Float, default: 1e-4)
Yields:   nodeId (Int), communityId (Int)
```

```cypher
CALL uni.algo.louvain(['Person'], ['KNOWS'])
YIELD nodeId, communityId
RETURN communityId, count(*) AS size
ORDER BY size DESC
```

### uni.algo.labelPropagation

```
Args:     nodeLabels (List), relationshipTypes (List)
Optional: maxIterations (Int, default: 10), write (Bool, default: false),
          writeProperty (String, default: 'community')
Yields:   nodeId (Int), communityId (Int)
```

### uni.algo.scc

```
Args:     nodeLabels (List), relationshipTypes (List)
Yields:   nodeId (Int), componentId (Int)
```

---

## 6. Similarity

### uni.algo.nodeSimilarity

```
Args:     nodeLabels (List), relationshipTypes (List)
Optional: metric (String: 'JACCARD'|'COSINE'|'OVERLAP', default: 'JACCARD'),
          similarityCutoff (Float, default: 0.1), topK (Int, default: 10)
Yields:   node1 (Int), node2 (Int), similarity (Float)
```

```cypher
CALL uni.algo.nodeSimilarity(['User'], ['PURCHASED'], 'JACCARD', 0.3, 5)
YIELD node1, node2, similarity
RETURN node1, node2, similarity
ORDER BY similarity DESC
```

---

## 7. Structural & Connectivity

### uni.algo.triangleCount

```
Args:     nodeLabels (List), relationshipTypes (List)
Yields:   nodeId (Int), triangleCount (Int)
```

### uni.algo.topoSort

```
Args:     nodeLabels (List), relationshipTypes (List)
Yields:   nodeId (Int), order (Int)
Note:     Errors if graph has a cycle.
```

### uni.algo.hasCycle

```
Args:     nodeLabels (List), relationshipTypes (List)
Yields:   hasCycle (Bool), cycleNodes (List)
```

### uni.algo.isBipartite

```
Args:     nodeLabels (List), relationshipTypes (List)
Yields:   isBipartite (Bool), partition (Map)
```

### uni.algo.bridges

```
Args:     nodeLabels (List), relationshipTypes (List)
Yields:   source (Node), target (Node)
```

### uni.algo.articulationPoints

```
Args:     nodeLabels (List), relationshipTypes (List)
Yields:   node (Node)
```

### uni.algo.kCore

```
Args:     nodeLabels (List), relationshipTypes (List)
Optional: k (Int, default: null -- computes all core numbers)
Yields:   nodeId (Int), coreNumber (Int)
```

```cypher
CALL uni.algo.kCore(['Person'], ['KNOWS'])
YIELD nodeId, coreNumber
RETURN nodeId, coreNumber
ORDER BY coreNumber DESC
```

---

## 8. Flow & Matching

### uni.algo.fordFulkerson

```
Args:     nodeLabels (List), relationshipTypes (List), sourceNode (Node),
          sinkNode (Node), capacityProperty (String)
Yields:   maxFlow (Float)
```

### uni.algo.maxFlow

```
Args:     nodeLabels (List), relationshipTypes (List), sourceNode (Node),
          sinkNode (Node), capacityProperty (String)
Yields:   maxFlow (Float), flowEdges (Int)
Note:     Uses Dinic's algorithm. Preferred over fordFulkerson for large networks.
```

```cypher
CALL uni.algo.maxFlow(['Router'], ['LINK'], $sourceId, $sinkId, 'bandwidth')
YIELD maxFlow
RETURN maxFlow
```

### uni.algo.maxMatching

```
Args:     nodeLabels (List), relationshipTypes (List)
Yields:   node1 (Node), node2 (Node), matchId (Int)
```

---

## 9. Configuration & Tuning

### Graph Projection Options

Projection-based algorithms automatically build a CSR from the specified labels and types.

| Parameter | Effect |
|---|---|
| `nodeLabels` | Filter to nodes with these labels only |
| `relationshipTypes` | Filter to edges of these types only |
| `weightProperty` | Edge property to use as weight (bellmanFord, kShortestPaths, mst, eigenvector, katz) |
| `capacityProperty` | Edge property for flow capacity (fordFulkerson, maxFlow) |
| `heuristicProperty` | Node property for A* heuristic |

### Projection Limits

| Setting | Default | Description |
|---|---|---|
| `max_projection_memory` | 1 GB | Maximum memory for a single graph projection |
| `max_vertices` | 100M | Maximum number of vertices in a projection |

### Directed vs. Undirected

Most algorithms (pageRank, wcc, louvain, betweenness, triangleCount, bridges, etc.) include reverse edges automatically to treat the graph as undirected. Algorithms that are inherently directed (scc, topoSort, hasCycle) do not include reverse edges. `degreeCentrality` direction is controlled via the `direction` parameter.

---

## 10. Best Practices

### Algorithm Selection

| Need | Algorithm | Why |
|---|---|---|
| Find one shortest path | `shortestPath` | DirectTraversal, fastest |
| Weighted shortest path | `bellmanFord` or `bidirectionalDijkstra` | bellmanFord handles negative weights |
| Important nodes globally | `pageRank` | Iterative, captures transitive importance |
| Important nodes locally | `betweenness` | Identifies bottlenecks and bridges |
| Cluster membership | `wcc` | Fast, no parameters to tune |
| Community structure | `louvain` | Modularity-optimized, handles overlap |
| Fast community labels | `labelPropagation` | Linear time, good for large graphs |
| Network vulnerabilities | `bridges` + `articulationPoints` | Find single points of failure |
| Dense subgroups | `kCore` or `maximalCliques` | kCore for hierarchy, cliques for exact groups |
| Node-to-node similarity | `nodeSimilarity` | Jaccard/Cosine/Overlap metrics |

### Performance Tips

- **Use DirectTraversal for single-path queries.** shortestPath and allSimplePaths use zero-copy BFS -- much faster than projection-based alternatives for point queries.
- **Project only needed labels/types.** Smaller projection = faster execution + less memory.
- **Set iteration limits.** Always set `maxIterations` and `tolerance` on convergence algorithms (pageRank, eigenvector, katz, louvain) to prevent runaway computation.
- **Use samplingSize for betweenness.** On large graphs, approximate betweenness with `samplingSize` to avoid O(V*E) cost.
- **Prefer maxFlow over fordFulkerson.** Dinic's algorithm (maxFlow) is more efficient for large flow networks.

### Anti-Patterns

| Anti-Pattern | Problem | Solution |
|---|---|---|
| Full projection for a single path | Wastes memory materializing entire graph | Use `shortestPath` (DirectTraversal) |
| No convergence params on iterative algo | May not converge, wasted compute | Set `maxIterations` and `tolerance` |
| Running on unprojected graph | Processes irrelevant vertices/edges | Always specify `nodeLabels` and `edgeTypes` |
| Using allPairsShortestPath on large graphs | O(V^3) memory and time | Use single-source algorithms instead |

---

## 11. Examples

### Fraud Network Analysis

Identify suspicious accounts by combining centrality and community detection.

```cypher
// Step 1: Find connected fraud clusters
CALL uni.algo.wcc(['Account'], ['TRANSFERS_TO'])
YIELD nodeId, componentId
WITH componentId, collect(nodeId) AS members, count(*) AS size
WHERE size >= 3
RETURN componentId, members, size
ORDER BY size DESC
```

```cypher
// Step 2: Find broker accounts bridging clusters
CALL uni.algo.betweenness(['Account'], ['TRANSFERS_TO'], true, 500)
YIELD nodeId, score
RETURN nodeId, score
ORDER BY score DESC
LIMIT 20
```

### Social Network Community Detection

```cypher
// Detect communities with Louvain
CALL uni.algo.louvain(['Person'], ['KNOWS'], 1.0, 20)
YIELD nodeId, communityId
RETURN communityId, count(*) AS size
ORDER BY size DESC
LIMIT 10
```

```cypher
// Find influential members per community
CALL uni.algo.pageRank(['Person'], ['KNOWS'])
YIELD nodeId, score
RETURN nodeId, score
ORDER BY score DESC
LIMIT 50
```

### Infrastructure Dependency Analysis

```cypher
// Find single points of failure
CALL uni.algo.articulationPoints(['Service'], ['DEPENDS_ON'])
YIELD node
RETURN node AS criticalService
```

```cypher
// Find critical links whose removal disconnects the network
CALL uni.algo.bridges(['Service'], ['DEPENDS_ON'])
YIELD source, target
RETURN source, target AS criticalLink
```

```cypher
// Topological ordering for safe deployment
CALL uni.algo.topoSort(['Service'], ['DEPENDS_ON'])
YIELD nodeId, order
RETURN nodeId, order
ORDER BY order ASC
```
