# Guest-Authored Graph Algorithms

A graph algorithm is one of the plugin surfaces you can author in **any** loader — Rust, WASM Component Model, Extism, Rhai, or Python. Your code does not walk vertices. It **conducts**: it issues coarse kernel calls that each do `O(V+E)` or `O(batch)` work natively, and it holds only opaque integer handles to the results.

That constraint is what makes a sandboxed guest fast enough to ship. A per-element callback across a language boundary is one crossing per vertex; a coarse kernel is one crossing per *phase*. With batched kernels and handle-passing, a JIT'd WASM guest runs within ~1.3× of in-process Rust, and an *interpreted* Rhai guest is within a few percent of it — because the interpreter is not on the hot path at all.

For the mental model see [Concepts](concepts.md); for the capability boundary see [Trust & Capabilities](trust-and-capabilities.md).

---

## What you need

A guest-authored graph algorithm needs three grants and should declare two slices.

| Grant | Why |
| --- | --- |
| `Algorithm` | Register the algorithm so `CALL` can resolve it. |
| `GraphCompute` | Drive the kernels. |
| `HostQuery` | Project a graph from the store. Without it you can compute over a graph you were *handed*, but cannot project one yourself. |

**Slices** are versioned kernel families, checked at registration. Declaring them buys a clear load-time failure (`0x86A`, naming the slice) instead of a mysterious unknown-kernel error at call time.

| Slice | Provides |
| --- | --- |
| `graph-compute@1` | Read-only kernels over a projected graph. |
| `graph-arena@1` | Mutable structure your algorithm **grows** during the call. |

See [Reference → Capability slices](reference.md#capability-slices) for the declaration syntax.

---

## The two shapes

**Read a graph** (`graph-compute@1`) — you project a graph from the store and compute over it: PageRank, components, reachability, similarity. The graph is immutable and pinned to its projection-time snapshot, so concurrent writers cannot change it under you.

**Grow a graph** (`graph-arena@1`) — you build structure that does not exist in the store: a search tree, a residual network, an agent population. This is session-local and never observable by the store; it cannot write back.

The two compose. `arena_freeze` turns an arena into an ordinary graph handle, so the entire read-only kernel catalogue applies to structure you grew yourself.

---

## Kernel catalogue

59 kernels. Every one is reachable from every loader, except `graph` — sandboxed guests receive the graph handle in their invocation arguments rather than calling for it.

Operands are handles (opaque integers) and small scalars. **No vertex data crosses the boundary.**

### Graph shape and stored properties

| Kernel | Returns |
| --- | --- |
| `graph` | The bound graph handle (in-process loaders; sandboxed guests get it in their args). |
| `vertex_count` / `edge_count` | Scalar counts. |
| `degrees(g, dir)` | `[V]` degree map, `"out"` or `"in"`. |
| `vertex_ids(g)` | `[V]` map of each vertex's own slot id — the initializer for label-propagation-style algorithms. |
| `node_property(g, name)` | `[V]` column materialized from a stored vertex property. |
| `edge_property(g, name)` / `edge_weights(g)` | `[E]` columns in CSR out-edge order. |
| `edges_all(g)` | An edge mask selecting every edge. |

### Vertex sets

| Kernel | Returns |
| --- | --- |
| `frontier(g, seeds)` | A vertex set from external vertex ids. |
| `set_to_map(s, value)` / `map_to_set(m, pred, threshold)` | Lift and lower between sets and `[V]` maps. Predicates: `is_zero`, `gt`, `lt`, `eq`. |
| `set_union` / `set_diff` / `set_intersect` | Set algebra. |
| `set_len` / `is_empty` | Cardinality and emptiness. |

### Tensors and maps

| Kernel | Returns |
| --- | --- |
| `ewise(a, b, op, coef)` | Elementwise `mul` / `add` / `min` / `max` / `axpy`. |
| `zero_map(g)` | A zeroed `[V]` map. |
| `scatter(map, frontier, value)` | Write a scalar into the slots a vertex set selects. |
| `map_apply(m, op, a, b)` | Generic map: `recip`, `scale`, `log`, `affine`, `normalize_l1`, `normalize_l2`. |
| `recip` / `scale` / `normalize` | Fixed-form shorthands for the above. |
| `reduce_sum` / `reduce_sum_masked` | Deterministic sums (fixed-order, so bitwise-reproducible). |
| `arg_extreme(m, want_max)` | The extremal `(vertexId, value)`. |
| `topk(m, k)` | The ranked top-`k` pairs. |
| `l1_diff(a, b)` | Convergence metric for fixpoint loops. |

### Traversal

| Kernel | Returns |
| --- | --- |
| `expand(g, frontier, dir, exclude)` | Neighbor expansion, direction-optimized. `exclude` is a vertex set to skip — pass your visited mask to get a BFS step. |
| `spmv(g, m, semiring, dir)` | Sparse matrix-vector product over a named semiring: `reachability`, `shortest_path`, `propagate`, `linear_algebra`, `min_max`. |
| `expand_masked(g, frontier, dir, exclude, edge_mask)` / `spmv_masked(g, vec, semiring, edge_mask)` | The same, restricted to an edge mask — the result equals the kernel run on the subgraph of exactly the masked edges. |
| `next_bucket(dist, delta, bucket)` | Delta-stepping bucket selection. |

### Edge sets

| Kernel | Returns |
| --- | --- |
| `edge_set_len` | Cardinality of an edge mask. |
| `sample_edges(prob, seed, iter)` | Per-edge Bernoulli mask from a reproducible counter-hash; `prob` is an `[E]` tensor. |
| `edge_mask_window(vals, lo, hi)` | Threshold an `[E]` column into a mask — e.g. a temporal window. |
| `edge_intersect` / `edge_union` | Mask algebra. |
| `segmented_reduce(values, groups)` | Group a `[V]` map by a label map and reduce, **bitwise-identical regardless of vertex order or partitioning**. |

Together these express reachability or SpMV over a random edge subset (percolation, influence maximization) or a per-event-time subset (temporal reachability) — with no per-element guest code.

### Walks and sampling

| Kernel | Returns |
| --- | --- |
| `random_walks(g, seeds, len, n, p, q, seed)` | node2vec-biased walks. |
| `walk_visit_counts(walks, g)` | `[V]` visit histogram. |
| `sample(prob, seed, iter)` | Reproducible `Bernoulli(prob[v])` mask. Same counter-hash stream as walk seeding, so results are bitwise-identical across runs and threads; a fresh `iter` decorrelates. |
| `emit_walks(walks)` | Emit ragged walk rows. |

### Similarity

| Kernel | Returns |
| --- | --- |
| `neighborhood_overlap(g, source, metric)` | Overlap of `source` against its neighbourhood. Metrics: `count`, `jaccard`, `overlap`, `cosine`, `adamic_adar`. |
| `all_pairs_overlap(g, metric, pair_mode, k)` | `pair_mode` is `"adjacent"` or `"topk"`. |
| `emit_pairs(pairs)` | Emit `(srcId, dstId, value)` rows. |

### Mutable arena (`graph-arena@1`)

| Kernel | Returns |
| --- | --- |
| `arena_new(capacity, branching)` | An arena sized for `capacity` slots with `branching` child slack. |
| `arena_alloc(a, count)` | Bump-allocate slots; returns their ids. |
| `arena_expand(a, parents, fanout)` | Allocate `fanout` children per parent **and link them** — the tree-growth primitive. |
| `arena_link(a, parents, kids)` | Link explicit parent/child pairs. |
| `arena_column(a)` | A zeroed `[capacity]` `f64` state column; returns its index. Call it repeatedly — a search node needs *both* visits and value. |
| `arena_candidates(a, roots)` | The children of a frontier, concatenated. |
| `arena_gather(a, col, slots)` / `arena_scatter(a, col, slots, values)` | Move state between a column and a compact tensor. |
| `arena_descend(a, roots, score, visit, maximize, vloss)` | Descend to a leaf choosing the best-scoring child, applying one visit and the virtual loss at every step. |
| `arena_freeze(a)` | Compact into an ordinary graph handle. |

### Egress and lifecycle

| Kernel | Returns |
| --- | --- |
| `emit(name, handle)` | Emit a result column. |
| `free(h)` | Release a handle. **In a loop, free your intermediates** — the handle cap is a real bound, and a leaky guest hits `0x864` within a few hundred iterations. |

---

## Worked example: a guest-authored MCTS

This Rhai script owns the search. The tree shape, exploration constant, score formula and stopping rule are all the guest's; the host only runs the loops that do bulk work.

```rust
let arena = sess.arena_new(8191, 2);
let root  = sess.arena_alloc(arena, 1);

let visits = sess.arena_column(arena);
let value  = sess.arena_column(arena);
let score  = sess.arena_column(arena);

// Grow a binary tree, level by level.
let frontier = root;
for d in 0..12 {
    frontier = sess.arena_expand(arena, frontier, 2);
}

let c = 1.41;
let n = 0;
while n < 4096 {
    // Score only the candidates, not all 8191 slots.
    let cand = sess.arena_candidates(arena, root);
    let v    = sess.arena_gather(arena, visits, cand);
    let w    = sess.arena_gather(arena, value, cand);

    // UCB, composed by the guest from ordinary kernels.
    let inv  = sess.recip(v);
    let mean = sess.ewise(w, inv, "mul", 0.0);
    let expl = sess.scale(inv, c * c);
    let ucb  = sess.ewise(mean, expl, "add", 0.0);
    sess.arena_scatter(arena, score, cand, ucb);

    let leaves = sess.arena_descend(arena, root, score, visits, true, 0.35);

    sess.free(cand); sess.free(v);    sess.free(w);
    sess.free(inv);  sess.free(mean); sess.free(expl);
    sess.free(ucb);  sess.free(leaves);
    n += 1;
}
sess.arena_freeze(arena)   // now an ordinary graph — run PageRank on it if you like
```

Two details that matter more than they look:

**Score the candidates, not the column.** `arena_candidates` returns only the children of the active frontier. Rescoring all `N` slots each level instead costs about **6×** more, because it does `O(N)` work where only `O(frontier × branching)` entries can possibly be chosen.

**The virtual loss is not optional.** `arena_descend`'s `vloss` is applied *inside* the descent loop, not after the batch. Without it every rollout in a batch descends against identical statistics and follows the same path — in measurement, a 1024-rollout batch collapsed onto **16 distinct leaves**. With it, 1024.

---

## Determinism, budgets, and failure

**Deterministic.** CSR ordering is canonical and reductions are fixed-order, so results are bitwise-reproducible across thread counts. Sampling draws from a stateless counter-hash, so a seeded run repeats exactly. `arena_descend` breaks ties by lower slot.

Batching is *not* semantically free: rollouts in one batch descend against the same statistics a sequential search would have updated. That is a declared property, and the virtual-loss idiom above is how search workloads handle it.

**Budgets are fail-closed.** A native-work meter (`GraphComputeWork`) and a handle-memory cap (`GraphComputeArenaBytes`) both halt a runaway guest. Batched kernels charge the *full* work they do — batching amortizes the boundary crossing, never the meter, so you cannot evade the budget by batching harder.

**Non-convergence is a hard error**, never a silent partial result.

| Code | Meaning |
| --- | --- |
| `0x860` / `0x861` / `0x863` | Stale handle / wrong kind / wrong session. |
| `0x862` | Operand shape mismatch. |
| `0x864` | Handle-memory or arena capacity exceeded. |
| `0x865` | Native-work budget exhausted. |
| `0x866` / `0x867` | Iteration limit / wall-clock deadline. |
| `0x86A` | A declared capability slice the host does not provide. |
| `0x86C` | Kernel called without the `GraphCompute` grant. |
| `0x86E` | Invalid argument, unknown kernel name, or out-of-range slot. |

---

## Loader notes

| Loader | Kernel surface |
| --- | --- |
| **Rhai** / **PyO3** | Native per-kernel methods on a session object. Handles are plain integers — no marshalling. |
| **WASM Component Model** | Two imports: `host-graph` (one generic JSON entrypoint carrying the whole catalogue) and `host-arena` (typed functions carrying handles directly, for the hot path). |
| **Extism** | One host function, `uni_graph_call`, carrying the catalogue as JSON. |

A guest built before `host-arena` existed is unaffected by it — the interface is additive, and a component that does not import it links exactly as before.

The kernel catalogue is a *type* in the host, not a list of strings: adding a kernel fails to compile until it is dispatched, and a per-loader test asserts every kernel is exposed by Rhai and PyO3. A kernel cannot be callable from one loader and silently invisible to another.
