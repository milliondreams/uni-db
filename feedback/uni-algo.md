# uni-algo — Code Simplifier Review

Scope: `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-algo/`
(~10.2 kLOC across `src/algo`, `src/projection_input.rs`, `src/lib.rs`).

The crate splits cleanly into three layers: algorithm impls
(`src/algo/algorithms/*`), the projection layer
(`src/algo/projection.rs`, `id_map.rs`, `traversal.rs`), and the Cypher
procedure adapters (`src/algo/cypher/*`). The procedure-adapter layer
shows the heaviest duplication and is the highest-leverage area for
simplification.

---

## 1. Massive duplication across cypher procedure adapters (HIGH)

There are 35 adapter files in `src/algo/cypher/*` that almost all
follow the *same* shape:

```
impl GraphAlgoAdapter for FooAdapter { … }
pub type FooProcedure = GenericAlgoProcedure<FooAdapter>;
```

The duplication is mechanical and could be removed by a single
declarative macro. Concrete repeated micro-patterns:

### 1a. `map_result` — "score-per-node" mapper duplicated ~15×
**Effort: moderate**

Identical shape `(Vec<(Vid, f64-ish)>) → Vec<AlgoResultRow>` appears in:

- `src/algo/cypher/pagerank.rs:38-46`
- `src/algo/cypher/closeness.rs:32-40`
- `src/algo/cypher/harmonic_centrality.rs:30-38`
- `src/algo/cypher/eigenvector_centrality.rs:38-46`
- `src/algo/cypher/katz_centrality.rs:42-50`
- `src/algo/cypher/degree_centrality.rs:40-48`
- `src/algo/cypher/bellman_ford.rs:42-48`
- `src/algo/cypher/wcc.rs:32-40` (componentId)
- `src/algo/cypher/louvain.rs:38-46` (communityId)
- `src/algo/cypher/scc.rs:30-38`
- `src/algo/cypher/label_propagation.rs:39-…`
- `src/algo/cypher/betweenness.rs:36-…`
- `src/algo/cypher/kcore.rs:32-…`
- `src/algo/cypher/triangle_count.rs:33-…`
- `src/algo/cypher/cycle_detection.rs:33-…`

**Suggestion:** add a helper `fn rows_from_vid_pairs<T: Into<Value>>(it)
-> Vec<AlgoResultRow>` (or two helpers, one for ids+scalar, one for
"list of vids") in `procedure_template.rs` and call from `map_result`.
Drops ~7 lines × 15 files ≈ 100 LOC.

### 1b. `customize_projection` — "optional weight property at args[N]" duplicated ~10×
**Effort: trivial**

Verbatim repeated block:

```rust
fn customize_projection(mut builder: ProjectionBuilder, args: &[Value]) -> ProjectionBuilder {
    if let Some(prop) = args[N].as_str() {
        builder = builder.weight_property(prop);
    }
    builder
}
```

Found in: `bellman_ford.rs:51-56`, `bidirectional_dijkstra.rs:49-54`,
`k_shortest_paths.rs:59-64`, `eigenvector_centrality.rs:48-53`,
`katz_centrality.rs:52-57`, `diameter.rs:37-42`,
`graph_metrics.rs:58-63`, `mst.rs:45-50`, `ford_fulkerson.rs:45-50`,
`dinic.rs:45-50`.

**Suggestion:** expose
`procedure_template::weight_property_at(builder, args, idx)` (or
`GraphAlgoAdapter::weight_property_arg() -> Option<usize>` returning the
index, with default `None`, and delegate in the blanket
`customize_projection`).

### 1c. "List-of-VIDs row" mapper duplicated ~4×
**Effort: trivial**

`elementary_circuits.rs:33-44`, `maximal_cliques.rs:32-44`,
`random_walk.rs:57-68`, `all_simple_paths.rs:123-130` all do
`Vec<Vec<Vid>> -> rows with one List<u64> column`. Share helper.

### 1d. Declarative-macro option
**Effort: significant**

For ~25 of the 35 adapters, the entire body is 4 trivia values (name,
algo type, defaults, yields) plus a one-line `to_config`. A
`define_simple_adapter!(name = "uni.algo.pageRank", algo = PageRank, …)`
macro would collapse e.g. `pagerank.rs` (54 lines) and
`closeness.rs` (44 lines) to ~10 lines each. This is the largest
single LOC win available in the crate. Recommend after 1a/1b/1c are
applied so the macro only needs to handle the residual shape.

---

## 2. Deprecated / vestigial trait method (LOW-HANGING FRUIT)

`src/algo/procedure_template.rs:44-47`:

```rust
/// Deprecated: use customize_projection instead.
fn include_reverse() -> bool { true }
```

Still implemented in many adapters: `pagerank.rs:48-50`,
`louvain.rs:48-50`, `node_similarity.rs:59-61`,
`bipartite_check.rs:46-48`, `articulation_points.rs:40-42`,
`maximal_cliques.rs:46-48`, plus the default
`customize_projection` at `procedure_template.rs:40-42` already wires it
through. Either fully remove `include_reverse` and migrate the few
overrides to `customize_projection`, **or** delete the redundant `true`
overrides (they match the default).

**Effort: trivial** (delete-only).

---

## 3. Duplicated VID-parsing helpers across procedures

The `(Value -> Vid)` parser is rewritten multiple times with subtly
different semantics:

- `src/algo/cypher/shortest_path.rs:125-145` — `vid_from_value`
  (handles `"label:offset"` legacy format and `u64`).
- `src/algo/cypher/random_walk.rs:71-81` — `vid_from_value`
  (string parses via `Vid::parse`, no legacy format).
- `src/algo/cypher/astar.rs:55-65` — inline match on `Value::String`
  with `parse::<u64>().unwrap_or_default()` (lossy; silently uses 0).
- `src/algo/cypher/all_simple_paths.rs:51-61` — same pattern as astar,
  inline duplicated for `start_vid` and `end_vid`.
- `src/algo/cypher/bellman_ford.rs:33` — `args[0].as_u64().unwrap_or(0)`
  (also lossy default).
- `src/algo/cypher/k_shortest_paths.rs:39-40` — same `unwrap_or(0)`.
- `src/algo/cypher/bidirectional_dijkstra.rs:34-35` — same.
- `src/algo/cypher/ford_fulkerson.rs:34-35` — same.
- `src/algo/cypher/dinic.rs:34-35` — same.

**Issues:**
- Three different behaviours (legacy/strict/lossy) for what should be
  a single semantic.
- Several call sites silently map invalid input to vertex 0 — this is
  a correctness smell, not just style. A user mistyping a VID will get
  an algorithm run against node 0 instead of an error.

**Suggestion:** consolidate into a single
`procedures::vid_from_value(&Value) -> Result<Vid>` that handles
string/integer/legacy and returns a structured error; have adapters
use `?` in `to_config` (which currently returns `Self::Algo::Config`,
infallible — needs a small trait change to make `to_config` fallible,
or do the parsing in a new `parse_args` hook).

**Effort: moderate** (touches the `GraphAlgoAdapter` trait).

---

## 4. Dead / unused code

### 4a. Internal `Dijkstra` algorithm has no Cypher procedure (LOW)
`src/algo/algorithms/dijkstra.rs` defines `Dijkstra`,
`DijkstraConfig`, `DijkstraResult` (129 LOC) and is re-exported at
`src/algo/algorithms/mod.rs:38-39`, but **no cypher adapter or
registry entry refers to it** (only doc-comments mention it). Either
register a `DijkstraProcedure` (the public surface advertises
shortest-path coverage) or drop the standalone module — its
functionality is reachable through `bellman_ford`,
`bidirectional_dijkstra`, and `k_shortest_paths`.

**Effort: trivial** (deletion) or **moderate** (add adapter for parity).

### 4b. Hard-coded zero in dinic output (BUG-LIKE)
`src/algo/cypher/dinic.rs:39-43`:

```rust
fn map_result(result: …) -> Result<Vec<AlgoResultRow>> {
    Ok(vec![AlgoResultRow {
        values: vec![json!(result.max_flow), json!(0)],
    }])
}
```

The `yields()` advertises `"flowEdges"` (line 29) but the value is a
literal `0`. Either remove the column or wire it from the algorithm
result. **Effort: trivial.**

### 4c. Unused `_node_labels` / `_edge_types` fields
`src/algo/projection.rs:58-59`: `_node_labels`, `_edge_types` are
populated and never read. If genuinely unused, drop them; if intended
for debugging/serialization, add an accessor or delete. **Effort:
trivial.**

### 4d. `GraphAlgoAdapter::include_reverse()` (see §2).

---

## 5. Complex / hard-to-follow functions

### 5a. `DirectTraversal::shortest_path_with_hops` — confused control flow
`src/algo/traversal.rs:63-134`

The inner branch at lines 111-126 has dead/contradictory logic:

```rust
} else if new_depth < min_hops {
    // Path too short, but we found the shortest path
    // Since BFS finds shortest first, any other path will be longer
    // … For simplicity, return None if shortest is too short.
    return None;
} else {
    // Path too long (shouldn't happen since we stop at max_hops)
    return None;
}
```

`new_depth < min_hops` may be reachable for paths to `target` shorter
than `min_hops`, but the function then *fails to look for longer
paths* — silently returning `None` even though such paths could exist.
Either fix to honour `min_hops` (use `all_shortest_paths_with_hops` or
allow re-visits up to bound) or document the limitation in the
docstring instead of the body. The "shouldn't happen" arm is dead and
should be `unreachable!()` or removed. **Effort: moderate
(correctness + restructuring).**

### 5b. `ProjectionBuilder::collect_edges` — phased flow is hard to scan
`src/algo/projection.rs:353-458`

100+ lines with three loosely-coupled phases (topology collect / EID
dedup-and-batch-fetch / weighted-edge build), mixed `if let
(Some(pm), Some(prop)) = …` destructuring, and two near-identical
`out_edges` / `in_edges` `filter_map` blocks at lines 437-455.

**Suggestions:**
- Extract `fetch_weights(eids, prop) -> HashMap<Eid, f64>` and
  `weight_edges(raw, &id_map, &weights) -> WeightedEdgeList` helpers.
- The two `filter_map`s at 437-455 are identical; share a closure or
  small fn.
- The conditional EID extension at lines 408-415
  (`include_reverse.then(...).into_iter().flatten()`) is opaque —
  prefer an explicit `if include_reverse { all_eids.extend(...) }`
  before the dedup.

**Effort: moderate.**

### 5c. `astar.rs` — bespoke projection setup inlined
`src/algo/cypher/astar.rs:73-122`

This procedure manually builds its projection and loads heuristic
properties via `PropertyManager` (1000-row chunking). The chunking and
schema-label expansion (`labels.extend(edge_meta.dst_labels.clone());
labels.sort(); labels.dedup();`) duplicate logic in
`procedure_template::build_projection_from_direct_args`
(`procedure_template.rs:156-204`) and ad-hoc `PropertyManager`
construction in `projection.rs:389-397`.

The `1000` chunk constant also reappears in `projection.rs:394` —
extract a `const PROPERTY_BATCH_CHUNK: usize = 1000;`.

**Effort: moderate.**

### 5d. `shortest_path.rs` — single-result stream constructed awkwardly
`src/algo/cypher/shortest_path.rs:69-122`

`stream::once(...).filter_map(|res| match res { … })` to emit at most
one row is more complex than necessary. Use `async_stream::try_stream!
{ … if let Some(row) = … { yield row; } }` as elsewhere in the crate
(e.g. `astar.rs:73`, `all_simple_paths.rs:85`). Eliminates the
filter_map dance.

**Effort: trivial.**

---

## 6. Unnecessary abstractions / inconsistencies

### 6a. Two coexisting dispatch paths
`procedures.rs` defines both `execute_with_projection` (default
errors with `0x823`) and `execute_with_native_terminals` (default
errors with `0x824`), selected by `wants_native_terminals()`. The
`GenericAlgoProcedure` only implements the former; the four
"native-terminal" procedures (`shortest_path`, `astar`,
`all_simple_paths`, plus partial overlap in path-finders) each hand-roll
the latter.

The four hand-rolled procedures share a lot of boilerplate
(arg validation, VID parsing, projection build, label resolution).
A `NativeTerminalAdapter` trait analogous to `GraphAlgoAdapter` —
with associated `extract_terminals`, `build_projection`, `map_result`
methods — would let three of those four files shrink to ~30 lines and
move the duplicated VID/edge-type validation into one place.

**Effort: significant** (worthwhile if more native-terminal procs are
planned).

### 6b. `AlgorithmConfig` is dead/unused at the crate boundary
`src/algo/mod.rs:127-145` defines `AlgorithmConfig` with
`max_projection_memory`, `max_vertices`, `l0_warning_threshold`. It is
re-exported at `lib.rs:11` but never *read* inside this crate (no
field accesses, no enforcement). It is either:
- consumed externally (verify; if so add an internal doc comment), or
- aspirational and should be deleted until enforcement lands.

**Effort: trivial** (delete) or **moderate** (wire into
`ProjectionBuilder::build` to enforce `max_vertices` /
`max_projection_memory`).

### 6c. Inconsistent error-vs-default policy in `to_config`
Compare `pagerank.rs:30-36` (uses `.unwrap()` — panics on bad input)
vs `eigenvector_centrality.rs:31-36` (uses `.unwrap_or(default)` —
silently swallows). Both are post-`validate_args`, so unwraps are
"safe" — but the inconsistency obscures that. Pick one style and apply
crate-wide (recommend `unwrap()` post-validation; `unwrap_or` should
only be used where validation isn't possible, e.g. VID parsing where
the type allows multiple forms).

**Effort: trivial.**

### 6d. `vid.to_string()` for partition map key
`src/algo/cypher/bipartite_check.rs:34-39`

`partition_map.insert(vid.to_string(), …)` produces a string-keyed
JSON object instead of a `List<[vid, color]>` row stream, which is
the convention used by `wcc`/`louvain`/`scc`. The single-row map
output is inconsistent with the "one row per node" style and harder
for downstream Cypher to unnest. Consider switching to row-per-node
output.

**Effort: moderate** (API change — confirm with callers).

---

## 7. Other small wins

- `lib.rs:7-9`: `AlgoContext, AlgoProcedure, AlgoResultRow,
  ProcedureSignature, ValueType` are re-exported from
  `algo::procedures` but `algo::procedures` is itself `pub`. The
  re-exports at `lib.rs` create two stable paths for the same type;
  pick one. **Trivial.**
- `procedures.rs:230` ends with `// Placeholder procedure
  implementations will be added in Phase 3.3` — stale comment, the
  procedures now live in `cypher/*`. **Trivial.**
- `procedure_template.rs:99-125`: the comment "V2 entry point — `args[0]`
  and `args[1]` are placeholder empty arrays" describes an
  invariant that callers must honour but is not asserted. A `debug_assert!`
  that `args[0].as_array().map_or(true, |a| a.is_empty())` would make
  the contract explicit. **Trivial.**
- `random_walk.rs:48-54`: `return_param: 1.0, in_out_param: 1.0` are
  hard-coded — either expose them as args (the spec presumably
  intends node2vec parameters) or remove them from the config struct.
  **Trivial (or moderate if exposing).**
- `projection.rs:299-350` `collect_vertices`: the `.unwrap()` chain at
  lines 317-321 (column / downcast / batch-num-rows) will panic on
  any schema/scanner shape change. Convert to `Result` propagation.
  **Trivial.**
- `traversal.rs:432-474`: the `is_valid_path_length` helper is
  defined twice in two separate test fns — extract once.
  **Trivial.**

---

## Estimated impact

Applying §1a + §1b + §1c + §2 + §4 + §7 (all trivial/moderate) would
remove roughly **500–700 LOC of mechanical duplication** without
changing behaviour, and would centralise the patterns where future
bugs (e.g. the `dinic` `flowEdges` hard-coded zero) can be caught
once. The macro option in §1d on top would halve the cypher/* layer
again. §3 and §5a are the highest-value *correctness* items —
recommend prioritising them.
