# uni-db 3.2.0

**Release focus: making a guest algorithm a first-class citizen, and paying down the honesty debt
underneath it.** The headline is the **Plugin Compute ABI** — a guest algorithm can now *choose and
shape* the graph it sees, *grow structure that does not exist in the store* through a metered
mutable arena, and be told at a typed boundary when it mixes two graphs up. Underneath it, two
large sweeps: **lancedb is gone** (the backend is built directly on Lance, and `lance::Dataset` no
longer appears anywhere outside `uni-store`), and a **structured `FilterExpr`** replaces
string-SQL predicates from the planner to the backend — which deleted a live wrong-answer bug on
the way. A 15-item **correctness sweep** closed seven silent-wrong-answer / data-loss defects, and
the published Python wheel dropped from **536 MiB to 238 MiB**.

This release covers everything since **3.0.0**: **150 commits**, ~47,500 insertions across 304
files. Version bumped across the Rust workspace and the Python packages (`uni-db`, `uni-pydantic`)
to **3.2.0**. The intermediate `3.0.2` and `3.1.0` version bumps were never tagged, so everything
they carried is folded in here.

Gates at the release commit: the **6,174-test workspace suite** green, plus `fmt`,
`clippy -D warnings` and `rustdoc -D warnings`. Conformance gates reported across the sweep:
openCypher TCK **3925/3925**, Locy TCK **502/502**, `uni-db` Python **923 passed**. The release
commit additionally built the `uni-db` wheel through the `release-wheels.yml` recipe
(manylinux_2_28, `--profile dist`) and round-tripped a `CREATE`/`MATCH` in a clean venv, confirming
the dynamic-version chain end to end.

---

## ⚠️ Breaking changes

### Packaging — published wheels no longer carry the WASM loaders

To fit the wheel under PyPI's 100 MB limit, `bindings/uni-db` no longer enables the WASM loaders by
default. Published wheels therefore **no longer expose `Database.load_wasm_component` or
`Database.load_wasm_extism`**.

Both loaders had to go together: `extism` depends on `wasmtime` on its own path, independent of
`uni-plugin-wasm`, so dropping only `wasm-plugins` reclaims nothing. With both off, `wasmtime`,
`cranelift`, `wasi-common` and `extism` leave the dependency graph entirely. No first-party plugin
uses either loader.

**Migration:** build from source with `--features wasm-plugins,extism-plugins`. The Rhai and PyO3
loaders are unaffected and remain in every published wheel.

Also in this change: `strip = "symbols"` on `[profile.release]` means **panic backtraces from
release builds are unsymbolicated**. Panic *messages* still reach Python through PyO3 — only frame
names are lost.

### GraphCompute — projections must be scoped, and values carry an index space

Six breaking commits harden the GraphCompute contract. Together they convert a class of plausible
wrong answers into typed errors.

- **An unscoped projection is now an error** (`b4cda5ca3`). A guest must name `nodeLabels` /
  `edgeTypes`, or opt in explicitly with `{projectAll: true}`. `projectAll` itself now fails loud
  naming any undeclared schemaless label rather than quietly projecting a partial graph. The
  first-party `uni.algo.gc*` providers opt in automatically.
- **A detached L0 tier is refused** (`6d279ff75`). `ProjectionBuilder::build` rejects a detached L0
  tier on a live storage view — pass the query's `L0Manager`, or opt in with
  `allow_detached_l0(true)`. In the same change, property and weight names that resolve nowhere in
  scope are rejected instead of yielding NaN columns or silently defaulted `1.0` weights.
- **Tensors, sets, walks and pair lists now carry an index space** (`68e873aee`, `774a26bcf`,
  `c4d4ddac8`, `40e806923`). Every value tracks an `Origin` — `Graph(handle)`, `Arena(handle)`, or
  `Untracked` (which unifies with anything). Mixing spaces raises `0x862`. Concretely:
  `ewise` / `segmented_reduce` require matching shape *and* index space; `scatter` rejects an `[E]`
  map; set algebra requires equal capacities and same-projection operands; `expand*` and masked
  reductions reject foreign operands; `emit` rejects a column whose projection differs from the
  output keying; `topk` / `arg_extreme` return ids from the tensor's *own* projection rather than
  the first-bound one; `emit_walks` / `emit_pairs` label rows with the originating projection's
  vids. Previously each of these returned a wrong answer or aborted the host process.
  **Single-projection algorithms are unaffected.**
- **`rekey(value, g)`** is the one verified way to cross an index space.

**Migration:** add `nodeLabels`/`edgeTypes` (or `projectAll: true`) to your projection config; call
`rekey` where you deliberately move a value between graphs. A single-graph algorithm needs no
changes.

`4f33cf0ca` is labelled breaking but is strictly *permissive* relative to 3.1.0: `rekey` now
accepts arena-keyed values, which unblocks arena columns that `emit` had made unreturnable.

### Query limits and cancellation now produce a distinct error type

`b63deba8a` — an elapsed deadline raises **`UniError::Timeout { timeout_ms }`** on every query
terminal, where several paths previously reported a generic query error (or nothing at all).

- **Python:** catch `UniTimeoutError` instead of `UniQueryError`. **`CursorTimeoutError` is
  deleted.**
- **Rust:** match `UniError::Timeout` instead of matching `UniError::Query` against a
  `"Query timed out"` message.

See *Query engine: limits that were decorative* below for what this fixed.

### Locy — `prev` inside a comparison is refused at compile time

`e58a29714` — `ALONG ok = prev.h > 5` now fails to compile where it previously appeared to compile.

The comparison builder re-parses the source span with `CypherParser`, where `PREV` is not a
reserved word, so `prev.h` degraded to an ordinary property access on a variable named `prev` and
the `LocyExpr::PrevRef` marker was lost. The binding was wrongly *accepted* in a base case and
silently *stripped* in a recursive rule where it should have worked. A recursive scan now refuses
`prev` at any depth beneath a comparison, at compile time.

**Migration:** rewrite as arithmetic (`prev.h + 1` style). A variable genuinely named `prev` keeps
the backtick escape documented in `locy.pest`.

### Locy — registered and reloaded rules compile against the plugin registry

`ae635b8af` — a recursive `FOLD` over a plugin-registered aggregate, or over `COLLECT` / `COUNT`,
**now compiles where it was previously rejected**. That is the correct answer from the lattice
metadata (`monotone_join: true`), but it changes what compiles.

Because `Semilattice::COUNT` / `COLLECT` have `has_top: false`, such a rule can iterate to
`max_iterations` rather than converging — the iteration cap is the backstop. **No migration is
required**; if you were relying on the compiler to reject those programs, add your own guard.

Internally, `Uni::build` is reordered so the plugin registry is constructed *before* persisted Locy
rules are recompiled, and `rebuild_registry_from_sources`, `build_locy_registry_from_persisted`,
`register_rules_on_registry` and `compile_defined_names` take `&PluginRegistry` explicitly instead
of reading a task-local.

### Plugins — two ABI corrections

- **`55c103d31`** — an algorithm manifest declaring a yield type the emit path cannot build (e.g.
  `yields=["label:string"]`) now fails to **load**, instead of registering cleanly and then failing
  on every `CALL` with `0x862 unsupported emit column type`. The emit channel is `Vec<f64>` per
  column and assembles only Int64/Float64. The new `algorithm_yield_datatype` admits exactly those
  spellings and is routed only through the pyo3 and rhai loaders' *yields* — the general
  `type_name_to_datatype` is deliberately untouched, since `Utf8`/`Boolean` are fine for scalar UDFs
  and procedures. **In practice inert:** every algorithm registration in the tree already uses
  int/float; no fixture changed.
- **`1f05f5190`** — plugin procedure output now decodes the `CypherValue` transport.
  `arrow_scalar_to_value` mapped `LargeBinary` straight to `Value::Bytes`, but `LargeBinary` *is*
  how the framework transports a `CypherValue` — so a plugin yielding a CypherValue handed the
  caller its **wire bytes**, and the same procedure returned a decoded value on the DataFusion path
  and opaque bytes on the row-fallback path. **Migration:** a procedure yielding genuine opaque
  bytes must stamp `uni_raw_bytes` on the yield field.
  *Documented residual hazard:* a raw payload whose first byte is a valid codec tag and whose
  remainder is valid msgpack decodes silently and wrongly. `uni_raw_bytes` is the only escape and no
  loader stamps it yet. This trades a guaranteed wrong answer for every CypherValue yield against a
  rare one for colliding byte payloads.
- **`768b924a1`** — one wire-manifest schema for both loaders. `WireArgType` moves to
  `uni_plugin::wire_manifest` (re-exported by both loaders, so `uni_plugin_extism::WireArgType`
  stays put) and **gains `Vector` and `Variadic`**; exhaustive matches must be updated. This fixed a
  real divergence: a manifest with `kind: "vector"` or `"variadic"` loaded under Extism and failed
  to parse under the Component-Model loader.

### Pydantic OGM — `eager_load()` returns models, not dicts

`55a65a721` — `eager_load()` now returns `list[Model]` / `Model` where it returned `list[dict]` /
`dict`. Before this, `user.posts[0].title` raised `AttributeError` **after** `.eager_load("posts")`
and worked without it. It was worst on the async session, where `_load_relationship` raises and
tells you to use `eager_load()` — there the broken path was the only path.

**Migration:** drop any `["key"]` subscripting on eager-loaded relationships and use attribute
access. To-many relationships now correctly return lists where they previously looked to-one.

### Storage — schemaless edge property reads take a version bound

`fc099ef21` — `MainEdgeDataset::find_props_by_eid` gains a `version: Option<u64>` parameter. A
defaulted shim was deliberately rejected, because passing `None` *is* the bug.

**Migration:** pass the pinned version. The signature now matches `find_props_by_vid`.

### Rust embedder API removals

These affect code that builds against `uni-store` / `uni-query` / `uni-plugin` internals. If you
only use `Uni` / `Session` / `Transaction` and Cypher, nothing here reaches you.

| Commit | Removed / changed | Migration |
|---|---|---|
| `073a574a7` | `uni_store::lancedb`, `LanceDbStore`, the `lancedb` dependency and its `lance-backend` feature entry; `fork::recovery::join_uri_with` | `recover_forks` takes `Option<&dyn ForkBranching>` (was a URI-building closure); `fork::index_builder::build_fork_local_index` takes `&dyn ForkBranching` (was a base URI) |
| `df3332885` | `FilterExpr::{Sql, None}` and their `From<&str>` impls; `UidIndex::open` is now private | `StorageBackend::{count_rows, delete_rows}` take `FilterExpr`; `StorageManager`'s five engine-generated scan methods take `Option<&FilterExpr>`; `ScanRequest::with_filter` takes `FilterExpr`. Opaque predicate text must be spelled `FilterExpr::Raw` — which keeps it greppable. Use `resolve_uids` / `resolve_all_vids` instead of `UidIndex::open` |
| `1134fb166` | `LanceDbBackend::fork_branch` | Use `lance_branch` directly; `dataset_uri` duplication folded into `LanceDirectory::dataset_uri` |
| `b9c46421f` | `overlay_vertex_batch`, `overlay_edge_batch` | No replacement — no query path called them. Six keyed lookups now share `visit_l0_buffers` / `find_in_l0_buffers` |
| `e85fbdb2b` | 11 unused `MainVertexDataset` / `MainEdgeDataset` methods (`find_all_vids`, `find_vids_by_label_name`, `find_vids_by_labels`, `scan_vids`, `find_by_eid`, `find_all_eids`, `find_eids_by_type_name`, singular `find_edges_by_type_name`, `find_type_by_eid`, `extract_eids`, `extract_edges_from_batch`) | Kept: `with_version_bound`, `find_by_ext_id`, `find_labels_by_vid`, `find_batch_props_by_vids`, `find_props_by_vid`, `find_batch_labels_by_vids`, `find_props_by_eid`, plural `find_edges_by_type_names` |
| `246f04708` | `GraphScanExec::new_edge_scan` and its 6 private helpers | Edge reads go through `GraphTraverseExec`. EXPLAIN output is byte-identical |
| `0cd1e44fe` | `apply_anti_join`, `apply_prob_complement`; `InMemoryExec` and 3 `TestMemoryExec` copies; `ProbabilityConfig` | Use the `_composite` variants; use DataFusion's `MemorySourceConfig::try_new_exec` |
| `4fcfc9ab5` | `uni_db::api::query_builder` | Use `session.query_with(..)` → `api::session::QueryBuilder` |
| `1693a6a55` | `PluginRegistrar::pending_len`; `PluginRegistry::{iter_procedures, iter_locy_aggregates, iter_locy_generators, virtual_edge_type_by_name}` | No replacement (zero users). Hex codec shared as `uni_plugin::hex` |
| `c75b8384a` | 15 `TemporalValue` accessors (`millisecond`, `microsecond`, `nanosecond`, `quarter`, `week`, `week_year`, `ordinal_day`, `day_of_week`, `day_of_quarter`, `timezone`, `offset`, `offset_minutes`, `offset_seconds_value`, `epoch_seconds`, `epoch_millis`) | The live implementations are in `uni-query-functions/datetime.rs` — they genuinely diverged (`div_euclid` vs `truncate`). `year`..`second`, `to_date`, `to_time`, `temporal_type` are kept |

### Cypher scalar functions — seven divergences resolved against the TCK

`83a33581e` — uni-db had **two** Cypher scalar-function evaluators that disagreed: the `expr_eval`
interpreter and the registered DataFusion UDFs. Which answer you got depended on the plan shape the
optimizer picked. Resolved against the openCypher TCK (the interpreter was wrong on five of seven):

| Expression | Was | Now |
|---|---|---|
| `toString(1.0)` | `"1"` | `"1.0"` |
| `toString(list / map / node / rel / path)` | a string | TypeError |
| `toInteger('3.7')` | `null` | `3` (truncate) |
| `toBoolean(1)` | error | `true` |
| `substring(s, -1, n)`, `substring(s, 0, -1)` | silently clamped | error |
| `split('a', null)` | `['a']` | `null` |

Also fixed: `cypher_value_cmp` had no `Value::Temporal` arm and fell through to `_ => None`, so
comparing a list of dates came back incomparable **on the UDF path only**.

---

## Highlights

### 🔹 The Plugin Compute ABI — guests that shape and grow their own graph

In 3.0.0, GraphCompute could express exactly one thing: *deterministic propagation over a fixed
graph*. A guest drove coarse native kernels over an immutable projection it did not choose. The
Plugin Compute ABI (`docs/proposals/plugin_compute_abi_2026-07-13.md`) removes that envelope. A
plugin author in **Rhai, Python, WASM (Component Model) or Extism** can now:

- **Choose and shape the graph it sees** — labels, edge types, weight properties, node/edge property
  tensors, Cypher-defined and named graph refs, `includeReverse`, and several graphs at once.
- **Grow structure that does not exist in the store** — a search tree, a residual network, an agent
  population — through a metered mutable arena, then *freeze* it into an ordinary graph handle so
  the whole read-only kernel library applies to it.
- **Be told, at a typed boundary, when it mixes two graphs up** — rather than being handed a
  plausible wrong answer.

The **"conductor, not worker"** model from 3.0.0 is unchanged: the guest runs only the
O(iterations) control loop, native code does all O(V+E) work, and only opaque handles and scalars
cross the plugin boundary.

**The kernel catalogue is now 72 kernels**, single-sourced in `kernel_id.rs` through a `kernels!`
macro that generates `KernelId::ALL` / `op_name` / `reach` / `from_op_name`. The dispatcher matches
with no wildcard arm, so an undispatched kernel fails to compile.

- *Graph & properties* — `graph`, `graph_named`, `rekey`, `interp`, `edge_from_nodes`,
  `vertex_count`, `edge_count`, `degrees`, `vertex_ids`, `node_property`, `edge_property`,
  `edge_weights`, `edges_all`
- *Vertex sets* — `frontier`, `set_to_map`, `map_to_set`, `set_union`, `set_diff`, `set_intersect`,
  `set_len`, `is_empty`
- *Tensors & maps* — `ewise`, `compare`, `zero_map`, `scatter`, `map_apply`, `recip`, `scale`,
  `normalize`, `reduce_sum`, `reduce_sum_masked`, `arg_extreme`, `topk`, `l1_diff`, plus
  zero-charge `work_budget` / `work_spent` / `work_remaining`
- *Traversal* — `expand`, `expand_masked`, `expand_sampled`, `reach_fixpoint`, `spmv`,
  `spmv_masked`, `next_bucket`
- *Edge sets* — `edge_set_len`, `edge_mask_window`, `edge_intersect`, `edge_union`, `sample_edges`,
  `sample_edges_undirected`, `segmented_reduce`
- *Walks & pairs* — `random_walks`, `walk_visit_counts`, `emit_walks`, `sample`,
  `neighborhood_overlap`, `all_pairs_overlap`, `emit_pairs`
- *Lifecycle* — `emit`, `free`

Op-string vocabularies are now parsed from one source (`op_parse.rs`) instead of being hand-matched
in three loaders: `MapOp` `recip|scale|log|sqrt|exp|affine|floor|mod|normalize_l1|normalize_l2`;
`EwiseOp` `add|mul|min|max|axpy|div|mod`; `CmpOp` `gt|ge|lt|le|eq|ne`; `EndpointOp`
`src|dst|sub|absdiff|add|min|max`; `Predicate` `is_zero|gt|lt|eq`; plus norms, semirings and overlap
metrics.

### 🔹 The mutable graph arena (`graph-arena@1`)

A guest can allocate and link vertices that have no counterpart in the store, run kernels over them,
and freeze the result into a normal graph handle:

`arena_new(capacity, branching)`, `arena_alloc`, `arena_expand(parents, fanout)`, `arena_link`,
`arena_column`, `arena_candidates`, `arena_gather`, `arena_scatter`,
`arena_backup(value_col, leaves, deltas)`, `arena_descend(score_col, visit_col, maximize, vloss)`,
`arena_freeze`, `arena_seed(a, g)`.

`arena_seed` seeds an arena *from a projection*, so newly allocated vertices inherit real
neighbours. A graph frozen from an arena canonicalizes back to that arena's origin, so
`arena_freeze` → `spmv` composes. Arena memory is capped by the `GraphComputeArenaBytes` quota, and
a frozen graph now correctly **releases the budget it reserved** (`bbe8e838e` — a freeze/grow loop
previously died at 14 iterations against a 16-handle cap despite freeing everything).

WASM guests reach the arena through a typed, additive WIT interface
(`uni:plugin/host-arena@0.1.0`, handles as `u64`) at roughly **134 ns per call versus ~2 µs** for the
JSON crossing.

### 🔹 Named scopes — one algorithm over several graphs

An algorithm can pre-declare named scopes and be handed all of them at once:

```cypher
CALL myplugin.compare([], {
  nodeLabels: ['Cell'],
  edgeTypes:  ['ADJACENT'],
  scopes: {
    agg:  { nodeLabels: ['Cell'], edgeTypes: ['AGGREGATES'] },
    flow: { nodeQuery: '...',     edgeQuery: '...' }
  }
})
```

The guest reaches them with `gc.graph()` / `gc.graph_named("agg")` (Rhai, Python); sandboxed guests
receive `{"session": 3, "graph": …, "args": [], "graphs": {"agg": …}}`. Before `7e33e3eb8`,
`scopes` was accepted and **silently ignored** by first-party providers, and the WASM/Extism
`graphs` map was write-only.

Full config vocabulary (`GraphProjectionSpec::CONFIG_KEYS`): `nodeLabels`, `edgeTypes`,
`relationshipTypes`, `weightProperty`, `includeReverse`, `nodeProperties`, `edgeProperties`,
`projectAll`, `nodeQuery`, `edgeQuery`, `weightColumn`, `name`, `scopes`.

### 🔹 `interp` — the System Dynamics table function

Piecewise-linear table lookup following the Vensim/Stella `WITH LOOKUP` convention: map each element
through `(xs[i], ys[i])` breakpoints, interpolating between them and clamping outside the range.
Clamping is what makes a table safe to evaluate on an unbounded state variable.

This one is worth explaining, because "it is already composable" was the *wrong* answer here. It
genuinely is composable — a per-segment ramp `max(min(slope*(x-xi), rise), 0)` gated on `compare`
and summed, which needs no upper-bound comparison and gets end-clamping for free (which matters,
since `Predicate` has no `Ge`/`Le`). But the prototype also *measured* it: 112 work units for 3
breakpoints over V=4, roughly 14 passes over `[V]` per segment. A ten-point table runs ~126× the
work of a single pass, the meter charges all of it, and an SD model evaluates several tables per
tick. Expressible but not affordable is exactly the case a kernel slot is for. `interp` charges
O(V) **independent of breakpoint count**, and the test asserts that rather than assuming it.

It fails loud on a curve it cannot evaluate — fewer than two breakpoints, mismatched lengths, a NaN,
or x-breakpoints that do not *strictly* increase (naming the index where ordering breaks; strict
rather than merely sorted, because a repeated x makes the segment slope infinite and the caller
almost certainly meant a step function). Shape- and provenance-preserving, so an `[E]` curve stays
`[E]`.

### 🔹 Kernel rejections now publish an executable recipe

When a guest reaches for an op or a method that does not exist, the error carries a composition
recipe keyed by `(name, family)` — `RECIPES` for op strings, `METHOD_RECIPES` for missing methods
(surfaced through Rhai's `on_missing_function` and a PyO3 `__getattr__` raising an `AttributeError`
subclass). **Every published recipe is proven against a scalar oracle** in
`composition_recipes.rs`, so the suggestion cannot rot. `lookup` / `table` / `with_lookup` /
`piecewise` are published as hints pointing at `interp`; `gc.graph(scope)` answers with the named-
scope feature that replaced it.

### 🔹 `Session::scratch()` — a cheap write-isolated transaction

`crates/uni/src/api/session.rs:793`. A transient transaction that costs roughly **1 ms**, refuses to
commit, and is discarded on drop — the substrate for rollout-style algorithms that need to mutate a
graph and throw the mutation away. Reached from every loader.

The older `ScratchGraph`, `ScratchRegistry`, `ScratchRequest`/`Response`, `LoaderClass` and
`require_compiled_body` are `#[deprecated]`, naming their arena replacements.

### 🔹 Budgets, epochs and tracing — all fail closed

- **Native work budget**: `min(10_000 * (|V| + |E| + 1), 1_000_000_000)` by default, summed over
  *every* bound projection. A `GraphComputeWork` grant **replaces** it in both directions (it can
  lower as well as raise). Guests can read it: `work_budget` / `work_spent` / `work_remaining`
  charge nothing.
- **Session epochs are 16-bit and now saturate via CAS**, returning `0x86B` instead of wrapping.
- **`UNI_GC_TRACE`** — a bounded ring of handle resolutions attached to handle errors and abort
  tags. Always compiled, inert when unset; CI runs a `UNI_GC_TRACE=1` job. Before `efec80f32` the
  trace covered 2 of ~15 paths and aborts carried none.

Error block `0x860`–`0x87F`: `0x860` stale handle · `0x861` kind mismatch · `0x862` shape or
index-space mismatch · `0x863` epoch mismatch · `0x864` arena cap · `0x865` budget exhausted ·
`0x866` iteration limit · `0x867` timeout · `0x868` seed not in projection · `0x869` emit schema
mismatch · `0x86A` slice version mismatch · `0x86B` wrap fail-closed · `0x86C` capability denied ·
`0x86E` arg validation · `0x804` scope-grant refusal · `0x820` unresolvable `graphRef`.

### 🔹 Lance, directly — `lancedb` is gone

`uni-store` had **two** implementations of one storage surface: `backend/lance.rs` on lancedb, and
`backend/lance_branch.rs` on raw `lance::Dataset` (written because lancedb does not expose Lance's
branch API). They had drifted — the branch path disabled scalar-index pushdown (issue #106), the
primary path did not — so the same scan resolved through two different plans depending on which side
asked. Every "correct on primary, wrong on a fork" bug had that shape available to it.

lancedb's only real contribution was URI/table-name mapping and directory enumeration. That becomes
`LanceDirectory` (~271 lines), and everything else drops to `lance::Dataset`. The layout is now an
explicit compatibility contract: a table lives at `{base_uri}/{name}.lance`. The equivalence test
`lance_directory_listing_matches_lancedb` was written **while lancedb was still a dependency**, to
assert listing / `dataset_uri` / open equivalence against lancedb itself — no oracle exists
afterwards.

Three parity details were read out of lancedb's source, each of which degrades results *silently*
rather than erroring:

- the **distance metric is passed explicitly** via `Scanner::distance_metric` — Lance uses it to
  decide whether an index is usable at all, and the old branch path never passed one;
- **`prefilter(true)`** matches lancedb's `only_if` default — postfilter would let excluded rows
  consume top-k slots and return fewer than k;
- **`replace_table_atomic` with no batches stays `delete("true")`** — an empty Overwrite would drop
  the schema.

Fork branching moves behind a new **`ForkBranching`** trait on `StorageBackend`, so `crates/uni` no
longer knows Lance exists. `BranchedBackend` explicitly forwards the capability rather than
inheriting the trait default — without that override every fork-local index build would fail.

**Verified:** `lance::Dataset` now appears nowhere outside `crates/uni-store/src/`, and `lancedb`
appears in no `Cargo.toml` in the repository.

### 🔹 Structured `FilterExpr` — and the wrong answer it deleted

`FilterExpr` was `enum { Sql(String), None }` — the last place the storage abstraction spoke SQL,
and the blocker for any non-Lance backend (an in-memory or WASM implementation would have needed a
SQL parser). It is now a tree:

`Literal · And · Or · Not · Compare · In · ArrayContains · StringMatch · IsNull · IsNotNull · Raw`

with two lowerings: **`to_sql()`**, the single place quoting, escaping and parenthesisation happen
(nine hand-rolled `replace('\'', "''")` copies collapsed into it), and **`eval()`**, a native
three-valued (Kleene) evaluator. `Raw` is permanent, not transitional — the user-facing `filter`
argument on `uni.vector.query` and friends is opaque text nothing in the engine parses. A backend
that cannot evaluate `Raw` **must fail loudly**: treating it as match-all would return rows the
caller asked to exclude, which on a filtered query is a data leak, not a degraded result.

**This deleted a live wrong-answer bug.** The range-fusion path emitted `"col" >= L AND "col" <= U`.
In Lance's dialect a double-quoted name is a **string literal, not a quoted identifier** — so this
was the constant `'col' >= L AND 'col' <= U`, false for every row. A nonexistent column does not
even error. Triggering it required all three of: a Hash scalar index on the column, an `=`/`IN`
predicate on it, and a two-sided inclusive range on the same column. Then:

```cypher
MATCH (n:Ev) WHERE n.createdAt = 3 AND n.createdAt >= 2 AND n.createdAt <= 4 RETURN n
```

returned `[]` instead of the row with `createdAt = 3` — and the residual `FilterExec` cannot recover
rows the scan never emitted. The structured form deletes the fusion outright (~45 lines, including a
`HashSet<*const Expr>` pointer-identity set); it was a no-op transform whose only effect was the bad
string. Regression test: `crates/uni/tests/common/bugs/hash_index_range_quoting.rs`.

Three further predicates interpolated unescaped strings — `type = '…'`, `array_contains(labels, '…')`
in the schemaless scan, and a label filter — so a backtick-quoted label or edge type containing an
apostrophe produced a malformed predicate. All now travel as typed `Scalar::Str`. Separately,
`ee4a9a566` fixes the same class in `JsonPathIndex::get_vids`, where `O'Brien` produced an
unterminated string literal.

`Scalar` carries `UInt(u64)` separately from `Int(i64)` because `Vid::INVALID == u64::MAX` would
round-trip through `i64` as `-1`.

Acceptance test `backend::eval::eval_agrees_with_lance_sql` differentially compares `eval()` against
Lance SQL on a real dataset over random expressions, and is proven by five deliberate mutations.

### 🔹 The wheel: 536 MiB → 238 MiB

Measured on `lib_uni_db.so`, x86_64 Linux, reproduced across two builds:

| step | size |
|---|---|
| before | 536.5 MiB |
| `+ strip = "symbols"` | 374.8 MiB |
| `+ dist profile, no WASM loaders` | **238.4 MiB** |

The shipping wheel is **89.4 MiB** — under PyPI's 100 MB default limit even without a per-project
increase.

The split is counter-intuitive. A release build carries only ~30 MiB of debug info but a **~132 MiB
symbol table**, a consequence of monomorphizing across 500+ dependency crates — so
`strip = "debuginfo"` reclaims almost nothing and only `"symbols"` pays. In the new `[profile.dist]`
(ThinLTO, `codegen-units = 1`; both profiles live in `.cargo/config.toml`),
**`codegen-units` is the lever, not the LTO mode**: 16 → 1 is worth
~130 MiB, while fat-vs-thin LTO is worth 19 MiB. Fat LTO is deliberately rejected — it pushes peak
rustc RSS past 15.8 GiB and would OOM the macOS runner for that 19 MiB. Peak RSS floors around
11 GiB for the ThinLTO link regardless of codegen-units, so raising the latter buys size back
without buying memory. (The profile is named `dist` because cargo reserves `publish`.)

`build-metal` moves from `macos-latest` (7 GB) to `macos-latest-xlarge` (14 GB, Apple Silicon —
`-large` is Intel, the wrong host for `aarch64-apple-darwin` plus Metal); the lower codegen-units
would OOM the 7 GB default. Release builds take roughly **25% longer** (19m26s vs 15m40s locally for
`-p uni-python`).

---

## Correctness — data loss and silent wrong answers

Seven defects in this release returned wrong results or lost data with **no error, no warning and no
log entry**. These are the headline fixes.

### A read-only open silently rolled back committed transactions — `a06e7b299`

Reopening a partially-flushed database read-only returned **2 of 4 nodes**. A plain `MATCH` missed
committed rows, with no error.

WAL replay restores the committed-but-unflushed suffix into the *writer's* `L0Manager`. A read-only
open then dropped the writer — and with it the only handle to that tier — so reads saw flushed L1
only. The `l0_manager` `Arc` is now lifted out before the writer is dropped (no write capability is
granted; writes stay gated on `UniInner.writer`, pinned by a test). Two adjacent defects in the same
change: a writer-less `get_context` was dropping `pending_flush_l0s`, and read-only opens were
spawning auto-flush and index-rebuild tasks that wrote L1.

### Edge properties vanished after ~300 write transactions — `c56340a07`

From the first `compact_adjacency` onward, `get_batch_edge_props` returned nothing for **every**
EID and never recovered.

The blast radius is worse than it sounds: a missing property becomes NaN, and `edge_mask_window`'s
`v >= lo && v <= hi` fails under *any* window — so **every masked traversal silently returned zero
rows**, and weighted algorithms degraded to unit weights. An external adopter's run matched a
reference implementation for ~150 ticks and then diverged.

Compaction deletes incorporated delta rows on the invariant that edge properties survive in
`main_edges`. Two sibling readers fall back to `MainEdgeDataset` correctly; this one — the one a
GraphCompute projection calls — did not.

### `properties()` came back empty on an unlabelled multi-label endpoint — `d0b948e24`

`MATCH (a:Author)-[:WROTE]->(b) RETURN properties(b)` returned `{}` whenever `WROTE` declared more
than one label on an endpoint. The row was found and `labels(b)` was correct — only the properties
were missing.

The planner collapses a multi-label destination to `None` and takes the label-agnostic branch, which
filtered out the `_all_props` wildcard and then skipped the storage read entirely. `_all_props` is
not an internal name to strip — it is what `get_batch_vertex_props` understands. The sentinel now
passes through at **both** sites: single-hop and `hydrate_vlp_target_properties`, which was broken
identically. (This is the issue-#135 fix finally reaching the copy it missed.)

### A NULL-bearing `IN` list gave different rows pushed-down vs. native — `60ef82f6b`

`NOT ((i IN (NULL)) AND (i IN (0)))` — the native Kleene evaluator returned the correct 80 rows,
Lance returned all 120. Lance rewrites two `InList` predicates over the same column in a way that
loses the unknown. `to_sql` now states it outright: an all-NULL list renders
`CAST(NULL AS BOOLEAN)`, a mixed list renders `(col IN (non_nulls) OR CAST(NULL AS BOOLEAN))`. The
cast is load-bearing — a bare `NULL` is untyped and foldable. NULL-free lists keep the plain
rendering.

### The Pydantic OGM silently dropped rows during hydration — `ce8ba6381`

`Model.query().all()` returned fewer rows than matched, with no log, warning or counter.
`len(q.all()) != q.count()`, and `.one()` raised *"Query returned no results"* for a row that
demonstrably exists.

Two defects. First, `except Exception: return None` could not distinguish "the stored data is
invalid" from "the mapping layer is broken" — a validator `RuntimeError`, or a typo in
`_convert_db_values`, silently removed a row. It is narrowed to `ValidationError`, which now warns
via `_warn_unhydratable` naming the label and vid. Second, `if instance:` is a truthiness test — a
model defining `__bool__` or `__len__` was dropped *after* hydrating perfectly. All nine guards are
now `is not None`. It warns rather than logs because the package has no logging configuration, so a
logger would be silent by default.

### Row-path procedure hosts saw flushed storage only — `b7e55e944`

A mode-dependent wrong answer: a label-scoped projection saw committed-but-unflushed rows, while a
Cypher-scoped one on the *same call* silently saw none. This reached `execute_plugin_procedure` —
every row-path plugin procedure, including `uni.graph.project`'s Cypher mode and everything routed
through `CypherProcedureSynthesizer`. Fixed with a `with_l0_context` builder mirroring `with_writer`.
It is reachable only on the row path, which is why an earlier `uni.algo.gcpagerank` probe came back
falsely clean.

### A stringly-typed "not found" check handed out already-live VIDs — `eb35fe6db`

`IdAllocator::new` classified errors with `e.to_string().contains("not found")`. A proxy 404 body,
or a wrapped S3 "bucket not found", started the allocator from a defaulted manifest **against a
populated database** — and it then handed out VIDs that were already in use. It now uses the typed
`store_utils::is_not_found`. Two sibling loaders had been migrated long ago; this one was never
converted.

### Schemaless edge property reads escaped snapshot isolation — `fc099ef21`

Bidirectionally wrong. The L1 main-table fallback filtered on `_eid` only and took the highest
`_version` row with **no snapshot bound**, while the delta scan and the L0 overlay were both bounded.
Because schemaless and overflow edge properties live only in `props_json` and never in delta
columns, that fallback fired on *every* such read. A pinned view saw post-snapshot values (observed
`Int(2)` past the pin), and a post-pin delete wrote a tombstone that won the version race — making
an edge that existed at the snapshot vanish entirely.

The version bound is added as a *conjunct*, so the tombstone-winner rule from issue #53 runs
unchanged over the survivors. `exists_by_eid` is deliberately left unbounded (it is a
compaction dual-write invariant check). In practice the leak bites when the `PropertyManager` was
built over pinned storage — i.e. `UniInner::at_snapshot` time-travel reads — not ordinary
read-write transactions. Still open and filed: `find_edges_by_type_names` is also unbounded, but
that is topology-level and needs its own design.

### Edge-type and label counts returned a confident zero — `a5154a35f`

`get_edge_type_info` interpolated the type name into Cypher **unquoted** and swallowed failures with
`Err(_) => 0`. Any name outside `[A-Za-z_][A-Za-z0-9_]*` — hyphens, dots (documented as supported
for qualified names), leading digits, anything non-ASCII — produced a parse error reported as *0
edges* inside an otherwise-correct `EdgeTypeInfo`. Separately, `get_label_info` counted via
`backend.count_rows`, which is L0-blind, so unflushed rows also counted as 0.

Names are now backtick-quoted, errors propagate, and a name containing a backtick is refused
explicitly (the quoted grammar has no escape rule). A Python assertion that had been weakened to
tolerate the zero is restored. Callers that relied on `0` never erroring will now see errors.

### The CHECK evaluator diverged between bulk and transactional writes — `33601464a`

`uni-bulk::BulkWriter` and `uni-store::Writer` each carried a ~100-line copy of the CHECK evaluator.
The bulk one routed numeric `=`/`!=` through `compare_values` (because `Value`'s `PartialEq` is
type-strict with no Int/Float arm); the writer used bare `==`. So `CHECK (score = 5)` against a
stored `Float(5.0)` **passed** via `BulkWriter::insert_vertices` and **failed** via
`tx.execute("CREATE ...")`. Both now call `uni_common::core::check_constraint::evaluate`.
Transactional CHECK equality now coerces Int/Float; warnings move from `log` to `tracing`.

### `map_variables` dropped ten `Expr` variants — `7cdc811e3`

The hand-written match ended in `other => other`, silently skipping `Exists`, `CountSubquery`,
`CollectSubquery`, `Quantifier`, `Reduce`, `ListComprehension`, `PatternComprehension`, `ValidAt`,
`MapProjection` and `LabelCheck` — all reachable from Locy, since `locy_yield_item` and
`fold_expression` take an arbitrary Cypher expression. An `ALONG` binding in one of those positions
stayed a bare `Variable` and resolved against a schema without that field. Both this and its
arm-for-arm duplicate `rewrite_is_ref_cols` now delegate to `Expr::map_children_in_scope` (chosen
over `map_children` so comprehension binders are not captured). Net ~120 lines deleted, and
fold-output detection widens correctly, so such expressions are now deferred post-fold instead of
failing at plan time.

### Four `Expr` walkers silently swallowed four variants — `9f8dd7993`

`MapProjection`, `ValidAt`, `LabelCheck` and `PatternComprehension` fell through `_ => {}`. Reachable:
`MATCH (n:P) RETURN n{.name, c: count(*)}` built no aggregation at all and died with
`DataFusion planning failed: Schema error: No field named "n.name"` — physical-planner internals
surfacing as a user-facing error. A new `Expr::for_each_child_in_scope` fixes the walkers; the query
now reports `AmbiguousAggregationExpression`. It still does not succeed where the flat
`RETURN n.name, count(*)` does — that limitation is pinned by a named test.

---

## Query engine: limits that were decorative

`f18fb7170` and `b63deba8a` closed a set of guarantees that were advertised and not delivered:

- `QueryBuilder::cursor` accepted `.timeout()`, `.max_memory()` and `.cancellation_token()` and
  **honoured none of them**. A self-join whose `fetch_all` was rejected on memory grounds streamed
  all **1,999,000 rows** through `.cursor()`.
- `Session::cancel()` did nothing on the plain `query()` path — `execute_cached` hard-coded `None` at
  all three sites.
- `Transaction::cancel()` was inert.
- `max_query_memory` was inert across the whole transaction surface.
- Polling a drained cursor could panic across PyO3 as a **process abort** (the added `.fuse()` is
  load-bearing).

Every path now flows through one `build_guarded_cursor` tail and takes a **required** `CancelScope` —
making it required is what let the compiler enumerate the droppers, including `PreparedQuery`. The
guard races the stream against the token, because the executor token is only an optimisation: no
scan, join or traverse plan actually reaches `GraphContext::check_timeout`. Python's
`TxQueryBuilder` gains `cancellation_token` across sync, async and the stub.

`f2f776d45` closes the same shape in Locy: `LocyBuilder::cancellation_token` was **write-only** —
the setter existed on the session and transaction builders and the Python bindings called it, but
`LocyEngine` carried no cancellation state at all. And `execute_ast_internal_with_tx_l0` had no
scope parameter, so `Transaction::cancel()` was inert for `Transaction::apply` / `replay_facts` and
tx-bound Locy. Crucially, `evaluate_compiled_capturing` now races the whole fixpoint evaluation —
statement-level plumbing alone would have left the common case, a long-running fixpoint, unguarded.

### `VERSION AS OF` panicked on three of four terminals — `ef0d863a9`

`explain`, `profile` and `cursor` **panicked** on a time-travel query; only `fetch_all` worked.
Across PyO3 it surfaced as a `pyo3_runtime.PanicException` from a `block_on`. Reachable from Rust,
the CLI REPL and Python. `split_time_travel` is now one named function with seven call sites, and
the three terminals resolve through a shared `pinned_at` and re-dispatch. The fix deliberately does
*not* soften the `unreachable!`, and does *not* merely strip the version spec — answering against
the live version silently would be worse than the panic.

### `classify_verb` panicked on non-ASCII statements — `5ac4e5ab8`

`s[..s.len().min(32)]` used **byte** length where the comment said chars, so any statement with a
multi-byte codepoint straddling byte 32 panicked with `end byte index 32 is not a char boundary`.
Every write routes through this function, and across PyO3 it surfaced as a process abort rather than
an exception. It now iterates by `char`; the workspace was swept for the same shape and no other
`&str` slice sites are at risk.

---

## Locy

- **Parenthesised patterns are no longer a wardedness false positive** (`c251415f0`). Parenthesising
  an otherwise-identical pattern turned a legal rule into `WardednessViolation`. Since a quantified
  sub-pattern *must* be parenthesised, `MATCH ((a)-[:R]->(b)){1,3}` could never be warded.
  `PatternElement::Parenthesized` was an empty arm — the only variant carrying a nested
  `PathPattern`, so every variable bound inside parens was invisible; every other consumer already
  recursed. The arm stays explicit so a future binding variant fails to compile.
- **An `ASSUME` body can reference the rules around it** (`86d377b28`). Previously this never worked,
  with or without `MODULE`, across two independent layers: a compile-time namespace mismatch (the
  body compiled with `module: None`, so `adult` vs. catalogued `m.adult` → `UndefinedRule`), and a
  runtime gap where planning the body's strata saw only the body's catalog while body commands
  dispatched against the *parent's*. `evaluate_assume` now builds one merged catalog — parent rules,
  body shadowing on collision, module-qualified parents also exposed bare — while each construction
  keeps its own `strata`. Merging catalogs without merging strata is what makes it safe.
- **Plugin-registered aggregates work in recursive `FOLD`** (`e54c97629`, `ae635b8af`). See
  *Breaking changes* above. Previously `check_non_monotonic_in_recursion` consulted a hardcoded
  six-name oracle, so an aggregate registered with `monotone_join: true` was rejected at compile
  time — while the *planner's* guard did consult the registry, meaning compile and plan disagreed
  about the same program. Both now route through `locy_fold::locy_monotonicity_verdict`. The oracle
  is threaded through a fifth, previously unnoticed call site inside `compile_with_context`'s
  ASSUME-body recursion, so a host oracle no longer stops at the ASSUME boundary. The TCK's
  `when_compile` step now compiles through the same composed verdict — it previously called bare
  `uni_locy::compile` and could never exercise the registry.
- **`BEST BY` no longer consumes its monotonicity oracle inverted** (`23de3997f`). Behaviour is
  unchanged for in-tree callers, but a naive registry-backed oracle swap would have rejected valid
  programs like `FOLD peak = MAX(a.cost) BEST BY peak ASC`. The doc comment is corrected too — it
  claimed an `M`-prefix convention that never existed; it is an exact six-name list, so `MFOO`
  answers `None`.

---

## Storage & concurrency

- **Shadow-CSR retention is bounded and observable** (`36b05898f`, `4bd219be3`). `AdjacencyManager::warm`
  pushed a `ShadowEdge` for every `op == 1` row scanned out of the L1 delta and — unlike
  `warm_coalesced` — had no `has_csr` short-circuit, so each warm of the same `(edge_type, direction)`
  re-pushed the **entire** delete history. Growth was unbounded in the number of *warms*, not
  deletes. It could never trip `max_bytes`, because `memory_usage` read only `current_bytes`. Reads
  dedupe by `Eid` downstream, which is why it surfaced as memory rather than duplicated neighbours.
  `add_deleted_edge` now ignores a repeat of an `Eid` it already holds (deduping there rather than
  short-circuiting `warm`, which would also skip deletions since the last warm and lose adjacency
  correctness). New accessors: **`AdjacencyManager::shadow_entry_count()`** exposes retention
  directly, and **`memory_usage()`** folds in `ShadowCsr::approx_bytes()`.
  `4bd219be3` then actually reclaims — `ShadowCsr::gc` existed, was tested, and was called from
  nowhere. The bound was the hard part: `StorageManager::pinned()` and `at_fork` each build a *fresh*
  `AdjacencyManager` with an empty `ShadowCsr`, so only `pinned_at_version` (the path every
  read-write transaction takes) shares the live instance. A new refcounted `PinnedVersions` registry
  tracks in-flight pins — several transactions routinely pin the same version — and
  `pinned_at_version` returns a `PinGuard` that releases on drop.
  **`AdjacencyManager::gc_shadow(current_version)`** computes the floor and sweeps after CSR
  compaction.
- **Object-store errors are classified by variant, not by message text** (`14ef0876d`). The same
  stringly-typed defect as `eb35fe6db`, one layer up: `ResilientObjectStore::retry` classified by
  lowercased text, so a transient `Error::Generic` that happened to read like a 404 was abandoned —
  and the classifier ran *after* the attempt-budget check. Separately, `put_opts` counted every
  failure against the circuit breaker with no classifier, so **five OCC losers opened the breaker
  and took the whole store offline for 30 seconds**. Both now use the typed
  `store_utils::is_terminal`, whose wildcard answers *retryable*.
- **WAL segments removed by a concurrent truncation are tolerated** (`f382561ec`). The flush
  finalizer's spawned task outlives the `Uni` handle (`Drop` only *signals*), so concurrent
  truncators raced list-then-delete and the loser failed its **entire flush** with
  `Object at location .../wal/… not found`. `replay_since` had the same list-then-get exposure. Only
  `NotFound` is tolerated, and `deleted_count` stays an honest count.
- **SSI read-set granularity documented** (`42f27c7d8`, docs-only). An audit read three non-recording
  L0 accessors (`get_vertex_properties`, `get_edge_properties`, `edge_exists_in_l0`) as a write-skew
  hole. That is refuted: the read set is item-granular (`HashSet<Vid>` / `HashSet<Eid>`) and every
  caller records the id first. Adding the recording would double read-set lock acquisitions on the
  per-row materialisation path.
- **A latent compaction bug removed with dead code** (`1746fcc8c`). Two of ten deleted methods were
  broken paths: `EdgeDataset::new` built `{base}/edges_{type}` and `DeltaDataset::new` built
  `{base}/deltas/{type}_{dir}` against a canonical layout of `{base}/edges_{type}.lance` /
  `{base}/deltas_{type}_{dir}.lance`, and `DeltaDataset::scan_all` swallowed the open error and
  returned `Ok(vec![])` — a silent empty delta table in a compaction path, had it ever been called.

---

## Python / PyO3

| Fix | Symptom before |
|---|---|
| `9581eadc6` | `next_row_async` returned `Result<_, String>`, so `fetch_one` / `fetch_many` / `__anext__` all raised a bare `PyRuntimeError`. `_retry.RETRIABLE_EXCEPTIONS` matches by class — so **a retriable failure reached via `async for` was unrecognisable and a retryable transaction failed permanently**. The sync twin was correct, and `fetch_all` bypasses the path, which showed false parity. Now routed through `uni_error_to_pyerr` |
| `0821205dd` | `UniSync::shutdown` ended in `mem::forget(self)`, which suppressed only `Runtime::drop` (the `Drop` guard was already `None` after `inner.take()`). **48 → 232 OS threads over 8 cycles, ~23 leaked per call** |
| `fee97302d` | The follow-up: `AsyncUni.__aexit__` open-coded `flush()` instead of delegating to `shutdown`, so `async with await AsyncUni.in_memory()` — the idiomatic path — still stranded a runtime. All four teardown paths now covered: **0 stranded across 60 cycles each** |
| `0847283ef` | `value_to_py`'s wildcard returned `py.None()` — the mechanism that shipped two releases silently dropping `SparseVector` and `BinaryVector` properties. Now raises `PyValueError` naming the variant. The arm is unreachable today (all 15 variants handled) but `#[non_exhaustive]` means the wildcard cannot be deleted |
| `0ca8efb50` | `PyForkStatus::from_rust` mapped unknown states to `Active`. Non-`Active` is precisely the "mid-lifecycle, unsafe" signal — so `db.list_forks()` reported such a fork healthy and callers opened sessions against it. New `Unknown` variant fails closed |
| `c56340a07` (D12) | `in_memory()` + `shutdown()` stranded its scratch directory (**8/300 leaked → 0/300**). Python `shutdown()` only flushed; `Uni::shutdown_in_place` now gives an `Arc<Uni>` a real shutdown and reaps the directory, warning on failure |

---

## Pydantic OGM

Beyond the row-dropping fix above, `55a65a721` fixed two prerequisite defects that were **invisible
until `ce8ba6381` stopped the swallow**:

1. `descriptor.is_list` was always `False` and `target_type` always `None`, because the metaclass
   deletes a relationship's annotation *before* the descriptor loop re-reads it. The shape is now
   captured before deletion.
2. Relationship-implied edge types declared **every registered label** on both endpoints, which
   triggered the engine's multi-label `properties()` bug (`d0b948e24`) — so every relationship target
   loaded propertyless and failed validation. Endpoints now derive from the declaration, honouring
   `direction` and unioning across shared edge types. Quoted annotations (`"Bio | None"`,
   `"list[Book]"`) resolve by identifier match, with a permissive fallback rather than a guess.

Entities with no related rows now get an explicit empty cache entry; previously they fell through to
the lazy path, which on the async session raises.

---

## Plugins & loaders

- **`algorithms_registered` channel** (`0882bc168`, `463e8025a`) — `LoadOutcome` now reports which
  algorithms a plugin registered, across all four loaders, surfaced in the Python dicts and in
  `plugin install`. `denied_capabilities` is fixed via `CapabilitySet::denied_against(effective)`.
- **`Algorithm` and `GraphCompute` are grantable** (`f24db798e`, **fixes #150**). A `GraphCompute`
  grant string could not be parsed at all, so a guest algorithm loaded via `load_rhai_plugin` failed
  with `ProcedureNotFound`. There is now a single `Capability::parse_grant` / `grant_name` pair
  (accepting PascalCase or kebab-case) used by the Python bindings and the CLI.
- **Session-scoped plugin aggregate and window functions resolve from Cypher** (`383f6ce41`).
- **Declared algorithm args are validated across all loaders** (`f82cf6441`) — arity and type checked,
  defaults filled at call time.
- **Capability grant tables reconciled to source of truth** (`456565163`). Internal capabilities
  (`Auth`, `Authz`, `Cdc`, `Catalog`, `PluginDeclare`) and the phantom `Connector` are removed from
  the grantable list; `GraphCompute` and `LocyGenerator` are added; `BackgroundJob` moves to quotas
  and `PluginStorage` to extension surfaces. Backed by `GRANTABLE_NAMES` / `QUOTA_NAMES` /
  `INTERNAL_NAMES` in `uni_plugin::Capability`.
- **The `uni.http.*` policy is shared between loaders** (`3aa2854be`). This security-relevant policy —
  allow-list match, egress requirement, `WallClockMillisPerCall` timeout with a 10 s default, 8 MiB
  response cap, `>= 400` rejection — existed in two independent copies. Consolidated into
  `uni_plugin::host_services::http_request`; guest-facing error codes (`0xC20`/`0xC21`/`0xC23`) and
  Rhai error strings are byte-identical.

---

## Performance

**`2f0479579` — plugin instantiation was serialized behind a pointless mutex.** `InstancePool` stored
its factory as `Mutex<Box<dyn Fn() -> Result<T, E> + Send + Sync>>` and called
`(self.factory.lock())()`. The parking_lot guard lives to end-of-statement, so the lock was held
across the **entire** instance construction: a pool configured `max_instances = 4` advertised
four-way concurrency and delivered one-way. The mutex bought nothing — the closure was already
`Send + Sync`.

The serialized segment was `InstancePre::instantiate` plus `fresh_store` (not a full compile;
`build_pool` caches the `InstancePre`). The evidence is a two-thread `Barrier` test that **deadlocks
to a 10 s timeout** under the old code and passes in ~0 s after.

---

## Simplification & dead-code removal

Driven by an explicit audit (`85aa17df1`, 56 findings, including a deliberate do-not-do list of four
consolidations that look right and are not). Beyond the removals listed in *Breaking changes*:

- **`LogicalPlan::input_mut` / `map_input`** (`3eb5261d7`) replace 19-field destructure-and-rebuild
  dances where a forgotten field is dropped silently.
- **`plan_children` is exhaustive over `LogicalPlan`** (`e2c4b4da2`). Its `_ => vec![]` treated
  `FusedIndexScanWrapped` and five `Locy*` operators as leaves. It gates `is_ddl_or_admin` (DataFusion
  vs. row-fallback routing) and `contains_write_operations`. Behaviour is unchanged today — those
  arms are unreachable from a root plan — but the row-fallback Locy arms are `unreachable!()`, so a
  future misclassification would have panicked.
- **28 mechanical UDF shells** moved behind a `cypher_scalar_udf!` macro, with all 49 `name()` strings
  and 60 `create_*_udf` constructors byte-identical (`83a33581e`).
- **`IndexDefinition::{name, label, metadata, metadata_mut}`** generated from one
  `for_each_index_variant!` table (`c75b8384a`); the Python `Diff` pyclasses generated from one macro
  (`9b1c3b436`).
- Shared implementations for the detach-delete edge sweep (`4c8aee781`), the empty-expansion prologue
  in traverse batch builders (`30182c9e8`), and `arg_string_list` in `uni-algo` (`c061e43a9`).

---

## Testing & CI

- **Integration-test binaries consolidated 33 → 7** (`5ec5b76b2`), with the rule codified
  (`02282bc11`). Cargo builds one test binary per `.rs` file directly under `tests/`, each statically
  linking the crate's full transitive dep set (datafusion, lance, candle), so loose files inflate
  build and link time and the process count nextest spawns. Per crate: uni-cypher 9→1,
  uni-plugin-host 6→1, uni-locy 5→1, uni-query-functions 4→1, uni-query 4→1 (three drifted
  standalones folded back), uni-store 5→2 — `occ_model` stays standalone because it is the
  loom/shuttle model-check target and merging would drag real-`Writer` tests into the loom build,
  which panics. **Test counts are identical before and after** (105/61/121/266/757/625).
  New project rule: a **hard cap of 3 integration-test binaries per crate** — add a `mod` to
  `tests/integration.rs` rather than a new `tests/*.rs`. Documented in `docs/test_layout.md` and
  `AGENTS.md`, so it is seen when adding a test rather than at review.
- **Cross-loader Python plugin conformance suite** (`9a6b7714f`) — every loader (rhai / pyo3 /
  wasm-component / extism) × every extension kind, with **each granted behaviour paired with a denied
  one**, so each row proves the capability actually gates. That pairing is the hole that let #150
  ship. Covers scalar/aggregate/procedure/algorithm/host-fn registration and invocation, native
  `uni.algo.gcpagerank` parity for all three guest graph algorithms, the `algorithms_registered`
  channel, and strict grant-string rejection of internal and quota names.
- **A `python-wasm-tests` lane** (`9634e7613`, moved post-merge by `745038451`). When the wheel's
  default features were emptied for size, 12 loader tests silently broke. Tests are now
  feature-gated, and the lane builds a `wasm-plugins,extism-plugins` wheel on its own runner —
  fixtures built *before* the wheel (otherwise vacuously green), and the job **asserts the loaders
  are present** before pytest, so a feature build that silently failed cannot pass on skips.
  *Accepted tradeoff:* it runs post-merge, so a broken WASM/Extism loader now fails on main rather
  than on the PR. `gate` still requires it, so loaders are verified before a tag can publish.
- **The pyo3 loader's Rust tests now compile at all** (`a9a0c0496`, `041b13e10`). `uni-plugin-pyo3` is
  entirely `#![cfg(feature = "pyo3")]` with no default features, and the `uni-python*` crates are
  excluded for linker reasons — so its tests compiled in **no CI invocation whatsoever**. They now
  run in the Python job, the only one with a pyarrow venv (`auto-initialize` links system Python,
  whose `sys.path` excludes the project venv). Every job in `pr.yml` / `ci.yml` / `nightly.yml` /
  `release.yml` was audited for the missing-nextest shape; none remain.
- **The notebooks job genuinely tests the release wheel** (`7526fa514`). `uv run --project
  bindings/uni-db` was re-syncing the environment — uninstalling the built wheel and restoring the
  editable install — so six notebooks had been validating a `maturin develop` build while reporting
  they validated the release wheel (reproduced: wheel 3.1.0 vs. 3.5.0 editable). Every `uv run` now
  passes `--no-sync`, `dist/*.whl` is cleaned before build, and a guard asserts the installed
  distribution matches the workspace version before any notebook runs.
- **A real flake removed** (`47fc03463`). Three heavy issue-#55 `get_edges` probes each saturated the
  machine, timing out against the default 180 s ceiling (60 s period × 3). Fixed by grouping them at
  `max-threads = 1` **and** raising that ceiling to 360 s. Both halves are load-bearing, and the
  config records the measurements proving it: raising the ceiling for the heaviest test alone just
  moved the failure to the other two; raising it to 360 s for all three without grouping was *still*
  red (the suite went 367 s with 1 failed + 1 timed out, and it also starved an unrelated
  `uni-plugin-rhai` throughput-floor test). Grouped *and* raised: green at 6126/6126, with the three
  taking 245 s / 210 s / 56 s. Serialising within the group does not hand them an idle box — ~6,000
  other tests keep running alongside. **No assertion was relaxed**, the global 3-minute backstop is
  untouched elsewhere, and both rejected approaches are recorded in
  `.config/nextest.toml` so they are not retried blindly.
- **A reproduction harness for the external stepped-dynamics gap report** (`26ac83be6`) — six red
  `#[ignore]`d repros runnable via `--run-ignored all -E 'test(repro_)'`, plus two green probes that
  *correct* the analysis (forks project identically to the parent; Cypher-mode `graphRef` does see
  committed-unflushed rows).
- `68e105381` covers `UniBuilder::build_sync`, which had no in-repo caller; the test drives the handle
  from its own runtime, proving the database outlives the runtime `build_sync` opened and dropped.
- The local CI runbook (`3e9a47355`) picks up the two missing steps (`UNI_GC_TRACE=1`, and
  `--features pyo3` with its PYTHONPATH incantation) plus four gotchas: a stale wheel in `dist/`,
  `maturin develop` needing an existing `TMPDIR` (and `tail` masking its exit status), `LD_PRELOAD`
  being venv-scoped rather than global, and `wasm-tools` being needed only for
  `wasm32-unknown-unknown` fixtures.

---

## Documentation

New in `docs/proposals/`: the Plugin Compute ABI design (`plugin_compute_abi_2026-07-13.md`), guest
stateful compute (`guest_stateful_compute_2026-07-20.md`), the 12-gap pluggability verification
(`graphcompute_pluggability_gaps_verification_2026-07-22.md`), projection parity
(`graphcompute_projection_parity_2026-07-19.md`), the floor/mod analysis, the stepped-dynamics
requirements response, the duplication / dead-code audit (`dup_dead_simplify_audit_2026-07-30.md`),
the cleanup sweep (`cleanup_sweep_2026-07-31.md`), and the Python plugin ABI gap plan.

`docs/UNI_BLACK_BOOK.md` gains a full **GraphCompute** part — the guest surface, kernel catalogue and
handles, determinism/budgets/safety, capability gating, the scoped-or-loud L0-consistent projection
contract, first-party dogfooding providers, and loader surfaces — plus a `ShadowCsr` section.
`website/docs/plugins/graph-algorithms.md` (676 lines) is the user-facing counterpart, and the
`uni-db` skill reference gains a graph-algorithms reference.

### A design proposal that deliberately landed nothing

`docs/proposals/simulation_first_class_2026-07-23.md` (`42f549d38`) answers *"should MCTS be
first-class in uni-db?"* with **no — and it already correctly isn't**, living as `graph-arena@1`
kernels driven by a guest `AlgorithmProvider`.

Its substantive finding is that the pieces of a simulation engine already exist as **four
disconnected islands that cannot compose**: Locy `ASSUME`/`ABDUCE` (real semantics, but a cold
full-fixpoint recompute per hypothetical, single-ply), `Session::scratch()` (a ~1 ms ephemeral world,
but a non-branchable leaf that pins primary), `graph-arena@1` (18.5 M rollouts/s, but over a
synthetic in-arena tree with a guest-supplied score), and Lance forks (durable, but ~10 ms
create/drop). You can have fast-but-synthetic or real-but-slow, never *"fast tree search whose
rollouts mutate the real graph and are scored by a real Locy rule."*

The proposal makes the **environment** first-class rather than the search, and names **incremental
fixpoint maintenance as the keystone capability host code fundamentally cannot supply**.
**Status: design only — nothing landed.**

---

## Dependencies

- **`lancedb` removed entirely** from the workspace dependency table and from the `lance-backend`
  feature. `uni-store` builds directly on `lance`.
- Published `uni-db` wheels no longer link `wasmtime`, `cranelift`, `wasi-common` or `extism`.
- Two unused dev-dependencies removed alongside the lancedb drop.

---

## Migration guide — upgrading from 3.0.0 to 3.2.0

**If you use uni-db through Cypher, `Uni`/`Session`/`Transaction`, or the Python `Database` API**, the
changes that can reach you are:

1. **WASM/Extism plugin loading from a published wheel** — `Database.load_wasm_component` and
   `Database.load_wasm_extism` are gone. Build from source with
   `--features wasm-plugins,extism-plugins`, or use the Rhai / PyO3 loaders.
2. **Timeout handling** — catch `UniTimeoutError` (Python) or match `UniError::Timeout` (Rust).
   `CursorTimeoutError` is deleted.
3. **Cypher scalar function results** — re-check any code depending on `toString(1.0)`,
   `toInteger('3.7')`, `toBoolean(1)`, `split(s, null)`, or negative-argument `substring`. See the
   table in *Breaking changes*.
4. **Pydantic `eager_load()`** — drop `["key"]` subscripting; use attribute access. To-many
   relationships now correctly return lists.
5. **Locy `prev` in a comparison** — rewrite `ALONG ok = prev.h > 5` as arithmetic.

**If you author GraphCompute plugins:**

6. **Scope your projection** — add `nodeLabels`/`edgeTypes`, or `{projectAll: true}`.
7. **Cross index spaces with `rekey`** — single-projection algorithms need no change; multi-graph
   ones must be explicit.
8. **Algorithm `yields`** must be Int64/Float64 spellings; a string or bool yield now fails at load
   rather than at call.
9. **Procedures yielding genuine opaque bytes** must stamp `uni_raw_bytes` on the yield field.

**If you build against `uni-store` / `uni-query` / `uni-plugin` internals**, see the *Rust embedder
API removals* table. The two changes most likely to reach you are `FilterExpr` (string predicates
must become `FilterExpr::Raw` or, better, a structured tree) and `find_props_by_eid` gaining a
`version` parameter.

---

## Upgrade notes (behaviour changes to re-validate)

- **A recursive `FOLD` over `COLLECT` / `COUNT` now compiles.** It can iterate to `max_iterations`
  rather than converging, since those semilattices have `has_top: false`. If you were relying on the
  compiler to reject such programs, add your own guard.
- **Transactional `CHECK` equality now coerces Int/Float**, matching what the bulk path always did.
  `CHECK (score = 5)` now passes against a stored `Float(5.0)` via `tx.execute` as well as via
  `BulkWriter`.
- **Edge-type and label counts now propagate errors** instead of reporting `0`. A name that cannot be
  quoted (one containing a backtick) is refused explicitly.
- **Query timeouts, memory limits and cancellation are now actually enforced** on cursors, on
  `Session::cancel()`, on `Transaction::cancel()`, and on Locy fixpoint evaluation. Work that
  previously ran to completion despite a cancel, or streamed past a memory limit, will now stop.
- **Release-build panic backtraces are unsymbolicated** (`strip = "symbols"`). Panic *messages* still
  reach Python through PyO3.
- **Release builds take roughly 25% longer** under `[profile.dist]`.
- **A broken WASM/Extism loader now fails on main, not on the PR** — the lane moved post-merge. `gate`
  still requires it before a tag can publish.

---

## Closed issues

- **#150** — Python: cannot load or invoke a guest GraphCompute algorithm plugin
  (`Algorithm`/`GraphCompute` grants unavailable via `load_rhai_plugin`)
- **#151** — Python: a guest GraphCompute algorithm cannot scope its projection or use edge-property
  weights (`edge_weights` returned `1.0`)
- **#152** — Rhai: expose the mutable scratch-graph primitives to Rhai guest algorithms
- **#148** — Implement Plugin Compute ABI phases 0–4

Also carried fixes touching **#53**, **#55**, **#106**, **#115**, **#131** and **#135**.
