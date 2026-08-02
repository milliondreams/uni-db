# Duplication / dead-code / simplification audit — 2026-07-30

Source: 14-agent workflow (7 domain scouts + 7 adversarial verifiers) over the
full Rust workspace. 56 findings raised, **24 CONFIRMED, 31 PARTIAL, 1 REFUTED**.
Every finding was re-verified against the source by a second agent whose default
position was that the finding is wrong; LOC and risk below are the *verifier's*
re-estimates, not the scout's.

Confirmed removable: **~3,980 LOC**. Partial (real, but the proposed action needed
rewriting): **~4,314 LOC**.

> Nothing in this document has been executed. No file was modified by the audit.

---

## 0. Correctness bugs found as a byproduct — do these first

These are not cleanup. They are live wrong answers that the duplication produced.

### 0.1 UDF vs interpreter semantic drift (CONFIRMED, medium)

Both evaluation paths are live: `read.rs:2368` and `locy_eval.rs:551` call
`expr_eval::eval_scalar_function`; `unwind.rs:480` calls `expr_eval::eval_split`
directly; the DataFusion path calls the registered UDFs. They disagree:

| Expression | `expr_eval` | `df_udfs` |
|---|---|---|
| `toString(1.0)` | `"1.0"` (`format!("{f:.1}")`, expr_eval.rs:1290) | `"1"` (`f.to_string()`, df_udfs.rs:2096) |
| `toString(list/map)` | `Value::String(..)` (expr_eval.rs:1300) | `TypeError: InvalidArgumentValue` (df_udfs.rs:2099-2112) |
| `toInteger("3.7")` | `Null` (expr_eval.rs:1266) | `Int(3)` — f64 fallback (df_udfs.rs:2800-2808) |
| `toBoolean(1)` | `Err` (expr_eval.rs:1318) | `Bool(true)` (df_udfs.rs:2952) |
| `substring(s, -1, n)` | `Err` on negative start (expr_eval.rs:1588) | clamps `start.max(0)` (df_udfs.rs:6178) |
| `substring(s, 0, -1)` | whole tail (`as usize` → huge, clamped at 1602) | `Err: NegativeIntegerArgument` (df_udfs.rs:6190) |
| `split("a", null)` | `Err` (expr_eval.rs:1572-1573) | `Null` (df_udfs.rs:6257) |

`substring` is inverted in **both** directions. Prior art:
`crates/uni-query-functions/tests/repro_df_udfs_sync.rs` documents two earlier bugs
of this exact class that existed only in the DF copy.

**Action.** Not a mechanical body-swap. Per pair, decide the openCypher-correct
answer and change *both* sides to it, then collapse. Three constraints:
1. `eval_tostring`/`eval_tointeger`/`eval_toboolean`/`eval_substring` are private
   `fn` — they need `pub(crate)` plus an `anyhow` → `DataFusionError` adapter.
2. The DF UDFs propagate `Null` when *any* argument is null (df_udfs.rs:6151, 6257);
   `expr_eval` only special-cases arg 0. **Adopt DF's whole-arg propagation** — it is
   the openCypher rule — and change `expr_eval` to match.
3. DF error strings carry openCypher error-code prefixes
   (`ArgumentError: NegativeIntegerArgument`, `TypeError: InvalidArgumentValue`) that
   TCK scenarios match on. Keep the prefixed strings as the shared text.

**Land the differential test first** — one input table driven through both
`eval_scalar_function` and the registered UDF — so the chosen semantics are pinned
before any collapse.

### 0.2 `cypher_value_cmp` is missing its `Value::Temporal` arm (PARTIAL, 4-line fix)

`df_udfs.rs:6621` falls to `_ => None` for temporals while
`expr_eval::cypher_partial_cmp` (742-813) does full temporal comparison. List-of-dates
comparison goes incomparable on the UDF path only. Fix independently of any refactor.

---

## Tier 1 — confirmed, low risk, mechanical (~1,300 LOC)

Ordered by value. All verified reachable-nowhere by a second agent that checked
PyO3 exports, string-keyed plugin registration, macro dispatch, dyn-trait impls,
cucumber TCK steps, `tests/`, `examples/`, `benches/`, and `.py` files.

| # | Finding | LOC | Where |
|---|---|---|---|
| 1 | **`GraphScanExec` edge-scan path is unreachable** — `new_edge_scan` has zero callers repo-wide, so `is_edge_scan` is always `false` and the whole `poll_next` arm plus 6 transitive callees are dead. `build_edge_schema` is alive only via its own unit test. Real edge reads go through `GraphTraverseExec`. | 570 | `df_graph/scan.rs` |
| 2 | **7 unused `find_*` lookups** on `MainVertexDataset`/`MainEdgeDataset` plus `scan_vids`, `extract_eids`, `extract_edges_from_batch`. Keep `with_version_bound`. | 220 | `storage/main_vertex.rs`, `main_edge.rs` |
| 3 | **Hand-rolled `InMemoryExec` + 3 byte-identical `TestMemoryExec` copies** reimplement DataFusion's `MemorySourceConfig::try_new_exec`. | 200 | `df_graph/locy_{fixpoint,best_by,priority,fold}.rs` |
| 4 | **`apply_anti_join` / `apply_prob_complement`** fully superseded by their `_composite` variants; zero callers. | 154 | `df_graph/locy_fixpoint.rs` |
| 5 | **`api/query_builder.rs` is a whole dead module**, superseded by `session::QueryBuilder`. | 133 | `crates/uni/src/api/` |
| 6 | **`ProbabilityConfig` threaded through 3 builder signatures, never read** — the receiving param is literally named `_prob_config`. Its own `#[allow(dead_code, reason=...)]` admits it. The two values it wraps already reach the runtime via `LogicalPlan::LocyProgram`. 26 of the LOC are test-site boilerplate. | 130 | `query/locy_planner.rs` |
| 7 | **`overlay_edge_batch` + `overlay_edge_from_l0` unreachable**; `overlay_vertex_batch` alive only via a benchmark that therefore measures nothing real. | 105 | `runtime/l0_visibility.rs` |
| 8 | **8 unreferenced `pub` items** in `uni-plugin` / `uni-plugin-host`. | 85 | `registrar.rs`, `registry.rs`, `batch_builder.rs` |
| 9 | **lancedb-removal orphans** — `LanceDbBackend::fork_branch` dead, `dataset_uri` triplicated. | 55 | `backend/lance.rs` |
| 10 | **`impl_query.rs` pipeline copy-pasted 9×**, including a verbatim-duplicated 5-line comment (a past bug patched into 2 of 9 copies). CLAUDE.md's Phase-5a invariant makes this an invariant-spread hazard, not just verbosity. | 30 | `crates/uni/src/api/impl_query.rs` |
| 11 | **`IndexDefinition` repeats the same 6-arm match 4×.** Mirror the existing `for_each_*` macro idiom at `uni-crdt/src/lib.rs:64-85`. | 30 | `common/core/schema.rs` |
| 12 | **`arg_string_list` re-implemented inline 2× in the file that defines it.** | 20 | `uni-algo/procedure_template.rs` |

### Semver note — applies to items 1, 2, 4, 5, 7, 8

`uni-query`'s `pub mod query` → `pub mod df_graph` → `pub mod scan` chain, `uni-store`'s
`pub` storage modules, `uni-plugin`'s registry, and `crates/uni`'s `pub mod api` all make
these public API of published crates, even though nothing in-repo names them. Land under
`refactor!`/`feat!` per this repo's convention, batched into one breaking bump.

**Item 1 extra:** after deleting `is_edge_scan`, keep the `DisplayAs` string as a constant
`"Vertex"` rather than dropping the field from output — grep `compliance_reports/` and tests
for `"GraphScanExec: Vertex"` first so no EXPLAIN golden changes shape.

---

## Tier 2 — confirmed, needs care (~1,600 LOC)

| Finding | LOC | Risk | The catch the verifier found |
|---|---|---|---|
| **61 hand-written `ScalarUDFImpl` shells** in `df_udfs.rs`, each ~35 lines of struct/`new`/`as_any`/`name`/`signature`/`return_type`. Collapse to a table + generic `ValueUdf`. | 900 | med | **Sequence after §0.1.** Collapsing shells while the bodies still disagree just relocates the drift. `name()` strings are matched by `df_expr.rs` at planning time — the table's name column must be byte-identical. The `pub create_*_udf` constructors must survive as thin wrappers. |
| **Plugin wire-manifest schema duplicated between Extism and Component-Model loaders — and already drifted.** Extism's `WireArgType` has `Primitive/CypherValue/Vector/Variadic`; the CM loader's has only the first two. **A manifest declaring a `vector` or `variadic` arg loads over Extism and is rejected by the CM loader.** `build_algorithm_signature` is character-identical apart from the error variant. | 250 | med | The shared type needs the *union* of derives (Extism has `Serialize + PartialEq + Eq`, wasm only `Deserialize`) and must keep `RegistrationEntry::qname()`. Extism's `wire_translate` names are published API (`lib.rs:123-126`) — keep as re-exports. **Unifying `WireArgType` silently widens the CM wire contract** — land that as a separate, separately-tested change with a CM test that a `kind:"vector"` manifest parses *and* round-trips a `FixedSizeList` arg. Precedent for the shared module: `uni_plugin::adapter_common::arrow_types` (its doc records this same union-of-two-loaders unification being done once before). |
| **`l0_visibility.rs` hand-unrolls the same L0-chain traversal in ~11 mirrored vertex/edge functions.** | 200 | med | Keep every `record_vertex_read`/`record_edge_read` call **outside and before** the walk exactly as today — the SSI read-set antidependency contract depends on it. `test_keyed_reads_recorded_in_ssi_read_set` is the guard rail; run it plus the OCC/SSI suites before and after. `*_exists_in_l0`'s pending-buffer order flips to newest-first. Introduce the `L0Keyed` trait for the tombstone/props pair **only** — label/type/endpoint accessors aren't mirrored. |
| **Host-service policy duplicated across Extism and Rhai loaders** — capability attenuation, timeout resolution, hex framing implemented independently. | 120 | med | Two steps. Step 1 alone: move `to_hex`/`from_hex` + tests into `uni_plugin::hex`, pure code motion. Step 2: `uni_plugin::host_services` owning the attenuation check, `DEFAULT_TIMEOUT`/`MAX_RESPONSE_BYTES` and the `status >= 400` gate. Preserve Extism's `0xC0x`/`0xC2x` `FnError` codes exactly. Thread `traceparent` as a parameter so `dispatch_injects_active_traceparent` still anchors. |
| **15 of 21 `TemporalValue` accessors dead**; the live implementation is an independent second one in `uni-query-functions/datetime.rs` — and they diverge (`div_euclid` vs `truncate`). | 108 | med | Keep `year`…`second`, `to_date`, `to_time`, `temporal_type`, `format_offset`. Do **not** re-add any as thin delegations — the semantics differ. |
| **`detach_delete_vertex`/`check_vertex_has_no_edges` are N=1 cases of the existing `batch_*` pair**, split across the read/write file boundary. | 100 | med | **Naive consolidation double-deletes**: `batch_detach_delete_vertices` performs `writer.delete_vertex` internally at `read.rs:5634`, and `write.rs:4065` performs it again unconditionally. `batch_load_incident_edges` is private and `write.rs` is a sibling module — needs `pub(crate)`. Reconcile the TCK-asserted error string in the *batch* direction first (`read.rs:5689` uses `edge.src_vid` where the openCypher text means the vid being deleted). |
| **`types.rs` ships 3 vertex/edge pyclass families + 2 id-newtypes as hand-copied twins.** | 100 | low | Have the macro accept per-getter doc strings so `PyEdgeDiff` doesn't silently inherit `PyVertexDiff`'s docstrings. Do **not** macro-ize `PyVertexPropertyChange`/`PyEdgePropertyChange` — their identity getters differ in arity. |
| **`build_traverse_output_batch_sync` is a hand-copied specialization** of the async version with hydration inlined away. | 95 | med | The sync core must keep taking **borrows** so the `poll_next` fast path at 1313-1330 still avoids eight `.clone()`s and the `Box::pin`. Verify with the traverse benches. |
| **Locy clause-eval block copy-pasted verbatim** between the recursive fixpoint loop and the non-recursive stratum loop. | 75 | med | Fixpoint negation semantics — a behaviour diff would be silent. Land as a pure no-op refactor with the IS NOT / PROB suite plus the `uni-locy-tck` cucumber run green on both sides. |

---

## Tier 3 — structural, worth a design decision (from PARTIAL)

These are real but large enough that the *approach* is the question, not the edit.

- **`graph-compute-kernel-surface-triplicated`** (~450 LOC, **high risk**) — every GraphCompute
  kernel is marshalled three times: JSON dispatch, Rhai methods, PyO3 methods.
- **`expr-walker-proliferation-no-visitor`** (~400 LOC) — 13 hand-rolled full-`Expr` recursive
  walkers in `planner.rs`, plus 2 more in `df_planner.rs`/`locy_planner.rs`, each re-enumerating
  the same ~20 `Expr` variants. Pairs with **`logicalplan-walker-boilerplate`** (~180 LOC, 5
  exhaustive `LogicalPlan` walkers re-listing all ~55 variants) and
  **`logicalplan-rewriter-field-copy-boilerplate`** (~200 LOC, 5 rewriters each hand-rebuilding
  `Traverse`'s 19 fields). **Common root cause: `LogicalPlan` and `Expr` expose no visitor/`map_children`.**
  One `TreeNode`-style trait retires ~800 LOC across three findings — this is the single
  highest-leverage structural change in the audit.
- **`wasm-extism-adapter-quartet`** (~400 LOC) — the four Extism adapters and four Component-Model
  adapters are a verbatim port of each other. Same root as the wire-manifest finding above.
- **`py-db-async-sync-mirror-macro`** (~200 LOC, **high risk**) — `Database` ↔ `AsyncDatabase` is a
  44/44 method mirror over an identical `Arc<Uni>` receiver. **Mechanisable, but only via a
  whole-impl-block macro.** CLAUDE.md documents the mirror as a deliberate cross-language symmetry
  contract, so this is a judgement call, not a defect. `DatabaseBuilder` ↔ `AsyncDatabaseBuilder`
  (~65 LOC) is 100% duplicated config code with zero runtime involvement and is the safe subset.
- **`arrow-decode-two-implementations`** / **`arrow-encode-two-implementations`** (~260 LOC, one
  rated **high**) — two full Arrow↔`Value` codecs in `uni-store`. Encode side is high-risk; treat
  decode and encode as separate decisions.

---

## Explicitly NOT to do

- **`crates/uni/src/api/sync.rs` (`UniSync`, 830 lines) — REFUTED.** Claimed dead. It is
  constructed in five tests in `crates/uni/tests/common/e2e/api_sync_test.rs` (lines 10, 48, 77,
  97, 113), wired in via `common/e2e/mod.rs:3` → `integration.rs:30-31`, so it runs on every
  `cargo nextest run`. It is also publicly documented at
  `website/docs/reference/rust-api.md:1126-1184`. **Do not touch it.** The one narrow real
  observation: `UniBuilder::build_sync` (`api/mod.rs:4028`) has no code caller — add a line to the
  existing test rather than deleting it.
- **`invoke_cypher_string_op` → `invoke_cypher_udf`** — the semantics flip. The UDF returns `NULL`
  for a non-string operand; `eval_string_predicate` returns `Err`. `WHERE 5 CONTAINS 'a'` would go
  from a filtered-out row to a query error. It is also the WHERE-clause hot path: the current code
  does one downcast then a tight `iter().map()`, the shared path does a per-row downcast and a
  fresh `Vec<Value>`.
- **`RangeUdf` → `values_to_array`** — outright broken as proposed: `values_to_array`
  (`arrow_convert.rs:1365-1388`) accepts only `Utf8` as `List` inner and errors on `Int64`, which is
  `RangeUdf`'s declared return type. Needs a `List(Int64)` arm added first.
- **Blanket-delegating `cypher_value_cmp` → `cypher_partial_cmp`** — `(Null, Null)` returns
  `Some(Equal)` in the former and `None` in the latter. Delegating flips `[null,1] < [null,2]` from
  `true` to `NULL`. Do the rank-table extraction (~40 LOC, safe) and the missing-`Temporal`-arm fix
  (4 LOC) separately. Note there is a **fifth** rank table at `df_udfs.rs:6771` for the min/max
  UDAFs with deliberately different semantics — leave it.
- **`CypherToFloat64Udf`** — its `_ =>` all-null fallback and single-downcast builder loop are
  load-bearing on the `df_planner.rs` arithmetic path (12 call sites).

---

## Suggested sequencing

1. **§0.2** — 4-line temporal-arm fix. Standalone.
2. **§0.1 differential test** — pin UDF-vs-interpreter semantics before touching either.
3. **§0.1 reconciliation** — fix the seven divergences, both sides, TCK green.
4. **Tier 1 items 1, 4, 6, 7, 9, 10, 11, 12** — mechanical deletions, one commit each.
5. **Tier 1 items 2, 5, 8** — batch into the breaking bump with items 1/4/7.
6. **Tier 2** in the order listed; each wants its own test-green-before-and-after cycle.
7. **Tier 3 visitor/`TreeNode` trait** — design first. Highest leverage (~800 LOC across three
   findings) but touches the planner core.

Raw per-finding evidence, verifier reasoning and refined actions:
workflow run `wf_7777710d-9fc`, journal at
`~/.claude/projects/-home-rohit-work-dragonscale-uni/<session>/subagents/workflows/wf_7777710d-9fc/journal.jsonl`.
