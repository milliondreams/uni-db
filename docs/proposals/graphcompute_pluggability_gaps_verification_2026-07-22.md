# GraphCompute Pluggability Gaps — Source Verification

**Date:** 2026-07-22
**Status:** Verification report (no code changes; closure plan proposed, not scheduled)
**Source of claims:** `uniscape/packages/uniscape-mcts/docs/UNIDB_PLUGGABILITY_GAPS.md` —
a field report of 12 gaps hit while building `uniscape-mcts` (arena / kernel MCTS
rollout) and `uniscape-influence` (influence maximization) on uni-db's GraphCompute /
Rhai-plugin surface. That report is written from the **guest author's chair** against a
read-only snapshot of `uni`.

**This document** re-checks every claim against the **live** `uni` source from the
**engine maintainer's chair**, and classifies each as:

- **REAL** — the behavior exists as described and is a genuine gap/bug.
- **REAL but by-design** — the behavior exists, but is a deliberate contract; the gap is
  documentation, not correctness.
- **PARTIAL** — the core claim holds but the report over-states cause or blast radius.
- **MISCHARACTERIZED** — the operative conclusion is false; the capability already exists.

Every verdict below carries `file:line` evidence collected by direct source reading.

---

## Scorecard

**8 REAL · 1 REAL-but-by-design · 1 PARTIAL · 2 MISCHARACTERIZED.**

The report is ~75% accurate — strong for a black-box field report. The two largest
*structural* complaints (G4 "can't pass a set," G8 "no cheap snapshot") are both
**MISCHARACTERIZED**: the capability already ships and the guest author did not discover
it. That is itself a finding — a **discoverability** failure, not a capability failure.

| Gap | Kind | Verdict | One-line |
|-----|------|---------|----------|
| G1  | CAP  | ✅ REAL | arena has no `parent`/`backup`/path-descend → no value backprop above depth-1 (but `parent_of` already exists, private) |
| G2  | CAP  | ✅ REAL (half) | no `Sqrt` MapOp → true UCB uncomposable (`div` *is* composable via recip+mul) |
| G3  | CAP  | ⚠️ REAL by-design | `arena_descend` vloss is a flat linear score offset, not per-visit UCB — a documented-contract gap |
| G4  | CAP  | ❌ MISCHAR | variable-length set args already work (`ArgType::CypherValue`; Rhai `gc.frontier([...])`) — narrow real sub-gap only |
| G5  | CAP  | ✅ REAL | `expand_masked` out-only (`0x86E`); `sample_edges` draws per CSR half-edge |
| G6  | PERF | ⚠️ PARTIAL | no native BFS-fixpoint primitive = real; but O(V·E) claim is wrong — uni-db's own reach is O(V+E) |
| G7  | PERF | ✅ REAL | `sample_edges` draws the whole edge mask eagerly per call; no fused frontier sampler |
| G8  | PERF | ❌ MISCHAR | fork ~10ms cost is real, but fork-free read snapshot already exists (`pin_to_version`) |
| G9  | TRAP | ✅ REAL | default projection = *all* schema labels/edge-types, silently → CSR pollution |
| G10 | TRAP | ✅ REAL | `emit` keys `nodeId` to the projected input graph, not arena slots → opaque Arrow error |
| G11 | TRAP | ✅ REAL | undeclared-label nodes are silently absent from projections |
| G12 | TRAP | ✅ REAL (worse) | in-memory stored properties read NaN (`ctx=None` skips L0); edge weights silently default to `1.0` |

---

## Tier 1 — TRAP (silent wrong answer). The correctness debt uni-db actually owes.

These four return a **wrong-but-plausible answer with no error raised**, which violates the
"no silent drop" discipline enforced elsewhere in this codebase. Regardless of design
intent, a silent wrong answer is indefensible from the engine's chair. This is the tier to
close first.

### G9 — default projection projects every schema label/edge-type — **REAL**

`GraphProjectionSpec` documents "empty `node_labels`/`edge_types` mean 'all'"
(`crates/uni-plugin/src/traits/algorithm.rs:318`); `from_config_object` leaves both
`unwrap_or_default()` empty when no config is given (`:363-370`). The empty→all expansion
is in `ProjectionBuilder::resolve_ids`:

```rust
if label_ids.is_empty()     { label_ids = schema.labels.values().map(|m| m.id).collect(); }
if edge_type_ids.is_empty() { edge_type_ids = schema.edge_types.values().map(|m| m.id).collect(); }
```

`crates/uni-algo/src/algo/projection.rs:364-370`. So `gc.graph()` with no scoping pulls in
every declared label/edge type. During an MCTS search the DB also holds `:MCTSNode`/
`:PARENT` search-tree data, which lands in the CSR; because `sample_edges` keys per CSR
edge index (G5b), the extra edges **shift `:E` indices** → the same seed set yields a
different spread as the tree grows. No error.

**Relationship to #151 (projection parity, `graphcompute_projection_parity_2026-07-19.md`):**
#151 *added* `{nodeLabels,edgeTypes}` scoping plus a fail-closed guard — but **did not change
the default**. The guard fires **only under a restricted HostQuery scope** (`bridge.rs:252-270`,
error `0x804`). Under the default `**`/`*` grant — what native first-party and the Python
`"HostQuery"` string resolve to — `scope_restricted == false`, so an unscoped projection
**silently projects the whole graph**. A closed ticket sits directly beside a live
silent-corruption default.

**Fix shape:** require explicit `nodeLabels`/`edgeTypes`, *or* warn/error on an unscoped
projection even under a broad grant, *or* (least) document that the default is "everything"
and index-keyed kernels are unsafe under it.

### G10 — `emit` keys to the projected input graph, not arena slots — **REAL**

`emit` only checks the emitted columns are mutually equal-length and stashes them; it
performs **no** check against projection vertex count (`session.rs:1473-1520`,
`self.emitted = captured`). Batch assembly for the generic guest path hardcodes the row
count to the input projection:

```rust
let n = graph.vertex_count();                       // adapter_algorithm.rs:205
// nodeId column: (0..n).map(|slot| graph.to_vid(slot))   // :210, :218-219
RecordBatch::try_new(schema, columns)               // :254
```

A guest that emits an arena-slot-keyed column of length ≠ `n` trips Arrow's
"all columns in a record batch must have the same length", surfaced as `FnError 0x15`
(`:254-255`). Note the asymmetry: the first-party `gcpagerank` path sizes `nodeId` from
`scores.len()` (`provider.rs:260-268`) so it *cannot* hit the mismatch; only the generic
guest path can.

**Fix shape:** either allow `emit` to key to a frozen-arena vertex space, or raise a clear
error naming the mismatch ("emit column length N ≠ projected input node count M").

### G11 — undeclared labels project empty, silently — **REAL**

`resolve_ids` looks each *named* label up in `schema.labels` (errors only if you explicitly
name an undeclared one, `projection.rs:347-353`); the empty→all path enumerates
`schema.labels.values()` exclusively (`:365-366`). `collect_vertices` then iterates only
those resolved `label_ids` (`:402-437`). There is no "unknown label" reconciliation — a
vertex whose label is not schema-declared is never enumerated and never errored.

**Fix shape:** warn (or include) when a projection's underlying graph holds
present-but-undeclared labels, rather than silently dropping them.

### G12 — in-memory stored property values read NaN — **REAL (worse than reported)**

The projection reads property values via the **batch** `PropertyManager` methods passing
**`ctx = None`**:

- node: `pm.get_batch_vertex_props(&vids, &name_refs, None)` (`projection.rs:614`)
- edge: `pm.get_batch_edge_props(&all_eids, &fetch_names, None)` (`projection.rs:532`)

In `get_batch_vertex_props` the L0 overlay is gated `if let Some(ctx) = ctx { … overlay_l0_batch … }`
(`crates/uni-store/src/runtime/property_manager.rs:469+`). With `ctx = None` the **L0 buffers
are never overlaid** — values come only from flushed Lance storage; missing values become
`f64::NAN` (`projection.rs:625` nodes, `:574` edges). The code comment states it:
"Property *values* are read from committed storage … so unflushed L0 weights fall back to
the 1.0 default" (`projection.rs:483-486`). Meanwhile graph **structure** *does* overlay L0
(`collect_vertices`, `projection.rs:424-437`), so topology projects correctly while
`gc.node_property`/`gc.edge_property` return NaN until flush — NaN then poisons any
downstream reduction.

**Worse than the report states:** the report says "edge_weights project fine in-memory."
They do not — edge *weights* travel the same `ctx=None` path and **silently default to
`1.0`** when unflushed (`projection.rs:552-556`). NaN at least fails *loudly* (visible NaN
in output); the weight path fails *quietly* with a plausible `1.0`.

**Fix shape (single chokepoint):** thread the read context through so the property read
applies the same L0 overlay the structure read already uses — or, if a context isn't
available on the projection path, fail loudly on an unflushed property read rather than
returning NaN/1.0.

---

## Tier 2 — CAP (missing expressiveness). Two near-free wins; the rest is docs.

### G1 — arena exposes leaves, not paths → no value backprop above depth-1 — **REAL**

The complete arena kernel surface is exactly **10 functions**, enumerated identically in
the `KernelId` catalog (`kernel_id.rs:158-167`), the `GraphArenaCompute` trait
(`session.rs:2186-2297`), and the Rhai `reg!` macro (`graph_compute.rs:901-910`):
`arena_new`, `arena_alloc`, `arena_link`, `arena_column`, `arena_candidates`,
`arena_gather`, `arena_scatter`, `arena_descend`, `arena_expand`, `arena_freeze`.

- No `arena_parent`, no `arena_backup`, no path-returning descend.
- `arena_descend` surfaces **only leaf slots** (`session.rs:2446-2452`); the traversed
  path's `inspected` count is consumed for budget charging, not returned.
- So a guest can `arena_scatter` a value onto a leaf but cannot walk to its ancestors —
  value backprop is expressible only at depth-1 (path = `[root, leaf]`). (Visit-count
  backprop *does* happen natively: `descend` increments `visit` and offsets `score` along
  the full path in-loop.)

**Near-free fix:** `GraphArena::parent_of(slot) -> Option<u32>` **already exists**
(`arena.rs:217`) — it's currently used only in arena unit tests (`arena.rs:422-433`), never
promoted to a `KernelId`/trait method/Rhai registration. Exposing `arena_backup` (add
`delta` to `value_col` along a leaf's root path) or `arena_parent` is plumbing an existing
private function through the 3 registration points — not new algorithm work. This
reclassifies G1 from "big capability unlock" to "cheap exposure."

### G2 — no element-wise `sqrt`; no element-wise `div` — **REAL (the `sqrt` half)**

`MapOp` has exactly 5 variants — `Normalize`, `Scale`, `AxPlusB`, `Recip`, `Log`
(`session.rs:69-80`, match arms `:1252-1270`). `EwiseOp` has exactly 5 — `Add`, `Mul`,
`Min`, `Max`, `Axpy` (`session.rs:93-104`). The Rhai string decoders match exactly
(`graph_compute.rs:106-113` maps, `:247-252` ewise) and are catalog-driven, so they cannot
silently diverge from the enums.

- **`div` is *not* a real gap** — `a/b = a·recip(b)` composes via `Recip` (with the
  `recip(0)=0` convention) + `Mul`. Half of G2 is ergonomics/discoverability.
- **`sqrt` is genuinely blocked** — no `Sqrt`, and not composable: `sqrt(x)=exp(0.5·ln x)`
  needs an `Exp` MapOp that does not exist (there is `Log`, but not its inverse). So the
  canonical UCT term `c·√(ln N / n)` cannot be built. uni-db's *own* shipped MCTS template
  concedes this — `dispatch.rs:1071` computes UCB as `w/v + c²·(1/v)`, dropping the `sqrt`.

**Near-free fix:** add a `Sqrt` variant (and ideally `Exp`) — one enum variant + one match
arm in each of `session.rs` and `dispatch.rs`, plus one Rhai decoder string. Unlocks true
UCB/PUCT composition.

### G3 — arena virtual loss is linearized, not per-visit UCB — **REAL but by-design**

`apply_visit` (`arena.rs:348-355`) is called at **every** descent step (`arena.rs:323`):
`visit[slot] += 1.0`, then `if vloss != 0 { score[slot] += (maximize ? -vloss : +vloss) }`
— a **flat linear offset on the precomputed score column**, *not* recomputed from
`visits + vloss`. Child selection is strict `>` (ties → lower slot, `arena.rs:329-339`).
The guest owns the score formula (composes UCB and `arena_scatter`s it into the score
column, `graph_compute.rs:1074-1077`); `descend` only linearly perturbs it.

This is accurate but a **deliberate contract**, not a bug — the report itself grants it is
"fine as-is *if* the contract is explicit." The only real gap is that it silently differs
from the canonical MCTS virtual-loss most guests will assume.

**Fix shape:** document `arena_descend`'s vloss semantics, or add a
`vloss_mode: "linear" | "visit"` flag. No correctness fix required.

### G4 — algorithm args are fixed scalars, no list/set — **MISCHARACTERIZED**

The operative conclusion ("cannot pass a variable-length seed set; must generate the plugin
per-arity and pad with `-1`") is **false from every chair**.

- **Rust surface:** `ArgType` has four variants, not scalars-only — `Primitive`,
  `CypherValue` (opaque, accepts any JSON incl. arbitrary-length arrays), `Vector`,
  `Variadic` (`crates/uni-plugin/src/traits/scalar.rs:81-95`). `json_matches_argtype`
  accepts arrays for both `CypherValue` and `Vector` (`algorithm.rs:184-201`). The
  first-party `gcpagerank` **dogfoods** a variable-length seed set: it declares
  `sourceVid: ArgType::CypherValue` "accepts a single vid *or* an array of vids"
  (`provider.rs:55-61`) and `parse_config` accepts a `Number` or an `Array` of vids
  (`provider.rs:130-146`). No sentinel, no per-arity generation.
- **Rhai guest surface (reachable today):** because `build_algorithm_signature` builds
  `AlgorithmSignature` from `entry.yields` only and **never references the manifest `args`**
  (`crates/uni-plugin-rhai/src/loader.rs:346-362`), a Rhai algorithm's `args` is always
  empty → `coerce_config_json` is a no-op (`algorithm.rs:132-134`) → no arity/type
  rejection. The adapter deserializes the raw positional JSON array and converts each
  element (arrays included) to a Rhai `Dynamic` via `rhai::serde::to_dynamic`
  (`adapter_algorithm.rs:89-105`). Host kernels consume arrays natively:
  `GcSession::frontier(&mut self, g, seeds: Array)` (`graph_compute.rs:153`). Concrete
  reachable pattern:

  ```rust
  // manifest: algorithms: [ #{ name: "ppr", args: ["int"], yields: [...] } ]
  fn ppr(gc, source) { let f = gc.frontier(gc.project(), [source]); ... }
  // CALL ppr([1,2,3,4]) → `source` arrives as a Rhai array of length 4.
  ```

**The one genuine, narrow sub-gap:** the Rhai manifest string vocabulary is
scalar-primitive-only — `type_name_to_datatype` (`wire_translate.rs:26-32`) accepts
`float/int/string/bool/null` families and errors on anything else, with no
`value`/`list`/`array` token. And for *algorithms* the declared `args` are **silently
unvalidated dead metadata** (parsed into `AlgorithmEntry.args` at `manifest.rs:237`, never
read again). So a Rhai author cannot *declare* an array arg and gets no validation — an
honesty/ergonomics gap in the manifest surface, not a functional block.

**Fix shape:** either wire the Rhai manifest parser to accept a `CypherValue`/list token,
or stop silently ignoring declared algorithm `args` (validate them, or document that they
are advisory). Plus a docs example showing `gc.frontier([...])`.

### G5 — `expand_masked` out-only; `sample_edges` per half-edge — **REAL**

- `expand_masked` rejects any non-`Out` direction: `if !matches!(dir, Direction::Out)
  { return Err(error::arg_validation("expand_masked is defined on the out-CSR; use
  Direction::Out")); }` (`session.rs:1799-1803`, error `0x86E` at `error.rs:54`). The
  `Direction` enum has only `Out`/`In` — **no `Both`** (`session.rs:60-65`). `spmv_masked`
  is restricted identically.
- `sample_edges` keys each Bernoulli on the flat CSR edge index:
  `for (edge, &p) in probs.iter().enumerate() { if sample_bernoulli(p, seed, iter, edge as u64) …`
  (`session.rs:1722`), where `elem = edge` feeds `counter_hash` (`uni-algo/src/algo/rng.rs:122-129`).
  An undirected edge stored as two directed half-edges gets two distinct `edge` indices →
  two independent draws; no "undirected link up/down as a unit."

**Fix shape:** a `dir="both"`/undirected expansion, and/or a `sample_edges` variant keyed on
an undirected-edge id so both halves share a draw.

---

## Tier 3 — PERF. One over-claim; the rest is real but heavier work.

### G6 — no BFS-fixpoint primitive; "the obvious loop is O(V·E)" — **PARTIAL**

- **"No native BFS-to-fixpoint / reachability kernel" — REAL.** The native kernel table
  (`kernel_id.rs:104-146`) exposes only single-step/stateless primitives (`frontier`,
  `expand`, `expand_masked`, `spmv`, `spmv_masked`, set ops, `sample`, `sample_edges`).
  There is no `reach_fixpoint`/BFS-loop kernel that does frontier bookkeeping internally.
  Reachability is composed in guest-level code.
- **"The natural loop is O(V·E)" — MISCHARACTERIZED for uni-db's own code.** uni-db's
  shipped `reachable_set` (`crates/uni-plugin-builtin/src/algorithms/first_party.rs:116-130`)
  does **not** re-pass `visited`; it passes only the new frontier and uses `visited` purely
  as the `exclude` arg — textbook O(V+E) frontier BFS. The O(V·E) blowup the report
  describes lived in the *guest's* `reach.py` (uniscape side), which re-passed the whole
  `visited` set. The correct O(E) pattern was already available *and demonstrated* in
  uni-db's first-party code.

**Fix shape (ergonomic, not corrective):** a single-CALL `reach_fixpoint(g, seeds, mask,
dir) -> visited_set` so guests can't accidentally write the quadratic version — "make the
pit of success deeper," not a bug fix.

### G7 — `sample_edges` samples all edges eagerly per simulation — **REAL**

`sample_edges` copies all probabilities (`t.values().to_vec()`, `session.rs:1718`) then
iterates **every** edge (`:1722`), hashing each regardless of frontier/cascade size — O(E)
hashes per call. No fused frontier-scoped sampler (`expand_sampled` or any `*_sampled`
traversal) exists in `kernel_id.rs`, the trait, or `dispatch.rs`. So on sparse cascades the
native kernel does *more* work than a lazy Python IC; it wins only when cascades are large
or simulations-per-reward is high — which is why the honest flagship number is 3.8×.

**Fix shape:** a frontier-scoped/lazy `expand_sampled(g, frontier, dir, exclude, prob,
seed, it)` that fuses draw+expand and only samples the current frontier's out-edges.

### G8 — fork create/drop too expensive for per-rollout use — **MISCHARACTERIZED**

Two sub-claims; the cost is real, the "no alternative" conclusion is false.

- **Cost — REAL.** `create_fork_2pc` (`crates/uni/src/api/fork.rs`) is an O(#datasets)
  object-store sequence: flush+capture fork point (`:262-294`), `begin_create` PUT
  registry Pending (`:326`), allocator PUT (`:336`), **one `lance_branch::create_branch`
  per dataset** (`:354`, calls `:487`/`:535`; the dataset set is `vertices`, `edges`, one
  `vertices_{label}` per label, four tables per edge type — `manager` at `:389-412`),
  `finish_create` PUT registry Active (`:376`), plus per-fork WAL under `wal_forks/{id}/`.
  So ≥2 registry PUTs + 1 allocator PUT + N Lance branch-manifest writes + WAL setup.
  ~10ms/call is entirely plausible; drop is symmetric.
- **"No cheap transient read-snapshot" — FALSE.** A fork-free, read-consistent snapshot
  already ships and is on the hot path:
  - `StorageManager::pinned_at_version(hwm)` (`crates/uni-store/src/storage/manager.rs:514`)
    — an in-memory `StorageManager` clone (Arc-clones + fresh `AdjacencyManager`), no
    branch/registry/WAL/allocator. Already built **per read-write transaction**
    (`crates/uni/src/api/transaction.rs:262`). This *is* the cheap snapshot the report says
    doesn't exist.
  - `Session::pin_to_version(snapshot_id)` → `at_snapshot` → `StorageManager::pinned(manifest)`
    (`session.rs:980`, `mod.rs:842`, `manager.rs:460`) — a durable, multi-query, read-only
    pinned view (writes rejected via `UniError::ReadOnly`); no branch, no 2PC.
  - A plain read session already gets MVCC snapshot isolation without any fork.

**Caveat (why not "already-fixed"):** pinning is read-only by construction. A rollout that
genuinely **mutates** in isolation (speculative writes then discard) still needs a fork —
the ~10ms cost is unavoidable *for that use case*. But the report's own workaround
(`rollout="graph_readonly"`, which skips the fork) is a read-only case, and for those
`pin_to_version` already delivers the fork-free snapshot.

**Fix shape:** document `pin_to_version` for read-only rollouts (closes the report's own
grievance). A cheaper write-isolated transient fork is real, deeper engine work — defer.

---

## Cross-cutting findings

1. **The correctness debt is the TRAP tier (G9/G11/G12, plus G10's opaque error).** All
   four return a wrong-but-plausible answer with no error. That is the class this codebase's
   "no silent drop" discipline exists to prevent, and it is indefensible independent of
   design intent. **G12 has a single root cause** (property reads pass `ctx=None` and skip
   the L0 overlay that structure reads already apply) — the cleanest high-value fix.

2. **Two "gaps" are near-free because the primitive already exists.** G1 needs the existing
   private `parent_of` (`arena.rs:217`) promoted to `arena_backup`; G2 needs one `Sqrt`
   enum variant. Neither is algorithm work.

3. **The two MISCHARACTERIZED gaps (G4, G8) are discoverability failures, not capability
   failures.** Variable-length set args (`CypherValue` / `gc.frontier([...])`) and fork-free
   read snapshots (`pin_to_version`) both ship today; the guest author reached for a
   workaround instead. The lesson for uni-db is documentation + one honesty fix (G4's
   dead-metadata algorithm `args`), not new features.

4. **`#151` is half-closed.** It added projection scoping but left the default at
   "everything," and its guard only fires under a restricted scope. G9 is the live other
   half of that ticket.

---

## Proposed closure tiering (not scheduled)

| Priority | Gaps | Rationale | Effort |
|----------|------|-----------|--------|
| **P0 — correctness (fail loud, not silent-wrong)** | G12, G9, G11, G10 | Silent wrong answers; violate "no silent drop" | G12 = 1 chokepoint; others small |
| **P1 — near-free capability** | G2 (`Sqrt`), G1 (`arena_backup`) | Primitive already exists / one enum variant | Small |
| **P2 — docs & honesty** | G3 (vloss docs), G8 (`pin_to_version` docs), G4 (dead-metadata `args`) | Capabilities exist; close the discoverability gap | Docs + 1 small fix |
| **P3 — real engineering, defer** | G5 (`dir=both` + undirected sample), G6 (`reach_fixpoint`), G7 (`expand_sampled`), G8-write (cheap write-isolated fork) | New kernels / engine work | Medium–large |

**Recommended first move:** P0 — the silent-wrong-answer group is the only tier uni-db
cannot defend as "by design."

---

## Verification method

Twelve claims were checked by six parallel read-only source sweeps over the live `uni`
workspace (arena; element-wise ops; algorithm args + Rhai manifest follow-up; expand/sample
kernels; fork cost + pinning; projection traps). Each claim was held as a falsifiable
hypothesis and confirmed or refuted against `file:line` evidence — enum definitions, trait
surfaces, kernel dispatch, and the projection read path — never from the report's own
assertions. Key files: `crates/uni-plugin-builtin/src/algorithms/graph_compute/{arena,session,dispatch}.rs`,
`crates/uni-algo/src/algo/projection.rs`, `crates/uni-store/src/runtime/property_manager.rs`,
`crates/uni/src/api/{fork,transaction}.rs`, `crates/uni-plugin-rhai/src/{loader,wire_translate,adapter_algorithm,graph_compute}.rs`,
`crates/uni-plugin/src/traits/{algorithm,scalar,procedure}.rs`.
