# Plugin Compute ABI — closing the third-party graph-compute gap

**Status:** Design proposal (not implemented). **Date:** 2026-07-13.
**Supersedes/extends:** `docs/proposals/graphcompute_plugin_api_2026-07-10.md` (the GraphCompute v1 proposal).
**Relationship:** GraphCompute v1 shipped in 3.0.0. This proposal keeps its coarse-kernel core intact,
extends it additively (Mode A), draws the DataFusion-reuse boundary explicitly, and *deliberately
revises* two v1 decisions: the §5.6 ban on per-element guest callbacks (adding Mode B for workload
classes v1 kept native) and the §12 grant-as-clamp quota semantics (§9). Every load-bearing claim
below is grounded in the current source (file:line) or verified DataFusion behavior.

---

## 1. Problem

GraphCompute v1's thesis — *"third parties author graph algorithms in Rhai/Python/WASM/Extism,
no forking, no Rust"* — is only true for one species of algorithm: **deterministic propagation over a
fixed graph** (`state_{k+1} = f(A ⊗ state_k)`). That is exactly the six first-party algorithms shipped
to dogfood it. The moment an *outsider* brings an algorithm outside that envelope, the kernel catalog
has no vocabulary for it and the third party is stuck writing native Rust — which is not
extensibility, it's us doing it for them.

Two forcing counterexamples make the gap concrete:

- **Grid connectivity-reliability Monte Carlo** (uniscape): reachability over a *per-iteration random
  edge subset*, weighted reduce, ×N, distributional output. Its heart — reachability under a
  stochastic edge mask — cannot be expressed today.
- **The broader uniscape roadmap** — MCTS, agent-based models (ABM), system-dynamics (SD)
  simulation — plus "implement all of Neo4j APOC," third-party geo algorithms, GNN inference, and so
  on. These stress *different* parts of the design, and most fall in quadrants v1 excluded by
  construction.

The design risk is symmetric: v1 validated its kernels against a 12-algorithm F/C-class corpus and
claims ~70–90% coverage *of that corpus* — but the corpus itself was drawn from the propagation
family, so the kernels fit the quadrants v1 chose to see; we must not now overfit a new ABI to
grid-reliability the same way. So this proposal is derived from a **deliberately diverse workload corpus**
(Appendix A), with primitives falling out of what recurs and one-offs routed elsewhere.

## 2. What is true today (verified)

| Fact | Evidence |
|---|---|
| Kernels are a **closed, coarse, vertex-shaped** set; only stochastic kernel is `random_walks` | `graph_compute/session.rs:405` (trait), no `sample`/`bernoulli`/`random_tensor` |
| **No `[E]` (edge-indexed) tensor** — `Shape ∈ {V, Vd, D, Dd}`, all vertex/free-dim; per-edge data only as egress-only `Pairs` | `graph_compute/value.rs:48-57`, `handle.rs:73` |
| Tensors are **single-column, effectively-f64** (no struct/multi-field state). The dtype lift is *partially started*: `TensorBuf` already carries a live `I64` variant (i64 path-counting) and the `DType` tags `{F32,F64,I64,U32,Bool}` exist as reserved | `value.rs:12-18, 250-252`, `value.rs:30-41` |
| Graph projection is **immutable CSR** — no add/remove/grow-vertex/edge kernel | `handle.rs:66`, trait has none |
| Traversal masks are **vertex-sets**, not edge predicates; `EdgeFilter` deferred | `session.rs:499, 36-39` |
| Reductions are **fixed-order** (deterministic); seeded RNG (`walk_seed`, SplitMix64) is **private to `random_walks`** | `value.rs:9-10`, `session.rs:1082`, `uni-algo/.../random_walk.rs:59` |
| **Work budget can only be *lowered* by a grant, never raised** — `work_cap.map_or(size_budget, \|w\| w.min(size_budget))` at all 7 sites; `size_budget = min(10_000·(\|V\|+\|E\|+1), 1e9)` | `provider.rs:205`, `provider_walks.rs:266`, `provider_pairs.rs:212`, four loaders' `adapter_algorithm.rs`; `mod.rs:238-242` |
| Plugin **scalar/aggregate/window → real DataFusion `ScalarUDF`/`AggregateUDF`/`WindowUDF`**; aggregate is driven per-group under `GROUP BY`; **loader-agnostic** | `df_udfs_plugin.rs:519-558,148-163`; `df_udaf_plugin.rs:108-201`; `df_planner.rs:5090-5104`; registry stores `Arc<dyn AggregatePluginFn>` (`registry.rs:66`) |
| **Guest window functions are NOT authorable** — the DF bridge exists (`PluginWindowUdwf`) but `WindowPluginFn` has only a Rust test impl, no loader adapter | trait `uni-plugin/src/traits/window.rs:16`; bridge `df_udwf_plugin.rs:77`; no `adapter_window*` in any loader |
| A `CALL uni.algo.*` result **is a DataFusion `ExecutionPlan` node** (`GraphProcedureCallExec`) — a *leaf/source* node (`children()` empty; MATCH-bound vars enter via `outer_values`) that **fully materializes to one `RecordBatch`** before any row flows downstream — **gated by a hardcoded allowlist keyed on name prefix, not provenance** (`starts_with("uni.algo.")` + a fixed `uni.*` name set). Third-party names (`myco.algo.*`) fall to the row-based interpreter, which **still composes correctly** (the whole plan — WHERE/GROUP BY/ORDER BY — runs through `execute_subplan`); what's lost is DF's **vectorized engine**, not composability. A third party could reach the DF path today only by squatting the `uni.algo.` namespace | `procedure_call.rs:420,531-588,799-833`, `df_planner.rs:2110`; allowlist `read.rs:1160-1182`; fallback routing `read.rs:1228-1230`, `executor/procedure.rs:771` |
| DataFusion 53 **cannot host graph fixpoint** (recursive CTE is linear-only, off-by-default, buggy) and its **float `SUM` is not bit-reproducible** (partitioned nondeterministic combine) | DF #9554/#9680/#9804; float-assoc reproducibility literature |

## 3. Design principles

1. **Primitives from the corpus, not one algo.** A capability earns a place only if it lights up many
   workload rows (Appendix A); one-offs are routed to another surface or pushed into a guest body.
2. **Reuse DataFusion for everything relational; build bespoke *only* for what it structurally can't
   do** — verified to be exactly **iteration** and **float-deterministic reduction** (§2), plus
   **random-access/mutable structure**. Nothing else.
3. **Two modes, one data model.** Mode A = composable bulk kernels (extend v1). Mode B = guest-authored
   *programs* (the §5.6 revision), in two profiles: **vectorized** (rides existing UDF/UDAF) and
   **sequential** (new runtime).
4. **Perf is a loader property.** Hot per-element bodies → WASM (compiled); cold/orchestration → Rhai
   (row-mode interpreter); data-science → PyO3-vectorized; reference → Rust. The ABI is loader-agnostic.
5. **Determinism and governance are mandatory and *correct*** — fixed-order reduction, seeded-sample as
   a first-class primitive, and a budget a grant can actually *raise*.

## 4. Architecture

```
Query        CALL / table-fn → DataFusion ExecutionPlan (composable, registration-driven — not allowlist)
Mode A       Kernels          composable bulk operators over [V]/[E] tensors + sets + immutable CSR   (extend v1)
Mode B-vec   Vectorized progs guest map body → Scalar UDF ; guest segment body → UDAF under GROUP BY   (reuse, exists)
Mode B-seq   Sequential progs guest driver + fast random-access + mutable scratch graph                (new runtime)
Bridge       DataFusion       scans · joins(gather) · UDF/UDAF · UDTF · custom ExecutionPlan
Beside DF    Bespoke          iteration driver · deterministic reduction · mutable graph · seeded RNG
Governance   cross-cutting    native-work budget (grant may RAISE) · arena · wall-clock · determinism tier
```

### Where each workload lands (the anti-overfit check)

- **Grid-reliability, percolation, influence-max, temporal reachability** → **Mode A** (needs the
  additive kernels below; *no* guest body). This is the correction the reconciliation forced:
  grid-reliability is a v1-aligned kernel extension, not a guest-compute problem.
- **ABM tick, GNN message-pass, belief propagation** → **Mode B-vec** (guest UDAF over a graph
  gather-join — a path that already exists and is loader-agnostic).
- **MCTS, max-flow augmenting paths, subgraph isomorphism** → **Mode B-seq** (new runtime; the genuine
  §5.6 revision).
- **APOC scalar tail, geo math, IO, refactor, spatial index, dense linalg** → **routed off** this ABI
  (§9).

## 5. Mode A — extend the kernels (uncontested; pure continuation of v1)

Every addition here is *additive* and preserves all six v1 §5.x mandates — §5.1 native-work budget,
§5.2 timeout-as-hard-error (never silent truncation), §5.3 determinism, §5.4 panic isolation,
§5.5 snapshot isolation, §5.6 no per-element escape hatch — plus the §1 one-crossing-per-O(E) thesis.
v1 explicitly architected for this via reserved slices (`tensor-compute@1` etc.) and reserved
`Shape` variants (`Vd`, `Dd`); these are
the general primitives the corpus demands, not grid-reliability one-offs:

- **`[E]` edge-indexed tensor** (new `Shape::E`) + an **edge-set/mask** handle. Closes the "no per-edge
  data" gap. Used by: grid availabilities/masks, temporal edge windows, edge features, flow residuals.
- **Seeded sampling** `sample(prob: Tensor, seed, iter) -> Mask` over `[V]` and `[E]` — Bernoulli /
  threshold-vs-uniform, using the **existing counter-hash** (`walk_seed` pattern) promoted from
  `random_walks` to a first-class, reproducible primitive. Used by: grid-MC, percolation,
  influence-max, Gibbs, stochastic ABM sampling.
- **Edge-masked traversal** — `expand`/`spmv` accept an edge mask (activate the deferred `EdgeFilter`
  as a *closed, host-evaluated* predicate, honoring v1 §5.6's "no custom-predicate arm"). Enables
  reachability over a subset without re-projection.
- **Segmented reduce** — reduce grouped by a component/label tensor. *Prefer to delegate to DataFusion
  UDAF + `GROUP BY`* (§6) rather than a bespoke kernel, subject to the determinism contract (§8).
- **Typed / multi-field tensors** — complete the partially-started dtype lift (the `DType` tags exist
  and `TensorBuf` already carries a live `I64` variant, §2) so a `[V]`/`[E]` element can carry a small
  struct (agent state, GNN feature row).

Direction: **GraphBLAS-completeness** (semiring × mask × both-operands-sparse). Borrow GraphBLAS's
semiring vocabulary; **own the combine order** (GraphBLAS libraries get determinism from fixed
reduction order — we must too, because our substrate does not; §8).

## 6. The DataFusion reuse boundary (draw it hard)

Verified: plugin scalar/aggregate functions already *are* DataFusion UDFs/UDAFs, loader-agnostically
(§2). So reuse is not aspirational — it's shipped plumbing.

**Reuse DataFusion for:**
- elementwise → **Scalar UDF** (`df_udfs_plugin.rs`)
- grouped / **segmented reduction** → **UDAF + `GROUP BY`** (`df_udaf_plugin.rs`) — this *is* the
  "segmented reduce" gap, solved one layer down
- gather (neighbor state) → **hash-join** against an adjacency `TableProvider`
- table-valued output / exogenous vectors → **UDTF / columns**
- the escape hatch for a bespoke operator → **custom `ExecutionPlan`**

**Build beside DataFusion (verified to be the only things it can't do):**
1. **Iteration driver.** DF recursive CTE is linear-only, off-by-default, buggy (§2) — it *cannot*
   express PageRank/BFS fixpoint. The loop lives in the guest conductor (Mode A) or a custom
   `ExecutionPlan` (Mode B), re-invoking a cached sub-plan per round, never re-planning.
2. **Deterministic reduction.** DF's partitioned float `SUM` is **not bit-reproducible** (§2). A
   determinism-owning accumulator (fixed-order / compensated Kahan–Neumaier sum, or
   `target_partitions=1` for the reducing stage) is mandatory for reproducible study numbers — this is
   the concrete reason grid-reliability §7 reproducibility cannot come from a stock `SUM`.
3. **Random-access + mutable graph** (Mode B-seq) — off-DataFusion entirely.

**Fix the vectorized-path gap (§2):** CALL-into-DataFusion eligibility is a **hardcoded allowlist keyed
on name prefix, not provenance** (`read.rs:1160-1182`). Third-party algorithms still *compose* — the
row-based interpreter runs the whole surrounding plan correctly — but they are **locked out of the
vectorized DF plan path** (joins/group-by/optimizer) unless they squat the `uni.algo.` namespace, which
must not be the sanctioned route. Make eligibility **registration-driven** (a provider *declares*
DF-composability in its manifest / signature) so a third-party `myco.algo.*` is a first-class plan node
like `uni.algo.*`. Also lift the **materialize-to-one-RecordBatch** limitation toward streaming where
the provider supports it, and note `GraphProcedureCallExec` is a *leaf* node — even on the DF path a
CALL composes as an upstream source, never by consuming a DF child. Both lifts are **prerequisites for
the Mode B-vec iteration driver** (§7a), which must re-invoke a cached sub-plan per round rather than
buffer one giant batch per iteration.

## 7. Mode B — guest programs (the deliberate §5.6 revision)

**Honesty first (this is the intellectual core).** GraphCompute v1 §5.6 names the per-element guest
callback *"the one thing to cut, not redesign"* — *"an anti-feature that makes every other guarantee
conditional"* — and builds "conductor, not worker," the closed-enum ABI, the native-work budget, and
the determinism contract *against* it. Note the precise scope of that ban: v1's guest *does* author a
compute body (the conductor's control-flow skeleton); what §5.6 forbids is the **per-element** callback.
v1 further declares its M-class — sequential / mutation / enumeration algorithms (Louvain, Tarjan-SCC,
clique enumeration, …) — "out of scope by design," to "stay native" and "remain authorable only as
native `uni.algo.*`" (v1 §3, §3.1); simulation and adaptive search are not discussed in v1 at all, but
fall in the same excluded quadrants. **Mode B reopens that decision on purpose** — because "stays
native" is exactly the third-party gap (§1). So Mode B must *beat v1's objections on their own terms*,
not hand-wave past them. It does so by splitting into two profiles with different contracts.

### 7a. Mode B-vec — vectorized programs (low-risk; already exists)

A guest supplies a **batched body**: a **map** (`f(fields)->fields`, elementwise) or a **segment
aggregate** (`(state,elem)->state; merge; finalize`). This is *not* the §5.6 per-kernel callback:

- It runs in the **existing, already-blessed UDF/UDAF sandbox** (`df_udfs_plugin.rs`/`df_udaf_plugin.rs`)
  — one layer *over* in DataFusion, **not inside `AlgoSession`**. Verified real and loader-agnostic (§2).
- The boundary crossing is **per batch, not per element** (WASM batch-invoke / PyO3 vectorized), so v1's
  "one crossing per edge" objection does not apply.
- Budget/panic isolation are the loaders' existing fuel/wall-clock meters + adapter `catch_unwind`.
- Determinism is the reduction-order contract (§8) plus a declared **activation schedule** (the ABM
  lesson: update order is part of the contract).

The message-passing workhorse — ABM tick, GNN message-pass, belief propagation — is then:
`edges JOIN state → GROUP BY dst → guest-UDAF`, wrapped by the iteration driver (§6). **The remaining
work is graph-gather wiring + the driver, not a new body ABI.** (Anchor: Ligra/Gunrock frontier-batched,
*not* classic Pregel — determinism comes from deliberately giving up async/message-parallelism.)

Caveats to resolve: the `uni-plugin-custom` declared-aggregate path ships `supports_partial: false`
with **empty `state_fields`** (`uni-plugin-custom/src/aggregate.rs:117`) — and it is the empty
`state_fields`, which the DF adapter consumes at `df_udaf_plugin.rs:137-144`, that actually forces
single-partition grouped aggregation (the flag itself is never read by the adapter; the four guest
loaders all declare `true`). The accumulator is also `Mutex`-wrapped (`df_udaf_plugin.rs:154-156`).
Acceptable for correctness; the parallel-scale work item is **implementing real partial-state
serialization** (non-empty `state_fields` + merge), not flipping a flag.

### 7b. Mode B-seq — sequential programs (the genuinely new runtime)

For adaptive search over evolving structure — **MCTS, Dinic augmenting paths, VF2 subgraph match** — a
guest runs its *own* loop against a fast **random-access read + mutable scratch-graph** API
(`neighbors(slot)`, `get/set fields`, `add_node/add_edge`, `sample`), executed as a custom DataFusion
`ExecutionPlan` (the only DF extension point that owns a sequential loop over batches, §2). This is the
part that truly diverges from v1; it carries its own contracts:

- **Compiled bodies only** (WASM/Rust) — an interpreted per-step body (Rhai row-mode) is disqualified on
  perf (per-step interpretation cannot be amortized).
- **Metered per random-access op** — a runaway guest loop is charged and halts (extends the §5.1 budget
  to pointer-chasing).
- **Deterministic activation/tie-break order** — declared, seeded via the promoted counter-hash.
- **Bounded mutable arena** — graph growth charges the arena cap.
- **Snapshot-isolation contract (v1 §5.5 upheld):** the scratch graph is **session-local, never
  observable by the store**, and pinned to the version stamp captured at projection time. A B-seq
  program is the first surface where a guest holds mutable state across a session, so this is a named
  contract, not an afterthought (the residual SSI interaction is open question 3, §14).

Open risk (must prototype, §11): whether JIT'd WASM random-access is fast enough per step, or whether
B-seq needs a host-resident fast path. This is the one item that could change the shape of B-seq.

## 8. Determinism

- **Promote and extend the counter-hash RNG** (`walk_seed`, currently private to `random_walks`,
  `random_walk.rs:59`) to a first-class `sample`/seed primitive. Today its domain is
  `walk_seed(base, start_slot, walk_idx)` — walks are non-iterative, so there is no iteration counter;
  the promoted primitive **extends the domain** to `hash(seed, iter, elem)` (mixing `iter` in without
  cross-iteration collisions is part of the design). The property carries over: a counter-hash stream
  is order/partition-independent by construction — grid-reliability §7 (and any reproducible study)
  gets per-iteration masks for free.
- **Fixed-order / compensated reduction** everywhere a float is summed — mandatory, because DF's default
  aggregation is not bit-stable (§2). No `fast_nondeterministic` mode (v1 §5.3 / D2 stands).
- **Declared activation order** for Mode B programs (the ABM-frameworks lesson).
- **Manifest determinism tier** (`Pure`/`SessionScoped`/`Nondeterministic`) gates planner memoization.

## 9. Resource governance — revise the grant-as-clamp semantics (deliberate v1 §12 revision)

The native-work budget is v1's "make-or-break governor," and it works **as v1 designed it**: v1 §12
specifies quota grants "intersected with grants like all quotas — same pattern as `FuelPerCall`," and
the shipped code documents the min as intended (`provider.rs:201`). So this is **not a bug fix — it is
the second deliberate revision of a ratified v1 decision** (alongside §7's §5.6 revision), and it is
argued the same way, on the merits:

- The clamp makes the grant **only a ceiling-lowering quota**: at all 7 sites a
  `Capability::GraphComputeWork` grant is `.min()`-clamped to the size-derived
  `size_budget = min(10_000·(|V|+|E|+1), 1e9)` (§2), so **no caller can authorize a legitimately
  large job** (e.g. N≈1e5–1e6 reachability passes). For a governor whose purpose is authorization,
  a grant that cannot authorize is self-defeating.
- The codebase itself is already in tension with the clamp: capability attenuation
  (`capability.rs:248-249`) deliberately **preserves the declared quota value as authoritative**
  through host attenuation — and then the providers silently discard any portion above `size_budget`.

Revision: a grant may **raise** the ceiling; `size_budget` becomes the **default for the ungranted**,
not a hard cap. This is a change to the governance model's security posture (an explicit grant now
authorizes more native work), so it ships with an explicit grant-review note, not as a silent fix.
Keep the two-dimensional model (work-units + arena-bytes + wall-clock), keep per-kernel charging
and the `BUDGET_CHECK_CHUNK` in-kernel checks, and extend charging to Mode B-seq random-access ops and
mutable-graph growth.

## 10. Loader mapping (decisive)

| Loader | Role | Hot per-element bodies? |
|---|---|---|
| **Rust** | reference + hot first-party; all surfaces | yes |
| **WASM (CM/Extism)** | **the substrate for hot third-party Mode-B bodies** — compiled/JIT, batch-invocable, epoch-interruptible | **yes** |
| **PyO3** | vectorized bodies (B-vec) for data science; GIL-per-batch | yes-ish |
| **Rhai** | **conductor/orchestration only** (Mode A loop, config); defaults to **row-mode interpretation** (`adapter.rs:107`; a `vectorized` branch exists at `adapter.rs:98` but the body is still interpreted) | **no** |

Position: **WASM is the Mode-B answer; Rhai is not.** Rhai remains excellent at orchestrating Mode-A
kernels, where per-element cost never touches the script. Also: **window functions are not guest-
authorable today** (Rust-only, §2) — a gap to close if guest windowing is wanted.

## 11. Routing — explicitly NOT in this ABI

APOC scalar tail (`coll`/`text`/`date`/`map`) → **Scalar UDFs** (exists) · load/export → Catalog/
connector · `refactor` (mutation) → the write path · geo spatial index (kNN/geofence/H3) → a
**spatial-index surface** (`IndexKindProvider`) · dense linalg / eigensolve (spectral, GNN weights) →
a **tensor/BLAS-UDF surface** (v1's reserved `tensor-compute@1`). Naming these keeps the compute ABI
from bloating into "everything." "Implement all of APOC" is a *routing* exercise across existing
surfaces, not a GraphCompute ask.

## 12. Migration & compatibility

Additive on 3.0.0 (which already removed the dead plugin traits). No breakage. Each phase merges only
when its acceptance-test family (§15) is green *plus all prior families* (append-only regression).
Phasing:

| Phase | Ships | Risk |
|---|---|---|
| **0** | **grant-semantics revision** (§9 — grant may raise; deliberate v1 §12 revision, ships with a grant-review note); promote seeded `sample`/counter-hash (§8) | low-med — small diff, but a governance-posture change, not a bug fix |
| **1** | Mode A: `[E]` tensor + edge-mask traversal + segmented-reduce (delegate to UDAF) → unlocks the whole stochastic-structural cluster (grid-MC, percolation, influence-max, temporal) | low — v1-aligned additive |
| **2** | DataFusion vectorized path: registration-driven CALL eligibility (not allowlist); streaming lift of the one-RecordBatch CALL node; iteration driver + deterministic reduce as reusable operators | med |
| **3** | Mode B-vec: graph-gather wiring so guest UDAFs do message-passing (ABM/GNN/BP). **Depends on phase 2** — the iteration driver needs the streaming/cached-sub-plan lift (§6) | med — path exists, needs wiring |
| **4** | Mode B-seq: mutable scratch-graph + random-access runtime (MCTS/flows) | high — genuine new runtime; prototype first |

## 13. Documentation sync (source of truth = code; docs mirror as it ships)

The Black Book and website document **shipped** behavior (Part XVII opens with that invariant), so this
design lives here in `docs/proposals/` until each phase lands. Locations that move **together** per
phase:

| Location | Content | Trigger |
|---|---|---|
| this proposal | design + Appendix A matrix | now |
| Black Book §XVII (Plugin Framework) | capability model, surfaces, loaders, budget, "What's Not" | per phase |
| Black Book §IX (Graph Algorithms) | GraphView/GraphCompute extensibility | per phase |
| Black Book §VIII | the DataFusion-UDF connection (scalar/agg → UDF; Mode B-vec) | phase 3 |
| `website/docs/plugins/`, `concepts/`, `reference/` | authoring guide, two-mode model, surface reference | per phase |
| `crates/uni-plugin/CHANGELOG.md` | ABI-surface changes | per ABI change |
| `docs/release_notes/RELEASE_NOTES_*.md` | user-facing summary | per release |

Phase 0 also adds an **honest roadmap note** to Black Book §XVII "What's Not" and website `plugins/`
(clearly marked *direction*, not shipped), and updates the budget line the moment the §9 grant
revision lands. Guard:
a "compute-ABI concepts" canonical section in the Black Book that the website `concepts/` page mirrors
(the same "one source of truth, mirror points at it" discipline as the CI runbook).

## 14. Open questions (must resolve before committing a phase)

1. **B-seq perf** — is JIT'd WASM random-access fast enough per step, or is a host-resident fast path
   needed? (Prototype before phase 4.)
2. **Determinism vs. parallelism** — accept single-partition reducing stages, or invest in a
   fixed-order parallel reduce (compensated tree)? Affects Mode A and B-vec at scale.
3. **Mutable graph × SSI** — how does a Mode B-seq scratch graph interact with snapshot isolation and
   the version-stamp contract? **RESOLVED (Q-3).** The scratch graph is structurally session-local: it
   holds no store handle and has no write-back path, so it is *never observable by the store* — a
   concurrent reader sees no trace during or after a B-seq run. Reads into the projected input are
   pinned to the projection-time snapshot (an owned, materialized `GraphProjection`), so they are
   unaffected by concurrent commits. Isolation is by construction, not by holding a read transaction
   open. Verified live-store by `q3_scratch_graph_is_never_observable_by_the_store` and
   `q3_projected_reads_are_pinned_across_concurrent_commits` (`graph_compute_pagerank.rs`).
4. **Multi-field tensors vs. multiple `[V]` columns** — struct tensors, or keep parallel single-column
   tensors and let DataFusion carry the schema?
5. **Dense linalg** — in-scope as a sibling tensor surface (`tensor-compute@1`) or delegated to a
   BLAS-backed UDF?

---

## 15. Test scenarios & automated acceptance tests

### 15.0 Conventions (inherit v1 §9)

The v1 suite is live and stays authoritative for what it covers: differential oracles in
`graph_compute/differential_tests.rs` (F/C families), budget/arena substrate tests in
`graph_compute/mod.rs` (P0 family), handle security in `handle.rs`/`table.rs` (H family),
`graph.*` conformance probes in `conformance.rs`, per-loader e2e in
`crates/uni/tests/common/loaders/*_graph_compute.rs` (L family), and full-pipeline CALL e2e in
`crates/uni/tests/common/graph_algo/graph_compute_pagerank.rs` (E family). Rules carried forward:

- **IDs are append-only** — never deleted, renumbered, or weakened. Existing families extend upward
  (`F-10`, `M-6`, …); genuinely new surfaces get new families: **G** (governance revision),
  **S** (seeded sample), **A** (Mode A additive kernels), **DF** (DataFusion path),
  **V** (Mode B-vec), **Q** (Mode B-seq). Two sanctioned behavior flips are called out inline
  (G-1, DF-3) — the same discipline as v1's P0-6.
- **Numeric bounds are pinned here**, not in test code: guest/native parity `≤ 1e-9` per element
  (matches the shipped PPR parity tests); statistical tests χ² at `p > 0.01` over ≥ 32 probes
  (the flaky-HNSW lesson: never assert on a single stochastic draw); determinism assertions are
  **bitwise**, not epsilon.
- Test names are behavior sentences (`grant_above_size_budget_is_honored`), not `test_*`.
- New WASM fixtures extend `scripts/build-wasm-fixtures.sh` **and** the fixture-freshness check
  (the `e9e3784a1` lesson: a fixture missing from the build script false-passes on stale artifacts).

### 15.1 Phase 0 — governance revision (G) + seeded sample (S)

Harness: `graph_compute/mod.rs` + `bridge.rs` unit tests; e2e in `graph_compute_pagerank.rs`.

| ID | Acceptance test |
|---|---|
| **G-1** | **(sanctioned flip)** `Capability::GraphComputeWork(w)` with `w > size_budget` yields an effective budget of `w` — inverts today's `.min()` clamp. Asserted at the provider level *and* e2e via CALL. |
| G-2 | Ungranted invocation still gets exactly `size_budget = min(10_000·(\|V\|+\|E\|+1), 1e9)` — the default is unchanged. |
| G-3 | The grant value survives capability attenuation end-to-end (manifest → `effective_caps` → provider), pinning the `capability.rs:248-249` authoritative-value semantics. |
| G-4 | The 7 clamp sites are collapsed into one shared helper; a unit test on the helper is the single truth (no site can drift back to `.min()`). |
| G-5 | A raised budget still fails closed: exhaustion at the granted ceiling is hard error `0x865`, chunked mid-kernel checks intact (extends P0-3/P0-4). |
| G-6 | A work grant does not move arena-bytes or wall-clock ceilings (dimensions are independent). |
| S-1 | `sample(prob, seed, iter)` is bitwise-identical across runs and thread counts. |
| S-2 | Sample masks are invariant under slot/partition permutation (counter-hash order-independence). |
| S-3 | Distinct `iter` values yield decorrelated masks; no stream collision across `(seed, iter)` (χ² over pairwise mask overlaps). |
| S-4 | Marginal distribution is Bernoulli(p): χ² goodness-of-fit at pinned bound. |
| S-5 | `sample` charges the work budget per element and respects `BUDGET_CHECK_CHUNK`. |
| S-6 | `random_walks` output is byte-identical before/after the `walk_seed` promotion (non-regression: the shared counter-hash refactor must not change shipped walk streams). |

Conformance: add probe **`graph.sample_determinism`** to `run_probes()` (self-certification for
algorithm authors, alongside `graph.determinism`).

### 15.2 Phase 1 — Mode A kernels (A; extends F/M/H)

Harness: `differential_tests.rs` naive-oracle pattern; metamorphic folded into
`crates/uni/tests/common/metamorphic/` (which today covers Cypher only — v1's M-1..M-5 land here too).

| ID | Acceptance test |
|---|---|
| A-1 | `[E]` tensor lifecycle: shape/dtype ops, handle kind-mismatch is a typed error not a panic (extends H-4 to `Shape::E`). |
| A-2 | **The key equivalence:** `expand`/`spmv` under an edge mask ≡ the same kernel on a re-projected subgraph containing exactly the masked edges — bitwise, for random masks over random graphs. |
| A-3 | Masked `spmv` vs naive masked matrix-vector oracle (differential, per semiring). |
| A-4 | Segmented reduce delegated to UDAF + `GROUP BY` equals a fixed-order bespoke oracle **bitwise** (this is the §6/§8 determinism contract made executable — it fails today on stock partitioned `SUM`, passes only with the determinism-owning accumulator). |
| A-5 | Typed / multi-field tensor roundtrip: write/read each `DType`, struct fields preserved across kernel boundaries and egress. |
| F-10 | Influence-max (IC) vs a naive seeded Monte-Carlo oracle. |
| F-11 | Temporal reachability (edge-window masks) vs naive time-respecting BFS oracle. |
| M-6 | Mask monotonicity: reachable-set under mask ⊆ reachable-set unmasked; shrinking the mask never grows the set. |
| M-7 | Relabel invariance extends to `[E]` tensors and sampled masks (permute vertex ids + seeds fixed → canonically equal results). |

**Flagship scenario `AT-GRID` (grid connectivity-reliability MC, e2e):** a small series-parallel
network with a *closed-form* reliability; N seeded iterations of `sample`-masked reachability +
weighted reduce via CALL. Asserts (1) estimate within the analytic confidence bound, (2) bitwise
reproducibility across runs and thread counts, (3) runs under a `GraphComputeWork` grant *above* the
default `size_budget` — exercising G-1 in anger. PR gate at N=1e3; nightly soak at N=1e5.

### 15.3 Phase 2 — DataFusion path (DF)

Harness: plan-shape assertions via `EXPLAIN` / physical-plan inspection in `crates/uni-query` tests;
e2e beside `graph_compute_pagerank.rs`.

| ID | Acceptance test |
|---|---|
| DF-1 | A third-party provider declaring DF-composability plans as `GraphProcedureCallExec` (plan-shape assertion: the node appears; downstream join/group-by are DF operators). |
| DF-2 | Differential path parity: the same CALL through the DF path and the row-based interpreter returns identical rows (order-normalized) — the fallback stays a correctness twin. |
| **DF-3** | **(sanctioned flip)** eligibility is registration-driven: a `uni.algo.`-prefixed name *without* the declaration no longer gets the DF path by prefix alone; `is_df_eligible_procedure`'s hardcoded list is deleted. |
| DF-4 | Streaming: a provider emitting K batches is consumed incrementally — peak buffered batches bounded « K (metric assertion), replacing today's `concat_batches`-to-one materialization. |
| DF-5 | Iteration driver: a fixpoint (PageRank) over the driver re-invokes a **cached** sub-plan per round — planning-count metric stays 1 — and matches native `gcpagerank` ≤ 1e-9. |
| DF-6 | Deterministic-reduce operator is bitwise-stable across `target_partitions ∈ {1, 8}` and input-batch permutations (the compensated/fixed-order accumulator contract). |

### 15.4 Phase 3 — Mode B-vec (V; extends L/E)

Harness: per-loader files in `common/loaders/` (parameterized like the L family); reference guest
bodies in each loader's idiom; WASM/Extism fixtures under `examples/example-*-graph/`.

| ID | Acceptance test |
|---|---|
| V-1 | Guest-UDAF message-passing PageRank (`edges JOIN state → GROUP BY dst → guest agg`, wrapped by the driver) matches native `gcpagerank` ≤ 1e-9 — **per loader** (Rust ref, WASM, Extism, PyO3-vectorized). |
| V-2 | Declared activation schedule ⇒ bitwise-identical results across runs and thread counts (the ABM lesson as a test). |
| V-3 | Partial-state aggregation: with real `state_fields` + merge implemented (§7a), multi-partition grouped agg equals single-partition **bitwise**. |
| V-4 | Boundary-crossing bound: host↔guest crossings ≤ ⌈rows / batch⌉ for WASM/Extism bodies (counter metric — the "per batch, not per element" contract made executable). |
| V-5 | A panicking/trapping guest body is isolated per §5.4: typed error, session survives, no poisoned state. |
| V-6 | Guest-body fuel + wall-clock budgets enforced; runaway body halts with the loader's typed budget error. |

**Scenario `AT-ABM` (SIR epidemic):** seeded SIR on a fixture graph via guest UDAF; matches a
reference native Rust implementation exactly (same schedule, same seeds), deterministic across
thread counts; PyO3 and WASM bodies agree with each other bitwise.

### 15.5 Phase 4 — Mode B-seq (Q)

Harness: new runtime's own unit tests + `common/loaders/` e2e; the perf gate is a criterion bench.

| ID | Acceptance test |
|---|---|
| Q-1 | Every random-access op (`neighbors`, `get/set`, `add_node/add_edge`, `sample`) charges the work budget; a runaway guest loop halts at `0x865` (extends §5.1 to pointer-chasing). |
| Q-2 | Scratch-graph growth charges the arena cap; exceeding it is a typed error at the allocating op. |
| Q-3 | **SSI contract (§7b):** the scratch graph is never observable by the store — a concurrent reader sees no trace during/after a B-seq run; reads inside the run are pinned to the projection-time version stamp even across concurrent commits. |
| Q-4 | Seeded tie-break/activation order ⇒ a full MCTS run is bitwise-reproducible. |
| Q-5 | **Perf go/no-go for §14 Q1 (pre-phase gate):** benchmarked JIT'd-WASM random-access step rate within a pinned ratio (≤ 10×) of a host-resident Rust baseline on a pointer-chasing microbench; failing this bound triggers the host-resident fast-path redesign *before* the phase is committed. |
| Q-6 | Registration rejects an interpreted (Rhai) B-seq body with a typed capability error (compiled-only contract). |

**Scenarios:** `AT-FLOW` — Dinic max-flow on fixture networks equals a naive Ford-Fulkerson oracle;
`AT-MCTS` — seeded MCTS on a small game tree reproduces a pinned principal variation.

### 15.6 CI lanes (extends the v1 §9.5 mapping)

| Lane | Runs |
|---|---|
| PR gate | G/S/A/DF unit + differential; `AT-GRID` @ N=1e3; Rhai + Rust-ref loader rows |
| Workspace (after `build-wasm-fixtures.sh`) | full L/V per-loader matrix incl. WASM/Extism fixtures |
| Nightly | `AT-GRID` @ N=1e5 soak; V-4 crossing-count audit; Q-5 perf bench with pinned bound |
| TSan | S-1/S-2, V-2, Q-4 determinism families under threads (races surface here, not as flakes) |

---

## Appendix A — Workload × capability matrix (the anti-overfit spine)

Rows = a deliberately diverse corpus; columns = candidate capabilities. A capability that lights up
**many** rows is a primitive worth building; a **single**-row capability is a guest-body concern or
routes elsewhere. Grid-reliability is one row, not the design.

Legend: ● needs · ○ optional/partial · — n/a. Mode: **A** kernels · **Bv** vectorized program ·
**Bs** sequential program · **R** routed off-ABI.

| Workload | rand `sample` | `[E]` tensor | edge-mask traverse | seg. reduce | multi-field state | guest map/agg body | seq. + mutable | iteration | Mode |
|---|---|---|---|---|---|---|---|---|---|
| PageRank / HITS / Katz | — | — | — | ● | — | — | — | ● | A |
| BFS/SSSP/WCC/k-core | — | — | ○ | ● | — | — | — | ● | A |
| **Grid reliability / percolation** | ● | ● | ● | ● | ○ | — | — | ● | **A** |
| Influence-max (IC/LT) | ● | ● | ● | ● | — | — | — | ● | A |
| Temporal reachability | ○ | ● | ● | ● | — | — | — | ● | A |
| **ABM (SIR/opinion/Schelling)** | ● | ○ | — | ● | ● | ● | ○ | ● | **Bv (+Bs if moving)** |
| **GNN message-pass (infer)** | — | ● | — | ● | ● | ● | — | ● | **Bv** |
| Belief propagation | — | ● | — | ● | ● | ● | — | ● | Bv |
| **System dynamics** | ○ | — | — | ○ | ● | ● | — | ● | Bv |
| **MCTS** | ● | — | — | — | ● | ● | ● | ● | **Bs** |
| Max-flow / matching | — | ● | ○ | — | ○ | ● | ● | ● | Bs |
| Subgraph iso / motif | — | — | ● | — | ○ | ● | ● | — | Bs |
| Louvain / SCC | — | — | — | ● | ● | ○ | ● | ● | Bs / native |
| APOC coll/text/date | — | — | — | — | — | ● | — | — | R (Scalar UDF) |
| Geo routing / spatial | — | ● | ● | — | ● | ○ | — | ○ | A + R (spatial index) |

**Reading the columns:** `sample`, `[E]` tensor, edge-mask, and segmented-reduce each light up many
rows → **Mode A primitives** (phase 1). Guest map/agg body lights up the whole simulation/ML band →
**Mode B-vec** (phase 3, rides existing UDAF). Sequential+mutable lights up the search/flow band →
**Mode B-seq** (phase 4, new runtime). Iteration is near-universal → the **cross-cutting driver**.
Multi-field state recurs across B → a real data-model need, not a one-off.
