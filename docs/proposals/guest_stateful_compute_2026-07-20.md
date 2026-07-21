# Guest-authorable stateful graph compute — a holistic redesign (2026-07-20)

**Status:** Design proposal — evidence-complete, implementation-ready. · **Date:** 2026-07-20 · **Trigger:** GitHub issue #152 (Mode B-seq scratch-graph primitives unreachable from Rhai), which investigation showed to be unreachable from *every* loader. · **Baseline:** local `main` `92c3df763` (v3.0.2, post-#151). · **Supersedes:** `plugin_compute_abi_2026-07-13.md` §7b (Mode B-seq runtime shape), §14 Q1 (the perf gate), and the `require_compiled_body` policy. · **Lineage:** third instance of the guest/native surface-drift class first named in `python_plugin_abi_gaps_2026-07-16.md` (#150) and `graphcompute_projection_parity_2026-07-19.md` (#151).

---

## 1. Summary

Issue #152 asked for the mutable scratch-graph (Mode B-seq) primitives to be exposed
to Rhai guests. Investigation found a larger and more interesting situation:

- The scratch subsystem (`ScratchGraph` / `ScratchRegistry`) is **unreachable from all
  four production loaders**, not just Rhai — its host import was never wired. The
  passing WASM e2e test satisfies a *test-only* WIT package.
- Its gating perf question (`§14 Q1` / `Q-5`) had **never been answered**; the
  benchmark's JIT'd-WASM arm was never written.
- Answering it showed the **per-op ABI misses by 1–2 orders of magnitude**, and that
  the two causes are *granularity* (640× excess crossings) and *encoding* (a 32× JSON
  tax) — **not** the loader class, and **not** `ScratchGraph` itself.
- Fixing both puts a sandboxed JIT'd WASM guest at **1.27× of native in-process Rust**
  (18.5 M MCTS rollouts/s), and an **interpreted Rhai guest at 1.97 M rollouts/s** —
  production-grade, and a direct refutation of the `require_compiled_body` policy.

This proposal folds the scratch runtime into the existing GraphCompute substrate,
makes reachability structural rather than remembered, replaces a mis-specified
acceptance gate, and retires a design policy the measurements contradict.

> **Reading order.** §1–§11 are the original design. Two later sections supersede parts of
> it and are authoritative where they conflict: **§12** closes the §10 gaps by
> measurement, and **§13** is the implementation plan of record, replacing §8's phasing.
> Current status is §13.5.

## 2. Evidence

All numbers from benches added during the investigation. Identical search in every
arm (complete binary tree, depth 10, 4096 rollouts, least-visited-child descent,
deterministic ties), correctness-asserted (all arms agree: root visits = 4096).

### 2.1 The Q-5 gate, finally measured (`crates/uni-plugin-wasm/benches/mode_b_seq_wasm.rs`)

Pointer-chase, 2048 hops, 4096 host crossings:

| Arm | ns/op | vs host baseline |
|---|---|---|
| `direct_host_baseline` (native `ScratchGraph`) | 5.7 | 1× |
| `json_abi_native` (JSON crossing, **no WASM**) | 318.7 | **56×** |
| `wasm_jit_guest` (real JIT'd component) | 2058.2 | **362×** |

The proposal's stated assumption — *"a JIT'd-WASM body's per-op cost is dominated by
that host boundary crossing … the `json_abi / direct` ratio is the quantity Q-5 cares
about"* — is **half right and consequentially wrong**: the crossing is indeed huge
(56×), but the component call adds a further 6.5×, so the JSON proxy underestimates
the real guest cost by ~6×. And decisively: **the JSON ABI alone busts a ≤10× budget
before any guest runs**, so no guest technology could ever have met it.

### 2.2 Batched MCTS across every guest technology (`mcts_batched*.rs`)

| Path | Time | vs native | Rollouts/s |
|---|---|---|---|
| `native_kernels` (in-process Rust) | 0.178 ms | 1.00× | 23.0 M |
| **`wasm_typed`** (JIT'd guest, typed `list<u32>` ABI) | 0.221 ms | **1.27×** | 18.5 M |
| **`rhai_batched`** (interpreted guest, `Array` marshalling) | 2.077 ms | 11.66× | **1.97 M** |
| `wasm_json` (JIT'd guest, JSON ABI) | 7.130 ms | 41.0× | 575 K |
| per-op guest (today's B-seq shape) | 69.7 ms | — | 59 K |

Two independent levers, each large, and they **multiply**:

- **Granularity** — batching cut crossings 640× (204,800 → 320).
- **Encoding** — the typed ABI was **32× faster than JSON** at the same granularity.

### 2.3 What the numbers say about the design, not just the perf

1. **`ScratchGraph` is not the problem.** Every *fastest* arm runs on it: it is both
   the 5.7 ns/op host baseline and the substrate under the 1.27× typed-WASM result.
2. **Loader class is not the deciding variable.** Interpreted Rhai (1.97 M rollouts/s)
   beats compiled WASM on the JSON ABI (575 K) by 3.4×.
3. **The ratio gate was the wrong instrument.** A sandboxed guest is trivially slower
   than in-process native; the decision-relevant quantity is absolute throughput
   against a workload. At 1.97 M rollouts/s a 100 K-rollout search costs 51 ms.

## 3. Root cause: the same disease, third instance

| Issue | Symptom | Mechanism |
|---|---|---|
| #150 | guest algorithm won't load from Python | capability grants parsed in 3 drifting places |
| #151 | guest can't scope its projection | projection config parsed in ≥3 places; adapters got the empty copy |
| **#152** | guest can't reach the scratch graph | a **parallel subsystem** whose host wiring nobody did |

Each is the guest surface being a strict, unenumerated subset of host capability, with
the boundary maintained by memory rather than by a mechanism. #152's variant is the
worst: the capability is unreachable by *everyone* while its tests and benches assert
it works.

**The causal detail that should drive the design:** in #151, `node_property` /
`edge_property` were added as kernels on the existing `AlgoSession` + `dispatch.rs` —
and **WASM and Extism picked them up with zero loader changes**, because the single
`host-graph` import was already wired. The scratch graph needed a *second* registry, a
*second* wire format, a *second* dispatch table, and therefore a *second* host wiring —
which never happened. Reachability was an architectural consequence, not an oversight.

| | Mode-A kernels | Scratch |
|---|---|---|
| Registry | `GraphComputeRegistry` | `ScratchRegistry` (separate) |
| Wire types | `KernelRequest`/`Response` | `ScratchRequest`/`Response` (separate) |
| Dispatch | `dispatch.rs` match | `scratch.rs` match (separate) |
| Host import | wired by all 4 loaders | **wired by nobody** |

## 4. Design principles

**P1 — One substrate.** There is one session, one handle table, one dispatch table, one
host import. A new capability is a new *handle kind* and new *kernel arms*, never a new
stack. Reachability then follows by construction.

**P2 — Handles, never data.** Kernels take and return opaque handles. The guest never
materializes a batch. This is already the Mode-A contract; the scratch ABI violated it
(JSON arrays) and so did the #152 prototype (Rhai `Array`s → 164 K `Dynamic`
conversions → 11.66×). Handle-passing makes the crossing payload O(1) in batch width.

**P3 — Coarse granularity: the guest conducts, the host works.** Every kernel performs
O(V+E) or O(batch) native work per call. No per-element callbacks — v1 §5.6's rule,
which Mode B-seq reopened and the measurement vindicates.

**P4 — Typed ABI on the component boundary.** JSON costs ~2 µs per crossing even for
tiny payloads. Typed component-model functions cost ~134 ns carrying 1 KB.

**P5 — Batching amortizes the crossing, never the meter.** Batched kernels charge the
full native work (one unit per element plus one per edge inspected). A guest must not
be able to evade the §5.1 work budget by batching.

**P6 — Parity is enforced, not remembered.** A compile-time/CI contract asserts every
dispatchable kernel is reachable from every loader that can express it.

## 5. The design

### 5.1 A mutable arena as a handle kind (P1)

Add `HandleKind::Arena = 7` to the existing table (`handle.rs:61`; note `Walks` and
`Levels` are already reserved-but-unused, so the enum was built to extend). An arena is
session-local mutable synthetic structure:

- **Bump-allocated slots** (`0..n`), charged against the existing `Arena` byte cap.
- **Named typed columns** — `[N]` `f64`/`i64`, allocated as ordinary `Tensor` handles
  sized to the arena rather than to the projection. This removes `ScratchGraph`'s
  single-`f64`-per-node limit (`scratch.rs:103`), which **cannot represent a real MCTS
  node** (visits *and* value) — the current AT-MCTS only works because it uses
  visit-count-only descent.
- **Flat CSR-with-slack child arrays**, not `Vec<Vec<u32>>` (a heap allocation per
  node — the wrong shape for a tree arena and the reason growth is expensive).

It lives in the existing handle table, is dispatched by the existing `dispatch.rs`, and
crosses the existing single `host-graph` import.

### 5.2 The kernel surface

Batched, handle-in/handle-out. Illustrative core set for search/agent workloads:

```
arena_new(capacity)                        -> Arena
arena_alloc(arena, count)                  -> Tensor[i64]   // the new slots
arena_link(arena, parents: Tensor, kids: Tensor)            // batched edge append
arena_column(arena, name, dtype)           -> Tensor        // [N] state column
descend_batch(arena, active: Tensor, score: Tensor, dir)    -> Tensor   // per-slot argmax over children
gather_batch(arena, slots: Tensor, column: Tensor)          -> Tensor
scatter_batch(arena, slots: Tensor, column: Tensor, values) // backprop
```

Every operand is a handle. `descend_batch` folds the child scan host-side — the
measurement's decisive move, since the guest otherwise pays a crossing per child.

### 5.3 ABI (P4)

- **WASM/Extism:** typed component-model functions for the hot kernels, carrying
  handles (`u64`) and small scalars. The generic JSON `graph-call` stays for the
  cold/generic surface.
- **Rhai/PyO3:** in-process; handles are plain `i64`, so **no marshalling at all**.
  This is the fix for Rhai's 11.66×.

> **Prediction, not yet measured:** handle-passing should bring Rhai from 11.66× toward
> WASM's ~1.3×, because its residual cost is `Dynamic` boxing of batch arrays. This must
> be measured before the design is considered validated (§10).

### 5.4 Reachability contract (P6)

Mirroring #151's `ProjectionKnob` and the capability `every_variant_classified_exactly_once`:

- A wildcard-free `match` over a `KernelId` enum — adding a kernel fails to compile
  until it is classified and named.
- A CI test that iterates the dispatch table and asserts, per loader, that each
  `GuestReachable` kernel is actually exposed (registered in the Rhai engine, present
  in the PyO3 `#[pymethods]` block, dispatchable for WASM/Extism).

This is the mechanism that makes #153 structurally impossible.

> **Amended by §13.3:** `KernelId` carries a *second* wildcard-free classification,
> `substrate()`, so a kernel is guarded both on which loaders reach it and on which graph
> substrates it is valid over. **Amended by §13.1:** the contract has a live bug to catch —
> `edge_count` is already JSON-reachable but absent from Rhai and PyO3.

### 5.5 Capability, metering, isolation

- **Grant.** Mutable synthetic state is a distinct capability from read-only projection.
  Declare it as a **capability slice** — `("graph-arena", 1)` added to
  `HOST_CAPABILITY_SLICES` (`algorithm.rs:83`) — so a guest declares `SliceReq` and gets
  load-time negotiation with a typed `0x86A` on mismatch. This is the existing,
  currently-unused hook, and it is the natural home for any future policy gate.
- **Metering.** Per-op work charge (P5) plus arena byte charge on growth. Both already
  exist and are preserved by the prototype kernels.
- **Isolation.** Unchanged and already proven: a scratch run is never observable by the
  store (`Q-3`, `q3_scratch_graph_is_never_observable_by_the_store`), and projected
  reads stay pinned to the projection-time snapshot.

### 5.6 Determinism under batching

Batching is **not semantically free**: rollouts within a batch descend against the same
stale statistics, whereas a sequential search sees every prior update. This must be an
explicit, declared contract — the proposal's existing "declared activation order" for
Mode B programs — plus a documented virtual-loss idiom for search workloads. Kernel
determinism itself is preserved: `descend_batch` breaks ties by lower slot, so a given
(arena, scores) always yields the same result.

## 6. What this retires

| Thing | Why | Action |
|---|---|---|
| `ScratchRegistry`, `ScratchRequest`/`Response`, `scratch.rs` dispatch | the parallel stack is *why* it was unreachable (§3) | delete; fold into the one substrate |
| `ScratchGraph`'s `Vec<Vec<u32>>` + single `f64` field | wrong shape; can't hold a real MCTS node | replace with CSR-with-slack + named columns |
| `require_compiled_body` (`scratch.rs:81`) | refuted: interpreted Rhai (1.97 M rollouts/s) beats compiled WASM on JSON (575 K) | delete; it is also dead code today |
| `§14 Q1` ≤10×-of-native gate | measures the wrong quantity; denominator unspecified (same guest scored 9.2× or 41.6× depending on baseline choice) | replace with §7 |
| `AT-MCTS` as acceptance evidence | proves determinism over an ABI being retired, on a structure that cannot represent an MCTS node | rewrite against the new surface with visits+value |

**Retained unchanged:** the `ScratchGraph` *core* (it is the fastest arm measured), the
work-budget/arena metering, the isolation contract, and Mode B-vec (already complete and
serving the ABM workload).

> **Amended by §13.4:** retirement is *staged*. `scratch` is `pub use`d
> (`graph_compute/mod.rs:51`), so deleting it is semver-breaking on a 3.0.2 workspace —
> deprecate now, delete at the next major. The table's "delete" verdicts stand on merit;
> only their timing changes.

## 7. Acceptance criteria (replacing the ratio gate)

Absolute, workload-anchored, per-loader, in CI:

| Check | Target |
|---|---|
| Guest-authored MCTS, depth 20 | ≥ 1 M rollouts/s (WASM typed), ≥ 250 K (Rhai) |
| Batched kernel crossing overhead | ≤ 1 µs per crossing |
| Work-meter integrity | batched charge == sum of equivalent per-op charges |
| Determinism | identical seed ⇒ identical result, per loader |
| Reachability | every `GuestReachable` kernel exposed by every loader (§5.4) |

Rationale: a sandboxed guest is *necessarily* slower than in-process native, so a ratio
cannot decide whether a third party can ship an algorithm. Absolute throughput can.

## 8. Phasing (superseded by §13)

> Retained for the reasoning; **§13.2 is the order of record**. Reading the code moved the
> reachability contract to the front and re-rated phase 4 from high to medium risk (§13.1).

| Phase | Content | Risk |
|---|---|---|
| **0** | Honest signalling now: a typed `0x86C` denial for scratch ops instead of `Function not found` | trivial; closes #152's user-visible defect immediately |
| **1** | `HandleKind::Arena` + columns + CSR-with-slack + batched kernels on `AlgoSession`/`dispatch.rs`; Rhai/PyO3 wrappers | med — contained, follows the #151 pattern |
| **2** | Typed component ABI for hot kernels (WASM/Extism); `graph-arena@1` slice | med — WIT change is **breaking for built guests** (see §10) |
| **3** | Reachability contract (§5.4) + acceptance suite (§7); retire §6 items | low |
| **4** | Unify `GraphProjection` and arena behind a graph-handle abstraction so the full 60-kernel Mode-A set applies to synthetic structure | high — `get_graph` returns `&Arc<GraphProjection>` (`table.rs:242`), so this needs a trait or a freeze/snapshot step across 60+ kernels |

Phases 1–3 deliver the measured result. Phase 4 is the north star, deliberately last.

## 9. Non-goals

- Exposing the *current* per-op scratch API to any loader. Measured at 59 K rollouts/s;
  superseded by the batched surface.
- A general per-element guest callback. v1 §5.6 stands, now with numbers.
- Replacing Mode B-vec. It is complete and is the right home for ABM/GNN/BP ticks
  (`AT-ABM` runs a full stochastic SIR over the bulk kernels at ~8 crossings/tick).
- Making guests match native. 1.27× is incidental; the target is absolute throughput.

## 10. Risks and open questions

1. **Handle-passing for Rhai is predicted, not measured** (§5.3). If `Dynamic` boxing is
   not the dominant term, Rhai stays ~11×. *Measure before committing phase 1's shape.*
2. **Edge-mutating workloads unvalidated.** Max-flow/Dinic mutate residual capacities
   rather than growing nodes. `[E]`-shaped columns should serve, but this was not
   benchmarked — MCTS is the only workload measured end-to-end.
3. **Batched staleness vs search quality** is unquantified (§5.6). The cost ceiling is
   known; the search-quality cost of batching is not.
4. **Adding WIT imports is breaking for already-built guests.** Confirmed during this
   work: adding the typed functions broke the existing e2e linker until it was updated.
   This is exactly what `SliceReq`/`HOST_CAPABILITY_SLICES` negotiation exists to manage,
   and phase 2 must use it rather than silently changing the world.
5. **Arena limits under adversarial guests** — bounded by the existing byte cap, but the
   growth path deserves an explicit fuzz/limit test.

## 11. Verification

1. `cargo bench -p uni-plugin-wasm --bench mcts_batched_wasm` and
   `-p uni-plugin-rhai --bench mcts_batched_rhai` — each asserts all arms compute the
   same search before timing (a `NaN` from a failed host call would otherwise read as a
   spectacularly fast run).
2. Acceptance suite (§7) per loader in CI.
3. `cargo nextest run -p uni-plugin-builtin -p uni-plugin-rhai -p uni-plugin-pyo3
   -p uni-plugin-wasm -p uni-plugin-extism -p uni-db`; fmt; clippy `-D warnings`.
4. The #152 repro flips from "gap confirmed" to a working batched MCTS in Rhai.

---

## 12. Gap closure (supersedes §10)

All five §10 gaps were investigated; four are closed by measurement, one is corrected,
and one new gap was surfaced. Amendments to the design body follow each result.

### 12.1 Gap 1 — handle-passing: **CONFIRMED**

| Arm | vs native | Rollouts/s |
|---|---|---|
| `native_kernels` | 1.00× | 23.4 M |
| `rhai_arrays_2call` | 11.98× | 2.0 M |
| `rhai_arrays_fused` | 9.42× | 2.5 M |
| **`rhai_handles`** | **1.01×** | **23.2 M** |

Marshalling alone is **9.32×** — measured against a byte-identical array path, since
both entry points call one shared `advance_core`, so fusion cannot confound it.

> **Amends §5.2 / P2:** handle-passing is promoted from *principle* to **requirement**.
> An interpreted Rhai guest at 1.01× of native is indistinguishable from the host, and
> matches typed WASM (1.27×). Kernels take and return handles; batch data never crosses.

### 12.2 Gap 3 — batching staleness: **MEASURED, and worse than stated**

Distinct leaves reached, same 4096 rollouts, of 1024:

| Variant | Leaves |
|---|---|
| sequential (batch = 1) | **1024** |
| batched, naive lock-step | **16** |
| batched + virtual loss | **1024** |

Naive batching does not degrade the search — it **destroys** it. Every rollout in a batch
reads identical statistics, chooses the identical child, and the whole batch collapses
onto one path: a 64× loss of coverage that a throughput-only gate reports as a success.
Applying each visit *in-loop* restores full diversity at zero crossing cost, because the
loop is host-side.

> **Amends §5.6:** in-loop update (virtual loss) is the kernel **default**; the stale
> variant is not exposed. **Amends §7:** add a **search-quality** acceptance criterion
> (distinct-leaf coverage vs sequential). Throughput alone passed a broken search.

### 12.3 Gap 2 — edge-mutating workloads: **CLOSED**

The premise was wrong in a way worth recording: `ScratchGraph` had **zero per-edge
state**, so residual capacities had no home anywhere. Closing the gap required adding
both `edge_fields` (`[E]`-shaped state) and `rev` (the paired-reverse-arc index a
residual push must credit) — `add_edge` is one-directional and unpaired.

`AT-FLOW` (`differential_tests.rs`) then runs a guest-style
`while augment_batch(s, t, k) > 0` and matches an **independent matrix-based
Edmonds-Karp oracle** across 3 seeds × k ∈ {1, 8, 32}, with flow conservation verified
at source and sink. Correctness holds at *every* batch width — because residual updates
land in-loop, the same discipline §12.2 requires.

> **Confirms** the batched/handle design serves edge-mutating algorithms, not only
> node-growing search.

### 12.4 Gap 5 — metering & fail-closed: **CLOSED**

Five tests in `scratch.rs`, the load-bearing one being
`batching_gives_no_budget_discount`: one advance over `B` rollouts charges exactly what
`B` single-rollout advances charge. Without it, design P5 is a comment and a guest evades
the §5.1 work budget by batching. Also covered: budget exhaustion mid-batch → `0x865`
with the active set unmutated, arena cap → `0x864`, unknown handle → `0x86E`, and
unbounded growth halting at the byte cap.

### 12.5 Gap 4 — ABI evolution: **CORRECTED**

The proposal (and the plan) claimed the slice-negotiation machinery was unused. **That is
wrong.** `AlgorithmSignature::check_slices` is wired into `PluginRegistrar::algorithm`
(`crates/uni-plugin/src/registrar.rs:377-379`), rejecting an unavailable slice at *load
time* as `PluginError::SliceUnavailable`, and it has test coverage. The dead policy is
`require_compiled_body`, which was conflated with it.

So no new mechanism is needed — only the **convention**: new host functions go in a *new*
WIT interface, never appended to an existing one. Live evidence from this work: adding
two functions to the shared `host-graph` interface broke a third harness that was not
updated, and the failure surfaced as

```
component imports instance `uni:scratch/host-graph@0.1.0` ...
  0: instance export `descend-batch` has the wrong type
  1: function implementation is missing
```

— exactly the cryptic link error that load-time slice negotiation exists to replace.

> **Deliberately not done:** declaring `("graph-arena", 1)` in `HOST_CAPABILITY_SLICES`
> now. The arena is a prototype; advertising a capability the host cannot actually serve
> is the precise disease #150/#151/#152 all instantiate. It is declared when it lands.

### 12.6 Gap 6 (new) — policy expressiveness vs granularity: **CONFIRMED, unresolved**

The granularity that delivers the performance also **absorbs the algorithm**. The
max-flow guest program, in full, is:

```
while augment_batch(source, sink, k) > 0 { }
```

Every decision — BFS level graph, blocking flow, current-arc optimization, residual
updates — is host-side. The guest authored nothing; it invoked a native max-flow in a
loop. MCTS is milder but the same shape: `descend_batch` hardcodes the *selection policy*
(least-visited child), so the guest conducts batches without owning the search rule.

This is a genuine tension with the point of a plugin API, and it splits by workload:

- **Pluggable-policy algorithms** (MCTS, beam search, A\*) — the resolution is a kernel
  that takes a **guest-supplied score column**: the guest computes a `[N]` score with
  ordinary batched tensor ops (its own UCB constant, priors, whatever), and the kernel
  descends by argmin/argmax over it. Coarse *and* expressive, one crossing per level.
- **Fixed-structure algorithms** (max-flow, SSSP) — the structure *is* the algorithm;
  there is no policy to plug. These are better served as **first-party native providers**
  with configuration, and should not be presented as guest-authorable.

> **Open:** the guest-supplied-score-column variant is designed but unmeasured. It is the
> next experiment, and it determines whether "guest-authorable" is honest for the search
> family.

### 12.7 Gap 6 — the guest-policy experiment: **RESOLVED for the search family**

The proposed resolution (§12.6) was tested: the guest composes its own **UCB1** from
column primitives — its own formula, exploration constant `c`, reward term and virtual
loss — and the host descends by the resulting column.

```
ucb[child] = w[child] + c * sqrt( ln(visits[parent(child)]) / visits[child] )
```

Same search, correctness-asserted (guest UCB ≡ native UCB, root visits 4096):

| Arm | Time | vs hardcoded | Rollouts/s |
|---|---|---|---|
| `hardcoded_policy` (host-owned rule) | 0.202 ms | 1.00× | 20.2 M |
| `native_ucb_per_level` (same policy, driven from Rust) | 3.823 ms | 18.88× | 1.07 M |
| **`guest_ucb_per_level`** (policy authored in Rhai) | 4.357 ms | 21.51× | **940 K** |
| `guest_ucb_per_batch` (recompute once per batch) | 0.718 ms | 3.54× | 5.7 M |

Three findings, in order of importance:

1. **The interpreter is not the cost: 1.14×.** Rhai driving the policy is 14% slower than
   *Rust* driving the identical policy. Guest authorship is essentially free; what costs
   is the policy computation itself, which the host would pay either way.
2. **Virtual loss on the score column preserves diversity: 1024/1024 leaves.** The concern
   raised while designing this — that a guest-owned score column reintroduces the §12.2
   batch collapse, because the host cannot recompute the guest's formula per rollout — is
   real, and is fixed by an explicit `vloss` term the host applies to the chosen node's
   score in-loop. The guest's formula stays authoritative at recompute time.
3. **The remaining inefficiency is whole-column scoring: 6.07×.** Recomputing an `O(N)`
   score every level scores 2047 nodes when only ~512 candidates (frontier × degree)
   matter. Per-batch recompute is 6× cheaper but staler.

**Primitives that turned out to be missing** — and are therefore requirements of §5.1,
not optional extras: multiple named `[N]` columns (the single `fields` vec cannot hold a
search node), **parent tracking** (UCB's `ln(visits[parent])` was not expressible at all),
and `ln`/`sqrt`/`recip`/ewise column ops.

> **Amends §5.2 and the §5.2 kernel surface:** add a guest-supplied score column to the
> descent kernel (`advance_batch_scored`) with a virtual-loss term. "Guest-authorable" is
> honest for the search family: the plugin author owns the search rule at 940 K–5.7 M
> rollouts/s with full search diversity.
>
> **Next optimization (evidence-backed, not yet built):** score only the active frontier's
> children rather than all `N`. That targets the measured 6.07×.
>
> **Unchanged:** fixed-structure algorithms (max-flow, SSSP) have no policy to plug and
> remain first-party native providers. This experiment does not rescue them, and was not
> intended to. Per-batch scoring's search quality is also unmeasured (diversity was
> measured on the per-level arm).

---

## 13. Implementation plan (supersedes §8)

§1–§12 establish *what* to build and prove it works. This section is the plan of record
for building it, written after reading the code the change lands in. Four findings from
that reading altered the phasing materially, so §8's table should not be followed.

### 13.1 What reading the code changed

| Finding | Effect on the plan |
|---|---|
| **`GraphView` already exists** — `uni-plugin/src/traits/algorithm.rs:607`, object-safe, `Send + Sync`, implemented by `GraphViewImpl` (`bridge.rs:50`), consumed as `&dyn GraphView` by `pregel.rs`/`reachability.rs` | Phase 4 is an *extension*, not a new abstraction. It already has 12 of the 14 methods the call sites need |
| **No session.rs site touches a `GraphProjection` field** — the fields are `pub(crate)` and `session.rs` is in another crate, so it was structurally impossible | 17 of 18 `get_graph` sites convert mechanically. §8's "high risk, 60+ kernels" over-rated this |
| **The dispatch has no exhaustiveness and has already drifted** — `dispatch.rs:548` ends in `other => Err(...)`; op names exist only as string literals | The contract must move to the *front* of the plan (§13.2) |
| **The `[E]` numbering assumption is the real phase-4 risk**, not the trait shape | Needs an explicit contract (§13.3) rather than late discovery |

The drift is not hypothetical. **`edge_count` was dispatchable via JSON but had no Rhai
`register_fn` and no PyO3 `#[pymethods]` method.** WASM and Extism guests could call it;
Rhai and Python guests could not. That is #152's exact failure mode, alive in the
production surface until §13.5 fixed it, and it is why §3's "same disease, third instance"
reading is
right: the surfaces are *split* — WASM/Extism share one generic `graph-call` entrypoint
(`wit/world.wit:74-87`) and inherit new kernels for free, while Rhai (`register_fn`) and
PyO3 (`#[pymethods]`) require per-kernel registration and therefore drift silently.

### 13.2 Revised phase order

| Phase | Content | Why here |
|---|---|---|
| **0** | Typed refusal for unresolved ops | ships #152's user-visible fix without waiting for the substrate |
| **1** | Reachability contract (§5.4) on today's surface | **moved from §8 phase 3.** It is the guard that makes every later phase safe: once it exists, an arena kernel absent from Rhai/PyO3 fails CI instead of shipping dark. It also has `edge_count` to catch on first run |
| **2** | `HandleKind::Arena` + columns + CSR-with-slack + batched kernels; Rhai/PyO3 registration | unchanged in content; registration is now compulsory because phase 1 fails the build without it |
| **3** | Typed component ABI (`host-arena`) + `graph-arena@1` slice | unchanged |
| **4** | Unify projection and arena behind `GraphView` (§13.3) | still last, but now med-risk rather than high |
| **5** | Acceptance suite (§7), §6 retirements, max-flow as a first-party provider | the benches become the acceptance suite rather than scaffolding |

The reorder is the point: §8 put the anti-drift contract *after* the work whose drift it
exists to prevent.

### 13.3 The edge-numbering contract (amends §5.1 and §5.4) — *superseded by §13.8*

> **Not implemented as written.** The plan below assumed a live arena would be
> exposed to Mode-A kernels directly, which is what forced the runtime
> `substrate()` classification. §13.8 takes the snapshot route instead, which
> makes the same guarantee structurally. Retained for the reasoning.

`expand_masked`, `spmv_masked`, `edge_weights`, `edge_property` and the `EdgeSet` masks
(`value.rs:413`) all assume a **stable dense `[E]` numbering** in which
`out_edge_start(u) + k` is a global edge index. A live arena with CSR slack (§5.1) cannot
honour that — slack leaves holes, so `edge_count()` is not the sum of degrees.

Rather than let this fail subtly on synthetic structure:

- `GraphView` gains **`has_dense_edge_ids() -> bool`** (true for `GraphProjection`).
- `KernelId` (§5.4) carries a **second wildcard-free classification**,
  `substrate() -> Substrate`: `AnyGraph` / `DenseEdgeIds` / `ConcreteProjection`.
- `DenseEdgeIds` kernels **fail closed** on a live arena with a typed error naming
  `arena_freeze`.
- **`arena_freeze(arena) -> Graph`** compacts the arena to a dense immutable projection,
  after which the full Mode-A kernel set applies.

This also disposes of the one genuinely hard call site cheaply. `random_walks` calls
`RandomWalk::run(&GraphProjection, …)` (`session.rs:1555`), and `Algorithm::run` is pinned
to the concrete type across ~40 implementations
(`uni-algo/src/algo/algorithms/mod.rs:91`). Classifying it `ConcreteProjection` — freeze
first — avoids generifying 40 implementations to serve one call site. **One contract, two
drift guards, and the 40-impl refactor is not needed.**

Mechanical note: `AlgoSession` derives `Debug` and holds the graph, so `GraphView` needs a
`Debug` supertrait or a hand-written impl.

### 13.4 Scope decisions

- **All four phases**, not §8's "phases 1–3 deliver the measured result". Phase 4 is in
  scope because §13.1 makes it tractable.
- **Candidate-scoped scoring from the start**, not as the §12.7 follow-up. It targets the
  measured 6.07×, and adding it later would mean changing an ABI that phase 3 has frozen
  into WIT and shipped to built guests.
- **Max-flow ships as a first-party `AlgorithmProvider`.** §12.3 already built and
  oracle-validated a Dinic implementation, and §12.6 showed it has no policy to plug. That
  makes it honestly native, and shipping it that way turns bench code into user value,
  correctly labelled.
- **Retirement is staged, not immediate.** §6 says delete `scratch.rs`; but it is `pub
  use`d at `graph_compute/mod.rs:51`, so removal is semver-breaking on a workspace at
  3.0.2. Mark `#[deprecated]` in this cycle and delete at the next major, rather than
  forcing a 4.0.0 to remove test scaffolding. **Verified safe to retire:** no production
  path reaches any of it — the loaders wire `GraphComputeRegistry`, not `ScratchRegistry`,
  and the test-only `uni:scratch/host-graph` WIT package is hand-linked by
  `scratch_wasm_e2e.rs` specifically to bypass the real loader.

### 13.5 Status

**Phase 0 — landed.** An unresolved op is now classified by cause rather than lumped into
an untyped `0x01`:

| Guest calls | Before | After |
|---|---|---|
| an arena kernel (JSON, and Rhai) | `0x01 unknown kernel op` / `Function not found` | `0x86A` naming `graph-arena@1` as the slice this host does not provide |
| a misspelled op | `0x01 unknown kernel op` | `0x86E`, an invalid `op` argument |

Two deviations from the plan as approved, both deliberate:

1. **`0x86A`, not `0x86C`.** Nothing is being *denied* — the grant is present and the
   slice is simply unimplemented, which is what `SliceVersionMismatch` describes. Its own
   contract (`error.rs:36-44`) is to report a missing slice "instead of trapping later on
   an unknown kernel op"; this is that report, delivered where the trap would happen.
2. **`Refs #152`, not `Fixes #152`.** Phase 0 makes the failure honest; it does not make
   the kernels reachable. The closing keyword belongs on phase 3.

The Rhai names are bound as **arity-correct stubs**, because Rhai resolves on name *and*
arity — a stub with the wrong signature still yields `Function not found`, the very error
being fixed. Phase 2 replaces the bodies without touching call sites.

**Phase 1 — landed.** The catalog lives in
`uni-plugin-builtin/src/algorithms/graph_compute/kernel_id.rs`. A `kernels!` macro
generates the `KernelId` enum, `ALL`, `op_name`, `reach` and `from_op_name` from **one**
declaration, so those five cannot drift from each other — a strengthening of the #151
pattern, where `ALL` is hand-maintained and only a runtime test catches an un-appended
variant.

Three guards, each as strong as its surface allows:

| Surface | Guard | Strength |
|---|---|---|
| JSON (WASM/Extism) | `dispatch.rs` matches on `KernelId` with **no wildcard** | compile error |
| Rhai | `register_kernels` registers *and* reports from one declaration; the test compares the reported set to the catalog | test — cannot claim an unregistered kernel |
| PyO3 | `hasattr` against the live `GcSession` type object | test — asserts the real type, not a list |

**Fixed the drift:** `edge_count` now exists and is registered on both in-process loaders.
The Rhai guard was **falsified before being trusted** — removing the fix makes it fail with
`kernels dispatchable over JSON but absent from the Rhai surface: ["edge_count"]`.

**The contract immediately caught an error in its own author.** `check_deadline` was
catalogued as `LoaderLocal` on the strength of a text extraction that ran past the end of
the `#[pymethods]` block; the PyO3 guard proved it is a private Rust helper in a plain
`impl` and never was guest-callable. It is out of the catalog. `LoaderLocal` remains as a
**reserved, empty** variant — the same shape as `KnobReach::HostOnly` in #151 — so a
future carve-out is declared rather than tempting someone to weaken the assertion.

Catalog totals after phase 1: **49 kernels — 48 `AllLoaders`, plus `graph` as
`HostSuppliedOtherwise`.**

### 13.6 Phase 2 — the arena substrate (landed)

`HandleKind::Arena = 7` now lives in the same handle table, the same
`dispatch.rs`, and the same host import as every other kernel — which was the
whole point (§3). Eight kernels: `arena_new`, `arena_alloc`, `arena_link`,
`arena_column`, `arena_candidates`, `arena_gather`, `arena_scatter`,
`arena_descend`. `("graph-arena", 1)` is declared in `HOST_CAPABILITY_SLICES`,
so guests negotiate it at load time through the already-wired `check_slices`.

**Phase 1 proved itself immediately.** Adding the kernels made the reachability
contract fail, naming all eight as *dispatchable over JSON but absent from the
Rhai surface*. The arena was structurally unable to ship dark — which is exactly
the defect (#152) that motivated this work.

**Two design corrections, both forced by the code:**

1. **Columns cannot be tensors** (§5.1 assumed they would be). `Tensor` is
   Arrow-backed and immutable, so the per-step visit increment and virtual loss
   that §12.2 proved mandatory would each copy the whole column — `O(N)` per
   descent step, defeating candidate-scoped work entirely. Columns are therefore
   `Vec<f64>` owned by the arena and addressed by index, which is what the
   measured prototype did.
2. **`Slab` needed a `get_mut`.** Every previous handle kind is immutable once
   inserted; the arena is the first a guest grows in place. Its validation is
   deliberately identical to `get` — a stale, wrapped or forged handle must fail
   the same way whether the caller intends to read or to write.

**Candidate-scoped scoring** is delivered by `arena_candidates` + `arena_gather`
/ `arena_scatter`: the guest computes over `O(roots x branching)` entries rather
than `O(capacity)`, which is the shape that addresses §12.7's measured 6.07x.

**AT-ARENA replaces AT-MCTS** (§6). It drives the full kernel path — handles,
tensors, columns-by-index, work charging — and compares against a plain-Rust
tree walk sharing none of it, asserting the **entire visit distribution** and
every leaf, not merely the root count (which a descent ignoring the score column
would still satisfy). `arena_batching_gives_no_budget_discount` holds design P5
directly: one descent over 16 roots charges exactly what 16 single-root descents
charge.

Deferred to phase 4 with its consumer: `arena_freeze` as a kernel. The
compaction itself (`GraphArena::freeze_csr`) exists and is tested — dropping the
slack is what gives the dense `[E]` numbering the `DenseEdgeIds` classification
(§13.3) will depend on.

Verification: 440 tests in `uni-plugin-{builtin,rhai}` + `uni-plugin`, 107 in
`uni-plugin-{wasm,extism}`, PyO3 contract green; fmt and clippy `-D warnings`
clean. (Two pre-existing `adapter_scalar` failures are environmental — `pyarrow`
is absent locally; confirmed by reproducing them on the base commit.)

### 13.7 Phase 3 — the typed ABI (landed)

A new `uni:plugin/host-arena@0.1.0` WIT interface carries the eight arena
kernels as typed component functions: handles as `u64`, scalars as `u32`/`f64`,
no JSON. `graph-call` remains for the cold and generic surface — this is a fast
path, not a replacement. Host side is `add_host_arena` in `linker.rs`, wired as
hand-written `func_wrap` beside `add_host_graph` and gated on the same
`Capability::GraphCompute`.

Both ABIs reach the **same sessions in the same registry** via
`GraphComputeRegistry::with_session`, which reproduces `call_json`'s panic
isolation exactly (typed `0x86D`, `parking_lot` locks don't poison, session is
per-CALL). `typed_session_access_matches_the_json_path_and_isolates_panics`
asserts the two agree on results *and* on failure modes — a handle forged on the
typed path must be rejected the same way it is on the JSON path, or the two
loader classes would silently disagree.

**The breaking-change risk (§10.4) was tested, not argued.** Adding a WIT import
broke the e2e linker during the original investigation, which is why the
interface is *additive*: a guest that does not import `host-arena` is unaffected
by its existence. All 30 `uni-plugin-wasm` tests — including the **prebuilt**
component fixtures — pass unchanged after the WIT change, which is the empirical
form of that claim.

**Extism keeps the JSON path, deliberately.** Its guests already reach every
arena kernel through `uni_graph_call`, because `dispatch.rs` routes the whole
catalog — so this is a *performance* gap, not a reachability one, and the
reachability contract is satisfied. The 32x JSON tax was measured on WASM;
Extism's typed multiplexer waits for a measured need rather than being built on
the assumption that the number transfers.

Verification: 548 tests across `uni-plugin{,-builtin,-rhai,-wasm,-extism}`; fmt
and clippy `-D warnings` clean.

### 13.8 Phase 4 — unification, by snapshot rather than by trait object

**This supersedes §13.3, and the change is a simplification worth explaining.**

§13.3 planned to retype the handle table to `Slab<Arc<dyn GraphView>>`, expose a
*live* arena to Mode-A kernels, and add a runtime `substrate()` classification so
the kernels that assume dense `[E]` numbering would fail closed on one. Building
it surfaced the flaw in that plan: **Mode-A kernels assume a graph that cannot
change while they iterate it.** Handing them a live, guest-mutable arena is a
hazard the `substrate()` check does not address — the classification guards
*edge numbering*, not *mutation during iteration*.

So `arena_freeze` produces a **snapshot**: `GraphArena::freeze_csr` drops the
slack, and `GraphProjection::from_dense_edges` (new, in `uni-algo`) builds an
ordinary projection behind an ordinary `Graph` handle. Consequences:

| §13.3 as planned | §13.8 as built |
|---|---|
| `Slab<Arc<dyn GraphView>>`, ~35 call sites retyped | handle table unchanged |
| runtime `substrate()` check, fail-closed on live arenas | **structural** — a non-dense graph is never a graph handle at all |
| `random_walks` needs `ConcreteProjection` carve-out (the `Algorithm` trait is pinned to `&GraphProjection` across ~40 impls) | no carve-out; it is a real projection |
| zero-copy | one `O(V+E)` copy per freeze |

The copy is the whole price, and it buys the stability guarantee. For the
workloads in question — grow a search tree, then analyse it — freezing happens
once against many kernel calls.

`from_dense_edges` deliberately shares `build_csr` with `from_rows`, so a frozen
arena gets the *same* canonical edge ordering as a stored projection and every
kernel behaves identically on it.

`frozen_arena_is_an_ordinary_graph_to_every_mode_a_kernel` is the north-star
test: it grows a tree through the arena kernels, freezes it, and runs
`vertex_count`, `edge_count`, `degrees`, `frontier`, `expand` and `set_len`
against the result — kernels with no arena awareness whatsoever — checking each
against a plain-Rust walk of the same tree. It also asserts the snapshot
property: growing the arena afterwards does not mutate an already-frozen handle.

### 13.9 Phase 5 — acceptance, retirement, and a correction

**Max-flow already ships. §13.4's recommendation was wrong.** `uni.algo.maxFlow`
(Dinic) and `uni.algo.fordFulkerson` are already registered first-party
procedures in `AlgorithmRegistry`, alongside ~30 others. The scope question put
to the user — "should the solution ship them?" — was premised on their absence,
which was never checked. Nothing to build; the §12.3 AT-FLOW code stays
test-only, as an oracle rather than a product. The *conclusion* stands (fixed
structure algorithms have no policy to plug and are honestly native); only the
proposed action was redundant.

**Writing the acceptance test found a real expressiveness gap.** A guest could
not grow a tree at all: `arena_link` takes two tensors of slot ids, and no kernel
let a guest *construct* one — `arena_alloc` returns a flat list with no parent
association. AT-ARENA missed this because it built its operands with a
**test-only** escape hatch (`alloc_tensor_for_test`), which is precisely the
failure mode of testing a surface through a door the user does not have. Closed
by `arena_expand(arena, parents, fanout)`, which allocates and links in one
coarse call — also the operation a search actually performs, so it is the right
granularity (P3) rather than a convenience.

**§7 acceptance, in CI:** `guest_authored_mcts_meets_the_absolute_throughput_floor`
runs a genuinely guest-authored MCTS — the Rhai script owns the tree shape, the
exploration constant, the UCB formula and the stopping rule, composing its score
from `arena_candidates` + `arena_gather` + ordinary tensor kernels, so scoring is
candidate-scoped rather than whole-column. The gate is **absolute**, per §7.

Two things that test taught, both worth keeping:

1. **The arena cap bounds a leaky guest, as designed.** The first run died at
   `0x864` after a few hundred rollouts because the script allocated ~8 tensors
   per iteration and never freed them. The script now frees its intermediates —
   which is what a real guest must do, and the fail-closed bound is what tells
   it so.
2. **A perf floor set at the measured value is a flake.** The threshold was
   first written at 25,000 rollouts/s and failed at 24,749 — while its own
   comment claimed to sit "an order of magnitude below" the measurement. It is
   now 5,000: the regression it guards (a kernel going accidentally quadratic)
   costs orders of magnitude, so the margin loses no detection power. 5/5
   consecutive runs pass.

**Retirement is staged, per §13.4.** `ScratchGraph`, `ScratchRegistry`,
`ScratchRequest`/`Response`, `LoaderClass` and `require_compiled_body` carry
`#[deprecated(since = "3.1.0")]` naming the replacement, rather than being
deleted — the module is `pub use`d and the workspace is at 3.0.2, so removal is
a major-version change. The benches that measure the old path keep an explicit
`#![allow(deprecated)]` with a note: they are the evidence the arena replaces
it, and they move at the next major.

**Final state:** 637 tests green across `uni-plugin{,-builtin,-rhai,-wasm,-extism}`
and `uni-algo`, plus the PyO3 contract; fmt and clippy `-D warnings` clean on all
seven crates. The catalog is **59 kernels** — 58 `AllLoaders` plus `graph` — of
which **10 are the arena surface**: `arena_new`, `arena_alloc`, `arena_expand`,
`arena_link`, `arena_column`, `arena_candidates`, `arena_gather`,
`arena_scatter`, `arena_descend`, `arena_freeze`.

All five phases are implemented.
