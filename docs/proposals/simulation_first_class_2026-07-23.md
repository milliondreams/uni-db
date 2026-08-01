# First-class simulation — unifying the ephemeral-world substrate (2026-07-23)

**Status:** Design proposal — evidence-complete, implementation-ready. · **Date:** 2026-07-23 · **Trigger:** design question — "does MCTS (or some other simulation) belong as first-class in uni-db?" · **Baseline:** local `main` `254b4c26c` (v3.0.x, post graph-compute gap-closure). · **Supersedes:** nothing — additive. · **Lineage:** builds on `guest_stateful_compute_2026-07-20.md` (#152, the graph arena / batched MCTS), the Fork track (Part XVI), and the Locy hypothetical/abductive surface (`ASSUME`/`ABDUCE`).

---

## 1. Summary

The question "should MCTS be first-class?" has a precise answer once you look at the code: **MCTS should not be a core operator — and it already isn't.** It correctly lives as `graph-arena@1` kernels driven by a guest `AlgorithmProvider`. The black book already made that call deliberately (Part XVI: "a per-rollout MCTS that speculatively writes then throws the result away, thousands of times" is named as a *scratch/arena* workload, not a fork or a query operator).

The real finding is different and more interesting: **uni-db has already built the pieces of a simulation engine, but as four disconnected islands that cannot compose.**

| Island | What it gives | What it can't do |
|---|---|---|
| Locy `ASSUME`/`ABDUCE` | Semantic hypotheticals over the *real* graph, scored by a *real* rule (incl. `PROB`) | **Cold full-fixpoint recompute per hypothetical**; `ABDUCE` is single-ply; one-shot, no reuse |
| `Session::scratch()` (G8/E2) | ~1 ms write-isolated ephemeral world over the real graph | **Non-branchable leaf** — can't cheaply spawn a *tree* of child worlds; can't be scored declaratively |
| `graph-arena@1` (arena_descend/expand/backup) | 18.5 M MCTS rollouts/s, real select→expand→backup | Runs over a **synthetic in-arena tree** with a guest-supplied score — disconnected from real graph state and from Locy value |
| Lance forks | Durable, diffable, promotable worlds | ~10 ms create/drop — too heavy for per-rollout simulation |

The consequence: today you can have **fast-but-synthetic** (arena MCTS on a made-up tree) *or* **real-but-slow** (a Locy-scored `ASSUME`, cold-recomputed). Nobody can express *"fast tree search whose rollouts mutate the real graph and are scored by a real Locy rule."* That composition is the thing worth making first-class.

**This proposal makes the *environment* first-class, not the *search*.** Three layers, cleanly separated, each reusing machinery that already exists:

1. **WS-1 — Branchable ephemeral worlds.** Promote `scratch()` from a leaf to a tree: stacked in-memory L0 overlays so a child world is an O(1) delta on its parent. This is the tree-of-worlds substrate MCTS needs, with full Cypher/Locy read semantics.
2. **WS-2 — Incremental fixpoint maintenance** *(the keystone engine investment).* After a small graph delta, recompute only the affected strata instead of the whole fixpoint. The runtime is *already* semi-naive with per-iteration delta relations and a reusable stratification DAG — the missing pieces are (a) persisting keyed derived relations across calls and (b) mapping a base-fact change to affected strata. This is the single capability host code fundamentally cannot provide, and it is the actual bottleneck for every semantic simulation.
3. **WS-3 / WS-4 — A composable surface.** Expose "fork a world / apply a delta / score a world" as `world_*` kernels on the *single* GraphCompute substrate so the existing arena MCTS controller drives *real* worlds scored by *real* Locy `PROB`; and let `ASSUME`/`ABDUCE` ride WS-1+WS-2 for a large speedup for free.

Net: MCTS stays a swappable plugin; the reversible, incrementally-maintained, Locy-scored world becomes the first-class thing — which is exactly what a database is uniquely positioned to own.

---

## 2. Evidence (file:line)

### 2.1 `ASSUME`/`ABDUCE` cold-recompute every hypothetical

- `evaluate_assume` — `crates/uni-query/src/query/df_graph/locy_assume.rs:23`: `fork_l0()` → `execute_mutation()` → `re_evaluate_strata()` → dispatch body → `restore_l0()`.
- `fork_l0` / `restore_l0` — `crates/uni/src/api/impl_locy.rs:1458` / `:1471`: deep-clone the Locy L0 buffer, push/pop an `l0_save_stack` (nesting works); rollback is a pointer swap.
- **The cost:** `re_evaluate_strata` — `impl_locy.rs:1485` clones **all** `program.strata` into a fresh program (`:1490`) and calls `run_strata_native` (`:1507`), which rebuilds a fresh logical plan + new DataFusion planner and runs the **entire** multi-stratum fixpoint to convergence. No memo/cache/delta is carried from the base evaluation. **Every hypothetical is a cold full fixpoint.**
- `evaluate_abduce` — `locy_abduce.rs:33`: candidates from the target rule's own MATCH pattern, `truncate(max_abduce_candidates=20)`, then validated **sequentially** (`:95`), each doing its *own* full `fork_l0`→re-eval→`restore_l0` (`validate_modification` `:294`). Cost ≈ up to ~20 complete program evaluations, serial. Single-modification only.

### 2.2 The fixpoint runtime is *already* semi-naive with a reusable stratification DAG

- `run_fixpoint_loop` — `locy_fixpoint.rs:1191`; semi-naive: `update_derived_scan_handles` (`:4714`) injects **only the delta** for linear self-recursive rules (`:4732`), full facts otherwise (`:4736`); `merge_delta` (`:557`) keeps truly-new rows; convergence = empty delta + stable monotone aggregates (`:705`).
- Derived state: `FixpointState { facts: Vec<RecordBatch>, delta, key_column_indices, monotonic_agg, row_dedup }` — `locy_fixpoint.rs:443`; KEY-based dedup via Arrow `RowConverter` + persistent `HashSet` (`RowDedupState` `:334`).
- Stratification DAG exists and is reusable: `DependencyGraph { positive_edges, negative_edges }` — `crates/uni-locy/src/compiler/dependency.rs:11`; `stratify` (Tarjan SCC + Kahn) → `LocyStratum { id, rules, is_recursive, depends_on }` (`planner_locy_types.rs:21`).
- **The two real gaps** (confirmed): (a) **no cross-call persistence** — `DerivedStore::new()` is fresh per call (`locy_program.rs:838`) and dropped; only rule *source* persists (`catalog/locy_rules.json`). (b) **base relations are not nodes** in the dependency graph — only inter-rule edges exist, so a base-fact change can't yet be traced to affected strata.

### 2.3 Cheap ephemeral worlds exist — but as a non-branchable leaf

- `Session::scratch()` — `crates/uni/src/api/session.rs:782`; `Transaction::new_scratch` (`transaction.rs:210`) sets `ephemeral=true`, writes into a private `tx_l0` over `pinned_at_version(hwm)` (shares the AdjacencyManager, so scratch edges traverse), in-memory id allocator seeded above primary HWM; `commit()` refused (`transaction.rs:966`). ~1 ms, no branch/registry/WAL. **But it pins *primary* — it cannot open a child world off *another scratch world's* state.**
- L0 clone-on-freeze substrate that makes this cheap: `L0Manager::pin_snapshot` (O(1) Arc clones, `l0_manager.rs:247`), `freeze_current_for_snapshot` (lazy deep-clone only on write-into-pinned, `:289`). This is the mechanism WS-1 extends to *stacked* overlays.

### 2.4 The MCTS controller already exists — over a synthetic tree

- Mutable arena `graph-arena@1` (`HandleKind::Arena`): `arena_descend(roots, score_col, visit_col, vloss)` = UCT selection with in-loop virtual loss (`arena.rs:352`), `arena_backup(value_col, leaves, deltas)` = value backprop (`:237`), `freeze_csr()` (`:418`). 18.5 M rollouts/s (WASM typed ABI) per `guest_stateful_compute_2026-07-20.md` §2.2.
- Authored as an `AlgorithmProvider` (`crates/uni-plugin/src/traits/algorithm.rs:687`: `signature()` + `run(ctx) -> SendableRecordBatchStream`), invoked via the CALL bridge (`executor/procedure.rs:641`). **No write-back to the store** — arena mutation is discarded at session close; the only egress is `emit`. The arena's node *state* is synthetic (`f64` columns), **not** a real graph world, and its *value* is a guest formula, **not** a Locy rule.

### 2.5 The value function is already expressible

- `ASSUME`'s THEN block returns arbitrary `QUERY`/`Cypher` rows (`locy_assume.rs:72`); a `QUERY` returning a folded `PROB` column yields a `Value::Float` scalar per KEY. `MnorAgg` (noisy-OR `1−∏(1−pᵢ)`, `locy_aggregates.rs:662`), `MprodAgg` (`:779`), BDD exact WMC per shared-lineage group (`locy_bdd.rs:42`). **A Locy rule is already a scalar value function over a world** — it just isn't callable cheaply in a loop.

---

## 3. Root cause: the environment was never made composable

Each island was built for its own use case and stops at its own boundary:

- `ASSUME`/`ABDUCE` own the *semantics* (real graph, logic, probability) but pay a **cold full recompute** because the fixpoint runtime has no cross-call materialized derived store and no base-fact→stratum change tracking — even though it is *already* semi-naive internally.
- `scratch()` owns *cheap real-graph isolation* but stops at a **single leaf** — no child worlds, so no search *tree*.
- The arena owns the *search controller* but over a **synthetic structure**, because there is no kernel that says "fork a real world, apply this delta, score it."
- Forks own *durability* but are **too heavy** per rollout — correctly excluded already.

The disease is the same one named in `guest_stateful_compute_2026-07-20.md` §3: capabilities built as **parallel stacks** instead of composable pieces on one substrate. A simulation is intrinsically *environment × value-function × controller*; uni-db has strong implementations of all three that **cannot be wired to each other**.

---

## 4. Design principles

- **P1 — Make the environment first-class, not the search.** The DB owns the reversible, incrementally-maintained, scored world. The search policy (MCTS, beam, branch-and-bound, RL rollout) stays a swappable guest. This mirrors AlphaZero-class separation and the existing `AlgorithmProvider` split.
- **P2 — One substrate (inherited from #152).** New capability = new *handle kind* + new *kernel arms* on the single `GraphComputeRegistry`/dispatch, never a new stack. `world_*` kernels join `arena_*` and the read kernels; reachability follows by construction.
- **P3 — Reuse, don't rebuild.** WS-1 extends the existing `tx_l0`/clone-on-freeze overlay; WS-2 extends the existing semi-naive delta loop + stratification DAG; WS-3 reuses `PROB` fold as the scalar scorer; WS-4 reuses the arena controller. No greenfield engine.
- **P4 — Phase by monotonicity.** Incremental maintenance is easy for monotone growth (edges added) and hard for retraction (edges/props removed under negation/`PROB`/non-monotone aggregates). Ship monotone-first; gate retraction behind a differential oracle.
- **P5 — Never persist a simulation silently.** Ephemeral worlds discard on drop (like `scratch()`); promotion to a durable timeline is an *explicit* hop to the fork system, not a side effect.

---

## 5. Design

### 5.1 WS-1 — `ScratchWorld`: branchable ephemeral worlds

Promote the scratch path from a leaf transaction to a tree of worlds via **stacked in-memory L0 overlays**.

- New handle `ScratchWorld` wrapping the existing pinned read base + a *chain* of `tx_l0` overlays. Reads resolve `child_overlay ∪ … ∪ parent_overlay ∪ pinned_base` — the same base-chaining idea Lance forks use via `base_paths`, but entirely in-memory and O(1) to create.
- API (Rust, mirrored to Python per the cross-language symmetry contract):
  - `Session::scratch_world() -> ScratchWorld` — root world over primary's pinned HWM (thin wrapper over today's `scratch()`).
  - `ScratchWorld::fork(&self) -> ScratchWorld` — child sharing the parent's overlay chain by `Arc`, plus a fresh empty overlay on top. Cost = Arc clones; **no re-pin of primary**.
  - `ScratchWorld::apply(&self, mutations)` / `query(&self, cypher)` / `locy(&self, program)` — read-your-writes against the chain.
  - Drop = discard (Arc reclaim). `commit()` refused, exactly as scratch tx.
- This fixes the leaf limitation flagged in §2.3 and is the tree substrate MCTS descends.

### 5.2 WS-2 — Incremental fixpoint maintenance *(keystone)*

Given a world `W` with a materialized derived store `D(W)` and a delta `ΔG` from a rollout, compute `D(W ⊕ ΔG)` incrementally.

- **(a) Persist keyed derived relations per world.** Attach `FixpointState` Arrow batches (already KEY-keyed, §2.2) to a `ScratchWorld` as a `MaterializedDerivedStore`, tagged with an epoch. A child world inherits the parent's store by `Arc` and overlays its own deltas — the same chaining as WS-1, one level up the stack.
- **(b) Base-fact → stratum change tracking.** Augment `DependencyGraph` with base-relation leaf nodes (labels/edge-types → rules that read them) so `ΔG` maps to the minimal set of affected strata via the existing `depends_on` topology. Unaffected strata are reused verbatim from `D(W)`.
- **(c) Monotone incremental (Phase 1).** For additive deltas over monotone rules (no `IS NOT`, monotone folds): seed the semi-naive loop with `ΔG` as the initial delta and run only the affected strata to a local fixpoint. This is a direct extension of `run_fixpoint_loop` — the delta plumbing already exists; it just needs a non-empty seed and a stratum mask.
- **(d) Retraction (Phase 3).** For removals and the non-monotone tier, apply **DRed** (Delete-and-Rederive) counting scoped to affected strata; for `PROB` groups, recompute WMC/noisy-OR only for touched KEY groups (BDD is already per-group, §2.5). This is the genuinely hard part — see Risk R1.
- **Payoff:** `ASSUME` re-evaluation drops from "one full program eval" to "the affected strata over `ΔG`," and `ABDUCE`'s ~20 serial full evals become ~20 incremental evals off a shared cached base. This is the capability host code cannot replicate.

### 5.3 WS-3 — `Scorer`: a compiled value function over a world

- A `Scorer` compiles a Locy program (with a designated `PROB`/aggregate output column) or a Cypher aggregate **once**, and evaluates it against a `ScratchWorld`'s derived store via WS-2, returning a `Value` (typically `Value::Float`) — no re-parse/re-plan per call.
- API: `world.score(&scorer) -> Value`. This is exactly what `ASSUME { … } THEN { QUERY r RETURN prob }` already computes (§2.5), lifted into a reusable, loop-callable object.
- Neural value models (Xervo `CREATE MODEL` predicates) plug in here later without changing the interface.

### 5.4 WS-4 — Composable surface

Two fronts, both additive:

- **(a) `world_*` kernels on the one substrate (P2).** Add handle kind `World` and kernel arms `world_fork`, `world_apply(delta_handle)`, `world_score(scorer_handle) -> f64`, `world_free` to `dispatch.rs`/`kernel_id.rs`. Now the **existing** arena controller (`arena_descend`/`expand`/`backup`) can bind each tree node to a real `World` handle and use `world_score` as its value — real graph rollouts, Locy-scored, at arena throughput. The controller stays a guest `AlgorithmProvider` (e.g. `uni.sim.plan` / `uni.algo.mcts`); **no core operator added.**
- **(b) Locy surface upgrade (free win + new capability).** Re-implement `ASSUME`/`ABDUCE` on WS-1+WS-2 so every existing user gets the incremental speedup. Add an optional multi-step abduction (`ABDUCE … USING mcts`) that drives the WS-4a controller to find *interventions requiring several coordinated edits* — lifting today's single-modification limit (§2.1).

### 5.5 WS-5 — Batched worlds *(stretch)*

Evaluate N sibling worlds' deltas in one vectorized fixpoint pass, tagged by a `world_id` column, to reach the 18.5 M-rollouts/s regime validated in `guest_stateful_compute_2026-07-20.md` §2.2 — but now over real, Locy-scored worlds. Only pursued if the controller becomes throughput-bound after WS-1–WS-4.

---

## 6. Phasing

| Phase | Work-streams | Breaking? | Gate |
|---|---|---|---|
| 0 | WS-1 `ScratchWorld` branchable tree | additive | child-fork isolation tests (writes in child invisible to parent/sibling) |
| 1 | WS-2 (a)(b)(c) monotone incremental + persisted store; wire `ASSUME` behind a flag | additive | **differential oracle**: incremental result ≡ cold recompute on monotone programs; ≥1 order-of-magnitude speedup on repeated `ASSUME` |
| 2 | WS-3 `Scorer` + WS-4a `world_*` kernels; arena MCTS over real worlds e2e | additive | end-to-end `uni.sim.plan` bench: real-graph rollouts scored by a `PROB` rule |
| 3 | WS-2 (d) retraction / non-monotone; WS-4b multi-step `ABDUCE` | additive | differential oracle on negation/`PROB`/aggregate programs; multi-edit abduction correctness |
| 4 (stretch) | WS-5 batched worlds | additive | throughput vs Phase 2 baseline |

Each phase is independently shippable and additive (semver-minor), consistent with the repo's "land non-breaking" convention.

---

## 7. Non-goals

- **MCTS (or any search policy) as a core Cypher/Locy operator.** It stays a guest `AlgorithmProvider` over the substrate. The codebase already made this call; this proposal affirms it.
- **Durable / promotable simulation.** That is the fork system (Part XVI) and is out of scope; ephemeral worlds discard on drop. Promotion is an explicit hop to a fork.
- **Distributed / multi-process simulation.** Single-process, in-memory only.
- **A learned value model.** WS-3 admits one later via Xervo, but training/serving neural value functions is not in scope.
- **Replacing the arena.** WS-4a *reuses* `arena_descend`/`backup`; it does not reimplement tree search.

## 8. Verification / acceptance

- **Correctness — differential oracle (repo's metamorphic style).** For every phase, assert `incremental(W, ΔG) ≡ cold_recompute(W ⊕ ΔG)` over a generated corpus of programs and deltas, including the non-monotone tier in Phase 3. This is the primary gate for WS-2 and reuses the TLP/NoREC-style oracle discipline already in the codebase.
- **Isolation.** WS-1: property tests that a child world's writes are invisible to parent and siblings, and that a discarded child leaves primary's id counter and L0 untouched (mirrors the existing scratch-tx invariants).
- **Performance.** Extend the existing MCTS bench arms (`mcts_batched*.rs`) with a "real-world + Locy value" arm; target ≥1 order-of-magnitude over today's cold-`ASSUME` loop, and characterize rollouts/s vs the synthetic-arena baseline.
- **Surface.** `ASSUME`/`ABDUCE` regression suite passes unchanged after the WS-4b re-implementation (semantics identical, faster).

## 9. Risks & decisions

- **R1 — Retraction correctness (WS-2d) is the hard part.** Deleting a derived fact under negation/`PROB` can *add* outputs; getting DRed right for the full Locy semiring is nontrivial. *Mitigation:* Phase monotone-first (covers AddEdge rollouts, reachability, risk-propagation — a large fraction of real use); gate retraction behind the differential oracle; fall back to cold recompute on any stratum the incremental path can't certify.
- **R2 — Memory: derived stores × tree depth.** Persisting `D(W)` per world could blow up on deep trees. *Mitigation:* reuse `max_derived_bytes` (256 MiB) per world, Arc-share unaffected strata down the chain, and LRU-evict with recompute-on-miss.
- **R3 — Positioning (the decision that scales ambition).** The substrate is worth building for **either** driver, but how far WS-4/WS-5 go depends on which one:
  - *What-if analytics / root-cause* → WS-1+WS-2 alone already transform `ASSUME`/`ABDUCE`; WS-4b is the ceiling.
  - *Graph-as-agent-environment* (the DB as a world model an agent plans against) → WS-4a + WS-5 become the point, and this is the differentiated bet.
  - **Recommendation:** commit to WS-1+WS-2 unconditionally (they pay for themselves on the existing surface); scope WS-4a/WS-5 to the answer on R3.
- **Decision D1 — Keep MCTS a plugin.** Confirmed by the code and by the black book's own reasoning. ✅
- **Decision D2 — Incremental fixpoint maintenance is the keystone.** It is the one capability host code cannot provide, the actual bottleneck for semantic simulation, and it upgrades `ASSUME`/`ABDUCE` for free. Recommend committing to it as the highest-leverage engine investment regardless of R3.

## 10. Implementation status

Design only — nothing landed. Baseline `254b4c26c`.
