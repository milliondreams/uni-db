# Fork Implementation Plan

**Companion to:** `uni-locy-docs/proposals/FORK_SPEC_v3.1.md`
**Status:** Historical — superseded by phase commits 0 through 7c.
**Date:** 2026-05-03 (original); preserved for design-rationale reference.

> Phases 0–7c have shipped on `worktree-graph-fork`. The phase
> commit log (`git log --oneline | grep "feat(fork)"`) is the
> authoritative tracker; this document captures the original
> sequencing intent that informed those commits. Stale references
> to "Phase X will…" should be read as historical phrasing —
> consult the latest `AGENTS.md` "Fork system — current
> invariants" block and `API_REVISION.md` "Forks (Phase 6b — diff
> & promote, UID-keyed + edge promotion)" section for the
> shipped contract.

This document phases the implementation of named, durable, isolated graph
sessions (Forks) on top of the current uni-db codebase. It does not
restate the spec — read it first. This plan is about *sequencing*:
what's a prerequisite, what can land independently, what defers risk.

Every phase ends with two non-negotiable subsections — **Testing** and
**Documentation** — covering all the surfaces (Rust tests, Python tests,
TCK feature files, internal docs, public docs, bindings, examples,
skills). The lists in §A and §B below define the canonical set of
surfaces; per-phase entries call out which ones change and how.

---

## 0. Reference points in the current code

| Concern | Location |
|---|---|
| `Session`, `Transaction` | `crates/uni/src/api/session.rs`, `transaction.rs` |
| `UniInner` (the Arc wrapping db state) | `crates/uni/src/api/mod.rs` |
| Multi-dataset commit | `crates/uni-store/src/runtime/writer.rs` (`commit_transaction_l0`, `flush_to_l1`) |
| L0 buffer + rotation | `crates/uni-store/src/runtime/l0_manager.rs` |
| WAL | `crates/uni-store/src/runtime/wal.rs` (and writer.rs) |
| Lance backend | `crates/uni-store/src/backend/lance.rs` |
| Snapshot manifest (uni-level) | `crates/uni-common/src/core/snapshot.rs`, `crates/uni-store/src/snapshot/manager.rs` |
| Schema / catalog | `crates/uni-common/src/core/schema.rs` |
| Read path | `crates/uni-store/src/storage/manager.rs` (`open_label_table`, `open_edge_table`) |
| Indexes | `crates/uni-store/src/storage/index*.rs` |
| Tests / TCK | `crates/uni/tests/`, `crates/uni-tck/`, `crates/uni-locy-tck/` |
| Bindings | `bindings/uni-db/` (Python via PyO3), `bindings/uni-pydantic/` |
| Public docs site | `website/docs/` |
| API references | `docs/complete_rust_api.md`, `docs/complete_python_api.md`, `docs/complete_pydantic_api.md`, `docs/complete_locy.md` |
| Internals doc | `docs/UNI_BLACK_BOOK.md` |
| Skills | `skills/uni-db/`, `skills/uni-db-workspace/` |

---

## §A. Testing surfaces (referenced by every phase)

Every phase must enumerate which of these it touches. "Not applicable"
is an acceptable answer; "I forgot" is not. Use `cargo nextest run` for
all Rust tests (per project convention); use `poetry run pytest -n auto`
for Python.

| Surface | Location | Convention |
|---|---|---|
| **Rust unit tests** | inline `#[cfg(test)] mod tests` in each crate | `cargo nextest run -p <crate>` |
| **Rust integration tests** | `crates/uni/tests/*.rs` | `tokio::test`, `Uni::in_memory()` builder |
| **Storage-layer tests** | `crates/uni-store/tests/` | hit Lance directly; no Cypher |
| **Cypher TCK** | `crates/uni-tck/tck/*.feature` | Cucumber via libtest-mimic; per-scenario JSON in `target/cucumber/nextest/` |
| **Locy TCK** | `crates/uni-locy-tck/tck/*.feature` | same harness |
| **Python tests** | `bindings/uni-db/tests/` | `pytest -n auto` under `poetry run` |
| **Pydantic tests** | `bindings/uni-pydantic/tests/` | same |
| **Crash / fault injection** | `crates/uni-store/tests/recovery_*.rs` | inject failure between WAL and L1, between branches in 2PC |
| **Concurrency / stress** | `crates/uni/tests/concurrency_*.rs` | loom or hand-rolled multi-task; tokio::join! |
| **Examples (compile-as-tests)** | `crates/uni/examples/`, `examples/`, `bindings/uni-db/examples/` | `cargo build --examples` in CI |
| **Compliance reports** | `compliance_reports/`, `locy_compliance_reports/` | regenerated from TCK results |

**Cross-cutting expectations for every phase**

1. No regression in existing TCK (Cypher + Locy). Treat any new red
   scenario as a hard blocker, not "pre-existing."
2. Crash/fault tests live next to the code they exercise. If a phase
   adds a new write path, it adds at least one fault-injection test.
3. Concurrency tests cover both *same-fork concurrent writers* and
   *different-fork concurrent writers*, the latter expected to be
   fully parallel.
4. Both Rust and Python surfaces are exercised once a binding-level
   capability lands. Python tests are not optional — they're the
   only ones that catch FFI marshalling bugs.

---

## §B. Documentation surfaces (referenced by every phase)

| Surface | Location | What it's for |
|---|---|---|
| **Rust API reference** | `docs/complete_rust_api.md` | Exhaustive `Session` / `Db` / `Transaction` API listing |
| **Python API reference** | `docs/complete_python_api.md` | Mirror of the above, in Python |
| **Pydantic OGM reference** | `docs/complete_pydantic_api.md` | OGM-level surface |
| **Locy reference** | `docs/complete_locy.md` | Locy syntax + semantics |
| **Internals (Black Book)** | `docs/UNI_BLACK_BOOK.md` | Architecture, invariants, design rationales |
| **API revision log** | `API_REVISION.md` | Public-API changelog at repo root |
| **Public website — features** | `website/docs/features/` | Feature pages aimed at users (sibling to `snapshots-time-travel.md`) |
| **Public website — concepts** | `website/docs/concepts/` | Architecture, concurrency, identity, data-model |
| **Public website — internals** | `website/docs/internals/` | Storage engine, query planning, etc. |
| **Public website — API** | `website/docs/api/{rust,python}/` | Generated/curated public API |
| **Public website — guides / use-cases** | `website/docs/guides/`, `website/docs/use-cases/` | How-to walkthroughs |
| **Repo README** | `README.md` | Top-level pitch and quickstart |
| **Bindings README** | `bindings/uni-db/README.md`, `bindings/uni-pydantic/` | Python install + quickstart |
| **AGENTS.md** | `AGENTS.md` | Conventions for AI/automation agents working in the repo |
| **Skills** | `skills/uni-db/`, `skills/uni-db-workspace/` | Claude Code skill definitions used by `uni-db` skill |
| **Examples** | `crates/uni/examples/`, `examples/`, `bindings/uni-db/examples/` | End-to-end runnable scenarios |
| **Doctests** | inline `///` in pub items | Compile-checked snippets |

**Cross-cutting documentation expectations for every phase**

1. **Everything user-visible lands in `API_REVISION.md`** before merge.
2. **A new public method gets a doctest.** A doctest that compiles is
   worth more than a paragraph of prose. Run `cargo test --doc`.
3. **The website's feature page is updated in the same PR**, not a
   follow-up. "Docs to follow" PRs rot.
4. **The skill files in `skills/uni-db/`** are how Claude Code agents
   answer fork questions. Update them at the same time as the public
   docs or the skill will lie.
5. **AGENTS.md** is updated when conventions change (e.g. fork-tagged
   WAL recovery, new fault-injection harness).

---

## 1. Risk map (what determines phase order)

1. **Lance branch reachability.** The backend currently uses
   `lancedb::Connection.open_table` — the high-level table API. The
   spec relies on Lance's lower-level branch primitives
   (`_refs/branches/{name}.json`, `base_paths`, `Dataset::open_branch`,
   branch-scoped commit). We need to confirm what the *lancedb* crate
   we're on exposes, and either (a) use it through lancedb, or (b)
   descend to the `lance` crate for branch-aware paths. **This blocks
   everything else** and must be settled in Phase 0.

2. **L0/WAL routing.** Today there is one global L0 and one WAL. Spec
   §6.7 says "single WAL, entries tagged with optional `fork_id`."
   Touching this is the highest-blast-radius change and must be
   phased separately from API work.

3. **2PC across N Lance branches.** Create/drop atomicity (§6.4)
   needs a fork-registry transition machine (`pending` → `active`,
   `tombstoned` → `gone`) that survives crash. New surface, new
   recovery code path.

4. **Snapshot isolation at fork point.** Mostly free if Lance
   `base_paths` works as documented; expensive to fake otherwise.

5. **Session/Transaction generalization.** The `Session` struct wraps
   `UniInner`; today there is no notion of "scope." A
   `fork_scope: Option<ForkId>` field needs to thread to writer,
   storage manager, and watch — touching many call sites but each
   change is mechanical.

The plan front-loads the risky bits (Phase 0, 1, 2) and leaves
ergonomics, indexing, diff, and promotion for later phases.

---

## 2. Resolved unknowns and design decisions

The plan-review pass surfaced ~20 unknowns. Codebase + Lance/lancedb
research has resolved most; the rest are decided here on principle so
implementation isn't blocked. Anything still empirical is in §3
(verification suites).

### 2.1 Lance branch API path — descend to the `lance` crate

- Pinned versions: **lancedb 0.27.1**, **lance 3.0.1** (root
  `Cargo.toml`).
- **lancedb 0.27.1 does not expose branches.**
  `DatasetConsistencyWrapper::as_branch()` is literally
  `todo!("Branch support not yet implemented")`.
- **lance 3.0.1 has full branch primitives:** `create_branch`,
  `delete_branch`, `force_delete_branch`, `checkout_branch`,
  `list_branches`, `tags()`, plus `Manifest::branch` and
  `Dataset::base_store_params` for multi-base resolution.
- **Decision:** add `crates/uni-store/src/backend/lance_branch.rs`
  that wraps `lance::Dataset` directly for fork ops. Keep `lancedb`
  for high-level table ops (vector search, table-level indexes).
  Don't bump lancedb.
- **Concrete call mapping** (Phase 0 §7 deliverable):

  | Operation | Lance API |
  |---|---|
  | Create branch | `Dataset::create_branch(name, parent_version)` |
  | Open on branch | `Dataset::checkout_branch(name)` |
  | Commit to branch | `table.add(...).execute()` after checkout |
  | Drop branch | `Dataset::delete_branch(name)` |
  | Tag | `Dataset::tags().create(...)` |
  | List | `Dataset::list_branches()` |

- **`base_paths` chain** is stable in 3.0.1; nested forks chain
  automatically via `base_store_params`.
- **Closes Open Question 1.**

### 2.2 Adjacency and on-the-fly dataset creation

A fork tx touching `Supplier` (vertex) + `SUPPLIES` (edge) writes to:
- 1 vertex dataset (`vertices_Supplier`)
- 2 delta datasets (`deltas_SUPPLIES_fwd`, `deltas_SUPPLIES_bwd`)
- 2 adjacency datasets (`adjacency_SUPPLIES_fwd/bwd`) — *only if
  they exist*; created lazily by compaction (`storage/adjacency.rs`).

→ **Fork creation must branch every dataset that exists at fork
point** for labels/edge types in the schema. Adjacency branches are
conditional; if compaction runs on the fork, the fork's compactor
creates them branch-locally.

→ **New labels/edge types CAN be introduced on a forked session.**
`flush_to_l1` (`runtime/writer.rs:2365`,
`storage/vertex.rs:270-276`) creates the Lance dataset on demand if
absent. Fork-local creation flow:
  1. flush_to_l1 detects the dataset doesn't exist on the fork's
     branch.
  2. Create the Lance dataset *and* its branch in one step.
  3. Update a **fork-local schema overlay** (new construct), not
     primary's schema.

→ **This resolves the spec §14 "no schema evolution on forks"
ambiguity:** fork-local CREATEs of new labels are *allowed*. What's
banned is mutating primary's schema from a fork. Promotion of a
fork-local label to primary creates the primary dataset as a side
effect (Phase 6 concern; tracked in new Open Question 7).

### 2.3 Fork-local schema overlay — new construct

Not in the spec; needed for §2.2. Phase 1 scope addition:

- `ForkInfo` gets a sibling file `catalog/fork_schemas/{fork_id}.json`
  carrying a `SchemaDelta` (added labels, added edge types, added
  properties — all *additions only*, no renames or drops).
- Read-time schema for a fork session: `primary_schema ⊕ overlay`,
  walking parent chain.
- Separate file (not embedded in `fork_registry.json`) so a registry
  read doesn't pull schemas for inactive forks.

### 2.4 Shared-state scoping

| Surface | Today | Forked-session policy | Rationale |
|---|---|---|---|
| **Plan cache** | Session-scoped, Arc-shared on `Session::clone()` (`session.rs:163-165, 906-932`) | **Fresh, empty per fork** | Plans encode storage layout; branch-aware reads may pick different operators (esp. Phase 5 `FusedIndexScan`) |
| **Procedure registry** | DB-global, read-only via API (`mod.rs:70`) | **Stays DB-global** | Procedures are platform code |
| **Locy rule registry** | Session-scoped, cloned per `Session::clone()` (`session.rs:189`) | **Fresh copy of parent's at fork creation** | Matches today's clone semantics; `forked.tx().locy("CREATE RULE")` rules become fork-local, aligned with §4.4 hooks |
| **ASSUME state** | Tx-scoped, ephemeral (`locy_assume.rs:23-94`) | **Unchanged** | Spec §14 defers hypothesis persistence |

**Quirk to thread through:** `tx.locy()` today consults the *global*
`db.locy_rule_registry`, not the session's
(`locy_builder.rs:245-253`). On a forked session, `tx.locy()` must
consult the fork's rule registry, with promotion-on-commit landing on
the fork. Phase 2 scope addition.

### 2.5 Other semantic decisions

- **TTL clock** — `expires_at` stored as absolute UTC `SystemTime` in
  registry. Sweeper compares to `SystemTime::now()`. No monotonic
  clock; clock-jump behavior documented (forks may expire earlier or
  later than wall-clock-elapsed if system clock jumps).
- **ASSUME inside a fork tx** — unchanged; forks the fork tx's L0,
  restores on exit.
- **Watch event ordering** — per-source FIFO. Cross-source ordering
  is the client's problem.
- **Cross-binding fork concurrency** — no special handling. Lance MVCC
  + per-session write-guard handles it regardless of language.
- **WAL retention with long-lived forks** — fork-tagged WAL entries
  retained until fork is dropped. `WalGc` becomes fork-aware: a
  segment is reclaimable iff (primary entries flushed) AND
  (fork-tagged entries either flushed to fork L1 or fork is
  tombstoned). **Phase 2 scope addition.**
- **Cancel propagation** (§4.6) already added in earlier review pass.

### 2.6 Updated Open Questions

| # | Question | Status |
|---|---|---|
| 1 | Lance API path | ✅ resolved (§2.1) |
| 2 | `fork_scope` on `UniInner` vs `Session` | ✅ **on `Session`** — supports many fork sessions per `UniInner` |
| 3 | Recovery for partial branch creation | ✅ tombstone partials eagerly; covered in Phase 1 recovery test |
| 4 | Watch filter source vs subscriber | ✅ **at source** — cheaper, cleaner contract |
| 5 | Python timing | ✅ smoke in Phase 2, full in Phase 4 |
| 6 | Fork-local schema overlay storage | Separate `catalog/fork_schemas/{id}.json` (§2.3) |
| 7 | Promoting a fork-only label to primary | Phase 6 must create primary dataset as side effect; tagged historical primary states unaffected |

### 2.7 Empirical questions still open (verified post-implementation)

These cannot be answered without running code; §3 below has the
test suite for each.

- E1. Lance compaction *actually* honors branch references in 3.0.1.
- E2. Practical branch-count limit per dataset.
- E3. `base_paths` chain-depth performance curve.
- E4. Branch creation latency under concurrent contention.
- E5. Memory footprint per fork session.
- E6. Fork-local dataset+branch creation atomicity for new labels.
- E7. Long-lived fork WAL retention behavior under GC.
- E8. Cross-binding (Python ↔ Rust) fork concurrency under load.

---

## 3. Verification test suites (empirical post-implementation)

Each suite below has a **threshold** (passes spec assumptions) and a
**kill-switch** (failure mode that forces a redesign rather than a
prose fix). These are *additional* to the per-phase functional tests
listed inside each phase — they exist to validate the empirical
unknowns from §2.7.

### 3.1 Lance retention honors branches (E1) — Phase 0 spike + Phase 2 regression

**Suite:** `crates/uni-store/tests/lance_branch_retention.rs`
- Create dataset → V0 (1k rows). Branch off V0; write 1k rows on
  branch. Run `cleanup_old_versions(Duration::ZERO, false, false)`
  on primary. Read all rows via branch.

**Threshold:** branch reads return original V0 rows + branch-local
rows.
**Kill-switch:** any V0 rows missing → spec §10 assumption invalid →
fall back to "(d) multi-dataset emulation" (spec-deferred). **Stops
Phase 1.**

### 3.2 Branch-count scaling per dataset (E2) — Phase 4

**Suite:** `crates/uni-store/tests/lance_branch_scaling.rs`
(gated `#[ignore]`; nightly CI).
- Create N branches off one dataset for N ∈ {10, 100, 1000, 10000}.
  Measure: branch creation time, open-on-branch latency, manifest
  size, primary read latency.

**Threshold:** branch creation ≤ O(log N); manifest size linear in
N; primary read latency unchanged.
**Kill-switch:** any quadratic curve → cap `max_forks` at the
measured safe value; document the cap on
`website/docs/features/forks.md`.

### 3.3 `base_paths` chain depth (E3) — Phase 3 + Phase 5

**Suite:** `crates/uni/tests/fork_chain_depth.rs`
- Chain of depth D ∈ {1, 5, 20, 50}. Full-table scan and indexed
  lookup at each depth.

**Threshold:** depth 5 ≤ 5× depth 1; depth 50 ≤ 25× depth 1.
**Kill-switch:** depth 50 > 100× depth 1 → unblock spec §16
"deep-chain materialization"; ship with a `max_chain_depth`
enforcement in registry.

### 3.4 Concurrent branch creation (E4) — Phase 1 + Phase 2

**Suite:** `crates/uni/tests/fork_creation_concurrency.rs`
- 16 concurrent `session.fork(name_i)` calls, distinct names, same
  parent. Compare wall time vs serial.

**Threshold:** ≤ 2× serial.
**Kill-switch:** ≥ 16× serial → fork creation is a global bottleneck;
add a creation queue with backpressure + document.

### 3.5 Memory per fork session (E5) — Phase 4

**Suite:** `crates/uni/benches/fork_session_memory.rs` (Criterion +
RSS tracking).
- Open 1, 10, 100, 1000 fork sessions; measure resident memory delta
  per session after stabilization.

**Threshold:** ≤ 1 MiB per idle session.
**Kill-switch:** ≥ 10 MiB per session → revisit per-fork
rule-registry copy (likely heaviest); consider copy-on-write.

### 3.6 Fork-local schema overlay (E6) — Phase 1 + Phase 6

**Suite:** `crates/uni/tests/fork_schema_overlay.rs`
- Fork primary with labels {A, B}. On fork, `CREATE (n:C)`. Assert:
  (a) primary schema file unmodified; (b) fork sees A, B, C;
  (c) restart preserves fork's view; (d) `fork_info` reports schema
  delta. Promotion: `promote_from_fork` of `(c:C)` creates dataset
  C on primary as a side effect.

**Threshold:** all four assertions pass; primary schema file
byte-for-byte unchanged by fork tx.
**Kill-switch:** primary schema mutated by fork tx → overlay leak →
block promotion until fixed.

### 3.7 WAL retention with long-lived forks (E7) — Phase 2

**Suite:** `crates/uni-store/tests/wal_fork_retention.rs`
- Create fork; alternate 100 MB writes to fork and primary. Run WAL
  GC. Assert fork-tagged segments retained, primary-only segments
  past flush reclaimed. Drop fork; re-run GC; assert fork segments
  now reclaimable.

**Threshold:** disk usage grows monotonically until drop, then drops
to primary baseline.
**Kill-switch:** GC drops live fork WAL → fork data loss → block
Phase 2 merge.

### 3.8 Cross-binding fork concurrency (E8) — Phase 4

**Suite:** `bindings/uni-db/tests/test_fork_cross_binding.py`
- Same fork held by a Rust session and a Python session
  concurrently; interleaved writes.

**Threshold:** matches the Rust-only same-fork concurrent-writers
test result; no FFI deadlock.
**Kill-switch:** any deadlock → block Python fork release; ship with
a "Python forks are single-process-only" note while we fix.

### 3.9 Index fusion recall (Phase 5)

Already inside Phase 5; restating thresholds here for the empirical
table.
- **Threshold:** recall ≥ 99% (lossless types: BTree, Sorted,
  VID/UID); ≥ 95% (ANN, BM25). Latency within spec §8.2 bounds.
- **Kill-switch:** recall regression → fall back to scan-over-(local
  + parent-indexed); document in
  `compliance_reports/fork_index_<date>.md`.

### 3.10 Stress soak (Phase 7)

**Suite:** `crates/uni/tests/fork_replay_soak.rs` — 4-hour run.
- Random mix of create/drop/cascade/write across 50 forks; periodic
  process kill + restart; state consistency check.

**Threshold:** zero data loss, zero registry inconsistency over
4 hours.
**Kill-switch:** any inconsistency → block Phase 7 close-out.

### Verification matrix

| ID | Empirical question | Suite | Phase | Kill-switch consequence |
|---|---|---|---|---|
| E1 | Retention honors branches | 3.1 | 0 + 2 | Stops Phase 1 |
| E2 | Branch-count scaling | 3.2 | 4 | Cap `max_forks` |
| E3 | Chain-depth perf | 3.3 | 3 + 5 | Add `max_chain_depth` |
| E4 | Concurrent creation | 3.4 | 1 + 2 | Add creation queue |
| E5 | Per-session memory | 3.5 | 4 | Copy-on-write rule registry |
| E6 | Schema overlay isolation | 3.6 | 1 + 6 | Block promotion |
| E7 | WAL retention | 3.7 | 2 | Block Phase 2 merge |
| E8 | Cross-binding concurrency | 3.8 | 4 | Single-process Python forks |

---

## Phase 0 — Spike: Lance branch primitives (1 week)

**Goal:** prove out the storage substrate before committing to the
spec's design. Output is a go/no-go and one of three concrete plans
for Phase 1.

**Scope**

- Read the version of `lancedb` / `lance` we depend on; enumerate
  what branch API surface is actually exposed in our pinned versions.
- Write a throw-away binary (`crates/uni-store/examples/fork_spike.rs`)
  that creates a Lance dataset, opens a branch, commits writes to it,
  reads back via `base_paths`, and drops the branch.
- If the high-level `lancedb` crate doesn't expose branches, decide:
  (a) bump dependency, (b) descend to `lance` crate, (c) negotiate
  upstream patch, (d) fall back to a multi-dataset emulation
  (deferred — would invalidate §6.1).
- Document fault behavior: what happens if a branch creation
  half-commits across N datasets? This shapes Phase 1/2 (2PC).
- **Produce a concrete API call list** — function names and
  signatures we'll call from `LanceDbBackend` for: open-branch,
  branch-targeted commit, drop-branch, list-branches, retention/GC
  behavior, branch tag. Phase 1 starts day 1 if this list is in
  hand; otherwise Phase 1 begins with archaeology.

### Testing

- **Spike binary** (`crates/uni-store/examples/fork_spike.rs`) is the
  test. It must:
  - Round-trip writes through a Lance branch and read them back.
  - Confirm branch reads chain to parent at depth 2 and depth 3.
  - Confirm parent state is unchanged after branch drop.
  - Confirm primary compaction with a live branch does not delete
    fragments referenced by the branch (retention honors branch
    references — spec §10).
  - Simulate a process kill mid-branch-commit (using a panic point or
    a separate process) and observe the on-disk state — what gets
    left behind?
- **No production tests** in this phase. We don't ship anything.
- Record outcomes as a checklist in the design memo (below) — this is
  what Phase 1 will start from.

### Documentation

- **Design memo** at `docs/proposals/graph_fork_phase0_findings.md`:
  - Lance / lancedb version pinned and what branch APIs it exposes.
  - The recommended call sites in `LanceDbBackend` for each spec §6
    operation (create branch, open for read, commit to branch, drop).
  - The fault-injection observations and what they imply for Phase 3.
  - Decision: (a)/(b)/(c)/(d) above with rationale.
- **No public docs change** in this phase.
- **No `API_REVISION.md` entry** — nothing user-visible.
- **`AGENTS.md`** unchanged unless the spike reveals a workflow we
  want future agents to follow (e.g. "always run the spike binary
  before touching the backend").

**Exit criteria**

- Spike binary green.
- Memo merged, reviewed, and concluded with a Go/No-go for Phase 1.

**Risk if skipped:** the entire spec rests on Lance branches working
the way §6 describes. Skipping the spike turns Phase 1 into the spike.

---

## Phase 1 — Read-only forks (2 weeks)

**Goal:** the smallest possible end-to-end vertical slice. A fork
exists as a named, isolated view of primary at a fork point. **No
user-driven writes** through `forked.tx()` yet — but fork *creation*
itself is a multi-dataset write protected by 2PC, and `drop_fork`
must be crash-safe from day one. Proves the routing and the
2PC machinery on a small surface.

**Scope**

- New crate module: `crates/uni/src/api/fork.rs` (forked-session
  surface) + `crates/uni-common/src/core/fork.rs` (`ForkId`,
  `ForkInfo`, `ForkRegistry` types).
- New persisted file: `catalog/fork_registry.json` (alongside
  `catalog/manifests/`). Schema follows §5 of the spec, including
  the `schema_version_at_creation` field — captured from day one
  even though it's only consumed by the Phase 7 schema-evolution
  spike. Backfilling later is impossible.
- New persisted file: `catalog/fork_schemas/{fork_id}.json` —
  fork-local schema overlay (§2.3). Empty at fork creation; populated
  when the fork creates new labels/edge types. Schema reads on a
  fork session resolve as `primary_schema ⊕ overlay`, walking the
  parent chain.
- New backend module `crates/uni-store/src/backend/lance_branch.rs`
  wrapping `lance::Dataset` directly (§2.1). `lancedb` stays for
  high-level table ops.
- Fresh, empty plan cache and a copy-of-parent Locy rule registry
  per forked session (§2.4). Procedure registry stays DB-global.
- `Session` carries an `Option<ForkScope>`. (The Open Question 2
  outcome may move this onto `UniInner` instead — kept on `Session`
  for now so a single `UniInner` can back many fork sessions.)
- `session.fork(name)` (open-or-create path):
  - Open if exists; create registry entry + Lance branches per
    dataset at the current snapshot version if not.
- `session.fork(name).new_()` — must-create variant; errors with
  `ForkError::AlreadyExists` if the fork is present (spec §2.1,
  §4.2, §13).
- Read routing: `StorageManager::open_label_table` /
  `open_edge_table` accept fork scope and call branch-aware open.
- `Session::tx()` on a forked session returns
  `ForkError::WritesNotYetSupported` in this phase. **Gate is at
  the API layer, not in the registry** — no `read_only` flag is
  added to `ForkInfo`.
- `db.list_forks()`, `db.fork_info(name)`, `db.drop_fork(name)`.
  Drop is the **full 2PC dance** from Phase 1: tombstoned → drop
  all branches → remove entry, with crash recovery. (Phase 2 only
  adds the "drain in-flight writers" step on top — not the
  atomicity itself, which has to hold here.)

### Testing

- **Rust unit tests**
  - `ForkRegistry` round-trip serde + atomic-write under concurrent
    writers (use `tempfile` + spawn threads).
  - `ForkId` allocation and uniqueness.
- **Rust integration tests** (`crates/uni/tests/fork_read_only.rs`)
  - Create primary state, fork, mutate primary further, confirm
    fork's reads still see fork-point state.
  - Drop fork, confirm primary unchanged.
  - Restart (`Uni::open` after drop) — confirm fork persists.
  - Attempt `forked.tx()` and assert
    `ForkError::WritesNotYetSupported` specifically (don't accept
    a generic `anyhow!`).
  - `session.fork(name).new_()` on an existing fork → assert
    `ForkError::AlreadyExists`; on a fresh name → succeeds.
  - Attempt to drop a fork held by another open session — assert
    we surface a clear error rather than corrupting state.
  - **Fork creation does not block primary** (spec §10): start a
    long-running fork creation in one task and assert primary
    reads/writes complete with no measurable latency bump. Catches
    accidental global locks at the registry or backend layer.
- **Storage-layer tests** (`crates/uni-store/tests/fork_branch.rs`)
  - Open dataset, create branch, read through branch — bypass the
    full uni stack to keep failures localized.
- **Cypher TCK**
  - New feature file `crates/uni-tck/tck/Fork.feature` exercising
    `list_forks` and read-only fork creation through a scenario
    helper. (Cypher itself doesn't gain syntax — the TCK exists to
    cover the API at the integration level.)
  - Run full existing TCK and confirm zero regressions.
- **Python tests** — *not in this phase*. Python binding lands in
  Phase 4 (lifecycle/admin) when the surface stabilizes. Document
  this deferral in the phase 1 PR.
- **Crash recovery** (`crates/uni-store/tests/recovery_fork_create.rs`,
  `recovery_fork_drop.rs`)
  - Create: inject failure after creating M of N branches. Restart
    → registry transitions `pending` → either `active` (if all
    branches present) or rolled back (if not).
  - Drop: kill mid-tombstone (after tombstone marker, mid-branch
    deletion). Restart → drop completes.
- **Concurrency** — non-trivial user-write concurrency lives in
  Phase 2; Phase 1 only validates that fork creation itself is
  non-blocking against primary (covered above).

### Documentation

- **`docs/complete_rust_api.md`**
  - New "Forks" section listing `Session::fork(name)`,
    `Session::fork(name).new_()`, `Db::list_forks`, `Db::fork_info`,
    `Db::drop_fork`. Mark write-related methods as "Phase 2" so the
    reader knows what's coming.
- **`docs/UNI_BLACK_BOOK.md`**
  - New chapter "Forks: storage layout" reflecting Phase 0 findings
    and the registry shape.
- **`API_REVISION.md`**
  - Entry for the new `fork`/`list_forks`/`fork_info`/`drop_fork`
    APIs; explicitly mark "read-only in Phase 1; writable in Phase 2."
- **`website/docs/features/forks.md`** (new page)
  - Sibling of `snapshots-time-travel.md`. Position fork against
    snapshot — both are time-travel-adjacent, but forks are
    *named*, *durable*, and *writable* (with the Phase 2 caveat
    while it's pending).
- **`website/docs/concepts/architecture.md`** — add fork to the
  storage-layer overview diagram + paragraph.
- **`website/docs/api/rust/`** — sync new API.
- **`README.md`** — add a one-line mention under "what's new" with
  a link to the feature page.
- **Examples** — `crates/uni/examples/fork_read_only.rs`: walk
  through the read-only use case.
- **Doctests** on every new public method.
- **`skills/uni-db/`** — add a "Forks" knowledge file mirroring the
  feature page; the skill triggers on mentions of "fork" and
  "branch."
- **`AGENTS.md`** — note that fork registry edits go through
  `ForkRegistry::edit()` (not direct file write) and that the
  registry has 2PC semantics agents must respect.

**Exit criteria**

- Read-only forks survive process restart.
- Primary writes after fork creation invisible to the fork.
- Existing TCK green; new Fork.feature green; recovery test green.
- All documentation surfaces above updated in the same PR.

---

## Phase 2 — Writable forks, single-level (3 weeks)

**Goal:** make `forked_session.tx().commit()` work, landing writes
on the fork's Lance branches. Single fork (no nesting yet).

**Scope**

- L0 buffer becomes scope-aware. Pick **option (B)** — single L0
  with `fork_id`-tagged entries, routed at flush. Document the
  choice in code comments referencing spec §6.7.
- `Writer::commit_transaction_l0` routes per-tx mutations to the
  fork's branches, using `ForkRegistry` to map dataset → branch.
- WAL entries tagged with `Option<ForkId>`; recovery dispatches
  appropriately. Dropped-fork entries are skipped on replay (no
  WAL rewriting — confirms §6.7).
- **Fork-aware WAL GC** (§2.5): a segment is reclaimable iff
  primary entries are flushed AND fork-tagged entries are either
  flushed to their fork's L1 or the fork is tombstoned. Without
  this, a long-lived fork's WAL grows unbounded.
- `flush_to_l1` partitions rotated L0 by `fork_id` and writes each
  partition to its target branches. **On-the-fly dataset+branch
  creation** (§2.2): if a fork tx writes to a label/edge type whose
  Lance dataset doesn't exist yet, flush creates the dataset and
  its branch atomically and updates the fork's schema overlay.
- **`tx.locy()` registry routing** (§2.4 quirk): on a forked session,
  `tx.locy()` consults the fork's rule registry, not the global
  one. Promotion-on-commit lands rules on the fork.
- Per-fork serialization: existing write-guard already covers it.
- `db.drop_fork(name)`: extends the Phase 1 2PC drop with a
  **drain-in-flight-writers** step — tombstone → drain open
  writers (clean error to them) → drop branches → remove entry.
  The atomicity machinery already exists from Phase 1.

### Testing

- **Rust unit tests**
  - `L0Buffer` partition-by-`fork_id` correctness, including the
    `None` (primary) case.
  - WAL replay dispatch under each `fork_id` tag (live, dropped,
    unknown).
- **Rust integration tests** (`crates/uni/tests/fork_writes.rs`)
  - `forked.tx().commit()` lands writes on the fork; primary
    unaffected (verified via `Db` opened with no fork scope).
  - Mixed scenario: alternating primary writes and fork writes
    interleave correctly.
  - Watch on primary does not see fork commits.
- **Storage-layer tests** (`crates/uni-store/tests/fork_commit.rs`,
  `fork_compaction.rs`)
  - Direct test of branch-targeted commit isolation: read primary
    manifest after fork commit, confirm zero references to fork
    files (structural isolation per spec §10).
  - **Primary compaction does not break forks** (spec §10): create
    fork → write to primary heavily → trigger compaction → fork
    reads still resolve. Verifies Lance retention honors branch
    references. This is silent failure surface; without the test
    a future GC tightening will eat fork data.
- **Cypher TCK**
  - Extend `Fork.feature` with write scenarios (CREATE / SET /
    DELETE on a forked session, then read-back). Add a
    `Fork_Concurrency.feature` covering interleaved writes.
- **Crash recovery** — comprehensive matrix in
  `crates/uni-store/tests/recovery_fork_writes.rs`:
  - Kill after WAL append, before L0 rotation → replay reconstructs
    L0; commit visible.
  - Kill after L0 rotation, before L1 write → replay completes L1.
  - Kill mid-L1 across N branches → replay finishes remaining
    branches; manifest atomicity preserved.
  - Kill mid-drop_fork (after tombstone, mid-branch-deletion) →
    restart resumes drop.
- **Concurrency / stress** (`crates/uni/tests/fork_concurrency.rs`)
  - 8 concurrent writers to **different** forks: must show
    near-linear throughput (no contention beyond shared WAL).
  - 4 concurrent writers to the **same** fork via independent
    `session.fork("x")` handles (spec §5: "multiple sessions can
    hold the same fork concurrently"): serialize cleanly via Lance
    MVCC; final state deterministic.
  - Drop-fork while a transaction is open on it: assert clean
    error, not corruption.
- **Python smoke spike** (~1 day): expose `Db.list_forks()` and
  read-only `Session.fork()` to Python so cross-language smoke
  coverage starts here. Full Python surface still lands in Phase 4;
  this spike just shakes out PyO3 wiring before the API moves more.
  Tests in `bindings/uni-db/tests/test_fork_smoke.py`.
- **Spec spike 15.1** — multi-dataset 2PC fault injection. Lands
  here, not in Phase 7, because we want the assurance before
  Phases 3+ build on top.

### Documentation

- **`docs/complete_rust_api.md`** — flip the Phase 2 markers; add
  full write-path docs including the per-fork write-guard rule.
- **`docs/UNI_BLACK_BOOK.md`**
  - New chapter "Forks: write path" — L0 routing, WAL tagging,
    flush partitioning, 2PC for create/drop. Include a sequence
    diagram. This is the canonical reference for future agents
    debugging fork writes.
- **`API_REVISION.md`** — entry for now-writable forks, listing
  any error variants (e.g. `ForkError::DroppedDuringTx`).
- **`website/docs/features/forks.md`** — promote from "read-only
  preview" to full feature; add the write-audit-publish use case.
- **`website/docs/concepts/concurrency.md`** — new subsection
  "Forks and concurrency": same-fork serialization vs
  cross-fork parallelism.
- **`website/docs/internals/storage-engine.md`** — diagram of L0
  partitioning by `fork_id`, WAL tagging, flush routing.
- **Examples**
  - `crates/uni/examples/fork_write_audit_publish.rs` — spec §3.3.
  - `crates/uni/examples/fork_scenario_compare.rs` — spec §3.4.
- **Doctests** for new public methods, including the error path
  for `forked.tx()` after drop.
- **`skills/uni-db/`** — update fork knowledge file: now writable.
  Add a section on the write-path semantics for agents that get
  asked "can two sessions write to the same fork at once?"
- **`AGENTS.md`** — record two invariants:
  - WAL replay must dispatch by `fork_id`; agents touching
    recovery code must preserve it.
  - **Lance compaction retention must not be tightened below the
    longest live fork chain.** Silent corruption otherwise. Any
    perf work touching retention must check fork chains first.

**Exit criteria**

- `forked.tx().commit()` lands writes on the fork; primary
  manifest never references fork files.
- TCK regressions: zero. New fork-write features green.
- Crash-recovery test matrix green.
- 2PC fault-injection spike (spec §15.1) green.
- All documentation surfaces above updated.

---

## Phase 3 — Nested forks (1.5 weeks)

**Goal:** `forked.fork(name)` creates a child whose reads chain
through `base_paths` to its parent.

**Scope**

- `ForkRegistry::parent_fork_id` wired in creation: when called on
  a forked session, the new fork's parent is *that fork*.
- Lance branch creation specifies the parent branch (not main) so
  `base_paths` resolves correctly.
- `db.drop_fork(name)` errors if children exist;
  `db.drop_fork_cascade(name)` walks the tree.
- Cross-fork diff at depth >1 deferred to Phase 6.

### Testing

- **Rust unit tests**
  - `ForkRegistry` parent-child invariants: cycle detection
    (should be impossible by construction, but assert it),
    orphan detection.
- **Rust integration tests** (`crates/uni/tests/fork_nested.rs`)
  - 3-level chain: primary → fork → fork-of-fork. Reads at the
    leaf resolve through both ancestors.
  - 5-level chain perf sanity: latency at depth 5 within 5× depth-1.
  - Snapshot isolation at each level: parent writes after child
    creation invisible to child.
  - `drop_fork` on parent with children → error.
  - `drop_fork_cascade` removes whole subtree.
- **Storage-layer tests**
  - Verify Lance `base_paths` chain resolves correctly across the
    branch parents we set.
- **Cypher TCK**
  - `Fork_Nested.feature`: multi-level scenario per spec §3.5.
- **Crash recovery**
  - Kill mid-cascade-drop across a 3-level subtree → resume
    correctly; orphans not left behind.
- **Concurrency**
  - Concurrent writers across different forks in the same subtree
    do not block each other (same expectation as Phase 2 cross-fork).
  - Cascade-drop while a leaf has an open writer: clean error.

### Documentation

- **`docs/complete_rust_api.md`** — add `drop_fork_cascade`,
  document parent inference rule (parent is implicit — the
  receiver session's fork or primary).
- **`docs/UNI_BLACK_BOOK.md`** — extend forks chapter with
  "fork trees and read resolution" subsection; depth-vs-cost note.
- **`API_REVISION.md`** — entry for nested forks +
  `drop_fork_cascade`.
- **`website/docs/features/forks.md`** — add deep-tree section
  with the spec §3.8 (MCTS-style) example, and a clear callout
  that hypothesis persistence (ASSUME snapshots) is *not* part of
  this so users don't expect it.
- **`website/docs/use-cases/`** — new page or extension covering
  scenario exploration / counterfactuals using nested forks.
- **Examples**
  - `crates/uni/examples/fork_nested.rs` — spec §3.5.
- **`skills/uni-db/`** — update fork file with nested semantics
  and the parent-inference rule. This is a common point of
  confusion; spell it out.

**Exit criteria**

- Spec §3.5 example runs end-to-end.
- Read latency at depth 5 within 5× depth-1.
- All documentation surfaces above updated.

---

## Phase 4 — Lifecycle, admin, Python bindings (2 weeks)

**Goal:** TTL, tagging, budget, capabilities, watch/hooks/params/pin
on forks, **and** the Python binding for everything that's landed
through Phase 3. Bundling Python here pays off: Phases 1–3 stabilize
the Rust API, and Phase 4 binds in one go.

**Scope**

- `SessionConfig::max_forks`, `fork_default_ttl`.
- TTL on creation: `session.fork("x").ttl(...)`.
- Background sweeper task: reaps expired forks via cascade drop.
- `db.tag_fork(name, tag)` — Lance tag, GC-exempt.
- `Session::pin_to_version` / `pin_to_timestamp` / `refresh` made
  fork-aware.
- `Session::watch` on forked session: fork-only filter at
  notification source.
- Hooks/params on forked sessions: per-session, no propagation
  (spec §4.4–4.5).
- **Cancellation propagation (spec §4.6):** cancelling a parent
  session cancels its forked children. Forked sessions link their
  cancellation token to the parent's at construction.
- **Python bindings (`bindings/uni-db/`)**: expose
  `Session.fork()`, `Db.list_forks()`, `Db.fork_info()`,
  `Db.drop_fork()`, `Db.drop_fork_cascade()`, `Db.tag_fork()`, TTL
  builder. Mirror the Rust ergonomics.

### Testing

- **Rust unit tests** — sweeper interval, TTL clock-skew handling,
  budget enforcement.
- **Rust integration tests**
  - `fork_ttl.rs` — TTL expires → cascade drop fires.
  - `fork_pin.rs` — pinned fork rejects writes; refresh restores.
  - `fork_watch.rs` — watch on fork sees only fork commits;
    primary watch doesn't see fork commits; cross-fork doesn't
    bleed.
  - `fork_hooks.rs` — hooks isolated per session; no propagation.
  - `fork_budget.rs` — `max_forks` enforced; clear error.
  - `fork_cancel.rs` — cancelling the parent session cancels
    forked children: child rejects new ops with the right error;
    in-flight tx on the child surfaces cancellation cleanly.
  - `fork_external_sandbox.rs` — spec §3.7 (external system holds
    a forked session and writes to it; operator reviews via the
    same fork). Exercises the watch + hook surface together.
- **Cypher TCK** — `Fork_Lifecycle.feature` covering TTL and tag.
- **Python tests** (`bindings/uni-db/tests/test_fork.py`,
  run via `poetry run pytest -n auto`) — mirror the Rust integration
  tests at the binding level. **Catches FFI marshalling bugs that
  Rust tests can't.** Cover at minimum: read-only fork lifecycle,
  fork writes round-trip, nested forks, TTL, drop, cascade, tag,
  watch, error variants surface as the right Python exception type.
- **Pydantic tests** (`bindings/uni-pydantic/tests/`) — confirm OGM
  works against a forked session.
- **Concurrency** — sweeper running concurrently with writes:
  no torn reads, no crashes.
- **Stress** — 1000-fork creation/drop cycle to exercise budget +
  registry under load.

### Documentation

- **`docs/complete_rust_api.md`** — TTL, tag, watch/hooks/params/pin
  on fork sessions; capabilities reporting.
- **`docs/complete_python_api.md`** — full fork API in Python.
  This is the first phase where this surface gains content.
- **`docs/complete_pydantic_api.md`** — note OGM works on forked
  sessions; minimal example.
- **`docs/UNI_BLACK_BOOK.md`** — extend forks chapter with
  lifecycle (TTL state machine, sweeper) and watch-source filtering.
- **`API_REVISION.md`** — entries for everything in this phase.
- **`website/docs/features/forks.md`** — full lifecycle section;
  the auditable-counterfactual use case (spec §3.6) becomes a
  great fit here.
- **`website/docs/api/python/`** — fork API.
- **`website/docs/guides/`** — new "Working with forks" guide
  walking through TTL and tagging.
- **`bindings/uni-db/README.md`** — fork section + quickstart.
- **Examples**
  - `bindings/uni-db/examples/fork_quickstart.py`.
  - `crates/uni/examples/fork_audit.rs` (spec §3.6).
  - `crates/uni/examples/fork_external_sandbox.rs` and
    `bindings/uni-db/examples/fork_external_sandbox.py` (spec §3.7).
- **Doctests** in both Rust and Python.
- **`skills/uni-db/`** — Python fork knowledge file. Update the
  uni-db skill description to reflect Python coverage.
- **`AGENTS.md`** — note the sweeper task and how to suppress it
  in tests (`SessionConfig::disable_sweeper`).

**Exit criteria**

- Lifecycle features green in Rust and Python.
- Budget and TTL respected under stress.
- Python tests green under `poetry run pytest -n auto`.
- All documentation surfaces above updated.

---

## Phase 5 — Index fusion (2 weeks)

**Goal:** parent-inherited indexes + fork-local incremental indexes
with two-phase fusion (spec §8).

**Scope**

- Inherited path: parent indexes for parent-sourced data.
- Fork-local indexes built when fork-local fragment count exceeds
  threshold (default 10k rows, configurable).
- Planner emits `FusedIndexScan` when fork scope active and
  fork-local index exists. Per-type fusion: BTree union; ANN
  rerank; BM25 RRF; sorted k-way merge; VID/UID fork-first.

### Testing

- **Rust unit tests**
  - Fusion correctness per index type. Property tests where
    feasible: BTree fusion equivalent to parent-only when
    fork-local index is empty.
  - Build trigger: cross threshold → index built; below → not.
- **Rust integration tests** (`crates/uni/tests/fork_index_*.rs`,
  one file per type)
  - End-to-end: write data on primary, fork, write more data on
    fork, run a query that benefits from the index, assert
    correct result and that the planner picked `FusedIndexScan`
    (use plan capture).
- **Storage-layer tests** — fork-local index file lifecycle:
  built, used, dropped on fork drop.
- **Cypher TCK**
  - Extend existing index TCK scenarios with `Fork_Index_*.feature`
    variants ensuring no recall regressions.
- **Recall + latency benchmarks** (a new benches set, optional but
  recommended): land in `crates/uni/benches/fork_index.rs`. Spec's
  §8.2 target numbers (~1.2–1.5×) are the bar; record results in
  `compliance_reports/fork_index_<date>.md`.
- **Python tests** — confirm Python query results agree with Rust
  for the same scenario; no separate index API needed.

### Documentation

- **`docs/complete_rust_api.md`** — note that fork-local indexes
  build automatically; expose any tuning knobs.
- **`docs/UNI_BLACK_BOOK.md`** — new chapter "Fork index fusion"
  covering the per-type strategies and the build trigger.
- **`API_REVISION.md`** — config knobs added.
- **`website/docs/features/forks.md`** — section on fork-local
  indexing + the spec §8.2 fusion table for users.
- **`website/docs/internals/query-planning.md`** — `FusedIndexScan`
  operator + when planner emits it.
- **`website/docs/concepts/indexing.md`** — fork-aware paragraph.
- **`compliance_reports/`** — first benchmark report.
- **Examples** — `crates/uni/examples/fork_index_fusion.rs`.
- **`skills/uni-db/`** — concise paragraph: "fork-local indexes
  are automatic; here's the threshold."

**Risk:** this phase is the most likely to overrun. Land trigger
+ scalar BTree first, then ANN, BM25, Sorted, VID/UID in that
order. If deadline pressure forces a cut, the **MVP bar is all
five types** (spec §8.2) — but Sorted and VID/UID are simpler
fusions (k-way merge, fork-first fallback) and worth landing even
under pressure. Document any deferral explicitly in
`docs/proposals/graph_fork_phase5_followups.md` rather than
silently shrinking exit criteria.

**Exit criteria**

- All five fusion types from spec §8.2 implemented (BTree, ANN,
  BM25, Sorted, VID/UID), each within stated recall/latency
  bounds. Any deferral documented and tracked.
- TCK + Python tests green.
- All documentation surfaces above updated.

---

## Phase 6 — Diff & Promotion (1.5 weeks)

**Goal:** `db.diff_*` and `db.promote_from_fork`.

**Scope**

- `db.diff_forks(a, b)` and `db.diff_fork_primary(a)`: structural
  diff (vertex/edge add+delete + property changes), bounded by
  smaller fork.
- `db.promote_from_fork(name, &[PromotePattern])`: scan fork via
  fork session, primary-targeted tx, insert matches, commit. UID
  dedup. Edge handling on vertex promotion is deferred per §16;
  document the current "silently skip touching edges with a
  warning" behavior explicitly.

### Testing

- **Rust unit tests**
  - `PromotePattern` matching (subset of Cypher pattern semantics
    we support).
  - Diff symmetry: `diff(a,b) == invert(diff(b,a))`.
  - Diff idempotence: `diff(a,a)` is empty.
- **Rust integration tests**
  - Spec §3.3 end-to-end: write-audit-publish.
  - Spec §3.4: side-by-side scenario diff.
  - Promote with UID conflict: dedup behavior matches doc.
  - Promote vertex pattern with dangling edges: confirm warning
    fires + edges skipped.
- **Cypher TCK** — `Fork_Promote.feature`, `Fork_Diff.feature`.
- **Python tests** — promote and diff round-trip from Python.
- **Stress** — diff on a 1M-vertex fork completes within bound.

### Documentation

- **`docs/complete_rust_api.md`** — diff + promote APIs;
  `PromotePattern` builder.
- **`docs/complete_python_api.md`** — Python equivalents.
- **`docs/UNI_BLACK_BOOK.md`** — chapter "Promotion and diff"
  with the algorithm + bounds.
- **`API_REVISION.md`** — entries.
- **`website/docs/features/forks.md`** — promotion + diff
  sections, including a worked example.
- **`website/docs/use-cases/`** — write-audit-publish guide.
- **Examples**
  - `crates/uni/examples/fork_promote.rs` (spec §3.3).
  - `bindings/uni-db/examples/fork_promote.py`.
  - `crates/uni/examples/fork_rule_developer.rs` (spec §3.1) —
    full loop: fork → tx → query → diff → promote → drop. This
    is the spec's first-class advertisement; it deserves a
    standalone example, not just coverage by parts.
- **`skills/uni-db/`** — promotion + diff notes.

**Exit criteria**

- Spec §3.3 and §3.4 examples run end-to-end (Rust + Python).
- All documentation surfaces above updated.

---

## Phase 7 — Polish & doc completion (1 week)

**Goal:** finish the long tail. By this phase, every feature exists
and most docs do. This phase is the audit pass.

**Scope**

- Run spec §15 spikes that didn't already land:
  - 15.1 multi-dataset 2PC (already in Phase 2 — re-run with the
    final code).
  - 15.2 schema evolution × forks: document open-behavior of
    older-schema forks.
- Final ergonomic sweep: Rust doc comments, Python type stubs,
  error message audit.

### Testing

- **End-to-end "all use cases pass" suite**
  (`crates/uni/tests/fork_use_cases.rs`): one test per spec §3
  scenario (3.1 through 3.8). Some already exist from earlier
  phases — collect them.
- **Schema evolution × forks** test
  (`crates/uni/tests/fork_schema_evolution.rs`): create v1 fork,
  evolve primary to v2, confirm v1 fork still readable + behavior
  matches what the doc claims.
- **Negative tests audit**: every `ForkError` variant has a
  test that triggers it.
- **Docs CI**: `cargo test --doc` green for all fork doctests;
  Python doctests green via `pytest --doctest-modules`.
- **Examples CI**: every `fork_*.rs` and `fork_*.py` example
  builds in CI.
- **TCK final pass**: full Cypher + Locy TCK with no regressions.
- **Stress soak**: a multi-hour run mixing fork create / write /
  drop / cascade; no leaks, no growth in registry on disk.

### Documentation

- **`docs/complete_rust_api.md`** — final pass; cross-link to
  feature page and Black Book chapters.
- **`docs/complete_python_api.md`** — same.
- **`docs/complete_pydantic_api.md`** — final OGM ↔ fork notes.
- **`docs/complete_locy.md`** — explicitly note "no FORK syntax;
  forks are a Session concept" (spec §14, non-goal 6) so Locy
  users don't ask for it. Cross-link to the fork feature page.
- **`docs/UNI_BLACK_BOOK.md`** — finalize fork chapters; add a
  "schema evolution × forks" section reflecting the §15.2 spike.
- **`API_REVISION.md`** — closing entry summarizing the final
  fork surface; link to migration notes.
- **`website/docs/features/forks.md`** — close out: all use
  cases, FAQ section, comparison matrix from spec §17.
- **`website/docs/why-uni.md`** — add forks to the value-prop
  list with a one-liner.
- **`website/docs/index.md`** — link to the feature page from
  the landing.
- **`README.md`** — final fork mention with a link.
- **`AGENTS.md`** — final invariants checklist for agents
  working in this area.
- **`skills/uni-db/`** + **`skills/uni-db-workspace/`** — final
  consolidated fork knowledge files; bump the skill description
  to mention forks so the trigger fires reliably.
- **Migration note** in `API_REVISION.md`: existing snapshots and
  named-snapshots are not forks; spell out the difference.

**Exit criteria**

- All spec §3 use cases run as end-to-end tests, in both Rust
  and Python where applicable.
- Schema-evolution × forks behavior documented and tested.
- All doctests and examples build in CI.
- Stress soak passes overnight.
- Every documentation surface in §B above either updated this
  phase or earlier in the project — verified via a checklist
  PR comment.

---

## Out of scope (deferred per spec §14, §16)

- Locy `FORK { ... }` syntax — explicitly excluded.
- Hypothesis persistence (ASSUME snapshots) — separate spec.
- Deep-chain materialization / fork-scoped reindex — deferred.
- Cross-fork joins, distributed forks, fork-level permissions.
- PROMOTE referential integrity policy.
- Cross-branch watch.

For each deferred item, the **plan still owns the doc surface**:
non-goals are mentioned in `website/docs/features/forks.md` and the
Black Book so users learn the boundary from the docs rather than
from a confused error.

---

## Sequencing summary

```
Phase 0 (spike)      ──┐
                       ▼
Phase 1 (read-only)  ──┐
                       ▼
Phase 2 (writes)    ───┐
                       ├──▶ Phase 3 (nested) ──┐
                       │                       ├──▶ Phase 5 (indexes)
                       │                       └──▶ Phase 6 (diff/promote)
                       └──▶ Phase 4 (lifecycle, admin, Python) ──▶ Phase 7 (polish/docs)
```

Phase 4 can start in parallel with Phase 3 once Phase 2 lands.
Phases 5 and 6 are independent and can land in either order or
in parallel.

**Total estimated effort: 13–15 engineer-weeks** (Phase 4 grew by
a week to absorb Python bindings). Phase 0's outcome is the
largest single source of variance.

---

## Open questions for the design review

See §2.6 above for the resolved-questions table and §2.7 for empirical
questions tracked through the verification suites in §3.
