# Correctness Scan — Deferred Wave 0 Findings

**Date:** 2026-07-06
**Context:** Wave 0 of `docs/correctness_scan_2026-07-05.md` (see the triage in
`docs/correctness_scan_triage_2026-07-05.md`). 18 of 22 Wave-0 findings were fixed and
committed to `main` (16 commits, `6ec2c9963..b659ca0fa`, FF-merged, **not pushed**). This
doc captures the deferred findings with enough analysis to resume without re-investigating.

**Update (2026-07-06):** the two data-correctness deferrals — **D2** (`uni[6]`) and **D4**
(`uni-query[29]`) — have since been fixed on branch `fix/correctness-deferred-d2-d4` (D4
`c393dfb87`, D2 `c9e80cff2`). **2 findings remain deferred: D1 (`uni[2]`, perf) and D3
(`uni-fork[3]`, blocked on a UidIndex design decision).** The D2/D4 sections below are kept
for the record and marked **DONE**.

Each entry gives: the exact code locations, the root cause, what was tried (if anything),
**why it was deferred**, the **concrete fix plan**, the **existing repro** to flip/un-ignore,
and the **verification** commands. Repro files already exist in-tree (untracked) unless noted.

Shared build/test conventions (from the fixed work):
- Rust: `RUSTC_WRAPPER="" cargo nextest run -p <crate> ...` (nextest, not `cargo test`).
- uni-store repros are gated behind the default `lance-backend` feature and aggregated via
  `crates/uni-store/tests/common/bugs/mod.rs` → `integration.rs`.
- uni-db (crate `uni-python` / test crate `uni-db`) integration tests live under
  `crates/uni/tests/common/bugs/` aggregated via `crates/uni/tests/integration.rs`.
- A repro is only "done" when its assertion is flipped from capturing the bug to asserting
  the fix, and the full crate suite stays green.

---

## D1 — `uni[2]` fork-local index kind collision (perf; NOT data-loss)

**Severity:** perf/planner defect — retrieval stays correct (falls back to a plain scan);
only the fused-index-scan *plan* is missed. This is why it was safe to defer.

### Locations
- **Storage (root):** `crates/uni-store/src/fork/scope.rs:125`
  `fork_local_indexes: Arc<DashMap<(String, String), ForkLocalIndexKind>>` — one kind per
  `(label, column)`. `register_fork_local_index` (`scope.rs:205`) does `.insert(...)`, which
  **overwrites**; `fork_local_index` accessor (`scope.rs:215`) returns `Option<one kind>`;
  `all_fork_local_indexes` (`scope.rs:223`).
- `ForkLocalIndexKind` enum: `scope.rs:46` — `#[derive(Clone, Copy, Debug, Eq, PartialEq,
  Hash)]`, variants `ScalarBtree, Sorted, VidUid, Vector, FullText, Sparse`, `#[non_exhaustive]`.
  (Already `Eq + Hash` → usable in a `HashSet` with no derive change.)
- **Maintenance skip check:** `crates/uni/src/api/fork_maintenance.rs:194`
  `if scope.fork_local_index(label, column) == Some(*kind) { continue; }` — the comment at
  `:178` already acknowledges a column can carry multiple kinds, but the map can't represent it.
- **Storage accessor:** `crates/uni-store/src/storage/manager.rs:607` `fork_index_exists`
  → `s.fork_local_index(label, column)` (`:614`).
- **Planner consumers** (`crates/uni-query/src/query/planner.rs`), all read "the one stored
  kind" then map via `into_fusion_kind` (`:10438`, `kind → FusionKind`):
  - `:10185` equality-scan fusion (`fork_index_for(&labels[0], &col)` → `into_fusion_kind`) —
    **the site the repro exercises** (`{email: 'x'}` equality → `BtreeUnion`).
  - `:10248` `VectorKnn` (wants `Vector` → `AnnRerank`).
  - `:10279` `InvertedIndexLookup` (wants `FullText` → `Bm25Rrf`).
  - `:10342` Sorted / ORDER-BY (`Some(ForkLocalIndexKind::Sorted)` → `SortedKWayMerge`).
  - `:10428` `procedure_call_fusion_kind` — `let registered = fork_index_for(...)?;` compares
    `registered == expected`.
  - Helper defs: `fork_index_for` / `fork_index_for_label_id` at `:9891`/`:9899` both delegate
    to `fork_index_exists`.

### Root cause
The map holds a single kind per column, so building a second kind (e.g. `FullText` on a column
that already has a `ScalarBtree`) **clobbers** the first. Each planner site then reads whatever
one kind survived and blindly maps it with `into_fusion_kind` — so the fusion for the *other*
kind is lost, and (worse in principle) a site can wrap a scan with a fusion kind that doesn't
match the scan shape.

### Why deferred
The correct fix isn't just "store a set" — each planner read-site must ask for the **specific
kind that its scan shape can fuse** (equality scan → `VidUid`/`ScalarBtree`; `VectorKnn` →
`Vector`; `InvertedIndexLookup` → `FullText`; ORDER BY → `Sorted`). The current code is loose
(maps "the one kind" generically), so naively returning a set and picking "any" risks a
**subtle mis-fusion** — exactly the kind of planner regression the scan is trying to prevent.
Needs careful per-site reasoning + plan-shape tests.

### Fix plan
1. `scope.rs`: change the value type to `HashSet<ForkLocalIndexKind>` (enum is already
   `Eq + Hash`). `register_fork_local_index` → `entry(key).or_default().insert(kind)`. Update
   `all_fork_local_indexes` to flatten. Add `has_fork_local_index(label, column, kind) -> bool`.
2. `manager.rs`: add `fork_index_has_kind(label, column, kind) -> bool` (delegates to scope);
   keep or drop `fork_index_exists` depending on remaining callers.
3. `fork_maintenance.rs:194`: `if scope.has_fork_local_index(label, column, *kind) { continue; }`.
4. **Planner** — make each site kind-specific:
   - `:10185` equality scan: check for `VidUid` first, else `ScalarBtree` (mirror the current
     precedence implied by `into_fusion_kind`), producing `VidUidForkFirst` / `BtreeUnion`.
   - `:10248` `VectorKnn`: `has_kind(..., Vector)` → `AnnRerank`.
   - `:10279` `InvertedIndexLookup`: `has_kind(..., FullText)` → `Bm25Rrf`.
   - `:10342` Sorted: `has_kind(..., Sorted)` → `SortedKWayMerge`.
   - `:10428` `procedure_call_fusion_kind`: `has_kind(..., expected)` instead of
     `registered == expected`.
   Keep `into_fusion_kind` as the kind→FusionKind mapper for whichever kind each site selects.

### Repro (flip)
`crates/uni/tests/common/bugs/repro_fork_index_kind_collision.rs` →
`two_fork_index_kinds_on_one_column_cannot_coexist` (currently asserts `!plan.contains("BtreeUnion")`
after building `FullText` over an existing `ScalarBtree`). **After fix:** the ScalarBtree
equality fusion must survive alongside FullText → assert the plan **still** contains
`FusedIndexScan` + `BtreeUnion`. Register in `crates/uni/tests/common/bugs/mod.rs`.

### Verify
`RUSTC_WRAPPER="" cargo nextest run -p uni-db -E 'test(two_fork_index_kinds_on_one_column_cannot_coexist)'`
then full `-p uni-query` and `-p uni-db` suites (planner regressions surface there).

---

## D2 — `uni[6]` nested-fork tip capture race (fork-creation concurrency) — ✅ DONE (`c9e80cff2`)

**Severity:** snapshot-isolation violation at fork creation — a nested fork can branch off a
**post-fork-point** version of its parent. Distinct from issue #103 (that was a read-path
scalar-index bug on an existing fork; confirmed different code path/failure mode).

### Locations
- `crates/uni/src/api/fork.rs:449-471` — `build_datasets_for_fork`, nested-fork arm. Line
  **452**: `current_version_on_branch(&dataset_uri, &parent_branch)` reads the parent **branch**
  tip **live**, then `create_branch_from(..., parent_v)`.
- `crates/uni-store/src/runtime/writer.rs:4153-4190` — `flush_and_capture_fork_point`. The
  `flush_lock` is acquired at `:4160` and dropped when this fn returns. It captures
  `current_version(&uri)` per dataset at `:4180` — that is **main's** tip, NOT the parent
  branch's tip.
- Primary-parent arm (contrast, already correct): `fork.rs:491-513` branches at
  `captured_versions.get(&dataset_name)` (captured under the lock; the M1 fix). The live
  re-read at `:497-506` is a should-never-fire fallback.

### Root cause
The nested arm has **no captured value to use**, because `flush_and_capture_fork_point` only
snapshots main's `current_version`, never the parent *branch's* tip (branch tips advance
independently of main — see `lance_branch.rs:103-116`). So the nested path is forced to read the
parent-branch tip **live and unserialized, after `flush_lock` dropped**. A concurrent commit+flush
on the parent fork between capture and line 452 advances the parent branch tip; the child then
`create_branch_from(..., parent_v)` at that advanced version → sees parent writes committed after
the fork point.

### Why deferred
Not a one-line "move the read inside the lock" — the value the nested arm needs (the parent
*branch* tip) is **never captured under the lock today**. The fix extends the capture path and
threads a new per-dataset map into the nested arm — a change to the fork-creation concurrency
path, which is race-sensitive and hard to test deterministically (see repro note).

### Fix plan
Capture the parent-branch tip **under `flush_lock`** and thread it into the nested arm:
- Option A (preferred): extend `flush_and_capture_fork_point` (or the fork setup at
  `fork.rs:271-279`) to also record `current_version_on_branch(uri, parent_branch)` per dataset
  while the guard is held (requires the parent branch names at capture time), then have the
  nested arm branch at that captured version — mirroring the primary path's capture-under-lock.
- Option B: hold/re-acquire `flush_lock` across `build_datasets_for_fork` for the nested case so
  the `:452` read is serialized against parent commit/flush.

### Repro
`crates/uni/tests/common/bugs/repro_nested_fork_capture_race.rs` →
`nested_fork_reads_parent_live_tip_under_concurrency` (`#[ignore]`, non-deterministic stress; its
module doc names `fork.rs:452`). Deterministic firing needs an injected suspension point between
capture and branch in production code. Aim: make it a deterministic guarding regression (test-only
await/hook between capture and branch, or assert the captured-tip path is taken); if full
determinism is impractical, keep the stress loop but assert **no live read** occurs (captured
version used).

### Verify
`RUSTC_WRAPPER="" cargo nextest run -p uni-db -E 'test(nested_fork)'` plus the fork suites
(`test(fork)`); run repeatedly under load if the test stays stress-based.

---

## D3 — `uni-fork[3]` promote content-UID mismatch (applied → REVERTED)

**Severity:** non-idempotent promote (unbounded twins on re-promote for ext_id-bearing rows).
**Status:** a fix was committed then **reverted** — commit `b659ca0fa` — because it exposed a
deeper issue. The current tree keeps the *original* (mismatching) behavior with a NOTE comment.

### Locations
- `crates/uni-fork/src/diff.rs:643` — `run_promote` recomputes the content-UID from
  `node.properties` (a Cypher query result, which **strips** the `ext_id` key).
- `crates/uni-store/src/runtime/writer.rs:5182` — the **registered** UID is hashed from the
  **stored** props, which **still contain** the `ext_id` key.
- `crates/uni-store/src/storage/vertex.rs:47` — `compute_vertex_uid(label, ext_id, properties)`
  folds `ext_id` in **twice**: once as the dedicated arg AND once via every property key
  (including `ext_id`). Changing this hasher would invalidate every persisted `UidIndex` entry
  (rejected).
- Consuming path: `diff.rs:660-685` — ext_id-bearing candidates resolve via
  `batch_resolve_primary_by_ext_id` (ext-id keyed); non-resolved ones fall to the content-UID
  `uid_candidates` path resolved against the primary `UidIndex`.
- **Conflicting contract / regression:** `crates/uni/tests/common/fork/fork_promote.rs:316` —
  `promote_default_is_insert_only_twin`. It creates Alice (ext_id `p1`, age 30), forks, then the
  **primary** edits age→99, then `promote_from_fork` under default (insert-only) options; asserts
  `vertices_inserted == 1` and primary ends with `ages == [30, 99]` (twin + edited).

### Root cause & what was tried
The tried fix re-injected the `ext_id` key into `node.properties` before `compute_vertex_uid`, so
the promote-side UID matched the registered UID and the dedup could fire. **Correct in isolation.**
But it regressed `promote_default_is_insert_only_twin`: the fork's Alice (age 30) then matched a
**stale** age-30 UID in the primary's `UidIndex` (the primary was edited to age 99, but the index
apparently still carried the pre-edit UID), so the promote wrongly **skipped** the insert instead
of twinning the divergent fork vertex.

So there are really **two coupled issues**:
1. The promote-side vs registered content-UID mismatch (the finding), and
2. **`UidIndex` staleness on property update** — the index does not appear to drop/replace a
   vertex's content-UID when its properties change (age 30 → 99), so a content-UID dedup can match
   a version of the vertex that no longer exists.

Fixing only (1) surfaces (2) and breaks a deliberate contract.

### Why deferred
The finding cannot be fixed safely until the `UidIndex`-on-update semantics are decided:
- If content-UID dedup is meant to reflect **current** content, then `UidIndex` must be updated
  (remove old UID, add new) whenever properties change — then re-applying the `ext_id` re-injection
  is correct and the insert-only-twin test still passes (fork age-30 ≠ primary age-99 → no dedup →
  twin).
- If default promote is meant to be strictly insert-only (twin regardless), then the finding's
  "dedup should fire" premise is wrong for the default path and only applies to the
  no-content-change re-promote case (idempotency), which needs a narrower guard.

### Fix plan (pick one, after deciding semantics)
- **Preferred:** (a) make the writer's UID registration update-aware (drop the old content-UID and
  register the new one on property update), then (b) re-apply the `ext_id` re-injection at
  `diff.rs:643`. Verify `promote_default_is_insert_only_twin` still twins (fork age-30 ≠ primary
  age-99) AND the no-change re-promote is idempotent (no twin).
- **Narrower alternative:** leave `diff.rs:643` as-is and add an idempotency guard keyed on the
  ext_id + fork-point baseline so a *no-content-change* re-promote skips, without touching the
  content-UID path (avoids the staleness interaction).

### Repro
`crates/uni-fork/tests/promote_diff_bugs.rs` →
`finding2_promote_uid_can_never_match_registered_uid` (currently `assert_ne!`, pinning the
mismatch). The reverted version asserted `assert_eq!` after re-injection. Add an **end-to-end**
idempotency test (CREATE ext_id vertex, flush, fork with NO changes, promote twice, assert exactly
one primary vertex) and keep `promote_default_is_insert_only_twin` green.

### Verify
`RUSTC_WRAPPER="" cargo nextest run -p uni-fork --test promote_diff_bugs` **and**
`cargo nextest run -p uni-db -E 'test(promote_default_is_insert_only_twin)'` (the regression
canary) plus the full `-p uni-db` fork suite.

---

## D4 — `uni-query[29]` L0 label overlay union-only — ✅ DONE (`c393dfb87`)

**Severity:** wrong query results — a `REMOVE n:Label` on a flushed vertex is invisible; the
removed label resurrects in `labels(n)` and in `MATCH (n:RemovedLabel)`.

### Locations
- `crates/uni-query/src/query/df_graph/scan.rs:2803-2813` — `map_to_output_schema` label-column
  build (schemaless `RETURN n` path). Union-only overlay; never consults
  `vertex_label_overwrites`.
- `crates/uni-query/src/query/df_graph/scan.rs:1817-1827` — `build_labels_column_for_known_label`
  (known-label scan). Same union-only overlay, plus a defensive "ensure scanned label present"
  push at `:1812-1815`.
- Buffer iteration order: `crates/uni-query/src/query/df_graph/mod.rs:269` `iter_l0_buffers` yields
  **oldest → newest** (pending-flush oldest first, then current, then transaction L0).
- Marker semantics (reference): `crates/uni-store/src/runtime/l0.rs:372` live `set_vertex_labels`
  sets `vertex_label_overwrites`; the M8 flush pass at `l0.rs:1408` treats the marker as REPLACE.

### What was tried (and validated — then reverted)
A label-COLUMN fix at both sites: walk buffers oldest→newest; if a buffer has the vid in
`vertex_label_overwrites`, its `vertex_labels[vid]` is the resolved full set and **REPLACES** the
stored labels (newest overwrite wins); otherwise union additive labels. This **worked** — after
`CREATE (n:A:B)`, flush, `REMOVE n:B`, `labels(n)` correctly returned `['A']`.

**But** `MATCH (n:B)` still returned the node. Reverted to avoid a confusing partial state
(`labels(n)` says `[A]` while `MATCH (n:B)` still matches).

### Root cause of the remaining half
`MATCH (n:B)` resolves its **candidate set** through the label-scan / label-index path, **not**
through the label-column builder that was fixed. After `REMOVE n:B` the L0 reverse index
(`label_to_vids`) no longer lists the vid under `B`, but the **flushed Lance row** still carries
`B`, so the label-B scan re-includes the vid. The scan must consult the L0 overwrite marker to
**exclude** a vid whose current (overwrite-resolved) label set no longer contains the scanned
label.

### Why deferred
The candidate-filtering change lives in the schemaless label-scan candidate resolution (core scan
path), separate from the two label-column sites. Needs to find where label-B candidates are
gathered from Lance + L0 and apply an L0-overwrite exclusion — a riskier, less-localized change
than the label-column overlay.

### Fix plan
1. Re-apply the **label-column** overlay fix at `scan.rs:2803` and `:1817` (REPLACE on
   `vertex_label_overwrites`, honoring newest-buffer-wins; also drop labels for tombstoned vids).
   This was validated to fix `labels(n)`.
2. Add **candidate filtering** in the label-scan path: when gathering candidates for a known label
   `L`, for any vid whose newest L0 buffer flags it in `vertex_label_overwrites`, include it only
   if that buffer's resolved `vertex_labels[vid]` contains `L` (and exclude tombstoned vids). Find
   the schemaless known-label candidate resolution in `scan.rs` (the path feeding `MATCH (n:L)`)
   and apply there.

### Repro (un-ignore)
`crates/uni-query/tests/correctness_repros.rs` → `repro_10_label_remove_resurrect`. Currently
`#[ignore = "uni-query[29]: label-scan candidate filtering for REMOVE label pending"]` with the
**target assertions already written**: `labels(n) == ['A']`, `A` present, and `MATCH (n:B)` empty.
Remove the `#[ignore]` when both halves land.

### Verify
`RUSTC_WRAPPER="" cargo nextest run -p uni-query --test correctness_repros -E 'test(repro_10_label_remove_resurrect)'`
then full `-p uni-query` (scan-path regressions) and a spot-check of the schemaless MATCH/label
tests.

---

## Quick resume checklist
- [x] **D4** (`uni-query[29]`) — DONE `c393dfb87` (label-column replace + candidate-set
      overwrite filter; repro un-ignored).
- [x] **D2** (`uni[6]`) — DONE `c9e80cff2` (parent-branch tip captured under `flush_lock`;
      deterministic failpoint repro, negative-control proven).
- [ ] **D3** (`uni-fork[3]`) — decide `UidIndex`-on-update semantics first; canary test =
      `promote_default_is_insert_only_twin`.
- [ ] **D1** (`uni[2]`) — perf-only; do last unless a fork-index plan-quality issue surfaces.

D3/D1 remain, each with an in-tree repro pinning current behavior. Branch
`fix/correctness-scan-wave0` is FF-merged into local `main`; `fix/correctness-deferred-d2-d4`
(D4 + D2) is not yet merged. Nothing pushed.
