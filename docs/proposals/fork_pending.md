# Fork — Pending Work

**Status as of:** 2026-05-03 (Phase 2 era — **historical**)
**Companion to:** `docs/proposals/graph_fork_plan.md`
**Worktree:** `graph-fork`

> All work described in this document has since shipped across
> phase commits Phase 2 through Phase 7c. This file is preserved
> as a historical artifact of the Phase 2-era planning. For the
> current state see `git log --oneline | grep "feat(fork)"`,
> `AGENTS.md` "Fork system — current invariants", and
> `API_REVISION.md`.

This document captures all fork-feature work that is **not yet merged** as of the
end of the current session, with enough context to resume in a fresh session
without rediscovering the substrate.

Phase 1 (read-only forks) and Phase 2 Days 1-7 (writable forks substrate +
end-to-end demo) are **complete and tested** (1589/1589 uni-store + uni-db tests
green; clippy clean across `uni-common`, `uni-store`, `uni-db`). What follows
are the items still owed.

---

## Phase 2: Days 8-15 (writable forks — hardening + ergonomics)

### Day 8 — Per-fork commit lock + concurrent-writers test

**Goal:** spec §5 says "multiple sessions can hold the same fork concurrently"
with the implication that commits on the same fork serialize while commits on
different forks proceed in parallel.

**Current state:** The fork's `Writer` is wrapped in `Arc<RwLock<Writer>>` on
`UniInner.writer` (Day 4). Different forks already proceed in parallel because
each forked `UniInner` is independent. **What's missing**: two
`Session::fork(name)` calls for the *same* name today produce **different**
`UniInner` instances (each calls `at_fork` afresh), so they have separate
Writers/L0s/WALs and don't share commit state. Reads on session B don't see
session A's pre-flush L0 mutations.

**Required change:** add a UniInner cache so `Session::fork(name)` for the same
name returns sessions backed by the same `Arc<UniInner>`.

**Implementation sketch:**
- Add `fork_inners: Arc<DashMap<ForkId, Weak<UniInner>>>` to `UniInner`
  (initialized on primary's UniInner; `at_fork` clones the `Arc` so forked
  inners share the same map).
- `ForkBuilder::build` consults the map after resolving `ForkInfo` to an
  `Active` state. If a live `Arc<UniInner>` exists for that `fork_id`, reuse
  it (and bump the holder count); otherwise call `at_fork` to construct a new
  one and `insert` it into the map (downgrading to `Weak`).
- `drop_fork` removes the entry from the map.

**Test plan (`crates/uni/tests/fork_concurrent_writers.rs`):**
- Two sessions on the same fork: serial commits — both visible to subsequent
  reads on either session.
- Two sessions on **different** forks: 8 concurrent committing tasks complete
  with near-linear throughput (no global serialization).
- Cross-fork drop while one fork is being written: clean error, not corruption.

**Risk:** the lifetime story (Weak vs Strong, when to GC) is the trickiest
piece. Phase 4's watch/lifecycle work has the same shape, so this pairs well
with that.

---

### Day 9 — Locy rule registry routed through fork

**Goal:** `forked.tx().locy("CREATE RULE r AS ...")` registers the rule on the
fork only; primary's rule registry is unchanged.

**Current state:** Phase 1 already deep-clones `locy_rule_registry` into each
forked `UniInner` (`crates/uni/src/api/mod.rs::at_fork`). However:

- `tx.locy()` today consults the *global* `db.locy_rule_registry`
  (`crates/uni-store/src/runtime/locy_builder.rs:245-253`) — not the session's.
- On commit, new rules in `tx.rule_registry` promote to
  `session.rule_registry` (`crates/uni/src/api/transaction.rs:584-616`) — but
  the promotion target is the session's, not the fork's UniInner's.

**Required change:**
1. `Transaction::new` reads `tx.session_rule_registry` from the session that
   created it. On a forked session that's already the deep-cloned registry —
   no change needed at construction.
2. Modify `locy_builder` so `tx.locy(...)` consults `tx.session_rule_registry`
   instead of `db.locy_rule_registry` when the session is forked. (Add a
   `forked: bool` discriminator or pass the registry by reference.)
3. Promotion-on-commit lands rules on the forked session's registry (already
   correct in Phase 1 for clone-on-fork; verify the path).

**Test plan (`crates/uni/tests/fork_locy_rules.rs`):**
- Fork session creates a rule; primary's registry doesn't have it.
- Primary creates a rule; fork session sees it (because fork inherited at
  fork-point).
- Both create different rules; each sees its own + inherited at fork-point.

**Risk:** `tx.locy()` today consults global; changing the dispatch breaks
primary's existing semantics if not careful. Test that primary `tx.locy()`
still hits global after the change.

---

### Day 10 — On-the-fly schema overlay growth

**Goal:** a fork session can `CREATE (n:NewLabelNotOnPrimary)` and have the
new label exist only on the fork (primary's schema file unchanged byte-for-byte).

**Current state:** `BranchedBackend::create_table`, `create_empty_table`, and
`open_or_create_table` currently bail with "Day 10 work" when the fork has no
branch for the table.

**Required change:**
1. Detect new label/edge-type at flush time (in fork's `Writer::flush_to_l1`).
2. For new labels: `lance_branch::create_dataset_then_branch` (already
   implemented Day 1). The fork's `datasets` map gets the new entry.
3. Update fork's `SchemaDelta` (in-memory `ForkScope.overlay` — likely needs
   to switch from `Arc<SchemaDelta>` to `arc_swap::ArcSwap<SchemaDelta>` for
   hot reload) and PUT the updated overlay to
   `catalog/fork_schemas/{fork_id}.json`.
4. The schema-overlay PUT happens inside the per-fork commit critical section
   (Day 8 lock) so concurrent commits don't race.

**Test plan (`crates/uni/tests/fork_new_label.rs`):**
- Primary has labels {A, B}. Fork creates `(n:C)`. Confirm: (a) primary's
  schema file unchanged byte-for-byte; (b) fork sees A, B, C; (c) restart
  preserves fork's view of C; (d) `db.fork_info` reports the schema delta;
  (e) primary still sees only {A, B}.

**Risk:** schema-overlay PUT timing — must happen before any read can observe
the new label. Easy to land if the commit critical section is honored.

**Open Question 2 from the plan (resolved here):** add `arc-swap` to
`Cargo.toml` workspace deps when this lands.

---

### Day 11 — `drop_fork` widened in-flight check

**Goal:** `drop_fork(name)` while a transaction is in flight on the fork
returns a typed error, not silent corruption.

**Current state:** Phase 1's `drop_fork` errors with `ForkInUse { name,
holder_count }` if any session holds the fork (the holder count is tracked
via `ForkScope::Drop`). It does **not** check for in-flight transactions.

**Required change:**
1. Add `UniError::ForkInflightTx { name }` variant to
   `crates/uni-common/src/api/error.rs`.
2. In `crates/uni-store/src/fork/registry.rs`, extend `begin_drop` (or the
   caller in `crates/uni/src/api/mod.rs::Uni::drop_fork`) to check the fork's
   `Writer.l0_manager.get_current().mutation_count > 0` (or similar
   "uncommitted state" predicate). Refuse if any.
3. Document that callers must explicitly close/commit/rollback before drop.

**Test plan (`crates/uni/tests/fork_drop_inflight.rs`):**
- Open fork, start tx, attempt drop → `ForkInflightTx`.
- Commit/rollback the tx, drop succeeds.
- Sessions held without active tx → still `ForkInUse` (unchanged).

**Risk:** the "in-flight tx" predicate needs to be precise. `mutation_count`
> 0 is a proxy; an open tx that hasn't yet executed any mutation might still
need to be drained. Phase 4 (drain semantics) lands the proper version.

---

### Day 12 — Fragment-count guard rail

**Goal:** make the deferred fork-compaction risk operationally visible. Long-
lived heavy-write forks accumulate L1 fragments; fork reads degrade. Phase 5
adds compaction; Phase 2 adds a metric + log.

**Implementation (~50 LOC):**
- On `flush_to_l1` for a forked Writer, count fragments per Lance dataset on
  the fork's branch (use `Dataset::manifest().fragments.len()`).
- Emit `uni_fork_l1_fragments{fork=<name>, dataset=<name>}` gauge.
- If count > threshold (default 256, configurable via
  `SessionConfig::fork_fragment_warn_threshold`), `tracing::warn!` once per
  threshold crossing.

**Test plan:** unit test that confirms the metric is emitted with correct
labels; threshold-crossing log fires once.

**Risk:** none. Pure observability addition.

---

### Day 13 — TCK regression sweep

**Goal:** confirm Phase 2's substrate changes (especially Day 9's Locy
registry rewiring) didn't break primary semantics.

**Run:** `cargo nextest run -p uni-tck -p uni-locy-tck`. Target zero
regressions.

**Likely failure modes:**
- Day 9's `tx.locy()` registry-routing change breaks a Locy TCK scenario
  where primary's tx.locy expected to see global registry state. Fix in
  `locy_builder.rs` to keep primary path on global, fork path on session.

---

### Day 14 — Stress soak

**Goal:** confirm long-running fork+write workloads don't leak resources or
corrupt state, and recovery works under chaos.

**Test (`crates/uni/tests/fork_writes_soak.rs`, `#[ignore]` for nightly):**
- 100 forks created.
- 1000 mutations per fork interleaved with 1000 primary mutations.
- 30-minute minimum wall time.
- Periodic process kill via `tokio::process::Command::kill` followed by
  reopen + verify; no row count changes; no orphan branches; no orphan
  tombstones.
- Final verification: `db.list_forks()` count matches; each fork's reads
  return the expected mutation count.

**Risk:** memory leak detection requires longer runs (4h+ on real CI).
Document this as a separate `--release` nightly step.

---

### Day 15 — Documentation + doctests + self-review

**Touch list (per Phase 2 plan §Documentation surfaces):**

- `docs/proposals/graph_fork_plan.md` — flip §Phase 2 "option B" → "option A"
  (per-fork L0/WAL); mark §2.4 (`tx.locy()` registry routing fix) resolved.
- `API_REVISION.md` — new "Forks (Phase 2, writable)" section.
- `website/docs/features/forks.md` — flip status from "Phase 1 read-only" to
  "Phase 2 writable"; add write-audit-publish example (spec §3.3).
- `crates/uni/examples/fork_write_audit_publish.rs` — new (spec §3.3).
- `AGENTS.md` — drop Phase-1 "no writes through forks" line; add the new
  invariants:
  - Per-fork `IdAllocator` lives at `catalog/forks/{fork_id}/id_allocator.json`
    and is bootstrapped from primary's HWM at fork creation. Never let it
    start at 0 against a non-empty primary.
  - Per-fork WAL lives at `wal_forks/{fork_id}/` (NOT under `wal/` — that
    prefix collides with primary's listing). See
    `crates/uni-store/src/fork/wal.rs` module rustdoc.
  - Fork compaction is deferred to Phase 5; long-lived heavy-write forks
    should be `drop_fork`-and-recreate until then.
  - Per-fork commit lock: writes on the same fork serialize through the
    Writer's `Arc<RwLock<...>>`; cross-fork writes proceed in parallel.
- Doctests on every newly-public path:
  - `Session::tx()` (no longer gates on forked sessions — update the
    `///` example).
  - `IdAllocator::current_hwm`, `IdAllocator::checkpoint`.
  - `bootstrap_fork_from_primary_hwm`.
- `skills/uni-db/` — fork knowledge file: writable.

---

## Phase 2 design decisions taken during Days 1-7 (record for future you)

These resolve open questions or differ from the Phase 2 plan; documenting so
they're not relitigated.

### D1.a: WAL prefix is `wal_forks/{fork_id}/`, not `wal/forks/{fork_id}/`

The plan called for `wal/forks/{fork_id}/`. That collides with primary's `wal/`
listing because `ObjectStore::list` is recursive — primary recovery would catch
fork WAL segments. Moved to top-level `wal_forks/`. Documented in
`crates/uni-store/src/fork/wal.rs` module rustdoc.

### D2.a: Fork IdAllocator is HWM-bootstrapped from primary's in-memory state

The plan implied a file-copy bootstrap (`catalog/forks/{fork_id}/id_allocator.json`
copied from primary's `id_allocator.json`). That fails because primary's
allocator file lives on a different `ObjectStore` than the fork's path
resolves through (`<root>/id_allocator.json` vs
`<root>/storage/catalog/forks/{id}/id_allocator.json` — different store roots).

Resolution: read primary's HWM from in-memory `IdAllocator::current_hwm()`
(new public method), build a fresh `CounterManifest` in memory, write it to
the fork's path. See `crates/uni-store/src/fork/id_alloc.rs`.

### D3.a: Same-fork-multi-session shared state is deferred

Phase 2 plan Day 8 implied per-fork commit lock with shared state across
sessions. Achieving "shared state" requires a UniInner cache keyed by
`ForkId`, which is a substantive refactor. Deferred — Phase 4's watch /
lifecycle work has the same UniInner-lifetime shape and is the natural place
to land this.

For now: cross-fork commits proceed in parallel ✓; same-fork-multi-session
commits each have their own L0 (Lance MVCC handles disk-level conflicts but
session-level isolation is per-session). Document this limit.

---

## Followups outside the original Phase 2 day-by-day

These surfaced during review/implementation and aren't on the day-by-day plan
but should land before declaring Phase 2 shipping-complete:

### F1: `BranchedBackend::open_or_create_table` for known datasets

Currently bails with "Day 10 work" when the fork has no branch for the
table. The Day 10 implementation should distinguish:
- **Known label**, branch missing → on-the-fly branch creation (Day 10).
- **Unknown label** in fork session → fork-local schema overlay growth
  (Day 10).
- **Unknown label** in primary session → existing primary path (no change).

### F2: Drop the `eprintln!` debug noise from `bootstrap_fork_from_primary_hwm`

Already done in the final version; flagging in case a partial revert restores
the eprintlns.

### F3: Add `IdAllocator::checkpoint()` doctest

Public method added Day 7 but no doctest yet. One-liner showing
`writer.allocator.checkpoint().await?`.

### F4: Re-enable Day 7 lint expectation cleanup

The Phase 1 `#[cfg_attr(not(test), expect(dead_code))]` markers for
`name_lock` etc. should be re-audited after Phase 2 wires more callers; some
may now be live in lib builds and the `expect` lint becomes "unfulfilled."

### F5: Fork session metrics

`Session::metrics()` exists but its semantics on a forked session aren't
defined. Phase 4 covers per-session metrics scoping; flag for that.

---

## Test posture at end of Phase 2 Days 1-7

- **Total tests passing:** 1589 (uni-store + uni-db). 25 skipped (pre-existing
  `#[ignore]`d nightly tests).
- **New fork-specific tests added in Phase 2:** 33.
- **Clippy:** clean across `uni-common`, `uni-store`, `uni-db` with
  `--all-targets -- -D warnings`.
- **Doctests:** unchanged from end of Phase 1 (no Phase 2 doctests added
  yet — that's Day 15).
- **TCK:** not yet re-run for Phase 2 (Day 13 work).

---

## Files added in Phase 2 (canonical list)

```
crates/uni-store/src/fork/id_alloc.rs            (new — Day 3)
crates/uni-store/src/fork/wal.rs                 (new — Day 5)
crates/uni-store/src/fork/writer_factory.rs      (new — Day 4)
crates/uni-store/tests/fork_branch_writes.rs     (new — Day 1)
crates/uni-store/tests/branched_backend_writes.rs (new — Day 2)
crates/uni-store/tests/recovery_fork_wal.rs      (new — Day 6)
crates/uni/tests/fork_writes.rs                  (new — Day 7)
```

## Files modified in Phase 2

```
crates/uni-store/src/backend/lance_branch.rs     (added 4 write helpers — Day 1)
crates/uni-store/src/backend/branched.rs         (write methods, accessors — Day 2)
crates/uni-store/src/backend/mod.rs              (re-export branched module)
crates/uni-store/src/runtime/id_allocator.rs     (current_hwm, checkpoint — Day 7)
crates/uni-store/src/fork/mod.rs                 (3 new submodules — Days 3/4/5)
crates/uni/src/api/mod.rs                        (at_fork builds Writer + WAL replay — Days 4/6)
crates/uni/src/api/session.rs                    (tx() ungate, doc updates — Day 7)
crates/uni/src/api/fork.rs                       (HWM bootstrap during create — Day 7)
crates/uni/tests/fork_read_only.rs               (Phase-1 negative test → Phase-2 positive — Day 7)
```

---

## Quick resume commands

```sh
# From the worktree root
cd /home/rohit/work/dragonscale/uni/.claude/worktrees/graph-fork

# Verify current state
cargo clippy -p uni-common -p uni-store -p uni-db --all-targets -- -D warnings
cargo nextest run -p uni-store -p uni-db

# Run only fork tests
cargo nextest run -p uni-db --test fork_read_only --test fork_writes \
    --test fork_creation_concurrency --test fork_no_primary_blocking
cargo nextest run -p uni-store --test fork_branch_writes \
    --test branched_backend_writes --test recovery_fork_wal \
    --test recovery_fork_create --test recovery_fork_drop \
    --test storage_at_fork --test fork_branch \
    --test recovery_fork_create_fault --test lance_branch_retention

# Resume Day 8 work (per-fork commit lock + UniInner cache)
# Read /home/rohit/.claude/plans/peppy-popping-summit.md §Day 8
# Read this file's "Day 8" section above for design notes
```
