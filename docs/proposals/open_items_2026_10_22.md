# Open items after PR #318 (2026-10-22)

These are the defects, limits and coverage gaps that were found while fixing
commit-ordered CDC, bulk-load events, single authorization and DDL crash safety
(PR #318, merged as `c56f98f73`) and that the PR left unaddressed.

Each entry says how it is known:
- **Reproduced:** a test or run showed it.
- **Code-read:** it was traced through the source, and no test has shown it.

Line numbers are as of `c56f98f73`.

## Defects

### D1. A commit durable in the WAL whose process crashed before the announcement is never announced

**Status: fixed (uncommitted).** Each WAL segment records the highest announced LSN and the spans of its commits' own mutations. `Drop` and `shutdown` seal the mark. A flush waits for every issued announcement before rotating L0, and the post-commit auto-flush now runs after the publish. At open, `wal_recovery::replay_wal` classifies each unannounced commit against the state before it, and `deliver_recovered_commits` announces it ahead of the recovered loads. Reproduced by `triggers::wal_recovery_announce_resilience` (5 tests). The 3 crash tests and the drop test fail with the mechanisms switched off, and the flush test fails with only the barrier switched off. One finding along the way: a declared trigger's action writes waited in the WAL buffer until the next segment, so dropping the database lost them. The seal now persists them.

- **How known:** code-read.
- **Impact:** it can silently lose events in normal operation.

**Mechanism**
- `commit_tx` is sent from only two places:
  - `Transaction::commit` (`crates/uni/src/api/transaction.rs`, the
    `ticket.publish` after `save_if_dirty`);
  - the bulk event sink (`crates/uni/src/api/bulk_events.rs`).
- After-triggers are dispatched after the announcement.
- If the process dies between the WAL append and the announcement, `replay_wal`
  restores the commit into L0 at the next open, with no `CommitNotification` and
  no trigger fire.

**Consequences**
- CDC never sees the commit, and nothing halts.
- The CDC runtime (`crates/uni-plugin-host/src/cdc_runtime.rs`) persists only
  each provider's acknowledged LSN and never re-derives history.
- It does not check LSN contiguity either: `lsn_start` is the transaction's
  `causal_version`, so ranges overlap by design.
- A resuming provider could only notice the hole by comparing its next batch
  with its checkpoint.

**Contrast with bulk loads:** they no longer have this hole. A bulk marker that
is still `Active`, or `Committed` with `events_pending`, is delivered again at
open (`recover_interrupted_bulk_load` → `bulk_events::deliver_recovered_loads`).

**Fix direction**
- Record durably the highest announced commit version (or WAL LSN), written
  after each announcement and coalesced like the CDC checkpoint writer.
- At open, re-announce the WAL-replayed commits above it, in WAL order, before
  `build` returns. This is the same place the recovered bulk loads are announced.
- Delivery is then at least once. A crash between the announcement and the
  record re-announces the commit, which matches the bulk-load contract.

**Test:** a crash-harness child that aborts at a new seam between the WAL append
and `ticket.publish`. Reopen with a declared trigger and assert that it fires
for the replayed commit.

### D2. A CDC output registered mid-commit is handed an empty batch, then checkpoints past the commit

**Status: fixed (uncommitted).** `CommitNotification::cdc_active` records whether rows were captured. A provider discovered at a notification with `cdc_active == false` starts after that notification, and a stream that was already running halts on one. Reproduced by `cdc_runtime::tests::a_provider_registered_after_a_commit_without_cdc_rows_starts_after_it`: with the check removed, the log shows `checkpoint 5` for the commit that was never received. `a_running_stream_halts_on_a_commit_without_cdc_rows` also fails with the check removed.

- **How known:** code-read. This is the existing "registered mid-commit"
  fallback in `cdc_runtime.rs`.

**Mechanism**
- A commit announced while no CDC output was registered carries no mutation
  batch, because the commit path extracts events only when `cdc_active`.
- If an output registers before the driver reaches that notification, it is
  handed an empty batch with the commit's LSN range.
- It then checkpoints past that commit, whose rows it never received.
- This also applies to loads recovered at open, when a provider registers
  immediately after `build()`.

**Fix direction:** when a newly discovered provider meets a notification
announced before it was registered, start it after that notification rather
than delivering an empty batch for it. Its first checkpoint must then be the
first commit it actually received.

### D3. Lance 11 panics in `filtered_read.rs:639` when a scan's runtime is dropped

**Status: pinned in Lance; read-back guarded (uncommitted).** Kill test result: `uni-store` `lance_scan_polled_after_its_runtime_is_dropped_panics_in_lance` reproduces the panic deterministically (`JoinError::Cancelled` unwrapped at `filtered_read.rs:639`) whenever a scan outlives the runtime that spawned its fragment reads. A uni-level test held the delivery mid-scan and dropped the runtime under it (`triggers::bulk_events_runtime_drop_resilience`). It did not panic, even before the guard. The likely reason, inferred and not measured: the delivery task returns `Pending` at its next I/O await and is never polled again, so a panic needs a fragment read cancelled during the delivery's own in-flight poll, and tokio contains any such panic inside a task it is already cancelling. The guard runs the read-back scan on the IO runtime (`uni_bulk::events::ScanFeed`). `tests::a_scan_feed_survives_the_runtime_that_started_it` fails when the feed runs on the caller's runtime.

- **How known:** observed once, during runtime teardown in a test that was
  failing for another reason. Not reproduced on purpose.

**Mechanism:** Lance `unwrap`s a `JoinError::Cancelled` from its own spawned
read task.

**Why it matters now:** bulk-load event delivery runs on a detached task that is
tracked by the shutdown handle. That task reads the load back with a scan. If
the runtime is dropped without a shutdown while that scan is running, the task
may hit this panic.

**Next step (cheapest kill test)**
- Start a bulk load whose delivery is held mid-scan by a seam.
- Drop the runtime without calling shutdown.
- See whether the panic reaches anything beyond the cancelled task.

If it does, guard the read-back so it never outlives a dropped runtime. A Lance
fix, if one is needed, requires a minimal repro that pins the panic there first.

## Limits (documented, by design for now)

### L1. Events delivered at open reach only consumers present during `build`

- **Who receives them:** built-in triggers, and triggers declared with
  `uni.plugin.declareTrigger`. Those are reactivated by
  `register_builtin_plugins` before recovery runs.
- **Who never does:**
  - CDC outputs;
  - plugins added with `add_plugin` or a `PluginRegistrar`.
- **Why:** `UniBuilder` has no plugin hook, so those consumers can only register
  after `build()` returns.

**Fix direction**
- Add a builder-time registration hook, so CDC outputs and plugin triggers exist
  before open-time delivery.
- Or keep the pending announcements durable and expose an explicit
  `Uni::emit_recovered_events()` for the caller to invoke once registration is
  done.

### L2. Python cannot register a synchronous before-trigger

- Cypher-declared triggers are always synthesized as after-commit and
  asynchronous; `[SYNC]` is rejected when the trigger is declared.
- No Python plugin loader implements `TriggerPlugin`.
- As a result, `UniError::BulkLoadBlockedByTrigger` cannot be raised from
  Python. It has no dedicated Python exception (it maps to the base `UniError`)
  and no Python test of the refusal.
- `skip_before_triggers` is exposed and tested only for chaining and for loading
  successfully.

### L3. Fork commits build CDC batches nobody consumes

- Forks have no CDC runtime (`UniInner::cdc_runtime` is `None` on derived
  inners), which is documented in `website/docs/features/triggers-cdc.md`,
  under "Forks".
- `cdc_active` reads the shared plugin registry, though. So when a CDC output is
  registered, a fork commit still extracts and materializes its mutation batch,
  which only that fork's `Session::watch` could read.
- The docs acknowledge the cost.
- **Fix direction:** gate the extraction on the inner having a CDC runtime or a
  live `watch` subscriber.

## Coverage gaps

### G1. Ignored soaks and probes that do not run to completion anywhere

**Unfinished in a local `--run-ignored only` failpoint run:** these hit the
180 s debug ceiling.
- **DQP soaks:** `plan_cache_agg_soak`, `pinned_agg_soak`, `pinned_soak`,
  `fork_agg_soak`, `fork_soak`, `flush_agg_soak`, `flush_pushdown_soak`,
  `flush_soak`, `flush_wide_soak`, `cert_soak`.
- **Perf probes:** `dqp_feasibility_measure_tiers`,
  `perf_class_probe_best_by_merge`.
- **Issue-55 repro:** `repro_get_edges_scales_with_autoembed`.

**Excluded from the nightly soak job** (`.github/workflows/nightly.yml`),
whose filter is `(test(/soak/) | test(/stress/)) and not test(/dqp::/)`:
- the DQP soaks;
- the probes and the issue-55 repro, because their names match neither pattern.

**Verified the same day:**
- **Passed when run to completion:** the three 1M-row tests
  `w2_residual_memory_growth_observation`, `trigger_vertex_probe_cost_1m` and
  `trigger_edge_probe_cost_1m`, at 355–628 s in debug.
- **Passed under the nightly `soak` profile:** the two stress tests
  `test_stress_create_edges_5k` and `test_stress_merge_10k_ops`.

**Action**
- Decide where each listed test runs: a release-profile nightly lane, or a
  `soak` profile override.
- Any test that should not run anywhere should be deleted, not left ignored.

### G2. The Rust↔Python parity check does not cover the bulk builders

- `scripts/ci/check_rust_python_parity.py` compares builder methods only for
  `QueryBuilder`, `TxQueryBuilder`, `LocyBuilder` and `TxLocyBuilder`.
  `RUST_BUILDER_SRC` lists only `session.rs`, `transaction.rs` and
  `locy_builder.rs`.
- A new option on `BulkWriterBuilder` or `AppenderBuilder` (in `uni-bulk`) can
  therefore be missing from Python without CI noticing. `skip_before_triggers`
  was exactly that until it was bound by hand.
- **Action:** add both builders to `BUILDER_PARITY`, with their Rust sources.

### G3. Local verification missed two checks CI runs

PR #318's first CI run failed on two checks that the local lanes had not run
after the binding change:
- **Static Guards:** `scripts/gen_python_api_reference.py --check` was stale
  after new `.pyi` stubs were added.
- **Fuzz Smoke:** `cargo +nightly metadata --locked` in `fuzz/` was stale after
  `uni-common` gained a dependency.

Both commands are in `docs/local_ci_runbook.md`.

**Action**
- Rerun the whole static-guard step after any change to `bindings/` or the
  stubs.
- Rerun the fuzz lockfile check after any change to dependencies.
