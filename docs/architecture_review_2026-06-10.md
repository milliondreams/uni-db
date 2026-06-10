# Uni-DB Architecture Review — 2026-06-10

**Scope:** full workspace at v2.0.6 (27 crates under `crates/`, 7 binding crates under `bindings/`, ~290k LOC src; `uni-query` alone 88.8k).
**Method:** ten parallel subsystem reviews (storage, query engine, Locy, transactions/SSI, plugin framework, API/bindings, auxiliary crates, cross-cutting concerns, testing/CI, documentation, crate-dependency graph), synthesized here. File:line references were verified by the reviewers at the time of writing.

---

## 1. Overall verdict

The architecture is fundamentally sound and unusually deliberate for a system this size. The big design bets are coherent and mostly well-executed:

- **L0-over-Lance hybrid storage** with a WAL-anchored commit protocol and a clean `StorageBackend` trait.
- **DataFusion-hosted execution** with ~30 custom graph operators and mutations modeled as operators (unified `MutationExec`), not a separate engine.
- **Runtime-config SSI** (no cargo features) with a single commit chokepoint, `Option`-gated read-set recording, and a principled CRDT carve-out.
- **A capability-based plugin kernel** that genuinely unifies five runtimes (native, WASM, Extism, Rhai, PyO3) behind one registrar/registry, with three-layer capability enforcement.
- **A one-source-tree 6-wheel matrix** (provider × linking × acceleration) where the five variant crates are pure Cargo.toml permutations.

The debt clusters into four themes:

1. **A handful of likely-real correctness bugs** found during this review (§2).
2. **Dual-engine drift** — legacy row interpreters coexisting with the Arrow/DataFusion paths in both Cypher and Locy (§4.1).
3. **God-modules** — `writer.rs` (5.0k), `planner.rs` (~10k), `df_planner.rs` (8.4k), `locy_fixpoint.rs` (6.4k), `read.rs` (5.6k), `UniInner` (~30 fields) (§4.2).
4. **Deleted-not-archived documentation** — the flagship 2.0 subsystem (SSI) has no in-repo design record, and the Black Book actively contradicts it (§7).

---

## 2. Critical findings (suspected bugs — verify before anything else)

These were discovered during the review, not previously tracked.

### 2.1 Plan-cache hash collision can execute the wrong plan — HIGH — **FIXED (2026-06-10)**
`plan_cache_key` is a 64-bit `DefaultHasher` of the query text (`crates/uni/src/api/session.rs`) and `PlanCache::get` never verified the stored query text on a hit, so two colliding queries would silently execute each other's plan — including on the **transaction write path** (cached since `d6adcf327`), where a collision corrupts data. Risk calibration: accidental collision among ≤1000 cached entries is negligible (~n²/2⁶⁵); the real exposure was adversarial — `DefaultHasher::new()` is fixed-key, so a crafted collision pair (a ~2^32 offline birthday search) is portable across processes and deployments.

**Fix:** `PlanCacheEntry` now stores the query text and `get(key, query, schema_version)` compares it on every lookup; a text mismatch is a miss that does not evict the resident entry. Both call sites (read path in session.rs, tx write path in impl_query.rs) thread the query text. Regression test: `plan_cache_tests::colliding_keys_with_different_query_text_miss` (session.rs).

### 2.2 Non-linear Locy recursion / multiple positive IS-refs per clause — HIGH — **FIXED (2026-06-10)**
Verification found a broader failure than the suspected semi-naive gap: ANY clause with two positive IS-refs silently derived **zero rows** — self-rule or cross-rule, chained (`a IS r TO mid, mid IS r TO b`) or with both subjects MATCH-bound — while a single IS-ref worked. Two distinct bugs:

1. **Column collision (the dominant bug):** each positive IS-ref cross-joins a `LocyDerivedScan` whose columns keep the target rule's yield names; the join filter from `build_is_ref_predicate` referenced those columns **unqualified**, so the second ref's filter resolved against the **first** scan's columns — contradictory predicates (e.g. `mid = a`), empty result, no error. (`strip_conflicting_structural_columns` strips only struct-typed graph-scan columns, not the first derived scan's flat UInt64 columns.)
2. **Semi-naive incompleteness (latent behind #1):** `update_derived_scan_handles` injected per-iteration delta into every self-ref scan, so non-linear recursion joined Δ×Δ and missed Δ×F_old — empirically confirmed at exactly the predicted 8/10 facts once #1 was fixed.

**Fix:** (a) the second and later positive IS-refs of a clause alias their derived scan's columns with a per-occurrence `__isref{n}_` prefix and point their predicates at the aliased names (`locy_planner.rs` Step 3 + `alias_derived_schema`); `DerivedScanExec` re-stamps emitted batches with its own schema (zero-copy, names-only); `TO`-bound targets are registered as node variables so chained refs can use them as subjects. The clause's final yield projection drops aliased columns, so they never leak into derived facts. (b) `FixpointRulePlan::non_linear` (set in `convert_to_fixpoint_plans`, which runs per stratum): rules with a clause holding ≥2 positive same-stratum IS-refs get full facts (naive evaluation) on their self-ref scans — covers both `p :- p, p` and `p :- p, q` (same SCC) shapes; dedup keeps convergence.

Regression coverage: `crates/uni/tests/common/locy/locy_nonlinear_recursion.rs` (5 tests — non-linear TC 10/10 on the 5-chain, chained + MATCH-bound cross-rule probes 3/3, linear + single-ref controls) and 2 new Locy TCK scenarios (`tck/features/evaluate/NonLinearRecursion.feature`). Suites green after fix: Locy TCK 499, uni-db 1659, uni-query 656.

### 2.3 WASM plugin wall-clock timeout is a no-op — HIGH — **FIXED (2026-06-10)**
`epoch_interruption` was enabled and `set_epoch_deadline(1)` set, but nothing ever called `Engine::increment_epoch` — a runaway pure-compute component hung the executor indefinitely while timeouts *looked* configured. Resource limits were plugin-author-declared only (`fuel_per_call`/`timeout_ms` defaulted to `None`), `memory_max_pages` was parsed but never applied, fuel was set once per pooled store (depleting across calls), and resource-limit traps were misclassified as `WasmError::Invoke`.

**Fix** (`crates/uni-plugin-wasm/src/loader.rs`): `EffectiveLimits` resolves manifest values against host floors — **default timeout 30 s, default memory cap 1 GiB (16384 pages)** when undeclared; manifest overrides win; no host fuel default (fuel costs are opaque, wall-clock is the universal guard). `build_engine` always enables epoch interruption and spawns the canonical per-engine ticker thread (`Engine::weak()` + `increment_epoch` every 50 ms; exits when the engine drops). `reset_call_limits` re-arms the epoch deadline and fuel before **every** export call. `memory_max_pages` is enforced via `StoreLimits` in `HostState` + `Store::limiter`. `classify_trap` maps `Trap::OutOfFuel`/`Trap::Interrupt` → `WasmError::ResourceLimit`. Regression tests (`loader.rs::tests::resource_limits`, core-module driven): infinite loop traps within the budget (pre-fix: hangs forever), memory growth past the cap denied, fuel exhausts → `ResourceLimit` and resets per call, defaults/overrides resolution. uni-plugin-wasm 27/27 incl. the real geo/net component e2e under the new defaults.

### 2.4 Locy reads bypass the SSI read-set — HIGH — **FIXED (2026-06-10)**
Empirically confirmed: the identical RMW shape conflicted via Cypher reads but **committed cleanly** via `tx.locy(...)` reads. Root cause: the Locy evaluation built its DataFusion executor without installing the transaction's L0 (`impl_locy.rs` step 2 never called `set_transaction_l0`, unlike the Cypher path), so the planner's `L0Context` had no `occ_read_set` and the `ReadSetRecordingExec` gate never fired — Locy bodies also lacked read-your-writes.

**Fix:** (a) `impl_locy.rs` installs `tx_l0_override` on the executor exactly like the Cypher path — Locy scans now record into the OCC read-set and see the tx's uncommitted state; (b) **behavior change:** `tx.apply()` is **fresh-by-default** (session-level DERIVE reads can never be OCC-validated, so the version-gap check is the only guard) — `StaleDerivedFacts` on any gap unless `.allow_stale()` or `.max_version_gap(n)`; mirrored in the sync API and both Python APIs (async `apply(..., require_fresh=True)` default; `require_fresh=False` maps to the explicit stale opt-out); (c) `ProjectionBuilder::build` pins an L0 snapshot (`pin_snapshot()`) for the whole build instead of live `get_current()`/`get_pending_flush()` reads — no more torn projections across a concurrent rotation (Lance-tier reads remain analytics-grade, documented). Regression coverage: `crates/uni/tests/common/locy/locy_ssi_read_set.rs` (Cypher control + Locy RMW conflict + stale-apply default + fresh-apply).

### 2.5 WAL durability is weaker than its "durable commit point" role — HIGH — **FIXED (2026-06-10)** (group commit deliberately out of scope — P2)
Segments were raw JSON with no checksum (any torn segment hard-failed the entire recovery with an opaque parse error), and `object_store::LocalFileSystem` PUTs don't fsync (power loss could drop acknowledged commits).

**Fix** (`crates/uni-store/src/runtime/wal.rs`): (a) **checksummed v2 envelope** `UNIWAL2\n<blake3-hex>\n<payload>`; legacy raw-JSON segments stay readable; (b) **tail-vs-middle policy** in `replay_since`: a corrupt/empty segment at the tail is a torn write — warn + treat as end-of-WAL (the commit was never acknowledged); a corrupt segment with valid segments after it fails recovery with an error naming the file; truncation never deletes corrupt segments (evidence preserved); (c) **fsync-on-flush for local stores**: `WriteAheadLog::with_local_root` fsyncs the segment file + parent directory after PUT (pattern from uni-sidecar) before the flush reports durable — wired for the main WAL (data-dir layouts) and fork WALs via `StorageManager::local_fs_root()`. Regression coverage: 6 new/updated tests in `wal_durability_test.rs` (tail skip ×3, middle hard-error, checksum mismatch, legacy compat) + e2e `ssi_resilience::corrupt_wal_tail_does_not_block_reopen` (pre-fix: DB unopenable after a torn tail). **Still open (P2):** group commit on the append/flush seam; compact binary encoding.

---

## 3. Known-debt confirmations (severity validated; no surprises)

- **C2 Lance base-pinning is a stub.** `SnapshotView.started_at_version` exists but is "not yet consulted" (`crates/uni-store/src/runtime/l0_manager.rs:34,49-50`). Snapshots pin only L0 generations; a flush completing mid-transaction silently degrades snapshot isolation across the L0/L1 boundary for long transactions. Live footgun: `lance_version` is hardcoded to 0 (`writer.rs:3660,3823`) yet flows into a real `checkout_version` path — populating it without per-tx pinning would read the empty initial version. The architecture accommodates the fix cleanly (per-tx pinned storage view).
- **Clone-on-freeze is O(entire main L0)** (`l0_manager.rs:253-263` deep-clones every map) per *contended* commit, while holding `flush_lock`. Zero cost uncontended (self-pin released pre-commit). Known, deferred; persistent/COW maps (`im::HashMap`) or generation-chained overlays would make it O(1).
- **Phantom gap** (documented): item-level read-set granularity misses predicate phantoms; FOR UPDATE is exact-key-mutex only. Mitigated for declared unique keys by the commit-time `constraint_index` check (`writer.rs:605-620`). The read-set could grow a predicate component (label + key ranges) checked in `CommitRegistry::check` — recording sites already exist at the planner scan sites.
- **`Transaction::bulk_writer()` bypasses tx_l0/OCC/rollback** ("writes directly to storage", `transaction.rs:665`) while living on the type that promises isolation-until-commit — a documented API trap. Either move it off `Transaction` or make it consume/poison the transaction.
- **Compaction loads all vertices into memory** — explicit OOM TODO at `crates/uni-store/src/storage/compaction.rs:111`, mitigated only by a row-count guard. Prioritize streaming compaction before large-label deployments.

---

## 4. Structural debt

### 4.1 Dual engines (the biggest drift engine)
- **Cypher:** a full legacy row interpreter survives in `crates/uni-query/src/query/executor/read.rs:963-2621` ("fallback executor"), routed by `is_ddl_or_admin` (read.rs:1088), alongside the DataFusion path. Two expression evaluators: row-level `expr_eval.rs` (3k, used by fallback + write helpers) vs the DF translation stack (`df_expr.rs` 4.2k + `df_udfs.rs` 8.2k + `expr_compiler.rs` 2.6k). Every NULL/coercion semantics fix must land twice.
- **Locy:** the vectorized Arrow fixpoint coexists with a row-based path (`locy_eval.rs`, `locy_delta.rs` `RowRelation`, `locy_slg.rs` SLG tabling) used by QUERY/EXPLAIN/ASSUME/ABDUCE/DERIVE. `SemanticParity.feature` acknowledges but cannot eliminate the drift risk.

**Direction:** shrink the routing predicates (true DDL only), converge write-helper expression eval onto one evaluator, shrink the Locy row engine to EXPLAIN/SLG-only, and keep growing parity TCK until deletion is safe.

### 4.2 God modules
| Module | Size | Contents |
|---|---|---|
| `uni-store/src/runtime/writer.rs` | 4,974 | commit protocol, WAL replay, three flush phases, OCC, FOR UPDATE locks, embedding runtime, ID allocation, fork metrics |
| `uni-query/src/query/planner.rs` | ~10k | AST→plan, the 40-variant `LogicalPlan` enum, fusion rewrites |
| `uni-query/src/query/df_planner.rs` | 8.4k | physical lowering, all operator families |
| `uni-query/src/query/df_graph/locy_fixpoint.rs` | 6,440 | fixpoint state, 3 dedup strategies, complement/anti-join, provenance, WMC, neural invocation, `DerivedScanExec`; `run_fixpoint_loop` takes 25 parameters |
| `uni-query/src/query/executor/read.rs` | 5.6k | incl. the legacy interpreter |
| `crates/uni/src/api/mod.rs` (`UniInner`) | 3,836 | ~30 fields: storage, plugins, scheduler, CDC, metrics, connectors, fork registries |

`CompiledClause` fragility is confirmed (`crates/uni-locy/src/types.rs:123`): 9 fields, no `Default`/builder, 27 literal constructions in `locy_planner.rs` tests alone. Same for `ModelInvocation` (10 fields) and `LogicalPlan::Traverse` (17 fields, QPP bolted on via `Option<Vec<QppStepInfo>>`).

### 4.3 Crate graph (~80% pulling its weight)
Layering is acyclic with **one genuine upward smell**, self-documented in `crates/uni-query/Cargo.toml:21-28`: `uni-query → uni-plugin-builtin` (downward) while `uni-plugin-host → uni-query` (upward) — the M3 planner constructs builtin trait objects eagerly instead of resolving through `PluginRegistry`. Moving that edge makes the plugin stack strictly above the engine.

Other observations:
- `uni-query` (88.8k src, ~30% of the workspace) holds the **Locy runtime** while `uni-locy` (8.7k) holds only the front-end — a split along a compile seam, not a domain seam. Natural cut: extract `df_graph/locy_*` toward uni-locy, or split planner vs executor; uni-query is on the critical build path for fork, plugin-host, bulk, db, and all four top-level consumers.
- `uni-crdt → uni-plugin` looks inverted but is deliberate and documented (registry dispatch for `merge_via_registry`); side effect: uni-plugin (12k) is load-bearing for storage.
- Merge candidates: `uni-sidecar` (0.4k), `uni-bulk` (1.6k), `uni-fork` (1.4k), `uni-plugin-wasm-rt` (1.1k) — each single-purpose glue; `uni-plugin-apoc-core`/`uni-plugin-custom` could fold into builtin.
- **uni-xervo (external ML provider) enters at three layers** — uni-store, uni-query, uni-db; the storage-layer entry is the most surprising.
- The 9-crate plugin family is justified: each backend isolates a heavy optional dep (wasmtime/extism/pyo3/rhai) behind a cargo feature.

### 4.4 Layering leaks
- `StorageBackend` leaks Lance: SQL-string filters, `merge_insert` documented as "Lance MergeInsert", `notify_table_created` existing solely to patch the Lance backend's existence cache (`crates/uni-store/src/backend/traits.rs:60-73`). Tighten with a typed filter AST.
- `executor/write.rs` encodes Lance flush mechanics (partial MergeInsert vs full Append, `PendingVertexSet.partial`) two layers up. Introduce a narrow storage-write trait (mirroring `ForkIndexLookup`).
- `uni-algo` reaches directly into `L0Manager`/`PropertyManager`/`AdjacencyManager` (`projection.rs:16-19`, `traversal.rs:10-11`) — any store refactor (e.g. the in-flight L0 refactor) ripples here. A narrow `GraphReadSource` trait (vertex iter + adjacency + property read at a pinned snapshot) fixes this **and** the §2.4 projection-isolation gap together.
- `crates/uni/src/lib.rs:120-123` still re-exports `algo_crate`/`common`/`query_crate`/`store` raw — duplicate public paths for every internal type (API_REVISION Phase 4, unfinished).

---

## 5. Cross-cutting concerns

### 5.1 Error handling
`UniError` (`crates/uni-common/src/api/error.rs`, `#[non_exhaustive]`, thiserror 2) is a well-designed taxonomy with exemplary `is_retriable()` reasoning — **but** ~1,150 non-test `UniError::Internal`/`anyhow!` sites exist vs zero uses of the structured `Storage { source }` variant in uni-store. The implicit `#[from] anyhow::Error` (error.rs:119) encourages `?`-laundering: an Internal-wrapped conflict won't retry, and Python exception mapping degrades. **Fix:** drop the `#[from]`, route storage failures through `Storage`, audit conflict paths for retriability laundering.

### 5.2 Async/sync boundary
- **Seven sites** in uni-query spawn a scoped thread + fresh `current_thread` runtime to `block_on` async work from inside synchronous DataFusion expression evaluation — per traversal step, potentially per batch (`df_graph/pattern_exists.rs:213,346`, `pattern_comprehension.rs:236,316,363`, `similar_to_expr.rs:362`, `expr_compiler.rs:2292`). A shared lazy runtime handle (or pre-warming CSR before execution) removes all seven.
- **Python sync API holds the GIL across 109 `block_on` sites**; only the 6 Locy paths use `py.detach` (added to fix a real deadlock — `bindings/uni-db/src/sync_api.rs:236-248`). Long queries stall every Python thread; any future Rust→Python callback on a non-Locy path deadlocks the same way Locy did. Audit and wrap all of them.
- Three runtime-creation idioms coexist (`UniSync` owns one; bindings use pyo3-async-runtimes with 8 MB stacks; executors build ad-hoc ones). Pick one documented policy.

### 5.3 Lint/unsafe policy
No `[workspace.lints]`, no `forbid/deny(unsafe_code)` anywhere. ~25 unsafe sites are all in expected FFI locations, but `unsafe impl Send for PyProgressWrapper` (`bindings/uni-db/src/convert.rs:429`) and `Send/Sync for PySessionHook` (`builders.rs:1923-1924`) carry no SAFETY comments. **Fix:** workspace lints with `unsafe_code = "deny"` + per-crate allows for the 4 FFI crates; require SAFETY comments.

### 5.4 Dependencies & observability
118 of 1,288 lock packages have duplicate versions (hashbrown ×5, windows-sys ×5, rand ×3) — mostly forced by lance/candle/wasmtime; track with `cargo deny`/`cargo tree -d` in CI. tokio uses blanket `features = ["full"]`. Observability is structured but uneven: plugin spans are excellent (TraceId propagation query→plugin→HTTP); uni-locy/uni-fork/uni-crdt have ≤2 tracing call sites each; ~20 stray `println!`/`eprintln!` remain in library code.

### 5.5 Python binding surface
~140 `m.add_class` registrations, parallel sync (`builders.rs` 2,693 + `sync_api.rs` 1,443) and async (`async_api.rs` 4,247) wrapper sets, plus a hand-maintained 2,398-line `__init__.pyi` — three places to update per feature, with no CI check that the stub matches the compiled module. Generate or verify the stub (stubgen diff / `pyright --verifytypes`). No Python `transact_with_retry` equivalent exists while SSI default-on makes aborts a first-class Python experience — add one.

### 5.6 Wheel bloat is structural
`default` features (`crates/uni/Cargo.toml:177-192`) pull candle + mistralrs + onnx + 8 HTTP providers into every standard wheel on top of lance/datafusion/arrow → ~157 MB wheels (>100 MB PyPI limit). The provider×linking×acceleration matrix already exists; "default = everything local" defeats it for the flagship wheel. Slimming the default wheel directly attacks the PyPI-limit problem.

---

## 6. Testing & CI

**Strengths:** the SSI suite is excellent (Hermitage-style anomaly matrix, proptest invariants, 8-thread stress, soak tier); recovery tests use failpoint injection (`commit::after-validate`, `commit::mid-wal`, `commit::after-wal-flush`); release-path guards are unusually good (wheel-variant feature-unification check, publish-closure check, version-consistency); dual-mode TCK runs with reporting tooling; notebooks have drift verification.

**Gaps, ranked:**
1. **openCypher TCK does not run on PRs** (`pr.yml:107` excludes uni-tck; TCK only on main-push) — conformance breakage lands green.
2. **Zero fuzzing** despite a pest grammar, a value codec with a recent security fix (`decode_batch`), and a WASM boundary. Add cargo-fuzz targets: Cypher parser, Locy grammar, `value_codec::decode_batch`, WAL decode; 5-min on PRs, longer nightly.
3. **No perf regression tracking** — 12 criterion benches (incl. `ssi_commit_overhead`, `ssi_contention`) and the ingest harness exist, but no workflow runs them. The recent −38%/−41% ingest wins can silently regress. Promote `examples/cypher_ingest_speedup.rs` to a tracked bench with a stored baseline.
4. No `.config/nextest.toml` (no slow-test timeout/retry policy; a hung test stalls CI).
5. Cloud (LocalStack S3) tests skip PRs; object-store regressions surface post-merge.
6. Crash testing is in-process only — add one out-of-process SIGKILL recovery test.
7. Soak/`#[ignore]` tier (35 files) has no scheduled lane — add a nightly cron workflow.
8. pr.yml/ci.yml duplication — factor into a `workflow_call` reusable workflow.

---

## 7. Documentation

**Headline:** the design-proposal layer was **deleted, not archived**. Commit `27fecacaf` (2026-06-01) removed all 12 docs in `docs/proposals/` (including `serializable_snapshot_isolation.md` — the only authoritative record of the C2 footgun, the gap inventory, and the rotate-aside-vs-clone-on-freeze rationale), plus `docs/migrations/` and `docs/plans/`; `017e534f4` removed `docs/KNOWN_GAPS.md`. Copies survive only in git history and stale worktrees.

Consequences:
- `RELEASE_NOTES_2.0.0.md` (uncommitted) references two deleted docs (line 45 migration guide; line 219 design proposals). **Must fix before tagging 2.0.0.**
- `docs/UNI_BLACK_BOOK.md` Part XI still says "One writer at a time: prevents write-write conflicts entirely" — directly contradicting default-on SSI abort/retry, the flagship 2.0 behavioral change. Zero mentions of `ssi_enabled`, `async_flush`, or `FlushCoordinator`.
- `AGENTS.md` lines 1–43 describe a single-crate `src/` layout and mandate updating `DESIGN.md`/`CYPHER_GAPS.md`/`docs/KNOWN_ISSUES.md` — none exist. (Lines 45–155, the fork invariants, are current and excellent.)
- `README.md` pins `uni-db = "0.1.3"` (workspace is 2.0.6) with pre-Session/Transaction Python examples.
- The L0 refactor's target-state hierarchy (the invariant clone-on-freeze builds on) has no in-repo description; phases 1–3 landed, 4–5 untracked.

**Recommendations:** restore proposals under `docs/proposals/archive/` (archive-don't-delete policy); rewrite Black Book Part XI; fix AGENTS.md/README; recreate a living gaps doc; add a doc map (Black Book = internals truth, website = user truth).

---

## 8. Per-subsystem notes (condensed)

- **Storage (uni-store, uni-btic):** commit sequence carefully ordered with failpoint seams; manifest publish body-then-pointer; WAL truncation respects pending flushes; pin-token snapshot design elegant (O(1) capture, freeze only when pinned). Lock topology sound but undocumented as a contract. uni-btic (temporal interval codec) is well-factored, proptest-covered, no concerns. Stale comment at `l0.rs:684` ("WAL logging for label mutations not yet implemented" — it is, on the tx path).
- **Query engine:** pest → walker → AST → 40-variant LogicalPlan → hybrid DF physical plan. Hand-rolled openCypher 3VL where DF SQL semantics diverge (3-valued IN, XOR UDF, null-propagation-first coercion) — correct and documented. `uni-query-functions` extracted specifically to keep leaf edits off the 89k SCC. `GraphApplyExec` non-batched fallback is O(rows × subplan) for correlated CALL{} with distinct params — extend batching eligibility. Plan cache: LFU eviction is an O(n) scan per insert at capacity (fine at 1000).
- **Locy:** probabilistic semantics are *principled, not bolted on* — semiring abstraction (AddMultProb/MaxMinProb/BddExact/TopKProofs), epsilon-aware log-space multiplication, provenance store, two-tier shared-proof detection, BDD exact-WMC with fallback. Stratification (iterative Tarjan + cyclic-negation rejection) is correct and directly tested. Fragile spots: convergence rests on a magic 1e-12 float-rounding constant decoupled from `probability_epsilon`; `reconcile_schema` patches planner type mistakes at runtime; `__prob_complement_*`/`__model_*` string-prefix column conventions; mutually-recursive SCCs re-derive from full facts every iteration (correct but quadratic).
- **Transactions/SSI:** single chokepoint design (all validation/merge under `flush_lock` in one function) is easy to reason about; `CommitRegistry::check` is clean backward-validation OCC with a sound conservative abort on history truncation; read-set capture is two-pronged (inline in `l0_visibility.rs` + `ReadSetRecordingExec` at 4 planner sites + `record_edge_adjacency`), self-gating on `Option`. FOR UPDATE re-stamps `occ_read_seq` + re-pins so locked RMW commits without retry. Minor smear: uni-query reaches into `l0.read().occ_read_set` directly (`df_planner.rs:1677`).
- **Plugins:** three-layer capability enforcement is real (registrar gate → structural linker omission → call-time pattern attenuation); signature design thoughtful (domain-separated versioned canonical payload; fail-closed) but `SignaturePolicy::Disabled` is the default posture; secret-handle IPC membrane blocks exfiltration both directions. Host-fn parity uneven: Rhai full, Extism wired, WASM CM only log/trace/net (no kms/secret WIT yet). `uni-plugin`'s "kernel" claim drifting — it now carries scheduler/circuit-breaker/reload/fs-guard runtime machinery.
- **API/bindings:** the Rust core contract is genuinely hard to misuse (read-only sessions, single write guard, tx consumed on commit, Drop-time lock release, `derived_clone` single construction point). The old Deref pattern is gone (`Uni` is an opaque handle). API_REVISION Phases 2B/3 landed; Phases 1 (no `IntoFuture` on `UniBuilder`) and 4 (raw re-exports) unfinished. uni-pydantic is reflection-based (metaclass-derived labels/relationships), one-way push to DB schema.
- **Auxiliary crates:** uni-algo's CSR projection + rayon + spawn_blocking is the right compute model, but CSR uses unchecked `u32` offsets (~4.29B-edge silent wrap — add an overflow guard). CRDT carve-out is precise (variant-mismatch still conflicts) and macro-driven against drift. uni-fork's host-trait inversion (`ForkQueryHost`/`ForkPromoteSink`) keeps it cycle-free; fork creation is a 4-step 2PC with allocators bootstrapped above primary's HWM. uni-bulk's abort-via-Lance-version-rollback is a real rollback story; its SSI invisibility rests entirely on the Transaction write-guard convention — document/assert it.

---

## 9. Recommended priority order

**P0 — Now (correctness):**
1. ~~Plan-cache collision check (store + compare query text)~~ — **DONE 2026-06-10**, §2.1.
2. ~~Non-linear-recursion / multi-IS-ref fix~~ — **DONE 2026-06-10** (column aliasing + full-facts fallback), §2.2.
3. ~~WASM epoch ticker + host-imposed resource floors + `memory_max_pages`~~ — **DONE 2026-06-10**, §2.3.
4. ~~WAL checksums + tail policy + local fsync~~ — **DONE 2026-06-10** (group commit stays P2), §2.5.

**P1 — Before tagging 2.0.0 (docs honesty):**
5. Fix RELEASE_NOTES dead references; restore proposals as archive; rewrite Black Book Part XI; fix AGENTS.md top half + README — §7.

**P2 — Near-term hardening:**
6. ~~Locy/algo read-set recording + default-fresh `apply()` + projection pin~~ — **DONE 2026-06-10**, §2.4.
7. C2 Lance base-pinning (hook exists) — §3.
8. Python `transact_with_retry` + `py.detach` audit — §5.2/§5.5.
9. TCK on PRs, fuzz targets, `.config/nextest.toml`, nightly soak/bench lane — §6.
10. Group commit on the existing WAL flush seam — §2.5.

**P3 — Structural (scheduled, not urgent):**
11. Retire the legacy row interpreters (Cypher fallback + Locy row path) — §4.1.
12. Split `writer.rs` / `planner.rs` / `df_planner.rs` / `locy_fixpoint.rs`; builders for `CompiledClause`/`ModelInvocation` — §4.2.
13. Move the `uni-query → uni-plugin-builtin` edge; consider extracting the Locy runtime from uni-query — §4.3.
14. Drop `#[from] anyhow` from `UniError`; workspace lints + `deny(unsafe_code)`; shared async-bridge runtime in uni-query — §5.
15. Typed filter AST for `StorageBackend`; `GraphReadSource` trait for uni-algo; storage-write trait for `write.rs` — §4.4.
16. Slim the default wheel (move local inference providers out of `default`) — §5.6.

---

*Review conducted 2026-06-10 against local `main` (tip `d6adcf327` + uncommitted release notes). Line numbers will drift; symbol names are the stable reference.*
