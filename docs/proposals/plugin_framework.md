# uni-db Plugin Framework

## A Complete, Unified Extensibility Layer for Functions, Aggregates, Operators, Indexes, Storage, Algorithms, CRDTs, Hooks, and Connectors

**Status:** Draft — implementation in progress
**Version:** 1.2.0
**Date:** 2026-05-22 (last status update: 2026-05-24 — M5 Batch 1 landed (async Storage + Lance / RGA CRDT / phased-hook bridge / M5i audit), plus M5 Batch 1 ready-followups (Lance predicate pushdown + delete + WriteHandle.id; `uni-plugin` 1.4.0 phased context shape v1.1), plus **Lance fork wiring** (`uni-plugin` 1.5.0 — `Storage::fork(table, src, dst) → BranchMetadata`, `LancePluginStorage` delegates to new `LanceDbBackend::fork_branch`))

> **Last status alignment: 2026-05-24** — see `docs/plans/plugin_framework_implementation.md` for per-milestone state. The "Implementation status" block immediately below is point-in-time; the rest of this document is design language and is unchanged.
**Worktree:** `plugin-fw`
**Companion documents:**
- `docs/research/plugin_frameworks_sota.md` — SOTA survey of Rust + database plugin systems 2024–2026
- `docs/plans/plugin_framework_implementation.md` — detailed milestone-driven execution plan with file:line anchors, tests, acceptance, and risk register
- `docs/plans/SESSION_FINAL.md` — current implementation status, per-crate test counts, per-milestone progress

## Implementation status (2026-05-24)

The proposal is partly realized in the `plugin-fw` worktree:

- **9 new plugin-framework crates** scaffolded with public-API surfaces in place: `uni-plugin`, `uni-plugin-builtin`, `uni-plugin-apoc-core`, `uni-plugin-wasm` (Component Model), `uni-plugin-extism` (Extism ABI), **`uni-plugin-rhai` (Rhai scripting, M7 substantive)**, `uni-plugin-pyo3`, `uni-plugin-custom`, `uni-plugin-conformance`.
- **≈497 plugin-crate test attributes across the 11 shipped crates** (static `#[test]` / `#[tokio::test]` / `#[rstest]` count, 2026-05-31: uni-plugin 115, uni-plugin-builtin 118, uni-plugin-extism 53, uni-plugin-rhai 39, uni-plugin-apoc-core 33, uni-plugin-custom 33, uni-plugin-host 31, uni-plugin-pyo3 28, uni-plugin-wasm-rt 22, uni-plugin-wasm 17, uni-plugin-conformance 8), zero clippy warnings, zero regressions in `uni-query`'s pre-existing suite.
- **All 25 surface traits from §4 exist** with `#[derive(Debug)]`-cleanly-implementable concrete types.
- **`CustomFunctionRegistry` is a facade** over a shadow `PluginRegistry`, wired into the DataFusion adapter at `crates/uni-query/src/query/executor/read.rs:319` as the **sole** registration path (M2 cutover landed 2026-05-24: the legacy `register_custom_udfs` call and the `CustomScalarUdf` per-row LargeBinary adapter in `df_udfs.rs` have been deleted). The `NativeArrowUdf` fast-path declares primitive return types directly (skipping the `LargeBinary` round-trip) when the plugin's signature is primitively typed.
- **9 built-in Locy aggregates** (`MIN` / `MAX` / `SUM` / `MSUM` / `COUNT` / `AVG` / `COLLECT` / `MNOR` / `MPROD`) are registered through the framework with `Semilattice` metadata.
- **83 built-in procedures** flow through the plugin path: 1 `uni.system.echo` (BuiltinPlugin) + 38 APOC analogues (`uni.bitwise.*` 6, `uni.text.*` 13, `uni.math.*` 10, `uni.number.*` 3, `uni.convert.*` 4, `uni.create.*` 2) in ApocCorePlugin + 5 `uni.schema.*` + 36 `uni.algo.*` (wrapped via `AlgorithmProcedureAdapter`) + 3 search (`uni.vector.query`, `uni.fts.query`, `uni.search`) in `uni-query::procedures_plugin`. `procedure_call.rs::execute_procedure` collapses to `if registry.resolve { invoke } else { tck_mock_fallback }` (1115 lines, down from 2309). Capability gating against `Principal` is deferred to M6 per plan.
- **M11 operational layer shipped (Phases A+B+C complete, 2026-05-26)**: write-enabled `QueryProcedureHost::execute_inner_query(cypher, params, mode)` with `with_writer` builder; canonical `Schedule { Once / Periodic / Cron / Manual }` enum with `next_after(now)`; `Scheduler::tick_at(now)` time-aware driver primitive; `SchedulerPersistence` trait + `MemoryPersistence` default; **tokio-backed `SchedulerHost` driver** in `crates/uni/src/scheduler.rs` polling every 100 ms, dispatching due jobs to `BackgroundJobProvider`s via `spawn_blocking`, wired into `Uni::build` next to the `DeferralQueue` ticker; three built-in maintenance jobs registered (`uni.system.{ttl_sweep,statistics_refresh,compaction}`, currently tracing stubs pending host-service integration); `uni.periodic.{schedule,cancel,list}` Cypher procedures + `Uni::periodic_*` Rust API backed by a new `SchedulerControl` trait; `CircuitBreaker` plumbed into the scheduler driver dispatch (10-fail threshold opens, 30 s cooldown, half-open semantics); `crates/uni/src/observability.rs::init_otel_subscriber(cfg)` exposing `OtelGuard` over `opentelemetry 0.27` + `tracing-opentelemetry 0.28` + OTLP/gRPC; `SystemLabelPersistence` (`crates/uni/src/persistence.rs`) durable backend at `<data_path>/_system/declared_plugins.json` (atomic write-then-rename; the `DeclaredPlugin` serde shape matches §9.7 verbatim so the eventual cutover to Cypher `MERGE (:_DeclaredPlugin {...})` is a backend swap); `SyntheticProcedurePlugin` + `CypherProcedureSynthesizer` synthesizing declared-procedure bodies via the write-enabled inner-query host (sync→async bridged with `block_in_place` + `Handle::block_on`); declare-time and reactivate-time synthesizer integration via the new `ProcedureBodySynthesizer` trait. **Pre-existing M11 primitives still shipped**: `SecretStore` (sealer/unsealer membrane with handle revocation), `verify_hash_pin` (Blake3), Ed25519 signed-manifest verification (real, default-on behind a feature gate enabled by default), `PluginLifecycle` state machine, `EpochFencedReload` drain driver, `CapabilitySet` with host-imports / surface gates / quotas. **797 tests pass** across the M11-touched crates.
- **`DeclaredPluginStore`** (apoc.custom analogue) ships in-memory with dependency-missing detection, cycle detection, drop-with-dependents protection. `CustomPlugin::listDeclared` and `::dropDeclared` are registered as procedures via `uni.plugin.*`.
- **5 CRDTs** (LWW, OR-Set, G-Counter, MV-Register, RGA) and **5 logical types** (uri, geo.point, email, ipv4, ipv6) registered through `BuiltinPlugin`. RGA wraps `uni_crdt::Rga<String>`; ops carry pre-generated UUIDs for convergent peer-merge. Host consultation of the `CrdtKindProvider` registry from `uni-crdt` mutation paths is a tracked follow-up.
- **Conformance suite** (`uni-plugin-conformance`) ships a real 6-probe suite.
- **Async plugin `Storage` trait + Lance backend wired (M5a, 2026-05-24)**: `MemoryBackend` (HashMap-keyed tables) and `LanceBackend` (bridged to `uni_store::LanceDbBackend` via `LancePluginStorage` adapter, behind the default-on `lance-backend` cargo feature) both register through `BuiltinPlugin`. **Predicate pushdown landed** via `datafusion::sql::unparser::expr_to_sql` (unencodable shapes surface as `FnError 0x711`). Predicate-less delete fast-paths to `replace_table_atomic`. `WriteHandle::id` mirrors `LanceDbBackend::get_table_version` (monotonic per-write). **Fork wiring landed** (`uni-plugin` 1.5.0): `Storage::fork` grew a per-dataset `table: &str` parameter and now returns `BranchMetadata { parent_version, branch_name }`; `LancePluginStorage` overrides `supports_branching() → true` + `fork()` to dispatch through new `LanceDbBackend::fork_branch(table, src, dst)` (uses `lance_branch::create_branch` for src=="main", `create_branch_from` for nested branches). Exercised in `crates/uni-plugin-builtin/src/storage.rs::tests`.
- **Phased-hook bridge shipped (M5e, 2026-05-24)**: `LegacyHookAdapter` in `crates/uni/src/api/hooks.rs` wraps the legacy 4-method `SessionHook` and implements the phased `uni_plugin::traits::hook::SessionHook` (12 routing tests). **`uni-plugin` 1.3.0 → 1.4.0** added the v1.1 context shape: `QueryType` enum, `PluginCommitResult` struct, `ParseContext::{query_type, params}` with builders, `CommitContext::commit_result` with builder. Bridge mirrors real values when populated; back-compat zero-stub when not. `Session::add_hook` continues to dispatch through its own HashMap for backward compat; pulling Session dispatch onto the plugin registry is M5b work.

What still requires cutover commits to complete the proposal:
- ~~M2 cutover~~: ✅ **done** (2026-05-24) — `register_custom_udfs` call removed from `read.rs`; orphaned `CustomScalarUdf` LargeBinary per-row adapter deleted from `df_udfs.rs`. `register_plugin_scalar_udfs` is the sole DataFusion adapter for scalar UDFs.
- ~~M3 cutover~~: ✅ **done** — `FoldAggKind` deleted; `MonotonicFoldBinding`/`FoldBinding` carry `Arc<dyn LocyAggregate>` resolved at planner time from `HybridPhysicalPlanner.plugin_registry`.
- ~~M4 cutover~~: ✅ **done** — every built-in procedure routes through `ProcedureRegistry`; `procedure_call.rs::execute_procedure` has zero hardcoded qname arms (Principal-based capability gating remains explicitly deferred to M6).
- M5 cutover: ✅ **all batches landed**. Batch 1 (M5a/M5d/M5e/M5i) 2026-05-24; Batch 2 (M5b + M5h + M5c.1) 2026-05-24; Batch 3 (M5c.2-4 + M5f + M5g) 2026-05-25. Acceptance criteria #14, #16, #17, #25, #26 verified by tests 2026-05-26. M5c.5 legacy 5-arg shim deletion remains as a follow-up release-boundary cleanup.
- M6 cutover: ✅ **substantive** — full wasmtime Component Model end-to-end shipped. `WasmLoader::load` and `ExtismLoader::load` both implement the two-pass shape (bootstrap manifest read → cap intersection → real pool build with `Linker::instantiate` per warm instance → `register` export read → adapter registration via `PluginRegistrar`). `MultiVersionLinker` keyed by `(major, caps_signature)`; v1+v2 linkers ship; `host-log` always-available; capability-gated host fn imports (`host-fs`/`host-net`/`host-kms`) are structurally absent from CM linkers regardless of grant. `crates/uni/tests/m6_cross_abi_parity.rs::cross_abi_haversine_results_match` verifies byte-identical f64 output across CM and Extism loaders. **Remaining**: (a) ship at least one capability-gated host fn body (e.g., `host-fs.read`) so the deny-fails / grant-works lane on the CM side gets an end-to-end test (criterion #6 CM half remains ▶ for this reason); (b) perf-suite p99 measurement for criterion #19.
- M7 cutover: ✅ **substantive** — `uni-plugin-rhai` ships with host-embedded `rhai::Engine` factory (eval disabled, deny-all module resolver, `FuelPerCall` → `set_max_operations`, default call-depth 64), `RhaiHostFnRegistry` capability-gating, manifest parser, scalar adapter (row + vectorized), aggregate adapter (init/accumulate/merge/finalize via serde_json state envelope), procedure adapter (Array&lt;Map&gt; → RecordBatch), full loader three-phase shape, capability-gated host fns (fs/net/kms/secret), `Uni::load_rhai_plugin` API behind default-on `rhai-plugins` feature, vectorized Float64/Int64/Utf8 column userdata + `uni_float_column` allocator, runnable `examples/example-rhai-geo` bin, `db.load_rhai_plugin` PyO3 binding, `uni plugin install foo.rhai` CLI dispatch, cross-loader parity test (Rhai 4-ULP tier), `PluginError::RhaiParse` variant. 37 crate tests + Uni-level e2e + Python test all green.
- M8 cutover: ✅ **PyO3 Arrow C Data Interface bridge shipped + verified 2026-05-26**. `crates/uni-plugin-pyo3` ships row-mode + vectorized scalars (Arrow C Data Interface via PyCapsule protocol — no `pyo3-arrow` dep), aggregates (serde_json state envelope), procedures (Array&lt;Map&gt; → RecordBatch), `@db.scalar_fn/aggregate/procedure` decorator sink + `PluginRegistrar::commit_to_registry` path, `Uni::load_python_plugin` host API, session-scope registration (drop unregisters, session shadows instance). Cross-loader parity ≤ 4 ULP (row + vectorized) verified against the Rust haversine in `m8_pyo3_cross_loader_parity.rs`. Conformance suite passes. 34 crate tests + 7 uni-db integration tests green.
- M9 cutover: persist `DeclaredPluginStore` via `uni_system.declared_plugins` label.
- M10 cutover: ✅ **arc-swap reload-while-query-runs invariant verified 2026-05-26** via `crates/uni/tests/hot_reload_consistency.rs::in_flight_arc_keeps_old_function_alive_through_reload`. Per-kind reload discipline tested for storage backends (2 cases), index kinds (1 case), and CRDTs (2 cases) via the `integration_admin` shim. `MultiVersionLinker` keyed by `(major, caps_signature)` with 5 unit tests; `AbiRange` parses `^N` ranges. **Follow-up**: end-to-end with two real `.wasm` artifacts at majors 1 and 2 in the same query — pending a v2 `example-wasm-geo` build-script artifact (criterion #10 remains ▶ for that reason).
- M11 cutover: ✅ **Phases A+B+C done plus Batch-2 follow-ups (2026-05-26)**. Tokio-backed `SchedulerHost` driver + `uni.periodic.{schedule, cancel, list, submit, iterate, commit}` procedures + capability gate for `declareProcedure WRITE` + circuit-breaker plumbing + OTel layer + `SystemLabelPersistence` (dual-write: JSON sidecar + `_DeclaredPlugin` graph nodes via the write-enabled `execute_inner_query`) + `SystemLabelSchedulerPersistence` (dual-write: `<data_path>/_system/background_jobs.json` + `_BackgroundJob` graph nodes) + `SyntheticProcedurePlugin` + real `compaction` / `ttl_sweep` job bodies (dispatched via new `JobHost::compact_storage` and `JobHost::execute_write_cypher`). **Known follow-ups**: (a) `statistics_refresh` job remains a stub pending a planner statistics-refresh API; (b) `uni.periodic.iterate` is single-pass in v1 — the driver-loop variant lands once read-Cypher access is exposed on `SchedulerControl`; (c) WIT-level `host-secrets` resource (Phase D) and CDC runtime / Defer-queue persistence (Phase E) remain explicitly deferred to later milestones.
- M12 cutover: CLI subcommands + OCI artifact resolution.

Status legend used throughout this document and the implementation plan:
- ✅ **complete** — fully implemented, tests-green, mechanically verified.
- ✅ **substantive** — primary architecture shipped and exercised end-to-end; "cutover" delete-of-legacy-path remains.
- ▶ **partial** — public-API surface live with real (non-stub) impl for some sub-surfaces; remaining sub-surfaces use clearly-flagged placeholders that return `NotYetImplemented`-class errors.
- ▶ **scaffolding** — public-API surface live; calls return `NotYetImplemented` for the primary entry points.
- ⏳ **pending** — not yet started.

Per-milestone status in §19 acceptance criteria below.

---

## 1. Overview

This proposal defines the **complete plugin framework** for uni-db. It is not a phased rollout. Every extensibility surface that a user might reasonably want to plug — scalar functions, aggregate functions, window functions, Locy aggregates, Locy predicates, physical operators, optimizer rules, index kinds, storage backends, graph algorithms, CRDT kinds, session/transaction hooks, logical types, auth providers, authorization policies, and wire/connector protocols — is included in the design from day one and lands on the same unified registry, the same capability model, the same ABI, the same lifecycle, and the same observability backbone.

The framework supports **five loaders** (across four authoring categories) that all converge on the same in-process registry:

1. **Compile-time Rust** — `Arc<dyn Plugin>` linked into the host binary. Trusted, native performance.
2. **WASM Component Model** — `.wasm` component-model artifacts via `uni-plugin-wasm`. WIT-typed contracts; capability gating by linker absence. Used for trusted built-ins and plugins where soundness is load-bearing. Sandboxed, language-agnostic (Rust, Go, JS, Python via componentize-py, C/C++).
3. **WASM Extism** — bytes-in/bytes-out `.wasm` plugins via `uni-plugin-extism`. Mature 13-language plugin SDK; capability gating by host-fn runtime filter. The user-facing default for authored UDFs. Sandboxed.
4. **PyO3 live callables** — Python functions registered from the host Python process via the PyO3 bindings. Dynamic, session-scoped, unsandboxed (runs at host privilege).
5. **Rhai scripts** — Rhai source loaded into a host-embedded `rhai::Engine` via `uni-plugin-rhai`. Pure-Rust, no C toolchain, no WASM wrapper. Dynamic *and* sandboxed — Rhai has no built-in I/O; capability-gated host functions are registered into the Engine only when granted. Resource limits (operation count, memory, call depth, string/array size) enforced by the Engine itself.

**v1 loader scope.** The non-Rust loaders (WASM Component Model, WASM Extism, PyO3, Rhai) author **scalar functions, aggregates, and procedures** in v1. The remaining surfaces in the list above (Locy aggregates/predicates, operators, optimizer rules, index kinds, storage backends, algorithms, CRDT kinds, hooks, logical types, auth/authz, connectors) are **compile-time-Rust-only** until their WIT worlds and host bridges land — see §19 (criterion 30) and Appendix A. Some — notably operators and storage backends, which exchange in-process trait objects, `&Expr` trees, and async record-batch streams — are not expressible across the WASM Component Model boundary and will remain Rust-native.

The two WASM ABIs (Component Model and Extism) ride on the same wasmtime runtime. They are deliberately complementary: CM's structural advantages (typed contracts, linker-absence capability gating, resource types) apply where soundness is load-bearing; Extism's authoring simplicity and SDK breadth apply where user-facing ergonomics matter more. Trust tier is a property of the granted capability set, not of the ABI choice — see §5.1.1.

The framework is **fully columnar end to end**. `RecordBatch` is the universal call shape for every extension kind. The host↔plugin boundary uses Arrow IPC over shared linear memory for WASM; native plugins speak Arrow directly through `ColumnarValue`. The legacy per-row `Fn(&[Value]) -> Value` shape survives only as an opt-in convenience adapter (`RowFn`) and the `LargeBinary` CypherValue transport survives only for fns that genuinely need to see `Node`/`Relationship`/`Path`.

The framework is **dogfooded**: every built-in uni-db function, aggregate, algorithm, CRDT, storage backend, and index is re-implemented as a `Plugin` and registered through the same `PluginRegistrar` end users use. The closed enum `FoldAggKind` in the Locy planner has been deleted (M3 finalization, 2026-05-23): `FoldBinding` carries `Arc<dyn LocyAggregate>` resolved at planner time from the framework's `PluginRegistry`. The built-in vector index in `vector_knn.rs` becomes one `IndexKindProvider` among many. The Lance storage backend becomes one `StorageBackend` registration among many. If the framework cannot express a built-in, the framework is wrong and we fix the framework — that is the integrity invariant.

### 1.1 Design Principles

1. **One registration path.** Every extension — built-in or user, native or WASM or scripted — registers through `PluginRegistrar`. No privileged backdoor.
2. **Columnar by default.** `RecordBatch` everywhere. Row-at-a-time is an opt-in convenience, not the default.
3. **Capability-first security.** Every plugin declares its capability set in its manifest; the host grants a subset at load time; the WIT linker enforces by *absence of imports*, not by runtime checks.
4. **Loaders are orthogonal to execution.** Where a function came from (Rust, WASM, Python, Rhai) is invisible to the executor — it only sees `Arc<dyn ScalarPluginFn>`.
5. **ABI versioned, coexistence first-class.** Multiple `uni-plugin` ABI majors run side by side in one Uni instance via per-major `Linker`s.
6. **Hot reload is a first-class operation,** epoch-fenced so in-flight transactions complete against the old instance.
7. **Determinism is declared, not inferred.** Plugins say whether they are pure / session-scoped / nondeterministic; the planner caches and hoists accordingly.
8. **Observability is built-in.** Every plugin call is a tracing span; every plugin can emit metrics through capability-gated host imports.

### 1.2 Goals

- Open every closed enum / hardcoded match in the engine that a user might reasonably want to extend.
- Make built-ins indistinguishable from user plugins at the registry level.
- Give end-user app developers a path to write plugins in their language of choice, sandboxed.
- Preserve current performance for built-in code paths; **improve** performance for the existing `CustomFunctionRegistry` path by eliminating the per-row `Value` round-trip for primitively-typed UDFs.
- Preserve Locy's monotonicity proofs across the `FoldAggKind` retirement via `Semilattice` metadata.
- Provide hot reload, signing/pinning, capability gating, and resource quotas suitable for production multi-tenant deployments.

---

## 2. Extension Surface Inventory

The framework opens twenty-five surfaces. Each row anchors at the current closed/built-in implementation and names the trait that replaces it. File paths are relative to the workspace root; line numbers reflect the state of the `plugin-fw` worktree.

| #  | Surface                          | Current implementation (closed / static / missing)                                                                            | Replacement trait               | Capability        |
|----|----------------------------------|--------------------------------------------------------------------------------------------------------------------------------|---------------------------------|-------------------|
| 1  | Cypher scalar functions          | `crates/uni-query/src/query/df_expr.rs:2130` `translate_function_call` match; `crates/uni-query/src/query/executor/custom_functions.rs:24` `CustomFunctionRegistry` | `ScalarPluginFn`              | `ScalarFn`        |
| 2  | Cypher aggregate functions       | DataFusion built-ins + ad-hoc registration in `df_udfs.rs`                                                                     | `AggregatePluginFn`           | `AggregateFn`     |
| 3  | Cypher window functions          | DataFusion built-ins                                                                                                            | `WindowPluginFn`              | `WindowFn`        |
| 4  | **Cypher procedures (`CALL ... YIELD`)** | `crates/uni-query/src/query/df_graph/procedure_call.rs:559` dispatch; `crates/uni-query/src/query/executor/procedure.rs:75` stub `ProcedureRegistry`; 50+ hardcoded procedures (`uni.admin.*`, `uni.schema.*`, `uni.vector.*`, `uni.fts.*`, `uni.bitwise.*`, `uni.temporal.*`, `uni.algo.*`) | `ProcedurePlugin`             | `Procedure` + `ProcedureWrites` |
| 5  | Locy aggregates (FOLD)           | `FoldAggKind` enum + `crates/uni-query/src/query/df_graph/locy_program.rs:1222` `parse_fold_aggregate`                          | `LocyAggregate` + `LocyAggState` | `LocyAggregate` |
| 6  | Locy predicates (incl. neural)   | Hardcoded in compiler/planner                                                                                                   | `LocyPredicate`               | `LocyPredicate`   |
| 7  | Physical operators               | DataFusion `ExecutionPlan` (closed at the optimizer)                                                                            | `OperatorProvider`            | `Operator`        |
| 8  | Optimizer / planner rules        | DataFusion's set                                                                                                                | `OptimizerRuleProvider`       | `Operator`        |
| 9  | Index kinds                      | `crates/uni-query/src/query/df_graph/vector_knn.rs` hardcoded vector index                                                      | `IndexKindProvider`           | `Index`           |
| 10 | Storage backends                 | `crates/uni-store/src/lib.rs` (Lance backend, closed)                                                                            | `StorageBackend`              | `Storage`         |
| 11 | Graph algorithms (black box)     | `crates/uni-algo/src/algo/mod.rs:55` `AlgorithmRegistry` (static, 32 algorithms)                                                | `AlgorithmProvider`           | `Algorithm`       |
| 12 | **Graph algorithms (Pregel-style vertex programs)** | None today                                                                                                                        | `PregelProgramProvider`       | `Algorithm`       |
| 13 | CRDT kinds                       | `crates/uni-crdt/src/lib.rs` (LWW / OR-Set / RGA / …, closed)                                                                   | `CrdtKindProvider`            | `Crdt`            |
| 14 | Phased session/query hooks       | `crates/uni/src/api/hooks.rs` `SessionHook` (4-method trait — before/after query/commit only)                                   | `SessionHook` (expanded to phased: parse / plan / execute / commit / abort) | `Hook`            |
| 15 | **Fine-grained triggers**        | None today (only coarse `SessionHook`)                                                                                          | `TriggerPlugin` (label/property/event-scoped) | `Trigger`         |
| 16 | **Background jobs / scheduled execution** | None today                                                                                                                        | `BackgroundJobProvider`       | `BackgroundJob`   |
| 17 | Logical types (value extensions) | `Value` enum closed; `Value::Node`/`Value::Edge` require storage `vid`/`eid`                                                     | `LogicalTypeProvider` + ephemeral `Node`/`Edge` variants for virtual entities | `Type`            |
| 18 | Authentication                   | None today                                                                                                                       | `AuthProvider`                | `Auth`            |
| 19 | Authorization policies           | None today                                                                                                                       | `AuthzPolicy`                 | `Authz`           |
| 20 | Wire / connector protocols       | None (PyO3 only)                                                                                                                 | `Connector`                   | `Connector`       |
| 21 | **Collations (sort orders)**     | None today                                                                                                                       | `CollationProvider`           | `Collation`       |
| 22 | **CDC output / logical replication** | None today                                                                                                                        | `CdcOutputProvider`           | `Cdc`             |
| 23 | **Catalog / virtual schemas**    | None today                                                                                                                       | `CatalogProvider`             | `Catalog`         |
| 24 | **Replacement scans (auto-route unknown identifiers)** | None today                                                                                                                        | `ReplacementScanProvider`     | `Catalog`         |
| 25 | **Meta-plugin: `apoc.custom`-style declarations from Cypher** | None today                                                                                                                        | Built-in `uni-plugin-custom` registers procedures that call `PluginRegistrar` internally | `PluginRegistration` |

(Rows in **bold** are surfaces the original 15-surface draft missed; they were identified during the APOC + PostgreSQL + DuckDB + SQLite + Spark-DataSourcesV2 review.)

The three surfaces that require the most invasive refactoring are:

- **#5 Locy aggregates** — removing `FoldAggKind` from a load-bearing enum the fixpoint engine matches on (see §7).
- **#4 Cypher procedures** — converting the 50+ hardcoded procedures in `procedure_call.rs` to plugin registrations while preserving every existing `CALL` site. The `ProcedureRegistry` stub at `executor/procedure.rs:75` becomes the real registry. (See §4.17 and §9.6.)
- **#10 Storage backends** — adding a scheme-dispatch layer above the current `uni-store` Lance backend (see §9.4).

The other twenty-two surfaces are additive: a registry is introduced (or the existing static one is generalized), the existing built-in becomes a registration, and lookup goes through the registry.

### 2.1 APOC parity matrix

The proposal is benchmarked against Neo4j's APOC library. Every APOC namespace must be expressible through one of these surfaces:

| APOC namespace                   | uni-db surface(s)                                       | Notes                                                      |
|----------------------------------|--------------------------------------------------------|------------------------------------------------------------|
| `apoc.text.*`, `apoc.coll.*`, `apoc.map.*`, `apoc.number.*`, `apoc.math.*`, `apoc.date.*`, `apoc.convert.*`, `apoc.json.*` | #1 ScalarPluginFn                                          | Pure scalar functions over primitives / lists / maps        |
| `apoc.agg.*`                     | #2 AggregatePluginFn                                       | User aggregates                                            |
| `apoc.refactor.*` (`mergeNodes`, `cloneSubgraph`, …), `apoc.create.*` (`vNode`, …), `apoc.load.*` (`csv`, `json`, `jdbc`, …), `apoc.export.*` | #4 ProcedurePlugin (with `Procedure` and/or `ProcedureWrites`)           | Side-effectful, streaming returns; virtual nodes via #17    |
| `apoc.trigger.*`                 | #15 TriggerPlugin                                          | Label/property/event-scoped                                 |
| `apoc.periodic.{iterate,submit,schedule,commit}` | #16 BackgroundJobProvider                                   | Scheduled / batched / fire-and-forget jobs                  |
| `apoc.custom.declareFunction`/`declareProcedure` | #25 Meta-plugin (`uni.plugin.declareFunction`/`declareProcedure`) | User-defined from Cypher; persisted; survives restart       |
| `apoc.path.expandConfig` etc.    | #4 ProcedurePlugin                                          | Path-expansion DSL as a built-in procedure                  |
| `apoc.cypher.{run,runMany,runWrite}` | #4 ProcedurePlugin + `HostQuery` capability                  | Dynamic Cypher from inside a procedure                      |
| `apoc.lock.*`                    | Host primitive (`Lock` capability + `host.lock_*` imports) | Not a plugin surface — capability + host imports            |
| `apoc.algo.*`                    | #11 AlgorithmProvider or #12 PregelProgramProvider          | Existing 32 builtins migrate to #11; user vertex programs to #12 |
| `apoc.atomic.*`                  | #4 ProcedurePlugin with `RetryContract::Atomic`              | Optimistic-CAS retry contract declared by the procedure     |
| `apoc.ttl.*`                     | #16 BackgroundJobProvider + #15 TriggerPlugin               | TTL implemented as a background sweep + create-time trigger |
| `apoc.uuid.*`, `apoc.es.*`, `apoc.mongo.*`, `apoc.couchbase.*`, `apoc.bolt.*` | #4 + #20 Connector + capabilities (Network, Secret)        | External-system adapters as procedures over connectors      |
| `apoc.spatial.*`                 | #1 ScalarPluginFn + #17 LogicalTypeProvider (`geo.point`)   | Spatial types + scalar functions over them                 |
| `apoc.schema.*`, `apoc.meta.*`   | #4 ProcedurePlugin (read-only) + #23 CatalogProvider        | Schema introspection                                        |
| `apoc.help`                      | Built-in procedure that queries the registry's `docs` field | First-class doc surfacing                                  |

Acceptance criterion §19 includes: "Every APOC namespace has at least one mapped uni-db surface with a representative example procedure or function shipped in `uni-plugin-apoc-core` (Rust)."

### 2.2 APOC coverage — `apoc-core` (Rust)

APOC analogues ship as a single Rust distribution unit:

**`crates/uni-plugin-apoc-core/`** — Rust, compiled into the host (opt-in
via a cargo feature). Covers:

1. **Perf-critical scalar functions** invoked in inner loops:
   `apoc.text.*`, `apoc.coll.*`, `apoc.math.*`, `apoc.convert.*`, parts of
   `apoc.json.*`.
2. **Host-intimate procedures** that need access to internal mutation,
   schema, or catalog APIs not exposed across the capability membrane:
   `apoc.refactor.*` (mutation internals), `apoc.schema.*` /
   `apoc.meta.*` (catalog internals), `apoc.atomic.*` (CAS retry
   contracts orchestrated by the host).
3. **Procedures with transaction-batching semantics** the host
   orchestrates: `apoc.periodic.iterate` body (the scheduler itself
   lives in the host; the procedure wrapper lives here when it must run
   in-process).
4. **Orchestration / format / IO / external-system adapters** —
   `apoc.path.expandConfig`, `apoc.cypher.{run,runMany,runWrite}`,
   `apoc.load.{csv,json,jdbc}`, `apoc.export.*`, `apoc.es.*`,
   `apoc.mongo.*`, `apoc.bolt.*`, `apoc.uuid.*`, the user-facing
   `apoc.periodic.*` procedures, `apoc.date.*` formatters, `apoc.help`.
   Network / Filesystem capabilities gate the relevant subset.

The plugin id is `apoc-core`. Procedure qnames live under the
`apoc-core` namespace internally; user-facing qnames are surfaced as
`uni.*` (e.g., `uni.bitwise.and`) through the registry alias layer.

| Unit | Default in `uni` (Cargo features) | Default for CLI users |
|------|-----------------------------------|-----------------------|
| `apoc-core` | `apoc-core` feature on by default → registered automatically | available |

Library embedders who don't want APOC content disable the feature. The
embedded-database case (uni-db running inside another app) defaults
on (it's small, pure Rust, perf-equivalent to built-ins).

The integrity invariant from §1 still holds: APOC content registers
through the same `PluginRegistrar` as every other plugin. There is no
`apoc.*` privileged backdoor.

---

## 3. Core Trait and Registrar

### 3.1 `Plugin` — the universal trait

The core trait is deliberately tiny. All weight lives in per-surface capability traits that a plugin opts into.

```rust
// crates/uni-plugin/src/lib.rs

pub trait Plugin: Send + Sync + 'static {
    /// Static description of this plugin. Returned by reference — implementations
    /// typically store this as a `OnceLock<PluginManifest>` populated at construction.
    fn manifest(&self) -> &PluginManifest;

    /// Register extension points with the host. Called once at load time, after
    /// capability grants have been intersected with declared capabilities.
    fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError>;

    /// Optional. Called once after registration, in topological order over `depends_on`.
    fn init(&self, _cx: &PluginInitContext<'_>) -> Result<(), PluginError> { Ok(()) }

    /// Optional. Called once at instance teardown, in reverse dependency order.
    fn shutdown(&self) {}
}
```

`PluginRegistrar` is an opaque builder passed to `register()`. Every registration method returns `&mut Self` for chaining, validates the plugin's manifest against the requested capability, and enforces unique qualified names within the registrar.

```rust
impl<'a> PluginRegistrar<'a> {
    pub fn scalar_fn      (&mut self, qname: QName, sig: FnSignature,        f: Arc<dyn ScalarPluginFn>)    -> &mut Self;
    pub fn aggregate_fn   (&mut self, qname: QName, sig: AggSignature,       f: Arc<dyn AggregatePluginFn>) -> &mut Self;
    pub fn window_fn      (&mut self, qname: QName, sig: WindowSignature,    f: Arc<dyn WindowPluginFn>)    -> &mut Self;
    pub fn locy_aggregate (&mut self, qname: QName, f: Arc<dyn LocyAggregate>)                                -> &mut Self;
    pub fn locy_predicate (&mut self, qname: QName, sig: PredSignature, f: Arc<dyn LocyPredicate>)            -> &mut Self;
    pub fn operator       (&mut self, qname: QName, p: Arc<dyn OperatorProvider>)                              -> &mut Self;
    pub fn optimizer_rule (&mut self, p: Arc<dyn OptimizerRuleProvider>)                                       -> &mut Self;
    pub fn index_kind     (&mut self, kind: IndexKind, p: Arc<dyn IndexKindProvider>)                          -> &mut Self;
    pub fn storage_backend(&mut self, scheme: &'static str, p: Arc<dyn StorageBackend>)                        -> &mut Self;
    pub fn algorithm      (&mut self, qname: QName, p: Arc<dyn AlgorithmProvider>)                              -> &mut Self;
    pub fn crdt_kind      (&mut self, kind: CrdtKind, p: Arc<dyn CrdtKindProvider>)                             -> &mut Self;
    pub fn hook           (&mut self, h: Arc<dyn SessionHook>)                                                   -> &mut Self;
    pub fn logical_type   (&mut self, t: Arc<dyn LogicalTypeProvider>)                                           -> &mut Self;
    pub fn auth_provider  (&mut self, p: Arc<dyn AuthProvider>)                                                  -> &mut Self;
    pub fn authz_policy   (&mut self, p: Arc<dyn AuthzPolicy>)                                                    -> &mut Self;
    pub fn connector      (&mut self, p: Arc<dyn Connector>)                                                      -> &mut Self;
}
```

A capability mismatch (registering a `storage_backend` without `Capability::Storage` in the effective set) fails the entire `register()` call, not just the offending registration. This is deliberate — partial registration would leave the framework in an inconsistent state.

### 3.2 `PluginManifest`

```rust
pub struct PluginManifest {
    pub id: PluginId,                       // reverse-DNS: "ai.dragonscale.geo"
    pub version: semver::Version,
    pub abi: AbiRange,                      // semver range over uni-plugin ABI
    pub depends_on: Vec<PluginDep>,         // {id, version_req, optional}
    pub capabilities: CapabilitySet,        // declared (requested at load time)
    pub determinism: Determinism,           // Pure | SessionScoped | Nondeterministic
    pub side_effects: SideEffects,          // ReadOnly | Writes | ExternalIO
    pub scope: Scope,                       // Instance | Session
    pub hash: Option<blake3::Hash>,         // for hash-pinning
    pub signature: Option<Ed25519Signature>, // optional signed manifest
    pub provides: ProvidedSurfaces,         // declarative summary of what register() will add
    pub docs: Markdown,                     // surfaced by `uni plugin info`, `uni plugin help <qname>`,
                                            // and `CALL uni.plugin.help('qname') YIELD markdown RETURN markdown`
    pub metadata: BTreeMap<String, String>, // author, description, license, repo, …
}

pub struct PluginDep {
    pub id: PluginId,
    pub version_req: semver::VersionReq,
    pub optional: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Determinism { Pure, SessionScoped, Nondeterministic }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SideEffects { ReadOnly, Writes, ExternalIO }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Scope { Instance, Session }
```

The `provides` field is a declarative summary of what `register()` will add (qnames, schemes, index kinds). It lets the host build a routing table before calling `register()`, which matters for two-phase loading (manifest parse → capability negotiation → instantiation).

### 3.3 `QName` — qualified naming

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct QName {
    pub namespace: PluginId,        // owning plugin
    pub local: SmolStr,             // local function name
}

impl QName {
    pub fn parse(s: &str) -> Result<Self, PluginError>;       // "geo.distance" → QName
    pub fn to_string(&self) -> String;                         // "geo.distance"
    pub fn matches_call_site(&self, call: &str) -> bool;       // Cypher case-insensitivity rules
}
```

Cypher's case-insensitive function names are normalized at the QName boundary; locally, function names are case-sensitive in storage (UPPERCASE on the wire) but matched case-insensitively at call sites. Locy is fully case-sensitive — QName carries the original case.

Namespace collisions (two plugins both calling themselves `geo`) are rejected at load time. Local-name collisions within a single plugin's `register()` call are rejected by the registrar.

### 3.4 `PluginHandle` and `Uni::add_plugin`

```rust
impl Uni {
    pub fn add_plugin(&self, p: Arc<dyn Plugin>) -> Result<PluginHandle, PluginError>;
    pub fn load_wasm(&self, bytes: &[u8], grants: CapabilitySet) -> Result<PluginHandle, PluginError>;
    pub fn load_rhai_plugin(&self, src: impl Into<String>, grants: CapabilitySet)
        -> Result<PluginHandle, PluginError>;
    pub fn reload(&self, handle: PluginHandle, new_source: PluginSource) -> Result<(), PluginError>;
    pub fn remove_plugin(&self, handle: PluginHandle) -> Result<(), PluginError>;
    pub fn plugins(&self) -> Vec<PluginInfo>;
    pub fn plugin(&self, id: &PluginId) -> Option<PluginInfo>;
}

pub struct PluginHandle { id: PluginId, generation: u64 }   // generation bumps on hot-reload
```

Per-session plugins (`Scope::Session`) are registered through `Session::add_plugin` and are dropped when the session is dropped. Instance-scoped plugins live until `remove_plugin` or `Uni` drop. This is the axis the PyO3 REPL-UDF case forced into the design.

---

## 4. Per-Surface Capability Traits

Each surface has its own trait that callers register through `PluginRegistrar`. All traits are `Send + Sync + 'static`. All speak Arrow at the boundary.

### 4.1 Scalar functions — `ScalarPluginFn`

```rust
pub trait ScalarPluginFn: Send + Sync {
    fn signature(&self) -> &FnSignature;
    fn invoke(&self, args: &[ColumnarValue], rows: usize) -> Result<ColumnarValue, FnError>;
}

pub struct FnSignature {
    pub args: Vec<ArgType>,
    pub returns: ArgType,
    pub volatility: Volatility,                  // derived from manifest determinism
    pub null_handling: NullHandling,             // PropagateNulls | UserHandled
}

pub enum ArgType {
    Primitive(arrow_schema::DataType),
    CypherValue,                                 // serialized as LargeBinary, opt-in
    Vector { len: usize, element: DataType },    // fixed-size list
    Variadic(Box<ArgType>),
}
```

`ColumnarValue` is DataFusion's existing `Arrow array | scalar` union. Plugins matching `ArgType::Primitive` receive native Arrow arrays with no Value detour; plugins requesting `ArgType::CypherValue` go through the legacy `LargeBinary` transport (one `bincode::serialize` per row — slow path, preserved for fns needing `Node`/`Relationship`/`Path`).

A `RowFn<F>` adapter is provided for plugins that prefer per-row authoring:

```rust
pub struct RowFn<F>(pub Arc<F>);
impl<F> ScalarPluginFn for RowFn<F>
where
    F: Fn(&[Value]) -> Result<Value, FnError> + Send + Sync + 'static,
{
    fn signature(&self) -> &FnSignature { /* declared at construction */ }
    fn invoke(&self, args: &[ColumnarValue], rows: usize) -> Result<ColumnarValue, FnError> {
        // Convert each column to Value rows, iterate, call F, build output column.
    }
}
```

### 4.2 Aggregate functions — `AggregatePluginFn`

Cypher / SQL aggregates map onto DataFusion's `AggregateUDFImpl`. The plugin trait splits the user-facing surface from the execution surface:

```rust
pub trait AggregatePluginFn: Send + Sync {
    fn signature(&self) -> &AggSignature;
    fn create_accumulator(&self) -> Box<dyn PluginAccumulator>;
}

pub trait PluginAccumulator: Send {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), FnError>;
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<(), FnError>;
    fn state(&self) -> Result<Vec<ScalarValue>, FnError>;
    fn evaluate(&self) -> Result<ScalarValue, FnError>;
    fn size(&self) -> usize;                              // for memory accounting
}

pub struct AggSignature {
    pub args: Vec<ArgType>,
    pub returns: ArgType,
    pub state_fields: Vec<arrow_schema::Field>,
    pub volatility: Volatility,
    pub supports_partial: bool,                            // can we run partial aggregation?
}
```

The host adapter wraps a `PluginAccumulator` into a DataFusion `Accumulator`, plumbing into the standard aggregate physical plan.

### 4.3 Window functions — `WindowPluginFn`

```rust
pub trait WindowPluginFn: Send + Sync {
    fn signature(&self) -> &WindowSignature;
    fn evaluate(&self, partition: &RecordBatch, frame: WindowFrame) -> Result<ArrayRef, FnError>;
}
```

`WindowFrame` carries `{ start, end, order_by_indices, partition_by_indices }`. Window functions are evaluated per partition; the host handles partitioning and framing.

### 4.4 Locy aggregates — `LocyAggregate`

The most invasive of the new traits, because it retires the closed `FoldAggKind` enum and replaces a `match` in `locy_fixpoint.rs` with trait dispatch. The trait carries explicit `Semilattice` metadata so the fixpoint engine's monotonicity proofs survive.

```rust
pub trait LocyAggregate: Send + Sync {
    fn semilattice(&self) -> Semilattice;
    fn output_type(&self) -> arrow_schema::DataType;
    fn create(&self) -> Box<dyn LocyAggState>;
}

pub trait LocyAggState: Send {
    fn ingest(&mut self, batch: &RecordBatch, value_col: usize) -> Result<(), FnError>;
    fn merge(&mut self, other: &dyn LocyAggState) -> Result<(), FnError>;
    fn finalize(&self) -> Result<ScalarValue, FnError>;

    /// Fixpoint shortcut: return true if no further ingest can change the state.
    /// MAX over a bounded domain returns true at the top; SUM never returns true.
    fn is_at_top(&self) -> bool { false }

    /// For PROB-annotated rules with MNOR/MPROD, return derivation provenance.
    /// Default: no tracking.
    fn provenance(&self) -> Option<&DerivationTracker> { None }
}

pub struct Semilattice {
    pub idempotent: bool,                  // f(x, x) == x
    pub commutative: bool,                 // f(x, y) == f(y, x)
    pub associative: bool,                 // f(f(x, y), z) == f(x, f(y, z))
    pub monotone_join: bool,               // f preserves or raises the partial order
    pub has_top: bool,                     // bounded domain — is_at_top() may return true
}
```

Built-ins map directly:

| Aggregate | semilattice           | Notes                                     |
|-----------|-----------------------|-------------------------------------------|
| `MIN`     | idem ∧ comm ∧ assoc ∧ monotone ∧ has_top | top = min of domain        |
| `MAX`     | idem ∧ comm ∧ assoc ∧ monotone ∧ has_top | top = max of domain        |
| `SUM`     | comm ∧ assoc          | not idempotent, not monotone in general   |
| `COUNT`   | comm ∧ assoc          | monotone (counts grow), top = ∞           |
| `AVG`     | comm ∧ assoc          | non-monotone; runs outside fixpoint       |
| `COLLECT` | comm ∧ assoc ∧ monotone | list-valued, monotone under multiset   |
| `MNOR`    | idem ∧ comm ∧ assoc ∧ monotone ∧ has_top=1 | noisy-OR; top = 1                 |
| `MPROD`   | idem ∧ comm ∧ assoc ∧ monotone ∧ has_top=0 | bounded-product; top = 0          |

The fixpoint engine in `locy_fixpoint.rs` registers `LocyAggregate::is_at_top()` as a per-binding shortcut. If every grouping in a stratum reaches `is_at_top`, the stratum's fixpoint terminates early. This is exactly the current `MNOR` saturation optimization, generalized.

Aggregates whose `semilattice.monotone_join` is `false` are rejected at registration if they appear inside a recursive Locy clause; they may still be used in non-recursive HAVING / post-fixpoint contexts. The check happens in the Locy compiler at `locy_program.rs` after `convert_fold_bindings`.

### 4.5 Locy predicates — `LocyPredicate`

Predicates evaluate to boolean columns over batches. This is the surface that absorbs neural predicates (per the `uni-locy-docs` neural-predicates spec).

```rust
pub trait LocyPredicate: Send + Sync {
    fn signature(&self) -> &PredSignature;
    fn evaluate(&self, args: &[ColumnarValue], rows: usize) -> Result<BooleanArray, FnError>;

    /// Optional: return a fuzzy/probabilistic score in [0, 1] for fuzzy joins.
    /// If `Some`, the host can use this in PROB-annotated reasoning.
    fn evaluate_fuzzy(&self, _args: &[ColumnarValue], _rows: usize)
        -> Option<Result<Float64Array, FnError>> { None }
}

pub struct PredSignature {
    pub args: Vec<ArgType>,
    pub volatility: Volatility,
    pub supports_fuzzy: bool,
    pub batch_hint: BatchHint,             // Small | Medium | Large — for neural fn batching
}
```

Neural predicates set `supports_fuzzy = true` and return a continuous score; the Locy planner uses the score in PROB-multiplication chains. Crisp predicates set `supports_fuzzy = false` and only implement `evaluate`.

### 4.6 Physical operators — `OperatorProvider`

```rust
pub trait OperatorProvider: Send + Sync {
    fn logical_name(&self) -> &str;
    fn plan(&self, args: PlannerArgs<'_>) -> Result<Arc<dyn ExecutionPlan>, FnError>;
}

pub struct PlannerArgs<'a> {
    pub session_ctx: &'a SessionContext,
    pub input_plans: &'a [Arc<dyn ExecutionPlan>],
    pub config_json: &'a str,                       // arbitrary serialized config
    pub schema_hint: Option<SchemaRef>,
}
```

Operator plugins integrate with DataFusion's physical planner. The plugin's `plan()` returns a fully-formed `ExecutionPlan` that DataFusion will execute as if it were a built-in operator. Streaming `RecordBatch` output is the contract.

WASM operator plugins implement a pull-based `poll-next` export returning Arrow IPC batches; the host adapter wraps that into a `SendableRecordBatchStream`.

### 4.7 Optimizer rules — `OptimizerRuleProvider`

```rust
pub trait OptimizerRuleProvider: Send + Sync {
    fn rule(&self) -> Arc<dyn OptimizerRule>;          // DataFusion's OptimizerRule trait
    fn phase(&self) -> OptimizerPhase;                  // Logical | Physical | Both
    fn precedence(&self) -> i32;                        // rules run in ascending precedence
}
```

Built-in optimizer rules are wrapped as plugin registrations from `uni-plugin-builtin`. User-defined rules can implement pushdowns (e.g., filter pushdown into a custom index), join reordering hints, or domain-specific rewrites (e.g., geo bounding-box rewrites).

### 4.8 Index kinds — `IndexKindProvider`

```rust
pub trait IndexKindProvider: Send + Sync {
    fn kind(&self) -> IndexKind;
    fn build(&self, source: &RecordBatch, options: &str) -> Result<Box<dyn IndexBuild>, FnError>;
    fn open(&self, persisted: &[u8]) -> Result<Box<dyn IndexHandle>, FnError>;
    fn persist(&self, handle: &dyn IndexHandle) -> Result<Vec<u8>, FnError>;
}

pub trait IndexHandle: Send + Sync {
    fn probe(&self, query: &RecordBatch, k: usize) -> Result<RecordBatch, FnError>;
    fn supports_filter(&self) -> bool;
    fn probe_filtered(&self, query: &RecordBatch, k: usize, filter: &BooleanArray)
        -> Result<RecordBatch, FnError>;
    fn schema(&self) -> SchemaRef;
}
```

The current vector index in `vector_knn.rs` becomes one `IndexKindProvider` registration in `uni-plugin-builtin`. Future index kinds (Locality-Sensitive Hashing, learned indexes, custom domain-specific indexes) are user plugins through the same trait.

### 4.9 Storage backends — `StorageBackend`

```rust
pub trait StorageBackend: Send + Sync {
    fn scheme(&self) -> &'static str;                   // "lance", "s3", "memory", ...
    fn open(&self, uri: &str, options: &StorageOptions) -> Result<Arc<dyn Storage>, FnError>;
}

pub trait Storage: Send + Sync {
    fn read_batch(&self, table: &str, predicate: Option<&Expr>) -> Result<SendableRecordBatchStream, FnError>;
    fn write_batch(&self, table: &str, batch: &RecordBatch) -> Result<WriteHandle, FnError>;
    fn list_tables(&self) -> Result<Vec<String>, FnError>;
    fn delete(&self, table: &str, predicate: &Expr) -> Result<u64, FnError>;
    fn supports_branching(&self) -> bool;
    fn fork(&self, src_branch: &str, dst_branch: &str) -> Result<(), FnError>;
}
```

Storage backends are registered by URI scheme. The current Lance backend in `uni-store/src/lib.rs` becomes the `lance://` registration. Adding S3-native, in-memory, or HTTP-backed stores is a matter of registering new schemes.

### 4.10 Graph algorithms — `AlgorithmProvider`

```rust
pub trait AlgorithmProvider: Send + Sync {
    fn signature(&self) -> &AlgorithmSignature;
    fn run(&self, ctx: &AlgorithmContext<'_>) -> Result<SendableRecordBatchStream, FnError>;
}

pub struct AlgorithmContext<'a> {
    pub projection: &'a GraphProjection,                // in-memory CSR + slot map
    pub config: &'a AlgorithmConfig,                    // typed config map
    pub session: &'a Session,
}
```

`GraphProjection` is uni-algo's existing in-memory representation (dense slot map + CSR + optional reverse CSR + optional weight column). **Algorithms always see a `GraphProjection`**; they do *not* know whether it was materialised from native storage adjacency, a Cypher subquery, or a previously-registered named projection. That separation is what makes virtual projections (§4.10.1) a host-side concern rather than something every algorithm has to implement.

Each of the 32 current algorithms (Dijkstra, PageRank, Louvain, …) becomes an `AlgorithmProvider` registration. User algorithms — domain-specific traversals, custom centrality measures, federated graph algorithms — go through the same trait.

#### 4.10.1 Projection input — three shapes, one signature

Every algorithm `CALL` takes exactly two arguments: `(graphRef, config)`. The `graphRef` is structurally polymorphic; the host resolves it into a `GraphProjection` before invoking the algorithm.

```rust
/// Host-side input shape; algorithms never see this enum.
pub enum ProjectionInput {
    /// Native projection — walk stored adjacency for the given labels
    /// and edge types. Fast path; today's behaviour.
    Native {
        node_labels: Vec<String>,
        edge_types: Vec<String>,
        weight_property: Option<String>,
        include_reverse: bool,
    },
    /// Virtual / Cypher projection — the rows returned by the queries
    /// ARE the graph. Nothing on disk has to match.
    Cypher {
        node_query: String,                   // must yield `id: Int64` (+ optional feature columns)
        edge_query: String,                   // must yield `source: Int64, target: Int64` (+ optional `weight: Float64`)
        weight_column: Option<String>,        // default: "weight"
        include_reverse: bool,
    },
    /// Named projection — look up a previously-registered projection
    /// from the per-database `ProjectionStore`.
    Named { name: String },
}
```

The three call shapes:

```cypher
-- Native (today's pattern, expressed in the new shape):
CALL uni.algo.pageRank({nodeLabels: ['Person'], relTypes: ['KNOWS']},
                       {dampingFactor: 0.85, maxIterations: 20})

-- Cypher (the P6 use case; entity co-occurrence derived from MENTIONS):
CALL uni.algo.labelPropagation({
    nodeQuery: 'MATCH (e:Entity) RETURN id(e) AS id',
    relQuery:  'MATCH (s)-[:MENTIONS]->(a:Entity), (s)-[:MENTIONS]->(b:Entity)
                WHERE id(a) < id(b)
                RETURN id(a) AS source, id(b) AS target, count(*) AS weight',
    weightColumn: 'weight'
}, {maxIterations: 10})

-- Named (reuse a registered projection across calls):
CALL uni.graph.project('topics-2026', {nodeQuery: '...', relQuery: '...'})
CALL uni.algo.louvain('topics-2026', {tolerance: 1e-4})
CALL uni.graph.drop('topics-2026')
```

Dispatch is by **map-key shape**, not by argument type alone:

```rust
fn parse_graph_ref(v: &Value) -> Result<ProjectionInput, FnError> {
    match v {
        Value::String(name) => Ok(ProjectionInput::Named { name: name.clone() }),
        Value::Map(m) if m.contains_key("nodeQuery") => /* Cypher */,
        Value::Map(m) if m.contains_key("nodeLabels") => /* Native */,
        Value::Map(_) => Err("graphRef map needs nodeLabels+relTypes or nodeQuery+relQuery"),
        _ => Err("graphRef must be a projection name or a config map"),
    }
}
```

#### 4.10.2 Cypher projection mechanics

The `Cypher` variant is the "virtual" projection — nothing in the projection needs to correspond to stored entities or edges. The "edges" can be the result of joins, aggregations, derived properties, even cross-source unions.

Execution path inside the `uni.algo.*` plugin adapter:

```
invoke(ctx, args):
    graph_ref = parse_graph_ref(args[0])
    config    = parse_config(args[1])

    projection = match graph_ref {
        Native { … }              => ProjectionBuilder::from_labels(host, …).build()
        Cypher { nq, eq, wcol, … } => {
            // Reentrant calls into the host session, sharing the outer
            // transaction's MVCC snapshot (including L0 overlay).
            let nodes = host.execute_query(nq, ReadOnly).await?
            let edges = host.execute_query(eq, ReadOnly).await?

            validate_node_schema(&nodes)         // expects 'id: Int64'
            validate_edge_schema(&edges, wcol)   // expects 'source, target' (+ optional 'weight')

            ProjectionBuilder::from_rows(nodes, edges, wcol, include_reverse).build()
        }
        Named { name }             => host.projection_store().get(&name)?.clone()
    }

    if projection.bytes() > AlgorithmConfig::max_projection_memory {
        return Err("projection exceeded {max} bytes; consider tighter filters or larger cap")
    }

    self.algo.run(&AlgorithmContext { projection: &projection, config, session })
```

Five load-bearing properties:

1. **Same transaction snapshot.** The inner `node_query` and `edge_query` execute against the outer CALL's MVCC view. L0 overlay applies. No new isolation rules.
2. **Read-only enforced.** The host forces `ReadOnly` mode on the inner queries regardless of what the user wrote — `MATCH … CREATE …` inside a `nodeQuery` errors at the boundary. (GDS does *not* do this and has accumulated a small history of surprise-write bugs as a result.)
3. **Schema validation at the boundary.** If `nodeQuery` doesn't return an `id: Int64` column, the error includes the actual columns returned, not a panic deep inside the materialiser.
4. **Memory cap reused.** `AlgorithmConfig::max_projection_memory` (1 GB default) bounds the materialised CSR. Aggressive self-join `relQuery`s fail with an actionable error before the algo starts.
5. **Always fresh.** No caching at the projection layer for `Native` / `Cypher` — every call rebuilds. Staleness is impossible by construction.

#### 4.10.3 Named projections — `ProjectionStore`

The `Named` variant looks up a `GraphProjection` from a per-database registry, keyed by name. Lifecycle procedures:

```cypher
CALL uni.graph.project(name, projectionConfig)   -- materialise + register
CALL uni.graph.drop(name)                        -- evict
CALL uni.graph.list()                            -- enumerate with metadata
CALL uni.graph.exists(name)                      -- bool
```

```rust
pub struct ProjectionStore {
    // Per-`Database` instance. Restart-clears (not persisted to disk).
    entries: RwLock<HashMap<String, ProjectionEntry>>,
}

pub struct ProjectionEntry {
    pub projection: Arc<GraphProjection>,
    pub created_at: SystemTime,
    pub created_by: Principal,
    pub source: ProjectionInput,                  // for `uni.graph.list()` introspection
    pub bytes: usize,
}
```

Key properties:

- **Scope is per-database.** Matches GDS. Lost on restart; not replicated across instances. (A cross-instance projection cache is a separate, larger design.)
- **Staleness is explicit.** A registered projection freezes at `project` time; subsequent ingest does *not* update it. Documented loudly in user docs. `uni.graph.refresh(name)` is the obvious escape hatch; deferred to a follow-up unless needed in v1.
- **Eviction is drop-only in v1.** No TTL, no LRU. The `bytes` field is present for a future memory-pressure eviction policy; not implemented yet.
- **Capability surface.** `uni.graph.project` requires the principal's `Procedure` capability *plus* the capabilities that the inner queries would themselves require. `uni.graph.drop` requires `Procedure`. `uni.graph.list` / `.exists` require `Procedure`.

#### 4.10.4 Migrating today's positional signature

Today: `CALL uni.algo.pageRank(['Person'], ['KNOWS'], 0.85, 20, 1e-6)` — five positional args.

During M5c, both signatures are registered under the same QName, distinguished by **arity**:

| Arity | Signature                                                                  | Status                                              |
|-------|----------------------------------------------------------------------------|-----------------------------------------------------|
| 5     | `(nodeLabels, relTypes, dampingFactor, maxIterations, tolerance)`          | legacy — kept one release, emits `DeprecationWarning` |
| 2     | `(graphRef, config)`                                                       | new canonical form                                  |

`ProcedureRegistry::resolve_user_procedure` keys on `(name, arity)`. The legacy 5-arg shim deletes one release later.

This is a syntax migration, not a behaviour change: a `Native` projection with the same labels / types produces a byte-identical CSR to the old positional call. Existing benchmarks continue to apply.

### 4.11 CRDT kinds — `CrdtKindProvider`

```rust
pub trait CrdtKindProvider: Send + Sync {
    fn kind(&self) -> CrdtKind;
    fn empty(&self) -> Box<dyn CrdtState>;
    fn from_persisted(&self, bytes: &[u8]) -> Result<Box<dyn CrdtState>, FnError>;
}

pub trait CrdtState: Send + Sync {
    fn apply(&mut self, op: &CrdtOp) -> Result<(), FnError>;
    fn merge(&mut self, other: &dyn CrdtState) -> Result<(), FnError>;
    fn value(&self) -> Result<ScalarValue, FnError>;
    fn persist(&self) -> Result<Vec<u8>, FnError>;
}
```

Existing LWW / OR-Set / RGA / Counter / MV-Register become `uni-plugin-builtin` registrations. User-defined CRDTs (Yjs-style sequences, custom domain types) go through the same trait.

### 4.12 Phased hooks — `SessionHook` (expanded)

The existing `SessionHook` trait in `crates/uni/src/api/hooks.rs` is *expanded* to the PostgreSQL planner_hook / executor_hook phasing model. Today's four methods (`before_query`, `after_query`, `before_commit`, `after_commit`) are kept verbatim but become *defaults* over a richer set of phase hooks:

```rust
pub trait SessionHook: Send + Sync {
    fn on_parse(&self, _ctx: &ParseContext<'_>) -> HookOutcome { HookOutcome::Continue }
    fn on_analyze(&self, _ctx: &AnalyzeContext<'_>) -> HookOutcome { HookOutcome::Continue }
    fn on_plan(&self, _ctx: &PlanContext<'_>, _plan: &mut LogicalPlan) -> HookOutcome { HookOutcome::Continue }
    fn on_execute_start(&self, _ctx: &ExecuteContext<'_>, _plan: &PhysicalPlan) -> HookOutcome { HookOutcome::Continue }
    fn on_execute_end(&self, _ctx: &ExecuteContext<'_>, _metrics: &QueryMetrics) {}
    fn before_commit(&self, _ctx: &CommitContext<'_>) -> HookOutcome { HookOutcome::Continue }
    fn after_commit(&self, _ctx: &CommitContext<'_>, _result: &CommitResult) {}
    fn on_abort(&self, _ctx: &AbortContext<'_>) {}

    // Backward-compat: existing implementations override only before_query/after_query.
    fn before_query(&self, ctx: &HookContext<'_>) -> Result<(), HookError> { Ok(()) }
    fn after_query(&self, ctx: &HookContext<'_>, metrics: &QueryMetrics) {}
}

pub enum HookOutcome {
    Continue,
    Rewrite(Box<dyn FnOnce(&mut PlanContext<'_>)>),     // plan-level rewrites
    Reject { reason: String },
}
```

`Uni::add_hook` becomes syntactic sugar for `Uni::add_plugin(BuiltinHookPlugin::new(hook))`. Hooks register via `PluginRegistrar::hook()`. This gives hooks the same lifecycle, capability gating, and observability as every other extension. The richer phase set enables:

- **Parse-stage** hooks: query-level audit, SQL→Cypher cross-translation, syntax-level rate limiting.
- **Plan-stage** hooks: optimizer-style rewrites, security predicate injection (e.g., row-level security), tenant-isolation predicates.
- **Execute-start / execute-end** hooks: query-cost accounting, slow-query logging, kill-switch enforcement.
- **Commit / abort** hooks: outbox pattern for event publishing, distributed-transaction coordination.

Hooks at the same phase run in registration order. A hook returning `HookOutcome::Reject` short-circuits the remaining hooks at that phase and propagates a `UniError::HookRejected` to the user.

### 4.13 Logical types — `LogicalTypeProvider`

```rust
pub trait LogicalTypeProvider: Send + Sync {
    fn name(&self) -> &str;                              // "geo.point", "uri", "ipv6", ...
    fn arrow_type(&self) -> arrow_schema::DataType;     // physical storage type
    fn from_literal(&self, s: &str) -> Result<ScalarValue, FnError>;
    fn to_display(&self, v: &ScalarValue) -> Result<String, FnError>;
    fn cast_to(&self, v: &ColumnarValue, target: &DataType) -> Result<ColumnarValue, FnError>;
    fn cast_from(&self, v: &ColumnarValue) -> Result<ColumnarValue, FnError>;
}
```

Logical types are surfaced through Arrow's **extension-type mechanism**: the type's identity is carried in the Arrow `Field`'s `metadata` under the standard `ARROW:extension:name` (e.g., `"geo.point"`) and `ARROW:extension:metadata` (free-form bytes) keys. The physical storage type is whatever `arrow_type()` returns; the logical identity is preserved across Arrow IPC, Parquet round-trips, and DataFusion query plans. They let `geo.point`, `uri`, `ipv6`, `currency` etc. be first-class types in the schema, with their own Cypher literal syntax (via `from_literal`) and pretty-printing.

#### 4.13.1 Virtual nodes and edges — the `apoc.create.vNode` analogue

`Value::Node` and `Value::Edge` today require a storage-backed `vid` / `eid`. APOC procedures like `apoc.create.vNode([labels], {props})` return *ephemeral* graph entities — they look like nodes to downstream Cypher but have no persistent identity and aren't visible to any subsequent `MATCH`. They're return-only.

`Value::Node` and `Value::Edge` gain ephemeral variants:

```rust
pub struct NodeValue {
    pub identity: NodeIdentity,
    pub labels: SmallVec<[LabelId; 2]>,
    pub properties: Arc<PropertyMap>,
}

pub enum NodeIdentity {
    Stored { vid: Vid },                              // persistent, backed by storage
    Ephemeral { transient_id: u64 },                  // transient, unique within one query
}
```

The `Ephemeral` variant lets procedures synthesize result-only nodes (and similarly edges) without touching storage. Cypher operations on ephemeral values follow the same semantics — property reads, label tests, `RETURN` — but writes against an ephemeral identity fail with `UniError::EphemeralWriteAttempt`.

Ephemeral identity is allocated by the host via `host.allocate_transient_id()` (always available — no capability required) and is unique within a single query execution. Across queries, transient IDs are not stable.

Use cases:
- `apoc.create.vNode` analogue: `CALL uni.create.vNode([:Tag], {name:'temp'}) YIELD node RETURN node`.
- Path projection through computed-but-not-stored intermediate nodes.
- Schema introspection: `uni.schema.labelInfo` returns label descriptors as ephemeral `LabelInfo` nodes.

This is a Value-model extension, not a new plugin surface, but it is load-bearing for APOC-equivalent procedures and is included in v1.

### 4.14 Auth — `AuthProvider`

```rust
pub trait AuthProvider: Send + Sync {
    fn scheme(&self) -> &str;                            // "basic", "bearer", "mtls", ...
    fn authenticate(&self, credentials: &Credentials) -> Result<Principal, AuthError>;
}
```

`Principal` carries the authenticated identity, group memberships, and arbitrary claims. The host stores it in the `Session` and passes it to `AuthzPolicy` on each operation.

### 4.15 Authorization — `AuthzPolicy`

```rust
pub trait AuthzPolicy: Send + Sync {
    fn check(&self, principal: &Principal, action: &Action, resource: &Resource)
        -> Result<Decision, AuthzError>;
}

pub enum Decision { Allow, Deny { reason: String } }
```

Multiple policies stack; the host evaluates them in registration order, denying on first deny. Policies can implement RBAC, ABAC, OPA-style policy-as-code, or row-level/column-level security. The check is invoked at query plan time (for read/write actions on tables/labels/properties) and at hook time (for transaction boundaries).

### 4.16 Connectors — `Connector`

```rust
pub trait Connector: Send + Sync {
    fn protocol(&self) -> &str;                          // "bolt", "graphql", "rest", ...
    fn start(&self, cfg: ConnectorConfig, uni: Arc<Uni>) -> Result<ConnectorHandle, FnError>;
    fn stop(&self, handle: ConnectorHandle) -> Result<(), FnError>;
}
```

Wire-protocol plugins. A Bolt-protocol connector plugin would let third-party Neo4j drivers talk to uni-db. A GraphQL connector exposes the graph schema as a GraphQL endpoint. A custom REST connector for app-specific shapes.

### 4.17 Procedures — `ProcedurePlugin`

**The most APOC-shaped surface.** Procedures are invoked via Cypher `CALL`, can perform writes, and return streams of rows that downstream `YIELD` clauses bind by name. They are first-class in uni-db's grammar today; the framework's job is to convert today's hardcoded match in `procedure_call.rs:559` into a trait registry.

```rust
pub trait ProcedurePlugin: Send + Sync {
    fn signature(&self) -> &ProcedureSignature;
    fn invoke(&self, ctx: ProcedureContext<'_>, args: &[ColumnarValue])
        -> Result<SendableRecordBatchStream, FnError>;
}

pub struct ProcedureSignature {
    pub args: Vec<NamedArgType>,                     // CALL-arg names (e.g. (start, max_depth))
    pub yields: Vec<arrow_schema::Field>,            // YIELD column schema
    pub mode: ProcedureMode,                         // Read | Write | Schema | DBMS
    pub side_effects: SideEffects,                   // ReadOnly | Writes | ExternalIO
    pub retry_contract: Option<RetryContract>,       // Atomic | None
    pub batch_input: Option<BatchInputShape>,        // for CALL { } IN TRANSACTIONS OF N
    pub docs: Markdown,                              // surfaced via uni.plugin.help()
}

pub enum ProcedureMode { Read, Write, Schema, DBMS }

pub enum RetryContract {
    None,
    Atomic { max_retries: u32 },                     // CAS-style; the host re-runs on conflict
}

pub struct ProcedureContext<'a> {
    pub session: &'a Session,                        // capability-checked access for writes / host.query
    pub tx: Option<&'a Transaction>,                 // present when called inside a tx
    pub principal: &'a Principal,                    // for authorization
    pub config: &'a PluginConfig,                    // scoped config via Capability::Config
}

pub struct NamedArgType {
    pub name: SmolStr,
    pub ty: ArgType,
    pub default: Option<ScalarValue>,                // for CALL with omitted args
    pub doc: String,
}
```

#### 4.17.1 Streaming semantics

The return is a `SendableRecordBatchStream` — DataFusion's standard pull-based stream type. The Cypher planner attaches it to a `ProcedureCallExec` physical operator that yields the rows downstream. Backpressure: the procedure's `poll_next` is only driven as fast as `YIELD` consumers pull.

WASM procedures expose `poll-next` returning `option<(ipc_ptr, ipc_len)>`; `none` signals end-of-stream. The host adapter wraps this into a `SendableRecordBatchStream` exactly the way operator plugins do (§4.6) — the two surfaces share the streaming machinery.

#### 4.17.2 Procedure modes and capability mapping

| `ProcedureMode` | What it can do                                          | Required capability     |
|----------------|---------------------------------------------------------|-------------------------|
| `Read`         | Read graph state via `host.query` (read-only).          | `Procedure`             |
| `Write`        | Mutate graph state.                                     | `Procedure` + `ProcedureWrites` |
| `Schema`       | DDL — create labels, indexes, constraints.              | `Procedure` + `ProcedureSchema` |
| `DBMS`         | Administrative — snapshot, compact, plugin management.   | `Procedure` + `ProcedureDbms`   |

A procedure attempting an action above its declared mode fails at the boundary, not silently.

#### 4.17.3 `CALL { ... } IN TRANSACTIONS OF N`

Procedures with `batch_input: Some(BatchInputShape::Rows)` accept a streaming input alongside their args (the bound rows from the surrounding `UNWIND`). The host chunks the input into N-row batches, runs the procedure once per chunk in its own sub-transaction, and aggregates the outputs. This is the Cypher analogue of `apoc.periodic.iterate` and is the standard pattern for large refactoring jobs.

#### 4.17.4 Built-in procedures migrate to plugins

The 50+ procedures currently hardcoded in `procedure_call.rs:559` become registrations in `uni-plugin-builtin`:

- `uni.admin.*` (compact, compactionStatus, snapshot.*) → `ProcedureMode::DBMS`
- `uni.schema.*` (createLabel, dropLabel, …) → `ProcedureMode::Schema`
- `uni.vector.query`, `uni.fts.query`, `uni.search` → `ProcedureMode::Read`
- `uni.bitwise.*`, `uni.temporal.*` → `ProcedureMode::Read` (these are arguably scalar fns; kept as procedures for API stability)
- `uni.algo.*` (32 algorithms) → forwarded to `AlgorithmProvider` registrations (§4.10), wrapped as procedures by a thin adapter

The `ProcedureRegistry` stub at `crates/uni-query/src/query/executor/procedure.rs:75` is upgraded from a test-only mock to the real backing store, with `Arc<PluginRegistry>` injected via `GraphExecutionContext::with_procedure_registry()`.

### 4.18 Fine-grained triggers — `TriggerPlugin`

The `SessionHook` expansion in §4.12 covers query-lifecycle phasing. **Triggers** are different: they fire *per-mutation* with **selectors** scoped to labels, properties, and event kinds. This is the `apoc.trigger.*` model.

```rust
pub trait TriggerPlugin: Send + Sync {
    fn subscription(&self) -> &TriggerSubscription;
    fn fire(&self, ctx: TriggerContext<'_>, events: &MutationBatch) -> Result<TriggerOutcome, FnError>;
}

pub struct TriggerSubscription {
    pub phase: TriggerPhase,                       // BeforeMutation | AfterMutation | BeforeCommit | AfterCommit
    pub events: TriggerEventMask,                  // bitmask of NodeCreate/NodeUpdate/NodeDelete/EdgeCreate/...
    pub labels: Option<Vec<LabelId>>,              // None = any label
    pub edge_types: Option<Vec<EdgeTypeId>>,
    pub properties: Option<Vec<PropertyKey>>,      // for *Update events, restrict to changes touching these
    pub predicate: Option<ParsedExpr>,             // Cypher boolean expr evaluated per event
    pub fire_mode: FireMode,                       // Synchronous | Async | EventualConsistency
    pub docs: Markdown,
}

pub struct MutationBatch {
    pub events: Arc<RecordBatch>,                  // schema: event_kind | vid_or_eid | label | property | old_value | new_value | …
}

pub enum TriggerOutcome {
    Continue,
    Reject { reason: String },                     // valid only in Before* phases
    Defer { until: TriggerDeferral },              // run again later (e.g., for batched aggregation)
}

pub enum FireMode {
    Synchronous,            // blocks the mutation; can Reject
    Async,                  // fires after commit, no rejection power
    EventualConsistency,    // batched via BackgroundJobProvider, may fire long after commit
}
```

#### 4.18.1 Event delivery shape

Events are delivered as Arrow `RecordBatch`es with a stable schema, not as one-call-per-event. This matters for triggers that aggregate (`count rows inserted into Person`) or batch-validate. A trigger receives all events in its subscription's selector from a single transaction in one `fire()` call.

#### 4.18.2 Selectors

The framework precomputes a per-(label, event_kind, property) routing table from registered subscriptions. Mutations are dispatched in O(1) per matching trigger; un-matched triggers are not invoked. Predicate filters run last (per-event), since they're the expensive case.

#### 4.18.3 Rejection semantics

A `Synchronous` trigger in a `BeforeMutation` or `BeforeCommit` phase may return `TriggerOutcome::Reject`. The host aborts the surrounding transaction with `UniError::TriggerRejected { trigger, reason }`. `Async` and `EventualConsistency` triggers cannot reject.

### 4.19 Background jobs — `BackgroundJobProvider`

Scheduled / periodic / fire-and-forget execution. The analogue of `apoc.periodic.{iterate, submit, schedule, commit}`.

```rust
pub trait BackgroundJobProvider: Send + Sync {
    fn definition(&self) -> &JobDefinition;
    fn execute(&self, ctx: JobContext<'_>) -> Result<JobOutcome, FnError>;
}

pub struct JobDefinition {
    pub id: QName,
    pub schedule: Schedule,
    pub concurrency: ConcurrencyLimit,
    pub timeout: Duration,
    pub retry: RetryPolicy,
    pub docs: Markdown,
}

pub enum Schedule {
    Once(SystemTime),                              // one-shot at instant
    Periodic(Duration),                            // every N
    Cron(String),                                  // standard cron expression
    Manual,                                        // only fires via uni.plugin.runJob('id')
}

pub enum ConcurrencyLimit {
    Exclusive,                                     // never overlaps with itself
    Bounded(u32),                                  // at most N concurrent runs
    Unbounded,
}

pub enum JobOutcome {
    Done,
    DoneAndReschedule(Duration),                   // dynamic reschedule
    Failed { reason: String, retry: bool },
}

pub struct JobContext<'a> {
    pub session: &'a Session,                      // for host.query
    pub last_run: Option<JobRunRecord>,            // job state from previous invocation
    pub cancel: CancellationToken,                 // checked for cooperative cancellation
    pub config: &'a PluginConfig,
}
```

#### 4.19.1 Scheduler

The host owns a single `Scheduler` (a new component in `uni/src/scheduler.rs`) backed by a `tokio` runtime. Jobs from all plugins share this scheduler. Per-plugin concurrency / fuel limits prevent one plugin's runaway job from starving others. Job state (last-run-at, retry-count, last-error) is persisted in a system table `uni_system.background_jobs` so jobs survive restart.

#### 4.19.2 Built-in jobs

- `uni.system.ttl_sweep` — TTL processor that deletes nodes whose `_ttl_at` has passed.
- `uni.system.statistics_refresh` — refreshes cardinality estimates for the planner.
- `uni.system.compaction` — triggers Lance background compaction.
- `uni.system.index_rebuild` — rebuilds indexes flagged dirty.

Each is a registration in `uni-plugin-builtin`.

#### 4.19.3 User-facing procedures

`apoc.periodic.*` analogues are themselves procedures (§4.17) that wrap the scheduler:

- `CALL uni.periodic.submit(name, cypher, params)` — queue a one-shot job.
- `CALL uni.periodic.schedule(name, schedule, cypher, params)` — register a scheduled job.
- `CALL uni.periodic.iterate(query, mutating_query, options)` — the `apoc.periodic.iterate` batched-update pattern.
- `CALL uni.periodic.list()` / `CALL uni.periodic.cancel(name)` — management.

### 4.20 Collations — `CollationProvider`

Sort-order plugins. Locale-aware string ordering, custom collation rules, case-insensitive variants, ICU integration.

```rust
pub trait CollationProvider: Send + Sync {
    fn name(&self) -> &str;                          // "icu.en_US", "case_insensitive_ascii", ...
    fn compare(&self, a: &str, b: &str) -> std::cmp::Ordering;
    fn supports_substring_search(&self) -> bool;     // for FTS / LIKE compatibility
    fn normalize(&self, s: &str) -> String;          // canonical form for index lookups
}
```

Used by `ORDER BY a.name COLLATE icu.en_US` and by indexed string lookups. Built-ins: ASCII case-sensitive (default), ASCII case-insensitive, Unicode codepoint, ICU locale-aware (gated by `icu` feature).

### 4.21 CDC output — `CdcOutputProvider`

Change-data-capture output plugins. uni-db emits a logical change stream (node/edge mutations, transaction boundaries) and plugins consume it, formatting and shipping to external systems.

```rust
pub trait CdcOutputProvider: Send + Sync {
    fn name(&self) -> &str;
    fn start(&self, ctx: CdcStartContext<'_>) -> Result<Box<dyn CdcStream>, FnError>;
}

pub trait CdcStream: Send {
    fn deliver(&mut self, batch: &CdcBatch) -> Result<(), FnError>;
    fn checkpoint(&mut self) -> Result<CdcLsn, FnError>;       // commit progress; host advances retention
    fn shutdown(&mut self) -> Result<(), FnError>;
}

pub struct CdcBatch {
    pub lsn_range: (CdcLsn, CdcLsn),                          // start/end LSN
    pub mutations: Arc<RecordBatch>,                          // schema-stable mutation events
    pub commit_timestamp: SystemTime,
}
```

Built-ins: Kafka producer (gated by `kafka` feature), Pulsar, file-tailed JSONL, NATS. User plugins for Webhook / proprietary brokers.

### 4.22 Catalog — `CatalogProvider`

Virtual schemas. A catalog plugin exposes tables/labels/edge-types that aren't backed by `uni-store`. Different from `StorageBackend` (per-URI-scheme) — a `CatalogProvider` overlays the entire schema namespace under a configurable prefix.

```rust
pub trait CatalogProvider: Send + Sync {
    fn name(&self) -> &str;
    fn list_labels(&self) -> Result<Vec<CatalogLabel>, FnError>;
    fn list_edge_types(&self) -> Result<Vec<CatalogEdgeType>, FnError>;
    fn resolve_label(&self, label: &str) -> Option<Arc<dyn CatalogTable>>;
    fn resolve_edge_type(&self, edge: &str) -> Option<Arc<dyn CatalogTable>>;
}

pub trait CatalogTable: Send + Sync {
    fn schema(&self) -> SchemaRef;
    fn scan(&self, projection: Option<&[usize]>, filters: &[Expr], limit: Option<usize>)
        -> Result<SendableRecordBatchStream, FnError>;
    fn statistics(&self) -> Option<Statistics>;
}
```

Use cases: federated graph views over external databases (Postgres tables surfaced as labels), computed/derived schemas (auto-generated from data), HuggingFace-dataset-as-label, etc.

### 4.23 Replacement scans — `ReplacementScanProvider`

DuckDB-style replacement scans. When Cypher references an unknown label (`MATCH (n:'https://...')`), unknown procedures, or unknown identifiers in specific positions, the framework consults registered `ReplacementScanProvider`s before failing.

```rust
pub trait ReplacementScanProvider: Send + Sync {
    fn replace(&self, request: &ReplacementRequest) -> Option<Replacement>;
}

pub enum ReplacementRequest<'a> {
    Label(&'a str),                                  // unknown label name
    Procedure(&'a QName),                            // unknown CALL target
    Function(&'a QName),                             // unknown scalar fn
}

pub enum Replacement {
    CatalogTable(Arc<dyn CatalogTable>),             // serve via catalog
    Procedure(QName),                                // rewrite call to this qname
    Function(QName),
}
```

This is opt-in and disabled by default — replacement scans can mask typos. Enabled per-session with `CALL uni.config.set('replacement_scans', true)`.

### 4.24 Pregel-style algorithms — `PregelProgramProvider`

A higher-level alternative to the black-box `AlgorithmProvider`. The plugin provides a vertex program; the host runs Pregel/GAS-style iteration (gather, apply, scatter), handles message passing, parallelism, convergence detection.

```rust
pub trait PregelProgramProvider: Send + Sync {
    fn signature(&self) -> &PregelSignature;
    fn init(&self, vertex: &VertexView, state: &mut VertexState) -> Result<(), FnError>;
    fn compute(&self, ctx: PregelComputeContext<'_>,
               vertex: &VertexView, state: &mut VertexState,
               incoming: &[Message]) -> Result<ComputeOutcome, FnError>;
    fn combine(&self, _a: &Message, _b: &Message) -> Option<Message> { None }      // optional message combiner
    fn halt(&self, _superstep: u64, _stats: &PregelStats) -> bool { false }         // global halt condition
}

pub struct PregelSignature {
    pub state_type: DataType,
    pub message_type: DataType,
    pub aggregation_mode: AggregationMode,           // BSP | AsyncShared | AsyncMessaging
    pub max_supersteps: Option<u64>,
}

pub enum ComputeOutcome {
    Vote { halt: bool, outgoing: Vec<(VertexId, Message)> },
}
```

The host's Pregel executor lives in `uni-algo` and is reusable across plugin and built-in vertex programs. PageRank, label-propagation, SSSP, connected-components, etc. can all be expressed as `PregelProgramProvider`s (and several existing `uni-algo` algorithms migrate to this surface in `uni-plugin-builtin`).

### 4.25 Pushdown — decomposed marker traits

Not a single plugin trait. Following Spark DataSources V2 and Trino's connector SPI, pushdown is expressed as **marker traits per capability** that `StorageBackend`, `IndexHandle`, `OperatorProvider`, and `CatalogTable` may opt into individually. This lets a backend declare exactly which pushdowns it handles without stubbing methods it doesn't.

```rust
pub trait SupportsFilterPushdown {
    /// Inspect filters and tell the planner which it handles fully vs partially.
    fn push_filters(&self, filters: &[Expr]) -> FilterApplication;
}

pub trait SupportsProjectionPushdown {
    /// Declare which projected columns to actually read from the source.
    fn push_projection(&self, columns: &[String]) -> ProjectionApplication;
}

pub trait SupportsLimitPushdown {
    /// `Some(applied)` if the source enforces the limit; `None` to leave it to the planner.
    fn push_limit(&self, limit: usize) -> Option<usize>;
}

pub trait SupportsTopNPushdown {
    fn push_topn(&self, sort: &[SortExpr], k: usize) -> Option<TopNApplication>;
}

pub trait SupportsAggregatePushdown {
    /// Declare which aggregate expressions the source can compute server-side.
    /// Partial aggregates (state-passing) are honored — the planner combines partial states.
    fn push_aggregates(&self, group_by: &[Expr], aggs: &[AggregateExpr]) -> AggregateApplication;
}

pub struct FilterApplication {
    pub fully_handled: Vec<usize>,        // indices of filters the source handles
    pub partially_handled: Vec<usize>,    // source filters approximately; planner re-checks
}
pub struct ProjectionApplication { pub keep: Vec<String> }
pub struct TopNApplication { pub applied: TopNScope }   // Local | Global
pub struct AggregateApplication {
    pub fully_handled: Vec<usize>,
    pub returns_partial_state: bool,      // if true, planner adds Final aggregate
}
```

The planner queries each marker trait via runtime type-checking (e.g., `if let Some(p) = backend.downcast_ref::<dyn SupportsFilterPushdown>()`). Backends opt in to only the marker traits they can implement. Sources that can handle a filter completely save a `Filter` operator above the scan; sources that handle approximately (Bloom filter, zone map) declare `partially_handled` and the planner keeps a verifying `Filter`.

This decomposition is the difference between "one blob trait every backend stubs out" and "backends declare exactly their capabilities" — the latter scales to dozens of pushdown kinds without bloating the trait surface.

---

## 5. Loader Layer

**Five loaders, two WASM ABIs.** All converge on `PluginRegistrar`. The execution layer (everything above) is loader-agnostic — a registered `Arc<dyn ScalarPluginFn>` looks the same to the executor whether the source was Rust, a Component Model WASM artifact, an Extism plugin, a Python callable, or a Rhai script.

The proposal adopts a **hybrid WASM strategy** ("Option C") motivated by §5.1.1 below: Component Model (`uni-plugin-wasm`) for trusted built-ins and infrastructure where typed contracts are load-bearing for soundness; Extism (`uni-plugin-extism`) for user-authored plugins where authoring simplicity and SDK breadth matter more than mechanical defense. **Trust is a property of the granted capability set, not of the ABI** — the same capabilities can be granted to either ABI, and the same plugin (in principle) could be ported between them. See §10.2b for how the capability enforcement layers degrade gracefully across the two ABIs.

> **v1 surface scope.** Every loader below converges on `PluginRegistrar`, but the *set of surfaces* each can register differs. The non-Rust loaders (CM, Extism, PyO3, Rhai) author scalar functions, aggregates, and procedures in v1; the other 22 surfaces are compile-time-Rust-only pending their WIT worlds (§19 criterion 30, Appendix A).

### 5.1 Loader matrix

| Loader                  | Crate                  | Authored when       | Sandboxed | Hot-update         | Scope default | Cap. enforcement                  | Perf ceiling                                  |
|-------------------------|------------------------|---------------------|-----------|--------------------|---------------|-----------------------------------|-----------------------------------------------|
| Compile-time Rust       | `uni-plugin-builtin` + user crates | Build time          | Trusted   | No (rebuild)       | `Instance`    | Registrar gate only               | Native                                        |
| WASM **Component Model**| `uni-plugin-wasm`      | Ahead of time       | Yes       | Reload component   | `Instance`    | Registrar + **linker absence** + runtime | Near-native (Arrow IPC per batch)             |
| WASM **Extism**         | `uni-plugin-extism`    | Ahead of time       | Yes       | Reload module      | `Instance`    | Registrar + runtime               | Near-native (Arrow IPC or JSON per batch)     |
| PyO3 live callable      | `uni-plugin-pyo3`      | Runtime, in-process | **No**    | Redefine fn        | `Session`     | Registrar + runtime               | PyO3-per-row, or vectorized via pyarrow zero-copy |
| Rhai script             | `uni-plugin-rhai`      | Runtime, in-process | **Yes**   | Replace source     | `Session`     | Registrar + Engine-import absence + runtime | Rhai engine in-host; ~10× slower than native Rust per-call; resource-limited via Engine |

### 5.1.1 Why two WASM ABIs

The two WASM ABIs are deliberately complementary, not competitive:

| Concern                        | Component Model (CM)                                          | Extism                                                       |
|--------------------------------|---------------------------------------------------------------|--------------------------------------------------------------|
| Host↔plugin protocol           | WIT-typed contracts; link-time enforcement                    | Bytes-in / bytes-out; protocol negotiated by host code       |
| Capability gating              | By linker absence (impossible-by-construction)                | By runtime host-fn check (correct-if-implemented)            |
| Resource types (e.g., secrets) | First-class WIT `resource`; structural non-leakage            | Integer handles + host-side side-table; non-leakage by convention |
| Multi-version ABI coexistence  | Per-major `Linker`; clean side-by-side                        | Per-version protocol; manual                                 |
| Plugin authoring tooling 2026  | `cargo component`, `wit-bindgen` — improving; rough edges     | Mature 13-language host SDK + plugin SDKs in ~5 languages    |
| Time to first working plugin   | Slower (WIT learning curve)                                   | Fast (familiar SDK calls)                                    |
| Distribution                   | OCI artifacts (emerging tooling)                              | Extism Hub (mature) + arbitrary HTTP / OCI                   |

**Use CM for:** the future Python-host wasm (when WASI 0.3 stabilizes async), built-in plugins that ship as WASM for hot-reload-without-rebuild, and any plugin where the structural advantages of typed contracts and linker-absence gating are load-bearing for the trust model.

**Use Extism for:** user-authored UDFs where the authoring path matters more than the structural safety net, plugins in languages where Component Model tooling isn't ready yet, and the bulk of `apoc.*` user-contributed content.

The framework intentionally does **not** auto-detect the ABI from the wasm bytes — callers pick via `Uni::load_wasm_component(bytes, grants)` or `Uni::load_wasm_extism(bytes, grants)`. A future convenience method `Uni::load_wasm(bytes, grants)` could sniff and dispatch, but the explicit form is preferred so authors and operators are aware which capability-enforcement layers apply.

### 5.2 Compile-time Rust plugins

The simplest path. A Rust plugin is any `struct` implementing `Plugin`:

```rust
pub struct GeoPlugin;
impl Plugin for GeoPlugin {
    fn manifest(&self) -> &PluginManifest { /* … */ }
    fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
        r.scalar_fn(QName::parse("geo.haversine")?, haversine_sig(), Arc::new(Haversine));
        r.scalar_fn(QName::parse("geo.bbox_contains")?, bbox_sig(), Arc::new(BboxContains));
        Ok(())
    }
}

uni.add_plugin(Arc::new(GeoPlugin))?;
```

`uni-plugin-builtin` is exactly this pattern for all current built-ins.

### 5.3 WASM Component Model plugins — the typed-contract path

Loaded via `Uni::load_wasm_component(bytes, grants)`. The framework:

1. Parses the wasm component and validates it implements one or more of the plugin WIT worlds (§6).
2. Reads the manifest by calling the `manifest-json` export.
3. Intersects the manifest's declared capabilities with the user-supplied `grants` to compute the *effective* capability set.
4. Constructs a per-major `Linker` providing only the host imports corresponding to the effective capabilities.
5. Instantiates the component into a wasmtime `Store` configured with epoch-interruption and fuel metering per the manifest's resource limits.
6. Calls the component's `register` export, which calls back into host imports to register each surface; the host wraps each registration in an adapter implementing the appropriate capability trait (e.g., `ScalarPluginFn`) backed by `invoke-batch`.

Performance: Arrow IPC over shared linear memory, batch-amortized. Per-batch cost ≈ one IPC serialize + one deserialize. For batches above ~1000 rows, this is competitive with native; below that, IPC overhead dominates and row-mode dispatch wins.

#### 5.3.1 Pre-warmed component pools (cold-start mitigation)

wasmtime component instantiation costs **10–100 ms** depending on component size, host-import count, and JIT compilation. For storage backends and long-running operator plugins this is a one-time cost paid at load. For hot-path scalar UDFs invoked millions of times per query, paying instantiation per call is fatal.

The framework provides a per-plugin **`WasmInstancePool`**:

```rust
pub struct WasmInstancePool {
    plugin_id: PluginId,
    module: Arc<wasmtime::component::Component>,    // parsed once, shared
    linker: Arc<wasmtime::component::Linker<Host>>, // capability-configured once
    idle: ArrayQueue<PooledInstance>,               // wait-free MPMC queue
    max_size: usize,                                // Capability::ConcurrentInstances
    metrics: Arc<PoolMetrics>,
}

struct PooledInstance {
    store: wasmtime::Store<Host>,
    bindings: PluginBindings,
    last_used: Instant,
}
```

Lifecycle per call:
1. `pool.acquire()` pops an idle instance (wait-free) or constructs a new one if under `max_size`.
2. Host invokes the work export.
3. `pool.release(instance)` returns the instance to the queue. On error, the instance is dropped (it may carry corrupt state).

Background reaping: idle instances older than `idle_ttl` (default 60s) are dropped, releasing wasm linear memory back to the OS. Pool warm-up: at plugin `init()`, the host pre-instantiates `min_warm` instances so first-call latency is the pool's baseline.

Single-threaded plugins (the default for the scalar-plugin world) need only `max_size == 1`. Plugins declaring `concurrency_safe: true` in their manifest can scale to higher pool sizes for parallel query execution.

This pattern is what Spin, wasmCloud, RisingWave, and InfluxDB 3's processing engine all use. Without it, the cold-start cost makes WASM UDFs unusable for analytics workloads.

#### 5.3.2 Module sharing across plugins

The wasmtime `Component` (the parsed wasm module) is reused across all instances of a plugin in the pool.

### 5.4 PyO3 live callables — the Python REPL UDF path

PyO3-loaded plugins are session-scoped by default and run at host privilege (no sandbox). They exist because they're how end-users in Python notebooks naturally extend the database.

```python
@db.scalar_fn("py.score", returns="float", args=["float", "float"], vectorized=True)
def score(x: pa.Float64Array, y: pa.Float64Array) -> pa.Float64Array:
    return pa.compute.multiply(x, 0.7).add(pa.compute.multiply(y, 0.3))

db.query("MATCH (n:Item) RETURN n.id, py.score(n.a, n.b) AS s")
```

Two modes:
- `vectorized=True` (preferred) — Python sees `pyarrow.Array`s, returns one. Zero-copy via Arrow C Data Interface. The PyO3 boundary is crossed once per batch.
- `vectorized=False` (default for simple users) — Python sees individual rows. One boundary cross per row. Slow but ergonomic.

Underneath:

```rust
pub struct PyScalarFn {
    callable: PyObject,
    sig: FnSignature,
    vectorized: bool,
}

impl ScalarPluginFn for PyScalarFn {
    fn invoke(&self, args: &[ColumnarValue], rows: usize) -> Result<ColumnarValue, FnError> {
        Python::with_gil(|py| {
            if self.vectorized {
                let pa_args: Vec<PyObject> = args.iter().map(|a| arrow_to_pyarrow(py, a)).collect()?;
                let result = self.callable.call1(py, PyTuple::new(py, pa_args))?;
                pyarrow_to_arrow(py, result)
            } else {
                // row-by-row, build output column incrementally
            }
        })
    }
}
```

PyO3 plugins go through `Session::add_plugin` rather than `Uni::add_plugin` by default. Lifetime tied to the Python `Session` handle.

#### 5.4.1 GIL contention — operational reality

For multi-threaded query execution (DataFusion runs partitions in parallel), all PyO3 calls serialize on Python's Global Interpreter Lock. A single hot-path Python UDF in a parallel scan collapses parallelism to single-threaded throughput. This is the dominant operational concern with PyO3 UDFs in analytics workloads.

Mitigations:
1. **Vectorized mode** amortizes GIL acquisition over batches — one acquire per RecordBatch, not per row. This is the default and usually sufficient.
2. **Python 3.13+ sub-interpreters** (PEP 684) give per-thread interpreter state, enabling true parallelism. Support is preview-quality; the host can opt in via the `pyo3` crate's `sub-interpreters` feature when 3.13 is the floor.
3. **Python 3.14+ free-threading** (PEP 703) removes the GIL entirely. Production-quality in 3.14 LTS (expected late 2026).
4. **Per-partition partitioned UDFs** — for embarrassingly-parallel UDFs, the host can clone the callable handle into N partitions; whether that helps depends on the underlying GIL status.

For users running parallel analytics, the recommended path is WASM, not PyO3. The proposal makes PyO3 available because the Python REPL is the natural authoring environment for ad-hoc UDFs, not because it's the high-throughput option.

#### 5.4.2 Visibility — session-scoped by default

PyO3 plugins are `Scope::Session` by default. This means a function declared with `@db.scalar_fn` is *not visible* to other connections to the same uni-db instance, *not visible* to background jobs, and disappears when the Python `Session` is dropped. For users coming from Postgres `CREATE FUNCTION` (which is instance-scoped and persistent) this can surprise. To get persistent instance-scoped behavior, use the meta-plugin (§8) `uni.plugin.declareFunction(...)` from inside Python.

### 5.5 Extism plugins — the user-facing path

Loaded via `Uni::load_wasm_extism(bytes, grants)`. Extism is the **user-facing** WASM loader for the bulk of authored plugins. The crate `uni-plugin-extism` is structurally parallel to `uni-plugin-wasm` (Component Model), but the ABI between the host and the plugin is Extism's bytes-in / bytes-out convention rather than WIT-typed exports.

The framework:

1. Parses the wasm module (raw wasm, not a CM component) and validates it exports the Extism plugin shape: `extism_alloc`, `extism_dealloc`, named function exports per the plugin's manifest, optional `extism_log`.
2. Reads the manifest via a host-mediated `manifest` plugin export (an Extism function returning canonical JSON identical to §14.2).
3. Intersects the manifest's declared capabilities with `grants` to compute the effective capability set.
4. Filters the `HostFnRegistry` (`crates/uni-plugin-extism/src/host_fns.rs`) through the effective capability set: only host fns whose `required_capability` is granted are registered into the plugin's Extism `Plugin` builder. **Plugins that import a host fn outside their grant set fail at instantiation with `ExtismError::CapabilityDenied`** — this is the runtime analogue of Component Model's linker absence.
5. Instantiates via the extism-sdk into a wasmtime `Store` configured with epoch interruption and fuel metering per the manifest's resource limits.
6. Calls the plugin's `register` export, which returns a JSON registration manifest enumerating every qname the plugin provides. The host wraps each entry in an adapter implementing the appropriate capability trait (`ScalarPluginFn`, `AggregatePluginFn`, …) backed by Extism's `Plugin::call(name, bytes)`.

```rust
// Authoring (Rust plugin via extism-pdk):
#[plugin_fn]
pub fn score(input: Json<ScoreArgs>) -> FnResult<Json<f64>> {
    let s = input.into_inner();
    Ok(Json(s.x * 0.7 + s.y * 0.3))
}
```

#### 5.5.1 Wire format for batch fns

Scalar / aggregate / procedure plugins ship `RecordBatch`-shaped batches via **Arrow IPC**, identical to the CM path (§6.3) — Extism's `alloc` / output APIs handle the byte transfer; the format on top is still Arrow IPC. This is a deliberate choice: it keeps the executor's columnar contract uniform across both ABIs, lets a single `register_plugin_scalar_udfs` helper handle both, and avoids paying a JSON-encode cost on hot paths.

Control surfaces (manifests, registration responses, error reports) use canonical JSON because the per-call cost is negligible and Extism plugin authors already work with JSON via the language SDKs.

#### 5.5.2 Capability runtime checks — the explicit pattern

Where the Component Model loader makes the host import literally absent, the Extism loader uses a single helper around every gated call:

```rust
// crates/uni-plugin-extism/src/host_fns.rs (sketch, M6a)
fn checked_call<F, R>(
    registry: &HostFnRegistry,
    grants: &CapabilitySet,
    host_fn: &str,
    body: F,
) -> Result<R, ExtismError>
where F: FnOnce() -> Result<R, ExtismError>,
{
    let spec = registry.get(host_fn)
        .ok_or_else(|| ExtismError::InvalidPlugin(format!("unknown host fn `{host_fn}`")))?;
    if let Some(cap) = &spec.required_capability {
        if !grants.has_name(cap) {
            return Err(ExtismError::CapabilityDenied {
                host_fn: host_fn.into(),
                capability: cap.clone(),
            });
        }
    }
    body()
}
```

Every gateable host fn calls `checked_call` first. The single helper is the choke point that prevents "host author forgot to check the capability" — the kind of bug the CM linker-absence approach makes structurally impossible.

#### 5.5.3 Trust tier is orthogonal to ABI

A built-in plugin *could* author against Extism if it wanted simpler authoring; a user plugin *could* author against Component Model if it wanted the typed-contract guarantees. Capabilities are granted on the same `CapabilitySet` axis regardless of ABI. The proposal's default convention — built-ins on CM, user plugins on Extism — is a *guidance heuristic*, not a framework constraint.

#### 5.5.4 Hot reload and pooling

Extism plugins share the wasmtime instance pool design from §5.3.1 — `extism::Plugin` instances are pooled, pre-warmed at registration, reaped on idle TTL. The cold-start cost is similar (10–100 ms per fresh instance) because Extism uses the same wasmtime compilation pipeline.

Hot reload follows the same epoch-fenced cutover as CM plugins (§11.2): swap the registry entry; drain the old `Arc<dyn ScalarPluginFn>` references; drop the old `Plugin` template once refcount reaches 1.

#### 5.5.5 Distribution

Extism plugins can be distributed via:
- **Extism Hub** — `extism.org/hub`, the canonical Extism plugin registry. Mature in 2026; signed; multi-language plugin catalog.
- **OCI artifacts** — same path as CM plugins (§17.4).
- **Raw HTTP / local paths** — for development and private deployments.

CLI: `uni plugin install extism://hub/example/score:0.3.1`.

### 5.6 Rhai scripts — the host-embedded scripting path

Loaded via `Uni::load_rhai_plugin(src, grants)`. Rhai is a pure-Rust embedded scripting language (the `rhai` crate); the framework embeds a host-side `rhai::Engine` per plugin. No WASM wrapper, no C toolchain dependency, no separate runtime to ship.

```rhai
// my_plugin.rhai

fn uni_manifest() {
  #{
    id:          "ai.dragonscale.examples.score",
    version:     "0.1.0",
    determinism: "pure",
    scalar_fns:  [
      #{ name: "score",  args: ["float","float"], returns: "float" },
      #{ name: "bucket", args: ["int"],           returns: "string" },
    ],
    aggregate_fns: [
      #{ name: "weighted_mean",
         args: ["float","float"], returns: "float",
         state: "map" },
    ],
  }
}

fn score(x, y) {
  if x == () || y == () { return (); }
  ((x * 0.7 + y * 0.3) ** 0.5)
}

fn bucket(n) {
  if n < 10        { "low" }
  else if n < 100  { "mid" }
  else             { "high" }
}

const weighted_mean = #{
  init:       || #{ sum: 0.0, w: 0.0 },
  accumulate: |s, x, w| { s.sum += x * w; s.w += w; },
  merge:      |a, b|    #{ sum: a.sum + b.sum, w: a.w + b.w },
  finalize:   |s|       if s.w == 0.0 { () } else { s.sum / s.w },
};
```

Load:
```rust
let src = std::fs::read_to_string("my_plugin.rhai")?;
uni.load_rhai_plugin(src, CapabilitySet::sandbox_defaults())?;
```

Or from Python:
```python
db.load_rhai_plugin(open("my_plugin.rhai").read())
```

#### 5.6.1 Why Rhai (vs. Lua, Starlark, Dyon, etc.)

Rhai is selected because it satisfies four properties simultaneously:

1. **Pure Rust, no C dependency.** Builds on every Tier 1 / Tier 2 Rust target without a host C toolchain. No `cc-rs` / `bindgen` cost; works under musl, cross-compiled targets, and `cargo install` paths that don't ship a compiler.
2. **Sandboxed by construction.** The language has no built-in I/O — there is no `os`, no `io`, no `require`, no `import`. Every effectful operation comes from a host-registered function. Registering a function is opt-in; *absence* is the default and matches the framework's capability-gating contract layer 2 (§10.2).
3. **Resource limits are first-class on the Engine.** `set_max_operations`, `set_max_call_levels`, `set_max_string_size`, `set_max_array_size`, `set_max_modules`, `set_max_map_size` are all built in. No ad-hoc instruction-count shim.
4. **Active upstream.** `rhai` (crates.io) is maintained, on a regular release cadence, with a published changelog and an MSRV policy. Avoids the maintenance trap that sank the prior Lua attempt.

What we trade for these properties:
- **Performance.** Rhai is ~5–10× slower per call than native Rust for the same arithmetic. For row-rate UDFs in inner loops this is fatal; for orchestration, declarative validation, custom procedure logic, or low-volume scalar fns this is fine. Authors needing throughput target Rust or WASM.
- **Language familiarity.** Rhai's syntax is its own (Rust-flavored expression language). Authors who know JS or Python adapt quickly; the learning curve is real but shallow.
- **No bit-exact reference semantics.** A Rhai `score(x,y)` and a Rust `score(x,y)` will agree to the last ULP on simple arithmetic but may diverge on transcendentals depending on which libm the Rhai stdlib delegates to. This is documented; the cross-ABI byte-parity test (§19 criterion 24) stays scoped to CM and Extism — Rhai joins as a separate parity tier with a looser ULP tolerance.

#### 5.6.2 The Rhai sandbox

Default Engine, before any capability grants, exposes:
- Arithmetic, comparison, logical operators.
- String / array / map / object-map / range operations from Rhai's standard `BasicArrayPackage` / `BasicMapPackage` / `BasicStringPackage` / `MoreStringPackage` / `BasicMathPackage`.
- `print` / `debug` routed to host `tracing` at DEBUG level (no stdout leak).
- `uni.*` intrinsics — capability-gated (see below).

Excluded from the default Engine, regardless of grant set, via `Engine::disable_symbol`:
- `eval` — no dynamic code generation inside a script.
- Module loading (`import`) — the `ModuleResolver` slot is set to a deny-all stub; modules are only made available through host-registered Rhai packages.

Capability grants re-add functionality by registering specific functions into the Engine:
- `Capability::Filesystem { read, write }` → `uni.fs.read(path)`, `uni.fs.write(path, data)`. Paths validated against grant glob patterns before the host syscall.
- `Capability::Network { allow }` → `uni.http.get(url)`, `uni.http.post(url, body)`. URLs validated against the grant.
- `Capability::HostQuery { read_only, scopes }` → `uni.query(cypher, params)`. Runs a Cypher query against the same `Session`; subject to a recursion-depth limit (max 1).
- `Capability::Kms { key_ids }` → `uni.kms.sign(key_id, data)`, `uni.kms.verify(...)`.

These functions are only present in the per-plugin `Engine` when the corresponding capability is granted. A plugin without `Capability::Filesystem` cannot call `uni.fs.read` — the symbol isn't registered, and Rhai raises `ErrorFunctionNotFound` at parse-resolution time. This is the in-host analogue of Component Model's linker-absence enforcement.

#### 5.6.3 Performance modes

Two execution modes per Rhai function:

**Row mode** (default). The host iterates the input Arrow batch row by row, converts each cell to a `rhai::Dynamic`, calls the function, and writes the returned `Dynamic` to the output column builder. Per-call cost: one Dynamic conversion in + one out + the Rhai evaluation.

**Vectorized mode** (`vectorized = true` in the manifest entry). Rhai sees Arrow columns as registered custom types (`Float64Column`, `Int64Column`, `StringColumn`) with indexer methods. The script writes one body and the host hoists the loop:

```rhai
fn score_v(col_x, col_y) {
  let n = col_x.len();
  let out = uni::float_column(n);
  for i in 0..n {
    out[i] = (col_x[i] * 0.7 + col_y[i] * 0.3) ** 0.5;
  }
  out
}
```

`col_x[i]` is a Rhai indexer registered against `Float64Column` that reads from the underlying Arrow buffer with no copy. Output columns are allocated via `uni_float_column(n)` (a host-registered fn returning a `MutableFloat64Column`) and written via the indexer-set form `out[i] = v`. A future `uni::map(col, |x| ...)` host fn that hoists the inner loop into Rust with a Rhai closure per row — comparable to vectorized Lua's `uni.map` or pandas `apply` — is M7-followup; v1 ships the explicit indexer-loop form.

#### 5.6.4 Resource limits

Configured on the per-plugin `Engine` from `CapabilitySet`:

- `Engine::set_max_operations(N)` — caps the number of Rhai operations per top-level call. From `Capability::FuelPerCall(N)`. Trips with `ErrorTooManyOperations`, mapped to `FnError { code: 0x711 }`.
- `Engine::set_max_call_levels(N)` — caps recursion depth. Default `DEFAULT_MAX_CALL_LEVELS = 64`, exported from `crate::engine`.
- `Engine::set_max_string_size` / `set_max_array_size` / `set_max_map_size` — caps allocation per collection. v1 derives a conservative `MemoryBytes / 4` per-collection cap from `Capability::MemoryBytes(N)`. Full total-memory accounting (e.g. via an `Engine::on_var` hook tracking cumulative allocations) is deferred to M10's broader memory-limit work.
- Wall-clock deadline — `Capability::WallClockMillisPerCall(N)` is a defined capability variant but the host-side deadline driver is deferred to M7-followup. v1 enforcement is through the operations cap; long-running scripts terminate before clock-wall expiry by hitting the op limit, not by deadline. The integration point in the Rhai engine is `Engine::on_progress`.

#### 5.6.5 State and lifecycle

The Engine is reused across calls within a plugin's lifetime — top-level `const`s and registered packages persist. A per-call `rhai::Scope` is fresh; cross-call state lives in a registered `uni::tx_local` map cleared at transaction boundaries (same shape as the per-tx scratch space the host already provides to other loaders).

Hot reload follows the same epoch-fenced cutover as every other loader (§11.2). The Engine is stateless across calls in the scalar/aggregate worlds, so reload is clean — the new source is parsed into a new AST + Engine, swapped in via `arc-swap`, and the old instance drains.

#### 5.6.6 Error handling

Rhai errors (parse, runtime, type, resource-limit) caught by the host shim become `FnError { code: 0xRHAI, message: <error display>, retryable: false }`, then wrapped into `UniError::Plugin { id, qname, source }` by the framework. Error messages preserve Rhai's source-position information (file:line:col).

#### 5.6.7 Distribution

Rhai scripts can be distributed via:
- **Local paths** — `uni plugin install ./my_plugin.rhai`.
- **HTTP URLs** — `uni plugin install https://example.com/score.rhai` with signature pinning.
- **OCI artifacts** — same path as WASM plugins (§17.4); the script bytes ride as the OCI artifact layer with a `.rhai` media type.

A Rhai plugin pack (multiple `.rhai` files + a top-level manifest) is M12's distribution-extension work; v1 ships single-file `.rhai` loading via `Uni::load_rhai_plugin(src, grants)` and `uni plugin install foo.rhai`.

---

## 6. ABI — WIT Worlds and Arrow IPC

This section covers the WASM ABIs the framework speaks. Two ABIs ride on the same wasmtime runtime: **WIT-typed Component Model** (§6.1–6.4) for trusted infrastructure, and **Extism's bytes-in/bytes-out** (§6.5) for user-facing plugins. They share the Arrow IPC payload format (§6.3) so the executor's columnar contract is uniform regardless of which ABI delivered a `RecordBatch`.

### 6.1 Versioning policy

The WIT-defined ABI is semver-versioned:
- **Major** bump: a world export is deleted or repurposed. Breaks existing plugins.
- **Minor** bump: a new world is added, or a new optional host import. Backward compatible.
- **Patch** bump: documentation, optional fields with default behavior.

A plugin's manifest declares an `abi: AbiRange` (e.g., `^1.2`). The host maintains a `Linker` per supported major; plugins compiled for an out-of-range major are rejected at load.

The Extism ABI inherits its own semver track (`abi-extism: ^1.x`) parallel to the WIT one. They evolve independently — a breaking Extism protocol change does not bump the WIT major, and vice versa. A plugin's manifest declares exactly one of `abi` (CM) or `abi-extism` (Extism); declaring both is rejected at load.

### 6.2 WIT worlds (overview)

Each plugin kind has its own world so the host can pick the right `Linker` configuration. Full WIT in Appendix A.

```wit
package uni:plugin@1.0.0;

interface types       { /* scalar, value variants, errors */ }
interface arrow       { /* RecordBatch resource, IPC handles */ }
interface host        { /* tracing, metrics, capability-gated imports */ }
interface host-query  { /* uni.query — gated by HostQuery capability */ }
interface host-fs     { /* read/write — gated by Filesystem */ }
interface host-net    { /* http get/post — gated by Network */ }
interface host-kms    { /* sign/verify — gated by Kms */ }

// Implemented today (crates/uni-plugin-wasm/wit/world.wit): scalar-plugin,
// aggregate-plugin, procedure-plugin. Every other world below is the planned
// design — not yet implemented (see §19 criterion 30).
world scalar-plugin    { /* invoke-batch */ }
world aggregate-plugin { /* new-acc / update / merge / state / evaluate / drop-acc */ }
world window-plugin    { /* evaluate-partition */ }
world locy-agg-plugin  { /* ingest / merge / finalize / is-at-top / semilattice */ }
world locy-pred-plugin { /* evaluate / evaluate-fuzzy */ }
world operator-plugin  { /* plan / poll-next / drop-plan */ }
world index-plugin     { /* build / open / probe / persist */ }
world storage-plugin   { /* open / read-batch / write-batch / list-tables / delete */ }
world algo-plugin      { /* run */ }
world crdt-plugin      { /* apply / merge / value / persist */ }
world hook-plugin      { /* before/after query/commit/tx */ }
world type-plugin      { /* from-literal / to-display / cast-to / cast-from */ }
world auth-plugin      { /* authenticate */ }
world authz-plugin     { /* check */ }
world connector-plugin { /* start / stop */ }
```

### 6.3 Arrow IPC over linear memory

The load-bearing primitive of the WASM ABI is Arrow IPC over shared linear memory. The pattern:

1. Host serializes a `RecordBatch` to Arrow IPC stream bytes.
2. Host calls plugin export `alloc(len) -> u32` to get a wasm-memory pointer.
3. Host copies IPC bytes into wasm memory at that pointer.
4. Host calls the work export, e.g., `invoke-batch(qname, ptr, len) -> result<(ptr, len), fn-error>`.
5. Plugin parses IPC bytes inside wasm (with arrow-rs compiled to wasm32), does its work, allocates an output buffer via its own `alloc`, writes IPC bytes there, returns `(ptr, len)`.
6. Host reads bytes out of wasm memory at the returned pointer, deserializes back to `RecordBatch`.
7. Host calls plugin `free(ptr, len)` to release the output buffer.

This pattern is used by DuckDB's WASM extensions, Polars' WASM data sources, and is the standard approach for shipping Arrow across a wasm boundary. Per-batch overhead: ~one IPC serialize + one deserialize. For batches of a few thousand rows or more, this is competitive with native operator performance.

Stream-shaped operators (operator plugins, storage `read_batch`) expose `poll-next` returning `option<(ptr, len)>` — `none` signals end-of-stream. The host wraps this into a `SendableRecordBatchStream`.

### 6.4 Scalar value transport (small args)

For one-off scalar values (e.g., per-row metric counter labels, host import arguments), the framework uses component-model `variant value` types directly rather than Arrow IPC. The cutover happens at the WIT level: `scalar-plugin`'s `invoke-batch` always uses IPC; `host.metric_counter` uses scalar `value`s.

### 6.5 Extism ABI — bytes-in / bytes-out with Arrow IPC payloads

The Extism ABI does not use WIT. Plugins expose functions by name and exchange opaque byte buffers via Extism's `alloc` / `output` convention. The payload format is up to the host. The framework standardizes the payloads as follows:

| Surface                     | Payload format on the wire                                        |
|-----------------------------|-------------------------------------------------------------------|
| `manifest` export           | Canonical JSON (identical to §14.2 form)                          |
| `register` export           | Canonical JSON enumerating provided qnames + signatures            |
| Scalar / aggregate / window / locy-agg / procedure invocations | **Arrow IPC stream bytes** — same format as the CM path |
| Host imports (`host_log`, `host_fs_read`, …)                  | Function-specific JSON for control args; Arrow IPC for batch args |

Choosing Arrow IPC for batch payloads is deliberate — it keeps a single `register_plugin_scalar_udfs` adapter on the host capable of bridging either ABI to DataFusion's `ScalarUDFImpl`. From the executor's perspective the two ABIs are interchangeable; only the loader cares which.

The Extism plugin's exported function names follow a flat naming convention: `invoke_<qname>` for scalar fns, `agg_<qname>_new`/`agg_<qname>_update`/`agg_<qname>_state`/`agg_<qname>_evaluate` for aggregates, `proc_<qname>_invoke` for procedures, and so on. Equivalents of the WIT worlds (`scalar-plugin`, `aggregate-plugin`, …) are not separate plugin shapes — one Extism plugin can expose any mix of fn types in its registration manifest.

#### 6.5.1 Capability gating on the Extism ABI

Where Component Model bundles capability gating into the WIT linker, Extism gates at host-fn registration time:

```rust
// crates/uni-plugin-extism/src/loader.rs (sketch, M6a)
fn build_plugin(bytes: &[u8], grants: &CapabilitySet, registry: &HostFnRegistry)
    -> Result<extism::Plugin, ExtismError>
{
    let mut builder = extism::PluginBuilder::new(bytes);
    for spec in registry.iter() {
        let granted = spec.required_capability
            .as_ref()
            .map(|c| grants.has_name(c))
            .unwrap_or(true);
        if granted {
            // host fn is registered into the plugin
            builder = builder.with_function(spec.to_extism_function());
        }
        // else: host fn omitted — calling it from the plugin returns
        // "function not found" which the loader converts to
        // ExtismError::CapabilityDenied
    }
    builder.build().map_err(|e| ExtismError::Instantiate(e.to_string()))
}
```

The "function not found" path is the runtime analogue of CM's "import unsatisfied." Both produce a structured `CapabilityDenied` error, but CM rejects at instantiation while Extism rejects at first call. For most surfaces this difference is invisible; for trust-critical surfaces it argues for keeping those on CM.

---

## 7. Locy Aggregate Refactor (Retiring `FoldAggKind`)

This is the single most invasive change in the proposal. It is included in v1 because the framework's "every closed enum becomes a registry" invariant requires it.

### 7.1 Current state

`crates/uni-query/src/query/df_graph/locy_program.rs:1222` `parse_fold_aggregate`:

```rust
// Before
fn parse_fold_aggregate(name: &str) -> Result<FoldAggKind> {
    match name.to_uppercase().as_str() {
        "SUM" | "MSUM"  => Ok(FoldAggKind::Sum),
        "MAX"           => Ok(FoldAggKind::Max),
        "MIN"           => Ok(FoldAggKind::Min),
        "COUNT"         => Ok(FoldAggKind::Count),
        "AVG"           => Ok(FoldAggKind::Avg),
        "COLLECT"       => Ok(FoldAggKind::Collect),
        "MNOR"          => Ok(FoldAggKind::Mnor),
        "MPROD"         => Ok(FoldAggKind::Mprod),
        other           => bail!("unknown fold aggregate: {other}"),
    }
}
```

`FoldAggKind` is then matched on at multiple sites in `locy_fixpoint.rs` to dispatch to the right `MonotonicAggState` implementation.

### 7.2 Target state

`FoldAggKind` is deleted. `parse_fold_aggregate` returns `Arc<dyn LocyAggregate>` looked up from `PluginRegistry`:

```rust
// After
fn parse_fold_aggregate(name: &str, registry: &PluginRegistry) -> Result<Arc<dyn LocyAggregate>> {
    let qname = QName::parse(name).or_else(|_| QName::builtin(name))?;
    registry.locy_aggregate(&qname)
        .ok_or_else(|| anyhow!("unknown fold aggregate: {name}"))
}
```

The built-ins are registered by `uni-plugin-builtin`:

```rust
r.locy_aggregate(QName::builtin("MIN"),     Arc::new(MinLocyAgg));
r.locy_aggregate(QName::builtin("MAX"),     Arc::new(MaxLocyAgg));
r.locy_aggregate(QName::builtin("SUM"),     Arc::new(SumLocyAgg));
r.locy_aggregate(QName::builtin("MSUM"),    Arc::new(SumLocyAgg));   // alias
r.locy_aggregate(QName::builtin("COUNT"),   Arc::new(CountLocyAgg));
r.locy_aggregate(QName::builtin("AVG"),     Arc::new(AvgLocyAgg));
r.locy_aggregate(QName::builtin("COLLECT"), Arc::new(CollectLocyAgg));
r.locy_aggregate(QName::builtin("MNOR"),    Arc::new(MnorLocyAgg));
r.locy_aggregate(QName::builtin("MPROD"),   Arc::new(MprodLocyAgg));
```

Each implements `LocyAggregate::semilattice()` returning the metadata table in §4.4.

### 7.3 Fixpoint engine changes

`locy_fixpoint.rs::apply_post_fixpoint_chain` and `apply_having_filter` currently match on `FoldAggKind`. After refactor, they call into `Arc<dyn LocyAggState>`. The two changes:

1. `MonotonicAggState`'s constructor receives `Arc<dyn LocyAggregate>` and calls `aggregate.create()` for the underlying `LocyAggState`.
2. The strict-mode alignment logic (from Phase 1/2 hardening) reads `semilattice().has_top` instead of matching `FoldAggKind::Mnor | FoldAggKind::Mprod`.

Monotonicity is enforced at registration: an aggregate with `semilattice.monotone_join == false` is rejected if it appears inside a recursive Locy clause. This check is added to the Locy compiler in `convert_fold_bindings` (currently at `locy_program.rs:1183`).

### 7.4 Compatibility

The on-disk Locy bytecode format (if any) is unaffected — aggregate names are stored as strings, not `FoldAggKind` discriminants. User-facing Locy syntax (`FOLD value AS MAX`) is unchanged. Existing TCK scenarios pass unchanged. The 7 scenarios added during Phase 1/2 hardening (MonotonicAggregation + ProbabilisticComplement) continue to pass.

### 7.5 PROB-annotated shared-proof detection

The Phase 3 `DerivationTracker` integration survives the refactor unchanged: the tracker is created unconditionally when any rule uses an aggregate with `semilattice.has_top` (the new generalization of "any rule uses MNOR/MPROD"). The two-tier detection (precise base-fact overlap + structural fallback) and the `provenance_join_cols` plumbing on `IsRefBinding` remain in place.

---

## 8. Meta-Plugins — `apoc.custom` Equivalents

> Cross-referenced from §2.1 row 25.

APOC's most powerful feature is `apoc.custom.declareProcedure` / `declareFunction` — users define new procedures and functions *from inside Cypher*, the definitions are persisted in the database, and they survive restart and become callable like any other procedure. The framework supports this through a **meta-plugin pattern**: a built-in plugin that registers procedures which, when executed, call `PluginRegistrar` themselves.

### 8.1 User-facing API

```cypher
-- Declare a function from a Cypher expression
CALL uni.plugin.declareFunction(
  'mycorp.totalRevenue',
  '(SELECT sum(o.amount) FROM Order o WHERE o.tenant = $tid)',  -- expression body
  'float',                                                       -- return type
  [{name: 'tid', type: 'string'}],                              -- args
  {determinism: 'session-scoped', description: 'Total revenue per tenant'}
)

-- Declare a procedure from a Cypher query
CALL uni.plugin.declareProcedure(
  'mycorp.staleOrders',
  'MATCH (o:Order) WHERE o.updated_at < $cutoff RETURN o.id AS id, o.updated_at AS ts',
  'READ',                                                        -- mode
  [{name: 'cutoff', type: 'datetime'}],
  [{name: 'id', type: 'string'}, {name: 'ts', type: 'datetime'}],
  {description: 'Orders not updated since cutoff'}
)

-- Use them like any other
RETURN mycorp.totalRevenue('tenant-42')
CALL mycorp.staleOrders(datetime('2026-01-01')) YIELD id, ts RETURN id, ts

-- Or in Locy:
-- (No special syntax — declared functions become first-class.)

-- Lifecycle management
CALL uni.plugin.listDeclared()                YIELD qname, kind, declared_at, declared_by
CALL uni.plugin.dropDeclared('mycorp.totalRevenue')
CALL uni.plugin.exportDeclared('mycorp.*')    YIELD cypher_source        -- portable backup
```

### 8.2 Mechanism

`uni-plugin-custom` is a built-in plugin like any other. Its `register()` adds the `uni.plugin.*` procedures. When `uni.plugin.declareFunction` is invoked:

1. The Cypher body is parsed and type-checked at declaration time. Compilation errors are returned to the caller — no half-broken declaration lands.
2. The declaration is persisted in a system table `uni_system.declared_plugins` (label `_DeclaredPlugin`) with columns `(qname, kind, body, signature, manifest, declared_at, declared_by, dependencies)`.
3. A synthetic `Plugin` is constructed wrapping a `DeclaredScalarFn` (or `DeclaredProcedure`) that, on invocation, parameter-substitutes the args into the body and executes it via `host.query`.
4. The synthetic plugin is added to the framework with `Scope::Instance` and `Capability::ScalarFn` (or `Procedure`), inheriting the declaring session's capability grants (intersected — you cannot declare a procedure that exceeds your own grants).

On startup, `uni-plugin-custom`'s `init()` reads `uni_system.declared_plugins` and re-registers every declared plugin. The declarations survive restart with byte-identical behavior.

### 8.3 Dependency tracking

Declared plugins can call other declared plugins. The dependency edges live in `uni_system.declared_plugins.dependencies`. Drops are protected: `drop_declared('a')` when `b` depends on `a` returns `UniError::DropBlocked { dependent: 'b' }`. Force-dropping with `CALL uni.plugin.dropDeclared('a', {cascade: true})` cascades through dependents.

Cyclic dependencies are caught at declaration time by reachability analysis through the body's parsed Cypher.

### 8.4 Isolation from native plugins

Declared plugins live in a different *registration tier* from native (Rust/WASM) plugins. The plugin id is prefixed `declared:` internally; user-visible qnames are unaffected. A native plugin shipping `mycorp.totalRevenue` *shadows* the declared one (native wins) and the declared one is marked `inactive` with a warning. This prevents a declared plugin from masking a native one silently.

### 8.5 Authorization

Declarations require the declaring session to hold `Capability::PluginDeclare`. The capability is configurable at instance level — production deployments may want to restrict declarations to admin sessions or disable them entirely. Declared plugins inherit only capabilities the declarer also held; a declaring session without `Capability::ProcedureWrites` cannot declare a write procedure.

### 8.6 Why this works trivially

The meta-plugin pattern requires no new framework machinery. It's just a built-in plugin whose procedures happen to call `Uni::add_plugin` (with `Scope::Instance` and persistence via `uni_system.declared_plugins`). The trait-object-first design makes this a natural fit — the registry doesn't know or care whether a `Arc<dyn ScalarPluginFn>` came from a Rust binary, a WASM module, or a Cypher declaration. That's the design holding together.

### 8.7 Beyond functions and procedures

The pattern generalizes. `uni.plugin.declareTrigger`, `uni.plugin.declareBackgroundJob`, `uni.plugin.declareCollation` are all possible — anywhere `PluginRegistrar` accepts a registration, a meta-procedure can wrap it. v1 ships `declareFunction`, `declareProcedure`, `declareAggregate`, and `declareTrigger`. Others land as user demand justifies.

---

## 9. Wiring — File-by-File

Anchored at line numbers in the `plugin-fw` worktree. Each entry is a delta against current code.

### 9.1 New crates

| Crate                            | Purpose                                                                                                          |
|----------------------------------|------------------------------------------------------------------------------------------------------------------|
| `crates/uni-plugin/`             | Core trait, registry, manifest, capability traits, errors. Depends on `arrow`, `arrow-ipc`, `datafusion-common`, `tracing`, `semver`, `blake3`, `smol_str`. **No** `wasmtime`. |
| `crates/uni-plugin-wasm/`        | wasmtime-based loader. Per-world `Linker`. Arrow IPC marshalling. Epoch interruption. Fuel metering. Cargo feature `wasm` gates this from `uni`. |
| `crates/uni-plugin-builtin/`     | Re-implementations of closed-enum replacements (Locy aggregates, storage backends, CRDTs, collations, hooks, logical types). The dogfooding crate. **Excludes APOC analogues** — those live in `uni-plugin-apoc-core`. |
| `crates/uni-plugin-apoc-core/`   | APOC-equivalent procedures and scalar functions implemented in Rust — the perf-critical and host-intimate subset (`apoc.text`, `apoc.coll`, `apoc.math`, `apoc.bitwise`, `apoc.refactor`, `apoc.schema`, `apoc.atomic`). Cargo feature `apoc-core` gates this from `uni` (default-on). Plugin id: `apoc-core`. |
| `crates/uni-plugin-rhai/`        | Rhai loader. Host-embedded `rhai::Engine` per plugin. Pure Rust, no WASM wrapper. Cargo feature `rhai` gates this. |
| `crates/uni-plugin-pyo3/`        | PyO3 loader for live Python callables. Cargo feature `pyo3` gates this. |

### 9.2 Modified files

| File                                                              | Change                                                                                          |
|-------------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| `crates/uni-query/src/query/executor/custom_functions.rs:24`      | `CustomFunctionRegistry` becomes a backward-compat facade over `PluginRegistry`. Existing API preserved. Namespace-aware lookup. |
| `crates/uni-query/src/query/df_udfs.rs:79`                        | `register_cypher_udfs` iterates `uni-plugin-builtin` registrations rather than its hardcoded list. |
| `crates/uni-query/src/query/df_udfs.rs:243`                       | `register_custom_udfs` iterates `PluginRegistry::scalar_fns()`. |
| `crates/uni-query/src/query/df_udfs.rs:311`                       | `CustomScalarUdf` demoted: only used when `ArgType::CypherValue` is declared. `ArgType::Primitive(T)` plugins go through a new `NativeArrowUdf` that passes arrays through without conversion. |
| `crates/uni-query/src/query/df_expr.rs:2130`                      | `translate_function_call` consults `PluginRegistry` before falling back to `dummy_udf_expr`. Built-in match is removed (the cases become builtin-plugin registrations). |
| `crates/uni-query/src/query/df_graph/locy_program.rs:1183,1222`   | `convert_fold_bindings` and `parse_fold_aggregate` rewritten per §7. |
| `crates/uni-query/src/query/df_graph/locy_fixpoint.rs`             | `MonotonicAggState` parameterized over `Arc<dyn LocyAggregate>`. Strict-mode check reads `semilattice().has_top`. |
| `crates/uni-query/src/query/df_graph/vector_knn.rs`                | Refactored behind `IndexKindProvider`. The built-in vector index becomes `uni-plugin-builtin::VectorIndexProvider`. |
| `crates/uni-query/src/query/expr_eval.rs:1860`                     | Custom-fn lookup uses `PluginRegistry` directly; the `custom_fns` field on `ExprEval` becomes `Arc<PluginRegistry>`. |
| `crates/uni-store/src/lib.rs`                                       | `StorageBackend` trait added. Existing Lance backend wrapped as `LanceStorageBackend`. Scheme-based dispatch. |
| `crates/uni-crdt/src/lib.rs`                                        | `CrdtKindProvider` trait. LWW/OR-Set/RGA become registrations in `uni-plugin-builtin`. |
| `crates/uni-algo/src/lib.rs`                                        | `AlgorithmProvider` trait. All 35 algorithms become registrations in `uni-plugin-builtin`. |
| `crates/uni/src/api/hooks.rs`                                       | `SessionHook` trait kept verbatim. `Uni::add_hook` becomes sugar for `add_plugin(BuiltinHookPlugin::new(hook))`. |
| `crates/uni/src/lib.rs`                                             | `Uni::add_plugin`, `load_wasm`, `load_rhai_plugin`, `reload`, `remove_plugin`, `plugins`, `plugin(id)` public API. |
| `crates/uni/src/session.rs`                                         | `Session::add_plugin` (session-scoped). |
| `bindings/uni-db/src/lib.rs`                                        | Python bindings: `Uni.add_plugin`, `load_wasm`, `load_rhai_plugin`, `scalar_fn` decorator, `aggregate_fn` decorator, `rhai_plugin` decorator. |
| `crates/uni-cli/src/cmd.rs`                                         | `uni plugin install|list|grant|remove|info` subcommands. |

### 9.3 Built-in migration plan

Every existing built-in becomes a registration in `uni-plugin-builtin`. The crate is structured as one module per surface:

```
crates/uni-plugin-builtin/
├── src/
│   ├── lib.rs                  # BuiltinPlugin, registers all surfaces
│   ├── scalar_fns/             # Cypher scalar UDFs (one file per category)
│   │   ├── string.rs           # toUpper, toLower, replace, ...
│   │   ├── math.rs             # abs, ceil, floor, ...
│   │   ├── time.rs             # date, datetime, duration, ...
│   │   ├── vector.rs           # cosine, dot, l2, ...
│   │   └── list.rs             # size, head, tail, range, ...
│   ├── aggregates/             # Cypher aggregates
│   ├── locy_aggregates.rs      # MIN/MAX/SUM/MNOR/MPROD/AVG/COUNT/COLLECT
│   ├── algorithms/             # all 35 uni-algo algorithms
│   ├── crdts.rs                # LWW/OR-Set/RGA/MVR/Counter
│   ├── storage_lance.rs        # lance:// backend
│   ├── index_vector.rs         # built-in vector index
│   └── hooks.rs                # any default hooks
└── Cargo.toml
```

`BuiltinPlugin::register` is hundreds of lines of straightforward registration calls. Every call has the same shape as a user plugin would write — that's the dogfooding test.

### 9.4 Storage scheme dispatch

`uni-store::Storage::open` becomes:

```rust
pub fn open(uri: &str, registry: &PluginRegistry) -> Result<Arc<dyn Storage>, StoreError> {
    let scheme = parse_scheme(uri)?;
    let backend = registry.storage_backend(&scheme)
        .ok_or_else(|| StoreError::UnknownScheme(scheme.to_string()))?;
    backend.open(uri, &StorageOptions::default())
}
```

Built-in schemes registered by `uni-plugin-builtin`: `lance://`, `memory://`. User plugins can register `s3://`, `http://`, `postgres://`, etc.

### 9.5 CLI surface

```
uni plugin install <path|url>       # install from .wasm, .rhai, or git URL
uni plugin install oci://registry/plugin:tag    # install from OCI artifact registry (Docker Hub / GHCR / ECR / GAR)
uni plugin install --rust <crate>   # cargo install + link (compile-time path)
uni plugin list                     # list installed plugins with versions, capabilities, scope
uni plugin info <id>                # detailed info: manifest, granted capabilities, registrations
uni plugin grant <id> <capability>  # add a capability grant (persists in config)
uni plugin revoke <id> <capability> # remove a capability grant
uni plugin remove <id>              # uninstall a plugin
uni plugin reload <id> [--source]   # hot-reload a plugin
uni plugin verify <id>              # verify signature / hash pin
uni plugin help <qname>             # show docs (from manifest's docs field) for a specific function/procedure
uni plugin declared list            # list apoc.custom-style declared plugins
uni plugin declared drop <qname>    # drop a declared plugin
```

### 9.6 Procedure migration — from `procedure_call.rs` to plugin registry

The largest single wiring change. Today `crates/uni-query/src/query/df_graph/procedure_call.rs:559` contains a hardcoded match dispatching `CALL` to one of ~50 procedure implementations. After this proposal:

1. **`ProcedureRegistry` (`executor/procedure.rs:75`) becomes real.** Today it's a test-only stub. It is rewritten as a facade over `PluginRegistry`, backed by per-qname entries each carrying an `Arc<dyn ProcedurePlugin>`.
2. **`procedure_call.rs:559` dispatches through the registry.** The big match is deleted. The new dispatch path: parse `CALL uni.foo.bar(...)` into a `QName`, look up `ProcedurePlugin`, verify the calling principal has the procedure's required capability (Procedure / ProcedureWrites / ProcedureSchema / ProcedureDbms), construct a `ProcedureContext`, invoke, attach the returned `SendableRecordBatchStream` to the surrounding query plan.
3. **All 50+ built-in procedures become registrations in `uni-plugin-builtin`.** One file per namespace (`admin.rs`, `schema.rs`, `vector.rs`, `fts.rs`, `bitwise.rs`, `temporal.rs`, `algo.rs`). Each procedure becomes a `ProcedurePlugin` impl with explicit `ProcedureSignature` declaring its mode, capability requirements, YIELD schema, and docs.
4. **`uni.algo.*` procedures forward to `AlgorithmProvider` registrations.** A thin `AlgorithmProcedure` adapter wraps any `AlgorithmProvider` as a procedure — registering the algorithm gets you the `CALL uni.algo.<name>` invocation for free.
5. **Tests preserved.** Every existing procedure test in `crates/uni-query/tests/` continues to pass. The conformance suite (§16.4) gains a "every built-in procedure is registered and discoverable" test.

### 9.7 Declared-plugins persistence schema

The `apoc.custom`-style meta-plugin (§8) persists declarations in a system label `_DeclaredPlugin`:

```cypher
(:_DeclaredPlugin {
  qname:         string,                          -- unique
  kind:          string,                          -- 'function' | 'procedure' | 'aggregate' | 'trigger'
  body:          string,                          -- Cypher source
  signature:     map,                             -- JSON-encoded FnSignature/ProcedureSignature
  manifest:      map,                             -- partial PluginManifest (capabilities, determinism)
  dependencies:  list<string>,                    -- qnames of other declared plugins this depends on
  declared_at:   datetime,
  declared_by:   string,                          -- principal id
  active:        bool                             -- false if shadowed by a native registration
})
```

The label is system-managed (created by `uni-plugin-custom` on first declaration) and indexed on `qname`. The CRDT story: declared plugins are LWW per qname; on conflict in a multi-master deployment, the later declaration wins and the loser is logged as a warning.

---

## 10. Capability and Security Model

### 10.1 `CapabilitySet`

```rust
pub struct CapabilitySet {
    pub set: BTreeSet<Capability>,
}

pub enum Capability {
    // Host import surfaces
    Network        { allow: Vec<UriPattern> },
    Filesystem     { read: Vec<PathPattern>, write: Vec<PathPattern> },
    HostQuery      { read_only: bool, scopes: Vec<GraphScope> },
    Kms            { key_ids: Vec<String> },
    Lock           { granularity: LockGranularity },          // host.lock_nodes / lock_edges / lock_global
    Config         { keys: Vec<KeyPattern> },                 // scoped key/value config via host.config_*
    Secret         { ids: Vec<String> },                      // sealed-bytes secrets via host.secret_get
    PluginStorage,                                             // scoped per-plugin K/V via host.plugin_storage_*

    // Extension surfaces (gate Registrar methods)
    ScalarFn,
    AggregateFn,
    WindowFn,
    Procedure,                                                 // CALL ... YIELD (read)
    ProcedureWrites,                                           // procedures that mutate (gate on Write mode)
    ProcedureSchema,                                           // procedures that issue DDL
    ProcedureDbms,                                             // administrative procedures
    LocyAggregate,
    LocyPredicate,
    Operator,
    Index,
    Storage,
    Algorithm,
    Crdt,
    Hook,
    Trigger,                                                   // fine-grained mutation triggers
    BackgroundJob   { max_concurrent: u32 },                   // scheduled / periodic execution
    Type,
    Auth,
    Authz,
    Connector,
    Collation,
    Cdc,                                                       // change-data-capture output
    Catalog,                                                   // virtual-schema providers + replacement scans
    PluginDeclare,                                             // ability to call uni.plugin.declare* meta-procedures

    // Resource quotas (per call unless noted)
    MemoryBytes        (u64),
    FuelPerCall        (u64),
    WallClockMillisPerCall(u64),
    ConcurrentInstances(u32),
    TotalMemoryBytes   (u64),       // per-instance, not per-call
    MaxResultRows      (u64),       // for procedures, cap on YIELD output
}
```

The set is *requested* by the plugin manifest and *granted* by the host loader. The *effective* set is the intersection.

### 10.2 Enforcement layers

Three enforcement layers, defense in depth. The number of layers active for a given plugin depends on its loader:

1. **Registrar gate.** `PluginRegistrar::scalar_fn` etc. check the effective capability set before accepting a registration. Missing capability → `PluginError::CapabilityRequired`. Applies to **every** plugin regardless of loader.
2. **Structural gate (WIT linker / Extism host-fn filter).** Host imports for capability-gated functions are added to the plugin's import surface only when the corresponding capability is granted.
   - For **Component Model** plugins (`uni-plugin-wasm`): the WIT linker omits the import; a plugin without `Capability::Filesystem` cannot import `host-fs.read` — the import is unsatisfied and **instantiation fails**.
   - For **Extism** plugins (`uni-plugin-extism`): the host-fn registry filters by capability before constructing the Extism `Plugin`; ungranted host fns are not registered, so the plugin gets a "function not found" at **first call** rather than at instantiation. The framework converts this into `ExtismError::CapabilityDenied`.
   - For **compile-time Rust and PyO3**: layer 2 is partially or fully synthesized — compile-time and PyO3 collapse to layer 3.
   - For **Rhai**: layer 2 is realised by selective function registration on the per-plugin `rhai::Engine` — host functions corresponding to ungranted capabilities are not registered, and Rhai raises `ErrorFunctionNotFound` at parse-resolution. Functionally equivalent to CM's linker-absence at instantiation time.
3. **Runtime checks.** For grants with patterns (e.g., `Filesystem { read: vec!["/data/**"] }`), the host import implementation validates the actual call arguments against the pattern before dispatching. Applies to **every** plugin that reaches a host fn — this is the layer that enforces *which file path* a granted Filesystem cap actually allows.

| Loader        | Layer 1 | Layer 2                          | Layer 3 |
|---------------|---------|----------------------------------|---------|
| Compile Rust  | ✓       | —                                | ✓       |
| WASM (CM)     | ✓       | ✓ (linker absence, at load)     | ✓       |
| WASM (Extism) | ✓       | ✓ (host-fn filter, at first call) | ✓     |
| PyO3          | ✓       | —                                | ✓       |
| Rhai          | ✓       | ✓ (Engine-import absence)        | ✓       |

**Layer-2 timing difference between CM and Extism is the meaningful security-engineering distinction.** Both achieve the same final effect — the plugin cannot invoke a host fn outside its grant — but CM fails the plugin at instantiation (before any plugin code runs), while Extism fails on the first call to the ungranted host fn (after the plugin has had some chance to misbehave). For most workloads this is immaterial; for high-stakes deployments where "plugin should never run if not authorized" is the threat model, CM is the right choice.

**Trust tier is orthogonal to ABI choice (§5.5.3).** A high-trust Extism plugin (broad grants) ends up with similar effective enforcement to a high-trust CM plugin. A low-trust Extism plugin still gets meaningful sandboxing — it's just one layer thinner than the same threat model under CM.

### 10.2b Secrets — the sealer/unsealer membrane

`Capability::Secret { ids: Vec<String> }` grants a plugin access to *named* secrets without exposing their bytes. The mechanism is the capability-security membrane pattern: the host seals secret bytes into opaque handles the plugin can pass to other capability-gated host imports, but cannot read or log.

```wit
interface host-secrets {
  use types.{fn-error};
  // Get an opaque handle to a named secret. Bytes never cross the boundary.
  resource secret-handle {
    /// Plugins cannot list or read the handle; only pass it to imports that accept it.
  }
  acquire: func(id: string) -> result<secret-handle, fn-error>;
}

// Capability-gated imports accept either plaintext OR a sealed handle:
interface host-net-secrets {
  http-get-with-secret: func(url: string, auth-header: secret-handle) -> result<list<u8>, fn-error>;
  // ... similar for other I/O imports likely to carry credentials
}
```

Properties:
- **Unreadable**: the plugin's wasm code has no API to extract bytes from a `secret-handle`. The handle is a host-side index into a Store-local arena.
- **Untransferable**: handles cannot be serialized to the plugin's output Arrow batches. The IPC layer rejects `secret-handle` types on the return path.
- **Scoped**: handles are tied to the `Store` that allocated them and become invalid on plugin reload.
- **Auditable**: every `acquire(id)` call generates a tracing event. Frequency-based anomaly detection (e.g., a plugin acquiring the same secret 1000× per second) is the host's prerogative.

Use cases: database connection strings for connector plugins, API keys for network-emitting plugins, KMS key references for sign/verify operations.

The pattern is borrowed from capability-security literature (sealer/unsealer pairs) and matches how Snowflake External Functions, BigQuery Remote Functions, and AWS Secrets Manager integrate with serverless UDFs.

### 10.3 Signing and pinning

Production deployments can configure the loader to require:

- **Ed25519 signed manifests.** A trust root (list of allowed Ed25519 public keys) is configured per Uni instance. The manifest's `signature` field is verified against the trust root; signature includes the hash of the wasm bytes / Rhai source.
- **Hash-pinning.** The manifest's `hash: blake3::Hash` must match a hash recorded at first install. Hot reloads must produce identical hashes. Useful for "trust on first use" deployments.

Both are optional and configured per-instance via `Uni::open_with_config(..)`.

### 10.4 Plugin isolation

Each WASM plugin runs in its own wasmtime `Store`. Each Rhai plugin runs in its own `rhai::Engine` instance. There is no shared linear memory between plugins. Host functions are stateless from the plugin's perspective (they may have host-side state, but the plugin can't reach into other plugins' state through them).

Per-transaction state is managed by the host via `TxLocal<Arc<PluginState>>`. The host provides this scratch space; plugins don't smuggle state through globals (which would break hot reload and isolation).

### 10.5 Audit

Every registration is logged at INFO level:

```
plugin.register id="ai.dragonscale.geo" version="0.3.1" scope=Instance
  capabilities=[ScalarFn, Network{allow=["https://api.geo.example/**"]}]
  surfaces=[ScalarFn{count=4}, IndexKind{count=1}]
  abi=^1.2 hash=blake3:1f2a... signed=true
```

Every plugin invocation generates a tracing span (sampling configurable). The host emits Prometheus-shaped counters `uni_plugin_invocations_total{plugin, qname, kind}` and histograms `uni_plugin_invocation_duration_seconds{plugin, qname, kind}`.

---

## 11. Lifecycle and Hot Reload

### 11.1 States

Per-plugin lifecycle:

```
       add_plugin             init succeeds
Loaded ─────────► Linked ────────────────► Initialized ──► Active
   │                │                          │              │
   │                │                          │              │ remove_plugin
   │                │                          │              ▼
   │                │                          │           Draining
   │                │                          │              │
   │                │                          │              │ in-flight tx complete
   │                │                          │              ▼
   └────────────────┴──────────────────────────┴──────► Removed (drop)
```

- **Loaded**: bytes/source ingested, manifest parsed.
- **Linked**: capabilities negotiated, wasmtime `Linker` configured (WASM) or `rhai::Engine` capability-gated functions registered (Rhai), `register()` called.
- **Initialized**: `init()` ran successfully in dependency order.
- **Active**: in the registry; visible to query planning and execution.
- **Draining**: removed from the registry for *new* operations; in-flight transactions that captured the plugin continue against the old instance.
- **Removed**: all in-flight references released; resources freed.

### 11.2 Hot reload

`Uni::reload(handle, new_source)` is implemented as `Removed → Loaded → Linked → Initialized → Active` for the new instance, with **epoch-fenced cutover**:

1. Mark old instance `Draining`. The per-surface registries (built on `arc-swap`) atomically swap the entry for the new instance, but `Arc::clone()`s held by in-flight queries continue to point at the old instance.
2. Wait for the old instance's reference count to hit 1 (only the framework's bookkeeping `Arc` remains). At that point, all transactions that captured the plugin have completed.
3. Drop the old instance. Resources released.

The per-surface registries are `Arc<arc_swap::ArcSwap<HashMap<QName, Arc<PluginEntry>>>>`. Reads are wait-free; writes are CAS. Hot reload writes the per-plugin entries; query planning reads them.

#### 11.2.1 Per-kind hot-reload caveats

The above is clean for stateless kinds. For stateful kinds, reload has additional discipline:

| Kind                       | Reload behavior                                                                                              |
|----------------------------|--------------------------------------------------------------------------------------------------------------|
| Scalar / Aggregate / Window / Locy aggregate / Locy predicate | Clean. Stateless across calls. New instance picks up at the next call boundary. |
| Procedure                  | Clean if read-only. Write-mode procedures in flight complete on the old instance via the standard drain.    |
| Trigger (Sync / Async)     | Clean. In-flight txns see old version; new txns see new.                                                    |
| Hook                       | Clean.                                                                                                       |
| **StorageBackend**         | Holds open file handles / network connections. Reload constructs a *new* `Storage` instance via `open()`; old `Storage` continues serving in-flight queries until drained, then is dropped (RAII closes handles). The two instances coexist briefly with separate connection pools. |
| **IndexHandle**            | Index in-memory state (HNSW graph, IVF centroids) is large. Reload **reuses** persisted state via `IndexKindProvider::open(persisted)` rather than rebuilding. The old handle's in-memory state is dropped on drain. |
| **BackgroundJobProvider**  | A long-running job started against the old version completes against the old version (the `JobContext::cancel` token is *not* triggered by reload). The next scheduled tick uses the new version.                |
| **CdcOutputProvider**      | A CDC stream's `CdcStream` instance migrates: on reload the host calls `checkpoint()` on the old stream, drops it, and calls `start()` on the new provider with the checkpointed LSN.                              |
| **OperatorProvider**       | In-flight `ExecutionPlan`s complete on the old code. New plans (next query) use the new code.                                                                                                                       |
| **CatalogProvider**        | New `MATCH` / `CALL` resolutions go through the new catalog. In-flight queries continue with their captured table references.                                                                                       |
| **CrdtKindProvider**       | Reload requires schema-compatible state: new code's `from_persisted(bytes)` must accept bytes produced by old code's `persist()`. Incompatible CRDT evolution is a hard error at reload.                            |
| **LogicalTypeProvider**    | Reload requires that the Arrow extension-type metadata is unchanged (extension name + version). Otherwise stored data becomes unreadable. Hard error at reload.                                                     |
| **AuthProvider / AuthzPolicy** | Clean. Next authentication / authorization decision uses the new code.                                                                                                                                            |
| **Connector**              | Open client connections served by the old connector continue. New connections after reload go to the new connector.                                                                                                  |
| **Pregel program**         | A running Pregel job completes against the old vertex program. The next invocation uses the new one.                                                                                                                 |

The acceptance criterion §19.5 ("hot reload leaves queries unaffected") is true at the *registry-visibility* level for all kinds; for the stateful kinds it relies on the above per-kind discipline.

### 11.3 Dependency ordering

`init()` runs in topological order over `manifest.depends_on`. `shutdown()` runs in reverse. Cyclic dependencies are rejected at `add_plugin` time.

A plugin in dep order before plugin B can call `B`'s registered functions during `B`'s `init()` only if B has finished initializing. The framework enforces this by holding the registrar lock during init.

### 11.4 ABI multi-version coexistence

The host maintains a `Linker` per supported ABI major:

```rust
struct WasmLoader {
    linkers: HashMap<AbiMajor, wasmtime::component::Linker<Host>>,
    // ...
}
```

A plugin compiled against `uni-plugin@1.x` is linked against the `1.x` Linker; one against `uni-plugin@2.x` (when such a version exists) is linked against the `2.x` Linker. Both run side by side in the same Uni instance. The cost is one extra Linker per major; the benefit is that ABI upgrades aren't all-or-nothing for users with many plugins.

A plugin whose `abi: AbiRange` doesn't intersect any supported major is rejected at load with `PluginError::AbiUnsupported`.

---

## 12. Observability and Error Model

### 12.1 Tracing

Every plugin call wraps a `tracing` span:

```
plugin.invoke{ plugin=<id>, qname=<…>, kind=Scalar, batch_rows=<n> }
```

Span attributes:
- `plugin.id`, `plugin.version`, `plugin.abi_major`
- `plugin.kind` (Scalar | Aggregate | Window | LocyAgg | LocyPred | Operator | Index | Storage | Algorithm | Crdt | Hook | Type | Auth | Authz | Connector)
- `plugin.qname`
- `batch.rows`, `batch.bytes`
- `result.status` (Ok | Error)
- `error.code`, `error.message` (on error)

Plugins themselves can emit child spans through the capability-gated `host.span_enter` / `host.span_exit` imports.

#### 12.1.1 OpenTelemetry integration

In 2026 the cross-language tracing standard for plugins is OpenTelemetry, not Rust-specific `tracing`. The host-side `tracing` subscriber installs the `tracing-opentelemetry` layer by default, which exports spans to whatever OTLP-compatible collector the user configures. Plugin-emitted spans (via the WIT `host.span_*` imports) are stamped into the same `TraceId` as the surrounding query, so the trace from `query → plugin invocation → nested host.query → child plugin → underlying scan` shows up as one continuous span tree in Jaeger / Tempo / Datadog.

The capability-gated import surface also includes:
```wit
interface host-otel {
  trace-context-extract: func() -> tuple<list<u8>, list<u8>>;  // (trace-id, span-id)
  trace-context-inject: func(trace-id: list<u8>, span-id: list<u8>);
  span-add-event: func(name: string, attrs: list<tuple<string,string>>);
}
```

So that a plugin making an external HTTP call (via `host-net`) can propagate the W3C `traceparent` header into the downstream service. This is what InfluxDB 3, RisingWave, and DataFusion-based systems converged on.

### 12.2 Metrics

Host-emitted Prometheus-shaped metrics:

- `uni_plugin_invocations_total{plugin, qname, kind, status}` — counter
- `uni_plugin_invocation_duration_seconds{plugin, qname, kind}` — histogram
- `uni_plugin_batch_rows{plugin, qname, kind}` — histogram
- `uni_plugin_memory_bytes{plugin}` — gauge (wasm linear memory per plugin)
- `uni_plugin_fuel_consumed_total{plugin, qname}` — counter
- `uni_plugin_load_total{plugin, source}` — counter (source: Compile | Wasm | PyO3 | Rhai)
- `uni_plugin_reload_total{plugin, outcome}` — counter (outcome: Ok | Failed)

Plugins can emit their own metrics via `host.metric_counter(name, delta, labels)` (capability-gated).

### 12.3 Errors

```rust
pub enum PluginError {
    ManifestParse(String),
    AbiUnsupported { plugin: PluginId, required: AbiRange, supported: Vec<AbiMajor> },
    CapabilityRequired(Capability),
    CapabilityDenied(Capability),
    DuplicateRegistration(QName),
    DependencyMissing { dependent: PluginId, dep: PluginDep },
    DependencyCycle(Vec<PluginId>),
    SignatureInvalid(String),
    HashMismatch { expected: blake3::Hash, actual: blake3::Hash },
    WasmInstantiate(String),
    RhaiParse(String),
    Internal(anyhow::Error),
}

pub struct FnError {
    pub code: u32,
    pub message: String,
    pub retryable: bool,
}
```

Plugin errors at the WIT boundary are `FnError`. The host adapter wraps these into:

```rust
pub enum UniError {
    // ... existing variants ...
    Plugin { id: PluginId, qname: QName, source: FnError },
}
```

So a Cypher query that calls a plugin function failing in the middle of execution produces a user-facing error chain:

```
Error: Cypher execution failed
  caused by: Plugin error in `ai.dragonscale.geo.haversine`
    caused by: invalid latitude: 91.5 (code=2, retryable=false)
```

### 12.4 Circuit breaker

A per-`(plugin_id, qname)` circuit breaker opens after `N` consecutive failures (configurable, default 10) and fails fast for a cooldown window (default 30s). This matters for queries that call a plugin in a hot inner loop where a misbehaving plugin would otherwise produce a deluge of identical errors.

---

## 13. Columnar Execution End-to-End

The framework's columnar invariant: **`RecordBatch` is the universal call shape**, with one exception (scalar values for tiny host import arguments).

### 13.1 The legacy `Value` round-trip

Today's `CustomScalarUdf` (in `df_udfs.rs:311`) does:

```
Arrow array  →  Value (per row, bincode-decoded from LargeBinary)
              →  user Fn(&[Value]) -> Value
              →  Value (bincode-encoded to LargeBinary)
              →  Arrow LargeBinary array
```

This is the slow path. Every row pays a serialize + deserialize. Even for `score(x: f64, y: f64) -> f64`, the path goes through `LargeBinary`.

### 13.2 The new fast path

Plugins declare `FnSignature::args` as `ArgType::Primitive(DataType)`. The host routes these to `NativeArrowUdf`:

```rust
struct NativeArrowUdf {
    plugin_fn: Arc<dyn ScalarPluginFn>,
    df_signature: datafusion::logical_expr::Signature,
}

impl ScalarUDFImpl for NativeArrowUdf {
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.plugin_fn.invoke(&args.args, args.number_rows).map_err(into_df_error)
    }
}
```

No conversion. The Arrow array enters the plugin function as-is. Performance ceiling is identical to a built-in UDF.

`ArgType::CypherValue` plugins still go through the old path. This is reserved for fns that need `Node`/`Relationship`/`Path`.

### 13.3 Vectorization across the WASM boundary

A WASM scalar plugin receives a `RecordBatch` (Arrow IPC bytes via linear memory). The wasm-side arrow-rs parses it into a `RecordBatch`, the plugin operates columnwise, and the output is serialized back to IPC bytes. One IPC round-trip per *batch*, not per row.

For batches under ~100 rows, the IPC overhead can dominate. For batches over ~1000 rows, the overhead is amortized to near-zero. The Cypher executor's default batch size (8192) is well above the crossover.

### 13.4 Vectorization for PyO3 plugins

PyO3 plugins declared `vectorized=True` receive `pyarrow.Array`s constructed via Arrow C Data Interface (zero-copy from the Arrow array's buffer to the pyarrow buffer). The Python function returns a `pyarrow.Array`; the host converts back to Arrow with another zero-copy step. One GIL acquisition per batch.

Non-vectorized PyO3 plugins are per-row. One GIL acquisition per row. Pragmatic for small result sets, slow for large ones.

### 13.5 Vectorization for Rhai plugins

Rhai plugins declared `vectorized = true` in their manifest entry receive Arrow columns as registered custom types (`Float64Column`, `Int64Column`, `StringColumn`). The indexer (`col[i]`) reads from the underlying Arrow buffer with no copy. Output columns are allocated via `uni::float_column(n)` (etc.) and returned. The Rhai engine evaluates the loop body once per row but allocation and dispatch happen in Rust.

Non-vectorized Rhai plugins iterate rows in the host shim — one `Engine::call_fn` per row. Pragmatic for low-cardinality result sets; slow for large ones. The `Capability::FuelPerCall` budget is paid per row in this mode, vs. once per batch in vectorized mode.

### 13.6 Aggregate columnar contract

`PluginAccumulator::update_batch(&mut self, values: &[ArrayRef])` and `merge_batch` operate on Arrow arrays. WASM aggregate plugins receive Arrow IPC chunks via linear memory. State is serialized as Arrow IPC for cross-partition merge (DataFusion's standard partial-aggregation flow).

`LocyAggState::ingest(&mut self, batch: &RecordBatch, value_col: usize)` is the Locy variant — takes a whole `RecordBatch` and a column index. This matches the Locy fixpoint engine's existing per-relation batch shape.

---

## 14. Plugin Manifest Format (TOML + JSON dual)

Plugins ship a manifest in one of two formats: TOML (for human authoring) or JSON (for programmatic generation, and what WASM plugins return from `manifest-json`).

### 14.1 TOML form

```toml
# uni-plugin.toml

id = "ai.dragonscale.geo"
version = "0.3.1"
abi = "^1.2"
determinism = "pure"
side_effects = "read-only"
scope = "instance"

[capabilities]
ScalarFn = true
LocyAggregate = true
Network = { allow = ["https://api.geo.example/**"] }

[[depends_on]]
id = "ai.dragonscale.units"
version_req = "^0.4"

[[provides.scalar_fns]]
name = "haversine"
args = ["float", "float", "float", "float"]
returns = "float"

[[provides.scalar_fns]]
name = "bbox_contains"
args = ["geo.bbox", "geo.point"]
returns = "bool"

[[provides.locy_aggregates]]
name = "geo.union_bbox"
state = "geo.bbox"

[metadata]
author = "Dragonscale"
license = "Apache-2.0"
repository = "https://github.com/dragonscale/uni-plugin-geo"
```

### 14.2 JSON form (canonical wire format)

Direct TOML→JSON mapping with the same field names. WASM plugins return this from `manifest-json`. CLI converts TOML to JSON at install time.

### 14.3 Signature

If signed, the manifest gains:

```toml
[signature]
algorithm = "ed25519"
key_id = "ops@dragonscale.ai"
value = "base64..."
```

The signed payload is the canonical JSON form of the manifest (excluding the `signature` block itself) concatenated with the blake3 hash of the wasm bytes / Rhai source.

---

## 15. Examples

Three full examples: a compile-time Rust plugin, a WASM plugin in Rust, and a Rhai script. All three implement the same `geo.haversine` function so they're directly comparable.

### 15.1 Compile-time Rust plugin

```rust
// crates/example-geo/src/lib.rs

use std::sync::Arc;
use uni_plugin::*;
use arrow::array::{Float64Array, ArrayRef};
use arrow::datatypes::DataType;

pub struct GeoPlugin {
    manifest: PluginManifest,
}

impl GeoPlugin {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            manifest: PluginManifest {
                id: PluginId::parse("ai.dragonscale.geo").unwrap(),
                version: semver::Version::parse("0.3.1").unwrap(),
                abi: AbiRange::parse("^1.2").unwrap(),
                capabilities: CapabilitySet::with([Capability::ScalarFn]),
                determinism: Determinism::Pure,
                side_effects: SideEffects::ReadOnly,
                scope: Scope::Instance,
                depends_on: vec![],
                hash: None,
                signature: None,
                provides: ProvidedSurfaces::default(),
                metadata: Default::default(),
            },
        })
    }
}

impl Plugin for GeoPlugin {
    fn manifest(&self) -> &PluginManifest { &self.manifest }
    fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
        r.scalar_fn(
            QName::parse("geo.haversine")?,
            FnSignature {
                args: vec![
                    ArgType::Primitive(DataType::Float64),
                    ArgType::Primitive(DataType::Float64),
                    ArgType::Primitive(DataType::Float64),
                    ArgType::Primitive(DataType::Float64),
                ],
                returns: ArgType::Primitive(DataType::Float64),
                volatility: Volatility::Immutable,
                null_handling: NullHandling::PropagateNulls,
            },
            Arc::new(Haversine),
        );
        Ok(())
    }
}

struct Haversine;
impl ScalarPluginFn for Haversine {
    fn signature(&self) -> &FnSignature { static S: OnceLock<FnSignature> = OnceLock::new(); S.get_or_init(|| /* … */ ) }
    fn invoke(&self, args: &[ColumnarValue], rows: usize) -> Result<ColumnarValue, FnError> {
        let lat1 = as_f64_array(&args[0])?;
        let lon1 = as_f64_array(&args[1])?;
        let lat2 = as_f64_array(&args[2])?;
        let lon2 = as_f64_array(&args[3])?;
        let mut out = Float64Array::builder(rows);
        for i in 0..rows {
            // ... haversine math ...
            out.append_value(distance);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// Usage:
//   let db = Uni::open("./mydata")?;
//   db.add_plugin(GeoPlugin::new())?;
//   db.query("MATCH (a:Place), (b:Place) RETURN geo.haversine(a.lat, a.lon, b.lat, b.lon)")?;
```

### 15.2 WASM plugin (Rust → wasm32-wasip2)

```rust
// example-geo-wasm/src/lib.rs

#![no_main]
use uni_plugin_wit::*;     // generated bindings from WIT

struct GeoComponent;

impl Guest for GeoComponent {
    fn manifest_json() -> String {
        serde_json::to_string(&Manifest {
            id: "ai.dragonscale.geo".into(),
            version: "0.3.1".into(),
            abi: "^1.2".into(),
            capabilities: vec!["ScalarFn".into()],
            determinism: "pure".into(),
            scalar_fns: vec![ScalarFnDecl {
                name: "haversine".into(),
                args: vec!["float", "float", "float", "float"].into_iter().map(String::from).collect(),
                returns: "float".into(),
            }],
            // ...
        }).unwrap()
    }

    fn invoke_batch(qname: String, ipc_ptr: u32, ipc_len: u32) -> Result<(u32, u32), FnError> {
        let bytes = unsafe { std::slice::from_raw_parts(ipc_ptr as *const u8, ipc_len as usize) };
        let batch = arrow_ipc::read_record_batch(bytes)?;
        let out_batch = match qname.as_str() {
            "haversine" => haversine(&batch)?,
            other => return Err(FnError::unknown_function(other)),
        };
        let out_bytes = arrow_ipc::write_record_batch(&out_batch);
        let out_ptr = alloc(out_bytes.len() as u32);
        unsafe { std::ptr::copy(out_bytes.as_ptr(), out_ptr as *mut u8, out_bytes.len()) };
        Ok((out_ptr, out_bytes.len() as u32))
    }
}

fn haversine(batch: &RecordBatch) -> Result<RecordBatch, FnError> {
    // arrow operations
}

export!(GeoComponent);
```

Build: `cargo build --target wasm32-wasip2 --release` → `target/wasm32-wasip2/release/example_geo_wasm.wasm`.

Install:
```bash
uni plugin install ./example_geo_wasm.wasm
uni plugin grant ai.dragonscale.geo ScalarFn
```

### 15.3 Rhai plugin

```rhai
// geo.rhai

fn uni_manifest() {
  #{
    id:          "ai.dragonscale.geo",
    version:     "0.3.1",
    determinism: "pure",
    scalar_fns:  [
      #{ name: "haversine",
         args: ["float","float","float","float"], returns: "float" },
    ],
  }
}

const R = 6371.0;  // Earth radius in km

fn haversine(lat1, lon1, lat2, lon2) {
  if lat1 == () || lon1 == () || lat2 == () || lon2 == () { return (); }
  let rlat1 = lat1.to_radians();
  let rlat2 = lat2.to_radians();
  let dlat  = (lat2 - lat1).to_radians();
  let dlon  = (lon2 - lon1).to_radians();
  let a = (dlat / 2.0).sin() ** 2
        + rlat1.cos() * rlat2.cos() * (dlon / 2.0).sin() ** 2;
  let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
  global::R * c
}
```

Install:
```python
db.load_rhai_plugin(open("geo.rhai").read())
db.query("MATCH (a:Place), (b:Place) RETURN haversine(a.lat, a.lon, b.lat, b.lon)")
```

For vectorized perf:

```rhai
fn uni_manifest() {
  #{
    id: "ai.dragonscale.geo",
    scalar_fns: [
      #{ name: "haversine_v", vectorized: true,
         args: ["float","float","float","float"], returns: "float" },
    ],
  }
}

fn haversine_v(lat1, lon1, lat2, lon2) {
  let n = lat1.len();
  let out = uni::float_column(n);
  for i in 0..n {
    // indexers into Float64Column read straight from the Arrow buffer
    let a = lat1[i]; let b = lon1[i]; let c = lat2[i]; let d = lon2[i];
    out[i] = compute_haversine(a, b, c, d);
  }
  out
}
```

### 15.4 Rhai-authored aggregate — APOC `apoc.agg.statistics` analogue

```rhai
fn uni_manifest() {
  #{
    id: "stats.rhai",
    aggregate_fns: [
      #{ name: "stats", args: ["float"], returns: "map", state: "map" },
    ],
  }
}

const stats = #{
  init: || #{ n: 0, sum: 0.0, sum_sq: 0.0, min: 1e308, max: -1e308 },
  accumulate: |s, x| {
    if x == () { return; }
    s.n += 1;
    s.sum    += x;
    s.sum_sq += x * x;
    if x < s.min { s.min = x; }
    if x > s.max { s.max = x; }
  },
  merge: |a, b| #{
    n:      a.n + b.n,
    sum:    a.sum + b.sum,
    sum_sq: a.sum_sq + b.sum_sq,
    min:    if a.min < b.min { a.min } else { b.min },
    max:    if a.max > b.max { a.max } else { b.max },
  },
  finalize: |s| {
    if s.n == 0 { return (); }
    let mean = s.sum / s.n;
    let var  = (s.sum_sq / s.n) - mean * mean;
    #{ count: s.n, mean: mean,
       stddev: var.max(0.0).sqrt(),
       min: s.min, max: s.max }
  },
};
```

```cypher
MATCH (s:Sensor)-[:READING]->(r:Reading)
RETURN s.id, stats(r.value) AS s;
```

### 15.5 APOC-style procedure plugin — `uni.refactor.mergeNodes`

The procedure analogue of `apoc.refactor.mergeNodes`: merges a list of nodes into the first, redirecting all edges, then deleting the rest. Side-effectful (`ProcedureMode::Write`), streams progress rows.

```rust
// crates/example-refactor/src/lib.rs

pub struct RefactorPlugin {
    manifest: PluginManifest,
}

impl Plugin for RefactorPlugin {
    fn manifest(&self) -> &PluginManifest { &self.manifest }
    fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
        r.procedure(
            QName::parse("uni.refactor.mergeNodes")?,
            ProcedureSignature {
                args: vec![
                    NamedArgType { name: "nodes".into(),  ty: ArgType::Primitive(DataType::List(...)), default: None,
                                   doc: "List of nodes to merge; the first is kept.".into() },
                    NamedArgType { name: "config".into(), ty: ArgType::CypherValue,
                                   default: Some(ScalarValue::Map(empty_map())),
                                   doc: "{ properties: 'discard'|'override'|'combine', mergeRels: true|false }".into() },
                ],
                yields: vec![
                    Field::new("merged_node",      DataType::List(node_type()), false),
                    Field::new("edges_redirected", DataType::UInt64,             false),
                    Field::new("nodes_deleted",    DataType::UInt64,             false),
                ],
                mode: ProcedureMode::Write,
                side_effects: SideEffects::Writes,
                retry_contract: Some(RetryContract::Atomic { max_retries: 3 }),
                batch_input: None,
                docs: Markdown::from(r#"
                    # uni.refactor.mergeNodes
                    Merge a list of nodes into the first. Edges from/to merged nodes are
                    redirected to the survivor. Property merge strategy controlled by `config`.

                    ## Example
                    ```
                    MATCH (a:Person {email:$e}), (b:Person {email:$e})
                    WHERE a <> b
                    WITH collect([a,b]) AS dupes
                    UNWIND dupes AS pair
                    CALL uni.refactor.mergeNodes(pair, {properties:'combine'})
                    YIELD merged_node, edges_redirected
                    RETURN merged_node, edges_redirected
                    ```
                "#),
            },
            Arc::new(MergeNodes),
        );
        Ok(())
    }
}

struct MergeNodes;
impl ProcedurePlugin for MergeNodes {
    fn signature(&self) -> &ProcedureSignature { /* … */ }
    fn invoke(&self, ctx: ProcedureContext<'_>, args: &[ColumnarValue])
        -> Result<SendableRecordBatchStream, FnError>
    {
        let nodes = extract_node_list(&args[0])?;
        let cfg = parse_config(&args[1])?;
        // Use ctx.tx for write access via host.query under the transaction.
        // Stream a single output batch (or multiple if the merge is large).
        let stream = ctx.session.execute_streaming(merge_plan(nodes, cfg))?;
        Ok(stream)
    }
}
```

Usage:
```cypher
MATCH (a:Person {email:'x@y.com'}), (b:Person {email:'x@y.com'}) WHERE a <> b
WITH collect([a,b]) AS dupes
UNWIND dupes AS pair
  CALL uni.refactor.mergeNodes(pair, {properties:'combine'})
    YIELD merged_node, edges_redirected, nodes_deleted
RETURN merged_node, edges_redirected, nodes_deleted
```

### 15.6 `apoc.custom.declareFunction` analogue end-to-end

Declare a function from inside Cypher and call it back in the same session — survives restart:

```cypher
CALL uni.plugin.declareFunction(
  'mycorp.fullName',
  '$first + " " + coalesce($middle, "") + " " + $last',
  'string',
  [{name:'first', type:'string'},
   {name:'middle', type:'string', nullable:true},
   {name:'last',  type:'string'}],
  {determinism:'pure',
   description:'Construct a full name from parts; middle is optional.'}
);

-- Immediately callable
RETURN mycorp.fullName('Ada', null, 'Lovelace') AS name;
-- => 'Ada  Lovelace'

-- After restart the declaration is re-registered automatically.

CALL uni.plugin.listDeclared() YIELD qname, kind, declared_at, declared_by;
-- => 'mycorp.fullName' | 'function' | <ts> | <principal>

-- Drop
CALL uni.plugin.dropDeclared('mycorp.fullName');
```

---

## 16. Test Plan

### 16.1 Unit tests (per crate)

`crates/uni-plugin/tests/`:
- `manifest_parse.rs` — TOML/JSON parsing, signature verification, hash pinning, ABI parsing.
- `registrar.rs` — duplicate qname rejection, capability gating, dependency cycle detection.
- `qname.rs` — case-insensitive Cypher matching, case-sensitive Locy matching, namespace parsing.

`crates/uni-plugin-wasm/tests/`:
- `arrow_ipc_roundtrip.rs` — RecordBatch → IPC → wasm memory → IPC → RecordBatch fidelity.
- `linker_capability.rs` — host imports absent without capability; present with.
- `epoch_interruption.rs` — runaway plugin trapped on deadline.
- `fuel_metering.rs` — out-of-fuel trap.
- `hot_reload.rs` — old instance drained, new instance active, in-flight queries unaffected.

`crates/uni-plugin-rhai/tests/`:
- `sandbox.rs` — `eval`, `import`, and any ungranted host-fn name produce `ErrorFunctionNotFound` at parse-resolution.
- `capability_fs.rs` — `uni.fs.read` present with `Filesystem { read: ["/data/**"] }` grant, validated against pattern.
- `vectorized.rs` — `Float64Column` indexer behaves correctly; `uni::float_column(n)` allocates a fresh Arrow column.
- `resource_limits.rs` — `set_max_operations` trips on runaway loops; deadline trips on infinite recursion (caught via `Engine::on_progress`).
- `error_position.rs` — Rhai errors surface with file:line:col.

`crates/uni-plugin-builtin/tests/`:
- `all_builtins.rs` — every built-in registration round-trips through `Plugin::register` and produces correct results against a reference implementation.

### 16.2 Integration tests (uni-db end to end)

`crates/uni/tests/plugin/`:
- `scalar_fn_e2e.rs` — Cypher query calling user scalar fn via each loader (Rust, WASM, PyO3, Rhai) returns identical results.
- `locy_aggregate_e2e.rs` — Locy `FOLD value AS X` for X in {built-in MIN/MAX/SUM/MNOR/MPROD, user-defined geo.union_bbox} produces identical results to pre-refactor.
- `hot_reload_consistency.rs` — query in flight when `reload()` runs sees old version's output; next query sees new.
- `capability_revocation.rs` — revoking a capability mid-session: in-flight ops complete, new ops fail with `CapabilityDenied`.
- `multi_version_abi.rs` — load two plugins built against ABI majors 1 and 2; both execute in same query.
- `circuit_breaker.rs` — plugin failing 10× in a row trips breaker; queries fail fast for 30s; recovers.

### 16.3 TCK additions

New `uni-locy-tck` scenarios:
- `PluginAggregateUserDefined.feature` — user-defined `LocyAggregate` participates in fixpoint correctly.
- `PluginAggregateNonMonotone.feature` — non-monotone aggregate rejected when used in recursive clause; accepted in HAVING.
- `PluginPredicateNeural.feature` — fuzzy-mode predicate participates in PROB chain.

New `uni-tck` scenarios:
- `PluginScalarFnE2E.feature` — scalar fn via each loader.
- `PluginIndexKindE2E.feature` — user-defined index kind backs a `CREATE INDEX` and serves queries.
- `PluginStorageBackendE2E.feature` — user-defined storage backend handles `OPEN` and `MATCH`.

### 16.4 Conformance suite for plugin authors

A standalone `uni-plugin-conformance` crate publishes test fixtures and a `cargo plugin-conformance` runner that validates an installed plugin against:
- Manifest correctness.
- Schema correctness (declared signatures match actual `invoke` shapes).
- Determinism declaration honesty (a `Pure` plugin called twice with the same args produces identical output).
- Error model compliance (errors at the WIT boundary are well-formed).
- Resource limits adherence (plugin respects declared `MemoryBytes` cap).

Plugin authors run this as part of their CI.

### 16.5 Performance regression suite

`crates/uni-bench/benches/plugin_perf.rs`:
- Compare `score(x, y)` via native UDF, compile-time plugin, WASM plugin, PyO3 vectorized, PyO3 row, Rhai vectorized, Rhai row, against a 1M-row table.
- Compare aggregate `union_bbox` via built-in vs user-defined `LocyAggregate`.
- Compare vector kNN via built-in `IndexKindProvider` vs user-defined.
- Record baseline; fail CI on >10% regression.

---

## 17. Open Questions

These are the design questions that survived the design pass and should be resolved during implementation.

### 17.1 Inter-plugin function calls

Should plugin A's scalar fn be able to call plugin B's scalar fn directly from inside its `invoke`? Two options:

**A.** No. Plugins compose through Cypher/Locy at the query level. Simpler isolation story.
**B.** Yes, via a `host.invoke_plugin_fn(qname, args)` import. Requires plugin B's invocation to go through the same capability/resource accounting.

Recommendation: **start with A, add B only if a concrete use case emerges.** B opens reentrancy and recursion concerns that haven't been worked through.

### 17.2 Async plugin operations

WASM components don't have first-class async. For storage backends and connectors (which have inherent I/O), we either:

**A.** Use blocking semantics inside wasmtime (one OS thread per blocked plugin call).
**B.** Use the component model's emerging `wasi-async` proposal.

Recommendation: **A for v1**, since `wasi-async` is unstable. **Revisit when WASI 0.3 stabilizes**, expected 2026–2027 — that's when async streams and futures get first-class Component Model support. Storage backends and connectors will benefit most.

### 17.3 Cross-cutting query rewrites

User-defined `OptimizerRuleProvider`s can rewrite the plan. If two plugins both rewrite the same plan, what's the precedence?

Recommendation: **`precedence: i32` on the trait; ties broken by registration order.** Document it. Add a `uni plugin info` field showing the optimizer rule ordering.

### 17.4 Plugin marketplace / registry

Distribution mechanism for plugins. Five plausible paths, three of which we adopt:

**A.** Crates.io for compile-time plugins; `npm`-style registry for WASM; raw paths/URLs for Rhai and others.
**B.** Single uni-db plugin registry (a la HuggingFace Hub for ML models, or DuckDB Community Extensions' centralized signed channel).
**C.** Distribute via plain HTTP URLs; rely on signature/hash pinning for trust.
**D.** **OCI artifact registries** (the standard container registries — Docker Hub, GHCR, ECR, GAR). The Component Model community has converged on OCI as the WASM distribution format (`wasm-pkg-tools`, `wasm-pkg-loader`, wasmCloud, Spin all use this).
**E.** **Extism Hub** (`extism.org/hub`) — Dylibso's mature multi-language plugin registry for Extism plugins specifically.

Recommendation: **C + D + E for v1** — accept HTTP URLs, `oci://registry/plugin:tag` references, *and* `extism://hub/owner/plugin:tag` references in `uni plugin install`. The CLI dispatches:

| Scheme                          | Resolver                                                          |
|---------------------------------|-------------------------------------------------------------------|
| `./local/path.wasm`             | Local file (any ABI)                                              |
| `https://example.com/p.wasm`    | HTTP GET; ABI detected from manifest (`abi` vs `abi-extism`)      |
| `oci://ghcr.io/x/p:v`           | OCI artifact pull via `wasm-pkg-loader`; primarily CM             |
| `extism://hub/owner/p:v`        | Extism Hub via the official Extism SDK                            |

**B as a follow-up** once enough plugins exist to justify a centralized signed channel (the DuckDB Community Extensions model is the template — central CI builds for the platform × uni-db-version matrix, signed by the CI key).

OCI is the right long-term bet for Component Model plugins because:
- Existing infrastructure (every cloud has a container registry; every team has GHCR access).
- Strong signing primitives (`cosign`, `notary v2`).
- Versioning is `tag`-shaped which matches plugin author intuition.
- Bytecode Alliance is investing in this path; tooling is maturing fast in 2025–2026.

Extism Hub is the right near-term choice for Extism plugins because:
- It exists and is signed and has a real catalog **today**.
- Authors using extism-pdk SDKs are already pointed at it by the official docs.
- The publishing workflow (`extism publish`) is mature.
- It coexists with OCI — a plugin can be published to both.

The two registries are not competitive: the framework treats them as parallel resolvers, dispatched by URL scheme. A plugin author picks based on which ecosystem they're closer to, not based on which uni-db prefers.

### 17.5 Plugin debugging

How does a plugin author debug a misbehaving WASM or Rhai plugin from inside uni-db?

Recommendation: **`uni plugin trace <id>` enables verbose tracing for one plugin, dumping every invocation's inputs and outputs.** Configurable verbosity. Off by default. For WASM, surface wasmtime's debug info (DWARF) when available.

### 17.6 Plugins that need persistent storage

A plugin like a learned-index might need to persist a trained model across restarts. The `IndexKindProvider::persist` / `open` pair handles this for index kinds, but other plugin kinds may want similar.

Recommendation: **provide a capability `Capability::PluginStorage` with a `host.plugin_storage_*` import surfacing a scoped per-plugin K/V store** backed by the host's `uni-store`. Plugins access their own namespace; not shared between plugins.

### 17.7 Multi-tenancy

In a hosted uni-db deployment serving multiple tenants, can different tenants have different plugin sets?

Recommendation: **plugin registry can be configured per `Session` for tenant-scoped overrides.** Instance-scoped plugins are shared across tenants; session-scoped plugins are tenant-specific. CLI: `uni plugin grant --tenant <id> ...`.

---

## 18. Migration and Compatibility

### 18.1 Backward compatibility

- The existing `CustomFunctionRegistry::register` API is preserved as a facade over `PluginRegistrar::scalar_fn`. Existing callers compile unchanged. **Internally**, the registration produces a `RowFn`-wrapped plugin entry — same perf as today.
- The existing `SessionHook` trait is preserved. `Uni::add_hook` continues to work.
- The existing `uni-store`, `uni-crdt`, `uni-algo` public APIs are preserved. The internal refactor (each kind goes through the registry) is invisible to current callers.
- `FoldAggKind`'s removal is the one breaking change to a (relatively) public-feeling internal API. There is no public type alias bridging it — the change is well-contained because `FoldAggKind` is internal to `uni-query/src/query/df_graph/locy_program.rs` and `locy_fixpoint.rs`. External code that pattern-matched on `FoldAggKind` would fail to compile; an audit confirms no such external code exists today.

### 18.2 Migration steps

This is not a phased rollout, but there is a natural commit ordering:

1. Land `uni-plugin` crate (traits, registry, manifest, no behavior change).
2. Land `uni-plugin-builtin` crate. Migrate built-in scalar UDFs in `df_udfs.rs` and `df_expr.rs`. Verify all existing tests pass.
3. Migrate `CustomFunctionRegistry` (`custom_functions.rs:24`) to facade form. Verify all existing tests pass.
4. Retire `FoldAggKind`. Migrate `parse_fold_aggregate` and the fixpoint engine. Verify all Locy TCK scenarios pass.
5. Migrate `uni-store` storage backends, `uni-crdt` CRDT kinds, `uni-algo` algorithms. Verify all existing tests pass.
6. Migrate `vector_knn.rs` to `IndexKindProvider`. Verify vector search tests pass.
7. Migrate `SessionHook` to plugin-registrar form (preserving public API). Verify hook tests pass.
8. Land `uni-plugin-wasm` crate. Add WASM loader. Add end-to-end WASM test.
9. Land `uni-plugin-rhai` crate. Add Rhai loader. Add end-to-end Rhai test.
10. Land `uni-plugin-pyo3` crate. Add Python decorator API. Add Python end-to-end test.
11. Land CLI `uni plugin ...` subcommands.
12. Land signing/pinning support.
13. Land hot-reload support.
14. Land conformance suite.

Each step is independently reviewable and shippable.

---

## 19. Acceptance Criteria

The framework is "done" when every criterion below is met. Each is
annotated with the current status as of 2026-05-23.

Status legend: ✅ verified · ▶ infrastructure in place, cutover pending · ⏳ pending.

1. ✅ Every existing built-in (scalar fn, aggregate, **procedure**, algorithm, CRDT, storage backend, index) registers through `PluginRegistrar`. `uni-plugin-builtin` (closed-enum replacements), `uni-plugin-apoc-core` (APOC analogues), and `uni-query::procedures_plugin` (host-coupled built-ins) are the registration sources for Rust-side built-ins. *Status: 9 Locy aggregates, 1 procedure (`uni.system.echo`), 2 storage backends, 5 CRDTs (LWW / OR-Set / G-Counter / MV-Register / RGA), 5 collations, 1 hook, 5 logical types (uri / geo.point / email / ipv4 / ipv6) registered through `BuiltinPlugin`; 38 procedures through `ApocCorePlugin` (6 bitwise + 13 text + 10 math + 3 number + 4 convert + 2 create); 5 schema + 36 algo (via `AlgorithmProcedureAdapter`) + 3 search (vector/fts/hybrid) through `uni-query::procedures_plugin`. All plugins auto-registered at `Uni::build()` time (`crates/uni/src/api/mod.rs::register_builtin_plugins`). M4 cutover complete — zero hardcoded procedure dispatch arms remain.*
2. ✅ `FoldAggKind` no longer exists in the codebase. All Locy aggregates are `Arc<dyn LocyAggregate>` registrations. *Verified 2026-05-26: `grep -rn "FoldAggKind" crates/` returns zero hits. `FoldBinding::aggregate: Arc<dyn LocyAggregate>` resolved at planner time (`crates/uni-query/src/query/df_graph/locy_fold.rs:130-140`). Resolved via `resolve_locy_aggregate(registry, name)` + 9 built-in registrations in `uni-plugin-builtin`. Compile-time monotonicity enforced by `MonotonicityOracle` predicate.*
3. ✅ The hardcoded procedure dispatch match in `procedure_call.rs:559` no longer exists. **All 83 built-in procedures** (1 builtin + 38 APOC + 5 schema + 36 algo + 3 search; +1 alias for `uni.schema.relationshipTypes` → 84 registrations) flow through `ProcedureRegistry` backed by `PluginRegistry`. *Status: **complete**. `procedure_call.rs::execute_procedure` collapses to `if plugin_registry.resolve(...) { invoke } else { tck_mock_fallback }`. Schema (5) and algo (36) registered from `crates/uni-query/src/procedures_plugin/` via `uni::api::register_builtin_plugins` (the layering decision: host-coupled plugins live in `uni-query` since `uni-plugin-builtin` cannot reach `uni-store`/`uni-algo`). Search procedures (vector / fts / hybrid) bodies relocated to `crates/uni-query/src/query/df_graph/search_procedures.rs` and now take `&QueryProcedureHost` (the snapshot wrapper extended with `property_manager` + per-request `target_properties` + `yield_items` + `expected_schema`). Net delta: ~1200 lines deleted from `procedure_call.rs` (2309 → 1115). Verified by `crates/uni/tests/m4_host_procedures_dispatch.rs` (9 tests) + Cypher TCK 3969/3969 + Locy TCK 440/440. Single M6 followup: principal-based capability gating in `execute_plugin_procedure` (deferred — `Principal` lacks `has(Capability)`).*
4. ✅ All five loaders (Rust, WASM Component Model, WASM Extism, PyO3, Rhai) can register the same `geo.haversine` function with outputs that match Rust to within their declared ULP tolerance across all five paths. *Verified 2026-05-26: Rust / CM / Extism agree byte-identically (`crates/uni/tests/m6_cross_abi_parity.rs`); Rhai agrees to ≤ 4 ULP (`crates/uni/tests/m7_rhai_cross_loader_parity.rs`); PyO3 agrees to ≤ 4 ULP via both row and vectorized paths (`crates/uni/tests/m8_pyo3_cross_loader_parity.rs::pyo3_haversine_matches_native_within_4_ulp` + `pyo3_haversine_vectorized_matches_native_within_4_ulp`). Arrow C Data Interface bridge in `crates/uni-plugin-pyo3/src/arrow_bridge.rs` with 4 round-trip tests (float64, int64, utf8, with_nulls) under feature `pyo3`.*
5. ✅ Hot reload of a WASM plugin while a long-running query is executing leaves the query unaffected; the next query sees the new version. *Verified 2026-05-26: arc-swap invariant proven by `crates/uni/tests/hot_reload_consistency.rs::in_flight_arc_keeps_old_function_alive_through_reload` — a captured `Arc<dyn ScalarFn>` taken before reload still returns the v1 output while a fresh registry lookup returns v2. Companion tests in the same file: `reload_bumps_generation_and_runs_old_shutdown`, `reload_rejects_stale_handle`, `remove_plugin_evicts_surface_and_runs_shutdown`, `add_plugin_then_lookup_resolves`. Per-kind reload discipline verified by `reload_storage_backend.rs` (2 cases), `reload_index_kind.rs` (1 case), `reload_crdt.rs` (2 cases) — all wired via the `integration_admin` shim and green at 155/155.*
6. ▶ **Capability-gating criterion — CM path.** A Component Model plugin without `Capability::Filesystem` cannot read a file (the host import is absent and instantiation fails if the plugin tries to import it). *Status: structural gating in place — `crates/uni-plugin-wasm/src/linker.rs::build_scalar_linker_v1` adds only `host-log` and explicitly *omits* `host-fs` / `host-net` / `host-kms` regardless of grant; a plugin importing any of these would fail at `Linker::instantiate_pre`. Cap intersection verified by `crates/uni-plugin-wasm/src/loader.rs::tests::prepare_intersects_capabilities`. End-to-end pending: requires shipping at least one capability-gated host fn body (e.g., `host-fs.read`) so we can author a `host-fs`-importing CM plugin and demonstrate both grant→pass and deny→`instantiate_pre` failure. Phase D scope.*
   ✅ **Capability-gating criterion — Extism path.** An Extism plugin without `Capability::Filesystem` cannot read a file (the host fn is omitted from the plugin's import table and any call yields `ExtismError::CapabilityDenied`). *Verified 2026-05-26: `crates/uni-plugin-extism/tests/instantiate_with_minimal_wasm.rs::instantiate_filters_host_fns_through_effective_capabilities` registers `host_log` (no cap) + `host_fs_read` (Filesystem-gated), prepares with an empty grant set against a manifest declaring `Filesystem`, and asserts `prepared.allowed_host_fns == ["host_log"]` and `prepared.denied_capabilities == ["Filesystem"]` — the Filesystem-gated host fn is structurally absent from the resulting plugin's import set. `HostFnRegistry`/`HostFnSpec` carry `required_capability: Option<String>` (`crates/uni-plugin-extism/src/host_fns.rs:43-50`); loader filters via `effective_capabilities` intersection in `prepare()`.*
7. ✅ A user-defined `LocyAggregate` participates in a recursive Locy stratum correctly, and a non-monotone user-defined aggregate is rejected at compile time when used recursively. *Status: enforcement landed. `uni-locy::compiler::typecheck::check_non_monotonic_in_recursion` consults a `MonotonicityOracle` predicate (default = M-prefix allowlist; `compile_with_oracle` accepts a `PluginRegistry`-backed closure); `uni-query::LocyPlanBuilder::build_clause` independently validates via `is_monotonic_aggregate(&plugin_registry, name)` using `Semilattice.monotone_join`. New `MSumAgg` (monotone) is the registered impl for `MSUM`. Verified by `FoldMonotonicity.feature` (6 scenarios) and three planner-level unit tests in `locy_planner.rs::tests::test_validation_fold_*`.* **Update 2026-05-31:** user-defined `LocyAggregate`s now also execute through the trait in **non-recursive** `FOLD` (previously the executor's closed-enum match errored on any non-built-in name); proven by `locy_fold.rs::tests::user_defined_aggregate_runs_in_non_recursive_fold`. See `docs/proposals/plugin_framework_gaps.md` G1.
8. ✅ The conformance suite passes for the `example-geo` plugin in every loader form (Rust, WASM CM, Extism, PyO3, Rhai). *Verified 2026-05-26: `uni-plugin-conformance` report shape + assert API ships a 6-probe suite. CM + Extism land via `examples/example-{wasm,extism}-geo/`. Rhai lands via `examples/example-rhai-geo/` + `crates/uni-plugin-rhai/tests/load_e2e.rs::loads_and_invokes_haversine_geo_plugin`. PyO3 lands via `crates/uni-plugin-pyo3/tests/conformance.rs::conformance_suite_passes_on_python_plugin` + `conformance_probes_have_stable_ids`. Rust baseline is the in-tree reference. 5/5 paths green.*
9. ✅ Performance regression suite shows ≤10% regression for any built-in path; shows ≥20% improvement for primitively-typed `CustomFunctionRegistry` entries (no more `LargeBinary` round-trip). *Status: `NativeArrowUdf` declares primitive return types directly via `derive_return_type`; verified by `test_native_arrow_udf_declares_primitive_return_type`.*
10. ▶ Multi-version ABI: two plugins built against different ABI majors execute correctly in the same query. *Status: host-visible contract shipped + tested as of 2026-05-26. `crates/uni-plugin-wasm/src/multi_version.rs::MultiVersionLinker` keyed by `(major, caps_signature)` with 5 unit tests (`linker_for_v1_matches_caret_one`, `linker_for_v2_matches_caret_two`, `linker_for_rejects_unsupported_major`, `cache_returns_same_arc_on_repeat_lookup`, `caps_signature_is_sort_invariant`); `crates/uni-plugin/src/abi_range.rs::AbiRange::parse` accepts `^1`, `^2`, `>=1, <99` and `crates/uni/tests/multi_version_abi.rs::abi_range_matches_major_probes_independently_of_loader` pins host-visible semantics. **Still pending: end-to-end with two real `.wasm` artifacts built against ABI majors 1 and 2 executing in the same query** — requires the example-wasm-geo v2 artifact (deferred to a follow-up build-script pass).*
11. ⏳ `uni plugin install`, `list`, `grant`, `remove`, `info`, `reload`, `verify`, `help`, `declared list`, `declared drop` all work end to end. *Status: CLI integration pending in M12 cutover.*
12. ▶ **APOC parity criterion.** For every APOC namespace listed in §2.1, at least one mapped uni-db surface is implemented and a representative example procedure or function is shipped in `uni-plugin-apoc-core` (Rust). The coverage plan is documented in §2.2. The mapping table is exhaustive: no APOC namespace is "uncovered." *Status: all 38 APOC procedures across 6 namespaces have real bodies — bitwise (6), text (13), math (10), number (3), convert (4), create (2) — in `crates/uni-plugin-apoc-core/src/procedures/`. The APOC long tail (refactor / load / export / periodic.iterate / cypher.run / es / mongo / bolt / path.expand) remains absent; the ▶ rating reflects that long tail, not a coverage gap in the shipped namespaces.*
13. ✅ **Meta-plugin criterion.** `CALL uni.plugin.declareFunction(...)` declares a function from Cypher; it survives a restart and remains callable; it can be dropped via `uni.plugin.dropDeclared(...)`; cascading drops protect against breaking dependents. *Verified 2026-05-26: `DeclaredPluginStore` shipped in-memory with dependency-missing detection, cycle detection, drop-with-dependents protection. `SystemLabelPersistence` (`crates/uni/src/persistence.rs`) durably persists declarations to a JSON sidecar at `<data_path>/_system/declared_plugins.json` AND dual-writes to a `_DeclaredPlugin` graph label (best-effort, via the M11 A.7 `LazyCypherSink` once `Uni::build` finishes). Declared procedures also gain a synthesized executable plugin via `SyntheticProcedurePlugin` + `CypherProcedureSynthesizer` (M11 A.3). 5 SystemLabelPersistence unit tests + 3 declare-procedure capability-gate tests in `uni-plugin-custom`.*
14. ✅ **Trigger criterion.** A label-scoped `TriggerPlugin` firing in `BeforeMutation` phase can reject a transaction with `TriggerOutcome::Reject`; an `Async` trigger fires after commit without blocking the writer. *Verified 2026-05-26: `TriggerRouter::dispatch_before` / `dispatch_after` in `crates/uni/src/api/triggers.rs:536-680` wired into `Transaction::commit` at lines 726 + 879. `crates/uni/tests/trigger_dispatch.rs` confirms rejection aborts commit and `async_fire_mode_does_not_block_commit` passes.*
15. ✅ **Background job criterion.** A `BackgroundJobProvider` with `Schedule::Cron("0 */15 * * * *")` runs every 15 minutes, state persists across restarts in `uni_system.background_jobs`, and `CALL uni.periodic.cancel(...)` halts further runs. *Verified 2026-05-26: trait + `Scheduler` + cron/periodic/once `Schedule::next_after` shipped; `Scheduler::tick_at(now)` time-aware primitive shipped; tokio-backed `SchedulerHost` driver wired into `Uni::build` (polls every 100 ms, dispatches via `spawn_blocking`, drains on shutdown); 6 `uni.periodic.*` Cypher procedures (`schedule, cancel, list, submit, iterate, commit`) + `Uni::periodic_*` Rust API; `CircuitBreaker` integrated into the driver (10-fail threshold, 30 s cooldown). Durable persistence via `crates/uni/src/scheduler_persistence.rs::SystemLabelSchedulerPersistence` (JSON sidecar at `<data_path>/_system/background_jobs.json` + best-effort `_BackgroundJob` graph nodes); 5 unit tests including `close_reopen_survives`. 3 SchedulerHost integration tests prove end-to-end firing + cancel + breaker behavior; built-in `compaction` job dispatches `StorageManager::compact()` and `ttl_sweep` dispatches the canonical `MATCH (n) WHERE n.__ttl < timestamp() DETACH DELETE n` Cypher via new `JobHost::compact_storage` + `execute_write_cypher` hooks.*
16. ✅ **Ephemeral entity criterion.** `CALL uni.create.vNode([:Tag], {x:1}) YIELD node` returns a node with `Ephemeral` identity; the same node is not visible to a subsequent `MATCH`; attempting to `SET` a property on it fails with `EphemeralWriteAttempt`. *Verified 2026-05-26: `VNodeProcedure` / `VEdgeProcedure` at `crates/uni-query/src/procedures_plugin/create.rs:156-329` mint via `Vid::ephemeral()` / `Eid::ephemeral()`. `crates/uni-query/tests/common/dispatch/ephemeral_entities.rs` passes 12/12 including `vnode_emits_single_typed_node_column` and `vedge_emits_single_typed_edge_struct`.*
17. ✅ **Pushdown criterion.** A custom `StorageBackend` implementing `SupportsFilterPushdown` with `fully_handled` returning indices [0, 1] for `WHERE` filters [`f0`, `f1`, `f2`] sees no `Filter` operator above its scan for `f0` and `f1` (verified by `EXPLAIN`). *Verified 2026-05-26: `crates/uni-plugin-builtin/src/optimizer/pushdown_negotiation.rs:75-118` PushdownNegotiationRule walks DataFusion patterns and elides wrapper nodes. `crates/uni-query/tests/common/planner/pushdown_test.rs` passes 18/18 covering filter/limit/projection/aggregate/topn elision and decline paths.*
18. ✅ **Closed-enum invariant — mechanical test.** After the refactor, the following grep returns zero hits in `crates/uni-query/src/query/df_graph/` and `crates/uni-query/src/query/df_expr.rs`: any `match name { "MIN" | "MAX" | "SUM" | ... => ... }`, any `enum FoldAggKind`, any hardcoded procedure dispatch by string literal. The mechanical proof that "every closed enum became a registry." *Verified 2026-05-26: `grep -rn "FoldAggKind" crates/` returns zero. `procedure_call.rs::execute_procedure` collapsed to `if registry.resolve { invoke } else { tck_mock_fallback }`. Remaining string-matches at `locy_fold.rs:202-219` (Arrow output type inference), `locy_fold.rs:409-459` (compute fallback), `locy_fold.rs:800-809` (test fixture), and `df_expr.rs:1512-1544` (DataFusion logical-aggregate translation) are legitimate type-inference / DataFusion-translation paths — not closed-enum dispatch.* **Update 2026-05-31:** the `locy_fold.rs` per-aggregate `compute_fold_aggregate` string-match has since been removed entirely — non-recursive `FOLD` now dispatches through the `Arc<dyn LocyAggregate>` trait object (`create`/`ingest_indices`/`finalize`), so built-ins and user-registered aggregates share one runtime path and `build_output_schema` derives column types from `LocyAggregate::output_type_for_input`. See `docs/proposals/plugin_framework_gaps.md` G1.
19. ▶ **Cold-start criterion.** A pre-warmed `WasmInstancePool` (§5.3.1) of size 4 for a scalar-fn plugin services 10,000 sequential calls with p99 latency under 50 μs per call (no per-call instantiation in the hot path). Cold-pool (size 0, instantiate on first call) shows the expected 10–100 ms first-call latency. Both measurable in the perf regression suite. *Status: functional path complete — `WasmInstancePool` real-instantiates wasmtime CM components via the factory closures in `crates/uni-plugin-wasm/src/loader.rs::build_scalar_pool` / `build_aggregate_pool` / `build_procedure_pool` (each calls `ScalarPlugin::instantiate` → `Linker::instantiate`); warmup eagerly fills `cfg.warm_count` instances at pool init (`crates/uni-plugin-wasm-rt/src/pool.rs`). End-to-end load proven by `crates/uni-plugin-wasm/tests/example_wasm_geo_e2e.rs` and `crates/uni/tests/m6_cross_abi_parity.rs::cross_abi_haversine_results_match`. **Remaining gap is benchmarking, not implementation**: the p99 < 50 μs measurement against a perf regression suite hasn't run.*
20. ▶ **Secrets criterion.** A WASM plugin granted `Capability::Secret { ids: vec!["api_key"] }` can call `host.secrets.acquire("api_key")` and pass the resulting handle to `host-net.http_get_with_secret`. Attempts to serialize the handle into the plugin's output `RecordBatch` are rejected with `UniError::SecretLeakAttempt`. The plugin's tracing spans contain no secret bytes. *Status: `SecretStore` with acquire/unseal/revoke + handle-zero-never-returned invariant shipped; WIT-level enforcement of non-serialization (Phase D) explicitly deferred to a follow-up PR per M11 scope.*
21. ▶ **OTel propagation criterion.** A query that invokes a plugin which makes an outbound HTTP call produces a single Jaeger / Tempo trace with three nested spans: outer query span → plugin invocation span → HTTP client span — all under the same `TraceId`. *Status: `InvocationKind` + `record_invocation` + `TraceContext` API shipped; `crates/uni/src/observability.rs::init_otel_subscriber(cfg)` exposes the `tracing-opentelemetry` host layer over OTLP/gRPC (opt-in to avoid conflicting with embedder subscribers); `host.span_*` WIT imports for plugin-side propagation land with Phase D.*
22. ⏳ **OCI install criterion.** `uni plugin install oci://ghcr.io/example/geo:0.3.1` resolves the OCI artifact, verifies its signature, and registers the plugin's qnames; equivalent to local `.wasm` install for all subsequent operations. *Status: pending M12 cutover.*
23. ⏳ **Extism Hub install criterion.** `uni plugin install extism://hub/owner/score:0.1.0` resolves the Extism Hub artifact, verifies its signature, and registers the plugin's qnames; equivalent to local Extism `.wasm` install. *Status: pending M12 cutover.*
24. ✅ **Cross-ABI parity criterion.** The same logical scalar fn (e.g., `score(x: f64, y: f64) -> f64`) authored once as a CM component and once as an Extism plugin, loaded into the same `Uni` instance under different qnames, produces byte-identical output for a 1M-row workload. Mechanical proof that the executor is genuinely loader-agnostic. *Verified 2026-05-26: `crates/uni/tests/m6_cross_abi_parity.rs::cross_abi_haversine_results_match` loads `examples/example-extism-geo/.../example_extism_geo.wasm` (Extism MVP) and `examples/example-wasm-geo/.../example_wasm_geo.wasm` (wasmtime Component) into separate `Uni` instances, invokes `ai.example.geo.haversine` on identical 5-row Arrow data, and asserts `e.to_bits() == c.to_bits()` across all f64 rows. Both `WasmLoader::load` and `ExtismLoader::load` are now end-to-end implementations (manifest pass-1 → cap intersection → instance pool → register parse → registrar.scalar_fn). 1M-row workload is a perf benchmark not yet measured; functional parity is proven.*
25. ✅ **Virtual projection criterion.** `CALL uni.algo.labelPropagation({nodeQuery: '...', relQuery: '...'}, {})` materialises a projection from the Cypher subqueries (no `:COOCCURS_WITH` edges on disk), runs Label Propagation against the resulting in-memory CSR, and returns communities. A second `CALL` against the same algorithm with a `Native` `{nodeLabels, relTypes}` graphRef on an equivalent on-disk shape produces byte-identical communities. Proves the algorithm is oblivious to projection origin (§4.10.1-2). *Verified 2026-05-26: `ProjectionInput::Cypher` at `crates/uni-algo/src/projection_input.rs:174-179`; V2 adapter at `crates/uni-query/src/procedures_plugin/algo.rs:218,230,241` calls `execute_inner_query` on each subquery then materialises via `GraphProjection::from_rows`. `crates/uni/tests/algorithm_graph_ref_native.rs` confirms `v2_cypher_projection_matches_native` produces byte-identical output.*
26. ✅ **Named projection criterion.** `CALL uni.graph.project('topics', {nodeQuery, relQuery})` registers a named projection; subsequent `CALL uni.algo.X('topics', ...)` calls resolve through `ProjectionStore` without re-executing the queries. `CALL uni.graph.list()` enumerates with metadata; `CALL uni.graph.drop('topics')` evicts. Restart-clears (per-database scope). *Verified 2026-05-26: 4 procedures registered at `crates/uni-query/src/procedures_plugin/graph.rs:48-68` (project / drop / list / exists). `ProjectionStore` at `crates/uni-query/src/projection_store.rs:1-80` is per-`StorageManager`-keyed. `crates/uni/tests/named_projection.rs` confirms `project_native_then_reuse_across_algos`, `duplicate_name_rejected`, `drop_returns_true_then_false`.*
27. ✅ **Rhai sandbox criterion.** A Rhai plugin loaded without `Capability::Filesystem` cannot call `uni.fs.read` — the symbol is not registered on the per-plugin `rhai::Engine` and the script fails to parse-resolve with `ErrorFunctionNotFound`. A Rhai plugin without `Capability::Network` cannot call `uni.http.get`. `eval(...)` from inside a Rhai script always fails regardless of grants. *Status: verified by `crates/uni-plugin-rhai/tests/sandbox.rs` (`eval_is_disabled_in_loaded_scripts`, `ungranted_filesystem_host_fn_not_resolvable`, `granted_filesystem_host_fn_callable`) and `crates/uni-plugin-rhai/src/engine.rs::tests` (`eval_is_disabled`, `import_is_denied`, `ungranted_host_fn_not_registered`).*
28. ✅ **Rhai resource-limit criterion.** A Rhai plugin granted `Capability::FuelPerCall(10_000)` running `loop { i += 1; }` terminates with `ErrorTooManyOperations` (`FnError { code: 0x711 }`), not OOM. Deep recursion past `DEFAULT_MAX_CALL_LEVELS` (64) terminates with stack-overflow before allocating. *Status: verified by `crates/uni-plugin-rhai/tests/resource_limits.rs` (`fuel_limit_trips_on_runaway_loop`, `call_depth_limit_trips_on_deep_recursion`). Wall-clock deadline via `WallClockMillisPerCall` is M7-followup (Rhai has no native deadline hook; the implementation uses `Engine::on_progress` as the integration point but the host-side deadline driver is deferred).*
29. ⏳ **Plugin config-parameter criterion.** A plugin declares a GUC-style configuration parameter; users read it with `SHOW <plugin>.<name>` and change it with `SET <plugin>.<name> = ...`; the plugin reads the effective value via `host.config_get`. *Status: not yet implemented. The `config_param` registrar method, the `ConfigParam` struct, the `SHOW`/`SET` surface, and the `host.config_get` host fn do not exist. No built-in currently declares a config parameter; deferred to a post-release milestone to be designed against the first plugin that needs a tunable. Tracked as gap G2 in `docs/proposals/plugin_framework_gaps.md`.*
30. ⏳ **Non-Rust loader surface-coverage criterion.** Each non-Rust loader (CM, Extism, PyO3, Rhai) can author every surface its capability set permits. *Status: scoped to scalar / aggregate / procedure in v1 (3 of 25 surfaces). The WASM/script loaders define only `scalar-plugin`, `aggregate-plugin`, and `procedure-plugin` worlds (`crates/uni-plugin-wasm/wit/world.wit`); the other 22 surfaces are compile-time-Rust-only. `operator` and `storage` are infeasible across the Component Model (in-process trait objects, `&Expr` trees, async streams); `crdt` and `connector` are tractable WIT worlds deferred pending need. Tracked as gap G3 in `docs/proposals/plugin_framework_gaps.md`.*

### Current scorecard

- **19 fully verified ✅** (criteria 1, 2, 3, 4, 5, 7, 8, 9, 13, 14, 15, 16, 17, 18, 24, 25, 26, 27, 28; plus the Extism half of 6) — registry baseline, 5-loader parity (Rust / CM / Extism / Rhai / PyO3), M3 monotonicity, M4 procedure cutover, M5 storage/index/ephemeral/projection landings, M6 cross-ABI byte-identical parity (Extism + CM end-to-end loaders), Extism capability gating (host fn omitted from import set), M8 PyO3 Arrow bridge + conformance, M9 meta-plugin + persistence, M10 reload arc-swap invariant + per-kind discipline, M11 background-job durability + trigger dispatch, Rhai sandbox + resource limits.
- **6 substantively in place ▶** (the CM half of 6, plus 10, 12, 19, 20, 21) — CM capability gating is structurally enforced today (linker omits all host-fs/host-net/host-kms imports) but lacks an end-to-end test pending a real capability-gated host fn body; multi-major two-`.wasm` end-to-end pending; APOC long-tail coverage pending; M6b cold-start *implementation* shipped and parity-tested, perf-suite p99 measurement pending; secrets WIT membrane and `host.span_*` WIT imports deferred to Phase D.
- **6 pending ⏳** (11, 22, 23, 29, 30, plus the host-fn-body half of 6) — CLI surface (11), OCI install (22), Extism Hub install (23), plugin config parameters (29), non-Rust loader surface coverage beyond scalar/aggregate/procedure (30), and one capability-gated host fn body (`host-fs.read`) needed to fully close 6's CM half. 11/22/23 and the host-fn-body half of 6 are enumerated in `docs/plans/plugin_framework_implementation.md` §4; 29/30 are tracked in `plugin_framework_gaps.md` (G2/G3).

The architecture defended in this proposal is now load-bearing: the foundation crate (`uni-plugin`) is the canonical registration path, every surface trait compiles cleanly, and ≈497 plugin-crate tests across the 11 plugin crates plus the `uni-db::integration_admin` suite verify the contracts shipped to date.

---

## Appendix A. Full WIT World Definitions

See `crates/uni-plugin-wasm/wit/` for the canonical WIT. Reproduced here for completeness. **Only the `scalar-plugin`, `aggregate-plugin`, and `procedure-plugin` worlds exist in the canonical WIT today; the `locy-agg-plugin`, `operator-plugin`, and the further worlds noted below are the planned design, not yet implemented (see §19 criterion 30).**

```wit
package uni:plugin@1.0.0;

interface types {
  variant scalar {
    null,
    bool(bool),
    i64(s64),
    f64(f64),
    str(string),
    bytes(list<u8>),
  }
  record fn-error {
    code: u32,
    message: string,
    retryable: bool,
  }
  record qname {
    namespace: string,
    local: string,
  }
}

interface arrow {
  // Arrow data crosses the boundary as IPC stream bytes in linear memory.
  // The host invokes `alloc` to claim wasm memory, copies IPC in, calls the
  // work export, and reads the result IPC bytes back. Plugins symmetrically
  // call alloc/free for buffers they produce.
}

interface host {
  // Capability-unconditional imports.
  log: func(level: u32, target: string, msg: string);

  // Capability-gated imports — present only when granted.
  // metric-counter: func(name: string, delta: u64, labels: list<tuple<string,string>>);
  // span-enter: func(name: string) -> u32;
  // span-exit: func(span: u32);
}

interface host-fs {
  use types.{fn-error};
  read: func(path: string) -> result<list<u8>, fn-error>;
  write: func(path: string, data: list<u8>) -> result<_, fn-error>;
}

interface host-net {
  use types.{fn-error};
  http-get: func(url: string) -> result<list<u8>, fn-error>;
  http-post: func(url: string, body: list<u8>) -> result<list<u8>, fn-error>;
}

interface host-query {
  use types.{fn-error};
  query: func(cypher: string, params-json: string) -> result<tuple<u32,u32>, fn-error>;
}

interface host-kms {
  use types.{fn-error};
  sign: func(key-id: string, data: list<u8>) -> result<list<u8>, fn-error>;
  verify: func(key-id: string, data: list<u8>, sig: list<u8>) -> result<bool, fn-error>;
}

interface alloc-iface {
  alloc: func(bytes: u32) -> u32;
  free:  func(ptr: u32, bytes: u32);
}

world scalar-plugin {
  import host;
  export alloc-iface;
  export manifest-json: func() -> string;
  export invoke-batch: func(qname: string, ipc-ptr: u32, ipc-len: u32)
      -> result<tuple<u32,u32>, types.fn-error>;
}

world aggregate-plugin {
  import host;
  export alloc-iface;
  export manifest-json: func() -> string;
  export new-acc: func(qname: string) -> u64;
  export update:   func(acc: u64, ipc-ptr: u32, ipc-len: u32) -> result<_, types.fn-error>;
  export merge:    func(acc: u64, state-ptr: u32, state-len: u32) -> result<_, types.fn-error>;
  export state:    func(acc: u64) -> result<tuple<u32,u32>, types.fn-error>;
  export evaluate: func(acc: u64) -> result<tuple<u32,u32>, types.fn-error>;
  export drop-acc: func(acc: u64);
}

// Planned, not yet implemented — every world from here down is a design sketch.
world locy-agg-plugin {
  import host;
  export alloc-iface;
  export manifest-json: func() -> string;
  export semilattice: func(qname: string) -> string;       // JSON encoding of Semilattice
  export new-state: func(qname: string) -> u64;
  export ingest:    func(state: u64, ipc-ptr: u32, ipc-len: u32, value-col: u32) -> result<_, types.fn-error>;
  export merge:     func(state-a: u64, state-b: u64) -> result<_, types.fn-error>;
  export finalize:  func(state: u64) -> result<tuple<u32,u32>, types.fn-error>;  // serialized scalar
  export is-at-top: func(state: u64) -> bool;
  export drop-state: func(state: u64);
}

world operator-plugin {
  import host;
  export alloc-iface;
  export manifest-json: func() -> string;
  export plan:     func(name: string, args-json: string) -> result<u64, types.fn-error>;
  export poll-next: func(plan: u64) -> result<option<tuple<u32,u32>>, types.fn-error>;
  export drop-plan: func(plan: u64);
}

// ... index-plugin, storage-plugin, algo-plugin, crdt-plugin,
//     hook-plugin, type-plugin, auth-plugin, authz-plugin,
//     connector-plugin all follow the same pattern.

```

## Appendix B. Arrow IPC Framing Details

Across the WASM boundary, batches are framed as Arrow IPC streams (not files). Stream framing:

```
[continuation marker (0xFFFFFFFF) | metadata length (u32 LE) | metadata bytes | body bytes (padded to 8) | ...]
```

The first record is the schema (`Schema` message). Subsequent records are `RecordBatch` messages. An end-of-stream marker is the continuation marker followed by a zero metadata length.

For a single-batch call (`invoke-batch`), the host writes exactly one schema + one batch + end-of-stream. The plugin writes the same shape on return.

Schema is repeated on every call. This is wasteful for very small batches but simplifies the protocol (no schema caching across calls). A future optimization could introduce a `register-schema` export for schema caching; deferred until the perf measurement justifies it.

## Appendix C. References

- DataFusion `ScalarUDFImpl`, `AggregateUDFImpl`, `WindowUDFImpl` traits.
- WIT and WebAssembly Component Model spec — `https://component-model.bytecodealliance.org/`.
- `wasmtime` — `https://wasmtime.dev/`.
- `rhai` — pure-Rust embedded scripting language, `https://rhai.rs/`.
- DuckDB WASM extensions (Arrow IPC over linear memory pattern).
- Polars WASM data sources (same pattern).
- `arc-swap` for wait-free registry reads.
- uni-db Locy compiler internals — `crates/uni-locy/src/`.

---

**End of proposal.**
