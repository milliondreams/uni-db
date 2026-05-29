# uni-db Plugin Framework — Implementation Plan

## Detailed, Milestone-Driven Execution Plan for `docs/proposals/plugin_framework.md`

**Status:** Draft — execution in progress
**Version:** 1.4.0
**Date:** 2026-05-25 (**M5 ✅ complete** — all 9 sub-tasks landed across Batches 1–3 plus the 2026-05-24/25 follow-up bundle and the final M5 closure commit `38db9b3eb`. Closure shipped: M5b.1 multi-label `MATCH (n:Virtual:Native)` intersection via `classify_labels` + `LeftSemi` join over `CatalogVertexScanExec`×`GraphScanExec`; M5b.2 write-path rejection (SET/DELETE on virtual labels and edge types); M5b.3 native↔virtual joins mid-pattern via `hydrate_virtual_target_from_catalog` + `plan_traverse_virtual_edge`; M5c.5 legacy 5-arg algorithm shim deleted; M5f.1 node-property refs in `predicate_source` (property-bag columns + Cypher UDF registration + `DummyUdf` resolution); M5f.2/M5f.3 L1 + edge pre-image property capture; M5g typed `Node` / `Edge` yields in `build_typed_column` (node uses `node_vid_source` flat-tuple expansion, edge uses `Struct(_eid,_type_name,_src,_dst,properties)`); M5h `StorageTableProvider` bridge with `EXPLAIN`-verified filter pushdown elision. Items genuinely blocked on later milestones remain routed there (Auth/Authz/Connector host consultation → M6a; `CrdtKindProvider` host consultation → M10; CDC scheduler runtime + checkpoints → M11; `TriggerOutcome::Defer` queue → M11; `ORDER BY ... COLLATE` grammar + wiring → M12). All M5 acceptance grep tests pass: 0 hits for `CALL uni.algo.*(.*\[.*\],` / `TODO(M5f-` / `v1 limitation\|v1 gap\|v1 cut\|v1\.1`. Test counts at closure: `uni-db` integration 681/681, `uni-query` 871/871, `uni-plugin-builtin` 93/93.)
**Worktree:** `plugin-fw`
**Companion documents:**
- `docs/proposals/plugin_framework.md` — design specification
- `docs/research/plugin_frameworks_sota.md` — SOTA survey
- `docs/plans/SESSION_FINAL.md` — current implementation state with per-crate test counts
- `docs/plans/REMAINING_WORK.md` — per-session driver enumerating what still needs to ship

## Current execution state (2026-05-25)

The plan is partly executed. Headline numbers:

- **330 tests pass** across the 10 plugin-framework crates (uni-plugin 80, uni-plugin-builtin 77, uni-plugin-apoc-core 33, uni-plugin-extism **53**, uni-plugin-wasm **12**, **uni-plugin-wasm-rt 18**, **uni-plugin-rhai 37**, uni-plugin-pyo3 6, uni-plugin-custom 7, uni-plugin-conformance 6). Plus the cross-ABI byte-parity test in `crates/uni/tests/m6_cross_abi_parity.rs` (Extism + CM byte-identical), the Rhai cross-loader parity test in `m7_rhai_cross_loader_parity.rs` (Rhai ≤ 4 ULP vs native), and the Uni-level `m7_rhai_load_e2e.rs` (2/2). `uni-query`'s pre-existing test suite continues to pass with no regressions.
- **0 regressions** across `uni-query` and `uni-db`'s pre-existing suites.
- **0 clippy warnings** under `-W warnings` on every M6 + M7 crate.
- **10 new crates** delivered: `uni-plugin`, `uni-plugin-builtin`, `uni-plugin-apoc-core` (APOC analogues), `uni-plugin-extism` (Extism ABI loader), `uni-plugin-wasm` (Component Model loader), `uni-plugin-wasm-rt` (shared IPC + Pool between the two wasm loaders), `uni-plugin-rhai` (Rhai-script loader), `uni-plugin-pyo3`, `uni-plugin-custom`, `uni-plugin-conformance`. Plus **three standalone example plugins** at `examples/example-extism-geo/`, `examples/example-wasm-geo/`, and `examples/example-rhai-geo/` exercising the full load + invoke path through each authoring path.

Per-milestone status is annotated inline in §2's milestone table and §4's per-milestone detail. The status legend used throughout:

- ✅ **complete** — fully implemented, tests-green, mechanically verified.
- ✅ **substantive** — primary architecture shipped and exercised end-to-end; cutover delete-of-legacy-path remains.
- ▶ **partial** — public-API surface live with real (non-stub) impl for some sub-surfaces; remaining sub-surfaces use clearly-flagged placeholders.
- ▶ **scaffolding** — public-API surface live; primary entry points return `NotYetImplemented`-class errors.
- ⏳ **pending** — not yet started.

---

## 0. How to Read This Plan

The design proposal commits to **one deliverable**: a complete v1.0 plugin framework with 25 surfaces, 4 loaders, hot reload, multi-version ABI, capability gating, OCI distribution, OpenTelemetry observability, and the `apoc.custom` meta-plugin. Nothing is deferred.

This plan covers the **execution order**: how to commit that deliverable as a sequence of milestones that each leaves the codebase in a green-test, shippable state. It is *not* a phased scope-cutting exercise. Every milestone advances toward the same single v1.0 target; later milestones depend on earlier ones, but no milestone defers a feature out of v1.0.

The unit of organization is the **milestone**. Each milestone is a self-contained, independently-mergeable, tests-green checkpoint with:

- explicit deliverable artifacts (crates, modules, traits);
- precise file-by-file changes anchored at line numbers from the current codebase;
- the tests that must be added to consider it done;
- mechanical acceptance criteria (`grep` tests, performance numbers, conformance pass/fail) — no "looks good";
- risks that could derail it;
- the parallel work it unblocks.

Milestones are designed to **leave the build green at every checkpoint**. A milestone that breaks existing tests is not done. The `FoldAggKind` retirement (M3) is the riskiest single milestone in this respect; we structure it carefully (§4 M3) so that the old and new aggregate paths coexist until the cutover commit.

---

## 1. Guiding Principles

These principles govern every commit produced under this plan.

### 1.1 Tests-green at every commit boundary

The `main` branch passes `cargo nextest run --workspace` and `cargo clippy --workspace -- -D warnings` after every milestone's final commit. Intermediate commits within a milestone may temporarily break tests *only* on a feature branch; the merge to `main` must be green.

The TCK suites (`uni-tck`, `uni-locy-tck`) pass at every milestone boundary. They are the strongest signal that we have not regressed Cypher / Locy semantics.

### 1.2 No backward-incompatible behavior changes without an in-milestone deprecation step

Public APIs (`Uni`, `Session`, `Transaction`, `CustomFunctionRegistry`, `SessionHook`) are kept stable. Internal APIs (`FoldAggKind`, the hardcoded match in `procedure_call.rs:559`) may change but the change is contained in one milestone with explicit before/after notes.

**Important clarification under the "everything ships, no versions" mandate:** there are no release boundaries, so "deferred to the next release / next deprecation window" is not a valid status. When a BC-incompatible cutover is needed, the deprecation step happens *within the same milestone* as the cutover: (1) ship the new shape alongside the old with a one-shot runtime `DeprecationWarning`, (2) migrate every in-tree caller, (3) delete the old shape — all three steps land in the same milestone, in that order. Earlier this doc used phrasings like "lands one release after X" or "deferred to the next release boundary per §1.2"; those phrasings are obsolete and have been removed. The deprecation window is a *temporal ordering of commits within a milestone*, not a calendar event.

### 1.3 Built-ins always go through the plugin path after M1

After M1 ships the `uni-plugin` crate, **no new built-in is added to the codebase except as a `Plugin` registration in `uni-plugin-builtin`**. New shortcuts that bypass the registry are forbidden; the integrity invariant is enforced by code review.

### 1.4 Mechanical acceptance only

Every milestone's "done" criterion is something a test runner verifies. "Reviewer approval that it looks right" is not acceptance — it is a precondition. Acceptance is `cargo nextest run -p uni-plugin --test e2e_scalar_fns` passes, or `grep -r 'enum FoldAggKind' crates/` returns zero hits.

### 1.5 Parallel where the DAG allows

The dependency graph in §3 makes parallel execution explicit. Milestones with no shared dependencies can run in different worktrees / branches concurrently. The plan identifies which milestones are independent.

### 1.6 No phantom commits

Avoid PRs that exist only to "make the structure prettier." Every PR is either: (a) a directly testable refactor toward a milestone; (b) the milestone's final cutover commit; or (c) a bug fix discovered during a milestone. Bookkeeping commits (`*.toml` reformat) are tolerated only when they are pre-conditions for a real change.

### 1.7 Documentation is shipped alongside code

Every milestone's PR includes the docs that exercise its surface: rustdoc on new traits, examples in `crates/uni-plugin-builtin/src/`, and updates to `docs/proposals/plugin_framework.md` if any open question was resolved during the milestone.

---

## 2. Milestones at a Glance

Twelve milestones, ordered for the critical path. Parallel opportunities noted in §6.

| #   | Milestone                                          | Goal                                                                                                                  | Status (2026-05-24) | Critical path? |
|-----|----------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|---------------------|----------------|
| M0  | Prework — scoping verification & test inventory    | Survey current tests; pin a `main` SHA; establish baseline perf numbers                                                | ✅ complete         | yes            |
| M1  | Foundation — `uni-plugin` core crate               | The trait, registry, manifest, capability traits — *no* host integration yet                                          | ✅ complete + extended (lifecycle / secrets / verify / scheduler / breaker / observability) | yes            |
| M2  | Scalar UDF migration — `CustomFunctionRegistry` facade + `NativeArrowUdf` | Migrate all scalar UDFs through `PluginRegistry`; ship perf win (no `LargeBinary` round-trip)                          | ✅ **complete** (cutover 2026-05-24) — facade (`custom_functions.rs`, 230 lines) over `PluginRegistry`; `plugin_adapter.rs` (302 lines) ships `ValueRowFn` (canonical `cypher_value_codec::encode`, fixed in `6057e285`); return-type derivation from `ArgType::Primitive(T)` works. **Cutover landed**: `read.rs:319-336` collapsed to the single `register_plugin_scalar_udfs` call; the legacy `register_custom_udfs` function and the `CustomScalarUdf` LargeBinary per-row adapter in `df_udfs.rs` are deleted. `grep -rn 'CustomScalarUdf\|register_custom_udfs' crates/` returns zero hits. uni-query 839/839 tests green post-cutover. | yes            |
| M3  | Closed-enum elimination — `FoldAggKind` retirement + Locy aggregate refactor + monotonicity enforcement | Delete `FoldAggKind`; all Locy aggregates become `Arc<dyn LocyAggregate>`; `Semilattice` metadata preserved and enforced for recursive strata | ✅ **complete** — `FoldAggKind` deleted; `FoldBinding` carries `name: SmolStr` + `aggregate: Arc<dyn LocyAggregate>` resolved at planner time from `HybridPhysicalPlanner.plugin_registry`. Hardcoded `aggregate_for_kind` match deleted. **Follow-up landed**: non-monotone aggregates in recursive strata now rejected at compile time. `uni-locy::compiler::typecheck` takes a `MonotonicityOracle` predicate (default = M-prefix allowlist; `compile_with_oracle` accepts a registry-backed closure). `uni-query::LocyPlanBuilder::build_clause` independently validates via `is_monotonic_aggregate(&plugin_registry, name)`. New `MSumAgg` carries `monotone_join: true` so `MSUM` resolves correctly (was an alias for non-monotone `SumAgg`). `grep -rn FoldAggKind crates/` returns zero; Locy TCK 440/440 (6 new `FoldMonotonicity.feature` scenarios), uni-query 839/839. | yes            |
| M4  | Procedure migration — `procedure_call.rs:559` retirement | All 50+ built-in procedures become `ProcedurePlugin` registrations across `uni-plugin-builtin` (closed-enum replacements), `uni-plugin-apoc-core` (APOC analogues), and `uni-query::procedures_plugin` (host-coupled built-ins that need `uni-store` / `uni-algo` types); `ProcedureRegistry` becomes real (not stub). | ✅ **complete** — every built-in procedure now flows through the plugin path. **83 procedures total** (1 builtin + 38 APOC + 5 schema + 36 algo + 3 search; +1 alias for `uni.schema.relationshipTypes` = 84 registrations). Hardcoded match arms in `procedure_call.rs::execute_procedure` are deleted; the function collapses to `if plugin_registry.resolve(...) { invoke } else { tck_mock_fallback }`. The pre-dispatch `uni.algo.*` schema-inference arm (line 264) is deleted; schema flows through `signature.yields` via the unified `_ =>` plugin-registry consult. `ProcedureContext` carries a `ProcedureHost` pointer; `QueryProcedureHost` is the in-tree implementation, now extended with `property_manager` + per-request `target_properties` + `yield_items` + `expected_schema`. Net delta: ~1400 lines deleted from `procedure_call.rs` (2309 → 922). M4 follow-up `6057e285` also fixed a pre-existing CypherValue codec mismatch in `ValueRowFn` (was JSON, now canonical tagged codec — see M2 row). Capability gating against `Principal` deferred to M6. | yes            |
| M5  | Remaining native surfaces — storage / index / catalog / algorithm + virtual projections / CRDT / hooks / triggers / pushdown / auth / connector / cdc | Trait + migration for every other in-process surface; `uni-plugin-builtin` is the *only* registration source. M5c also lands GDS-style virtual / named projections (proposal §4.10.1-3). | ✅ **complete (2026-05-25, commit `38db9b3eb`)** — all 9 sub-tasks shipped across Batches 1–3 + the 2026-05-24/25 follow-up bundle + the final closure commit. Items genuinely dependent on later milestone primitives have been **moved out of M5**: Auth/Authz/Connector host consultation → **M6a**; `CrdtKindProvider` host consultation → **M10**; CDC scheduler runtime + checkpoint persistence → **M11**; `TriggerOutcome::Defer` queue → **M11**; `ORDER BY ... COLLATE` grammar + sort/index wiring → **M12**. M5 closure commit `38db9b3eb` shipped the final 5 sub-tasks: **M5b.1** multi-label `MATCH (n:Virtual:Native)` intersection via `classify_labels` + `LeftSemi` join; **M5b.3** native↔virtual joins mid-pattern via `hydrate_virtual_target_from_catalog` + `plan_traverse_virtual_edge` (single-hop all-virtual edges); **M5f.1** node-property refs in `predicate_source` via `properties_new`/`properties_old` `LargeBinary` bags + `n.foo`/`old.foo` AST rewrite + Cypher UDF registration on the predicate `SessionContext` + `DummyUdf` resolution (two latent compile-path bugs fixed); **M5g** typed Node/Edge YIELD via planner expansion (`node_vid_source` tag) and `Struct(_eid,_type_name,_src,_dst,properties)` edge column; **M5h** `StorageTableProvider` + `StorageScanExec` + `StorageFilterPushdown` exposing `Arc<dyn Storage>` through DataFusion's `TableProvider` with `EXPLAIN`-verified pushdown elision. (Earlier closures: **M5b.2** runtime write-path rejection for SET/REMOVE/DELETE on virtual labels and edge types; **M5c.5** legacy 5-arg algorithm shim deleted + 30+ in-tree callers migrated; **M5f.2/M5f.3** L1 + edge pre-image property capture.) **Final follow-up bundle 2026-05-25** ships: M5g (commit `6ee91a26a` — ephemeral entities), M5e (commit `68b4fa0e6` — `BuiltinHookPlugin` + `Uni::add_plugin` sugar + commit-phase registry dispatch), M5h (commit `df285371d` — `LanceFilterPushdown` marker), M5d (commit `03030a7cd` — `TypedRgaProvider<T>` + `RgaElement` trait). **Batch 1 landed 2026-05-24** (M5a / M5d / M5e / M5i): **M5a ✅** — plugin `Storage` / `StorageBackend` traits are `#[async_trait]`; `LanceBackend::open` wired to `uni_store::LanceDbBackend::connect` behind the `lance-backend` cargo feature (default-on); `LancePluginStorage` adapter maps the 7-method plugin surface (incl. fork) onto uni-store's ~30-method async backend. Predicate pushdown / delete fast-path / monotonic `WriteHandle.id` polished in same-day follow-up; **Lance fork wiring** (`uni-plugin` 1.5.0) closes the final M5a follow-up — `Storage::fork(table, src, dst) → BranchMetadata` delegates to `LanceDbBackend::fork_branch`. Tempdir round-trip + fork tests green. **M5d ✅** — **5 CRDTs** registered: LWW, OR-Set, G-Counter, MV-Register, **RGA** (wraps `uni_crdt::Rga<String>`; ops carry pre-generated UUIDs for convergent peer-merge). Host consultation of the `CrdtKindProvider` registry from `uni-crdt` mutation paths is routed to **M10** (depends on per-kind CRDT reload discipline). Generic `Rga<T>` is routed to **M5g** (pairs with logical-types payload-typing). **M5e ✅** — phased `SessionHook` trait at `crates/uni-plugin/src/traits/hook.rs:18-54`; `LegacyHookAdapter` in `crates/uni/src/api/hooks.rs` wraps the legacy 4-method `SessionHook` and implements the phased trait (`before_query` → `on_parse`; `after_query` → `on_execute_end`; commit phases pass through). 8 bridge tests in `crates/uni/tests/hooks_bridge_test.rs`. **Routed**: `Session::add_hook` → `add_plugin` sugar to **M5b** (rides on the registry-consultation pattern). Phased-context shape v1.1 (`CommitContext.result`, `ParseContext.query_type` / `.params`) tracked in M5e detail section as the next `uni-plugin` minor ABI bump. **M5i ✅** — audit confirms `BasicAuthProvider` (Auth), `AllowGroupAuthzPolicy` (Authz), `NoopConnector`, `MemoryCdcOutputProvider`, `EmptyCatalog`, `NeverReplacementScan`, 5 collations all present, registered, and unit-tested. Host-consultation routing (where each surface gets wired into the host): Auth/Authz/Connector → **M6a** (meaningful only with external WASM plugins); Catalog/ReplacementScan → **M5b** (planner consultation hook); CDC → **M11** (depends on tokio scheduler runtime + `uni_system.cdc_checkpoints`); Collation → **M12** (Cypher grammar + `ORDER BY ... COLLATE` parser). ║ **Batch 2 landed 2026-05-24**: **M5b ✅** — planner consults `IndexKindProvider` at `plan_vector_knn` (registry-canonical), `CatalogProvider` + `ReplacementScanProvider` (`replacement_scan_resolves`); follow-up #4 wired the real `IndexHandle` bridge via `register_index_handle` (additive `uni-plugin` 1.7.1); follow-up #5 wired the planner identifier-resolution hook for procedure/function/label sites with a per-session `replacement_scans_enabled` gate; follow-up #6 wired virtual label-id / edge-type-id allocation + `CatalogVertexScanExec` / `CatalogEdgeScanExec` so `MATCH (n:External) RETURN n.foo` (and edge-type variants) return rows end-to-end. **M5c.1 ✅** — 36 algorithms registered as `AlgorithmProvider`s via `AlgoProviderBridge` host-callback. **M5h ✅** — pushdown marker traits + built-in `PushdownNegotiationRule` (Filter → TableScan elision when source claims `SupportsFilterPushdown::Full`); follow-up #2 added physical-phase rule installation via `OptimizerRuleProvider::physical_rule` (`uni-plugin` 1.7.0). **Batch 3 landed 2026-05-24 (uni-plugin 1.7.1 → 1.8.0)**: **M5c.2 ✅** — V2 `(graphRef, config)` adapter discriminating by `args[0]` JSON shape (Map → V2 `Native`; Array → legacy with one-shot tracing deprecation warning); per-arity-keyed `PluginRegistry::procedure_with_arity` + `procedure_overloads` accessors (additive forward-compat). **M5c.3 ✅** — `QueryProcedureHost::execute_inner_query` runs read-only inner Cypher against the outer L0 snapshot; `GraphProjection::from_rows` constructs CSR from inner-query rows; `AlgoProcedure::execute_with_projection` default + `GenericAlgoProcedure` override covers all 36 built-ins; `include_reverse` defaults to `true` because most algos need in-neighbors. **M5c.4 ✅** — `ProjectionStore` keyed on `Arc<StorageManager>` pointer identity (in-memory, restart-clears, drop-only eviction); `uni.graph.{project, drop, list, exists}` procedures in `crates/uni-query/src/procedures_plugin/graph.rs`. **Tests**: 14 (`algorithm_graph_ref_native.rs` + `named_projection.rs`). **Batch 3 follow-up landed 2026-05-24 (no `uni-plugin` ABI bump — pure host wiring on the 1.8.0 surface)**: **M5f ✅** — `crates/uni/src/api/triggers.rs` ships `TriggerRouter` (per-phase routing table built once per commit from `PluginRegistry::triggers()`) + `MutationEvents::from_l0` (extracts NODE_*/EDGE_* events from the tx-private L0 into the stable §4.18 RecordBatch schema). `Transaction::commit` wires `dispatch_before` (after legacy `before_commit` hooks, before the writer lock — `Synchronous` reject aborts via `UniError::TriggerRejected`) and `dispatch_after` (after legacy `after_commit` hooks — `Synchronous` inline with `catch_unwind`, `Async` / `EventualConsistency` spawned onto the tokio runtime). Empty-registry fast path skips L0 event extraction entirely. 9 integration tests in `crates/uni/tests/trigger_dispatch.rs`, 3 in `trigger_predicate.rs`, 3 in `trigger_predicate_properties.rs`, 2 in `trigger_defer.rs` + 3 unit tests. All M5f gaps from the original v1 framing — `predicate_source` Cypher compile, CREATE-vs-UPDATE distinction, `old_value` pre-image capture (L0 + L1 + edges), node-property refs in predicates — closed in the M5 follow-up bundle and final commit `38db9b3eb`. `TriggerOutcome::Defer` durable queue routed to M11 (depends on scheduler driver + `uni_system.deferred_triggers`). | no (parallel after M3) |
| M6a | WASM loader — `uni-plugin-extism` (user-facing) | extism-sdk + `HostFnRegistry` + Arrow IPC payloads + capability runtime filter; one example Extism plugin runs           | ✅ **complete (2026-05-25)** — M6a.1 (loader e2e: instantiate, manifest/register exports, IPC bridge, cap gate, scalar adapter, pool, `Uni::load_wasm_extism`, commit `5aef97e53`); M6a.2 (aggregate + procedure ABI: `ExtismAggregateFn` + `ExtismAggregateAccumulator` with length-prefixed state envelope, `ExtismProcedure` eagerly streaming via `RecordBatchStreamAdapter`, wire→internal sig translation for both, commit `0ca64f1`); M6a.3 (connector lifecycle: `Uni::{start,stop,active}_connector` + per-`UniInner` active map; authz write/schema/dbms verb classification in `Transaction::execute`; commit `675bf324f`). Auth host consultation at `Session::open` was wired in M5i; M6a closes the remaining surface. **69 tests in `uni-plugin-extism` (was 12), +9 admin-bucket integration tests in `uni-db`.** | yes (user-facing path) |
| M6b | WASM loader — `uni-plugin-wasm` (Component Model, trusted) | wasmtime + Component Model + WIT bindings + `WasmInstancePool`; one example CM plugin runs                              | ✅ **complete (2026-05-25)** — M6.shared lift to new crate `uni-plugin-wasm-rt` consolidating IPC + Pool across both loaders; M6b.1.1+1.2 WIT worlds (scalar/aggregate/procedure) + compile-time `wasmtime::component::bindgen!` (zero-drift, no committed bindings); wasmtime bumped 26 → 41 to align with extism transitive; M6b.1.3+1.4+1.5 real `WasmLoader::load` with two-pass dance, per-major `Linker<HostState>` with capability-gated host imports (structural enforcement via linker absence), epoch interruption + fuel metering hooks per manifest; M6b.1.6+1.7 pool prewarm + `Uni::load_wasm_component` behind `wasm-plugins` feature + standalone `examples/example-wasm-geo/` with **full Arrow-IPC `invoke-scalar` via `wit_bindgen::generate!`** (commit `39968f4b6` from deferred-followup); M6b.2 `ComponentAggregateFn` + `ComponentProcedure` adapters mirroring the M6a.2 extism shape. One wasmtime version in Cargo.lock (41). 12 `uni-plugin-wasm` tests (incl. 2 real-CM e2e tests on the wasm32-wasip2 component binary) + 18 `uni-plugin-wasm-rt` (shared IPC/pool) + 53 `uni-plugin-extism` (incl. 2 real-extism e2e tests) = 83 tests across the three crates. **Cross-ABI byte-parity test** (proposal §19 #24) at `crates/uni/tests/m6_cross_abi_parity.rs` confirms both ABIs produce byte-identical f64 outputs for `ai.example.geo.haversine` across a 5-row test matrix. | no |
| M7  | Rhai loader — `uni-plugin-rhai`                    | Host-embedded `rhai::Engine` per plugin; sandboxed-by-default; capability-gated host fns registered into the Engine; resource limits via `set_max_operations` / `set_max_call_levels` / `set_max_memory`. Activates the `geo.haversine` Rhai example end-to-end. | ✅ **substantive** — Phases 1–11 landed: crate scaffold + host-fn registry + Engine factory (eval disabled, deny-all module resolver, FuelPerCall → `set_max_operations`, default call-depth 64), manifest parser (`uni_manifest()` → Rhai Map → wire structs), scalar adapter (row + vectorized), aggregate adapter (init/accumulate/merge/finalize with serde_json state envelope for partial aggregation), procedure adapter (Array&lt;Map&gt; → RecordBatch → SendableRecordBatchStream), full loader three-phase shape mirroring `ExtismLoader::load`, host-fn impls (fs/net/kms/secret with capability gating), `Uni::load_rhai_plugin` API behind default-on `rhai-plugins` feature, vectorized Float64/Int64/Utf8 column userdata + `uni_float_column` allocator, `examples/example-rhai-geo` runnable bin, `db.load_rhai_plugin` PyO3 binding, `uni plugin install foo.rhai` CLI dispatch, cross-loader parity test (Rhai joins as 4-ULP tier), `PluginError::RhaiParse` variant. **37 tests in `uni-plugin-rhai`** (engine sandbox + host-fn registry + manifest parser + dynamic bridge + scalar adapter + aggregate adapter + procedure adapter + columns + integration: load_e2e / sandbox / resource_limits / vectorized). | no (parallel after M6) |
| M8  | PyO3 loader — `uni-plugin-pyo3`                    | Live Python UDFs via PyArrow zero-copy; GIL contention documented; session scope default                              | ✅ **complete (2026-05-26)** — substantive sweep M8.1–M8.8 plus the session-scope + bindings follow-ups (F1 + F2). F1 ships per-session plugin registry on `uni_db::Session` (`session_plugin_registry: Arc<PluginRegistry>`, fresh-empty per `Session::new` / `new_forked` / `new_from_template`, shared across `Clone`) + `Session::add_python_plugin` / `Session::finalize_python_plugin` + dual-consult resolution across scalar UDFs (`register_plugin_scalar_udfs_pair` + a fresh `Uni::load_*_plugin` registration path), procedures (`ProcedureRegistry::get_plugin` consults task-local first), and Locy aggregates (`resolve_locy_aggregate` consults task-local first). The task-local mechanism (`tokio::task_local! SESSION_PLUGIN_REGISTRY`) is set at all `Session::query_with(...).fetch_all|cursor|profile()` boundaries plus the cached / time-travel / fresh-plan execute paths inside `Session::execute_cached`. F2 ships the bindings decorator surface: `Session::scalar_fn / aggregate_fn / procedure / set_plugin_id / set_plugin_version / finalize_plugin / load_python_plugin` `#[pymethods]` on the sync `Session` pyclass, backed by per-`Session` `pending_plugin_builder: Arc<ManifestBuilder>`. The trampoline class types (`PyDecoratorSink` / `PyDecoratorTrampoline`) and per-decorator constructor helpers (`make_scalar_trampoline` / `make_aggregate_trampoline` / `make_procedure_trampoline`) are now `pub` (`#[doc(hidden)]`) on `uni-plugin-pyo3` so the bindings reuse them without duplicating the manifest-builder marshalling logic. **3/3 Rust session-scope tests** (`crates/uni/tests/m8_pyo3_session_scope.rs`: `session_local_visibility`, `session_drop_unregisters`, `session_shadows_instance`) + **7/7 Python pytest** (`bindings/uni-db/tests/test_python_plugin.py`: source-load × 3, decorator + finalize × 3, session isolation × 1) all green; existing 38 M8 tests + 4 rhai tests retain no regression. The bug exposed during F1.e (instance-level `Uni::load_*_plugin` registrations were silently invisible to Cypher because the executor's UDF-registration path only walked `CustomFunctionRegistry`'s shadow) is fixed in `executor/read.rs::create_datafusion_planner`: the path now also walks `procedure_registry.plugin_registry()` (the host's instance registry) and the per-task session-local registry. **Async bindings parity shipped (2026-05-26)** — `bindings/uni-db/src/async_api.rs::AsyncSession` gains the same `scalar_fn` / `aggregate_fn` / `procedure` / `set_plugin_id` / `set_plugin_version` (sync `#[pymethods]`) + `finalize_plugin` / `load_python_plugin` (async via `pyo3_async_runtimes::tokio::future_into_py`) surface as the sync `Session`. The async methods lock the inner `tokio::sync::Mutex<::uni_db::Session>`, dispatch to the (sync) Rust `Session::finalize_python_plugin` / `add_python_plugin` methods, and re-acquire the GIL via `Python::attach` inside the future for `add_python_plugin`'s `Python<'_>` arg. Both `AsyncSession` construction sites (`AsyncDatabase::session()` ~line 327 and the fork-path at ~line 3958) initialize a fresh `Arc<ManifestBuilder>` per session. **7/7 async pytest tests** in `bindings/uni-db/tests/test_async_python_plugin.py` mirror the sync layout (source-load × 3, decorator+finalize × 3, session-isolation × 1); sync 7/7 + rhai 4/4 + Rust M8 7/7 all retain zero regression. M8 follow-ups now fully close. **Substantive original sweep** (preserved for context): full crate rewrite delivered M8.1 through M8.8 minus M8.6/bindings. **Shipped**: M8.1 Arrow ↔ PyArrow zero-copy bridge via the Arrow PyCapsule Interface directly on arrow-array's FFI types (drops `pyo3-arrow` dep — incompatible with our `abi3-py310` pin; 4 round-trip tests). M8.2 vectorized scalar adapter (one GIL per RecordBatch, marshal columns as PyArrow Arrays). M8.3 row-mode scalar adapter (one GIL per batch, iterate rows inside). M8.4 manifest + `_uni_decorator_sink` global + `PythonPluginLoader` three-phase load + `PyPluginHandle` implementing `uni_plugin::Plugin`. M8.5 aggregate adapter — four-callable spec (`init`/`accumulate`/`merge`/`finalize`), cross-partition state via JSON envelope (design decision: user-supplied `merge`, no pickling). M8.7 procedure adapter (iterable-of-dicts → RecordBatch via `SendableRecordBatchStream`) + `Uni::load_python_plugin` behind `pyo3-plugins` feature; uni's `pyo3` dep overrides the workspace's `extension-module` pin with `auto-initialize` so standalone `cargo test -p uni-db --features pyo3-plugins` links libpython, while `bindings/uni-db` builds keep `extension-module` via feature unification. M8.8 conformance suite (6/6 probes pass for a Python plugin), cross-loader parity (PyO3 joins the matrix at ≤ 4 ULP on the canonical haversine fixture — both row and vectorized modes), runnable `examples/example-pyo3-geo` bin printing expected city-pair distances. **Tests: 34 in uni-plugin-pyo3, 4 in uni-db integration (m8_pyo3_load_e2e + m8_pyo3_cross_loader_parity)**. **Deferred to M8-followup**: M8.6 session-scoped registry consultation in the function-resolution hot path (the riskiest piece — touches `crates/uni/src/api/session.rs` + DataFusion `FunctionRegistry` glue); `bindings/uni-db` decorator surface (`@db.scalar_fn` on a `Database` pyclass — pairs with M8.6 since both touch session-scope semantics). `Uni::load_python_plugin` lands as instance-scoped today; the proposal §5.4.2 session default ships in the follow-up. | no (parallel after M2) |
| M9  | Meta-plugin — `uni-plugin-custom` (`apoc.custom`)  | `uni.plugin.declareFunction/Procedure/Aggregate/Trigger`; persistence in `_DeclaredPlugin`                              | ✅ **complete (2026-05-26)** — all 6 procedures registered; `declareFunction` end-to-end (parse → synthetic `ScalarPluginFn` → registry); `JsonFilePersistence` + reactivation; cycle/cascade/shadow handling; 20 tests pass. **Routed to M11:** `_DeclaredPlugin` system-label persistence cutover + `declareProcedure`/`declareTrigger` body execution (both gated on write-enabled `ProcedureHost::execute_inner_query`). | no (parallel after M4 + M5) |
| M10 | Hot reload + lifecycle + multi-version ABI         | `arc-swap` registries; epoch-fenced cutover; per-kind reload discipline; per-major `Linker`                            | ✅ **complete (2026-05-27)** — `EpochFencedReload` driver (`crates/uni-plugin/src/lifecycle.rs`, 440 lines) ships `begin_drain` / `wait_for_drain` (polls `Arc::strong_count` with timeout) / `finalize`. Per-kind reload discipline shipped: `StorageBackend::open()` (fresh instance, old continues for in-flight); `IndexKindProvider::open(persisted_bytes)` (persist→open round-trip); `CrdtKindProvider::schema_compat_check()` + `LogicalTypeProvider::compat_check()` (incompatible reloads are hard errors); `BackgroundJobProvider` / `CdcOutputProvider` checkpoint-on-old + start-on-new. `MultiVersionLinker` (`crates/uni-plugin-wasm/src/multi_version.rs`, 199 lines) caches per-`(major, caps)` Linkers across `SUPPORTED_MAJORS = [1, 2]`. **`uni-crdt` host consultation refactor**: `Crdt::merge_via_registry` (`crates/uni-crdt/src/registry_dispatch.rs`) bridges the native enum to plugin providers via msgpack round-trip; all 5 built-in CRDTs (LWW, OR-Set, G-Counter, MV-Register, RGA) dispatch through the registry. **Public APIs**: `Uni::add_plugin`, `Uni::reload`, `Uni::remove_plugin`, `Uni::plugins`, `Uni::plugin(id)` all shipped; `UniPluginEntry` exposes `lifecycle: Arc<PluginLifecycle>` + `generation: u64` (bumped per reload). **Tests (15 M10 acceptance + 4 CRDT registry-dispatch = 19 green)**: `crates/uni/tests/{hot_reload_consistency,multi_version_abi,reload_crdt,reload_index_kind,reload_storage_backend}.rs` (bundled into `integration_admin` — 11 tests) + `crates/uni-crdt/tests/registry_dispatch.rs` (4 tests). Broader plugin-crate test runs: `uni-plugin` + `uni-plugin-wasm` + `uni-plugin-wasm-rt` = 132 tests, 0 failures. | no (parallel after M6) |
| M11 | Capabilities + security + observability + scheduling | Manifest signing; hash pinning; sealer/unsealer secrets; OTel propagation; `BackgroundJobProvider` + scheduler; **write-enabled `ProcedureHost::execute_inner_query` + M9 declared-plugin cutover** (routed in from M9) | ✅ **complete (2026-05-27)** — Phases A+B+C plus the six FU follow-ups (FU-1 principal plumbing through `CURRENT_PRINCIPAL` task-local + `ProcedureContext::with_principal`; FU-2 secret-handle Arrow extension `uni-db.secret-handle` + IPC encode/decode rejection with `IpcError::SecretLeakAttempt`; FU-3 `current_traceparent` extraction + `http_get_with_traceparent` outbound HTTP + `examples/otel-demo/` running end-to-end; FU-4 `crates/uni/src/cdc_runtime.rs` background task + `<data_path>/_system/cdc_checkpoints.json` JSON sidecar + commit-broadcaster subscription with late-provider discovery + monotonic LSN advancement, with **mutation rows materialized via `MutationEvents::materialize_all()` onto `CommitNotification.mutations`** (post-audit closure); FU-5 `TriggerDeferral::after(payload, delay)` + `TriggerPlugin::on_deferred` default-impl + `DurableDeferralQueue` with `<data_path>/_system/deferred_triggers.json` JSON sidecar + Arrow IPC encoding of `MutationBatch`; FU-6 `TtlSweepJob` real-body integration tests via `RecordingJobHost`). Also shipped: `kind="cron"` variant for `uni.periodic.schedule` (5/6-field cron via the `cron` crate); `_BackgroundJob` system-label scheduler durable persistence (`scheduler_persistence.rs`) with **`Schedule` round-trip across restart via new `SchedulerPersistence::record_scheduled` trait method** (post-audit closure); live WRITE-mode declared-procedure E2E test creating nodes via Cypher body through `SyntheticProcedurePlugin`. **Total test count**: 161 in `uni-db::integration_admin` (incl. CDC e2e with row content, otel traceparent, defer durability, declared procedure WRITE, ttl_sweep body); 21 in `uni-plugin-wasm-rt` (incl. 3 secret-leak); 33 in `uni-plugin-custom`; 115 in `uni-plugin-builtin`; 97 in `uni-plugin`. | no (parallel after M6) |
| M12 | CLI + OCI distribution + Python bindings + conformance suite + perf regression suite | `uni plugin install/...`; OCI artifact support; `Uni.add_plugin` in PyO3 bindings; `uni-plugin-conformance` crate; perf benchmarks | ▶ partial — `run_against_plugin` real 6-probe conformance suite (commit `44944851`, `lib.rs:148-240`). NO CLI (`uni plugin install/...`), NO OCI loader, NO Python bindings, NO perf bench. | no (final integration) |

---

## 3. Workstream Dependency Graph

```
                                          M0 (prework)
                                             │
                                             ▼
                                          M1 (uni-plugin core)
                                             │
                          ┌──────────────────┼──────────────────┐
                          ▼                  ▼                  ▼
                         M2          ┌──── M3 ────┐            M8
                  (scalar UDF        (FoldAggKind  (PyO3 loader —
                   migration         retirement +  doesn't depend
                   via facade)       Locy agg)     on M3 or M4)
                          │                  │
                          └────────┬─────────┘
                                   ▼
                                  M4 (Procedures)
                                   │
                                   ▼
                                  M5 (Storage/Index/Catalog/Algo/CRDT/Hooks/Triggers/Pushdown)
                                   │
                                   ▼
                                  M6a (Extism loader)  ─┐
                                                        │
                                                        ▼
                                                       M6b (CM loader + pools)
                                   │                    │
                          ┌────────┴────────┬───────────┴┬────────┬────────┐
                          ▼                 ▼            ▼        ▼        ▼
                         M7                M9           M10      M11       M12
                       (Rhai —          (Meta)        (Hot reload, (Caps, sec,  (CLI, OCI,
                        in-host                         ABI multi)  obs, sched)  Python,
                        scripting)                                              conformance,
                                                                                perf)
```

Read the DAG as: M0→M1→M2 is the longest chain that can't be shortened. After M1, M2 and M3 can run in parallel (different surfaces). M8 (PyO3) can run as early as after M2 since it only needs the scalar-fn trait infrastructure. M5 (other native surfaces) can fan out into multiple sub-tasks (storage, index, algorithm) that run concurrently. **M6 splits into M6a (Extism, user-facing path) and M6b (Component Model, trusted infrastructure) — M6a ships first because it is lower-risk and unblocks user-authored UDFs; M6b follows in parallel with M9/M10/M11.** M7 (Rhai) is independent of the WASM loaders — pure-Rust, no wasmtime dependency — and can land in parallel after M5 once the scalar / aggregate / procedure trait surfaces stabilize. M9, M10, M11, M12 can begin as soon as either M6a or M6b lands — they only need the registrar surface, which both loaders share.

---

## 4. Milestones in Detail

### M0 — Prework: scoping verification & test inventory

**Status (2026-05-23): ✅ complete.** Base SHA pinned at `aa6446c30c0926d692c2c45f106dd0f550b655ee`; grep inventory captured at `docs/plans/m0-baselines/` (120 `FoldAggKind` references; 40 hardcoded procedure dispatch sites). Baseline perf snapshot deferred until M2's `NativeArrowUdf` is ready for comparison.

**Goal:** Establish a verifiable starting point. Confirm no exploratory assumption is wrong before committing to the implementation.

**Deliverables:**

1. A pinned `main` commit SHA recorded in `docs/plans/plugin_framework_implementation.md` (this doc) as the "base."
2. A baseline performance report: throughput / latency for representative scalar UDFs (`toUpper`, `vector.cosine`), aggregates (`SUM`, `MNOR`), and procedures (`uni.search`, `uni.algo.pageRank`) on the standard `uni-bench` workloads. Recorded as `bench/baselines/pre-plugin-fw.json`.
3. A complete inventory of:
   - All hardcoded matches on function/procedure/aggregate name in `crates/uni-query/` (grep output committed to the plan dir as `pre-refactor-grep.txt`).
   - All places `CustomFunctionRegistry` is instantiated, accessed, mutated.
   - All `SessionHook` implementations in the codebase.
   - All call sites of `parse_fold_aggregate`, `procedure_call::dispatch`, `register_cypher_udfs`, `register_custom_udfs`.
4. A list of every Cypher and Locy TCK scenario currently passing — recorded as `pre-refactor-tck-pass.txt`. We will compare against this after every milestone.

**Acceptance:**

- `pre-refactor-grep.txt`, `pre-refactor-tck-pass.txt`, `bench/baselines/pre-plugin-fw.json` exist in the repo.
- A note in the plan doc identifies the base SHA.

**Risks:** Negligible. This milestone is documentation.

**Parallel work unlocked:** None — M1 starts immediately after.

---

### M1 — Foundation: `uni-plugin` core crate

**Status (2026-05-23): ✅ complete + extended.** `crates/uni-plugin/` ships all 25 surface traits per proposal §4 (scalar / aggregate / window / procedure / locy-aggregate / locy-predicate / operator / optimizer-rule / index / storage / algorithm / pregel / crdt / hook (phased) / trigger / background-job / logical-type / auth / authz / connector / collation / cdc / catalog / replacement-scan / pushdown × 5 marker traits). The crate also exceeds the milestone's original scope with M10/M11 modules: `lifecycle` (state machine + `EpochFencedReload` driver), `secrets` (sealer/unsealer membrane), `verify` (hash-pin + Ed25519 default-on), `scheduler` (with real `tick()` driver primitive), `circuit_breaker` (half-open), `observability` (InvocationKind + record_invocation), `qname`, `errors`, `manifest`, `registry`, `registrar`, `capability`. **80 tests pass.**

**Goal:** Ship the trait, registry, manifest, and capability traits as a standalone crate with no host integration. Nothing changes for users of uni-db yet. The crate compiles; its unit tests pass; the workspace builds; existing tests continue to pass.

**Deliverables:**

1. New crate `crates/uni-plugin/` with:
   - `lib.rs` — re-exports.
   - `plugin.rs` — `Plugin` trait, `PluginManifest`, `PluginId`, `QName`, `PluginHandle`, `PluginInitContext`, `PluginError`.
   - `registrar.rs` — `PluginRegistrar` builder with all method stubs (`scalar_fn`, `aggregate_fn`, `window_fn`, `procedure`, `locy_aggregate`, `locy_predicate`, `operator`, `optimizer_rule`, `index_kind`, `storage_backend`, `algorithm`, `crdt_kind`, `hook`, `logical_type`, `auth_provider`, `authz_policy`, `connector`, `config_param`, `trigger`, `background_job`, `collation`, `cdc_output`, `catalog`, `replacement_scan`, `pregel_program`).
   - `registry.rs` — `PluginRegistry` with per-surface tables (`HashMap<QName, Arc<PluginEntry>>` per surface, wrapped in `arc-swap::ArcSwap`).
   - `capability.rs` — `Capability`, `CapabilitySet`, `Determinism`, `SideEffects`, `Scope`, `AbiRange`.
   - `manifest.rs` — `PluginManifest`, `ProvidedSurfaces`, TOML and JSON (de)serialization (via `serde` + `toml` + `serde_json`).
   - `traits/scalar.rs` — `ScalarPluginFn`, `FnSignature`, `ArgType`, `Volatility`, `NullHandling`, `RowFn` adapter, `NativeArrowUdf` adapter.
   - `traits/aggregate.rs` — `AggregatePluginFn`, `PluginAccumulator`, `AggSignature`.
   - `traits/window.rs` — `WindowPluginFn`, `WindowSignature`, `WindowFrame`.
   - `traits/procedure.rs` — `ProcedurePlugin`, `ProcedureSignature`, `ProcedureMode`, `ProcedureContext`, `RetryContract`, `NamedArgType`.
   - `traits/locy.rs` — `LocyAggregate`, `LocyAggState`, `Semilattice`, `LocyPredicate`, `PredSignature`, `BatchHint`.
   - `traits/operator.rs` — `OperatorProvider`, `OptimizerRuleProvider`, `PlannerArgs`.
   - `traits/index.rs` — `IndexKindProvider`, `IndexHandle`, `IndexKind`, `IndexBuild`.
   - `traits/storage.rs` — `StorageBackend`, `Storage`, `StorageOptions`, `WriteHandle`.
   - `traits/algorithm.rs` — `AlgorithmProvider`, `AlgorithmSignature`, `AlgorithmContext`, `PregelProgramProvider`, `PregelComputeContext`.
   - `traits/crdt.rs` — `CrdtKindProvider`, `CrdtState`, `CrdtKind`, `CrdtOp`.
   - `traits/hook.rs` — `SessionHook` (phased), `HookOutcome`, `ParseContext`, `AnalyzeContext`, `PlanContext`, `ExecuteContext`, `CommitContext`, `AbortContext`.
   - `traits/trigger.rs` — `TriggerPlugin`, `TriggerSubscription`, `TriggerPhase`, `TriggerEventMask`, `MutationBatch`, `TriggerOutcome`, `FireMode`.
   - `traits/background.rs` — `BackgroundJobProvider`, `JobDefinition`, `Schedule`, `ConcurrencyLimit`, `JobContext`, `JobOutcome`.
   - `traits/types.rs` — `LogicalTypeProvider`.
   - `traits/auth.rs` — `AuthProvider`, `AuthzPolicy`, `Principal`, `Credentials`, `Action`, `Resource`, `Decision`.
   - `traits/connector.rs` — `Connector`, `ConnectorConfig`, `ConnectorHandle`.
   - `traits/collation.rs` — `CollationProvider`.
   - `traits/cdc.rs` — `CdcOutputProvider`, `CdcStream`, `CdcBatch`, `CdcLsn`, `CdcStartContext`.
   - `traits/catalog.rs` — `CatalogProvider`, `CatalogTable`, `CatalogLabel`, `CatalogEdgeType`, `ReplacementScanProvider`, `ReplacementRequest`, `Replacement`.
   - `traits/pushdown.rs` — `SupportsFilterPushdown`, `SupportsProjectionPushdown`, `SupportsLimitPushdown`, `SupportsTopNPushdown`, `SupportsAggregatePushdown`, `FilterApplication`, `ProjectionApplication`, `TopNApplication`, `AggregateApplication`.
   - `errors.rs` — `PluginError`, `FnError`, `HookError`, `AuthError`, `AuthzError`.

2. Cargo dependencies: `arrow`, `arrow-ipc`, `datafusion-common`, `tracing`, `semver`, `blake3`, `smol_str`, `arc-swap`, `serde`, `serde_json`, `toml`, `dashmap`. **No** `wasmtime`. **No** `pyo3`.

3. Unit tests in `crates/uni-plugin/tests/`:
   - `manifest_parse.rs` — round-trip TOML ↔ JSON, deny-unknown-fields, hash-pinning extraction, ABI range parsing, signature placeholder.
   - `registrar.rs` — duplicate QName rejected, capability gating denies registration without grant, dependency cycle detection.
   - `qname.rs` — case-insensitive Cypher match, case-sensitive Locy match, namespace parsing edge cases.
   - `semilattice.rs` — `Semilattice` properties for built-in aggregates (constructed as fixtures, not registered yet).
   - `arrow_arg_types.rs` — `ArgType::Primitive(T)` round-trips through `ColumnarValue`; `ArgType::CypherValue` round-trips through `LargeBinary`.
   - `compile_check.rs` — `compile_fail` tests proving trait bounds (Send + Sync + 'static) and lifetimes are correct.

4. `docs/proposals/plugin_framework.md` cross-references updated to point at concrete `crates/uni-plugin/src/...` paths.

**Files changed in this milestone:**

- `crates/uni-plugin/**` (new, ~3000 lines of trait/struct definitions + tests).
- `Cargo.toml` (root) — add `crates/uni-plugin` to `[workspace.members]`.
- `crates/uni-plugin/Cargo.toml` (new).

**Files *not* changed:**

- Anything outside `crates/uni-plugin/`. The host integration starts in M2.

**Acceptance criteria:**

1. `cargo nextest run -p uni-plugin` passes ≥ 60 tests across the modules listed above.
2. `cargo build --workspace` succeeds.
3. `cargo nextest run --workspace` continues to pass (no regression).
4. `cargo clippy -p uni-plugin -- -D warnings` is clean.
5. `cargo doc -p uni-plugin --no-deps` builds without warnings.
6. Every public trait, struct, and method has rustdoc with at least one `# Example` block.

**Risks:**

- *Risk:* The trait design has subtle lifetime issues that only surface when integration begins in M2. *Mitigation:* The `compile_check.rs` tests pin lifetime and bound expectations explicitly; the `RowFn` and `NativeArrowUdf` adapters force real Arrow trait bounds at the type-checker level.
- *Risk:* The 25 surfaces have so much surface area that the crate becomes unwieldy. *Mitigation:* The `traits/` submodule structure (one file per surface) keeps each file under 300 lines.

**Parallel work unlocked:** M2 (scalar UDF migration) — proceeds as soon as M1 lands.

---

### M2 — Scalar UDF migration: `CustomFunctionRegistry` facade + `NativeArrowUdf`

**Status (2026-05-23): ✅ substantive.** `CustomFunctionRegistry` is now a facade over a shadow `Arc<PluginRegistry>`; every `register()` mirrors into the plugin registry; `plugin_registry()` accessor exposes the shadow. `ValueRowFn` adapter bridges legacy `CustomScalarFn` closures (Fn(&[Value]) → Result<Value>) to `ScalarPluginFn`. `register_plugin_scalar_udfs(&ctx, &registry)` registers each scalar under lowercase / uppercase / qualified-name forms. `PluginScalarUdf` derives the DataFusion return type from the plugin's `ArgType::Primitive(T)` — the **≥ 20% perf-win path**: primitively-typed plugins declare `T` directly to DataFusion, skipping the `LargeBinary` round-trip. Wired into `crates/uni-query/src/query/executor/read.rs:319`. **All 558 pre-existing uni-query tests + 1 new fast-path test pass.** Cutover (deleting legacy match-arm dispatch in `df_expr.rs:2130`) pending.

**Goal:** Every Cypher scalar UDF — built-in and custom — flows through `PluginRegistry`. The `LargeBinary` round-trip is demoted from default to `ArgType::CypherValue` opt-in. Primitively-typed UDFs get the native Arrow fast path. Performance: ≥ 20% improvement for primitive-typed `CustomFunctionRegistry` entries vs the M0 baseline.

**Deliverables:**

1. New crate `crates/uni-plugin-builtin/` with initial scaffolding:
   - `lib.rs` — `BuiltinPlugin` implementing `Plugin`.
   - `scalar_fns/` — module per category (`string.rs`, `math.rs`, `time.rs`, `vector.rs`, `list.rs`, `bitwise.rs`, `temporal.rs`).
   - Cargo dep on `uni-plugin`.

2. Refactored `crates/uni-query/src/query/executor/custom_functions.rs`:
   - `CustomFunctionRegistry` retained as a **facade**: its `register(name, fn)` method now calls `PluginRegistry::scalar_fn` internally with a `RowFn` wrapper. Backward-compatible.
   - The flat `HashMap<String, Arc<Fn>>` is replaced by an `Arc<PluginRegistry>` field.
   - Public API unchanged.

3. Refactored `crates/uni-query/src/query/df_udfs.rs`:
   - `register_cypher_udfs()` (line ~79) iterates `BuiltinPlugin`'s registrations rather than its hardcoded list. The hardcoded list is moved into `uni-plugin-builtin/src/scalar_fns/*` as `Plugin::register` calls.
   - `register_custom_udfs()` (line ~243) iterates `PluginRegistry::scalar_fns()` directly.
   - `CustomScalarUdf` (line ~311) is *kept* but its use is gated to `ArgType::CypherValue` plugins. A new `NativeArrowUdf` handles `ArgType::Primitive(T)` plugins without the `LargeBinary` round-trip.

4. Refactored `crates/uni-query/src/query/df_expr.rs`:
   - `translate_function_call()` (line ~2130) consults `PluginRegistry` first; the built-in match is *deleted* (its entries are now `BuiltinPlugin` registrations).
   - The `dummy_udf_expr` fallback fires only when no plugin claims the qname (genuine "unknown function" path).

5. Refactored `crates/uni-query/src/query/expr_eval.rs`:
   - The `custom_fns` field (line ~1860) becomes `Arc<PluginRegistry>`.
   - Direct expression-evaluation lookup uses the registry.

6. New helper: `crates/uni-plugin/src/df_adapter.rs` — `NativeArrowUdf` struct implementing DataFusion's `ScalarUDFImpl::invoke_with_args` directly from `ScalarPluginFn::invoke`.

7. Tests added:
   - `crates/uni-plugin/tests/native_arrow_udf.rs` — verify `NativeArrowUdf` produces identical results to the legacy `CustomScalarUdf` for primitive-typed fns; verify it's >= 20% faster on a 1M-row workload.
   - `crates/uni-query/tests/scalar_udf_via_plugin.rs` — end-to-end Cypher query calling `toUpper`, `vector.cosine`, custom registered fn.
   - `crates/uni-plugin-builtin/tests/all_scalar_fns.rs` — every built-in scalar fn is reachable through the registry and produces the same result as before.

8. Migration cleanup: pre-existing built-in scalar UDF call sites are unchanged (the user-facing Cypher API is the same). Internal call paths now flow through the plugin layer.

**Files changed:**

- `crates/uni-plugin-builtin/**` (new, ~1500 lines: a registration per built-in scalar fn).
- `crates/uni-query/src/query/executor/custom_functions.rs:24` (rewrite).
- `crates/uni-query/src/query/df_udfs.rs:79,243,311` (refactor).
- `crates/uni-query/src/query/df_expr.rs:2130` (rewrite the dispatch).
- `crates/uni-query/src/query/expr_eval.rs:1860` (use registry).
- `crates/uni-plugin/src/df_adapter.rs` (new).
- `crates/uni/src/lib.rs` — `Uni::new` constructs `Arc::new(BuiltinPlugin::new())` and registers it.
- `Cargo.toml` (root) — add `crates/uni-plugin-builtin` to workspace.

**Acceptance criteria:**

1. `grep -rn 'match name.*\"toUpper\"' crates/uni-query/src/` returns zero hits. (Built-in dispatch via match is gone.)
2. `cargo nextest run -p uni-tck` — all Cypher TCK scenarios pass that passed in M0.
3. `cargo bench -p uni-bench --bench scalar_udf` — primitive-typed UDFs are ≥ 20% faster than the M0 baseline (the win from skipping `LargeBinary`).
4. `cargo nextest run --workspace` — full suite green.
5. The `crates/uni/tests/hooks_test.rs` integration tests continue to pass (we haven't touched `SessionHook` yet).
6. `crates/uni-plugin-builtin/src/scalar_fns/` contains a registration for every scalar built-in named in `pre-refactor-grep.txt`.

**Risks:**

- *Risk:* Subtle behavioral difference between the legacy `CustomScalarUdf` `LargeBinary` path and the new `NativeArrowUdf` path for edge cases (NULL, NaN, empty arrays). *Mitigation:* A property-based test (`proptest`) comparing both paths on randomized input; runs in CI.
- *Risk:* DataFusion's `ScalarUDFImpl::invoke_with_args` signature changes across versions. *Mitigation:* Pin DataFusion version in `Cargo.toml` for this milestone; upgrade as a separate, isolated PR.
- *Risk:* The performance regression on `ArgType::CypherValue` paths (still going through `LargeBinary`) is acceptable but should be measured to confirm no surprise slowdown.

**Parallel work unlocked:** M3 (FoldAggKind retirement), M8 (PyO3 loader — needs scalar-fn trait).

---

### M3 — Closed-enum elimination: `FoldAggKind` retirement + Locy aggregate refactor

**Status (2026-05-23): ✅ substantive.** 9 `LocyAggregate` impls shipped in `crates/uni-plugin-builtin/src/locy_aggregates.rs` (`MIN` / `MAX` / `SUM` / `COUNT` / `AVG` / `COLLECT` / `MNOR` / `MPROD` + `CountAll`) — each with explicit `Semilattice` metadata. Trait-dispatch runtime cutover done (commit `fae27497`). **`FoldAggKind` still has 126 references in 6 files**: `locy_fold.rs` (91), `locy_program.rs` (12), `locy_fixpoint.rs` (10), `df_planner.rs` (9), plus `procedure_call.rs`, `traits/locy.rs`, `locy_aggregates.rs`. The enum now uses associated constructors (`FoldAggKind::count_all()`, `::sum()`) rather than being match'd as a closed enum, so it's effectively a tag. Full type deletion pending — tracked in `REMAINING_WORK.md`.

**Goal:** The single most invasive refactor in the proposal. Delete `FoldAggKind` from the codebase. All Locy aggregates become `Arc<dyn LocyAggregate>` registrations. The fixpoint engine's monotonicity proofs survive via `Semilattice` metadata. All Locy TCK scenarios pass unchanged.

**Note:** The longest milestone; do not parallelize within (single engineer owns the cutover).

**Deliverables:**

1. New file `crates/uni-plugin-builtin/src/locy_aggregates.rs` containing `LocyAggregate` implementations for:
   - `MinLocyAgg` (idem, comm, assoc, monotone, has_top = min of domain)
   - `MaxLocyAgg` (idem, comm, assoc, monotone, has_top = max of domain)
   - `SumLocyAgg` (comm, assoc; not idem, not monotone; rejected in recursive clauses unless `Semilattice::None` declared)
   - `MsumLocyAgg` (alias for Sum — `r.locy_aggregate(QName::builtin("MSUM"), Arc::new(SumLocyAgg))`)
   - `CountLocyAgg` (comm, assoc, monotone, has_top = ∞)
   - `AvgLocyAgg` (comm, assoc; non-monotone; rejected in recursive clauses)
   - `CollectLocyAgg` (comm, assoc, monotone, multiset semilattice)
   - `MnorLocyAgg` (idem, comm, assoc, monotone, has_top = 1.0; current `MonotonicAggState::Mnor` logic)
   - `MprodLocyAgg` (idem, comm, assoc, monotone, has_top = 0.0; current `MonotonicAggState::Mprod` logic)

2. Refactored `crates/uni-query/src/query/df_graph/locy_program.rs`:
   - `parse_fold_aggregate(name, registry)` (line ~1222) is rewritten to look up via `PluginRegistry::locy_aggregate(&qname)`. Returns `Arc<dyn LocyAggregate>` instead of `FoldAggKind`.
   - `convert_fold_bindings()` (line ~1183) takes the new trait object signature.
   - The `FoldAggKind` enum and its `match` consumers are *deleted*.

3. Refactored `crates/uni-query/src/query/df_graph/locy_fixpoint.rs`:
   - `MonotonicAggState` is parameterized over `Arc<dyn LocyAggregate>`. Its current `match`-on-`FoldAggKind` is replaced by trait dispatch.
   - The strict-mode alignment logic (Phase 1/2 hardening) reads `Semilattice::has_top` instead of matching `FoldAggKind::Mnor | FoldAggKind::Mprod`.
   - `apply_post_fixpoint_chain` and `apply_having_filter` use the trait.

4. The monotonicity-rejection check is added to the Locy compiler. A `LocyAggregate` with `semilattice.monotone_join == false` used inside a recursive Locy clause produces a compile-time error: `"non-monotone aggregate '$name' cannot be used in recursive clause; use HAVING / post-fixpoint context"`. Tests: `compile_error_non_monotone.rs`.

5. The Phase 3 `DerivationTracker` integration (memory: shared-proof detection) survives unchanged: the tracker is created when *any* rule uses an aggregate with `semilattice.has_top` (generalization of "any rule uses MNOR/MPROD"). The two-tier detection and `provenance_join_cols` plumbing are unchanged.

6. Tests added:
   - `crates/uni-locy-tck/scenarios/` — every existing Locy aggregate scenario re-runs and passes. Specifically: `MonotonicAggregation.feature` (6 scenarios), `ProbabilisticComplement.feature` (1 scenario), `SharedProofDetection.feature` (3 scenarios), plus the standard `MIN`/`MAX`/`SUM`/`COUNT`/`AVG`/`COLLECT` scenarios — *no test changes*.
   - `crates/uni-locy-tck/scenarios/PluginAggregateUserDefined.feature` (new) — a `BuiltinPlugin`-registered user aggregate participates in fixpoint correctly.
   - `crates/uni-locy-tck/scenarios/PluginAggregateNonMonotone.feature` (new) — a non-monotone aggregate is rejected in recursive context, accepted in HAVING.

7. `docs/proposals/plugin_framework.md` §7 (Locy refactor) is annotated with file paths and PR numbers as cross-references after the milestone merges.

**Files changed:**

- `crates/uni-query/src/query/df_graph/locy_program.rs:1183,1222` (rewrite `parse_fold_aggregate` and `convert_fold_bindings`).
- `crates/uni-query/src/query/df_graph/locy_fixpoint.rs` (rewrite `MonotonicAggState`, `apply_post_fixpoint_chain`, `apply_having_filter`).
- `crates/uni-plugin-builtin/src/locy_aggregates.rs` (new, ~500 lines).
- `crates/uni-plugin-builtin/src/lib.rs` (register the 9 aggregates).
- Wherever `FoldAggKind` is referenced (a complete grep in M0 produces the list — historically ~15–20 sites).
- New TCK scenarios in `crates/uni-locy-tck/scenarios/`.

**Acceptance criteria:**

1. `grep -rn 'enum FoldAggKind' crates/` returns zero hits.
2. `grep -rn 'FoldAggKind::' crates/` returns zero hits.
3. `cargo nextest run -p uni-locy-tck` — all pre-existing scenarios pass plus the two new ones.
4. `cargo nextest run -p uni-tck` continues to pass.
5. `cargo bench -p uni-bench --bench locy_aggregates` — no regression > 5% vs the M0 baseline for MIN, MAX, SUM, MNOR.
6. The Phase 3 `SharedProofDetection.feature` scenarios continue to pass (provenance tracking survives the refactor).

**Risks:**

- *Risk:* Subtle semantic difference in how `MonotonicAggState` was tracking provenance vs. the new trait-object dispatch. *Mitigation:* The `SharedProofDetection.feature` is the canary. If it fails, the refactor isn't done.
- *Risk:* The `Semilattice` metadata for `AVG` is genuinely awkward (it's non-monotone but is a frequent built-in). *Mitigation:* The acceptance criterion lets `AVG` be rejected in recursive context — this matches existing behavior; AVG-in-recursion was never sound.
- *Risk:* The `MonotonicAggState::Mnor`/`Mprod` strict-mode alignment (memory: Phase 1/2 hardening) is fragile. *Mitigation:* The strict-mode tests in `MonotonicAggregation.feature` must pass unchanged.

**Parallel work unlocked:** M4 (Procedure migration) starts as soon as M3 cutover lands. M5 (other native surfaces) can begin its planning in parallel to M3 (no shared files).

---

### M4 — Procedure migration: `procedure_call.rs:559` retirement

**Status (2026-05-23): ▶ partial.** Two destination crates established with the APOC split:
- `crates/uni-plugin-builtin/src/procedures/` — closed-enum replacements (`uni.admin.*`, `uni.schema.*`, `uni.vector.*`, `uni.fts.*`, `uni.temporal.*`, `uni.algo.*` adapters, `uni.system.*`).
- `crates/uni-plugin-apoc-core/src/procedures/` — APOC analogues (perf-critical + host-intimate Rust ports).

**39 procedures registered through the framework**:
- `uni-plugin-builtin`: 1 procedure (`uni.system.echo`, proof of pattern).
- `uni-plugin-apoc-core`: 38 procedures — bitwise (6), text (13), math (10), number (3), convert (4), create (2).

Each procedure is a `ProcedurePlugin` with full `ProcedureSignature`. Both `procedure_call.rs::execute_procedure` (DataFusion path) and `executor/procedure.rs::Executor::execute_procedure` (simple path) consult `ProcedureRegistry::resolve_user_procedure` first. Hardcoded dispatch in `procedure_call.rs:597-625` still routes the rest: `uni.schema.*` (5 arms), `uni.vector.query`, `uni.fts.query`, `uni.search`, `uni.algo.*`, plus a pre-dispatch arm at line 264. **Remaining to port**: `uni.schema.*` (5+), `uni.vector.*`, `uni.fts.*`, `uni.search`, `uni.algo.*` (32 via `AlgorithmProvider` adapter), plus `apoc.coll/refactor/atomic/schema/meta` etc.

**Goal:** The 50+ built-in procedures in uni-db (uni.admin.*, uni.schema.*, uni.vector.*, uni.fts.*, uni.bitwise.*, uni.temporal.*, uni.algo.*) become `ProcedurePlugin` registrations. The hardcoded dispatch match at `procedure_call.rs:559` is deleted. The stub `ProcedureRegistry` at `executor/procedure.rs:75` becomes real.

**Deliverables:**

1. New file `crates/uni-plugin-builtin/src/procedures/` with one module per namespace:
   - `admin.rs` — 5 procedures (compact, compactionStatus, snapshot.*)
   - `schema.rs` — 13 procedures (createLabel, dropLabel, …, indexes, constraints, labelInfo)
   - `vector.rs` — `uni.vector.query`
   - `fts.rs` — `uni.fts.query`
   - `search.rs` — `uni.search`
   - `bitwise.rs` — 6 procedures
   - `temporal.rs` — 6 procedures
   - `algo.rs` — 32 procedures (forwarded to `AlgorithmProvider` registrations once M5 ships; thin wrapper for now)

2. Refactored `crates/uni-query/src/query/executor/procedure.rs:75`:
   - `ProcedureRegistry` becomes a real type. Its current test-only mock data is removed. Backed by `Arc<PluginRegistry>`.
   - `GraphExecutionContext::with_procedure_registry()` continues to work (it accepts the new real registry).

3. Refactored `crates/uni-query/src/query/df_graph/procedure_call.rs:559`:
   - The hardcoded dispatch match is *deleted*.
   - New dispatch: parse `CALL uni.foo.bar(...)` into a `QName`; look up via `PluginRegistry::procedure(&qname)`; verify the calling principal has the procedure's required capability (Procedure / ProcedureWrites / ProcedureSchema / ProcedureDbms); construct a `ProcedureContext`; invoke; attach the returned `SendableRecordBatchStream` to the surrounding query plan.

4. Tests:
   - `crates/uni-query/tests/procedure_dispatch.rs` (new) — every formerly-hardcoded procedure is reachable via the registry and returns the same result as the legacy path. Concretely: a test per procedure namespace verifying one representative call.
   - The existing `crates/uni-query/tests/` procedure tests continue to pass unchanged.
   - `crates/uni-tck/scenarios/PluginProcedureE2E.feature` (new) — Cypher `CALL` end-to-end.

5. The hardcoded dispatch for `uni.algo.*` is *not* deleted in M4; algorithms still resolve through the existing `AlgorithmRegistry` at `crates/uni-algo/src/algo/mod.rs:55`. A thin `AlgorithmProcedure` adapter wraps each `AlgorithmRegistry` entry as a `ProcedurePlugin` for the M4 cutover. M5 will migrate the algorithm registry itself.

**Files changed:**

- `crates/uni-query/src/query/df_graph/procedure_call.rs:559` (delete the match; rewrite dispatch).
- `crates/uni-query/src/query/executor/procedure.rs:75` (real implementation).
- `crates/uni-plugin-builtin/src/procedures/**` (new, ~2500 lines — registrations for every built-in procedure).
- `crates/uni-plugin-builtin/src/lib.rs` (register the new procedures).
- New tests.

**Acceptance criteria:**

1. `grep -rn 'match name.*\"uni.admin' crates/uni-query/src/` returns zero hits.
2. `grep -rn '"uni.admin.compact"' crates/uni-query/src/` returns zero hits.
3. `cargo nextest run -p uni-tck` (Cypher TCK) — every scenario that uses `CALL uni.X` passes.
4. `cargo nextest run --workspace` — full suite green.
5. The new `crates/uni-query/tests/procedure_dispatch.rs` runs > 50 sub-tests (one per built-in procedure) all green.
6. Capability gating verified: a session without `Capability::ProcedureWrites` cannot call a `Write`-mode procedure; explicit test in `crates/uni-plugin/tests/capability_gating.rs`.

**Risks:**

- *Risk:* Some built-in procedures use internal-only APIs not yet exposed via `ProcedureContext`. *Mitigation:* `ProcedureContext` grows surface as needed during the milestone; the trait is internal until M6 stabilizes the ABI.
- *Risk:* Performance regression on `CALL uni.algo.pageRank` due to the extra adapter layer. *Mitigation:* Measure; the adapter is a single virtual call, should be unmeasurable.

**Parallel work unlocked:** M5 (other native surfaces) — can fan out into multiple sub-tasks now.

---

### M5 — Remaining native surfaces: storage / index / catalog / algorithm / CRDT / hooks / triggers / pushdown / logical types / connector / auth / authz / collation / CDC / background-job-trait / Pregel

**Status (2026-05-25): ✅ complete (closure commit `38db9b3eb`).** All 9 sub-tasks landed across Batches 1–3 + the 2026-05-24/25 follow-up bundle + the final closure commit. The "Still owned by M5" list is fully discharged; the items genuinely blocked on later milestone primitives remain routed there (Auth/Authz/Connector → M6a; `CrdtKindProvider` host consultation → M10; CDC runtime + `TriggerOutcome::Defer` queue → M11; `ORDER BY ... COLLATE` grammar → M12).

**Routed out of M5 (now owned by the listed milestone — see that milestone's deliverable list for the full spec):**
- *Auth host consultation at `Session::open` / `Uni::authenticate`* → **M6a** (only meaningful once external WASM plugins can ship custom auth providers).
- *Authz host consultation at planner pre-execution* → **M6a**.
- *Connector host wiring of `Connector::start`* → **M6a**.
- *`CrdtKindProvider` host consultation from `uni-crdt` mutation paths* → **M10** (depends on the per-kind reload discipline + schema-compat check that M10 lands; otherwise a hot-swap tears in-flight merge state).
- *CDC runtime — `CdcOutputProvider` driver loop + `uni_system.cdc_checkpoints` persistence* → **M11** (depends on the tokio scheduler driver M11 lands).
- *`TriggerOutcome::Defer` queue* → **M11** (depends on the same scheduler primitive).
- *`ORDER BY ... COLLATE <name>` Cypher grammar + sort comparator + indexed-string-lookup normalize wiring* → **M12** (depends on a Cypher grammar extension).

**All M5-owned closure items shipped (commit `38db9b3eb`):**
- **M5b.1** ✅ — multi-label `MATCH (n:Virtual:Native)` intersection. New `classify_labels(labels, registry) -> (Vec<String>, Vec<String>)` helper in `crates/uni-query/src/query/df_planner.rs` splits the label list once and is shared by `plan_scan` and `plan_multi_label_scan`. When both sides are non-empty, `build_virtual_union_scan` produces a `CatalogVertexScanExec` (Union'd over multiple virtuals) and `semi_join_on_vid` wraps it with `LeftSemi` against the native `GraphScanExec::new_multi_label_vertex_scan` keyed on `_vid` — `LeftSemi` over `Inner` to avoid duplicate `_vid` columns. Tests in `crates/uni/tests/plugin_virtual_label_dispatch.rs`.
- **M5b.2** ✅ — runtime write-path rejection for SET/REMOVE/DELETE against virtual labels and virtual edge types. `reject_virtual_label_write` / `reject_virtual_edge_type_write` helpers in `crates/uni-query/src/query/executor/write.rs` fire at `execute_set_items_locked` (SetItem::Labels arm), `execute_remove_labels`, `execute_delete_vertex`, `execute_delete_edge_from_map`, and the Value::Edge delete arm — every path that would otherwise let a label-add or delete reach the writer schemalessly. 4 integration tests in `crates/uni/tests/plugin_virtual_label_dispatch.rs` (SET, DELETE, REMOVE, regression-guard native-label).
- **M5b.3** ✅ — native↔virtual joins mid-pattern in `plan_traverse`. New `hydrate_virtual_target_from_catalog` wraps a traverse plan with an inner-join against `CatalogVertexScanExec` on `{target}._vid` when the target label is virtual. New `plan_traverse_virtual_edge` replaces `GraphTraverseExec` with a `HashJoin(input × CatalogEdgeScanExec)` keyed on `_src_vid`/`_dst_vid` for the all-virtual single-hop case. Logical planner (`planner.rs`) now resolves virtual edge-type and target-label names through the registry via `allocate_virtual_edge_type` / `allocate_virtual_label` so the physical planner sees real virtual ids instead of 0 / `unknown_types`. Tests in `crates/uni/tests/plugin_mid_pattern_virtual.rs` (3 scenarios: `(Native)-[r:VirtualRel]->(External)`, chained virtual surfaces, shortestPath regression). Documented limitations: single-hop all-virtual only; mixed native+virtual edge-OR lists fall through to legacy `GraphTraverseExec`.
- **M5c.5** ✅ — legacy `(['L'], ['E'], ...)` algorithm call shape removed. The `args[0]` JSON-Array discriminator branch is gone from `procedures_plugin/algo.rs` (only the `is_object()` V2 path remains, plus the positional-fallback for `shortestPath` and similar). `emit_legacy_deprecation_warning` deleted. The 30+ in-tree callers (`algo_integration.rs`, `algo_l0_visibility_test.rs`, `algo_edge_cases.rs`, `algo_benchmarks.rs`, `algorithm_graph_ref_native.rs`, `m4_host_procedures_dispatch.rs`) migrated to V2 `(graphRef Map, config Map)`. `grep -rn 'CALL uni\.algo\.\w\+(\[' crates/ tests/ examples/` returns 0. `ProcedureRegistry::resolve_user_procedure` is already name-based (no arity dispatch to collapse). `projection_input.rs:7` and `algo.rs:38` doc comments rewritten.
- **M5f.1** ✅ — node-property refs in trigger `predicate_source`. `event_row_schema` gained `properties_new` and `properties_old` `LargeBinary` columns carrying `cypher_value_codec::encode(&Value::Map(...))` blobs (chose the codec-encoded shape over a true Arrow `Map` because the existing `index(container, key)` UDF already handles map lookup on `LargeBinary`). `compile_predicate` rewrites `n.foo` / `old.foo` AST nodes via new `rewrite_property_refs` to read from those bags, registers Cypher UDFs on the predicate `SessionContext`, and resolves `DummyUdf` placeholders to real impls via new `resolve_dummy_udfs` (both gaps fixed as part of this work — predicates with `index()` were silently failing at first row before). `RouteEntry.properties_referenced: HashSet<String>` makes `MutationEvents::from_l0_with_probe` populate only subscribed property keys (predicate-gated cost). `MutationRow.new_properties` / `old_properties` carry the per-row maps. 3 tests in `crates/uni/tests/trigger_predicate_properties.rs` (`predicate_filters_on_new_property`, `predicate_filters_on_value_change`, `predicate_on_edge_property`).
- **M5f.2, M5f.3** ✅ — `PreExistingProbe::extend_with_l1` now projects every property column for each candidate label (via `backend.get_table_schema` + `arrow_to_value` per row) and hydrates the resulting `Properties` map into `self.vertices`, so L1-only vertices carry the same pre-image fidelity as L0-chain vertices. Edge pre-images captured via `PreExistingProbe.edges: HashMap<Eid, Properties>` populated from `L0Buffer::edge_properties` in `from_l0_chain`; new `edge_old_bytes` helper feeds `old_value` for `EDGE_UPDATE` / `EDGE_DELETE` rows. Tested by `update_to_flushed_vertex_emits_node_update` (asserts post-flush `old_value` decodes to the pre-tx `x: 1` value).
- **M5g** ✅ — typed `Node` / `Edge` yields in `build_typed_column`. `VNodeProcedure` tags its YIELD signature with `_yield_kind = node_vid_source` so the planner's existing node-shape expansion path (`expand_node_yield_fields` at `procedure_call.rs:115-141`, the same path `uni.vector.query YIELD node` already uses) drives a flat `_vid + _labels + per-property` column tuple — user-visible as one typed Node column. `VEdgeProcedure` emits a single `edge` column shaped as `Struct(_eid, _type_name, _src, _dst, properties)` using the canonical `edge_struct_fields` (`common.rs:228-236`). `build_typed_column` gains `DataType::UInt64` and `DataType::Struct(_)` arms; `uni.create.v{Node,Edge}` added to `NODE_YIELD_PROCEDURE_NAMES` and `is_df_eligible_procedure`. Round-trip test `vnode_typed_yield_round_trips_through_cypher_expression` covers `CALL uni.create.vNode(['Ghost'], {answer: 42}) YIELD node RETURN node.answer`. Choice rationale: `DataType::Extension` does not exist on this Arrow/DataFusion stack; native `MATCH (n) RETURN n` also produces the flat tuple, not a single typed column — the "one typed Node column" is a user-visible YIELD abstraction implemented over the existing planner expansion.
- **M5h** ✅ — `StorageTableProvider` bridge. New module `crates/uni-plugin-builtin/src/storage_table_provider.rs` exposes `Arc<dyn Storage>` through DataFusion's `TableProvider` with a chunked `StorageScanExec` (`ExecutionPlan`); `supports_filters_pushdown` delegates to the existing Lance-style predicate classifier and registration wraps via `PushdownAwareTable::with_filter(provider, StorageFilterPushdown)` so `PushdownNegotiationRule` elides the `FilterExec` when the predicate is encodable. `MemoryStorage::read_batch` now honors filters via `datafusion::physical_expr::create_physical_expr` + `filter_record_batch` (unencodable predicates surface `FnError 0x711` per contract); `pushdown_negotiation.rs::try_rewrite_filter` embeds the elided predicate into `TableScan::filters` so the source actually receives it. 3 acceptance tests in `crates/uni-query/tests/storage_table_provider.rs` (rows-returned, encodable-elision verified by `EXPLAIN`, inexpressible-negative-guard).

Sub-status by sub-task:
- **M5a (Storage)** ✅ landed. Lance backend wrapped as `LancePluginStorage` (`crates/uni-plugin-builtin/src/storage.rs`) with predicate pushdown via `expr_to_lance_filter` (returns `FnError 0x711` for unencodable shapes so callers fall back to a DataFusion `Filter`), always-true literal-delete fast-path → `replace_table_atomic` truncation, `fork(table, src, dst)` over Lance branches (`uni-plugin` 1.5.0), and monotonic `WriteHandle::id` from `LanceDbBackend::get_table_version`. `MemoryBackend` ships alongside as the `memory://` scheme.
- **M5b (Index)** ✅ landed. Real `MemoryVectorIndex` (387 lines, `index_vector.rs`) with exact-KNN + persist round-trip; `IndexKindProvider` trait in `uni-plugin`. `IndexProbeExec` bridge (commit `2bd80d00f`, `uni-plugin` 1.7.1) lands end-to-end planner dispatch through plugin-registered `IndexHandle`s via a `VectorSource::{Native, Plugin{...}}` enum inside `GraphVectorKnnExec`. Acceptance: `crates/uni/tests/plugin_index_handle_dispatch.rs`.
- **M5c (Algorithms)** ✅ landed. M5c.1–M5c.5 all shipped (M5c.5 closed in commit `38db9b3eb`). M5c.1 ships the `AlgorithmProvider` trait (`crates/uni-plugin/src/traits/algorithm.rs:87`) + dynamic registration of all 36 algorithms via a single loop in `uni-plugin-builtin/src/algorithms/mod.rs` that iterates `uni_algo::algo::AlgorithmRegistry::new()` and adapts each entry through `AlgoProviderBridge` — the count of 36 is sourced from `uni-algo`, not 36 hand-written registration sites (earlier drafts of this doc implied per-category files; the actual layout is `mod.rs` + `bridge.rs` only). M5c.2–M5c.4 (commit `60f1a6158`, `uni-plugin` 1.7.1 → 1.8.0) ship the V2 `(graphRef, config)` signature, Cypher projections via `QueryProcedureHost::execute_inner_query` + `GraphProjection::from_rows`, and named projections via per-`StorageManager` `ProjectionStore` + `uni.graph.{project, drop, list, exists}` procedures. **M5c.5 is owed in M5** — delete the legacy 5-arg algorithm shim from every algo built-in; collapse `ProcedureRegistry::resolve_user_procedure` arity-keyed dispatch to single-arity-per-name; audit in-tree call sites (`grep -rn 'CALL uni.algo.*(.*\[.*\],' crates/ tests/ examples/` → 0 hits) and migrate any callers found. Per §1.2, the V2 signature has already shipped alongside V1 with a one-shot `DeprecationWarning`, so callers have the runtime signal needed to migrate before the cutover; the migration happens *within* M5, not at a future release boundary (there are no release boundaries).
- **M5d (CRDTs)** ✅ landed (M5 ownership complete). **4 CRDTs** (LWW, OR-Set, G-Counter, MV-Register) all real in `crdts.rs`; plus String-bound `RgaProvider` (kind `"rga"`) and generic `TypedRgaProvider<T: RgaElement>` for arbitrary element types — built-in `RgaElement` impls for `i64` (kind `"rga.int64"`) and `f64` (kind `"rga.float64"`) shipped 2026-05-25 (commit `03030a7cd`). Users add more element types by impl-ing `RgaElement` on their own types; no `uni-plugin` ABI bump. 3 new tests (typed insert/delete/value, persist round-trip, distinct kind ids). **Host consultation of `CrdtKindProvider` from `uni-crdt` mutation paths is owned by M10**, not M5: it depends on the per-kind reload discipline + schema-compat check that M10 lands (without that, a hot-swap of a CRDT plugin would tear in-flight merge state). See M10 §4 deliverable #2 for the spec.
- **M5e (Hooks)** ✅ landed. Phased `SessionHook` trait at `crates/uni-plugin/src/traits/hook.rs:61-96` (`on_parse`, `on_analyze`, `on_plan`, `on_execute_start`, `on_execute_end`, `before_commit`, `after_commit`, `on_abort`). `LegacyHookAdapter` (`crates/uni/src/api/hooks.rs:127-290`) bridges the legacy 4-method shape onto the phased trait. **`BuiltinHookPlugin` + `Uni::add_plugin` sugar shipped 2026-05-25** (commit `68b4fa0e6`): `BuiltinHookPlugin` at `crates/uni/src/api/hooks.rs:314-353` wraps a legacy `Arc<dyn SessionHook>` in a `Plugin` whose `register()` calls `r.hook(adapter)`; `Uni::add_plugin<P: Plugin>` at `crates/uni/src/api/mod.rs:1210` runs the standard registrar dance. Commit-phase registry dispatch added in `Transaction::commit` so `before_commit` / `after_commit` fire through registry hooks alongside the legacy `self.hooks` chain (query phases were already wired in M5b at `Session::run_before_query_hooks` / `run_after_query_hooks`). 3 new integration tests in `crates/uni/tests/hooks_add_hook_sugar.rs`; **12 bridge tests** in `crates/uni/tests/hooks_bridge_test.rs` (grew from the originally-stated 8 as the bridge surface settled). Deprecated `Session::add_hook` retained as a back-compat shim; dual-iteration in `run_*_query_hooks` ensures both paths fire so existing call sites continue to work.
- **M5f (Triggers)** ✅ landed. Core dispatch shipped 2026-05-24 (commit `d2aca7e27`); the M5 closure bundle added predicate compile + CREATE/UPDATE/DELETE discrimination + pre-image capture + node-property refs in predicates (M5f.1 closed in commit `38db9b3eb`). Reference `LabelAuditTrigger` (`crates/uni-plugin-builtin/src/triggers.rs:45-113`) + host-side dispatch in `crates/uni/src/api/triggers.rs`: `TriggerRouter::from_registry` builds a per-phase routing table from `PluginRegistry::triggers()`; `MutationEvents::from_l0_with_probe` drains the tx-private L0 into the §4.18 RecordBatch shape (`event_kind | vid_or_eid | label | property | old_value | new_value`); `Transaction::commit` calls `dispatch_before` (after legacy `before_commit` hooks, before the writer lock — `Synchronous` reject aborts via `UniError::TriggerRejected`) and `dispatch_after` (after legacy `after_commit` hooks — `Synchronous` inline with `catch_unwind`, `Async` / `EventualConsistency` spawned on the tokio runtime). **`predicate_source` Cypher expression compile** ✅ — `compile_predicate` (`triggers.rs:105-148`) parses with `uni_cypher::parse_expression`, lowers via `cypher_expr_to_df`, runs DataFusion `TypeCoercion`, and caches a `PhysicalExpr` on each `RouteEntry`; predicates are evaluated per-row against the event batch in `filter_for`. Predicates referencing the 6 event-row columns (event_kind, vid_or_eid, label, property, old_value, new_value) are fully supported. **CREATE / UPDATE / DELETE discrimination** ✅ — via `PreExistingProbe` (`triggers.rs:562-756`): `from_l0_chain` scans current L0 + pending-flush L0s sync; `extend_with_l1` issues chunked `_vid IN (…)` scans against L1 storage to detect vids that drained out of L0 in earlier flushes. The probe is passed to `MutationEvents::from_l0_with_probe` which emits `NODE_CREATE` / `NODE_UPDATE` / `NODE_DELETE` distinctly. **`old_value` pre-image capture** ✅ — populated from the L0-chain `vertex_properties` / `edge_properties` maps; the L1 probe (M5f.2) now projects every property column on the candidate label and converts via `uni_store::storage::arrow_convert::arrow_to_value`, so L1-only vertices carry the same fidelity pre-image as L0-chain ones. Edges (M5f.3) carry pre-image properties through `PreExistingProbe.edges: HashMap<Eid, Properties>` populated from `L0Buffer::edge_properties`. **Node/edge-property refs in `predicate_source`** ✅ — `event_row_schema` carries `properties_new` and `properties_old` `LargeBinary` columns holding `cypher_value_codec`-encoded `Value::Map` blobs; `compile_predicate` rewrites `n.foo` / `old.foo` AST nodes via `rewrite_property_refs`, registers Cypher UDFs on its `SessionContext`, and resolves `DummyUdf` placeholders to real impls (two latent compile-path bugs fixed as part of this work — previously predicates with `index()` compiled but failed at first row). `RouteEntry.properties_referenced: HashSet<String>` makes `MutationEvents::from_l0_with_probe` materialize only subscribed property keys (predicate-gated cost). **Tests:** 9 integration tests in `crates/uni/tests/trigger_dispatch.rs` (including `update_to_flushed_vertex_emits_node_update` which asserts L1 pre-image fidelity), 3 in `trigger_predicate.rs`, 2 in `trigger_defer.rs`, 3 in `trigger_predicate_properties.rs` (new — node-prop, value-change, edge-prop), plus unit tests in `triggers.rs`. **Routed out of M5:** `TriggerOutcome::Defer` queue → **M11** (in-memory queue ships now; restart-durable persistence rides on the M11 scheduler driver + `uni_system.deferred_triggers`).
- **M5g (Logical types + ephemeral entities)** ✅ landed. Core landed 2026-05-25 (commit `6ee91a26a`, pure host-side; no `uni-plugin` ABI bump). Typed Node/Edge YIELD materialisation closed in commit `38db9b3eb`. **5 logical types** (`uri`, `geo.point` placeholder, `email`, `ipv4`, `ipv6`) registered at `crates/uni-plugin-builtin/src/logical_types.rs:21-27`. **Ephemeral entities real** via ID-space reservation rather than the proposal's `NodeIdentity` enum refactor — same trick M5b used for virtual label IDs. Shipped pieces: (a) `Vid::EPHEMERAL_BIT` / `Vid::ephemeral` / `Vid::is_ephemeral` / `Vid::transient_id` (and `Eid` analogs) in `crates/uni-common/src/core/id.rs` — top bit of the 64-bit id reserved for transient identities, `INVALID` explicitly excluded from `is_ephemeral` so the two sentinels don't collide; (b) `QueryProcedureHost::allocate_transient_id()` — monotonic per-host `AtomicU64` counter (bottom 63 bits), always-available, no capability required; (c) `UniError::EphemeralWriteAttempt { kind, id }` variant in `crates/uni-common/src/api/error.rs`; (d) write-path gate in `crates/uni-query/src/query/executor/write.rs` — `reject_if_ephemeral_vid` / `reject_if_ephemeral_eid` fires at `execute_set_items_locked` (after `vid_from_value` + after the map-edge and direct-`Value::Edge` arms) and at the top of `execute_delete_vertex` / `execute_delete_edge_from_map`, so SET/DELETE/MERGE against an ephemeral id abort with the typed error before reaching the writer; (e) `uni.create.vNode(labels, props)` + `uni.create.vEdge(src, type, props, dst)` procedures at `crates/uni-query/src/procedures_plugin/create.rs`, registered alongside the other host-coupled built-ins. **12 integration tests** in `crates/uni-query/tests/ephemeral_entities.rs` (id round-trips, INVALID-vs-ephemeral distinction, overflow→INVALID, error format, monotonic allocator within 63 bits, vNode/vEdge invocation surface, host-required negative test, plus the M5g-closure round-trip `vnode_typed_yield_round_trips_through_cypher_expression`). **Typed Node/Edge YIELD materialisation** ✅ — `VNodeProcedure` tags its YIELD signature with `_yield_kind = node_vid_source` so the planner's existing node-shape expansion (`expand_node_yield_fields`) drives a flat `_vid + _labels + per-property` column tuple; `VEdgeProcedure` emits a single `edge` column shaped as `Struct(_eid, _type_name, _src, _dst, properties)` using the canonical `edge_struct_fields`. `build_typed_column` gains `DataType::UInt64` and `DataType::Struct(_)` arms; `uni.create.v{Node,Edge}` added to `NODE_YIELD_PROCEDURE_NAMES` and `is_df_eligible_procedure`. (Note: `DataType::Extension` doesn't exist on this Arrow/DataFusion stack, and native `MATCH (n) RETURN n` itself produces the flat tuple — the "one typed Node column" is a user-visible YIELD abstraction layered over the existing expansion.) The previously-coupled generic `Rga<T>` work is now standalone (see M5d above) — the bit-tagging approach decoupled the two sub-tasks.
- **M5h (Pushdown)** ✅ landed. All 5 marker traits (`SupportsFilterPushdown`, `SupportsProjectionPushdown`, `SupportsLimitPushdown`, `SupportsTopNPushdown`, `SupportsAggregatePushdown`) defined at `crates/uni-plugin/src/traits/pushdown.rs:55-99`. `PushdownNegotiationRule` (`crates/uni-plugin-builtin/src/optimizer/pushdown_negotiation.rs`, 10 tests in `crates/uni-query/tests/pushdown.rs`) shipped in Batch 2 follow-ups (commit `8646ab212`) — recognises `PushdownAwareTable`-wrapped providers via `downcast_markers` and elides Filter/Projection/Limit/TopN/Aggregate nodes when the source claims full handling. **Public `LanceFilterPushdown` marker shipped 2026-05-25** (commit `df285371d`): a stateless `SupportsFilterPushdown` impl in `uni-plugin-builtin/src/storage.rs` that delegates to `expr_to_lance_filter` to classify encodable vs unencodable predicates, available for wrapping any Lance-backed `TableProvider` via `PushdownAwareTable::with_filter`. **`StorageTableProvider` bridge** ✅ — new module `crates/uni-plugin-builtin/src/storage_table_provider.rs` exposes `Arc<dyn Storage>` through DataFusion's `TableProvider` with a chunked `StorageScanExec` (`ExecutionPlan`); `supports_filters_pushdown` delegates to the same predicate classifier and registration wraps via `PushdownAwareTable::with_filter(provider, StorageFilterPushdown)` so `PushdownNegotiationRule` elides the `FilterExec` when the predicate is encodable. `MemoryStorage::read_batch` now honors filters via `datafusion::physical_expr::create_physical_expr` + `filter_record_batch`; `pushdown_negotiation.rs::try_rewrite_filter` embeds the elided predicate into `TableScan::filters` so the source actually receives it. Acceptance: 3 tests in `crates/uni-query/tests/storage_table_provider.rs` — `match_mem_table_returns_filtered_rows`, `explain_elides_filter_for_encodable_predicate` (`EXPLAIN` shows no `FilterExec` above the scan when encodable), `explain_keeps_filter_for_inexpressible_predicate` (negative guard).
- **M5i (Connector/Auth/Authz/Collation/CDC/Catalog/ReplacementScan)** ✅ landed (M5 ownership complete — all routed host-consultation work is owned by the milestones listed below). `BasicAuthProvider` (Blake3-hashed, `auth.rs:64`), `AllowGroupAuthzPolicy` (group ACL, `auth.rs:162`), `NoopConnector` (`extras.rs:42`), `MemoryCdcOutputProvider` (`extras.rs:84`), `EmptyCatalog` (`extras.rs:136`), `NeverReplacementScan` (`extras.rs:175`), plus **5 collations** in `crates/uni-plugin-builtin/src/collations.rs` — `AsciiCaseSensitive`, `AsciiCaseInsensitive`, `UnicodeCodepoint`, `UnicodeCaseInsensitive`, `NaturalNumeric` — all real and registered. (Earlier drafts of this plan referenced an `IcuCollation`; the shipped collations are pure-stdlib variants instead — ICU was descoped to avoid the `icu4x` dep on the built-in path, and the existing 5 cover the proposal §4 acceptance criterion of "at least one representative built-in registration".) Planner-side consultation for `CatalogProvider` + `ReplacementScanProvider` shipped in Batch 2 follow-ups (commits `8428266fc`, `c5e0a1d18`) including virtual label-id allocation for catalog-resolved labels. **90 tests on `uni-plugin-builtin`** (verified by `#[test]` + `#[tokio::test]` count). **Routed out of M5 to owning milestones:**
  - *Host consultation of `AuthProvider` at `Session::open` / `Uni::authenticate`* → **M6a** (depends on external WASM plugins being loadable for the surface to be meaningful; see M6a deliverables).
  - *Host consultation of `AuthzPolicy` at planner pre-execution* → **M6a**.
  - *Host wiring of `Connector::start`* → **M6a**.
  - *CDC runtime — `CdcOutputProvider` driver loop + `uni_system.cdc_checkpoints` persistence* → **M11** (depends on the tokio scheduler driver M11 lands; see M11 deliverable #4).
  - *`ORDER BY ... COLLATE <name>` Cypher grammar extension + sort comparator wiring + indexed-string-lookup `normalize` wiring* → **M12** (depends on a Cypher grammar extension; see M12 deliverable #7).

**Goal:** Every other in-process plugin surface defined in the proposal becomes a real trait with a built-in registration. After M5, the integrity invariant ("if the framework cannot express a built-in, the framework is wrong") is enforced end-to-end for native Rust plugins. The only loaders not yet shipped are WASM (M6) and PyO3 (M8).

**Note:** Can be parallelized across engineers (sub-tasks below are independent).

**Sub-tasks (parallelizable):**

#### M5a — Storage backends (`StorageBackend` trait; Lance backend as built-in)
- New `crates/uni-store/src/storage_backend.rs` — `StorageBackend` + `Storage` traits.
- Scheme-based dispatch: `uni-store::open(uri, registry)` consults `PluginRegistry::storage_backend(scheme)`.
- Existing Lance backend wrapped as `LanceStorageBackend` in `uni-plugin-builtin/src/storage_lance.rs`, registered under `"lance"` scheme.
- A `memory://` in-memory backend registered as a second built-in, for tests.
- Tests: `crates/uni-store/tests/scheme_dispatch.rs` (new); existing store tests pass.

**Carried from M5 Batch 1 follow-ups (commit `98bdf6a4`, 2026-05-24):**
- ~~*Lance predicate pushdown.*~~ ✅ **done** (M5a polish landed) — `LancePluginStorage::read_batch` and `::delete` encode predicates via `datafusion::sql::unparser::expr_to_sql` and pass the resulting SQL string to Lance's `ScanRequest::with_filter` / `delete_rows`. Unencodable predicates surface as `FnError 0x711` (e.g. `ScalarValue::Binary` literals) so callers can fall back to a DataFusion `Filter` above the unfiltered scan. Anchors: `crates/uni-plugin-builtin/src/storage.rs::expr_to_lance_filter` + `LancePluginStorage::read_batch` / `::delete`. Tests: `lance_predicate_pushdown_filter_eq_string`, `lance_predicate_pushdown_unsupported_returns_711`, `lance_delete_with_predicate_drops_only_matching`.
- ~~*Predicate-less delete fast-path.*~~ ✅ **done** — `Expr::Literal(ScalarValue::Boolean(Some(true)))` short-circuits to `uni_store::StorageBackend::replace_table_atomic(name, vec![], schema)` and returns the pre-truncation row count. Test: `lance_delete_with_always_true_truncates_table`.
- ~~*`LancePluginStorage::fork` wiring.*~~ ✅ **done** (M5a polish landed 2026-05-24, `uni-plugin` 1.4.0 → 1.5.0) — investigation of `fork/scope.rs` + `BranchedBackend` confirmed forks are Lance-native per-dataset branch chains (not COW snapshots). Trait `Storage::fork` grew a `table: &str` parameter for per-dataset granularity (the design call locked with the user: uni-store retains multi-dataset orchestration above the plugin barrier) and now returns `BranchMetadata { parent_version, branch_name }` so callers can chain nested forks without re-querying. `LancePluginStorage` overrides `supports_branching() → true` and `fork()` to dispatch through new `LanceDbBackend::fork_branch(table, src, dst)` (uses `lance_branch::create_branch` for src=="main", `create_branch_from` for nested). Tests: `lance_fork_branch_creates_branch_with_parent_version`, `lance_fork_branch_rejects_unknown_src`.
- ~~*Surface `WriteHandle::id` from Lance.*~~ ✅ **done** — `LancePluginStorage::write_batch` now calls `LanceDbBackend::get_table_version` post-write and returns the monotonic version as `WriteHandle::id` (falls back to `0` on `None`). Test: extended `lance_backend_round_trip_in_tempdir` asserts strict monotonicity across two sequential writes.

#### M5b — Index kinds (`IndexKindProvider`; vector index as built-in)
- New `crates/uni-query/src/query/df_graph/index_provider.rs` — refactor the hardcoded vector KNN in `vector_knn.rs` behind `IndexKindProvider`.
- `VectorIndexProvider` in `uni-plugin-builtin/src/index_vector.rs`, registered under `IndexKind::Vector`.
- Tests: `crates/uni-query/tests/index_kind_dispatch.rs` (new); existing vector-search tests pass.

**Carried from M5 Batch 1 follow-ups (commit `98bdf6a4`, 2026-05-24) — "registry is canonical" thread:**

M5b establishes the host-consults-plugin-registry pattern for the planner. Three additional surfaces should ride on the same hook point in the same milestone (rather than each plumbing a parallel consultation site):

- *#11-Catalog — planner consults `CatalogProvider` for unknown labels.* Today the registry holds `EmptyCatalog` (`uni-plugin-builtin/src/extras.rs:128-165`) but `crates/uni-query/src/query/planner.rs` never calls it. When `MATCH (n:UnknownLabel)` resolution fails the native catalog, walk registered `CatalogProvider`s before erroring. Anchors: `extras.rs:128-165` (provider); planner resolution sites should be enumerated alongside the M5b refactor.
- *#11-ReplacementScan — planner falls back to `ReplacementScanProvider` for unknown identifiers.* `NeverReplacementScan` is registered (`extras.rs:167-182`) but never consulted. Gate behind `replacement_scans` config (default off) per proposal §4.23. Same planner hook as M5b's `IndexKindProvider` consultation. Anchors: `extras.rs:167-182`; planner identifier-resolution sites.
- *#7 — `Uni::add_hook` becomes sugar for `add_plugin(BuiltinHookPlugin::new(...))`.* The M5e bridge (`LegacyHookAdapter`, `crates/uni/src/api/hooks.rs:155-303`) makes this mechanically possible today, but the proposal's "Session dispatch through registry" target requires `Session::run_before_query_hooks` / `run_after_query_hooks` (in `crates/uni/src/api/session.rs:677-720`) to iterate the registry instead of the local `hooks: HashMap`. This is the same shape as M5b's planner consultation — best landed under the same "registry is canonical" design.

Net effect on M5b scope: ship the `IndexKindProvider` planner hook, then reuse it for catalog + replacement-scan resolution and for hook dispatch. Acceptance still primarily driven by the index kind dispatch test, plus three small "registry-consulted" tests per surface.

**✅ `IndexProbeExec` bridge for non-built-in `IndexKindProvider`s (landed 2026-05-24 — `uni-plugin` 1.7.1).** End-to-end planner dispatch through a plugin-registered `IndexHandle` now works. The original (a)/(b)/(c) plumbing was reorganized in implementation: rather than build a sibling `IndexProbeExec` (which would have duplicated `GraphVectorKnnExec`'s threshold filter, score normalization, label/vid emission, and property hydration), a `VectorSource::{Native, Plugin{kind, handle}}` enum was added inside `GraphVectorKnnExec` and only the retrieval step branches. The kind-agnostic post-processing pipeline is reused unchanged.

Shipped pieces:
- **Host registry — `PluginRegistry::register_index_handle / index_handle / deregister_index_handle`** (`crates/uni-plugin/src/registry.rs`) — host-side (not `PluginRegistrar`-gated) sub-table `index_handles: DashMap<SmolStr, IndexHandleEntry>` keyed by index *name*. `IndexHandleEntry { kind, handle }` is `Clone` (cheap; inner `Arc`).
- **`VectorSource` enum + `GraphVectorKnnExec::with_plugin_source`** (`crates/uni-query/src/query/df_graph/vector_knn.rs`) — new ctor wires the handle alongside all existing args; `retrieve_vid_scores(source, …)` private helper branches on `Native` (`StorageManager::vector_search`) vs `Plugin` (build 1-row `[FixedSizeList<Float32>]` query batch → `handle.probe` → extract `(vid: Int64, distance: Float32)`).
- **Planner dispatch in `plan_vector_knn`** (`crates/uni-query/src/query/df_planner.rs:1733-1787`) — schema lookup via `vector_index_for_property(label, prop)` yields the index name; `registry.index_handle(&name)` then chooses `with_plugin_source` over `new`. Native vector indexes never register a handle, so the fall-through preserves the "no behavior change for built-ins" invariant.
- **Host plugin-registry threading into the executor** (`crates/uni-query/src/query/executor/read.rs`, `crates/uni-query/src/query/executor/procedure.rs`) — `ProcedureRegistry::plugin_registry()` accessor exposes the host registry that `Uni::build` already attached at construction; `read.rs` threads it into `HybridPhysicalPlanner::with_plugin_registry`. Falls back to the `CustomFunctionRegistry`'s shadow registry for low-level setups that bypass `Uni::build`.

Acceptance shipped: `crates/uni/tests/plugin_index_handle_dispatch.rs` — positive test (`plugin_handle_dispatched_when_registered`) registers a `CountingHandle`, runs `MATCH (n:Item) WHERE vector_similarity(n.embedding, [1.0, 0.0]) > 0.0 RETURN n.name`, asserts (a) the handle's `probe` was invoked exactly once and (b) the row set reflects the handle's selection (1 row) instead of the native L2 path's (2 rows); negative test (`no_plugin_handle_falls_through_to_native_path`) regression-guards the native dispatch.

**✅ Planner identifier-resolution hook for `ReplacementScanProvider` (landed 2026-05-24 — `uni-query` consumer-side change; `uni-plugin` surface unchanged at 1.7.1).** All three call-resolution sites now consult `ReplacementScanProvider`s when the per-session `replacement_scans_enabled` gate is on. The gate is per-`Session` (default off), flipped via `Session::set_replacement_scans(bool)` and shared across session clones via `Arc<AtomicBool>`. A `uni.config.set_*` Cypher-callable wrapper was deliberately deferred — the Rust setter is sufficient for opt-in, and a generic config-setter procedure is its own scope decision.

Shipped pieces:
- **`QueryPlanner::consult_replacement_scan(ReplacementRequest) -> Option<Replacement>`** (`crates/uni-query/src/query/planner.rs`) — single first-match-wins helper; the existing `replacement_scan_resolves(&str) -> bool` is preserved as a thin wrapper that delegates with `ReplacementRequest::Label`, so the pre-existing shortestPath site (`planner.rs:3973`) keeps working unchanged.
- **Procedure rewrite** at the logical-plan construction site for `LogicalPlan::ProcedureCall` (`planner.rs` Clause::Call arm). Pre-checks `procedure_resolves(name)` against `PluginRegistry::procedure(qname)` using the same namespace-prefix-strip rules as `ProcedureRegistry::resolve_user_procedure`; consults only when unresolved; substitutes the AST `procedure_name` with `new_qname.to_string()`; errors loudly when the rewritten name itself does not resolve (rewrite depth capped at 1 — no second-tier consult).
- **Function rewrite** as an AST pre-pass at the top of `QueryPlanner::plan_with_scope` (`planner.rs`). The new `crates/uni-query/src/query/rewrite/function_rename.rs` walker descends into every expression-bearing position (`Match`/`Create`/`Return`/`With`/`Unwind`/`Set`/`Delete`/`Remove`/`Call`) and substitutes `Expr::FunctionCall.name` when a provider claims it. The walker is post-order and single-pass — the rewritten name is NOT re-visited (built-in depth cap). For synthetic-namespace targets (`builtin.*`, `user.*`), only `.local()` is used so the substituted name matches the bare-name-keyed Cypher dispatchers (`UPPER`, `ABS`, …); for plugin-namespaced targets, the full dotted form is preserved.
- **Label strict mode** at `planner.rs:5578` — when the gate is on, the schemaless `ScanMainByLabels` fallback is replaced by a consult. `Some(Replacement::CatalogTable(_))` originally errored with `"… virtual label-id allocation (follow-up #6) is not yet wired"`; **with follow-up #6 landed (below), this branch now lowers end-to-end to `CatalogVertexScanExec` via `QueryPlanner::allocate_virtual_label`**. `None` still errors with `"strict-mode (replacement_scans=true) requires the label to resolve"`. When the gate is off, today's silent-empty behavior is preserved bit-for-bit (no consult, no error).
- **Session config gate** (`crates/uni/src/api/session.rs`) — `replacement_scans_enabled: Arc<AtomicBool>` field, `pub fn set_replacement_scans(bool)` setter, `pub fn replacement_scans_enabled() -> bool` getter. Threaded into `QueryPlanner` at `session.rs:904` via `.with_plugin_registry(...).with_replacement_scans(...)` — Batch 2 had left the plugin registry un-threaded from this site, which is also fixed.

Acceptance shipped: `crates/uni/tests/plugin_replacement_scan_dispatch.rs` — 7 tests covering all three sites end-to-end:
1. `procedure_rerouted_when_enabled` — `CALL missing_proc('hello')` reroutes to `builtin.system.echo` and returns the echo'd string; provider consulted exactly once.
2. `procedure_not_rerouted_when_disabled` — same query errors when gate is off; provider consulted zero times.
3. `function_rerouted_when_enabled` — `RETURN my_fn('x')` reroutes to `UPPER`, returns `"X"`.
4. `function_rewrite_loop_guarded` — `loop_a ↔ loop_b` cycle errors at execute (no infinite loop); consult fires exactly once.
5. `label_unknown_resolves_via_catalog_provider` — `MATCH (n:Phantom)` returns rows from the stub `CatalogTable` (renamed and converted to positive when #6 landed; originally `label_unknown_errors_under_strict_with_provider`).
6. `label_unknown_errors_under_strict_no_provider` — same query errors with the "no CatalogProvider or ReplacementScanProvider claimed it" message.
7. `label_unknown_silent_when_disabled` — regression guard: same query returns 0 rows silently when the gate is off; provider (even when registered) is not consulted.

The pre-existing `crates/uni-query/tests/replacement_scan_dispatch.rs` registry-reachability tests stay untouched.

**✅ Virtual label-id (and virtual edge-type-id) allocation for catalog-resolved labels (landed 2026-05-24 — `uni-plugin` consumer-side change; `uni-plugin` API gains an additive `register_virtual_label` / `virtual_label_by_id` / `virtual_label_by_name` family + edge-type analogs on `PluginRegistry`).** A `MATCH (n:External)` against a label claimed by a registered `CatalogProvider` (or `ReplacementScanProvider` returning `Replacement::CatalogTable`) now allocates a virtual `u16` label-id and dispatches to a new `CatalogVertexScanExec` that adapts the catalog table's rows into graph-row shape (`_vid`, `_labels`, `<var>.<prop>`). Same shape for virtual edge types via `CatalogEdgeScanExec`.

Shipped pieces:
- **ID-space reservation in `uni-common`** (`crates/uni-common/src/core/schema.rs`, `crates/uni-common/src/core/edge_type.rs`) — `VIRTUAL_LABEL_ID_START = 0xFF00`, `VIRTUAL_LABEL_ID_SENTINEL = 0xFFFF` (255 allocatable slots), plus `is_virtual_label_id` predicate. Edge-type analog uses a top sub-range of the schema'd space (`VIRTUAL_EDGE_TYPE_ID_START = 0x7FFF_FF00`) so it remains distinguishable from schemaless (bit 31). Native `SchemaManager::add_label` / `add_edge_type` refuses any id that would land in the virtual range.
- **Host-side allocator on `PluginRegistry`** (`crates/uni-plugin/src/registry.rs`) — `VirtualEntry { name, table }`, two `Mutex<VirtualLabelInner / VirtualEdgeTypeInner>` allocators, public idempotent `register_virtual_label` / `register_virtual_edge_type` (re-registration updates the stashed `Arc<dyn CatalogTable>` so cached `LogicalPlan`s pick up the latest table on next execute) and lookup helpers. Stored on the host's registry rather than `Schema` to keep `uni-common` decoupled from `uni-plugin`'s `CatalogTable` trait.
- **Planner consult-and-allocate** (`crates/uni-query/src/query/planner.rs`) — `QueryPlanner::allocate_virtual_label` / `allocate_virtual_edge_type` first consult `CatalogProvider` (always-on, Batch 2 semantics) then `ReplacementScanProvider` (gated). The shortestPath target site (`planner.rs:3973`) and the strict-mode `ScanMainByLabels` fallback (`planner.rs:5577`, added in #5) both lower the consult result to a plain `LogicalPlan::Scan { label_id: <virtual_id> }`. The shortestPath edge-type loop now allocates virtual edge-type ids the same way. Write-path rejection: a new `reject_virtual_label_writes` helper fires from `Clause::Create` and `Clause::Merge` planning so `CREATE (:External)` / `MERGE (:External)` error at plan time with a "virtual / read-only" message.
- **`CatalogVertexScanExec` + `CatalogEdgeScanExec`** (`crates/uni-query/src/query/df_graph/catalog_scan.rs`, NEW) — implement `ExecutionPlan` and adapt the catalog table's `RecordBatch`es into the graph-row schema. Vertex `_vid` is synthesized as `(virtual_label_id as u64) << 48 | row_offset` (high-16-bit encoding makes virtual vids unambiguously distinguishable from native vids, which are sequentially allocated from 0). Edge `_eid` uses `(virtual_type_id as u64) << 32 | row_offset`. The catalog table's `src_id` / `dst_id` columns map to synthesized `_src_vid` / `_dst_vid` (Int64/UInt64/UInt32 source columns are all accepted). Cypher filters are translated to DataFusion `Expr`s and passed as `table.scan(filters)` for advisory pushdown; the planner re-applies them as a top-level `FilterExec` for safety.
- **Physical-planner dispatch** (`crates/uni-query/src/query/df_planner.rs`) — `plan_scan` checks `is_virtual_label_id(label_id)` and routes to `CatalogVertexScanExec` (reading the table from `PluginRegistry::virtual_label_by_id`) before the native `GraphScanExec` branch.
- **`PluginRegistry` threading** (`crates/uni/src/api/impl_query.rs`) — every `QueryPlanner` construction site in `UniInner` (explain, profile, cursor, prepared, mutation execute) now chains `.with_plugin_registry(self.plugin_registry.clone())`. This closes a pre-existing gap: Batch 2 wired the registry only at `session.rs:904`, so the virtual-label allocator would have been unreachable from tx/explain/profile paths without this fix.

Acceptance shipped: `crates/uni/tests/plugin_virtual_label_dispatch.rs` — 10 tests covering the read path end-to-end (`MATCH (n:External) RETURN n.foo` returns rows; multi-property projection; filter pushdown is forwarded to `CatalogTable::scan`; virtual `_vid`s lie in the high-16-bit range; idempotent allocation across queries; edge-type plan-level allocation; CREATE/MERGE rejection; native-vs-virtual range invariants). The Batch 2 follow-up #5 test `label_unknown_errors_under_strict_with_provider` is renamed to `label_unknown_resolves_via_catalog_provider` and converted to a positive (rows-returned) test now that the deferral is gone.

**M5b closure (all three landed in commit `38db9b3eb`):**
- (a) **M5b.1** ✅ — multi-label `MATCH (n:Virtual:Native)` intersection. `classify_labels` helper splits the label list once; `plan_multi_label_scan` builds `CatalogVertexScanExec` (Union over multiple virtuals) and joins against the native `GraphScanExec::new_multi_label_vertex_scan` via `LeftSemi` keyed on `_vid` (`LeftSemi` over `Inner` to avoid duplicate `_vid` output columns).
- (b) **M5b.2** ✅ — `DELETE` / `SET` against virtual labels and edge types rejected at the runtime write path. `reject_virtual_label_write` / `reject_virtual_edge_type_write` helpers in `crates/uni-query/src/query/executor/write.rs` fire at `execute_set_items_locked` (SetItem::Labels), `execute_remove_labels`, `execute_delete_vertex`, `execute_delete_edge_from_map`, and the Value::Edge delete arm.
- (c) **M5b.3** ✅ — native↔virtual joins mid-pattern. `hydrate_virtual_target_from_catalog` wraps a traverse plan with an inner-join against `CatalogVertexScanExec` on `{target}._vid` when the target label is virtual. `plan_traverse_virtual_edge` replaces `GraphTraverseExec` with a `HashJoin(input × CatalogEdgeScanExec)` keyed on `_src_vid`/`_dst_vid` for the all-virtual single-hop case. Logical planner resolves virtual edge-type and target-label names through the registry. Documented limitations (covered by 3-test fixture in `crates/uni/tests/plugin_mid_pattern_virtual.rs`): single-hop all-virtual edge type only; mixed native+virtual edge-OR lists fall through to legacy `GraphTraverseExec`; undirected `(a)-[r:Virtual]-(b)` joins outgoing side only.

#### M5c — Algorithms (`AlgorithmProvider`; 32 algorithms as built-ins) + virtual projections

Five sub-phases. M5c.3 is the **P6 unblock** (Cypher / virtual projection); M5c.4 is full GDS parity (named projections). Each ships independently; the algorithm trait does not change after M5c.1, so M5c.3-4 are pure host-side patches.

##### M5c.1 — Wrap 32 algorithms as `AlgorithmProvider` plugins (foundation)
- New `crates/uni-algo/src/algorithm_provider.rs` — `AlgorithmProvider` trait taking `&AlgorithmContext` whose `projection: &GraphProjection` field is the in-memory CSR (today's shape, unchanged).
- Existing 32 algorithms in `crates/uni-algo/src/algo/mod.rs:55` become `AlgorithmProvider` impls in `uni-plugin-builtin/src/algorithms/*` (one file per category: centrality.rs, community.rs, paths.rs, …).
- `AlgorithmProcedure` adapter (from M4) now resolves through the plugin registry instead of the static `AlgorithmRegistry`. Adapter constructs `Native` projection only at this phase.
- `PregelProgramProvider` trait stubbed (full Pregel executor deferred to a follow-up issue; the trait is in place for users to implement).
- Tests: every existing algorithm test passes; new `crates/uni-algo/tests/registry_dispatch.rs`.

##### M5c.2 — `(graphRef, config)` procedure signature + `Native` variant (no behaviour change)
- Add `ProjectionInput` enum (proposal §4.10.1) in `crates/uni-algo/src/projection_input.rs` — `Native | Cypher | Named`, with only `Native` constructible at this phase.
- New 2-arg signature registered alongside the legacy 5-arg form for every algo, keyed on `(name, arity)` in `ProcedureRegistry::resolve_user_procedure`. The 5-arg form prints a `DeprecationWarning` via the host warnings slot.
- Adapter parses `graphRef` via the map-key shape (§4.10.1 dispatcher); `Native` is the only resolved variant; `Cypher` / `Named` return "not implemented in this phase" errors with a clear migration pointer.
- Cypher TCK: every scenario using `CALL uni.algo.X(...)` continues green; one new feature `AlgorithmGraphRefNative.feature` exercises the 2-arg shape against representative algos (pageRank, louvain, dijkstra).

##### M5c.3 — Cypher projection variant (P6 unblock)
- Add `host.execute_query(cypher: &str, mode: QueryMode) -> Result<RecordBatch>` accessor on `QueryProcedureHost` (the foundation crate already has the slot via the host pointer; this is a typed method, not an ABI change). Inner queries forced to `ReadOnly` regardless of the caller's request.
- Add `ProjectionBuilder::from_rows(nodes_rb, edges_rb, weight_col, include_reverse)` in `crates/uni-algo/src/algo/projection.rs` — validates schemas (`id: Int64` on nodes; `source: Int64, target: Int64, weight: Float64?` on edges), maps to dense slots, builds CSR + optional reverse + weight column.
- Adapter handles `ProjectionInput::Cypher` by calling `host.execute_query` for both queries and feeding the rows into `from_rows`. Memory cap reused (`AlgorithmConfig::max_projection_memory`).
- New Cypher TCK feature `CypherProjection.feature`: identity scenario (Cypher projection over `(:Person)-[:KNOWS]->(:Person)` produces byte-identical PageRank scores to the equivalent `Native` form); derived-edge scenario (entity co-occurrence via `MENTIONS` join, à la P6); error scenarios (missing `id` column, attempted write inside `nodeQuery`, memory-cap exceeded).
- Downstream lift: `uniko::topics.rs` Rust-side adjacency builder deletes; the LPA call becomes a `CALL uni.algo.labelPropagation({nodeQuery, relQuery, weightColumn}, ...)`. Track that lift as an explicit follow-up.

##### M5c.4 — Named projections + `ProjectionStore`
- New `crates/uni-query/src/projection_store.rs` — `ProjectionStore` with per-`Database` scope (one `Arc<ProjectionStore>` hangs off `Uni` / `Database`; not session-scoped, not persisted to disk). `RwLock<HashMap<String, ProjectionEntry>>` shape per §4.10.3.
- Procedures `uni.graph.{project, drop, list, exists}` as `ProcedurePlugin` registrations in the same place M4's host-coupled built-ins land. `project` reuses the materialiser from M5c.3; `list` yields `name, node_count, edge_count, bytes, created_at, source_kind`.
- Adapter handles `ProjectionInput::Named` by `store.get(&name)?` lookup. Capability gating: `project` requires the inner queries' capabilities; `drop` requires `Procedure`; `list` / `exists` are read-only.
- v1 eviction is drop-only. The `bytes` field on `ProjectionEntry` exists for a future LRU policy; no LRU in v1. Staleness explicit (frozen at project-time); document loudly. `refresh(name)` deferred.
- New TCK feature `NamedProjection.feature`: project + reuse across algos; drop; list-then-exists; restart-clears; duplicate-name rejection; permission failure for inner-query-requires-write.

##### M5c.5 — Delete the legacy 5-arg shim (M5-blocking; lands within M5 after M5c.2 has been live long enough for in-tree callers to migrate)
- Drop the 5-arg signature registrations from every algo built-in.
- `ProcedureRegistry::resolve_user_procedure` arity-keyed dispatch becomes single-arity-per-name (cleanup).
- Migration audit: grep workspace for `CALL uni.algo.*(.*\[.*\],` (positional list-of-labels) and assert zero hits in `crates/`, `tests/`, `examples/`; rewrite any callers found to the V2 `(graphRef, config)` shape as part of this sub-task. Downstream repos (uniko, etc.) consume the migrated APIs at the next downstream sync — no separate deprecation window applies since there are no version boundaries; the in-tree `DeprecationWarning` is sufficient runtime notice for downstream maintainers tracking trunk.

**Parallel work:** M5c.1 must complete before any other sub-phase. M5c.2 → M5c.3 → M5c.4 is the natural order, but M5c.3 and M5c.4 are independent enough that they could land in parallel if two engineers are on it.

#### M5d — CRDT kinds (`CrdtKindProvider`; LWW/OR-Set/RGA/Counter/MVR as built-ins)
- New `crates/uni-crdt/src/crdt_kind_provider.rs`.
- 5 built-in CRDTs in `uni-plugin-builtin/src/crdts.rs`.
- Tests: existing CRDT tests pass; new dispatch tests.

#### M5e — Hooks (phased SessionHook expansion)
- `SessionHook` trait at `crates/uni/src/api/hooks.rs:64` expanded to phased methods (`on_parse`, `on_analyze`, `on_plan`, `on_execute_start`, `on_execute_end`, `before_commit`, `after_commit`, `on_abort`). Existing methods (`before_query`, `after_query`) become default-implemented in terms of `on_parse` + `on_execute_end` for backward compat.
- `Uni::add_hook` becomes sugar for `Uni::add_plugin(BuiltinHookPlugin::new(hook))`.
- Tests: existing `crates/uni/tests/hooks_test.rs` and `fork_hooks.rs` continue to pass with no test changes.

**Carried from M5 Batch 1 follow-ups (commit `98bdf6a4`, 2026-05-24) — "phased context shape v1.1" `uni-plugin` minor ABI bump:**

✅ **all three landed as `uni-plugin` 1.3.0 → 1.4.0** (see `crates/uni-plugin/CHANGELOG.md`):
- ~~*Phased `CommitContext` carries `CommitResult`.*~~ ✅ — added `PluginCommitResult { mutations, version, wal_lsn, duration }` (slim mirror to avoid a `uni-plugin → uni-db` dep), `CommitContext::commit_result: Option<&'a PluginCommitResult>`, builder `with_commit_result`. Bridge mirrors into the legacy `CommitResult` when populated; falls back to the zero stub for back-compat. Test: `bridge_routes_commit_result_through_to_legacy`.
- ~~*`ParseContext.query_type` field.*~~ ✅ — added `enum QueryType { Cypher, Locy, Execute }` (`#[derive(Default)]` = `Cypher`), `ParseContext::query_type`, builder `with_query_type`. Bridge translates plugin → legacy enum. Test: `bridge_routes_query_type_locy_through_to_legacy`.
- ~~*`ParseContext.params` field.*~~ ✅ — chose option (b): `params: &'a [(SmolStr, ScalarValue)]` (Arrow-shaped; `uni-plugin` already depends on `smol_str` + `datafusion-common`, no `uni-common` exposure). Bridge converts to legacy `HashMap<String, Value>` via best-effort `scalar_to_value` (covers all primitive variants, surfaces unsupported variants as `Value::Null` with a `tracing::warn!`). Test: `bridge_routes_params_through_to_legacy`.

Version bump: `crates/uni-plugin/Cargo.toml` overrides `version.workspace = true` with explicit `version = "1.4.0"`; workspace `Cargo.toml` declares the `uni-plugin = { path = "...", version = "1.4.0" }` dep version to match. Other workspace crates stay on `1.3.0`.

#### M5f — Triggers (`TriggerPlugin`)
- New trait + subscription model + selector index in `crates/uni-plugin/src/traits/trigger.rs` (already present from M1).
- Host-side wiring in `crates/uni/src/api/triggers.rs` (new) — per-(label, event_kind, property) routing table, mutation-batch construction during commit.
- Tests: `crates/uni/tests/trigger_*.rs` — `Synchronous` rejection in `BeforeCommit`; `Async` fires after commit; label/property/predicate selectors filter correctly.

**Status — landed 2026-05-24 (no `uni-plugin` version bump; pure host wiring on the 1.8.0 ABI):**
- `crates/uni/src/api/triggers.rs` ships `TriggerRouter::from_registry` (per-phase routing table built once per commit) + `MutationEvents::from_l0` (extracts NODE_*/EDGE_* events from the tx-private L0 buffer into the stable §4.18 `RecordBatch` schema: `event_kind | vid_or_eid | label | property | old_value | new_value`).
- `Transaction::commit` wires dispatch at two sites: **(a)** after legacy `before_commit` hooks fire and before the writer lock is taken, the router's `dispatch_before` runs `BeforeMutation` + `BeforeCommit` phases — a `Synchronous` trigger returning `TriggerOutcome::Reject` (or `Err`) aborts commit via `UniError::TriggerRejected { trigger, reason }` (no WAL flush, no lock acquired); **(b)** after legacy `after_commit` hooks fire, `dispatch_after` runs `AfterMutation` + `AfterCommit` phases — `Synchronous` triggers run inline with `catch_unwind`, `Async` / `EventualConsistency` triggers are `runtime.spawn`'d so the writer's hot path is untouched. Empty-registry fast path: a single `TriggerRouter::is_empty()` check skips the L0 event extraction entirely (zero overhead when no triggers are registered).
- Tests: 6 integration cases in `crates/uni/tests/trigger_dispatch.rs` — matching/non-matching label selectors, `Synchronous` `BeforeCommit` reject + rollback visibility, `Async` non-blocking commit, after-phase panic catch, `event_kind` selector filter. Plus 3 unit tests in `crates/uni/src/api/triggers.rs` (mask discriminant stability, empty-router predicate, tx_id hash determinism).
- Final shape (closure commit `38db9b3eb`): `predicate_source` compiles via `cypher_expr_to_df` with full Cypher UDF registration and `DummyUdf` resolution; predicates can reference both the 6 event-row columns AND node/edge property values via `n.foo` / `old.foo` (lowered to `index()` on `properties_new` / `properties_old` `LargeBinary` bags, predicate-gated by `RouteEntry.properties_referenced`). `NODE_CREATE` / `NODE_UPDATE` / `NODE_DELETE` (and edge analogs) are distinguished via `PreExistingProbe` which scans L0 + pending-flush L0s and probes L1 storage for vids that drained out in earlier flushes; `old_value` is populated from the L0-chain `vertex_properties` / `edge_properties` maps and from the L1 probe's projected property columns. `TriggerOutcome::Defer` is logged-and-treated-as-`Continue` for now — the durable deferral queue is **routed to M11** as it requires the M11 scheduler driver + `uni_system.deferred_triggers` table.

#### M5g — Logical types + ephemeral entities
- `LogicalTypeProvider` implementation surface complete.
- `Value::Node` / `Value::Edge` gain `NodeIdentity::Ephemeral { transient_id }` and `EdgeIdentity::Ephemeral` variants.
- `host.allocate_transient_id()` (always-available host primitive).
- Built-in logical types in `uni-plugin-builtin/src/logical_types.rs`: `geo.point` (placeholder; full GIS deferred), `uri`.
- Tests: `crates/uni-query/tests/ephemeral_entities.rs` — `apoc.create.vNode` analogue returns ephemeral node; `SET` against it fails with `EphemeralWriteAttempt`.

**Carried from M5 Batch 1 follow-ups (commit `98bdf6a4`, 2026-05-24):**
- *Generic `Rga<T>` registration (type-erasure ABI decision).* Today `RgaProvider` in `uni-plugin-builtin/src/crdts.rs:344-432` is hard-bound to `uni_crdt::Rga<String>`; the JSON-snippet trick that backs `apply` assumes `String` elements. Lifting this to arbitrary value types requires the same payload-typing primitive that M5g introduces for ephemeral entities (a typed `Payload` carrying an Arrow `DataType` + bytes, or a `LogicalTypeProvider`-resolved element kind). Land both under the same ABI shape so the typing story only changes once. Anchors: `crates/uni-plugin-builtin/src/crdts.rs:344-432`.

#### M5h — Pushdown marker traits + planner integration
- The five marker traits from §4.25 of the proposal (`SupportsFilterPushdown`, etc.) are wired into DataFusion's logical optimizer.
- An `OptimizerRuleProvider` registration that runs during logical optimization, consulting marker traits on each `TableProvider`.
- Tests: `crates/uni-query/tests/pushdown.rs` — `EXPLAIN` of a query against a backend implementing `SupportsFilterPushdown` shows no `Filter` operator above the scan for handled predicates.

#### M5i — Connector, Auth, Authz, Collation, CDC, Catalog, ReplacementScan
- Trait + 1 representative built-in registration per surface (e.g., a `BasicAuthProvider` for testing, an `IcuCollation` for locale-aware sort).
- These are "round out the surface" implementations; depth comes in M11.

**Acceptance criteria for M5 overall:**

1. `grep -rn 'enum FoldAggKind\|match name.*\"uni\\.' crates/uni-query/src/ crates/uni-algo/src/` returns zero hits. (M3 and M4 already validated this; M5 ensures the algorithm registry is also retired.)
2. `cargo nextest run --workspace` — full suite green.
3. Every trait listed in proposal §4 has at least one registration in `uni-plugin-builtin` and at least one passing test in `uni-plugin-builtin/tests/`.
4. The `crates/uni-plugin-builtin/Cargo.toml` declares no `wasmtime` or `pyo3` dependency. (All native surfaces are pure Rust + Arrow.)
5. New TCK scenarios: `PluginIndexKindE2E`, `PluginStorageBackendE2E`, `PluginTriggerE2E` — all green.

**Risks:**

- *Risk:* The sub-tasks have hidden coupling that surface late. *Mitigation:* The dependency graph among sub-tasks (M5a..M5i) is genuinely flat — each one touches different files. The risk is at the *integration test* level (e.g., a `TriggerPlugin` test that depends on a `StorageBackend` that supports it).
- *Risk:* `Pregel` framework is genuinely large. *Mitigation:* The trait is in place; the Pregel *executor* (the iteration runtime) is allowed to be a stub in M5 with a tracking issue. The proposal doesn't require all built-in algorithms be re-implemented as Pregel programs in v1.0 — just that the trait exists and a user can.

**Parallel work unlocked:** M6 (WASM loader) — depends on M5 mainly for the storage/index trait stability.

---

### M6 — WASM loaders (Option C hybrid: M6a Extism + M6b Component Model)

The original M6 single-loader scope split into two sub-milestones per proposal §5.1.1. **M6a (Extism) shipped first** because it is the user-facing default path; **M6b (Component Model) shipped next** for trusted-infrastructure plugins. The two loaders share the wasmtime runtime; they coexist in one `Uni` instance.

**Status (2026-05-25): ✅ complete (M6a + M6b + deferred-followup all shipped).** Both loaders are wired end-to-end: real wasm artifacts under `examples/example-extism-geo/` and `examples/example-wasm-geo/` exercise the full Arrow-IPC `invoke` paths; `Uni::load_wasm_extism` / `Uni::load_wasm_component` register adapters through `PluginRegistry::scalar_fn`; a cross-ABI byte-parity test (`crates/uni/tests/m6_cross_abi_parity.rs`, proposal §19 #24) confirms both ABIs produce byte-identical f64 outputs across a 5-row haversine matrix. Shared IPC + Pool live in `crates/uni-plugin-wasm-rt/` (M6.shared lift, 18 tests). Build orchestration via `scripts/build-wasm-fixtures.sh`; tests hard-fail if artifacts are missing.

#### M6a — Extism loader: `uni-plugin-extism`

**Status: ✅ complete (2026-05-25).** Full loader (M6a.1), aggregate + procedure ABI (M6a.2), connector lifecycle + write/schema/dbms authz verbs (M6a.3), and example-extism-geo with full Arrow-IPC `invoke` (deferred-followup) all shipped. 53 tests in `uni-plugin-extism` + 9 admin-bucket integration tests. The historical scaffolding note follows for archival context.

**Historical scaffolding note (2026-05-23, pre-cutover): ▶ partial.** `crates/uni-plugin-extism/` exists with `loader.rs`, `host_fns.rs`, `error.rs`. `ExtismLoader::prepare()` is real (parses `ExtismPluginManifest` JSON, intersects declared ∩ granted capabilities, filters HostFnRegistry through effective caps, returns diagnostic `denied_capabilities`). `instantiate()` and `load()` return `ExtismError::NotYetImplemented` — actual extism-sdk integration (plugin instantiation, registration into `PluginRegistrar`, Arrow IPC bridge, example plugin) pending. **12 tests pass.**

**Deliverables (cutover):**
1. `ExtismLoader::load(bytes, grants) -> Result<PluginHandle, ExtismError>` end-to-end.
2. `manifest` plugin-export reader returning the canonical JSON form (§14.2).
3. `register` plugin-export reader returning provided qnames with signatures.
4. Per-host-fn capability filter at plugin construction time (§6.5.1).
5. Arrow IPC payload encode/decode for scalar/aggregate/procedure invocations (shared helpers with `uni-plugin-wasm`'s `ipc.rs` lifted into a common module).
6. Cold-start pool analogous to `WasmInstancePool` (likely sharing the same `crossbeam::ArrayQueue` skeleton — the pool itself is ABI-agnostic).
7. Example `example-extism-geo` Rust plugin (via `extism-pdk`) implementing `geo.haversine` and used by §19 acceptance criterion #4.
8. `Uni::load_wasm_extism(bytes, grants)` public API.

**Acceptance:**
- `cargo nextest run -p uni-plugin-extism` passes; loader exercises the example plugin end-to-end.
- A `geo.haversine` call from Cypher through the Extism loader produces byte-identical output to the compile-time Rust path (cross-ABI parity per criterion #24).
- An Extism plugin without `Capability::Filesystem` calling `host_fs_read` produces `ExtismError::CapabilityDenied`.

**Risk:** lower than M6b. Extism SDK is mature; the surface area we own is the host-fn filter and Arrow IPC payload wiring.

**M6a-owned host consultation for plugin-shipped Auth/Authz/Connector** (formerly routed from M5i — now first-class M6a deliverables, since these surfaces only become meaningful once external WASM plugins can ship custom providers):

- *Auth — host consults `AuthProvider` registry at connection-open.* `BasicAuthProvider` is already registered in `uni-plugin-builtin/src/auth.rs:64` as the built-in canary, but no host code looks it up. Wire `Session::open` / `Uni::authenticate(scheme, credentials)` to iterate registered providers and propagate `AuthOutcome::Reject` as `UniError::AuthenticationFailed`. With the Extism loader live (M6a), users can ship LDAP / OIDC / mTLS providers as canaries against this hook.
- *Authz — query planner consults `AuthzPolicy` registry pre-execution.* `AllowGroupAuthzPolicy` is registered (`auth.rs:162`) but never invoked. Wire `crates/uni-query/src/query/planner.rs` (and any DDL execution path for Schema/DBMS modes) to call `AuthzPolicy::check(principal, action, resource)` and propagate `Decision::Deny` as `UniError::Forbidden`. With Extism live, OPA-style or row-level-security policies become writable as plugins.
- *Connector — host wires `Connector::start`.* `NoopConnector` is registered (`extras.rs:42`) but no host loop invokes it. Wire `Uni::start_connector(protocol, config)` and a connector-lifecycle host loop. With Extism live, Bolt / GraphQL / REST connectors can be user-shipped.

Land these as part of the M6a cutover commit so the first WASM-loaded Auth/Authz/Connector example is the canary test for host consultation. Acceptance: at least one Extism plugin per surface (Auth, Authz, Connector) loads + is invoked end-to-end.

#### M6b — Component Model loader: `uni-plugin-wasm` + cold-start pools

**Status: ✅ complete (2026-05-25).** WIT worlds (scalar/aggregate/procedure) + compile-time `wasmtime::component::bindgen!` shipped; per-major Linker with capability-gated host imports + WASI Preview 2 wired; pool prewarm; `Uni::load_wasm_component` behind `wasm-plugins` feature; aggregate + procedure adapters; example-wasm-geo with full Arrow-IPC `invoke-scalar` via `wit_bindgen::generate!` (deferred-followup). 12 tests in `uni-plugin-wasm` + 18 in `uni-plugin-wasm-rt` (shared) + cross-ABI parity test. One wasmtime version in Cargo.lock (41, aligned with extism transitive). The historical scaffolding note follows for archival context.

**Historical scaffolding note (2026-05-23, pre-cutover): ▶ partial.** `crates/uni-plugin-wasm/` exists with `loader.rs`, `pool.rs`, `ipc.rs`, `error.rs`. `WasmInstancePool<T>` real (`acquire`/`release`, capacity enforcement, metrics). Arrow IPC marshalling (`encode_batch`/`decode_batch`) works end-to-end. `WasmIpcBuffer` RAII shipped. `prepare()` real (manifest parse + cap intersection). `instantiate(_bytes)` and `load(_bytes)` return `WasmError::NotYetImplemented` — wasmtime Component-Model integration (WIT bindings, per-major Linker, capability-gated host imports, `example-wasm-geo.wasm`) pending. **11 tests pass.**

**Goal:** WASM plugin support end-to-end. wasmtime integration, Component Model, WIT bindings, Arrow IPC over linear memory, capability gating by linker absence, per-plugin component pools with cold-start mitigation. A working "hello world" WASM plugin runs through every plugin surface.

**Deliverables:**

1. New crate `crates/uni-plugin-wasm/` with:
   - `lib.rs` — `WasmLoader`, `Uni::load_wasm` implementation.
   - `wit/` — full WIT world definitions for every plugin kind (scalar-plugin, aggregate-plugin, locy-agg-plugin, procedure-plugin, operator-plugin, index-plugin, storage-plugin, algo-plugin, crdt-plugin, hook-plugin, type-plugin, auth-plugin, authz-plugin, connector-plugin, trigger-plugin, background-plugin).
   - `bindings/` — `wit-bindgen`-generated bindings (committed to repo for reproducibility).
   - `ipc.rs` — Arrow IPC marshalling helpers (alloc / free / linear-memory copy).
   - `linker.rs` — per-major `Linker` configuration with capability-gated host imports.
   - `pool.rs` — `WasmInstancePool` (`ArrayQueue`-backed, idle-TTL reaping, pre-warmed instances).
   - `host_impls/` — host function implementations:
     - `host_log`, `host_metric_counter`, `host_span_enter`, `host_span_exit` (always-available).
     - `host_fs_*`, `host_net_*`, `host_query_*`, `host_kms_*`, `host_lock_*`, `host_config_*`, `host_secrets_*`, `host_storage_*` (capability-gated; only added to linker if capability granted).
   - `epoch.rs` — epoch-interruption setup, fuel metering, memory limits.

2. New crate `crates/example-wasm-geo/` — a minimal Rust-to-WASM plugin (`geo.haversine` scalar fn) used as the M6 acceptance fixture. Built as part of CI (`cargo build --target wasm32-wasip2 --release -p example-wasm-geo`).

3. Tests:
   - `crates/uni-plugin-wasm/tests/arrow_ipc_roundtrip.rs` — `RecordBatch` → IPC → wasm memory → IPC → `RecordBatch` fidelity, including ephemeral entities, list types, struct types.
   - `crates/uni-plugin-wasm/tests/linker_capability.rs` — host imports absent without capability; present with.
   - `crates/uni-plugin-wasm/tests/epoch_interruption.rs` — runaway plugin trapped on deadline.
   - `crates/uni-plugin-wasm/tests/fuel_metering.rs` — out-of-fuel trap.
   - `crates/uni-plugin-wasm/tests/pool_cold_start.rs` — pre-warmed pool achieves p99 < 50 μs per call; cold pool shows expected 10–100 ms first-call latency.
   - `crates/uni-plugin-wasm/tests/e2e_geo_haversine.rs` — load `example-wasm-geo.wasm`, register, call from Cypher, verify result.

4. `Uni::load_wasm(bytes, grants) -> Result<PluginHandle, PluginError>` public API.

5. Documentation: `crates/uni-plugin-wasm/README.md` walks through building a WASM plugin from Rust, including `cargo component build` invocation.

**Files changed:**

- `crates/uni-plugin-wasm/**` (new, ~3000 lines including WIT, bindings, host impls).
- `crates/example-wasm-geo/**` (new, ~150 lines + Cargo config for wasm32-wasip2).
- `crates/uni/src/lib.rs` — `Uni::load_wasm` public API (delegates to `uni-plugin-wasm`).
- `Cargo.toml` (root) — new workspace members; `[workspace.dependencies]` for wasmtime.

**Acceptance criteria:**

1. `cargo build --target wasm32-wasip2 --release -p example-wasm-geo` produces a `.wasm` artifact in `target/wasm32-wasip2/release/`.
2. `cargo nextest run -p uni-plugin-wasm` — all tests pass, including the e2e geo test.
3. `cargo bench -p uni-bench --bench wasm_scalar_fn` — pre-warmed pool p99 < 50 μs; cold-pool first call 10–100 ms (as expected).
4. A WASM plugin without `Capability::Filesystem` fails to instantiate when it imports `host-fs.read` — linker rejects at instantiation time, not runtime.
5. The `example-wasm-geo` plugin compiles to a `.wasm` file < 500 KB.

**Risks:**

- *Risk:* WASI Preview 2 toolchain changes break the WIT bindings. *Mitigation:* Pin `wasmtime`, `wit-bindgen`, `wasm-tools` versions in `Cargo.toml` and `Cargo.lock`. Bindings are committed to the repo (not generated on each build).
- *Risk:* Arrow IPC fidelity issues for less-common types (timestamp with timezone, decimals, large strings). *Mitigation:* The `arrow_ipc_roundtrip.rs` test exhaustively covers every Arrow primitive + list/struct + Cypher value types.
- *Risk:* Pool sizing under heavy parallel load causes contention on the `ArrayQueue`. *Mitigation:* The pool's metrics expose hit rate and contention; a tuning guide ships in the README.

**Parallel work unlocked:** M7 (Rhai loader), M9 (meta-plugin), M10 (hot reload), M11 (capabilities + observability + scheduling), M12 (CLI + OCI + Python bindings + conformance).

---

### M7 — Rhai loader: `uni-plugin-rhai`

**Status: ✅ substantive.** Phases 1–11 landed in a single session. 37 unit + integration tests pass in `uni-plugin-rhai`; Uni-level e2e (`m7_rhai_load_e2e`) and cross-loader parity (`m7_rhai_cross_loader_parity`) both green; example CLI install round-trip verified (`uni plugin install ./geo.rhai` → `ok: loaded plugin 'ai.dragonscale.geo' v0.3.1`).

**Context.** This milestone replaces the reverted Lua-in-WASM attempt (commits `2213d3213` → `aa6e05656` → `586b42139`, reverted in `5ef0ec7fe`). The lessons learned there directly shaped the Rhai choice:

- **Pure Rust eliminates the C-toolchain surface.** mlua / piccolo both have stdlib gaps (piccolo) or C dep (mlua + system Lua). Rhai builds anywhere the rest of uni-db builds — Tier 1 / 2 Rust targets, musl, cross-compile, no `cc-rs`.
- **Sandboxing by language design, not by wrapper.** Rhai has no built-in I/O; registering a function is opt-in, absence is the default. The wasm wrapper added nothing the host couldn't already do; Rhai's design removes the need.
- **Resource limits are first-class** on `Engine`: `set_max_operations`, `set_max_call_levels`, `set_max_string_size`, `set_max_array_size`, `set_max_map_size`. No wasmtime epoch hooks, no instruction-count shim.
- **Active upstream.** `rhai` 1.x is on a regular release cadence with an MSRV policy.

**What shipped:**

1. **Crate `crates/uni-plugin-rhai/`** (gated by `rhai-runtime` feature, default-on; 14 source files, ~1700 lines):
   - `lib.rs` — re-exports.
   - `error.rs` — `RhaiError` enum (InvalidPlugin, ManifestInvalid, ParseFailed, CapabilityDenied, RuntimeError, Conversion, ResourceLimit, NotYetImplemented, Internal).
   - `host_fns.rs` — `RhaiHostFnRegistry` + `RhaiHostFnSpec { name, required_capability, docs, register: Arc<dyn Fn(&mut Engine)> }`. Closure-carrying spec lets the engine factory invoke registration only when the cap is granted (Engine-import absence).
   - `engine.rs` — `build_engine(effective_caps, host_fns) -> rhai::Engine`. Default Rhai stdlib (math/array/map/string). `disable_symbol("eval")`. `set_module_resolver(DummyModuleResolver::new())` (deny-all). `set_max_call_levels(DEFAULT_MAX_CALL_LEVELS = 64)`. `FuelPerCall(N)` → `set_max_operations(N)`. `MemoryBytes(N)` → per-collection caps of `N/4`. Walks `host_fns` and invokes each `register` closure only when `required_capability` is in the effective set.
   - `manifest.rs` — `compile(engine, script)` + `parse_manifest(engine, ast)`. Calls the script's `uni_manifest()` fn, expects a Rhai `Map` with `id` / `version` / `determinism` / `scalar_fns` / `aggregate_fns` / `procedures` arrays, returns a typed `RhaiManifest`.
   - `wire_translate.rs` — `"float"` → `DataType::Float64`, `"int"` → `Int64`, `"string"` → `Utf8`, `"bool"` → `Boolean`, `"null"` → `Null`. `determinism_to_volatility` maps `"pure"` → `Immutable`, `"session"` → `Stable`, else `Volatile`.
   - `dynamic_bridge.rs` — `column_row_to_dynamic` + `scalar_to_dynamic` + `OutBuilder` (Bool/Int/Float/Str). `i64` round-trips via explicit `Dynamic::from::<i64>` (no float coercion).
   - `runtime.rs` — `RhaiPluginRuntime { plugin_id, engine: Arc<Engine>, ast: Arc<AST> }`. `Send + Sync` via Rhai's `sync` feature — adapters share an `Arc<RhaiPluginRuntime>` across DataFusion partitions without locking.
   - `adapter.rs` — `RhaiScalarFn` implementing `ScalarPluginFn`. Row-mode dispatch via `Engine::call_fn(scope, ast, name, dyn_args)` with a fresh `Scope` per call. Vectorized branch (`new_vectorized` constructor) passes registered `Float64Column` / `Int64Column` / `Utf8Column` userdata args, expects a `MutableFloat64Column` return.
   - `adapter_aggregate.rs` — `RhaiAggregateFn` + `RhaiAccumulator` dispatching to four Rhai callables (`${name}_init`, `${name}_accumulate`, `${name}_merge`, `${name}_finalize`). Per-partition state serialized as `LargeBinary(serde_json::to_vec(&state))`; `merge_batch` deserializes peer states from a `LargeBinary` column. `build_agg_signature` honours `"map"` returns by mapping to `LargeUtf8`.
   - `adapter_procedure.rs` — `RhaiProcedure` implementing `ProcedurePlugin`. Expects the Rhai fn to return `Array<Map>` (rows). Numeric coercion (`Int` ↔ `Float`) applied per yield field type. Wraps in `RecordBatchStreamAdapter` → `SendableRecordBatchStream`.
   - `loader.rs` — `RhaiLoader::load(script, registrar, registrar_caps) -> LoadOutcome`. Three-phase shape mirroring `ExtismLoader::load`: build engine with host grants → compile + parse manifest → register entries on the `PluginRegistrar`. Entries are only registered if their declared cap (ScalarFn/AggregateFn/Procedure) is in the effective set; denied caps surface in `LoadOutcome.denied_capabilities`.
   - `columns.rs` — `Float64Column`, `Int64Column`, `Utf8Column` immutable wrappers and `MutableFloat64Column` (output builder). All `Clone + Send + Sync + 'static`. Registered via `register_column_types(engine)` with indexer-get / -set methods reading/writing the underlying Arrow buffers. `uni_float_column(n)` host fn allocates a fresh output column.
   - `host_fn_impls/` directory — `fs.rs` (`uni.fs.read` / `uni.fs.write` gated by `Capability::Filesystem`), `net.rs` (`uni.http.get` / `uni.http.post` gated by `Network`; v1 returns `NotYetImplemented` runtime errors — symbol presence proves the gate, the HTTP client wiring is M7-followup), `kms.rs` (sign/verify stubs gated by `Kms`), `secret.rs` (acquire stub gated by `Secret`). `register_default_host_fns(loader)` wires all four.

2. **Host integration** (`crates/uni/`):
   - `Uni::load_rhai_plugin(loader, script, registrar_caps) -> LoadOutcome` in `crates/uni/src/api/mod.rs:1394-1450`. Three-phase: `PluginRegistrar::new(placeholder, caps, registry)` → `loader.load` → `registrar.commit_to_registry`. Wraps `RhaiError` variants into `UniError::InvalidArgument` (plugin-side) or `UniError::Internal` (host-side).
   - `crates/uni/Cargo.toml`: `uni-plugin-rhai = { workspace = true, optional = true }`; feature `rhai-plugins = ["dep:uni-plugin-rhai"]`; **`rhai-plugins` is in default features** (Rhai is pure-Rust with no C toolchain or wasmtime cost, so the ~1 MB binary impact is acceptable as default-on — a deliberate divergence from the WASM loaders).

3. **`uni-plugin` ABI addition** — `PluginError::RhaiParse(String)` variant in `crates/uni-plugin/src/errors.rs`.

4. **Python binding** (`bindings/uni-db/`):
   - `Uni.load_rhai_plugin(script, grants=None)` PyO3 method on the `Database` (pyclass name `Uni`) at `bindings/uni-db/src/sync_api.rs`. Accepts a string list of grant variant names (`ScalarFn`, `AggregateFn`, `Procedure`, `Filesystem`, `Network`, `HostQuery`, `Kms`, `Secret`) and returns a dict carrying `plugin_id`, `version`, `scalars_registered`, `aggregates_registered`, `procedures_registered`, `denied_capabilities`.

5. **CLI** (`crates/uni-cli/`):
   - `uni plugin install <path>` subcommand. Dispatches by file extension: `.rhai` → `Uni::load_rhai_plugin`; `.wasm` / `oci://` / `extism://` / `https://` → "not yet supported in M12" with an actionable error.
   - `--grants` flag accepts comma-separated capability variant names; defaults to `ScalarFn,AggregateFn,Procedure`.
   - Behind default-on `rhai-plugins-cli` feature.

6. **Example plugin** (`examples/example-rhai-geo/`):
   - Own-workspace bin crate. Embeds `geo.rhai` via `include_str!`, loads into an in-memory Uni, invokes `haversine` against a 3-city test matrix, prints results.

7. **Tests** (37 in `uni-plugin-rhai` + 2 Uni e2e + 1 cross-loader parity + 4 Python = 44 new tests):
   - **Crate-level** (`crates/uni-plugin-rhai/`):
     - `tests/load_e2e.rs` — minimal scalar fn round-trip; `haversine_geo_plugin` matches expected NYC↔SF distance.
     - `tests/sandbox.rs` — `eval` rejected at parse; `import` denied; ungranted `uni_fs_read` not resolvable; granted `uni_fs_read` reads from a tempfile.
     - `tests/resource_limits.rs` — `FuelPerCall(100_000)` trips on 1M-iter loop with `FnError { code: 0x711 }`; depth-200 recursion trips against `DEFAULT_MAX_CALL_LEVELS=64`.
     - `tests/vectorized.rs` — vectorized `score_v(xs, ys)` round-trips; vectorized vs row-mode haversine agree within 1 ULP.
     - `src/engine.rs::tests` — eval disabled, import denied, ungranted/granted host fn presence, fuel limit trips.
     - `src/manifest.rs::tests` — minimal manifest parse, missing `id` rejected, aggregate+procedure entries parse.
     - `src/adapter_aggregate.rs::tests` — stats aggregate (`init/accumulate/finalize`) round-trips; `LargeBinary` state serializes and merges across two partitions.
     - `src/adapter_procedure.rs::tests` — procedure emits 3 rows as `RecordBatch` with declared yield schema.
     - `src/dynamic_bridge.rs::tests` — `Int64` round-trip without float coercion (`i64::MAX`, `i64::MIN`); null row → `Dynamic::UNIT`; string round-trip; `OutBuilder` handles nulls.
     - `src/wire_translate.rs::tests` — type-name mapping; unknown type rejected; determinism → volatility.
     - `src/host_fns.rs::tests` — registry CRUD; always-available specs (no required cap).
     - `src/loader.rs::tests` — minimal scalar plugin loads + registers; declared-but-not-granted caps appear in `denied_capabilities`; malformed source returns `ParseFailed`.
     - `src/columns.rs::tests` — `Float64Column` indexer + null cells; `MutableFloat64Column` set/freeze round-trips.
   - **Uni-level** (`crates/uni/tests/`):
     - `m7_rhai_load_e2e.rs` — `Uni::load_rhai_plugin` registers a scalar fn end-to-end; malformed Rhai source surfaces a typed error.
     - `m7_rhai_cross_loader_parity.rs` — Rhai-loaded `haversine` matches native libm within 4 ULP on the canonical 5-row matrix (antipodal row 3 uses a 1e-3 absolute tolerance — `asin(1.0)` has a sharp gradient where Rhai's libm and the Rust libm diverge by ~1e-7).
   - **Python** (`bindings/uni-db/tests/test_rhai_plugin.py`): outcome dict shape; bad grant name → `ValueError`; explicit grants list; malformed script → exception.

**Files changed:**

- `crates/uni-plugin-rhai/**` (new — 14 source files + 4 integration test files, ~1700 + ~600 lines of tests).
- `Cargo.toml` (workspace) — added `rhai = "1"` to `[workspace.dependencies]`; `uni-plugin-rhai` workspace dep entry; `crates/uni-plugin-rhai` in workspace members + default-members.
- `crates/uni-plugin/src/errors.rs` — `PluginError::RhaiParse(String)` variant.
- `crates/uni/Cargo.toml` — optional `uni-plugin-rhai` dep; `rhai-plugins` feature (default-on).
- `crates/uni/src/api/mod.rs` — `Uni::load_rhai_plugin` impl (~50 lines).
- `crates/uni/tests/m7_rhai_load_e2e.rs` + `m7_rhai_cross_loader_parity.rs` (new).
- `crates/uni-cli/Cargo.toml` — optional `uni-plugin-rhai` dep; `rhai-plugins-cli` feature (default-on).
- `crates/uni-cli/src/main.rs` — `Plugin` subcommand + `PluginCmd::Install` + `install_plugin` dispatch + `parse_grants` helper.
- `bindings/uni-db/Cargo.toml` — `uni-plugin` + `uni-plugin-rhai` deps.
- `bindings/uni-db/src/sync_api.rs` — `Uni.load_rhai_plugin` PyO3 method.
- `bindings/uni-db/tests/test_rhai_plugin.py` (new).
- `examples/example-rhai-geo/{Cargo.toml, src/main.rs, geo.rhai}` (new).

**Acceptance criteria (mechanical):**

1. ✅ `cargo nextest run -p uni-plugin-rhai` — 37/37 tests pass.
2. ✅ `cargo nextest run -p uni-db --test m7_rhai_load_e2e --test m7_rhai_cross_loader_parity` — 3/3 tests pass.
3. ✅ The default Rhai engine fails `eval("1+1")` at parse with `ParseFailed("reserved keyword 'eval' is disabled (line N, position M)")`; ungranted `uni_fs_read` fails with `ErrorFunctionNotFound`.
4. ✅ `FuelPerCall(100_000)` trips on a long loop with `FnError { code: 0x711 }`.
5. ✅ Rust / Rhai cross-loader parity for `geo.haversine` passes at ≤ 4 ULP tolerance (per-row check; antipodal singularity uses 1e-3 absolute tolerance).
6. ✅ `uni plugin install ./examples/example-rhai-geo/geo.rhai --path /tmp/x` round-trips.
7. ⏳ Performance bench (≥ 100k rows/sec row mode, ≥ 1M rows/sec vectorized) — pending M12 perf-regression harness.

**Genuinely deferred (M7-followup or later milestones):**

- Wall-clock deadline driver. `Capability::WallClockMillisPerCall(N)` is a defined variant; the v1 implementation does not wire a host-side deadline via `Engine::on_progress`. The operations cap (`FuelPerCall`) is the v1 termination path. M7-followup adds the deadline driver.
- Full memory accounting via `Engine::on_var` / `on_def_var`. v1 uses per-collection caps (`set_max_string_size` / `set_max_array_size` / `set_max_map_size`) derived as `MemoryBytes / 4`. Total-allocation tracking is M10's broader memory-limit work.
- Real network HTTP client wiring (`uni_http_get` / `uni_http_post`). v1 registers the symbols under the capability gate but returns `NotYetImplemented` from the body — proves the gate; the HTTP client + URL allow-list validation is M7-followup.
- Real KMS / SecretStore wiring (`uni.kms.sign` / `uni.secret.acquire`). Same shape as HTTP — symbol presence proves the gate, real impl is M7-followup.
- `uni.query` host-fn for re-entrant Cypher. Cap variant (`HostQuery`) is recognised by the Python binding's grant parser; the actual `uni_query` host-fn registration is M7-followup.
- `uni::map(col, |x| ...)` vectorized helper. v1 ships explicit `for i in 0..n { out[i] = ... }` form; the closure-hoisting variant is M7-followup.
- Hot-reload integration with `EpochFencedReload`. Rhai entries are stateless across calls (Engine + AST behind `Arc`s), so the standard drain-and-swap pattern works out of the box; explicit reload integration test is M10's per-kind reload work.
- Multi-file `.rhai` plugin pack (`load_rhai_pack`). v1 ships single-script loading; pack support is M12.
- OCI artifact distribution for `.rhai` (`oci://` and `extism://hub/` schemes). M12 work.
- `proptest` for `i64` boundary coverage in `dynamic_bridge`. v1 uses fixed `i64::MAX` / `MIN` test inputs; randomized fuzzing is a quality follow-up.

**Risks (retrospective):**

- *Rhai's `register_indexer_get` requires `&mut self`* — discovered mid-Phase 7. Fix: change `Float64Column::len/get` etc. signatures from `&self` to `&mut self` to satisfy Rhai's `RhaiNativeFunc<(Mut<T>, IDX), 2, ...>` bound. Cheap retroactively.
- *Rhai's `atan2` is not in `BasicMathPackage`* — discovered when the original haversine test used `a.sqrt().atan2(...)`. Fix: rewrite using `2 * asin(sqrt(a))`, mathematically equivalent. Documented in the geo example.
- *Registrar caps gate registration; manifest-derived "declared" caps gate effective surface set; host-fn registration uses the granted-caps set not the manifest-effective set* — this distinction was non-obvious during Phase 5. The implementation now passes `registrar_caps` directly to `build_engine` for host-fn gating, and uses `effective` (manifest ∩ granted) for surface-registration gating. Two different "effective" sets for two different enforcement points; documented in `loader.rs:101-108`.

**Parallel work unlocked:** None directly — M7 is a fan-out leaf. Its tests inform M12's perf regression suite when that lands.

---

### M8 — PyO3 loader: `uni-plugin-pyo3`

**Status (2026-05-23): ▶ partial.** `crates/uni-plugin-pyo3/src/lib.rs` exists with `PyPluginLoader` registration surface + lookup/register/unregister (commit `8c68abfb`): `PyScalarFnRecord` (qname/args/returns/vectorized/determinism) backed by `parking_lot::RwLock<HashMap>`. No PyO3 Arrow C Data Interface bridge, no Python decorator API, no GIL-strategy implementations yet. **6 tests pass.**

**Goal:** Live Python UDFs via the existing PyO3 bindings. PyArrow zero-copy for vectorized mode. GIL contention documented; session-scope default. Python decorator API on the user side.

**Note:** Can start as early as after M2.

**Deliverables:**

1. New crate `crates/uni-plugin-pyo3/` (gated by Cargo feature `pyo3`):
   - `lib.rs` — `PyScalarFn`, `PyAggregateFn`, `PyProcedurePlugin` wrappers implementing the corresponding capability traits.
   - `arrow_bridge.rs` — Arrow C Data Interface zero-copy to/from PyArrow.
   - `gil_strategy.rs` — `Vectorized` (one GIL acquisition per batch) vs `RowByRow` (one per row).

2. `bindings/uni-db/src/plugins.rs` (PyO3 side):
   - `@db.scalar_fn` decorator.
   - `@db.aggregate_fn` decorator.
   - `@db.procedure` decorator.
   - `db.load_wasm(...)` (WASM bytes/path).
   - `db.load_rhai_plugin(...)` (Rhai source / path) — wires M7.

3. Tests:
   - `bindings/uni-db/tests/test_pyo3_plugins.py` — register a Python scalar fn (vectorized + row mode), call from Cypher, verify result.
   - `crates/uni-plugin-pyo3/tests/gil_contention.rs` — measure throughput of a Python UDF under parallel query execution; document the GIL ceiling.

**Files changed:**

- `crates/uni-plugin-pyo3/**` (new, ~800 lines).
- `bindings/uni-db/src/plugins.rs` (new).
- `bindings/uni-db/src/lib.rs` (re-export).
- Python tests.

**Acceptance criteria:**

1. `pytest bindings/uni-db/tests/test_pyo3_plugins.py` — passes.
2. Vectorized Python UDF achieves > 5M rows/sec on a simple operation (PyArrow zero-copy verified).
3. Row-by-row Python UDF documented as ~100k rows/sec ceiling (PyO3 boundary cost per row).
4. Decorator surface matches the proposal §5.4 examples.

**Risks:**

- *Risk:* PyArrow / Arrow C Data Interface version skew. *Mitigation:* Pin PyArrow minimum version in `pyproject.toml`; CI runs against multiple PyArrow versions.

**Parallel work unlocked:** Validated end-to-end Python developer experience needed for M12.

---

### M9 — Meta-plugin: `uni-plugin-custom` (`apoc.custom` analogue)

**Status (2026-05-26): ✅ complete.** All four `declare*` procedures (`declareFunction`, `declareProcedure`, `declareAggregate`, `declareTrigger`) plus `listDeclared` / `dropDeclared` are registered as Cypher-callable procedures via the `custom` plugin id. **`declareFunction`** parses the Cypher expression body at declare time and registers a synthetic `ScalarPluginFn` (`DeclaredScalarFn`) under a per-namespace plugin id, so `CALL uni.plugin.declareFunction('mycorp.fullName', '$first + " " + $last', 'string', '["first","last"]')` followed by `RETURN mycorp.fullName('Ada', 'Lovelace')` returns `'Ada Lovelace'` (proven by `crates/uni/tests/plugin_custom_declare.rs`). **`declareAggregate`** is fully end-to-end as of the M9-followup commit: it parses three Cypher expression bodies (`init` / `update` / `finalize`), wraps them in a `DeclaredAggregateFn` (impls `AggregatePluginFn` over the `eval_expr` interpreter), and registers a `SyntheticAggregatePlugin` via `install_aggregate_into_registry`; Cypher dispatch rides through `crates/uni-query/src/query/df_udaf_plugin.rs::PluginAggregateUdaf` (DataFusion `AggregateUDFImpl` adapter) + a plugin-aggregate hint set in `uni-cypher::plugin_aggregates` so the AST-level `is_aggregate` check routes declared aggregates through `translate_aggregates` instead of scalar UDF resolution. `MATCH (n:Item) RETURN mycorp.sumSquares(n.value) AS s` returns the correct sum of squares (`crates/uni/tests/plugin_custom_declare.rs::declare_aggregate_round_trip_via_cypher`). The body evaluator handles literals, parameter substitution, arithmetic/boolean/string-concat binary ops, unary negation, comparisons, lists/maps, a handful of scalar fns (toString/upper/lower/trim/length/abs), CASE WHEN, IS NULL, and `LargeBinary` (CypherValue-encoded) property decoding for aggregate input columns — sufficient for the M9 happy path; the lane to swap in `cypher_expr_to_df` for the full Cypher surface is open. **Dependencies** are now declarable from Cypher: every declare* signature accepts a trailing `deps_json` arg (default `"[]"`), populated into `DeclaredPlugin.dependencies` and validated against the `DeclaredPluginStore` cycle/missing-dep checks. **Persistence** rides through a `Persistence` trait with two impls: `NullPersistence` (in-memory; the default for `Uni::build` until M11 lands write-enabled `ProcedureHost::execute_inner_query`) and `JsonFilePersistence` (sidecar JSON matching proposal §9.7's `_DeclaredPlugin` schema field-for-field). **Reactivation** — `CustomPlugin::new` calls `persistence.load_all()` and `reactivate_into_registry` re-installs every function **and aggregate** into the registry (procedures and triggers stay record-only pending M11); restart fidelity proven by `crates/uni-plugin-custom/tests/declare_persistence.rs`. Shadow detection downgrades declarations to `active=false` when a native qname collides — forward direction (`crates/uni-plugin-custom/tests/shadow_native.rs`). `declared_by` snapshots the declaring principal id (`crates/uni-plugin-custom/tests/declared_by.rs`); the M9 status block previously described this as "capability inheritance via declarer id snapshot" — note that *enforced* capability denial requires net-new `Principal`-side caps infrastructure outside M9 scope (`Principal` today carries only `id` + `groups`). Dependency cycles, cascade drops, and the id snapshot are all in place. **34 tests pass** across the `uni-plugin-custom` crate (eval interpreter, scalar dispatch, aggregate accumulator + install, store, persistence + restart, shadow_native, declared_by) plus 4 in the `uni-db` integration suite (Cypher end-to-end including aggregate + dependencies). **Routed to M11** (both gated on write-enabled `ProcedureHost::execute_inner_query`): (a) the cutover from `JsonFilePersistence` → `_DeclaredPlugin` Cypher system label, a drop-in `impl Persistence for SystemLabelPersistence` once the host primitive lands; and (b) `declareProcedure` body execution — the procedure is registered and persisted today, but its Cypher body cannot be invoked until the inner-query host supports parameter binding + write mode. `declareTrigger` body execution rides the same write-enabled host and is bundled into the same M11 deliverable. Reverse-shadow direction (declared first, then native plugin loaded later) remains TODO — requires a `mark_declared_inactive` hook on `PluginRegistry`, outside M9 scope.

**Goal:** `uni.plugin.declareFunction`, `declareProcedure`, `declareAggregate`, `declareTrigger` from Cypher. Definitions persisted in `_DeclaredPlugin` system label. Re-registration on startup. Drops with cascade protection.

**Note:** Depends on M4 (procedures) and M5e (hooks) for the registration machinery.

**Deliverables:**

1. New crate `crates/uni-plugin-custom/` with:
   - `lib.rs` — `CustomPlugin::new()` returning a `Plugin` implementation that registers the `uni.plugin.*` meta-procedures.
   - `declare_function.rs`, `declare_procedure.rs`, `declare_aggregate.rs`, `declare_trigger.rs` — implementations.
   - `persistence.rs` — `_DeclaredPlugin` system label CRUD via `host.query`.
   - `reactivation.rs` — startup hook that re-registers every declared plugin.
   - `dependency.rs` — dependency graph + cycle detection + cascade drops.

2. Tests:
   - `crates/uni-plugin-custom/tests/declare_function_e2e.rs` — declare from Cypher, call back, verify result. Restart simulated by dropping `Uni`, re-opening with the same store, verify the declared function is re-registered and still callable.
   - `dependency_cycle.rs` — declaring `a` depending on `b` depending on `a` is rejected at declaration time.
   - `cascade_drop.rs` — `drop_declared('a', {cascade: true})` drops all dependents.
   - `shadow_native.rs` — a native plugin shadowing a declared qname marks the declared one inactive with a warning.

3. `_DeclaredPlugin` label registered as a system label (created on `uni-plugin-custom`'s `init()`).

**Files changed:**

- `crates/uni-plugin-custom/**` (new, ~1500 lines).
- `crates/uni/src/lib.rs` — `Uni::new` registers `Arc::new(CustomPlugin::new())` after `BuiltinPlugin`.

**Acceptance criteria:**

1. `CALL uni.plugin.declareFunction('mycorp.fullName', '$first + " " + $last', 'string', [{name:'first', type:'string'}, {name:'last', type:'string'}])` succeeds; immediately `RETURN mycorp.fullName('Ada', 'Lovelace')` returns `'Ada Lovelace'`.
2. After restart, the same query returns the same result (re-registration verified).
3. `cargo nextest run -p uni-plugin-custom` — all tests pass.

**Risks:**

- *Risk:* Declared procedures with `Capability::ProcedureWrites` can issue writes. The declaration capability check needs to verify the declaring session held those caps. *Mitigation:* `declare_procedure` checks `principal.has_capability(...)` against the declared mode's required caps; explicit test in `capability_inheritance.rs`.

**Parallel work unlocked:** None (it's a leaf).

---

### M10 — Hot reload + multi-version ABI

**Status (2026-05-27): ✅ complete.** All five deliverables shipped. `PluginLifecycle` state machine + `EpochFencedReload` driver in `crates/uni-plugin/src/lifecycle.rs` (440 lines): `begin_drain` (Active→Draining), `wait_for_drain(threshold, poll, max_wait)` polls `Arc::strong_count` with timeout, `finalize` (idempotent Draining→Removed). `DrainError` covers `NotActive` / `Timeout` / `UnexpectedTransition`. Per-surface `arc-swap` registries from M1 in place. Per-kind reload discipline (Storage open-on-new, Index persist→open round-trip, CRDT/LogicalType schema-compat check, BG/CDC checkpoint-on-old + start-on-new) all wired through `Uni::reload`. `MultiVersionLinker` (`crates/uni-plugin-wasm/src/multi_version.rs`, 199 lines) caches per-`(major, caps)` Linkers; `^1` and `^2` AbiRange plugins coexist. `uni-crdt` host consultation: `Crdt::merge_via_registry` (`crates/uni-crdt/src/registry_dispatch.rs`) dispatches through registered `CrdtKindProvider`s for all 5 built-in CRDTs via msgpack round-trip. **Tests**: 11 in `integration_admin` (`hot_reload_consistency` / `multi_version_abi` / `reload_crdt` / `reload_index_kind` / `reload_storage_backend`) + 4 in `uni-crdt::registry_dispatch` = 19 acceptance tests green; broader `uni-plugin` + `uni-plugin-wasm` + `uni-plugin-wasm-rt` suite is 132 green.

**Goal:** `Uni::reload(handle, new_source)` works across all plugin kinds with per-kind discipline (proposal §11.2.1). Per-major `Linker` infrastructure so two plugins built against different ABI majors coexist.

**Deliverables:**

1. `crates/uni-plugin/src/registry.rs` — `PluginRegistry` already uses `arc-swap` from M1. M10 adds the **draining** state-machine: `Active → Draining → Removed` with Arc-refcount waiting.

2. Per-kind reload handlers in `crates/uni-plugin/src/lifecycle.rs`:
   - Stateless kinds: trivial atomic swap.
   - `StorageBackend`: new `open()` constructs a fresh `Storage`; old continues for in-flight queries.
   - `IndexHandle`: `persist()` on old, `open(persisted_bytes)` on new (preserves built indexes).
   - `BackgroundJobProvider`: in-flight run completes on old; next tick uses new.
   - `CdcOutputProvider`: `checkpoint()` on old, `start(cdc_lsn)` on new.
   - `CrdtKindProvider`: schema-compatibility check — old's `persist()` bytes must be readable by new's `from_persisted()`. Mismatch is a hard reload error.
   - `LogicalTypeProvider`: Arrow extension name + version unchanged; otherwise hard reload error.

   **M10-owned `uni-crdt` host consultation of the `CrdtKindProvider` registry** (formerly routed from M5d — now a first-class M10 deliverable, since it depends on the schema-compat reload check above): the per-kind `CrdtKindProvider` reload handler is the **gating prerequisite for `uni-crdt` mutation paths consulting the `CrdtKindProvider` registry**. Today 5 CRDTs (LWW, OR-Set, G-Counter, MV-Register, RGA) are registered through `uni-plugin-builtin/src/crdts.rs` but `crates/uni-crdt/` still dispatches on its native enum (`grep CrdtKindProvider crates/uni-crdt` → 0 hits). Once the schema-compat reload check above lands, refactor `uni-crdt::merge_*` paths to look up the provider by `CrdtKind` and dispatch through `CrdtState::apply` / `merge`. Without the reload discipline, a hot-swap of a CRDT plugin would tear in-flight merge state. Acceptance: `crates/uni-crdt/tests/registry_dispatch.rs` exercises mutation → registry lookup → `apply` for at least 2 of the 5 built-in CRDTs.

3. `crates/uni-plugin-wasm/src/multi_version.rs` — per-major `Linker` cache. A plugin's `manifest.abi: AbiRange` selects the matching Linker.

4. `Uni::reload`, `Uni::remove_plugin`, `Uni::plugins`, `Uni::plugin(id)` public APIs in `crates/uni/src/lib.rs`.

5. Tests:
   - `crates/uni/tests/hot_reload_consistency.rs` — long-running query in flight when `reload()` runs; query completes on old version; next query sees new.
   - `crates/uni/tests/multi_version_abi.rs` — two plugins built against ABI majors 1 and 2 (via test fixtures) coexist in one Uni instance.
   - `crates/uni/tests/reload_storage_backend.rs`, `reload_index_kind.rs`, `reload_crdt.rs` — per-kind reload behavior.

**Files changed:**

- `crates/uni-plugin/src/registry.rs` (extend with draining).
- `crates/uni-plugin/src/lifecycle.rs` (new).
- `crates/uni-plugin-wasm/src/multi_version.rs` (new).
- `crates/uni/src/lib.rs` (`Uni::reload` etc.).
- New tests.

**Acceptance criteria:**

1. `cargo nextest run -p uni --tests hot_reload` — all reload tests pass.
2. A long-running query (10s wall-clock) is unaffected by a `reload()` issued mid-flight.
3. Multi-version ABI: an `abi: ^1` plugin and an `abi: ^2` plugin both execute correctly in the same query (a query that calls one then the other).

**Risks:**

- *Risk:* Detecting that the old plugin's refcount has reached 1 races with new references being captured. *Mitigation:* The draining state-machine takes the write-lock on the registry during the transition; new captures block until the swap is atomic.

**Parallel work unlocked:** M11 (capabilities — depends on M6 but not M10), M12 (CLI — needs M10's `Uni::reload` for `uni plugin reload`).

---

### M11 — Capabilities + security + observability + scheduling + background jobs

**Status (2026-05-27): ✅ complete — Phases A+B+C + the six FU follow-ups (FU-1 through FU-6) all landed in a single closure pass.**

#### What shipped

**Phase A — write-enabled inner-query host + M9 cutover.**

- **A.1 — `execute_inner_query` extended.** Signature: `pub async fn execute_inner_query(&self, cypher: &str, params: &HashMap<String, Value>, mode: ProcedureMode)` in `crates/uni-query/src/query/executor/procedure_host.rs`. `Read` keeps `Executor::new()`; `Write` / `Schema` / `Dbms` use `Executor::new_with_writer` and require a writer attached via the new `QueryProcedureHost::with_writer(writer)` builder. Four callers (`algo.rs:216,223`; `graph.rs:258,265`) updated to pass `Read` + empty params.
- **A.2 — `SystemLabelPersistence`.** New `crates/uni/src/persistence.rs` exposes a `Persistence` backend that writes to `<data_path>/_system/declared_plugins.json` (atomic write-then-rename; same mechanism as `JsonFilePersistence`). The `DeclaredPlugin` serde shape matches proposal §9.7 verbatim so the eventual cutover to Cypher `MERGE (:_DeclaredPlugin {...})` through the write-enabled `execute_inner_query` is a backend swap, not a schema migration. `persistence_for_data_path()` picks `SystemLabelPersistence` for local-disk URIs, `NullPersistence` for remote / object-store URIs. Wired into `register_builtin_plugins` (which now takes `data_path: Option<&Path>`) replacing the previous `NullPersistence` default.
- **A.3 — `SyntheticProcedurePlugin` + declare-extension.** `crates/uni/src/synthetic_procedure.rs::SyntheticProcedurePlugin` reads `(qname, body, mode, param_names)` from a `DeclaredPlugin`, downcasts the `ProcedureContext::host` to `QueryProcedureHost` in `invoke()`, bridges sync→async via `tokio::task::block_in_place` + `Handle::current().block_on(...)`, and runs the body through the write-enabled `execute_inner_query`. `CypherProcedureSynthesizer` implements a new `ProcedureBodySynthesizer` trait added to `uni-plugin-custom`. The `declare_kind_procedure!` macro gained `new_with_synthesis`; both declare-time and `reactivate_into_registry` paths install the synthesized procedure via `procedures::install_synthesized_procedure` (`NativeShadow` handled by marking the record inactive). The host wires `with_procedure_synthesizer(Arc::new(CypherProcedureSynthesizer::new()))` in `register_builtin_plugins`.
- **A.4 — Phase A tests.** 5 `SystemLabelPersistence` unit tests in `crates/uni/src/persistence.rs::tests` (including `save_then_close_reopen_survives`); 5 `SyntheticProcedurePlugin` construction tests in `crates/uni/src/synthetic_procedure.rs::tests`; 2 `ProcedureBodySynthesizer` integration tests in `crates/uni-plugin-custom/src/lib.rs::tests` (StubSynthesizer-driven) verifying that `reactivate_into_registry` (a) calls the synthesizer for procedure-kind records and (b) preserves pre-M11 behavior when no synthesizer is attached.

**Phase B — scheduler + persistence + built-in jobs + `uni.periodic.*`.**

- **B.1 — `Schedule` enum + time-aware `tick_at`.** The existing `Schedule { Once(SystemTime) / Periodic(Duration) / Cron(SmolStr) / Manual }` in `crates/uni-plugin/src/traits/background.rs` gained `next_after(from: SystemTime) -> Option<SystemTime>` (cron parsing via `cron = "0.12"` + `chrono` 0.4 conversions; periodic = `from + every`; once = `Some(at) if at >= from`; manual = `Some(from)`). `SchedulerJobRecord` gained `schedule: Schedule` + `next_fire_at: Option<SystemTime>` fields. `Scheduler::add_scheduled_job(id, schedule)` + `Scheduler::tick_at(now)` shipped; legacy `add_job` / `tick` preserved (delegate to `Manual` + `SystemTime::now()`). `mark_finished` recomputes `next_fire_at` for Periodic / Cron and bounces the job back to `Pending` so it re-fires (a previous-pass bug had failed Periodic jobs stuck in `FailedRetrying`).
- **B.2 — scheduler `Persistence` trait + `MemoryPersistence` default.** `SchedulerPersistence` trait (`record_started` / `record_finished` / `cancel` / `load_all`) in `crates/uni-plugin/src/scheduler.rs`. `MemoryPersistence` no-op default. `SchedulerControl` trait (separate from persistence) lets cross-crate consumers like the `uni.periodic.*` procedures call into the live scheduler without depending on `uni-db`.
- **B.3 — Tokio driver.** `crates/uni/src/scheduler.rs::SchedulerHost::spawn(registry, persistence, shutdown, tick_interval)` returns an `Arc<SchedulerHost>` and spawns the driving loop on the ambient tokio runtime. The loop polls `tick_at(SystemTime::now())` every `DEFAULT_TICK_INTERVAL` (100 ms), looks up each due job's `BackgroundJobProvider` in the registry (`background_jobs()` snapshot), dispatches via `tokio::task::spawn_blocking` (the trait's `execute` is sync), and threads lifecycle transitions through the configured `SchedulerPersistence`. Replays persisted jobs + calls `requeue_orphaned_runs` on startup. Wired into `Uni::build` (`crates/uni/src/api/mod.rs`) alongside the existing `DeferralQueue` ticker; drained via `ShutdownHandle::track_task`.
- **B.4 — built-in maintenance jobs.** `crates/uni-plugin-builtin/src/background_jobs.rs` ships `TtlSweepJob` (60 s), `StatisticsRefreshJob` (5 min), `CompactionJob` (15 min) — each implementing `BackgroundJobProvider` with `Schedule::Periodic`. Registered through the "uni" plugin block in `register_builtin_plugins`. **Known follow-up:** the three `execute()` bodies are currently tracing stubs returning `JobOutcome::Done` — real work lands as the supporting host services come online (write-mode Cypher for `ttl_sweep`, the planner statistics API for `statistics_refresh`, the Lance compaction trigger for `compaction`).
- **B.5 — `uni.periodic.*` procedures.** `crates/uni-plugin-builtin/src/procedures/periodic.rs` ships `PeriodicSchedule` (`uni.periodic.schedule(qname, kind, interval_secs)`), `PeriodicCancel` (`uni.periodic.cancel(qname)`), `PeriodicList` (`uni.periodic.list()`). Each holds `Arc<dyn SchedulerControl>` injected by the host at registration time (after `SchedulerHost::spawn`). Plus the Rust-level API on `Uni`: `Uni::scheduler_host()`, `Uni::periodic_schedule(qname, schedule)`, `Uni::periodic_cancel(qname) -> bool`, `Uni::periodic_list() -> Vec<SchedulerJobRecord>`. `UniInner` gained a `scheduler_host: Arc<SchedulerHost>` field threaded through `at_snapshot` / `at_fork` clone sites.
- **B.6 — Phase B integration tests.** `crates/uni/src/scheduler.rs::tests` ships 3 driver tests using a multi-thread tokio runtime: (a) `driver_fires_periodic_job` proves end-to-end — register provider, register schedule, sleep 400 ms, counter > 0; (b) `cancel_halts_further_runs` proves cancellation stops firing within one in-flight grace; (c) `circuit_breaker_opens_after_threshold_failures` proves an `AlwaysFailJob` with a 20 ms periodic schedule has its attempts capped around the 10-failure threshold (not unbounded).

**Phase C — circuit breaker plumbing + OTel observability.**

- **C.1 — `CircuitBreaker` plumbed into the scheduler driver.** `SchedulerHost` owns `Arc<CircuitBreaker>` (default 10-fail threshold, 30 s cooldown). `dispatch_one_tick` consults `breaker.allow(&plugin_id, &qname)` before spawning each provider; on open it skips this tick and calls `mark_finished(.., false)` so the schedule recomputes a next fire instead of leaving the job `Running`. On run completion, `record_success` / `record_failure` based on `JobOutcome`. Per-(plugin_id="uni", qname) keying so a flapping `ttl_sweep` cannot poison `compaction`. Verified by `circuit_breaker_opens_after_threshold_failures` test.
- **C.2 — OTel observability layer.** `crates/uni/src/observability.rs::init_otel_subscriber(OtelConfig { service_name, otlp_endpoint })` returns an `OtelGuard` whose `Drop` shuts down the tracer provider cleanly. Uses `opentelemetry = "0.27"` + `tracing-opentelemetry = "0.28"` + `opentelemetry_sdk` + `opentelemetry-otlp` (gRPC/Tonic). Conservative shape — **not** auto-installed by `Uni::open` (would conflict with embedder subscribers); embedders opt in by calling the helper. `tracing::debug!` events emitted by `uni_plugin::observability::record_invocation` automatically become OTLP spans once the layer is installed.

**Pre-existing primitives that round out the M11 surface (unchanged):**

- **`secrets`** — `SecretStore` with seal / acquire / unseal_for_host_use / revoke; `SecretHandle` opaque-by-construction; handle-zero-never-returned invariant.
- **`verify`** — `TrustRoot` + `verify_hash_pin` (Blake3, constant-time eq) + `verify_signed_manifest`. **Real Ed25519 cryptographic verification default-on** behind feature gate (`verify.rs:84-119`, commit `8a48445e`), with tamper-detection canary.
- **`lifecycle`** — Per §M10 above.

`BackgroundJobProvider` trait + `JobDefinition` / `Schedule` / `ConcurrencyLimit` / `RetryPolicy` / `JobOutcome` / `JobContext` (now with a public `JobContext::new(cancel, last_run)` constructor since the struct is `#[non_exhaustive]`) / `CancellationToken` types shipped.

#### Closure pass 2026-05-27 — FU-1 through FU-6 all shipped

The earlier audit identified six follow-ups (FU-1 through FU-6) needed to declare M11 complete. All landed in the closure pass.

**FU-1 ✅ — Principal plumbing from `Transaction` → `ProcedureContext`.** `CURRENT_PRINCIPAL` tokio task-local in `crates/uni-query/src/query/df_udfs.rs:295`; `scoped_with_session_context(registry, principal, fut)` wraps both the session-plugin-registry and principal scopes in one call. Wired at all 6 `Session::execute_cached` + `TxQueryBuilder` sites in `crates/uni/src/api/session.rs` (lines 1112/1140/1172/1337/1377/1397) and at `Transaction::query` in `crates/uni/src/api/transaction.rs:286`. Read at `crates/uni-query/src/query/executor/procedure.rs:~672` + `crates/uni-query/src/query/df_graph/procedure_call.rs:~672` via `crate::current_principal()`. Bonus: added `GraphExecutionContext::with_writer` so declared `WRITE`-mode procedures can mutate via the inner-query host. Live E2E `declared_procedure_write_mode_creates_nodes` green.

**FU-2 ✅ — Secret-handle IPC extension-type rejection.** `SECRET_HANDLE_EXTENSION = "uni-db.secret-handle"` constant in `crates/uni-plugin-wasm-rt/src/ipc.rs`; `reject_secret_handles(batch)` walks `Field::metadata` for `ARROW:extension:name`, recursing into `Struct`/`List`/`LargeList`/`FixedSizeList`/`Map` children. `encode_batch`/`encode_batches`/`decode_batches` all gate through it; first offending column name surfaces in `IpcError::SecretLeakAttempt { column }`. 3 tests green: `encode_batch_rejects_secret_handle_column`, `decode_batches_rejects_secret_handle_column`, `encode_batch_rejects_secret_handle_inside_struct`. **Routed to a future M11 follow-up**: the `host.secrets.acquire` WIT export across Component Model + Extism — the host-side `SecretStore` is already complete; only the WIT plumbing remains and is a contained ~600 LOC PR.

**FU-3 ✅ — OTel trace propagation + outbound HTTP demo.** `current_traceparent()` in `crates/uni/src/observability.rs` reads `tracing::Span::current()` via `tracing_opentelemetry::OpenTelemetrySpanExt::context()` and formats the W3C `traceparent` header (`00-<trace_id>-<span_id>-<flags>`). `http_get_with_traceparent(url)` injects it on outbound `reqwest` GETs. Running `cargo run --manifest-path examples/otel-demo/Cargo.toml` confirms end-to-end: outer span trace-id matches the `traceparent` header the local capture server saw. 2 integration tests in `crates/uni/tests/otel_traceparent_e2e.rs` validate the no-leak and round-trip invariants. The plugin-side `host-otel`/`host-net` WIT imports are routed to the same M11 follow-up as the secret-acquire WIT export — host-side plumbing is done.

**FU-4 ✅ — CDC runtime + checkpoint persistence + mutation rows.** `crates/uni/src/cdc_runtime.rs` spawns a tokio task that subscribes to the commit broadcaster, calls `provider.start(CdcStartContext { from_lsn })` per registered `CdcOutputProvider`, forwards each `CommitNotification` as a `CdcBatch` with `lsn_start = causal_version` / `lsn_end = version`, and persists per-provider LSN to `<data_path>/_system/cdc_checkpoints.json` via `CdcCheckpointSidecar` (atomic write-rename, single-file JSON array, schema matches proposal §9.7). `CdcRuntime::discover_new_providers` runs on every commit so providers registered post-`Uni::build` (via `Uni::add_plugin`) don't miss commits. **`CdcBatch::mutations` carries the actual mutation rows** as of the M11 closure: `Transaction::commit` materializes `MutationEvents::from_l0_with_probe(&tx_l0, probe, &props)` → `MutationEvents::materialize_all() -> RecordBatch` (in the canonical `event_row_schema` shape) onto `CommitNotification.mutations: Option<Arc<RecordBatch>>` whenever `PluginRegistry::cdc_outputs_is_empty()` is false. Zero-cost empty-registry hot path preserved: no extraction when neither triggers nor CDC subscribers are registered. Tests: `cdc_runtime_delivers_batches_per_commit` (LSN monotonicity across 3 commits), `cdc_runtime_delivers_mutation_rows` (row content + canonical schema match), plus 3 unit tests covering sidecar round-trip + close-reopen + provider-overwrite semantics.

**FU-5 ✅ — `TriggerOutcome::Defer` durable queue + `delay: Duration` on `TriggerDeferral`.** `TriggerDeferral` gains `delay: Option<Duration>` (marked `#[non_exhaustive]` with `from_payload(...)` / `after(payload, delay)` constructors). Added `TriggerPlugin::on_deferred(ctx, events, payload) -> TriggerOutcome` with a default impl delegating back to `fire(ctx, events)` — no ABI break. `DeferralQueue::with_persistence(<data_path>)` enables JSON-sidecar persistence at `<data_path>/_system/deferred_triggers.json` (encodes `MutationBatch` as Arrow IPC stream bytes); `load_from_sidecar(&registry)` re-binds each persisted row to its `TriggerPlugin` by qname; `push`/`drain_due` mirror state to the sidecar after every change. Tests: `defer_with_explicit_delay_waits_at_least_that_long` (350 ms gap), `on_deferred_receives_payload_from_first_fire` (payload round-trip), legacy `defer_with_short_delay_fires_eventually` + `defer_retry_cap_drops_after_n` still green.

**FU-6 ✅ — Built-in job body completions + TTL sweep test.** `TtlSweepJob` calls `host.execute_write_cypher("MATCH (n) WHERE n.__ttl < timestamp() DETACH DELETE n")` (`background_jobs.rs:89-100`); added unit tests (`ttl_sweep_job_issues_delete_cypher_to_host`, `ttl_sweep_job_returns_failed_retry_on_host_error`) using a `RecordingJobHost` that asserts the issued Cypher contains `__ttl` and `DETACH DELETE`, plus error-path retry semantics. `CompactionJob` dispatches to `host.compact_storage()` (`background_jobs.rs:202-215`) — the host trait's default impl is a no-op, so the call is wired but does nothing until a storage backend overrides it; `compaction_job_calls_compact_storage` confirms the dispatch. `StatisticsRefreshJob` remains a documented stub pending the planner-statistics-refresh API. These represent a planner/storage extension, not a plugin-framework gap.

**M11 closed (2026-05-27, refined 2026-05-27 post-audit closure):** all six FU follow-ups landed in the original closure pass; the audit-driven refinement filled the remaining two semantic gaps — `SystemLabelSchedulerPersistence` now round-trips the `Schedule` kind across restart via a new `SchedulerPersistence::record_scheduled` trait method (called by `SchedulerHost`'s `SchedulerControl::add_scheduled_job` impl), and `CdcBatch::mutations` carries actual events via `MutationEvents::materialize_all()` materialized on the commit path. §4 acceptance criteria #2, #3, #4, and #6 are now ✅. §2 row is `✅ complete`. Outstanding follow-ups routed to M12: (a) `host.secrets.acquire` WIT export across CM + Extism (the IPC-layer rejection is the security-critical half and is shipped — adding the WIT export only matters once external plugins want to consume the acquire surface); (b) `host-otel`/`host-net` WIT exports for plugin-side trace propagation (the host-side `current_traceparent` + `http_get_with_traceparent` are shipped); (c) `StatisticsRefreshJob` real body once planner-stats API solidifies, and concrete `Storage::compact` impls (the host call site is wired but the trait default is a no-op).

#### Known follow-ups (deliberately not in committed scope)

1. **Built-in job body coverage is mixed.** `TtlSweepJob` ✅ real (issues `MATCH (n) WHERE n.__ttl < timestamp() DETACH DELETE n` via `JobHost::execute_write_cypher`). `CompactionJob` ✅ wired (calls `JobHost::compact_storage`, which is a default no-op until a storage backend implements compaction). `StatisticsRefreshJob` remains a tracing stub pending the planner-statistics-refresh API. The trait shape, registration path, scheduler driver, and circuit-breaker gating are all real and tested.
2. **`SystemLabelSchedulerPersistence` uses a JSON sidecar.** Scheduler state persists at `<data_path>/_system/background_jobs.json` (atomic write-then-rename), not in a `_BackgroundJob` graph label. The `Schedule` kind round-trips across restart (`record_scheduled` captures it; `load_all` restores `Periodic` / `Cron` / `Once` / `Manual` faithfully). The Cypher-MERGE mirror via `_BackgroundJob` is best-effort and a documented follow-up; the backend swap is a drop-in once write-enabled inner-query host coverage lands for system labels.
3. **`SystemLabelPersistence` (declared plugins) uses a JSON sidecar.** Records live at `<data_path>/_system/declared_plugins.json`, not in the `_DeclaredPlugin` graph label. The `DeclaredPlugin` serde shape matches proposal §9.7 verbatim, so the Cypher-MERGE cutover is a backend replacement. Declarations *do* survive restart with this layout.
4. **Phase D (secrets WIT membrane)** — the IPC-layer rejection of `secret-handle`-tagged Arrow columns is shipped (FU-2); the remaining WIT-side `host.secrets.acquire` export and `host-otel`/`host-net` plugin-side imports are routed to M12 along with the rest of the WIT surface work.

**Test counts after M11 (all green, measured 2026-05-27 post-closure):** `uni-plugin` 97, `uni-plugin-custom` 33, `uni-plugin-builtin` 115, `uni-plugin-wasm-rt` 21, `uni-db::integration_admin` 161, plus the in-tree library suites — well over **800 tests pass** across the M11-touched workspace.

**Goal:** The operational layer. Manifest signing (Ed25519), hash pinning (blake3), sealer/unsealer secret handles, OpenTelemetry propagation, the background-job scheduler with persistence, the `uni.periodic.*` procedures.

**Deliverables:**

1. **Signing & pinning** in `crates/uni-plugin/src/verify.rs`:
   - Ed25519 signed-manifest verification.
   - Blake3 hash-pinning enforcement.
   - Trust-root configuration in `UniConfig`.

2. **Secrets membrane** in `crates/uni-plugin-wasm/src/host_impls/secrets.rs`:
   - `secret-handle` WIT resource — opaque to the plugin.
   - `host.secrets.acquire(id)` → handle.
   - Capability-gated imports accepting `secret-handle` (e.g., `host-net-secrets.http_get_with_secret`).
   - IPC layer rejects serializing `secret-handle` into output Arrow batches.
   - Tracing emits an audit event per `acquire` call.

3. **OpenTelemetry** in `crates/uni/src/observability.rs`:
   - `tracing-opentelemetry` layer installed by default in `Uni::open`.
   - `host.span_enter` / `host.span_exit` propagate `TraceId` to plugin spans.
   - `host-otel.trace-context-extract/inject` for plugins making outbound HTTP calls.
   - Prometheus metrics endpoint (`/metrics`) exporting the `uni_plugin_*` counters/histograms.

4. **Scheduler** in `crates/uni/src/scheduler.rs`:
   - Tokio-backed scheduler with `Schedule::Once | Periodic | Cron | Manual`.
   - Persistent state in `uni_system.background_jobs` (label).
   - Per-plugin concurrency limits via `Capability::BackgroundJob { max_concurrent }`.
   - Background reaping of completed runs.

   **M11-owned CDC runtime** (formerly routed from M5i — now a first-class M11 deliverable, since it depends on this tokio scheduler driver): `MemoryCdcOutputProvider` is registered in `uni-plugin-builtin/src/extras.rs:84` but no host loop drives its `deliver` / `checkpoint` / `shutdown` cycle. Add a CDC-specific `BackgroundJob` (or a sibling `CdcRuntime`) that: (a) pulls registered `CdcOutputProvider`s from the registry at startup; (b) opens a stream per provider via `start(ctx)`; (c) delivers commit-stream `CdcBatch`es to each; (d) calls `checkpoint()` on commit boundary and persists LSN to `uni_system.cdc_checkpoints` (sibling to `uni_system.background_jobs`) for crash recovery; (e) honors the M10 per-kind reload discipline for `CdcOutputProvider` (checkpoint → drop → restart at checkpointed LSN). Anchor: `crates/uni-plugin-builtin/src/extras.rs:84`.

   **M11-owned `TriggerOutcome::Defer` queue** (formerly routed from M5f — now a first-class M11 deliverable, since it shares the same scheduler primitive): today `TriggerRouter::dispatch_before` / `dispatch_after` log-and-treat-as-`Continue` when a trigger returns `TriggerOutcome::Defer { delay, payload }`. Once the scheduler driver lands, queue `Defer` returns as deferred trigger invocations: persist `{ trigger_id, payload, fire_at }` to a sibling label `uni_system.deferred_triggers`, register a sweeper `BackgroundJob` that fires due rows at each scheduler tick, and re-invoke the trigger body via `TriggerPlugin::on_deferred(payload)`. Crash-recovery comes for free via the persistence layer. Acceptance: a trigger returning `Defer { delay: 5s }` from `AfterCommit` re-fires ≈ 5s later; durable across restart.

5. **Built-in background jobs** in `uni-plugin-builtin/src/background_jobs.rs`:
   - `uni.system.ttl_sweep` — TTL processor.
   - `uni.system.statistics_refresh` — planner cardinality refresh.
   - `uni.system.compaction` — Lance background compaction trigger.

6. **`uni.periodic.*` procedures** in `uni-plugin-builtin/src/procedures/periodic.rs`:
   - `submit`, `schedule`, `iterate`, `commit`, `list`, `cancel`.

7. **Circuit breaker** in `crates/uni-plugin/src/circuit_breaker.rs`:
   - Per-(plugin_id, qname) breaker, N consecutive failures → open for cooldown.

8. **M11-owned write-enabled `ProcedureHost::execute_inner_query` + M9 declared-plugin cutover** (formerly routed from M9 — now a first-class M11 deliverable, since both blocked items share the same host primitive). Today `QueryProcedureHost::execute_inner_query` in `crates/uni-query/src/query/executor/procedure_host.rs` is **read-only** and **does not bind parameters**, which blocks two M9 deferrals. Extend the host with:

   - **Parameter binding** — accept a `params: BTreeMap<String, Value>` arg (or equivalent) and thread bindings into the inner `Executor` so `$first`, `$last` etc. resolve against caller-supplied values.
   - **Write mode** — allow the inner query to run inside the *outer* transaction's write scope when the calling procedure declares `ProcedureMode::Write` and the principal holds `Capability::ProcedureWrites`. (Read-mode callers still get a read-only inner executor.)
   - **Two M9 cutovers ride this primitive:**
     - **(a) `_DeclaredPlugin` system-label persistence.** Add `impl Persistence for SystemLabelPersistence` in `crates/uni-plugin-custom/src/persistence.rs` that issues `MERGE (:_DeclaredPlugin {qname: $qname, kind: $kind, ...})` / `MATCH ... DETACH DELETE` / `MATCH (p:_DeclaredPlugin) RETURN p` through the write-enabled inner-query host. The `DeclaredPlugin` serde shape already matches proposal §9.7 field-for-field, so this is a drop-in swap of `Arc::new(NullPersistence)` for `Arc::new(SystemLabelPersistence::new(host))` in `crates/uni/src/api/mod.rs:174`. Register `_DeclaredPlugin` as a system label on `uni-plugin-custom`'s init path.
     - **(b) `declareProcedure` body execution.** The `DeclareProcedureProcedure` (`crates/uni-plugin-custom/src/lib.rs` macro at `declare_kind_procedure!`) currently records-and-persists only. Add a `SyntheticProcedurePlugin` (mirroring `SyntheticScalarPlugin` at `lib.rs:827`) that, on invocation, calls `ProcedureHost::execute_inner_query(body, params, mode)` with caller args bound by name. The declared procedure's `ProcedureMode::Read`/`Write` flows from the `mode` declare-time arg (`READ` / `WRITE`); the registered synthetic plugin inherits the appropriate capability set so the M11 capability gate enforces write authorization at invocation time. `declareTrigger` body execution rides the same primitive — the M5f `TriggerRouter` invokes the trigger body, and a Cypher-body trigger plugin calls `execute_inner_query` from inside its `on_event` handler.

   Acceptance:
   - `CALL uni.plugin.declareFunction('mycorp.fullName', '$first + " " + $last', 'string', '["first","last"]')` survives a `Uni` close/reopen via system-label persistence (default backend swap, no opt-in flag).
   - `CALL uni.plugin.declareProcedure('mycorp.findFriends', 'MATCH (p:Person {name: $name})-[:KNOWS]->(f) RETURN f.name AS friend', 'READ', '[{"name":"friend","type":"string"}]')` followed by `CALL mycorp.findFriends('Ada') YIELD friend` returns the friends-of-Ada rows.
   - A `declareProcedure` declared in `WRITE` mode by a principal without `ProcedureWrites` is rejected at declare time with `CustomError::CapabilityDenied`.

   Anchor: `crates/uni-query/src/query/executor/procedure_host.rs` (host extension); `crates/uni-plugin-custom/src/persistence.rs` (new backend); `crates/uni-plugin-custom/src/lib.rs` (synthetic procedure plugin).

9. Tests:
   - `crates/uni-plugin/tests/signed_manifest.rs` — invalid signatures rejected; valid signatures accepted.
   - `crates/uni-plugin-wasm/tests/secret_leak_attempt.rs` — secret-handle in output Arrow batch → `UniError::SecretLeakAttempt`.
   - `crates/uni-plugin-wasm/tests/otel_propagation.rs` — plugin span shares `TraceId` with surrounding query.
   - `crates/uni/tests/scheduler_*.rs` — cron schedules fire; state persists across restart; cancel halts further runs.
   - `crates/uni/tests/circuit_breaker.rs` — 10 consecutive failures open the breaker; queries fail fast for 30s; recovery.
   - `crates/uni-plugin-custom/tests/system_label_persistence.rs` — declared function survives `Uni::close` → `Uni::build` against the same backing store (replaces the JSON-sidecar restart test once `SystemLabelPersistence` is the default).
   - `crates/uni-plugin-custom/tests/declare_procedure_e2e.rs` — declared `READ`-mode procedure returns rows from Cypher body; declared `WRITE`-mode procedure creates nodes; capability-denied write-mode declaration rejected.

**Files changed:**

- `crates/uni-plugin/src/verify.rs` (new).
- `crates/uni-plugin/src/circuit_breaker.rs` (new).
- `crates/uni-plugin-wasm/src/host_impls/{secrets,otel,fs,net,kms}.rs` (new + completion).
- `crates/uni/src/scheduler.rs` (new).
- `crates/uni/src/observability.rs` (new).
- `crates/uni-plugin-builtin/src/background_jobs.rs` (new).
- `crates/uni-plugin-builtin/src/procedures/periodic.rs` (new).
- New tests.

**Acceptance criteria:**

1. ✅ A signed plugin with valid Ed25519 signature loads; the same plugin with a flipped bit fails with `PluginError::SignatureInvalid`. *(Real Ed25519 verification default-on in `crates/uni-plugin/src/verify.rs:84-119`.)*
2. ✅ FU-2 shipped: `uni-db.secret-handle` Arrow extension type with `encode_batch` / `decode_batches` rejection in `crates/uni-plugin-wasm-rt/src/ipc.rs` — including nested `Struct`/`List`/`Map` field walks. Returns `IpcError::SecretLeakAttempt { column: String }` naming the offending column. 3 tests green: `encode_batch_rejects_secret_handle_column`, `decode_batches_rejects_secret_handle_column`, `encode_batch_rejects_secret_handle_inside_struct`. Exposing `host.secrets.acquire` across the Component Model + Extism WIT membrane is the next-up follow-up (the host-side `SecretStore::acquire` is already complete; only the WIT plumbing remains).
3. ✅ FU-3 shipped: `cargo run --manifest-path examples/otel-demo/Cargo.toml` prints a matching trace-id across the outer query span and the captured outbound HTTP `traceparent`. `uni_db::observability::current_traceparent()` extracts the W3C `traceparent` from the current `tracing::Span` via `tracing_opentelemetry::OpenTelemetrySpanExt`; `http_get_with_traceparent(url)` injects it on outbound `reqwest` GETs. 2 tests green in `crates/uni/tests/otel_traceparent_e2e.rs` + working demo binary at `examples/otel-demo/`.
4. ✅ `CALL uni.periodic.schedule('foo', 'periodic', '900')` registers; the SchedulerHost driver fires the job's `BackgroundJobProvider::execute` on its interval. *(Procedure shipped at `crates/uni-plugin-builtin/src/procedures/periodic.rs`. Cron variant landed 2026-05-27: `kind="cron"` accepts a 5- or 6-field cron expression in `schedule_arg`, validated at registration via the `cron` crate and stored as `Schedule::Cron(SmolStr)` — see `schedule_cron_dispatches_to_scheduler` + `schedule_cron_rejects_bad_expression`.)* **Cross-restart scheduler durability:** `SystemLabelSchedulerPersistence` (`crates/uni/src/scheduler_persistence.rs`) ships as the default for local-disk instances and now **round-trips the `Schedule` kind across restart** via the new `SchedulerPersistence::record_scheduled` trait method (default no-op so other backends stay unchanged); `Periodic` / `Cron` / `Once` / `Manual` reload faithfully via `#[serde(default)]`-compatible JSON sidecar. In-memory / object-store instances retain `MemoryPersistence`. New tests `periodic_schedule_survives_restart` / `cron_schedule_survives_restart` / `once_schedule_survives_restart` / `legacy_sidecar_without_schedule_falls_back_to_manual` cover the round-trip + back-compat. The Cypher-MERGE mirror via `_BackgroundJob` system label is best-effort + a documented follow-up.
5. ✅ `cargo nextest run -p uni-db --lib scheduler:: -p uni-plugin --lib scheduler::` — 29 scheduler tests pass (incl. the 5 new round-trip tests), including `circuit_breaker_opens_after_threshold_failures` which proves attempts cap at the failure threshold under a flapping provider.
6. ✅ **M9 cutover (system-label persistence + `declareProcedure` execution):** a declared function persists and survives close/reopen via `SystemLabelPersistence` (at `<data_path>/_system/declared_plugins.json` — the Cypher-MERGE backend swap is a documented M12 follow-up); a `READ`-mode declared procedure runs its Cypher body through `SyntheticProcedurePlugin` synthesized by `CypherProcedureSynthesizer`. **Capability-denied WRITE-mode rejection at declare time** is unit-tested at `crates/uni-plugin-custom/src/lib.rs:1879-1928` (3 tests green). **FU-1 unblocked the live WRITE-mode E2E** — `crates/uni/tests/plugin_custom_declare.rs::declared_procedure_write_mode_creates_nodes` green: a session with a `ProcedureWrites`-capable principal calls `uni.plugin.declareProcedure(..., 'WRITE', ...)`, then `CALL mycorp.createmarker()`, and the procedure's Cypher body `CREATE (m:Marker {at: 42})` produces a visible :Marker node verified by a subsequent MATCH. The plumbing: `CURRENT_PRINCIPAL` tokio task-local set at `Session::query_*` / `Transaction::query` boundaries; read at `procedure.rs:~672` + `procedure_call.rs:~672` immediately before `plugin.invoke(ctx, ...)`; writer threaded via new `GraphExecutionContext::with_writer` from the `Executor`.

**Risks (resolved or noted):**

- ✅ *Tokio runtime collision* — resolved by hooking `SchedulerHost::spawn` into the existing `Uni::build` tokio context (next to the `DeferralQueue` ticker); blocking-sync providers run on `spawn_blocking`. `Uni::build_sync` constructs a multi-thread runtime first, so the `block_in_place` bridge used by `SyntheticProcedurePlugin` is sound in both modes.
- Noted: the inner-query write path under an outer transaction must not deadlock on the writer lock. The outer tx *owns* the writer; the inner query reuses the same `Arc<RwLock<Writer>>` (via `Executor::new_with_writer`) rather than acquiring a separate one.

**Parallel work unlocked:** M12 (CLI consumes the signing/pinning APIs; OCI installation consumes signature verification).

---

### M12 — CLI + OCI distribution + Python bindings + conformance + perf regression suite + docs

**Status (2026-05-23): ▶ partial.** `uni-plugin-conformance` crate shipped with `ConformanceTarget` (`WasmPath` / `LiveRust` variants), `CheckResult`, `ConformanceReport`, `run_against(target) -> ConformanceReport`, and `assert_pass()`. **`run_against_plugin(&dyn Plugin)` real 6-probe suite shipped** (commit `44944851`, `lib.rs:148-240`): manifest.parse / id_format / abi.in_range / capabilities.declared / registration.commit / registration.idempotent. 6 tests. NO CLI (`uni plugin install/...`), NO OCI artifact loader, NO Python bindings (`Uni.add_plugin`, decorators), NO perf regression suite, NO 100-page plugin-author guide — all pending.

**Goal:** The final integration milestone. Polish for v1.0 release.

**Deliverables:**

1. **CLI** in `crates/uni-cli/src/cmd/plugin.rs`:
   - `uni plugin install <path|url|oci-ref>` — auto-detects format (`.wasm` / `.rhai` / `oci://` / `https://`).
   - `uni plugin list`, `info <id>`, `grant <id> <cap>`, `revoke <id> <cap>`, `remove <id>`, `reload <id>`, `verify <id>`, `help <qname>`, `declared list`, `declared drop <qname>`.

2. **OCI artifact loader** in `crates/uni-plugin-wasm/src/oci.rs`:
   - Pulls WASM components from OCI registries (Docker Hub, GHCR, ECR, GAR).
   - Verifies signatures via `cosign`-compatible attestations.
   - Caches downloaded artifacts in `~/.uni/plugins/cache/`.

3. **Python bindings** in `bindings/uni-db/src/plugins.rs`:
   - `Uni.add_plugin(wasm_bytes_or_path, grants)`.
   - `Uni.load_rhai_plugin(path_or_str, grants)`.
   - `@Uni.scalar_fn`, `@Uni.aggregate_fn`, `@Uni.procedure`, `@Uni.rhai_plugin` decorators.
   - `Uni.plugins()`, `Uni.plugin(id).info()`.

4. **Conformance suite** as `crates/uni-plugin-conformance/`:
   - Test fixtures for every plugin kind.
   - `cargo plugin-conformance --plugin path/to/foo.wasm` runner.
   - Fixtures verify: manifest correctness, schema correctness, determinism honesty (a `Pure` plugin called twice produces identical output), error model compliance, resource-limit adherence.

5. **Performance regression suite** in `crates/uni-bench/benches/plugin_perf.rs`:
   - Compare `score(x, y)` across: native built-in, compile-time plugin, WASM pre-warmed, WASM cold, PyO3 vectorized, PyO3 row, Rhai vectorized, Rhai row.
   - Compare aggregate `union_bbox` via built-in vs `LocyAggregate` plugin.
   - Compare vector kNN via built-in vs `IndexKindProvider` plugin.
   - Records baselines; CI fails on > 10% regression.

6. **Documentation:**
   - `docs/plugin-author-guide.md` — 100+ page comprehensive guide.
   - Update `docs/proposals/plugin_framework.md` cross-references to the now-existing code.
   - One full example plugin per loader (Rust, WASM, Python, Rhai) in `examples/`.

7. **M12-owned Collation grammar + wiring** (formerly routed from M5i — now a first-class M12 deliverable, since it depends on a Cypher grammar extension): 5 collation providers (`AsciiCaseSensitive`, `AsciiCaseInsensitive`, `UnicodeCodepoint`, `UnicodeCaseInsensitive`, `NaturalNumeric`) are registered in `crates/uni-plugin-builtin/src/collations.rs:18-24` but unreachable from Cypher because the parser doesn't accept `ORDER BY ... COLLATE name`. Three deliverables:
   - Extend `crates/uni-cypher/src/grammar/` to accept the optional `COLLATE <identifier>` clause on `ORDER BY` items.
   - Thread the collation choice through `LogicalPlan::Sort` → physical sort operator so the comparator function looks up `CollationProvider::compare` from the plugin registry.
   - Extend the indexed-string-lookup path to consult `CollationProvider::normalize` when building keys, so `WHERE name = 'X' COLLATE ascii_case_insensitive` hits the right index entries.
   - Tests: TCK feature `Collation.feature` exercising sort + index-lookup with the 5 built-in collations.
   Independent of the other M12 deliverables but lives here because the grammar extension is the natural M12 surface. Acceptance: `ORDER BY n.name COLLATE ascii_case_insensitive` parses, executes, and returns rows in the expected order.

**Files changed:**

- `crates/uni-cli/src/cmd/plugin.rs` (new, ~600 lines).
- `crates/uni-plugin-wasm/src/oci.rs` (new).
- `bindings/uni-db/src/plugins.rs` (extended).
- `crates/uni-plugin-conformance/**` (new).
- `crates/uni-bench/benches/plugin_perf.rs` (new).
- `docs/plugin-author-guide.md` (new).
- `examples/**`.

**Acceptance criteria:**

1. `uni plugin install oci://ghcr.io/example/geo:0.3.1` round-trips: resolves, verifies signature, registers; `RETURN geo.haversine(...)` works.
2. Python `from uni_db import Uni; db.add_plugin(open('geo.wasm','rb').read()); db.query(...)` round-trips.
3. `cargo plugin-conformance --plugin example_geo.wasm` reports zero failures.
4. `cargo bench --bench plugin_perf` records the 8 numbers (one per loader/mode) and they meet the targets in the proposal §19.
5. All 22 acceptance criteria from proposal §19 pass.

**Risks:**

- *Risk:* OCI registry quirks (rate limits, regional ECR endpoints). *Mitigation:* The cache + retry logic + clear error messages. Acceptance is one happy-path OCI install; multi-registry hardening is a separate issue.

**Parallel work unlocked:** Release.

---

## 5. Critical Path

The longest dependency chain is:

```
M0 → M1 → M2 → M3 → M4 → M5 → M6a → M6b → M11 → M12
                                          ↑
                                          └── M10 is parallel to M11
```

M7 (Rhai), M8 (PyO3), M9 (meta-plugin), M10 (hot reload) are not on the critical path and run in parallel after their dependencies (see §6).

---

## 6. Parallelization Strategy

The dependency DAG (see §3 and §5) defines the ordering constraints. Where the DAG allows fan-out, work can proceed in parallel.

**Core lane:** M0 → M1 → M2 → M3 → M4 → M5 (M5a, M5b, M5c) → M6b (CM) → M11 → M12 (final).

**Loaders + meta lane:** Picks up after M3 with M5d (CRDT), M5e (hooks), M5f (triggers). After M5 lands, takes M6a (Extism, lower-risk, parallel to M6b). After M6b lands, takes M7 (Rhai) and M9 (meta-plugin) in parallel.

**Operations lane:** Picks up M8 (PyO3) as soon as M2 lands (only needs the scalar-fn trait). After M6, takes M10 (hot reload) and the operations sub-tasks of M11 (signing, secrets, OTel).

Past a certain headcount the bottleneck shifts: M5 sub-tasks parallelize freely, but M3 (FoldAggKind) and M4 (procedure migration) stay single-owner because they require tight coordination on the closed-enum-to-registry refactor.

---

## 7. Risk Register

Quantified risks. Probability × severity, with mitigations.

| #  | Risk                                                                                  | Prob. | Severity | Mitigation                                                                                                                       |
|----|---------------------------------------------------------------------------------------|-------|----------|----------------------------------------------------------------------------------------------------------------------------------|
| R1 | M3 (FoldAggKind retirement) breaks a TCK scenario in non-obvious ways                  | M     | High     | M0 captures pre-refactor TCK pass-list; M3 is gated on byte-identical reproduction of every scenario.                            |
| R2 | M2 perf win (NativeArrowUdf vs LargeBinary) fails to materialize                       | L     | Med      | Hard target ≥20% in M2 acceptance. If unmet, root-cause before milestone-merge.                                                  |
| R3 | wasmtime / Component Model toolchain churn breaks M6 mid-implementation                | M     | High     | Pin all WASM-related crates in Cargo.toml + Cargo.lock; never use `*` ranges; CI matrix tests against multiple wasmtime versions.|
| R4 | M6 cold-start pool design has thread-safety bugs                                       | M     | High     | The pool sits behind `lock_free`/`crossbeam` `ArrayQueue` — battle-tested. Property tests with `loom` for race conditions.       |
| R6 | OCI registry integration in M12 hits authentication/rate-limit complexity              | M     | Low      | M12 ships one happy-path; multi-registry hardening is a follow-up issue, not v1.0-blocking.                                      |
| R7 | DataFusion API changes between Cargo workspace's pinned version and the next release   | H     | Med      | DataFusion-version upgrades are isolated PRs, not bundled with milestones.                                                       |
| R8 | The `uni-plugin-builtin` crate becomes a circular-dep nightmare                        | M     | High     | `uni-plugin-builtin` depends *only* on `uni-plugin`, `uni-query`, `uni-store`, `uni-crdt`, `uni-algo`. Never the reverse.        |
| R9 | M5's per-kind hot-reload discipline (M10) exposes subtle bugs in the storage layer     | M     | High     | The `reload_storage_backend.rs` test is the canary; specifically tests the new-Storage / old-Storage coexistence window.         |
| R10| Capability-gating-by-import-absence is mis-implemented (a host fn leaks despite ungrant)| L     | Critical | The `linker_capability.rs` test in M6 is exhaustive: for every host fn, attempt to instantiate without the gating cap.           |
| R11| The Rhai sandbox is bypassable (e.g., via a Rhai-side metaprogramming trick)            | L     | High     | M7's `sandbox.rs` test enumerates the known Rhai escape catalog (eval, dynamic dispatch, custom-syntax injection, module-resolver fall-throughs); `eval` is `disable_symbol`'d at Engine construction. |
| R12| OTel propagation adds measurable overhead to hot-path queries                          | M     | Low      | Bench in M11 quantifies; if regression > 5%, configure tracing subscriber for sampling rather than always-on.                    |
| R13| Multi-version ABI coexistence (M10) consumes too much memory (one Linker per major)    | L     | Low      | Currently only one major exists; the cost is theoretical until a second major is published.                                      |
| R14| The `uni.plugin.declareFunction` meta-plugin enables privilege escalation              | L     | Critical | M9 capability-inheritance test verifies declarer must hold all caps the declared procedure requires.                              |
| R15| End-user plugin authors find the WIT toolchain painful enough to avoid v1.0            | M     | Med      | M12 docs include a `cargo-component` cheatsheet; one full example plugin per loader.                                              |

The "Critical" severity entries (R10, R14) get explicit security-team review during their milestones.

---

## 8. Testing Strategy Across Milestones

### 8.1 Layered test suite

Every milestone contributes to four test layers:

1. **Unit** — per-crate, single-function-or-type-under-test.
2. **Integration** — per-crate, multi-component interactions.
3. **TCK** — `uni-tck` (Cypher) and `uni-locy-tck` (Locy) scenarios.
4. **Conformance** (post-M12) — exercises external plugin authors' contracts.
5. **Benchmark** — `cargo bench` perf regression.

Every milestone has acceptance criteria specifying which layers must be green.

### 8.2 Test pyramid by milestone

```
                    Conformance (M12)
                  Benchmarks (M2, M3, M6, M12)
            TCK additions per milestone
       Integration tests per milestone
   Unit tests per milestone (always the largest layer)
```

### 8.3 Property testing

Particularly for M2 (`LargeBinary` vs `NativeArrowUdf` parity), M3 (`FoldAggKind` vs `LocyAggregate` parity), and M6 (Arrow IPC round-trip), `proptest`-based property tests are required. The properties:

- M2: for any randomly-generated Arrow input, both paths produce equal output.
- M3: for any randomly-generated Locy stratum, the new path produces the same fixpoint as the old.
- M6: for any randomly-generated `RecordBatch`, IPC serialize + deserialize is the identity.

### 8.4 `loom` for concurrent code

M6's `WasmInstancePool` and M10's `arc-swap` registries are concurrent code that benefits from model-checking. `loom` tests for both.

### 8.5 Fuzzing

Post-M6: `cargo fuzz` for the WIT IPC layer (the most adversarial surface — malformed Arrow IPC bytes from a misbehaving WASM plugin must not corrupt the host).

### 8.6 CI matrix

Every milestone's PR runs:
- `cargo nextest run --workspace --release` on Linux (Ubuntu 24.04), macOS (Sonoma), Windows (Server 2022).
- `cargo clippy --workspace --release -- -D warnings`.
- `cargo fmt --check`.
- For M6+: `cargo build --target wasm32-wasip2 --release -p example-wasm-geo`.
- For M7+: `cargo nextest run -p uni-plugin-rhai --features rhai`.
- For M11+: a smoke test exercising the OTel exporter.

---

## 9. Staging & Rollback

### 9.1 Branching model

- `main` — always green; every milestone merges here.
- `feature/plugin-fw-Mn` — milestone branches, rebased onto `main` before merge.
- `worktree/plugin-fw` — the umbrella worktree; sub-worktrees for parallel work (`worktree/plugin-fw-wasm`, `worktree/plugin-fw-rhai`, etc.).

### 9.2 Rollback per milestone

Each milestone is designed to be **revertable as a single PR-merge commit** without breaking subsequent milestones — except M3 (FoldAggKind retirement) and M4 (procedure dispatch) which contain destructive deletions. For M3 and M4, the deletion is the *last* commit in the PR; if the milestone fails post-merge, the deletion commit is the revert target.

A milestone's revert is acceptable if:
1. No subsequent milestone has shipped (depending on the reverted milestone).
2. The TCK still passes after revert.

### 9.3 Feature-flagging during integration

For M6 (WASM), M7 (Rhai), and M8 (PyO3): the loader crates are gated behind Cargo features (`wasm`, `rhai`, `pyo3`). Embedders not wanting a loader can disable it. Default features enable all loaders.

For M11 (capabilities + scheduling): the scheduler is on by default but can be disabled via `UniConfig::with_scheduler(false)` for embedded use cases.

### 9.4 Staging environment

A dedicated staging instance of uni-db running on the team's test infrastructure pulls every milestone-merge for soak testing (the test suite plus a synthetic-load harness) before the next milestone begins.

---

## 10. Effort Summary

| Milestone | Done so far | Remaining | Status (2026-05-25) |
|-----------|-------------|-----------|---------------------|
| M0        | full        | none      | ✅ complete         |
| M1        | full (+ extras for M10/M11 modules) | none | ✅ complete + extended |
| M2        | full (cutover landed 2026-05-24) | none | ✅ complete |
| M3        | full        | none — `FoldAggKind` deleted; monotonicity enforced at compile time | ✅ complete |
| M4        | full (83 procedures via plugin path; 922-line `procedure_call.rs`) | none — Principal-based capability gating intentionally deferred to M6 per plan | ✅ complete |
| M5        | Batch 1 (M5a / M5d / M5e / M5i) ✅ landed 2026-05-24; Lance fork wiring (M5a final follow-up) ✅ landed 2026-05-24 — `uni-plugin` 1.5.0; **Batch 2 (M5b + M5h + M5c.1) ✅ landed 2026-05-24 — `uni-plugin` 1.6.0** (registry-canonical pattern: planner consults `IndexKindProvider` / `CatalogProvider` / `ReplacementScanProvider`; phased `SessionHook` chain dispatched additively alongside legacy per-session map; built-in `PushdownNegotiationRule` elides `Filter → TableScan` when source claims `SupportsFilterPushdown::Full`; all 36 algorithms registered as `AlgorithmProvider`s via `AlgoProviderBridge` host-callback); **Batch 2 follow-ups ✅**: #2 physical-phase optimizer rules + `OptimizerRuleProvider::physical_rule` ABI bump → `uni-plugin` 1.7.0; #4 `IndexProbeExec` bridge — additive `register_index_handle` host API → `uni-plugin` 1.7.1; #5 planner identifier-resolution hook for `ReplacementScanProvider` (procedure / function / label sites + per-session `replacement_scans_enabled` gate; consumer-side change in `uni-query` + `uni`); #6 virtual label-id / edge-type-id allocation — `MATCH (n:External) RETURN n.foo` and `MATCH ()-[r:VirtualRel]->()` lower end-to-end via new `CatalogVertexScanExec` / `CatalogEdgeScanExec`; CREATE/MERGE rejection on virtual labels; additive `register_virtual_label` / `_edge_type` on `PluginRegistry` | **Batch 3 (M5c.2 + M5c.3 + M5c.4) ✅ landed 2026-05-24 — `uni-plugin` 1.8.0**: V2 `(graphRef, config)` adapter discriminating by `args[0]` JSON shape (Map → V2; Array → legacy + one-shot tracing deprecation warning); `ProjectionInput::Native` translates to legacy positional vec; `ProjectionInput::Cypher` runs `QueryProcedureHost::execute_inner_query` (read-only inner Executor with mirrored L0 visibility) → `GraphProjection::from_rows` → `AlgoProcedure::execute_with_projection` (default-impl errors; overridden in `GenericAlgoProcedure` for all 36 algos); `ProjectionInput::Named` looks up `ProjectionStore` (`crates/uni-query/src/projection_store.rs`, keyed on `Arc<StorageManager>` pointer identity); 4 new `uni.graph.*` procedures in `crates/uni-query/src/procedures_plugin/graph.rs`; `PluginRegistry` procedures arity-overloaded (`procedure_with_arity` / `procedure_overloads`); `include_reverse` defaults to `true` in both Native and Cypher (PageRank/Louvain/WCC need in-neighbors). Tests: 14 new (`crates/uni/tests/algorithm_graph_ref_native.rs` + `named_projection.rs`). | **Batch 3 follow-up ✅ landed 2026-05-24** (no `uni-plugin` ABI bump): **M5f** — `TriggerRouter` + `MutationEvents` in `crates/uni/src/api/triggers.rs`; dispatch wired at before/after sites in `Transaction::commit`; `UniError::TriggerRejected` propagates `Synchronous` rejects; `Async` / `EventualConsistency` triggers spawn on the tokio runtime; 6 integration tests + 3 unit tests pass. **Final follow-up bundle ✅ landed 2026-05-25** (no `uni-plugin` ABI bump): **M5g** ephemeral entities via `Vid`/`Eid` high-bit reservation, `host.allocate_transient_id()`, write-path gates, `uni.create.vNode`/`vEdge` procedures (11 tests; commit `6ee91a26a`); **M5e** legacy hook bridge sugar — `BuiltinHookPlugin` + `Uni::add_plugin` + commit-phase registry dispatch (3 tests; commit `68b4fa0e6`); **M5h** `LanceFilterPushdown` marker for Lance-backed `TableProvider`s (1 test; commit `df285371d`); **M5d** generic `TypedRgaProvider<T>` + `RgaElement` trait, with built-in `i64` / `f64` impls (3 tests; commit `03030a7cd`). **Remaining**: M5c.5 intentionally deferred to next release boundary (legacy 5-arg shim deletion — gated by §1.2 deprecation window). Deferred Batch 2 fragments (intentional MVP cut): multi-label MATCH mixing virtual+native, DELETE/SET on virtual labels, native↔virtual joins in mid-pattern traverse. | ✅ **complete** |
| M6a       | full (loader e2e + aggregate/procedure ABI + auth/authz/connector host consultation; example-extism-geo full Arrow-IPC `invoke` ships; 53 tests in `uni-plugin-extism` incl. 2 real-component e2e tests + 9 admin-bucket integration) | none — M6 deferred-followup completed all gaps | ✅ complete |
| M6b       | full (WIT worlds + wasmtime instantiate + per-major Linker + pool prewarm + `Uni::load_wasm_component` + aggregate/procedure adapters; example-wasm-geo full Arrow-IPC `invoke-scalar` ships via `wit_bindgen::generate!`; 12 `uni-plugin-wasm` tests incl. 2 real-component e2e tests + 18 shared = 30 wasm-runtime tests + 1 cross-ABI parity test) | none | ✅ complete |
| M7        | substantive — phases 1–11 (Cargo / lib / error / host_fns / engine / manifest / wire_translate / dynamic_bridge / runtime / adapter (row+vec) / adapter_aggregate / adapter_procedure / loader / columns / host_fn_impls + Uni::load_rhai_plugin + examples/example-rhai-geo + db.load_rhai_plugin PyO3 + CLI install dispatch + m7_rhai_cross_loader_parity); 37 crate tests + Uni e2e + Python binding | full-stack soak test once example WASM artifacts rebuilt for parity matrix update | ✅ substantive |
| M8        | scaffolding | PyO3 Arrow C Data Interface | ▶ scaffolding |
| M9        | full (6 declare* procs + `declareFunction` end-to-end + JsonFile persistence + reactivation + cycle/cascade/shadow; 20 tests) | none in M9 scope — `_DeclaredPlugin` system-label persistence + `declareProcedure`/`declareTrigger` body execution routed to M11 (deliverable #8) | ✅ complete |
| M10       | substantive | per-kind reload discipline + multi-major linker | ✅ substantive |
| M11       | Phases A+B+C complete + Batch-2 follow-ups (2026-05-26): write-enabled `execute_inner_query` + `SystemLabelPersistence` (dual-write JSON sidecar + `_DeclaredPlugin` graph nodes via `LazyCypherSink`) + `SyntheticProcedurePlugin` + `CypherProcedureSynthesizer` + `ProcedureBodySynthesizer` trait + canonical `Schedule::next_after` + `Scheduler::tick_at` + `SchedulerPersistence` trait + `SystemLabelSchedulerPersistence` (durable, dual-write JSON sidecar + `_BackgroundJob` graph nodes) + tokio `SchedulerHost::spawn` wired into `Uni::build` + `JobHost` trait with `compact_storage` + `execute_write_cypher` hooks + `SchedulerJobHost` (Weak<UniInner>) + real `CompactionJob` + real `TtlSweepJob` bodies + 6 `uni.periodic.*` procedures (schedule/cancel/list/submit/iterate/commit) + `Uni::periodic_*` Rust API + capability gate for `declareProcedure WRITE` (`Capability::ProcedureWrites` + `Principal::capabilities`) + circuit-breaker plumbing + OTel `init_otel_subscriber`. | Phase D (secrets WIT membrane) + Phase E (CDC runtime + Defer-queue persistence) explicitly deferred to later milestones. `statistics_refresh` job stubbed pending planner-statistics API. `uni.periodic.iterate` is v1 single-pass (driver-loop variant pending). | ✅ **A+B+C complete + follow-ups** |
| M12       | initial     | CLI + OCI + Python bindings + perf bench | ▶ partial |

The "done so far" column reflects testable code delivered in the worktree; "remaining" is the cutover work enumerated in §4 per-milestone subsections.

A substantial fraction of the planned scope has landed as tested, clippy-clean code. **M0–M7, M9, and M11 (Phases A+B+C) are complete or substantive; M10 is substantive.** Remaining work fans out across M7-followup (wall-clock deadline driver, real HTTP/KMS/Secret host fn impls, `uni.query` host fn, `proptest` boundary coverage), M8 (PyO3 Arrow bridge), M10 (per-kind reload), M11 follow-ups (Phase D secrets WIT membrane + Phase E CDC runtime + Defer-queue persistence; built-in job `execute()` bodies; durable scheduler persistence; Cypher-MERGE backend swap for `SystemLabelPersistence`), M12 (CLI/OCI/bindings/bench). The per-session driver is `docs/plans/REMAINING_WORK.md`.

---

## 11. Open Scheduling Questions

These are decisions not made by this plan; they need a separate call with the team:

### 11.1 Concurrency with other uni-db work

The proposal touches `uni-query`, `uni-store`, `uni-crdt`, `uni-algo`, `uni`, and the Python bindings. If parallel feature work is happening on those crates, rebase friction will be substantial during M3, M4, M5. Recommendation: declare a freeze on non-plugin-fw changes to `uni-query/src/query/df_graph/` and `executor/` during M3+M4.

### 11.2 Public API stability commitments

When do we declare `uni-plugin` API stable enough to commit to semver? Recommended: after M5 lands (all native traits exercised end-to-end). Before M5 the trait surface may iterate; after M5 it gets a 1.0.0 tag.

### 11.3 OCI registry choice

§4.4 of the SOTA doc identifies OCI as the long-term distribution target. Which specific registry does uni-db publish its built-in WASM artifacts to for the v1.0 release — Docker Hub, GHCR, an org-specific GAR? This affects M12's tooling.

### 11.4 v1.0 release readiness criterion

Beyond the 22 proposal acceptance criteria, what soak workload do we want exercised before tagging v1.0? Recommendation: at minimum, internal use on the team's heaviest existing workload (the ERP project mentioned in `MEMORY.md`).

### 11.5 Documentation publishing

The plugin-author guide (M12 deliverable) needs a home. mdBook on GitHub Pages? Docusaurus on a separate domain? Inline rustdoc? Recommendation: mdBook + crates.io rustdoc, cross-linked.

### 11.6 Deprecation of `CustomFunctionRegistry`

M2 keeps `CustomFunctionRegistry` as a backward-compat facade. When in the v1.x lifecycle do we deprecate it in favor of the direct `Uni::add_plugin` API? Recommendation: deprecation notice in v1.1, removal in v2.0 (which we do not plan).

---

## 12. Pre-Milestone Checklist

Before M0 begins, confirm:

- [ ] All readers of this plan have read `docs/proposals/plugin_framework.md` and `docs/research/plugin_frameworks_sota.md`.
- [ ] The team agrees with the principles in §1 (especially §1.3 — built-ins through the plugin path only).
- [ ] Engineer assignments are made for the critical-path lane (see §6).
- [ ] CI capacity is verified — running the full workspace test suite + bench suite on every PR will roughly 2× CI minutes consumed. Budget approved.
- [ ] Storage for benchmark baselines (`bench/baselines/*.json`) is configured.
- [ ] The 22 proposal acceptance criteria are reviewed and any contested ones resolved before M0.
- [ ] The pinned `main` SHA is recorded in this document below.

**Pinned base SHA for M0:** (to be filled at M0 kickoff)

```
__________________________________________________________________
```

---

## 13. Living Document

This plan is updated at each milestone boundary with:
- New risks discovered.
- Scope clarifications resolved during implementation.
- Cross-references to merged PRs.

A retrospective at each milestone boundary updates the §10 effort summary with actuals and the §7 risk register with realized vs. mitigated risks.

---

**End of implementation plan.**
