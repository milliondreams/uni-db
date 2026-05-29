# Black Book — Plugin Framework Documentation TODO

**Source docs:** `docs/proposals/plugin_framework.md` (3216 lines, §19 scorecard 19 ✅ / 6 ▶ / 4 ⏳ as of 2026-05-27) + `docs/plans/plugin_framework_implementation.md` (1480 lines, M0–M12 milestones).

**Target:** `docs/UNI_BLACK_BOOK.md` (currently 5168 lines, 16 Parts + Appendices).

**Coverage policy:** Document only what's shipped / verified (✅ in proposal §19 + tests-green). Use a visible status callout for partially-in-place items (▶) and explicitly defer pending items (⏳) to the eventual M12 / Phase D passes.

---

## Pass 1 — Add a new Part XVII (Plugin Framework). This document's primary work.

Insert between Part XVI (Forks) and the Appendices. Estimated 800–1100 lines, reference-grade depth modelled on Parts V (Storage Engine) / VIII (Cypher Extensions).

### XVII.1 What Is the Plugin Framework?

- 1-paragraph definition: a single registry-backed extensibility layer that replaced ~5 separate ad-hoc registries (CustomFunctionRegistry, FoldAggKind, hardcoded procedure dispatch, AlgorithmRegistry, hardcoded index/storage dispatch).
- The registration shape: every extension implements one of 25 surface traits, gets wrapped in a `PluginManifest` + `PluginRegistrar`, and ships through `PluginRegistry`.
- Cross-reference to `crates/uni-plugin/`, the foundation crate.

### XVII.2 Why Plugins?

- One paragraph each on: (a) closing closed-enum dispatch (mechanical: zero hits for `match name { "MIN" | "MAX" | ... }`), (b) host-language extensibility (Python data scientists, Rhai for ops scripts, Rust for performance, WASM for sandboxed third parties), (c) capability gating as a security boundary.
- The "20% perf win" claim from criterion #9 (NativeArrowUdf declares primitive return types directly, avoiding LargeBinary round-trip).

### XVII.3 Capability Model

- `CapabilitySet` — the type that gates what a plugin can do.
- The two axes:
  - **Surface capabilities** (what kind of registration the plugin makes): `ScalarFn`, `AggregateFn`, `Procedure`, `ProcedureWrites`, `Algorithm`, `Storage`, `Index`, `Crdt`, `Hook`, `Trigger`, `Auth`, `Authz`, `Connector`, `Cdc`, `Catalog`, `ReplacementScan`, `OptimizerRule`, `Pushdown`, `Type`, `Collation`.
  - **Resource capabilities** (what host services the plugin can call): `Filesystem`, `Network`, `Kms`, `HostQuery`, `Secret { ids: Vec<String> }`, `FuelPerCall(N)`, `WallClockMillisPerCall(N)`, `MemoryPagesMax(N)`.
- Intersection rules: declared capabilities ∩ host grants = effective capabilities; denied capabilities are observable for telemetry.
- Code excerpt: a manifest declaring `[Filesystem, Network]` against a host granting only `[Filesystem]` → `effective = [Filesystem]`, `denied = [Network]`.

### XVII.4 Plugin Manifest

- The canonical shape: `id`, `version`, `abi`, `capabilities`, `determinism`, `description`, `fuel_per_call`, `memory_max_pages`, `timeout_ms`.
- ABI range parser: `^1`, `^2`, `>=1, <99` (see `crates/uni-plugin/src/abi_range.rs::AbiRange::parse`).
- **Verification & signing** — Blake3 hash pinning (`verify_hash_pin`), Ed25519 signed-manifest verification (real, default-on behind feature gate).
- Per-loader manifest extensions:
  - Component Model: `manifest-json` export, Arrow IPC framing.
  - Extism: `manifest` export, JSON.
  - Rhai: `uni_manifest()` function returning a Rhai Map.
  - PyO3: `@db.scalar_fn` decorator collects manifest fields.

### XVII.5 Five Loaders — the Loader Matrix

A reference table comparing the 5 loaders along the axes that matter:

| Axis | Rust | CM (wasmtime) | Extism | Rhai | PyO3 |
|---|---|---|---|---|---|
| Host crate | `uni-plugin-builtin` / `uni-plugin-apoc-core` | `uni-plugin-wasm` | `uni-plugin-extism` | `uni-plugin-rhai` | `uni-plugin-pyo3` |
| Sandbox | none (trusted) | wasmtime + WIT | extism-sdk | Rhai engine | none (trusted) |
| ABI | Native trait | WIT bindings | Arrow IPC over linear memory | Dynamic via rhai::Engine | PyCapsule (Arrow C Data Interface) |
| Surfaces shipped | All 25 | Scalar / Aggregate / Procedure | Scalar / Aggregate / Procedure | Scalar (row + vectorized) / Aggregate / Procedure | Scalar (row + vectorized) / Aggregate / Procedure |
| Capability gating | compile-time | structural (linker omits) | runtime (HostFnRegistry filter) | runtime (engine factory) | manifest |
| Parity tier | reference | byte-identical | byte-identical | ≤ 4 ULP | ≤ 4 ULP |
| Reload | full | epoch-fenced | epoch-fenced | full | session-scope unregister |
| Verifying parity test | (reference) | `m6_cross_abi_parity.rs::cross_abi_haversine_results_match` | (same) | `m7_rhai_cross_loader_parity.rs` | `m8_pyo3_cross_loader_parity.rs` |

### XVII.6 Loading a Plugin (Host API)

#### Rust

- `Uni::add_plugin(plugin)` — for in-process plugins (the typed path most internal code uses).
- `Uni::load_wasm_component(loader, bytes, grants, plugin_id)` — wasmtime Component Model.
- `Uni::load_wasm_extism(loader, bytes, grants, plugin_id)` — Extism.
- `Uni::load_rhai_plugin(loader, source, grants, plugin_id)` — Rhai (source file).
- `Uni::load_python_plugin(loader, source, grants, plugin_id)` — PyO3 (module source / file).
- All five return a `LoadOutcome { qnames_registered, denied_capabilities, plugin_id }`.

#### Python

- `db.load_wasm_component(...)`, `db.load_wasm_extism(...)`, `db.load_rhai_plugin(...)`, `db.load_python_plugin(...)`.
- Decorator-driven authoring path: `@db.scalar_fn`, `@db.aggregate`, `@db.procedure` — collects metadata into a sink, then `db.commit_python_plugins()` registers the batch.

#### CLI

- `uni plugin install foo.rhai` — Rhai install dispatch shipped today; WASM / OCI / Extism Hub branches deferred to M12 (status: ⏳ — call this out explicitly).

### XVII.7 Authoring Plugins — the geo.haversine reference example

Walk the same conceptual scalar fn (great-circle distance between two points) through every loader. Use existing artifacts under `examples/`:
- `examples/example-wasm-geo/` — Component Model with `cargo component`.
- `examples/example-extism-geo/` — Extism with `wasm32-unknown-unknown`.
- `examples/example-rhai-geo/` — Rhai source script.
- `crates/uni-plugin-pyo3/tests/conformance.rs` — PyO3 reference.

For each loader, show:
- Manifest declaration.
- Function signature (`(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64`).
- Implementation excerpt (5–15 lines).
- Build command.
- Load command (Rust + Python).

### XVII.8 Surface Traits Reference (the 25)

Brief subsection per surface trait, with: file:line of trait definition, what concrete impls ship today, what tests verify it.

Order them by user relevance:
1. `ScalarPluginFn` (most common)
2. `AggregatePluginFn`
3. `LocyAggregate` (Locy-flavored monotone aggregates with `Semilattice` metadata)
4. `ProcedurePlugin`
5. `Storage` / `StorageBackend` (M5a — async, plus Lance fork wiring)
6. `IndexKindProvider`
7. `CrdtKindProvider`
8. `AlgorithmProvider`
9. `SessionHook` (phased: parse / execute / commit; legacy 4-method bridge in `LegacyHookAdapter`)
10. `TriggerPlugin` (phases + outcomes — see XVII.12)
11. `BackgroundJobProvider` (M11 — see XVII.11)
12. `AuthProvider`
13. `AuthzPolicy`
14. `ConnectorProvider`
15. `CdcOutputProvider`
16. `CatalogProvider`
17. `ReplacementScanProvider`
18. `OptimizerRuleProvider`
19. `SupportsFilterPushdown` / `SupportsProjectionPushdown` / `SupportsLimitPushdown` / `SupportsTopNPushdown` / `SupportsAggregatePushdown` (marker traits)
20. `TypePlugin` (logical types: uri / geo.point / email / ipv4 / ipv6)
21. `Collation`

For each, one-paragraph "what it does, when to implement one, what concrete impls ship".

### XVII.9 The PluginRegistry — Read Side

- Wait-free reads via `arc-swap` (Arc<dyn Fn> handed out per lookup).
- The `PluginRegistry::resolve_*` methods: `resolve_scalar_fn`, `resolve_aggregate`, `resolve_procedure`, `resolve_storage_backend`, `resolve_index_kind`, `resolve_crdt`, `resolve_authz_policy`, `resolve_auth_provider`, `iter_triggers`, etc.
- How call sites consult it: `procedure_call.rs::execute_procedure` collapsed to `if registry.resolve(...) { invoke } else { tck_mock_fallback }` (the M4 cutover, mechanical proof that closed-enum dispatch is gone).

### XVII.10 Declared Plugins (`uni.plugin.declare*`)

The meta-plugin path — `apoc.custom` analogue.

- `uni.plugin.declareFunction(qname, signature, body_kind, body)` — synthesize a `ScalarPluginFn` from Cypher / Rhai / Python source.
- `uni.plugin.declareProcedure(qname, signature, body_kind, body)` — synthesize a `ProcedurePlugin`; gated on `Capability::ProcedureWrites` for `WRITE`-mode procedures.
- `uni.plugin.declareAggregate(qname, signature, init_body, accumulate_body, merge_body, finalize_body)`.
- `uni.plugin.declareTrigger(qname, label, phase, predicate, body)`.
- `uni.plugin.listDeclared()` / `uni.plugin.dropDeclared(qname)` — admin.
- `DeclaredPluginStore` semantics: dependency-missing detection, cycle detection, drop-with-dependents protection.
- **Persistence**: `_DeclaredPlugin` system label + JSON sidecar dual-write via `LazyCypherSink` (see `crates/uni/src/persistence.rs`); declarations survive restart.

### XVII.11 Background Jobs & Scheduler (M11)

- `Schedule` enum: `Once(SystemTime)`, `Periodic(Duration)`, `Cron(CronExpr)`, `Manual`.
- `BackgroundJobProvider` trait — `execute(ctx) -> JobOutcome { Done, Reschedule(Duration), Failed(Reason) }`.
- `SchedulerHost` driver — tokio-backed, polls every 100 ms, dispatches via `spawn_blocking`.
- Cypher API: `uni.periodic.schedule(name, cron, qname, params)`, `uni.periodic.cancel(name)`, `uni.periodic.list()`, `uni.periodic.submit(name, cypher, params)`, `uni.periodic.iterate(query, mutating_query, options)`, `uni.periodic.commit()`.
- Rust API: `Uni::periodic_schedule(...)`, `Uni::periodic_cancel(...)`, `Uni::periodic_list()`.
- Built-in jobs: `uni.system.ttl_sweep` (TTL deletion via `MATCH (n) WHERE n.__ttl < timestamp() DETACH DELETE n`), `uni.system.compaction` (`StorageManager::compact()`), `uni.system.statistics_refresh` (stub; deferred).
- **Persistence**: `SystemLabelSchedulerPersistence` — `<data_path>/_system/background_jobs.json` + `_BackgroundJob` graph nodes.
- **CircuitBreaker** — 10-fail threshold opens, 30 s cooldown, half-open probe.

### XVII.12 Triggers (M5f)

- `TriggerPlugin` trait — `fire(ctx, events: RecordBatch) -> TriggerOutcome`.
- Phases:
  - `BeforeMutation` — fires before the writer lock; `Reject` aborts the commit.
  - `AfterCommit` — fires after the writer lock releases.
  - `Async` — spawned on the runtime, never blocks the writer.
  - `EventualConsistency` — best-effort, durable retry queue (Defer outcome).
- `TriggerOutcome` — `Allow`, `Reject(reason)`, `Defer(retry_after)`.
- `MutationEvents` schema (§4.18) — `event_kind` (NODE_CREATE / NODE_UPDATE / NODE_DELETE / EDGE_*), `vid_or_eid`, `label_or_type`, `old_value`, `new_properties`, `old_properties`.
- Predicate compile path — `properties_new` / `properties_old` LargeBinary bags, `n.foo` / `old.foo` AST rewrite.

### XVII.13 Hot Reload & Multi-Version ABI (M10)

- The arc-swap invariant: a captured `Arc<dyn ScalarFn>` returns v1 output while a post-reload registry lookup returns v2 — long-running queries finish on the version they started with.
- `PluginLifecycle` state machine: `Loaded → Active → Draining → Drained → Removed`.
- `EpochFencedReload` driver — `begin_drain` / `wait_for_drain` / `finalize`.
- `MultiVersionLinker` (wasmtime) — per-major linker map keyed by `(major, caps_signature)`.
- Per-kind reload discipline — storage backend / index kind / CRDT each have a dedicated reload path tested in `reload_*.rs` files.

### XVII.14 Observability

- `init_otel_subscriber(cfg) -> OtelGuard` — opt-in OTLP/gRPC exporter built on `opentelemetry 0.27` + `tracing-opentelemetry 0.28`.
- `host-log` host import — always available across all loaders; routes plugin tracing into the host's `tracing` macros at the matching level.
- `InvocationKind` + `record_invocation` — per-plugin invocation telemetry.
- `host.span_*` WIT imports — deferred to Phase D (status: ▶ — call out explicitly).

### XVII.15 The Conformance Suite

- `uni-plugin-conformance` ships a 6-probe suite that every loader runs:
  1. Manifest round-trip.
  2. Scalar call with 1 row.
  3. Scalar call with batch of 1000 rows.
  4. Error surface check.
  5. Aggregate state round-trip.
  6. Procedure stream cancellation.
- Probe stability — `conformance_probes_have_stable_ids` test guarantees probe IDs don't change across versions.
- Each loader has its own `loads_and_invokes_geo_haversine_end_to_end` test that runs the full suite.

### XVII.16 Plugin Best Practices

- Prefer the Rust path for performance-critical scalars (no sandbox tax).
- Use Rhai / PyO3 for ops scripts and data-science notebooks.
- Use CM / Extism for untrusted code or polyglot ecosystems.
- Mark deterministic functions as `determinism: "deterministic"` so the planner can memoize.
- Always declare the minimum capability set — `[ScalarFn]` is enough for pure-compute fns.
- Reach for `Capability::FuelPerCall(N)` to bound runaway scripts (Rhai `set_max_operations`; CM `wasmtime` epoch+fuel).
- Use the `@db.scalar_fn(vectorized=True)` PyO3 mode for batch workloads — one GIL crossing per RecordBatch instead of per row.

### XVII.17 Plugin Anti-Patterns

- Don't reach for full Filesystem / Network when a `host-query` callback suffices.
- Don't synthesize per-row Python objects inside a `vectorized=False` scalar; use vectorized mode or move to Rust.
- Don't write a CM plugin that holds linear-memory pointers across `invoke-batch` calls — the host owns those buffers between calls.
- Don't ship a plugin without a manifest signature / hash pin in production.
- Don't declare a capability you don't use — the conformance suite checks unused-capability hygiene.

### XVII.18 What's Not in the Plugin Framework (Current Scope)

Status callouts for the remaining ⏳ items per the §19 scorecard:
- `uni plugin install/list/grant/remove/info/reload/verify` CLI — M12 (pending).
- `oci://...` install — M12 (pending).
- `extism://...` Hub install — M12 (pending).
- Capability-gated `host-fs.read` body for CM (the structural gating is in place; the host fn body itself is deferred).
- Secrets WIT membrane (`host-secrets`) — Phase D (pending).
- `host.span_*` WIT imports for plugin-side OTel propagation — Phase D (pending).
- APOC long-tail (one real procedure per remaining namespace) — open-ended.

---

## Pass 2 — Ripple updates to existing Parts. Do after Pass 1 lands.

### Part I — Executive Summary & Vision
- **§ Key Differentiators (line 50)** — add an 11th bullet:
  > 11. **Polyglot Extensibility** — five plugin loaders (Rust, WASM Component Model, Extism, Rhai, PyO3) share one `PluginRegistry`. A scalar function authored once runs byte-identically across CM + Extism; Rhai and PyO3 agree to ≤ 4 ULP. Capability-gated sandboxing for untrusted code.
- **§ Target Use Cases (line 63)** — add a use case bullet on "Embed Uni in a data-science notebook with `@db.scalar_fn` decorators."

### Part II — Architecture Deep Dive
- **§ Layered Design (line 116)** — add a layer between "Query Layer" and "Graph Runtime": "Extensibility Layer (`uni-plugin` + 8 loader crates)."
- **§ Workspace Structure (line 182)** — add the 9 plugin-fw crates (`uni-plugin`, `uni-plugin-builtin`, `uni-plugin-apoc-core`, `uni-plugin-wasm`, `uni-plugin-wasm-rt`, `uni-plugin-extism`, `uni-plugin-rhai`, `uni-plugin-pyo3`, `uni-plugin-custom`, `uni-plugin-conformance`).
- **§ Crate Dependency Graph (line 202)** — add `uni-plugin` as a foundation node consumed by `uni-query`, `uni`, and every `*-plugin-*` crate.
- **§ Read Path End-to-End (line 285)** — add the registry consultation step ("the planner consults `PluginRegistry::resolve_scalar_fn` for unknown UDF names").

### Part VIII — Cypher Extensions & Procedures
- **Opening section (line 2155)** — add a paragraph: "Every procedure documented in this Part is now registered through the plugin framework — see Part XVII for the trait surface and authoring path."
- **§ Admin Procedures (line 2546)** — add subsections for `uni.plugin.declareFunction/Procedure/Aggregate/Trigger` + `uni.plugin.listDeclared` + `uni.plugin.dropDeclared`.
- **New § Background Job Procedures** — `uni.periodic.{schedule, cancel, list, submit, iterate, commit}` with examples.

### Part XI — Transactions, Sessions & Concurrency
- **New § Triggers** — short user-facing summary of `TriggerPlugin` semantics (phases / outcomes / commit interaction), cross-reference Part XVII.12 for the trait surface.

### Part XIV — Python Bindings
- **New § Authoring Python Plugins** — under `uni-db` section, document `@db.scalar_fn`, `@db.aggregate`, `@db.procedure` decorators with one runnable example each.
- **New subsection in § Database Connection** — document `db.load_python_plugin()`, `db.load_rhai_plugin()`, `db.load_wasm_extism()`, `db.load_wasm_component()` host APIs.
- **§ Sessions** — document session-scoped plugin registration: `session.load_python_plugin()` shadows the global registry for the session's lifetime.

### Part XV — Configuration Reference
- **New § PluginConfig** — `PluginConfig` (if it ends up a discrete config struct) covering:
  - default host grants (e.g. enable `Filesystem` for trusted local builds, disable in production).
  - signature verification policy (require_signed: bool).
  - hash pin policy.
  - default `fuel_per_call`, `memory_max_pages`, `timeout_ms`.

### Appendix A — CLI Reference
- **§ Commands** — add `uni plugin install/list/grant/remove/info/reload/verify` once M12 lands; today document only `uni plugin install foo.rhai` (the shipped Rhai install dispatch).

### Appendix D — Glossary
Add the following terms (one-line definition each):
- ABI Major
- Background Job
- Capability / CapabilitySet
- Conformance Suite
- Declared Plugin
- Effective Capabilities
- Epoch-Fenced Reload
- Hot Reload
- Loader (Plugin)
- Manifest (Plugin)
- Multi-Version ABI
- Plugin
- PluginRegistrar
- PluginRegistry
- Resource Capability
- Surface Capability
- Surface Trait
- Synthesized Plugin
- Trigger

---

## Pass 3 — Add runnable examples. Do after Pass 2.

Land alongside the existing `examples/example-{wasm,extism,rhai}-geo/` directories.

### 3.1 Python plugin example

New: `examples/example-python-geo/`
- `pyproject.toml` declaring `uni-db` dep.
- `geo_plugin.py` — `@db.scalar_fn` haversine + a `@db.procedure` that batches lat/lon pairs.
- `README.md` showing how to load via Rust (`Uni::load_python_plugin`) and Python (`db.load_python_plugin`).
- One conformance probe pass under the example's `tests/` dir.

### 3.2 Trigger example

New: `examples/example-trigger-audit/`
- `audit_trigger.rs` — a `TriggerPlugin` impl that mirrors every `NODE_CREATE` / `NODE_UPDATE` into an `_AuditLog` system label.
- Demonstrates: phase choice (`AfterCommit`), predicate compile, `MutationEvents` schema consumption.
- `README.md` walks the end-to-end load + verify-on-write flow.

### 3.3 Background job example

New: `examples/example-bgjob-cleanup/`
- A `BackgroundJobProvider` that prunes nodes with `__expires_at < now` every 5 minutes.
- Shows `Schedule::Cron("0 */5 * * * *")` registration via Rust API + `uni.periodic.schedule` Cypher API.
- `README.md` includes the circuit-breaker observation script.

### 3.4 Declared plugin example

New: `examples/example-declared-functions/`
- A SQL session file showing `CALL uni.plugin.declareFunction('myco.discount', '(price: f64, pct: f64) -> f64', 'cypher', 'RETURN price * (1.0 - pct)')`.
- Demonstrates: cycle detection (try to declare a function that calls itself), cascade-drop protection (try to drop a function another declared function depends on).
- `README.md` includes restart-survival verification.

### 3.5 CM plugin with capability gating

Extend `examples/example-wasm-geo/` with a second variant: `examples/example-wasm-geo-restricted/`
- Declares `[Filesystem, ScalarFn]` in its manifest.
- Host load with grants = `[ScalarFn]` only.
- Shows the `denied_capabilities` field in the `LoadOutcome`.
- Once the M6 CM `host-fs.read` body lands, demonstrate "grant → reads file; deny → instantiate-time error."

### 3.6 Conformance-runner example

New: `examples/example-conformance-runner/`
- A small Rust binary that takes a plugin path + loader hint and runs the full 6-probe `uni-plugin-conformance` suite, printing per-probe pass/fail.
- Acts as a "lint your plugin before shipping it" tool.
- `README.md` shows running it against each of the five `example-*-geo` artifacts.

---

## Execution order

1. **Pass 1 first**, in a single PR (~800–1100 lines added to Black Book). This is the meaty content the user asked for.
2. **Pass 2** as a second PR, after Pass 1 lands. Ripple updates touch ~12 sites across existing Parts.
3. **Pass 3** as a third PR (or split into one per example). Each example is independently shippable; no need to land them as a unit.

Per the §19 scorecard refresh, document only ✅ shipped items as user-facing features; mark ▶ items as "in place; subject to refinement"; mark ⏳ items as explicit deferrals so the reader doesn't expect them.

---

## Open questions to resolve before Pass 1 starts

1. Where exactly should Part XVII sit — after Part XVI (Forks) or after Part X (Locy)? **Recommendation:** after Part XVI, so the reading order goes runtime → forks → extensibility, matching the proposal's framing.
2. Should the loader matrix table (§XVII.5) include all 25 surface traits per loader or only the 3 currently shipping ones (scalar / aggregate / procedure)? **Recommendation:** show what ships and add a footnote that the Rust path covers all 25 already.
3. Do we want to inline the Mermaid architecture diagram from the proposal §3 into the Black Book or keep it text-only? **Recommendation:** inline — the Black Book already uses Mermaid in Parts II / VI / XIII.
4. Should `examples/example-*-geo` be cross-referenced by relative repo path (good for local readers) or by GitHub URL (good for the published HTML book)? **Recommendation:** relative path with a note in Part I about cloning the repo.
