# Plugin Framework — Remaining Work Handoff

**As of:** session ending 2026-05-24, 52 commits delivered (M4 fully landed + test-binary consolidation + CypherValue codec fix).
**Use this:** open this doc + the relevant milestone section of
`plugin_framework_implementation.md` at the start of every focused
session, work one section to completion, mark it done, move on.

## Session-by-session priority order

The recommended order is in `plugin_framework_implementation.md` §3
(critical-path DAG). This doc gives the **next concrete action** for
each remaining milestone so a future engineer can start without
re-reading the full plan.

---

## M3 — `FoldAggKind` enum literal deletion — ✅ COMPLETE

**Status:** delivered in the M3 finalization commit. `FoldAggKind` is
gone from `crates/`; `FoldBinding` carries `name: SmolStr` plus
`aggregate: Arc<dyn LocyAggregate>` resolved at planner time from
`HybridPhysicalPlanner.plugin_registry` (defaulting to
`default_locy_plugin_registry()` pre-populated with the built-ins
from `uni-plugin-builtin`). The hardcoded `aggregate_for_kind` match
in `locy_fold.rs` is deleted.

**Acceptance:** `grep -rn "FoldAggKind" crates/` returns zero hits.
Locy TCK (434 tests) and uni-query (837 tests) pass.

**Follow-up — ✅ COMPLETE.** Compile-time rejection of non-monotone
aggregates in recursive strata is now enforced via `Semilattice.monotone_join`:

- `uni-locy::compiler::typecheck` takes a `MonotonicityOracle` predicate; the
  default oracle accepts the M-prefixed canonical names; hosts can supply a
  registry-backed closure (`compile_with_oracle`).
- `uni-query::LocyPlanBuilder::build_clause` independently validates each
  recursive clause using `is_monotonic_aggregate(&plugin_registry, name)`
  (defense in depth — covers callers that bypass uni-locy typecheck).
- New `MSumAgg` (`uni-plugin-builtin`) carries
  `Semilattice { monotone_join: true, has_top: false, .. }`; `MSUM` resolves
  to it (was an alias for `SumAgg` which is `NON_MONOTONE`).
- TCK: `crates/uni-locy-tck/tck/features/compile/FoldMonotonicity.feature`
  ships 6 scenarios (SUM reject, AVG reject, MMAX/MSUM/MNOR accept, SUM in
  non-recursive accept). TCK total 434 → 440, all green.

---

## M4 — Procedure migration: ✅ COMPLETE

**Status:** ✅ **complete**. Every built-in procedure now flows through the plugin dispatch path. **83 procedures total** (1 builtin + 38 APOC + 5 schema + 36 algo + 3 search; +1 alias for `uni.schema.relationshipTypes`). The hardcoded match arms in `procedure_call.rs::execute_procedure` are deleted; the function collapses to `if plugin_registry.resolve(...) { invoke } else { tck_mock_fallback }`. Net delta: ~1400 lines deleted from `procedure_call.rs` (2309 → 922).

Landed in four commits:
1. `60d31038` — M4 foundation: `ProcedureHost` trait + `QueryProcedureHost` snapshot bridge.
2. `ccb0a98a` — schema (5) + algo (32, now 36 after a registry change) namespace cutover via `uni-query::procedures_plugin`.
3. `ac856817` — vector/fts/search ports via helper-signature refactor (search bodies relocated to `crates/uni-query/src/query/df_graph/search_procedures.rs`; `QueryProcedureHost` extended with `property_manager` + per-request `target_properties` + `yield_items` + `expected_schema`; legacy match arms deleted).
4. `6057e285` — chore + bonus M4 follow-up: 237 → 9 test binaries via `autotests = false` + categorical `integration_*.rs` shims; fixed a pre-existing CypherValue codec mismatch in `plugin_adapter::ValueRowFn` (was JSON-encoding `LargeBinary` payload; downstream readers use the canonical tagged `cypher_value_codec`).

**Verification:** Cypher TCK 3969/3969 + Locy TCK 440/440. New `crates/uni/tests/m4_host_procedures_dispatch.rs` (9 tests) exercises every M4 namespace through the plugin path end-to-end.

**Single remaining M6 follow-up — capability gating:**

`execute_plugin_procedure` does not yet check `ctx.principal` against `signature.mode`. The `Principal` struct in `uni-plugin::traits::connector` has no `has(Capability)` method, and capabilities currently bind to plugins rather than principals. Building the principal-capability surface is M6 ABI-freeze scope; until then in-tree built-ins run unrestricted (matching today's behaviour).

**Delivered this session:**

- `uni_plugin::traits::procedure::ProcedureHost` — tiny marker trait
  (`Send + Sync + Any`) with a single `as_any` method.
- `ProcedureContext` grew `host: Option<&'a dyn ProcedureHost>`,
  `principal: Option<&'a Principal>`, and builder methods
  (`new`, `with_host`, `with_deadline`, `with_principal`). Backwards
  compatible — every existing `ProcedureContext::default()` caller in
  `uni-plugin-apoc-core` / `uni-plugin-builtin` keeps working because
  the new fields default to `None`.
- `QueryProcedureHost` (`crates/uni-query/src/query/executor/procedure_host.rs`)
  — owned-snapshot wrapper (storage / algo_registry / procedure_registry
  as `Arc`-clones), `'static`-friendly so `Any` downcasting works
  without unsafe.
- Both dispatch sites construct a `QueryProcedureHost` and attach it
  via `ProcedureContext::with_host`:
  - DataFusion path: `execute_plugin_procedure` gained a `graph_ctx`
    parameter (`procedure_call.rs:637`).
  - Simple-executor path: `execute_procedure` builds the host from
    `Arc<StorageManager>` + `Arc<AlgorithmRegistry>` + the optional
    procedure registry (`executor/procedure.rs:608`).

Verification: `cargo nextest run -p uni-plugin -p uni-plugin-builtin -p uni-plugin-apoc-core -p uni-query` — 1029/1029 pass; `cargo clippy -p uni-plugin -p uni-query --all-targets -- -D warnings` clean.

**Next actions (in dependency order):**

1. **Layering decision** — host-coupled procedure plugins (schema /
   vector / fts / search / algo) cannot live in `uni-plugin-builtin`
   because that crate only depends on `uni-plugin`. They need access
   to `StorageManager`, `AlgorithmRegistry`, etc. Two viable homes:
   - **`uni-query` itself** (recommended): new module
     `crates/uni-query/src/query/procedures_plugin/` registering via
     a function called by `uni::api::register_builtin_plugins`.
     Pragmatic; reuses all existing helpers.
   - **New crate `uni-plugin-host-procedures`** depending on
     `uni-plugin + uni-query + uni-store + uni-algo`. Cleaner separation
     but adds a crate.

2. **Algo adapter (Step 5 of the M4 plan)** — biggest LoC win.
   `AlgorithmProcedureAdapter` wraps each entry from
   `AlgorithmRegistry::all()` (32 algorithms) as a `ProcedurePlugin`.
   Adapter body translates `ProcedureContext` ⇒ algo `AlgoContext`
   exactly as `execute_algo_procedure` (procedure_call.rs:~1225) does
   today, then drives the algo's stream. One registration loop covers
   all 32; the `uni.algo.*` arm (procedure_call.rs:618) and the
   schema-inference arm (procedure_call.rs:264) can both be deleted.

3. **Schema procedures (Step 3)** — port the 5 in
   `procedure_call.rs::execute_schema_*` (~345 lines total). Smallest
   per-proc complexity. Read mode; capability `Procedure`.

4. **Search procedures (Step 4)** — port `execute_vector_query`,
   `execute_fts_query`, `execute_hybrid_search` (~742 lines combined).
   Heaviest: vector/fts hooks into `xervo_runtime` for auto-embedding;
   `uni.search` does RRF fusion. Extract the bodies into pure
   `pub(crate)` helpers in a new `df_graph/search_procedures.rs` so
   the plugin impls and (transitionally) the legacy arms can both call
   them.

5. **Delete the hardcoded match (Step 7)** — collapse
   `procedure_call.rs::execute_procedure` (lines 597-625) to the
   plugin-path lookup + legacy TCK-mock fallback. Delete each
   `execute_schema_*`, `execute_vector_query`, `execute_fts_query`,
   `execute_hybrid_search`, `execute_algo_procedure` once its plugin
   port lands.

6. **Schema inference (Step 6)** — replace the `name if name.starts_with("uni.algo.")`
   arm at `procedure_call.rs:264` with a single
   `procedure_registry.resolve_user_procedure(...).signature.yields`
   consult.

7. **Capability gating (Step 8)** — in `execute_plugin_procedure`,
   check `ctx.principal` against `signature.mode` before invoking. The
   current `Principal` (in `uni-plugin::traits::connector`) has no
   `has(Capability)` method — that surface needs to land first (it's
   on the M6 ABI freeze path per the proposal). Skip until then.

8. **Tests** — new `crates/uni-query/tests/procedure_dispatch.rs` with
   one test per ported namespace; Cypher TCK should stay green
   throughout.

The full plan with file-paths and code sketches is in
`/home/rohit/.claude/plans/let-us-plan-properly-streamed-tome.md`.

---

## M5 — per-impl depth (4–5 sessions, parallelizable)

### M5a — Real `LanceStorage` (1–2 sessions)

Replace the placeholder `LanceBackend` in
`crates/uni-plugin-builtin/src/storage.rs:128` with a real bridge
to `uni-store::lance::*`. The `StorageBackend::open(uri, options)`
constructs a `uni_store::StorageManager` and returns
`Arc<LanceStorageBridge>` where `LanceStorageBridge` wraps the
manager and implements the framework's `Storage` trait by routing
`read_batch` / `write_batch` / `list_tables` / `delete` to the
underlying vertex/edge/adjacency datasets.

**Layering:** add `uni-store` as a `uni-plugin-builtin` dep. This is
another pragmatic inversion (same direction as `uni-plugin-builtin`
already depending on `uni-plugin` from `uni-query`).

**Tests:** `crates/uni-store/tests/scheme_dispatch.rs` (new) — open
via `lance://` scheme, verify the lance backend serves reads/writes.

### M5b — `vector_knn` planner integration (1 session)

Today `LogicalPlan::VectorKnn` in `crates/uni-query/src/query/planner.rs:1978`
is a closed enum variant. The cutover routes it through
`IndexKindProvider::probe` instead. Steps:
1. Add `LogicalPlan::IndexProbe { kind: IndexKind, ... }` as the
   generalized form.
2. Update `apply_vector_knn_rewrite` (or equivalent) to construct
   `IndexProbe { kind: IndexKind::Vector, ... }` when it would have
   built `VectorKnn`.
3. The physical planner consults the framework's registered
   `IndexKindProvider::vector` (already shipped as
   `MemoryVectorIndex`) to actually execute.
4. Delete `LogicalPlan::VectorKnn`.

**Tests:** every existing vector-search test must pass unchanged.

### M5c — `uni-algo` migration + virtual / named projections (3–4 sessions)

Reshaped from a single 1–2 session port into 5 sub-phases — see
`plugin_framework_implementation.md` §M5c for full details. The major
addition is **GDS-style virtual (Cypher) and named projections**
(proposal §4.10.1–3): the P6 / uniko entity-co-occurrence pattern wants
algorithms to accept a Cypher subquery as input, which today forces
them to either persist derived edges (write amplification + staleness)
or build adjacency in Rust outside uni-algo (current `topics.rs`).

The fix moves projection materialisation to the **host side** — the
`AlgorithmProvider` trait keeps taking a pre-built `GraphProjection`
(unchanged from today), and the `uni.algo.*` plugin adapter resolves
one of three `ProjectionInput` variants (`Native | Cypher | Named`)
into a `GraphProjection` before invoking the algorithm. Algorithms stay
oblivious to projection origin.

**Sub-phases (in execution order):**

1. **M5c.1** — Wrap 32 algorithms as `AlgorithmProvider` plugins. No
   API change. `AlgorithmProcedure` adapter (from M4) resolves through
   the plugin registry instead of the static `AlgorithmRegistry`.
2. **M5c.2** — Switch procedure signatures to `(graphRef, config)`;
   `Native` variant only. New signature registered alongside the
   legacy 5-arg form (keyed on `(name, arity)`); legacy emits
   `DeprecationWarning`. Behaviour identical to today.
3. **M5c.3** — `Cypher` variant. Add `host.execute_query(cypher,
   ReadOnly)` to `QueryProcedureHost`; add
   `ProjectionBuilder::from_rows(...)`. **This is the P6 unblock** —
   `uniko::topics.rs` can delete its Rust adjacency builder and become
   a plain `CALL uni.algo.labelPropagation({nodeQuery, relQuery,
   weightColumn}, ...)`.
4. **M5c.4** — `Named` variant + `ProjectionStore` (per-`Database`,
   restart-clears) + procedures `uni.graph.{project, drop, list,
   exists}`. Full GDS parity for projection lifecycle.
5. **M5c.5** — Delete the legacy 5-arg shim (one release after M5c.2).

M5c.3 and M5c.4 are independent enough to land in parallel after M5c.2.

**Acceptance** (acceptance criteria §19.25, §19.26 in the proposal):
- Cypher projection identity test (same result as equivalent Native).
- Cypher projection derived-edge test (P6 entity-cooccurrence).
- Cypher projection error tests (missing `id`, write attempt, memory cap).
- Named projection project + reuse + drop + list + restart-clears.

**Coordination note:** the deprecation window opened in M5c.2 must be
held until downstream repos (uniko, etc.) migrate; M5c.5 ships one
release later.

### M5d — CRDT kinds (✅ complete-enough)

All 4 CRDTs (LWW, OR-Set, G-Counter, MV-Register) real in
`crates/uni-plugin-builtin/src/crdts.rs:15-29`. No follow-up needed
unless the RGA / sequence CRDT envisaged in the proposal (§4.11) is
prioritized.

### M5e — Phased SessionHook legacy bridge (1 session)

Phased trait is fully shipped at `crates/uni-plugin/src/traits/hook.rs:18-54`
(`on_parse` / `on_analyze` / `on_plan` / `on_execute_start` /
`on_execute_end` / `before_commit` / `after_commit` / `on_abort`).
`LoggingHook` reference impl present.

Remaining work: bridge the legacy `crates/uni/src/api/hooks.rs:64` shape
(`before_query` / `after_query` / `before_commit` / `after_commit`) into
the phased trait via default implementations that funnel `before_query`
through `on_parse` and `after_query` through `on_execute_end`. Then
make `Uni::add_hook` sugar for `Uni::add_plugin(BuiltinHookPlugin::new(hook))`
so the dual API surface collapses to one path.

**Tests:** existing `crates/uni/tests/hooks_test.rs` + `fork_hooks.rs`
must pass unchanged.

### M5f — Trigger host-side dispatch routing (1 session)

`LabelAuditTrigger` reference impl shipped at
`crates/uni-plugin-builtin/src/triggers.rs:38-113` (subscription model,
`FireMode::Async`, in-memory event counter).

Remaining work: build the per-(label, event_kind, property) routing
table in a new `crates/uni/src/api/triggers.rs`. On every committed
mutation batch, look up matching `TriggerPlugin` subscriptions and
invoke them. `Synchronous` triggers in `BeforeCommit` phase get the
mutation batch with the rejection power; `Async` triggers fire after
commit.

**Tests:** new `crates/uni/tests/trigger_*.rs` covering Synchronous
rejection, Async fire-after-commit, and label / property / predicate
selector filtering.

### M5g — Ephemeral entities (1 session)

5 logical types shipped (`uri`, `geo.point`, `email`, `ipv4`, `ipv6`)
at `crates/uni-plugin-builtin/src/logical_types.rs:21-27`.

Remaining work: add `NodeIdentity::Ephemeral { transient_id }` and
`EdgeIdentity::Ephemeral` variants in `crates/uni-common/src/value.rs`
(per proposal §4.13.1). Plus a `host.allocate_transient_id()` always-on
host primitive. Plus the `apoc.create.vNode` / `apoc.create.vEdge`
procedures that return ephemeral entities.

**Tests:** `crates/uni/tests/ephemeral_entities.rs` — `apoc.create.vNode`
returns an ephemeral; `SET` against it fails with `EphemeralWriteAttempt`.

### M5h — DataFusion pushdown planner integration (1 session)

The marker traits (`SupportsFilterPushdown`, …) are defined in
`crates/uni-plugin/src/traits/pushdown.rs`. The planner doesn't
consult them yet. The cutover:
1. In DataFusion's logical optimizer pass, downcast each `TableProvider`
   to the marker traits (`if let Some(p) = provider.as_any().downcast_ref::<dyn SupportsFilterPushdown>()`).
2. Apply `FilterApplication` to determine which filters can be
   served at the source vs. above the scan.
3. Same for projection / limit / topn / aggregate.

**Tests:** new `crates/uni-query/tests/pushdown.rs` — `EXPLAIN` query
against a backend implementing `SupportsFilterPushdown` shows no
`Filter` operator above the scan for handled predicates.

---

## M6a — Extism SDK runtime instantiation (2 sessions)

`ExtismLoader::prepare(manifest_json, grants)` is shipped. The SDK-gated
`ExtismLoader::instantiate(bytes)` returns `NotYetImplemented`. The
cutover wires:
1. Extract the manifest JSON from the wasm by calling the plugin's
   `manifest` Extism export (via `extism::Plugin::call("manifest", &[])`).
2. Parse with `prepare()` (already shipped) → get effective caps.
3. Filter the `HostFnRegistry` through effective caps; build an
   `extism::PluginBuilder` registering only the allowed `Function`s.
4. Call `register` Extism export to get the plugin's qname inventory.
5. For each qname, wrap the plugin in an adapter implementing
   `ScalarPluginFn` / `AggregatePluginFn` / `ProcedurePlugin` backed
   by `extism::Plugin::call(invocation_name, ipc_bytes)`.
6. Add the adapter into a `uni_plugin::PluginRegistry`.

**Example plugin:** `crates/example-extism-geo/` — Rust plugin via
`extism-pdk` implementing `geo.haversine`. Build with `cargo build
--target wasm32-wasip2 --release`. Acceptance test: load it, call
`geo.haversine(...)` from Cypher, get the expected great-circle
distance.

## M6b — Component Model SDK runtime instantiation (3 sessions)

`WasmLoader::prepare(manifest_json, grants)` is shipped. The SDK-gated
`WasmLoader::instantiate(bytes)` needs:
1. Extract manifest JSON from the wasm via `wasm-tools` (the component's
   `manifest-json` export).
2. `prepare()` → effective caps.
3. Build a `wasmtime::component::Linker<Host>` per ABI major; add only
   the host imports matching the effective capability set (capability
   gating by linker absence — proposal §10.2 layer 2).
4. Instantiate the component into a `wasmtime::Store<Host>` with
   epoch interruption + fuel metering per the manifest's resource
   limits.
5. Pre-warm `WasmInstancePool` (already shipped) with `min_warm`
   instances.
6. Wrap each WIT-world export in the appropriate plugin trait adapter
   backed by Arrow IPC over linear memory (proposal §6.3).
7. Register into `uni_plugin::PluginRegistry`.

**WIT bindings:** generate via `wit-bindgen`; commit the generated
bindings under `crates/uni-plugin-wasm/bindings/` (do not regenerate
on each build).

**Example plugin:** `crates/example-wasm-geo/` — Rust → wasm32-wasip2
via `cargo component`. Same acceptance criterion as the Extism example.

---

## M7 — piccolo-in-WASM packaging + apoc-ext bodies (2 sessions)

`LuaPlugin::parse_static_manifest` is shipped (scanner over Lua
source). The next steps:
1. Create `crates/uni-plugin-lua/lua-host/` as a `wasm32-wasip2` Rust
   crate with `piccolo`, exposing the same WIT worlds as native CM
   plugins.
2. Build `lua-host.wasm`; commit under `crates/uni-plugin-lua/assets/`.
3. `LuaPlugin::instantiate(src)` constructs a wasmtime `Store` over
   the pre-built `lua-host.wasm`, calls `load_source(src)` export,
   then routes plugin invocations through the wasmtime path (depends
   on M6b being ready).
4. Replace the `error("not yet implemented (M7+)")` placeholders in
   `lua-plugins/uni-plugin-apoc-ext/*.lua` with real bodies.

**Sandbox:** the Lua-host whitelists `math.*` / `string.*` / `table.*`
/ `pairs` / `pcall` / `error`; removes `io.*` / `os.execute` / `require`
/ `load` / `loadstring`; adds capability-gated `uni.fs.*` / `uni.http.*`
/ `uni.query` / `uni.kms.*` only when the corresponding capability is
granted (host imports omitted from the Linker if not).

---

## M8 — PyO3 bridge (1–2 sessions)

`PyPluginLoader` registration surface is shipped. The PyO3-gated
runtime bridge:
1. New `PyScalarFn` struct holding a `PyObject` callable + signature.
2. `impl ScalarPluginFn for PyScalarFn` calls `Python::with_gil`,
   converts `&[ColumnarValue]` to PyArrow arrays via the Arrow C
   Data Interface (zero-copy when possible), invokes the Python
   callable, converts the return value back to `ColumnarValue`.
3. Python decorator API in `bindings/uni-db/src/plugins.rs`:
   ```python
   @db.scalar_fn("py.score", returns="float", args=["float", "float"], vectorized=True)
   def score(x, y):
       return x * 0.7 + y * 0.3
   ```

**GIL strategy:** vectorized mode = one `Python::with_gil` per batch;
row-by-row = one per row (documented as slow). GIL contention under
parallel query execution discussed in proposal §5.4.1.

---

## M9 — `declareFunction` body parsing + persistence (2 sessions)

`uni.plugin.listDeclared` + `dropDeclared` are shipped. The
`declareFunction` / `declareProcedure` / `declareAggregate` /
`declareTrigger` procedures need:
1. **Body parsing:** parse the Cypher body string at declaration time
   (via `uni_cypher` parser); type-check arg types; reject malformed.
2. **Synthetic plugin construction:** wrap the parsed body in a
   `DeclaredScalarFn` or `DeclaredProcedure` that re-executes the body
   via `host.query` on each invocation.
3. **Persistence:** write the declaration to a `_DeclaredPlugin`
   system label (a new label managed by the meta-plugin). The label
   schema is documented in proposal §9.7.
4. **Re-registration on startup:** `CustomPlugin::init()` reads
   `_DeclaredPlugin` rows and re-registers each declared plugin.
5. **Native-shadow detection:** if a native plugin shadows a declared
   qname, mark the declared as `active: false`.

---

## M10 — per-kind reload plumbing (2 sessions)

`EpochFencedReload` driver is shipped. The per-kind discipline
(proposal §11.2.1) needs:
- **StorageBackend reload:** new `open()` constructs a fresh Storage;
  old continues serving until drain.
- **IndexHandle reload:** `persist()` on old, `open(persisted_bytes)`
  on new — preserves built indexes (HNSW graph, IVF centroids).
- **BackgroundJobProvider reload:** in-flight runs complete against
  old; next tick uses new.
- **CdcOutputProvider reload:** `checkpoint()` on old, `start(lsn)`
  on new.
- **CrdtKindProvider reload:** schema-compat check — new's
  `from_persisted` must accept old's `persist()` bytes; otherwise
  hard reload error.

**Tests:** per-kind reload tests in `crates/uni/tests/reload_*.rs`
each verifying the discipline.

---

## M11 — Tokio runtime integration + persistence (1 session)

`Scheduler::tick()` driver primitive is shipped. The runtime
integration:
1. `crates/uni/src/scheduler.rs` — spawn a `tokio::spawn` task that
   loops: sleep poll-interval → call `scheduler.tick()` → for each
   returned QName, spawn the job's `BackgroundJobProvider::execute`
   → on completion call `scheduler.mark_finished`.
2. Persist `SchedulerJobRecord`s to a `uni_system.background_jobs`
   label (similar to `_DeclaredPlugin`); reload on startup via
   `requeue_orphaned_runs`.
3. `tracing-opentelemetry` layer in `crates/uni/src/observability.rs`:
   exports spans to whatever OTLP collector the user configures.

---

## M12 — CLI + OCI + Python bindings + perf (3 sessions)

`run_against_plugin` conformance probe suite is shipped. The
M12 remaining work:
1. **CLI** in `crates/uni-cli/src/cmd/plugin.rs`:
   - `uni plugin install <path|url|oci-ref>` (auto-detect format).
   - `uni plugin list / info / grant / revoke / remove / reload`.
   - `uni plugin verify / help / declared list / declared drop`.
2. **OCI artifact loader** in `crates/uni-plugin-wasm/src/oci.rs`:
   pulls WASM components from OCI registries (`wasm-pkg-loader`);
   verifies signatures via `cosign`; caches in `~/.uni/plugins/cache/`.
3. **Python bindings** in `bindings/uni-db/src/plugins.rs`:
   `Uni.add_plugin`, `Uni.load_lua_plugin`, `Uni.load_wasm`,
   `@Uni.scalar_fn` / `@Uni.aggregate_fn` / `@Uni.procedure` decorators.
4. **Perf regression suite** in `crates/uni-bench/benches/plugin_perf.rs`:
   compare `score(x, y)` via native / compile-time plugin / WASM
   pre-warmed / WASM cold / PyO3 vectorized / Lua vectorized.

---

## Session etiquette

When picking up a remaining-work section:
1. Read this doc + the matching `plugin_framework_implementation.md`
   §4 milestone block.
2. Pull the latest from `worktree-plugin-fw`.
3. Work the section to completion — TCK green, clippy clean, all
   tests pass, doc updated.
4. Commit with the conventional prefix (`feat(plugin-fw): MX — ...`).
5. Update this doc: strike out the completed section + reference the
   commit SHA.
6. Stop.

When the entire doc is empty / strikethrough, v1.0 is shippable.
