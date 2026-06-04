# Code Simplifier Review: `crates/uni-plugin-wasm/`

Scope: `src/{loader,adapter,adapter_aggregate,adapter_procedure,linker,multi_version,buffer,error,host_state,lib}.rs`. Total ~2.5K LOC. Findings prioritized by impact.

---

## 1. Major duplication: three nearly-identical pool builders

**Files / lines**: `src/loader.rs:678-710` (`build_scalar_pool`), `src/loader.rs:712-744` (`build_aggregate_pool`), `src/loader.rs:746-778` (`build_procedure_pool`).

**Problem**: The three `build_*_pool` functions are mechanical copies. Each clones `bytes` + `prepared` into an Arc, builds an engine, compiles a `Component`, picks a linker, opens a `Store`, applies resource limits, and calls `<TypedBinding>::instantiate`. Only the final binding type (`ScalarPlugin` / `AggregatePlugin` / `ProcedurePluginBindings`) differs.

**Suggestion**: Extract a generic `build_pool<B, I>(bytes, prepared, mk_instance)` where the caller supplies a small closure that wraps the typed `instantiate` plus the `{store, bindings}` struct construction. Alternatively: define a tiny `trait WasmBinding { fn instantiate(...) -> Self; fn into_instance(store, b) -> Self::Instance; }` implemented for the three binding types. Reduces ~100 LOC of triplicated factory boilerplate and removes risk of one variant drifting (e.g., a future config bump applied to only two of three).

**Effort**: M (1-2 hours).

---

## 2. Duplicated `arrow_name_to_dt` / `arrow_name` and `wire_arg` helpers

**Files / lines**: `src/loader.rs:886-903` (`arrow_name_to_dt`), `src/loader.rs:918-934` (inner `arrow_name` inside `wire_fn_sig_to_internal`); also `src/loader.rs:832-837` (`wire_arg` inside `wire_proc_sig_to_internal`) and `src/loader.rs:912-917` (`wire_arg` inside `wire_fn_sig_to_internal`).

**Problem**: `arrow_name`/`arrow_name_to_dt` are byte-for-byte identical. Two nested `wire_arg` helpers in different functions are also identical.

**Suggestion**: Keep only one module-level `arrow_name_to_dt` and one module-level `wire_arg`; delete the inner copies and call the top-level fn. Also unify the volatility match (lines 801-810 and 937-946) into a single `parse_volatility(&str) -> Result<Volatility, WasmError>` helper.

**Effort**: S (15 min).

---

## 3. Duplicated `argtype_arrow` across adapters

**Files / lines**: `src/adapter_aggregate.rs:246-253`, `src/adapter_procedure.rs:145-152`.

**Problem**: Identical function defined twice. Also overlaps with the inline match in `wire_proc_sig_to_internal` (`src/loader.rs:856-860`).

**Suggestion**: Hoist a single `pub(crate) fn argtype_to_arrow_dt(t: &ArgType) -> DataType` to a shared module (e.g., a new `src/wire.rs` or extend `bindings.rs`). Three call sites collapse to one.

**Effort**: S (15 min).

---

## 4. Repetitive `Ok(Ok(_)) / Ok(Err(_)) / Err(_)` trap-handling triplet

**Files / lines**: `src/loader.rs:196-203` (`invoke_scalar`), `:243-250` (`agg_new`), `:260-270` (`agg_update`), `:280-290` (`agg_merge`), `:295-306` (`agg_evaluate`), `:325-337` (`invoke_procedure`).

**Problem**: Six near-identical 8-line `match` blocks. Each maps `(plugin-fn-error | trap)` into `WasmError::Instantiate` with a slightly different operation label.

**Suggestion**: Add a private helper:

```text
fn unwrap_plugin_call<T>(op: &str, r: Result<Result<T, FnError>, wasmtime::Error>) -> Result<T, WasmError>
```

Each call site becomes a single line. Also: `WasmError::Instantiate` is being misused as the "invoke trap" bucket (acknowledged in the comment at `:188-192`); consider adding a dedicated `WasmError::InvokeTrap { op, source }` variant for clearer diagnostics.

**Effort**: S-M (30 min).

---

## 5. `bootstrap` placeholder `PreparedComponent` is verbose

**File / lines**: `src/loader.rs:431-445`.

**Problem**: Inline-constructs a `PreparedComponent` with every field set to `Default`-like empty values just to pass to `instantiate` for the manifest-read pass.

**Suggestion**: Add `impl Default for ComponentManifest` (or a `ComponentManifest::empty()` ctor) and `impl Default for PreparedComponent`; the bootstrap becomes `PreparedComponent::default()`. Also: the round-trip "parse JSON → struct → re-serialize JSON → re-parse" at lines 456-468 is wasteful — `prepare` should accept an already-parsed `ComponentManifest` via a sibling fn (`prepare_from_manifest(manifest, grants)`) so pass 2 skips the JSON dance.

**Effort**: S (20 min). Removes ~25 LOC and one allocation per load.

---

## 6. `LoadOutcome` registration loop has duplicated pool-lazy-init + adapter-registration boilerplate

**File / lines**: `src/loader.rs:488-567`.

**Problem**: Each `match` arm (Scalar/Aggregate/Procedure) duplicates the same six steps: parse qname, convert wire signature, lazily build the right pool, construct an `Arc<adapter>`, call the registrar method, push qname to a tracking vec.

**Suggestion**: Two small helpers — `parse_qname(&str) -> Result<QName, WasmError>` (eliminates the repeated `.map_err(|e| WasmError::InvalidWasm(format!("invalid qname ...")))` triplet) and a `lazy_pool!` macro or `Option::get_or_insert_with` pattern for the pool init. Arm bodies shrink to ~5 lines each. Consider factoring registration into separate `register_scalar` / `register_aggregate` / `register_procedure` methods on `WasmLoader` for readability.

**Effort**: M (45 min).

---

## 7. Dead code

- **`src/loader.rs:93-95`** — `default_proc_mode` returns `"read"`. `RegistrationEntry::Procedure.mode` already has `#[serde(default = "default_proc_mode")]` but the function is fine; however the `Duration` import at `:22` is only used by the dead `epoch_timeout_marker` stub at `:964-970`. Remove the stub (or wire it) and the `use std::time::Duration;` import.
- **`src/loader.rs:967-970`** — `epoch_timeout_marker` is `#[allow(dead_code)]` and returns `Duration::from_millis(0)`. Comment says "Phase D expands this"; if there is no near-term plan, delete to reduce noise. If kept, track via a TODO that references an issue.
- **`src/adapter.rs:135-138`** — `wasm_err_to_fn_err` is `#[allow(dead_code)]`. Delete or wire into the trap path (see finding #4).
- **`src/loader.rs:670-675`** — `apply_resource_limits` reads `manifest.timeout_ms` into `ms` and immediately discards it via `let _ = ms;`. Either pass it to `store.set_epoch_deadline` meaningfully or remove the binding and just call `store.set_epoch_deadline(1)`. The current shape is misleading — readers think `ms` is used.
- **`src/linker.rs:70-75`** — `build_scalar_linker` is a back-compat shim with no internal callers (loader uses `select_linker_for_manifest`, multi_version uses `build_scalar_linker_v1` directly). If there are no external callers (it's `pub`, so check downstream crates), delete.

**Effort**: S (10 min for each).

---

## 8. `select_linker_for_manifest` duplicates `MultiVersionLinker::linker_for`

**Files / lines**: `src/loader.rs:622-649` and `src/multi_version.rs:85-119`.

**Problem**: Both pieces of code probe `SUPPORTED_MAJORS`, match on the major, and dispatch to `build_scalar_linker_v1` / `_v2`. The loader builds a *fresh* linker on every instantiate (no caching), even though `MultiVersionLinker` exists exactly to cache.

**Suggestion**: Have `WasmLoader` own (or borrow) an `Arc<MultiVersionLinker>` and route linker resolution through it. Removes ~30 LOC, eliminates the duplicated dispatch logic, and gives the load path the caching benefit the multi-version module advertises. Today the cache is effectively orphaned in production.

**Effort**: M (1 hour). Higher-impact than line-count suggests because it activates an existing feature.

---

## 9. `ScalarPluginInstance`/`AggregatePluginInstance`/`ProcedurePluginInstance` are structurally identical

**Files / lines**: `src/loader.rs:169-225`, `:227-307`, `:309-339`.

**Problem**: Three structs that each hold `(store, bindings)` with a hand-rolled `Debug` impl and very thin method delegates.

**Suggestion**: A generic `PluginInstance<B> { store: Store<HostState>, bindings: B }` with a `Debug` impl bound on `B`. Specific call helpers (e.g., `agg_new`) can stay as `impl PluginInstance<AggregatePlugin>` blocks. Halves the boilerplate; the three `Debug` impls collapse.

**Effort**: M (45 min).

---

## 10. Minor: trap-error formatting inconsistency

**File / lines**: throughout `loader.rs` invoke methods. Some include `retryable=` in the message (`:198-201` scalar), others don't (`:245-248` agg_new and siblings).

**Suggestion**: Unify via the helper proposed in #4. Pick one canonical format.

**Effort**: trivial once #4 lands.

---

## Suggested order of attack

1. Findings #2, #3, #7 (low-risk cleanups, ~30 min total).
2. Finding #4 (trap-helper) — unlocks #10.
3. Finding #1 (pool builder generic) and #9 (instance generic) together — they share the same generic-binding refactor.
4. Finding #5 (`Default` + skip the JSON round-trip).
5. Finding #8 (route loader through `MultiVersionLinker`) — biggest architectural win.
6. Finding #6 (registration loop helpers).

Total estimate: ~5-6 focused hours to land all changes. Each is independently testable against the existing `tests/instantiate_minimal_component.rs` and `tests/example_wasm_geo_e2e.rs`.
