# uni-plugin-extism — Code Simplifier Findings

Scope: `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-plugin-extism/`

## High-impact: duplication across adapters

### 1. `acquire(&pool)` helper duplicated in three adapter modules
- **Locations:**
  - `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-plugin-extism/src/adapter.rs:125-130` (inline in `invoke`)
  - `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-plugin-extism/src/adapter_aggregate.rs:338-347`
  - `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-plugin-extism/src/adapter_procedure.rs:180-189`
- **Description:** Each module defines (or inlines) the same `PooledInstance::acquire(...).map_err(|e| FnError::new(CODE_RESOURCE_LIMIT, "acquire plugin instance: {e}"))` block.
- **Suggestion:** Promote a single `pub(crate) fn acquire_pooled(...) -> Result<PooledInstance<extism::Plugin>, FnError>` into a new `adapter_common` (or a small private module). Replace the inline use in `adapter.rs:125` with the helper.
- **Effort:** ~10 minutes.

### 2. `extism_err_to_fn_err(IpcError)` duplicated three times
- **Locations:** `adapter.rs:169-171`, `adapter_aggregate.rs:349-351`, `adapter_procedure.rs:191-193` — identical bodies.
- **Suggestion:** Move to the same shared helper module alongside `acquire_pooled`.
- **Effort:** ~5 minutes.

### 3. `argtype_arrow`/`argtype_to_arrow` duplicated three times
- **Locations:** `adapter_aggregate.rs:329-336`, `adapter_procedure.rs:171-178`, `wire_translate.rs:268-274` — all three are byte-identical match expressions on `ArgType`.
- **Suggestion:** Single `pub(crate) fn argtype_to_arrow(&ArgType) -> DataType` (likely in `wire_translate.rs` since that file is already used by every adapter caller).
- **Effort:** ~5 minutes.

### 4. `build_args_schema` near-duplicate
- **Locations:** `adapter_aggregate.rs:315-323` and `adapter_procedure.rs:161-169` differ only in how they reach the `ArgType` (one indexes `sig.args[i]`, the other `sig.args[i].ty`).
- **Suggestion:** A small generic `fn args_schema_from<F: Fn(&T) -> &ArgType>(args: &[T], f: F) -> SchemaRef` or just an iterator helper `argtypes_to_schema(impl Iterator<Item = &ArgType>)`. Borderline — only collapse if other adapters appear; otherwise leave a doc-comment cross-reference.
- **Effort:** ~10 minutes (optional).

### 5. Export-name builders share a single sanitization pattern
- **Locations:** `adapter.rs:40-42`, `adapter_aggregate.rs:38-64` (five fns), `adapter_procedure.rs:36-38` — all do `qname.to_string().replace('.', "_")` then prefix/suffix.
- **Suggestion:** Keep `sanitize_qname` (already in aggregate), expose as `pub(crate)`, reuse from all three modules. Consider one parameterized `export_name(prefix, qname, suffix)` if it doesn't hurt clarity; the per-fn wrappers in aggregate are fine to keep for grep-ability.
- **Effort:** ~10 minutes.

### 6. Plugin-call boilerplate ("acquire, get_mut, call, to_vec, drop") repeated five times
- **Locations:** `adapter.rs:125-144`, `adapter_aggregate.rs:109-123` (new), `adapter_aggregate.rs:190-203` (envelope), `adapter_aggregate.rs:253-266` (evaluate), `adapter_procedure.rs:125-136`.
- **Description:** Each site acquires a pooled instance, calls a named export with a byte payload, copies the borrowed `&[u8]` to `Vec<u8>`, drops the lease, and maps errors to `FnError::CODE_UNEXPECTED_NULL`. Error variant choice is also questionable: `CODE_UNEXPECTED_NULL` for "extism call failed" reads as a miscategorization.
- **Suggestion:** Add a `pub(crate) fn call_export(pool: &Arc<…>, export: &str, payload: &[u8]) -> Result<Vec<u8>, FnError>` to the new shared helper module. Pick a more accurate error code (e.g., introduce `CODE_PLUGIN_INVOKE` or reuse `CODE_RESOURCE_LIMIT`/a new constant). Each adapter site reduces to one line.
- **Effort:** ~20 minutes plus an error-code audit.

## Dead / scaffold code that can be deleted

### 7. `ExtismError::NotYetImplemented` and `ExtismError::not_yet`
- **Location:** `error.rs:60-80`.
- **Description:** The doc-comment on the variant ("M6a cutover commits remove these") confirms intent; `loader.rs` no longer returns it (the actual `load()` is wired), and a `rg NotYetImplemented` showed only the variant + constructor + lib.rs doc reference.
- **Suggestion:** Remove the variant + helper + the obsolete sentence in `lib.rs:26-32` referring to it. If kept for forward-compat, mark it `#[doc(hidden)]` and stop documenting it in the crate-level docs.
- **Effort:** ~10 minutes (plus a sweep across the workspace to confirm no callers).

### 8. Stale crate-level doc ("Scaffolding only", "Until the cutover")
- **Location:** `lib.rs:22-32`.
- **Description:** The crate is no longer scaffolding — `load()`, adapters, pool wiring all ship. The doc still claims `load()` returns `NotYetImplemented`.
- **Suggestion:** Rewrite that paragraph to reflect M6a completion (status: aggregate + procedure live; M6b host-imports pending).
- **Effort:** ~5 minutes.

### 9. Redundant `read_register_export` call in `loader.rs::load`
- **Location:** `loader.rs:346-354`.
- **Description:** The register export is called twice in immediate succession against separate leased instances. The comment says "Re-read fresh to consume entries (cheap — JSON parse)", but `read_register_export` returns an owned `RegistrationManifest` — the first call is already a complete owned value. The first `let _registration_only = …` is dead work.
- **Suggestion:** Delete the first `acquire/read_register_export/drop` block; keep only the second call.
- **Effort:** ~2 minutes. (Verify with `cargo nextest run -p uni-plugin-extism`.)

### 10. `instantiate` is a one-line forwarder to `build_plugin`
- **Location:** `loader.rs:246-252`.
- **Description:** Public `instantiate` just calls `self.build_plugin(bytes, prepared)`. Two public methods with identical behavior risks divergence and confuses callers.
- **Suggestion:** Either delete `instantiate` (preferred — `build_plugin` is more descriptive) or make `build_plugin` private and keep `instantiate` as the sole entry point. The integration test `instantiate_with_minimal_wasm.rs` will need a one-name update.
- **Effort:** ~5 minutes.

## Complex / over-shaped functions

### 11. `ExtismLoader::load` is 150+ lines and mixes five concerns
- **Location:** `loader.rs:285-438`.
- **Description:** It does (a) pass-1 build, (b) manifest re-serialization, (c) prepare(), (d) pool construction, (e) per-entry registration. The per-entry match arms are near-identical: parse qname, translate signature, build adapter, register, push qname to a kind-specific Vec.
- **Suggestion:** Extract three private helpers (`register_scalar`, `register_aggregate`, `register_procedure`) each taking `(registrar, pool, qname, …)` and returning `Result<String, ExtismError>`. The match shrinks to one line per arm; `load` becomes ~50 lines and reads as the sequencing it actually is. The manifest-re-serialization step (loader.rs:324-335) can also become `parsed_manifest_to_json(&parsed_manifest)`.
- **Effort:** ~25 minutes.

### 12. `loader.rs::load` re-serializes the manifest only to pass `&[u8]` to `prepare`
- **Location:** `loader.rs:324-336`.
- **Description:** `prepare(manifest_json: &[u8], grants)` parses bytes, but the caller already holds a parsed `ExtismPluginManifest`. Going manifest → JSON → manifest is wasteful and adds an `Internal` error path that can never reasonably fire.
- **Suggestion:** Add a sibling `pub fn prepare_from_manifest(&self, manifest: ExtismPluginManifest, grants: &[String]) -> PreparedExtismPlugin` (infallible). Keep `prepare(bytes, grants)` as a thin wrapper that parses then delegates. Eliminates the re-serialize block entirely.
- **Effort:** ~15 minutes.

### 13. `ExtismAggregateAccumulator::call_with_envelope` clones `self.update_export`
- **Location:** `adapter_aggregate.rs:216`, `:243`.
- **Description:** `self.call_with_envelope(&self.update_export.clone(), …)` clones a `String` only to immediately take `&str` of it. The clone is unnecessary because `call_with_envelope` takes `&str` and the borrow doesn't conflict (it borrows `&self` immutably, and we already have `&self`).
- **Suggestion:** Replace with `self.call_with_envelope(&self.update_export, batch)`. Likely a leftover from when `call_with_envelope` took `&mut self`.
- **Effort:** ~2 minutes (verify borrow-check).

### 14. Two-phase init in `create_accumulator` is more elaborate than needed
- **Location:** `adapter_aggregate.rs:131-152` + `surface_init_err` plumbing throughout.
- **Description:** Carries an `Option<FnError>` and re-checks on every `update_batch` / `merge_batch` / `state` / `evaluate`. Reasonable given DataFusion's infallible factory contract, but each `surface_init_err` allocates a new `String` per call when the error is set, and the design encourages all four PluginAccumulator methods to "remember to call it first".
- **Suggestion:** Either (a) collapse the four checks by making `state: Result<Vec<u8>, FnError>` and using `?` once at each entry; or (b) document why the option is preserved and keep it. Lower priority — the current shape is defensible.
- **Effort:** ~15 minutes (optional).

## Smaller / stylistic

### 15. `lib.rs::ipc` module is a one-line re-export
- **Location:** `lib.rs:61-63`.
- **Description:** `pub use uni_plugin_wasm_rt::ipc::{…}` wrapped in a module purely for the path `uni_plugin_extism::ipc::*`. Fine, but the same pattern is used twice (`ipc` and `pool`); consider documenting them under a single "Re-exports from `uni-plugin-wasm-rt`" doc heading rather than two near-identical doc blocks.
- **Effort:** ~5 minutes (cosmetic).

### 16. `build_envelope` swallows `u32::MAX` overflow silently
- **Location:** `adapter_aggregate.rs:356-362`.
- **Description:** `u32::try_from(state.len()).unwrap_or(u32::MAX)` will encode a wrong length if `state.len() > u32::MAX`. The "real" `call_with_envelope` path correctly returns `FnError::CODE_RESOURCE_LIMIT` for the same case (lines 180-185). The test-only helper diverges from the production envelope.
- **Suggestion:** Either return `Result<Vec<u8>, &'static str>` so tests also exercise the overflow path, or panic explicitly (`expect`) — silent saturation is the worst option.
- **Effort:** ~5 minutes.

### 17. `LoadOutcome` carries three parallel `Vec<String>` per registration kind
- **Location:** `loader.rs:512-531`.
- **Description:** `scalars_registered` / `aggregates_registered` / `procedures_registered` — symmetric and likely both written and read together.
- **Suggestion:** Consider `Vec<(RegistrationKind, String)>` or a single `BTreeMap<RegistrationKind, Vec<String>>`. Only worthwhile if downstream callers iterate all three — otherwise leave as is.
- **Effort:** ~15 minutes (only if downstream wants the unified shape).

### 18. `ExtismLoader::Debug` is hand-rolled but `LoadOutcome::Debug` and `ExtismScalarFn::Debug` are too
- **Locations:** `loader.rs:95-102`, `loader.rs:533-545`, `adapter.rs:58-66`, `adapter_aggregate.rs:77-84`, `adapter_procedure.rs:50-57`.
- **Description:** All five Debug impls are pure boilerplate to skip non-Debug fields (`extism::Function`, `extism::Plugin`, pool internals). They cannot be derived but the pattern repeats.
- **Suggestion:** A tiny declarative macro `impl_debug_skip!(Struct { field1, field2, … })` would replace five `impl std::fmt::Debug` blocks with one-liners. Borderline — only worth it if more such types are coming.
- **Effort:** ~15 minutes (optional).

---

## Summary of priorities

| Priority | Items | Combined effort |
|---|---|---|
| High (clear duplication, dead code) | 1, 2, 3, 6, 7, 8, 9, 10 | ~1 hour |
| Medium (function complexity) | 5, 11, 12, 13 | ~1 hour |
| Low (stylistic / optional) | 4, 14, 15, 16, 17, 18 | ~1 hour |

All suggestions are behavior-preserving. The duplication cleanups (1-6) and dead-code removal (7-10) yield the most clarity for the least effort and should land first.
