# Code-simplifier review: `crates/uni-plugin-rhai/`

Scope: 14 source files in `src/` (+ `host_fn_impls/`) totaling ~2,800 LoC plus
~500 LoC of integration tests. Focus: duplication, dead code, complex
functions, unnecessary abstractions. No changes were made.

---

## 1. Duplication

### 1.1 Triplicated column wrappers (`columns.rs`)
- **File:** `src/columns.rs:24-127`
- **Issue:** `Float64Column`, `Int64Column`, and `Utf8Column` are near-identical
  shells around `Arc<...Array>`. Each has copy-pasted `new`, `len`, `is_empty`,
  and `get` implementations (~30 lines x 3 = ~90 LoC). Their `get`
  null-handling logic is byte-identical apart from `value(idx)` typing and one
  `to_owned()` call for Utf8.
- **Suggestion:** Extract a generic `ArrayColumn<A: Array>` wrapper with a
  trait-driven `to_dynamic(arr, idx) -> Dynamic` conversion, or alternatively
  keep three structs but generate them through a single `column_wrapper!`
  macro. The Rhai `register_type_with_name` registration on lines 223-247 is
  also a candidate for macro consolidation.
- **Effort:** Medium (~1-2 hrs). Macro route is lowest-risk.

### 1.2 Parallel `parse_*_entries` functions (`manifest.rs`)
- **File:** `src/manifest.rs:137-204`
- **Issue:** `parse_scalar_entries`, `parse_aggregate_entries`, and
  `parse_procedure_entries` follow the same template: lookup key, cast to
  `rhai::Array`, iterate, cast each to `Map`, extract fields. ~20 LoC each
  with three duplicate `try_cast::<rhai::Array>` blocks and three duplicate
  per-row `try_cast::<Map>` blocks.
- **Suggestion:** Introduce `fn each_entry<T>(map: &Map, key: &str, f: impl
  Fn(&Map) -> Result<T, RhaiError>) -> Result<Vec<T>, RhaiError>` that handles
  the array-lookup/per-row-map-cast plumbing; each parser becomes ~6 lines.
- **Effort:** Low (~30 min).

### 1.3 Repeated `downcast_ref` per Arrow type in vectorized path
- **File:** `src/adapter.rs:155-189` and `src/dynamic_bridge.rs:53-83`
- **Issue:** Same Float64/Int64/Utf8 downcast pattern appears in two places
  with slightly different error codes. The vectorized branch in `adapter.rs`
  is essentially a "decode-array-to-column-userdata" reverse of the column
  module.
- **Suggestion:** Move the downcast-to-column-userdata logic into
  `columns.rs::Float64Column::from_array_ref` (and siblings); adapter loop
  becomes a single dispatch.
- **Effort:** Low (~30 min).

### 1.4 `RhaiAccumulator` repeats engine-call boilerplate
- **File:** `src/adapter_aggregate.rs:100-171`
- **Issue:** `update_batch`, `merge_batch`, and `evaluate` each open a fresh
  `Scope`, call `engine.call_fn`, and rewrap the error. The `format!("{}_init",
  ...)` / `"_accumulate"` / `"_merge"` / `"_finalize"` strings are computed
  on every call instead of being cached on the struct.
- **Suggestion:** Precompute the four `SmolStr` callable names in
  `RhaiAggregateFn::new` and store them on `RhaiAccumulator`; extract a small
  `call(scope_fn_name, args) -> Result<Dynamic, FnError>` helper. Saves
  per-row allocations on the hot accumulate path.
- **Effort:** Low (~30 min). Real perf win in inner loop.

### 1.5 Identical host-fn stub shape across kms / net / secret
- **Files:** `src/host_fn_impls/{net.rs,kms.rs,secret.rs}`
- **Issue:** All four "M7-followup" stub registrations duplicate the
  `Err(Box::new(EvalAltResult::ErrorRuntime(..., Position::NONE)))` boilerplate
  (4 occurrences). The `register(loader)` functions are also structurally
  identical: build a placeholder capability, register one or two specs.
- **Suggestion:** Add a small `pub(crate) fn not_yet_runtime_err(name: &str)
  -> Box<EvalAltResult>` in `error.rs` and a `register_stub_fn!(engine, name,
  args, ret)` macro. Each stub module shrinks to ~10 lines.
- **Effort:** Low (~30 min).

---

## 2. Dead / vestigial code

### 2.1 Force-import constants in `adapter_procedure.rs`
- **File:** `src/adapter_procedure.rs:171-174`
- **Issue:** `const _: Option<ProcedureMode> = None;` and `const _SE:
  Option<SideEffects> = None;` exist solely "so rustdoc cross-links resolve".
  Both are flagged `#[allow(dead_code)]`. `ProcedureMode` and `SideEffects` are
  already used in the same file at lines 210-211 in tests and the live
  `RhaiProcedure::invoke` path (transitively via `ProcedureSignature`), so the
  rustdoc justification is suspect.
- **Suggestion:** Delete both constants; if rustdoc cross-links break,
  consider an intradoc link in the module docstring.
- **Effort:** Trivial.

### 2.2 `RhaiError::NotYetImplemented` and `not_yet` constructor
- **File:** `src/error.rs:57-76`
- **Issue:** No call site references `NotYetImplemented` or `not_yet()` in the
  crate (host-fn stubs use `EvalAltResult::ErrorRuntime` instead). It's a
  scaffold leftover from M7.
- **Suggestion:** Either remove the variant + helper (preferred — M7 is now
  cutover) or wire stubs to use it so error types are consistent.
- **Effort:** Trivial.

### 2.3 `RhaiError::CapabilityDenied` never constructed
- **File:** `src/error.rs:32-38`
- **Issue:** No call site produces `CapabilityDenied`. The loader returns
  `ManifestInvalid` / parse failures instead, and capability denials surface
  as `denied_capabilities` on `LoadOutcome`. The error variant looks like an
  artifact of an abandoned reconciliation path.
- **Suggestion:** Delete the variant, or actually classify Rhai's
  `ErrorFunctionNotFound` against the host-fn registry to produce it (matches
  the docstring in `lib.rs:30-35`).
- **Effort:** Low if removing; medium if implementing.

### 2.4 `RhaiError::RuntimeError` never constructed
- **File:** `src/error.rs:42-43`
- **Issue:** Adapters convert Rhai errors to `FnError`, not `RhaiError`.
  `RuntimeError` has no producer.
- **Suggestion:** Delete.
- **Effort:** Trivial.

### 2.5 `RhaiError::ResourceLimit` never constructed
- **File:** `src/error.rs:50-53`
- **Issue:** Same as above — limit-trip errors flow through
  `classify_rhai_error` in `adapter.rs:224` into `FnError`, not `RhaiError`.
- **Suggestion:** Delete or wire `classify_rhai_error` to map operations/stack/
  too-large variants into `RhaiError::ResourceLimit` for the load path. The
  three unused error variants together cost ~25 lines of dead surface.
- **Effort:** Low.

### 2.6 Unused `_determinism` argument
- **File:** `src/loader.rs:218-221`
- **Issue:** `build_procedure_signature` takes a `_determinism: &str` and
  ignores it; the call site at `loader.rs:194` passes it. Procedures don't
  carry volatility in their signature, so the param is genuinely unused.
- **Suggestion:** Drop the parameter; simplify call site.
- **Effort:** Trivial.

### 2.7 `RhaiHostFnRegistry::get` and `RhaiLoader::host_fns/host_fn_count`
- **Files:** `src/host_fns.rs:89-91`, `src/loader.rs:91-99`
- **Issue:** `RhaiHostFnRegistry::get` has no in-tree callers (search shows
  only `iter()` is consumed by `engine.rs`). Same for
  `RhaiLoader::host_fns()` (the test-only path uses `host_fns_mut`).
  `host_fn_count` is only used in `Debug` impl; it could be inlined.
- **Suggestion:** Either remove or document as a public API contract; if kept,
  add at least one test that exercises `get`. `host_fn_count` can be replaced
  with `self.host_fns.len()` inline in `Debug`.
- **Effort:** Trivial.

### 2.8 `&mut self` on read-only column accessors
- **File:** `src/columns.rs:38-55, 73-90, 109-126`
- **Issue:** `Float64Column::len`, `is_empty`, `get` (and the Int64/Utf8
  twins) take `&mut self` even though they only read `Arc<...Array>`. This is
  presumably to match Rhai's registrar signature, but at minimum the `#[must_use]`
  + `&mut self` combo on a getter is misleading.
- **Suggestion:** Confirm whether Rhai's `register_fn` / `register_indexer_get`
  actually requires `&mut`. If not, take `&self`. If yes, drop `#[must_use]`
  and add a `// rhai requires &mut self for registrar` comment.
- **Effort:** Trivial (verify + edit).

---

## 3. Complex / problematic functions

### 3.1 `MutableFloat64Column::set` rebuilds the entire builder per write
- **File:** `src/columns.rs:166-196`
- **Issue:** O(n) per `set` — the function clones the full values slice into
  a `Vec<Option<f64>>`, mutates one slot, then rebuilds a fresh
  `Float64Builder` and re-appends every value. A script doing `out[i] = x` in
  a loop over an n-row column is O(n²). Comment acknowledges this ("v1 uses a
  side-vec approach").
- **Additionally:** the null-detection logic at lines 178-186 is broken — it
  uses `values_slice().get(j).is_some()` which is true whenever `j < len`,
  meaning every previously-appended-null becomes `Some(0.0)` (the default
  Float64Builder fill). Combined with the rebuild-on-write, nulls written via
  `append_null` are silently lost on any subsequent `set`.
- **Suggestion:** Back the wrapper with `Arc<Mutex<Vec<Option<f64>>>>`
  directly; `freeze()` constructs the `Float64Array` once. Eliminates O(n²)
  and fixes the null-loss bug.
- **Effort:** Medium (~1-2 hrs incl. test for nulls preservation).

### 3.2 `dynamic_to_scalar_loose` cascade of `if let Ok` (`adapter_aggregate.rs:256-275`)
- **File:** `src/adapter_aggregate.rs:256-275`
- **Issue:** Five sequential `if let Ok(x) = d.as_*()` early returns followed
  by a JSON fallback. Type-order dependency is fragile: `as_int` runs before
  `as_float`, but Rhai's `Dynamic` `as_float()` succeeds on int values too
  (number-tower coercion), so the int branch is what actually catches floats
  too. Subtle and undocumented.
- **Suggestion:** Use `d.type_id()` (or `d.type_name()`) to dispatch on the
  concrete type once. Eliminates the cascade and clarifies precedence.
- **Effort:** Low (~30 min).

### 3.3 `RhaiAggregateFn::create_accumulator` swallows init errors
- **File:** `src/adapter_aggregate.rs:65-81`
- **Issue:** `.unwrap_or(Dynamic::UNIT)` silently swallows any failure from
  `${name}_init`. Comment says "carries an error and reports it on first
  update", but no such error is actually stored — the accumulator just starts
  with `UNIT` state, and `accumulate(UNIT, x)` will fail with a Rhai runtime
  error that surfaces with no hint of the init failure.
- **Suggestion:** Either propagate the init error (change trait signature) or
  store the `EvalAltResult` on `RhaiAccumulator` and return it from the next
  `update_batch`. At minimum, log a tracing warning.
- **Effort:** Medium — depends on whether `create_accumulator` can be made
  fallible upstream.

### 3.4 `coerce_for` returns the original value on every miss
- **File:** `src/adapter_procedure.rs:139-161`
- **Issue:** Every match arm has a fall-through `Ok(value)` even when the
  declared target type is, say, `Float64` and the actual `Dynamic` is a
  string. This defers the type mismatch to `OutBuilder::push` which then
  produces a less-helpful error. The function exists specifically to coerce
  between int/float, but its current shape just hides type errors.
- **Suggestion:** Return `Err(FnError)` when value is neither unit nor
  coercible to target, with row + field-name context.
- **Effort:** Low (~30 min).

### 3.5 `apply_resource_limits` over-shares one budget
- **File:** `src/engine.rs:83-101`
- **Issue:** `Capability::MemoryBytes(n)` is divided by 4 and applied
  identically as the cap for string-size, array-size, and map-size. So
  `MemoryBytes(1MB)` becomes "every string, every array, every map ≤ 256KB"
  — counter-intuitive and not what an operator setting "1MB" likely expects
  (they probably expect the sum to be ≤ 1MB). Hardcoded `/4` and
  `max(1024)` aren't documented either.
- **Suggestion:** Either keep but rename the capability variant for clarity
  (`CollectionByteCap`), or implement an actual total-memory accounting using
  the progress callback. At minimum, document the heuristic in the rustdoc.
- **Effort:** Low (doc) to large (real accounting).

---

## 4. Unnecessary abstractions

### 4.1 Two-phase engine build in `RhaiLoader::load`
- **File:** `src/loader.rs:121-147`
- **Issue:** `load` builds a `probe_engine`, compiles `probe_ast`, parses
  manifest, then **rebuilds** another engine + recompiles the same AST. The
  comment claims engine 2 may differ because Phase 2 derives the effective
  set, but in practice both engines are built with `registrar_caps` (line 123
  and 145 — identical args). The recompile is pure waste.
- **Suggestion:** Reuse `probe_engine` and `probe_ast` for the runtime. If
  there is a future case where the second engine differs, gate the rebuild on
  a `caps != probe_caps` check. Saves a Rhai compile per plugin load (the
  expensive step in `RhaiLoader::load`).
- **Effort:** Low (~20 min). Caveat: ensure no host-fn state is mutated in
  place by the probe call.

### 4.2 `OutBuilder` enum vs trait objects
- **File:** `src/dynamic_bridge.rs:111-188`
- **Issue:** The 4-variant enum repeats `match self { Self::Bool(b) => ...,
  Self::Int(b) => ..., Self::Float(b) => ..., Self::Str(b) => ... }` three
  times: in `push`'s null branch, in `push`'s value branch, and in `finish`.
  ~50 LoC of match scaffolding.
- **Suggestion:** Either trait-objectify (`Box<dyn ArrayBuilder + Send>` with
  a small trait extension for `push_dynamic`) — keeps the enum site narrow —
  or extract a small `match_builder!($self, $b => $body)` macro. The enum
  shape is fine if kept; the duplication is the smell.
- **Effort:** Low.

### 4.3 `RhaiLoader` is a thin wrapper
- **File:** `src/loader.rs:63-99`
- **Issue:** `RhaiLoader` only holds `host_fns: RhaiHostFnRegistry`, and most
  of its API is pass-through (`host_fns_mut`, `host_fns`, `host_fn_count`).
  The `load` method takes `&self` and only uses `self.host_fns`. The loader
  abstraction adds little over passing `&RhaiHostFnRegistry` directly into a
  free `load` function.
- **Suggestion:** Consider either (a) folding `RhaiLoader` into a single
  `pub fn load_rhai_plugin(script, registrar, caps, host_fns) -> ...` free
  function, or (b) giving `RhaiLoader` more state (e.g., shared compile cache,
  hot-reload version index) that justifies the type. Option (a) matches
  `uni-plugin-extism`'s shape better if it uses a similar light wrapper.
- **Effort:** Low if collapsing; defer if state will arrive.

### 4.4 `_register_for_test` doc-hidden trapdoor
- **File:** `src/host_fn_impls/fs.rs:76-87`
- **Issue:** `_register_for_test` is `pub` + `#[doc(hidden)]`. This is a test
  helper that should live in `#[cfg(test)]`, not in the public surface (even
  hidden — it still appears in the ABI and downstream `cargo doc --document-
  private-items`). It also duplicates the spec built in `register()` above.
- **Suggestion:** Move into a `#[cfg(test)] pub(crate) mod test_support {
  ... }` module, or accept a closure-based constructor in `register()` that
  tests can call directly.
- **Effort:** Trivial.

### 4.5 `RhaiPluginRuntime::new` returns `Arc<Self>` (`runtime.rs:34-40`)
- **File:** `src/runtime.rs:34-40`
- **Issue:** Constructors returning `Arc<Self>` make it impossible for a
  caller to ever own a non-shared `RhaiPluginRuntime`, even briefly. This is
  a minor anti-pattern — the convention is `fn new() -> Self` then
  `Arc::new(...)` at the use site if shared. Cost is one `Arc::new` call at
  the loader site (already shared anyway).
- **Suggestion:** Return `Self`; let `loader.rs:147` wrap with `Arc::new`.
- **Effort:** Trivial.

### 4.6 Stub-only `build_engine` for non-runtime feature
- **File:** `src/engine.rs:79-80`
- **Issue:** `#[cfg(not(feature = "rhai-runtime"))] pub fn build_engine(...)
  -> () {}` returns unit so callers can't do anything with it; meanwhile most
  of the crate is `#![cfg(feature = "rhai-runtime")]` anyway. This stub
  appears callable in the public surface but only exists to keep one
  function name resolvable — and `lib.rs` already gates the re-export.
- **Suggestion:** Drop the non-runtime stub; the entire `engine` module
  should be `#[cfg(feature = "rhai-runtime")]` (like the rest), matching
  `lib.rs:66`.
- **Effort:** Trivial.

---

## 5. Minor cleanups

- **`adapter.rs:107-143` & `adapter.rs:145-218`** — `invoke_row` and
  `invoke_vectorized` both build `dyn_args`, create a `Scope`, call
  `engine.call_fn`, and classify errors with the same closure. Extract
  `fn call_rhai(&self, args: Vec<Dynamic>) -> Result<Dynamic, FnError>` and
  collapse the two functions' tails into it. ~15 LoC saved.
- **`adapter.rs:226-237` `classify_rhai_error`** — all match arms set
  `retryable: false`, so the second tuple element is dead. Simplify to just
  the code lookup; drop the retryable assignment loop. Mention in a comment
  that none of the trip conditions are retryable.
- **`columns.rs:215-218` `freeze`** — `std::mem::replace(&mut *b,
  Float64Builder::new()).finish()` allocates an extra empty builder just to
  satisfy the borrow. After fixing 3.1, `freeze` becomes
  `Arc::new(Float64Array::from(self.values))` and the dance is gone.
- **`manifest.rs:121` determinism default** — magic string `"pure"`. Define a
  `const DEFAULT_DETERMINISM: &str = "pure"` (or better, change the field to
  an enum so `wire_translate::determinism_to_volatility` is the only
  string-parsing site).
- **`adapter_aggregate.rs:224-254` `serde_json_to_dynamic`** — recursive but
  not depth-limited; a malicious peer state could induce stack overflow during
  merge. Add a depth budget (8-16 levels is plenty for v1 aggregate states).
- **`error.rs:9-11` `#[non_exhaustive]`** — combined with `pub` constructors
  for every variant, this is a low-value attribute on an internal enum. If
  the crate isn't planning to ship as a stable public API, drop it.
- **Test duplication** — `adapter_aggregate.rs:322-326`,
  `adapter_procedure.rs:186-190`, `columns.rs:256-260` all define a
  `build_runtime(script)` / `engine_with_columns()` helper with the same
  shape. Hoist into `#[cfg(test)] pub(crate) mod test_support` to share.

---

## Summary of effort

| Bucket | Items | Approx. effort |
|--------|-------|----------------|
| Trivial (delete/rename) | 2.1, 2.2, 2.4, 2.6, 4.4, 4.5, 4.6 | < 1 hr total |
| Low (extract helper / macro) | 1.2, 1.3, 1.4, 1.5, 3.2, 3.4, 4.1, 4.2, plus all of §5 | ~4 hrs total |
| Medium | 1.1 (column macro), 3.1 (correctness + perf), 3.3 (error-prop), 2.3 / 2.5 (wire variants properly) | ~4-6 hrs |
| Larger (semantic) | 3.5 memory-cap heuristic, 4.3 loader-shape decision | 1+ days each if pursued |

Highest-impact wins: **3.1** (correctness bug + O(n²)), **4.1** (avoid the
double-compile per plugin load), **1.4** (per-row allocation in the
accumulator hot loop), **1.1 + 1.2** (~120 LoC of duplication removed).
