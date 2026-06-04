# uni-plugin-pyo3 — Code Simplification Review

Scope: `/home/rohit/work/dragonscale/uni/.claude/worktrees/plugin-fw/crates/uni-plugin-pyo3/`
Focus: M8 PyO3 live-callable loader. Duplication, dead code, complex functions, unnecessary abstractions, FFI boundary.

Legend: **S** = small (<30 min), **M** = medium (30–120 min), **L** = large (>2 h).

---

## High-impact items

### 1. Two near-identical `PyPluginLoader` types live in the same crate (significant confusion / dead code)

- `src/lib.rs:150-188` defines a *non-feature-gated* `PyPluginLoader` carrying only a `HashMap<String, PyScalarFnRecord>` with register/lookup/unregister methods. Its sole tests just exercise that HashMap.
- `src/loader.rs:73-303` defines the *real* PyO3 loader (also named `PyPluginLoader`), re-exported from `lib.rs:88-91` under the alias `PythonPluginLoader` to disambiguate.
- The shadow `PyScalarFnRecord` / `PyDeterminism` / `PyPluginLoader` in `lib.rs` is M8.0 scaffolding superseded by `loader.rs` + `manifest.rs::PyScalarEntry`. Nothing in the crate consumes `PyScalarFnRecord` outside its own tests.
- **Action:** delete `lib.rs:104-255` (the scaffold types, alias, and tests) and rename `PythonPluginLoader` back to `PyPluginLoader` in re-exports. Verify no external bindings use `PyPluginLoader::register_scalar_fn` (the lib.rs surface).
- **Effort: M** (depends on external bindings audit; `register_scalar_fn` etc. is publicly exported).

### 2. `PyPluginLoader::load` and `load_from_builder` are 90 % duplicated

- `src/loader.rs:125-212` and `src/loader.rs:224-282` are structurally identical after the source-exec phase: manifest validation, plugin-id resolution, runtime construction, capability intersection, three conditional `register_*` calls returning the same `LoadOutcome`.
- **Action:** extract `fn finalize_load(&self, manifest, registrar, registrar_caps, module_name_for_id) -> Result<LoadOutcome, _>`; have both entry points call it. Reduces ~60 LOC and makes future post-load behavior changes apply to both paths.
- **Effort: S**.

### 3. `make_*_trampoline` free functions duplicate `PyDecoratorSink::{scalar_fn,aggregate_fn,procedure}`

- `src/loader.rs:709-781`: three `pub` free functions `make_scalar_trampoline`, `make_aggregate_trampoline`, `make_procedure_trampoline` perform the same `extract_args_list` + `PyDecoratorTrampoline::new_*` + `Py::new(...).into_any()` work as the pymethods at `src/loader.rs:600-676`.
- The free functions exist for `bindings/uni-db` to construct trampolines without the `_uni_decorator_sink` source-load path. Both call sites flow into identical trampoline construction.
- **Action:** have `PyDecoratorSink::scalar_fn` etc. delegate to the `make_*_trampoline` helpers (or vice versa). Eliminates 3×~15 LOC of parallel boilerplate. Alternative: move construction onto `PyDecoratorTrampoline::for_*` constructors that take the `Bound<PyAny>` directly.
- **Effort: S**.

### 4. `classify_pyerr` triplicated across adapters

- `src/adapter_scalar.rs:311-325`, `src/adapter_aggregate.rs:379-396`, `src/adapter_procedure.rs:291-308`. The three copies differ only in the error-code constant (`0x820` scalar/agg, `0x830` procedure) and the prefix string (`PyO3 `qname``, `PyO3 aggregate `qname``, `PyO3 procedure `qname``).
- **Action:** move to `adapter_scalar_helpers.rs` (or a new `errors.rs`) as `pub(crate) fn classify_pyerr(adapter_kind: &str, qname: &str, code: u32, e: PyErr) -> FnError` and call from all three sites. Saves ~45 LOC and centralizes the traceback-capture pattern (important since traceback handling is exactly the kind of thing that needs to be consistent across the FFI boundary).
- **Effort: S**.

### 5. `type_name_to_argtype` / `type_name_to_datatype` / `determinism_to_volatility` duplicated between loader and aggregate adapter

- `src/loader.rs:455-488` defines `type_name_to_datatype`, `type_name_to_argtype`, `determinism_to_volatility`.
- `src/adapter_aggregate.rs:339-367` defines a separate `type_name_to_argtype` plus an inline volatility match in `build_py_agg_signature`.
- Same string→DataType tables, same determinism strings, divergent error-code families (`0x80` vs the loader's `PyPluginError::ManifestInvalid`). The aggregate adapter's `build_py_agg_signature` is only called from the loader (`register_aggregates`), so the duplication is gratuitous.
- **Action:** put a single `pub(crate) mod type_table` (or extend `adapter_scalar_helpers`) hosting `argtype_from_name(&str) -> Result<ArgType, ...>`, `datatype_from_name(&str) -> Result<DataType, ...>`, `volatility_from_determinism(&str) -> Volatility`. Have both sites depend on a single error variant (define `From<ManifestInvalid>` if FnError-vs-PyPluginError signatures diverge).
- **Effort: M** (touches FnError↔PyPluginError boundary).

### 6. Per-call `lambda` / `type(...)` factory in `arrow_array_to_pyarrow`

- `src/arrow_bridge.rs:82-138`: each call to `arrow_array_to_pyarrow` (= once per vectorized batch *per column*) runs `py.eval(b"lambda _t: (lambda self, ...: _t)")`, imports `builtins`, fetches `type` and `object`, builds a fresh `_UniDbArrowCapsuleHolder` class via `type(name, bases, dict)`, then `pa.array(holder)` if pyarrow is present.
- For an N-column vectorized scalar over a batch this does N class creations and (if pyarrow available) N `pa.array()` round-trips just to satisfy `__arrow_c_array__`. Comment claims "the cost is dominated by FFI" but `py.eval` + dynamic type creation per column per batch is measurably non-trivial.
- **Action:** cache the holder class (one-time per `Python::attach`) using a `pyo3::sync::GILOnceCell<Py<PyType>>` stored in module-level state. The holder class stores capsules in an instance attribute and reads them in `__arrow_c_array__`. Then `arrow_array_to_pyarrow` becomes: build capsules → instantiate cached class with capsules → optionally `pa.array(instance)`.
- **Effort: M**. Significant FFI-perf win for vectorized UDFs (one of the project's named performance ceilings, `lib.rs:32-39`).

### 7. `pyarrow_to_arrow_array` unsafe pointer-swap is correct but hard to verify

- `src/arrow_bridge.rs:149-220`: the `ptr::read` + `ptr::write(empty)` dance to move out of the array capsule (avoiding double-release when the producer's capsule destructor runs) works, but the SAFETY comment runs into the comment-vs-code ratio. Three concerns:
  - The `schema_cap.clone()` and `array_cap.clone()` (lines 178, 184) are `Bound::clone` (refcount bump on the capsule), not deep clones — fine, but worth a one-line comment explaining why we clone.
  - `as_ptr() as *mut FFI_ArrowArray` (line 202) casts away `const` from `pointer_checked`'s `NonNull<c_void>`. This is intentional because we'll `ptr::write` through it, but should be commented.
  - If `from_ffi` fails between `ptr::read` and `ptr::write(empty)`, we leak (we've already done the read). The current code does the write *before* `from_ffi`, so this is actually fine — but the ordering is load-bearing for safety and worth a comment.
- **Action:** tighten the SAFETY block (one cohesive paragraph) and add an `// Important: write(empty) before from_ffi so a from_ffi failure doesn't leak.` line. No functional change.
- **Effort: S**.

### 8. `ScalarBuilder` and `ColumnBuilder` are duplicate enums

- `src/adapter_scalar.rs:239-308` (`ScalarBuilder` with `new`/`push_null`/`push_py_value`/`finish`) and `src/adapter_procedure.rs:215-289` (`ColumnBuilder` with `new`/`push_null`/`push_py`/`finish`) are the same 4-variant Arrow builder dispatch wrapping the same four datatypes (`Float64`/`Int64`/`Utf8`/`Boolean`).
- The procedure variant additionally goes through `py_to_scalar` → `match (self, scalar)` which is more verbose than the scalar variant's direct `value.extract()` route — and produces a less helpful error for unexpected-variant mismatches.
- **Action:** unify into one `pub(crate) PrimitiveArrayBuilder` in `adapter_scalar_helpers.rs` with both `push_py_value` (direct extract, scalar's path) and `push_py_via_scalar` (procedure's path, if needed) — or just standardize on the direct-extract approach in both places. Saves ~90 LOC.
- **Effort: M**.

### 9. `PyAccumulator::merge_batch` and `state` re-import `json` per call

- `src/adapter_aggregate.rs:225-280`: each `merge_batch` and `state` call does `py.import("json")?.getattr("loads/dumps")?`. Hot path; cross-partition merge runs once per partial → final, but `state()` runs once per group on the final aggregator and `merge_batch` runs once per partition.
- **Action:** cache `json.loads` / `json.dumps` references on the `PyAccumulator` (lazy `OnceCell<Py<PyAny>>` for each), or on the `PyPluginRuntime` (since the runtime is per-plugin). Modest perf, but more importantly the call-site reads `py.import("json")?.getattr("loads")?` four times across two methods — clearer with a `fn json_codec(&self, py)` accessor.
- **Effort: S**.

### 10. `extract_args_list` triple-branch can be a single `try_iter`

- `src/loader.rs:783-808`: separate branches for `PyList`, `PyTuple`, then generic iterable. `PyList` and `PyTuple` both implement the iterator protocol, so the generic `try_iter` branch handles them with one path.
- **Action:** drop the list/tuple specializations; keep just the `try_iter` form (use `size_hint` if you want `with_capacity`). Saves ~20 LOC, no measurable perf difference for a list of <10 type names.
- **Effort: S**.

### 11. `extract_agg_methods` early-return + late-return is asymmetric

- `src/loader.rs:953-991`: the dict branch unbinds inline with four `ok_or_else` mirrors; the attribute branch uses `getattr?.unbind()` four times. Both paths can share a closure `fn pull(key) -> PyResult<Py<PyAny>>` that switches on dict-vs-attr once. Currently the four error messages in the dict branch are mechanical duplication.
- **Action:** refactor to a single helper closure parameterized over the four method names: `["init", "accumulate", "merge", "finalize"].map(|k| pull(k))`. Saves ~25 LOC.
- **Effort: S**.

### 12. `PyDecoratorTrampoline` carries unused fields per variant

- `src/loader.rs:818-897`: one struct with all fields (`args`, `returns`, `yields`, `mode`, `vectorized`, `determinism`) for the union of three kinds. Each constructor leaves several fields at `default()`. This is the classic "fat struct" anti-pattern in disguise.
- **Action:** make `TrampolineKind` an `enum` with per-variant data (`Scalar { args, returns, vectorized, determinism }`, `Aggregate { args, returns, determinism }`, `Procedure { args, yields, mode }`). The `__call__` body becomes a clean `match` and impossible states (e.g., scalar-with-yields) are unrepresentable. ~30 LOC delta, cleaner invariants.
- **Effort: M** (touches the pyclass; pyo3 supports `enum` in pyclasses via `#[pyclass(eq)]` if attributes are needed, but here only `__call__` is exposed so a plain `enum` field works).

### 13. `PyDecoratorSink::new` shadows `from_builder`

- `src/loader.rs:587-593`: both methods are identical (`Self { builder }`); `new` is `pub(crate)` and `from_builder` is `pub`. Only one is needed.
- **Action:** delete `new`, use `from_builder` everywhere (one internal call site).
- **Effort: S** (1 minute).

### 14. `PyManifest::id` default sentinel `"py.live"` couples manifest and loader logic

- `src/manifest.rs:99-111` and `src/loader.rs:290`: the loader treats `"py.live"` as "not set" — but `"py.live"` is also a valid id a user might choose. This is a fragile sentinel.
- **Action:** make `PyManifest::id: Option<SmolStr>` (default `None`); `ManifestBuilder::set_id` becomes `Some(...)`. `resolve_plugin_id` reads `manifest.id.clone().or_else(|| self.default_plugin_id.clone()).or_else(...)`. Removes a hidden sentinel and simplifies the precedence chain.
- **Effort: S**.

### 15. `register_scalars` / `register_aggregates` / `register_procedures` share scaffolding

- `src/loader.rs:305-453`: each function does (a) build signature from entries, (b) iterate, (c) clone runtime under GIL, (d) insert callable(s) into runtime, (e) construct adapter, (f) call `registrar.xxx(...)`. The shape is identical except for the trait being registered.
- **Action:** can't fully unify (the three traits differ), but factor a private `register_one<F>(entries, signature_builder, adapter_factory, register_fn)` helper. Lower priority than items 2 and 4; the duplication here is structural and arguably clearer left split. Mention for completeness.
- **Effort: M**, **low value** — leave as-is unless someone touches all three.

---

## Minor / cosmetic

### 16. Dead variables / discouraged warnings

- `src/loader.rs:142-144, 157`: `module_name_c`, `filename_c` are built and then `let _ = (module_name_c, filename_c);` to silence unused warnings. They were presumably intended for `py.run`'s filename, but `py.run` doesn't take one. Drop the lets entirely.
- **Effort: S** (1 minute).

### 17. `PyPluginError::NotYetImplemented` unused

- `src/error.rs:52-57, 65-69`: the `not_yet` constructor and `NotYetImplemented` variant exist for M8 sub-milestones but `grep -rn NotYetImplemented` shows zero call sites now that M8 is complete. Either delete or document as "reserved for future deferred surfaces".
- **Effort: S**.

### 18. `PyPluginError::SignatureUnrecognized` unused

- `src/error.rs:18-19`: variant defined but never constructed. The loader uses `ManifestInvalid` for what its docstring describes.
- **Effort: S** (delete the variant or use it where appropriate; loader's "unknown type name `quaternion`" path looks like the intended use case but currently uses `ManifestInvalid`).

### 19. `PyPluginRuntime::get` always clones under the GIL

- `src/runtime.rs:53-58`: `get` does `Python::attach(|py| p.clone_ref(py))`. Callers (`adapter_*::lookup_callable`) immediately re-bind under another `Python::attach`. Each lookup costs two GIL acquisitions for a refcount bump.
- **Action:** add `get_with(name, py)` that takes an existing `Python<'_>` and avoids the inner attach. Adapters already have a `Python::attach` around their hot loop, so the lookup can move inside. Minor perf, modest clarity win.
- **Effort: S**.

### 20. `PyAggregateFn::create_accumulator` always boxes

- `src/adapter_aggregate.rs:77-83`: returns `Box<dyn PluginAccumulator>`. Fine for the trait. Note: `state: Option<Py<PyAny>>` on `PyAccumulator` is a per-group allocation that survives across `update_batch` calls; ensure DataFusion drops accumulators promptly. (Audit, not a fix.)

### 21. `arrow_bridge::arrow_array_to_pyarrow_capsules` is `pub` but only used internally

- `src/arrow_bridge.rs:52-67`: no external caller. Demote to `pub(crate)`. Same for `make_arrow_capsule`.
- **Effort: S**.

### 22. `scalar_value_to_py` in `adapter_procedure.rs` duplicates `scalar_to_py` semantics

- `src/adapter_procedure.rs:137-164`: builds Python values from `ScalarValue`; `adapter_scalar_helpers.rs::scalar_to_py` builds from `&dyn Array + row`. Two adjacent surfaces with different input shapes but the same output. Consider a `ScalarValueToPy` helper next to `scalar_to_py` in the helpers module so both adapters import from one place.
- **Effort: S**.

### 23. Test code triplicates `ensure_python()` + `runtime_with_*` helpers

- `src/adapter_scalar.rs:336-356`, `src/adapter_aggregate.rs:405-430`, `src/adapter_procedure.rs:322-342` each define `fn ensure_python() -> bool { Python::initialize(); true }` and a tiny module-loading helper. Could live once in a `pub(crate) mod test_support` gated `#[cfg(test)]`.
- **Effort: S**.

---

## Summary of recommended action order

1. (S) #1 delete the lib.rs scaffold `PyPluginLoader`/`PyScalarFnRecord` (largest dead-code win — but verify external bindings).
2. (S) #4 unify `classify_pyerr` (consistency at FFI boundary).
3. (S) #2 extract `finalize_load` helper.
4. (S) #3 collapse `make_*_trampoline` ↔ `PyDecoratorSink::*`.
5. (M) #5 unify type-name tables.
6. (M) #8 unify `ScalarBuilder` ↔ `ColumnBuilder`.
7. (M) #12 split `PyDecoratorTrampoline` into per-kind variants.
8. (M) #6 cache the `_UniDbArrowCapsuleHolder` class.
9. (S) #14 replace `"py.live"` sentinel with `Option<SmolStr>`.
10. (S) #9, #10, #11, #13, #16–#23 are quick cleanups.

Total estimated effort to land items 1–10: ~6–10 hours of focused work, all behavior-preserving with adequate test coverage already in place.
