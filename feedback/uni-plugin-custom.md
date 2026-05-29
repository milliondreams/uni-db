# Code Simplifier Review — `crates/uni-plugin-custom/`

Scope: recently modified files `src/aggregate.rs` and `src/lib.rs`; supporting context from `src/scalar.rs`, `src/eval.rs`, `src/persistence.rs`.

---

## 1. Cross-module helper duplication (HIGH priority)

Multiple small helpers are copied verbatim (or near-verbatim) across `aggregate.rs`, `scalar.rs`, and `procedures` mod in `lib.rs`. The header comment at `aggregate.rs:286-287` already acknowledges this ("duplicated here ... If a third caller appears, promote into a shared helper module") — and the third caller has now arrived.

| Helper | Locations | Notes |
| --- | --- | --- |
| `array_value_at` | `aggregate.rs:288-362`, `scalar.rs:144-215` | Aggregate version adds `LargeBinary` + uses `match` instead of nested `if`. Should be one shared impl. |
| `stringify` | `aggregate.rs:272-281`, `scalar.rs:291-300` | Identical bodies. |
| `eval_err_to_fn` | `aggregate.rs:227-235`, `scalar.rs:125-133` | Identical bodies. |
| `type_str_to_arrow` | `aggregate.rs:461-469`, `lib.rs:1005-1013` | Identical. |
| `declared_plugin_id` | `aggregate.rs:471-476`, `lib.rs:1015-1023` | Identical. |
| `local_part` | `aggregate.rs:478-480`, `lib.rs:1025-1027` | Identical. |
| `map_plugin_error` | `aggregate.rs:454-459`, `lib.rs:949-954` | Identical. |
| Synthetic plugin `manifest_owned` | `aggregate.rs:500-517`, `lib.rs:1047-1064` | Same pattern, two-line diff (`Capability::AggregateFn` vs `ScalarFn`, docs string). |
| Synthetic plugin install boilerplate | `aggregate.rs:429-451`, `lib.rs:925-946` | qname construction + `PluginRegistrar::new` + `register` + `commit` is a copy-paste idiom. |

**Suggestion**: introduce a private `mod common` (or `mod synth`) containing `array_value_at`, `stringify`, `eval_err_to_fn`, `type_str_to_arrow`, `declared_plugin_id`, `local_part`, `map_plugin_error`. Have `scalar::array_value_at` delegate to the unified version (the only "hot loop" justification is now contradicted by aggregate.rs using the same helper). Extract a `register_synthetic_plugin(...)` helper that accepts the manifest + a closure-style `register` body to remove the ~25-line duplicated registrar dance.

**Effort**: ~1-2 hours (mechanical extraction + delete duplicates + ensure no `clippy::module_name_repetitions` regressions).

---

## 2. `lib.rs` is overweight (MEDIUM priority)

`lib.rs` is 2003 lines with a 600-line inline `pub mod procedures` and a 1693-2002 `#[cfg(test)] mod tests` block. Crate docs hint the file mixes facade, store, procedures, helpers, and tests.

**Suggestion**: split:
- `src/store.rs` — `DeclaredPluginStore`, `would_introduce_cycle`, `chain_starting_at` (`lib.rs:1506-1691`).
- `src/procedures/mod.rs` — currently `pub mod procedures` inline at `lib.rs:444-1504`; move to file.
- `src/procedures/declare_kind.rs` — the `declare_kind_procedure!` macro and its two expansions.
- `src/synth.rs` — `SyntheticScalarPlugin` + `SyntheticAggregatePlugin` + `register_synthetic_plugin` helper.

Top-level `lib.rs` would shrink to ~250 lines (re-exports, `CustomError`, `CustomPlugin`, `ProcedureBodySynthesizer`, `Plugin` impl).

**Effort**: ~2 hours (move + adjust visibility + path qualifiers).

---

## 3. Dead / questionable code

- `lib.rs:1686-1691` `chain_starting_at` returns `vec![start.to_owned()]` and ignores the map entirely. The cycle-report value is misleading — caller expects the cycle chain but only gets the seed. Either compute the real chain (DFS during `would_introduce_cycle` already has the visited set) or drop the function and inline `vec![plugin.qname.clone()]` at the single call site (`lib.rs:1548`). Effort: 15 min (inline) or 1 h (real chain).
- `scalar.rs:302-308` `mod _silence` exists only to keep a `ListBuilder` import "explicit". This is the textbook anti-pattern for justified dead code. Either implement list-return now or remove the import + comment until needed. Effort: 5 min.
- `lib.rs:1253` In the `declare_kind_procedure!` macro, the no-synthesizer constructor creates a throwaway `Arc::new(uni_plugin::PluginRegistry::new())` and stores it in `self.registry`, but `self.registry` is only read when `synthesizer.is_some()`. This is a phantom field whose default value is allocated on every `new()`. Use `Option<Arc<PluginRegistry>>` or only allocate inside `new_with_synthesis`. Effort: 30 min.
- `aggregate.rs:222-224` `PluginAccumulator::size` returns `std::mem::size_of::<Self>()` which is constant and ignores the heap-allocated `state: Option<Value>` (Value can hold Strings/Lists). DataFusion uses `size()` for memory accounting; this under-reports. Either include a recursive `Value::size_of()` or document the limitation. Effort: 30 min – 2 h.
- `aggregate.rs:186` `let st = self.state.clone().unwrap_or(Value::Null);` — `ensure_state` was just called and guarantees `Some`. Replace with `let st = self.state.as_ref().expect("ensure_state guarantees state").clone();` or `mem::replace`. Effort: 10 min.

---

## 4. Complex / repetitive functions (MEDIUM priority)

- `lib.rs:818-881` `DeclareFunctionProcedure::invoke` and `lib.rs:1147-1217` `DeclareAggregateProcedure::invoke` share a ~30-line pattern: extract args → build record → `store.declare` → call `install_*_into_registry` → handle `NativeShadow` shadow-downgrade vs rollback vs persist → `single_bool("registered", true)`. The macro-expanded `declare_kind_procedure!` at `lib.rs:1225-1391` follows the same structure for "procedure"/"trigger" minus the registry install.

  **Suggestion**: a helper `fn finalize_declaration<F>(store, persistence, record, install: F, err_code: u32) -> Result<bool, FnError>` collapses the three flows. Effort: 1.5 h.

- `lib.rs:1284-1389` macro body is ~100 lines including capability gating that only applies to `$kind == "procedure"`. The `if $kind == "procedure"` block is compile-time deterministic per expansion; consider splitting the macro into `declare_procedure_kind!` (with mode/capability gate) and `declare_trigger_kind!` (without), or pulling the body out into a free function `record_only_declare(store, persistence, registry, synthesizer, kind, args)`. The macro right now generates duplicated struct definitions but only differs in three constants; a regular generic-over-`KindMeta` struct would be clearer. Effort: 2-3 h.

- `aggregate.rs:288-362` `array_value_at` mixes two styles (`as_primitive_opt` fallback for Int32/Float32, plain `downcast_ref` elsewhere). Once unified, prefer the explicit `downcast_ref` style throughout — the `as_primitive_opt` then `unwrap_or_else` ladder at lines 307-316 / 324-333 is harder to read than a straight match. Effort: 30 min.

- `aggregate.rs:243-270` `value_to_scalar` — long `match` is fine; flag only because `(DataType::Utf8, other) => Ok(ScalarValue::Utf8(Some(stringify(other))))` is the only branch invoking `stringify`. Once `stringify` moves to common, fine.

---

## 5. Unnecessary abstraction

- `DeclaredPluginStore::replace` (`lib.rs:1638-1640`) is a one-line alias for `declare_unchecked` with no added validation. Either inline calls (`self.store.declare_unchecked(...)`) or keep one method. Effort: 15 min.
- `lib.rs:170-191` `ProcedureBodySynthesizer` trait is justified (host-side wiring), but the docs note this is a workaround for `uni-plugin-custom`'s missing `uni-query` dep. If/when that constraint changes, the trait can collapse. No action now; flag for follow-up.

---

## 6. `extract_string` / `extract_string_or` near-duplication (LOW priority)

`lib.rs:1415-1435` (`extract_string_or`) and `lib.rs:1449-1478` (`extract_string`) duplicate the Utf8 downcast logic. The optional version could be implemented as `extract_string(...).ok().unwrap_or_else(|| default.to_owned())` if error semantics were aligned (currently they differ for null-vs-missing). Suggest:

```text
fn extract_string_opt(args, i, name) -> Result<Option<String>, FnError>
fn extract_string(args, i, name) -> Result<String, FnError>      // wraps; errors on None
fn extract_string_or(args, i, name, default) -> Result<String, FnError>  // wraps; defaults on None
```

Effort: 30 min.

---

## 7. Tests

- `lib.rs:1809-1815` and `1842-1848` construct `CustomPlugin { ... }` with all private fields. Means `CustomPlugin` must keep field-level pub-in-crate visibility just for tests. Consider a `#[cfg(test)] pub(crate) fn with_parts(...)` constructor to make these tests stable across field additions. Effort: 20 min.
- `lib.rs:1864-1876` `drive_declare_procedure` is a test helper duplicated mentally between the three `declare_procedure_*` tests; fine as-is.

---

## Summary of priorities

| Priority | Item | Effort |
| --- | --- | --- |
| HIGH | §1 Extract shared `common` helpers module | 1-2 h |
| MEDIUM | §2 Split `lib.rs` into store / procedures / synth files | 2 h |
| MEDIUM | §4 Unify three declare* invoke flows | 1.5-3 h |
| LOW | §3 Dead code (chain_starting_at, _silence mod, phantom registry, size()) | 1-2 h total |
| LOW | §5-7 Misc abstraction + extract_string + test ctor | ~1 h total |

Total cleanup budget: roughly one focused day. None of the suggestions alter externally observable behavior; §1 and §2 are by far the highest leverage and unlock further consolidation.
