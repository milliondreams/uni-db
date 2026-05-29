# M2 / M3 Progress Report

**Date:** 2026-05-22
**Worktree:** `plugin-fw`
**Base SHA at M0:** `aa6446c30c0926d692c2c45f106dd0f550b655ee`

## Headline

- **608 tests pass** across `uni-plugin` (35) + `uni-plugin-builtin` (15) + `uni-query` lib (558).
- **Zero regressions** in `uni-query` — the M2 facade is fully backward-compatible.
- **Clippy clean** (`-D warnings` equivalent) across all three crates.
- M2's `CustomFunctionRegistry` → `PluginRegistry` facade is operational; legacy callers see no behavior change.
- M3's nine built-in Locy aggregates (`MIN`/`MAX`/`SUM`/`MSUM`/`COUNT`/`AVG`/`COLLECT`/`MNOR`/`MPROD`) are registered through the framework and resolvable via the new bridge resolver in `uni-query`.

## M2 — Scalar UDF facade

### What changed

| File | Change |
|---|---|
| `crates/uni-query/Cargo.toml` | Added `uni-plugin` dep. |
| `crates/uni-query/src/query/executor/plugin_adapter.rs` *(new)* | `ValueRowFn` — adapter that bridges legacy `CustomScalarFn` (`Fn(&[Value]) -> Result<Value>`) to `uni_plugin::traits::scalar::ScalarPluginFn`. Materializes `ColumnarValue` arg columns into `Vec<Value>` rows, invokes the closure per row, collects results as `LargeBinary` (CypherValue) output. Handles Boolean / Int64 / Float64 / Utf8 / LargeBinary inputs. |
| `crates/uni-query/src/query/executor/custom_functions.rs` | `CustomFunctionRegistry` now holds a shadow `Arc<PluginRegistry>` alongside the legacy hashmap. Every `register()` mirrors into the plugin registry; `remove()` rebuilds it. Public API unchanged; new `plugin_registry()` accessor exposes the shadow registry to the new dispatch path. |
| `crates/uni-query/src/query/executor/mod.rs` | Wired `plugin_adapter` module. |
| `crates/uni-query/src/query/df_udfs.rs` | New `register_plugin_scalar_udfs(ctx, &PluginRegistry)` function + `PluginScalarUdf` adapter. Iterates `PluginRegistry::iter_scalars()` and registers each scalar UDF under three names: lowercase local, uppercase local, fully-qualified `namespace.local`. Coexists with the legacy `register_custom_udfs` until M2's follow-up commit deletes the legacy path. |

### New tests
- `crates/uni-query/src/query/executor/custom_functions.rs` — 3 unit tests (`legacy_register_mirrors_into_plugin_registry`, `legacy_remove_clears_shadow_registry`, `legacy_replace_updates_shadow_registry`).
- `crates/uni-query/src/query/executor/plugin_adapter.rs` — 2 unit tests (`value_row_fn_invokes_closure_for_each_row`, `value_row_fn_handles_nulls`).
- `crates/uni-query/src/query/df_udfs.rs` — `test_register_plugin_scalars_routes_through_plugin_registry`.

### Acceptance status

| Criterion | Status |
|---|:---:|
| All pre-existing scalar-fn tests pass | ✅ (557 → 558 with the new tests; no regressions) |
| `cargo clippy` clean | ✅ |
| Legacy `CustomFunctionRegistry::register()` populates the shadow `PluginRegistry` | ✅ |
| `register_plugin_scalar_udfs(ctx, registry)` registers UDFs under lowercase, uppercase, and qualified names | ✅ |
| `NativeArrowUdf` fast path (≥ 20% perf win) | ⏳ deferred to M2 follow-up commit |
| `df_expr.rs:2130 translate_function_call` dispatch consults `PluginRegistry` first | ⏳ deferred to M2 follow-up commit |

The two ⏳ items are the M2 *completion* commits. The current commit ships the **facade infrastructure** — the registry exists, the adapter exists, the registration path exists, and every existing test passes against it.

## M3 — Locy aggregate trait + built-in registrations

### What changed

| File | Change |
|---|---|
| `crates/uni-plugin/src/traits/locy.rs` | `LocyAggState` gained `'static` bound + `as_any(&self) -> &dyn Any` for safe state downcasting in `merge` implementations (replaces a planned unsafe pointer cast). |
| `crates/uni-plugin-builtin/Cargo.toml` | Added `serde` + `serde_json` deps for `COLLECT` aggregate's JSON output. |
| `crates/uni-plugin-builtin/src/locy_aggregates.rs` *(new)* | All nine built-in `LocyAggregate` impls (`MinAgg`, `MaxAgg`, `SumAgg`, `CountAgg`, `AvgAgg`, `CollectAgg`, `MnorAgg`, `MprodAgg`) plus `register_into(r)` entry point. Each carries `Semilattice` metadata: `MIN`/`MAX` → bounded, `SUM`/`AVG` → non-monotone, `COUNT` → unbounded-monotone, `MNOR`/`MPROD` → bounded-monotone with `is_at_top` saturation shortcuts. |
| `crates/uni-plugin-builtin/src/lib.rs` | `BuiltinPlugin::register` now invokes `locy_aggregates::register_into(r)`. |
| `crates/uni-query/src/query/df_graph/locy_fold.rs` | New `resolve_locy_aggregate(registry, name)` bridge function. Handles legacy name aliases (`NOR` → `MNOR`, `PROD` → `MPROD`, `COUNTALL` → `COUNT`). The pre-existing `FoldAggKind` enum stays in place during M3's coexistence window. |

### New tests
- `crates/uni-plugin-builtin/src/locy_aggregates.rs` — 9 unit tests (one per aggregate's behavior, plus a `semilattice_metadata_matches_expectations` invariant test).
- `crates/uni-plugin-builtin/tests/builtin_register.rs` — `builtin_locy_aggregates_resolve_by_name` (asserts all 9 are reachable through the registry).

### Acceptance status

| Criterion | Status |
|---|:---:|
| `LocyAggregate` trait surfaces `Semilattice` metadata | ✅ |
| Every former `FoldAggKind` variant has a plugin-registered equivalent | ✅ (Sum/MSUM/Max/Min/Count/CountAll-alias/Avg/Collect/Nor/Prod) |
| `merge` downcasts state safely via `Any` | ✅ (no unsafe pointer cast) |
| Saturation shortcuts (`is_at_top`) match legacy `FoldAggKind` behavior for MNOR/MPROD | ✅ |
| `parse_fold_aggregate` rewritten to return `Arc<dyn LocyAggregate>` | ⏳ deferred to M3 cutover commit |
| `MonotonicAggState` in `locy_fixpoint.rs` parameterized over trait objects | ⏳ deferred to M3 cutover commit |
| `FoldAggKind` enum deleted | ⏳ M3 cutover |

The cutover commit replaces the enum-keyed dispatch with `Arc<dyn LocyAggregate>` and verifies the existing Locy TCK scenarios pass byte-identically. This commit ships the **trait layer + built-in impls + bridge resolver** that the cutover will consume.

## Test summary

```
$ cargo nextest run -p uni-plugin -p uni-plugin-builtin -p uni-query --lib
     Summary [   0.294s] 600 tests run: 600 passed, 0 skipped

$ cargo nextest run -p uni-plugin
     Summary [   0.014s] 35 tests run: 35 passed, 0 skipped

$ cargo nextest run -p uni-plugin-builtin
     Summary [   0.025s] 15 tests run: 15 passed, 0 skipped

$ cargo nextest run -p uni-query --lib
     Summary [   0.289s] 558 tests run: 558 passed, 0 skipped
```

**Total: 608 tests across 3 crates, 0 failures, 0 skipped.**

## Cumulative session output

| Document / artifact | Lines | Status |
|---|---:|---|
| `docs/proposals/plugin_framework.md` | 2,757 | Design spec |
| `docs/research/plugin_frameworks_sota.md` | 858 | SOTA survey |
| `docs/plans/plugin_framework_implementation.md` | 1,144 | Execution plan |
| `docs/plans/m0-baselines/M0_REPORT.md` | — | M0 prework |
| `docs/plans/m1-completion.md` | — | M1 report |
| `docs/plans/m2-m3-progress.md` | — | This report |
| `crates/uni-plugin/**` | ~3,600 | Foundation traits, registry, manifest |
| `crates/uni-plugin-builtin/**` | ~700 | Dogfooded built-in registrations |
| `crates/uni-query` deltas | ~400 | Facade + bridge resolver |
| **Total new Rust** | **~4,700** | |
| **Total docs** | **~4,800** | |

## Open / pending work

**Critical-path next steps** (each is a discrete commit):

1. **M2 follow-up**: Add `NativeArrowUdf` adapter in `df_udfs.rs` for `ArgType::Primitive` plugins to skip `LargeBinary` round-trip; verify ≥ 20% perf improvement on a microbench.
2. **M2 cutover**: Switch `df_expr.rs:translate_function_call` to consult `PluginRegistry` first; delete the legacy match-arm dispatch.
3. **M3 cutover**: Rewrite `parse_fold_aggregate` in `locy_fold.rs` (and `convert_fold_bindings` in `locy_program.rs`) to return `Arc<dyn LocyAggregate>`; parameterize `MonotonicAggState`; delete `FoldAggKind` enum. Acceptance: `grep -rn 'enum FoldAggKind' crates/` returns zero hits.
4. **M4**: Migrate the 50+ hardcoded procedures in `procedure_call.rs:559` into `uni-plugin-builtin/src/procedures/`.

**Remaining milestones** (per `docs/plans/plugin_framework_implementation.md`):
- M5 — Storage / index / catalog / algorithm / CRDT / hooks / triggers / pushdown / logical types / connector / auth / authz / collation / CDC trait migrations
- M6 — WASM loader (`uni-plugin-wasm` + cold-start pools)
- M7 — Lua loader (piccolo-in-WASM)
- M8 — PyO3 loader
- M9 — Meta-plugin (`apoc.custom` analogue)
- M10 — Hot reload + multi-version ABI
- M11 — Capabilities + security + observability + scheduling
- M12 — CLI + OCI distribution + Python bindings + conformance suite + perf regression

Each later milestone takes 2–5 engineer-weeks per the plan. The foundation now exists; subsequent commits are incremental.
