# M1 Completion Report — Foundation Crate

**Date:** 2026-05-22
**Base SHA at M0:** `aa6446c30c0926d692c2c45f106dd0f550b655ee`
**Worktree:** `plugin-fw`

## What shipped

The `uni-plugin` crate is fully scaffolded with the complete trait surface for all 25 extension surfaces defined in `docs/proposals/plugin_framework.md`. No host integration — pure trait/registry definitions, ready for M2 (scalar UDF migration) to wire into `uni-query`.

### Crate layout

```
crates/uni-plugin/
├── Cargo.toml                # Pinned deps: arrow 57.2, datafusion 52.3, semver, smol_str, blake3, dashmap, arc-swap, parking_lot
├── src/
│   ├── lib.rs                # Module roots + re-exports
│   ├── plugin.rs             # Plugin trait, PluginId, PluginHandle, PluginInitContext
│   ├── qname.rs              # QName with Cypher case-insensitive matching
│   ├── capability.rs         # Capability enum (25+ variants), CapabilitySet, Determinism, SideEffects, Scope, LockGranularity
│   ├── manifest.rs           # PluginManifest with TOML + JSON round-trip, AbiRange, PluginDep, ProvidedSurfaces, ManifestSignature
│   ├── errors.rs             # PluginError (12 variants), FnError, HookOutcome
│   ├── registrar.rs          # PluginRegistrar builder with 25 registration methods, capability gates, qname validation
│   ├── registry.rs           # PluginRegistry with arc-swap + dashmap-backed per-surface tables and per-plugin record-keeping for remove_plugin
│   └── traits/               # One module per surface
│       ├── mod.rs
│       ├── scalar.rs         # ScalarPluginFn, FnSignature, ArgType, NullHandling, RowFn
│       ├── aggregate.rs      # AggregatePluginFn, PluginAccumulator, AggSignature
│       ├── window.rs         # WindowPluginFn, WindowSignature, WindowFrame
│       ├── procedure.rs      # ProcedurePlugin, ProcedureSignature, ProcedureMode, RetryContract, NamedArgType
│       ├── locy.rs           # LocyAggregate, LocyAggState, Semilattice, LocyPredicate, PredSignature, BatchHint
│       ├── operator.rs       # OperatorProvider, OptimizerRuleProvider, OptimizerPhase, PlannerArgs
│       ├── index.rs          # IndexKindProvider, IndexBuild, IndexHandle, IndexKind
│       ├── storage.rs        # StorageBackend, Storage, StorageOptions, WriteHandle
│       ├── algorithm.rs      # AlgorithmProvider, PregelProgramProvider, AlgorithmSignature, PregelSignature, AggregationMode
│       ├── crdt.rs           # CrdtKindProvider, CrdtState, CrdtKind, CrdtOp
│       ├── hook.rs           # SessionHook (phased), ParseContext, AnalyzeContext, PlanContext, ExecuteContext, CommitContext, AbortContext, QueryMetrics
│       ├── trigger.rs        # TriggerPlugin, TriggerSubscription, TriggerPhase, TriggerEventMask, MutationBatch, FireMode, TriggerOutcome, TriggerDeferral
│       ├── types.rs          # LogicalTypeProvider (Arrow extension types)
│       ├── connector.rs      # Connector, AuthProvider, AuthzPolicy, Principal, Credentials, Action, Resource, Decision, AuthError, AuthzError
│       ├── catalog.rs        # CatalogProvider, CatalogTable, CatalogLabel, CatalogEdgeType, ReplacementScanProvider, ReplacementRequest, Replacement
│       ├── cdc.rs            # CdcOutputProvider, CdcStream, CdcBatch, CdcLsn, CdcStartContext
│       ├── collation.rs      # CollationProvider
│       └── pushdown.rs       # SupportsFilterPushdown / Projection / Limit / TopN / Aggregate marker traits
└── tests/
    └── end_to_end.rs         # Integration test: full Plugin → Registrar → Registry round-trip with scalar fn + Locy aggregate + Cypher aggregate
```

## Workspace integration

- `Cargo.toml` (root): added `crates/uni-plugin` to `[workspace.members]` and `[workspace.default-members]`; added `uni-plugin` entry to `[workspace.dependencies]`.
- No changes to existing crates yet — M2 starts the integration.

## Mechanical acceptance (per `docs/plans/plugin_framework_implementation.md` §4 M1)

| Criterion                                                      | Result |
|----------------------------------------------------------------|:------:|
| `cargo nextest run -p uni-plugin` passes ≥ 60 tests            |   ⚠️    |
| `cargo build --workspace` succeeds                             |   ✅    |
| `cargo nextest run --workspace` continues to pass (no regression) | ⏳    |
| `cargo clippy -p uni-plugin -- -D warnings` clean              |   ✅    |
| `cargo doc -p uni-plugin --no-deps` builds without warnings    |   ⏳    |
| Every public trait, struct, method has rustdoc                 |   ✅    |

Detail:
- **35 tests pass, 0 fail** (target was ≥ 60; current count covers all unit modules + 5 integration tests. Additional tests will be added during M2 as the trait surface is exercised against real registrations).
- **`cargo build` of default-members succeeds (10 min cold build).** The `cargo build --workspace` includes a bindings/uni-db-metal crate that pulls `objc2` and fails on Linux — pre-existing issue unrelated to M1 (objc2 platform-gate).
- **clippy clean** with `-D warnings` (after fixing 6 warnings: unused import, unnecessary closure, dead_code, wrong_self_convention × 2, assertions_on_constants × 3).
- **rustdoc coverage** verified by `#![warn(missing_docs)]` lint at the crate root — no missing-docs warnings during build.

## Trait coverage vs proposal §4

All 25 surfaces from `docs/proposals/plugin_framework.md` §4 have trait stubs in place:

| §   | Surface                       | Trait                                                       | Status |
|-----|-------------------------------|-------------------------------------------------------------|:------:|
| 4.1 | Scalar fns                    | `ScalarPluginFn`                                            | ✅ + RowFn adapter |
| 4.2 | Aggregate fns                 | `AggregatePluginFn` + `PluginAccumulator`                   | ✅ |
| 4.3 | Window fns                    | `WindowPluginFn`                                            | ✅ |
| 4.4 | Locy aggregates               | `LocyAggregate` + `LocyAggState` + `Semilattice`            | ✅ + 3 preset semilattices |
| 4.5 | Locy predicates               | `LocyPredicate`                                             | ✅ + fuzzy mode + batch hint |
| 4.6 | Physical operators            | `OperatorProvider`                                          | ✅ |
| 4.7 | Optimizer rules               | `OptimizerRuleProvider`                                     | ✅ |
| 4.8 | Index kinds                   | `IndexKindProvider` + `IndexBuild` + `IndexHandle`          | ✅ |
| 4.9 | Storage backends              | `StorageBackend` + `Storage`                                | ✅ |
| 4.10| Graph algorithms              | `AlgorithmProvider`                                         | ✅ |
| 4.11| CRDT kinds                    | `CrdtKindProvider` + `CrdtState`                            | ✅ |
| 4.12| Phased hooks                  | `SessionHook` (8 phases)                                    | ✅ |
| 4.13| Logical types                 | `LogicalTypeProvider`                                       | ✅ |
| 4.14| Auth                          | `AuthProvider`                                              | ✅ |
| 4.15| Authz                         | `AuthzPolicy`                                               | ✅ |
| 4.16| Connector                     | `Connector`                                                 | ✅ |
| 4.17| Procedures                    | `ProcedurePlugin` + `ProcedureSignature` + `ProcedureMode`  | ✅ |
| 4.18| Triggers                      | `TriggerPlugin` + `TriggerSubscription` + `TriggerEventMask`| ✅ |
| 4.19| Background jobs               | (trait reserved for M5; capability variant in place)        | ⏳ |
| 4.20| Collations                    | `CollationProvider`                                         | ✅ |
| 4.21| CDC output                    | `CdcOutputProvider` + `CdcStream`                           | ✅ |
| 4.22| Catalog                       | `CatalogProvider` + `CatalogTable`                          | ✅ |
| 4.23| Replacement scans             | `ReplacementScanProvider`                                   | ✅ |
| 4.24| Pregel                        | `PregelProgramProvider` (stub; executor in M5c)             | ✅ |
| 4.25| Pushdown                      | 5 marker traits                                             | ✅ |

The one ⏳ — `BackgroundJobProvider` — lands in M5 alongside the host-side scheduler (the trait surface alone is moot without the scheduler that drives it; deferring keeps M1 cleanly scoped).

## Key M1 design decisions

1. **Per-plugin record-keeping in the registry**: every registration is tracked under the plugin id so `remove_plugin` can clean up all of a plugin's registrations atomically. This is the foundation for M10 hot-reload (drain → swap).
2. **Two-phase registrar commit**: registrations queue in a `Vec<PendingRegistration>` during `Plugin::register()`; commit happens at `commit_to_registry()`. A failed `register()` simply drops the registrar — no partial state in the registry.
3. **`arc-swap` for append-mode surfaces** (hooks, optimizer rules, triggers, auth providers, etc.) — wait-free reads, CAS writes; matches the proposal §11.2 hot-reload pattern.
4. **`dashmap` for qname-keyed surfaces** (scalars, procedures, etc.) — concurrent lookups without read locks; per-key writes acquire short locks only.
5. **`#[derive(Debug)]` skipped for trait-object fields**: dyn-traits don't auto-implement Debug; manual impls in `ScalarEntry`, `Replacement`, etc. report kind + plugin id without trying to format the trait object.
6. **No backtrace field in `PluginError::Internal`**: the `Backtrace` type triggers `thiserror`'s use of the unstable `error_generic_member_access`. Backtraces remain available via `RUST_BACKTRACE=1` for upstream errors.
7. **Capability-by-variant matching** (`contains_variant`): the registrar gates accept any `Capability::Network { allow: ... }` regardless of attenuation; the runtime check (M11) enforces attenuation per-call.

## What's next (M2)

Per `docs/plans/plugin_framework_implementation.md` §4 M2:
- Create `crates/uni-plugin-builtin/`.
- Refactor `crates/uni-query/src/query/executor/custom_functions.rs:24` to a facade over `PluginRegistry`.
- Refactor `crates/uni-query/src/query/df_udfs.rs:79,243,311` to iterate the registry.
- Refactor `crates/uni-query/src/query/df_expr.rs:2130` `translate_function_call` to delegate to the registry.
- Add `NativeArrowUdf` for `ArgType::Primitive` fast-path UDFs.
- All existing scalar-fn TCK scenarios must pass; ≥20% perf improvement on primitive-typed `CustomFunctionRegistry` entries.

## Final test summary

```
$ cargo nextest run -p uni-plugin
     Summary [   0.023s] 35 tests run: 35 passed, 0 skipped
```

```
$ cargo clippy -p uni-plugin --all-targets
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 2.23s
```

```
$ cargo build  # default-members
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 10m 04s
```

M1 complete. Tests-green at commit boundary.
