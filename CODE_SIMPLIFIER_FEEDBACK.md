# Code Simplifier Feedback — Workspace-Wide Review

Date: 2026-05-27
Branch: `main` (worktree `plugin-fw`)
Scope: All 22 crates in `crates/`, reviewed by parallel `code-simplifier` subagents.
Mode: **Review-only** — no source files were modified.

Per-crate detail reports live in `feedback/<crate>.md` next to this file.
Total findings collected: ~4500 lines across 22 reports.

## Status (updated 2026-05-28)

- **§2 Correctness** — ✅ DONE in commit `3f571157f` ("post-simplifier bug
  sweep"). 19 fixes landed with regression tests; 4 items refuted with
  rationale in the commit body.
- **§3 Dead code** — 🟡 PARTIAL. The 5 truly DEAD items + 1 VESTIGIAL scaffold
  have been deleted in this PR (see §3 below for the strikethrough table). The
  remaining items were reclassified as STUBs for unfinished features and
  tracked in `docs/KNOWN_GAPS.md` so they weren't silently erased (that file was
  removed once every gap closed — see git history).
- **§4 Hygiene** — ✅ DONE in commit `19b2281` ("chore(hygiene)"). All
  four items addressed plus one extra duplication found in passing; see
  the strikethrough table in §4 below. Two items the doc flagged were
  refuted on inspection (`0xB33` was already renumbered to `0xB37`;
  apoc-core `0x700`-`0x800` codes are distinct per-procedure, not
  duplicates).
- **§1.2 Async / principal threading** — ✅ DONE (2026-05-28).
  `uni-plugin::host::{context::build_procedure_context, principal::maybe_scope_with_principal}`
  collapses the 25-line procedure-context block (uni-query) and 5+ `match
  principal` sites (uni-db/uni-query). `CURRENT_PRINCIPAL` task-local moved
  into `uni-plugin::host::principal`, re-exported from `uni-query`. Threading
  policy documented on `BackgroundJobProvider::execute` / `TriggerPlugin::fire`
  + long-form `docs/PLUGIN_THREADING.md`. Hand-rolled `CancellationToken`
  replaced with `tokio_util::sync::CancellationToken` (side benefit: fixed a
  latent disconnect where `Scheduler::cancel(id)` didn't reach in-flight runs).
- **§1.1 Plugin-framework registration boilerplate** — ✅ DONE (2026-05-28).
  - ✅ Loader commons consolidated in `uni-plugin::adapter_common`
    (`arrow_types`, `batch_builder`) — wasm/extism/rhai migrated; manifest
    JSON round-trip eliminated via `Loader::prepare_parsed`.
  - ✅ apoc-core procedure-signature drift closed — single source of truth
    per variant across `bitwise`/`text`/`math`/`number`/`convert`/`create`.
  - ✅ `uni-plugin-builtin` collapsed: 3 background-job structs →
    `BuiltinJob` enum; 6 procedure markers → `PeriodicProc` enum.
  - ✅ Phase 4a registry-`Surface` scaffolding landed at
    `crates/uni-plugin/src/surfaces/mod.rs` (633 LOC): 4 family traits,
    `SurfaceKind`/`RecordedKey`/`Discriminator`, 25 zero-sized markers.
    Compiles alongside the legacy `PendingRegistration` / `apply_one` /
    `preflight` matches; no call sites migrated yet.
  - ✅ Phase 4 foundation tasks landed (2026-05-28). All three
    prerequisites for the 25-surface migration now compile alongside
    the legacy code in `crates/uni-plugin/src/surfaces/mod.rs`:
    - **F1 — Dispatch methods.** Four family-ops traits
      (`NamedUniqueOps` / `VersionedOps` / `KeyedUniqueOps` /
      `AppendOps`) with `slot` / `record_slot` / `preflight` / `insert`
      / `remove` dispatch; 25 impls (one per marker); object-safe
      `DynPendingRegistration` wrapper + four per-family `*Reg<S>`
      payload structs for heterogeneous batch flows (`Loader::prepare`).
    - **F2 — `KeyedUniqueSurface::key_of`.** Default `None`; 7 of 8
      keyed-unique surfaces override with provider self-identification
      (`IndexKindProvider::kind`, `StorageBackend::scheme`,
      `CollationProvider::name`, etc.). `LabelStorageSurface` remains
      the documented outlier — `KeyedUniqueReg::key_override` carries
      the explicit label.
    - **F3 — Append-family removal.** 8 `ArcSwap<Vec<Arc<dyn P>>>`
      fields migrated to `ArcSwap<Vec<AppendEntry<dyn P>>>`. Public
      read accessors preserved via `project_append`. `remove_plugin`
      now filters append entries by owner — **closes the legacy
      "deferred to M5e" gap at `registry.rs:1206-1211` where hook /
      trigger / auth / connector entries were silently leaked across
      hot reloads.** User-approved choice: skip the shadow
      `DashMap<(SurfaceKind, PluginId), Vec<usize>>` (slot-index
      invariant is brittle) in favour of O(n≈100) filter-on-remove
      using the existing `AppendEntry<P>` owner tag.
    - 4 new regression tests in `surfaces::tests` exercise the
      end-to-end dispatch and the M5e gap closure. 2546 workspace
      tests pass across `uni-plugin` + dependents; zero new warnings.
  - ✅ Phase 4b-4f landed (2026-05-28). All 25 surfaces now flow through
    the family-ops trait dispatch:
    - **4b — Registrar.** `PluginRegistrar::pending` switched to
      `Vec<Box<dyn DynPendingRegistration>>`; the 25 `register_*` methods
      push the corresponding `NamedUniqueReg` / `VersionedReg` /
      `KeyedUniqueReg` / `AppendReg` payload directly.
    - **4c — Registry batch dispatch.** `apply_pending` reduced to a
      preflight loop + apply loop over `DynPendingRegistration`; the
      850-line `preflight` + `apply_one` matches in `registry.rs` are
      gone.
    - **4d — `remove_plugin`.** Now dispatches through
      `NamedUniqueOps::remove` / `VersionedOps::remove` /
      `KeyedUniqueOps::remove` / `AppendOps::remove_plugin` per family.
      The label-storage path now flows through the same trait too
      (previously inline in the old match).
    - **4e — `PendingRegistration` enum deleted.** Plus its hand-rolled
      `Debug` impl and the private `append_tagged` / `drop_owned_by`
      helpers (their work now lives on the `AppendOps` blanket impl).
    - **4f — Per-key tracking for the 4 count-only keyed-unique
      surfaces.** `PluginRecord::{type_count, collation_count, cdc_count,
      catalog_count}` replaced with `Vec<SmolStr>` fields
      (`logical_types`, `collations`, `cdc_outputs`, `catalogs`); same on
      `PluginRecordSnapshot`. **Closes the gap where hot reload leaked
      the old `Arc<Provider>` on these four surfaces** — `remove_plugin`
      now drops the slot entries by key.
    - New regression test `surfaces::tests::keyed_unique_collation_per_key_record_round_trip`
      asserts the round-trip on `CollationSurface`; the existing
      `surfaces::tests` continue to pass plus `uni-plugin` (111),
      `uni-db` (1445), `uni-query` / `uni-cypher` / `uni-locy` /
      `uni-store` / `uni-crdt` (1602), and the plugin loader crates
      (185) — 3343 tests verified green.
- **§1.3 N-way AST/value visitors** — ✅ DONE (2026-05-28).
  - `uni-cypher` `ast.rs`: `substitute_variable` and `is_aggregate` now
    delegate the structural-recursion arms to `for_each_child` /
    `map_children`. Only the 5 variants with non-mechanical behavior keep
    explicit arms (matching `Variable`, the 3 shadow-aware binders,
    and `MapProjection` whose `Variable` projection items need direct
    substitution). ~250 LOC → ~110 LOC; 4 sync points → 2.
  - `uni-locy` `typecheck.rs`: `for_each_fold_call` /
    `try_for_each_fold_call` helpers next to the removed
    `extract_function_name`; all 5 fold-iteration sites refactored.
  - `uni-crdt` `lib.rs` + `registry_dispatch.rs`: single
    `for_each_crdt_variant!` macro (the variant table) drives
    `try_merge`, `type_name`, and `kind` — three sync points → one.
    The §2 `TypeMismatch` formatting issue was already resolved upstream
    (`try_merge`'s error path uses `type_name`); `mem::discriminant` in
    `merge_via_registry` is a variant-equality check, not formatting.
  - `uni-common` `cypher_value_codec.rs`: `decode_msgpack` /
    `encode_msgpack` helpers; ~250 LOC of error-wrap boilerplate → ~80.
  - **Bonus sites — 2 of 4 landed:**
    - ✅ `uni-query` `pushdown.rs`: `collect_label_or_branches` /
      `collect_type_or_branches` collapsed into one generic
      `collect_or_branches` parameterized by a leaf-predicate fn
      (`label_leaf` / `type_eq_leaf`).
    - ✅ `uni-query` `df_planner.rs`:
      `extract_static_unwind_values` and `..._field_values` now share
      `walk_static_unwind_chain`; adding a recognized LogicalPlan node
      touches one place.
    - 🛑 `df_udfs.rs` `encode_sort_key_to_buf` + `sort_key_type_rank`:
      not actually a parallel enumeration — the encoder already calls
      `sort_key_type_rank` to get its rank byte. Exploration agent
      overcalled this; left as-is.
    - 🛑 `walker.rs` `rewrite_clause` / `rewrite_expr`: per-handler
      delegation already appropriate, as noted in the original plan.
  - Verification: 3241 tests pass across `uni-db` + `uni-common` +
    `uni-crdt` + `uni-cypher` + `uni-locy` + `uni-query` + `uni-store` +
    `uni-plugin`; clippy clean on the touched crates.
- **§1.4 TCK matcher families** — ✅ DONE (2026-05-28).
  - `uni-tck` `steps/then.rs`: 5 result-matcher `#[then]` handlers now
    delegate through one `assert_result_matches(world, step, matcher_fn,
    label)` helper. Each handler shrinks from ~18 LOC to ~3.
  - `uni-locy-tck` `matcher/result.rs`: four `match_result_*` public
    entries are 2-line wrappers over a single
    `match_result_inner(actual, expected, ordered, ignore_list_order)`.
    The dispatch picks `values_equal` / `values_equal_ignoring_list_order`
    via fn-pointer; `compare_row` is gone (inlined into the unified
    loop). ~190 LOC → ~120 LOC.
  - `uni-locy-tck` `steps/then_evaluate.rs`:
    - `LocyWorld::expect_locy_ok` and `LocyWorld::expect_command_result(idx)`
      accessors land in `world.rs`, collapsing the 4-line
      `locy_result().expect().as_ref().expect()` + `command_results.get(idx).unwrap_or_else(...)`
      preamble that 12 `command_result_*` handlers all repeated.
    - `assert_warning_present` / `assert_warning_absent` helpers replace
      the 6 warning-pair `#[then]` bodies (SharedProbabilisticDependency,
      BddLimitExceeded, CrossGroupCorrelationNotExact).
    - BddLimitExceeded-metadata + approximate-fact handlers now call
      `expect_locy_ok()` too.
  - `uni-locy-tck` `steps/when_evaluate.rs`: 6 `when_evaluating_with_*`
    handlers reduced to ~4 lines each via a `run_with_config(world, step,
    |c: &mut LocyConfig| ...)` helper that owns the init-db /
    docstring / build-config / run / apply-derived sequence.
  - **§2 bug entry "uni-tck steps/then.rs:75" refuted on inspection**:
    line 89 (the `result_should_be_in_order_ignoring_list_order` body)
    dispatches `match_result_ignoring_list_order` which is row-ordered
    and list-relaxed — matching its Gherkin regex `"in order (ignoring
    element order for lists)"`. The companion
    `result_should_be_in_any_order_ignoring_list_order` at line 109
    dispatches `match_result_unordered_ignoring_list_order`. They are
    *different* matchers; no wrong-dispatch bug.
  - Verification: `cargo nextest run -p uni-tck -p uni-locy-tck` →
    4409 tests passed, 1 pre-existing skip. Clippy clean on both
    crates (the one warning on `uni/src/scheduler.rs:462` is
    pre-existing and unrelated).
- **§1.5 Atomic sidecar/manifest IO** — 🟡 PARTIAL. Manifest JSON
  round-trip eliminated in §1.1 (wasm + extism). `SystemSidecar<T>`
  consolidation of `uni::{DeferralSidecar, CdcCheckpointSidecar,
  SystemLabelSchedulerPersistence}` not started.

---

## 1. Cross-cutting themes

These patterns recur in five or more crates and would benefit from coordinated cleanup rather than per-crate fixes.

### 1.1 Plugin-framework registration boilerplate — ✅ RESOLVED
See the status section above for the per-sub-phase breakdown. Outcome:

- ~~`uni-plugin`: ~400 LOC of `*Entry` structs with hand-rolled `Debug` impls; 7 near-identical wrappers (registry.rs).~~ → `registry.rs` lost the 850-line `PendingRegistration` / `apply_one` / `preflight` matches; dispatch is per-family via the `*Ops` traits in `crates/uni-plugin/src/surfaces/mod.rs`.
- ~~`uni-plugin-wasm`, `uni-plugin-extism`, `uni-plugin-pyo3`, `uni-plugin-rhai`: each redefines `argtype_arrow`, `arrow_name_to_dt`, error classifiers, and a single-row `RecordBatch` builder.~~ → centralized in `uni-plugin::adapter_common::{arrow_types, batch_builder}`; loaders migrated.
- ~~`uni-plugin-apoc-core`: `ProcedureSignature` constructed twice per variant…~~ → single source of truth per procedure variant.
- ~~`uni-plugin-builtin`: 3 near-identical job structs and 6 `ProcedurePlugin` structs…~~ → collapsed to `BuiltinJob` / `PeriodicProc` enums.

The shared trait pieces — `DynPendingRegistration`, `NamedUniqueReg` / `VersionedReg` / `KeyedUniqueReg` / `AppendReg`, and the four `*Ops` traits — are the realized "shared `plugin_adapter_common`" from the original recommendation. Adding a new surface now means: one marker, one `*Ops` impl, one `register_*` method. No more four-place edits.

### 1.2 Async / principal-threading inconsistency
FU-1 (writer threading) and M11 #6 (principal task-local) were added recently and replicate the same boilerplate at many sites:

- `uni-query`: 25-line "build host + attach writer + read principal + invoke" block duplicated between `executor/procedure.rs:659` and `df_graph/procedure_call.rs:655`. Six near-duplicate doc paragraphs **already drifting** ("outer executor's writer" vs "outer transaction's writer").
- `uni` (`api/session.rs`, `api/transaction.rs`): 5+ sites of `match principal { Some(p) => scoped_with_principal(...).await, None => fut.await }`.
- `uni-plugin`: `CancellationToken` re-invents `tokio_util`'s; sync `BackgroundJobProvider::execute` and `TriggerPlugin::fire` are expected to be driven from Tokio without a documented policy.

**Recommended:** extract `build_procedure_context` and `maybe_scope_with_principal` helpers; pick a single async policy for `uni-plugin` traits and document it.

### 1.3 N-way duplicated AST/value visitors — ✅ RESOLVED
See the status section above for the per-site outcome. Outcome:

- ~~`uni-cypher` `ast.rs`: four parallel visitors enumerate 27 `Expr` variants each.~~ → `substitute_variable` / `is_aggregate` delegate structural recursion to `for_each_child` / `map_children`; only 5 non-mechanical variants keep explicit arms.
- ~~`uni-locy` `typecheck.rs`: five fold-iteration loops duplicate `extract_function_name + to_uppercase + literal-check`.~~ → `for_each_fold_call` / `try_for_each_fold_call` helpers replace all five loops.
- ~~`uni-crdt`: three 8-variant matches.~~ → single `for_each_crdt_variant!` macro is the variant table; `try_merge` / `type_name` / `kind` expand from it.
- ~~`uni-common` `cypher_value_codec.rs`: ~38 arms repeating `rmp_serde` + `UniError::Storage` wrap.~~ → `decode_msgpack` / `encode_msgpack` helpers; each arm shrinks from 5–6 lines to one.

**Bonus sites landed in the same PR:**
- ✅ `uni-query` `pushdown.rs`: `collect_or_branches` parameterized by leaf-predicate fn replaces `collect_label_or_branches` / `collect_type_or_branches`.
- ✅ `uni-query` `df_planner.rs`: `walk_static_unwind_chain` shared by both `extract_static_unwind_*` callers.

**Bonus sites refuted on inspection:**
- 🛑 `df_udfs.rs` `encode_sort_key_to_buf` already calls `sort_key_type_rank` for its rank byte — not a parallel enumeration; exploration overcalled.
- 🛑 `walker.rs` `rewrite_clause` / `rewrite_expr` — per-handler split is appropriate; noted as skip in the original plan.

### 1.4 TCK / test-harness duplication
- `uni-locy-tck` and `uni-tck` both define families of `match_result_*` and `*_ignoring_list_order` matchers that differ only by an ordering flag. `uni-tck`'s `then.rs:75` has a **likely bug**: the "in order ignoring list order" arm dispatches to the same matcher as the order-insensitive variant.
- `uni-locy-tck`: six warning-code assertion pairs + 10 `command_result_*` + 6 `when_evaluating_with_*` variants follow identical templates (~800 LOC reduction available).

### 1.5 Atomic sidecar/manifest IO repeated
- `uni`: three near-identical "atomic JSON sidecar" implementations (`DeferralSidecar`, `CdcCheckpointSidecar`, `SystemLabelSchedulerPersistence`) reimplement write-tmp-rename. One `SystemSidecar<T>` would consolidate.
- `uni-plugin-wasm` and `uni-plugin-extism`: both round-trip manifests through JSON during pass-2 of `load` wastefully.

---

## 2. Correctness findings (not just simplifications)

The simplifier flagged real likely-bugs in passing. These deserve issues regardless of any refactor.

| Crate | File:Line | Issue |
|---|---|---|
| `uni-plugin-wasm-rt` | `src/ipc.rs:128` | `decode_batch` doc says "first batch" but `pop()` returns **last**; also allocates full `Vec` before discard. |
| `uni-plugin-wasm-rt` | `src/pool.rs` | Check-then-increment race in `InstancePool::acquire` lets `live` briefly exceed `max_instances`. |
| `uni-plugin-wasm-rt` | `src/pool.rs:37` | `PoolConfig::idle_ttl_secs` plumbed + tested but never consulted (no reaper). |
| `uni-common` | `config.rs:349-407` | `FileSandboxConfig::validate_path` silently falls back to non-canonical path on `canonicalize` failure — weakens documented CWE-22 mitigation. |
| `uni-cypher` | `walker.rs:1519` | Parenthesized-pattern WHERE clause `eprintln!`'d and **silently dropped**. |
| `uni-cypher` | `walker.rs:2098, 2109, 2237` | `IF EXISTS` / `IF NOT EXISTS` flags parsed but dropped for DROP / CREATE INDEX / CREATE CONSTRAINT. |
| `uni-cypher` | `walker.rs:2174` | CHECK-constraint properties are an empty `vec` with a TODO. |
| `uni-cypher` | `walker.rs:407-412` | `SET n:A\|B` silently flattens **disjunction → conjunction**. |
| `uni-tck` | `steps/then.rs:75` | ~~"in order ignoring list order" arm dispatches to the same matcher as the order-insensitive variant.~~ **Refuted on inspection during §1.4** — line 89 calls `match_result_ignoring_list_order` (row-ordered, list-relaxed) and line 109 calls `match_result_unordered_ignoring_list_order`. These are different matchers; each matches its Gherkin regex correctly. |
| `uni-algo` | (multiple `*_vid` helpers) | Three divergent VID parsers, several silently default invalid input to **vertex 0**. |
| `uni-algo` | `DirectTraversal::shortest_path_with_hops` | Confused control flow drops paths failing `min_hops`. |
| `uni-plugin-rhai` | `MutableFloat64Column::set` | O(n²) **and** latent null-loss when filling sparsely. |
| `uni-plugin-rhai` | aggregate accumulator init | Errors swallowed at construction. |
| `uni-locy` | `LocyResult::columns()` | Nondeterministic column order via `HashMap::keys()`. |
| `uni-locy` | `stratify.rs` | Hand-rolled recursive Tarjan SCC — stack-overflow risk on deep graphs. |
| `uni-cli` | `repl.rs` | Off-by-one in "N rows" footer (`table.len()` includes header); fragile byte-slice `query[7..]` on PROFILE prefix. EXPLAIN/PROFILE asymmetric. |
| `uni-btic` | `parse.rs::strip_timezone` | Parses an offset that is then discarded — dead helper or latent correctness gap. |
| `uni-plugin-builtin` | error codes | `0xB30`-`0xB36` collide; `0xB33` reused at lines 217 and 272. |
| `uni-plugin` | `Schedule::Manual` | Semantically means "immediate one-shot," not manual; malformed cron silently disables a job; `SchedulerJobRecord::PartialEq` ignores most fields. |
| `uni-crdt` | `Crdt::try_merge` | `TypeMismatch` error formats with opaque `mem::discriminant`; sibling path uses `type_name`. |

---

## 3. Significant dead code

After re-classification, items split into three buckets:

**✅ Deleted in the safe-delete cleanup PR** (truly DEAD + 1 VESTIGIAL):

- ✅ `uni-plugin-pyo3` `lib.rs:104-255`: vestigial M8.0 scaffold deleted.
- ✅ `uni-plugin-conformance`: `manifest.version.to_string().is_empty()`
  unreachable branch deleted. (Other items reclassified as STUB.)
- ✅ `uni-algo`: `AlgorithmConfig` struct + re-export deleted (limits were
  never enforced).
- ✅ `uni-plugin-apoc-core`: `_force_int64array` + unused `Int64Array` import
  deleted.
- ✅ `uni` `scheduler.rs:267-268`: dead `mem::take` deleted.
- ✅ `uni-btic`: `BticError::SentinelExclusivity` deleted.

**📝 Reclassified as STUBs and tracked in `docs/KNOWN_GAPS.md`** (since removed
once every gap closed — see git history; immediate deletion would have hidden
unfinished features):

- `uni-store` `load_properties_columnar` — Phase 2 zero-copy perf path never
  finished.
- `uni-plugin-conformance` WASM `run_against` — requires M6a/M6b SDK
  integration.
- `uni-plugin-extism` duplicate `read_register_export` at `loader.rs:346-350`
  — aborted refactor remnant.
- `uni-plugin-custom` `chain_starting_at` — cycle path reconstruction never
  written; `mod _silence` marks pending list-return support.
- `uni-algo` `dinic flowEdges: 0` — count never implemented.
- `uni-plugin-builtin` `PeriodicCommit::_scheduler` — wait on durable
  scheduler persistence.
- `uni` `api/mod.rs:3276 _manifest` — placeholder for future signature
  verification.

**🛑 Kept (DEFENSIVE / VESTIGIAL-with-migration / not yet decided)**:

- `uni-plugin-conformance` `capabilities.declared` probe — real invariant
  check, not a no-op once you read it carefully.
- `uni-plugin-rhai` `RhaiError::{CapabilityDenied,RuntimeError,ResourceLimit,
  NotYetImplemented}` — public error enum surface.
- `uni-plugin-extism` `ExtismError::NotYetImplemented` + "Scaffolding only"
  doc — superseded by `Instantiate`; doc is stale (§4).
- `uni-plugin-custom` `size()` ignores heap — required `Accumulator` trait
  impl; the "ignore heap" concern is a perf nit, not dead code.
- `uni-algo` `Dijkstra` (library) + `include_reverse()` shim — real migration
  work, not a delete; needs ~7 procs moved to `customize_projection`.
- `uni-cypher` `LabelExpr` 3 dead operator combos — appears already cleaned
  in current source (only `Empty`/`Conjunction`/`Disjunction` remain).
- `uni-btic` `Btic::new_unchecked` — `pub(crate)` escape hatch, intentional.
- `uni-cli` `demo.rs` 4-line shim — trivial public re-export.
- `uni-crdt` `op_from_bytes`, `MergeResult` — exercised by tests, not
  production-dead.

---

## 4. Workspace hygiene

**✅ Addressed in commit `19b2281` ("chore(hygiene): close §4 of
CODE_SIMPLIFIER_FEEDBACK.md"):**

- ✅ **`Cargo.toml` discipline:** ~~`uni-plugin-builtin` pins `uuid`,
  `semver`, `blake3`, `cron` directly while every sibling uses
  `workspace = true`.~~ All four migrated to `[workspace.dependencies]`;
  workspace `uuid` widened to `features = ["v4", "serde"]` so the
  builtin crate's needs are met centrally.
- ✅ **Stale crate docs:** ~~`uni-plugin-extism` `lib.rs` still says
  "Scaffolding only"; `uni-plugin-builtin` `lib.rs` and
  `uni-plugin-conformance` `lib.rs` have similar stale "M12 scaffolding"
  text contradicting now-implemented suites.~~ `uni-plugin-extism` and
  `uni-plugin-conformance` doc blocks rewritten to reflect current
  state. (`uni-plugin-builtin` `lib.rs` was already accurate — the
  doc's claim was wrong.)
- ✅ **Reserved IDs:** ~~`uni-plugin-conformance` hardcodes a
  reserved-id list at line 174 that should live in `uni-plugin`.~~
  Centralized in `uni_plugin::qname` as `RESERVED_PLUGIN_IDS` const +
  `is_reserved_plugin_id()` helper; conformance probe now calls the
  shared helper.
- ✅ **Bonus** (found during investigation, not in original list):
  `STORAGE_FILTER_UNENCODABLE = 0x711` was declared verbatim in both
  `uni-plugin/src/adapters/catalog_from_storage.rs:82` and
  `uni-plugin-builtin/src/storage_table_provider.rs:67`. Promoted the
  `uni-plugin` const to `pub`; deleted the duplicate; builtin imports
  the canonical symbol.

**🛑 Refuted on inspection (not real issues):**

- ~~**Magic constants:** `uni-plugin-apoc-core` error codes
  `0x700`-`0x800` and OOM caps duplicated across files without shared
  `const`s.~~ Each procedure file uses a *unique* code
  (`bitwise=0x700`, `text=0x701`, `math=0x702`, `number=0x703`,
  `convert=0x704`, `create=0x705`); they are a per-procedure error-code
  register, not a duplication. Centralizing them in a registry is a
  reasonable refactor for discoverability, but it belongs in the §1.1
  plugin-fw consolidation pass — not a pure-hygiene item.
- The `0xB33` collision in `uni-plugin-builtin/src/procedures/periodic.rs`
  (§2 correctness table) was already resolved upstream by renumbering
  to `0xB37`, with an in-source comment at `periodic.rs:271-274`. No
  hygiene work needed.

---

## 5. Per-crate summary index

| Crate | Lines | Headline finding |
|---|---:|---|
| `uni` | 180 | 3× sidecar boilerplate; 4× catch_unwind dispatch in `triggers.rs`; principal-threading repetition. |
| `uni-algo` | 371 | Massive procedure-adapter mechanical duplication (500–700 LOC reducible); VID-parse silent fallback to 0. |
| `uni-btic` | 165 | Mostly polish; `chrono::Months` available but `parse.rs` reimplements arithmetic; `strip_timezone` discards data. |
| `uni-cli` | 116 | `execute_query` 3× near-identical blocks; off-by-one row count; PROFILE prefix slicing. |
| `uni-common` | 195 | `Vid`/`Eid` duplicate ~95 LOC; codec arms ×24; `FileSandboxConfig::validate_path` CWE-22 weakening. |
| `uni-crdt` | 141 | 3× 8-variant matches; error-format inconsistency. |
| `uni-cypher` | 262 | Multiple **silent correctness drops** (WHERE, IF EXISTS, SET disjunction); 4× parallel `Expr` visitors. |
| `uni-locy` | 199 | 4× `compile_*` entry points; nondeterministic columns; recursive Tarjan SCC. |
| `uni-locy-tck` | 339 | ~~~800 LOC reducible across `then_*` step handlers + matcher families.~~ ✅ §1.4 — matcher unified, warning/command-result helpers landed, `run_with_config` collapses 6 `when_evaluating_with_*` handlers. |
| `uni-plugin` | 254 | ~~Core plumbing duplication~~ (§1.1); ~~async inconsistency~~ (§1.2); scheduler O(n) lookups + lifecycle bugs remain. |
| `uni-plugin-apoc-core` | 124 | `ProcedureSignature` doubly-defined with **already-diverged** docstrings. |
| `uni-plugin-builtin` | 186 | 3 jobs + 6 procs collapse to enum-driven dispatch (~270 LOC); colliding error codes. |
| `uni-plugin-conformance` | 116 | Repeated registration probes; dead `capabilities.declared`; WASM branch stub. |
| `uni-plugin-custom` | 107 | Cross-module helper duplication finally has a "third caller"; 2003-line `lib.rs` needs split. |
| `uni-plugin-extism` | 129 | Adapter triplication; `ExtismLoader::load` 150 lines + manifest round-trip. |
| `uni-plugin-pyo3` | 171 | **Vestigial M8.0 scaffold** in `lib.rs:104-255` shadows real loader. |
| `uni-plugin-rhai` | 344 | `MutableFloat64Column::set` O(n²) + null-loss bug; double engine-build + AST recompile in loader. |
| `uni-plugin-wasm` | 140 | 3× `build_*_pool` factories; orphaned `select_linker_for_manifest`; 3 `*PluginInstance` types collapse to one generic. |
| `uni-plugin-wasm-rt` | 186 | **`decode_batch` likely off-by-one bug**; idle TTL plumbed but unused; pool race. |
| `uni-query` | 256 | 25-line procedure-context boilerplate duplicated; doc paragraphs already drifting. |
| `uni-store` | 206 | Unimplemented stub deletable; 3× `scan_*_table` skeleton; `get_neighbors` + `_at_version` merge candidate. |
| `uni-tck` | 296 | ~~**Likely bug** at `steps/then.rs:75`;~~ refuted on inspection. ✅ §1.4 — `assert_result_matches` helper collapses 5 matcher handlers. |

---

## 6. Suggested next steps

1. **File issues** for everything in §2 (correctness) before touching refactors — they're independent and quick to surface.
2. **Delete dead code in §3** as one focused PR — almost zero risk, gives clearer diffs for the bigger refactors.
3. **Coordinate the plugin-framework consolidation (§1.1, §1.2)** as a single design pass before adding any further loaders or surfaces — every new surface multiplies the existing duplication.
4. ~~**Leave §1.3 (AST visitors) and §1.4 (TCK harness) for later**~~ — both landed (§1.3 on 2026-05-28; §1.4 on 2026-05-28). Remaining cross-cutting work is §1.5 (`SystemSidecar<T>` consolidation in `uni`).
