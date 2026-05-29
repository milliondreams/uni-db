# Known Gaps

Tracked stubs, placeholders, and unfinished features surfaced during the
2026-05-27 workspace-wide code-simplifier review (see
`CODE_SIMPLIFIER_FEEDBACK.md` §3).

These items look like dead code at a glance — no callers, `_`-prefixed bindings,
`#[allow(dead_code)]`, or trivial bodies — but each one represents real
unfinished functionality. They are listed here so a future dead-code cleanup
sweep does not silently erase the gap. The fix is to **implement** (or
explicitly decide to remove the feature), not to delete the placeholder.

Format: one row per gap, with the source anchor and the gap's nature.

---

## Notes

- This document is not a roadmap; it's a "do not silently delete" list.
- When a gap is closed, remove its entry here in the same commit that
  implements it.
- New stubs introduced after this review should land with a corresponding
  entry, not as an `_`-prefixed binding alone.
- Items judged truly dead (e.g., `BticError::SentinelExclusivity`,
  `AlgorithmConfig` limit fields, `mem::take` no-op at `scheduler.rs:267`,
  the unreachable `version.is_empty()` branch in conformance) are *not*
  listed here — those are safe to delete in a routine cleanup PR.

---

## §1.1 Phase 4 — Registry `Surface` migration (4b-4f)

Added 2026-05-28. Phase 4a landed the trait scaffolding at
`crates/uni-plugin/src/surfaces/mod.rs` (633 LOC: `SurfaceKind`,
`RecordedKey`, `Discriminator`, 4 family traits, 25 zero-sized markers).
It compiles alongside the legacy `PendingRegistration` enum + the giant
`apply_one` / `preflight` / `remove_plugin` matches in
`crates/uni-plugin/src/registry.rs` and `registrar.rs`.

**The new abstraction is currently dead code** — no call site dispatches
through it. Migrating the 25 surfaces (sub-phases 4b-4f) requires three
foundation tasks first:

1. **Add dispatch methods.** The family traits today carry only
   metadata (`KIND`, `Sig`, `Provider`, `Storage` associated types).
   They need either per-surface `insert` / `remove` methods on each
   marker's `Surface` impl, OR a `DynPendingRegistration` trait the
   registrar can box and dispatch through. The 4f sketch in
   `CODE_SIMPLIFIER_FEEDBACK.md` favors the latter.
2. **Add `KeyedUniqueSurface::key_of(provider) -> Self::Key`.**
   Surfaces like `Collation`, `Cdc`, `Catalog`, `LogicalType` derive
   their key from the provider via `provider.name()`. Without this
   hook, the dispatch path must keep passing the key explicitly from
   the registrar (which defeats some of the consolidation).
3. **Append-family per-plugin index tracking.** The user-approved
   design is a parallel `DashMap<(SurfaceKind, PluginId), Vec<usize>>`
   tracking which indices in each
   `ArcSwap<Vec<Arc<dyn P>>>` belong to which plugin, so
   `remove_plugin` can rebuild without them. Read accessors
   (`hooks()`, `auth_providers()`, etc.) keep their existing
   signatures — the shadow map is internal. Care needed: index updates
   must stay consistent with the `ArcSwap` content under concurrent
   registration (likely via a per-surface `Mutex` around the
   copy-on-write transaction).

**`PluginRecordSnapshot` is `pub`** with struct-literal users at
`crates/uni-plugin/tests/reload_dispatch.rs:157` and
`crates/uni/tests/reload_index_kind.rs:66`. The 4f collapse keeps the
25 `pub` fields and populates them from the new internal
`Vec<(SurfaceKind, RecordedKey)>` to preserve external compatibility.

**Procedure family.** Arity-overload semantics
(`procedure_with_arity`, `procedure_overloads`, `Discriminator::Arity`)
must survive verbatim. Procedure gets a bespoke `VersionedSurface`
impl, not force-fitted into `NamedUniqueSurface`. Regression test
required: register two arities under one qname, unregister the
plugin, assert the other survives.

**Pre-existing inconsistency to clean up in 4c**:
`PendingRegistration::StorageBackend(&'static str, ...)` holds a
`&'static str` scheme while all other keyed-unique surfaces use
`SmolStr`. Pick one (probably `SmolStr`) when migrating.

**Pool-factory unification for the Extism loader** was deferred from
Phase 1 — the Extism `build_pool` exists but is structurally
different (no `Store`/`Component`/`Linker`; uses
`extism::PluginBuilder` with host-fn filtering). Unifying with
wasm's generic `build_pool<I, F>` would force cross-crate trait
abstraction over wasmtime and extism types. Documented in
`crates/uni-plugin/src/adapter_common/mod.rs`. Revisit only if a
third loader appears.

## §1.5 sidecar IO

`SystemSidecar<T>` consolidation of `uni::DeferralSidecar`,
`uni::CdcCheckpointSidecar`, `uni::SystemLabelSchedulerPersistence` —
not started; small focused cleanup.
