# uni-plugin Code Review

Scope: `crates/uni-plugin/` (registry, scheduler, traits/background, traits/trigger, registrar, reload, supporting modules).

## 1. Massive duplication: `*Entry` newtypes around `(PluginId, Signature, Arc<dyn …>)`

**File:** `src/registry.rs:40-148`

Seven near-identical entry structs (`ScalarEntry`, `AggregateEntry`, `WindowEntry`, `ProcedureEntry`, `LocyAggregateEntry`, `LocyPredicateEntry`, plus `IndexHandleEntry`/`VirtualEntry`). Each has identical shape (plugin id + signature + Arc) and an identical hand-rolled `Debug` impl. The `Debug` impls especially are pure boilerplate.

**Suggestion:** Introduce a generic `RegistryEntry<S, T: ?Sized> { plugin: PluginId, signature: S, value: Arc<T> }` with a single `Debug` impl that requires `S: Debug`. For `LocyAggregateEntry` (which has no signature) use `()` for `S`. Adds clarity and eliminates 7 boilerplate Debug impls (~60 lines).

**Effort:** moderate (touches all consumers; many are public).

## 2. `PluginRecord` and `PluginRecordSnapshot` are duplicate types

**File:** `src/registry.rs:246-337, 1122-1152`

`PluginRecord` (private) and `PluginRecordSnapshot` (public) have identical fields. The `iter_for_plugin` function field-by-field clones every member into the snapshot — 27 lines of mechanical copies. Maintenance hazard: adding a new surface requires editing both structs, `Default` derivation, `apply_one`, `remove_plugin`, and `iter_for_plugin`.

**Suggestion:** Make `PluginRecord` itself public (or alias the snapshot to `PluginRecord`) and derive `Clone`. `iter_for_plugin` collapses to `self.per_plugin.read().get(plugin).cloned()`.

**Effort:** trivial.

## 3. `PluginRegistry` field/registration count explosion — driven by `PendingRegistration` enum

**Files:** `src/registry.rs:344-396`, `src/registrar.rs:59-112`, `apply_one` (961-1103), `preflight` (852-949), `remove_plugin` (1157-1212)

The cross-cutting concern of "register a thing" is repeated in 4 places per surface (enum variant, preflight branch, apply_one branch, remove_plugin branch, plus per-plugin record field). Adding a surface is a ~5-touch change. Append-mode kinds (`ArcSwap<Vec<…>>`) are not actually removed in `remove_plugin` — there is a comment acknowledging the gap, which is dead/broken behavior pending M5e.

**Suggestion:** Introduce a small trait `RegisteredSurface { fn preflight(&self, registry) -> Result<…>; fn apply(self, registry, record); fn remove(record, registry); }` implemented per variant. Reduces 250+ lines of giant match arms to one branch per surface co-located with its logic. Also — surface the "append-mode kinds not removed" gap as a tracked TODO with an explicit panic in tests, instead of a silent comment.

**Effort:** significant (architectural), but pays off given how many surfaces exist.

## 4. Repetitive lookup/snapshot helpers in `PluginRegistry`

**File:** `src/registry.rs:421-823`

`scalar_fn`, `aggregate`, `window`, `locy_aggregate`, `locy_predicate`, `storage_backend`, `lookup_label_storage`, `index_kind`, `algorithm`, `crdt_kind`, `logical_type`, `cdc_outputs_snapshot` are all variants of `dashmap.get(&key).map(|e| Arc::clone(e.value()))`. Similarly the `iter_scalars`/`iter_procedures`/`iter_locy_aggregates`/`iter_algorithms`/`iter_index_kinds`/`catalogs` variants all do the same `.iter().map(...).collect()`.

**Suggestion:** Provide two private helpers `fn lookup<K, V: Clone>(map: &DashMap<K,V>, k: &K) -> Option<V>` and `fn snapshot_all<K: Clone, V: Clone>(map: &DashMap<K,V>) -> Vec<(K,V)>`, then keep the public methods as one-liners. Also expose a single `arcswap_snapshot::<T>(slot) -> Arc<Vec<T>>` mirroring `arcswap_push`.

**Effort:** trivial.

## 5. Two near-identical virtual-id allocators

**File:** `src/registry.rs:208-242, 649-741`

`VirtualLabelInner` (u16) and `VirtualEdgeTypeInner` (u32) plus the corresponding `register_virtual_*`, `virtual_*_by_name`, `virtual_*_by_id` methods are textually-duplicated apart from the integer type and sentinel constant. The exhaustion-check branches use different operators (`==` vs `>=`) for no apparent reason — small bug surface.

**Suggestion:** Generic `VirtualIdAllocator<T: Copy + Ord + SaturatingAdd>` parameterised on the ID type, with sentinel/start passed at construction. Removes ~120 LOC and harmonises the comparison operator.

**Effort:** moderate.

## 6. Repetitive `Debug` boilerplate via finish_non_exhaustive

**Files:** `src/registry.rs:49-56,68-75,87-94,106-113,123-128,141-148,173-178,196-201`, `src/registrar.rs:114-145`, `src/reload.rs:61-68,80-86,99-104,118-124,258-264`

Twelve hand-written `Debug` impls all of the form "show this one field, hide the trait object". This pattern is correct but is exactly the kind of mechanical noise a macro removes. The `PendingRegistration` 26-arm match also belongs to this category — adding a variant means editing two places.

**Suggestion:** A tiny `debug_trait_field!` macro, or a `#[derive(Debug)]` with `#[debug(skip)]` via the `derivative` crate (already in workspace? confirm) on the Arc fields. For the 26-arm match in `PendingRegistration::fmt`, derive `strum::Display` against variant names instead.

**Effort:** trivial.

## 7. `Scheduler` linear scans through `records: Mutex<Vec<…>>`

**File:** `src/scheduler.rs:101-337`

Every operation (`cancel`, `mark_started`, `mark_finished`, `tick_at`, `running_count`, `pending_count`, `requeue_orphaned_runs`) does `records.lock().iter[_mut]().find(|r| &r.id == id)` — O(n) per lookup. For a registry of a few jobs this is fine, but the API is set up so future job counts are unbounded. Also: `Mutex<Vec<…>>` precludes per-job concurrent updates.

**Suggestion:** Swap `Vec<SchedulerJobRecord>` for `DashMap<QName, SchedulerJobRecord>` (or `HashMap` inside the same `Mutex` if write ordering matters). Lookups become O(1), and `list()` collapses to `.iter().map(...).collect()`. Note also that `add_scheduled_job` does not check for duplicate ids — easy correctness fix in the same pass.

**Effort:** moderate.

## 8. `SchedulerJobStatus` lifecycle conditionals are awkward

**File:** `src/scheduler.rs:300-337`

`mark_finished` computes `has_next`, then has a top-level if/else, and two cascading `if success` branches setting `consecutive_failures` and `next_fire_at`. Read in one pass it's tricky to verify all transitions. The `Schedule::Manual` case is hidden — it goes through `next_after` returning `Some(from)` then through the "not Periodic/Cron" branch — but a reader has to chase that across two files.

**Suggestion:** Pull the next-state decision into a small helper `fn compute_next_state(schedule, success, now) -> (SchedulerJobStatus, Option<SystemTime>)` with explicit match arms on `Schedule`. The body of `mark_finished` then reduces to record mutation, which is the actual side effect.

**Effort:** trivial.

## 9. `Schedule::next_after` for `Manual` returns `Some(from)` — semantic surprise

**File:** `src/traits/background.rs:96-109`

`Manual` jobs are described as "fired only via explicit run", yet `next_after` returns `Some(from)`, which causes the scheduler's `tick_at` to dispatch them immediately on every tick after registration. The `tick_at` doc-comment papers over this with "Manual jobs have `next_fire_at = now` at registration and so are immediately due". This makes "manual" a misnomer for "immediate one-shot".

**Suggestion:** Either rename `Manual` → `Immediate`, or have `next_after(Manual)` return `None` and let `add_scheduled_job` separately stamp `next_fire_at = Some(now)`. Doc the chosen semantics in one place.

**Effort:** trivial (rename), moderate (semantic change with test updates).

## 10. Default-method `on_deferred` couples `TriggerPlugin` to a synchronous re-fire

**File:** `src/traits/trigger.rs:45-52`

The default `on_deferred` impl invokes `self.fire(ctx, events)`, ignoring the `payload` entirely. This is convenient but breaks the deferral contract for plugins that need it: they must override every time, and the existence of a default makes it easy for a plugin to silently drop deferral payload data. The doc even calls this out, which is a smell.

**Suggestion:** Remove the default and require explicit implementation. If backward-compat is critical, mark the default `#[deprecated(note="…")]` and require a feature flag.

**Effort:** moderate (requires audit of trigger plugins downstream).

## 11. `JobHost`/`SchedulerControl` default-error methods hide capability gaps

**Files:** `src/traits/background.rs:184-200`, `src/scheduler.rs:498-507`

`execute_write_cypher` and `submit_cypher` both default to returning `FnError::new(0xD10/0xD20, "not supported by this host")`. This is the same anti-pattern as `on_deferred`: it makes it impossible to tell at compile-time whether a particular host wires the capability. The error codes are magic-numbered.

**Suggestion:** Split into two traits (`JobHost` + `JobHostWriteCypher`) or have the methods return `Option<…>` / a marker query (`fn supports_write_cypher(&self) -> bool`). At minimum, define the error codes as `pub const` named items, not bare hex literals.

**Effort:** moderate.

## 12. `JobContext::_marker: PhantomData<&'a ()>` is leaking implementation

**File:** `src/traits/background.rs:215-237`

`pub _marker: …` exposes a `PhantomData` field in a `pub` struct (mirrored in `ProcedureContext` at `src/traits/procedure.rs:148`). Combined with `#[non_exhaustive]` this is awkward — outsiders cannot construct via struct literal anyway, so the field can be private.

**Suggestion:** Make `_marker` private (`pub(crate)` or no `pub`). Same fix in `ProcedureContext`.

**Effort:** trivial.

## 13. `CancellationToken` re-invents a wheel

**File:** `src/traits/background.rs:259-287`

A `Clone + Default` AtomicBool wrapper named `CancellationToken`. `tokio_util::sync::CancellationToken` already exists, is widely used in the workspace (visible from imports elsewhere), and additionally provides `cancelled().await` futures — useful for cooperative cancel inside async background jobs. The current type only supports polling.

**Suggestion:** Either adopt `tokio_util::sync::CancellationToken` (preferred — gives futures-aware cancellation) or document explicitly why this minimal poll-only type is intentional. Currently the plugin-fw is non-async at trait level but the M11 host loop is Tokio-backed, so the choice matters.

**Effort:** moderate.

## 14. Mixed sync/async story across the trait surface

**Files:** `src/traits/background.rs:18-29` (`execute` sync), `src/traits/trigger.rs:11-53` (`fire` sync returning result), `src/traits/procedure.rs:40-44` (`invoke` returns `SendableRecordBatchStream`), `src/scheduler.rs:200-227` (sync `tick_at`)

`BackgroundJobProvider::execute` is sync but is described as "the eventual Tokio driver wraps a poll loop that calls `tokio::spawn` invoking each job's execute". Triggers are sync too. Procedures already return an async stream. The whole surface is non-async but expected to be invoked from Tokio executors — the burden of "yield cooperatively" is on the plugin author, with no compile-time guarantee.

**Suggestion:** Decide and document the policy in one place (`lib.rs` rustdoc): "all plugin trait methods are synchronous; the host wraps each invocation in `spawn_blocking` (or equivalent)". Or, if async is on the table, introduce `async-trait` for the long-running surfaces (background, procedure, cdc) consistently.

**Effort:** significant (policy decision; impacts entire crate).

## 15. `ReloadDispatcher::dispatch` failure paths drop resources without rollback

**File:** `src/reload.rs:213-237`

Each iteration of the index/cdc handoff loop fails-fast with `?`, but earlier successfully-rebuilt handles already pushed into `outcome.index_handles` / `outcome.cdc_streams` are dropped when the error propagates. This silently loses the rebuilt resources. The doc comment "the host must be prepared to surface the failure and continue serving against the new providers' freshly-initialized resources" reads as rationalisation rather than a designed contract.

**Suggestion:** Either return the partial outcome alongside the error (`Err((ReloadError, ReloadOutcome))`), or document explicitly that partial success is undefined and recommend `dispatch` be called only with single-handoff batches. Either way, surface the design choice in the public API.

**Effort:** moderate.

## 16. `Schedule::next_after` swallows malformed cron expressions

**File:** `src/traits/background.rs:100-107`

`cron::Schedule::from_str(expr).ok()?` silently maps a malformed cron expression to `None`, which the scheduler interprets as "schedule exhausted". A plugin that ships a typo'd cron string therefore registers a job that never fires, with no diagnostic.

**Suggestion:** Validate the cron expression at registration time (in `add_scheduled_job` or in the `Schedule::Cron` constructor — make it `try_cron(s)`), and return a typed error. Then `next_after` can `expect`.

**Effort:** trivial.

## 17. `SchedulerJobRecord::PartialEq` compares only id and status

**File:** `src/scheduler.rs:340-344`

The hand-written `PartialEq` ignores most fields (next_fire_at, schedule, cancel, counters). This is surprising for callers — and probably wrong; if it's a sentinel "logical equality" it should be a named method like `fn matches(&self, other: &Self) -> bool`, not the `==` operator.

**Suggestion:** Drop the `PartialEq` impl and replace any callers with the explicit `r.id == other.id` they actually want. Or, derive `PartialEq` properly (after making `cron::Schedule` / `SystemTime` comparable — both already are).

**Effort:** trivial.

## 18. `pending_count` / `running_count` walk the entire vec under a Mutex

**File:** `src/scheduler.rs:232-248`

Each counter takes the lock and linear-scans. With the `DashMap` change suggested in #7 these become per-bucket scans, but better: maintain two `AtomicUsize` counters updated on every state transition. The transitions are already centralised in `tick_at` / `mark_finished` / `cancel` / `add_scheduled_job` / `requeue_orphaned_runs`.

**Suggestion:** Add `pending: AtomicUsize`, `running: AtomicUsize` and increment/decrement in the lifecycle methods. The "list/counts" surfaces then become O(1).

**Effort:** moderate.

## 19. Dead/unused `SchedulerHandle`

**File:** `src/scheduler.rs:524-542`

Wraps `Arc<Scheduler>` with a single accessor and no other methods. There's no usage in this crate; it appears intended for consumers but adds no value beyond `Arc<Scheduler>` itself.

**Suggestion:** Delete unless a consumer outside this crate uses it (a quick grep across the workspace will confirm). If retained, document the role that justifies the wrapper.

**Effort:** trivial.

## 20. Tests live alongside production code in `registry.rs`

**File:** `src/registry.rs:1215-1233`

Only two smoke tests in the same file; meanwhile, the registry has 1200+ lines and almost no test coverage of `apply_pending` / `remove_plugin` semantics — those are the load-bearing methods. The crate also has separate `tests/end_to_end.rs` and `tests/reload_dispatch.rs` integration files.

**Suggestion:** Move the apply/remove/preflight unit coverage in-tree. The architectural simplification in #3 will naturally make these easier to test.

**Effort:** moderate.

## 21. `procedure_with_arity` vs `procedure` arity-overload semantics are inconsistent

**File:** `src/registry.rs:516-560`

`procedure(&q)` returns the first overload deterministically by insertion order — but `DashMap` iteration order is not deterministic across registries, so "first" is actually undefined for a multi-arity name. The doc comment says "preserves the legacy single-arity lookup contract" but the contract is implicit and brittle.

**Suggestion:** Make `procedure(&q)` return `None` when multiple arities exist (forcing callers to disambiguate), or return the minimum-arity overload (deterministic). Document the choice. Currently a planner bug that omits arity could silently bind to the wrong overload.

**Effort:** trivial (semantics) or moderate (if planner sites need updating).

## 22. `Mutex<DashMap<…>>` in `per_plugin` is double-locking

**File:** `src/registry.rs:395, 847, 1124, 1158`

`per_plugin: RwLock<dashmap::DashMap<PluginId, PluginRecord>>` — DashMap is already internally synchronised, and the outer `RwLock` is taken `.read()` everywhere it's used (never `.write()`). The `RwLock` adds nothing.

**Suggestion:** Drop the `RwLock` and use `DashMap<PluginId, PluginRecord>` directly. Removes one lock acquisition per registration / lookup / removal.

**Effort:** trivial.

---

## Summary table

| #  | Severity     | Effort        | Area                              |
|----|--------------|---------------|-----------------------------------|
| 1  | High noise   | moderate      | registry — entry boilerplate      |
| 2  | High noise   | trivial       | registry — record duplication     |
| 3  | High value   | significant   | registrar/registry — surface plumbing |
| 4  | Low noise    | trivial       | registry — lookup helpers         |
| 5  | Medium       | moderate      | registry — virtual id allocators  |
| 6  | Low          | trivial       | crate-wide — Debug impls          |
| 7  | Medium       | moderate      | scheduler — data structure        |
| 8  | Low          | trivial       | scheduler — mark_finished         |
| 9  | Medium       | trivial-mod   | background — Manual semantics     |
| 10 | Medium       | moderate      | trigger — on_deferred default     |
| 11 | Medium       | moderate      | background — JobHost defaults     |
| 12 | Low          | trivial       | background/procedure — PhantomData|
| 13 | Medium       | moderate      | background — CancellationToken    |
| 14 | High value   | significant   | crate-wide — async policy         |
| 15 | Medium       | moderate      | reload — partial-failure contract |
| 16 | Medium       | trivial       | background — cron validation      |
| 17 | Low          | trivial       | scheduler — PartialEq surprise    |
| 18 | Low          | moderate      | scheduler — counter perf          |
| 19 | Low          | trivial       | scheduler — dead handle           |
| 20 | Low          | moderate      | tests organisation                |
| 21 | Medium       | trivial-mod   | registry — arity overload         |
| 22 | Low          | trivial       | registry — double-lock            |

Highest-leverage wins are #3 (architectural surface plumbing), #2 (record dedup, almost free), and #14 (async policy decision). Quick-wins worth doing in a single pass: #2, #4, #12, #16, #17, #19, #22.
