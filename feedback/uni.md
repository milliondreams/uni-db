# Code Simplifier Review — `crates/uni/`

Review-only pass on the plugin-fw branch. Findings target recently-modified files (see `git status`) with a focus on duplicated code, dead code, overly complex functions, and inconsistent patterns.

## `src/api/triggers.rs`

### F1. Duplicated atomic-JSON-sidecar I/O pattern (DeferralSidecar, CdcCheckpointSidecar, SystemLabelSchedulerPersistence)
- **Location**: triggers.rs:1800-1828 (`DeferralSidecar::read_all`, `write_all`), cdc_runtime.rs:91-127 (`CdcCheckpointSidecar::load_all`, `write_all`), scheduler_persistence.rs:90-118 (`read_all`, `write_all`).
- **Issue**: Three near-identical implementations of "exists check → read bytes → empty short-circuit → JSON parse" and "mkdir parent → write tmp → fsync → rename" appear across three files. The error-formatting style (`format!("read {:?}: {e}", path)`) and tmp-file rename pattern are line-for-line copies.
- **Suggestion**: Extract a `SystemSidecar<T: Serialize + DeserializeOwned>` helper in `src/persistence.rs` (or a new `src/sidecar.rs`) exposing `read_all() -> Result<Vec<T>>` and `write_all(&[T])`. The three call sites collapse from ~30 lines each to ~5.
- **Effort**: moderate

### F2. Triple-nested `Ok(Ok(...))` / `Ok(Err(...))` / `Err(_)` match on `catch_unwind` over `plugin.fire` is repeated 3x
- **Location**: triggers.rs:555-590 (sync before-phase), 630-657 (async after-phase spawn), 1715-1753 (deferral queue tick).
- **Issue**: The trio of arms (`Defer → enqueue`, `Continue/other → noop`, `Err → warn`, panic → warn) is duplicated with only the log message and the `enqueue_deferral` call shape varying. The sync `fire_caught` at line 699 is yet a fourth near-copy.
- **Suggestion**: Extract `handle_fire_result(result, ...) -> ()` that takes the outcome plus an enqueue closure / config; all four call sites reduce to one function call. Or define an internal `FireDisposition` enum.
- **Effort**: moderate

### F3. `mask_to_discriminant` reinvents `u32::trailing_zeros`
- **Location**: triggers.rs:1441-1454.
- **Issue**: Hand-rolled bit-position loop. Standard library provides `m.0.trailing_zeros()`.
- **Suggestion**: `if m.0 == 0 { 0 } else { (m.0.trailing_zeros() as u8) + 1 }`.
- **Effort**: trivial

### F4. `EventRowColumns::extend` is dead-code-ish — used once
- **Location**: triggers.rs:1367-1372, called only by `materialize_all` (triggers.rs:1278-1281).
- **Issue**: `extend` is an unused-style consuming builder method that wraps a trivial for-loop. `materialize_all` is the only caller and can inline `for row in &self.rows { cols.push_row(row); }`.
- **Suggestion**: Inline at the single call site; remove `extend`.
- **Effort**: trivial

### F5. `rewrite_property_refs` walker is hand-rolled where `tree_node::TreeNode` would do
- **Location**: triggers.rs:250-332.
- **Issue**: ~80-line recursive match on every `Expr` variant. Adding new Cypher AST variants requires editing this match. The `uni_cypher::ast::Expr` family likely already implements visitor traits (similar walker patterns appear in the codebase).
- **Suggestion**: If `uni_cypher::ast` exposes a visitor, use it. Otherwise consider macro-generated walk. Lower-priority — current code is at least exhaustive.
- **Effort**: significant (depends on AST surface)

### F6. `compile_predicate` is a 70-line procedure mixing 7 concerns
- **Location**: triggers.rs:126-203.
- **Issue**: Parses, rewrites, translates, registers UDFs, resolves UDFs (twice), coerces types, wraps in filter plan, re-resolves, creates physical expr. Each stage has an `Err` arm with hand-formatted error. The comment-density is high but the function is hard to skim.
- **Suggestion**: Split into 3 helpers: `parse_and_rewrite`, `coerce_against_event_schema`, `to_physical_expr`. Error-wrapping moves to a single `.map_err(|e| format!("{stage}: {e}"))` per helper, keeps the contextual prefixes.
- **Effort**: moderate

### F7. `RouteEntry` field-by-field clone of `sub.labels.as_ref().map(|v| v.iter().map(|s| s.to_string()).collect())` is repeated 3x
- **Location**: triggers.rs:478-489 — `label_filter`, `edge_type_filter`, `property_filter` all use the same pattern.
- **Suggestion**: A local closure `let to_vec_string = |o: Option<&[SmolStr]>| o.map(|v| v.iter().map(ToString::to_string).collect());` or a single utility fn.
- **Effort**: trivial

### F8. `enqueue_deferral` and `fire_caught` could share the post-fire dispatcher
- **Location**: triggers.rs:671-696 (`enqueue_deferral`), 699-733 (`fire_caught`).
- **Issue**: `fire_caught` is exclusively used by the sync after-phase path and just inlines the same dispatch table from F2.
- **Suggestion**: see F2 — the consolidated `handle_fire_result` subsumes both.
- **Effort**: moderate (part of F2)

## `src/cdc_runtime.rs`

### F9. `write_one` is a read-modify-write that races against itself
- **Location**: cdc_runtime.rs:144-155.
- **Issue**: `load_all → mutate → write_all` with no lock. Multiple `CdcRuntime::deliver_commit` calls run under `Mutex<Vec<ActiveStream>>` so callers are serialized in practice — but the contract isn't expressed and a future refactor could break it.
- **Suggestion**: Document the implicit serialization or wrap the sidecar in `Mutex<>`. Minimum: add a comment at line 144.
- **Effort**: trivial

### F10. `CdcRuntime::Debug` impl duplicates `field("checkpoint_path", ...)` pattern
- **Location**: cdc_runtime.rs:182-189.
- **Issue**: Fine as-is but `.path.clone()` allocates on every debug print. Use `Debug::fmt` directly via `&self.checkpoint.as_ref().map(|c| &c.path)`.
- **Effort**: trivial

### F11. Empty-batch fallback in `deliver_commit` duplicates schema knowledge
- **Location**: cdc_runtime.rs:337-341.
- **Issue**: `Arc::new(RecordBatch::new_empty(event_row_schema()))` materializes a fresh empty batch on every CDC-active commit when `notif.mutations` is `None`. Cache it as a `LazyLock<Arc<RecordBatch>>`.
- **Suggestion**: `static EMPTY_BATCH: LazyLock<Arc<RecordBatch>> = ...;` — avoids the per-commit allocation.
- **Effort**: trivial

## `src/scheduler.rs`

### F12. `tokio::task::block_in_place(|| Handle::current().block_on(...))` pattern repeated
- **Location**: scheduler.rs:188-191 (`compact_storage`), 209-219 (`execute_write_cypher`).
- **Suggestion**: A small `fn block_on_async<F: Future>(f: F) -> F::Output { ... }` helper in the module.
- **Effort**: trivial

### F13. `dispatch_one_tick` is 87 lines mixing 4 concerns
- **Location**: scheduler.rs:400-486.
- **Issue**: Circuit-breaker gate, provider lookup, persistence transition, spawn_blocking dispatch — all in one function with 3 `continue` early-exits. The Arc::clone fan-out (lines 443-448) is repetitive.
- **Suggestion**: Extract `dispatch_due_job(...)` that handles one job; the loop becomes `for id in due { dispatch_due_job(...); }`. The Arc::clones move into a small struct passed by ref.
- **Effort**: moderate

### F14. Dead `let mut s = scheduler.list(); let _ = std::mem::take(&mut s);`
- **Location**: scheduler.rs:267-268.
- **Issue**: Calls `scheduler.list()` and immediately discards the result via `mem::take` on a local Vec — does nothing observable. Likely a leftover from a refactor that was meant to drain the scheduler before reload.
- **Suggestion**: Remove the two lines or replace with the intended `scheduler.clear()`-style call if one exists.
- **Effort**: trivial

## `src/scheduler_persistence.rs`

### F15. `upsert` and `record_scheduled` both do find-or-push with cloned strings
- **Location**: scheduler_persistence.rs:120-154 (`upsert`), 158-182 (`record_scheduled`).
- **Issue**: Two implementations of the same find/update/insert pattern, differing only in which fields are mutated. `record_scheduled` also doesn't update the `status` field on the existing row, which may be intentional but isn't documented.
- **Suggestion**: Single `fn modify_row(id, |row| { ... })` helper that handles read-load-find-or-insert-write boilerplate.
- **Effort**: moderate

### F16. Cypher mirror uses ad-hoc string-escape (`replace('\'', "''")`) instead of parameter binding
- **Location**: scheduler_persistence.rs:141-145, 212-215.
- **Issue**: SQL-injection-style escaping of qnames into Cypher literals. The `LazyCypherSink::try_write_cypher` likely accepts parameters; using them avoids the escape dance entirely and the inherent fragility (a qname containing `\\` or other characters that need escaping will still break).
- **Suggestion**: Switch to parameterized writes if the sink supports them; otherwise centralize the escape into a `cypher_escape_literal` helper used by both call sites.
- **Effort**: moderate

### F17. `load_all`'s qname parse via `rsplitn(2, '.')` silently drops malformed rows
- **Location**: scheduler_persistence.rs:228-247.
- **Issue**: A qname like `"bare"` (no namespace dot) silently drops via `filter_map`. No warn-log. Operators will see "jobs disappeared after restart" with no signal.
- **Suggestion**: Use `QName::parse` if one exists, or at minimum log a warn on the discarded row.
- **Effort**: trivial

## `src/observability.rs`

### F18. `http_get_with_traceparent` belongs in a host-net module, not observability
- **Location**: observability.rs:168-185.
- **Issue**: An `http_get` helper has nothing to do with OTel besides the header injection. Coupling them puts an opinionated reqwest dep on what should be a pure tracing module.
- **Suggestion**: Move to a `host_net` module or `examples/otel-demo/`. The header-injection logic is one line; demo callers can inline it.
- **Effort**: moderate

### F19. `OtelConfig` is a 2-field struct with no defaults; consider `impl Default`
- **Location**: observability.rs:57-63.
- **Issue**: Embedders must always supply both fields explicitly. A `Default` impl with `localhost:4317` and `"uni-db"` would shrink call sites.
- **Effort**: trivial

## `src/api/notifications.rs`

### F20. `CommitStream::next`'s filter chain has redundant `is_some_and`/closure idiom
- **Location**: notifications.rs:73-102.
- **Issue**: Three sequential filter `if` blocks each follow the same `if filter.as_ref().is_some_and(|x| !matches) { continue; }` shape. Workable, but a small helper or a builder of `Box<dyn Fn(&Notif) -> bool>` would centralize it.
- **Effort**: moderate (low value)

### F21. `WatchBuilder::labels` / `edge_types` allocate twice (`iter().map().collect()`)
- **Location**: notifications.rs:142-150.
- **Issue**: `labels.iter().map(|s| s.to_string()).collect()` — `s` is `&&str`, so `(*s).to_string()` saves one deref. Minor.
- **Effort**: trivial

## `src/api/transaction.rs`

### F22. `classify_verb` is a fragile prefix-match classifier
- **Location**: transaction.rs:152-179.
- **Issue**: Acknowledged "deliberately shallow" but the comment promises parser-driven follow-up. Test coverage isn't visible; a single trailing-whitespace bug in user Cypher (e.g., `"  CREATE  INDEX"`) would mis-classify.
- **Suggestion**: Switch to `s.split_whitespace().take(2)` and match on the lexical tokens instead of substring `starts_with`. Slightly more robust without becoming a real parser.
- **Effort**: trivial

### F23. New CDC-batch materialization is a 3-line branch that could be inlined
- **Location**: transaction.rs:834-844 (per the diff).
- **Issue**: `let mutations_batch = if cdc_active { trigger_events.as_ref().and_then(|e| e.materialize_all()).map(Arc::new) } else { None };` is fine, but mixing it with the existing notification-construction block makes the commit fn longer than it needs to be. Keep readable.
- **Effort**: trivial (just inline as `cdc_active.then(|| ...).flatten()`)

### F24. `Transaction::query` now has dual `principal-or-not` branch around `execute_internal_with_tx_l0`
- **Location**: transaction.rs:275-291 (per the diff).
- **Issue**: The pattern `match principal { Some(p) => scoped_with_principal(p, fut).await, None => fut.await }` appears in `session.rs` too (multiple sites — see diff). When the same principal-threading wraps 5+ call sites it's worth a helper `fn maybe_scope(principal, fut)`.
- **Suggestion**: Add `uni_query::maybe_scope_with_principal(Option<Arc<Principal>>, fut)` that handles `None` internally. Each call site collapses to one call.
- **Effort**: moderate

## `src/api/mod.rs`

### F25. `cloud_config_to_lancedb_storage_options` is 80 lines of `if let Some(v) = ... { opts.insert(...) }`
- **Location**: mod.rs:3388-3460.
- **Issue**: Every branch of the `CloudStorageConfig` match uses the same `if let Some` insert idiom. A small `macro_rules! insert_some { ($opts:expr, $key:expr, $val:expr) => { ... } }` or a `fn insert_opt(opts, key, value: Option<&str>)` helper cuts the function to ~30 lines.
- **Effort**: moderate

### F26. Periodic-procedure registration block carries a dead `_manifest` (lines 3276-3292)
- **Location**: mod.rs:3274-3296.
- **Issue**: Comment says "Manifest is held only for future signature-verification logic; the registrar dispatches by plugin id." The manifest is built (~16 lines) and then immediately discarded — there's no future use bound, and tracking dead-code-for-future-use scattered through the codebase causes drift.
- **Suggestion**: Delete the unused manifest construction; restore it when the signature-verification path lands. Or, if the construction is meant to validate that the manifest builder doesn't panic, hoist it into a unit test.
- **Effort**: trivial

### F27. `Uni::periodic_schedule` fully-qualified trait disambiguation is verbose
- **Location**: mod.rs:691-700 (per the diff).
- **Issue**: `<crate::scheduler::SchedulerHost as uni_plugin::scheduler::SchedulerControl>::add_scheduled_job(&self.inner.scheduler_host, id, schedule)` is verbose. Likely because `Scheduler` (the primitive) also has an inherent method of the same name.
- **Suggestion**: Add a `pub fn schedule(...)` inherent method to `SchedulerHost` that delegates to the trait — then `self.inner.scheduler_host.schedule(id, schedule)` reads naturally.
- **Effort**: trivial

## Cross-file themes summary
- **JSON sidecar copy-paste** (F1, F9, F15) — three near-identical persistence backends would benefit from a shared `SystemSidecar` abstraction.
- **catch_unwind + outcome match** (F2, F8) — dispatch table over `Result<Result<TriggerOutcome>>` is duplicated 4 times in triggers.rs alone.
- **Principal/session-scope threading** (F24) — same `match Option<Principal>` ceremony at every call site.
- **Hand-rolled standard idioms** (F3, F14, F21) — `trailing_zeros`, dead `mem::take`, double-allocation in `.iter().map().to_string()`.
- **Function size & concern-mixing** (F6, F13, F25) — `compile_predicate`, `dispatch_one_tick`, and `cloud_config_to_lancedb_storage_options` each mix 4+ concerns and would benefit from extraction.
