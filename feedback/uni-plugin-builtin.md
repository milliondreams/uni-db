# Code Simplifier Feedback — `crates/uni-plugin-builtin/`

Scope: recently modified files (`Cargo.toml`, `src/background_jobs.rs`, `src/procedures/periodic.rs`).

## Summary

The two new modules ship correct and well-tested code, but both exhibit
substantial mechanical duplication: three near-identical `BackgroundJobProvider`
structs and six near-identical `ProcedurePlugin` structs that differ only by a
signature constructor and a small `invoke` body. Several helper signatures
share field lists, error-code constants are ad-hoc magic numbers, and one
field is dead. Below are concrete simplification opportunities ordered by
estimated payoff.

---

## `src/background_jobs.rs`

### 1. Triple-duplicated job-struct boilerplate (HIGH payoff)

- **Lines**: 46–105 (`TtlSweepJob`), 112–149 (`StatisticsRefreshJob`),
  157–213 (`CompactionJob`).
- **Problem**: All three jobs follow the identical pattern:
  - a `pub struct Foo { definition: JobDefinition }`
  - `new()` / `Default` boilerplate
  - `BackgroundJobProvider::definition` returning `&self.definition`
  - `execute()` that either no-ops (StatisticsRefresh) or dispatches one host
    method and translates errors to `JobOutcome::Failed { retry: true }`.
- **Suggestion**: Collapse to a single generic
  `struct MaintenanceJob { definition: JobDefinition, action: JobAction }`
  where `JobAction` is an enum (`Noop`, `WriteCypher(&'static str)`,
  `CompactStorage`). One `impl BackgroundJobProvider` covers all three; the
  `register_into` body becomes a table of `(qname, schedule, timeout, action)`
  tuples. Eliminates ~120 lines and centralises the
  "no host -> Done; host err -> Failed{retry}" pattern that currently lives
  twice (lines 87–103 and 196–212).
- **Effort**: ~30 min.

### 2. Magic error code `0xBAD` in test (LOW)

- **Line**: 333 (`FnError::new(0xBAD, ...)`).
- **Suggestion**: Use an existing `FnError::CODE_*` constant or document
  intent. Cosmetic.
- **Effort**: 2 min.

### 3. Tests duplicate `JobContext` construction (LOW)

- **Lines**: 264–266, 302–303, 339, 354–355, 368–369.
- **Suggestion**: Add a tiny `fn ctx_with(host: Option<&dyn JobHost>) -> JobContext<'_>`
  helper inside the test module.
- **Effort**: 5 min.

### 4. `RecordingJobHost::as_any` / `AlwaysFailHost::as_any` repeated (LOW)

- **Lines**: 278–280, 329–331.
- **Suggestion**: If `JobHost` has only `compact_storage` + `execute_write_cypher`
  used in tests, consider providing a default `as_any` impl on the trait (the
  upstream change); otherwise leave as-is.
- **Effort**: trait-side change; defer.

---

## `src/procedures/periodic.rs`

### 5. Six near-identical `ProcedurePlugin` structs (HIGHEST payoff)

- **Lines**: 161–239 (`PeriodicSchedule`), 242–279 (`PeriodicCancel`),
  282–334 (`PeriodicList`), 426–457 (`PeriodicSubmit`),
  469–516 (`PeriodicIterate`), 523–555 (`PeriodicCommit`).
- **Problem**: Each is `{ scheduler, signature }` with the same `new`,
  the same `signature()` accessor, and an `invoke` whose only variation is
  argument extraction + a scheduler call.
- **Suggestion**: Introduce one
  `struct PeriodicProc { scheduler: Arc<dyn SchedulerControl>,
   signature: ProcedureSignature, kind: PeriodicKind }`
  where `PeriodicKind` is an enum dispatching inside `invoke`. Or, less
  invasively, define a `make_proc!` macro or a closure-carrying generic
  wrapper `FnProc<F>` that owns a boxed `Fn(&[ColumnarValue], &dyn SchedulerControl)
   -> Result<SendableRecordBatchStream, FnError>`. Saves ~150 lines and
  removes the chance of drift between siblings.
- **Effort**: ~45 min.

### 6. `PeriodicCommit::_scheduler` is dead (MEDIUM)

- **Line**: 525 (`_scheduler: Arc<dyn SchedulerControl>`).
- **Problem**: Held only to keep the constructor symmetric; never read. If
  the v2 path will need it, fine, but today it forces the registrar to clone
  an `Arc` for no reason and emits an underscore-named field that confuses
  readers.
- **Suggestion**: Drop the field and accept `()` in `new` (or take
  `_scheduler` by value and discard). Worth doing alongside #5 since the
  unified struct sidesteps it.
- **Effort**: 5 min standalone; folds into #5.

### 7. Two `single_bool`-shaped batch builders (MEDIUM)

- **Lines**: 309–333 (`PeriodicList::invoke`), 503–515 (`PeriodicIterate::invoke`),
  557–570 (`single_bool`).
- **Problem**: All three hand-roll the same
  `Schema::new(...) -> RecordBatch::try_new -> RecordBatchStreamAdapter::new(stream::iter([Ok(batch)]))`
  pattern.
- **Suggestion**: Factor a `fn single_row_batch(fields: Vec<Field>, cols:
   Vec<Arc<dyn Array>>, err_code: u32, ctx: &str) -> Result<SendableRecordBatchStream, FnError>`
  helper. `single_bool` becomes a one-liner over it; `PeriodicList` and
  `PeriodicIterate` lose ~10 lines each.
- **Effort**: 15 min.

### 8. Ad-hoc error codes `0xB30`..`0xB36` (LOW)

- **Lines**: 194, 204, 217, 227, 272, 324, 510, 565.
- **Problem**: Magic constants; one code (`0xB33`) is reused for two
  different conditions (lines 217 cron-parse vs 272 cancel-qname).
- **Suggestion**: Promote to `const ERR_BAD_QNAME: u32 = ...;` etc. at module
  top; resolve the `0xB33` collision.
- **Effort**: 10 min.

### 9. Repeated `smol_str::SmolStr::new("...")` in signatures (LOW)

- **Lines**: 100, 106, 112, 130, 365, 383, 389, 395.
- **Suggestion**: Add a `named_arg(name, ty, doc)` helper that returns a
  `NamedArgType`; signatures become declarative tables.
- **Effort**: 15 min, but pairs nicely with #5.

### 10. `extract_utf8` discards the empty/null distinction inconsistently (LOW)

- **Lines**: 336–360.
- **Observation**: For `ColumnarValue::Scalar(Utf8(Some(s)))` an empty string
  is accepted, but for the `Array` branch `is_empty()` is treated as null.
  Probably fine but worth a one-line comment clarifying the asymmetry, or
  unify by checking `s.is_empty()` in the scalar branch too if that is
  intended.
- **Effort**: 5 min.

### 11. `_query` / `_options_json` extracted but unused (LOW)

- **Lines**: 496, 498.
- **Observation**: The Cypher inputs are pulled, validated as Utf8, then
  thrown away. Good for input validation; consider a `validate_utf8` helper
  that does not allocate the `String` (returns `()`), or document that the
  extraction is deliberate for arity-/type-checking only.
- **Effort**: 5 min.

---

## `Cargo.toml`

### 12. Direct `uuid` and `semver` deps not visible in the changed sources (LOW)

- **Lines**: 20, 32.
- **Observation**: `uuid` (1.8) and `semver` (1.0) are pinned as direct
  versions rather than `workspace = true`, unlike every other dep in the
  file. If the workspace already publishes these (check `[workspace.dependencies]`),
  switch to `workspace = true` for consistency and single-source version
  control. Also verify `uuid` is still used after the latest refactor — a
  quick `rg "uuid::" crates/uni-plugin-builtin/src` is worth running.
- **Effort**: 5 min including the audit.

### 13. `blake3` / `cron` likewise pinned directly (LOW)

- **Lines**: 34–35.
- **Suggestion**: Same as #12 — promote to workspace deps if other crates use
  the same versions, otherwise leave a one-line comment explaining why this
  crate pins its own.
- **Effort**: 5 min.

---

## Cross-cutting

- `register_into` (background_jobs.rs:228) and `register_into`
  (periodic.rs:59) live in different modules; both could share a brief
  doc-link from `lib.rs` so readers find them. Cosmetic.
- The two test modules each carry a `Recording*` fixture (background:270,
  periodic:582). If `uni-plugin` exposed a `mock` feature with these stubs,
  every downstream crate would benefit, but that's an upstream change rather
  than a local simplification.

## Suggested ordering

1. #5 (biggest reduction, unlocks #6, #7, #8, #9 cleanups).
2. #1 (same payoff shape, fewer downstream knock-ons).
3. #7 + #8 + #9 in one pass after #5 lands.
4. #12 + #13 + #2 + #3 + #4 + #10 + #11 as a final tidy.

Total estimated effort: **~2 hours** for the full refinement; **~75 min** for
just #5 + #1 + #7 which capture most of the duplication.
