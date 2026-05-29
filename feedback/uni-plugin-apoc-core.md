# Code Simplifier Review — `crates/uni-plugin-apoc-core`

Scope: `src/lib.rs`, `src/procedures/{mod,bitwise,convert,create,math,number,text}.rs`, `tests/apoc_core_register.rs` (~2.3k LOC total).

The crate ships APOC-equivalent procedure plugins. The structure is consistent across files but suffers from heavy duplication of `ProcedureSignature` literals, `RecordBatch` assembly boilerplate, per-crate copies of arg-extraction helpers, and double registration (signatures inline in `register_into` plus duplicated again in `signature()`). Below are concrete simplification opportunities.

---

## 1. Massive duplication of `ProcedureSignature` between `register_into` and `signature()`

Across every procedure module the same signature is constructed twice with slightly different `docs` strings — once at registration time, once inside `ProcedurePlugin::signature()` (with a private `OnceLock` cache per variant) — and the registration value is then discarded by the registrar in favor of `signature()`.

- `src/procedures/bitwise.rs:33-65` (register) vs `bitwise.rs:120-136` (signature) — and the cache reproduces `binary_sig`/`unary_sig` with shorter docs (`"Bitwise AND."` vs `"Bitwise AND of two integers."`), creating an inconsistency hazard.
- `src/procedures/math.rs:32-83` vs `math.rs:167-191` — same pattern, ten variants, all duplicated; docs strings diverge (`"sigmoid"` vs `"Logistic sigmoid 1/(1 + exp(-x))."`).
- `src/procedures/text.rs:33-156` vs `text.rs:250-339` — duplication is worst here because `Length`, `Repeat`, `IndexOf` are inlined as full struct literals in *both* sites.
- `src/procedures/number.rs:28-83` vs `number.rs:93-152` — three `fn`s (`nullable_int`, `nullable_float`, `float_to_string`) are defined *inside* `signature()` solely to be called via `OnceLock`.
- `src/procedures/convert.rs:29-104` vs `convert.rs:115-143` — has a cleaner `build(yields_type, docs)` helper, but the structure is still duplicated.
- `src/procedures/create.rs:31-66` vs `create.rs:75-104` — same.

Suggestion: Define one `static SIG: OnceLock<ProcedureSignature>` per variant at module scope (or build a `static SIGS: OnceLock<[ProcedureSignature; N]>` table indexed by the enum discriminant). Make a single `fn signature_for(variant) -> &'static ProcedureSignature` and have `register_into` use the same value: `r.procedure(qname, signature_for(v).clone(), Arc::new(v))`. Removes 200+ LOC of duplicated literals and the docs-string drift. Effort: **moderate** (mechanical).

---

## 2. Per-file copies of `extract_i64` / `extract_f64` / `extract_string`

The same arg-extraction helpers appear with near-identical bodies and only the error-prefix string differing:

- `bitwise.rs:177-205` `extract_i64`
- `math.rs:266-281` `extract_i64`, `math.rs:283-315` `extract_f64`
- `number.rs:212-227` `extract_string`, `number.rs:229-244` `extract_f64`
- `text.rs:436-450` `extract_i64_text`, `text.rs:452-484` `extract_string`
- `create.rs:120-131` does it inline (`args.first().and_then(...)`).

Each diverges only in the error-prefix (`"bitwise: "`, `"math: "`, `"text: "`, `"number: "`) and in which `ScalarValue`/`Array` variants it accepts. Suggestion: lift a small `extract` module (e.g. `src/procedures/args.rs`) exposing `extract_i64(args, idx, ns: &str)`, `extract_f64(args, idx, ns)`, `extract_string(args, idx, ns)` — pass the namespace prefix instead of duplicating bodies. Effort: **trivial-to-moderate**.

---

## 3. `RecordBatch` / `SchemaRef` assembly boilerplate in every `invoke()`

Every `invoke` ends with: build a single-field `Schema`, wrap a one-element `Array`, call `RecordBatch::try_new`, wrap in `RecordBatchStreamAdapter`. The code is materially identical at:

- `bitwise.rs:162-174`
- `math.rs:201-263` (twice — Int path + Float path)
- `text.rs:395-401` plus `text.rs:404-434` (`string_result`/`bool_result`/`int_result` helpers — each is the same shape inline)
- `number.rs:163-206` (the same 12-line build is repeated three times for the three variants — `ParseInt`, `ParseFloat`, `ToString`)
- `convert.rs:219-225` plus `convert.rs:228-266` (four near-identical `build_*_result` helpers)
- `create.rs:111-158`

Suggestion: a single generic helper `fn single_row_stream(field_name: &str, dtype: DataType, nullable: bool, array: Arc<dyn Array>, err_code: u32, err_ns: &str) -> Result<SendableRecordBatchStream, FnError>` (or a `into_single_row_stream(field: Field, array: Arc<dyn Array>)`) collapses all the per-type `build_*` helpers and inline blocks. The `number.rs:160-209` block in particular shrinks from ~50 lines to ~15. Effort: **moderate**.

---

## 4. Inner `fn`s defined inside `signature()` purely for `OnceLock` init

- `number.rs:99-146` defines three `fn`s (`nullable_int`, `nullable_float`, `float_to_string`) *inside* `signature()`.
- `text.rs:266-281` defines `fn length_sig()` inside `signature()`.

These should be module-level (or eliminated entirely once Item 1 collapses the two registration sites). They are awkward and harm readability. Effort: **trivial**.

---

## 5. Dead/unused code

- `create.rs:163-164` — `#[allow(dead_code)] fn _force_int64array(_a: Int64Array) {}` with a misleading "Silence the unused-import warning" comment. The import (`Int64Array`) is in fact unused — the proper fix is to drop both the import and the stub. Effort: **trivial**.
- `bitwise.rs:111-119` `BitwiseProc::Not` is special-cased in `invoke` via a `match other { Self::Not => unreachable!() }` arm after already branching on `Self::Not` earlier — minor smell but harmless.

---

## 6. Awkward control flow in `BitwiseProc::invoke`

`bitwise.rs:143-160` does an outer `match self { Self::Not => ..., other => { let a = ...; let b = ...; match other { ... Self::Not => unreachable!() } } }`. Cleaner as a flat `match self` returning the result directly, with `Self::Not` extracting one arg and the binary variants extracting two — no nested match, no `unreachable!`. Effort: **trivial**.

---

## 7. Repetition between two `MathProc::invoke` schema branches

`math.rs:202-219` has two near-identical branches for `MaxLong`/`MinLong` differing only in the constant. Collapse to one: `Self::MaxLong | Self::MinLong => { let v = if matches!(self, Self::MaxLong) { i64::MAX } else { i64::MIN }; ... }` or factor into helper. Effort: **trivial**.

---

## 8. `ApocCorePlugin::manifest_value`: `unwrap_or_else` masks a real failure

`lib.rs:71-73`: `env!("CARGO_PKG_VERSION").parse::<Version>().unwrap_or_else(|_| Version::new(0, 0, 0))` — if the crate's `CARGO_PKG_VERSION` ever stops being a valid semver, the plugin silently reports `0.0.0`. Since this is compile-time-known, prefer `.expect("CARGO_PKG_VERSION is valid semver")` to surface the failure. Effort: **trivial**.

---

## 9. UUID generation lives in `procedures/create.rs` and is non-cryptographic

`create.rs:166-227` rolls a homegrown UUIDv4 with `xorshift64*` seeded by nanos + thread-id-hash + counter. The implementation is correct but:

- Comment at `create.rs:166-172` is accurate but the function would be more discoverable in a `mod uuid` (or a `util.rs`) since other namespaces may want UUIDs later.
- The crate already pulls many deps; consider adding `uuid` (with `v4` feature) once a non-trivial UUID requirement appears, rather than carrying a custom xorshift.

Suggestion: extract to `src/util/uuid.rs` and document the non-crypto contract once, rather than threading it through a procedure module. Effort: **moderate**.

---

## 10. Hard-coded magic numbers and error codes

Error codes `0x700` / `0x701` / `0x702` / `0x703` / `0x704` / `0x705` / `0x800` are sprinkled across files (`bitwise.rs:169`, `text.rs:396`, `math.rs:231,258`, `number.rs:170,186,202`, `convert.rs:220`, `create.rs:153`). They should be `pub(crate) const`s in a shared `errors` module so future readers can correlate them. Effort: **trivial**.

The OOM caps `1_000_000` appear at `text.rs:381` (`repeat`) and `create.rs:141` (`uuids`) — make a `const MAX_REPETITIONS` to share the policy and document it once. Effort: **trivial**.

---

## 11. `mod.rs` doc list is stale

`procedures/mod.rs:7-11` lists "Currently shipped: bitwise / text / math" but the crate now ships `convert`, `create`, and `number` as well (visible at `mod.rs:18-23`). Update the doc list. Effort: **trivial**.

---

## 12. Test boilerplate could be shared

Each module's test module re-derives an `invoke_one(...)` helper that builds `ColumnarValue::Scalar(...)`, calls `invoke`, and downcasts the first column (e.g. `bitwise.rs:212-225`, `math.rs:322-332`, `text.rs:491-504`, `create.rs:258-269`). Extract a small `#[cfg(test)] mod test_util` (or top-level `tests/common.rs`) with generic `async fn first_row<T: ArrayDowncast>(...)` that returns the scalar. Effort: **moderate**.

---

## Summary of effort

- Trivial: items 4, 5, 6, 7, 8, 10, 11.
- Moderate: items 1, 2, 3, 9, 12.
- Significant: none individually — but applying items 1 + 2 + 3 together would reduce the crate by an estimated 600+ LOC (≈25–30 %) with no behavior change.

The biggest single win is item 1 (collapsing the registration-site vs `signature()`-site duplication). It directly causes the docs-string drift visible across all six namespaces and is the dominant source of mechanical noise in the crate.
