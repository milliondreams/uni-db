# Code-Simplifier Review: `crates/uni-btic/`

Scope: full crate, with focus on recently modified `src/encode.rs`. No files were modified.

---

## src/encode.rs (recently modified)

### 1. `decode_slice` duplicates work done by `decode`
- **Location:** `src/encode.rs:42-50`
- **Issue:** The function manually checks length, then re-converts the slice with `try_into().expect(...)`. The fallback `expect` is justified but slightly noisy.
- **Suggestion:** Use `<&[u8; 24]>::try_from(bytes).map_err(|_| BticError::InvalidLength(bytes.len())).and_then(decode)` — eliminates the redundant `if/return` plus `expect`, in one expression. Alternatively, keep current shape but drop the inner `expect` by pattern-matching the `try_into` result instead. Either way the change reduces three statements to one.
- **Effort:** ~5 min.

### 2. Nested `word` helper inside `decode`
- **Location:** `src/encode.rs:24-29`
- **Issue:** The inner `fn word(slice: &[u8]) -> u64` exists only to wrap `try_into().expect(...)` three times. It is a slight abstraction tax; readers must scan the helper before reading the three call sites.
- **Suggestion:** Either inline as `u64::from_be_bytes(bytes[0..8].try_into().unwrap())` (lengths are statically known from a `&[u8;24]`, so `unwrap` is acceptable and matches the existing `expect` justification comment), or hoist to a module-private `fn read_u64_be(arr: &[u8;24], offset: usize) -> u64` if reuse becomes desirable. Current form is fine; flagging as a minor stylistic option, not a defect.
- **Effort:** ~5 min (optional).

### 3. Encode/decode are mirror operations but written in different styles
- **Location:** `src/encode.rs:8-16` vs `21-39`
- **Issue:** `encode` writes three windows inline with `copy_from_slice`; `decode` introduces a helper. Symmetric operations benefit from symmetric structure.
- **Suggestion:** Pick one style consistently. Inline both, or factor both. Slight readability win.
- **Effort:** ~5 min.

---

## src/set_ops.rs

### 4. `pick_bound_meta` uses a slightly awkward match-with-guard pattern
- **Location:** `src/set_ops.rs:104-120`
- **Issue:** `match va.cmp(&vb) { ord if ord == pick => ..., Ordering::Equal => ..., _ => ... }` mixes a guard with a literal arm. Because `pick` is only ever `Greater` or `Less` at call sites, the third arm is implicitly "the opposite ordering". The Equal arm being in the middle is unusual.
- **Suggestion:** Replace with explicit if/else: `if va == vb { (ga.finer(gb), ca.least_certain(cb)) } else if va.cmp(&vb) == pick { (ga, ca) } else { (gb, cb) }`. Clearer intent, no guard, no `_` catch-all hiding an inverse.
- **Effort:** ~5 min.

### 5. `bound_meta` / `bound_val` could be methods on `BoundSide`
- **Location:** `src/set_ops.rs:84-97`
- **Issue:** Two free functions exist solely to dispatch on `BoundSide`. They are fine but slightly verbose; could be `BoundSide::value_of(btic)` and `BoundSide::meta_of(btic)`. Pure stylistic preference.
- **Effort:** ~10 min (optional).

### 6. `Ordering` imported via both `std::cmp::Ordering` (full path) and `use std::cmp::Ordering`
- **Location:** `src/set_ops.rs:1` and inline uses `std::cmp::Ordering::Equal` at `115-117`
- **Issue:** `use std::cmp::Ordering;` is already imported at top of file, but `pick_bound_meta` re-qualifies via `std::cmp::Ordering` in the signature and arms. Minor inconsistency.
- **Suggestion:** Drop `std::cmp::` prefix from the function signature and match arms now that `Ordering` is in scope.
- **Effort:** ~2 min.

---

## src/btic.rs

### 7. `validate` duplicates bit-extraction logic with `lo_granularity`/`hi_granularity`/`lo_certainty`/`hi_certainty`
- **Location:** `src/btic.rs:49-94` vs `119-141`
- **Issue:** `validate` re-extracts `lo_gran_code`, `hi_gran_code`, `lo_cert_code`, `hi_cert_code`, `version`, `flags`, `reserved` via inline shifts/masks. The accessors do the same shifts; the duplication invites drift if the layout changes.
- **Suggestion:** Introduce `const` masks/shifts at module top (e.g., `const LO_GRAN_SHIFT: u32 = 60; const GRAN_MASK: u64 = 0xF;`) and have both `validate` and the accessors use them. Lower risk than coupling validate to the typed accessors (which themselves call `expect`).
- **Effort:** ~20 min.

### 8. `Display` impl for `Btic` has nested if/else-if chain on sentinel combinations
- **Location:** `src/btic.rs:210-253`
- **Issue:** Three sequential branches inspect `(lo != NEG_INF, hi != POS_INF)` combinations to emit granularity. Readable but mildly repetitive; the four-way logic could be a `match (self.lo == NEG_INF, self.hi == POS_INF)`.
- **Suggestion:** `match (lo_inf, hi_inf) { (false, false) => ..., (true, false) => ..., (false, true) => ..., (true, true) => () }`. Slight clarity improvement; not high-value.
- **Effort:** ~10 min.

### 9. Comment about INV-2 implication
- **Location:** `src/btic.rs:90-92`
- **Issue:** Comment is helpful but the corresponding `BticError::SentinelExclusivity` variant (`src/error.rs:9-10`) is now unreachable from validation. Confirm whether it is constructed anywhere; if not, it is dead code.
- **Suggestion:** `grep -r SentinelExclusivity crates/uni-btic`; if zero non-definition hits, remove the variant or document why it is retained (e.g., reserved for future external validators).
- **Effort:** ~5 min.

### 10. `new_unchecked` is marked `#[allow(dead_code)]`
- **Location:** `src/btic.rs:43-46`
- **Issue:** `#[allow(dead_code)]` indicates the function is unused inside the crate. If no downstream consumer needs it, this is dead code; if a downstream crate uses it, mark visibility accordingly and drop the allow.
- **Suggestion:** Audit callers across the workspace. Remove if unused, or remove the `allow` if used.
- **Effort:** ~5 min.

---

## src/parse.rs

### 11. `strip_bce_suffix` length checks have an off-by-one inconsistency
- **Location:** `src/parse.rs:100-108`
- **Issue:** Branch 1 uses `s.len() >= 4` matching `" BCE"` (4 chars). Branch 2 uses `s.len() > 3` matching `"BCE"` (3 chars). These are equivalent (`> 3` == `>= 4`), so the two branches are guarded by the same bound, which is confusing. Also: `"BCE"` alone (length 3) is rejected by `> 3` but would be a parse error anyway. Worth aligning style: both should be `>= len`.
- **Suggestion:** Use `s.len() >= 4` and `s.len() >= 3` for clarity, or rewrite with `s.to_ascii_uppercase().ends_with(" BCE")` / `.ends_with("BCE")` style. Or, more idiomatic: `s.strip_suffix_ignore_ascii_case(" BCE").or_else(|| s.strip_suffix_ignore_ascii_case("BCE"))` — though that method does not exist in std; consider a tiny helper.
- **Effort:** ~10 min.

### 12. `parse_datetime_component`: silently swallows timezone offset
- **Location:** `src/parse.rs:147-184`, helper at `249-274`
- **Issue:** `strip_timezone` returns `(s, offset)` but the offset is bound to `_tz_offset_secs` and discarded. This is either an unfinished feature or dead code in the helper. If TZ is intentionally not yet applied, the helper could just return the cleaned string; if it should be applied, that is a correctness bug worth noting.
- **Suggestion:** Either (a) collapse `strip_timezone` to return `&str` only until TZ handling is implemented, or (b) thread the offset through and adjust `ms`. Flagged as a simplification candidate, not a behavior change.
- **Effort:** ~10 min to simplify; ~30+ min to implement properly.

### 13. `expand_months` manual month-arithmetic loop
- **Location:** `src/parse.rs:314-332`
- **Issue:** `while month > 12 { month -= 12; year += 1; }` plus mirror loop handles month wrap-around. Works but is unnecessary because callers only pass `months ∈ {1, 3}`, so a single normalization is enough. More importantly, `chrono::Months` exists for exactly this.
- **Suggestion:** Use `date.checked_add_months(chrono::Months::new(months as u32))` (already pulling in chrono). Same for `expand_years` via `chrono::Months::new(years as u32 * 12)` or `Years` (in `chrono` ≥ 0.4.31). Removes ~15 lines and eliminates the while-loops.
- **Effort:** ~15 min.

### 14. Repeated `BticError::ParseError(format!(...))` boilerplate
- **Location:** `src/parse.rs:115-127, 211-244, 290, 330, 339, 350`
- **Issue:** A dozen sites construct the same wrapping; `parse_iso_component` has a particularly verbose chain.
- **Suggestion:** Add a private helper `fn parse_err(msg: impl Into<String>) -> BticError { BticError::ParseError(msg.into()) }` or, more idiomatically, an `impl From<String> for BticError` so `.map_err(|e| format!("invalid year: {e}"))?` flows naturally. Minor ergonomic win.
- **Effort:** ~10 min.

---

## src/predicates.rs

### 15. Predicate functions could be `impl Btic` methods
- **Location:** `src/predicates.rs:6-73`
- **Issue:** All twelve predicates take `&Btic` and `&Btic` (or `&Btic, i64`). They read naturally as methods (`a.overlaps(&b)`, `a.contains(&b)`, `a.before(&b)`) and would discover better via IDE. Currently they are free functions.
- **Suggestion:** Either move to `impl Btic { ... }` or keep as free functions but document why (e.g., for parity with `set_ops`). If kept free, the file is fine; this is purely an API-shape consideration.
- **Effort:** ~20 min if migrated (callers across workspace would need updates).

### 16. `btic_equals` is the natural meaning of `==` from a temporal-semantics viewpoint
- **Location:** `src/predicates.rs:56-58`
- **Issue:** The function exists because `PartialEq` is bytewise (includes meta). The doc explains this, but having both `==` and `btic_equals` invites footguns. Consider whether the temporal-equality semantics should be the default.
- **Suggestion:** Leave as-is (changing `PartialEq` would break hashing / storage), but consider renaming to `temporally_equal` to make the distinction unmistakable at call sites.
- **Effort:** ~5 min rename + caller updates.

---

## src/granularity.rs / src/certainty.rs

### 17. `Granularity::from_name` allocates via `to_lowercase`
- **Location:** `src/granularity.rs:64-79`
- **Issue:** `s.to_lowercase()` allocates a new `String` on every lookup. For a tiny match, `s.eq_ignore_ascii_case("millisecond")` chained or matched on `s.to_ascii_lowercase().as_str()` (still allocates) is typical; better, use `match s.as_bytes()` against known patterns, or simply `eq_ignore_ascii_case` per arm. Low-impact unless on a hot path.
- **Suggestion:** Pre-`let lower = s.to_ascii_lowercase();` (avoids unicode-fold cost) and match on `&lower`. ASCII-only is sufficient for these names.
- **Effort:** ~5 min.

### 18. `Certainty::from_code` returns `BticError::ParseError` instead of a dedicated variant
- **Location:** `src/certainty.rs:21-31`
- **Issue:** Inconsistent with `Granularity::from_code`, which uses `GranularityRange`. Either add a `CertaintyRange(u8)` variant or, since `from_code` is internal and validated upstream, mark it unreachable. The current error message is reasonable but does not match the invariant-numbered style of the others.
- **Suggestion:** Add a `CertaintyRange(u8)` variant to `BticError` and use it here. Symmetric with `GranularityRange`.
- **Effort:** ~5 min.

---

## src/error.rs

### 19. `SentinelExclusivity` variant — see item 9
- See finding 9. Likely dead.

---

## Cross-cutting / dead code

### 20. No `Cargo.toml` audit performed
- Not requested; flagging only that `#[allow(dead_code)]` on `new_unchecked` plus the potentially-unused `SentinelExclusivity` warrant a `cargo +nightly udeps` or simple grep sweep before any cleanup commit.

---

## Priority ranking

| Pri | Item | Effort |
|-----|------|--------|
| High | #13 use `chrono::Months` in `expand_months`/`expand_years` | 15 min |
| High | #7 deduplicate meta-word shift/mask constants between `validate` and accessors | 20 min |
| High | #9, #19 audit + remove `SentinelExclusivity` if dead | 5 min |
| Med | #4 simplify `pick_bound_meta` match-with-guard | 5 min |
| Med | #12 clarify or wire through timezone offset | 10–30 min |
| Med | #1 fold length-check into `decode_slice` | 5 min |
| Med | #11 align length-check style in `strip_bce_suffix` | 10 min |
| Low | #2, #3, #5, #6, #8, #10, #14, #15, #16, #17, #18 stylistic / consistency | 2–20 min each |

Total estimated effort to address all high+med items: ~1.5 hours. All low items combined: ~1 hour.
