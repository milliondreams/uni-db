# Code-simplifier review: `crates/uni-crdt/`

Scope: all `src/*.rs`, with primary focus on the recently modified `src/lib.rs`. No files were modified — findings only.

Overall the crate is small, well-factored, and consistent. The CRDT modules follow a uniform shape (struct + `new`/`default` + ops + `CrdtMerge`). Most observations are minor; the larger items are duplication around the `Crdt` enum dispatch and one inefficient `VectorClock::happened_before` path.

---

## High-value findings

### 1. Triple enum-variant dispatch in the `Crdt` enum
**Files**: `src/lib.rs:68-101` (`try_merge`, `type_name`), `src/registry_dispatch.rs:52-64` (`kind`).

`try_merge`, `type_name`, and `kind` each have a hand-written `match` that lists all 8 variants. Adding a new CRDT variant requires updating three sites in two files, and the wire-name (`type_name`) and `kind` strings can drift independently.

**Suggestion**: Centralize variant metadata. Either (a) a single private `fn variant_info(&self) -> (&'static str, &'static str)` returning `(type_name, kind_str)` and have public APIs delegate to it, or (b) drive everything from a `macro_rules!` that emits the enum, `try_merge`, `type_name`, `kind`, and the `merge` impl from one table. Option (a) is the lower-risk, higher-clarity refactor and matches the "explicit over clever" preference.

**Effort**: S (30-60 min including tests).

### 2. `type_name` in `TypeMismatch` error path uses `discriminant` formatting in one branch and the human name elsewhere
**File**: `src/lib.rs:78-83` vs `src/registry_dispatch.rs:84-89`.

`Crdt::try_merge` builds `CrdtError::TypeMismatch` using `format!("{:?}", std::mem::discriminant(a))` (opaque debug output like `Discriminant(0)`), while `merge_via_registry` uses the friendly `self.type_name()`. The struct-level docstring on `type_name` even says it exists "for error messages", so `try_merge` should use it too.

**Suggestion**: In `try_merge`, replace the `discriminant` formatting with `self.type_name().to_owned()` / `other.type_name().to_owned()`. This makes the two error sites consistent and the message readable. (Bonus: drop the unused `std::mem::discriminant` call.)

**Effort**: XS (5 min).

### 3. Two `impl Crdt` blocks in `lib.rs`
**File**: `src/lib.rs:64-101` and `src/lib.rs:117-127`.

`Crdt` has two `impl` blocks separated only by the `impl CrdtMerge for Crdt` block. There is no reason to split them — merging them into a single `impl Crdt { ... }` improves locality of related methods (`try_merge`, `type_name`, `to_msgpack`, `from_msgpack`).

**Effort**: XS (2 min).

### 4. `VectorClock::happened_before` does redundant lookups and an unnecessary second pass
**File**: `src/vector_clock.rs:42-66`.

The first loop calls `other.get(actor)` twice per entry (lines 47 and 50). The second loop only triggers when `strictly_less` is still false, but does a `contains_key` followed by a value test that re-checks logic already covered by the first loop's `count > other.get(actor)` guard.

**Suggestion**: Cache `other.get(actor)` once per iteration. Simplify the second pass: when `strictly_less` is still false at the end of the first loop, it's enough to detect any actor in `other.clocks` whose value exceeds the corresponding `self` entry (which is 0 when absent). The control flow is also clearer as a single helper that returns `(all_le, any_lt)`.

**Effort**: S (15-30 min, plus existing tests cover correctness).

### 5. Repeated `Default for X { ... } { FxHashMap::default() }` boilerplate
**Files**: `src/gset.rs:18-24`, `src/lww_map.rs:17-23`, `src/orset.rs:22-29`, `src/rga.rs:29-35`.

Each generic struct hand-writes `Default` because `#[derive(Default)]` does not work through generics without `T: Default` bounds — but in these cases the field is `FxHashMap`/`FxHashSet` which has its own `Default`. Rust supports `#[derive(Default)]` with `#[default]` bounds in newer editions, and on stable you can keep the derive by removing the unnecessary `T: Default` requirement using `#[derive(Default)]` on a struct whose fields' `Default` impls don't depend on `T`. Confirm and switch where possible.

**Suggestion**: Try replacing the manual `Default` impls with `#[derive(Default)]`. If trait bounds cause an issue, an explicit `where` on the derive (or a single inherent `impl<T> Default` without restating the where-clause beyond what the field requires) keeps the boilerplate down. Low priority — purely line-count win, no behavior change.

**Effort**: XS (10 min).

---

## Lower-value findings

### 6. `VCRegister::merge` discards `MergeResult`
**File**: `src/vc_register.rs:72-76`.

`impl CrdtMerge for VCRegister` calls `merge_register` and drops the return value. That's fine, but the `MergeResult` enum (`src/vc_register.rs:9-14`) is currently only used by the test (`vc_register.rs:117`). If no production caller observes it, consider either (a) documenting it as test-only / public-API for future causal-conflict UIs, or (b) inlining `merge_register` into `merge` and removing `MergeResult`.

**Effort**: XS (5 min decision; S to remove if chosen).

### 7. `Ordering::Greater | Ordering::Equal` match arm
**File**: `src/vc_register.rs:56-60`.

Combining `Greater` and `Equal` into one arm with the comment "Self is newer or equal" is correct, but worth noting that if `self == other` you still call `self.clock.merge(&other.clock)`, which is a no-op but allocates iteration overhead. Negligible.

**Effort**: XS (skip unless profiling motivates it).

### 8. `LWWMap::keys`, `LWWMap::len`, `LWWMap::is_empty` traverse the map three different ways
**File**: `src/lww_map.rs:55-70`.

`keys` filters via `iter().filter(...).map(...)`, `len` does `values().filter(...).count()`, `is_empty` calls `self.len()`. `is_empty == self.len() == 0` is O(n); the more typical CRDT semantics also let `is_empty` be O(n) since tombstoned entries exist, so this is acceptable. Consider a single private helper `live_entries()` returning an iterator and define the three in terms of it for consistency.

**Effort**: XS (10 min).

### 9. `LWWMap::remove` clones the key unconditionally
**File**: `src/lww_map.rs:41-47`.

`self.map.entry(key.clone())` clones even when the entry exists. Use `if let Some(reg) = self.map.get_mut(key) { reg.set(None, ts); } else { self.map.insert(key.clone(), LWWRegister::new(None, ts)); }` — or use `raw_entry` / `entry`'s `or_insert_with`. Microoptimization; only matters for expensive `K`.

**Effort**: XS.

### 10. `ORSet::contains`, `elements`, `len` repeat the "any non-tombstoned tag" predicate three times
**File**: `src/orset.rs:56-79`.

Extract `fn is_visible(tags: &FxHashSet<Uuid>, tombstones: &FxHashSet<Uuid>) -> bool` and use it in all three. Tiny clarity win.

**Effort**: XS (5 min).

### 11. `LWWRegister::merge` uses `serde_json::to_vec(...).unwrap_or_default()` for tie-break
**File**: `src/lww_register.rs:47-53`.

If serialization fails, both sides degrade to empty `Vec<u8>` which short-circuits the tie-break and silently keeps `self`. That's defensible but undocumented; a one-line comment noting "serialization failure falls back to keep-self" would help future readers. Not a behavior change.

**Effort**: XS.

### 12. `op_from_bytes` is a one-liner wrapper
**File**: `src/registry_dispatch.rs:122-124`.

`pub fn op_from_bytes(bytes: Vec<u8>) -> CrdtOp { CrdtOp { bytes } }` adds little over the struct literal. Either justify with a doc example showing real use, or drop it. (Module doc says "Convenience" — if no in-tree caller uses it, it's dead weight.)

**Effort**: XS (verify call sites; remove if unused).

### 13. `GCounter::increment` early-returns on `value == 0` but still allocates on first non-zero
**File**: `src/gcounter.rs:26-32`.

Behaviorally correct. Consider matching `LWWMap::remove`'s pattern (avoid `to_string()` clone of `actor` when entry exists) via `if let Some(c) = self.counts.get_mut(actor) { *c += value; } else { self.counts.insert(actor.to_string(), value); }`. Microoptimization.

**Effort**: XS.

### 14. `Rga::to_vec` stack-traversal comment vs implementation
**File**: `src/rga.rs:79-96`.

The iterative traversal is correct and well-commented (test_stack_overflow_prevention covers it). One small clarity nit: the sort order comment at line 74 says "(timestamp DESC, id DESC)" — note that combined with stack-based pop semantics this yields ascending traversal (the actual user-visible order). A one-line note "(reversed because we pop from the stack)" would prevent future "fix" attempts.

**Effort**: XS.

### 15. Dead code check
No obvious dead code. `MergeResult` (#6) is the only suspect — used in tests and as a public API but unused internally outside `merge_register`.

### 16. Module ordering in `lib.rs`
**File**: `src/lib.rs:7-15`.

`pub mod registry_dispatch;` is interleaved alphabetically (between `orset` and `rga`). That's fine, but since it's a cross-cutting bridge rather than a CRDT type, grouping it after all data-type modules (with a blank line) signals its different role.

**Effort**: XS (cosmetic).

---

## Summary of priorities

1. **Fix `try_merge`'s opaque `discriminant` error** (#2) — quick correctness/UX win.
2. **Centralize the 3-way variant dispatch** (#1) — pays off as variants grow.
3. **Merge the two `impl Crdt` blocks** (#3) — trivial cleanup.
4. **Tighten `VectorClock::happened_before`** (#4) — small perf + clarity.
5. The rest are XS-effort polish items; apply opportunistically when touching the surrounding code.

No functionality changes are required; everything above preserves observable behavior.
