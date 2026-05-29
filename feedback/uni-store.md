# Code Simplifier Feedback — `crates/uni-store/`

Scope: 30,652 LOC across 53 source files. No clippy warnings emitted. Focus areas below are ordered by impact-to-effort ratio.

---

## 1. Duplication

### 1.1 `scan_*_table` family — ~330 lines of near-identical code
**Files:**
- `src/storage/manager.rs:1031` `scan_vertex_table`
- `src/storage/manager.rs:1092` `scan_delta_table`
- `src/storage/manager.rs:1155` `scan_main_vertex_table`

**Problem:** Three async methods share the same skeleton: (a) `backend.table_exists`, (b) filter columns against `get_table_schema`, (c) compose `_version <= hwm AND (caller_filter)` from the 4-way `(Option<HWM>, Option<filter>)` cartesian, (d) `backend.scan` → `concat_batches`. Only the resolved `table_name` and the column-filtering policy differ. The 4-way `match` for HWM+filter composition is repeated verbatim in each.

**Suggestion:** Extract a private helper `scan_table_with_hwm(&self, table_name, columns, additional_filter, restrict_columns_to_schema: bool)` returning `Result<Option<RecordBatch>>`. Add a small `compose_version_filter(hwm: Option<u64>, user: Option<&str>) -> Option<String>` to fold the 4-way match into one place (it appears at least 3x in this file). Net: removes ~180 LOC.

**Effort:** ~1 hour (mechanical, 7 callers across `uni-query`, `uni-algo`, `uni`).

---

### 1.2 `AdjacencyManager::get_neighbors` vs `get_neighbors_at_version`
**Files:** `src/storage/adjacency_manager.rs:96` and `:161` (~150 LOC combined).

**Problem:** Identical four-stage merge (Main CSR → frozen segments → active overlay → Shadow CSR) with the *only* difference being a `ver <= version` filter and tombstone `ts.version > version` comparison. The "shadow CSR resurrect" stage is the lone branch unique to versioned.

**Suggestion:** Parameterize as `get_neighbors_inner(vid, edge_type, dir, visibility: VersionFilter)` where `VersionFilter::All` (current) and `VersionFilter::AtVersion(u64)` (snapshot) are matched at each gate. Encapsulate the `not_tombstoned` predicate as a small closure. Net: ~70 LOC saved, single source of truth for visibility.

**Effort:** ~1.5 hours (well-tested via 19 unit tests in same file — low risk).

---

### 1.3 Constraint-validation duplication
**File:** `src/runtime/writer.rs:395`–`820`.

**Problem:** `validate_vertex_constraints_for_label` (single-vertex) and `validate_vertex_batch_constraints` (batch) re-implement the constraint dispatch loop (`enabled` check + `ConstraintTarget::Label` match + `Unique/Exists/Check` arms). The Unique key-building (`format!("{}:{}", prop, val)`, `key_parts.join("|")`, `all_present` flag) is implemented three times (lines ~440, ~684, ~768) plus once in `collect_constraint_keys_from_properties`.

**Suggestion:** Introduce `fn build_unique_key(properties: &Properties, unique_props: &[String]) -> Option<String>` returning `None` when any prop is absent. Introduce `fn iter_label_constraints<'a>(schema, label) -> impl Iterator<Item=&'a Constraint>` that handles the `enabled` + `ConstraintTarget::Label` filtering once. Net: ~80 LOC saved, eliminates bug-class where one copy diverges from another.

**Effort:** ~2 hours.

---

### 1.4 `values_to_*_array` builder family
**File:** `src/storage/arrow_convert.rs:580`–`664` (uint64/int64/int32/string/bool/float32/float64 — 85 LOC of 7 near-identical 12-line bodies).

**Suggestion:** A generic helper `fn build_numeric<B: ArrayBuilder, T>(values: &[Value], extract: impl Fn(&Value) -> Option<T>, append: impl FnMut(&mut B, T))` collapses six of the seven to call sites. Or simply a `build_optional_column!` macro mirroring the existing `build_list_column`/`build_map_column` generic helpers already in this file (lines around 700–800). Net: ~50 LOC saved.

**Effort:** ~45 minutes; the file's existing tests cover all variants.

---

### 1.5 `value_from_column` vs `decode_column_value`
**File:** `src/storage/value_codec.rs:43, 381`.

**Problem:** `decode_column_value` is a 20-line shim that detects six temporal/bytes types and routes to `arrow_to_value`; everything else falls through to `value_from_column`. Meanwhile `runtime/property_manager.rs:1720` defines yet another `value_from_column` that *also* just delegates to `decode_column_value`. So we have three names for two behaviors.

**Suggestion:** Inline the temporal/Bytes routing into `value_from_column` itself (it already knows `DataType`); delete `decode_column_value` and the `PropertyManager::value_from_column` shim. Net: simpler call graph; one canonical decoder.

**Effort:** ~30 minutes (6 call sites).

---

### 1.6 `execute_primary_scan` vs `execute_branch_scan`
**File:** `src/backend/lance.rs:160`, `:192`.

**Problem:** Both apply columns / filter / limit to a query, but to different underlying APIs (`lancedb::Table::query` vs `lance_branch::open_branch().scan()`). Significant structural duplication with subtly different error-message formats.

**Suggestion:** Lower priority than the others because the underlying APIs genuinely differ. Consider extracting a `ScanParams` struct holding `(columns, filter, limit)` and two thin `apply_to_lancedb_query` / `apply_to_lance_scanner` methods on it, so additions (e.g. new `ScanRequest` field) update both paths in lockstep.

**Effort:** ~1 hour.

---

## 2. Dead / Stub Code

### 2.1 `load_properties_columnar` is an unimplemented stub
**File:** `src/runtime/property_manager.rs:787`–`855`.

The function returns `Err("Columnar property load not fully implemented yet")` after doing real work building a vid vector and calling `get_batch_vertex_props`. The body contains ~40 lines of explanatory comments and the literal stub message `"// Skipping detailed columnar builder for brevity"`. A grep shows **zero callers** in the workspace.

**Suggestion:** Delete the function and its comments outright. If/when columnar loading is needed, add it then. Net: ~70 LOC removed; eliminates an exception-returning code path that masquerades as an API surface.

**Effort:** 5 minutes.

---

### 2.2 `recovery.rs` blanket suppression
**File:** `src/fork/recovery.rs:154` — `#[allow(unused_imports, dead_code)]` over a test module.

**Suggestion:** Audit the items below it; remove what is truly dead and let clippy police the rest. Bare-minimum `allow` should target individual items, not the module.

**Effort:** ~20 minutes.

---

## 3. Overly Complex Functions

### 3.1 `Writer::flush_to_l1` — 647 lines, single function
**File:** `src/runtime/writer.rs:~960` (line range straddles 600+).

**Problem:** A single async function 647 lines long is the L0→L1 flush pipeline. Reading the surrounding code shows it stitches together: manifest read, per-label batch building, index updates, snapshot manifest write, adjacency dual-write, post-flush fragment-warn metric, and cached-manifest update. Stack traces and code-review become unmanageable.

**Suggestion:** Split into private helpers along natural seams:
- `materialize_l0_vertex_batches(&l0, &schema) -> Vec<(label, RecordBatch)>`
- `materialize_l0_edge_batches(&l0) -> Vec<...>`
- `write_label_batch(&self, label, batch) -> Result<LabelSnapshot>`
- `update_indexes_for_label(...)`
- `assemble_manifest(...)`
- `check_fork_fragment_threshold(&self)`

The function body becomes ~80 lines of orchestration calling six well-named helpers, each independently testable. The 3258-line `writer.rs` benefits structurally too.

**Effort:** ~4–6 hours. Highest single-file payoff in the crate. Existing tests around `commit_transaction_l0` / `flush_to_l1` provide good safety net.

---

### 3.2 `Writer::validate_vertex_batch_constraints` — 244 lines
**File:** `src/runtime/writer.rs:585`.

Combined with §1.3 extraction (`build_unique_key`, `iter_label_constraints`), this drops to ~100 LOC. The current 4-step inline plan (NOT NULL → build index → per-vertex check → storage probe) should each be its own helper for readability.

**Effort:** included in §1.3.

---

### 3.3 `arrow_convert::arrow_to_value` — 403 lines
**File:** `src/storage/arrow_convert.rs:177`.

**Problem:** One function with ~15 `DataType` arms; each arm independently downcasts, checks null, extracts, decodes. Average arm is 25–35 lines with heavily-nested `if let … && let … && let … {}` chains (e.g. DateTime arm lines 187–218).

**Suggestion:** Per-arm extraction into module-private `decode_datetime_struct(col, row)`, `decode_time_struct(col, row)`, etc. (already partially done for `values_to_*`). This is a low-risk mechanical split that improves both readability and unit-test granularity.

**Effort:** ~2 hours.

---

### 3.4 `value_codec::value_from_column_inner` — 328 lines
**File:** `src/storage/value_codec.rs:53`.

**Problem:** Same shape as §3.3: one giant `match data_type`. Each arm is independently testable.

**Suggestion:** Per-type private helpers; the arms become one-liners (`DataType::String => decode_string(col, row)`).

**Effort:** ~1.5 hours.

---

## 4. Unnecessary Abstractions / Style

### 4.1 `build_timestamp_column_from_vid_map` + `build_timestamp_column_from_eid_map`
**File:** `src/storage/arrow_convert.rs:55, 65`.

Both are 9-line monomorphization wrappers around the already-generic `build_timestamp_column_from_id_map<K, I>`. Since the generic version is fully usable directly (`K: Eq + Hash`), the wrappers exist only to spell the type. Either delete them (callers pass concrete `HashMap<Vid, i64>`, type inference handles `K`) or, if turbofish at call sites is unwelcome, keep just one and drop the other.

**Effort:** 15 minutes.

---

### 4.2 `PropertyManager::new` vs `with_plugin_registry`
**File:** `src/runtime/property_manager.rs:42-60`.

The legacy `new` builds an empty `PluginRegistry` and delegates. Documented as a "back-compat shim for ~17 call sites." Consider: add `Default for PluginRegistry` and let callers pass `Default::default()`, or replace the 17 call sites with explicit registry passing. Removing `new` would make the registry-aware code path the only one and prevent silent divergence.

**Effort:** ~30 minutes including call-site updates.

---

### 4.3 Workspace-style TODO comment in `property_manager.rs:838`
A 15-line block of stream-of-consciousness commentary ("Let me inspect schema for first label found", "For now, support basic types", "Let's throw Unimplemented for columnar for now") ending in `Err("Columnar property load not fully implemented yet")`. See §2.1; deletion resolves both.

---

### 4.4 `Properties` overlay loop with same-keyed shapes
**File:** `src/runtime/property_manager.rs:523-578` `overlay_l0_batch`.

The inner `is_crdt` lookup (`labels.and_then(|ll| ll.iter().find_map(|ln| schema.properties.get(ln).and_then(|lp| lp.get(k)).filter(...))).is_some()`) is duplicated nearly verbatim around lines 760–785 in `apply_l0_overlay` (the edge-side variant). Extract `fn is_crdt_prop(schema, labels: Option<&[String]>, key: &str) -> bool` once.

**Effort:** 20 minutes.

---

## 5. Test Code Notes

- `adjacency_manager.rs` has 19 in-file tests; very thorough, no simplification needed beyond §1.2.
- `writer.rs` test module at file end contains a 141-line `test_commit_transaction_wal_before_merge` — consider splitting into focused unit tests, but low priority.
- `value_codec.rs` test coverage of `value_from_column` is excellent (one test per DataType) and would catch any regression from §1.5 / §3.4 refactors.

---

## Summary of Recommended Effort

| Item | Effort | LOC saved | Risk |
|------|--------|-----------|------|
| §2.1 delete `load_properties_columnar` | 5 min | ~70 | none |
| §1.5 collapse `decode_column_value` | 30 min | ~30 | low |
| §4.1 drop typedef wrappers | 15 min | ~20 | none |
| §1.4 numeric builder helper | 45 min | ~50 | low |
| §1.1 `scan_*_table` extraction | 1 hr | ~180 | low (callers stable) |
| §1.2 `get_neighbors*` merge | 1.5 hr | ~70 | low (19 tests) |
| §1.3 constraint helpers | 2 hr | ~80 | medium |
| §3.3/3.4 per-arm decoders | 3.5 hr | structural | low |
| §3.1 split `flush_to_l1` | 4–6 hr | structural | medium |

Top three by ROI: §2.1 (free win), §1.1 (~180 LOC removed in 1 hour), §1.2 (single source of visibility logic).
