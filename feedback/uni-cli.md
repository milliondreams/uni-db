# uni-cli Code Simplifier Review

Scope: `crates/uni-cli/src/{repl.rs, main.rs, demo.rs}`. Focus on `repl.rs` (recently modified) plus full-crate sweep for duplication, dead code, complex functions, and inconsistent patterns. No files modified.

---

## repl.rs (recently modified)

### 1. Duplicated session+query block in `execute_query`
- **Location:** `repl.rs:72-115`
- **Issue:** Three near-identical scoped blocks build a session and invoke `query` / `query_with(..).explain()` / `query_with(..).profile()`. Each has the same `Error: {}` red-print branch. The function's body is essentially three copies of "open session, call variant, match Ok/Err, print, return".
- **Suggestion:** Extract a small dispatch enum (e.g. `enum Mode { Plain, Explain, Profile }`) chosen once from the prefix check, then a single `match mode { .. }` that calls the right session method. The error branch (`println!("{}", format!("Error: {}", e).red())`) can be shared via a closure or a `print_err` helper. Removes ~30 lines, eliminates the early `return`s, and centralises error formatting.
- **Effort:** trivial.

### 2. Fragile `PROFILE` prefix stripping
- **Location:** `repl.rs:88-89`
- **Issue:** `query[7..].trim()` slices by byte offset on the *original-case* string after testing against `query_upper`. Works for ASCII `PROFILE` but is brittle: any leading whitespace before `PROFILE` (which `query_upper = query.trim_start().to_uppercase()` tolerates) would shift indices and panic or produce wrong slice. `EXPLAIN` branch sidesteps this by passing the full query, but `PROFILE` does not.
- **Suggestion:** Use `query.trim_start().get(7..).unwrap_or("").trim()` or, better, `query.trim_start().strip_prefix_ignore_ascii_case("PROFILE")` (manual two-line helper) to avoid index math entirely.
- **Effort:** trivial.

### 3. Inconsistent prefix detection between EXPLAIN / PROFILE
- **Location:** `repl.rs:76, 88`
- **Issue:** `EXPLAIN` passes the whole `query` (including the literal `EXPLAIN` keyword) down to `query_with`, while `PROFILE` strips its keyword. Either the underlying session understands `EXPLAIN ...` literally (in which case PROFILE stripping is asymmetric) or it does not (in which case EXPLAIN is wrong). Pick one convention.
- **Effort:** trivial (investigation), moderate if behavior actually differs.

### 4. JSON round-trip just to unwrap a `String`
- **Location:** `repl.rs:147-153`
- **Issue:** Each cell value is converted to `serde_json::Value`, then matched only to peel off the `String` variant before falling back to `to_string()`. The conversion allocates per cell and obscures intent.
- **Suggestion:** If `v` is `uni_db::Value` (or similar), add/inspect a `Display` or `as_str()` accessor and format directly. If serde conversion is unavoidable, at least factor the unwrap into a tiny `value_to_cell_string(v)` helper so the table-building loop reads cleanly.
- **Effort:** moderate (depends on `Value` API surface).

### 5. `table.len()` reported as "rows"
- **Location:** `repl.rs:162`
- **Issue:** `table.len()` includes the header row, so the "N rows" footer is off-by-one vs. the actual result count. Latent correctness bug exposed by the simplification opportunity.
- **Suggestion:** Track row count from `into_rows()` length (collect first or count via the iterator before consuming).
- **Effort:** trivial.

### 6. `history.txt` is a CWD-relative path
- **Location:** `repl.rs:16, 59`
- **Issue:** Not strictly a simplification, but the REPL silently writes/reads `history.txt` in whatever directory the user launched from. Inconsistent with `--path` default. A `dirs::config_dir()`-based path would be more conventional. Flag for awareness only.
- **Effort:** moderate.

### 7. `Error: {:?}` vs `Error: {}` inconsistency
- **Location:** `repl.rs:53` (debug formatter) vs `repl.rs:83, 100, 112` (display formatter, red).
- **Issue:** Readline error path uses `{:?}` and no color; query error paths use `{}` colored red. Unify on one error-print helper.
- **Effort:** trivial.

---

## main.rs

### 8. Duplicated `Uni::open(path).build().await?` boilerplate
- **Location:** `main.rs:130-131, 136-137, 141-143, 151-152`
- **Issue:** Four arms of the `match command` each repeat the same two-line builder dance. `Query`, `Repl`, `Snapshot`, `Plugin` only differ in what they do with `db`.
- **Suggestion:** Extract `async fn open_db(path: &Path) -> Result<Uni>` (or hoist the open above the match once `path` is uniformly extracted — every variant except `Import` carries a `path`). Removes ~8 lines and one inconsistency (`Plugin` arm omits the `let builder = ...` indirection used by the others).
- **Effort:** trivial.

### 9. `parse_grants` repetitive insert match
- **Location:** `main.rs:281-320`
- **Issue:** The match returns `bool` discarded by `;`. Each arm hand-constructs the `Capability` variant with hardcoded `"**"` / `"*"` wildcards. The default-grants list `vec!["ScalarFn", "AggregateFn", "Procedure"]` duplicates string literals already present as arm labels.
- **Suggestion:** Move per-name capability construction into a `fn capability_from_name(&str) -> Option<Capability>` returning `Option`, then iterate with `for_each`. Defaults become `[Capability::ScalarFn, Capability::AggregateFn, Capability::Procedure]` directly, avoiding the string-list round-trip. Drops ~15 lines and removes the throwaway `false` arm.
- **Effort:** moderate.

### 10. `install_plugin` scheme dispatch ladder
- **Location:** `main.rs:204-221`
- **Issue:** Three `if source.starts_with(...)` branches each `anyhow::bail!` with a nearly identical "not yet supported (M12)" message. The fourth (extension-based) dispatch is structurally different but conceptually part of the same "what kind of source?" decision.
- **Suggestion:** Define `enum PluginSource { Oci, Extism, Http, Rhai(PathBuf), Wasm(PathBuf), Unknown }`, write a single `classify(source) -> PluginSource`, then a single `match` produces either the bail message (with a shared `not_yet_supported(kind)` helper) or the rhai loader path. Keeps the M12 TODOs visible in one place.
- **Effort:** moderate.

### 11. `Snapshot::List` table-building duplicated with `print_results`
- **Location:** `main.rs:160-176`
- **Issue:** Snapshot table construction (header cells with `bf` spec, body cells, `printstd`) mirrors the header/body loop in `repl.rs:131-159`. Two more usages will exist soon (index usage table in `repl.rs:172-188`). All three roll their own.
- **Suggestion:** Tiny helper `fn print_table(headers: &[&str], rows: impl IntoIterator<Item = Vec<String>>)` in a shared module (e.g. `repl::table` or new `ui.rs`). Each call site shrinks to one statement and the `"bf"` style is centralised.
- **Effort:** moderate.

### 12. `let builder = Uni::open(..); let db = builder.build()...` two-step
- **Location:** `main.rs:130-131, 136-137, 151-152`
- **Issue:** `builder` is never reused; the two-step is purely stylistic. The `Plugin` arm (`main.rs:141-143`) chains correctly. Tighten the others for consistency.
- **Effort:** trivial.

### 13. `path.to_string_lossy().to_string()` repeated
- **Location:** `main.rs:130, 136, 141, 151`
- **Issue:** Four identical conversions. If `Uni::open` accepts `impl AsRef<Path>` or `Into<PathBuf>`, drop the conversion. Otherwise wrap once.
- **Effort:** trivial.

---

## demo.rs

### 14. Trivial re-export module
- **Location:** `demo.rs:1-5`
- **Issue:** A 5-line file existing only to `pub mod semantic_scholar;`. With one submodule this is pure indirection; `pub mod demo { pub mod semantic_scholar; }` could be inlined in `main.rs` or `semantic_scholar` could move up to a top-level module.
- **Suggestion:** Leave as-is *if* more demos are imminent; otherwise flatten. Flagging only.
- **Effort:** trivial.

---

## Cross-cutting observations

- **Color formatting pattern:** `format!("...", x).red()` / `.green()` / `.dimmed()` appears 10+ times across both files. A small `style` module with `fn err(s: impl Display) -> ColoredString`, `fn ok(...)`, `fn dim(...)` would tighten call sites and ensure the format-then-color order is consistent (currently `colored` is applied to an already-formatted `String`, which works but allocates twice).
- **`use colored::*;`:** Glob import; project convention (per CLAUDE.md guidance on Rust) typically favors explicit imports. Switch to `use colored::Colorize;` (the only trait needed).
- **No tests:** Neither `repl.rs` nor `main.rs` has unit tests for `parse_grants`, `install_plugin` classification, or `execute_query`'s prefix dispatch. Any refactor in items 1, 2, 9, or 10 should add a tiny test module first to lock behavior.
- **`function` keyword over closures:** Codebase guideline. Current code already uses `fn` for helpers; no violations spotted.
- **MSFT Rust guidelines (M-CANONICAL-DOCS):** Public items lack `///` rustdoc with `# Errors` / `# Panics` sections where applicable (e.g. `pub async fn run_repl`, `pub async fn execute_query`). Add canonical doc blocks during any touch.

---

## Priority order (suggested)

1. (Item 5) Fix `table.len()` off-by-one — correctness bug, trivial.
2. (Item 2) Replace byte-slice with case-insensitive `strip_prefix` — robustness, trivial.
3. (Item 1) Collapse `execute_query` three-branch duplication — biggest readability win.
4. (Items 8, 12, 13) Hoist `open_db` helper — small consistent cleanup.
5. (Item 11) Shared `print_table` helper — pays off as more tables appear.
6. (Items 9, 10) `parse_grants` and `install_plugin` enum-driven dispatch — moderate effort, scales with M12 work.
7. (Items 3, 7) Resolve EXPLAIN/PROFILE asymmetry and unify error printing — trivial once decided.
