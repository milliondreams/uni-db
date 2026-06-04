# Code Simplifier Feedback: `uni-plugin-conformance`

Scope: `crates/uni-plugin-conformance/src/lib.rs` (439 lines, single-file crate).

---

## 1. Duplicated registry setup in `registration.commit` and `registration.idempotent` probes

- **Location:** `crates/uni-plugin-conformance/src/lib.rs:207-249`
- **Description:** Both probe closures repeat the same pattern: build `PluginRegistry::new()`, clone `manifest.capabilities`, construct `PluginRegistrar`, call `plugin.register(&mut r)`, then `r.commit_to_registry()`. The `idempotent` probe runs this twice with only the error-prefix string changing per call.
- **Suggestion:** Extract a small helper inside `run_against_plugin` (or a free `fn register_once(plugin, &registry, &caps, id, label) -> Result<(), String>`) that performs `new PluginRegistrar -> register -> commit` and prefixes errors with `label`. The two probes become 2-3 lines each, and the `idempotent` probe reads as `register_once("first") -> remove_plugin -> register_once("re")`.
- **Effort:** ~10 minutes. Low risk - pure local refactor inside one function.

---

## 2. Duplicated `Plugin` trait impl boilerplate across three test fakes

- **Location:** `crates/uni-plugin-conformance/src/lib.rs:301-345` (`GoodPlugin`), `367-385` (`BadIdPlugin`), `407-427` (`BuiltinLikePlugin`)
- **Description:** All three test plugins have an identical shape: a `OnceLock<PluginManifest>` field, a `manifest()` impl that calls `get_or_init` with a closure that mutates a clone of `GoodPlugin::manifest_value()`, and an empty `register()` that returns `Ok(())`. `BuiltinLikePlugin` additionally re-implements `Debug` by hand because it is declared inside a `#[test]` fn (so `#[derive]` was avoided).
- **Suggestion:** Introduce a single test helper in `mod tests`:
  ```text
  fn make_fake_plugin(mutate: impl FnOnce(&mut PluginManifest)) -> impl Plugin + Debug
  ```
  backed by one generic `FakePlugin<F>` struct (or a `Box<dyn Fn>` field with a manual `Debug`). Each test then becomes a one-liner that supplies only the mutation it cares about (`|m| m.id = PluginId::new("no-dot")`, etc.). Removes ~60 lines of boilerplate and the hand-rolled `Debug` impl.
- **Effort:** ~20 minutes.

---

## 3. Repeated `check(...)` invocation pattern with closures returning `Result<(), String>`

- **Location:** `crates/uni-plugin-conformance/src/lib.rs:151-249`
- **Description:** Six consecutive `checks.push(check(id, name, || { ... }))` calls. Idiomatic, but the `id`/`name` pairs and ordering are duplicated again in the test at lines 355-364 as a string array - two sources of truth for the probe inventory.
- **Suggestion:** Define a `const PROBE_IDS: &[&str] = &["manifest.parse", ...]` (or a small `&[(id, name)]` table), drive both `run_against_plugin` ordering assertions and test coverage assertions from it. Prevents drift if a probe is added/renamed. Alternatively, keep imperative pushes but expose a `pub fn probe_ids() -> &'static [&'static str]` so downstream CI can pin without hard-coding.
- **Effort:** ~15 minutes.

---

## 4. Doc-listed probes (1-6) duplicate the implementation list

- **Location:** `crates/uni-plugin-conformance/src/lib.rs:132-145` (rustdoc on `run_against_plugin`) vs. `151-249` (actual probes)
- **Description:** The doc comment enumerates probes 1-6 in prose. Any change to the probe set requires editing both places; the test at `355-364` is a third copy.
- **Suggestion:** Once #3 is applied, the rustdoc can simply reference the constant table (`see [PROBE_IDS]`) instead of restating names. Cuts maintenance surface from three sites to one.
- **Effort:** ~5 minutes (after #3).

---

## 5. Stale scaffolding text in module rustdoc

- **Location:** `crates/uni-plugin-conformance/src/lib.rs:17-22`
- **Description:** The "Crate status" section still says "M12 scaffolding ... actual probe suite ... lands during M12", but six real probes are now implemented (lines 151-249) and the comment on line 128 already calls itself "M12 substantive". This is mildly misleading dead documentation rather than dead code.
- **Suggestion:** Update or delete the "Crate status" paragraph; or move it under a `# Status` heading describing what is *still* outstanding (e.g., schema/determinism/error-model/memory-cap probes from the bullet list at lines 8-15, which are advertised in the top doc but not yet implemented).
- **Effort:** ~5 minutes.

---

## 6. `run_against` WASM branch is effectively a stub returning a guaranteed failure

- **Location:** `crates/uni-plugin-conformance/src/lib.rs:114-124`
- **Description:** `ConformanceTarget::WasmPath` always yields a single failing `CheckResult` with the message "not yet wired". The path arg is only used for its `display()` in the error string; no validation (existence, extension) is performed. This is exercised by the `wasm_target_returns_not_yet_wired` test (line 433), which mainly pins the stub identifier.
- **Suggestion:** Either (a) at minimum, `Path::exists()` check + `.wasm` extension check so the error gives an actionable signal beyond "M6a/M6b SDK integration", or (b) gate the WASM branch behind a `wasm` feature so the public API doesn't ship a known-failing probe. Option (a) is cheaper and preserves the deferred-implementation contract.
- **Effort:** ~10 minutes.

---

## 7. `capabilities.declared` probe is a no-op

- **Location:** `crates/uni-plugin-conformance/src/lib.rs:198-205`
- **Description:** The probe body is `let _ = &manifest.capabilities; Ok(())` - it cannot fail. Field access on a `&PluginManifest` returned by reference does not panic; there is nothing to assert. Functionally dead.
- **Suggestion:** Either remove the probe (and drop it from the doc and test inventory) or strengthen it to assert something real - e.g., that the declared capabilities are internally consistent (no conflicting flags), or that the set is non-empty for non-pure determinism, or that the capabilities round-trip through serde. As-is it inflates the passing-check count without adding signal.
- **Effort:** ~15 minutes to give it real teeth; ~2 minutes to delete.

---

## 8. `manifest.parse` over-trusts type system

- **Location:** `crates/uni-plugin-conformance/src/lib.rs:151-163`
- **Description:** `manifest.version.to_string().is_empty()` cannot return true for a `semver::Version` - even `Version::new(0,0,0)` stringifies to `"0.0.0"`. The check is structurally dead. The id-empty branch is meaningful.
- **Suggestion:** Drop the version branch, or replace it with a real invariant such as `version >= Version::new(0,1,0)` or "pre-release tag is well-formed". Update the probe name accordingly.
- **Effort:** ~5 minutes.

---

## 9. `assert_pass` formatting via `format!` + `push_str` in a loop

- **Location:** `crates/uni-plugin-conformance/src/lib.rs:84-94`
- **Description:** Builds the panic message by allocating a temporary `String` per failure and pushing. Minor; readable. Mentioned only for completeness.
- **Suggestion:** Use `writeln!(&mut msg, "  - {} [{}]: {}", ...)` to avoid the intermediate allocation. Cosmetic.
- **Effort:** ~2 minutes. Skip unless touching this function for another reason.

---

## 10. Reserved-id list is a magic literal

- **Location:** `crates/uni-plugin-conformance/src/lib.rs:174`
- **Description:** `matches!(id, "builtin" | "apoc-core" | "custom" | "user.legacy")` hard-codes the reserved set. If `uni-plugin` adds another reserved id, this list silently goes stale and a perfectly valid plugin starts failing conformance. No cross-reference in `uni-plugin` exists.
- **Suggestion:** Expose the reserved-id list from `uni-plugin` (e.g., `pub const RESERVED_PLUGIN_IDS: &[&str]`) and consume it here. Single source of truth across the workspace.
- **Effort:** ~15 minutes (small cross-crate touch).

---

## Summary table

| # | Theme | Lines | Effort |
|---|---|---|---|
| 1 | Registry setup dup | 207-249 | 10 min |
| 2 | Test fake boilerplate | 301-427 | 20 min |
| 3 | Probe inventory duplicated | 151-249 + 355-364 | 15 min |
| 4 | Doc list duplication | 132-145 | 5 min (after 3) |
| 5 | Stale "scaffolding" rustdoc | 17-22 | 5 min |
| 6 | WASM stub branch | 114-124 | 10 min |
| 7 | No-op capabilities probe | 198-205 | 2-15 min |
| 8 | Dead version-string check | 151-163 | 5 min |
| 9 | `assert_pass` formatting | 84-94 | 2 min |
| 10 | Magic reserved-id list | 174 | 15 min |

Aggregate: roughly 1.5 hours of net work, of which #1, #2, #3, #7, #8 yield the largest readability and signal improvements. None of the suggestions change observable behavior except #7 and #8, which intentionally strengthen probes that currently always pass.
