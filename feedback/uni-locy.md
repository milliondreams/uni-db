# Code Simplifier Feedback: `crates/uni-locy/`

Scope reviewed: the entire `uni-locy` crate (~2150 LOC across `lib.rs`, `config.rs`, `errors.rs`, `result.rs`, `types.rs`, and `compiler/{mod,dependency,errors,modules,stratify,typecheck,warded}.rs`). Despite the project memory describing a large logic engine, only the compiler front-end and result types live here; the runtime/planner/walker layers are in sibling crates. Overall the crate is small, well-factored, and the code reads cleanly. Findings below are minor quality polish — no significant restructuring is warranted.

Effort legend: trivial (≤5 min), moderate (15–45 min), significant (>1 h).

---

## 1. Public API surface duplication

**File:** `crates/uni-locy/src/compiler/mod.rs:21-73`

There are four nearly identical public entry points: `compile`, `compile_with_external_rules`, `compile_with_modules`, `compile_with_oracle`. Each one is a thin shim that fills in defaults and calls `compile_with_context`. `compile_with_oracle` is in fact a verbatim wrapper for `compile_with_context` (same arg order, same body).

- Suggestion: keep `compile` as the convenience entry, expose `compile_with_context` (renamed `compile_with_options` or accepting a small builder/options struct) as the single full-control entry. Either remove the three intermediate wrappers or keep them as one-line aliases consolidated into a single inline `pub use` / `pub fn = ...` block at the bottom of the file.
- Alternative: introduce a `CompileOptions` struct (`available_modules`, `external_rules`, `oracle`) with `Default`; callers then write `compile_with(prog, CompileOptions { external_rules, .. Default::default() })`. This avoids future combinatorial explosion if more knobs are added.
- Effort: moderate (touches downstream callers in other crates).

## 2. `group_rules` vs `group_rules_with_context`

**File:** `crates/uni-locy/src/compiler/mod.rs:217-242`

The two grouping functions differ only in whether `modules::resolve_rule_name` is applied. They can collapse into one function that always takes a `&ModuleContext` (an empty default context is a no-op for resolution, since `resolve_rule_name` already handles both branches). The call site at line 181 currently constructs `ModuleContext::default()` explicitly, which is exactly the right pattern to use everywhere.

- Suggestion: delete `group_rules`, route the one call at line 181 through `group_rules_with_context` with a default context.
- Effort: trivial.

## 3. `extract_commands` ASSUME branch duplicates pipeline work

**File:** `crates/uni-locy/src/compiler/mod.rs:173-207`

The ASSUME body branch (a) re-runs `group_rules` ignoring the outer module context, (b) re-derives `all_rule_names` to call `extract_commands` recursively, and (c) separately invokes `compile` on the body program, which itself re-groups. This means rule grouping runs twice for every ASSUME body, and module context is silently dropped for nested bodies.

- Suggestion: factor a single `compile_inner(program, module_ctx, external_rules, oracle)` that returns both the compiled program and the commands list, then call it recursively for ASSUME bodies, threading the parent `module_ctx` through (or documenting why it must be reset).
- Also consider whether the empty-rules path on lines 191-200 (constructing a fully empty `CompiledProgram`) can be replaced by just calling `compile` unconditionally — `compile` already handles the empty case at line 88.
- Effort: moderate.

## 4. Tarjan's SCC implementation — recursion + manual state struct

**File:** `crates/uni-locy/src/compiler/stratify.rs:138-204`

The hand-rolled Tarjan implementation uses recursion (stack-overflow risk for pathological programs) and a custom `State` struct with manual `index`/`lowlink` hashmaps. The crate already depends on graph utilities elsewhere in the workspace (the broader `uni` tree has `petgraph`-based code paths).

- Suggestion: replace with `petgraph::algo::tarjan_scc` or `kosaraju_scc`. This eliminates ~70 lines, removes the recursion-depth risk, and makes the topological sort that follows trivially expressible via `petgraph::algo::toposort` over the condensation. If adding a dependency is undesirable, at minimum convert `strongconnect` to an explicit work-stack loop — large rule graphs can blow the call stack.
- The current implementation also has minor inefficiencies: `state.lowlink[v]` is looked up via `HashMap` indexing repeatedly inside the inner loop (line 159-169). A small local mutable variable would read better.
- Effort: moderate (if reusing petgraph), significant (if rewriting to iterative).

## 5. `is_recursive` computation has a redundant branch

**File:** `crates/uni-locy/src/compiler/stratify.rs:67-80`

`is_recursive[i] = false` is assigned by `vec![false; ...]`, then `if scc.len() > 1 { is_recursive[i] = true; } else { ... has_self_edge ... }`. The "size > 1 ⇒ self-loop in condensation" property already implies recursion. The expression simplifies to: `is_recursive[i] = scc.len() > 1 || has_self_edge(rule)`. Inlining the lookup makes the intent obvious.

- Suggestion: collapse into a single boolean expression per SCC; bonus: drop `mut` on the vector.
- Effort: trivial.

## 6. `scc_depends_on` builds in-degree + reverse adjacency in a second pass

**File:** `crates/uni-locy/src/compiler/stratify.rs:82-125`

The condensation DAG is built once (lines 82-96), then in-degrees + reverse adjacency are computed in a second pass (lines 99-107) just to run Kahn's. Since the graph is already known to be a DAG (SCCs collapsed), a simpler iterative topo sort directly over `scc_depends_on` (or DFS post-order on the condensation) avoids the temporary `reverse_deps` vector. Even simpler: SCCs from Tarjan are emitted in reverse-topological order, so `scc_order = (0..sccs.len()).rev().collect()` is correct without Kahn's at all — but that requires verifying Tarjan's order semantics in your implementation.

- Suggestion: if you keep the current Tarjan, leave it; if you migrate to `petgraph`, the topo-sort comes for free.
- Effort: trivial-to-moderate.

## 7. Repeated `extract_function_name` + `to_uppercase()` pattern

**File:** `crates/uni-locy/src/compiler/typecheck.rs:60-70, 332-348, 352-374, 378-405, 409-428`

Five separate fold-iteration loops each do: extract function name, uppercase it, match on aggregate names, peek at `args.first()` literal-ness. The boilerplate is moderate but the patterns differ enough that a single helper function would be awkward. A lighter refinement:

- Cache `to_uppercase()` once per fold (currently called repeatedly: lines 64, 339, 355, 385, 397, 419).
- Introduce a small helper `fn fold_function(fold: &FoldBinding) -> Option<(String /* upper */, &[Expr] /* args */)>` returning `None` when the aggregate is not a function call, and use it across the five sites. This collapses the `if let Expr::FunctionCall { args, .. } = &fold.aggregate` block currently duplicated between `check_msum_warning` (line 356) and `check_probability_domain_warning` (line 386).
- The two warning functions (`check_msum_warning` and `check_probability_domain_warning`) are nearly identical except for the target name set and warning code; consider one helper `emit_arg_domain_warning(rule, def, target_funcs, code, message_fn, warnings)`.
- Effort: moderate.

## 8. Implicit-PROB pass duplicates aggregate inspection

**File:** `crates/uni-locy/src/compiler/typecheck.rs:60-70`

The implicit-PROB pass at lines 60-70 walks all definitions/folds, but the same loop body later (line 84-122) walks each definition for monotonicity and warning checks. Two passes over the same fold list are unnecessary — the implicit-PROB marking can move into the per-definition loop.

- Suggestion: fold the prob-marking into the main `for def in definitions` loop; emit `MultipleProbColumns` after the loop. Reduces one full pass over folds.
- Effort: trivial.

## 9. Second-pass IS-arity + prev validation lives outside the main loop

**File:** `crates/uni-locy/src/compiler/typecheck.rs:137-203`

The second pass exists because it depends on `yield_schema` for the target rule, which may not yet be inferred when the source rule is processed. The comment on line 137 explains this. That said:

- `scc_idx`/`scc_rules` are recomputed per rule even though they were known in the first pass.
- The available-columns set is built by scanning every clause of every IS-target rule; this can be cached once per target rule outside the inner loop.
- The Vec → sort → join on line 190-192 only runs in the error path; that is fine.
- Suggestion: lift the per-rule `(scc_idx, scc_rules)` lookup outside the clause loop; consider caching `available_cols_for(rule_name)` as a `HashMap<String, HashSet<String>>` built once at the start of the second pass.
- Effort: moderate.

## 10. `find_prev_ref` / `collect_prev_refs` are near-duplicates

**File:** `crates/uni-locy/src/compiler/typecheck.rs:306-328`

`find_prev_ref` (returns first match) and `collect_prev_refs` (returns all) share an identical recursive shape over `LocyExpr`. A single iterator-returning helper, or one `walk_prev_refs(expr, &mut FnMut(&str))` visitor, removes the duplication. Given `LocyExpr` is likely to grow new variants, the duplicated match arms are a maintenance hazard.

- Suggestion: add a single visitor `fn for_each_prev_ref(expr: &LocyExpr, f: &mut impl FnMut(&str))` and implement both call sites in terms of it.
- Effort: trivial.

## 11. `yield_columns_from_items` / `expr_name` only handles three Expr variants

**File:** `crates/uni-locy/src/compiler/typecheck.rs:284-290`

`expr_name` falls back to `"?"` for every Expr variant other than `Variable` and `Property`. Anonymous columns named `"?"` will collide. If the grammar already requires aliases for arbitrary expressions this is fine; otherwise it is silently wrong. Worth at least leaving a `// TODO:` or asserting that the parser guarantees alias presence for compound expressions.

- Suggestion: change the fallback to a generated unique name (`format!("_col{}", idx)`) or return `Option<String>` and force callers to supply an alias.
- Effort: trivial (if just documenting); moderate (if changing semantics).

## 12. `default_monotonicity_oracle` magic string list

**File:** `crates/uni-locy/src/compiler/typecheck.rs:27-32`

The list of "M-prefix monotone aggregates" is hardcoded here and re-checked in plain literals elsewhere (e.g., `"MNOR" | "MPROD"` at lines 64, 385). The `M`-prefix convention is described in comments but never enforced by code.

- Suggestion: centralize the set as `pub const MONOTONE_AGGREGATES: &[&str] = &["MMAX", ...];` plus a `is_probability_aggregate(name)` helper for the `MNOR | MPROD` subset. Use them at the four sites that currently hardcode literal matches.
- Effort: trivial.

## 13. `CommandResult` accessors are partial

**File:** `crates/uni-locy/src/result.rs:197-221`

`CommandResult` has six variants but only three `as_*` accessors (`as_explain`, `as_query`, `as_abduce`). The omissions (`as_assume`, `as_derive`, `as_cypher`) are either dead code or callers reach into the enum directly elsewhere — both signs of an inconsistent pattern. Either add the missing accessors symmetrically or drop the existing ones and let callers use exhaustive `match`.

- Suggestion: prefer dropping the partial accessors; an exhaustive `match` at call sites makes new variant additions a compile error rather than a silent miss.
- Effort: trivial-to-moderate (depends on number of call sites in dependent crates).

## 14. `columns()` returns nondeterministic column order

**File:** `crates/uni-locy/src/result.rs:171-174`

`columns()` does `row.keys().cloned().collect()` on a `HashMap` — order is not deterministic across runs. Any caller relying on this for display, CSV export, or schema introspection will see flaky output.

- Suggestion: either store rows as `IndexMap` / `Vec<(String, Value)>` to preserve insertion order, or look up the column order from the corresponding `CompiledRule.yield_schema` and project in that order. Document the chosen invariant.
- Effort: moderate (touches dependent crates).

## 15. `RuntimeWarning.variable_count` / `key_group` semantics tied to one variant

**File:** `crates/uni-locy/src/types.rs:107-119`

`RuntimeWarning` carries two optional fields that are documented as "BddLimitExceeded only". This is a classic stringly-typed enum smell — the per-code payload belongs in the `RuntimeWarningCode` enum itself (e.g., `BddLimitExceeded { variable_count: usize, key_group: String }`).

- Suggestion: move payload into the variant. This makes invalid combinations (e.g., `SharedProbabilisticDependency` with a `variable_count`) unrepresentable. Loops elsewhere that filter by code (e.g., `has_warning`) keep working.
- Effort: moderate (touches construction sites in runtime crates).

## 16. `CompilerWarning` vs `RuntimeWarning` parallel structures

**Files:** `crates/uni-locy/src/types.rs:78-119`

`CompilerWarning` (compile-time) and `RuntimeWarning` (runtime) have nearly identical shapes (code, message, rule_name) but no shared trait. If diagnostic surfacing (CLI, telemetry) ever needs to treat them uniformly, a shared `Diagnostic` trait or struct would help. This is speculative — only act if the duplication grows.

- Effort: moderate (only if needed).

## 17. Empty `paths` and patterns iteration in `extract_match_variables`

**File:** `crates/uni-locy/src/compiler/warded.rs:25-48`

Idiomatic: this loop is correct but verbose for what amounts to "collect all named variables in the pattern". If `Pattern` exposes a `variables()` iterator (or could), this 20-line nested match collapses to `def.match_pattern.variables().collect()`. Worth checking if `uni_cypher::ast::Pattern` already provides such a helper; if so, use it.

- Effort: trivial (if helper exists).

## 18. `LocyError::ExecutorError` / `EvaluationError` / `TypeError` / `AbductionError` — stringly typed

**File:** `crates/uni-locy/src/errors.rs:24-41`

Six variants are just `{ message: String }`. This is acceptable for top-level error reporting but indicates the executor/evaluator/typecheck/abduction submodules don't yet model their errors structurally. Not actionable here, but worth a `// TODO:` for whoever owns those modules.

- Effort: significant (across crates).

## 19. Tests in `compiler/mod.rs` (lines 244-680)

The 440-line `#[cfg(test)]` block at the bottom of `compiler/mod.rs` is fine in scope but mixes step-by-step unit tests for stratification, typecheck, and command extraction. If the file grows further, splitting into `compiler/tests/` per-concern files would improve discoverability. The numbered "Step N" comments suggest a tutorial-style progression that could be preserved in module-level doc comments rather than test comments.

- Effort: trivial.

## 20. Minor: `is_some_and` consistency

The crate mixes `.is_some_and(|x| ...)` (lines 358, 388) with `if let Some(...) = ... && ...` (lines 63-67, 339-340, 354-356, 384-386, 418-419). Both are idiomatic on recent Rust; uniform use of one style improves grep-ability. Given the let-chain pattern reads better for the multi-condition cases, I'd standardize on that.

- Effort: trivial.

---

## Summary of top recommendations (rough ROI order)

1. (#1) Collapse the four public `compile_*` entry points into one + `CompileOptions` — biggest API-clarity win.
2. (#7) Centralize `extract_function_name + to_uppercase + literal_check` into one helper used by all five fold-iteration loops.
3. (#10) Merge `find_prev_ref` / `collect_prev_refs` into a single visitor.
4. (#4) Replace hand-rolled recursive Tarjan with `petgraph` or convert to iterative — eliminates a stack-overflow footgun.
5. (#15) Move per-code payloads into `RuntimeWarningCode` variants.
6. (#14) Make `LocyResult::columns()` return deterministic order.

Nothing in the crate looked dead, and the layering (dependency → stratify → warded → typecheck → assemble) is clean. The biggest readability wins are deduplication inside `typecheck.rs` and consolidating the public compile API.
