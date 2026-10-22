# Locy IS-reference binding: catch and close the #300 class

Status: implemented (Phase F, F1–F7, and T1), 2026-10-16; uncommitted. The oracle and
generator items T2–T8 are tracked separately and are not covered by §8. Decisions 1–3 of §7
were taken as recommended. Measured outcomes are in §8; §1–§7 are the proposal as written.

## 1. What is measured

Issue #300: `MATCH (o:E) WHERE o IS seed AND (o, e, pct) IS stake YIELD KEY e, pct`
returns 0 rows; with the two conjuncts swapped it returns the right answer.
A probe (`crates/uni/tests/common/locy/locy_issue_300_probe.rs`) ran 15 cases
plus controls in every conjunct order, before and after a flush. Results do not
depend on the flush.

| Case | Shape | Result |
|---|---|---|
| P1 | #300 | 0 rows in one order |
| P1c | #300, recursive | `{x}` vs `{x,a,b}` |
| P2 | later ref's value column named like a MATCH variable | that variable is replaced (`b = null`) |
| P3 | `(e, o) IS stake` (names permuted vs rule's KEY names) | 0 rows in every order |
| P4 | KEY subject named like an earlier ref's value column | 0 rows / loud, by order |
| Q1-A | `a IS tag TO col`, tag's KEY a scalar | 0 rows (String); **unrelated nodes** when an Int equals a VID |
| Q1-B | `(a, col) IS tag`, `col` on a KEY position | `No field named col`, node KEYs too |
| Q2 | ALONG `prev.cost` beside a ref that yields `cost` | 101 instead of 11, by order |
| Q3 | two refs yield `score`, WHERE reads it | first wins silently; reversed order gives 0 rows |
| P1b, P7 | unbound name with no colliding column | loud `No field named`, by order |
| P5 | `NOT b IS r` before the ref that binds `b` | compile error in one order only |
| P6, P8, Cypher Q4–Q6 | deferred WHERE, EXPLAIN vs evaluation, Cypher candidates | correct (EXPLAIN agrees with evaluation, but in P1 both are wrong) |

## 2. The class, in three axes

1. **Name resolution (N).** The planner has no single binding environment. It
   keeps three partial ones (`clause_node_vars`, `scalar_bindings`,
   `exposed_before`). The first positive IS ref's columns, KEY columns included,
   are joined bare; later refs are prefixed `__isref{n}_`. A name none of the
   three knows is emitted as a bare `Expr::Variable` and DataFusion resolves it
   by column name, so it captures whichever bare column exists. KEY-position
   subjects are always *tested* (`build_is_ref_predicate`), never bound.
   Collision → silent wrong answer; no collision → loud error; which one you get
   depends on conjunct order. P1, P1c, P2, P3, P4, Q1-B, Q2, Q3, P1b, P7.
2. **Kind (K).** `is_ref_binds_node` (`locy_planner.rs:122`) returns `true` for
   every KEY column: "a KEY holds a VID". A projected scalar KEY
   (`KEY t.name AS c`) is then joined against `ScanAll._vid`. This is #272's
   failure on the KEY side. Q1-A.
3. **Disagreement (D).** Four components decide "is this name bound" on their own:
   - compiler scope (an unordered set; every subject counts as bound);
   - compiler typecheck (left to right);
   - the planner (ordered; KEY positions are tests);
   - EXPLAIN / `locy_delta` (binds anything).

   The compiler accepts programs the planner then plans wrongly. No binding
   decision travels in `CompiledClause`. P5.

The test suites cannot reach any of it. Every generated program has:
- a fixed conjunct order and fixed variable names;
- at most one positive IS ref per clause;
- IS subjects bound only by MATCH;
- reused column names that never meet in one clause;
- integer values that never coincide with VIDs.

The oracle panics on a subject the ref itself binds. No relation permutes
conjuncts or renames anything.

## 3. Phase T: tests that catch the class (first, and red before any fix)

Each item lists its *teeth check*: it must fail on current `main` (52e7a4d70).

**T1. Regression suite from the probes.**
- Turn the probe into `locy_is_ref_name_resolution.rs`, registered in `locy/mod.rs`, with asserted answers.
- Run each case in every conjunct order, flushed and unflushed.
- Cover every row of the table in §1, plus a recursive case checked against a hand-computed fixpoint.
- Teeth: P1, P1c, P2, P3, P4, Q1-A, Q1-B and Q2 fail. Q3 fails once the ambiguity error (F5) is specified.

**T2. Oracle generator: a Locy IR → text renderer.** Program text is currently
hand-written beside the IR. Rendering the text from the IR is what lets the
generator vary the text freely. Generator extensions, each with a tally
(see T7):
- clauses with 2–3 positive IS refs;
- subjects bound by a ref, at every position (KEY and value, unary, tuple, `TO`);
- *capture-seeking names*:
  - body variables equal to a referenced rule's KEY or value column names;
  - the same value column name yielded by two referenced rules;
  - `prev.x` beside a ref that yields `x`;
- scalar KEYs (String, Int) with **Int values deliberately equal to existing VIDs**;
- `IS NOT` placed before and after its binder.

**T3. Oracle evaluation: a unifying natural join.**
- An unbound subject binds, a bound one tests, and a repeated variable must agree with itself.
- The join must not depend on order.
- `IS NOT` keeps the safety rule from F1.
- Without this the oracle cannot judge the programs T2 now produces.

**T4. Metamorphic relations that change only the program text** (no oracle needed). Add them to
`uni-locy-oracle/tests/differential.rs` next to `interleave_yield`, and to
`metamorphic/dqp/topo.rs` next to `LocyBinding`:
- `permute_conjuncts`: every permutation of the WHERE conditions (up to 6), compared as bags.
- `alpha_rename`: a consistent renaming of body variables, YIELD aliases, rule names and rule output columns. Answers must agree once column labels are mapped through the renaming.
- `rename_into_collision`: rename a body variable to a name already used as a column by *another* referenced rule. This is the relation that hunts capture directly; a uniformly fresh renaming would hide it.
- `explain_agrees`: the facts with an EXPLAIN derivation equal the evaluated facts, so D cannot recur silently.

**T5. Cypher guard.**
- Add `permute_conjuncts` and `alpha_rename` over `querygen/render.rs`. Its AST renderer and its round-trip property test already exist.
- Survey sites with the same shape, decided by schema name lookup:
  - `df_planner.rs:2737` `detect_bound_target`;
  - `expr_compiler.rs` bound-target / correlated checks;
  - ORDER BY alias `df_planner.rs:6062`.
- Probes Q4–Q6 were correct, so this is a guard, not a fix.

**T6. Kind fixture.**
- Every Locy and Cypher fixture with integer properties also gets values that equal live VIDs.
- Any VID-vs-scalar confusion then returns rows instead of hiding behind "0 rows".

**T7. Coverage tallies (anti-vacuity).**
- `drive_random_programs` already counts non-empty relations. Add counters for:
  - clauses with ≥2 positive refs;
  - ref-bound subjects;
  - name collisions;
  - scalar KEYs.
- Each needs a floor (e.g. ≥15% of cases), or the run fails.
- A relation that never sees the shape it targets passes for free.

**T8. Lanes.**
- PR: fixed-seed runs inside the existing `rust-tests` and `metamorphic-smoke` budgets.
- Nightly: the oracle soak (10k) and `topo_soak` pick up the new relations automatically.

**Exit criterion for T:** the generator, *unseeded*, re-finds P1, Q1-A and Q2
on current `main` within the PR case budget. If it cannot, the generator is
still blind, and fixing the bugs first would leave the class unguarded.

## 4. Phase F: one binding plan (fix the class, not the cases)

**F1. `ClauseBindingPlan` in `uni-locy`** (`src/binding_plan.rs`), stored on
`CompiledClause`.
- **When it is computed:** in typecheck pass 2, where referenced yield schemas are complete.
- **Structure:**
  - an evaluation order of steps (ref / generator / filter / anti-join);
  - per IS ref: a prefix (for **every** ref, the first included), a logical-to-physical column map, and one action per position: `BindNode`, `BindScalar`, `TestNode`, `TestScalar`, `Existential`;
  - a scope `var → source`;
  - an exposure map (value columns only, `Unique` | `Ambiguous`).
- **Order-independent** (the language guide already says "an unbound variable is bound; a bound one is tested", with no order):
  1. bound set = MATCH ∪ every positive-ref position ∪ generator outputs;
  2. one binder per variable, preferring refs anchored on a bound KEY;
  3. every other occurrence is a test;
  4. generators are scheduled by dependency (a cycle is a compile error);
  5. filters go after their last producer;
  6. negations go last.
- **`IS NOT` safety:** every KEY-position variable of a negated ref must be bound by a positive literal or a generator *anywhere* in the body. Unbound value positions stay existential.
- **Kind:** each position records node vs scalar from the referenced rule's *yield expression*, never from "is it a KEY". This fixes K.

**F2. The planner consumes only the plan.**
- Step 3 builds joins from the position actions.
- `build_is_ref_predicate` handles only `Test*`.
- `is_ref_binds_node` is deleted.
- The DataFusion name-lookup fallback is removed. A name the plan does not source is an internal error that names the variable, never a bare `Expr::Variable`.

**F3. Every other consumer moves to the plan** (map in §6):
- deferred-WHERE split;
- generators (Step 3.4);
- deferred WHERE (3.5);
- FOLD / YIELD / hidden YIELD;
- **BEST BY**: currently not rewritten at all;
- ALONG `prev` and `validate_prev_refs`;
- `locy_delta` / EXPLAIN (replaces `SubjectMode` and first/last-wins);
- the `IS NOT` anti-join in `locy_program.rs` / `locy_complement.rs` (`Existential`, instead of a runtime `index_of`);
- the `locy_fixpoint` feature resolvers (four sites with opposite fallback orders);
- `df_planner` YIELD resolution;
- `entity_columns.rs:76`.

**F4. Compiler agreement.**
- `scope.rs` and both `typecheck.rs` IS NOT checks derive their answers from the plan.
- After this there is one binding rule instead of four.

**F5. Ambiguity.**
- Reading a name that two refs expose (`Exposure::Ambiguous`) is a compile error, `AmbiguousRuleColumn`, with the hint "bind it positionally: `(o, e, p1) IS stake`".
- Exposing a duplicate name without reading it stays legal.
- This replaces the documented "first wins" contract.

**F6. Structural guard.**
- In debug builds, and in the planner's own tests, check every emitted expression: each column reference must resolve to a physical column the plan produced.
- Future planner code that builds a bare name then fails in CI instead of capturing.
- This is the class guard on the code side, the counterpart of T4 on the test side.

**F7. Docs.**
- `website/docs` Locy language guide:
  - binding is order-independent;
  - `IS NOT` safety;
  - the ambiguity error replaces "first wins";
  - scalar KEY targets.
- `docs/UNI_BLACK_BOOK.md`: the binding plan as the single source of truth.

## 5. Sequencing

1. T1 + T3 + T2 (generator + renderer), then T4. Confirm the exit criterion on current `main`.
2. F1 + F4: the plan, and the compiler consuming it. The planner is unchanged, so behavior is unchanged at this step.
3. F2 + F3: switch every consumer over. The suite from step 1 goes green.
4. F5 + F6 + F7.
5. T5 + T6 Cypher guard, then a full local CI run per `docs/local_ci_runbook.md`.

Estimated churn (heuristic scan):
- About 25 hand-written `CompiledClause` literals plus `prune.rs:208` need a plan helper.
- 7 first-wins collision sites in 3 files: `CrossGroupCorrelation.feature`, `locy_is_ref_binding.rs`, `locy_nonlinear_recursion.rs`.
- Plus any program that reads a first ref's KEY column under a name nothing binds.

The bare read of a referenced rule's *value* column is documented. Unique names
stay exposed bare, so the ~199 such clauses do not change.

## 6. Consumer map (from code reading; verify while implementing)

| Site | Decides today by | Order-dependent |
|---|---|---|
| `uni-locy/.../scope.rs` ~115–140 | union of all names | no |
| `typecheck.rs` ~308–352, 916–951 | running bound set, earlier `TO` only | yes |
| `locy_planner.rs` 1952–1990 deferred split | targets + non-KEY value columns | no |
| `locy_planner.rs` 2280–2400, 3094–3140 Step 3 | three partial envs + DataFusion name lookup | yes |
| `locy_planner.rs:122` `is_ref_binds_node` | "KEY ⇒ VID" | — (kind) |
| `locy_planner.rs` ~2851 BEST BY | not rewritten | — |
| `locy_planner.rs` ~3148 / 3347 ALONG `prev` | bare name, union validation | yes |
| `df_graph/locy_delta.rs` 110–207 | binds anything; first-wins / last-wins | — |
| `locy_program.rs` 1734–1766, `locy_complement.rs` ~170 | `subject_vid_cols`, else runtime `index_of` | yes |
| `locy_fixpoint.rs` 4005 / 4153 / 4233 / 4690 | bare-then-`_vid` vs `_vid`-then-bare | — |
| `df_planner.rs` 5260–5300 YIELD | `_eid` → `_vid` → bare column | — |

## 7. Decisions needed

1. **Order-independent binding** (recommended; the guide already promises it).
   - The alternative is to *reject* any order the planner cannot plan left to right.
   - That is simpler, but it breaks valid programs and contradicts the docs.
2. **Ambiguous reads become a compile error** (recommended).
   - This is a breaking change to the documented first-wins rule; 7 known sites are affected.
3. **`NOT x IS r` before x's binder becomes legal** (recommended).
4. **PR #299 ships `374be77f2`, which made these tuple forms plannable** (wrongly). Two options:
   - land this work on top of the PR before merge;
   - merge the PR as is and fix after.

## 8. Outcome (measured)

**What landed.**
- **F1** `uni_locy::binding_plan::ClauseBindingPlan`, attached to every `CompiledClause` whose
  referenced rules the compiler knows (`ClauseBinding::{Pending, Ready}`), recomputed by
  `attach_binding_plans` after registered rules are merged and for ASSUME bodies.
- **F2/F3** The planner, EXPLAIN (`locy_delta`), the anti-join, the feature resolvers, YIELD
  resolution and `entity_columns` read the plan. Every positive reference is prefixed.
- **F4** `compiler/scope.rs` takes its rule-body scope from the plan (plan scope + `Unique`
  exposures; YIELD aliases excluded). Both order-dependent `typecheck.rs` IS NOT checks are
  deleted. Their decisions are now plan diagnostics: `IsNotSubjectNotANode`, which keeps the
  literal `IS NOT subject 'x'` message and the YIELD-alias-of-a-node exception, and
  `IsNotBindsValueColumn`. All plan diagnostics are raised: in typecheck right after the plans
  are attached, and by `attach_binding_plans`. The planner's own IS NOT value check is now a
  `debug_assert!`.
- **One reporter for unbound names.** The plan used to raise its own `UnboundVariable` for WHERE
  filters and generator arguments. That duplicated `scope.rs` and, raised first, preempted its
  sharper `PrevOutsideAlong` (`locy_scope_check::scope_check_rejects_what_cannot_be_evaluated`).
  The plan no longer raises one. `scope.rs` reports every unbound name and gained a "generator
  arguments" slot, the one place only the plan used to check. The plan keeps the binding
  decision; the scope check reports what is not bound. Plan diagnostics are raised before the
  scope check, so an ambiguous read reports `AmbiguousRuleColumn` and not the unbound name it
  looks like to a scope without the ambiguous column.
- **F5** `AmbiguousRuleColumn` is raised. First-wins is gone from:
  - `ClauseScope::source`: an ambiguous read is an internal error that names the references;
  - `plan_sourced_column` (type inference);
  - `locy_delta`: an ambiguous name is exposed through no reference.
  `positive_is_ref_with_non_node_subject_still_compiles` became
  `positive_is_ref_with_non_node_subject_is_a_kind_mismatch`.
- **F6** `check_sources`, under `#[cfg(debug_assertions)]` at the end of `build_clause`. It walks
  the body bottom-up and refuses any expression reading a name no node below it produced. The
  produced names are the pattern's recorded variables, derived-scan schemas, `ScanAll` nodes,
  generator outputs, model outputs and projection aliases.

**Numbers** (env `TMPDIR=/home/rohit/tmp-tests RUSTC_WRAPPER="" CARGO_BUILD_JOBS=8`):

| Check | Result |
|---|---|
| T1 `is_ref_name_resolution` | 17/17. Was 15/17 after F3: P5 and Q3 failed. |
| `uni-locy` + `uni-query` | 1057/1057: 192 `uni-locy`, 865 `uni-query` including 4 new `guard_*` tests |
| `uni-db --test integration`, full | 3191/3191 passed; 93 skipped (ignored) |
| Locy TCK, schemaless and sidecar | 530/530 each, including 2 new `CrossGroupCorrelation` scenarios |
| clippy `-D warnings` (uni-locy, uni-query, uni-db, all targets), fmt, rustdoc `-D warnings`, `uni-query` checked with debug assertions off | clean |

**Raising every plan diagnostic rejected no existing program.** The first full integration run
with diagnostics raised (3191 tests) had 14 failures. All 14 were a false positive in the first
draft of the F6 guard: it took the pattern's names from the binding plan, which does not know the
hidden names of anonymous relationships (`__anon_edge_*`, issues #294/#162). The guard now takes
them from the Cypher planner's record of the pattern. Those 14 then passed, and no compile error
appeared anywhere.

**Guard revert checks.**
- The planner was made to test each IS position against the bare column name instead of its
  prefixed one (the pre-#300 first-reference layout). Results: the `guard_accepts_*` tests
  failed, and T1 failed 15/17.
- The guard was then disabled with the bare read still in place. Results: the teeth test
  `guard_rejects_a_planner_emitting_an_unprefixed_read` failed, and T1 still failed 15/17 on
  wrong answers and DataFusion errors.
- Both changes were then reverted.

**Known limit of the guard.** It accepts a bare *entity* name, because a MATCH variable or a
reference-bound node is legitimately read bare. So it catches a bare referenced *column*, which
is the capture class, but not a bare entity read emitted where its identity column was meant.
