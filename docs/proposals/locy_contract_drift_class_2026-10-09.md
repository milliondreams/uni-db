# Locy contract drift: one rule, several meanings

**Date:** 2026-10-09 · **Status:** L1 done (`9e24aa3d6`); L2 done (`374be77f2`, closes #298); L3 done (`5d9224467`); YIELD value typing fixed (`821fcca98`); L4 done (`8e081a882`); L5 and L6 implemented (uncommitted) · **Trigger:** issue #298

## Why

Issue #298 reports that a rule's non-KEY value column cannot be read by another rule. What the
reporter needs already works: an IS-reference exposes the referenced rule's value columns by their
bare name, so `WHERE o IS stake TO e ... FOLD agg = MSUM(pct)` returns the expected `['x', 'y']`.
The four spellings the reporter tried fail. That failure is not a one-off. It is a symptom of
something general: **the same Locy construct is given different meanings by different layers**.

- The compiler checks a loose positional model of a program. It counts YIELD columns, and takes
  names and KEY flags from a rule's first clause.
- The DataFusion planner binds IS-reference subjects to KEY columns, and maps clause outputs by
  column name.
- `IS NOT` and provenance (in the same planner) bind by YIELD position.
- The in-memory evaluator (SLG / `locy_delta`) binds by position over all columns and does not
  implement `FOLD`, `BEST BY`, `HAVING`, `REQUIRE` or `PRIORITY` at all.
- Every expression slot except `ALONG` is reparsed as plain Cypher, so Locy syntax there (`prev`)
  is never checked.

Where two layers disagree, the user gets either a late error or, worse, a wrong answer with no
error. This is the class of #158 and #160 (two evaluators), one level further out.

## 1. Confirmed defects

Every row was observed by running a program from the probe file
`crates/uni/tests/common/locy/locy_issue_298_is_ref_value_column.rs`. It only prints, so it is
kept out of `mod.rs` and uncommitted; each work item turns its cases into asserting tests. Each was run next to a control
that differs only in the trigger.

### Silent wrong answers

| # | Program shape | Expected | Got | Root |
|---|---|---|---|---|
| S1 | `EXPLAIN RULE tot WHERE <no match>`, then `QUERY tot` (FOLD rule) | `(a, 12.0)` | 2 rows, `s = null` | R1 |
| S2 | the same, BEST BY rule | the winner | the loser too | R1 |
| S3 | the same, `IS NOT` against a FOLD rule with post-FOLD WHERE | `{a, b, c}` | `{b, c}` | R1 |
| S4 | the same, `count(*)` of a rule | 1 | 2 (bag) | R1 |
| S5 | the same, mutual recursion | `{b, d}` | `{}` | R1 |
| S6 | the same, `IS link TO t` | `t` = nodes | `t` = 5, 7 | R1 + R2 |
| S7 | `IS NOT` against a rule yielding `b, KEY a` | `{b, c}` | `{a}` | R2 |
| S8 | two clauses yielding `KEY a, KEY b` and `KEY a, KEY c` | 5 pairs | 3 | R4 |
| S9 | `FOLD k = COUNT(DISTINCT y)` with parallel edges | 2 | 3 | R5 |
| S10 | `ABDUCE NOT r WHERE zz.name = 'a'` (unbound `zz`) | compile error | empty result | R3 |

S1–S6 need an `EXPLAIN` whose `WHERE` matches nothing (or one on a rule with no facts). An `EXPLAIN`
that matches leaves later results unchanged. That was checked.

### Wrong programs that pass the compiler

| # | Program shape | Outcome | Root |
|---|---|---|---|
| L1 | `(o, e, pct) IS stake` (#298) | plan error: "3 subjects but 2 KEY columns" | R2 |
| L2 | `prev.x` in FOLD, WHERE, YIELD, HAVING, REQUIRE | run-time "No field named prev" | R3 |
| L3 | `ALONG c = coalesce(prev.c, 0) + e.w` in a base case | run-time error; `PrevInBaseCase` missed | R3 |
| L4 | `REQUIRE t * 1.0 <= 50` in a recursive MSUM rule | the #265 direction check is bypassed | R3 |
| L5 | `FOLD t = SUM(e.w) * 1.0` in a recursive rule | monotonicity check bypassed (planner error) | R3 |
| L6 | `BEST BY zz.w` (typo variable) | accepted; ranks on column `w` | R3 |
| L7 | `IS NOT link TO t` with interleaved KEYs | plan error, while the positive `IS` works | R2 |
| L8 | two FOLD clauses, `SUM` and `MAX` into one column | Arrow concatenate error | R4 |

### Found while building L3 (confirmed; fixed, see below)

A list built from values in a Locy YIELD comes back wrong, outside any scope question:

| YIELD item | Result |
|---|---|
| `[1, 2] AS ws` | `[1, 2]` (correct) |
| `e.w * 2 AS ws` | `10.0`, `14.0` (correct) |
| `[e.w, e.w] AS ws` | `[0205, 0205]` — **silent wrong answer**: the values' codec bytes |
| `[v IN [1, 2] \| v * 2] AS ws` | run-time `Encountered non UTF-8 data` |

*Mechanism (confirmed in the code, and broader than first probed):* the YIELD projection casts each
column to a type inferred from its expression, and the inference's catch-all for an expression it
did not know (a list, a comprehension, a map, `reduce`) was `LargeUtf8`. **Every** list a rule
yielded came back as its text, schema or not (`[5, 5]` → `"[5, 5]"`); with no schema the elements
were encoded values and the text was their bytes, a nested list or comprehension failed UTF-8
validation, and a map failed the cast. A relationship property declared nowhere was guessed
`Float64`, so a stored `5` came back `5.0`. Plain Cypher was right throughout.
*Fix:* those expressions, and a property declared nowhere, are typed as Cypher values
(`LargeBinary`), and the Locy projection encodes a column into that type value by value
(`to_cypher_value_expr`) instead of an Arrow cast. `locy_yield_list_values` holds ten expressions
to plain Cypher's answer with and without a schema (15 of 20 differed before). Two of this
track's own tests had recorded the float guess (`w=5.0` for a stored `5`) and were corrected.

### Predicted, not yet run

These need a probe before work on them is scheduled:

- an IS-reference inside `MODULE` (the TCK says modules are not wired into the compiler);
- a rule that IS-references a generator rule (predicted 0 rows);
- `COLLECT` in a self-referencing recursive FOLD;
- `prepare_locy` compiling with a different aggregate oracle than `session.locy()`;
- the in-memory evaluator stopping silently at the iteration limit.

## 2. Five roots

- **R1. EXPLAIN writes its re-derivation into the store that QUERY reads.**
  `explain_rule_mode_b` (`locy_explain.rs:446-457`) re-derives the rule and its dependencies through
  the in-memory evaluator, then inserts the result into `orch_store`. ABDUCE goes through the same
  path. The re-derivation exists only because node VIDs have a hydration step
  (`enrich_vids_with_nodes`) and edge EIDs do not (see the comment above that block).
- **R2. There is no single IS-binding rule.** Four places choose which referenced column a subject or
  `TO` target binds:
  - the planner's positive IS (KEY columns, then the first non-KEY column);
  - IS NOT and provenance (`locy_program.rs:1737-1798`, YIELD position, with a comment assuming
    "KEY columns come first");
  - `locy_delta` (all columns, by position);
  - EXPLAIN child matching (KEY-only).

  The compiler's arity check uses a fifth rule (the full yield width).
- **R3. Expression slots outside ALONG are never scope-checked.** They are reparsed as Cypher.
  `prev` is not reserved there, unbound variables are caught only by DataFusion (or become NULL in
  the in-memory evaluator), and checks such as REQUIRE direction and FOLD monotonicity match only
  the canonical syntactic form, so they are skipped for any other form.
- **R4. The cross-clause contract is unchecked.** The compiler takes a rule's schema from its first
  clause. The planner maps every clause's outputs by name (falling back to an unrelated variable)
  and reads FOLD, HAVING, REQUIRE and BEST BY from the first clause that has a FOLD.
- **R5. Aggregate modifiers are dropped.** `DISTINCT` inside a FOLD aggregate is parsed and then
  ignored (`parse_fold_aggregate`, `resolve_fold_bindings`).

## 3. Why testing missed all of it

No committed test contains the trigger of any confirmed defect. Each harness only generates the
engine's canonical program shape:

| Shape dimension | What every harness does | Hides |
|---|---|---|
| Commands after the rules | The oracle generates rules only. Topo adds one trailing QUERY. In the TCK, EXPLAIN and ABDUCE are always the last statement. | R1 |
| YIELD column order | KEY columns first, everywhere (0 of 528 TCK scenarios differ) | R2 |
| Names across clauses | Always identical | R4 |
| Aggregate modifiers | `DISTINCT` appears nowhere; the oracle's `Agg` cannot express it | R5 |
| Command WHERE scope | Always bound, always matching | R3 |
| Compile vs plan | Never cross-checked. The only fuzz target is parse-only. Tuple IS is tested as "parses". | R2, R3 |

Three structural causes make widening the generators insufficient on its own:

- **Assertions read the wrong store.** `result.derived` comes from the fixpoint's own store, and
  QUERY reads `orch_store`. The oracle differential, the DQP levers and most TCK assertions read
  `result.derived`, so they cannot see R1 whatever program they run.
- **The oracle encodes the engine's conventions.** It matches IS subjects by prefix and groups by
  `row[..key_count]`. A non-KEY-first program would probably make it agree with the buggy engine
  (inferred, not run).
- **The TCK QUERY/derived parity guard is nearly tautological.** Since #160 both sides read the same
  store. It checks only that derived KEY tuples are a subset of QUERY rows, and it skips programs
  with a real WHERE, LIMIT, EXPLAIN or ABDUCE.

## 4. The contract to converge on

**C-IS (one binding rule).** A referenced rule's columns form one binding sequence: its KEY columns
in YIELD order, then its non-KEY columns in YIELD order. Subjects consume the sequence from the
front, and a `TO` target takes the next column.

- For a rule whose KEYs come first, this is the positional binding `complete_locy.md` describes.
  So documented programs keep their meaning, and `(o, e, pct) IS stake` binds `pct`.
- For one or two subjects, it is what the planner's positive IS already does, including #272's
  "`TO` binds the first non-KEY column once the subjects span the KEYs". So no program that runs
  correctly today changes.
- A subject or target already bound in the body becomes an equality test. An unbound one is
  bound, as a node when the column is a KEY node column and as a value otherwise.
- The same function serves positive IS, IS NOT, provenance, `locy_delta` and EXPLAIN children. The
  compiler's arity check becomes "subjects + target ≤ length of the sequence", which is what it
  already enforces.

**C-SCOPE.** Every expression in a rule body is checked against the variables bound at that
point:

- MATCH variables, IS targets, the referenced rules' non-KEY columns, ALONG names and generator
  outputs;
- after FOLD, only KEY columns and FOLD outputs.

`prev` is legal only inside ALONG, and only in a clause with a same-SCC reference. Command WHERE
clauses (EXPLAIN, ABDUCE, DERIVE) are checked against the rule's yield schema, as QUERY already
is. Violations are compile errors that name the rule, clause, slot and variable.

**C-CLAUSE.**
- All clauses of a rule agree on YIELD names and KEY and PROB flags, by name; column order is
  free. (Proposed as position by position; L4 measured that a positional check would reject
  programs answered correctly.)
- Every folding clause states the same FOLD names, aggregates and DISTINCT flags (arguments may
  differ), and the same post-FOLD WHERE, REQUIRE and BEST BY. In a rule without FOLD, every clause
  states the same BEST BY or none does.
- A seeding clause without FOLD stays legal, and states none of them.
- Disagreement is a compile error, with a hint to alias with `AS`.

**C-NONINTERFERENCE.** A command never changes what a later command reads. The facts a stratum
publishes are fixed for the rest of the program. Only ASSUME, inside its own scope, may present
different facts.

## 5. Work items, in order

Each item lands tests first: they fail on the current code, then pass with the fix. Every probe
pair in §1 becomes an asserting regression test in the item that fixes it.

**L0 — Tests that fail now.** Each item converts its probe pairs into asserting tests that fail on
the code before its fix, and lands them with the fix. (The first draft wrote every item's tests up
front; tests for later items could only have been committed under `#[ignore]`, which hides exactly
what they exist to show.)

**L1 — Store isolation (R1).**
- Fix: Mode B re-derives into a store local to the EXPLAIN, filled in from the published facts for
  any reachable rule the re-derivation did not produce, and `build_derivation_node` reads it.
  Nothing is written back. ABDUCE inherits the fix through `explain_rule`.
- *Done, and stronger than planned:* every command now receives the published store by shared
  reference (`&RowStore`) at the top level and inside an `ASSUME` body, where the same leak
  existed. A command that writes it no longer compiles, which replaces the planned debug-build
  fingerprint check.
- Coverage, in three places:
  - *Cost note (measured during the YIELD-typing fix):* the topo `locy_commands` relation with four
    commands took the flushed lane from 82 s to 224 s alone, near the 6-minute slow-timeout under
    full-suite load (one run timed out). It now interposes one command, a non-matching EXPLAIN of
    the rule that reads every other (145 s alone); it still fails with the L1 fix reverted.
  - **Non-interference relation:** for a program P, `P; <cmd>; QUERY r` must equal `P; QUERY r`, for
    every rule `r` and for `<cmd>` drawn from EXPLAIN and ABDUCE with matching, non-matching and
    unbound-key WHERE. Added to the oracle differential (the oracle generator starts emitting
    trailing commands), the topo harness (each Locy relation runs once with and once without an
    interposed command) and a TCK step applied to every scenario that contains EXPLAIN or ABDUCE.
  - ~~Debug invariant~~: superseded by the shared-reference store above.
  - **TCK parity guard:** now asserts KEY-tuple equality in both directions, so an extra QUERY row
    fails as well as a missing one. Appending a QUERY to every scenario was not done: each TCK
    step runs under its own configuration, and the type-level guarantee plus the three relations
    above already cover command interference.

**L2 — One binding rule (R2). Fixes #298.**
- Fix: implement C-IS as one function, `is_ref_bindings(target_schema, subjects, to)`, and route
  the four call sites and the compiler's arity check through it. Correct `complete_locy.md`
  §5.1 (lines 411, 419, 427 and 432), and state the by-name visibility rule in the website Locy
  guide.
- Coverage: **shape-preserving rewrite relations** in topo and the oracle generator. None needs
  the oracle to be right, only two forms to agree:
  - permuting a rule's YIELD columns, including putting a non-KEY column first and interleaving
    KEYs, leaves every consumer's facts unchanged;
  - binding a value column by tuple, by `TO` and by bare name gives the same facts;
  - renaming a clause's variables leaves its facts unchanged.

  The oracle switches from prefix matching to C-IS, so that it stops sharing the engine's
  assumption.
- *Done:* `uni_locy::is_binding` is the one definition. The compiler's arity check, the planner's
  positive references (subjects past the KEY columns now bind like a `TO` target: a node is
  scanned and joined, a scalar is aliased, an already-bound variable is tested), IS NOT and
  provenance (via `LocyIsRef::binding_columns`, resolved at planning time), the in-memory
  evaluator and EXPLAIN children all bind through it. `IS NOT` comparing a bound variable with a
  value column is `IsNotBindsValueColumn` (decision 1, as refined). Tests: `locy_is_ref_binding` (8). The topo harness has a
  `locy_binding` relation (tuple / `TO` / bare name / permuted YIELD). Rather than teach the oracle
  C-IS, its second run of every generated program interleaves each rule's value columns among its
  KEYs: under C-IS that changes no answer, so the KEY-first oracle still applies.
- *Found on the way:* ABDUCE builds its tree with the in-memory evaluator, which bound `TO t` by
  the stored relation's column order. With a value column between two KEYs, no clause reproduced
  the fact, the tree ended there, and ABDUCE reported **no valid edit** (silently) where removing
  either edge would do. Fixed by the same change (`is_ref_binding_abduce_follows_the_binding`).
  A matching EXPLAIN cannot show this: with EXPLAIN in a program the provenance tracker always
  exists, and Mode A records no children.
- *Found on the way, by the oracle's interleaved-YIELD run on its first random program:* the whole
  FOLD pipeline assumed KEY-first. The FOLD operator emits KEYs before aggregates, a seeding clause
  emitted YIELD order, derived scans were declared in YIELD order, and `fold_input_column_types`
  paired schema and items by position. `YIELD 1 AS s, KEY a` beside `FOLD s = ... YIELD s, KEY a`
  failed ("concatenate arrays of different data types"), and the recursive form failed ("column
  types must match schema types"). Rather than patch each consumer, the compiler now stores every
  rule's schema in binding order (`into_binding_order`); column order is not observable, since
  facts are read by name. A seeding clause also lays its columns out like the FOLD clause
  (`rule_fold_layout`), which fixed a separate loud failure even with KEYs first: two FOLD outputs
  yielded in an order other than their FOLD bindings (`YIELD KEY a, t, n` with `FOLD n = ..., t =
  ...`). Each fix is needed: disabling either one fails `is_ref_binding_yield_order_changes_no_answer`.

**L3 — Scope checking (R3).**
- Fix: a compile pass implementing C-SCOPE over every Cypher `Expr` slot, including inside ASSUME
  bodies. In the same pass:
  - a FOLD aggregate that is not a top-level call becomes a compile error, so the monotonicity
    check cannot be bypassed;
  - BEST BY resolves its full expression against the bound set, instead of the property name alone;
  - the REQUIRE direction check splits top-level `AND` and classifies each side by its
    monotonicity in the fold output. A form it cannot classify is rejected in a recursive rule:
    fail closed, since admitting an upper bound is what turns a fixpoint into an oscillation.
  - In the walker, `prev` outside ALONG becomes `PrevOutsideAlong`, and the ALONG path checks `prev`
    under function calls and parentheses (L3 in §1).
- Coverage:
  - **Construct × position matrix:** a table test with one row per (Locy construct, expression slot)
    pair, each expecting either a stated result or a named compile error. A cell with neither
    fails the test, so a new construct or slot has to be classified.
  - **Compile implies plan:** every program that compiles must plan and run on an empty graph without
    a planning error. This applies to the oracle generator, topo, the 18 TCK compile-success
    scenarios (the step gains a plan-and-run phase) and a mutation generator that moves constructs
    from valid programs into other slots.
  - Extend the `locy_parse` fuzz target to compile. A planning stage can follow if the fuzz
    harness can host a database.
- *Done:* `compiler/scope.rs` (`PrevOutsideAlong`, `UnboundVariable`, `FoldNotAnAggregateCall`),
  EXPLAIN / ABDUCE / DERIVE WHERE checks, and REQUIRE splitting `AND` and refusing a fold output
  inside an expression. The pass runs after every existing check, so their errors keep
  precedence. Coverage: `locy_scope_check` — 18 rejections and 10 acceptances by hand, plus a
  generated sweep of 9 slots × 8 candidates asserting no "No field named" reaches the planner.
  The sweep caught the first version's post-FOLD YIELD scope being too wide (it admitted body
  variables the folded batch does not hold). The TCK compile step now evaluates every program it
  compiles. The construct × position table was replaced by the generated sweep, which classifies
  every cell without listing them. The fuzz target is unchanged (it is parse-only and has no
  database to plan against).
- *Before the fix, silently:* `REQUIRE prev.t >= 1` returned rows; `BEST BY zz.w` ranked by `w`;
  an unbound variable in an ABDUCE, EXPLAIN or DERIVE WHERE matched nothing without an error.

**L4 — Cross-clause contract and aggregate modifiers (R4, R5).**
- Fix: C-CLAUSE as a compile check. For `DISTINCT` in FOLD, see decision 2 in §6.
- Coverage:
  - the oracle generator emits multi-clause rules with renamed variables, YIELD aliasing and
    `DISTINCT`;
  - the oracle's `Agg` gains a `distinct` flag;
  - a topo relation checks `FOLD COUNT(DISTINCT y)` against Cypher `count(DISTINCT y)`.

- *Done:* `check_yield_agreement` (names, KEY and PROB by name, order free; a positional PROB
  check and `check_count_fold_seed`'s positional seed lookup became by-name) and
  `check_fold_tail_agreement` (`ClauseMismatch`). Decision 3 as recommended, refined by
  measurement: the contract is **by name, not by position**. A clause yielding `KEY b, KEY a`
  beside `KEY a, KEY b` was already answered correctly, and rejecting it would break a correct
  program; FOLD *arguments* legitimately differ per clause (`ProbabilisticStress.feature:221`
  folds `MNOR(e.prob)` in the base clause and `MNOR(p)` in the recursive one), so the FOLD check
  compares names, aggregates and DISTINCT only. DISTINCT per decision 2(a): `FoldBinding::distinct`,
  set by both FOLD resolvers (two copies of the same parser, another instance of R2's drift),
  deduplicated per group in `FoldExec`; `DistinctFoldInRecursion` in recursive rules.
  Tests: `locy_clause_contract` (5 tests; the DISTINCT test holds 8 aggregate forms, with and
  without DISTINCT, to plain Cypher; with the dedup disabled 6 of 16 differ). A survey of 248
  multi-clause rules in the corpus found 3 that disagree: 2 negative tests already expecting the
  error, and `test_similar_to_in_locy_along`, whose `BEST BY` on the recursive clause only was
  the defect below (fixed by stating it on both clauses). `locy_scope_check`'s recursive template
  stated its REQUIRE on one clause only; its base clause now seeds instead of folding.
  The oracle generator and a topo `count(DISTINCT)` relation were not added: the oracle's
  generated rules never differ across clauses, and the Cypher-oracle test covers DISTINCT on
  both engines directly.
- *Found on the way, silent:*
  - **S11.** A REQUIRE on any clause but the first that folds was ignored: on CHAIN,
    `REQUIRE t >= 2` on the recursive clause only returned all six `t = 1` facts (both
    clauses: none).
  - **S12.** The same for a post-FOLD WHERE.
  - **S13.** A BEST BY in a rule without FOLD ranked the whole rule when clause 1 stated it and
    only its own clause otherwise (`w=5` versus `w=5, w=7` for the same two clauses).
  - **S14 (Cypher).** `sum(DISTINCT e.w)` over a schemaless property summed every row (17 for
    distinct values {1, 2, 3, 4}), and so would `percentileDisc` / `percentileCont`, `stdev` and
    any plugin aggregate under DISTINCT: DataFusion passes DISTINCT to the accumulator as a flag
    that only its own count / sum / avg / min / max read. The planner's comment said percentiles
    "handle their own distinct"; they did not. Fixed with one wrapper (`distinct_udaf`) for every
    aggregate outside that set.
  - **A regression from the YIELD-typing fix (`821fcca98`):** clauses type their columns
    independently, and with an undeclared property now a Cypher value, `r.w` in one clause and
    `e.w + 1` in another failed ("concatenate arrays of different data types"). Both were guessed
    `Float64` before. Fixed in the planner by unifying a column the clauses type differently
    (`clause_column_types`).
- *Found, not fixed:* Cypher's `sum` over a dynamically typed column skips a value it cannot add
  (`// skip non-numeric` in `CypherSumAccumulator`), so `sum(x.name)` is 0 and a sum of durations
  is 0. A literal `sum('a')` fails loudly; Neo4j raises a type error for strings and sums
  durations. The fix is to implement duration sums and error on other types.

**L5 — The in-memory evaluator as a second engine (R1, remaining).**
- Fix:
  - Add edge EID hydration next to `enrich_vids_with_nodes`, then make Mode B read the fixpoint's
    facts, and delete its re-derivation. That removes the second evaluator from EXPLAIN and ABDUCE
    entirely, not just its write-back.
  - What remains of SLG serves generator rules. It then seeds every non-generator dependency from
    the fixpoint's store (today only FOLD rules), refuses with a typed error any rule using FOLD,
    BEST BY, HAVING, REQUIRE or PRIORITY, and reports the iteration limit instead of stopping
    silently.
- Coverage: run the probes for the five predicted items in §1 first, and schedule fixes only for
  what reproduces.

- *Done, further than planned:* the in-memory evaluator is **deleted** (`locy_slg.rs`, 678
  lines), not reduced. Its two remaining callers went away at the root:
  - **Generator rules run in the fixpoint.** A generator is a columnar operator
    (`LocyGenerateExec`); the plugin contract was already columnar. `build_stratum` used to skip
    every generator rule, which was the cause of the predicted "generator dependents see 0
    rows" and more: probed, a rule referencing a generator rule got **no facts** (`IS` → none,
    `IS NOT` → everything), and a generator rule's own **FOLD returned NULL per input row**, its
    BEST BY and post-FOLD WHERE were ignored. QUERY's fallback to the evaluator is now an error
    (no path reaches it: a rule with no facts has an empty relation). `prepare_locy` failed every
    generator program ("not registered"): the generator was found through a registry only
    `Session::locy` installed; the operator resolves it from the planner's registry, which
    sub-plans now inherit.
  - **EXPLAIN Mode B reads the published facts.** Its re-derivation existed because edge EIDs
    were not hydrated; probing that found a **silent wrong answer of its own: a rule yielding a
    relationship yielded NULL** for it (typed as a string), and a rule reading it failed. Edges
    are now carried by EID and hydrated like nodes. The first hydration attempt resolved EIDs as
    VIDs (the ID spaces overlap) and bound unrelated nodes, so edge columns are classified from
    the rules (`uni_locy::entity_columns`), and DERIVE's separate hydration copy, which also
    swallowed lookup failures, goes through the same path and fails loudly.
  - The planned "SLG refuses FOLD / reports its iteration limit" items are moot.
- *Probed predicted items:* MODULE IS references, EXPLAIN, ABDUCE and DERIVE all failed with
  "unknown rule" (loud): the compiler resolved module names for its own checks only. Fixed by
  handing on the resolved names. `COLLECT` in a recursive FOLD, which the TCK documents as
  accepted, failed at run time: its output was typed `Float64`; typed as a list, it runs and
  answers per the bag semantics. `prepare_locy` agreed with `locy` on every compile error.
- *Found on the way:*
  - EXPLAIN Mode A hydrated every integer KEY as a node (a generator output `i = 1` came back as
    a node); it now classifies columns from the hydrated published facts.
  - **The provenance store keyed a fact by its row alone**, keeping the first annotation: when two
    rules derive the same tuple, EXPLAIN of the second silently lost it. Keyed by rule and row
    now (`provenance_fact_hash`).
  - ABDUCE never proposed a node's or edge's property (it read only map bindings) and proposed
    `SET i.i` for scalar bindings, which cannot apply; on a generator dependent it reported no
    valid edit where `n.k → 0` is one. Fixed, with duplicate candidates removed.
  - **ABDUCE built its tree from recorded provenance whenever it existed** (any program with an
    EXPLAIN or a probabilistic FOLD), and that record has no children, so a cause in a referenced
    rule was invisible: over `risk ← MNOR(w) ← link(a, b)` it proposed nothing valid. Two TCK
    scenarios (`TripleCombinations`, `SimilarToAbduceAssume`) passed only because of the bogus
    `SET p.p` candidate removed above; with it gone they failed, which is how this surfaced.
    ABDUCE now always builds the tree from the published facts.
  - A regression in my own L4 type unification (per-clause MATCH node sets typed an IS-bound
    node unlike a MATCH node) was latent until a projection path honoured target types; it now
    uses the rule-level node set.
- *Found, not fixed:* `type(e)` in a QUERY RETURN fails ("Function type not implemented"): the
  row-level RETURN evaluator lacks it (loud). `max_slg_depth` is kept as a no-op field (Rust and
  Python), since removing it breaks existing configurations.
- Tests: `locy_single_engine` (7), `locy_generator_plugin` (+3, one the plan-shape proof for
  `LocyGenerateExec`). `repro_issue_157`'s test pinning the module "known limitation" (an
  ambiguous bare name refused) now asserts the answer comes from the enclosing module. Toggled off one at a time,
  the rule-qualified provenance hash, the edge/node classification, ABDUCE's entity properties
  and Mode A's classification each fail a test.

**L6 — Record the invariants.** Black Book Appendix B2 gets C-IS, C-SCOPE, C-CLAUSE and
C-NONINTERFERENCE, each with the test that pins it. The correctness-class harness proposal gains
the "shape dimensions" table of §3 as a standing checklist for new generators.
- *Done:* all four entries were already in B2, each added with its item and naming its
  tests. L6 tags three of them with their contract names (C-CLAUSE had its tag). It also corrects
  descriptions L5 left stale: C-IS listed the deleted evaluator as a caller; QUERY was described
  as SLG resolution (Black Book, website internals, the landing page, `why-uni.md`); and the
  compliance-remediation example said `queries_executed` counts SLG clause evaluations (it counts
  Cypher statements and EXPLAIN's pattern queries; measured 0 for that program). The harness
  proposal gains §6, the checklist, with three dimensions L5 added: relationships in facts,
  colliding ID spaces, and modules and generators.

**Order.** L1 comes first because it is the smallest change with the most silent wrong answers
(six), and its relation catches any future command that writes shared state. L2 closes the issue.
L3 and L4 are compile-time tightening and can run in parallel. L5 is the largest and depends on
L1's tests.

## 6. Decisions needed

1. **IS NOT bound to a non-KEY column.** C-IS makes it expressible: an anti-join on a value. Today
   the planner refuses a scalar `TO` target under IS NOT (L7). *Recommendation:* allow IS NOT to bind
   KEY columns only, as a compile error naming the column. Negation's stratification argument is
   about facts, and a fact is identified by its KEY.
   *Refined during L2 (decided 2026-10-09):* an existing test (`locy_is_not_scalar_to_target`) pins
   an **unbound** variable on a value column as existentially quantified (`b IS NOT stake TO agg`
   means `b IS NOT stake`). That stays legal; only a variable already bound there is a compile
   error, since it would need a value comparison.
2. **`DISTINCT` inside FOLD.**
   - (a) Implement it for non-recursive rules and reject it in recursive ones, where a distinct
     contribution has no clear monotone meaning.
   - (b) Reject it everywhere.

   *Recommendation:* (a). `COUNT(DISTINCT x)` is the natural way to write what S9 needs, and the
   non-recursive FOLD already sees the whole bag.
3. **Strictness of C-CLAUSE.** Rejecting differing YIELD names breaks programs that currently run.
   Some of them run correctly by coincidence, when the differently named variable happens to hold
   the right value. *Recommendation:* reject, with an `AS` hint. The alternative, positional mapping,
   silently changes the meaning of those same programs.

## 7. User-visible changes

Programs that are **rejected** after this work:
- differing YIELD names or FOLD specs across clauses;
- `prev` outside ALONG;
- unbound variables in rule bodies and command WHERE;
- non-call FOLD expressions;
- unclassifiable REQUIRE in recursion;
- `DISTINCT` in recursive FOLD (under decision 2a);
- IS NOT on non-KEY columns (under decision 1).

**Results change** for IS NOT against a rule whose KEYs are not leading (S7: those results were
wrong), and for every program with an EXPLAIN or ABDUCE before a QUERY (S1–S6). These go first in
the release notes.
