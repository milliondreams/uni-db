# Issue #162 — recursive FOLD loses child multiplicity: analysis and fix scoping

Status: **complete** (§12). Nesting is the decided semantics; implementation,
tests, TCK and documentation all landed. Green on every gate.
Repro: `crates/uni/tests/common/locy/locy_issue_162_prob_fold_consumer.rs`,
`crates/uni/tests/common/locy/locy_issue_162_shape_matrix.rs`.

## 1. What is broken

A recursive rule whose clause consumes its own relation and folds the consumed
value loses one contribution per group of equal-valued child derivations. The
reporter framed the trigger as "equality of the folded values"; that part is
right, but the blast radius is much wider than the single shape in the issue.

Measured on 3.3.0 (`issue_162_shape_matrix`), `FOLD b = MPROD(b)` rollup over a
BOM, every leaf at reliability 0.5:

| shape | want | got |
|---|---|---|
| S1 `TOP→MID→{L1,L2}` (the reported case) | 0.25 | **0.5** |
| S2 same, leaves 0.5/0.4 (reporter's control) | 0.2 | 0.2 |
| S3 `TOP→{L1,L2}` | 0.25 | 0.25 |
| S4 `TOP→MID→X→{L1,L2}` | 0.25 | **0.5** (and `MID` = 0.5) |
| S5 `TOP→{A,B}`, `A→{L1,L2}`, `B→{L3,L4}` | 0.0625 | **0.25** |
| S6 `TOP→MID→{L1,L2,L3}` | 0.125 | **0.5** |
| S7 `TOP→{MID,L3}`, `MID→{L1,L2}` | 0.125 | **0.25** |

5 of 7 shapes are wrong. The rule is: **any node with ≥2 children whose derived
values are equal silently drops all but one of them, and the loss propagates to
every ancestor.** Uniform reliabilities — the common case in a risk model — hit
this almost everywhere. Errors are optimistic for `MNOR`/`MPROD`.

Note S4: the error is not confined to the node directly above the branch. `MID`
itself is wrong there, so "verify the rule in isolation" does not localise it.

## 2. Mechanism

Confirmed, not inferred:

1. A self-reference inside a recursive rule reads the rule's **pre-fold
   contribution rows**, never a folded value — `update_derived_scan_handles`
   (`crates/uni-query/src/query/df_graph/locy_fixpoint.rs:4207-4243`) writes
   `all_delta()` / `all_facts()`, which are `FixpointState`'s contribution rows.
   FOLD runs only after loop exit, in `apply_post_fixpoint_chain`
   (`locy_fixpoint.rs:1545`, `:4724`).
2. So a parent joins one row per *contribution* of the child, not one row per
   child.
3. Whether those rows survive is decided by the whole-row dedup in
   `merge_delta` (`locy_fixpoint.rs:564`, `compute_delta` `:634`), keyed on all
   columns including the `__deriv_*` discriminators added by `fbe60af91`.
4. Those discriminators are built from the **consuming clause's own bindings** —
   clause index plus the vids of MATCH-bound node variables
   (`locy_planner.rs:519-569`). They carry nothing about *which derivation of
   the child* was joined. For `MATCH (p)-[:CONTAINS]->(c) WHERE c IS build`,
   every contribution of the same child `c` produces the same discriminator.
5. Equal values + equal discriminator ⇒ whole-row dedup collapses them.

Direct evidence for (1): a control with unequal leaves and a *scaled* fold,
`FOLD b = MPROD(b * 0.5)`, returns 0.0125 — exactly
`(0.5·0.5)·(0.4·0.5)`, the product over `MID`'s two contribution rows — where
folding `MID`'s value gives `0.05·0.5 = 0.025`.

Scope check: a consumer in a **later stratum** is unaffected. Cross-stratum
handles read published, folded, stripped facts
(`a_later_stratum_consumer_sees_the_folded_value` passes). The defect is
confined to same-stratum references.

## 3. This is the residue of the #159 fix, and it was documented in advance

`FOLD_DISCRIMINATOR_COL_PREFIX` (`planner_locy_types.rs:129-176`) records that
per-iteration folding was prototyped and rejected because "a self-ref reads
pre-fold contribution rows, so a parent joins a multi-contribution child more
than once". `fbe60af91` fixed the producing rule's own value and left the
consuming join reading pre-fold rows. #162 is the first wrong answer out of
that residue.

## 4. The semantics fork

Two readings of a recursive FOLD:

- **Flattening** — the value of `f(p)` aggregates the bag of *all derivations*
  reaching `p`, i.e. the leaves of the derivation tree. This is what
  `docs/complete_locy.md:502` says ("a FOLD aggregates the bag of derivations,
  not the set of distinct row values") and what the `FoldInRecursivePath`
  warning presumes ("FOLD groups by KEY columns, not by path").
- **Nesting** — `f(p)` aggregates its *children's folded values*. This is the
  invariant the reporter used, and the one an assembly/BOM rollup means.

For every shape in §1 the two readings **agree**, because `MPROD` with an
identity argument is associative. So the fix target for #162 is unambiguous —
all five BAD rows are wrong under both readings. The readings diverge only for
a fold whose argument is not the bare inherited column (`MPROD(b * 0.5)`) or
whose aggregate is non-associative (`MCOUNT` — flattening counts leaves,
nesting counts children).

There is **no declarative semantics documented anywhere** for a self-referential
FOLD: no fixpoint equation, no lattice order, only "iterate until the f64
accumulator stops moving" (`docs/complete_locy.md:1242, 1272-1288`). Whether an
in-stratum IS-ref observes folded or pre-fold rows is not stated. Whatever fix
lands must also close that spec gap.

## 5. Option A — derivation identity (stay with flattening)

Extend the dedup key so distinct derivations of the child stay distinct.

**One level is provably insufficient.** S4 is the counterexample: projecting the
target's base discriminators (`clause`, `vid_c`) into the consumer gives `TOP`'s
two rows the same source identity `(clause=1, vid_c=X)` and they collapse again.
Correctness needs the *transitive* derivation identity, which is not
fixed-width — it would have to be a single recursive `__deriv_id: UInt64` hash
of `(clause, bound vids, joined refs' __deriv_id)`.

Consequences:

- **Inherently path-exponential.** Under flattening, a root's contribution count
  *is* its root-to-leaf path count. A 10-level BOM with fan-out 3 gives ~59k
  rows for the root. Today's engine is cheap only because it is wrong.
- **Termination regresses on cycles.** The current finiteness argument is that
  discriminators are vids, so the domain is finite. Derivation-tree identities
  over a cycle are unbounded, so a cyclic recursive FOLD would move from
  "converges with a wrong number" to `LocyIncomplete`.

Cheaper to build than Option B, but it institutionalises a semantics that does
not scale and that users demonstrably do not mean.

## 6. Option B — per-iteration fold (nesting) — recommended

Give a self-reference the rule's **folded value as of the previous iteration**,
while leaving contribution rows in place for everything else that depends on
them.

Design, contained deliberately to avoid the breakage the earlier prototype hit:

1. **Dual-view handles.** `DerivedScanRegistry` entries gain a view kind;
   handles keyed by `(rule_name, is_self_ref, view)`. Today they are keyed by
   `(rule_name, is_self_ref)` (`locy_planner.rs:1861-1867`).
2. **The planner requests `Folded` only where it is needed** — a same-stratum
   IS-ref whose target carries FOLD and whose fold-output column this clause
   actually consumes. Everything else keeps the `Contributions` view: ALONG
   `prev.x` (which resolves to a self-ref scan column,
   `locy_planner.rs:2076-2080`), self-negated IS NOT, and provenance.
3. **`FixpointState` gains a `folded` buffer**, recomputed at the end of each
   iteration by running PRIORITY + `FoldExec` over `all_facts()` — the same
   `FoldExec` the post-fixpoint chain uses (`locy_fixpoint.rs:4724`), so there
   is one fold implementation, not two.
4. **Contribution rows must be replaced, not appended.** Once a child's folded
   value can change between iterations, the parent re-derives with a new value
   and the stale row would be folded in as well. Dedup key becomes
   KEY + `__deriv_*` (value columns excluded); the newer row replaces the older.
   `merge_best_by` (`locy_fixpoint.rs:743`) is the existing precedent for
   replace-per-KEY and its `changed` return.
5. **Convergence becomes value-based** — "no key added and no value moved" —
   because delta-emptiness is meaningless once a clause re-derives from a full
   snapshot. The existing 12-decimal float rounding
   (`round_float_columns`, `:926`) supplies the epsilon. Note that
   `MonotonicAggState` cannot serve this role: it is only ever fed the delta
   (`:576-584`) and `merge_delta` returns early without calling `update` when
   the delta is empty (`:598-605`), so only *new rows* can keep the loop alive.
   That is sound today, when values never move without a new row; it stops
   being sound the moment a self-reference reads a value that can change in
   place.
6. **Termination.** A DAG converges in O(depth). Unbounded aggregates around a
   cycle exhaust `max_iterations` → `LocyIncomplete`, which
   `website/docs/locy/rule-semantics.md:60` already documents as the backstop.
   No regression relative to today.

Cost is linear in nodes × in-degree, not in paths.

### Compatibility with the existing oracle set

The value-asserting scenarios were hand-checked: `ProbabilisticStress:212`
(0.58 / 0.84), `MonotonicAggregation` (MSUM 3.0 / 2.0, MCOUNT 3),
`ExactProbability` (0.79 / 0.21), and the five `locy_issue_159` tests all have
at most one derivation per child, so nesting and flattening agree on them and
the constants should stand. Each still has to be run — this is a hand-check,
not a result.

### Known risks to resolve during implementation

| # | Risk |
|---|---|
| R1 | `DerivedScanExec` re-stamps its declared schema onto every batch (`locy_fixpoint.rs:4319-4330`); the folded view's declared schema must match `FoldExec`'s output exactly (KEY + fold columns, discriminators stripped) or it is a hard Arrow error. |
| R2 | `collect_is_ref_inputs` mints `base_fact_id` by hashing rows of the registry entry (`:2104-2107`) and must keep reading the `Contributions` view, or lineage silently truncates against `record_provenance`'s pre-fold hashes (`:1621`). |
| R3 | `find_clause_for_row` skips any batch whose arity differs from the yield schema (`:2775`) and silently returns clause 0. Must never see a folded batch. |
| R4 | `detect_shared_lineage` / `apply_exact_wmc` operate on `pre_fold_facts` and ignore KEY groups of size < 2 (`:2236`). Unaffected in shape, but the values inside those rows change; re-derive and re-run the 0.79 / 0.21 constants. |
| R5 | `body_support_map` for TopKProofs is a pre-fold full-column row hash built once post-fixpoint (`:4635-4653` ↔ `locy_fold.rs:730`); confirm it is still built from contribution rows. |
| R6 | HAVING / BEST BY are *not* proposed for the per-iteration view (only PRIORITY + FOLD). Non-monotone filters per iteration threaten termination. Document the deviation; a self-referencing rule with HAVING is arguably ill-defined today too. |
| R7 | The `FoldInRecursivePath` warning steers authors to ALONG on the premise that FOLD groups by KEY and not by path. Under nesting that advice needs rewording. |

## 7. Effort

| Work | Estimate |
|---|---|
| Spike: dual-view handle + per-iteration fold, no replace semantics — enough to turn S1–S7 green and run the TCK | 0.5–1 day |
| Full implementation: replace-by-derivation-key, value-based convergence, provenance routing, schema plumbing | 3–5 days |
| TCK scenarios for the S1–S7 shapes + the nesting/flattening divergence cases; write the declarative semantics the docs lack; update `complete_locy.md` §8.2 and the `FoldInRecursivePath` prose | 1–2 days |

Regression surface: ~84 tests touch recursive FOLD/ALONG, but roughly half are
compile-only (monotonicity oracle, warning codes) and insensitive to what a
self-reference reads. The load-bearing set is ~20 value-asserting tests, listed
in R4 and the compatibility note above. Termination tripwires:
`bugs::locy_is_not_complement_recursion` (1350 facts, key-only) and the cyclic
`along/PathCarriedValues.feature:102`.

## 8. Interim mitigation

A full fix is a core-engine change and #162 is a silent, optimistic wrong
answer in the field. The trigger is exactly detectable and cheap: within one
iteration of a recursive FOLD rule, `dedup_batches_all_columns` dropping a
candidate row means the fold lost multiplicity. Raising a runtime warning (or
erroring, in the spirit of `9d3ac7c16`) at that point converts the silent wrong
answer into a visible one without touching semantics. This needs verifying —
a base clause legitimately re-emits rows across iterations, so the detector must
be scoped to *within-iteration* duplicates.

A hard compile-time rejection of "recursive FOLD reading its own fold output" is
**not** viable: that is exactly the shape `ProbabilisticStress:212` asserts.

## 9. Open decision for the maintainer

Nesting or flattening. The recommendation is nesting (Option B) on three
grounds: it is what the BOM/risk invariant means, it is linear rather than
path-exponential, and it agrees with every value-asserting test currently in the
suite. But it is a semantics choice that the spec has never made, and it should
be made explicitly and written down rather than settled by whichever fix lands.

Note that `locy_issue_162_prob_fold_consumer::consumer_folds_the_childs_value_not_its_contribution_rows`
asserts 0.025, i.e. it encodes **nesting**. If flattening is chosen instead,
that test's expectation becomes 0.0125 and only the other two tests stand.

---

## 10. Spike result (2026-08-07)

Option B was spiked. **It works.** +341 lines across
`locy_fixpoint.rs` and `locy_program.rs`.

What landed:

- `FoldViewState` on `FixpointState` — a KEY-grouped snapshot of the rule's own
  facts, refreshed after every merge.
- `merge_fold_contributions` — replaces a contribution whose derivation key
  (every column except the fold inputs) already exists, instead of appending
  it, and reports change over the whole-row set rather than delta emptiness.
- `update_derived_scan_handles` feeds that snapshot to same-stratum references.
- `FixpointRulePlan::fold_self_ref`, set when a rule carries FOLD, is consumed
  positively by its own stratum, and has no ALONG.

Results:

| gate | before | after |
|---|---|---|
| `issue_162_shape_matrix` (S1–S7) | 5 of 7 wrong | **0 wrong** |
| `locy_issue_162_prob_fold_consumer` (3 tests) | 2 failing | **3 passing** |
| `cargo nextest -p uni-db` | 2801 | **2801 passing** |
| `uni-query` + `uni-locy` | 909 | **909 passing** |
| Locy TCK (`uni-locy-tck`) | 514 | **514 passing, 0 failed** |
| `cargo clippy` / `cargo fmt` | clean | clean |

Nothing needed re-deriving. `ProbabilisticStress:212` (0.58 / 0.84),
`ExactProbability` (0.79 / 0.21), `MonotonicAggregation` (3.0 / 2.0 / 3) and all
five `locy_issue_159` tests hold unchanged, confirming the §6 compatibility
hand-check. The termination tripwire
`bugs::locy_is_not_complement_recursion` (1350 facts) is unaffected.

Cyclic termination was the one risk the suite did not cover, so
`issue_162_cyclic_recursive_fold_terminates` was added: `A→B→C→A` with a
supplied leaf converges in 0.19 s to 0.0 — the correct limit for an unbounded
product around a cycle. No spin to `max_iterations`.

### What the spike deliberately does not do

These are the gap between the spike and a shippable fix:

1. **No dual-view handles.** The spike overwrites the single existing buffer, so
   provenance (`collect_is_ref_inputs`), self-negated IS NOT and TopK
   `body_support_map` now read folded rows. Every test passes, but that is
   likely because in each covered scenario a KEY has a single contribution, so
   the folded row *is* the contribution row. R2/R3/R5 from §6 are unretired —
   production still needs `(rule, is_self_ref, view)`-keyed handles so those
   consumers keep reading contributions.
2. **ALONG is excluded**, so an ALONG+FOLD recursive rule still exhibits #162.
   `prev.x` reads a contribution column a KEY-grouped snapshot cannot carry per
   path; that case needs the dual-view separation first.
3. **The fold is not `FoldExec`.** The spike aggregates through
   `LocyAggregate::update_step`, the same row-level path `MonotonicAggState`
   uses, so aggregates without a row-level fast path (AVG, COLLECT) fall back
   to the representative row's value. §6.3's "one fold implementation" still
   stands as the production requirement.
4. **PRIORITY / HAVING / BEST BY are not applied to the snapshot**, per R6.
5. **`MonotonicAggState` is not updated on this path**, so convergence rests
   entirely on the whole-row change test. That is the intent, but it should be
   made explicit rather than incidental.
6. **No performance measurement.** `merge_fold_contributions` rebuilds a single
   concatenated batch per iteration, O(facts) rather than O(delta). Correct,
   but unbenchmarked.

### Bearing on §9

The spike does not settle nesting vs flattening — it *implements* nesting, and
the whole existing suite accepts it. That is evidence the choice is safe, not
evidence it is the intended semantics. The decision in §9 still has to be made
and written down.

---

## 11. Dual-view handles and ALONG (2026-08-07)

The spike's two structural shortcuts are closed. `DerivedScanView` now
distinguishes the two shapes a recursive rule's facts take during iteration,
handles are keyed by `(rule_name, is_self_ref, view)`, and each *clause* picks
its own view.

### What changed relative to §10

**Dual-view handles.** `DerivedScanView::{Contributions, Folded}` lives on both
`DerivedScanHandle` and `DerivedScanEntry`. Both views of one rule can be live
simultaneously, so the folded snapshot no longer overwrites the buffer that
other consumers depend on. That retires R2/R3/R5:

- `convert_is_refs` resolves `IsRefBinding::derived_scan_index` to the
  **contributions** entry explicitly. That index drives the IS NOT anti-join /
  probabilistic complement and provenance's `base_fact_id` hashes, both defined
  over pre-fold rows — anti-joining a one-row-per-KEY snapshot would
  under-filter, and its row hashes would never match the ones
  `record_provenance` stores.
- `find_clause_for_row`'s arity guard and `body_support_map`'s pre-fold row
  hashes never see a folded batch, because nothing writes one into the
  contributions entry.

**The fold-view decision moved out of the plan and into the registry.**
`FixpointRulePlan::fold_self_ref` is gone. `run_fixpoint_loop` enables a rule's
folded snapshot when some registry entry for that rule has `view == Folded`, so
the planner is the single authority and the two places cannot drift.

**ALONG is per clause, not per rule.** The spike disabled the folded view for
any rule with an ALONG clause anywhere. Now ALONG opts out only *its own*
clause, and a sibling clause that folds an inherited value still gets the
folded view. `issue_162_along_on_a_sibling_clause_keeps_the_folded_view` pins
it: the base clause carries `ALONG b = s.r` while the recursive clause folds,
and `TOP` is 0.25 — the spike returned 0.5 there.

### The ALONG precedence rule, and why it is not a workaround

A clause that carries ALONG keeps the contributions view even when it also
folds an inherited column. This is a semantic decision, not a limitation of the
plumbing:

- `prev.x` is rewritten to a bare column reference over the same scan, so one
  handle cannot serve a per-path value to `prev` and a per-KEY aggregate to the
  fold.
- More fundamentally, the two are contradictory. ALONG aggregates *across
  paths*; "the child's folded value" is not defined per path. A clause asking
  for both is asking a row to be a path and an aggregate at once.

So `ALONG total = prev.total + e.w  FOLD total = MSUM(total)` — the shape
`ProbabilisticStress` and `phase_b_f1_fires_even_when_along_present` use — keeps
exactly its current behaviour, which is the correct behaviour for path carry.

### Gates

| gate | result |
|---|---|
| `issue_162_shape_matrix` (S1–S7) | 0 wrong |
| `issue_162_along_on_a_sibling_clause_keeps_the_folded_view` | passing (0.25; was 0.5 under the spike) |
| `issue_162_cyclic_recursive_fold_terminates` | passing, 0.19 s |
| `uni-db` + `uni-query` + `uni-locy` + `uni-plugin-builtin` + `uni-cypher` | **3712 passing, 0 failed** |
| Locy TCK | **514 passing, 0 failed** |
| `cargo clippy --all-targets` / `cargo fmt` | clean |

Diff: +407 / −10 across `locy_fixpoint.rs`, `locy_program.rs`,
`locy_planner.rs`.

### Still open

1. **The fold is still not `FoldExec`** (§10 gap 3). Aggregation goes through
   `LocyAggregate::update_step`, so aggregates with no row-level fast path
   (AVG, COLLECT) fall back to the representative row's value in the snapshot.
   Their *final* value is unaffected — `apply_post_fixpoint_chain` still runs
   `FoldExec` — but a same-stratum consumer of such a rule would read a
   representative rather than an aggregate. Routing the snapshot through
   `FoldExec` is the remaining correctness item.
2. **PRIORITY / HAVING / BEST BY are not applied to the snapshot** (R6).
3. **No performance measurement.** `merge_fold_contributions` rebuilds one
   concatenated batch per iteration — O(facts), not O(delta).
4. **`MonotonicAggState` is not updated on the fold-view path**, so convergence
   rests entirely on the whole-row change test. Intended, but it should be
   asserted by a test rather than left incidental.
5. **§9 is still open.** The implementation commits to nesting; the spec has
   never stated a semantics, and it should now say one.

---

## 12. Completion (2026-08-07)

The §11 gaps are closed. **Nesting is the decided semantics** and is now written
down rather than merely implemented.

### W1 — the snapshot folds with `FoldExec`

`recompute_folded` no longer hand-rolls aggregation through
`LocyAggregate::update_step`. It now builds a `MemorySourceConfig` over the
accumulated contributions, wraps it in `PriorityExec` when the rule carries
PRIORITY (which must precede FOLD, or `FoldExec` drops `__priority`), wraps that
in `FoldExec::new_with_semiring`, and collects.

`FoldExec` emits **KEY plus fold columns only**, so its output is grafted back
onto a representative row per KEY rather than used directly — that keeps the
contribution schema the deriv-widened self-ref scan declares. A fold whose
output type cannot be represented in the contribution column (`COLLECT` → List)
now **fails loudly**, naming the rule and pointing at the cross-stratum
workaround, instead of silently substituting the representative row's value.

`provenance_tracker`, `top_k_proofs_k` and `body_support_map` are deliberately
not threaded: all three are post-fixpoint-only, and `body_support_map` in
particular is keyed by a full-column row hash over an unconverged registry.

### W2/W3 — boundary and convergence pinned by test

- `issue_162_having_is_not_applied_to_the_per_iteration_snapshot` — `MID`
  (0.75) is filtered from the answer by `WHERE b > 0.8` yet must still have
  been visible to `TOP` (0.875) while the fixpoint ran. Applying HAVING per
  iteration would erase `TOP` entirely.
- `issue_162_a_moved_child_value_replaces_the_stale_contribution` — a staggered
  DAG where `MID` folds to 0.5 from one child before gaining a second. `TOP`
  must re-derive *over* its earlier row: 0.25, not `MPROD(0.5, 0.25) = 0.125`.

### W4 — scaling guard

`issue_162_fold_view_work_stays_within_its_documented_shape` reconstructs total
fold work (`Σᵢ factsᵢ`) from the profile's per-iteration `delta_facts` over
chains of depth 10/20/40. Measured **100 → 295 → 985**, i.e. 9.85× for a 4×
depth increase against a 16× quadratic asymptote; all three depths run in
0.32 s. The guard sits at 1.5× the asymptote. The O(facts) snapshot is
comfortably within budget, so the incremental-update contingency was not needed.

### W5 — TCK, 514 → 519

- `MonotonicAggregation.feature` — MPROD across two and three levels with
  equal-valued siblings, and **MCOUNT counting children, not leaves** (`MID` = 2,
  `TOP` = 1). MCOUNT is the aggregate where nesting and flattening genuinely
  disagree, and it is not associative, so no coincidence hides the difference.
- `PathCarriedValues.feature` — ALONG on one clause leaves a sibling clause
  folding the folded value.
- `fold/Aggregation.feature` — the contrast: a later-stratum consumer folds the
  published folded value, so bag-of-derivations still describes a non-recursive
  fold.
- `FoldInRecursivePath.feature` — the scenario titled "ALONG suppresses the
  warning" never asserted suppression, only that evaluation succeeded, which is
  why the mismatch with `phase_b_f1_fires_even_when_along_present` survived. It
  is retitled and now asserts the warning fires.

### W6/W7 — semantics written down

New: *What a self-reference reads* in `website/docs/locy/rule-semantics.md`
(the declarative statement that never existed), a recursive-FOLD section in
`advanced/along-fold-bestby.md`, two `troubleshooting.md` entries, and a
Black Book architecture note covering `DerivedScanView`, the replace-not-append
merge and value-based convergence — the derived-scan registry was previously
undocumented there entirely.

Corrected: the flattening prose in `complete_locy.md` (split by scope) and
`skills/uni-db/references/locy.md`; the `FoldInRecursivePath` message and its
three doc copies. Absorbed while in those passages: `complete_locy.md:622`
"guarantees fixpoint convergence" (contradicted `rule-semantics.md:60`), the
§8.2 aggregate table predating the registry oracle, the delta-emptiness
convergence description, and the `MonotonicAggState` caveat.

### Gates

| gate | result |
|---|---|
| `uni-db` + `uni-query` + `uni-locy` + `uni-plugin-builtin` + `uni-cypher` | **3715 passing, 0 failed** |
| Locy TCK | **519 passing, 0 failed** |
| `cargo clippy --all-targets`, `cargo fmt`, `RUSTDOCFLAGS=-D warnings cargo doc` | clean |
| Test-binary cap | only `uni-tck = 4`, pre-existing and tracked as R2 elsewhere |

`ProbabilisticStress:212` (0.58 / 0.84), `ExactProbability` (0.79 / 0.21),
`MonotonicAggregation` (3.0 / 2.0 / 3) and the five `locy_issue_159` tests are
unchanged throughout, including across the W1 switch to `FoldExec`.

### Deferred by decision

Versioning and release notes (3.3.0 remains set, unpushed, untagged, with no
release notes), issue #161, `uni-tck`'s 4th binary, and the broader
documentation-remediation backlog.
