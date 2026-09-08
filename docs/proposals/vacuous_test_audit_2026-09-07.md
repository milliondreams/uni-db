# Vacuous-fixture test audit (#205)

**Date:** 2026-09-07
**Issue:** [#205](https://github.com/rustic-ai/uni-db/issues/205) — "Audit for
vacuously-passing tests: the IC14 test never executed the code it covered"

## What was already done, and what this closes

#205 proposed three pieces of work. Only the point fixes had landed:

| # | Proposed | Status before this audit |
|---|---|---|
| 1 | Audit tests whose fixture may not reach the feature under test | **not done** |
| 2 | Where a test covers a computation, assert the computed value | done for IC14 (`start_end_node_test.rs`) and for the D7 repro (`correctness_repros.rs::repro_08_pattern_comprehension_colorder`) |
| 3 | A fixture assertion helper that fails loudly instead of passing vacuously | **not done** |

This document is (1). The helper is (3), and (2) is extended to the tests the
audit found.

## The defect signature

A test builds a **real** query whose pattern requires certain labels and edge
types; the **fixture** never creates them. The pattern matches nothing, the
inner expression never evaluates, and the test passes without executing the code
it was written for.

Two properties make it invisible, and both are general:

* **The query text is real.** A reviewer asking "does this test the real query?"
  gets yes. The reduction is in the *data*, which is not visible beside the
  query.
* **Vacuous truth reads as success.** A comprehension over an empty match yields
  an empty list and `reduce` returns its seed — no error anywhere — so every
  assertion that avoids the computed value passes on its own terms.

## Method

Two independent sweeps over `crates/*/tests/` and `#[cfg(test)]` modules:

1. pattern comprehensions (`[(a)-[r]->(b) WHERE … | expr]`);
2. `startNode`/`endNode`, `reduce`, `shortestPath`/`allShortestPaths`, list
   comprehensions and quantifiers over possibly-empty lists.

For each test: what the expression needs before its body can evaluate once, what
the fixture actually creates, and whether any assertion checks the **computed
value** rather than a row count, a column name, or the absence of an error.

Roughly 150 tests were examined. The sweeps overlapped on the pattern-
comprehension files and agreed on every shared verdict.

## Findings

**No undisclosed VACUOUS test remains.** IC14 — the one #205 was filed for — was
already fixed. What the audit found is the weaker neighbouring class: tests whose
pattern *does* match but whose assertions never look at the result.

### Fixed here

| file:line | test | was | now |
|---|---|---|---|
| `cypher_path/pattern_comprehension_test.rs:107` | `test_pattern_comprehension_path_variable` | **no assertion at all** — matched on `Ok`/`Err` and printed either; an error passed identically | asserts the query succeeds and that the per-row path-list lengths are `[0, 1, 1]` |
| `cypher_path/pattern_comprehension_test.rs:10` | `test_pattern_comprehension_basic_traversal` | `results.len() == 3`; list contents printed to stderr | asserts each person's friend list |
| `cypher_path/pattern_comprehension_test.rs:55` | `test_pattern_comprehension_node_property` | `results.len() == 3` | projects `n.ext_id` and asserts each row's list, including the NULL element |
| `cypher_path/pattern_comprehension_test.rs:81` | `test_pattern_comprehension_edge_property` | `results.len() == 3`; nodes created anonymously so no assertion was *possible* | fixture gives the nodes `ext_id`; asserts each row's list |
| `cypher_read/unanchored_pattern_comprehension_test.rs:304` | `a_correlated_comprehension_runs_once_per_row_by_necessity` | `subquery_executions` only — **and its predicate `a.name > n.name` matched nothing on this fixture**, so every row's list was empty and a wrongly-hoisted comprehension produced the same all-empty answer | predicate flipped to `<` so rows genuinely differ; asserts both the metric and the per-row values |
| `cypher_read/unanchored_pattern_comprehension_test.rs:344` | `a_list_comprehension_over_its_own_binding_is_still_uncorrelated` | `subquery_executions == 1` only | also asserts the element `[2, 3]` |
| `cypher_read/unanchored_pattern_comprehension_test.rs:363` | `a_reduce_that_reads_the_outer_row_stays_correlated` | metric + row count | also asserts the reduce result |
| `cypher_read/unanchored_pattern_comprehension_test.rs:386` | `a_subquery_in_the_body_still_takes_the_per_row_path` | metric only | also asserts the projected value |
| `cypher_read/unwind_source_pruning_test.rs:296` | `a_list_read_in_a_pattern_comprehension_survives_the_unwind` | `rows.len() == 2` only | asserts the comprehension's list; see the note below on why this took a fixture change |

The `:304` finding is the sharpest: a test written to catch an over-eager hoist
could not have caught one by value, because its own predicate rejected every
candidate. The metric carried the whole test.

### Examined and left alone, with reasons

* **`bugs/deleted_vertex_label_resurrection.rs:110`**
  (`deleted_person_not_seen_via_pattern_comprehension`). One sweep flagged this
  VACUOUS. **It is not a #205 instance.** The file's module docstring already
  states that these probes "pass both before and after the fix", explains the
  masking mechanism (cascade tombstoning plus edge-tombstone filtering), and
  names the white-box test in `uni-query` that *is* red without the guard.
  #205 is about a docstring that claims coverage the test does not have; this one
  claims the opposite. Disclosed limitation, not a hidden one.
* **Deliberate empty-result tests** — `test_pc_isolated_node`,
  `test_pc_nonexistent_edge_type`, `test_pc_empty_list_no_outgoing`,
  `unanchored_with_no_matches_is_an_empty_list`, and the quantifier
  vacuous-truth tests. The empty result *is* the contract, and each file carries
  positive controls beside them.
* **`cypher_path/qpp_group_variables.rs:357`** — inspects no result, but its
  subject is that a set of QPP group-variable usages parse and execute at all.
  Worth strengthening; not this class.

### A note on the `unwind_source_pruning` fix

This one was first set aside as unfixable-without-flakiness, and that judgement
was wrong — worth recording, because the wrong reasoning is the tempting one.
The comprehension is `[(x:P {name: head(names)})-[:KNOWS]->(y) | y.name]`, and
`names` is a `collect(DISTINCT ...)` whose order is not pinned, so `head(names)`
is 'b' or 'c' unpredictably. That much is true, and it does rule out asserting a
value that depends on *which* head is picked.

But on the bare fixture neither 'b' nor 'c' has an outgoing `:KNOWS`, so the
comprehension returns an empty list either way. Deterministic — and identical to
what a comprehension that never ran returns, which is the defect class itself.
The fix is to make the two arms *symmetric*: give 'b' and 'c' the same outgoing
neighbour, and the result is `['d']` whichever head is chosen. Non-empty,
order-independent, and now able to fail.

Stopping at "the order is not pinned" would have left a test that cannot
distinguish success from the bug, with a plausible-sounding reason attached.

## The helper (#205 item 3)

Two pieces, mirroring how `plan_shape` is split so both crates' tests can use the
core:

* **`uni_cypher::pattern_requirements`** — from the query text alone, extracts
  the labels and edge types its patterns need. It walks **pattern comprehensions
  inside expressions**, which is the whole point: IC14's requirement lives in an
  expression, so an extractor that only visited `MATCH` clauses would have
  reported exactly the entities that fixture already had.
* **`fixture_shape::assert_pattern_reachable(&session, query)`** (in
  `crates/uni/tests/common/`) — counts rows for each requirement and panics
  naming the empty ones.

Deliberately **not** required, each because an empty match is legitimate there:
`CREATE`/`MERGE` patterns, `OPTIONAL MATCH`, `EXISTS`/`COUNT`/`COLLECT`
subqueries, and variable-length hops that start at zero (`[:T*0..]` — IC14's own
`allShortestPaths` hop is written that way).

### What it proves, and what it does not

A pass says the **ingredients** exist, not that they **compose** into a match:
every label and edge type can be non-empty while no path threads them together.
It is a necessary condition, chosen because it is mechanical, needs no engine
observable, and catches the signature that actually occurred. The sufficient
condition is asserting the computed value, and the helper is not a substitute —
the IC14 test now carries both, and that pairing is the pattern to copy.

The helper also fails when a query turns out to require **nothing**, so it cannot
itself become a no-op that reads like coverage.

### Verified discriminating

`fixture_shape::tests` reconstructs the pre-#205 IC14 fixture — two `Person`s and
one `KNOWS` edge, with `Comment`/`Post`/`HAS_CREATOR`/`REPLY_OF` *declared but
empty* — and asserts the helper rejects it; a positive twin adds only the missing
rows and passes. Declaring-but-not-populating is deliberate: it proves the helper
fires on empty tables, not merely on unknown names.

One overclaim was found and removed during that work. The helper originally
reported an unknown label as "not declared in the schema", assuming the count
query would error. Measured, `MATCH (n:Nonexistent)` returns zero rows instead,
so an empty table and a typo are indistinguishable from the probe's side. The
message now says exactly that.

## Retrofit recipe

1. Does the test depend on a pattern matching? Add
   `fixture_shape::assert_pattern_reachable(&session, query).await;`.
2. Does it exist to cover a computation? Assert the **value**, not the row count.
   A row count is satisfied by N empty lists, which is precisely what a
   non-matching comprehension returns.
3. If the natural assertion would be order-dependent, project an identifying
   column rather than dropping the assertion.
