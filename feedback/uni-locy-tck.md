# Code Simplifier Review — `crates/uni-locy-tck`

Scope: ~4000 LOC of Rust support code for the Locy TCK (test compatibility kit).
Focus areas as requested: duplication, dead code, complex helpers, and repeated
patterns that could become helpers. **No files were modified.**

The TCK harness is generally well-factored — the parser, fixtures, and
test-name builder are clean. The largest opportunities are in the `then_*`
step handlers and the four near-identical match functions in `matcher/result.rs`.

---

## 1. Massive duplication of "warning code" assertion blocks

**Files / Lines:**
- `src/steps/then_evaluate.rs:881-1028` (6 functions, ~150 lines)

**Description:** Three pairs of `should_contain` / `should_not_contain` warning
assertions (`SharedProbabilisticDependency`, `BddLimitExceeded`,
`CrossGroupCorrelationNotExact`) follow an identical pattern: fetch result,
filter warnings by `RuntimeWarningCode`, assert presence/absence, format
diagnostic message. Adding a new warning code requires copying ~50 lines.

**Suggestion:** Introduce two private helpers (positive and negative variants)
parameterised by `code: RuntimeWarningCode` and an optional `rule_name`. Each
public `#[then(...)]` becomes a 2-3-line wrapper. Helpers can also use
`locy_result_or_panic(world)` (see item 4) so the boilerplate around
`world.locy_result().expect(...).as_ref().expect(...)` collapses.

**Effort:** S (~30 min). High readability win, removes ~100 lines.

---

## 2. Four near-identical `match_result_*` variants

**Files / Lines:**
- `src/matcher/result.rs:7-195` (`match_result`, `match_result_unordered`,
  `match_result_ignoring_list_order`, `match_result_unordered_ignoring_list_order`)

**Description:** The four entry points combine two orthogonal flags
(ordered vs unordered × strict-list-order vs ignoring-list-order). Each
re-implements the same row-count check, empty short-circuit,
`validate_columns` call, and (un)ordered comparison loop, differing only by
which value-equality function they call (`values_equal` vs
`values_equal_ignoring_list_order`).

**Suggestion:** Either (a) introduce a single private
`match_result_impl(actual, expected, ordered: bool, ignore_list_order: bool)`
and have the four public functions delegate, or (b) take a `cmp: fn(&Value,
&Value) -> bool` plus a `ordered: bool` flag. Similarly,
`values_equal_ignoring_list_order` (lines 309-346) duplicates most arms of
`values_equal` (lines 248-275); only the `List` and `Map` arms differ. A
single function taking an `ignore_list_order: bool` would halve the surface
area, as would unifying `nodes_equal` / `nodes_equal_ignoring_list_order`
(identical except for the recursive comparator) and the same for
`edges_equal`, `paths_equal`, `maps_equal`.

**Effort:** M (~1-2 h). Eliminates ~150 lines and one whole class of
"forgot to update the other variant" bugs.

---

## 3. Repeated boilerplate in `command_result_*` assertions

**Files / Lines:**
- `src/steps/then_evaluate.rs:418-781` (10 functions, ~360 lines)

**Description:** Every `command_result_*` step repeats:

```
let locy_result = world.locy_result().expect(...);
let result = locy_result.as_ref().expect(...);
let cmd = result.command_results.get(idx).unwrap_or_else(...);
match cmd { CommandResult::X(...) => {...}, other => panic!(...) }
```

The match arm's `other => panic!("Expected command result {idx} to be a X,
got {other:?}")` is structurally identical in every function.

**Suggestion:** Add two helpers:

1. `fn get_command<'a>(world: &'a LocyWorld, idx: usize) -> &'a CommandResult`
   that performs the result-result-index lookup.
2. A macro (or a generic helper using a closure) `expect_cmd!(cmd, Query(rows) => { ... }, "Query")`
   that handles the type-mismatch panic uniformly.

Pure-function alternative: `fn as_query(cmd: &CommandResult, idx: usize) ->
&[Row]` (one per variant) which panics with the consistent message; the
caller writes the actual assertion. Either form removes ~150 lines and
makes each step body the actual assertion, nothing else.

**Effort:** S-M (~45 min).

---

## 4. `world.locy_result().expect(...).as_ref().expect(...)` repeated ~30 times

**Files / Lines:**
- `src/steps/then_evaluate.rs` throughout (`evaluation_should_succeed`,
  `evaluation_should_succeed_with_timed_out`, `evaluation_should_fail`,
  `evaluation_error_should_mention`, `derived_relation_*`, every
  `command_result_*`, every warning assertion, etc.)

**Description:** The two-level `Option<Result<...>>` access is unwrapped
identically everywhere. The two phrasings — "succeeded then check value"
versus "failed then check error" — show up in 20+ functions.

**Suggestion:** Add `LocyWorld::expect_locy_ok(&self) -> &LocyResult` and
`expect_locy_err(&self) -> &UniError` on `world.rs`. The first replaces
~30 occurrences of the four-line idiom with one call. This also gives a
single place to standardise error messages
("No evaluation result found — did you forget `when evaluating ...`?").

**Effort:** XS (~15 min). Big readability win.

---

## 5. `parse_gherkin_value` (then_evaluate) duplicates the logic in `set_parameter` (given)

**Files / Lines:**
- `src/steps/then_evaluate.rs:23-40` (`parse_gherkin_value`)
- `src/steps/given.rs:55-72` (inline closure in `set_parameter`)

**Description:** Both convert a Gherkin literal (`'foo'`, `"foo"`, integer,
float, `true`/`false`, `null`) into a `Value`. `set_parameter` additionally
inlines the logic instead of delegating, and omits the `null` branch.

**Suggestion:** Move `parse_gherkin_value` to a shared module
(`src/parser/gherkin_literal.rs` or extend `src/parser/value.rs`) and call
from both sites. Bonus: `set_parameter` gains `null` support for free.

**Effort:** XS (~10 min).

---

## 6. Three near-identical `graph_should_*` Cypher helpers

**Files / Lines:**
- `src/steps/then_evaluate.rs:785-877`
  (`graph_should_contain_n_nodes_with_label`, `graph_should_contain_edge`,
  `graph_should_not_contain_edge`, `graph_should_not_contain_edge_type`)

**Description:** All four format a Cypher MATCH query, call
`world.db().session().query(&query).await.expect("graph query failed")`,
extract `cnt` as `i64`, and assert a count. The
`should_contain_edge` and `should_not_contain_edge` pair share the entire
query string verbatim, differing only in their assertion.

**Suggestion:** Helper `async fn graph_count(world: &LocyWorld, query: &str)
-> i64`. Then collapse the contain/not-contain edge pair into one helper
parameterised by `expect_present: bool`. Reduces ~90 lines to ~35.

**Effort:** XS (~15 min).

---

## 7. `derived_relation_*` fact assertions — N×M combinatorial explosion

**Files / Lines:**
- `src/steps/then_evaluate.rs:189-376` (5 functions for 1-, 2-, 3-field
  positive & negative variants)

**Description:** Each "should contain fact where X = v" step is duplicated
for 1, 2, and 3 fields (and again as a negative). The 3-field variant is
flagged with `#[allow(clippy::too_many_arguments)]` which is itself a
smell. The bodies differ only in the count of `extract_field_value /
values_match` checks.

**Suggestion:** Use a single regex that captures `field = value` repeated
N times into one trailing `Vec<(String, String)>` (cucumber supports a
trailing `Step` with a docstring/data table). Alternatively, accept the
constraints as a data table directly — e.g.

```
Then the derived relation "Foo" should contain a fact where:
  | field | value |
  | name  | 'Bob' |
  | age   | 30    |
```

That eliminates 4 of the 5 functions, the `too_many_arguments` allow, and
all the parallel "and" regex variants. If a table is too invasive, factor
the row-match loop into a helper that takes `&[(String, String)]`.

**Effort:** M (~1 h, plus a feature-file sweep if the data-table form is
chosen). High structural payoff.

---

## 8. `when_evaluate.rs` — repeated config-build-and-evaluate boilerplate

**Files / Lines:**
- `src/steps/when_evaluate.rs:91-263` (six `when_evaluating_with_*` steps)

**Description:** Each variant repeats: extract docstring, `init_db`, build
a `LocyConfig { <one or two fields>, ..Default::default() }`, then
`world.db().session().locy_with(program).with_config(config).run().await`
followed by `apply_derived_and_store(...)`. About 25 lines per variant ×
6 variants = ~150 lines, of which ~135 are identical.

**Suggestion:** Helper

```rust
async fn run_locy_with(
    world: &mut LocyWorld,
    step: &cucumber::gherkin::Step,
    config: LocyConfig,
) { ... }
```

That gives each step a 5-line body. Optionally, a single `#[when]` regex
could accept comma-separated `key=value` config tweaks (e.g. `with
max_iterations=10 and exact_probability=true`) and parse them into a
config; that would shrink the six steps to one. Recommend the helper as
the safe minimum.

**Effort:** S (~30 min).

---

## 9. Duplicate `edge` / `edge_in_path` parsers

**Files / Lines:**
- `src/parser/value.rs:204-242`

**Description:** `edge` and `edge_in_path` already share
`parse_edge_brackets` (good), but the wrapper bodies still duplicate
construction of `Edge { eid, edge_type, src, dst, properties }`. The only
real difference is that `edge` rejects the no-type case.

**Suggestion:** Have `parse_edge_brackets` return an `Edge` (with
`edge_type` already extracted to a `String`); `edge` then becomes a thin
`verify`/`fail` wrapper that rejects empty type. Removes ~15 lines.

**Effort:** XS.

---

## 10. Likely dead / no-op code

**Files / Lines:**
- `tests/locy_tck.rs:153-161` — `ignored_scenario_reason` always returns
  `None`. All parameters are `_`-prefixed. Either delete the function and
  the call site at line 115, or leave a clear `TODO` comment. Currently
  reads like a placeholder that was never reactivated.

- `src/steps/given.rs:14-20` — `any_graph` and `an_empty_graph` are
  identical apart from the regex. Could be unified via `#[given(regex =
  r"^(?:any|an empty) graph$")]`. Marginal but reduces a near-duplicate.

- `src/world.rs:135-139` — Manual `impl Default for LocyWorld` that just
  calls `Self::new()` exists alongside `pub fn new()`. Since `World` is
  derived via `#[world(init = Self::new)]`, the manual `Default` impl is
  only needed if something else relies on `Default`; otherwise it's
  vestigial. (Low priority — verify with `cargo check` before removing.)

- `src/world.rs:66-86` — `impl Debug for LocyWorld` is hand-rolled; given
  most fields are `Debug` already, you could derive `Debug` after
  wrapping the `db: Option<Arc<Uni>>` with a `#[debug(skip)]`-style
  helper or via `derivative`. The manual impl is fine but ~20 lines.

**Effort:** XS each.

---

## 11. `value_sort_key` is order-fragile and over-engineered

**Files / Lines:**
- `src/matcher/result.rs:288-305`

**Description:** Generates string sort keys using prefixes `"0:"`,
`"1:"`, …, `"A:"`, `"B:"`, `"C:"` with an out-of-order alphabet
(`"A:path"`, `"B:len="`, `"C:..."`). Only used for sorting lists for
order-agnostic comparison.

**Suggestion:** Either (a) sort by a `(u8, ...)` tuple with a discriminant
function so the discriminant ordering is explicit and contiguous, or
(b) use a stable per-variant rank `enum` + `derive(Ord)` rather than
formatted strings. Comparing formatted floats with `{:020.10}` will
mis-sort negative numbers — `"-0000000000.0000000001"` sorts before
`"+0000000000.0000000001"` lexically but `-` actually compares less than
digits, so this may already be wrong for mixed-sign lists. Worth checking
with a test.

**Effort:** S.

---

## 12. Snapshot/diff in `world.rs::capture_state_after` is duplicated

**Files / Lines:**
- `src/world.rs:201-271` (`capture_state_before` / `capture_state_after`)

**Description:** Both methods share the snapshot-collection sequence
(node ids, edge ids, property snapshot, labels). The `_after` variant
adds diffing on top.

**Suggestion:** Private helper `async fn snapshot(&self) ->
(HashSet<u64>, HashSet<u64>, HashMap<String, Value>, HashSet<String>)`
returning all four. `capture_state_before` stores them; `capture_state_after`
stores them and computes the diff. Removes ~15 redundant lines and clarifies
intent.

**Effort:** XS-S.

---

## Summary of estimated wins

| Item | Effort | Approx LOC removed |
|------|--------|--------------------|
| 1. Warning code helpers | S | ~100 |
| 2. `match_result_*` unification | M | ~150 |
| 3. `command_result_*` helpers | S-M | ~150 |
| 4. `expect_locy_ok` accessor | XS | ~60 (across files) |
| 5. Shared gherkin literal parser | XS | ~20 |
| 6. `graph_count` helper | XS | ~55 |
| 7. Multi-field fact-where assertions | M | ~120 |
| 8. `run_locy_with` helper | S | ~135 |
| 9. Edge parser unification | XS | ~15 |
| 10. Dead-code trims | XS | ~30 |
| 11. `value_sort_key` cleanup | S | (correctness) |
| 12. `snapshot()` helper | XS | ~15 |

**Total realistic reduction:** ~800 lines from ~4000 (~20%) without
losing any TCK coverage, plus a real correctness fix in item 11 if the
mixed-sign float case is exercised. Highest-leverage items are #1, #2,
#3, #7, and #8.

## Notes on what NOT to change

- The cucumber step regex literals look verbose but are the public
  contract with the `.feature` files; collapsing them risks breaking
  scenarios. Helper extraction should keep the regexes intact.
- The hand-rolled `string` parser in `parser/value.rs` handles
  TCK-specific escapes; leave it alone.
- The libtest-mimic harness (`tests/locy_tck.rs`) is intricate but each
  function has a clear single responsibility (manifest, expansion,
  test-name, runner, JSON write); not worth restructuring.
