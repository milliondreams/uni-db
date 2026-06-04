# Code Simplifier Review — `crates/uni-tck/`

Scope: Cypher TCK harness sources under `src/` and `tests/tck.rs`.
Focus: duplication, dead code, repeated test patterns. No files modified.

---

## 1. Major Duplication

### 1.1 Four near-identical `match_result_*` functions
**File:** `src/matcher/result.rs:7-195`

`match_result`, `match_result_unordered`, `match_result_ignoring_list_order`, and
`match_result_unordered_ignoring_list_order` are 40-50 lines apiece and differ only by:
- ordered vs. unordered row matching
- which value-equality predicate they call (`values_equal` vs.
  `values_equal_ignoring_list_order`)

The row-count check, `is_empty` short-circuit, `validate_columns` call, and the
"unmatched-rows" loop are duplicated four times.

**Suggestion:** Collapse into a single `match_result_with(actual, expected, opts)`
where `opts` carries `{ordered: bool, ignore_list_order: bool}` and the equality
predicate is selected once. The four public functions become 2-line wrappers. Cuts
~150 LOC and centralises mismatch-error formatting.
**Effort:** ~45 min.

### 1.2 Parallel `*_ignoring_list_order` value/maps/nodes/edges/paths helpers
**File:** `src/matcher/result.rs:248-416`

`values_equal` / `values_equal_ignoring_list_order`, `maps_equal` /
`maps_equal_ignoring_list_order`, `nodes_equal` / `nodes_equal_ignoring_list_order`,
`edges_equal` / `edges_equal_ignoring_list_order`, `paths_equal` /
`paths_equal_ignoring_list_order` are pairs that branch only on list ordering.

**Suggestion:** Single `values_equal_with(a, b, ignore_list_order)` recursing with
the same flag. Removes ~100 LOC and the chance of one variant drifting from the
other (e.g., the `Temporal`/`Vector` cross-arms are already present in both but
were clearly copy-pasted).
**Effort:** ~30 min.

### 1.3 Six near-identical `then` step handlers
**File:** `src/steps/then.rs:20-112`

`result_should_be_in_any_order`, `result_should_be_in_order`,
`result_should_be_ignoring_list_order`, `result_should_be_in_order_ignoring_list_order`,
`result_should_be_in_any_order_ignoring_list_order` each repeat the same
five-statement preamble (None-check, error panic, table parse) before dispatching
to a matcher. Note also that `result_should_be_in_order_ignoring_list_order`
(line 75) and `result_should_be_ignoring_list_order` (line 55) currently call the
**same** matcher (`match_result_ignoring_list_order`) — almost certainly a bug
where the "in order" variant should be distinct, or the two should be merged.

**Suggestion:** Extract `fn require_result_and_table(world, step) -> (&QueryResult, Vec<...>)`
and a `dispatch_match(opts)` helper. After 1.1, each `#[then]` becomes 3-5 lines.
**Effort:** ~25 min (and fixes the suspected duplicate-matcher bug).

### 1.4 Three identical `when` step handlers
**File:** `src/steps/when_step.rs:20-81`

`executing_query`, `executing_control_query`, and `executing_query_with_params`
are byte-for-byte identical in body (capture-before, `execute_via_tx`, set
result/error, capture-after). The "with parameters" variant doesn't even read
the parameter list from the regex — it relies solely on params previously
registered via the `parameters_are` step, so the regex's `(.+)` capture group
is unused.

**Suggestion:** One private helper `async fn run_query(world, step)` called by
all three thin `#[when]` attribute functions. Drop the unused regex capture or
parse it. Cuts ~40 LOC.
**Effort:** ~15 min.

### 1.5 Duplicated `Given` "init db" handlers
**File:** `src/steps/given.rs:5-19`

`an_empty_graph` and `any_graph` have identical bodies. They could share
one helper, or use a single regex `^(an empty|any) graph$`.
**Effort:** ~5 min.

### 1.6 Repetitive side-effect arms
**File:** `src/steps/and.rs:36-150`

The eight match arms (`+nodes` / `-nodes` / `+relationships` / `-relationships` /
`+labels` / `-labels` / `+properties` / `-properties`) each follow the same
shape: read a counter from `effects`, `assert_eq!` against `expected`, format an
identical context message. The label arms additionally compute a `difference()`.

**Suggestion:** Build a small table `[(&str, fn(&SideEffects) -> (i64, String))]`
mapping each header to (actual_value, debug_context). The match becomes a lookup
and a single `assert_eq!`. Cuts ~80 LOC and makes the debug context format
guaranteed-consistent.
**Effort:** ~30 min.

---

## 2. Smaller Duplication

### 2.1 `edge` vs `edge_in_path` parsers
**File:** `src/parser/value.rs:204-242`

Two functions differ only by the `if edge_type.is_none() { Err }` guard in
`edge`. The shared bracket parser already exists (`parse_edge_brackets`).

**Suggestion:** Single `parse_edge(input, require_type: bool)` parameterised on
that flag.
**Effort:** ~5 min.

### 2.2 `nodes_equal` label-matching block duplicated
**File:** `src/matcher/result.rs:363-371` and `397-405`

The `labels_match` computation (empty-empty short circuit + length + subset
check) is duplicated verbatim between `nodes_equal` and
`nodes_equal_ignoring_list_order`. Eliminated naturally by 1.2.

### 2.3 `collect_node_ids` and `collect_edge_ids`
**File:** `src/world.rs:322-355`

Both functions are identical except for the Cypher query and the comment.

**Suggestion:** `async fn collect_ids(&self, query: &str) -> HashSet<u64>`. Cuts
~15 LOC.
**Effort:** ~5 min.

### 2.4 Procedure step: table parsing repeats `parse_table` logic
**File:** `src/steps/procedure.rs:126-149`

The `data` table parsing (header row, body rows, parse each cell with
`parse_value`, default to `Null`) re-implements the logic in
`parser/table.rs:41-64`. Differences: it tolerates an empty single-row table.

**Suggestion:** Either reuse `parse_table` (after extending it to tolerate empty
single-row), or factor a shared `parse_named_rows(&Table) -> Vec<HashMap<…>>`.
**Effort:** ~10 min.

### 2.5 `*_separator_patterns` linear search
**File:** `src/steps/procedure.rs:73-83`

The four patterns `") :: ("`, `")::("`, `") ::("`, `"):: ("` differ only in
whitespace around `::`. A single regex `\)\s*::\s*\(` would replace the loop,
the manual `sep_len` tracking, and the `split_pos` Option.
**Effort:** ~10 min.

---

## 3. Dead / Suspect Code

### 3.1 Unused parameter on `ignored_scenario_reason`
**File:** `tests/tck.rs:153-169`

`_scenario_line` and `_schema_mode` are both unused; the function only checks a
single hard-coded feature path/scenario name. Either thread them through or
drop them from the signature (and from the call site at line 117-120).
**Effort:** ~5 min.

### 3.2 `executing_query_with_params` ignores its regex capture
**File:** `src/steps/when_step.rs:62`

The `(.+)` capture in `executing query with parameters (.+):` is never passed
to the handler (the function signature has no `String` arg for it). Either the
capture group is dead and should be a non-capturing `(?:.+)` (or removed
entirely), or the step intends to parse inline params and is silently buggy.
**Effort:** ~5 min to confirm and clean up.

### 3.3 `value_sort_key` partially handles `Value::Vector` out of order
**File:** `src/matcher/result.rs:291-308`

Sort-key prefixes are `0..A` and `C`, with `Vector` assigned `B` — keys are not
in alphanumeric order with the variants above them. Functionally harmless
(unique prefixes are all that matters) but confusing on read.
**Effort:** ~2 min cosmetic fix.

### 3.4 `_temp_dir` field never populated
**File:** `src/world.rs:53,130`

`UniWorld::_temp_dir` is declared and `Debug`-printed but never assigned (the
ctor sets it to `None`, and no other code writes to it). Either wire it up to
the `Uni::in_memory()` path or drop the field.
**Effort:** ~5 min.

### 3.5 `result_should_be_in_order_ignoring_list_order` calls wrong matcher
**File:** `src/steps/then.rs:75-92`

This handler is for the "in order" variant but invokes
`match_result_ignoring_list_order` — the same function called by
`result_should_be_ignoring_list_order` at line 55. Look genuine: should likely
be `match_result_ignoring_list_order` for order-sensitive and there is no
distinct unordered helper used here. Worth verifying against the TCK spec.
**Effort:** ~10 min investigation.

---

## 4. Repeated Test Patterns

### 4.1 `Vid::from(N)` + empty `HashMap::new()` boilerplate
**File:** `src/matcher/result.rs:459-535`

Five `nodes_equal*` tests construct `Node { vid: Vid::from(N), labels: vec![…], properties: HashMap::new() }`
with only the labels vector varying. The `use uni_common::core::id::Vid;` line
is repeated in every test.

**Suggestion:** Move the `use` to the `mod tests` top, and add a small builder
`fn node_with_labels(vid: u64, labels: &[&str]) -> Node`.
**Effort:** ~10 min.

### 4.2 `parse_value` tests share an "extract or panic" pattern
**File:** `src/parser/value.rs:368-556`

Tests like `test_parse_list`, `test_parse_map`, `test_parse_*_node`, and the
seven path tests all repeat the `if let Value::Variant(x) = parse_value(...).unwrap() { ... } else { panic!(...) }`
pattern.

**Suggestion:** Helper macros or `fn expect_path(s: &str) -> Path` /
`expect_node(s: &str) -> Node` / `expect_list(s: &str) -> Vec<Value>` would
collapse each test by 3-4 lines and remove the duplicated panic-message strings.
**Effort:** ~15 min.

### 4.3 `procedure.rs` tests reconstruct the signature parse triplet
**File:** `src/steps/procedure.rs:181-239`

Four tests destructure `(name, params, outputs)` and individually assert each
field. A small `assert_proc_sig(sig, expected_name, expected_params, expected_outputs)`
would clarify intent and reduce noise.
**Effort:** ~10 min.

---

## 5. Other Observations

### 5.1 `extract_error_from_output` re-scans the full output
**File:** `tests/tck.rs:386-421`

Iterates lines, then for the first match runs `output.find(trimmed)` to recover
the byte offset — but the line iterator already has it via line lengths. Minor
allocation issue: `relevant.to_string()` clones a substring that's then thrown
away if it fits the truncation.

**Suggestion:** Track the running byte offset in the `for line in output.lines()`
loop; return `&output[offset..]` and truncate once.
**Effort:** ~10 min.

### 5.2 `classify_phase` second-pass logic is awkward
**File:** `src/matcher/error.rs:84-128`

The function returns `base_phase`, but mid-function checks may override it via
two unrelated detail-code passes. Three early returns plus a fallthrough makes
the control flow hard to follow.

**Suggestion:** Compute `base_phase`, then `if expected == Runtime && (detail_is_runtime || extracted_is_runtime) { return Runtime }`, then handle the `_cypher_in` compile-time override as the very first guard. Single linear flow.
**Effort:** ~15 min.

### 5.3 `add_entity_to_snapshot` closure captures `prefix` by ref but inserts via `format!`
**File:** `src/world.rs:286-319`

`insert_props` is a closure that builds `format!("{}:{}:{}", prefix, id, k)`
for every property. The `prefix` here is a `&str` so cheap, but the entire
closure could be a free function `fn insert_props(snapshot, prefix, id, props)`
— closures over `&mut HashMap` here add no value and cost readability.
**Effort:** ~5 min.

### 5.4 `_scenario_line` parameter typo / drift risk
**File:** `tests/tck.rs:115-120, 158`

`run_single_scenario` takes `scenario_line: usize` and uses it; the ignored-
reason check takes `_scenario_line` and never uses it. If the ignore logic ever
needs to differentiate expanded outline rows, the parameter is in the wrong
place. Document or remove.
**Effort:** ~2 min.

### 5.5 `make_test_name` could collide silently when scenarios contain `__`
**File:** `tests/tck.rs:299-302`

`scenario_name.replace("::", "__")` is a one-way mapping. If a scenario
literally contains `__`, the test name and a `::` scenario alias to the same
key, which is then disambiguated by `@L<line>` only when both exist. Low risk
but worth noting in the doc-comment.

---

## Summary of Estimated Wins

| Theme | LOC removed | Effort |
|---|---|---|
| Matcher result fns (1.1) | ~150 | 45 min |
| `*_ignoring_list_order` helpers (1.2) | ~100 | 30 min |
| `then` step handlers (1.3) | ~70 | 25 min |
| `when` step handlers (1.4) | ~40 | 15 min |
| `and.rs` side-effect arms (1.6) | ~80 | 30 min |
| Smaller dedup (2.1-2.5) | ~60 | 35 min |
| Dead code (3.1-3.5) | ~30 | 30 min |
| Test pattern helpers (4.1-4.3) | ~50 | 35 min |
| **Total** | **~580 LOC** | **~4 hours** |

Total source under review is ~3,227 LOC, so a full pass at ~18% reduction is
realistic without altering any test semantics. All proposed changes are
purely structural and preserve cucumber/nextest behaviour, error messages
(in spirit), and Cypher TCK conformance.
