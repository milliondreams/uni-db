# Cleanup Audit — Sequenced Execution Plan

**Workspace:** `/home/rohit/work/dragonscale/uni` @ 3.1.0 · 64 findings adjudicated · 3 REFUTED, 18 PARTIAL, 43 CONFIRMED

The verifier corrected LOC, risk, or root cause on 21 items. Every number below is the **verifier's** figure, not the scout's. Read the "Do NOT do" section before starting — it contains four items where the naive version of a CONFIRMED finding is actively wrong.

---

## Implementation status

*Last updated 2026-07-31. Scope in flight: **Tier 0 only** (all 14). Tier 1–5 untouched.*

Every item below landed test-first: the regression test was written and **observed to fail** before the fix, then observed to pass. Where the test could not be made to fail, that is recorded as a correction rather than glossed over.

| Item | Status | Commit | Repro evidence before the fix |
|---|---|---|---|
| 0.1 IdAllocator substring NotFound | ✅ done | `eb35fe6db` | transient error whose text reads "not found" silently reset the allocator |
| 0.6 `UniSync::shutdown` runtime leak | ✅ done | `0821205dd` | **48 → 232 OS threads** over 8 shutdown cycles |
| 0.8 pool Mutex serializes instantiation | ✅ done | `2f0479579` | two-thread `Barrier` test **deadlocked**, tripped its 10 s timeout |
| 0.11 `value_to_py` nulls unknown variants | ✅ done | `0847283ef` | arm unreachable today; converted to loud failure (future guard) |
| 0.12 `PyForkStatus` fails open | ✅ done | `0ca8efb50` | arm unreachable today; now fails closed (future guard) |
| 0.10 async cursor stringifies `UniError` | ✅ done, **uncommitted** | — | async `fetch_one`/`fetch_many`/`async for` raised `RuntimeError`; sync twin raised `UniQueryError` |
| 0.13 pydantic `_result_to_model` swallow | ✅ done, **uncommitted** | — | a falsy-but-valid model **vanished** from `.all()`; a hydration `RuntimeError` vanished with it |
| 0.14 pydantic `eager_load` raw dicts | ⬜ next — 0.13 has landed, so it is unblocked | — | |
| 0.5 `VERSION AS OF` panics | ✅ done, **uncommitted** | — | `explain`/`profile`/`cursor` all **panicked** on a time-travel query |
| 0.3 wardedness parenthesized paths | ✅ done, **uncommitted** | — | parenthesising an identical pattern turned a legal rule into `WardednessViolation` |
| 0.9 algorithm yield types | ⬜ pending | — | |
| 0.7 plugin CypherValue transport | ⬜ pending | — | |
| 0.2 Locy `prev.X` in comparison | ⬜ pending | — | breaking; needs changelog |
| 0.4 ASSUME drops MODULE context | ⬜ pending, riskiest | — | has a stop-and-rescope gate |

Plus **D1/D2/D3** — three cursor-path bugs not in the original 84 findings, discovered while
reproducing 0.10 and fixed on request. See *Bugs discovered during implementation* below.

**Gate results after 0.11/0.12/0.10 + D1/D2/D3:** workspace `cargo nextest run --no-fail-fast`
(runbook exclusions) **6078/6078 passed** · uni-db Python suite **915 passed**, 15 skipped,
2 xfailed, 2 xpassed (was 910 pre-Tier-0) · `cargo fmt --check` and workspace
`clippy -D warnings` clean.

> **Running the gate:** use the runbook's exclusion list. A bare `cargo nextest run --workspace`
> does not fail your tests — it fails to *build*, pulling in `uni-xervo`, `candle-kernels`,
> `mistralrs-*` and a `gpu-metal` feature that are Apple-only or otherwise unbuildable here.
> Also capture `${PIPESTATUS[0]}`: piping cargo through `tail` reports `tail`'s exit code, which
> is always 0 and will silently disguise a red run as green.
>
> **Two perf tests fail under concurrent load and are not regressions.**
> `uni-plugin-rhai::guest_authored_mcts_meets_the_absolute_throughput_floor` (a throughput
> *floor*) and `uni-db::bugs::issue_55_instrumented::instrumented_get_edges_scaling` both failed
> when the Rust and Python suites ran simultaneously. Serially the first passes in 0.14 s and the
> second in 177 s against nextest's 180 s cap — inherently marginal. Do not run the two suites
> concurrently when judging a red run.

### Corrections to this document found while implementing

The audit was accurate on root cause everywhere, but four details were wrong or incomplete in ways that changed the work:

1. **0.8 overstated the win.** The document implies a 10–100 ms compile was serialized. It is not: `build_pool` caches the `InstancePre` (`loader.rs:955`), so the segment held under the guard is `instantiate` + `fresh_store`. Still a real defect — `max_instances = 4` delivered 1-way concurrency — but the commit message says so rather than repeating the larger claim.
2. **0.11's suggested Rust exhaustiveness test cannot exist.** `bindings/uni-db` builds with pyo3's `extension-module`, so libpython is not linked and no Rust unit test in that crate can start an interpreter. That is why no existing Rust test there touches Python. Behavioural coverage moved to `tests/test_value_conversion.py`; the Rust side keeps only a pure helper test.
3. **0.1's trap is cleared, but the harness needed extending.** `repro_10` stays green as predicted. However the existing `FaultStore` could not express the failing input at all — its injected error text never contains "not found", so it already propagated. `set_transient_message()` had to be added before the bug was reachable. Fixing first would have produced a test that passed against a fault that never triggered the defect.
4. **0.10's blast radius is narrower than "the cursor path".** `fetch_all` bypasses `next_row_async` entirely for `collect_remaining`, so it was always correct — spot-checking async cursor errors with `fetch_all` shows false parity. Only `fetch_one` / `fetch_many` / `__anext__` were affected. Also: the cursor stream is **single-shot** (`read.rs:1255` yields exactly one `Result`), so a runtime error always arrives on the *first* `next_batch()`. A test asserting "N good rows, then an error" cannot pass.

The honesty note on 0.10 was right to hedge: the "serialization conflict mid-stream" scenario remains undemonstrated, and the commit claims only type erasure. The reproducer used is integer division by zero on an `UNWIND`-supplied divisor.

### Bugs discovered during implementation, not in this audit — ✅ FIXED (uncommitted)

Found while searching for a 0.10 reproducer, then fixed on request. Filed as **D1/D2/D3**;
none was in the original 84 findings. All three live in
`execute_cursor_internal_with_config` and are the same defect wearing three hats: **the
cursor path enforced none of the limits the materializing path enforces**, while
`QueryBuilder::cursor` happily accepted all three options.

| | Bug | Before | After |
|---|---|---|---|
| D1 | `cancellation_token` dropped | all 1 999 000 rows | `Query cancelled` |
| D2 | `max_memory` inert | all 1 999 000 rows | `Query exceeded memory limit` |
| D3 | `query_timeout` inert | all 1 999 000 rows | `Query timed out` |

Same query, same options: `fetch_all` rejected all three, `.cursor()` honoured none.

**D1 needed more than plumbing.** The obvious fix — pass `self.cancellation_token` down —
would have changed nothing observable. A pre-cancelled token does not abort `fetch_all`
either: cancellation is purely cooperative and no scan/join/traverse plan reaches a
`GraphContext::check_timeout()` call. `test_concurrent_query_cancellation_isolation`
(`common/e2e/api_integration.rs:482`) already documents this from the other side — it
accepts a cancelled query "racing to completion" as a valid outcome. The cursor therefore
needs an outer guard racing the stream against the token, which is what landed.

**D3 needed two mechanisms, not one.** `tokio::time::timeout` alone does not enforce
anything here: it polls the inner future first and consults the timer only on `Pending`,
and an in-memory query finishes the whole plan in a single poll. `execute_plan_internal`
passes its 1 ns test because of a *second*, explicit `Instant::now() > deadline`
comparison afterwards. The cursor mirrors both, with the deadline check on the executor's
output batch rather than the re-chunked pieces so a slow consumer paging an
already-computed result is not charged against the query's execution budget.

**Regression caught by the test suite, then pinned.** `stream::unfold` panics if polled
after it yields `None`, and two ordinary patterns do exactly that — an empty result set,
and `fetch_one()` on a drained cursor (how Python reports "no more rows"). The previous
`map`/`flat_map` chain tolerated it, so the guard must be `.fuse()`d or the panic crosses
the pyo3 boundary as an abort instead of an exception.
`test_cursor_tolerates_polling_past_exhaustion` pins both cases.

Accounting was **extracted, not duplicated**: `estimate_result_bytes` +
`memory_limit_error` are now shared by both paths, so the cursor cannot start accepting a
result set the materializing path rejects.

**Tests:** 4 new in `crates/uni/tests/common/perf/query_limits_test.rs` (an
already-registered module — no new integration binary).
**Gates:** workspace `nextest --no-fail-fast` **6078/6078 passed**; uni-db Python suite
**915 passed**; `cargo fmt --check` and workspace `clippy -D warnings` clean.

#### D4 — the transaction cursor, and unifying the two cursor bodies ✅ FIXED (uncommitted)

D1–D3 fixed the session cursor and left its twin broken, which is how the defect arose in
the first place: `execute_cursor_internal_with_config` and
`execute_cursor_internal_with_tx_l0` held **two copies of the same cursor-assembly code**,
and every limit added to one was absent from the other.

The tail from the configured executor onward — batches to rows, re-chunk to `batch_size`,
guard against deadline / memory ceiling / cancellation — now lives once, in
`UniInner::build_guarded_cursor`. The two entry points keep only what genuinely differs:
the session one takes a per-query config override, the transaction one rejects time-travel
and installs the tx-private L0. They are now 29 and 44 lines of preparation around a
single 131-line shared body.

Fixed as a consequence:

- **`TxQueryBuilder::cursor_inner` dropped both `.timeout()` and `.cancellation_token()`.**
  `execute`/`fetch_all` apply the timeout by wrapping their future in
  `tokio::time::timeout`; a cursor returns a stream that outlives the call, so the ceiling
  now travels with the stream via the config the guard reads.
- **`max_query_memory` was inert on the whole transaction surface**, not just its cursor —
  `execute_internal_with_tx_l0` never called `enforce_memory_limit`. Adding the ceiling to
  the tx cursor alone would have handed that surface a *fresh* asymmetry (streaming
  stricter than materializing), so it was added to both.
  `test_tx_fetch_all_enforces_configured_memory_limit` exists specifically to stop a future
  change from fixing only the streaming half again.

Two corrections to what D1–D3 shipped:

1. **The cancellation error was the wrong variant.** The guard raised
   `Query { "Query cancelled" }`, but `into_execution_error` already classifies a cancelled
   execution as `UniError::Cancelled`, which the bindings map to a dedicated
   `UniCancelledError`. Rendering it as `Query` collapsed it into the same class as every
   other query failure — precisely the defect 0.10 fixed on the async cursor. Python now
   reports `UniCancelledError: Operation cancelled`.
2. **`into_stream_error`'s doc comment was falsified** by the guard and has been corrected.
   Its cancellation/timeout arms are still unreachable, but for a new reason: the guard
   raises those *around* the stream rather than through this function.

An elapsed deadline is rendered per surface — `Query { "Query timed out" }` for the
session, `Timeout { timeout_ms }` for the transaction — via `CursorTimeoutError`, because
the two surfaces' *materializing* terminals already disagree. That divergence predates this
work and is not resolved here; each cursor matches its own builder so `.timeout()` is
consistent across a given builder's terminals. Verified from Python:
`UniTimeoutError: Operation timed out after 50ms`.

**Gates:** workspace `nextest --no-fail-fast` **6083/6083 passed** · uni-db Python
**915 passed** · fmt and `clippy -D warnings` clean. 5 new tests, all four
limit-related ones observed failing first.

#### D5 — cancellation made real everywhere ✅ FIXED (uncommitted) · closes Tier 1 item 1.7

Fixing the cursors exposed that cancellation was **decorative across the whole engine**, not
just on the two paths D1 touched. Three findings, none of them in the original 84:

- **`Session::cancel()` did nothing at all.** Plain `Session::query()` routes through
  `execute_cached`, which hard-coded `None` for the token at all three of its call sites.
  Only `QueryBuilder` *with an override* ever carried one, so the most-used API on the
  most-used surface had a `cancel()` that cancelled nothing.
- **`Transaction::query_with` never seeded the builder from the transaction's own token**,
  which is the whole reason `Transaction::cancel()` was inert — the child token created at
  `transaction.rs:316` was simply never read.
- **`execute_ast_internal` is cancellation-blind by construction** — no parameter, and
  `apply_session_executor_state` does not set one. The time-travel branch of
  `execute_internal_with_config_and_token` routes there, so a time-travel query silently
  discarded its token.

So the fix is not per-builder plumbing. **Every execution path now runs under a
`CancelScope`** (`impl_query.rs`), which carries the enclosing session/transaction scope plus
an optional caller token and is awaited by whatever enforces it. Because it is a required
parameter, the compiler enumerated every path that had been silently dropping cancellation —
including `PreparedQuery`, which had no token on either of its branches and now captures the
scope of whatever prepared it.

Two design notes worth keeping:

1. **The guard is authoritative; the executor's token is an optimisation.**
   `Executor::set_cancellation_token` takes exactly one token and only feeds
   `GraphContext::check_timeout`, which fires solely where an operator happens to call it —
   no scan/join/traverse plan reaches one. It is still worth setting for the long-running
   operators that *do* check (traverse, shortest-path, vector-KNN, procedure calls), but
   correctness comes from racing `CancelScope::cancelled()` against execution at every
   terminal.
2. **Two independent tokens cannot be merged cheaply.** `tokio_util` has no "whichever fires
   first" combinator, and merging would cost a spawned linker task plus a drop guard per
   statement. `CancelScope` holds both and `select!`s on them instead; an empty scope pends
   forever so the branch is safe to take unconditionally.

Also closed, because leaving them would have re-created the asymmetry:

- **Rust `ExecuteBuilder` had no `cancellation_token`**, so Python's
  `TxQueryBuilder.execute()` would have accepted a token and silently ignored it — the exact
  defect class this work removes. Added a field and setter.
- **Python's `TxQueryBuilder` never exposed `cancellation_token`** at all (`builders.rs`),
  despite the Rust builder having carried the field all along. Added on the sync class, the
  async twin, and `__init__.pyi` in one change — `test_stub_drift.py` enumerates both
  directions and fails on either half alone.

**Gates:** workspace `nextest --no-fail-fast` **6088/6088 passed** · uni-db Python
**922 passed** (was 915) · fmt and `clippy -D warnings` clean. 12 new tests — 6 Rust, 6
Python — all observed failing first, each paired with an inverse guard so that wiring a scope
into every path cannot silently break ordinary queries.

#### D6 — one timeout error across both surfaces ✅ FIXED (uncommitted) · **BREAKING**

An elapsed deadline used to render per surface: `Query { "Query timed out" }` on the session,
`Timeout { timeout_ms }` on the transaction. The same condition therefore needed two
different `except` clauses depending on which terminal produced it — the same stringly-typed
erasure item 0.10 fixed on the async cursor, in a second place.

Everything now raises `UniError::Timeout { timeout_ms }`: typed, carrying the budget that was
exceeded, and mapped by the bindings to a dedicated `UniTimeoutError`.

The lower layers signal an elapsed deadline with a bare `anyhow!("Query timed out")` and
cannot know the budget they blew, so `into_execution_error` takes the effective
`query_timeout` and reattaches it when re-classifying.

**`CursorTimeoutError` is deleted.** It existed only to model the divergence — a type, a
parameter threaded through two entry points, and a branch in `cursor_inner`, all of which
were the cost of the inconsistency rather than anything intrinsic. Removing the parameter
also brought `build_guarded_cursor` back under clippy's argument limit, so its
`#[expect(clippy::too_many_arguments)]` went too; the suppression turning unfulfilled is
independent evidence the simplification was real rather than cosmetic.

Assertions were rewritten to match on the **variant** rather than on message text, which is
the point of the change; `drain_cursor` now hands back the `UniError` instead of its
rendering.

> **Breaking:** Python code catching `UniQueryError` for a query timeout must catch
> `UniTimeoutError`. Rust code matching `UniError::Query` with a `"Query timed out"` message
> must match `UniError::Timeout`. Belongs in the breaking-change ledger; the version decision
> is deferred until the whole sweep lands.

**Gates:** workspace `nextest --no-fail-fast` **6088/6088 passed** · uni-db Python
**923 passed** · fmt and `clippy -D warnings` clean.

### Documentation gap blocking the changelog requirement

Items 0.10, 0.13, 0.14, 0.2 and 0.7 are user-visible and were scoped as "fix now, document in CHANGELOG". **There is no changelog to write to.** The only one in the repo is `crates/uni-plugin/CHANGELOG.md`, which is plugin-scoped, and there is no release-notes page under `website/docs/`. Commit messages are currently the sole record. A `bindings/uni-db/CHANGELOG.md` needs creating before the remaining user-visible items land.

---

## Tier 0 — Latent bugs, independently landable, no API surface change

These are worth more than every LOC saving in this document combined. Each is one commit, each is independently testable, none depends on another.

### 0.1 — `IdAllocator` resets to zero on any error containing "not found"

**File:** `crates/uni-store/src/runtime/id_allocator.rs:60`
**LOC:** 20 · **Risk:** low · **Verdict:** CONFIRMED
**✅ DONE** — `eb35fe6db`. Test `repro_id_allocator_substring_notfound.rs`. The trap cleared (`repro_10` stayed green), but `FaultStore` needed a new `set_transient_message()` before the failing input was expressible at all.

Replace `Err(e) if e.to_string().contains("not found")` with `Err(e) if crate::store_utils::is_not_found(&e)`. The typed predicate already exists at `store_utils.rs:93` and two sibling loaders (`fork/registry.rs:172-185`, `snapshot/manager.rs:180-186`) already use it with comments explaining exactly why substring matching is unsafe. The allocator is the one loader never migrated. A proxy 404, a wrapped S3 "bucket not found", or a Generic error from a misconfigured store currently starts the allocator at `next_vid_batch = 0` against a non-empty database — handing out VIDs that already exist.

Add a regression test: fault store returning a Generic error whose message contains "not found"; assert `IdAllocator::new` propagates rather than returning a zero allocator.

> **Trap:** `in_memory_seeded` (`id_allocator.rs:236`) PUTs the manifest then calls `Self::new`, so the genuine-NotFound arm must stay reachable. `repro_10_id_allocator_persist_fail.rs` wraps a fault store — check that test still passes rather than assuming; if the harness surfaces NotFound as Generic, tightening the guard changes its observed behaviour.

### 0.2 — Locy ALONG silently loses `prev.X` inside a comparison

**Files:** `crates/uni-cypher/src/grammar/locy_walker.rs:937-958`, `crates/uni-locy/src/compiler/typecheck.rs:383-393`, `crates/uni-query/src/query/locy_planner.rs:2060`
**LOC:** 35 · **Risk:** med · **Verdict:** CONFIRMED (reproduced with a compiled probe)

`build_locy_comparison_expression` re-slices the raw source span and re-parses with `CypherParser` whenever a `comparison_tail` is present. `PREV` is not in `cypher.pest`'s `keyword_reserved`, so the re-parse succeeds and destroys the `LocyExpr::PrevRef` marker. Verified:

- `ALONG h = prev.h + 1` on a non-recursive rule → `Err(PrevInBaseCase{rule:"t", field:"h"})` ✅
- `ALONG f = prev.h > 5` on the same rule → `Ok`, AST is `Cypher(BinaryOp{left: Property(Variable("prev"),"h"), ...})` ❌

Both validation sites are blinded identically: `collect_prev_refs` returns `vec![]` for `LocyExpr::Cypher`, and `validate_prev_refs` has `LocyExpr::Cypher(_) => Ok(())`.

**Do the loud-failure fix first:** scan the pair tree for `LocyRule::prev_reference` before falling back to the re-parse, and return an explicit `ParseError`. Add the regression test as a `mod` in the existing `crates/uni-locy/tests/integration.rs`.

> **Trap:** `ALONG f = prev.h > 5` **parses today**, so a hard `ParseError` is a breaking change on a published 3.1.0 parse surface. Going the structured-walk route instead risks regressing the non-prev comparison forms the re-parse handles for free (`IN`, `STARTS WITH`, `IS NULL`, list literals) — `LocyBinaryOp` does not model all of them. Note the perverse current state: prev-in-comparison is *accepted* while prev-in-arithmetic is *rejected*.

### 0.3 — Wardedness check false-positives on parenthesized/quantified paths

**File:** `crates/uni-locy/src/compiler/warded.rs:43`
**LOC:** 20 · **Risk:** low · **Verdict:** CONFIRMED (reproduced)
**✅ DONE (uncommitted)** — extracted `collect_path_vars`, which recurses into the nested
`PathPattern`; the arm stays explicit so a future variable-binding `PatternElement` fails to
compile rather than silently reintroducing the false positive. **The trap was checked, not
assumed:** a compile-only fix would have moved the failure downstream, so
`crates/uni/tests/common/locy/locy_warded_parenthesized_path.rs` runs the rule against a real
database and asserts it derives the same edge as the unparenthesised form. 4 compiler tests +
2 runtime tests; the parenthesised and quantified cases failed first, the baseline and the
still-unwarded case passed throughout.

`PatternElement::Parenthesized { .. } => {}` — empty body, no comment. This is the swallowing-walker case, not a documented narrowing (checked: no comment anywhere in the 81-line file). Every other consumer of this variant in the repo recurses (`planner.rs:4415/4632/9453/10141`, `df_planner.rs:6968/7097`, `session.rs:1901`).

Probe: `MATCH (a:A)-[:E]->(b:B) DERIVE (NEW x:C)<-[:IN]-(b)` → `Ok(None)`; wrap the path in parens → `Err(WardednessViolation{variable:"b"})`.

Extract the per-path body of `extract_match_variables` (`warded.rs:27-45`) into a recursive `fn collect_path_vars(path: &PathPattern, vars: &mut HashSet<String>)` and call it from the `Parenthesized` arm. Keep the arm explicit — no `_ =>` — so a future `PatternElement` variant still fails to compile.

> **Trap:** Fixing wardedness may just move the failure downstream. Verify `uni-query`'s Locy planner actually exposes a variable bound inside a quantified sub-pattern as a column, or the rule now compiles and fails later with a more confusing planner error. Also confirm no existing test pins `WardednessViolation` for a parenthesized MATCH as intended behaviour.

### 0.4 — ASSUME body drops the enclosing MODULE context

**File:** `crates/uni-locy/src/compiler/mod.rs:264-300`
**LOC:** 40 · **Risk:** med · **Verdict:** CONFIRMED (reproduced)

The ASSUME branch rebuilds the body program with `module: None, uses: vec![]`, `body_module_ctx = ModuleContext::default()`, and the context-free `group_rules` (`mod.rs:271` is its only caller). Outer rules were qualified as `"<module>.<rule>"` by `modules::resolve_rule_name`. The namespaces disagree.

Probe: without `MODULE` → `Ok(None)`; add a leading `MODULE acme` to the identical program → `Err(UndefinedRule{name:"adult"})`.

This is the MODULE-qualified sibling of the bug already pinned by `crates/uni-locy/tests/repro_assume_body_outer_rule_ref.rs`, whose comment shows `external_rules` were threaded but module context was not. Thread `module_ctx` (and outer `module`/`uses`) into the ASSUME branch via `group_rules_with_context`. Deleting the now-unused `group_rules` falls out. Add the case as a second `#[test]` **inside the existing repro file** — no new test binary.

> **Trap:** Qualifying body rule names as `module.rule` changes the keys of the body's `rule_catalog` and `Stratum.rules`. Anything looking up body rules by unqualified name at runtime (the ASSUME body executor, body `QUERY`/`DERIVE` commands, explain output) silently misses. It also puts body rules in the same namespace as outer rules, so a name collision replaces shadowing. **Trace the `CompiledCommand::Assume` consumer in uni-query before changing the naming.**

### 0.5 — `VERSION AS OF` panics through `.explain()`, `.profile()`, `.cursor()`

**Files:** `crates/uni/src/api/impl_query.rs:347-441`, `crates/uni-query/src/query/planner.rs:2635`
**LOC:** 50 · **Risk:** low · **Verdict:** CONFIRMED (AST shape confirmed empirically)
**✅ DONE (uncommitted)** — `split_time_travel` is now a named function with one definition and
seven call sites; it had been inlined at four and simply absent from three, and the three that
omitted it panicked. `explain`/`profile`/`cursor` resolve the spec and re-dispatch against a
pinned instance via a shared `pinned_at`, so none of them plans a wrapped AST.
**There is a second `unreachable!`**, not mentioned in this document, at
`uni-query-functions/src/rewrite/walker.rs:57` ("TimeTravel should be resolved at API layer
before rewriting") — it fires *first*, so that is the panic the repro actually caught. Fixing
at the API layer satisfies both.
The tests assert the **historical** row count, not merely the absence of a panic: stripping
the spec and running against the live version would also stop the panic while silently
answering a different question. `explain` stays read-only-permissive, matching its existing
documented behaviour (EXPLAIN never executes, so describing a write plan is legitimate);
`profile` and `cursor` validate read-only because they do execute.

`planner.rs:2635` is a literal `unreachable!("TimeTravel should be resolved at API layer before planning")`. Four entry points destructure `Query::TimeTravel` first (`impl_query.rs:495-505, 612-622, 664-674, 729-742`); three do not — `explain_internal` (347-359), `profile_internal` (361-391), `execute_cursor_internal_with_config` (393-441). `run_query_guards` does **not** intercept: `validate_read_only_with` recurses through TimeTravel and returns `Ok`, and `explain()` doesn't even call the guards.

Reachable from Rust (`QueryBuilder::explain/profile/cursor`), the uni-cli REPL (`repl.rs:101,115`), and Python (`bindings/uni-db/src/builders.rs:1382,1410,1420`).

Extract `fn split_time_travel(ast) -> (Query, Option<TimeTravelSpec>)` in `impl_query.rs`; call from all three. explain/profile resolve the snapshot and delegate to a **pinned** `UniInner` as `execute_internal_with_config` does (`impl_query.rs:735-742`); the cursor path may instead return the `UniError::Query` its tx sibling returns.

> **Trap:** Do **not** soften `planner.rs:2635` to an `Err` arm — that `unreachable!` is the load-bearing signal that the API layer owns TimeTravel resolution. And do not merely *strip* the spec on explain/profile: that would silently explain against the live version when the user asked for a historical one — worse than the panic. `profile()` actually executes, so the pinned instance is mandatory there.

### 0.6 — `UniSync::shutdown()` leaks the tokio runtime

**File:** `crates/uni/src/api/sync.rs:78-91`
**LOC:** 5 · **Risk:** low · **Verdict:** CONFIRMED
**✅ DONE** — `0821205dd`. Both masked hazards were checked before deleting the `forget`; neither reproduced. Test `shutdown_does_not_leak_runtime_threads` counts `/proc/self/task` and measured **48 → 232 threads** over 8 cycles beforehand.

`shutdown(mut self)` does `self.inner.take()`, blocks on `uni.shutdown()`, then `std::mem::forget(self)` "to prevent Drop from running". But `impl Drop for UniSync` is `if let Some(ref uni) = self.inner` — after the `.take()` it's already a no-op. The only thing `forget` suppresses is `Runtime::drop`, so every explicit `shutdown()` leaks a multi-threaded runtime and never joins its workers. The `Ok(())` early-return at line 90 does not forget, confirming it isn't load-bearing.

Delete the `mem::forget` and return `result` directly.

> **Trap:** Two hazards currently *masked* by the forget. (1) `Runtime::drop` blocks until every `spawn_blocking` task completes — a stuck blocking task turns a fast shutdown into a hang. (2) Dropping a Runtime from inside another runtime's async context panics; a user calling `UniSync::shutdown()` from an `async fn` goes from working to panicking. Verify both before deleting. Do **not** "fix" by making the `Drop` guard unconditional — that double-shuts-down an already-shutdown `Uni`.

### 0.7 — Plugin procedure output never decodes the CypherValue transport

**Files:** `crates/uni-query/src/query/executor/procedure.rs:307`, call sites at `:753` and `:856`
**LOC:** 25 · **Risk:** med (verifier raised from low) · **Verdict:** CONFIRMED

`arrow_scalar_to_value` maps `Dt::LargeBinary => Ok(Value::Bytes(...))` with no codec decode. But `LargeBinary` **is** the framework's CypherValue transport (`adapter_common/arrow_types.rs:51`, `uni-plugin-wasm/src/loader.rs:1560-1571`), the plugin input side decodes it correctly (`plugin_adapter.rs:194-207`), and the DataFusion `CALL` path decodes it (`read.rs:783-806` → `arrow_convert.rs:711-721`). So `CALL proc() YIELD listCol` returns a decoded List on the DF path and opaque codec bytes on the row-fallback path.

Change `arrow_scalar_to_value` to take the `&Field` (both call sites already hold it). For `LargeBinary`: return `Value::Bytes` when metadata carries `uni_raw_bytes=true`, else `cypher_value_codec::decode(...)`.

> **Trap — this is why risk went to med:** `uni_raw_bytes` is stamped **only** on the storage/scan path (`uni-common/src/core/schema.rs:113`, `df_graph/scan.rs:638`, `raw_bytes_marker.rs`). No plugin loader sets it on a yield `Field`. **Decode must fall back to `Value::Bytes` on decode error, not hard-error** — otherwise every existing plugin yielding genuine opaque bytes goes from `Value::Bytes` to an error at the CALL site. The fallback branch is load-bearing, not optional. Add both a decode test and a raw-bytes round-trip test.

### 0.8 — Plugin instance pool serializes every cold start behind a Mutex

**File:** `crates/uni-plugin-wasm-rt/src/pool.rs:117,199`
**LOC:** 8 · **Risk:** low · **Verdict:** CONFIRMED
**✅ DONE** — `2f0479579`. **Scope corrected:** no 10–100 ms compile is serialized — `build_pool` caches the `InstancePre`, so the guarded segment is `instantiate` + `fresh_store`. Still 4-way concurrency delivering 1-way. Inline `Barrier` test deadlocked under the `Mutex`.

`factory: Mutex<Box<dyn Fn() -> Result<T, E> + Send + Sync>>` invoked as `(self.factory.lock())()` — the parking_lot guard lives to end-of-statement, so the whole instantiation runs under the lock. The boxed closure already carries `Sync` and `InstancePool::new` bounds it `Send + Sync + 'static`, so the Mutex buys nothing. `max_instances = 4` advertises 4-way concurrency and delivers 1-way.

Change the field to `Box<dyn Fn() -> Result<T, E> + Send + Sync>`, call `(self.factory)()`, drop the `Mutex::new` wrap at `:156`. Add a contention test (N threads, sleeping factory, wall-clock < N × sleep).

> **Correction to the scout's impact claim:** `build_pool` compiles + links the component **once** and caches the `InstancePre` (`loader.rs:955-963`). The serialized segment is `InstancePre::instantiate` + `fresh_store`, **not** the 10–100 ms full compile. Fix the writeup accordingly.
>
> **Trap:** `parking_lot::Mutex` is imported at `pool.rs:41` and used nowhere else — remove the import or the `-D warnings` build goes red. Do **not** relax `Sync` on the `new` bound: `InstancePool` is shared as `Arc<...>` across query workers, and dropping `Sync` makes the whole pool `!Sync`.

### 0.9 — Algorithm yield types accepted at registration that the emit path cannot assemble

**Files:** `crates/uni-plugin-pyo3/src/loader.rs:548`, `crates/uni-plugin-rhai/src/loader.rs:382`
**LOC:** 50 · **Risk:** low · **Verdict:** CONFIRMED

`AlgoSession::finish_emitted` yields `Vec<(String, Vec<f64>)>` and every `build_batch` matches only `Float64 | Int64`, erroring `0x862` otherwise. Extism and WASM restrict yields inline at registration. PyO3 routes through `adapter_scalar_helpers.rs:25` (admits `string`→Utf8, `bool`→Boolean); Rhai routes through `wire_translate.rs:23-39` (additionally `f32`→Float32, `i32`→Int32, `null`→Null). So `yields=["label:string"]` (Python) and `["w:f32"]` (Rhai) register cleanly and fail `0x862` on **every** CALL.

Add `algorithm_yield_datatype(token) -> Option<DataType>` beside `guest_emit_columns` in `uni_plugin_builtin::algorithms::graph_compute`, admitting exactly the Int64/Float64 spellings, and route all four loaders through it. Keep per-loader error wrapping.

> **Note the error code:** it is **`0x862`**, not `0x869` (`0x869` is the missing-column arm). The scout had this wrong.
>
> **Trap:** This turns "loads fine, never CALLed" into "fails to load" for existing plugins. Check `crates/uni/tests/common/loaders/pyo3_graph_compute.rs` and the rhai loader tests for fixtures with non-numeric yields first. Do **not** implement by reusing `arg_type_from_token` — it maps `value`→CypherValue and `list`→Vector, which `build_batch` also cannot assemble, widening the hole.

### 0.10 — Async cursor stringifies `UniError`, defeating retry classification

**File:** `bindings/uni-db/src/async_api.rs:3136,3157,3174,3190,3249`
**LOC:** 8 · **Risk:** low · **Verdict:** CONFIRMED
**✅ DONE (uncommitted)** — 5 new cases in `tests/test_sync_async_parity.py` (a sync baseline, 3 parametrized async paths, an exhaustion guard); the 3 async ones failed before the fix and pass after. Repro is integer div-by-zero on an `UNWIND` divisor. **Blast radius narrower than stated:** only `fetch_one`/`fetch_many`/`__anext__` — `fetch_all` already used `collect_remaining` correctly and masks the bug if used to check parity. Both traps honoured: `UniError` is `Send + 'static`, and the `Ok(None)` arm is untouched (pinned by its own test).

`next_row_async` is typed `Result<Option<Row>, String>` and collapses at `:3157`. Its three consumers rebuild as bare `PyRuntimeError`. The divergence is internal to the same impl — `fetch_all` at `:3213` correctly uses `uni_error_to_pyerr`, as does the sync twin `QueryCursor::next_row` (`sync_api.rs:60`). `_retry.py:32-37` catches by class, so every cursor-stream error — including `UniTimeoutError` and `UniCancelledError` — is unclassifiable.

Change the return type to `Result<Option<Row>, uni_common::UniError>`, drop `.to_string()`, replace the three `PyRuntimeError` wrappers with `uni_error_to_pyerr`.

> **Honesty note:** the scout's specific "serialization conflict mid-stream" scenario is *plausible but not demonstrated* — uni's OCC/SSI aborts land at commit. The type-erasure defect stands on its own regardless of variant. Don't over-claim in the commit message.
>
> **Trap:** The futures go to `future_into_py`, so the error type must be `Send + 'static` (`UniError` is, but a future non-Send variant fails at the call site with a confusing HRTB error, not at the fn definition). Do not disturb the `Ok(None)` arm at `:3251` — `__anext__` depends on it raising `PyStopAsyncIteration` or `async for` loops forever.

### 0.11 — `value_to_py` silently nulls unknown `Value` variants

**File:** `bindings/uni-db/src/convert.rs:244`
**LOC:** 4 · **Risk:** low · **Verdict:** CONFIRMED
**✅ DONE** — `0847283ef`. Debug rendering truncated to 120 chars, char-boundary safe. **The suggested Rust exhaustiveness test is impossible** — `extension-module` means libpython is not linked, so no Rust test in this crate can start an interpreter; coverage lives in `tests/test_value_conversion.py` instead.

`_ => Ok(py.None())`. The two arms above carry comments recording that this fallback **already caused shipped data loss twice** (`:226-227` SparseVector, `:236-237` BinaryVector). All 15 current `Value` variants are handled, so the arm is unreachable today and the change is behaviour-preserving.

Replace with `other => Err(PyValueError::new_err(format!("unsupported Value variant in Python conversion: {other:?}")))`. Consider truncating the Debug rendering — a vector-shaped variant produces a multi-KB message.

> **⚠️ This does NOT restore a compile-time exhaustiveness guarantee.** `uni_common::Value` is `#[non_exhaustive]` (`value.rs:423`) and bindings/uni-db is a separate crate — the wildcard is **mandatory**. Anyone who "does it properly" by removing it breaks the build. The fix converts silent-wrong-answer into loud runtime failure, nothing more.

### 0.12 — `PyForkStatus::from_rust` fails open to `Active`

**File:** `bindings/uni-db/src/types.rs:3580`
**LOC:** 6 · **Risk:** low · **Verdict:** CONFIRMED
**✅ DONE** — `0ca8efb50`. `Unknown` added to both the pyclass and `__init__.pyi:2590` in the same commit, as the trap required; `test_stub_drift.py` green.

`_ => Self::Active, // future variants surface as Active for now`. `ForkStatus` is `#[non_exhaustive]` and its doc says "Recovery resumes any non-Active state" — non-Active is precisely the "mid-lifecycle, unsafe to use" signal. A future recovery/quiescing state reports as healthy to `db.list_forks()`, and Python branching on `status == ForkStatus.Active` opens sessions against it.

Add an `Unknown` variant to `PyForkStatus`, map the wildcard to it, **and** add `Unknown` to `class ForkStatus(Enum)` at `__init__.pyi:2584` in the same commit.

> **Trap:** `test_stub_drift.py` enumerates runtime class members against the stub in both directions — a variant added without the stub fails it. Same `#[non_exhaustive]` caveat as 0.11: the wildcard cannot be removed, only its value changed.

### 0.13 — Pydantic OGM: `_result_to_model` swallows every exception

**Files:** `bindings/uni-pydantic/src/uni_pydantic/session.py:835-840`, `async_session.py:561-567`, `query.py:938,942`
**LOC:** 20 · **Risk:** med · **Verdict:** CONFIRMED

`except Exception: return None`, then `if instance: instances.append(instance)` — the row vanishes with no log, no warning, no counter. Consequences: `Model.query().all()` returns fewer rows than exist; `.count()` uses separate Cypher so `len(q.all()) != q.count()`; `.one()` raises `QueryError("Query returned no results")` for a row that demonstrably exists.

**Additional defect the scout missed:** the guard is `if instance:` (truthiness), so a validly-hydrated model whose type defines a falsy `__bool__`/`__len__` is *also* dropped.

**✅ DONE (uncommitted)** — `except Exception` narrowed to `ValidationError`; invalid stored data
is still skipped but now warns through `_warn_unhydratable`, naming the label and vid. All
**9** truthiness guards became `is not None` (8 × `if instance:` plus one `if not instance:` in
`_rows_to_scored_instances`). `query.py` is shared by the sync and async builders, so one fix
covers both; the swallow itself was duplicated per-surface and needed fixing twice.

`warnings.warn` rather than a logger: the package has no logging configuration at all, so a
logger would be silent by default under pytest and most app configs — reproducing the original
defect more quietly. Warnings surface by default and are capturable with `pytest.warns`.

> **The predicted regressions did not happen, and the prediction was wrong.** This document's
> "expect previously-green tests to fail" rests on the try block covering `from_properties(…,
> session=self)` *plus* session wiring, `int()` coercion and `@before_load` hooks. It does not:
> `run_class_hooks(_BEFORE_LOAD)` and `int(vid)` both sit **outside** the try. Only
> `from_properties` was ever wrapped, so narrowing the except exposed nothing pre-existing —
> 263 tests passed unchanged.

Narrow both excepts to `pydantic.ValidationError`, log a warning naming label + vid, change both guards to `if instance is not None:`. Prefer a session-level strictness flag defaulting to raise.

> **Trap:** Narrowing surfaces exceptions currently swallowed that are *not* validation failures — the try block also covers `run_class_hooks(model, _BEFORE_LOAD, data)` mutations and `int(vid)` coercion, and `from_properties` may raise `TypeError`/`KeyError` on schema drift. Expect previously-green tests to fail. **Those failures are the bug becoming visible — do not widen the except back to make them pass.**

### 0.14 — Pydantic OGM: `eager_load()` caches raw dicts

**Files:** `bindings/uni-pydantic/src/uni_pydantic/session.py:898-943`, `async_session.py:590-635`
**LOC:** 30 · **Risk:** med · **Verdict:** CONFIRMED

Eager loading caches `_row_to_node_dict(row)` — plain dicts — while the lazy path runs each row through `_result_to_model` and returns model *instances*. `RelationshipDescriptor.__get__` returns the cache under `cast("list[NodeT] | NodeT | None", cached)`, so the declared type is a lie. Two divergences: `user.posts[0].title` raises `AttributeError` after `.eager_load("posts")`; and the eager path ignores `descriptor.is_list` entirely, so a to-one relationship is `list[dict]` eagerly and a single model lazily.

**Worse on async than the scout stated:** `async_session.py:578-588` makes `_load_relationship` *raise* `SessionError` telling the user to use `eager_load()` — so on async the **only** relationship access path is the broken one.

Resolve the target model from `node_data.get('_label')` via `self._schema_gen._node_models`, run through `_result_to_model`, and honour `descriptor.is_list`.

> **⚠️ Order dependency:** this composes badly with 0.13 unfixed. Routing eager rows through `_result_to_model` means a validation failure now silently **drops** the related node, where today it at least survives as a dict. **Land 0.13 first**, or you trade a type bug for a data-loss bug.
>
> **Trap:** User-visible break on a shipped OGM — code written against today's eager path uses `user.posts[0]['title']`.

---

## Tier 1 — Latent bugs requiring a scope decision before code

Each needs a call made, not just a patch. Do not start these as "quick fixes."

### 1.1 — Schemaless edge property reads escape snapshot isolation

**Files:** `crates/uni-store/src/storage/main_edge.rs:296`, `runtime/property_manager.rs:368,946,1576`
**LOC:** 25 · **Risk:** med · **Verdict:** CONFIRMED

Every vertex-side main-table reader takes `version: Option<u64>` and conjoins `_version <= hwm` via `with_version_bound` (`main_vertex.rs:53`, six call sites). `find_props_by_eid` takes only `(backend, eid)`; grep for `version: Option<u64>` in `main_edge.rs` returns zero. The asymmetry is *inside one function*: the delta tier applies `apply_version_filter` (`property_manager.rs:263`) and `overlay_l0_edge_batch` skips `entry_version > hwm` (`:975-982`), but the three L1 main-edge fallbacks pass `self.storage.backend()` with no bound. On a pinned/SSI session, L0 and delta are bounded and L1 is not — a post-snapshot edge SET flushed to L1 becomes visible to a transaction that must not see it. Reachable only post-compaction, which is exactly the path added for issue #53.

**Decision required:** `main_edge.rs`'s other readers (`find_edges_by_type_names`, `find_edge_by_eid_including_deleted` at `:261`) are equally unbounded. Fixing only `find_props_by_eid` leaves schemaless traversal reads unbounded. Decide deliberately whether the scope is one function or the module, and say so in the commit.

> **Trap:** `find_props_by_eid` is the #53 tombstone-aware reader — it must keep scanning `_deleted` rows and dropping a deleted winner. Do **not** conflate the version bound with a `_deleted = false` filter.

### 1.2 — `ShadowCsr::gc` never invoked in production

**Files:** `crates/uni-store/src/storage/shadow_csr.rs:86,99`, `adjacency_manager.rs:393,685`
**LOC:** 30 · **Risk:** med · **Verdict:** CONFIRMED

`gc` and `get_entries` appear nowhere outside `shadow_csr.rs`'s own `#[cfg(test)] mod tests`. Not exported from `lib.rs`, no PyO3 wrapper, no name dispatch, no TCK/example/bench/doc mention. Meanwhile both writer paths push unconditionally, and `AdjacencyManager` has no `clear`/`evict`/`reset`. Every deleted edge is retained in a `DashMap<(u32,Direction), HashMap<Vid, Vec<ShadowEdge>>>` for the process lifetime.

**Decision required — the two options have opposite operational consequences:**
- **(a)** Wire `gc` into the snapshot-retention/compaction path with the oldest live snapshot version, or
- **(b)** Delete `gc` + `get_entries` + `test_gc_removes_old_entries` and document retention as unbounded-in-memory.

Do not leave it ambiguous.

> **Trap on (a):** `get_entries_at_version` resurrects an edge when `created_version <= v < deleted_version`, so the GC bound must be the oldest version any live pinned `StorageManager` can read — **not** the current HWM. And `pinned()` / `at_fork` each build a *fresh* `AdjacencyManager` (`manager.rs:474,591`) with its own empty `ShadowCsr`, so the oldest-snapshot version must be tracked outside `AdjacencyManager`.

### 1.3 — `map_variables` `other => other` swallows 10 Expr variants

**File:** `crates/uni-query/src/query/locy_planner.rs:1880-1968`
**LOC:** 60 · **Risk:** med · **Verdict:** PARTIAL

The doc claims recursion through *every* `Expr` variant is what prevents the "No field named …" class. The match ends in `other => other`, silently dropping **10** (not 8) sub-expression-carrying variants: `Exists`, `CountSubquery`, `CollectSubquery`, `Quantifier`, `Reduce`, `ListComprehension`, `PatternComprehension`, `ValidAt`, `MapProjection`, `LabelCheck`. Reachable: `locy.pest:324-328` falls through to Cypher's full `primary_expression`.

This is **the `_ =>`-arm bug case**, not the deliberate-exhaustiveness case — verified against the brief's lesson 2.

Replace `other => other` with explicit arms for all 10 plus the three genuine leaves (`Literal`, `Parameter`, `Wildcard`) so a new variant fails to compile. Recurse into `ValidAt.entity/timestamp`, `MapProjection.base` + `LiteralEntry` values, `LabelCheck.expr`.

> **The scout's proposed action is partly infeasible:** `Exists`/`CountSubquery`/`CollectSubquery` own `Box<Query>`, not `Expr`. `map_variables` cannot recurse into them without a whole Query walker — they can only be **explicit pass-through arms with a comment** stating Query-level substitution is out of scope. Do not silently drop them into a wildcard again.
>
> **Trap:** For `Quantifier`/`Reduce`/`ListComprehension`/`PatternComprehension`, you must **shadow the binder** before descending into predicate/map_expr/expr. Naively recursing rewrites the loop variable itself, turning `reduce(acc = 0, x IN list | acc + x)` into a *different* class of "No field named" failure. `substitute_along_vars` inlines arbitrary expressions, so a substituted expression can capture the binder name.

### 1.4 — CHECK-constraint evaluator diverged between uni-bulk and uni-store

**Files:** `crates/uni-bulk/src/bulk.rs:769-858`, `crates/uni-store/src/runtime/writer.rs:2503-2607`
**LOC:** 200 (verifier raised from 110) · **Risk:** med · **Verdict:** CONFIRMED

Same parser token-for-token. They differ in one behavioural place: for `=`/`==`/`!=`/`<>` `bulk.rs:809-822` routes numeric operands through `compare_values` (with a comment explaining `Value`'s `PartialEq` is type-strict); `writer.rs:2560-2561` uses bare `prop_val == &target_val`. So `CHECK (score = 5)` against a stored `Float(5.0)` **passes** via `BulkWriter::insert_vertices` and **fails** via `tx.execute("CREATE ...")`.

**Stronger than the scout knew:** `crates/uni/tests/common/bugs/bug_bulk_check_int_float_repro.rs` is a *landed regression test* pinning the fixed bulk behaviour. This is a fix applied to one copy and never propagated — not an intentional split.

Move to `uni_common::check_constraint::evaluate(expression, &Properties) -> Result<bool>`, taking bulk.rs's numeric-coercion arms **plus** writer.rs's `Number(...)` branch and warn logging. Delegate from `bulk.rs:653`, `writer.rs:1868`, `writer.rs:2226`. Extend the existing repro with the mirrored `tx.execute` case — that's the currently-failing direction and the whole point.

> **Trap:** This is a live behaviour change on the transactional write path. `CHECK (score = 5)` against `Float(5.0)` currently **rejects** via `tx.execute` and will start **accepting**. Grep for tests pinning the strict behaviour first. `writer.rs:2226` is in **guard position** — the shared fn must keep returning `Result` and the `?`/guard shape must survive or the arm silently changes meaning. Do **not** unify by taking writer.rs's strict `==`: that reopens the already-fixed, test-pinned uni-bulk bug.

### 1.5 — Plugin-registered monotone aggregates can never pass the recursive-stratum FOLD check

**Files:** `crates/uni-locy/src/compiler/mod.rs:107`, `typecheck.rs:398-413`, `crates/uni/src/api/impl_locy.rs`
**LOC:** 60 · **Risk:** med · **Verdict:** PARTIAL

Every host path compiles with `&default_monotonicity_oracle` (`compiler/mod.rs:44,63,81,97`), which answers `Some(true)` only for the six hardcoded M-prefixed names. The registry-backed oracle exists and the injection point (`compile_with_oracle`) exists; neither is on any host path. A plugin aggregate registered `monotone_join: true` is rejected at compile time.

Wire the registry-backed oracle into `impl_locy.rs`, and **reconcile the two checks**: the planner guard at `locy_planner.rs:936-960` rejects `None` (unregistered) as well, so once the compile-time oracle is registry-backed the two must agree or a program passes compile and fails plan.

> **Scout evidence correction:** `is_monotonic_aggregate` is **not** test-only — it has a live production caller at `locy_planner.rs:946`. Only `compile_with_oracle` is genuinely uncalled.
>
> **Trap:** Swapping in the registry oracle **changes what compiles**. `default_monotonicity_oracle` accepts any M-prefixed name as a user-asserted contract; a registry-backed oracle returns `Some(false)`/`None` for an unregistered M-prefixed name, so existing programs using `MSUM`/`MPROD` in recursion start failing **unless the new oracle falls back to the default on a registry miss**. Also the oracle is `&dyn Fn` borrowed for the call — check lifetimes against the session's `Arc<PluginRegistry>`.

### 1.6 — `Uni::get_edge_type_info` unescaped identifier + swallowed error

**File:** `crates/uni/src/api/mod.rs:2817,2825`
**LOC:** 20 · **Risk:** low · **Verdict:** PARTIAL

`format!("MATCH ()-[r:{}]->() RETURN count(r) AS cnt", name)` with no backticks, and `Err(_) => 0`. Confirmed empirically that `MATCH ()-[r:HAS-PART]->()...` fails to parse while the backticked form parses. The sibling `get_label_info` (`:2766-2802`) has no bug — it counts via `backend.count_rows(vertex_table_name(name))`.

Replace the Cypher round-trip with `backend.count_rows(&edge_table_name(name), None)` guarded by `table_exists`, and **propagate the error** instead of `Err(_) => 0`.

> **Scout evidence correction:** name validation **does** exist — `SchemaManager::validate_schema_element_name` (`uni-common/src/core/schema.rs:1923-1941`) rejects empty/whitespace/control/`/`/`\`. So `IS A` is *not* accepted. `HAS-PART`, `X.Y` and backtick-containing names still pass validation and still break. Drop the "no validation exists" claim from the writeup.
>
> **Trap:** Do not merely add backticks and keep `Err(_) => 0` — a backticked name containing a backtick still errors and still silently reports 0. Conversely `count_rows` counts flushed L1 rows only, where Cypher counts L0 + storage and respects fork branches. `get_label_info` already has that L0-blindness so matching it is consistent, but verify against a test that creates edges and reads the count **before flushing**, or you trade wrong-on-weird-names for wrong-on-unflushed.

### 1.7 — `TxQueryBuilder::cancellation_token()` is write-only

**Files:** `crates/uni/src/api/transaction.rs:1657,1669`, `session.rs:1569-1589`
**LOC:** 40 · **Risk:** low · **Verdict:** CONFIRMED

The field is declared and written and never read. All three terminals (`execute_inner`, `fetch_all_inner`, `cursor_inner`) build futures from methods that take no token. Repo-wide there is exactly **one** `executor.set_cancellation_token` call site (`impl_query.rs:961`). `QueryBuilder::cursor` also drops its token. Python forwards a token into that exact path at `builders.rs:1395-1397`. A caller that cancels observes the query run to completion.

Thread `Option<CancellationToken>` through `execute_internal_with_tx_l0` / `execute_cursor_internal_with_tx_l0` / `execute_cursor_internal_with_config`.

**While in there:** check `Transaction.cancellation_token` (`transaction.rs:117`, cancelled at `:1430`, exposed at `:1434`) — it is likewise never handed to any executor, so `Transaction::cancel()` appears inert too. Verify before claiming the fix complete.

> **Trap:** The delete option is *not* the cheap one — removing the setter is a semver-visible removal from published `uni` and breaks the Python binding. On the thread-it-through option: `execute_cursor_internal_with_tx_l0` returns a stream that outlives the call, so the token must be moved into the executor **before** `execute_stream`, not applied to the returned stream, or cancellation silently only affects planning. Do not reuse the session token for a tx builder — `transaction.rs:316` deliberately makes it a *child*.

### 1.8 — `plan_children` is the one plan walker with `_ => vec![]`

**Files:** `crates/uni-query/src/query/executor/read.rs:1062`, `crates/uni-query/src/query/planner.rs:2036-2044`
**LOC:** 45 · **Risk:** low · **Verdict:** CONFIRMED

Five `df_planner.rs` walkers are deliberately exhaustive with the #131 comment verbatim ("a new LogicalPlan variant should fail to compile here until it is classified" — where a missing variant silently degraded an equi-join to a quadratic `CrossJoinExec`). `plan_children` ends in `_ => vec![]`, treating `FusedIndexScanWrapped { inner }` and the five `Locy*{ input }` operators as leaves. It drives `is_ddl_or_admin` (routing) and `contains_write_operations` (MutationContext construction).

**No live wrong answer found** — the planner wraps only `VectorKnn`/`InvertedIndexLookup`/`uni.vector.query`/`uni.fts.query`, all of which `is_df_eligible_procedure` accepts. This is fragility, and it is the only unguarded member of a family whose guardedness was paid for with a production bug.

**This ADDS a compile-time exhaustiveness guarantee.** Enumerate every variant, delete `_ => vec![]`, add the #131 comment. Apply the same to `LogicalPlan::input()` (`planner.rs:2036-2044`), which has the identical `_ => None` hole the scout missed.

> **Trap — not purely additive:** once `plan_children` descends into `Locy*` inputs and `FusedIndexScanWrapped.inner`, `contains_write_operations` starts returning true for shapes previously called read-only, and `is_ddl_or_admin` starts recursing into the wrapped `ProcedureCall`. Verify: `is_ddl_or_admin(FusedIndexScanWrapped{ProcedureCall("uni.vector.query")})` must stay false, and **no Locy plan may now route to the row-fallback executor**, whose Locy arms are `unreachable!()` at `read.rs:3075-3082`. Second trap: hand-enumerating ~40 leaf variants is exactly where a copy-paste typo puts a *wrapper* variant in the leaf list, reintroducing #131 in the walker meant to be hardened — cross-check against the tail already written at `df_planner.rs:6911-6952`.

### 1.9 — SSI read-set holes in three l0_visibility accessors

**File:** `crates/uni-store/src/runtime/l0_visibility.rs:437,468,498`
**LOC:** 12 · **Risk:** low (verifier lowered from med) · **Verdict:** PARTIAL

`get_vertex_properties`, `get_edge_properties` and `edge_exists_in_l0` record no SSI read while eleven siblings do, and the H1 regression test enumerates exactly seven accessors, omitting these three.

**The concrete write-skew scenario is refuted at the cited call site:** `append_node_to_struct` calls `get_vertex_labels(vid, query_ctx)` at `common.rs:470` — which *does* record — immediately before `get_vertex_properties` at `:482`. On the edge side `append_edge_to_struct_optional` resolves type via `get_edge_type`, which also records. Only the `traverse.rs:139` path (type from a batch column) reaches `append_edge_to_struct` with no preceding record on that line, and it was not established that traverse leaves the eid unrecorded overall.

Still apply the recording to all three and extend the test to all ten — 12 LOC of cheap defence-in-depth. **But drop the "silent write-skew today" framing.** Either produce a failing SSI test through `traverse.rs:139` or state the change as hardening. Replace `edge_exists_in_l0`'s "preserved as-is" comment with the real reason.

> **Trap:** `record_*_read` takes the OCC read-set lock per call. Adding it to `get_*_properties` puts a lock acquisition on the per-row node/rel materialisation path (`common.rs:389,482`, plus traverse), so a hot `RETURN n` over a large result set pays it per row **on top of** the `get_vertex_labels` call that already records the same vid. Measure before landing.

### 1.10 — Nested object-store retry amplification

**Files:** `crates/uni-store/src/store_utils.rs:33`, `storage/resilient_store.rs:87,119`
**LOC:** 25 · **Risk:** med · **Verdict:** PARTIAL

`store_utils::retry_with_timeout` does 4 attempts; `ResilientObjectStore::retry` does 4 more inside — 16 physical attempts. The two layers classify non-retryable errors differently: typed `object_store::Error::NotFound` at `store_utils.rs:47` vs `e.to_string().to_lowercase().contains("not found")` at `resilient_store.rs:119`.

**Do the cheap unambiguous fix only:** switch `resilient_store.rs:119` to `matches!(e, Error::NotFound{..} | Error::AlreadyExists{..})`.

**Do NOT collapse the retry layers yet.** Two supporting claims are wrong: (a) the circuit-breaker arithmetic — `report_failure` fires once per inner exhaustion and the outer loop makes 4 inner calls, so one logical op contributes ~4 failures against a threshold of 5; the breaker opens during the *second* logical op, not after ~5. (b) The premise "every production store is a `ResilientObjectStore`" is unverified — `store_utils` is also called from `runtime/wal.rs`, `fork/wal`, `fork/id_alloc.rs`, `uni-bulk/flush_intent.rs`, whose stores are not all routed through `StorageManager`'s wrapper.

Before collapsing, enumerate each caller and confirm which stores are wrapped.

> **Trap:** Deleting the `store_utils` retry loop silently removes **all** retry from the unwrapped callers (WAL segment PUTs, fork WAL, bulk flush intents), turning a transient blip into a hard WAL write failure. Also, once the breaker opens the inner layer returns `Error::Generic{"Circuit breaker open"}`, which the outer loop treats as retryable — removing the outer loop changes failure-latency semantics that `flush_resilience.rs` may depend on.

---

## Tier 2 — Safe consolidations, no behaviour change, no API surface

Land freely once Tier 0 is in. Each is independently revertible unless noted.

| # | What | Anchors | LOC | Trap |
|---|---|---|---|---|
| 2.1 | Rewrite six l0_visibility accessors as `find_in_l0_buffers(ctx, \|buf\| …)` one-liners; factor the two `accumulate_*` bodies into one map-selector helper | `l0_visibility.rs:292,325,355,401,437,468` + `:112,157` | 150 | **Keep `record_*_read` ABOVE the walk** — the H1 contract (`:243-248`) requires the SSI read register whether or not a layer hits; folding it into the visitor breaks SSI. `get_vertex_labels` returns `Vec::new()` on miss, `_optional` returns `None`; `get_*_properties` is first-layer-wins, `accumulate_*` merges — do not unify those two shapes. No enum match here, so no exhaustiveness lost. |
| 2.2 | Extract `configure_and_stream(dataset, request, ctx)` from `execute_primary_scan` / `execute_branch_scan` | `backend/lance.rs:139,178` | 45 | Keep `use_scalar_index(false)` **at the branch call site, not inside the helper** — hiding the #106 divergence regresses nested forks to the `page_lookup.lance` error or costs primary its scalar-index acceleration. Keep the `@{branch}` suffix in the context string. |
| 2.3 | Collapse `IdAllocator`'s four allocate fns along the vid/eid axis | `id_allocator.rs:84,115,148,173` | 70 | The scout's `(&mut u64, &mut u64)` signature **does not compile** — `persist_manifest` needs the whole `AllocatorState`. Use a `match kind` field-selector taking `&mut AllocatorState`. "`allocate_vid` is exactly `allocate_vids(1)`" is **false**: it reserves `current + batch_size` vs `current + 1 + batch_size`. Pin the batch-boundary difference with a test on the persisted `next_*_batch`. The rollback block is safety-critical — `repro_10_id_allocator_persist_fail.rs` must stay green. |
| 2.4 | Reimplement `UidIndex::get_vid` over `resolve_uids` | `storage/index.rs:201,260,313` | 50 | **Pick the tie-break deliberately first.** `get_vid` is last-equal-version-wins, `resolve_uids` is first-wins, and `manager.rs:2260` routes production UID resolution through `get_vid`. Extend `test_get_vid_picks_highest_version` with an equal-version pair. Perf note: `resolve_uids` projects an extra `_uid_hex` column and builds a hex→UniId map per call — if the single-UID path is measured, wrap `scan_uid_rows` instead. |
| 2.5 | Add `derive_view(...)` owning `StorageManager`'s 14-field construction | `storage/manager.rs:460,514,565` | 35 | Three field decisions are load-bearing: `adjacency_manager` must be **fresh** in `pinned`/`at_fork` (#73) but **shared** in `pinned_at_version` (`:500-512`); `vid_labels_index` must be a **deep copy**, never Arc-clone (review H1/L2, #99); `flush_in_progress`/`compaction_status` fresh per view. **Leave the `debug_assert` at `:577` and `pinned_version_hwm: None` alone** — the scout called it stale, unverified: Phase 4a made *fork-then-pin* work, the assert guards *pin-then-fork*, which nothing exercises. |
| 2.6 | Delete the vestigial `for _label in schema.labels.keys()` loop in `recover_all_staging_tables` | `storage/manager.rs:422-426` | 5 | **Not a pure no-op:** today a schema with edge types and *zero labels* performs no adjacency recovery at all. Removing the loop makes it fire. That is correct, but verify against the `LanceDbBackend` existence cache (`lance.rs:263`) and the crash-recovery tests. |
| 2.7 | Move `parse_props_json` to a shared `storage/mvcc.rs`; have main_vertex's two inline copies call it | `main_vertex.rs:594-598,690-694`, `main_edge.rs:359` | ~40 first step, 150 if the full `mvcc_winners` fold follows | **The "tie-break has already drifted" claim is FALSE.** The batch guard `if …version < *bv { continue; }` does *not* continue on equality, so it inserts and last-equal-wins — identical to the single-key `version >= best_version`, and `main_vertex.rs:583-584` says so. Consolidating on "batch is first-wins" would invert the tie-break for every batch reader and return stale props after delete+reinsert at the same version — the exact review-C2 bug. **Add a tie-break test pinning last-equal-wins BEFORE any generic fold.** |
| 2.8 | Reduce `rewrite_is_ref_cols` to `map_variables(expr, &\|n\| aliases.get(n)...)` | `locy_planner.rs:1804,1880,1960` | 62 | **Land in the same commit as 1.3.** If 1.3 lands first and this second, the collapse silently changes `rewrite_is_ref_cols`'s behaviour on the 10 newly-handled variants — desired, but it means the two are not independently revertible. Use `.get()` not the current panicking `aliases[name]`. |
| 2.9 | `block_on_scoped(what, \|rt\| …)` helper for the 7 scoped-runtime sites | `pattern_comprehension.rs:233,313,360`, `pattern_exists.rs:210,359`, `similar_to_expr.rs:382`, `expr_compiler.rs:2367` | 85 | The Runtime must be built and dropped **on the spawned thread** — the helper takes a closure over `&Runtime`, never a `&Runtime` argument, or you get "Cannot start a runtime from within a runtime". Closures capture `&self` and batch-local borrows, so the helper must be generic over the closure, never `Box<dyn FnOnce + 'static>`. **Hoisting the runtime out of the two per-step loops is a separate behaviour-affecting change — do not fold it in.** |
| 2.10 | Merge `collect_mutation_node_hints` + `collect_mutation_edge_hints` into one walker | `df_planner.rs:6850,6983,6957,7086` | 125 | **Halves the exhaustive tail without weakening it — keep the no-`_`-arm property and the #131 comment on the survivor.** Both dedup with `!hints.contains(v)` against their own Vec and the consumers are order-sensitive: append to the two vectors independently, preserving per-vector dedup and visit order, or `startNode`/`endNode`/`id()` resolution picks a different variable. **Do not add `_ => {}` to make the merge "simpler"** — that reopens #131. |
| 2.11 | `build_time_from_projection` helper for the 3 copied time-of-day blocks | `datetime.rs:1652,2325,2413` (helper next to `build_date_from_projection` at `:1295`) | 58 | The `contains_key("millisecond") \|\| ... ` guard distinguishes *not specified* (inherit source sub-seconds) from *specified as 0*. A helper calling `build_nanoseconds` unconditionally, or treating a 0 result as absent, **breaks TCK Temporal3**. Carry that comment onto the helper. Take `&NaiveTime`, not the source `Value` — re-parsing changes the timezone handling that follows each site. |
| 2.12 | Extract `write_vertex_props(...)` from `apply_properties_to_entity`'s two node arms | `executor/write.rs:311-327,333-352` | 20 | The arms differ in what they do **after** the write and order matters: the Node arm rebinds `n.properties` filtering nulls; the map arm `retain`s `_`-prefixed keys, inserts non-null props, then rewrites `_all_props`. **The helper must return `enriched` and leave the binding update in each arm** — folding it collapses two distinct shapes and breaks `_all_props` readers. Preserve arm order: the second guard `vid_from_value(...).is_ok()` also matches `Value::Node`. Drop the dead `write_props.clone()`. |
| 2.13 | `shape_cursor(stream, projection_order, cypher, batch_size)` helper | `impl_query.rs:405-441,681-714` | 35 | **Land after (or with) 0.5** — it makes the missing TimeTravel guard structurally obvious. `batch_size` must be a **parameter**, not read from `self`, or the tx cursor starts honouring a per-call override it never had. Preserve the `.boxed()` fallback exactly — "simplifying" to unconditional chunking panics in `chunks(0)`. |
| 2.14 | `bin_predictions(...)` helper for `expected_calibration_error` / `debiased_ece` | `calibration.rs:949-963,994-1005` | 20 | Keep the empty-bin skip **inside** the helper — moving it to the fold lets a zero-n bin reach `(avg_y*(1-avg_y)/n).sqrt()` and produce NaN. |
| 2.15 | Sort `snapshot_neighbours` result; sort `Stratum.rules` and `depends_on` | `stratify.rs:163-167`, `compiler/mod.rs:183-186,192` | 8 | Sorting `Stratum.rules` changes **rule execution order within a stratum**. Datalog is order-insensitive at the fixpoint, but check `df_graph/locy_fixpoint.rs` for anything depending on the first rule (seed selection, warning-emission order, RuntimeWarning dedup). Existing tests assert `sccs.len()`, not order — they will not catch a regression. |
| 2.16 | Dedupe BTIC Allen predicates: `eval_btic_binary_predicate` → `uni_btic::predicates::*` | `uni-btic/src/predicates.rs:13`, `expr_eval.rs:2530` | 25 | **Rationale correction:** the scout claimed uni-btic is nearly unreachable. It is **live cross-language API** — `bindings/uni-db/src/btic.rs:7` reaches it via `uni_common::uni_btic` and 8 `#[pymethods]` call the canonical fns. Dedupe *because Python and Cypher surfaces must not drift*, not because it's dead. **Trap:** `expr_eval` destructures raw `(lo, hi, meta)` and never validates; `Btic::new` runs full `validate()` — a stored BTIC with an invariant-violating meta flips from a correct Bool to an `invalid BTIC` error. Confirm no fixture writes unvalidated meta. |
| 2.17 | `.ok()` → `.expect()` in `intersection` and `gap` | `uni-btic/src/set_ops.rs:29,70` | 6 | Unreachability re-derived and confirmed (guards establish INV-1; `build_result_meta` establishes INV-5/6). **Trap:** converts impossible-in-theory `None` into a panic reachable from arbitrary user Cypher. If a future meta bit widens `validate()`, failure goes from "query returns NULL" to "process panics". If BTIC values are ever deserialized without revalidation, return an error instead. **Do not touch `span`'s existing `.expect()`.** |
| 2.18 | `build_batch` + `to_i64` → `uni_plugin_builtin::algorithms::graph_compute`; `run_guest_algorithm` for extism+wasm only | extism/wasm/rhai/pyo3 `adapter_algorithm.rs` | 290 (not 600 — the 4 files total 1,114 lines) | **"Four ways" is refuted.** Rhai and pyo3 build `Arc<Mutex<AlgoSession>>` in-process and never call `registry.open`/`close`; the shared lifecycle is **extism+wasm only, two callers**. `acquire` signatures differ (`(&pool)` vs `(&pool, "algorithm")`). Rhai's `build_batch` uniquely names the offending field — folding to one message degrades it. **Do NOT sweep rhai into `run_guest_algorithm`: its `catch_unwind` is load-bearing** (Rhai does not isolate a panic in a registered host fn; it would unwind past the engine and crash the query worker). |
| 2.19 | Hoist `args_to_batch`, both `build_args_schema` overloads, `build_returns_field`, `validate_scalar_result` | extism/wasm `adapter.rs`, `adapter_aggregate.rs`, `adapter_procedure.rs` | 120 | **Put them in `uni_plugin::adapter_common::arrow_types`, NOT `batch_builder`** (whose doc scopes it to single-row batch/stream builders) and **NOT `uni-plugin-wasm-rt`** — that crate has no `uni-plugin` dep by deliberate design and `FnError`/`AggSignature` live in `uni-plugin`; the scout's `GuestInvoker`-in-wasm-rt proposal inverts the layering. The two evaluate paths use different error mappers (`extism_err_to_fn_err` vs `ipc_to_fn_err`) — the shared validator takes mapper and subject as params. Do not fold away `build_envelope` (`#[doc(hidden)] pub`, used by round-trip tests). Step 2 (`GuestInvoker` trait) is optional. |
| 2.20 | Unify plugin **argument** type-token mappers onto `arg_type_from_token` | `wire_translate.rs:23`, `pyo3/adapter_scalar_helpers.rs:25` | 40 | **"No stated rationale" is refuted** — `rhai/loader.rs:344-347` states it explicitly (value/list args let a variable-length seed set pass without per-arity generation). Unify **arguments only**, as a widening; leave yield/return surfaces on the narrow `algorithm_yield_datatype` from 0.9 — **land these two together.** **Trap:** return types differ (`Option<ArgType>` vs `Result<DataType, LoaderError>`) and `ArgType::CypherValue`→`LargeBinary` / `Vector{len:0}`→zero-length `FixedSizeList` are unmarshallable by `scalar_to_py`. Routing pyo3's *return* parsing through it reintroduces exactly the register-clean-then-fail bug 0.9 closes. Widening pyo3's arg vocabulary changes the Arrow schema args arrive in — `scalar_to_py` must gain those arms first. |
| 2.21 | `command_result_pyclass!` macro for the 5 identical Locy result shells | `bindings/uni-db/src/types.rs:3252,3280,3308,3336,3388` | 102 | Precedent is in-file: `diff_pyclass!` at `:3904`. **Leave `PyDeriveCommandResult` (`:3360`) hand-written** — different shape. **Trap:** fields are `pub(crate)` and constructed from `convert.rs`, so the macro must emit `pub(crate)`; the classes are `frozen`, which is what makes the `clone_ref` getter sound; `__getitem__`'s key set (`"type"` + field name) is published dict-like surface. |
| 2.22 | `BuilderState` in `core.rs`; both builder pyclasses become `{ state: BuilderState }` | `builders.rs:114-419`, `async_api.rs:979-1266`, `core.rs:477` | 200 | Verified: all 24 method names identical in both directions, all setter bodies identical after normalising `crate::convert::` vs `convert::`. Retires the `#[allow(clippy::too_many_arguments)]`. **Trap:** setters take/return `PyRefMut<'_, Self>` for fluent chaining on the *same* Python object — delegating must keep that shape (`slf.state.field = ...; slf`); an `&self -> Self` rewrite changes Python semantics from mutate-in-place to copy. `BuilderState` needs Debug + Clone. **Does not conflict with the cross-language symmetry contract** — that governs the Python-visible surface, which stays two mirrored pyclasses. |
| 2.23 | Single-pass prepared-param marshalling (drop the per-Value deep clone) | `types.rs:1546,1826,1936,1981` | 45 | Verbatim in all four: `.map(\|(k, v)\| (k.as_str(), v.clone()))` — a full deep copy of every param on the prepared-statement hot path (a 768–1536-float embedding is 3–6 KiB per execute). **Trap:** you cannot move the Value out while borrowing the key from the same tuple, so "just drop the `.clone()`" does not compile — split into `keys: Vec<String>` / `values: Vec<Value>` and zip. **Do not hoist marshalling above `Arc::clone(&self.inner)` / `prepared.borrow(py)`** — the comments at `:1566/1846/1949/1994` record the R13 ABBA deadlock; nothing may be held across the GIL-releasing `py.detach`. Also give `PyPreparedQuery::__repr__` the char-boundary-safe truncation `PyPreparedLocy::__repr__` already has. |

---

## Tier 3 — Dead code removals that are **breaking changes to published crates**

Every crate here publishes. **Batch these into one major-version commit — do not ship them in 3.1.x.** There is no `CHANGELOG.md` at repo root (verified); `crates/uni-plugin/CHANGELOG.md` is the only one, so removals from `uni-plugin` need an entry there and the rest need release notes.

| # | Item | Anchors | LOC | Crate | Note |
|---|---|---|---|---|---|
| 3.1 | `Writer::insert_vertex_partial` + `validate_vertex_constraints_partial` + `..._for_label_partial` + `L0Buffer::insert_vertex_partial`; drop the `partial` param | `writer.rs:3452,1947,1755,1772`, `l0.rs:695` | 175 | uni-store | **Trap:** name-matching deletes two live things — `L0Buffer::insert_vertex_with_labels_partial_impl` (`l0.rs:730`) is called by the live `insert_vertex_partial_full`, and `touched_needs_full_read`/`props_subset` are used at `writer.rs:3405`. Repoint doc refs at `config.rs:495`, `writer.rs:315/321`. |
| 3.2 | `PropertyManager::invalidate_vertex` / `invalidate_edge` + their 2 `does_not_panic` tests | `property_manager.rs:117,128` | 35 | uni-store | Both ignore their id arg and `cache.clear()`. **Do not "fix" them to be targeted** — caches are `LruCache` keyed by `(id, prop_name)`; a correct eviction needs a per-id key index that doesn't exist, and an iterate-and-pop half-measure is O(n) per SET. |
| 3.3 | `ForkScope::all_fork_local_indexes` + `all_fragment_counts` | `fork/scope.rs:196-206,235-248` (scout's line numbers off by a few) | 30 | uni-store | Superseded by the keyed probes; `docs/correctness-deferred.md:57,96` records the supersession — mark it closed. **Do NOT remove `fragment_count` / `has_fork_local_index` / `record_fork_fragment` / `register_fork_local_index`** — those are the live probes. |
| 3.4 | The FOREACH pipeline: `mutation_foreach.rs` (263 lines), `execute_foreach_body_plan`, both df_planner arms, the `LogicalPlan::Foreach` variant, ~12 single-line arms | `mutation_foreach.rs`, `read.rs:3088-3241`, `df_planner.rs:1278-1296,8212-8225`, `planner.rs:1707-1713` | 483 | uni-query | Grammar has zero `foreach` hits; no construction site exists; `LogicalPlan` derives only `Debug, Clone` so it cannot arrive via serde. Corroborating: the MERGE arm at `read.rs:3166` binds `on_match: _` and unconditionally CREATEs — a defect that could not survive a live path. **`LogicalPlan` is re-exported at `lib.rs:46`, so removing a variant breaks external embedders. Removing the arms *without* the variant leaves five walkers non-exhaustive and they will fail to compile — which is the #131 guard working correctly.** |
| 3.5 | `PathBuilder` + its `pub mod`/`pub use` | `executor/path_builder.rs`, `executor/mod.rs:12,24` | 260 | uni-query | Reachable only via the undocumented deep path `uni_query::query::executor::PathBuilder`. Paths are actually built by `df_graph/common.rs::build_path_struct_array` and `ResultNormalizer::map_to_path`. **Do not "preserve" the six unit tests by porting them onto `map_to_path`** — different semantics (Map→Path conversion, not incremental building). |
| 3.6 | `SourceType`, `resolve_source_type`, `validate_pair` + 5 tests | `similar_to.rs:175-213,492-510,722-757` | 96 | uni-query-functions | **Leave the 3 `SimilarToError` variants for 3.1.x** — removing enum variants is breaking and they cost nothing. **Precondition:** these are the *only* implementation of the "Vector query against FTS source" and "String query against Vector source with no embedding config" checks. Confirm `similar_to_expr.rs`'s plan-time `source_metrics` path re-implements them. **If it doesn't, this is a validation gap to file, not code to delete.** |
| 3.7 | Seven string-based temporal helpers | `datetime.rs:424-501` | 66 | uni-query-functions | **Delete by function, not by line range.** `is_duration_value` (`:447`, 5 live callers) sits *between* two of them, and `parse_duration_to_micros` (`:505`) immediately follows. A range delete of 424–510 takes both live functions. Do not re-add as delegations: `add_duration_to_date` truncates to `%Y-%m-%d` and `duration_to_micros` treats bare `Value::Int` as microseconds. |
| 3.8 | `single_row_record_batch` | `uni-plugin/src/adapter_common/batch_builder.rs:20-38` | 19 | uni-plugin | **Needs a `crates/uni-plugin/CHANGELOG.md` entry** (the scout omitted it). **Do not delete the module or its `pub mod`** — `batch_into_stream` in the same file is live from `uni-plugin-rhai/src/adapter_procedure.rs:23,95`. Trim the module doc to match. |
| 3.9 | 10 orphaned `*_core` fns in the Python binding | `bindings/uni-db/src/core.rs:29-113,251-283,434-461` + the empty `// Index Core` banner at `:243-246` | 145 | *not published* (`publish = false`, cdylib) — safe now | **Must fix imports in the same commit:** `use std::time::Duration;` (`:12`) loses its only two uses; `use ::uni_db::{Uni, Value, Vid};` loses `Value` and `Vid`. `unused_imports` is warn-by-default and CI builds `-D warnings`. Keep `pub use ...QueryCursor;` (`sync_api.rs:26,34`). `pub use` re-exports do not warn — leave them. |
| 3.10 | 4 superseded dict-shaped converters | `bindings/uni-db/src/convert.rs:924-1007,1133-1225` | 177 | not published | **Leave every private helper** — `locy_rows_to_py`, `locy_incomplete_to_py`, `command_result_to_py`, `derivation_node_to_py`, `modification_to_py` are all independently reachable from `locy_result_to_py_class`. A regex-driven delete that sweeps them (they sit immediately above, at `:604/617/688/742/901`) breaks the only path that hydrates `PyLocyResult.derived` — and **no Rust test covers it, because the crate has no Rust test target.** |
| 3.11 | The no-op `block_on` in `PySchema.label_info` | `bindings/uni-db/src/types.rs:3208-3213` | 6 | not published | **Keep the `// Build LabelInfo directly from the schema metadata.` comment at `:3214`** — it is the surviving explanation for the hardcoded `count: 0` / `is_indexed: false`, which otherwise reads as a bug. The scout's "latent panic" framing is overstated: a sync `#[pymethods]` getter runs on the Python thread, not inside the tokio context. |

---

## Tier 4 — Organization / pure code motion

Land **alone**, each as its own commit, and rebase dependents immediately. These buy no correctness.

| # | Move | Anchors | LOC | Trap |
|---|---|---|---|---|
| 4.1 | `read.rs:4094-4637` + `read.rs:5132-5518` + `write.rs:606-1260` → `executor/bulk_io.rs` | uni-query | 1,584 | Several moved fns are private (`backup_to_local`, `backup_to_cloud`, `open_parquet_from_cloud`, `write_parquet_to_cloud`, write.rs's export/read helpers) — reachable today only via same-module `impl Executor`. Any called from the *other* file needs visibility widened; check both directions. **Will conflict with any in-flight branch touching read.rs/write.rs.** Only after it lands, factor the shared src/dst-column resolution and `insert_edge` tail out of the CSV/Parquet importers. |
| 4.2 | `UniBuilder::build` (914 lines, 22% of `api/mod.rs`) → sequenced private methods | `crates/uni/src/api/mod.rs:3111-4024` | ~950 · **risk: med** | The scout logged this at LOC 0. **Risk is ordering, not mechanics.** Undocumented order-dependencies: plugin registry built early *specifically* so `PropertyManager` can see it; read-only carve-outs (auto-flush skip, L0 lift-out) interleaved with unrelated phases; WAL replay must follow start-version/HWM determination *including* lost-manifest-pointer recovery; `max_forks` counts Tombstoned, so recovery-candidate resolution must not hoist above registry construction. Extracting into `&mut self` fns can silently reorder Arc construction relative to `shutdown_handle.track_task`, **orphaning background tasks from shutdown.** |
| 4.3 | `uni-plugin-custom/src/lib.rs`: inline `pub mod procedures` (`:504-1654`) → `procedures.rs`; `DeclaredPluginStore` + 2 cycle helpers (`:1666-1885`) → `store.rs` | uni-plugin-custom | 1,370 | The inline module does `use super::{... DeclaredPluginStore ...}` at `:527`; after the move that path dies unless re-exported at the crate root — and `DeclaredPluginStore` is `pub`, so `uni_plugin_custom::DeclaredPluginStore` **must** be preserved or it's a break on a published crate. `would_introduce_cycle`/`chain_starting_at` are private and consumed by both the store and `mod tests` via `use super::*` — promote to `pub(crate)` or the cycle tests silently lose their target. Split `mod tests` (`:1887-2199`) alongside. |
| 4.4 | `uni-plugin-host/src/triggers.rs` (2,452 lines) → `triggers/{mod,predicate,router,events,deferral}.rs` | uni-plugin-host | 2,200 | The single `mod tests` at `:2196` does `use super::*` and reaches private items from **all four groups** (`RouteEntry`, `MutationRow`, `EcBucket`, `DeferredItem`, `PersistedDeferral`, `phase_index`, `mask_to_discriminant`, `vid_to_i64`, `eid_to_i64`). A naive four-way move breaks it — every one needs `pub(super)`/`pub(crate)` or the tests split in the same commit. **Leave the local `arrow_ipc_*` alone**: `uni-plugin-wasm-rt` is deliberately below `uni-plugin`, so taking the dep is a layering decision, and `decode_batch` is not drop-in (`Result<Option<_>, IpcError>` vs `Result<_, String>`, and `encode_batch` can now *fail* on secret handles, turning an infallible sidecar write into an error path). |
| 4.5 | Extract `_UniSessionBase` mirroring `_QueryBuilderBase` | `uni_pydantic/session.py`, `async_session.py` | 220 · **risk: med** | **Sequence AFTER 0.13** so the shared `_result_to_model` lands correct once. **Do NOT pull `_load_relationship` into the base** — the async override at `async_session.py:578-588` deliberately *raises* because the descriptor protocol (`fields.py:239`) is synchronous. Hoisting the sync impl silently gives `AsyncUniSession` a method that calls `.query()` on an async handle and returns an un-awaited coroutine that iterates as garbage. Same for `transaction()` — `@contextmanager` vs `async def`, same name, incompatible protocols. Also restore the precise return annotations on async `explain`/`profile` (`-> Any` at `:433,437`) and fix the `:583-584` docstring naming a nonexistent `_async_load_relationship`. |
| 4.6 | Extract `grammar/mod.rs:179-492` → `grammar/diagnostics.rs` | uni-cypher | ~300 moved, ~25 net removal | **Split the proposal.** The file move is worth doing alone. The generic `map_error` saves ~25 lines (not 90) at the cost of a 6-parameter signature with two closures and an `Option<fn>` — prefer a small `Option<&'static str>` helper for the four shared probes. **Trap:** the reserved-keyword gate is asymmetric on purpose (Cypher passes an empty extra-keyword slice, Locy passes `LOCY_RESERVED_KEYWORDS`; `expects_identifier` matches `identifier \| identifier_or_keyword`, `expects_locy_identifier` only `locy_identifier`). Collapsing into one generic with a defaulted param risks giving Cypher the Locy keyword set — **uni-tck/uni-locy-tck assert on error categories.** Land the move, verify TCK green, then the generic separately. |
| 4.7 | Move `Uni::build_promote_baseline` body → `uni_fork::diff` | `crates/uni/src/api/mod.rs:1747-1808` | 65 | **Bill as co-location, not dedup** — the two loops populate different types and are not textually mergeable; net removal is the 2-line escaping idiom. **`pin_to_version(&info.parent_snapshot_id)` and the `parent_snapshot_id == "uninitialized"` early-return must stay facade-side.** `ForkQueryHost` has no pin API; moving the pin into uni-fork would scan primary at HEAD instead of at the fork point — turning delete-promotion into deleting rows primary added *after* the fork. The empty-baseline early return is what makes a never-flushed primary safe. |
| 4.8 | Move `wait_for_holders_drained` / `drop_fork` / `drop_fork_cascade` bodies to `impl UniInner`; delete the `ManuallyDrop` + `unsafe { ptr::read }` | `fork_maintenance.rs:66,78-85`, `api/mod.rs:1257,1289,1454` | **210** (scout said 40 — a 5× underestimate) · **risk: med** | This removes the only `unsafe` in the crates/uni facade, but relocates ~210 lines through the **fork drop 2PC** — the most safety-critical path in the fork subsystem. (1) `Uni::drop_fork`'s rustdoc contains a **compiled doctest** — it must stay on the `Uni` delegation or the doctest stops running. (2) `drop_fork` holds `fork_registry.name_lock(name)` for the whole sequence to serialize against `fork(name).build()` — must move verbatim, and `drop_fork_cascade` must keep calling through per-node `drop_fork` to inherit it. **Do not shortcut with `mem::forget` on a cloned-Arc `Uni`** — that leaks a strong ref and the database can never shut down. |

---

## Tier 5 — Test-layout and documentation hygiene

Cheap, independent, no code risk. Good filler commits between the heavier tiers.

| # | Item | Anchors | LOC | Note |
|---|---|---|---|---|
| 5.1 | Add `"model"` to `LOCY_RESERVED_KEYWORDS` | `grammar/mod.rs:386-388` (`locy.pest:86-88` has 10 tokens, the Rust table has 9) | 12 | Reproduced: `CREATE RULE fold` gives the actionable `ReservedKeyword ... use backtick-quoting`; `CREATE RULE model` gives a raw `InvalidRuleDefinition --> 1:13`. **Anti-drift test:** pest token names are **not** mechanically lowercasable (`QUERY_KW`→`query`, `VALID_AT`→`valid_at`) — hand-maintain the mapping or drive both from one const list; **do not write a naive `to_lowercase()` extractor over the .pest source.** **Trap:** changes the error *message* for every parse failure whose token is `model` — grep TCK features for the current `InvalidRuleDefinition`/`InvalidAlongClause` strings first. |
| 5.2 | Delegate `locy_walker::unquote_string_literal` → `walker::unescape_string` | `locy_walker.rs:671-704` (34 lines) vs `walker.rs:1099-1160` (62) | 40 | Category is **behaviour divergence**, not duplication — they are not near-copies. The weak copy misses `\r`, `\b`, `\f`, `\uXXXX`/`\UXXXXXX` with error reporting, and doubled-quote escaping. `unescape_string` returns `Result`, so `unquote_string_literal` must become fallible (all three call sites already return `Result<_, ParseError>`). Fix the self-contradictory docstring at `:667-670` regardless. **Trap:** three currently-lenient inputs become hard parse errors, and a doubled quote in a model alias changes meaning (`''`→`'`). Grep `examples/`, `demos/`, `crates/uni-locy-tck/**/*.feature`, `website/docs` for `USING xervo(` and `VERSION '` first. |
| 5.3 | Calibration metrics: length guard + `auc` degenerate guard | `calibration.rs:918,931,948,993,1026,1050`; `validate_inputs` at `:1092` | 50 · **risk: med** | `auc(&[0.5], &[true, true])` **panics**: `n = preds.len()`, `n_pos` counted over `labels`, `n_neg = n - n_pos` underflows `usize`, and the rank sum indexes `ranks[i]` with a labels index. All six are re-exported from `lib.rs:12-18`. **Converting to `Result` is semver-major** and cascades into `locy_validate.rs:245-249`, `locy_calibrate.rs:327-331` and the tck steps at `then_evaluate.rs:1014-1017`. **Minimal correct patch now:** `let n = preds.len().min(labels.len())` at the top of each, plus `if n_pos == 0 \|\| n_pos >= n { return 0.5; }` in `auc` before the subtraction. Do not silently clamp `n_pos` — a mismatch is a caller bug. |
| 5.4 | Fix misattached doc comments + dead doc paths | `rhai/graph_compute.rs:1040-1046,1315-1323`; `uni-plugin/src/scheduler.rs:9,413`; `uni/src/api/mod.rs:245`; `surfaces/mod.rs:6,11,66` | 20 | Two doc blocks are glued to the *following* item (rustdoc attaches them there, leaving `register_kernels` at `:1139` and the reachability test at **`:1408`** undocumented). `crates/uni/src/scheduler.rs` does not exist — the driver is `crates/uni-plugin-host/src/scheduler.rs`, and the dead path appears **3×**. Surface count says 21 at `:6` **and `:11`** vs 22 at `:66`; `SurfaceKind` has 22 variants. **Trap:** when moving the `register_kernels` doc, do **not** drag the "Registers the short forms declared by `OPTIONAL_ARITIES`" paragraph — cutting one line off leaves `register_optional_arities` undocumented and drops the "Deliberately not pushed onto the registered-kernel list" note, which is the *rationale for the reachability contract holding*. Losing it invites a future contributor to "fix" the count by pushing the optional arities, weakening the check. |
| 5.5 | Consolidate 4 unambiguous plugin test binaries; add `[[test]]` rationale to the rest | `uni-plugin`, `uni-plugin-custom`, `uni-plugin-apoc-core`, `uni-plugin-extism` | 40 | **Reframe: not a cap violation.** `docs/test_layout.md:15` caps at 3 and no crate exceeds it (max 3). The breach is the narrower `:30-34` rule that a 2nd/3rd binary carry a `[[test]]` entry with a recorded reason — none do. **For `uni-plugin-pyo3` and `uni-plugin-builtin`, check the documented process-isolation exception first and add the `[[test]]` entry *with* the reason rather than merging.** Merging two Python-interpreter-initializing tests into one binary sharing one embedded CPython + GIL is exactly the exception test_layout.md carves out (and the repo has prior art for spurious PyO3 concurrency failures). `builtin`'s consolidated binary is a *registry* test — merging means shared process-global registry state. `extism/tests/it.rs` uses `#[path = "it/..."]`; `wasm/tests/scratch_wasm_e2e.rs` carries `#![cfg(feature = "wasmtime-runtime")]` + `#![allow(deprecated)]` that must survive. Separately: delete the stale `// BUG:` markers sitting above *fixed* assertions (`apoc-core/bug_repros.rs:81`) and rewrite the `uni-plugin/tests/bug_repros.rs:1-7` header. |
| 5.6 | `autotests = false` + `[[test]]` for `uni`, `uni-crdt`, `uni-common` | `crates/uni/Cargo.toml`, `uni-crdt`, `uni-common` | 45 | Preventive — no crate is over cap today. `uni`'s two extras carry legitimate documented rationale (mutually-exclusive `provider-onnx-dynamic`; process-global failpoint isolation) — declare them as explicit `[[test]]` stanzas with the reason as a comment. `uni-crdt`'s two do **not** qualify (`bugs_repro.rs` is "a separate topic", which test_layout.md explicitly rejects; `registry_dispatch.rs` builds a fresh `PluginRegistry::new()` per test with no process-global state) — fold them in. `uni-common` has 3 loose `repro_*.rs` and no `integration.rs`. **Leave `uni-cli` alone** — its binaries link only `std::process::Command` + `CARGO_BIN_EXE_uni`. **Trap:** `autotests = false` converts an accidental new `tests/foo.rs` from "an expensive fourth binary" into "a file that silently never runs" — strictly the *more* dangerous failure. Needs a CI check or a header comment in `tests/integration.rs`. Re-run `cargo nextest run -p uni-crdt` and confirm the test count is unchanged. |
| 5.7 | Fix the 4 false WASM-loader docs | `bindings/uni-db/Cargo.toml:24-26`; `website/docs/plugins/quickstart.md:18-22`, `authoring.md:395-397`, `loaders/index.md:25-26` | 12 | `default = []` — **no shipped wheel** has `load_wasm_component`/`load_wasm_extism`. The Cargo comment 27 lines above contradicts itself. **The scout's two worst-cited items are refuted and two worse ones were missed:** the `.pyi` stubs are *deliberate* and allowlisted at `test_stub_drift.py:78-87` with a 5-line rationale (plus a `python-wasm-tests` CI lane that builds with the features and asserts presence) — **leave them**; `extism.md:180-183`, three lines above the cited line, already states the gate — **leave it**. The actual falsehoods are `quickstart.md` ("The default `uni-db` wheel already bundles wasmtime… Both are compiled into the published Python wheel") and `authoring.md` ("The default wheel ships the Component Model loader"). **Do not "fix" by restoring `default`** — `Cargo.toml:44-50` documents ~7 MiB of wasmtime+cranelift as the reason, against a PyPI size budget across six crates. |
| 5.8 | `AsyncUniSession`: add `update_edge` + `get_edge` | `session.py:540-604` → `async_session.py` | 66 | An async OGM user can create and delete edges but cannot read or update edge properties. **Trap:** do not transliterate `self._validate_edge_endpoints` — `AsyncUniSession` has no such method; it reaches it statically as `UniSession._validate_edge_endpoints(...)` (`async_session.py:367,386`). Sync `get_edge` passes `session=self` into `from_properties` (`:598`); handing an `AsyncUniSession` there means lazy relationship access hits the raising `_load_relationship` — keep `get_edge` dict-only on async or verify the model's lazy paths. **Leave `begin` out** (async `transaction()` serves the role) and say so in the class docstring — while fixing that docstring, which currently misdescribes its own usage (`async with session.transaction()` — it's `async def`, callers need `async with await ...`). Write the OGM sync/async parity rule into CLAUDE.md. |
| 5.9 | Fix `PromoteBaseline`/`DiffVertex` UID doc contract | `uni-fork/src/types.rs:147-149,162-163` | 15 | **Take option (b), documentation only.** Three UID schemes exist: `content_uid_with_ext_id` (double-fold, used by `run_promote`), the single-fold `compute_vertex_uid(label, ext_id, props)` used by `scan_label_nodes`, and the doc at `types.rs:147` claiming `compute_vertex_uid(label, None, properties)` — which matches neither. The diff is self-consistent, so this is a doc/API-contract defect, **not** a wrong answer. State that the UID is a diff-local pairing key deliberately **not** the registered storage `_uid`. **Trap — do not take option (a):** routing `scan_label_nodes`/`scan_edge_type` through `content_uid_with_ext_id` changes every bucketing key, hence `edge_uid` via `compute_edge_uid`, hence Phase 7d multi-edge identity and the H4 collision-warning path (`diff.rs:170-180`), and would silently change `diff(a,b).invert() == diff(b,a)` witnesses. If ever done, it must cover vertices **and both edge endpoints** in one change with the full fork suite re-run. |
| 5.10 | `InstancePool` doc honesty | `uni-plugin-wasm/src/lib.rs:12-13`, `uni-plugin-extism/src/adapter.rs:49`, `feedback/uni-plugin-wasm-rt.md:28` | 15 | The pool builds fresh and drops every call. Rewrite to state fresh-instance-per-invoke for state isolation with `max_instances` as a pure concurrency semaphore. **Leave `warm_count`, `PoolMetrics::hits` and the type name alone** — `pool.rs:54-60` documents them as deliberate compat shims, `PoolConfig` is **not** `#[non_exhaustive]` so deleting a field is a hard compile break for every downstream struct literal (`#[deprecated]` on a field does not stop a literal requiring it), and `PoolMetrics::hits` is read by the `host.metric_counter` guest-visible surface. If the name bothers, do it at 4.0 with the field removal — not as a deprecation dance across a minor. |

---

## Do NOT do

### Refuted outright

**`uni_common::sync` is NOT dead.** (`uni-common/src/sync.rs`) The scout's grep was simply wrong. It is used across the workspace in *committed* code: `uni-common/src/core/schema.rs:8` imports `acquire_read`/`acquire_write` and calls them at 20+ sites; `uni-store/src/runtime/wal.rs:16` uses `acquire_mutex` at 7 sites and **exposes `LockPoisonedError` in a public return type at `wal.rs:475`**; `storage/manager.rs:742` likewise. `git log` shows the wal.rs usage predates this audit and `git diff` on schema.rs is empty. **Deleting it fails to compile three crates and would have shipped as a semver removal from a published crate whose public API appears in uni-store return signatures.** The only salvageable sub-observation — whether the ~46 `(read|write|lock)().unwrap()` sites under `crates/uni/src/api` should adopt these helpers — is a standalone, unverified hardening proposal that must be re-scoped on its own.

**Do not feature-gate the deprecated `ScratchGraph` stack.** (`uni-plugin-builtin/.../scratch.rs`, 1,677 lines) The finding classifies it as dead code; it is the opposite. `graph_compute/mod.rs:63-64` states the re-export exists "for one more minor release so downstream code gets a deprecation warning rather than a hard break; removed at the next major (§13.4)". It is deliberately-live public API in its documented removal window. Putting the `pub use` behind a non-default feature removes it from every downstream default build — **which IS the hard break the comment exists to prevent** — and turns today's downstream warning into `E0432`. The gating also cascades: `differential_tests.rs:2533` lives under `src/`, so `cargo nextest run -p uni-plugin-builtin` would stop covering it by default; `crates/uni/tests/.../graph_compute_pagerank.rs` is in a *different* crate and would need the feature plumbed through dev-deps, whereupon Cargo feature unification re-enables it and reinstates the compile cost the change was meant to avoid. **Re-file as a 4.0 removal task.** If build cost is genuinely the concern, run `cargo build --timings` first — this is one module against datafusion/lance/candle.

**Do not delete `PooledInstance::take`, and do not "fix" its doc.** (`uni-plugin-wasm-rt/src/pool.rs:294`) The dead-code half is true (one caller, its own test). The "doc contradicts its body" half is **refuted by reading the full doc**: `:285-293` says "without freeing its concurrency slot **via the pool**" — i.e. not via `pool.release` — and then states outright that "`take` still moves the instance out **and decrements the live counter**". The body matches. The scout's alternative fix (stop decrementing `live`, make the caller do it) **leaks a concurrency slot permanently** for any caller that forgets and immediately fails the existing assertion at `pool.rs:419`. Leave it, or `#[deprecated]` now and remove at 4.0 alongside `warm_count`/`hits`.

### CONFIRMED items where the naive version is wrong

**Do not consolidate the MVCC readers on the assumption that batch variants are first-wins.** (Tier 2.7) The "tie-break has already drifted" claim is false. Inverting the batch tie-break returns stale props/labels after delete+reinsert at the same version — the exact review-C2 bug these functions close.

**Do not decode plugin `LargeBinary` with a hard error on failure.** (Tier 0.7) No plugin loader stamps `uni_raw_bytes` on a yield field. The fallback-to-`Value::Bytes` branch is load-bearing; without it every plugin yielding genuine opaque bytes breaks.

**Do not delete `map_variables`'s wildcard by recursing into `Exists`/`CountSubquery`/`CollectSubquery`.** (Tier 1.3) They own `Box<Query>`, not `Expr` — impossible with the current signature. Explicit pass-through arms with a comment, not a wildcard.

**Do not delete the `store_utils` retry loop.** (Tier 1.10) It is the only retry for the WAL, fork WAL, fork id-alloc and bulk flush-intent paths, whose stores are not all behind `ResilientObjectStore`.

**Do not delete `normalize_path_if_needed` yet.** (`read.rs:822`) The claim that the pre-pass "adds nothing" is **refuted**: `ResultNormalizer::normalize_value` has exactly one call site in the workspace (`impl_query.rs:202`) and it is gated on `normalize == true`. The streaming cursor paths call `rows_for_results(..., false)` at `impl_query.rs:423,698` and **nothing downstream normalizes them** — the doc comment at `:187-188` saying otherwise is simply wrong. Deleting the pre-pass silently changes `Session::query_stream`/`QueryCursor` output: `_id`→`_vid`, unstringified `_src`/`_dst`, no `_type_name`→`_type` rename, and loss of the `ensure_properties_map` empty-map guarantee (without which `extract_properties_from_field_or_inline` falls through to the `_all_props` and inline-field branches and can surface a *different* property set). **First fix the false doc comment**, then decide the streaming contract deliberately: either apply `normalize_value` in the cursor stream and *then* delete, or keep the pre-pass and document it as the cursor path's normalizer. Option (a) is the right end state but is a user-visible change needing a test that streams `RETURN p` and `RETURN collect(p)` and asserts shape parity.

**Do not add the four `btic_*` names to `Expr::is_aggregate`.** (`ast.rs:1197`, `planner.rs:650`) The drift is real but the claimed consequence at `planner.rs:6957` is **refuted**: `aggregate_column_name` is literally `expr.to_string_repr()` (`planner.rs:8097-8099`), so the ORDER BY map and the rename-Project cannot disagree. Tracing the actual routing shows no live defect. Adding the names flips `btic_min/max/span_agg/count_at` from the compound-aggregate branch to the bare-aggregate branch across ~6 call sites in both RETURN and WITH planning, changing which expressions land in `aggregates` vs `compound_agg_exprs` — with only one test file covering it, and `Expr::is_aggregate` is published uni-cypher API. **Downgrade to a comment at both list sites, or hoist a single `is_builtin_aggregate_name` with no membership change.**

**Do not thread `WallClockMillisPerCall` grants into the WASM loaders.** (`wasm/loader.rs:826`, `extism/loader.rs:686`) The scout's grep claim is false — it has **four** consumers outside rhai (`host_services.rs:132-138`, `linker.rs:418-425` documented as the host-authoritative ceiling, `bridge.rs:445-451`, `extism/host_svc/net.rs:7`). Clamping `timeout_ms` to it would collide with `linker.rs:418`, which already uses the same grant as the host-service ceiling — double-applying makes a legitimate host call consume the guest's whole budget. `capability.rs:541` also says bucket C is grantable "**via the plugin manifest**", so "clamp manifest against grant" is partly circular, and `extism/loader.rs:690-692` records the permissive behaviour as a reviewed decision ("review H15"). **Reclassify as hardening and scope to the three capabilities with genuinely zero readers: `ConcurrentInstances` (unambiguous — derive `PoolConfig.max_instances` at `extism/loader.rs:720`, `wasm/loader.rs:979,1392`), and `MemoryBytes`/`FuelPerCall` only after resolving where a host-authoritative grant is supposed to come from.** Note `MemoryBytes` is bytes and `memory_max_pages` is 64 KiB pages — a naive `grant/65536` clamp shrinks every plugin below `DEFAULT_MEMORY_MAX_PAGES` (1 GiB); and `fuel_per_call` is deliberately `Option` with no host default because "fuel costs are opaque to plugin authors" (`loader.rs:806-809`).

**Do not delete `compile_with_modules`.** (`uni-locy/src/compiler/mod.rs:72`) It **is** called — `compile` delegates to it at `mod.rs:26`. Only `compile_with_oracle` is caller-free. If the `CompileOptions` refactor happens, keep all six as one-line shims (published API, and `compile_with_modules` is on the hot path) and land it **after** the oracle is wired (Tier 1.5). Prototype the `Default` impl first: `MonotonicityOracle` is `&'a dyn Fn(&str) -> Option<bool>`, so the struct needs a lifetime parameter and cannot simply `#[derive(Default)]`.

**Do not delete `UniLocyCompileError`/`UniLocyRuntimeError`.** (`bindings/uni-db/src/exceptions.rs:463-471,536-537`) They are registered, stubbed and documented in **four** places (the scout found two: add `docs/complete_python_api.md:2034-2035` and `skills/uni-db/references/python-api.md:622-623`), and nothing raises them because upstream stringifies (`session.py`→`UniError::Query { message: format!("LocyCompileError: {e}") }`). **Prefer wiring them up** — thread `uni_locy::LocyCompileError` through `Session::compile_logy` at `crates/uni/src/api/session.rs:731`. Deletion is a breaking change to a published, documented Python exception hierarchy (`except UniLocyCompileError` → `ImportError`) and would fail `test_stub_drift.py` unless the `.pyi` is swept in the same commit — a 4.0-flavoured removal, not a cleanup.

### Not worth doing at all

- **The generic `map_error` in `grammar/mod.rs`** — ~25 lines saved for a 6-parameter signature with two closures, guarding error categories the TCK asserts on. Do the file move (4.6); skip the generic.
- **The shared "build bounds + meta + construct" tail in `uni-btic/set_ops.rs`** — four parameters to save six lines. The `.ok()`→`.expect()` change (2.17) is the whole value.
- **Renaming `InstancePool` to `InstanceFactory`** — churn with no correctness payoff; the fields it complains about are already self-documenting compat shims with the most honest comments in the file.

### Removes a compile-time exhaustiveness guarantee

Only one item in this plan does, and only transiently:

- **Tier 3.4 (FOREACH removal).** Deleting the ~12 `| LogicalPlan::Foreach {...}` arms from the five deliberately-exhaustive `df_planner` walkers is safe **only if the variant is removed in the same commit**. Removing arms alone leaves them non-exhaustive and they fail to compile — which is the #131 guard working as designed. Do not "fix" that failure with a `_ =>`.

Two items **add** a guarantee and should be framed that way in their commit messages: **1.8** (`plan_children` + `LogicalPlan::input()`) and **1.3** (`map_variables`). One item **halves** an exhaustive tail without weakening it: **2.10** (mutation-hint walkers) — keep the no-wildcard property and the #131 comment on the survivor.

Two items **cannot** gain a guarantee no matter what: **0.11** (`value_to_py`) and **0.12** (`PyForkStatus`). Both cross a `#[non_exhaustive]` crate boundary; the wildcard is mandatory and only its *value* changes. Anyone who reads "exhaustiveness" into these will break the build.

---

## Breaking-change ledger

Published crates: `uni-query`, `uni-store`, `uni-plugin`, `uni-plugin-host`, `uni-common`, `crates/uni`, plus `uni-query-functions`, `uni-locy`, `uni-cypher`, `uni-plugin-wasm-rt`, `uni-fork` (per `release.yml`). `bindings/uni-db` is `publish = false` + cdylib, so its removals carry no Rust API contract.

| Change | Crate | Severity |
|---|---|---|
| 3.1 `Writer::insert_vertex_partial` (re-exported `lib.rs:22`) | uni-store | major |
| 3.2 `PropertyManager::invalidate_*` (re-exported `lib.rs:21`) | uni-store | major |
| 3.3 `ForkScope` accessors | uni-store | major |
| 3.4 `LogicalPlan::Foreach` variant (re-exported `lib.rs:46`) | uni-query | **major — external embedders pattern-match this** |
| 3.5 `PathBuilder` | uni-query | minor-breaking |
| 3.6 `SourceType`/`resolve_source_type`/`validate_pair` | uni-query-functions | major (defer the 3 error variants) |
| 3.7 seven `datetime` helpers | uni-query-functions | major |
| 3.8 `single_row_record_batch` | uni-plugin | minor-breaking + CHANGELOG entry required |
| 0.2 Locy `prev`-in-comparison → hard `ParseError` | uni-cypher | **behaviour-breaking on the parse surface** |
| D6 query timeout → `UniError::Timeout` | uni / bindings | **breaking**: `UniQueryError` → `UniTimeoutError` for an elapsed deadline |
| 1.4 CHECK `=` numeric coercion on the tx path | uni-store | **behaviour-breaking** (a currently-rejected CHECK starts passing) |
| 1.5 registry-backed oracle | uni-locy | **behaviour-breaking** unless it falls back to default on registry miss |
| 5.2 Locy string unescape delegation | uni-cypher | behaviour-breaking on 3 lenient inputs |
| 5.3 calibration `Result` conversion | uni-locy | major — **use the length-guard patch instead** |
| 0.9 algorithm yield restriction | plugin loaders | behaviour-breaking: "loads fine" → "fails to load" |
| 0.14 eager-load returns models | uni-pydantic | **user-visible break on a shipped OGM** |

---

## Suggested commit sequence

Each step compiles and tests independently. `cargo nextest run` throughout; never `cargo test`.

**Phase A — latent bugs, no API change (14 commits, land first, ship in 3.1.x)**
~~`0.1`~~ → ~~`0.6`~~ → ~~`0.8`~~ → ~~`0.11`~~ → ~~`0.12`~~ → `0.5` → `0.7` → ~~`0.10`~~ → `0.3` → `0.2` → `0.4` → `0.13` → `0.14` → `0.9`+`2.20`(together)

**6 of 14 landed** (struck through). *Actual* order taken so far: `0.1` → `0.6` → `0.8` → `0.11` → `0.12` → `0.10`, i.e. 0.10 was pulled forward ahead of 0.5/0.7 because it is a single-file binding change in the same area as 0.11/0.12 and shares their build. Next is `0.13` → `0.14` (the one hard ordering constraint), then `0.5`, `0.3`, `0.9` in any order. See **Implementation status** at the top of this document for evidence and corrections.

Ordering rationale: 0.1/0.6/0.8/0.11/0.12 are single-file and trivially revertible — get them in while the tree is quiet. 0.13 strictly precedes 0.14. 0.9 and 2.20 land together because 2.20 must not widen the yield surface 0.9 narrows.

**Phase B — scope-decision bugs (7 commits)**
`1.10a` (the typed-`NotFound` one-liner only) → `1.8` → `1.9` → `1.6` → `1.7` → `1.1` → `1.2` → `1.4` → `1.5`

1.8 before 1.3/3.4 — the exhaustiveness work in `plan_children` should land before FOREACH removal touches the same walkers. 1.4 and 1.5 last in this phase; both change what compiles/validates and want a quiet tree.

**Phase C — safe consolidations (interleave freely)**
`2.6` → `2.2` → `2.1` → `2.13` (after 0.5) → `2.12` → `2.5` → `2.4` → `2.3` → `2.7` (test first) → `2.9` → `2.10` → `2.11` → `2.14` → `2.15` → `2.16` → `2.17` → `2.18` → `2.19` → `2.21` → `2.22` → `2.23`
**One combined commit:** `1.3 + 2.8` (not independently revertible).

**Phase D — hygiene (any time, good gap-fillers)**
`5.4` → `5.7` → `5.1` → `5.9` → `5.10` → `5.5` → `5.6` → `5.3` → `5.2` → `5.8`

**Phase E — code motion (one commit each, alone, rebase dependents immediately)**
`4.6a` (file move only) → `4.3` → `4.4` → `4.7` → `4.1` → `4.5` (after 0.13) → `4.8` → `4.2` (last — highest ordering risk)

**Phase F — the 4.0 breaking batch (one release, one set of notes)**
`3.9` + `3.10` + `3.11` (bindings, unpublished — can actually go earlier and safely) → then `3.8` → `3.3` → `3.2` → `3.1` → `3.5` → `3.6` (only after confirming `similar_to_expr.rs` re-implements the checks) → `3.7` → `3.4` (last — largest, and it is the one that removes an enum variant from a published `LogicalPlan`).

Also in this release: the deferred `SimilarToError` variants, `PooledInstance::take`, `warm_count`/`PoolMetrics::hits`, and the `ScratchGraph` deletion + bench/test port.

---

## Honest assessment

**The real value in this document is Tier 0 and Tier 1.** Twenty-one findings, roughly 500 lines of change, closing an id-reuse hazard, a snapshot-isolation escape, four silent Locy correctness bugs, a plugin-output decode gap, a cursor-cancellation no-op, two OGM data-loss paths, and two fail-open Python conversions. That is the work.

**Tier 4 is ~5,700 lines of motion for zero correctness.** 4.2 (`UniBuilder::build`) is the highest-risk item in the entire plan — the scout logged it at LOC 0 and the verifier at ~950 with med risk and five undocumented ordering dependencies. If time is limited, **skip 4.1, 4.2 and 4.8 entirely**; they buy readability against a real chance of a boot-sequence regression. 4.3, 4.4 and 4.6a are cheap and safe.

**Tier 3 is ~1,000 lines of genuinely dead code, but it is all semver-gated.** Landing it in 3.1.x is wrong; landing it never is also fine. It costs nothing to leave in place. Prioritize 3.9/3.10/3.11 (the bindings, `publish = false`, ~330 lines, zero contract) — those are free wins available today.

**Three things in this audit would have broken the build or shipped a regression if executed as written:** deleting `uni_common::sync`, feature-gating `ScratchGraph`, and deleting `normalize_path_if_needed`. A fourth, adding `btic_*` to `Expr::is_aggregate`, would have changed planner routing across six call sites to fix a consequence that does not exist. The pattern is consistent with the prior audit's failure mode: the *location* of the smell was right every time; the *root cause* and the *proposed remedy* were wrong roughly a third of the time. Verify the consequence, not just the shape, before touching anything below Tier 1.