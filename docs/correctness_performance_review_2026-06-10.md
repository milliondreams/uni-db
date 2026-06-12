# Codebase Review: Correctness & Performance — 2026-06-10

**Scope:** Full workspace (~240k LOC, 27 crates) of the `uni` embedded multi-model graph
database — storage engine, OpenCypher query engine, Locy logic engine, public API + Python
bindings, plugin subsystem, graph algorithms, CRDTs, and test harnesses.

**Method:** 11 parallel specialist reviews (per-subsystem correctness + performance, a
cross-crate concurrency audit, and a workspace-wide panic/overflow/unsafe sweep). The 7
most severe findings were then **adversarially verified** by reading the exact code paths;
one was confirmed by running a live query. Each item below is tagged accordingly.

- **✅ VERIFIED** — personally traced to exact lines (and, where noted, reproduced live).
- **Credible** — specific line-level trace from a specialist agent; not independently re-verified.
- **REFUTED** — original claim investigated and found not to hold.

The original review was read-only analysis. The **Fix status** section below tracks what has
since been remediated.

---

## Fix status — updated 2026-06-11

All remediation lives on local `main` (FF-merged, **not pushed**). Each fix was done
verify-first (failing repro → fix → green) with full regression gates (workspace nextest 4756,
openCypher TCK 3925×2, Locy TCK 501×2, pytest 815+213, clippy/fmt/doc clean). Commit groups:

- **Tier 1 & 2** — `03944c135` (+ the SSI/builder commits): Criticals **#1–#6**, builder
  authz/hooks/read-only bypass, parameterized `FOR UPDATE` no-lock.
- **Tier 3** — `6f2e3c8e7`: Criticals **#7–#8**; MERGE fast-path fails-open; adjacency-warm +
  time-travel error-swallowing; `VCRegister` convergence; `QueryBuilder::explain` /
  `PreparedQuery` guards.
- **Tier 4** — `3790bb822` → `293a0868a` → `936838382`: flush-finalizer wedge; non-tx/rotation
  race; unique-constraint flush-window + WAL-recovery hole; stale property-cache window;
  mid-statement torn write (now rollback-only); interpreted **and** DataFusion-column integer
  arithmetic; `Value` Hash/Eq; MST NaN; WAL-filename slice; a flaky WAL-reopen test.

**✅ FIXED**
- **All 8 Criticals (#1–#8).**
- High / query: MERGE single-node fast-path fails-open (fail-closed + canonical label +
  numeric-key canonicalization).
- High / durability: failed-rotate finalizer wedge; non-tx-write↔rotation lost-write; unique
  constraint flush-window **and** WAL-recovery rebuild; stale property-cache window; mid-statement
  torn write (→ `TransactionRollbackOnly`); adjacency-warm + time-travel error-swallowing.
- High / API: authz + before-query hooks on all builder paths (incl. `explain`, `PreparedQuery`);
  parameterized `FOR UPDATE` now locks.
- High / functions: interpreted integer arithmetic (overflow/`%0`/substring/range/temporal) **and**
  the DataFusion column-projection path (native arrow ops wrapped — `293a0868a`, gated Int64×Int64
  → arrow checked kernels); `Value` Hash/Eq contract; MST NaN panic; WAL-filename byte-slice panic;
  `VCRegister` CRDT convergence.

**🟦 RE-VERIFIED — not a bug as stated (no fix needed)**
- **VID→u32 truncation in CSR** — REFUTED: `csr.rs:142` `as u32` is only in `CompressedSparseRow`,
  which is **test-only**; production `MainCsr` stores full `Vid` and indexes `as usize`.
- **No read-path memory limit / `u32::MAX` hops** — OVERSTATED: a post-hoc `max_query_memory`
  (1 GiB) check, a 30 s `query_timeout`, and 500k-frontier / 2M-pool BFS caps already exist. Real
  residual gap is only *no in-flight working-set bound* + *silent* var-length truncation (a
  hardening item, not the host-OOM hole implied).

**🟨 DEFERRED (verified real)**
- Plugin signature/hash-pin enforcement into the 4 dynamic loaders. Needs a wire-manifest
  signature-sidecar format first (wire manifests carry no signature field), so wiring
  `RequireSigned` today would blanket-reject all dynamic plugins. The correct *fail-closed* interim
  (reject unverifiable dynamic plugins under `RequireSigned`) is a small follow-up.

**✅ FIXED — correctness-cluster batch (`eca24239a`, 2026-06-11; on branch
`fix/correctness-review-2026-06-10-cluster2`, not yet FF-merged):** the remaining OPEN *correctness*
items were verify-first'd (9 REAL, 3 REFUTED, 1 overstated) and the 9 REAL are now fixed
(workspace nextest 4764, TCK 3925×2 + Locy 501×2, pytest 819+213, clippy/fmt/doc clean):
`schema_version` never bumped → stale plans (now bumped in every DDL mutator); projection registry
keyed on the raw storage pointer → invisible inside an SSI tx (now keyed on the shared
schema-manager identity); `similar_to`/`vector_similarity` no null-prop; `StreamingAppender::abort`
left flushed batches (now async → `BulkWriter::abort`); Python prepared-exec lost typed exceptions;
`AsyncTransaction.cancel()` blocked behind the in-flight op (now lock-free); PageRank dangling mass;
Dijkstra negative-weight (now rejects with `DijkstraError`); Louvain mis-scaled modularity (+
order-stable tie-break).

**🟦 RE-VERIFIED — correctness-cluster (no fix needed):**
- **SSI read-set holes in EXISTS/comprehension/shortestPath** — REFUTED: they DO record reads via
  `record_neighbor_reads` (inside `get_neighbors`), a different recorder than the cited
  `record_edge_adjacency`; a concurrent edge-delete is caught. Only a test-coverage gap.
- **Simple `CASE`/`IN` 3VL** — REFUTED: the raw-`==` site (`read.rs:1895`) is the DDL/admin fallback
  executor, unreachable by ordinary reads (which route through DataFusion + `cypher_eq`, 3VL-correct).
- **Commit snapshot-pin dropped before timeout** — REFUTED: `commit(mut self)` consumes the tx, so a
  timed-out tx is dropped; there is no post-timeout read path.
- **PatternExists/comprehension block tokio workers** — OVERSTATED: the per-batch warm runs on a
  separate OS thread (no deadlock, doesn't "freeze everything"); a perf/robustness refactor item, not
  a wrong-answer bug.

**⬜ OPEN (not yet addressed)**
- Plugins: Rhai default op-limit; pooled WASM/Extism reset between calls; trapped instances returned
  to the pool. (Plus the deferred signature-enforcement wiring above.)
- Test harness: all 5 items. **Entire performance track** (§Performance below).
- Residuals: projection registry cross-`Uni` ABA (the schema-manager keying fixes the reproducible
  case + the leak; address-reuse across instances remains theoretical); Louvain non-determinism has a
  fix but no deterministic test (cross-process only).

**🆕 New findings during Tier 4 verification (both FIXED)**
- DataFusion **column-projection** integer arithmetic wrapped silently (a third path beyond the
  interpreted + CypherValue-UDF paths) → `293a0868a`.
- `corrupt_wal_tail_does_not_block_reopen` flaked under load: the default time-based auto-flush
  promoted a post-flush commit into L1 and `Drop for Uni` does not drain in-flight async flushes →
  test pinned to no-auto-flush (`936838382`). **Observation (not fixed, low-sev):** `Uni` has no
  async `close()`/drain.

---

## Cross-cutting themes

The most dangerous bugs cluster around two architectural seams:

1. **The plan cache** is keyed on *query text only*. Anything resolved at plan time but
   varying at runtime (parameter-folded `LIMIT`/`SKIP`, runtime planner flags, schema
   version that is never bumped) is a latent correctness hole.
2. **Commit-point ordering.** Work placed *after* the durable WAL flush can still fail —
   but the transaction is already durable, producing ghost/partial commits and, in the
   worst case, an unopenable database.

A third recurring pattern: **defensive pass-through** — silently returning unfiltered /
empty / default results on an unexpected shape — repeatedly converts loud bugs into silent
wrong answers (Locy anti-joins, MERGE fast-path scan errors, adjacency warm, FoldExec).

---

## Critical — verified, fix first

### 1. ✅ Plan cache returns wrong results for parameterized `LIMIT` / `SKIP`
*(found independently by 3 reviewers; verified)*

> **✅ FIXED** — Tier 1&2 (`03944c135`): planner records folded `LIMIT`/`SKIP` param names; the
> cache key folds their values on both the read and tx-write paths. Repro `plan_cache_param_fold_test.rs`.

The planner folds `$n` into a concrete `LogicalPlan::Limit { fetch: Some(usize) }` at plan
time (`eval_const_numeric_expr`, `crates/uni-query/src/query/planner.rs:1202`; consumed at
`:3167` / `:7119`). Both plan caches key on query text + `schema_version` only
(`crates/uni/src/api/session.rs:1641`, `crates/uni/src/api/impl_query.rs:400`). So:

```
session.query("MATCH (n) RETURN n LIMIT $n").param("n", 1)    // caches Limit{fetch:1}
session.query("MATCH (n) RETURN n LIMIT $n").param("n", 100)  // cache HIT → returns 1 row
```

Same for `SKIP $n` and any const expression over params, on both the read path and the
tx-write path. The tx-cache's own comment ("reuse is independent of parameter values") is
false here. On a cache hit the baked-in plan executes directly; the executor's `Limit`
handler (`crates/uni-query/src/query/executor/read.rs:2843`) never consults `params`.

**Fix:** keep `LIMIT`/`SKIP` symbolic in the plan and resolve at execution, or refuse to
cache plans that consumed a parameter during planning, or fold the resolved values into the
cache key.

### 2. ✅ `session.query()` read-only enforcement bypassed by `CALL { … }`

> **✅ FIXED** — Tier 1&2 (`03944c135`): validator recurses `CallKind::Subquery` and rejects write
> procedures via `validate_read_only_with`. Repro `session_read_only.rs`.

`validate_read_only` / `check_statement` (`crates/uni-query/src/lib.rs:67`) checks only
top-level clauses; `Clause::Call` falls through `_ => {}` and is never recursed into, while
the planner fully supports writes inside `CALL { … }`.

```
session.query("CALL { CREATE (:X) } RETURN 1")   // passes validation, gets cached
```

The write executes with `tx_l0 = None`, which `resolve_l0`
(`crates/uni-store/src/runtime/writer.rs:510`) resolves to the **global L0** — a
non-transactional write bypassing SSI/OCC validation, commit hooks, and the WAL discipline.

**Fix:** make `check_statement` recurse into `CallKind::Subquery` (and reject write
procedures), mirroring the planner's recursion.

### 3. ✅ `PreparedQuery` skips read-only validation and escapes its transaction

> **✅ FIXED** — Tier 1&2 (`03944c135`): `validate_read_only` in `new`; a tx-bound variant
> (`new_tx_bound` + `PreparedTxBinding`) threads `tx_l0` + shared snapshot. Repro `prepared_query_test.rs`.

`PreparedQuery::new` (`crates/uni/src/api/prepared.rs:45`) never calls `validate_read_only`;
`execute()` always passes `tx_l0 = None` (`prepared.rs:89`, `:165`). Two failure modes:

- `session.prepare("CREATE …").execute()` is an unvalidated, non-transactional write (same
  blast radius as #2).
- `tx.prepare(...)` (`crates/uni/src/api/transaction.rs:734`) passes only the shared
  `Arc<UniInner>`, **not** `self.tx_l0`. Its reads don't see the tx's uncommitted writes,
  and its writes land in main L0 immediately — visible outside the tx and **not** undone by
  `tx.rollback()`. Exposed to Python via `Transaction.prepare`.

**Fix:** validate read-only in `new()`, and add a tx-bound variant that threads
`tx_l0` + read snapshot through execution.

### 4. ✅ SET→CREATE fusion silently drops `SET` on a MATCH-bound var reused in CREATE
*(reproduced live)*

> **✅ FIXED** — Tier 1&2 (`03944c135`): the planner excludes upstream-bound vars from the fusion
> eligibility set (`df_planner::collect_plan_variables`). Repro `create_set_fusion_test.rs`.

`try_fuse_set_items` (`crates/uni-query/src/query/planner.rs:9417`) builds its `owner` set
from `pattern_variable_names` of the CREATE pattern
(`crates/uni-query/src/query/df_graph/mutation_common.rs:990`), which returns **all**
pattern variables — including ones bound by an upstream MATCH.

```
MATCH (a:X) CREATE (a)-[r:T]->(b:Y) SET a.p = 1
```

`p:1` fuses into the `(a)` element; the executor
(`crates/uni-query/src/query/executor/write.rs:1989`) sees `a` already bound and **skips the
inline-property block entirely** → the SET is lost with no error. A live probe confirmed
`RETURN a.p` returns `null`. The guard test `set_on_matched_var_does_not_fuse` only covers
`SET` directly on a MATCH (`Set{input: Match}`), not the matched-var-in-CREATE-pattern shape.

**Fix:** exclude variables bound by the Create's input plan from the fusion eligibility set
(thread `vars_in_scope` into the fusion, or mark introduced vars on the Create node).

### 5. ✅ Ghost / partial commit when `merge` fails *after* the durable WAL flush

> **✅ FIXED** — Tier 1&2 (`03944c135`): `validate_merge_edge_endpoints` runs pre-WAL-flush (and at
> `merge` start for atomicity); WAL replay skips-and-warns. Repros in L0 unit tests + `persistence_restart_test.rs`.

In `commit_transaction_l0` (`crates/uni-store/src/runtime/writer.rs`), the WAL flush at
`:741` is labeled "THIS IS THE COMMIT POINT"; `main_l0.merge(&tx_l0)?` runs at `:767`,
**after** it. `merge` can return `Err` on a reachable cross-tx condition — the issue-#77
guard bails when an edge endpoint is tombstoned in main L0 (`crates/uni-store/src/runtime/l0.rs:892`),
and that check first runs *inside merge* (query-time inserts only check the tx's own
tombstones; SSI validation never checks edge-endpoint liveness against main L0). Consequences:

- (a) Caller gets an error but the tx is durable in the WAL and resurrects on recovery
  (ghost commit, never registered in the SSI registry).
- (b) `merge` is not atomic — the tx's earlier vertex mutations remain applied (partial commit).
- (c) WAL replay re-hits the same bail (`l0.rs:1304`) → `Uni::open` fails
  (`crates/uni/src/api/mod.rs:3308`) → **DB unopenable** until manual WAL surgery.

The codebase acknowledges the invariant in-code (`writer.rs:551`: "the WAL has no abort
marker, so aborting after it would resurrect this transaction"). SSI validation was
deliberately placed before the flush for this reason — but the #77 endpoint check was not.

**Fix:** pre-validate edge endpoints against main L0 (under `flush_lock`) before the WAL
flush; make WAL replay skip-and-warn on this bail; make `merge` validate-then-apply.

### 6. ✅ WASM/Extism host capability attenuation is a no-op for allow-lists (sandbox escape)

> **✅ FIXED** — Tier 1&2 (`03944c135`): `attenuate_to_host` + `intersect_globs` bound guest
> allow-lists by the host ceiling for `Network`/`Filesystem`/`Kms`/`Secret`/`Config`/`HostQuery`.
> Repro in `capability.rs` tests.

`CapabilitySet::intersect` (`crates/uni-plugin/src/capability.rs:235`) iterates `self` (the
guest manifest) and inserts the guest's capability whenever the host grant merely shares the
same *variant* — `contains_variant` → `variant_matches` compares only
`std::mem::discriminant`, ignoring the `allow` / `key_ids` / `ids` / `read` / `write`
payloads. Both binary loaders call `declared.intersect(grants)` (self = guest), so:

```
host grant:      Network{allow:["https://api.example/**"]}
guest manifest:  Network{allow:["**"]}
effective:       Network{allow:["**"]}        // guest value survives; host ceiling ignored
```

Call-time enforcement (`crates/uni-plugin-wasm/src/linker.rs:255`,
`crates/uni-plugin-extism/src/host_svc/net.rs:57`) matches the URL against this
guest-controlled list → the plugin reaches any host. Same for KMS key-ids, Secret ids, and
Filesystem paths. **Correction to the original claim:** Rhai uses the *same* flawed
`intersect`/enforcement; it is only incidentally safe because its declared caps are derived
from fn-kind presence and never carry these variants — not a real difference in logic.

**Fix:** for payload-bearing variants (`Network`, `Filesystem`, `Kms`, `Secret`, `Config`,
`HostQuery`), intersect the inner allow-lists (host grant as the ceiling) rather than
cloning the guest's value.

### 7. ✅ Parser stack overflow aborts the host process
*(reproduced live)*

> **✅ FIXED** — Tier 3 (`6f2e3c8e7`): the overflow is in pest's parse, before the walker, so an
> O(n) iterative `check_nesting_depth` pre-scan (`MAX_NESTING_DEPTH=200`) runs before pest in
> `parse`/`parse_expression`/`parse_locy`. Repro `parse_depth_limit.rs` (5000-deep on a 1 MiB stack).

No recursion-depth limit in the pest walker (`crates/uni-cypher/src/grammar/walker.rs`,
`build_expression`). `RETURN` + ~500 nested parens → `fatal runtime error: stack overflow,
aborting` (also reachable via nested lists/maps/CASE). For an **embedded** library inside a
customer process this is an *uncatchable* abort from a query string.

**Fix:** depth counter in the walker returning `ParseError`, plus a grammar-level guard.

### 8. ✅ Locy `IS NOT <recursive rule>` anti-joins against the last delta, not converged facts

> **✅ FIXED** — Tier 3 (`6f2e3c8e7`): `convert_is_refs` now takes `stratum_rule_names` and, for a
> negated binding, selects the `!is_self_ref` converged-facts entry (fixes the boolean anti-join
> and the PROB-complement path). Repro `negation/RecursiveNegation.feature`; Locy TCK now 501.

`convert_is_refs` (`crates/uni-query/src/query/df_graph/locy_program.rs:1474`) selects
`entries.iter().find(|e| e.is_self_ref).or_else(|| entries.first())` for *all* refs. When
the negated target is itself recursive, this picks the self-ref handle, which after fixpoint
holds only the final (usually empty) semi-naive delta — converged facts are written only to
`!is_self_ref` entries (`locy_program.rs:909`, `:1346`). Result: `IS NOT` **silently
under-filters** (keeps rows it should remove); the PROB-complement path is wrong the same way.

**TCK gap confirmed:** every `IS NOT` target across all 17 Locy TCK feature files is
non-recursive; the only recursive rules (`reachable`, `r`) are never negation targets. The
triggering shape is completely uncovered.

**Fix:** thread stratum membership into `convert_is_refs` and select the `!is_self_ref`
full-facts entry for negated cross-stratum refs.

---

## High-severity — credible, not independently re-verified

Specific line-level traces from the specialist reviews; treat as real pending a confirming read.

### Query correctness

- 🟦 **[REFUTED · re-verified]** **SSI read-set holes.** `EXISTS { }` (`pattern_exists.rs`), pattern comprehensions
  (`pattern_comprehension.rs`), and `shortestPath` (`shortest_path.rs`) read existing
  adjacency but never call `record_edge_adjacency` (unlike `traverse.rs:2222`). In an RW tx,
  `MATCH (a) WHERE EXISTS {(a)-[:F]->()} SET a.flag = true` can commit non-serializably
  against a concurrent edge delete. Beyond the documented phantom limitation — these are
  reads of *existing* edges. *REFUTED: all three record their reads via `record_neighbor_reads`
  inside `get_neighbors` (a different recorder than the cited `record_edge_adjacency`), so the
  concurrent edge-delete IS detected. Only a test-coverage gap, not a correctness hole.*
- ✅ **[FIXED · Tier 3]** **MERGE single-node fast path fails open.** `merge_lookup_persisted`
  (`crates/uni-query/src/query/executor/write.rs:1598`) → `scan_vertex_table`
  (`crates/uni-store/src/storage/manager.rs:1284`) maps every scan error to `Ok(None)`,
  indistinguishable from "no match" → a MERGE that should match **creates a duplicate** on
  transient I/O or an unparsable filter (e.g. NaN/inf key). Same path compares numeric keys
  with derived `Value` equality (`Int(1) != Float(1.0)`) and matches labels
  case-*sensitively* while the general path is case-insensitive → more duplicate divergence
  by flush state.
- ✅ **[FIXED · cluster2 `eca24239a`]** **`schema_version` is never incremented anywhere** (`crates/uni-common/src/core/schema.rs:461`).
  Plan-cache invalidation, prepared-statement re-prepare, and fork metadata all read it; no
  code writes it. DDL never invalidates cached read plans. Bounded today (label ids are
  tombstoned, validation re-reads live schema), but the safety mechanism everything is
  written against is inert. *Confirmed a real wrong answer: a reused session's untyped
  `MATCH (a)-[]->(b)` count went stale after adding an edge type. Fix: `bump_version()` in every
  `SchemaManager` DDL mutator.*
- 🟦 **[REFUTED · re-verified]** **Simple `CASE` and IN-list use Rust equality** (`read.rs:1895`, `expr_eval.rs:188`):
  `WHEN null` matches null (3VL violation), `CASE 1 WHEN 1.0` doesn't match, and a node
  compares equal to a bare integer via its `_vid`. *REFUTED for ordinary reads: `read.rs:1895`
  is the DDL/admin fallback executor; normal `RETURN CASE…`/`x IN […]` route through DataFusion
  + `cypher_eq` (3VL- and cross-type-correct, empirically verified).*

### Concurrency / durability

- 🟦 **[REFUTED · re-verified]** **Commit snapshot-pin dropped before the timeout-wrapped commit** (`transaction.rs:912`):
  a tx surviving a `CommitTimeout` keeps reading without its snapshot (non-repeatable reads).
  *(The related claim that cancellation orphans WAL-buffer entries into a later commit was
  **REFUTED** — see below.)* *REFUTED: `commit(mut self)` consumes the transaction, so a
  `CommitTimeout` drops it — there is no live, unpinned tx to read from (the borrow checker
  forbids `tx.commit().await; tx.query(...)`).*
- ✅ **[FIXED · Tier 4]** **Failed async rotate permanently wedges the flush finalizer** (`writer.rs:844`,
  `crates/uni-store/src/runtime/flush_coordinator.rs:377`): the finalizer requires strictly
  consecutive seqs; one rotate `Err` leaves a gap → manifests never publish, WAL pins
  forever, the pending-flush gate eventually disables async flushing. Same hole in
  `flush_to_l1_async` (`writer.rs:3088`). **Fix:** allocate the seq only after a successful
  rotate, or submit a tombstone.
- ✅ **[FIXED · Tier 4]** **Non-tx writes race L0 rotation** (`writer.rs` `delete_vertex`/`insert_vertex` resolve
  `get_current()` then `.await` storage lookups without `flush_lock`): a tombstone/write can
  land in an already-streamed buffer and be dropped at `complete_flush` — lost write, no
  crash needed. Related: `pin_snapshot` (`l0_manager.rs:220`) doesn't take `flush_lock`, so
  a bulk write crossing awaits can mutate a freshly-pinned SSI snapshot.
- 🟦 **[OVERSTATED · re-verified]** **`PatternExists`/pattern-comprehension block tokio workers** (`pattern_exists.rs:211`):
  spawn a scoped thread + fresh current-thread runtime and `block_on` real Lance I/O inside
  `PhysicalExpr::evaluate`, once per traversal step per batch. No deadlock today, but stalls
  unrelated tasks; on a current-thread host runtime it freezes everything. *OVERSTATED: the warm
  runs on a separate OS thread (`std::thread::scope`), so it neither deadlocks nor "freezes
  everything"; it's a per-batch perf/overhead hazard → a robustness/async-warm refactor, not a
  wrong-answer bug. No red-green repro.*
- ✅ **[FIXED · cluster2 `eca24239a`]** **Global projection registry keyed by `Arc::as_ptr`** (`crates/uni-query/src/projection_store.rs:172`):
  never-evicted process-global map keyed by raw `StorageManager` pointer → ABA reuse leaks
  named projections across `Uni` instances; C2's per-tx `Arc<StorageManager>` makes
  `graph.project` inside a pinned tx invisible afterward + leak one entry per such tx. *Fix: key
  on the shared schema-manager identity — a pinned snapshot shares the parent's `schema_manager`
  Arc (so the projection is visible + no per-tx leak), forks don't (isolation). Residual: cross-`Uni`
  ABA address-reuse is theoretical, not addressed.*

### API / Python

- ✅ **[FIXED · Tier 2/3]** **Authz + before-query hooks bypassed on all builder paths.** `authorize()` and hooks run
  only on `Session::query` / `Transaction::execute`, not `QueryBuilder::{fetch_all,cursor,
  profile}` or `ExecuteBuilder`/`TxQueryBuilder` — and the Python bindings use builders
  whenever params are supplied. Any deployed `AuthzPolicy` is bypassed by parameterizing the
  statement. The same builder paths skip read-only validation, so `query_with("CREATE …")
  .profile()` / `.cursor()` write non-transactionally.
- ✅ **[FIXED · Tier 2]** **Parameterized `FOR UPDATE` silently takes no locks** (`transaction.rs`):
  `acquire_for_update_locks` is only called from `Transaction::query(cypher)` with an empty
  param map; the builder paths never call it, though the key collector supports
  `Expr::Parameter`. Every Python `tx.query(cypher, params)` with `FOR UPDATE` loses its
  pessimistic-lock guarantee, no warning.
- ✅ **[FIXED · Tier 4]** **Timeout/cancel mid-statement leaves a committable half-applied statement in `tx_l0`**
  (`transaction.rs:1273`): mutation operators write row-by-row; a caught timeout + commit
  persists a torn statement. *Fix: the transaction is now marked rollback-only
  (`UniError::TransactionRollbackOnly`) on any statement error, so a later commit refuses.*
- ✅ **[FIXED · cluster2 `eca24239a`]** **Python prepared-statement errors lose the typed exception hierarchy** (`bindings/uni-db/src/types.rs:1505`):
  bare `PyRuntimeError` instead of `uni_error_to_pyerr` → `SerializationConflict` /
  `ConstraintConflict` invisible to `transact_with_retry`. *Fix: route the 4 prepared-execute
  `UniError` sites through `uni_error_to_pyerr` (Poison-lock sites kept as `PyRuntimeError`).*
- ✅ **[FIXED · cluster2 `eca24239a`]** **`AsyncTransaction.cancel()` can't fire while an op is in flight** (`bindings/uni-db/src/async_api.rs:1139`):
  every op holds the tx mutex for its full duration; `cancel()` awaits the same mutex. *Fix:
  `AsyncTransaction` now holds a `CancellationToken` clone; `cancel()`/`cancellation_token()` fire
  it lock-free.*

### Storage

- ✅ **[FIXED · Tier 3]** **Adjacency warm swallows scan errors** (`crates/uni-store/src/storage/adjacency_manager.rs:494,563`):
  `unwrap_or_default()` on the CSR-building scan caches an empty/partial adjacency on
  transient object-store error → traversals **silently miss edges until restart**.
- ✅ **[FIXED · Tier 3]** **Time-travel silently falls back to an older snapshot**
  (`crates/uni-store/src/snapshot/manager.rs:147`): an I/O error or corrupt manifest is
  treated as "not a candidate" → query answers from an older snapshot instead of erroring.
- ✅ **[FIXED · Tier 4]** **Unique-constraint hole during every flush window and after WAL recovery**
  (`writer.rs:1680`): checks consult only the current L0 + tx index + Lance; rotated-but-
  unflushed keys are invisible, and `replay_mutations` never rebuilds `constraint_index`.
- ✅ **[FIXED · Tier 4]** **Stale property-cache window after flush finalize** (`writer.rs:4036`): cache is cleared
  *after* `complete_flush` + WAL truncate (hundreds of ms) → a reader can observe the new
  value via L0 then re-read the old value from cache (non-monotonic read).

### Plugins

- 🟨 **[DEFERRED · verified real]** **Signature/hash-pin enforcement never wired into the dynamic loaders.**
  `PluginTrustConfig::enforce` is called only from `add_plugin` (native Rust plugins,
  `crates/uni/src/api/mod.rs:1814`); the WASM/Extism/Rhai/Python load paths never call
  `enforce()` or `verify_hash_pin()`. `SignaturePolicy::RequireSigned` governs only the
  *trusted* path. The Ed25519 crypto itself is sound — it's just not applied where it matters.
- ✅ **[FIXED · security-trio `cf2d8c248`]** **Rhai plugins have no default op limit** (`crates/uni-plugin-rhai/src/engine.rs:85`):
  `set_max_operations` is applied only if `FuelPerCall` was granted; `while true {}` wedges
  the query thread forever (WASM has a 30s epoch floor). *Fix: `DEFAULT_MAX_OPERATIONS` (10M) set
  unconditionally; a grant may only raise the floor.*
- ✅ **[FIXED · security-trio `cf2d8c248`]** **Pooled WASM/Extism instances not reset between calls** (`crates/uni-plugin-wasm/src/loader.rs:833`):
  guest linear memory, globals, and WASI ctx persist across calls → state leakage between
  unrelated queries; a `Pure`-declared fn can behave impurely. *Fixed together with the next item.*
- ✅ **[FIXED · security-trio `cf2d8c248`]** **Trapped instances returned to the pool** (`crates/uni-plugin-wasm-rt/src/pool.rs:285`):
  `take()` exists for corrupt instances but no adapter calls it; a trapped store goes back
  into the warm queue. *Fix (covers both this and the previous item): the shared `InstancePool::acquire`
  now builds a fresh instance per acquire (no warm reuse), so guest state never leaks and a trapped
  store is dropped, never recycled. wasm caches Engine+InstancePre + a fresh Store per call; extism's
  per-acquire factory already built fresh. Aggregators unaffected (state threaded host-side).*

### Functions / algorithms / CRDTs

- ✅ **[FIXED · Tier 4]** **Hand-rolled integer arithmetic** (`crates/uni-query-functions/src/expr_eval.rs:367`):
  `9223372036854775807 + 1` panics in debug / wraps in release; `*`/`%` route i64 through
  f64 (precision loss above 2⁵³); `1 % 0` yields `NaN` instead of a div-by-zero error. The
  DataFusion UDF path is correct; the interpreted write-path/`UNWIND`/Locy path is not.
  Companion panics: `substring('x', -1, 5)`, `range(i64::MAX-1, i64::MAX)`, temporal
  `+ duration({days: 2e14})`. *Fix: a shared `checked_int_op` (overflow/÷0 errors) reused by the
  interpreted path + substring/range/temporal overflow guards. **A third path — the DataFusion
  column-projection arithmetic (`df_expr.rs`) — also wrapped silently and was fixed separately
  (`293a0868a`, Int64×Int64 → arrow checked kernels).***
- ✅ **[FIXED · cluster2 `eca24239a`]** **`similar_to` / `vector_similarity` UDFs don't null-propagate** (`df_udfs.rs:7392`): one
  NULL embedding fails the entire query instead of yielding null for that row. *Fix: a
  `ScoringMode::Null` arm + `append_null` (mode derived from the first non-null row) on the
  executor path, and an early `Ok(Value::Null)` in `eval_similar_to_pure`.*
- ✅ **[FIXED · Tier 3]** **`VCRegister` CRDT violates convergence** (`crates/uni-crdt/src/vc_register.rs:66`): on
  concurrent clocks it keeps `self.value`; after `A.merge(B)` and `B.merge(A)` the replicas
  hold equal clocks but different values and never reconcile. Needs a deterministic value
  tie-break like `LWWRegister`.
- ✅ **[FIXED · cluster2 `eca24239a`]** **`StreamingAppender::abort()` leaves flushed batches in storage** (`crates/uni-bulk/src/appender.rs:146`):
  clears only the buffer; never calls `BulkWriter::abort()` → half-ingested data on error. *Fix:
  `abort()` is now async and calls `BulkWriter::abort().await` (rolls back/drops flushed tables);
  callers updated.*
- ✅ **[FIXED · cluster2 `eca24239a` · MST Tier 4]** **PageRank drops dangling-node mass** (`crates/uni-algo/.../pagerank.rs:76`); **Dijkstra
  has no negative-weight guard + broken heap order for negatives** (`dijkstra.rs:61`);
  **Louvain is non-deterministic with mis-scaled modularity** (`louvain.rs:99`); **MST
  panics on a NaN edge weight** (`mst.rs:67`). *Fixes: PageRank redistributes dangling mass;
  Dijkstra returns `Result<_, DijkstraError>` and rejects negatives; Louvain uses full undirected
  degree (modularity) + an order-stable tie-break (determinism, no deterministic test); MST uses
  `total_cmp` (Tier 4).*

### Robustness sweep (mechanical hazards)

- 🟦 **[OVERSTATED · re-verified]** **No DataFusion memory limit on the read path** (`read.rs:483`) + var-length expansion
  defaults to `u32::MAX` hops (`planner.rs:4345`) → one query can OOM the host (the write
  side has `check_transaction_memory`; reads have nothing). *A post-hoc 1 GiB `max_query_memory`
  check, a 30 s `query_timeout`, and 500k-frontier / 2M-pool BFS caps already exist; the real
  residual is no in-flight working-set bound + silent var-length truncation — a hardening item,
  not the host-OOM hole implied.*
- 🟦 **[REFUTED · re-verified]** **VID→u32 truncation in CSR** (`crates/uni-store/src/storage/csr.rs:142`): stores the raw
  VID truncated to u32 as a "dense" index → silently wrong adjacency once VIDs exceed
  `u32::MAX` (reachable on long-lived churned DBs without 4B live nodes). *The `as u32` is only in
  `CompressedSparseRow`, which is **test-only**; production `MainCsr` stores full `Vid` and indexes
  `as usize`. Not reachable in production.*
- ✅ **[FIXED · Tier 4]** **`Value` Hash/Eq contract violation** (`crates/uni-common/src/value.rs:515` vs `:744`):
  `PartialEq` says `0.0 == -0.0` and `NaN != NaN` while `Hash` uses `to_bits` → wrong window
  PARTITION BY buckets (`read.rs:3421`). *Fix: hand-written `PartialEq` + normalized `Hash` for the
  float arm (signed-zero, NaN); Cypher `=`/`IN`/`DISTINCT` route through `cypher_eq`, so unaffected.*
- ✅ **[FIXED · Tier 4]** **WAL filename byte-slice panic** (`crates/uni-store/src/runtime/wal.rs:26`): `filename[..20]`
  panics on a non-UTF-8-boundary foreign file during replay listing. *Fix: char-boundary-safe
  `filename.get(..20)`.*

### Test harness (could mask regressions)

- **Error matcher treats any *unclassified* engine error as matching any expected error**
  (`crates/uni-tck/src/matcher/error.rs:221`), and `UnknownFunction` satisfies
  `NumberOutOfRange` → an unimplemented function counts as a *passing* scenario.
- **Path comparison ignores edge direction** (`crates/uni-tck/src/parser/value.rs:251`).
- **`{,m}` path quantifier parsed as `{m,}`** (inverted bound, `walker.rs:1582`); `{-2}` /
  `{0x2}` / `{2^32}` panic the parser.
- **CLI REPL routes every statement through read-only `session.query()`** (`crates/uni-cli/src/repl.rs:117`)
  → no mutation works in the REPL at all.
- **TCK value equality conflates `Int(1)` with `Float(1.0)`** and temporals with their
  string rendering (`result.rs:225`); runners can pass with zero scenarios executed
  (`tck.rs:318`).

**✅ FIXED — test-harness + repl batch (branch `fix/tck-harness-and-repl`, 2026-06-11, not
yet FF-merged):** all five items, verify-first. Gates: openCypher TCK **3925×2**, Locy TCK
**501×2**, workspace nextest **4787**, pytest **819 + 213**, clippy/fmt/doc clean.

- `8fd40049f` — **engine bug exposed by the path-direction fix**: returned-path relationships
  were built with traversal-order src/dst, not their stored orientation, across the fixed /
  variable-length / shortest-path / pattern-comprehension builders and both storage tiers.
  Endpoints now resolve from the stored edge (L0 visibility chain → L1 Outgoing-adjacency probe).
- `c45cd7030` — TCK oracles aligned to openCypher: `Int ≠ Float` (typed), temporals compared by
  canonical string rendering (openCypher renders temporal/BTIC and `toString` results identically,
  so the reference comparison is rendering-based — a *type-strict* temporal oracle is stricter than
  the spec and was dropped); path orientation compared; `Unknown(_)` no longer blanket-matches a
  typed expectation; a 0-scenario run now fails.
- `f9804de43` — the kept `Int ≠ Float` strictness exposed a **test-infrastructure** defect, not an
  engine bug: the sidecar schema generator merged `Int64+Float64 → Float64`, so a typed column
  widened stored `Int(-11)` to `-11.0` in sidecar mode. Mixed concrete types now fall back to
  lossless `CypherValue`; the bit-rotted `Json` sentinel is corrected to `CypherValue`.
- `dea6d46b2` — `{,m}` quantifier no longer inverted; `{-2}` / `{0x2}` / `{2^32}` return a
  `ParseError` instead of panicking.
- `48dfb3ccb` — CLI REPL / one-shot route through the new autocommitting `Session::run`; writes work.

**Open (real, not a conformance bug — deferred):** the engine stringifies temporals stored in node
properties through the `_all_props` / `serde_json` path (`scan.rs`); a `date()` stored and read back
becomes a `String`. The rendering-based TCK cannot gate this; the fix is a medium-risk `_all_props`
rewrite touching the Python-binding surface. Tracked separately.

---

## Performance — highest-payoff

> **Status update 2026-06-12: items #2–#7 + four P2s FIXED** (verify-first, bench-before/after;
> criterion medians below from `target/perf-baselines/`).
>
> **#1 clone-on-freeze: NO-GO after a full implementation + measurement** (same verdict shape as
> the group-commit proposal). Generation chaining (O(1) freeze, frozen generations chained on
> `pending_flush`, fold-at-flush, overlay-aware commit validation, CRDT merge-seeding, chain cap)
> was built, fully green on the suites — and **slower than the lazy clone at every measured L0
> size** (`ssi_freeze` commit-with-pin deltas, old → chained: 1k 2.8→4.0 ms; 10k 3.5→5.1 ms;
> 50k 9.9→14.5 ms; a new 50k bench arm was added for the scaling check). Root cause: chaining
> trades the O(L0) commit clone for an **O(chain-length) read tax** (~0.2 ms per chained buffer
> per scan, paid by every query), and the clone cost it eliminates is already fenced by
> `auto_flush_threshold` — L0 rotates at 10k mutations, so the "hundreds of MB" cliff cannot
> accumulate under default config. Lowering the fold cap converges back to the clone's cost plus
> overhead. The chaining implementation was reverted; the 50k bench arm is kept.
>
> **Salvage — three REAL pre-existing commit-time races found and FIXED** (the chaining work
> exposed them; they exist in the shipped design whenever `async_flush_enabled` lets a commit
> interleave with the post-rotate flush window — the commit-time layer of the Bug #9A window):
> - the serializable-MERGE unique-key and ext_id commit-time re-probes consulted only the current
>   buffer → two pre-inserted txs could both commit the same key around a mid-flush rotation;
> - the CRDT carve-out merge ran against the (post-rotation, empty) current buffer → a concurrent
>   counter increment committed mid-flush was silently shadowed (lost update);
> - the issue-#77 edge-endpoint check missed tombstones sitting mid-flush → ghost edge.
>
> All three now walk `[current, pending_flush…]` (`Writer::validate_edge_endpoints_overlay`,
> `Writer::seed_crdt_props`, and the overlay re-probes in `commit_transaction_l0`); red-green
> failpoint repros in `flush_resilience.rs` (`extid_commit_probe_covers_flush_window`,
> `crdt_increments_merge_across_flush_window`,
> `edge_to_vertex_tombstoned_in_flush_window_rejected`). `ssi_freeze` is unchanged by the fixes
> (4.5/7.1/20.7 ms with-pin at 1k/10k/50k ≈ pre-fix baseline).
>
> | Fix | Bench | Before → After |
> |---|---|---|
> | #3a ext_id index | `mutation_extid_ingest/4000` | 193.1 ms → **38.6 ms** (5.0×; quadratic → linear) |
> | #4 batched MERGE | `mutation_unwind_merge_batched` (1000 rows, 50% hit, flushed) | 1.702 s → **1.091 s** (1.56×; 1000 Lance scans → 1); then → **17.5 ms** with the statement-level property prefetch (2026-06-12, `f614f317c`; 97× total) |
> | #5 traversal pushdown | `schemaless_traversal/one_source_100k_edges` | 32.9 ms → **7.7 ms** (4.3×); all-sources arm unregressed (47.2 → 50.7 ms, threshold 1024) |
> | #2 WAL double-write | WAL entries per edge-commit | 2 → **1** (`commit_writes_each_mutation_to_wal_exactly_once`, red-green) |
> | #2+#7 cluster | `mutation_*` / `commit_throughput` collateral | within noise |
>
> Detail per item:
> - **#2** — `delete_vertex`/`insert_edge`/`delete_edge` gained `skip_wal` impls; `merge()` uses them.
> - **#3a** — `L0Buffer.extid_index` (maintained in both insert impls, the WAL-replay arm, and
>   cascade deletion; synced to the post-CRDT-merge value). Also closed a real **concurrent-commit
>   ext_id race** (commit-time re-probe next to the unique-key check; new `ConstraintConflict`).
>   #3b (per-row Lance `count_rows` on single inserts) deferred as documented.
> - **#4** — per-statement prefetch `merge_lookup_persisted_batch` (canonical key tuples, 1000/chunk,
>   type-grouped `IN`/OR-of-ANDs, fail-closed); per-row liveness re-checks kept at row time.
>   ~~Residual: per-row ON CREATE/ON MATCH SET + post-SET property re-read now dominate
>   (~1.1 ms/row).~~ **FIXED 2026-06-12** (`f614f317c`): statement-level property `Prefetch` —
>   matched vids batch-read once (`get_batch_vertex_props_for_label`, one `_vid IN (…)` scan),
>   created vids seeded with an empty base, per-row reads resolve as base + L0 layering; CRDT-bearing
>   labels keep the per-row path; fail-open on batch errors.
>   `mutation_unwind_merge_batched`: 1.103 s → **17.5 ms** (−98.4%, ~63×); null-SET guard
>   `merge_on_match_set_null_after_flush`.
>   ~~**Found (pre-existing, parity-pinned):** MERGE on an *undeclared* label cannot match FLUSHED
>   rows (persisted lookup sees nothing) and re-creates them — identical on the old per-row path;
>   see `merge_batch_mixed_key_types`.~~ **FIXED 2026-06-12** (`d5b77ea71`): undeclared labels route
>   to a main-vertex-table lookup (`array_contains(labels,…)` + MVCC dedup with tombstones visible +
>   in-memory `props_json` key match — the schemaless MATCH read path). Fixing it exposed two more
>   per-label blind spots, both fixed at the root: `fetch_prop_from_storage` (single-prop reads,
>   e.g. SET RHS `n.freq + e.c`) had no main-table fallback (its all-props sibling did) — added,
>   gated on "no per-label verdict"; and `find_props_by_vid` filtered `_deleted = false` *before*
>   the max-version pick, letting a deleted-and-flushed schemaless vertex resurrect its older live
>   row — now dedups with tombstones visible. Red-green: `merge_batch_mixed_key_types` tightened
>   6 → 4, plus `merge_schemaless_{flushed_no_duplicates,on_match_set_after_flush,deleted_after_flush_recreates}`.
> - **#5** — source-VID pushdown (`find_edges_by_type_names` endpoint filter, ≤1024 sources, both
>   storage tiers — the L0 overlay too, so the SSI read-set footprint is consistent) + `Arc`-shared
>   type/props in the adjacency map (the `Both` deep-clone is gone). New SSI tests cover the
>   narrowed read-set in both directions.
> - **#6** — `SchemaManager::get_or_assign_edge_type_id` read-lock fast path (double-checked; the
>   write-lock + `Arc::make_mut` Schema deep-clone now only on first assignment).
> - **#7** — CRDT-merge clones removed (props moved, size pre-computed); `WriteAheadLog::append`
>   takes `Mutation` by value; `flush` serializes via borrowed `WalSegmentRef` (byte-identical,
>   unit-tested); commit merges via `merge_take` (drains tx property maps; `merge` stays
>   non-draining). Remaining clone: properties → WAL `Mutation` at commit (needs Arc/Cow `Mutation`,
>   separate proposal).
> - **P2s** — `=~` thread-local regex cache; plan caches hold `Arc<LogicalPlan>` (deep clone off the
>   mutex, both read + tx paths); `COUNT(DISTINCT)` uses `HashSet<Value>` with numeric
>   canonicalization (also fixes the latent `1` vs `'1'` collapse of the stringifying accumulator);
>   `get_neighbors_batch` records the SSI read-set once per batch (zero-neighbor sources still
>   recorded).

1. **Clone-on-freeze deep-copies the entire main L0 per commit under SSI**
   (`crates/uni-store/src/runtime/l0_manager.rs:262`). With `ssi_enabled` default-ON every
   RW tx pins, so each commit pays `O(L0 size)` (potentially hundreds of MB) and blocks all
   readers for the duration. The single biggest cliff. **Fix:** COW/persistent structures or
   generation-chaining (the read path already overlays multiple L0s). *Known open item.*
2. **Commit double-writes every edge & delete to the WAL.** `merge` re-calls
   `insert_edge`/`delete_edge`/`delete_vertex` (`l0.rs:1133`), which re-append to the WAL —
   2× volume/serialization on edge-heavy commits, plus extra clones, under the main-L0 write
   lock. Only the vertex-insert path has the `skip_wal` variant. **Fix:** add `skip_wal`
   variants for the rest.
3. **O(n²) constrained ingest.** `ext_id` uniqueness is a full linear scan of all L0 buffers
   per insert (`writer.rs:1437`); unique constraints issue a per-row `count_rows(filter)`
   Lance scan on the single-insert path (`writer.rs:1724`). Batch path is fine; Cypher
   per-row `CREATE`/`MERGE` isn't. **Fix:** maintain an `ext_id→vid` index; route per-row
   through the batched check.
4. **Batched MERGE still does one Lance scan per row** (`write.rs:1598`). The issue-#69 fast
   path killed per-row *planning* but not per-row *scanning*; `UNWIND $rows MERGE` issues N
   independent scans where one `key IN (...)` would do. ~5–20× plausible.
5. **Schemaless traversal materializes the whole edge type per hop** (`traverse.rs:2246`). A
   1-source `(a)-[r:KNOWS]->(b)` loads every KNOWS edge with full property maps; `Both`
   doubles it via `props.clone()`. **Fix:** push the source-VID set into the scan; share
   props via `Arc`.
6. **Per-edge-row schema write lock on CREATE** (`write.rs:2183`).
   `get_or_assign_edge_type_id` takes the schema write lock + `Arc::make_mut` (full Schema
   deep-clone when contended) per relationship row, for a type name constant per statement.
   **Fix:** resolve once before the row loop.
7. **Property-map clone churn** — ~4–5 deep clones of every row's `Properties` between API
   and commit (`l0.rs:513`/`:642` unconditional clone for a `.len()`; `wal.rs` append-then-
   clone; batch insert double-clone). Severe for embedding vectors. Directly relevant to the
   ongoing cypher-ingest-speedup work.

**Other measurable P2s:** `=~` regex compiled per row in interpreted paths
(`expr_eval.rs:97`); plan-cache deep-clone under the global mutex (`impl_query.rs:402`);
correlated `CALL` subqueries re-planning physical per row (`apply.rs:809`); per-vertex lock
churn in adjacency reads (`df_graph/mod.rs:578`); `COUNT(DISTINCT)` stringifying every value
(`core.rs:153`); vertex scan collecting the whole label table as one batch + unconditional
MVCC sort (`scan.rs:2074`); SSI read-set validation O(|read_set| × commits-since-begin)
under `flush_lock` (`occ.rs:173`).

---

## Refuted / verified-clean (so absences are meaningful)

- **REFUTED:** cancellation orphaning WAL-buffer entries into a later commit. `flush` does
  `std::mem::take` of the buffer synchronously *before* any `.await`, so a dropped commit
  future takes the entries with it — they are lost, not left to be flushed by the next
  commit. The cancelled tx correctly returns `Err(CommitTimeout)` and is not durable.

**Traced and found clean:** WAL blake3 envelope + tail-vs-middle corruption policy; SSI
commit ordering (validation strictly before the durable flush); C1 clone-on-freeze
refcount/leak handling; no std/parking_lot guard held across `.await` anywhere in the 5 core
crates; the FOR-UPDATE row-lock DashMap reclamation; the plan-cache hash-collision
text-compare fix (the prior P0); 3VL truth tables in `eval_binary_op`; DELETE/DETACH
ordering; the Ed25519 verifier *crypto* itself (sound — just not wired into the dynamic
loaders); WASM epoch/fuel/memory enforcement mechanics; uni-crdt GCounter/GSet/ORSet/RGA
merge laws; uni-bulk batching thresholds; uni-sidecar atomic write protocol; the 7 non-test
`unsafe` lines (all in the pyo3 Arrow PyCapsule bridge, verified sound).

---

## Suggested triage order

> **✅ All 8 Criticals are now FIXED** (Tiers 1–4), plus most High-severity correctness/durability
> items — see **Fix status** at the top. The original ordering below is retained for the record.

The eight **Critical** items are user-facing correctness/security bugs reachable from
ordinary queries. Within those, the cleanest, most self-contained fixes with obvious
regression tests:

1. **#1 plan-cache `LIMIT`/`SKIP`** — keep symbolic / exclude from caching.
2. **#4 SET-fusion drop** — exclude upstream-bound vars from fusion.
3. **#2 + #3 read-only bypass** (`CALL` subquery + `PreparedQuery`) — recurse the validator,
   thread `tx_l0`.
4. **#5 ghost commit** — move the edge-endpoint check before the WAL flush; make replay
   skip-and-warn.
5. **#6 plugin capability intersect** — intersect inner allow-lists (security boundary).
6. **#8 Locy recursive `IS NOT`** + **#7 parser depth limit**.
