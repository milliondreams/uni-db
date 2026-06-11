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

This is read-only analysis; no code was modified.

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

No recursion-depth limit in the pest walker (`crates/uni-cypher/src/grammar/walker.rs`,
`build_expression`). `RETURN` + ~500 nested parens → `fatal runtime error: stack overflow,
aborting` (also reachable via nested lists/maps/CASE). For an **embedded** library inside a
customer process this is an *uncatchable* abort from a query string.

**Fix:** depth counter in the walker returning `ParseError`, plus a grammar-level guard.

### 8. ✅ Locy `IS NOT <recursive rule>` anti-joins against the last delta, not converged facts

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

- **SSI read-set holes.** `EXISTS { }` (`pattern_exists.rs`), pattern comprehensions
  (`pattern_comprehension.rs`), and `shortestPath` (`shortest_path.rs`) read existing
  adjacency but never call `record_edge_adjacency` (unlike `traverse.rs:2222`). In an RW tx,
  `MATCH (a) WHERE EXISTS {(a)-[:F]->()} SET a.flag = true` can commit non-serializably
  against a concurrent edge delete. Beyond the documented phantom limitation — these are
  reads of *existing* edges.
- **MERGE single-node fast path fails open.** `merge_lookup_persisted`
  (`crates/uni-query/src/query/executor/write.rs:1598`) → `scan_vertex_table`
  (`crates/uni-store/src/storage/manager.rs:1284`) maps every scan error to `Ok(None)`,
  indistinguishable from "no match" → a MERGE that should match **creates a duplicate** on
  transient I/O or an unparsable filter (e.g. NaN/inf key). Same path compares numeric keys
  with derived `Value` equality (`Int(1) != Float(1.0)`) and matches labels
  case-*sensitively* while the general path is case-insensitive → more duplicate divergence
  by flush state.
- **`schema_version` is never incremented anywhere** (`crates/uni-common/src/core/schema.rs:461`).
  Plan-cache invalidation, prepared-statement re-prepare, and fork metadata all read it; no
  code writes it. DDL never invalidates cached read plans. Bounded today (label ids are
  tombstoned, validation re-reads live schema), but the safety mechanism everything is
  written against is inert. **Fix:** bump it in every `SchemaManager` mutator.
- **Simple `CASE` and IN-list use Rust equality** (`read.rs:1895`, `expr_eval.rs:188`):
  `WHEN null` matches null (3VL violation), `CASE 1 WHEN 1.0` doesn't match, and a node
  compares equal to a bare integer via its `_vid`.

### Concurrency / durability

- **Commit snapshot-pin dropped before the timeout-wrapped commit** (`transaction.rs:912`):
  a tx surviving a `CommitTimeout` keeps reading without its snapshot (non-repeatable reads).
  *(The related claim that cancellation orphans WAL-buffer entries into a later commit was
  **REFUTED** — see below.)*
- **Failed async rotate permanently wedges the flush finalizer** (`writer.rs:844`,
  `crates/uni-store/src/runtime/flush_coordinator.rs:377`): the finalizer requires strictly
  consecutive seqs; one rotate `Err` leaves a gap → manifests never publish, WAL pins
  forever, the pending-flush gate eventually disables async flushing. Same hole in
  `flush_to_l1_async` (`writer.rs:3088`). **Fix:** allocate the seq only after a successful
  rotate, or submit a tombstone.
- **Non-tx writes race L0 rotation** (`writer.rs` `delete_vertex`/`insert_vertex` resolve
  `get_current()` then `.await` storage lookups without `flush_lock`): a tombstone/write can
  land in an already-streamed buffer and be dropped at `complete_flush` — lost write, no
  crash needed. Related: `pin_snapshot` (`l0_manager.rs:220`) doesn't take `flush_lock`, so
  a bulk write crossing awaits can mutate a freshly-pinned SSI snapshot.
- **`PatternExists`/pattern-comprehension block tokio workers** (`pattern_exists.rs:211`):
  spawn a scoped thread + fresh current-thread runtime and `block_on` real Lance I/O inside
  `PhysicalExpr::evaluate`, once per traversal step per batch. No deadlock today, but stalls
  unrelated tasks; on a current-thread host runtime it freezes everything.
- **Global projection registry keyed by `Arc::as_ptr`** (`crates/uni-query/src/projection_store.rs:172`):
  never-evicted process-global map keyed by raw `StorageManager` pointer → ABA reuse leaks
  named projections across `Uni` instances; C2's per-tx `Arc<StorageManager>` makes
  `graph.project` inside a pinned tx invisible afterward + leak one entry per such tx.

### API / Python

- **Authz + before-query hooks bypassed on all builder paths.** `authorize()` and hooks run
  only on `Session::query` / `Transaction::execute`, not `QueryBuilder::{fetch_all,cursor,
  profile}` or `ExecuteBuilder`/`TxQueryBuilder` — and the Python bindings use builders
  whenever params are supplied. Any deployed `AuthzPolicy` is bypassed by parameterizing the
  statement. The same builder paths skip read-only validation, so `query_with("CREATE …")
  .profile()` / `.cursor()` write non-transactionally.
- **Parameterized `FOR UPDATE` silently takes no locks** (`transaction.rs`):
  `acquire_for_update_locks` is only called from `Transaction::query(cypher)` with an empty
  param map; the builder paths never call it, though the key collector supports
  `Expr::Parameter`. Every Python `tx.query(cypher, params)` with `FOR UPDATE` loses its
  pessimistic-lock guarantee, no warning.
- **Timeout/cancel mid-statement leaves a committable half-applied statement in `tx_l0`**
  (`transaction.rs:1273`): mutation operators write row-by-row; a caught timeout + commit
  persists a torn statement.
- **Python prepared-statement errors lose the typed exception hierarchy** (`bindings/uni-db/src/types.rs:1505`):
  bare `PyRuntimeError` instead of `uni_error_to_pyerr` → `SerializationConflict` /
  `ConstraintConflict` invisible to `transact_with_retry`.
- **`AsyncTransaction.cancel()` can't fire while an op is in flight** (`bindings/uni-db/src/async_api.rs:1139`):
  every op holds the tx mutex for its full duration; `cancel()` awaits the same mutex.

### Storage

- **Adjacency warm swallows scan errors** (`crates/uni-store/src/storage/adjacency_manager.rs:494,563`):
  `unwrap_or_default()` on the CSR-building scan caches an empty/partial adjacency on
  transient object-store error → traversals **silently miss edges until restart**.
- **Time-travel silently falls back to an older snapshot**
  (`crates/uni-store/src/snapshot/manager.rs:147`): an I/O error or corrupt manifest is
  treated as "not a candidate" → query answers from an older snapshot instead of erroring.
- **Unique-constraint hole during every flush window and after WAL recovery**
  (`writer.rs:1680`): checks consult only the current L0 + tx index + Lance; rotated-but-
  unflushed keys are invisible, and `replay_mutations` never rebuilds `constraint_index`.
- **Stale property-cache window after flush finalize** (`writer.rs:4036`): cache is cleared
  *after* `complete_flush` + WAL truncate (hundreds of ms) → a reader can observe the new
  value via L0 then re-read the old value from cache (non-monotonic read).

### Plugins

- **Signature/hash-pin enforcement never wired into the dynamic loaders.**
  `PluginTrustConfig::enforce` is called only from `add_plugin` (native Rust plugins,
  `crates/uni/src/api/mod.rs:1814`); the WASM/Extism/Rhai/Python load paths never call
  `enforce()` or `verify_hash_pin()`. `SignaturePolicy::RequireSigned` governs only the
  *trusted* path. The Ed25519 crypto itself is sound — it's just not applied where it matters.
- **Rhai plugins have no default op limit** (`crates/uni-plugin-rhai/src/engine.rs:85`):
  `set_max_operations` is applied only if `FuelPerCall` was granted; `while true {}` wedges
  the query thread forever (WASM has a 30s epoch floor).
- **Pooled WASM/Extism instances not reset between calls** (`crates/uni-plugin-wasm/src/loader.rs:833`):
  guest linear memory, globals, and WASI ctx persist across calls → state leakage between
  unrelated queries; a `Pure`-declared fn can behave impurely.
- **Trapped instances returned to the pool** (`crates/uni-plugin-wasm-rt/src/pool.rs:285`):
  `take()` exists for corrupt instances but no adapter calls it; a trapped store goes back
  into the warm queue.

### Functions / algorithms / CRDTs

- **Hand-rolled integer arithmetic** (`crates/uni-query-functions/src/expr_eval.rs:367`):
  `9223372036854775807 + 1` panics in debug / wraps in release; `*`/`%` route i64 through
  f64 (precision loss above 2⁵³); `1 % 0` yields `NaN` instead of a div-by-zero error. The
  DataFusion UDF path is correct; the interpreted write-path/`UNWIND`/Locy path is not.
  Companion panics: `substring('x', -1, 5)`, `range(i64::MAX-1, i64::MAX)`, temporal
  `+ duration({days: 2e14})`.
- **`similar_to` / `vector_similarity` UDFs don't null-propagate** (`df_udfs.rs:7392`): one
  NULL embedding fails the entire query instead of yielding null for that row.
- **`VCRegister` CRDT violates convergence** (`crates/uni-crdt/src/vc_register.rs:66`): on
  concurrent clocks it keeps `self.value`; after `A.merge(B)` and `B.merge(A)` the replicas
  hold equal clocks but different values and never reconcile. Needs a deterministic value
  tie-break like `LWWRegister`.
- **`StreamingAppender::abort()` leaves flushed batches in storage** (`crates/uni-bulk/src/appender.rs:146`):
  clears only the buffer; never calls `BulkWriter::abort()` → half-ingested data on error.
- **PageRank drops dangling-node mass** (`crates/uni-algo/.../pagerank.rs:76`); **Dijkstra
  has no negative-weight guard + broken heap order for negatives** (`dijkstra.rs:61`);
  **Louvain is non-deterministic with mis-scaled modularity** (`louvain.rs:99`); **MST
  panics on a NaN edge weight** (`mst.rs:67`).

### Robustness sweep (mechanical hazards)

- **No DataFusion memory limit on the read path** (`read.rs:483`) + var-length expansion
  defaults to `u32::MAX` hops (`planner.rs:4345`) → one query can OOM the host (the write
  side has `check_transaction_memory`; reads have nothing).
- **VID→u32 truncation in CSR** (`crates/uni-store/src/storage/csr.rs:142`): stores the raw
  VID truncated to u32 as a "dense" index → silently wrong adjacency once VIDs exceed
  `u32::MAX` (reachable on long-lived churned DBs without 4B live nodes).
- **`Value` Hash/Eq contract violation** (`crates/uni-common/src/value.rs:515` vs `:744`):
  `PartialEq` says `0.0 == -0.0` and `NaN != NaN` while `Hash` uses `to_bits` → wrong window
  PARTITION BY buckets (`read.rs:3421`).
- **WAL filename byte-slice panic** (`crates/uni-store/src/runtime/wal.rs:26`): `filename[..20]`
  panics on a non-UTF-8-boundary foreign file during replay listing.

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

---

## Performance — highest-payoff

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
