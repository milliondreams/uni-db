# Deep Logical-Correctness Scan — All Crates

**Date:** 2026-07-05  
**Method:** 45 line-bounded shards (~12k lines each) across all 30 workspace crates. One correctness-only reviewer per shard read every file in full, then each raw finding was handed to an independent adversarial verifier prompted to *refute* it (default-refute on uncertainty). Only findings the verifier could not refute are listed as confirmed.

**Coverage:** 45/45 shards reviewed · 199 raw findings · 153 survived verification · 18 findings' verifiers were interrupted by usage limits (listed separately, unverified).

**Scope:** logic defects producing wrong runtime behavior — wrong results, boundary/overflow errors, broken invariants, silently-swallowed failures, concurrency-logic races. Style/perf/docs excluded.


---

## Severity summary

| Severity | Confirmed | Unverified (interrupted) |
|---|---|---|
| Critical | 2 + 1 self-verified | 1 |
| High | 58 | 6 |
| Medium | 56 | 8 |
| Low | 37 | 3 |

> The 77% confirm rate is higher than a well-calibrated adversarial gate; treat the medium/low tail as *candidates needing triage* rather than settled bugs. The critical and high tiers — several **empirically reproduced** by the agents against the built library — are the actionable core.


---

## CRITICAL

### `bindings/uni-db/src/builders.rs:2202` — CRITICAL
**record_batch_from_pyarrow takes ownership of the Arrow C Data structs with std::ptr::read instead of move-and-null (ptr::replace with an empty struct / FFI_ArrowArray::from_raw), leaving the producer capsules' release callbacks non-NULL, so the buffers are released both by Rust and by pyarrow's capsule destructor.**

*Failure scenario:* Python calls appender.write_batch(pyarrow_record_batch) (sync StreamingAppender or AsyncStreamingAppender). The temporary (schema_capsule, array_capsule) tuple is refcount-collected right after the function returns; pyarrow's capsule destructor sees release != NULL and calls it, freeing the very buffers the imported RecordBatch still references, and Rust later calls release again on its ptr::read copy — use-after-free of the ingested row data (silent corruption of written vertices) and a double-free/segfault. The Arrow PyCapsule spec requires the consumer to mark the struct released; arrow-rs's own from_raw does ptr::replace(ptr, Self::empty()) for exactly this reason.

<details><summary>Verifier evidence</summary>

CONFIRMED empirically — the claim is correct in every particular. Evidence chain: (1) bindings/uni-db/src/builders.rs:2195-2202 extracts the C Data structs from the pyarrow capsules with `std::ptr::read(schema_ptr)` / `std::ptr::read(array_ptr)`, which copies the structs but leaves the capsule-owned originals intact with `release != NULL`. (2) pyarrow 24.0.0's capsule destructors (site-packages/pyarrow/types.pxi:6035-6036 and 6054-6055) call `release(schema/array)` whenever `release != NULL` when the capsule is deallocated — and the `capsule_tuple` returned by `__arrow_c_array__` is a temporary that drops (refcount→0) as soon as record_batch_from_pyarrow returns, so this fires immediately. (3) arrow-rs 58.3.0's `FFI_ArrowArray` Drop impl (~/.cargo/registry/.../arrow-data-58.3.0/src/ffi.rs:67-73) calls `release(self)` on the ptr::read copy when the imported ArrayData drops — same release fn, same already-deleted `private_data` → double delete. arrow-rs's own `FFI_ArrowArray::from_raw` (ffi.rs:234-236) is `std::ptr::replace(array, Self::empty())` precisely to null the source's release, exactly as the claim states. (4) Reachability: public Python API — sync StreamingAppender::write_batch (builders.rs:2291-2292) and AsyncStreamingAppender::write_batch (async_api.rs:1826) both call record_batch_from_pyarrow. (5) Empirical repro: in bindings/uni-db/.venv (extension .so built Jul 3, newer than last builders.rs change Jun 23, so it contains this code), a plain `tx.appender("Person"); app.write_batch(pa.record_batch(...))` aborts DURING write_batch with `free(): double free detected in tcache 2` (SIGABRT, exit 134). This is not an edge case — it is a deterministic crash on the first use of write_batch with a real pyarrow RecordBatch, and the same mechanism implies use-after-free of the zero-copy row buffers (silent data corruption) in runs where the allocator does not detect the double free. Severity critical is supported: unconditional memory-safety violation (heap corruption/crash and potential silent corruption of ingested data) on a documented public ingestion API.

</details>

### `crates/uni-query/src/query/df_planner.rs:4918` — CRITICAL
**Non-DISTINCT count(variable) is translated to count(lit(1)), which counts NULL entity rows produced by OPTIONAL MATCH instead of excluding them per Cypher semantics.**

*Failure scenario:* EMPIRICALLY CONFIRMED: 'MATCH (n:Person {name:"Charlie"}) OPTIONAL MATCH (n)-[:KNOWS]->(m:Person) RETURN count(m)' returns 1 (Charlie has no KNOWS edges; expected 0), and 'OPTIONAL MATCH (x:NoSuchLabel) RETURN count(x)' returns 1 (expected 0). The DISTINCT branch explicitly uses the {var}._vid column to exclude nulls, but the non-distinct branch falls through to count(lit(1)) which counts every row including the all-NULL OPTIONAL row. TCK has no non-distinct count(node)-over-unmatched-OPTIONAL scenario, so this passes CI while silently returning wrong aggregates on a very common query shape.

<details><summary>Verifier evidence</summary>

CONFIRMED by code reading and a live end-to-end reproduction. Code: crates/uni-query/src/query/df_planner.rs:4894-4919 — in translate_aggregates, for `count(variable)` the non-DISTINCT branch (line 4918) emits `count(datafusion::logical_expr::lit(1))`, which counts every input row including the all-NULL padding row produced by an unmatched OPTIONAL MATCH. The DISTINCT branch (lines 4895-4916) deliberately counts the `{var}._vid`/`{var}._eid` identity column precisely so "null rows (from OPTIONAL MATCH) are excluded" (the comment at lines 4890-4891 acknowledges this exclusion requirement but only applies it to DISTINCT). No guard elsewhere prevents the null row from reaching the aggregate: I built and ran a repro example against Uni::in_memory(). Results: `MATCH (n:Person {name:'Charlie'}) OPTIONAL MATCH (n)-[:KNOWS]->(m:Person) RETURN count(m)` → 1 (Cypher expects 0); `OPTIONAL MATCH (x:NoSuchLabel) RETURN count(x)` → 1 (expects 0); control cases behave correctly: `count(DISTINCT m)` → 0, `count(m.name)` → 0, and matched-case `count(m)` for Alice → 1. So the defect is real, reachable via the most common OPTIONAL MATCH + count(entity) idiom, silent (no error), and produces wrong aggregate results. Fix direction is already demonstrated in the adjacent DISTINCT branch (count the non-null `_vid`/`_eid` identity column instead of lit(1)). Severity: wrong query results on a core, extremely common query shape in a database engine — critical per the given rubric (silently wrong aggregates, off-by-one per unmatched group, no workaround visible to users who write the idiomatic query).

</details>

### `crates/uni/src/api/fork_maintenance.rs:61` — CRITICAL (self-verified by main session)
**sweep_tick wraps the live UniInner in a transient `Uni { inner }` whose Drop impl calls `shutdown_handle.shutdown_blocking()`, broadcasting shutdown to every background task of the still-running database.**

*Failure scenario:* Create any fork with a TTL (or with UniConfig::fork_default_ttl set) and let it expire. The sweeper tick upgrades the Weak, builds `let db = Uni { inner }` to call drop_fork_cascade, and when `db` drops at end of sweep_tick, `impl Drop for Uni` (api/mod.rs:2967) fires `shutdown_blocking()` -> `tx.send(())` on the shared ShutdownHandle broadcast. Every subscriber exits permanently: the auto-flush ticker (L0 is never again flushed to Lance), background compaction, the index-rebuild worker, the CDC runtime (downstream consumers silently stop receiving commits), the job scheduler, the deferral queue, and both fork maintenance loops themselves (so no further expired fork is ever swept). The database keeps answering queries, so the loss of all background processing is completely silent. The comment on line 60 ('thin newtype; does not duplicate state') misses that Drop is not neutral.

*Verified:* `Uni::drop` (crates/uni/src/api/mod.rs:2969) unconditionally calls `shutdown_handle.shutdown_blocking()`, which (uni-plugin-host/src/shutdown.rs:76-78) broadcasts `()` on the same channel every background driver subscribes to — CDC, scheduler, deferral tick, and the sweeper itself. `sweep_tick` wraps an *upgraded* `Arc<UniInner>` in `let db = Uni { inner }` (fork_maintenance.rs:61); when `db` drops at function end, it signals shutdown on the live, still-open database. Triggered the first time any fork's TTL expires and the sweeper finds ≥1 expired fork.


---

## HIGH (58 confirmed)


### uni-algo

- **`crates/uni-algo/src/algo/algorithms/bidirectional_dijkstra.rs:145`** — The backward step resolves the weight of edge v->u by linearly scanning v's out-neighbors and taking the FIRST match, while the forward direction relaxes every parallel edge; with parallel edges of different weights the backward distance uses whichever edge happens to be first in CSR order, overestimating dist_bwd and producing a non-minimal reported distance.
  - *Scenario:* Two parallel edges v->u with weights [5.0, 1.0] in CSR order on the target side of the search: backward relaxation of u charges 5.0 instead of 1.0, so dist_bwd along the true shortest path is overestimated; the termination test min_fwd+min_bwd >= mu can then stop with mu computed from the inflated backward distance, and uni.algo.bidirectionalDijkstra returns a distance larger than the true shortes
- **`crates/uni-algo/src/algo/algorithms/kcore.rs:110`** — K-core mixes two degree conventions: initial degrees count edge multiplicity (out_degree + in_degree counts a reciprocal u<->v pair as 2 per endpoint) but the peeling loop dedups neighbors and decrements only 1 per unique neighbor, so removals under-subtract and core numbers are inflated.
  - *Scenario:* Undirected graph stored as reciprocal edges (u->v and v->u), e.g. a star: center c mutually linked to leaves l1..l3. Degrees: c=6, leaves=2. Peeling each leaf decrements c by 1 (dedup) instead of 2, so c ends with core number 3; the correct core number is 1 (simple) or 2 (multigraph) for every node. uni.algo.kCore reports wrong coreNumber and wrong k-core membership on any graph with mutual edges.
- **`crates/uni-algo/src/algo/algorithms/mst.rs:55`** — Kruskal edge collection keeps an edge only when source slot < destination slot (`if u < v`), silently dropping every directed edge whose source slot is greater, instead of normalizing to (min,max); the cypher adapter builds the projection with include_reverse=false so the dropped edges are unrecoverable.
  - *Scenario:* Graph with vertices Vid 1 and Vid 2 and a single edge 2->1 (slots: 1->0). `u < v` is false, the edge list is empty, and uni.algo.mst returns zero edges / total_weight 0 for a connected graph. Generally any dataset whose edges point from higher to lower VIDs loses those edges, yielding a wrong (heavier or disconnected) spanning forest.

### uni-bulk

- **`crates/uni-bulk/src/bulk.rs:581`** — UNIQUE validation checks only keys seen within this bulk load (`seen_unique_keys`), never rows already committed in storage, so bulk-loading a key that pre-exists in the database silently creates a duplicate.
  - *Scenario:* Table `User` has a UNIQUE(email) constraint and already contains a committed vertex with email 'a@x.com'. A `BulkWriter` with default `validate_constraints=true` (whose builder doc claims it matches 'the behavior of regular Writer', which calls `check_unique_constraint_multi` against L0+storage) runs `insert_vertices("User", [{email:'a@x.com'}])`. Validation consults only the writer-lifetime `seen
- **`crates/uni-bulk/src/flush_intent.rs:184`** — Crash recovery deletes the bulk-flush intent marker even when one or more table rollbacks failed, permanently abandoning reconciliation and leaving tables divergent.
  - *Scenario:* A bulk load crashes mid-flush leaving an Active marker for the per-label table and the main vertices table. On reopen, rollback_table succeeds for the per-label table but fails transiently for the main table (e.g. object-store timeout). The Active branch only increments `failures` and logs a warning, then falls through to `clear(&store).await?` which deletes the marker. The next reopen finds no ma

### uni-common

- **`crates/uni-common/src/core/schema.rs:2107`** — rename_property never validates the new property name, bypassing both the reserved-storage-column guard and the leading-underscore rule that add_property/declare_property enforce.
  - *Scenario:* User runs `ALTER ... RENAME PROPERTY foo TO ext_id` (or `_vid`, `eid`, `src_vid`, `dst_vid`, `op`, `overflow_json`) — the DDL path in crates/uni-query/src/query/executor/write.rs:1408 calls sm.rename_property directly with no validate_property_name(new_name). The schema now declares a user property colliding with an internal Arrow column; per this module's own doc comment on validate_property_name

### uni-crdt

- **`crates/uni-crdt/src/orset.rs:253`** — The v1-to-v2 ORSet upgrade mints synthetic dots under the shared '__legacy__' actor with counters restarting at 1 per decode, so two independently-upgraded replicas produce colliding dots and each other's version vectors falsely 'observe' the other's elements, causing silent element loss on merge.
  - *Scenario:* Replica A decodes a legacy v1 payload with live elements {x, y} (upgrade yields x->(__legacy__,1), y->(__legacy__,2), vv{__legacy__:2}); replica B decodes a diverged v1 payload where only y is live (y->(__legacy__,1), vv{__legacy__:1}). A.merge(&B): for key x, the self-only dot (__legacy__,1) fails the survival test 1 > other.vv[__legacy__]=1, so x is deleted from the merged set even though no rep

### uni-cypher

- **`crates/uni-cypher/src/grammar/mod.rs:113`** — check_nesting_depth treats every bare word 'end' as closing a CASE level, but 'end' is a legal non-reserved identifier/function name, so crafted input keeps the counter near zero while actual parser recursion grows unboundedly, defeating the stack-overflow guard and violating its documented never-under-counts invariant.
  - *Scenario:* Query 'RETURN ' + 'end('*30000 + '1' + ')'*30000 passes the depth check (each 'end' decrements what the '(' incremented, max_depth stays ~1) and then overflows the thread stack during pest parsing — empirically reproduced: the process aborted with 'fatal runtime error: stack overflow' (core dumped), an uncatchable crash of the embedding host from a query string.
- **`crates/uni-cypher/src/grammar/mod.rs:141`** — parse_expression parses with Rule::expression, which has no SOI/EOI anchor (expression = { or_expression } in cypher.pest), so pest matches a prefix and trailing garbage is silently discarded, returning a truncated expression instead of an error.
  - *Scenario:* Empirically reproduced: parse_expression("n.age > 18 AND") returns Ok(n.age > 18) and parse_expression("1 + 2 THIS IS GARBAGE )))") returns Ok(1 + 2). Callers pass raw user strings — plugin trigger conditions (uni-plugin-host/src/triggers.rs:135) and custom UDF bodies (uni-plugin-custom) — so a malformed condition like "a = 1 b = 2" registers successfully and the trigger evaluates only "a = 1", si

### uni-db

- **`bindings/uni-db/src/convert.rs:299`** — Naive datetime ingestion uses Python's local-timezone-dependent .timestamp() (and the reverse path uses naive fromtimestamp()), while the Rust core defines LocalDateTime.nanos_since_epoch as wall-clock-as-if-UTC, so stored values are shifted by the machine's UTC offset.
  - *Scenario:* On a machine with TZ=America/New_York, py_object_to_value(datetime(2026,7,4,12,0)) calls .timestamp() which interprets the naive value as local time, storing nanos for 16:00 UTC; uni-common's Display renders it as '2026-07-04T16:00:00' (from_timestamp_nanos().naive_utc()), and 'WHERE n.t = localdatetime("2026-07-04T12:00:00")' finds nothing. The reverse arm (line 153, fromtimestamp without tz) shi
- **`bindings/uni-db/src/convert.rs:416`** — py_object_to_value silently converts any unrecognized Python type (tuple, set, numpy array, custom objects) to Value::Null instead of raising, so parameters are stored as NULL without any error.
  - *Scenario:* db.execute("CREATE (n:X {p: $v})", {"v": (1, 2, 3)}) — a tuple is not a PyList, falls through every branch, and stores p = NULL with no exception; the user discovers missing data only later. py_object_to_json (line 248) has the same silent-Null fallback, so bulk-insert property maps drop such values too.
- **`bindings/uni-db/src/types.rs:1561`** — PyPreparedQuery::execute acquires the std::sync::Mutex while holding the GIL and only then calls py.detach(block_on(...)), re-acquiring the GIL while still holding the mutex — a GIL/mutex ABBA lock-order inversion (same pattern in PyPreparedLocy::execute at line 1849, PyPreparedQueryBinder::execute at line 1964, and PyPreparedLocyBinder::execute at line 2011).
  - *Scenario:* Two Python threads share one PreparedQuery (the Mutex exists precisely to permit this) and both call execute(). Thread A takes the mutex, detaches the GIL, and runs the query; thread B, holding the GIL, blocks inside inner.lock(). When A's query finishes, py.detach must re-acquire the GIL held by B, while B waits on the mutex held by A — permanent deadlock that freezes the entire interpreter (B ne
- **`bindings/uni-db/src/types.rs:2131`** — PyCommitStream::__next__ holds the stream mutex across a potentially unbounded py.detach(block_on(stream.next())) and re-attaches the GIL while still holding it, so close()/__exit__ (which lock the same mutex while holding the GIL) deadlock against an in-flight iteration.
  - *Scenario:* Thread A iterates `for n in session.watch(): ...` and is blocked in stream.next() waiting for a commit (mutex held, GIL released). Thread B calls stream.close() to shut the consumer down: it blocks on the mutex while holding the GIL, freezing every Python thread. If no commit ever arrives the process hangs forever; if one arrives, A returns from block_on and waits for the GIL held by B while B wai

### uni-fork

- **`crates/uni-fork/src/diff.rs:597`** — run_promote swallows a failed fork-side get_vertex_ext_ids() with unwrap_or_default(), and the delete-promotion pass then interprets the resulting empty ext_id map as 'every ext_id-bearing baseline row was deleted on the fork', mass-deleting live primary vertices.
  - *Scenario:* PromoteOptions::with_merge() (delete_promotion=true) with a fork that deleted nothing; the fork's ext-id scan hits a transient Lance/scan error so fork_ext_ids is silently empty. In the delete pass (line 999) ext_id_for() returns None for every fork row, fork_now_ext stays empty, so deleted_ext = ALL baseline ext_ids (line 1016); each is resolved against primary (whose own ext-id fetch succeeded) 
- **`crates/uni-fork/src/diff.rs:643`** — CONFIRMED BY REPRO: the promote engine's recomputed content-UID (ext_id passed as parameter, properties WITHOUT the 'ext_id' key — query results strip it) can never equal the UID registered in UidIndex (writer.rs:5183 hashes properties that still CONTAIN the 'ext_id' key), so batch_resolve_primary_vids never resolves any ext_id-bearing row and the documented UID dedup silently never fires for them.
  - *Scenario:* Reproduced: CREATE (:Person {ext_id:'p1', name:'Alice'}), flush, fork with NO changes, then default promote_from_fork(label Person) → vertices_inserted=1, vertices_skipped_uid_conflict=0, and primary ends with TWO Alices (twin has no ext_id). Every re-promote adds another twin (non-idempotent, unbounded duplication). Same mismatch makes edge-endpoint resolution fail for ext_id-bearing endpoints, s
- **`crates/uni-fork/src/diff.rs:1031`** — CONFIRMED BY REPRO: delete-promotion never consults ConflictPolicy or the fork-point baseline props — the primary's current properties are fetched and then discarded as `_props` — so a fork-delete racing a primary-edit deletes primary's concurrently edited row even under ConflictPolicy::Skip ('leave primary's value untouched'), and vertices_conflicting is not incremented.
  - *Scenario:* Reproduced: primary has Alice (ext_id p1, age 30); fork deletes Alice; primary then edits Alice to age 99; promote with PromoteOptions::with_merge() (Skip policy) → report shows vertices_deleted=1, vertices_conflicting=0, and Alice (with primary's post-fork edit) is gone from primary. The update path at lines 699-707 detects exactly this both-sides-diverged case and honors Skip; the delete pass sk

### uni-locy

- **`crates/uni-locy/src/calibration.rs:341`** — IsotonicFitter's PAV never pools blocks with tied prediction values (merge only fires when ma > mb), producing multiple knots at the same x with different y, and apply() at that x returns the lowest block's mean instead of the pooled mean.
  - *Scenario:* Confirmed by repro: fit on preds [0.7, 0.7, 0.7, 0.7], labels [F, F, T, T] yields knots [(0.7,0),(0.7,0),(0.7,1),(0.7,1)]; apply(0.7) returns 0.0 where the correct isotonic value is the pooled mean 0.5, and apply(0.700001) returns 1.0. Calibrated Brier becomes 0.5 vs raw 0.29 — 'calibration' makes the classifier strictly worse. Tied predictions are the common case for discrete/constant-output clas
- **`crates/uni-locy/src/compiler/typecheck.rs:128`** — Typecheck compares raw (unresolved) IS-ref rule names against module-qualified SCC/rule-catalog names, so self-recursion detection and IS-ref validation break inside any program with a MODULE declaration.
  - *Scenario:* Confirmed by repro: `MODULE foo CREATE RULE r ... CREATE RULE r AS MATCH ... WHERE mid IS r TO b ALONG total = prev.total + e.weight FOLD total = MSUM(total) ...` fails to compile with PrevInBaseCase { rule: "foo.r", field: "total" } — dependency.rs resolves "r" to "foo.r" for stratification, but check()'s has_self_is tests scc_rules.contains("r") against {"foo.r"} and gets false, so the recursive

### uni-plugin

- **`crates/uni-plugin/src/registry.rs:911`** — apply_pending overwrites (not merges) the per-plugin ownership record, so a second commit under the same plugin id orphans every surface registered by earlier commits.
  - *Scenario:* CALL uni.plugin.declareFunction('mycorp.f1', ...) then declareFunction('mycorp.f2', ...): uni-plugin-custom's installers each run their own PluginRegistrar::commit_to_registry with plugin id `mycorp` (declared_plugin_id), so the second apply_pending's `self.per_plugin.read().insert(plugin_id, record)` replaces f1's record with one containing only f2. remove_plugin("mycorp") (hot reload, Uni::remov

### uni-plugin-custom

- **`crates/uni-plugin-custom/src/decode.rs:64`** — map_plugin_error folds every DuplicateRegistration into CustomError::NativeShadow, so re-declaring an existing *declared* qname (a supported store operation — DeclaredPluginStore::declare 'replace an existing declaration') is misclassified as native shadowing: the new body is stored/persisted as inactive while the registry silently keeps executing the old body.
  - *Scenario:* CALL uni.plugin.declareFunction('mycorp.f', '$x + 1', 'int', '["x"]') then re-declare with body '$x + 2': the second install hits DuplicateRegistration from its own prior synthetic registration → NativeShadow path (lib.rs:854) marks the NEW record inactive and persists it, returns registered=false, and queries calling mycorp.f keep returning x+1. After restart, reactivation installs x+2 but the re
- **`crates/uni-plugin-custom/src/lib.rs:773`** — dropDeclared removes the entire namespace-level PluginId (declared_plugin_id = first dotted segment), and because each declaration in a namespace commits its own registrar batch under that same PluginId, PluginRegistry::apply_pending (registry.rs:911) overwrites the previous ownership record — dropping one declared plugin unregisters a sibling and leaves the dropped one invocable.
  - *Scenario:* Declare mycorp.f1 then mycorp.f2 (both register under PluginId 'mycorp'; the second apply_pending replaces per_plugin['mycorp'] with a record containing only f2). CALL uni.plugin.dropDeclared('mycorp.f1') → registry.remove_plugin(PluginId('mycorp')) removes f2's scalar registration and leaves f1's in place. Result: mycorp.f1 (dropped, absent from listDeclared) still executes in queries, while myco

### uni-plugin-extism

- **`crates/uni-plugin-extism/src/host_svc/mod.rs:86`** — from_hex slices the guest-controlled string with byte indices (&s[i..i+2]) without checking char boundaries, so even-byte-length inputs containing multibyte UTF-8 panic instead of returning an error.
  - *Scenario:* A plugin calls uni_kms_sign with {"key_id":"k","data_hex":"aéb"} (serialized 'aéb' = 4 bytes, passes the even-length check). At i=0, &s[0..2] ends inside the 2-byte 'é' and panics 'byte index 2 is not a char boundary' (reproduced in a standalone build). The panic unwinds while the host_fn! shell holds the HostSvcCtx mutex guard, poisoning it — all subsequent uni.kms/uni.http/uni.secret calls on th
- **`crates/uni-plugin-extism/src/loader.rs:416`** — Pass-1 bootstrap in ExtismLoader::load builds the plugin with effective = host_grants (un-intersected) and materializes all host-offered service fns, so guest code in the `manifest` export runs with capabilities never narrowed by declared-cap intersection.
  - *Scenario:* A plugin whose manifest declares zero capabilities ships a `manifest` export that first calls `uni_http_post`/`uni_kms_sign`. During load, build_plugin is called with bootstrap_prepared.effective = host_grants and runtime_fns_for_load registers uni_http_*/uni_kms_*/uni_secret_acquire with the host's full offered attenuation patterns, so do_http/do_sign pass their allow-list checks and the calls su

### uni-plugin-host

- **`crates/uni-plugin-host/src/cdc_runtime.rs:334`** — A failed CdcStream::deliver is logged and skipped with `continue`, but the runtime keeps delivering subsequent commits to that stream and checkpointing them, creating a permanent, undetectable gap in the CDC feed (same for broadcast Lagged drops at line 258).
  - *Scenario:* Provider transiently errors on the batch for commit N (network blip). The runtime warns and continues; on commit N+1 deliver succeeds and stream.checkpoint() is persisted via write_one, advancing the sidecar LSN past N. The runtime has no redelivery path (it only forwards live broadcast commits), so commit N's mutations are permanently missing from the CDC output — both for the running process and

### uni-plugin-pyo3

- **`crates/uni-plugin-pyo3/src/adapter_aggregate.rs:268`** — state() of an empty accumulator emits Utf8(Some("{}")) but merge_batch only skips NULL entries, so the "empty state" is not the promised no-op — it is json.loads'ed to {} and fed to the user's merge(state, {}).
  - *Scenario:* Global aggregate (no GROUP BY) over a multi-partition DataFusion scan where at least one partition has zero rows: that partition's accumulator ships state "{}"; the Final accumulator's merge_batch calls the user's merge(init_state, {}). With the dict-shaped state this crate itself documents and tests (merge does a["sum"] + b["sum"]), Python raises KeyError and the whole aggregation query fails; a 

### uni-query

- **`crates/uni-query/src/query/df_graph/apply.rs:823`** — The per-row subplan dedup cache in run_apply is keyed only on row params and is consulted even when the correlated subquery contains writes, so duplicate outer rows execute the subquery's side effects only once.
  - *Scenario:* UNWIND [1,1,1] AS x CALL { CREATE (:N) } — SubqueryCall plans to GraphApplyExec (df_planner.rs:1267/2013, which explicitly unwraps the Limit-0 wrapper so 'the side effect executes per outer row'); all three rows have identical row_params {x:1}, so rows 2 and 3 hit subplan_cache and execute_subplan is skipped — only 1 node is created instead of 3. plan_contains_writes() is computed at line 631 but 
- **`crates/uni-query/src/query/df_graph/apply.rs:350`** — evaluate_filter/evaluate_comparison/resolve_expr_value mis-evaluate any pushed input_filter conjunct that is not a simple Eq/NotEq/Lt/LtEq/Gt/GtEq over literals/variables/dotted properties: unsupported operators (STARTS WITH, CONTAINS, Regex, XOR, arithmetic, function calls, IN, CASE) evaluate to false (or Null operands), and NOT of an unsupported expression inverts to true.
  - *Scenario:* planner.rs push_predicates_to_apply (line 8112) moves any conjunct referencing only input variables into Apply.input_filter and REMOVES it from the residual Filter, so this code is the sole enforcement point. E.g. MATCH (a:A), (b:B) CALL proc() YIELD s WHERE a.name STARTS WITH b.prefix — the two-variable predicate is not scan-pushable, gets pushed to input_filter, and evaluate_comparison's `_ => f
- **`crates/uni-query/src/query/df_graph/ext_id_lookup.rs:107`** — OPTIONAL MATCH by ext_id with no match errors out instead of producing a null row: build_schema declares `{var}._vid`/`{var}.ext_id`/`{var}._label` as non-nullable, but build_null_row appends nulls to those very columns, and arrow-array 58's RecordBatch::try_new rejects nulls in non-nullable fields.
  - *Scenario:* Query `OPTIONAL MATCH (n {ext_id: 'does-not-exist'}) RETURN n` — the planner emits ExtIdLookup{optional:true} (planner.rs:5929); execute_lookup finds no vertex, calls build_null_row, which appends null to the UInt64 `n._vid` column (nullable=false); RecordBatch::try_new returns "Column 'n._vid' is declared as non-nullable but contains null values" (arrow-array-58.3.0/src/record_batch.rs:350), so t
- **`crates/uni-query/src/query/df_graph/locy_fold.rs:720`** — TopKProofs MNOR returns exactly 1.0 for any group that mixes supported and unsupported rows: the plain noisy-OR fallback is gated on base_weights.is_empty() (no proof anywhere has support), so rows whose body-hash misses body_support_map contribute empty-base_rvs proofs to the DNF, and DependencyDnf::weight treats an empty clause as trivially true (probability 1.0), discarding every row's actual weight.
  - *Scenario:* Under SemiringKind::TopKProofs, a recursive rule with a base clause (rows with no IS-ref support, e.g. MNOR over e.p on direct edges) and a recursive clause (rows with IS-ref support): any key group containing both kinds of rows — e.g. row1 support {A} weight 0.5, row2 no support weight 0.1 — yields base_weights={A:0.5}, DNF clauses [{A},{}], weight = 0.5 + 1.0 - 0.5 = 1.0. Correct noisy-OR is 1 -
- **`crates/uni-query/src/query/df_graph/locy_query.rs:138`** — RETURN DISTINCT dedup keys rows by `format!("{row:?}")` where FactRow is std HashMap<String, Value>, whose Debug iteration order differs between instances with identical content, so duplicate multi-column rows survive DISTINCT.
  - *Scenario:* A Locy `QUERY rule ... RETURN DISTINCT a, b` where two result rows have identical (a, b) values: each projected row is a fresh HashMap with its own RandomState, so equal-content rows format to different Debug strings (empirically verified: two HashMaps with the same 5 keys print in different orders), `seen.insert(key)` returns true for both, and the caller receives duplicate rows despite DISTINCT.
- **`crates/uni-query/src/query/df_graph/optional_filter.rs:370`** — OptionalFilterExec applies OPTIONAL-MATCH null-row recovery independently per input batch with no cross-batch state, so a source group whose rows span two batches yields duplicate or spurious NULL rows.
  - *Scenario:* A source vertex's matched rows straddle a batch boundary (large expansion or upstream join output splitting a group across 8192-row batches). Case 1: all rows in batch 1 fail the predicate but a row in batch 2 passes -> batch 1 emits a NULL-padded recovery row AND batch 2 emits the real row, producing an extra row Cypher forbids. Case 2: the group's rows fail in both batches -> two NULL recovery r
- **`crates/uni-query/src/query/df_graph/pattern_comprehension.rs:301`** — Multi-hop pattern comprehension builds inner-batch property columns in a different order (per-step vertex-then-edge interleaved) than build_inner_schema declares (all vertex props across steps, then all edge props), silently misaligning columns.
  - *Scenario:* For `[(a)-[r1:X]->(b)-[r2:Y]->(c) | r1.w]` where the WHERE/map exprs reference both an edge prop on step 0 (r1.w) and a vertex prop on step 1 (c.name): evaluate() pushes columns as [.., r1.w, c.name] while build_inner_schema (expr_compiler.rs:1345 uses the same maps) declares [.., c.name, r1.w]. Both are LargeBinary so RecordBatch::try_new succeeds; the predicate and map expressions (compiled by c
- **`crates/uni-query/src/query/df_graph/pattern_exists.rs:410`** — When a bound target variable's VID is NULL for a row, the bound-target check is skipped entirely (treated as unbound), so the pattern EXISTS evaluates true if ANY neighbor exists instead of false.
  - *Scenario:* MATCH (n) OPTIONAL MATCH ... producing m = NULL, then WHERE (n)-[:R]->(m): expected_target resolves to None for the NULL row (lines 410-416, same skip in the property-predicate branch at 322-326), so the row passes if n has any R-neighbor at all. Cypher semantics require the pattern predicate with a NULL endpoint to evaluate to null (row filtered out); rows that should be excluded are returned.
- **`crates/uni-query/src/query/df_graph/scan.rs:2806`** — L0 label overlay is union-only and ignores the vertex_label_overwrites replacement marker, so a REMOVE n:Label whose pre-remove state is already flushed to Lance is invisible to scans: the removed label is resurrected in labels(n) and MATCH by the removed label still returns the node (same union pattern in build_labels_column_for_known_label at line 1818).
  - *Scenario:* Vertex flushed to Lance with labels [A,B]; execute_remove_labels (write.rs:3911) calls only L0Buffer::set_vertex_labels(vid,[A]) which never touches vertex_properties/versions. A schemaless scan then finds no L0 row for vid (build_l0_schemaless_vertex_batch keys candidates on vertex_properties), the Lance row with labels [A,B] wins the merge, and the overlay at line 2804-2813 only ADDS L0 labels (
- **`crates/uni-query/src/query/df_graph/vid_lookup_join.rs:463`** — VidJoinKind::Left NULL-pads unmatched BUILD-side rows and drops unmatched PROBE-side rows regardless of which side is the probe, so a LEFT outer join planned with probe_side==Left executes with inverted outer semantics (a RIGHT join).
  - *Scenario:* MATCH (a:Person) OPTIONAL MATCH (b:Employee) WHERE id(a) = id(b): planner.rs rewrites id() to Property(_, "_vid"), classify_join_predicate yields JoinType::Left, and try_emit_vid_lookup_join (df_planner.rs:4211-4221) picks probe=Left because l_expr is the _vid property, with left being a bare GraphScanExec. run_join then materializes the RIGHT (optional) side as build; Persons with no matching Emp
- **`crates/uni-query/src/query/df_planner.rs:5766`** — plan_window_functions builds one shared SortExec by concatenating every window's PARTITION BY/ORDER BY keys, so any window whose ORDER BY conflicts with an earlier window's is evaluated over wrongly-ordered input.
  - *Scenario:* EMPIRICALLY CONFIRMED: 'MATCH (n:P) RETURN n.name, row_number() OVER (ORDER BY n.age ASC) AS r1, row_number() OVER (ORDER BY n.age DESC) AS r2' returns r1 == r2 == (1,2,3) for ages (10,20,30); r2 should be (3,2,1). The combined sort (age ASC, age DESC) satisfies only the first window; WindowAggExec computes the second window's values (row_number/rank/lag/first_value...) over ASC-sorted rows, silen
- **`crates/uni-query/src/query/df_planner.rs:2963`** — plan_traverse_virtual_edge maps AstDirection::Both to the same (src->dst) join as Outgoing, so undirected traversal over a plugin virtual edge type only matches the outgoing orientation.
  - *Scenario:* With a registered virtual edge type VE containing a row (src=A, dst=B), 'MATCH (b)-[:VE]-(a) RETURN a' anchored at B returns nothing: the Both arm sets right_key = {edge}._src_vid, so the HashJoin only matches rows where the bound node is the edge SOURCE. An undirected pattern must union both orientations (join on _src_vid OR _dst_vid); half the expected matches (all incoming ones) are silently dr
- **`crates/uni-query/src/query/executor/core.rs:99`** — cypher_cross_type_cmp falls through to Ordering::Equal for same-rank value kinds it does not handle (Temporal, Map, Bytes, Vector all share rank 5), so MIN/MAX aggregates over temporal values return the first-encountered value rather than the true min/max.
  - *Scenario:* A query aggregated through the row-based path (Executor::execute_aggregate in read.rs:3345, which uses these Accumulators) computing `RETURN min(n.when)` over Value::Temporal DateTime properties: every comparison of two Temporals returns Equal, Accumulator::Min keeps whatever row arrived first, and the reported minimum is an arbitrary row's timestamp, not the smallest.
- **`crates/uni-query/src/query/locy_planner.rs:798`** — build_rule reads HAVING (and BEST BY criteria at line 806) from the FIRST clause only, while fold_bindings are deliberately sourced from whichever clause has a FOLD — so a multi-clause rule whose FOLD+HAVING/BEST BY live on a non-first clause silently loses its HAVING filter and BEST BY pruning.
  - *Scenario:* Rule with two definitions: clause 1 = base (no FOLD/HAVING), clause 2 = recursive with 'FOLD n = COUNT(*) ... HAVING n > 3'. uni-locy typecheck (typecheck.rs:163) only requires HAVING to co-occur with FOLD per-definition, so this compiles; fold_bindings are correctly picked from clause 2 via find(|c| !c.fold.is_empty()), but rule.having is taken from clause 1 (empty). The runtime post-fixpoint cha
- **`crates/uni-query/src/query/planner.rs:6130`** — plan_where_clause drops the vector_similarity predicate entirely when the variable's Scan sits under a Traverse (or Sort/Aggregate/Apply): find_scan_label_id descends those nodes but replace_scan_with_knn has no arms for them (`other => other`), yet current_predicate is unconditionally replaced with the residual/TRUE.
  - *Scenario:* MATCH (a:Doc)-[:REL]->(b) WHERE vector_similarity(a.embedding, $q) > 0.8 RETURN b — plan is Traverse{Scan(a)}; find_scan_label_id returns Some via the Traverse arm (line 7549), replace_scan_with_knn matches no arm and returns the plan unchanged, extraction.residual is None so current_predicate becomes Expr::TRUE, and step 5 adds no Filter. Every (a,b) pair is returned with no similarity filtering 
- **`crates/uni-query/src/query/planner.rs:6211`** — WHERE predicates on a scan-bound variable are silently discarded when the Scan lies below Sort/Limit/Aggregate/Apply: find_scan_label_id descends those nodes (lines 7541-7546) so the predicate is extracted and removed from current_predicate, but push_predicate_to_scan has no arms for them and falls through `other => other` (line 7739), losing the predicate.
  - *Scenario:* MATCH (n:Person) WITH n SKIP 1 MATCH (n)-[:KNOWS]->(m) WHERE n.age > 30 RETURN m — plan is Traverse{Limit{Project{Scan(n)}}}; `n.age > 30` is pushable per PredicateAnalyzer::is_pushable, is stripped from current_predicate, then push_predicate_to_scan recurses into the Traverse input, hits Limit, and returns the plan unchanged. The WHERE is dropped entirely, returning rows with n.age <= 30. Same dr
- **`crates/uni-query/src/query/planner.rs:3351`** — plan_return_clause wraps LIMIT/SKIP below DISTINCT (Limit at line 3288, Project at 3345, Distinct at 3351), so SKIP/LIMIT are applied to pre-deduplication rows instead of the distinct result rows required by openCypher; plan_with_clause has the same ordering (Limit line 7265, Distinct line 7284).
  - *Scenario:* Rows produce n.name values ["alice","alice","bob"]; RETURN DISTINCT n.name LIMIT 2 executes Limit(2) first (keeps [alice,alice]) then Distinct, returning 1 row ["alice"] instead of the correct 2 rows ["alice","bob"]. RETURN DISTINCT x SKIP 1 similarly skips a duplicate pre-distinct row and can return a value that should have been skipped or drop one that shouldn't.
- **`crates/uni-query/src/query/planner.rs:6224`** — Traverse-target analog of the scan-pushdown drop: is_traverse_target descends Sort/Limit/Aggregate/Apply (lines 6800-6805) so the predicate is consumed, but push_predicate_to_traverse only recurses through Traverse/Filter/Project/CrossJoin and falls through `other => other` (line 6935), silently losing the predicate.
  - *Scenario:* MATCH (a:X)-[:R]->(b:Y) WITH a, b SKIP 1 MATCH (c:Z) WHERE b.p = 1 RETURN c — plan is CrossJoin{Limit{Project{Traverse(target=b)}}, Scan(c)}; is_traverse_target finds b through the Limit, extract_variable_predicates removes `b.p = 1` from current_predicate, push_predicate_to_traverse recurses into the CrossJoin left, hits Limit, returns unchanged. The filter on b.p vanishes and rows with b.p != 1 
- **`crates/uni-query/src/query/planner.rs:6168`** — The WHERE label-disjunction rewrite marks the conjunct consumed based on is_scan_all_for (which descends Sort/Limit/Aggregate/Apply/Union, lines 6509-6520) but replace_scan_all_with_label_union has no arms for those nodes (`other => other`, line 6665), so the label predicate is dropped without the ScanAll ever being rewritten.
  - *Scenario:* MATCH (n) WITH n LIMIT 10 MATCH (m) WHERE n:A OR n:B RETURN n, m — plan is CrossJoin{Limit{Project{ScanAll(n)}}, ScanAll(m)}; is_scan_all_for returns true through the Limit, try_label_or_to_union matches, replace_scan_all_with_label_union no-ops on the Limit node, yet consumed=true removes the conjunct from `keep`. The `n:A OR n:B` filter is silently discarded and nodes of every label are returned
- **`crates/uni-query/src/query/planner.rs:4897`** — In plan_path, last_outer_node_var is never updated when a Parenthesized (QPP) element consumes its outer target node (only the Node arm updates it at lines 4728/4807), so a second consecutive QPP is anchored at the stale first source instead of the previous QPP's target.
  - *Scenario:* MATCH (a)((x)-[:R]->(y)){1,2}(b)((w)-[:S]->(z)){1,2}(c) — after the first QPP consumes (b) via `i += 2`, last_outer_node_var still holds "a"; the second QPP's source_variable resolves to a, producing traversals a→b crossed with a→c instead of the connected chain a→b→c. The grammar (`pattern_element+` with parenthesized_pattern) accepts this shape, so the wrong join is silent.

### uni-query-functions

- **`crates/uni-query-functions/src/df_expr.rs:3368`** — coerce_case_expr wraps WHEN expressions in _cv_to_bool even for simple CASE (operand form), where the WHEN is a comparison VALUE, not a boolean; the subsequent rewrite_simple_case_to_generic then compares the operand against _cv_to_bool(value), whose DummyUdf return type is Null, so build_cypher_comparison hits NullInvolved and emits a literal null condition — the branch can never match.
  - *Scenario:* Schemaless label (properties stored as LargeBinary CypherValue columns): `CASE n.status WHEN m.status THEN 1 ELSE 0 END` — m.status types as LargeBinary, gets wrapped in _cv_to_bool, the generated WHEN condition becomes literal NULL, and the query always returns the ELSE value (0) even when n.status = m.status. Same for any simple CASE whose WHEN value is a mixed/nested list literal (compiled to a
- **`crates/uni-query-functions/src/df_udfs.rs:4226`** — try_fast_compare's LargeBinary-vs-Int64 branch routes the native Int64 RHS through f64 (`int_arr.value(i) as f64`), and compare_cv_numeric then converts it back with `rhs as i64`, losing precision above 2^53 and yielding wrong equality/ordering for large integers.
  - *Scenario:* A CypherValue-encoded int property equal to 4611686018427387905 compared with an Int64 column holding the same value: 4611686018427387905 as f64 rounds to ...904, so the exact-int comparison in compare_cv_numeric (line 4183) compares 905 vs 904 and `=` returns false (and `<=` returns false) for equal values. This silently reintroduces the exact bug the slow path fixed (expr_eval test_large_integer
- **`crates/uni-query-functions/src/df_udfs.rs:4031`** — In invoke_cypher_string_op's array-vs-array branch, extract_string_at never checks is_null for StringArray/LargeStringArray (only for LargeBinaryArray), so a null string slot decodes as the empty string instead of None, breaking 3-valued logic for STARTS WITH / ENDS WITH / CONTAINS.
  - *Scenario:* MATCH ... WHERE NOT (a.s CONTAINS b.s) with a.s = null in a Utf8 column: value(idx) on the null slot returns "", op("", pattern) evaluates to false, NOT flips it to true, and the row is wrongly included (Cypher requires null CONTAINS x = null, which excludes the row). Similarly `null STARTS WITH ''` returns true instead of null in RETURN projections.
- **`crates/uni-query-functions/src/similar_to.rs:288`** — calculate_score passes the Dot-metric distance through unchanged, but the codebase-wide convention (DistanceMetric::compute_distance) makes that distance -dot, so the returned "similarity" is sign-inverted for Dot indexes.
  - *Scenario:* Create a vector index with metric=dot and run vector KNN/search: StorageManager::vector_search re-scores candidates with compute_distance (= -dot), then vector_knn.rs:636 and search_procedures.rs:941/1145 call calculate_score(distance, Dot) = -dot. A threshold of 0.5 then drops every genuinely similar row (score -dot < 0) via `if similarity < thresh { continue }` while anti-correlated vectors pass

### uni-store

- **`crates/uni-store/src/runtime/l0.rs:1561`** — replay_mutations' SetVertexLabels arm restores vertex_labels but never inserts the vid into vertex_label_overwrites, so a WAL-durable label-only mutation on a prior-window vertex is silently lost at the first post-recovery flush.
  - *Scenario:* SET n:NewLabel on a vertex flushed in a prior window commits (writer appends Mutation::SetVertexLabels, WAL flushed, ack'd), then the process crashes before the next flush. Recovery replays the record: vertex_labels is updated but vertex_label_overwrites stays empty and the vid is not in vertex_properties. The flush's M8 overwrite-only pass (writer.rs ~4730 filters on vertex_label_overwrites) find
- **`crates/uni-store/src/runtime/writer.rs:3231`** — insert_vertices_batch never populates the L0 constraint_index (only Writer::insert_vertex_with_labels at line 2881 does), so unique keys of batch-inserted, not-yet-flushed vertices are invisible to every has_constraint_key check.
  - *Scenario:* insert_vertices_batch inserts a vertex with UNIQUE key k (non-tx or tx). Before the next flush, a single insert_vertex_with_labels (or a transaction commit) with the same key k runs check_unique_constraint_multi / the commit-time overlay probe, which consult only the O(1) constraint_index of current/pending/tx buffers — all empty for the batch rows — and the Lance count_rows check, which sees noth
- **`crates/uni-store/src/runtime/writer.rs:1961`** — validate_vertex_batch_constraints scans only the current L0 and tx L0 for existing unique keys / ext_ids, skipping pending_flush buffers — the exact Bug #9A window the single-vertex paths (check_unique_constraint_multi step 1b, check_extid_globally_unique) were fixed to cover.
  - *Scenario:* Insert vertex with unique key k (or ext_id e); an auto-flush rotates the buffer onto pending_flush while the Lance write is in flight. During that window insert_vertices_batch with the same k/e finds nothing in the (fresh, empty) current L0, nothing in tx L0, and nothing in storage (row not yet written) — the duplicate is accepted and a second vertex with the same unique key / ext_id is created.
- **`crates/uni-store/src/storage/adjacency_manager.rs:381`** — compact() writes Incoming-direction shadow entries keyed by ts.src_vid with neighbor ts.dst_vid, instead of swapping to (dst_vid, src_vid) as the warm() path does for is_incoming, corrupting time-travel reads after compaction.
  - *Scenario:* Edge src->dst is inserted then deleted; compact() runs and moves the overlay tombstone into ShadowCsr for both (etype, Outgoing) and (etype, Incoming) keys, but both calls use add_deleted_edge(ts.src_vid, ShadowEdge{neighbor_vid: ts.dst_vid,..}, direction). ShadowCsr keys entries by (edge_type, direction) -> vid, and get_neighbors_at_version step 4 looks up the queried vid. So a snapshot query get
- **`crates/uni-store/src/storage/arrow_convert.rs:1428`** — PropertyExtractor::build_timestamp_column stores 0 (1970-01-01T00:00:00Z) for a live row that is simply MISSING the property (`is_deleted || val.is_none()` yields Some(0), pushed on the non-deleted branch) instead of NULL, unlike every sibling builder (string/int/float/bool/datetime-struct all append null).
  - *Scenario:* Label declares a Timestamp property `seen_at`; a vertex is created without it and flushed. The column stores epoch 0 rather than NULL, so `RETURN n.seen_at` yields 1970-01-01T00:00:00Z instead of null and `WHERE n.seen_at IS NULL` no longer matches the vertex — silent data corruption of absent values.
- **`crates/uni-store/src/storage/arrow_convert.rs:1512`** — build_date32_column has the same copy-paste defect: a live row missing the Date property gets days=Some(0) and is stored as 1970-01-01 instead of NULL.
  - *Scenario:* Label declares a Date property `birthday`; a vertex created without it flushes with the column set to 1970-01-01. Reads return Date(1970-01-01) instead of null, and IS NULL / range predicates classify the vertex incorrectly (e.g. it matches `birthday < date('2000-01-01')`).
- **`crates/uni-store/src/storage/compaction.rs:257`** — compact_vertices does an unguarded scan -> merge -> replace_table_atomic on the per-label vertex table, silently wiping rows a concurrent flush appends inside the window.
  - *Scenario:* Compaction runs on a background tokio task (StorageManager::trigger_async_compaction) with no flush_lock and no backend.lock_table_for_write; replace_table_atomic (lance.rs:550) issues a plain AddDataMode::Overwrite. Sequence: compaction scans vertices_Person at version N; a flush appends new/updated Person rows committing version N+1; compaction's overwrite commits N+2 containing only the pre-flu
- **`crates/uni-store/src/storage/manager.rs:2582`** — merge_l0_into_vector_results appends every l0_candidates entry without checking the tombstoned set, so a vertex live in an earlier L0 buffer but deleted in a later buffer is resurrected into vector-search results.
  - *Scenario:* Vertex with an embedding is committed (props in the pending-flush L0 or main L0), then deleted in a later buffer in the precedence chain (e.g. delete committed to the new current L0 during an async-flush window, or DETACH DELETE inside an open transaction whose tx-L0 holds the tombstone). The buffer loop records the vid in l0_candidates from the earlier buffer; the later buffer only inserts into `
- **`crates/uni-store/src/storage/manager.rs:2737`** — merge_l0_into_fts_results has the identical copy-paste defect: L0 text candidates from an earlier buffer are appended to FTS results even when a later L0 buffer tombstoned the vid.
  - *Scenario:* Same as the vector case: a vertex whose text property matched in the pending-flush or main L0 is deleted in a later buffer (tx-L0 delete, or delete committed after a flush rotation). `tombstoned` gains the vid but l0_candidates keeps it; results.retain only removes Lance rows; the append loop at line 2737 re-adds the deleted vid, so full-text search returns (or wastes a top-k slot on) a deleted ve

---

## HIGH — unverified (verifier interrupted)

- **`crates/uni-tck/src/matcher/result.rs:261`** — value_sort_key collapses all Nodes to "8:node", Edges to "9:edge", Paths to "A:path", and Lists/Maps/Vectors to length-only keys, so sorting cannot align permuted-but-equal elements and the 'ignoring element order for lists' comparison degenerates into an order-sensitive one for lists of container/graph values.
- **`crates/uni-store/src/runtime/id_allocator.rs:93`** — All four allocate paths advance the in-memory batch reservation (manifest.next_vid_batch/next_eid_batch) before persist_manifest, and a persist failure leaves that advance in place, so a retried allocation silently succeeds without ever durably reserving the batch.
- **`crates/uni-store/src/storage/compaction.rs:511`** — compact_adjacency skips the L2 table replace when the compacted output is empty but still clears the Delta L1 tombstones, resurrecting deleted edges.
- **`crates/uni/src/api/fork_maintenance.rs:181`** — The 'skip if same kind exists' check cannot work because ForkScope::fork_local_indexes is a DashMap keyed on (label, column) holding a single ForkLocalIndexKind, so two index kinds on the same column ping-pong forever and only the last-built kind is visible to the planner.
- **`crates/uni/src/api/transaction.rs:1044`** — commit() wraps the entire commit_transaction_l0 future in tokio::time::timeout, so a CommitTimeout can cancel the commit AFTER its durable point (WAL flush + main-L0 merge), returning a retriable error for a transaction that actually committed.
- **`crates/uni/src/api/transaction.rs:338`** — Transaction::query (query_inner) performs no AuthzPolicy consultation, and Session::run deliberately routes write statements through tx.query — so any write/schema/dbms statement bypasses authorization via these two entry points while tx.execute and all parameterized builders enforce it.

---

## MEDIUM (56 confirmed)


**uni-algo**
- `crates/uni-algo/src/algo/algorithms/astar.rs:154` — A* orders its BinaryHeap by raw f64::to_bits() of the f-score, which is only a valid ordering for non-negative floats; unlike Dijkstra (which rejects negative weights up front) A* has no guard on nega
- `crates/uni-algo/src/algo/algorithms/dijkstra.rs:128` — The max_distance cutoff only skips expansion (`continue`) of over-budget nodes but leaves their already-relaxed distances in `dist`, so the returned SSSP rows include nodes strictly beyond maxDistance
- `crates/uni-algo/src/algo/algorithms/elementary_circuits.rs:153` — Depth-bounded Johnson's algorithm keeps nodes blocked after the search is truncated by max_length: a node explored only via a too-deep path returns found=false and stays in `blocked`, so shorter in-bu

**uni-btic**
- `crates/uni-btic/src/parse.rs:174` — Hour-granularity datetime literals can never parse: chrono's NaiveDateTime::parse_from_str cannot build a datetime from %Y-%m-%dT%H alone (it requires a minute field), so the ("%Y-%m-%dT%H", Granulari

**uni-bulk**
- `crates/uni-bulk/src/bulk.rs:644` — compute_unique_key builds keys via lossy `Value::Display` joined with ':' — ambiguous multi-property joins and colliding renderings (Int 1 vs String "1"; every Bytes value of the same length renders '

**uni-cli**
- `crates/uni-cli/src/demo/semantic_scholar.rs:88` — Paper vertices are inserted with an empty label set — `w.insert_vertex(vid, uni_props, None)` forwards `labels: &[]` through Writer::insert_vertex_with_labels into L0 — so the 'Paper' label the import
- `crates/uni-cli/src/main.rs:139` — The one-shot `uni query` command exits with status 0 even when the query fails, because repl::execute_query swallows every error into a println (to stdout, not stderr) and returns ().

**uni-common**
- `crates/uni-common/src/value.rs:396` — Display for TemporalValue::Date uses unchecked `epoch + chrono::Duration::days(...)`, which panics for out-of-range days_since_epoch, unlike the checked_add_signed used by to_date() a few lines above.

**uni-cypher**
- `crates/uni-cypher/src/grammar/walker.rs:1410` — build_map_literal strips the surrounding quotes from string-literal map keys but never runs unescape_string, so escape sequences and doubled quotes remain literally in the key, unlike every other stri
- `crates/uni-cypher/src/grammar/walker.rs:445` — build_remove_item collects REMOVE label names with raw as_str() instead of normalize_identifier, so backtick-quoted labels keep their backticks — inconsistent with the SET branch (line 387) and node-p

**uni-db**
- `bindings/uni-db/src/builders.rs:2251` — StreamingAppender methods (append line 2243, write_batch line 2293, finish line 2259) and BulkWriter::with_writer callers (insert_vertices line 2499, insert_edges line 2529) lock the std::sync::Mutex 
- `bindings/uni-db/src/convert.rs:345` — Converting a timezone-aware datetime.time calls t.utcoffset(None), but CPython's time.utcoffset() takes no arguments, so every aware time raises TypeError instead of converting.
- `bindings/uni-db/src/convert.rs:300` — Datetime nanos are derived via f64 arithmetic ((timestamp_secs * 1e9) as i64, and fromtimestamp(secs as f64 + micros/1e6) on output), which cannot represent modern epoch-nanosecond values exactly, cor
- `bindings/uni-db/src/sync_api.rs:40` — QueryCursor::next_row holds the buffer and cursor std mutexes across py.detach(block_on(...)), whose GIL re-acquisition on return can deadlock against another Python thread that holds the GIL while bl

**uni-fork**
- `crates/uni-fork/src/diff.rs:43` — compute_diff swallows a one-sided get_vertex_ext_ids() failure with unwrap_or_default(), so when the fetch fails for side `a` but succeeds for side `b`, every ext_id-bearing vertex present unchanged o

**uni-locy**
- `crates/uni-locy/src/compiler/mod.rs:286` — ASSUME body rules are compiled with bare compile(), which drops the outer program's rule names, the neural_predicates_preview flag, and the outer model catalog — while body commands ARE validated agai

**uni-plugin-apoc-core**
- `crates/uni-plugin-apoc-core/src/procedures/number.rs:153` — number.toString accepts Int64 scalars but routes them through extract_f64's `*v as f64` widening, silently corrupting integers with magnitude above 2^53 before formatting.
- `crates/uni-plugin-apoc-core/src/procedures/text.rs:331` — text.repeat's OOM guard caps the repeat count instead of the total synthesized length, so the cap MAX_SYNTHESIZED_LEN (documented in support.rs as bounding 'text.repeat's total length') does not bound

**uni-plugin-builtin**
- `crates/uni-plugin-builtin/src/optimizer/pushdown_negotiation.rs:402` — try_rewrite_topn consults the marker with non-column sort keys silently stripped by sort_exprs_to_marker (possibly an empty list), then treats a Global answer as covering the full Sort and elides it —

**uni-plugin-custom**
- `crates/uni-plugin-custom/src/aggregate.rs:349` — install_aggregate_into_registry adds the qname to uni_cypher's global plugin-aggregate hint set, but dropDeclared never removes it (uni-cypher has no unregister — plugin_aggregates.rs:25 'will follow'
- `crates/uni-plugin-custom/src/eval.rs:200` — arith() routes all Int/Int arithmetic through f64: integer division returns a Float (Cypher truncates int/int), i64 values beyond 2^53 silently lose precision in Sub/Mul/Div/Mod, and the `out as i64` 
- `crates/uni-plugin-custom/src/lib.rs:1292` — For trigger declarations, the declare_kind_procedure! macro sets DeclaredPlugin.body to the arg at position 1, which for declareTrigger is `event_filter`, not the Cypher body at position 2 — so the sy
- `crates/uni-plugin-custom/src/scalar.rs:94` — DeclaredScalarFn::invoke forces row_count = rows.max(1), violating the ScalarPluginFn contract ('produce exactly rows values') for 0-row invocations: it fabricates one output row from out-of-range (Nu

**uni-plugin-host**
- `crates/uni-plugin-host/src/scheduler.rs:148` — SchedulerControl::cancel (and Uni::periodic_cancel via scheduler().cancel) only cancels the in-memory job and never calls self.persistence.cancel(id), so the persisted sidecar row survives and the can
- `crates/uni-plugin-host/src/scheduler.rs:145` — SchedulerHost::add_scheduled_job upserts the sidecar row idempotently but delegates to the primitive Scheduler::add_scheduled_job, which unconditionally pushes a new record — re-registering an existin
- `crates/uni-plugin-host/src/triggers.rs:559` — dispatch_before enqueues TriggerOutcome::Defer into the DeferralQueue before the transaction commits, so the deferred trigger later fires with mutation events from a transaction that may abort — viola

**uni-plugin-pyo3**
- `crates/uni-plugin-pyo3/src/loader.rs:614` — db.set_determinism(...) is a silent no-op: it writes PyManifest.determinism, which no code ever reads — finalize/register_* use only per-entry determinism, whose decorator default is "pure" (Volatilit

**uni-plugin-rhai**
- `crates/uni-plugin-rhai/src/adapter_aggregate.rs:291` — evaluate() converts the finalize result via value-directed dynamic_to_scalar_loose, ignoring the declared signature.returns type — unit maps to untyped ScalarValue::Null and Rhai INT maps to Int64 eve
- `crates/uni-plugin-rhai/src/host_fn_impls/kms.rs:99` — from_hex slices the signature string at fixed byte offsets (&s[i..i+2]) without char-boundary checks, so a multi-byte UTF-8 character in the hex string panics the host thread instead of returning a de
- `crates/uni-plugin-rhai/src/loader.rs:294` — Loader-built procedure yield fields are named col0..colN, but dynamic_to_record_batch (adapter_procedure.rs:122) silently substitutes NULL for any row-map key not matching those fabricated names, so e

**uni-query**
- `crates/uni-query/src/projection_store.rs:180` — The process-global projection registry is keyed on the raw address of the schema-manager Arc (`Arc::as_ptr as usize`) without holding the Arc alive or ever evicting entries, so a later database instan
- `crates/uni-query/src/query/df_graph/expr_compiler.rs:2656` — resolve_metric_for_property looks up the vector-index DistanceMetric by property name only, ignoring the label, so the first index in schema.indexes wins across labels.
- `crates/uni-query/src/query/df_graph/locy_abduce.rs:235` — The target_var fix-up pass in extract_edge_candidates (and its copy in extract_addition_candidates at line 281) mutates candidates.last_mut() instead of the candidate for the relationship just travers
- `crates/uni-query/src/query/df_graph/locy_eval.rs:371` — eval_binary_op's integer Div and Mod paths (and eval_locy_binary_op's Mod at line 334) have no zero-divisor guard, so Int/Int division or modulo by zero panics instead of returning an error.
- `crates/uni-query/src/query/df_graph/locy_eval.rs:561` — value_less_than has no arm for Temporal (or Bool) values and returns false for every such comparison, so <, >, <=, >= and MIN/MAX over dates/datetimes silently produce wrong results in the Locy in-mem
- `crates/uni-query/src/query/df_graph/locy_fixpoint.rs:2537` — apply_exact_wmc overwrites the PROB column and groups shared-lineage keys by raw yield-schema positions (prob_fold.input_col_index, rule.key_column_indices) against post-fixpoint fact batches whose sc
- `crates/uni-query/src/query/df_graph/recursive_cte.rs:260` — Recursive-CTE cycle detection keys rows by `format!("{val:?}")`, but multi-column rows are Value::Map(HashMap) whose Debug order is instance-dependent, so already-seen rows are never recognized and th
- `crates/uni-query/src/query/df_graph/search_procedures.rs:182` — parse_reranker_options computes reranker_k via `(v as usize).clamp(k, 1000)`, which panics (Ord::clamp asserts min <= max) whenever the user-supplied k exceeds 1000.
- `crates/uni-query/src/query/df_graph/vid_lookup_join.rs:561` — values_equal compares non-anchor equi-pair cells with ScalarValue PartialEq, under which NULL == NULL is true, so rows join on NULL keys — contradicting Cypher semantics and the HashJoinExec fallback 
- `crates/uni-query/src/query/df_planner.rs:2848` — hydrate_virtual_target_from_catalog always uses an Inner HashJoin on {target}._vid, discarding the NULL-target rows that an optional traverse emits, breaking OPTIONAL MATCH semantics for virtual targe
- `crates/uni-query/src/query/executor/read.rs:5066` — execute_union's UNION (non-ALL) dedup keys rows on Debug formatting of values, but Value derives Debug and nested Value::Map/Node contain HashMaps whose iteration order differs per HashMap instance (s
- `crates/uni-query/src/query/executor/write.rs:1048` — arrow_value_to_json silently returns Value::Null for every Arrow type it doesn't handle (Timestamp, Date32/64, LargeUtf8, Utf8View, lists, decimals), and its callers skip null values, so COPY FROM sil
- `crates/uni-query/src/query/planner.rs:4376` — plan_shortest_path only reads elements[0..3] and returns after planning the first hop, silently ignoring all further relationship/node elements even though the length check (`len >= 3 && odd`) admits 

**uni-query-functions**
- `crates/uni-query-functions/src/datetime.rs:1826` — format_timezone_offset loses the sign for negative offsets smaller than one hour: `hours = offset_secs / 3600` truncates -1800 to 0, and `{:+03}` formats 0 as "+00", so -00:30 renders as "+00:30".
- `crates/uni-query-functions/src/df_expr.rs:1374` — The regex operator `=~` is translated as regexp_match(left, right).is_not_null(), which collapses NULL inputs to false instead of propagating null per Cypher three-valued semantics.
- `crates/uni-query-functions/src/df_udfs.rs:1433` — RangeUdf advances with unchecked `current += step`, unlike the interpreted eval_range_function which was explicitly fixed to use checked_add, so ranges ending at/near i64::MAX overflow.
- `crates/uni-query-functions/src/df_udfs.rs:6959` — CypherSumAccumulator accumulates integers with wrapping_add (also at lines 6977 and 7022), so SUM over integers silently wraps around i64 overflow and reports a garbage integer result instead of error
- `crates/uni-query-functions/src/expr_eval.rs:1363` — eval_sign maps Float via `f.signum() as i64`, but Rust's f64::signum returns 1.0 for +0.0 (and -1.0 for -0.0), so sign(0.0) returns 1 instead of the Cypher-required 0.
- `crates/uni-query-functions/src/expr_eval.rs:1139` — eval_size (and eval_length at line 1195) return the byte length `s.len()` for strings, while the DataFusion path's cypher_size_scalar uses `s.chars().count()`, so size()/length() on non-ASCII strings 
- `crates/uni-query-functions/src/rewrite/function_rename.rs:146` — The function-rename walker's catch-all `other => other` skips Clause::Merge (pattern property maps + ON MATCH/ON CREATE SetItems) and Clause::WithRecursive (nested Box<Query>), so plugin ReplacementSc

**uni-store**
- `crates/uni-store/src/fork/registry.rs:171` — ForkRegistryHandle::load treats every GET failure — not just NotFound — as 'registry never created' and starts with an empty registry, which the next PUT persists, orphaning all existing forks.
- `crates/uni-store/src/runtime/property_manager.rs:506` — get_batch_vertex_props projects `_version` but never reads it: rows are applied in scan order with full-map `result.insert` overwrite and unconditional `result.remove` on `_deleted`, so results depend
- `crates/uni-store/src/snapshot/manager.rs:178` — load_named_snapshots maps every error — including transient store failures — to an empty map, so the read-modify-write in save_named_snapshot can silently wipe all previously named snapshots.
- `crates/uni-store/src/storage/manager.rs:1295` — scan_vertex_table (and scan_delta_table line 1365, scan_main_vertex_table line 1428, vector_search line 1797, fts_search line 2142) maps a table_exists() error to 'table absent' via .unwrap_or(false),

**uni-tck**
- `crates/uni-tck/src/steps/and.rs:17` — The 'no side effects' step only compares net node/edge counts and label sets; it never checks the gross counters (nodes_created/deleted, edges_created/deleted) or any property counters (properties_add
- `crates/uni-tck/src/steps/and.rs:45` — 'the side effects should be:' only asserts the counters listed in the scenario table and never verifies that unlisted counters are zero, contrary to TCK semantics where absent entries mean 0.

---

## MEDIUM/LOW — unverified (verifier interrupted)

- [LOW] `crates/uni-tck/src/world.rs:320` — collect_ids and collect_property_snapshot swallow query errors via `if let Ok(...)`, returning an empty set/map instead of propagating the failure, which corrupts all side-effect diffs computed from t
- [MEDIUM] `crates/uni-store/src/runtime/property_manager.rs:735` — get_batch_edge_props is the edge sibling of the version-ignoring batch read: `_version` is projected but never read, each storage row fully replaces the previous props via result.insert, deletes remov
- [MEDIUM] `crates/uni-store/src/runtime/property_manager.rs:572` — overlay_l0_batch removes a vid on any L0 tombstone unconditionally, while property overlays five lines below are gated on version_high_water_mark (`entry_version > hwm` → skip) — so a version-pinned r
- [MEDIUM] `crates/uni-store/src/storage/main_vertex.rs:637` — find_batch_props_by_vids (and find_batch_labels_by_vids at line 793) filter `_deleted = false` and take last-row-wins with no `_version` ranking — the exact MVCC bug (review C2) fixed in every single-
- [MEDIUM] `crates/uni/src/api/schema.rs:207` — SchemaBuilder::apply swallows the 'already exists' error for AddEdgeType, silently ignoring a re-declaration whose from_labels/to_labels differ from the stored definition.
- [MEDIUM] `crates/uni/src/api/fork.rs:452` — Nested-fork branch creation reads the parent branch's live tip via current_version_on_branch AFTER flush_and_capture_fork_point released the flush_lock, re-opening exactly the capture/branch race the 
- [MEDIUM] `crates/uni/src/api/rule_registry.rs:129` — RuleRegistry::remove snapshots sources under a read lock, rebuilds outside any lock, then clobbers the registry with `*write() = rebuilt`, losing any rule program registered concurrently (register_rul
- [MEDIUM] `crates/uni/src/api/transaction.rs:1096` — Commit-time rule promotion copies only the rules map into the session registry, omitting sources and strata, breaking the 'registry is a pure function of sources' invariant — promoted rules are silent
- [MEDIUM] `crates/uni/src/api/impl_query.rs:756` — execute_internal_with_config_and_token builds its QueryPlanner without .with_plugin_registry() (the same omission exists in execute_cursor_internal_with_tx_l0 at line 650), so plugin catalog/virtual-l
- [LOW] `crates/uni/src/api/impl_query.rs:246` — extract_projection_order's Aggregate arm emits group_by expressions followed by aggregates, discarding the RETURN clause's interleaved column order; since aggregate plans are not wrapped in a Project,
- [LOW] `crates/uni/src/api/impl_locy.rs:509` — LocyEngine::evaluate_with_config_capturing compiles via compile_only, which never forwards the LocyConfig to the compiler — so config.neural_predicates_preview is ignored on the transaction path, unli

---

## LOW (37 confirmed)


**uni-algo**
- `crates/uni-algo/src/algo/algorithms/apsp.rs:51` — The weighted APSP branch filters results with `dist.is_finite() && dist > 0.0`, intending to skip the source itself, but this also drops any target genuinely reachable at total wei
- `crates/uni-algo/src/algo/algorithms/betweenness.rs:130` — When sampling_size < n, the per-source Brandes contributions are summed over only k sampled sources but never rescaled by n/k (the standard Brandes-Pich estimator), and normalizati

**uni-btic**
- `crates/uni-btic/src/btic.rs:152` — duration_ms computes hi - lo with unchecked i64 subtraction; near-sentinel bounds that satisfy every invariant overflow, panicking in debug builds and returning a wrapped negative 

**uni-bulk**
- `crates/uni-bulk/src/bulk.rs:1116` — If both defer_vector_indexes and defer_scalar_indexes are set to false, commit() skips index rebuild entirely, yet no flush-path code builds user indexes either (flush only calls e
- `crates/uni-bulk/src/bulk.rs:686` — CHECK-expression '='/'!=' use Value's type-strict PartialEq (Int(5) != Float(5.0)) while '<'/'>'/'>='/'<=' coerce Int/Float via compare_values, so numeric equality checks falsely f

**uni-cli**
- `crates/uni-cli/src/demo/semantic_scholar.rs:168` — The per-1000-record progress indicator uses print!("\r...") without flushing stdout, so on a line-buffered terminal nothing is displayed until the loop finishes.

**uni-common**
- `crates/uni-common/src/value.rs:789` — Value's Eq is not reflexive for Vector/SparseVector containing NaN: these arms compare Vec<f32> with IEEE-754 == (NaN != NaN), even though Value implements Eq and the Float arm was

**uni-crdt**
- `crates/uni-crdt/src/lww_map.rs:34` — LWWMap's empty-register sentinel timestamp of -1 is not reserved: user timestamps <= -1 are accepted (i64), so a put with timestamp < -1 on a missing key is silently dropped, and a

**uni-cypher**
- `crates/uni-cypher/src/grammar/locy_walker.rs:1359` — build_derive_node_spec (and build_derive_edge_spec at line 1386) push label/edge-type names with raw as_str() instead of normalize_identifier, so backtick-quoted names in DERIVE he

**uni-db**
- `bindings/uni-db/src/core.rs:317` — xervo_generate_core silently maps any unknown message role to Message::user, so a typo'd or unsupported role (e.g. "System", "tool") is reinterpreted as a user message instead of e
- `bindings/uni-db/src/types.rs:1877` — PyPreparedLocy::__repr__ truncates the program text by byte index (&t[..60]) instead of char count, panicking when byte 60 is not a UTF-8 character boundary (sibling reprs like PyR

**uni-locy**
- `crates/uni-locy/src/compiler/dependency.rs:148` — PathContextWalker::walk_expr does not recurse into Expr::List or Expr::Map, while InvocationLifter::lift_expr does lift model calls from those shapes, so a path-context model invok
- `crates/uni-locy/src/compiler/typecheck.rs:696` — check_model_invocations only visits WHERE, FOLD, and YIELD expressions — ALONG and HAVING positions are skipped — but InvocationLifter::lift_expr relies on it ('Arity already valid

**uni-locy-tck**
- `crates/uni-locy-tck/src/steps/given.rs:35` — The 'having executed:' Given step silently becomes a no-op when the step has no docstring, skipping graph setup instead of failing, unlike the sibling when_parse/when_compile/when_

**uni-plugin**
- `crates/uni-plugin/src/manifest.rs:68` — AbiRange::matches probes with minor/patch = u64::MAX/2, so any requirement with an upper bound on minor/patch ("~1.2", "=1.2.3", ">=1.2, <1.6") reports the host major as unsupporte
- `crates/uni-plugin/src/registry.rs:902` — apply_pending's preflight only checks each pending registration against the live registry, never against the rest of the batch, so duplicate names within one register() call bypass
- `crates/uni-plugin/src/scheduler.rs:238` — tick_at treats next_fire_at == None as immediately due, so a Cron job whose expression fails to parse (Schedule::next_after returns None) is dispatched and executed once instead of

**uni-plugin-apoc-core**
- `crates/uni-plugin-apoc-core/src/procedures/math.rs:164` — math.round computes the scale as 10f64.powi(precision as i32): precision > 308 overflows scale to +inf making the result NaN, and the i64→i32 `as` cast wraps for |precision| >= 2^3

**uni-plugin-builtin**
- `crates/uni-plugin-builtin/src/locy_aggregates.rs:891` — MprodState::merge computes `self.product *= o.product * o.log_sum.exp()` when o.use_log — double-counting the pre-switch product (log_sum already contains ln(product) from the swit

**uni-plugin-custom**
- `crates/uni-plugin-custom/src/eval.rs:164` — apply_binary short-circuits any Null operand to Null before dispatching, breaking Cypher three-valued logic for AND/OR: `null AND false` must be false and `null OR true` must be tr
- `crates/uni-plugin-custom/src/lib.rs:1511` — DeclaredPluginStore::declare validates dependencies and cycles under a read lock, releases it, then inserts under a separate write lock — a check-then-act race that lets concurrent

**uni-plugin-extism**
- `crates/uni-plugin-extism/src/adapter_aggregate.rs:325` — build_returns_field uses argtype_to_arrow, which maps ArgType::Vector to its element type, while the DataFusion bridge (df_udaf_plugin.rs arg_type_to_arrow) declares Vector returns

**uni-plugin-host**
- `crates/uni-plugin-host/src/triggers.rs:835` — PreExistingProbe::from_l0_chain treats a vertex/edge tombstone as 'skip this buffer' instead of 'entity is dead', so older L0 buffers (iterated oldest-first from pending_flush) or 
- `crates/uni-plugin-host/src/triggers.rs:1555` — Persisted deferral rows are re-bound to trigger plugins by subscription_name — the first line of the subscription's docs — using find(), so two triggers with identical (or empty) d

**uni-plugin-rhai**
- `crates/uni-plugin-rhai/src/adapter_aggregate.rs:245` — Aggregate partial state is serialized with serde_json, which encodes non-finite floats (NaN/Inf) as JSON null, so peer-state NaN/Inf silently becomes Dynamic::UNIT after decode_sta
- `crates/uni-plugin-rhai/src/adapter_procedure.rs:146` — coerce_for coerces a Rhai float into an Int64-declared yield column with a bare `as i64` cast, so NaN silently becomes 0 and out-of-range/fractional values silently saturate/trunca

**uni-query**
- `crates/uni-query/src/procedures_plugin/schema.rs:467` — uni.schema.labelInfo's indexed check for JsonFullText indexes tests only the label (j.label == label_name) and never compares the property to j.column, so every property of a label
- `crates/uni-query/src/query/df_graph/locy_fixpoint.rs:5169` — The TopKProofs body_support_map built in apply_post_fixpoint_chain reconstructs each fact's IS-ref support from the DerivedScanRegistry, but after convergence a rule's self-ref reg
- `crates/uni-query/src/query/df_graph/search_procedures.rs:1578` — run_hybrid_search swallows auto_embed_text errors with `.unwrap_or_default()`, silently dropping the dense-vector arm of the hybrid search.
- `crates/uni-query/src/query/df_graph/traverse.rs:1152` — is_optional_column_for_vars classifies an internal edge-id column as belonging to an OPTIONAL variable via a suffix match (col_name.starts_with("__eid_to_") && col_name.ends_with(v
- `crates/uni-query/src/query/df_graph/traverse.rs:2049` — GraphTraverseMainStream::expand_batch has no per-source edge dedup for Direction::Both, while build_edge_adjacency_map's Both arm inserts a self-loop edge twice under the same sour
- `crates/uni-query/src/query/executor/core.rs:138` — Accumulator::Sum accumulates all values in f64, so integer sums whose magnitude exceeds 2^53 lose precision and finish() returns a wrong Value::Int.

**uni-query-functions**
- `crates/uni-query-functions/src/datetime.rs:877` — epochSeconds/epochMillis accessors on Value::Temporal use truncating division (`nanos_since_epoch / 1_000_000_000`) instead of floor, giving off-by-one results for pre-1970 instant
- `crates/uni-query-functions/src/df_udfs.rs:2956` — encode_sort_key_to_buf encodes Value::Int by casting to f64 (`let f = *i as f64`) for the ORDER BY sort key, collapsing distinct i64 values above 2^53 into identical keys.
- `crates/uni-query-functions/src/similar_to.rs:378` — value_to_sparse converts map indices with `as_i64().map(|i| i as u32)`, silently wrapping negative or >u32::MAX indices to unrelated term ids instead of erroring.
- `crates/uni-query-functions/src/spatial.rs:189` — point.withinBBox uses `(min_lon..=max_lon).contains(&lon)`, so a geographic bounding box crossing the antimeridian (lowerLeft.longitude > upperRight.longitude) is an empty range an
- `crates/uni-query-functions/src/spatial.rs:60` — eval_point silently ignores a present-but-non-numeric `z` via `map.get("z").and_then(|v| v.as_f64())`, returning a 2-D Cartesian point instead of erroring like non-numeric x/y do.