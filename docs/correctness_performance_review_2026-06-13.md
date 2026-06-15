# Correctness & Performance Review — 2026-06-13

Workspace-wide review by 10 parallel specialist agents covering ~330K LoC across 27 crates.
Each finding was verified against source; agent-reported false alarms are listed at the end of each
section and excluded from the ranking. Two "critical" leads were disproven by **running the
regression tests** (PageRank dangling-mass and Louvain modularity — stale `// RED today` comments).

Severity = blast radius × likelihood. `file:line` references are exact at time of review.

---

## CRITICAL — data loss, silently wrong results, or security bypass

### C1. Backend `scan` swallows all stream-creation errors into `Ok(vec![])` → MERGE creates duplicates
`crates/uni-store/src/backend/lance.rs:394-398`. Transient I/O / corrupt-fragment / bad-filter errors
all return an empty result instead of propagating. This directly defeats the documented fail-closed
contract in `manager.rs:1296-1428` ("a scan error must propagate... the MERGE fast path would create
a duplicate node on a transient failure"). The mid-stream collect error *is* propagated, so the
behavior is also internally inconsistent. **Fix:** only map genuine table-not-found to empty;
propagate everything else.

### C2. Deleted edges/vertices resurrect — `_deleted = false` SQL filter + first-row/no-version selection
`main_edge.rs:384` (`find_props_by_eid`), `:439` (`find_type_by_eid`), `main_vertex.rs:416`
(`find_by_ext_id`/`ext_id_exists`). Main tables are append-only; a delete appends a tombstone at a
higher `_version`. Filtering `_deleted = false` in SQL **excludes the winning tombstone** and returns
the older live row. `find_type_by_eid`/`find_by_ext_id` additionally take `value(0)` of the first
batch with no `_version` comparison (Lance scan order is unordered). `find_props_by_vid` already does
this correctly (scans all versions) — the edge and ext-id paths don't. **Fix:** drop the SQL
`_deleted` predicate, pick max `_version`, return None if the winner is a tombstone.

### C3. `UidIndex` is append-only with no tombstone path + `limit(1)` non-deterministic resolution
`crates/uni-store/src/storage/index.rs:74-82, 106-199`. `write_mapping` is always `Append`, and the
delete path never removes from this index. After delete/recreate, multiple rows share `_uid_hex`;
`get_vid` does `.limit(Some(1))` with no version column to order by → resolves UIDs to dead/wrong vids
non-deterministically. `resolve_uids` has the same flaw (last-writer-by-scan-order). **Fix:** add a
version column + select MAX per uid, emit tombstones on vertex delete.

### C4. Snapshot manifest + `catalog/latest` pointer are not fsync'd, but the WAL is truncated after publishing them
`crates/uni-store/src/snapshot/manager.rs:37-43, 94-104`; consumed at `writer.rs:4369-4417`.
`save_snapshot`/`set_latest_snapshot` write through `put_with_timeout`, which on `LocalFileSystem`
returns after buffering to the page cache **with no fsync** (the WAL has a dedicated fsync path; the
snapshot manager has none). `flush_finalize_body` publishes the manifest/pointer in step H (no fsync)
then truncates the WAL in step K. A power loss after the WAL deletes reach disk but before the
page-cached manifest/pointer are flushed loses **both** copies of committed data → unrecoverable.
**Fix:** fsync the manifest file + parent dir, then the `latest` file + parent dir, *before* the WAL
truncation in step K (reuse `wal::sync_file_and_parent` via the already-plumbed `local_fs_root`).

### C5. Schemaless edge-type minting doesn't bump schema version → stale cached plan silently drops edges
`uni-common/src/core/schema.rs` `get_or_assign_edge_type_id` (mints a new edge-type id without
`bump_version()`) × plan cache keyed on `schema_version` (`uni/src/api/session.rs:~1780`) ×
`all_edge_type_ids()` baked into cached plans (`planner.rs:4411/4931/5146`,
`pattern_comprehension.rs:887`). After `MATCH ()-[r]->() RETURN r` is cached, a later schemaless
`CREATE ()-[:NEWTYPE]->()` mints an id without invalidating the plan, so the cached untyped scan
**misses all NEWTYPE edges** until unrelated DDL bumps the version. **Fix:** call `bump_version()` on
the mint branch.

### C6. Plugin signature verification is never invoked by any dynamic loader (unsigned code executes)
`crates/uni/src/api/mod.rs:2137-2330`. `load_wasm_extism`, `load_wasm_component`, `load_rhai_plugin`,
`load_python_plugin` all go through `with_loading_registrar` and call `loader.load(...)` directly. The
only non-test callers of `plugin_trust.enforce()` are `add_plugin` (the native-Rust path that carries
no foreign code) and the built-in self-check. **Even with `SignaturePolicy::RequireSigned`,
unsigned/attacker-supplied WASM/Extism/Rhai/Python loads and executes.** Compounded by **C6b**
(`crates/uni-plugin-wasm/src/loader.rs:41-71`, `uni-plugin-extism/src/loader.rs:22-57`): the wire
manifest types have no `signature`/`hash` fields and use `deny_unknown_fields`, so a signed plugin
can't even parse. **Fix:** add `hash`/`signature` to the wire manifests (or a sidecar `.sig`) and call
`plugin_trust.enforce()` + hash-pin verification against the exact bytes in each `load_*` before
build/register. (Tracked as an open item in project memory — still unwired.)

---

## HIGH — incorrect results or resource exhaustion under realistic conditions

### H1. SSI read-set omits label / existence / edge-endpoint reads → write-skew & lost updates
`crates/uni-store/src/runtime/l0_visibility.rs` — only `lookup_vertex_prop`/`lookup_edge_prop`/
`accumulate_*_props` record reads. The item-level helpers serving **label reads** (`get_vertex_labels`
~454), **existence checks** (`is_vertex_deleted` ~25, `vertex_exists_in_l0` ~418), and
**`get_edge_endpoints`** (~553) record nothing. A transaction whose read-modify-write decision depends
on a label (`MATCH (n) WHERE n:Active SET ...`) or on edge endpoints reached off the scan/traverse
path can have that observation elided from the antidependency check → a concurrent mutation commits
without a `SerializationConflict`. Genuine SSI soundness hole. **Fix:** route label/endpoint/existence
reads consulted under a write-tx through `record_vertex_read`/`record_edge_read`, or require
FOR UPDATE for label-predicated RMW and document it.

### H2. Async-flush finalizer permanently wedges if a stream task panics
`crates/uni-store/src/runtime/flush_coordinator.rs:314-317, 370-409`. `submit_for_stream` only calls
`coord.submit(seq, …)` *after* `run_stream(...).await` returns. If the stream future panics or is
cancelled, `submit` is never called for that `seq`; the finalizer finalizes strictly in consecutive
order, so the missing seq blocks every later flush forever and `drain()`/`shutdown()` hang. **Fix:**
wrap the stream body in `AssertUnwindSafe(...).catch_unwind()` and submit an `Err(FlushOutcome)` so
`finalize_failure` advances `expected`.

### H3. WAL fsync-failure leaves bytes on disk but reports commit failure → ghost commit on recovery
`crates/uni-store/src/runtime/wal.rs:331-346`. On fsync failure after a successful segment PUT, the
code returns `Err` without deleting the just-written segment or rolling back `flushed_lsn`. The caller
treats the tx as aborted, but a later crash + `replay_since` finds that segment as valid and replays a
transaction the caller was told failed. **Fix:** best-effort delete the segment on fsync failure (clean
abort), or treat post-PUT fsync failure as fatal.

### H4. Variable-length-path edge-property conditions are bypassed for flushed edges → over-matching
`crates/uni-query/src/query/df_graph/traverse.rs:3004-3024, 3250-3252`. For a VLP like
`[r:KNOWS*1..3 {year:1988}]`, the inline edge-property filter is evaluated only against L0 in-memory
edges; for flushed (CSR/Lance) edges the code sets `passes = true` and defers to an `EidFilter` that is
hardcoded `AllAllowed` (a `TODO(Phase 3.5)`). **Edge property predicates on variable-length
relationships do not filter flushed edges** → returns paths through edges that fail the condition.
Single-hop and schemaless paths build the filter correctly; only typed VLP is affected. **Fix:** build
the `EidFilter` from a property pre-scan during warming, or fall back to per-edge property fetch.

### H5. `range()` in UNWIND overflows / can infinite-loop and ignores float bounds
`crates/uni-query/src/query/df_graph/unwind.rs:382-392`. `i += st` is unchecked: near `i64::MAX` it
panics (debug) / wraps (release), and on wrap with a positive step `i <= e` can stay true forever.
`range(1.0, 3.0)` silently yields `[]` (floats return `None` from `as_i64`) where openCypher requires
an error. **Fix:** `checked_add` + break on overflow; reject step 0; error on non-integer args.

### H6. `size()`/`length()` on a String returns byte length, not character count
`unwind.rs:422-431` (`Value::String(s) => s.len()`). openCypher `size('héllo')` must be 5; this
returns 6. **Fix:** `s.chars().count()`.

### H7. Correlated-subquery dedup cache keyed by a bare 64-bit hash of `format!("{:?}")` → wrong rows
`crates/uni-query/src/query/df_graph/apply.rs:563-572, 806-825`. `hash_row_params` hashes
`format!("{val:?}")` of each param, and `subplan_cache` is keyed by the `u64` hash **with no equality
re-check**. Any `DefaultHasher` collision (or two values whose Debug renders identically) returns
another row's subquery results. **Fix:** key the cache by the owned, sorted `Vec<(String,Value)>` and
compare on equality; implement `Hash` on `Value` directly.

### H8. Bulk UNIQUE validation misses already-flushed rows → silent duplicate inserts
`crates/uni-bulk/src/bulk.rs:524-542`. The UNIQUE check compares only against the incoming batch and
the in-memory `pending_vertices` buffer, which `flush_vertices_buffer` drains on every flush. Any bulk
load larger than `batch_size` (default 10K) can admit cross-batch duplicates that violate the
constraint. **Fix:** probe the persisted per-label dataset / UID index, or keep a writer-lifetime
`HashSet` of seen keys.

### H9. Bulk dual-write (per-label + main table) is not atomic
`crates/uni-bulk/src/bulk.rs:764-808, 914-1039`. Each flush writes the per-label table then the main
table as independent Lance commits. A crash/panic between them leaves the tables permanently divergent
(entity in one, not the other), with no reconciliation on reopen; `abort()` only helps if reached.
**Fix:** fold into one commit, or make the main table authoritative-last with documented recovery.

### H10. ORSet tombstones grow unbounded (memory leak + payload bloat)
`crates/uni-crdt/src/orset.rs:46-50, 88-99`. `tombstones` is a monotonically growing set with no GC;
churny long-lived registers leak indefinitely and bloat every serialized payload, and `len()`/
`elements()` keep scanning dead tags. **Fix:** causal-context (version-vector) GC to drop
fully-tombstoned tags; prune empty entries.

### H11. Compaction delta-clear loses concurrently-flushed deltas
`crates/uni-store/src/storage/compaction.rs:561-583`. Deltas are read early (~line 361), then after
merge work the whole delta table is wiped via unconditional empty-batch replace, gated only by an
instantaneous `flush_in_progress > 0` check. A flush that starts *and finishes* inside the read→clear
window has its rows wiped → permanently lost topology. **Fix:** capture a high-water mark at read time
and predicate-delete only `_version <= hwm`.

### H12. Adjacency `compact()` has no mutual exclusion and `clear()`s all frozen segments
`crates/uni-store/src/storage/adjacency_manager.rs:334-345, 462`. Step 2 snapshots `frozen_segments`
but Step 5 `clear()`s them, wiping any segment a concurrent compact/freeze pushed after the snapshot.
**Fix:** serialize compact under a mutex; `drain` exactly the snapshotted segments instead of `clear()`.

### H13. Silent integer truncation in column builders and CSR
`arrow_convert.rs:1135` (`i64→i32` wrap, `build_int32_column`), `:1264` (date32), `csr.rs:142`
(`neighbor.as_u64() as u32` truncates 64-bit VIDs ≥ 2³²). All silently corrupt out-of-range values
instead of erroring/nulling. **Fix:** `i32::try_from(...).ok()`; store full-width VIDs or enforce the
domain.

### H14. Plugin HTTP egress follows redirects past the network allow-list (SSRF)
`crates/uni-plugin-host/src/http_egress.rs:94`. The client is built with no `.redirect(...)`, so
reqwest follows up to 10 redirects; the allow-list is checked only on the initial URL. A 302 to
`http://169.254.169.254/...` or an internal host is followed unchecked. **Fix:** `Policy::none()` +
re-validate each `Location` hop; block RFC1918/loopback/link-local resolved IPs.

### H15. Extism plugins get no timeout/memory/fuel cap unless the (untrusted) manifest opts in
`crates/uni-plugin-extism/src/loader.rs:546, 568-572`. Limits apply only when declared (`if let
Some(...)`), letting the untrusted party opt out of its own sandbox. The component loader correctly
**floors** via `unwrap_or(DEFAULT_*)`. **Fix:** apply host defaults unconditionally; clamp declared
values to `min(declared, host_ceiling)`.

### H16. Graph-algorithm correctness: weighted APSP, nondeterminism, Louvain local-move
- `algorithms/apsp.rs:40` — APSP calls `bfs_levels` unconditionally, returning **hop count on weighted
  graphs** (result type is even `u32`). **Fix:** branch on `has_weights()` → Dijkstra-per-source, widen
  to `f64`.
- `algorithms/label_propagation.rs:78,84,128` and `random_walk.rs:65,78,85` — unseeded `rand::rng()`
  makes results **nondeterministic** run-to-run; RandomWalk also ignores its node2vec p/q params.
  **Fix:** add a seed, deterministic tie-breaks.
- `algorithms/louvain.rs:111-148` — local-move gain is seeded at `0.0` and never scores the current
  community as a baseline, so moves that *reduce* modularity can be taken. (`compute_modularity` itself
  is **correct** — verified by running `test_louvain_modularity_scaling`.) **Fix:** compute
  remove-then-best-including-original gain.

---

## MEDIUM — narrower blast radius, correctness footguns, or notable perf

- **Apply correlated `WHERE` collapses NULL/UNKNOWN to false** — `apply.rs:275-352`. Lost three-valued
  logic; `null <> 1` returns `true`, `NOT (p.x = null)` keeps the row. **Fix:** tri-valued
  (`Option<bool>`) Kleene evaluation.
- **`FOR UPDATE` lock key built from `serde_json::to_vec(Value)`** — `uni/src/api/for_update.rs:115`.
  Non-canonical floats (`1.0` vs `1`) and temporal-as-string can make two RMWs on the same row take
  *different* locks → lost update. It's also a predicate-lock, never resolved to a VID. **Fix:** use the
  store's canonical key encoding.
- **`classify_verb` is a 32-char prefix match → authz bypass** — `transaction.rs:167-194`. A leading
  comment (`/* c */ CREATE INDEX`) or long prefix misclassifies schema/DDL as a plain write, bypassing
  an AuthzPolicy that allows writes but denies DDL. **Fix:** classify from the parsed AST.
- **Pattern-comprehension builds a fresh Tokio runtime per CSR-warm and per property batch, per
  `evaluate()`** — `pattern_comprehension.rs:234-254, 314-384`. Runtime construction in the per-batch
  hot path. **Fix:** reuse a shared runtime handle / `Handle::current()`.
- **Single-hop traversal acquires the L0 read lock + manifest-hwm read per (vid × edge_type)** —
  `traverse.rs:673-674`, `mod.rs:580`, while a `get_neighbors_batch` that amortizes the lock exists and
  is unused. Same per-row lock pattern in `build_all_props_column` (`traverse.rs:829-844`) and
  schemaless label sync (`:1395-1413`). **Fix:** hoist guards / route through the batch API.
- **`AddMultProb::times` floors both operands at ε before log-add** — `uni-locy/src/semiring.rs:209-218`.
  `times(1e-20, 1e-20)` returns 1e-30 instead of 1e-40 (10 orders off); the flooring is unnecessary
  since 1e-40 doesn't underflow f64. MPROD folds compound the error. **Fix:** take logs of the actual
  values, guard only the genuine `0.0` case.
- **Locy provenance/lineage keys via `format!("{:?}", Vec<ScalarKey>)`** — `locy_fixpoint.rs:1578-1582,
  2053-2057, 2139-2141`. Debug-string collisions corrupt base-fact identity → wrong shared-proof
  detection and BDD weighting. **Fix:** use the typed `RowConverter` bytes already used for dedup.
- **`find_clause_for_row` mis-attributes provenance** when two clauses derive the same key-tuple —
  `locy_fixpoint.rs:2690-2710` returns the first match, feeding the wrong clause's IS-ref bindings into
  shared-proof detection.
- **`merge_best_by` change-detection ignores non-criteria columns** — `locy_fixpoint.rs:840-853`. A
  same-KEY, same-criteria row with a different payload column is treated as no-change → premature
  convergence with stale payload. **Fix:** hash the full retained row.
- **`merge_best_by` bypasses the `max_derived_bytes` memory limit** — `locy_fixpoint.rs:734-880`. The
  non-BEST-BY path enforces it; BEST BY can grow unbounded. **Fix:** apply the same limit check.
- **CHECK constraints silently unenforced in bulk load** — `uni-bulk/src/bulk.rs:587-633`. Only exact
  3-token `prop op value` expressions are evaluated; anything else (AND, parens, functions, unknown
  ops, missing props) returns `Ok(true)`. Bulk-loaded data can violate CHECKs the interactive writer
  rejects. **Fix:** delegate to the real evaluator or error.
- **Promote inserts intra-pattern duplicate UIDs** — `uni-fork/src/diff.rs:494-535`. `just_inserted` is
  populated only after the batch insert, so two rows in the same pattern with the same content UID both
  insert. **Fix:** dedup candidates by UID before insert.
- **`Direction::Both` dedups neighbors by `Eid` alone → self-loops under-reported** —
  `adjacency_manager.rs:99-151`. **Fix:** key by `(eid, direction)`.
- **Procedure-call `Float32` score yields fall through to stringify** — `procedure_call.rs:95-101` vs
  `build_typed_column:790-852` (no `Float32` arm). **Fix:** add the arm / normalize to Float64.
- **Pattern-comprehension empty vs non-empty rows emit different list child types** —
  `pattern_comprehension.rs:273-274` (compile-time type) vs `:442-466` (runtime type). **Fix:** cast to
  `output_item_type` before assembling.
- **Inclusion-exclusion `weight` rebuilds clause unions per subset** — `dependency_dnf.rs:250-264`,
  O(2ⁿ·n·|rv|) with allocation in the innermost step (K up to 24 = 16M iterations); `BaseRvSet::iter()`
  boxes a trait object per call. **Fix:** Gray-code incremental update + non-allocating iteration.
- **`save_named_snapshot` / OCC registry edge cases** — `snapshot/manager.rs:118-131` read-modify-write
  with no guard (lost named-snapshot update); `load_latest_snapshot` "not found" detection via substring
  match (`:88`) is backend-fragile.
- **Plugin scheduler: `JobDefinition.timeout` and `ConcurrencyLimit::Exclusive` are never enforced** —
  `scheduler.rs:447-469, 382-507`; no spawn backpressure. A hung job occupies a `spawn_blocking` worker
  forever. **PyO3 has no execution deadline** and holds the GIL across the whole batch
  (`adapter_scalar.rs:183`); adapter panics can unwind across the FFI boundary (`:140` — no
  `catch_unwind`). Filesystem allow-list globs span `/` → `read:["/data*"]` matches `/data_evil/secret`
  (`capability.rs:441`).

---

## PERFORMANCE — hot-path costs worth addressing

- **No group commit** — `wal.rs:260-352`, `writer.rs:730`. The entire commit holds `flush_lock` across
  the WAL PUT + fsync, fully serializing durable I/O (throughput capped at 1/fsync-latency). A design
  doc reportedly exists (`docs/proposals/group_commit.md`). Highest-leverage perf win.
- **`apply_incremental_updates` is O(total index size) per mutation** — `inverted_index.rs:392-452`.
  Each "incremental" update full-scans then overwrites the whole index → bulk loads quadratic. **Fix:**
  true delta append/compaction.
- **`load_subgraph` probes every label's adjacency dataset per (vid, edge_type)** — `manager.rs:1872`.
  O(vids × edge_types × labels). **Fix:** use `vid_labels_index` to pick the label directly.
- **Per-fixpoint-iteration full-fact clone/sort** — `locy_fixpoint.rs:4630-4664` (clones facts per
  registry entry even when unchanged — add a dirty flag), `:757-838` (`merge_best_by` clones + concats +
  lexsorts the cumulative fact set every iteration — keep an incremental best-per-KEY map).
- **`collect_is_ref_inputs` O(delta × source) nested scan per iteration** — `locy_fixpoint.rs:2034-2064`.
  **Fix:** hash-index the source once per binding.
- **WAL append deep-clones every property map under the state mutex** — `writer.rs:894-972`; the tx is
  consumed by the merge anyway, so a borrowed serialization path avoids the per-row clone.
- **Bulk per-row clones** — `bulk.rs:758-762` (clones the label vec + properties per vertex),
  `:959,980` (two full deep clones of every edge entry for fwd/bwd sort). **Fix:** share `Arc<[String]>`
  labels; sort index vectors.
- **L0 vector/FTS merge is O(candidates × results)** — `manager.rs:2128-2134, 2233-2239`; build a
  `HashMap<Vid, idx>` once. Unbounded `IN`/`OR` filter strings lacking the 8192-chunking used elsewhere
  (`inverted_index.rs:280`, `delta.rs:717`, `adjacency.rs:281`).
- **Schemaless L0 label filter allocates a String per label per row** — `scan.rs`
  (`labels.contains(&lf.to_string())`). **Fix:** compare by `&str`.
- **Tarjan SCC / DFS algorithms are recursive** — `scc.rs`, `cycle_detection.rs:86`, `bridges.rs:89`,
  `articulation_points.rs:89` — stack-overflow on long chains (10⁶ vertices). **Fix:** explicit
  work-stack for the hot ones.

---

## Cross-cutting systemic themes (fix the pattern, not just the instance)

1. **Debug-string / `serde_json` as a hashing/identity contract.** Instances: Locy provenance
   (`locy_fixpoint.rs:1578+`), correlated-subquery dedup (`apply.rs:569`), shared-feature detection
   (`uni-locy/typecheck.rs:880`), `FOR UPDATE` keys (`for_update.rs:115`). Debug/JSON rendering is not a
   stable, collision-free, type-preserving key. **Adopt one canonical typed key encoding** (the
   `RowConverter` bytes already used for dedup) everywhere identity matters.

2. **Append-only read paths without version + tombstone awareness.** Instances: C2, C3, JSON-path index
   (`json_index.rs`), inverted index. Reads must select max `_version` and treat the winner's tombstone
   as "absent." Centralize a "latest-visible-version" helper and route all index/main-table point reads
   through it.

3. **Per-row lock acquisition / per-call runtime construction in hot loops.** Instances: single-hop
   traversal, `build_all_props_column`, schemaless label sync, pattern-comprehension Tokio runtime.
   Hoist guards above the row loop; reuse runtime handles.

4. **Untrusted-input opt-in for resource limits & trust.** Plugin loaders don't verify signatures (C6),
   Extism limits are manifest-opt-in (H15), redirects bypass the allow-list (H14). The hardened
   component loader (floors limits, enforces caps) is the model the other loaders should match.

---

## Notable FALSE ALARMS cleared during review (do not act on these)

- **PageRank dangling-mass term** (`pagerank.rs:81`) — CORRECT; `test_pagerank_conserves_mass_with_dangling_node`
  **passes**. The `// RED today` comment is stale.
- **Louvain `compute_modularity`** (`louvain.rs:180-220`) — CORRECT; hand-computed 0.3571 matches,
  `test_louvain_modularity_scaling` **passes**. Stale comment.
- **`drop_superseded_pushdown_rows` "concurrent-delete data loss"** (`scan.rs:762-840`) — correct; runs
  on the persisted batch before the L0 overlay merge, sees deletion tombstones.
- **Plan cache "keyed by text not structure"** — correct; SipHash with full-string equality re-check +
  per-execution param re-binding (placeholders retained).
- **Stratified negation across strata** (`uni-locy/stratify.rs`) — correct; negative edges feed
  `scc_depends_on`, Kahn orders the negated stratum strictly earlier.
- **`VectorClock::happened_before`, `LWWRegister`/`GCounter` merge, `Crdt::try_merge` type-mismatch** —
  all correct/commutative/idempotent; type-mismatch is guarded, not silently overwritten.
- **WASM-component runtime** (epoch ticker, `StoreLimits`, single-use instance pool, 8 MiB response
  cap, secret-handle membrane) and **Rhai sandbox** (`eval` disabled, `import` denied, always-on
  op/depth floors) — both sound; the plugin problem is wiring (C6), not the crypto or the component
  sandbox.
- **`l0.rs:1433` constraint_index "lost update", `l0_manager.rs:265` "nested RwLock deadlock", OCC
  `read_seq` Relaxed read** — all verified sound under `flush_lock` serialization.

---

## Suggested remediation order

1. **C1–C5** (data loss / wrong results): backend scan error propagation, tombstone-aware reads,
   manifest fsync, schema-version bump. Small, localized, high-impact.
2. **C6 / H14 / H15** (security): wire signature enforcement, redirect re-validation, unconditional
   Extism limits.
3. **H1–H3** (durability & SSI soundness): read-set completeness, finalizer panic guard, WAL
   fsync-failure cleanup.
4. **H4–H13, H16** (query/bulk/algo correctness).
5. Systemic refactors (canonical key encoding, version-aware read helper) + the perf set, led by
   **group commit**.
