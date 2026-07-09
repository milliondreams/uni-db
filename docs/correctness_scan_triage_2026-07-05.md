# Correctness Scan — Triage, Prioritization & Solution-Region Grouping (2026-07-05)

Companion to `docs/correctness_scan_2026-07-05.md` (the 171-finding audit) and
`docs/correctness_scan_verification_2026-07-05.md` (independent re-verification: 167
confirmed, 1 uncertain, 3 refuted, all repro'd).

This document reorganizes the **167 confirmed findings** by **solution region** — the
code area and root-cause *shape* where a single fix (often one shared helper) clears
multiple findings — then prioritizes the regions for remediation.

Finding references use `crate[N]` from the scan (e.g. `uni-query[35]`, `uni-store[7]`).

---

## How to read this

- **Regions** are ordered by *remediation priority* = worst-member severity × blast
  radius × cluster leverage (how many findings one fix clears).
- **Leverage** flags where a single architectural change / shared helper resolves the
  whole cluster — do these first; they are the cheapest correctness-per-edit.
- Every confirmed finding appears in exactly one region (regions R1–R18 are the
  high-leverage clusters; the **Long Tail** holds genuinely independent bugs, grouped by
  domain).

## Priority tiers

| Tier | Meaning |
|---|---|
| **P0** | Memory-unsafety, silent data loss/corruption, whole-DB blast radius. Fix first. |
| **P1** | Wrong query results, broken durability/security invariants, silent drops. |
| **P2** | Localized correctness (single algorithm, single conversion, bounded panic). |
| **P3** | Test-harness fidelity, cosmetic, opt-in surfaces. |

---

# High-leverage solution regions (fix the cluster, not the finding)

## R1 — Fork lifecycle & content-UID (P0) · **leverage: high**
**Root:** fork machinery shares live state by `Arc` and mis-scopes lifecycle; one path
even shuts down the whole DB.
- `uni[1]` (Critical) `fork_maintenance.rs:61` — `sweep_tick` wraps live `UniInner` in a
  transient `Uni` whose `Drop` calls `shutdown_blocking()` → **broadcasts shutdown to the
  running database**.
- `uni[2]` `fork_maintenance.rs:181` — `fork_local_indexes` DashMap keyed on `(label,col)`
  holds one kind → two index kinds ping-pong forever.
- `uni[6]` `fork.rs:452` — nested-fork reads parent tip after `flush_lock` released →
  capture/branch race reopened.
- `uni-fork[3]` `diff.rs:643` — recomputed content-UID (props strip `ext_id`) can never
  equal registered UID (props still contain `ext_id`) → dedup silently never fires.
- `uni-fork[4]` `diff.rs:1031` — delete-promotion ignores `ConflictPolicy`/baseline props
  → deletes primary's concurrently-edited row even under `Skip`.

**Fix strategy:** never construct an owning `Uni` over borrowed inner (use a non-Drop
handle); unify the content-UID hash to a single canonical property set (with or without
`ext_id`, consistently) shared by writer + promote; make delete-promotion consult policy.

## R2 — Error-swallowing that destroys or masks state (P0) · **leverage: high**
**Root:** `.unwrap_or_default()` / map-any-error-to-empty on a fallible read whose empty
result *means* "nothing exists" — so a transient failure is read as "delete everything."
- `uni-fork[1]` `diff.rs:43` — one-sided `get_vertex_ext_ids()` failure → unchanged
  vertices reported as pure deletes.
- `uni-fork[2]` `diff.rs:597` — same in `run_promote` → **mass-deletes live primary
  vertices**.
- `uni-store[6]` `registry.rs:171` — any GET failure treated as "registry never created"
  → empty registry persisted → **orphans all existing forks**.
- `uni-store[8]` `snapshot/manager.rs:178` — load error → empty map → RMW **wipes all named
  snapshots**.
- `uni-store[9]` `storage/manager.rs:1295` — `table_exists()` error → `false` (across
  scan/vector/fts paths) → live table read as absent.
- `uni-query[4]` `search_procedures.rs:1578` — `run_hybrid_search` swallows
  `auto_embed_text` error → silently drops the dense arm.
- `uni-tck[2]` `world.rs:320` — `collect_ids`/snapshot swallow query errors → corrupt
  side-effect diffs (test-oracle blind spot).
- `uni-cli[3]` `main.rs:139` — one-shot `uni query` swallows errors to stdout, **exits 0**
  on failure.
- `uni[5]` `schema.rs:207` — `AddEdgeType` swallows "already exists" even when
  from/to_labels differ.

**Fix strategy:** one audited pass distinguishing `NotFound` (empty is correct) from
transient/other (propagate). This is a mechanical but high-value sweep.

## R3 — L0 tombstone resurrection & union-only overlays (P0/P1) · **leverage: high**
**Root:** an overlay/merge is union-only and ignores a *later* buffer's tombstone or the
replacement/overwrite marker → deleted data reappears in reads and search.
- `uni-store[2]` `compaction.rs:511` — empty compacted output skips L2 replace but clears
  L1 tombstones → resurrects deleted edges.
- `uni-store[4]` `property_manager.rs:572` — `overlay_l0_batch` removes vid on *any*
  tombstone unconditionally (vs version-gated props) → version-pinned read mismatch.
- `uni-store[10]` `l0.rs:1561` — `replay_mutations` SetVertexLabels restores labels but
  never sets `vertex_label_overwrites` → WAL-durable label mutation lost at first flush.
- `uni-store[17]` `manager.rs:2582` — `merge_l0_into_vector_results` appends without
  checking tombstoned set → deleted vertex resurrected into vector search.
- `uni-store[18]` `manager.rs:2737` — identical copy-paste in
  `merge_l0_into_fts_results`.
- `uni-query[29]` `scan.rs:2806` — L0 label overlay union-only, ignores
  `vertex_label_overwrites` → `REMOVE n:Label` invisible; label resurrected in
  `labels(n)` and MATCH.
- `uni-plugin-host[1]` `triggers.rs:835` — tombstone treated as "skip buffer" instead of
  "entity dead" → older buffers resurrect it for triggers.

**Fix strategy:** one tombstone-aware, recency-ordered merge primitive honoring
overwrite markers; route all six read/merge paths through it.

## R4 — MVCC version-ignoring batch reads (P1) · **leverage: high**
**Root:** `_version` is projected but never used to rank; rows applied in scan order,
last-row-wins → result depends on physical layout.
- `uni-store[3]` `property_manager.rs:735` — `get_batch_edge_props`.
- `uni-store[5]` `main_vertex.rs:637` — `find_batch_props_by_vids` /
  `find_batch_labels_by_vids` (`_deleted=false`, no `_version` ranking).
- `uni-store[7]` `property_manager.rs:506` — `get_batch_vertex_props`.

**Fix strategy:** the single-row paths already do version-max selection (review C2 fix);
lift that into the shared batch reducer. One helper, three call sites.

## R5 — UNIQUE / constraint visibility gaps (P1) · **leverage: high**
**Root:** uniqueness checked against only a subset of the write horizon (in-load, or
L0-only), never the full L0 + txL0 + pending_flush + storage set → silent duplicates.
- `uni-store[11]` `writer.rs:3231` — `insert_vertices_batch` never populates L0
  `constraint_index`.
- `uni-store[12]` `writer.rs:1961` — `validate_vertex_batch_constraints` skips
  `pending_flush` buffers (the Bug #9A window).
- `uni-bulk[4]` `bulk.rs:581` — UNIQUE checks only keys seen in this load, never committed
  storage → duplicate against pre-existing key.
- `uni-bulk[1]` `bulk.rs:1116` — both defer flags false → index rebuild skipped and flush
  never builds user indexes.

**Fix strategy:** one constraint-lookup surface consulting the full horizon; batch and
bulk paths call it. Mirrors the single-vertex `check_unique_constraint_multi` fix.

## R6 — Planner "consume-but-don't-apply" predicate drop (P1) · **leverage: high**
**Root:** a helper *descends* `Sort/Limit/Aggregate/Apply/Union` to mark a predicate
consumed, but the sibling rewriter has no arm for those nodes and drops it via
`other => other` → WHERE / label / vector predicate silently vanishes.
- `uni-query[36]` `planner.rs:6211/7739` — WHERE on scan-bound var below
  Sort/Limit/Aggregate/Apply (`push_predicate_to_scan`).
- `uni-query[39]` `planner.rs:6168/6665` — label disjunction `n:A OR n:B`
  (`replace_scan_all_with_label_union`).
- `uni-query[38]` `planner.rs:6224/6935` — traverse-target predicate
  (`push_predicate_to_traverse`).
- `uni-query[35]` `planner.rs:6130` — `vector_similarity` KNN predicate
  (`replace_scan_with_knn`).
- `uni-query[22]` `apply.rs:350` — `Apply` `input_filter`: unsupported operators
  (STARTS WITH / IN / arithmetic / CASE) evaluate to `false` (sibling of the same
  "descend-but-don't-handle" shape, in the Apply evaluator).

**Fix strategy:** make the consume-traversal and the apply-traversal share one recursion
(or add the missing arms). The four planner cases are literally the same missing-arm bug
in four rewriters — one refactor pattern.

## R7 — Non-deterministic Debug/hash dedup keys (P1) · **leverage: high**
**Root:** using `format!("{:?}")` of a `HashMap`-backed row/Value as a dedup or
cycle-detection key; HashMap `Debug` order is instance-dependent → identical content
splits across keys.
- `uni-query[25]` `locy_query.rs:138` — `RETURN DISTINCT` → duplicate multi-column rows
  survive.
- `uni-query[18]` `executor/read.rs:5066` — `UNION` (non-ALL) dedup.
- `uni-query[14]` `recursive_cte.rs:260` — cycle detection → **non-termination** (already
  seen rows never recognized).
- `uni-common[1]` `value.rs:789` — Value `Eq` not reflexive for Vector/SparseVector with
  NaN (undermines any hash/eq built on Value).

**Fix strategy:** one canonical, order-stable row-key encoder (sorted keys / structural
hash); DISTINCT, UNION, CTE all consume it. Fix Value `Eq`/`Hash` reflexivity as the
foundation.

## R8 — OPTIONAL MATCH null-row plumbing (P1) · **leverage: medium**
**Root:** optional variables' NULL rows are dropped, mis-typed as non-nullable, or
recovered without cross-batch state.
- `uni-query[17]` `df_planner.rs:2848` — `hydrate_virtual_target` uses Inner join →
  drops NULL-target rows.
- `uni-query[23]` `ext_id_lookup.rs:107` — null row appended to non-nullable columns →
  arrow-58 `try_new` rejects it (errors instead of null row).
- `uni-query[26]` `optional_filter.rs:370` — per-batch null-recovery, no cross-batch
  state → duplicate/spurious NULL rows.
- `uni-query[28]` `pattern_exists.rs:410` — NULL bound-target VID treated as unbound →
  EXISTS true if *any* neighbor exists.
- `uni-query[30]` `vid_lookup_join.rs:463` — `VidJoinKind::Left` NULL-pads BUILD side
  regardless of probe side → LEFT executes as RIGHT.
- `uni-query[5]` `traverse.rs:1152` — `is_optional_column_for_vars` suffix-matches
  internal `__eid_to_` columns → mis-classifies optionality.

**Fix strategy:** a consistent optional-var nullability contract (schema fields nullable,
join kind honors probe side, filter carries cross-batch group state).

## R9 — NULL / three-valued-logic violations (P1) · **leverage: medium**
**Root:** NULL collapsed to `false`/empty/first-value instead of propagating per Cypher
3VL.
- `uni-plugin-custom[1]` `eval.rs:164` — `null AND false` / `null OR true` short-circuit
  wrong.
- `uni-query-functions[7]` `df_expr.rs:1374` — `=~` → `is_not_null()` collapses NULL to
  false.
- `uni-query-functions[15]` `df_udfs.rs:4031` — `extract_string_at` never null-checks
  String arrays → null decodes as `""` (breaks STARTS WITH/ENDS WITH/CONTAINS).
- `uni-query-functions[13]` `df_expr.rs:3368` — simple `CASE` WHEN wrapped in
  `_cv_to_bool` → condition becomes literal null → branch can never match.
- `uni-query[16]` `vid_lookup_join.rs:561` — `NULL == NULL` true → rows join on NULL keys.
- `uni-query[1]` (Critical) `df_planner.rs:4918` — `count(var)` → `count(lit(1))` counts
  NULL OPTIONAL-MATCH rows instead of excluding them.

## R10 — Integer precision loss & unchecked arithmetic (P1/P2) · **leverage: high**
**Root (a) — i64 routed through f64:** values > 2^53 lose precision.
- `uni-query[7]` `executor/core.rs:138` — `Accumulator::Sum` in f64.
- `uni-query-functions[2]` `df_udfs.rs:2956` — ORDER BY sort key `*i as f64`.
- `uni-query-functions[14]` `df_udfs.rs:4226` — LargeBinary-vs-Int64 compare via f64.
- `uni-plugin-apoc-core[2]` `number.rs:153` — `toString` widens Int64 via `as f64`.
- `uni-plugin-custom[4]` `eval.rs:200` — all Int/Int arithmetic through f64 (also
  int/int division returns Float — Cypher truncates).
- `uni-bulk[3]` `bulk.rs:644` — `compute_unique_key` via lossy `Display` join (Int `1`
  vs String `"1"` collide).

**Root (b) — unchecked add/sub/pow / wrapping:**
- `uni-query-functions[8]` `df_udfs.rs:1433` — `RangeUdf` unchecked `+= step` (interpreted
  path was fixed to `checked_add`; UDF path wasn't).
- `uni-query-functions[9]` `df_udfs.rs:6959` — `CypherSumAccumulator` `wrapping_add` →
  silent i64 overflow.
- `uni-btic[1]` `btic.rs:152` — `duration_ms` unchecked `hi - lo` overflow.
- `uni-plugin-apoc-core[1]` `math.rs:164` — `10f64.powi(precision)` overflow → NaN;
  i64→i32 `as` wrap.
- `uni-plugin-rhai[2]` `adapter_procedure.rs:146` — `as i64` cast: NaN→0, saturate/trunc.

**Fix strategy:** (a) a `Value`-preserving numeric path that keeps Int as i128/i64 until
forced; (b) sweep `+`/`-`/`powi`/`wrapping_*` on user-reachable arithmetic to
`checked_*` returning errors. Two mechanical sweeps.

## R11 — Locy compiler context loss (nested / body / module) (P1) · **leverage: high**
**Root:** body/nested/module compilation drops the outer program's rule names, model
catalog, config, or module-qualification → validation and filters silently disabled.
- `uni-locy[3]` `compiler/mod.rs:286` — ASSUME body compiled with bare `compile()` (drops
  rule names, `neural_predicates_preview`, model catalog).
- `uni-locy[2]` `typecheck.rs:696` — `check_model_invocations` skips ALONG & HAVING (but
  `InvocationLifter` lifts from them → arity assumed valid).
- `uni-locy[1]` `dependency.rs:148` — `PathContextWalker` doesn't recurse into List/Map
  (but lifter does) → path-context model invoke missed.
- `uni-locy[5]` `typecheck.rs:128` — raw (unresolved) IS-ref names compared to
  module-qualified catalog → self-recursion/IS-ref validation break under MODULE.
- `uni-query[34]` `locy_planner.rs:798` — `build_rule` reads HAVING/BEST BY from the
  *first* clause only → multi-clause rule loses HAVING filter & BEST BY pruning.
- `uni[8]` `transaction.rs:1096` — commit-time rule promotion copies only the rules map,
  omits sources & strata → "registry = f(sources)" invariant broken.
- `uni[11]` `impl_locy.rs:509` — `compile_only` never forwards `LocyConfig` →
  `neural_predicates_preview` ignored on the tx path.
- `uni[7]` `rule_registry.rs:129` — `remove` rebuilds outside lock then
  `*write() = rebuilt` → clobbers concurrent `register`.

**Fix strategy:** thread a single `CompileContext` (rule names, catalog, config,
module prefix) through every body/nested/promotion path; make the walker/lifter/typecheck
visitor sets identical.

## R12 — Locy probabilistic semantics (WMC / TopK / MNOR / calibration) (P1/P2)
**Root:** shared-lineage grouping keyed by raw positions, empty-clause treated as prob
1.0, double-counted products, unpooled PAV blocks.
- `uni-query[13]` `locy_fixpoint.rs:2537` — `apply_exact_wmc` groups shared-lineage keys
  by raw yield-schema positions against reordered post-fixpoint batches. *(latent e2e —
  see verification doc; repro pins the path.)*
- `uni-query[24]` `locy_fold.rs:720` — TopKProofs MNOR: empty-base proof → DNF empty
  clause → probability 1.0 for mixed supported/unsupported groups. *(latent e2e.)*
- `uni-query[10]` `locy_abduce.rs:235` — target_var fix-up mutates `candidates.last_mut()`
  not the just-traversed candidate → wrong edge attribution. *(reproduces cleanly.)*
- `uni-plugin-builtin[1]` `locy_aggregates.rs:891` — `MprodState::merge` double-counts
  pre-switch product when `o.use_log`.
- `uni-locy[4]` `calibration.rs:341` — Isotonic PAV never pools tied prediction values →
  multiple knots at same x; `apply()` returns lowest block's mean.

## R13 — pyo3 bindings: FFI safety & GIL/Mutex deadlocks (P0/P1) · **leverage: medium**
**Root:** Arrow C-Data ownership taken by `ptr::read` (double-free); and `std::Mutex`
guards held across `py.detach(block_on(...))` whose GIL re-acquire inverts lock order.
- `uni-db-bindings[1]` (Critical) `builders.rs:2202` — `record_batch_from_pyarrow`
  `ptr::read` leaves capsule release non-NULL → **double-free / use-after-free** on
  `write_batch` (empirically SIGABRT).
- `uni-db-bindings[10]` `types.rs:1561` — `PyPreparedQuery::execute` GIL/mutex ABBA (same
  in PyPreparedLocy/QueryBinder/LocyBinder).
- `uni-db-bindings[7]` `sync_api.rs:40` — `QueryCursor::next_row` holds buffer+cursor
  mutexes across `py.detach(block_on)`.
- `uni-db-bindings[11]` `types.rs:2131` — `PyCommitStream::__next__` holds stream mutex
  across `block_on(stream.next())` → `close()`/`__exit__` deadlock.

**Fix strategy:** `ptr::replace(_, empty())` for the FFI import (match arrow-rs
`from_raw`); release every `std::Mutex` guard *before* `py.detach`/`block_on` (clone or
scope-drop). Uniform lock discipline across the four prepared-statement types.

## R14 — Plugin registry ownership & lifecycle (P1) · **leverage: medium**
**Root:** `apply_pending` overwrites (not merges) per-plugin ownership; no unregister
path; duplicate detection is registry-only.
- `uni-plugin[4]` `registry.rs:911` — second commit under same plugin id **orphans**
  earlier-registered surfaces.
- `uni-plugin[2]` `registry.rs:902` — preflight checks only vs live registry, not the rest
  of the batch → intra-batch duplicate names slip through.
- `uni-plugin-custom[8]` `lib.rs:773` — `dropDeclared` on namespace-level PluginId +
  per-declaration batches under same id → **dropping one declared plugin unregisters a
  sibling** (root = `uni-plugin[4]`).
- `uni-plugin-custom[7]` `decode.rs:64` — every `DuplicateRegistration` folded to
  `NativeShadow` → re-declaring a declared qname stores new body inactive, keeps executing
  old.
- `uni-plugin-custom[3]` `aggregate.rs:349` — install adds qname to uni-cypher's global
  hint set; `dropDeclared` never removes it (no unregister seam).
- `uni-plugin[1]` `manifest.rs:68` — `AbiRange::matches` probes minor/patch at
  `u64::MAX/2` → any upper-bounded req reports host major unsupported.

**Fix strategy:** make `apply_pending` merge ownership per (plugin id, batch); add an
unregister seam in uni-cypher's hint set; batch-aware duplicate preflight.

## R15 — Trigger / scheduler / CDC lifecycle (uni-plugin-host) (P1) · **leverage: low**
**Root:** persistence and in-memory state diverge; deferrals/deliveries fire at the wrong
time or silently gap.
- `uni-plugin-host[6]` `cdc_runtime.rs:334` — failed `deliver` logged + `continue`, keeps
  checkpointing → **permanent undetectable CDC gap** (also broadcast Lagged at :258).
- `uni-plugin-host[5]` `triggers.rs:559` — `dispatch_before` enqueues `Defer` *before*
  commit → deferred trigger fires with events from an aborted tx.
- `uni-plugin-host[2]` `triggers.rs:1555` — deferrals rebound by `subscription_name` via
  `find()` → two triggers with equal/empty names collide.
- `uni-plugin-host[3]` `scheduler.rs:148` — `cancel` never calls `persistence.cancel(id)`
  → sidecar row survives, job resurrects on restart.
- `uni-plugin-host[4]` `scheduler.rs:145` — `add_scheduled_job` upserts sidecar but
  primitive scheduler pushes a new record → duplicate jobs.
- `uni-plugin[3]` `scheduler.rs:238` — `tick_at` treats `next_fire_at==None` as due → a
  cron whose expression fails to parse fires once.

## R16 — UTF-8 byte-index panics on non-char-boundaries (P2) · **leverage: high**
**Root:** slicing a string by byte index without char-boundary check → panic on multibyte
input.
- `uni-plugin-extism[2]` `host_svc/mod.rs:86` — `from_hex` `&s[i..i+2]`.
- `uni-plugin-rhai[4]` `kms.rs:99` — `from_hex` `&s[i..i+2]` (same bug, second crate).
- `uni-db-bindings[3]` `types.rs:1877` — `__repr__` `&t[..60]` byte-truncates.
- `uni-query-functions[11]` `expr_eval.rs:1139` — `size()`/`length()` return byte len vs
  `chars().count()` (correctness, same char-vs-byte root).

**Fix strategy:** one char-boundary-safe slice/hex helper; replace all four call sites.

## R17 — Arrow column builders: 0/empty stored instead of NULL (P1) · **leverage: high**
**Root:** a missing/unhandled value is materialized as `Some(0)`/`Null` instead of a true
NULL → wrong stored data / silent COPY loss.
- `uni-store[14]` `arrow_convert.rs:1428` — `build_timestamp_column` stores `Some(0)`
  (1970-01-01) for a live row missing the property.
- `uni-store[15]` `arrow_convert.rs:1512` — `build_date32_column` same copy-paste.
- `uni-query[19]` `write.rs:1048` — `arrow_value_to_json` returns `Null` for every
  unhandled Arrow type (Timestamp/Date/LargeUtf8/lists/decimals) → **COPY FROM silently
  drops columns**.

**Fix strategy:** null-on-missing in the builders (match sibling string/int builders);
exhaustive arrow-type handling (or hard error) in `arrow_value_to_json`.

## R18 — Bindings type conversion (datetime / tz / value) (P2) · **leverage: medium**
**Root:** Python↔Rust conversions use lossy f64 nanos, local-tz-dependent timestamps,
or silently coerce unknown types.
- `uni-db-bindings[8]` `convert.rs:299` — naive datetime via local-tz `.timestamp()` while
  core is wall-clock-as-UTC → values shifted by machine offset.
- `uni-db-bindings[6]` `convert.rs:300` — nanos via f64 arithmetic → modern
  epoch-nanoseconds corrupted.
- `uni-db-bindings[5]` `convert.rs:345` — aware `datetime.time` calls `utcoffset(None)`
  (CPython `time.utcoffset()` takes no arg) → TypeError on every aware time.
- `uni-db-bindings[9]` `convert.rs:416` — unknown Python type → `Value::Null` (no error).
- `uni-db-bindings[2]` `core.rs:317` — unknown message role → `Message::user`.
- `uni-query-functions[1]` `datetime.rs:877` — `epochSeconds/Millis` truncating div → off
  by one pre-1970.
- `uni-query-functions[6]` `datetime.rs:1826` — `format_timezone_offset` loses sign for
  `-00:30`.
- `uni-common[2]` `value.rs:396` — Date `Display` unchecked `epoch + Duration::days` →
  panic out of range.

---

# Long tail — independent findings (grouped by domain)

These don't share a fix locus with the clusters above. Grouped by subsystem for
assignment; each is its own fix.

## L1 — Graph algorithms (uni-algo, P2) — 8, distinct algorithms, same reviewer
`[1]` APSP drops targets reachable at total weight 0 · `[2]` Brandes sampling never
rescaled by n/k · `[3]` A* `to_bits` ordering invalid for negative f-scores (no guard) ·
`[4]` Dijkstra `maxDistance` leaves over-budget relaxed dists in output · `[5]` Johnson
keeps nodes blocked after depth-truncation → misses shorter circuits · `[6]` bidirectional
Dijkstra takes first parallel edge in backward scan → non-minimal distance · `[7]` k-core
mixes multiplicity/dedup degree conventions → inflated core numbers · `[8]` MST drops
directed edges with `src slot > dst slot` (should normalize min/max).

## L2 — Query-planner semantics, misc (uni-query, P1) — 8
`[37]` LIMIT/SKIP applied below DISTINCT (also WITH) · `[31]` window fns share one
concatenated SortExec → wrong order for conflicting windows · `[20]` `plan_shortest_path`
reads only first hop, ignores rest · `[40]` QPP `last_outer_node_var` not updated →
second consecutive QPP anchored at stale source · `[32]` virtual-edge `Both` → outgoing
only · `[6]` traverse `Both` no per-source dedup → self-loop double-counted · `[21]`
Apply subplan dedup cache runs even when subquery writes → side effects executed once ·
`[27]` pattern-comprehension inner column order ≠ declared schema → misaligned columns.

## L3 — Locy in-memory eval (uni-query, P2) — 2
`[11]` `eval_binary_op` Int Div/Mod by zero panics (no guard) · `[12]` `value_less_than`
has no Temporal/Bool arm → `< > <= >=` and MIN/MAX over dates silently wrong.

## L4 — Comparator / ordering (P2) — 2
`uni-query[33]` `cypher_cross_type_cmp` falls through to `Equal` for same-rank
Temporal/Map/Bytes/Vector → MIN/MAX return first-seen · `uni-query-functions[10]`
`eval_sign` maps `sign(0.0)` to 1 (Rust `signum` returns 1.0 for +0.0).

## L5 — Identifier normalization & parser (uni-cypher, P2) — 5
`[1]` DERIVE label/edge names raw `as_str()` (no `normalize_identifier`) → backticks kept
· `[3]` REMOVE labels raw `as_str()` · `[2]` map-literal string keys not `unescape`d ·
`[4]` `check_nesting_depth` counts bare `end` as CASE-close → stack-guard defeatable ·
`[5]` `parse_expression` uses unanchored `Rule::expression` (no SOI/EOI) → trailing
garbage silently truncated.

## L6 — Security / authz / capability (P1) — 5
`uni[4]` `transaction.rs:338` — `Transaction::query` performs no AuthzPolicy consult, and
`Session::run` routes writes through it → **write/schema/dbms bypass authorization** ·
`uni[9]` `impl_query.rs:756` — planner built without `.with_plugin_registry()` (also
:650) → plugin catalog/virtual-labels invisible · `uni-common[3]` `schema.rs:2107` —
`rename_property` skips reserved-column & leading-underscore validation · `uni-plugin-
extism[3]` `loader.rs:416` — Pass-1 bootstrap runs `manifest` export with un-intersected
`host_grants` → over-broad caps · `uni-plugin-pyo3[1]` `loader.rs:614` —
`set_determinism` is a silent no-op.

## L7 — Storage compaction / durability races (uni-store + uni, P0/P1) — 4
`uni-store[16]` `compaction.rs:257` — `compact_vertices` unguarded scan→merge→replace
wipes rows a concurrent flush appends · `uni-store[13]` `adjacency_manager.rs:381` —
`compact()` writes Incoming shadow keyed by `src_vid` (should swap to `(dst,src)`) →
corrupt time-travel · `uni-store[1]` `id_allocator.rs:93` — advances batch reservation
before `persist_manifest`; persist failure leaves phantom advance · `uni[3]`
`transaction.rs:1044` — `commit()` timeout can cancel *after* the durable point →
retriable error for a committed tx.

## L8 — Plugin adapters: type mapping & contracts (P2) — 9
`uni-plugin-extism[1]` Vector return maps to element type (bridge declares List) ·
`uni-plugin-pyo3[2]` empty accumulator emits `"{}"` → fed to user `merge()` ·
`uni-plugin-rhai[1]` serde_json encodes NaN/Inf as null → peer state corrupted ·
`uni-plugin-rhai[3]` finalize ignores declared return type · `uni-plugin-rhai[5]` yield
fields named `col0..colN` but row-map keys don't match → NULL substituted ·
`uni-plugin-custom[5]` `declareTrigger` body taken from arg 1 (event_filter) not 2 ·
`uni-plugin-custom[6]` scalar `row_count = rows.max(1)` fabricates a row for 0-row calls ·
`uni-plugin-custom[2]` `declare` check-then-act race (read lock → write lock) ·
`uni-plugin-builtin[2]` `try_rewrite_topn` strips non-column sort keys then elides Sort.

## L9 — Isolated query/function bugs (P2) — 8
`uni-query[2]` labelInfo JsonFullText check ignores column · `uni-query[8]`
projection_store keyed on `Arc::as_ptr` w/o keeping Arc alive or evicting → stale reuse ·
`uni-query[9]` `resolve_metric_for_property` ignores label → first index wins ·
`uni-query[15]` `parse_reranker_options` `clamp(k,1000)` panics when k>1000 ·
`uni-query-functions[3]` `value_to_sparse` wraps negative/large indices via `as u32` ·
`uni-query-functions[4]` `withinBBox` empty range across antimeridian ·
`uni-query-functions[5]` `eval_point` ignores non-numeric `z` · `uni-query-functions[16]`
`similar_to` Dot-metric similarity sign-inverted.

## L10 — CRDT / temporal / CLI / misc (P2/P3) — 6
`uni-crdt[2]` ORSet v1→v2 mints `__legacy__` dots restarting at 1 → cross-replica dot
collision → **silent element loss on merge** (P1) · `uni-crdt[1]` LWWMap `-1` sentinel not
reserved → `put` with ts≤-1 on missing key dropped · `uni-btic[2]` hour-granularity
datetime literal never parses (chrono needs minute) · `uni-plugin-apoc-core[3]`
`text.repeat` caps count not synthesized length → OOM guard ineffective · `uni-cli[2]`
Paper vertices inserted with empty label set → `Paper` label missing · `uni-cli[1]`
progress `print!("\r")` without flush.

## L11 — Test-harness fidelity (P3) — 4
`uni-tck[1]` `value_sort_key` collapses Nodes/Edges/containers to constant keys →
order-insensitive list compare degenerates to order-sensitive · `uni-tck[3]` "no side
effects" step ignores gross/property counters · `uni-tck[4]` "side effects should be:"
never asserts unlisted counters are 0 · `uni-locy-tck[1]` "having executed:" is a no-op
without a docstring (silent skip).

---

# Recommended remediation order

Ordered by (a) irreversibility/blast radius, then (b) cluster leverage. Bracketed count =
findings cleared.

**Wave 0 — stop the bleeding (P0, memory-unsafety & data destruction):**
1. **R13** Arrow FFI double-free `uni-db-bindings[1]` — memory unsafety on a public API. [1 of cluster]
2. **R1** Fork sweep_tick whole-DB shutdown `uni[1]` + fork data-loss. [5]
3. **R2** Error-swallow-destroys sweep (mass-delete / snapshot-wipe / fork-orphan). [9]
4. **R3** L0 tombstone resurrection primitive. [7]

**Wave 1 — shared-helper clusters (max correctness-per-edit):**
5. **R7** canonical row-key encoder (DISTINCT/UNION/CTE + Value Eq). [4]
6. **R6** planner consume-vs-apply recursion. [5]
7. **R4** MVCC batch version-ranking helper. [3]
8. **R5** unified constraint-visibility surface. [4]
9. **R10** integer-precision + checked-arithmetic sweeps. [11]
10. **R16** char-safe slice/hex helper. [4]
11. **R17** arrow null-on-missing builders. [3]
12. **R11** Locy `CompileContext` threading. [8]

**Wave 2 — correctness clusters (P1):**
13. **R9** NULL/3VL. [6] · **R8** OPTIONAL MATCH. [6] · **L6** security/authz. [5] ·
    **R14** plugin registry. [6] · **L7** compaction/durability races. [4] ·
    **R12** Locy probabilistic. [5] · **R15** host lifecycle. [6] · **L2** planner
    semantics misc. [8]

**Wave 3 — localized (P2) & harness (P3):**
14. **R18** bindings conversion. [8] · **L1** algorithms. [8] · **L8** plugin adapters. [9]
    · **L9** isolated query/fn. [8] · **L3/L4/L5/L10** eval/comparator/parser/misc · **L11**
    test-harness fidelity. [4]

## Coverage note

All 167 confirmed findings are assigned to exactly one region (R1–R18 + L1–L11). The 3
refuted (`uni[10]`, `uni-query[3]`, `uni-db-bindings` builders-`block_on`-under-`Mutex`)
and 1 uncertain (`uni-query[3]`, net refuted) are excluded — see the verification doc.
Repros for every confirmed finding already exist in-tree (see verification doc §Repro
coverage); this triage does not change any test.

---

# Appendix — untracked repro artifacts & status (reviewed 2026-07-08)

The files below are **untracked** (not yet `git add`ed) repro/verification artifacts left
over from the audit. Each maps to a region/finding above. Two idioms recur: *assert-the-
symptom* repros stay green while the bug lives and **flip red once the fix lands** (a
built-in fix-detector, but they must then be converted into forward regression guards);
*diagnostic* harnesses print a verdict and assert nothing.

| File | Region · finding | Status | Notes |
|---|---|---|---|
| `bindings/uni-db/tests/test_repro_gil_mutex_deadlocks.py` | **R13** · `uni-db-bindings[10]` (`types.rs`), `[7]` (`sync_api.rs`), `[11]` (`types.rs`) | **FIXED 2026-07-08** | 3 GIL/std::Mutex ABBA deadlocks, all eliminated at the root (no lock held across `py.detach(block_on)`): `[10]` prepared `execute` — redundant outer `Mutex`→`Arc` (4 sites); `[7]` `QueryCursor::next_row` — cursor taken out by value for the await, written back (mirrors `fetch_all`); `[11]` `PyCommitStream` — stream taken out for the await + interruptible `close()` via `AtomicBool`+`Notify`. Repro **converted** to forward regression guards (assert `DONE_NO_DEADLOCK`, 12 s timeout kept as hang backstop); all 3 pass in ~4 s + full cursor/prepared/watch suites green. |
| `bindings/uni-db/tests/test_repro_prepared_locy_repr_panic.py` | **R16** · `uni-db-bindings[3]` (`types.rs:1877`) | **FIXED (`1ea719890`)** | `PyPreparedLocy.__repr__` byte-slice `&t[..60]` → replaced with char-boundary-safe `char_indices().nth(60)` in the Wave-1 R16 char-safe sweep. Repro **converted 2026-07-08** from assert-the-panic into a forward regression guard (asserts clean truncation + the multibyte `é` survives the old byte-60 cut). |
| `crates/uni-tck/tests/repro_collect_ids_swallows_error.rs` | **R2** · `uni-tck[2]` (`world.rs:320`) | **CONFIRMED · OPEN (blocked)** | Error-swallow in test oracle. `#[ignore]` empty placeholder — private fns + non-failing queries give no injection seam. Fix needs a `#[cfg(test)]` unit test in `world.rs` or a signature change to `Result<…>`. |
| `crates/uni/tests/common/bugs/repro_nested_fork_capture_race.rs` | **R1** · `uni[6]` (`fork.rs:452`) | **OPEN (race) · NOT WIRED** | Nested-fork capture/branch race. `#[tokio::test] #[ignore]`, best-effort stress loop. **Not referenced in `crates/uni/tests/common/bugs/mod.rs` → currently uncompiled/dead.** Deterministic repro needs a production suspension hook. Action: wire into `mod.rs` or delete. |
| `crates/uni/examples/optional_batch_repro.rs` | **R8** · `uni-query[26]` (`optional_filter.rs:370`) | **Diagnostic** | `#[tokio::main]` example (not a test); prints `OK`/`MISMATCH` for OPTIONAL MATCH NULL-recovery across batch boundaries. Run manually; no in-file verdict. |
| `crates/uni/tests/tmp_verify_apply_filter.rs` | **R6** · `uni-query[22]` (`apply.rs:350`) | **DELETED 2026-07-08** | Throwaway `tmp_` scratch verifying the Apply `input_filter` predicate drop; superseded by the CALL … YIELD + WHERE fix in `2399fe6fa` and its permanent `bug_call_yield_where_dropped` module in `mod.rs`. (Note: R6 `uni-query[22]`'s broader unsupported-operator arm — STARTS WITH / IN / arithmetic / CASE → `false` — is not necessarily fully covered by that commit and remains open.) |
| `crates/uni-locy-tck/tck/features/semiring/ZZRepro720.feature` | **R12** · `uni-query[24]` (`locy_fold.rs:720`) | **DELETED 2026-07-08** | `ZZ`-prefixed "temporary" TopKProofs MNOR verification feature with a stale assertion (R2 prose "expects 0.76" vs `Then` asserting `p = 1.0`); no unique coverage over R12. |

**Actionable, still genuinely open:** the 3 PyO3 deadlocks (R13) and the `__repr__` panic
(R16) are confirmed and reproduce; the `__repr__` fix is one line. **Integration debt:**
`repro_nested_fork_capture_race.rs` is not wired into `mod.rs`; `tmp_verify_apply_filter.rs`
and `ZZRepro720.feature` are superseded/stale scratch (delete candidates).
