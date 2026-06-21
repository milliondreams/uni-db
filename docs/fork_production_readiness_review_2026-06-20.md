# uni-db Fork Paths — Production-Readiness Review

<!-- Generated 2026-06-20 by an 8-lane adversarial multi-agent review (56 agents): 47 findings raised, 41 confirmed after per-finding verification, 6 rejected as false positives. -->


## 1. Executive Summary

**Verdict: the fork subsystem is feature-rich and mostly sound on the happy path, but it is NOT production-ready as-is.** There is one genuine **critical primary-data-corruption** bug, a cluster of **high-severity isolation breaches** rooted in Arc-shared in-memory state, and a real **use-after-drop race** in the registry lifecycle. The recurring root cause is a single design pattern: `at_fork_with_schema` and the snapshot/version clones **share parent in-memory state by `Arc::clone` instead of giving the fork its own copy**, while fork-local writes mutate that shared state. The team already applied the correct pattern in two places (`adjacency_manager` is built fresh at `manager.rs:550`; `locy_rule_registry` is deep-copied) — it simply wasn't applied consistently to `vid_labels_index` (`manager.rs:558`) or `snapshot_manager` (`manager.rs:549`).

Top risks, in priority order:

1. **CRITICAL — Fork flush corrupts the PRIMARY on reopen.** A forked session's flush publishes the fork's tiny version/WAL high-water-marks into the single global `catalog/latest` pointer (`manager.rs:549` shared SnapshotManager → `writer.rs:4408-4417` → hardcoded `catalog/latest` at `snapshot/manager.rs:28-29`). On the next primary `Uni::open`, the primary's version counter **regresses** to the fork's value (`mod.rs:3168-3172`), causing silent lost updates and wrong WAL replay. This is verified end-to-end below.
2. **HIGH — Shared `vid_labels_index` Arc breaks fork↔parent isolation.** A fork flush mutates the parent's in-memory label index (`manager.rs:558` shared Arc → `writer.rs:3938-3943`), so since #99 the parent's traversal-time label resolution returns fork-polluted/deleted labels — cross-session query-correctness corruption.
3. **HIGH — `drop_fork`/`drop_fork_cascade` take no name-lock**, racing a concurrent `fork().build()` into a use-after-drop over force-deleted Lance branches (`mod.rs:1282-1390` vs `fork.rs:83-84`).
4. **HIGH/MEDIUM — Durability & leak gaps:** fork WAL dirs + id-allocator files are never deleted on drop (unbounded disk leak); fork WAL replay always starts at LSN 0 with no HWM gate (double-apply on interrupted truncate); `drop_fork` deletes the recovery tombstone even when branch deletion failed.
5. **HIGH/MEDIUM — Diff/promote semantic gaps:** content-addressed UID ignores `ext_id`, so distinct vertices silently collapse in diffs and promote inserts twins; promote is additive-only with no conflict/delete handling.

The good news: most of these are **narrow-window or fork-feature-scoped** (no primary corruption except #1), the fixes are well-understood and low-risk (mostly "deep-copy instead of Arc-clone" + "hold the name-lock"), and the existing test suite (40 Rust fork files + `test_fork.py`) is strong on happy-path correctness. The gaps are concentrated in **concurrency, durability/recovery, and diff/promote semantics**.

---

## 2. Confirmed Findings (grouped by severity)

Severities below are the **adversarially-corrected** severities from the verdicts, not the original reporter severities. Where two findings describe the same defect across lanes, they are merged.

### CRITICAL

#### C1. Fork flush overwrites the global `catalog/latest` pointer + version/WAL HWM, corrupting the primary on reopen
- **Dimension:** data-reliability (escalates from isolation to primary corruption)
- **File:line:** `crates/uni-store/src/storage/manager.rs:549` (shared `snapshot_manager`); `crates/uni-store/src/snapshot/manager.rs:28-29` (hardcoded `catalog/latest`, no fork namespace); `crates/uni-store/src/runtime/writer.rs:3815-3816` (HWM overwrite from fork's version namespace), `:4408-4417` (unconditional publish); `crates/uni/src/api/mod.rs:3168-3172` (primary reopen reads the poisoned HWM). All five verified directly during this review.
- **Impact:** When a forked session flushes (explicit `flush()`, auto-flush at `auto_flush_threshold`, or shutdown) and is the **last writer to `catalog/latest`** before the primary reopens, the primary's `start_version` regresses to the fork's small value and WAL replay runs against the wrong watermark. Result: new primary writes get `_version` numbers that collide with / are shadowed by existing higher-versioned rows (**silent lost updates**), plus ghost/skipped WAL segments. Masked today only by the accident that the existing soak test (`fork_writes_soak.rs:130`) does a *primary* flush last, healing the regression, and only asserts row-count not version monotonicity.
- **Root cause:** Fork shares the parent's `SnapshotManager` by Arc, and the snapshot pointer is a single un-namespaced global path — the one piece of fork catalog state that was NOT namespaced (contrast `catalog/forks/{id}/id_allocator.json`, which IS).
- **Recommended fix:** Give forks their own snapshot namespace. Either (a) construct a fork-scoped `SnapshotManager` writing `catalog/forks/{fork_id}/latest` + `.../manifests/`, or (b) make the fork writer's `flush_finalize_body` skip `save_snapshot`/`set_latest_snapshot` and persist fork flush state in a per-fork manifest keyed by `fork_id`. Add a `debug_assert` in `flush_finalize_body` that a fork-id writer never touches global `catalog/latest`.
- **Guarding test:** `fork_flush_global_pointer_isolation.rs` — primary writes+flush (record version HWM); fork, write on fork, **flush the fork as the LAST writer** (no healing primary flush); drop; reopen primary and assert (1) `version_high_water_mark` unchanged, (2) all primary rows visible, (3) a new primary write gets a version strictly greater than the old HWM. Must check version-HWM monotonicity, not just row count (the key difference from `fork_writes_soak.rs`).

---

### HIGH

#### H1. Shared `vid_labels_index` Arc: a fork-local flush mutates the parent's label index (and vice-versa)
*(Merges the two `iso-shared-vidlabels-index` entries across the isolation-shared-state and index-inheritance lanes.)*
- **Dimension:** isolation → correctness
- **File:line:** `crates/uni-store/src/storage/manager.rs:558` (`vid_labels_index: self.vid_labels_index.clone()` — Arc clone of `Arc<RwLock<VidLabelsIndex>>`); mutated at `crates/uni-store/src/runtime/writer.rs:3938-3943` (every flush calls `update_vid_labels_index`/`remove_from_vid_labels_index`, no `fork_scope()` guard); `vid_labels.rs:47-66` (`insert` unconditionally replaces); read at `crates/uni-query/src/query/df_graph/mod.rs:489-491` (the #99 fallback `resolve_vertex_labels`).
- **Impact:** Fork and parent share ONE interior-mutable index. (a) Fork-created vids (allocated above parent HWM, `fork.rs:252-270`) **leak into the parent's index**. (b) A fork relabel/delete of an **inherited** vid (same vid as parent) overwrites/removes the parent's own entry. Since #99 wired `resolve_vertex_labels` to fall back to this index, the parent's `MATCH (a)-[r]->(b:Label)` traversal resolution now sees fork-polluted or fork-deleted labels → silent cross-session wrong results. The relabel case has no fallback and returns wrong labels; the delete case reintroduces the #99 false-negative on the parent.
- **Root cause:** Same Arc-share pattern as C1; isolation reasoning applied to `adjacency_manager` (`manager.rs:550`, built fresh) but not to `vid_labels_index`.
- **Recommended fix:** Deep-copy at fork time: `vid_labels_index: Arc::new(parking_lot::RwLock::new(self.vid_labels_index.read().clone()))` (`VidLabelsIndex` already derives `Clone`). The clone happens after flush-before-branch, preserving #99 inheritance while making fork mutations private.
- **Guarding test:** `fork_parent_index_isolation.rs` — parent `CREATE (:A)-[:R]->(:B)`, commit; fork; on fork `MATCH (b:B) DETACH DELETE b` (and a relabel variant) + `forked.flush()`; assert parent `MATCH (a)-[r:R]->(b:B)` still returns 1 and `MATCH (n:B)` still 1. Plus a leak case (fork `CREATE (:Widget)` + flush → parent `MATCH (w:Widget)` returns 0) and a sibling-fork variant. Keep an inheritance assertion to prove #99 is preserved. **This is also the test-gap finding `test-gap-parent-index-isolation` — same test closes both.**

#### H2. `drop_fork`/`drop_fork_cascade` take no name-lock → use-after-drop race with concurrent `fork().build()`
*(Merges `fork-drop-no-namelock-create-race` and the production-path trigger `fork-sweeper-vs-manual-drop-overlap`.)*
- **Dimension:** isolation/correctness
- **File:line:** `crates/uni/src/api/mod.rs:1282-1390` (`drop_fork`, no `name_lock`), `:1408+` (`drop_fork_cascade`, same); `crates/uni/src/api/fork.rs:83-84` (`build()` holds the per-name lock for its whole duration) vs `fork.rs:131-134` (holder registered only later, via `scope.rs:134`); `registry.rs:569-575` (begin_drop's only gate is `holder_count`, read against a counter the racing builder hasn't incremented yet); TTL sweeper trigger at `fork_maintenance.rs:62-63`.
- **Impact:** A concurrent `fork().build()` reads `status=Active` (`fork.rs:87-93`) and is mid-construction when `drop_fork` observes `holder_count==0`, tombstones, and `lance_branch::delete_branch` **force-deletes the Lance branches** (`mod.rs:1376-1377` → `lance_branch.rs:523-526`). The builder then returns `Ok(Session)` over deleted branches → subsequent `MATCH` errors or silently returns 0 rows. The TTL sweeper is the production trigger (re-opening a fork right at expiry races the sweep). Destroyed entity is the ephemeral fork's own branch, not primary data — hence high, not critical.
- **Root cause:** Asymmetry: create/open holds `registry.name_lock(name)`; drop does not. `build()` also never re-checks status after registering the holder.
- **Recommended fix:** Acquire `registry.name_lock(name)` in `drop_fork`/`drop_fork_cascade` across the `begin_drop → delete_branch → finish_drop` sequence (and around each per-node drop in cascade). Equivalent: flip status to Tombstoned under the cache lock first, and have `build()` re-check `status==Active` *after* `register_holder`, aborting+deregistering if it changed.
- **Guarding test:** `fork_drop_open_race.rs` (`multi_thread`, ~200 iters) — `tokio::join!` `fork("x").build()` against `drop_fork("x")`; whenever build returns `Ok(sess)`, `sess.query("MATCH ... RETURN count")` must return the seeded count, never error/0. Plus a TTL variant (short TTL + sweeper) asserting a re-opened session either reads correctly or fails fast with a typed lifecycle error, never a backend "branch not found".

#### H3. Fork WAL directory and id_allocator file are never deleted on drop or recovery (unbounded disk leak)
- **Dimension:** data-reliability (durability/hygiene)
- **File:line:** `crates/uni/src/api/mod.rs:1372-1389` (`drop_fork` deletes only Lance branches in `info.datasets`); `crates/uni-store/src/fork/registry.rs:594-604` (`finish_drop` deletes only registry entry + tombstone + schema overlay); `crates/uni-store/src/fork/recovery.rs:102-122` (`delete_all_branches` iterates only `info.datasets`). WAL lands at `wal_forks/{fork_id}/` (`writer_factory.rs:55-59`); allocator at `catalog/forks/{fork_id}/id_allocator.json` (`fork.rs:258`). `wal.rs:21-23` doc promises one-shot delete on drop; **unimplemented**. Note the allocator lives under `catalog/forks/` — a different prefix than the deleted overlay at `catalog/fork_schemas/`, so `delete_schema_overlay` does not incidentally cover it.
- **Impact:** Monotonic on-disk leak proportional to fork churn; defeats the budget cap's intent (dropped forks free the registry slot but leak disk). No correctness/collision risk (ForkId is a fresh ULID). *(Verdict corrected this from high to medium on impact; kept under High here because it shares a remediation phase with H2 and is the same drop-path defect. Treat as the lower-priority end of P1.)*
- **Recommended fix:** In `finish_drop` (or `drop_fork` just before it) and in recovery's Tombstoned/orphan paths, list-and-delete `uni_store::fork::wal::wal_prefix(&id)` and delete `uni_store::fork::id_alloc::id_allocator_path(&id)`. Best-effort (warn-on-error) and idempotent so recovery can re-run it.
- **Guarding test:** `fork_drop_disk_leak.rs` — create fork, commit a write (assert `wal_forks/{id}` and `id_allocator.json` exist via the object store), drop, assert both are gone. Second variant: crash mid-drop (tombstone written), reopen, assert recovery removes them too.

#### H4. Diff bucket silently overwrites distinct vertices/edges that hash to the same content UID (`ext_id` ignored)
- **Dimension:** data-reliability
- **File:line:** `crates/uni-fork/src/diff.rs:98-99` (`bucket.insert(uid, ..)`, `uid = compute_vertex_uid(label, None, &props)`), `:132` (edge equivalent); `crates/uni-store/src/storage/vertex.rs:63-88` (hashes label+ext_id+props, but diff passes `ext_id=None`). `ext_id` is stripped from user-facing properties (`write.rs:245`, `scan.rs:1536`), so two vertices differing only in `ext_id` collapse to one bucket entry.
- **Impact:** Empirically confirmed in the verdict: primary with `{ext_id:'a',name:'Same'}` and `{ext_id:'b',name:'Same'}` (count=2) collapses to one diff identity; deleting one on a fork yields `diff_fork_primary` reporting `added=0 deleted=0 changed=0` — the deletion is **silently lost**. Affects the advertised audit/merge workflow.
- **Root cause:** Diff UID is content-addressed with `ext_id` passed as `None`; `bucket.insert` overwrites with no collision check.
- **Recommended fix:** Include `ext_id` in the diff UID (extract it before it is stripped, or re-read the `ext_id` column). Independently harden `scan_label_nodes`/`scan_edge_type` to emit a `tracing::warn!` / `diff.collisions` counter when `bucket.insert` returns `Some(existing)`, so any residual identity collision is observable.
- **Guarding test:** `fork_diff_uid.rs::diff_does_not_collapse_distinct_ext_id_vertices` — seed two `ext_id`-distinct, otherwise-identical vertices (assert primary count=2); fork, delete one; assert `diff.vertices.deleted.len() == 1` (currently 0).

#### H5. No crash/reopen durability test exercises a fork that has WRITTEN and FLUSHED data
- **Dimension:** completeness *(verdict corrected to medium; the highest-value sub-test — WAL/allocator cleanup — exposes the real H3 leak, so it rides the same phase)*
- **File:line:** `crates/uni/tests/common/fork/fork_nested_recovery.rs` (covers only registry-state reconciliation); `fork_writes.rs`/`fork_unflushed_l0.rs` (in-process visibility, not durability across reopen).
- **Impact:** The three highest-value durability modes are uncovered: (a) primary snapshot pointer/HWM intact after a fork flush (guards C1), (b) fork WAL replays committed rows exactly once after commit-then-crash, (c) `wal_forks/{id}/` + allocator gone after drop (guards H3). Note (b) is *partly* covered at unit level (`recovery_fork_wal.rs::fork_wal_replay_restores_persisted_mutations`); only the end-to-end Cypher-commit + reopen path is missing.
- **Recommended fix / tests:** Add `fork_durability.rs` with the three tests above. These largely overlap with C1's and H3's guarding tests — implement once.

---

### MEDIUM

#### M1. `create_fork_2pc` holds no write barrier: concurrent parent commit between flush and branch read causes fork VID collisions and leaked post-fork-point data
*(Merges `fork-create-hwm-version-toctou-race`, the dead-metadata enabler `fork-snapshot-id-dead-metadata`, the HWM-bootstrap variant `fork-hwm-bootstrap-not-atomic-with-pending`, and the test-gap `fork-create-concurrent-writes-test-gap`.)*
- **Dimension:** isolation / data-reliability
- **File:line:** `crates/uni/src/api/fork.rs:208-211` (flush), `:215-220` (`parent_snapshot_id` captured but **never consulted** — 0 read-path consumers), `:252-257` (HWM read), `:398` (`lance_branch::current_version` reads the LIVE Lance tip, `lance_branch.rs:63-66`). No sustained lock spans these (`writer.rs:3419-3421` drops `flush_lock` inside `flush_to_l1`). `async_flush_enabled` defaults TRUE (`config.rs:663-669`; the `:539` doc comment claiming false is stale).
- **Impact:** With a concurrent parent commit crossing the flush threshold in the window, the fork (a) branches off a version containing **post-fork-point** parent rows (isolation breach vs the `fork.rs:197-200` contract) and (b) bootstraps its allocator to a stale HWM, so fork writes collide with base_paths rows and Lance read-merge shadows them. `parent_snapshot_id` is dead metadata that masks the missing pin. Per-dataset version skew also exists (different datasets read at different instants, `fork.rs:345-415`).
- **Root cause:** Fork-point capture (flush + HWM + per-dataset versions) is not atomic w.r.t. parent flushes; the operative fork-point is the live version, not the captured snapshot id.
- **Recommended fix:** Capture HWM and **all** per-dataset versions under one held `flush_lock`, then pass explicit versions into `create_branch` instead of re-reading `current_version` inside `build_datasets_for_fork`. This removes the race, the per-dataset skew, AND unifies `parent_snapshot_id` with reality (`SnapshotManifest.EntitySnapshot.lance_version` already carries the needed per-dataset versions).
- **Guarding test:** `fork_create_concurrent_writes.rs` (`multi_thread`, low `auto_flush_threshold`) — tight parent write+commit loop concurrent with `session.fork(name)`; assert (A) fork sees no parent rows committed after `fork()` returned, (B) a fork write survives (no VID shadowing). Loop 50× under both `async_flush_enabled` true/false.

#### M2. Fork WAL replay always uses HWM 0 with no per-fork snapshot gate → interrupted truncation double-applies flushed mutations
- **Dimension:** data-reliability (durability)
- **File:line:** `crates/uni/src/api/mod.rs:971-974` (`replay_wal(0)` unconditional) vs `:3385` (primary gates on `wal_high_water_mark`); truncate at `crates/uni-store/src/runtime/writer.rs:4466-4475` (last durable step, after branch write + publish); `runtime/wal.rs:507-550` (per-segment delete loop — partial completion possible).
- **Impact:** Crash between durable branch write and complete truncate → surviving pre-flush segments replayed from 0 on top of branch data that already contains them. Mostly idempotent for pure inserts (L0 shadows), but (a) the next fork flush re-Appends the rows (count inflation) and (b) CRDT/version state diverges (double-merge, resurrected/re-tombstoned rows). Depends on C1's fix (a per-fork manifest is the natural home for the per-fork HWM).
- **Root cause:** No per-fork `wal_high_water_mark`; correctness relies entirely on truncation completing.
- **Recommended fix:** Persist a per-fork `wal_high_water_mark` (part of the per-fork snapshot/state from C1) and pass it to `replay_wal` instead of 0.
- **Guarding test:** `fork_flush_replay_idempotent.rs` (failpoints, `#[ignore]`, `--test-threads=1`) — inject crash via `flush::after-complete-before-cache-clear` (between `complete_flush` and `truncate_before`); reopen fork; assert exact-once count, single-apply CRDT counter, deleted row stays deleted; trigger a second flush and re-assert count (catches Append-duplication).

#### M3. `drop_fork` deletes the recovery tombstone even when `delete_branch` failed → permanently leaks Lance branches
- **Dimension:** data-reliability
- **File:line:** `crates/uni/src/api/mod.rs:1376-1384` (branch-delete loop warns-and-continues on error), `:1388` (unconditional `finish_drop`); `registry.rs:594-604` (`finish_drop` removes registry entry + tombstone + overlay). Tombstone is the only recovery anchor (`recovery.rs:65-94`). Fault hook `UNI_FORK_INJECT_FAIL_DELETE_AFTER` exists.
- **Impact:** On a transient object-store error during branch delete, the entry vanishes with the branch still on disk → permanent unreachable leak; possible later `create_branch` collision on the same name. Recoverable-by-design path that the code fails to recover.
- **Recommended fix:** Track per-branch delete success; if any failed, do NOT call `finish_drop` — leave the fork Tombstoned with its tombstone present so `recover_forks` retries `delete_all_branches` on next boot, and/or return a typed incomplete-drop error.
- **Guarding test:** `fork_drop_recovery.rs` — arm `UNI_FORK_INJECT_FAIL_DELETE_AFTER=0`, `drop_fork`; assert tombstone file + Tombstoned entry + branch still present; clear env, `recover_forks`, assert branch + tombstone now gone.

#### M4. Promote has no conflict detection, lost-update protection, or deletion semantics
- **Dimension:** completeness
- **File:line:** `crates/uni-fork/src/diff.rs:518-535` (vertex path: only `to_insert` vs `vertices_skipped_uid_conflict`, no update branch), `:696-723` (edge mirror); `:500` (`compute_vertex_uid(label, None, ..)` keys purely by content, `ext_id` always None); `types.rs:366-391` (`PromoteReport` has no update/delete/conflict counter). API doc `mod.rs:1555-1568`.
- **Impact:** A fork edit to an existing vertex (→ new content UID) is inserted as a **twin**, not an upsert; no lost-update warning; fork deletions never propagate. Silent wrong results for the advertised "publish fork changes to primary" workflow. No destructive loss (primary rows never dropped), hence medium.
- **Recommended fix:** Document the additive-only contract prominently; for a real merge add (a) an `ext_id`-keyed upsert path and (b) optional delete-promotion driven by a diff `deleted` set; surface unresolved conflicts in `PromoteReport` (e.g. `vertices_conflicting`).
- **Guarding test:** `fork_promote.rs::promote_edit_creates_twin_documents_additive_only` — fork edits a vertex, promote, assert both old+new coexist + `vertices_inserted==1` (locks current contract; flip to upsert assertions when implemented).

#### M5. `batch_resolve_primary_vids` swallows all errors → transient failure causes duplicate inserts
- **Dimension:** data-reliability
- **File:line:** `crates/uni-fork/src/diff.rs:307-313`, `:330-333` (every error path `return out` with a partial/empty map); `:518-524` (absence interpreted as "insert"); whole promote commits in one tx (`mod.rs:1641-1643`).
- **Impact:** A transient UidIndex open / Lance scan / primary round-trip failure during dedup silently turns into committed duplicate vertices, with only an inflated `vertices_inserted` count and no error/warn.
- **Recommended fix:** Distinguish "confirmed absent" from "could not determine" (tri-state or propagate error) so `run_promote` aborts the tx; or at minimum a `vertices_inserted_unverified` counter + `tracing::warn!`.
- **Guarding test:** Inject a resolve failure (corrupt/remove the label's UidIndex) and assert promote errors or reports unverified inserts — not a silent successful duplicate.

#### M6. Branch `vector_search`/`full_text_search` drop the filter (deleted rows + ignored predicates/HWM on forks)
- **Dimension:** data-reliability *(verdict corrected high→medium: gated behind the fork-local-index/fork-write path)*
- **File:line:** `crates/uni-store/src/backend/branched.rs:278` (`let _ = (metric, filter);`), `:307` (`let _ = filter;`); `lance_branch.rs:273-297`/`:307-337` (no filter arg); caller builds `combined_filter` always including `_deleted = false` (`manager.rs:1701-1714`, `build_active_filter` `:652-657`); `extract_vid_score_pairs` does no `_deleted` re-filter.
- **Impact:** On a fork with a branch for the target table, kNN/FTS ignores user `WHERE` predicates, ignores the version HWM pin, and surfaces soft-deleted vertices (most robustly: a vertex soft-deleted on the parent *before* forking, inherited via base_paths). Diverges from primary semantics.
- **Recommended fix:** Thread the `FilterExpr` into `vector_search_on_branch`/`full_text_search_on_branch` (`scanner.filter(sql)` before `nearest()`/`full_text_search()`, mirroring `lance.rs:237-247`). If pushing the predicate through the indexed scan is infeasible, post-filter by `_deleted` + re-evaluate the predicate in `extract_vid_score_pairs` and over-fetch k.
- **Guarding test:** `fork_index_vector.rs`/`fork_index_bm25.rs` — soft-delete the top-k match on **primary**, fork, build the fork-local index, assert the deleted vid is NOT returned by the fork query; assert a user predicate and the HWM pin are honored.

#### M7. Fork inherits parent vector/FTS index but never auto-builds a fork-local one
- **Dimension:** completeness
- **File:line:** `crates/uni/src/api/fork_maintenance.rs:97-103` (collects only single-column `IndexDefinition::Scalar`), `:155` (only `ForkLocalIndexKind::ScalarBtree`); `index_builder.rs:109-152` (Vector/FullText only via manual `build_fork_local_index`). Default `disable_fork_index_builder=false` (`config.rs:678`).
- **Impact:** Fork rows written after branch-creation are unindexed. For **FTS/BM25** there is generally no brute-force fallback → fork-local matches can be **silently omitted** (correctness). For vector, Lance flat-scans + merges (degraded recall/perf, not loss).
- **Recommended fix:** Extend `index_builder_tick` to also collect `IndexDefinition::Vector`/`FullText` and schedule the matching `ForkLocalIndexKind` past the fragment threshold, mirroring the ScalarBtree branch.
- **Guarding test:** `fork_index_bm25.rs::fork_local_fts_inherited_index_misses_new_rows` — parent FTS index + seed; fork writes a new matching doc; do NOT build a fork-local index; assert the fork query returns both parent and fork-local matches. Mirror for vector. Use default config so the scalar-only auto-builder path is exercised.

#### M8. `SetVertexLabels` (label-only) mutation is not reflected in the index/datasets at flush across flush windows
- **Dimension:** correctness *(verdict corrected low→medium)*
- **File:line:** `crates/uni-store/src/runtime/writer.rs:3938-3943` (VidLabelsIndex maintenance keyed strictly off `main_vertices`, derived from `vertex_properties`); SET/REMOVE label path (`uni-query/write.rs:3362`, `:3821`) calls only `l0.set_vertex_labels`, which never adds the vid to `vertex_properties`; `vertex_label_overwrites` is referenced in `writer.rs` only at the WAL append (`:943`), never in the L1 flush block.
- **Impact:** A pure label change to a vid that was flushed in a **prior** window is, after its own flush: not in the in-memory index, not written to per-label Lance datasets, not in the main vertex table. Durable label mutation is **silently lost** and survives restart (`rebuild_vid_labels_index` reads the stale main-table labels column). The common same-window create+relabel case works, which is why it wasn't caught.
- **Recommended fix:** Add a `vertex_label_overwrites`-driven pass in the flush that re-derives labels into `main_vertices`/main-table/per-label datasets and calls `update_vid_labels_index`, fetching existing props from storage for overwrite-only vids.
- **Guarding test:** `label_only_mutation_survives_flush.rs` (single session, two flush windows) — `CREATE (:A)`, commit, flush; then `SET n:B REMOVE n:A`, commit, flush; assert `get_labels_from_index(vid)==["B"]`, `MATCH (n:B)` == 1, `MATCH (n:A)` == 0; close+reopen and re-assert.

#### M9. TTL sweeper / manual drop / double-drop are unsynchronized
*(Merges `fork-double-drop-window` and the sweeper-overlap portion of `fork-sweeper-vs-manual-drop-overlap`; the use-after-drop core is H2.)*
- **Dimension:** correctness/isolation *(verdict on `fork-double-drop-window` corrected to low)*
- **File:line:** `crates/uni-store/src/fork/registry.rs:557-588` (`begin_drop` reads status, releases lock, holder-check + tombstone PUT outside lock, re-locks and flips status with no `Active` precondition); no `name_lock` in any drop path.
- **Impact:** Two concurrent `drop_fork(name)` (manual racing the TTL sweeper) both pass the Active-snapshot checks → double `delete_branch` (cosmetic warn — `delete_branch` is idempotent) + double `finish_drop` (idempotent). The genuinely harmful narrow path: a stale drop's `finish_drop` removes a freshly-recreated same-named fork's entry, orphaning the new fork's branches/overlay.
- **Recommended fix:** Same `name_lock` fix as H2 covers this; additionally make `begin_drop` atomic+idempotent (holder-check, `Active` precondition, status flip in one cache-lock section before the tombstone PUT); return a distinct "already dropping" signal.
- **Guarding test:** Concurrent `drop_fork` + `drop_fork_cascade` (+ a `fork(name).build()`) on the same fork; assert exactly one does real work, the other returns Ok/NotFound, and the registry ends in a single consistent state.

---

### LOW

These are real but bounded; batch into a hardening sweep.

- **L1 — Fork drop is timing-dependent (`UniForkInUseError` under CPU starvation).** *(Merges `fork-inuse-after-release-bounded-poll` and `iso-drop-fork-holder-starvation` — the same `wait_for_holders_drained` ~800ms bounded poll, `mod.rs:1238-1255`, return value discarded at `:1363`, then `begin_drop` hard-fails at `registry.rs:569-575`.)* On the user-released-session path, `weak.upgrade()` returns None so the deterministic `coord.shutdown()` block (`mod.rs:1309-1344`) is skipped; only the poll guards correctness, and the orphan FlushCoordinator finalizer can be starved past 800ms under pytest-xdist → the reported #99 intermittent flake. **Fix:** make drop deterministic — track the finalizer `JoinHandle` and await it on all paths, or release `ForkHolderGuard` on `Session` drop rather than on storage Arc reaching zero; at minimum scale the poll by `config.drop_fork_drain_timeout` (which exists at `config.rs:553` but is only used in the upgrade-succeeded branch). **Test:** saturate the runtime with busy tasks, release a fork session that did an async flush, then `drop_fork` in a loop and assert never `ForkInUse`.

- **L2 — Shared `vid_labels_index` Arc also leaks into `pinned`/`pinned_at_version` (time-travel/tx-version-pinned clones).** `manager.rs:468`, `:504`. A relabel+reflush after a snapshot poisons the pinned reader's label resolution AND steers property fetch to the wrong label dataset (`property_manager.rs:391-408`). Same deep-copy fix as H1, applied to the pin constructors. **Test:** snapshot S, relabel+reflush a vid, read at S, assert old labels + correct props.

- **L3 — Partial `create_branch` failure leaks orphaned ("zombie") Lance branches.** *(Merges `fork-create-orphan-branch-leak`, `fork-recovery-orphan-zombie-branches`, and the test-gap `fork-create-orphan-branch-test-gap`.)* `fork.rs:276-288` rolls back only the registry entry; `recovery.rs:134` no-ops because a Pending entry's `datasets` map is empty. The existing `recovery_fork_create_fault.rs:106-122` *documents* the leak and manually reclaims it. **Fix:** force-delete created branches on the in-process error path (track names locally); in recovery, reconstruct `fork_{fork_id}_{dataset}` for each candidate dataset and force-delete on Pending rollback. **Test:** `UNI_FORK_INJECT_FAIL_AFTER=1`, assert no `fork_{id}_*` branch survives in-process and after reopen; invert `recovery_fork_create_fault.rs:114-122` from "assert zombie remains" to "assert no zombie remains".

- **L4 — `name_locks` and `holders` DashMaps grow without bound.** *(Merges `fork-namelock-map-unbounded-growth` and `sec-namelocks-holders-leak`.)* `registry.rs:60`/`:63`, inserted at `:264-270`/`:273-281`, never removed (`finish_drop` only touches `cache.forks`). `holders` is keyed by fresh ULID so it grows even under same-name churn. **Fix:** reap both in `finish_drop`/`rollback_create` (guard `name_locks` removal on Arc strong_count) or in the TTL sweeper. **Test:** churn M distinct forks, assert map sizes stay bounded.

- **L5 — No fork-name validation.** *(Merges `fork-name-no-validation` and `sec-no-fork-name-validation`.)* Empty/whitespace/unbounded/control-char names flow straight into the registry BTreeMap key + whole-file JSON rewrite (`session.rs:353`, `fork.rs:41-48`, `registry.rs:492`, `put_registry` `:303-318`). Contrast snapshots, which reject empty (`api/mod.rs:2514`). Not path-traversal (paths use the ULID). **Fix:** validate at `ForkBuilder::build` *before* `name_lock` acquisition; new `UniError::ForkNameInvalid`. **Test:** `fork("")`, `fork("   ")`, very-long, embedded `\n`/`\0` all error and create no entry.

- **L6 — User-controlled label/edge-type names break fork creation.** `sec-invalid-label-breaks-branch-create`. Backtick-escaped labels (`cypher.pest:135` allows ANY non-backtick char) with spaces/slashes flow into `fork_{fork_id}_{dataset_name}` (`fork.rs:351`); Lance `check_valid_branch` rejects them → every fork on that DB fails mid-loop with partial residue. **Fix:** constrain label/edge-type names at schema-definition time to the branch-safe charset, OR deterministically encode/hash `dataset_name` into the branch name; pre-validate at fork-create and fail fast before `begin_create`. **Test:** `CREATE (n:`bad label`)` then `fork()` → clear error + no residue.

- **L7 — Fork-local and primary label/edge-type IDs allocated independently can collide.** `schema.rs:1216`/`:1259` (`max(existing)+1` in each view). Latent today (promote re-inserts by name); a footgun for any future id-trusting path. **Fix:** allocate fork-local ids from a reserved disjoint range, or document/enforce that fork-origin ids are never trusted outside the fork view. **Test:** fork adds label X, primary later adds Y, assert `X.id != Y.id`.

- **L8 — Adjacency factory key mismatch + dead direct-open URI.** `fork-adjacency-branch-key-mismatch`. `manager.rs:1602` builds `adjacency_{direction}_{edge_type}_{label}`; registered/canonical is `adjacency_{edge_type}_{direction}` (`fork.rs:334-335`, `table_names.rs:30-32`), so `fork_branch_for` for adjacency always returns None; `AdjacencyDataset::new`'s `{base}/adjacency/{dir}_{et}_{label}` URI is never written. Dead/misleading code (live reads use the correct backend route), no runtime bug today. **Fix:** align the factory key to `table_names::adjacency_table_name`, fix or delete the direct-open machinery, add a drift-guard assertion. **Test:** assert `adjacency_dataset(..).table_name() == adjacency_table_name(..)` and round-trip fork branch resolution.

- **L9 — `tag_fork`/`untag_fork` are not atomic across datasets.** `mod.rs:1664-1679`. A mid-loop `create_tag` failure (RefConflict/IO) leaves the fork half-tagged; the retention/regulatory-hold guarantee is partial. **Fix:** pre-validate + pre-check conflicts across all datasets, best-effort delete already-created tags on failure. **Test:** pre-create the namespaced tag on one dataset, call `tag_fork`, assert it errors AND no partial tags remain.

- **L10 — Promote dedup test is too lenient (`rows.len() <= 2`).** `fork_promote.rs:90-117`. Passes even if dedup is fully broken; this is how H4 went unnoticed. Note: re-promote is idempotent only **after a flush** (the verdict empirically refuted the original "inserted=0 without flush" claim). **Fix:** exact-count assertions + a re-promote-after-flush idempotence assertion + the `ext_id` case.

- **L11 — Forks unbounded / never expire by default.** `sec-unbounded-forks-default`. `max_forks: None`, `fork_default_ttl: None` (`config.rs:672-673`); enforcement is opt-in (`registry.rs:485-490`). For an **embedded** DB this is a defaults-hardening gap, not a remote DoS. **Fix:** ship a safe non-None default and/or document loudly that production must set `max_forks`; consider bounding total branch count (one fork's branches scale with schema size). **Test:** with `max_forks: Some(N)`, the N+1-th create returns `ForkBudgetExceeded`; a defaults-regression test pinning the documented `None`.

- **L12 — `VertexDiff.changed` is unreachable dead code documented as a feature.** `diff.rs:201-210`, `types.rs:72`. Content-addressed identity means same-UID rows have identical props, so `changed` is never populated; the doc claims otherwise. Cosmetic. **Fix:** annotate `VertexDiff.changed`/`EdgeDiff.changed` as reserved/never-populated; drop the dead `property_changes` call. **Test:** regression asserting a property mutation surfaces as add+delete with `changed.is_empty()`.

- **L13 — Fork IdAllocator uses unchecked u64 addition.** `id_allocator.rs:90,105-107,114`. Physically unreachable (~1.8e19 allocations) and not attacker-controlled; debug panics, release wraps. Defense-in-depth only. **Fix:** `checked_add`/`saturating_add` + typed exhaustion error. **Test:** construct an allocator with `next_*_batch` near `u64::MAX` and assert clean exhaustion error.

- **L14 — Custom functions/plugins are shared (not isolated) across primary and all forks.** `sec-fork-udf-shared-registry`. `mod.rs:838` Arc-clones `custom_functions`; intentional and tested (`fork_custom_functions.rs`), but the public fork docs market forks as "isolated"/"sandbox" (`forks.md:8,143,256`) without the carve-out. Registering a UDF already requires in-process Rust/Python access, so impact is bounded. **Fix:** documentation — add an explicit carve-out that custom functions/plugins are Uni-level and forks are NOT a code/privilege sandbox. **Test:** a docs-contract test asserting `forks.md` contains the carve-out.

---

## 3. Test-Coverage Gap Matrix

Legend: **OK** = well-covered · **partial** = some coverage, key case missing · **GAP** = uncovered (finding flags it) · — = N/A.

| Lifecycle ↓ / Dimension → | correctness | completeness | data-reliability | security | isolation |
|---|---|---|---|---|---|
| **create (2PC)** | partial | GAP (M1) | partial | partial (L6) | GAP (M1) |
| **read / isolation** | OK | OK | OK | — | partial (H1, L2) |
| **write** | OK | OK | partial (C1) | — | GAP (H1) |
| **drop / cascade** | partial (M9) | GAP (L4) | GAP (M3) | partial (L5) | GAP (H2) |
| **ttl / sweep** | partial | partial | — | partial (L11) | GAP (H2/M9) |
| **recovery / crash** | partial | GAP (H5) | GAP (C1, M2, H3) | — | partial |
| **diff** | OK | partial (L12) | GAP (H4) | — | — |
| **promote** | partial (L10) | GAP (M4) | GAP (M5) | — | partial (L7) |
| **schema-evolution** | OK | OK | — | partial (L6) | GAP (L7) |
| **index-inheritance** | partial (M8) | GAP (M7) | GAP (M6) | — | GAP (H1) |

**Concrete new tests to add for the gaps** (file → what it asserts):

1. `fork/fork_flush_global_pointer_isolation.rs` — C1: primary version-HWM monotonicity after a fork flush + reopen.
2. `fork/fork_parent_index_isolation.rs` — H1 + test-gap-parent-index-isolation: parent label resolution unchanged after fork delete/relabel/create + flush; sibling-fork isolation.
3. `fork/fork_drop_open_race.rs` — H2: concurrent build vs drop, build never returns a session over a deleted branch; TTL-reopen variant.
4. `fork/fork_drop_disk_leak.rs` — H3 + H5(c): `wal_forks/{id}/` + allocator gone after drop and after crash-mid-drop reopen.
5. `fork/fork_durability.rs` — H5(a/b): primary snapshot intact after fork flush; fork committed rows survive crash-reopen exactly once.
6. `fork/fork_create_concurrent_writes.rs` — M1: parent write loop racing `fork()`; no post-fork-point leakage, no VID shadowing; both flush modes.
7. `fork/fork_flush_replay_idempotent.rs` (failpoints) — M2: interrupted truncate → exact-once on reopen + after a second flush.
8. `fork/fork_drop_recovery.rs` — M3: `delete_branch` failure leaves fork Tombstoned + recoverable.
9. `fork/fork_diff_uid.rs` — H4: `ext_id`-distinct vertices not collapsed; deletion reported.
10. `fork/fork_promote.rs` additions — M4 (twin/additive contract), M5 (resolve-failure → no silent dup), L10 (exact counts + idempotence-after-flush).
11. `fork/fork_index_bm25.rs` / `fork_index_vector.rs` additions — M6 (filter honored, no deleted/HWM leak), M7 (auto-build covers new fork rows).
12. `index/label_only_mutation_survives_flush.rs` — M8: cross-flush-window label change persists + survives reopen.
13. `fork/fork_invalid_label_name.rs` (L6), `fork_validation.rs` (L5), `fork_schema_evolution.rs` id-collision guard (L7), `fork_tag.rs` partial-failure atomicity (L9).

---

## 4. Prioritized Remediation Plan

### P0 — Must fix before production (primary-data integrity)
- **C1** Fork-scoped snapshot namespace (stop fork flush poisoning global `catalog/latest`). Brings tests #1 and #5(a).
- **M2** Per-fork `wal_high_water_mark` gate for `replay_wal` (naturally lands with C1's per-fork manifest). Test #7.
- **Effort:** ~3–5 days (C1 is the load-bearing change; design the per-fork manifest once, M2 rides it).

### P1 — High (isolation + lifecycle safety)
- **H1 + L2** Deep-copy `vid_labels_index` in `at_fork_with_schema`, `pinned`, `pinned_at_version` (one-line-each change + tests #2). ~1 day incl. tests.
- **H2 + M9** Hold `registry.name_lock` across drop/cascade; make `begin_drop` atomic+idempotent; have `build()` re-check status after `register_holder`. Test #3. ~2 days.
- **H3 + M3** Drop-path cleanup: delete fork WAL prefix + allocator; don't delete tombstone when branch-delete failed. Tests #4, #8. ~1–2 days.
- **H4** Include `ext_id` in diff UID + collision warn. Test #9. ~1 day.
- **H5** Implement the durability test suite (mostly overlaps #1/#4 above). ~0.5 day net.
- **Effort total:** ~5–7 days.

### P2 — Medium (correctness/completeness gaps)
- **M1** Atomic fork-point capture under one `flush_lock` (HWM + per-dataset versions), pass explicit versions into `create_branch`; unify `parent_snapshot_id`. Test #6. ~2 days.
- **M8** `vertex_label_overwrites`-driven flush pass. Test #12. ~1–2 days.
- **M6** Thread filter into branch vector/FTS search. Test #11a. ~1 day.
- **M7** Auto-build fork-local vector/FTS indexes. Test #11b. ~1–2 days.
- **M4 + M5 + L10** Promote merge semantics (upsert/delete/conflict counters) + resolve-error propagation + tighten dedup test. Test #10. ~2–3 days (M4 is the larger piece; ship the doc + tests first if time-boxed).
- **Effort total:** ~7–10 days.

### P3 — Low (hardening sweep, batchable)
- **L4** reap `name_locks`/`holders`; **L1** deterministic drop drain; **L5/L6** name + label validation; **L3** zombie-branch recovery; **L7** fork-id range; **L8** adjacency key cleanup; **L9** tag atomicity; **L11** safe `max_forks` default + docs; **L12/L13** dead-code/overflow nits; **L14** docs carve-out. Tests #13. 
- **Effort:** ~3–5 days as a single sweep.

---

## 5. Open Questions / Human Decisions

1. **C1 fix strategy:** per-fork `SnapshotManager` (clean isolation, more plumbing) vs fork-writer skips snapshot publish + per-fork manifest (smaller diff, but fork reopen needs its own HWM source). Recommend the per-fork manifest, since M2's per-fork WAL HWM wants to live there too — but it's an architectural call.
2. **Fork-point semantics under concurrent parent writes (M1):** Should fork creation block parent flushes for the capture window, or is a brief `flush_lock` hold acceptable? The current design explicitly does NOT block primary (`fork_no_primary_blocking.rs`); option (b) (capture under one held lock) is a momentary hold, not a sustained barrier — confirm that's acceptable.
3. **Promote contract (M4):** Is promote intended to be a true merge (upsert + delete-propagation + conflict surfacing) or deliberately additive-only? If additive-only is the product decision, this drops to a docs+counter task; if merge, it's a multi-day feature with `ext_id`-keyed identity.
4. **`max_forks` default (L11):** For an embedded DB, ship a non-None default (e.g. a few hundred/thousand) or keep `None` + loud docs? Affects out-of-the-box behavior of existing embedders.
5. **Custom-function/plugin isolation (L14):** Confirm Uni-level sharing is the intended contract (then it's purely a docs fix), or is a per-fork registry overlay wanted for sandbox use cases?
6. **Label-name charset (L6):** Constrain at schema-definition time (breaking for any existing odd-named labels) vs encode/hash into branch names (non-breaking but changes on-disk branch naming)? Recommend encode/hash to avoid a breaking schema change.

Files cited throughout are absolute under `/home/rohit/work/dragonscale/uni/`. The fork subsystem is well-architected on the happy path; the work above is concentrated, well-scoped, and dominated by one root pattern (share-by-Arc vs copy-per-fork) plus the drop-path lock asymmetry.