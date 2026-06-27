# Sparse (#95) & Multi-vector (#96) Implementation Review

**Date:** 2026-06-27  
**Scope:** Completeness, correctness, safety, reliability, concurrency, and test-coverage of the sparse-vector (issue #95) and multi-vector / ColBERT-MaxSim / MUVERA (issue #96) features.  
**Method:** 12-reviewer dynamic workflow (2 features × 6 dimensions), each finding independently re-verified by an adversarial agent that re-read the cited code. 44 agents total.  
**Result:** 32 findings, **all confirmed**, 0 refuted, 0 uncertain. Verifiers down-graded 5 severities (no upgrades).

## Severity summary (post-verification)

| Severity | Count |
|---|---|
| critical | 1 |
| high | 6 |
| medium | 12 |
| low | 12 |
| info | 1 |

## How to read this

The 32 findings collapse into ~7 root causes; the top two account for 11 of them. The **Root causes** section below is the actionable view. The **Full findings** appendix lists every finding verbatim with file:line, evidence, suggested fix, and the verifier's independent reasoning.

Several agents independently chose overlapping IDs (e.g. `SP-REL-1` appears for multiple feature/dimension pairs). Findings are therefore keyed as `feature/dimension/id` throughout.

---

## Remediation status (2026-06-27)

All root causes RC1–RC6 plus the headline test gap and the low-severity correctness items are **fixed**, with regression tests. `cargo fmt`/`clippy` clean on the touched crates; 150 sparse/multivector/muvera tests + 408 fork/index/backend tests green (no regressions).

| Root cause | Status | Fix summary | Tests |
|---|---|---|---|
| **RC1** Unserialized Lance `Overwrite` data loss (`SP-REL-1` crit, `SP-REL-2`, `SP-CONC-1`, `SP-CONC-2`) | ✅ Fixed | New `StorageBackend::lock_table_for_write` owned guard. MUVERA backfill holds the per-table write lock across scan→splice→`replace_table_atomic` (closes the TOCTOU vs flush append). Sparse/inverted posting writers serialize create-backfill vs flush-update on the postings dataset path via the same keyed mutex. | `lock_table_for_write_provides_mutual_exclusion`; existing backfill suite |
| **RC2** Unvalidated `Value::SparseVector` → WAL panic (`SP-CORR-1`, `SP-SAFE-1/2/3`, `SP-COMP-2`, `SP-TG-2`) | ✅ Fixed | Ingest validation/canonicalization in `coerce_and_validate_property_value` (sort+sum via `from_pairs`, reject non-finite / length-mismatch / out-of-range term). WAL codec `encode` made non-panicking (defensive canonicalize). Arrow native-arm length guard. Read-side non-finite skip. | `sparse_malformed_value_is_rejected_not_panicked`, `encode_canonicalizes_non_canonical_sparse_without_panicking` |
| **RC3** FDE param integer overflow (`SP-SAFE-1` mv) | ✅ Fixed | `MAX_REPS`/`MAX_PROJ_DIM` bounds before the product; `fde_dim` uses `checked_mul` (saturating); `buckets` uses `checked_shl`. | `validate_rejects_overflowing_reps_and_d_proj_without_panicking` |
| **RC4** Wrong-dim token wedges flush (`SP-REL-1` mv) | ✅ Fixed | `materialize_fde_columns` skips a malformed row (leaves FDE NULL, structured warn) instead of aborting the flush. | `muvera_wrong_dim_token_does_not_wedge_flush` |
| **RC5** Stale postings on update-reflush (`SP-CORR-2`/`SP-REL-2` sparse) | ✅ Fixed | Updated vid is unioned into `removed` so prior postings are purged before re-append (remove-then-add). | existing update/reflush suite |
| **RC6** Fail-open `table_exists().unwrap_or(false)` (`SP-REL-2` mv, `SP-REL-3` sparse) | ✅ Fixed | The four vector-search `table_exists` sites + the two backfill sites now propagate `Err` and only treat `Ok(false)` as "nothing flushed". (Dense `vector_search` shares the pattern but is pre-existing/out-of-scope — noted, not changed.) | covered by existing suite |
| `SP-CORR-3` non-deterministic top-k ties | ✅ Fixed | Ascending-vid tie-break added to all four rerank sorts, matching the DAAT `HeapEntry` path. | — |
| `SP-REL-4` ignored `uni.sparse.query` filter arg | ✅ Fixed | A non-null `filter` now returns a clear "not yet supported" error instead of being silently dropped. | — |
| **SP-TG-1** recall@10 printed-but-never-asserted | ✅ Fixed | Recall@10 == 1.0 asserted against the f64 oracle over a 2000-doc overlap corpus, both quantized and lossless. | `sparse_recall_at_10_is_perfect_on_overlap_corpus` |

**Consciously deferred** (not bugs / out of scope): `SP-COMP-1` (inline `max_sim` scalar never exposed), `SP-COMPL-1` (MUVERA real-corpus recall bench unrun), `SP-COMPL-2` (OGM `hybrid_search` → #114), `SP-CONC-3` (SSI phantom, info). Lower-priority residual test gaps `SP-TG-3/4/5/6` and the multivec crash-recovery test remain open; the highest-value gaps (recall, malformed-input, flush-wedge, lock serialization) are now covered.

---

## Root causes (actionable)

### 🔴 RC1 — Unserialized Lance `Overwrite` → silent data loss

`replace_table_atomic` (`crates/uni-store/src/backend/lance.rs:550`) is the **only** backend write method that does not take the per-table `write_lock_for`; `insert`/`merge_insert`/`create_table` all do, and the code comment documents that the lock exists *because* concurrent appends caused data loss. The new vector backfills ride on this gap.

- **`multivec/concurrency/SP-REL-1` (CRITICAL)** — MUVERA FDE backfill does `scan(all) → replace_table_atomic(Overwrite)` over the **primary vertex table** while holding no flush lock. A concurrent flush appends committed rows between scan and overwrite; the overwrite replaces the table with only pre-scan rows → **durable loss of committed data**. Reachable whenever `CREATE VECTOR INDEX type:'muvera'` runs during active ingest.
- **`multivec/concurrency/SP-REL-2` (HIGH)** — the index-rebuild path forces the same backfill and auto-fires after recovery/bulk-load when async flushes are active.
- **`sparse/concurrency/SP-CONC-1` (HIGH)** — sparse postings use `WriteMode::Overwrite` (last-writer-wins); DDL-create-backfill and flush-incremental-update Overwrite the same path with no shared lock → lost update → a vid's posting silently vanishes from candidates.
- **`sparse/concurrency/SP-CONC-2` (MEDIUM)** — identical pattern in the set-membership inverted index.

**Fix:** make `replace_table_atomic` acquire `write_lock_for`; hold the writer's `flush_lock` (or a per-index mutex) across the whole scan→replace and the posting load-modify-write; prefer a merge-by-`_vid` over a blind `Overwrite`.

### 🔴 RC2 — No ingest-time validation of `Value::SparseVector` invariants

`Value::SparseVector` is a plain public struct; its "strictly ascending, finite, equal-length" invariant is doc-only. Schema `accepts()` checks only the variant, so a directly-constructed bad value (Rust/Python `.param`) reaches the WAL encoder, which does `SparseVector::new(...).expect("invalid SparseVector value")` → **panic on the commit/WAL-write thread**. Three independent agents found this same panic from three lenses.

- `sparse/correctness/SP-CORR-1`, `sparse/safety/SP-SAFE-1`, `sparse/reliability/SP-REL-1` — the panic itself.
- `sparse/safety/SP-SAFE-2` — NaN weight poisons the min-heap top-k (`partial_cmp().unwrap_or(Equal)` makes NaN compare equal to everything).
- `sparse/safety/SP-SAFE-3` — length-mismatched value desyncs the Arrow indices/values child lists, later silently `.zip()`-truncated.
- `sparse/completeness/SP-COMP-2`, `sparse/test-gap/SP-TG-2` — read-path and test manifestations.

**Fix:** validate once in `coerce_and_validate_property_value` for `DataType::SparseVector` (round-trip through `SparseVector::from_pairs`, reject with `TypeError`); make the codec `.expect` a propagated error as defense-in-depth. Neutralizes ~6 findings.

### 🟠 RC3 — FDE param integer overflow (`multivec/safety/SP-SAFE-1`, HIGH)
`fde_dim() = reps * buckets * proj_dim` is unchecked; user `reps`/`d_proj` (u32) are unbounded → panics the absurd-config guard in debug builds, or wraps *under* it in release → ~2.1B-iteration allocation DoS. Concrete bypass values confirmed. Precondition: CREATE-INDEX privilege. **Fix:** `checked_mul` + bound `reps`/`d_proj` before the product.

### 🟠 RC4 — Wrong-dim multivector token wedges every flush (`multivec/reliability/SP-REL-1`, HIGH)
FDE `encode_doc` errors hard on a dimension-mismatched token and aborts the flush; the normal column path *coerces* the same token (`valid=false`). The rotated L0 buffer stays stuck on `pending_flush`, L0 grows unbounded → writes wedged for that label. **Fix:** make `materialize_fde_columns` tolerant (skip/null the row) to match the column builder, or add a write-time dimension guard so both paths agree.

### 🟡 RC5 — Stale posting accumulation on update-reflush (`sparse/correctness/SP-CORR-2` = `sparse/reliability/SP-REL-2`, MEDIUM)
On a flushed in-place update, the vid enters `added` but never `removed`, and postings are a `Vec` (no dedup, unlike the inverted index's `HashSet`). Old+new postings both persist → unbounded growth on hot-updated docs + double-counted advisory `query_topk` scores. Production query is shielded (rerank dedups + re-scores from the latest property). **Fix:** union `added` keys into `removed`.

### 🟡 RC6 — Fail-open error swallowing
`table_exists().unwrap_or(false)` (`multivec/reliability/SP-REL-2`) and `sparse_vector_index(...) Err(_) => Ok(empty)` (`sparse/reliability/SP-REL-3`) turn transient backend faults into silently-incomplete results (L0-only) instead of surfaced errors. **Fix:** distinguish `Err` from `Ok(false)`; propagate or at least log.

### 🟡 RC7 — Test-coverage gaps
- **`sparse/test-gap/SP-TG-1` (headline):** the benchmark *prints* `recall@10` but never **asserts** it. The live path caps candidates at `k×4` before exact rescore, so a quantization/over-fetch regression could drop recall below 1.0 in exactly the 2k/10k regime the bench runs — nothing fails CI. The "recall@10 = 1.000" claim in the issue comment is not guarded by a test.
- `SP-TG-2` malformed-input rejection untested · `SP-TG-3` quantized DAAT scoring never validated (rerank discards index scores) · `SP-TG-4` procedure edge cases (`k=0`/empty/`over_fetch`/`threshold`) · `multivec/SP-REL-4` no crash-recovery test for MUVERA unflushed rows · `SP-TG-5` no concurrent-writers-under-flush sparse test · `SP-TG-6` flush-equivalence asserts titles not per-rank scores.

### ⚪ Acknowledged deferrals (not regressions)
- `multivec/completeness/SP-COMPL-1` — MUVERA real-corpus recall unbenchmarked/untuned (matches the issue's own open item).
- `multivec/completeness/SP-COMPL-2` — OGM `hybrid_search()` builder deferred to #114.
- `sparse/completeness/SP-COMP-1` — inline `max_sim` Cypher scalar promised in the #96 proposal, never registered as a UDF (rerank/procedure surfaces only).
- `sparse/concurrency/SP-CONC-3` (info) — sparse rerank inherits the general SSI phantom limitation (deferred FOR UPDATE).

## Recommended fix order

1. **RC1** critical data-loss race — lock `replace_table_atomic` + quiesce flush around backfill.
2. **RC2** — one validation guard in `coerce_and_validate_property_value` kills ~6 findings.
3. **RC4** flush-wedge + **RC3** overflow guard.
4. **SP-TG-1** recall assertion so the headline claim is actually defended.

---

## Full findings (verbatim, verified)

### `multivec/concurrency/SP-REL-1` — CRITICAL
**MUVERA FDE backfill (scan + full-table overwrite) is not serialized against concurrent flushes, and replace_table_atomic skips the per-table write lock — committed flushed rows can be silently lost**

- **File:** `crates/uni-store/src/storage/index_manager.rs`
- **Location:** 384-428 (backfill_fde_column); race with crates/uni-store/src/backend/lance.rs:550-586 (replace_table_atomic)
- **Category:** reliability

**Description:** backfill_fde_column scans ALL rows of the vertex table (backend.scan(ScanRequest::all(&table))), recomputes the FDE column per row, then calls backend.replace_table_atomic(table, new_batches, ...) which overwrites the ENTIRE table (AddDataMode::Overwrite). This runs in IndexManager, which holds no reference to the Writer's flush_lock, so a concurrent flush (esp. under async_flush_enabled, or a flush() on another task) can append new committed vertex rows to the same Lance table between the scan and the overwrite. The overwrite then replaces the table with only the stale pre-scan rows, silently dropping every row the concurrent flush appended. This is durable data loss of committed data. Critically, replace_table_atomic (lance.rs:550) is the ONLY backend write method that does NOT acquire write_lock_for(name): insert (lance.rs:481), merge_insert (lance.rs:519), and create_table (lance.rs:335) all take the per-table mutex, and the comment at lance.rs:474-479 explicitly states that lock was added because concurrent Append/Create mixes 'producing data loss on the in-memory backend' were observed. So even the overwrite step alone races appends; the scan->overwrite TOCTOU widens the window. MUVERA is uniquely exposed: it is the only index backfill that rewrites the primary vertex table — inverted/sparse backfills (index_manager.rs:230-233) scan then build a SEPARATE index structure via build_from_batches and never touch the vertex table. No test covers a flush concurrent with the backfill (multivector_muvera.rs only does sequential write->flush->CREATE INDEX; multivector_resilience.rs only does crash-during-flush).

**Evidence:**
```
index_manager.rs: `let batches = backend.scan(ScanRequest::all(&table)).await?; ... backend.replace_table_atomic(&table, new_batches, target_schema).await?;` and lance.rs replace_table_atomic body has NO `let lock = self.write_lock_for(name); let _guard = lock.lock().await;` whereas insert() at lance.rs:481-482 does: `let lock = self.write_lock_for(table_name); let _guard = lock.lock().await;`
```

**Suggested fix:** Two layers: (1) make replace_table_atomic acquire write_lock_for(name) for its whole read-clear/overwrite sequence, matching insert/merge_insert/create_table; (2) make the FDE backfill atomic vs flush — either quiesce/await in-flight flushes (drain via flush_in_progress / flush_lock) for the duration of scan->replace, or hold the per-table write lock across BOTH the scan and the replace (not just inside replace_table_atomic) so no append can interleave. A simple merge-by-_vid (only splice FDE into matched rows, append-merge new ones) instead of a blind Overwrite would also remove the lost-append class.

<details><summary>Independent verification</summary>

Re-derived every claim from the code:

(1) lance.rs:550-586 replace_table_atomic does NOT acquire write_lock_for; it calls table.add(batches).mode(AddDataMode::Overwrite). By contrast write() (481-482), merge_insert() (519-520), and create_table() (335) all take `let lock = self.write_lock_for(name); let _guard = lock.lock().await;`. The comment at 474-479 explicitly documents the lock was added because concurrent Append/Create mixes produced data loss on the in-memory backend. The asymmetry is exactly as claimed.

(2) index_manager.rs:413-426 backfill_fde_column does `let batches = backend.scan(ScanRequest::all(&table)).await?;` (table = vertex_table_name(label)) then `backend.replace_table_atomic(&table, new_batches, target_schema).await?` — a scan-all-then-full-overwrite of the PRIMARY vertex table.

(3) IndexManager struct (170-177) holds only base_uri, schema_manager, backend — no Writer/flush_lock. The only concurrency guard in prepare_muvera_fde (322-353) is add_internal_property's write-lock, which dedupes concurrent identical index CREATEs, NOT flushes.

(4) The flush path appends to the same `_vertex_<label>` table under the write lock: flush_stream_l1 (writer.rs:4062) -> per-label VertexDataset::write_batch (4777) -> write_batch_with_lance_conflict_retry (vertex.rs:258 / manager.rs:201-225) -> backend.write(..., WriteMode::Append), which DOES acquire write_lock_for (lance.rs:481). So flush and backfill lock the same table on disjoint locks (write path: write_lock_for; backfill: none) — genuine TOCTOU + unlocked overwrite.

(5) DDL is unserialized against flush: `CREATE VECTOR INDEX` dispatches on the read executor (read.rs:2491) `idx_mgr.create_vector_index(config)` with no flush_lock; a concurrent write+flush() goes through the Writer's flush_lock independently (writer.rs:1007, 1392 async path). Nothing forces ordering, so a flush's Append can land between the backfill scan and overwrite, and the Overwrite then drops those committed rows.

(6) Test-gap confirmed: crates/uni/tests/multivector_muvera.rs is entirely sequential — the backfill test (line 254-258) flushes BEFORE CREATE INDEX; no tokio::join!/spawn concurrency exists in the file. multivector_resilience covers only crash-during-flush. No test exercises flush concurrent with backfill.

MUVERA-uniqueness claim holds among index backfills (inverted/sparse build separate structures via build_from_batches and never overwrite the vertex table). Minor note: replace_table_atomic is also called by compaction (vertex.rs:446) and main-vertex/delta/adjacency paths, so the unlocked-overwrite primitive is a broader latent hazard than MUVERA alone — but that strengthens, not weakens, the finding. On real Lance, Overwrite is not an Append, so Lance optimistic-concurrency conflict-retry will not fold in the concurrently-appended rows; the whole-table replace logically discards them. Critical severity is appropriate: durable loss of committed data, reachable whenever write+flush runs concurrently with CREATE VECTOR INDEX type:'muvera' on a label with already-flushed rows.

</details>

---

### `multivec/concurrency/SP-REL-2` — HIGH
**force_backfill rebuild path runs the same unguarded scan->replace_table_atomic over the vertex table**

- **File:** `crates/uni-store/src/storage/index_manager.rs`
- **Location:** 281-306 (create_vector_index_inner with force_backfill=true) -> 351 -> 366 (prepare_muvera_fde) -> backfill_fde_column; called from rebuild at line 759
- **Category:** reliability

**Description:** The index rebuild path (labels_needing_rebuild -> create_vector_index_inner(cfg, true) at index_manager.rs:759) forces the MUVERA FDE backfill regardless of whether the __fde_ column was already registered, re-running the same backfill_fde_column scan->replace_table_atomic over the full vertex table. This inherits the identical concurrent-flush data-loss exposure as SP-REL-1, but is arguably more likely to fire because rebuild can run during normal operation (e.g. after recovery / bulk-load) when background async flushes are also active. The crash-window note in prepare_muvera_fde (index_manager.rs:359-365) reasons about crash atomicity but not about a concurrent flush appending rows during the rewrite.

**Evidence:**
```
index_manager.rs:351 `if !newly_added && !force_backfill { return Ok(()); }` then unconditionally `self.backfill_fde_column(&spec).await` which does the unguarded scan+replace_table_atomic.
```

<details><summary>Independent verification</summary>

Every link in the claimed chain is verified in crates/uni-store/src/storage/index_manager.rs and the rebuild driver:

1. create_vector_index_inner(config, force_backfill) at line 281 → for a Muvera index calls prepare_muvera_fde(&config, force_backfill) at line 288.
2. prepare_muvera_fde at line 351 has exactly the cited guard `if !newly_added && !force_backfill { return Ok(()); }`. When force_backfill=true this guard is bypassed even if the __fde_ column was already registered, and the code unconditionally calls self.backfill_fde_column(&spec) at line 366.
3. backfill_fde_column (lines 384-428) does precisely an unguarded full-table read+rewrite: `backend.scan(ScanRequest::all(&table))` at line 413, splices the FDE column per batch (414-423), then `backend.replace_table_atomic(&table, new_batches, target_schema)` at line 425. replace_table_atomic (lancedb/mod.rs:363) uses Lance `AddDataMode::Overwrite` (line 390), so it replaces the table with ONLY the batches derived from the scanned version. Any rows appended by a concurrent flush between the scan (version N) and the overwrite are not in new_batches and are silently dropped — identical mechanism to SP-REL-1.
4. The rebuild path is real and forces force_backfill=true: rebuild_indexes_for_label (line 742) matches IndexDefinition::Vector(cfg) => create_vector_index_inner(cfg, true) at line 759, exactly as claimed.

The "more likely to fire during normal operation" claim is independently confirmed and arguably understated: rebuild is driven by IndexRebuildManager whose start_background_worker (index_rebuild.rs:346) spawns a tokio task that periodically calls process_next_pending_task → execute_rebuild (line 528) → rebuild_indexes_for_label. execute_rebuild constructs a fresh IndexManager::new(...).with_backend(...). Crucially, IndexManager only holds Option<Arc<dyn StorageBackend>> (struct at index_manager.rs:170-176) — it has NO reference to the Writer's flush_lock, so there is structurally no way to quiesce flushes on this path. Rebuilds are also scheduled directly from the async-flush finalize path via schedule_index_rebuilds_if_needed_static (writer.rs:5272) which tokio::spawns the schedule, meaning a flush completing can kick off a rebuild that then races subsequent flushes. No flush_lock acquisition, no quiescing, and no snapshot-pinning exists anywhere on scan→replace_table_atomic in the rebuild path.

The crash-window note in prepare_muvera_fde (lines 360-365) only reasons about crash atomicity (orphan column vs schema-save ordering); it does not address a concurrent flush appending rows during the rewrite — confirming the finding's observation.

Test coverage: grep over crates/uni-store/tests found no test exercising concurrent flush during backfill_fde_column / rebuild / replace_table_atomic; the only flush-resilience and muvera-related tests do not model this race. So this is also an uncovered gap.

Severity high is correct: silent data loss of concurrently-flushed rows, on a path that auto-fires during normal post-flush/post-recovery operation, with no guard and no test.

</details>

---

### `multivec/reliability/SP-REL-1` — HIGH
**Wrong-dimension multivector token wedges every flush on a MUVERA-indexed label (FDE encoder errors where the column builder silently coerces)**

- **File:** `crates/uni-store/src/runtime/writer.rs`
- **Location:** 3566-3608 (materialize_fde_columns) and 4086 (call site)
- **Category:** reliability

**Description:** materialize_fde_columns runs at flush before column extraction and calls encoder.encode_doc(&tokens) on the raw decoded tokens. FdeEncoder::check_tokens returns FdeError::DimensionMismatch when any token length != input_dim, and materialize_fde_columns propagates that as anyhow Err. The call site `self.materialize_fde_columns(&old_l0_arc)?;` (writer.rs:4086) therefore aborts the whole flush. But the SAME data on the normal multivector column path (arrow_convert.rs:1811-1846 via extract_vector_f32_values) does NOT error — extract_vector_f32_values pads/zeros a wrong-dim token and returns valid=false (arrow_convert.rs:822-833), so the stored column tolerates it. There is no write-time guard rejecting wrong-dim tokens (constraint validation only covers UNIQUE/CHECK/NOT NULL). Net effect: with a MUVERA index present, one row whose token has the wrong dimension makes every subsequent flush fail; begin_flush has already pushed the rotated buffer onto pending_flush (l0_manager.rs:158-166) and complete_flush only runs on finalize success (l0_manager.rs:170-173), so the buffer is stuck on the pending list and L0 keeps growing — writes are effectively wedged for that label. On a non-MUVERA multivector column the identical row flushes fine.

**Evidence:**
```
writer.rs:3594 `let fde = encoder.encode_doc(&tokens).map_err(|e| anyhow!("MUVERA index '{}' vid {:?}: {e}", spec.index_name, vid))?;` vs arrow_convert.rs:823 `Some(Value::Vector(_)) => (zeros(), false), // Wrong dimensions` (no error). muvera.rs:300-311 check_tokens returns DimensionMismatch.
```

**Suggested fix:** Make materialize_fde_columns tolerant of malformed rows in the same way the column builder is: skip (or zero-fill) a row whose tokens fail the dimension check and log a warning, instead of failing the whole flush. Alternatively add a write-time declared-type guard that rejects wrong-dim multivector tokens at commit so the two paths agree. The FDE for a malformed row can be left NULL (it already ranks last/harmless per splice_fde_batch's all-zero comment).

<details><summary>Independent verification</summary>

Every cited code location and the causal chain check out.

MUVERA FDE path (errors hard):
- writer.rs:3566-3608 materialize_fde_columns: for each MUVERA spec, decodes source tokens via value_to_multivec, then writer.rs:3594-3596 `let fde = encoder.encode_doc(&tokens).map_err(|e| anyhow!(...))?;` propagates as anyhow Err.
- muvera_index.rs:119 value_to_multivec and token_to_f32 preserve each token's RAW length (Value::Vector(f) => f.clone(); List collected as-is) — no pad/truncate to input_dim. So a length-7 token reaches the encoder intact.
- muvera.rs:314-315 encode_doc calls check_tokens; muvera.rs:300-311 check_tokens returns FdeError::DimensionMismatch{got,expected} when any token.len() != input_dim. Confirmed.

Normal multivector column path (tolerates silently):
- arrow_convert.rs:1811-1846 (DataType::Vector multi-vector builder) runs each token through extract_vector_f32_values.
- arrow_convert.rs:808-835: extract_vector_f32_values returns (zeros(), false) for a wrong-dim Value::Vector (line 823) / Value::List (line 832) — appends invalid (valid=false) but does NOT error. So the stored column tolerates a wrong-dim token.

No write-time dimension guard: there is no constraint source file validating vector dimensions in uni-store/src (only a CHECK-constraint test fixture in tests/). Confirmed constraint validation does not cover token dimension.

Flush-wedge lifecycle:
- l0_manager.rs:158-166 begin_flush calls rotate() then pending_flush.write().push(old_l0.clone()) — buffer is on the pending list before Lance writes.
- writer.rs:4021 flush path: begin_flush; writer.rs:4086 materialize_fde_columns(&old_l0_arc)? inside flush_stream_l1 (writer.rs:4062); on Err it propagates via writer.rs:4944 `.flush_stream_l1(...).await?`, so flush_finalize_locked is never invoked.
- complete_flush (l0_manager.rs:168-173, called at writer.rs:5107 inside flush_finalize_locked) is therefore skipped, leaving the buffer stuck on pending_flush. Subsequent flushes re-encounter the same wrong-dim row each time, so they keep failing and L0 keeps growing — writes effectively wedged for that label.

Differential is real: identical row flushes fine on a non-MUVERA multivector column (extract_vector_f32_values path, valid=false) but wedges every flush once a MUVERA index is present.

Test gap confirmed: multivector_muvera.rs (823 lines) exercises backfill, create-before-ingest, L0 mix, reopen, fork, source-update recompute, etc., but ALL tokens use a uniform DIM constant — no test injects a wrong-dimension token to exercise the flush-error/wedge. No test in crates/uni/tests references DimensionMismatch for this scenario. So the reliability gap is genuinely uncovered.

</details>

---

### `multivec/safety/SP-SAFE-1` — HIGH
**Integer overflow in FdeParams::validate()/fde_dim() bypasses or panics the absurd-config guard (user-controlled reps/d_proj)**

- **File:** `crates/uni-common/src/muvera.rs`
- **Location:** 112-138 (fde_dim line 113; validate calls it line 132)
- **Category:** safety

**Description:** fde_dim() computes `self.reps as usize * self.buckets() * self.proj_dim()` with native unchecked multiplication. k_sim is range-checked (<=16) BEFORE this, but reps and d_proj/input_dim (all u32, user-supplied via CREATE VECTOR INDEX OPTIONS / uni.schema.createIndex / Python config / VectorAlgo::Muvera) are NOT bounded before the product is formed. With e.g. k_sim=16 (buckets=65536), reps=4e9, d_proj=4e9 the product (~1e24) overflows usize. In debug / overflow-checks builds this PANICS inside validate() itself — a malicious DDL statement crashes the engine (availability DoS). In release it wraps mod 2^64; the wrapped value can fall below MAX_FDE_DIM and PASS validation, after which FdeEncoder::new runs `(0..params.reps).map(RepMatrices::build)` — billions of iterations each allocating k_sim*input_dim + d_proj*input_dim floats — an unbounded-allocation/hang DoS. The guard that exists specifically 'to fail fast on absurd configurations rather than allocating gigabytes' can be defeated through its own arithmetic. None of the entry points (vector_index_opts::build_vector_index_type, api/schema.rs VectorAlgo::Muvera, index_manager::prepare_muvera_fde) pre-bound reps/d_proj.

**Evidence:**
```
pub fn fde_dim(&self) -> usize { self.reps as usize * self.buckets() * self.proj_dim() }  // line 113, called unguarded by validate() at line 132 before the `dim > MAX_FDE_DIM` check. reps/d_proj originate from user CREATE INDEX OPTIONS (vector_index_opts.rs:96-98 `o.reps.unwrap_or(20)`, `o.d_proj.unwrap_or(16)`).
```

**Suggested fix:** Bound reps and d_proj to a sane MAX before any multiplication, and compute fde_dim with checked_mul, treating overflow as InvalidParams. E.g. add `if self.reps > MAX_REPS || self.proj_dim() > MAX_PROJ_DIM { return Err(InvalidParams(...)); }` and use `self.reps.checked_mul(...).and_then(|x| x.checked_mul(...))` returning the out-of-range error on None. fde_dim() itself should saturate or be made fallible so callers (index_manager.rs:343, muvera_index.rs:154) never see a wrapped value.

<details><summary>Independent verification</summary>

Verified directly against the code. crates/uni-common/src/muvera.rs:112-114 defines fde_dim() = `self.reps as usize * self.buckets() * self.proj_dim()` with native unchecked multiplication. buckets() = `1usize << k_sim` (line 107). validate() (lines 117-139) range-checks k_sim (<=MAX_K_SIM=16, line 118), reps!=0 (124), input_dim!=0 (127), but imposes NO upper bound on reps or d_proj before forming the product at line 132, then compares the (possibly wrapped) result against MAX_FDE_DIM=200_000 (line 133). Fields k_sim/reps/d_proj/input_dim are all u32 (lines 82-88).

Both failure modes reproduce:
(1) Debug/overflow-checks panic: dev/test profile defaults overflow-checks=on (no profile override in Cargo.toml). I compiled a minimal reproduction with `rustc -C overflow-checks=on` of `reps*buckets*proj` for reps=4e9,buckets=65536,proj=4e9 — it panics "attempt to multiply with overflow" (exit 101) INSIDE the multiplication, i.e. inside validate() itself, before the dim>MAX_FDE_DIM check can return Err. So a malicious DDL crashes the engine in debug builds.
(2) Release wrap bypass: with overflow-checks off it wraps mod 2^64. The finding's specific example (reps=4e9,d_proj=4e9) wraps to 7.7e18 which exceeds MAX_FDE_DIM and IS rejected — so that exact example does NOT bypass. However the bypass CLASS is real and I found concrete passing values: k_sim=1, reps=2147516416, d_proj=4294901761 wraps to fde_dim=65536 (<=200000) and PASSES validate(). After that, FdeEncoder::new (muvera.rs:277-281) runs `(0..params.reps).map(RepMatrices::build)` = ~2.1 billion iterations, each allocating k_sim*input_dim + d_proj*input_dim floats — unbounded allocation/hang DoS. The very guard 'to fail fast on absurd configurations rather than allocating gigabytes' (comment line 72-73) is defeated by its own arithmetic.

Entry points confirmed unbounded: vector_index_opts.rs:37-38 reps/d_proj are Option<u32>; build_vector_index_type (93-104) passes `o.reps.unwrap_or(20)`/`o.d_proj.unwrap_or(16)` straight through with no cap. api/schema.rs VectorAlgo::Muvera fields (623-629) are plain u32, into_internal (694-705) passes through unbounded. The chain reaches validate() at index_manager.rs:334 and FdeEncoder::new at index_manager.rs:411, both inside prepare_muvera_fde — a user-triggered CREATE VECTOR INDEX path.

Test gap confirmed: validate_rejects_bad_params (muvera.rs:502-511) only exercises params(16,1000,64,96) for the absurd case — product 1000*65536*64=4.19e9 does NOT overflow usize and is rejected on its merits. No test constructs an overflowing config; the panic/wrap path is untested.

Severity: finding says high. The defect is real (guard defeatable + debug-build crash) but both the panic and the wrap-bypass require the caller to already hold DDL/CREATE-INDEX privilege, and the production-shipped release build wraps (no crash). A privileged user could also force large allocations with merely-large-but-valid configs. The integer-overflow guard bypass is nonetheless a genuine safety/availability bug worth fixing (use checked_mul / saturating_mul, or bound reps & d_proj before the product). Keeping high but noting the privileged-access precondition tempers real-world exploitability toward the high/medium boundary.

**Severity correction:** The finding's specific example values (reps=4e9, d_proj=4e9) do NOT bypass release validation — they wrap to ~7.7e18 which exceeds MAX_FDE_DIM and is rejected. They DO panic in debug/overflow-checks builds, matching the panic claim. The wrap-bypass claim is still correct as a class: concrete passing values exist, e.g. k_sim=1, reps=2147516416, d_proj=4294901761 wraps fde_dim to 65536 (<=200000), passing validate() and then triggering ~2.1 billion encoder iterations. Also: all entry points require DDL/CREATE-INDEX privilege (not anonymous input), so real-world blast radius is bounded by that precondition.

</details>

---

### `sparse/concurrency/SP-CONC-1` — HIGH
**Unserialized concurrent WriteMode::Overwrite of sparse postings (DDL create vs background-flush incremental) is a silent lost update**

- **File:** `crates/uni-store/src/storage/sparse_index.rs`
- **Location:** write_postings 322-401 (Overwrite at 393-399); apply_incremental_updates 597-621
- **Category:** reliability

**Description:** SparseVectorIndex::write_postings persists the postings with lance WriteMode::Overwrite (line 394), which is unconditional last-writer-wins (no optimistic conflict check). Two distinct paths call it on the SAME on-disk path ({base}/indexes/{label}/{property}_sparse) with NO shared lock: (a) IndexManager::create_sparse_vector_index (index_manager.rs:865) full-scans the flushed vertex table and Overwrites, called from the read/DDL executor (read.rs:2505, ddl_procedures.rs:431) which does NOT acquire flush_lock; (b) IndexManager::update_sparse_vector_index_incremental (index_manager.rs:917) does load_postings -> modify -> write_postings, invoked from the flush finalize body (writer.rs:4862-4869). The flush finalize runs under flush_lock, but with async_flush_enabled it runs on a spawned coordinator task (writer.rs flush_finalize_now:4995 re-acquires only flush_lock, which DDL never takes). IndexManager holds no lock (struct at index_manager.rs:170 has only base_uri/schema_manager/backend). So 'CREATE SPARSE INDEX while ingest is flushing' interleaves: incremental update loads postings at disk version V, DDL backfill Overwrites to V+1, incremental Overwrites V+2 and clobbers the backfill (or vice-versa). The flushed postings end up inconsistent with the vertex table. Because non-fork sparse_search (manager.rs:2058-2064) generates flushed candidates SOLELY from query_topk over these postings, a vid whose posting was lost AND is not in the live L0 set silently disappears from candidates -> missing search results, not just a stale score.

**Evidence:**
```
sparse_index.rs:393-399: `let write_params = lance::dataset::WriteParams { mode: lance::dataset::WriteMode::Overwrite, .. }; ... let ds = Dataset::write(iterator, &path, Some(write_params)).await?;` ; create path read.rs:2504-2505: `let idx_mgr = self.storage.index_manager(); idx_mgr.create_sparse_vector_index(config).await?;` (no flush_lock) ; flush path writer.rs:4866-4869: `self.storage.index_manager().update_sparse_vector_index_incremental(cfg, added, removed).await?;`
```

**Suggested fix:** Serialize all writes to a given index's postings dataset. Cheapest: have create_sparse_vector_index and update_sparse_vector_index_incremental both acquire the writer's flush_lock (or a dedicated per-index-path async Mutex held in IndexManager) around the load-modify-write/backfill + Overwrite. Alternatively make write_postings use a conflict-checked commit (read base version, CommitBuilder with expected version, retry on conflict) so a clobber surfaces as an error instead of silent data loss.

<details><summary>Independent verification</summary>

I traced every cited claim against the actual tree (the files are in uni-query, not uni-store as the finding's read.rs/ddl_procedures.rs paths imply, but the line numbers and content match).

1. Overwrite is unconditional last-writer-wins: crates/uni-store/src/storage/sparse_index.rs:392-399 builds WriteParams{mode: WriteMode::Overwrite} and calls Dataset::write — no commit-conflict check or retry. Confirmed.

2. apply_incremental_updates (sparse_index.rs:597-621) is load_postings() -> mutate in memory -> write_postings (Overwrite). Read-modify-write, so a concurrent clobber loses the in-flight delta. Confirmed.

3. IndexManager (index_manager.rs:170-177) holds only base_uri/schema_manager/backend — no mutex. Confirmed.

4. DDL create path: read.rs:2504-2505 and ddl_procedures.rs:430-431 both call idx_mgr.create_sparse_vector_index, which (index_manager.rs:865-908) scans the flushed table via backend and build_from_batches -> write_postings (Overwrite). Neither call site nor create_sparse_vector_index takes flush_lock. Confirmed.

5. Flush path: the sparse incremental update at writer.rs:4859-4871 lives inside flush_stream_l1 (4062-4914), which contains NO flush_lock acquisition (the only flush_lock token in that range, line 4904, is an unrelated doc comment). In the async_flush_enabled path, writer.rs:1416 drops _flush_lock_guard BEFORE submitting flush_stream_l1 to the coordinator (spawned task, line 1429-1433). So the flush-side Overwrite of postings runs with NO lock held. flush_finalize_now (4995-5017) re-acquires flush_lock, but that is the publish/finalize phase, AFTER the postings were already written in the stream phase — so the lock does not protect the posting write. Confirmed (and the race window is actually wider than the finding states).

6. Non-fork sparse_search (manager.rs:1999-2066): when not branched, it goes straight to sparse_vector_index(...).query_topk(query,k) (2058-2064) — flushed candidates come solely from the postings. A vid whose posting was clobbered and is not in live L0 silently vanishes from candidates (missing result, not merely stale score). Confirmed.

7. Test gap: no test spawns CREATE SPARSE INDEX concurrently with a flush. sparse_index.rs:647 (sparse_snapshot_isolates_reader_from_concurrent_insert) is MVCC snapshot isolation with a sequential writer commit, unrelated to the Overwrite race. sparse_resilience.rs and metamorphic/sparse.rs (referenced in the finding) do not exist in the current tree. Confirmed no coverage.

Severity high is justified: silent lost-update producing missing search results (a correctness/data-loss class bug), reachable by a plausible operational sequence (CREATE SPARSE INDEX during active ingest), with no serialization and no test.

**Severity correction:** The finding implies the flush-side incremental update is protected because the async coordinator's flush_finalize_now re-acquires flush_lock. That under-describes the gap: the sparse posting Overwrite happens in the STREAM phase (flush_stream_l1), which holds no flush_lock at all in the async path (flush_lock is dropped at writer.rs:1416 before the stream task is spawned). flush_finalize_now's lock only guards the later publish/finalize phase. Moreover, the DDL create path never takes flush_lock in EITHER flush mode (sync or async), so even with async_flush disabled — the default — the DDL-create-vs-flush Overwrite race still exists because the two Overwrites of the same {base}/indexes/{label}/{property}_sparse path are never serialized against each other. Net effect: the race is real and somewhat broader than characterized; severity remains high.

</details>

---

### `sparse/correctness/SP-CORR-1` — HIGH
**WAL value-codec panics on a non-canonical Value::SparseVector (unsorted/duplicate indices or non-finite weight)**

- **File:** `crates/uni-common/src/cypher_value_codec.rs`
- **Location:** 436-441 (encode); panic at .expect line 439)
- **Category:** safety

**Description:** Value::SparseVector is a plain public struct variant whose docs claim indices are 'strictly ascending', but the type enforces nothing (value.rs:559). The CV encode path used by the WAL does `SparseVector::new(indices.clone(), values.clone()).expect("invalid SparseVector value")`. `SparseVector::new` returns Err for unsorted/duplicate indices or non-finite weights, so the .expect() panics. This encode is called for every property on every commit (wal.rs:139 `cypher_value_codec::encode(v)`). A Rust-API user can supply `Value::SparseVector { indices: vec![5,1], values: vec![1.0,2.0] }` or a NaN/Inf weight via `.param(...)`; the schema `validate` (schema.rs:387) only checks the variant, not order/finiteness, and the column builders (`sparse_pair_from_value`/`build_sparse_vector_column`) store raw values without sorting or validation. The result is a panic in the commit/WAL-write thread (crash on plausible input), not a clean error.

**Evidence:**
```
let sv = uni_sparse_vector::SparseVector::new(indices.clone(), values.clone()).expect("invalid SparseVector value"); buf.extend_from_slice(&uni_sparse_vector::encode::encode(&sv));
```

**Suggested fix:** Canonicalize/validate at the ingest boundary: when coercing a value into a DataType::SparseVector column (or in the executor param path), route through SparseVector::from_pairs (sort+sum+finite-check) and reject non-finite with a UniError instead of relying on a downstream .expect(). At minimum, make cypher_value_codec::encode return a Result for the SparseVector arm (or fall back to from_pairs canonicalization) rather than panicking.

<details><summary>Independent verification</summary>

All load-bearing claims verified against the actual code:

1. PANIC SITE (cypher_value_codec.rs:436-441): The encode arm for `Value::SparseVector` does exactly `uni_sparse_vector::SparseVector::new(indices.clone(), values.clone()).expect("invalid SparseVector value")` — a hard panic on Err.

2. `SparseVector::new` REJECTS the cited inputs (sparse.rs:30-52): returns `Err(SparseError::UnsortedIndices)` when `indices[i] <= indices[i-1]` (so `vec![5,1]` fails), `Err(SparseError::LengthMismatch)`, and `Err(SparseError::NonFiniteWeight)` when `!value.is_finite()` (so NaN/Inf fails). So `.expect()` panics on these.

3. UNENFORCED VARIANT (value.rs:559-564): `Value::SparseVector { indices: Vec<u32>, values: Vec<f32> }` is a plain public struct variant; the "strictly ascending" guarantee is doc-only, enforced by nothing at the type level.

4. WAL CALL SITE (wal.rs:139, in `cv_props::serialize`, applied via `#[serde(with = "cv_props")]` on the mutation record at wal.rs:178): per-property `cypher_value_codec::encode(v)` is invoked on serialization — i.e. on the commit/WAL-write path — reaching the panicking arm.

5. SCHEMA validate (core/schema.rs:387-389) only does `matches!(value, Value::SparseVector { .. } | Value::Map(_))` — it checks the variant, never order/finiteness, so it does not reject a non-canonical value.

6. COLUMN BUILDERS store raw (arrow_convert.rs:1176-1203 `sparse_pair_from_value`): clones `indices`/`values` verbatim with no sort/finiteness check — no canonicalization between param and persist.

7. CONTRAST PROVEN (writer.rs:82-93 `sparse_pairs_to_value`): the autoembed path DOES canonicalize via `BTreeMap` and drops non-finite weights, confirming the explicit-param path is the unguarded one.

8. PARAM PATH (uni/src/api/sync.rs / session.rs `param<K,V: Into<Value>>`): inserts the `Value` directly into params with no transformation; a Rust user can supply a non-canonical `Value::SparseVector` that flows unmodified to the encoder.

9. TEST GAP: the only sparse codec tests (cypher_value_codec.rs:717-755) all use canonical, finite, sorted inputs; none exercise unsorted/duplicate/non-finite, so nothing catches the panic.

The query-input paths (search_procedures.rs:531/561/599) that use `SparseVector::new`/`from_pairs` with `.map_err` are a DIFFERENT path (sparse search input), not the SET-property persistence path, so they do not mitigate this. The finding is real and correctly characterized: a plausible Rust-API input causes an `.expect()` panic on the commit/WAL-write thread instead of a clean error.

</details>

---

### `sparse/safety/SP-SAFE-1` — HIGH _(reviewer said critical; verifier corrected to high)_
**User-supplied invalid Value::SparseVector panics the database at WAL encode via .expect()**

- **File:** `crates/uni-common/src/cypher_value_codec.rs`
- **Location:** 436-440 (encode), reached from crates/uni-store/src/runtime/wal.rs:139
- **Category:** safety

**Description:** The CypherValue encoder reconstructs a kernel SparseVector with `SparseVector::new(indices.clone(), values.clone()).expect("invalid SparseVector value")`. WAL serialization (uni-store/src/runtime/wal.rs:139 `cypher_value_codec::encode(v)`) runs this on every property value of every committed write. A `Value::SparseVector` with non-finite weights (NaN/±inf), non-ascending/duplicate indices, or `indices.len() != values.len()` makes `SparseVector::new` return Err, so `.expect` panics inside the commit path, taking down the process / write task. The value reaches here unvalidated: a user passes it as a query parameter (e.g. `CREATE (:Doc {emb: $emb})` with `.param("emb", Value::SparseVector { indices: vec![5,1], values: vec![1.0,2.0] })`, or `vec![f32::NAN]`, or mismatched lengths). `DataType::accepts` (schema.rs:387) only checks the enum variant, and `coerce_and_validate_property_value` (write.rs:3022) applies no sparse invariant check. The auto-embed path (writer.rs:82 `sparse_pairs_to_value`) is safe because it sorts and drops non-finite weights, but the explicit user-write path is not guarded.

**Evidence:**
```
let sv = uni_sparse_vector::SparseVector::new(indices.clone(), values.clone()).expect("invalid SparseVector value");  // cypher_value_codec.rs:438-439
// reached for every prop: let bytes = uni_common::cypher_value_codec::encode(v);  // wal.rs:139
// accepts() never validates: matches!(value, Value::SparseVector { .. } | Value::Map(_))  // schema.rs:388
```

**Suggested fix:** Validate sparse invariants at ingest in coerce_and_validate_property_value (and/or sparse_pair_from_value) by round-tripping through SparseVector::new/from_pairs and returning a TypeError on failure — mirroring how the BTIC/temporal paths reject bad input — so a malformed sparse value is rejected as a query error rather than panicking deep in the WAL. As defense in depth, change cypher_value_codec::encode's `.expect` to return a codec error rather than panic.

<details><summary>Independent verification</summary>

Verified the full chain end-to-end (line numbers shifted from the finding but the code is exactly as claimed):

1. ENCODE PANIC SITE — crates/uni-common/src/cypher_value_codec.rs:436-440: the `Value::SparseVector { indices, values }` arm calls `uni_sparse_vector::SparseVector::new(indices.clone(), values.clone()).expect("invalid SparseVector value")`.

2. SparseVector::new returns Err on exactly the claimed conditions — crates/uni-sparse-vector/src/sparse.rs:30-52: `LengthMismatch` if indices.len() != values.len(); `UnsortedIndices` if any indices[i] <= indices[i-1] (catches non-ascending AND duplicates); `NonFiniteWeight` if any !value.is_finite() (NaN/±inf). So .expect() panics on each.

3. ON THE COMMIT/WAL PATH — Mutation::InsertVertex/InsertEdge carry `properties: Properties` annotated `#[serde(with = "cv_props")]` (wal.rs:178-179, 193-194). cv_props::serialize (wal.rs:134-144) calls `cypher_value_codec::encode(v)` at wal.rs:139 for every property. This serialize runs inside WalFlush::flush via `serde_json::to_vec(&segment)` (wal.rs:358), which is invoked by writer.rs flush_wal() at the synchronous commit (writer.rs:1277, before the `commit::after-wal-flush` failpoint). The only catch_unwind (flush_coordinator.rs:390) wraps the background L0→Lance flush future, NOT this commit-path WAL serialize — so the panic is unguarded and takes down the commit/write task.

4. VALIDATION GAP CONFIRMED — schema.rs accepts() (crates/uni-common/src/core/schema.rs:387-389) for DataType::SparseVector only does `matches!(value, Value::SparseVector { .. } | Value::Map(_))` — variant check only, no invariants. coerce_and_validate_property_value (crates/uni-query/src/query/executor/write.rs:3022-3096) returns Ok(val) immediately at line 3054-3056 when `dt.accepts(&val)`, so an invalid Value::SparseVector passes through untouched. The arrow column builder build_sparse_vector_array (arrow_convert.rs:1210-1243) also passes raw indices/values into Arrow builders without calling SparseVector::new, so it does not validate either — the WAL encode is the failing point.

5. AUTO-EMBED IS SAFE as claimed — writer.rs:82-93 sparse_pairs_to_value uses a BTreeMap (sorted keys, dedup) and skips non-finite weights (`if weight.is_finite()`), so it can only produce valid SparseVectors.

6. TEST GAP CONFIRMED — every test that constructs Value::SparseVector (sparse_resilience.rs, ssi_read_path_matrix.rs, metamorphic/sparse.rs, sparse_scoring.rs, sparse_ddl_type.rs, sparse_autoembed.rs) uses ascending, finite, equal-length data (e.g. vec![1,5,9,42], vec![2,3]). None exercise unsorted ([5,1]), duplicate, NaN, or length-mismatched input through the write path. No existing test covers the panic.

The finding is real and correctly characterized.</reasoning>
<parameter name="correction">Severity lowered from critical to high. Reachability requires a caller to programmatically construct a malformed Value::SparseVector and pass it as a query parameter (Rust/Python API param surface). It is NOT reachable from normal Cypher text/literals or the auto-embed path (both produce canonicalized vectors). It is a genuine unguarded panic-on-commit (process/write-task DoS, abort with .expect rather than clean Err), but it needs a directly-constructed malformed value rather than ordinary untrusted text, so critical overstates the trigger surface. High is appropriate for an unvalidated user-input panic on the durable commit path.</correction>
</invoke>


</details>

---

### `multivec/reliability/SP-REL-2` — MEDIUM
**Backend table_exists errors are swallowed (fail-open) in muvera_fde_candidates / multivector_search, yielding silent zero flushed candidates**

- **File:** `crates/uni-store/src/storage/manager.rs`
- **Location:** 1840 (muvera_fde_candidates), 1924 and 1954 (multivector_search)
- **Category:** reliability

**Description:** muvera_fde_candidates does `if !backend.table_exists(&name).await.unwrap_or(false) { return Ok(Vec::new()); }`. A transient backend/object-store error (network, permissions, manifest read) is mapped by unwrap_or(false) to 'table does not exist', so the function silently returns zero flushed candidates instead of propagating the error. The query then returns only L0 results — silently wrong (incomplete) results rather than a surfaced failure. The same fail-open `.unwrap_or(false)` on table_exists appears at multivector_search:1924 (branched arm) and :1954 (main arm). The dense vector_search path and the FDE first stage are first-stage-only so this isn't always catastrophic, but a backend that is temporarily unreadable will quietly drop all flushed/indexed documents from MaxSim results.

**Evidence:**
```
manager.rs:1840 `if !backend.table_exists(&name).await.unwrap_or(false) {` then `return Ok(Vec::new());`. manager.rs:1954 `if backend.table_exists(&name).await.unwrap_or(false) {` (a true Err is treated as absent → results stays empty).
```

**Suggested fix:** Propagate the table_exists error (use `?`) and only treat a genuine NotFound/false as 'nothing flushed'. At minimum distinguish Err from Ok(false) so a backend fault surfaces instead of degrading silently.

<details><summary>Independent verification</summary>

Verified the exact code at all three cited lines in crates/uni-store/src/storage/manager.rs. (1) muvera_fde_candidates:1840 `if !backend.table_exists(&name).await.unwrap_or(false) { return Ok(Vec::new()); }`. (2) multivector_search branched arm:1924 same `!...unwrap_or(false)` early-return Ok(empty). (3) multivector_search main arm:1954 `if backend.table_exists(&name).await.unwrap_or(false) { ...search... }` — a true Err skips the search block, leaving `results` empty and returning Ok(empty). table_exists genuinely can return Err: lance.rs:302 and lancedb/mod.rs:77 implement it via `table_names().await?`, an object-store/connection listing that fails on transient network/permission/manifest-read errors; `.unwrap_or(false)` collapses that Err into 'table absent'. Confirmed downstream impact: multivector_rerank (uni-query search_procedures.rs:368-410) maps a real Err to a DataFusion execution error, so the swallowing means no error ever surfaces — instead the union (line 413+) proceeds with only L0 candidates, yielding silently incomplete (or fully empty if the corpus is entirely flushed) MaxSim results. The finding is real and correctly characterized as fail-open / silently-incomplete.

**Severity correction:** The behavior is accurately described, but two nuances temper severity: (a) results are incomplete rather than empty in the common case (live L0 candidates still flow through), only fully empty when the corpus is entirely flushed; (b) this exact `.unwrap_or(false)` on table_exists is a deliberate, systemic fail-open convention used at ~14 other call sites in the same file (e.g. lines 837, 1125, 1295, 1365, 1428, 1531, 1775, 2036, 2103), not a multivector-specific oversight — and lance.rs has an existence_cache that reduces sustained false negatives after a first successful probe. Medium is appropriate (lower-medium); not high, since it requires a transient backend outage during a cold-cache table_exists probe and degrades recall rather than corrupting data.

</details>

---

### `multivec/safety/SP-SAFE-2` — MEDIUM
**Multivector / MaxSim / FDE path does not reject non-finite (NaN/Inf) token weights, poisoning Dot-metric ordering**

- **File:** `crates/uni-query-functions/src/similar_to.rs`
- **Location:** maxsim 261-277; sort sites search_procedures.rs:264,305,463,549
- **Category:** safety

**Description:** Unlike the sparse-vector kernel (uni-sparse-vector/src/sparse.rs:47 rejects non-finite weights with SparseError::NonFiniteWeight), the multivector/ColBERT path performs NO finiteness validation on token values at any stage: value_to_multivec / token_to_f32 (muvera_index.rs:119-138), extract_vector_list (search_procedures.rs:85) and evaluate_query_multivector (vector_knn.rs:296-322) accept arbitrary f32 including NaN/±Inf. maxsim() with DistanceMetric::Dot returns the raw dot product (similar_to.rs:241), so a single NaN/Inf token in a stored document or in an attacker-supplied query token makes the per-candidate score NaN/Inf. The result vectors are then sorted with `b.1.partial_cmp(&a.1).unwrap_or(Equal)` (search_procedures.rs:463 and 264/305/549), where NaN comparisons collapse to Equal — yielding nondeterministic / incorrect top-k ordering (a doc with a NaN score can sort anywhere) and an Inf score can dominate all real results. This is silent wrong results, not a crash, but it is data/attacker-controlled. NaN also propagates into FDE bucket sums/centroids (muvera.rs encode_doc/encode_query) and into the ANN candidate generation.

**Evidence:**
```
maxsim: `let sim = score_vectors(q, d, metric)?; best = Some(best.map_or(sim, |b| b.max(sim)));` with Dot returning `-distance` (raw dot, no clamp). Sort: `scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));` (search_procedures.rs:463). Contrast sparse.rs:47 `if !value.is_finite() { return Err(SparseError::NonFiniteWeight {...}); }`.
```

**Suggested fix:** Reject (or sanitize) non-finite token values at the multivector ingest/extract boundary (token_to_f32 in muvera_index.rs and extract_vector/extract_vector_list in search_procedures.rs) the same way sparse vectors do, and/or guard the rerank sort to push non-finite scores to the bottom deterministically (treat NaN as -inf) rather than relying on partial_cmp().unwrap_or(Equal).

<details><summary>Independent verification</summary>

Verified every cited location. (1) sparse.rs:47 does reject non-finite weights (SparseError::NonFiniteWeight), confirming the contrast. (2) Multivector ingest/extraction performs NO finiteness check: muvera_index.rs token_to_f32 (126-138) and value_to_multivec accept any f32; search_procedures.rs extract_vector_list (85-92) and extract_vector (58-78) only validate that elements are numeric via as_f64(), not finite — NaN/Inf pass through unchanged. (3) similar_to.rs score_vectors Dot arm returns -distance (raw dot, no clamp, line 241); maxsim (261-277) accumulates via b.max(sim) (271) and total += (274), so a single NaN/Inf token poisons the per-candidate score. (4) All four sort sites (search_procedures.rs 264, 305, 463, 549) use `b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)`; for a NaN score partial_cmp returns None -> Equal, so the NaN-scored row sorts nondeterministically (not deterministically last) and an Inf score dominates. (5) Searched all maxsim/multivector unit tests (similar_to.rs 538-577: hand-computed, empty edge cases, dimension-mismatch, metric-change — none with NaN/Inf) and all integration tests (multivector_maxsim/index/l0/snapshot/muvera/resilience, metamorphic/multi) — no test feeds a non-finite token weight; the only INFINITY hit is an unrelated fold-seed accumulator. uni-common/src/muvera.rs (FDE encode) likewise has no is_finite/is_nan guard, so NaN propagates into FDE bucket sums/centroids as claimed. The finding is real and correctly characterized in all specifics.

</details>

---

### `sparse/concurrency/SP-CONC-2` — MEDIUM
**Same unguarded Overwrite race exists for the set-membership inverted index (shared pattern)**

- **File:** `crates/uni-store/src/storage/inverted_index.rs`
- **Location:** write_postings 211-256 (Overwrite 235-252); apply_incremental_updates 390-451
- **Category:** reliability

**Description:** InvertedIndex mirrors the sparse index exactly: create_inverted_index (index_manager.rs:207) full-scan backfill and update_inverted_index_incremental (writer.rs:4852-4855 flush path) both call write_postings -> Dataset::write Overwrite on {base}/indexes/{label}/{property}_inverted with no shared lock. The same DDL-create-vs-background-flush lost-update window applies. Listed separately at medium because it is pre-existing code that issue #95 forked from, but the fix for SP-CONC-1 should cover it too (same root cause: per-index postings write path is not serialized against DDL backfill).

**Evidence:**
```
inverted_index.rs:235-252: `let write_params = WriteParams { mode: WriteMode::Overwrite, .. }; ... let ds = Dataset::write(iterator, &path, Some(write_params)).await?;` ; flush caller writer.rs:4852-4855: `update_inverted_index_incremental(cfg, added, removed).await?`
```

**Suggested fix:** Apply the same per-index-path write serialization (or conflict-checked commit) chosen for SP-CONC-1 to the inverted index.

<details><summary>Independent verification</summary>

Verified every cited claim against the code.

1. inverted_index.rs:211-256 write_postings: builds a RecordBatch and calls `Dataset::write(iterator, &path, Some(write_params))` with `WriteParams { mode: WriteMode::Overwrite, .. }` to path `{base_uri}/indexes/{label}/{property}_inverted` (lines 231-252). This is a full-table Overwrite (replace), exactly as claimed.

2. inverted_index.rs:390-451 apply_incremental_updates is load_postings() -> mutate in memory -> write_postings() (Overwrite at 449). Classic load-modify-write, mirroring sparse_index.rs (sparse write_postings at sparse_index.rs:322 also uses WriteMode::Overwrite / Dataset::write at lines 394-398). The two index types share the identical pattern.

3. create_inverted_index (index_manager.rs:207-252) is the DDL backfill path: scans the flushed vertex table and calls `index.build_from_batches(...)` (-> write_postings -> Overwrite), then add_index + schema save. It does NOT acquire flush_lock — no flush_lock reference exists anywhere in inverted_index.rs or in this function.

4. The flush caller: writer.rs ~4845-4858 calls `update_inverted_index_incremental(cfg, added, removed)` inside flush_stream_l1 (the enclosing fn starting at writer.rs:4062). flush_stream_l1 is invoked from the async-flush task at writer.rs:1433, which is spawned AFTER `drop(_flush_lock_guard)` at writer.rs:1416 ('Release flush_lock BEFORE the spawn so concurrent commits can proceed'). So the background flush's Overwrite of the postings table runs with flush_lock NOT held.

Net: both the DDL-create backfill and the background flush issue full-table Overwrite writes to the same {base}/indexes/{label}/{property}_inverted path, and neither serializes against the other via flush_lock. A create-vs-flush interleave produces a lost-update / last-writer-wins window (one Overwrite clobbers the other's manifest commit), identical to the sparse SP-CONC-1 root cause. No test exercises DDL-create vs background-flush concurrency for inverted (or sparse) indexes (grep for create_inverted_index/create_sparse_vector_index in test files returned nothing).

Severity medium is appropriate and consistent with the finding's own framing: it is pre-existing code that #95 forked from, the race requires concurrent DDL CREATE INDEX while a background flush touches the same label/property (a narrow window), and impact is a corrupted/stale postings table rather than primary-data loss (postings are a derivable secondary index, rebuildable). The fix for SP-CONC-1 (serialize per-index postings writes against DDL backfill) should cover this path too.

</details>

---

### `sparse/correctness/SP-CORR-2` — MEDIUM
**Flushed sparse index accumulates stale postings on update-then-reflush (updated vids added but never removed; Vec not deduped)**

- **File:** `crates/uni-store/src/runtime/writer.rs`
- **Location:** 4728-4741 (update collection); applied in sparse_index.rs apply_incremental_updates 612-617
- **Category:** correctness

**Description:** When a vertex with an existing flushed sparse posting has its sparse property updated and is flushed again, the writer adds it to `added` (writer.rs:4739) but only adds *deleted* rows and tombstones to `removed` (4729-4730, 4743-4746). In SparseVectorIndex::apply_incremental_updates the old postings for that vid are only dropped via `removed`; the new (term,weight) pairs are then push()ed onto the per-term Vec (sparse_index.rs:615) with no dedup. So both the old and new postings for the same vid persist in the flushed index, growing unboundedly on hot-updated docs. By contrast the set-membership InvertedIndex stores postings as HashSet<u64> (inverted_index.rs:437 `.insert`), which naturally dedups. The production `sparse_rerank` path dedups candidates by vid and re-scores from the latest property, so final query results stay correct, but the storage-layer public `SparseVectorIndex::query_topk`/`sparse_search` advisory scores are double-counted (scores.entry(vid).or_insert(0.0) += qw*w runs twice for the stale and fresh posting).

**Evidence:**
```
for (vid, _labels, props, deleted, _version) in &vertices { if *deleted { removed.insert(*vid); } else if let Some(SparseVector{indices,values}) = props.get(&cfg.property) { ...; added.insert(*vid, pairs); } }  // updated (non-deleted) vid never added to `removed`
```

**Suggested fix:** When building sparse_updates, insert every vid present in `added` into `removed` as well (remove-then-add semantics), or change Postings to drop all existing entries for an updated vid before pushing the new ones in apply_incremental_updates.

<details><summary>Independent verification</summary>

Every code claim re-derived and verified.

1) writer.rs:4728-4747 (correct lines, the finding's 4728-4741 maps onto this): for each L0 vertex being flushed, a non-deleted vertex carrying the sparse property is inserted into `added` ONLY (line 4739). It enters `removed` solely when `*deleted` (4730) or it appears in `tombstones_by_label` (4745). An in-place SET update produces a non-deleted L0 row, so the updated vid lands in `added` but never in `removed`.

2) `vertices_by_label` (the source of the 4728 loop) is built at flush_stream_l1 from `old_l0.vertex_properties` (writer.rs:4195), i.e. the rows currently in the L0 buffer being flushed — the DELTA since the last flush, not a full L1 scan. So a second flush after a SET carries only the updated `target`, confirming the incremental (not rebuild) path. The flush consumes it via update_sparse_vector_index_incremental (writer.rs:4868 -> index_manager.rs:917-929 -> apply_incremental_updates).

3) sparse_index.rs apply_incremental_updates (597-621): `removed` retain-filters out only vids in the `removed` set (604-610). Since the updated vid is absent from `removed`, its OLD postings survive. Then new (term,weight) pairs are push()ed onto the per-term Vec (615) with no dedup. Postings is a Vec per term; write_postings (322-364) serialises entries verbatim with no per-vid dedup, and there is no compaction-time vid dedup. So the vid persists under both old and new terms (and, if a term is shared across old/new vectors, twice under the same term).

4) Double-count in the public scorer: query_topk (pub, 437) does `*scores.entry(vids.value(j)).or_insert(0.0) += qw * weights.get(j)` (501). If a vid appears in two posting entries for a query term (same-term-in-both-vectors case) its contribution is summed twice; the disjoint-term case adds spurious stale-term contributions. Confirmed.

5) Contrast claim verified: InvertedIndex.apply_incremental_updates stores postings as HashSet<u64> and uses `.insert` (inverted_index.rs:437), which naturally dedups; the sparse index uses Vec::push, which does not.

6) Production-correctness caveat verified exactly as the finding states. sparse_rerank (search_procedures.rs:479-552) uses sparse_search/query_topk ONLY as a candidate generator, dedups candidates by vid into a HashSet `seen` (500-511), then re-fetches the LATEST (L0-merged) property and re-scores via sparse_dot (524-546). So stale duplicate postings only yield a redundant (deduped) candidate; the final query result is correct. This is precisely why the existing regression test `sparse_recovered_update_overrides_stale_posting_without_rebuild` (crates/uni/tests/sparse_index.rs:806) passes even though it executes the exact reflush-after-SET scenario the finding describes — its assertion (target drops out) is satisfied by the re-score path, not by the index being pruned. No existing test asserts on query_topk/load_postings directly to catch the stale posting, so the storage-layer test gap the finding cites is real.

Net: the bug is real and accurately characterized at both the write side (stale + non-deduped postings, unbounded growth on hot-updated docs) and the read side (advisory query_topk/sparse_search scores double-count). Production query path is shielded by rerank dedup+re-score.

**Severity correction:** Severity is at the low end of medium. There is no production query-correctness impact (sparse_rerank dedups candidates and re-scores from the latest property; verified). The real, durable concerns are (a) unbounded postings growth on hot-updated documents — a storage/perf leak that persists across flushes with no compaction-time cleanup, and (b) incorrect advisory scores from the public SparseVectorIndex::query_topk / StorageManager::sparse_search APIs, which any external/future caller bypassing sparse_rerank would observe. Given the blast radius is confined to an advisory path plus a slow storage leak (not user-visible query results), low-medium is the fair rating; I keep it at medium because the unbounded growth has no upper bound and accumulates silently.

</details>

---

### `sparse/reliability/SP-REL-1` — MEDIUM _(reviewer said high; verifier corrected to medium)_
**Invalid Value::SparseVector panics in the WAL serialization (durability) path**

- **File:** `crates/uni-common/src/cypher_value_codec.rs`
- **Location:** 436-440 (Value::SparseVector encode arm)
- **Category:** reliability

**Description:** Write-time validation (`coerce_and_validate_property_value` -> `DataType::accepts` -> `matches_data_type`) only checks the value *variant* for a SparseVector column (schema.rs:387-389: `matches!(value, Value::SparseVector { .. } | Value::Map(_))`); it does NOT re-validate the sparse invariants (sorted/unique indices, finite weights, equal lengths). A directly-constructed invalid `Value::SparseVector` (e.g. via the Rust `.param("emb", Value::SparseVector{indices: vec![5,1], values: vec![f32::NAN]})` API, which bypasses `SparseVector::new`) therefore passes validation and reaches the WAL property serializer (`cv_props::serialize` -> `cypher_value_codec::encode`), whose SparseVector arm does `uni_sparse_vector::SparseVector::new(indices.clone(), values.clone()).expect("invalid SparseVector value")`. The `.expect` panics, aborting the commit task inside the durability path. This is a crash on plausible (if malformed) input. The Python binding is safe because PySparseVector always canonicalizes via `from_pairs`, but the Rust API and any internally-built Value::SparseVector are not.

**Evidence:**
```
cypher_value_codec.rs:438-439: `let sv = uni_sparse_vector::SparseVector::new(indices.clone(), values.clone()).expect("invalid SparseVector value");` ; schema.rs:387-389 accepts any Value::SparseVector variant; write.rs:3054 `if dt.accepts(&val) { return Ok(val); }` returns early with no invariant check.
```

**Suggested fix:** In `coerce_and_validate_property_value`, when the declared type is `DataType::SparseVector`, run `uni_sparse_vector::SparseVector::new(indices, values)` (or `from_pairs` to canonicalize) and return a TypeError on failure, rejecting the write up front. Additionally, make `cypher_value_codec::encode`'s SparseVector arm return an error instead of `.expect()` so the durability path can never panic.

<details><summary>Independent verification</summary>

All four links of the claimed chain check out in the actual code:

1. WAL durability path reaches the panicking encode arm. crates/uni-store/src/runtime/wal.rs:134-144 `cv_props::serialize` iterates props and calls `uni_common::cypher_value_codec::encode(v)` for each (line 139). This is the WAL mutation-log property serializer on the commit/durability path.

2. The encode arm panics on invalid sparse invariants. crates/uni-common/src/cypher_value_codec.rs:436-440: `Value::SparseVector { indices, values } => { ... let sv = uni_sparse_vector::SparseVector::new(indices.clone(), values.clone()).expect("invalid SparseVector value"); ... }`. The `.expect` panics on Err.

3. `SparseVector::new` returns Err for malformed input. crates/uni-sparse-vector/src/sparse.rs:30-52 rejects length mismatch (31-36), non-strictly-ascending/duplicate/unsorted indices (37-45), and non-finite weights (46-50). So `{indices: vec![5,1], values: vec![1.0,2.0]}` (unsorted) or a NaN weight => Err => panic.

4. Write-time validation does NOT re-check invariants. crates/uni-common/src/core/schema.rs:387-389 `DataType::SparseVector { .. } => matches!(value, Value::SparseVector { .. } | Value::Map(_))` is a variant-only check. crates/uni-query/src/query/executor/write.rs:3022-3056 `coerce_and_validate_property_value` early-returns at line 3054 when `dt.accepts(&val)` is true, with no sparse-invariant validation; schemaless properties (no declared type) skip type checks entirely too. So an invalid Value::SparseVector passes validation and reaches the WAL serializer.

Reachability: the Rust embedded `.param()` API (crates/uni/src/api/template.rs:95, transaction.rs:1395) is `V: Into<Value>` and stores the value verbatim with no canonicalization, so a directly-constructed invalid `Value::SparseVector` flows through. The Python binding is safe (PySparseVector canonicalizes via from_pairs). Notably, the parallel call sites that build SparseVector from a Value on read/query paths handle the error gracefully (search_procedures.rs:531/561 use match/map_err; similar_to.rs:364), confirming the codec arm's `.expect` is the outlier.

Test gap confirmed: no test constructs an invalid Value::SparseVector and writes/commits it. The only invalid-input tests (sparse.rs:135-158) exercise `SparseVector::new` directly and never reach the codec or WAL.

**Severity correction:** The bug is real but the "plausible (if malformed) input" / "plausible input" framing overstates reachability. The panic is NOT reachable from Cypher query literals or the Python binding (both canonicalize through SparseVector::new/from_pairs). It is only reachable by a host embedding the Rust crate and directly constructing `Value::SparseVector{..}` with bad invariants (bypassing new/from_pairs) and passing it via .param(). That is an internal programming error, not arbitrary external input, so likelihood is lower than "high" implies. The impact (panic inside an async commit/durability task) is genuinely serious, hence medium rather than low. CLAIMED LINE/LOC was 436-440; exact `.expect` is at lines 438-439 — accurate.

</details>

---

### `sparse/reliability/SP-REL-2` — MEDIUM
**Flushed in-place UPDATE of an indexed sparse column leaks stale duplicate postings**

- **File:** `crates/uni-store/src/runtime/writer.rs`
- **Location:** 4728-4741 (sparse_updates collection) + sparse_index.rs:597-621 (apply_incremental_updates)
- **Category:** reliability

**Description:** When a sparse-indexed vertex is updated in place (SET d.emb = ...) and that change is flushed, the writer's incremental-update collection puts the vid into `added` only — a vid enters `removed` only when `*deleted` (writer.rs:4729-4740) or as a tombstone. `apply_incremental_updates` (sparse_index.rs) removes vids in `removed`, then unconditionally *appends* the `added` `(term,weight)` pairs WITHOUT first dropping the vid's prior postings (line 612-617). So every flushed update of a sparse vector leaves both the old and the new posting entries for that vid in the on-disk postings. Over repeated updates this is unbounded index bloat, and in `query_topk` the per-vid score accumulator double-counts the old+new weights, inflating that vid's candidate score and distorting the top-`retrieval_k` candidate set (it can crowd out or mis-rank genuine matches within the over-fetch window). Final query results stay correct ONLY because `sparse_rerank` dedups by vid and re-scores exactly against the authoritative property; the index itself is left inconsistent. The existing test `sparse_l0_update_last_writer_wins` exercises an *unflushed* L0 update (which never touches apply_incremental_updates), so this flushed-update path is uncovered.

**Evidence:**
```
writer.rs:4729-4740: vid added to `removed` only `if *deleted`, else (SparseVector present) added to `added`; sparse_index.rs:612-617: `for (vid, terms) in added { ... postings.entry(term).or_default().push((vid_u64, weight)); }` with no prior removal of that vid's existing entries.
```

**Suggested fix:** In `apply_incremental_updates` (or in the writer collection), treat every `added` vid as also `removed`: before appending, retain-filter out all postings whose vid is in `added.keys()` (union the added vids into the removal set), so an in-place update replaces rather than accumulates. Add a regression test: insert+flush a sparse doc, SET a different sparse value, flush, then assert the on-disk postings contain exactly one entry for that vid.

<details><summary>Independent verification</summary>

All claims verified against the actual code.

(1) writer.rs:4728-4740 — for each vertex, the vid is inserted into `removed` ONLY `if *deleted` (line 4729-4730); otherwise, when a SparseVector property is present, its (term,weight) pairs go into `added` (line 4731-4740). Tombstones additionally feed `removed` (4742-4747). So an in-place flushed SET update (not a delete, not a tombstone) places the vid in `added` only, NEVER in `removed`.

(2) sparse_index.rs:597-621 — `apply_incremental_updates` loads postings, retains-out only vids in `removed` (604-610), then for every (vid,terms) in `added` does `postings.entry(term).or_default().push((vid_u64, weight))` (612-617) with NO prior removal of that vid's existing postings. Because postings are `type Postings = HashMap<u32, Vec<(u64,f32)>>` (line 47) — a Vec, not a set — and `write_postings` (322-361) serializes every Vec entry verbatim with no dedup, the vid's old and new postings both persist on disk after a flushed update. Confirmed unbounded bloat over repeated updates.

(3) The doc comment claims it "mirrors the set-membership inverted index", but inverted_index.rs:437 uses `HashSet::insert(vid_u64)` per term, which is idempotent for same-term re-adds — so the inverted index naturally avoids the same-vid duplicate that the sparse Vec path creates. The "mirrors" comment is misleading for the in-place-update case.

(4) query_topk double-counts: sparse_index.rs:501 `*scores.entry(vids.value(j)).or_insert(0.0) += qw * weights.get(j)` — a vid appearing twice in a term's posting list has both weights added, inflating its candidate score and distorting the top-`retrieval_k` over-fetch set when the updated vector overlaps the query.

(5) Final results stay correct only via rerank: search_procedures.rs:479-552 dedups candidates by vid (HashSet `seen`, lines 500-511) and re-scores each exactly once against the authoritative property via `sparse_dot` (524-546). So the bug is index-internal (bloat + candidate distortion), not a result-correctness defect — exactly as the finding states.

(6) Test gap confirmed: `sparse_l0_update_last_writer_wins` (sparse_index.rs test:472) performs an UNFLUSHED L0 update and asserts only on query results, never reaching apply_incremental_updates. The nearest test, `sparse_recovered_update_overrides_stale_posting_without_rebuild` (test:806), DOES flush an in-place update at stage [B] (line 845 `db.flush()`), but it uses a DISJOINT replacement vector and only asserts the doc drops from results — rerank's re-score masks the stale duplicate, so it cannot detect the leak. No test calls `load_postings` to count entries; grep over the whole repo shows `load_postings` is invoked only from production code. The flushed-update posting-count path is genuinely uncovered.

</details>

---

### `sparse/safety/SP-SAFE-2` — MEDIUM _(reviewer said high; verifier corrected to medium)_
**Sparse index ingests stored documents without re-validating invariants, so NaN/Inf weights poison scoring and top-k ordering**

- **File:** `crates/uni-store/src/storage/sparse_index.rs`
- **Location:** read_sparse_row 71-90; accumulate_batch 260-269; query_topk scoring 497-507; HeapEntry::cmp 651-657
- **Category:** safety

**Description:** read_sparse_row reads the on-disk Struct{indices,values} into raw Vec<u32>/Vec<f32> and pushes them straight into the postings (accumulate_batch line 265-266) without going through SparseVector::new — so any non-finite weight or unsorted index that slipped past ingest (see SP-SAFE-1, or a corrupted/hand-built segment) is indexed verbatim. At query time `*scores.entry(vid).or_insert(0.0) += qw * weights.get(j)` (line 501): a single stored NaN weight produces NaN for that vid's accumulated score, and the bounded min-heap orders via `self.score.partial_cmp(&other.score).unwrap_or(Ordering::Equal)` (HeapEntry::cmp line 653-655), so the NaN entry compares Equal to everything and can be retained or evicted nondeterministically — silently corrupting the top-k result set rather than erroring. The lossless f32 write path (write_postings line 352-361) also propagates a non-finite stored weight into max_impact (it only resets NEG_INFINITY/non-finite max to 0.0, but a finite-looking NaN-containing list still poisons per-vid scores).

**Evidence:**
```
for (term, weight) in indices.into_iter().zip(values) { postings.entry(term).or_default().push((vid, weight)); }  // accumulate_batch:265-266 — no SparseVector::new revalidation
*scores.entry(vids.value(j)).or_insert(0.0) += qw * weights.get(j);  // query_topk:501
self.score.partial_cmp(&other.score).unwrap_or(std::cmp::Ordering::Equal)  // HeapEntry::cmp:653-655
```

**Suggested fix:** Fix SP-SAFE-1 (validate at ingest) so non-finite/unsorted weights can never be persisted; additionally make read_sparse_row defensive — skip or zero non-finite weights when accumulating into postings, mirroring sparse_pairs_to_value (writer.rs:85 `if weight.is_finite()`). The dense vector_search and brute-force sparse_rerank already guard against this (sparse_rerank re-validates via SparseVector::new at search_procedures.rs:531), but the flushed-index query_topk path does not.

<details><summary>Independent verification</summary>

All four cited code sites are verbatim accurate. read_sparse_row (sparse_index.rs:71-90) collects raw Vec<u32>/Vec<f32> from the on-disk Struct{indices,values} with no validation. accumulate_batch (265-266) pushes `(vid, weight)` into postings directly — `for (term, weight) in indices.into_iter().zip(values) { postings.entry(term).or_default().push((vid, weight)); }` — never routing through uni_sparse_vector::SparseVector::new (which DOES enforce SV-2 sorted-unique and SV-3 finiteness, sparse.rs:39/47). query_topk (501) accumulates `*scores.entry(...).or_insert(0.0) += qw * weights.get(j)`, so a stored NaN weight produces a NaN accumulated score. HeapEntry::cmp (652-656) uses `partial_cmp(...).unwrap_or(Ordering::Equal)`, so a NaN-scored vid compares Equal and is retained/evicted nondeterministically — silent top-k corruption, no error. The lossless write path (358-360) only resets a non-finite max_impact; the per-vid weights pass through unchanged (max_impact only feeds future P2 block-max pruning, so it does not gate today's score). The asymmetry the finding claims is real: the L0 brute-force re-score path (search_procedures.rs:531-534) calls SparseVector::new and returns score 0.0 on invalid input, while the flushed-index path performs no revalidation. The Lance struct build path is also unguarded — sparse_pair_from_value (arrow_convert.rs:1176) clones indices/values through and build_sparse_vector_column (1681-1682) appends each `w` unconditionally. No existing test exercises a stored-NaN read/score on the flushed index (only quantize_term/dequantize finiteness asserts at lines 711/749).

**Severity correction:** The defense-in-depth gap and NaN-nondeterministic heap ordering are real and correctly described, but the headline severity is overstated for the live param path. A NaN weight is NOT independently reachable through the normal happy path: the auto-embed path drops non-finite weights (writer.rs:85 `if weight.is_finite()`), and the WAL cypher_value_codec encode path calls SparseVector::new(...).expect(...) (cypher_value_codec.rs:438), which panics rather than silently persists a NaN. The finding itself concedes reachability is gated on SP-SAFE-1 ("once SP-SAFE-1 is bypassed/forced") or on a corrupted/hand-built segment. So today the realistic trigger is on-disk corruption or a future ingest-validation regression, not the standard parameter write path. That makes this a genuine latent safety/robustness defect (the read path trusts on-disk bytes with zero revalidation, and the heap is NaN-nondeterministic), but conditional — medium rather than high.

</details>

---

### `sparse/safety/SP-SAFE-3` — MEDIUM
**sparse_pair_from_value does not enforce equal length for the native Value::SparseVector arm**

- **File:** `crates/uni-store/src/storage/arrow_convert.rs`
- **Location:** 1176-1203 (sparse_pair_from_value); used by build_sparse_vector_column 1656-1704
- **Category:** safety

**Description:** sparse_pair_from_value enforces `indices.len() == values.len()` only for the degraded Value::Map form (line 1196-1198) but returns the native `Value::SparseVector { indices, values }` arm (line 1178) verbatim with no length check. build_sparse_vector_column then appends the indices list and values list independently into two separate ListBuilders (lines 1677-1684), so a length-mismatched value yields an Arrow sparse struct whose `indices` and `values` child lists have different lengths for that row. read_sparse_row (sparse_index.rs:87-88) later zips them with `.zip()`, silently truncating to the shorter — pairing weights with the wrong term ids and dropping terms, i.e. silent wrong scores. This is the storage-layer manifestation of the same missing-validation root cause as SP-SAFE-1, on the Arrow (non-WAL) column path which does not panic but corrupts instead.

**Evidence:**
```
Value::SparseVector { indices, values } => Some((indices.clone(), values.clone())),  // arrow_convert.rs:1178 — no len check
// vs the Map arm: if indices.len() != values.len() { return None; }  // line 1196-1198
// build appends independently:
for ix in indices { indices_builder.values().append_value(ix); } indices_builder.append(true);
for w in values { values_builder.values().append_value(w); } values_builder.append(true);  // 1677-1684
```

**Suggested fix:** Add the same `if indices.len() != values.len() { return None; }` guard (or full SparseVector::new validation) to the native Value::SparseVector arm of sparse_pair_from_value so a mismatched value becomes a null row instead of a desynced indices/values pair. Ideally validate once at ingest (SP-SAFE-1) so this layer can assume well-formed input.

<details><summary>Independent verification</summary>

All cited code matches exactly. arrow_convert.rs:1178 returns the native `Value::SparseVector { indices, values }` arm verbatim (indices.clone(), values.clone()) with NO length check, while the degraded Map arm at lines 1196-1198 DOES enforce `if indices.len() != values.len() { return None; }`. This asymmetry is real. build_sparse_vector_column (1656-1704) appends indices and values into two independent ListBuilders in separate loops (1677-1684), each followed by its own .append(true), so a length-mismatched pair yields a row whose `indices` and `values` child lists have different lengths. build_sparse_vector_array (1210+) has the identical pattern. The read side is correctly characterized: read_sparse_row (sparse_index.rs:71-90) reads the two child lists independently, and the consumer at sparse_index.rs:265 does `indices.into_iter().zip(values)`, which silently truncates to the shorter list — mispairing term ids with weights and dropping terms (silent wrong scores), no panic. The Value::SparseVector variant (uni-common/src/value.rs:559-564) is a plain public-field struct variant whose parallel-array invariant exists only as a doc comment; nothing enforces equal length at construction, so a mismatched value IS constructible and reaches line 1178 unchecked. No existing test covers this: the only *_length_mismatch tests (similar_to_integration.rs:346,889) exercise the similar_to() query-function weights argument, an unrelated path — they do not touch build_sparse_vector_column / sparse_pair_from_value. The 'same root cause as SP-SAFE-1, on the Arrow non-WAL column path that corrupts rather than panics' framing is reasonable.

**Severity correction:** Accurate as written. One nuance on exploitability (does not change severity): the normal write path constructs SparseVector via sparse_pairs_to_value (writer.rs:82-93) using a BTreeMap, which always yields equal-length parallel arrays, so this is a defense-in-depth gap reachable only when a Value::SparseVector with a violated invariant is supplied from another source (raw user value, a future caller, or a degraded round-trip recovered into the native variant) rather than something the standard ingest path triggers on its own. The medium severity (silent score corruption, no crash, but not trivially reachable through default ingest) is appropriate.

</details>

---

### `sparse/test-gap/SP-TG-1` — MEDIUM _(reviewer said high; verifier corrected to medium)_
**Benchmark recall@10 is printed but never asserted; no scale test exercises the k*4 over-fetch candidate cutoff**

- **File:** `crates/uni/benches/sparse_retrieval.rs`
- **Location:** 211-214 (println recall); recall_at_k 172-190
- **Category:** test-gap

**Description:** The benchmark computes recall@K against the brute-force oracle but only emits it with println!("[sparse_retrieval] docs={n} weights={kind} recall@{K}={recall:.3}"); there is no assert. The live query path (run_sparse_query -> sparse_rerank) uses the index as a candidate generator limited to retrieval_k = k * MULTIVECTOR_OVER_FETCH (=4) before the exact rescore. On a large corpus a true top-k doc whose QUANTIZED candidate-stage score falls below the k*4 cutoff is dropped before rerank, so recall can silently fall below 1.0 — exactly the regime the bench runs (2k/10k docs) but never checks. The metamorphic test (the only oracle test that varies queries) uses CORPUS=60, k<=10, retrieval_k<=40, so the over-fetch margin covers essentially the whole overlapping candidate set and never stresses the cutoff. Result: the headline 'recall@10=1.000' correctness claim is unpinned by any failing-on-regression assertion.

**Evidence:**
```
bench: `println!("[sparse_retrieval] docs={n} weights={kind} recall@{K}={recall:.3}");` with no assert; proc: `let retrieval_k = (((k as f64) * over_fetch).ceil() as usize).max(k);` and `MULTIVECTOR_OVER_FETCH: usize = 4`
```

**Suggested fix:** Add an integration test that builds a few-thousand-doc skewed corpus, runs uni.sparse.query, and asserts recall@K == 1.0 (or a documented floor) against the f64 oracle, so a future change to over_fetch / quantization / candidate-gen that regresses recall fails CI. Optionally also assert recall inside the bench.

<details><summary>Independent verification</summary>

Verified every cited line. In crates/uni/benches/sparse_retrieval.rs the bench computes recall_at_k (line 213) then only println!s it (line 214: `[sparse_retrieval] docs={n} weights={kind} recall@{K}={recall:.3}`); the sole assert in the timed loop is `assert!(!titles.is_empty())` (line 219). Bench scales = vec![2_000, 10_000] (line 116), K=10, run for both quantize=true (int8) and false. In crates/uni-query/src/query/df_graph/search_procedures.rs, MULTIVECTOR_OVER_FETCH=4 (line 326) and run_sparse_query sets `retrieval_k = (((k as f64) * over_fetch).ceil() as usize).max(k)` (line 647), with over_fetch defaulting to MULTIVECTOR_OVER_FETCH — the index is a candidate generator capped at k*4 before sparse_rerank exact rescore. The only query-varying oracle test (crates/uni/tests/common/metamorphic/sparse.rs) uses CORPUS=60, VOCAB=256, query nnz 1..=8, k 1..=10 (so retrieval_k<=40 of 60 docs), and builds the index UNquantized (IndexType::sparse(VOCAB), no int8) — so it never stresses the k*4 cutoff nor the quantized candidate-stage drift that the bench recall number is meant to certify. grep for `recall` in tests finds only multivector/fork recall benches, none asserting sparse recall. So the headline recall@10=1.000 at 2k/10k quantized scale is unpinned by any failing-on-regression assertion, and no existing test meaningfully covers the candidate-cutoff regime. Test-gap confirmed.

**Severity correction:** The finding is accurate. Severity adjusted from high to medium: this is a missing-assertion/coverage gap, not a demonstrated live bug, and a genuinely strong exact-equivalence oracle (score fidelity + rank-by-rank within EPS) does exist in the metamorphic test — it just runs at CORPUS=60 with an unquantized index, which is precisely the regime that cannot expose the quantized k*4 candidate-cutoff recall loss. So the gap is real and worth closing (the bench's recall number is decorative, and no scale/quantized test guards it), but it is one step removed from a confirmed correctness defect.

</details>

---

### `sparse/test-gap/SP-TG-2` — MEDIUM _(reviewer said high; verifier corrected to medium)_
**No test that a malformed/out-of-range stored sparse vector is rejected on write; the write path stores it verbatim and the read path silently scores it 0**

- **File:** `crates/uni-store/src/storage/arrow_convert.rs`
- **Location:** build_sparse_vector_column 1656-1704
- **Category:** test-gap

**Description:** Value::SparseVector is a plain struct (uni-common/src/value.rs ~559) constructible directly, bypassing SparseVector::new validation. build_sparse_vector_column appends indices/values verbatim with no check that indices are sorted/unique, lengths match, weights are finite, or term ids are < the declared `dimensions`. There is no write-time validation pass for sparse values anywhere (grep found none). On read, sparse_rerank reconstructs via SparseVector::new and on failure does `Err(_) => 0.0` (search_procedures.rs line 533), so an unsorted/duplicate stored doc is silently scored 0 and dropped rather than surfaced as an error — and an out-of-range term id (term_id >= dimensions) is never rejected at all. No test in sparse_ddl_type.rs, sparse_index.rs, or sparse_scoring.rs feeds an invalid/out-of-range sparse value through CREATE/SET and asserts either rejection or well-defined behavior.

**Evidence:**
```
arrow_convert: `for ix in indices { indices_builder.values().append_value(ix); }` (no bound/sort check); rerank: `Err(_) => 0.0` swallowing an invalid reconstructed doc
```

**Suggested fix:** Add tests that CREATE/SET a Doc with (a) term_id >= declared dimensions, (b) unsorted indices, (c) duplicate indices, (d) indices.len()!=values.len(), (e) NaN/inf weight via the param surface, asserting either a clear validation error at write or documented coercion. If the engine is meant to validate, add the validation; currently it neither validates nor has a test pinning the silent-0 behavior.

<details><summary>Independent verification</summary>

I re-derived every claim against the actual code.

WRITE PATH: crates/uni-store/src/storage/arrow_convert.rs build_sparse_vector_column (1656-1704) appends indices/values verbatim (lines 1677-1684) with zero validation — no sortedness, uniqueness, length-equality, finiteness, or term_id < dimensions check. sparse_pair_from_value (1176-1203) for the Value::SparseVector branch (1178) only clones; only the Map branch has a length-equality check (1196). The struct-shape arm for DataType::SparseVector in the type dispatch only warns on Arrow shape mismatch and never validates content.

VALUE TYPE: uni-common/src/value.rs 559-564 — Value::SparseVector is a plain public-field struct, directly constructible, bypassing uni_sparse_vector::SparseVector::new validation (sparse.rs new() validates LengthMismatch/UnsortedIndices(also catches duplicates)/NonFiniteWeight at lines 30-48).

DECLARED DIMENSION: DDL SPARSE_VECTOR(N) maps to DataType::SparseVector { dimensions: N } (sparse_ddl_type.rs:36-38 declares SPARSE_VECTOR(1000)). SparseVector::new takes NO dimensions argument, and the write path never uses N to bound-check term ids — so term_id >= dimensions is genuinely never rejected anywhere.

READ PATH: crates/uni-query/src/query/df_graph/search_procedures.rs sparse_rerank, lines 529-545. Line 533 `Err(_) => 0.0` swallows a failed SparseVector::new reconstruction; line 543 then drops score==0 docs silently. This is asymmetric: the query-vector side at line 561 surfaces the same error via map_err. So an unsorted/duplicate/non-finite STORED doc is silently scored 0 and dropped, not surfaced.

TEST GAP: Grepped all sparse integration tests (sparse_ddl_type.rs, sparse_index.rs, sparse_scoring.rs, sparse_autoembed.rs, common/sparse_resilience.rs). sparse_ddl_type.rs only round-trips a VALID vector and an empty vector (lines 34-84) with no rejection/defined-behavior assertion. No test feeds an unsorted/duplicate/non-finite/out-of-range (term_id >= declared N) sparse value through CREATE/SET. No should_panic/expect_err/is_err/invalid-input coverage exists. The only validation coverage is the leaf-crate unit tests on SparseVector::new in isolation (sparse.rs 134-159), which never exercise the store write/read path.

All four pillars of the finding (write stores verbatim, no write-time validation pass, read swallows Err->0.0 and drops, no integration test) are confirmed exactly as described.

**Severity correction:** The finding is accurate in every technical particular. I lower severity from high to medium: the failure mode requires a caller to hand-construct an invalid Value::SparseVector (the normal SPLADE/auto-embed ingestion path and uni.sparse.query both build sorted-unique vectors via SparseVector::new / from_pairs), so this is a garbage-in path, not a routine one. The consequence is bounded — a silent score-0/drop and an unenforced declared-dimension bound — rather than data corruption, a panic, or a crash. It is a real correctness/observability gap (silent-drop instead of surfaced error, asymmetric with the query side which does surface it) and a genuine test gap worth closing, but its blast radius is narrower than a high.

</details>

---

### `sparse/test-gap/SP-TG-3` — MEDIUM
**Quantized DAAT scoring in query_topk is never validated for score correctness end-to-end (rerank discards index scores)**

- **File:** `crates/uni-store/src/storage/sparse_index.rs`
- **Location:** query_topk 437-507; top_k_from_scores 511-530; HeapEntry 634-658
- **Category:** test-gap

**Description:** query_topk computes a dot-product accumulator over dequantized 8-bit posting weights and ranks via a bounded min-heap, returning (Vid, score). But every live query (uni.sparse.query, uni.search sparse arm, fork path) routes through sparse_rerank, which uses these results ONLY as a candidate VID set and then re-scores exactly from the lossless stored Value::SparseVector — the index's returned score is explicitly discarded (search_procedures.rs: 'The returned score is a placeholder (the only caller re-ranks)'). Consequently the quantized-accumulator scoring math, the min-heap ordering, the dequantize path in TermWeights, and the k>n / k==0 / empty-query branches of query_topk are exercised for membership but never asserted for SCORE accuracy or ordering against an oracle at the storage layer. The only direct unit tests of this module are top_k_from_scores ordering/tie-break and quantize_term bounds — not query_topk itself.

**Evidence:**
```
search_procedures.rs comment: "The returned score is a placeholder (the only caller re-ranks)."; query_topk has no #[cfg(test)] direct test — the module's tests cover merge_postings_segments, top_k_from_scores, quantize_term only.
```

**Suggested fix:** Add a storage-layer async test that builds a small SparseVectorIndex (both quantize=true and false), calls query_topk directly, and asserts the returned (vid, score) ranking matches a brute-force oracle within a quantization tolerance; include k>num_terms, k==0, empty-query, and a tie-break case.

<details><summary>Independent verification</summary>

I verified every load-bearing claim by reading the code.

1. query_topk (sparse_index.rs:437-507) computes a dot-product accumulator over dequantized 8-bit posting weights (line 495 `term_weights(...)` dequantizes, line 501 `*scores.entry(...) += qw * weights.get(j)`), then ranks via top_k_from_scores's bounded min-heap (511-530) and returns Vec<(Vid, f32)>. It has k==0/empty-query early-out (442-444) and a None-dataset early-out (438-441).

2. The ONLY production caller is manager.rs:2063 `idx.query_topk(query, k)` inside StorageManager::sparse_search. The other `query_topk` grep hits are an unrelated local fn in examples/multivec_recall_*.rs.

3. sparse_search is itself only consumed by sparse_rerank (search_procedures.rs:479-552). At line 491-494 it gets `flushed`; at line 502 it iterates `for (vid, _) in &flushed` — the index score is bound to `_` and DISCARDED, used only to seed the candidate VID set. Lines 524-545 re-score every candidate EXACTLY from the lossless stored Value::SparseVector via uni_sparse_vector::ops::sparse_dot, then sort/truncate (549-550). manager.rs:1992-1994 and the inline comment at 2023 corroborate: "the prelim score is advisory" / "The returned score is a placeholder (the only caller re-ranks)." (The finding cited the comment as being in search_procedures.rs; it actually lives in manager.rs — minor location error, substance correct.)

4. The #[cfg(test)] module (sparse_index.rs:660-753) contains NO test invoking query_topk. Its tests cover merge_postings_segments, top_k_from_scores (ordering/tie-break/empty), and quantize_term/dequantize bounds only — exactly as claimed.

5. Integration tests in uni/tests/sparse_index.rs DO assert exact dot scores against a brute-force oracle (assert_matches_oracle, 185-217), including a quantized path (sparse_quantized_and_lossless_agree). But the asserted `score` field comes from uni.sparse.query → sparse_rerank's exact sparse_dot re-score, NOT from query_topk's quantized accumulator (which is discarded at search_procedures.rs:502). So these oracle tests validate (a) that quantized term-matching surfaces the right candidate set and (b) that the rerank's lossless re-score is exact — but they never assert query_topk's own quantized-accumulator score values, its min-heap ordering, or its k>n/k==0/empty-query branches for SCORE/ORDER correctness. The branches are exercised for membership only.

The finding is real and accurately characterized as a test-gap. Medium severity is appropriate, not higher: because the live path discards index scores and re-scores losslessly, a scoring/ordering bug in query_topk cannot corrupt query results — it could only affect recall via retrieval_k truncation (a candidate ranked out of the top retrieval_k by a buggy accumulator would be dropped). That recall risk keeps it above low/info, but the lossless rerank caps the blast radius below high.

**Severity correction:** The cited "placeholder" comment lives in crates/uni-store/src/storage/manager.rs (lines 1992-1994, 2023), not in search_procedures.rs as the finding states. The substance is unchanged — sparse_rerank (search_procedures.rs:502) does discard the index score and re-score via sparse_dot. Additionally: query_topk IS indirectly exercised end-to-end by uni/tests/sparse_index.rs (including a quantized variant), but only for candidate-set membership — those oracle assertions read the rerank's exact score, never query_topk's quantized accumulator output — so the test-gap claim holds.

</details>

---

### `sparse/test-gap/SP-TG-4` — MEDIUM
**Procedure-surface edge cases for uni.sparse.query (k=0, empty query vector, over_fetch and threshold options) are untested**

- **File:** `crates/uni-query/src/query/df_graph/search_procedures.rs`
- **Location:** run_sparse_query 614-690 (threshold 638, over_fetch 643-647)
- **Category:** test-gap

**Description:** run_sparse_query reads an `over_fetch` option and a `threshold` (min score) argument and computes retrieval_k from them, and query_topk has explicit `query.is_empty() || k == 0 => Ok(Vec::new())` guards. None of these are exercised: grep of all sparse integration tests (sparse_index.rs, sparse_scoring.rs, sparse_autoembed.rs, metamorphic/sparse.rs, fork_index_sparse.rs, sparse_resilience.rs) shows zero references to `over_fetch` or `threshold`, and no test passes k=0 or an empty SparseVector query. So the threshold filter (`results.retain(|(_, s)| *s >= min_score)`), the over_fetch parsing/clamp (`filter(|f| *f >= 1.0)`), and the empty/k=0 early-returns are unguarded by tests — a regression in any of them would pass CI.

**Evidence:**
```
`if let Some(min_score) = threshold { results.retain(|(_, s)| *s >= min_score as f32); }` and `.filter(|f| *f >= 1.0).unwrap_or(MULTIVECTOR_OVER_FETCH as f64)`; tests grep for over_fetch/threshold in sparse tests returns nothing
```

**Suggested fix:** Add tests for: uni.sparse.query with k=0 (empty result), an empty query vector (empty result), a `threshold` that filters out low-overlap docs (assert only docs with score>=threshold returned), and a small/large `over_fetch` (assert recall/candidate behavior).

<details><summary>Independent verification</summary>

Verified the cited code in crates/uni-query/src/query/df_graph/search_procedures.rs::run_sparse_query (lines 614-692): threshold is read via extract_optional_threshold(args,5) and applied at lines 669-671 (`results.retain(|(_,s)| *s >= min_score as f32)`); over_fetch is parsed at lines 642-647 with the `.filter(|f| *f >= 1.0).unwrap_or(MULTIVECTOR_OVER_FETCH as f64)` clamp feeding retrieval_k at line 647. The empty/k=0 guard exists in uni-store/src/storage/sparse_index.rs::query_topk line 442 (`if query.is_empty() || k == 0`). Test coverage: every single `uni.sparse.query` invocation across all test files (sparse_index.rs, sparse_autoembed.rs, sparse_scoring.rs, metamorphic/sparse.rs, fork_index_sparse.rs, sparse_resilience.rs, ssi_read_path_matrix.rs, autoembed_parity.rs) passes the literal arg tuple `null, null, {}` for (filter, threshold, options) — never a non-null threshold, never an over_fetch option. k is always positive (1, 5, $k) and queries are non-empty sparse vectors. `over_fetch` in tests appears only for uni.vector.query/multivector (multi.rs) and reranker (reranker_integration.rs), never sparse. The sparse_index.rs unit test `test_top_k_empty` exercises `top_k_from_scores(HashMap::new(), 5)` — a different helper, NOT the query_topk empty/k=0 guard. So the threshold retain, over_fetch clamp, and empty/k=0 early returns on the sparse procedure surface are all untested; a regression would pass CI. The finding is real and correctly characterized. Minor immaterial error in the HOW-TO-VERIFY: the matching fork string is `fork_index_build_n`, not `fork_index_build_threshold` — neither contains an over_fetch/threshold sparse-query test, so the conclusion is unaffected.

**Severity correction:** No substantive correction to the finding. Only a trivial inaccuracy in the verification hint: the unrelated fork test string is `fork_index_build_n` (not `fork_index_build_threshold`), which has no bearing on the conclusion. Severity medium is reasonable, leaning to the lower edge since these are option-parsing/early-return edges rather than core retrieval-correctness paths, but the threshold-retain and over_fetch retrieval_k computation do shape results and a silent regression there would pass CI.

</details>

---

### `multivec/completeness/SP-COMPL-1` — LOW
**MUVERA real-corpus recall benchmark exists but is un-run / untuned; shipped FDE defaults are explicitly not recall-validated**

- **File:** `crates/uni-common/src/muvera.rs`
- **Location:** 44-51 (module doc 'Parameter tuning'); bench crates/uni-store/examples/multivec_recall_real.rs
- **Category:** completeness

**Description:** The MUVERA Phase-3 proposal (docs/proposals/multivector_colbert_maxsim.md:264-269) and the muvera.rs module doc both state the default FDE params (k_sim=4, reps=20, d_proj=16) are starting points NOT validated for recall on any real ColBERT corpus, and that the authoritative recall/latency GO number from multivec_recall_real.rs has not been produced for a Phase-3 number. The exact MaxSim re-rank guarantees a weak FDE only costs recall (never precision), so this is a tuning/validation gap rather than a correctness gap, but it means the first-stage MUVERA index ships with unverified recall characteristics. This matches the project memory note ('Only open item: MUVERA FDE real-corpus recall bench (untuned)').

**Evidence:**
```
muvera.rs:47-51: 'The shipped defaults (k_sim=4, reps=20, d_proj=16ÔÇª) are reasonable starting points but are **not** validated for recall on any particular corpus. ÔÇª measure recall@k on a real ColBERT corpus with crates/uni-store/examples/multivec_recall_real.rs and tune from there.'
```

**Suggested fix:** Run multivec_recall_real.rs against a representative ColBERT corpus/model, record a recall@k GO number in the proposal, and either confirm or adjust the shipped defaults. Until then, document prominently in user-facing index docs that MUVERA recall is corpus-dependent and unbenchmarked at default params.

<details><summary>Independent verification</summary>

I re-derived every claim against the actual code/docs.

1. muvera.rs:44-51 (module doc "Parameter tuning"): verbatim states the shipped defaults `k_sim=4, reps=20, d_proj=16` "are reasonable starting points but are **not** validated for recall on any particular corpus" and directs the reader to "measure recall@k on a real ColBERT corpus with `crates/uni-store/examples/multivec_recall_real.rs` and tune from there." Also explicitly notes "Synthetic self-retrieval ... is NOT evidence of real recall" and that "a poor FDE only costs recall, never precision" (because of the exact MaxSim re-rank). Matches the EVIDENCE CLAIMED exactly.

2. docs/proposals/multivector_colbert_maxsim.md:264-269 ("Parameter caveat"): states defaults are "**not** recall-validated on a real corpus" and that "the recall bench (`crates/uni-store/examples/multivec_recall_real.rs`) requires a real ColBERT MVEC corpus and has not been run for a Phase-3 GO number."

3. The bench example (multivec_recall_real.rs) confirms: it reads `$BENCH_DIR/docs.bin` and `queries.bin` in MVEC format (real ColBERT embeddings from uni-xervo), errors out via `.context("read docs.bin")` if absent, and computes the MUVERA recall@k at runtime (line 178 prints `recall@{K} = {:.3}`). No hardcoded/committed result.

4. Grep across the whole repo: the only committed recall@10 ≈ 0.966 number (proposal lines 126/131/358) is explicitly for the **Phase-2** native IVF_PQ+refine multivector index, NOT Phase-3 MUVERA/FDE. There is NO committed Phase-3 MUVERA recall number, no committed corpus .bin files, and no results artifact anywhere.

5. This matches the project-memory note: "Only open item: MUVERA FDE real-corpus recall bench (untuned)."

The finding is real and correctly characterized as a tuning/validation gap, not a correctness gap — the exact MaxSim re-rank guarantees a weak FDE costs only recall (first-stage candidate completeness), never precision. Severity 'low' is appropriate: the index ships functional and self-healing, with unverified-but-bounded recall characteristics on real corpora, and the gap is honestly documented in both the module doc and the proposal.

</details>

---

### `multivec/completeness/SP-COMPL-2` — LOW
**OGM hybrid_search() builder not implemented (proposal-acknowledged deferral #114)**

- **File:** `bindings/uni-pydantic/src/uni_pydantic/query.py`
- **Location:** 433 (vector_search), 453 (sparse_search) — no hybrid_search
- **Category:** completeness

**Description:** The sparse proposal (docs/proposals/sparse_vectors_splade.md:54, step 7) lists OGM hybrid_search() as the one remaining out-of-scope OGM item, deferred to GitHub #114. The Pydantic OGM query builder exposes vector_search() and sparse_search() but no hybrid_search()/RRF fusion builder, so a Python OGM user cannot construct a 3-way dense+sparse+BM25 hybrid query through the typed OGM surface (they must drop to raw Cypher / uni.search). This is an explicitly tracked scope boundary, not a regression, but it is a promised-then-deferred surface gap worth surfacing for completeness.

**Evidence:**
```
query.py defines `def vector_search(` (line 433) and `def sparse_search(` (line 453) but no hybrid_search method; proposal step 7: 'Remaining: Ô¼ø OGM hybrid_search() (deferred ÔåÆ #114)'.
```

**Suggested fix:** Either implement the OGM hybrid_search() builder mirroring run_hybrid_search's options, or leave as-is given it is explicitly tracked under #114 — but ensure user docs note that hybrid/RRF search from the OGM requires raw Cypher today.

<details><summary>Independent verification</summary>

I verified the finding directly. In bindings/uni-pydantic/src/uni_pydantic/query.py the QueryBuilder exposes exactly two public retrieval methods: vector_search( (line 433) and sparse_search( (line 453), plus the internal builders _build_vector_search_cypher (574) and _build_sparse_search_cypher (609). A repo-wide grep for "hybrid_search"/"hybrid" across the entire uni-pydantic binding tree (src and tests) returns zero hits — there is no hybrid_search()/RRF-fusion builder on the typed OGM surface. The deferral is explicitly documented in docs/proposals/sparse_vectors_splade.md: line 54 ("OGM hybrid_search() builder — deferred (GitHub issue #114)"), line 258 (step 7: "Remaining: ⬜ OGM hybrid_search() (deferred → #114)"), and line 264 (tracked follow-up #114). The Rust 3-way dense+sparse+BM25 hybrid (run_hybrid_search, FusionKind::SparseRrf/SparseDot) IS implemented, so a Python OGM user can only reach hybrid via raw Cypher/uni.search, not the typed builder. This matches the description precisely: a promised-then-deferred, explicitly tracked OGM surface gap, not a regression. Severity "low" is correct — documented scope boundary with a clear workaround; the proposal itself marks v1 feature-complete with this as a separate tracked enhancement.

</details>

---

### `multivec/concurrency/SP-CORR-1` — LOW
**FDE backfill computes from a non-snapshot live scan, so it can encode tokens from rows a concurrent in-flight transaction may roll back / supersede**

- **File:** `crates/uni-store/src/storage/index_manager.rs`
- **Location:** 413-426 (backfill_fde_column scan)
- **Category:** correctness

**Description:** backfill_fde_column uses backend.scan(ScanRequest::all(&table)) with no version/MVCC filter and no QueryContext, so it reads the raw current on-disk state including any rows written by flushes that landed mid-rebuild and (depending on backend visibility) potentially superseded versions. Because the FDE column is only a first-stage candidate generator and the exact MaxSim re-rank always recomputes from the live source tokens at query time (search_procedures.rs:443-460), a transiently-stale FDE only affects recall, not correctness of returned scores — so this is low severity. But it means the derived FDE column is not guaranteed consistent with the MVCC-visible source column at any single snapshot; combined with SP-REL-1 the same window also drops rows.

**Evidence:**
```
index_manager.rs: `let batches = backend.scan(ScanRequest::all(&table)).await?;` (no with_filter(apply_version_filter), no ctx) versus the query-time read path get_batch_vertex_props_for_label which applies self.storage.apply_version_filter(base_filter) (property_manager.rs:982).
```

<details><summary>Independent verification</summary>

All three citations check out against the actual code.

(1) index_manager.rs:413 — `let batches = backend.scan(ScanRequest::all(&table)).await?;` inside `backfill_fde_column`. There is no `.with_filter(...)`, no `apply_version_filter`, and no QueryContext/snapshot pin. It scans the raw current on-disk flushed state of the vertex table and rewrites it via `replace_table_atomic`. Confirmed verbatim.

(2) property_manager.rs:982 — the query-time read path (`get_batch_vertex_props_for_label`) builds `base_filter` then `let filter_expr = self.storage.apply_version_filter(base_filter);` before scanning. `apply_version_filter` (manager.rs:671) wraps with `AND (_version <= hwm)` only when a snapshot is pinned, else returns the base filter unchanged. So the read path is MVCC/snapshot-aware while the backfill path is not. Confirmed.

(3) search_procedures.rs:443-460 — the exact MaxSim re-rank fetches candidate token properties via `get_batch_vertex_props_for_label(&candidates, label, Some(query_ctx))` (MVCC/tombstone-aware, line 438-439) and recomputes the score with `maxsim(query, &doc_tokens, metric)` for every candidate. A vid absent from the visible props map is dropped; missing property scores 0. So the FDE column only generates first-stage candidates and never contributes to the returned scores. Confirmed.

Characterization is accurate: a transiently-stale FDE (encoding tokens from rows a concurrent in-flight tx may roll back/supersede, or from flushes landing mid-rebuild) only changes which candidates surface in stage 1, i.e. it affects recall, not score correctness, because stage 2 re-scores from the MVCC-visible live source column. The backfill genuinely is not pinned to any single snapshot consistent with the MVCC-visible source column. Low severity is correctly justified; the dropped-rows interaction with SP-REL-1 is plausibly noted but that is a separate finding. I did not find a test guarding snapshot-consistency of the backfilled FDE against the source column, consistent with this being a real (if low-impact) gap.

</details>

---

### `multivec/reliability/SP-REL-3` — LOW
**backfill-failure rollback can race a concurrent identical CREATE INDEX, building the ANN over an unpopulated FDE column**

- **File:** `crates/uni-store/src/storage/index_manager.rs`
- **Location:** 339-374 (prepare_muvera_fde)
- **Category:** reliability

**Description:** prepare_muvera_fde registers the derived column under the schema write-lock (atomic, only one caller gets newly_added=true), then runs backfill_fde_column OUTSIDE the lock. If the inserter's backfill fails it rolls back via drop_property (lines 366-373). A second concurrent create of the same index that observed newly_added=false (because registration had already happened) returns early at line 351 WITHOUT backfilling, then proceeds to build_physical_vector_index over the __fde_ column. If that second create's physical build interleaves after the first create registered the column but before/while the first's backfill is still running or has just been rolled back, the second build runs over an empty/partial FDE column and produces a silently under-populated ANN. The #107 hardening guarantees single-backfill but not single-ANN-build ordering relative to backfill. The window is narrow (concurrent CREATE INDEX of an identical index, with DDL usually serialized) hence low severity.

**Evidence:**
```
index_manager.rs:351 `if !newly_added && !force_backfill { return Ok(()); }` returns before backfill for the racing creator; the rollback at 366-373 `let _ = self.schema_manager.drop_property(...)` removes registration only for the inserter, leaving a racing creator's view inconsistent. The ANN build (build_physical_vector_index, 298) is not synchronized with the other creator's backfill.
```

**Suggested fix:** Serialize the full prepare+backfill+build of a single MUVERA index (e.g. a per-index async mutex), or have the non-inserter await the inserter's backfill completion (a once-cell / latch keyed by index name) before building the physical ANN.

<details><summary>Independent verification</summary>

I traced the cited code and confirmed the mechanism is real. In prepare_muvera_fde (crates/uni-store/src/storage/index_manager.rs:323-375): add_internal_property (schema.rs:1589-1629) holds a write lock and returns Ok(true) only for the FIRST caller, Ok(false) for the second (idempotent re-registration) — verified, atomicity claim holds. That lock is released before backfill_fde_column (line 366) and build_physical_vector_index (line 298) run. The early-return at line 351 `if !newly_added && !force_backfill { return Ok(()); }` is exactly as quoted: the second concurrent creator skips backfill, then create_vector_index_inner proceeds to build_physical_vector_index (line 298) over the __fde_ column.

The rollback at lines 366-373 (`let _ = self.schema_manager.drop_property(...)`) only restores the inserter's view; a racing second creator that already observed newly_added=false and is mid/post physical-build is not affected by it.

Crucially, IndexManager (struct at line 170) has NO internal mutex serializing create_vector_index, and the DDL entry path (read.rs:2491 -> idx_mgr.create_vector_index) acquires no DDL-level lock either, so two concurrent identical CREATE VECTOR INDEX calls genuinely can interleave at this layer. backfill_fde_column writes the FDE column via replace_table_atomic only after the schema-lock release (lines 413-426); before that the flushed table has no __fde_ column, so the second creator's ANN build can run over an empty/missing/partial FDE column.

The silent-under-population claim is reinforced: build_physical_vector_index (lines 504-512) only WARNs on a build failure over an empty column ("column may be empty") and returns Ok — the error is swallowed, so a degenerate ANN is built with no detection. The #107 hardening test (schema.rs:2254 add_internal_property_reports_newly_added) only verifies the newly_added boolean reporting; it does NOT cover ANN-build-vs-backfill ordering between two creators — so the test-gap part of the finding holds.

Caveats lowering practical impact: (1) the window requires two truly concurrent creates of the SAME index name on an already-flushed table with a precise interleaving (second creator's physical build landing after first's registration but before/while first's backfill completes or after rollback); (2) duplicate concurrent identical CREATE INDEX is an unusual workload and DDL is typically serialized by the application; (3) the IF NOT EXISTS guard (read.rs:2487) would short-circuit many such cases, though it checks index_exists_by_name not the FDE column and the two creators could both pass it before either calls add_index. Net: real, correctly characterized, low severity is appropriate.

**Severity correction:** No correction to the core mechanism; the finding is accurate. Minor nuance: build_physical_vector_index does not merely build over partial data — it actively SWALLOWS a build error on an empty column (warn-only, returns Ok at lines 508-512), which is what makes the under-population silent. Severity stays low (could be argued up toward low/medium given the silent-swallow, or down toward info given how contrived the concurrent-identical-DDL trigger is); low is a fair midpoint.

</details>

---

### `multivec/reliability/SP-REL-4` — LOW
**No crash-recovery test for MUVERA-indexed unflushed rows replaying into L0 then re-materializing FDE on the next flush**

- **File:** `crates/uni/tests/common/multivector_resilience.rs`
- **Location:** whole file (define_multi_schema has no index)
- **Category:** test-gap

**Description:** multivector_resilience.rs exercises crash/recovery only on the NO-INDEX multivector path (define_multi_schema declares Doc.tokens but creates no MUVERA index), and multivector_muvera.rs::muvera_persists_across_reopen only covers clean close+reopen AFTER a flush (FDE column already durable in Lance). Neither tests the path where a MUVERA index exists, rows are committed-but-unflushed (only in WAL), a crash occurs, recovery replays them into L0, and the next flush runs materialize_fde_columns over the recovered rows. Given SP-REL-1 (encoder errors abort flush) and the general 'recovered rows land in L0, unioned + re-scored' contract, this is the scenario most likely to surface a real defect and is currently unverified.

**Evidence:**
```
multivector_resilience.rs:66-77 define_multi_schema creates the label/property but no CREATE VECTOR INDEX; muvera_muvera.rs:374-397 muvera_persists_across_reopen calls db.flush() then drop+reopen (no crash, no unflushed rows at reopen).
```

**Suggested fix:** Add a failpoint test that (1) creates a MUVERA index, (2) commits docs without flushing (durable in WAL only), (3) crashes, (4) reopens so the rows replay into L0, (5) flushes (triggering materialize_fde_columns on recovered rows), and asserts the exact-MaxSim target self-matches first.

<details><summary>Independent verification</summary>

Verified all claims by reading both cited files and grepping the whole test tree.

1) multivector_resilience.rs: define_multi_schema (lines 66-77) declares label Doc + properties title/tokens with NO CREATE VECTOR INDEX. Its four crash tests (multivector_committed_value_survives_crash_recovery, multivector_crash_before_wal_recovers_nothing, multivector_crash_during_flush_loses_no_committed_data, multivector_corrupt_wal_tail_skipped_on_reopen) all use DiskHarness + fail::cfg but exercise only the no-index exact-MaxSim rerank path.

2) multivector_muvera.rs: uses create_muvera_index in 9 tests but contains ZERO fail::cfg / DiskHarness / crash. muvera_persists_across_reopen (lines 373-397) calls db.flush() BEFORE drop(db)+reopen — clean close, FDE column already durable in Lance, no unflushed-at-reopen rows. Confirmed exactly as the evidence claimed.

3) A combined search loop (files with fail::cfg|DiskHarness that also reference muvera|VECTOR INDEX|__fde|materialize_fde) returned EMPTY — no test combines MUVERA index creation with a failpoint crash.

4) The claimed code path is real: Writer::materialize_fde_columns exists at crates/uni-store/src/runtime/writer.rs:3566 and runs at flush (line 4086, after rotate). So the path 'MUVERA index exists → committed-unflushed rows in WAL → crash → recovery replays into L0 → next flush runs materialize_fde_columns over recovered rows' is a genuine, currently-unverified path.

Nuance: muvera_l0_and_flushed_mix (line 332) DOES test MUVERA + unflushed L0 rows being unioned and re-scored, but with no crash; and happy-path tests exercise materialize_fde over freshly-inserted (non-recovered) rows. The precise crash-recovery-then-materialize combination is unverified. The finding's characterization is accurate.

</details>

---

### `sparse/completeness/SP-COMP-1` — LOW
**Proposed inline Cypher MaxSim scalar (max_sim / ScoringMode::Sparse) never exposed as a callable function**

- **File:** `crates/uni-query-functions/src/similar_to.rs`
- **Location:** maxsim @261; eval_vector_similarity @ expr_eval.rs:2040
- **Category:** completeness

**Description:** The #96 proposal §5 (Phase 1) explicitly promised "add a max_sim(query, doc) scalar next to score_vectors in similar_to.rs for WHERE/RETURN use", and §8 promised a ScoringMode::Sparse marker. The pure `maxsim()` kernel exists in similar_to.rs:261 but is only ever called from the internal rerank path (search_procedures.rs MAXSIM_RERANKER) — it is NOT registered as a DataFusion UDF (no `maxsim`/`max_sim` in df_udfs.rs) nor reachable as a Cypher scalar. The dense inline scalar `vector_similarity` (read.rs:2173 -> eval_vector_similarity, expr_eval.rs:2040) flattens its operands via `vector_arg_to_f64` and requires equal-length single vectors, so it cannot compute MaxSim over a List<Vector> multivector. Likewise no `ScoringMode::Sparse` arm exists (grep for ScoringMode returns nothing). Net effect: MaxSim and sparse-dot are usable via the rerank mode / `uni.sparse.query` procedure / `sparse_similar_to` UDF, but the inline WHERE/RETURN `max_sim(...)` predicate the proposal listed is missing. The #95 status doc openly acknowledges the ScoringMode::Sparse delta; the MaxSim inline scalar delta is undocumented.

**Evidence:**
```
df_udfs.rs registers create_n_udf/create_sparse_similar_to_udf but `grep maxsim crates/uni-query-functions/src/df_udfs.rs` => no match; eval_vector_similarity (expr_eval.rs:2040) does `let arr1 = vector_arg_to_f64(v1)?;` then a single dot/cosine over equal-length arrays — no per-token max-over-doc loop.
```

**Suggested fix:** Either register a `max_sim` ScalarUDF (delegating to similar_to::maxsim with a metric option) for inline WHERE/RETURN use, or update the #96 proposal/status to record that inline MaxSim scalar was intentionally dropped in favor of the rerank-mode + procedure surfaces only.

<details><summary>Independent verification</summary>

I re-derived every claim against the code.

1) Inline `max_sim`/`maxsim` Cypher scalar is genuinely absent. `grep "maxsim"|"max_sim" crates/uni-query-functions/src/df_udfs.rs` yields no UDF registration; the only repo-wide non-test occurrence outside similar_to.rs is `search_procedures.rs:142 MAXSIM_RERANKER = "maxsim"` (the internal rerank alias) — NOT a registered ScalarUDF. No `create_maxsim_udf`/`create_max_sim_udf` exists anywhere.

2) `eval_vector_similarity` (expr_eval.rs:2040) cannot compute MaxSim over a multivector: it calls `vector_arg_to_f64(v1)`/`(v2)` which flattens `Value::Vector`/`Value::List` into a single Vec<f64>, then hard-errors on `arr1.len() != arr2.len()` and does a single cosine dot over equal-length arrays — no per-token max-over-doc loop. By contrast the real kernel `similar_to.rs:261 maxsim()` does the nested `for q in query { for d in doc { ... best.max(sim) } }` loop, and is only reached from search_procedures.rs:258/457 (rerank path).

3) The pure `maxsim` kernel exists and is exercised, but only via the rerank/`uni.sparse.query`/`sparse_similar_to` surfaces — `create_sparse_similar_to_udf` IS registered (df_udfs.rs:220), confirming the finding's characterization of which surfaces work.

4) No `ScoringMode::Sparse` arm exists. The finding's evidence ("grep ScoringMode returns nothing") is slightly inaccurate: a `ScoringMode` enum DOES exist in a different crate (uni-query/.../similar_to_expr.rs:236), with variants Vector/AutoEmbed/Fts/Null — but crucially NO Sparse variant. The substantive claim (no Sparse scoring mode) holds.

5) Proposal text verified: multivector_colbert_maxsim.md:225-228 offers `max_sim(query, doc)` "scalar next to score_vectors in similar_to.rs ... for WHERE/RETURN use" — but as an alternative ("call Lance's multivec_distance directly, OR add a max_sim scalar") and explicitly says "Prefer delegating to Lance's kernel." So the inline scalar was a soft, deprioritized option, not a firm Phase-1 commitment; the primary promised mechanism (maxsim rerank branch) IS implemented and tested (multivector_maxsim.rs).

Net: the inline WHERE/RETURN max_sim predicate is real-missing, and ScoringMode::Sparse is real-missing — both confirmed. This is a completeness gap in optional surfaces, not a functional defect: MaxSim and sparse-dot are fully usable via rerank/procedure/UDF paths.

**Severity correction:** The finding's evidence line "grep ScoringMode returns nothing" is imprecise: a ScoringMode enum does exist in crates/uni-query/src/query/df_graph/similar_to_expr.rs:236 (variants Vector/AutoEmbed/Fts/Null), just in a different crate than df_udfs.rs. The correct statement is that no ScoringMode::Sparse variant exists — which is true and supports the finding. Also, the proposal listed the inline max_sim scalar as an optional alternative ("or add a max_sim scalar ... Prefer delegating to Lance's kernel"), not an unconditional promise; the primary Phase-1 mechanism (maxsim rerank mode) shipped and is tested. Low severity is appropriate (borderline info): this is a missing convenience surface, not a missing capability.

</details>

---

### `sparse/completeness/SP-COMP-2` — LOW
**Sparse Arrow read reconstructs Value::SparseVector without re-validating sorted-unique invariant -> latent .expect panic on CV re-encode**

- **File:** `crates/uni-store/src/storage/arrow_convert.rs`
- **Location:** 317-348; re-encode at cypher_value_codec.rs:438
- **Category:** completeness

**Description:** arrow_to_value builds `Value::SparseVector { indices, values }` directly from the on-disk struct arrays (arrow_convert.rs:348 and the result-row path at :440) with no call to SparseVector::new, so the strictly-ascending-unique/finite invariant is not re-checked on read. The CV encoder reconstructs the validated type via `SparseVector::new(indices, values).expect("invalid SparseVector value")` (cypher_value_codec.rs:438). If an on-disk postings/property struct is ever non-canonical (corruption, a future write path that skips validation, or a manually-spliced column), reading it back and routing it through the tagged codec (WAL persistence of a mutation, Map-nested value) would panic the process rather than returning a decode error. Normal writes go through validated constructors so this is not hit on the happy path, hence low severity, but it is a missing guard relative to the proposal's stated invariant discipline (§5.1 'reconstruct SparseVector::new only at boundaries').

**Evidence:**
```
arrow_convert.rs:346-348: `let indices: Vec<u32> = ...; let values: Vec<f32> = ...; return Value::SparseVector { indices, values };` (no SparseVector::new). cypher_value_codec.rs:438: `let sv = uni_sparse_vector::SparseVector::new(indices.clone(), values.clone()).expect("invalid SparseVector value");`
```

**Suggested fix:** Validate on the read boundary (construct via SparseVector::new and log+null or hard-error on failure, mirroring value_codec.rs which already returns errors), and/or replace the `.expect` in cypher_value_codec.rs:438 with a propagated CodecError so malformed input cannot panic the writer/WAL path.

<details><summary>Independent verification</summary>

Verified all three cited code locations. (1) arrow_convert.rs:346-348 (the data_type-tagged path) and :438-440 (the result-row data_type=None path via is_sparse_vector_struct) both construct `Value::SparseVector { indices, values }` directly from the on-disk Arrow StructArray (UInt32 indices list + Float32 values list) with NO call to SparseVector::new and no sorted-unique/finite re-validation. The surrounding code only validates Arrow *shape* (downcast checks), not semantic invariants. (2) cypher_value_codec.rs:436-441 encodes Value::SparseVector by calling `uni_sparse_vector::SparseVector::new(indices.clone(), values.clone()).expect("invalid SparseVector value")` — confirmed verbatim. (3) sparse.rs:30-52: SparseVector::new returns Err on length mismatch, non-strictly-ascending indices (indices[i] <= indices[i-1]), or non-finite weights; combined with the `.expect`, any non-canonical reconstructed value panics the process. Reachability is real: a non-canonical on-disk sparse vector read via arrow_to_value, then routed through encode() (WAL mutation persistence — TAG_SPARSE_VECTOR=20 is a WAL/codec tag — or Map-nested value re-encode), panics rather than returning a decode error. The decode side (cypher_value_codec.rs:243-251) correctly returns a structured error, making the encode-side .expect the asymmetric weak point. Severity 'low' is appropriate: only triggered by corruption / a future unvalidated write path / manual column splice; the happy path goes through validated constructors (SparseVector::new / from_pairs). This is a defense-in-depth gap relative to the proposal's 'reconstruct SparseVector::new only at boundaries' invariant discipline, not a happy-path defect. Finding's file/line citations and evidence quotes are all accurate.

</details>

---

### `sparse/correctness/SP-CORR-3` — LOW
**Non-deterministic top-k membership at score ties in sparse_rerank/multivector_rerank (no vid tie-break)**

- **File:** `crates/uni-query/src/query/df_graph/search_procedures.rs`
- **Location:** 549-550 (sparse_rerank step 6); 461-462 (multivector_rerank step 6)
- **Category:** correctness

**Description:** Both production rerank paths sort candidates by score only — `scored.sort_by(|a,b| b.1.partial_cmp(&a.1).unwrap_or(Equal))` — then truncate to k. Candidates are gathered in nondeterministic order (HashSet dedup + HashMap props iteration), so when scores tie across the k-boundary the *set* of returned vids is nondeterministic. This diverges from the DAAT storage path top_k_from_scores (sparse_index.rs:524-528), which deterministically tie-breaks by ascending vid, and from the metamorphic oracle which tie-breaks by title. The metamorphic test only checks scores rank-by-rank (not membership), so it passes despite the nondeterminism; a deterministic-recall expectation at a tie boundary would not.

**Evidence:**
```
scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)); scored.truncate(k);  // no tie-break on vid
```

**Suggested fix:** Add a deterministic secondary key matching the DAAT path: `.then(a.0.as_u64().cmp(&b.0.as_u64()))` in both rerank sort comparators so ties resolve by ascending vid.

<details><summary>Independent verification</summary>

Verified all four claims against the actual code:

1. Both production rerank paths sort by score only and truncate, with NO vid tie-break:
   - sparse_rerank step 6: search_procedures.rs:549-550 — `scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Equal)); scored.truncate(k);`
   - multivector_rerank step 6: search_procedures.rs:463-464 (finding cited 461; actual is 463 — minor off-by-two, substance correct) — same score-only sort + truncate.

2. The DAAT storage path DOES deterministically tie-break by ascending vid. top_k_from_scores lives in uni-store/src/storage/sparse_index.rs:511-530 (finding cited the file without the crate but the function/line content matches). Its final sort at 524-528 has `.then(a.0.as_u64().cmp(&b.0.as_u64()))`, AND critically the bounded min-heap pruning is also vid-deterministic: HeapEntry::cmp (lines 651-657) tie-breaks `.then(self.vid.cmp(&other.vid))`, so heap eviction at the k-boundary is deterministic by (score, vid). DAAT membership is therefore reproducible.

3. The candidate-gathering order in the rerank paths IS nondeterministic. collect_l0_label_candidates (manager.rs:2515-2550) builds `live` as a HashSet and returns `live.into_iter().collect()` (line 2549) — nondeterministic Vec order; it also iterates `buf.vertex_labels` (a HashMap) at 2538. The deduped `candidates` Vec in sparse_rerank (search_procedures.rs:500-511) inherits this order. Rust's sort_by is stable, so a score tie at the k-boundary preserves whatever (nondeterministic) input order existed → the returned set can vary across runs.

4. The metamorphic oracle tie-breaks by title and the test does NOT check membership. sparse.rs:183-188 sorts oracle by score `.then_with(|| a.0.cmp(b.0))` (title) then truncates. The assertions: (a) each engine-returned title carries the correct score (does NOT assert the engine set equals the oracle set), (b) score vectors agree rank-by-rank only (zip on scores, titles ignored), (c) descending order. The code comment at lines 200-201 explicitly acknowledges ties "permute titles but not scores," so a tie-boundary swap passes. No existing test asserts deterministic membership at a tie — the test-gap claim is verified.

The finding is real and correctly characterized. The divergence between the DAAT path (vid tie-break in both heap and final sort) and the brute-force rerank paths (no tie-break) is genuine.

**Severity correction:** Minor: the multivector_rerank step-6 sort is at search_procedures.rs:463 (not 461 as cited), and the DAAT top_k_from_scores is in crate uni-store (crates/uni-store/src/storage/sparse_index.rs), not the uni-query df_graph sparse_index.rs implied by the HOW-TO. The DAAT determinism is reinforced not only by the final sort tie-break (524-528) but also by the min-heap's HeapEntry::cmp vid tie-break (651-657), which is what actually governs membership at the k-boundary. Severity 'low' is appropriate: scores and score-ranking are always correct; only the *membership* of returned vids at exact score ties is nondeterministic, a reproducibility/parity concern rather than a wrong-results bug.

</details>

---

### `sparse/reliability/SP-REL-3` — LOW
**sparse_search swallows all index-open errors as an empty candidate set**

- **File:** `crates/uni-store/src/storage/manager.rs`
- **Location:** 2058-2065
- **Category:** reliability

**Description:** The non-branched sparse retrieval path does `match self.index_manager().sparse_vector_index(label, property).await { Ok(idx) => idx.query_topk(query, k).await, Err(_) => Ok(Vec::new()) }`. The `Err(_)` arm is intended for 'no sparse index registered' (the only documented error source), but it discards the actual error and fails open with an empty flushed-candidate set for ANY error. Today `sparse_vector_index` only errors on not-registered (SparseVectorIndex::new uses `Dataset::open(..).ok()` and never errors), so this is currently benign, but it is a latent fail-open: if index lookup ever gains a transient/corruption error path, queries would silently return reduced recall (flushed candidates dropped) with no signal. The rerank L0-union still returns L0 rows, masking the degradation further.

**Evidence:**
```
manager.rs:2063-2064: `Ok(idx) => idx.query_topk(query, k).await, Err(_) => Ok(Vec::new()),`
```

**Suggested fix:** Match specifically on the 'no index registered' condition (e.g. return Ok(empty) only when the schema lookup finds no Sparse config) and propagate any other error, or at minimum log a warning so a real failure is observable rather than silently downgrading recall.

<details><summary>Independent verification</summary>

Verified all cited code. manager.rs:2058-2065 matches the evidence exactly: `match self.index_manager().sparse_vector_index(label, property).await { Ok(idx) => idx.query_topk(query, k).await, Err(_) => Ok(Vec::new()) }`. The catch-all `Err(_)` arm discards the actual error and fails open with an empty flushed-candidate set.

index_manager.rs:935-952 (`sparse_vector_index`) has exactly two potential error sources: (a) the not-found `.ok_or_else(|| anyhow!("No sparse vector index found for {}.{}"))?` at line 950, and (b) propagation of `SparseVectorIndex::new`'s Result at line 951. I confirmed sparse_index.rs:223-233 `SparseVectorIndex::new` does `let dataset = (Dataset::open(&path).await).ok();` and unconditionally returns `Ok(Self {...})` — it never errors, even on a failed/corrupt dataset open (the open error is swallowed into `None`). So today the ONLY reachable Err source is the not-registered case, making the `Err(_) => Ok(Vec::new())` benign — exactly as the finding states.

The latent-fail-open claim is correct: the catch-all would also swallow any future hard error (transient/corruption) introduced into the lookup path, silently returning reduced recall.

I also confirmed the masking claim via the caller `sparse_rerank` (search_procedures.rs:479-552): line 491-494 calls `sparse_search`; if it returns empty, the candidate set falls back to only `l0_live` rows (lines 497-511). Flushed/on-disk candidates would be silently dropped while L0 rows still surface, masking the degradation with no error signal.

Severity 'low' is correctly stated — the issue is currently unreachable (no error path can fire) and is a design-robustness/latent concern, not a live bug. The description's own framing ('currently benign... latent fail-open') is accurate and appropriately qualified.

</details>

---

### `sparse/reliability/SP-REL-4` — LOW
**uni.sparse.query user filter argument is silently ignored**

- **File:** `crates/uni-query/src/query/df_graph/search_procedures.rs`
- **Location:** 637 (let _filter = extract_optional_filter(args, 4))
- **Category:** completeness

**Description:** The 5th argument (`filter`) to `uni.sparse.query` is parsed into `_filter` and then discarded — the sparse rerank path applies no user predicate, only MVCC/tombstone visibility from the property fetch. A caller that passes a filter expecting it to scope results gets unfiltered results with no error or warning. This is documented inline as 'accepted for API symmetry', but silently dropping a user-supplied predicate is a correctness/least-surprise gap (a user could believe results are constrained when they are not).

**Evidence:**
```
search_procedures.rs:635-637: `// `filter` is accepted for API symmetry; MVCC/tombstone visibility is already enforced ... let _filter = extract_optional_filter(args, 4);` — `_filter` is never used thereafter.
```

**Suggested fix:** Either apply the filter to the candidate set (parse + evaluate against fetched props) or reject a non-null filter argument with a clear 'filter not yet supported on uni.sparse.query' error so users are not misled into thinking results are scoped.

<details><summary>Independent verification</summary>

Verified directly in crates/uni-query/src/query/df_graph/search_procedures.rs. In run_sparse_query (lines 614-692), line 637 binds `let _filter = extract_optional_filter(args, 4)` with the comment "filter is accepted for API symmetry; MVCC/tombstone visibility is already enforced by the property fetch in sparse_rerank" (lines 635-636). The underscore-prefixed `_filter` is never read again anywhere in the function. The call to sparse_rerank (lines 657-667) passes only storage, property_manager, query_ctx, label, property, query, k, retrieval_k — and sparse_rerank's signature (lines 479-488) has NO filter parameter, so it is structurally incapable of applying a user predicate; its only visibility filtering is MVCC/tombstone via get_batch_vertex_props_for_label (lines 515-518), which is orthogonal to a user-supplied predicate. The asymmetry is real and demonstrable: every sibling search procedure forwards the filter as a functional scoping predicate — uni.vector.query multivector path passes filter.as_deref() to multivector_rerank (line 1195) which threads it into storage.muvera_fde_candidates/multivector_search (lines 390/403); dense path passes filter.as_deref() at line 1269; uni.fts.query at line 1342; hybrid uni.search at lines 1499/1516. Only the sparse path drops it. Test-coverage check: every test call to uni.sparse.query (sparse_index.rs:164/633, ssi_read_path_matrix.rs:349/385, autoembed_parity.rs:623, sparse_resilience.rs:82, sparse_autoembed.rs:140) passes `null` as the 5th argument, so no existing test would catch a caller relying on the filter. A caller who passes a non-null filter gets unfiltered results with no error or warning — exactly as the finding states.

</details>

---

### `sparse/test-gap/SP-TG-5` — LOW
**No concurrency test for concurrent writers mutating an indexed sparse column under flush**

- **File:** `crates/uni/tests/sparse_index.rs`
- **Location:** sparse_snapshot_isolates_reader_from_concurrent_insert 646-690
- **Category:** test-gap

**Description:** The only multi-threaded sparse test isolates a single reader from a single committing writer (snapshot isolation). There is no test of two concurrent writers both inserting/updating the indexed sparse column while a flush re-indexes (the incremental-update / load-modify-write path apply_incremental_updates in sparse_index.rs is exercised only single-threaded via flush). Given the broader codebase's SSI concern, the absence of a concurrent-writer-vs-flush sparse test leaves the index-maintenance race surface unverified for the sparse posting list specifically.

**Evidence:**
```
sparse_index.rs has exactly one `flavor = "multi_thread"` test (the snapshot isolation reader); no test spawns concurrent writers against the sparse-indexed label.
```

**Suggested fix:** Add a multi-thread test with N concurrent writers inserting/updating sparse-indexed docs interleaved with flush(), then assert the post-quiesce query matches the brute-force oracle over the committed set (writers abort+retry on conflict per SSI).

<details><summary>Independent verification</summary>

Verified every claim by reading the code. (1) `rg multi_thread crates/uni/tests/sparse_index.rs` returns exactly one hit: line 646, `sparse_snapshot_isolates_reader_from_concurrent_insert`. Reading lines 646-690 confirms it is a single-reader (pinned tx) vs single-committing-writer snapshot-isolation test — no flush during the concurrency, no two concurrent writers. (2) Listed all 18 tests in sparse_index.rs (lines 219-860): none spawn concurrent writers; `grep spawn` over sparse_index.rs and sparse_scoring.rs returns nothing. (3) Swept all sparse-related Rust files with concurrency primitives: only sparse_index.rs (the one snapshot test) and sparse_resilience.rs carry sparse-specific concurrency. sparse_resilience.rs's spawns are crash-injection (a single task that panics at a failpoint like `flush::after-rotate-before-lance`, then join returns Err) — crash-recovery, not a concurrent-writer-vs-flush race. metamorphic/sparse.rs has zero spawn/multi_thread/join. The two sparse rows in ssi_read_path_matrix.rs (`sparse_query_records_matches`, `sparse_query_disjoint_label_no_false_abort`) exercise the READ-path OCC (a sparse query records its read-set so a concurrent write to a matched vid aborts) — that file has no `flush` and no `spawn` at all, and does not test two writers mutating the indexed column under flush. (4) apply_incremental_updates at sparse_index.rs:597-621 is exactly the load-modify-write described: takes `&mut self`, calls load_postings() → mutates the HashMap (retain removed, push added) → write_postings(). It is only invoked from the flush path, single-threaded. So the concurrent-writers-vs-flush index-maintenance surface for the sparse posting list has no test. Finding is real and correctly characterized.

**Severity correction:** No correction to the finding's substance. One nuance worth recording for severity: `apply_incremental_updates` takes `&mut self`, and flush/index-maintenance is serialized through the single storage writer, so concurrent *re-index* calls on the same index are not structurally reachable — which is why low (not medium) is the right severity. The genuinely-unverified path is the higher-level scenario of two concurrent committing writers to the same indexed sparse column whose deltas are then folded by a flush; that has no dedicated sparse test, matching the finding.

</details>

---

### `sparse/test-gap/SP-TG-6` — LOW
**L0-vs-flush flush_equivalence test asserts title-set equality but not exact scores in that path**

- **File:** `crates/uni/tests/sparse_index.rs`
- **Location:** sparse_flush_equivalence 449-469
- **Category:** test-gap

**Description:** sparse_flush_equivalence compares only the ordered TITLE lists before/after flush (names_before == names_after) and then runs assert_matches_oracle on the post-flush results. It does not assert the pre-flush (L0-only) scores equal the post-flush scores rank-by-rank. Since the comment claims 'scale consistency across the L0 and flushed-index paths', a quantization-induced score drift that preserves title order but perturbs scores in the L0 vs flushed path would not be caught here. (assert_matches_oracle IS applied to the L0-only path in a separate test, so this is a minor redundancy gap, not a hole.)

**Evidence:**
```
`assert_eq!(names_before, names_after, ...)` compares titles only; before-scores are never compared to after-scores.
```

**Suggested fix:** In sparse_flush_equivalence, also assert the per-rank scores of `before` and `after` agree within EPS, not just the titles.

<details><summary>Independent verification</summary>

Read crates/uni/tests/sparse_index.rs:449-469 (sparse_flush_equivalence) and helpers query_results (159-180) and assert_matches_oracle (185-217).

The mechanical claim is exactly correct: query_results returns Vec<(String, f64)> (full title+score tuples). In the test, both `before` (line 457) and `after` (line 459) capture these tuples, but lines 461-462 project to titles only (names_before/names_after), and the only cross-flush assertion (line 463 assert_eq!(names_before, names_after, ...)) compares titles. The scores held in `before` are discarded — there is no rank-by-rank before-vs-after score comparison. So a sub-rank-changing score perturbation in the L0 path vs the flushed path is not caught by this test's own equivalence assertion.

The finding's self-mitigation is also accurate: assert_matches_oracle IS applied to `after` (line 467, post-flush) pinning post-flush scores to the brute-force oracle within EPS=1e-3, and sparse_l0_only_no_flush_matches_oracle (430-447) applies the same oracle check to a pure L0 path. So both paths are independently oracle-pinned in their respective tests; any drift beyond 1e-3 in either path would be caught elsewhere. What's genuinely missing here is (a) a direct before==after score assertion and (b) the before/L0 path being oracle-checked within this same test. That is a minor redundancy/coverage gap, not a correctness hole — matching the claimed 'low' severity. The only thing that could slip entirely uncaught is sub-1e-3 drift that preserves title order, which is below the precision anyone asserts.

</details>

---

### `sparse/concurrency/SP-CONC-3` — INFO
**Phantom: a concurrent insert of a new term-matching doc is not antidependency-tracked by sparse rerank**

- **File:** `crates/uni-query/src/query/df_graph/search_procedures.rs`
- **Location:** sparse_rerank 479-552 (candidate gen 489-511)
- **Category:** safety

**Description:** sparse_rerank records SSI reads only for vids it actually fetches (every candidate goes through is_vertex_deleted -> record_vertex_read, so existing-candidate write-skew IS covered). But candidates are the union of (flushed-index top-k) and (this tx's L0 snapshot). A vertex inserted by a concurrent committed transaction after this read-write tx began is neither in the flushed postings (built earlier) nor in this tx's pinned L0, so it is never a candidate and never enters the read-set. If this tx's commit decision depends on 'no doc matches query Q', a concurrent insert of a matching doc is a classic phantom that escapes the item-level read-set. This is the documented general SSI limitation (phantoms -> FOR UPDATE; see record_vertex_read doc comment in l0_visibility.rs:100-112) and is NOT specific to sparse, so it is info-level. The sparse_snapshot_isolates_reader_from_concurrent_insert test (sparse_index.rs:646) verifies snapshot isolation (reader does not see the insert) but not antidependency tracking of an RW tx.

**Evidence:**
```
search_procedures.rs:502-511 builds candidates only from `flushed` (the index) and `l0_live`; a post-begin concurrent insert is in neither set. l0_visibility.rs:102-104 doc: 'records the queried id whether or not it currently exists' -- only for ids actually queried.
```

**Suggested fix:** No code change required for issue #95; document that sparse retrieval inherits the general SSI predicate/phantom limitation. If predicate protection is desired, the candidate-generation predicate (matching term set) would need a coarse-grained range/predicate lock, which is the deferred FOR UPDATE work.

<details><summary>Independent verification</summary>

Verified every claim against the code.

(1) Candidate generation (search_procedures.rs:489-511): candidates are exactly union(flushed sparse-index postings, this tx's L0-live set) minus tombstones. `flushed` comes from storage.sparse_search (the index built earlier) and `l0_live` from collect_l0_label_candidates.

(2) collect_l0_label_candidates (manager.rs:2515-2550) reads ONLY this tx's pinned buffers: pending_flush_l0s, ctx.l0, and ctx.transaction_l0. It never observes another tx's committed-but-unmerged state. So a vertex inserted+committed by a concurrent tx after this tx began is in neither the (earlier-built) flushed postings nor this tx's pinned L0 → it is never enumerated as a candidate.

(3) Read-set recording is per-candidate only: get_batch_vertex_props_for_label (property_manager.rs:935) calls is_vertex_deleted on each candidate vid, which calls record_vertex_read (l0_visibility.rs:33,106-112) inserting that vid into occ_read_set. record_vertex_read's own doc (l0_visibility.rs:100-105) confirms it is item-level and records 'the queried id' only. A never-enumerated vid is therefore never in the read-set, so a concurrent matching insert is an antidependency phantom that escapes commit-time SSI validation. The finding's nuance that EXISTING candidates' write-skew IS covered is also correct (they pass through record_vertex_read).

(4) The cited test sparse_snapshot_isolates_reader_from_concurrent_insert (sparse_index.rs:646-690) only asserts the reader's pinned snapshot does not surface the post-begin insert (snapshot isolation) and a fresh live view does. It uses read-only query_results_tx, never drives an RW commit decision and never asserts an abort — so it does not cover antidependency tracking. Test-gap claim confirmed.

(5) Correctly characterized as the general documented SSI phantom limitation (phantoms → FOR UPDATE per project memory ssi_occ_concurrency_initiative; record_vertex_read item-level doc), not sparse-specific. Severity info is appropriate.

</details>

---
