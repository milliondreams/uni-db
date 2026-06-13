# Uni 2.0.0 Release Notes

**Release scope:** `v1.1.0` → `2.0.0` · 283 commits · ~245K insertions across 1,261 files
**Dates:** 2026-04-15 → 2026-06-03
**Version path:** `1.1.0` → `1.1.1` → `1.2.0` → `1.3.0` (breaking wheel collapse) → `1.4.0` → `2.0.0`

Uni 2.0 is the largest release in the project's history. It graduates four headline subsystems from prototype to default-on production behavior — **Serializable Snapshot Isolation**, **Graph Forks**, the **Plugin Framework**, and **Locy neural predicates** — alongside a sweeping query-planner performance overhaul, an asynchronous write path, and a refreshed dependency baseline (LanceDB 0.30 / Lance 7 / Arrow 58 / DataFusion 53, plus every workspace dependency brought to latest).

---

## ⚠️ Breaking & Behavioral Changes (read first)

Two changes can affect existing deployments. Neither is an API-signature break — which is exactly why they need attention.

### 1. Concurrency model: Last-Writer-Wins → Serializable Snapshot Isolation (default-on)

This is a **silent behavioral change**: your code compiles and runs unchanged, but concurrent writers now behave differently.

- **Before (1.x):** Two transactions writing the same vertex/edge concurrently → one write silently overwrote the other (lost update). Concurrent `MERGE` on the same unique key could create duplicates.
- **Now (2.0):** Conflicting concurrent commits **abort with a retriable error** (`SerializationConflict` / `ConstraintConflict`) instead of silently losing data.

**Migration:**
- For correctness, **no action is required** — you are now safe from lost updates by default.
- Workloads with concurrent writers should wrap transactions in retry logic (see [Concurrency](#serializable-snapshot-isolation--occ-default-on) below for `transact_with_retry` / `execute_with_retry`).
- To restore exact 1.x semantics, set `UniConfig.ssi_enabled = false` (opt-in, not recommended; logs a warning when `FOR UPDATE` is used).
- The compile-time `ssi` and `l0-snapshot` cargo features have been **removed** — SSI is always compiled and toggled at runtime.

### 2. Python wheel matrix collapsed: 12 → 6 wheels (v1.3.0)

Three GPU/provider axes were consolidated. Several wheel package names no longer exist.

| Removed | Replacement |
|---|---|
| `uni-db-fastembed*` (×3) | `uni-db` (ONNX provider; FastEmbed alias strings unchanged) |
| `uni-db-mistralrs*` (×3) | folded into `uni-db` / `uni-db-cuda` |
| `uni-db-all*` (×3) | `uni-db` (CPU, all 11 providers) / `uni-db-cuda` (GPU) |

**Surviving wheels:** `uni-db`, `uni-db-onnx`, `uni-db-cuda`, `uni-db-metal`, `uni-db-onnx-cuda`, `uni-db-onnx-metal`.

**Provider migrations:**
- `local/fastembed` → `local/onnx`; `local/onnx-reranker` → `local/onnx` (reranking unified under the ONNX provider).
- Embed cache path moved: `.uni_cache/fastembed/<alias>/` → `.uni_cache/onnx-embed/<sanitized-repo>/`.
- Seven retired GPU execution providers (`gpu-tensorrt`, `gpu-rocm`, `gpu-coreml`, `gpu-directml`, `gpu-openvino`, `gpu-qnn`, `gpu-wgpu`) remain reachable via `--features provider-onnx-dynamic` + `ORT_DYLIB_PATH`.

**Picking a wheel** comes down to two questions — the Python API is identical
across all six:

1. *Do you need local LLM inference (candle/mistralrs)?* If yes, pick a
   default wheel (`uni-db`, `-cuda`, `-metal`); if no, pick a slim `-onnx`
   variant and skip hundreds of MB of binary. All six include the 8 remote
   API providers.
2. *Which accelerator?* CPU (no suffix), NVIDIA (`-cuda`), or Apple GPU/ANE
   (`-metal`). On Metal wheels, ONNX embeddings now actually reach the GPU
   via the CoreML execution provider (previously silently CPU).

FastEmbed users: change `provider_id` from `"local/fastembed"` to
`"local/onnx"` — all 25 model alias strings are unchanged, with verified
embedding parity. Old `.uni_cache/fastembed/` caches can be deleted; the
first call re-downloads the model.

---

## 🌟 Headline Features

### Serializable Snapshot Isolation & OCC (default-on)

Uni now provides **serializable** transaction isolation via Snapshot Isolation plus Optimistic Concurrency Control, replacing Last-Writer-Wins as the default.

- **Conflict detection** is both write-write (two transactions touching the same item) and read-write/SSI (a committed write touches an item the aborting transaction read). Read-set tracking records vertices, edges, and neighbor traversals — including post-filter scan reads.
- **Retry helpers:**
  - `UniError::is_retriable()` classifies transient contention vs. permanent failure.
  - `Session::transact_with_retry(closure, RetryOptions)` and `execute_with_retry(query, …)` automatically re-run a fresh transaction on retriable errors with jittered exponential backoff (default 5 attempts, 200µs base, 50ms cap).
- **`FOR UPDATE` (pessimistic escape hatch):** Acquiring `FOR UPDATE` on a fresh transaction **re-pins the snapshot** to the latest committed state, enabling zero-retry read-modify-write:
  ```cypher
  MATCH (n:Counter {id: 1}) FOR UPDATE
  SET n.count = n.count + 1
  ```
- **CRDT carve-out:** Concurrent increments to CRDT-declared properties (`COUNT`, `SUM`, …) **merge** at commit time instead of aborting — provided the committed value is the same CRDT variant (a variant mismatch aborts to prevent silent loss).
- **Config:** `UniConfig.ssi_enabled` (default `true`).

**Known limitations:** phantom reads from bare label scans are not tracked (guard scan-based RMW with `FOR UPDATE`); `FOR UPDATE` re-pinning only applies to fresh transactions; read-only `session.query()` paths observe their begin snapshot.

### Graph Forks

**Named, durable, isolated, writable graph branches** — derived from a primary database or a parent fork via Lance copy-on-write, for safe experimentation, staging, audit, and what-if workflows without touching production data.

- **Lifecycle:** `session.fork(name)` (open-or-create; `.new_()` to require fresh; `.ttl(duration)` for wall-clock expiration). Admin via `list_forks()`, `fork_info()`, `drop_fork()`, `drop_fork_cascade()`.
- **Nested forks:** children inherit parent state and read through the parent→child branch chain.
- **Governance:** TTL with a background sweeper, a `max_forks` budget (`ForkBudgetExceeded`), tagging for GC-exempt regulatory holds (`tag_fork` / `untag_fork` / `list_fork_tags`), and parent→child cancellation cascades.
- **Fork-local schema:** `forked_session.fork_schema().label(…).apply()` grows schema in the fork only, persisted in an overlay file; the primary is unaffected.
- **Structural diff & promotion:**
  - `diff_fork_primary(name)` / `diff_forks(a, b)` → `ForkDiff` of added/deleted/changed vertices and edges, paired by **content-addressed UID** (SHA3-256 of label+properties) so identity is correct even across VID collisions.
  - `promote_from_fork(name, &[PromotePattern])` bulk-promotes matched rows back to primary in one atomic transaction, with a `PromoteReport` of inserted/skipped counts. Patterns support `PromotePattern::label(...)` / `::edge_type(...)` with an optional `.where_clause(...)`.
- **Fork-local indexes:** vector (IVF-Flat) and full-text (BM25 RRF) indexes built over fork branches, with lossless (BTree union, k-way merge) and lossy (ANN/BM25 RRF) fusion against the parent — visible in `explain()` as `FusedIndexScan`.
- **Python:** full sync + async bindings for the entire fork surface.

**Known limitations:** the shared UID→VID index dataset is not branch-isolated (lookups verify against the primary session); adding a *new property column* to an existing primary label is unsupported on active forks (drop-and-recreate the fork); parallel edges of the same type between the same endpoints currently share a UID.

### Plugin Framework

A single capability-gated **`PluginRegistry`** replaces five ad-hoc registries. Every built-in (vector indexes, Lance storage, Locy aggregates, APOC procedures) is itself a plugin.

- **Five loaders:** native **Rust** (all 23 extension surfaces, zero-cost), **WASM Component Model** (wasmtime), **Extism**, **Rhai** (sandboxed scripting), and **PyO3** (in-process Python, vectorized eval). Sandboxed loaders currently expose scalar / aggregate / procedure surfaces.
- **Capability & security model:** a `CapabilitySet` gates both extension surfaces and host imports (network, filesystem, host-query, KMS, secrets, locks, config, plugin-storage), each with attenuation (glob patterns, scopes, key/secret allowlists) and resource quotas (memory, fuel, wall-clock, concurrency, result rows). Effective capabilities = `declared ∩ granted`, enforced at three layers: registrar gate, WASM linker (ungranted host fns are structurally omitted), and runtime argument checks.
- **Trust & manifests:** `PluginManifest` carries id/version/ABI/capabilities/determinism/side-effects plus a Blake3 hash pin (always verified) and an optional Ed25519 signature verified against a `TrustRoot`. Host trust policy (`Disabled` / `WarnIfUnsigned` / `RequireSigned`) is set via `.plugin_trust(...)`.
- **APIs:** Rust `add_plugin` / `load_wasm_component` / `load_wasm_extism` / `load_rhai_plugin` / `load_python_plugin`; Python `db.load_wasm_component(...)` / `load_wasm_extism(...)` / `load_rhai_plugin(...)` / `load_python_plugin(...)` plus `@db.scalar_fn` / `@db.aggregate_fn` / `@db.procedure` decorators, all with sync + async parity. Each load returns an outcome dict (effective/denied capabilities, registered surfaces).
- **Worked example:** `geo.haversine` ships for all five loaders in `examples/`, with cross-loader byte/ULP parity tests.

**Security fixes during the cycle:** closed a WASM secret-handle bypass in `decode_batch` and a Rhai filesystem path-traversal escape.

### Locy Neural Predicates & Probabilistic Reasoning

Locy gains **DeepProbLog-class neuro-probabilistic reasoning** operating natively on the property graph.

- **Neural classifiers:** declare learned classifiers with `CREATE MODEL … INPUT … FEATURES … OUTPUT PROB|SCORE|LABEL|VECTOR … USING xervo('provider') CALIBRATION … VERSION …`, then invoke them in rule bodies (`WHERE fraud_detector(inv) > 0.7`, `YIELD … AS risk PROB`). **Python callables** can be registered directly as classifiers via `config.register_classifier(alias, fn)` / `classifier_registry={…}`.
- **FEATURE expressions:** node properties, embeddings, hybrid retrieval (`semantic_match(prop, 'query')`, `similar_to(...)` with auto-embed), graph topology (`degree_centrality`, `pagerank_score`, `betweenness_centrality`, and more), neighbor aggregation (`avg_neighbor`/`max_neighbor`/`sum_neighbor` with direction), and path-context values carried from prior rule derivations.
- **Calibration & validation:** `CALIBRATE model ON … METHOD {platt_scaling | isotonic_regression | temperature_scaling | beta_calibration | dirichlet} HOLDOUT 0.2` (reports ECE + Brier); `VALIDATE rule ON … METRICS brier_score, ece, auc` for end-to-end symbolic+neural evaluation. Confidence bands surface from conformal / ensemble-variance / credal sources, and drift detection emits `CalibrationDrift` warnings.
- **EXPLAIN & provenance:** `NeuralProvenance` records raw vs. calibrated probability and confidence band per invocation; **EXPLAIN Mode A** decomposes a fact's probability into symbolic (rule/MNOR/MPROD) and neural (model/version/calibration) contributions.
- **Correctness detectors:** compile-time and runtime warnings for uncalibrated predicates feeding semiring operators, shared neural inputs/features, shared retrieval context, cross-predicate correlation, and shared-proof dependencies in MNOR/MPROD. `TopKProofs` now computes MNOR exactly over shared base facts via DNF inclusion-exclusion.
- **Flagship notebooks** (Python + Rust companions): Adverse Drug Reaction (ADR), Drug-Drug Interaction (DDI), and Predictive Maintenance, on real datasets with offline-trained models.

---

## 🚀 Performance

This release dramatically accelerates point lookups, joins, and bulk write/merge patterns. Reported speedups are from the issue reproducers.

**Point lookups & `id()` predicates (#47/#48):** `WHERE id(n) = $x` rewritten from O(N) scans to O(1) (L0 HashMap) / O(log N) (Lance `_vid` BTree). Scaling regression went from 8× growth to flat.

**Joins (#53–#55):**
- `Filter(CrossJoin)` → `HashJoinExec` for equi-joins — `UNWIND $edges + MATCH WHERE id(a)=e.src` from 138s (timeout) to **1.18s (117×)**.
- Extended to string keys, `OPTIONAL MATCH` (LeftOuter), and IN-list pushdown for static UNWIND.
- IN-list pushdown for UNWIND-of-maps batch edges; new `VidLookupJoinExec` for cross-MATCH dynamic VID pushdown with multi-equi-pair, LEFT OUTER, and runtime chunking.

**Property point lookups (#57):** equality/IN predicates against scalar **hash indexes** push down as Lance filters — `GraphScanExec` rows drop from full-label to index-bounded.

**Bulk MERGE (#69):** single-node `MERGE` in `UNWIND` skips per-row DataFusion planning (one L0 snapshot per statement, intra-batch dedup) — 2,000 upserts from 17.8s to **1.1s (16.7×)**.

**Write path:**
- Batch prefetch for `SET`/`REMOVE`/non-detach `DELETE` — 1,000-row UPDATE from 450ms to **76ms (5.9×)**.
- Per-transaction VID/EID reservoir cuts global allocator lock acquisitions ~14×.
- Fork vertex/edge promotion batches VID resolution — 1k-row promote from 9.2s to **220ms (42×)**.
- Per-query DataFusion/Executor setup amortized — single-row `CREATE` from ~425µs to ~30µs (**14×**).

**Compaction:** tunable `frozen_segments_compact_threshold` (default lowered 4 → 2) to bound read latency under bursty ingest.

**Allocator:** optional `mimalloc` global allocator (enabled in the Python cdylib and CLI) reduces contention on allocation-heavy concurrent workloads.

---

## 💾 Storage Engine & Write Path

- **Asynchronous L0→L1 flush (default-on):** `async_flush_enabled = true`. A `FlushCoordinator` decouples stream I/O from the commit critical path — concurrent commits rotate their own L0 buffer and return immediately while finalize runs on a background task. Rotate-order is preserved via a min-heap; manifest parent-chains are fixed up on interleave. Shutdown drains all in-flight tasks (critical for fork cleanup); `drop_fork` enforces a drain deadline (`PendingFlushTimeout`). Disable for testing with `UNI_ASYNC_FLUSH=0`.
- **Partial-column Lance writes (opt-in, `partial_lance_writes = true`):** property updates flush only touched columns via `MergeInsert` instead of rewriting wide rows. Covers tombstones (always, via MergeInsert), edge SET deltas, and generated columns (DERIVE/ASSUME/ABDUCE), guided by a per-row touched-keys hint. Schemas with overflow/non-schema properties degrade gracefully to full-row append.
- **Flush-latency & race fixes:** eliminated a 600ms NotFound retry backoff on first flush (#43); fixed a compaction/flush race (#46); serialized per-table writes and removed a stale table cache; retry on Lance concurrent-write conflicts.

**Tunable knobs:** `async_flush_enabled`, `max_pending_flushes` (default 2), `partial_lance_writes`, `CompactionConfig::frozen_segments_compact_threshold`, plus the `mimalloc` build feature.

---

## 🔤 Cypher & Schema

### Cypher language
- **Label & edge-type disjunction:** `(n:A|B)`, `(n:A|:B)`, `[r:A|B]`, plus automatic narrow-scan rewrites of `WHERE n:A OR n:B` / `type(r)='A' OR …` into label-scoped scan unions (~3.9× faster than full-table filter). Internally, labels/types now use a `LabelExpr` enum (`Empty | Conjunction | Disjunction`).
- **System timestamp functions:** `created_at(n)` and `updated_at(n)` return per-row UTC nanosecond timestamps.
- **`CALL { … }` subquery fixes:** unit subqueries (no inner `RETURN`) now pass input rows through unchanged with writes as side effects (per spec); row bindings refresh across the SET→RETURN boundary so post-SET values are visible downstream; write-bearing subqueries correctly disable per-row IN-list coalescing.
- **`UNION` schema guard relaxed** to type-only (allows differently-named columns across branches).
- **`profile()` for writes:** now available on transaction `execute` builders, returning `(ExecuteResult, ProfileOutput)`.

### Schema
- **`DataType::Bytes`** for raw binary properties (#50) — Cypher `BYTES`/`BLOB`/`BINARY`, Python `DataType.BYTES()`, mapped to Arrow `LargeBinary`.
- **`strict_schema` config flag:** rejects CREATE/MERGE against undeclared labels/edge types with actionable errors.
- **Optional `description`** on labels, edge types, and properties (Cypher `DESCRIPTION`, builder `*_with_desc()`/`set_*_description()`, JSON schema, procedures).
- **Reserved property-name validation (#67):** schema apply rejects names colliding with internal/system columns at apply time instead of crashing at flush.
- **Type-mismatch write rejection/coercion (#68):** mismatched property writes now error at the call site rather than silently nulling; safe coercions allowed (Int→Float/Int32, String→temporal via constructors, Null→nullable).
- **Idempotent `apply()`** caps super-linear index bloat on repeated schema application.
- **`document_prefix` / `query_prefix` on `EmbeddingCfg` (#42)** for asymmetric retrieval models (E5, Nomic, BGE, ModernBERT).

---

## 🔎 Reranking & Embeddings (uni-xervo)

- **Cross-encoder reranking** for `uni.vector.query`, `uni.fts.query`, and `uni.search`: re-scores an over-fetched candidate set with a (query, document) cross-encoder. Options: `reranker`, `reranker_property`, `reranker_k`, `reranker_query`; new `rerank_score` YIELD column.
- ONNX cross-encoder provider (`local/onnx-reranker`, now unified under `local/onnx`) with WordPiece tokenization (e.g. `ms-marco-MiniLM-L6-v2`); BGE & Qwen3 rerankers covered.
- **`prefetch(aliases)` / `prefetch_all()`** (#52) to warm model caches and avoid cold-start latency; exposed on the Rust facade and in Python.
- `ort` switched to `load-dynamic`; uni-xervo bumped 0.5.0 → **0.13.0** across the cycle (breaking changes consolidated at 0.9.0).

---

## 🐍 Python API

- **Plugin loading** with full sync/async parity: `Uni`/`AsyncUni`.`load_wasm_component` / `load_wasm_extism` / `load_rhai_plugin` / `load_python_plugin`, plus `@scalar_fn` / `@aggregate_fn` / `@procedure` decorators. Type stubs added for all variants; the default wheel now bundles wasmtime.
- **Locy:** register Python callables as neural classifiers; expose `Calibrate`/`Validate` command results; `allow_partial` config plus `LocyResult.timed_out` / `.incomplete` diagnostics and a catchable `UniLocyIncompleteError`; new warning codes (`FuzzyNotProbabilistic`, `TopKPruningCrossedDependency`).
- **Forks:** full sync + async bindings for create/diff/promote/tag/lifecycle.
- **`profile()`** for transaction mutations on sync and async execute builders.
- **Performance:** mimalloc as the Rust-side global allocator in the PyO3 cdylib; opt-in embedding deferral (`defer_embeddings`).

---

## 📦 Packaging & Distribution

- **Wheel matrix 12 → 6** (see [Breaking Changes](#2-python-wheel-matrix-collapsed-12--6-wheels-v130)).
- **Host probe CLI:** `python -m uni_db check` verifies the installed wheel's host runtime deps (extension load, CUDA driver + cuDNN for `*-cuda`, Metal for `*-metal`); `python -m uni_db recommend` suggests the best wheel for the host. Both are pure-Python and CI-gating friendly; `uni_db.VARIANT` identifies the installed wheel.
- **Dynamic ORT linking** via `--no-default-features --features provider-onnx-dynamic` + `ORT_DYLIB_PATH` as an escape hatch for retired GPU execution providers.

---

## ⬆️ Dependencies

- **LanceDB 0.30 / Lance 7 / Arrow 58 / DataFusion 53 / object_store 0.13** — required an `ObjectStoreExt` trait split, DataFusion 53 plan/statistics API migration, and a `null_aware = false` pin for the Locy anti-join dedup.
- **All workspace dependencies brought to latest** at 2.0.0 (`d90b1fa05`), including major bumps across the tree.
- Workspace `rand` aligned to 0.9 to match Lance.

---

## 🐛 Notable Bug Fixes

- Recursive-delta dedup within a candidate batch above the anti-join threshold (Locy IS NOT correctness).
- Preserve scan-side filter merging across HashJoin bailouts; multi-VID IN-list pushdown on the L0 scan path.
- `LargeBinary` node encoding in the per-row cross-join path; `_type_name` accepted on edge maps in SET.
- `MutationContext` propagated into `CALL` subqueries and `RecursiveCTE` sub-plans; `BaselineMetrics` wired across custom execution plans.
- Label-disjunction panic on heterogeneous property schemas (three-layer fix); `UNION` schema guard relaxed to type-only.
- Reranker CI workflow (onnx-dynamic flags + xet-bridge gating); hardened fork-lifecycle tests against CI flakes.
- Reverted an MVP async-flush spawn that measured 3–40× **slower**, replaced by the coordinator design.

---

## 📚 Docs, Notebooks & Examples

- Website documentation fully revised for 2.0 (isolation model, Locy neural-predicate pages, configuration regen, plugin framework).
- New flagship Locy neural notebooks (ADR, DDI, Predictive Maintenance) with Rust companions, on real datasets with offline-trained models.
- Fork notebooks (Rust + Python, sync/async); five-loader plugin examples (`examples/example-{wasm,extism,rhai,pyo3}-geo/`).

---

## Upgrade Checklist

1. **Audit concurrent writers** — add `transact_with_retry` / `execute_with_retry`, or set `ssi_enabled = false` to defer the change (not recommended).
2. **Update wheel/package references** — migrate off removed `*-fastembed` / `*-mistralrs` / `*-all` wheels; update provider aliases and run `python -m uni_db check`.
3. **Re-point embed caches** if you depended on the old `.uni_cache/fastembed/...` path.
4. **Review write-heavy schemas** — consider enabling `partial_lance_writes` and the `mimalloc` feature; tune `frozen_segments_compact_threshold`.
5. **Rebuild against the new dependency baseline** (LanceDB 0.30 / Arrow 58 / DataFusion 53).
