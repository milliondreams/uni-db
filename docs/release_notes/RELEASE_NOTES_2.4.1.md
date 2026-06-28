# uni-db 2.4.1

**Release focus: learned-sparse (SPLADE) vector search**, a vector-index test-parity and
hardening pass across all three modalities (dense / multi-vector / sparse), and a storage-path
correctness fix.

This is a minor release covering everything since **2.3.0**. 31 commits; version bumped across the
Rust workspace and the Python packages (`uni-db`, `uni-pydantic`) to **2.4.1**. The feature work is
driven by a new public API (`StorageBackend::lock_table_for_write` + `TableWriteGuard`) and
API-boundary behavior changes (malformed sparse values now return a clean error / are canonicalized
instead of panicking; `uni.sparse.query` rejects an unsupported filter argument).

> **Note on 2.4.0.** The `v2.4.0` tag was cut but its release run failed immediately at the
> version-validation gate — `bindings/uni-pydantic/pyproject.toml` had not been bumped in lock-step
> with the Rust workspace, so no artifacts were ever published (no GitHub assets, no PyPI wheels).
> **2.4.1 is the first published release of this line** and ships the identical feature set. The
> version-sync process that prevents this is now documented in
> [`docs/releasing_version_bump.md`](../releasing_version_bump.md).

---

## Highlights

### 🔹 Learned-sparse (SPLADE) vector search — #95

uni-db now has a first-class **sparse / learned-sparse** vector type and a scored inverted-index
retrieval path, completing the third vector modality alongside dense and multi-vector (ColBERT).

- **Type & crate.** A new `uni-sparse-vector` leaf crate (BTIC-style) provides the `SparseVector`
  type with a validating constructor, a lossless binary codec, and pure `sparse_dot` / `l2_norm` /
  `prune_top_k` kernels (unit + property tested). `Value::SparseVector` / `DataType::SparseVector`
  lower to an Arrow `Struct{indices: List<UInt32>, values: List<Float32>}` with `TAG_SPARSE_VECTOR`
  CV-tag framing.
- **Scored index.** A `SparseVectorIndex` (fork of the inverted index) stores per-term postings with
  a `max_impact` upper bound and serves a `query_topk` dot-accumulator + min-heap. Built via
  segment-merge (backend-scan backfill + L0-incremental flush), MVCC/tombstone-correct: `sparse_rerank`
  unions L0, gates on version / `_deleted`, and exact-dot rescores.
- **Surfaces.** `uni.sparse.query('Label', 'prop', query, k)` procedure, `SPARSE_VECTOR(N)` Cypher
  type DDL, and `CREATE VECTOR INDEX ... OPTIONS{type:'sparse'}` index DDL (now correctly routed to
  the sparse path and backfilled after a flush — it previously built a dense IVF_PQ index silently).
- **8-bit weight quantization (M3).** Per-term unsigned `UInt8` codes + a `Float32` weight scale;
  `max_impact` is computed from the *dequantized* weights so it stays a valid rank-safety upper
  bound. A single reader detects the encoding by element type, so legacy `f32` segments and
  `quantize=false` share one lossless path — no version marker, no rebuild.
- **Scoring / fusion / hybrid (M4).** N-ary RRF (`fuse_rrf_multi`) and source-aware weighted fusion
  (`fuse_weighted_sources` with `DistanceToSim` / `ScoreByMax` normalization); a scalar
  `sparse_similar_to(a, b)`; and a **3-way hybrid search** (dense + full-text + sparse) emitting a
  `sparse_score` column. Two-way fusion is byte-identical (an empty source is a no-op).
- **Fork-aware.** `uni.sparse.query` now works inside a fork (Approach A: brute-force branch scan
  over the inherited + fork rows, with the already-fork-correct re-rank unioning fork L0).
  Previously sparse search returned nothing on a fork.
- **Text auto-embed.** Sparse columns auto-embed at write time, query time, and in hybrid search
  (xervo 0.17 sparse head); columns sharing an alias + source with a dense/multi-vector column are
  filled in a single hybrid forward pass. A cross-modality fix ensures a `SET` on an embed *source*
  column **re-embeds** the target (all modalities previously left a stale vector on `SET`).
- **Python / OGM.** `PySparseVector`, `DataType.sparse_vector(N)`, the `SparseVector[N]` OGM type
  with a `sparse_search()` builder and schema auto-indexing, and a fix for returned sparse
  properties being silently dropped (`value_to_py` had no `SparseVector` arm).
- **Performance.** End-to-end `uni.sparse.query` (k=10, median): ~10 ms at 10k docs (int8 ~ f32),
  recall@10 = 1.000. Sub-15 ms at 10k confirms the P1 index needs no P2 block-max pruning at this
  scale, which is documented as deferred-by-design.

### 🔹 Vector-index hardening & test parity — #95/#96 adversarial review, #121, #122

An adversarial multi-agent review of the sparse (#95) and multi-vector/MUVERA (#96) features
surfaced 32 verified findings; the critical and high-severity ones are fixed here, each with a
regression test (report: `docs/sparse_multivector_review_2026-06-27.md`). In parallel, dense and
multi-vector were brought up to the sparse test suite's "gold standard", which uncovered two **real**
correctness bugs.

- **RC1 (critical data loss)** — an unserialized Lance `Overwrite` vs flush race. Fixed with a new
  `StorageBackend::lock_table_for_write` → `TableWriteGuard` (owned per-table mutex guard); MUVERA
  FDE backfill and sparse/inverted posting backfill now hold the lock across scan → splice →
  atomic-replace, closing the read-then-overwrite TOCTOU window where a concurrent flush append was
  silently dropped.
- **RC2–RC6** — malformed `SparseVector` values are canonicalized / rejected with a clean error
  instead of panicking (ingest, WAL, and Arrow paths); FDE param integer-overflow guards
  (`checked_mul`/`checked_shl` + bounds); a wrong-dim multi-vector token now skips the row instead of
  wedging every flush; stale postings are purged on update-reflush; and `table_exists()` checks
  fail-closed (`?`) instead of fail-open across the sparse/multivector/muvera search and backfill
  paths.
- **Dense L0-union bug (real).** `uni.vector.query` silently ignored committed-but-unflushed L0 data
  — dense search returned **stale** results until a flush. Root cause: the L0 candidate scorer used
  `Value::as_array()`, which matches only `Value::List` and dropped the typed `Value::Vector` the
  Cypher write path actually stores. Now extracted via the canonical `TryFrom<&Value> for Vec<f32>`.
- **RETURN-projection type fidelity.** A top-level `FixedSizeList<Float32>` now decodes to
  `Value::Vector` (not a generic `Value::List`), restoring type identity on `RETURN d.vec_col` for
  dense and multi-vector (parity with `SparseVector`); `VECTOR_SIMILARITY`/`VECTOR_DISTANCE`/`=~` were
  routed through a helper accepting both variants. Also fixes an L0 multi-vector projection
  hard-error.
- **Prebuilt-runtime reopen.** Opening a DB whose schema has embedding-configured indexes no longer
  errors "catalog is required" when a prebuilt runtime (`.xervo_runtime(...)`) — which carries its
  own catalog — is attached.
- **Parity suites.** Dense and multi-vector now match sparse on every axis: brute-force exact oracles
  (cosine-KNN, MaxSim), crash/WAL failpoint resilience, MVCC snapshot isolation, and metamorphic
  oracles (PR smoke + nightly soak). The `#121` partial-Lance-write report was investigated and found
  to be a stale *test* assertion, not a production bug (regressions added). Residual auto-embed cells
  are tracked in `#122`.

### 🔹 Storage-path correctness — #115, #116, #117

`VertexDataset::new` built `{base}/vertices_<label>` while the LanceDB backend stores
`{base}/vertices_<label>.lance`, so raw-path reads of flushed vertex tables silently returned
`Err`/0 rows. Two confirmed consequences: composite-key uniqueness failed **open** across the flush
boundary (#116), and scalar/inverted/FTS/vector index builds were silently skipped after a flush
(#115) — masked by full-scan / brute-force fallbacks, which is why result-asserting tests never
caught it (#117).

- The canonical `.lance` path is now built from a single source of truth.
- All index creation routes through `StorageBackend` and the `VertexDataset` raw-open escape hatch
  (`open`/`open_at`/`open_raw`/`new_branched`) is **deleted** — there is now one on-disk path
  reconstruction, so the two paths can no longer drift. This eliminates the bug *class* and unifies
  the primary index path with the fork path. The trait gained
  `create_vector_index`/`create_scalar_index`/`create_fts_index`/`drop_index` with full param
  fidelity against lancedb 0.30.
- Mechanism tests (via `list_indexes`) now assert the index was *physically built*, not just that
  query results are correct — proven to catch the bug on revert.

### 🔹 Durability — crash-during-flush

A panic at the flush seam followed by a graceful close (Drop → shutdown flush) silently dropped an
acknowledged, committed-but-unflushed transaction. The next flush truncated the pending buffer's own
WAL segment and advanced the checkpoint past it — both keyed off the buffer's *high* watermark
instead of its *start* watermark. Fixed by tracking `wal_lsn_at_start` per L0 buffer and capping both
WAL truncation and the published high-water-mark at the floor over all *other* pending buffers.
(Under a real, Drop-less crash the WAL stayed durable, so only the graceful-close path lost data.)

### 🔹 Locy (Datalog) typed-value boundary — #111, #112, #113

Three distinct root causes, unified by typed values losing their logical type when crossing the
Locy ↔ DataFusion boundary:

- **#112** — a property-expression `KEY` (`i.tag AS tag`) projected `NULL` without a `FOLD`; the
  QUERY/SLG path now evaluates `KEY` expressions whenever one needs evaluation.
- **#111** — duration arithmetic (`duration.inDays(...)` + math) crashed at plan time
  ("Unsupported CAST from LargeBinary to Float64") and poisoned all rules; math now routes through
  `cypher_to_float64_expr` so non-numeric values yield a clean `NULL`. Numeric extraction via
  `.days`/`.months`/`.seconds`.
- **#113** — the dot-namespaced `btic.contains(...)` spelling is now accepted as an alias for
  `btic_<fn>`, mirroring `duration.<fn>`.
- **Latent** — property columns are now typed from the schema (`property_arrow_type`) instead of
  being blindly typed `Float64`, fixing "Casting Struct to Float64" for DateTime/Duration/Btic
  columns.

### 🔹 Fork reverse-adjacency — #110

On a fork under a typed edge schema, **any** relationship `SET` silently broke recursive
reverse-direction traversal (`(a)<-[l:LINE]-(b)`) — it returned only the seed and dropped every
inherited reverse edge (forward traversal and all non-fork cases were correct). The CSR-warming gate
keyed off the dual-write *overlay* (`is_active_for`) rather than the CSR itself: a single `SET`
dual-writes one edge into the overlay, making the gate report "active" and skipping the warm, so the
inherited reverse edges were never loaded. Fixed by gating on `has_csr` alone, per direction — one
shared gate fixes every direction, edge type, and traversal surface (Cypher and Locy alike).

### 🔹 Nested-fork read regression — #103

A fork-of-a-fork over schemaless data errored on read (`Object not found`): Lance does not resolve a
scalar (BTree) index's `page_lookup.lance` across more than one branch level. Fixed earlier by
disabling scalar-index pushdown on branch scans; this release adds the faithful regression test
closing the issue.

---

## Reliability & testing infrastructure

- **Per-modality oracles.** Brute-force ground-truth oracles for dense (cosine-KNN), multi-vector
  (MaxSim), and sparse (`sparse_dot`), with recall@k benchmark harnesses for the index paths.
- **Failpoint resilience.** `--features failpoints` crash/recovery tests for all three modalities
  (committed-value-survives-crash, crash-before-WAL, crash-during-flush lost-commit regression,
  corrupt-WAL-tail skip) — confirming post-recovery queryability via the L0-union path with **no
  index rebuild**.
- **MVCC snapshot isolation.** Reader-pinned-snapshot parity tests across dense / multi / sparse.
- **Metamorphic oracles.** First vector-modality metamorphic cases (PR smoke + nightly soak).
- A new **vector-index test-parity contract** (`docs/vector_index_test_parity.md`) requires new
  modalities/capabilities to ship with an oracle-backed test.
- Disproved (with tests) the assumption that secondary indexes need a rebuild after WAL recovery —
  the L0-union read path and full-L0-scan reflush already cover it; a bogus defensive `rebuild()` was
  removed.

## Dependencies

- New internal `uni-sparse-vector` leaf crate (`uni-common` depends on it). No external dependency
  bumps — `uni-xervo` stays at **0.17** (its sparse embedding head powers sparse auto-embed).

## Documentation & CI

- Documented multi-vector / ColBERT search, MUVERA, parameterized DDL container types
  (`LIST<VECTOR(N)>`, `MAP<K,V>`), and the corrected default ANN (IVF-PQ) across the website.
- Fixed `cargo doc -D warnings` intra-doc links exposed by the storage-path refactor; `ruff format`
  on the sparse Python query files.
- CI hardening: `uni-sparse-vector` added to the crates.io publish list (a release would otherwise
  abort mid-publish), and every workflow job is now restricted to `rustic-ai/uni-db` so forks don't
  run CI / nightly / release workflows.

---

## Upgrade notes

- **Versioning**: the Rust workspace and both Python packages are now **2.4.1**; `uni-pydantic`
  tracks the `uni-db` version in lock-step.
- **New public API**: `StorageBackend::lock_table_for_write` → `TableWriteGuard` (default no-op for
  custom backends; the real implementation lives on `LanceDbBackend`).
- **API-boundary behavior changes**: malformed `SparseVector` values now return a clean error or are
  canonicalized instead of panicking; `uni.sparse.query` errors on an unsupported `filter` argument
  instead of silently dropping it.
- **Dense vector correctness**: dense `uni.vector.query` now sees committed-but-unflushed (L0) data —
  results that previously appeared stale until a flush are now correct. Code that relied on the old
  behavior should be re-validated.
- **Auto-embed on `SET`**: a `SET` that touches an embedding *source* column now re-embeds the target
  (dense, multi-vector, and sparse). Previously the embedding went stale.
- **`RETURN` of vector columns** now yields the typed `Value::Vector` (dense) / nested `Vector`
  tokens (multi-vector) rather than a generic `Value::List`. Code pattern-matching the returned
  variant should expect `Vector`.
- No breaking changes to existing Cypher or Locy query surfaces.

## Closed issues

#103, #110, #111, #112, #113, #115, #116, #117, #121.

The #95 (sparse) and #96 (multi-vector) epics are **feature-complete** with this release; both remain
open to track lower-value follow-ups (#114 OGM `hybrid_search()`, #122 residual auto-embed test
cells).
