# Sparse Vectors (SPLADE / learned-sparse) — Storage, Indexing & Hybrid Retrieval

- **Issue:** #95 (consumer/storage side). Producer side `rustic-ai/uni-xervo#40` (`EmbedSparse` / `SparseEmbeddingModel`) is **CLOSED/shipped** — dependency unblocked.
- **Status:** PARTIALLY IMPLEMENTED — M1–M2 core + M4 surface committed (`c5d8d6225` + `83e0ba686`) on branch `feat/sparse-vectors-issue-95` (not merged/pushed). Fork-local index (task #4) landed via Approach A. Test sets F (crash/WAL failpoints), H (OCC matrix; loom N/A), and I (metamorphic) now landed. Remaining: P2 block-max (M3), benchmark (M5), test set B′ (rank-safety, needs M3). See [§0 Implementation status](#0-implementation-status).
- **Date:** 2026-06-25 (proposal); implementation 2026-06-25
- **v1 decision (locked at impl time):** P1 brute-force DAAT **and** P2 block-max (rank-safe α=0 default), delivered via milestone gates **M1–M5**.

## 0. Implementation status

**Landed & green** (committed `c5d8d6225`; full `default-members` build + clippy + fmt clean):

| Area | What shipped | Tests |
|---|---|---|
| **M1** crate + type/codecs | `uni-sparse-vector` leaf crate (`SparseVector` + validating ctor, lossless LE binary codec, `sparse_dot`/`l2_norm`/`prune_top_k`); `Value::SparseVector` + `DataType::SparseVector`; Arrow `Struct{indices:List<UInt32>,values:List<Float32>}`; CV `TAG_SPARSE_VECTOR=20`; columnar + `arrow_to_value` read | crate unit + proptests; CV round-trip; columnar round-trip + silent-null regression |
| **M2** index + query | `SparseVectorIndex` (fork of `inverted_index.rs`): postings `(term_id, vids, weights, max_impact)`, `query_topk` (dot accumulator + min-heap), segment-merge build (backend-scan backfill + L0-incremental flush); `IndexDefinition::Sparse`; MVCC-correct `sparse_rerank` (L0 union + version/`_deleted` gating + exact rescore, drops zero-overlap); `uni.sparse.query` procedure; index DDL `OPTIONS{type:'sparse'}` + Rust builder `IndexType::Sparse` | `sparse_index.rs` (10) |
| **Durability** A/B/C/D/G | brute-force-oracle E2E (both build paths), L0 matrix (flush-equivalence, last-writer-wins, tombstone), snapshot isolation, restart/reopen | included in `sparse_index.rs` |
| **WAL durability fix** | mutation `Value`s now persist through the explicit CV codec, not untagged serde_json (fixes lossy crash-recovery for SparseVector + latently dense Vector); backward-compatible with legacy segments | WAL replay test + 97 uni-store WAL/recovery + 119 uni-db recovery/CRDT/temporal/btic regression |
| **Read/write surface** | `RETURN d.sparse_col` projection round-trip; `SPARSE_VECTOR(N)` Cypher type DDL | `sparse_ddl_type.rs` (2) + projection test |

**8 integration/correctness bugs found-and-fixed by the test surface:** DataFusion `is_df_eligible_procedure` routing allowlist; zero-overlap result semantics; backend-scan backfill (raw `Dataset::open` can't see the LanceDB table); `max_impact` init for negative weights; **WAL untagged-serde durability**; projection Utf8 fallback; generic-`Struct`-arm shadowing in `build_property_column_static`; `UInt32`-list result-row materialization.

**M4 landed & green (committed `83e0ba686` on the same branch; clippy 0, fmt, Rust + Python tests pass):**
- **Scalar `sparse_similar_to(a, b)`** — `eval_sparse_similar_to_pure` (`similar_to.rs:348`) registered at **2** sites (`df_udfs.rs:220`, `expr_eval.rs:1928`) — *not* the 3 the design predicted; `df_expr.rs` took only a 1-line change and there is **no `ScoringMode::Sparse` arm** (the sparse scalar routes through the UDF/`expr_eval` path, not the `similar_to_expr` ScoringMode). Accepts both `Value::SparseVector` and the `{indices,values}` Map form (a sparse param reaches a scalar UDF as an Arrow `Struct` decoded without schema context → arrives as a Map).
- **N-ary fusion** — `fuse_rrf_multi` (+ 2-arg `fuse_rrf` shim) and source-aware `fuse_weighted_sources` (`NormKind::{DistanceToSim,ScoreByMax}`) in `fusion.rs`; empty source = no-op, so 2-way is byte-identical.
- **3-way hybrid** — `run_hybrid_search` (`search_procedures.rs:1341`) parses an optional `sparse` property + `options.sparse_query`, reuses `sparse_rerank`, fuses under RRF *and* weighted, emits a `sparse_score` column (`HybridScoreContext.sparse_score_map`); `FusionKind::SparseRrf` + `FusionKind::SparseDot` in EXPLAIN (`planner.rs:1797,1802`).
- **Index DDL** — `CREATE VECTOR INDEX … OPTIONS{type:'sparse'}` is routed through `ddl_procedures.rs` (+`planner.rs`), *not* `build_vector_index_type` (`vector_index_opts.rs` still lists only dense algorithms); the design's predicted `Some("sparse")` hook there was unused.
- **Python** — fixed a real latent bug (`value_to_py` returned `None` for sparse props); `PySparseVector` + ingestion collision fix; `DataType.sparse_vector(N)`; `sparse_vector:N` parser; OGM `SparseVector[N]` + `sparse_search()` builder + schema auto-indexing.
- Tests: `sparse_scoring.rs` (7), `test_async_e2e_sparse.py` (5), OGM `test_types.py`+`test_queries.py` (5).

**Task #4 — fork-local sparse index landed (UNCOMMITTED on the same branch; gates green — clippy 0 on touched crates, fmt-clean for new files, set E + sparse + all fork-index tests pass).** Approach A: on a fork, `StorageManager::sparse_search` brute-force scans the branch vertices table (Lance `base_paths` supplies inherited+fork rows; `_deleted=false` prefilter handles tombstones) and returns candidates; the already-correct `sparse_rerank` re-scores by exact `sparse_dot` — mirroring `multivector_search`'s branched path. `ForkLocalIndexKind::Sparse` is a planner/EXPLAIN marker (no index file, like `VidUid`) wired to `FusionKind::SparseDot`; the auto-build scheduler registers it. Approach B (a dedicated fork-local postings dataset) is documented in-code as a benchmark-gated future path (M5). New: `crates/uni/tests/common/fork/fork_index_sparse.rs` (5 tests).

**Test-set coverage (proposal §12 sets A–J), verified against `83e0ba686` + task-#4 work:** all sparse Rust tests are consolidated into 3 files (`sparse_index.rs` ~18 fns, `sparse_scoring.rs` 7, `sparse_ddl_type.rs` 2) + `uni-sparse-vector/tests/proptest.rs` (8) rather than the separate per-set files the plan sketched.

| Set | Status | Where |
|---|---|---|
| **A** type/codec + silent-null | ✅ | crate proptests; `sparse_ddl_type.rs`; columnar/`value_codec.rs` arm |
| **B** index vs brute-force oracle | ✅ | `sparse_index.rs` (`*_matches_oracle`); `sparse_scoring.rs` dot tests |
| **B′** P2 rank-safety (α=0 ≡ oracle) | ⬜ | blocked on M3 (P2 not built) |
| **C** L0/flush visibility matrix | ✅ | `sparse_index.rs`: l0-only, last-writer-wins, tombstone-hides-flushed, flush-equivalence |
| **D** MVCC/snapshot isolation | ◐ | `sparse_snapshot_isolates_reader_from_concurrent_insert`; not yet a full read-path matrix entry |
| **E** fork isolation + fusion | ✅ | `fork/fork_index_sparse.rs` (5): fused results, isolation both ways, honors-deleted, nested-fork, auto-built + EXPLAIN `SparseDot` |
| **F** crash/WAL failpoints | ✅ | `sparse_resilience.rs` (4): crash after-wal-flush (CV-codec decode fidelity), crash after-validate (atomicity), crash at `flush::after-rotate-before-lance` (no double-apply of flushed data), WAL-tail corruption skip. `--features failpoints` |
| **G** restart/reopen durability | ✅ | `sparse_persists_across_reopen` |
| **H** concurrency (OCC/loom) | ✅ | `ssi_read_path_matrix.rs` (2): `sparse_query_records_matches` (RW antidependency aborts), `sparse_query_disjoint_label_no_false_abort` (label-level precision). **Loom N/A** — `SparseVectorIndex` is stateless per-query (no `Arc`/`Mutex`/`RwLock`), so there is no shared mutable state to model-check. Read-set is recorded via `sparse_rerank`'s property fetch (`record_vertex_read`), not a procedure-exec wrap |
| **I** metamorphic/soak | ✅ | `metamorphic/sparse.rs`: `uni.sparse.query` index path ≡ brute-force `sparse_dot` oracle; score-vector comparison (tie-stable); smoke (per-PR) + `#[ignore]` soak (nightly `test(/soak/)` filter). First vector-modality metamorphic case |
| **J** Python E2E | ✅ | `test_async_e2e_sparse.py` (5) + OGM (5) |

**Not yet implemented (follow-up, tracked in §15):**
- **M3 / P2** — block-max pruning + rank-safety equivalence test (set B′). The `max_impact` column is stored from M2 as the prerequisite but is **never read** in `query_topk` today; weights are still **f32** (8-bit quantization ships with P2, `SparseVectorIndexConfig.quantize` flag present, default true, but unimplemented).
- **Test set B′** (P2 rank-safety) — needs M3. (Sets **F**, **H**, **I** are now ✅; see the table above.)
- **M5** — real-corpus benchmark; OGM `hybrid_search()` builder (deferred — GitHub issue #114).

**Set F durability observation (general, not sparse-specific, unconfirmed):** the crash-during-flush test surfaced that a panic at `flush::after-rotate-before-lance` followed by a graceful `drop(db)` + reopen recovers only the *flushed* rows — a committed-but-unflushed delta sitting in the rotated buffer is gone (reopen replays the WAL from the last manifest's `wal_high_water_mark`, `uni/src/api/mod.rs:3381`/`:3592`). The rotation (`writer.rs:3845-3860`) is payload-agnostic, so this is engine-wide, not a sparse concern; it may also be a graceful-`Drop` artifact that wouldn't reproduce under a real (no-`Drop`) crash. Set F therefore asserts only the **no-double-apply / flushed-data-intact** invariant the proposal's §12.F intended; the unflushed-delta atomicity question is flagged for a separate durability investigation.
- **Auto-embed wiring to xervo `EmbedSparse`** — upstream-blocked: the current `uni-xervo` dependency exposes no `EmbedSparse`/`HeadSet::SPARSE` (only `DENSE | MULTI_VECTOR`), and `SparseVectorIndexConfig` has no `embedding_config`.

**Known residual gaps (documented):** dense `Vector` likely shares the (now-fixed-for-sparse) WAL gap and a `RETURN` projection gap — only the sparse arms were added; secondary indexes (all kinds) are not maintained during WAL recovery and need a rebuild after an unflushed-then-recovered write (universal engine property, not sparse-specific).
- **Dependency arrow:** uni-db → uni-xervo (uni-db calls xervo to embed text → stores + indexes + scores the sparse result).
- **Scope:** new first-class `SparseVector` value type (its own crate, BTIC-style), a scored sparse inverted index, dot-product scoring fusible into the existing dense-ANN + BM25 hybrid machinery, auto-embed wiring, and full production-readiness (MVCC / fork / crash-recovery / restart / WAL-replay) coverage.

**Validation summary.** Every structural claim below is anchored to a `crate/path.rs:line`. The design was derived from a code-level read of all seven layers a vector modality touches, using the multivector/ColBERT feature (#96, shipped 2.3.0) and the `uni-btic` crate as the two governing precedents. The headline conclusions: (1) this type **cannot** be Option-A "free" the way #96 was — it needs real `Value`/`DataType` variants; (2) it lives in **its own leaf crate** (`uni-sparse-vector`) exactly like `uni-btic`; (3) the index is a **fork of the hand-rolled `inverted_index.rs`**, not a Lance backend call (Lance has no sparse ANN primitive); and (4) the existing `InvertedIndex` has three production defects — not fork-aware, no MVCC/tombstone awareness, O(all-postings) per flush — that a production sparse index **must** close. The retrieval algorithm choice is staged behind benchmarks (P1 brute-force DAAT → P2 block-max pruning → optional P3 clustering), because the SOTA research shows classical WAND *degrades* on learned-sparse weights.

---

## 1. Summary

Store a learned-sparse vector (`{u32 term_id → f32 weight}`, e.g. SPLADE-v3 / BGE-M3 sparse head) as a first-class column type, index it in a scored inverted index (`term_id → postings of (vid, weight)`), score by dot product, return top-k, and fuse with dense ANN + BM25. P1 ships brute-force document-at-a-time (DAAT) dot — correct and fast at realistic corpus sizes; P2/P3 add block-max pruning only when a real-corpus benchmark proves it is needed.

## 2. The pivotal finding — this is the *inverse* of #96

#96's pivotal finding was "Lance already does the hard part" (MaxSim over `List<FixedSizeList>`), so multivector rode existing machinery with zero new enum variants ("Option A", confirmed at `arrow_convert.rs` where a multivector is `List<Vector>` built by the `DataType::Vector` arm at `arrow_convert.rs:1571-1607`; no `Value`/`DataType` variant exists).

For sparse the finding is the **opposite, in two ways**:

1. **The dependency does NOT do the hard part.** Lance has no sparse/SPLADE ANN primitive — `LanceDbBackend::vector_search` (`backend/lance.rs:727`) wraps `tbl.vector_search`, but there is no `tbl.sparse_search`. So a sparse index cannot ride the `StorageBackend` trait the way dense does. Instead it must extend the **hand-rolled `InvertedIndex`** (`uni-store/src/storage/inverted_index.rs`), which is *already* a term→VID posting store persisted as its own Lance dataset (`{base}/indexes/{label}/{property}_inverted`, `inverted_index.rs:73-76`) and queried by scalar-filter scan, bypassing the backend trait entirely (reached via `manager.inverted_index()` at `manager.rs:1735`).

2. **The type cannot be "free."** A multivector is homogeneous floats (`List<Vector>`). A sparse vector is two *heterogeneous* parallel arrays (`u32` indices + `f32` weights) that compose with nothing existing. It therefore needs genuine new `Value::SparseVector` and `DataType::SparseVector` variants — the same surface as the original `Vector` variant, not the #96 feature.

**Consequence:** the closest precedent for the *index* is `inverted_index.rs`; the closest precedent for the *type* is the original `Vector` variant + the `uni-btic` crate's organization; the closest precedent for the *retrieval orchestration* is `multivector_rerank` (`search_procedures.rs:347`). All three are used below.

## 3. Current state (what blocks us today)

| Layer | Today | File anchor |
|---|---|---|
| Value | `Value::Vector(Vec<f32>)`; no sparse | `uni-common/src/value.rs:551` |
| Schema type | `DataType::Vector { dimensions }`; no sparse | `uni-common/src/core/schema.rs:154` |
| Arrow lowering | `Vector` → `FixedSizeList<Float32>` (exhaustive `to_arrow`, no `_`) | `schema.rs:168-248` (arm 203) |
| Type accept | `accepts()` exhaustive (no `_`) | `schema.rs:292-352` (arm 348) |
| Arrow write (typed) | `build_vector_column` via `FixedSizeListBuilder` | `arrow_convert.rs:1107-1137`, `1443-1464` |
| Arrow read | `value_from_column` — **`_ => Ok(Value::Null)` silently drops unknown** | `value_codec.rs:112`, danger arm `:362` |
| CV codec | tagged msgpack, `TAG_VECTOR=16`, `TAG_BTIC=19` | `cypher_value_codec.rs:69,72` |
| Index (inverted) | term→VID, **set-membership only, no weights/scores** | `inverted_index.rs:261` `query_any` |
| Index (dense ANN) | Lance-backed; no sparse primitive | `backend/lance.rs:727` |
| Scoring kernel | `maxsim` (multivector); no sparse dot | `uni-query-functions/src/similar_to.rs:261` |
| Fusion | `fuse_rrf`/`fuse_weighted` hard-coded **2 lists** | `fusion.rs:15,22,39` |
| Hybrid orchestration | dense + BM25 only | `search_procedures.rs:1086` `run_hybrid_search` |
| Cypher type DDL | parameterized `VECTOR(N)`/`LIST<>` (#96) | `cypher.pest:751-761`; `parse_data_type` `write.rs:1172` |
| Python | `DataType.vector(N)`; dict→`Value::Map` | `bindings/uni-db/src/types.rs:2605`, `convert.rs:376-384` |
| Fork-local index | `ForkLocalIndexKind` has no inverted/sparse variant | `uni-store/src/fork/scope.rs:46` (`#[non_exhaustive]`) |

**Key observation:** the inverted index exists but is (a) unscored, (b) not fork-aware (hardcoded primary `base_uri`), (c) MVCC-unaware, and (d) O(all postings) per flush. Closing (b)–(d) is the bulk of the production work and is *net-new value* (it also fixes latent issues in the existing FTS-style inverted path).

## 4. Crate structure — a new leaf crate, BTIC-style

Per the BTIC precedent (`crates/uni-btic`), the new type gets **its own crate** with **zero `uni-*` dependencies**, sitting at the bottom of the dependency graph so `uni-common → uni-sparse-vector` is strictly one-directional (no cycle).

**New crate: `crates/uni-sparse-vector`** (`uni-btic/Cargo.toml` is the template — inherit `version/edition/authors/license/repository` from `.workspace`; deps minimal: `serde`, `thiserror`, optionally `half` for f16 weights and `bytemuck` for packing; `proptest` as dev-dep).

What the crate **owns** (mirroring how `uni-btic` owns its struct + binary codec + interval ops):
- `pub struct SparseVector { indices: Vec<u32>, values: Vec<f32> }` with a **validating constructor** `new(indices, values) -> Result<Self, SparseError>` (enforces equal length, sorted-unique indices, finite weights) — mirror `Btic::new` (`uni-btic/src/btic.rs`).
- `encode` module: `encode(&SparseVector) -> Vec<u8>` and `decode_slice(&[u8]) -> Result<SparseVector, SparseError>` (variable-length, unlike BTIC's fixed 24 bytes; length-prefixed `[n][indices…][values…]`, optionally quantized). Self-contained, dependency-free — mirror `uni-btic/src/encode.rs`.
- `ops` module: the **pure `sparse_dot(a, b) -> f32`** merge-join kernel (the analogue of `uni-btic`'s `predicates`/`set_ops`), plus `l2_norm`, `prune_top_k` (query-side term pruning). Pure CPU, no graph/runtime deps — this is the right home (not `uni-query-functions`) because the kernel has no DB dependencies, exactly like BTIC's interval math lives in the crate.
- `SparseError` (thiserror); `lib.rs` re-exporting `SparseVector`, `SparseError`.

What stays in the **integration crates** (BTIC puts the tag framing / Arrow / DDL glue here, not in the type crate):
- `uni-common`: the `Value`/`DataType` variants (plain fields, reconstruct `SparseVector::new` only at boundaries — BTIC pattern), the CV tag framing, schema→Arrow mapping.
- `uni-store`: Arrow column builders, the `SparseVectorIndex`.
- `uni-query` / `uni-query-functions`: DDL string parsing, scoring/fusion orchestration, procedures (these *call* `uni_sparse_vector::ops::sparse_dot`).

**Workspace wiring:** add `"crates/uni-sparse-vector"` to `members` and `default-members` in root `Cargo.toml`; add `uni-sparse-vector = { path = "crates/uni-sparse-vector", version = "2.0.0" }` under `[workspace.dependencies]`; then `uni-sparse-vector = { workspace = true }` in `uni-common`, `uni-store`, `uni-query`, `uni-query-functions` (the same four consumers BTIC has).

## 5. Proposed data model

### 5.1 Type + Value (the new variants — unavoidable)
- `DataType::SparseVector { dimensions: usize }` at `schema.rs:154` (dimensions = term-space cardinality / max term_id, for validation + index config). `#[non_exhaustive]`, so external matches are safe; **in-crate exhaustive matches `to_arrow` (`:203`) and `accepts` (`:348`) are compiler-enforced.**
- `Value::SparseVector { indices: Vec<u32>, values: Vec<f32> }` at `value.rs:551` — a **top-level** variant (unlike BTIC which nests under `TemporalValue`), holding plain fields per the BTIC split. Must touch the exhaustive arms: Display (`:729`), PartialEq (`:773`), **Hash (`:817`) — replicate the `Vector` arm's f32 signed-zero/NaN bit-normalization** (`value.rs:822-829`) or break the Hash/Eq contract (`Value` is a HashMap key), and `From<Value> for serde_json::Value` (`:1540`, no `_` arm). Hand-write `Eq`/`Hash` (cannot derive on `f32`) following the `TemporalValue` pattern (`value.rs:70-113`).

> Gotcha: `Value` is `#[serde(untagged)]` (`value.rs:521`). A struct-shaped variant can mis-deserialize as `Map`. **Do not rely on untagged serde for persistence** — all real persistence goes through the explicit codecs (§5.3).

### 5.2 Arrow representation
`Struct { indices: List<UInt32> (non-null), values: List<Float32> (non-null) }` — two parallel variable-length lists in one struct. Chosen over `Map<UInt32,Float32>` (which lowers to interleaved `List<Struct{key,value}>`, `schema.rs:221-246` — worse for SIMD dot) and over two top-level columns (a property is one column). Add `is_sparse_vector_struct()` + `sparse_vector_struct_fields()` helpers in `schema.rs` next to `is_datetime_struct` (`:70-77`) so write (`arrow_convert.rs`) and read sides cannot drift — the lockstep discipline the codebase already uses for temporal structs.

### 5.3 Codecs (two paths, both must learn sparse)
- **Columnar (typed/declared schema):** write `build_sparse_vector_column` (new, mirror `build_vector_column` `arrow_convert.rs:1443`, dispatched from `:1118`); read explicit arm in `value_from_column` (`value_codec.rs:112`) — **must be explicit** because the `_ => Ok(Value::Null)` fallback (`:362`) silently vanishes data; add `SparseVector` to the `decode_column_value` fidelity routing (`:378`).
- **Tagged msgpack (CV / nested / Map-value):** `TAG_SPARSE_VECTOR = 20` (BTIC took 19; 15 is a stale hole — use 20). Encode (`cypher_value_codec.rs:424`, exhaustive) frames `uni_sparse_vector::encode::encode`; decode (`:196`, loud error) calls `uni_sparse_vector::encode::decode_slice`. Add a round-trip unit test mirroring `test_round_trip_vector` (`:691`).

### 5.4 Schemaless
**Out of scope for v1**, matching dense/multivector (both declared-schema-only; the schemaless `values_to_array` path explicitly does not handle `List<FixedSizeList>`, `arrow_convert.rs:1578-1581`). Declared sparse columns are non-nullable; an empty sparse vector stores as empty lists (`[]`, valid), not null/omitted.

## 6. The sparse index (route B — fork `inverted_index.rs`)

A new `SparseVectorIndex` (new file `uni-store/src/storage/sparse_index.rs`, forked from `inverted_index.rs`) + a new `IndexDefinition::Sparse(SparseVectorIndexConfig)` arm at `schema.rs:788` (update all 4 match arms `:794-835`).

**Postings (on disk, Lance dataset):** `(term_id: UInt32, vids: List<UInt64>, weights: List<Float32>, max_impact: Float32)`. The `max_impact` column is the per-term upper bound — cheap to maintain and the prerequisite for P2 block-max pruning. (Fork `write_postings` `inverted_index.rs:213`.)

**In-memory build accumulator:** `HashMap<u32, Vec<(u64, f32)>>` (was `HashMap<String, Vec<u64>>`, `inverted_index.rs:98`). Per-doc duplicate term_ids resolved by sum (or last-write). Segment flush at 256 MB (`DEFAULT_MAX_POSTINGS_MEMORY` `:26`), merge zips weights (`merge_postings_segments` `:37`).

**Query — `query_topk(query: &[(u32,f32)], k)`** (fork `query_any` `:261`): SQL filter `term_id IN (…)`, stream postings, accumulate `HashMap<Vid,f32>` dot product (`*scores.entry(vid) += qw * dw`), drain into a bounded min-heap, return `Vec<(Vid,f32)>`. Dot is intrinsic — no `DistanceMetric`.

**Weight quantization:** keep build accumulator at f32; quantize to 8-bit at the serialization boundary in `write_postings` (store `weights: List<UInt8>` + per-term `scale: Float32`), dequantize in `query_topk`/`load_postings`. Research consensus: 8-bit ≈ lossless, ~4× smaller. Gate behind a config flag, default on.

## 7. Retrieval modes (phased, benchmark-gated)

The SOTA literature is unambiguous that classical dynamic pruning (WAND/BlockMax-WAND) **degrades on learned-sparse weights** — measured *slower than brute force* on SPLADE because the impact distribution is flat and high-DF terms (stopwords, subwords) defeat skipping. So phasing is deliberate:

- **P1 — brute-force DAAT dot (ship first).** `query_topk` as above, plus a rerank-style path that brute-forces over candidates when no index exists (mirror the fork branch of `multivector_rerank` `search_procedures.rs:428-434`). Correct and adequate at realistic corpus sizes (a memory system is thousands–low-millions of items; Turso measured brute-force at 269–448ms on 400k docs and exact pruning gave only ~8.7×). This is what Qdrant/Milvus effectively ship on the live path.
- **P2 — block-max pruning (BMP-style), gated on a real-corpus benchmark.** Block-max forward index + per-block max-impact + `α` (rank-safe at α=0) + `β` (query-term pruning). Uses the `max_impact` column already stored in P1. Only pursue if P1 latency on uniko's real corpus misses target. Rank-safe-capable.
- **P3 — clustered index (Seismic-style), optional.** Static list pruning + k-means blocks + summary vectors + forward-index rescore. Approximate-only. Only if P2 is insufficient at much larger scale. (OpenSearch 3.3 shipped Seismic — it is productionizable, but it fights incremental mutation; treat as a separate index variant, not the default.)

**Decision fork to settle before P2:** rank-safe (BMP/SP lineage) vs approximate-first (Seismic). For a database default, rank-safe is the safer first choice; expose approximation as an explicit opt-in knob.

## 8. Scoring + fusion

- **Kernel:** `uni_sparse_vector::ops::sparse_dot` (in the crate). `uni-query-functions/src/similar_to.rs` adds a thin `eval_sparse_similar_to` wrapper next to `maxsim` (`:261`) that calls it — same layering rationale as #96 (the kernel is pure; orchestration needs `PropertyManager`).
- **N-ary fusion:** generalize `fuse_rrf`/`fuse_weighted` (`fusion.rs:15,39`) to a slice of lists — the `for ranked_list in [vec, fts]` at `:22` becomes `for ranked_list in lists`. Keep 2-arg shims → backward compatible; an empty sparse list is a no-op, so existing 2-retriever queries are unchanged. Default fusion: **RRF (k=60)** (rank-based → sidesteps the scale mismatch between unbounded sparse dot and cosine dense); weighted fusion with min-max normalization as opt-in.
- **Orchestration (uni-query, not uni-store):** `sparse_search`/`sparse_rerank` as a near-clone of `multivector_rerank` (`search_procedures.rs:347`): candidate-gen (postings `IN`-filter, or brute-force scan) → L0 union (`collect_l0_label_candidates` `manager.rs:2424`, reusable as-is) → `property_manager.get_batch_vertex_props_for_label` → `sparse_dot` rescore → top-k. Lives in uni-query because property fetch needs `PropertyManager` (a runtime-layer struct `StorageManager` lacks — same constraint as #96).
- **Hybrid wiring:** in `run_hybrid_search` (`search_procedures.rs:1086`) parse a 3rd `sparse` property (`:1110`), add a sparse retrieval block after `:1204`, switch fusion dispatch to the N-ary forms (`:1206`), extend `HybridScoreContext` (`:524`) + `build_hybrid_search_batch` (`:691`) with `sparse_score`.
- **`similar_to`:** add `ScoringMode::Sparse` (`similar_to_expr.rs:236`) with an explicit marker to disambiguate from dense `List`.

## 9. Cypher surface

- **Type DDL:** `type_sparse_vector = ${ ^"sparse_vector" ~ "(" ~ ASCII_DIGIT+ ~ ")" }` in `cypher.pest:755` (clone of #96 `type_vector`); `parse_data_type` arm at `write.rs:1194`. Auto-propagates to `CREATE LABEL`/`ALTER ADD PROPERTY`.
- **Index DDL:** *no grammar change* — `CREATE VECTOR INDEX … OPTIONS {type:'sparse'}` reuses the existing rule; add `Some("sparse")` to `build_vector_index_type` (`vector_index_opts.rs:94`) + the two callers (`planner.rs:8389`, `ddl_procedures.rs:290`).
- **Procedure:** new `procedures_plugin/sparse.rs` (clone `fts.rs`) → `uni.sparse.query`; `run_sparse_query` in `search_procedures.rs`; 2-line registration in `procedures_plugin/mod.rs`.
- **Scalar fn `sparse_similar_to`:** register in 3 sites — `df_expr.rs:2238`, `df_udfs.rs:219`, `expr_eval.rs:2188`+`1914`.

## 10. Python surface

- **pyo3:** `DataType.sparse_vector(N)` factory (`types.rs:2610`); **fix the dict→Map collision** — a `PySparseVector` wrapper extracted *before* the PyDict branch in `convert.rs:376` (mirror `PyBtic` at `:337`), since `dict[int,float]` otherwise fails on `k.extract::<String>()`; `core.rs:552` string parser (`sparse_vector:N`, follow #96 full-remainder rule).
- **Pydantic OGM:** `SparseVector[N]` type + metaclass (`types.py:60`), emitter `python_type_to_uni` (`:452`), round-trip (`:343/401`). **Two greenfield gaps to close opportunistically:** the OGM has *no* hybrid/sparse/RRF search method (`query.py` has only `vector_search`), and `Field()` exposes only `metric` (no algorithm knobs) — add a `hybrid_search`/`sparse_search` builder + widen `FieldConfig`.

## 11. Production-readiness — maintenance obligations (the crux)

The existing `InvertedIndex` has three defects a production sparse index **must** close, plus the lifecycle obligations the dense/MUVERA path already meets:

1. **Fork-awareness (currently broken).** `InvertedIndex` path is hardcoded `{base_uri}/…` (`inverted_index.rs:73`), so a fork reads/writes the *parent's* postings — no isolation. **Fix:** add `ForkLocalIndexKind::Sparse` (the enum at `scope.rs:46` is `#[non_exhaustive]` and literally comments that an inverted variant is anticipated), resolve the dataset path through the fork branch (mirror `fork/index_builder.rs`), register on build, and fuse inherited + fork-local at query time honoring inherited `_deleted=false` (as the BM25 fork tests assert).
2. **MVCC / tombstone correctness (currently absent).** `InvertedIndex` has no `_version`/`_deleted` awareness. **Fix:** every result passes `collect_l0_label_candidates` (tombstones, `manager.rs:2424`) + `_version <= hwm` (`apply_version_filter` `manager.rs:671`) + `_deleted=false` — exactly the gates `vector_search` applies at `search_procedures.rs:419-451`.
3. **Scalable updates (currently O(all postings)/flush).** `apply_incremental_updates` does load-all → mutate → `WriteMode::Overwrite` (`inverted_index.rs:392-451`). **Fix:** reuse the segment-append/merge machinery (`temp_segments`, 256 MB) for updates instead of full rewrite; recompute per-term `max_impact` on delete.

Inherited lifecycle obligations (met by dense/MUVERA):
4. **Flush-time materialization** of postings/derived columns into the L0 buffer before Lance write — mirror `materialize_fde_columns` (`writer.rs:3424`), including `vertex_partial_keys` so SET updates don't go stale.
5. **Incremental update collection** in the flush loop (`writer.rs:4484-4627`).
6. **L0 visibility** — an L1-only index can't see unflushed rows; the query path unions L0 candidates (the brute-force-over-label path).
7. **Two build paths, tested separately** — DDL create vs `rebuild_indexes_for_label` (`index_manager.rs:833`); the `force_backfill` asymmetry between them is the exact #96 "rebuild-only bug hides from DDL" class. Verify all four `IndexManager::new` sites chain `.with_backend` if backfill scans the table.

**Observability:** emit a fusion-kind string in `.explain()` (mirror `FusedIndexScanWrapped` + `Bm25Rrf`/`AnnRerank`; add `SparseRrf`/`SparseDot`); add counters (postings scanned, candidates, L0-merged) reusing the SSI metrics substrate (`ssi_support/metrics.rs`).

**Failure injection:** add failpoints for sparse-index build/flush mirroring the `flush::*` seams (`writer.rs`).

## 12. Test plan — production-grade, mapped to real harnesses

Gates: `cargo nextest run` (workspace), TCK, Locy, pytest, clippy/fmt — per repo CI. Concurrency lanes: `--features loom` and `--profile soak`.

**A. Type / codec (crate + uni-common)**
- proptest round-trips in `uni-sparse-vector`: `encode`↔`decode_slice`, validating ctor rejects malformed input (mirror `uni-btic` proptests).
- CV codec round-trip (`TAG_SPARSE_VECTOR`) mirroring `test_round_trip_vector` (`cypher_value_codec.rs:691`); columnar round-trip both declared + nested-in-Map; `mutation_serde_roundtrip` analogue (`uni-store/tests/common/property/property_tests.rs`).
- **Explicit test that a missing read arm doesn't silently null** (regression for `value_codec.rs:362`).

**B. Index correctness (E2E, with a brute-force oracle)**
- Mirror **`multivector_muvera.rs::assert_matches_oracle`**: a `sparse_dot` brute-force oracle, asserting (a) exact score within EPS, (b) descending order, (c) top == oracle max, (d) full recall when retrieval is exhaustive. New file `crates/uni/tests/sparse_index.rs`.
- Incremental updates: mirror `inverted_index_test.rs::test_inverted_index_incremental_updates`.
- Quantization: assert recall delta float vs 8-bit within tolerance.

**C. L0 / flush visibility** — mirror **`multivector_l0.rs`** matrix: L0-only (no flush), mixed L0+flushed, L0 update (last-writer-wins), **L0 tombstone hides a flushed doc** (`test_l0_tombstone_hides_flushed_doc`), pre/post-flush ordering equivalence (`test_flush_equivalence`).

**D. MVCC / snapshot isolation** — mirror **`l0_snapshot_e2e.rs`** (`snapshot_isolates_tx_reads_from_concurrent_commit`, `snapshot_preserves_read_your_writes`) and add a sparse entry to the read-path conflict matrix (`ssi_read_path_matrix.rs`, alongside `vector_knn_records_matches`). `_deleted`/version-visibility via `*_honors_deleted_filter` analogues.

**E. Fork isolation + fusion** — mirror **`fork/fork_index_bm25.rs`** (the closest sparse analogue) + `fork_index_btree.rs` for planner observability: `sparse_fork_local_returns_fused_results`, `sparse_fork_isolation_both_ways`, `sparse_fork_honors_deleted_filter`, `nested_fork_sparse_resolves_through_ancestors`, `sparse_fork_auto_built_for_new_rows`. Assert `.explain().plan_text` contains the sparse fusion-kind string. Requires the `ForkLocalIndexKind::Sparse` variant (§11.1).

**F. Crash / recovery / WAL replay** — mirror **`ssi_resilience.rs`** (failpoints + `DiskHarness`) and **`wal_durability_test.rs`**: `sparse_index_survives_wal_recovery`, `crash_after_wal_flush_is_atomic` (sparse write), `crash_during_sparse_flush_no_double_apply` (panic failpoint at `flush::after-rotate-before-lance` → `drop(db)` → reopen → assert idempotent), tail-corruption skip / mid-corruption hard-error for the postings dataset. Fork recovery: mirror `fork_recovery/recovery_fork_*.rs`.

**G. Restart / reopen durability** — mirror **`multivector_muvera.rs::muvera_persists_across_reopen`**: build sparse index → flush → `drop(db)` → `Uni::open(path).build()` → re-query → assert identical top-k. Plus rebuild-from-WAL after reopen.

**H. Concurrency models** — add a sparse case to the OCC matrix (`ssi_occ_test.rs`); if any new shared mutable state is introduced, cover it under `--features loom` (`occ_model.rs`).

**I. Metamorphic / soak** — add sparse queries to the querygen/diff oracle harness (`metamorphic/`), run under `--profile soak`.

**J. Python E2E** (load-bearing — caught a Rust bug the Rust test missed in #96): pytest covering `DataType.sparse_vector`, dict ingestion (collision fix), `uni.sparse.query`, and OGM round-trip.

## 13. Storage cost & mitigations (priority order)
1. 8-bit weight quantization (~4× smaller, default on).
2. Segment-merge updates (avoid full-rewrite memory spikes; bound at 256 MB).
3. Query-side top-k term pruning (drop high-DF/low-weight query terms — the universal latency lever; the high-DF term is the worst-case driver).
4. Optional f16 weights (crate-level, via `half`).

## 14. Scope boundaries
**In:** `uni-sparse-vector` crate; `Value`/`DataType` variants + codecs; declared-schema sparse columns; `SparseVectorIndex` (P1 brute-force DAAT, scored); dot-product scoring; N-ary fusion + hybrid wiring; `uni.sparse.query` + `sparse_similar_to`; Python type + dict ingestion + OGM type; auto-embed wiring to xervo `EmbedSparse`; full MVCC/fork/crash/restart test coverage; fork-local sparse index.
**Out (deferred):** schemaless sparse columns; P2 block-max pruning and P3 clustering (separate, benchmark-gated); IDF query-weight modifier (Qdrant-style); sparse on edges (v1 = vertices); GPU scoring.

## 15. Recommended sequencing

Status legend: ✅ done · ◐ partial · ⬜ remaining. Committed across `c5d8d6225` (M1–M2 core + index) and `83e0ba686` (M4 scoring/fusion/hybrid/Cypher/Python).

1. ✅ **Crate** `uni-sparse-vector` (struct + validating ctor + binary codec + `sparse_dot` + proptests) — isolated, no `uni-*` deps. **(test A)**
2. ✅ **Type + codecs** in uni-common (variants, Arrow struct, CV tag) + uni-store column builders. **(test A)**
3. ✅ **`SparseVectorIndex`** (build/flush/`query_topk`/incremental, scored) with MVCC + tombstone gating + segment-merge build. **(tests B, C, D, G)** — *weight quantization deferred to step 9 (P2); crash failpoints (F) ⬜.*
4. ✅ **Fork-local sparse index** — Approach A (brute-force branch scan re-scored by `sparse_dot`, mirroring `multivector_search`): `ForkLocalIndexKind::Sparse` marker, fork-aware `sparse_search` branch scan, planner `SparseDot` fusion arm, auto-build scheduler arm. Approach B (dedicated fork-local postings dataset) documented in-code, deferred behind M5. **(test E ✅)**
5. ✅ **Scoring + N-ary fusion + hybrid wiring + `similar_to`.** — `sparse_rerank` orchestration, scalar `sparse_similar_to` (both `SparseVector` + Map forms), `fuse_rrf_multi`/`fuse_weighted_sources` (+`NormKind`), 3-way hybrid + `sparse_score` + `FusionKind::SparseRrf`/`SparseDot`. Delta from design: routed via the UDF/`expr_eval` path, **no** new `ScoringMode::Sparse` arm. **(test B ✅; metamorphic I ⬜)**
6. ✅ **Cypher surface** — `SPARSE_VECTOR(N)` type DDL, `CREATE VECTOR INDEX … OPTIONS{type:'sparse'}` (routed through `ddl_procedures.rs`, not `build_vector_index_type`), `uni.sparse.query`, `sparse_similar_to` scalar (registered in `df_udfs.rs` + `expr_eval.rs`).
7. ◐ **Python** — pyo3 `DataType.sparse_vector(N)` + `PySparseVector` + dict-collision fix + `value_to_py` arm ✅; OGM `SparseVector[N]` + `sparse_search()` builder + schema auto-indexing ✅ **(test J ✅)**. Remaining: ⬜ OGM `hybrid_search()` (deferred #114); ⬜ auto-embed wiring to xervo `EmbedSparse` (upstream-blocked).
8. ⬜ **Benchmark P1** on a real SPLADE corpus.
9. ⬜ **P2 block-max** (rank-safe α=0 default) + 8-bit weight quantization + rank-safety equivalence test (`max_impact` column already stored).

**Also landed beyond the original plan** (surfaced during implementation): WAL CV-codec durability fix (mutation `Value`s no longer lost on crash recovery); `RETURN`-projection round-trip for sparse columns.

## 16. Open questions
- ~~Own crate or in uni-common?~~ **Own crate `uni-sparse-vector`, BTIC-style** (decided per maintainer direction).
- ~~New variant or Option-A reuse?~~ **New variant required** (heterogeneous parallel arrays).
- Rank-safe (BMP) vs approximate-first (Seismic) for the eventual index — settle before P2, behind a benchmark.
- IDF query-weight modifier — useful for BM25-like sparse heads (BGE-M3); defer to a follow-up.
- Producer model default — SPLADE-Doc / no-query-encoder (zero query-side GPU) fits an embedded DB; confirm with xervo presets.
- Weight quantization default — 8-bit on by default vs opt-in.

## 17. Decision needed before implementation
Confirm the **v1 retrieval target = P1 brute-force DAAT only** (no block-max pruning in v1), with P2/P3 gated on a real-corpus benchmark. This keeps v1 correct, incremental-mutation-friendly, and shippable, and avoids committing the storage engine to a fast-churning research index (SP→LSP moved within a year) before scale demands it.
