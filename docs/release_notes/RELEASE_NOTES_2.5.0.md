# uni-db 2.5.0

**Release focus: typed 3-way hybrid search in the Python OGM**, a query-time HNSW `ef_search`
knob backed by at-scale recall benchmarks for all three vector modalities, capability-based
embedding-alias routing (one BGE-M3 alias can now serve dense, sparse, and multi-vector columns),
and a query-planner correctness/performance pass (equi-join recovery, projection-pushdown leaks).

This is a minor release covering everything since **2.4.1**. 29 commits; version bumped across the
Rust workspace and the Python packages (`uni-db`, `uni-pydantic`) to **2.5.0**. New public surfaces:
the OGM `hybrid_search()` builder and `.search_scores` sidecar, the `ef_search` option on
`uni.vector.query`, and `UniConfig::flush_stream_timeout`. API-boundary behavior changes are listed
under Upgrade notes.

---

## Highlights

### 🔹 OGM `hybrid_search()` builder + `.search_scores` sidecar — #114

The engine's three-way fused retrieval (`uni.search`: dense + full-text + sparse) is now reachable
from the typed Pydantic OGM instead of hand-written Cypher.

- **`Model.query().hybrid_search(vector=, fts=, sparse=, method=, weights=/alpha=, k=, ...)`** emits
  the 7-positional `uni.search` call. All user data is bound as `$params`; only schema identifiers
  and numeric knobs are interpolated, and every resolved property name passes a format-only
  validation before it reaches the query string (closing an injection surface across all three
  search builders).
- **`.search_scores` sidecar.** Results carry fused + per-arm scores via a `UniNode` private
  attribute, so scores survive hydration under `extra="forbid"` and never collide with a user field
  named `score` (the elasticsearch-dsl `.meta.score` pattern, done Pydantic-correctly).
  `vector_search` and `sparse_search` were retrofitted with the same sidecar for a uniform scored
  surface.
- **Latent hydration bug fixed.** `vector_search`/`sparse_search` previously emitted
  `RETURN node AS n`, which cannot hydrate through the OGM's row decoder — their `.all()` silently
  returned `[]`. Both now return the `properties()/id()/labels()` triple plus score columns, with
  execution tests (including previously-absent async coverage) locking the surface in.
- Design and ecosystem survey: `docs/proposals/ogm_hybrid_search.md`. Package suite grew 240 → 257
  tests.

### 🔹 Query-time HNSW `ef_search` + at-scale recall benchmarks

New CI-runnable criterion recall benches give all three modalities (sparse / dense / multi-vector)
an oracle-vs-engine recall+latency benchmark through the public `uni.vector.query` path
(`benches/dense_retrieval.rs`, `benches/multivec_retrieval.rs`; sparse already existed).

- The dense bench surfaced a real product gap: the HNSW search beam width was never plumbed to
  query time, so it sat at lancedb's `1.5·k` default and **recall@10 collapsed to 0.300 at 10k
  docs**. `ef_search` (alias `ef`) is now accepted in the `uni.vector.query` options map and
  threaded through `VectorQueryOpts.ef` to the index search.
- With a wide beam, dense HNSW recall@10 recovers **0.300 → 1.000 at 10k docs** at essentially
  unchanged latency (6.1 → 6.6 ms).

### 🔹 Capability-based embedding alias routing — #129, #130

Routing and open-time validation assumed one task per alias, so a hybrid model (BGE-M3,
`EmbedHybrid`) could not serve a plain vector column on its own alias.

- **Validation (#130).** The open path rejected any non-`Embed` task on a Vector index — a dense or
  multi-vector index on an `EmbedHybrid`/`EmbedMultiVector` alias created fine but failed to
  reopen, and sparse aliases were never validated at all. The blanket check is replaced with a
  capability test (`required_heads ⊆ text_embedding_heads(task)`) across Vector and Sparse indexes,
  naming the offending column on failure. Image/audio/multimodal embed tasks map to no text heads,
  so binding a text auto-embed column to them is rejected at open.
- **Routing (#129).** A lone head routed to the narrow single-task facade, which hybrid models do
  not implement. Lone-head requests now fall back to the hybrid embedder for the requested head —
  with no double model load, and a head the model does not expose remains a hard error, never a
  silently-empty column.
- The shared capability model lives in a new `uni-store` module (`embed_caps`), with a unit matrix
  plus end-to-end alias-capability tests. Design: `docs/proposals/embedding_alias_capability_model.md`.

### 🔹 Equi-join recovery: HashJoin instead of Cartesian products — #131

A recursive Locy IS-ref join degraded to a quadratic `CrossJoinExec` because the join-recovery
variable collector silently dropped `LocyDerivedScan` behind a `_ => {}` wildcard — the derived
relation's columns were invisible, so equality conjuncts were never recognized as equi-join pairs.
The same defect was reachable from Cypher via named-path cross-MATCH equi-joins (`BindPath`).

- The collector (and five sibling plan walkers with the same latent bug class) are now exhaustive:
  a new `LogicalPlan` variant fails to compile until it is classified.
- Node scans register their `_vid` key form so IS-ref equality keys match exactly; the hash-join
  recovery path mirrors the CrossJoin lowering's structural-column stripping.
- Regression tests prove `CrossJoinExec` rows now scale linearly, and HashJoin recovery is
  exercised for `BindPath`, `VectorKnn`, `ShortestPath`, `AllShortestPaths`, and
  `BindZeroLengthPath` — each shown to go red on a blinded collector.

### 🔹 Projection-pushdown "*" leaks closed — #134

A dense `similar_to` scan ran **~60× slower** when scanned rows carried an unread `List(Vector)`
column: a bare entity reference marked the scan as needing all properties (`"*"`), decoding wide,
unused columns per row. The whole leak family is closed:

- Scalar functions over entities (`id`/`elementId`/`type`/`count`/`startNode`/`endNode`) no longer
  re-add `"*"` through a second recursion into their arguments.
- `count(DISTINCT n)` dedups on the entity identity column instead of the full struct;
  `collect(DISTINCT n)` dedups by identity for entity maps — fixing a latent HashMap-order
  nondeterminism that meant it previously **never actually deduped**.
- `WITH n` / `WITH n AS m` narrow the forwarded entity to the properties actually accessed
  downstream; returned-whole entities, rename chains, and non-narrowable kinds (paths) stay wide to
  avoid silent NULLs.

### 🔹 Traversal reads survive the L0 → Lance flush — #135 and the schemaless `_all_props` fix

Two related property-materialization gaps, both reported as mysterious NULL/empty reads:

- **#135.** The single-hop traverse operator materialized target-vertex properties from the L0
  in-memory buffers only. Once the ~5s auto-flush migrated rows to Lance, every target property of
  `MATCH (a)-[:REL]->(b) RETURN b.prop` read back NULL — deterministic, not the "race under CPU
  load" the report hypothesized. Targets are now prefetched through the property manager, which
  merges L0 + persisted storage under the query's MVCC visibility.
- **Schemaless `_all_props`.** In schemaless mode the "all properties" wildcard collapsed to an
  empty name list (it enumerated schema-declared names — none exist without a schema), so multi-hop
  traversal targets and whole-node projections returned empty `{}` nodes. The wildcard sentinel is
  now threaded through the batch property helpers, which also gained a main-table (`props_json`)
  fallback for flushed schemaless vertices. Fixes the schemaless-lane openCypher TCK
  `MatchWhere6 [7]/[8]` and Locy TCK `AssumeAbduce` failures.

### 🔹 Traversal label resolution survives the L0 → Lance flush — the `_labels` family, #141, #140

A vertex's *labels*, like its properties (#135), leave the in-memory L0 buffers once it is flushed
to Lance and live only in the persisted `VidLabelsIndex`. Three label-resolution seams read L0 only
and misbehaved after a flush (the ~5 s auto-flush, an explicit `db.flush()`, or a fork, which
flushes all data before branching):

- **Output columns (the `_labels` family).** Three traversal output builders — the single-hop sync
  fast-path and both variable-length-path builders (schema-aware and schemaless) — resolved a
  target's `_labels` column from L0 only, so `labels(m)` / `hasLabel(m, …)` over a flushed or forked
  vertex returned an **empty** label set. All three now route through a shared per-row resolver
  (`resolve_vertex_labels`: L0 chain then persisted index), making every current and future
  traversal label-column correct by construction; a nullable-aware builder covers unmatched VLP rows.
- **VLP label filters over-admitted (#141).** The variable-length-path predicates
  `check_target_label` (terminal `(m:Label)`) and `check_state_constraint` (QPP intermediate
  `(y:Label)`) read L0-only labels and failed **open** — admitting a flushed vertex without checking
  its label. `MATCH (a)-[:R*1..n]->(m:Label)` and QPP intermediate-label constraints could return
  paths through vertices that don't carry the required label. Both predicates now evaluate against
  the vertex's real labels via `resolve_vertex_labels`. **Behavior-changing**: affected VLP/QPP
  queries now reject rows they previously over-admitted.
- **Deleted-vertex label resurrection (#140).** `resolve_vertex_labels` consulted the persisted
  index without checking tombstones, so a flushed-then-deleted vertex — its tombstone recorded in a
  live L0, its index entry not yet re-flushed — could resolve to its **stale** labels. The resolver
  now returns an empty label set for a tombstoned vid, so every label predicate rejects the deleted
  vertex instead of admitting it through a fail-open path. (End-to-end this is masked by cascade
  edge-tombstoning; the guard closes the seam at the resolution chokepoint.)

Each fix ships with tests verified red-before / green-after: the QPP over-admission is the observable
#141 regression, a white-box resolver test is the #140 regression, and the terminal-filter hardening
is defense-in-depth behind a downstream `_labels` filter that already resolves correctly.

### 🔹 Declared `VECTOR(dim)` columns now enforce their dimensions — #137

Wrong-dimension vectors written into a declared `VECTOR(dim)` (or multi-vector
`List(Vector(dim))`) column were silently accepted and **nulled at flush**, and a mismatched query
vector silently returned 0 rows. Dimensions are now enforced end-to-end:

- **Write-time:** a wrong-length vector (or a list with non-numeric elements, or an empty list)
  fails with a `TypeError` naming the declared and actual lengths — on Cypher `CREATE`/`SET`, the
  bulk insert APIs, and auto-embed output (a model whose output width differs from the declared
  dimension fails with an error naming the embedding alias). Multi-vector columns enforce
  per-token dimensions.
- **Query-time:** `uni.vector.query` (and hybrid dense search) with a wrong-length query vector
  errors with "vector dimension mismatch" instead of silently returning 0 rows.
- **Schema re-declare:** re-applying an identical schema stays idempotent (the
  register-on-every-open pattern), but re-declaring an existing property with a different
  type/dimension (e.g. `VECTOR(4)` → `VECTOR(8)`) now raises a schema conflict error (Python:
  `UniSchemaError`). Property types are immutable — use a new property name or migrate the data.
- **Flush is fail-closed:** a wrong-dimension value that somehow reaches flush errors instead of
  being silently nulled; WAL replay of values written by pre-2.5.0 versions nulls them with a
  warning log so old databases stay recoverable.

### 🔹 Stalled flush streams can no longer wedge the runtime — #132

A sparse/multi-vector flush performs an extra Lance read-modify-write that can stall; the stalled
stream never reported to the flush coordinator, whose strictly-ordered finalizer then wedged. Under
saturation the next commit fell back to an *untimed inline* flush holding the flush lock — parking
the entire runtime.

- The stream phase is now bounded by **`UniConfig::flush_stream_timeout`** (default 60 s, env
  `UNI_FLUSH_STREAM_TIMEOUT`); an elapsed or cancelled stream converts into a data-safe failure the
  pipeline already recovers from (old L0 retained in `pending_flush` + WAL). An RAII guard covers
  task cancellation, and a new metric `uni_flush_stream_timeouts_total` reports occurrences.
- Under async flush, pipeline saturation no longer falls back to a blocking inline flush — the
  commit skips and retries the flush later (the inline path has no timeout; this fallback was the
  actual full-park).
- Failpoint tests model a persistent stall and assert recovery with no data loss (with a negative
  control proving they catch the original bug); the underlying Lance stall is environment-specific,
  and this layer bounds and reports it whenever it recurs.

### 🔹 Cypher parser: exponential backtracking eliminated

The nightly `locy_parse` fuzz target timed out on a 130-byte input: the grammar's index (`[e]`) and
slice (`[e?..e?]`) alternatives both began `"[" ~ expression`, so each unmatched `[` parsed the full
expression twice — O(2^N) across a run of brackets, blowing up below the legitimate nesting-depth
guard. The shared prefix is factored into a single silent rule (accepted language and parse tree
unchanged): the failing artifact drops **1.56 s → 1.6 ms (~1000×)**, and a regression test with a
thread-based timeout makes any reintroduced ambiguity fail loudly instead of hanging the fuzzer.

### 🔹 Python type-stub resync + drift guard

The hand-maintained `uni_db/__init__.pyi` had silently diverged from the compiled pyo3 surface —
about 100 drift items. The stub now documents 17 previously-missing classes (fork diff/promote
types, `SparseVector`, `Btic`, …) and ~60 missing methods (fork lifecycle, `explain`/`profile`
builders, Xervo prefetch, …), drops 9 phantom methods, and fixes 11 mypy builtin-shadowing errors.
A new `test_stub_drift.py` introspects the compiled extension and asserts stub ↔ module parity in
both directions on every PR, so the stub can no longer drift silently.

---

## Reliability & testing infrastructure

- **Vector index coverage parity.** `HnswFlat` (previously never instantiated) and explicit
  `IvfRq` `num_bits` round-trip/query coverage; a new `vector_recall.rs` suite measures recall@k
  per quantized mode against a brute-force oracle with floors calibrated to each quantizer's real
  fidelity; per-field multi-model auto-embed coverage; 2-way vector+FTS `uni.search` RRF/weighted
  fusion tests.
- **De-flaked the HNSW `ef_search` recall test.** The old single-probe recall@10 assertion tripped
  on nondeterministic graph builds (~1/4 runs). It now averages recall over 32 deterministic probe
  queries and asserts a calibrated low-vs-wide beam gap — a dead `ef_search` knob yields a gap of
  exactly 0, making the regression check stronger *and* stable (20/20 in isolation).
- **BGE-M3 real-model tests use the standard Hugging Face hub cache** by default (override via
  `BGE_M3_DIR`) instead of stranding a ~2.2 GB download inside the source tree per checkout.
- **Positive 3-way BGE-M3 coverage.** The `issue_133` tests were renamed to
  `bge_m3_{hybrid_3way,real_onnx}` after #133 was closed as misattributed — the multi-vector column
  round-trips correctly, and the suites now serve as the canonical single-pass `EmbedHybrid`
  end-to-end coverage (mock + real ~2.1 GB ONNX model).
- **Candle embed tests modernized** to the alias-catalog API (the dead inline
  `embedding: {provider, model, source}` DDL form is no longer accepted).
- **Code-simplifier sweep** across 35 crates: idiomatic rewrites, dead-code and
  redundant-abstraction removal; no functional changes; net −543 lines with the full local CI green
  on the swept tree.

## Dependencies

- No external dependency changes in this cycle (`uni-xervo` stays at 0.17; lance/arrow pins
  unchanged).

## Documentation & CI

- **Local CI runbook** (`docs/local_ci_runbook.md`): every pr.yml + ci.yml job with its exact local
  command, prerequisites, ordering/contention notes, and local-only gotchas.
- **New website guides** for sparse vectors and BGE-M3 3-way hybrid retrieval, plus extended
  indexing/hybrid-search pages; the OGM search builders (`hybrid_search`, `sparse_search`,
  `.search_scores`) are documented across the website, the uni-db skill, and the Black Book.
- **Corrected the documented default vector index algorithm** in the skill reference: the verified
  no-type default is IVF-PQ (HNSW-SQ is a recommendation, not the default).
- **Merge-CI runner indirection**: post-merge CI job targeting now reads repository variables
  (`CI_RUNNER_HEAVY`/`_STD`/`_LIGHT`) with the current labels as fallback, so a self-hosted runner
  cutover (or rollback) is a variable flip, not a workflow change. The authoritative `gate` job
  stays pinned to GitHub-hosted infra.

---

## Upgrade notes

- `uni-pydantic` tracks `uni-db` in lock-step: both ship as **2.5.0**.
- **New public APIs**: OGM `hybrid_search()` and the `.search_scores` sidecar on all OGM search
  builders; `ef_search`/`ef` in the `uni.vector.query` options map; `UniConfig::flush_stream_timeout`
  (default 60 s, env `UNI_FLUSH_STREAM_TIMEOUT`); metric `uni_flush_stream_timeouts_total`.
- **Behavior changes to re-validate against**:
  - OGM `vector_search`/`sparse_search` `.all()` now returns hydrated models (it previously
    returned `[]` silently); property names passed to the OGM search builders are format-validated
    and invalid names now raise.
  - `collect(DISTINCT n)` over entities now actually dedups (by entity identity), and
    `count(DISTINCT n)` dedups on identity rather than the full struct.
  - Under async flush, a saturated flush pipeline defers (skip-and-retry) instead of blocking the
    committing thread on an inline flush; a stalled flush stream now surfaces as a bounded, logged
    failure and a metric increment instead of an indefinite hang.
  - Hybrid/multi-vector embedding aliases that previously failed to reopen (#130) now validate by
    capability; conversely, text auto-embed columns bound to image/audio/multimodal tasks are
    rejected at open instead of failing later.
  - Declared `VECTOR(dim)` dimensions are enforced (#137): wrong-length writes raise a `TypeError`
    (previously silently nulled at flush), mismatched query vectors to `uni.vector.query` error
    instead of returning 0 rows, and re-declaring an existing property with a different
    type/dimension raises a schema conflict error (Python: `UniSchemaError`).
  - Variable-length / QPP label-scoped traversals now filter against the vertex's persisted labels
    (#141): `MATCH (a)-[:R*1..n]->(m:Label)` and QPP intermediate-label constraints reject
    flushed endpoints that lack the required label, where they were previously over-admitted. Label
    resolution also no longer returns the stale labels of a deleted-but-unflushed vertex (#140).
    `labels(m)` / `hasLabel(m, …)` over flushed or forked traversal targets now return the real
    label set instead of an empty one.
- No breaking changes to existing Cypher or Locy surfaces; queries that relied on over-wide `WITH`
  projections (#134) keep their results — returned-whole entities and paths are deliberately kept
  wide.

## Closed issues

#114, #129, #130, #131, #132, #133 (closed as misattributed; superseded by positive BGE-M3
coverage), #134, #135, #137, #140, #141.
