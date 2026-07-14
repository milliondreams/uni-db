# uni-db 3.0.0

**Release focus: a plugin-framework honesty pass** — the advertised plugin API is subtracted down
to what the engine actually honors (four dead traits removed, one ABI changed) — landed alongside
the release's headline capability, **GraphCompute: guest-authorable graph algorithms** in Rhai,
Python, WASM, and Extism. Rounding out the release: **real firing triggers**, **configurable
full-text tokenizers/analyzers**, a **`BINARY_VECTOR` type** with exact Hamming/Jaccard, new
**hybrid-search fusion methods**, **relationship and composite-key UNIQUE constraints**, **Locy
generator predicates**, and a large multi-wave **correctness-scan** hardening pass.

This is a **major** release covering everything since **2.5.0**: **139 commits**, version bumped
across the Rust workspace and the Python packages (`uni-db`, `uni-pydantic`) to **3.0.0**. It is
numbered 3.0.0 — not 3.5.0, despite the internal development history bumping through 3.1–3.5 while
features landed — because it carries **breaking removals of plugin surfaces**; all breaking changes
are consolidated under this single major tag. If your code does not implement custom `uni-plugin`
traits, it should recompile unchanged. See **Migration guide** and **Upgrade notes** below.

---

## ⚠️ Breaking changes

This release makes the plugin API match what the engine dispatches. Four registrable-but-never-invoked
traits are removed, and the trigger ABI changes to enable real firing.

### Removed plugin surfaces

- **`PregelProgramProvider`** (`traits::algorithm`) — a stub with no executor; never invoked. Its
  support types (`PregelSignature`, `AggregationMode`, `ComputeOutcome`, `PregelStats`) and the
  `.pregel(..)` registrar are removed too. Author against **`AlgorithmProvider`** (retained), or use
  the new **GraphCompute** engine for guest-authored graph algorithms.
- **`OperatorProvider`** (`traits::operator`) — custom physical operators were never inserted into
  the planner. Removed with its `.operator(..)` registrar. Use the retained **`OptimizerRuleProvider`**
  (`rule()` / `physical_rule()`) for planner extension.
- **Plugin `StorageBackend`** (`traits::storage`) — the scheme-keyed durable backend was never
  consulted by the storage engine. Removed with `.storage_backend(..)`. The per-label **`Storage`**
  surface (`label_storage()` / `lookup_label_storage`) is retained. *(The internal
  `uni_store::backend::StorageBackend` is a different, unaffected trait.)*
- **`Connector`** (`traits::connector`) — a lifecycle-only stub with no query-time data path.
  Removed, along with `Capability::Connector`, `SurfaceKind::Connector`, and the public
  **`Uni::start_connector`** / **`Uni::stop_connector`** / `ConnectorLifecycle` API. For external
  data, use **`CatalogProvider`** / **`ReplacementScanProvider`**. `AuthProvider` / `AuthzPolicy`
  are retained.

The shared capabilities `Capability::{Operator, Storage, Algorithm}` are **retained** — they back the
delivered `optimizer_rule` / `label_storage` / `algorithm` surfaces.

### Changed ABI

- **`TriggerContext`** now carries an owned private `Option<Arc<dyn ProcedureHost>>` with `with_host()`
  / `host()` accessors — this is what lets a declared trigger execute its Cypher action body.
  `TriggerContext::new` is unchanged (host defaults to `None`), so existing `TriggerPlugin`
  implementors keep compiling, but the struct's shape/size changed: **recompile custom plugins
  against 3.0.0**; do not mix ABIs across the boundary.
- **`FireMode::EventualConsistency`** now batches for real instead of aliasing `Async`. Events
  coalesce per-trigger and drain on an interval/size threshold (`UniConfig::ec_flush_interval`,
  default 1 s; `ec_flush_threshold`, default 10,000). If you relied on immediate per-event firing,
  set `Async` explicitly or tune the thresholds.

---

## Highlights

### 🔹 GraphCompute — guest-authorable graph algorithms (Rhai · Python · WASM · Extism)

Third parties can now author graph algorithms — PageRank / Personalized PageRank, reachability/BFS,
WCC, Bellman–Ford, k-core, eigenvector, HITS/Katz, random walks, neighbourhood similarity — as
**plugins**, with no forking and no shipping Rust.

The model is **"conductor, not worker"**: the guest runs only the O(iterations) control loop while
native code does all O(V+E) work, so **only opaque handles and scalars cross the plugin boundary** —
never frontiers, neighbour lists, or property columns. Because nothing heavy is marshalled, the same
design runs uniformly across **all four loaders** (Rhai, Python, WASM Component Model, Extism); each
ships a Personalized PageRank example that matches the native provider to **1e-9**.

- **Kernel catalog** — a fixed set of coarse native kernels: frontier/expand (direction-optimized,
  mask-fused), SpMV over named semirings, map/reduce/scatter, set ops, arg-extreme / top-k, random
  walks, all-pairs / neighbourhood overlap (Jaccard, cosine, Adamic–Adar, triangle count), and
  result `emit` (including ragged walk and pairwise egress). Values are Arrow-backed; handles are
  generational, epoch-tagged, kind-checked, and session-scoped.
- **Deterministic & fail-closed** — deterministic CSR ordering and fixed-order reductions give
  bitwise-reproducible results across thread counts. A native-work budget and a per-session
  handle-memory arena are fail-closed, and non-convergence is a hard error (`GraphComputeIncomplete`
  with distinct Exhausted / IterationLimit / Timeout reasons). Guest loops are bounded per loader
  (Rhai `catch_unwind`, a Python `KeyboardInterrupt` watchdog, WASM/Extism epoch interruption).
- **Typed args & version negotiation** — signatures declare typed, defaulted arguments (arity- and
  type-checked, default-filled at call time) and negotiate host kernel-slice versions at registration.
- First-party providers `uni.algo.gcpagerank`, `uni.algo.gcwalks`, and `uni.algo.gcoverlap` dogfood
  the surface. Design: `docs/proposals/graphcompute_plugin_api_2026-07-10.md`.

### 🔹 GraphView topology API

A slot-indexed, read-only topology trait (mirroring the internal CSR `GraphProjection`) plus
`AlgorithmHost::project()`, gated on `Capability::HostQuery`. This is the **in-process** path for
authoring graph algorithms against public traits, and `AlgorithmProvider::run` is now wired into
`CALL` dispatch on both the planner and simple-executor paths (miss-only, so built-in `uni.algo.*`
never regress). First-party `uni.algo.reachability`, `uni.algo.pagerank`, `uni.algo.sssp`, and
`uni.path.expand` (an APOC-style bounded path expander) are authored purely against this surface.

### 🔹 Real triggers — `declareTrigger` that actually fires

`uni.plugin.declareTrigger` now installs a real firing `TriggerPlugin` (AfterCommit / Async) instead
of a callable procedure that never fired. Declared triggers support an event filter
(`CREATE|UPDATE|DELETE [ON :Label | -[:Type]-] [WHEN pred] [ASYNC]`), bind event columns
(`$vid` / `$label` / `$event_kind`), run the declared Cypher action body, and **replay across
restart**. `FireMode::EventualConsistency` now provides a real batched queue that coalesces
per-trigger events and drains on interval/size thresholds with lossless back-pressure.

### 🔹 Full-text tokenizer / analyzer configuration

Full-text indexes now honor tokenizer/analyzer/stemmer/stop-word configuration end-to-end:

```cypher
CREATE FULLTEXT INDEX ... OPTIONS {
  analyzer, language, stemmer, stopwords,
  ascii_folding, lower_case, max_token_length, ngram_min, ngram_max
}
```

Previously every FTS index used Lance's default "simple" tokenizer regardless of configuration,
making CJK / multilingual text effectively unindexable. 18-language stemming and stop-words plus
ngram tokenization are now supported (CJK requires dictionary files under `LANCE_LANGUAGE_MODEL_HOME`).

### 🔹 `BINARY_VECTOR(n)` type with exact Hamming & Jaccard

Bit-packed binary embeddings stored at full fidelity (`n` = byte count, so `BINARY_VECTOR(4)` = 32
bits) — ideal for hash/fingerprint embeddings and set-overlap similarity.

```cypher
CREATE LABEL Doc (bits BINARY_VECTOR(4));
CREATE (:Doc {bits: [0, 255, 165, 60]});
RETURN VECTOR_DISTANCE(a.bits, b.bits, 'hamming');   // or 'jaccard'
```

Brute-force exact only (binary metrics have no ANN index; attempting to build one is rejected with
guidance). Additive, no breaking change. Relatedly, **`VECTOR_DISTANCE` is now a real DataFusion
scalar UDF**, so it works in DataFusion-planned `RETURN` clauses for all metrics, not just the
interpreter path.

### 🔹 Hybrid search: two new fusion methods

`uni.search` gains **DBSF** (distribution-based, z-score normalized) and **relative-score** (min-max
+ weighted) fusion, joining the existing `rrf` and `weighted` methods for fusing dense + FTS/BM25 +
learned-sparse arms. Select via `options.method`; tune with `rrf_k` (default 60), `weights`, `alpha`,
`over_fetch`. Also added: **DBSF / relative-score** exposure through the OGM, and **L1/Manhattan** and
geo `Point` compute paths.

### 🔹 Relationship & composite-key UNIQUE constraints

- **Relationship (edge-type) UNIQUE / NODE KEY** constraints with full flush-safe, false-positive-free
  write-horizon enforcement and a commit-time SSI guard.
- **Composite node-key constraints** (`(a, b) IS UNIQUE` / `IS NODE KEY`) enforced end-to-end.

### 🔹 Locy: fixed-arity generator predicates

Plugin-registered **table-valued** predicates that bind new variables and explode one source row into
many — e.g. `myplugin.range(n.k) -> (i)` yields `i ∈ {0,1,2}` for `k = 3`. The `->` arrow
distinguishes a generator from a filter predicate; bindings flow through `YIELD` like `ALONG`, and
multiple generators compose per body. (Deferred: variable arity, recursion-through-generator, and
feeding `FOLD`.) This work also fixed a latent bug where the plugin registry was never task-scoped on
the Locy path, so any `add_plugin`-registered custom predicate was previously unreachable.
**Plugin-namespaced `FOLD` aggregates** (dotted `ns.NAME`) now resolve too.

### 🔹 Window-function & predicate plugins

`WindowPluginFn` dispatched through `OVER (PARTITION BY ...)`, and Locy predicate plugins, are now
wired live end-to-end.

---

## Correctness & reliability

Roughly 90 fixes landed across the multi-wave **correctness-scan** (waves R1–R18 / L1–L11), grouped
here by user-visible symptom:

- **Process safety (Critical).** A background sweep tick could broadcast shutdown to a *live*
  database, and the PyArrow FFI import path could double-free capsule structs. Both fixed —
  long-running and Python-embedded databases no longer risk spurious teardown or memory corruption.
- **Errors propagate instead of being swallowed.** Transient load failures (named snapshots, fork
  registry, `table_exists` across scan/vector/FTS), snapshot query errors, and auto-embed failures in
  hybrid search were silently dropped, yielding wrong or empty results; they now surface. The CLI
  exits non-zero on a failed one-shot query, and a conflicting edge-type re-declaration is rejected.
- **Tombstone & compaction correctness.** Deleted edges could resurrect after compaction, tombstoned
  vertices could reappear in vector/FTS results, and L0 tombstones weren't always treated as globally
  dead. Compaction now always replaces L2 adjacency, version-gates the tombstone branch, and honors
  label-overwrite markers on replay.
- **MVCC version-ranking in batch reads.** Batched property/label reads now version-rank rows to
  match single-row MVCC, so a batch scan and a point read of the same entity agree.
- **UNIQUE across the full write horizon.** Constraints consult the entire write path — including the
  main writer's unflushed L0 and the bulk-ingest channel — using a lossless type-tagged key rather
  than a lossy `Display` join, closing duplicate-admission windows.
- **NULL / three-valued logic.** `count(entity)`, vid-lookup join keys, `=~` / string ops, simple
  `CASE`, and `AND`/`OR` with a NULL operand now follow proper 3VL/NULL propagation.
- **OPTIONAL MATCH null rows.** Reworked null-row plumbing (including cross-batch late-pass NULL
  cancellation) so unmatched optional patterns yield correct NULL-padded rows.
- **DISTINCT / UNION / CTE dedup** now uses a canonical value key instead of `HashMap`/`Debug` string
  identity (which mis-merged or mis-split rows); NaN is normalized in vector/sparse value equality.
- **Exact & overflow-safe arithmetic.** Row-based `SUM`, UDF arithmetic, APOC `toString`/`round`,
  duration math, and Cypher sort keys use checked/exact i64 paths; non-finite / out-of-range floats
  coerce to NULL rather than wrapping or panicking.
- **Temporal fidelity.** Missing temporal properties store NULL (not epoch-0), hour-granularity
  datetime literals parse, temporal ordering is corrected, and COPY decodes all Arrow types.
- **Bytes / UTF-8 safety.** Hex decode and string length respect char boundaries.
- **Query-planner semantics.** Corrected DISTINCT/LIMIT interaction, per-window sort, self-loops,
  Apply writes, shortestPath, QPP anchoring, predicate-consumption gating, `WHERE` after
  `CALL … YIELD` (previously silently dropped), and REMOVE-label visibility on flushed vertices.
- **Probabilistic / Locy logic.** Fixed ABDUCE target attribution, MNOR mixed-support, exact-WMC
  column resolution, Mprod log-switch double-counting, isotonic-PAV tied-prediction pooling, and
  compile-context threading through body/module/visitor paths.
- **Plugin / trigger / scheduler lifecycle** now persists correctly; per-plugin records merge (not
  overwrite); intra-batch duplicates are rejected; fire-time-less cron entries are skipped; scheduled
  jobs upsert by id; Extism host-fn side effects are denied during manifest bootstrap.
- **Authorization & config.** Queries are authorized at `query_inner` (closing a bypass), the plugin
  registry attaches on the config path, and `rename_property` validates the new name.

Scan catalog and verification: `docs/correctness_scan_2026-07-05.md` (+ triage / verification docs).

## Storage, concurrency & fork hardening

- **Nested-fork snapshot isolation.** Nested-fork creation captures the parent-branch tip under
  `flush_lock`, closing a race where a fork could observe an inconsistent parent snapshot.
- **Flush vs. compaction.** Compaction and flush are serialized, id reservations roll back on persist
  failure, and an Incoming-shadow-key bug is fixed — preventing corruption under concurrent flush/compact.
- **Durable point vs. timeout.** `commit_timeout` now bounds only `flush_lock` acquisition, never the
  durable commit point, so a slow flush can't be reported as a lost/timed-out durable write.
- **Fork index & identity.** Coexisting fork-local index kinds share one column; `ext_id` promotion
  is idempotent via content re-verification; promote/diff propagates ext-id errors, fixes content-UID
  dedup, and honors delete-conflict policy.
- **CRDT convergence.** Custom `CrdtKindProvider` merges now route through the plugin registry on both
  durable paths — compaction and L0 flush — where they were previously bypassed (bit-for-bit
  preserved via native fallback when no provider is registered). ORSet v1→v2 upgrade uses a unique
  per-decode actor and drops the LWWMap `-1` sentinel.

## Python / OGM

- **Datetime correctness.** Naive datetimes no longer shift by the machine's UTC offset (ingestion
  previously used local-tz `.timestamp()` against a wall-clock-as-if-UTC core); datetime nanoseconds
  are now exact (integer math, not `f64`); an aware `datetime.time` no longer raises.
- **Type & role validation.** Unrecognized Python types raise `TypeError` naming the offending type
  instead of silently coercing to `Null`; chat-message roles are validated against
  `user`/`assistant`/`system` instead of silently mapping unknown roles to `user`.
- **GIL / FFI safety (Critical).** Fixed an Arrow FFI double-free / use-after-free (SIGABRT) on the
  first `write_batch` of a real pyarrow `RecordBatch`, and resolved three PyO3 GIL / `std::Mutex` ABBA
  deadlocks that could freeze the interpreter; added an interruptible cursor `close()`.
- **uni-pydantic OGM.** Fixed a silent value-conversion outage (a `TYPE_CHECKING`-only import broke
  `get_type_hints()`, so `Vector`/`Btic` field values reached Rust unconverted and were rejected).
  `uni-pydantic` tracks `uni-db` in lock-step at 3.0.0.

## Sparse & multivector (correctness follow-ups)

The full **SPLADE** learned-sparse stack (`SPARSE_VECTOR(N)` columns, inverted-index sparse ANN,
8-bit quantization, N-way fusion, text auto-embed) and the **ColBERT** multivector stack (MaxSim
late-interaction over `LIST<VECTOR(dim)>`, MUVERA first-stage ANN) both shipped in **2.5.0** and are
unchanged in feature scope. New in 3.0.0 are correctness follow-ups: negative/oversized sparse-map
indices now **error** instead of silently wrapping, and NaN-containing sparse/dense values compare
reflexively.

## Dependencies

**No third-party dependency versions changed in this range** — the `Cargo.toml` / `Cargo.lock` diff
is only the internal `uni-*` crates bumping `2.5.0 → 3.0.0`. This is a feature/API release, not a
dependency-upgrade one. For the record, the pins carried into 3.0.0: `lance 7.0.0`, `lancedb =0.30.0`,
`arrow* 58.3.0`, `datafusion 53.1.0`, `pyo3 0.29`, `chrono 0.4.45`.

## Packaging & platforms

- **Six wheel variants** (provider set × acceleration): `uni-db` (default, all providers, CPU),
  `uni-db-onnx` (slim: ONNX-local + remote APIs, CPU), `uni-db-cuda`, `uni-db-metal`,
  `uni-db-onnx-cuda`, `uni-db-onnx-metal`. A CI feature guard
  (`scripts/ci/check_wheel_variant_features.py`) fails the build unless every variant enables both
  `rhai-plugins` and `pyo3-plugins` (which the plugin loaders call unconditionally) — drift invisible
  to nextest that only surfaces at wheel-build time.
- **Platforms.** Python ≥ 3.10 as abi3-py310 stable-ABI (one wheel covers 3.10+); Linux x86_64 +
  aarch64 (manylinux_2_28), macOS Apple Silicon, Windows x86_64 (MSVC); CUDA on Linux x86_64.
- **crates.io publish ordering** fixed so `uni-algo`/`uni-store`/`uni-crdt`/`builtin` publish before
  the loader crates that depend on them.

---

## Migration guide — upgrading from 2.5.0 to 3.0.0

3.0.0 is a **plugin-framework honesty pass**. If your code does not implement custom `uni-plugin`
traits, it should recompile unchanged. Source of truth: `crates/uni-plugin/CHANGELOG.md`.

1. **Grep your plugin code** for removed surfaces and migrate each per the Breaking-changes section:
   `PregelProgramProvider`, `OperatorProvider`, the plugin `StorageBackend` trait, `Connector`,
   `start_connector`, `stop_connector`, `ConnectorLifecycle`, `Capability::Connector`,
   `SurfaceKind::Connector`, and the `.pregel(..)`/`.operator(..)`/`.storage_backend(..)`/`.connector(..)`
   registrars.
   - Graph algorithms → `AlgorithmProvider` (in-process, via GraphView) or **GraphCompute** (guest).
   - Planner extension → `OptimizerRuleProvider`.
   - Durable per-label storage → the `Storage` surface (`label_storage()` / `lookup_label_storage`).
   - External data → `CatalogProvider` / `ReplacementScanProvider`.
2. **Recompile** all custom `TriggerPlugin` / `uni-plugin` consumers against 3.0.0 — the
   `TriggerContext` struct changed shape (ABI). Do not mix ABIs across the boundary.
3. **Audit `FireMode::EventualConsistency` triggers** — set `ec_flush_interval` / `ec_flush_threshold`,
   or switch to `Async` if you relied on immediate per-event firing.
4. **Python: no API breaks, but rebuild the extension.** Datetime values are now correct (naive
   datetimes previously shifted by the machine's UTC offset), and previously-silent `Null` coercions
   of unknown types now raise `TypeError`.

*(Relevant only if jumping from a pre-1.9.0 signed-manifest deployment — not new in 3.0.0: manifest
signing now covers the whole manifest, so pre-1.9.0 signatures must be re-signed.)*

## Upgrade notes (behavior changes to re-validate)

- **`declareTrigger` now actually fires** — triggers you declared under 2.5.0 (which silently never
  ran) will now execute their action bodies on matching events.
- **Custom CRDT merges** now route through the registry on the compaction and L0 durable paths
  (previously bypassed). No change unless you registered a `CrdtKindProvider`.
- **FTS indexes honor tokenizer/analyzer config** — a `CREATE FULLTEXT INDEX ... OPTIONS { ... }` that
  was previously ignored now takes effect (re-create indexes to pick up the intended analyzer).
- **Hybrid search fails loud on misconfiguration** — a text-query hybrid search over a vector property
  whose label has no embedding config now **errors** instead of silently degrading to FTS-only.
- **Locy fails loud on two previously-silent cases** — an unsupported `YIELD` expression errors
  instead of returning `Null`, and a missing `FOLD` input column is a hard error instead of a
  fabricated zeros column. No syntax breakage.
- **Sparse-map bounds** — negative/oversized indices now error instead of silently wrapping.

## Closed issues

No new numbered GitHub issues closed in this range (the #131/#134/#135/#137/#138 fixes shipped in
2.5.0). The one issue referenced here — **#145** — was a Locy `FOLD` aggregate + `YIELD`-alias
regression introduced by an earlier merge; it is **fixed** in 3.0.0 (value-aggregates no longer zero
out, probabilistic aggregates no longer crash, and `WHERE` applies to `FOLD` results in both the
query and `ASSUME` paths).
