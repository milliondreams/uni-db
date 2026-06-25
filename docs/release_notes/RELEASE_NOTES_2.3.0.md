# uni-db 2.3.0

**Release focus: native late-interaction (ColBERT / MaxSim) vector search**, a parameterized
`MAP<K,V>` DDL type, and a deep fork/branch production-readiness pass.

This is a minor release covering everything since **2.2.0** (the 2.2.1–2.2.5 patch line is rolled
up here). 79 commits; version bumped across the Rust workspace and the Python packages
(`uni-db`, `uni-pydantic`) to **2.3.0**.

---

## Highlights

### 🔹 Native multi-vector (ColBERT / late-interaction) search — #96, #104

uni-db now stores and retrieves **per-token embedding matrices** end to end — the late-interaction
("ColBERT") shape — not just single dense vectors.

- **Storage type.** A `List<Vector[N]>` property holds a ragged list of token vectors per row. The
  Pydantic OGM maps `list[Vector[N]]` fields to `List(Vector)` automatically.
- **MaxSim retrieval.** Two-stage retrieval: a first-stage candidate generation followed by exact
  **MaxSim** re-ranking (sum over query tokens of the best per-document token similarity).
  - Native first-stage **index** retrieval (not just brute-force re-rank).
  - **L0-merge**: native multi-vector queries see committed-but-unflushed writes (in-process exact
    MaxSim re-rank over the L0 union), so results are correct without a flush.
  - **Fork/branch retrieval**: multi-vector queries resolve on branched tables.
- **MUVERA FDE index.** A self-contained, deterministic **Fixed-Dimensional Encoding** derives a
  single-vector surrogate (`__fde_*`) from each token matrix, enabling a fast single-vector ANN
  first stage over multi-vector data, followed by exact MaxSim. Created uniformly via
  `type:'muvera'`; the derived column is materialized at flush and backfilled at create. Hardened
  for the bulk full-rebuild path and against concurrent-create races (with failure rollback).
- **Auto-embed.**
  - Multi-vector (ColBERT) columns auto-embed per-token from a configured model alias.
  - **Single-pass hybrid auto-embed**: a dense `Vector` column and a multi-vector `List<Vector>`
    column that share the same embedding alias + source are detected as a hybrid group and filled
    from **one** model forward pass (e.g. BGE-M3 producing dense + ColBERT heads together). Opt-in
    is type-inferred — no new configuration.
- **Tooling.** A brute-force MaxSim ground-truth oracle and cross-surface tests; recall@k
  benchmark harnesses for the index paths.

> Requires `uni-xervo` 0.17 for the hybrid/multi-vector embedding heads.

### 🔹 Cypher DDL: parameterized container & property types — #105

- New **`MAP<K,V>`** parameterized container property type in DDL, stored as a typed Arrow
  `List<Struct{key,value}>` with a CypherValue-encoded fallback for non-uniform values.
- DDL can now **declare parameterized property types** generally (the same machinery underpins
  `List<Vector[N]>` and `MAP<K,V>`).

### 🔹 Fork / branch: production-readiness pass — #97, #99, #102, #103, #106

A broad correctness and durability hardening of forks (Lance branches), driven by an adversarial
review and a run of reported issues:

- **#97** — a fork now inherits the parent's committed-but-**unflushed** (L0) writes at creation.
- **#99 + P0/P1** — fork-scoped snapshot manager, per-fork WAL high-water-mark, deep-copied
  per-fork indexes, drop-path artifact cleanup, and telemetry isolation.
- **P2 correctness (M1/M4–M8)** — atomic fork-point capture; label-only mutation flush durability;
  branch search now honors filters; fork vector/FTS indexes auto-build; `ext_id`-keyed upsert
  **promote** with delete-promotion + conflict detection; mutate/delete of inherited vertices.
- **#102** — `SET`/`REMOVE` on an **inherited relationship** in a fork no longer wipes the
  relationship's other properties (the batch edge-property prefetch now falls back to the main-edge
  `props_json` for schemaless/overflow props).
- **#103 / #106** — nested (2-level) forks read correctly, including `vector.query` / full-text
  queries (branch scans no longer push down an unresolvable scalar-index lookup).
- **P3 hardening** — deterministic fork drop (fixes a `ForkInUse` flake), zombie-branch reclamation
  from partial fork-create, atomic cross-dataset tag, schema-name safety, fork-local-id invariants,
  and adjacency cleanup.
- **Python** — promote merge options (conflict policy, baseline promotion) and richer promote
  report counters are exposed.

### 🔹 `Bytes` data-type round-tripping — #93, #100

Closed the surface where raw `DataType::Bytes` values were mis-decoded:

- Preserved through Cypher `RETURN` / projection (#93).
- Closed the broader mis-decode surface (#100): list comprehensions over `Bytes`, `b{.*}` wildcard
  map projection, typed `List(Bytes)` / `Map(_, Bytes)` round-trips (field-level marker), and
  computed/derived projections (`coalesce`, `CASE`, list literals).

### 🔹 Locy (Datalog) engine — #94

- **#94** — property-access expressions (`x.prop`) usable in `YIELD KEY`.
- **`profile()`** — per-iteration execution profiling for Locy programs (sync and async builders).
- A **naive-Datalog reference oracle** for differential testing of the Locy engine.
- IS-ref value-column resolution fixes (in `FOLD`/`YIELD`/`WHERE` and derived-scan schema; no more
  shared-target double-scan); correct iteration reporting for non-recursive strata.
- `uniscape` gap-report fixes (REQ-1/1b/2/3/4/5b) and dotted plugin-id qualified-name resolution.

---

## Reliability & testing infrastructure

- **Model checking**: loom + shuttle models for the OCC commit core.
- **ThreadSanitizer**: a nightly lane running the real-thread SSI suites under TSan.
- **Metamorphic oracles**: query-correctness oracles (TLP / NoREC / structural) wired into PR and
  nightly CI.

## Dependencies

- `uni-xervo` upgraded to **0.17** (hybrid + multi-vector embedding heads); the requirement was
  also widened earlier in the line to `>=0.14, <1.0`.

## Documentation & CI

- Refreshed Locy semantics, skill reference, and "Black Book" accuracy; repaired and regenerated
  example notebooks (fraud / regulatory / compliance / sales / patent-FTO) so generated output
  matches source.
- Documented vector-index algorithm options (including `muvera`; `ivf_pq` default).
- Release/publish workflow fixes (PyPI-by-default, uni-pydantic publish + propagation wait,
  recovery workflow), nightly Fuzz/Soak repair, and least-privilege workflow permissions.

---

## Upgrade notes

- **Versioning**: the Rust workspace and both Python packages are now **2.3.0**; `uni-pydantic`
  tracks the `uni-db` version in lock-step.
- **Embeddings**: multi-vector / hybrid auto-embed requires `uni-xervo` 0.17. Single-pass hybrid
  auto-embed activates automatically when a `Vector` and a `List<Vector>` column share an embedding
  alias + source — no config change, but be aware both columns will be populated from one pass.
- **Forks**: behavior is now correct in cases that previously failed silently (inherited-edge
  partial SET/REMOVE, nested-fork reads, unflushed-L0 inheritance). Code relying on the old buggy
  behavior should be re-validated.
- No breaking API changes to existing single-vector search, Cypher, or Locy surfaces.

## Closed issues

#93, #94, #96 (epic), #97, #99, #100, #102, #103, #104, #105, #106, #107.
