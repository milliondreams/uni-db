# uni-db Plugin Framework — Gap-Closure Phase Plan (2026-07-07)

**Companion to:** `docs/proposals/plugin_framework_gaps_2026-07-07.md` (the audit + demand calibration).
**Scope:** a sequenced, demand-calibrated implementation plan to close the *genuine* gaps and align the advertised plugin API with what the engine can actually honor.
**Not a completeness checklist.** Priorities follow §8c demand, not §2 completeness. Some of the highest-value work is *subtractive* (delete traits that shouldn't exist) and much of the rest is *"ship as built-in/config,"* not *"add a plugin trait."*

Effort scale: **S** < 1 wk · **M** 1–2 wk · **L** 2–4 wk · **XL** > 4 wk (one engineer-equiv).
Risk: **Lo / Med / Hi** (blast radius on correctness/durability/planner).

---

## Guiding principles (learned from the audit)

1. **No registrable-but-dead traits.** A trait + registrar with no runtime dispatch advertises a contract you can't honor. Either wire it or delete it.
2. **Built-in/config before plugin trait.** Most real needs (BM25 params, distance metrics, fusion, constraints) are config knobs / enum members / first-party features — not extension points. Only open a *plugin* surface where there is third-party precedent.
3. **Earn every open surface with precedent.** If no comparable engine (Neo4j/Postgres/DuckDB/Lucene/LanceDB/Datomic/XTDB) opens X to third parties, default to closed and document why.
4. **Embedded model changes the math.** The host app owns the process, file, and data — so wire-protocol connectors, network RBAC depth, result formats, and guest-owned durable storage are low-value here.
5. **Honesty first.** Ship truth-in-advertising (Phase 0) before new capability — it's the cheapest trust win and prevents the "looks done" trap that produced this audit.

---

## Phase overview & sequencing

| Phase | Theme | Gates | Effort | Parallelizable? |
|---|---|---|---|---|
| **P0** | Honesty & subtraction (truth-in-advertising) | none | ~M | yes — independent fixes |
| **P1** | `GraphView` — the flagship must-open | none | **L–XL** | critical path; blocks P5 |
| **P2** | FTS analyzer config — the 2nd must-open | none | M | independent |
| **P3** | Cheap should-opens (config / enum / built-in) | none | M–L | independent, high-parallel |
| **P4** | Genuine plugin should-opens | P0 | L | 4.1 blocks P6 governance |
| **P5** | Pregel (advanced graph tier) | **P1** | L | after GraphView |
| **P6** | Framework quality & breadth (gated/niche) | P1, P4.1 | L–XL | mixed |
| **Appx** | Intentionally-closed register | — | S (doc) | — |

**Critical path:** P1 (`GraphView`) → P5 (Pregel) and P1 → P6.3 (guest algorithms). Everything else can proceed in parallel. Recommended start: **P0 + P1 + P2 concurrently** (different subsystems, no shared files).

**Rough total:** ~4–6 months of focused single-track effort; ~2.5–3.5 months with 2–3 parallel workstreams.

---

## Phase 0 — Honesty & Subtraction

**Goal:** make the advertised plugin API match reality. No new capability; pure trust. This is the cheapest, highest-confidence phase and should land first (or alongside P1/P2).

| ID | Task | Touchpoints | Acceptance | Effort | Risk |
|---|---|---|---|---|---|
| 0.1 | `declareTrigger` must register a real `TriggerPlugin` or reject with a clear error (today it installs a `ProcedurePlugin` that never fires) | `uni-plugin-custom/src/lib.rs:299`; `plugin-host/src/synthetic_procedure.rs:114` | A declared trigger fires on the mutation path in a test, OR `declareTrigger` returns an explicit "not supported" error; no silent no-op | M | Med |
| 0.2 | Custom-namespace Locy aggregates must resolve (resolver only queries `builtin` ns today) | `df_graph/locy_fold.rs:80` | A plugin-namespaced `LocyAggregate` dispatches end-to-end in the fixpoint loop (test) | S | Lo |
| 0.3 | `FireMode::EventualConsistency` — implement a real batched queue or remove the variant (today aliases Async) | `plugin-host/src/triggers.rs:666` | Either a batched-delivery test passes, or the enum variant is gone | S–M | Lo |
| 0.4 | Route CRDT merges through the registry on compaction & L0 (bypassed today → custom CRDTs silently ignored there) | `store/src/storage/compaction.rs:297`, `runtime/l0.rs:405` | A custom `CrdtKindProvider` merge is honored on a compaction + L0-flush path (test) | M | Med |
| 0.5 | Honor `TokenizerConfig::Custom` or drop the arm (config silently dropped before index build) | `common/src/core/schema.rs:1317`; `store/.../index_manager.rs:630` | Custom tokenizer either takes effect (see P2) or is rejected; no silent default | S | Lo |
| 0.6 | Stale-comment sweep: correct docs that claim unwired-what-works and vice-versa | `plugin-custom/src/lib.rs:40`; `plugin-host/src/cdc_runtime.rs:29`; `triggers.rs:37` | Comments match code; grep for the known stale strings returns none | S | Lo |
| 0.7 | Guest loaders self-verify signatures (or explicitly document the single-entry trust model) | 4 loaders; `verify.rs`; `api/mod.rs:3900` | Each loader verifies, or a SECURITY.md states the `uni`-API-only trust boundary | S–M | Med |
| 0.8 | **Subtract** the registrable-but-legitimately-closed traits: delete/feature-gate `OperatorProvider`, the plugin durable `StorageBackend`, wire `Connector`; gate `PregelProgramProvider` as `experimental` until P5; stop advertising pattern-operator/semiring extensibility | `traits/operator.rs`, `traits/storage.rs`, `traits/connector.rs`, `traits/algorithm.rs:151`; `registrar.rs` | Removed traits no longer appear in the registrar/`Capability`/docs; `SurfaceKind` count drops to the wired set | M | Med |

**DoD:** every §6 honesty hazard is closed; the advertised surface (`Capability` enum + registrar methods + docs) contains only surfaces that are wired or explicitly experimental.

---

## Phase 1 — `GraphView` (flagship must-open)

**Goal:** give a third-party `AlgorithmProvider` a stable, read-only way to read graph topology, and actually invoke it. This is the single highest-value item — it unblocks *every* topology-touching algorithm and resolves the reachability thread "through the front door." Precedent: Neo4j GDS, TigerGraph, Oracle PGX, Souffle all open this.

| ID | Task | Touchpoints | Acceptance | Effort | Risk |
|---|---|---|---|---|---|
| 1.1 | Design & implement the `GraphView` trait: read-only `vertex_count()`, `out_neighbors(vid,dir)->impl Iterator`, `in_neighbors`, `degree`, optional `edge_weight`, `slot↔vid` mapping. Source from existing CSR internals | new in `uni-algo`; back by `algo/projection.rs:39` (`GraphProjection`) | Trait compiles; a `GraphProjection` yields a `GraphView` with zero-copy neighbor iteration; documented stability contract | L | Med |
| 1.2 | Thread `GraphView` into `AlgorithmContext`; host adapter constructs it from the snapshot and passes it (replaces the `as_any()` downcast hack) | `traits/algorithm.rs:31,73`; `plugin-builtin/src/algorithms/bridge.rs` | `AlgorithmContext` carries a `&dyn GraphView`; no downcast to concrete host types needed | M | Med |
| 1.3 | Wire `AlgorithmProvider::run` into CALL dispatch (today CALL uses the legacy `AlgorithmProcedureAdapter` over the static registry) | `procedures_plugin/algo.rs:587`; `df_graph/procedure_call.rs`; `executor/procedure.rs` | A registered `AlgorithmProvider` is invoked by `CALL uni.<ns>.<algo>(...)`; MVCC snapshot correctness preserved | L | Hi |
| 1.4 | Dogfood: port 2 built-in algorithms (e.g. `wcc`, `bfs_levels`) to `AlgorithmProvider` + `GraphView`; keep legacy path for the rest during transition | `uni-algo/src/algo/algorithms/` | Ported algos produce identical results via the new path (differential test vs legacy) | M | Med |
| 1.5 | Reference third-party example: `uni.custom.reachable(startId, ['REL'], maxDepth)` implemented as an out-of-tree `AlgorithmProvider` doing BFS over `GraphView` | `examples/` | Example crate builds outside the workspace tree and runs against a real DB; **this closes the original thread** | M | Lo |
| 1.6 | Conformance probe: an out-of-tree `AlgorithmProvider` that actually executes (not just registers) | `uni-plugin-conformance/src/lib.rs` | Conformance suite runs a provider's `.run()` and checks output | S | Lo |

**DoD:** a third party can write a topology-reading algorithm (reachability, custom centrality, Steiner variant) as an `AlgorithmProvider`, register it, and have `CALL` invoke it — with no dependency on internal host types.

**Design note:** deliver `GraphView` as the *coherent host-access layer*, not a reachability shim. Scope it read-only in v1 (no mutation, no cross-query state) to keep the correctness surface small; that covers the entire algorithm-plugin use case.

---

## Phase 2 — FTS Analyzer Config (2nd must-open)

**Goal:** honor tokenizer/analyzer/stemmer/stop-word configuration so CJK & multilingual FTS work. Config surface, **not** an arbitrary-code plugin. Precedent: Lucene/ES/Postgres/Tantivy.

| ID | Task | Touchpoints | Acceptance | Effort | Risk |
|---|---|---|---|---|---|
| 2.1 | Thread `config.tokenizer` end-to-end into the FTS index build (dropped today) | `store/.../index_manager.rs:630`; `backend/lance.rs:1038`, `lance_branch.rs:437` | The configured analyzer reaches the Lance/Tantivy `FtsIndexBuilder`; verified by index behavior | M | Med |
| 2.2 | Expose analyzer selection in DDL/schema (`CREATE FULLTEXT INDEX ... WITH { analyzer, stemmer, stopwords, language }`) | `uni-cypher` grammar + DDL executor; `common/src/core/schema.rs:1311` | DDL round-trips analyzer config into the index | M | Med |
| 2.3 | Language analyzers incl. CJK (kuromoji/nori/ICU) via the underlying tokenizer library | `backend/lance.rs` FTS path | Japanese/Chinese text tokenizes correctly (recall test) | M | Med |
| 2.4 | Tests: CJK segmentation, English stemming, stop-word removal, per-language recall | `crates/uni*/tests` | Green recall tests for each analyzer mode | S | Lo |

**DoD:** a user indexing non-English/CJK text selects an analyzer via DDL and gets correct tokenization; `TokenizerConfig::Custom` (0.5) is honored or removed.

---

## Phase 3 — Cheap Should-Opens (config / enum / built-in)

**Goal:** high-value, low-risk relevance & modeling wins that are *not* plugin traits. Highly parallelizable.

| ID | Task | Touchpoints | Acceptance | Effort | Risk |
|---|---|---|---|---|---|
| 3.1 | Expose BM25 `k1`/`b` params (only `fts_k` post-squash exists today) | `query-functions/src/similar_to.rs:87`; `backend/lance.rs:1046` | Per-field/index `k1`,`b` settable; scoring changes observably | S | Lo |
| 3.2 | Grow the vector-metric enum: add L1/Manhattan + Hamming/Jaccard (binary) | `common/src/core/schema.rs:1267`; `backend/types.rs:137`; `backend/lance.rs:749` | Binary/L1 metrics selectable and correct; still a closed enum (no plugin) | M | Med |
| 3.3 | Add a fusion strategy: DBSF / relative-score alongside RRF/weighted | `query-functions/src/fusion.rs:17`, `similar_to.rs:67`; `query/planner.rs:1791` | New fusion selectable by name; ranking differs from RRF as expected | M | Lo |
| 3.4 | Durable-backend selection: wire a config knob into the **existing** `new_with_backend` seam (internal trait is live; Lance hardwired at top) | `store/src/storage/manager.rs:276,~383`; `api/mod.rs:3296` | An alternate `uni_store::StorageBackend` impl is selectable via config without forking `uni`; Lance remains default | M | Hi |
| 3.5 | APOC-style config-driven path expander as a **built-in** `CALL uni.algo.expandConfig(...)`: BFS/DFS + label/rel-type/uniqueness/min-max-level/predicate filters | `uni-algo/src/algo/` (reuse `bfs_levels`, `all_simple_paths`) | Config-driven expansion works; absorbs the "custom reachability" demand at the Cypher level (complements 1.5) | M | Med |
| 3.6 | First-party **geo type** (`Point`/`Polygon`) + distance/within predicates + spatial index | `DataType` (`schema.rs`), value codec, index_manager | Geo values store/index/query with ordering; the top domain-modeling ask, delivered first-party not as a generic type registry | L | Hi |

**DoD:** relevance tuning (k1/b), binary-vector metrics, an alt fusion, backend selection, config-driven traversal, and a geo type all land — none as a new plugin trait.

---

## Phase 4 — Genuine Plugin Should-Opens

**Goal:** the surfaces where a *plugin* (not built-in) is genuinely warranted and precedented. Gated on P0 (clean surface).

| ID | Task | Touchpoints | Acceptance | Effort | Risk |
|---|---|---|---|---|---|
| 4.1 | Deepen the authz `Resource` from raw Cypher string to a structured model (label / rel-type / property / operation) | `session.rs:1845`; `traits/connector.rs:112,135` | A policy can allow/deny by label & property, not by regexing a query string; multi-tenant test | L | Med |
| 4.2 | Storage read/attach remote object store (read-only foreign tables) — likely via the already-wired catalog/replacement-scan surface, not the dead `StorageBackend` plugin | `query/planner.rs:2713` (replacement scan); `traits/catalog.rs` | `SELECT`-style read of external Parquet/Lance on S3 without import; durable-write stays closed | L | Med |
| 4.3 | Locy *generator* predicates (bind fresh variables), extending `LocyPredicate` beyond `BooleanArray`, with a safety/range-restriction contract | `traits/locy.rs:275`; `df_graph/locy_fixpoint.rs` | A generator predicate binds new bindings soundly under stratification; termination preserved | L | Hi |
| 4.4 | Wire the dead `WindowPluginFn` into window dispatch (planner hardcodes built-ins today) | `df_planner.rs:5555`; `registry.rs:652` | A registered window UDF executes in an `OVER (...)` query | M | Med |
| 4.5 | Declarative cardinality / relationship-existence constraints as **built-in DDL** (extend `ConstraintType`; arbitrary validator code stays closed) | `common/src/core/schema.rs:674`; `uni-cypher/src/ast.rs:223`; writer validation | "at most one `:PRIMARY_EMAIL`" enforced declaratively & replay-safe | L | Med |

**DoD:** authz can express row/property policy; external data is queryable read-only; generator predicates, window UDFs, and declarative constraints are usable.

---

## Phase 5 — Pregel (advanced graph tier) — **depends on P1**

**Goal:** vertex-centric programs for iterative message-passing algorithms. Downstream of `GraphView`; do not stabilize the trait before P1 lands. Precedent: Neo4j GDS Pregel API.

| ID | Task | Touchpoints | Acceptance | Effort | Risk |
|---|---|---|---|---|---|
| 5.1 | Design the real `PregelProgramProvider`: `init`/`compute`/`combine`/aggregators/`halt` (today only `signature()`+`halt()`) | `traits/algorithm.rs:151` | Complete trait able to express a vertex program | M | Med |
| 5.2 | Pregel executor in `uni-algo`: BSP supersteps, message combiners, halt voting, over `GraphView`/`GraphProjection` | `uni-algo` | Executor runs a program to convergence; parallel message passing | XL | Hi |
| 5.3 | Reference example: label-propagation / belief-propagation as a Pregel program | `examples/` | Runs on a real graph, matches expected communities | M | Lo |

**DoD:** a third party writes a custom iterative algorithm as a Pregel program and runs it via CALL.

---

## Phase 6 — Framework Quality & Breadth (gated / niche)

**Goal:** harden and widen once the core is right. Sequenced last because each is niche or gated.

| ID | Task | Touchpoints | Acceptance | Effort | Risk |
|---|---|---|---|---|---|
| 6.1 | Hot-path panic isolation: `catch_unwind` around scalar/agg/proc invoke (only triggers today) | `df_udfs_plugin.rs`, `df_udaf_plugin.rs`, `executor/procedure.rs`; cf. `triggers.rs:678` | A panicking native UDF returns a query error, doesn't crash the worker | M | Med |
| 6.2 | Plugin observability API: wire `record_invocation` + give plugins a metric/trace emit handle | `plugin/src/observability.rs:90`; `plugin-host/src/observability.rs` | Plugin invocations emit spans; a plugin can emit a custom metric | M | Lo |
| 6.3 | Guest breadth for pure-compute kinds only: Locy predicate/aggregate, algorithm-via-`GraphView`, trigger — **never** storage/index | 4 guest loaders; guest ABIs | A Rhai/WASM plugin authors one non-UDF pure-compute kind | L | Med |
| 6.4 | Distribution: CLI `list`/`remove` + OCI artifact loader (only local `.rhai` install today) | `uni-cli/src/main.rs:151` | `uni plugin list/remove` work; a plugin pulls from an OCI ref | L | Med |
| 6.5 | Conformance depth: determinism + resource-limit probes (6 structural checks today) | `plugin-conformance/src/lib.rs:220` | Suite catches a non-deterministic / over-limit plugin | M | Lo |
| 6.6 | Governance: property masking / row-level filter — **gated behind 4.1** | authz resource model | A masking policy redacts a property for a principal | L | Med |
| 6.7 | Python native `add_plugin` story (generic-over-Rust-type can't cross PyO3) — decide: register a compiled cdylib, or document guest-only | `bindings/uni-db/src/sync_api.rs` | Either a Python path to load a native plugin, or a documented limitation | M | Med |
| 6.8 | ML/Xervo: integrate model/embedder/reranker providers into the unified `Capability`/manifest model, OR document it as an intentionally separate subsystem | `uni-xervo` boundary; `api/mod.rs:3036,3049` | A model provider is registrable under the capability model, or a clear "separate subsystem" doc exists | L | Med |

**DoD:** the framework is fault-isolated, observable, distributable, and either unifies or cleanly documents the ML subsystem.

---

## Appendix — Intentionally-closed register (do NOT build)

Each was pressure-tested against real-world precedent and the embedded model and found legitimately closed. Documenting them is part of an honest "complete API" claim.

| Item | Why closed | Precedent |
|---|---|---|
| Custom path-expander **into the planner** | Breaks plan costing/uniqueness; use Locy recursion / `shortestPath` / 3.5 built-in | Nobody opens the var-length planner |
| Custom semirings | Silent-wrong provenance; algebraic-law burden | ProvSQL keeps new semirings internal; research-only |
| Custom pattern / relationship operators | Un-costable, un-optimizable | No precedent |
| Custom CRDT kinds | Silent permanent replica divergence | Yjs/Automerge keep it closed |
| Temporal / BTIC extensibility | Allen's 13 relations are mathematically closed; codec must stay fixed | XTDB ships bitemporality non-extensible |
| Custom Cypher grammar / AST rewrite | Parser stability underpins planning/caching/security | No production DB opens its parser at runtime |
| Custom distance-metric **plugin** | Co-designed with index pruning/quantization; footgun | pgvector/Qdrant/Milvus/Weaviate all fixed enums (→ 3.2 grows the enum instead) |
| Custom quantization | Deepest index internals; no stable ABI | No vector DB opens it |
| Custom FTS **scorer** plugin | Near-zero adoption where it exists | ES has it, ~unused (→ 3.1 exposes k1/b instead) |
| Custom physical operators | Fork-scale only | Postgres CustomScan = Citus/Timescale |
| Wire-protocol connectors | Contradicts embedded model | Run a server if you want a server |
| Query-result serialization / export | Client concern over an Arrow batch | — |
| Schema-migration hooks | Belongs in external tooling | Flyway/Alembic/Liquibase |
| Guest-owned durable storage / index | Durability + security; can't cross the sandbox | SQLite vtab/VFS are in-process C, not sandboxed guests |

---

## Milestone rollup

- **M-Trust (P0):** advertised API == reality; honesty hazards closed. *~M.*
- **M-Graph (P1):** third-party topology algorithms possible; reachability closed. *~L–XL.* **Flagship.**
- **M-Search (P2+3.1–3.3):** analyzers + BM25 params + metrics + fusion. *~L.*
- **M-Model (P3.4–3.6 + P4.5):** backend selection, geo type, config traversal, declarative constraints. *~L.*
- **M-Extend (P4 + P5):** authz depth, external read, generator predicates, windows, Pregel. *~XL.*
- **M-Harden (P6):** isolation, observability, distribution, guest breadth, ML integration. *~XL.*

**Recommended first cut (one quarter, 2–3 parallel tracks):** P0 (trust) + P1 (GraphView) + P2/P3.1–3.3 (search). That delivers both must-opens, the honesty fixes, and the highest-demand relevance wins — i.e. the parts with universal precedent — before touching the niche tail.
