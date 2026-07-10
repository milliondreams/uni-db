# uni-db Plugin Framework — Gaps Review (2026-07-07)

**Status:** Findings. Adversarial, code-only audit. **Superseded by §0 (2026-07-09, P0 + P1 subtractive tier), §0.1 (2026-07-10, P2 quick-wins + compute-surface opens, v3.1.0), and §0.2 (2026-07-10, P2 M-tier completion, v3.2.0). P0/P1 fully shipped; P2 fully addressed (built or resolved-with-rationale).**
**Method:** 10 parallel code-reading auditors, one per extension domain, each required to verify every claim against three things — the **trait**, its **registrar** method, and its **runtime dispatch/call-site**. Docs, memory, and prior gaps files were explicitly excluded as sources. A surface counts as "delivered" only if the engine actually invokes a registered plugin of that kind at runtime.
**Motivating question:** Can a third party build a BFS/DFS reachability function as a plugin? (Answer at time of writing: **no** — see §7. **As of 2026-07-09: yes**, through the front door via `AlgorithmProvider` + `GraphView` — see §0.)

---

## 0. Status update — 2026-07-09 re-audit (HEAD `fe64b48f5`, v3.0.0)

The original findings below were re-verified against the current tree by 6 parallel auditors using the same discipline (trait + registrar + **runtime call-site**, not just registry presence). **Every P0 item and the entire P1 subtractive tier from §9 have shipped** in a 2.5.0 → **3.0.0** breaking release. The remaining gaps are, without exception, the P2/P3/legitimately-closed items in this doc's own demand-calibrated roadmap (§8c/§9).

### What shipped (verified on-path, not just registrable)

| §9 item | Verdict | Evidence (current tree) |
|---|---|---|
| **P0.1 — `GraphView` + wire `AlgorithmProvider::run`** | ✅ **Done** | Trait `GraphView` (slot-indexed neighbors/degree/weight/vid↔slot) `uni-plugin/src/traits/algorithm.rs:145-195`; `AlgorithmHost::project()` `algorithm.rs:99`; host impl `uni-plugin-builtin/src/algorithms/bridge.rs:127-170`; `run()` invoked on **both** CALL paths (planner `df_graph/procedure_call.rs:651-709`, executor `executor/procedure.rs:636-810`) via `run_algorithm_provider` `procedures_plugin/algo.rs:594-609`; miss-only fallthrough (built-ins never regress); L0 snapshot for read-your-writes. First-party dogfood `ReachabilityProvider` (BFS) `algorithms/reachability.rs`, authored purely against the public surface (no host downcast). Reachability flagship (§7) now achievable **as a plugin**. |
| **P0.1b — `HostQuery` capability made real** | ✅ **Done (graph-access facet)** | `Capability::HostQuery{read_only,scopes}` now enforced: `project()` returns `FnError 0x804` unless granted `bridge.rs:131-146`; built-in `uni` granted at `api/mod.rs:220-223`; negative capability-gate test present. (Broader Cypher/Locy re-entrancy facet still unimplemented — only topology projection is wired.) |
| **P0.2 — FTS analyzer/tokenizer/stemmer config** | ✅ **Done** | New backend-agnostic mapping layer `uni-store/src/backend/fts_analyzer.rs` translates every `TokenizerConfig` arm (incl. `Custom{name}` pass-through `:87-89`) into Lance `InvertedIndexParams`; stemmer/stop-word/lowercase/ascii-fold/language/token-length all wired `:128-191`; forwarded through `index_manager.rs:654-671` → `backend/lance.rs:1040-1061`; DDL `OPTIONS` parsed `planner.rs:9012-9120`; 10 mapping tests. |
| **P0.3 — honesty hazards (the 5 named)** | ✅ **Done** | (1) `declareTrigger` installs a real firing `TriggerPlugin` via `CypherTriggerSynthesizer` `plugin-custom/src/lib.rs:357-370` + `synthetic_trigger.rs:125`; (2) `FireMode::EventualConsistency` is a real coalescing `EcQueue` `triggers.rs:706-713,1599-1770`; (3) custom-ns Locy aggregates resolve via `candidate_splits` `df_graph/locy_fold.rs:102-113`; (4) CRDT merges route through registry on compaction `compaction.rs:303-308` **and** L0 `runtime/l0.rs:424-425`; (5) `TokenizerConfig::Custom` honored (P0.2). |
| **P1 — subtract 4 legitimately-closed traits** | ✅ **Done** | `OperatorProvider`, `PregelProgramProvider`, durable-write `StorageBackend` plugin, and `Connector` **removed** (`surfaces/mod.rs:1258-1260`); `NoopConnector` gone; no `storage_backend(scheme)` getter. Matches the §9 P1 recommendation exactly. |

### What remains open (all P2/P3/legitimately-closed — nothing new, nothing regressed)

- **§6 #6 — guest loaders never self-verify signatures** — STILL TRUE; verification centralized in the top-level `uni` API (`api/mod.rs:3777`), skippable by direct loader use. Tracked as the P0.7 **security** milestone.
- **Still Trait-only/dead** (as of 2026-07-09): `window_fn`, `locy_predicate`, `logical_type`, `collation`, and `index_kind` build/open/finalize (probe-only remains partial). **→ §0.1: `window_fn` and `locy_predicate` are now Delivered (P2, 2026-07-10); `logical_type`/`collation`/`index_kind` remain.**
- **Guest authoring still 3/23** — the new `Algorithm` kind is native-Rust-authorable only; no guest loader parses it (§8 gap #2, §9 P3).
- **Closed enums (as of 2026-07-09)** — `DistanceMetric`, `FusionMethod`, `ConstraintType`, `VectorIndexKind`, semirings. **→ §0.1: `FusionMethod` grew `dbsf`/`relative_score`, the constraint surface grew an enforced `NodeKey`, and a first-party `DataType::Point` shipped (P2, 2026-07-10). `DistanceMetric`/`VectorIndexKind`/semirings unchanged; BM25 `k1`/`b` stays backend-blocked.**
- **§8 #3/#5/#6** — plugin distribution (CLI still `Install`-only, non-local schemes bail `M12`), hot-path panic isolation (only triggers/hooks `catch_unwind`-wrapped, not scalar/agg/proc), and dead observability (`record_invocation` still uncalled) — all remain, all P3.

### Recalibrated bottom line

The doc's §10 thesis — *"the framework over-modeled the surface and under-wired the two things with universal precedent"* — has been acted on. The two genuine must-fixes (`GraphView`, FTS analyzer config) are wired, the named honesty hazards are closed, and the subtractive work (deleting registrable-but-unhonorable traits) is done. **The advertised plugin API now materially matches what the engine can honor.** What's left is the deliberately-deferred tier: the P0.7 signature-verification security hardening, and the P2/P3 "ship-as-config/built-in" conveniences.

Row-level verdicts in §2–§9 below are annotated inline with `→ 2026-07-09:` where the state changed; unannotated rows were re-confirmed unchanged.

---

## 0.1. Status update — 2026-07-10 (P2 tranche, HEAD `78da69c8d`, v3.1.0)

The **P2 config/built-in quick-wins and compute-surface opens** from §9 have shipped in an additive
`3.0.0 → 3.1.0` release (commit `78da69c8d`, branch `feat/plugin-p2-quickwins-compute` FF-merged to
`main`). Six items, each verified end-to-end (21 new integration tests + unit/doc tests, clippy- and
fmt-clean). This does **not** touch the deferred P2 heavier-should-opens or the P3/legitimately-closed
tiers. The user opted into two of the four P2 tranches (quick-wins + compute surfaces); the heavier
should-opens and the APOC path-expander were deferred by choice.

### What shipped (P2)

| §9 P2 item | Verdict | Evidence (current tree) |
|---|---|---|
| **Fusion: DBSF / relative-score** | ✅ **Done** | `fuse_dbsf` (per-source z-score, sign-flip distances) `query-functions/src/fusion.rs`; `"dbsf"` + `"relative_score"`/`"rsf"` arms in `uni.search` `df_graph/search_procedures.rs`. `uni.search`-only (per-list distribution needed); `similar_to()` unchanged by design. |
| **First-party geo `Point` type** | ✅ **Done** | Finished the dead `DataType::Point` storage path — Arrow struct encode/decode `store/src/storage/arrow_convert.rs`, decode routing `value_codec.rs`, and a `SpatialUdf` DataFusion adapter (`point`/`distance`/`point.withinBBox`) `query-functions/src/df_udfs.rs` so spatial calls run in DF-planned queries. A declared Point column previously **errored on write** and decoded to `Null`. (Path A: value stays `Value::Map`.) |
| **`NodeKey` constraint (composite key DDL)** | ✅ **Done** | New `ConstraintType::NodeKey` + a `unique_properties()` helper threaded through single/batch/index-population(×3)/bulk/introspection sites `schema.rs`, `writer.rs`, `bulk.rs`; Cypher `IS NODE KEY` + `IS KEY` map to it (`write.rs`, `ddl_procedures.rs`). Also fixed a **pre-existing walker crash** on composite `(a, b) IS UNIQUE`/`IS NODE KEY` DDL `uni-cypher/src/grammar/walker.rs`. (Relationship-cardinality constraints remain deferred.) |
| **Window-function dispatch** | ✅ **Done** — surface #16 no longer dead | New `PluginWindowUdwf` (`WindowUDFImpl` + `PartitionEvaluator`) `uni-query/src/query/df_udwf_plugin.rs`, resolved from the planner fallthrough `df_planner.rs` via `candidate_splits`. A plugin window fn dispatches through `OVER (PARTITION BY …)`. v1 evaluates over the whole partition (no ROWS/RANGE narrowing yet). |
| **Pregel executor** | ✅ **Done** | New vertex-centric library `uni-plugin-builtin/src/algorithms/pregel.rs` (`VertexProgram` + `run_pregel`, message combiner, vote-to-halt) atop the public `GraphView`; first-party `uni.algo.pagerank` + `uni.algo.sssp` authored purely against `AlgorithmProvider`/`GraphView`, registered in `algorithms/mod.rs`. Shipped as a **library atop `AlgorithmProvider`**, not a revived surface kind (per §9 P1). |
| **Locy filter-predicate wake** | ✅ **Done** — surface #19 no longer dead | Both eval paths dispatch a registered `LocyPredicate`: an `eval_function` interception (in-memory: SLG/DERIVE/QUERY projection) `df_graph/locy_eval.rs`, **plus** a boolean `PluginPredicateUdf` + `iter_locy_predicates()` registered alongside plugin scalars `df_udfs_plugin.rs` (rule-body `WHERE` / DataFusion). Filter-only; the 1:N *generator* variant remains deferred. |

### What remains open in P2 (deferred by scope or backend-blocked — nothing regressed)

- **BM25 `k1`/`b` params** — **backend-blocked, will not ship as-is.** Lance hardcodes `K1=1.2`/`B=0.75`
  as compile-time constants (`lance-index/.../scorer.rs`); uni-side plumbing would be inert. Needs an
  upstream/vendored Lance change.
- **Vector-metric enum growth** — L1/Manhattan (scalar-only, no Lance ANN) and Hamming/Jaccard (need a
  new binary-vector schema type; Jaccard unsupported by Lance) not built.
- **Relationship-cardinality / relationship-uniqueness constraints** — the greenfield edge-constraint
  path (needs a full-horizon degree probe + commit-time race guard) is deferred; only `NodeKey` (node
  composite key) shipped.
- **APOC-style config-driven path-expander built-in** — deferred (blocked on extending `GraphView` to a
  typed/labeled multigraph — no per-edge type/label today).
- **Structured authz `Resource`** and **read/attach remote object-store** — the two heavier "genuine
  plugin should-opens" (§9 P2 item 6) not started (breaking ABI / touches all scan seams).
- **Locy *generator* predicates** — only the filter-predicate wake shipped; the 1:N binding variant
  (new termination/safety analysis) is deferred.

### Net effect on the master tables

Of the §2 "Trait-only/dead" surfaces, **two are now Delivered** — `window_fn` (#16) and `locy_predicate`
(#19). The still-dead set narrows to `logical_type` (#22), `collation` (#23), and `index_kind`
build/open/finalize (#12 partial). Two closed enums grew a member each without opening a plugin path:
`FusionMethod` (+`dbsf`/`relative_score`) and the constraint surface (+`NodeKey`, enforced). A
first-party geospatial `DataType::Point` is now a real persisted type. The genuine remaining P2 deficit
is small: relationship-cardinality constraints, the APOC path-expander, authz depth, and remote-attach —
plus the two backend-blocked items documented above.

---

## 0.2. Status update — 2026-07-10 (P2 M-tier completion, v3.2.0)

The **P2 M-tier** (the tractable remainder of §9 P2) was addressed: three items **built** end-to-end,
one **reclassified** to deferred-L after implementation revealed it balloons past M, one **found already
open** via an existing public seam, and four documented as **deferred/closed** (below). This closes out
P2 — every line is now built or has a resolved, rationale-backed status.

### Built (P2 M-tier)

| Item | Verdict | Evidence |
|---|---|---|
| **L1 / Manhattan vector metric** | ✅ **Done** | `DistanceMetric::L1` (`schema.rs`) + `compute_distance` + `parse_vector_metric` + the string-keyed `VECTOR_DISTANCE` path (`expr_eval.rs`). L1 has no Lance ANN, so an L1 vector index builds **no physical index** (`index_manager.rs` `build_physical_vector_index` early-return) and search is **exact/brute-force** — the query path fetches the full row count and re-scores by exact L1 (`manager.rs vector_search`). E2e: L1 picks a different nearest neighbor than L2 would. |
| **APOC-style path-expander** | ✅ **Done** | First-party `uni.path.expand` `AlgorithmProvider` (`uni-plugin-builtin/src/algorithms/expand.rs`) — bounded BFS over a projected subgraph with `nodeLabels`/`edgeTypes`/`direction`/`minLevel`/`maxLevel` + `NODE_GLOBAL` uniqueness, reusing the shipped `GraphView` (label/type filtering at projection-build time, **no** typed-multigraph extension). Authored against the public surface like reachability/Pregel. (APOC's per-type directional `relationshipFilter` DSL + label terminate/end modes are follow-ups.) |
| **Structured authz `Resource`** | ✅ **Done** | `Resource` gains `{labels, rel_types, properties, operations}` **additively** (`path` retained → no break for external policies; authz has no wasm/extism ABI). `authorize_query` parses the query (only when policies are active) and populates the structured fields via an AST walk — **at the single existing chokepoint**, not relocated across the 6 call sites (lower risk; the bypass-closure regressions still pass). E2e: a label-gating policy allows `Doc` and denies `Other`. |

### Reclassified / found-open (during implementation)

- **Edge-property uniqueness constraint — RECLASSIFIED to deferred-L.** The vertex uniqueness machinery
  does *not* cleanly mirror to edges: committed edges live in a single `MainEdgeDataset` keyed by
  src/dst/type with **properties packed** (not per-property queryable columns like vertices), so the
  committed-edge uniqueness probe is new code, not a reuse — plus a new edge L0 index, a commit-time SSI
  guard, and grammar. That is L, not M. Shipping only L0-horizon enforcement would be a flush-leaky
  "unique" (duplicates slip through after a flush) — a correctness lie, so it was **not** shipped. Moved
  to the deferred D-list.
- **Read/attach remote object-store — the capability is already open.** The per-label plugin-`Storage`
  seam (`scan.rs:2213`, `lookup_label_storage`) already lets a user register a read-only `Storage`
  backed by `object_store` for a label; `object_store` is a direct workspace dep and
  `cloud_config_to_lancedb_storage_options` supplies S3/GCS/Azure credentials. The genuine remaining gap
  is only a *bundled first-party remote-Lance adapter + `UniBuilder::attach_label` sugar*, deferred as a
  nice-to-have (marginal over the existing public seam; the v1 seam is vertex-scan-only regardless).

### Deferred / closed (documented, not built)

- **D1 — Hamming/Jaccard metrics — DEFERRED (L).** Needs a new binary/uint8 vector `DataType` + `Value`
  first (cross-crate: Arrow lowering, WAL/serde, cypher literals, index config); the metric arms are the
  last 10%. Lance ANN support for binary metrics is also uncertain at the pinned version.
- **D2 — Relationship cardinality constraint — DEFERRED (L).** New full-horizon out-degree probe across
  overlay + CSR + **tx-local L0** (the tx-local adjacency gap makes it new code, not a `get_neighbors`
  call), a brand-new commit-time degree-conflict SSI guard, and super-node O(degree) perf mitigation.
  (Same edge-write surface as the reclassified edge-uniqueness.)
- **D3 — Locy generator predicates — DEFERRED (L).** No non-invasive path: a `LocyPredicate` trait
  redesign (table-valued output + `PredSignature` output-vars/cardinality), a grammar/AST binding
  production, a new flat-map/explode exec operator (none exists — `locy_fixpoint` hard-assumes a fixed,
  pre-inferred column set), and a new range-restriction/termination safety analysis. Its own multi-stage
  project. (The 1:1 **filter** predicate already shipped in v3.1.0.)
- **D4 — BM25 `k1`/`b` — CLOSED (backend-blocked).** Lance hardcodes `K1=1.2`/`B=0.75` as compile-time
  constants; uni-side plumbing would be inert. Reopen only on an upstream/vendored Lance change.

### Net effect

P2 is now fully addressed. Vector metrics gained L1 (exact); a first-party `uni.path.expand` covers the
common APOC expansion; authz can gate on structured label/type/property/operation. The remaining deficit
is four clearly-scoped L/blocked items (D1–D4) plus the edge-constraint family (edge-uniqueness +
cardinality share the same edge-write + SSI surface, best done together as one L effort) and the
remote-attach first-party sugar — none of which any comparable embedded engine treats as table-stakes.

---

## 1. The rubric — what "complete" must mean

> **Rubric-bias caveat (added after review).** The first pass of this rubric was substantially *reverse-engineered from the existing 25-trait registrar* — i.e. it measured the code against surfaces the code already imagines. That is backwards, and it caused the audit to **understate** the deficit: a rubric derived from the trait list is blind to whole use-case categories the designers never modeled. §8b adds those missing categories (analyzers, distance metrics, fusion, constraints, temporal/BTIC, governance, result formats, migration, …) — verified by code, and almost all **Missing**. Read §2–§8 as the audit of *modeled* surfaces and §8b as the audit of *unmodeled* ones.

A GraphDB + Datalog logic engine + vector/document/columnar store has a far wider extension surface than a relational DB. A plugin developer should be able to extend **every** layer without forking. We evaluated 10 domains covering ~25 native registrar surfaces and ~9 additional surfaces that a complete framework would expose. Each surface was graded:

- **Delivered** — trait + registrar + runtime dispatch + host-access sufficient + tested.
- **Partial** — works but constrained, hacky, or bypassed on some paths.
- **Trait-only (dead)** — registrable and stored in the registry, but **no runtime path ever invokes it**. Compiles, registers, passes a naive "is it in the registry?" test — and does nothing. This is the dangerous "looks done" class.
- **Missing** — no trait / closed enum / no plugin path at all.

---

## 2. Master status table

### Native registrar surfaces (25)

| # | Surface | Verdict | Evidence (file:line) |
|---|---|---|---|
| 1 | scalar_fn | **Delivered** | dispatch `uni-query/src/query/df_udfs_plugin.rs:140,455` |
| 2 | aggregate_fn | **Delivered** | dispatch `df_planner.rs:5091`, `df_udaf_plugin.rs:180` |
| 3 | procedure | **Delivered** | dispatch `executor/procedure.rs:664` |
| 4 | optimizer_rule | **Delivered** | folded into SessionState `executor/read.rs:491-518` |
| 5 | hook (SessionHook) | **Delivered** | fired `api/transaction.rs:940,954,1250,1275` |
| 6 | trigger | **Delivered** | fired `api/transaction.rs:1061,1291`; router `plugin-host/src/triggers.rs:414` |
| 7 | cdc_output | **Delivered** | `plugin-host/src/cdc_runtime.rs:191,335`; commit fills batch `transaction.rs:1219` |
| 8 | catalog | **Delivered** | planner consults `query/planner.rs:2636,2693` |
| 9 | replacement_scan | **Delivered** (opt-in, default off) | `query/planner.rs:2713`; gate `api/session.rs:261` |
| 10 | background_job | **Delivered** | driver loop `plugin-host/src/scheduler.rs:359,395,460`; persisted `scheduler_persistence.rs:64` |
| 11 | locy_aggregate | **Partial** → **Delivered (2026-07-09)** | wired `df_graph/locy_fixpoint.rs:258`; ~~resolver only looks up `builtin` ns → custom-ns aggregates never dispatch~~ **fixed: resolver now iterates `candidate_splits` over session+instance registries `locy_fold.rs:102-113`** |
| 12 | index_kind | **Partial** | probe wired via `register_index_handle` (no engine caller) `df_planner.rs:2056`; build/open/finalize unwired — DDL rejects unknown kinds `ddl_procedures.rs:403` |
| 13 | crdt_kind | **Partial** | merge dispatch wired `crdt/src/registry_dispatch.rs:102`, `store/.../property_manager.rs:2091`; **bypassed** on compaction `storage/compaction.rs:297` and L0 `runtime/l0.rs:405`; builtin kind-strings mismatch native enum |
| 14 | auth_provider | **Partial** | wired `api/mod.rs:1198`; shallow (no conn/cert metadata, no re-auth) and bypassed by default `session()` |
| 15 | authz_policy | **Partial** | wired `api/session.rs:1828`; "resource" = raw Cypher string `session.rs:1846` → RBAC/ABAC not expressible |
| 16 | window_fn | ~~**Trait-only (dead)**~~ → **Delivered (2026-07-10)** | ~~no dispatch; planner hardcodes builtins~~ **plugin window fns now dispatch via `PluginWindowUdwf` (`WindowUDFImpl`+`PartitionEvaluator`) `df_udwf_plugin.rs`, resolved from the planner fallthrough `df_planner.rs` via `candidate_splits`; usable in `OVER (PARTITION BY …)`. v1 whole-partition frame.** |
| 17 | algorithm (AlgorithmProvider) | ~~**Trait-only (dead)**~~ → **Delivered (2026-07-09)** | ~~`run()` never invoked~~ **`run()` now invoked on both CALL paths (`procedure_call.rs:651-709`, `executor/procedure.rs:636-810`) via `run_algorithm_provider` `procedures_plugin/algo.rs:594-609`; `GraphView` host-access + `HostQuery` gate wired; first-party `uni.algo.reachability` dogfoods it** |
| 18 | pregel | ~~**Trait-only (stub)**~~ → **Removed in 3.0 (2026-07-09)** | trait deleted (`surfaces/mod.rs:1258-1260`); no executor ever built — deferred until GraphView proved out, per §9 P1 |
| 19 | locy_predicate | ~~**Trait-only (dead)**~~ → **Delivered (filter) (2026-07-10)** | ~~NOT WIRED — no resolver, zero consumers~~ **filter/fuzzy predicates now dispatch on both eval paths: `eval_function` interception (in-memory: SLG/DERIVE/QUERY) `df_graph/locy_eval.rs` + boolean `PluginPredicateUdf` registered alongside plugin scalars `df_udfs_plugin.rs` (rule-body `WHERE`/DataFusion). The 1:N *generator* variant remains deferred (§9 P2).** |
| 20 | operator (OperatorProvider) | ~~**Trait-only (dead)**~~ → **Removed in 3.0 (2026-07-09)** | trait deleted (`surfaces/mod.rs:1258-1260`), per §9 P1 — no comparable engine opens physical operators. (Distinct from #4 `optimizer_rule`, which stays Delivered `read.rs:459,485-510`.) |
| 21 | storage_backend (plugin) | ~~**Trait-only (dead)**~~ → **Removed in 3.0 (2026-07-09)** | the durable-write **plugin** trait was deleted (`surfaces/mod.rs:1259`); no `storage_backend(scheme)` getter remains. The internal `StorageManager::new_with_backend` injection seam still exists (`manager.rs:286`) but is not yet config-exposed for third-party backend selection (§9 P2 "cheap should-open" remains open). Original finding: the **plugin** `uni_plugin::…StorageBackend` (`traits/storage.rs:49`) was never consulted — zero engine callers of `registry.storage_backend(scheme)`. NOTE: a *second, same-named* **internal** trait `uni_store::backend::StorageBackend` (`store/src/backend/traits.rs:69`) is the REAL, live durable abstraction the engine dispatches through (`StorageManager::backend()` `manager.rs:1113`); `LanceDbBackend` is its only real impl (`BranchedBackend` = fork wrapper). Lance is hardwired at the top (`api/mod.rs:3296`, no backend-selection config), but a `pub` injection seam `StorageManager::new_with_backend` (`manager.rs:276`) exists and is unused by the high-level API. So "replace LanceDB" is not a plugin capability, but the internal swap seam is ~90% built. |
| 22 | logical_type | **Trait-only (shell)** | no storage/literal/cast path calls it; builtins are placeholders `plugin-builtin/src/logical_types.rs:3` |
| 23 | collation | **Trait-only (dead)** | no `COLLATE` grammar; `compare`/`normalize` never invoked `plugin-builtin/src/collations.rs:3` |
| 24 | connector | ~~**Trait-only (inert)**~~ → **Removed in 3.0 (2026-07-09)** | data `Connector` trait deleted; `NoopConnector` gone; `traits/connector.rs` now holds only `AuthProvider`/`AuthzPolicy`, per §9 P1 (contradicts the embedded model). Original: lifecycle-only stub, never consulted at query time |
| 25 | label_storage | **Delivered** (correction) | per-label storage IS wired — consumed at `query/df_graph/scan.rs:2214` (M5h); distinct from the dead URI-scheme `storage_backend` |

### Surfaces with no plugin trait at all (a complete framework would have these)

| Surface | Verdict | Evidence |
|---|---|---|
| Custom path-expander / traversal (BFS/DFS/reachability) | **Missing — NO TRAIT** → **Partially resolved (2026-07-09)** | The Cypher variable-length planner hook `MATCH (a)-[*]->(b)` remains hardcoded core (`df_graph/nfa.rs`, `traverse.rs`) — **and stays closed by design** (§8c legitimately-closed). **But the flagship reachability use case is now authorable as a plugin** via `AlgorithmProvider` + `GraphView` (see §0 / §2 #17), which is the front-door path §7 said was missing. |
| Custom graph projection | **Missing — NO TRAIT** | built-in `uni.graph.project` only `procedures_plugin/graph.rs` |
| Pattern/relationship operator | **Missing — NO TRAIT** | `traits/operator.rs` is a DataFusion op, not a graph pattern hook |
| Custom vector-index algorithm | **Missing — closed enum** | `uni-common/src/vector_index_opts.rs:44-102` |
| Custom Locy semiring | **Missing — closed enum** | `uni-locy/src/semiring.rs:322`, `types.rs:315` |
| Encoding / compression | **Missing — NO TRAIT** | none in `uni-plugin/src/traits` or uni-store |
| Cost-model / cardinality hook | **Missing — NO TRAIT** | only `CatalogTable::statistics()` |
| ML/model provider (embed/rerank/generate) as plugin surface | **Missing — no SurfaceKind/Capability** | Xervo is a parallel silo; `surfaces/mod.rs:70-121` has no ML kind |
| Abduction / ASSUME / fixpoint hooks | **Missing — internal only** | `df_graph/locy_abduce.rs`, `locy_assume.rs` |

---

## 3. Headline accounting

Of **25 native registrar surfaces**:
- **10 Delivered**, **5 Partial**, **~10 Trait-only/dead**.
- Plus **~9 important surfaces have no plugin trait at all** (including the flagship: custom graph traversal).

Of **23 advertised capability kinds**, guest languages (WASM-CM, Extism, Rhai, PyO3) can author exactly **3** — scalar, aggregate, procedure. The other **20 are native-Rust-only**. The `Capability` enum advertises all 23 (`capability.rs:33-154`); the guest wire vocabulary implements 3 (`plugin-wasm/src/loader.rs:123`, `plugin-extism/src/exports.rs:88`, `plugin-rhai/src/loader.rs:212`, `plugin-pyo3/src/loader.rs:759`).

**Delivered fraction against the advertised surface: ~40% native, ~13% (3/23) multi-language.**

The original proposal's claim (`plans/plugin_framework_implementation.md:36`, since deleted from tree): *"a complete v1.0 plugin framework with 25 surfaces, 4 loaders … Nothing is deferred."* That is not what shipped.

---

## 4. Per-domain summary (completeness estimates)

| Domain | Est. | One-line verdict |
|---|---|---|
| Eventing / lifecycle / txn | ~72% | Strongest. Triggers/hooks/CDC/scheduler genuinely commit-path-wired. `declareTrigger` fraud + no guest authoring. |
| Compute UDFs | ~70% | Scalar/agg/proc solid across all guests. Window functions a hollow shell. |
| Planner / optimizer | ~45% | Optimizer rules delivered; operators trait-only; pushdown not reached by mainline planner; no cost model. |
| ML / AI (Xervo) | ~45% | Runtime works but is a parallel silo — zero plugin-framework integration, external crate, no guest authoring. |
| Cross-cutting quality | ~45% | Strong plumbing (caps, WASM sandbox, signing, hot reload); no graph host API; distribution stubbed; no hot-path panic isolation; dead observability. |
| Storage / indexing | ~40% | Catalog + replacement-scan delivered; index build lifecycle unwired; storage backend registration-only; vector algos closed enum. |
| Locy / logic engine | ~35% | FOLD aggregates the lone wired surface (custom-ns unreachable); LocyPredicate dead; semirings closed; neural via config field. |
| Connectivity / security | ~35% | Auth/authz wired but shallow; connector an inert lifecycle stub. |
| Types / values / CRDT | ~30% | CRDT partial; logical types a shell; collations trait-only, no `COLLATE` grammar. |
| **Graph / topology** | **~15%** → **materially raised (2026-07-09)** | **Flagship domain. As of 3.0.0: reachability IS a plugin (`AlgorithmProvider`+`GraphView` wired, dogfooded); Pregel removed (deferred); Cypher variable-length planner hook remains closed by design.** Original: reachability impossible; AlgorithmProvider dead; Pregel a stub; no host graph access. |

---

## 5. The root-cause pattern: "registrable but never invoked"

Ten surfaces share one failure shape: a well-designed trait, a `PluginRegistrar` method, and a `DashMap` slot in the registry — **and no runtime code that ever calls the trait**. Because the plumbing compiles and a registration test passes, each surface *reads as done* on a milestone checklist. The tell is always the same: grep for the trait's method name across `uni-query`/`uni-store` returns only the definition, the registrar, and a test — never a hot-path caller.

This is why the milestone table said "M1–M11 complete": milestones were graded on the surfaces that had a runnable end-to-end example (the scalar/agg/proc trio had `geo.rhai`-style demos and conformance probes), while the structural surfaces had no example that could fail. **A capability with no test cannot report that it is missing.**

Verified dead-on-arrival surfaces: `window_fn`, `algorithm` (AlgorithmProvider), `pregel`, `locy_predicate`, `operator`, `storage_backend`, `logical_type`, `collation`, `connector` (data path), `label_storage`.

---

## 6. Honesty / correctness hazards (beyond "missing")

These are worse than gaps — they mislead a user into thinking something works. **(2026-07-09: hazards 1–4 and 7 are FIXED; only 5 (partly) and 6 remain — see §0.)**

1. ~~**`declareTrigger` is a fraud vector.**~~ **FIXED (2026-07-09, WS-A):** now installs a real firing `TriggerPlugin` via `CypherTriggerSynthesizer` (`plugin-custom/src/lib.rs:357-370`, `synthetic_trigger.rs:125`). Original finding: `uni.plugin.declareTrigger` installs a `SyntheticProcedurePlugin` (a `ProcedurePlugin`), **not** a `TriggerPlugin` — so a "declared trigger" becomes a callable procedure that **never fires on mutations**. `plugin-custom/src/lib.rs:299`; synthetic type `plugin-host/src/synthetic_procedure.rs:114`.
2. ~~**`FireMode::EventualConsistency` is a lie**~~ **FIXED (2026-07-09, WS-E):** real coalescing `EcQueue` (`triggers.rs:706-713,1599-1770`). Original: collapsed onto Async with no batched queue.
3. ~~**Custom-namespace Locy aggregates silently never dispatch**~~ **FIXED (2026-07-09):** resolver now iterates `candidate_splits` over session + instance registries (`df_graph/locy_fold.rs:102-113`). Original: resolver only queried the reserved `builtin` namespace.
4. ~~**CRDT registry bypassed on compaction/L0**~~ **FIXED (2026-07-09, WS-D):** merges route through the registry on compaction (`compaction.rs:303-308`) and L0 (`runtime/l0.rs:424-425`); registry installed at init (`api/mod.rs:3219`). Original: silently ignored during compaction and L0 flush.
5. **Stale comments in both directions** — partially addressed (`930c9904e`); some remain. Original: some claim things unwired that now work; others imply completeness that isn't there.
6. **Guest loaders never self-verify signatures** — **STILL TRUE (2026-07-09).** Manifest/hash verification lives only in the top-level `uni` API path (`api/mod.rs:3777`); the four loaders never call `verify.rs`. Trust depends on the entry path. **Tracked as the P0.7 security milestone.**
7. ~~**`TokenizerConfig::Custom{name}` is a dead config arm**~~ **FIXED (2026-07-09, WS-F):** honored via `fts_analyzer.rs` — see §0 P0.2. Original: — the FTS schema accepts a custom tokenizer (`uni-common/src/core/schema.rs:1317`), but `create_fts_index` drops the `tokenizer` field entirely and builds Lance's default (`store/src/storage/index_manager.rs:630`, `backend/lance.rs:1038`). A user who sets a custom analyzer gets silently ignored default tokenization.

---

## 7. Flagship use case: third-party BFS/DFS reachability — **NOT POSSIBLE as a plugin**

Three independent blockers, any one of which is fatal:

1. **No path-expander hook.** Variable-length/reachability expansion in `MATCH (a)-[*]->(b)` is hardcoded core (`df_graph/nfa.rs`, `traverse.rs`) with zero plugin entry point. There is no trait to inject a custom traversal.
2. **No host graph-access API.** `AlgorithmHost` exposes exactly one method — `as_any()` (`traits/algorithm.rs:73`). There is no `GraphView`/neighbors/CSR abstraction (`algorithm.rs:82-86` admits it is "out of scope … once those APIs are available"). A plugin can reach topology only by downcasting the host to the concrete internal `AlgorithmHostBridge` in `uni-plugin-builtin` and pulling a raw `StorageManager` — not viable out-of-tree.
3. **`AlgorithmProvider::run` is never called.** CALL dispatch uses the legacy `ProcedurePlugin` adapter over the static `uni_algo::AlgorithmRegistry`; nothing binds a host or invokes the plugin trait. A correct out-of-tree `AlgorithmProvider` would register and then never run.

The only way to add reachability today is to add a `uni_algo::AlgoProcedure` **inside the tree** — which is forking, not plugin authoring — and even that cannot hook Cypher variable-length traversal.

---

## 8. Cross-cutting gaps (framework quality)

**Delivered strengths (credit where due):** capability model with registration-time enforcement (`capability.rs`, `registrar.rs:124`); real WASM sandbox — memory/fuel/epoch limits (`plugin-wasm/src/loader.rs:827-844`); Ed25519 signing + Blake3 pinning (`verify.rs:143`); arc-swap wait-free hot reload (`registry.rs:480`); per-major ABI linker (`multi_version.rs`).

**Gaps:**
1. **No coherent graph/query host-callback API.** `Capability::HostQuery` exists (`capability.rs:52`) but has **no implementation**. Graph access is ad-hoc downcasting. Blocks every graph-algorithm and data-aware plugin.
2. **20/23 capability kinds are native-Rust-only.** A Python/WASM/Rhai author can ship only UDFs.
3. **Distribution unimplemented.** `uni plugin install` loads only local `.rhai`; OCI/Hub/HTTP/`.wasm` all `bail!("M12")`; no `list`/`remove` subcommand (`uni-cli/src/main.rs:151-289`). No way to ship or manage a plugin.
4. **No native `add_plugin` from Python** — it's generic over a Rust type; PyO3 users get guest loaders only (`bindings/uni-db/src/sync_api.rs:847`).
5. **No panic isolation on the invoke hot path** — `catch_unwind` wraps triggers only (`triggers.rs:678`); a panicking native scalar/agg/proc unwinds the query worker.
6. **Plugin observability is dead wiring** — `record_invocation` is never called (`observability.rs:90-111`); plugins have no metrics/trace emit API.
7. **Conformance harness is shallow** — 6 structural checks (manifest/abi/registration); no determinism or resource-limit probes (`plugin-conformance/src/lib.rs:220`).

---

## 8b. Use-case categories the trait list never modeled

The §2 tables audit surfaces the framework *has a trait for*. But a rubric built from the trait list can't see what was never modeled. Auditing from plugin-developer personas (search/RAG engineer, domain modeler, platform/governance, data engineer) surfaces **12 more categories — verified by code, essentially all Missing.** This is the part the "23-capability coat" hides: even the 23 kinds are a designer's list, not a user's.

| Category (persona need) | Verdict | Evidence |
|---|---|---|
| **FTS analyzers / tokenizers / stemmers / stop-words** | **Missing (no trait)** | `TokenizerConfig` enum exists but is **dropped** before index build; hardcoded `split_whitespace().to_lowercase()` `store/.../inverted_index.rs:146`; Lance default `backend/lance.rs:1038` |
| **Custom vector distance/similarity metric** | **Missing (closed enum)** | `DistanceMetric {Cosine,L2,Dot}` `schema.rs:1267`, `backend/types.rs:137`; exhaustive match to Lance `backend/lance.rs:749` |
| **Custom vector quantization** | **Missing (closed enum)** | `VectorIndexKind` closed `backend/types.rs:155`; escape only via whole-index `IndexKindProvider` |
| **Chunkers / text splitters / sparse text encoders** | **Missing (no trait; external)** | all in external `uni-xervo` (`Cargo.toml:175`); no uni-plugin trait |
| **Hybrid-search fusion / ranking strategy** | **Missing (closed enum)** | `FusionMethod {Rrf,Weighted}` `query-functions/src/similar_to.rs:67`; hardcoded `fusion.rs:17` |
| **Custom FTS / BM25 scorer (even k1/b params)** | **Missing (no trait)** | delegated to Lance `FtsIndexBuilder::default()` `backend/lance.rs:1046`; params not exposed |
| **Write-path constraints: cardinality / referential / custom validators** | **Missing (closed enum)** | `ConstraintType {Unique,Exists,Check}` closed `schema.rs:674`; CHECK is a built-in interpreter `writer.rs:2394` |
| **Computed / derived properties, default-value generators** | **Missing (no trait)** | no `DefaultValueProvider`/`ComputedProperty` trait |
| **Custom Cypher syntax / keyword / AST rewrite** | **Missing (hardcoded grammar)** | static PEG `uni-cypher/src/grammar/cypher.pest`; `on_parse` hook can only reject, not rewrite |
| **Temporal / BTIC extensibility** (custom Allen relations, certainty, granularity) | **Missing (closed, zero traits)** | `uni-btic/src/` has no traits; `Certainty`/`Granularity` closed enums |
| **Data governance: masking / PII redaction / row- or property-level security / retention-TTL** | **Missing (no trait)** | authz `Resource{path}` is coarse Allow/Deny `traits/connector.rs:112`; can veto, cannot mask/filter/rewrite |
| **Query-result serialization / export formats** | **Missing (no trait)** | no result-format trait; only `CdcOutputProvider` (CDC sinks, not query results) |
| **Schema-migration / DDL-evolution hooks** | **Missing (no trait)** | DDL goes straight to `schema_manager().add_constraint(...)` `ddl_procedures.rs:517`; hooks observe/reject only |

**The one honest mitigation:** `SessionHook` lifecycle phases (`on_parse`/`on_analyze`/`before_commit`/…) and `TriggerPlugin::fire` can return `Reject`, giving a partial *imperative veto* path for validation, governance, and migration. But they can only **reject** — never rewrite, mask, filter, or transform. So even the "escape hatch" cannot express row-level security, PII masking, computed properties, or query rewriting.

**Implication for the headline.** §3's "~40% native" counts only modeled surfaces. Adding these 12 unmodeled-but-expected categories (≈0% delivered) pulls the *true* completeness against a persona-driven rubric meaningfully lower — the framework is comprehensive about UDFs and eventing, and sparse-to-absent across search-pipeline, domain-modeling, governance, and language-extension needs.

---

## 8c. Demand calibration (adversarial pass)

The §2/§8b tables say what is *technically* missing. They do **not** say what *should* be built — a completeness checklist is not a roadmap. So every Missing/closed/dead surface was pressure-tested by adversarial reviewers: steelman the AGAINST (why a serious engine keeps it closed) *first*, then FOR with a concrete blocked use case and **real-world precedent** (does Neo4j / Postgres / DuckDB / Lucene / LanceDB / Datomic / XTDB actually open this to third parties?). Two disciplines drove the verdicts: **precedent** (if no comparable engine opens X, that's strong evidence it's legitimately closed) and the **embedded model** (server-DB extension points often collapse in value when the host app owns the process, the file, and the data).

Two structural findings cut across everything:
- **The padding is the "arbitrary-code plugin" framing.** Most real needs are a *config knob, an enum member, or a built-in addition* — not a plugin trait. No vector DB ships a pluggable distance metric or quantizer; no production DB opens its parser at runtime; nobody exposes a pluggable temporal codec.
- **"Registrable but dead" is itself a defect.** Shipping a trait + registrar for a surface that is legitimately closed (operator, wire-connector, durable storage backend, semiring, pattern operator) is worse than not shipping it — it advertises an extension contract that shouldn't exist.

### Consolidated verdict matrix

> **Note (2026-07-10):** the "Current state" column is the 2026-07-07 snapshot. Since shipped (see §0/§0.1):
> `GraphView` (MUST-OPEN) ✅; FTS analyzers ✅; **Fusion DBSF/relative-score** ✅; **Window functions** ✅;
> **Pregel** ✅; **Locy filter predicates** ✅ (generator still open); **first-party geo type** ✅;
> **declarative constraints** ✅ node-key (relationship-cardinality still open). The "Demand verdict" column is unchanged.

| Extension point | Current state | Demand verdict | Precedent / rationale |
|---|---|---|---|
| **`GraphView` — real topology access for `AlgorithmProvider`** | Trait exists, GraphView unbuilt | **MUST-OPEN** | Neo4j GDS, TigerGraph, Oracle PGX, Souffle all open it. Unblocks the reachability flagship "through the front door." |
| **FTS analyzers / tokenizers / stemmers (config)** | Enum arm dead-dropped | **MUST-OPEN** (as config, not code plugin) | Lucene/ES/Postgres/**Tantivy** overwhelming; CJK/multilingual entirely blocked today |
| Pregel vertex-centric | Stub trait, no `compute` | **Should-open** (downstream of GraphView) | Neo4j GDS flagship "write-your-own-algo"; don't stabilize before GraphView |
| Locy *generator* predicates (bind vars) | Filter predicates shipped | **Should-open** | Datomic/Souffle functors; filter half already done |
| Custom logical types (**geospatial**) | Closed `DataType` enum | **Should-open** (first-party geo > generic registry) | PostGIS canonical; DuckDB ext types. Tempered: Neo4j ships zero UDTs |
| Declarative cardinality / relationship constraints | Closed `ConstraintType` | **Should-open** (built-in DDL, **not** code hook) | Neo4j node-key/rel-uniqueness; arbitrary validator code stays closed |
| Storage: read/attach remote object store | Plugin path dead; `label_storage` **is** wired (`scan.rs:2214`) | **Should-open** (read side only) | DuckDB httpfs/S3 — strongest embedded precedent |
| Storage: **swap the durable backend** (replace LanceDB) | Internal trait live but Lance-hardwired; `new_with_backend` injection seam exists, unused | **Should-open (cheap)** — expose backend selection via config into existing `new_with_backend`; distinct from durable-write *plugin* (which stays closed) | Real `pub` seam already ~90% built; not a correctness rewrite, just a selection knob |
| Authz `Resource` depth | Wired but `resource` = raw Cypher string | **Should-open** (near-bug) | Neo4j sub-graph/property security; real for multi-tenant embedded |
| BM25 `k1`/`b` params | Not exposed | **Should-open** (params, not scorer plugin) | Lucene `BM25Similarity(k1,b)`, ES per-field |
| Vector metric menu: L1 / Hamming / Jaccard | Closed enum | **Should-open** (enum growth, not plugin) | Qdrant added Manhattan; Milvus binary metrics |
| Fusion: DBSF / relative-score | Closed 2-variant enum | **Should-open** (built-in add) | Qdrant DBSF, Weaviate relativeScore; partly absorbed by reranker |
| Window functions | Trait dead (§2 #16) | **Should-open** (moderate) | Postgres/DuckDB window UDFs exist; modeled-but-unwired |
| APOC-style config-driven path expander | — | **Nice-to-have** (ship as **built-in**, not plugin) | Neo4j APOC `expandConfig`; Locy recursion + shortestPath cover most |
| Collations / COLLATE | Trait dead, no grammar | **Nice-to-have** | Postgres real; thin in embedded graph/vector niche; FTS covers most |
| Computed / derived properties | No trait | **Nice-to-have** | Largely covered by Locy `DERIVE` + system timestamps |
| Custom index kinds | Build lifecycle dead | **Nice-to-have** | pgvector lesson: *build in*, don't open up |
| Governance: masking / PII / RLS / TTL | Absent | **Nice-to-have** (gated behind authz depth) | Snowflake/BigQuery — enterprise-server tier; embedded app can mask in-code |
| Guest hooks / triggers / algorithms | Native-only | **Nice-to-have** (pure-compute only) | safe subset of guest breadth |
| Sparse-encoder alias | External Xervo | **Nice-to-have** | Xervo already exists; small step |
| Plugin distribution (OCI, CLI list/remove) | Missing | **Nice-to-have** (gated on ecosystem) | DuckDB community repo — valuable *once plugins exist* |
| Custom path-expander **into the planner** | No trait | **Legitimately-closed** | Nobody opens the variable-length planner; use Locy/shortestPath |
| Custom semirings | Closed enum | **Legitimately-closed** | Even ProvSQL keeps new-semiring extension internal; research-only |
| Custom pattern / relationship operators | No surface | **Legitimately-closed** | No precedent; breaks planner cost/uniqueness |
| Custom CRDT kinds | 8 built-ins | **Legitimately-closed** | Silent divergence risk; Yjs/Automerge keep it closed |
| **Temporal / BTIC extensibility** | Closed, zero traits | **Legitimately-closed — clearest vanity padding** | Allen's 13 relations are mathematically closed; XTDB ships bitemporality non-extensible |
| Custom Cypher grammar / AST rewrite | Static PEG | **Legitimately-closed** | No production DB opens its parser at runtime |
| Custom distance-metric **plugin** | Closed enum | **Legitimately-closed** | pgvector/Qdrant/Milvus/Weaviate all fixed enums |
| Custom quantization | Closed enum | **Legitimately-closed** | No vector DB opens it; footgun |
| Custom FTS **scorer** plugin | No trait | **Legitimately-closed** | ES has it, near-zero adoption |
| Custom physical operators | Trait dead | **Legitimately-closed** | Postgres CustomScan = Citus/Timescale fork-scale only |
| Wire-protocol connectors | Inert stub | **Legitimately-closed** | Contradicts embedded model; run a server if you want a server |
| Query-result serialization / export | No trait | **Legitimately-closed** | Client concern over an Arrow batch |
| Schema-migration hooks | No trait | **Legitimately-closed-leaning** | Flyway/Alembic/Liquibase live outside the DB |
| Guest storage / index authoring | Native-only | **Legitimately-closed** | Durability/security; SQLite vtab/VFS are in-process C, not sandboxed guests |

### What the calibration changes

The raw audit read as "the framework is ~40% done and broken." The calibrated read is sharper and less alarming: **the framework is comprehensive where it matters most for an embedded graph+logic+vector DB (UDFs, eventing, optimizer, catalog/replacement-scan), and its *genuine* deficit is small and focused.** Most of the dead/missing surfaces are things no comparable engine opens, or that the embedded model makes low-value. The real, precedented must-fix list is **two items** (`GraphView`, FTS analyzer config) plus a focused Should-open tier. The rest splits into "ship as built-in/config, not a plugin" and "legitimately closed — and should be *removed* from the advertised trait surface."

---

## 9. Prioritized remediation (demand-calibrated)

Priorities follow §8c demand, not §2 completeness. Three of these are **subtractive** — the highest-value move for a "complete plugin API" claim is often to *stop advertising* a surface that shouldn't exist.

> **2026-07-09: P0 (all three) and P1 are DONE** — shipped in v3.0.0 (HEAD `fe64b48f5`). See §0 for on-path evidence. P2/P3 remain as written below.

**P0 — the two genuine must-fixes + stop the lies** — ✅ **DONE (2026-07-09)**
1. ✅ **Build `GraphView`** — a stable read-only topology API (neighbor/degree/weight iteration + slot↔vid; `GraphProjection` already has the internals) passed via `AlgorithmContext`, and wire `AlgorithmProvider::run` into CALL dispatch. This is the *only* precedented MUST-OPEN with high demand, and it satisfies the reachability flagship through the front door (write BFS as a first-class `AlgorithmProvider`). Do it as the coherent host-access layer.
2. ✅ **FTS analyzer/tokenizer/stemmer config** — honor `TokenizerConfig` (currently dead-dropped) and expose language-analyzer + stop-word + stemmer selection on the FTS index. Unblocks CJK/multilingual, which is entirely broken today. Config surface, not an arbitrary-code plugin.
3. ✅ **Fix the honesty hazards** (cheap, high-trust): `declareTrigger` must register a real `TriggerPlugin` or reject; custom-namespace Locy aggregates must resolve; `EventualConsistency` real-or-removed; route CRDT merges through the registry on compaction/L0; honor or drop `TokenizerConfig::Custom`. **All 5 done.** (The related §6 #6 signature-self-verify hazard is NOT part of this item — it lands in the P0.7 security milestone.)

**P1 — subtract: retire the "registrable but legitimately-closed" traits** — ✅ **DONE (2026-07-09)**
4. ✅ **Remove the trait + registrar** for surfaces that no comparable engine opens and the embedded model makes low-value: `OperatorProvider` (physical operators), the durable-write `StorageBackend` plugin path, wire-protocol `Connector`, and the `PregelProgramProvider` stub *until* GraphView lands — **all four removed** in the 3.0.0 breaking release (`surfaces/mod.rs:1258-1260`). A registrable-but-dead trait is an extension contract you can't honor — deleting it makes the framework *more* complete, not less.

**P2 — the focused Should-open tier (built-in/config first, plugin only where precedented)** — **partially DONE (2026-07-10, v3.1.0); see §0.1**
5. **Ship as built-in/config, not plugins:** ~~fusion (DBSF/relative-score)~~ ✅; ~~first-party geo type~~ ✅ (`DataType::Point`); ~~declarative constraints (DDL)~~ ✅ **partial** — `NodeKey` done, edge-uniqueness + relationship-cardinality deferred-L (§0.2 D2); ~~vector-metric enum growth — L1~~ ✅ **done** (exact/brute-force; §0.2); **Hamming/Jaccard** ⬜ deferred-L (need a binary-vector type, §0.2 D1); ~~APOC-style config-driven path-expander built-in~~ ✅ **done** (`uni.path.expand` via `GraphView`, §0.2); **BM25 `k1`/`b`** ❌ backend-blocked (§0.2 D4).
6. **Genuine plugin Should-opens:** ~~window-function dispatch~~ ✅; ~~Pregel~~ ✅ (`uni.algo.pagerank`/`sssp`); ~~Locy *generator* predicates~~ — filter half ✅ (v3.1.0), 1:N generator deferred-L (§0.2 D3); ~~deepen the authz `Resource`~~ ✅ **done** (structured labels/rel-types/properties/operations, additive — not ABI-breaking after all; §0.2); ~~read/attach remote-object-store path~~ ✅ **already open** via the public per-label plugin-`Storage` seam; first-party sugar deferred (§0.2).

**P3 — gated / niche (do only when a trigger arrives)**
7. Guest hooks/triggers/algorithms (pure-compute breadth only — never guest storage/index); plugin distribution (OCI + `list`/`remove`) once a real plugin ecosystem exists; governance masking/RLS (regulated-edge niche, gated behind the authz-depth work in P2); hot-path panic isolation and the plugin observability API.

**Explicitly NOT on the roadmap (document as intentionally closed):** custom quantization, custom distance-metric/scorer *plugins*, custom CRDT kinds, BTIC/temporal extensibility, custom Cypher grammar, custom physical operators, result-format plugins, guest-owned durable storage/index, schema-migration hooks. Each was pressure-tested against precedent and found legitimately closed; documenting them as such is part of an honest "complete API" claim.

---

## 10. Bottom line

The plugin framework has a genuinely strong **core** (capability model, sandboxing, signing, hot reload, ABI versioning) and delivers the **compute trio** (scalar/aggregate/procedure) end-to-end across all four languages, plus a solid **eventing** layer (triggers/hooks/CDC/scheduler). That is real and valuable.

Two readings, both true:
- **Against raw completeness** (§2/§8b): ~40% of modeled surfaces are runtime-wired, ~3/23 kinds are guest-authorable, and ~12 use-case categories were never modeled. The flagship graph use case is not achievable as a plugin, and several surfaces actively mislead (`declareTrigger`, custom-ns aggregates, `TokenizerConfig::Custom`, dead registrable traits).
- **Against calibrated demand** (§8c): the *genuine* deficit is small and focused. Most dead/missing surfaces are things no comparable engine opens, or that the embedded (app-owns-the-data) model makes low-value. The real, precedented must-fix list is **two items** — `GraphView` and FTS analyzer config — plus a focused Should-open tier that is mostly "ship as built-in/config," not "add a plugin trait."

The honest conclusion is not "the framework is 40% broken" but "**the framework over-modeled the surface and under-wired the two things that actually have universal precedent.**" The single highest-value investment is **`GraphView`** (unblocks every topology-touching algorithm *and* the reachability flagship in one stroke). The second-highest-value work is **subtractive** — deleting the registrable-but-legitimately-closed traits so the advertised API matches what the engine can actually honor. A "complete plugin API" is defined as much by what it credibly refuses to open as by what it opens.

> **2026-07-09 postscript.** Both of those highest-value investments have shipped (v3.0.0, HEAD `fe64b48f5`): `GraphView` + `AlgorithmProvider::run` are wired and dogfooded, and the four registrable-but-closed traits (Operator, Pregel, StorageBackend, Connector) are deleted. Together with the FTS analyzer config and the five named honesty-hazard fixes, **the entire P0 and the P1 subtractive tier are done** — the advertised plugin API now materially matches what the engine can honor. Remaining work is the deliberately-deferred tier: the P0.7 signature-verification security hardening (§6 #6), and P2/P3 "ship-as-config/built-in" conveniences. See §0 for the full verified delta.
