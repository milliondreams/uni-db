# uni-plugin changelog

All notable additions to `uni-plugin`'s public surface. Versions track
the workspace version unless an entry is annotated otherwise — the
workspace stays on `1.3.x` while individual crates publish additive
v1.4 minor bumps when their ABI grows.

## Unreleased — Plugin Compute ABI, Phase 0 (additive)

First phase of the Plugin Compute ABI (`docs/proposals/plugin_compute_abi_2026-07-13.md`),
which extends GraphCompute additively. No breaking changes.

### Changed — GraphCompute native-work grant semantics (governance-posture change)

- **`Capability::GraphComputeWork(w)` now *raises* the ceiling, not just lowers it.**
  A grant is authoritative: when present it is the effective budget (it may exceed
  the size-derived default `min(10_000·(|V|+|E|+1), 1e9)`); when absent, that default
  applies. Previously the grant was `.min()`-clamped to the default, so no caller
  could authorize a legitimately large job. The policy is now single-sourced in
  `WorkBudget::resolve(work_cap, vertices, edges)`; the seven kernel/adapter install
  sites each call it. **Grant-review note:** an explicit `GraphComputeWork` grant now
  authorizes *more* native work — treat it as a real authorization. Arena-bytes and
  wall-clock ceilings are unaffected (independent dimensions).

### Added — seeded `sample` kernel + shared counter-hash

- **`GraphCompute::sample(prob, seed, iter) -> Handle`** — a reproducible
  `Bernoulli(prob[v])` mask (a `VertexSet`) over a `[V]` `f64` tensor, exposed to
  every loader (Rhai/PyO3/Extism/WASM dispatch `"sample"` op; `KernelRequest` gains an
  `iter` field). Charges `|V|` work in `BUDGET_CHECK_CHUNK` increments; rejects an
  `i64`-backed tensor with `0x862`.
- **`uni_algo::algo::rng`** — the counter-hash RNG (`counter_hash`, `splitmix64_finalize`,
  `hash_to_unit_f64`, `sample_bernoulli`) promoted from the private `random_walks`
  seeding into a shared, reproducible primitive. Stateless streams are order/partition/
  thread-independent by construction. `random_walks` output is byte-identical (guarded).
- **Conformance probe `graph.sample_determinism`** added to `run_probes()`.

### Added — Mode A edge kernels (Phase 1, proposal §5)

- **`[E]` per-edge tensor** (`Shape::E`, `Tensor::from_f64_edge`) indexed by CSR
  out-edge order (`GraphProjection::out_edge_start`), and an **edge mask**
  (`HandleKind::EdgeSet`, `value::EdgeSet`).
- **New `GraphCompute` kernels**, exposed across all loaders (dispatch `op`s +
  Rhai/PyO3 methods): `edge_weights`, `edges_all`, `sample_edges(prob, seed, iter)`,
  `edge_set_len`, `edge_intersect`, `edge_union`, `expand_masked(g, frontier, dir,
  exclude, edge_mask)`, `spmv_masked(g, vec, semiring, edge_mask)`. Masked traversal
  is out-direction only (an `In` mask is `0x86E`); the result equals the kernel on
  the subgraph of exactly the masked edges.
- **`map_apply` is now shape-preserving** — an elementwise op on a `[E]` tensor
  stays `[E]` (previously collapsed to `[V]`). `KernelRequest` gains a fourth handle
  operand `c` and an `iter` field. New error constructor `error::arg_validation`
  (`0x86E`).
- **`segmented_reduce(values, groups)`** (A-4) — a deterministic grouped reduce
  using the `deterministic_sum` accumulator (bitwise group totals independent of
  vertex order/partitioning); **`edge_mask_window(vals, lo, hi)`** (F-11) — a
  deterministic threshold from a `[E]` tensor to an edge mask (e.g. a temporal
  event window). Both exposed across dispatch + Rhai/PyO3.

### Changed — registration-driven DataFusion CALL eligibility (Phase 2, proposal §6, DF-3)

- **`AlgorithmSignature` gains `df_composable: bool`** (default `false`, additive). A
  provider sets it `true` to declare its `CALL` may be planned as a first-class
  DataFusion `GraphProcedureCallExec` node (the vectorized path).
- **Third-party algorithm providers can now reach the DataFusion plan path by
  declaration** (the DF-3 registration-driven flip). `is_df_eligible_procedure`
  qualifies a `CALL` from three sources: (1) the built-in DF-native procedure set
  (`uni.schema.*`, `uni.{vector,fts,sparse}.query`, `uni.search`, `uni.create.v*`);
  (2) first-party graph-algorithm procedures under the reserved `uni.algo.*`
  namespace (DF-native adapters — third parties cannot register under `uni`, so this
  is a first-party shortcut, not a squatting vector); and (3) **any registered
  algorithm provider whose `AlgorithmSignature` declares `df_composable`** —
  previously a third-party algorithm provider could reach the DF path *only* by
  squatting the prefix; now it declares composability and is a first-class plan
  node, while a non-declaring one stays on the row path. The row-based fallback
  stays a correctness twin (DF-2). First-party GraphCompute/algo providers
  (`gcpagerank`, `gcwalks`, `gcoverlap`, `reachability`, `pagerank`, `sssp`) declare
  `df_composable = true`.
- **Streaming lift (DF-4):** an algorithm-registry provider's `CALL` is now
  forwarded batch-by-batch through `GraphProcedureCallExec` (per-batch `YIELD`
  projection via a `RecordBatchStreamAdapter`) instead of buffered to one
  `RecordBatch` via `concat_batches`. Non-algorithm procedures keep the buffered
  single-batch path unchanged.

### Added — iteration driver, determinism accumulator, Mode B cores

- **`uni_algo::algo::reduce::deterministic_sum`** (DF-6) — a canonical-order +
  Neumaier-compensated reduction, bitwise-identical across input permutations and
  partition splits (the determinism-owning accumulator DataFusion's partitioned
  float `SUM` cannot provide).
- **`uni-query` `df_graph::iteration_driver`** (DF-5, Mode B-vec §7a) —
  `IterationDriver` re-invokes a **cached** physical sub-plan once per round to a
  graph fixpoint (`plan_count == 1`), feeding state back through a shared handle;
  `PowerStepExec` (vertex-centric) and `GraphGatherStepExec` (message-passing
  `edges → GROUP BY dst → pluggable MessageAggregate` — the guest-UDAF slot) are
  reference round bodies, matching native PageRank to `1e-9`.
- **`uni-plugin-builtin` `graph_compute::scratch::ScratchGraph`** (Mode B-seq
  §7b) — a per-invocation, session-local **mutable** scratch graph with
  budget-metered random-access ops (`0x865` on runaway) and a bounded arena
  (`0x864` on growth); seeded sampling is reproducible (counter-hash). Includes
  `require_compiled_body` (`Q-6` compiled-only gate, `0x86C`) and the host-side
  guest ABI: `ScratchGraph::call_json` (single-session) and `ScratchRegistry`
  (multi-session — unguessable ids, per-session mutex, panic isolation,
  `open`/`call_json`/`close`, the `host-graph` surface a compiled WASM/Extism guest
  drives, mirroring `GraphComputeRegistry`). Contracts `Q-1…Q-6` are all met, and
  a **real `wasm32-wasip2` guest** (`examples/example-wasm-scratch`) drives the ABI
  end-to-end through wasmtime (`crates/uni-plugin-wasm/tests/scratch_wasm_e2e.rs`).
  The **live-store `Q-3` SSI contract** (proposal open question 3) is closed: a
  Mode B-seq run is proven never observable by the store — a concurrent reader sees
  no trace during/after — and a T0 `GraphProjection` stays pinned across a concurrent
  commit (`q3_*` tests in `graph_compute_pagerank.rs`).
- **Mode B-vec is complete** — the graph gather runs as a real DataFusion
  `edges JOIN state → GROUP BY dst` aggregate, driven by the DF-5 driver, with the
  aggregate authored either as built-in `sum` or an actual guest `AggregatePluginFn`
  bridged via `PluginAggregateUdaf` (both matched to the hand-coded gather ≤1e-9);
  plus the AT-ABM SIR scenario.
- **`Q-5` perf-gate harness** — `crates/uni/benches/mode_b_seq_random_access.rs`
  (host-resident baseline + JSON-ABI crossing cost). The Mode B-seq WASM guest
  fixture and the live-store `Q-3` concurrent-reader isolation are both landed;
  the whole proposal (Phases 0–4, open questions included) is now implemented.

## 3.0.0 — 2026-07-07 — BREAKING: remove dead surfaces + trigger honesty pass

Plugin-framework honesty & subtraction (P0). This release makes the advertised
plugin API match what the engine actually honors: it **removes four registrable
but never-dispatched traits**, fixes surfaces that silently did nothing, and
changes the `TriggerContext` ABI. All breaking changes land under this single
3.0 tag.

### Removed (BREAKING)

- **`PregelProgramProvider`** (`traits::algorithm`) — a stub trait with no
  executor; never invoked. Its support types (`PregelSignature`,
  `AggregationMode`, `ComputeOutcome`, `PregelStats`) are removed too.
- **`OperatorProvider`** (`traits::operator`) — custom physical operators were
  never inserted into the planner. `OptimizerRuleProvider` (same module) is
  **retained** and is the supported planner-extension surface.
- **Plugin `StorageBackend`** (`traits::storage`) — the scheme-keyed durable
  backend plugin was never consulted by the storage engine. The per-label
  `Storage` surface (`label_storage()` / `lookup_label_storage`) is **retained**.
  (Unrelated: the internal `uni_store::backend::StorageBackend` is a different
  trait and is unaffected.)
- **`Connector`** (`traits::connector`) — a lifecycle-only stub with no
  query-time data path. Also removed: `Capability::Connector`, the
  `SurfaceKind::Connector` variant, and the public `Uni::start_connector` /
  `Uni::stop_connector` / `ConnectorLifecycle` API. `AuthProvider` / `AuthzPolicy`
  (same module) are **retained**. For external data, use the `CatalogProvider` /
  `ReplacementScanProvider` surfaces instead.
- The corresponding registrar methods (`pregel`, `operator`, `storage_backend`,
  `connector`) and `SurfaceKind` variants are removed. The shared capabilities
  `Capability::{Operator, Storage, Algorithm}` are **retained** (they back the
  delivered `optimizer_rule` / `label_storage` / `algorithm` surfaces).

### Changed (BREAKING)

- **`TriggerContext` gains an owned `Option<Arc<dyn ProcedureHost>>`** (private
  field; `with_host()` / `host()` accessors). `TriggerContext::new` is unchanged
  (host defaults to `None`), so existing `TriggerPlugin` implementors keep
  compiling, but the struct's shape changed. This enables declared triggers to
  execute Cypher action bodies.
- **`FireMode::EventualConsistency` now batches for real** instead of aliasing
  `Async`. Events coalesce per-trigger and drain via the deferral queue on an
  interval/size threshold (`UniConfig::ec_flush_interval` / `ec_flush_threshold`).

### Fixed

- **`uni.plugin.declareTrigger` now installs a real, firing `TriggerPlugin`**
  (AfterCommit/Async v1) instead of a callable procedure that never fired.
- **Plugin-namespaced Locy `FOLD` aggregates now resolve** (dotted `ns.NAME`).
- **Custom CRDT merges now route through the registry on the compaction and L0
  durable paths** (previously bypassed).
- **FTS indexes now honor tokenizer/analyzer/stemmer/stop-word config**
  (`TokenizerConfig::Analyzer` + `CREATE FULLTEXT INDEX ... OPTIONS { ... }`).
- Corrected several stale doc-comments that contradicted the code.

## 1.9.0 — 2026-06-01 — always-on Ed25519 manifest verification + signing-payload fix (security)

Hardens signed-manifest verification. Two changes, both security-relevant:

### Changed

- **`ed25519` Cargo feature removed; signature verification is now always
  compiled.** `ed25519-dalek` and `base64` are non-optional dependencies. A
  build could previously be configured (feature off) to skip the cryptographic
  check and accept any signature whose `key_id` was merely *named* in the trust
  root — a silent-acceptance footgun. Verification is a security primitive and
  is no longer a build-time opt-out. This drops the `default`/`ed25519` features
  entirely; consumers that set `default-features = false` on `uni-plugin` (none
  in-tree) no longer disable crypto.
- **Manifest signing payload now covers the whole manifest, not just the hash
  pin.** `canonical_payload` previously signed only the blake3 hash string when
  present, so an attacker could rewrite `capabilities` / `side_effects` while
  preserving the hash and the signature still verified (capability escalation
  via manifest substitution). The payload is now a domain-separated, versioned
  (`uni-plugin-manifest-sig:v1`) canonical JSON serialization of the entire
  manifest with the `signature` field excluded. **Pre-1.9.0 signatures no longer
  verify and must be re-signed.**
- **`verify_signed_manifest` is fail-closed**: a `key_id` present in the trust
  root but without bound public-key bytes (the shape-only `TrustRoot::allow`
  path) is now rejected rather than accepted.

### Added

- **`otel` Cargo feature** (default-off) wiring real OTel trace-context
  extraction in `observability::current_trace_context()`: with the feature on
  and a `tracing-opentelemetry` layer installed, it reads the current span's
  `SpanContext`. Pulls only the lightweight `opentelemetry` API +
  `tracing-opentelemetry` (not the OTLP exporter SDK), so loaders that never
  emit OTel spans stay lean. `uni-plugin-host` enables it.
- **`TraceContext::to_traceparent()`** renders a W3C `traceparent` header value,
  and **`TraceContext` gains a `trace_flags: u8` field** and is now
  `#[non_exhaustive]` (additive; future fields like `tracestate` won't break
  downstream construction).
- **`host_services` module** with the `KmsProvider` and `HttpEgress` traits
  (+ `HttpResponse`) — the shared seam backing capability-gated `uni.kms.*` /
  `uni.http.*` host functions, so every loader binds one abstraction. Re-exported
  at the crate root.
- **`Capability::network_allows` / `kms_allows` / `secret_allows`** — call-time
  (layer-3) attenuation helpers matching a URL / key id / secret id against a
  granted `Network` / `Kms` / `Secret` allow-list (anchored `*`/`**` globs).

## 1.8.0 — 2026-05-24 — arity-overloaded procedures + `AlgoProcedure::execute_with_projection` (M5 Batch 3)

M5c.2 + M5c.3 land additive surface on the procedure registry and the
algorithm trait so the new `(graphRef, config)` 2-arg algorithm shape
can coexist with the legacy `(nodeLabels, edgeTypes, ...)` form during
the deprecation window. Adapter dispatch discriminates the two by
inspecting the JSON shape of `args[0]` (Map → V2; List → legacy);
arity-keyed registry lookup is independently available for future
overloads.

### Added

- **`PluginRegistry::procedure_with_arity(qname, arity)`** — arity-aware
  lookup; falls through to `None` if no overload matches.
- **`PluginRegistry::procedure_overloads(qname)`** — returns every
  registered overload for a qname.
- Procedure-registration now permits multiple entries under the same
  `QName` as long as each has a distinct `signature.args.len()`.
  Duplicate `(qname, arity)` pairs still error with
  `PluginError::DuplicateRegistration`.
- **`AlgoProcedure::execute_with_projection(ctx, args, projection)`**
  (`uni-algo`) — pre-built-projection entry point for V2 Cypher / Named
  graphRef variants. Default impl returns an error; the in-tree
  `GenericAlgoProcedure` overrides it for all 36 built-in algorithms.
- **`GraphProjection::from_rows(node_rows, edge_rows, weight_col, include_reverse)`**
  (`uni-algo`) — build a CSR projection from in-memory row data
  (`Vec<HashMap<String, uni_common::Value>>` shape returned by Cypher
  inner queries).
- **`ProjectionInput` enum + `parse_graph_ref`** (`uni-algo`) — V2
  graphRef map dispatcher (`Native` / `Cypher` / `Named`).
- **`ProjectionStore` + `for_storage(Arc<StorageManager>)`**
  (`uni-query`) — per-`StorageManager` cache of named projections
  backing `uni.graph.{project, drop, list, exists}`.

### Behaviour changes

- `ProjectionInput::Native.include_reverse` and `Cypher.include_reverse`
  default to **true** when omitted from `graphRef`. PageRank / Louvain /
  WCC etc. all require in-neighbors; defaulting false silently
  collapsed scores to the dangling-node baseline.
- The legacy `(nodeLabels, edgeTypes, ...)` shape now emits a one-shot
  `tracing::warn!` per algorithm per process flagging the planned
  removal in M5c.5.

### Migration

No source breakage. External plugins that implement `AlgoProcedure`
gain a default `execute_with_projection` that returns
`AlgoError::ProjectionInputUnsupported`; override it to gain V2 Cypher
/ Named support.

## 1.7.1 — 2026-05-24 — `register_index_handle` host API (M5 Batch 2 follow-up #4)

Additive host-side `PluginRegistry` API for live `IndexHandle` lookup by
index name. Enables the planner to route vector-KNN probes through a
custom `IndexKindProvider`'s handle instead of always dispatching to the
native storage path. The native path remains the fall-through when no
handle is registered (preserving the "no behavior change for built-ins"
invariant).

### Added

- **`pub struct IndexHandleEntry { kind, handle }`** in
  `crates/uni-plugin/src/registry.rs` — `Clone`able lookup payload.
- **`PluginRegistry::register_index_handle(name, kind, handle)`** —
  inserts a handle keyed by index name; replaces on duplicate.
- **`PluginRegistry::index_handle(name) -> Option<IndexHandleEntry>`** —
  cheap clone (inner `handle: Arc<dyn IndexHandle>`).
- **`PluginRegistry::deregister_index_handle(name)`** — removes and
  returns the prior entry.

### Migration

No source breakage. Existing callers that did not interact with index
handles are unaffected.

## 1.7.0 — 2026-05-24 — `OptimizerRuleProvider::physical_rule()` (M5h follow-up #2)

`OptimizerRuleProvider` grows an additive `physical_rule()` method that
returns `Option<Arc<dyn PhysicalOptimizerRule + Send + Sync>>`, enabling
plugin-registered physical-phase optimizer rules to be installed via
DataFusion's `SessionStateBuilder::with_physical_optimizer_rule`. The
default impl returns `None`, so existing logical-only providers compile
unchanged. The `rule()` method also gains a default that returns a
no-op rule, letting physical-only providers omit it. A new
`NoopOptimizerRule` public type backs that default.

### Added

- **`OptimizerRuleProvider::physical_rule()`** in
  `crates/uni-plugin/src/traits/operator.rs` — default `None`.
- **`OptimizerRuleProvider::rule()` gained a default impl** returning
  `Arc::new(NoopOptimizerRule)`.
- **`pub struct NoopOptimizerRule`** in
  `crates/uni-plugin/src/traits/operator.rs` — sentinel logical rule.

### Migration

No source breakage for existing providers — both methods have defaults.
Physical-phase providers should override `physical_rule()`; logical-only
providers continue overriding `rule()` as before.

## 1.6.0 — 2026-05-24 — AlgorithmContext gains opaque host handle (M5c.1)

`AlgorithmContext` now carries an optional `&dyn AlgorithmHost` callback
so plugin algorithms can downcast to the concrete host type
(`StorageManager` + `L0Manager` for the built-in bridge) without
`uni-plugin` taking upward dependencies on `uni-store` / `uni-algo`.
Direct struct-literal construction is forbidden by `#[non_exhaustive]`;
use [`AlgorithmContext::new`] / [`AlgorithmContext::with_host`].

### Added

- **`trait AlgorithmHost: Send + Sync`** in
  `crates/uni-plugin/src/traits/algorithm.rs` — opaque host callback
  with `fn as_any(&self) -> &dyn std::any::Any`.
- **`AlgorithmContext::new(config_json)`** and
  **`AlgorithmContext::with_host(host)`** builders.
- **`AlgorithmContext::host: Option<&'a dyn AlgorithmHost>`** field.

### Migration

`AlgorithmContext` is `#[non_exhaustive]` so direct struct-literal
construction was already forbidden outside the defining crate. Hosts
that previously used `AlgorithmContext { config_json: "…" }` inside
`uni-plugin` itself must switch to the builder.

## 1.5.0 — 2026-05-24 — Lance fork wiring (M5a follow-up #3)

`Storage::fork` grows a per-dataset `table` parameter and returns rich
metadata so callers can chain nested forks. The `LancePluginStorage`
adapter in `uni-plugin-builtin` now overrides both `supports_branching()`
and `fork()` to wire Lance-native branching through the plugin barrier.

### Added

- **`struct BranchMetadata { parent_version: u64, branch_name: String }`**
  in `crates/uni-plugin/src/traits/storage.rs` — surfaces the backend
  version pinned as the fork-point so caller-side nested-fork
  orchestration can chain without re-querying.

### Changed (breaking)

- **`Storage::fork`** signature was
  `async fn fork(&self, src_branch: &str, dst_branch: &str) -> Result<(), FnError>`;
  it is now
  `async fn fork(&self, table: &str, src_branch: &str, dst_branch: &str) -> Result<BranchMetadata, FnError>`.
  Granularity is per-dataset because real branching backends (Lance)
  track branches and versions independently per table. Multi-dataset
  orchestration stays the caller's responsibility (uni-store's
  `BranchedBackend` retains the multi-table coordination it already had).
  The default impl continues to return `FnError 0x10`, so non-branching
  backends are unaffected at runtime — only the signature changes.

### Why this matters

M5 Batch 1 (1.4.0) shipped `LancePluginStorage` but left `fork()` on the
trait's default no-op. M5 follow-up #3 closes that gap so plugin-backed
storage can participate in fork creation. The version field on the
returned metadata is the wire-feasibility bit for future nested-fork
support — callers don't have to round-trip back through the backend to
discover the parent version.

### Version policy

`crates/uni-plugin/Cargo.toml` overrides `version.workspace = true` with
`version = "1.5.0"`; the workspace stays on `1.3.0`. Other workspace
crates remain on the workspace version until they accumulate ABI
additions of their own.

## 1.4.0 — 2026-05-24 — phased context shape v1.1

`crates/uni-plugin/src/traits/hook.rs` grows three additions to surface
real query / commit metadata to phased hooks. All changes are additive
and back-compatible: existing constructors keep their signatures and the
new fields default to zero-valued placeholders.

### Added

- **`enum QueryType { Cypher, Locy, Execute }`** — classification of the
  query under observation. `#[derive(Default)]` (= `Cypher`). Mirrors
  `uni_db::api::hooks::QueryType` without taking a `uni-db` dep
  (circular).
- **`struct PluginCommitResult { mutations, version, wal_lsn, duration }`**
  — slim mirror of the host's commit metadata, surfaced to phased
  `after_commit` hooks. `Default::default()` is all zeros.
- **`ParseContext::query_type: QueryType`** — populated via
  `ParseContext::new(...).with_query_type(t)`; defaults to
  `QueryType::Cypher`.
- **`ParseContext::params: &'a [(SmolStr, ScalarValue)]`** —
  Arrow-shaped bound-parameter slice, populated via
  `ParseContext::new(...).with_params(&[...])`; defaults to `&[]`.
  Chosen over `HashMap<String, Value>` so `uni-plugin` doesn't grow a
  `uni-common` dep.
- **`CommitContext::commit_result: Option<&'a PluginCommitResult>`** —
  `None` in `before_commit`; `Some(_)` in `after_commit` once the host
  bridges the real result through.
- **Builders** on both contexts: `with_query_type`, `with_params`,
  `with_commit_result`.

### Why this matters

The M5e legacy-hook bridge (`LegacyHookAdapter` in `uni-db`) previously
synthesized zero-filled stubs because `ParseContext` carried no
query-type / params and `CommitContext` carried no result. With v1.1,
the bridge can route real values through, so legacy hooks observing the
phased path see the same metadata they'd see through the legacy
`Session::add_hook` HashMap.

### Version policy

This is the first `uni-plugin`-only minor bump since the workspace
adopted unified versioning. `crates/uni-plugin/Cargo.toml` overrides
`version.workspace = true` with an explicit `version = "1.4.0"`. Other
workspace crates stay on `1.3.0` until they accumulate their own ABI
additions. When the workspace later bumps to `1.4.0`, this override is
removed.

## 1.3.0 and earlier

See git history (`git log -- crates/uni-plugin`).
