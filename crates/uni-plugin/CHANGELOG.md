# uni-plugin changelog

All notable additions to `uni-plugin`'s public surface. Versions track
the workspace version unless an entry is annotated otherwise — the
workspace stays on `1.3.x` while individual crates publish additive
v1.4 minor bumps when their ABI grows.

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
