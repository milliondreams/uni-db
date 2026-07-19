# GraphCompute — guest projection & property parity (2026-07-19)

**Status:** Design proposal — implementation-ready. · **Date:** 2026-07-19 · **Trigger:** GitHub issue #151 (guest GraphCompute algorithm can't scope its projection or use edge-property weights). · **Baseline:** HEAD `564b8bbaa` (v3.0.2). · **Lineage:** follows `docs/proposals/graphcompute_plugin_api_2026-07-10.md` (the guest-authorable kernel ABI) and `docs/proposals/python_plugin_abi_gaps_2026-07-16.md` (issue #150, the single-`parse_grant` anti-drift fix this reuses).

---

## 1. Summary

A guest-authored GraphCompute algorithm (Rhai / PyO3 / WASM / Extism) now loads and
runs end-to-end from Python (fixed in #150). But the guest has **no control over the
graph it sees**: the projection is always the whole graph — every label, every edge
type, unweighted, `include_reverse = true`. Issue #151 reports two concrete
consequences and one absence:

- **OBS 1** — the algorithm cannot scope its projection to chosen labels / edge-types
  (returns all 4 nodes, not the 3 `:A` nodes).
- **OBS 2** — `gc.edge_weights(g)` returns `1.0` for every edge even when edges carry
  a numeric property (`reduce_sum` = 3.0, the edge count, not 15.0 = Σ `w`).
- **Absence** — there is no way to read a **node** property into a per-vertex tensor
  (a vertex analogue of `edge_weights`).

Both reproduce against the shipped 3.0.2 wheel (repro exits 0). This proposal reframes
them as a single class of defect — a **guest/native symmetry-contract violation** — and
fixes the class, not the two instances, in three tiers plus a compile-time contract that
makes the next such gap impossible to ship silently.

The framing matters: #150 ("can't load") and #151 ("can't shape the graph") are the same
disease surfacing one GitHub issue at a time — the guest algorithm surface is a **strict,
unenumerated subset** of the native `uni.algo.*` surface, and nothing enforces or even
documents where the subset boundary is. A point fix adds a fifth projection-config parser
and guarantees a #152. The systematic move is a single source-of-truth parser, a capability
invariant that closes the security bypass by construction, a design-consistent property
primitive, and an exhaustiveness contract that pins the guest/native delta in the type
system.

## 2. Evidence (file:line)

### 2.1 Reproduction (confirmed)

`bindings/uni-db` 3.0.2 wheel; the #151 repro loads a Rhai algorithm that sums
`gc.edge_weights(g)` over a 3×`:A` + 1×`:B` graph with 3 `:LINK` edges each `w = 5.0`.
Output: 4 projected rows (not 3), `reduce_sum(edge_weights)` = 3.0 (not 15.0). Exit 0
(both observations reproduced).

### 2.2 OBS 1 root cause — every guest adapter hardcodes an unscoped spec

Identical block in all four adapters — the guest's decoded `config_json` is passed to the
guest *function* but never into the projection spec:

- `crates/uni-plugin-rhai/src/adapter_algorithm.rs:99-105` (`// v1: project the whole graph`)
- `crates/uni-plugin-pyo3/src/adapter_algorithm.rs:99-103`
- `crates/uni-plugin-wasm/src/adapter_algorithm.rs:97-101`
- `crates/uni-plugin-extism/src/adapter_algorithm.rs:109-113`

```rust
let spec = GraphProjectionSpec {
    include_reverse: true, // enable In-direction kernels (WCC/k-core/HITS)
    ..GraphProjectionSpec::default()   // node_labels = [], edge_types = [] => ALL
};
let projection = bridge.project_for_graph_compute(&spec);
```

The scoping fields already exist and are honored end-to-end:
`GraphProjectionSpec { node_labels, edge_types, weight_property, include_reverse }`
(`crates/uni-plugin/src/traits/algorithm.rs:320-330`); the bridge feeds all four into
`ProjectionBuilder` (`crates/uni-plugin-builtin/src/algorithms/bridge.rs:200-209`); empty
selects all (`crates/uni-algo/src/algo/projection.rs:26-35`). The native
`uni.algo.gcpagerank` provider already parses `nodeLabels`/`edgeTypes` from its config JSON
(`crates/uni-plugin-builtin/src/algorithms/graph_compute/provider.rs:153-167`). The guest
path simply omits that parse.

### 2.3 OBS 2 root cause — the property is dropped at projection time, not at the kernel

The `edge_weights` kernel is correct: it reads `graph.out_weight(u, k)`
(`crates/uni-plugin-builtin/src/algorithms/graph_compute/session.rs:1589-1602`). The `1.0`
is injected one layer below. In `ProjectionBuilder::build`, the OUT CSR is always built
with weights on (`crates/uni-algo/src/algo/projection.rs:259`), but the weight *cache* is
populated only when `weight_property.is_some()` (`projection.rs:419`); otherwise each edge
falls to `weights_cache.get(&eid).copied().unwrap_or(1.0)` (`projection.rs:471`, `:481`).
The bridge honors `spec.weight_property` (`bridge.rs:207-209`), and native weighted algos
wire it via a `weightProperty` config key → `builder.weight_property(prop)`
(`crates/uni-algo/src/algo/procedure_template.rs:174`). No guest path — and not even
`provider.rs::parse_config` for `gcpagerank` — ever sets it.

### 2.4 Latent security bypass — scope enforcement is defeated by the unscoped default

`bridge.rs:176-195` rejects only **named** labels/edge-types outside the granted
`HostQuery` scopes. Because the guest spec is empty (= all), the deny-loop iterates nothing
and passes — a plugin granted a narrow `HostQuery` scope (e.g. `Person`) still gets the
whole graph, directly contradicting the comment at `bridge.rs:162-165`. This is not
incidental; it is structural (§4.2).

### 2.5 Enforcement gap — no test exercises a shaped projection

Every GraphCompute *algorithm* row in the conformance suite calls `gc.graph()` with no args
on a single-label/single-edge-type graph with no numeric property, and asserts parity only
against the default native call `uni.algo.gcpagerank(vid, 0.85)` (no projection args):
`bindings/uni-db/tests/test_plugin_conformance.py:171, 180-183, 218-228, 420-433`;
`bindings/uni-db/tests/test_graph_compute_plugin.py:41, 123-142`. No test passes a scoped
or weighted projection. This is exactly the hole #151 fell through.

## 3. Root cause: an unenforced guest/native symmetry contract

Projection configuration is parsed in **at least three places** today, each an independent
copy of the same intent:

1. `crates/uni-algo/src/projection_input.rs:99-185` — the Cypher-facing `graphRef` parser
   (Native / Cypher / Named modes).
2. `crates/uni-plugin-builtin/src/algorithms/graph_compute/provider.rs:153-167` — the
   `gcpagerank` provider (Native only; missing `weightProperty`).
3. `crates/uni-algo/src/algo/procedure_template.rs` (`:159-177`, `:210-212`) — the native
   procedure adapters.

The four guest adapters (§2.2) are a **fourth, stub** copy that parses nothing. The bug is
not "a guest forgot to parse" — it is "there are N parsers and the guest got the empty one."
This is the identical disease #150 diagnosed for capability grants, where the fix was a
single `Capability::parse_grant` feeding every loader (`python_plugin_abi_gaps_2026-07-16.md`).
The same remedy applies here. The deeper failure is that **nothing declares what a guest
algorithm may or may not express** relative to native — the subset boundary lives only in
the collective memory of whoever last touched the adapters. §5.4 makes that boundary a
compile-time contract.

## 4. The complete guest/native projection delta

Validated by exhaustive read of `ProjectionConfig`/`GraphProjectionSpec`/`ProjectionBuilder`
and every projection-touching native config key. This is the *full* surface, not a sample —
the two OBS in #151 are 2 of the rows below.

| Projection knob | Native source | Guest-reachable today | Cost to expose | Tier |
|---|---|---|---|---|
| `nodeLabels` | `procedure_template.rs:210`; builder `projection.rs:198` | **No** (default `[]`) | Parse guest config → `spec.node_labels`; already carried by `project_for_graph_compute` (`bridge.rs:200-206`). No host change. | **P1** |
| `edgeTypes` / `relationshipTypes` | `procedure_template.rs:211`; `projection.rs:204` | **No** (default `[]`) | Same → `spec.edge_types`. Host-ready. | **P1** |
| `weightProperty` | `cypher/dijkstra.rs:27` et al.; `projection.rs:210` | **No** (`None`) | Parse → `spec.weight_property`; applied at `bridge.rs:207-209`. Host-ready. Fixes OBS 2. | **P1** |
| `includeReverse` | `projection_input.rs:137-141`; `projection.rs:216` | **Partial** — hardwired `true` | Read from guest config → `spec.include_reverse`. Trivial. | **P1** |
| **node property → `[V]` tensor** | *does not exist for anyone* | **No** | New projection-time config field + build-time fetch (slot→Vid) + `gc.node_property` kernel. Design-consistent (§5.3). | **P2** |
| **edge property → `[E]` tensor** (general, beyond the single weight) | *does not exist for anyone* | **No** | New projection-time config field + build-time fetch (Eid available only here) + `gc.edge_property` kernel; `edge_weights` becomes sugar. | **P2** |
| Cypher mode (`nodeQuery`/`edgeQuery`/`weightColumn`) | `projection_input.rs:148-176`; resolved in `uni-query` `algo.rs:206-252` | **No** — `GraphProjectionSpec` cannot express it; resolution lives only in `uni-query` | Add a query variant to the host surface + move/mirror `execute_inner_query` + `from_rows` into an `AlgorithmHost` path. Crosses the `uni-plugin`/`uni-query` boundary. | **P3** |
| Named mode (`name` → `ProjectionStore`) | `projection_input.rs:177-184`; `uni-query` `graph.rs`, `projection_store.rs` | **No** — `ProjectionStore` is unreachable from `AlgorithmHost` | Add a `name` variant/method + give the bridge access to the per-`StorageManager` store. Crosses the crate boundary. | **P3** |
| default weight, `orientation`/undirected, parallel-edge `aggregation`, `concurrency` | *do not exist for native callers either* | **No** | Net-new native features first; not a guest-parity gap. | **Out of scope** (§7) |

**Reading of the table:** the four Native knobs (rows 1–4) are already plumbed through
`project_for_graph_compute` and need only guest-config parsing — this is the cheap,
high-value tier that closes both reported OBS. The two property-tensor rows are the real
generalization the issue's third point asks for and are feasible **only** at projection
time (§5.3). Cypher/Named are structurally unreachable and demand-gated (§7).

### 4.2 Why the bypass is structural

The projection scope (built in the adapter, §2.2) and the capability scope (checked in the
bridge, §2.4) are decided in two unrelated places with no invariant tying them. "Unspecified
scope" currently means "everything," which is exactly the value that defeats the check. The
fix is not another guard clause — it is an **invariant** on the shared projection path
(§5.2): an unspecified scope resolves to *the granted scope*, never to "all." Wire it once
and every loader inherits it, the same way the fork subsystem centralizes its invariants
(`BranchedBackend` routing, single-writer registry).

## 5. Design

### 5.1 WS-1 — one `GraphProjectionSpec` parser (source of truth)

Add a single constructor, colocated with the spec it builds
(`crates/uni-plugin/src/traits/algorithm.rs`):

```rust
impl GraphProjectionSpec {
    /// Parse the Native-mode projection knobs from a guest/procedure config object.
    /// The single source of truth for nodeLabels / edgeTypes / weightProperty /
    /// includeReverse across every native provider and every guest loader.
    pub fn from_config_object(cfg: &serde_json::Map<String, serde_json::Value>) -> Self { … }
}
```

- Parses `nodeLabels`, `edgeTypes` (accepting the `relationshipTypes` alias — native
  procedures already treat them as synonyms, `procedure_template.rs:211`), `weightProperty`,
  and `includeReverse` (default `true`, matching `projection_input.rs:141`).
- **Refactor the existing parsers to call it**: `provider.rs::parse_config`
  (`:153-167`) — and while there, add the missing `weightProperty` key so `gcpagerank`
  gains weighted projection too. This is the anti-drift step: after WS-1 there is exactly
  one place that knows the Native knob names.
- **Wire it into all four guest adapters.** The wire format is already a positional JSON
  array (`json_args`, e.g. `adapter_algorithm.rs:89-97`); native `gcpagerank` already treats
  the trailing object as the projection config (`provider.rs:154`, `args.get(2)`). Adopt the
  same convention: the guest's trailing config object (if present) feeds
  `GraphProjectionSpec::from_config_object` **before** `project_for_graph_compute`; the
  remaining positional args still pass to the guest function unchanged. `include_reverse`
  defaults to `true` (preserving today's kernel behavior) unless the guest overrides it.

Result: OBS 1 and OBS 2 close together, on all four loaders, from one parser.

### 5.2 WS-2 — the projection-capability invariant (closes the bypass by construction)

In the shared bridge path (`bridge.rs`, the `HostQuery`-scope block at `:166-195`), change
the semantics of an unscoped spec under a restricted grant: when `scope_prefixes` is
non-empty **and** `spec.node_labels` / `spec.edge_types` are empty, resolve the projection to
the granted scope (narrow to the labels/edge-types the scopes permit) rather than letting
"all" through. A named label outside scope still errors `0x804` as today. This makes
"unspecified = the grant, never everything" a property of the one code path both native and
guest projection flow through.

### 5.3 WS-3 — property → tensor, the design-consistent way (projection-time binding)

The pivotal validated constraint: **`AlgoSession` holds no storage** — no `PropertyManager`,
`StorageManager`, or schema (`session.rs:8-12, 198-237`); a kernel's entire universe is the
materialized `Arc<GraphProjection>` behind its handle (`bind_graph`, `session.rs:266-271`).
And `GraphProjection` **discards every `Eid` during build** — the finished CSR stores
slot→slot adjacency + optional `[E]` f64 weights only (`projection.rs:37-56`); edge identity
survives just long enough to fetch weights in `collect_edges` (`projection.rs:389, 451-475`),
then is gone. Slot→`Vid` *is* retained via `id_map` (`projection.rs:143-153`).

Consequences that fix the design:

- A **runtime** `bind_edge_property(g, name)` kernel is **impossible** — there is no `Eid`
  left to fetch by, and the kernel dispatch path is synchronous while property reads are
  `async` (`dispatch.rs:269, 324`; `property_manager.rs:747` is `async`).
- A **runtime** node-property kernel would require putting storage back inside the session
  and going async — breaking the explicit storage-free / deterministic-projection invariant.
- The build path already holds `StorageManager` + `PropertyManager`, runs `async`, has both
  `Vid`s and `Eid`s in hand, and **already does exactly this** for `weight_property`
  (`projection.rs:419-463`).

Therefore property binding is **projection-time**, mirroring how weights already work:

1. Extend `ProjectionConfig` / `GraphProjectionSpec` with
   `node_properties: Vec<String>` and `edge_properties: Vec<String>` (names the guest
   declares in its config object, parsed by WS-1's parser).
2. In `ProjectionBuilder::build`, batch-fetch node props by `Vid`
   (`get_batch_vertex_props`) and edge props alongside the existing weight fetch in
   `collect_edges`, storing the results as named `[V]` / `[E]` `Vec<f64>` columns on
   `GraphProjection` (parallel to `out_weights`).
3. Add two kernels — `gc.node_property(g, name)` → `[V]` tensor, `gc.edge_property(g, name)`
   → `[E]` tensor — that mint a tensor from the pre-materialized column exactly as
   `edge_weights` does (`session.rs:1589-1602`, `alloc_tensor`). Charge `|V|` / `|E|` work
   and account tensor bytes through the existing arena discipline (`session.rs:383-406`).
4. Redefine `gc.edge_weights(g)` as **sugar** for "the edge tensor named by `weightProperty`"
   — one implementation, so weights and arbitrary edge properties can never diverge.

Only aggregate f64 tensors cross the guest boundary (never raw rows), so the isolation
invariant is *preserved*, not weakened. This is the "conductor, not worker" contract from
the GraphCompute proposal: the guest declares what data it needs; the host does the typed
bulk movement at build time.

### 5.4 WS-4 — the symmetry-contract test (enforcement, mirrors the capability exhaustiveness contract)

The capability system pins its taxonomy with a two-layer contract: a wildcard-free `match`
in `grant_name` that **fails to compile** when a new `Capability` variant is unclassified
(`capability.rs:615-662`), plus a runtime length + exactly-once assertion
(`every_variant_classified_exactly_once`, `:970-991`). Mirror it for projection knobs:

- Introduce a `ProjectionKnob` enum enumerating every knob in the §4 table.
- A wildcard-free `match` classifying each into
  `{ GuestReachable, HostOnly { reason }, NonexistentNative }` — adding a knob without
  classifying it **won't compile**.
- A test asserting each knob is classified exactly once and that every `GuestReachable`
  knob is actually parsed by WS-1's `from_config_object` (round-trip), so a knob can never
  be declared guest-reachable yet silently unparsed — the precise failure that produced #151.

This converts "guest surface ⊆ native surface" from tribal knowledge into a checked
relationship. It is the anti-#152 mechanism.

### 5.5 WS-5 — conformance graph-shaping axis

Extend `bindings/uni-db/tests/test_plugin_conformance.py` (the P3 suite from #150) with a
graph-shaping axis over the existing loader × kind matrix. Using the #151 fixture graph
(2 labels, edge property `w = 5.0`):

- **Scoped projection**: `nodeLabels=["A"]` → guest algorithm sees 3 rows, not 4 (OBS 1).
- **Weighted projection**: `weightProperty="w"` → `reduce_sum(edge_weights)` = 15.0, not 3.0
  (OBS 2), and parity against `uni.algo.gcpagerank(vid, 0.85, {weightProperty:"w"})`.
- **Node/edge property tensor** (P2): `gc.node_property` / `gc.edge_property` return the
  declared property values; `edge_weights` == `edge_property("w")`.
- **Scope-bypass denial** (P1/WS-2): a plugin granted `HostQuery{scopes:["A"]}` that leaves
  the spec unscoped is narrowed to `:A` (or an out-of-scope named label errors `0x804`) —
  it must not silently receive `:B`.

Rows run under the existing `python-tests` CI job; fixture-dependent rows skip cleanly when
wasm/extism artifacts are absent (existing `os.path.exists` guard).

## 6. Phasing

- **P1 (closes both reported OBS + the security bypass) — WS-1, WS-2, WS-5(scoped/weighted/deny):**
  the single parser, the capability invariant, the conformance shaping rows. Small, local to
  the four adapters + `bridge.rs` + `provider.rs`, no ABI change. This is the tier that
  resolves #151's headline complaints and the latent bypass.
- **P2 (the property-tensor generalization) — WS-3, WS-4, WS-5(property rows):** the
  projection-time node/edge property binding, `edge_weights` as sugar, and the
  `ProjectionKnob` exhaustiveness contract. Touches the kernel ABI (two new kernels) and the
  projection struct; larger but design-consistent.
- **P3 (guest Cypher/Named projection) — demand-gated:** only if a concrete workload needs a
  guest algorithm over a Cypher-defined or pre-registered named subgraph. Requires crossing
  the `uni-plugin`/`uni-query` boundary (host trait + inner-query capability wiring); scope
  separately when motivated.

## 7. Non-goals / out of scope

- **Cypher/Named guest projection (P3)** is deferred, not designed here beyond the delta
  entry — it is structurally larger (crate-boundary crossing) and unproven in demand.
- **Net-new native projection features** — configurable default weight, `orientation` /
  undirected symmetric CSR, parallel-edge `aggregation`, `concurrency` — do not exist for
  native callers either, so they are native-feature requests, not guest-parity gaps. Excluded
  unless separately motivated.
- **Quota self-escalation hardening (R3)** and the **schemaless-GraphCompute-projection
  sharp-edge** (native `gcpagerank` projects empty on a schemaless graph, noted in the P3
  suite comment `test_plugin_conformance.py:99-102`) remain separate tracked candidates.

## 8. Verification / acceptance

1. `cargo nextest run -p uni-plugin` — new `ProjectionKnob` exhaustiveness + round-trip test
   (WS-4) passes; a deliberately-unclassified knob fails to compile (manual check).
2. `cargo nextest run -p uni-plugin-rhai -p uni-plugin-pyo3 -p uni-plugin-wasm -p uni-plugin-extism -p uni`
   — the shared parser + capability invariant don't regress the native `*_graph_compute`
   reference tests.
3. `cargo fmt --all -- --check`; `cargo clippy --all-targets -- -D warnings`
   (`RUSTC_WRAPPER=""`, `TMPDIR` under `target/`).
4. Rebuild binding (`cd bindings/uni-db && uv run maturin develop`); build fixtures
   (`scripts/build-wasm-fixtures.sh`).
5. `uv run pytest tests/test_plugin_conformance.py tests/test_graph_compute_plugin.py -v` —
   the graph-shaping axis is green: scoped → 3 rows; weighted → Σ = 15.0 with parity;
   property tensors present (P2); scope-bypass denied.
6. The #151 standalone repro now exits **1** (both observations no longer reproduce) once P1
   lands — pin it as a regression fixture.
7. `docs/` (per the `uv`/`zensical` toolchain, not poetry): capability/plugin reference tables
   updated to list the guest-reachable projection knobs and the guest/native boundary.

## 9. Risks & decisions

- **Config-object convention.** P1 adopts "trailing JSON object in the positional args is the
  projection config," matching native `gcpagerank` (`provider.rs:154`). Alternative — a
  reserved key or a distinct `gc.graph(config)` guest call — is rejected for P1 as a larger
  surface; revisit only if guests need runtime (post-arg) projection choice.
- **`include_reverse` default flip.** Today it is hardwired `true` to enable In-direction
  kernels. WS-1 keeps `true` as the default and only lets the guest *override*; changing the
  default is out of scope (would alter existing guest algorithms' reverse CSR).
- **Projection-time vs runtime property binding** is not a preference — it is forced by the
  Eid-discard + storage-free-session findings (§5.3). A future runtime kernel would require
  retaining `Vec<Eid>` in CSR order and re-plumbing storage into the session; explicitly not
  pursued.
- **Contract-test maintenance.** `ProjectionKnob` must enumerate real knobs; the wildcard-free
  match makes drift a compile error, matching the capability precedent, so the maintenance
  burden is the same one the codebase already accepts for capabilities.

## 10. Implementation status (landed 2026-07-19)

All three tiers implemented and verified against the `uni-db` binding.

- **P1** — `GraphProjectionSpec::from_config_object` + `take_from_args`
  (`crates/uni-plugin/src/traits/algorithm.rs`) is the single Native-knob parser; the four
  `adapter_algorithm.rs` loaders and native `gcpagerank` (`provider.rs`) route through it.
  The fail-closed scope invariant is in `bridge.rs::project_for_graph_compute`, with a `**`/`*`
  wildcard treated as unrestricted (the Python `"HostQuery"` grant string parses to `["**"]`).
- **P2** — `ProjectionConfig`/`GraphProjection` carry `node_properties`/`edge_properties`
  columns; `collect_edges` fetches them alongside the weight, and `build_csr` co-permutes every
  edge-property column through one canonical `(dst, weight, props…)` permutation. Kernels
  `gc.node_property` / `gc.edge_property` (`session.rs` + `dispatch.rs`, reusing `req.name`)
  expose them; `edge_weights` stays a distinct traversal-weight channel (default `1.0`) while
  property tensors default to `NaN`. The `ProjectionKnob` exhaustiveness contract locks the
  guest/native surface at compile time.
- **P3 — refinement of §6.** The guest-dispatch seam (`run_algorithm_provider`) is *sync* while
  a Cypher projection build is *async*, so the finished-projection injection §6 sketched is
  replaced by **resolver injection**: a `GraphProjectionResolver` trait (in `bridge.rs`, the
  uni-plugin-builtin side of the dependency edge) is implemented by a uni-query newtype wrapping
  `QueryProcedureHost`, and the bridge awaits it inside its own always-async
  `project_for_graph_compute`. Both call paths (the DataFusion `procedure_call.rs` and the
  simple-executor `procedure.rs`) construct the resolver synchronously; a Cypher/Named graphRef
  with no resolver errors clearly (`0x820`) rather than silently projecting the whole graph.
- **Known limitation (storage-view).** The projection reads edge/vertex *property values*, and
  the P3 resolver's inner Cypher queries, from committed storage — L0-buffered property values
  need a flush (matching the native weighted `uni.algo.*` path, which shares `ProjectionBuilder`
  / `collect_edges`). Topology is L0-aware; property-*value* L0 visibility is a separate, broader
  gap tracked outside this issue.
- **Tests.** `bindings/uni-db/tests/test_plugin_conformance.py` (scoped / weighted / node+edge
  property / Cypher / Named rows, plus a not-projected error); the fail-closed scope reject is a
  Rust test (`crates/uni/tests/common/graph_algo/graph_compute_pagerank.rs`) because the Python
  grant string cannot express a narrow HostQuery scope.
