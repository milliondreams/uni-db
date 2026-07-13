# GraphCompute — a guest-authorable coarse-kernel graph API (2026-07-10)

**Status:** Design proposal — **implementation-ready**. Revised 2026-07-10 after a 6-stream verification + adversarial design review (all `file:line` citations re-verified at HEAD `ae9eb1c68` / v3.5.0; kernel catalog dry-run against every claimed algorithm and repaired). Handoff surface: guest contract §4.6, acceptance tests + anti-gaming rules §9, ratified decisions §11, pinned constants §12, phase sizing §8.
**Author context:** Grounded by a 5-stream code+literature audit (graph internals, loader binding mechanisms, external prior art, use-case demand, adversarial pitfalls). Every claim about the current tree is cited `file:line`.
**Supersedes the P3-tier deferral** in `docs/proposals/plugin_framework_gaps_2026-07-07.md` (§9 tier P3: guest algorithms "gated/niche… pure-compute breadth only"; §0: "no guest loader parses" the `Algorithm` kind). That deferral was calibrated to *the current `GraphView` API shape* (borrowed slices, in-process only), not to a fundamental limit. This proposal shows guest-authored graph algorithms are tractable across **all four** loaders once we stop moving graph data and move only opaque handles — and is itself the "trigger" that tier asked to wait for.

---

## 1. Thesis

Let third parties author graph algorithms — PageRank, reachability, community detection, GraphRAG retrieval — as **plugins in Rhai / Python / WASM / Extism**, without forking uni and without shipping Rust.

The design principle is **conductor, not worker**: the guest runs only the algorithm's control-flow *skeleton* (a loop of O(iterations) coarse calls); native Rust does all O(V+E) work. The boundary carries only **opaque handles + scalars** — never frontiers, neighbor lists, or property columns. This is the NumPy/GraphBLAS lesson ("don't loop *in* the interpreter, loop *over* it") applied to graphs.

Because only handles and scalars cross, **the same design works uniformly across every loader**, including the linear-memory sandboxes (WASM/Extism) where "hand the guest a `&[u32]`" is impossible. The Rhai-vs-WASM difficulty gap that motivated deferring this feature *disappears* once no data crosses.

### Why now / why this is cheap

The substrate already ships (GraphView P0 in `fe64b48f5`, `run_pregel` in 3.1.0 `78da69c8d`; verified at HEAD `ae9eb1c68`, v3.5.0):

- `GraphProjection` (`crates/uni-algo/src/algo/projection.rs:39`) is an **immutable dense-`u32`-slot CSR** behind an `Arc` — textbook `out_offsets`/`out_neighbors`/`in_*`/`out_weights` + `IdMap`. It is the ideal input to frontier/SpMV/reduction kernels.
- It is already exposed to plugins as `GraphView` (`crates/uni-plugin/src/traits/algorithm.rs:145`), already **`HostQuery`-gated** at the bridge (`crates/uni-plugin-builtin/src/algorithms/bridge.rs:127`, `FnError 0x804` on miss at `:142`).
- `run_pregel` (`crates/uni-plugin-builtin/src/algorithms/pregel.rs:97`) already performs the **exact kernel set internally** per superstep (scatter → combine → active-set; the combiner *is* the reduction — it folds messages bound for the same target). PageRank and SSSP are authored on top of it. **This proposal is essentially "expose `run_pregel`'s inner ops as a guest-drivable catalog."**
- The result path (`bridge.rs:294` `build_record_batch`, per-`ValueType` Arrow builders) is a ready-made `emit` sink.

The genuine gap is narrow: **no guest loader is wired to the graph surface** (grep for `GraphView|GraphCompute` across `uni-plugin-rhai/wasm/extism/pyo3` returns zero hits), and there is **no first-party handle table** anywhere in the workspace (grep for `SlotMap|generational|HandleTable|Arena` returns nothing in `crates/`; `slab`/`sharded-slab` appear only as transitive deps; only `SecretHandle(u64)` — `crates/uni-plugin/src/secrets.rs:32` — exists as a handle-registry precedent). This proposal fills exactly those two holes plus the enforcement gaps the adversarial review surfaced.

---

## 2. Prior art we are stealing from (and the lessons)

| Source | What we take | The load-bearing lesson |
|---|---|---|
| **GraphBLAS** (SuiteSparse) | one `spmv`/`vxm` kernel + a **semiring** re-purposes it for BFS/SSSP/PageRank; **mask** fuses the visited-set filter into the multiply | *Name the intent, not the algebra.* A wrong identity silently gives wrong answers — so the surface says `reachability`/`shortest_path`/`propagate`, and we derive the monoid. Carry mask/accum as **named optional** args, never GraphBLAS's 9 positional slots. |
| **Ligra / GBBS** | `edgeMap`/`vertexMap`/`vertexFilter`; sparse-vs-dense frontier; **direction-optimization inside the kernel**; `cond` early-exit; typed frontier payload | *Never expose one direction.* Push-vs-pull must be auto-selected by the kernel (Beamer threshold); a one-direction primitive cripples scale-free graphs. `cond` early-exit is what bounds work. |
| **Neo4j GDS Pregel** | **two-step `project → named handle → run in modes`**; combiner is first-class & host-applied; typed value-schema → dense columnar storage; `.mutate` writes back = the **composition primitive** | *The guest names only `nodeId/degree/neighbors` + typed slots* — never storage, threads, or wire format. That portability is exactly what lets one kernel run under four loaders. |
| **WASI Preview 2 / wasmtime resources** | opaque `i32` = index into a per-instance **handle table**; generational; `own` vs `borrow`; **reclaim on guest death**; type your handles | *A handle is an unforgeable capability.* Structural unforgeability (host validates every table access) is what makes the security model sound. Never a raw pointer, never a guessable id. |
| **NumPy / JAX / Spark GraphX / DataFusion** | thin-driver/fat-kernel; **batch the escape hatch** (per-batch UDF, not per-row); structured control primitives (`lax.scan`, `while_loop`); COST discipline | *The per-element callback is the wall.* Every one of these systems collapses when custom per-element logic enters the hot loop. If the guest interpreter runs once per vertex, we have already lost. |

Ranked pitfalls the design must pre-empt (from the literature): **P1** the per-vertex-logic wall; **P2** kernel/semiring ABI explosion (use combinators, not one symbol per combination); **P3** intermediate materialization (fuse the mask); **P4** direction-optimization (engine-owned); **P5** power-law super-node skew (GAS-shaped kernels); **P6** COST (benchmark every kernel vs a single-threaded cache-friendly loop); **P7** determinism (a host-kernel contract the guest can't fix through an opaque handle).

The cross-cutting synthesis: **most failures are the same mistake — baking into the guest ABI a dimension the engine should own** (direction, cardinality, op×type×semiring, masking, per-entity work granularity). Keep all of these engine-owned.

---

## 3. Demand — what this unlocks (and what stays native)

Classification of the algorithm space by fit with the coarse-kernel model — **F** = clean fit (iterated spmv + reduce + converge), **C** = needs one extra affordance, **M** = structurally outside (mutation/sequential):

| Class | Algorithms | Verdict |
|---|---|---|
| **F** (author freely against the core catalog, §4.3 groups 0–8) | BFS / k-hop / reachability, unweighted SP, Bellman-Ford, **PageRank / Personalized PageRank / HITS / eigenvector / Katz**, WCC / min-label propagation, k-core (peel via `degrees`+set ops+`spmv` recount), random-walk generation (node2vec/DeepWalk walks via `random_walks`) | ~70% of demand from the core catalog. **Caveat honored by design:** each of these was pseudocode-dry-run against the literal §4.3 signatures; PageRank-family, Bellman-Ford, and WCC are only expressible *because* of the group-0 plumbing kernels (`degrees`, `vertex_ids`, `ewise`, set↔map) — an earlier catalog draft without them could not express half this row. |
| **C** (one starred kernel each) | Dijkstra / Δ-stepping (`next_bucket`), Brandes betweenness (`bfs_levels` + `reverse_accumulate`), triangle / k-truss + neighbourhood similarity — Jaccard/cosine/Adamic-Adar (`neighborhood_overlap`: these are *pairwise*-state per the §3.1 dividing test, so they need a bulk host kernel, never guest loops), LPA majority-vote (needs a dedicated step kernel — majority is not an associative semiring), KNN top-K (`topk`) | → ~90% with the 4 starred kernels |
| **M** (stay native, reachable via `as_any` downcast) | Louvain / Leiden (needs graph coarsening), DFS / Tarjan-SCC / general cycle detection (P-complete, sequential), SimRank (O(n²) pair-indexed state), embedding *training* (SGD, not a graph kernel) | out of scope by design |

**Domain → algorithm** (grounded in uni's own `examples/`): fraud → cycles + WCC/Louvain + PPR + k-core; recommendation → **PPR** + node2vec + SimRank + common-neighbours; knowledge-graph → reachability + PPR; **GraphRAG** → vector-KNN seed → PPR/expand → RRF fuse (HippoRAG/Microsoft GraphRAG); cyber → reachability/attack-path + centrality; temporal → time-respecting BFS/Dijkstra.

**Single highest-leverage algorithm: Personalized PageRank** — it recurs across fraud, reco, KG, *and* GraphRAG. Ship the kernel set that makes PPR a ~15-line guest script and most of the demand follows.

> **Demand signal from the tree:** uni already has exactly 37 native `uni.algo.*` algorithms, but **zero example notebooks call them** — users reach for Cypher variable-length paths and Locy recursion instead. They want to *author* graph computation in their own layer. That is precisely what a guest-facing API provides.

### 3.1 Non-goals — what stays out, and why that is correct

Three boundaries are deliberate, not gaps to close:

- **Sequential / mutation / enumeration algorithms stay native.** The M-class names the specific casualties from uni's *own* library: **Louvain / Leiden** (needs graph coarsening), **Tarjan-SCC**, **articulation-points / bridges** (DFS-lowlink), **maximal-clique enumeration** (backtracking), and **SimRank** (pairwise V² state). These remain authorable only as native `uni.algo.*`, reachable from a plugin via the `as_any()` downcast — never as kernels. This is the same line GraphBLAS/LAGraph draw (they too keep Louvain and SCC-enumeration as hand-written code, not linear algebra).
- **Columnar analytics is DataFusion's job, not this API's.** SUM/AVG/GROUP BY/filter/join over property columns already runs through uni's vectorized Arrow pipeline, extensible via the *already-shipped* scalar/aggregate/window plugin UDFs. GraphCompute must **compose** with it — `emit` hands a per-vertex topological result back into DataFusion for the columnar rollup — not reinvent OLAP. The one graph-shaped thing in scope that is easily confused with columnar analytics is **neighbourhood aggregation** (per-vertex rollup over the CSR), which is a segment-reduce *kernel*, distinct from a group-by, and is the bridge to GNNs (§10).
- **The dividing test:** an algorithm is a kernel iff it can be written as `state_{k+1} = f(A ⊗ state_k)` with per-vertex `state` and commutative-associative `f`. Pairwise state, graph mutation, strict ordering, or instance materialization put it outside — no amount of kernel-set growth changes that.

---

## 4. Architecture

### 4.1 The three objects

```
  guest script (Rhai/Py/WASM/Extism)          ── holds ──▶  Handle(u64)  [opaque]
        │  calls coarse kernels (u64 in, u64 out, scalars)
        ▼
  GraphCompute  (host trait; the kernel catalog)
        │  operates on
        ▼
  AlgoSession   (per-invocation arena: the handle table + budget + graph)
        │  wraps
        ▼
  Arc<GraphProjection>   (immutable CSR — already exists)
```

- **`GraphProjection`** — unchanged; the immutable CSR (with the determinism fix in §5.3).
- **`AlgoSession`** — **new**, per-CALL, host-owned. Holds the generational handle table, the projected graph(s), the native-work budget, and the determinism/seed config. Dropped when `run()` returns → all handles freed, no leaks. Natural home: a field on the existing transient `AlgorithmHostBridge` (`bridge.rs:96`), which is already built-and-dropped per CALL and already owns `StorageManager`/`L0`/caps.
- **`GraphCompute`** — **new** host trait; the kernel catalog. Every method takes/returns handles + scalars and a `Result` (never panics). This is a *parallel* surface to `GraphView`, **not** an extension of it — `GraphView::out_neighbors -> &[u32]` returns a borrowed slice that fundamentally cannot cross a WASM boundary, so the guest-facing trait must be handle-only.

**Two invariants make the whole thing forward-extensible (see §10) — adopt both in v1:**
- **Arrow-backed values.** Every value handle's buffer is an Arrow array, so a `[V]`-shaped map *is* a DataFusion column: the graph↔columnar boundary becomes a zero-copy view, not a marshal. (Nearly free — the result path already builds Arrow arrays.)
- **Functionally pure kernels.** Guest-visible kernels return a *new* handle; no in-place mutation (the host may optimize with copy-on-write internally). Purity + explicit handles means a future op-tape is trivially recordable — the enabler for autodiff/training. The v1 cost is extra allocation, mitigated by COW and accounted for by the native-work budget (§5.1).

### 4.2 The handle model (mitigation for adversarial §1)

Handles are **generational, kind-tagged, session-scoped `u64`s** (the WASI-resource model) and — crucially — the value kind is a **shaped, typed tensor**, not a scalar-only map. v1 implements only the `[V]` scalar case, but the taxonomy speaks tensors from day one so embeddings/weights bolt on **additively** (§10), never as a breaking handle-kind change:

```rust
/// Packed: [ session_epoch:16 | kind:4 | generation:12 | slot:32 ]
#[derive(Clone, Copy)]
pub struct Handle(u64);

pub enum HandleKind { VertexSet, Tensor, Graph, Walks, Levels }

/// Shaped + Arrow-backed. v1 uses only Shape::V (the scalar value-map);
/// Vd (embeddings) and Dd (free weight matrices) are RESERVED, unimplemented.
pub enum Shape { V, Vd(u32), D(u32), Dd(u32, u32) }
pub enum DType { F32, F64, I64, U32, Bool }
pub struct Tensor { shape: Shape, dtype: DType, buf: ArrowBuffer }

struct HandleTable {
    session_epoch: u16,                          // forged/stale cross-session ids rejected
    sets:    Slab<(u16 /*gen*/, VertexSet)>,     // RoaringBitmap over slots
    tensors: Slab<(u16, Tensor)>,                // v1: only Shape::V; always Arrow-backed
    graphs:  Slab<(u16, Arc<GraphProjection>)>,
    walks:   Slab<(u16, WalkMatrix)>,            // [n_walks, len] u32 slot matrix (random_walks)
    levels:  Slab<(u16, BfsLevels)>,             // per-vertex depth + σ path counts (Brandes)
    bytes_live: usize,                           // arena memory accounting (adversarial §2)
}
```

Every kernel resolves a handle through `table.get(h) -> Result<&T, FnError::StaleHandle>` which checks epoch + generation + kind (and, for tensors, **shape**) and **returns an error value, never indexes raw and never panics**. A forged `Handle(0x41414141)`, a use-after-free, a cross-session handle, or a shape/kind mismatch all become typed `FnError`s. For WASM specifically, prefer the component-model `resource` type so the id is a runtime-owned table index the guest genuinely cannot fabricate; the `u64`-packing above is the in-process (Rhai/PyO3) and Extism representation.

**Honest security framing:** only the WASM `resource` lowering is *structurally* unforgeable. The packed `u64` is defense-in-depth, not capability security — a 12-bit generation wraps after 4096 reuses of one slot (retire slots on wrap rather than recycle) and a 16-bit epoch wraps after 65k sessions in one process (reject on wrap, or fold in a per-process random salt). Both wraps must fail closed.

**Handle lifetime (mandatory, or §4.1 purity OOMs its own examples):** pure-functional kernels allocate a new O(V) buffer per call, and a 50-iteration loop would otherwise hold ~150 dead maps live against the arena cap. The catalog therefore includes an explicit `free(h)`; additionally each loader shim layers its natural reclaim on top — Rhai/PyO3 wrap handles in a refcounted object whose `Drop` frees the slot; WASM component resources get `resource.drop` for free; Extism (bare `u64`) relies on explicit `free` plus the session-end sweep. The arena cap (§5.1) remains the backstop, not the mechanism.

### 4.3 The kernel catalog (core + 4 starred)

**The catalog is a versioned capability *slice*, not a flat trait** (prior-art P2, ABI-explosion mitigation). v1 ships exactly `graph-compute@1` (below). The kind/shape system **reserves — but v1 does not implement —** three sibling slices, negotiated the same way and added additively (§10): `tensor-compute@1` (matmul / activation / neighbour-aggregate → GNN inference), `autodiff@1` (tape record + backward → GNN training), `columnar-bridge@1` (zero-copy `Tensor ↔ DataFusion column`). Adding a slice never changes a shipped signature.

Named-intent, not raw algebra. `dir ∈ {Out, In, Both}`. All operate on session handles.

```rust
pub trait GraphCompute {  // === graph-compute@1 — the only slice shipped in v1 ===
    // ---- graph acquisition (kernel surface gated by Capability::GraphCompute;
    //      project() ADDITIONALLY requires the existing HostQuery gate — data-read
    //      and compute are orthogonal capabilities, both must be granted) ----
    fn project(&mut self, spec: &GraphProjectionSpec) -> Result<Handle /*Graph*/, FnError>;
    fn vertex_count(&self, g: Handle) -> Result<u64, FnError>;
    fn edge_count(&self, g: Handle) -> Result<u64, FnError>;

    // ---- 0. plumbing (load-bearing: PageRank/WCC/Bellman-Ford are inexpressible without) ----
    fn degrees(&mut self, g: Handle, dir: Direction) -> Result<Handle /*Tensor*/, FnError>;
    fn vertex_ids(&mut self, g: Handle) -> Result<Handle /*Tensor: slot→own-id*/, FnError>; // WCC init
    fn ewise(&mut self, a: Handle, b: Handle, op: EwiseOp) -> Result<Handle, FnError>; // map⊕map
    fn set_to_map(&mut self, s: Handle, value: Scalar) -> Result<Handle /*Tensor*/, FnError>;
    fn map_to_set(&mut self, m: Handle, pred: Predicate) -> Result<Handle /*Set*/, FnError>;
    fn free(&mut self, h: Handle) -> Result<(), FnError>;   // see §4.2 handle lifetime

    // ---- 1. frontiers & sets ----
    fn frontier(&mut self, g: Handle, seeds: &[Vid]) -> Result<Handle /*Set*/, FnError>;
    // Vid→slot translation happens host-side via the projection's IdMap;
    // seeds absent from the projection are a typed error (fail closed, not skip).
    fn set_union(&mut self, a: Handle, b: Handle) -> Result<Handle, FnError>;
    fn set_diff(&mut self, a: Handle, b: Handle) -> Result<Handle, FnError>;
    fn set_intersect(&mut self, a: Handle, b: Handle) -> Result<Handle, FnError>;
    fn set_len(&self, s: Handle) -> Result<u64, FnError>;
    fn is_empty(&self, s: Handle) -> Result<bool, FnError>;

    // ---- 2. traversal (direction-optimized inside; mask fused; charges Σdegree) ----
    fn expand(&mut self, g: Handle, frontier: Handle, dir: Direction,
              exclude: Option<Handle /*visited mask*/>,
              filter: Option<EdgeFilter>) -> Result<Handle /*Set*/, FnError>;

    // ---- 3. the workhorse: sparse mat-vec under a named semiring ----
    //      vec must be a Tensor (use set_to_map to lift a Set); Set input is a kind error.
    fn spmv(&mut self, g: Handle, vec: Handle /*Tensor*/, sr: Semiring,
            dir: Direction, mask: Option<Handle>) -> Result<Handle /*Tensor*/, FnError>;

    // ---- 4. per-vertex value maps (GAS scatter/gather; ALL pure — new handle out) ----
    fn zero_map(&mut self, g: Handle, ty: DType) -> Result<Handle, FnError>;
    fn scatter(&mut self, map: Handle, frontier: Handle,
               value: Scalar) -> Result<Handle, FnError>;   // pure: returns a NEW map (§4.1)
    fn map_apply(&mut self, map: Handle, op: MapOp) -> Result<Handle, FnError>;

    // ---- 5. reductions ----
    fn reduce(&self, map: Handle, op: ReduceOp,
              mask: Option<Handle /*Set*/>) -> Result<Scalar, FnError>; // mask ⇒ dangling-mass etc.
    fn arg_extreme(&self, map: Handle, op: ArgOp) -> Result<(Vid, Scalar), FnError>; // lowest-slot tie-break
    fn topk(&self, map: Handle, k: u32) -> Result<Vec<(Vid, Scalar)>, FnError>;
    // topk returns k (Vid, score) pairs directly — k is small, this IS coarse; a Set
    // would destroy ranking (RoaringBitmap can't order) and force a host-side re-join.

    // ---- 6. iteration control ----
    fn l1_diff(&self, a: Handle, b: Handle) -> Result<f64, FnError>;

    // ---- 7. stochastic (seeded → deterministic); one coarse call generates ALL walks
    //      natively — a per-step surface cannot express node2vec's second-order p/q bias
    //      (needs the previous vertex per walk) and destroys walk identity ----
    fn random_walks(&mut self, g: Handle, starts: Handle /*Set*/, len: u32,
                    walks_per_start: u32, bias: WalkBias,
                    seed: u64) -> Result<Handle /*Walks*/, FnError>;

    // ---- 8. the ONLY bulk egress — HOST-TERMINAL: writes to the session result sink
    //      consumed by the bridge's existing build_record_batch path (bridge.rs:294).
    //      Returning a RecordBatch to the guest would be the bulk marshal this design
    //      exists to avoid (Arrow IPC into WASM linear memory). The host prepends a
    //      `nodeId` column via IdMap slot→Vid reverse translation. Charges O(V) (§5.1).
    fn emit(&mut self, cols: &[(&str, Handle)]) -> Result<(), FnError>;
    fn emit_walks(&mut self, walks: Handle) -> Result<(), FnError>; // rows: (walk_id, step, nodeId)

    // ---- starred: extends F→C coverage ~70%→90% ----
    // Δ-stepping/Dijkstra: returns the next distance-bucket [i·delta, (i+1)·delta) as a
    // frontier — ONE crossing per bucket. (A pop-the-min surface would be one crossing
    // per settled vertex = the banned P1 pattern; that is why there is no priority queue.)
    fn next_bucket(&mut self, dist: Handle /*Tensor*/, delta: f64,
                   settled: Handle /*Set*/) -> Result<Handle /*Set*/, FnError>;
    // Brandes: forward pass computes per-vertex depth AND σ path counts in one native
    // call (both are required inputs to the dependency accumulation); reverse pass
    // walks levels deepest-first natively.
    fn bfs_levels(&mut self, g: Handle, seeds: Handle) -> Result<Handle /*Levels*/, FnError>;
    fn reverse_accumulate(&mut self, g: Handle,
                          levels: Handle /*Levels*/) -> Result<Handle /*Tensor*/, FnError>;
    // Pairwise neighbourhood work (triangles, k-truss support, Jaccard/cosine/Adamic-Adar):
    // pairwise state fails the §3.1 dividing test for guest loops, so it ships as ONE bulk
    // kernel — host-side masked intersection over adjacent pairs (or top-k candidates),
    // never per-pair guest calls.
    fn neighborhood_overlap(&mut self, g: Handle, pairs: PairSpec,
                            metric: OverlapMetric) -> Result<Handle /*Tensor*/, FnError>;
}
```

**The parameter enums are part of the ABI, not an afterthought** — they are exactly the surfaces prior-art P2 warns explode, and the only parts that must serialize identically across four loaders. v1 pins them closed:

```rust
pub enum MapOp   { Normalize(Norm), Scale(f64), AxPlusB(f64, f64), Recip, Log }
pub enum Norm    { L1, L2 }
pub enum EwiseOp { Add, Mul, Min, Max, Axpy(f64) /* a + coef·b */ }
pub enum ReduceOp{ Sum, Min, Max, Count, NormL1, NormL2 }
pub enum Predicate { IsZero, Gt(f64), Lt(f64), Eq(f64) }
pub enum WalkBias  { Uniform, Weighted, Node2Vec { p: f64, q: f64 } }
pub enum PairSpec  { AdjacentPairs, TopKCandidates(u32) }
pub enum OverlapMetric { Count, Jaccard, Cosine, AdamicAdar }
pub enum EdgeFilter {  // closed, host-evaluated; deliberately minimal in v1 (see §5.6/§6)
    WeightRange { min: f64, max: f64 },
    EdgeType(String),
    TimestampLe { prop: String, cutoff: i64 },  // time-respecting expansion (§6.2)
}
pub struct Scalar { /* tagged union over DType */ }
```

**Semirings stay a closed-but-curated enum** (adversarial §8 / prior-art P2) — an open, guest-supplied semiring reintroduces the per-element boundary crossing we are trying to eliminate *and* defeats determinism:

```rust
#[non_exhaustive]
pub enum Semiring {
    Reachability,   // (lor, land)   — boolean BFS
    ShortestPath,   // (min, plus)   — tropical
    Propagate,      // (min, first)  — label / parent
    LinearAlgebra,  // (plus, times) — dtype-parameterized: f64 → PageRank/HITS/eigenvector;
                    //                 i64 → path counting (there is no separate "Count"
                    //                 semiring: (plus, pair) is not a semiring, and integer
                    //                 counting IS (plus, times) at a different dtype —
                    //                 the kernel takes dtype from the input Tensor)
    MinMax,         // (max, min)    — bottleneck / widest path
}
```

Adding a variant is a **minor version** (the enum is `#[non_exhaustive]`); removing or changing signatures is forbidden within a major version. The kernel namespace is versioned **per capability slice** (`graph-compute@1`, and later `tensor-compute@1` etc.): the guest declares the slices + versions it needs and the host checks them at load, refusing mismatches with a clear error rather than a mysterious trap.

### 4.4 A worked example — Personalized PageRank in Rhai (~15 lines)

This example is *dry-run against the literal §4.3 signatures* — including out-degree normalization and dangling-mass redistribution, without which a "PPR" converges to the wrong vector while `l1_diff` happily reports convergence:

```rust
fn personalized_pagerank(g, seeds, alpha) {
    let teleport = map_apply(set_to_map(frontier(g, seeds), 1.0), Normalize(L1));
    let inv_deg  = map_apply(degrees(g, Out), Recip);        // Recip(0) = 0 → dangling rows drop out
    let dangling = map_to_set(degrees(g, Out), IsZero);      // zero-out-degree vertices
    let rank = teleport;
    for iter in 0..50 {
        let contrib = ewise(rank, inv_deg, Mul);             // rank(u)/outdeg(u)
        let next    = spmv(g, contrib, LinearAlgebra, Out, ()); // native O(E), f64 dtype
        let dm      = reduce(rank, Sum, dangling);           // dangling mass this round
        let next    = map_apply(next, Scale(alpha));
        let next    = ewise(next, teleport, Axpy(1.0 - alpha + alpha * dm)); // teleport blend
        if l1_diff(rank, next) < 1e-6 { rank = next; break; }
        free(rank); rank = next;                             // §4.2 lifetime: don't hold dead maps
    }
    emit([("score", rank)]);   // host-terminal; host prepends nodeId via IdMap (§4.3 group 8)
}
```

Every line except the loop control is a native kernel. The interpreter runs ~50 trivial iterations; native does all O(E) work per iteration. **Zero vertex data crosses the boundary.** BFS reachability, WCC (init via `vertex_ids`, iterate `spmv(Propagate)`), min-label propagation, Bellman-Ford (`ewise(dist, spmv(g, dist, ShortestPath), Min)`), and node2vec walk generation are the same shape.

### 4.5 The four loader shims (one trait, thin bindings)

Confirmed capabilities per loader (`(a)` hold/pass handle, `(b)` call host fn on handles, `(c)` enforce capability, `(d)` bound runaway loops):

| Loader | (a) | (b) | (c) | (d) | New plumbing |
|---|---|---|---|---|---|
| **Rhai** | ✅ (`register_type_with_name`, cf. `MutableFloat64Column` `columns.rs:185`) | ✅ (`register_fn`) | ✅ 3-layer (manifest → grant intersection → call-time Layer-3 guard, `host_fn_impls/mod.rs:39`) | ✅ (`set_max_operations`, `engine.rs:71`) | `host_fn_impls/graph.rs`; handle carries `Arc<Mutex<AlgoSession>>` (no `NativeCallContext` in-tree, matches the columns pattern) |
| **WASM** | ✅ (`u32`/`u64` scalar) | ✅ (`func_wrap`, cf. `add_host_net` `linker.rs:198`) | ✅ structural + call-time (`linker.rs:103`) | ✅ (epoch deadline `loader.rs:840` + fuel + `StoreLimits`) | `host-graph` interface in `world.wit` (WIT `resource`); handle table on `HostState` |
| **Extism** | ⚠️ (needs manual `Val`) | ✅ mechanically | ✅ (`allowed_host_fn_names` `loader.rs:310`) | ✅ (30s `with_timeout` + 1 GiB) | raw-`Val` `Function::new` graph fns — **existing in-tree precedent**, not new territory: `build_service_fn` (`host_svc/mod.rs:142-180`) already builds all five service fns this way; only the JSON-string marshalling inside the `host_fn!` shells changes. Table on `HostSvcCtx` |
| **PyO3** | ✅ (`#[pyclass]`) | ❌ **no query-time host callback today** | ⚠️ load-time only | ❌ **no loop bounding at all** | `#[pyclass] GraphHandle`; query-time host-context injection; per-call cap gate; **interrupt/deadline mechanism** (currently GIL held whole batch; the whole `ProcedureContext` — including its `deadline` field, `traits/procedure.rs:143` — is discarded as `_ctx` at `adapter_procedure.rs:80`) |

**PyO3 is the weak loader** — its two gaps (no query-time callback, no loop bounding) are prerequisites, not part of the handle design. Sequence PyO3 last, or ship it read-only-then-harden.

### 4.6 The guest algorithm contract (declaration → invocation → YIELD)

Everything here mirrors a shipped convention — the `Algorithm` entry kind already exists end-to-end for native providers; this section defines only how *guests* plug into it.

**Declaration.** The manifest already carries `provides.algorithms: Vec<SmolStr>` (`manifest.rs:143`) — names only, per the manifest's summary-not-schema design. Rich signatures live at registration time, as for every other entry kind. The existing `AlgorithmSignature` (`traits/algorithm.rs:19-24`) is today only `output_fields: Vec<arrow_schema::Field>` + `docs` — **extend it additively** (defaulted fields, no break):

```rust
pub struct AlgorithmSignature {
    pub output_fields: Vec<arrow_schema::Field>,   // = the YIELD schema (existing)
    pub docs: String,                              // (existing)
    pub args: Vec<NamedArgType>,                   // NEW: adopt the procedure convention
                                                   //      (traits/procedure.rs:68) — today
                                                   //      algorithm args are untyped JSON
    pub slices: Vec<SliceReq>,                     // NEW: e.g. [("graph-compute", 1)] —
                                                   //      checked at load (§4.3 versioning)
}
```

**Capabilities (three orthogonal gates).** `Capability::Algorithm` (exists, `capability.rs:115`) gates *registration* of the entry, as today (`registrar.rs:367-384`). `Capability::GraphCompute` (new variant, kebab-case `graph-compute`, same enum-growth pattern as `LocyGenerator`) gates the *kernel catalog*. `Capability::HostQuery` gates `project` (data read), as it already does at `bridge.rs:127`. A guest algorithm needs all three declared in its manifest TOML (`[[capabilities]] kind = "graph-compute"` …).

**Invocation & argument marshaling.** Unchanged host-side contract: CALL args are evaluated and serialized as a **positional JSON array into `AlgorithmContext::config_json`** (`executor/procedure.rs:770-816`; the reachability provider's `parse_config` is the precedent). What v1 adds: the host **validates and coerces the JSON array against the declared `args: Vec<NamedArgType>` before the guest runs** — arity, types, defaults — so guests receive typed values, not raw JSON (this also closes the existing native providers' untyped-arg weakness; a map-shaped config arg is declared `ArgType::CypherValue`, the existing opaque-value convention, since no dedicated map `ArgType` exists). Per loader, the entrypoint is bound exactly the way that loader binds procedures today:

| Loader | Declaration | Entrypoint invocation |
|---|---|---|
| Rhai | `uni_manifest()` map gains an `algorithms: [#{ name, args, yields }]` array (mirrors `procedures`, `uni-plugin-rhai/src/manifest.rs:4-24`) | `Engine::call_fn(scope, ast, name, coerced_args)`; kernels are global registered fns bound to the per-call `Arc<Mutex<AlgoSession>>` (§4.5) |
| WASM | new `algorithm-plugin` world in `world.wit`: exports `manifest`, `register`, `invoke-algorithm: func(qname: string, args-ipc: list<u8>) -> result<_, string>` (mirrors `invoke-procedure`, world.wit:148-155); imports the `host-graph` interface (`resource` handles) | one dispatch export multiplexed by qname, per the existing per-kind-world pattern |
| Extism | `RegistrationEntry::Algorithm { qname, args, yields }` added to the register-export JSON (mirrors `Procedure`, `exports.rs:88-119`) | named guest export per entry (loader.rs:464-499); graph host fns via raw-`Val` `Function::new` (§4.5) |
| PyO3 | module attribute by name (as `adapter_procedure.rs:248`) | `def my_algo(gc, *args)` receiving a `#[pyclass]` GraphCompute session object — **Phase 5 only** |

**Result egress (YIELD).** The declared `output_fields` *are* the YIELD schema, as today — the CALL planner reads them before execution. The guest never returns data: it calls `emit(cols)` (§4.3 group 8), and the host **validates the emitted column names/types against `output_fields` at emit time** (mismatch = typed error `0x869`, §12). If `output_fields` declares a leading `nodeId: Int64`, the host supplies it via IdMap reverse translation; `emit_walks` has the fixed schema `(walk_id: Int64, step: Int64, nodeId: Int64)`. The guest entrypoint's return value is ignored on success; a guest-raised error aborts the invocation with its message wrapped in the standard per-batch stream-error path (`AlgorithmProvider::run`'s existing contract).

**Dispatch.** Guest algorithms register as `AlgorithmEntry` (`registry.rs:102-109`) with a loader-provided `Arc<dyn AlgorithmProvider>` adapter (one per loader, exactly like scalar/procedure adapters); `run_algorithm_provider` (`procedures_plugin/algo.rs:594`) needs no changes beyond constructing the `AlgoSession` on the bridge.

---

## 5. The hard problems (mandated mitigations)

These are non-negotiable; the adversarial and prior-art streams agree that skipping any one makes the API a DoS vector or a correctness lie.

### 5.1 Native-work budgeting — *the make-or-break decision* (adversarial §2, prior-art P5/P6)

Every existing uni budget meters the **wrong quantity**: WASM fuel and Rhai `max_operations` count *interpreter* ops, but one cheap interpreter op (`expand`) triggers O(E) *native* work the meter never sees. A guest `while !is_empty(f) { f = expand(...) }` is a handful of Rhai ops per iteration and runs the host at full native speed until (WASM) the 30s epoch deadline or (Rhai) never.

**Mandate:** a **native-work budget** on `AlgoSession`, charged per kernel proportional to work done — `expand` charges Σ frontier degree; `spmv` charges nnz; set ops charge |set|; **the O(V) kernels are charged too** (`map_apply`/`ewise`/`reduce`/`zero_map`/`scatter`/`vertex_ids`/`degrees` charge |V|; `emit` charges |V| per column; `neighborhood_overlap` charges Σ over pairs of min-degree) — on sparse graphs with |V| ≫ active-set, the O(V) ops dominate and an edges-only meter undercharges. Decrement a counter, return `GraphComputeExhausted` at zero. This is the graph analogue of Locy's iteration budget (`LocyIncompleteReason::IterationLimit`). The budget must be **checkable inside expensive kernels** (chunk super-node expansion, check every N edges) — not only between calls, or a single celebrity-vertex `expand` blows past it (prior-art P5); chunked checking bounds overshoot to the chunk size, which is acceptable. The budget is expressed as a **multiple of |E| combined with an absolute ceiling** — the multiple alone is unbounded in practice on a 10⁹-edge projection (resolves open decision §11.3). Plus a **hard arena cap** (max live handles + max total bytes) enforced at *allocation* time, since none of the loaders' memory limits see host-side arena allocations; `free` (§4.2 lifetime) is the mechanism, the cap is the backstop.

### 5.2 Timeout = hard error, never silent truncation (adversarial §6)

Copy Locy exactly: over-budget/non-convergence is `UniError::GraphComputeIncomplete` **by default**, with opt-in `allow_partial` for anytime semantics. Distinguish `Timeout` vs `IterationLimit` vs `Exhausted` so users can tell "too slow" from "didn't converge." Do **not** silently truncate the way `run_pregel` currently does at `max_supersteps` (a correctness-honesty hazard).

### 5.3 Determinism is a host-kernel contract (adversarial §4, prior-art P7)

The substrate is **nondeterministic today**: neighbor order is HashMap-derived (`AdjacencyManager::get_neighbors`, `crates/uni-store/src/storage/adjacency_manager.rs:101`, accumulates into a `HashMap` and returns `into_iter().collect()` at `:158`), and float reductions run under rayon (`crates/uni-algo/src/algo/algorithms/pagerank.rs:77-80` dangling-mass `into_par_iter()…sum::<f64>()`, `:104-107` convergence delta; float `+` is non-associative under work-stealing reduction trees). A guest can't fix this through an opaque handle.

**Mandate deterministic-by-construction:** sort every neighbor row at projection build (`collect_edges`, `projection.rs:362`, and `build_csr`, `projection.rs:601` — today neither sorts neighbor lists); specify a fixed reduction order for kernel reductions (sequential or fixed-tree or Kahan); define explicit tie-breaks (`arg_extreme` = lowest-slot-id wins); seed every stochastic kernel (`random_walks` takes a `seed`, per the existing pattern in `crates/uni-algo/src/algo/algorithms/random_walk.rs:197-210` — seeded per-walk, scheduling-independent, deterministic by default). Nondeterminism *inside a composition primitive* is uniquely corrosive — far worse than inside one native algo.

### 5.4 Panic isolation at the session boundary (adversarial §7)

There is no `catch_unwind` on the invoke hot path today (only trigger dispatch, `triggers.rs:728,828,2068`; session/tx *hook* dispatch has its own, but that is not the invoke path). Every kernel returns `Result` and never panics (validated up front). **Additionally**, wrap the whole `AlgoSession` orchestration entry in `catch_unwind(AssertUnwindSafe(...))` so a defensive panic in a kernel becomes an aborted invocation, not a query-worker crash. This is worth doing here regardless of the separate hot-path-panic gap (`plugin_framework_gaps` §8 #5 — which names the gap but no helper; a shared `guard_plugin_call`-style wrapper serving both is the natural shape).

### 5.5 Snapshot isolation — document honestly (adversarial §3)

The projection is stable for the invocation (`L0Manager::pin_snapshot()`, `projection.rs:213`, clone-on-freeze) — but it is explicitly **analytics, not serializable**: it may pick up Lance rows committed during the build window and does **not** observe the caller's own uncommitted writes. Surface a **captured version stamp** to the guest and document the isolation level as *"snapshot-analytics: does not observe writes committed during projection build, nor the caller's uncommitted writes."* Do not let the API imply live/consistent reads.

### 5.6 No `expand_with(guest_closure)` escape hatch in v1 (adversarial §5, prior-art P1)

The per-vertex guest callback is the one thing to **cut, not redesign**. It inverts the entire value proposition (one crossing per O(E) work → one crossing per edge), and for WASM/PyO3 it's catastrophic (component re-entry / GIL acquisition per edge). It also breaks the §5.1 budget, §5.3 determinism, and §5.4 no-panic contracts simultaneously — "not a feature with footguns; an anti-feature that makes every other guarantee conditional." Invest that effort in growing the coarse kernel set (§4.3) so arbitrary per-vertex logic is unnecessary for the target algorithms. Genuinely stateful/sequential algorithms (Louvain, DFS-SCC, SimRank) stay native and are reachable via the existing `as_any()` downcast to `uni.algo.*`.

**The same rule constrains `EdgeFilter`.** The filter must stay a *closed, host-evaluated* enum (§4.3) — the moment it grows a "custom predicate" arm it *is* the per-edge callback by another name. This bounds the §6 differentiators honestly: anything expressible as a closed variant (weight range, edge type, monotonic-time cutoff) ships; anything needing guest logic per edge does not.

---

## 6. uni-specific differentiators (the reason this beats a generic GDS clone)

The handle+kernel model composes with uni's vector and logic engines in ways no comparable engine offers:

1. **Vector-guided walks / GraphRAG retrieval** *(the seeding+fusion half ships with v1; the similarity-weighted half is a designed follow-up, not v1)*. Vector search picks seeds (`frontier` from `uni.vector.query`'s `Vec<(Vid,f32)>`), PPR/expand does structural multi-hop, `emit` + the existing `fuse_rrf_multi` (`crates/uni-query-functions/src/fusion.rs:17`) fuses — all expressible today. The stronger form — a PPR whose transition weights are `cosine(neighbour.embedding, query)`, i.e. semantically-steered expansion à la **HippoRAG / Microsoft-GraphRAG** — requires either a `SimilarityToQuery` `EdgeFilter`/`WalkBias` variant (closed, host-evaluated: the host computes similarities natively at projection or expansion time) or similarity-weighted projection build. That variant is real design work (embedding access inside the kernel, budget model for it) and is **deliberately not in the v1 enum** — per §5.6, it must never become a guest callback. This also mirrors the existing `VectorSource::Plugin{handle: Arc<dyn IndexHandle>}` seam (`vector_knn.rs:60`), so "opaque host handle + coarse kernels" is *one* mental model spanning vector and graph.

2. **Temporal / BTIC-aware expansion.** `expand`'s `filter` given the closed `TimestampLe` variant (§4.3) yields time-respecting reachability / earliest-arrival paths (cyber attack-timing, AML cycle timing) within the v1 enum. `AdjacencyManager::get_neighbors_at_version` (`adjacency_manager.rs:172`) already supports snapshot-time reads → a `view_at(version)` projection variant is a small addition.

3. **Locy-derived edges as compute input.** A projection built from Locy `DERIVE`d edges lets users run PageRank/reachability over **logically-inferred** graphs (transitive risk, inferred org structure), closing the logic→algorithm loop. Caveat to design around: DERIVE output is label/property-space, GraphView is Vid/slot-space — needs a `project_from_derived(rule)` builder that bridges the two identity spaces.

---

## 7. What GraphView still lacks (small prereqs)

- **Vertex/edge property accessors.** `GraphView` is topology-only (one baked `weight_property`, `algorithm.rs:125`). Vector-guided, filtered, and multi-property kernels need property reads → add projection-time property columns (dense, slot-indexed) + a `vertex_prop(map_from_property)` accessor.
- **Edge-type discrimination.** The projection **merges edge types** into one adjacency (`projection.rs`, only weights are per-edge). Typed frontier expansion needs either multiple projections or a type-tagged CSR. v1: filter at projection-build via `GraphProjectionSpec.edge_types` (`algorithm.rs:123`, wired at `bridge.rs:156-160` — already works); typed-multigraph kernels are a follow-up.
- **One work item, not two tracks:** both bullets overlap the typed-multigraph / relationship-filter follow-ups already deferred in `plugin_framework_gaps_2026-07-07.md` §0.2 — track them as a single GraphView-extension item shared by both proposals.
- **Write-back is an explicit v1 non-goal.** GDS's `.mutate`/`.write` (cited in §2) has no analogue here: persisting a computed score means `emit` → Cypher `SET` round-trip. Deliberate for v1 (write-back implies a mutation surface and interacts with §11.5's session-lifetime question); recorded so the omission is a decision, not an oversight.

---

## 8. Phased delivery

1. **Phase 0 — determinism + budget substrate (prereq, no API yet).** Sort neighbor rows at projection build (§5.3); add the native-work budget + arena cap to a new `AlgoSession` (§5.1); `GraphComputeIncomplete` hard-error type (§5.2). Ships value even before guests: makes the *native* `uni.algo.*` reproducible.
2. **Phase 1 — `GraphCompute` trait + core kernels (groups 0–8), native-only.** Generalize `run_pregel` internals; generational handle table + `free`/lifetime (§4.2); port one first-party algorithm (PPR) onto the kernel catalog as the dogfood + differential-test oracle. **Gate before the trait freezes:** write pseudocode for *every* F-row algorithm in §3 using only the literal catalog signatures, and fix the catalog until all of them type-check — the same discipline §5 applies to safety. (This revision already applied that gate once; it added group 0, fixed `scatter` purity, and killed a fake semiring. Re-run it on the final signatures.)
3. **Phase 2 — Rhai shim.** The strongest loader; `host_fn_impls/graph.rs` + `Capability::GraphCompute` (gates the kernel surface; `project` additionally requires the existing `HostQuery` grant — see §4.3) + manifest `algorithms:` entry-kind. First guest-authored algorithm. **Ordering constraint:** the still-open P0.7 signature-enforcement milestone (`plugin_framework_gaps_2026-07-07.md` §6 #6 — loaders never self-verify manifests) must land **before or alongside** any guest shim phase; wiring all four loaders to a powerful host surface without it widens the unverified-manifest blast radius.
4. **Phase 3 — WASM shim.** `host-graph` WIT interface (component resources) + handle table on `HostState`.
5. **Phase 4 — Extism shim** (manual `Val` fns, per the `build_service_fn` precedent) **+ the 4 starred kernels** (`next_bucket`, `bfs_levels`+`reverse_accumulate`, `neighborhood_overlap`).
6. **Phase 5 — PyO3 shim** — after closing its two prerequisite gaps (query-time host callback + loop bounding).
7. **Ongoing — differentiators (§6):** `SimilarityToQuery` filter/bias design, `view_at(version)`, `project_from_derived`.

Each phase's exit criterion is its acceptance-test bar in §9 (Phase 0 → §9.1, Phase 1 → §9.2, each shim phase → §9.3 for that loader + §9.4).

**Sizing (engineer-weeks, estimates; oracles from §9.0 are written inside Phase 0/1, not extra):**

| Phase | Estimate | Dominant cost |
|---|---|---|
| 0 | ~1–1.5 wk | fixed-order reductions in existing native algos without perf regression; P0-6 behavior flip |
| 1 | ~2.5–3.5 wk | the catalog itself + handle table + 12 F/C differential drivers with oracles; the §8 type-check gate |
| 2 (Rhai) | ~1–1.5 wk | thin — columns-pattern precedent; corpus authoring |
| 3 (WASM) | ~1.5–2 wk | WIT `resource` interface + component fixture crate + build-script wiring |
| 4 (Extism + starred) | ~2 wk | 4 starred kernels dominate (~1.5 wk); raw-`Val` shim is precedented |
| 5 (PyO3) | ~2–2.5 wk | the two prerequisites (host callback injection, deadline/interrupt) dominate; shim itself is small |

**Validation-strength note for the implementer:** every `file:line` claim in this document was re-verified against `ae9eb1c68`; the catalog was dry-run *analytically* (review-strength) against every F/C algorithm — the §8 Phase-1 gate re-runs that check at compile-strength, and small signature adjustments there are expected and in-scope, not plan deviations.

---

## 9. Automated acceptance tests (adversarial §9)

Two principles: **(1)** every §5 mandate gets a *failing-guest* test — the mitigations are only real if a hostile guest demonstrably cannot break them; **(2)** the §8 Phase-1 "dry-run every F-row algorithm" gate is not a review step but an *executable test suite* — each F-row algorithm exists as a driver against the literal trait, differentially checked against an independent oracle. Each phase below ends with its acceptance bar: **the phase is done when its test IDs are green.**

### 9.0 Infrastructure & placement (follows existing conventions)

- **E2E home:** new module dir `crates/uni/tests/common/graph_compute/` wired into `crates/uni/tests/integration.rs` via `#[path]` — the established pattern (cf. `common/graph_algo/`); **no new test binary**.
- **Kernel/handle unit + property tests:** in the crate that owns `AlgoSession` (`uni-plugin-builtin`), plus `uni-algo` for projection-determinism tests.
- **Oracle:** `common/graph_compute/oracle.rs` — naive, sequential, dependency-free reference implementations (adjacency-list BFS/Bellman-Ford/power-iteration with Kahan summation), following the `uni-locy-oracle` independence invariant: the oracle must share *no code* with `GraphProjection` or the kernels. Exact equality for integer/boolean results; **tolerance-aware comparator** (`ScoreBag`, per-column abs+rel ε — the exact `RowBag` in `common/diff/` won't do for floats) for scores.
- **Guest fixtures:** Rhai/PyO3 inline source in tests (existing convention); WASM/Extism as new own-workspace fixture crates `examples/example-wasm-graph/` + `examples/example-extism-graph/` added to `scripts/build-wasm-fixtures.sh`, tests failing with the standard "run build-wasm-fixtures.sh" panic if artifacts are missing (no silent skip).
- **Conformance probes:** extend `crates/uni-plugin-conformance` with stable-ID probes (`graph.handle_forgery`, `graph.capability_gate`, `graph.budget`, `graph.determinism`, `graph.slice_version`) so third-party algorithm authors can self-certify with the same corpus we gate on.
- **Gating tiers:** PR gate = non-ignored tests, small case counts; nightly = `#[ignore]`d `*soak*` variants with `GRAPH_COMPUTE_FUZZ_CASES` / `METAMORPHIC_CASES` env scaling (mirrors `metamorphic-smoke` vs nightly `soak`).

**Anti-gaming provisions** (this suite will likely be implemented by the same hands as the kernels; these rules make "green by weakening" visible instead of easy):

1. **Oracles are written and merged first** (Phase-0-adjacent, before any kernel exists), from the spec, never from kernel code. Each oracle additionally carries **hand-computed golden literals** — tiny graphs (≤ 8 vertices) with expected outputs written as constants in the test — so an oracle bug shared with the kernel still fails the literal.
2. **Every numeric bound is pinned here, not in test code:** float differential ε = **1e-9 abs / 1e-7 rel** vs the Kahan oracle; F-8 χ² at **p = 0.001** with the seed fixed in the test; the COST-guard factor = **8×** a single-threaded loop; crossing budgets as stated per test. Loosening any of these is a *spec change to this section*, reviewed as such — not a test edit.
3. **Test IDs are append-only.** An acceptance-bar test may be strengthened or added, never deleted or weakened, within the proposal's lifetime; the one sanctioned behavior flip (P0-6) is called out explicitly below and is the only one.
4. **Defaults are tested, not just test configs:** wherever a test constructs a tiny budget/cap to force an error, a sibling test asserts the *production default* is finite and actually installed on the CALL path (see P0-8/P0-9).

### 9.1 Phase 0 acceptance — determinism + budget substrate

| ID | Test | Asserts |
|---|---|---|
| P0-1 `projection_csr_insertion_order_invariant` | proptest: build the projection from N random permutations of the same edge insertion order | byte-identical CSR (`out_offsets`/`out_neighbors`/`in_*`) every time — kills the HashMap-order nondeterminism at the source |
| P0-2 `native_algo_bitwise_reproducible` | run `CALL uni.algo.pageRank` twice, and under `RAYON_NUM_THREADS=1` vs `=N` | bitwise-identical YIELD rows (fixed reduction order) — the Phase-0 "ships value before guests" claim, tested |
| P0-3 `budget_exhaustion_is_hard_error` | `AlgoSession` with a tiny budget; loop `expand` | `GraphComputeIncomplete{Exhausted}` — and wall-clock far below any loader timeout (the meter fired, not the clock) |
| P0-4 `supernode_chunked_budget_check` | star graph, one celebrity vertex with degree ≫ remaining budget; single `expand` | error raised *mid-kernel*; overshoot ≤ chunk size (the §5.1 in-kernel check, not between-calls) |
| P0-5 `arena_cap_enforced_at_allocation` | allocate maps until the byte cap | typed error at the allocating kernel; `bytes_live` never exceeds cap; no OOM |
| P0-6 `pregel_max_supersteps_is_error` | `run_pregel` reaching `max_supersteps` unconverged | returns the incomplete error, not silent partial output — **flips current behavior** (§5.2); existing pregel e2e tests updated in the same commit |
| P0-7 `error_kinds_distinguishable` | drive each of Timeout / IterationLimit / Exhausted | three distinct variants surface distinctly through CALL ("too slow" ≠ "didn't converge") |
| P0-8 `per_kernel_charge_accounting` | run *every* kernel once on a known graph; read the budget counter before/after each | decrement equals the §5.1 formula for that kernel (Σdegree, nnz, \|set\|, \|V\|, \|V\|·cols…) — kills the "charge only `expand`, pass the loop tests" implementation |
| P0-9 `default_budget_is_finite_and_installed` | CALL with **default configuration** (no test-tuned budget) running a deliberately unbounded computation | hits `Exhausted` — proves the production default is finite *and* the CALL path installs it; a test-only budget cannot satisfy this |

### 9.2 Phase 1 acceptance — handle table + kernel catalog (native drivers)

**Handle security (the §4.2 model, attacked):**

| ID | Test | Asserts |
|---|---|---|
| H-1 `handle_forgery_fuzz` (+ soak) | proptest random `u64`s into *every* kernel parameter position, under a `catch_unwind` harness | typed `FnError` every time; zero panics, zero OOB |
| H-2 `use_after_free_is_stale` | `free(h)` then use `h`; also use a pre-`free` copy after slot reuse | `StaleHandle` (generation check) |
| H-3 `cross_session_rejected` | handle minted in session A passed to session B | epoch-mismatch error |
| H-4 `kind_and_shape_mismatch` | Set into `spmv`; i64 tensor into an f64-only op | typed kind/shape errors |
| H-5 `generation_wrap_retires_slot` | force 4096 free/alloc cycles on one slot | slot retired (never recycled into ambiguity); old handles still rejected — the §4.2 fail-closed wrap |
| H-6 `session_drop_reclaims_all` | run a script that leaks handles deliberately; drop session | table empty, `bytes_live == 0` after `run()` returns |
| H-7 `kernels_are_pure` | for each kernel: hash all input buffers, call, re-hash | inputs bit-unchanged; output is a fresh handle (guards the §4.1 invariant `scatter` once violated) |

**F-row differential suite — the §8 gate, executable.** One native driver per F-row algorithm written against the *literal* `GraphCompute` signatures (these drivers ARE the dry-run; if the catalog can't express an algorithm, its driver doesn't compile or its test fails):

| ID | Algorithm(s) | Oracle & comparator |
|---|---|---|
| F-1 | BFS levels / k-hop / reachability | naive BFS — exact |
| F-2 | WCC (init `vertex_ids`, iterate `spmv(Propagate)`) | naive union-find — exact |
| F-3 | Bellman-Ford (`ewise(dist, spmv(ShortestPath), Min)`) | naive relaxation — exact (int weights) / ε (float) |
| F-4 | PageRank + **PPR** (§4.4 verbatim, incl. dangling mass) | sequential Kahan power-iteration — ε; **plus** parity vs native `uni.algo.pageRank` |
| F-5 | Katz, HITS, eigenvector | power-iteration oracles — ε (HITS exercises `Normalize(L1)`+`NormL2`) |
| F-6 | min-label propagation | naive — exact |
| F-7 | k-core (peel: `degrees` + set ops + `spmv` recount) | naive iterative peel — exact |
| F-8 | `random_walks` node2vec | statistical: empirical transition frequencies vs p/q-bias expectation (χ² bound, fixed seed); plus determinism: same seed ⇒ identical walk matrix; different seed ⇒ different |
| F-9 | `LinearAlgebra` @ i64 (path counting) | naive matrix-power count — exact (the dtype-parameterized semiring, tested so "Count was removed" costs nothing) |
| C-1 | Δ-stepping via `next_bucket` | naive Dijkstra — exact; **plus** crossing-count assertion: host-fn invocations ≤ O(buckets), not O(V) (the anti-P1 property, measured) |
| C-2 | Brandes via `bfs_levels`+`reverse_accumulate` | naive Brandes — ε; parity vs `uni.algo.betweenness` |
| C-3 | `neighborhood_overlap` (triangles, Jaccard, Adamic-Adar) | naive pairwise intersection — exact counts / ε scores; triangle count cross-checked on K_n (known formula) |

**Metamorphic properties** (fold into `common/metamorphic/` conventions; smoke on PR, soak nightly):

- M-1 `relabel_invariance`: permute vertex ids, rebuild, rerun → permuted-but-equal results (any failure = slot-order leak; the strongest single determinism test).
- M-2 `ppr_mass_conservation`: Σ scores = 1 ± ε including dangling-node graphs.
- M-3 `reachability_monotone`: adding edges never shrinks a reachable set.
- M-4 `mask_fusion_equivalence`: `spmv(mask=m)` ≡ `spmv` then filter-by-m; `expand(exclude=x)` ≡ `expand` then `set_diff` (proves fusion is an optimization, not a semantics change).
- M-5 `direction_duality`: `expand(g, f, Out)` on G ≡ `expand(g', f, In)` on reversed G'.

**Acceptance bar for Phase 1:** F-1…F-9, C-1…C-3 (drivers compile against the frozen trait and pass), H-1…H-7, M-1…M-5 smoke.

### 9.3 Phases 2–5 acceptance — per-loader e2e (one corpus, four shims)

**The cross-loader conformance corpus:** the *same five guest algorithms* (PPR, BFS reachability, WCC, Bellman-Ford, top-k egress) authored in each loader's language. One shared driver in `common/graph_compute/loaders.rs` (feature-gated per loader, mirroring `common/loaders/`) runs the corpus and asserts against the Phase-1 native-driver results: **byte-identical** for integer/boolean algorithms, ε for floats — across Rhai, WASM, Extism, PyO3. A guest algorithm that agrees with the native kernel driver on all graphs *is* the acceptance proof that the shim carries handles faithfully.

Per-loader, additionally (each test id parameterized by loader):

| ID | Test | Asserts |
|---|---|---|
| L-1 `runaway_guest_hits_budget` | `while true { expand }` **and a sibling looping `spmv`** in guest code, under **default** budget config | `GraphComputeExhausted` (not the loader's own timeout — assert error kind and wall-clock < loader deadline); the §5.1 make-or-break, per loader; the spmv variant + P0-8 close the selective-charging hole |
| L-2 `kernels_denied_without_cap` | guest without `Capability::GraphCompute` calls any kernel | typed capability error (0x804-family), per the existing `third_party_provider_without_hostquery_is_denied` pattern |
| L-3 `project_needs_hostquery_too` | guest with `GraphCompute` but no `HostQuery` calls `project` | denied — the §4.3 orthogonal-gates rule, tested |
| L-4 `slice_version_negotiation` | manifest declares `graph-compute@2` | load-time refusal with a clear error, not a runtime trap |
| L-5 `guest_handle_abuse_is_survivable` | guest passes forged / freed / cross-kind handles | guest receives a catchable error; worker healthy; the *next* CALL succeeds (composes H-1 with §5.4 isolation, e2e) |
| L-6 `panic_isolation` | test-only kernel rigged to panic (`#[cfg(test)]` hook) | CALL returns an error; process alive; subsequent CALL succeeds |
| L-7 `emit_nodeid_roundtrip` | create vertices with known external ids; guest BFS; read YIELD | `nodeId` column contains the external Vids (IdMap slot→Vid reverse translation, e2e) |
| L-8 `handle_lifetime_per_loader` | script creates handles, exits scope without `free` (Rhai/PyO3); WASM `resource.drop`; Extism explicit `free` | table empty after invocation — each loader's §4.2 reclaim mechanism |
| L-9 `crossing_count_is_graph_size_invariant` | instrument the shim with a host-fn invocation counter; run the corpus PPR on \|V\|=10³ and \|V\|=10⁶ graphs (same iteration count) | **identical host-fn call counts** — the conductor thesis itself, measured. Any implementation that smuggles per-vertex or per-edge crossings (or marshals data through handles) scales with graph size and fails; this is the single most Goodhart-resistant test in the suite |

**Loader-specific gates:** WASM fixture exercises the WIT `resource` lowering specifically (H-1 forgery is *structurally impossible* there — the test instead asserts the guest cannot construct a resource it wasn't handed). PyO3's Phase-5 prerequisite gets its own gate test `pyo3_deadline_honored`: a guest sleeping past the deadline is interrupted with a typed error — this test failing = Phase 5 not started.

**Acceptance bar per shim phase:** the corpus (5 algorithms × loader) + L-1…L-8 green for that loader; conformance-probe IDs green via `uni-plugin-conformance`.

### 9.4 Full-pipeline e2e through Cypher CALL (uni-db integration binary)

| ID | Test | Asserts |
|---|---|---|
| E-1 `guest_ppr_via_call` | register Rhai guest plugin; `CALL guest.ppr(...) YIELD nodeId, score` | rows match native `uni.algo.pageRank` (ε); **over unflushed L0** (mirrors `first_party_reachability_bfs_over_l0`) |
| E-2 `both_dispatch_paths` | same CALL through both AlgorithmProvider dispatch paths | identical results (mirrors existing dual-path coverage) |
| E-3 `snapshot_semantics` | writer thread commits during projection build; separate: caller's own uncommitted tx writes | run completes; results consistent with §5.5's documented level (no torn vertices); uncommitted writes **not** visible; version stamp surfaced in result metadata |
| E-4 `graphrag_composition_smoke` | vector-KNN seeds → guest PPR → `fuse_rrf_multi` | the §6.1 v1 pipeline works end-to-end (seeding+fusion half only — no similarity-weighted expansion, per §6.1) |
| E-5 `determinism_e2e` | E-1 twice, and across thread counts | bitwise-identical batches — §5.3 holds through the whole stack, not just the kernel layer |
| E-6 `guest_perf_parity` (nightly) | guest-corpus PPR (Rhai) vs native `uni.algo.pageRank`, same graph (~10⁶ edges), same iteration count, wall-clock | guest ≤ **3×** native. This is the objective's teeth: L-9 proves crossings don't scale, E-6 proves the kernels behind them run at native speed. Without it, a correct-but-pathologically-slow implementation passes everything |

### 9.5 CI lanes

- **PR gate:** §9.1 + §9.2 (small proptest counts) + Rhai corpus (`rhai-plugins` is default-on) + §9.4, via nextest filter `test(/graph_compute/) and not test(soak)`.
- **Workspace run:** WASM/Extism corpus rides the loader crates' own test binaries after `scripts/build-wasm-fixtures.sh` (extended with the graph fixtures); PyO3 corpus in `uni-plugin-pyo3/tests/` behind its feature, as today.
- **Nightly:** `*soak*` variants — H-1 forgery at `GRAPH_COMPUTE_FUZZ_CASES=10k+`, M-1…M-5 at `METAMORPHIC_CASES` scale, F-row differential on random graphs (proptest, ~300-vertex), plus a **COST guard**: each kernel benchmarked against a single-threaded cache-friendly loop with a generous factor bound (prior-art P6 — regression-detected, not just promised).
- **TSan lane:** the handle table + rayon-internal kernels added to the existing nightly tsan job (kernels are internally parallel even though the session is externally single-threaded).

### 9.6 What green does NOT prove (residual holes, stated so they read as decisions)

Honesty about the suite's own limits — each hole is either accepted for v1 or covered structurally rather than behaviorally:

- **Mask/exclude *fusion* is untested as a perf property.** M-4 proves mask semantics; a materialize-then-filter implementation passes it. Partially caught by P0-8 (a materializing implementation does more work than its declared charge) and E-6; fully verifying fusion needs allocation-count instrumentation — deferred, accepted for v1.
- **Arrow-backing (§10 invariant) has no behavioral signature until `columnar-bridge@1` exists.** Locked structurally instead: a compile-time assertion that `Tensor::buf` is an Arrow buffer type. A `Vec<f64>`-backed implementation would fail *that*, not any e2e test.
- **A fully sequential (single-threaded) kernel engine passes every correctness and determinism test**, and passes COST trivially. E-6's 3× bound vs the (parallel) native algos is the only thing that catches it — which is why E-6 is in the acceptance bar and not merely a benchmark.
- **E-3 snapshot semantics is a smoke test, not a proof** — concurrent-write windows are timing-dependent; the documented §5.5 isolation level is validated by construction (`pin_snapshot`) more than by this test.
- **Coverage claims (~70%/~90%, §3) are argued by the F/C suites existing, not measured** — there is no demand-side metric in CI.
- **The suite cannot police its own maintenance.** Provision 3 (§9.0, append-only IDs) makes weakening *visible in review*; it cannot make it impossible. The one sanctioned behavior flip is P0-6 (pregel truncation → error); any other acceptance-test edit that loosens an assertion should be treated as a red flag in review.

---

## 10. Forward-compatibility / extensibility

The v1 artifact is deliberately a **tensor-general, Arrow-backed, pure-functional, capability-sliced kernel VM** that *implements only the scalar-graph slice*. This section records the futures it must not foreclose and the exact v1 invariant that keeps each one **additive** rather than breaking. We build none of these now — we only refuse to design them out. The discipline: pay in v1 *only* for what is breaking-if-deferred.

| Future | Enabled by (v1 invariant) | Added later as | Breaking if invariant omitted? |
|---|---|---|---|
| **GNN inference** (pretrained weights) | shaped-tensor handles (§4.2) + Arrow-backing | `tensor-compute@1`: `neighbour_aggregate(g, emb, agg)` [graph-aware segment-reduce] + `dense_layer(emb, W, b, σ)` [BLAS GEMM] over `[V,d]`/`[d,d']` tensors | **Yes** — scalar-only maps would force a new handle kind |
| **GNN training** | functional-purity of kernels (§4.1) | `autodiff@1`: `record`-mode op-tape + `backward()` + optimizer, *or* tape-export to an external trainer | **Yes** — in-place mutation would corrupt the tape |
| **Fused traversal + columnar** | Arrow-backed values (§4.1) | `columnar-bridge@1`: zero-copy `Tensor ↔ DataFusion column`, plus a `GraphCompute` **ExecutionPlan node** so a query fuses `scan → filter → graph-kernel → aggregate` | **Yes** — bespoke `Vec<f64>` storage would need re-plumbing every kernel |

**GNN message passing decomposes exactly onto two slices.** `h_v^{l+1} = UPDATE(h_v^l, AGGREGATE{h_u^l : u∈N(v)})`: the AGGREGATE is a **graph** kernel (`neighbour_aggregate` = segment-reduce of `[V,d]` embeddings over the CSR — sum/mean/max), the UPDATE is a **tensor** kernel (one `(V×d)·(d×d')` GEMM + activation). A GraphSAGE forward pass is ~10 orchestration lines alternating the two, with the `[V,d]` matrices never crossing the boundary — the conductor model fits message passing *natively*. **Training stays out of the kernel catalog**: backprop needs an autodiff engine + optimizer + loss, so the realistic shape is *train externally (PyTorch/JAX), import weights, infer in uni* — which is how HippoRAG/GraphRAG already consume pretrained embeddings (dovetails with §6).

**Feature/property plumbing serves both futures at once.** GNN node-features and property-aware analytics both need vertex/edge properties as tensors. Extend `GraphProjectionSpec` to request property columns — note it is **not** currently `#[non_exhaustive]` (that attribute is on `AlgorithmContext`, `algorithm.rs:33`; the spec at `:118` is plain `Clone/Debug/Default`) — adding fields is additive in practice because it is `Default`+builder-constructed, but **mark it `#[non_exhaustive]` in Phase 1** to make that guarantee structural → they land as `[V]`/`[V,d]` Arrow-backed tensor handles. One mechanism = the GNN feature matrix `X` *and* the analytics property columns; also unblocks the topology-only limitation flagged in §7.

**Honest limit on the fusion future.** Zero-copy *handoff* (Tensor↔column view) and an in-plan operator are genuinely deliverable. But **true push-down fusion** — the planner optimizing a columnar predicate *through* a graph kernel — is research-grade: DataFusion treats custom operators as optimization barriers, and few graph+relational engines cross it. Reserve the operator path; do **not** promise transparent cross-boundary optimization.

### 10.1 Grounding — what the tree already has vs. net-new (verified 2026-07-10)

So §10 is honest, not hopeful — an audit of the current tree for the GNN/tensor path:

| Building block | Status | Evidence |
|---|---|---|
| Dense `[V,d]` vector storage, Arrow-backed | **Exists** — `DataType::Vector{dim}` → `FixedSizeList<Float32,dim>` | `crates/uni-common/src/core/schema.rs:243-246` |
| One-hop neighbour aggregation | **Exists but scalar-only** — Locy `FeatureResolverKind::NeighborAggregate` (Avg/Max/Sum over one scalar property, 1-hop, Out/In/Both) | `crates/uni-query/src/query/df_graph/locy_fixpoint.rs:3176` (variant), `:3932` (precompute) |
| A GEMM path (`[N×d]·[d×d']`) | **Exists but narrow** — Candle `0.10` already a workspace dep, used for one single-layer inference matmul (`CandleLinearClassifier`) | `crates/uni-locy/src/neural.rs:539` (struct), `:655-662` (forward); workspace `Cargo.toml:177` |
| node2vec biased **walks** | **Exists** (`random_walks`' precedent: p/q second-order bias, seeded) | `crates/uni-algo/src/algo/algorithms/random_walk.rs:32-33` |
| General batched dense linalg / `neighbour_aggregate` over **vectors** | **Net-new** | no *direct* `ndarray`/`nalgebra`/`faer` dep (ndarray/nalgebra appear only transitively via lance-index/ort/statrs; faer absent); NeighborAggregate is scalar |
| In-process embedding model forward pass | **Net-new** — all embedding compute is external via `uni-xervo` | `crates/uni/Cargo.toml:84` (dep), `:105-107` (facade-only comment) |
| Autodiff / gradients / optimizer (**training**) | **Absent** — zero hits workspace-wide | confirms *train externally, import weights* |
| Arrow 2-D tensor extension type | **Net-new** — a `[V,d]` handle is `V` rows of `FixedSizeList<Float32,d>` today, not a `FixedShapeTensor` | — |

**What this means for the plan:** the `tensor-compute@1` slice is *less* net-new than it looks — Candle is already vendored (the GEMM primitive exists, just uni-locy-scoped), the `[V]`/`[V,d]` Arrow backing is the *existing* `FixedSizeList<Float32>` representation, and Locy's scalar `NeighborAggregate` is a direct conceptual precedent for the vector `neighbour_aggregate` kernel. Conversely, the audit **confirms** the training boundary: there is no autodiff anywhere, so `autodiff@1` is a genuine greenfield slice and the "train externally" framing is the honest one — not a hedge.

---

## 11. Decisions (ratified 2026-07-10 — the implementer builds against these; reopening one is a spec change)

| # | Decision | Ruling | Rationale |
|---|---|---|---|
| D1 | Handle representation | **WIT `resource` for WASM; packed `u64` (epoch\|kind\|gen\|slot) for Rhai/PyO3/Extism** — one logical model, two lowerings | The only genuinely unforgeable option where available; §4.2 documents the packed form as defense-in-depth, with fail-closed wrap handling |
| D2 | Determinism default | **Deterministic-by-construction for all kernels; NO `fast_nondeterministic` mode in v1** | Nondeterminism inside a composition primitive is uniquely corrosive (§5.3); a fast mode can be added later as an opt-in, the reverse migration cannot |
| D3 | Budget units | **Multiple of \|E\| AND absolute ceiling, both enforced** (values pinned in §12) | The multiple alone is unbounded on 10⁹-edge projections; the ceiling alone misconfigures easily |
| D4 | Semiring set | **Ship all 5 of §4.3** (`Count` stays removed — not a semiring; i64 `LinearAlgebra` covers counting) | The 5 are each load-bearing for a specific F-row algorithm; starting with 3 just delays two minor bumps |
| D5 | Cross-CALL `AlgoSession` | **Deferred.** Session strictly per-CALL in v1 | No lifetime anchor exists (bridge is per-invocation); consistently defers handle-composition pipelines *and* write-back (§7); revisit alongside `tensor-compute@1` (§10) |
| D6 | Four forward-compat invariants | **Adopted, all four** — shaped-tensor handles, Arrow-backed values, pure-functional kernels, capability slices | Each cheap now, breaking-if-deferred; purity's allocation cost is handled by `free` (§4.2) + COW + budget, and every signature (incl. `scatter`) honors it |
| D7 | Guest arg marshaling | **Keep the positional-JSON `config_json` contract; add host-side validation against declared `NamedArgType`s** (§4.6) | Preserves the shipped native-provider contract; typing moves to the one place all four loaders share |
| D8 | Error codes | **Reserve block `0x860–0x87F`** inside the `0x8xx` algorithm family (§12) | `0x800-0x805` host/bridge, `0x810+`/`0x820+`/`0x850+` taken by existing providers; `0x860+` is free |

---

## 12. Pinned constants (normative — changing one is a spec change, per §9.0)

**Error codes — GraphCompute block `0x860–0x87F`** (inside the `0x8xx` algorithm family; framework range `0x00–0xFF` and existing allocations `0x800-0x805`/`0x810+`/`0x820+`/`0x830+`/`0x850+` untouched):

| Code | Name | Raised by |
|---|---|---|
| `0x860` | `StaleHandle` | generation mismatch (use-after-free) |
| `0x861` | `KindMismatch` | Set where Tensor expected, etc. |
| `0x862` | `ShapeMismatch` | tensor shape/dtype vs kernel requirement |
| `0x863` | `EpochMismatch` | cross-session / forged handle |
| `0x864` | `ArenaCapExceeded` | allocation past bytes/handle cap (§5.1) |
| `0x865` | `BudgetExhausted` | native-work meter at zero (§5.1) |
| `0x866` | `IterationLimit` | convergence-loop cap (§5.2) |
| `0x867` | `Timeout` | wall-clock deadline (§5.2) |
| `0x868` | `SeedNotInProjection` | `frontier` given an unmapped Vid (§4.3) |
| `0x869` | `EmitSchemaMismatch` | `emit` cols vs declared `output_fields` (§4.6) |
| `0x86A` | `SliceVersionMismatch` | manifest wants `graph-compute@N` host lacks (§4.3) |
| `0x86B` | `WrapFailClosed` | generation/epoch wrap rejection (§4.2) |
| `0x86C` | `CapabilityDenied` | kernel call without `graph-compute` grant (§4.6) |

`0x865–0x867` are the three variants of `UniError::GraphComputeIncomplete` (P0-7 asserts they surface distinctly). `0x86D–0x87F` reserved for future kernels in this slice.

**Resource defaults** (pub consts in the crate owning `AlgoSession`, per the existing per-loader-consts convention — `DEFAULT_MAX_OPERATIONS` engine.rs:40, `DEFAULT_TIMEOUT_MS` loader.rs:21 — overridable per plugin via new capability quota variants, same pattern as `FuelPerCall`):

| Const | Default | Notes |
|---|---|---|
| `DEFAULT_WORK_EDGE_MULTIPLIER` | `10_000` | budget = min(multiplier × (\|V\| + \|E\|), ceiling) — D3 (retuned, see below) |
| `DEFAULT_WORK_ABS_CEILING` | `1_000_000_000` | 1e9 work units (≈ edges touched) |
| `BUDGET_CHECK_CHUNK` | `65_536` | in-kernel check granularity; P0-4 overshoot bound |
| `DEFAULT_ARENA_MAX_BYTES` | `256 MiB` | host-side handle arena (§5.1) |
| `DEFAULT_ARENA_MAX_HANDLES` | `4_096` | also the generation-wrap horizon per slot (§4.2) |
| `DEFAULT_MAX_SUPERSTEPS` | `10_000` | iteration cap behind `0x866` |
| quota variants | `Capability::GraphComputeWork(u64)`, `Capability::GraphComputeArenaBytes(u64)` | manifest-declarable, intersected with grants like all quotas |

P0-9 (§9.1) asserts these defaults are installed on the CALL path; the values themselves are starting points and may be tuned by the E-6/COST nightly data — via a change to this table.

**Ratified retune (implementation):** `DEFAULT_WORK_EDGE_MULTIPLIER` was moved from `100 × |E|` to `10_000 × (|V| + |E|)`. The original `100 × |E|` was inconsistent with `DEFAULT_MAX_SUPERSTEPS = 10_000` — an iterative algorithm running a few `O(V + E)` passes per iteration exhausted the meter at ~25 iterations, far below the superstep cap — and it undercounted the `O(V)` per-vertex kernels (`map_apply`, `reduce`, `ewise`, `scatter`) which the meter charges `|V|` each (§5.1). Basing the allowance on `|V| + |E|` matches what a pass actually touches, and `10_000` lets a default run fit comfortably below the superstep cap while an unbounded loop still hits the finite `DEFAULT_WORK_ABS_CEILING`. §9.0 provision-2 permits tuning these starting-point values; the code (`DEFAULT_WORK_EDGE_MULTIPLIER` in the crate owning `AlgoSession`) is authoritative and this table now matches it.

**Trait-extension notes for the implementer:** `AlgorithmSignature` gains `args`/`slices` as `Default`-ed fields (§4.6, additive); `GraphProjectionSpec` gets `#[non_exhaustive]` in Phase 1 (§10); `Capability` gains `GraphCompute` + the two quota variants above (kebab-case serde, matching `capability.rs:30` conventions).
