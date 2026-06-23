# Design Proposal: Multi-vector (late-interaction / ColBERT) storage + MaxSim retrieval

- **Issue:** rustic-ai/uni-db#96
- **Status:** Phases 1 **and 2** IMPLEMENTED (Rust + Cypher scope), uncommitted. **Phase 1**:
  storage `List<Vector>` column, in-process `maxsim()`, `reranker: "maxsim"` mode. **Phase 2**:
  native first-stage index over multivector columns — the **full `VectorIndexType` menu**
  (Flat/IVF_FLAT/PQ/SQ/RQ/HNSW_FLAT/SQ/PQ), `nprobes`/`refine_factor` query options, the
  `uni.vector.query` procedure path **and** the inline `vector_similarity()` predicate, plus
  index-creation fixes (metric-from-OPTIONS, numeric-OPTIONS parsing, PQ `dim%sub_vectors`
  validation). Tests green (6 Phase-2 + Phase-1 regression). **Deferred:** multivector L0-merge
  (flush-before-query), fork/branch multivector (clear error), multivector auto-embed, Python
  OGM, Phase 3 MUVERA. Validated by the real-data recall benchmark (§2.4).
- **Date:** 2026-06-22 (Phase 1), 2026-06-23 (Phase 2)
- **Producer dependency:** rustic-ai/uni-xervo#41 (per-token vector emission). Dependency arrow is **uni-db → uni-xervo**: this proposal is the *consumer/storage* half and does not require the producer to land first (multivecs can be written directly).

**Validation summary (2026-06-22).** Tier-0 (does Lance do it) and Tier-1 (does it fit our
plumbing) are both confirmed — see §2.1 / §2.2. Native multivector storage + MaxSim works on
the pinned `lance 7.0.0` / `lancedb 0.30.0` through our production Table API with **no index
required**, and the uni-db query/rerank surface needs only small additive code (no
architectural change). The only unresolved item is **product scope** (§13): whether the
consumer needs rerank-only Phase 1 or first-stage Phase 2.

## 1. Summary

Support **storing a variable-count set of vectors per row** (per-token / ColBERT) and
**MaxSim scoring** (`Σ_i max_j (q_i · d_j)`), so late-interaction retrieval (ColBERT,
ColQwen2, ColPali) runs natively in uni-db — first as a **rerank stage** over the existing
dense ANN, later as an optional **first-stage** index.

## 2. The pivotal finding: Lance already does the hard part

The hardest-looking piece of #96 — a hand-rolled MaxSim operator and a new index type — is
**already implemented in the pinned dependency**. `lance = "7.0.0"` / `lancedb = "=0.30.0"`
(`Cargo.toml`, `Cargo.lock`) ship native multi-vector support:

- **Storage shape:** Lance treats a `List<FixedSizeList<Float32, dim>>` column as a
  multi-vector column (`lance-index-7.0.0/src/vector/flat.rs` branches on `DataType::List(_)`;
  `vector/transform.rs` `Flatten`/`KeepFiniteVectors` expand List→FixedSizeList).
- **MaxSim scoring:** `lance-linalg-7.0.0/src/distance.rs::multivec_distance_impl` computes
  exact ColBERT MaxSim — per query sub-vector, `max_by` over the doc's sub-vectors, then
  `sum` across query sub-vectors.
- **Query plan:** `lancedb-0.30.0/src/table/query.rs` detects a List-typed vector column +
  multiple query vectors and routes to `create_multi_vector_plan()`.
- **Index types:** Flat is fully multivector-aware; HNSW/IVF use the flat index as their
  sub-index and inherit it. There is **no separate "ColBERT index" param** — you store a
  List column and MaxSim scoring is automatic on query.

**Consequence:** #96 in uni-db is mostly a **data-model + plumbing** task — thread a
multi-vector value/type through the schema, write/read codec, index config, and query
surface — *not* a numerical-kernel task. This materially lowers risk and scope.

### 2.1 Empirically confirmed (2026-06-22)

The above is no longer a read-from-source assumption. A standalone probe —
`crates/uni-store/examples/multivec_lance_probe.rs` (run:
`cargo run -p uni-store --example multivec_lance_probe`) — validates it against the
**production `lancedb::Table` API** uni-store already uses (`backend/lance.rs::vector_search`):

- A `List<FixedSizeList<Float32, dim>>` column accepts a **variable token count per row**
  (the probe stores rows of 1, 2, and 3 token vectors).
- A multi-vector query built by chaining `vector_search(tok0).add_query_vector(tok1)…`
  returns **MaxSim-ordered** results (`Σ_i max_j q_i·d_j`), with **no vector index created** —
  i.e. brute-force MaxSim works out of the box (this *is* the Phase-1 path).
- Both `Dot` and `Cosine` reproduce canonical MaxSim ordering on unit-norm vectors
  (Tier-0 metric question resolved → **default to Cosine**, with Dot as the raw-dot option).
  Lance returns `_distance` monotonically decreasing in MaxSim (observed
  `_distance ≈ 1 − MaxSim` on the orthonormal fixture); the rerank stage can either compute
  MaxSim directly or use `score = −_distance`.

> Still to benchmark (Phase 2 only): HNSW/IVF *recall* over multivector columns in 0.30.0.
> The probe exercised the **Flat / no-index** path, which is all Phase 1 needs.

### 2.2 Integration surface confirmed (2026-06-22, code-read)

The two uni-db-side Tier-1 risks are clear — no blockers, only small additive code:

- **Query plumbing carries `[[f32]]` losslessly.** `Value::List(Vec<Value>)` nests, and the
  Cypher map-literal eval (`df_graph/locy_eval.rs`), `$param` binding, and the MessagePack
  codec (`cypher_value_codec.rs` `TAG_LIST`) are all recursive — no flattening/coercion. The
  only new code is an `extract_vector_list` helper paralleling the existing single-vector
  `extract_vector` (`df_graph/search_procedures.rs:58`).
- **The rerank slot can fetch a stored multivec per candidate.** The fetch API
  `property_manager::get_batch_vertex_props_for_label` (`runtime/property_manager.rs:924`)
  returns `HashMap<Vid, HashMap<String, Value>>` — fully generic over property type and
  **batched** (one call, no per-candidate round-trips), exactly the shape MaxSim wants. The
  *only* text-specialization is a single hardcoded `.as_str()` at
  `search_procedures.rs:159`; the change is to branch on reranker type (text→cross-encoder,
  multivec→MaxSim) and extend `RerankerConfig` with an optional `maxsim_query: Vec<Vec<f32>>`.

Remaining unrun validation is **#5 storage codec round-trip** — but that *is* the Phase-1
storage work item (the `build_list_column` arm + read decoder), not a separate risk: the
`Value`-space MessagePack path already round-trips nested lists; only the Arrow column
builder/decoder needs the new branch.

### 2.3 Follow-ups landed (2026-06-23)

- **Python OGM** — `list[Vector[N]]` Pydantic fields now map to `List(Vector{dim})`
  (`bindings/uni-pydantic/.../types.py`). Surfaced + fixed a **latent binding bug**:
  `bindings/uni-db/src/core.rs::parse_data_type` used `split(':').nth(1)` for `list:`, dropping
  trailing segments, so `list:vector:N` (and `list:list:string`) failed — fixed to
  `strip_prefix("list:")`.
- **Schemaless multivec = design boundary (no code).** Nested-list property values are rejected
  at write by `validate_structural_property_value` (`write.rs:2931`) for non-`CypherValue`
  columns (standard OpenCypher), so a multivec must be a declared `List(Vector{dim})` **or** a
  declared `CypherValue`/json column (the latter a dim-flexible alternative that maxsim handles).
- **Cypher DDL parameterized types** — `CREATE LABEL Doc (... tokens LIST<VECTOR(2)>)` now parses
  (new compound-atomic `type_specification` rule in `cypher.pest`), so multivec is declarable via
  Cypher DDL, not just the builder API. (`MAP<...>` deferred — needs a backend `parse_data_type`
  arm.)

### 2.4 Phase-2 recall benchmark — GO on real data (2026-06-23)

Two benchmarks were run. The first used **synthetic random** vectors
(`multivec_recall_bench.rs`) and gave a misleading NO-GO (IVF recall ~0.30–0.40). Random
vectors are a worst case for ANN — they have no cluster structure for IVF to exploit, and that
run did not use `refine_factor`. **On real embeddings the conclusion reverses.**

The authoritative benchmark (`multivec_recall_real.rs`) uses **real ColBERT embeddings**: 2000
scifact passages + 100 real queries encoded by the local **uni-xervo** `answerai-colbert-small`
model (dim 96, 375k tokens), scored through the pinned Lance stack. `recall@10` is averaged over
all 100 queries:

| Config | recall@10 | latency |
|---|---|---|
| no-index (Flat, exact MaxSim) | **1.000** | 1580 ms/query (baseline) |
| **IVF_PQ + refine×10, nprobes=8** | **0.966** | **154 ms/query (~10× faster)** |
| IVF_PQ + refine×10, nprobes 16–256 | 0.964–0.967 | 154–178 ms/query |
| IVF_HNSW_SQ, nprobes=256 (no refine) | 0.721 | 78 ms/query (below gate) |

**Decision: GO — a native first-stage multivector index is viable.** `IVF_PQ` with
`refine_factor` (re-rank the index candidates with exact distances) reaches **recall@10 ≈ 0.966
at nprobes=8** — clearing the 0.95 gate — at a **~10× speedup** over exact brute-force MaxSim.
Recall is *flat across nprobes*, so the cheapest probing already suffices. This is a conservative
test: at 2000 docs brute force is only 1.6 s; IVF's advantage grows with corpus size (linear vs
sublinear), so the speedup is larger at scale. PQ *without* refine (and HNSW_SQ) miss the gate —
**`refine_factor` is the load-bearing knob.**

Caveats: measured on one corpus (scientific) + one model (96-dim); recall may vary by
domain/model/dim, and the index stores full vectors (for refine) + PQ codes. Recommend
re-confirming on the consumer's model/corpus before shipping, but the result is a clear,
data-backed reversal of the synthetic NO-GO.

> Correction note: the earlier "NO-GO / structural mismatch" claim (synthetic data) was wrong.
> The indexed path does *not* fundamentally mis-rank MaxSim — PQ discards precision that
> `refine_factor` recovers. Real-data validation overturned a premature negative.

## 3. Current state (what blocks us today)

| Layer | Today | File anchor |
|---|---|---|
| Schema type | `DataType::Vector { dimensions }` — one dense vector/property | `crates/uni-common/src/core/schema.rs:154` |
| Arrow lowering | `Vector` → `FixedSizeList(Float32, N)`; **`List(inner)` already recurses** → `List(inner.to_arrow())` | `schema.rs:203`, `:209-213` |
| Value | `Value::Vector(Vec<f32>)` only | `crates/uni-common/src/value.rs:551` |
| Write (column build) | `build_list_column()` handles only `List<Utf8\|Int64\|Float64\|LargeBinary>`; **errors on any other inner type** | `crates/uni-store/src/storage/arrow_convert.rs:1571` |
| Write (vector build) | `build_vector_column()` enforces fixed stride of exactly `dimensions` f32 | `arrow_convert.rs:740`, `:1443` |
| Read | `FixedSizeListArray` → `Value::List`; no `List<FixedSizeList>` round-trip | `arrow_convert.rs:350` |
| Index config | `VectorIndexType` {Flat, IvfFlat, IvfPq, IvfSq, IvfRq, HnswFlat, HnswSq, HnswPq}; metrics L2/Cosine/Dot | `crates/uni-store/src/storage/index_manager.rs:221-307` |
| Query surface | `uni.vector.query` / `uni.fts.query` / `uni.search`; planner `VectorKnn`; scalar `cosine_similarity`/`score_vectors` | `crates/uni-query/src/procedures_plugin/vector.rs:30`, `planner.rs:2045`, `uni-query-functions/src/similar_to.rs:219` |
| Rerank | Async cross-encoder over-fetch→score→truncate, options-driven, already wired into all 3 search procedures | `crates/uni-query/src/query/df_graph/search_procedures.rs:85-198` |

**Key observation:** the schema's `to_arrow()` already produces the correct Arrow shape for a
multivector — the only write-path blocker is the explicit reject in `build_list_column`. And
the **rerank stage is already a pluggable async slot** we can hang MaxSim off without new
plumbing.

## 4. Proposed data model

### 4.1 Type and Value

Two viable encodings; **recommend Option A** for minimal surface area:

- **Option A — reuse `List(Vector{dim})` (recommended).** No new `DataType` variant. The
  type is already expressible and already lowers to `List<FixedSizeList<Float32, dim>>` via
  the existing recursion at `schema.rs:213`. Work is concentrated in the write/read codec and
  query recognition. Risk: `List<Vector>` is structurally a generic list, so we must guard
  that index creation / MaxSim only apply where the inner type is `Vector`.
- **Option B — new `DataType::MultiVector { dimensions }`.** Self-describing and
  unambiguous for index/query dispatch, but adds a variant that must be threaded through
  *every* `match` on `DataType` (schema validation, codec, Python bindings, OGM, docs). Higher
  blast radius.

Decision driver: Option A minimizes churn and rides existing `List` plumbing; choose B only
if the ambiguity guards in Option A prove fragile in review. **This proposal assumes A.**

For the in-memory `Value`, add `Value::MultiVector(Vec<Vec<f32>>)` (or accept
`Value::List(Vec<Value::Vector>)` and normalize). A dedicated variant is cleaner for the
codec and for MaxSim arg-typing; it parallels the existing `Value::Vector`.

### 4.2 Write path

- Extend `build_list_column` (`arrow_convert.rs:1571`) with a `DataType::Vector { dimensions }`
  inner arm that builds `ListBuilder<FixedSizeListBuilder<Float32>>`, validating each inner
  vector is exactly `dimensions` wide and the per-row count is ≥ 1.
- Mirror the validation already in `extract_vector_f32_values` (`arrow_convert.rs:740`) so
  the failure modes (wrong dim, non-numeric) match single-vector behavior.

### 4.3 Read path / round-trip

- Detect `List<FixedSizeList<Float32>>` in the read decoder (`arrow_convert.rs:350` area) and
  reconstruct `Value::MultiVector` rather than a nested `Value::List`.
- Add a round-trip test (write `MultiVector` → flush → reopen → read) — this codec surface
  has bitten us before (see the bytes mis-decode sweeps), so a dedicated test is mandatory.

### 4.4 WAL / schemaless

- `Mutation` carrying property writes must encode `MultiVector` (CypherValue/MessagePack path
  like other non-primitive values). Confirm schemaless ingest (no declared type) round-trips
  a multivec via the CV codec, since `List<FixedSizeList>` won't be inferred without a hint.

## 5. Retrieval modes

### Phase 1 — Rerank-only (no new index; the "good first increment" #96 names)

This reuses the **existing rerank slot** end-to-end:

1. Run the normal dense-ANN candidate fetch (`uni.vector.query` / `uni.search`) to get top-K.
2. In `rerank_candidates` (`search_procedures.rs:132`), add a `maxsim` reranker mode that,
   instead of calling the cross-encoder, reads the candidate's stored `MultiVector` property
   and computes MaxSim against the query multivec.
3. Reuse the existing `RerankerConfig` / `parse_reranker_options` plumbing
   (`search_procedures.rs:85-120`) — add options like
   `{reranker: "maxsim", maxsim_property: "tok_emb", maxsim_query: <multivec>}` — and the
   existing `rerank_score` yield column (`procedures_plugin/vector.rs:71`).

MaxSim itself: call Lance's `multivec_distance` directly, or add a `max_sim(query, doc)`
scalar next to `score_vectors` in `uni-query-functions/src/similar_to.rs:219` for
WHERE/RETURN use. Prefer delegating to Lance's kernel to stay numerically identical to the
index path.

**This phase needs no storage *index* — only the storage *column* (§4) + the rerank branch.**
It is the smallest shippable increment and immediately useful to the uniko consumer.

### Phase 2 — Native multivector first-stage index (optional)

Once the column exists, expose Lance's native multivector retrieval as a first-stage:

- Add a guard in `create_vector_index` (`index_manager.rs:203`) allowing the target property
  to be a `List<FixedSizeList<Float32>>` column; pass it to Lance's existing index params
  (Flat first; benchmark HNSW/IVF recall before enabling).
- Extend the planner `VectorKnn` (`planner.rs:2045`) / `GraphVectorKnnExec`
  (`df_graph/vector_knn.rs`) to carry a multivector query and route to Lance's multi-vector
  plan. The Cypher surface (`uni.vector.query`) gains a multivec query argument.

### Phase 3 — MUVERA (arXiv:2405.19504), optional/research

Fixed-dimension encoding that approximates MaxSim using the **existing single-vector ANN**,
then MaxSim-rerank. Pure add-on: an encoder that maps a multivec → one dense vector stored in
a normal `Vector` column, reusing today's ANN unchanged, with Phase-1 MaxSim as the rerank.
Defer until Phases 1–2 are proven.

## 6. Producer integration (uni-xervo#41)

uni-xervo currently emits single-vector only and "explicitly defers the index to uni-db"
(`uni-xervo/xervo-multimodal-api-proposal.md`). Auto-embed for multivecs is therefore blocked
on uni-xervo#41. Until then:

- Phase 1/2 are exercised by **writing pre-computed multivecs directly** (Cypher/Python set a
  `MultiVector` property).
- When uni-xervo#41 lands, wire a late-interaction model alias into the existing auto-embed
  path so writes can emit per-token vectors, mirroring today's single-vector auto-embed.

## 7. Storage cost & mitigations

Per-token vectors are large (N tokens × dim × 4 bytes/row). Mitigations, in priority order:

1. **Token pooling** (cluster/pool tokens) at the producer — biggest win, no uni-db change.
2. **Quantization** — Lance already supports PQ/SQ/RQ; the multivector Flat sub-index can be
   quantized. Confirm quantized multivector recall in 0.30.0.
3. Document the footprint prominently; default to rerank-only over a *small* candidate set.

## 8. Surface / API sketch (subject to review)

```cypher
-- write (direct, pre-computed multivec)
CREATE (d:Doc { title: "...", tok_emb: $multivec })   -- $multivec = [[...dim...], [...], ...]

-- Phase 1: dense ANN then MaxSim rerank
CALL uni.vector.query('Doc', 'dense_emb', $dense_q, 200,
     null, null,
     { reranker: 'maxsim', maxsim_property: 'tok_emb', maxsim_query: $multivec_q })
YIELD vid, score, rerank_score
RETURN vid, rerank_score ORDER BY rerank_score DESC LIMIT 10
```

Python (uni_db / uni_pydantic) and OGM schema get a `MultiVector` property type mirroring the
existing `Vector` field; details in implementation.

## 9. Test plan

- **Codec round-trip:** write → flush → reopen → read `MultiVector` (declared + schemaless).
- **MaxSim correctness:** uni-db `max_sim` vs an independent reference (and vs Lance's
  `multivec_distance`) on hand-computed fixtures.
- **Phase-1 rerank:** assert MaxSim reorders a dense-ANN candidate set; `reranker_k` over-fetch
  honored; pre-computed vs auto-embed query paths.
- **Phase-2 index:** recall@k vs brute-force MaxSim on a fixed corpus; Flat first, then
  HNSW/IVF gated on the recall benchmark.
- **Bindings:** Python set/get of a multivec property; OGM field round-trip.
- Gates: nextest workspace, TCK, Locy, pytest, clippy/fmt — per repo CI.

## 10. Scope boundaries

- **In:** multivector storage column, `Value::MultiVector` + codec, MaxSim scalar/rerank
  (Phase 1), optional native multivector index (Phase 2).
- **Out (separate work):** producer per-token emission (uni-xervo#41); MUVERA (Phase 3);
  auto-embed wiring (blocked on #41).

## 11. Recommended sequencing

1. **Phase 1** — DONE. Storage column + `maxsim` rerank, no index; unblocks the rerank use case.
2. **Benchmark** — DONE (§2.4). Real-data result: `IVF_PQ + refine_factor` clears the recall gate
   at ~10× speedup → **Phase 2 is viable.**
3. **Phase 2** native first-stage index — **DONE.** The full `VectorIndexType` menu builds over a
   `List<Vector>` column; a multivector query threads through procedure + inline-predicate paths via
   the `add_query_vector` chain; `nprobes`/`refine_factor` are query options. Storage seams:
   `multivector_search` in `backend/lance.rs` + `manager.rs`; routing in `search_procedures.rs`
   (`run_vector_query`) and `vector_knn.rs` (`GraphVectorKnnExec`); index-creation fixes in
   `planner.rs` (metric + numeric OPTIONS) and `index_manager.rs` (PQ dim guard). **Deferred:**
   multivector L0-merge (Lance's internal MaxSim distance scale can't be matched against in-process
   MaxSim without re-reading candidate props — flush before query meanwhile); fork/branch multivector;
   auto-embed.
4. **Phase 3 / auto-embed** once uni-xervo#41 lands.

## 12. Open questions

- ~~Confirm HNSW/IVF (not just Flat) multivector recall in Lance 0.30.0~~ — **resolved (§2.4):
  GO on real data.** `IVF_PQ + refine_factor` reaches recall@10 ≈ 0.966 at nprobes=8 (~10×
  faster than brute force) on real ColBERT embeddings. (Synthetic-data NO-GO was a false
  negative — no cluster structure + no refine.) Native first-stage index is viable.
- Option A (`List(Vector)`) ambiguity guards vs Option B (`MultiVector` variant) — settle in review.
- Max per-row vector count / dim caps and host-memory floors for MaxSim brute-force.
- ~~Distance metric semantics for MaxSim~~ — **resolved (§2.1): default Cosine** (unit-norm
  vectors), Dot exposed as the raw-dot option. Confirm the producer emits normalized vectors
  so Cosine is the right default.

## 13. Decision needed before implementation

Validation is complete; what remains is a **product-scope call**, not a technical one:

- **Does the consumer (uniko) need only Phase 1 (rerank-only MaxSim), or also Phase 2
  (first-stage multivector retrieval)?** Phase 1 is fully de-risked and useful on its own;
  Phase 2 additionally needs the HNSW/IVF recall benchmark (§12). This decides how much to build.
- **Which late-interaction model / dimension** (e.g. ColBERTv2 128-dim vs ColQwen2)? Sets the
  fixture and the default `dimensions`.

Until then, this document + the retained probe are the deliverable; no feature code is written.
