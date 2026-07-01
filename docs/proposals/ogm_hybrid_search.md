# Design Proposal: OGM `hybrid_search()` builder (dense + text + sparse)

- **Issue:** rustic-ai/uni-db#114 (`enhancement`, `python`); relates to #95 (M4 sparse), #122 (auto-embed parity).
- **Status:** **Implemented** (2026-07-01). `hybrid_search()` + the `.search_scores` sidecar landed in
  `uni-pydantic`; the sidecar was retrofitted onto `vector_search`/`sparse_search` too. Implementing it
  surfaced and fixed a real latent bug: the existing `vector_search`/`sparse_search` emitted
  `RETURN node AS n`, which cannot hydrate (needs the `properties`/`id`/`labels` triple), so their
  `.all()` silently returned `[]` — now fixed and guarded by tests. See "Implementation notes" below.
- **Author:** (design)
- **Date:** 2026-06-30 (rev. 2026-07-01 — design + plan updated against a 20+-library ecosystem survey
  and a read of the real `query.py`/`base.py` internals; see §9).
- **Scope:** `bindings/uni-pydantic/src/uni_pydantic/query.py` (builder + Cypher emission + a scored
  hydration path), a one-line `PrivateAttr` on `bindings/uni-pydantic/src/uni_pydantic/base.py`
  (`UniNode`, for the score sidecar — §8), and `bindings/uni-pydantic/tests/test_queries.py`
  (string-generation + score-surfacing unit tests). **No Rust changes.**

## 1. Problem

The Pydantic OGM query builder exposes single-source retrieval — `vector_search()` (dense, via
`uni.vector.query`) and, since #95 M4, `sparse_search()` (learned-sparse, via `uni.sparse.query`).
It does **not** expose a builder for the engine's three-way fused retrieval (`uni.search`). Today a
Python user reaches 3-way hybrid only by hand-writing raw Cypher:

```python
rows = await session.query(
    "CALL uni.search('Doc', {vector:'embedding', fts:'content', sparse:'emb'}, "
    "$qtext, $qvec, 10, null, {method:'rrf', sparse_query:$sq}) "
    "YIELD node, score, vector_score, fts_score, sparse_score "
    "RETURN node AS n, score ORDER BY score DESC",
    {"qtext": "...", "qvec": [...], "sq": {"indices": [...], "values": [...]}},
)
```

— losing the typed, model-bound ergonomics the OGM exists to provide.

**Why this is worth doing (ecosystem context, §9).** A 2026-07 survey of 20+ ORMs / OGMs / vector
clients found that *no* graph database or graph OGM exposes fused three-way hybrid (dense + fulltext +
sparse) as a single typed, model-bound call — nobody in the graph world even does 3-way — and that
*every* mature SQL ORM (SQLAlchemy/pgvector, Django, SQLModel, Prisma, Drizzle, Ent) stops at
single-source vector search and punts fusion to hand-written raw SQL/CTEs. uni's engine already crossed
that line; this binding is what turns a rare engine capability into a usable one. The survey also
settles the two decisions #114 flags for deferral (API shape §3, score surfacing §8) — see §9.

## 2. Engine surface — ground truth (verified)

> **The issue body's example Cypher is inaccurate and must not be implemented as written.**
> It shows a 3-argument call `uni.search('Label', {props}, {options})`. The registered procedure
> takes **seven positional arguments**. Building the 3-arg form will fail argument validation.

Registered at `crates/uni-query/src/procedures_plugin/search.rs:119-126` (`HybridSearchProc`);
body and argument parsing at `crates/uni-query/src/query/df_graph/search_procedures.rs:1440-1670`;
YIELD schema at `crates/uni-query/src/procedures_plugin/vector.rs:112-122`. Live usage:
`crates/uni/tests/sparse_scoring.rs:143`.

### 2.1 Positional signature

```
uni.search(label, properties, query_text, query_vector, k, filter, options)
            0      1           2           3             4  5       6
```

| # | Arg | Type | Required | Notes |
|---|-----|------|----------|-------|
| 0 | `label` | String | yes | node label |
| 1 | `properties` | map \| string | yes | source→property map (see §2.2) |
| 2 | `query_text` | String | **yes** (`require_string_arg`) | drives FTS **and** dense auto-embed |
| 3 | `query_vector` | List\<Float\> \| null | no | precomputed dense; `null` ⇒ auto-embed from `query_text` |
| 4 | `k` | Int | **yes** (`require_int_arg`) | top-k |
| 5 | `filter` | String \| null | no | pre-filter predicate |
| 6 | `options` | map | no | fusion + reranker knobs (see §2.3) |

### 2.2 `properties` map — bare property-name strings

Keys are case-sensitive; each **value is a property name string**, *not* a `{property, query}` map
(`search_procedures.rs:1455-1478`):

```
{vector: 'embedding', fts: 'content', sparse: 'emb'}
```

A bare string (e.g. `'content'`) is shorthand: same property for `vector` + `fts`, sparse off.
The queries themselves come from the positionals/options, **not** this map.

### 2.3 `options` map (case-sensitive keys)

| Key | Type | Default | Applies to |
|-----|------|---------|-----------|
| `method` | `"rrf"` \| `"weighted"` | `"rrf"` | fusion strategy |
| `alpha` | Float | `0.5` | **two-way** weighted (vector vs fts) blend |
| `weights` | `[v, f, s]` Float×3 | `[⅓,⅓,⅓]` | **three-way** weighted blend |
| `rrf_k` | Int | `60` | RRF constant |
| `over_fetch` | Float | `2.0` | ×k candidate over-fetch |
| `sparse_query` | SparseVector \| `{indices,values}` | — | **required to enable the sparse arm** |
| `reranker` | String (alias \| `"maxsim"`) | — | second-stage rerank |
| `reranker_property` / `reranker_k` / `reranker_query` | — | — | rerank knobs |
| `maxsim_query` / `maxsim_metric` | — | — | only when `reranker:"maxsim"` |

ANN-tuning knobs (`nprobes`/`refine_factor`/`ef_search`) are **not** plumbed through `uni.search`
(it calls dense search with `VectorQueryOpts::default()`, `search_procedures.rs:1544`). Out of scope.

> **Default-`alpha` note (from the survey).** uni's weighted `alpha` defaults to `0.5` (neutral).
> The field norm leans toward the vector arm — Weaviate defaults hybrid `alpha` to `0.75`, LanceDB's
> `LinearCombinationReranker` to `0.7`. `0.5` is a defensible neutral choice, but call it out in the
> docstring so users porting from Weaviate/LanceDB aren't surprised by different rankings.
> A future `method:"dbsf"` (distribution-based score fusion — normalize each arm by mean/variance, as
> in Qdrant/Haystack/LlamaIndex) is the natural next fusion mode; it is not in the engine today, so it
> is out of scope here but noted for the roadmap.

### 2.4 Three constraints the issue glosses over (each shapes the API)

1. **`query_text` is a single shared positional** — FTS matching and dense auto-embed read the same
   string. You cannot give FTS one query and dense-auto-embed a different one.
2. **`query_vector` is separate** from `query_text` — precomputed dense lives in arg 3; pass `null`
   to auto-embed.
3. **Sparse needs both halves** — `sparse:'prop'` in the map **and** `options.sparse_query`. Either
   alone is a silent no-op (`search_procedures.rs:1573-1595`).

### 2.5 YIELD columns (`vector.rs:112-122`)

`node` (vid source → expands to node), `score` (fused), `rerank_score`, `vector_score`,
`fts_score` (BM25 normalized by max), `sparse_score` (raw dot), `distance` (raw dense).

## 3. Proposed Python API

Two viable shapes. They produce identical Cypher for the common case (precomputed dense + FTS text
+ sparse); they differ only in where the per-source query lives. #114 flagged this as the reason for
deferral.

**Resolved by the survey (§9): adopt Option A.** The closest comparable — **LanceDB, which shares
uni's Lance storage engine** — offers *both* a shared-query default *and* per-source `.vector()` /
`.text()` overrides on one builder. Option A below **is** that pattern (a shared `query_text=` default
plus per-source tuples), so it is no longer a compromise but the field-validated shape. The one
awkward edge — a single shared `query_text` driving both FTS and dense auto-embed (§2.4.1) — is
*inherent to the engine* and is the identical constraint Weaviate ships (`hybrid(query=…, alpha=…)`),
so it is not a wart Option A introduces.

### Option A — tuple-per-source (the issue's proposed shape) — *recommended, survey-validated*

```python
results = await (
    Model.query(session)
    .hybrid_search(
        vector=("embedding", query_vec),     # (property, precomputed dense vec)
        fts=("content", "query text"),        # (property, FTS query string)
        sparse=("emb", sparse_query),         # (property, sparse query)
        method="rrf",                         # or "weighted" + weights=/alpha=
        k=10,
    )
    .all()                                    # -> list[Model]; each carries .search_scores (§8)
)
```

Mapping to the 7-arg call:

- `properties` ← `{vector, fts, sparse}` from the first tuple element of each present source.
- `query_text` ← `fts[1]` (or the `query_text=` override kwarg; else `''`).
- `query_vector` ← `vector[1]` if a vector is supplied; else `null` (auto-embed). Allow bare
  `vector="embedding"` to mean auto-embed.
- `sparse_query` (option) ← `sparse[1]`, coerced via the existing `_coerce_sparse_query()`.
- **Pro:** groups *where* + *what* per source; matches the ticket; reads well; precomputed-dense is
  the common path.
- **Con (accepted):** because the engine shares `query_text`, dense *auto-embed* is forced to use the
  FTS string. Acceptable edge-case limitation; documented.

### Option B — flat / engine-faithful

```python
.hybrid_search(
    query_text="query text",
    vector=("embedding", query_vec),   # or "embedding" to auto-embed from query_text
    fts="content",
    sparse=("emb", sparse_query),
    method="rrf", k=10,
)
```

- **Pro:** 1:1 with the engine positionals; no shared-text surprise.
- **Con:** less grouping; diverges from the ticket's proposed surface.

**Recommendation: Option A**, with an optional `query_text=` override kwarg to (a) supply text when
`fts` is omitted but dense auto-embed is wanted, and (b) escape the shared-text constraint when
needed. This keeps the ticket's ergonomics while covering the auto-embed path — and maps exactly onto
LanceDB's "shared-text default + per-source override" builder (§9), the least-surprising shape for
Lance-adjacent users.

### 3.1 Full signature (Option A)

```python
def hybrid_search(
    self: SelfT,
    *,
    vector: tuple[PropertyProxy[Any] | str, list[float] | None] | PropertyProxy[Any] | str | None = None,
    fts: tuple[PropertyProxy[Any] | str, str] | PropertyProxy[Any] | str | None = None,
    sparse: tuple[PropertyProxy[Any] | str, Any] | None = None,
    query_text: str | None = None,
    k: int = 10,
    method: Literal["rrf", "weighted"] = "rrf",
    weights: list[float] | None = None,   # 3-way [vector, fts, sparse]
    alpha: float | None = None,           # 2-way vector/fts blend
    rrf_k: int | None = None,
    over_fetch: float | None = None,
    filter: str | None = None,
) -> SelfT: ...
```

Validation: require ≥1 source; if `method="weighted"` with all three sources, require `weights`
(else equal thirds); `weights` (when given) must be length-3 and is `[vector, fts, sparse]`.

Two behaviours the signature implies, made explicit:

- **`filter`** is wired for real into the engine's arg-5 positional (§4). Note the existing
  `vector_search`/`sparse_search` carry a `pre_filter` field that is **dead code** — never emitted into
  their Cypher. `hybrid_search` must not mirror that no-op; it actually applies `filter` (and appends
  model-level `_filters` as a trailing `WHERE` over `node`).
- **Scores are always surfaced** on the returned instances via a `.search_scores` sidecar (§8) — no
  opt-in flag, no change to the `list[Model]` return type. `.all()`/`.first()`/`.one()` keep returning
  model instances; each just additionally carries its fused + per-arm scores.

## 4. Cypher emission

`_build_hybrid_search_cypher()` returns `(cypher, params)` exactly like the existing helpers. All
user data (dense vector, sparse map, query text, filter) is **bound as `$params`** — never inlined
(mirror `sparse_search`, which binds `$sparse_q` as a nested `{indices, values}` map). Only
schema-derived identifiers (label, property names) and integers (k, rrf_k) are f-string
interpolated, consistent with the existing builders.

```
CALL uni.search(
    'Doc',
    {vector: 'embedding', fts: 'content', sparse: 'emb'},
    $qtext, $qvec, 10, null,
    {method: 'rrf', sparse_query: $sparse_q}
)
YIELD node, score, vector_score, fts_score, sparse_score
RETURN node AS n, score, vector_score, fts_score, sparse_score
ORDER BY score DESC
[LIMIT n]
```

- Omit absent source keys from the `properties` map.
- `query_vector` positional: emit `$qvec` when precomputed, literal `null` when auto-embedding.
- `filter` positional: bind/emit the caller's `filter` predicate here; literal `null` when unset.
- **`RETURN node AS n` plus all score columns is mandatory.** Return `score, vector_score, fts_score,
  sparse_score` (and `rerank_score`/`distance` when applicable). Hybrid hydration uses a **new**
  `_rows_to_scored_instances` (§5) that builds the model from `n` via the same session row-to-model
  path the other builders use, **and** reads the score columns off the row to populate the `.search_scores`
  sidecar. The existing `_rows_to_instances` reads only node props (`_props`/`_vid`/`_labels`) and drops
  `score`/`distance` entirely, so it cannot be reused for hybrid.
- Append model-level `_filters` as a trailing `WHERE` over `node` via `FilterExpr.to_cypher`. (This is
  the *model* filter chain — distinct from the engine `filter` positional above; both may be present.)

## 5. Implementation plan (mechanical; mirror `sparse_search`, plus one score-sidecar addition)

Line numbers verified against `query.py` (789 lines) and `base.py` on 2026-07-01.

### 5.1 The builder + Cypher (in `query.py`)

1. **Config dataclass** `HybridSearchConfig` next to `VectorSearchConfig` (250-258) /
   `SparseSearchConfig` (261-270), just before `_coerce_sparse_query` (273-290): resolved property
   names (`vector`/`fts`/`sparse`, each optional), optional dense `query_vector: list[float] | None`,
   optional sparse `(indices, values)` tuple, `query_text: str`, fusion `method` + `alpha`/`weights`/
   `rrf_k`/`over_fetch`, `k`, `filter`.
2. **Builder state** — add `_hybrid_search: HybridSearchConfig | None` in **three** places:
   class annotations (312-313, beside `_vector_search`/`_sparse_search`), `_init_state` (329-330,
   `self._hybrid_search = None`), and `_clone` (347-348, `new._hybrid_search = copy.copy(self._hybrid_search)`).
3. **Builder method** `hybrid_search()` after `sparse_search` (ends line 478): resolve each present
   source's `PropertyProxy` → property name; run **`_coerce_sparse_query()` (273-290)** on the sparse
   half — note it returns a **`(indices, values)` tuple**, not a dict (the `{indices, values}` dict is
   built later in the Cypher helper, mirroring `_build_sparse_search_cypher:618`); apply the §3.1
   validation; populate the config; `return self._clone()`.
4. **Dispatch** — add a hybrid branch to `_build_cypher()` (517-522), alongside the existing
   `if self._vector_search` / `if self._sparse_search` checks: `if self._hybrid_search: return
   self._build_hybrid_search_cypher()`. Only one search mode is ever set at a time.
5. **Cypher helper** `_build_hybrid_search_cypher() -> tuple[str, dict]` after
   `_build_sparse_search_cypher` (609-647), per §4. Bind all user data as `$params`
   (`$qvec`, `$sparse_q` as a nested `{indices, values}` map, `$qtext`, and the filter predicate);
   f-string only schema identifiers + ints. **Wire `filter` into the arg-5 positional for real** —
   do *not* replicate the dead `pre_filter` (which the vector/sparse helpers accept but never emit).

### 5.2 The score sidecar (§8) — the one non-`query.py` change

6. **`PrivateAttr` on `UniNode`** (`base.py`, beside the existing private attrs at 202-206:
   `_vid`/`_uid`/`_session`/`_dirty`/`_is_new`): add `_scores: SearchScores | None = PrivateAttr(default=None)`.
   Because `UniNode` is Pydantic v2 with `model_config = ConfigDict(extra="forbid",
   validate_assignment=True, ...)` (186-195), a loose attribute can't be attached — a declared
   `PrivateAttr` is the clean, collision-proof channel (this is the ES-`hit.meta.score` pattern done
   Pydantic-correctly; see §8). Expose a read-only `search_scores` property returning `_scores`.
   `SearchScores` is a small frozen dataclass: `score` (fused) + optional `vector`/`fts`/`sparse`/
   `rerank`/`distance`.
7. **`_rows_to_scored_instances`** in `query.py` (beside `_rows_to_instances`, 681-691): build the
   instance via the same session path `_rows_to_instances` uses, then read `score`/`vector_score`/
   `fts_score`/`sparse_score`/`rerank_score`/`distance` off the *same row* and set the instance's
   `_scores` (via the model's private-attr channel). The hybrid execution path (`.all()`/`.first()`/
   `.one()`) routes through this method instead of `_rows_to_instances` when `_hybrid_search` is set.

Async parity: the builder logic lives entirely in `_QueryBuilderBase`, so `AsyncQueryBuilder`
inherits `hybrid_search()` for free (same as the existing search methods). Execution methods
(`all`/`first`/`one` — note there is **no `.fetch()`**) live on `QueryBuilder`/`AsyncQueryBuilder`
and need the one-line reroute to `_rows_to_scored_instances`.

## 6. Testing

Mirror `TestQueryBuilderCypherGeneration` in `tests/test_queries.py` — pure string-generation unit
tests against a `MagicMock()` session (no DB), substring-asserting the Cypher and exact-asserting the
params dict. Cases:

- 3-way precomputed (`vector`+`fts`+`sparse`, `method="rrf"`) → asserts `uni.search`, the
  `{vector:..., fts:..., sparse:...}` map, `YIELD node, score, ...`, `RETURN node AS n`, and
  `params["sparse_q"] == {"indices": [...], "values": [...]}`, `params["qvec"] == [...]`.
- 2-way `vector`+`fts`, `method="weighted"`, `alpha=` → asserts `method: 'weighted'`, `alpha`.
- 3-way `weights=[...]` → asserts the weights array.
- dense auto-embed: bare `vector="embedding"` + `query_text=` → asserts `null` query-vector
  positional and the bound `$qtext`.
- sparse-arm guard: `sparse=` present ⇒ both the map key **and** `options.sparse_query` emitted.
- validation errors: no sources; `weights` length ≠ 3.
- **RETURN carries the score columns** — assert `score, vector_score, fts_score, sparse_score` are in
  the `RETURN`, so the sidecar has data to read.

Score-sidecar tests (need a session that returns rows, so a `MagicMock` returning canned rows with
`score`/`vector_score`/… keys — still no real DB):

- `.all()` returns model instances **and** each instance's `.search_scores.score` equals the row's
  fused score; `.search_scores.vector`/`.fts`/`.sparse` map to the per-arm columns.
- a model that itself declares a field named `score` still gets its search score via `.search_scores`
  (no collision) — the regression the `PrivateAttr` channel exists to prevent (§8).
- rows missing an arm's column (e.g. sparse off) ⇒ that sub-score is `None`, not an error.

(Optional, separate) a uni-db binding e2e in `bindings/uni-db/tests/` against a real session, mirroring
`test_e2e_vector.py` — recommended but not required for the unit-level acceptance of #114.

## 7. Scope / deferred

- **In scope:** `method` (rrf/weighted), `alpha`, `weights`, `rrf_k`, `over_fetch`, `filter`,
  `sparse_query`; all three sources; precomputed dense + dense auto-embed; the `.search_scores` sidecar.
- **Deferred (follow-ups), with committed shapes so they land non-breaking:**
  - **Reranker** (`reranker*`, `maxsim_*`): expose as a **chained `.rerank(reranker=…)` method** on the
    builder, *not* extra `hybrid_search()` kwargs. The survey (§9) shows the fluent systems closest to
    uni — **LanceDB `.rerank(RRFReranker())`**, Weaviate, Typesense — all fold rerank into the same
    chained builder, and uni's engine already accepts the reranker inside `uni.search` options (§2.3),
    so `.rerank()` just sets those options. The `rerank_score` column already flows into `.search_scores`.
  - **`method:"dbsf"`** (distribution-based fusion): additive third `method` value once the engine
    supports it (§2.3 note).
  - **ANN-tuning knobs** (`nprobes`/`refine_factor`/`ef_search`): not plumbed through `uni.search` at
    all — blocked on the engine, not the binding.

## 8. Resolved — score surfacing via a `.search_scores` sidecar

`vector_search`/`sparse_search` today **`RETURN` a `score` column but `.all()`/`.first()`/`.one()` only
hydrate the model instance and drop the scores** (`_rows_to_instances`, 681-691, reads only node props).
#114 asks to "surface the `score`/`vector_score`/`fts_score`/`sparse_score` columns," which no
single-source builder does yet. The original draft recommended just matching that drop-and-defer
behaviour; **the ecosystem survey (§9) reverses that call — dropping scores on hydration is the one
default multiple systems demonstrably regret**, and there is a small, collision-proof pattern that does
it right:

- **LanceDB drops scores on `.to_pydantic()` by default** — `LanceModel` inherits Pydantic v2
  `extra="ignore"`, so `_relevance_score`/`_distance` silently vanish; the alias route to keep them
  (`Field(alias="_relevance_score")`) is **broken — LanceDB issue #2436**. This is *uni's exact problem
  in uni's exact stack*. uni's `extra="forbid"` (stricter than LanceDB's `ignore`) makes the pseudo-field
  route impossible outright, not just buggy.
- **Beanie / MongoEngine** likewise drop the score (un-projected metadata; needs a separate projection model).
- **elasticsearch-dsl got it right**: score lives in a **separate metadata namespace** (`hit.meta.score`),
  so it always survives hydration, never collides with a user field named `score`, and typing + scoring
  coexist.

**Decision: adopt the elasticsearch-dsl sidecar pattern, realized as a Pydantic `PrivateAttr` (§5.2).**
Every hybrid result is a normal model instance that additionally carries `.search_scores` (a small frozen
`SearchScores` with `.score` + optional `.vector`/`.fts`/`.sparse`/`.rerank`/`.distance`). Rationale:

- **Small diff, no new return type** — `.all()` still returns `list[Model]`; ordering already comes from
  the Cypher `ORDER BY score DESC`. No `(instance, scores)` wrapper, no `.with_scores()` variant to thread
  through every execution method.
- **Collision-proof** — a user model with its own `score: float` field is untouched; the search score is
  reached only via `.search_scores`. This is impossible with a naive extra-field approach under
  `extra="forbid"`, and is the LanceDB-#2436 / silent-`extra="ignore"` trap avoided by construction.
- **Sets the uniform precedent #114 wanted** — because `vector_search`/`sparse_search` have the identical
  gap, the same `_rows_to_scored_instances` + `_scores` sidecar retrofits all three builders. Rather than
  deferring a cross-cutting "scored surface" ticket, this *establishes* the pattern that ticket would
  generalize. Follow-up: switch `vector_search`/`sparse_search` onto the same sidecar (trivial once it
  exists) so scores are consistent across every search builder.

Alternatives considered and rejected: a `(instance, scores)` tuple return (diverges the return type from
every other builder, forces a parallel execution method) and mapping scores onto model fields (impossible
under `extra="forbid"`; the buggy LanceDB path).

## 9. Prior art — ecosystem survey (2026-07-01)

A survey of 20+ ORMs / OGMs / vector clients across four clusters (native vector DBs; SQL ORMs; graph
DBs + OGMs; LanceDB + document ODMs + retriever frameworks). It grounds the three decisions above.

### 9.1 Where uni sits

| System | In-DB fusion | Modalities | Typed model-bound surface |
|---|---|---|---|
| Neo4j + neomodel | ✗ (2-way, only in the `graphrag` py pkg; not RRF) | 2 | ✓ vector `(node, score)`; no hybrid |
| SurrealDB | ✓ `search::rrf()`, N-list | N (manual arms) | ✗ raw SurrealQL only |
| ArangoDB / TigerGraph | ✗ (RRF only in LangChain/GraphRAG app layer) | manual | ✗ |
| Memgraph | ✗ hand-written | 2 | ✗ raw `CALL` |
| SQL ORMs (SQLAlchemy/pgvector, Django, Prisma, Drizzle, Ent) | ✗ (fusion = raw SQL/CTEs, always) | 1 | vector-only; several are raw-SQL escape hatches |
| **uni-db (this proposal)** | **✓ engine `uni.search` RRF/weighted** | **3 (+ multivector rerank)** | **✓ typed `hybrid_search()`** |

Takeaway: **no graph DB/OGM exposes fused 3-way hybrid as a typed builder call; no SQL ORM offers
fusion at all.** SurrealDB has in-DB RRF but no OGM/sparse; neomodel has the typed OGM but no fusion.
uni is the only system combining (a) in-engine RRF/weighted, (b) 3+ modalities, and (c) an OGM that
surfaces it as one typed call — the motivation to lead #114 with.

### 9.2 Decision → evidence map

- **API shape (§3) → tuple-per-source + shared-text default.** Ecosystem splits: strictly per-source
  (Qdrant `Prefetch`, Milvus `AnnSearchRequest`, MongoDB `$rankFusion` sub-pipelines, ES retriever
  tree) vs flat shared query (Weaviate `hybrid(query=…, alpha=…)`, Typesense, Meilisearch). **LanceDB —
  uni's Lance sibling — does both** (shared default + `.vector()`/`.text()` overrides). Option A is that
  pattern; the shared-`query_text` constraint is inherent (Weaviate ships the same one).
- **Fusion knobs (§2.3) → validated.** `rrf_k=60` is the universal RRF constant (Milvus, MongoDB,
  LanceDB, ES, LlamaIndex). `alpha` (2-way convex blend) matches Weaviate/Pinecone/Typesense — though
  their defaults lean vector-heavy (0.75 / 0.7) vs uni's neutral 0.5. `weights=[v,f,s]` positional
  matches Milvus `WeightedRanker` / MongoDB `combination.weights`. `dbsf` (Qdrant/Haystack/LlamaIndex)
  is the natural future mode.
- **Score surfacing (§8) → sidecar namespace, not drop.** LanceDB/Beanie/MongoEngine all drop scores on
  model hydration by default (LanceDB via `extra="ignore"`, and its alias fix is broken — #2436).
  elasticsearch-dsl is the one that got it right: a separate metadata namespace (`hit.meta.score`).
  uni's `PrivateAttr` `.search_scores` is that pattern, done Pydantic-correctly under `extra="forbid"`.
- **Reranker (§7) → chained `.rerank()`.** Fluent systems (LanceDB `.rerank(RRFReranker())`, Weaviate
  `rerank=`, Qdrant nested `query=`, Typesense) fold rerank into the same builder; pipeline systems
  (MongoDB `$rerank`, ES `text_similarity_reranker`, LlamaIndex/Haystack post-processors) make it a
  separate stage. uni's engine already accepts the reranker inside `uni.search` options, so the fluent
  `.rerank()` is the natural fit.

### 9.3 Cross-cutting patterns worth knowing

- **Fusion location:** most fuse server-side; Pinecone is the outlier (client-side alpha — you scale
  the vectors before the call). uni fuses in-engine — the good side.
- **Sub-score visibility:** only Weaviate exposes a keyword-vs-vector breakdown, and only as an
  `explain_score` *string*. Qdrant/Milvus/Pinecone/MongoDB return the fused score only. uni's
  `.search_scores` surfacing *structured* per-arm scores is ahead of most of the field.
- **Version-sensitive naming (implementation caution):** Qdrant `RrfQuery(Rrf(k=, weights=))` vs plain
  `FusionQuery`; Milvus `ranker=` (MilvusClient) vs `rerank=` (Collection). Not uni's concern, but a
  reminder that fusion-API surfaces churn — keep uni's kwargs stable once shipped.

## 10. Implementation notes (2026-07-01)

Landed in `uni-pydantic` (no Rust changes). Where the shipped code deviates from or extends the design:

- **`SearchScores`** is a frozen dataclass in `base.py`; `UniNode` gained a `_scores` `PrivateAttr` and a
  read-only `search_scores` property. Set on instances via plain `instance._scores = …` (the sanctioned
  path `from_properties` uses for `_vid`); unaffected by `extra="forbid"`. Exported from the package.
- **Scope widened to all three builders** (per an explicit decision): `_rows_to_scored_instances` +
  `_is_search()` on `_QueryBuilderBase`, and the sync/async `all()` reroute, cover `vector_search` and
  `sparse_search` too — so `.search_scores` is uniform across every search builder, not hybrid-only.
- **Latent bug fixed:** `_build_vector_search_cypher`/`_build_sparse_search_cypher` emitted
  `RETURN node AS n`, which `_row_to_node_dict` cannot hydrate (it needs `_props`/`_vid`/`_labels`), so
  their `.all()` returned `[]`. Both now emit the triple + score columns. There were no execution tests
  for these paths; there are now (`TestSearchScoreSurfacing`), plus string-level guards asserting
  `node AS n` is gone.
- **Single-source `SearchScores`:** `.score` = the row's `score` column; the vector arm additionally
  carries `.distance`; other per-arm fields stay `None`.
- **Validation** (`hybrid_search`): raises `QueryError` on no sources and on `weights` length ≠ 3;
  missing `weights` under `method="weighted"` falls back to the engine's equal-thirds default (not an error).
- **Tests/lint:** `test_queries.py` full suite green (59 in-file / 240 package), `ruff` clean, and `mypy`
  adds **no** new errors (the 2 it reports are a pre-existing metaclass quirk on `cls.__label__`/
  `cls.__edge_type__`, present before this change).
