# Design Proposal: OGM `hybrid_search()` builder (dense + text + sparse)

- **Issue:** rustic-ai/uni-db#114 (`enhancement`, `python`); relates to #95 (M4 sparse), #122 (auto-embed parity).
- **Status:** **Proposed** — design only, not implemented. Engine support is complete and tested;
  this is a pure `uni-pydantic` binding gap.
- **Author:** (design)
- **Date:** 2026-06-30
- **Scope:** `bindings/uni-pydantic/src/uni_pydantic/query.py` (builder + Cypher emission),
  `bindings/uni-pydantic/tests/test_queries.py` (string-generation unit tests). **No Rust changes.**

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
+ sparse); they differ only in where the per-source query lives. **This is the deliberate decision
#114 flags as the reason for deferral — it must be made explicitly before coding.**

### Option A — tuple-per-source (the issue's proposed shape) — *recommended*

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
    .fetch()
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
needed. This keeps the ticket's ergonomics while covering the auto-embed path.

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
- `filter` positional: literal `null` when unset.
- **`RETURN node AS n`** is mandatory — `_rows_to_instances` reads the `n` column to hydrate model
  instances (same contract as `vector_search`/`sparse_search`).
- Append model `_filters` as `WHERE` over `node` via `FilterExpr.to_cypher`, exactly as the existing
  helpers do.

## 5. Implementation plan (all mechanical; mirror `sparse_search`)

In `bindings/uni-pydantic/src/uni_pydantic/query.py`:

1. **Config dataclass** `HybridSearchConfig` next to `VectorSearchConfig`/`SparseSearchConfig`
   (~line 250-270): resolved property names, optional dense vector, optional sparse `(indices,
   values)`, query text, fusion method + params, k, filter.
2. **Builder state** — add `_hybrid_search: HybridSearchConfig | None` in **three** places:
   class annotations (~313), `_init_state` (~329), and `_clone` (~347, `copy.copy(...)`).
3. **Builder method** `hybrid_search()` after `sparse_search` (~line 478): resolve `PropertyProxy`
   names; `_coerce_sparse_query()` for the sparse half; populate the config; return `self._clone()`.
4. **Dispatch** — add a branch to `_build_cypher()` (~line 519): `if self._hybrid_search: return
   self._build_hybrid_search_cypher()`. Place it consistently (e.g. before/after the existing
   vector/sparse branches; only one search mode is set at a time).
5. **Cypher helper** `_build_hybrid_search_cypher()` after `_build_sparse_search_cypher()`
   (~line 647), per §4.

Async parity: the builder logic lives entirely in `_QueryBuilderBase`, so `AsyncQueryBuilder`
inherits `hybrid_search()` for free (same as the existing search methods).

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

(Optional, separate) a uni-db binding e2e in `bindings/uni-db/tests/` against a real session, mirroring
`test_e2e_vector.py` — recommended but not required for the unit-level acceptance of #114.

## 7. Scope / deferred

- **In scope:** `method` (rrf/weighted), `alpha`, `weights`, `rrf_k`, `over_fetch`, `filter`,
  `sparse_query`; all three sources; precomputed dense + dense auto-embed.
- **Deferred (follow-ups):** reranker knobs (`reranker*`, `maxsim_*`) — additive once the base
  builder lands; ANN-tuning knobs (not plumbed through `uni.search` at all).

## 8. Open question — score surfacing (decide before coding)

`vector_search`/`sparse_search` today **`RETURN` a `score` column but `all()`/`fetch()` only hydrate
the model instance and drop the scores** (`_rows_to_instances` reads the `n` column only). The issue
asks to "surface the `score`/`vector_score`/`fts_score`/`sparse_score` columns," which no single-source
builder does yet. Two paths:

- **(a) Match current behavior** — emit the score columns in the `RETURN` but return plain model
  instances (scores dropped). Smallest diff; consistent with the existing search builders; defers the
  scored-result surface to a separate cross-cutting issue.
- **(b) Add a scored-result surface** — e.g. `.with_scores()` / a `(instance, scores)` wrapper. Larger,
  cross-cutting (ideally retrofits `vector_search`/`sparse_search` too), and arguably its own ticket.

**Recommendation: (a) for #114**, and file a separate issue for (b) covering all three search builders
uniformly — so score-surfacing is a deliberate, consistent decision rather than a one-off bolted onto
hybrid only.
