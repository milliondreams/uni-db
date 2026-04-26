# Cross-Encoder Reranking for Search Procedures v0.1

## Post-Retrieval Reranking via Cross-Encoder Models

**Status:** Draft
**Version:** 0.1.0
**Date:** 2026-04-23

---

## 1. Overview

This proposal adds **cross-encoder reranking** as an optional post-retrieval stage in the three search procedures: `uni.search`, `uni.vector.query`, and `uni.fts.query`. Cross-encoders jointly attend to a (query, document) pair and produce a relevance score that is more accurate than bi-encoder similarity or BM25, but too expensive to run on the full corpus. By running them on a small over-fetched candidate set, we get the best of both worlds: fast retrieval with high-precision final ranking.

### 1.1 Design Principles

1. **Opt-in** — reranking is off by default; enabled via an options key.
2. **Model-agnostic** — any provider that implements the `Reranker` trait in Uni-Xervo can be used (local ONNX, Cohere, Voyage, etc.).
3. **No new procedures** — reranking is a configuration of existing procedures, not a separate `CALL`.
4. **Transparent scoring** — the reranker score is exposed as a dedicated YIELD column alongside existing scores.
5. **Composable** — the reranked `score` column is what `ORDER BY` and downstream Cypher see; original retrieval scores remain available for debugging.

### 1.2 Scope

**In scope:**
- `uni.search` (hybrid), `uni.vector.query`, `uni.fts.query`
- Cross-encoder model integration via Uni-Xervo `Reranker` trait
- Over-fetch control for reranking candidates
- Document text hydration from a configurable source property
- YIELD column for reranker score

**Out of scope:**
- `similar_to()` expression (rowwise scalar — no bounded candidate set)
- Write-time reranking or index-time cross-encoder distillation
- Multi-field document construction (concatenation of multiple properties)
- Reranker model training or fine-tuning infrastructure

---

## 2. User-Facing API

### 2.1 `uni.search` (Hybrid)

```cypher
CALL uni.search(
    'Document',
    {vector: 'embedding', fts: 'content'},
    'quantum computing applications',
    null,           -- query_vector (auto-embed)
    10,             -- k
    null,           -- filter
    {
        method: 'rrf',
        reranker: 'rerank/cohere-v3',
        reranker_property: 'content',
        reranker_k: 30
    }
) YIELD node, score, rerank_score, vector_score, fts_score
```

### 2.2 `uni.vector.query`

```cypher
CALL uni.vector.query(
    'Document',
    'embedding',
    'quantum computing applications',
    10,             -- k
    null,           -- filter
    null,           -- threshold
    {
        reranker: 'rerank/cohere-v3',
        reranker_property: 'content',
        reranker_k: 30
    }
) YIELD node, score, rerank_score, distance
```

### 2.3 `uni.fts.query`

```cypher
CALL uni.fts.query(
    'Article',
    'body',
    'machine learning transformers',
    10,             -- k
    null,           -- filter
    null,           -- threshold
    {
        reranker: 'rerank/cohere-v3',
        reranker_property: 'body',
        reranker_k: 30
    }
) YIELD node, score, rerank_score
```

### 2.4 Options Keys

New keys added to the options map (position 6 for `uni.search`, new position 6 for `uni.vector.query` / `uni.fts.query`):

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `reranker` | String | `null` (disabled) | Uni-Xervo model alias for the cross-encoder (e.g., `"rerank/cohere-v3"`). When present, enables the rerank stage. |
| `reranker_property` | String | Same as FTS property, or first `source_properties` from embedding config | The node property whose text value is fed as the "document" side of the cross-encoder. |
| `reranker_k` | Integer | `k * 3` | Number of candidates to over-fetch for reranking. The retrieval stage returns `reranker_k` results; the reranker re-scores them; the top `k` are returned. |

**Validation rules:**
- `reranker_k` must be ≥ `k`. If `reranker_k < k`, emit a warning and clamp to `k`.
- `reranker_k` must be ≤ 1000. Cross-encoders are O(n) in inference cost; unbounded over-fetch is a foot-gun.
- `reranker` alias must exist in the Uni-Xervo catalog with `ModelTask::Rerank`. Error at parse time if missing.
- `reranker_property` must be a string-typed property on the target label. Error if the property does not exist or is not a string type in the schema.

### 2.5 YIELD Columns

New column added to all three procedures when reranking is enabled:

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `rerank_score` | Float32 | Yes | Raw cross-encoder relevance score. `null` when reranking is not enabled. |

When reranking is enabled, the `score` column reflects the **reranker score** (not the fusion/retrieval score). The original retrieval score is still available:
- `uni.search`: via `vector_score`, `fts_score` (individual), or the fused score is replaced
- `uni.vector.query`: via `distance` (raw) — `score` becomes reranker score
- `uni.fts.query`: `score` becomes reranker score

Rationale: `score` should always represent the "best available relevance signal." Users who `ORDER BY score DESC` get the right behavior whether or not reranking is enabled. The dedicated `rerank_score` column is available for callers who want to inspect or combine scores manually.

### 2.6 Score Semantics When Reranking

| Column | Without reranker | With reranker |
|--------|-----------------|---------------|
| `score` | Fusion score (hybrid) or normalized distance (vector/FTS) | Reranker score (normalized to [0, 1]) |
| `rerank_score` | `null` | Raw cross-encoder score (normalized to [0, 1]) |
| `vector_score` | Normalized vector similarity | Unchanged |
| `fts_score` | Normalized BM25 | Unchanged |
| `distance` | Raw vector distance | Unchanged |

---

## 3. Uni-Xervo Changes

### 3.1 `Reranker` Trait

New trait alongside existing `Embedder` and `Generator`:

```rust
/// A cross-encoder reranker that scores (query, document) pairs.
#[async_trait]
pub trait Reranker: Send + Sync {
    /// Score a query against a batch of documents.
    ///
    /// Returns one relevance score per document, in the same order as `documents`.
    /// Scores are raw logits; the caller is responsible for normalization.
    async fn rank(
        &self,
        query: &str,
        documents: &[&str],
    ) -> Result<Vec<f32>>;
}
```

### 3.2 `ModelTask::Rerank`

New variant in the `ModelTask` enum:

```rust
pub enum ModelTask {
    Embed,
    Generate,
    Onnx,
    Rerank,   // ← new
}
```

### 3.3 `ModelRuntime::reranker()`

New method on `ModelRuntime`:

```rust
impl ModelRuntime {
    pub async fn reranker(&self, alias: &str) -> Result<Arc<dyn Reranker>>;
}
```

Mirrors the pattern of `embedding()` and `generator()`.

### 3.4 `UniXervo::rerank()` Facade

```rust
impl UniXervo {
    /// Rerank documents against a query using a configured cross-encoder model.
    pub async fn rerank(
        &self,
        alias: &str,
        query: &str,
        documents: &[&str],
    ) -> Result<Vec<f32>> {
        let runtime = self.runtime.as_ref().ok_or_else(not_configured)?;
        let reranker = runtime.reranker(alias).await.map_err(into_uni_error)?;
        reranker.rank(query, documents).await.map_err(into_uni_error)
    }
}
```

### 3.5 Provider Implementations

Initial providers (each behind a feature flag):

| Provider | Feature Flag | Model Examples |
|----------|-------------|----------------|
| Cohere | `provider-cohere` | `rerank-v3.5`, `rerank-english-v3.0`, `rerank-multilingual-v3.0` |
| Voyage AI | `provider-voyageai` | `rerank-2`, `rerank-lite-1` |
| ONNX (local) | `provider-onnx` | `cross-encoder/ms-marco-MiniLM-L-6-v2`, `BAAI/bge-reranker-v2-m3` |
| Jina AI | `provider-jina` | `jina-reranker-v2-base-multilingual` |

Each provider implements the `Reranker` trait. The ONNX provider uses the existing `RawTensorModel` infrastructure for local inference.

### 3.6 Catalog Configuration

```yaml
# xervo_catalog.yaml
- alias: "rerank/cohere-v3"
  task: rerank
  provider: cohere
  model: "rerank-v3.5"

- alias: "rerank/local"
  task: rerank
  provider: onnx
  model: "cross-encoder/ms-marco-MiniLM-L-6-v2"
```

---

## 4. Execution Flow

### 4.1 `execute_hybrid_search` (Modified)

Current flow:
```
1. Parse args
2. vector_search(over_fetch_k) → Vec<(Vid, f32)>
3. fts_search(over_fetch_k) → Vec<(Vid, f32)>
4. Fuse (RRF or weighted) → Vec<(Vid, f32)>
5. Take top k
6. Build output batch
```

New flow with reranker:
```
1. Parse args (including reranker options)
2. vector_search(reranker_k or over_fetch_k) → Vec<(Vid, f32)>
3. fts_search(reranker_k or over_fetch_k) → Vec<(Vid, f32)>
4. Fuse (RRF or weighted) → Vec<(Vid, f32)>
5. Take top reranker_k (not k)
6. ── NEW: Rerank stage ──
   a. Hydrate document text for reranker_k candidates
   b. Call reranker.rank(query_text, &doc_texts)
   c. Normalize reranker scores to [0, 1]
   d. Re-sort by reranker score descending
   e. Take top k
7. Build output batch (with rerank_score column)
```

When `reranker` is absent, the flow is unchanged (step 5 takes `k` directly, step 6 is skipped).

### 4.2 `execute_vector_query` (Modified)

Current flow:
```
1. Parse args
2. vector_search(k) → Vec<(Vid, f32)>
3. Apply threshold filter
4. Build output batch
```

New flow with reranker:
```
1. Parse args (including reranker options from new arg position 6)
2. vector_search(reranker_k) → Vec<(Vid, f32)>
3. Apply threshold filter
4. ── NEW: Rerank stage ──
   a. Hydrate document text for candidates
   b. Call reranker.rank(query_text, &doc_texts)
   c. Normalize, re-sort, take top k
5. Build output batch (with rerank_score column)
```

Note: `uni.vector.query` currently accepts a vector or text as the query (arg 2). When the query is text, it's auto-embedded for vector search. The same text is reused as the cross-encoder query. When the query is a pre-computed vector, the caller must provide `reranker_query` in options (see §4.5).

### 4.3 `execute_fts_query` (Modified)

Analogous to vector query. The search term (arg 2) is reused as the cross-encoder query text.

### 4.4 Document Text Hydration

The reranker needs the text content of each candidate document. This is fetched via the existing `PropertyManager::get_batch_vertex_props_for_label()`:

```rust
// In the rerank stage:
let vids: Vec<Vid> = candidates.iter().map(|(vid, _)| *vid).collect();

// Fetch properties for the candidate set
let props_map = graph_ctx
    .storage()
    .property_manager()
    .get_batch_vertex_props_for_label(&vids, label, Some(&query_ctx))
    .await?;

// Extract the reranker_property text from each
let doc_texts: Vec<String> = vids.iter().map(|vid| {
    props_map
        .get(vid)
        .and_then(|props| props.get(&reranker_property))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}).collect();
```

This reuses the same batch property loading path that `build_hybrid_search_batch` already uses for node hydration. The property fetch is deferred to the rerank stage so we only hydrate `reranker_k` candidates (not the full index scan).

### 4.5 Pre-Computed Vector Queries

When `uni.vector.query` receives a pre-computed vector (not text), there is no query string for the cross-encoder. In this case:

- The caller must provide `reranker_query` in the options map:
  ```cypher
  CALL uni.vector.query('Doc', 'embedding', [0.1, 0.2, ...], 10, null, null,
      {reranker: 'rerank/cohere', reranker_property: 'content',
       reranker_query: 'quantum computing'})
  ```
- If `reranker` is set but `reranker_query` is absent and the query is a vector, return an error:
  `"Cannot rerank: query is a pre-computed vector. Provide reranker_query in options."`

### 4.6 Score Normalization

Cross-encoder models output raw logits that vary in scale across providers. To normalize to [0, 1] for the `score` and `rerank_score` columns:

```rust
fn normalize_reranker_scores(scores: &[f32]) -> Vec<f32> {
    if scores.is_empty() {
        return vec![];
    }
    // Sigmoid normalization — maps arbitrary logits to [0, 1]
    // This is provider-agnostic and preserves ranking order.
    scores.iter().map(|&s| 1.0 / (1.0 + (-s).exp())).collect()
}
```

Sigmoid is preferred over min-max because:
1. It's stable with a single result (min-max degenerates).
2. It preserves relative spacing of scores.
3. Most cross-encoders are trained with sigmoid/logistic loss, so their logits are already calibrated for sigmoid.

Providers that return pre-normalized scores (e.g., Cohere returns [0, 1]) should be detected via a provider trait flag, and normalization skipped.

---

## 5. Options Parsing Changes

### 5.1 `uni.search` Options (Arg 6)

Existing keys are unchanged. New keys are added:

```rust
// procedure_call.rs — in execute_hybrid_search, after parsing existing options:

let reranker_alias = options_map
    .get("reranker")
    .and_then(|v| v.as_str())
    .map(String::from);

let reranker_property = options_map
    .get("reranker_property")
    .and_then(|v| v.as_str())
    .map(String::from)
    .or_else(|| Some(fts_prop.clone())); // default: FTS property

let reranker_k = options_map
    .get("reranker_k")
    .and_then(|v| v.as_u64())
    .map(|v| v as usize)
    .unwrap_or(k * 3); // default: 3x final k

let reranker_query = options_map
    .get("reranker_query")
    .and_then(|v| v.as_str())
    .map(String::from);
```

### 5.2 `uni.vector.query` and `uni.fts.query` Options (New Arg)

These procedures currently take 6 positional args. A 7th optional argument is added for the options map:

```
uni.vector.query(label, property, query, k, filter?, threshold?, options?)
uni.fts.query(label, property, term, k, filter?, threshold?, options?)
```

The options map for these procedures only recognizes reranker keys:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `reranker` | String | `null` | Model alias |
| `reranker_property` | String | Required when `reranker` is set | Source text property |
| `reranker_k` | Integer | `k * 3` | Over-fetch count |
| `reranker_query` | String | Query arg (arg 2) | Override query text for cross-encoder |

---

## 6. Schema & YIELD Changes

### 6.1 Canonical Yield Mapping

Add `rerank_score` to `map_yield_to_canonical()` in `procedure_call.rs`:

```rust
fn map_yield_to_canonical(name: &str) -> &str {
    match name {
        // ... existing mappings ...
        "rerank_score" | "_rerank_score" => "rerank_score",
        _ => "node",
    }
}
```

### 6.2 Schema Field

In the schema builder (`build_schema`, line 205-262):

```rust
"rerank_score" => {
    fields.push(Field::new(output_name, DataType::Float32, true));
}
```

### 6.3 Batch Building

In `build_hybrid_search_batch()` and `build_search_result_batch()`, when the yield includes `rerank_score`:

```rust
"rerank_score" => {
    let values: Vec<Option<f32>> = results.iter().map(|(vid, _)| {
        rerank_scores.as_ref().and_then(|m| m.get(vid).copied())
    }).collect();
    let array = Float32Array::from(values);
    columns.push(Arc::new(array));
}
```

---

## 7. Over-Fetch Interaction

When reranking is enabled, the over-fetch chain is:

```
                    uni.search                    uni.vector.query / uni.fts.query
                    ──────────                    ─────────────────────────────────
Storage retrieval:  max(over_fetch_k, reranker_k) reranker_k
Fusion output:      reranker_k                    N/A
Reranker input:     reranker_k                    reranker_k
Final output:       k                             k
```

For `uni.search`, the existing `over_fetch` factor controls how many candidates each modality (vector, FTS) returns. The fusion step then produces a merged list. If `reranker_k > over_fetch_k`, the per-modality fetch is increased to `reranker_k` so the fusion stage has enough candidates. The fusion output is then truncated to `reranker_k` before reranking.

```rust
let effective_retrieval_k = if reranker_alias.is_some() {
    reranker_k.max(over_fetch_k)
} else {
    over_fetch_k
};
```

---

## 8. Error Handling

| Condition | Behavior |
|-----------|----------|
| `reranker` alias not in Xervo catalog | Error: `"Reranker model '{alias}' not found in Xervo catalog"` |
| `reranker` alias exists but task ≠ `Rerank` | Error: `"Model '{alias}' is registered as {task}, not Rerank"` |
| `reranker_property` not a string-typed property | Error: `"reranker_property '{prop}' is not a string property on label '{label}'"` |
| `reranker_property` not provided and no FTS property to default from (vector/FTS procedures) | Error: `"reranker_property is required when using reranker with uni.vector.query"` |
| Pre-computed vector query without `reranker_query` | Error: `"Cannot rerank: query is a pre-computed vector. Provide reranker_query in options."` |
| `reranker_k < k` | Warning + clamp: `"reranker_k ({rk}) < k ({k}), clamping to k"` |
| `reranker_k > 1000` | Warning + clamp: `"reranker_k ({rk}) exceeds maximum 1000, clamping"` |
| Reranker API call fails | Error propagated: `"Reranker inference failed: {provider_error}"` |
| Document text is null/missing for a candidate | Score as empty string `""` — cross-encoder handles gracefully |
| Xervo runtime not configured | Error: `"Cannot rerank: Uni-Xervo runtime not configured"` |

---

## 9. Performance Considerations

### 9.1 Latency Budget

Cross-encoder inference is the dominant cost. Typical latencies:

| Provider | 30 documents | 100 documents |
|----------|-------------|---------------|
| Cohere Rerank v3 (API) | ~100ms | ~200ms |
| Voyage Rerank 2 (API) | ~80ms | ~180ms |
| ms-marco-MiniLM-L-6 (local ONNX, CPU) | ~15ms | ~50ms |
| bge-reranker-v2-m3 (local ONNX, CPU) | ~40ms | ~120ms |

The default `reranker_k = k * 3` keeps the candidate set small. With k=10, that's 30 documents — well within the fast path for all providers.

### 9.2 Property Hydration Cost

Document text hydration (`get_batch_vertex_props_for_label`) is a single batch query over `reranker_k` VIDs. This is the same path used for node YIELD — no additional storage round-trips beyond what already happens for result building. When the node column is also YIELDed, the property fetch can be shared (fetched once, used for both reranking and output).

### 9.3 Optimization: Shared Property Fetch

When both `node` and `reranker_property` are requested, we should fetch properties once:

```rust
// Fetch all properties needed for both reranking and output in one call
let props_map = property_manager
    .get_batch_vertex_props_for_label(&vids, label, Some(&query_ctx))
    .await?;

// Extract reranker text from props_map
let doc_texts = extract_text_property(&props_map, &vids, &reranker_property);

// Reuse props_map for building node columns later
```

### 9.4 Concurrency: Parallel Retrieval

For `uni.search`, vector search and FTS search already run sequentially. With reranking, the retrieval + rerank pipeline is:

```
vector_search ──┐
                 ├── fuse ── rerank ── output
fts_search    ──┘
```

Future optimization: run vector and FTS search concurrently with `tokio::join!`. This is independent of the reranking work and should be considered separately.

---

## 10. Implementation Plan

### Phase 1: Uni-Xervo `Reranker` Trait

**Files:**
- `uni-xervo/src/traits.rs` — add `Reranker` trait
- `uni-xervo/src/runtime.rs` — add `ModelRuntime::reranker()`, `ModelTask::Rerank`
- `uni-xervo/src/catalog.rs` — accept `task: rerank` in catalog specs

**Deliverable:** `Reranker` trait defined, `ModelRuntime` can resolve reranker aliases. No provider implementations yet — just the trait surface.

### Phase 2: First Provider (Cohere)

**Files:**
- `uni-xervo/src/providers/cohere.rs` — implement `Reranker` for Cohere API

**Deliverable:** End-to-end reranking works with Cohere's API. Integration test with a real API key.

### Phase 3: Search Procedure Integration

**Files:**
- `crates/uni/src/api/xervo.rs` — add `UniXervo::rerank()` facade
- `crates/uni-query/src/query/df_graph/procedure_call.rs`:
  - Options parsing for reranker keys (§5)
  - Rerank stage in `execute_hybrid_search` (§4.1)
  - Rerank stage in `execute_vector_query` (§4.2)
  - Rerank stage in `execute_fts_query` (§4.3)
  - YIELD column additions (§6)
  - Shared property fetch optimization (§9.3)
  - Score normalization (§4.6)

**Deliverable:** All three procedures support reranking. TCK tests covering each procedure.

### Phase 4: Additional Providers

**Files:**
- `uni-xervo/src/providers/voyageai.rs` — Voyage AI
- `uni-xervo/src/providers/onnx.rs` — local ONNX cross-encoder
- `uni-xervo/src/providers/jina.rs` — Jina AI

**Deliverable:** Multiple provider options. ONNX provider enables fully local reranking.

### Phase 5: Python Bindings

**Files:**
- `bindings/uni-db/src/builders.rs` — expose reranker options in sync API
- `bindings/uni-db/src/async_api.rs` — expose in async API

**Deliverable:** Python users can pass reranker options through the existing `session.query()` Cypher interface. No new Python API needed — it's all in the Cypher options map.

---

## 11. Testing Strategy

### 11.1 Unit Tests

- Options parsing: valid keys, defaults, validation errors
- Score normalization: sigmoid, edge cases (empty, single, all-same)
- Over-fetch interaction: `reranker_k` vs `over_fetch_k` clamping

### 11.2 Integration Tests (Mock Reranker)

A `MockReranker` implementing the `Reranker` trait with deterministic scores:

```rust
struct MockReranker {
    /// Returns scores in reverse document order (last doc scores highest)
    /// to verify that reranking actually reorders results.
}

#[async_trait]
impl Reranker for MockReranker {
    async fn rank(&self, _query: &str, documents: &[&str]) -> Result<Vec<f32>> {
        // Score by document length — longer docs rank higher
        Ok(documents.iter().map(|d| d.len() as f32).collect())
    }
}
```

Test scenarios:
1. **Reranking reorders results** — verify final order differs from retrieval order
2. **reranker_k controls candidate set** — verify exactly `reranker_k` candidates are reranked
3. **score column reflects reranker** — verify `score` = reranker score when reranker is enabled
4. **rerank_score column populated** — verify non-null when enabled, null when disabled
5. **vector_score / fts_score unchanged** — verify retrieval scores are preserved
6. **Error: missing alias** — verify error when alias not in catalog
7. **Error: vector query without reranker_query** — verify error message
8. **Default reranker_property** — verify falls back to FTS property in hybrid search
9. **Shared property fetch** — verify single storage call when both node and reranker are needed

### 11.3 TCK Scenarios

```gherkin
Feature: CrossEncoderReranking

  Scenario: Hybrid search with reranking reorders results
    Given a graph with Document nodes having 'embedding' and 'content' properties
    And a reranker model 'rerank/mock' is configured
    When I run:
      """
      CALL uni.search('Document', {vector: 'embedding', fts: 'content'},
          'search query', null, 5, null,
          {reranker: 'rerank/mock', reranker_property: 'content'})
      YIELD node, score, rerank_score, vector_score, fts_score
      RETURN node.title, score, rerank_score
      """
    Then the results should be ordered by rerank_score descending
    And rerank_score should not be null for any row
    And vector_score and fts_score should reflect original retrieval scores

  Scenario: Vector search with text query and reranking
    Given a graph with Document nodes having 'embedding' and 'content' properties
    And a reranker model 'rerank/mock' is configured
    When I run:
      """
      CALL uni.vector.query('Document', 'embedding', 'search query', 5,
          null, null, {reranker: 'rerank/mock', reranker_property: 'content'})
      YIELD node, score, rerank_score
      RETURN node.title, score, rerank_score
      """
    Then score should equal rerank_score

  Scenario: Reranker with pre-computed vector requires reranker_query
    When I run:
      """
      CALL uni.vector.query('Document', 'embedding', [0.1, 0.2], 5,
          null, null, {reranker: 'rerank/mock', reranker_property: 'content'})
      YIELD node, score
      """
    Then it should fail with "Cannot rerank: query is a pre-computed vector"

  Scenario: Reranker disabled by default
    When I run:
      """
      CALL uni.search('Document', {vector: 'embedding', fts: 'content'},
          'search query', null, 5)
      YIELD node, score, rerank_score
      RETURN node.title, rerank_score
      """
    Then rerank_score should be null for all rows
```

---

## 12. Future Extensions

These are explicitly **out of scope** for this proposal but noted for future consideration:

1. **Multi-field documents** — concatenate multiple properties (`title + body`) as the document input. Requires a `reranker_properties: ['title', 'body']` list and a join strategy (separator, template).

2. **Reranker cascades** — chain a fast reranker (MiniLM) then a slow one (GPT-based) for progressive refinement.

3. **Reranker-aware fusion** — instead of fuse-then-rerank, use the reranker as a fusion signal alongside vector and FTS scores (e.g., `fused = α·vec + β·fts + γ·rerank`).

4. **similar_to with reranking** — if a `LIMIT` is pushed down to the DataFusion plan, `similar_to` could theoretically rerank its top-k. Requires plan-level optimization, not expression-level.

5. **Caching** — cache reranker scores for repeated queries against the same document set (useful for paginated search).

---

## Appendix A: Full Options Reference

### `uni.search` Options (Arg 6)

| Key | Type | Default | Since |
|-----|------|---------|-------|
| `method` | `"rrf"` \| `"weighted"` | `"rrf"` | v0.1 |
| `alpha` | Float | `0.5` | v0.1 |
| `over_fetch` | Float | `2.0` | v0.1 |
| `rrf_k` | Integer | `60` | v0.1 |
| `reranker` | String | `null` | **this proposal** |
| `reranker_property` | String | FTS property | **this proposal** |
| `reranker_k` | Integer | `k * 3` | **this proposal** |
| `reranker_query` | String | `query_text` arg | **this proposal** |

### `uni.vector.query` / `uni.fts.query` Options (Arg 6, new)

| Key | Type | Default | Since |
|-----|------|---------|-------|
| `reranker` | String | `null` | **this proposal** |
| `reranker_property` | String | Required | **this proposal** |
| `reranker_k` | Integer | `k * 3` | **this proposal** |
| `reranker_query` | String | Query arg (arg 2) | **this proposal** |
