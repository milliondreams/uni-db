# MistralRS EmbeddingGemma produces NaN vectors on CPU with F16 dtype

## Summary

The `local/mistralrs` provider with `google/embeddinggemma-300m` produces embedding vectors where **all 768 values are `NaN`** when running on CPU. MistralRS auto-selects `F16` (half-precision) as the compute dtype, but F16 computation on CPU without native hardware support produces invalid results.

This silently corrupts all auto-embedded vectors stored in the database, making vector similarity search meaningless.

## Environment

- **uni-xervo**: 0.1.1
- **mistralrs-core**: 0.7.0
- **Platform**: Linux x86_64 (CPU only, no GPU)
- **CPU**: Does not have native F16 compute support
- **Model**: `google/embeddinggemma-300m` (768-dimensional, SafeTensors format)

## Reproduction

### Minimal test case

```rust
use anyhow::Result;
use serde_json::json;
use uni_db::Uni;
use uni_db::api::schema::{EmbeddingCfg, IndexType, VectorAlgo, VectorIndexCfg, VectorMetric};
use uni_xervo::api::{ModelAliasSpec, ModelTask, WarmupPolicy};

#[tokio::test]
async fn test_mistralrs_f16_nan_embeddings() -> Result<()> {
    let db = Uni::temporary()
        .xervo_catalog(vec![ModelAliasSpec {
            alias: "embed/default".to_string(),
            task: ModelTask::Embed,
            provider_id: "local/mistralrs".to_string(),
            model_id: "google/embeddinggemma-300m".to_string(),
            revision: None,
            warmup: WarmupPolicy::Lazy,
            required: false,
            timeout: None,
            load_timeout: None,
            retry: None,
            options: json!({}),
        }])
        .build()
        .await?;

    db.schema()
        .label("Article")
        .property("title", uni_db::DataType::String)
        .property("body", uni_db::DataType::String)
        .property("embedding", uni_db::DataType::Vector { dimensions: 768 })
        .index(
            "embedding",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::Cosine,
                embedding: Some(EmbeddingCfg {
                    alias: "embed/default".to_string(),
                    source_properties: vec!["title".to_string(), "body".to_string()],
                    batch_size: 32,
                }),
            }),
        )
        .apply()
        .await?;

    // Auto-embed fills in the embedding on write
    db.execute(
        "CREATE (:Article {title: 'Rust Guide', \
         body: 'Rust is a systems programming language'})",
    )
    .await?;
    db.flush().await?;

    // Read back the stored embedding
    let result = db
        .query("MATCH (a:Article) RETURN a.embedding AS emb")
        .await?;
    let emb: Vec<f64> = result.rows[0].get("emb")?;

    // FAILS: all 768 values are NaN
    let nan_count = emb.iter().filter(|x| x.is_nan()).count();
    assert_eq!(
        nan_count, 0,
        "Expected valid embedding values, but {nan_count}/768 are NaN"
    );

    Ok(())
}
```

### MistralRS log output showing F16 selection

```
INFO mistralrs_core::pipeline::embedding: Loading `tokenizer.json` at `google/embeddinggemma-300m`
INFO mistralrs_core::pipeline::embedding: Loading `config.json` at `google/embeddinggemma-300m`
INFO mistralrs_core::pipeline::embedding: Prompt chunk size is 1024.
INFO mistralrs_core::utils::normal: DType selected is F16.           <-- ROOT CAUSE
INFO mistralrs_quant::utils::log: Automatic loader type determined to be `embeddinggemma`
INFO mistralrs_quant::utils::log: Layers 0-23: cpu (63 GB)
```

## Impact

### Silent data corruption

The NaN embeddings are stored without error. Downstream operations are affected differently:

| Operation | Behavior | Detection |
|-----------|----------|-----------|
| `IS NOT NULL` check | **Passes** — NaN is not NULL in Arrow | ❌ Not detected |
| `~=` vector search (Lance ANN) | **Returns results** — Lance returns k-nearest regardless | ❌ Not detected (if only checking count) |
| `similar_to()` cosine similarity | **Returns NaN** — NaN propagates through arithmetic | ✅ Detected |
| Manual score validation | **Fails** — scores are NaN, not in [0, 1] | ✅ Detected |

### Existing tests that silently pass with NaN

The `test_mistralrs_embeddinggemma_auto_embed` test only checks `IS NOT NULL`:
```rust
let result = db
    .query("MATCH (d:Document) WHERE d.embedding IS NOT NULL RETURN count(d) AS cnt")
    .await?;
assert_eq!(emb_count, 1); // Passes because NaN ≠ NULL
```

The `test_mistralrs_embeddinggemma_vector_search` test only checks result count:
```rust
let results = db
    .query_with("MATCH (a:Article) WHERE a.embedding ~= $q RETURN a.title LIMIT 1")
    .param("q", probe)
    .fetch_all()
    .await?;
assert_eq!(results.rows.len(), 1); // Passes because Lance returns something
```

## Root Cause

MistralRS 0.7.0 auto-selects `F16` (half-precision float) as the compute dtype for `embeddinggemma-300m`. On CPUs without native F16 hardware support, F16 arithmetic produces NaN values. The model weights load correctly, but forward-pass computation generates NaN activations that propagate to the output embeddings.

## Suggested Fixes

### 1. Expose dtype option in `ModelAliasSpec.options` (recommended)

Allow users to force F32 via the existing `options` JSON field:

```rust
ModelAliasSpec {
    options: json!({ "dtype": "f32" }),  // Force F32 on CPU
    ..
}
```

The MistralRS provider should parse this option and pass `DType::F32` to the pipeline builder instead of relying on auto-detection.

### 2. Auto-detect and fallback

When running on CPU without F16 hardware support, automatically use F32:

```rust
// In the MistralRS provider initialization
let dtype = if has_gpu() {
    DType::F16  // F16 is fast and correct on GPU
} else {
    DType::F32  // F32 is safe on CPU
};
```

### 3. Validate embeddings post-inference

Add a validation check after embedding inference to catch NaN/Inf values:

```rust
let embeddings = embedder.embed(texts).await?;
for emb in &embeddings {
    if emb.iter().any(|v| v.is_nan() || v.is_infinite()) {
        return Err(anyhow!(
            "Embedding model produced invalid values (NaN/Inf). \
             This may be caused by F16 computation on CPU. \
             Try setting options: {{\"dtype\": \"f32\"}} in the model alias."
        ));
    }
}
```

## Affected Tests

These tests in `uni-db` are affected (all `#[ignore]`, require `--features provider-mistralrs`):

- `similar_to_integration::auto_embed_tests::test_similar_to_auto_embed_string_query`
- `similar_to_integration::auto_embed_tests::test_similar_to_auto_embed_in_where`
- `embedding_stack_overflow_test::mistralrs_tests::test_mistralrs_embeddinggemma_auto_embed` (passes but with corrupt data)
- `embedding_stack_overflow_test::mistralrs_tests::test_mistralrs_embeddinggemma_vector_search` (passes but with corrupt data)
