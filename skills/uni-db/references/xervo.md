# Xervo ML Runtime Reference

## 1. Overview

Xervo is uni-db's built-in ML runtime for embeddings and text generation. It supports local inference (Candle, FastEmbed, mistral.rs) and remote APIs (OpenAI, Anthropic, Gemini, Mistral, Cohere, Voyage AI, Vertex AI, Azure OpenAI). Models are configured via a **catalog** of alias specs, and accessed through the `db.xervo()` facade.

**Key concepts:**
- **Alias**: human-readable model identifier in `task/name` format (e.g., `embed/default`, `llm/gpt4`)
- **Provider**: inference backend (`local/candle`, `remote/openai`, etc.)
- **Catalog**: JSON array of model alias specs loaded at database startup
- **Auto-embedding**: vector indexes can automatically embed text on write using a catalog alias

---

## 2. Providers

### Local Providers

Run inference on the host machine. No API keys needed.

| Provider ID | Engine | Tasks | Best For |
|---|---|---|---|
| `local/candle` | HuggingFace Candle | Embed | Lightweight CPU embeddings (Bert, JinaBert, Gemma) |
| `local/fastembed` | ONNX Runtime | Embed | Fastest CPU embeddings, smallest footprint |
| `local/mistralrs` | mistral.rs | Embed, Generate | GPU inference, quantized models, multi-modal |

**Local provider options:**

```json
// candle / fastembed
{ "cache_dir": "/path/to/model/cache" }

// mistralrs
{
  "isq": "Q4K",                    // in-situ quantization type
  "force_cpu": false,              // force CPU even if GPU available
  "dtype": "auto",                 // "auto", "f16", "bf16", "f32"
  "pipeline": "text",             // "text", "vision", "diffusion", "speech"
  "paged_attention": true,         // enable paged attention
  "max_num_seqs": 16,              // max concurrent sequences
  "embedding_dimensions": 768,     // override output dimensions
  "chat_template": "/path/to/template",
  "tokenizer_json": "/path/to/tokenizer.json",
  "gguf_files": ["model.gguf"]    // for GGUF models
}
```

### Remote Providers

Call external APIs. Require API keys via environment variables.

| Provider ID | Tasks | Default Env Var | Auth Header |
|---|---|---|---|
| `remote/openai` | Embed, Generate | `OPENAI_API_KEY` | `Authorization: Bearer` |
| `remote/anthropic` | Generate only | `ANTHROPIC_API_KEY` | `x-api-key` |
| `remote/gemini` | Embed, Generate | `GEMINI_API_KEY` | Query parameter |
| `remote/vertexai` | Embed, Generate | `VERTEXAI_API_TOKEN` | `Authorization: Bearer` |
| `remote/mistral` | Embed, Generate | `MISTRAL_API_KEY` | `Authorization: Bearer` |
| `remote/voyageai` | Embed, Rerank | `VOYAGEAI_API_KEY` | `Authorization: Bearer` |
| `remote/cohere` | Embed, Rerank, Generate | `CO_API_KEY` | `Authorization: Bearer` |
| `remote/azure-openai` | Embed, Generate | `AZURE_OPENAI_API_KEY` | `api-key` header |

**Provider-specific options:**

```json
// All remote providers — custom API key env var
{ "api_key_env": "MY_CUSTOM_KEY" }

// Anthropic
{ "anthropic_version": "2023-06-01" }

// Vertex AI
{
  "api_token_env": "MY_TOKEN",
  "project_id": "my-gcp-project",
  "location": "us-central1",
  "publisher": "google",
  "embedding_dimensions": 768
}

// Cohere
{ "input_type": "search_document" }

// Azure OpenAI
{
  "resource_name": "my-azure-resource",
  "api_version": "2024-02-01"
}
```

---

## 3. Catalog Format

The catalog is a JSON array of model alias specs:

```json
[
  {
    "alias": "embed/default",
    "task": "Embed",
    "provider_id": "local/fastembed",
    "model_id": "sentence-transformers/all-MiniLM-L6-v2",
    "revision": null,
    "warmup": "Lazy",
    "required": false,
    "timeout": null,
    "load_timeout": null,
    "retry": null,
    "options": {}
  },
  {
    "alias": "llm/default",
    "task": "Generate",
    "provider_id": "remote/openai",
    "model_id": "gpt-4o-mini",
    "warmup": "Lazy",
    "required": false,
    "timeout": 30,
    "retry": { "max_attempts": 3, "initial_backoff_ms": 200 },
    "options": {}
  }
]
```

### Field Reference

| Field | Required | Default | Description |
|---|---|---|---|
| `alias` | Yes | — | Identifier in `task/name` format (must contain `/`) |
| `task` | Yes | — | `"Embed"`, `"Generate"`, or `"Rerank"` |
| `provider_id` | Yes | — | Provider identifier (e.g., `"remote/openai"`) |
| `model_id` | Yes | — | HuggingFace repo ID (local) or API model name (remote) |
| `revision` | No | `null` | HuggingFace branch/tag/commit hash (local only) |
| `warmup` | No | `"Lazy"` | `"Lazy"`, `"Eager"`, or `"Background"` |
| `required` | No | `false` | If true, failed eager warmup aborts database startup |
| `timeout` | No | `null` | Per-inference timeout in seconds |
| `load_timeout` | No | `600` | Model load timeout in seconds |
| `retry` | No | `null` | `{ "max_attempts": N, "initial_backoff_ms": N }` |
| `options` | No | `{}` | Provider-specific JSON (see provider sections above) |

### Warmup Policies

| Policy | Behavior |
|---|---|
| `Lazy` | Load model on first inference request (default) |
| `Eager` | Load during database startup; blocks until loaded. If `required: true`, startup fails on error |
| `Background` | Spawn background load at startup; requests before load completes will wait |

### Model Deduplication

Multiple aliases pointing to the same `(provider_id, model_id, revision, options)` share a single loaded model instance.

---

## 4. Configuration

### Python

```python
from uni_db import Uni

# From JSON file
db = Uni.open("./my_db") \
    .xervo_catalog_from_file("./catalog.json") \
    .build()

# From JSON string
catalog_json = '[{"alias": "embed/default", "task": "Embed", ...}]'
db = Uni.open("./my_db") \
    .xervo_catalog_from_str(catalog_json) \
    .build()
```

### Rust

```rust
use uni_db::Uni;

// From catalog vector
let db = Uni::open("./my_db")
    .xervo_catalog(catalog_vec)
    .build()
    .await?;

// From JSON file
let db = Uni::open("./my_db")
    .xervo_catalog_from_file("./catalog.json")?
    .build()
    .await?;

// From JSON string
let db = Uni::open("./my_db")
    .xervo_catalog_from_str(&json_string)?
    .build()
    .await?;
```

### Environment Variables

Set API keys before opening the database:

```bash
export OPENAI_API_KEY="sk-..."
export ANTHROPIC_API_KEY="sk-ant-..."
export GEMINI_API_KEY="..."
```

Or use custom env var names per alias:

```json
{ "options": { "api_key_env": "MY_PROJECT_OPENAI_KEY" } }
```

---

## 5. Usage API

### Embeddings

```python
xervo = db.xervo()

# Embed a batch of texts
vectors = xervo.embed("embed/default", [
    "Graph databases store relationships.",
    "Vector search enables semantic retrieval."
])
# vectors: list[list[float]], one vector per input text

# Async
vectors = await async_db.xervo().embed("embed/default", texts)
```

### Text Generation

```python
from uni_db import Message

xervo = db.xervo()

# Simple text prompt
result = xervo.generate_text("llm/default", "What is a graph database?")
print(result.text)

# With options
result = xervo.generate_text(
    "llm/default",
    "Explain HNSW indexing.",
    max_tokens=500,
    temperature=0.3,
    top_p=0.9
)

# Structured messages
result = xervo.generate("llm/default", [
    Message.system("You are a graph database expert."),
    Message.user("What is the difference between BFS and DFS?"),
])
print(result.text)
print(result.usage)  # TokenUsage(prompt_tokens=..., completion_tokens=..., total_tokens=...)

# Dict messages also accepted
result = xervo.generate("llm/default", [
    {"role": "system", "content": "Be concise."},
    {"role": "user", "content": "What is PageRank?"},
])
```

### Availability Check

```python
if db.xervo().is_available():
    vectors = db.xervo().embed("embed/default", texts)
else:
    # No catalog configured — use fallback
    pass
```

### Result Types

```python
class GenerationResult:
    text: str                    # Generated text
    usage: TokenUsage | None     # Token counts (if provider reports them)

class TokenUsage:
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
```

---

## 6. Auto-Embedding

Vector indexes can automatically embed text properties on write and query.

### Schema Configuration

```python
db.schema() \
    .label("Document") \
        .property("title", DataType.STRING()) \
        .property("content", DataType.STRING()) \
        .vector("embedding", 384) \
        .index("embedding", {
            "type": "vector",
            "metric": "cosine",
            "embedding": {
                "alias": "embed/default",
                "source": ["title", "content"],
                "batch_size": 32
            }
        }) \
    .apply()
```

### Cypher DDL

```cypher
CREATE VECTOR INDEX doc_embeddings
FOR (d:Document) ON d.embedding
OPTIONS {
  embedding: {
    alias: 'embed/default',
    source: ['title', 'content'],
    batch_size: 32
  }
}
```

### Embedding Config Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `alias` | Yes | — | Model alias from Xervo catalog |
| `source` | Yes | — | List of text properties to concatenate and embed |
| `batch_size` | No | `32` | Texts per batch for API calls |

**Behavior:**
- **On write**: text from `source` properties is concatenated and embedded automatically
- **On query**: string arguments to `uni.vector.query()` and `similar_to()` are auto-embedded using the same alias
- **Validation**: database startup fails if schema references an alias not in the catalog

---

## 7. Reliability

### Circuit Breaker

Built-in per provider/model, protects against cascading failures:

| Parameter | Default | Description |
|---|---|---|
| Failure threshold | 5 | Consecutive failures before opening circuit |
| Recovery wait | 10s | Time before attempting half-open probe |
| State machine | Closed -> Open -> HalfOpen -> Closed | Auto-recovers on successful probe |

### Retry

Exponential backoff for transient errors (429 rate limits, 5xx server errors, timeouts):

```json
{
  "retry": {
    "max_attempts": 3,
    "initial_backoff_ms": 100
  }
}
```

Backoff formula: `initial_backoff_ms * 2^(attempt - 1)` (100ms, 200ms, 400ms with defaults).

### Metrics

Automatic instrumentation (when metrics enabled):

| Metric | Type | Labels |
|---|---|---|
| `model_load.duration_seconds` | Histogram | alias, provider |
| `model_load.total` | Counter | alias, provider, status |
| `model_inference.duration_seconds` | Histogram | alias, task, provider |
| `model_inference.total` | Counter | alias, task, provider, status |

---

## 8. Recommended Models

### Embedding Models

| Use Case | Provider | Model ID | Dimensions | Notes |
|---|---|---|---|---|
| **General purpose (local)** | `local/fastembed` | `sentence-transformers/all-MiniLM-L6-v2` | 384 | Fast, small, good quality. Best default for local. |
| **Higher quality (local)** | `local/candle` | `BAAI/bge-base-en-v1.5` | 768 | Better quality, larger model |
| **Best quality (local)** | `local/mistralrs` | `nomic-ai/nomic-embed-text-v1.5` | 768 | Top-tier open-source embeddings |
| **Production (remote)** | `remote/openai` | `text-embedding-3-small` | 1536 | Best cost/quality ratio for OpenAI |
| **Max quality (remote)** | `remote/openai` | `text-embedding-3-large` | 3072 | Highest quality OpenAI embeddings |
| **Privacy-focused (remote)** | `remote/voyageai` | `voyage-large-2` | 1536 | Strong retrieval, data not used for training |
| **Multilingual (remote)** | `remote/cohere` | `embed-multilingual-v3.0` | 1024 | 100+ languages |

### Generation Models

| Use Case | Provider | Model ID | Notes |
|---|---|---|---|
| **Fast + cheap** | `remote/openai` | `gpt-4o-mini` | Good for RAG summarization, structured extraction |
| **Best quality** | `remote/openai` | `gpt-4o` | Complex reasoning, detailed analysis |
| **Long context** | `remote/anthropic` | `claude-sonnet-4-6` | 200k context, strong instruction following |
| **Code generation** | `remote/anthropic` | `claude-sonnet-4-6` | Best for code tasks |
| **Local (GPU)** | `local/mistralrs` | `mistralai/Mistral-7B-Instruct-v0.3` | Requires GPU, fully private |
| **Local (quantized)** | `local/mistralrs` | Any GGUF model | CPU-viable with Q4/Q5 quantization |

### Reranking Models

| Use Case | Provider | Model ID | Notes |
|---|---|---|---|
| **Best quality** | `remote/voyageai` | `rerank-2` | Top-tier reranking accuracy |
| **Cost-effective** | `remote/cohere` | `rerank-english-v3.0` | Good quality, lower cost |

---

## 9. Example Catalogs

### Minimal Local-Only

```json
[
  {
    "alias": "embed/default",
    "task": "Embed",
    "provider_id": "local/fastembed",
    "model_id": "sentence-transformers/all-MiniLM-L6-v2"
  }
]
```

### Production RAG Pipeline

```json
[
  {
    "alias": "embed/default",
    "task": "Embed",
    "provider_id": "remote/openai",
    "model_id": "text-embedding-3-small",
    "warmup": "Background",
    "timeout": 10,
    "retry": { "max_attempts": 3, "initial_backoff_ms": 200 }
  },
  {
    "alias": "llm/default",
    "task": "Generate",
    "provider_id": "remote/openai",
    "model_id": "gpt-4o-mini",
    "timeout": 30,
    "retry": { "max_attempts": 2, "initial_backoff_ms": 500 }
  },
  {
    "alias": "llm/reasoning",
    "task": "Generate",
    "provider_id": "remote/anthropic",
    "model_id": "claude-sonnet-4-6",
    "timeout": 60,
    "retry": { "max_attempts": 2, "initial_backoff_ms": 1000 }
  }
]
```

### Hybrid Local + Remote

```json
[
  {
    "alias": "embed/local",
    "task": "Embed",
    "provider_id": "local/fastembed",
    "model_id": "sentence-transformers/all-MiniLM-L6-v2",
    "warmup": "Eager",
    "required": true
  },
  {
    "alias": "embed/remote",
    "task": "Embed",
    "provider_id": "remote/openai",
    "model_id": "text-embedding-3-small",
    "warmup": "Lazy"
  },
  {
    "alias": "llm/default",
    "task": "Generate",
    "provider_id": "remote/openai",
    "model_id": "gpt-4o-mini",
    "timeout": 30
  }
]
```

### Azure Enterprise

```json
[
  {
    "alias": "embed/default",
    "task": "Embed",
    "provider_id": "remote/azure-openai",
    "model_id": "text-embedding-3-small",
    "options": {
      "resource_name": "my-company-openai",
      "api_version": "2024-02-01",
      "api_key_env": "AZURE_OPENAI_KEY"
    }
  },
  {
    "alias": "llm/default",
    "task": "Generate",
    "provider_id": "remote/azure-openai",
    "model_id": "gpt-4o",
    "options": {
      "resource_name": "my-company-openai",
      "api_version": "2024-02-01"
    }
  }
]
```

---

## 10. Common Patterns

### RAG Pipeline

```python
db = Uni.open("./rag_db") \
    .xervo_catalog_from_file("./catalog.json") \
    .build()

xervo = db.xervo()
session = db.session()

# Embed and insert documents
with session.tx() as tx:
    docs = ["Graph databases model relationships.", "Vector search finds similar items."]
    vectors = xervo.embed("embed/default", docs)
    for text, vec in zip(docs, vectors):
        tx.execute(
            "CREATE (:Doc {text: $t, embedding: $e})",
            params={"t": text, "e": vec}
        )
    tx.commit()

# Query: embed question, retrieve, generate
question = "How do graph databases work?"
q_vec = xervo.embed("embed/default", [question])[0]

results = session.query("""
    CALL uni.vector.query('Doc', 'embedding', $qv, 5)
    YIELD node, score
    RETURN node.text AS text, score
""", params={"qv": q_vec})

context = "\n".join(row["text"] for row in results)
answer = xervo.generate_text("llm/default", f"Context:\n{context}\n\nQ: {question}")
print(answer.text)
```

### Auto-Embedding (No Manual Embed Calls)

```python
# Configure catalog + schema with embedding config
db = Uni.open("./auto_db") \
    .xervo_catalog_from_file("./catalog.json") \
    .build()

db.schema() \
    .label("Doc") \
        .property("text", DataType.STRING()) \
        .vector("embedding", 384) \
        .index("embedding", {
            "type": "vector",
            "metric": "cosine",
            "embedding": {"alias": "embed/default", "source": ["text"]}
        }) \
    .apply()

# Just insert text — embedding happens automatically
with db.session().tx() as tx:
    tx.execute("CREATE (:Doc {text: 'Graph databases are powerful.'})")
    tx.commit()

# Query with text — auto-embedded at query time too
results = db.session().query("""
    CALL uni.vector.query('Doc', 'embedding', 'How do graphs work?', 5)
    YIELD node, score RETURN node.text, score
""")
```

---

## 11. Gotchas

1. **Xervo must be configured at startup** -- `db.xervo()` returns a facade that checks `is_available()`. If no catalog was provided via `xervo_catalog_from_file()` or `xervo_catalog_from_str()`, all embed/generate calls will error.

2. **Schema validation at startup** -- If your schema has vector indexes with `embedding.alias` config, the database will fail to open if the referenced alias is not in the catalog.

3. **Embedding dimensions must match** -- The vector property dimensions (e.g., `vector("embedding", 384)`) must match the model's output dimensions. Mismatches cause runtime errors on insert.

4. **Anthropic is generate-only** -- `remote/anthropic` does not support embeddings. Use a different provider for `Embed` tasks.

5. **Local models download on first use** -- `local/candle` and `local/fastembed` download models from HuggingFace on first load. Use `warmup: "Eager"` with `required: true` to catch download failures at startup rather than at first request.

6. **API keys are read at model load time** -- Environment variables are resolved when the model is first loaded (or at startup for eager warmup), not when the database is opened.

7. **Retry only applies to transient errors** -- 4xx errors (except 429 rate limits) are not retried. Invalid API keys, malformed requests, and model-not-found errors fail immediately.

8. **Alias format requires a slash** -- Aliases must be in `task/name` format (e.g., `embed/default`). Aliases without `/` are rejected during catalog validation.
