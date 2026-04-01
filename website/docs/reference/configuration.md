# Configuration Reference

This document provides a comprehensive reference for all Uni configuration options, environment variables, and tuning parameters.

## Configuration Overview

**Status (2026-01-30):** Configuration is currently applied **programmatically** via the Rust API (`UniConfig`) or the Python builder. Configuration files (`uni.toml`) and `UNI_*` environment overrides are **planned** but not wired into the CLI/server yet.

Uni can be configured through:
1. **Rust API** — `UniConfig` (available)
2. **Environment Variables** — Planned (not yet supported)
3. **Configuration File** — Planned (`uni.toml` or JSON)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       CONFIGURATION HIERARCHY                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Defaults (Code)                                                           │
│       ↓ overridden by                                                       │
│   Configuration File (uni.toml) [planned]                                   │
│       ↓ overridden by                                                       │
│   Environment Variables (UNI_*) [planned]                                   │
│       ↓ overridden by                                                       │
│   Programmatic Config (Rust API)                                            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Main Configuration

### UniConfig

```rust
pub struct UniConfig {
    /// Maximum adjacency cache size in bytes (default: 1GB)
    pub cache_size: usize,

    /// Number of worker threads for parallel execution
    pub parallelism: usize,

    /// Size of each data morsel/batch (number of rows)
    pub batch_size: usize,

    /// Maximum size of traversal frontier before pruning
    pub max_frontier_size: usize,

    /// Auto-flush threshold for L0 buffer (default: 10_000 mutations)
    pub auto_flush_threshold: usize,

    /// Auto-flush interval for L0 buffer (default: 5 seconds).
    /// Flush triggers if time elapsed AND mutation count >= auto_flush_min_mutations.
    /// Set to None to disable time-based flush.
    pub auto_flush_interval: Option<Duration>,

    /// Minimum mutations required before time-based flush triggers (default: 1).
    /// Prevents unnecessary flushes when there's minimal activity.
    pub auto_flush_min_mutations: usize,

    /// Enable write-ahead logging (default: true)
    pub wal_enabled: bool,

    /// Compaction configuration
    pub compaction: CompactionConfig,

    /// Write throttling configuration
    pub throttle: WriteThrottleConfig,


    /// File sandbox configuration for BACKUP/COPY commands
    pub file_sandbox: FileSandboxConfig,

    /// Default query execution timeout (default: 30s)
    pub query_timeout: Duration,

    /// Default maximum memory per query (default: 1GB)
    pub max_query_memory: usize,

    /// Maximum transaction buffer memory in bytes (default: 1GB)
    pub max_transaction_memory: usize,

    /// Maximum rows allowed for in-memory compaction (default: 5M)
    pub max_compaction_rows: usize,

    /// Enable in-memory VID→labels index for O(1) lookups (default: true)
    pub enable_vid_labels_index: bool,

    /// Object store resilience configuration
    pub object_store: ObjectStoreConfig,

    /// Maximum iterations for recursive CTE evaluation (default: 1000)
    pub max_recursive_cte_iterations: usize,

    /// Background index rebuild configuration
    pub index_rebuild: IndexRebuildConfig,
}
```

### Parameter Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `cache_size` | bytes | 1 GB | Maximum adjacency cache size |
| `parallelism` | count | CPU cores | Worker threads for parallel execution |
| `batch_size` | rows | 1,024 | Rows per morsel/batch |
| `max_frontier_size` | count | 1,000,000 | Maximum traversal frontier size |
| `auto_flush_threshold` | count | 10,000 | Mutations triggering auto-flush |
| `auto_flush_interval` | duration | 5s | Time-based flush interval (None to disable) |
| `auto_flush_min_mutations` | count | 1 | Minimum mutations for time-based flush |
| `wal_enabled` | bool | true | Enable write-ahead logging |
| `query_timeout` | duration | 30s | Default query execution timeout |
| `max_query_memory` | bytes | 1 GB | Maximum memory per query |
| `max_transaction_memory` | bytes | 1 GB | Maximum memory per transaction |
| `max_compaction_rows` | rows | 5,000,000 | OOM guard for in-memory compaction |
| `enable_vid_labels_index` | bool | true | Enable O(1) VID→labels lookups |
| `max_recursive_cte_iterations` | count | 1,000 | Maximum iterations for recursive CTE evaluation |

### CompactionConfig

```rust
pub struct CompactionConfig {
    /// Enable background compaction (default: true)
    pub enabled: bool,

    /// Max L1 runs before triggering compaction (default: 4)
    pub max_l1_runs: usize,

    /// Max L1 size in bytes before compaction (default: 256MB)
    pub max_l1_size_bytes: u64,

    /// Max age of oldest L1 run before compaction (default: 1 hour)
    pub max_l1_age: Duration,

    /// Background check interval (default: 30s)
    pub check_interval: Duration,

    /// Number of compaction worker threads (default: 1)
    pub worker_threads: usize,
}
```

### WriteThrottleConfig

```rust
pub struct WriteThrottleConfig {
    /// L1 run count to start throttling (default: 8)
    pub soft_limit: usize,

    /// L1 run count to stop writes entirely (default: 16)
    pub hard_limit: usize,

    /// Base delay when throttling (default: 10ms)
    pub base_delay: Duration,
}
```

### FileSandboxConfig (Security-Critical)

```rust
pub struct FileSandboxConfig {
    /// If true, file operations are restricted to allowed_paths
    /// MUST be enabled for server mode with untrusted clients
    pub enabled: bool,

    /// List of allowed base directories for file operations
    pub allowed_paths: Vec<PathBuf>,
}
```

**Security Note:** File sandbox MUST be enabled in server mode to prevent path traversal attacks (CWE-22).

### ObjectStoreConfig

```rust
pub struct ObjectStoreConfig {
    pub connect_timeout: Duration,    // Default: 10s
    pub read_timeout: Duration,       // Default: 30s
    pub write_timeout: Duration,      // Default: 60s
    pub max_retries: u32,             // Default: 3
    pub retry_backoff_base: Duration, // Default: 100ms
    pub retry_backoff_max: Duration,  // Default: 10s
}
```

### IndexRebuildConfig

```rust
pub struct IndexRebuildConfig {
    /// Maximum retry attempts for failed builds (default: 3)
    pub max_retries: u32,

    /// Delay between retry attempts (default: 60s)
    pub retry_delay: Duration,

    /// Check interval for pending tasks (default: 5s)
    pub worker_check_interval: Duration,

    /// Row growth ratio to trigger rebuild (default: 0.5)
    pub growth_trigger_ratio: f64,

    /// Max index age before rebuild (default: None/disabled)
    pub max_index_age: Option<Duration>,

    /// Enable post-flush automatic rebuild scheduling (default: false)
    pub auto_rebuild_enabled: bool,
}
```

---

## Locy Configuration

### LocyConfig

Configuration for the Locy (Datalog) rule engine. These settings control fixpoint evaluation, probabilistic reasoning, abductive inference, and other Locy-specific behavior.

```rust
pub struct LocyConfig {
    /// Maximum fixpoint iterations per recursive stratum (default: 1000)
    pub max_iterations: usize,

    /// Overall evaluation timeout (default: 300s)
    pub timeout: Duration,

    /// Maximum bytes of derived facts to hold in memory per relation (default: 256MB)
    pub max_derived_bytes: usize,

    /// Maximum recursion depth for EXPLAIN derivation trees (default: 100)
    pub max_explain_depth: usize,

    /// Maximum recursion depth for SLG resolution (default: 1000)
    pub max_slg_depth: usize,

    /// Maximum candidate modifications to generate during ABDUCE (default: 20)
    pub max_abduce_candidates: usize,

    /// Maximum validated results to return from ABDUCE (default: 10)
    pub max_abduce_results: usize,

    /// When true, MNOR/MPROD reject values outside [0,1] with an error
    /// instead of clamping (default: false)
    pub strict_probability_domain: bool,

    /// Underflow threshold for MPROD log-space switch (default: 1e-15)
    pub probability_epsilon: f64,

    /// When true, use exact BDD-based probability computation for groups
    /// with shared dependencies instead of independence assumption (default: false)
    pub exact_probability: bool,

    /// Maximum BDD variables per aggregation group (default: 1000)
    pub max_bdd_variables: usize,

    /// Top-k proof filtering: retain at most k proofs per derived fact.
    /// 0 means unlimited (default: 0)
    pub top_k_proofs: usize,

    /// Override top_k_proofs during training. None uses top_k_proofs
    /// for both training and inference (default: None)
    pub top_k_proofs_training: Option<usize>,

    /// When true, BEST BY applies secondary sort for deterministic
    /// tie-breaking (default: true)
    pub deterministic_best_by: bool,

    /// Parameters bound to $name references inside rules and
    /// QUERY/RETURN expressions (default: empty)
    pub params: HashMap<String, Value>,
}
```

### Locy Parameter Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_iterations` | count | 1,000 | Maximum fixpoint iterations per recursive stratum |
| `timeout` | duration | 300s | Overall Locy evaluation timeout |
| `max_derived_bytes` | bytes | 256 MB | Maximum derived facts memory per relation |
| `max_explain_depth` | count | 100 | Maximum recursion depth for EXPLAIN trees |
| `max_slg_depth` | count | 1,000 | Maximum recursion depth for SLG resolution |
| `max_abduce_candidates` | count | 20 | Maximum candidate modifications for ABDUCE |
| `max_abduce_results` | count | 10 | Maximum validated ABDUCE results |
| `strict_probability_domain` | bool | false | Reject MNOR/MPROD values outside [0,1] instead of clamping |
| `probability_epsilon` | float | 1e-15 | Underflow threshold for MPROD log-space switch |
| `exact_probability` | bool | false | Use BDD-based exact probability for shared-dependency groups |
| `max_bdd_variables` | count | 1,000 | Maximum BDD variables per aggregation group |
| `top_k_proofs` | count | 0 (unlimited) | Retain at most k proofs per derived fact (0 = all) |
| `top_k_proofs_training` | count | None | Override `top_k_proofs` during training |
| `deterministic_best_by` | bool | true | Deterministic tie-breaking for BEST BY |
| `params` | map | empty | Named parameters bound to `$name` references |

---

## Query and Index Tuning

Query limits and timeouts are configured via `UniConfig` (or per-query via `QueryBuilder.timeout()` / `QueryBuilder.max_memory()`).

Index tuning is **not** controlled by `UniConfig`. Cypher DDL currently selects index **type** only; for metrics or advanced parameters, use the Rust/Python schema builders (`VectorIndexCfg`, `VectorAlgo`).

---

## Environment Variables

Uni does not currently read any `UNI_*` variables. Environment variables in use today:

| Variable | Used For |
|----------|----------|
| `RUST_LOG` | Log level filter |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_REGION`, `AWS_ENDPOINT_URL` | `CloudStorageConfig::s3_from_env` |
| `GOOGLE_APPLICATION_CREDENTIALS` | `CloudStorageConfig::gcs_from_env` |
| `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_ACCESS_KEY`, `AZURE_STORAGE_SAS_TOKEN` | `CloudStorageConfig::azure_from_env` |

---

## Cloud Storage Configuration

Uni supports cloud storage backends for bulk data storage while keeping metadata and WAL local for low latency.

### Supported Backends

| Backend | URL Scheme | Status |
|---------|------------|--------|
| Local filesystem | `/path` or `file://` | Fully supported |
| Amazon S3 | `s3://bucket/path` | Supported |
| Google Cloud Storage | `gs://bucket/path` | Supported |
| Azure Blob Storage | `az://account/container` | Supported |
| S3-compatible (MinIO) | `s3://` with custom endpoint | Supported |

### Hybrid Mode

Hybrid mode stores bulk data in cloud storage while keeping WAL and metadata local:

```rust
use uni_db::Uni;

let db = Uni::open("./local_meta")
    .hybrid("./local_meta", "s3://my-bucket/graph-data")
    .build()
    .await?;
```

**Benefits:**
- Low-latency writes via local WAL
- Scalable storage via cloud object store
- Cost-effective for large datasets

### S3 Configuration

```rust
use uni_db::Uni;
use uni_common::CloudStorageConfig;

// Using environment variables (recommended)
let config = CloudStorageConfig::s3_from_env("my-bucket");

// Or explicit configuration
let config = CloudStorageConfig::S3 {
    bucket: "my-bucket".to_string(),
    region: Some("us-west-2".to_string()),
    endpoint: None,  // Use AWS default
    access_key_id: None,  // Use env/IAM
    secret_access_key: None,
    session_token: None,
    virtual_hosted_style: true,
};

let db = Uni::open("./local")
    .hybrid("./local", "s3://my-bucket/data")
    .cloud_config(config)
    .build()
    .await?;
```

**For S3-compatible services (MinIO, LocalStack):**

```rust
let config = CloudStorageConfig::S3 {
    bucket: "test-bucket".to_string(),
    region: Some("us-east-1".to_string()),
    endpoint: Some("http://localhost:9000".to_string()),
    access_key_id: Some("minioadmin".to_string()),
    secret_access_key: Some("minioadmin".to_string()),
    session_token: None,
    virtual_hosted_style: false,  // Path-style for MinIO
};
```

### GCS Configuration

```rust
use uni_common::CloudStorageConfig;

// Using environment variable (GOOGLE_APPLICATION_CREDENTIALS)
let config = CloudStorageConfig::gcs_from_env("my-gcs-bucket");

// Or explicit configuration
let config = CloudStorageConfig::Gcs {
    bucket: "my-gcs-bucket".to_string(),
    service_account_path: Some("/path/to/service-account.json".to_string()),
    service_account_key: None,
};
```

### Azure Configuration

```rust
use uni_common::CloudStorageConfig;

// Using environment variables
let config = CloudStorageConfig::azure_from_env("my-container");

// Or explicit configuration
let config = CloudStorageConfig::Azure {
    container: "my-container".to_string(),
    account: "mystorageaccount".to_string(),
    access_key: Some("account-key".to_string()),
    sas_token: None,
};
```

### Cloud Storage with BACKUP/COPY

BACKUP and COPY commands support cloud URLs:

```cypher
-- Backup to S3
BACKUP TO 's3://backup-bucket/uni-backup-2024'

-- Import from S3
COPY Person FROM 's3://data-bucket/people.parquet'

-- Export to GCS
COPY Person TO 'gs://export-bucket/people.parquet'
```

### Example

```bash
export RUST_LOG=uni_db=info,lance=warn
```

---

## Configuration File (Planned)

Planned support for TOML configuration files (not yet wired into the CLI/server).

### Location

Planned search order:
1. Path specified with `--config`
2. `./uni.toml`
3. `~/.config/uni/config.toml`
4. `/etc/uni/config.toml`

These paths are tentative and may change.

### Format

Configuration file support is planned, but the schema is **TBD**. Do not rely on any specific keys yet.

---

## Schema Configuration

### Schema JSON Format

**Important:** The schema JSON format used by `uni` requires internal metadata fields (`created_at`, `state`, `added_in`) for correct deserialization, even though they are often managed automatically by the system. When manually creating a schema file, you must include these fields.

```json
{
  "schema_version": 1,

  "labels": {
    "Paper": {
      "id": 1,
      "created_at": "2024-01-01T00:00:00Z",
      "state": "Active"
    },
    "Author": {
      "id": 2,
      "created_at": "2024-01-01T00:00:00Z",
      "state": "Active"
    }
  },

  "edge_types": {
    "CITES": {
      "id": 1,
      "src_labels": ["Paper"],
      "dst_labels": ["Paper"],
      "state": "Active"
    },
    "AUTHORED_BY": {
      "id": 2,
      "src_labels": ["Paper"],
      "dst_labels": ["Author"],
      "state": "Active"
    }
  },

  "properties": {
    "Paper": {
      "title": {
        "type": "String",
        "nullable": false,
        "added_in": 1,
        "state": "Active"
      },
      "year": {
        "type": "Int32",
        "nullable": true,
        "added_in": 1,
        "state": "Active"
      },
      "embedding": {
        "type": { "Vector": { "dimensions": 768 } },
        "nullable": true,
        "added_in": 1,
        "state": "Active"
      }
    },
    "Author": {
      "name": {
        "type": "String",
        "nullable": false,
        "added_in": 1,
        "state": "Active"
      }
    }
  },

  "indexes": [
    {
      "type": "Vector",
      "name": "paper_embeddings",
      "label": "Paper",
      "property": "embedding",
      "index_type": {
        "Hnsw": {
          "m": 32,
          "ef_construction": 200,
          "ef_search": 50
        }
      },
      "metric": "Cosine"
    },
    {
      "type": "Scalar",
      "name": "paper_year",
      "label": "Paper",
      "properties": ["year"],
      "index_type": "BTree"
    },
    {
      "type": "Scalar",
      "name": "composite_venue_year",
      "label": "Paper",
      "properties": ["venue", "year"],
      "index_type": "BTree"
    }
  ]
}
```

**Note on Case Sensitivity:**
The JSON schema parser is generally case-sensitive for enum values. Use PascalCase for types (e.g., `Vector`, `Scalar`, `BTree`, `Hnsw`, `Active`) as shown in the example above.

### Data Types

| Type | JSON Name | Description |
|------|-----------|-------------|
| Boolean | `Bool` | true/false |
| 32-bit integer | `Int32` | -2³¹ to 2³¹-1 |
| 64-bit integer | `Int64` | -2⁶³ to 2⁶³-1 |
| 32-bit float | `Float32` | IEEE 754 single |
| 64-bit float | `Float64` | IEEE 754 double |
| String | `String` | UTF-8 text |
| Vector | `Vector` | Float32 array (requires `dimensions`) |
| Timestamp | `Timestamp` | UTC datetime |
| Date | `Date` | Calendar date |
| Time | `Time` | Time of day |
| DateTime | `DateTime` | UTC datetime |
| Duration | `Duration` | Time interval |
| JSON | `Json` | Semi-structured data |
| Point | `Point` | Geographic / Cartesian point |
| CRDT | `Crdt` | Conflict-free replicated data type |
| List | `List` | Homogeneous list |
| Map | `Map` | Key/value map |

---

## Performance Profiles

### High Throughput (Batch Processing)

```rust
use std::time::Duration;

let config = UniConfig {
    // Large L0 buffer for batch writes
    auto_flush_threshold: 100_000,
    auto_flush_interval: None,  // Disable time-based flush during batch
    cache_size: 10 * 1024 * 1024 * 1024,  // 10 GB
    parallelism: num_cpus::get(),
    batch_size: 8192,
    ..Default::default()
};
```

### Low Latency (Interactive)

```rust
use std::time::Duration;

let config = UniConfig {
    // Small L0 for fast flush
    auto_flush_threshold: 1_000,
    auto_flush_interval: Some(Duration::from_secs(1)),  // Flush quickly
    auto_flush_min_mutations: 1,
    cache_size: 5 * 1024 * 1024 * 1024,  // 5 GB
    parallelism: 4,
    batch_size: 2048,
    query_timeout: Duration::from_secs(30),
    ..Default::default()
};
```

### Low-Transaction System

For systems with infrequent writes that still need timely durability:

```rust
use std::time::Duration;

let config = UniConfig {
    auto_flush_threshold: 10_000,
    // Time-based flush ensures data reaches storage
    auto_flush_interval: Some(Duration::from_secs(5)),
    auto_flush_min_mutations: 1,  // Flush even with 1 pending mutation
    ..Default::default()
};
```

### Cloud Storage (Cost Optimized)

For cloud storage backends where minimizing API calls matters:

```rust
use std::time::Duration;
use uni_db::{Uni, UniConfig};
use uni_common::CloudStorageConfig;

let config = UniConfig {
    // Larger batches = fewer PUT operations
    auto_flush_threshold: 50_000,
    auto_flush_interval: Some(Duration::from_secs(30)),
    auto_flush_min_mutations: 100,  // Don't flush for just a few writes
    ..Default::default()
};

let cloud = CloudStorageConfig::s3_from_env("my-bucket");

let db = Uni::open("./local-cache")
    .hybrid("./local-cache", "s3://my-bucket/data")
    .cloud_config(cloud)
    .config(config)
    .build()
    .await?;
```

### Memory Constrained

```rust
use std::time::Duration;

let config = UniConfig {
    // Small caches
    cache_size: 512 * 1024 * 1024,  // 512 MB
    // Flush frequently to keep memory low
    auto_flush_threshold: 1_000,
    auto_flush_interval: Some(Duration::from_secs(2)),
    parallelism: 2,
    batch_size: 1024,
    max_query_memory: 256 * 1024 * 1024,  // 256 MB per query
    ..Default::default()
};
```

---

## Next Steps

- [Rust API Reference](rust-api.md) — Complete API documentation
- [Troubleshooting](troubleshooting.md) — Common issues and solutions
- [Performance Tuning](../guides/performance-tuning.md) — Optimization strategies
