// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Core [`StorageBackend`] trait definition.

use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use futures::Stream;

use super::types::*;

/// A record batch stream returned by [`StorageBackend::scan_stream`].
pub type RecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;

/// Core storage backend trait.
///
/// All persistent storage operations go through this trait. Backends must be
/// thread-safe ([`Send`] + [`Sync`]) and have a static lifetime for use with
/// `Arc<dyn StorageBackend>`.
///
/// # Design Principles
///
/// - **Arrow-native**: All data interchange uses Arrow [`RecordBatch`].
/// - **SQL-string filters**: Filter expressions use SQL-like strings initially.
///   Backends that don't support SQL must parse/translate these strings.
/// - **Capabilities via default methods**: Optional features (vector search, FTS)
///   have default implementations that return "not supported" errors.
/// - **Table-level operations**: The backend manages individual tables (not the
///   higher-level graph schema). Table naming conventions are in [`super::table_names`].
#[async_trait]
pub trait StorageBackend: Send + Sync + 'static {
    // ========================
    // Table Lifecycle
    // ========================

    /// List all table names in the backend.
    async fn table_names(&self) -> Result<Vec<String>>;

    /// Check if a table exists.
    async fn table_exists(&self, name: &str) -> Result<bool>;

    /// Create a new table with initial data batches.
    async fn create_table(&self, name: &str, batches: Vec<RecordBatch>) -> Result<()>;

    /// Create a new empty table with the given schema.
    async fn create_empty_table(&self, name: &str, schema: Arc<ArrowSchema>) -> Result<()>;

    /// Open a table if it exists, or create it with the given schema.
    async fn open_or_create_table(&self, name: &str, schema: Arc<ArrowSchema>) -> Result<()>;

    /// Drop a table by name.
    async fn drop_table(&self, name: &str) -> Result<()>;

    // ========================
    // Read Operations
    // ========================

    /// Scan a table, collecting all matching rows into batches.
    async fn scan(&self, request: ScanRequest) -> Result<Vec<RecordBatch>>;

    /// Scan a table, returning a streaming iterator over record batches.
    async fn scan_stream(&self, request: ScanRequest) -> Result<RecordBatchStream>;

    /// Get the Arrow schema for a table. Returns `None` if the table doesn't exist.
    async fn get_table_schema(&self, name: &str) -> Result<Option<Arc<ArrowSchema>>>;

    /// Count rows in a table, optionally with a filter.
    async fn count_rows(&self, table_name: &str, filter: Option<&str>) -> Result<usize>;

    // ========================
    // Write Operations
    // ========================

    /// Write record batches to a table.
    async fn write(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
        mode: WriteMode,
    ) -> Result<()>;

    /// Delete rows matching a filter expression.
    async fn delete_rows(&self, table_name: &str, filter: &str) -> Result<()>;

    /// Atomically replace a table's contents.
    ///
    /// Handles the case where batches may be empty (clears the table) and the
    /// table may not exist yet (creates it).
    async fn replace_table_atomic(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
        schema: Arc<ArrowSchema>,
    ) -> Result<()>;

    // ========================
    // Versioning / MVCC
    // ========================

    /// Get the current version of a table. Returns `None` if the table doesn't exist.
    async fn get_table_version(&self, table_name: &str) -> Result<Option<u64>>;

    /// Roll back a table to a specific version.
    async fn rollback_table(&self, table_name: &str, target_version: u64) -> Result<()>;

    // ========================
    // Maintenance
    // ========================

    /// Optimize a table (compaction, cleanup, etc.).
    async fn optimize_table(&self, table_name: &str) -> Result<()>;

    /// Recover a table from crash state (incomplete staging writes, etc.).
    async fn recover_staging(&self, table_name: &str) -> Result<()>;

    // ========================
    // Cache Management
    // ========================

    /// Invalidate any cached state for a table.
    fn invalidate_cache(&self, _table_name: &str) {}

    /// Clear all cached state.
    fn clear_cache(&self) {}

    // ========================
    // Metadata
    // ========================

    /// Get the base URI for this backend's storage location.
    fn base_uri(&self) -> &str;

    // ========================
    // Capability Checks
    // ========================

    /// Whether this backend supports vector similarity search.
    fn supports_vector_search(&self) -> bool {
        false
    }

    /// Whether this backend supports full-text search.
    fn supports_full_text_search(&self) -> bool {
        false
    }

    /// Whether this backend supports scalar indexes.
    fn supports_scalar_index(&self) -> bool {
        false
    }

    // ========================
    // Optional Capabilities
    // ========================

    /// Perform a vector similarity search.
    async fn vector_search(
        &self,
        _table: &str,
        _column: &str,
        _query: &[f32],
        _k: usize,
        _metric: DistanceMetric,
        _filter: FilterExpr,
    ) -> Result<Vec<RecordBatch>> {
        anyhow::bail!("Vector search not supported by this backend")
    }

    /// Perform a full-text search.
    async fn full_text_search(
        &self,
        _table: &str,
        _column: &str,
        _query: &str,
        _k: usize,
        _filter: FilterExpr,
    ) -> Result<Vec<RecordBatch>> {
        anyhow::bail!("Full-text search not supported by this backend")
    }

    /// Create a vector index on a column.
    async fn create_vector_index(
        &self,
        _table: &str,
        _column: &str,
        _config: VectorIndexConfig,
    ) -> Result<()> {
        anyhow::bail!("Vector indexing not supported by this backend")
    }

    /// Create a full-text search index on a column.
    async fn create_fts_index(&self, _table: &str, _column: &str) -> Result<()> {
        anyhow::bail!("FTS indexing not supported by this backend")
    }

    /// Create a scalar index on a column.
    async fn create_scalar_index(
        &self,
        _table: &str,
        _column: &str,
        _index_type: ScalarIndexType,
    ) -> Result<()> {
        anyhow::bail!("Scalar indexing not supported by this backend")
    }

    /// Drop an index by name.
    async fn drop_index(&self, _table: &str, _index_name: &str) -> Result<()> {
        anyhow::bail!("Index drop not supported by this backend")
    }

    /// List all indexes on a table.
    async fn list_indexes(&self, _table: &str) -> Result<Vec<IndexInfo>> {
        Ok(vec![])
    }
}
