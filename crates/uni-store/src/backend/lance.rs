// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! LanceDB implementation of the [`StorageBackend`] trait.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use dashmap::DashMap;
use futures::{Stream, StreamExt, TryStreamExt};
use lancedb::Table;
use lancedb::connection::Connection;
use lancedb::query::{ExecutableQuery, QueryBase, Select};

use super::lance_branch;
use super::traits::{RecordBatchStream, StorageBackend};
use super::types::*;

/// LanceDB implementation of [`StorageBackend`].
///
/// Wraps a LanceDB [`Connection`] and manages an internal table cache
/// for performance. All Lance-specific code is confined to this module.
pub struct LanceDbBackend {
    connection: Connection,
    base_uri: String,
    /// Legacy table cache. Kept as a field for binary-compatibility with
    /// the existing invalidate_cache / clear_cache trait methods, but
    /// NEVER populated by `get_or_open_table` — see that method's
    /// doc-comment for why we open fresh every time under async-flush.
    /// The remove()/clear() calls in this file are now no-ops.
    #[allow(dead_code)]
    table_cache: DashMap<String, Table>,
    /// Per-table write serialization mutex. Acquired by `write` and
    /// `create_table` around the check-then-create. Without this, two
    /// concurrent async-flush streams that both observe a table as
    /// not-yet-existing can both succeed at `create_table`, and Lance's
    /// CreateTableMode::Create (default) does NOT atomically reject
    /// the second — observed under in-memory backend, where the
    /// second Create writes a new dataset that REPLACES the first,
    /// silently losing the first's batch. Per-table mutex preserves
    /// parallelism across different tables (different labels).
    table_write_locks: DashMap<String, Arc<tokio::sync::Mutex<()>>>,
    /// Existence cache populated lazily by [`Self::table_exists`].
    ///
    /// Avoids paying for [`Connection::table_names`] (which lists every
    /// table in the database) on every `table_exists` call. uni-db's
    /// query planner calls `table_exists` per-table per-query, so without
    /// this cache, post-flush latency scales with total schema size.
    /// Updated synchronously by `create_table`, `create_empty_table`,
    /// `open_or_create_table`, and `drop_table` so the cache is the
    /// authoritative source after first population. See issue #55.
    existence_cache: DashMap<String, bool>,
    /// Schema cache populated lazily by [`Self::get_table_schema`].
    ///
    /// Lance schemas are stable for the table's lifetime under our usage
    /// (we never alter columns in place — schema-evolving migrations would
    /// drop/recreate the table). Caching avoids the per-query
    /// `table.schema().await` round-trip for every Cypher query that
    /// scans a label or edge type. See issue #55.
    schema_cache: DashMap<String, Arc<ArrowSchema>>,
}

// `cache_key` removed alongside the `lancedb::Table` cache (see
// `get_or_open_table` doc comment). The branch cache key form is no
// longer needed because branch reads route through `lance_branch`
// rather than via lancedb's `Table` type.

impl LanceDbBackend {
    /// Connect to a LanceDB database at the given URI.
    pub async fn connect(
        uri: &str,
        storage_options: Option<HashMap<String, String>>,
    ) -> Result<Self> {
        let mut builder = lancedb::connect(uri);
        if let Some(opts) = storage_options {
            builder = builder.storage_options(opts);
        }
        let connection = builder
            .execute()
            .await
            .map_err(|e| anyhow!("Failed to connect to LanceDB at {}: {}", uri, e))?;

        Ok(Self {
            connection,
            base_uri: uri.to_string(),
            table_cache: DashMap::new(),
            table_write_locks: DashMap::new(),
            existence_cache: DashMap::new(),
            schema_cache: DashMap::new(),
        })
    }

    /// Get or insert the per-table write mutex used to serialize
    /// concurrent `write` / `create_table` against the same table.
    /// See `table_write_locks` field doc for context.
    fn write_lock_for(&self, name: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.table_write_locks
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Open a `lancedb::Table` by name (primary branch only).
    ///
    /// Always opens fresh via `connection.open_table()` — does NOT use
    /// a cache. A cached `lancedb::Table` handle is pinned to the
    /// dataset version it was opened at (per `Table::checkout_latest`
    /// docs in lancedb 0.27.1) and does NOT auto-refresh after
    /// subsequent writes. Under async-flush, multiple stream phases
    /// commit concurrent versions; a cached handle's reads then drop
    /// rows committed after the handle was opened. Fix: open fresh
    /// every time so each query sees the latest committed version.
    ///
    /// `connection.open_table()` is a cheap manifest-pointer read in
    /// LanceDB; the previous DashMap cache was a micro-optimization
    /// for sync workloads. Under async-flush it's a correctness
    /// hazard, so we remove it.
    ///
    /// Branch reads still bypass this and route through
    /// [`super::lance_branch`] because lancedb 0.27.1 cannot open a
    /// `Table` on a non-main branch.
    async fn get_or_open_table(&self, name: &str) -> Result<Table> {
        self.connection
            .open_table(name)
            .execute()
            .await
            .map_err(|e| anyhow!("Failed to open table '{}': {}", name, e))
    }

    /// Build the on-disk URI for `name` (used for branch reads via lance).
    fn dataset_uri(&self, name: &str) -> String {
        // lancedb stores tables as `<base_uri>/<name>.lance`.
        if self.base_uri.ends_with('/') {
            format!("{}{}.lance", self.base_uri, name)
        } else {
            format!("{}/{}.lance", self.base_uri, name)
        }
    }

    /// Execute a scan query on the primary branch, returning a lancedb stream.
    async fn execute_primary_scan(
        &self,
        request: &ScanRequest,
    ) -> Result<lancedb::arrow::SendableRecordBatchStream> {
        let table = self.get_or_open_table(&request.table_name).await?;
        let mut query = table.query();

        match &request.columns {
            ColumnProjection::Columns(cols) => {
                query = query.select(Select::Columns(cols.clone()));
            }
            ColumnProjection::All => {}
        }

        match &request.filter {
            FilterExpr::Sql(sql) => {
                query = query.only_if(sql);
            }
            FilterExpr::None => {}
        }

        if let Some(limit) = request.limit {
            query = query.limit(limit);
        }

        query
            .execute()
            .await
            .map_err(|e| anyhow!("Scan failed on '{}': {}", request.table_name, e))
    }

    /// Execute a scan query on a Lance branch via the lower-level lance crate.
    async fn execute_branch_scan(
        &self,
        request: &ScanRequest,
        branch: &str,
    ) -> Result<RecordBatchStream> {
        let uri = self.dataset_uri(&request.table_name);
        let dataset = lance_branch::open_branch(&uri, branch).await?;

        let mut scanner = dataset.scan();

        if let ColumnProjection::Columns(cols) = &request.columns {
            scanner.project(cols).map_err(|e| {
                anyhow!(
                    "Project columns {:?} on '{}@{}': {}",
                    cols,
                    request.table_name,
                    branch,
                    e
                )
            })?;
        }

        if let FilterExpr::Sql(sql) = &request.filter {
            scanner.filter(sql).map_err(|e| {
                anyhow!(
                    "Filter '{}' on '{}@{}': {}",
                    sql,
                    request.table_name,
                    branch,
                    e
                )
            })?;
        }

        if let Some(limit) = request.limit {
            scanner
                .limit(Some(limit as i64), None)
                .map_err(|e| anyhow!("Limit on branched scan failed: {}", e))?;
        }

        let stream = scanner.try_into_stream().await.map_err(|e| {
            anyhow!(
                "Branched scan stream on '{}@{}': {}",
                request.table_name,
                branch,
                e
            )
        })?;

        let mapped: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>> =
            Box::pin(stream.map(|r| r.map_err(|e| anyhow!("{}", e))));
        Ok(mapped)
    }

    /// Run a scan, dispatching to the primary or branch path based on `request.branch`.
    async fn execute_scan_stream(&self, request: &ScanRequest) -> Result<RecordBatchStream> {
        if let Some(branch) = request.branch.clone() {
            return self.execute_branch_scan(request, &branch).await;
        }
        let stream = self.execute_primary_scan(request).await?;
        let mapped: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>> =
            Box::pin(stream.map(|r| r.map_err(|e| anyhow!("{}", e))));
        Ok(mapped)
    }
}

#[async_trait]
impl StorageBackend for LanceDbBackend {
    // ========================
    // Table Lifecycle
    // ========================

    async fn table_names(&self) -> Result<Vec<String>> {
        self.connection
            .table_names()
            .execute()
            .await
            .map_err(|e| anyhow!("Failed to list tables: {}", e))
    }

    async fn table_exists(&self, name: &str) -> Result<bool> {
        if let Some(entry) = self.existence_cache.get(name) {
            return Ok(*entry);
        }
        let tables = self.table_names().await?;
        let exists = tables.iter().any(|t| t == name);
        // entry().or_insert preserves a value written by a concurrent
        // create_table/drop_table during our `table_names` await, which
        // is the authoritative state. Plain `insert` would race and
        // could overwrite a writer's `true` with our stale `false`.
        let final_value = *self
            .existence_cache
            .entry(name.to_string())
            .or_insert(exists);
        Ok(final_value)
    }

    async fn create_table(&self, name: &str, batches: Vec<RecordBatch>) -> Result<()> {
        if batches.is_empty() {
            return Err(anyhow!(
                "Cannot create table '{}' with empty data. Use create_empty_table instead.",
                name
            ));
        }
        // Serialize concurrent create_table / write per-table. Without
        // this, two threads that both observed "table doesn't exist"
        // can both call create_table; CreateTableMode::Create's
        // exists-error is not perfectly atomic on some backends
        // (notably in-memory in lancedb 0.27.1), and the second Create
        // overwrites the first's data. See `table_write_locks` field doc.
        let lock = self.write_lock_for(name);
        let _guard = lock.lock().await;
        // Re-check existence under the lock. If a sibling stream
        // created the table while we were waiting, fall back to Append
        // (calling the inner machinery directly since we already hold
        // the per-table write lock).
        if self.table_exists(name).await? {
            let table = self.get_or_open_table(name).await?;
            table.add(batches).execute().await.map_err(|e| {
                anyhow!(
                    "Failed to append (fallback from create) to '{}': {}",
                    name,
                    e
                )
            })?;
            return Ok(());
        }
        self.connection
            .create_table(name, batches)
            .execute()
            .await
            .map_err(|e| anyhow!("Failed to create table '{}': {}", name, e))?;
        self.existence_cache.insert(name.to_string(), true);
        Ok(())
    }

    async fn create_empty_table(&self, name: &str, schema: Arc<ArrowSchema>) -> Result<()> {
        self.connection
            .create_empty_table(name, schema)
            .execute()
            .await
            .map_err(|e| anyhow!("Failed to create empty table '{}': {}", name, e))?;
        self.existence_cache.insert(name.to_string(), true);
        Ok(())
    }

    async fn open_or_create_table(&self, name: &str, schema: Arc<ArrowSchema>) -> Result<()> {
        if self.table_exists(name).await? {
            // Just verify it can be opened
            self.get_or_open_table(name).await?;
        } else {
            self.create_empty_table(name, schema).await?;
        }
        Ok(())
    }

    async fn drop_table(&self, name: &str) -> Result<()> {
        self.table_cache.remove(name);
        self.schema_cache.remove(name);
        self.connection
            .drop_table(name, &[])
            .await
            .map_err(|e| anyhow!("Failed to drop table '{}': {}", name, e))?;
        self.existence_cache.insert(name.to_string(), false);
        Ok(())
    }

    async fn notify_table_created(&self, name: &str) {
        // BranchedBackend creates fork-side datasets via Lance's branch
        // primitives directly, bypassing this backend's create_table.
        // Without this hook the existence_cache (issue #55) would keep
        // a stale `false` and cause queries to silently see no rows.
        self.existence_cache.insert(name.to_string(), true);
    }

    // ========================
    // Read Operations
    // ========================

    async fn scan(&self, request: ScanRequest) -> Result<Vec<RecordBatch>> {
        let stream = match self.execute_scan_stream(&request).await {
            Ok(s) => s,
            Err(_) => return Ok(vec![]),
        };

        stream
            .try_collect()
            .await
            .map_err(|e| anyhow!("Failed to collect scan results: {}", e))
    }

    async fn scan_stream(&self, request: ScanRequest) -> Result<RecordBatchStream> {
        self.execute_scan_stream(&request).await
    }

    async fn get_table_schema(&self, name: &str) -> Result<Option<Arc<ArrowSchema>>> {
        if let Some(entry) = self.schema_cache.get(name) {
            return Ok(Some(entry.clone()));
        }
        match self.get_or_open_table(name).await {
            Ok(table) => {
                let schema = table
                    .schema()
                    .await
                    .map_err(|e| anyhow!("Failed to get schema for '{}': {}", name, e))?;
                self.schema_cache.insert(name.to_string(), schema.clone());
                Ok(Some(schema))
            }
            Err(_) => Ok(None),
        }
    }

    async fn count_rows(&self, table_name: &str, filter: Option<&str>) -> Result<usize> {
        let table = self.get_or_open_table(table_name).await?;
        table
            .count_rows(filter.map(|s| s.to_string()))
            .await
            .map_err(|e| anyhow!("Failed to count rows in '{}': {}", table_name, e))
    }

    // ========================
    // Write Operations
    // ========================

    async fn write(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
        mode: WriteMode,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        // Serialize per-table writes. Lance's optimistic concurrency on
        // commit is sufficient for parallel Appends in theory, but
        // under async-flush we observed two concurrent Append/Create
        // mixes producing data loss on the in-memory backend. Holding
        // a per-table mutex eliminates that whole class of races at a
        // cost of serializing writes per-table (parallelism preserved
        // across different tables).
        let lock = self.write_lock_for(table_name);
        let _guard = lock.lock().await;

        let table = self.get_or_open_table(table_name).await?;

        match mode {
            WriteMode::Append => {
                table
                    .add(batches)
                    .execute()
                    .await
                    .map_err(|e| anyhow!("Failed to append to '{}': {}", table_name, e))?;
            }
            WriteMode::Overwrite => {
                use lancedb::table::AddDataMode;
                table
                    .add(batches)
                    .mode(AddDataMode::Overwrite)
                    .execute()
                    .await
                    .map_err(|e| anyhow!("Failed to overwrite '{}': {}", table_name, e))?;
            }
        }

        Ok(())
    }

    async fn merge_insert(
        &self,
        table_name: &str,
        on: &[&str],
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        // Serialize per-table writes (same as `write`).
        let lock = self.write_lock_for(table_name);
        let _guard = lock.lock().await;

        let table = self.get_or_open_table(table_name).await?;

        // Build a reader for the partial-column source. The first batch's
        // schema describes the source subschema; Lance compares it against
        // the target via `allow_subschema=true` internally.
        let schema = batches[0].schema();
        let reader = arrow_array::RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            schema,
        );

        let mut mi = table.merge_insert(on);
        mi.when_matched_update_all(None);
        // Note: deliberately NOT calling `when_not_matched_insert_all`.
        // Partial writes only update existing rows; CREATE goes through
        // the full-row Append path. Unmatched source rows are dropped.
        mi.execute(Box::new(reader))
            .await
            .map_err(|e| anyhow!("merge_insert on '{}': {}", table_name, e))?;
        Ok(())
    }

    async fn delete_rows(&self, table_name: &str, filter: &str) -> Result<()> {
        let table = self.get_or_open_table(table_name).await?;
        table
            .delete(filter)
            .await
            .map_err(|e| anyhow!("Failed to delete from '{}': {}", table_name, e))?;
        Ok(())
    }

    async fn replace_table_atomic(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
        schema: Arc<ArrowSchema>,
    ) -> Result<()> {
        // Clean up leftover staging table
        let staging_name = format!("{}_staging", name);
        if self.table_exists(&staging_name).await? {
            self.drop_table(&staging_name).await?;
        }

        if self.table_exists(name).await? {
            let table = self.get_or_open_table(name).await?;
            if batches.is_empty() {
                table
                    .delete("true")
                    .await
                    .map_err(|e| anyhow!("Failed to clear table '{}': {}", name, e))?;
            } else {
                use lancedb::table::AddDataMode;
                table
                    .add(batches)
                    .mode(AddDataMode::Overwrite)
                    .execute()
                    .await
                    .map_err(|e| anyhow!("Failed to overwrite table '{}': {}", name, e))?;
            }
            // Invalidate cache since data changed
            self.table_cache.remove(name);
        } else if batches.is_empty() {
            self.create_empty_table(name, schema).await?;
        } else {
            self.create_table(name, batches).await?;
        }
        Ok(())
    }

    // ========================
    // Versioning / MVCC
    // ========================

    async fn get_table_version(&self, table_name: &str) -> Result<Option<u64>> {
        if !self.table_exists(table_name).await? {
            return Ok(None);
        }
        let table = self.get_or_open_table(table_name).await?;
        let version = table
            .version()
            .await
            .map_err(|e| anyhow!("Failed to get version for '{}': {}", table_name, e))?;
        Ok(Some(version))
    }

    async fn rollback_table(&self, table_name: &str, target_version: u64) -> Result<()> {
        let table = self.get_or_open_table(table_name).await?;
        table.checkout(target_version).await.map_err(|e| {
            anyhow!(
                "Failed to checkout version {} for '{}': {}",
                target_version,
                table_name,
                e
            )
        })?;
        table.restore().await.map_err(|e| {
            anyhow!(
                "Failed to restore '{}' to version {}: {}",
                table_name,
                target_version,
                e
            )
        })?;
        self.table_cache.remove(table_name);
        Ok(())
    }

    // ========================
    // Maintenance
    // ========================

    async fn optimize_table(&self, table_name: &str) -> Result<()> {
        let table = self.get_or_open_table(table_name).await?;
        table
            .optimize(lancedb::table::OptimizeAction::All)
            .await
            .map_err(|e| anyhow!("Failed to optimize '{}': {}", table_name, e))?;
        self.table_cache.remove(table_name);
        Ok(())
    }

    async fn recover_staging(&self, name: &str) -> Result<()> {
        let staging_name = format!("{}_staging", name);

        if !self.table_exists(&staging_name).await? {
            return Ok(());
        }

        let main_exists = self.table_exists(name).await?;

        if main_exists {
            log::info!("Cleaning up leftover staging table: {}", staging_name);
            self.drop_table(&staging_name).await?;
        } else {
            log::warn!("Recovering table '{}' from staging after crash", name);

            let staging_table = self.get_or_open_table(&staging_name).await?;
            let schema = staging_table
                .schema()
                .await
                .map_err(|e| anyhow!("Failed to get staging schema: {}", e))?;

            let stream = staging_table
                .query()
                .execute()
                .await
                .map_err(|e| anyhow!("Failed to query staging: {}", e))?;
            let batches: Vec<RecordBatch> = stream
                .try_collect()
                .await
                .map_err(|e| anyhow!("Failed to collect staging data: {}", e))?;

            if batches.is_empty() {
                self.create_empty_table(name, schema).await?;
            } else {
                self.create_table(name, batches).await?;
            }

            self.drop_table(&staging_name).await?;
            log::info!("Successfully recovered table '{}' from staging", name);
        }

        Ok(())
    }

    // ========================
    // Cache Management
    // ========================

    fn invalidate_cache(&self, table_name: &str) {
        self.table_cache.remove(table_name);
    }

    fn clear_cache(&self) {
        self.table_cache.clear();
    }

    // ========================
    // Metadata
    // ========================

    fn base_uri(&self) -> &str {
        &self.base_uri
    }

    // ========================
    // Capability Checks
    // ========================

    fn supports_vector_search(&self) -> bool {
        true
    }

    fn supports_full_text_search(&self) -> bool {
        true
    }

    fn supports_scalar_index(&self) -> bool {
        true
    }

    // ========================
    // Optional Capabilities
    // ========================

    async fn vector_search(
        &self,
        table: &str,
        column: &str,
        query: &[f32],
        k: usize,
        metric: DistanceMetric,
        filter: FilterExpr,
    ) -> Result<Vec<RecordBatch>> {
        let tbl = self.get_or_open_table(table).await?;

        let distance_type = match metric {
            DistanceMetric::L2 => lancedb::DistanceType::L2,
            DistanceMetric::Cosine => lancedb::DistanceType::Cosine,
            DistanceMetric::Dot => lancedb::DistanceType::Dot,
        };

        let mut query_builder = tbl
            .vector_search(query.to_vec())
            .map_err(|e| anyhow!("Failed to create vector search on '{}': {}", table, e))?
            .column(column)
            .distance_type(distance_type)
            .limit(k);

        if let FilterExpr::Sql(sql) = &filter {
            query_builder = query_builder.only_if(sql);
        }

        query_builder
            .execute()
            .await
            .map_err(|e| anyhow!("Vector search execution failed on '{}': {}", table, e))?
            .try_collect()
            .await
            .map_err(|e| {
                anyhow!(
                    "Failed to collect vector search results from '{}': {}",
                    table,
                    e
                )
            })
    }

    async fn full_text_search(
        &self,
        table: &str,
        column: &str,
        query: &str,
        k: usize,
        filter: FilterExpr,
    ) -> Result<Vec<RecordBatch>> {
        use lance_index::scalar::FullTextSearchQuery;
        use lance_index::scalar::inverted::query::MatchQuery;

        let tbl = self.get_or_open_table(table).await?;

        let match_query = MatchQuery::new(query.to_string()).with_column(Some(column.to_string()));
        let fts_query = FullTextSearchQuery {
            query: match_query.into(),
            limit: Some(k as i64),
            wand_factor: None,
        };

        let mut query_builder = tbl.query().full_text_search(fts_query).limit(k);

        if let FilterExpr::Sql(sql) = &filter {
            query_builder = query_builder.only_if(sql);
        }

        query_builder
            .execute()
            .await
            .map_err(|e| anyhow!("FTS search execution failed on '{}': {}", table, e))?
            .try_collect()
            .await
            .map_err(|e| anyhow!("Failed to collect FTS results from '{}': {}", table, e))
    }

    async fn create_scalar_index(
        &self,
        table: &str,
        column: &str,
        index_type: ScalarIndexType,
    ) -> Result<()> {
        let tbl = self.get_or_open_table(table).await?;
        let lance_idx = match index_type {
            ScalarIndexType::BTree => {
                lancedb::index::Index::BTree(lancedb::index::scalar::BTreeIndexBuilder::default())
            }
            ScalarIndexType::Bitmap => {
                lancedb::index::Index::Bitmap(lancedb::index::scalar::BitmapIndexBuilder::default())
            }
            ScalarIndexType::LabelList => lancedb::index::Index::LabelList(
                lancedb::index::scalar::LabelListIndexBuilder::default(),
            ),
        };
        tbl.create_index(&[column], lance_idx)
            .execute()
            .await
            .map_err(|e| {
                anyhow!(
                    "Failed to create {:?} index on '{}.{}': {}",
                    index_type,
                    table,
                    column,
                    e
                )
            })
    }

    async fn create_fts_index(&self, table: &str, column: &str) -> Result<()> {
        let tbl = self.get_or_open_table(table).await?;
        let fts_params =
            lancedb::index::Index::FTS(lancedb::index::scalar::FtsIndexBuilder::default());
        tbl.create_index(&[column], fts_params)
            .execute()
            .await
            .map_err(|e| {
                anyhow!(
                    "Failed to create FTS index on '{}.{}': {}",
                    table,
                    column,
                    e
                )
            })
    }

    async fn list_indexes(&self, table: &str) -> Result<Vec<IndexInfo>> {
        let tbl = self.get_or_open_table(table).await?;
        let indices = tbl
            .list_indices()
            .await
            .map_err(|e| anyhow!("Failed to list indexes on '{}': {}", table, e))?;

        Ok(indices
            .into_iter()
            .map(|idx| IndexInfo {
                name: idx.name,
                columns: idx.columns.clone(),
                index_type: format!("{:?}", idx.index_type),
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, UInt64Array};
    use arrow_schema::{DataType, Field};
    use tempfile::TempDir;

    async fn create_test_backend() -> (TempDir, LanceDbBackend) {
        let temp_dir = TempDir::new().unwrap();
        let uri = temp_dir.path().to_str().unwrap();
        let backend = LanceDbBackend::connect(uri, None).await.unwrap();
        (temp_dir, backend)
    }

    fn test_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    fn test_batch(ids: Vec<u64>, values: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(UInt64Array::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_table_lifecycle() {
        let (_dir, backend) = create_test_backend().await;

        // Create empty table
        backend
            .create_empty_table("test", test_schema())
            .await
            .unwrap();
        assert!(backend.table_exists("test").await.unwrap());

        let names = backend.table_names().await.unwrap();
        assert!(names.contains(&"test".to_string()));

        // Drop table
        backend.drop_table("test").await.unwrap();
        assert!(!backend.table_exists("test").await.unwrap());
    }

    #[tokio::test]
    async fn test_scan_with_filter() {
        let (_dir, backend) = create_test_backend().await;

        backend
            .create_table("test", vec![test_batch(vec![1, 2, 3], vec![100, 200, 300])])
            .await
            .unwrap();

        // Scan all
        let batches = backend.scan(ScanRequest::all("test")).await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);

        // Scan with filter
        let batches = backend
            .scan(ScanRequest::all("test").with_filter("id > 1"))
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn test_write_append_and_overwrite() {
        let (_dir, backend) = create_test_backend().await;

        backend
            .create_table("test", vec![test_batch(vec![1, 2], vec![100, 200])])
            .await
            .unwrap();
        assert_eq!(backend.count_rows("test", None).await.unwrap(), 2);

        // Append
        backend
            .write(
                "test",
                vec![test_batch(vec![3], vec![300])],
                WriteMode::Append,
            )
            .await
            .unwrap();
        assert_eq!(backend.count_rows("test", None).await.unwrap(), 3);

        // Overwrite
        backend
            .write(
                "test",
                vec![test_batch(vec![10], vec![1000])],
                WriteMode::Overwrite,
            )
            .await
            .unwrap();
        assert_eq!(backend.count_rows("test", None).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_replace_table_atomic() {
        let (_dir, backend) = create_test_backend().await;

        backend
            .create_table("test", vec![test_batch(vec![1, 2, 3], vec![100, 200, 300])])
            .await
            .unwrap();

        // Replace with new data
        backend
            .replace_table_atomic(
                "test",
                vec![test_batch(vec![4, 5], vec![400, 500])],
                test_schema(),
            )
            .await
            .unwrap();
        assert_eq!(backend.count_rows("test", None).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_version_and_rollback() {
        let (_dir, backend) = create_test_backend().await;

        backend
            .create_table("test", vec![test_batch(vec![1], vec![100])])
            .await
            .unwrap();

        let v1 = backend.get_table_version("test").await.unwrap().unwrap();
        assert!(v1 > 0);

        // Append to create a new version
        backend
            .write(
                "test",
                vec![test_batch(vec![2], vec![200])],
                WriteMode::Append,
            )
            .await
            .unwrap();
        assert_eq!(backend.count_rows("test", None).await.unwrap(), 2);

        // Rollback to v1
        backend.rollback_table("test", v1).await.unwrap();
        assert_eq!(backend.count_rows("test", None).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_recover_staging() {
        let (_dir, backend) = create_test_backend().await;

        // No staging table — should be a no-op
        backend.recover_staging("test").await.unwrap();
        assert!(!backend.table_exists("test").await.unwrap());
    }

    #[tokio::test]
    async fn test_get_table_schema() {
        let (_dir, backend) = create_test_backend().await;

        // Non-existent table
        assert!(backend.get_table_schema("missing").await.unwrap().is_none());

        // Create table and check schema
        backend
            .create_empty_table("test", test_schema())
            .await
            .unwrap();
        let schema = backend.get_table_schema("test").await.unwrap().unwrap();
        assert_eq!(schema.fields().len(), 2);
    }

    #[tokio::test]
    async fn test_cache_invalidation() {
        // The `table_cache` was removed for async-flush correctness
        // (see `get_or_open_table` doc comment). `invalidate_cache`
        // and `clear_cache` are still public on the backend trait but
        // are no-ops on `table_cache` now (they retain the legacy
        // signature for callers). This test now just exercises that
        // scan-then-invalidate doesn't error out.
        let (_dir, backend) = create_test_backend().await;

        backend
            .create_table("test", vec![test_batch(vec![1], vec![100])])
            .await
            .unwrap();

        let _ = backend.scan(ScanRequest::all("test")).await.unwrap();
        backend.invalidate_cache("test"); // no-op now, just check it doesn't panic
        let _ = backend.scan(ScanRequest::all("test")).await.unwrap();
        backend.clear_cache();
        let _ = backend.scan(ScanRequest::all("test")).await.unwrap();
    }
}
