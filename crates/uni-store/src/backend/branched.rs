// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Fork-aware backend wrapper.
//!
//! When `StorageManager::at_fork` is called, the resulting fork-scoped
//! manager swaps its `backend` to a [`BranchedBackend`] that wraps the
//! primary backend plus the fork's scope. Every read passes through
//! [`BranchedBackend`], which auto-fills `ScanRequest.branch` from the
//! scope's dataset → branch map. Writes are forbidden in Phase 1 and
//! return [`anyhow::Error`] surfaced as `UniError::ForkWritesNotYetSupported`
//! by the API gate above this layer.

// Rust guideline compliant

use std::sync::Arc;

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;

use super::traits::{RecordBatchStream, StorageBackend};
use super::types::*;
use crate::fork::ForkScope;

/// Backend decorator that routes reads through a fork's branches.
///
/// Owns an `Arc<dyn StorageBackend>` to the primary backend plus an
/// `Arc<ForkScope>` for branch lookups. Cloning is cheap (Arc-only).
pub struct BranchedBackend {
    inner: Arc<dyn StorageBackend>,
    scope: Arc<ForkScope>,
}

impl BranchedBackend {
    /// Wrap `inner` so reads route through `scope`'s branches.
    #[must_use]
    pub fn new(inner: Arc<dyn StorageBackend>, scope: Arc<ForkScope>) -> Self {
        Self { inner, scope }
    }

    /// Borrow the wrapped primary backend.
    ///
    /// Used by Day 4's fork-scoped `Writer` construction: the Writer
    /// needs an `Arc<dyn StorageBackend>` and on a fork that's *this*
    /// backend, but Writer-internal helpers that reach for the
    /// underlying lancedb path (e.g. `connection.create_table`) must
    /// route through the inner backend instead. This accessor makes
    /// the choice explicit.
    #[must_use]
    pub fn inner_backend(&self) -> Arc<dyn StorageBackend> {
        self.inner.clone()
    }

    /// Borrow the active fork scope.
    #[must_use]
    pub fn scope(&self) -> Arc<ForkScope> {
        self.scope.clone()
    }

    /// Apply the fork's branch to a `ScanRequest` if the table has one
    /// recorded in the scope and the request hasn't already set a branch.
    fn apply_branch(&self, mut request: ScanRequest) -> ScanRequest {
        if request.branch.is_none()
            && let Some(branch) = self.scope.branch_for(&request.table_name)
        {
            request.branch = Some(branch.to_string());
        }
        request
    }

    /// Build the on-disk Lance dataset URI for a table name, matching
    /// the convention `lancedb` uses when it stores tables.
    fn dataset_uri(&self, table_name: &str) -> String {
        let base = self.inner.base_uri();
        if base.ends_with('/') {
            format!("{base}{table_name}.lance")
        } else {
            format!("{base}/{table_name}.lance")
        }
    }
}

#[async_trait]
impl StorageBackend for BranchedBackend {
    // ── Reads — branch-aware ─────────────────────────────────────────

    async fn scan(&self, request: ScanRequest) -> Result<Vec<RecordBatch>> {
        self.inner.scan(self.apply_branch(request)).await
    }

    async fn scan_stream(&self, request: ScanRequest) -> Result<RecordBatchStream> {
        self.inner.scan_stream(self.apply_branch(request)).await
    }

    async fn count_rows(&self, table_name: &str, filter: Option<&str>) -> Result<usize> {
        // Primary path counts via `Table::count_rows`. Branched count
        // delegates by scanning the branch and summing row counts; the
        // upstream lancedb 0.27.1 doesn't expose branch-aware count.
        if let Some(_branch) = self.scope.branch_for(table_name) {
            let request = ScanRequest::all(table_name)
                .with_optional_filter(filter)
                .with_branch(_branch);
            let batches = self.inner.scan(request).await?;
            Ok(batches.iter().map(|b| b.num_rows()).sum())
        } else {
            self.inner.count_rows(table_name, filter).await
        }
    }

    async fn get_table_schema(&self, name: &str) -> Result<Option<Arc<ArrowSchema>>> {
        // Schema is identical across branches — the schema is captured
        // at fork creation and overlays only add new columns. Delegate
        // to the primary backend.
        self.inner.get_table_schema(name).await
    }

    async fn vector_search(
        &self,
        table: &str,
        column: &str,
        query: &[f32],
        k: usize,
        metric: DistanceMetric,
        filter: FilterExpr,
    ) -> Result<Vec<RecordBatch>> {
        // Phase 5 will route vector search through fork-local index
        // fusion. Phase 1 falls back to primary's vector search even on
        // a forked session — the result is correct (parent-inherited
        // vector index covers fork-point data) but doesn't fuse with
        // any fork-local writes. Phase 2/5 will revisit.
        self.inner
            .vector_search(table, column, query, k, metric, filter)
            .await
    }

    async fn full_text_search(
        &self,
        table: &str,
        column: &str,
        query: &str,
        k: usize,
        filter: FilterExpr,
    ) -> Result<Vec<RecordBatch>> {
        self.inner
            .full_text_search(table, column, query, k, filter)
            .await
    }

    // ── Lifecycle / writes — Phase 2 routes to the fork's branches ──
    //
    // Phase 1 was bail-on-every-write. Phase 2 routes through
    // `crate::backend::lance_branch` helpers when the fork has a branch
    // for the named table; falls back to a `ForkLifecycle` error when
    // it doesn't (Phase 2 Day 10 lifts this with on-the-fly branch
    // creation for new labels).

    async fn table_names(&self) -> Result<Vec<String>> {
        self.inner.table_names().await
    }

    async fn table_exists(&self, name: &str) -> Result<bool> {
        self.inner.table_exists(name).await
    }

    async fn create_table(&self, name: &str, batches: Vec<RecordBatch>) -> Result<()> {
        // On-the-fly branch creation for a fork-only label lands in
        // Day 10. Day 2's surface: error with the named stage so the
        // caller can see the gap and the test confirms we don't
        // silently fall through to primary.
        let _ = batches;
        anyhow::bail!(
            "create_table('{name}') on a forked backend requires on-the-fly \
             schema overlay growth (Phase 2 Day 10); not implemented yet"
        )
    }

    async fn create_empty_table(&self, name: &str, schema: Arc<ArrowSchema>) -> Result<()> {
        let _ = schema;
        anyhow::bail!(
            "create_empty_table('{name}') on a forked backend requires on-the-fly \
             schema overlay growth (Phase 2 Day 10); not implemented yet"
        )
    }

    async fn open_or_create_table(&self, name: &str, schema: Arc<ArrowSchema>) -> Result<()> {
        // Idempotent: if the fork has a branch for this table the
        // dataset already exists on disk, so we're done. If not, we
        // refuse rather than create the dataset on primary — which
        // would leak schema into primary's namespace. Day 10 lifts the
        // refuse path into on-the-fly creation.
        if self.scope.branch_for(name).is_some() {
            return Ok(());
        }
        let _ = schema;
        anyhow::bail!(
            "open_or_create_table('{name}') on a forked backend with no \
             branch for the table; on-the-fly creation is Day 10 work"
        )
    }

    async fn drop_table(&self, name: &str) -> Result<()> {
        // Forks do not drop primary tables. The right Phase 6 verb for
        // fork-side drop is `db.drop_fork(...)`; per-table drop on a
        // fork has no spec story.
        anyhow::bail!(
            "drop_table('{name}') on a forked backend is not supported; \
             use db.drop_fork(...) to remove a fork in its entirety"
        )
    }

    async fn write(
        &self,
        table_name: &str,
        batches: Vec<RecordBatch>,
        mode: WriteMode,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        let branch = self.scope.branch_for(table_name).ok_or_else(|| {
            anyhow::anyhow!(
                "write('{table_name}') on a forked backend with no branch \
                 for the table; on-the-fly creation is Day 10 work"
            )
        })?;
        let uri = self.dataset_uri(table_name);
        let arrow_schema = batches[0].schema();
        let reader = arrow_array::RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            arrow_schema,
        );
        match mode {
            WriteMode::Append => {
                super::lance_branch::write_to_branch(&uri, branch, reader).await
            }
            WriteMode::Overwrite => {
                super::lance_branch::replace_branch_tip(&uri, branch, reader).await
            }
        }
    }

    async fn delete_rows(&self, table_name: &str, filter: &str) -> Result<()> {
        let branch = self.scope.branch_for(table_name).ok_or_else(|| {
            anyhow::anyhow!(
                "delete_rows('{table_name}') on a forked backend with no \
                 branch for the table; on-the-fly creation is Day 10 work"
            )
        })?;
        let uri = self.dataset_uri(table_name);
        super::lance_branch::delete_from_branch(&uri, branch, filter).await
    }

    async fn replace_table_atomic(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
        schema: Arc<ArrowSchema>,
    ) -> Result<()> {
        // On a fork, "replace the table atomically" means "replace the
        // branch's tip" — Lance commits a delete-all then an append.
        // Two manifest commits, not one; primary's main branch is
        // untouched. Spec contract differs from primary semantics, so
        // callers should be aware (commented at Phase 2 Decision D3).
        let branch = self.scope.branch_for(name).ok_or_else(|| {
            anyhow::anyhow!(
                "replace_table_atomic('{name}') on a forked backend with no \
                 branch for the table; on-the-fly creation is Day 10 work"
            )
        })?;
        let uri = self.dataset_uri(name);
        // Homogenize the iterator type by always going through a Vec.
        let (rows, arrow_schema) = if batches.is_empty() {
            (
                vec![Ok(RecordBatch::new_empty(schema.clone()))],
                schema,
            )
        } else {
            let s = batches[0].schema();
            (batches.into_iter().map(Ok).collect::<Vec<_>>(), s)
        };
        let reader =
            arrow_array::RecordBatchIterator::new(rows.into_iter(), arrow_schema);
        super::lance_branch::replace_branch_tip(&uri, branch, reader).await
    }

    // ── MVCC ─────────────────────────────────────────────────────────

    async fn get_table_version(&self, table_name: &str) -> Result<Option<u64>> {
        self.inner.get_table_version(table_name).await
    }

    async fn rollback_table(&self, _table_name: &str, _target_version: u64) -> Result<()> {
        anyhow::bail!(
            "rollback_table on a forked backend is not supported in Phase 1"
        )
    }

    // ── Maintenance ──────────────────────────────────────────────────

    async fn optimize_table(&self, table_name: &str) -> Result<()> {
        // Compaction on a fork is a Phase 5 concern. Phase 1 silently
        // delegates to the primary backend; for a fork-only table this
        // is a no-op because the fork has no L1 fragments yet.
        self.inner.optimize_table(table_name).await
    }

    async fn recover_staging(&self, table_name: &str) -> Result<()> {
        self.inner.recover_staging(table_name).await
    }

    // ── Cache passthrough ────────────────────────────────────────────

    fn invalidate_cache(&self, table_name: &str) {
        self.inner.invalidate_cache(table_name);
    }

    fn clear_cache(&self) {
        self.inner.clear_cache();
    }

    fn base_uri(&self) -> &str {
        self.inner.base_uri()
    }

    // ── Capability flags — same as inner ────────────────────────────

    fn supports_vector_search(&self) -> bool {
        self.inner.supports_vector_search()
    }

    fn supports_full_text_search(&self) -> bool {
        self.inner.supports_full_text_search()
    }

    fn supports_scalar_index(&self) -> bool {
        self.inner.supports_scalar_index()
    }

    // ── Index management — Phase 5 will revisit ─────────────────────

    async fn create_vector_index(
        &self,
        _table: &str,
        _column: &str,
        _config: VectorIndexConfig,
    ) -> Result<()> {
        anyhow::bail!(
            "create_vector_index on a forked backend is not supported in Phase 1"
        )
    }

    async fn create_fts_index(&self, _table: &str, _column: &str) -> Result<()> {
        anyhow::bail!("create_fts_index on a forked backend is not supported in Phase 1")
    }

    async fn create_scalar_index(
        &self,
        _table: &str,
        _column: &str,
        _index_type: ScalarIndexType,
    ) -> Result<()> {
        anyhow::bail!(
            "create_scalar_index on a forked backend is not supported in Phase 1"
        )
    }

    async fn drop_index(&self, _table: &str, _index_name: &str) -> Result<()> {
        anyhow::bail!("drop_index on a forked backend is not supported in Phase 1")
    }

    async fn list_indexes(&self, table: &str) -> Result<Vec<IndexInfo>> {
        self.inner.list_indexes(table).await
    }
}
