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
            request.branch = Some(branch);
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

    /// Local-fs heuristic for "does this dataset URI exist on disk?"
    /// Mirrors `path_exists` in `crate::api::fork`. For non-local stores
    /// we conservatively return `true` so the caller falls back to
    /// `lance_branch::current_version` (which surfaces the right error
    /// if the dataset is actually missing).
    fn dataset_path_exists(uri: &str) -> bool {
        if uri.contains("://") {
            return true;
        }
        std::path::Path::new(uri).exists()
    }

    /// Phase 2 Day 10: ensure a branch exists for `table_name` on the
    /// fork, creating one on-the-fly when the dataset already lives on
    /// primary but wasn't branched at fork-point. Returns the branch
    /// name to write to.
    ///
    /// Errors when `table_name` doesn't exist on primary either —
    /// the caller (typically a write or a delete) needs a
    /// schema-bearing path (`create_table` / `create_empty_table`) to
    /// materialize the dataset.
    async fn ensure_branch_for_existing(&self, table_name: &str) -> Result<String> {
        if let Some(b) = self.scope.branch_for(table_name) {
            return Ok(b);
        }
        let dataset_uri = self.dataset_uri(table_name);
        if !Self::dataset_path_exists(&dataset_uri) {
            anyhow::bail!(
                "ensure_branch_for_existing('{table_name}'): dataset not on \
                 primary either; use create_table/create_empty_table"
            );
        }
        let parent_v = super::lance_branch::current_version(&dataset_uri).await?;
        let branch_name =
            format!("fork_{}_{}", self.scope.fork_id(), table_name);
        super::lance_branch::create_branch(&dataset_uri, &branch_name, parent_v).await?;
        // Persist + record. Persistence first so a crash between the
        // Lance commit and the in-memory register leaves the on-disk
        // record consistent with what reads will resolve.
        self.scope
            .registry()
            .register_dataset_branch(self.scope.fork_id(), table_name, &branch_name)
            .await
            .map_err(|e| anyhow::anyhow!("persist dynamic branch: {e}"))?;
        self.scope
            .register_dynamic_branch(table_name.to_string(), branch_name.clone());
        Ok(branch_name)
    }

    /// Phase 2 Day 10: ensure a branch exists, creating both the
    /// dataset *and* the branch on the fork when neither exists on
    /// primary. Used by `create_table` / `create_empty_table` /
    /// `open_or_create_table`. The dataset is created with `schema`
    /// and (optionally) seeded with `initial_batches`.
    async fn ensure_branch_for_new(
        &self,
        table_name: &str,
        schema: Arc<ArrowSchema>,
        initial_batches: Vec<RecordBatch>,
    ) -> Result<String> {
        if let Some(b) = self.scope.branch_for(table_name) {
            return Ok(b);
        }
        let dataset_uri = self.dataset_uri(table_name);
        let branch_name =
            format!("fork_{}_{}", self.scope.fork_id(), table_name);
        if Self::dataset_path_exists(&dataset_uri) {
            // Dataset exists on primary but no branch yet — branch from
            // the current parent version. Treat the supplied batches
            // (if any) as the first writes on the new branch.
            let parent_v = super::lance_branch::current_version(&dataset_uri).await?;
            super::lance_branch::create_branch(&dataset_uri, &branch_name, parent_v).await?;
            if !initial_batches.is_empty() {
                let arrow_schema = initial_batches[0].schema();
                let reader = arrow_array::RecordBatchIterator::new(
                    initial_batches.into_iter().map(Ok),
                    arrow_schema,
                );
                super::lance_branch::write_to_branch(&dataset_uri, &branch_name, reader)
                    .await?;
            }
        } else {
            // Brand-new dataset — materialize an *empty* parent on
            // main first, branch from it, then write the real batches
            // to the branch. The two-step is critical for fork
            // isolation: writing the batches to main first (the
            // shape `create_dataset_then_branch` does) would leak the
            // fork's data into primary's view of the dataset, since
            // primary's reads always resolve through main.
            //
            // Phase 3 (nested forks): branching off main here is
            // correct even when this scope is a nested fork. By
            // construction `ensure_branch_for_new` only runs when
            // `scope.branch_for(table_name)` returned None, which for
            // a nested child means no ancestor in the chain had a
            // branch for this dataset at the child's creation time.
            // An ancestor's state for a never-touched dataset is empty,
            // so chaining through main vs. through an ancestor's
            // (nonexistent) branch produces the same reads. Primary
            // still cannot see the data because its schema doesn't
            // list the fork-only label — its reads never open this
            // dataset.
            let empty_reader = arrow_array::RecordBatchIterator::new(
                vec![Ok(RecordBatch::new_empty(schema.clone()))].into_iter(),
                schema.clone(),
            );
            super::lance_branch::create_dataset_then_branch(
                &dataset_uri,
                &branch_name,
                empty_reader,
            )
            .await?;
            if !initial_batches.is_empty() {
                let arrow_schema = initial_batches[0].schema();
                let reader = arrow_array::RecordBatchIterator::new(
                    initial_batches.into_iter().map(Ok),
                    arrow_schema,
                );
                super::lance_branch::write_to_branch(
                    &dataset_uri,
                    &branch_name,
                    reader,
                )
                .await?;
            }
        }
        self.scope
            .registry()
            .register_dataset_branch(self.scope.fork_id(), table_name, &branch_name)
            .await
            .map_err(|e| anyhow::anyhow!("persist dynamic branch: {e}"))?;
        self.scope
            .register_dynamic_branch(table_name.to_string(), branch_name.clone());
        Ok(branch_name)
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
        if batches.is_empty() {
            anyhow::bail!(
                "create_table('{name}') on a forked backend requires at least \
                 one batch to derive the schema; use create_empty_table"
            );
        }
        let schema = batches[0].schema();
        // Phase 5a: tally rows for the fork's fragment counter.
        let rows_added: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        self.ensure_branch_for_new(name, schema, batches).await?;
        self.scope.record_fork_fragment(name, rows_added);
        Ok(())
    }

    async fn create_empty_table(&self, name: &str, schema: Arc<ArrowSchema>) -> Result<()> {
        self.ensure_branch_for_new(name, schema, Vec::new()).await?;
        Ok(())
    }

    async fn open_or_create_table(&self, name: &str, schema: Arc<ArrowSchema>) -> Result<()> {
        // Idempotent: if the fork has a branch for this table the
        // dataset already exists on disk, so we're done. Otherwise
        // create dataset+branch (or branch from primary if the
        // dataset already exists on primary) so subsequent writes
        // through the fork's branched backend resolve correctly.
        if self.scope.branch_for(name).is_some() {
            return Ok(());
        }
        self.ensure_branch_for_new(name, schema, Vec::new()).await?;
        Ok(())
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
        // Phase 5a: tally rows up-front so we can bump the fork's
        // fragment counter after a successful write. Computed here
        // (not after the write) because batches are consumed by the
        // RecordBatchIterator below.
        let rows_added: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        // Try to ensure a branch from an existing primary dataset; if
        // primary doesn't have it either, materialize dataset+branch
        // on the fork using the supplied batches as the seed.
        let arrow_schema = batches[0].schema();
        let branch = match self.ensure_branch_for_existing(table_name).await {
            Ok(b) => b,
            Err(_) => {
                // Dataset doesn't exist on primary either — create it
                // on the fork via `ensure_branch_for_new`, seeded with
                // the batches. The branch returned then receives any
                // remaining append/overwrite semantics below.
                let _b = self
                    .ensure_branch_for_new(
                        table_name,
                        arrow_schema.clone(),
                        batches.clone(),
                    )
                    .await?;
                // create_dataset_then_branch already wrote the batches;
                // nothing more to do for Append. For Overwrite, the
                // batches *are* the only content, which matches.
                self.scope.record_fork_fragment(table_name, rows_added);
                return Ok(());
            }
        };
        let uri = self.dataset_uri(table_name);
        let reader = arrow_array::RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            arrow_schema,
        );
        match mode {
            WriteMode::Append => {
                super::lance_branch::write_to_branch(&uri, &branch, reader).await?;
            }
            WriteMode::Overwrite => {
                super::lance_branch::replace_branch_tip(&uri, &branch, reader).await?;
            }
        }
        self.scope.record_fork_fragment(table_name, rows_added);
        Ok(())
    }

    async fn delete_rows(&self, table_name: &str, filter: &str) -> Result<()> {
        let branch = self.ensure_branch_for_existing(table_name).await?;
        let uri = self.dataset_uri(table_name);
        super::lance_branch::delete_from_branch(&uri, &branch, filter).await
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
        // If no branch exists, ensure one — branch from primary when
        // possible, otherwise create dataset+branch with the supplied
        // schema.
        let branch = match self.ensure_branch_for_existing(name).await {
            Ok(b) => b,
            Err(_) => {
                self.ensure_branch_for_new(name, schema.clone(), Vec::new())
                    .await?
            }
        };
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
        super::lance_branch::replace_branch_tip(&uri, &branch, reader).await
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
