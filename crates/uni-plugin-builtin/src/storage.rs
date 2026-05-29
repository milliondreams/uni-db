//! Built-in storage backend registrations.
//!
//! M5a cutover (2026-05-24): the plugin `Storage` / `StorageBackend`
//! traits are async. `MemoryBackend` is a real in-memory implementation;
//! `LanceBackend` is wired to `uni-store::LanceDbBackend` behind the
//! `lance-backend` cargo feature.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;
use parking_lot::RwLock;
use uni_plugin::traits::storage::{
    BranchMetadata, Storage, StorageBackend, StorageOptions, WriteHandle,
};
use uni_plugin::{FnError, PluginError, PluginRegistrar};

/// Register the built-in storage-backend implementations.
///
/// Registers `memory://` (always) and `lance://` (when the
/// `lance-backend` feature is enabled).
///
/// # Errors
///
/// Returns [`PluginError`] on duplicate scheme registration.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    r.storage_backend("memory", Arc::new(MemoryBackend))?;
    #[cfg(feature = "lance-backend")]
    r.storage_backend("lance", Arc::new(LanceBackend))?;
    Ok(())
}

/// Evaluate `predicate` against every row of `batches` and return only
/// the rows that survive.
///
/// Uses DataFusion's [`DefaultPhysicalPlanner::create_physical_expr`]
/// to lower the logical [`Expr`] against the table schema, then
/// invokes `arrow::compute::filter_record_batch` per batch. Failures
/// in lowering (custom UDFs the planner cannot resolve, etc.) bubble
/// up as [`FnError`] `0x711` — the wire-format the plugin Storage
/// contract reserves for "predicate not encodable by this backend",
/// so callers fall back to a DataFusion `Filter` above the unfiltered
/// scan.
///
/// # Errors
///
/// Returns [`FnError`] code `0x711` for unencodable predicates, or
/// `0x713` for runtime evaluation failures (schema mismatch, type
/// errors at evaluation time).
fn apply_predicate_in_memory(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    predicate: &Expr,
) -> Result<Vec<RecordBatch>, FnError> {
    use datafusion::arrow::compute::filter_record_batch;
    use datafusion::common::DFSchema;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::create_physical_expr as build_physical_expr;
    use std::sync::Arc as StdArc;

    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let df_schema = DFSchema::try_from(schema.as_ref().clone()).map_err(|e| {
        FnError::new(
            0x711,
            format!("memory backend: schema lowering failed: {e}"),
        )
    })?;
    let exec_props = datafusion::execution::context::ExecutionProps::new();
    let physical: StdArc<dyn PhysicalExpr> =
        build_physical_expr(predicate, &df_schema, &exec_props).map_err(|e| {
            FnError::new(
                0x711,
                format!("memory backend: predicate not encodable (physical lowering failed): {e}"),
            )
        })?;

    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        let evaluated = physical.evaluate(batch).map_err(|e| {
            FnError::new(0x713, format!("memory backend: predicate eval failed: {e}"))
        })?;
        let arr = evaluated.into_array(batch.num_rows()).map_err(|e| {
            FnError::new(
                0x713,
                format!("memory backend: predicate result-to-array failed: {e}"),
            )
        })?;
        let mask = arr
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .ok_or_else(|| {
                FnError::new(
                    0x713,
                    "memory backend: predicate did not produce BooleanArray".to_owned(),
                )
            })?;
        let kept = filter_record_batch(batch, mask).map_err(|e| {
            FnError::new(
                0x713,
                format!("memory backend: filter_record_batch failed: {e}"),
            )
        })?;
        if kept.num_rows() > 0 {
            out.push(kept);
        }
    }
    Ok(out)
}

/// In-memory storage backend — useful for tests and the conformance suite.
#[derive(Debug)]
pub struct MemoryBackend;

#[async_trait]
impl StorageBackend for MemoryBackend {
    fn scheme(&self) -> &'static str {
        "memory"
    }
    async fn open(
        &self,
        _uri: &str,
        _options: &StorageOptions,
    ) -> Result<Arc<dyn Storage>, FnError> {
        Ok(Arc::new(MemoryStorage::new()))
    }
}

/// In-memory `Storage` — keeps every batch per table in a `Vec`.
///
/// Real implementation: write appends a batch, read concatenates and
/// streams. Filter pushdown is honored on a best-effort basis: when a
/// predicate is supplied, [`MemoryStorage::read_batch`] evaluates it
/// against each batch via the standard DataFusion physical-expr
/// planner and returns the surviving rows. Expressions the planner
/// cannot lower (custom UDFs missing from the planner state, etc.)
/// surface as [`FnError`] `0x711` so callers can fall back to a
/// DataFusion-side `FilterExec` above an unfiltered scan — matching
/// the contract documented on [`Storage::read_batch`]. Schema is
/// derived from the first written batch.
#[derive(Debug)]
pub struct MemoryStorage {
    tables: RwLock<HashMap<String, Vec<RecordBatch>>>,
    next_handle: std::sync::atomic::AtomicU64,
}

impl MemoryStorage {
    /// Construct a fresh, empty in-memory store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            next_handle: std::sync::atomic::AtomicU64::new(1),
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    async fn read_batch(
        &self,
        table: &str,
        predicate: Option<&Expr>,
    ) -> Result<SendableRecordBatchStream, FnError> {
        let tables = self.tables.read();
        let batches = tables.get(table).cloned().unwrap_or_default();
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()));

        // Apply predicate at scan time. The plugin Storage contract says
        // a backend that cannot encode the predicate must surface
        // `FnError 0x711`; the in-memory backend trivially supports every
        // predicate DataFusion can lower against the table's schema, so
        // we route through the standard physical-expr planner and
        // surface `0x711` only for the lowering-failed branch.
        let filtered = match predicate {
            None => batches,
            Some(expr) => apply_predicate_in_memory(&schema, &batches, expr)?,
        };

        let owned_batches: Vec<datafusion::common::Result<RecordBatch>> =
            filtered.into_iter().map(Ok).collect();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(owned_batches),
        )))
    }
    async fn write_batch(&self, table: &str, batch: &RecordBatch) -> Result<WriteHandle, FnError> {
        self.tables
            .write()
            .entry(table.to_owned())
            .or_default()
            .push(batch.clone());
        let id = self
            .next_handle
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(WriteHandle { id })
    }
    async fn list_tables(&self) -> Result<Vec<String>, FnError> {
        Ok(self.tables.read().keys().cloned().collect())
    }
    async fn delete(&self, table: &str, _predicate: &Expr) -> Result<u64, FnError> {
        // M5a in-memory backend: predicate-less delete clears the table.
        // Real predicate evaluation will arrive when the plugin Storage
        // trait grows a predicate-encoding contract (deferred follow-up).
        let mut tables = self.tables.write();
        let count = tables
            .get(table)
            .map(|b| b.iter().map(|x| x.num_rows() as u64).sum::<u64>())
            .unwrap_or(0);
        tables.remove(table);
        Ok(count)
    }
    async fn schema(&self, table: &str) -> Option<SchemaRef> {
        self.tables
            .read()
            .get(table)
            .and_then(|v| v.first().map(|b| b.schema()))
    }
}

// ============================================================================
// Lance backend — bridges uni-store's `StorageBackend` (richer, async) onto
// the plugin's 6-method `Storage` shape. Predicates flow through
// `datafusion::sql::unparser::expr_to_sql` to produce a Lance-compatible
// SQL filter string; unencodable shapes surface as `FnError 0x711` so
// callers can fall back to a DataFusion `Filter` operator above the
// unfiltered scan. Always-true literal deletes short-circuit to
// `replace_table_atomic` for atomic truncation.
// ============================================================================

/// Lance storage backend — opens a `LanceDbBackend` per `lance://` URI.
#[cfg(feature = "lance-backend")]
#[derive(Debug)]
pub struct LanceBackend;

#[cfg(feature = "lance-backend")]
#[async_trait]
impl StorageBackend for LanceBackend {
    fn scheme(&self) -> &'static str {
        "lance"
    }
    async fn open(
        &self,
        uri: &str,
        _options: &StorageOptions,
    ) -> Result<Arc<dyn Storage>, FnError> {
        let backend = uni_store::LanceDbBackend::connect(uri, None)
            .await
            .map_err(|e| FnError::new(0x702, format!("lance connect failed: {e}")))?;
        Ok(Arc::new(LancePluginStorage {
            inner: Arc::new(backend),
        }))
    }
}

/// Plugin-side adapter wrapping a [`uni_store::LanceDbBackend`].
#[cfg(feature = "lance-backend")]
pub struct LancePluginStorage {
    inner: Arc<uni_store::LanceDbBackend>,
}

#[cfg(feature = "lance-backend")]
impl std::fmt::Debug for LancePluginStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `LanceDbBackend` does not implement Debug; surface a stable
        // identifier for tracing/diagnostics.
        f.debug_struct("LancePluginStorage").finish_non_exhaustive()
    }
}

#[cfg(feature = "lance-backend")]
#[async_trait]
impl Storage for LancePluginStorage {
    async fn read_batch(
        &self,
        table: &str,
        predicate: Option<&Expr>,
    ) -> Result<SendableRecordBatchStream, FnError> {
        use uni_store::backend::traits::StorageBackend as _;
        use uni_store::backend::types::ScanRequest;
        let mut req = ScanRequest::all(table);
        if let Some(expr) = predicate {
            let filter = expr_to_lance_filter(expr)?;
            req = req.with_filter(filter);
        }
        let stream = self
            .inner
            .scan_stream(req)
            .await
            .map_err(|e| FnError::new(0x703, format!("lance scan_stream failed: {e}")))?;
        let schema = self
            .inner
            .get_table_schema(table)
            .await
            .map_err(|e| FnError::new(0x704, format!("lance get_table_schema failed: {e}")))?
            .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()));
        let mapped = futures::StreamExt::map(stream, |r| {
            r.map_err(|e| datafusion::common::DataFusionError::External(e.into()))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, mapped)))
    }

    async fn write_batch(&self, table: &str, batch: &RecordBatch) -> Result<WriteHandle, FnError> {
        use uni_store::backend::traits::StorageBackend as _;
        use uni_store::backend::types::WriteMode;
        let exists = self
            .inner
            .table_exists(table)
            .await
            .map_err(|e| FnError::new(0x705, format!("lance table_exists failed: {e}")))?;
        if exists {
            self.inner
                .write(table, vec![batch.clone()], WriteMode::Append)
                .await
                .map_err(|e| FnError::new(0x706, format!("lance write failed: {e}")))?;
        } else {
            self.inner
                .create_table(table, vec![batch.clone()])
                .await
                .map_err(|e| FnError::new(0x707, format!("lance create_table failed: {e}")))?;
        }
        // Surface the post-write table version as the opaque WriteHandle
        // id (M5a follow-up #4). `get_table_version` returns the snapshot
        // version of the just-committed write; on `None` (table missing
        // version metadata) fall back to 0 so the trait contract holds.
        let id = self
            .inner
            .get_table_version(table)
            .await
            .map_err(|e| FnError::new(0x709, format!("lance get_table_version failed: {e}")))?
            .unwrap_or(0);
        Ok(WriteHandle { id })
    }

    async fn list_tables(&self) -> Result<Vec<String>, FnError> {
        use uni_store::backend::traits::StorageBackend as _;
        self.inner
            .table_names()
            .await
            .map_err(|e| FnError::new(0x708, format!("lance table_names failed: {e}")))
    }

    async fn delete(&self, table: &str, predicate: &Expr) -> Result<u64, FnError> {
        use uni_store::backend::traits::StorageBackend as _;

        // Always-true literal → atomic truncation via replace_table_atomic
        // with an empty batch list. This is the M5a follow-up #2 fast-path.
        if matches!(
            predicate,
            Expr::Literal(datafusion::scalar::ScalarValue::Boolean(Some(true)), _)
        ) {
            let schema = self
                .inner
                .get_table_schema(table)
                .await
                .map_err(|e| FnError::new(0x70a, format!("lance get_table_schema failed: {e}")))?
                .ok_or_else(|| {
                    FnError::new(0x70b, format!("lance delete: table `{table}` not found"))
                })?;
            let count = self
                .inner
                .count_rows(table, None)
                .await
                .map_err(|e| FnError::new(0x70c, format!("lance count_rows failed: {e}")))?
                as u64;
            self.inner
                .replace_table_atomic(table, Vec::new(), schema)
                .await
                .map_err(|e| {
                    FnError::new(0x70d, format!("lance replace_table_atomic failed: {e}"))
                })?;
            return Ok(count);
        }

        // General predicate path: encode → count → delete.
        let filter = expr_to_lance_filter(predicate)?;
        let count = self
            .inner
            .count_rows(table, Some(&filter))
            .await
            .map_err(|e| FnError::new(0x70e, format!("lance count_rows failed: {e}")))?
            as u64;
        self.inner
            .delete_rows(table, &filter)
            .await
            .map_err(|e| FnError::new(0x70f, format!("lance delete_rows failed: {e}")))?;
        Ok(count)
    }

    async fn schema(&self, table: &str) -> Option<SchemaRef> {
        use uni_store::backend::traits::StorageBackend as _;
        self.inner.get_table_schema(table).await.ok().flatten()
    }

    fn supports_branching(&self) -> bool {
        true
    }

    async fn fork(
        &self,
        table: &str,
        src_branch: &str,
        dst_branch: &str,
    ) -> Result<BranchMetadata, FnError> {
        let (parent_version, branch_name) = self
            .inner
            .fork_branch(table, src_branch, dst_branch)
            .await
            .map_err(|e| FnError::new(0x712, format!("lance fork_branch failed: {e}")))?;
        Ok(BranchMetadata {
            parent_version,
            branch_name,
        })
    }
}

/// Encode a DataFusion [`Expr`] as a Lance-compatible SQL filter string.
///
/// Delegates to `datafusion::sql::unparser::expr_to_sql`, which renders the
/// expression as a [`sqlparser::ast::Expr`]; `Display` produces the textual
/// SQL Lance's `filter` parameter accepts.
///
/// # Errors
///
/// Returns [`FnError`] with code `0x711` when the expression contains a
/// shape the unparser can't render (custom UDFs, certain extension types,
/// etc.). Callers should fall back to a DataFusion `Filter` operator
/// above the unfiltered scan in that case.
#[cfg(feature = "lance-backend")]
fn expr_to_lance_filter(expr: &Expr) -> Result<String, FnError> {
    match datafusion::sql::unparser::expr_to_sql(expr) {
        Ok(sql_expr) => Ok(sql_expr.to_string()),
        Err(e) => Err(FnError::new(
            0x711,
            format!(
                "lance plugin adapter: predicate not encodable to Lance SQL \
                 (datafusion unparser: {e}); wrap in DataFusion Filter above"
            ),
        )),
    }
}

// ============================================================================
// M5h — Pushdown marker for Lance-backed table providers.
//
// `LanceFilterPushdown` is a public [`SupportsFilterPushdown`] impl that
// classifies filters via [`expr_to_lance_filter`]: predicates the
// DataFusion unparser can render to Lance-compatible SQL land in
// `fully_handled`; predicates that fail to render are omitted (left for
// a DataFusion `Filter` operator above the scan to enforce).
//
// Wrap it through [`super::PushdownAwareTable::with_filter`] when
// constructing a Lance-backed `TableProvider` so the optimizer's
// `PushdownNegotiationRule` can elide the redundant `Filter`:
//
// ```ignore
// let provider = PushdownAwareTable::with_filter(
//     my_lance_table_provider,
//     Arc::new(LanceFilterPushdown),
// );
// ctx.register_table("t", Arc::new(provider))?;
// ```
//
// Note: M5h does NOT today auto-wrap built-in `TableProvider`s — the
// plugin `Storage` layer (`Storage::read_batch`) is separate from
// DataFusion's `TableProvider`, and no built-in bridges between them
// (uni-query has its own physical execution path for graph rows). This
// marker is therefore the *public* pushdown primitive users / extension
// crates plug into when they construct their own Lance-backed
// `TableProvider`s; building the in-tree bridge is its own scope.
// ============================================================================

/// `SupportsFilterPushdown` marker for Lance-backed `TableProvider`s.
///
/// Delegates to `expr_to_lance_filter` (which uses
/// `datafusion::sql::unparser::expr_to_sql`) to determine which filters
/// Lance can encode in its `ScanRequest::with_filter`. Encodable filters
/// are reported as `fully_handled`; unencodable filters are omitted so
/// the optimizer keeps a verifying `Filter` operator above the scan.
///
/// Stateless and `Sync` — wrap it in `Arc` once and reuse across all
/// Lance-backed `TableProvider`s.
#[cfg(feature = "lance-backend")]
#[derive(Debug, Default)]
pub struct LanceFilterPushdown;

#[cfg(feature = "lance-backend")]
impl uni_plugin::traits::pushdown::SupportsFilterPushdown for LanceFilterPushdown {
    fn push_filters(&self, filters: &[Expr]) -> uni_plugin::traits::pushdown::FilterApplication {
        let mut fully_handled = Vec::new();
        for (idx, expr) in filters.iter().enumerate() {
            if expr_to_lance_filter(expr).is_ok() {
                fully_handled.push(idx);
            }
        }
        uni_plugin::traits::pushdown::FilterApplication {
            fully_handled,
            partially_handled: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_backend_scheme_is_memory() {
        assert_eq!(MemoryBackend.scheme(), "memory");
    }

    #[cfg(feature = "lance-backend")]
    #[test]
    fn lance_backend_scheme_is_lance() {
        assert_eq!(LanceBackend.scheme(), "lance");
    }

    #[tokio::test]
    async fn memory_storage_list_tables_returns_empty() {
        let b = MemoryBackend;
        let s = b
            .open("memory://test", &StorageOptions::default())
            .await
            .unwrap();
        assert!(s.list_tables().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn memory_storage_write_then_list_includes_table() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};

        let s = MemoryStorage::new();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
        )
        .unwrap();
        let h = s.write_batch("people", &batch).await.unwrap();
        assert!(h.id >= 1);

        let tables = s.list_tables().await.unwrap();
        assert_eq!(tables, vec!["people".to_owned()]);
        assert_eq!(s.schema("people").await.unwrap().as_ref(), schema.as_ref());
    }

    #[tokio::test]
    async fn memory_storage_delete_removes_table() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};

        let s = MemoryStorage::new();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2]))]).unwrap();
        s.write_batch("t", &batch).await.unwrap();

        let predicate = Expr::Literal(datafusion::scalar::ScalarValue::Boolean(Some(true)), None);
        let deleted = s.delete("t", &predicate).await.unwrap();
        assert_eq!(deleted, 2);
        assert!(s.list_tables().await.unwrap().is_empty());
    }

    #[cfg(feature = "lance-backend")]
    #[tokio::test]
    async fn lance_backend_round_trip_in_tempdir() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};
        use futures::StreamExt as _;

        let tmp = std::env::temp_dir().join(format!(
            "uni-plugin-builtin-lance-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&tmp).unwrap();
        let uri = tmp.to_string_lossy().to_string();

        let backend = LanceBackend;
        let storage = backend
            .open(&uri, &StorageOptions::default())
            .await
            .expect("lance open");

        // empty list
        assert!(storage.list_tables().await.unwrap().is_empty());

        // write + read
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10_i64, 20, 30]))],
        )
        .unwrap();
        let h1 = storage.write_batch("t", &batch).await.expect("write 1");

        let tables = storage.list_tables().await.unwrap();
        assert_eq!(tables, vec!["t".to_owned()]);

        let mut stream = storage.read_batch("t", None).await.expect("read_batch");
        let mut total = 0_usize;
        while let Some(b) = stream.next().await {
            total += b.expect("batch").num_rows();
        }
        assert_eq!(total, 3);

        // Two sequential writes — `WriteHandle.id` mirrors Lance's
        // monotonic table version (M5a follow-up #4).
        let h2 = storage.write_batch("t", &batch).await.expect("write 2");
        assert!(
            h2.id > h1.id,
            "write ids must be monotonic: {h1:?} -> {h2:?}"
        );

        // Clean up tempdir (best-effort).
        drop(storage);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[cfg(feature = "lance-backend")]
    #[tokio::test]
    async fn lance_predicate_pushdown_filter_eq_string() {
        use arrow_array::{Int64Array, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::common::Column;
        use datafusion::scalar::ScalarValue;
        use futures::StreamExt as _;

        let tmp = tempdir_for("predicate-eq");
        let uri = tmp.to_string_lossy().to_string();
        let storage = LanceBackend
            .open(&uri, &StorageOptions::default())
            .await
            .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
                Arc::new(StringArray::from(vec!["foo", "bar", "foo"])),
            ],
        )
        .unwrap();
        storage.write_batch("t", &batch).await.unwrap();

        let predicate = Expr::Column(Column::new_unqualified("label")).eq(Expr::Literal(
            ScalarValue::Utf8(Some("foo".to_owned())),
            None,
        ));
        let mut stream = storage
            .read_batch("t", Some(&predicate))
            .await
            .expect("filtered read");
        let mut rows = 0_usize;
        while let Some(b) = stream.next().await {
            rows += b.expect("batch").num_rows();
        }
        assert_eq!(rows, 2, "label = 'foo' matches 2 of 3 rows");

        drop(storage);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[cfg(feature = "lance-backend")]
    #[tokio::test]
    async fn lance_predicate_pushdown_unsupported_returns_711() {
        // Direct exercise of the encoder. `ScalarValue::Binary(Some(_))`
        // has no SQL literal form so `datafusion::sql::unparser::expr_to_sql`
        // returns `not_impl_err`. We confirm the wrapper surfaces this as
        // `FnError 0x711` so callers can fall back to a DataFusion Filter
        // above the unfiltered scan.
        let predicate = Expr::Literal(
            datafusion::scalar::ScalarValue::Binary(Some(vec![0xde, 0xad, 0xbe, 0xef])),
            None,
        );

        let err =
            expr_to_lance_filter(&predicate).expect_err("binary literal must fail the unparser");
        assert_eq!(err.code, 0x711, "expected 0x711, got {err:?}");
        assert!(
            err.message.contains("predicate not encodable"),
            "expected message to mention encoding failure, got {err:?}"
        );
    }

    #[cfg(feature = "lance-backend")]
    #[tokio::test]
    async fn lance_delete_with_always_true_truncates_table() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::scalar::ScalarValue;
        use futures::StreamExt as _;

        let tmp = tempdir_for("delete-truncate");
        let uri = tmp.to_string_lossy().to_string();
        let storage = LanceBackend
            .open(&uri, &StorageOptions::default())
            .await
            .unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![10_i64, 20, 30, 40, 50]))],
        )
        .unwrap();
        storage.write_batch("t", &batch).await.unwrap();

        let predicate = Expr::Literal(ScalarValue::Boolean(Some(true)), None);
        let deleted = storage.delete("t", &predicate).await.unwrap();
        assert_eq!(deleted, 5);

        // Table still exists (truncation, not drop) but yields zero rows.
        assert_eq!(storage.list_tables().await.unwrap(), vec!["t".to_owned()]);
        let mut stream = storage.read_batch("t", None).await.unwrap();
        let mut rows = 0_usize;
        while let Some(b) = stream.next().await {
            rows += b.expect("batch").num_rows();
        }
        assert_eq!(rows, 0, "table truncated, must yield zero rows");

        drop(storage);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[cfg(feature = "lance-backend")]
    #[tokio::test]
    async fn lance_delete_with_predicate_drops_only_matching() {
        use arrow_array::{Int64Array, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::common::Column;
        use datafusion::scalar::ScalarValue;
        use futures::StreamExt as _;

        let tmp = tempdir_for("delete-predicate");
        let uri = tmp.to_string_lossy().to_string();
        let storage = LanceBackend
            .open(&uri, &StorageOptions::default())
            .await
            .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
                Arc::new(StringArray::from(vec!["foo", "bar", "foo"])),
            ],
        )
        .unwrap();
        storage.write_batch("t", &batch).await.unwrap();

        let predicate = Expr::Column(Column::new_unqualified("label")).eq(Expr::Literal(
            ScalarValue::Utf8(Some("foo".to_owned())),
            None,
        ));
        let deleted = storage.delete("t", &predicate).await.unwrap();
        assert_eq!(deleted, 2, "expected 2 rows matching label = 'foo'");

        // One survivor remains.
        let mut stream = storage.read_batch("t", None).await.unwrap();
        let mut rows = 0_usize;
        while let Some(b) = stream.next().await {
            rows += b.expect("batch").num_rows();
        }
        assert_eq!(rows, 1);

        drop(storage);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[cfg(feature = "lance-backend")]
    #[tokio::test]
    async fn lance_fork_branch_creates_branch_with_parent_version() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};

        let tmp = tempdir_for("fork-ok");
        let uri = tmp.to_string_lossy().to_string();
        let storage = LanceBackend
            .open(&uri, &StorageOptions::default())
            .await
            .unwrap();

        // Seed a table on the main trunk.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))])
                .unwrap();
        storage.write_batch("t", &batch).await.unwrap();

        assert!(
            storage.supports_branching(),
            "lance plugin supports branching"
        );

        let meta = storage
            .fork("t", "main", "fork_a")
            .await
            .expect("fork should succeed");
        assert_eq!(meta.branch_name, "fork_a");
        assert!(
            meta.parent_version >= 1,
            "parent_version must be Lance's pre-fork main version, got {}",
            meta.parent_version
        );

        drop(storage);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[cfg(feature = "lance-backend")]
    #[tokio::test]
    async fn lance_fork_branch_rejects_unknown_src() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};

        let tmp = tempdir_for("fork-err");
        let uri = tmp.to_string_lossy().to_string();
        let storage = LanceBackend
            .open(&uri, &StorageOptions::default())
            .await
            .unwrap();

        // Need a table to exist so the URI resolves to a real dataset.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))]).unwrap();
        storage.write_batch("t", &batch).await.unwrap();

        let err = storage
            .fork("t", "does-not-exist", "fork_x")
            .await
            .expect_err("forking from a missing branch should fail");
        assert_eq!(err.code, 0x712, "unexpected fork error: {err:?}");

        drop(storage);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[cfg(feature = "lance-backend")]
    #[test]
    fn lance_filter_pushdown_classifies_encodable_and_unencodable() {
        use datafusion::common::Column;
        use datafusion::logical_expr::{BinaryExpr, Operator};
        use datafusion::scalar::ScalarValue;
        use uni_plugin::traits::pushdown::SupportsFilterPushdown;

        // Encodable: `col = 'foo'` round-trips through the SQL unparser
        // and lands in `fully_handled`.
        let encodable = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name("col"))),
            Operator::Eq,
            Box::new(Expr::Literal(ScalarValue::Utf8(Some("foo".into())), None)),
        ));
        // Unencodable: a binary-literal predicate hits the same shape as
        // `lance_predicate_pushdown_unsupported_returns_711` above; the
        // unparser declines, so it must be omitted from `fully_handled`.
        let unencodable = Expr::Literal(
            ScalarValue::Binary(Some(vec![0xde, 0xad, 0xbe, 0xef])),
            None,
        );

        let marker = LanceFilterPushdown;
        let filters = vec![encodable, unencodable];
        let app = marker.push_filters(&filters);
        assert_eq!(
            app.fully_handled,
            vec![0],
            "only the encodable predicate (index 0) should be fully_handled"
        );
        assert!(
            app.partially_handled.is_empty(),
            "LanceFilterPushdown never reports partial handling"
        );
    }

    #[cfg(feature = "lance-backend")]
    fn tempdir_for(label: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "uni-plugin-builtin-lance-{label}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }
}
