// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! M5h — DataFusion `TableProvider` bridge for plugin [`Storage`] backends.
//!
//! Pairs an `Arc<dyn Storage>` (the plugin storage trait) with a stable
//! Arrow [`SchemaRef`] and a `table` name so it shows up as a regular
//! relation inside a DataFusion `SessionContext`. The bridge is the
//! missing leg between the M5a plugin storage surface and the M4
//! `PushdownNegotiationRule`: filters classified as encodable by the
//! [`SupportsFilterPushdown`] marker land in `Storage::read_batch`'s
//! predicate slot; non-encodable filters are left for a DataFusion
//! `FilterExec` above the scan.
//!
//! ## Pushdown wiring
//!
//! The provider itself does NOT implement `SupportsFilterPushdown`.
//! Instead, callers wrap it through
//! [`super::optimizer::PushdownAwareTable::with_filter`] alongside a
//! marker — [`StorageFilterPushdown`] for any backend that accepts SQL
//! filter strings (via `datafusion::sql::unparser::expr_to_sql`), or
//! the existing [`super::storage::LanceFilterPushdown`] for Lance
//! specifically (they share the same encoder today, but keeping the
//! marker per-backend leaves room to diverge).
//!
//! ## Async bridging
//!
//! `Storage::read_batch` is async; `ExecutionPlan::execute` is sync but
//! returns a `SendableRecordBatchStream`. [`StorageScanExec`] uses
//! `stream::once().try_flatten()` to lazily await `read_batch` when the
//! returned stream is first polled.

// Rust guideline compliant

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Statistics;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{
    BinaryExpr, Expr, Operator, TableProviderFilterPushDown, TableType,
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
};
use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use uni_plugin::adapters::catalog_from_storage::STORAGE_FILTER_UNENCODABLE;
use uni_plugin::traits::pushdown::{FilterApplication, SupportsFilterPushdown};
use uni_plugin::traits::storage::Storage;

/// DataFusion [`TableProvider`] backed by a plugin [`Storage`] handle.
///
/// Construction is cheap — the schema must be supplied by the caller
/// so `TableProvider::schema()` can return it synchronously without
/// awaiting `Storage::schema`.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use arrow_schema::{DataType, Field, Schema};
/// use datafusion::execution::context::SessionContext;
/// use uni_plugin::traits::storage::Storage;
/// use uni_plugin_builtin::optimizer::PushdownAwareTable;
/// use uni_plugin_builtin::storage_table_provider::{
///     StorageTableProvider, StorageFilterPushdown,
/// };
///
/// async fn register(ctx: &SessionContext, storage: Arc<dyn Storage>) {
///     let schema = Arc::new(Schema::new(vec![
///         Field::new("x", DataType::Int64, false),
///     ]));
///     let provider = StorageTableProvider::new(storage, "mem_table".to_owned(), schema);
///     let wrapped = PushdownAwareTable::with_filter(
///         Arc::new(provider),
///         Arc::new(StorageFilterPushdown),
///     );
///     ctx.register_table("mem_table", Arc::new(wrapped)).unwrap();
/// }
/// ```
pub struct StorageTableProvider {
    storage: Arc<dyn Storage>,
    schema: SchemaRef,
    table: String,
}

impl fmt::Debug for StorageTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageTableProvider")
            .field("table", &self.table)
            .field("schema", &self.schema)
            .field("storage", &"<dyn Storage>")
            .finish()
    }
}

impl StorageTableProvider {
    /// Construct a new provider over `storage` exposing rows from `table`.
    #[must_use]
    pub fn new(storage: Arc<dyn Storage>, table: String, schema: SchemaRef) -> Self {
        Self {
            storage,
            schema,
            table,
        }
    }

    /// The backing table name this provider reads from.
    #[must_use]
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Reference to the underlying storage handle.
    #[must_use]
    pub fn storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }
}

#[async_trait]
impl TableProvider for StorageTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(p) => project_schema(&self.schema, p)?,
            None => Arc::clone(&self.schema),
        };
        let exec = StorageScanExec::new(
            Arc::clone(&self.storage),
            self.table.clone(),
            Arc::clone(&self.schema),
            projected_schema,
            projection.cloned(),
            filters.to_vec(),
            limit,
        );
        Ok(Arc::new(exec))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        // Conservative default: report `Inexact` for every filter, so the
        // optimizer leaves a verifying `FilterExec` above the scan unless
        // the wrapping `PushdownAwareTable` + `StorageFilterPushdown`
        // marker upgrades the classification through
        // `PushdownNegotiationRule`. This keeps correctness regardless of
        // whether the provider was registered standalone.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

fn project_schema(schema: &SchemaRef, projection: &[usize]) -> DfResult<SchemaRef> {
    let mut fields = Vec::with_capacity(projection.len());
    for &i in projection {
        let f = schema.fields().get(i).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "StorageTableProvider: projection index {i} out of bounds for schema with {} fields",
                schema.fields().len()
            ))
        })?;
        fields.push(f.as_ref().clone());
    }
    Ok(Arc::new(arrow_schema::Schema::new(fields)))
}

/// AND-combine a slice of filter expressions into a single conjunction.
fn and_combine(filters: &[Expr]) -> Option<Expr> {
    let mut iter = filters.iter().cloned();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, next| {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(acc),
            Operator::And,
            Box::new(next),
        ))
    }))
}

/// `ExecutionPlan` for [`StorageTableProvider::scan`].
///
/// On `execute()` calls `Storage::read_batch(table, predicate)`,
/// adapts the resulting stream to honor a client-side projection and
/// limit (the plugin trait doesn't take those today), and propagates
/// schema metadata. When the backend rejects the encoded predicate
/// with `FnError 0x711` the scan retries unfiltered — DataFusion's
/// `FilterExec` above will re-apply the predicate.
pub struct StorageScanExec {
    storage: Arc<dyn Storage>,
    table: String,
    /// Full table schema (unprojected) — needed to decode batches
    /// before projection.
    full_schema: SchemaRef,
    /// Schema after projection — what DataFusion sees.
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for StorageScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageScanExec")
            .field("table", &self.table)
            .field("projection", &self.projection)
            .field("filters", &self.filters.len())
            .field("limit", &self.limit)
            .finish()
    }
}

impl StorageScanExec {
    /// Construct a new physical scan over a plugin [`Storage`] handle.
    #[must_use]
    pub fn new(
        storage: Arc<dyn Storage>,
        table: String,
        full_schema: SchemaRef,
        projected_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
    ) -> Self {
        let eq_props = EquivalenceProperties::new(Arc::clone(&projected_schema));
        let properties = Arc::new(PlanProperties::new(
            eq_props,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            storage,
            table,
            full_schema,
            projected_schema,
            projection,
            filters,
            limit,
            properties,
        }
    }
}

impl DisplayAs for StorageScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StorageScanExec: table={}", self.table)?;
        if !self.filters.is_empty() {
            write!(f, ", filters={}", self.filters.len())?;
        }
        if let Some(p) = &self.projection {
            write!(f, ", projection={p:?}")?;
        }
        if let Some(lim) = self.limit {
            write!(f, ", limit={lim}")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for StorageScanExec {
    fn name(&self) -> &str {
        "StorageScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan(
                "StorageScanExec has no children".to_owned(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "StorageScanExec only emits partition 0, got {partition}"
            )));
        }
        let storage = Arc::clone(&self.storage);
        let table = self.table.clone();
        let predicate = and_combine(&self.filters);
        let projection = self.projection.clone();
        let limit = self.limit;
        let projected_schema = Arc::clone(&self.projected_schema);
        let full_schema_for_stream = Arc::clone(&self.full_schema);

        let inner = stream::once(async move {
            let _ = full_schema_for_stream;
            match storage.read_batch(&table, predicate.as_ref()).await {
                Ok(s) => Ok(s),
                Err(e) if e.code == STORAGE_FILTER_UNENCODABLE => {
                    storage.read_batch(&table, None).await.map_err(fn_err_to_df)
                }
                Err(e) => Err(fn_err_to_df(e)),
            }
        })
        .try_flatten();

        let adapted = ProjectionAndLimitStream::new(
            Box::pin(inner),
            Arc::clone(&projected_schema),
            projection,
            limit,
        );

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projected_schema,
            adapted,
        )))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DfResult<Statistics> {
        Ok(Statistics::new_unknown(&self.projected_schema))
    }
}

fn fn_err_to_df(e: uni_plugin::errors::FnError) -> DataFusionError {
    DataFusionError::Execution(format!(
        "plugin Storage::read_batch failed (code 0x{:x}): {}",
        e.code, e.message
    ))
}

type BatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>>;

/// Wrap a batch stream with client-side projection and limit. Mirrors
/// the helper in `catalog_from_storage` because the plugin trait
/// doesn't currently take projection / limit — backends that can push
/// these advertise it via the [`super::optimizer::PushdownMarkers`]
/// bundle, but the in-tree memory backend ignores them.
struct ProjectionAndLimitStream {
    inner: BatchStream,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    remaining: Option<usize>,
    done: bool,
}

impl ProjectionAndLimitStream {
    fn new(
        inner: BatchStream,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            inner,
            schema,
            projection,
            remaining: limit,
            done: false,
        }
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, DataFusionError> {
        if let Some(p) = self.projection.as_deref() {
            batch
                .project(p)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        } else {
            Ok(batch)
        }
    }
}

impl Stream for ProjectionAndLimitStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match self.inner.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                self.done = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(e))) => {
                self.done = true;
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(Some(Ok(batch))) => {
                let projected = match self.apply(batch) {
                    Ok(b) => b,
                    Err(e) => {
                        self.done = true;
                        return Poll::Ready(Some(Err(e)));
                    }
                };
                let take = match self.remaining {
                    Some(n) if n <= projected.num_rows() => {
                        self.done = true;
                        n
                    }
                    Some(n) => {
                        self.remaining = Some(n - projected.num_rows());
                        projected.num_rows()
                    }
                    None => projected.num_rows(),
                };
                if take == projected.num_rows() {
                    Poll::Ready(Some(Ok(projected)))
                } else {
                    Poll::Ready(Some(Ok(projected.slice(0, take))))
                }
            }
        }
    }
}

impl RecordBatchStream for ProjectionAndLimitStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

// ============================================================================
// SupportsFilterPushdown marker — generic Storage backend.
// ============================================================================

/// `SupportsFilterPushdown` marker for plugin storage backends that
/// accept SQL-string filters.
///
/// Mirrors [`super::storage::LanceFilterPushdown`] but lives next to
/// the generic `StorageTableProvider`; both delegate to
/// `datafusion::sql::unparser::expr_to_sql` for classification. Wrap a
/// `StorageTableProvider` in
/// [`super::optimizer::PushdownAwareTable::with_filter`] with one of
/// these so `PushdownNegotiationRule` elides redundant `FilterExec`
/// nodes above the scan.
#[derive(Debug, Default)]
pub struct StorageFilterPushdown;

impl SupportsFilterPushdown for StorageFilterPushdown {
    fn push_filters(&self, filters: &[Expr]) -> FilterApplication {
        let fully_handled = filters
            .iter()
            .enumerate()
            .filter(|(_, expr)| datafusion::sql::unparser::expr_to_sql(expr).is_ok())
            .map(|(idx, _)| idx)
            .collect();
        FilterApplication {
            fully_handled,
            partially_handled: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryStorage;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::common::Column;
    use datafusion::scalar::ScalarValue;

    #[tokio::test]
    async fn storage_filter_pushdown_classifies_encodable() {
        let encodable = Expr::Column(Column::new_unqualified("x"))
            .eq(Expr::Literal(ScalarValue::Int64(Some(5)), None));
        let unencodable = Expr::Literal(ScalarValue::Binary(Some(vec![0xde, 0xad])), None);
        let m = StorageFilterPushdown;
        let app = m.push_filters(&[encodable, unencodable]);
        assert_eq!(app.fully_handled, vec![0]);
    }

    #[tokio::test]
    async fn provider_schema_round_trips() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
        let p = StorageTableProvider::new(storage, "t".to_owned(), Arc::clone(&schema));
        assert_eq!(p.schema().as_ref(), schema.as_ref());
        assert_eq!(p.table(), "t");
    }

    #[tokio::test]
    async fn scan_returns_written_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 5, 6, 7, 8]))],
        )
        .unwrap();
        storage.write_batch("mem", &batch).await.unwrap();

        let provider = Arc::new(StorageTableProvider::new(
            storage,
            "mem".to_owned(),
            Arc::clone(&schema),
        ));
        let ctx = datafusion::execution::context::SessionContext::new();
        ctx.register_table("mem", provider).unwrap();
        let df = ctx.sql("SELECT x FROM mem WHERE x > 5").await.expect("sql");
        let batches = df.collect().await.expect("collect");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3, "x > 5 matches 6, 7, 8");
    }
}
