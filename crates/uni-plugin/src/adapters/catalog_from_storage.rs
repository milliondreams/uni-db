// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Bridge a [`Storage`] plugin into the [`CatalogTable`] surface so the
//! host's graph planner can route a virtual label through plugin
//! storage (M5h).
//!
//! Background: plugin `Storage::read_batch` is async; `CatalogTable::scan`
//! is sync but returns a `SendableRecordBatchStream`. The adapter
//! builds a stream that *lazily* awaits `read_batch` when first polled,
//! so the sync `scan()` signature is honored without blocking inside
//! the planner.
//!
//! Filter handling: `CatalogTable::scan` passes a slice of
//! [`datafusion::logical_expr::Expr`] filters. `Storage::read_batch`
//! accepts a single optional filter. We AND-combine the slice into one
//! conjunction before forwarding; backends that can't encode the
//! conjunction signal that via [`FnError::code`] `0x711` per the
//! Storage contract, and the adapter falls back to an unfiltered scan
//! plus a DataFusion-side filter applied by the planner (the planner
//! already wraps `CatalogTable::scan` in a `Filter` node when the
//! source declines pushdown — see `CatalogVertexScanExec`).
//!
//! Projection / limit: applied client-side on the resulting stream,
//! after `Storage` returns its batches. Backends that can push these
//! down should advertise [`SupportsProjectionPushdown`] /
//! [`SupportsLimitPushdown`] markers and route through
//! `PushdownAwareTable`; this adapter is the minimum-viable bridge and
//! does not negotiate pushdown.
//!
//! # Examples
//!
//! ```no_run
//! use std::sync::Arc;
//! use arrow_schema::{DataType, Field, Schema};
//! use uni_plugin::adapters::StorageCatalogTable;
//! use uni_plugin::traits::storage::Storage;
//! use uni_plugin::traits::catalog::CatalogTable;
//!
//! fn wrap(storage: Arc<dyn Storage>) -> Arc<dyn CatalogTable> {
//!     let schema = Arc::new(Schema::new(vec![
//!         Field::new("id", DataType::Int64, false),
//!         Field::new("name", DataType::Utf8, true),
//!     ]));
//!     Arc::new(StorageCatalogTable::new(storage, "people".to_owned(), schema))
//! }
//! ```
//!
//! [`Storage`]: crate::traits::storage::Storage
//! [`CatalogTable`]: crate::traits::catalog::CatalogTable
//! [`SupportsProjectionPushdown`]: crate::traits::pushdown::SupportsProjectionPushdown
//! [`SupportsLimitPushdown`]: crate::traits::pushdown::SupportsLimitPushdown

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream::{self, Stream, StreamExt, TryStreamExt};

/// Trait object alias for the plain (no schema) stream carried by the
/// projection/limit layer. `SendableRecordBatchStream` requires a
/// schema accessor — `RecordBatchStreamAdapter` provides it externally
/// — so the inner stream type is a plain `Stream<Item = Result<…>>`.
type BatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send>>;

use crate::errors::FnError;
use crate::traits::catalog::CatalogTable;
use crate::traits::storage::Storage;

// Rust guideline compliant

/// `FnError` code reserved by the `Storage` contract for "predicate
/// cannot be encoded by this backend". When the wrapped storage
/// returns this, the adapter retries with an unfiltered scan and
/// trusts the planner to wrap a `Filter` node on top.
pub const STORAGE_FILTER_UNENCODABLE: u32 = 0x711;

/// Adapter that exposes a [`Storage`] plugin as a [`CatalogTable`].
///
/// Construction is cheap — the schema is supplied by the caller so the
/// adapter doesn't need an async I/O to satisfy `CatalogTable::schema()`.
/// If the wrapped storage's [`Storage::schema`] yields a `SchemaRef`,
/// callers can await it during plugin registration and feed it here.
pub struct StorageCatalogTable {
    storage: Arc<dyn Storage>,
    table: String,
    schema: SchemaRef,
}

impl std::fmt::Debug for StorageCatalogTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageCatalogTable")
            .field("table", &self.table)
            .field("schema", &self.schema)
            .field("storage", &"<dyn Storage>")
            .finish()
    }
}

impl StorageCatalogTable {
    /// Build a new adapter over `storage` exposing rows from `table`
    /// with the supplied `schema`.
    #[must_use]
    pub fn new(storage: Arc<dyn Storage>, table: String, schema: SchemaRef) -> Self {
        Self {
            storage,
            table,
            schema,
        }
    }

    /// Reference to the wrapped storage (useful in tests).
    #[must_use]
    pub fn storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }

    /// Name of the underlying table.
    #[must_use]
    pub fn table(&self) -> &str {
        &self.table
    }
}

impl CatalogTable for StorageCatalogTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn scan(
        &self,
        projection: Option<&[usize]>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<SendableRecordBatchStream, FnError> {
        let storage = Arc::clone(&self.storage);
        let table = self.table.clone();
        let predicate = and_combine(filters);
        let projection_owned: Option<Vec<usize>> = projection.map(<[usize]>::to_vec);

        // Output schema reflects the projection so downstream nodes see
        // a stable column shape even before the first batch arrives.
        let output_schema: SchemaRef = match projection_owned.as_deref() {
            Some(p) => project_schema(&self.schema, p),
            None => Arc::clone(&self.schema),
        };

        let inner = stream::once(async move {
            let res = storage.read_batch(&table, predicate.as_ref()).await;
            match res {
                Ok(s) => Ok(s),
                // Backend rejected the encoded predicate — retry
                // unfiltered. Planner-side `Filter` re-applies it.
                Err(e) if e.code == STORAGE_FILTER_UNENCODABLE => {
                    storage.read_batch(&table, None).await.map_err(fn_err_to_df)
                }
                Err(e) => Err(fn_err_to_df(e)),
            }
        })
        .map(|res| match res {
            Ok(stream) => Ok(stream),
            Err(e) => Err(e),
        })
        .try_flatten();

        let projected = ProjectionAndLimitStream::new(inner.boxed(), projection_owned, limit);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            projected,
        )))
    }
}

/// AND-combine a slice of filter expressions into a single conjunction.
/// Returns `None` for an empty slice (full scan) and the lone element
/// unchanged when only one filter is supplied.
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

/// Project an Arrow schema down to the indices in `projection`.
fn project_schema(schema: &SchemaRef, projection: &[usize]) -> SchemaRef {
    let fields: Vec<arrow_schema::Field> = projection
        .iter()
        .filter_map(|i| schema.fields().get(*i).map(|f| f.as_ref().clone()))
        .collect();
    Arc::new(arrow_schema::Schema::new(fields))
}

/// Translate `FnError` into a `DataFusionError` for stream emission.
fn fn_err_to_df(e: FnError) -> DataFusionError {
    DataFusionError::Execution(format!(
        "plugin Storage::read_batch failed (code 0x{:x}): {}",
        e.code, e.message
    ))
}

/// Wrap a batch stream with client-side projection and limit. Avoids
/// pulling the DataFusion `ProjectionExec` / `LimitExec` for the
/// trivial cases this adapter handles, and keeps the bridge a
/// single-allocation stream layer.
struct ProjectionAndLimitStream {
    inner: BatchStream,
    projection: Option<Vec<usize>>,
    remaining: Option<usize>,
    done: bool,
}

impl ProjectionAndLimitStream {
    fn new(inner: BatchStream, projection: Option<Vec<usize>>, limit: Option<usize>) -> Self {
        Self {
            inner,
            projection,
            remaining: limit,
            done: false,
        }
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, DataFusionError> {
        let projected = if let Some(p) = self.projection.as_deref() {
            batch
                .project(p)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?
        } else {
            batch
        };
        Ok(projected)
    }
}

impl Stream for ProjectionAndLimitStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
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
                    // Slice the batch down to the limit.
                    Poll::Ready(Some(Ok(projected.slice(0, take))))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use datafusion::execution::SendableRecordBatchStream;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::stream::{self, StreamExt};
    use std::sync::Mutex;

    use crate::traits::storage::WriteHandle;

    struct StaticStorage {
        batches: Mutex<Vec<RecordBatch>>,
        schema: SchemaRef,
        last_predicate: Mutex<Option<Expr>>,
        fail_on_filter: bool,
    }

    #[async_trait]
    impl Storage for StaticStorage {
        async fn read_batch(
            &self,
            _table: &str,
            predicate: Option<&Expr>,
        ) -> Result<SendableRecordBatchStream, FnError> {
            if self.fail_on_filter && predicate.is_some() {
                return Err(FnError::new(STORAGE_FILTER_UNENCODABLE, "unencodable"));
            }
            *self.last_predicate.lock().expect("predicate mutex") = predicate.cloned();
            let batches = self.batches.lock().expect("batches mutex").clone();
            let schema = Arc::clone(&self.schema);
            let s = stream::iter(batches.into_iter().map(Ok));
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, s)))
        }

        async fn write_batch(
            &self,
            _table: &str,
            _batch: &RecordBatch,
        ) -> Result<WriteHandle, FnError> {
            Err(FnError::new(1, "read-only fixture"))
        }

        async fn list_tables(&self) -> Result<Vec<String>, FnError> {
            Ok(vec!["t".to_owned()])
        }

        async fn delete(&self, _table: &str, _predicate: &Expr) -> Result<u64, FnError> {
            Err(FnError::new(1, "read-only fixture"))
        }
    }

    fn fixture_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn fixture_batch(schema: &SchemaRef, ids: &[i64], names: &[&str]) -> RecordBatch {
        let id_arr = Arc::new(Int64Array::from(ids.to_vec()));
        let name_arr = Arc::new(StringArray::from_iter(names.iter().map(|s| Some(*s))));
        RecordBatch::try_new(Arc::clone(schema), vec![id_arr, name_arr]).expect("fixture batch")
    }

    #[tokio::test]
    async fn full_scan_streams_all_rows() {
        let schema = fixture_schema();
        let storage = Arc::new(StaticStorage {
            batches: Mutex::new(vec![fixture_batch(&schema, &[1, 2, 3], &["a", "b", "c"])]),
            schema: Arc::clone(&schema),
            last_predicate: Mutex::new(None),
            fail_on_filter: false,
        });
        let storage: Arc<dyn Storage> = storage;
        let table = StorageCatalogTable::new(storage, "people".to_owned(), schema);

        let mut stream = table.scan(None, &[], None).expect("scan starts");
        let mut total = 0usize;
        while let Some(b) = stream.next().await {
            total += b.expect("batch").num_rows();
        }
        assert_eq!(total, 3);
    }

    #[tokio::test]
    async fn limit_is_applied_client_side() {
        let schema = fixture_schema();
        let storage = Arc::new(StaticStorage {
            batches: Mutex::new(vec![fixture_batch(&schema, &[1, 2, 3], &["a", "b", "c"])]),
            schema: Arc::clone(&schema),
            last_predicate: Mutex::new(None),
            fail_on_filter: false,
        });
        let storage: Arc<dyn Storage> = storage;
        let table = StorageCatalogTable::new(storage, "people".to_owned(), schema);

        let mut stream = table.scan(None, &[], Some(2)).expect("scan starts");
        let mut total = 0usize;
        while let Some(b) = stream.next().await {
            total += b.expect("batch").num_rows();
        }
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn projection_drops_columns() {
        let schema = fixture_schema();
        let storage = Arc::new(StaticStorage {
            batches: Mutex::new(vec![fixture_batch(&schema, &[1, 2], &["a", "b"])]),
            schema: Arc::clone(&schema),
            last_predicate: Mutex::new(None),
            fail_on_filter: false,
        });
        let table = StorageCatalogTable::new(storage, "people".to_owned(), Arc::clone(&schema));

        let mut stream = table.scan(Some(&[0]), &[], None).expect("scan starts");
        let mut total_cols = 0usize;
        while let Some(b) = stream.next().await {
            let b = b.expect("batch");
            total_cols = b.num_columns();
        }
        assert_eq!(total_cols, 1, "projection should drop name column");
    }

    #[tokio::test]
    async fn unencodable_filter_falls_back_to_unfiltered() {
        use datafusion::logical_expr::{col, lit};
        let schema = fixture_schema();
        let storage = Arc::new(StaticStorage {
            batches: Mutex::new(vec![fixture_batch(&schema, &[1, 2, 3], &["a", "b", "c"])]),
            schema: Arc::clone(&schema),
            last_predicate: Mutex::new(None),
            fail_on_filter: true,
        });
        let storage: Arc<dyn Storage> = storage;
        let table = StorageCatalogTable::new(storage, "people".to_owned(), schema);

        let filter = col("id").eq(lit(2_i64));
        let mut stream = table.scan(None, &[filter], None).expect("scan starts");
        let mut total = 0usize;
        while let Some(b) = stream.next().await {
            total += b.expect("batch").num_rows();
        }
        // Backend rejected the filter — adapter retried unfiltered, so
        // all 3 rows come back. Planner-side `Filter` would re-apply
        // the predicate in a real query path.
        assert_eq!(total, 3);
    }
}
