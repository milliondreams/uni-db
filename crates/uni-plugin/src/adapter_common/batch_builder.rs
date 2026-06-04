//! Single-row [`RecordBatch`] / stream builders shared by loader adapters.
//!
//! `uni-plugin-rhai`'s procedure adapter materializes the plugin's
//! returned rows into one `RecordBatch` and then wraps it in a
//! `RecordBatchStreamAdapter` yielding exactly one item. The 1-batch
//! stream pattern is generic and unrelated to rhai. This module hosts
//! the shared implementation so future adapters (custom in-process
//! procedures, builtin synthetic catalogs) reuse it.
//!
//! These helpers are pure Arrow utilities; they do **not** depend on any
//! plugin-loader feature gate.

// Rust guideline compliant

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{ArrowError, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;

/// Construct a single-row [`RecordBatch`] from the supplied schema and
/// column arrays.
///
/// Thin wrapper over `RecordBatch::try_new` whose only purpose is to
/// give shared call sites a named function (so the rhai loader, future
/// synthetic catalogs, and tests share a vocabulary).
///
/// # Errors
///
/// Propagates [`ArrowError::SchemaError`] / [`ArrowError::InvalidArgumentError`]
/// when the supplied columns do not match the schema (column count, type
/// mismatch, or row-count mismatch).
pub fn single_row_record_batch(
    schema: SchemaRef,
    cols: Vec<ArrayRef>,
) -> Result<RecordBatch, ArrowError> {
    RecordBatch::try_new(schema, cols)
}

/// Wrap a single [`RecordBatch`] in a one-item
/// [`SendableRecordBatchStream`].
///
/// Used by procedure adapters whose plugin produced exactly one batch
/// (and by tests that need a stream-shaped fixture for a one-row
/// constant input). The yielded schema is `Arc`-cloned from the
/// supplied `batch`.
#[must_use]
pub fn batch_into_stream(batch: RecordBatch) -> SendableRecordBatchStream {
    let schema = batch.schema();
    let stream = stream::iter(vec![Ok(batch)]);
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}
