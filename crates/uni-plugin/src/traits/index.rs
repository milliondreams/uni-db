//! Index kind plugins — custom vector, FTS, geo indexes.

use arrow_array::BooleanArray;
use arrow_schema::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use smol_str::SmolStr;

use crate::errors::FnError;

/// Identifier for an index kind (`"vector"`, `"fts"`, `"hnsw"`, …).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct IndexKind(pub SmolStr);

impl IndexKind {
    /// Construct an `IndexKind` from a string.
    #[must_use]
    pub fn new(s: impl Into<SmolStr>) -> Self {
        Self(s.into())
    }
}

/// An index-kind provider that knows how to build / open / probe / persist
/// a custom index.
pub trait IndexKindProvider: Send + Sync {
    /// The index kind this provider implements.
    fn kind(&self) -> IndexKind;

    /// Build a new index from a source record batch.
    ///
    /// `options` is free-form JSON configuration.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] on build failure (out of memory, bad
    /// configuration).
    fn build(&self, source: &RecordBatch, options: &str) -> Result<Box<dyn IndexBuild>, FnError>;

    /// Open an index from previously-persisted bytes.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the bytes are malformed or incompatible.
    fn open(&self, persisted: &[u8]) -> Result<Box<dyn IndexHandle>, FnError>;
}

/// In-flight index build (write side).
pub trait IndexBuild: Send + Sync {
    /// Finalize the build and produce a queryable handle.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if finalization fails.
    fn finalize(self: Box<Self>) -> Result<Box<dyn IndexHandle>, FnError>;
}

/// Queryable, persistable index handle (read side).
pub trait IndexHandle: Send + Sync {
    /// Probe the index with `query` and return up to `k` matches.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] on probe failure.
    fn probe(&self, query: &RecordBatch, k: usize) -> Result<RecordBatch, FnError>;

    /// Whether this index supports per-probe filter pushdown.
    fn supports_filter(&self) -> bool {
        false
    }

    /// Probe with a row-level filter applied.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if filtered probe is unsupported or fails.
    fn probe_filtered(
        &self,
        _query: &RecordBatch,
        _k: usize,
        _filter: &BooleanArray,
    ) -> Result<RecordBatch, FnError> {
        Err(FnError::new(
            0x20,
            "index does not support filter-pushdown probe",
        ))
    }

    /// Serialize this index for persistence.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if serialization fails.
    fn persist(&self) -> Result<Vec<u8>, FnError>;

    /// Output schema of `probe` / `probe_filtered`.
    fn schema(&self) -> SchemaRef;
}
