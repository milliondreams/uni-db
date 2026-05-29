//! CDC output / logical-replication plugins.

use std::sync::Arc;
use std::time::SystemTime;

use datafusion::arrow::record_batch::RecordBatch;

use crate::errors::FnError;

/// Logical sequence number for change-data-capture.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct CdcLsn(pub u64);

/// Per-instance start context for a CDC sink.
#[derive(Debug)]
#[non_exhaustive]
pub struct CdcStartContext<'a> {
    /// LSN to resume from (`None` for fresh streams).
    pub from_lsn: Option<CdcLsn>,
    /// Lifetime marker — host adapter wires session reference.
    pub _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> CdcStartContext<'a> {
    /// Construct a fresh context. The struct is `#[non_exhaustive]` so
    /// external callers can't use struct-literal syntax; this
    /// constructor is the supported path.
    #[must_use]
    pub fn new(from_lsn: Option<CdcLsn>) -> Self {
        Self {
            from_lsn,
            _marker: std::marker::PhantomData,
        }
    }
}

/// A batch of CDC events with the LSN range it covers.
#[derive(Clone, Debug)]
pub struct CdcBatch {
    /// Inclusive start of the LSN range.
    pub lsn_start: CdcLsn,
    /// Exclusive end of the LSN range.
    pub lsn_end: CdcLsn,
    /// Schema-stable mutation events as a typed batch.
    pub mutations: Arc<RecordBatch>,
    /// Wall-clock timestamp of the source commit.
    pub commit_timestamp: SystemTime,
}

/// A CDC-output provider — produces an `Arc<dyn CdcStream>` on start.
pub trait CdcOutputProvider: Send + Sync {
    /// Provider name (`"kafka"`, `"pulsar"`, `"jsonl"`, …).
    fn name(&self) -> &str;

    /// Start a new CDC stream.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the sink cannot be initialized.
    fn start(&self, ctx: CdcStartContext<'_>) -> Result<Box<dyn CdcStream>, FnError>;
}

/// A live CDC sink instance.
pub trait CdcStream: Send {
    /// Deliver a batch to the sink.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] on delivery failure (network error, queue full).
    fn deliver(&mut self, batch: &CdcBatch) -> Result<(), FnError>;

    /// Acknowledge progress — host advances retention to this LSN.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the checkpoint cannot be persisted.
    fn checkpoint(&mut self) -> Result<CdcLsn, FnError>;

    /// Gracefully shut down the sink.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if shutdown fails (network errors, etc.).
    fn shutdown(&mut self) -> Result<(), FnError>;
}
