//! Storage backend plugins — `MATCH` / `CREATE` against pluggable stores.
//!
//! M5a (2026-05-24): traits are `#[async_trait]`. Real backends such as the
//! `lance://` adapter in `uni-plugin-builtin` need to drive async I/O
//! through `uni-store`; the plugin surface mirrors that shape so adapters
//! don't have to fabricate a blocking runtime per call.

use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Expr;

use crate::errors::FnError;

/// Options passed at backend open time.
#[derive(Clone, Debug, Default)]
pub struct StorageOptions {
    /// Free-form JSON configuration.
    pub config_json: String,
}

/// Opaque write handle returned by [`Storage::write_batch`].
#[derive(Clone, Debug)]
pub struct WriteHandle {
    /// Backend-specific identifier (LSN, transaction id, …).
    pub id: u64,
}

/// Metadata returned by [`Storage::fork`] describing the newly-created branch.
///
/// `parent_version` is the backend version pinned as the fork-point, so
/// callers orchestrating nested forks can chain `create_branch_from`-style
/// calls without re-querying the backend. `branch_name` echoes the
/// `dst_branch` argument, surfaced explicitly so backends with name
/// canonicalization can return the resolved form.
#[derive(Clone, Debug)]
pub struct BranchMetadata {
    /// Backend version pinned as the new branch's fork-point.
    pub parent_version: u64,
    /// Branch identifier as registered on the backend.
    pub branch_name: String,
}

/// A storage backend identified by URI scheme.
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// URI scheme this backend handles (`"lance"`, `"s3"`, `"memory"`).
    fn scheme(&self) -> &'static str;

    /// Open the backend at `uri`.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the URI is malformed or the backend cannot
    /// be opened (auth failure, network error).
    async fn open(&self, uri: &str, options: &StorageOptions) -> Result<Arc<dyn Storage>, FnError>;
}

/// Per-instance storage interface.
#[async_trait]
pub trait Storage: Send + Sync {
    /// Stream batches from `table` matching `predicate`.
    ///
    /// `predicate = None` means a full scan.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the read cannot start.
    async fn read_batch(
        &self,
        table: &str,
        predicate: Option<&Expr>,
    ) -> Result<SendableRecordBatchStream, FnError>;

    /// Write a single batch to `table`.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] on write failure.
    async fn write_batch(&self, table: &str, batch: &RecordBatch) -> Result<WriteHandle, FnError>;

    /// List tables known to this backend.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the listing cannot complete.
    async fn list_tables(&self) -> Result<Vec<String>, FnError>;

    /// Delete rows in `table` matching `predicate`. Returns the number of
    /// rows actually deleted.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] on delete failure.
    async fn delete(&self, table: &str, predicate: &Expr) -> Result<u64, FnError>;

    /// Whether this backend supports branched / forked state.
    fn supports_branching(&self) -> bool {
        false
    }

    /// Fork `src_branch` of `table` into `dst_branch`. Default: unsupported.
    ///
    /// Granularity is per-dataset (`table`) because real branching backends
    /// (Lance) track branches and versions independently per dataset.
    /// Multi-dataset orchestration (atomic across all tables of a logical
    /// fork) is the caller's responsibility.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if branching is not supported or the fork
    /// operation fails (missing source branch, name collision, I/O).
    async fn fork(
        &self,
        _table: &str,
        _src_branch: &str,
        _dst_branch: &str,
    ) -> Result<BranchMetadata, FnError> {
        Err(FnError::new(
            0x10,
            "storage backend does not support branching",
        ))
    }

    /// Backend-declared schema for `table`, if known.
    async fn schema(&self, _table: &str) -> Option<SchemaRef> {
        None
    }
}
