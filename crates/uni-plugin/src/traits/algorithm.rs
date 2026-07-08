//! Graph algorithm plugins.
//!
//! One surface: [`AlgorithmProvider`] for black-box algorithms (the
//! existing `uni-algo` library style).

use datafusion::execution::SendableRecordBatchStream;

use crate::errors::FnError;

/// Static signature of an algorithm.
#[derive(Clone, Debug)]
pub struct AlgorithmSignature {
    /// Output column schema.
    pub output_fields: Vec<arrow_schema::Field>,
    /// Markdown docs.
    pub docs: String,
}

/// Per-invocation context passed to an [`AlgorithmProvider`].
///
/// `host` is an opaque [`AlgorithmHost`] callback the host populates
/// when invoking the algorithm. Algorithms that need a concrete
/// graph-projection / storage handle downcast through `host` rather
/// than depend on `uni-store` / `uni-algo` types directly — this keeps
/// `uni-plugin` free of upward dependencies.
#[non_exhaustive]
pub struct AlgorithmContext<'a> {
    /// JSON-serialized algorithm configuration.
    pub config_json: &'a str,
    /// Optional opaque host handle. `None` when no host is bound — the
    /// algorithm may fall back to a config-only path or surface an
    /// `Unbound` error.
    pub host: Option<&'a dyn AlgorithmHost>,
}

impl std::fmt::Debug for AlgorithmContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlgorithmContext")
            .field("config_json", &self.config_json)
            .field("host_bound", &self.host.is_some())
            .finish()
    }
}

impl<'a> AlgorithmContext<'a> {
    /// Construct an `AlgorithmContext` with no host bound.
    #[must_use]
    pub fn new(config_json: &'a str) -> Self {
        Self {
            config_json,
            host: None,
        }
    }

    /// Attach a host handle.
    #[must_use]
    pub fn with_host(mut self, host: &'a dyn AlgorithmHost) -> Self {
        self.host = Some(host);
        self
    }
}

/// Opaque host callback surfacing graph access to plugin algorithms.
///
/// Hosts implement this trait; bridges (e.g. `uni-plugin-builtin`)
/// downcast via [`AlgorithmHost::as_any`] to recover the concrete host
/// type and its `StorageManager` / `L0Manager` handles. Keeps
/// `uni-plugin` free of upward dependencies on `uni-store` / `uni-algo`.
pub trait AlgorithmHost: Send + Sync {
    /// Downcast hook — bridges implement this to expose the concrete
    /// host type.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// A black-box graph algorithm.
///
/// The trait is intentionally minimal: a signature describing the output,
/// plus a `run` method returning a streaming `RecordBatch` sequence. The
/// algorithm is responsible for fetching graph data via host APIs (out of
/// scope of this trait — `uni-algo` will provide a `GraphView` abstraction
/// the host adapter passes via `AlgorithmContext` once those APIs are
/// available).
pub trait AlgorithmProvider: Send + Sync {
    /// Static signature.
    fn signature(&self) -> &AlgorithmSignature;

    /// Execute the algorithm.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the algorithm cannot be started; per-batch
    /// failures are signaled via `Err` items in the returned stream.
    fn run(&self, ctx: AlgorithmContext<'_>) -> Result<SendableRecordBatchStream, FnError>;
}
