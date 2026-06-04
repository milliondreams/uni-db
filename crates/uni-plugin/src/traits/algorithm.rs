//! Graph algorithm plugins.
//!
//! Two surfaces: [`AlgorithmProvider`] for black-box algorithms (the
//! existing `uni-algo` library style), and [`PregelProgramProvider`] for
//! vertex-program-style algorithms the host's Pregel executor runs.

use arrow_array::ArrayRef;
use arrow_schema::DataType;
use datafusion::execution::SendableRecordBatchStream;
use smol_str::SmolStr;

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

/// Signature of a Pregel-style vertex program.
#[derive(Clone, Debug)]
pub struct PregelSignature {
    /// Per-vertex state column type.
    pub state_type: DataType,
    /// Message column type.
    pub message_type: DataType,
    /// Synchronization model.
    pub aggregation_mode: AggregationMode,
    /// Optional hard cap on supersteps.
    pub max_supersteps: Option<u64>,
}

/// Synchronization model for Pregel programs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AggregationMode {
    /// Bulk Synchronous Parallel — classic Pregel.
    Bsp,
    /// Asynchronous with shared state.
    AsyncShared,
    /// Asynchronous via point-to-point messaging.
    AsyncMessaging,
}

/// Outcome of a vertex's `compute` step.
#[derive(Debug)]
pub struct ComputeOutcome {
    /// Whether this vertex votes to halt.
    pub halt: bool,
    /// Outgoing messages, addressed to neighbor vertices.
    pub outgoing: Vec<(SmolStr, ArrayRef)>,
}

/// Statistics surfaced to the Pregel host between supersteps.
#[derive(Clone, Copy, Debug, Default)]
pub struct PregelStats {
    /// Active vertices in the current superstep.
    pub active_vertices: u64,
    /// Messages sent in the previous superstep.
    pub messages_sent: u64,
    /// Wall-clock duration of the previous superstep, milliseconds.
    pub last_superstep_ms: u64,
}

/// A Pregel-style vertex program plugin.
///
/// Detailed `init` / `compute` / `combine` signatures will land alongside
/// the Pregel executor in `uni-algo` during M5c. The trait is in place so
/// plugin authors can build against the surface from M1; full integration
/// follows.
pub trait PregelProgramProvider: Send + Sync {
    /// Static signature.
    fn signature(&self) -> &PregelSignature;

    /// Optional global halt condition consulted between supersteps.
    fn halt(&self, _superstep: u64, _stats: &PregelStats) -> bool {
        false
    }
}
