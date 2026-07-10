//! Graph algorithm plugins.
//!
//! Two surfaces: [`AlgorithmProvider`] for black-box algorithms (the
//! existing `uni-algo` library style), and [`GraphView`] — the stable,
//! read-only topology API a provider obtains from its [`AlgorithmHost`]
//! via [`AlgorithmHost::project`] to walk the graph without depending on
//! `uni-store` / `uni-algo` types.

use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use futures::future::BoxFuture;
use uni_common::core::id::Vid;

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

/// Host callback surfacing graph access to plugin algorithms.
///
/// A provider's [`AlgorithmProvider::run`] receives an [`AlgorithmHost`]
/// through its [`AlgorithmContext`] and calls [`AlgorithmHost::project`]
/// to materialize a [`GraphView`] over the requested subgraph. Hosts
/// (e.g. `uni-plugin-builtin`) implement `project` by building a
/// projection from their `StorageManager` / `L0Manager`; the
/// [`AlgorithmHost::as_any`] downcast hook remains for hosts that expose
/// additional concrete state. This keeps `uni-plugin` free of upward
/// dependencies on `uni-store` / `uni-algo`.
pub trait AlgorithmHost: Send + Sync {
    /// Downcast hook — bridges implement this to expose the concrete
    /// host type.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Materialize a read-only [`GraphView`] over the subgraph named by
    /// `spec`.
    ///
    /// The returned future is `'static` (owns its inputs) so a provider
    /// can move it into the stream it returns from the synchronous
    /// [`AlgorithmProvider::run`] and `.await` it there. The default
    /// implementation reports that the host offers no graph access;
    /// graph-capable hosts override it.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the host offers no graph access, the
    /// caller lacks the required capability (e.g. `HostQuery`), or the
    /// projection cannot be built.
    fn project(
        &self,
        spec: &GraphProjectionSpec,
    ) -> BoxFuture<'static, Result<Arc<dyn GraphView>, FnError>> {
        let _ = spec;
        Box::pin(async {
            Err(FnError::new(
                0x805,
                "AlgorithmHost: project() is not supported by this host",
            ))
        })
    }
}

/// Selects which subgraph an [`AlgorithmHost::project`] call materializes.
///
/// Empty `node_labels` / `edge_types` mean "all". `weight_property`
/// names an edge property to expose through [`GraphView::out_weight`];
/// `include_reverse` requests inbound adjacency ([`GraphView::in_neighbors`]).
#[derive(Clone, Debug, Default)]
pub struct GraphProjectionSpec {
    /// Vertex labels to include; empty selects every label.
    pub node_labels: Vec<String>,
    /// Edge types to include; empty selects every type.
    pub edge_types: Vec<String>,
    /// Edge property surfaced as the traversal weight, if any.
    pub weight_property: Option<String>,
    /// Whether to also build inbound adjacency.
    pub include_reverse: bool,
}

/// Stable, read-only topology view handed to a plugin algorithm.
///
/// Vertices are addressed by dense `u32` slots (`0..vertex_count`);
/// [`GraphView::to_vid`] / [`GraphView::to_slot`] translate to and from
/// external [`Vid`]s at the boundary. Neighbor accessors return neighbor
/// *slots*, not vids. A `GraphView` reflects the subgraph named by the
/// [`GraphProjectionSpec`] that produced it and does not observe later
/// writes.
///
/// # Panics
///
/// [`GraphView::out_weight`] panics unless [`GraphView::has_weights`] is
/// `true`, and [`GraphView::in_neighbors`] / [`GraphView::in_degree`]
/// panic unless [`GraphView::has_reverse`] is `true`. Guard with those
/// predicates before calling.
pub trait GraphView: Send + Sync {
    /// Number of vertices; valid slots are `0..vertex_count`.
    fn vertex_count(&self) -> usize;

    /// Total number of outbound edges.
    fn edge_count(&self) -> usize;

    /// Outbound neighbor slots of `slot`.
    fn out_neighbors(&self, slot: u32) -> &[u32];

    /// Number of outbound edges from `slot`.
    fn out_degree(&self, slot: u32) -> u32;

    /// Inbound neighbor slots of `slot`.
    ///
    /// # Panics
    ///
    /// Panics unless [`GraphView::has_reverse`] is `true`.
    fn in_neighbors(&self, slot: u32) -> &[u32];

    /// Number of inbound edges into `slot`.
    ///
    /// # Panics
    ///
    /// Panics unless [`GraphView::has_reverse`] is `true`.
    fn in_degree(&self, slot: u32) -> u32;

    /// Whether inbound adjacency is available.
    fn has_reverse(&self) -> bool;

    /// Weight of the `edge_idx`-th outbound edge of `slot`.
    ///
    /// `edge_idx` indexes into [`GraphView::out_neighbors`] of `slot`.
    ///
    /// # Panics
    ///
    /// Panics unless [`GraphView::has_weights`] is `true`.
    fn out_weight(&self, slot: u32, edge_idx: usize) -> f64;

    /// Whether edge weights are available.
    fn has_weights(&self) -> bool;

    /// Translate a dense slot to its external [`Vid`].
    fn to_vid(&self, slot: u32) -> Vid;

    /// Translate an external [`Vid`] to its dense slot, if present.
    fn to_slot(&self, vid: Vid) -> Option<u32>;

    /// Iterate over every `(slot, vid)` pair in the view.
    fn vertices(&self) -> Box<dyn Iterator<Item = (u32, Vid)> + '_>;
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
