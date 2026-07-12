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
///
/// `args` and `slices` are additive, defaulted fields (construct with
/// `..Default::default()`): existing providers that leave them empty keep the
/// legacy untyped `config_json` contract unchanged. A provider that declares
/// `args` opts into host-side arity/type validation before it runs (proposal
/// §4.6 / decision D7); a provider that declares `slices` opts into load-time
/// capability-slice version negotiation (proposal §4.3 / decision D6).
#[derive(Clone, Debug, Default)]
pub struct AlgorithmSignature {
    /// Output column schema.
    pub output_fields: Vec<arrow_schema::Field>,
    /// Markdown docs.
    pub docs: String,
    /// Declared positional arguments, in call order.
    ///
    /// Empty (the default) preserves the legacy behavior: arguments arrive as a
    /// raw positional `config_json` array the provider parses itself. When
    /// non-empty, the host validates arity and coerces each positional argument
    /// against the declared [`NamedArgType`] before the provider runs, filling
    /// omitted trailing arguments from their declared defaults.
    pub args: Vec<crate::traits::procedure::NamedArgType>,
    /// Required capability slices, checked at load time.
    ///
    /// Empty (the default) means the algorithm targets only the always-present
    /// `graph-compute@1` surface. A declared [`SliceReq`] whose version the host
    /// does not provide fails the load with a clear error (`0x86A`) rather than a
    /// mysterious runtime "unknown kernel op" trap.
    pub slices: Vec<SliceReq>,
}

/// A required capability-slice version an algorithm declares in its signature.
///
/// The host checks each requirement against the slices it actually implements
/// (today only `graph-compute@1`) when the algorithm loads, refusing a mismatch
/// up front (proposal §4.3 / decision D6). Adding a slice or bumping a version is
/// a forward-compatible, additive change: an algorithm that declares no slices is
/// grandfathered onto the base surface.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SliceReq {
    /// The capability-slice name, e.g. `"graph-compute"`.
    pub slice: smol_str::SmolStr,
    /// The minimum slice version the algorithm requires.
    pub version: u16,
}

/// The capability slices this host implements, for load-time negotiation.
///
/// A guest algorithm's declared [`SliceReq`]s are checked against this table
/// when it loads. Today the host implements only `graph-compute@1`; a future
/// slice (e.g. `tensor-compute@1`) is added here in lockstep with its kernels so
/// negotiation stays a pure lookup (proposal §4.3 / §10).
pub const HOST_CAPABILITY_SLICES: &[(&str, u16)] = &[("graph-compute", 1)];

impl AlgorithmSignature {
    /// Validates the declared capability slices against `host_slices`.
    ///
    /// Each requirement must be met by a host slice of the same name whose
    /// version is at least the requested one. Pass [`HOST_CAPABILITY_SLICES`] for
    /// the production surface.
    ///
    /// # Errors
    /// Returns `0x86A` (`SliceVersionMismatch`) naming the first requirement the
    /// host cannot satisfy (proposal §4.3 / §12, decision D6).
    pub fn check_slices(&self, host_slices: &[(&str, u16)]) -> Result<(), FnError> {
        for req in &self.slices {
            let satisfied = host_slices
                .iter()
                .any(|(name, ver)| *name == req.slice.as_str() && *ver >= req.version);
            if !satisfied {
                return Err(FnError::new(
                    0x86A,
                    format!(
                        "algorithm requires capability slice `{}@{}` the host does not provide",
                        req.slice, req.version
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Validates and normalizes a positional `config_json` array against `args`.
    ///
    /// When `args` is empty this is a no-op returning `config_json` unchanged, so
    /// providers on the legacy untyped contract are unaffected. Otherwise it
    /// parses the positional JSON array and, per declared argument: rejects a
    /// present value whose JSON kind is incompatible with the declared
    /// [`ArgType`](crate::traits::scalar::ArgType), errors on a missing argument
    /// that has no default, and appends the declared default for an omitted
    /// trailing argument. Extra positional arguments beyond the declared arity
    /// are rejected. The returned JSON array is what the provider then parses, so
    /// it observes defaults already filled in.
    ///
    /// # Errors
    /// Returns `0x86E` (argument arity/type violation) with a message naming the
    /// offending argument (proposal §4.6, decision D7).
    pub fn coerce_config_json(&self, config_json: &str) -> Result<String, FnError> {
        use crate::traits::scalar::ArgType;

        if self.args.is_empty() {
            return Ok(config_json.to_owned());
        }
        let mut provided: Vec<serde_json::Value> = if config_json.trim().is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(config_json)
                .map_err(|e| FnError::new(0x86E, format!("bad positional config json: {e}")))?
        };
        if provided.len() > self.args.len() {
            return Err(FnError::new(
                0x86E,
                format!(
                    "too many arguments: got {}, expected at most {}",
                    provided.len(),
                    self.args.len()
                ),
            ));
        }
        let mut out = Vec::with_capacity(self.args.len());
        for (i, arg) in self.args.iter().enumerate() {
            match provided.get_mut(i) {
                Some(value) => {
                    let value = std::mem::replace(value, serde_json::Value::Null);
                    // A `CypherValue` argument is opaque and accepts any JSON.
                    if !matches!(arg.ty, ArgType::CypherValue)
                        && !json_matches_argtype(&value, &arg.ty)
                    {
                        return Err(FnError::new(
                            0x86E,
                            format!(
                                "argument `{}` (position {i}) has the wrong type",
                                arg.name
                            ),
                        ));
                    }
                    out.push(value);
                }
                None => match &arg.default {
                    Some(default) => out.push(scalar_default_to_json(default)),
                    None => {
                        return Err(FnError::new(
                            0x86E,
                            format!("missing required argument `{}` (position {i})", arg.name),
                        ));
                    }
                },
            }
        }
        serde_json::to_string(&out)
            .map_err(|e| FnError::new(0x86E, format!("re-encoding coerced config: {e}")))
    }
}

/// Whether a JSON value is compatible with a declared primitive/vector arg type.
fn json_matches_argtype(
    value: &serde_json::Value,
    ty: &crate::traits::scalar::ArgType,
) -> bool {
    use arrow_schema::DataType;

    use crate::traits::scalar::ArgType;
    match ty {
        ArgType::CypherValue => true,
        ArgType::Vector { .. } => value.is_array(),
        ArgType::Variadic(inner) => json_matches_argtype(value, inner),
        ArgType::Primitive(dt) => match dt {
            DataType::Boolean => value.is_boolean(),
            DataType::Utf8 | DataType::LargeUtf8 => value.is_string(),
            DataType::Float16 | DataType::Float32 | DataType::Float64 => value.is_number(),
            d if d.is_integer() => value.is_i64() || value.is_u64(),
            // Unknown/opaque primitive: don't reject, defer to the provider.
            _ => true,
        },
    }
}

/// Renders a declared [`ScalarValue`](datafusion::scalar::ScalarValue) default as
/// JSON to append for an omitted trailing argument.
fn scalar_default_to_json(default: &datafusion::scalar::ScalarValue) -> serde_json::Value {
    use datafusion::scalar::ScalarValue;

    match default {
        ScalarValue::Null => serde_json::Value::Null,
        ScalarValue::Boolean(Some(b)) => serde_json::Value::Bool(*b),
        ScalarValue::Float32(Some(x)) => serde_json::json!(*x),
        ScalarValue::Float64(Some(x)) => serde_json::json!(*x),
        ScalarValue::Int8(Some(x)) => serde_json::json!(*x),
        ScalarValue::Int16(Some(x)) => serde_json::json!(*x),
        ScalarValue::Int32(Some(x)) => serde_json::json!(*x),
        ScalarValue::Int64(Some(x)) => serde_json::json!(*x),
        ScalarValue::UInt8(Some(x)) => serde_json::json!(*x),
        ScalarValue::UInt16(Some(x)) => serde_json::json!(*x),
        ScalarValue::UInt32(Some(x)) => serde_json::json!(*x),
        ScalarValue::UInt64(Some(x)) => serde_json::json!(*x),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            serde_json::Value::String(s.clone())
        }
        // Any other/None scalar defaults to JSON null.
        _ => serde_json::Value::Null,
    }
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

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;
    use datafusion::scalar::ScalarValue;

    use super::{AlgorithmSignature, SliceReq, HOST_CAPABILITY_SLICES};
    use crate::traits::procedure::NamedArgType;
    use crate::traits::scalar::ArgType;

    fn sig_with(args: Vec<NamedArgType>, slices: Vec<SliceReq>) -> AlgorithmSignature {
        AlgorithmSignature {
            args,
            slices,
            ..Default::default()
        }
    }

    fn arg(name: &str, ty: ArgType, default: Option<ScalarValue>) -> NamedArgType {
        NamedArgType {
            name: name.into(),
            ty,
            default,
            doc: String::new(),
        }
    }

    #[test]
    fn check_slices_accepts_available_and_rejects_missing() {
        // graph-compute@1 is the host surface; @1 passes, @2 and unknown fail 0x86A.
        let ok = sig_with(vec![], vec![SliceReq { slice: "graph-compute".into(), version: 1 }]);
        assert!(ok.check_slices(HOST_CAPABILITY_SLICES).is_ok());

        let too_new =
            sig_with(vec![], vec![SliceReq { slice: "graph-compute".into(), version: 2 }]);
        let err = too_new
            .check_slices(HOST_CAPABILITY_SLICES)
            .expect_err("graph-compute@2 must be refused");
        assert_eq!(err.code, 0x86A, "slice mismatch is 0x86A");

        let unknown =
            sig_with(vec![], vec![SliceReq { slice: "tensor-compute".into(), version: 1 }]);
        assert_eq!(
            unknown.check_slices(HOST_CAPABILITY_SLICES).unwrap_err().code,
            0x86A
        );

        // No declared slices is grandfathered onto the base surface.
        assert!(sig_with(vec![], vec![]).check_slices(HOST_CAPABILITY_SLICES).is_ok());
    }

    #[test]
    fn coerce_config_passes_through_when_untyped() {
        // Empty `args` preserves the legacy raw contract byte-for-byte.
        let s = sig_with(vec![], vec![]);
        assert_eq!(s.coerce_config_json("[1, 2, 3]").unwrap(), "[1, 2, 3]");
    }

    #[test]
    fn coerce_config_fills_defaults_and_validates() {
        let s = sig_with(
            vec![
                arg("src", ArgType::CypherValue, None),
                arg("alpha", ArgType::Primitive(DataType::Float64), Some(ScalarValue::Float64(Some(0.85)))),
            ],
            vec![],
        );

        // A single provided arg fills the omitted `alpha` default.
        let out = s.coerce_config_json("[5]").unwrap();
        let arr: Vec<serde_json::Value> = serde_json::from_str(&out).unwrap();
        assert_eq!(arr.len(), 2, "the omitted default is appended");
        assert_eq!(arr[0], serde_json::json!(5));
        assert!((arr[1].as_f64().unwrap() - 0.85).abs() < 1e-12);

        // A missing required arg is rejected.
        let err = s.coerce_config_json("[]").expect_err("src is required");
        assert_eq!(err.code, 0x86E);

        // A wrong-typed alpha (string, not number) is rejected.
        let err = s
            .coerce_config_json(r#"[5, "not-a-number"]"#)
            .expect_err("alpha must be numeric");
        assert_eq!(err.code, 0x86E);

        // Too many positional args is rejected.
        assert_eq!(
            s.coerce_config_json("[5, 0.9, 1]").unwrap_err().code,
            0x86E
        );

        // A CypherValue arg accepts an array (the `sourceVids` shape).
        let arr_src = s.coerce_config_json("[[1, 2, 3], 0.9]").unwrap();
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&arr_src).unwrap();
        assert!(parsed[0].is_array(), "CypherValue accepts an array");
    }
}
