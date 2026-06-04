//! Scalar plugin functions — Cypher `RETURN myfn(x)`.
//!
//! Scalar plugins are the bread and butter of the plugin framework: pure (or
//! session-scoped) functions that map a row of input columns to one output
//! value per row. They are columnar by default; a [`RowFn`] adapter provides
//! the row-at-a-time convenience for plugin authors who don't need
//! vectorization.

use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::logical_expr::{ColumnarValue, Volatility};

use crate::errors::FnError;

/// A scalar plugin function — `(rows of columnar input) → 1 columnar output`.
///
/// Implementations are expected to be `Send + Sync` because they are shared
/// across query workers; non-thread-safe state (e.g., a mutable cache) must
/// be wrapped behind `Mutex` or `RwLock`.
pub trait ScalarPluginFn: Send + Sync {
    /// The function's static signature (arg types, return type, volatility).
    fn signature(&self) -> &FnSignature;

    /// Invoke the function on a batch of inputs.
    ///
    /// `args[i]` is the `i`-th argument's column-or-scalar; `rows` is the
    /// number of rows the caller expects to be produced. Implementations
    /// should produce exactly `rows` values when returning a column.
    ///
    /// # Errors
    ///
    /// Returns an [`FnError`] for any per-call failure; this is wrapped into
    /// `UniError::Plugin` at the call site.
    fn invoke(&self, args: &[ColumnarValue], rows: usize) -> Result<ColumnarValue, FnError>;
}

/// Static signature of a scalar plugin function.
#[derive(Clone, Debug)]
pub struct FnSignature {
    /// Argument types, in order.
    pub args: Vec<ArgType>,
    /// Return type.
    pub returns: ArgType,
    /// DataFusion volatility (drives caching and hoisting).
    pub volatility: Volatility,
    /// Null-handling policy.
    pub null_handling: NullHandling,
}

impl FnSignature {
    /// Convenience constructor for the common case: known args/returns,
    /// derived volatility, propagate-nulls semantics.
    #[must_use]
    pub fn new(args: Vec<ArgType>, returns: ArgType, volatility: Volatility) -> Self {
        Self {
            args,
            returns,
            volatility,
            null_handling: NullHandling::PropagateNulls,
        }
    }
}

/// How the framework handles `NULL` values in scalar-fn arguments.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NullHandling {
    /// Any `NULL` in an arg short-circuits to `NULL` output (standard Cypher).
    PropagateNulls,
    /// The function handles `NULL` explicitly via `Option<T>` semantics.
    UserHandled,
}

/// Logical type of a scalar function argument or return.
///
/// `Primitive` arguments take the **native Arrow fast path** — no
/// `LargeBinary` round-trip. `CypherValue` arguments go through the
/// legacy `LargeBinary` transport for fns that genuinely need to see
/// `Node` / `Relationship` / `Path` values.
#[derive(Clone, Debug)]
pub enum ArgType {
    /// Native Arrow primitive type (`Float64`, `Int64`, `Utf8`, etc.).
    Primitive(DataType),
    /// Full `CypherValue` (serialized as `LargeBinary` opaque to the plugin).
    CypherValue,
    /// Fixed-size list of `element` with declared `len`.
    Vector {
        /// Number of elements per row.
        len: usize,
        /// Element type.
        element: DataType,
    },
    /// Variadic — repeats the inner `ArgType` zero or more times.
    Variadic(Box<ArgType>),
}

/// Row-at-a-time adapter wrapping a closure into a [`ScalarPluginFn`].
///
/// This is the *convenience* path for plugin authors who don't care about
/// vectorization. The default columnar contract is preferred for hot-path
/// UDFs; use `RowFn` for one-off ad-hoc fns where per-row author ergonomics
/// matter more than per-row performance.
pub struct RowFn<F> {
    signature: FnSignature,
    #[allow(
        dead_code,
        reason = "row evaluation is wired by uni-query host adapter; field held for downstream extraction"
    )]
    inner: Arc<F>,
}

impl<F> std::fmt::Debug for RowFn<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowFn")
            .field("signature", &self.signature)
            .finish_non_exhaustive()
    }
}

impl<F> RowFn<F> {
    /// Wrap a row-shaped closure into a scalar plugin fn.
    #[must_use]
    pub fn new(signature: FnSignature, f: F) -> Self {
        Self {
            signature,
            inner: Arc::new(f),
        }
    }
}

// Note: actual row-by-row invocation requires the Value type from
// uni-common, which we cannot reference here without a cyclic dep.
// The real `RowFn::invoke` implementation lives in `uni-query` where
// `Value` is available; this struct is the type-level placeholder.

impl<F> ScalarPluginFn for RowFn<F>
where
    F: Send + Sync + 'static,
{
    fn signature(&self) -> &FnSignature {
        &self.signature
    }

    fn invoke(&self, _args: &[ColumnarValue], _rows: usize) -> Result<ColumnarValue, FnError> {
        // RowFn::invoke is implemented by the host adapter in uni-query,
        // which knows how to deserialize ColumnarValue → Value rows and
        // re-serialize the result. The trait impl here exists so RowFn
        // implements ScalarPluginFn (for type-erasure into Arc<dyn>); the
        // actual row evaluation is replaced at the registration boundary
        // with a closure that has access to uni-common's Value type.
        Err(FnError::new(
            0xDEAD,
            "RowFn::invoke must be intercepted by the host adapter; \
             see uni-query::custom_functions::register_row_fn",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_constructor() {
        let sig = FnSignature::new(
            vec![ArgType::Primitive(DataType::Float64)],
            ArgType::Primitive(DataType::Float64),
            Volatility::Immutable,
        );
        assert_eq!(sig.args.len(), 1);
        assert_eq!(sig.null_handling, NullHandling::PropagateNulls);
    }

    #[test]
    fn arg_type_variants_round_trip_in_debug() {
        let t = ArgType::Vector {
            len: 384,
            element: DataType::Float32,
        };
        let s = format!("{t:?}");
        assert!(s.contains("Vector"));
        assert!(s.contains("384"));
    }
}
