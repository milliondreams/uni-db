//! Shared plumbing for the APOC-core procedure modules.
//!
//! Each `apoc.*` namespace module (`bitwise`, `text`, `math`, …) models its
//! procedures as a `Copy` discriminant enum implementing [`ApocProc`]. This
//! module factors out the machinery that would otherwise be copy-pasted into
//! every file: the registration loop, the per-enum signature cache, the
//! columnar-argument extractors, the single-row Arrow result builders, and the
//! `RecordBatch` → stream tail.

use std::sync::{Arc, OnceLock};

use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::scalar::ScalarValue;
use futures::stream;
use uni_plugin::traits::procedure::{ProcedurePlugin, ProcedureSignature};
use uni_plugin::{FnError, PluginError, PluginRegistrar, QName};

/// Upper bound on the length of a synthesized string output (`text.repeat`'s
/// total length, `create.uuids`' row count). Caps pathological inputs so a
/// single call cannot exhaust memory.
pub(super) const MAX_SYNTHESIZED_LEN: usize = 1_000_000;

/// `FnError` codes for the `RecordBatch::try_new` failure tail, one per module.
/// These only fire on an Arrow schema/array mismatch (an internal invariant
/// violation), never on user input.
pub(super) mod batch_err {
    /// `bitwise.*` result-batch construction failure.
    pub(crate) const BITWISE: u32 = 0x700;
    /// `text.*` result-batch construction failure.
    pub(crate) const TEXT: u32 = 0x701;
    /// `math.*` result-batch construction failure.
    pub(crate) const MATH: u32 = 0x702;
    /// `number.*` result-batch construction failure.
    pub(crate) const NUMBER: u32 = 0x703;
    /// `convert.*` result-batch construction failure.
    pub(crate) const CONVERT: u32 = 0x704;
    /// `create.*` result-batch construction failure.
    pub(crate) const CREATE: u32 = 0x705;
}

/// `math.coth` undefined-at-zero error code.
pub(super) const CODE_MATH_DOMAIN: u32 = 0x800;

/// A discriminant enum describing one APOC-core namespace's procedures.
///
/// Implementors are `Copy` unit-like enums; the blanket [`register_all`] loop
/// registers every variant with its cached signature.
pub(super) trait ApocProc: ProcedurePlugin + Copy + 'static {
    /// Every variant of this namespace, in registration order.
    const ALL: &'static [Self];

    /// Fully-qualified name (`apoc-core` plugin id + local path).
    fn qname(&self) -> QName;

    /// Position of this variant within [`ALL`](Self::ALL); used to index the
    /// per-enum signature cache.
    fn index(&self) -> usize;

    /// Build this variant's signature from scratch (called at most once per
    /// variant, on first cache miss).
    fn build_signature(&self) -> ProcedureSignature;
}

/// Look up `proc`'s signature in `cache`, materializing the whole `ALL` table
/// on first use. The cache holds one entry per variant, indexed by
/// [`ApocProc::index`], so a namespace pays a single allocation pass rather
/// than one `OnceLock` static per variant.
pub(super) fn cached_signature<P: ApocProc>(
    cache: &'static OnceLock<Vec<ProcedureSignature>>,
    proc: &P,
) -> &'static ProcedureSignature {
    let sigs = cache.get_or_init(|| P::ALL.iter().map(ApocProc::build_signature).collect());
    &sigs[proc.index()]
}

/// Register every variant of `P` into `r` using its cached signature.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a qname is taken.
pub(super) fn register_all<P: ApocProc>(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    for proc in P::ALL {
        r.procedure(proc.qname(), proc.signature().clone(), Arc::new(*proc))?;
    }
    Ok(())
}

/// Wrap a single-row `(schema, array)` pair into the one-batch stream every
/// procedure returns. `batch_code`/`label` tag the (invariant-only) failure.
pub(super) fn one_row_stream(
    schema: SchemaRef,
    array: Arc<dyn Array>,
    batch_code: u32,
    label: &str,
) -> Result<SendableRecordBatchStream, FnError> {
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array])
        .map_err(|e| FnError::new(batch_code, format!("{label}: {e}")))?;
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema,
        stream::iter(vec![Ok(batch)]),
    )))
}

/// Single-column `result` schema of the given type and nullability.
fn result_schema(ty: DataType, nullable: bool) -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("result", ty, nullable)]))
}

/// Non-null single-row `Utf8` result.
pub(super) fn string_result(s: String) -> (SchemaRef, Arc<dyn Array>) {
    let arr = Arc::new(StringArray::from(vec![s])) as Arc<dyn Array>;
    (result_schema(DataType::Utf8, false), arr)
}

/// Non-null single-row `Boolean` result.
pub(super) fn bool_result(b: bool) -> (SchemaRef, Arc<dyn Array>) {
    let arr = Arc::new(BooleanArray::from(vec![b])) as Arc<dyn Array>;
    (result_schema(DataType::Boolean, false), arr)
}

/// Non-null single-row `Int64` result.
pub(super) fn int_result(n: i64) -> (SchemaRef, Arc<dyn Array>) {
    let arr = Arc::new(Int64Array::from(vec![n])) as Arc<dyn Array>;
    (result_schema(DataType::Int64, false), arr)
}

/// Non-null single-row `Float64` result.
pub(super) fn float_result(v: f64) -> (SchemaRef, Arc<dyn Array>) {
    let arr = Arc::new(Float64Array::from(vec![v])) as Arc<dyn Array>;
    (result_schema(DataType::Float64, false), arr)
}

/// Nullable single-row `Utf8` result.
pub(super) fn nullable_string_result(s: Option<String>) -> (SchemaRef, Arc<dyn Array>) {
    let arr = Arc::new(StringArray::from(vec![s])) as Arc<dyn Array>;
    (result_schema(DataType::Utf8, true), arr)
}

/// Nullable single-row `Boolean` result.
pub(super) fn nullable_bool_result(b: Option<bool>) -> (SchemaRef, Arc<dyn Array>) {
    let arr = Arc::new(BooleanArray::from(vec![b])) as Arc<dyn Array>;
    (result_schema(DataType::Boolean, true), arr)
}

/// Nullable single-row `Int64` result.
pub(super) fn nullable_int_result(i: Option<i64>) -> (SchemaRef, Arc<dyn Array>) {
    let arr = Arc::new(Int64Array::from(vec![i])) as Arc<dyn Array>;
    (result_schema(DataType::Int64, true), arr)
}

/// Nullable single-row `Float64` result.
pub(super) fn nullable_float_result(f: Option<f64>) -> (SchemaRef, Arc<dyn Array>) {
    let arr = Arc::new(Float64Array::from(vec![f])) as Arc<dyn Array>;
    (result_schema(DataType::Float64, true), arr)
}

/// How a `Float64` argument is coerced when an integer is requested.
///
/// The APOC namespaces deliberately disagree on this — see [`extract_i64`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FloatToInt {
    /// Reject any float argument (`bitwise`, `text`).
    Reject,
    /// Silently truncate toward zero (`math`, matching Java APOC).
    Truncate,
}

/// Extract a non-null `i64` from `args[idx]`.
///
/// `label` prefixes every error message. `float_policy` and `accept_array`
/// reproduce the historically divergent behavior of the three original copies
/// (math truncated floats and rejected arrays; bitwise rejected floats but
/// accepted an `Int64Array`; text rejected both) — they are explicit
/// parameters precisely so each call site keeps its prior semantics.
pub(super) fn extract_i64(
    args: &[ColumnarValue],
    idx: usize,
    label: &str,
    float_policy: FloatToInt,
    accept_array: bool,
) -> Result<i64, FnError> {
    let arg = args.get(idx).ok_or_else(|| {
        FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("{label}: expected argument at position {idx}"),
        )
    })?;
    match arg {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => Ok(*v),
        ColumnarValue::Scalar(ScalarValue::Float64(Some(v)))
            if float_policy == FloatToInt::Truncate =>
        {
            Ok(*v as i64)
        }
        ColumnarValue::Array(arr) if accept_array => {
            let a = arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| FnError::new(FnError::CODE_TYPE_COERCION, "expected Int64Array"))?;
            if a.is_empty() || a.is_null(0) {
                Err(FnError::new(
                    FnError::CODE_UNEXPECTED_NULL,
                    format!("{label}: integer argument must not be null"),
                ))
            } else {
                Ok(a.value(0))
            }
        }
        _ => Err(FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("{label}: integer argument required"),
        )),
    }
}

/// Extract a non-null `f64` from `args[idx]`. Accepts `Int64` (widened) and
/// `Float64` scalars and `Float64Array`. `label` prefixes every error message.
pub(super) fn extract_f64(args: &[ColumnarValue], idx: usize, label: &str) -> Result<f64, FnError> {
    let arg = args.get(idx).ok_or_else(|| {
        FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("{label}: expected argument at position {idx}"),
        )
    })?;
    match arg {
        ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => Ok(*v),
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => Ok(*v as f64),
        ColumnarValue::Array(arr) => {
            if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
                if a.is_empty() || a.is_null(0) {
                    Err(FnError::new(
                        FnError::CODE_UNEXPECTED_NULL,
                        format!("{label}: numeric argument must not be null"),
                    ))
                } else {
                    Ok(a.value(0))
                }
            } else {
                Err(FnError::new(
                    FnError::CODE_TYPE_COERCION,
                    format!("{label}: expected Float64Array"),
                ))
            }
        }
        _ => Err(FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("{label}: numeric argument required"),
        )),
    }
}

/// Extract a non-null `String` from `args[idx]`.
///
/// `accept_array` selects whether a `StringArray` first element is accepted
/// (`text` does; `number` only takes scalars). `label` prefixes errors.
pub(super) fn extract_string(
    args: &[ColumnarValue],
    idx: usize,
    label: &str,
    accept_array: bool,
) -> Result<String, FnError> {
    let arg = args.get(idx).ok_or_else(|| {
        FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("{label}: expected argument at position {idx}"),
        )
    })?;
    match arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s))) => {
            Ok(s.clone())
        }
        ColumnarValue::Array(arr) if accept_array => {
            if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
                if a.is_empty() || a.is_null(0) {
                    Err(FnError::new(
                        FnError::CODE_UNEXPECTED_NULL,
                        format!("{label}: string argument must not be null"),
                    ))
                } else {
                    Ok(a.value(0).to_owned())
                }
            } else {
                Err(FnError::new(
                    FnError::CODE_TYPE_COERCION,
                    format!("{label}: expected StringArray"),
                ))
            }
        }
        _ => Err(FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("{label}: string argument required"),
        )),
    }
}
