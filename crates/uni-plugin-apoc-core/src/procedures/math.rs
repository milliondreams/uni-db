// Rust guideline compliant
//! `apoc.math.*` analogue — numeric helpers beyond core Cypher built-ins.
//!
//! Mirrors Neo4j's `apoc.math.*` namespace. Lives in `uni-plugin-apoc-core`
//! because these are perf-critical (called per row in numeric workloads)
//! and operate on primitive f64 values with no host integration needed.
//!
//! Initial set: `math.sigmoid`, `math.tanh`, `math.cosh`, `math.sinh`,
//! `math.coth`, `math.round`. Cypher already ships abs/ceil/floor/exp/
//! log/sqrt/trig natively via `expr_fn::*` translation; this namespace
//! covers the analytics-flavoured extras.

use std::sync::{Arc, OnceLock};

use arrow_array::{Array, Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::scalar::ScalarValue;
use futures::stream;
use uni_plugin::traits::procedure::{
    NamedArgType, ProcedureContext, ProcedureMode, ProcedurePlugin, ProcedureSignature,
};
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::{FnError, PluginError, PluginRegistrar, QName, SideEffects};

/// Register `uni.math.*` procedures into `r`.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a qname is taken.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    for proc in MathProc::ALL {
        r.procedure(
            proc.qname(),
            proc.signature_cached().clone(),
            Arc::new(*proc),
        )?;
    }
    Ok(())
}

fn nullary_int_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![],
        yields: vec![Field::new("result", DataType::Int64, false)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

fn nullary_float_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![],
        yields: vec![Field::new("result", DataType::Float64, false)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

fn round_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![
            NamedArgType {
                name: smol_str::SmolStr::new("value"),
                ty: ArgType::Primitive(DataType::Float64),
                default: None,
                doc: "Value to round.".to_owned(),
            },
            NamedArgType {
                name: smol_str::SmolStr::new("precision"),
                ty: ArgType::Primitive(DataType::Int64),
                default: None,
                doc: "Number of decimal places.".to_owned(),
            },
        ],
        yields: vec![Field::new("result", DataType::Float64, false)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

fn unary_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![NamedArgType {
            name: smol_str::SmolStr::new("x"),
            ty: ArgType::Primitive(DataType::Float64),
            default: None,
            doc: "Input value.".to_owned(),
        }],
        yields: vec![Field::new("result", DataType::Float64, false)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

/// All math procedures via one discriminant.
#[derive(Debug, Clone, Copy)]
enum MathProc {
    Sigmoid,
    Tanh,
    Cosh,
    Sinh,
    Coth,
    MaxLong,
    MinLong,
    MaxDouble,
    MinDouble,
    Round,
}

impl MathProc {
    const ALL: &'static [Self] = &[
        Self::Sigmoid,
        Self::Tanh,
        Self::Cosh,
        Self::Sinh,
        Self::Coth,
        Self::MaxLong,
        Self::MinLong,
        Self::MaxDouble,
        Self::MinDouble,
        Self::Round,
    ];

    fn qname(&self) -> QName {
        match self {
            Self::Sigmoid => QName::new("apoc-core", "math.sigmoid"),
            Self::Tanh => QName::new("apoc-core", "math.tanh"),
            Self::Cosh => QName::new("apoc-core", "math.cosh"),
            Self::Sinh => QName::new("apoc-core", "math.sinh"),
            Self::Coth => QName::new("apoc-core", "math.coth"),
            Self::MaxLong => QName::new("apoc-core", "math.maxLong"),
            Self::MinLong => QName::new("apoc-core", "math.minLong"),
            Self::MaxDouble => QName::new("apoc-core", "math.maxDouble"),
            Self::MinDouble => QName::new("apoc-core", "math.minDouble"),
            Self::Round => QName::new("apoc-core", "math.round"),
        }
    }

    /// Canonical docstring per variant. Picked the descriptive
    /// `register_into` strings over the placeholder
    /// `OnceLock` ones (e.g. "sigmoid" -> "Logistic sigmoid 1/(1 + exp(-x)).").
    fn docs(&self) -> &'static str {
        match self {
            Self::Sigmoid => "Logistic sigmoid 1/(1 + exp(-x)).",
            Self::Tanh => "Hyperbolic tangent.",
            Self::Cosh => "Hyperbolic cosine.",
            Self::Sinh => "Hyperbolic sine.",
            Self::Coth => "Hyperbolic cotangent cosh(x)/sinh(x); errors at x=0.",
            Self::MaxLong => "Maximum representable signed 64-bit integer.",
            Self::MinLong => "Minimum representable signed 64-bit integer.",
            Self::MaxDouble => "Maximum finite 64-bit float (≈1.797e308).",
            Self::MinDouble => "Minimum positive normal 64-bit float (≈2.225e-308).",
            Self::Round => "Round `value` to `precision` decimal places (half-up).",
        }
    }

    fn build_signature(&self) -> ProcedureSignature {
        match self {
            Self::Sigmoid | Self::Tanh | Self::Cosh | Self::Sinh | Self::Coth => {
                unary_sig(self.docs())
            }
            Self::MaxLong | Self::MinLong => nullary_int_sig(self.docs()),
            Self::MaxDouble | Self::MinDouble => nullary_float_sig(self.docs()),
            Self::Round => round_sig(self.docs()),
        }
    }

    fn signature_cached(&self) -> &'static ProcedureSignature {
        static SIGMOID_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static TANH_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static COSH_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static SINH_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static COTH_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static MAX_LONG_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static MIN_LONG_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static MAX_DOUBLE_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static MIN_DOUBLE_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static ROUND_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        match self {
            Self::Sigmoid => SIGMOID_SIG.get_or_init(|| self.build_signature()),
            Self::Tanh => TANH_SIG.get_or_init(|| self.build_signature()),
            Self::Cosh => COSH_SIG.get_or_init(|| self.build_signature()),
            Self::Sinh => SINH_SIG.get_or_init(|| self.build_signature()),
            Self::Coth => COTH_SIG.get_or_init(|| self.build_signature()),
            Self::MaxLong => MAX_LONG_SIG.get_or_init(|| self.build_signature()),
            Self::MinLong => MIN_LONG_SIG.get_or_init(|| self.build_signature()),
            Self::MaxDouble => MAX_DOUBLE_SIG.get_or_init(|| self.build_signature()),
            Self::MinDouble => MIN_DOUBLE_SIG.get_or_init(|| self.build_signature()),
            Self::Round => ROUND_SIG.get_or_init(|| self.build_signature()),
        }
    }
}

impl ProcedurePlugin for MathProc {
    fn signature(&self) -> &ProcedureSignature {
        self.signature_cached()
    }

    fn invoke(
        &self,
        _ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        use arrow_array::Int64Array;
        // Nullary integer constants take the int path; everything else
        // returns a Float64 result.
        let (schema, array): (SchemaRef, Arc<dyn Array>) = match self {
            Self::MaxLong => {
                let s: SchemaRef = Arc::new(Schema::new(vec![Field::new(
                    "result",
                    DataType::Int64,
                    false,
                )]));
                let a = Arc::new(Int64Array::from(vec![i64::MAX])) as Arc<dyn Array>;
                (s, a)
            }
            Self::MinLong => {
                let s: SchemaRef = Arc::new(Schema::new(vec![Field::new(
                    "result",
                    DataType::Int64,
                    false,
                )]));
                let a = Arc::new(Int64Array::from(vec![i64::MIN])) as Arc<dyn Array>;
                (s, a)
            }
            _ => {
                let result = match self {
                    Self::Sigmoid => 1.0 / (1.0 + (-extract_f64(args, 0)?).exp()),
                    Self::Tanh => extract_f64(args, 0)?.tanh(),
                    Self::Cosh => extract_f64(args, 0)?.cosh(),
                    Self::Sinh => extract_f64(args, 0)?.sinh(),
                    Self::Coth => {
                        let x = extract_f64(args, 0)?;
                        let s = x.sinh();
                        if s == 0.0 {
                            return Err(FnError::new(
                                0x800,
                                "math.coth: cosh(0)/sinh(0) is undefined",
                            ));
                        }
                        x.cosh() / s
                    }
                    Self::MaxDouble => f64::MAX,
                    Self::MinDouble => f64::MIN_POSITIVE,
                    Self::Round => {
                        let value = extract_f64(args, 0)?;
                        let precision = extract_i64(args, 1)?;
                        let scale = 10f64.powi(precision as i32);
                        (value * scale).round() / scale
                    }
                    Self::MaxLong | Self::MinLong => unreachable!(),
                };
                let s: SchemaRef = Arc::new(Schema::new(vec![Field::new(
                    "result",
                    DataType::Float64,
                    false,
                )]));
                let a = Arc::new(Float64Array::from(vec![result])) as Arc<dyn Array>;
                (s, a)
            }
        };

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array])
            .map_err(|e| FnError::new(0x702, format!("math: {e}")))?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(vec![Ok(batch)]),
        )))
    }
}

fn extract_i64(args: &[ColumnarValue], idx: usize) -> Result<i64, FnError> {
    let arg = args.get(idx).ok_or_else(|| {
        FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("math: expected argument at position {idx}"),
        )
    })?;
    match arg {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => Ok(*v),
        ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => Ok(*v as i64),
        _ => Err(FnError::new(
            FnError::CODE_TYPE_COERCION,
            "math: integer argument required",
        )),
    }
}

fn extract_f64(args: &[ColumnarValue], idx: usize) -> Result<f64, FnError> {
    let arg = args.get(idx).ok_or_else(|| {
        FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("math: expected argument at position {idx}"),
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
                        "math: numeric argument must not be null",
                    ))
                } else {
                    Ok(a.value(0))
                }
            } else {
                Err(FnError::new(
                    FnError::CODE_TYPE_COERCION,
                    "math: expected Float64Array",
                ))
            }
        }
        _ => Err(FnError::new(
            FnError::CODE_TYPE_COERCION,
            "math: numeric argument required",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    async fn invoke_one(proc: MathProc, x: f64) -> f64 {
        let cols: Vec<ColumnarValue> = vec![ColumnarValue::Scalar(ScalarValue::Float64(Some(x)))];
        let mut stream = proc.invoke(ProcedureContext::default(), &cols).unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        col.value(0)
    }

    #[tokio::test]
    async fn sigmoid_zero_is_half() {
        let v = invoke_one(MathProc::Sigmoid, 0.0).await;
        assert!((v - 0.5).abs() < 1e-12);
    }

    #[tokio::test]
    async fn sigmoid_large_positive_is_near_one() {
        assert!(invoke_one(MathProc::Sigmoid, 50.0).await > 1.0 - 1e-12);
    }

    #[tokio::test]
    async fn tanh_zero_is_zero() {
        assert!(invoke_one(MathProc::Tanh, 0.0).await.abs() < 1e-12);
    }

    #[tokio::test]
    async fn cosh_zero_is_one() {
        let v = invoke_one(MathProc::Cosh, 0.0).await;
        assert!((v - 1.0).abs() < 1e-12);
    }

    #[tokio::test]
    async fn sinh_zero_is_zero() {
        assert!(invoke_one(MathProc::Sinh, 0.0).await.abs() < 1e-12);
    }

    #[tokio::test]
    async fn coth_at_zero_errors() {
        let cols: Vec<ColumnarValue> = vec![ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0)))];
        let err = MathProc::Coth
            .invoke(ProcedureContext::default(), &cols)
            .err()
            .expect("coth(0) must error");
        assert_eq!(err.code, 0x800);
    }
}
