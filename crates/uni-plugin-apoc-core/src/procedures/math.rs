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

use std::sync::OnceLock;

use arrow_schema::{DataType, Field};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use uni_plugin::traits::procedure::{
    NamedArgType, ProcedureContext, ProcedureMode, ProcedurePlugin, ProcedureSignature,
};
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::{FnError, PluginError, PluginRegistrar, QName, SideEffects};

use super::support::{self, ApocProc, CODE_MATH_DOMAIN, FloatToInt, batch_err, int_result};

/// Register `uni.math.*` procedures into `r`.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a qname is taken.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    support::register_all::<MathProc>(r)
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

    /// Compute the (typed) result for this procedure.
    fn eval(&self, args: &[ColumnarValue]) -> Result<MathOutput, FnError> {
        // `math` truncates a float passed where an integer is expected and
        // does not accept array arguments (see `support::extract_i64`).
        let f64_arg = |idx| support::extract_f64(args, idx, "math");
        let i64_arg = |idx| support::extract_i64(args, idx, "math", FloatToInt::Truncate, false);
        let out = match self {
            Self::MaxLong => MathOutput::Int(i64::MAX),
            Self::MinLong => MathOutput::Int(i64::MIN),
            Self::Sigmoid => MathOutput::Float(1.0 / (1.0 + (-f64_arg(0)?).exp())),
            Self::Tanh => MathOutput::Float(f64_arg(0)?.tanh()),
            Self::Cosh => MathOutput::Float(f64_arg(0)?.cosh()),
            Self::Sinh => MathOutput::Float(f64_arg(0)?.sinh()),
            Self::Coth => {
                let x = f64_arg(0)?;
                let s = x.sinh();
                if s == 0.0 {
                    return Err(FnError::new(
                        CODE_MATH_DOMAIN,
                        "math.coth: cosh(0)/sinh(0) is undefined",
                    ));
                }
                MathOutput::Float(x.cosh() / s)
            }
            Self::MaxDouble => MathOutput::Float(f64::MAX),
            Self::MinDouble => MathOutput::Float(f64::MIN_POSITIVE),
            Self::Round => {
                let value = f64_arg(0)?;
                let precision = i64_arg(1)?;
                let scale = 10f64.powi(precision as i32);
                MathOutput::Float((value * scale).round() / scale)
            }
        };
        Ok(out)
    }
}

/// A math procedure produces either an integer constant or a float value.
enum MathOutput {
    Int(i64),
    Float(f64),
}

impl ApocProc for MathProc {
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

    fn index(&self) -> usize {
        *self as usize
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
}

impl ProcedurePlugin for MathProc {
    fn signature(&self) -> &ProcedureSignature {
        static CACHE: OnceLock<Vec<ProcedureSignature>> = OnceLock::new();
        support::cached_signature(&CACHE, self)
    }

    fn invoke(
        &self,
        _ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let (schema, array) = match self.eval(args)? {
            MathOutput::Int(n) => int_result(n),
            MathOutput::Float(v) => support::float_result(v),
        };
        support::one_row_stream(schema, array, batch_err::MATH, "math")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Float64Array;
    use datafusion::scalar::ScalarValue;
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
        assert_eq!(err.code, CODE_MATH_DOMAIN);
    }
}
