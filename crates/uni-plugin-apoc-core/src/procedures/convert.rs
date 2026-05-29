// Rust guideline compliant
//! `apoc.convert.*` analogue — type conversions over Cypher primitives.
//!
//! Mirrors Neo4j's `apoc.convert.*` namespace. These overlap somewhat
//! with Cypher's built-in `toString`/`toInteger`/`toFloat`/`toBoolean`
//! functions but are exposed as procedures (`CALL ... YIELD`) for
//! parity and for the cases where users want explicit conversion
//! semantics in the procedure-result shape.
//!
//! Initial set: `convert.toString`, `convert.toBoolean`,
//! `convert.toInteger`, `convert.toFloat`. NULL-tolerant: invalid
//! coercions yield NULL rather than erroring.

use std::sync::{Arc, OnceLock};

use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
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

/// Register `uni.convert.*` procedures into `r`.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a qname is taken.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    for proc in ConvertProc::ALL {
        r.procedure(
            proc.qname(),
            proc.signature_cached().clone(),
            Arc::new(*proc),
        )?;
    }
    Ok(())
}

fn build_sig(yields_type: DataType, arg_doc: &str, docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![NamedArgType {
            name: smol_str::SmolStr::new("value"),
            ty: ArgType::CypherValue,
            default: None,
            doc: arg_doc.to_owned(),
        }],
        yields: vec![Field::new("result", yields_type, true)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

#[derive(Debug, Clone, Copy)]
#[allow(clippy::enum_variant_names)] // matches APOC's naming: toString/toBoolean/...
enum ConvertProc {
    ToString,
    ToBoolean,
    ToInteger,
    ToFloat,
}

impl ConvertProc {
    const ALL: &'static [Self] = &[
        Self::ToString,
        Self::ToBoolean,
        Self::ToInteger,
        Self::ToFloat,
    ];

    fn qname(&self) -> QName {
        match self {
            Self::ToString => QName::new("apoc-core", "convert.toString"),
            Self::ToBoolean => QName::new("apoc-core", "convert.toBoolean"),
            Self::ToInteger => QName::new("apoc-core", "convert.toInteger"),
            Self::ToFloat => QName::new("apoc-core", "convert.toFloat"),
        }
    }

    fn arg_doc(&self) -> &'static str {
        match self {
            Self::ToString => "Value to coerce to string.",
            Self::ToBoolean => "Value to coerce to boolean.",
            Self::ToInteger => "Value to coerce to integer.",
            Self::ToFloat => "Value to coerce to float.",
        }
    }

    /// Canonical docstring per variant. The `register_into` versions
    /// were descriptive; we keep them over the `OnceLock` placeholders.
    fn docs(&self) -> &'static str {
        match self {
            Self::ToString => {
                "Coerce a primitive to its default string representation; NULL on null input."
            }
            Self::ToBoolean => {
                "Coerce a primitive to boolean. Strings 'true'/'false' match (case-insensitive); other strings yield NULL."
            }
            Self::ToInteger => "Coerce a primitive to integer; NULL on failure.",
            Self::ToFloat => "Coerce a primitive to float; NULL on failure.",
        }
    }

    fn yields_type(&self) -> DataType {
        match self {
            Self::ToString => DataType::Utf8,
            Self::ToBoolean => DataType::Boolean,
            Self::ToInteger => DataType::Int64,
            Self::ToFloat => DataType::Float64,
        }
    }

    fn build_signature(&self) -> ProcedureSignature {
        build_sig(self.yields_type(), self.arg_doc(), self.docs())
    }

    fn signature_cached(&self) -> &'static ProcedureSignature {
        static TO_STRING_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static TO_BOOLEAN_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static TO_INTEGER_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static TO_FLOAT_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        match self {
            Self::ToString => TO_STRING_SIG.get_or_init(|| self.build_signature()),
            Self::ToBoolean => TO_BOOLEAN_SIG.get_or_init(|| self.build_signature()),
            Self::ToInteger => TO_INTEGER_SIG.get_or_init(|| self.build_signature()),
            Self::ToFloat => TO_FLOAT_SIG.get_or_init(|| self.build_signature()),
        }
    }
}

impl ProcedurePlugin for ConvertProc {
    fn signature(&self) -> &ProcedureSignature {
        self.signature_cached()
    }

    fn invoke(
        &self,
        _ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let val = args.first().ok_or_else(|| {
            FnError::new(FnError::CODE_TYPE_COERCION, "convert: missing argument")
        })?;
        let scalar = match val {
            ColumnarValue::Scalar(s) => s.clone(),
            ColumnarValue::Array(_) => {
                return Err(FnError::new(
                    FnError::CODE_TYPE_COERCION,
                    "convert: array argument not supported in single-call procedure shape",
                ));
            }
        };
        let (schema, array) = match self {
            Self::ToString => {
                let result: Option<String> = match scalar {
                    ScalarValue::Null => None,
                    ScalarValue::Boolean(Some(b)) => Some(b.to_string()),
                    ScalarValue::Int64(Some(i)) => Some(i.to_string()),
                    ScalarValue::Float64(Some(f)) => Some(f.to_string()),
                    ScalarValue::Utf8(Some(s)) => Some(s),
                    ScalarValue::LargeUtf8(Some(s)) => Some(s),
                    _ => None,
                };
                build_string_result(result)
            }
            Self::ToBoolean => {
                let result: Option<bool> = match scalar {
                    ScalarValue::Null => None,
                    ScalarValue::Boolean(b) => b,
                    ScalarValue::Int64(Some(i)) => Some(i != 0),
                    ScalarValue::Float64(Some(f)) => Some(f != 0.0),
                    ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                        match s.to_lowercase().as_str() {
                            "true" => Some(true),
                            "false" => Some(false),
                            _ => None,
                        }
                    }
                    _ => None,
                };
                build_bool_result(result)
            }
            Self::ToInteger => {
                let result: Option<i64> = match scalar {
                    ScalarValue::Null => None,
                    ScalarValue::Boolean(Some(b)) => Some(if b { 1 } else { 0 }),
                    ScalarValue::Int64(i) => i,
                    ScalarValue::Float64(Some(f)) if f.is_finite() => Some(f as i64),
                    ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                        s.trim().parse().ok()
                    }
                    _ => None,
                };
                build_int_result(result)
            }
            Self::ToFloat => {
                let result: Option<f64> = match scalar {
                    ScalarValue::Null => None,
                    ScalarValue::Boolean(Some(b)) => Some(if b { 1.0 } else { 0.0 }),
                    ScalarValue::Int64(Some(i)) => Some(i as f64),
                    ScalarValue::Float64(f) => f,
                    ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                        s.trim().parse().ok()
                    }
                    _ => None,
                };
                build_float_result(result)
            }
        };
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array])
            .map_err(|e| FnError::new(0x704, format!("convert: {e}")))?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(vec![Ok(batch)]),
        )))
    }
}

fn build_string_result(s: Option<String>) -> (SchemaRef, Arc<dyn Array>) {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "result",
        DataType::Utf8,
        true,
    )]));
    let arr = Arc::new(StringArray::from(vec![s])) as Arc<dyn Array>;
    (schema, arr)
}

fn build_bool_result(b: Option<bool>) -> (SchemaRef, Arc<dyn Array>) {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "result",
        DataType::Boolean,
        true,
    )]));
    let arr = Arc::new(BooleanArray::from(vec![b])) as Arc<dyn Array>;
    (schema, arr)
}

fn build_int_result(i: Option<i64>) -> (SchemaRef, Arc<dyn Array>) {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "result",
        DataType::Int64,
        true,
    )]));
    let arr = Arc::new(Int64Array::from(vec![i])) as Arc<dyn Array>;
    (schema, arr)
}

fn build_float_result(f: Option<f64>) -> (SchemaRef, Arc<dyn Array>) {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "result",
        DataType::Float64,
        true,
    )]));
    let arr = Arc::new(Float64Array::from(vec![f])) as Arc<dyn Array>;
    (schema, arr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    async fn invoke(proc: ConvertProc, scalar: ScalarValue) -> RecordBatch {
        let mut stream = proc
            .invoke(
                ProcedureContext::default(),
                &[ColumnarValue::Scalar(scalar)],
            )
            .unwrap();
        stream.next().await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn to_string_int() {
        let b = invoke(ConvertProc::ToString, ScalarValue::Int64(Some(42))).await;
        let a = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(a.value(0), "42");
    }

    #[tokio::test]
    async fn to_boolean_string_true() {
        let b = invoke(
            ConvertProc::ToBoolean,
            ScalarValue::Utf8(Some("TRUE".into())),
        )
        .await;
        let a = b.column(0).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(a.value(0));
    }

    #[tokio::test]
    async fn to_integer_float_truncates() {
        let b = invoke(ConvertProc::ToInteger, ScalarValue::Float64(Some(3.9))).await;
        let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(a.value(0), 3);
    }

    #[tokio::test]
    async fn to_float_int_widens() {
        let b = invoke(ConvertProc::ToFloat, ScalarValue::Int64(Some(7))).await;
        let a = b.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((a.value(0) - 7.0).abs() < 1e-12);
    }

    #[tokio::test]
    async fn to_integer_unparseable_string_returns_null() {
        let b = invoke(
            ConvertProc::ToInteger,
            ScalarValue::Utf8(Some("not-a-number".into())),
        )
        .await;
        let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(a.is_null(0));
    }
}
