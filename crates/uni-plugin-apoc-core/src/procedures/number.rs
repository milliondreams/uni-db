// Rust guideline compliant
//! `apoc.number.*` analogue — number formatting and parsing.
//!
//! Initial set: `number.parseInt`, `number.parseFloat`,
//! `number.toString`. More formatting (pattern-based) lands as a
//! follow-up — Rust has no direct equivalent of Java's `DecimalFormat`
//! patterns, so format procedures use the `format!` syntax.

use std::sync::{Arc, OnceLock};

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
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

/// Register `uni.number.*` procedures into `r`.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a qname is taken.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    for proc in NumberProc::ALL {
        r.procedure(
            proc.qname(),
            proc.signature_cached().clone(),
            Arc::new(*proc),
        )?;
    }
    Ok(())
}

fn parse_int_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![NamedArgType {
            name: smol_str::SmolStr::new("text"),
            ty: ArgType::Primitive(DataType::Utf8),
            default: None,
            doc: "String representation of an integer.".to_owned(),
        }],
        yields: vec![Field::new("result", DataType::Int64, true)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

fn parse_float_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![NamedArgType {
            name: smol_str::SmolStr::new("text"),
            ty: ArgType::Primitive(DataType::Utf8),
            default: None,
            doc: "String representation of a float.".to_owned(),
        }],
        yields: vec![Field::new("result", DataType::Float64, true)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

fn float_to_string_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![NamedArgType {
            name: smol_str::SmolStr::new("value"),
            ty: ArgType::Primitive(DataType::Float64),
            default: None,
            doc: "Numeric value to format.".to_owned(),
        }],
        yields: vec![Field::new("result", DataType::Utf8, false)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

#[derive(Debug, Clone, Copy)]
enum NumberProc {
    ParseInt,
    ParseFloat,
    ToString,
}

impl NumberProc {
    const ALL: &'static [Self] = &[Self::ParseInt, Self::ParseFloat, Self::ToString];

    fn qname(&self) -> QName {
        match self {
            Self::ParseInt => QName::new("apoc-core", "number.parseInt"),
            Self::ParseFloat => QName::new("apoc-core", "number.parseFloat"),
            Self::ToString => QName::new("apoc-core", "number.toString"),
        }
    }

    /// Canonical docstring per variant. The `register_into` versions
    /// were descriptive ("Parse a string as a 64-bit signed integer; ...")
    /// whereas the `OnceLock` versions were placeholders ("parseInt").
    /// We keep the descriptive ones as canonical.
    fn docs(&self) -> &'static str {
        match self {
            Self::ParseInt => "Parse a string as a 64-bit signed integer; NULL on failure.",
            Self::ParseFloat => "Parse a string as a 64-bit float; NULL on failure.",
            Self::ToString => "Format a number as its default string representation.",
        }
    }

    fn build_signature(&self) -> ProcedureSignature {
        match self {
            Self::ParseInt => parse_int_sig(self.docs()),
            Self::ParseFloat => parse_float_sig(self.docs()),
            Self::ToString => float_to_string_sig(self.docs()),
        }
    }

    fn signature_cached(&self) -> &'static ProcedureSignature {
        static PARSE_INT_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static PARSE_FLOAT_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static TO_STRING_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        match self {
            Self::ParseInt => PARSE_INT_SIG.get_or_init(|| self.build_signature()),
            Self::ParseFloat => PARSE_FLOAT_SIG.get_or_init(|| self.build_signature()),
            Self::ToString => TO_STRING_SIG.get_or_init(|| self.build_signature()),
        }
    }
}

impl ProcedurePlugin for NumberProc {
    fn signature(&self) -> &ProcedureSignature {
        self.signature_cached()
    }

    fn invoke(
        &self,
        _ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        match self {
            Self::ParseInt => {
                let s = extract_string(args, 0)?;
                let parsed: Option<i64> = s.trim().parse().ok();
                let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
                    "result",
                    DataType::Int64,
                    true,
                )]));
                let arr = Arc::new(Int64Array::from(vec![parsed])) as Arc<dyn Array>;
                let batch = RecordBatch::try_new(Arc::clone(&schema), vec![arr])
                    .map_err(|e| FnError::new(0x703, format!("number.parseInt: {e}")))?;
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    stream::iter(vec![Ok(batch)]),
                )))
            }
            Self::ParseFloat => {
                let s = extract_string(args, 0)?;
                let parsed: Option<f64> = s.trim().parse().ok();
                let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
                    "result",
                    DataType::Float64,
                    true,
                )]));
                let arr = Arc::new(Float64Array::from(vec![parsed])) as Arc<dyn Array>;
                let batch = RecordBatch::try_new(Arc::clone(&schema), vec![arr])
                    .map_err(|e| FnError::new(0x703, format!("number.parseFloat: {e}")))?;
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    stream::iter(vec![Ok(batch)]),
                )))
            }
            Self::ToString => {
                let v = extract_f64(args, 0)?;
                let s = format!("{v}");
                let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
                    "result",
                    DataType::Utf8,
                    false,
                )]));
                let arr = Arc::new(StringArray::from(vec![s])) as Arc<dyn Array>;
                let batch = RecordBatch::try_new(Arc::clone(&schema), vec![arr])
                    .map_err(|e| FnError::new(0x703, format!("number.toString: {e}")))?;
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    stream::iter(vec![Ok(batch)]),
                )))
            }
        }
    }
}

fn extract_string(args: &[ColumnarValue], idx: usize) -> Result<String, FnError> {
    let arg = args.get(idx).ok_or_else(|| {
        FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("number: expected argument at position {idx}"),
        )
    })?;
    match arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(s.clone()),
        ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => Ok(s.clone()),
        _ => Err(FnError::new(
            FnError::CODE_TYPE_COERCION,
            "number: string argument required",
        )),
    }
}

fn extract_f64(args: &[ColumnarValue], idx: usize) -> Result<f64, FnError> {
    let arg = args.get(idx).ok_or_else(|| {
        FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("number: expected argument at position {idx}"),
        )
    })?;
    match arg {
        ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => Ok(*v),
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => Ok(*v as f64),
        _ => Err(FnError::new(
            FnError::CODE_TYPE_COERCION,
            "number: numeric argument required",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn parse_int_succeeds_on_valid_int() {
        let cols = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "42".to_owned(),
        )))];
        let mut stream = NumberProc::ParseInt
            .invoke(ProcedureContext::default(), &cols)
            .unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 42);
    }

    #[tokio::test]
    async fn parse_int_returns_null_on_failure() {
        let cols = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "not a number".to_owned(),
        )))];
        let mut stream = NumberProc::ParseInt
            .invoke(ProcedureContext::default(), &cols)
            .unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(col.is_null(0));
    }

    #[tokio::test]
    async fn parse_float_succeeds_on_valid_float() {
        let cols = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "2.5".to_owned(),
        )))];
        let mut stream = NumberProc::ParseFloat
            .invoke(ProcedureContext::default(), &cols)
            .unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((col.value(0) - 2.5).abs() < 1e-12);
    }

    #[tokio::test]
    async fn to_string_formats_float() {
        let cols = vec![ColumnarValue::Scalar(ScalarValue::Float64(Some(2.5)))];
        let mut stream = NumberProc::ToString
            .invoke(ProcedureContext::default(), &cols)
            .unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "2.5");
    }
}
