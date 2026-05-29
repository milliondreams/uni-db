// Rust guideline compliant
//! `apoc.bitwise.*` analogue — `uni.bitwise.*` procedures.
//!
//! Mirrors Neo4j's `apoc.bitwise.*` namespace. Lives in
//! `uni-plugin-apoc-core` because it is APOC-equivalent content, not a
//! replacement for a closed enum in the core engine.
//!
//! Ports the six legacy bitwise procedures
//! (`and`, `or`, `xor`, `not`, `shiftLeft`, `shiftRight`) from the
//! hardcoded match in `crates/uni-query/src/query/df_graph/procedure_call.rs`
//! to `ProcedurePlugin` registrations. Each procedure yields one row with
//! a single `result` column.

use std::sync::{Arc, OnceLock};

use arrow_array::{Array, Int64Array, RecordBatch};
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

/// Register `uni.bitwise.*` procedures into `r`.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a qname is taken.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    for proc in BitwiseProc::ALL {
        r.procedure(
            proc.qname(),
            proc.signature_cached().clone(),
            Arc::new(*proc),
        )?;
    }
    Ok(())
}

fn unary_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![NamedArgType {
            name: smol_str::SmolStr::new("x"),
            ty: ArgType::Primitive(DataType::Int64),
            default: None,
            doc: "Integer input.".to_owned(),
        }],
        yields: vec![Field::new("result", DataType::Int64, false)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

fn binary_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![
            NamedArgType {
                name: smol_str::SmolStr::new("a"),
                ty: ArgType::Primitive(DataType::Int64),
                default: None,
                doc: "First integer.".to_owned(),
            },
            NamedArgType {
                name: smol_str::SmolStr::new("b"),
                ty: ArgType::Primitive(DataType::Int64),
                default: None,
                doc: "Second integer.".to_owned(),
            },
        ],
        yields: vec![Field::new("result", DataType::Int64, false)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

/// Implementations of all bitwise procedures via one discriminant.
#[derive(Debug, Clone, Copy)]
enum BitwiseProc {
    And,
    Or,
    Xor,
    Not,
    ShiftLeft,
    ShiftRight,
}

impl BitwiseProc {
    const ALL: &'static [Self] = &[
        Self::And,
        Self::Or,
        Self::Xor,
        Self::Not,
        Self::ShiftLeft,
        Self::ShiftRight,
    ];

    fn qname(&self) -> QName {
        match self {
            Self::And => QName::new("apoc-core", "bitwise.and"),
            Self::Or => QName::new("apoc-core", "bitwise.or"),
            Self::Xor => QName::new("apoc-core", "bitwise.xor"),
            Self::Not => QName::new("apoc-core", "bitwise.not"),
            Self::ShiftLeft => QName::new("apoc-core", "bitwise.shiftLeft"),
            Self::ShiftRight => QName::new("apoc-core", "bitwise.shiftRight"),
        }
    }

    /// Canonical docstring per variant. The previous `register_into`
    /// strings were more descriptive than the `OnceLock` fallbacks
    /// ("Bitwise AND." vs "Bitwise AND of two integers."); we keep the
    /// descriptive form here.
    fn docs(&self) -> &'static str {
        match self {
            Self::And => "Bitwise AND of two integers.",
            Self::Or => "Bitwise OR of two integers.",
            Self::Xor => "Bitwise XOR of two integers.",
            Self::Not => "Bitwise NOT of an integer.",
            Self::ShiftLeft => "Bitwise left-shift of an integer.",
            Self::ShiftRight => "Bitwise right-shift of an integer.",
        }
    }

    fn build_signature(&self) -> ProcedureSignature {
        match self {
            Self::Not => unary_sig(self.docs()),
            _ => binary_sig(self.docs()),
        }
    }

    fn signature_cached(&self) -> &'static ProcedureSignature {
        static AND_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static OR_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static XOR_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static NOT_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static SHL_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static SHR_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        match self {
            Self::And => AND_SIG.get_or_init(|| self.build_signature()),
            Self::Or => OR_SIG.get_or_init(|| self.build_signature()),
            Self::Xor => XOR_SIG.get_or_init(|| self.build_signature()),
            Self::Not => NOT_SIG.get_or_init(|| self.build_signature()),
            Self::ShiftLeft => SHL_SIG.get_or_init(|| self.build_signature()),
            Self::ShiftRight => SHR_SIG.get_or_init(|| self.build_signature()),
        }
    }
}

impl ProcedurePlugin for BitwiseProc {
    fn signature(&self) -> &ProcedureSignature {
        self.signature_cached()
    }

    fn invoke(
        &self,
        _ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let result = match self {
            Self::Not => {
                let a = extract_i64(args, 0)?;
                !a
            }
            other => {
                let a = extract_i64(args, 0)?;
                let b = extract_i64(args, 1)?;
                match other {
                    Self::And => a & b,
                    Self::Or => a | b,
                    Self::Xor => a ^ b,
                    Self::ShiftLeft => a.wrapping_shl((b & 63) as u32),
                    Self::ShiftRight => a.wrapping_shr((b & 63) as u32),
                    Self::Not => unreachable!(),
                }
            }
        };

        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "result",
            DataType::Int64,
            false,
        )]));
        let arr = Arc::new(Int64Array::from(vec![result])) as Arc<dyn Array>;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![arr])
            .map_err(|e| FnError::new(0x700, format!("bitwise: {e}")))?;
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
            format!("bitwise: expected argument at position {idx}"),
        )
    })?;
    match arg {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => Ok(*v),
        ColumnarValue::Array(arr) => {
            let a = arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| FnError::new(FnError::CODE_TYPE_COERCION, "expected Int64Array"))?;
            if a.is_empty() || a.is_null(0) {
                Err(FnError::new(
                    FnError::CODE_UNEXPECTED_NULL,
                    "bitwise: integer argument must not be null",
                ))
            } else {
                Ok(a.value(0))
            }
        }
        _ => Err(FnError::new(
            FnError::CODE_TYPE_COERCION,
            "bitwise: integer argument required",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    async fn invoke_one(proc: BitwiseProc, args: Vec<i64>) -> i64 {
        let cols: Vec<ColumnarValue> = args
            .into_iter()
            .map(|v| ColumnarValue::Scalar(ScalarValue::Int64(Some(v))))
            .collect();
        let mut stream = proc.invoke(ProcedureContext::default(), &cols).unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        col.value(0)
    }

    #[tokio::test]
    async fn and_computes_bitwise_and() {
        assert_eq!(
            invoke_one(BitwiseProc::And, vec![0b1100, 0b1010]).await,
            0b1000
        );
    }

    #[tokio::test]
    async fn or_computes_bitwise_or() {
        assert_eq!(
            invoke_one(BitwiseProc::Or, vec![0b1100, 0b1010]).await,
            0b1110
        );
    }

    #[tokio::test]
    async fn xor_computes_bitwise_xor() {
        assert_eq!(
            invoke_one(BitwiseProc::Xor, vec![0b1100, 0b1010]).await,
            0b0110
        );
    }

    #[tokio::test]
    async fn not_computes_bitwise_not() {
        assert_eq!(invoke_one(BitwiseProc::Not, vec![0]).await, !0_i64);
    }

    #[tokio::test]
    async fn shift_left_shifts_left() {
        assert_eq!(invoke_one(BitwiseProc::ShiftLeft, vec![1, 4]).await, 16);
    }

    #[tokio::test]
    async fn shift_right_shifts_right() {
        assert_eq!(invoke_one(BitwiseProc::ShiftRight, vec![16, 2]).await, 4);
    }

    #[tokio::test]
    async fn shift_masks_by_63() {
        // Wrapping shift with mask matches Rust semantics for i64.
        assert_eq!(invoke_one(BitwiseProc::ShiftLeft, vec![1, 65]).await, 2);
    }
}
