//! `uni.system.*` procedures — health, diagnostics, framework probes.

use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;
use uni_plugin::traits::procedure::{
    NamedArgType, ProcedureContext, ProcedureMode, ProcedurePlugin, ProcedureSignature,
};
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::{FnError, PluginError, PluginRegistrar, QName};

/// Register `uni.system.*` procedures into `r`.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a qname is taken.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    let sig = ProcedureSignature {
        args: vec![NamedArgType {
            name: smol_str::SmolStr::new("message"),
            ty: ArgType::Primitive(DataType::Utf8),
            default: None,
            doc: "Message to echo back unchanged.".to_owned(),
        }],
        yields: vec![Field::new("echo", DataType::Utf8, false)],
        mode: ProcedureMode::Read,
        side_effects: uni_plugin::SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: "Health-check procedure: returns its `message` argument unchanged.".to_owned(),
    };
    r.procedure(
        QName::new("builtin", "system.echo"),
        sig,
        Arc::new(EchoProcedure),
    )?;
    Ok(())
}

/// `uni.system.echo(message)` — returns the message unchanged.
///
/// The simplest possible procedure: a single-row, single-column `YIELD`
/// that demonstrates the [`ProcedurePlugin`] implementation pattern.
#[derive(Debug)]
pub struct EchoProcedure;

impl ProcedurePlugin for EchoProcedure {
    fn signature(&self) -> &ProcedureSignature {
        static SIG: std::sync::OnceLock<ProcedureSignature> = std::sync::OnceLock::new();
        SIG.get_or_init(|| ProcedureSignature {
            args: vec![NamedArgType {
                name: smol_str::SmolStr::new("message"),
                ty: ArgType::Primitive(DataType::Utf8),
                default: None,
                doc: "Message to echo.".to_owned(),
            }],
            yields: vec![Field::new("echo", DataType::Utf8, false)],
            mode: ProcedureMode::Read,
            side_effects: uni_plugin::SideEffects::ReadOnly,
            retry_contract: None,
            batch_input: None,
            docs: "uni.system.echo".to_owned(),
        })
    }

    fn invoke(
        &self,
        _ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let message = extract_first_string(args)?;
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("echo", DataType::Utf8, false)]));
        let arr = Arc::new(StringArray::from(vec![message])) as Arc<dyn Array>;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![arr]).map_err(|e| {
            FnError::new(
                0x600,
                format!("uni.system.echo: failed to build output batch: {e}"),
            )
        })?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(vec![Ok(batch)]),
        )))
    }
}

fn extract_first_string(args: &[ColumnarValue]) -> Result<String, FnError> {
    use datafusion::scalar::ScalarValue;
    match args.first() {
        Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))) => Ok(s.clone()),
        Some(ColumnarValue::Array(arr)) => {
            let a = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                FnError::new(
                    FnError::CODE_TYPE_COERCION,
                    "uni.system.echo: expected Utf8 first argument",
                )
            })?;
            if a.is_empty() || a.is_null(0) {
                Err(FnError::new(
                    FnError::CODE_UNEXPECTED_NULL,
                    "uni.system.echo: message argument must not be null",
                ))
            } else {
                Ok(a.value(0).to_owned())
            }
        }
        _ => Err(FnError::new(
            FnError::CODE_TYPE_COERCION,
            "uni.system.echo: missing or non-Utf8 message argument",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::scalar::ScalarValue;
    use futures::StreamExt;

    #[tokio::test]
    async fn echo_returns_message_unchanged() {
        let p = EchoProcedure;
        let args = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "hello".to_owned(),
        )))];
        let mut stream = p
            .invoke(ProcedureContext::default(), &args)
            .expect("invoke");
        let batch = stream.next().await.unwrap().expect("batch");
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "hello");
    }

    #[test]
    fn echo_signature_is_read_mode() {
        let p = EchoProcedure;
        assert_eq!(p.signature().mode, ProcedureMode::Read);
    }
}
