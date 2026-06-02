//! Procedure adapter — bridges Extism procedure plugins to
//! [`ProcedurePlugin`].
//!
//! ## Wire contract (per qname `q`)
//!
//! - `proc_<q>_invoke` — input is an Arrow IPC stream with one 1-row
//!   batch whose columns match `proc.signature().args`. Output is an
//!   Arrow IPC stream containing zero or more batches, each matching
//!   the declared `yields` schema. M6a.2 collects every output batch
//!   eagerly and serves them from an in-memory stream; true streaming
//!   via a `host_yield` callback lands with M6b (host imports under
//!   the Component Model).

// Rust guideline compliant

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;
use uni_plugin::QName;
use uni_plugin::adapter_common::arrow_types::argtype_to_arrow;
use uni_plugin::errors::FnError;
use uni_plugin::traits::procedure::{ProcedureContext, ProcedurePlugin, ProcedureSignature};

use crate::adapter_common::{acquire, extism_err_to_fn_err, sanitize_qname};
use crate::ipc::{decode_batches, encode_batch};
use crate::pool::ExtismInstancePool;

/// Plugin-side procedure-invoke export name from a qname.
///
/// `.` in qnames is replaced with `_` so plugin authors can use
/// idiomatic Rust function names (Rust identifiers can't contain
/// `.`). Matches the scalar / aggregate sanitization.
#[must_use]
pub(crate) fn proc_invoke_export_name(qname: &QName) -> String {
    format!("proc_{}_invoke", sanitize_qname(qname))
}

/// `ProcedurePlugin` adapter wrapping an Extism plugin pool.
pub struct ExtismProcedure {
    pool: Arc<ExtismInstancePool<extism::Plugin>>,
    qname: QName,
    invoke_export: String,
    sig: ProcedureSignature,
    args_schema: SchemaRef,
    yields_schema: SchemaRef,
}

impl std::fmt::Debug for ExtismProcedure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExtismProcedure")
            .field("qname", &self.qname)
            .field("signature", &self.sig)
            .finish_non_exhaustive()
    }
}

impl ExtismProcedure {
    /// Construct a new adapter against the supplied pool.
    #[must_use]
    pub fn new(
        pool: Arc<ExtismInstancePool<extism::Plugin>>,
        qname: QName,
        sig: ProcedureSignature,
    ) -> Self {
        let invoke_export = proc_invoke_export_name(&qname);
        let args_schema = build_args_schema(&sig);
        let yields_schema = Arc::new(Schema::new(sig.yields.clone()));
        Self {
            pool,
            qname,
            invoke_export,
            sig,
            args_schema,
            yields_schema,
        }
    }
}

impl ProcedurePlugin for ExtismProcedure {
    fn signature(&self) -> &ProcedureSignature {
        &self.sig
    }

    fn invoke(
        &self,
        _ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        // M6a.2: procedures receive scalar args, packed into a 1-row
        // RecordBatch (one column per arg). Plugins decode and produce
        // a stream of `yields`-shaped batches eagerly.
        let arrays: Vec<arrow::array::ArrayRef> = args
            .iter()
            .map(|c| {
                c.clone().into_array(1).map_err(|e| {
                    FnError::new(
                        FnError::CODE_TYPE_COERCION,
                        format!("ColumnarValue::into_array: {e}"),
                    )
                })
            })
            .collect::<Result<_, _>>()?;
        if arrays.len() != self.args_schema.fields().len() {
            return Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!(
                    "procedure `{}` expected {} args; got {}",
                    self.qname,
                    self.args_schema.fields().len(),
                    arrays.len()
                ),
            ));
        }
        let args_batch =
            RecordBatch::try_new(Arc::clone(&self.args_schema), arrays).map_err(|e| {
                FnError::new(
                    FnError::CODE_TYPE_COERCION,
                    format!("procedure `{}` args RecordBatch: {e}", self.qname),
                )
            })?;
        let ipc = encode_batch(&args_batch).map_err(extism_err_to_fn_err)?;

        let mut leased = acquire(&self.pool)?;
        let out_bytes: Vec<u8> = leased
            .get_mut()
            .call::<&[u8], &[u8]>(&self.invoke_export, &ipc)
            .map_err(|e| {
                FnError::new(
                    FnError::CODE_UNEXPECTED_NULL,
                    format!("extism call `{}` failed: {e}", self.invoke_export),
                )
            })?
            .to_vec();
        drop(leased);

        let batches = decode_batches(&out_bytes).map_err(extism_err_to_fn_err)?;

        // Validate every batch matches the declared yields schema.
        for (i, b) in batches.iter().enumerate() {
            if b.schema().fields() != self.yields_schema.fields() {
                return Err(FnError::new(
                    FnError::CODE_TYPE_COERCION,
                    format!(
                        "procedure `{}` batch[{i}] schema mismatch: got {:?}, expected {:?}",
                        self.qname,
                        b.schema().fields(),
                        self.yields_schema.fields()
                    ),
                ));
            }
        }

        let schema = Arc::clone(&self.yields_schema);
        let stream = stream::iter(batches.into_iter().map(Ok));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn build_args_schema(sig: &ProcedureSignature) -> SchemaRef {
    let fields: Vec<Field> = sig
        .args
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("arg{i}"), argtype_to_arrow(&a.ty), true))
        .collect();
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;
    use uni_plugin::capability::SideEffects;
    use uni_plugin::traits::procedure::{NamedArgType, ProcedureMode};
    use uni_plugin::traits::scalar::ArgType;

    fn sample_sig() -> ProcedureSignature {
        ProcedureSignature {
            args: vec![NamedArgType {
                name: "arg0".into(),
                ty: ArgType::Primitive(DataType::Utf8),
                default: None,
                doc: String::new(),
            }],
            yields: vec![
                Field::new("yield0", DataType::Int64, true),
                Field::new("yield1", DataType::Utf8, true),
            ],
            mode: ProcedureMode::Read,
            side_effects: SideEffects::default(),
            retry_contract: None,
            batch_input: None,
            docs: String::new(),
        }
    }

    #[test]
    fn export_name_format() {
        let q = QName::parse("myorg.scan").expect("valid");
        assert_eq!(proc_invoke_export_name(&q), "proc_myorg_scan_invoke");
    }

    #[test]
    fn build_args_schema_matches_named_args() {
        let sig = sample_sig();
        let schema = build_args_schema(&sig);
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "arg0");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    }
}
