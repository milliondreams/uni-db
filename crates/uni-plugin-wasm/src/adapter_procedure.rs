//! Procedure adapter — bridges a CM `procedure-plugin` instance to
//! [`ProcedurePlugin`].
//!
//! Port of `uni_plugin_extism::adapter_procedure`. Eagerly collects
//! the plugin's output IPC stream into a `RecordBatchStreamAdapter`.

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
use uni_plugin_wasm_rt::ipc::{decode_batches, encode_batch};

use crate::adapter_common::{acquire, ipc_to_fn_err};
use crate::loader::ProcedurePluginInstance;
use crate::pool::WasmInstancePool;

/// `ProcedurePlugin` adapter wrapping a CM procedure-plugin pool.
pub struct ComponentProcedure {
    pool: Arc<WasmInstancePool<ProcedurePluginInstance>>,
    qname: QName,
    sig: ProcedureSignature,
    args_schema: SchemaRef,
    yields_schema: SchemaRef,
}

impl std::fmt::Debug for ComponentProcedure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComponentProcedure")
            .field("qname", &self.qname)
            .field("signature", &self.sig)
            .finish_non_exhaustive()
    }
}

impl ComponentProcedure {
    /// Construct a new adapter against the supplied pool.
    #[must_use]
    pub fn new(
        pool: Arc<WasmInstancePool<ProcedurePluginInstance>>,
        qname: QName,
        sig: ProcedureSignature,
    ) -> Self {
        let args_schema = build_args_schema(&sig);
        let yields_schema = Arc::new(Schema::new(sig.yields.clone()));
        Self {
            pool,
            qname,
            sig,
            args_schema,
            yields_schema,
        }
    }
}

impl ProcedurePlugin for ComponentProcedure {
    fn signature(&self) -> &ProcedureSignature {
        &self.sig
    }

    fn invoke(
        &self,
        _ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
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
        let batch = RecordBatch::try_new(Arc::clone(&self.args_schema), arrays).map_err(|e| {
            FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!("procedure `{}` args RecordBatch: {e}", self.qname),
            )
        })?;
        let ipc = encode_batch(&batch).map_err(ipc_to_fn_err)?;

        let qname_str = self.qname.to_string();
        let mut leased = acquire(&self.pool, "procedure")?;
        let out_bytes = leased
            .get_mut()
            .invoke_procedure(&qname_str, &ipc)
            .map_err(|e| {
                FnError::new(
                    FnError::CODE_UNEXPECTED_NULL,
                    format!("wasm invoke_procedure `{qname_str}`: {e}"),
                )
            })?;
        drop(leased);

        let batches = decode_batches(&out_bytes).map_err(ipc_to_fn_err)?;
        for (i, b) in batches.iter().enumerate() {
            if b.schema().fields() != self.yields_schema.fields() {
                return Err(FnError::new(
                    FnError::CODE_TYPE_COERCION,
                    format!("procedure `{qname_str}` batch[{i}] schema mismatch"),
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
