//! `ComponentScalarFn` — bridges a CM `scalar-plugin` instance to
//! [`ScalarPluginFn`].
//!
//! Mirrors `uni-plugin-extism`'s `ExtismScalarFn`: encode args as
//! Arrow IPC, call the plugin's typed `invoke-scalar` export, decode
//! the returned IPC bytes back into a `ColumnarValue`. The pool's
//! cold-start factory rebuilds the wasmtime `Store<HostState>` + the
//! linked `ScalarPlugin` typed wrapper for each new instance.

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::logical_expr::ColumnarValue;
use uni_plugin::QName;
use uni_plugin::errors::FnError;
use uni_plugin::traits::scalar::{FnSignature, ScalarPluginFn};
use uni_plugin_wasm_rt::ipc::{decode_batch, encode_batch};

use crate::adapter_common::{acquire, ipc_to_fn_err};
use crate::loader::ScalarPluginInstance;
use crate::pool::WasmInstancePool;

/// Adapter that registers as `ScalarPluginFn` on the host's
/// `PluginRegistrar`. Holds an `Arc` to the pool so multiple
/// concurrent Cypher calls each acquire their own warm instance.
pub struct ComponentScalarFn {
    pool: Arc<WasmInstancePool<ScalarPluginInstance>>,
    qname: QName,
    sig: FnSignature,
}

impl std::fmt::Debug for ComponentScalarFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComponentScalarFn")
            .field("qname", &self.qname)
            .field("signature", &self.sig)
            .finish_non_exhaustive()
    }
}

impl ComponentScalarFn {
    /// Construct a new adapter against the supplied pool.
    #[must_use]
    pub fn new(
        pool: Arc<WasmInstancePool<ScalarPluginInstance>>,
        qname: QName,
        sig: FnSignature,
    ) -> Self {
        Self { pool, qname, sig }
    }

    fn args_to_batch(&self, args: &[ColumnarValue], rows: usize) -> Result<RecordBatch, FnError> {
        let arrays: Vec<ArrayRef> = args
            .iter()
            .map(|c| {
                c.clone().into_array(rows).map_err(|e| {
                    FnError::new(
                        FnError::CODE_TYPE_COERCION,
                        format!("ColumnarValue::into_array: {e}"),
                    )
                })
            })
            .collect::<Result<_, _>>()?;
        let fields: Vec<Field> = arrays
            .iter()
            .enumerate()
            .map(|(i, a)| Field::new(format!("arg{i}"), a.data_type().clone(), true))
            .collect();
        let schema: SchemaRef = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays).map_err(|e| {
            FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!("RecordBatch assembly: {e}"),
            )
        })
    }
}

impl ScalarPluginFn for ComponentScalarFn {
    fn signature(&self) -> &FnSignature {
        &self.sig
    }

    fn invoke(&self, args: &[ColumnarValue], rows: usize) -> Result<ColumnarValue, FnError> {
        let batch = self.args_to_batch(args, rows)?;
        let bytes = encode_batch(&batch).map_err(ipc_to_fn_err)?;

        let mut leased = acquire(&self.pool, "plugin")?;
        let qname_str = self.qname.to_string();
        let out_bytes: Vec<u8> =
            leased
                .get_mut()
                .invoke_scalar(&qname_str, &bytes)
                .map_err(|e| {
                    FnError::new(
                        FnError::CODE_UNEXPECTED_NULL,
                        format!("wasm component invoke_scalar `{qname_str}`: {e}"),
                    )
                })?;
        drop(leased);

        let out_batch = decode_batch(&out_bytes)
            .map_err(ipc_to_fn_err)?
            .ok_or_else(|| {
                FnError::new(
                    FnError::CODE_UNEXPECTED_NULL,
                    format!("wasm component `{qname_str}` returned empty IPC stream"),
                )
            })?;

        if out_batch.num_columns() != 1 {
            return Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!(
                    "wasm component `{qname_str}` returned {} columns; scalar fns must return 1",
                    out_batch.num_columns()
                ),
            ));
        }
        Ok(ColumnarValue::Array(out_batch.column(0).clone()))
    }
}
