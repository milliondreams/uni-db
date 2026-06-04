//! Adapters bridging Extism plugin exports to the
//! [`uni_plugin`] capability traits.
//!
//! For each [`crate::exports::RegistrationEntry`] kind, an adapter wraps
//! the live [`extism::Plugin`] in the trait object the executor expects
//! (`ScalarPluginFn`, `AggregatePluginFn`, …). The adapter handles:
//!
//! 1. Marshalling `&[ColumnarValue]` into an Arrow `RecordBatch`.
//! 2. Encoding the batch as Arrow IPC stream bytes.
//! 3. Calling the plugin's `invoke_<qname>` export under the plugin
//!    mutex.
//! 4. Decoding the returned IPC bytes back into a `RecordBatch` and
//!    extracting the single output column as a `ColumnarValue`.
//!
//! Per proposal §5.6.1, Arrow IPC over linear memory keeps the columnar
//! contract uniform across Extism and the Component Model. The executor
//! cannot tell which ABI delivered a batch — only the adapter does.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::logical_expr::ColumnarValue;
use uni_plugin::QName;
use uni_plugin::errors::FnError;
use uni_plugin::traits::scalar::{FnSignature, ScalarPluginFn};

use crate::adapter_common::{acquire, extism_err_to_fn_err, sanitize_qname};
use crate::ipc::{decode_batch, encode_batch};
use crate::pool::ExtismInstancePool;

/// Plugin-side scalar-fn export name from a qname.
///
/// Plugins expose `invoke_<sanitized-qname>` where every `.` in the
/// qname is replaced by `_` — this lets plugin authors use idiomatic
/// Rust function names (which can't contain `.`). The mapping is
/// deterministic; per proposal §6.5 the qname itself flows through
/// the plugin's `register` JSON, so the host always knows the
/// canonical qname even though the export symbol has underscores.
pub(crate) fn scalar_export_name(qname: &QName) -> String {
    format!("invoke_{}", sanitize_qname(qname))
}

/// `ScalarPluginFn` adapter wrapping an Extism plugin pool.
///
/// Each adapter holds an `Arc<ExtismInstancePool<extism::Plugin>>`
/// shared across all adapters bound to the same plugin manifest.
/// `invoke()` acquires a `PooledInstance`, runs the call, and releases
/// on drop — wait-free in steady state when `warm_count > 0`. The
/// pool's `max_instances` config bounds peak wasmtime memory.
pub struct ExtismScalarFn {
    pool: Arc<ExtismInstancePool<extism::Plugin>>,
    qname: QName,
    export_name: String,
    sig: FnSignature,
}

impl std::fmt::Debug for ExtismScalarFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExtismScalarFn")
            .field("qname", &self.qname)
            .field("export_name", &self.export_name)
            .field("signature", &self.sig)
            .finish_non_exhaustive()
    }
}

impl ExtismScalarFn {
    /// Construct a new adapter against the supplied pool.
    #[must_use]
    pub fn new(
        pool: Arc<ExtismInstancePool<extism::Plugin>>,
        qname: QName,
        sig: FnSignature,
    ) -> Self {
        let export_name = scalar_export_name(&qname);
        Self {
            pool,
            qname,
            export_name,
            sig,
        }
    }

    /// Build the host-side `RecordBatch` we ship to the plugin.
    ///
    /// Column names default to `arg0`, `arg1`, …; the plugin doesn't
    /// inspect names, only positions / types.
    fn args_to_batch(&self, args: &[ColumnarValue], rows: usize) -> Result<RecordBatch, FnError> {
        let arrays: Vec<arrow::array::ArrayRef> = args
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

impl ScalarPluginFn for ExtismScalarFn {
    fn signature(&self) -> &FnSignature {
        &self.sig
    }

    fn invoke(&self, args: &[ColumnarValue], rows: usize) -> Result<ColumnarValue, FnError> {
        let batch = self.args_to_batch(args, rows)?;
        let bytes = encode_batch(&batch).map_err(extism_err_to_fn_err)?;

        let mut leased = acquire(&self.pool)?;
        let out_bytes: Vec<u8> = {
            let plugin = leased.get_mut();
            let out: &[u8] = plugin
                .call(&self.export_name, bytes.as_slice())
                .map_err(|e| {
                    FnError::new(
                        FnError::CODE_UNEXPECTED_NULL,
                        format!("extism call `{}` failed: {e}", self.export_name),
                    )
                })?;
            // Copy out of the plugin's borrow before releasing the lease.
            out.to_vec()
        };
        drop(leased);

        let out_batch = decode_batch(&out_bytes)
            .map_err(extism_err_to_fn_err)?
            .ok_or_else(|| {
                FnError::new(
                    FnError::CODE_UNEXPECTED_NULL,
                    format!("plugin `{}` returned an empty IPC stream", self.export_name),
                )
            })?;

        if out_batch.num_columns() != 1 {
            return Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!(
                    "plugin `{}` returned {} columns; scalar fns must return exactly 1",
                    self.export_name,
                    out_batch.num_columns()
                ),
            ));
        }
        Ok(ColumnarValue::Array(out_batch.column(0).clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_export_name_format() {
        let q = QName::parse("geo.haversine").expect("valid");
        assert_eq!(scalar_export_name(&q), "invoke_geo_haversine");
    }
}
