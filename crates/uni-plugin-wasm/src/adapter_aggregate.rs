//! Aggregate adapter — bridges a CM `aggregate-plugin` instance to
//! [`AggregatePluginFn`] / [`PluginAccumulator`].
//!
//! Port of `uni_plugin_extism::adapter_aggregate`. Same envelope-less
//! state-passing shape — the CM ABI carries `state: list<u8>` as a
//! typed parameter rather than packing it into the IPC bytes, so the
//! envelope helper from the extism side is unnecessary here.

// Rust guideline compliant

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_array::ArrayRef;
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::scalar::ScalarValue;
use uni_plugin::QName;
use uni_plugin::adapter_common::arrow_types::argtype_to_arrow;
use uni_plugin::errors::FnError;
use uni_plugin::traits::aggregate::{AggSignature, AggregatePluginFn, PluginAccumulator};
use uni_plugin_wasm_rt::ipc::{decode_batch, encode_batch};

use crate::adapter_common::{acquire, ipc_to_fn_err};
use crate::loader::AggregatePluginInstance;
use crate::pool::WasmInstancePool;

/// `AggregatePluginFn` adapter wrapping a CM aggregate-plugin pool.
pub struct ComponentAggregateFn {
    pool: Arc<WasmInstancePool<AggregatePluginInstance>>,
    qname: QName,
    sig: AggSignature,
}

impl std::fmt::Debug for ComponentAggregateFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComponentAggregateFn")
            .field("qname", &self.qname)
            .field("signature", &self.sig)
            .finish_non_exhaustive()
    }
}

impl ComponentAggregateFn {
    /// Construct a new adapter against the supplied pool.
    #[must_use]
    pub fn new(
        pool: Arc<WasmInstancePool<AggregatePluginInstance>>,
        qname: QName,
        sig: AggSignature,
    ) -> Self {
        Self { pool, qname, sig }
    }

    fn call_new(&self) -> Result<Vec<u8>, FnError> {
        let mut leased = acquire(&self.pool, "aggregate")?;
        let qname_str = self.qname.to_string();
        let state = leased.get_mut().agg_new(&qname_str).map_err(|e| {
            FnError::new(
                FnError::CODE_UNEXPECTED_NULL,
                format!("wasm agg_new `{qname_str}`: {e}"),
            )
        })?;
        drop(leased);
        Ok(state)
    }
}

impl AggregatePluginFn for ComponentAggregateFn {
    fn signature(&self) -> &AggSignature {
        &self.sig
    }

    fn create_accumulator(&self) -> Box<dyn PluginAccumulator> {
        let (state, init_err) = match self.call_new() {
            Ok(s) => (s, None),
            Err(e) => (Vec::new(), Some(e)),
        };
        Box::new(ComponentAggregateAccumulator {
            state,
            init_err,
            pool: Arc::clone(&self.pool),
            qname: self.qname.to_string(),
            args_schema: build_args_schema(&self.sig),
            returns_field: build_returns_field(&self.sig),
        })
    }
}

struct ComponentAggregateAccumulator {
    state: Vec<u8>,
    init_err: Option<FnError>,
    pool: Arc<WasmInstancePool<AggregatePluginInstance>>,
    qname: String,
    args_schema: SchemaRef,
    returns_field: Field,
}

impl ComponentAggregateAccumulator {
    fn surface_init_err(&self) -> Result<(), FnError> {
        if let Some(e) = &self.init_err {
            return Err(FnError::new(
                e.code,
                format!("aggregate init failed: {}", e.message),
            ));
        }
        Ok(())
    }
}

impl PluginAccumulator for ComponentAggregateAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), FnError> {
        self.surface_init_err()?;
        let batch =
            RecordBatch::try_new(Arc::clone(&self.args_schema), values.to_vec()).map_err(|e| {
                FnError::new(
                    FnError::CODE_TYPE_COERCION,
                    format!("update_batch RecordBatch: {e}"),
                )
            })?;
        let ipc = encode_batch(&batch).map_err(ipc_to_fn_err)?;
        let mut leased = acquire(&self.pool, "aggregate")?;
        let new_state = leased
            .get_mut()
            .agg_update(&self.qname, &self.state, &ipc)
            .map_err(|e| {
                FnError::new(
                    FnError::CODE_UNEXPECTED_NULL,
                    format!("wasm agg_update `{}`: {e}", self.qname),
                )
            })?;
        drop(leased);
        self.state = new_state;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<(), FnError> {
        self.surface_init_err()?;
        if states.len() != 1 {
            return Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!(
                    "merge_batch expects 1 state column (opaque Binary); got {}",
                    states.len()
                ),
            ));
        }
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "partial_state",
            states[0].data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, states.to_vec()).map_err(|e| {
            FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!("merge_batch RecordBatch: {e}"),
            )
        })?;
        let ipc = encode_batch(&batch).map_err(ipc_to_fn_err)?;
        let mut leased = acquire(&self.pool, "aggregate")?;
        let new_state = leased
            .get_mut()
            .agg_merge(&self.qname, &self.state, &ipc)
            .map_err(|e| {
                FnError::new(
                    FnError::CODE_UNEXPECTED_NULL,
                    format!("wasm agg_merge `{}`: {e}", self.qname),
                )
            })?;
        drop(leased);
        self.state = new_state;
        Ok(())
    }

    fn state(&self) -> Result<Vec<ScalarValue>, FnError> {
        self.surface_init_err()?;
        Ok(vec![ScalarValue::Binary(Some(self.state.clone()))])
    }

    fn evaluate(&self) -> Result<ScalarValue, FnError> {
        self.surface_init_err()?;
        let mut leased = acquire(&self.pool, "aggregate")?;
        let out_bytes = leased
            .get_mut()
            .agg_evaluate(&self.qname, &self.state)
            .map_err(|e| {
                FnError::new(
                    FnError::CODE_UNEXPECTED_NULL,
                    format!("wasm agg_evaluate `{}`: {e}", self.qname),
                )
            })?;
        drop(leased);
        let batch = decode_batch(&out_bytes)
            .map_err(ipc_to_fn_err)?
            .ok_or_else(|| {
                FnError::new(
                    FnError::CODE_UNEXPECTED_NULL,
                    format!("plugin agg_evaluate `{}` empty IPC", self.qname),
                )
            })?;
        if batch.num_columns() != 1 || batch.num_rows() != 1 {
            return Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!(
                    "plugin agg_evaluate `{}` must return 1×1; got {}×{}",
                    self.qname,
                    batch.num_rows(),
                    batch.num_columns()
                ),
            ));
        }
        if batch.column(0).data_type() != self.returns_field.data_type() {
            return Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!(
                    "plugin agg_evaluate `{}` returned {:?}, expected {:?}",
                    self.qname,
                    batch.column(0).data_type(),
                    self.returns_field.data_type()
                ),
            ));
        }
        ScalarValue::try_from_array(batch.column(0), 0).map_err(|e| {
            FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!("agg_evaluate ScalarValue: {e}"),
            )
        })
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.state.capacity()
    }
}

fn build_args_schema(sig: &AggSignature) -> SchemaRef {
    let fields: Vec<Field> = sig
        .args
        .iter()
        .enumerate()
        .map(|(i, t)| Field::new(format!("arg{i}"), argtype_to_arrow(t), true))
        .collect();
    Arc::new(Schema::new(fields))
}

fn build_returns_field(sig: &AggSignature) -> Field {
    Field::new("returns", argtype_to_arrow(&sig.returns), true)
}
