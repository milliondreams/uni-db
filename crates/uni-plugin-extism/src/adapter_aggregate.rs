//! Aggregate adapter — bridges Extism aggregate plugins to
//! [`AggregatePluginFn`] / [`PluginAccumulator`].
//!
//! ## Wire contract (per qname `q`)
//!
//! - `agg_<q>_new` — input empty; output is the initial state bytes
//!   (opaque, plugin-defined; carried as Arrow `Binary` on the host).
//! - `agg_<q>_update` — input is `[state_len: u32 LE][state_bytes]
//!   [arrow_ipc_stream]` where the stream contains one batch whose
//!   columns match `agg.signature().args`. Output is the updated state
//!   bytes.
//! - `agg_<q>_merge` — input is `[state_len: u32 LE][state_bytes]
//!   [arrow_ipc_stream]` where the stream contains one batch with a
//!   single `Binary` column of `M` partial states. Output is the
//!   merged state bytes.
//! - `agg_<q>_evaluate` — input is the raw state bytes; output is an
//!   Arrow IPC stream with one 1-row batch whose single column has the
//!   declared `returns` type.
//!
//! The length-prefixed envelope is used because Extism's host↔plugin
//! call boundary takes a single byte buffer; the prefix lets the
//! plugin recover the opaque state without parsing the IPC bytes.

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

use crate::adapter_common::{acquire, extism_err_to_fn_err, sanitize_qname};
use crate::ipc::{decode_batch, encode_batch};
use crate::pool::ExtismInstancePool;

/// Plugin-side aggregate-`new` export name from a qname.
#[must_use]
pub(crate) fn agg_new_export_name(qname: &QName) -> String {
    format!("agg_{}_new", sanitize_qname(qname))
}

/// Plugin-side aggregate-`update` export name from a qname.
#[must_use]
pub(crate) fn agg_update_export_name(qname: &QName) -> String {
    format!("agg_{}_update", sanitize_qname(qname))
}

/// Plugin-side aggregate-`merge` export name from a qname.
#[must_use]
pub(crate) fn agg_merge_export_name(qname: &QName) -> String {
    format!("agg_{}_merge", sanitize_qname(qname))
}

/// Plugin-side aggregate-`evaluate` export name from a qname.
#[must_use]
pub(crate) fn agg_evaluate_export_name(qname: &QName) -> String {
    format!("agg_{}_evaluate", sanitize_qname(qname))
}

/// `AggregatePluginFn` adapter wrapping an Extism plugin pool.
pub struct ExtismAggregateFn {
    pool: Arc<ExtismInstancePool<extism::Plugin>>,
    qname: QName,
    sig: AggSignature,
    new_export: String,
    update_export: String,
    merge_export: String,
    evaluate_export: String,
}

impl std::fmt::Debug for ExtismAggregateFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExtismAggregateFn")
            .field("qname", &self.qname)
            .field("signature", &self.sig)
            .finish_non_exhaustive()
    }
}

impl ExtismAggregateFn {
    /// Construct a new adapter against the supplied pool.
    #[must_use]
    pub fn new(
        pool: Arc<ExtismInstancePool<extism::Plugin>>,
        qname: QName,
        sig: AggSignature,
    ) -> Self {
        let new_export = agg_new_export_name(&qname);
        let update_export = agg_update_export_name(&qname);
        let merge_export = agg_merge_export_name(&qname);
        let evaluate_export = agg_evaluate_export_name(&qname);
        Self {
            pool,
            qname,
            sig,
            new_export,
            update_export,
            merge_export,
            evaluate_export,
        }
    }

    fn call_new(&self) -> Result<Vec<u8>, FnError> {
        let mut leased = acquire(&self.pool)?;
        let bytes: Vec<u8> = leased
            .get_mut()
            .call::<&[u8], &[u8]>(&self.new_export, &[])
            .map_err(|e| {
                FnError::new(
                    FnError::CODE_UNEXPECTED_NULL,
                    format!("extism call `{}` failed: {e}", self.new_export),
                )
            })?
            .to_vec();
        drop(leased);
        Ok(bytes)
    }
}

impl AggregatePluginFn for ExtismAggregateFn {
    fn signature(&self) -> &AggSignature {
        &self.sig
    }

    fn create_accumulator(&self) -> Box<dyn PluginAccumulator> {
        // `create_accumulator` returns a Box without a Result; if the
        // plugin's `_new` export fails, we surface that on the first
        // update/evaluate call by carrying an empty state and a
        // remembered init error. Two-phase init keeps the trait shape
        // (DataFusion expects an infallible accumulator factory).
        let (state, init_err) = match self.call_new() {
            Ok(s) => (s, None),
            Err(e) => (Vec::new(), Some(e)),
        };
        Box::new(ExtismAggregateAccumulator {
            state,
            init_err,
            pool: Arc::clone(&self.pool),
            update_export: self.update_export.clone(),
            merge_export: self.merge_export.clone(),
            evaluate_export: self.evaluate_export.clone(),
            args_schema: build_args_schema(&self.sig),
            returns_field: build_returns_field(&self.sig),
        })
    }
}

/// Per-group state machine.
struct ExtismAggregateAccumulator {
    state: Vec<u8>,
    init_err: Option<FnError>,
    pool: Arc<ExtismInstancePool<extism::Plugin>>,
    update_export: String,
    merge_export: String,
    evaluate_export: String,
    args_schema: SchemaRef,
    returns_field: Field,
}

impl ExtismAggregateAccumulator {
    fn surface_init_err(&self) -> Result<(), FnError> {
        if let Some(e) = &self.init_err {
            return Err(FnError::new(
                e.code,
                format!("aggregate init failed: {}", e.message),
            ));
        }
        Ok(())
    }

    fn call_with_envelope(&self, export: &str, batch: RecordBatch) -> Result<Vec<u8>, FnError> {
        let ipc = encode_batch(&batch).map_err(extism_err_to_fn_err)?;
        // Reject states wider than the u32 length prefix before building the
        // envelope (`build_envelope` would otherwise silently clamp to
        // `u32::MAX`, corrupting the wire framing).
        if u32::try_from(self.state.len()).is_err() {
            return Err(FnError::new(
                FnError::CODE_RESOURCE_LIMIT,
                "aggregate state exceeds u32::MAX bytes",
            ));
        }
        let buf = build_envelope(&self.state, &ipc);

        let mut leased = acquire(&self.pool)?;
        let out: Vec<u8> = leased
            .get_mut()
            .call::<&[u8], &[u8]>(export, &buf)
            .map_err(|e| {
                FnError::new(
                    FnError::CODE_UNEXPECTED_NULL,
                    format!("extism call `{export}` failed: {e}"),
                )
            })?
            .to_vec();
        drop(leased);
        Ok(out)
    }
}

impl PluginAccumulator for ExtismAggregateAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), FnError> {
        self.surface_init_err()?;
        let batch =
            RecordBatch::try_new(Arc::clone(&self.args_schema), values.to_vec()).map_err(|e| {
                FnError::new(
                    FnError::CODE_TYPE_COERCION,
                    format!("update_batch: RecordBatch assembly: {e}"),
                )
            })?;
        let new_state = self.call_with_envelope(&self.update_export, batch)?;
        self.state = new_state;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<(), FnError> {
        self.surface_init_err()?;
        if states.len() != 1 {
            return Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!(
                    "merge_batch expects exactly 1 state column (opaque Binary); got {}",
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
                format!("merge_batch: RecordBatch assembly: {e}"),
            )
        })?;
        let new_state = self.call_with_envelope(&self.merge_export, batch)?;
        self.state = new_state;
        Ok(())
    }

    fn state(&self) -> Result<Vec<ScalarValue>, FnError> {
        self.surface_init_err()?;
        Ok(vec![ScalarValue::Binary(Some(self.state.clone()))])
    }

    fn evaluate(&self) -> Result<ScalarValue, FnError> {
        self.surface_init_err()?;
        let mut leased = acquire(&self.pool)?;
        let out_bytes: Vec<u8> = leased
            .get_mut()
            .call::<&[u8], &[u8]>(&self.evaluate_export, &self.state)
            .map_err(|e| {
                FnError::new(
                    FnError::CODE_UNEXPECTED_NULL,
                    format!("extism call `{}` failed: {e}", self.evaluate_export),
                )
            })?
            .to_vec();
        drop(leased);

        let batch = decode_batch(&out_bytes)
            .map_err(extism_err_to_fn_err)?
            .ok_or_else(|| {
                FnError::new(
                    FnError::CODE_UNEXPECTED_NULL,
                    format!(
                        "plugin `{}` returned an empty IPC stream",
                        self.evaluate_export
                    ),
                )
            })?;
        if batch.num_columns() != 1 || batch.num_rows() != 1 {
            return Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!(
                    "plugin `{}` must return a 1-row × 1-col batch; got {} rows × {} cols",
                    self.evaluate_export,
                    batch.num_rows(),
                    batch.num_columns()
                ),
            ));
        }
        // Sanity-check declared returns type matches.
        if batch.column(0).data_type() != self.returns_field.data_type() {
            return Err(FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!(
                    "plugin `{}` returned column type {:?}, expected {:?}",
                    self.evaluate_export,
                    batch.column(0).data_type(),
                    self.returns_field.data_type()
                ),
            ));
        }
        ScalarValue::try_from_array(batch.column(0), 0).map_err(|e| {
            FnError::new(
                FnError::CODE_TYPE_COERCION,
                format!("evaluate: ScalarValue::try_from_array: {e}"),
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

/// Build the length-prefixed `update`/`merge` envelope
/// `[state_len: u32 LE][state_bytes][ipc_stream_bytes]`.
///
/// Used by [`ExtismAggregateAccumulator::call_with_envelope`] on the
/// host side and mirrored by the plugin's [`parse_envelope`] equivalent;
/// also exposed for envelope round-trip tests. Callers that cannot
/// tolerate a clamped length must reject `state.len() > u32::MAX` before
/// calling — this writer saturates the prefix at `u32::MAX`.
#[doc(hidden)]
#[must_use]
pub fn build_envelope(state: &[u8], ipc: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + state.len() + ipc.len());
    buf.extend_from_slice(&u32::try_from(state.len()).unwrap_or(u32::MAX).to_le_bytes());
    buf.extend_from_slice(state);
    buf.extend_from_slice(ipc);
    buf
}

/// Helper exposed for tests / plugin authors: parse the envelope shape
/// `[state_len: u32 LE][state_bytes][ipc_stream_bytes]`.
///
/// Returns `(state, ipc)`. The plugin uses the equivalent of this on
/// its side to recover state + values per `update`/`merge` call.
///
/// # Errors
///
/// Returns a string error if the buffer is shorter than 4 bytes or the
/// declared state length overruns the buffer.
pub fn parse_envelope(buf: &[u8]) -> Result<(&[u8], &[u8]), String> {
    if buf.len() < 4 {
        return Err(format!("envelope too short: {} bytes < 4", buf.len()));
    }
    let len_bytes: [u8; 4] = buf[..4].try_into().expect("4 bytes");
    let state_len = u32::from_le_bytes(len_bytes) as usize;
    let end = 4usize
        .checked_add(state_len)
        .ok_or_else(|| "state length overflow".to_owned())?;
    if end > buf.len() {
        return Err(format!(
            "declared state_len {} overruns buffer of {} bytes",
            state_len,
            buf.len()
        ));
    }
    Ok((&buf[4..end], &buf[end..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_name_format() {
        let q = QName::parse("stats.weighted_mean").expect("valid");
        assert_eq!(agg_new_export_name(&q), "agg_stats_weighted_mean_new");
        assert_eq!(agg_update_export_name(&q), "agg_stats_weighted_mean_update");
        assert_eq!(agg_merge_export_name(&q), "agg_stats_weighted_mean_merge");
        assert_eq!(
            agg_evaluate_export_name(&q),
            "agg_stats_weighted_mean_evaluate"
        );
    }

    #[test]
    fn envelope_roundtrip_preserves_state_and_ipc() {
        let state = b"opaque-state-blob".as_slice();
        let ipc = b"\x01\x02\x03not-real-but-distinct".as_slice();
        let env = build_envelope(state, ipc);
        let (got_state, got_ipc) = parse_envelope(&env).expect("parse");
        assert_eq!(got_state, state);
        assert_eq!(got_ipc, ipc);
    }

    #[test]
    fn envelope_with_empty_state() {
        let env = build_envelope(&[], b"ipc");
        let (state, ipc) = parse_envelope(&env).unwrap();
        assert!(state.is_empty());
        assert_eq!(ipc, b"ipc");
    }

    #[test]
    fn envelope_with_empty_ipc() {
        let env = build_envelope(b"state-only", &[]);
        let (state, ipc) = parse_envelope(&env).unwrap();
        assert_eq!(state, b"state-only");
        assert!(ipc.is_empty());
    }

    #[test]
    fn parse_envelope_rejects_short_buffer() {
        assert!(parse_envelope(&[1u8, 2]).is_err());
    }

    #[test]
    fn parse_envelope_rejects_overrun() {
        // state_len declared = 0xFFFFFFFF but buffer is only 4 bytes.
        let buf = vec![0xFFu8, 0xFF, 0xFF, 0xFF];
        assert!(parse_envelope(&buf).is_err());
    }

    #[test]
    fn args_schema_matches_signature_args() {
        use arrow_schema::DataType;
        use datafusion::logical_expr::Volatility;
        use uni_plugin::traits::scalar::ArgType;
        let sig = AggSignature::new(
            vec![ArgType::Primitive(DataType::Float64), ArgType::CypherValue],
            ArgType::Primitive(DataType::Float64),
            vec![Field::new("state", DataType::Binary, true)],
            Volatility::Immutable,
        );
        let schema = build_args_schema(&sig);
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "arg0");
        assert_eq!(schema.field(0).data_type(), &DataType::Float64);
        assert_eq!(schema.field(1).name(), "arg1");
        assert_eq!(schema.field(1).data_type(), &DataType::LargeBinary);
    }

    #[test]
    fn build_returns_field_uses_signature_returns() {
        use arrow_schema::DataType;
        use datafusion::logical_expr::Volatility;
        use uni_plugin::traits::scalar::ArgType;
        let sig = AggSignature::new(
            vec![],
            ArgType::Primitive(DataType::Int64),
            vec![Field::new("state", DataType::Binary, true)],
            Volatility::Immutable,
        );
        let f = build_returns_field(&sig);
        assert_eq!(f.data_type(), &DataType::Int64);
    }
}
