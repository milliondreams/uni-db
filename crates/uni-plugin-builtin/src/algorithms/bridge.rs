//! Bridge wiring `uni_algo::AlgoProcedure` into
//! `uni_plugin::AlgorithmProvider`.
//!
//! The bridge implements `AlgorithmProvider::run` by:
//! 1. Parsing `config_json` into `Vec<serde_json::Value>` args (the
//!    shape the algo expects from `CALL`).
//! 2. Downcasting `AlgorithmContext::host` to [`AlgorithmHostBridge`] to
//!    recover the concrete `AlgoContext` (StorageManager + L0Manager).
//! 3. Driving the algorithm's `AlgoResultRow` stream to completion and
//!    collecting it into a single Arrow `RecordBatch` matching the
//!    declared `AlgorithmSignature::output_fields`.
//!
//! When no host is bound, the bridge returns an
//! `Unbound` error code so the caller can supply the host on retry.
//
// Rust guideline compliant

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use uni_algo::algo::procedures::{AlgoContext, AlgoProcedure, AlgoResultRow, ValueType};
use uni_plugin::FnError;
use uni_plugin::traits::algorithm::{
    AlgorithmContext, AlgorithmHost, AlgorithmProvider, AlgorithmSignature,
};

/// Bridge host that surfaces `StorageManager` + optional `L0Manager`
/// to plugin algorithms through [`AlgorithmHost::as_any`].
pub struct AlgorithmHostBridge {
    /// The concrete algo context the wrapped procedures need.
    pub algo_ctx: AlgoContext,
}

impl std::fmt::Debug for AlgorithmHostBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlgorithmHostBridge")
            .finish_non_exhaustive()
    }
}

impl AlgorithmHostBridge {
    /// Construct a host bridge from an [`AlgoContext`].
    #[must_use]
    pub fn new(algo_ctx: AlgoContext) -> Self {
        Self { algo_ctx }
    }
}

impl AlgorithmHost for AlgorithmHostBridge {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Provider wrapping a single [`AlgoProcedure`].
pub struct AlgoProviderBridge {
    proc: Arc<dyn AlgoProcedure>,
    signature: AlgorithmSignature,
    yields: Vec<(&'static str, ValueType)>,
}

impl std::fmt::Debug for AlgoProviderBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlgoProviderBridge")
            .field("name", &self.proc.name())
            .finish_non_exhaustive()
    }
}

impl AlgoProviderBridge {
    /// Wrap an `AlgoProcedure` as an `AlgorithmProvider`.
    #[must_use]
    pub fn new(proc: Arc<dyn AlgoProcedure>) -> Self {
        let sig = proc.signature();
        let output_fields: Vec<Field> = sig
            .yields
            .iter()
            .map(|(n, vt)| Field::new((*n).to_owned(), value_type_to_arrow(vt), true))
            .collect();
        let signature = AlgorithmSignature {
            output_fields,
            docs: format!("uni.{} (algorithm)", proc.name()),
        };
        Self {
            proc,
            signature,
            yields: sig.yields,
        }
    }
}

impl AlgorithmProvider for AlgoProviderBridge {
    fn signature(&self) -> &AlgorithmSignature {
        &self.signature
    }

    fn run(&self, ctx: AlgorithmContext<'_>) -> Result<SendableRecordBatchStream, FnError> {
        let host = ctx
            .host
            .ok_or_else(|| FnError::new(0x800, "AlgoProviderBridge: host unbound"))?;
        let bridge = host
            .as_any()
            .downcast_ref::<AlgorithmHostBridge>()
            .ok_or_else(|| {
                FnError::new(0x801, "AlgoProviderBridge: host is not AlgorithmHostBridge")
            })?;

        let args: Vec<serde_json::Value> = if ctx.config_json.is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(ctx.config_json)
                .map_err(|e| FnError::new(0x802, format!("config_json parse: {e}")))?
        };

        // Clone what we need into the async stream; the wrapped
        // `AlgoContext` is `!Clone`, but `StorageManager` / `L0Manager`
        // inside are `Arc`, so we rebuild a fresh `AlgoContext` from
        // their clones.
        let algo_ctx = AlgoContext::new(
            Arc::clone(&bridge.algo_ctx.storage),
            bridge.algo_ctx.l0_manager.as_ref().map(Arc::clone),
        );
        let proc = Arc::clone(&self.proc);
        let yields = self.yields.clone();
        let fields = self.signature.output_fields.clone();
        let out_schema = Arc::new(Schema::new(fields.clone()));

        let stream = futures::stream::once(async move {
            // Same dispatch logic as `uni-query`'s V2Plan::Direct
            // branch: route cypher-path algos through
            // `execute_with_native_terminals`; everything else builds
            // a projection from `(nodeLabels, edgeTypes, …)` args and
            // takes the projection-aware entry point.
            let mut algo_stream = if proc.wants_native_terminals() {
                proc.execute_with_native_terminals(algo_ctx, args)
            } else {
                let projection =
                    uni_algo::algo::procedure_template::build_projection_from_direct_args(
                        proc.as_ref(),
                        &algo_ctx,
                        &args,
                    )
                    .await
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "AlgoProviderBridge projection build failed: {e}"
                        ))
                    })?;
                proc.execute_with_projection(algo_ctx, args, projection)
            };
            let mut rows: Vec<AlgoResultRow> = Vec::new();
            while let Some(row_res) = algo_stream.next().await {
                let row = row_res
                    .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
                rows.push(row);
            }
            build_record_batch(&rows, &yields, &fields)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }
}

fn value_type_to_arrow(vt: &ValueType) -> DataType {
    match vt {
        ValueType::Int => DataType::Int64,
        ValueType::Float => DataType::Float64,
        ValueType::String => DataType::Utf8,
        ValueType::Bool => DataType::Boolean,
        ValueType::List | ValueType::Map | ValueType::Path => DataType::LargeBinary,
        ValueType::Node => DataType::Int64,
        ValueType::Relationship => DataType::Int64,
        ValueType::Any => DataType::Utf8,
    }
}

fn build_record_batch(
    rows: &[AlgoResultRow],
    yields: &[(&'static str, ValueType)],
    fields: &[Field],
) -> Result<RecordBatch, datafusion::error::DataFusionError> {
    use arrow_array::{BooleanArray, Float64Array, Int64Array, LargeBinaryArray, StringArray};
    let schema = Arc::new(Schema::new(fields.to_vec()));
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(fields.len());
    for (idx, (_name, vt)) in yields.iter().enumerate() {
        let col: ArrayRef = match vt {
            ValueType::Int | ValueType::Node | ValueType::Relationship => {
                let v: Vec<Option<i64>> = rows
                    .iter()
                    .map(|r| {
                        r.values
                            .get(idx)
                            .and_then(|x| x.as_i64().or_else(|| x.as_u64().map(|u| u as i64)))
                    })
                    .collect();
                Arc::new(Int64Array::from(v))
            }
            ValueType::Float => {
                let v: Vec<Option<f64>> = rows
                    .iter()
                    .map(|r| r.values.get(idx).and_then(|x| x.as_f64()))
                    .collect();
                Arc::new(Float64Array::from(v))
            }
            ValueType::Bool => {
                let v: Vec<Option<bool>> = rows
                    .iter()
                    .map(|r| r.values.get(idx).and_then(|x| x.as_bool()))
                    .collect();
                Arc::new(BooleanArray::from(v))
            }
            ValueType::String | ValueType::Any => {
                let v: Vec<Option<String>> = rows
                    .iter()
                    .map(|r| {
                        r.values.get(idx).map(|x| {
                            x.as_str()
                                .map(str::to_owned)
                                .unwrap_or_else(|| x.to_string())
                        })
                    })
                    .collect();
                Arc::new(StringArray::from(v))
            }
            ValueType::List | ValueType::Map | ValueType::Path => {
                let v: Vec<Option<Vec<u8>>> = rows
                    .iter()
                    .map(|r| {
                        r.values
                            .get(idx)
                            .map(|x| serde_json::to_vec(x).unwrap_or_default())
                    })
                    .collect();
                Arc::new(LargeBinaryArray::from_iter(v.iter().map(|o| o.as_deref())))
            }
        };
        cols.push(col);
    }
    RecordBatch::try_new(schema, cols)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Helper: build an `AlgorithmHostBridge` from `StorageManager` + L0.
///
/// Hosts use this when constructing an `AlgorithmContext`.
#[must_use]
pub fn host_bridge_from_storage(
    storage: Arc<uni_store::storage::manager::StorageManager>,
    l0: Option<Arc<uni_store::runtime::L0Manager>>,
) -> AlgorithmHostBridge {
    AlgorithmHostBridge::new(AlgoContext::new(storage, l0))
}
