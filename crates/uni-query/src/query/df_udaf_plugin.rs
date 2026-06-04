// Rust guideline compliant
//! DataFusion adapter for [`uni_plugin::traits::aggregate::AggregatePluginFn`].
//!
//! Bridges plugin-registered aggregates (`AggregatePluginFn`) into the
//! DataFusion `AggregateUDFImpl` surface so the Cypher planner can
//! invoke `RETURN myAgg(x)` against any registry entry, not just the
//! handful of built-ins hard-coded in `df_planner.rs::translate_aggregates`.
//!
//! M9 ships this in support of `uni.plugin.declareAggregate` (see
//! `uni-plugin-custom::DeclaredAggregateFn`). The adapter is generic
//! across any `AggregatePluginFn` source — it does not assume the
//! declared shape.
//!
//! # State model
//!
//! Plugin aggregates' `AggSignature.state_fields` declares the schema
//! of partial state for distributed aggregation. The M9 declared
//! aggregates ship with `state_fields: vec![]` and
//! `supports_partial: false`; the adapter respects whatever the
//! registry entry declares.

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use arrow::array::ArrayRef;
use arrow::datatypes::Field;
use arrow_schema::DataType;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator as DfAccumulator, AggregateUDFImpl, Signature, TypeSignature,
};
use datafusion::scalar::ScalarValue;
use uni_plugin::traits::aggregate::{AggSignature, AggregatePluginFn, PluginAccumulator};
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::{PluginRegistry, QName};

/// DataFusion `AggregateUDFImpl` wrapping a plugin-registered
/// aggregate looked up by [`QName`] in the shared [`PluginRegistry`].
///
/// Each accumulator call re-fetches the entry so hot-reload swaps land
/// for the next group; in-flight groups keep their accumulator.
pub struct PluginAggregateUdaf {
    qname: QName,
    name: String,
    registry: Arc<PluginRegistry>,
    sig: AggSignature,
    df_signature: Signature,
}

impl PluginAggregateUdaf {
    /// Construct an adapter over the named registry entry.
    ///
    /// `qname` and `sig` are captured at planner time; the actual
    /// `AggregatePluginFn` is fetched per-accumulator-construction from
    /// `registry` so reloads pick up.
    #[must_use]
    pub fn new(qname: QName, registry: Arc<PluginRegistry>, sig: AggSignature) -> Self {
        let arity = sig.args.len();
        let df_signature = Signature::new(TypeSignature::Any(arity), sig.volatility);
        let name = format!("{}.{}", qname.namespace(), qname.local());
        Self {
            qname,
            name,
            registry,
            sig,
            df_signature,
        }
    }

    fn fetch(&self) -> DFResult<Arc<dyn AggregatePluginFn>> {
        self.registry
            .aggregate(&self.qname)
            .map(|e| Arc::clone(&e.aggregate))
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "PluginAggregateUdaf: registry entry for `{}` disappeared",
                    self.name
                ))
            })
    }
}

impl std::fmt::Debug for PluginAggregateUdaf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginAggregateUdaf")
            .field("qname", &self.qname)
            .field("supports_partial", &self.sig.supports_partial)
            .finish_non_exhaustive()
    }
}

impl PartialEq for PluginAggregateUdaf {
    fn eq(&self, other: &Self) -> bool {
        self.qname == other.qname
    }
}

impl Eq for PluginAggregateUdaf {}

impl Hash for PluginAggregateUdaf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl AggregateUDFImpl for PluginAggregateUdaf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.df_signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        arg_type_to_arrow(&self.sig.returns).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "PluginAggregateUdaf `{}`: non-Arrow return type",
                self.name
            ))
        })
    }

    fn accumulator(&self, _args: AccumulatorArgs<'_>) -> DFResult<Box<dyn DfAccumulator>> {
        let agg = self.fetch()?;
        Ok(Box::new(PluginAccumulatorAdapter {
            inner: Mutex::new(agg.create_accumulator()),
        }))
    }

    fn state_fields(&self, _args: StateFieldsArgs<'_>) -> DFResult<Vec<Arc<Field>>> {
        Ok(self
            .sig
            .state_fields
            .iter()
            .map(|f| Arc::new(f.clone()))
            .collect())
    }
}

/// DataFusion `Accumulator` that forwards to a [`PluginAccumulator`].
///
/// DataFusion's [`DfAccumulator`] trait requires `Send + Sync`, while
/// the plugin trait only requires `Send`. The `Mutex` provides the
/// `Sync` upgrade without modifying the plugin ABI; under
/// DataFusion's `&mut self`-only call pattern the lock is uncontended
/// in practice.
struct PluginAccumulatorAdapter {
    inner: Mutex<Box<dyn PluginAccumulator>>,
}

impl std::fmt::Debug for PluginAccumulatorAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginAccumulatorAdapter")
            .finish_non_exhaustive()
    }
}

impl PluginAccumulatorAdapter {
    fn with_inner<F, R>(&self, f: F) -> DFResult<R>
    where
        F: FnOnce(&mut dyn PluginAccumulator) -> Result<R, uni_plugin::FnError>,
    {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| DataFusionError::Execution(format!("plugin accumulator lock: {e}")))?;
        f(guard.as_mut()).map_err(fn_err_to_df)
    }
}

impl DfAccumulator for PluginAccumulatorAdapter {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        self.with_inner(|acc| acc.update_batch(values))
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        self.with_inner(|acc| acc.merge_batch(states))
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        self.with_inner(|acc| acc.evaluate())
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        self.with_inner(|acc| acc.state())
    }

    fn size(&self) -> usize {
        self.inner
            .lock()
            .map(|g| g.size())
            .unwrap_or(std::mem::size_of::<Self>())
    }
}

fn fn_err_to_df(e: uni_plugin::FnError) -> DataFusionError {
    DataFusionError::Execution(format!("plugin aggregate: {e}"))
}

/// Map a plugin [`ArgType`] to a concrete Arrow [`DataType`]. Returns
/// `None` for non-Arrow shapes (`Variadic`).
fn arg_type_to_arrow(a: &ArgType) -> Option<DataType> {
    match a {
        ArgType::Primitive(dt) => Some(dt.clone()),
        // `CypherValue` plugins ride through `LargeBinary` opaquely.
        ArgType::CypherValue => Some(DataType::LargeBinary),
        ArgType::Vector { len, element } => Some(DataType::FixedSizeList(
            Arc::new(Field::new("item", element.clone(), true)),
            i32::try_from(*len).ok()?,
        )),
        ArgType::Variadic(_) => None,
    }
}
