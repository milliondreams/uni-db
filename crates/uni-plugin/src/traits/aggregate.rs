//! Cypher aggregate plugin functions.
//!
//! Aggregates accumulate state across many input rows and produce a single
//! result. The trait splits the user-facing signature ([`AggregatePluginFn`])
//! from the per-group state machine ([`PluginAccumulator`]), matching
//! DataFusion's `AggregateUDFImpl` / `Accumulator` split so plugin
//! aggregates can run inside DataFusion's partial-aggregation flow.

use arrow_array::ArrayRef;
use arrow_schema::Field;
use datafusion::logical_expr::Volatility;
use datafusion::scalar::ScalarValue;

use crate::errors::FnError;
use crate::traits::scalar::ArgType;

/// A Cypher aggregate function plugin.
pub trait AggregatePluginFn: Send + Sync {
    /// Static signature.
    fn signature(&self) -> &AggSignature;

    /// Construct a fresh per-group accumulator.
    fn create_accumulator(&self) -> Box<dyn PluginAccumulator>;
}

/// Per-group state machine for an aggregate function.
///
/// One `PluginAccumulator` instance is created per group. The host calls
/// `update_batch` repeatedly with the group's rows, then `evaluate` for the
/// final value. For distributed aggregation, the host calls `state` on
/// partial accumulators and `merge_batch` on the final accumulator.
pub trait PluginAccumulator: Send {
    /// Ingest a batch of input rows into the accumulator.
    ///
    /// `values[i]` is the `i`-th argument's column, all of equal length.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the input cannot be accumulated (type
    /// mismatch, resource exhaustion).
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), FnError>;

    /// Merge per-partition partial states into this accumulator.
    ///
    /// `states[i]` is the `i`-th state field across partial accumulators.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the merge cannot proceed.
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<(), FnError>;

    /// Return the current accumulator state as scalar values, for transport
    /// across the partial / final aggregation boundary.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the state cannot be serialized.
    fn state(&self) -> Result<Vec<ScalarValue>, FnError>;

    /// Produce the final aggregate value.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the final value cannot be computed (e.g.,
    /// undefined for empty input and the aggregate forbids it).
    fn evaluate(&self) -> Result<ScalarValue, FnError>;

    /// Approximate in-memory size, in bytes — used for memory accounting.
    fn size(&self) -> usize;
}

/// Static signature of an aggregate function plugin.
#[derive(Clone, Debug)]
pub struct AggSignature {
    /// Argument types, in declaration order.
    pub args: Vec<ArgType>,
    /// Final return type.
    pub returns: ArgType,
    /// Schema of the per-partition partial state.
    pub state_fields: Vec<Field>,
    /// DataFusion volatility.
    pub volatility: Volatility,
    /// `true` if this aggregate supports partial aggregation (the common
    /// case). `false` aggregates only run in a single physical pass.
    pub supports_partial: bool,
}

impl AggSignature {
    /// Convenience constructor for partial-aggregation-capable signatures.
    #[must_use]
    pub fn new(
        args: Vec<ArgType>,
        returns: ArgType,
        state_fields: Vec<Field>,
        volatility: Volatility,
    ) -> Self {
        Self {
            args,
            returns,
            state_fields,
            volatility,
            supports_partial: true,
        }
    }
}
