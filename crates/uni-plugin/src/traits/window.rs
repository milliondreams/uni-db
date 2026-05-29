//! Cypher window-function plugins.
//!
//! Window functions evaluate over partitions of rows defined by `PARTITION
//! BY` / `ORDER BY` clauses, producing one output per input row. Unlike
//! aggregates they preserve the input cardinality.

use arrow_array::ArrayRef;
use arrow_schema::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::Volatility;

use crate::errors::FnError;
use crate::traits::scalar::ArgType;

/// A Cypher window-function plugin.
pub trait WindowPluginFn: Send + Sync {
    /// Static signature.
    fn signature(&self) -> &WindowSignature;

    /// Evaluate the window function over a partition.
    ///
    /// `partition` is the partition's rows, already sorted per `ORDER BY`.
    /// `frame` describes the relative window over which each row's value is
    /// computed. Implementations return an `ArrayRef` of length
    /// `partition.num_rows()`.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the partition cannot be evaluated.
    fn evaluate(&self, partition: &RecordBatch, frame: WindowFrame) -> Result<ArrayRef, FnError>;
}

/// Static signature of a window-function plugin.
#[derive(Clone, Debug)]
pub struct WindowSignature {
    /// Argument types, in declaration order.
    pub args: Vec<ArgType>,
    /// Output type.
    pub returns: ArgType,
    /// DataFusion volatility.
    pub volatility: Volatility,
}

/// Descriptor for the active window over a partition row.
#[derive(Clone, Debug)]
pub struct WindowFrame {
    /// Schema of the partition (for column resolution by name).
    pub schema: SchemaRef,
    /// Inclusive start row index relative to the partition (0-based).
    pub start: usize,
    /// Exclusive end row index relative to the partition.
    pub end: usize,
    /// Column indices participating in `ORDER BY`.
    pub order_by_indices: Vec<usize>,
    /// Column indices participating in `PARTITION BY`.
    pub partition_by_indices: Vec<usize>,
}
