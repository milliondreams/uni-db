// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! DataFusion adapter for plugin window functions.
//!
//! Bridges the [`WindowPluginFn`]
//! trait (bespoke `evaluate(partition, frame)` shape) onto DataFusion's
//! `WindowUDFImpl` + `PartitionEvaluator`, so a registered window plugin is
//! callable from a Cypher `... OVER (PARTITION BY ...)` clause. Mirrors the
//! aggregate adapter [`crate::query::df_udaf_plugin::PluginAggregateUdaf`].
//!
//! v1 evaluates over the **whole partition** (each row's frame spans the entire
//! partition); explicit `ROWS`/`RANGE` frame narrowing is not yet forwarded.

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, FieldRef, Schema};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{PartitionEvaluator, Signature, TypeSignature, WindowUDFImpl};
use uni_plugin::registry::WindowEntry;
use uni_plugin::traits::window::{WindowFrame, WindowPluginFn};

/// Map the plugin's declared return `ArgType` to a DataFusion `DataType`.
fn window_return_type(entry: &WindowEntry) -> DataType {
    use uni_plugin::traits::scalar::ArgType;
    match &entry.signature.returns {
        ArgType::Primitive(t) => t.clone(),
        _ => DataType::LargeBinary,
    }
}

/// A registered [`WindowPluginFn`] exposed as a DataFusion `WindowUDF`.
pub(crate) struct PluginWindowUdwf {
    name: String,
    window: Arc<dyn WindowPluginFn>,
    signature: Signature,
    return_type: DataType,
}

impl PluginWindowUdwf {
    pub(crate) fn new(name: String, entry: &WindowEntry) -> Self {
        Self {
            signature: Signature::new(TypeSignature::VariadicAny, entry.signature.volatility),
            name,
            window: Arc::clone(&entry.window),
            return_type: window_return_type(entry),
        }
    }
}

impl std::fmt::Debug for PluginWindowUdwf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginWindowUdwf")
            .field("name", &self.name)
            .finish()
    }
}

impl PartialEq for PluginWindowUdwf {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.signature == other.signature
    }
}

impl Eq for PluginWindowUdwf {}

impl Hash for PluginWindowUdwf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl WindowUDFImpl for PluginWindowUdwf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs,
    ) -> DFResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(PluginPartitionEvaluator {
            window: Arc::clone(&self.window),
        }))
    }
    fn field(&self, field_args: WindowUDFFieldArgs) -> DFResult<FieldRef> {
        Ok(Arc::new(Field::new(
            field_args.name(),
            self.return_type.clone(),
            true,
        )))
    }
}

/// Per-partition evaluator that hands the whole partition to the plugin.
struct PluginPartitionEvaluator {
    window: Arc<dyn WindowPluginFn>,
}

impl std::fmt::Debug for PluginPartitionEvaluator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginPartitionEvaluator").finish()
    }
}

impl PartitionEvaluator for PluginPartitionEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> DFResult<ArrayRef> {
        // Reassemble the argument columns into a RecordBatch — the plugin's
        // partition view. Field names are positional (`arg0`, `arg1`, …).
        let fields: Vec<Field> = values
            .iter()
            .enumerate()
            .map(|(i, a)| Field::new(format!("arg{i}"), a.data_type().clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), values.to_vec())
            .map_err(|e| DataFusionError::Execution(format!("window partition batch: {e}")))?;

        // v1: each row's frame is the entire partition.
        let frame = WindowFrame {
            schema,
            start: 0,
            end: num_rows,
            order_by_indices: Vec::new(),
            partition_by_indices: Vec::new(),
        };

        self.window
            .evaluate(&batch, frame)
            .map_err(|e| DataFusionError::Execution(format!("plugin window fn failed: {e}")))
    }
}
