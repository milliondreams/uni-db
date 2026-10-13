// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `LocyGenerateExec`: a Locy generator condition (`name(args) -> (outs)`) as
//! a columnar operator.
//!
//! A generator binds new variables one-to-many: for each input row it emits
//! zero or more tuples, and each tuple becomes an output row carrying the input
//! row's columns plus the bound outputs. The plugin contract is already
//! columnar ([`LocyGenerator::generate`] takes the argument columns and returns
//! a `row_map` from output row to input row), so the operator evaluates the
//! arguments, calls the generator once per batch, and gathers the input rows
//! through the `row_map`.
//!
//! Before this operator existed the fixpoint skipped every rule with a
//! generator, and only a QUERY re-derived it through the in-memory evaluator:
//! a rule referencing it saw no facts, and its FOLD, BEST BY and post-FOLD
//! WHERE were ignored.

// Rust guideline compliant

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use uni_plugin::traits::locy::LocyGenerator;

use crate::query::df_graph::common::operator_reservation;

/// Runs one generator condition over its input; see the module documentation.
pub struct LocyGenerateExec {
    input: Arc<dyn ExecutionPlan>,
    name: String,
    generator: Arc<dyn LocyGenerator>,
    args: Vec<Arc<dyn PhysicalExpr>>,
    schema: SchemaRef,
    plan_properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for LocyGenerateExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocyGenerateExec")
            .field("name", &self.name)
            .field("args", &self.args)
            .finish_non_exhaustive()
    }
}

impl LocyGenerateExec {
    /// Builds the operator.
    ///
    /// `outputs` names the bound output columns, typed as the generator's
    /// signature declares; they are appended to the input's columns.
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        name: String,
        generator: Arc<dyn LocyGenerator>,
        args: Vec<Arc<dyn PhysicalExpr>>,
        outputs: Vec<Field>,
    ) -> Self {
        let fields: Vec<Arc<Field>> = input
            .schema()
            .fields()
            .iter()
            .cloned()
            .chain(outputs.into_iter().map(Arc::new))
            .collect();
        let schema: SchemaRef = Arc::new(Schema::new(fields));
        let plan_properties = plan_properties(&input, Arc::clone(&schema));
        Self {
            input,
            name,
            generator,
            args,
            schema,
            plan_properties,
        }
    }
}

fn plan_properties(input: &Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Arc<PlanProperties> {
    use datafusion::physical_expr::EquivalenceProperties;
    Arc::new(PlanProperties::new(
        EquivalenceProperties::new(schema),
        input.properties().output_partitioning().clone(),
        input.properties().emission_type,
        input.properties().boundedness,
    ))
}

/// Applies the generator to one batch.
fn generate_batch(
    batch: &RecordBatch,
    name: &str,
    generator: &dyn LocyGenerator,
    args: &[Arc<dyn PhysicalExpr>],
    schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    let values = args
        .iter()
        .map(|arg| arg.evaluate(batch))
        .collect::<DFResult<Vec<_>>>()?;
    let output = generator
        .generate(&values, batch.num_rows())
        .map_err(|e| DataFusionError::Execution(format!("generator '{name}': {e}")))?;
    let input_columns = batch.num_columns();
    let declared = schema.fields().len() - input_columns;
    if output.columns.len() != declared {
        return Err(DataFusionError::Execution(format!(
            "generator '{name}' returned {} columns but binds {declared} output variable(s)",
            output.columns.len()
        )));
    }
    if let Some(bad) = output
        .row_map
        .iter()
        .find(|&&r| r as usize >= batch.num_rows())
    {
        return Err(DataFusionError::Execution(format!(
            "generator '{name}' mapped an output row to input row {bad} of {}",
            batch.num_rows()
        )));
    }
    let indices = UInt32Array::from(output.row_map);
    let mut columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c.as_ref(), &indices, None))
        .collect::<Result<_, _>>()?;
    for (column, field) in output
        .columns
        .into_iter()
        .zip(schema.fields().iter().skip(input_columns))
    {
        if column.len() != indices.len() {
            return Err(DataFusionError::Execution(format!(
                "generator '{name}' returned {} values for output '{}' but {} rows",
                column.len(),
                field.name(),
                indices.len()
            )));
        }
        let column = if column.data_type() == field.data_type() {
            column
        } else {
            arrow::compute::cast(&column, field.data_type())?
        };
        columns.push(column);
    }
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

impl DisplayAs for LocyGenerateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LocyGenerateExec: {}", self.name)
    }
}

impl ExecutionPlan for LocyGenerateExec {
    fn name(&self) -> &str {
        "LocyGenerateExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let [input] = <[_; 1]>::try_from(children).map_err(|c| {
            DataFusionError::Internal(format!(
                "LocyGenerateExec expects exactly 1 child, got {}",
                c.len()
            ))
        })?;
        let outputs = self
            .schema
            .fields()
            .iter()
            .skip(self.input.schema().fields().len())
            .map(|f| f.as_ref().clone())
            .collect();
        Ok(Arc::new(Self::new(
            input,
            self.name.clone(),
            Arc::clone(&self.generator),
            self.args.clone(),
            outputs,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // #261: one output batch in flight; a generator can multiply its input.
        let reservation = operator_reservation("LocyGenerateExec", partition, &context);
        let input = self.input.execute(partition, context)?;
        let name = self.name.clone();
        let generator = Arc::clone(&self.generator);
        let args = self.args.clone();
        let schema = Arc::clone(&self.schema);
        let stream_schema = Arc::clone(&self.schema);
        let stream = input.map(move |batch| {
            let out = generate_batch(&batch?, &name, generator.as_ref(), &args, &schema)?;
            reservation.try_resize(out.get_array_memory_size())?;
            Ok(out)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }
}
