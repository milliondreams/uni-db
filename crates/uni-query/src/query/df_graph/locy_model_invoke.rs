//! Phase B A4: `LocyModelInvokeExec` — a DataFusion `ExecutionPlan`
//! node that runs registered neural classifiers against its input
//! batches.
//!
//! This is the structural successor to the legacy
//! `apply_model_invocations` post-projection record-batch pass that
//! lived inside `run_fixpoint_loop` / `LocyProgramExec::run_program`.
//! The behavior is byte-identical — the inner implementation
//! (`super::locy_fixpoint::apply_model_invocations`) is unchanged;
//! only the call site moved into a proper plan node so the
//! invocation is part of the DataFusion plan tree instead of a
//! post-execute mutation.
//!
//! Async-in-stream pattern: the input stream is collected into a
//! `Vec<RecordBatch>`, the async classifier pass mutates it, and the
//! result is yielded as a `RecordBatchStreamAdapter`. Same shape as
//! `mutation_common::execute_mutation_inner` — no novel async
//! machinery in the codebase.

use std::any::Any;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::TryStreamExt;
use uni_locy::{ClassifierRegistry, ModelInvocation, ModelInvocationCache};

use super::locy_fixpoint::apply_model_invocations;

/// `ExecutionPlan` wrapper that runs `apply_model_invocations` over
/// the batches produced by `input`.
#[derive(Debug)]
pub struct LocyModelInvokeExec {
    input: Arc<dyn ExecutionPlan>,
    invocations: Vec<ModelInvocation>,
    registry: Arc<ClassifierRegistry>,
    cache: Option<Arc<ModelInvocationCache>>,
    /// Phase C B1-B3 follow-up: per-query side-channel store for
    /// (raw, calibrated, confidence_band) tuples. Written per row
    /// per invocation by `apply_model_invocations`; consumed by
    /// EXPLAIN's `collect_neural_calls_for_row` to surface
    /// `NeuralProvenance` regardless of whether the invocation
    /// lives in YIELD / ALONG / FOLD position.
    provenance_store: Option<Arc<uni_locy::NeuralProvenanceStore>>,
    /// Output schema: the input schema, post-invocation. Since
    /// `apply_model_invocations` overwrites a placeholder column
    /// emitted by the compiler at the same name and forces it to
    /// `Float64`, the output schema equals the input schema with
    /// each invocation's `output_column` retyped to `Float64`.
    schema: SchemaRef,
    plan_properties: PlanProperties,
}

impl LocyModelInvokeExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        invocations: Vec<ModelInvocation>,
        registry: Arc<ClassifierRegistry>,
        cache: Option<Arc<ModelInvocationCache>>,
        provenance_store: Option<Arc<uni_locy::NeuralProvenanceStore>>,
    ) -> Self {
        let schema = compute_output_schema(input.schema(), &invocations);
        let plan_properties = compute_plan_properties(&input, schema.clone());
        Self {
            input,
            invocations,
            registry,
            cache,
            provenance_store,
            schema,
            plan_properties,
        }
    }
}

fn compute_output_schema(input_schema: SchemaRef, invocations: &[ModelInvocation]) -> SchemaRef {
    use arrow_schema::{DataType, Field, Schema};
    if invocations.is_empty() {
        return input_schema;
    }
    let mut fields: Vec<Arc<Field>> = input_schema.fields().iter().cloned().collect();
    for invocation in invocations {
        if let Some((idx, _)) = input_schema
            .fields()
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == &invocation.output_column)
        {
            fields[idx] = Arc::new(Field::new(
                &invocation.output_column,
                DataType::Float64,
                true,
            ));
        } else {
            fields.push(Arc::new(Field::new(
                &invocation.output_column,
                DataType::Float64,
                true,
            )));
        }
    }
    Arc::new(Schema::new(fields))
}

fn compute_plan_properties(input: &Arc<dyn ExecutionPlan>, schema: SchemaRef) -> PlanProperties {
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};

    let eq = EquivalenceProperties::new(schema);
    PlanProperties::new(
        eq,
        input.properties().output_partitioning().clone(),
        EmissionType::Final,
        Boundedness::Bounded,
    )
}

impl DisplayAs for LocyModelInvokeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "LocyModelInvokeExec: invocations=[{}]",
            self.invocations
                .iter()
                .map(|inv| format!("{}→{}", inv.model_name, inv.output_column))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl ExecutionPlan for LocyModelInvokeExec {
    fn name(&self) -> &str {
        "LocyModelInvokeExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "LocyModelInvokeExec expects exactly 1 child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self::new(
            children.into_iter().next().unwrap(),
            self.invocations.clone(),
            Arc::clone(&self.registry),
            self.cache.as_ref().map(Arc::clone),
            self.provenance_store.as_ref().map(Arc::clone),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let invocations = self.invocations.clone();
        let registry = Arc::clone(&self.registry);
        let cache = self.cache.as_ref().map(Arc::clone);
        let provenance_store = self.provenance_store.as_ref().map(Arc::clone);
        let schema = self.schema.clone();

        let fut = async move {
            let batches: Vec<RecordBatch> = input_stream.try_collect::<Vec<_>>().await?;
            let out = apply_model_invocations(
                batches,
                &invocations,
                &registry,
                cache.as_ref(),
                provenance_store.as_ref(),
            )
            .await?;
            // Wrap the Vec<RecordBatch> as a stream so try_flatten
            // can splice it inline.
            Ok::<_, datafusion::error::DataFusionError>(futures::stream::iter(
                out.into_iter().map(Ok),
            ))
        };
        let stream = futures::stream::once(fut).try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
