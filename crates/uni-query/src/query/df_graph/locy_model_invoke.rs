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
use std::collections::HashMap;
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
use parking_lot::RwLock;
use uni_algo::algo::AlgorithmRegistry;
use uni_locy::{ClassifierRegistry, ModelInvocation, ModelInvocationCache};
use uni_store::runtime::L0Manager;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::storage::manager::StorageManager;
use uni_xervo::runtime::ModelRuntime;

use super::locy_fixpoint::apply_model_invocations;

/// Phase D D1 graph-structural runtime: a Clone+Debug bundle of the
/// pieces needed to invoke `uni.algo.*` procedures directly from the
/// FEATURE pipeline (no Cypher CALL roundtrip) and to traverse
/// one-hop neighborhoods for `avg_neighbor` / `max_neighbor` /
/// `sum_neighbor`.
///
/// Built fresh at physical-plan lowering (`df_planner.rs`) from
/// `GraphExecutionContext`; mirrors the `XervoRuntimeHandle` pattern
/// (logical plan is graph_ctx-agnostic).
#[derive(Clone, Default)]
pub struct GraphAlgoHandle {
    pub(crate) registry: Option<Arc<AlgorithmRegistry>>,
    pub(crate) storage: Option<Arc<StorageManager>>,
    pub(crate) l0_manager: Option<Arc<L0Manager>>,
    pub(crate) property_manager: Option<Arc<PropertyManager>>,
    /// Raw L0 buffers for building a fresh `QueryContext` when the
    /// neighbor-aggregator path calls `PropertyManager::get_vertex_prop_with_ctx`.
    /// L0-resident vertex properties are invisible to property reads
    /// without a `QueryContext`; topology procedures don't need this
    /// because they consume `L0Manager` directly via `AlgoContext`.
    pub(crate) l0_buffers: Option<L0Buffers>,
}

#[derive(Clone)]
pub(crate) struct L0Buffers {
    pub(crate) current: Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>,
    pub(crate) transaction: Option<Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
    pub(crate) pending_flush: Vec<Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
}

impl std::fmt::Debug for GraphAlgoHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.registry, &self.storage) {
            (Some(_), Some(_)) => write!(f, "GraphAlgoHandle(<configured>)"),
            _ => write!(f, "GraphAlgoHandle(<none>)"),
        }
    }
}

impl GraphAlgoHandle {
    pub fn is_configured(&self) -> bool {
        self.registry.is_some() && self.storage.is_some()
    }
}

/// Phase D D2 runtime: a Clone+Debug wrapper around the optional
/// Uni-Xervo runtime. `ModelRuntime` doesn't derive Debug (its
/// `providers: HashMap<String, Box<dyn ModelProvider>>` field
/// contains trait objects that aren't Debug), so we need this
/// shim to keep `LogicalPlan` derivable.
#[derive(Clone, Default)]
pub struct XervoRuntimeHandle(pub Option<Arc<ModelRuntime>>);

impl std::fmt::Debug for XervoRuntimeHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Some(_) => write!(f, "XervoRuntimeHandle(<configured>)"),
            None => write!(f, "XervoRuntimeHandle(<none>)"),
        }
    }
}

impl XervoRuntimeHandle {
    pub fn as_ref(&self) -> Option<&Arc<ModelRuntime>> {
        self.0.as_ref()
    }
}

/// Phase D D3 runtime: a shared handle into a source rule's derived
/// facts. The plan builder mints these for every `path_context.source_rule`
/// referenced by any invocation in the same `LocyModelInvoke`, so the
/// runtime can read the rule's `Vec<RecordBatch>` (already populated by
/// the fixpoint loop in an earlier stratum) and join by VID without
/// consulting the registry at exec time.
#[derive(Debug, Clone)]
pub struct PathContextHandle {
    pub source_rule: String,
    pub data: Arc<RwLock<Vec<RecordBatch>>>,
    pub schema: SchemaRef,
}

/// `ExecutionPlan` wrapper that runs `apply_model_invocations` over
/// the batches produced by `input`.
#[derive(Debug)]
pub struct LocyModelInvokeExec {
    input: Arc<dyn ExecutionPlan>,
    invocations: Vec<ModelInvocation>,
    registry: Arc<ClassifierRegistry>,
    cache: Option<Arc<ModelInvocationCache>>,
    /// Phase D D3: one handle per distinct `path_context.source_rule`
    /// referenced by the invocations on this node, indexed by rule name.
    path_context_handles: HashMap<String, PathContextHandle>,
    /// Phase D D2 runtime: Uni-Xervo runtime for auto-embedding
    /// `semantic_match(prop, 'text')` query literals once per
    /// `apply_model_invocations` call. `None` when no xervo runtime
    /// is configured — `semantic_match` calls then error with a
    /// clear message at row time.
    xervo_runtime: XervoRuntimeHandle,
    /// Phase D D1 graph-structural runtime: registry + storage handle
    /// for invoking topology algorithms (degree/pagerank/closeness)
    /// and walking one-hop neighborhoods. Built from `GraphExecutionContext`
    /// at physical lowering.
    graph_algo: GraphAlgoHandle,
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        invocations: Vec<ModelInvocation>,
        registry: Arc<ClassifierRegistry>,
        cache: Option<Arc<ModelInvocationCache>>,
        provenance_store: Option<Arc<uni_locy::NeuralProvenanceStore>>,
        path_context_handles: HashMap<String, PathContextHandle>,
        xervo_runtime: XervoRuntimeHandle,
        graph_algo: GraphAlgoHandle,
    ) -> Self {
        let schema = compute_output_schema(input.schema(), &invocations);
        let plan_properties = compute_plan_properties(&input, schema.clone());
        Self {
            input,
            invocations,
            registry,
            cache,
            provenance_store,
            path_context_handles,
            xervo_runtime,
            graph_algo,
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
            self.path_context_handles.clone(),
            self.xervo_runtime.clone(),
            self.graph_algo.clone(),
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
        let path_context_handles = self.path_context_handles.clone();
        let xervo_runtime = self.xervo_runtime.clone();
        let graph_algo = self.graph_algo.clone();
        let schema = self.schema.clone();

        let fut = async move {
            let batches: Vec<RecordBatch> = input_stream.try_collect::<Vec<_>>().await?;
            let out = apply_model_invocations(
                batches,
                &invocations,
                &registry,
                cache.as_ref(),
                provenance_store.as_ref(),
                &path_context_handles,
                &xervo_runtime,
                &graph_algo,
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
