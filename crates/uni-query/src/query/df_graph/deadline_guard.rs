// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A pass-through operator that checks the query deadline on every poll.
//!
//! Issue #283: a query given a 2-second timeout ran for sixteen and then
//! reported `UniTimeoutError`. The deadline was plumbed to every operator, and
//! the collecting loop checked it before each batch — but neither could fire.
//!
//! The reason is where the time is spent. A pipeline-breaking operator —
//! `AggregateExec` under `count(*)`, a sort, a join build — consumes its entire
//! input inside a single `poll_next` before emitting anything. The loop above it
//! therefore gets exactly one batch, at the end, so its per-batch check runs
//! once at t≈0 and once after the work is over. `tokio::time::timeout` cannot
//! preempt it either: a CPU-bound span that never yields `Pending` never lets
//! the timer run. What remained was an `Instant::now() > deadline` test after
//! the rows existed, which reports an overrun rather than preventing one.
//!
//! The fix is to put a checkpoint where the polling still happens. Inserting
//! this guard *beneath* a pipeline breaker means its `poll_next` runs once per
//! input batch — the aggregate is still draining, and each pull passes through
//! here. Operators we do not own stay uninstrumented, but they no longer need to
//! be: they pull their input through a checkpoint on the way.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::Stream;
use tokio_util::sync::CancellationToken;

use super::common::check_deadline;

/// Wraps `input` and checks the deadline before yielding each of its batches.
#[derive(Debug)]
pub(crate) struct DeadlineGuardExec {
    input: Arc<dyn ExecutionPlan>,
    deadline: Option<std::time::Instant>,
    token: Option<CancellationToken>,
    properties: Arc<PlanProperties>,
}

impl DeadlineGuardExec {
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        deadline: Option<std::time::Instant>,
        token: Option<CancellationToken>,
    ) -> Self {
        // Inherit the child's properties wholesale. This is a pure pass-through,
        // so everything a parent reasons about — schema, partitioning, output
        // ordering and the equivalence classes — must be the child's, unchanged.
        //
        // Rebuilding them from the schema instead loses the equivalences, and a
        // projection above then resolves a column by position against the wrong
        // name: `Input field name l does not match with the projection
        // expression d`. A pass-through that alters what it passes through is
        // not a pass-through.
        let properties = Arc::clone(input.properties());
        Self {
            input,
            deadline,
            token,
            properties,
        }
    }
}

impl DisplayAs for DeadlineGuardExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DeadlineGuardExec")
    }
}

impl ExecutionPlan for DeadlineGuardExec {
    fn name(&self) -> &str {
        "DeadlineGuardExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DeadlineGuardExec::new(
            children
                .into_iter()
                .next()
                .ok_or_else(|| DataFusionError::Internal("guard needs one child".into()))?,
            self.deadline,
            self.token.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        Ok(Box::pin(DeadlineGuardStream {
            input: self.input.execute(partition, context)?,
            deadline: self.deadline,
            token: self.token.clone(),
        }))
    }
}

struct DeadlineGuardStream {
    input: SendableRecordBatchStream,
    deadline: Option<std::time::Instant>,
    token: Option<CancellationToken>,
}

impl Stream for DeadlineGuardStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Err(e) = check_deadline(self.token.as_ref(), self.deadline) {
            return Poll::Ready(Some(Err(DataFusionError::Execution(e.to_string()))));
        }
        Pin::new(&mut self.input).poll_next(cx)
    }
}

impl RecordBatchStream for DeadlineGuardStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

/// Operators known to hold no contract with their children beyond the data.
///
/// Deliberately an allow-list rather than a deny-list of this project's
/// operators: a new operator here is presumed to have a contract until someone
/// establishes otherwise, which fails towards leaving a query uninstrumented
/// rather than towards changing its answer.
fn is_stock_datafusion(name: &str) -> bool {
    matches!(
        name,
        "AggregateExec"
            | "CoalesceBatchesExec"
            | "CoalescePartitionsExec"
            | "CrossJoinExec"
            | "FilterExec"
            | "GlobalLimitExec"
            | "HashJoinExec"
            | "LocalLimitExec"
            | "NestedLoopJoinExec"
            | "PlaceholderRowExec"
            | "ProjectionExec"
            | "RepartitionExec"
            | "SortExec"
            | "SortMergeJoinExec"
            | "SortPreservingMergeExec"
            | "UnionExec"
    )
}

/// Whether a node may be wrapped, judged over its whole subtree.
///
/// Wrapping is only safe where nothing below reasons about plan shape. This
/// project's operators do: UNWIND source pruning inspects what feeds it, and
/// entity materialization depends on where it sits — with a node inserted,
/// `collect(DISTINCT n)` stopped collapsing two representations of the same
/// vertex, returning 2 where it had returned 1. That is a wrong answer, and no
/// timeout is worth one.
///
/// A scan is the exception: it is a leaf, so it has no child relationship to
/// disturb, and the plans that need a checkpoint most are scans joined and
/// aggregated by stock operators.
fn subtree_is_insertable(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let name = plan.name();
    let ours_and_safe = matches!(
        name,
        // A leaf: no child relationship to disturb.
        "GraphScanExec"
            // Idempotent.
            | "DeadlineGuardExec"
            // Holds column *indices* resolved against its input schema and
            // clones them verbatim on rebuild. The guard is a pass-through that
            // inherits its child's properties exactly, so those indices stay
            // valid, and nothing outside reasons about what feeds it. Without
            // this a Locy evaluation gets no checkpoint at all, because the
            // read-set recorder sits over every clause body under SSI.
            | "ReadSetRecordingExec"
    );
    if !(is_stock_datafusion(name) || ours_and_safe) {
        return false;
    }
    plan.children().iter().all(|c| subtree_is_insertable(c))
}

/// Inserts a [`DeadlineGuardExec`] beneath every pipeline-breaking operator.
///
/// `EmissionType::Final` is DataFusion's own name for "emits only after
/// consuming all input", which is exactly the shape that swallows a deadline.
/// Guarding the *children* of such a node, rather than every node, keeps the
/// insertion rare and puts it where the polling actually repeats.
///
/// A guard is never placed directly above another guard, and never around a
/// node that is itself already a guard.
pub(crate) fn insert_deadline_guards(
    plan: Arc<dyn ExecutionPlan>,
    deadline: Option<std::time::Instant>,
    token: Option<&CancellationToken>,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    if deadline.is_none() && token.is_none() {
        return Ok(plan);
    }

    // Inserting a node means rebuilding every ancestor through
    // `with_new_children`, and not every plan survives that: some of the
    // UNWIND/collect shapes carry projections that fail to re-validate against
    // a rebuilt child, with `Input field name l does not match with the
    // projection expression d`. That fragility predates this guard, and a
    // checkpoint is not worth failing a query that used to run.
    //
    // So the rewrite is advisory. If it cannot be built, or it would change the
    // output schema, the original plan is used unchanged — those plans keep the
    // behaviour they had, which is a deadline noticed late rather than never.
    let original_schema = plan.schema();
    match insert_inner(Arc::clone(&plan), deadline, token) {
        Ok(rewritten) if rewritten.schema() == original_schema => Ok(rewritten),
        Ok(_) => {
            tracing::debug!("deadline guards skipped: rewriting changed the output schema");
            Ok(plan)
        }
        Err(e) => {
            tracing::debug!(
                error = %e,
                "deadline guards skipped: this plan cannot be rebuilt with inserted nodes"
            );
            Ok(plan)
        }
    }
}

fn insert_inner(
    plan: Arc<dyn ExecutionPlan>,
    deadline: Option<std::time::Instant>,
    token: Option<&CancellationToken>,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    use datafusion::physical_plan::execution_plan::EmissionType;

    let children = plan.children();
    if children.is_empty() {
        return Ok(plan);
    }

    // `EmissionType` propagates upward, so this is true of a pipeline breaker
    // and of everything above it. That breadth is wanted: the checkpoint has to
    // sit under the *aggregate*, whose child the aggregate re-polls per batch,
    // and restricting to the node that first introduces the property excludes
    // exactly that placement. What keeps the breadth safe is the child test
    // below, not this one.
    let breaks_pipeline = matches!(
        plan.properties().emission_type,
        EmissionType::Final | EmissionType::Both
    );

    let mut new_children = Vec::with_capacity(children.len());
    let mut changed = false;
    for child in children {
        let rewritten = insert_inner(Arc::clone(child), deadline, token)?;
        // DF 54 dropped `as_any` from `ExecutionPlan`, so identify by name.
        let already_guarded = rewritten.name() == "DeadlineGuardExec";
        // Only above a leaf or a stock DataFusion operator.
        //
        // This project's operators carry parent/child contracts: UNWIND source
        // pruning inspects what feeds it, and entity materialization depends on
        // where it sits. Inserting a node between such a pair is not neutral —
        // it changed what `DISTINCT` considered equal, and made a rebuilt
        // projection resolve a column against the wrong name. Stock DataFusion
        // operators make no such assumption about their children, and a leaf has
        // no relationship to disturb.
        let child_is_insertable = subtree_is_insertable(&rewritten);
        let guarded = if breaks_pipeline && child_is_insertable && !already_guarded {
            changed = true;
            Arc::new(DeadlineGuardExec::new(rewritten, deadline, token.cloned()))
                as Arc<dyn ExecutionPlan>
        } else {
            rewritten
        };
        if !Arc::ptr_eq(&guarded, child) {
            changed = true;
        }
        new_children.push(guarded);
    }

    if changed {
        plan.with_new_children(new_children)
    } else {
        Ok(plan)
    }
}
