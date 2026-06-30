//! Built-in pushdown-negotiation optimizer rule (M5h).
//!
//! Walks five logical-plan patterns and consults the underlying
//! `TableSource` for the matching marker trait from
//! [`uni_plugin::traits::pushdown`]:
//!
//! - `Filter → TableScan` ⇒ [`SupportsFilterPushdown`]
//! - `Projection → TableScan` ⇒ [`SupportsProjectionPushdown`]
//! - `Limit → TableScan` ⇒ [`SupportsLimitPushdown`]
//! - `Sort → Limit → TableScan` (TopN) ⇒ [`SupportsTopNPushdown`]
//! - `Aggregate → TableScan` ⇒ [`SupportsAggregatePushdown`]
//!
//! When a source fully handles a pattern, the surrounding wrapper
//! node is elided. The Filter case elides only when *all* predicates
//! are fully handled. Projection / Limit / TopN / Aggregate elide
//! when the source acknowledges the operation.
//!
//! Tables that wish to opt into pushdown wrap their `TableProvider`
//! in [`PushdownAwareTable`] (see the struct doc for marker setup).
//
// Rust guideline compliant

use std::sync::Arc;

use datafusion::common::tree_node::Transformed;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    Aggregate, Expr, FetchType, Filter, LogicalPlan, Projection, SkipType, Sort as SortPlan,
    TableScan,
};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use uni_plugin::traits::operator::{OptimizerPhase, OptimizerRuleProvider};
use uni_plugin::traits::pushdown::{
    SortExpr as MarkerSortExpr, SupportsAggregatePushdown, SupportsFilterPushdown,
    SupportsLimitPushdown, SupportsProjectionPushdown, SupportsTopNPushdown,
};

/// Marker bundle surfaced by [`PushdownAwareTable`].
///
/// Each capability is optional so users can opt into only the markers
/// their source actually implements.
#[derive(Default)]
pub struct PushdownMarkers {
    /// Filter pushdown marker (legacy single-field, kept for v1.6 API
    /// compatibility — the bundle below now drives all five rules).
    pub filter: Option<Arc<dyn SupportsFilterPushdown + Send + Sync>>,
    /// Projection pushdown marker.
    pub projection: Option<Arc<dyn SupportsProjectionPushdown + Send + Sync>>,
    /// Limit pushdown marker.
    pub limit: Option<Arc<dyn SupportsLimitPushdown + Send + Sync>>,
    /// TopN (`Sort → Limit`) pushdown marker.
    pub topn: Option<Arc<dyn SupportsTopNPushdown + Send + Sync>>,
    /// Aggregate pushdown marker.
    pub aggregate: Option<Arc<dyn SupportsAggregatePushdown + Send + Sync>>,
}

impl std::fmt::Debug for PushdownMarkers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PushdownMarkers")
            .field("filter", &self.filter.is_some())
            .field("projection", &self.projection.is_some())
            .field("limit", &self.limit.is_some())
            .field("topn", &self.topn.is_some())
            .field("aggregate", &self.aggregate.is_some())
            .finish()
    }
}

/// The built-in pushdown-negotiation rule.
///
/// Logical-phase rule that elides `Filter` / `Projection` / `Limit` /
/// `Sort+Limit` / `Aggregate` nodes whose work the underlying source
/// claims to fully handle.
#[derive(Debug, Default)]
pub struct PushdownNegotiationRule;

impl OptimizerRule for PushdownNegotiationRule {
    fn name(&self) -> &str {
        "uni_pushdown_negotiation"
    }

    fn apply_order(&self) -> Option<datafusion::optimizer::ApplyOrder> {
        Some(datafusion::optimizer::ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        // Try each pattern in turn — each `try_*` returns `Ok(None)` if
        // the pattern does not match or the source does not opt in, and
        // `Ok(Some(rewritten))` when the rewrite fires. The original
        // plan is moved into each attempt and threaded back out
        // unchanged on miss.
        let plan = match try_rewrite_filter(plan)? {
            Ok(rewritten) => return Ok(Transformed::yes(rewritten)),
            Err(plan) => plan,
        };
        let plan = match try_rewrite_projection(plan)? {
            Ok(rewritten) => return Ok(Transformed::yes(rewritten)),
            Err(plan) => plan,
        };
        let plan = match try_rewrite_topn(plan)? {
            Ok(rewritten) => return Ok(Transformed::yes(rewritten)),
            Err(plan) => plan,
        };
        let plan = match try_rewrite_limit(plan)? {
            Ok(rewritten) => return Ok(Transformed::yes(rewritten)),
            Err(plan) => plan,
        };
        let plan = match try_rewrite_aggregate(plan)? {
            Ok(rewritten) => return Ok(Transformed::yes(rewritten)),
            Err(plan) => plan,
        };
        Ok(Transformed::no(plan))
    }
}

/// Filter → TableScan rewrite.
///
/// Elides the `Filter` only when the source reports every predicate
/// as `fully_handled` (preserving correctness when partial handling
/// is reported).
fn try_rewrite_filter(
    plan: LogicalPlan,
) -> Result<Result<LogicalPlan, LogicalPlan>, DataFusionError> {
    let LogicalPlan::Filter(Filter {
        predicate, input, ..
    }) = &plan
    else {
        return Ok(Err(plan));
    };
    let LogicalPlan::TableScan(scan) = input.as_ref() else {
        return Ok(Err(plan));
    };
    let Some(markers) = downcast_markers(scan.source.as_any()) else {
        return Ok(Err(plan));
    };
    let Some(filter_marker) = markers.filter.as_ref() else {
        return Ok(Err(plan));
    };
    let filters = [predicate.clone()];
    let app = filter_marker.push_filters(&filters);
    if app.fully_handled.contains(&0) {
        tracing::debug!(
            target: "uni.plugin.optimizer",
            rule = "uni_pushdown_negotiation",
            pattern = "filter",
            "filter fully pushed down; eliding Filter node"
        );
        // Embed the predicate into `TableScan::filters` so the source
        // actually receives it via `TableProvider::scan(filters, …)`.
        // Without this, the rule strips the Filter node from the
        // logical plan but the source sees an empty predicate slice
        // and returns every row — silent over-fetch.
        let mut new_scan = scan.clone();
        new_scan.filters.push(predicate.clone());
        return Ok(Ok(LogicalPlan::TableScan(new_scan)));
    }
    Ok(Err(plan))
}

/// Projection → TableScan rewrite.
///
/// Elides the `Projection` when the source acknowledges every
/// projected column (i.e. `keep` covers them all). Only handles the
/// pure `Expr::Column` projection case; expression projections fall
/// through unchanged.
fn try_rewrite_projection(
    plan: LogicalPlan,
) -> Result<Result<LogicalPlan, LogicalPlan>, DataFusionError> {
    let LogicalPlan::Projection(Projection {
        expr,
        input,
        schema: proj_schema,
        ..
    }) = &plan
    else {
        return Ok(Err(plan));
    };
    let LogicalPlan::TableScan(TableScan {
        source,
        projected_schema,
        ..
    }) = input.as_ref()
    else {
        return Ok(Err(plan));
    };
    // Soundness guard: only elide when the projection is an identity
    // over the scan's Arrow schema. Compare inner `arrow::Schema` to
    // ignore qualifier differences.
    if projected_schema.inner() != proj_schema.inner() {
        return Ok(Err(plan));
    }
    let Some(markers) = downcast_markers(source.as_any()) else {
        return Ok(Err(plan));
    };
    let Some(proj_marker) = markers.projection.as_ref() else {
        return Ok(Err(plan));
    };
    // Extract column names; bail if any projection expression is not a
    // bare column reference.
    let mut requested: Vec<String> = Vec::with_capacity(expr.len());
    for e in expr {
        match e {
            Expr::Column(col) => requested.push(col.name.clone()),
            _ => return Ok(Err(plan)),
        }
    }
    let app = proj_marker.push_projection(&requested);
    if app.keep.len() == requested.len() && requested.iter().all(|c| app.keep.contains(c)) {
        tracing::debug!(
            target: "uni.plugin.optimizer",
            rule = "uni_pushdown_negotiation",
            pattern = "projection",
            "projection fully pushed down; eliding Projection node"
        );
        return Ok(Ok((**input).clone()));
    }
    Ok(Err(plan))
}

/// Limit → TableScan rewrite.
///
/// Elides the `Limit` when the source acknowledges the requested
/// fetch (`Some(applied)` returned). Skip-aware limits (`OFFSET`) are
/// left to the planner.
fn try_rewrite_limit(
    plan: LogicalPlan,
) -> Result<Result<LogicalPlan, LogicalPlan>, DataFusionError> {
    let LogicalPlan::Limit(limit) = &plan else {
        return Ok(Err(plan));
    };
    // Only handle simple `LIMIT n` with no skip.
    let SkipType::Literal(0) = limit.get_skip_type()? else {
        return Ok(Err(plan));
    };
    let FetchType::Literal(Some(fetch)) = limit.get_fetch_type()? else {
        return Ok(Err(plan));
    };
    let child = peel_transparent_projection(limit.input.as_ref());
    let LogicalPlan::TableScan(TableScan { source, .. }) = child else {
        return Ok(Err(plan));
    };
    let Some(markers) = downcast_markers(source.as_any()) else {
        return Ok(Err(plan));
    };
    let Some(limit_marker) = markers.limit.as_ref() else {
        return Ok(Err(plan));
    };
    if limit_marker.push_limit(fetch).is_some() {
        tracing::debug!(
            target: "uni.plugin.optimizer",
            rule = "uni_pushdown_negotiation",
            pattern = "limit",
            fetch,
            "limit fully pushed down; eliding Limit node"
        );
        // Return the original (possibly Projection-wrapped) input so
        // the surrounding schema contract is preserved.
        let inner: &LogicalPlan = limit.input.as_ref();
        return Ok(Ok(inner.clone()));
    }
    Ok(Err(plan))
}

/// Sort → Limit → TableScan (TopN) rewrite.
///
/// Elides both the `Sort` and the `Limit` when the source returns
/// [`uni_plugin::traits::pushdown::TopNScope::Global`]. `Local`-scoped
/// TopN is left to the planner to combine.
fn try_rewrite_topn(
    plan: LogicalPlan,
) -> Result<Result<LogicalPlan, LogicalPlan>, DataFusionError> {
    // Pattern A: Sort with fetch built in (DataFusion folds Limit→Sort).
    if let LogicalPlan::Sort(SortPlan { expr, input, fetch }) = &plan
        && let Some(k) = fetch
    {
        let child = peel_transparent_projection(input.as_ref());
        if let LogicalPlan::TableScan(TableScan { source, .. }) = child
            && let Some(markers) = downcast_markers(source.as_any())
            && let Some(topn_marker) = markers.topn.as_ref()
        {
            let sort = sort_exprs_to_marker(expr);
            if let Some(app) = topn_marker.push_topn(&sort, *k) {
                use uni_plugin::traits::pushdown::TopNScope;
                if matches!(app.applied, TopNScope::Global) {
                    tracing::debug!(
                        target: "uni.plugin.optimizer",
                        rule = "uni_pushdown_negotiation",
                        pattern = "topn",
                        k = *k,
                        "topN fully pushed down (Global); eliding Sort"
                    );
                    return Ok(Ok((**input).clone()));
                }
            }
        }
    }
    // Pattern B: Limit → Sort → TableScan (Limit has not been folded
    // into Sort.fetch yet).
    if let LogicalPlan::Limit(limit) = &plan
        && let Ok(SkipType::Literal(0)) = limit.get_skip_type()
        && let Ok(FetchType::Literal(Some(k))) = limit.get_fetch_type()
        && let LogicalPlan::Sort(SortPlan { expr, input, .. }) = limit.input.as_ref()
    {
        let child = peel_transparent_projection(input.as_ref());
        if let LogicalPlan::TableScan(TableScan { source, .. }) = child
            && let Some(markers) = downcast_markers(source.as_any())
            && let Some(topn_marker) = markers.topn.as_ref()
        {
            let sort = sort_exprs_to_marker(expr);
            if let Some(app) = topn_marker.push_topn(&sort, k) {
                use uni_plugin::traits::pushdown::TopNScope;
                if matches!(app.applied, TopNScope::Global) {
                    tracing::debug!(
                        target: "uni.plugin.optimizer",
                        rule = "uni_pushdown_negotiation",
                        pattern = "topn",
                        k,
                        "topN fully pushed down (Global); eliding Sort+Limit"
                    );
                    // Return the Sort's input (possibly Projection) so
                    // schema is preserved downstream.
                    let inner: &LogicalPlan = limit.input.as_ref();
                    if let LogicalPlan::Sort(SortPlan { input: si, .. }) = inner {
                        return Ok(Ok((**si).clone()));
                    }
                    return Ok(Ok(inner.clone()));
                }
            }
        }
    }
    Ok(Err(plan))
}

/// Aggregate → TableScan rewrite.
///
/// Elides the `Aggregate` when the source reports all aggregates as
/// fully handled and does not return partial state. Partial-state
/// aggregations require the planner to add a Final aggregate above —
/// keep the wrapper plan in that case.
fn try_rewrite_aggregate(
    plan: LogicalPlan,
) -> Result<Result<LogicalPlan, LogicalPlan>, DataFusionError> {
    let LogicalPlan::Aggregate(Aggregate {
        input,
        group_expr,
        aggr_expr,
        schema: agg_schema,
        ..
    }) = &plan
    else {
        return Ok(Err(plan));
    };
    let child = peel_transparent_projection(input.as_ref());
    let LogicalPlan::TableScan(TableScan {
        source,
        projected_schema,
        ..
    }) = child
    else {
        return Ok(Err(plan));
    };
    let Some(markers) = downcast_markers(source.as_any()) else {
        return Ok(Err(plan));
    };
    let Some(agg_marker) = markers.aggregate.as_ref() else {
        return Ok(Err(plan));
    };
    // Soundness guard: elision is only safe when the source's Arrow
    // schema already matches the aggregate's output Arrow schema.
    // (Compare the inner `arrow::Schema` rather than `DFSchema` so
    // table-qualifier mismatches don't spuriously block elision.)
    if projected_schema.inner() != agg_schema.inner() {
        return Ok(Err(plan));
    }
    let app = agg_marker.push_aggregates(group_expr, aggr_expr);
    let all_handled = !aggr_expr.is_empty()
        && aggr_expr
            .iter()
            .enumerate()
            .all(|(i, _)| app.fully_handled.contains(&i));
    if all_handled && !app.returns_partial_state {
        tracing::debug!(
            target: "uni.plugin.optimizer",
            rule = "uni_pushdown_negotiation",
            pattern = "aggregate",
            "aggregate fully pushed down; eliding Aggregate node"
        );
        return Ok(Ok((**input).clone()));
    }
    Ok(Err(plan))
}

/// Convert DataFusion sort expressions to marker-trait sort exprs.
///
/// Only `Expr::Column` keys are extracted; non-column sort
/// expressions are skipped (the source will see only the columns it
/// can act on).
fn sort_exprs_to_marker(sort: &[datafusion::logical_expr::SortExpr]) -> Vec<MarkerSortExpr> {
    sort.iter()
        .filter_map(|s| match &s.expr {
            Expr::Column(col) => Some(MarkerSortExpr {
                column: col.name.clone(),
                ascending: s.asc,
                nulls_first: s.nulls_first,
            }),
            _ => None,
        })
        .collect()
}

/// Peel a single transparent `Projection` node above a `TableScan`.
///
/// DataFusion injects an identity `Projection` for `SELECT *` queries
/// even before the user-supplied optimizer chain runs. When that
/// projection is the identity (same schema as the scan), we look
/// through it so the Limit / Sort / Aggregate rules can still match
/// the underlying scan.
fn peel_transparent_projection(plan: &LogicalPlan) -> &LogicalPlan {
    if let LogicalPlan::Projection(Projection {
        input,
        schema: proj_schema,
        ..
    }) = plan
        && let LogicalPlan::TableScan(TableScan {
            projected_schema, ..
        }) = input.as_ref()
        && projected_schema.inner() == proj_schema.inner()
    {
        return input.as_ref();
    }
    plan
}

/// Try to downcast a `&dyn Any` (from `TableSource::as_any`) to a
/// [`PushdownAwareTable`] and surface its [`PushdownMarkers`] bundle.
///
/// Path 1: source is itself a `PushdownAwareTable` (rare; users would
/// have to implement `TableSource` directly).
/// Path 2: `DefaultTableSource` → inner `TableProvider` → wrapper.
fn downcast_markers(source_any: &dyn std::any::Any) -> Option<&PushdownMarkers> {
    if let Some(pa) = source_any.downcast_ref::<PushdownAwareTable>() {
        return Some(&pa.markers);
    }
    let default = source_any
        .downcast_ref::<datafusion::datasource::default_table_source::DefaultTableSource>()?;
    let provider_any = default.table_provider.as_any();
    let pa = provider_any.downcast_ref::<PushdownAwareTable>()?;
    Some(&pa.markers)
}

/// Pushdown-aware wrapper around a DataFusion `TableProvider`.
///
/// Tests and user plugins wrap their `TableProvider` instance in this
/// type so the [`PushdownNegotiationRule`] can recognise pushdown
/// markers. The wrapper transparently delegates the `TableProvider`
/// interface to the inner provider.
///
/// The legacy `marker` field (single [`SupportsFilterPushdown`]) is
/// kept as a convenience constructor input — see [`Self::with_filter`]
/// — but new code should populate the [`PushdownMarkers`] bundle
/// directly.
pub struct PushdownAwareTable {
    /// The wrapped table provider.
    pub inner: Arc<dyn datafusion::datasource::TableProvider>,
    /// Bundle of optional pushdown markers surfaced to the optimizer.
    pub markers: PushdownMarkers,
}

impl PushdownAwareTable {
    /// Build a wrapper exposing only a filter-pushdown marker.
    ///
    /// Convenience constructor for the v1.6 single-marker path; new
    /// callers should populate [`PushdownMarkers`] directly via the
    /// public field.
    pub fn with_filter(
        inner: Arc<dyn datafusion::datasource::TableProvider>,
        marker: Arc<dyn SupportsFilterPushdown + Send + Sync>,
    ) -> Self {
        Self {
            inner,
            markers: PushdownMarkers {
                filter: Some(marker),
                ..PushdownMarkers::default()
            },
        }
    }
}

impl std::fmt::Debug for PushdownAwareTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PushdownAwareTable")
            .field("markers", &self.markers)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl datafusion::datasource::TableProvider for PushdownAwareTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.inner.schema()
    }
    fn table_type(&self) -> datafusion::logical_expr::TableType {
        self.inner.table_type()
    }
    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }
}

/// Provider that registers [`PushdownNegotiationRule`] into the plugin
/// registry's logical-phase optimizer chain.
#[derive(Debug, Default)]
pub struct PushdownNegotiationProvider;

impl OptimizerRuleProvider for PushdownNegotiationProvider {
    fn rule(&self) -> Arc<dyn OptimizerRule + Send + Sync> {
        Arc::new(PushdownNegotiationRule)
    }

    fn phase(&self) -> OptimizerPhase {
        OptimizerPhase::Logical
    }

    fn precedence(&self) -> i32 {
        100
    }
}
