#![allow(dead_code, unused_imports, clippy::all)]
//! M5h — verify the built-in `PushdownNegotiationRule` elides a
//! `Filter` node above a `TableScan` whose backing source claims to
//! fully handle the predicate via `SupportsFilterPushdown`.
//
// Rust guideline compliant

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{Expr, LogicalPlan};
use uni_plugin::traits::pushdown::{
    AggregateApplication, FilterApplication, ProjectionApplication, SortExpr as MarkerSortExpr,
    SupportsAggregatePushdown, SupportsFilterPushdown, SupportsLimitPushdown,
    SupportsProjectionPushdown, SupportsTopNPushdown, TopNApplication, TopNScope,
};
use uni_plugin_builtin::optimizer::{PushdownAwareTable, PushdownMarkers, PushdownNegotiationRule};

struct AlwaysFullPushdown;

impl SupportsFilterPushdown for AlwaysFullPushdown {
    fn push_filters(&self, filters: &[Expr]) -> FilterApplication {
        FilterApplication {
            fully_handled: (0..filters.len()).collect(),
            partially_handled: Vec::new(),
        }
    }
}

struct NeverPushdown;
impl SupportsFilterPushdown for NeverPushdown {
    fn push_filters(&self, _filters: &[Expr]) -> FilterApplication {
        FilterApplication::default()
    }
}

fn pushdown_aware_table(
    pushdown: Arc<dyn SupportsFilterPushdown + Send + Sync>,
) -> Arc<PushdownAwareTable> {
    Arc::new(PushdownAwareTable::with_filter(mem_provider(), pushdown))
}

fn mem_provider() -> Arc<dyn TableProvider> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("score", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3])),
            Arc::new(Int64Array::from(vec![10i64, 20, 30])),
        ],
    )
    .unwrap();
    let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    Arc::new(mem)
}

fn aware(markers: PushdownMarkers) -> Arc<PushdownAwareTable> {
    Arc::new(PushdownAwareTable {
        inner: mem_provider(),
        markers,
    })
}

async fn optimize(plan_sql: &str, table: Arc<PushdownAwareTable>) -> LogicalPlan {
    // Build a minimal state so default rules (e.g. `optimize_projections`,
    // `eliminate_limit`) don't race ours to elide nodes. We only want
    // to observe the effect of `PushdownNegotiationRule`.
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_optimizer_rules(vec![Arc::new(PushdownNegotiationRule)])
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_table("t", table).expect("register_table");
    let df = ctx.sql(plan_sql).await.expect("sql");
    df.into_optimized_plan().expect("optimize")
}

fn contains_filter(plan: &LogicalPlan) -> bool {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut found = false;
    let _ = plan.apply(|node| {
        if matches!(node, LogicalPlan::Filter(_)) {
            found = true;
        }
        Ok::<TreeNodeRecursion, datafusion::error::DataFusionError>(TreeNodeRecursion::Continue)
    });
    found
}

#[tokio::test(flavor = "multi_thread")]
async fn filter_elided_when_source_fully_handles() {
    let table = pushdown_aware_table(Arc::new(AlwaysFullPushdown));
    let plan = optimize("SELECT * FROM t WHERE id = 1", table).await;
    assert!(
        !contains_filter(&plan),
        "Filter must be elided when SupportsFilterPushdown returns Full; plan: {plan:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn filter_kept_when_source_declines() {
    let table = pushdown_aware_table(Arc::new(NeverPushdown));
    let plan = optimize("SELECT * FROM t WHERE id = 1", table).await;
    assert!(
        contains_filter(&plan),
        "Filter must be kept when SupportsFilterPushdown returns None; plan: {plan:?}"
    );
}

// ── Projection pushdown ─────────────────────────────────────────────

struct AlwaysProjection;
impl SupportsProjectionPushdown for AlwaysProjection {
    fn push_projection(&self, columns: &[String]) -> ProjectionApplication {
        ProjectionApplication {
            keep: columns.to_vec(),
        }
    }
}

struct NeverProjection;
impl SupportsProjectionPushdown for NeverProjection {
    fn push_projection(&self, _columns: &[String]) -> ProjectionApplication {
        ProjectionApplication { keep: Vec::new() }
    }
}

fn contains_projection(plan: &LogicalPlan) -> bool {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut found = false;
    let _ = plan.apply(|node| {
        if matches!(node, LogicalPlan::Projection(_)) {
            found = true;
        }
        Ok::<TreeNodeRecursion, datafusion::error::DataFusionError>(TreeNodeRecursion::Continue)
    });
    found
}

#[tokio::test(flavor = "multi_thread")]
async fn projection_elided_when_source_fully_handles() {
    let table = aware(PushdownMarkers {
        projection: Some(Arc::new(AlwaysProjection)),
        ..PushdownMarkers::default()
    });
    let plan = optimize("SELECT id, score FROM t", table).await;
    assert!(
        !contains_projection(&plan),
        "Projection must be elided when SupportsProjectionPushdown keeps all columns; plan: {plan:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn projection_kept_when_source_declines() {
    let table = aware(PushdownMarkers {
        projection: Some(Arc::new(NeverProjection)),
        ..PushdownMarkers::default()
    });
    let plan = optimize("SELECT id, score FROM t", table).await;
    assert!(
        contains_projection(&plan),
        "Projection must be kept when source does not handle it; plan: {plan:?}"
    );
}

// ── Limit pushdown ──────────────────────────────────────────────────

struct AlwaysLimit;
impl SupportsLimitPushdown for AlwaysLimit {
    fn push_limit(&self, limit: usize) -> Option<usize> {
        Some(limit)
    }
}

struct NeverLimit;
impl SupportsLimitPushdown for NeverLimit {
    fn push_limit(&self, _limit: usize) -> Option<usize> {
        None
    }
}

fn contains_limit(plan: &LogicalPlan) -> bool {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut found = false;
    let _ = plan.apply(|node| {
        if matches!(node, LogicalPlan::Limit(_)) {
            found = true;
        }
        Ok::<TreeNodeRecursion, datafusion::error::DataFusionError>(TreeNodeRecursion::Continue)
    });
    found
}

#[tokio::test(flavor = "multi_thread")]
async fn limit_elided_when_source_fully_handles() {
    let table = aware(PushdownMarkers {
        limit: Some(Arc::new(AlwaysLimit)),
        ..PushdownMarkers::default()
    });
    let plan = optimize("SELECT * FROM t LIMIT 2", table).await;
    assert!(
        !contains_limit(&plan),
        "Limit must be elided when SupportsLimitPushdown returns Some; plan: {plan:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn limit_kept_when_source_declines() {
    let table = aware(PushdownMarkers {
        limit: Some(Arc::new(NeverLimit)),
        ..PushdownMarkers::default()
    });
    let plan = optimize("SELECT * FROM t LIMIT 2", table).await;
    assert!(
        contains_limit(&plan),
        "Limit must be kept when source declines; plan: {plan:?}"
    );
}

// ── TopN pushdown ───────────────────────────────────────────────────

struct AlwaysTopN(TopNScope);
impl SupportsTopNPushdown for AlwaysTopN {
    fn push_topn(&self, _sort: &[MarkerSortExpr], _k: usize) -> Option<TopNApplication> {
        Some(TopNApplication { applied: self.0 })
    }
}

struct NeverTopN;
impl SupportsTopNPushdown for NeverTopN {
    fn push_topn(&self, _sort: &[MarkerSortExpr], _k: usize) -> Option<TopNApplication> {
        None
    }
}

fn contains_sort(plan: &LogicalPlan) -> bool {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut found = false;
    let _ = plan.apply(|node| {
        if matches!(node, LogicalPlan::Sort(_)) {
            found = true;
        }
        Ok::<TreeNodeRecursion, datafusion::error::DataFusionError>(TreeNodeRecursion::Continue)
    });
    found
}

#[tokio::test(flavor = "multi_thread")]
async fn topn_elided_when_source_handles_global() {
    let table = aware(PushdownMarkers {
        topn: Some(Arc::new(AlwaysTopN(TopNScope::Global))),
        ..PushdownMarkers::default()
    });
    let plan = optimize("SELECT * FROM t ORDER BY score ASC LIMIT 2", table).await;
    assert!(
        !contains_sort(&plan) && !contains_limit(&plan),
        "Sort+Limit must be elided for Global TopN pushdown; plan: {plan:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn topn_kept_when_source_declines() {
    let table = aware(PushdownMarkers {
        topn: Some(Arc::new(NeverTopN)),
        ..PushdownMarkers::default()
    });
    let plan = optimize("SELECT * FROM t ORDER BY score ASC LIMIT 2", table).await;
    assert!(
        contains_sort(&plan) || contains_limit(&plan),
        "Sort/Limit must be kept when source declines TopN; plan: {plan:?}"
    );
}

// ── Aggregate pushdown ──────────────────────────────────────────────

struct AlwaysAggregate;
impl SupportsAggregatePushdown for AlwaysAggregate {
    fn push_aggregates(&self, _group_by: &[Expr], aggs: &[Expr]) -> AggregateApplication {
        AggregateApplication {
            fully_handled: (0..aggs.len()).collect(),
            returns_partial_state: false,
        }
    }
}

struct NeverAggregate;
impl SupportsAggregatePushdown for NeverAggregate {
    fn push_aggregates(&self, _group_by: &[Expr], _aggs: &[Expr]) -> AggregateApplication {
        AggregateApplication::default()
    }
}

fn contains_aggregate(plan: &LogicalPlan) -> bool {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut found = false;
    let _ = plan.apply(|node| {
        if matches!(node, LogicalPlan::Aggregate(_)) {
            found = true;
        }
        Ok::<TreeNodeRecursion, datafusion::error::DataFusionError>(TreeNodeRecursion::Continue)
    });
    found
}

/// Build a pushdown-aware table whose inner provider already returns
/// exactly the aggregate output shape (`[count(Int64(1)): Int64]`).
///
/// This lets the elision guard (source schema must match the
/// aggregate's output schema) be satisfied so the positive test
/// actually exercises the rewrite branch.
fn aggregate_aware_table(
    marker: Arc<dyn SupportsAggregatePushdown + Send + Sync>,
) -> Arc<PushdownAwareTable> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "count(Int64(1))",
        DataType::Int64,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![3i64]))]).unwrap();
    let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    Arc::new(PushdownAwareTable {
        inner: Arc::new(mem),
        markers: PushdownMarkers {
            aggregate: Some(marker),
            ..PushdownMarkers::default()
        },
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn aggregate_elided_when_source_fully_handles() {
    let table = aggregate_aware_table(Arc::new(AlwaysAggregate));
    let plan = optimize("SELECT count(*) FROM t", table).await;
    assert!(
        !contains_aggregate(&plan),
        "Aggregate must be elided when source claims to handle it fully and schema matches; plan: {plan:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn aggregate_kept_when_source_declines() {
    let table = aggregate_aware_table(Arc::new(NeverAggregate));
    let plan = optimize("SELECT count(*) FROM t", table).await;
    assert!(
        contains_aggregate(&plan),
        "Aggregate must be kept when source declines; plan: {plan:?}"
    );
}
