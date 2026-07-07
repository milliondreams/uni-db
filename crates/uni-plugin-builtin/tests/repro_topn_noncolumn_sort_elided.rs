//! Repro for crates/uni-plugin-builtin/src/optimizer/pushdown_negotiation.rs:402
//!
//! `sort_exprs_to_marker` keeps only `Expr::Column` sort keys and silently
//! drops every non-column key (`a + b` is a `BinaryExpr`). So `ORDER BY a+b`
//! yields an EMPTY marker sort list. `try_rewrite_topn` then consults the
//! source's TopN marker with that empty spec; a `Global` answer causes the
//! rule to elide the ENTIRE `Sort` (and, in the Limit->Sort shape, the
//! `Limit` too) even though the true ordering key was never pushed down.
//!
//! Result: the ORDER BY (and LIMIT) are dropped from the plan -> wrong row
//! ordering / arbitrary top-k.
//!
//! This exercises the real public API: `PushdownAwareTable` +
//! `PushdownMarkers` + `PushdownNegotiationRule`, applied by DataFusion's
//! own `Optimizer` to a plan produced from real SQL.

use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::optimizer::{Optimizer, OptimizerContext};
use datafusion::prelude::SessionContext;
use uni_plugin::traits::pushdown::{SortExpr, SupportsTopNPushdown, TopNApplication, TopNScope};
use uni_plugin_builtin::optimizer::pushdown_negotiation::{
    PushdownAwareTable, PushdownMarkers, PushdownNegotiationRule,
};

// Rust guideline compliant

/// A TopN marker that always claims Global coverage regardless of the sort
/// keys it is handed (mirrors the bundled `AlwaysTopN(Global)` test impl).
#[derive(Debug)]
struct AlwaysTopN(TopNScope);

impl SupportsTopNPushdown for AlwaysTopN {
    fn push_topn(&self, _sort: &[SortExpr], _k: usize) -> Option<TopNApplication> {
        Some(TopNApplication { applied: self.0 })
    }
}

fn build_table() -> Arc<PushdownAwareTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    // Rows: (a,b) with a+b sums 10, 2, 8.
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![9, 0, 5])),
        ],
    )
    .unwrap();
    let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    Arc::new(PushdownAwareTable {
        inner: Arc::new(mem),
        markers: PushdownMarkers {
            topn: Some(Arc::new(AlwaysTopN(TopNScope::Global))),
            ..PushdownMarkers::default()
        },
    })
}

/// Apply ONLY the pushdown-negotiation rule to an unoptimized plan.
fn apply_rule(
    plan: datafusion::logical_expr::LogicalPlan,
) -> datafusion::logical_expr::LogicalPlan {
    let optimizer = Optimizer::with_rules(vec![Arc::new(PushdownNegotiationRule)]);
    let config = OptimizerContext::new();
    optimizer.optimize(plan, &config, |_, _| {}).unwrap()
}

#[tokio::test]
async fn topn_elides_sort_when_key_is_noncolumn_expr() {
    let ctx = SessionContext::new();
    ctx.register_table("t", build_table()).unwrap();

    // ORDER BY a + b -> a BinaryExpr sort key that sort_exprs_to_marker drops.
    let sql = "SELECT * FROM t ORDER BY a + b LIMIT 2";
    let df = ctx.sql(sql).await.unwrap();
    let unoptimized = df.logical_plan().clone();

    let before = unoptimized.display_indent().to_string();
    println!("--- BEFORE (unoptimized) ---\n{before}");
    assert!(
        before.contains("Sort:"),
        "sanity: unoptimized plan should contain a Sort node"
    );

    let optimized = apply_rule(unoptimized);
    let after = optimized.display_indent().to_string();
    println!("--- AFTER (pushdown rule applied) ---\n{after}");

    // FIXED: the Sort over the non-column key `a + b` must be KEPT, because
    // `sort_exprs_to_marker([a+b])` was EMPTY, so the source never actually
    // received the true ordering key. The elision is now guarded on the marker
    // faithfully covering every sort key.
    // Fix for pushdown_negotiation.rs:402 (consumed at lines 285-296 /
    // 313-330).
    assert!(
        after.contains("Sort:"),
        "the Sort over a non-column key must be preserved (not pushed down). \
         after=\n{after}"
    );
}

/// Partial-coverage variant: `ORDER BY a + b, a` -> marker sees only `[a]`,
/// Global still elides the FULL (a+b, a) sort.
#[tokio::test]
async fn topn_elides_sort_with_partial_key_coverage() {
    let ctx = SessionContext::new();
    ctx.register_table("t", build_table()).unwrap();

    let sql = "SELECT * FROM t ORDER BY a + b, a LIMIT 2";
    let df = ctx.sql(sql).await.unwrap();
    let unoptimized = df.logical_plan().clone();
    assert!(unoptimized.display_indent().to_string().contains("Sort:"));

    let optimized = apply_rule(unoptimized);
    let after = optimized.display_indent().to_string();
    println!("--- AFTER (partial coverage) ---\n{after}");

    // FIXED: only `a` survives sort_exprs_to_marker; because the composite
    // ordering (a+b, a) is not faithfully represented, the Sort must be KEPT.
    assert!(
        after.contains("Sort:"),
        "the composite Sort (a+b, a) must be preserved when not fully covered. \
         after=\n{after}"
    );
}
