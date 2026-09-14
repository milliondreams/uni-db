#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! M5h — acceptance test for the `StorageTableProvider` bridge.
//!
//! Registers a `memory://` plugin storage table with fixture data,
//! exposes it as a DataFusion [`TableProvider`] wrapped through
//! [`PushdownAwareTable::with_filter`] alongside the
//! [`StorageFilterPushdown`] marker, and verifies three things end-to-end:
//!
//! 1. A predicate over the table returns the expected rows.
//! 2. With an *encodable* predicate, `EXPLAIN` shows no `FilterExec`
//!    above the `StorageScanExec` — pushdown elision worked.
//! 3. With an *inexpressible* predicate (a UDF call the SQL unparser
//!    cannot render), `EXPLAIN` keeps the `FilterExec` above the scan.
//!
//! Notes on the surface under test
//! ------------------------------
//!
//! The plugin-framework plan calls this `MATCH (n:MemTable) WHERE n.x > 5`
//! against a virtual label. The MATCH-side wiring goes through
//! `CatalogVertexScanExec` (in `crates/uni-query/src/query/df_graph/`),
//! which already drives `CatalogTable::scan` (and through it the plugin
//! `Storage::read_batch`) for virtual labels — that path is exercised
//! by the existing `catalog_provider_dispatch.rs` test. The new
//! `StorageTableProvider` is the DataFusion-native bridge for the
//! same plugin `Storage` handle: SQL-level access plus the
//! `PushdownNegotiationRule` filter-elision plumbing. Both leg
//! through the same `Storage` trait, so the EXPLAIN-elision check
//! here corroborates the elision-correctness story end-to-end.
//
// Rust guideline compliant

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{Expr, LogicalPlanBuilder, col, lit};
use uni_plugin::traits::storage::Storage;
use uni_plugin_builtin::optimizer::{PushdownAwareTable, PushdownNegotiationRule};
use uni_plugin_builtin::storage::MemoryStorage;
use uni_plugin_builtin::storage_table_provider::{StorageFilterPushdown, StorageTableProvider};

/// Build a SessionContext whose only optimizer rule is
/// `PushdownNegotiationRule`, so the EXPLAIN test surfaces *only* the
/// effect of that rule. (The default rule set would also do
/// projection pruning, but we want to keep the assertion focused on
/// `FilterExec` elision via our pushdown path.)
fn pushdown_only_ctx() -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_optimizer_rules(vec![Arc::new(PushdownNegotiationRule)])
        .build();
    SessionContext::new_with_state(state)
}

fn fixture_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]))
}

async fn seed_storage() -> Arc<dyn Storage> {
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
    let schema = fixture_schema();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 5, 6, 7, 8]))],
    )
    .expect("fixture batch");
    storage
        .write_batch("mem_table", &batch)
        .await
        .expect("seed write");
    storage
}

async fn register_memtable(ctx: &SessionContext) {
    let storage = seed_storage().await;
    let provider = Arc::new(StorageTableProvider::new(
        storage,
        "mem_table".to_owned(),
        fixture_schema(),
    ));
    let wrapped = PushdownAwareTable::with_filter(provider, Arc::new(StorageFilterPushdown));
    ctx.register_table("mem_table", Arc::new(wrapped))
        .expect("register_table");
}

#[tokio::test(flavor = "multi_thread")]
async fn match_mem_table_returns_filtered_rows() {
    let ctx = pushdown_only_ctx();
    register_memtable(&ctx).await;

    // SQL stand-in for `MATCH (n:MemTable) WHERE n.x > 5 RETURN n.x` —
    // the test exercises the same Storage::read_batch path the MATCH
    // pipeline drives through CatalogVertexScanExec (see header doc).
    let df = ctx
        .sql("SELECT x FROM mem_table WHERE x > 5 ORDER BY x")
        .await
        .expect("sql");
    let batches = df.collect().await.expect("collect");
    let rendered = pretty_format_batches(&batches).expect("format").to_string();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, 3,
        "x > 5 must yield 3 rows (6, 7, 8); got {total}\nbatches:\n{rendered}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn explain_elides_filter_for_encodable_predicate() {
    let ctx = pushdown_only_ctx();
    register_memtable(&ctx).await;

    let df = ctx
        .sql("EXPLAIN SELECT x FROM mem_table WHERE x > 5")
        .await
        .expect("sql");
    let batches = df.collect().await.expect("collect");
    let rendered = pretty_format_batches(&batches).expect("format").to_string();

    // Not a proof that `StorageScanExec` ran — `TableScan` is the *logical*
    // node name, so this disjunction holds either way, and it matches by
    // substring besides. `a_storage_table_scan_runs_the_storage_scan_exec`
    // carries that claim against the physical plan by exact name (#177). What
    // is checked here is only that the scan survived into the EXPLAIN at all,
    // which is the precondition for the elision assertion below.
    assert!(
        rendered.contains("StorageScanExec") || rendered.contains("TableScan"),
        "EXPLAIN output should reference the scan; got:\n{rendered}"
    );
    // After PushdownNegotiationRule has run, a `Filter` node above the
    // TableScan must be elided because `StorageFilterPushdown` reports
    // the `x > 5` predicate as fully handled.
    assert!(
        !rendered.contains("Filter:"),
        "EXPLAIN must NOT contain a `Filter:` node above the scan when \
         the predicate is encodable; pushdown elision failed. Plan:\n{rendered}"
    );
}

/// Negative guard: when the WHERE predicate cannot be encoded by the
/// `StorageFilterPushdown` marker (here, a comparison against a
/// `ScalarValue::Binary` literal — the same shape the SQL unparser
/// declines via `not_impl_err` in the in-tree
/// `lance_predicate_pushdown_unsupported_returns_711` test), the
/// `PushdownNegotiationRule` MUST leave the `FilterExec` above the
/// scan so correctness holds. We build the plan via
/// `LogicalPlanBuilder` because no SQL surface naturally produces a
/// raw binary literal in a WHERE clause.
#[tokio::test(flavor = "multi_thread")]
async fn explain_keeps_filter_for_inexpressible_predicate() {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::logical_expr::LogicalPlan;

    let ctx = pushdown_only_ctx();
    register_memtable(&ctx).await;

    // Construct: SELECT x FROM mem_table WHERE x IS DISTINCT FROM 5
    // — `Operator::IsDistinctFrom` is one of the operators
    // `datafusion::sql::unparser::expr_to_sql` rejects with
    // `not_impl_err`, so the `StorageFilterPushdown` marker
    // classifies the predicate as unencodable.
    let scan = ctx.table("mem_table").await.expect("mem_table");
    let unencodable_predicate: Expr = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
        Box::new(col("x")),
        datafusion::logical_expr::Operator::IsDistinctFrom,
        Box::new(lit(5_i64)),
    ));

    // Build a Filter over the scan and run the optimizer manually so
    // we can inspect the post-rule logical plan without going through
    // a SQL path that the parser would reject.
    let plan = LogicalPlanBuilder::from(scan.into_optimized_plan().expect("optimize scan"))
        .filter(unencodable_predicate)
        .expect("filter")
        .build()
        .expect("build");

    let state = ctx.state();
    let optimized = state.optimize(&plan).expect("optimize");

    // Look through the tree for a Filter node — it must still be
    // present because the marker did NOT report the predicate as
    // fully handled.
    let mut has_filter = false;
    let _ = optimized.apply(|node| {
        if matches!(node, LogicalPlan::Filter(_)) {
            has_filter = true;
        }
        Ok::<TreeNodeRecursion, datafusion::error::DataFusionError>(TreeNodeRecursion::Continue)
    });
    assert!(
        has_filter,
        "Filter MUST stay above the scan when the predicate is inexpressible; \
         negative-guard regression. Optimized plan:\n{optimized:?}"
    );
}

/// Collect `ExecutionPlan::name()` for every node, depth-first.
fn physical_op_names(
    plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    out: &mut Vec<String>,
) {
    out.push(plan.name().to_string());
    for child in plan.children() {
        physical_op_names(child, out);
    }
}

/// A scan through `StorageTableProvider` runs `StorageScanExec` (#177).
///
/// Asserted on the **physical** plan by exact operator name. The `EXPLAIN`
/// assertion below cannot serve as this proof for two reasons #177 names
/// directly: it matches by substring, and it is a disjunction with
/// `"TableScan"` — the *logical* node name — so it passes whether or not the
/// physical operator was ever built.
#[tokio::test(flavor = "multi_thread")]
async fn a_storage_table_scan_runs_the_storage_scan_exec() {
    let ctx = pushdown_only_ctx();
    register_memtable(&ctx).await;
    let df = ctx
        .sql("SELECT x FROM mem_table WHERE x > 5")
        .await
        .expect("sql");
    let plan = df.create_physical_plan().await.expect("physical plan");
    let mut names = Vec::new();
    physical_op_names(&plan, &mut names);
    uni_query::query::executor::plan_shape::assert_uses(
        &names,
        "StorageScanExec",
        "SELECT x FROM mem_table WHERE x > 5",
    );
}

/// The negative twin: a plain in-memory table is served by DataFusion's own
/// scan, not this bridge.
#[tokio::test(flavor = "multi_thread")]
async fn a_datafusion_memtable_avoids_the_storage_scan_exec() {
    let ctx = pushdown_only_ctx();
    let batch = RecordBatch::try_new(
        fixture_schema(),
        vec![Arc::new(Int64Array::from(vec![6_i64, 7, 8]))],
    )
    .expect("fixture batch");
    let mem = datafusion::datasource::MemTable::try_new(fixture_schema(), vec![vec![batch]])
        .expect("memtable");
    ctx.register_table("plain_table", Arc::new(mem))
        .expect("register_table");

    let df = ctx
        .sql("SELECT x FROM plain_table WHERE x > 5")
        .await
        .expect("sql");
    let plan = df.create_physical_plan().await.expect("physical plan");
    let mut names = Vec::new();
    physical_op_names(&plan, &mut names);
    uni_query::query::executor::plan_shape::assert_avoids(
        &names,
        "StorageScanExec",
        "SELECT x FROM plain_table WHERE x > 5",
    );
}
