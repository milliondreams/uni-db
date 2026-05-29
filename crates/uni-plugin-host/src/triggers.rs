// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Host-side dispatch for `TriggerPlugin` registrations (M5f).
//!
//! Bridges `PluginRegistry::triggers()` into the transaction commit
//! path. The dispatcher builds a per-phase routing table once per
//! commit, drains mutation events from the transaction's private L0
//! buffer into a stable Arrow `RecordBatch`, applies subscription
//! selectors (event-kind mask + label / edge-type / property filter),
//! and invokes each matching trigger at the appropriate phase.
//!
//! Phase ordering inside a single commit:
//! 1. `BeforeMutation` then `BeforeCommit` — fired before the writer
//!    lock is taken. `Synchronous` reject aborts the transaction.
//! 2. WAL flush + L1 merge run.
//! 3. `AfterMutation` then `AfterCommit` — fired after publish. `Async`
//!    fire-mode triggers are spawned onto the tokio runtime so the
//!    writer's hot path stays untouched.
//!
//! Behavior contract:
//! - `predicate_source` is compiled at router build (per-commit) via
//!   `uni_cypher::parse_expression` → AST property-ref rewrite →
//!   `cypher_expr_to_df` → DataFusion `PhysicalExpr`, and evaluated
//!   against the per-row event batch in `filter_for`. Predicates may
//!   reference the event-row columns (`event_kind`, `vid_or_eid`,
//!   `label`, `property`, `old_value`, `new_value`) as well as
//!   per-entity properties: `n.foo` reads the new (post-mutation)
//!   property value, `old.foo` reads the pre-image. Referenced
//!   property keys are tracked in `RouteEntry::properties_referenced`
//!   so [`MutationEvents::from_l0_with_probe`] materializes exactly
//!   those keys into the per-row property bags — predicate-gated
//!   cost, no work for property-free predicates.
//! - `TriggerOutcome::Defer` enqueues the trigger fire into the
//!   per-`Uni` [`DeferralQueue`] (in-memory, ticked at 50ms by the
//!   background task spawned in `Uni::build`). Items re-fire on the
//!   next tick; re-deferring is capped at `DEFER_MAX_ATTEMPTS`.
//!   Restart-durable persistence lives with the M11 CDC scheduler.
//! - `NODE_CREATE` / `NODE_UPDATE` / `NODE_DELETE` (and the edge
//!   analogs) are distinguished via a committed-state probe
//!   ([`PreExistingProbe`]) passed to
//!   [`MutationEvents::from_l0_with_probe`]. The probe covers (a) the
//!   current L0 buffer + pending-flush L0s via
//!   [`PreExistingProbe::from_l0_chain`] (sync, no I/O) and (b) the
//!   L1 storage layer via [`PreExistingProbe::extend_with_l1`] (async,
//!   batched `_vid IN (…)` scan per label, chunked at 1024 VIDs).
//!   Callers that construct [`MutationEvents`] without a probe
//!   ([`MutationEvents::from_l0`]) fall back to emitting `NODE_UPDATE`
//!   / `EDGE_UPDATE` for every non-tombstoned write.
//! - `old_value` is populated from the L0-chain probe for vertices
//!   and edges visible there, and from the L1 probe (which now
//!   projects every property column on the candidate label) for
//!   vertices that were drained out of L0 by a previous flush. Edge
//!   pre-images are captured in the L0 chain via `edge_properties`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant as StdInstant};

use arrow_array::{BooleanArray, Int64Array, LargeBinaryArray, RecordBatch, UInt8Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::physical_plan::PhysicalExpr;
use tokio::runtime::Handle;
use tracing::warn;
use uni_common::cypher_value_codec;
use uni_common::{Properties, UniError, Value};
use uni_plugin::PluginRegistry;
use uni_plugin::traits::trigger::{
    FireMode, MutationBatch, TriggerContext, TriggerEventMask, TriggerOutcome, TriggerPhase,
    TriggerPlugin, TriggerSubscription,
};
use uni_store::runtime::L0Manager;
use uni_store::runtime::l0::L0Buffer;

/// Number of distinct `TriggerPhase` variants (`BeforeMutation`,
/// `AfterMutation`, `BeforeCommit`, `AfterCommit`).
const PHASE_COUNT: usize = 4;

/// Canonical Arrow schema for the per-row event batch handed to each
/// `TriggerPlugin::fire` call. Kept in one place so `filter_for` and
/// the `predicate_source` compiler agree on column names + types.
///
/// Also used by the CDC delivery path (M11 FU-4) so subscribers
/// receive events in the same shape triggers do.
pub fn event_row_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("event_kind", DataType::UInt8, false),
        Field::new("vid_or_eid", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("property", DataType::Utf8, false),
        Field::new("old_value", DataType::LargeBinary, true),
        Field::new("new_value", DataType::LargeBinary, true),
        // Per-row property bags carrying a CypherValue-encoded
        // `Value::Map` of the (selected) post-mutation and pre-image
        // property values. The Cypher predicate compiler rewrites
        // `n.foo` / `old.foo` references against these columns, which
        // the existing `index(map, key)` UDF handles via the
        // CypherValue codec — no bespoke map-access path required.
        Field::new("properties_new", DataType::LargeBinary, true),
        Field::new("properties_old", DataType::LargeBinary, true),
    ]))
}

/// Compile a Cypher boolean expression (`predicate_source`) into a
/// DataFusion `PhysicalExpr` that evaluates against [`event_row_schema`],
/// together with the set of node/edge property keys the predicate
/// references (used downstream to predicate-gate property-bag
/// materialization).
///
/// Pipeline: `uni_cypher::parse_expression` → in-place AST rewrite of
/// `n.foo` / `old.foo` into `properties_new.foo` / `properties_old.foo`
/// → `cypher_expr_to_df` (whose property-access translator emits
/// `index(col, "foo")` for non-graph-entity bases — the existing
/// `index` UDF then performs map lookup on the CypherValue-encoded
/// `LargeBinary` bag) → DataFusion `TypeCoercion` →
/// `create_physical_expr`. Same pattern as `apply_having_filter` in
/// `crates/uni-query/src/query/df_graph/locy_fixpoint.rs:2734-2810`,
/// just narrowed to a single expression against a fixed schema.
///
/// # Errors
///
/// Returns an error string if the predicate fails to parse, references
/// columns not present in the event-row schema (event-row columns or
/// `n.<prop>` / `old.<prop>` property references), or fails type
/// coercion.
fn compile_predicate(source: &str) -> Result<(Arc<dyn PhysicalExpr>, HashSet<String>), String> {
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::optimizer::AnalyzerRule;
    use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
    use datafusion::physical_expr::create_physical_expr;
    use datafusion::prelude::SessionContext;

    let mut cypher_expr =
        uni_cypher::parse_expression(source).map_err(|e| format!("parse: {e}"))?;
    let mut props_referenced: HashSet<String> = HashSet::new();
    rewrite_property_refs(&mut cypher_expr, &mut props_referenced);
    let df_expr_raw = uni_query::query::df_expr::cypher_expr_to_df(&cypher_expr, None)
        .map_err(|e| format!("translate: {e}"))?;

    let schema = event_row_schema();
    let df_schema = DFSchema::try_from(schema.as_ref().clone())
        .map_err(|e| format!("schema-conversion: {e}"))?;

    let ctx = SessionContext::new();
    // Register Cypher UDFs (`index`, `_cypher_gt`, ...) so (a) UDF
    // resolution below can swap placeholder `DummyUdf` nodes (which
    // declare `return_type = Null`) for the real impls (which declare
    // `LargeBinary` etc.), and (b) the resulting physical-expr can
    // invoke them at evaluation time.
    uni_query::query::df_udfs::register_cypher_udfs(&ctx)
        .map_err(|e| format!("udf-register: {e}"))?;
    let state = ctx.state();
    let config = state.config_options().clone();
    let props = state.execution_props();

    // Resolve UDFs first so the type-system sees the *real* return
    // types (e.g. `index` → LargeBinary) when the Cypher coercion pass
    // below decides whether `LargeBinary > Int64` needs to be rewritten
    // to `_cypher_gt`. Without this, `apply_type_coercion` sees Null and
    // routes through the bogus cast-to-Int64 path.
    let df_expr_resolved = resolve_dummy_udfs(df_expr_raw, &state)
        .map_err(|e| format!("resolve-udfs (pre-coerce): {e}"))?;

    // Apply Cypher-aware type coercion: rewrites `LargeBinary <op>
    // <native>` (e.g. `index(properties_new, "balance") > 100`) into
    // `_cypher_gt(left, right)` so the property-bag access path works
    // for native operands.
    let df_expr = uni_query::query::df_expr::apply_type_coercion(&df_expr_resolved, &df_schema)
        .map_err(|e| format!("cypher-coercion: {e}"))?;

    // Wrap in a Filter plan so TypeCoercion can align literals against
    // the event-row column types (e.g. `event_kind = 1` coerces `1`
    // from Int64 literal to UInt8 to match the column).
    let empty = datafusion::logical_expr::LogicalPlan::EmptyRelation(
        datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(df_schema.clone()),
        },
    );
    let filter_plan = LogicalPlanBuilder::from(empty)
        .filter(df_expr.clone())
        .map_err(|e| format!("filter-plan: {e}"))?
        .build()
        .map_err(|e| format!("plan-build: {e}"))?;
    let coerced_expr = match TypeCoercion::new().analyze(filter_plan, &config) {
        Ok(datafusion::logical_expr::LogicalPlan::Filter(f)) => f.predicate,
        _ => df_expr,
    };

    // Resolve placeholder `DummyUdf` scalar-function nodes (produced by
    // `cypher_expr_to_df` / `apply_type_coercion`) into the real UDF
    // impls registered on the SessionContext. Mirrors
    // `QueryExecutor::resolve_udfs` (`df_planner.rs:5168`) — without
    // this pass, `index` and `_cypher_gt` evaluation fails at runtime
    // with "UDF '<name>' is not registered".
    let resolved_expr =
        resolve_dummy_udfs(coerced_expr, &state).map_err(|e| format!("resolve-udfs: {e}"))?;

    let physical = create_physical_expr(&resolved_expr, &df_schema, props)
        .map_err(|e| format!("physical-expr: {e}"))?;
    Ok((physical, props_referenced))
}

/// Walk `expr` and replace every `ScalarFunction` whose name matches a
/// UDF registered on `state.scalar_functions()` with the registered
/// implementation. The Cypher translator (`cypher_expr_to_df`) emits
/// placeholder `DummyUdf` wrappers carrying only the name; the real
/// `IndexUdf` / `_cypher_gt` / ... impls live on the SessionContext.
fn resolve_dummy_udfs(
    expr: datafusion::logical_expr::Expr,
    state: &datafusion::execution::SessionState,
) -> Result<datafusion::logical_expr::Expr, String> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::logical_expr::Expr as DfExpr;

    let result = expr
        .transform_up(|node| {
            if let DfExpr::ScalarFunction(ref func) = node {
                let udf_name = func.func.name();
                if let Some(registered_udf) = state.scalar_functions().get(udf_name) {
                    return Ok(Transformed::yes(DfExpr::ScalarFunction(
                        datafusion::logical_expr::expr::ScalarFunction {
                            func: registered_udf.clone(),
                            args: func.args.clone(),
                        },
                    )));
                }
            }
            Ok(Transformed::no(node))
        })
        .map_err(|e| format!("udf-resolve walk: {e}"))?;
    Ok(result.data)
}

/// Walk a parsed Cypher expression and rewrite property references on
/// the canonical entity aliases (`n` for the post-mutation row,
/// `old` for the pre-image) so they resolve against the per-row
/// `properties_new` / `properties_old` columns of [`event_row_schema`].
///
/// `n.foo` → `properties_new.foo` (translates downstream to
/// `index(col("properties_new"), "foo")` via the standard
/// non-graph-entity property-access path in `cypher_expr_to_df`).
/// `old.foo` → `properties_old.foo`. All referenced property names
/// are collected into `referenced` for predicate-gated materialization
/// in [`MutationEvents::from_l0_with_probe`].
///
/// Other Cypher expressions are walked recursively so a predicate like
/// `n.balance > 100 AND old.status <> n.status` is fully rewritten.
fn rewrite_property_refs(expr: &mut uni_cypher::ast::Expr, referenced: &mut HashSet<String>) {
    use uni_cypher::ast::Expr;
    match expr {
        Expr::Property(base, prop) => {
            // First recurse into the base — supports chained access like
            // `n.address.city` (the inner `n.address` is rewritten to
            // `properties_new.address`, then `index(...)` chains).
            rewrite_property_refs(base, referenced);
            if let Expr::Variable(name) = base.as_ref() {
                match name.as_str() {
                    "n" => {
                        referenced.insert(prop.clone());
                        **base = Expr::Variable("properties_new".to_owned());
                    }
                    "old" => {
                        referenced.insert(prop.clone());
                        **base = Expr::Variable("properties_old".to_owned());
                    }
                    _ => {}
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            rewrite_property_refs(left, referenced);
            rewrite_property_refs(right, referenced);
        }
        Expr::UnaryOp { expr: inner, .. } => rewrite_property_refs(inner, referenced),
        Expr::FunctionCall { args, .. } => {
            for a in args {
                rewrite_property_refs(a, referenced);
            }
        }
        Expr::Case {
            expr: case_expr,
            when_then,
            else_expr,
        } => {
            if let Some(e) = case_expr.as_deref_mut() {
                rewrite_property_refs(e, referenced);
            }
            for (w, t) in when_then {
                rewrite_property_refs(w, referenced);
                rewrite_property_refs(t, referenced);
            }
            if let Some(e) = else_expr.as_deref_mut() {
                rewrite_property_refs(e, referenced);
            }
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) | Expr::IsUnique(inner) => {
            rewrite_property_refs(inner, referenced);
        }
        Expr::In { expr: e, list } => {
            rewrite_property_refs(e, referenced);
            rewrite_property_refs(list, referenced);
        }
        Expr::List(items) => {
            for i in items {
                rewrite_property_refs(i, referenced);
            }
        }
        Expr::Map(pairs) => {
            for (_, v) in pairs {
                rewrite_property_refs(v, referenced);
            }
        }
        Expr::ArrayIndex { array, index } => {
            rewrite_property_refs(array, referenced);
            rewrite_property_refs(index, referenced);
        }
        Expr::ArraySlice { array, start, end } => {
            rewrite_property_refs(array, referenced);
            if let Some(s) = start.as_deref_mut() {
                rewrite_property_refs(s, referenced);
            }
            if let Some(e) = end.as_deref_mut() {
                rewrite_property_refs(e, referenced);
            }
        }
        // Literal / Parameter / Variable / Wildcard / subquery variants
        // do not carry rewritable property refs at the surface level.
        _ => {}
    }
}

fn phase_index(p: TriggerPhase) -> usize {
    // `TriggerPhase` is `#[non_exhaustive]` — fall back to BeforeMutation
    // bucket so a future variant can't silently slot into an existing
    // route's phase by accident.
    match p {
        TriggerPhase::BeforeMutation => 0,
        TriggerPhase::AfterMutation => 1,
        TriggerPhase::BeforeCommit => 2,
        TriggerPhase::AfterCommit => 3,
        _ => 0,
    }
}

/// A single route in the per-phase dispatch table.
struct RouteEntry {
    plugin: Arc<dyn TriggerPlugin>,
    name: String,
    event_mask: u32,
    label_filter: Option<Vec<String>>,
    edge_type_filter: Option<Vec<String>>,
    property_filter: Option<Vec<String>>,
    fire_mode: FireMode,
    /// Compiled `predicate_source` expression, evaluated per-row in
    /// `filter_for` to drop rows where the predicate is false. `None`
    /// when the subscription has no predicate. The compile is done
    /// once per [`TriggerRouter::from_registry`] call.
    compiled_predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Property names that the compiled predicate references via
    /// `n.<prop>` or `old.<prop>`. Used to predicate-gate the
    /// property-bag materialization in
    /// [`MutationEvents::from_l0_with_probe`] — when this set is
    /// empty the event-row pipeline does no per-property work for
    /// this route.
    properties_referenced: HashSet<String>,
}

impl RouteEntry {
    fn matches(&self, kind: TriggerEventMask, label_or_type: &str) -> bool {
        if (self.event_mask & kind.0) == 0 {
            return false;
        }
        if let Some(ref labels) = self.label_filter
            && kind_is_node(kind)
            && !labels.iter().any(|l| l.as_str() == label_or_type)
        {
            return false;
        }
        if let Some(ref ets) = self.edge_type_filter
            && kind_is_edge(kind)
            && !ets.iter().any(|e| e.as_str() == label_or_type)
        {
            return false;
        }
        true
    }
}

fn kind_is_node(kind: TriggerEventMask) -> bool {
    let mask = TriggerEventMask::NODE_CREATE
        .union(TriggerEventMask::NODE_UPDATE)
        .union(TriggerEventMask::NODE_DELETE)
        .union(TriggerEventMask::LABEL_ADDED)
        .union(TriggerEventMask::LABEL_REMOVED);
    (kind.0 & mask.0) != 0
}

fn kind_is_edge(kind: TriggerEventMask) -> bool {
    let mask = TriggerEventMask::EDGE_CREATE
        .union(TriggerEventMask::EDGE_UPDATE)
        .union(TriggerEventMask::EDGE_DELETE);
    (kind.0 & mask.0) != 0
}

/// Per-commit trigger dispatcher.
pub struct TriggerRouter {
    by_phase: [Vec<RouteEntry>; PHASE_COUNT],
    /// Per-`Uni` deferral queue. `None` for read-only / test setups
    /// without a queue — `TriggerOutcome::Defer` then falls back to
    /// the legacy warn-and-collapse behavior.
    defer_queue: Option<Arc<DeferralQueue>>,
}

impl TriggerRouter {
    /// Snapshot the registered triggers into a routing table.
    ///
    /// Cheap for predicate-less subscriptions (one `Arc` clone for the
    /// trigger vector, then one pass to bucket by phase). For
    /// subscriptions carrying `predicate_source`, compiles the Cypher
    /// predicate into a DataFusion `PhysicalExpr` once and stashes it
    /// on the route — sub-millisecond per predicate, amortized against
    /// commit overhead.
    ///
    /// # Errors
    ///
    /// Returns [`UniError::TriggerRejected`] (with a descriptive
    /// `reason`) if any subscription's `predicate_source` fails to
    /// parse, references unknown columns, or fails type coercion. The
    /// error surfaces at commit time, not at registration — this is a
    /// deliberate trade-off to keep `uni-plugin` free of a `uni-cypher`
    /// dependency.
    pub fn from_registry(reg: &PluginRegistry) -> Result<Self, UniError> {
        Self::from_registry_with_queue(reg, None)
    }

    /// Variant that wires in a per-`Uni` deferral queue so
    /// `TriggerOutcome::Defer` enqueues for re-firing instead of
    /// being warned and dropped.
    ///
    /// # Errors
    ///
    /// Same as [`Self::from_registry`].
    pub fn from_registry_with_queue(
        reg: &PluginRegistry,
        defer_queue: Option<Arc<DeferralQueue>>,
    ) -> Result<Self, UniError> {
        let triggers = reg.triggers();
        let mut by_phase: [Vec<RouteEntry>; PHASE_COUNT] =
            [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
        for plugin in triggers.iter() {
            let sub: &TriggerSubscription = plugin.subscription();
            let name = subscription_name(sub);
            let (compiled_predicate, properties_referenced) = match sub.predicate_source.as_deref()
            {
                Some(src) => {
                    let (expr, refs) =
                        compile_predicate(src).map_err(|e| UniError::TriggerRejected {
                            trigger: name.clone(),
                            reason: format!(
                                "predicate_source compile failed: {e}. \
                                 Supported references: event-row columns \
                                 (event_kind, vid_or_eid, label, property, \
                                 old_value, new_value) and entity property \
                                 references `n.<prop>` (post-mutation) / \
                                 `old.<prop>` (pre-image)."
                            ),
                        })?;
                    (Some(expr), refs)
                }
                None => (None, HashSet::new()),
            };
            let entry = RouteEntry {
                plugin: Arc::clone(plugin),
                name,
                event_mask: sub.events.0,
                label_filter: sub
                    .labels
                    .as_ref()
                    .map(|v| v.iter().map(|s| s.to_string()).collect()),
                edge_type_filter: sub
                    .edge_types
                    .as_ref()
                    .map(|v| v.iter().map(|s| s.to_string()).collect()),
                property_filter: sub
                    .properties
                    .as_ref()
                    .map(|v| v.iter().map(|s| s.to_string()).collect()),
                fire_mode: sub.fire_mode,
                compiled_predicate,
                properties_referenced,
            };
            by_phase[phase_index(sub.phase)].push(entry);
        }
        Ok(Self {
            by_phase,
            defer_queue,
        })
    }

    /// True if no triggers are registered at any phase.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_phase.iter().all(|v| v.is_empty())
    }

    /// Union of node/edge property names that any compiled trigger
    /// predicate references (across all phases). Empty when no
    /// trigger has a `predicate_source` mentioning `n.<prop>` /
    /// `old.<prop>`. Drives predicate-gated property-bag
    /// materialization in [`MutationEvents::from_l0_with_probe`].
    #[must_use]
    pub fn properties_referenced(&self) -> HashSet<String> {
        let mut out: HashSet<String> = HashSet::new();
        for routes in &self.by_phase {
            for entry in routes {
                for p in &entry.properties_referenced {
                    out.insert(p.clone());
                }
            }
        }
        out
    }

    /// Fire `BeforeMutation` then `BeforeCommit` phases in order.
    ///
    /// Returns `Err(UniError::TriggerRejected)` if a `Synchronous`
    /// trigger returns `TriggerOutcome::Reject` or `Err`. `Async` /
    /// `EventualConsistency` triggers are ignored at before-phases
    /// (they ride on after-phases only — firing async work pre-commit
    /// would let it observe a transaction that subsequently aborts).
    ///
    /// # Errors
    ///
    /// `UniError::TriggerRejected` on reject or fire error.
    pub fn dispatch_before(
        &self,
        ctx: TriggerContext<'_>,
        events: &MutationEvents,
    ) -> Result<(), UniError> {
        for &phase in &[TriggerPhase::BeforeMutation, TriggerPhase::BeforeCommit] {
            let routes = &self.by_phase[phase_index(phase)];
            for entry in routes {
                if !matches!(entry.fire_mode, FireMode::Synchronous) {
                    continue;
                }
                let Some(batch) = events.filter_for(entry) else {
                    continue;
                };
                let mb = MutationBatch {
                    events: Arc::new(batch),
                };
                let ctx_ref = TriggerContext::new(ctx.session_id, ctx.tx_id);
                match entry.plugin.fire(ctx_ref, &mb) {
                    Ok(TriggerOutcome::Continue) => {}
                    Ok(TriggerOutcome::Reject { reason }) => {
                        return Err(UniError::TriggerRejected {
                            trigger: entry.name.to_string(),
                            reason,
                        });
                    }
                    Ok(TriggerOutcome::Defer { until }) => {
                        // Memory-backed in-process deferral. FU-5 adds
                        // an optional `delay` to `TriggerDeferral`;
                        // `None` re-fires on the next queue tick,
                        // `Some(d)` schedules at `now + d`.
                        enqueue_deferral(
                            &self.defer_queue,
                            Arc::clone(&entry.plugin),
                            entry.name.clone(),
                            mb.clone(),
                            ctx.session_id.to_owned(),
                            ctx.tx_id,
                            until,
                        );
                    }
                    Ok(_) => {
                        // `TriggerOutcome` is `#[non_exhaustive]`; an
                        // unrecognised future variant is conservatively
                        // treated as Continue.
                    }
                    Err(e) => {
                        return Err(UniError::TriggerRejected {
                            trigger: entry.name.to_string(),
                            reason: e.to_string(),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// Fire `AfterMutation` then `AfterCommit` phases. Cannot abort.
    ///
    /// `Synchronous` after-phase triggers run inline (panics caught and
    /// logged). `Async` triggers are spawned on `runtime`.
    /// `EventualConsistency` triggers are spawned the same as `Async`
    /// in v1 (a real batched queue lands with M5g).
    pub fn dispatch_after(
        &self,
        ctx: TriggerContext<'_>,
        events: &MutationEvents,
        runtime: &Handle,
    ) {
        for &phase in &[TriggerPhase::AfterMutation, TriggerPhase::AfterCommit] {
            let routes = &self.by_phase[phase_index(phase)];
            for entry in routes {
                let Some(batch) = events.filter_for(entry) else {
                    continue;
                };
                let mb = MutationBatch {
                    events: Arc::new(batch),
                };
                match entry.fire_mode {
                    FireMode::Synchronous => {
                        fire_caught(entry, ctx.session_id, ctx.tx_id, &mb, &self.defer_queue);
                    }
                    // `FireMode::Async`, `EventualConsistency`, and any
                    // future variant land on the spawn path. v1 collapses
                    // EventualConsistency onto Async (no batched queue);
                    // M5g adds the real queue.
                    _ => {
                        let plugin = Arc::clone(&entry.plugin);
                        let name = entry.name.clone();
                        let session_id = ctx.session_id.to_owned();
                        let tx_id = ctx.tx_id;
                        let queue = self.defer_queue.clone();
                        runtime.spawn(async move {
                            let mb_inner = mb;
                            let result =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    plugin.fire(TriggerContext::new(&session_id, tx_id), &mb_inner)
                                }));
                            match result {
                                Ok(Ok(TriggerOutcome::Defer { until })) => {
                                    enqueue_deferral(
                                        &queue,
                                        Arc::clone(&plugin),
                                        name.clone(),
                                        mb_inner,
                                        session_id.clone(),
                                        tx_id,
                                        until,
                                    );
                                }
                                Ok(Ok(_)) => {}
                                Ok(Err(e)) => {
                                    warn!(trigger = %name, error = %e, "async trigger errored");
                                }
                                Err(_) => {
                                    warn!(trigger = %name, "async trigger panicked");
                                }
                            }
                        });
                    }
                }
            }
        }
    }
}

/// Enqueue a [`TriggerOutcome::Defer`] result into the host's
/// in-memory [`DeferralQueue`]. When no queue is wired (read-only or
/// test setups) the item is dropped with a warn — matches the legacy
/// fallback behavior.
///
/// The fire instant honors `until.delay` (FU-5); `None` collapses to
/// "now" so the item fires on the next tick.
fn enqueue_deferral(
    queue: &Option<Arc<DeferralQueue>>,
    plugin: Arc<dyn TriggerPlugin>,
    name: String,
    mb: MutationBatch,
    session_id: String,
    tx_id: u64,
    until: uni_plugin::traits::trigger::TriggerDeferral,
) {
    let Some(queue) = queue else {
        warn!(trigger = %name, "Defer with no queue wired; dropping");
        return;
    };
    let fire_at = StdInstant::now() + until.delay.unwrap_or(Duration::ZERO);
    queue.push(
        DeferredItem {
            plugin,
            name,
            batch: mb,
            session_id,
            tx_id,
            attempts: 0,
            payload: until.payload,
        },
        fire_at,
    );
}

fn fire_caught(
    entry: &RouteEntry,
    session_id: &str,
    tx_id: u64,
    mb: &MutationBatch,
    defer_queue: &Option<Arc<DeferralQueue>>,
) {
    let plugin = Arc::clone(&entry.plugin);
    let name = entry.name.clone();
    let mb_clone = mb.clone();
    let session_id_owned = session_id.to_owned();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        plugin.fire(TriggerContext::new(&session_id_owned, tx_id), &mb_clone)
    }));
    match result {
        Ok(Ok(TriggerOutcome::Defer { until })) => {
            enqueue_deferral(
                defer_queue,
                plugin,
                name,
                mb_clone,
                session_id_owned,
                tx_id,
                until,
            );
        }
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            warn!(trigger = %name, error = %e, "after-phase trigger errored");
        }
        Err(_) => {
            warn!(trigger = %name, "after-phase trigger panicked");
        }
    }
}

fn subscription_name(sub: &TriggerSubscription) -> String {
    // `TriggerSubscription` carries no explicit name field; use the
    // first line of the docs as a stable identifier, falling back to
    // a generic label. Keeps `UniError::TriggerRejected` human-readable
    // without an ABI bump for a name field on the subscription struct.
    sub.docs
        .lines()
        .next()
        .map(str::to_owned)
        .unwrap_or_else(|| "<unnamed trigger>".to_owned())
}

// ── Mutation event extraction ──────────────────────────────────────

/// In-memory, untyped event log drained from `tx_l0`. Held by value
/// across the commit boundary and filtered per-route on dispatch.
pub struct MutationEvents {
    rows: Vec<MutationRow>,
}

struct MutationRow {
    event_kind: TriggerEventMask,
    vid_or_eid: i64,
    /// For NODE_* events: the affected label (one row per label).
    /// For EDGE_* events: the edge type.
    label_or_type: String,
    /// Pre-image properties when known (probe was supplied and the
    /// vertex/edge existed before this tx); `None` otherwise. The
    /// `BeforeCommit` dispatch path serializes this into the
    /// `old_value` Arrow column.
    old_value: Option<Vec<u8>>,
    /// Post-mutation property map filtered to the predicate-referenced
    /// keys; serialized into the `properties_new` LargeBinary column.
    /// `None` when no trigger references any property.
    new_properties: Option<Properties>,
    /// Pre-image property map filtered to the predicate-referenced
    /// keys; serialized into the `properties_old` LargeBinary column.
    /// `None` when no trigger references any property or the entity
    /// did not pre-exist.
    old_properties: Option<Properties>,
}

/// Snapshot of the committed graph state used to (a) distinguish
/// CREATE from UPDATE in [`MutationEvents::from_l0_with_probe`] and
/// (b) populate `old_value` for vertex and edge mutation events.
///
/// Built once per commit. The cheap [`Self::from_l0_chain`] scans the
/// writer's `L0Manager` (current L0 + pending-flush L0s) — no I/O,
/// runs before the writer write lock is acquired. The richer
/// [`Self::extend_with_l1`] adds an async L1 storage probe for VIDs
/// not found in the L0 chain — closes the gap where a vertex flushed
/// to L1 in a previous commit would otherwise be misclassified as
/// `NODE_CREATE` on its next mutation. The L1 probe also projects
/// every property column on the target label so the resulting
/// `old_value` carries the same pre-image fidelity as the L0-chain
/// path. Edge pre-images are captured via the L0 chain's
/// `edge_properties` snapshot.
#[derive(Default)]
pub struct PreExistingProbe {
    /// VIDs known to exist in committed state (with their pre-image
    /// properties, when captured — populated by L0 probe; empty
    /// `Properties` map when added by L1 existence probe).
    vertices: HashMap<uni_common::Vid, Properties>,
    /// EIDs known to exist in committed state (with their pre-image
    /// properties, when captured — populated by L0 probe). The map
    /// uses `Properties::default()` for entries added through an
    /// existence-only path.
    edges: HashMap<uni_common::Eid, Properties>,
}

impl PreExistingProbe {
    /// Build a probe by scanning the current L0 + pending-flush L0s
    /// for vertices/edges referenced in `tx_l0`. Properties are cloned
    /// from the committed L0 chain.
    ///
    /// Only mutations actually present in `tx_l0` are probed — keeping
    /// the work proportional to the commit's mutation count rather
    /// than to the total graph size.
    #[must_use]
    pub fn from_l0_chain(l0_manager: &L0Manager, tx_l0: &L0Buffer) -> Self {
        let mut vertices: HashMap<uni_common::Vid, Properties> = HashMap::new();
        let mut edges: HashMap<uni_common::Eid, Properties> = HashMap::new();

        let candidate_vids: Vec<uni_common::Vid> = tx_l0
            .vertex_properties
            .keys()
            .copied()
            .chain(tx_l0.vertex_tombstones.iter().copied())
            .collect();
        let candidate_eids: Vec<uni_common::Eid> = tx_l0
            .edge_endpoints
            .keys()
            .copied()
            .chain(tx_l0.tombstones.keys().copied())
            .collect();

        let mut probe_buffer = |buf: &L0Buffer| {
            for vid in &candidate_vids {
                if vertices.contains_key(vid) {
                    continue;
                }
                if buf.vertex_tombstones.contains(vid) {
                    continue;
                }
                if let Some(props) = buf.vertex_properties.get(vid) {
                    vertices.insert(*vid, props.clone());
                }
            }
            for eid in &candidate_eids {
                if edges.contains_key(eid) {
                    continue;
                }
                if buf.tombstones.contains_key(eid) {
                    continue;
                }
                if buf.edge_endpoints.contains_key(eid) {
                    let props = buf.edge_properties.get(eid).cloned().unwrap_or_default();
                    edges.insert(*eid, props);
                }
            }
        };

        {
            let current = l0_manager.get_current();
            let g = current.read();
            probe_buffer(&g);
        }
        for pending in l0_manager.get_pending_flush() {
            let g = pending.read();
            probe_buffer(&g);
        }

        Self { vertices, edges }
    }

    /// Snapshot the (vid, label) pairs that should be probed against
    /// L1 storage — VIDs in `tx_l0` not already marked pre-existing
    /// by the L0 chain. Sync — must run under the `tx_l0` read lock.
    /// Returned vector is sized by chunked-IN-list quota, ready to
    /// hand to [`Self::extend_with_l1`] outside the lock.
    #[must_use]
    pub fn pending_l1_candidates(&self, tx_l0: &L0Buffer) -> Vec<(uni_common::Vid, String)> {
        let mut out: Vec<(uni_common::Vid, String)> = Vec::new();
        for vid in tx_l0
            .vertex_properties
            .keys()
            .chain(tx_l0.vertex_tombstones.iter())
        {
            if self.vertices.contains_key(vid) {
                continue;
            }
            let label = tx_l0
                .vertex_labels
                .get(vid)
                .and_then(|labels| labels.first())
                .cloned();
            if let Some(label) = label {
                out.push((*vid, label));
            }
        }
        out
    }

    /// Extend an existing probe with an L1 storage scan for the
    /// supplied `(vid, label)` candidates (typically the output of
    /// [`Self::pending_l1_candidates`]). Async — runs outside the
    /// tx_l0 read lock.
    ///
    /// Groups candidates by label, chunks each group into 1024-VID
    /// batches, and issues one `scan_vertex_table` per chunk with a
    /// `_vid IN (…)` filter — bounded I/O proportional to the
    /// commit's mutation count, not the graph size. For every
    /// returned VID, every non-vid column is converted via
    /// [`uni_store::storage::arrow_convert::arrow_to_value`] and
    /// stashed as the pre-image `Properties` map; this populates the
    /// `old_value` column on `NODE_UPDATE` / `NODE_DELETE` events
    /// emitted by [`MutationEvents::from_l0_with_probe`] for vertices
    /// that were only visible after the last L0 flush.
    ///
    /// # Errors
    ///
    /// Per-chunk scan errors are logged and ignored — the L0 probe
    /// already captured the high-fidelity subset, so a failed L1
    /// probe degrades to "L1 vertices are misclassified as CREATE"
    /// rather than failing the commit.
    pub async fn extend_with_l1(
        &mut self,
        candidates: Vec<(uni_common::Vid, String)>,
        storage: &uni_store::storage::manager::StorageManager,
    ) {
        use arrow_array::Array;
        use std::collections::HashMap as StdHashMap;
        use uni_store::storage::arrow_convert::arrow_to_value;
        const CHUNK_SIZE: usize = 1024;

        let mut by_label: StdHashMap<String, Vec<uni_common::Vid>> = StdHashMap::new();
        for (vid, label) in candidates {
            by_label.entry(label).or_default().push(vid);
        }

        for (label, vids) in by_label {
            // Discover the table's full column set once per label so
            // we can request every property (not just `_vid`).
            let table_name = uni_store::backend::table_names::vertex_table_name(&label);
            let column_names: Vec<String> =
                match storage.backend().get_table_schema(&table_name).await {
                    Ok(Some(schema)) => schema.fields().iter().map(|f| f.name().clone()).collect(),
                    Ok(None) => {
                        // Table absent: nothing to probe.
                        continue;
                    }
                    Err(e) => {
                        warn!(label = %label, error = %e, "L1 pre-image probe: \
                          schema lookup failed; vids fall back to CREATE");
                        continue;
                    }
                };
            // Always include `_vid`; the column-filter inside
            // `scan_vertex_table` is permissive about missing columns,
            // so passing every name from the schema is safe.
            let col_refs: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();

            for chunk in vids.chunks(CHUNK_SIZE) {
                let in_list = chunk
                    .iter()
                    .map(|v| v.as_u64().to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                let filter = format!("_vid IN ({in_list})");
                let batch = match storage
                    .scan_vertex_table(&label, &col_refs, Some(&filter))
                    .await
                {
                    Ok(Some(b)) => b,
                    Ok(None) => continue,
                    Err(e) => {
                        warn!(label = %label, error = %e, "L1 pre-image probe failed; \
                              affected vids fall back to NODE_CREATE classification");
                        continue;
                    }
                };
                let vid_col = match batch
                    .column_by_name("_vid")
                    .and_then(|c| c.as_any().downcast_ref::<arrow_array::UInt64Array>())
                {
                    Some(c) => c,
                    None => {
                        warn!(label = %label, "L1 probe returned batch without _vid column");
                        continue;
                    }
                };
                // Cache (column_index, column_name) pairs for the
                // per-row property assembly. Skip storage-internal
                // columns (`_vid`, `_version`, `_label`) — user
                // properties are everything else.
                let schema = batch.schema();
                let property_cols: Vec<(usize, String)> = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, f)| {
                        let name = f.name();
                        if name == "_vid"
                            || name == "_version"
                            || name == "_label"
                            || name == "_labels"
                        {
                            None
                        } else {
                            Some((idx, name.clone()))
                        }
                    })
                    .collect();

                for row in 0..vid_col.len() {
                    if vid_col.is_null(row) {
                        continue;
                    }
                    let raw = vid_col.value(row);
                    let vid = uni_common::Vid::from(raw);
                    let mut props = Properties::new();
                    for (col_idx, col_name) in &property_cols {
                        let col = batch.column(*col_idx);
                        let value = arrow_to_value(col.as_ref(), row, None);
                        if !matches!(value, uni_common::Value::Null) {
                            props.insert(col_name.clone(), value);
                        }
                    }
                    // First insert wins: L0-chain entries always come
                    // first and may already hold richer pre-image data.
                    self.vertices.entry(vid).or_insert(props);
                }
            }
        }
    }

    /// True if `vid` was visible in committed state before this tx.
    #[must_use]
    pub fn vertex_pre_existed(&self, vid: uni_common::Vid) -> bool {
        self.vertices.contains_key(&vid)
    }

    /// True if `eid` was visible in committed state before this tx.
    #[must_use]
    pub fn edge_pre_existed(&self, eid: uni_common::Eid) -> bool {
        self.edges.contains_key(&eid)
    }

    fn edge_old_bytes(&self, eid: uni_common::Eid) -> Option<Vec<u8>> {
        self.edges.get(&eid).map(serialize_properties)
    }

    fn vertex_old_bytes(&self, vid: uni_common::Vid) -> Option<Vec<u8>> {
        self.vertices.get(&vid).map(serialize_properties)
    }

    /// Borrow the captured pre-image properties for `vid`, when the
    /// vertex pre-existed in committed state. Used by
    /// [`MutationEvents::from_l0_with_probe`] to populate the
    /// `properties_old` event-row column with the subset of keys any
    /// trigger predicate references.
    #[must_use]
    pub fn vertex_properties(&self, vid: uni_common::Vid) -> Option<&Properties> {
        self.vertices.get(&vid)
    }

    /// Borrow the captured pre-image properties for `eid`, when the
    /// edge pre-existed in committed state.
    #[must_use]
    pub fn edge_properties(&self, eid: uni_common::Eid) -> Option<&Properties> {
        self.edges.get(&eid)
    }
}

/// Serialize a `Properties` map into a stable byte representation for
/// the trigger event row's `old_value` column. Uses JSON for now —
/// matches the codec other plugin surfaces use for `CypherValue`
/// payloads and keeps the bytes inspectable in trigger plugins
/// without pulling a bespoke decoder.
fn serialize_properties(props: &Properties) -> Vec<u8> {
    serde_json::to_vec(props).unwrap_or_default()
}

impl MutationEvents {
    /// Drain the tx-private L0 buffer into a typed event log without a
    /// committed-state probe. Every non-tombstoned write emits an
    /// `UPDATE` event; `old_value` is `None`. Equivalent to
    /// [`Self::from_l0_with_probe`] with `probe = None`.
    #[must_use]
    pub fn from_l0(l0: &L0Buffer) -> Self {
        Self::from_l0_with_probe(l0, None, &HashSet::new())
    }

    /// Drain the tx-private L0 buffer into a typed event log.
    ///
    /// When `probe` is supplied, the probe distinguishes CREATE from
    /// UPDATE per-VID/EID and supplies the pre-image bytes used to
    /// populate `old_value` for `BeforeCommit` triggers. When `probe`
    /// is `None`, every write emits `UPDATE` and `old_value` stays
    /// `None` (legacy behavior — kept for callers that don't yet
    /// build a probe).
    ///
    /// Multi-label vertices emit one row per label so a label-filtered
    /// trigger fires exactly once per (vid, matching-label) pair.
    /// Vertices with no labels emit a single row with an empty label
    /// so unfiltered triggers still observe them.
    #[must_use]
    pub fn from_l0_with_probe(
        l0: &L0Buffer,
        probe: Option<&PreExistingProbe>,
        properties_referenced: &HashSet<String>,
    ) -> Self {
        let mut rows: Vec<MutationRow> = Vec::with_capacity(l0.mutation_count);
        let track_props = !properties_referenced.is_empty();

        // Extract the subset of `props` whose keys appear in
        // `properties_referenced`. Returns `None` when nothing is
        // tracked or no referenced key is present, keeping the column
        // null for property-free triggers.
        let filtered = |props: &Properties| -> Option<Properties> {
            if !track_props {
                return None;
            }
            let mut out: Properties = Properties::new();
            for k in properties_referenced {
                if let Some(v) = props.get(k) {
                    out.insert(k.clone(), v.clone());
                }
            }
            // Always emit the (possibly empty) bag when properties are
            // tracked so the predicate sees a Map rather than NULL
            // (which would short-circuit `index` to NULL and risk
            // type-coercion surprises in `>` / `<>` comparisons).
            Some(out)
        };

        // Vertex writes — CREATE if the probe says the vid didn't
        // pre-exist, UPDATE otherwise. Legacy callers with no probe
        // get UPDATE for every write.
        for (vid, props) in &l0.vertex_properties {
            if l0.vertex_tombstones.contains(vid) {
                continue;
            }
            let id = vid_to_i64(*vid);
            let labels = l0.vertex_labels.get(vid);
            let (kind, old, old_props_map) = match probe {
                Some(p) if p.vertex_pre_existed(*vid) => (
                    TriggerEventMask::NODE_UPDATE,
                    p.vertex_old_bytes(*vid),
                    p.vertex_properties(*vid).and_then(&filtered),
                ),
                Some(_) => (TriggerEventMask::NODE_CREATE, None, None),
                None => (TriggerEventMask::NODE_UPDATE, None, None),
            };
            let new_props_map = filtered(props);
            match labels {
                Some(ls) if !ls.is_empty() => {
                    for l in ls {
                        rows.push(MutationRow {
                            event_kind: kind,
                            vid_or_eid: id,
                            label_or_type: l.clone(),
                            old_value: old.clone(),
                            new_properties: new_props_map.clone(),
                            old_properties: old_props_map.clone(),
                        });
                    }
                }
                _ => {
                    rows.push(MutationRow {
                        event_kind: kind,
                        vid_or_eid: id,
                        label_or_type: String::new(),
                        old_value: old,
                        new_properties: new_props_map,
                        old_properties: old_props_map,
                    });
                }
            }
        }

        // Vertex deletes. `old_value` is the pre-tx property image when
        // the probe captured it (the row is about to disappear).
        for vid in &l0.vertex_tombstones {
            let id = vid_to_i64(*vid);
            let labels = l0.vertex_labels.get(vid);
            let old = probe.and_then(|p| p.vertex_old_bytes(*vid));
            let old_props_map = probe
                .and_then(|p| p.vertex_properties(*vid))
                .and_then(&filtered);
            match labels {
                Some(ls) if !ls.is_empty() => {
                    for l in ls {
                        rows.push(MutationRow {
                            event_kind: TriggerEventMask::NODE_DELETE,
                            vid_or_eid: id,
                            label_or_type: l.clone(),
                            old_value: old.clone(),
                            new_properties: None,
                            old_properties: old_props_map.clone(),
                        });
                    }
                }
                _ => {
                    rows.push(MutationRow {
                        event_kind: TriggerEventMask::NODE_DELETE,
                        vid_or_eid: id,
                        label_or_type: String::new(),
                        old_value: old,
                        new_properties: None,
                        old_properties: old_props_map,
                    });
                }
            }
        }

        // Edge writes — CREATE if not pre-existing, else UPDATE.
        // `old_value` carries the pre-image edge properties for UPDATE
        // and is `None` for CREATE.
        for eid in l0.edge_endpoints.keys() {
            if l0.tombstones.contains_key(eid) {
                continue;
            }
            let etype = l0.edge_types.get(eid).cloned().unwrap_or_default();
            let (kind, old, old_props_map) = match probe {
                Some(p) if p.edge_pre_existed(*eid) => (
                    TriggerEventMask::EDGE_UPDATE,
                    p.edge_old_bytes(*eid),
                    p.edge_properties(*eid).and_then(&filtered),
                ),
                Some(_) => (TriggerEventMask::EDGE_CREATE, None, None),
                None => (TriggerEventMask::EDGE_UPDATE, None, None),
            };
            let new_props_map = l0.edge_properties.get(eid).and_then(&filtered);
            rows.push(MutationRow {
                event_kind: kind,
                vid_or_eid: eid_to_i64(*eid),
                label_or_type: etype,
                old_value: old,
                new_properties: new_props_map,
                old_properties: old_props_map,
            });
        }

        // Edge deletes. `old_value` is the pre-tx property image when
        // the probe captured it.
        for (eid, ts) in &l0.tombstones {
            let etype = l0
                .edge_types
                .get(eid)
                .cloned()
                .unwrap_or_else(|| format!("type:{}", ts.edge_type));
            let old = probe.and_then(|p| p.edge_old_bytes(*eid));
            let old_props_map = probe
                .and_then(|p| p.edge_properties(*eid))
                .and_then(&filtered);
            rows.push(MutationRow {
                event_kind: TriggerEventMask::EDGE_DELETE,
                vid_or_eid: eid_to_i64(*eid),
                label_or_type: etype,
                old_value: old,
                new_properties: None,
                old_properties: old_props_map,
            });
        }

        Self { rows }
    }

    /// Project every captured row into the canonical [`event_row_schema`]
    /// `RecordBatch`, with no per-route filtering and no predicate.
    ///
    /// Used by the CDC delivery path to hand subscribers the full
    /// stream of mutations for a committed transaction (M11 FU-4). The
    /// per-trigger filtered shape is built by `Self::filter_for`.
    ///
    /// Returns `None` when there are zero rows (lets callers skip
    /// constructing an empty `CdcBatch`).
    #[must_use]
    pub fn materialize_all(&self) -> Option<RecordBatch> {
        if self.rows.is_empty() {
            return None;
        }
        EventRowColumns::with_capacity(self.rows.len())
            .extend(self.rows.iter())
            .into_batch()
    }

    /// Filter rows matching `entry`'s subscription selectors and
    /// project them into the §4.18 RecordBatch shape. Returns `None`
    /// if no rows match (caller skips the `fire` call).
    fn filter_for(&self, entry: &RouteEntry) -> Option<RecordBatch> {
        // property_filter is satisfied vacuously here — per-property
        // event-row population (one row per (vid, property) write) is
        // not the chosen surface; predicate authors instead reference
        // `n.<prop>` directly and the property-bag column resolves it
        // through `index`.
        let _ = &entry.property_filter;
        let mut cols = EventRowColumns::default();
        for row in &self.rows {
            if entry.matches(row.event_kind, &row.label_or_type) {
                cols.push_row(row);
            }
        }
        let batch = cols.into_batch()?;

        // Apply the compiled `predicate_source` boolean mask if any.
        // Evaluation failures degrade safely to "no rows match" — the
        // predicate was already validated at router build, so failures
        // here imply an Arrow/DataFusion bug we'd rather skip than
        // surface as a commit error.
        let batch = match &entry.compiled_predicate {
            Some(predicate) => apply_predicate(predicate, batch)?,
            None => batch,
        };

        if batch.num_rows() == 0 {
            return None;
        }
        Some(batch)
    }
}

/// Column-oriented builder for the canonical event-row [`RecordBatch`]
/// produced by [`MutationEvents::materialize_all`] and
/// [`MutationEvents::filter_for`]. Keeps the per-column allocation +
/// per-row push logic in one place so the two callers stay in lockstep.
#[derive(Default)]
struct EventRowColumns {
    kinds: Vec<u8>,
    ids: Vec<i64>,
    labels: Vec<String>,
    properties: Vec<String>,
    olds: Vec<Option<Vec<u8>>>,
    news: Vec<Option<Vec<u8>>>,
    props_new: Vec<Option<Vec<u8>>>,
    props_old: Vec<Option<Vec<u8>>>,
}

impl EventRowColumns {
    fn with_capacity(cap: usize) -> Self {
        Self {
            kinds: Vec::with_capacity(cap),
            ids: Vec::with_capacity(cap),
            labels: Vec::with_capacity(cap),
            properties: Vec::with_capacity(cap),
            olds: Vec::with_capacity(cap),
            news: Vec::with_capacity(cap),
            props_new: Vec::with_capacity(cap),
            props_old: Vec::with_capacity(cap),
        }
    }

    fn push_row(&mut self, row: &MutationRow) {
        self.kinds.push(mask_to_discriminant(row.event_kind));
        self.ids.push(row.vid_or_eid);
        self.labels.push(row.label_or_type.clone());
        self.properties.push(String::new());
        self.olds.push(row.old_value.clone());
        self.news.push(None);
        self.props_new.push(
            row.new_properties
                .as_ref()
                .map(|m| cypher_value_codec::encode(&Value::Map(m.clone()))),
        );
        self.props_old.push(
            row.old_properties
                .as_ref()
                .map(|m| cypher_value_codec::encode(&Value::Map(m.clone()))),
        );
    }

    fn extend<'a>(mut self, rows: impl IntoIterator<Item = &'a MutationRow>) -> Self {
        for row in rows {
            self.push_row(row);
        }
        self
    }

    /// Materialize the columns into a `RecordBatch`. Returns `None`
    /// when zero rows were collected (callers skip the empty case).
    fn into_batch(self) -> Option<RecordBatch> {
        if self.kinds.is_empty() {
            return None;
        }
        let kind_arr: Arc<dyn arrow_array::Array> = Arc::new(UInt8Array::from(self.kinds));
        let id_arr: Arc<dyn arrow_array::Array> = Arc::new(Int64Array::from(self.ids));
        let label_arr: Arc<dyn arrow_array::Array> =
            Arc::new(arrow_array::StringArray::from(self.labels));
        let prop_arr: Arc<dyn arrow_array::Array> =
            Arc::new(arrow_array::StringArray::from(self.properties));
        let olds_iter: Vec<Option<&[u8]>> = self.olds.iter().map(|o| o.as_deref()).collect();
        let news_iter: Vec<Option<&[u8]>> = self.news.iter().map(|o| o.as_deref()).collect();
        let old_arr: Arc<dyn arrow_array::Array> = Arc::new(LargeBinaryArray::from(olds_iter));
        let new_arr: Arc<dyn arrow_array::Array> = Arc::new(LargeBinaryArray::from(news_iter));
        let pnew_iter: Vec<Option<&[u8]>> = self.props_new.iter().map(|o| o.as_deref()).collect();
        let pold_iter: Vec<Option<&[u8]>> = self.props_old.iter().map(|o| o.as_deref()).collect();
        let pnew_arr: Arc<dyn arrow_array::Array> = Arc::new(LargeBinaryArray::from(pnew_iter));
        let pold_arr: Arc<dyn arrow_array::Array> = Arc::new(LargeBinaryArray::from(pold_iter));

        RecordBatch::try_new(
            event_row_schema(),
            vec![
                kind_arr, id_arr, label_arr, prop_arr, old_arr, new_arr, pnew_arr, pold_arr,
            ],
        )
        .ok()
    }
}

/// Run a compiled trigger predicate against the candidate batch,
/// returning `Some(filtered_batch)` when at least one row passes and
/// `None` when the predicate eliminates every row or the evaluation
/// fails (logged at warn level, treated as "no match" to avoid
/// silently firing on rows the predicate would have rejected).
fn apply_predicate(predicate: &Arc<dyn PhysicalExpr>, batch: RecordBatch) -> Option<RecordBatch> {
    use datafusion::arrow::compute::filter_record_batch;
    use datafusion::logical_expr::ColumnarValue;

    let value = match predicate.evaluate(&batch) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "trigger predicate evaluation failed; dropping batch");
            return None;
        }
    };
    let array = match value {
        ColumnarValue::Array(a) => a,
        ColumnarValue::Scalar(s) => match s.to_array_of_size(batch.num_rows()) {
            Ok(a) => a,
            Err(e) => {
                warn!(error = %e, "trigger predicate scalar→array failed");
                return None;
            }
        },
    };
    let bool_arr = match array.as_any().downcast_ref::<BooleanArray>() {
        Some(b) => b,
        None => {
            warn!("trigger predicate must yield Boolean; dropping batch");
            return None;
        }
    };
    filter_record_batch(&batch, bool_arr).ok()
}

fn mask_to_discriminant(m: TriggerEventMask) -> u8 {
    // Bit position of the (single) set bit; falls back to 0 if
    // multiple bits are set (not expected for emitted rows).
    let mut bits = m.0;
    let mut idx: u8 = 0;
    if bits == 0 {
        return 0;
    }
    while bits & 1 == 0 {
        bits >>= 1;
        idx += 1;
    }
    idx + 1
}

fn vid_to_i64(vid: uni_common::Vid) -> i64 {
    // Vid is a newtype around a u64; reinterpret-cast preserves bits.
    vid.as_u64() as i64
}

fn eid_to_i64(eid: uni_common::Eid) -> i64 {
    eid.as_u64() as i64
}

// ── M11 deferral queue (memory-backed v1) ──────────────────────────

/// Maximum number of times a `TriggerOutcome::Defer` will be re-queued
/// before the queue gives up and drops the item with a warning. Caps
/// the worst case for a pathological plugin that always returns
/// `Defer` from cascading.
const DEFER_MAX_ATTEMPTS: u32 = 10;

struct DeferredItem {
    plugin: Arc<dyn TriggerPlugin>,
    name: String,
    batch: MutationBatch,
    session_id: String,
    tx_id: u64,
    attempts: u32,
    /// `TriggerDeferral::payload` passed back to
    /// [`TriggerPlugin::on_deferred`] when this item fires (FU-5).
    payload: String,
}

/// In-memory deferral queue for `TriggerOutcome::Defer`.
///
/// Items are keyed by their scheduled fire instant in a `BTreeMap`,
/// so `drain_due` pops the next-due slot in O(log n). The queue is
/// drained by a per-`Uni` background tick task spawned at DB build
/// time; firing happens on the tokio runtime.
///
/// **v1 limitations** (in-memory only):
/// - Restart drops queued items. A persistent queue (system-table or
///   WAL extension) is `TODO(M11-persist)`.
/// - No transactional guarantee that a deferred item eventually fires
///   — if the process exits before the scheduled instant, the item is
///   lost.
/// - Per-item retry is capped at `DEFER_MAX_ATTEMPTS` to prevent
///   runaway re-deferral loops.
#[derive(Default)]
pub struct DeferralQueue {
    inner: parking_lot::Mutex<BTreeMap<StdInstant, Vec<DeferredItem>>>,
    /// Optional JSON-sidecar persistence (FU-5). When set, every
    /// `push` mirrors the queue state to disk and every `drain_due`
    /// rewrites the sidecar so a crash-restart can re-load the queue
    /// state. The persistence sink resolves [`TriggerPlugin`]s by qname
    /// from the host's [`uni_plugin::PluginRegistry`] at load time.
    sidecar: parking_lot::Mutex<Option<DeferralSidecar>>,
}

impl std::fmt::Debug for DeferralQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len: usize = self.inner.lock().values().map(|v| v.len()).sum();
        f.debug_struct("DeferralQueue").field("size", &len).finish()
    }
}

impl DeferralQueue {
    /// Build a fresh empty queue.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Build a queue with JSON-sidecar persistence rooted at
    /// `<data_path>/_system/deferred_triggers.json`.
    ///
    /// On startup the queue's `load` method walks the sidecar and
    /// re-binds each row to its `TriggerPlugin` by qname via the
    /// supplied [`uni_plugin::PluginRegistry`]. Items whose plugin
    /// can no longer be resolved are dropped with a warn.
    ///
    /// Persists on every `push` and after every `drain_due` (FU-5).
    /// I/O failures degrade to debug logs — in-memory queue state
    /// remains authoritative for the running process.
    #[must_use]
    pub fn with_persistence(data_path: std::path::PathBuf) -> Arc<Self> {
        let queue = Arc::new(Self::default());
        let mut sidecar_path = data_path;
        sidecar_path.push("_system");
        sidecar_path.push("deferred_triggers.json");
        *queue.sidecar.lock() = Some(DeferralSidecar { path: sidecar_path });
        queue
    }

    /// Borrow the sidecar path, if persistence is enabled.
    #[must_use]
    pub fn sidecar_path(&self) -> Option<std::path::PathBuf> {
        self.sidecar.lock().as_ref().map(|s| s.path.clone())
    }

    /// Replay persisted items from the sidecar, re-binding each row's
    /// trigger qname against the registry. Should be called once
    /// after `Uni::build` finishes wiring triggers but before the
    /// queue tick task starts. Idempotent.
    ///
    /// Returns the number of items reloaded.
    pub fn load_from_sidecar(
        self: &Arc<Self>,
        registry: &Arc<uni_plugin::PluginRegistry>,
    ) -> usize {
        let Some(sidecar) = self.sidecar.lock().clone() else {
            return 0;
        };
        let now_wall = std::time::SystemTime::now();
        let now_mono = StdInstant::now();
        let rows = match sidecar.read_all() {
            Ok(rows) => rows,
            Err(e) => {
                tracing::debug!(error = %e, "DeferralQueue: sidecar read failed");
                return 0;
            }
        };
        let mut restored = 0usize;
        for row in rows {
            let Some(entry) = registry
                .triggers()
                .iter()
                .find(|t| subscription_name(t.subscription()) == row.name)
                .cloned()
            else {
                tracing::warn!(
                    trigger = %row.name,
                    "DeferralQueue: dropping persisted item; trigger no longer registered"
                );
                continue;
            };
            // Re-decode the persisted MutationBatch from Arrow IPC.
            let batch = match arrow_ipc_decode(&row.batch_ipc) {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(error = %e, "DeferralQueue: drop persisted item; IPC decode failed");
                    continue;
                }
            };
            // Translate the persisted wall-clock fire_at to a monotonic
            // Instant relative to current time. Past-due fire-ats
            // collapse to "now" so they fire on the next tick.
            let fire_at_wall = std::time::UNIX_EPOCH + Duration::from_millis(row.fire_at_epoch_ms);
            let mono_delta = fire_at_wall
                .duration_since(now_wall)
                .unwrap_or(Duration::ZERO);
            let fire_at_mono = now_mono + mono_delta;
            let item = DeferredItem {
                plugin: entry,
                name: row.name,
                batch: MutationBatch {
                    events: Arc::new(batch),
                },
                session_id: row.session_id,
                tx_id: row.tx_id,
                attempts: row.attempts,
                payload: row.payload,
            };
            self.inner
                .lock()
                .entry(fire_at_mono)
                .or_default()
                .push(item);
            restored += 1;
        }
        restored
    }

    /// Persist the current queue state to the sidecar (no-op when
    /// persistence is disabled). I/O errors degrade to debug log.
    fn persist_locked(
        &self,
        guard: &parking_lot::MutexGuard<'_, BTreeMap<StdInstant, Vec<DeferredItem>>>,
    ) {
        let Some(sidecar) = self.sidecar.lock().clone() else {
            return;
        };
        let now_wall = std::time::SystemTime::now();
        let now_mono = StdInstant::now();
        let mut rows: Vec<PersistedDeferral> = Vec::new();
        for (fire_at_mono, items) in guard.iter() {
            for item in items {
                // Convert the monotonic Instant back to wall-clock by
                // measuring the delta against `now` and offsetting
                // `now_wall`. Past-due items get a fire_at slightly
                // before `now_wall` so they fire immediately on
                // restart.
                let fire_at_wall = if *fire_at_mono <= now_mono {
                    now_wall
                } else {
                    now_wall + fire_at_mono.duration_since(now_mono)
                };
                let fire_at_epoch_ms = fire_at_wall
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let batch_ipc = match arrow_ipc_encode(&item.batch.events) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::debug!(error = %e, "DeferralQueue: IPC encode failed; skipping row");
                        continue;
                    }
                };
                rows.push(PersistedDeferral {
                    name: item.name.clone(),
                    session_id: item.session_id.clone(),
                    tx_id: item.tx_id,
                    attempts: item.attempts,
                    payload: item.payload.clone(),
                    batch_ipc,
                    fire_at_epoch_ms,
                });
            }
        }
        if let Err(e) = sidecar.write_all(&rows) {
            tracing::debug!(error = %e, "DeferralQueue: sidecar write failed");
        }
    }

    fn push(&self, item: DeferredItem, fire_at: StdInstant) {
        let mut guard = self.inner.lock();
        guard.entry(fire_at).or_default().push(item);
        self.persist_locked(&guard);
    }

    /// Pop every item whose scheduled fire instant is `<= now`.
    fn drain_due(&self, now: StdInstant) -> Vec<DeferredItem> {
        let mut guard = self.inner.lock();
        let mut due = Vec::new();
        // BTreeMap::split_off gives us [now+ε..) so we keep that half
        // and the front half is everything ≤ now.
        let mut to_keep = guard.split_off(&(now + Duration::from_nanos(1)));
        std::mem::swap(&mut *guard, &mut to_keep);
        for (_, mut items) in to_keep {
            due.append(&mut items);
        }
        // FU-5: persist the remaining queue state after each drain so
        // a restart sees only the still-pending items.
        self.persist_locked(&guard);
        due
    }

    /// Approximate pending count — for diagnostics / tests.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.inner.lock().values().map(|v| v.len()).sum()
    }

    /// Tick the queue once: drain due items, fire each. Items that
    /// re-defer are re-enqueued until `DEFER_MAX_ATTEMPTS`. Async
    /// because plugin `fire` may block the runtime; we re-enter the
    /// tokio executor between items via `spawn_blocking` -- but since
    /// most triggers are CPU-light, the inline call here is fine for
    /// v1.
    pub fn tick(self: &Arc<Self>) {
        let due = self.drain_due(StdInstant::now());
        for mut item in due {
            // FU-5: invoke the dedicated `on_deferred` callback so
            // trigger plugins can receive the original `payload`.
            // The default impl on the trait delegates back to `fire`,
            // so existing trigger plugins keep working unchanged.
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                item.plugin.on_deferred(
                    TriggerContext::new(&item.session_id, item.tx_id),
                    &item.batch,
                    &item.payload,
                )
            }));
            match outcome {
                Ok(Ok(TriggerOutcome::Defer { until })) => {
                    item.attempts += 1;
                    if item.attempts >= DEFER_MAX_ATTEMPTS {
                        warn!(
                            trigger = %item.name,
                            attempts = item.attempts,
                            "deferred trigger exceeded DEFER_MAX_ATTEMPTS; dropping"
                        );
                        continue;
                    }
                    // FU-5: honor the new `delay` field when re-deferring.
                    // `None` falls back to "next tick" — matches the
                    // legacy semantics. The trigger may have updated
                    // the payload on re-defer; propagate the new one.
                    let fire_at = StdInstant::now() + until.delay.unwrap_or(Duration::ZERO);
                    item.payload = until.payload;
                    self.push(item, fire_at);
                }
                Ok(Ok(_)) => {
                    // Continue, Reject, or future variant — treat all
                    // as "done" for queue purposes. Reject after the
                    // fact has no commit to abort against.
                }
                Ok(Err(e)) => {
                    warn!(trigger = %item.name, error = %e, "deferred trigger errored");
                }
                Err(_) => {
                    warn!(trigger = %item.name, "deferred trigger panicked");
                }
            }
        }
    }
}

// ── Helpers used by `Transaction::commit` ──────────────────────────

/// Convenience: stable-hash a `&str` tx id (commit path stores tx_id
/// as `String`) down to the `u64` the `TriggerContext` carries.
#[must_use]
pub fn tx_id_to_u64(tx_id: &str) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    tx_id.hash(&mut hasher);
    hasher.finish()
}

// ── FU-5: persisted deferral sidecar ──────────────────────────────

/// On-disk row in `<data_path>/_system/deferred_triggers.json`.
///
/// `batch_ipc` is the trigger's [`MutationBatch`] encoded as Arrow
/// IPC stream bytes — preserves schema + values across restarts. The
/// `name` is the trigger's `subscription_name`, which the host's
/// re-resolution path uses to find the registered `TriggerPlugin`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct PersistedDeferral {
    name: String,
    session_id: String,
    tx_id: u64,
    attempts: u32,
    payload: String,
    /// Arrow IPC stream bytes for the [`MutationBatch::events`]
    /// `RecordBatch`.
    #[serde(with = "serde_bytes")]
    batch_ipc: Vec<u8>,
    /// Wall-clock fire instant, milliseconds since UNIX epoch.
    fire_at_epoch_ms: u64,
}

/// Atomic JSON-sidecar persistence handle for the deferral queue.
#[derive(Clone, Debug)]
struct DeferralSidecar {
    path: std::path::PathBuf,
}

impl DeferralSidecar {
    fn read_all(&self) -> Result<Vec<PersistedDeferral>, String> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let bytes = std::fs::read(&self.path).map_err(|e| format!("read {:?}: {e}", self.path))?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        serde_json::from_slice(&bytes).map_err(|e| format!("parse {:?}: {e}", self.path))
    }

    fn write_all(&self, rows: &[PersistedDeferral]) -> Result<(), String> {
        if let Some(parent) = self.path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|e| format!("mkdir {parent:?}: {e}"))?;
        }
        let json = serde_json::to_vec_pretty(rows).map_err(|e| format!("encode: {e}"))?;
        let tmp = self.path.with_extension("tmp");
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp).map_err(|e| format!("create {tmp:?}: {e}"))?;
            f.write_all(&json)
                .map_err(|e| format!("write {tmp:?}: {e}"))?;
            f.sync_all().map_err(|e| format!("sync {tmp:?}: {e}"))?;
        }
        std::fs::rename(&tmp, &self.path)
            .map_err(|e| format!("rename {tmp:?}->{:?}: {e}", self.path))?;
        Ok(())
    }
}

/// Encode a `RecordBatch` as Arrow IPC stream bytes (FU-5).
fn arrow_ipc_encode(batch: &arrow_array::RecordBatch) -> Result<Vec<u8>, String> {
    let schema = batch.schema();
    let mut buf: Vec<u8> = Vec::with_capacity(2048);
    {
        let mut w = arrow_ipc::writer::StreamWriter::try_new(&mut buf, schema.as_ref())
            .map_err(|e| format!("ipc writer: {e}"))?;
        w.write(batch).map_err(|e| format!("ipc write: {e}"))?;
        w.finish().map_err(|e| format!("ipc finish: {e}"))?;
    }
    Ok(buf)
}

/// Decode Arrow IPC stream bytes into a single `RecordBatch` (FU-5).
fn arrow_ipc_decode(bytes: &[u8]) -> Result<arrow_array::RecordBatch, String> {
    let reader = arrow_ipc::reader::StreamReader::try_new(bytes, None)
        .map_err(|e| format!("ipc reader: {e}"))?;
    let batches: Vec<arrow_array::RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("ipc collect: {e}"))?;
    batches
        .into_iter()
        .next()
        .ok_or_else(|| "ipc decode: empty stream".to_owned())
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use uni_plugin::traits::trigger::TriggerEventMask;

    #[test]
    fn mask_discriminants_are_stable() {
        assert_eq!(mask_to_discriminant(TriggerEventMask::NODE_CREATE), 1);
        assert_eq!(mask_to_discriminant(TriggerEventMask::NODE_UPDATE), 2);
        assert_eq!(mask_to_discriminant(TriggerEventMask::NODE_DELETE), 3);
        assert_eq!(mask_to_discriminant(TriggerEventMask::EDGE_CREATE), 4);
        assert_eq!(mask_to_discriminant(TriggerEventMask::EDGE_UPDATE), 5);
        assert_eq!(mask_to_discriminant(TriggerEventMask::EDGE_DELETE), 6);
    }

    #[test]
    fn empty_router_is_empty() {
        let by_phase = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
        let router = TriggerRouter {
            by_phase,
            defer_queue: None,
        };
        assert!(router.is_empty());
    }

    #[test]
    fn tx_id_to_u64_is_deterministic() {
        let a = tx_id_to_u64("tx-1");
        let b = tx_id_to_u64("tx-1");
        let c = tx_id_to_u64("tx-2");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
