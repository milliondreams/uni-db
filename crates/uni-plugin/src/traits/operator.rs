//! Physical operator + optimizer-rule plugins.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::context::SessionContext;
use datafusion::optimizer::OptimizerRule;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;

use crate::errors::FnError;

/// Per-planner-invocation context for [`OperatorProvider::plan`].
#[non_exhaustive]
pub struct PlannerArgs<'a> {
    /// Reference to the executing DataFusion `SessionContext`.
    pub session_ctx: &'a SessionContext,
    /// Input physical plans the operator should consume.
    pub input_plans: &'a [Arc<dyn ExecutionPlan>],
    /// Free-form JSON configuration.
    pub config_json: &'a str,
    /// Optional schema hint for the operator's output.
    pub schema_hint: Option<SchemaRef>,
}

impl std::fmt::Debug for PlannerArgs<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlannerArgs")
            .field("session_ctx", &"<SessionContext>")
            .field("input_plans.len", &self.input_plans.len())
            .field("config_json", &self.config_json)
            .field("schema_hint", &self.schema_hint)
            .finish()
    }
}

/// A custom physical operator factory.
pub trait OperatorProvider: Send + Sync {
    /// The logical name of this operator (`"hash_join_geo"`, …).
    fn logical_name(&self) -> &str;

    /// Construct an `ExecutionPlan` for an instance of this operator.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] on planning failure (incompatible inputs, bad
    /// configuration).
    fn plan(&self, args: PlannerArgs<'_>) -> Result<Arc<dyn ExecutionPlan>, FnError>;
}

/// Phase at which an `OptimizerRule` runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum OptimizerPhase {
    /// Logical optimizer.
    Logical,
    /// Physical optimizer.
    Physical,
    /// Both — the rule is applied at logical and physical phases.
    Both,
}

/// A registered optimizer-rule provider.
///
/// A provider that runs at the logical phase returns a logical
/// [`OptimizerRule`] from [`rule`](Self::rule); a provider that runs at
/// the physical phase returns a [`PhysicalOptimizerRule`] from
/// [`physical_rule`](Self::physical_rule). A `Both` provider must
/// supply both. The host iterates the registered providers, inspects
/// `phase`, and installs each rule into the matching DataFusion
/// optimizer chain.
///
/// The default `physical_rule` returns `None`, so existing
/// logical-only providers compile unchanged across the 1.6 → 1.7
/// minor bump.
pub trait OptimizerRuleProvider: Send + Sync {
    /// The DataFusion logical `OptimizerRule` to apply.
    ///
    /// Logical-phase and `Both`-phase providers must return a real
    /// rule. Physical-only providers may return any rule (the host
    /// ignores it when `phase()` is [`OptimizerPhase::Physical`]);
    /// returning a sentinel/no-op is conventional. The default impl
    /// returns a no-op rule that never rewrites.
    fn rule(&self) -> Arc<dyn OptimizerRule + Send + Sync> {
        Arc::new(NoopOptimizerRule)
    }

    /// The DataFusion physical [`PhysicalOptimizerRule`] to apply.
    ///
    /// Physical-phase and `Both`-phase providers should return
    /// `Some(...)`. The default `None` keeps existing logical-only
    /// providers source-compatible.
    fn physical_rule(&self) -> Option<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
        None
    }

    /// Phase the rule runs at.
    fn phase(&self) -> OptimizerPhase;

    /// Ordering hint — lower precedence rules run first.
    fn precedence(&self) -> i32 {
        0
    }
}

/// No-op logical `OptimizerRule` used as the default for
/// [`OptimizerRuleProvider::rule`].
///
/// Returned by the trait's default `rule()` implementation so that
/// physical-only providers do not have to construct a sentinel
/// themselves. The rule is `Bottom-Up` and never transforms the plan.
#[derive(Debug, Default)]
pub struct NoopOptimizerRule;

impl OptimizerRule for NoopOptimizerRule {
    fn name(&self) -> &str {
        "uni_noop_optimizer_rule"
    }

    fn apply_order(&self) -> Option<datafusion::optimizer::ApplyOrder> {
        Some(datafusion::optimizer::ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: datafusion::logical_expr::LogicalPlan,
        _config: &dyn datafusion::optimizer::OptimizerConfig,
    ) -> Result<
        datafusion::common::tree_node::Transformed<datafusion::logical_expr::LogicalPlan>,
        datafusion::error::DataFusionError,
    > {
        Ok(datafusion::common::tree_node::Transformed::no(plan))
    }
}
