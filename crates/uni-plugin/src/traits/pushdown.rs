//! Pushdown negotiation traits — Spark DSv2 / Trino-style marker traits.
//!
//! Storage backends, index handles, catalog tables, and operator providers
//! may *additionally* implement any subset of these traits to negotiate
//! filter / projection / limit / topN / aggregate pushdown with the
//! planner. Marker traits per capability let backends opt in to only what
//! they can handle.

use datafusion::logical_expr::Expr;

/// Result of consulting a filter-pushdown source.
#[derive(Clone, Debug, Default)]
pub struct FilterApplication {
    /// Indices into the filter list the source handles completely (the
    /// planner removes the corresponding `Filter` ops).
    pub fully_handled: Vec<usize>,
    /// Indices the source handles approximately (planner keeps a
    /// verifying `Filter`).
    pub partially_handled: Vec<usize>,
}

/// Result of consulting a projection-pushdown source.
#[derive(Clone, Debug, Default)]
pub struct ProjectionApplication {
    /// Column names to actually fetch.
    pub keep: Vec<String>,
}

/// Scope at which a TopN pushdown was applied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopNScope {
    /// Per-partition local TopN (planner combines).
    Local,
    /// Globally-applied — no further `Sort + Limit` needed.
    Global,
}

/// Result of consulting a TopN-pushdown source.
#[derive(Clone, Debug)]
pub struct TopNApplication {
    /// Scope at which the source applied the topN.
    pub applied: TopNScope,
}

/// Result of consulting an aggregate-pushdown source.
#[derive(Clone, Debug, Default)]
pub struct AggregateApplication {
    /// Indices into the aggregate-expression list the source handles.
    pub fully_handled: Vec<usize>,
    /// `true` if the source returns *partial* state (the planner adds a
    /// Final aggregate above to combine partials).
    pub returns_partial_state: bool,
}

/// Marker trait: source supports filter pushdown.
pub trait SupportsFilterPushdown {
    /// Inspect filters and report which the source handles.
    fn push_filters(&self, filters: &[Expr]) -> FilterApplication;
}

/// Marker trait: source supports projection pushdown.
pub trait SupportsProjectionPushdown {
    /// Declare which projected columns to actually read.
    fn push_projection(&self, columns: &[String]) -> ProjectionApplication;
}

/// Marker trait: source supports limit pushdown.
pub trait SupportsLimitPushdown {
    /// `Some(applied)` if the source enforces the limit; `None` if not.
    fn push_limit(&self, limit: usize) -> Option<usize>;
}

/// A sort expression for topN pushdown.
#[derive(Clone, Debug)]
pub struct SortExpr {
    /// Column name to sort by.
    pub column: String,
    /// Sort direction.
    pub ascending: bool,
    /// Null ordering.
    pub nulls_first: bool,
}

/// Marker trait: source supports topN (sort + limit) pushdown.
pub trait SupportsTopNPushdown {
    /// Apply topN at the source if possible.
    fn push_topn(&self, sort: &[SortExpr], k: usize) -> Option<TopNApplication>;
}

/// Marker trait: source supports aggregate pushdown.
pub trait SupportsAggregatePushdown {
    /// Declare which aggregates the source can compute.
    fn push_aggregates(&self, group_by: &[Expr], aggs: &[Expr]) -> AggregateApplication;
}
