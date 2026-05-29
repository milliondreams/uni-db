//! Session-lifecycle hooks — Postgres-style phased.
//!
//! The trait expands the legacy `before_query` / `after_query` shape to
//! phased hooks at parse / analyze / plan / execute_start / execute_end /
//! before_commit / after_commit / abort. Each phase has a default no-op
//! implementation so existing hooks that only override the legacy methods
//! continue to work.

use std::time::Duration;

use datafusion::scalar::ScalarValue;
use smol_str::SmolStr;

use crate::errors::HookOutcome;

/// Classification of the query under observation.
///
/// Mirrors the host's surface-level distinction between Cypher reads,
/// Locy program evaluations, and Execute (mutation) statements without
/// pulling a `uni-db` dependency into `uni-plugin` (which would create a
/// circular dep). The bridge in `uni-db` is responsible for translating
/// between this enum and the host's `crate::api::hooks::QueryType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueryType {
    /// A Cypher query (read or write).
    #[default]
    Cypher,
    /// A Locy program evaluation.
    Locy,
    /// An execute (mutation) statement.
    Execute,
}

/// Slim mirror of the host's commit metadata.
///
/// Surfaced to phased `after_commit` hooks via `CommitContext` so they
/// observe real post-commit values instead of zero-filled stubs. The
/// fields are a deliberate subset — anything operationally meaningful
/// to a hook (commit count, version, WAL LSN, wall-clock duration).
///
/// The host's bridge populates this from its own `CommitResult`; this
/// type stays free of host imports to preserve `uni-plugin`'s
/// loader-agnostic invariant.
#[derive(Debug, Clone, Default)]
pub struct PluginCommitResult {
    /// Number of mutations committed.
    pub mutations: u64,
    /// Database version after commit.
    pub version: u64,
    /// WAL log sequence number of the commit (0 when no WAL is configured).
    pub wal_lsn: u64,
    /// Duration of the commit operation (lock + WAL + merge).
    pub duration: Duration,
}

/// Session-lifecycle hook plugin.
///
/// Every method has a default that does nothing; implementations override
/// only the phases they need. Phased dispatch lets a hook plugin perform
/// audit at `on_execute_end` without paying parse-time cost, etc.
pub trait SessionHook: Send + Sync {
    /// Called after the query source is parsed; the hook may reject parse
    /// failures or annotate the parse for downstream phases.
    fn on_parse(&self, _ctx: &ParseContext<'_>) -> HookOutcome {
        HookOutcome::Continue
    }

    /// Called after semantic analysis. Useful for row-level security
    /// predicate injection.
    fn on_analyze(&self, _ctx: &AnalyzeContext<'_>) -> HookOutcome {
        HookOutcome::Continue
    }

    /// Called after logical planning; the hook may rewrite the plan.
    fn on_plan(&self, _ctx: &PlanContext<'_>) -> HookOutcome {
        HookOutcome::Continue
    }

    /// Called immediately before physical execution begins.
    fn on_execute_start(&self, _ctx: &ExecuteContext<'_>) -> HookOutcome {
        HookOutcome::Continue
    }

    /// Called once execution finishes with the collected metrics.
    fn on_execute_end(&self, _ctx: &ExecuteContext<'_>, _metrics: &QueryMetrics) {}

    /// Called before commit; may reject the transaction.
    fn before_commit(&self, _ctx: &CommitContext<'_>) -> HookOutcome {
        HookOutcome::Continue
    }

    /// Called after a successful commit.
    fn after_commit(&self, _ctx: &CommitContext<'_>) {}

    /// Called when a transaction aborts (by rollback or error).
    fn on_abort(&self, _ctx: &AbortContext<'_>) {}
}

/// Parse-phase context.
///
/// `query_type` defaults to [`QueryType::Cypher`] for back-compat with
/// hooks built against the v1.0 shape; populate via
/// [`Self::with_query_type`] when the host knows the language up front.
/// `params` defaults to an empty slice; populate via
/// [`Self::with_params`] to surface bound query parameters to hooks
/// (Arrow-shaped to keep `uni-plugin` free of any `uni-common` dep).
#[derive(Debug)]
#[non_exhaustive]
pub struct ParseContext<'a> {
    /// Raw source text of the query.
    pub source: &'a str,
    /// Session identifier.
    pub session_id: &'a str,
    /// Query language classification (v1.1).
    pub query_type: QueryType,
    /// Bound query parameters as `(name, value)` pairs (v1.1).
    pub params: &'a [(SmolStr, ScalarValue)],
}

impl<'a> ParseContext<'a> {
    /// Construct a parse context with defaults for the v1.1 fields.
    ///
    /// Hooks built outside of `uni-plugin` use this constructor; the
    /// struct is `#[non_exhaustive]` so direct struct-literal
    /// construction is forbidden. `query_type` defaults to
    /// [`QueryType::Cypher`]; `params` defaults to an empty slice.
    /// Override via the builders below.
    #[must_use]
    pub fn new(source: &'a str, session_id: &'a str) -> Self {
        Self {
            source,
            session_id,
            query_type: QueryType::default(),
            params: &[],
        }
    }

    /// Override the query-language classification.
    #[must_use]
    pub fn with_query_type(mut self, query_type: QueryType) -> Self {
        self.query_type = query_type;
        self
    }

    /// Attach a borrowed slice of bound query parameters.
    #[must_use]
    pub fn with_params(mut self, params: &'a [(SmolStr, ScalarValue)]) -> Self {
        self.params = params;
        self
    }
}

/// Analyze-phase context.
#[derive(Debug)]
#[non_exhaustive]
pub struct AnalyzeContext<'a> {
    /// Session identifier.
    pub session_id: &'a str,
    /// Lifetime marker.
    pub _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> AnalyzeContext<'a> {
    /// Construct an analyze context.
    #[must_use]
    pub fn new(session_id: &'a str) -> Self {
        Self {
            session_id,
            _marker: std::marker::PhantomData,
        }
    }
}

/// Plan-phase context — placeholder for the actual logical-plan handle.
#[derive(Debug)]
#[non_exhaustive]
pub struct PlanContext<'a> {
    /// Session identifier.
    pub session_id: &'a str,
    /// Lifetime marker.
    pub _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> PlanContext<'a> {
    /// Construct a plan context.
    #[must_use]
    pub fn new(session_id: &'a str) -> Self {
        Self {
            session_id,
            _marker: std::marker::PhantomData,
        }
    }
}

/// Execute-phase context.
#[derive(Debug)]
#[non_exhaustive]
pub struct ExecuteContext<'a> {
    /// Session identifier.
    pub session_id: &'a str,
    /// Lifetime marker.
    pub _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> ExecuteContext<'a> {
    /// Construct an execute context.
    #[must_use]
    pub fn new(session_id: &'a str) -> Self {
        Self {
            session_id,
            _marker: std::marker::PhantomData,
        }
    }
}

/// Commit-phase context.
///
/// `before_commit` callers leave `commit_result` as `None` (no result
/// exists yet). `after_commit` callers should populate it via
/// [`Self::with_commit_result`] so hooks observe real post-commit
/// metadata rather than zero-filled stubs (v1.1).
#[derive(Debug)]
#[non_exhaustive]
pub struct CommitContext<'a> {
    /// Session identifier.
    pub session_id: &'a str,
    /// Post-commit metadata (v1.1). `None` in `before_commit`; `Some`
    /// in `after_commit` when the host bridges the real result through.
    pub commit_result: Option<&'a PluginCommitResult>,
}

impl<'a> CommitContext<'a> {
    /// Construct a commit context with `commit_result = None`.
    #[must_use]
    pub fn new(session_id: &'a str) -> Self {
        Self {
            session_id,
            commit_result: None,
        }
    }

    /// Attach a borrowed post-commit result; used by `after_commit`.
    #[must_use]
    pub fn with_commit_result(mut self, result: &'a PluginCommitResult) -> Self {
        self.commit_result = Some(result);
        self
    }
}

/// Abort-phase context.
#[derive(Debug)]
#[non_exhaustive]
pub struct AbortContext<'a> {
    /// Session identifier.
    pub session_id: &'a str,
    /// Reason text.
    pub reason: &'a str,
}

impl<'a> AbortContext<'a> {
    /// Construct an abort context.
    #[must_use]
    pub fn new(session_id: &'a str, reason: &'a str) -> Self {
        Self { session_id, reason }
    }
}

/// Query execution metrics surfaced to `on_execute_end`.
#[derive(Clone, Debug, Default)]
pub struct QueryMetrics {
    /// Wall-clock duration of the entire query.
    pub elapsed: Duration,
    /// Rows produced (sum across output operators).
    pub rows_out: u64,
    /// Approximate bytes read from storage.
    pub bytes_read: u64,
}
