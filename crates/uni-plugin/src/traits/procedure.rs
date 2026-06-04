//! Cypher procedure plugins — `CALL ... YIELD ...`.
//!
//! Procedures differ from scalar functions in three ways: they can perform
//! writes, they return streams of rows (`YIELD a, b, c`), and they may
//! take optional input streams (`CALL ... { } IN TRANSACTIONS OF N`).

use std::any::Any;
use std::time::Duration;

use arrow_schema::Field;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use smol_str::SmolStr;

use crate::capability::SideEffects;
use crate::errors::FnError;
use crate::traits::connector::Principal;
use crate::traits::scalar::ArgType;

/// A Cypher procedure plugin — `CALL uni.foo.bar(args) YIELD ...`.
///
/// Procedures return a stream of `RecordBatch`es; the host attaches the
/// stream to the surrounding query plan via a `ProcedureCallExec` node.
pub trait ProcedurePlugin: Send + Sync {
    /// Static signature.
    fn signature(&self) -> &ProcedureSignature;

    /// Invoke the procedure with the given arguments and execution context.
    ///
    /// The returned stream is consumed lazily by downstream `YIELD`. The
    /// procedure is responsible for cooperatively yielding to the executor
    /// (no long blocking calls; use `tokio::task::yield_now` between batches).
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the procedure cannot start (validation
    /// failure, capability check). Errors raised *during* stream production
    /// are signaled via `Err` items in the stream.
    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError>;
}

/// Static signature of a procedure.
#[derive(Clone, Debug)]
pub struct ProcedureSignature {
    /// Named arguments, in declaration order.
    pub args: Vec<NamedArgType>,
    /// Schema of the `YIELD` columns.
    pub yields: Vec<Field>,
    /// Mode declaration — drives capability requirements.
    pub mode: ProcedureMode,
    /// Declared side-effects.
    pub side_effects: SideEffects,
    /// Optional retry contract for atomic / CAS-style procedures.
    pub retry_contract: Option<RetryContract>,
    /// Optional batch-input shape for `CALL { } IN TRANSACTIONS OF N`.
    pub batch_input: Option<BatchInputShape>,
    /// Markdown docs surfaced via `uni.plugin.help`.
    pub docs: String,
}

/// Named procedure argument.
#[derive(Clone, Debug)]
pub struct NamedArgType {
    /// Argument name (as `CALL fn(name => value)`).
    pub name: SmolStr,
    /// Argument type.
    pub ty: ArgType,
    /// Default value if omitted at call site.
    pub default: Option<ScalarValue>,
    /// Human-readable description.
    pub doc: String,
}

/// Procedure-mode declaration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProcedureMode {
    /// Read-only; requires `Capability::Procedure`.
    Read,
    /// May mutate graph; requires `Capability::Procedure + ProcedureWrites`.
    Write,
    /// May issue DDL; requires `Capability::Procedure + ProcedureSchema`.
    Schema,
    /// Administrative; requires `Capability::Procedure + ProcedureDbms`.
    Dbms,
}

/// Retry contract for procedures with optimistic-CAS semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RetryContract {
    /// Host will re-run the procedure on retryable conflict up to
    /// `max_retries` times.
    Atomic {
        /// Maximum retry count before giving up.
        max_retries: u32,
    },
}

/// Shape of an optional input stream for `CALL { } IN TRANSACTIONS OF N`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum BatchInputShape {
    /// Plain rows; the host batches them into N-row groups.
    Rows,
}

/// Marker trait for the host's procedure execution facilities.
///
/// Concrete hosts (such as `uni-query`'s `QueryProcedureHost`) implement
/// this and expose typed accessors on the concrete type. Plugins
/// downcast through [`ProcedureHost::as_any`] when they need
/// host-specific facilities (snapshot, schema manager, vector search,
/// algorithm registry). The trait is intentionally tiny — adding a new
/// host accessor does NOT touch the plugin ABI.
///
/// The proposal-spec `session: &Session` / `tx: Option<&Transaction>`
/// fields land in M6 once the public `Session` trait stabilizes; until
/// then the host pointer is the interim bridge for in-tree built-ins.
pub trait ProcedureHost: Send + Sync + Any {
    /// Returns the host as a downcastable `&dyn Any`.
    fn as_any(&self) -> &dyn Any;
}

/// Per-call context passed to [`ProcedurePlugin::invoke`].
///
/// Carries an optional host pointer (for in-tree built-ins that need
/// snapshot / schema / algorithm access), an optional principal (for
/// capability gating), and an optional wall-clock deadline. All fields
/// are `Option` so pure procedures and unit tests can construct a
/// context with [`ProcedureContext::default`].
#[derive(Default)]
#[non_exhaustive]
pub struct ProcedureContext<'a> {
    /// Host services pointer; `None` in pure procedure tests.
    pub host: Option<&'a dyn ProcedureHost>,
    /// Optional wall-clock deadline for the procedure invocation.
    pub deadline: Option<Duration>,
    /// Authenticated principal, if any.
    pub principal: Option<&'a Principal>,
    /// Lifetime marker. The plugin ABI keeps `'a` exposed so future
    /// fields (session / transaction) can borrow without a breaking
    /// change.
    pub _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> ProcedureContext<'a> {
    /// Construct a context with every field set to `None`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach a host pointer.
    #[must_use]
    pub fn with_host(mut self, host: &'a dyn ProcedureHost) -> Self {
        self.host = Some(host);
        self
    }

    /// Attach a wall-clock deadline.
    #[must_use]
    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Attach an authenticated principal.
    #[must_use]
    pub fn with_principal(mut self, principal: &'a Principal) -> Self {
        self.principal = Some(principal);
        self
    }
}

impl std::fmt::Debug for ProcedureContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcedureContext")
            .field("host", &self.host.map(|_| "<host>"))
            .field("deadline", &self.deadline)
            .field("principal", &self.principal)
            .finish()
    }
}
