//! Procedure-context construction helper.
//!
//! Collapses the ~25-line "build host + attach writer + read principal +
//! construct context" block previously duplicated between the
//! simple-executor and DataFusion-graph procedure-call paths in
//! `uni-query` (see `query/executor/procedure.rs` and
//! `query/df_graph/procedure_call.rs`).
//!
//! Writer attachment varies between the two paths (`from_components` vs.
//! `from_graph_ctx_with_request` plus a writer mutation on the concrete
//! `QueryProcedureHost`), so it stays at the caller. This helper owns
//! only the principal-attachment + context-build step, which is the
//! piece that was textually identical at both sites.

// Rust guideline compliant

use crate::traits::connector::Principal;
use crate::traits::procedure::{ProcedureContext, ProcedureHost};

/// Build a [`ProcedureContext`] wiring `host` and an optional `principal`.
///
/// Replaces the duplicated "construct `ProcedureContext::new()`, chain
/// `with_host`, then conditionally chain `with_principal`" block. The
/// principal, when `Some`, is attached via the existing
/// [`ProcedureContext::with_principal`] builder; when `None` it is
/// simply omitted.
///
/// Writer attachment to the host happens at the call site because the
/// two host construction paths (`from_components` and
/// `from_graph_ctx_with_request` on `QueryProcedureHost`) differ in
/// shape; only the context-build was textually identical.
///
/// # Examples
///
/// ```no_run
/// use uni_plugin::host::context::build_procedure_context;
/// use uni_plugin::traits::connector::Principal;
/// use uni_plugin::traits::procedure::ProcedureHost;
///
/// # fn demo(host: &dyn ProcedureHost, principal: Option<&Principal>) {
/// let ctx = build_procedure_context(host, principal);
/// // pass `ctx` to `procedure.invoke(ctx, &args)`
/// # let _ = ctx;
/// # }
/// ```
#[must_use]
pub fn build_procedure_context<'a>(
    host: &'a dyn ProcedureHost,
    principal: Option<&'a Principal>,
) -> ProcedureContext<'a> {
    let mut ctx = ProcedureContext::new().with_host(host);
    if let Some(p) = principal {
        ctx = ctx.with_principal(p);
    }
    ctx
}
