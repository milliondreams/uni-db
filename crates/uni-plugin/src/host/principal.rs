//! Authenticated-principal task-local plumbing.
//!
//! Hosts (Session / Transaction execute boundaries) install the current
//! [`Principal`] into a Tokio task-local at the top of each query so deeper
//! procedure / UDF invocation sites can read it without threading the
//! principal through every internal API.
//!
//! This module also exposes [`maybe_scope_with_principal`] — the convenience
//! wrapper that collapses the `match principal { Some(p) => scoped..., None
//! => fut }.await` pattern previously duplicated across
//! `uni::api::{session,transaction}` and `uni-query::df_udfs`.
//!
//! # Stability
//!
//! Moved here in Phase 5 of the §1.2 consolidation pass. `uni-query`
//! re-exports the items below for backwards compatibility.

// Rust guideline compliant

use std::future::Future;
use std::sync::Arc;

use crate::traits::connector::Principal;

tokio::task_local! {
    /// Tokio task-local carrying the **authenticated principal** for
    /// the in-flight query.
    ///
    /// Set by the host-crate execute boundaries (`Session::query`,
    /// `Transaction::query`, `Transaction::execute`) so procedure
    /// invocation sites can populate `ProcedureContext::with_principal`
    /// without threading the principal through every internal API.
    /// Read at `uni-query`'s procedure-call paths immediately before
    /// calling `plugin.invoke(ctx, ...)`.
    ///
    /// Propagates across `.await` points within the same task tree;
    /// does NOT propagate across `tokio::spawn`. The synthetic
    /// procedure body bridge (`block_in_place` + `Handle::block_on`)
    /// stays on the same task so the principal remains visible to
    /// any nested `execute_inner_query` calls.
    pub static CURRENT_PRINCIPAL: Arc<Principal>;
}

/// Run `fut` inside a scope where [`current_principal`] resolves to
/// `principal`.
///
/// Use this at every host-crate boundary where a `Session` or
/// `Transaction` dispatches into the executor.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use uni_plugin::host::principal::{scoped_with_principal, current_principal};
/// use uni_plugin::traits::connector::Principal;
///
/// # async fn demo(principal: Arc<Principal>) {
/// scoped_with_principal(principal.clone(), async {
///     assert!(current_principal().is_some());
/// })
/// .await;
/// # }
/// ```
pub fn scoped_with_principal<F: Future>(
    principal: Arc<Principal>,
    fut: F,
) -> tokio::task::futures::TaskLocalFuture<Arc<Principal>, F> {
    CURRENT_PRINCIPAL.scope(principal, fut)
}

/// Borrow the principal active for the current execute scope, if any.
///
/// Returns `None` outside a [`scoped_with_principal`] scope (e.g.,
/// low-level unit tests that bypass `Session`).
#[must_use]
pub fn current_principal() -> Option<Arc<Principal>> {
    CURRENT_PRINCIPAL.try_with(|p| p.clone()).ok()
}

/// Run `fut` either inside a principal task-local scope or unwrapped,
/// depending on whether a principal was supplied.
///
/// Replaces the duplicated
/// `match principal { Some(p) => scoped_with_principal(p, fut).await, None => fut.await }`
/// pattern across `uni::api::{session,transaction}` and
/// `uni-query::df_udfs`.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use uni_plugin::host::principal::maybe_scope_with_principal;
/// use uni_plugin::traits::connector::Principal;
///
/// # async fn demo(principal: Option<Arc<Principal>>) -> u32 {
/// maybe_scope_with_principal(principal, async { 42 }).await
/// # }
/// ```
pub async fn maybe_scope_with_principal<F>(principal: Option<Arc<Principal>>, fut: F) -> F::Output
where
    F: Future,
{
    match principal {
        Some(p) => scoped_with_principal(p, fut).await,
        None => fut.await,
    }
}
