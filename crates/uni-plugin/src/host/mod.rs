//! Host-side helpers for procedure execution and principal threading.
//!
//! Two helpers consolidate the §1.2 duplication flagged in
//! `CODE_SIMPLIFIER_FEEDBACK.md`:
//!
//! - [`context::build_procedure_context`] — replaces the duplicated
//!   "construct host + attach writer + read principal + build context"
//!   block between `uni-query/src/query/executor/procedure.rs` and
//!   `uni-query/src/query/df_graph/procedure_call.rs`.
//! - [`principal::maybe_scope_with_principal`] — wraps the conditional
//!   `match principal { Some(p) => scoped_with_principal(...).await, None => fut.await }`
//!   pattern duplicated across `uni::api::{session,transaction}` and
//!   `uni-query::df_udfs`.
//!
//! The principal task-local ([`principal::CURRENT_PRINCIPAL`]) and its
//! `scoped_with_principal` / `current_principal` helpers also live here.
//! `uni-query` re-exports them for backwards compatibility.

// Rust guideline compliant

pub mod context;
pub mod principal;

#[doc(inline)]
pub use context::build_procedure_context;
#[doc(inline)]
pub use principal::{
    CURRENT_PRINCIPAL, current_principal, maybe_scope_with_principal, scoped_with_principal,
};
