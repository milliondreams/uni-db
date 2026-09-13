// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Session-level wrappers for physical-operator assertions.
//!
//! The comparison core lives in `uni_query::plan_shape` so both this crate's
//! tests and `uni-query`'s can use it — `uni-query` cannot dev-depend on `uni`
//! (that is a dependency cycle), but `uni` depends on `uni-query`, so the
//! string logic goes in the lower crate and each side keeps a thin wrapper that
//! knows how to obtain a `ProfileOutput`.
//!
//! # Retrofit recipe
//!
//! The work-list is `docs/testing/silent-downgrades-2026-08-15.md`, which
//! catalogues the 29 planner sites where an optimization falls back to a
//! result-identical path. Note it also records what this module **cannot** reach:
//! five of those sites are logical-plan rewrites that never produce a distinct
//! physical operator name, so [`assert_plan_uses`] has nothing to match on.
//!
//! 1. Find a query that should emit `FooExec`, and read the guard conditions at
//!    its construction site in `df_planner.rs` — the fixture shape is usually
//!    load-bearing (a bare-variable projection, a `WHERE` on the probe side, or
//!    running inside a transaction can each silently defeat an optimization).
//! 2. Write the test **next to the feature it belongs to**, not here.
//! 3. Call [`assert_plan_uses`].
//! 4. **If the operator is an optimization with a fallback, the negative twin is
//!    mandatory**: a second query outside the guard conditions, asserting the
//!    operator is absent *and* the fallback present. Template:
//!    `sparse_scoring.rs:277/296`.
//! 5. Keep the existing result assertions. `assert_plan_uses` proves it ran;
//!    the bag proves it ran *correctly*. Neither substitutes for the other.
//! 6. Flip the row in `plan_shape/registry.rs` to `Proven` in the same change —
//!    the gate fails until you do.
//!
//! Note `profile()` **executes** the query, so these run real work; for
//! mutations use a throwaway `Uni::in_memory()`.

pub mod gate;
pub mod proofs;
pub mod registry;

use uni_db::{Session, Transaction};
use uni_query::plan_shape;

/// Physical operator names from profiling `query`.
///
/// # Panics
///
/// Panics if the query fails to execute — `profile()` runs it.
pub async fn plan_ops(session: &Session, query: &str) -> Vec<String> {
    let (_result, profile) = session
        .query_with(query)
        .profile()
        .await
        .unwrap_or_else(|e| panic!("profile failed for `{query}`: {e}"));
    plan_shape::op_names(&profile)
}

/// Asserts `query` runs physical operator `op`.
///
/// # Panics
///
/// Panics if the query fails, or if `op` is absent from the executed plan.
pub async fn assert_plan_uses(session: &Session, query: &str, op: &str) {
    let ops = plan_ops(session, query).await;
    plan_shape::assert_uses(&ops, op, query);
}

/// Asserts `query` does **not** run physical operator `op`.
///
/// # Panics
///
/// Panics if the query fails, or if `op` is present in the executed plan.
pub async fn assert_plan_avoids(session: &Session, query: &str, op: &str) {
    let ops = plan_ops(session, query).await;
    plan_shape::assert_avoids(&ops, op, query);
}

/// Physical operator names from profiling a Locy `program`'s clause bodies.
///
/// Locy has its own profile surface. `Session::locy_with(..).profile()` returns
/// a `LocyProfileOutput`, whose per-iteration `operators` are produced by the
/// same `collect_plan_metrics` walk Cypher's profile uses — but over each
/// rule's re-planned clause body, once per fixpoint iteration. Flattening
/// strata → rules → iterations → operators gives the same shape
/// [`plan_ops`] returns, so the ordinary matchers apply.
///
/// This reaches only what is lowered *into a clause body*. An operator the
/// evaluator builds imperatively in the post-fixpoint chain — `FoldExec`,
/// `PriorityExec` — never becomes part of a collected plan and cannot be seen
/// here however often it runs. The registry records which those are.
///
/// # Panics
///
/// Panics if the program fails to run — `profile()` evaluates it.
pub async fn locy_plan_ops(session: &Session, program: &str) -> Vec<String> {
    let (_result, profile) = session
        .locy_with(program)
        .profile()
        .await
        .unwrap_or_else(|e| panic!("locy profile failed for `{program}`: {e}"));
    let mut names: Vec<String> = profile
        .profile
        .strata
        .iter()
        .flat_map(|stratum| {
            // Stratum-level operators first — the fixpoint driver belongs to the
            // stratum, not to any one of its rules.
            stratum.operators.iter().chain(
                stratum
                    .rules
                    .iter()
                    .flat_map(|rule| rule.iterations.iter())
                    .flat_map(|iteration| iteration.operators.iter()),
            )
        })
        .map(|op| op.operator.clone())
        .collect();
    names.sort();
    names.dedup();
    names
}

/// Asserts a Locy `program` runs physical operator `op` in some clause body.
///
/// # Panics
///
/// Panics if the program fails, or if `op` is absent from every clause body.
pub async fn assert_locy_plan_uses(session: &Session, program: &str, op: &str) {
    let ops = locy_plan_ops(session, program).await;
    plan_shape::assert_uses(&ops, op, program);
}

/// Asserts a Locy `program` does **not** run physical operator `op`.
///
/// # Panics
///
/// Panics if the program fails, or if `op` is present in a clause body.
pub async fn assert_locy_plan_avoids(session: &Session, program: &str, op: &str) {
    let ops = locy_plan_ops(session, program).await;
    plan_shape::assert_avoids(&ops, op, program);
}

/// Physical operator names from profiling `query` inside a transaction.
///
/// The read-side [`plan_ops`] cannot reach a mutation at all: `Session::query`
/// refuses `CREATE`/`SET`/`DELETE`/`REMOVE`/`MERGE` outright, so every mutation
/// operator has to be profiled through `tx.query_with(..).profile()`.
///
/// The write-path profiler is `Transaction::execute_with(..).profile()` — on
/// `ExecuteBuilder`, not the `TxQueryBuilder` that `query_with` returns.
///
/// Callers pass the result to `uni_query::plan_shape::assert_uses` directly
/// rather than through a wrapper, so that the operator literal appears as an
/// argument to a recognised assertion helper — which is what
/// `gate::has_proof_call` accepts as evidence.
///
/// # Panics
///
/// Panics if the query fails — `profile()` runs it.
pub async fn tx_plan_ops(tx: &Transaction, query: &str) -> Vec<String> {
    let (_result, profile) = tx
        .execute_with(query)
        .profile()
        .await
        .unwrap_or_else(|e| panic!("tx profile failed for `{query}`: {e}"));
    plan_shape::op_names(&profile)
}
