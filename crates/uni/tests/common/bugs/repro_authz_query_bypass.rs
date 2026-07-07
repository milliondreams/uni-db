#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for crates/uni/src/api/transaction.rs:338 (finding [4]).
//!
//! `Transaction::query` (query_inner) performs NO `AuthzPolicy` consultation —
//! it never calls `self.authorize()`. `Session::run` deliberately routes write
//! statements through `tx.query` (to preserve trailing RETURN rows), so any
//! write/schema/dbms statement executed via `Session::run` or a direct
//! `Transaction::query` bypasses authorization. By contrast `tx.execute`
//! (authorize at line 483) and the parameterized builders enforce it.
//!
//! The bypass is observable only when at least one `AuthzPolicy` is registered
//! (an empty policy set returns Ok early).

use std::sync::Arc;

use uni_db::{Uni, UniError};
use uni_plugin::traits::connector::{
    Action, AuthzError, AuthzPolicy, Decision, Principal, Resource,
};
use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar};

/// Denies every action whose verb appears in `deny_verbs`.
struct DenyVerbs {
    deny_verbs: Vec<String>,
}

impl AuthzPolicy for DenyVerbs {
    fn check(
        &self,
        _principal: &Principal,
        action: &Action,
        _resource: &Resource,
    ) -> Result<Decision, AuthzError> {
        if self.deny_verbs.iter().any(|v| v == &action.verb) {
            Ok(Decision::Deny {
                reason: format!("verb `{}` denied by policy", action.verb),
            })
        } else {
            Ok(Decision::Allow)
        }
    }
}

fn register_deny(uni: &Uni, deny_verbs: Vec<String>) -> Result<(), uni_plugin::PluginError> {
    let registry = uni.plugin_registry();
    let caps = CapabilitySet::from_iter_of([Capability::Authz]);
    let mut r = PluginRegistrar::new(PluginId::new("test-deny-write"), &caps, registry);
    r.authz_policy(Arc::new(DenyVerbs { deny_verbs }))?;
    r.commit_to_registry()
}

#[tokio::test]
async fn write_via_session_run_and_tx_query_bypasses_authz() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    register_deny(&db, vec!["write".to_string()])?;

    let session = db.session();

    // Positive control: the identical statement on the `execute` path IS denied
    // (authorize fires at transaction.rs:483).
    {
        let tx = session.tx().await?;
        let denied = tx.execute("CREATE (n:Secret {x: 1})").await;
        tx.rollback();
        assert!(
            matches!(denied, Err(UniError::AuthorizationDenied { .. })),
            "control: tx.execute must be denied by the write policy, got {denied:?}"
        );
    }

    // (1) FIXED: Session::run routes the write through tx.query, which now
    // authorizes at query_inner — so the deny-write policy blocks it.
    let run_result = session.run("CREATE (n:Secret {x: 2})").await;
    assert!(
        matches!(run_result, Err(UniError::AuthorizationDenied { .. })),
        "session.run() write must be denied by the AuthzPolicy; got {run_result:?}"
    );

    // (2) FIXED: a direct Transaction::query write is also authorized.
    let tx = session.tx().await?;
    let q_result = tx.query("CREATE (n:Secret {x: 3})").await;
    assert!(
        matches!(q_result, Err(UniError::AuthorizationDenied { .. })),
        "tx.query() write must be denied by the AuthzPolicy; got {q_result:?}"
    );
    let _ = tx.commit().await;

    // No :Secret nodes were created — every write path was blocked.
    let count = session
        .query("MATCH (n:Secret) RETURN count(n) AS c")
        .await?;
    let c: i64 = count.rows()[0].get("c")?;
    assert_eq!(
        c, 0,
        "no :Secret node must exist once every write path enforces the deny-write policy; got {c}"
    );

    Ok(())
}
