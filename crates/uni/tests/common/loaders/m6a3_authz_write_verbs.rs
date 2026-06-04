#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! M6a.3 acceptance — `Transaction::execute` consults the registered
//! `AuthzPolicy` chain with a verb classified per the Cypher payload
//! prefix (`"write"`, `"schema"`, or `"dbms"`).

// Rust guideline compliant

use std::sync::Arc;
use std::sync::Mutex;

use uni_db::{Uni, UniError};
use uni_plugin::traits::connector::{
    Action, AuthzError, AuthzPolicy, Decision, Principal, Resource,
};
use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar};

/// Records every verb the policy is consulted with; allows everything.
struct VerbRecorder {
    seen: Arc<Mutex<Vec<String>>>,
}

impl AuthzPolicy for VerbRecorder {
    fn check(
        &self,
        _principal: &Principal,
        action: &Action,
        _resource: &Resource,
    ) -> Result<Decision, AuthzError> {
        self.seen.lock().unwrap().push(action.verb.clone());
        Ok(Decision::Allow)
    }
}

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

fn register_recorder(
    uni: &Uni,
    seen: Arc<Mutex<Vec<String>>>,
) -> std::result::Result<(), uni_plugin::PluginError> {
    let registry = uni.plugin_registry();
    let caps = CapabilitySet::from_iter_of([Capability::Authz]);
    let mut r = PluginRegistrar::new(PluginId::new("test-verb-recorder"), &caps, registry);
    r.authz_policy(Arc::new(VerbRecorder { seen }))?;
    r.commit_to_registry()
}

fn register_deny(
    uni: &Uni,
    deny_verbs: Vec<String>,
) -> std::result::Result<(), uni_plugin::PluginError> {
    let registry = uni.plugin_registry();
    let caps = CapabilitySet::from_iter_of([Capability::Authz]);
    let mut r = PluginRegistrar::new(PluginId::new("test-deny-verbs"), &caps, registry);
    r.authz_policy(Arc::new(DenyVerbs { deny_verbs }))?;
    r.commit_to_registry()
}

#[tokio::test]
async fn write_path_passes_write_verb_to_policies() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    register_recorder(&db, Arc::clone(&seen))?;

    let session = db.session();
    let tx = session.tx().await?;
    let _ = tx.execute("CREATE (n:Foo)").await;
    tx.rollback();

    let verbs = seen.lock().unwrap().clone();
    // First call comes from Session::query inside execute → "read";
    // M6a.3 adds the "write" call ahead of it from execute itself.
    assert!(
        verbs.contains(&"write".to_owned()),
        "policies should see `write` verb. Verbs seen: {verbs:?}"
    );
    Ok(())
}

#[tokio::test]
async fn schema_ddl_classified_as_schema() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    register_recorder(&db, Arc::clone(&seen))?;

    let session = db.session();
    let tx = session.tx().await?;
    // We don't care whether this succeeds — only that the verb is
    // classified before the executor sees it.
    let _ = tx
        .execute("CREATE INDEX idx_foo FOR (n:Foo) ON (n.bar)")
        .await;
    tx.rollback();

    let verbs = seen.lock().unwrap().clone();
    assert!(
        verbs.contains(&"schema".to_owned()),
        "CREATE INDEX should classify as `schema`. Verbs seen: {verbs:?}"
    );
    Ok(())
}

#[tokio::test]
async fn dbms_admin_classified_as_dbms() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    register_recorder(&db, Arc::clone(&seen))?;

    let session = db.session();
    let tx = session.tx().await?;
    let _ = tx.execute("CREATE USER alice SET PASSWORD 'x'").await;
    tx.rollback();

    let verbs = seen.lock().unwrap().clone();
    assert!(
        verbs.contains(&"dbms".to_owned()),
        "CREATE USER should classify as `dbms`. Verbs seen: {verbs:?}"
    );
    Ok(())
}

#[tokio::test]
async fn write_denial_propagates_authorization_denied() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    register_deny(&db, vec!["write".to_owned()])?;

    let session = db.session();
    let tx = session.tx().await?;
    let err = tx
        .execute("CREATE (n:Foo)")
        .await
        .expect_err("policy denies `write`");
    match err {
        UniError::AuthorizationDenied { reason } => {
            assert!(reason.contains("write"), "reason: {reason}");
        }
        other => panic!("expected AuthorizationDenied, got {other:?}"),
    }
    tx.rollback();
    Ok(())
}

#[tokio::test]
async fn empty_policy_chain_skips_authz_consultation() -> anyhow::Result<()> {
    // No policies registered — execute path must short-circuit on the
    // empty Arc<Vec<>> snapshot and proceed without consulting anyone.
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    let result = tx.execute("CREATE (n:Foo)").await;
    // We don't care whether the CREATE itself succeeds in this minimal
    // setup; only that the path doesn't error out on the authz hop.
    if let Err(e) = result
        && matches!(e, UniError::AuthorizationDenied { .. })
    {
        panic!("authz should not engage with an empty policy chain");
    }
    tx.rollback();
    Ok(())
}
