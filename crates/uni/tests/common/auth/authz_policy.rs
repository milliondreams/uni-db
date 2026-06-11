#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Workstream I acceptance — registered `AuthzPolicy`s gate session
//! query execution. Allow → query runs; Deny → `UniError::AuthorizationDenied`.

// Rust guideline compliant

use std::sync::Arc;

use uni_db::{Uni, UniError};
use uni_plugin::traits::connector::{
    Action, AuthzError, AuthzPolicy, Decision, Principal, Resource,
};
use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar};

/// Allow everyone in `allow_group`; deny everything else.
struct AllowGroup {
    allow_group: String,
}

impl AuthzPolicy for AllowGroup {
    fn check(
        &self,
        principal: &Principal,
        _action: &Action,
        _resource: &Resource,
    ) -> Result<Decision, AuthzError> {
        if principal.groups.iter().any(|g| g == &self.allow_group) {
            Ok(Decision::Allow)
        } else {
            Ok(Decision::Deny {
                reason: format!(
                    "principal `{}` is not in `{}`",
                    principal.id, self.allow_group
                ),
            })
        }
    }
}

fn register_policy(
    uni: &Uni,
    allow_group: &str,
) -> std::result::Result<(), uni_plugin::PluginError> {
    let registry = uni.plugin_registry();
    let caps = CapabilitySet::from_iter_of([Capability::Authz]);
    let mut r = PluginRegistrar::new(PluginId::new("test-allow-group"), &caps, registry);
    r.authz_policy(Arc::new(AllowGroup {
        allow_group: allow_group.to_owned(),
    }))?;
    r.commit_to_registry()?;
    Ok(())
}

#[tokio::test]
async fn allow_group_grants_read_to_member() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    // Use "admin" to align with the builtin AllowGroupAuthzPolicy
    // which auto-registers requiring `admin` membership. Both this
    // test policy and the builtin must allow (AND semantics).
    register_policy(&db, "admin")?;

    let session = db.session().with_principal(Arc::new(Principal {
        id: "alice".to_owned(),
        groups: vec!["admin".to_owned()],
        capabilities: uni_plugin::CapabilitySet::default(),
    }));
    let rows = session.query("RETURN 1 AS one").await?;
    assert_eq!(rows.len(), 1);
    Ok(())
}

#[tokio::test]
async fn deny_principal_blocks_query() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    // Use "admin" to align with the builtin AllowGroupAuthzPolicy
    // which auto-registers requiring `admin` membership. Both this
    // test policy and the builtin must allow (AND semantics).
    register_policy(&db, "admin")?;

    let session = db.session().with_principal(Arc::new(Principal {
        id: "mallory".to_owned(),
        groups: vec!["guests".to_owned()],
        capabilities: uni_plugin::CapabilitySet::default(),
    }));
    let err = session
        .query("RETURN 1 AS one")
        .await
        .expect_err("non-admin must be denied");
    match err {
        UniError::AuthorizationDenied { reason } => {
            // The reason text comes from whichever policy denied
            // first (builtin or test policy); both reference
            // "mallory" and the required group.
            assert!(
                reason.contains("mallory"),
                "reason should mention principal: {reason}"
            );
        }
        other => panic!("expected AuthorizationDenied, got {other:?}"),
    }
    Ok(())
}

/// Regression for review (Tier 2): the parameterized builder path
/// (`query_with(...).param(...).fetch_all()`) must consult the same
/// `AuthzPolicy` as the typed `query`. Previously the builder bypassed
/// authorization, so any policy could be evaded by parameterizing the query —
/// the path the Python bindings take whenever params are supplied.
#[tokio::test]
async fn deny_principal_blocks_parameterized_builder() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    register_policy(&db, "admin")?;

    let session = db.session().with_principal(Arc::new(Principal {
        id: "mallory".to_owned(),
        groups: vec!["guests".to_owned()],
        capabilities: uni_plugin::CapabilitySet::default(),
    }));

    // fetch_all
    let err = session
        .query_with("RETURN $x AS one")
        .param("x", 1i64)
        .fetch_all()
        .await
        .expect_err("non-admin must be denied on the builder fetch_all path");
    assert!(matches!(err, UniError::AuthorizationDenied { .. }));

    // profile
    let err = session
        .query_with("RETURN $x AS one")
        .param("x", 1i64)
        .profile()
        .await
        .expect_err("non-admin must be denied on the builder profile path");
    assert!(matches!(err, UniError::AuthorizationDenied { .. }));

    Ok(())
}

/// A write smuggled through the read builder's `.profile()` / `.cursor()` must
/// be rejected as read-only (it previously executed non-transactionally).
#[tokio::test]
async fn builder_profile_rejects_write() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let err = session
        .query_with("CREATE (:Smuggled {v: $v})")
        .param("v", 1i64)
        .profile()
        .await
        .expect_err("a write via .profile() must be rejected as read-only");
    assert!(
        err.to_string().contains("read-only"),
        "expected read-only violation, got: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn anonymous_principal_sees_policies() -> anyhow::Result<()> {
    // No principal set on session — policy sees "anonymous" with no
    // groups, gets denied.
    let db = Uni::in_memory().build().await?;
    // Use "admin" to align with the builtin AllowGroupAuthzPolicy
    // which auto-registers requiring `admin` membership. Both this
    // test policy and the builtin must allow (AND semantics).
    register_policy(&db, "admin")?;

    let err = db
        .session()
        .query("RETURN 1 AS one")
        .await
        .expect_err("anonymous must be denied when policy requires `readers`");
    match err {
        UniError::AuthorizationDenied { reason } => {
            assert!(
                reason.contains("anonymous"),
                "policy should see the anonymous principal: {reason}"
            );
        }
        other => panic!("expected AuthorizationDenied, got {other:?}"),
    }
    Ok(())
}
