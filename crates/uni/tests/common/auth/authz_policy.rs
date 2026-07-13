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

    // explain — leaks plan/cost info, so it must consult the policy too.
    let err = session
        .query_with("RETURN $x AS one")
        .param("x", 1i64)
        .explain()
        .await
        .expect_err("non-admin must be denied on the builder explain path");
    assert!(matches!(err, UniError::AuthorizationDenied { .. }));

    Ok(())
}

/// Regression for review (Tier 3, bonus #5b): a `PreparedQuery` must re-consult
/// the `AuthzPolicy` on every execution, not only at prepare time. A cached
/// handle previously executed without re-authorization.
#[tokio::test]
async fn deny_principal_blocks_prepared_execute() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    register_policy(&db, "admin")?;

    let session = db.session().with_principal(Arc::new(Principal {
        id: "mallory".to_owned(),
        groups: vec!["guests".to_owned()],
        capabilities: uni_plugin::CapabilitySet::default(),
    }));

    // `prepare` itself is allowed (no execution), but `execute` must authorize.
    let prepared = session.prepare("RETURN 1 AS one").await?;
    let err = prepared
        .execute(&[])
        .await
        .expect_err("non-admin must be denied when executing a prepared query");
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

/// Policy that denies if the query touches any label outside its allowlist —
/// exercises the structured `Resource.labels` extraction (not the raw string).
struct AllowLabels {
    allowed: Vec<String>,
}

impl AuthzPolicy for AllowLabels {
    fn check(
        &self,
        _principal: &Principal,
        _action: &Action,
        resource: &Resource,
    ) -> Result<Decision, AuthzError> {
        for label in &resource.labels {
            if !self.allowed.iter().any(|a| a == label) {
                return Ok(Decision::Deny {
                    reason: format!("label `{label}` is not permitted"),
                });
            }
        }
        Ok(Decision::Allow)
    }
}

#[tokio::test]
async fn structured_resource_gates_by_label() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;

    // Register a label-gating policy (allows only `Doc`). The always-on builtin
    // AllowGroupAuthzPolicy also applies (AND), so the principal is in `admin`.
    {
        let caps = CapabilitySet::from_iter_of([Capability::Authz]);
        let mut r = PluginRegistrar::new(
            PluginId::new("test-allow-labels"),
            &caps,
            db.plugin_registry(),
        );
        r.authz_policy(Arc::new(AllowLabels {
            allowed: vec!["Doc".to_owned()],
        }))?;
        r.commit_to_registry()?;
    }

    let session = db.session().with_principal(Arc::new(Principal {
        id: "alice".to_owned(),
        groups: vec!["admin".to_owned()],
        capabilities: uni_plugin::CapabilitySet::default(),
    }));

    // A query touching only `Doc` is allowed — the extractor surfaces labels=[Doc].
    let ok = session.query("MATCH (d:Doc) RETURN d").await;
    assert!(ok.is_ok(), "Doc-only query must be allowed: {ok:?}");

    // A query touching `Other` is denied — structured labels=[Other] fails the
    // policy even though the raw path string would need per-policy parsing.
    let denied = session.query("MATCH (o:Other) RETURN o").await;
    assert!(
        matches!(denied, Err(UniError::AuthorizationDenied { .. })),
        "Other-label query must be denied: {denied:?}"
    );
    Ok(())
}
