#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Workstream I acceptance — `Uni::session_with_credentials` reaches
//! registered `AuthProvider`s and surfaces success / failure as a
//! [`Principal`] on the session or a [`UniError::AuthenticationFailed`].

// Rust guideline compliant

use std::sync::Arc;

use uni_db::{Uni, UniError};
use uni_plugin::traits::connector::{AuthError, AuthProvider, Credentials, Principal};
use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar};

struct StaticBasic;

impl AuthProvider for StaticBasic {
    fn scheme(&self) -> &str {
        "basic"
    }
    fn authenticate(&self, credentials: &Credentials) -> Result<Principal, AuthError> {
        match credentials {
            Credentials::Basic { username, password } => {
                if username == "alice" && password == "open-sesame" {
                    Ok(Principal {
                        id: "alice".to_owned(),
                        groups: vec!["admin".to_owned()],
                        capabilities: uni_plugin::CapabilitySet::default(),
                    })
                } else {
                    Err(AuthError("bad credentials".to_owned()))
                }
            }
            _ => Err(AuthError("provider only handles basic".to_owned())),
        }
    }
}

fn register_provider(uni: &Uni) -> std::result::Result<(), uni_plugin::PluginError> {
    let registry = uni.plugin_registry();
    let caps = CapabilitySet::from_iter_of([Capability::Auth]);
    let mut r = PluginRegistrar::new(PluginId::new("test-basic-auth"), &caps, registry);
    r.auth_provider(Arc::new(StaticBasic))?;
    r.commit_to_registry()?;
    Ok(())
}

#[tokio::test]
async fn basic_auth_round_trip() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    register_provider(&db)?;

    let session = db.session_with_credentials(Credentials::Basic {
        username: "alice".to_owned(),
        password: "open-sesame".to_owned(),
    })?;
    let principal = session
        .principal()
        .expect("session should carry an authenticated principal");
    assert_eq!(principal.id, "alice");
    assert_eq!(principal.groups, vec!["admin".to_owned()]);
    Ok(())
}

#[tokio::test]
async fn auth_failure_aborts_session_open() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    register_provider(&db)?;

    let err = match db.session_with_credentials(Credentials::Basic {
        username: "mallory".to_owned(),
        password: "wrong".to_owned(),
    }) {
        Ok(_) => panic!("auth must reject bad credentials"),
        Err(e) => e,
    };
    match err {
        UniError::AuthenticationFailed { reason } => {
            assert!(reason.contains("bad credentials"), "reason: {reason}");
        }
        other => panic!("expected AuthenticationFailed, got {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn unknown_scheme_returns_authentication_failed() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    // No provider registered for "bearer". Credentials::Bearer must
    // fail loudly rather than fall through to anonymous.
    let err = match db.session_with_credentials(Credentials::Bearer("token".to_owned())) {
        Ok(_) => panic!("must reject when no matching provider is registered"),
        Err(e) => e,
    };
    match err {
        UniError::AuthenticationFailed { reason } => {
            assert!(reason.contains("scheme"), "reason: {reason}");
        }
        other => panic!("expected AuthenticationFailed, got {other:?}"),
    }
    Ok(())
}
