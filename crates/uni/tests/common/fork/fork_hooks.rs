// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 4a — hooks are per-session, no propagation across forks.
//!
//! Spec §4.4 contract: a forked session starts with empty hooks;
//! hooks added on the parent after fork creation do not propagate;
//! hooks added on the fork do not propagate back. Implementation:
//! `Session::new_forked` initializes hooks as an empty `HashMap`.

// Rust guideline compliant

// Tests cover the deprecated `Session::add_hook` API path; suppress the warning here.
#![allow(deprecated)]

use anyhow::Result;
use uni_common::api::error::Result as UniResult;
use uni_common::core::schema::DataType;
use uni_db::{CommitHookContext, HookContext, SessionHook, Uni};

struct NoopHook;
impl SessionHook for NoopHook {
    fn before_query(&self, _ctx: &HookContext) -> UniResult<()> {
        Ok(())
    }
    fn before_commit(&self, _ctx: &CommitHookContext) -> UniResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn fork_hooks_do_not_inherit_from_parent() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let mut primary = db.session();
    primary.add_hook("audit", NoopHook);
    assert_eq!(primary.list_hooks(), vec!["audit".to_string()]);

    let mut fork = primary.fork("scenario").await?;
    assert!(
        fork.list_hooks().is_empty(),
        "fork must not inherit primary's hooks; got {:?}",
        fork.list_hooks()
    );

    // Hook on the fork does not appear on primary.
    fork.add_hook("fork-only", NoopHook);
    assert_eq!(primary.list_hooks(), vec!["audit".to_string()]);
    assert_eq!(fork.list_hooks(), vec!["fork-only".to_string()]);

    db.shutdown().await?;
    Ok(())
}
