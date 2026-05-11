// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 4a — cancellation cascades from parent to forked children.
//!
//! Spec §4.6 contract: cancelling a parent session cancels its forked
//! children; cancelling a child does not affect the parent. Sibling
//! children are isolated (each gets its own child token). Implementation:
//! `Session::new_forked` stores `parent_token.child_token()` instead of
//! a fresh token.

// Rust guideline compliant

use anyhow::Result;
use uni_common::core::schema::DataType;
use uni_db::Uni;

// `Session::cancel()` cancels the currently-held token and immediately
// replaces it with a fresh one so the session remains usable. To
// observe that the cancellation actually fired (and propagated through
// the parent→child chain), tests capture token clones BEFORE calling
// cancel and assert against those clones afterwards.

#[tokio::test]
async fn parent_cancel_cascades_to_forked_child() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let fork = primary.fork("scenario").await?;

    let primary_token = primary.cancellation_token();
    let fork_token = fork.cancellation_token();
    assert!(!primary_token.is_cancelled());
    assert!(!fork_token.is_cancelled());

    primary.cancel();

    assert!(primary_token.is_cancelled());
    assert!(
        fork_token.is_cancelled(),
        "forked child must inherit parent's cancellation"
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn child_cancel_does_not_affect_parent() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let fork = primary.fork("scenario").await?;

    let primary_token = primary.cancellation_token();
    let fork_token = fork.cancellation_token();

    fork.cancel();

    assert!(fork_token.is_cancelled());
    assert!(
        !primary_token.is_cancelled(),
        "child cancellation must not propagate up"
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn sibling_forks_have_independent_cancellation() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let a = primary.fork("a").await?;
    let b = primary.fork("b").await?;

    let primary_token = primary.cancellation_token();
    let a_token = a.cancellation_token();
    let b_token = b.cancellation_token();

    a.cancel();

    assert!(a_token.is_cancelled());
    assert!(
        !b_token.is_cancelled(),
        "sibling fork must not see another fork's cancellation"
    );
    assert!(!primary_token.is_cancelled());

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn nested_fork_cancellation_cascades_through_levels() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let a = primary.fork("a").await?;
    let b = a.fork("b").await?;
    let c = b.fork("c").await?;

    let a_token = a.cancellation_token();
    let b_token = b.cancellation_token();
    let c_token = c.cancellation_token();

    // Cancelling at the top cascades all the way down.
    primary.cancel();
    assert!(a_token.is_cancelled());
    assert!(b_token.is_cancelled());
    assert!(c_token.is_cancelled());

    db.shutdown().await?;
    Ok(())
}
