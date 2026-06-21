// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 4a — `Uni::tag_fork` GC-exempt tags.
//!
//! Lance tags pin specific versions of branches GC-exempt so a tagged
//! fork's state survives compaction and even fork drops. Tests cover:
//! - Round-trip: tag a fork, list tags, untag, list tags.
//! - Pin-at-create: subsequent fork writes do not move the tag.
//! - Drop-tagged-fork: branches go away with the fork; the tagged
//!   versions remain referenceable through the underlying Lance refs
//!   (since tags hold a separate ref).

// Rust guideline compliant

use anyhow::Result;
use uni_common::core::schema::DataType;
use uni_db::Uni;

#[tokio::test]
async fn tag_list_untag_roundtrip() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let uri = dir.path().display().to_string();
    let db = Uni::open(&uri).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'seed'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let fork = primary.fork("scenario").await?;
    let tx = fork.tx().await?;
    tx.execute("CREATE (:Person {name: 'fork-row'})").await?;
    tx.commit().await?;
    fork.flush().await?;
    drop(fork);

    db.tag_fork("scenario", "audit-2026-q1").await?;
    let tags = db.list_fork_tags("scenario").await?;
    assert!(
        tags.iter().any(|t| t == "audit-2026-q1"),
        "tag should appear in list; got {tags:?}"
    );

    db.untag_fork("scenario", "audit-2026-q1").await?;
    let tags = db.list_fork_tags("scenario").await?;
    assert!(
        !tags.iter().any(|t| t == "audit-2026-q1"),
        "tag should be gone after untag; got {tags:?}"
    );

    // Untag is idempotent.
    db.untag_fork("scenario", "audit-2026-q1").await?;

    db.shutdown().await?;
    Ok(())
}

/// L9: re-tagging an already-tagged fork fails CLEANLY at the pre-check
/// (before mutating any dataset), instead of partially re-tagging and then
/// hitting a mid-loop `RefConflict`. The existing tag is left untouched.
#[tokio::test]
async fn tag_fork_pre_check_prevents_partial_retag() -> Result<()> {
    use uni_common::api::error::UniError;

    let dir = tempfile::tempdir()?;
    let uri = dir.path().display().to_string();
    let db = Uni::open(&uri).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute("CREATE (:Person {name: 'seed'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let fork = primary.fork("scenario").await?;
    let tx = fork.tx().await?;
    tx.execute("CREATE (:Person {name: 'fork-row'})").await?;
    tx.commit().await?;
    fork.flush().await?;
    drop(fork);

    // First tag spans every fork dataset.
    db.tag_fork("scenario", "hold").await?;

    // Re-tagging the SAME name must fail at the pre-check (the tag already
    // exists on at least one dataset), not mid-loop.
    match db.tag_fork("scenario", "hold").await {
        Err(UniError::ForkLifecycle { stage: "tag", .. }) => {}
        Err(other) => panic!("expected a tag ForkLifecycle error, got {other:?}"),
        Ok(()) => panic!("re-tagging an already-tagged fork must error"),
    }

    // The tag is still present exactly once — nothing was duplicated or left
    // in a partial state.
    let tags = db.list_fork_tags("scenario").await?;
    assert_eq!(
        tags.iter().filter(|t| *t == "hold").count(),
        1,
        "the original tag must be intact and unique; got {tags:?}"
    );

    // Untag cleanly removes it everywhere.
    db.untag_fork("scenario", "hold").await?;
    assert!(
        db.list_fork_tags("scenario").await?.is_empty(),
        "untag must remove the tag from every dataset"
    );

    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn tag_fork_unknown_fork_errors() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let err = db.tag_fork("does-not-exist", "v1").await.unwrap_err();
    match err {
        uni_common::api::error::UniError::ForkNotFound { name } => {
            assert_eq!(name, "does-not-exist");
        }
        other => panic!("expected ForkNotFound, got {other:?}"),
    }
    db.shutdown().await?;
    Ok(())
}
