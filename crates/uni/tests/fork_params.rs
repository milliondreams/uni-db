// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Phase 4a — params are per-session and do not propagate to forks.
//!
//! Spec §4.5 contract: a forked session starts with empty params; the
//! parent's params do not bleed into the fork, and the fork's params
//! do not bleed back. Implementation: `Session::new_forked` initializes
//! params as an empty `HashMap` regardless of parent state.

// Rust guideline compliant

use anyhow::Result;
use uni_common::core::schema::DataType;
use uni_db::Uni;

#[tokio::test]
async fn fork_params_do_not_inherit_from_parent() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let primary = db.session();
    primary.params().set("region", "us-east");

    let fork = primary.fork("scenario").await?;
    assert!(
        fork.params().get("region").is_none(),
        "fork must not inherit primary's params"
    );

    // Mutation on fork does not bleed back to primary.
    fork.params().set("region", "eu-west");
    assert_eq!(
        primary
            .params()
            .get("region")
            .and_then(|v| v.as_str().map(String::from)),
        Some("us-east".to_string()),
        "primary's param unchanged after fork set"
    );

    db.shutdown().await?;
    Ok(())
}
