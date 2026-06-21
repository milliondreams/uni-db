// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Review L5: fork names are validated before any registry/catalog state
//! is created. Empty, all-whitespace, over-long, and control-character
//! names are rejected with `ForkNameInvalid`, and no fork entry is left
//! behind.

// Rust guideline compliant

use uni_db::{Uni, UniError};

#[tokio::test]
async fn fork_rejects_invalid_names() {
    let db = Uni::in_memory().build().await.unwrap();
    let session = db.session();

    let long = "x".repeat(300);
    let invalid = ["", "   ", "\t\n", "bad\nname", "nul\0byte", long.as_str()];
    for name in invalid {
        match session.fork(name).await {
            Err(UniError::ForkNameInvalid { .. }) => {}
            Err(other) => panic!("expected ForkNameInvalid for {name:?}, got {other:?}"),
            Ok(_) => panic!("fork({name:?}) should be rejected"),
        }
    }

    // No invalid name leaked a registry entry.
    let active = db.list_forks().await;
    assert!(
        active.is_empty(),
        "invalid names must not create forks: {active:?}"
    );

    // A valid name still works.
    let _ok = session.fork("valid_name").await.unwrap();

    db.shutdown().await.unwrap();
}
