// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression test for rustic-ai/uni-db#67.
//!
//! Before the fix, declaring a property named `ext_id` plus a scalar index
//! on it would let `apply()` succeed and then fail much later inside
//! `flush()` / `shutdown()` with a Lance "Duplicate field name" error,
//! silently losing all in-session writes. The fix rejects reserved
//! property names at schema-apply time so the failure is synchronous and
//! recoverable.

use uni_db::{DataType, IndexType, ScalarType, Uni};

#[tokio::test]
async fn issue_67_reserved_ext_id_property_rejected_at_apply() {
    let db = Uni::in_memory().build().await.unwrap();

    let err = db
        .schema()
        .label("Tiny")
        .property("ext_id", DataType::String)
        .index("ext_id", IndexType::Scalar(ScalarType::Hash))
        .done()
        .apply()
        .await
        .expect_err("declaring 'ext_id' as a user property must fail at apply time");

    let msg = err.to_string();
    assert!(
        msg.contains("reserved"),
        "error should mention 'reserved'; got: {msg}"
    );
}

#[tokio::test]
async fn leading_underscore_property_rejected_at_apply() {
    let db = Uni::in_memory().build().await.unwrap();

    let err = db
        .schema()
        .label("Tiny")
        .property("_vid", DataType::Int64)
        .done()
        .apply()
        .await
        .expect_err("leading-underscore property names must be rejected");

    assert!(err.to_string().contains("reserved"));
}

#[tokio::test]
async fn non_reserved_property_with_index_still_works() {
    // Sanity check: the fix should only reject the reserved names; the
    // happy path for a scalar index on a normal property still works.
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Tiny")
        .property("name", DataType::String)
        .index("name", IndexType::Scalar(ScalarType::Hash))
        .done()
        .apply()
        .await
        .expect("non-reserved property name should succeed");
}
