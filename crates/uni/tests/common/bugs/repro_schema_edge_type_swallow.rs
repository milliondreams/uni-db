#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for crates/uni/src/api/schema.rs:207 (finding [5]).
//!
//! `SchemaBuilder::apply` handles `SchemaChange::AddEdgeType` by calling
//! `manager.add_edge_type_with_desc(...)` and, at line 207, swallows any
//! `Err(e) if e.to_string().contains("already exists")` with an empty body.
//! `add_edge_type_with_desc` errors purely on NAME collision and returns
//! *before* updating the stored `src_labels`/`dst_labels`. So re-declaring an
//! edge type with DIFFERENT from/to labels is silently no-op'd: no error
//! surfaces and the stored definition keeps the old labels.
//!
//! Contrast: `AddProperty` (declare_property) errors on a conflicting
//! re-declaration; `AddIndex` upserts. `AddEdgeType` alone neither upserts nor
//! detects the conflict.

use uni_db::{DataType, Uni};

#[tokio::test]
async fn edge_type_relabel_silently_swallowed() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .done()
        .label("Company")
        .property("name", DataType::String)
        .done()
        .apply()
        .await?;

    // (1) Establish KNOWS with from=[Person], to=[Person].
    db.schema()
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()
        .await?;

    let before = db.schema().current();
    let meta_before = before
        .edge_types
        .get("KNOWS")
        .expect("KNOWS must exist after first declaration");
    assert_eq!(meta_before.src_labels, vec!["Person".to_string()]);

    // (2) Re-declare KNOWS with a DIFFERENT from-label [Company].
    //     Correct behavior: either update the stored src_labels to [Company],
    //     or return Err reporting the conflict (mirroring declare_property).
    let result = db
        .schema()
        .edge_type("KNOWS", &["Company"], &["Person"])
        .apply()
        .await;

    // Fixed (schema.rs:207): the conflicting re-declaration is surfaced as an
    // error instead of being silently swallowed.
    assert!(
        result.is_err(),
        "re-declaring KNOWS with different endpoint labels must return an error"
    );

    // The stored definition is unchanged (the error is returned before any
    // mutation), so the original [Person] endpoint labels are intact — not
    // silently replaced or corrupted.
    let after = db.schema().current();
    let meta_after = after.edge_types.get("KNOWS").expect("KNOWS still present");
    assert_eq!(
        meta_after.src_labels,
        vec!["Person".to_string()],
        "the original endpoint labels must be preserved after the rejected re-declaration"
    );

    // An idempotent re-declaration with the SAME labels still succeeds.
    db.schema()
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()
        .await?;

    Ok(())
}
