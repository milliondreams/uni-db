// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for RC9: an empty typed `List<Float32>` column must keep its element
//! type across flush + reopen (not be coerced to `List<Utf8>`).
//!
//! The mutation path normalizes `List<T>` to a CV-encoded `LargeBinary`
//! (`uni-query/src/query/df_graph/mutation_common.rs`), but the audit flagged a
//! possible residual where a typed list column with no producer rows infers
//! `List<Utf8>` at schema reconstruction. This test settles that: it declares a
//! `List<Float32>` column, flushes with no rows written to it, reopens, and
//! asserts the persisted element type is unchanged.
//!
//! Unlike the RC4/RC8 repros, this one is left active: if it passes, RC9's
//! empty-column residual is already closed; if it fails, it documents the live
//! gap (mark `#[ignore]` then and file the residual).
//!
//! Run with:
//!   cargo nextest run -p uni --test integration bug_empty_typed_list_inference

use anyhow::Result;
use tempfile::tempdir;
use uni_db::{DataType, Uni};

/// A `List<Float32>` column survives flush + reopen without Utf8 coercion.
#[tokio::test]
async fn empty_typed_list_column_preserves_element_type() -> Result<()> {
    let dir = tempdir()?;
    let path = dir
        .path()
        .to_str()
        .expect("temp path is valid UTF-8")
        .to_string();

    // Declare the typed list column, flush with no rows, then close.
    {
        let db = Uni::open(&path).build().await?;
        db.schema()
            .label("Vec")
            .property_nullable("data", DataType::List(Box::new(DataType::Float32)))
            .done()
            .apply()
            .await?;
        db.flush().await?;
        db.shutdown().await?;
    }

    // Reopen and inspect the persisted element type.
    let db = Uni::open(&path).build().await?;
    let schema = db.schema().current();
    let data_type = schema
        .properties
        .get("Vec")
        .expect("label `Vec` present after reopen")
        .get("data")
        .expect("property `data` present after reopen")
        .r#type
        .clone();

    assert_eq!(
        data_type,
        DataType::List(Box::new(DataType::Float32)),
        "empty List<Float32> must not be coerced on reopen"
    );

    Ok(())
}
